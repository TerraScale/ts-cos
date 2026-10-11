package cos

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/a-h/templ"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/crypto/bcrypt"
)

// Users, sessions and lockouts live in three-replica JetStream KV, so every node
// accepts the same logins and sessions and a password change applies everywhere.
const (
	authBucket     = "cos_auth"
	sessionBucket  = "cos_sessions"
	sessionTTL     = 7 * 24 * time.Hour
	sessionCookie  = "cos_session"
	lockAfter      = 5 // consecutive failures per user, on any node
	lockFor        = 15 * time.Minute
	loginPerMinute = 10 // attempts per client address on one node
	passwordMin    = 12
	passwordMax    = 72 // bcrypt's limit
	bcryptCost     = 12
)

var authUserPattern = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

// dummyHash keeps unknown users as slow as real ones.
var dummyHash = sync.OnceValue(func() []byte {
	hash, _ := bcrypt.GenerateFromPassword([]byte(rand.Text()), bcryptCost)
	return hash
})

type authUser struct {
	Bcrypt string `json:"bcrypt"`
}

type authLock struct {
	Failures int       `json:"failures"`
	Last     time.Time `json:"last"`
	Until    time.Time `json:"until"`
}

type authUserKey struct{}

type Auth struct {
	home string
	js   jetstream.JetStream

	mu       sync.Mutex
	users    jetstream.KeyValue // nil until the cluster can store replicated buckets
	sessions jetstream.KeyValue
	limit    limiter
}

func NewAuth(home string, js jetstream.JetStream) *Auth {
	return &Auth{home: home, js: js}
}

// Start waits for cluster quorum, creates the buckets and seeds
// ~/.config/cos/admin.json once; a user already in KV is never overwritten.
func (a *Auth) Start(ctx context.Context) {
	for {
		users, sessions, err := a.open(ctx)
		if err == nil {
			a.seed(ctx, users)
			a.mu.Lock()
			a.users, a.sessions = users, sessions
			a.mu.Unlock()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (a *Auth) open(ctx context.Context) (jetstream.KeyValue, jetstream.KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	users, err := a.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: authBucket, History: 1, Replicas: 3})
	if err != nil {
		return nil, nil, err
	}
	sessions, err := a.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: sessionBucket, TTL: sessionTTL, History: 1, Replicas: 3})
	return users, sessions, err
}

func (a *Auth) seed(ctx context.Context, users jetstream.KeyValue) {
	path := filepath.Join(a.home, ".config/cos/admin.json")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		slog.Error("COS admin.json must be a regular file with mode 0600; not seeded")
		return
	}
	data, err := os.ReadFile(path)
	var admin struct{ User, Bcrypt string }
	if err == nil {
		err = json.Unmarshal(data, &admin)
	}
	if _, costErr := bcrypt.Cost([]byte(admin.Bcrypt)); err != nil || costErr != nil || !authUserPattern.MatchString(admin.User) {
		slog.Error("COS admin.json needs a valid user and bcrypt hash; not seeded")
		return
	}
	value, _ := json.Marshal(authUser{Bcrypt: admin.Bcrypt})
	if _, err := users.Create(ctx, "user."+admin.User, value); err == nil {
		slog.Info("COS admin user seeded from admin.json", "user", admin.User)
	} else if !errors.Is(err, jetstream.ErrKeyExists) {
		slog.Error("COS admin user seeding failed", "error", err)
	}
}

func (a *Auth) kv() (jetstream.KeyValue, jetstream.KeyValue) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.users, a.sessions
}

// Protect serves the login pages and the signed webhooks, and lets only signed-in
// requests reach next.
func (a *Auth) Protect(next, hooks http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /login", func(rw http.ResponseWriter, r *http.Request) {
		renderAuth(rw, r, http.StatusOK, loginPage(""))
	})
	servePWA(mux)
	mux.HandleFunc("POST /login", a.login)
	mux.HandleFunc("GET /static/{file}", serveStatic)
	mux.HandleFunc("POST /logout", a.logout)
	mux.Handle("POST /hooks/{source}", hooks)
	mux.Handle("GET /password", a.require(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		renderAuth(rw, r, http.StatusOK, passwordPage(""))
	})))
	mux.Handle("POST /password", a.require(http.HandlerFunc(a.changePassword)))
	mux.Handle("/", a.require(next))
	protected := http.NewCrossOriginProtection().Handler(mux)
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Cache-Control", "no-store")
		rw.Header().Set("X-Frame-Options", "DENY")
		if !trustedDashboardRemote(r.RemoteAddr) {
			http.Error(rw, "Dashboard access is limited to loopback and Tailscale", http.StatusForbidden)
			return
		}
		protected.ServeHTTP(rw, r)
	})
}

func (a *Auth) require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// A just-started node cannot check sessions yet; do not bounce signed-in users to /login.
		if _, sessions := a.kv(); sessions == nil {
			rw.Header().Set("Retry-After", "2")
			http.Error(rw, "COS is starting; retry in a moment", http.StatusServiceUnavailable)
			return
		}
		user := a.sessionUser(r)
		if user == "" {
			if r.Method == http.MethodGet && r.Header.Get("Datastar-Request") == "" && strings.Contains(r.Header.Get("Accept"), "text/html") {
				http.Redirect(rw, r, "/login", http.StatusSeeOther)
				return
			}
			http.Error(rw, "Sign in at /login", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(rw, r.WithContext(context.WithValue(r.Context(), authUserKey{}, user)))
	})
}

// sessionUser returns the user of a valid session cookie ("<user>.<token>").
func (a *Auth) sessionUser(r *http.Request) string {
	_, sessions := a.kv()
	cookie, err := r.Cookie(sessionCookie)
	if sessions == nil || err != nil {
		return ""
	}
	user, token, ok := strings.Cut(cookie.Value, ".")
	key, valid := sessionKey(user, token)
	if !ok || !valid {
		return ""
	}
	if _, err := sessions.Get(r.Context(), key); err != nil {
		return ""
	}
	return user
}

func sessionKey(user, token string) (string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 || !authUserPattern.MatchString(user) {
		return "", false
	}
	sum := sha256.Sum256(raw)
	return user + "." + hex.EncodeToString(sum[:]), true
}

func (a *Auth) startSession(rw http.ResponseWriter, r *http.Request, sessions jetstream.KeyValue, user string) error {
	raw := make([]byte, 32)
	rand.Read(raw)
	token := base64.RawURLEncoding.EncodeToString(raw)
	key, _ := sessionKey(user, token)
	if _, err := sessions.Put(r.Context(), key, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		return err
	}
	setSessionCookie(rw, r, user+"."+token, int(sessionTTL.Seconds()))
	return nil
}

// ProbeSession signs the deploy health probe in like a browser, as "cos-probe"
// (a session-only user with no password); end deletes the session.
func (a *Auth) ProbeSession(ctx context.Context) (cookie string, end func(), err error) {
	_, sessions := a.kv()
	if sessions == nil {
		return "", nil, errors.New("COS sessions are not available yet")
	}
	raw := make([]byte, 32)
	rand.Read(raw)
	cookie = "cos-probe." + base64.RawURLEncoding.EncodeToString(raw)
	_, token, _ := strings.Cut(cookie, ".")
	key, _ := sessionKey("cos-probe", token)
	if _, err := sessions.Put(ctx, key, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		return "", nil, err
	}
	return cookie, func() { _ = sessions.Delete(context.Background(), key) }, nil
}

func setSessionCookie(rw http.ResponseWriter, r *http.Request, value string, maxAge int) {
	http.SetCookie(rw, &http.Cookie{Name: sessionCookie, Value: value, Path: "/", MaxAge: maxAge,
		HttpOnly: true, Secure: secureRequest(r), SameSite: http.SameSiteLaxMode})
}

func (a *Auth) login(rw http.ResponseWriter, r *http.Request) {
	if !a.limit.allow(clientIP(r), loginPerMinute) {
		renderAuth(rw, r, http.StatusTooManyRequests, loginPage("Too many attempts from your address. Wait a minute."))
		return
	}
	users, sessions := a.kv()
	if users == nil {
		renderAuth(rw, r, http.StatusServiceUnavailable, loginPage("COS is starting. Try again in a minute."))
		return
	}
	user := r.PostFormValue("user")
	_, notice, status := a.verify(r.Context(), users, user, r.PostFormValue("password"))
	if notice != "" {
		renderAuth(rw, r, status, loginPage(notice))
		return
	}
	if err := a.startSession(rw, r, sessions, user); err != nil {
		renderAuth(rw, r, http.StatusServiceUnavailable, loginPage("Could not start a session. Try again."))
		return
	}
	http.Redirect(rw, r, "/", http.StatusSeeOther)
}

// verify checks a password with cluster-wide lockout; notice is empty on success.
func (a *Auth) verify(ctx context.Context, users jetstream.KeyValue, user, password string) (jetstream.KeyValueEntry, string, int) {
	const wrong = "Wrong username or password."
	if !authUserPattern.MatchString(user) || len(password) > passwordMax {
		_ = bcrypt.CompareHashAndPassword(dummyHash(), []byte(password))
		return nil, wrong, http.StatusUnauthorized
	}
	if lock, _ := a.lock(ctx, users, user); time.Now().Before(lock.Until) {
		return nil, "Too many failed attempts. Try again after " + lock.Until.Local().Format("15:04") + ".", http.StatusTooManyRequests
	}
	entry, err := users.Get(ctx, "user."+user)
	var stored authUser
	if err != nil || json.Unmarshal(entry.Value(), &stored) != nil {
		_ = bcrypt.CompareHashAndPassword(dummyHash(), []byte(password))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, wrong, http.StatusUnauthorized
		}
		return nil, "COS cannot read its users right now. Try again.", http.StatusServiceUnavailable
	}
	if bcrypt.CompareHashAndPassword([]byte(stored.Bcrypt), []byte(password)) != nil {
		a.fail(ctx, users, user)
		return nil, wrong, http.StatusUnauthorized
	}
	_ = users.Delete(ctx, "lock."+user)
	return entry, "", http.StatusOK
}

func (a *Auth) lock(ctx context.Context, users jetstream.KeyValue, user string) (authLock, uint64) {
	var lock authLock
	entry, err := users.Get(ctx, "lock."+user)
	if err != nil || json.Unmarshal(entry.Value(), &lock) != nil {
		return authLock{}, 0
	}
	return lock, entry.Revision()
}

// fail counts a failure for user on every node; the CAS retry keeps concurrent failures.
func (a *Auth) fail(ctx context.Context, users jetstream.KeyValue, user string) {
	for range 3 {
		lock, revision := a.lock(ctx, users, user)
		if time.Since(lock.Last) > lockFor {
			lock = authLock{}
		}
		lock.Failures++
		lock.Last = time.Now()
		if lock.Failures >= lockAfter {
			lock.Until = lock.Last.Add(lockFor)
		}
		value, _ := json.Marshal(lock)
		var err error
		if revision == 0 {
			_, err = users.Create(ctx, "lock."+user, value)
		} else {
			_, err = users.Update(ctx, "lock."+user, value, revision)
		}
		if err == nil {
			return
		}
	}
}

func (a *Auth) logout(rw http.ResponseWriter, r *http.Request) {
	_, sessions := a.kv()
	if cookie, err := r.Cookie(sessionCookie); err == nil && sessions != nil {
		user, token, _ := strings.Cut(cookie.Value, ".")
		if key, ok := sessionKey(user, token); ok {
			_ = sessions.Delete(r.Context(), key)
		}
	}
	setSessionCookie(rw, r, "", -1)
	http.Redirect(rw, r, "/login", http.StatusSeeOther)
}

// changePassword replaces the bcrypt hash in KV, signs out every other session of
// the user on every node, and keeps this browser signed in.
func (a *Auth) changePassword(rw http.ResponseWriter, r *http.Request) {
	user := r.Context().Value(authUserKey{}).(string)
	if !a.limit.allow(clientIP(r), loginPerMinute) {
		renderAuth(rw, r, http.StatusTooManyRequests, passwordPage("Too many attempts from your address. Wait a minute."))
		return
	}
	next := r.PostFormValue("new")
	if next != r.PostFormValue("confirm") {
		renderAuth(rw, r, http.StatusBadRequest, passwordPage("The new passwords do not match."))
		return
	}
	if len(next) < passwordMin || len(next) > passwordMax {
		renderAuth(rw, r, http.StatusBadRequest, passwordPage("Use 12 to 72 characters."))
		return
	}
	users, sessions := a.kv()
	entry, notice, status := a.verify(r.Context(), users, user, r.PostFormValue("current"))
	if notice != "" {
		if status == http.StatusUnauthorized {
			notice = "The current password is wrong."
		}
		renderAuth(rw, r, status, passwordPage(notice))
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(next), bcryptCost)
	if err != nil {
		renderAuth(rw, r, http.StatusBadRequest, passwordPage("That password cannot be used."))
		return
	}
	value, _ := json.Marshal(authUser{Bcrypt: string(hash)})
	if _, err := users.Update(r.Context(), "user."+user, value, entry.Revision()); err != nil {
		renderAuth(rw, r, http.StatusConflict, passwordPage("The password changed elsewhere meanwhile. Reload and try again."))
		return
	}
	if keys, err := sessions.ListKeysFiltered(r.Context(), user+".*"); err == nil {
		for key := range keys.Keys() {
			_ = sessions.Delete(r.Context(), key)
		}
	}
	if err := a.startSession(rw, r, sessions, user); err != nil {
		renderAuth(rw, r, http.StatusOK, passwordPage("Password changed. Sign in again."))
		return
	}
	renderAuth(rw, r, http.StatusOK, passwordPage("Password changed on every node. Other sessions were signed out."))
}

func renderAuth(rw http.ResponseWriter, r *http.Request, status int, page templ.Component) {
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.WriteHeader(status)
	if err := page.Render(r.Context(), rw); err != nil {
		slog.Error("render auth page", "error", err)
	}
}

func loopback(r *http.Request) bool {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// clientIP trusts Cloudflare's header only from the local cloudflared connector.
func clientIP(r *http.Request) string {
	if ip := net.ParseIP(r.Header.Get("Cf-Connecting-Ip")); ip != nil && loopback(r) {
		return ip.String()
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

func secureRequest(r *http.Request) bool {
	return r.TLS != nil || (loopback(r) && r.Header.Get("X-Forwarded-Proto") == "https")
}

func trustedDashboardRemote(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	_, tailscaleV4, _ := net.ParseCIDR("100.64.0.0/10")
	_, tailscaleV6, _ := net.ParseCIDR("fd7a:115c:a1e0::/48")
	return tailscaleV4.Contains(ip) || tailscaleV6.Contains(ip)
}
