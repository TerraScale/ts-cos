package cos

import (
	"bytes"
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
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/crypto/bcrypt"
)

// Users, sessions and lockouts live in three-replica JetStream KV, so every node
// accepts the same logins and sessions and a password change applies everywhere.
const (
	authBucket           = "cos_auth"
	sessionBucket        = "cos_sessions"
	lockoutBucket        = "cos_lockout"
	sessionTTL           = 7 * 24 * time.Hour
	sessionCookie        = "cos_session"
	lockAfter            = 5                // failures that lock an account or a client address
	lockFor              = 15 * time.Minute // also how long idle failures are remembered
	loginPerMinute       = 10               // attempts per client address on one node
	sessionMissPerMinute = 30               // leader reads for unknown session cookies per client address on one node
	passwordMin          = 12
	passwordMax          = 72 // bcrypt's limit
	bcryptCost           = 12
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

type lockoutCount struct {
	Failures int `json:"failures"`
}

type authUserKey struct{}

type Auth struct {
	home string
	js   jetstream.JetStream
	nc   *nats.Conn // reads the stream leader, see leaderGet

	mu       sync.Mutex
	users    jetstream.KeyValue // nil until the cluster can store replicated buckets
	sessions jetstream.KeyValue
	lockouts jetstream.KeyValue
	limit    limiter
}

func NewAuth(home string, js jetstream.JetStream) *Auth {
	a := &Auth{home: home, js: js}
	if js != nil {
		a.nc = js.Conn()
	}
	return a
}

// Start waits for cluster quorum, creates the buckets and seeds
// ~/.config/cos/admin.json once; a user already in KV is never overwritten.
func (a *Auth) Start(ctx context.Context) {
	for {
		users, sessions, lockouts, err := a.open(ctx)
		if err == nil {
			a.seed(ctx, users)
			a.mu.Lock()
			a.users, a.sessions, a.lockouts = users, sessions, lockouts
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

func (a *Auth) open(ctx context.Context) (jetstream.KeyValue, jetstream.KeyValue, jetstream.KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	users, err := a.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: authBucket, History: 1, Replicas: 3})
	if err != nil {
		return nil, nil, nil, err
	}
	sessions, err := a.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: sessionBucket, TTL: sessionTTL, History: 1, Replicas: 3})
	if err != nil {
		return nil, nil, nil, err
	}
	lockouts, err := a.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: lockoutBucket, TTL: lockFor, History: 1, Replicas: 3})
	return users, sessions, lockouts, err
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

func (a *Auth) kv() (jetstream.KeyValue, jetstream.KeyValue, jetstream.KeyValue) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.users, a.sessions, a.lockouts
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
		if _, sessions, _ := a.kv(); sessions == nil {
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
	_, sessions, _ := a.kv()
	cookie, err := r.Cookie(sessionCookie)
	if sessions == nil || err != nil {
		return ""
	}
	user, token, ok := strings.Cut(cookie.Value, ".")
	key, valid := sessionKey(user, token)
	if !ok || !valid {
		return ""
	}
	// The leader decides validity so logout and password change revoke at once; a local hit
	// still signs in while the session stream re-elects; a local miss (fresh session on a
	// lagging node, or a forged cookie) is capped per address before it may reach the leader.
	_, err = sessions.Get(r.Context(), key)
	local := err == nil
	if !local && !a.limit.allow("session-miss:"+clientIP(r), sessionMissPerMinute) {
		return ""
	}
	_, _, found, err := leaderGet(r.Context(), a.nc, sessionBucket, key)
	if err != nil {
		if !local {
			return ""
		}
		return user
	}
	if !found {
		return ""
	}
	return user
}

// untilRevoked retries op every 100 ms for up to 5 seconds and reports whether it succeeded.
func untilRevoked(op func() bool) bool {
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if op() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
	}
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
	_, sessions, _ := a.kv()
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
	users, sessions, lockouts := a.kv()
	if users == nil {
		renderAuth(rw, r, http.StatusServiceUnavailable, loginPage("COS is starting. Try again in a minute."))
		return
	}
	user := r.PostFormValue("user")
	_, notice, status := a.verify(r.Context(), lockouts, user, r.PostFormValue("password"), clientIP(r))
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

// verify checks a password against the account and client-address lockouts; notice is empty on success.
// Both locks are checked before the password, so the right password is refused while locked.
func (a *Auth) verify(ctx context.Context, lockouts jetstream.KeyValue, user, password, ip string) (uint64, string, int) {
	const wrong = "Wrong username or password."
	const locked = "Too many failed attempts. Try again in 15 minutes."
	const unreadable = "COS cannot read its users right now. Try again."
	ipKey := lockoutIPKey(ip)
	keys := []string{ipKey}
	valid := authUserPattern.MatchString(user)
	if valid {
		keys = append(keys, "acct."+user)
	}
	for _, key := range keys {
		failures, err := a.failures(ctx, key)
		if err != nil {
			return 0, unreadable, http.StatusServiceUnavailable
		}
		if failures >= lockAfter {
			return 0, locked, http.StatusTooManyRequests
		}
	}
	if !valid {
		_ = bcrypt.CompareHashAndPassword(dummyHash(), []byte(password))
		a.fail(ctx, lockouts, ipKey)
		return 0, wrong, http.StatusUnauthorized
	}
	acctKey := "acct." + user
	value, revision, found, err := leaderGet(ctx, a.nc, authBucket, "user."+user)
	var stored authUser
	if err == nil && found {
		err = json.Unmarshal(value, &stored)
	}
	if err != nil {
		_ = bcrypt.CompareHashAndPassword(dummyHash(), []byte(password))
		return 0, unreadable, http.StatusServiceUnavailable
	}
	if !found {
		_ = bcrypt.CompareHashAndPassword(dummyHash(), []byte(password))
		a.fail(ctx, lockouts, ipKey)
		return 0, wrong, http.StatusUnauthorized
	}
	if len(password) > passwordMax || bcrypt.CompareHashAndPassword([]byte(stored.Bcrypt), []byte(password)) != nil {
		a.fail(ctx, lockouts, ipKey)
		a.fail(ctx, lockouts, acctKey)
		return 0, wrong, http.StatusUnauthorized
	}
	_ = lockouts.Delete(ctx, ipKey)
	_ = lockouts.Delete(ctx, acctKey)
	return revision, "", http.StatusOK
}

// lockoutIPKey keeps every client address a valid KV key (IPv6 colons and zones, empty input).
func lockoutIPKey(ip string) string {
	clean := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.' {
			return r
		}
		return '_'
	}, ip)
	if clean == "" || strings.HasPrefix(clean, ".") || strings.HasSuffix(clean, ".") || strings.Contains(clean, "..") {
		clean = "unknown"
	}
	return "ip." + clean
}

// failures reads the failure count under key from the stream leader.
func (a *Auth) failures(ctx context.Context, key string) (int, error) {
	value, _, found, err := leaderGet(ctx, a.nc, lockoutBucket, key)
	if err != nil || !found {
		return 0, err
	}
	var count lockoutCount
	err = json.Unmarshal(value, &count)
	return count.Failures, err
}

// fail adds one failure under key. The read comes from the stream leader and the write is a
// CAS, so failures from every node add up; a lost race retries.
// Retry through a short NATS reconnect so a failed login is never silently uncounted.
func (a *Auth) fail(ctx context.Context, lockouts jetstream.KeyValue, key string) {
	// Count the failure even if the client hangs up mid-request, so a send-then-disconnect loop cannot dodge the lockout.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for attempt := 1; ; attempt++ {
		value, revision, found, err := leaderGet(ctx, a.nc, lockoutBucket, key)
		var count lockoutCount
		if err == nil && found {
			err = json.Unmarshal(value, &count)
		}
		if err == nil {
			count.Failures++
			value, _ = json.Marshal(count)
			if found || revision > 0 { // revision is the DEL/PURGE marker's sequence when found is false; Create would race the tombstone.
				_, err = lockouts.Update(ctx, key, value, revision)
			} else {
				_, err = lockouts.Create(ctx, key, value)
			}
		}
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			slog.Error("COS failed-login count lost", "key", key)
			return
		case <-time.After(min(time.Duration(attempt)*10*time.Millisecond, 250*time.Millisecond)):
		}
	}
}

// leaderGet reads the latest value of key from the stream leader, which has every acknowledged
// write. A local KV Get can answer from a lagging replica, so a failure could be read stale.
func leaderGet(ctx context.Context, nc *nats.Conn, bucket, key string) (value []byte, revision uint64, found bool, err error) {
	// Like the JetStream API default: a request to a stalled stream leader gets no reply.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, _ := json.Marshal(map[string]string{"last_by_subj": "$KV." + bucket + "." + key})
	reply, err := nc.RequestWithContext(ctx, "$JS.API.STREAM.MSG.GET.KV_"+bucket, req)
	if err != nil {
		return nil, 0, false, err
	}
	var resp struct {
		Message *struct {
			Seq  uint64 `json:"seq"`
			Hdrs string `json:"hdrs"`
			Data string `json:"data"`
		} `json:"message"`
		Error *struct {
			Code    int    `json:"code"`
			ErrCode int    `json:"err_code"`
			Desc    string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return nil, 0, false, err
	}
	if resp.Error != nil {
		if resp.Error.Code == http.StatusNotFound || resp.Error.ErrCode == 10037 {
			return nil, 0, false, nil
		}
		return nil, 0, false, errors.New("stream message get: " + resp.Error.Desc)
	}
	if resp.Message == nil {
		return nil, 0, false, errors.New("stream message get: no message")
	}
	hdrs, err := base64.StdEncoding.DecodeString(resp.Message.Hdrs)
	if err != nil {
		return nil, 0, false, err
	}
	if bytes.Contains(hdrs, []byte("KV-Operation: DEL")) || bytes.Contains(hdrs, []byte("KV-Operation: PURGE")) {
		return nil, resp.Message.Seq, false, nil
	}
	value, err = base64.StdEncoding.DecodeString(resp.Message.Data)
	if err != nil {
		return nil, 0, false, err
	}
	return value, resp.Message.Seq, true, nil
}

func (a *Auth) logout(rw http.ResponseWriter, r *http.Request) {
	_, sessions, _ := a.kv()
	if cookie, err := r.Cookie(sessionCookie); err == nil && sessions != nil {
		user, token, _ := strings.Cut(cookie.Value, ".")
		if key, ok := sessionKey(user, token); ok && !untilRevoked(func() bool { return sessions.Delete(r.Context(), key) == nil }) {
			slog.Error("COS session revocation failed", "user", user)
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
	users, sessions, lockouts := a.kv()
	revision, notice, status := a.verify(r.Context(), lockouts, user, r.PostFormValue("current"), clientIP(r))
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
	if _, err := users.Update(r.Context(), "user."+user, value, revision); err != nil {
		renderAuth(rw, r, http.StatusConflict, passwordPage("The password changed elsewhere meanwhile. Reload and try again."))
		return
	}
	revoked := untilRevoked(func() bool {
		keys, err := sessions.ListKeysFiltered(r.Context(), user+".*")
		if err != nil {
			return false
		}
		ok := true
		for key := range keys.Keys() {
			if sessions.Delete(r.Context(), key) != nil {
				ok = false
			}
		}
		return ok
	})
	if !revoked {
		slog.Error("COS session revocation failed", "user", user)
		renderAuth(rw, r, http.StatusServiceUnavailable, passwordPage("Password changed, but other sessions could not be signed out. Try again in a minute."))
		return
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
