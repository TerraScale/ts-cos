package cos

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/crypto/bcrypt"
)

// Access checks sign in as a throwaway "e2e-<random>" user created in the auth KV and
// deleted afterwards, so the user's own credential is never read, used or locked. Brute force runs
// only against tailnet URLs: the public URL shares the homelab's address with the user's browser.

const e2ePublicURL = "https://cos.terrascale.tech"

var e2eHTTP = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

type e2eUser struct {
	name, password string
	user           User
	users          jetstream.KeyValue
	sessions       jetstream.KeyValue
	lockouts       jetstream.KeyValue
}

// tempUser creates a throwaway account; cleanup deletes it with its sessions and the locks it caused (its account and the runner's addresses).
func (e *E2EEnv) tempUser(ctx context.Context, bus *Bus) (*e2eUser, func(), error) {
	js, err := jetstream.New(bus.conn)
	if err != nil {
		return nil, nil, err
	}
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	u := &e2eUser{}
	if u.users, err = js.KeyValue(call, authBucket); err != nil {
		return nil, nil, fmt.Errorf("auth store: %w", err)
	}
	if u.sessions, err = js.KeyValue(call, sessionBucket); err != nil {
		return nil, nil, fmt.Errorf("session store: %w", err)
	}
	if u.lockouts, err = js.KeyValue(call, lockoutBucket); err != nil {
		return nil, nil, fmt.Errorf("lockout store: %w", err)
	}
	sweepE2EUsers(call, u)
	raw := make([]byte, 18)
	rand.Read(raw)
	u.name, u.password = "e2e-"+hex.EncodeToString(raw[:4]), base64.RawURLEncoding.EncodeToString(raw[4:])
	hash, err := bcrypt.GenerateFromPassword([]byte(u.password), bcrypt.MinCost)
	if err != nil {
		return nil, nil, err
	}
	if u.user, err = createUser(call, u.users, u.name, roleMember, string(hash)); err != nil {
		return nil, nil, fmt.Errorf("create temporary user: %w", err)
	}
	return u, func() { deleteE2EUser(u, u.user) }, nil
}

// deleteE2EUser runs on a fresh context so a cancelled run still removes the account.
func deleteE2EUser(u *e2eUser, user User) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = deleteUser(ctx, u.users, u.sessions, u.lockouts, user)
	// Brute-force checks lock the runner's own address too, which would refuse every later check's login.
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if p, ok := a.(*net.IPNet); ok {
				_ = u.lockouts.Delete(ctx, lockoutIPKey(p.IP.String()))
			}
		}
	}
}

// sweepE2EUsers removes accounts a crashed earlier run left behind (older than 10 minutes).
func sweepE2EUsers(ctx context.Context, u *e2eUser) {
	keys, err := u.users.ListKeys(ctx)
	if err != nil {
		return
	}
	for key := range keys.Keys() {
		name, ok := strings.CutPrefix(key, "name.")
		if !ok || !strings.HasPrefix(name, "e2e-") {
			continue
		}
		if entry, err := u.users.Get(ctx, key); err == nil && ulidPattern.Match(entry.Value()) && time.Since(entry.Created()) > 10*time.Minute {
			deleteE2EUser(u, User{ID: string(entry.Value()), Name: name})
		}
	}
}

// nodeURL is a host's dashboard on its tailnet address; plain host names resolve to the LAN,
// which the dashboard refuses.
func (e *E2EEnv) nodeURL(ctx context.Context, host string) (string, error) {
	addrs, err := e.hostAddrs(ctx, host)
	if err != nil {
		return "", err
	}
	for _, a := range addrs {
		if a.Is4() && netip.MustParsePrefix("100.64.0.0/10").Contains(a) {
			return "http://" + a.String() + ":8790", nil
		}
	}
	return "", fmt.Errorf("%s has no tailnet IPv4", host)
}

func (e *E2EEnv) nodeURLs(ctx context.Context) ([]string, error) {
	urls := make([]string, len(e2eHosts))
	for i, h := range e2eHosts {
		var err error
		if urls[i], err = e.nodeURL(ctx, h); err != nil {
			return nil, err
		}
	}
	return urls, nil
}

// e2eRequest sends one request without following redirects; it returns the status, the
// Location, the new session cookie (if any) and the body. headers are name, value pairs.
func e2eRequest(ctx context.Context, method, target, cookie string, form url.Values, headers ...string) (int, string, string, string, error) {
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return 0, "", "", "", err
	}
	req.Header.Set("Accept", "text/html")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie})
	}
	res, err := e2eHTTP.Do(req)
	if err != nil {
		return 0, "", "", "", fmt.Errorf("%s %s: %w", method, target, errors.Unwrap(err))
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	session := ""
	for _, c := range res.Cookies() {
		if c.Name == sessionCookie && c.MaxAge >= 0 {
			session = c.Value
		}
	}
	return res.StatusCode, res.Header.Get("Location"), session, string(data), nil
}

// signIn posts the login form and returns the session cookie; status is the login response's.
func signIn(ctx context.Context, base, user, password string) (string, int, string, error) {
	status, location, cookie, body, err := e2eRequest(ctx, "POST", base+"/login", "", url.Values{"user": {user}, "password": {password}})
	if err == nil && status == http.StatusSeeOther && (location != "/" || cookie == "") {
		err = fmt.Errorf("login on %s redirected to %q without a session", base, location)
	}
	return cookie, status, body, err
}

// signedIn reports whether cookie opens the dashboard on base (200) rather than /login.
func signedIn(ctx context.Context, base, cookie string) (bool, error) {
	status, location, _, _, err := e2eRequest(ctx, "GET", base+"/", cookie, nil)
	if err != nil {
		return false, err
	}
	switch {
	case status == http.StatusOK:
		return true, nil
	case status == http.StatusSeeOther && location == "/login", status == http.StatusUnauthorized:
		return false, nil
	}
	return false, fmt.Errorf("GET %s/: unexpected HTTP %d (Location %q)", base, status, location)
}

// accessCheck wraps a check with the bus, node URLs and a throwaway user.
func accessCheck(run func(context.Context, []string, *e2eUser) (string, error)) func(context.Context, *E2EEnv) (string, error) {
	return func(ctx context.Context, e *E2EEnv) (string, error) {
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		urls, err := e.nodeURLs(ctx)
		if err != nil {
			return "", err
		}
		u, cleanup, err := e.tempUser(ctx, bus)
		if err != nil {
			return "", err
		}
		defer cleanup()
		return run(ctx, urls, u)
	}
}

// e2eSession is the run's shared throwaway user and its session cookie per base URL.
type e2eSession struct {
	user    *e2eUser
	cleanup func()
	cookies map[string]string
}

// session returns the run's shared session on base. The shared user signs in once per base, so the
// dashboard's login limit (10 POSTs a minute per address and node) is not spent by every check.
func (e *E2EEnv) session(ctx context.Context, base string) (string, error) {
	e.sessMu.Lock()
	defer e.sessMu.Unlock()
	if e.sess == nil {
		if err := e.openSession(ctx); err != nil {
			return "", err
		}
	}
	if cookie, ok := e.sess.cookies[base]; ok {
		return cookie, nil
	}
	u := e.sess.user
	cookie, status, _, err := signIn(ctx, base, u.name, u.password)
	if status == http.StatusTooManyRequests {
		// Wait the minute out once; the login limit is per address and node.
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Minute):
		}
		cookie, status, _, err = signIn(ctx, base, u.name, u.password)
	}
	if err != nil || status != http.StatusSeeOther {
		return "", fmt.Errorf("login on %s: HTTP %d %v", base, status, err)
	}
	e.sess.cookies[base] = cookie
	return cookie, nil
}

// ownerSession signs the first admin in over NATS, with no password, for checks that read owner pages (node T3
// data); end deletes the session and closes the connection. The session works on every node.
func (e *E2EEnv) ownerSession(ctx context.Context) (cookie string, end func(), err error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", nil, err
	}
	js, err := bus.JetStream()
	if err != nil {
		bus.Close()
		return "", nil, err
	}
	cookie, endSession, err := ownerProbeCookie(ctx, js, bus.conn)
	if err != nil {
		bus.Close()
		return "", nil, err
	}
	return cookie, func() { endSession(); bus.Close() }, nil
}

// openSession creates the run's shared user, unless a test replaced newUser. Callers hold sessMu.
func (e *E2EEnv) openSession(ctx context.Context) error {
	newUser := e.newUser
	if newUser == nil {
		newUser = e.bindTempUser
	}
	u, cleanup, err := newUser(ctx)
	if err != nil {
		return err
	}
	e.sess = &e2eSession{user: u, cleanup: cleanup, cookies: map[string]string{}}
	return nil
}

// bindTempUser creates a throwaway user on its own bus; its cleanup deletes the user before the bus closes.
func (e *E2EEnv) bindTempUser(ctx context.Context) (*e2eUser, func(), error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return nil, nil, err
	}
	u, cleanup, err := e.tempUser(ctx, bus)
	if err != nil {
		bus.Close()
		return nil, nil, err
	}
	return u, func() { cleanup(); bus.Close() }, nil
}

// closeSession deletes the run's shared user with its sessions; runE2E defers it so a cancelled run still cleans up.
func (e *E2EEnv) closeSession() {
	e.sessMu.Lock()
	defer e.sessMu.Unlock()
	if e.sess != nil {
		e.sess.cleanup()
		e.sess = nil
	}
}

// nodesCheck wraps a check that needs every node's dashboard URL.
func nodesCheck(run func(context.Context, *E2EEnv, []string) (string, error)) func(context.Context, *E2EEnv) (string, error) {
	return func(ctx context.Context, e *E2EEnv) (string, error) {
		urls, err := e.nodeURLs(ctx)
		if err != nil {
			return "", err
		}
		return run(ctx, e, urls)
	}
}

// loginWorks checks the form, the run's sign-in and the signed-in dashboard on base.
func loginWorks(ctx context.Context, e *E2EEnv, base string) error {
	status, _, _, body, err := e2eRequest(ctx, "GET", base+"/login", "", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK || !strings.Contains(body, `action="/login"`) {
		return fmt.Errorf("GET %s/login: HTTP %d without the login form", base, status)
	}
	cookie, err := e.session(ctx, base)
	if err != nil {
		return err
	}
	if ok, err := signedIn(ctx, base, cookie); err != nil || !ok {
		return fmt.Errorf("session from %s does not open its dashboard: %v", base, err)
	}
	return nil
}

func checkLoginTailnet(ctx context.Context, e *E2EEnv, urls []string) (string, error) {
	for _, base := range urls {
		if err := loginWorks(ctx, e, base); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("login form, sign-in and dashboard work on %v", e2eHosts), nil
}

func checkLoginPublic(ctx context.Context, e *E2EEnv) (string, error) {
	if err := loginWorks(ctx, e, e2ePublicURL); err != nil {
		return "", err
	}
	return "login form, sign-in and dashboard work on " + e2ePublicURL, nil
}

func checkWrongPassword(ctx context.Context, urls []string, u *e2eUser) (string, error) {
	base := urls[2]
	if ok, err := signedIn(ctx, base, ""); err != nil || ok {
		return "", fmt.Errorf("dashboard without a session: signed in=%t %v", ok, err)
	}
	_, wrong, wrongBody, err := signIn(ctx, base, u.name, u.password+"x")
	if err != nil {
		return "", err
	}
	_, unknown, unknownBody, err := signIn(ctx, base, u.name+"x", u.password)
	if err != nil {
		return "", err
	}
	const notice = "Wrong username or password."
	if wrong != http.StatusUnauthorized || unknown != http.StatusUnauthorized || !strings.Contains(wrongBody, notice) || !strings.Contains(unknownBody, notice) {
		return "", fmt.Errorf("wrong password HTTP %d, unknown user HTTP %d; both must be 401 with the same notice", wrong, unknown)
	}
	return "no session redirects to /login; wrong password and unknown user both get 401 with one notice", nil
}

func checkLockout(ctx context.Context, urls []string, u *e2eUser) (string, error) {
	for i := range lockAfter {
		if _, status, _, err := signIn(ctx, urls[i%len(urls)], u.name, "wrong-password"); err != nil || status != http.StatusUnauthorized {
			return "", fmt.Errorf("failed attempt %d: HTTP %d %v", i+1, status, err)
		}
	}
	// The right password on another node is refused: the lock is cluster-wide.
	base := urls[lockAfter%len(urls)]
	_, status, body, err := signIn(ctx, base, u.name, u.password)
	if err != nil {
		return "", err
	}
	if status != http.StatusTooManyRequests || !strings.Contains(body, "Too many failed attempts") {
		return "", fmt.Errorf("after %d failures the right password on %s got HTTP %d, want 429 lockout", lockAfter, base, status)
	}
	return fmt.Sprintf("%d failures across nodes lock the account; the right password on %s is refused with 429", lockAfter, base), nil
}

func checkSessionCrossNode(ctx context.Context, e *E2EEnv, urls []string) (string, error) {
	cookie, err := e.session(ctx, urls[1])
	if err != nil {
		return "", err
	}
	for _, base := range append(urls, e2ePublicURL) {
		if ok, err := signedIn(ctx, base, cookie); err != nil || !ok {
			return "", fmt.Errorf("session from %s not accepted on %s: %v", e2eHosts[1], base, err)
		}
	}
	return fmt.Sprintf("a session from %s opens every node and %s", e2eHosts[1], e2ePublicURL), nil
}

func checkLogout(ctx context.Context, urls []string, u *e2eUser) (string, error) {
	cookie, status, _, err := signIn(ctx, urls[0], u.name, u.password)
	if err != nil || status != http.StatusSeeOther {
		return "", fmt.Errorf("login on %s: HTTP %d %v", urls[0], status, err)
	}
	status, location, _, _, err := e2eRequest(ctx, "POST", urls[1]+"/logout", cookie, url.Values{})
	if err != nil || status != http.StatusSeeOther || location != "/login" {
		return "", fmt.Errorf("logout on %s: HTTP %d to %q %v", urls[1], status, location, err)
	}
	if ok, err := signedIn(ctx, urls[2], cookie); err != nil || ok {
		return "", fmt.Errorf("session still open on %s after logout on %s: %v", urls[2], urls[1], err)
	}
	return fmt.Sprintf("logout on %s ends the session on %s", e2eHosts[1], e2eHosts[2]), nil
}

func checkPasswordChange(ctx context.Context, urls []string, u *e2eUser) (string, error) {
	other, status, _, err := signIn(ctx, urls[1], u.name, u.password)
	if err != nil || status != http.StatusSeeOther {
		return "", fmt.Errorf("login on %s: HTTP %d %v", urls[1], status, err)
	}
	cookie, status, _, err := signIn(ctx, urls[2], u.name, u.password)
	if err != nil || status != http.StatusSeeOther {
		return "", fmt.Errorf("login on %s: HTTP %d %v", urls[2], status, err)
	}
	next := u.password + "-changed"
	status, _, _, body, err := e2eRequest(ctx, "POST", urls[2]+"/password", cookie, url.Values{"current": {u.password}, "new": {next}, "confirm": {next}})
	if err != nil || status != http.StatusOK || !strings.Contains(body, "Password changed") {
		return "", fmt.Errorf("password change on %s: HTTP %d %v", urls[2], status, err)
	}
	if _, status, _, err := signIn(ctx, urls[0], u.name, next); err != nil || status != http.StatusSeeOther {
		return "", fmt.Errorf("new password refused on %s: HTTP %d %v", urls[0], status, err)
	}
	if _, status, _, err := signIn(ctx, urls[1], u.name, u.password); err != nil || status != http.StatusUnauthorized {
		return "", fmt.Errorf("old password on %s: HTTP %d, want 401 %v", urls[1], status, err)
	}
	if ok, err := signedIn(ctx, urls[0], other); err != nil || ok {
		return "", fmt.Errorf("other session still open after the password change: %v", err)
	}
	return fmt.Sprintf("password changed on %s: the new one works on %s, the old one fails on %s, other sessions end", e2eHosts[2], e2eHosts[0], e2eHosts[1]), nil
}
