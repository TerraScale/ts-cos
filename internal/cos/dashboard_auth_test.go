package cos

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func authTestCluster(t *testing.T, ctx context.Context) ([]*Bus, []Config) {
	t.Helper()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	buses, configs := make([]*Bus, 3), make([]Config, 3)
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		cfg, node := busTestConfig(t, host, ports[i], ports[i+3], peers)
		bus, err := startBus(ctx, cfg, node, func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bus.Close)
		buses[i], configs[i] = bus, cfg
	}
	return buses, configs
}

func authTestAdmin(t *testing.T, home, user, password string, mode os.FileMode) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]string{"user": user, "bcrypt": string(hash)})
	if err := os.MkdirAll(filepath.Join(home, ".config/cos"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(home, ".config/cos/admin.json")
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil { // independent of the umask
		t.Fatal(err)
	}
}

type authTestClient struct {
	t      *testing.T
	client *http.Client
}

func (c authTestClient) do(server *httptest.Server, method, path string, form url.Values, cookie *http.Cookie, header ...string) (*http.Response, string) {
	c.t.Helper()
	req, _ := http.NewRequest(method, server.URL+path, strings.NewReader(form.Encode()))
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	response, err := c.client.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	return response, string(body)
}

func sessionOf(response *http.Response) *http.Cookie {
	for _, cookie := range response.Cookies() {
		if cookie.Name == sessionCookie && cookie.MaxAge > 0 {
			return cookie
		}
	}
	return nil
}

// TestAuthAcrossNodes signs in, changes the password and signs out on different
// nodes of a real three-node cluster, as cos.terrascale.tech fails over between hosts.
func TestAuthAcrossNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buses, configs := authTestCluster(t, ctx)
	authTestAdmin(t, configs[0].Home, "tester", "first-password-1", 0600)
	authTestAdmin(t, configs[1].Home, "public", "public-password-1", 0644)
	servers := make([]*httptest.Server, 3)
	auths := make([]*Auth, 3)
	for i, bus := range buses {
		js, err := bus.JetStream()
		if err != nil {
			t.Fatal(err)
		}
		auths[i] = NewAuth(configs[i].Home, js)
		go auths[i].Start(ctx)
		servers[i] = httptest.NewServer(auths[i].Protect(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			fmt.Fprint(rw, "dashboard for ", r.Context().Value(authUserKey{}))
		}), http.NotFoundHandler()))
		t.Cleanup(servers[i].Close)
	}
	deadline := time.Now().Add(60 * time.Second)
	for _, auth := range auths {
		for users, _, _ := auth.kv(); users == nil; users, _, _ = auth.kv() {
			if time.Now().After(deadline) {
				t.Fatal("auth buckets were not created")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	c := authTestClient{t, &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	a, b, d := servers[0], servers[1], servers[2]
	login := func(server *httptest.Server, user, password string, header ...string) (*http.Response, string) {
		return c.do(server, "POST", "/login", url.Values{"user": {user}, "password": {password}}, nil, header...)
	}
	// countFailure repeats the same wrong login until the server counts it.
	countFailure := func(server *httptest.Server, user string, header ...string) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			response, _ := login(server, user, "wrong-password-1", header...)
			if response.StatusCode == http.StatusUnauthorized {
				return
			}
			// A 503 is refused but not counted (lock state unreadable during a NATS reconnect); only a 401 counts.
			if response.StatusCode != http.StatusServiceUnavailable || time.Now().After(deadline) {
				t.Fatalf("wrong login for %s = %d, want 401", user, response.StatusCode)
			}
		}
	}

	t.Run("only the login page is public", func(t *testing.T) {
		if response, body := c.do(a, "GET", "/login", nil, nil); response.StatusCode != 200 || !strings.Contains(body, `type="password"`) {
			t.Fatalf("GET /login = %d", response.StatusCode)
		}
		if response, _ := c.do(a, "GET", "/", nil, nil, "Accept", "text/html"); response.StatusCode != http.StatusSeeOther || response.Header.Get("Location") != "/login" {
			t.Fatalf("GET / = %d %s", response.StatusCode, response.Header.Get("Location"))
		}
		for _, path := range []string{"/chat/events", "/status", "/password"} {
			if response, _ := c.do(a, "GET", path, nil, nil, "Datastar-Request", "true"); response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("GET %s = %d", path, response.StatusCode)
			}
		}
		if response, _ := c.do(a, "GET", "/", nil, &http.Cookie{Name: sessionCookie, Value: "tester.forged"}); response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("forged session = %d", response.StatusCode)
		}
	})

	var session *http.Cookie
	t.Run("a session from one node works on every node", func(t *testing.T) {
		if response, body := login(a, "tester", "wrong-password-1"); response.StatusCode != http.StatusUnauthorized || sessionOf(response) != nil || !strings.Contains(body, "Wrong username or password") {
			t.Fatalf("wrong password = %d", response.StatusCode)
		}
		if response, _ := login(b, "public", "public-password-1"); response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("admin.json without mode 0600: %d", response.StatusCode)
		}
		response, _ := login(a, "tester", "first-password-1")
		session = sessionOf(response)
		if response.StatusCode != http.StatusSeeOther || session == nil || !session.HttpOnly || session.Secure || session.SameSite != http.SameSiteLaxMode {
			t.Fatalf("login = %d, cookie %+v", response.StatusCode, session)
		}
		for _, server := range servers {
			if response, body := c.do(server, "GET", "/", nil, session); response.StatusCode != 200 || body != "dashboard for tester" {
				t.Fatalf("session on another node = %d %q", response.StatusCode, body)
			}
		}
		if response, _ := login(d, "tester", "first-password-1", "X-Forwarded-Proto", "https"); !sessionOf(response).Secure {
			t.Fatal("HTTPS session cookie is not Secure")
		}
	})

	t.Run("the deploy probe signs in like a browser until it ends", func(t *testing.T) {
		value, end, err := auths[2].ProbeSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
		probe := &http.Cookie{Name: sessionCookie, Value: value}
		if response, body := c.do(d, "GET", "/", nil, probe); response.StatusCode != 200 || body != "dashboard for cos-probe" {
			t.Fatalf("probe session = %d %q", response.StatusCode, body)
		}
		end()
		// Revocation reaches a lagging local replica within milliseconds; wait for it rather than read once.
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(100 * time.Millisecond) {
			response, _ := c.do(d, "GET", "/", nil, probe)
			if response.StatusCode == http.StatusUnauthorized {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the probe session outlived end")
			}
		}
		if response, _ := login(d, "cos-probe", ""); response.StatusCode != http.StatusUnauthorized {
			t.Fatal("cos-probe signed in with a password")
		}
	})

	t.Run("cross-site form posts are rejected", func(t *testing.T) {
		response, _ := login(a, "tester", "first-password-1", "Origin", "https://evil.example", "Sec-Fetch-Site", "cross-site")
		if response.StatusCode != http.StatusForbidden || sessionOf(response) != nil {
			t.Fatalf("cross-site login = %d", response.StatusCode)
		}
	})

	t.Run("a password change applies on every node and signs out other sessions", func(t *testing.T) {
		change := func(server *httptest.Server, cookie *http.Cookie, current, next, confirm string) (*http.Response, string) {
			return c.do(server, "POST", "/password", url.Values{"current": {current}, "new": {next}, "confirm": {confirm}}, cookie)
		}
		if response, _ := change(a, session, "first-password-1", "second-password-2", "different-password"); response.StatusCode != http.StatusBadRequest {
			t.Fatalf("mismatch = %d", response.StatusCode)
		}
		if response, _ := change(a, session, "first-password-1", "short", "short"); response.StatusCode != http.StatusBadRequest {
			t.Fatalf("short = %d", response.StatusCode)
		}
		if response, _ := change(a, session, "wrong-password-1", "second-password-2", "second-password-2"); response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("wrong current = %d", response.StatusCode)
		}
		response, body := change(a, session, "first-password-1", "second-password-2", "second-password-2")
		fresh := sessionOf(response)
		if response.StatusCode != 200 || fresh == nil || !strings.Contains(body, "Password changed") {
			t.Fatalf("change = %d %q", response.StatusCode, body)
		}
		if response, _ := c.do(b, "GET", "/", nil, session); response.StatusCode != http.StatusUnauthorized {
			t.Fatal("the session from before the change still works")
		}
		if response, _ := c.do(d, "GET", "/", nil, fresh); response.StatusCode != 200 {
			t.Fatal("the changing browser was signed out")
		}
		if response, _ := login(b, "tester", "first-password-1"); response.StatusCode != http.StatusUnauthorized {
			t.Fatal("old password still works on another node")
		}
		response, _ = login(b, "tester", "second-password-2")
		session = sessionOf(response)
		if session == nil {
			t.Fatal("new password rejected on another node")
		}
	})

	t.Run("admin.json seeds only once", func(t *testing.T) {
		js, _ := buses[2].JetStream()
		restarted := NewAuth(configs[0].Home, js)
		restarted.Start(ctx)
		if response, _ := login(d, "tester", "first-password-1"); response.StatusCode != http.StatusUnauthorized {
			t.Fatal("a restart re-seeded the old admin.json password")
		}
	})

	t.Run("logout on one node ends the session on every node", func(t *testing.T) {
		if response, _ := c.do(b, "POST", "/logout", url.Values{}, session); response.StatusCode != http.StatusSeeOther || response.Header.Get("Location") != "/login" {
			t.Fatalf("logout = %d", response.StatusCode)
		}
		if response, _ := c.do(a, "GET", "/", nil, session); response.StatusCode != http.StatusUnauthorized {
			t.Fatal("session survived logout on another node")
		}
	})

	t.Run("failures lock the account on every node, even with the right password", func(t *testing.T) {
		hash, err := bcrypt.GenerateFromPassword([]byte("locked-password-4"), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		users, _, _ := auths[0].kv()
		value, _ := json.Marshal(authUser{Bcrypt: string(hash)})
		if _, err := users.Put(ctx, "user.locktest", value); err != nil {
			t.Fatal(err)
		}
		for i, server := range []*httptest.Server{a, b, d, a, b} {
			countFailure(server, "locktest", "Cf-Connecting-Ip", fmt.Sprintf("198.51.100.%d", i+1))
		}
		for i, server := range servers {
			response, body := login(server, "locktest", "locked-password-4", "Cf-Connecting-Ip", fmt.Sprintf("198.51.100.%d", 50+i))
			if response.StatusCode != http.StatusTooManyRequests || sessionOf(response) != nil || !strings.Contains(body, "Too many failed attempts") {
				t.Fatalf("locked login = %d %q", response.StatusCode, body)
			}
		}
	})

	t.Run("failures from one address lock it on every node, for any username", func(t *testing.T) {
		authTestUser(t, auths[0], "carol", "ip-password-3")
		for i, server := range []*httptest.Server{a, b, d, a, b} {
			countFailure(server, fmt.Sprintf("nobody-%d", i+1), "Cf-Connecting-Ip", "203.0.113.77")
		}
		for _, server := range servers {
			response, body := login(server, "carol", "ip-password-3", "Cf-Connecting-Ip", "203.0.113.77")
			if response.StatusCode != http.StatusTooManyRequests || sessionOf(response) != nil || !strings.Contains(body, "Too many failed attempts") {
				t.Fatalf("locked address = %d %q", response.StatusCode, body)
			}
		}
		if response, _ := login(d, "carol", "ip-password-3", "Cf-Connecting-Ip", "203.0.113.78"); response.StatusCode != http.StatusSeeOther {
			t.Fatalf("another address = %d", response.StatusCode)
		}
	})

	t.Run("no lost increments under concurrency", func(t *testing.T) {
		const key = "acct.dave-concurrent"
		var wg sync.WaitGroup
		for i := range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				auth := auths[i%3]
				_, _, lockouts := auth.kv()
				auth.fail(ctx, lockouts, key)
			}()
		}
		wg.Wait()
		// The final read can hit the same reconnect blip, so poll until the leader answers.
		var value []byte
		var found bool
		var err error
		var count lockoutCount
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
			value, _, found, err = leaderGet(ctx, auths[1].nc, lockoutBucket, key)
			if err == nil && found {
				break
			}
		}
		if err == nil && found {
			err = json.Unmarshal(value, &count)
		}
		if err != nil || count.Failures != 10 {
			t.Fatalf("%s = %q found %v err %v", key, value, found, err)
		}
	})

	t.Run("leaderGet reads the stream leader on every node", func(t *testing.T) {
		_, _, lockouts := auths[0].kv()
		if _, err := lockouts.Put(ctx, "ip.test", []byte(`{"failures":1}`)); err != nil {
			t.Fatal(err)
		}
		value, revision, found, err := leaderGet(ctx, auths[1].nc, lockoutBucket, "ip.test")
		if err != nil || !found || revision == 0 || string(value) != `{"failures":1}` {
			t.Fatalf("found key = %q %d %v %v", value, revision, found, err)
		}
		if _, _, found, err := leaderGet(ctx, auths[2].nc, lockoutBucket, "ip.missing"); err != nil || found {
			t.Fatalf("missing key found = %v %v", found, err)
		}
		if err := lockouts.Delete(ctx, "ip.test"); err != nil {
			t.Fatal(err)
		}
		if _, _, found, err := leaderGet(ctx, auths[2].nc, lockoutBucket, "ip.test"); err != nil || found {
			t.Fatalf("deleted key found = %v %v", found, err)
		}
	})
}

// TestForgedSessionsCannotStampedeLeader sends forged cookies past the leader-read budget from one
// address, then checks that a real session from that address still works on its own node.
func TestForgedSessionsCannotStampedeLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buses, configs := authTestCluster(t, ctx)
	authTestAdmin(t, configs[0].Home, "tester", "first-password-1", 0600)
	js, err := buses[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	auth := NewAuth(configs[0].Home, js)
	go auth.Start(ctx)
	server := httptest.NewServer(auth.Protect(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		fmt.Fprint(rw, "dashboard for ", r.Context().Value(authUserKey{}))
	}), http.NotFoundHandler()))
	t.Cleanup(server.Close)
	deadline := time.Now().Add(40 * time.Second)
	for users, _, _ := auth.kv(); users == nil; users, _, _ = auth.kv() {
		if time.Now().After(deadline) {
			t.Fatal("auth buckets were not created")
		}
		time.Sleep(100 * time.Millisecond)
	}
	c := authTestClient{t, &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	header := []string{"Cf-Connecting-Ip", "203.0.113.40"}
	for i := range 31 {
		raw := make([]byte, 32)
		rand.Read(raw)
		forged := &http.Cookie{Name: sessionCookie, Value: "tester." + base64.RawURLEncoding.EncodeToString(raw)}
		response, _ := c.do(server, "GET", "/", nil, forged, header...)
		if response.StatusCode != http.StatusUnauthorized && response.StatusCode != http.StatusSeeOther {
			t.Fatalf("forged session %d = %d, want 401 or 303", i+1, response.StatusCode)
		}
	}
	response, _ := c.do(server, "POST", "/login", url.Values{"user": {"tester"}, "password": {"first-password-1"}}, nil, header...)
	session := sessionOf(response)
	if session == nil {
		t.Fatalf("login after forged cookies = %d", response.StatusCode)
	}
	if response, body := c.do(server, "GET", "/", nil, session, header...); response.StatusCode != 200 || body != "dashboard for tester" {
		t.Fatalf("real session after forged cookies = %d %q", response.StatusCode, body)
	}
}

// Only failed sign-ins count toward a lockout: signing in again and again from one address is never refused.
func TestRepeatedSuccessfulLoginsAreNotLimited(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buses, configs := authTestCluster(t, ctx)
	authTestAdmin(t, configs[0].Home, "tester", "first-password-1", 0600)
	js, err := buses[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	auth := NewAuth(configs[0].Home, js)
	go auth.Start(ctx)
	server := httptest.NewServer(auth.Protect(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		fmt.Fprint(rw, "dashboard for ", r.Context().Value(authUserKey{}))
	}), http.NotFoundHandler()))
	t.Cleanup(server.Close)
	deadline := time.Now().Add(40 * time.Second)
	for users, _, _ := auth.kv(); users == nil; users, _, _ = auth.kv() {
		if time.Now().After(deadline) {
			t.Fatal("auth buckets were not created")
		}
		time.Sleep(100 * time.Millisecond)
	}
	c := authTestClient{t, &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	header := []string{"Cf-Connecting-Ip", "203.0.113.41"}
	for i := range 25 {
		response, _ := c.do(server, "POST", "/login", url.Values{"user": {"tester"}, "password": {"first-password-1"}}, nil, header...)
		if response.StatusCode != http.StatusSeeOther || response.Header.Get("Location") != "/" || sessionOf(response) == nil {
			t.Fatalf("sign-in %d: HTTP %d", i+1, response.StatusCode)
		}
	}
}

// authTestUser adds a seeded user straight to KV, for tests that need more than the admin.
func authTestUser(t *testing.T, auth *Auth, user, password string) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		t.Fatal(err)
	}
	users, _, _ := auth.kv()
	value, _ := json.Marshal(authUser{Bcrypt: string(hash)})
	if _, err := users.Create(context.Background(), "user."+user, value); err != nil {
		t.Fatal(err)
	}
}

func TestLockoutIPKey(t *testing.T) {
	for ip, want := range map[string]string{
		"198.51.100.7": "ip.198.51.100.7",
		"fd7a:115c::1": "ip.fd7a_115c__1",
		"fe80::1%eth0": "ip.fe80__1_eth0",
		"":             "ip.unknown",
		"..":           "ip.unknown",
	} {
		if got := lockoutIPKey(ip); got != want {
			t.Errorf("lockoutIPKey(%q) = %q, want %q", ip, got, want)
		}
	}
}

func TestAuthStartingAsksToRetry(t *testing.T) {
	h := NewAuth(t.TempDir(), nil).Protect(http.NotFoundHandler(), http.NotFoundHandler())
	for _, path := range []string{"/", "/chat/events", "/password"} {
		req := httptest.NewRequest("GET", path, nil)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("Accept", "text/html")
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: "tester.token"})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "2" {
			t.Fatalf("GET %s while starting = %d, Retry-After %q", path, rec.Code, rec.Header().Get("Retry-After"))
		}
	}
	rec, req := httptest.NewRecorder(), httptest.NewRequest("GET", "/login", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /login while starting = %d", rec.Code)
	}
}

func TestDashboardShutdownJoinsHandlersBeforeReturn(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	web := NewWeb(store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	handlerDone := make(chan struct{})
	releaseHandler := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		web.routes().ServeHTTP(w, r)
		close(handlerDone)
		<-releaseHandler
	}), BaseContext: func(net.Listener) context.Context { return ctx }}
	t.Cleanup(func() {
		select {
		case <-releaseHandler:
		default:
			close(releaseHandler)
		}
		cancel()
		_ = server.Close()
		_ = store.Close()
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- serveDashboard(ctx, server, listener, cancel) }()
	client := &http.Client{Timeout: 3 * time.Second}
	req, _ := http.NewRequest("GET", "http://"+listener.Addr().String()+"/chat/events", nil)
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 1)
	if _, err := response.Body.Read(first); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not receive daemon cancellation")
	}
	select {
	case <-serveDone:
		t.Fatal("Serve returned before the active handler finished")
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := store.Revision(context.Background()); err != nil {
		t.Fatal("Store was unavailable while handler was active", err)
	}
	close(releaseHandler)
	select {
	case err := <-serveDone:
		if err != nil && err != http.ErrServerClosed {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server shutdown did not join SSE handler")
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatalf("read terminated SSE response: %v", err)
	}
	_ = response.Body.Close()
	web.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTrustedDashboardRemote(t *testing.T) {
	if trustedDashboardRemote("not-an-ip") || trustedDashboardRemote("") {
		t.Fatal("accepted invalid remote address")
	}
}
