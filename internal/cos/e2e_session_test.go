package cos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

// sharedEnv is a run whose shared user needs no bus; cleanups counts how often that user is deleted.
func sharedEnv(cleanups *atomic.Int32) *E2EEnv {
	return &E2EEnv{newUser: func(context.Context) (*e2eUser, func(), error) {
		return &e2eUser{name: "e2e-test", password: "pw"}, func() { cleanups.Add(1) }, nil
	}}
}

// loginServer accepts e2e-test / pw and counts every POST /login it receives.
func loginServer(t *testing.T, logins *atomic.Int32) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/login" {
			http.NotFound(rw, r)
			return
		}
		logins.Add(1)
		if r.FormValue("user") != "e2e-test" || r.FormValue("password") != "pw" {
			http.Error(rw, "Wrong username or password.", http.StatusUnauthorized)
			return
		}
		http.SetCookie(rw, &http.Cookie{Name: sessionCookie, Value: "e2e-test.token"})
		http.Redirect(rw, r, "/", http.StatusSeeOther)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestSessionLogsInOncePerBase(t *testing.T) {
	var logins atomic.Int32
	base := loginServer(t, &logins)
	env := sharedEnv(new(atomic.Int32))
	t.Cleanup(env.closeSession)

	cookies := make([]string, 8)
	errs := make([]error, 8)
	var wg sync.WaitGroup
	for i := range cookies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cookies[i], errs[i] = env.session(t.Context(), base)
		}()
	}
	wg.Wait()
	for i := range cookies {
		if errs[i] != nil || cookies[i] == "" || cookies[i] != cookies[0] {
			t.Fatalf("caller %d got cookie %q, error %v; want the same cookie as caller 0 (%q)", i, cookies[i], errs[i], cookies[0])
		}
	}
	if n := logins.Load(); n != 1 {
		t.Fatalf("logins = %d for 8 concurrent callers on one base, want 1", n)
	}
}

func TestSessionOneLoginPerBaseAndCloseDeletesUser(t *testing.T) {
	var logins1, logins2 atomic.Int32
	a, b := loginServer(t, &logins1), loginServer(t, &logins2)
	var cleanups atomic.Int32
	env := sharedEnv(&cleanups)
	for _, base := range []string{a, b, a, b} {
		if _, err := env.session(t.Context(), base); err != nil {
			t.Fatalf("session on %s: %v", base, err)
		}
	}
	if n1, n2 := logins1.Load(), logins2.Load(); n1 != 1 || n2 != 1 {
		t.Fatalf("logins per base = %d and %d, want 1 and 1", n1, n2)
	}
	env.closeSession()
	env.closeSession()
	if n := cleanups.Load(); n != 1 {
		t.Fatalf("shared user deleted %d times, want once", n)
	}
	if env.sess != nil {
		t.Fatal("session kept after closeSession")
	}
}

func TestSessionRefusedLoginIsNotCached(t *testing.T) {
	var logins atomic.Int32
	base := loginServer(t, &logins)
	env := &E2EEnv{newUser: func(context.Context) (*e2eUser, func(), error) {
		return &e2eUser{name: "e2e-test", password: "wrong"}, func() {}, nil
	}}
	t.Cleanup(env.closeSession)
	if _, err := env.session(t.Context(), base); err == nil {
		t.Fatal("a refused login must be an error")
	}
	if _, ok := env.sess.cookies[base]; ok {
		t.Fatal("a refused login was cached")
	}
}
