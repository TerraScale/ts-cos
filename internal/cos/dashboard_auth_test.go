package cos

import (
	"context"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDashboardAuthProtectsEveryRoute(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.AddNote(context.Background(), "direction", "PRIVATE-DASHBOARD-STORY"); err != nil {
		t.Fatal(err)
	}
	h := NewWeb(store, "test-secret").Handler()
	for _, path := range []string{"/", "/status", "/events"} {
		for _, credentials := range [][2]string{{"", ""}, {dashboardUser, "wrong"}} {
			req := httptest.NewRequest("GET", path, nil)
			req.RemoteAddr = "127.0.0.1:1234"
			if credentials[0] != "" {
				req.SetBasicAuth(credentials[0], credentials[1])
			}
			rw := httptest.NewRecorder()
			h.ServeHTTP(rw, req)
			if rw.Code != http.StatusUnauthorized || rw.Header().Get("WWW-Authenticate") == "" || rw.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("%s auth response: status=%d headers=%v", path, rw.Code, rw.Header())
			}
			if strings.Contains(rw.Body.String(), "PRIVATE-DASHBOARD-STORY") {
				t.Errorf("%s exposed state: %s", path, rw.Body.String())
			}
		}
	}

	for _, remote := range []string{"203.0.113.7:4321", "192.168.1.4:4321"} {
		req := httptest.NewRequest("GET", "/status", nil)
		req.RemoteAddr = remote
		req.Header.Set("X-Forwarded-For", "127.0.0.1")
		req.SetBasicAuth(dashboardUser, "test-secret")
		rw := httptest.NewRecorder()
		h.ServeHTTP(rw, req)
		if rw.Code != http.StatusForbidden || rw.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("remote %s: status=%d headers=%v", remote, rw.Code, rw.Header())
		}
	}

	for _, remote := range []string{"127.0.0.1:4321", "100.64.12.34:4321", "[fd7a:115c:a1e0::1]:4321"} {
		req := httptest.NewRequest("GET", "/status", nil)
		req.RemoteAddr = remote
		req.SetBasicAuth(dashboardUser, "test-secret")
		rw := httptest.NewRecorder()
		h.ServeHTTP(rw, req)
		if rw.Code != http.StatusOK || rw.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("trusted remote %s: status=%d headers=%v", remote, rw.Code, rw.Header())
		}
	}
	request := httptest.NewRequest("GET", "/status", nil)
	request.RemoteAddr = "127.0.0.1:4321"
	request.SetBasicAuth(dashboardUser, "")
	response := httptest.NewRecorder()
	NewWeb(store, "").Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatal("empty configured token allowed access", response.Code)
	}
}

func TestDashboardTokenIsPrivatePersistentAndErrorsAreSecretFree(t *testing.T) {
	home := t.TempDir()
	token, err := loadDashboardToken(home)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dashboardTokenPath(home))
	if err != nil || info.Mode().Perm() != 0600 || len(token) != 64 {
		t.Fatalf("token file info=%v err=%v", info, err)
	}
	got, err := loadDashboardToken(home)
	if err != nil || got != token {
		t.Fatalf("token did not persist: err=%v", err)
	}

	badHome := t.TempDir()
	if err := os.MkdirAll(filepath.Dir(dashboardTokenPath(badHome)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dashboardTokenPath(badHome), []byte(token), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dashboardTokenPath(badHome), 0644); err != nil {
		t.Fatal(err)
	}
	_, err = loadDashboardToken(badHome)
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("bad permissions error=%v", err)
	}
	emptyHome := t.TempDir()
	if err := os.MkdirAll(filepath.Dir(dashboardTokenPath(emptyHome)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dashboardTokenPath(emptyHome), []byte("\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = loadDashboardToken(emptyHome)
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("empty token error=%v", err)
	}

	symlinkHome := t.TempDir()
	if err := os.MkdirAll(filepath.Dir(dashboardTokenPath(symlinkHome)), 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(target, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, dashboardTokenPath(symlinkHome)); err != nil {
		t.Fatal(err)
	}
	_, err = loadDashboardToken(symlinkHome)
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("symlink error=%v", err)
	}
}

func TestConcurrentDashboardTokenLoadersPublishOneCompleteToken(t *testing.T) {
	home := t.TempDir()
	const loaders = 32
	start := make(chan struct{})
	tokens := make(chan string, loaders)
	errs := make(chan error, loaders)
	var wg sync.WaitGroup
	for range loaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			token, err := loadDashboardToken(home)
			if err != nil {
				errs <- err
				return
			}
			tokens <- token
		}()
	}
	close(start)
	wg.Wait()
	close(tokens)
	close(errs)
	for err := range errs {
		t.Fatal("concurrent token load failed:", err)
	}

	var winner string
	for token := range tokens {
		if winner == "" {
			winner = token
		}
		if token != winner || len(token) != 64 {
			t.Fatalf("loaders returned inconsistent or incomplete tokens")
		}
		if _, err := hex.DecodeString(token); err != nil {
			t.Fatalf("loader returned a non-hex token: %v", err)
		}
	}
	path := dashboardTokenPath(home)
	data, err := os.ReadFile(path)
	if err != nil || string(data) != winner+"\n" {
		t.Fatalf("published token is incomplete: err=%v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("published token mode=%v err=%v", info, err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("temporary credentials were left behind: entries=%v err=%v", entries, err)
	}
}

func TestDashboardTokenLoaderDoesNotReplaceWinner(t *testing.T) {
	home := t.TempDir()
	path := dashboardTokenPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	winner := strings.Repeat("a", 64)
	if err := os.WriteFile(path, []byte(winner+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	token, err := loadDashboardToken(home)
	if err != nil || token != winner {
		t.Fatalf("existing token changed: token=%q err=%v", token, err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != winner+"\n" {
		t.Fatalf("existing token file changed: err=%v", err)
	}
}

func TestDashboardShutdownJoinsHandlersBeforeReturn(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	web := NewWeb(store, "test-secret")
	ctx, cancel := context.WithCancel(context.Background())
	handlerDone := make(chan struct{})
	releaseHandler := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		web.Handler().ServeHTTP(w, r)
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
	req, _ := http.NewRequest("GET", "http://"+listener.Addr().String()+"/events", nil)
	req.SetBasicAuth(dashboardUser, "test-secret")
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
