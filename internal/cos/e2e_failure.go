package cos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const forgejo5xxToken = "e2e-dummy-token"

// checkForgejo5xx runs against a fake Forgejo, so the real token is never read or sent.
func checkForgejo5xx(ctx context.Context, _ *E2EEnv) (string, error) {
	return forgejo5xxScenario(ctx, true)
}

// forgejo5xxScenario starts the fake server down or healthy. Healthy from the start must fail the check.
func forgejo5xxScenario(ctx context.Context, down bool) (string, error) {
	home, err := os.MkdirTemp("", "cos-e2e-forgejo-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(home)
	keys := filepath.Join(home, ".local/share/forgejo-cli")
	if err := os.MkdirAll(keys, 0o700); err != nil {
		return "", err
	}
	creds := fmt.Sprintf(`{"hosts":{"git.terrascale.tech":{"token":%q}}}`, forgejo5xxToken)
	if err := os.WriteFile(filepath.Join(keys, "keys.json"), []byte(creds), 0o600); err != nil {
		return "", err
	}
	server := &forgejo5xxServer{down: down}
	srv := httptest.NewServer(server)
	defer srv.Close()
	f := Forgejo{Config: Config{Home: home, ForgejoURL: srv.URL, HTTP: srv.Client()}}

	if _, err := f.PRs(ctx); err == nil || !strings.Contains(err.Error(), "HTTP 503") || strings.Contains(err.Error(), forgejo5xxToken) {
		return "", fmt.Errorf("PRs during outage: %v, want HTTP 503 without the token", err)
	}
	if _, err := f.Merge(ctx, "ts-cos", 1, MergeOrder{}); err == nil {
		return "", errors.New("merge during outage succeeded")
	}
	if posts := server.Posts(); posts != 0 {
		return "", fmt.Errorf("merge during outage sent %d POST requests", posts)
	}
	server.SetDown(false)
	prs, err := f.PRs(ctx)
	if err != nil || len(prs) != 0 {
		return "", fmt.Errorf("PRs after recovery: %d PRs, %v", len(prs), err)
	}
	return "503: PRs failed cleanly (HTTP 503), merge sent 0 POSTs, recovered on 200", nil
}

// forgejo5xxServer answers 503 while down and counts POSTs, so a merge that left the client shows up.
type forgejo5xxServer struct {
	mu    sync.Mutex
	down  bool
	posts int
}

func (s *forgejo5xxServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method == http.MethodPost {
		s.posts++
	}
	if s.down {
		http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprint(w, "[]")
}

func (s *forgejo5xxServer) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

func (s *forgejo5xxServer) Posts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.posts
}
