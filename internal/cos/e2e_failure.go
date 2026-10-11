package cos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
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

// checkSQLiteDiskFull runs the real store on a temp database capped at its current size. SQLite returns
// SQLITE_FULL there, as it does on ENOSPC. The node's own cos.db is never touched.
func checkSQLiteDiskFull(ctx context.Context, _ *E2EEnv) (string, error) {
	dir, err := os.MkdirTemp("", "cos-e2e-diskfull-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	s, err := OpenStore(filepath.Join(dir, "cos.db"))
	if err != nil {
		return "", err
	}
	defer s.Close()
	const baseline = "e2e baseline"
	if _, err := s.AddNote(ctx, "suggest", baseline); err != nil {
		return "", fmt.Errorf("baseline write: %w", err)
	}
	r0, err := s.Revision(ctx)
	if err != nil {
		return "", err
	}
	var pages int64
	if err := s.DB.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err != nil {
		return "", err
	}
	if _, err := s.DB.ExecContext(ctx, fmt.Sprintf(`PRAGMA max_page_count = %d`, pages)); err != nil {
		return "", err
	}
	big := strings.Repeat("x", 256<<10)
	_, full := s.AddNote(ctx, "suggest", big)
	if full == nil || !strings.Contains(full.Error(), "full") {
		return "", fmt.Errorf("write past the page limit did not fail with disk full: %v", full)
	}
	if got, err := s.Revision(ctx); err != nil || got != r0 {
		return "", fmt.Errorf("revision after the failed write: %d, want %d: %v", got, r0, err)
	}
	story, err := s.Story(ctx)
	if err != nil {
		return "", fmt.Errorf("earlier data unreadable while full: %w", err)
	}
	if !slices.ContainsFunc(story.Notes, func(n Note) bool { return n.Text == baseline }) {
		return "", errors.New("baseline record missing while full")
	}
	var integrity string
	if err := s.DB.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		return "", fmt.Errorf("integrity_check while full: %q: %v", integrity, err)
	}
	if _, err := s.DB.ExecContext(ctx, `PRAGMA max_page_count = 1073741823`); err != nil {
		return "", err
	}
	if _, err := s.AddNote(ctx, "suggest", big); err != nil {
		return "", fmt.Errorf("write after space was freed failed: %w", err)
	}
	if got, err := s.Revision(ctx); err != nil || got != r0+1 {
		return "", fmt.Errorf("revision after the freed write: %d, want %d: %v", got, r0+1, err)
	}
	return fmt.Sprintf("temp store: a write at the page limit failed with %q; revision unchanged, earlier data readable, integrity ok; after space was freed the same write succeeded without reopening", full.Error()), nil
}

// procCgroup is a var so the test can point inUnit at a fake cgroup file.
var procCgroup = "/proc/self/cgroup"

// inUnit reports whether this process runs in the systemd unit's cgroup, which restarting the unit would stop.
// An unreadable cgroup file counts as inside, so the check skips rather than restart its own node.
func inUnit(unit string) bool {
	data, err := os.ReadFile(procCgroup)
	// Unreadable: assume we are inside, so the check skips instead of restarting its own run.
	return err != nil || strings.Contains(string(data), "/"+strings.TrimSuffix(unit, ".service")+".service")
}

// checkNATSRestart restarts the runner's own node, which restarts its embedded NATS server. The runner's
// client must survive it on the same connection: a publish made during the restart arrives, and JetStream accepts a write.
func checkNATSRestart(ctx context.Context, e *E2EEnv) (string, error) {
	if inUnit(e2eUnit) {
		return "", E2ESkip("runs only from the cos CLI: restarting this node would stop an e2e run inside cos.service")
	}
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	if err := e.waitNoRollout(ctx, bus); err != nil {
		return "", err
	}
	gate, err := e.allCurrent(ctx, bus, "failure.nats-restart")
	if err != nil {
		return "", err
	}
	if _, _, err := waitCluster(ctx, bus.conn, e2eHosts, "", 10*time.Second); err != nil {
		return "", fmt.Errorf("cluster unhealthy before restart, not perturbing it: %w", err)
	}
	pre, err := e.preSignal(ctx, bus)
	if err != nil {
		return "", err
	}
	target := e.cfg.LocalHost
	subject := "cos.e2e.nats-restart." + nats.NewInbox()[len("_INBOX."):]
	sub, err := bus.conn.SubscribeSync(subject)
	if err != nil {
		return "", err
	}
	if err := bus.conn.Flush(); err != nil {
		return "", err
	}
	before := bus.conn.Stats().Reconnects
	if err := e.chaos(ctx, bus, target, "failure.nats-restart", "restart"); err != nil {
		return "", err
	}
	if _, err := e.run(ctx, target, "systemctl --user restart "+e2eUnit); err != nil {
		return "", err
	}
	if err := bus.conn.Publish(subject, []byte("during")); err != nil {
		return "", fmt.Errorf("publish made during the NATS restart on %s was refused: %w", target, err)
	}
	msg, err := sub.NextMsg(30 * time.Second)
	if err != nil || string(msg.Data) != "during" {
		return "", fmt.Errorf("publish made during the NATS restart on %s was lost: %v", target, err)
	}
	reconnects := bus.conn.Stats().Reconnects
	if reconnects <= before || !bus.conn.IsConnected() {
		return "", fmt.Errorf("NATS restarted on %s but the client did not reconnect on its connection: %d reconnects (was %d), connected %t", target, reconnects, before, bus.conn.IsConnected())
	}
	writeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		kv, err := chaosKV(writeCtx, bus)
		if err == nil {
			_, err = kv.Put(writeCtx, target+".nats-restart-ok", []byte("ok"))
		}
		if err == nil {
			break
		}
		if writeCtx.Err() != nil {
			return "", fmt.Errorf("JetStream write after the NATS restart on %s: %w", target, err)
		}
		select {
		case <-writeCtx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
	summary, took, err := waitCluster(ctx, bus.conn, e2eHosts, "", 60*time.Second)
	if err != nil {
		return "", fmt.Errorf("%s restarted but did not catch up: %w", target, err)
	}
	return fmt.Sprintf("restarted NATS on %s (the runner's node): the client reconnected on its connection (%d reconnects), a publish made during the restart arrived after it, JetStream accepted a write; caught up in %s; %s; %s; %s", target, reconnects, took.Round(100*time.Millisecond), summary, gateEvidence(gate), pre), nil
}
