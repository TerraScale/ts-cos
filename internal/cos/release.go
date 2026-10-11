package cos

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

// A host keeps its builds in ~/.local/share/cos: builds/<revision>/cos, the "current"
// symlink systemd starts and the "good" symlink to the last-known-good build. A deploy
// writes trial.next; deploy/cos.service promotes it to "trial" for the next start, and
// any stop while "trial" exists points current back at good. That per-node watchdog is
// systemd and sh only, so it works when the new binary or the whole cluster does not.
type releases string

const (
	keepBuilds   = 3
	buildsBucket = "cos_builds"     // JetStream object store, one object per revision
	trialTimeout = 10 * time.Minute // an unconfirmed build stops itself, and the watchdog reverts it
)

func releasesOf(cfg Config) releases { return releases(filepath.Join(cfg.Home, ".local/share/cos")) }

func (r releases) path(parts ...string) string {
	return filepath.Join(append([]string{string(r)}, parts...)...)
}

func validRevision(revision string) bool {
	_, err := hex.DecodeString(revision)
	return err == nil && len(revision) == 40
}

// link returns the revision the current or good symlink points at.
func (r releases) link(name string) (string, error) {
	target, err := os.Readlink(r.path(name))
	if err != nil {
		return "", err
	}
	return filepath.Base(target), nil
}

func (r releases) marker(name string) string {
	data, _ := os.ReadFile(r.path(name))
	return strings.TrimSpace(string(data))
}

func (r releases) trial() string { return r.marker("trial") }

func (r releases) remove(names ...string) error {
	for _, name := range names {
		if err := os.Remove(r.path(name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// point atomically repoints the current or good symlink at builds/<revision>.
func (r releases) point(name, revision string) error {
	if _, err := os.Stat(r.path("builds", revision, "cos")); err != nil {
		return fmt.Errorf("build %.12s is not on this host", revision)
	}
	tmp := r.path(name + ".tmp")
	_ = os.Remove(tmp)
	if err := os.Symlink(filepath.Join("builds", revision), tmp); err != nil {
		return err
	}
	return os.Rename(tmp, r.path(name))
}

// stage stores a build under builds/<revision> once it starts and reports that revision.
func (r releases) stage(ctx context.Context, revision string, bin io.Reader) error {
	if !validRevision(revision) {
		return errors.New("invalid revision")
	}
	dir := r.path("builds", revision)
	if _, err := os.Stat(filepath.Join(dir, "cos")); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "cos.new")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o700)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, bin)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = preflight(ctx, tmp, revision)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "cos"))
}

// preflight rejects a binary that cannot run here (wrong arch, crash on start) or is another build.
func preflight(ctx context.Context, bin, revision string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "version").Output()
	var v Build
	if err != nil || json.Unmarshal(out, &v) != nil {
		return fmt.Errorf("build %.12s does not start", revision)
	}
	if v.Revision != revision || v.Modified {
		return fmt.Errorf("build %.12s reports revision %.12s (modified=%t)", revision, v.Revision, v.Modified)
	}
	// A build without the deploy watchdog support would never send READY under cos.service.
	if v.Protocol < releaseProtocol {
		return fmt.Errorf("build %.12s cannot be deployed safely: it lacks the release protocol", revision)
	}
	return nil
}

// activate makes a staged build the one systemd starts next, on trial.
func (r releases) activate(revision string) error {
	if err := r.remove("reverted"); err != nil {
		return err
	}
	if err := os.WriteFile(r.path("trial.next"), []byte(revision+"\n"), 0o600); err != nil {
		return err
	}
	if err := r.point("current", revision); err != nil {
		return err
	}
	// A leftover trial from an earlier deploy would make this process's stop revert current to good.
	return r.remove("trial")
}

// commit confirms the trial build as last-known-good. A build the watchdog already
// reverted, or one not restarted into yet, is no longer (or not yet) the trial.
func (r releases) commit(revision string) error {
	current, _ := r.link("current")
	good, _ := r.link("good")
	if current == revision && good == revision && r.trial() == "" {
		return nil
	}
	if current != revision || r.trial() != revision {
		return fmt.Errorf("build %.12s is not on trial on this host", revision)
	}
	if err := r.point("good", revision); err != nil {
		return err
	}
	return r.remove("trial")
}

// restore makes revision current and good and ends any trial; the caller restarts.
// good moves first, so a stop in between still lands on revision.
func (r releases) restore(revision string) error {
	if err := r.remove("trial.next"); err != nil {
		return err
	}
	if err := r.point("good", revision); err != nil {
		return err
	}
	if err := r.point("current", revision); err != nil {
		return err
	}
	return r.remove("trial")
}

// prune keeps the newest keepBuilds builds and anything current, good or on trial points at.
func (r releases) prune() error {
	entries, err := os.ReadDir(r.path("builds"))
	if err != nil {
		return err
	}
	pinned := []string{r.trial(), r.marker("trial.next")}
	for _, name := range []string{"current", "good"} {
		revision, _ := r.link(name)
		pinned = append(pinned, revision)
	}
	type build struct {
		name string
		at   time.Time
	}
	var builds []build
	for _, e := range entries {
		if info, err := e.Info(); err == nil && e.IsDir() {
			builds = append(builds, build{e.Name(), info.ModTime()})
		}
	}
	slices.SortFunc(builds, func(a, b build) int { return b.at.Compare(a.at) })
	for i, b := range builds {
		if i >= keepBuilds && !slices.Contains(pinned, b.name) {
			if err := os.RemoveAll(r.path("builds", b.name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// sdNotify sends a state to systemd for a Type=notify unit; outside systemd it does nothing.
func sdNotify(state string) error {
	socket := os.Getenv("NOTIFY_SOCKET")
	if socket == "" {
		return nil
	}
	if socket[0] == '@' {
		socket = "\x00" + socket[1:]
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: socket, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(state))
	return err
}

// watchdog reports READY=1 to systemd. A trial build reports it only once healthy, so
// systemd's start timeout reverts a build that never gets there, and it stops itself
// (which reverts it too) if no rollout confirms it within confirm.
func (r releases) watchdog(ctx context.Context, revision string, confirm time.Duration, healthy func(context.Context) []string, stop func()) {
	if revision == "" || r.trial() != revision {
		if err := sdNotify("READY=1"); err != nil {
			slog.Error("systemd notify failed", "error", err)
		}
		return
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for problems := healthy(ctx); len(problems) > 0; problems = healthy(ctx) {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
	if err := sdNotify("READY=1"); err != nil {
		slog.Error("systemd notify failed", "error", err)
	}
	timer := time.NewTimer(confirm)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
		if r.trial() == revision {
			slog.Warn("trial build was not confirmed; stopping so the watchdog reverts it", "revision", revision)
			stop()
		}
	}
}

// NodeHealth is what the rollout gate reads from a host.
type NodeHealth struct {
	Build
	Problems []string `json:"problems,omitempty"`
	Reverted string   `json:"reverted,omitempty"` // revision the watchdog last reverted here
}

var probeClient = &http.Client{Timeout: 5 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

// health lists what keeps this node from being a healthy member: cluster routes,
// JetStream (meta, streams and consumers current) and what probe finds on its dashboard.
func (b *Bus) health(ctx context.Context, probe func(context.Context) []string) []string {
	var problems []string
	switch {
	case b.server == nil || !b.server.JetStreamIsClustered() || b.server.NumRoutes() == 0:
		problems = append(problems, "not a NATS cluster member")
	default:
		if h := b.server.Healthz(nil); h.Error != "" || len(h.Errors) > 0 {
			problems = append(problems, "JetStream not in sync: "+h.Error)
		}
	}
	return append(problems, probe(ctx)...)
}

// dashboardProbe checks this node's dashboard over loopback the way a user meets it:
// the login page renders, and signed in, the dashboard renders and its SSE stream opens.
func dashboardProbe(ctx context.Context, port int, auth *Auth) []string {
	get := func(path, cookie string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
		if err != nil {
			return nil, err
		}
		if cookie != "" {
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie})
		}
		return probeClient.Do(req)
	}
	resp, err := get("/login", "")
	if err != nil {
		return []string{"dashboard unavailable"}
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return []string{fmt.Sprintf("login page answers HTTP %d", resp.StatusCode)}
	}
	cookie, end, err := auth.ProbeSession(ctx)
	if err != nil {
		return []string{"sign-in unavailable: " + err.Error()}
	}
	defer end()
	if resp, err = get("/", cookie); err != nil {
		return []string{"dashboard unavailable"}
	}
	page, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || err != nil {
		return []string{fmt.Sprintf("dashboard answers HTTP %d", resp.StatusCode)}
	}
	// Probe the stream the page itself opens, whichever page "/" is.
	stream := pageStream.FindSubmatch(page)
	if stream == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if resp, err = get(string(stream[1]), cookie); err != nil {
		return []string{"dashboard SSE unavailable"}
	}
	defer resp.Body.Close()
	if _, err := resp.Body.Read(make([]byte, 1)); resp.StatusCode != http.StatusOK ||
		!strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") || err != nil {
		return []string{fmt.Sprintf("dashboard SSE %s answers HTTP %d without events", stream[1], resp.StatusCode)}
	}
	return nil
}

// pageStream finds the first Datastar SSE a page opens, e.g. <body data-init="@get('/events', …)">.
var pageStream = regexp.MustCompile(`@get\((?:'|&#39;)(/[^'&]*)(?:'|&#39;)`)

type releaseRequest struct {
	Revision string `json:"revision"`
}

// serveReleases lets the rollout leader drive this host over cos.<host>.release.*.
// install and restore answer before restart() ends this process; systemd starts the new current.
func (b *Bus) serveReleases(ctx context.Context, cfg Config, probe func(context.Context) []string, restart func()) error {
	r := releasesOf(cfg)
	svc, err := micro.AddService(b.conn, micro.Config{Name: "cos-release", Version: "0.1.0",
		Metadata: map[string]string{"host": cfg.LocalHost}, QueueGroup: "cos-release-" + cfg.LocalHost})
	if err != nil {
		return fmt.Errorf("COS release service registration failed: %w", err)
	}
	context.AfterFunc(ctx, func() { _ = svc.Stop() })
	group := svc.AddGroup("cos." + cfg.LocalHost + ".release")
	add := func(name string, fn func(ctx context.Context, revision string) (any, bool, error)) error {
		return group.AddEndpoint(name, micro.HandlerFunc(func(req micro.Request) {
			var in releaseRequest
			if name != "health" && name != "version" && (json.Unmarshal(req.Data(), &in) != nil || !validRevision(in.Revision)) {
				_ = req.Error("400", "release request needs a full revision", nil)
				return
			}
			call, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			result, restarting, err := fn(call, in.Revision)
			if err != nil {
				_ = req.Error("500", err.Error(), nil)
				return
			}
			_ = req.RespondJSON(result)
			if restarting {
				slog.Info("restarting into another build", "action", name, "revision", in.Revision)
				time.AfterFunc(200*time.Millisecond, restart)
			}
		}))
	}
	err = errors.Join(
		add("version", func(context.Context, string) (any, bool, error) { return CurrentBuild(cfg), false, nil }),
		add("health", func(ctx context.Context, _ string) (any, bool, error) {
			return NodeHealth{Build: CurrentBuild(cfg), Problems: b.health(ctx, probe), Reverted: r.marker("reverted")}, false, nil
		}),
		add("install", func(ctx context.Context, revision string) (any, bool, error) {
			if err := b.fetchBuild(ctx, r, revision); err != nil {
				return nil, false, err
			}
			if err := r.activate(revision); err != nil {
				return nil, false, err
			}
			// Failed deploys never commit, so prune here too to keep the last three builds.
			if err := r.prune(); err != nil {
				slog.Warn("pruning old builds failed", "error", err)
			}
			return CurrentBuild(cfg), true, nil
		}),
		add("commit", func(ctx context.Context, revision string) (any, bool, error) {
			if err := r.commit(revision); err != nil {
				return nil, false, err
			}
			if err := r.prune(); err != nil {
				slog.Warn("pruning old builds failed", "error", err)
			}
			return CurrentBuild(cfg), false, nil
		}),
		add("restore", func(ctx context.Context, revision string) (any, bool, error) {
			v := CurrentBuild(cfg)
			// No-op only when both the running build and what systemd starts next already are revision.
			current, _ := r.link("current")
			if v.Revision == revision && current == revision && v.Good == revision && r.trial() == "" && r.marker("trial.next") == "" {
				return v, false, nil
			}
			if err := b.fetchBuild(ctx, r, revision); err != nil {
				return nil, false, err
			}
			if err := r.restore(revision); err != nil {
				return nil, false, err
			}
			return v, true, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("COS release endpoint registration failed: %w", err)
	}
	return nil
}

// fetchBuild stages revision from the replicated object store unless this host already has it.
func (b *Bus) fetchBuild(ctx context.Context, r releases, revision string) error {
	if _, err := os.Stat(r.path("builds", revision, "cos")); err == nil {
		return nil
	}
	js, err := jetstream.New(b.conn)
	if err != nil {
		return err
	}
	store, err := js.ObjectStore(ctx, buildsBucket)
	if err != nil {
		return fmt.Errorf("build store unavailable: %w", err)
	}
	// The object store checks the SHA-256 digest when the read reaches the end.
	object, err := store.Get(ctx, revision)
	if err != nil {
		return fmt.Errorf("build %.12s unavailable: %w", revision, err)
	}
	defer object.Close()
	return r.stage(ctx, revision, object)
}
