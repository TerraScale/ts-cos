package cos

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func testRevision(n int) string { return strings.Repeat(strconv.Itoa(n), 40) }

// fakeBuild is a script standing in for a cos binary: `version` prints revision, or fails.
func fakeBuild(revision string, works bool) string {
	if !works {
		return "#!/bin/sh\nexit 3\n"
	}
	return fmt.Sprintf("#!/bin/sh\necho '{\"protocol\":1,\"revision\":\"%s\"}'\n", revision)
}

func stageTest(t *testing.T, r releases, revision string) {
	t.Helper()
	if err := r.stage(context.Background(), revision, strings.NewReader(fakeBuild(revision, true))); err != nil {
		t.Fatal(err)
	}
}

// bootstrapTest lays out what deploy/install.sh leaves: one build that is current and good.
func bootstrapTest(t *testing.T) (releases, string) {
	t.Helper()
	r := releases(t.TempDir())
	good := testRevision(1)
	stageTest(t, r, good)
	for _, name := range []string{"current", "good"} {
		if err := r.point(name, good); err != nil {
			t.Fatal(err)
		}
	}
	return r, good
}

func linkOf(t *testing.T, r releases, name string) string {
	t.Helper()
	revision, err := r.link(name)
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

// unitScript returns the sh body of a line of deploy/cos.service with %h and $$ expanded as systemd does.
func unitScript(t *testing.T, key, home string) string {
	t.Helper()
	f, err := os.Open("../../deploy/cos.service")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line, ok := strings.CutPrefix(scanner.Text(), key+"=/bin/sh -c '")
		if ok {
			line = strings.TrimSuffix(line, "'")
			return strings.ReplaceAll(strings.ReplaceAll(line, "%h", home), "$$", "$")
		}
	}
	t.Fatalf("deploy/cos.service has no %s sh line", key)
	return ""
}

// systemdStep runs the unit's ExecStartPre or ExecStopPost against a releases dir laid out under home.
func systemdStep(t *testing.T, home, key string) {
	t.Helper()
	if out, err := exec.Command("/bin/sh", "-c", unitScript(t, key, home)).CombinedOutput(); err != nil {
		t.Fatalf("%s: %v: %s", key, err, out)
	}
}

func homeReleases(t *testing.T) (string, releases, string) {
	t.Helper()
	home := t.TempDir()
	r := releasesOf(Config{Home: home})
	if err := os.MkdirAll(string(r), 0o700); err != nil {
		t.Fatal(err)
	}
	good := testRevision(1)
	stageTest(t, r, good)
	for _, name := range []string{"current", "good"} {
		if err := r.point(name, good); err != nil {
			t.Fatal(err)
		}
	}
	return home, r, good
}

func TestWatchdogRevertsUnconfirmedBuild(t *testing.T) {
	home, r, good := homeReleases(t)
	next := testRevision(2)
	stageTest(t, r, next)
	if err := r.activate(next); err != nil {
		t.Fatal(err)
	}
	// The old build stops to restart into the new one: no revert, the deploy is only pending.
	systemdStep(t, home, "ExecStopPost")
	if got := linkOf(t, r, "current"); got != next {
		t.Fatalf("old build's stop reverted the pending deploy: current=%s", got)
	}
	systemdStep(t, home, "ExecStartPre")
	if r.trial() != next {
		t.Fatalf("trial = %q, want %s", r.trial(), next)
	}
	// The new build crashes, misses READY within 60 s or is killed: the watchdog reverts it.
	systemdStep(t, home, "ExecStopPost")
	if got := linkOf(t, r, "current"); got != good {
		t.Fatalf("current = %s after failed trial, want last-known-good %s", got, good)
	}
	if r.trial() != "" || r.marker("reverted") != next {
		t.Fatalf("trial=%q reverted=%q", r.trial(), r.marker("reverted"))
	}
	// Restarting the good build is not a trial and later stops leave it alone.
	systemdStep(t, home, "ExecStartPre")
	systemdStep(t, home, "ExecStopPost")
	if got := linkOf(t, r, "current"); got != good {
		t.Fatalf("current = %s, want %s", got, good)
	}
	if err := r.commit(next); err == nil {
		t.Fatal("committed a build the watchdog reverted")
	}
}

// A SIGKILL to the whole unit can kill ExecStopPost too; the next start still reverts.
func TestWatchdogRevertsWhenStopPostWasKilled(t *testing.T) {
	home, r, good := homeReleases(t)
	next := testRevision(2)
	stageTest(t, r, next)
	if err := r.activate(next); err != nil {
		t.Fatal(err)
	}
	systemdStep(t, home, "ExecStopPost")
	systemdStep(t, home, "ExecStartPre")
	// The trial build is killed together with its ExecStopPost, so only the next start runs.
	systemdStep(t, home, "ExecStartPre")
	if got := linkOf(t, r, "current"); got != good || r.trial() != "" || r.marker("reverted") != next {
		t.Fatalf("current=%s trial=%q reverted=%q; a trial build must get exactly one start", got, r.trial(), r.marker("reverted"))
	}
}

func TestWatchdogKeepsCommittedBuild(t *testing.T) {
	home, r, _ := homeReleases(t)
	next := testRevision(2)
	stageTest(t, r, next)
	if err := r.activate(next); err != nil {
		t.Fatal(err)
	}
	if err := r.commit(next); err == nil {
		t.Fatal("committed a build systemd has not started yet")
	}
	systemdStep(t, home, "ExecStopPost")
	systemdStep(t, home, "ExecStartPre")
	if err := r.commit(next); err != nil {
		t.Fatal(err)
	}
	if err := r.commit(next); err != nil {
		t.Fatalf("repeated commit: %v", err)
	}
	systemdStep(t, home, "ExecStopPost")
	if current, good := linkOf(t, r, "current"), linkOf(t, r, "good"); current != next || good != next {
		t.Fatalf("current=%s good=%s, want %s", current, good, next)
	}
}

func TestRestoreRollsBackCommittedBuild(t *testing.T) {
	home, r, good := homeReleases(t)
	next := testRevision(2)
	stageTest(t, r, next)
	if err := r.activate(next); err != nil {
		t.Fatal(err)
	}
	systemdStep(t, home, "ExecStopPost")
	systemdStep(t, home, "ExecStartPre")
	if err := r.commit(next); err != nil {
		t.Fatal(err)
	}
	if err := r.restore(good); err != nil {
		t.Fatal(err)
	}
	systemdStep(t, home, "ExecStopPost")
	systemdStep(t, home, "ExecStartPre")
	if current, g := linkOf(t, r, "current"), linkOf(t, r, "good"); current != good || g != good || r.trial() != "" {
		t.Fatalf("current=%s good=%s trial=%q, want %s", current, g, r.trial(), good)
	}
}

func TestStageRejectsBrokenBuild(t *testing.T) {
	r, _ := bootstrapTest(t)
	next := testRevision(2)
	if err := r.stage(context.Background(), next, strings.NewReader(fakeBuild(next, false))); err == nil {
		t.Fatal("staged a build that does not start")
	}
	if err := r.stage(context.Background(), next, strings.NewReader(fakeBuild(testRevision(3), true))); err == nil {
		t.Fatal("staged a build that reports another revision")
	}
	// Plain builds from before the watchdog answer `cos version` too, but never send READY.
	legacy := fmt.Sprintf("#!/bin/sh\necho '{\"revision\":\"%s\",\"modified\":false}'\n", next)
	if err := r.stage(context.Background(), next, strings.NewReader(legacy)); err == nil || !strings.Contains(err.Error(), "release protocol") {
		t.Fatalf("staged a build without the release protocol: %v", err)
	}
	if _, err := os.Stat(r.path("builds", next, "cos")); err == nil {
		t.Fatal("rejected build left behind")
	}
	if err := r.activate(next); err == nil {
		t.Fatal("activated a build that was never staged")
	}
	if err := r.stage(context.Background(), "../../etc", strings.NewReader("")); err == nil {
		t.Fatal("accepted a path as revision")
	}
}

func TestPruneKeepsLastThreeAndPinned(t *testing.T) {
	r, good := bootstrapTest(t)
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(r.path("builds", good), old, old); err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 6; i++ {
		stageTest(t, r, testRevision(i))
		at := old.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(r.path("builds", testRevision(i)), at, at); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.prune(); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(r.path("builds"))
	var kept []string
	for _, e := range entries {
		kept = append(kept, e.Name()[:1])
	}
	if strings.Join(kept, "") != "156" {
		t.Fatalf("kept builds %v, want 3: the good build 1 and the newest 5, 6", kept)
	}
}

func notifySocket(t *testing.T) <-chan string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "notify")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	t.Setenv("NOTIFY_SOCKET", path)
	states := make(chan string, 4)
	go func() {
		buf := make([]byte, 256)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			states <- string(buf[:n])
		}
	}()
	return states
}

func TestWatchdogReadiness(t *testing.T) {
	states := notifySocket(t)
	r, good := bootstrapTest(t)
	r.watchdog(context.Background(), good, time.Minute, func(context.Context) []string {
		t.Fatal("a build that is not on trial waited for health")
		return nil
	}, func() { t.Fatal("stopped a confirmed build") })
	if got := <-states; got != "READY=1" {
		t.Fatalf("notify = %q", got)
	}

	next := testRevision(2)
	stageTest(t, r, next)
	if err := os.WriteFile(r.path("trial"), []byte(next), 0o600); err != nil {
		t.Fatal(err)
	}
	var checks atomic.Int32
	stopped := make(chan struct{})
	start := time.Now()
	r.watchdog(context.Background(), next, 50*time.Millisecond, func(context.Context) []string {
		if checks.Add(1) < 3 {
			return []string{"not a NATS cluster member"}
		}
		return nil
	}, func() { close(stopped) })
	if got := <-states; got != "READY=1" || time.Since(start) < 2*time.Second {
		t.Fatalf("trial build reported %q after %s, before it was healthy", got, time.Since(start))
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("unconfirmed trial build kept running")
	}

	// A trial that never gets healthy never reports READY; systemd's start timeout stops it.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	r.watchdog(ctx, next, time.Minute, func(context.Context) []string { return []string{"JetStream not in sync"} }, func() {})
	select {
	case got := <-states:
		t.Fatalf("unhealthy trial build reported %q", got)
	default:
	}
}

func TestHealthNeedsClusterMembership(t *testing.T) {
	b, _ := releaseTestBus(t)
	ok := func(context.Context) []string { return nil }
	if got := strings.Join(b.health(context.Background(), ok), "; "); got != "not a NATS cluster member" {
		t.Fatalf("standalone node health = %q", got)
	}
	broken := func(context.Context) []string { return []string{"dashboard unavailable"} }
	if got := b.health(context.Background(), broken); len(got) != 2 {
		t.Fatalf("health = %q, want the cluster and the probe problem", got)
	}
}

// The probe signs in to the real dashboard (login, sessions, Web) the way the deployed node does.
func TestDashboardProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configs, buses := releaseCluster(t, ctx)
	js, err := buses[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	auth := NewAuth(configs[0].Home, js)
	go auth.Start(ctx)
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	web := NewWeb(store, auth)
	web.Hooks = http.NotFoundHandler()
	dashboard := web.Handler()
	broken := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == broken {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		dashboard.ServeHTTP(w, r)
	}))
	defer srv.Close()
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	var last []string
	busTestReady(t, "probe signs in", func() bool { last = dashboardProbe(ctx, port, auth); return len(last) == 0 })
	_, sessions := auth.kv()
	if keys, _ := sessions.Keys(ctx); len(keys) != 0 {
		t.Fatalf("probe sessions left behind: %v", keys)
	}
	for path, want := range map[string]string{
		"/login":       "login page answers HTTP 503",
		"/":            "dashboard answers HTTP 503",
		"/chat/events": "dashboard SSE /chat/events answers HTTP 503 without events",
	} {
		broken = path
		if got := strings.Join(dashboardProbe(ctx, port, auth), "; "); got != want {
			t.Fatalf("probe with %s broken = %q, want %q", path, got, want)
		}
	}
	for _, page := range []string{`<body data-init="@get('/chat/events', {retry: 'always'})">`, `<body data-init="@get(&#39;/events&#39;, {})">`} {
		if m := pageStream.FindStringSubmatch(page); m == nil || (m[1] != "/chat/events" && m[1] != "/events") {
			t.Fatalf("stream of %s = %v", page, m)
		}
	}
	srv.Close()
	if got := strings.Join(dashboardProbe(ctx, port, auth), "; "); got != "dashboard unavailable" {
		t.Fatalf("probe of a stopped dashboard = %q", got)
	}
}

// releaseTestBus is one standalone JetStream node, enough for the release endpoints.
func releaseTestBus(t *testing.T) (*Bus, jetstream.JetStream) {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	conn, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); s.Shutdown(); s.WaitForShutdown() })
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	return &Bus{conn: conn, server: s}, js
}

func releaseCall(t *testing.T, conn *nats.Conn, verb, revision string, out any) error {
	t.Helper()
	data, _ := json.Marshal(releaseRequest{Revision: revision})
	msg, err := conn.Request("cos.testhost.release."+verb, data, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		return fmt.Errorf("%s", e)
	}
	if out != nil {
		return json.Unmarshal(msg.Data, out)
	}
	return nil
}

func TestReleaseEndpoints(t *testing.T) {
	home, r, good := homeReleases(t)
	b, js := releaseTestBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restarts := make(chan struct{}, 4)
	cfg := Config{Home: home, LocalHost: "testhost"}
	unavailable := func(context.Context) []string { return []string{"dashboard unavailable"} }
	if err := b.serveReleases(ctx, cfg, unavailable, func() { restarts <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: buildsBucket})
	if err != nil {
		t.Fatal(err)
	}
	next, broken := testRevision(2), testRevision(3)
	if _, err := store.PutBytes(ctx, next, []byte(fakeBuild(next, true))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutBytes(ctx, broken, []byte(fakeBuild(broken, false))); err != nil {
		t.Fatal(err)
	}

	var v Build
	if err := releaseCall(t, b.conn, "version", "", &v); err != nil || v.Host != "testhost" || v.Good != good || v.Behavior == "" {
		t.Fatalf("version = %+v, %v", v, err)
	}
	if err := releaseCall(t, b.conn, "install", "../x", nil); err == nil {
		t.Fatal("install accepted an invalid revision")
	}
	if err := releaseCall(t, b.conn, "install", broken, nil); err == nil || !strings.Contains(err.Error(), "does not start") {
		t.Fatalf("install of a broken build: %v", err)
	}
	if err := releaseCall(t, b.conn, "install", testRevision(4), nil); err == nil {
		t.Fatal("installed a build missing from the store")
	}
	if got := linkOf(t, r, "current"); got != good || len(restarts) != 0 {
		t.Fatalf("failed installs changed current to %s or restarted", got)
	}
	if err := releaseCall(t, b.conn, "install", next, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-restarts:
	case <-time.After(5 * time.Second):
		t.Fatal("install did not restart")
	}
	if got := linkOf(t, r, "current"); got != next || r.marker("trial.next") != next {
		t.Fatalf("current=%s trial.next=%q", got, r.marker("trial.next"))
	}
	systemdStep(t, home, "ExecStopPost")
	systemdStep(t, home, "ExecStartPre")
	var health NodeHealth
	if err := releaseCall(t, b.conn, "health", "", &health); err != nil || len(health.Problems) == 0 {
		t.Fatalf("health = %+v, %v", health, err)
	}
	if err := releaseCall(t, b.conn, "commit", next, nil); err != nil {
		t.Fatal(err)
	}
	if got := linkOf(t, r, "good"); got != next || r.trial() != "" {
		t.Fatalf("good=%s trial=%q after commit", got, r.trial())
	}
	if err := releaseCall(t, b.conn, "restore", good, nil); err != nil {
		t.Fatal(err)
	}
	<-restarts
	if current, g := linkOf(t, r, "current"), linkOf(t, r, "good"); current != good || g != good {
		t.Fatalf("restore left current=%s good=%s", current, g)
	}
}

// releaseCluster starts three clustered nodes, as the leader test does.
func releaseCluster(t *testing.T, ctx context.Context) ([]Config, []*Bus) {
	t.Helper()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	configs, buses := make([]Config, 3), make([]*Bus, 3)
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
		configs[i], buses[i] = cfg, bus
	}
	return configs, buses
}

func TestHealthTracksClusterMembership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ok := func(context.Context) []string { return nil }
	_, buses := releaseCluster(t, ctx)
	for _, b := range buses {
		busTestReady(t, "healthy cluster member", func() bool { return len(b.health(ctx, ok)) == 0 })
	}
	// Without its peers a node is no longer a healthy member, so a trial build there never reports READY.
	buses[1].Close()
	buses[2].Close()
	busTestReady(t, "lone node reported unhealthy", func() bool { return len(buses[0].health(ctx, ok)) > 0 })
}

// A deploy over a host whose current build is still on trial must start the new build, not revert.
func TestActivateClearsLeftoverTrial(t *testing.T) {
	home, r, _ := homeReleases(t)
	first, second := testRevision(2), testRevision(3)
	stageTest(t, r, first)
	stageTest(t, r, second)
	if err := r.activate(first); err != nil {
		t.Fatal(err)
	}
	systemdStep(t, home, "ExecStopPost")
	systemdStep(t, home, "ExecStartPre")
	if err := r.activate(second); err != nil {
		t.Fatal(err)
	}
	systemdStep(t, home, "ExecStopPost")
	systemdStep(t, home, "ExecStartPre")
	if got := linkOf(t, r, "current"); got != second || r.trial() != second {
		t.Fatalf("current=%s trial=%q, want the second build on trial", got, r.trial())
	}
}

// A build the watchdog reverted is never activated again on that host, while another build is.
func TestActivateRefusesARevertedBuild(t *testing.T) {
	home, r, good := homeReleases(t)
	bad, next := testRevision(2), testRevision(3)
	stageTest(t, r, bad)
	stageTest(t, r, next)
	if err := r.activate(bad); err != nil {
		t.Fatal(err)
	}
	systemdStep(t, home, "ExecStopPost")
	systemdStep(t, home, "ExecStartPre")
	systemdStep(t, home, "ExecStopPost") // the trial build fails: the watchdog reverts it
	if r.marker("reverted") != bad || linkOf(t, r, "current") != good {
		t.Fatalf("setup: reverted=%q current=%s", r.marker("reverted"), linkOf(t, r, "current"))
	}
	if err := r.activate(bad); err == nil || !strings.Contains(err.Error(), "reverted on this host") {
		t.Fatalf("re-activated a reverted build: %v", err)
	}
	if got := linkOf(t, r, "current"); got != good || r.marker("trial.next") != "" {
		t.Fatalf("refused activate changed current=%s trial.next=%q", got, r.marker("trial.next"))
	}
	if err := r.activate(next); err != nil {
		t.Fatalf("a different build was refused: %v", err)
	}
	if r.marker("reverted") != "" {
		t.Fatal("activating another build kept the old reverted marker")
	}
}
