package cos

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats.go/jetstream"
)

// freezeDiagnosticLine selects the NATS log lines that explain leader and JetStream behaviour.
var freezeDiagnosticLine = regexp.MustCompile("raft|leader|Leader|JetStream|route|Route|election|quorum|stepdown|Ping|Stale")

// freezeTimelineLine selects the NATS log lines that show how survivors reacted to the freeze.
var freezeTimelineLine = regexp.MustCompile("Switching to candidate|Not switching|Switching to leader|metadata leader|new stream leader|catchup|Catchup|Stale|Slow Consumer|write deadline|lost quorum|Lost quorum")

// TestBusFreezeNode is the child process for TestBusFrozenLeaderLosesLease. It runs one COS bus node until signalled.
func TestBusFreezeNode(t *testing.T) {
	if os.Getenv("COS_FREEZE_NODE") == "" {
		t.Skip("helper process for TestBusFrozenLeaderLosesLease")
	}
	home, runtime := os.Getenv("COS_FREEZE_HOME"), os.Getenv("COS_FREEZE_RUNTIME")
	port, _ := strconv.Atoi(os.Getenv("COS_FREEZE_PORT"))
	route, _ := strconv.Atoi(os.Getenv("COS_FREEZE_ROUTE"))
	var peers []string
	for _, p := range strings.Split(os.Getenv("COS_FREEZE_PEERS"), ",") {
		peers = append(peers, "127.0.0.1:"+p)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	cfg := Config{Home: home, Runtime: runtime, LocalHost: os.Getenv("COS_FREEZE_NODE")}
	node := busNode{master: busTestMaster, clientPort: port, routeHost: "127.0.0.1", routePort: route,
		peers: func(context.Context) ([]string, error) { return peers, nil }}
	b, err := startBus(ctx, cfg, node, func(context.Context, string, []string) (any, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	l := logger.NewFileLogger(filepath.Join(home, "nats.log"), true, true, false, true)
	b.server.SetLogger(l, true, false)
	go b.Lead(ctx, func(c context.Context) {
		_ = os.WriteFile(filepath.Join(home, "leading"), nil, 0600)
		<-c.Done()
		_ = os.Remove(filepath.Join(home, "leading"))
	})
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			marker := filepath.Join(home, "meta-leader")
			if b.server.JetStreamIsLeader() {
				_ = os.WriteFile(marker, nil, 0600)
			} else {
				_ = os.Remove(marker)
			}
		}
	}()
	<-ctx.Done()
	b.Close()
}

// TestBusFrozenLeaderLosesLease reproduces the production outage: SIGSTOP freezes a node
// process (it keeps its sockets, so it is not dead), and the two survivors must elect a new
// leader and keep JetStream writable. Each case freezes the node holding one marked role.
func TestBusFrozenLeaderLosesLease(t *testing.T) {
	if testing.Short() {
		t.Skip("freezes real COS NATS processes")
	}
	for _, tc := range []struct {
		name   string
		marker string // file the frozen node holds while it has the role
	}{
		{"freeze-lease-holder", "leading"},
		{"freeze-meta-leader", "meta-leader"},
	} {
		t.Run(tc.name, func(t *testing.T) { freezeCluster(t, tc.marker) })
	}
}

// freezeCluster runs one 3-process cluster under production-like load, freezes the node
// whose home holds marker, and checks that the survivors recover.
func freezeCluster(t *testing.T, marker string) {
	hosts := []string{"delta", "beta", "mariogk-dev"}
	ports := busTestPorts(t, 6)
	homes := make([]string, len(hosts))
	runtimes := make([]string, len(hosts))
	for i := range hosts {
		homes[i], runtimes[i] = t.TempDir(), t.TempDir()
	}
	procs := make([]*exec.Cmd, len(hosts))
	clients := make([]*Bus, len(hosts))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		for _, cmd := range procs {
			if cmd != nil {
				_ = cmd.Process.Signal(syscall.SIGCONT)
			}
		}
		for _, cmd := range procs {
			if cmd == nil {
				continue
			}
			_ = cmd.Process.Signal(syscall.SIGTERM)
			exited := make(chan struct{})
			go func() { _ = cmd.Wait(); close(exited) }()
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				<-exited
			}
		}
		for _, client := range clients {
			client.Close()
		}
	})
	for i, host := range hosts {
		var peers []string
		for j := range hosts {
			if j != i {
				peers = append(peers, strconv.Itoa(ports[j+3]))
			}
		}
		out, err := os.Create(filepath.Join(homes[i], "node.out"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestBusFreezeNode$", "-test.v")
		cmd.Env = append(os.Environ(), "COS_FREEZE_NODE="+host, "COS_FREEZE_HOME="+homes[i],
			"COS_FREEZE_RUNTIME="+runtimes[i], "COS_FREEZE_PORT="+strconv.Itoa(ports[i]),
			"COS_FREEZE_ROUTE="+strconv.Itoa(ports[i+3]), "COS_FREEZE_PEERS="+strings.Join(peers, ","))
		cmd.Stdout, cmd.Stderr = out, out
		err = cmd.Start()
		out.Close()
		if err != nil {
			t.Fatal(err)
		}
		procs[i] = cmd
	}
	for i, host := range hosts {
		if !freezeWait(30*time.Second, 100*time.Millisecond, func() bool {
			_, err := os.Stat(filepath.Join(runtimes[i], "cos", "nats.json"))
			return err == nil
		}) {
			t.Fatalf("node %s never wrote its bus credential", host)
		}
		client, err := ConnectBus(ctx, Config{Runtime: runtimes[i], LocalHost: host})
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = client
	}

	var leader string
	if !freezeWait(90*time.Second, 500*time.Millisecond, func() bool {
		var ok bool
		leader, ok = freezeLeader(ctx, clients)
		return ok
	}) {
		t.Fatal("the three clients never agreed on one leader within 90s")
	}
	leaderIdx := slices.Index(hosts, leader)

	js, err := clients[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "freeze_probe", Replicas: 3}); err != nil {
		t.Fatal(err)
	}
	probes := make([]jetstream.KeyValue, len(clients))
	for i, client := range clients {
		js, err := client.JetStream()
		if err != nil {
			t.Fatal(err)
		}
		if probes[i], err = js.KeyValue(ctx, "freeze_probe"); err != nil {
			t.Fatal(err)
		}
	}
	if !freezeWait(30*time.Second, 250*time.Millisecond, func() bool {
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := probes[0].Put(call, "k", []byte("before"))
		return err == nil
	}) {
		t.Fatal("freeze_probe never accepted a write before the freeze")
	}

	// Production-like load: 12 R3 buckets, written through every client until the case ends.
	const loadBuckets = 12
	for i := range loadBuckets {
		if _, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: fmt.Sprintf("load_%d", i), Replicas: 3}); err != nil {
			t.Fatal(err)
		}
	}
	loadCtx, stopLoad := context.WithCancel(ctx)
	var load sync.WaitGroup
	t.Cleanup(func() { stopLoad(); load.Wait() })
	for _, client := range clients {
		js, err := client.JetStream()
		if err != nil {
			t.Fatal(err)
		}
		kvs := make([]jetstream.KeyValue, loadBuckets)
		for j := range kvs {
			if kvs[j], err = js.KeyValue(ctx, fmt.Sprintf("load_%d", j)); err != nil {
				t.Fatal(err)
			}
		}
		load.Add(1)
		go func() {
			defer load.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-loadCtx.Done():
					return
				case <-ticker.C:
				}
				call, cancel := context.WithTimeout(loadCtx, 2*time.Second)
				_, _ = kvs[rand.IntN(loadBuckets)].Put(call, "k", []byte("load"))
				cancel()
			}
		}()
	}

	var frozenIdx int
	if !freezeWait(60*time.Second, 200*time.Millisecond, func() bool {
		marked := freezeMarked(homes, marker)
		if len(marked) == 1 {
			frozenIdx = marked[0]
		}
		return len(marked) == 1
	}) {
		t.Fatalf("expected exactly one node holding %s within 60s", marker)
	}
	frozen := hosts[frozenIdx]
	_, metaErr := os.Stat(filepath.Join(homes[frozenIdx], "meta-leader"))
	wasMeta := metaErr == nil

	var survivors []int
	sizes := make([]int64, len(hosts))
	accountJS := make([]jetstream.JetStream, len(hosts))
	logPath := func(i int) string { return filepath.Join(homes[i], "nats.log") }
	for i := range hosts {
		if i != frozenIdx {
			survivors = append(survivors, i)
			if accountJS[i], err = clients[i].JetStream(); err != nil {
				t.Fatal(err)
			}
		}
		sizes[i] = freezeLogSize(logPath(i))
	}

	pid := procs[frozenIdx].Process.Pid
	t0 := time.Now()
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	t.Logf("SIGSTOP sent to %s (pid %d) at t0; lease holder: %t; meta leader: %t", frozen, pid, frozenIdx == leaderIdx, wasMeta)

	var newLeader string
	var newLeaderAt, firstPutAt, firstAcctAt time.Time
	errs := map[string]int{}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for time.Since(t0) < 45*time.Second {
		var leaders []string
		for _, i := range survivors {
			call, cancel := context.WithTimeout(ctx, 2*time.Second)
			host, err := clients[i].Leader(call)
			cancel()
			if err != nil {
				errs[hosts[i]+" Leader: "+err.Error()]++
			} else {
				leaders = append(leaders, host)
			}

			call, cancel = context.WithTimeout(ctx, 2*time.Second)
			_, err = probes[i].Put(call, "k", []byte(time.Now().Format(time.RFC3339Nano)))
			cancel()
			if err != nil {
				errs[hosts[i]+" Put: "+err.Error()]++
			} else if firstPutAt.IsZero() {
				firstPutAt = time.Now()
			}

			call, cancel = context.WithTimeout(ctx, 2*time.Second)
			_, err = accountJS[i].AccountInfo(call)
			cancel()
			if err != nil {
				errs[hosts[i]+" AccountInfo: "+err.Error()]++
			} else if firstAcctAt.IsZero() {
				firstAcctAt = time.Now()
			}
		}
		if newLeaderAt.IsZero() && len(leaders) == len(survivors) && leaders[0] == leaders[1] && leaders[0] != frozen {
			newLeader, newLeaderAt = leaders[0], time.Now()
		}
		<-ticker.C
	}
	// The window ends at the first loop exit after 45s, which can run one poll past 45s.
	windowEnd := time.Since(t0)
	ends := make([]int64, len(hosts))
	for i := range hosts {
		ends[i] = freezeLogSize(logPath(i))
	}

	t1 := time.Now()
	if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	var stepDownAt, resumedLeaderAt time.Time
	freezeWait(15*time.Second, 250*time.Millisecond, func() bool {
		if stepDownAt.IsZero() {
			if _, err := os.Stat(filepath.Join(homes[frozenIdx], marker)); os.IsNotExist(err) {
				stepDownAt = time.Now()
			}
		}
		if resumedLeaderAt.IsZero() && newLeader != "" {
			call, cancel := context.WithTimeout(ctx, 2*time.Second)
			host, err := clients[frozenIdx].Leader(call)
			cancel()
			if err == nil && host == newLeader {
				resumedLeaderAt = time.Now()
			}
		}
		return !stepDownAt.IsZero() && !resumedLeaderAt.IsZero()
	})

	rel := func(at, from time.Time) string {
		if at.IsZero() {
			return "never"
		}
		return "+" + at.Sub(from).Round(time.Millisecond).String()
	}
	t.Logf("survivors agreed on new lease leader %q (not %s): %s after t0", newLeader, frozen, rel(newLeaderAt, t0))
	t.Logf("first successful Put on freeze_probe: %s after t0", rel(firstPutAt, t0))
	t.Logf("first successful AccountInfo: %s after t0", rel(firstAcctAt, t0))
	t.Logf("SIGCONT at t1; frozen node released %s: %s after t1; reads new leader: %s after t1",
		marker, rel(stepDownAt, t1), rel(resumedLeaderAt, t1))
	t.Logf("error histogram (survivor, op, error):")
	for _, key := range slices.Sorted(maps.Keys(errs)) {
		t.Logf("  %4d x %s", errs[key], key)
	}
	if len(errs) == 0 {
		t.Logf("  (no errors)")
	}
	for _, i := range survivors {
		t.Logf("timeline %s: nats.log lines in the first 45s after SIGSTOP (window ended +%s)", hosts[i], windowEnd.Round(time.Millisecond))
		for _, line := range freezeLogLines(logPath(i), sizes[i], ends[i], freezeTimelineLine) {
			t.Logf("first 45s %s: %s", hosts[i], line)
		}
	}

	if newLeaderAt.IsZero() || newLeaderAt.Sub(t0) > 30*time.Second {
		t.Errorf("survivors did not agree on a new lease leader within 30s of SIGSTOP (agreed %s)", rel(newLeaderAt, t0))
	}
	if firstPutAt.IsZero() || firstPutAt.Sub(t0) > 30*time.Second {
		t.Errorf("no successful JetStream Put within 30s of SIGSTOP (first success %s)", rel(firstPutAt, t0))
	}
	if firstAcctAt.IsZero() || firstAcctAt.Sub(t0) > 30*time.Second {
		t.Errorf("no successful JetStream AccountInfo within 30s of SIGSTOP (first success %s)", rel(firstAcctAt, t0))
	}
	if stepDownAt.IsZero() || stepDownAt.Sub(t1) > 10*time.Second {
		t.Errorf("old %s holder did not step down within 10s of SIGCONT (stepped down %s)", marker, rel(stepDownAt, t1))
	}
	if t.Failed() {
		for i, host := range hosts {
			lines := freezeLogLines(logPath(i), 0, -1, freezeDiagnosticLine)
			if len(lines) > 60 {
				lines = lines[len(lines)-60:]
			}
			for _, line := range lines {
				t.Logf("nats.log %s: %s", host, line)
			}
		}
	}
}

// freezeLeader returns the leader that every client reports, or false if a call fails or they disagree.
func freezeLeader(ctx context.Context, clients []*Bus) (string, bool) {
	var leader string
	for i, client := range clients {
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		host, err := client.Leader(call)
		cancel()
		if err != nil || (i > 0 && host != leader) {
			return "", false
		}
		leader = host
	}
	return leader, true
}

// freezeMarked returns the indexes of the nodes whose home holds marker.
func freezeMarked(homes []string, marker string) []int {
	var idx []int
	for i, home := range homes {
		if _, err := os.Stat(filepath.Join(home, marker)); err == nil {
			idx = append(idx, i)
		}
	}
	return idx
}

// freezeWait polls check every interval until it returns true or within has passed.
func freezeWait(within, interval time.Duration, check func() bool) bool {
	deadline := time.Now().Add(within)
	for {
		if check() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

func freezeLogSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// freezeLogLines returns the lines matching re in bytes [from, to) of path; a negative to reads to the end.
func freezeLogLines(path string, from, to int64, re *regexp.Regexp) []string {
	data, _ := os.ReadFile(path)
	if to < 0 || to > int64(len(data)) {
		to = int64(len(data))
	}
	if from > to {
		return nil
	}
	var lines []string
	for line := range strings.Lines(string(data[from:to])) {
		if re.MatchString(line) {
			lines = append(lines, strings.TrimRight(line, "\n"))
		}
	}
	return lines
}
