package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeHost struct {
	revision, good, reverted string
	trial, down              bool
	problems                 []string
}

// fakeFleet models hosts the way release.go and deploy/cos.service behave: a build in
// broken never gets healthy, so the watchdog puts the host back on its good build.
type fakeFleet struct {
	t        *testing.T
	hosts    map[string]*fakeHost
	broken   map[string]bool // host that cannot run the new build
	smokeErr error
	installs []string
}

func newFakeFleet(t *testing.T, revision string) *fakeFleet {
	f := &fakeFleet{t: t, hosts: map[string]*fakeHost{}, broken: map[string]bool{}}
	for _, h := range []string{"beta", "delta", "mariogk-dev"} {
		f.hosts[h] = &fakeHost{revision: revision, good: revision}
	}
	return f
}

func (f *fakeFleet) health(_ context.Context, host string) (NodeHealth, error) {
	h := f.hosts[host]
	if h.down {
		return NodeHealth{}, errors.New(host + " is not answering")
	}
	return NodeHealth{Build: Build{Host: host, Revision: h.revision, Good: h.good, Trial: h.trial},
		Problems: h.problems, Reverted: h.reverted}, nil
}

func (f *fakeFleet) release(_ context.Context, host, verb, revision string) error {
	h := f.hosts[host]
	switch verb {
	case "install":
		for name, other := range f.hosts {
			if other.trial && name != host {
				f.t.Fatalf("installing on %s while %s is mid-update", host, name)
			}
		}
		f.installs = append(f.installs, host)
		h.revision, h.trial, h.reverted = revision, true, ""
		if f.broken[host] {
			h.revision, h.trial, h.reverted = h.good, false, revision
		}
	case "commit":
		if h.revision != revision || !h.trial {
			return fmt.Errorf("build %.12s is not on trial on this host", revision)
		}
		h.good, h.trial = revision, false
	case "restore":
		h.revision, h.good, h.trial = revision, revision, false
	}
	return nil
}

func (f *fakeFleet) smoke(context.Context, string, string) error { return f.smokeErr }

// drive steps r to a terminal phase. On a handover the next leader is the first host
// that is neither the action's target nor on trial, as Bus.Resign and RunRollouts make it.
func drive(t *testing.T, f *fakeFleet, r *Rollout, leader string, now *time.Time) (events []rolloutEvent, leaders []string) {
	t.Helper()
	leaders = []string{leader}
	for i := 0; r.Active(); i++ {
		if i > 500 {
			t.Fatalf("rollout stuck in %s at step %d", r.Phase, r.Step)
		}
		event, err := step(context.Background(), f, r, leader, *now)
		if errors.Is(err, errHandover) {
			for _, h := range []string{"beta", "delta", "mariogk-dev"} {
				if h != leader && !f.hosts[h].trial && !f.hosts[h].down {
					leader = h
					break
				}
			}
			leaders = append(leaders, leader)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if event != nil {
			events = append(events, *event)
		}
		*now = now.Add(rolloutTick)
	}
	return events, leaders
}

func kinds(events []rolloutEvent) string {
	var out []string
	for _, e := range events {
		out = append(out, string(e.Kind)+"@"+e.Host)
	}
	return strings.Join(out, " ")
}

func TestRolloutCanaryOrderAndHandover(t *testing.T) {
	old, next := testRevision(1), testRevision(2)
	f := newFakeFleet(t, old)
	plan, err := planRollout(context.Background(), f, []string{"delta", "beta", "mariogk-dev"}, "delta", next)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(plan, ",") != "beta,mariogk-dev,delta" {
		t.Fatalf("plan = %v, want non-leaders first and the leader last", plan)
	}
	now := time.Now()
	r := newRollout(next, "Add a thing", plan, now)
	events, leaders := drive(t, f, &r, "delta", &now)
	if r.Phase != phaseCompleted {
		t.Fatalf("phase = %s (%s)", r.Phase, r.Failure)
	}
	if got := kinds(events); got != "node.updated@beta node.updated@mariogk-dev node.updated@delta" {
		t.Fatalf("events = %s", got)
	}
	if strings.Join(f.installs, ",") != "beta,mariogk-dev,delta" {
		t.Fatalf("installs = %v", f.installs)
	}
	if len(leaders) != 2 || leaders[1] == "delta" {
		t.Fatalf("leaders = %v, want delta to hand over to an updated node before its update", leaders)
	}
	for name, h := range f.hosts {
		if h.revision != next || h.good != next || h.trial {
			t.Fatalf("%s = %+v", name, h)
		}
	}
	if r.From["beta"] != old || r.From["delta"] != old {
		t.Fatalf("from = %v", r.From)
	}
	if again, err := planRollout(context.Background(), f, []string{"delta", "beta", "mariogk-dev"}, "beta", next); err != nil || len(again) != 0 {
		t.Fatalf("second plan = %v, %v; every host already runs it", again, err)
	}
}

func TestRolloutGateFailureAtEachStep(t *testing.T) {
	order := []string{"beta", "mariogk-dev", "delta"}
	for failAt, bad := range order {
		t.Run(bad, func(t *testing.T) {
			old, next := testRevision(1), testRevision(2)
			f := newFakeFleet(t, old)
			f.broken[bad] = true
			now := time.Now()
			r := newRollout(next, "Broken change", order, now)
			events, _ := drive(t, f, &r, "delta", &now)
			if r.Phase != phaseRolledBack || !strings.HasPrefix(r.Failure, bad+": the watchdog reverted it") {
				t.Fatalf("phase=%s failure=%q", r.Phase, r.Failure)
			}
			var want []string
			for _, h := range order[:failAt] {
				want = append(want, "node.updated@"+h)
			}
			want = append(want, "gate.failed@"+bad)
			if got := kinds(events); got != strings.Join(want, " ") {
				t.Fatalf("events = %s, want %s", got, strings.Join(want, " "))
			}
			if strings.Join(f.installs, ",") != strings.Join(order[:failAt+1], ",") {
				t.Fatalf("installs = %v; hosts after the failing one must stay untouched", f.installs)
			}
			for name, h := range f.hosts {
				if h.revision != old || h.good != old || h.trial {
					t.Fatalf("%s not back on the previous build: %+v", name, h)
				}
			}
		})
	}
}

func TestRolloutGateTimeouts(t *testing.T) {
	old, next := testRevision(1), testRevision(2)
	for _, tc := range []struct {
		name, want string
		break_     func(f *fakeFleet)
	}{
		{"unhealthy", "JetStream not in sync", func(f *fakeFleet) { f.hosts["beta"].problems = []string{"JetStream not in sync"} }},
		{"smoke", "smoke: COS operations service runs", func(f *fakeFleet) { f.smokeErr = errors.New("COS operations service runs 111111111111") }},
		{"silent", "not answering", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeFleet(t, old)
			now := time.Now()
			r := newRollout(next, "Change", []string{"beta", "delta"}, now)
			if _, err := step(context.Background(), f, &r, "delta", now); err != nil || r.Phase != phaseGate {
				t.Fatalf("install step: %v, phase %s", err, r.Phase)
			}
			if tc.break_ != nil {
				tc.break_(f)
			} else {
				f.hosts["beta"].down = true
			}
			start := now
			for r.Phase == phaseGate {
				now = now.Add(rolloutTick)
				if _, err := step(context.Background(), f, &r, "delta", now); err != nil {
					t.Fatal(err)
				}
			}
			if waited := now.Sub(start); waited <= gateTimeout || waited > gateTimeout+2*rolloutTick {
				t.Fatalf("gate failed after %s, want just over %s", waited, gateTimeout)
			}
			if !strings.Contains(r.Failure, tc.want) {
				t.Fatalf("failure %q does not say %q", r.Failure, tc.want)
			}
			f.hosts["beta"].down, f.hosts["beta"].problems, f.smokeErr = false, nil, nil
			drive(t, f, &r, "delta", &now)
			if h := f.hosts["beta"]; r.Phase != phaseRolledBack || h.revision != old || h.good != old || h.trial {
				t.Fatalf("phase=%s beta=%+v", r.Phase, h)
			}
		})
	}
}

func TestPlanRolloutRefusesUnsafeCluster(t *testing.T) {
	old, next := testRevision(1), testRevision(2)
	hosts := []string{"beta", "delta", "mariogk-dev"}
	for _, tc := range []struct {
		name   string
		break_ func(h *fakeHost)
	}{
		{"down", func(h *fakeHost) { h.down = true }},
		{"unhealthy", func(h *fakeHost) { h.problems = []string{"not a NATS cluster member"} }},
		{"on trial", func(h *fakeHost) { h.trial = true }},
	} {
		f := newFakeFleet(t, old)
		tc.break_(f.hosts["mariogk-dev"])
		if plan, err := planRollout(context.Background(), f, hosts, "delta", next); err == nil {
			t.Fatalf("%s: planned %v with mariogk-dev %s", tc.name, plan, tc.name)
		}
	}
	f := newFakeFleet(t, old)
	f.hosts["beta"].revision, f.hosts["beta"].good = next, next
	if plan, err := planRollout(context.Background(), f, hosts, "delta", next); err != nil || strings.Join(plan, ",") != "mariogk-dev,delta" {
		t.Fatalf("plan = %v, %v; only lagging hosts, leader last", plan, err)
	}
}

func TestRolloutResumesAfterLeaderLoss(t *testing.T) {
	old, next := testRevision(1), testRevision(2)
	f := newFakeFleet(t, old)
	now := time.Now()
	r := newRollout(next, "Change", []string{"beta", "mariogk-dev", "delta"}, now)
	if _, err := step(context.Background(), f, &r, "delta", now); err != nil || r.Phase != phaseGate {
		t.Fatal(err)
	}
	// delta dies mid-gate; mariogk-dev takes over the stored record and finishes it.
	events, _ := drive(t, f, &r, "mariogk-dev", &now)
	if r.Phase != phaseCompleted || kinds(events) != "node.updated@beta node.updated@mariogk-dev node.updated@delta" {
		t.Fatalf("phase=%s events=%s", r.Phase, kinds(events))
	}
	if strings.Count(strings.Join(f.installs, ","), "beta") != 1 {
		t.Fatalf("installs = %v; a resumed gate must not reinstall", f.installs)
	}
}

func TestConcurrentRolloutsRefused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, buses := releaseCluster(t, ctx)
	js, err := buses[1].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	var kv jetstream.KeyValue
	busTestReady(t, "deploy KV", func() bool { kv, err = deployKV(ctx, js); return err == nil })
	var wg sync.WaitGroup
	results := make([]error, 8)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = startRollout(ctx, kv, newRollout(testRevision(i%9+1), "Change", []string{"beta"}, time.Now()))
		}()
	}
	wg.Wait()
	started := 0
	for _, err := range results {
		if err == nil {
			started++
		} else if !strings.Contains(err.Error(), "one rollout at a time") {
			t.Fatal(err)
		}
	}
	if started != 1 {
		t.Fatalf("%d concurrent rollouts started, want exactly 1", started)
	}
	r, revision, err := loadRollout(ctx, kv)
	if err != nil || !r.Active() {
		t.Fatalf("stored rollout %+v, %v", r, err)
	}
	r.Phase = phaseCompleted
	if _, err := saveRollout(ctx, kv, r, revision); err != nil {
		t.Fatal(err)
	}
	if err := startRollout(ctx, kv, newRollout(testRevision(3), "Next", []string{"beta"}, time.Now())); err != nil {
		t.Fatalf("a rollout after the last one completed: %v", err)
	}
}

func TestDeployEventsValidate(t *testing.T) {
	r := newRollout(testRevision(2), "Fix\nthe \x1b[31mthing", []string{"beta", "delta"}, time.Now())
	r.From = map[string]string{"beta": testRevision(1)}
	for _, e := range []rolloutEvent{{Kind: KindDeployStarted}, {Kind: KindNodeUpdated, Host: "beta"},
		{Kind: KindGateFailed, Host: "delta", Reason: "the watchdog reverted it"},
		{Kind: KindDeployRolledBack, Reason: strings.Repeat("x", 5000)}, {Kind: KindDeployCompleted}} {
		ev := deployEvent(r, e, time.Now())
		if err := ev.Validate(); err != nil {
			t.Fatalf("%s: %v (%q)", e.Kind, err, ev.Summary)
		}
		if ev.Subject() != "cos.events.deploy."+string(e.Kind) {
			t.Fatalf("subject %s", ev.Subject())
		}
	}
}

// The rollout build checks out a revision, stamps it and stores a binary that reports it.
func TestSourceBuildsStampedRevision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A one-commit copy of this tree, so shallow or detached CI checkouts work as origin.
	origin := t.TempDir()
	if out, err := exec.Command("sh", "-c", `git -C ../.. archive HEAD | tar -x -C "$1" && cd "$1" && git init -q -b main &&
		git add -A && git -c user.name=test -c user.email=test@example.com commit -qm "Test change"`, "sh", origin).CombinedOutput(); err != nil {
		t.Fatalf("origin: %v: %s", err, out)
	}
	cfg := Config{Home: t.TempDir()}
	src := sourceOf(cfg)
	if err := os.MkdirAll(src.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"remote", "add", "origin", origin}} {
		if _, err := src.run(ctx, "git", args...); err != nil {
			t.Fatal(err)
		}
	}
	head, err := exec.Command("git", "-C", origin, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	revision, change, err := src.resolve(ctx, "main")
	if err != nil || revision != strings.TrimSpace(string(head)) || change != "Test change" {
		t.Fatalf("resolve = %s %q %v", revision, change, err)
	}
	if _, _, err := src.resolve(ctx, "no-such-branch"); err == nil {
		t.Fatal("resolved a missing branch")
	}
	_, buses := releaseCluster(t, ctx)
	js, _ := buses[0].JetStream()
	busTestReady(t, "cluster meta leader", func() bool { _, err := deployKV(ctx, js); return err == nil })
	if err := src.build(ctx, js, revision, change); err != nil {
		t.Fatal(err)
	}
	store, _ := js.ObjectStore(ctx, buildsBucket)
	info, err := store.GetInfo(ctx, revision)
	if err != nil || info.Description != change || info.Size < 1<<20 {
		t.Fatalf("stored build %+v, %v", info, err)
	}
	if info.Metadata["rollout"] != strconv.Itoa(rolloutProtocol) {
		t.Fatalf("stored build level %q", info.Metadata["rollout"])
	}
}

func TestBuildErrorsAreRecognised(t *testing.T) {
	err := fmt.Errorf("rollout: %w", buildError{errors.New("build 2c4693dfd1de does not start")})
	if !errors.As(err, new(buildError)) || errors.As(errors.New("beta is unhealthy"), new(buildError)) {
		t.Fatal("only build failures may mark a revision failed")
	}
}

func TestRolledBackFYI(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, buses := releaseCluster(t, ctx)
	r := newRollout(testRevision(2), "Group the Inbox by ticket", []string{"beta", "delta"}, time.Now())
	r.From = map[string]string{"delta": testRevision(1), "beta": testRevision(1)}
	r.Failure = "beta: the watchdog reverted it to its last-known-good build"
	var err error
	busTestReady(t, "Inbox FYI", func() bool { err = rolledBackFYI(ctx, buses[0], "delta", r); return err == nil })
	if err := rolledBackFYI(ctx, buses[1], "beta", r); err != nil {
		t.Fatalf("repeat from a resumed leader: %v", err)
	}
	inbox, err := OpenInbox(ctx, buses[2], "mariogk-dev", false)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := inbox.kv.Get(ctx, inboxID("deploy:"+r.ID))
	if err != nil {
		t.Fatal(err)
	}
	var item InboxItem
	if err := json.Unmarshal(entry.Value(), &item); err != nil {
		t.Fatal(err)
	}
	if item.Kind != "fyi" || item.Source != "deploy" || item.Status != "open" || item.Title != "Deploy rolled back: Group the Inbox by ticket" ||
		!strings.Contains(item.Context, "beta on 111111111111, delta on 111111111111") || !strings.Contains(item.Context, "watchdog reverted") {
		t.Fatalf("FYI = %+v", item)
	}
	if keys, _ := inbox.kv.Keys(ctx); len(keys) != 1 {
		t.Fatalf("Inbox keys %v; the repeat must not add a second item", keys)
	}
}

func TestOnlyTheHostsOwnSmokeChecksGate(t *testing.T) {
	report := E2EReport{Checks: []E2EResult{
		{ID: "deploy.version.beta", Status: "pass"},
		{ID: "deploy.logs.delta", Status: "fail", Evidence: "another host"},
		{ID: "rules.heartbeat-leader", Status: "fail", Evidence: "cluster-wide, failing before this build"},
		{ID: "access.login.public", Status: "skip"},
		{ID: "deploy.logs.beta", Status: "fail", Evidence: "1 error line since start: ERROR mirror events context deadline exceeded"},
	}}
	if err := hostSmokeFailure(report, "beta"); err != nil {
		t.Fatalf("gate failed on another host's or a cluster-wide check: %v", err)
	}
	report.Checks[0].Status, report.Checks[0].Evidence = "fail", "beta runs 111111111111; want 222222222222"
	if err := hostSmokeFailure(report, "beta"); err == nil || !strings.Contains(err.Error(), "deploy.version.beta") {
		t.Fatalf("gate passed with the host's own version check failing: %v", err)
	}
}

func TestCanRollOutNeedsTheRolloutLevel(t *testing.T) {
	dir := t.TempDir()
	script := func(name, out string) string {
		path := dir + "/" + name
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho '"+out+"'\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if _, err := canRollOut(context.Background(), script("current", fmt.Sprintf(`{"protocol":1,"rollout":%d}`, rolloutProtocol))); err != nil {
		t.Fatalf("build at this rollout level refused: %v", err)
	}
	if _, err := canRollOut(context.Background(), script("older", `{"protocol":1}`)); err == nil || !strings.Contains(err.Error(), "rollout controller level 0") {
		t.Fatalf("build with an older controller accepted: %v", err)
	}
	if _, err := canRollOut(context.Background(), script("broken", `not json`)); err == nil {
		t.Fatal("build with unreadable version accepted")
	}
}

// During a merge train the head may be red or still in CI: the newest commit not found red deploys, never anything older.
func TestNewestDeployableMainSkipsRedAndFailed(t *testing.T) {
	head, failing, marked, green, deployed, older := testRevision(6), testRevision(5), testRevision(4), testRevision(3), testRevision(2), testRevision(1)
	state := map[string]string{head: "failure", failing: "error", marked: "pending", green: "pending", deployed: "success", older: "success"}
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/commits") {
			fmt.Fprintf(w, `[{"sha":%q,"commit":{"committer":{"date":"2026-10-11T06:00:00Z"}}},{"sha":%q,"commit":{"committer":{"date":"2026-10-11T05:00:00Z"}}},`+
				`{"sha":%q,"commit":{"committer":{"date":"2026-10-11T04:00:00Z"}}},{"sha":%q,"commit":{"committer":{"date":"2026-10-11T03:00:00Z"}}},`+
				`{"sha":%q,"commit":{"committer":{"date":"2026-10-11T02:00:00Z"}}},{"sha":%q,"commit":{"committer":{"date":"2026-10-11T01:00:00Z"}}}]`, head, failing, marked, green, deployed, older)
			return
		}
		sha := path.Base(strings.TrimSuffix(r.URL.Path, "/status"))
		fmt.Fprintf(w, `{"state":%q,"total_count":3}`, state[sha])
	})
	failed := func(revision string) bool { return revision == marked }
	got, err := newestDeployableMain(context.Background(), f.Config, deployed, time.Time{}, failed)
	if err != nil || got != green {
		t.Fatalf("newest deployable = %.12s, %v; want %.12s (head red, next error, next marked failed, green still pending)", got, err, green)
	}
	state[green] = "failure"
	if got, err := newestDeployableMain(context.Background(), f.Config, deployed, time.Time{}, failed); err != nil || got != deployed {
		t.Fatalf("got %.12s, %v; with nothing newer deployable it must stop at the deployed build, not pick an older one", got, err)
	}
	state[deployed], state[older] = "failure", "failure"
	if _, err := newestDeployableMain(context.Background(), f.Config, "", time.Time{}, failed); err == nil {
		t.Fatal("found a deployable commit where every commit is red")
	}
	// The running build is 02:30: green (03:00) is newer and deploys while it is not red; once it is red the walk
	// must stop there, not reach deployed (02:00, deployable) or older.
	notBefore := time.Date(2026, 10, 11, 2, 30, 0, 0, time.UTC)
	state[green], state[deployed] = "pending", "success"
	if got, err := newestDeployableMain(context.Background(), f.Config, "", notBefore, failed); err != nil || got != green {
		t.Fatalf("got %.12s, %v; want the deployable commit newer than the running build", got, err)
	}
	state[green] = "failure"
	if _, err := newestDeployableMain(context.Background(), f.Config, "", notBefore, failed); err == nil {
		t.Fatal("walked back to a commit older than the running build")
	}
}

// A merge train rolls out only once main has been quiet for mainQuiet.
func TestMainSettlesBeforeRollingOut(t *testing.T) {
	tip, deployed := testRevision(2), testRevision(1)
	var tipAt time.Time
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/commits") {
			fmt.Fprintf(w, `[{"sha":%q,"commit":{"committer":{"date":%q}}},{"sha":%q,"commit":{"committer":{"date":"2026-10-11T01:00:00Z"}}}]`, tip, tipAt.Format(time.RFC3339), deployed)
			return
		}
		fmt.Fprint(w, `{"state":"pending","total_count":1}`)
	})
	ctx, failed, now := context.Background(), func(string) bool { return false }, time.Now()
	tipAt = now.Add(-10 * time.Second)
	var settling mainSettling
	if _, err := newestDeployableMain(ctx, f.Config, deployed, time.Time{}, failed); !errors.As(err, &settling) || time.Duration(settling) < 45*time.Second || time.Duration(settling) > 60*time.Second {
		t.Fatalf("err = %v; want mainSettling with 45 to 60 s left", err)
	}
	tipAt = now.Add(-2 * time.Minute)
	if got, err := newestDeployableMain(ctx, f.Config, deployed, time.Time{}, failed); err != nil || got != tip {
		t.Fatalf("quiet main: got %.12s, %v; want the tip", got, err)
	}
	tipAt = now.Add(-10 * time.Second)
	if got, err := newestDeployableMain(ctx, f.Config, tip, time.Time{}, failed); err != nil || got != tip {
		t.Fatalf("cluster already runs the tip: got %.12s, %v; want no wait", got, err)
	}
}

// Main moving during a rollout stops it after the host it is on, and the next rollout skips the intermediate build.
func TestRolloutCoalescesWhenMainMoves(t *testing.T) {
	a, b := testRevision(2), testRevision(3)
	f := newFakeFleet(t, testRevision(1))
	now := time.Now()
	plan, err := planRollout(context.Background(), f, []string{"delta", "beta", "mariogk-dev"}, "delta", a)
	if err != nil {
		t.Fatal(err)
	}
	r := newRollout(a, "A", plan, now)
	r.Main = true
	copied := r
	if coalesce(&copied, a, nil, now) || coalesce(&copied, "", errors.New("forgejo down"), now) {
		t.Fatal("coalesced while main was still at the rollout's build, or unknown")
	}
	for i := 0; ; i++ {
		if i > 500 {
			t.Fatal("rollout never updated a host")
		}
		event, err := step(context.Background(), f, &r, "delta", now)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(rolloutTick)
		if event != nil {
			if event.Kind != KindNodeUpdated {
				t.Fatalf("event = %s", kinds([]rolloutEvent{*event}))
			}
			break
		}
	}
	if !coalesce(&r, b, nil, now) {
		t.Fatal("main moved on but the rollout was not superseded")
	}
	if r.Phase != phaseSuperseded || r.Active() || r.Step != 1 || strings.Join(f.installs, ",") != "beta" {
		t.Fatalf("phase %s, step %d, installs %v", r.Phase, r.Step, f.installs)
	}
	settling := newRollout(a, "A", plan, now)
	if !coalesce(&settling, a, mainSettling(time.Second), now) {
		t.Fatal("a settling main did not supersede")
	}
	plan2, err := planRollout(context.Background(), f, []string{"delta", "beta", "mariogk-dev"}, "delta", b)
	if err != nil {
		t.Fatal(err)
	}
	r2 := newRollout(b, "B", plan2, now)
	drive(t, f, &r2, "delta", &now)
	if r2.Phase != phaseCompleted {
		t.Fatalf("phase = %s (%s)", r2.Phase, r2.Failure)
	}
	for name, h := range f.hosts {
		if h.revision != b {
			t.Fatalf("%s runs %.12s, want %.12s", name, h.revision, b)
		}
	}
	if got := strings.Join(f.installs, ","); got != "beta,beta,mariogk-dev,delta" {
		t.Fatalf("installs = %s; mariogk-dev and delta must not get the intermediate build", got)
	}
}

// A gate that timed out (a busy cluster) is retried with backoff; a broken build never is.
func TestRolloutRetriesAfterAGateTimeout(t *testing.T) {
	now := time.Now()
	timedOut := Rollout{To: testRevision(2), Failure: "beta: not healthy within 2m0s: JetStream not in sync"}
	f := afterRollback(failedRevision{}, timedOut, now)
	if f.Permanent || f.Attempts != 1 || !f.RetryAt.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("first timeout: %+v", f)
	}
	value, _ := json.Marshal(f)
	if !failedBlocks(value, now.Add(time.Minute)) || failedBlocks(value, now.Add(6*time.Minute)) {
		t.Fatal("a retry must wait for its backoff and then be allowed")
	}
	f = afterRollback(f, timedOut, now)
	if f.Permanent || f.Attempts != 2 || !f.RetryAt.Equal(now.Add(15*time.Minute)) || !strings.Contains(f.outlook(), "attempt 3 of 3") {
		t.Fatalf("second timeout: %+v %s", f, f.outlook())
	}
	if f = afterRollback(f, timedOut, now); !f.Permanent {
		t.Fatalf("third timeout must stop retrying: %+v", f)
	}
	value, _ = json.Marshal(f)
	if !failedBlocks(value, now.Add(24*time.Hour)) || !strings.Contains(f.outlook(), "not retried") {
		t.Fatal("an exhausted revision must stay blocked")
	}
	broken := Rollout{To: testRevision(3), Failure: "beta: the watchdog reverted it", Broken: true}
	if f := afterRollback(failedRevision{}, broken, now); !f.Permanent {
		t.Fatalf("a build the watchdog reverted must never be retried: %+v", f)
	}
	if !failedBlocks([]byte("delta: the watchdog reverted it"), now) {
		t.Fatal("plain-text records from before retries must keep blocking")
	}
}

// One healthy reading is not enough: a node catching up after a restart can flap.
func TestGateNeedsTwoHealthyReadings(t *testing.T) {
	old, next := testRevision(1), testRevision(2)
	f := newFakeFleet(t, old)
	now := time.Now()
	r := newRollout(next, "Change", []string{"beta", "delta"}, now)
	if _, err := step(context.Background(), f, &r, "delta", now); err != nil || r.Phase != phaseGate {
		t.Fatal(err)
	}
	for i, problems := range [][]string{nil, {"JetStream not in sync"}, nil} {
		f.hosts["beta"].problems = problems
		now = now.Add(rolloutTick)
		if event, err := step(context.Background(), f, &r, "delta", now); err != nil || event != nil || r.Step != 0 {
			t.Fatalf("reading %d: event=%v step=%d err=%v; a flapping host must not pass", i, event, r.Step, err)
		}
	}
	now = now.Add(rolloutTick)
	if event, err := step(context.Background(), f, &r, "delta", now); err != nil || event == nil || event.Kind != KindNodeUpdated {
		t.Fatalf("second consecutive healthy reading must pass: %v %v", event, err)
	}
}

func TestRecordPollRoundTrips(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, buses := releaseCluster(t, ctx)
	js, _ := buses[0].JetStream()
	var kv jetstream.KeyValue
	var err error
	busTestReady(t, "deploy KV", func() bool { kv, err = deployKV(ctx, js); return err == nil })
	recordPoll(ctx, kv, "beta", testRevision(4), "waiting: delta is on trial")
	e, err := kv.Get(ctx, pollKey)
	if err != nil {
		t.Fatal(err)
	}
	var p pollNote
	if json.Unmarshal(e.Value(), &p) != nil || p.Leader != "beta" || p.Candidate != testRevision(4) || p.Reason != "waiting: delta is on trial" || p.At.IsZero() {
		t.Fatalf("poll note %s", e.Value())
	}
}
