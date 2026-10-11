package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	}}
	if err := hostSmokeFailure(report, "beta"); err != nil {
		t.Fatalf("gate failed on another host's or a cluster-wide check: %v", err)
	}
	report.Checks[0].Status, report.Checks[0].Evidence = "fail", "beta runs 111111111111; want 222222222222"
	if err := hostSmokeFailure(report, "beta"); err == nil || !strings.Contains(err.Error(), "deploy.version.beta") {
		t.Fatalf("gate passed with the host's own version check failing: %v", err)
	}
}

func TestCanRollOutNeedsTheDeployCommand(t *testing.T) {
	dir := t.TempDir()
	script := func(name, out string) string {
		path := dir + "/" + name
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho '"+out+"'\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if err := canRollOut(context.Background(), script("with", `{"commands":["fleet","deploy [<branch|commit>|status]"]}`)); err != nil {
		t.Fatalf("build with cos deploy refused: %v", err)
	}
	if err := canRollOut(context.Background(), script("without", `{"commands":["fleet","version"]}`)); err == nil || !strings.Contains(err.Error(), "rollout controller") {
		t.Fatalf("build without cos deploy accepted: %v", err)
	}
	if err := canRollOut(context.Background(), script("broken", `not json`)); err == nil {
		t.Fatal("build with unreadable help accepted")
	}
}
