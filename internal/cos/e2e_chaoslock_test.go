package cos

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestChaosLockBlocks(t *testing.T) {
	now := time.Date(2026, 10, 11, 7, 10, 0, 0, time.UTC)
	tests := []struct {
		name string
		cur  *chaosLock
		want bool
	}{
		{"free", nil, false},
		{"held fresh", &chaosLock{At: now.Add(-9 * time.Minute)}, true},
		{"held stale", &chaosLock{At: now.Add(-11 * time.Minute)}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := chaosLockBlocks(tc.cur, now); got != tc.want {
				t.Fatalf("chaosLockBlocks = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHeldChaosLockSkipsOnlyChaos(t *testing.T) {
	holder := "another e2e run on delta (pid 42) holds the chaos lock since 2026-10-11T07:00:00Z; not perturbing"
	ran := map[string]bool{}
	check := func(id string, chaos bool) E2ECheck {
		return E2ECheck{ID: id, Stream: "TS-816", Chaos: chaos, Run: func(context.Context, *E2EEnv) (string, error) {
			ran[id] = true
			return "ok", nil
		}}
	}
	env := &E2EEnv{chaosLock: func(context.Context) (func(), error) { return nil, chaosHeld(holder) }}
	checks := []E2ECheck{check("cluster.restart", true), check("deploy.version", false), check("cluster.leader-freeze", true)}
	// The run deadline ends the lock wait early, so the test does not wait the full 5 minutes.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	report := runE2E(ctx, env, checks, nil)
	if ran["cluster.restart"] || ran["cluster.leader-freeze"] {
		t.Fatal("a chaos check ran while another run held the lock")
	}
	if !ran["deploy.version"] {
		t.Fatal("non-chaos check did not run while the lock was held")
	}
	want := map[string]string{"cluster.restart": "skip", "deploy.version": "pass", "cluster.leader-freeze": "skip"}
	for _, r := range report.Checks {
		if r.Status != want[r.ID] {
			t.Fatalf("%s status = %s, want %s", r.ID, r.Status, want[r.ID])
		}
		if r.Status == "skip" && !strings.HasPrefix(r.Evidence, holder+", after waiting ") {
			t.Fatalf("%s evidence = %q, want the holder %q after waiting", r.ID, r.Evidence, holder)
		}
	}
}

func TestChaosLockWaitsForFreedLock(t *testing.T) {
	calls := 0
	env := &E2EEnv{chaosLock: func(context.Context) (func(), error) {
		calls++
		if calls < 3 {
			return nil, chaosHeld("holder")
		}
		return func() {}, nil
	}}
	l := &chaosLockRun{env: env, budget: time.Second, poll: time.Millisecond}
	if err := l.take(t.Context()); err != nil {
		t.Fatalf("take = %v, want the lock once the holder let go", err)
	}
	if calls != 3 || env.waited == 0 {
		t.Fatalf("calls=%d waited=%s, want 3 tries and a recorded wait", calls, env.waited)
	}
}

func TestChaosLockSkipsAfterWaitingForHolder(t *testing.T) {
	env := &E2EEnv{chaosLock: func(context.Context) (func(), error) { return nil, chaosHeld("holder") }}
	l := &chaosLockRun{env: env, budget: 20 * time.Millisecond, poll: time.Millisecond}
	var skip e2eSkip
	if err := l.take(t.Context()); !errors.As(err, &skip) || !strings.HasPrefix(err.Error(), "holder, after waiting ") {
		t.Fatalf("take = %v, want a skip naming the holder after waiting", err)
	}
}

func TestChaosLockUnavailableIsNotWaitedFor(t *testing.T) {
	calls := 0
	env := &E2EEnv{chaosLock: func(context.Context) (func(), error) {
		calls++
		return nil, chaosLockUnavailable(errors.New("no bus"))
	}}
	l := &chaosLockRun{env: env, budget: time.Second, poll: time.Millisecond}
	var skip e2eSkip
	if err := l.take(t.Context()); !errors.As(err, &skip) || calls != 1 {
		t.Fatalf("take = %v after %d tries, want one skip with no wait", err, calls)
	}
}

func TestWaitRolloutProceedsWhenRolloutSettles(t *testing.T) {
	now := time.Date(2026, 10, 11, 7, 10, 0, 0, time.UTC)
	looks := []Rollout{{ID: "r1", Phase: phaseGate, Since: now}, {ID: "r1", Phase: phaseCompleted, Since: now.Add(-3 * time.Minute)}}
	calls := 0
	look := func() (Rollout, error) {
		calls++
		return looks[min(calls, len(looks))-1], nil
	}
	r, waited, err := waitRollout(t.Context(), time.Second, time.Millisecond, look, func() time.Time { return now })
	if err != nil || rolloutHold(r, now) != "" || waited == 0 {
		t.Fatalf("waitRollout = %+v waited %s err %v, want the settled rollout after a wait", r, waited, err)
	}
}

func TestWaitRolloutSkipsWhenStillActive(t *testing.T) {
	now := time.Date(2026, 10, 11, 7, 10, 0, 0, time.UTC)
	look := func() (Rollout, error) { return Rollout{ID: "r1", Phase: phaseInstall, Since: now}, nil }
	r, waited, err := waitRollout(t.Context(), 20*time.Millisecond, time.Millisecond, look, func() time.Time { return now })
	if err != nil || rolloutHold(r, now) == "" || waited < 20*time.Millisecond {
		t.Fatalf("waitRollout = %+v waited %s err %v, want the active rollout still holding after the budget", r, waited, err)
	}
}

func TestChaosLockTakenOnceAndReleasedOnError(t *testing.T) {
	taken, released := 0, 0
	env := &E2EEnv{chaosLock: func(context.Context) (func(), error) {
		taken++
		return func() { released++ }, nil
	}}
	fail := E2ECheck{ID: "cluster.restart", Stream: "TS-810", Chaos: true, Run: func(context.Context, *E2EEnv) (string, error) {
		return "", errors.New("boom")
	}}
	pass := E2ECheck{ID: "cluster.partition", Stream: "TS-810", Chaos: true, Run: func(context.Context, *E2EEnv) (string, error) {
		return "ok", nil
	}}
	runE2E(t.Context(), env, []E2ECheck{fail, pass}, nil)
	if taken != 1 || released != 1 {
		t.Fatalf("taken=%d released=%d, want 1 and 1 for two chaos checks in one run", taken, released)
	}
	runE2E(t.Context(), env, []E2ECheck{{ID: "deploy.version", Stream: "TS-810", Run: pass.Run}}, nil)
	if taken != 1 {
		t.Fatalf("taken=%d after a run with no chaos check, want no acquisition", taken)
	}
}
