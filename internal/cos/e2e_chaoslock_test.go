package cos

import (
	"context"
	"errors"
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
	env := &E2EEnv{chaosLock: func(context.Context) (func(), error) { return nil, e2eSkip(holder) }}
	checks := []E2ECheck{check("cluster.restart", true), check("deploy.version", false), check("cluster.leader-freeze", true)}
	report := runE2E(t.Context(), env, checks, nil)
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
		if r.Status == "skip" && r.Evidence != holder {
			t.Fatalf("%s evidence = %q, want the holder %q", r.ID, r.Evidence, holder)
		}
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
