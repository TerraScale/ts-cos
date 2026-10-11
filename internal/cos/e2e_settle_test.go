package cos

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

// settledHosts is every host active, running, on one build and answering /login with 200.
func settledHosts(build string) []settleHost {
	var hosts []settleHost
	for _, h := range e2eHosts {
		hosts = append(hosts, settleHost{Name: h, Build: build, Unit: "active/running", Login: 200})
	}
	return hosts
}

func withHost(hosts []settleHost, i int, change func(*settleHost)) []settleHost {
	hosts = slices.Clone(hosts)
	change(&hosts[i])
	return hosts
}

func TestIsSettled(t *testing.T) {
	const build = "47f99316a8e0abcdef"
	hosts := settledHosts(build)
	tests := []struct {
		name  string
		r     Rollout
		hosts []settleHost
		want  bool
	}{
		{"all same build, active, login 200", Rollout{}, hosts, true},
		{"completed rollout is not active", Rollout{ID: "r1", Phase: phaseCompleted}, hosts, true},
		{"one host activating", Rollout{}, withHost(hosts, 1, func(h *settleHost) { h.Unit = "activating/start" }), false},
		{"different builds", Rollout{}, withHost(hosts, 2, func(h *settleHost) { h.Build = "0123456789ab" }), false},
		{"login 503", Rollout{}, withHost(hosts, 0, func(h *settleHost) { h.Login = 503 }), false},
		{"login refused", Rollout{}, withHost(hosts, 0, func(h *settleHost) { h.Login, h.Err = 0, "connection refused" }), false},
		{"unknown build", Rollout{}, withHost(hosts, 0, func(h *settleHost) { h.Build = "" }), false},
		{"rollout gate", Rollout{ID: "r1", Phase: phaseGate}, hosts, false},
		{"rollout install", Rollout{ID: "r1", Phase: phaseInstall}, hosts, false},
		{"rollout rollback", Rollout{ID: "r1", Phase: phaseRollback}, hosts, false},
		{"one host missing", Rollout{}, hosts[:2], false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSettled(tc.r, tc.hosts); got != tc.want {
				t.Fatalf("isSettled = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestSettledResult(t *testing.T) {
	settled := settledResult(settleState{Settled: true, Waited: 42 * time.Second, Hosts: settledHosts("47f99316a8e0abcdef")}, nil)
	if settled.ID != "cluster.settled" || settled.Group != "cluster" || settled.Stream != "TS-816" || settled.Status != "pass" {
		t.Fatalf("settled result = %+v", settled)
	}
	if want := "settled after 42s: build 47f99316a8e0 on all nodes, no rollout in progress"; settled.Evidence != want {
		t.Fatalf("evidence = %q, want %q", settled.Evidence, want)
	}

	hosts := settledHosts("aaa")
	hosts[0] = settleHost{Name: "beta", Build: "aaa", Unit: "activating/start", Login: 503}
	unsettled := settledResult(settleState{Rollout: "r9", Phase: phaseGate, Waited: 5 * time.Minute, Hosts: hosts}, nil)
	if unsettled.Status != "fail" {
		t.Fatalf("status = %q, want fail", unsettled.Status)
	}
	for _, want := range []string{"cluster not settled after 5m0s", "rollout r9 gate", "beta=aaa activating/start login 503"} {
		if !strings.Contains(unsettled.Evidence, want) {
			t.Fatalf("evidence %q lacks %q", unsettled.Evidence, want)
		}
	}

	if failed := settledResult(settleState{}, errors.New("no bus")); failed.Status != "fail" || failed.Evidence != "no bus" {
		t.Fatalf("error result = %+v", failed)
	}
}

// Not settled: cluster.settled fails and every selected check is a skip that never runs.
func TestUnsettledReportShape(t *testing.T) {
	var ran int
	run := func(context.Context, *E2EEnv) (string, error) { ran++; return "", nil }
	checks := []E2ECheck{
		{ID: "deploy.unit.beta", Stream: "TS-810", Run: run},
		{ID: "cluster.members", Stream: "TS-810", Run: run},
		{ID: "access.login.tailnet", Stream: "TS-810", Run: run},
	}
	st := settleState{Rollout: "r9", Phase: phaseInstall, Hosts: settledHosts("aaa")}
	report := addSettled(runE2E(t.Context(), &E2EEnv{cfg: Config{LocalHost: "delta"}}, skipChecks(checks, unsettledSkip()), nil), settledResult(st, nil))

	if ran != 0 {
		t.Fatalf("%d checks ran, want none", ran)
	}
	if want := map[string]int{"pass": 0, "fail": 1, "skip": 3}; !maps.Equal(report.Summary, want) {
		t.Fatalf("summary = %v, want %v", report.Summary, want)
	}
	if report.Checks[0].ID != "cluster.settled" || report.Checks[0].Status != "fail" {
		t.Fatalf("first result = %+v, want failed cluster.settled", report.Checks[0])
	}
	for _, c := range report.Checks[1:] {
		if c.Status != "skip" || c.Evidence != "cluster not settled after 5m0s (see cluster.settled)" {
			t.Fatalf("%s = %s %q, want skip with the settle reason", c.ID, c.Status, c.Evidence)
		}
	}
}

func TestRetryAfterRollout(t *testing.T) {
	report := E2EReport{Checks: []E2EResult{
		{ID: "deploy.logs.beta", Status: "fail", Evidence: "login 503"},
		{ID: "cluster.restart", Status: "fail", Evidence: "chaos"},
		{ID: "a.ok", Status: "pass", Evidence: "fine"},
	}}
	checks := []E2ECheck{
		{ID: "deploy.logs.beta", Stream: "TS-810"},
		{ID: "cluster.restart", Stream: "TS-810", Chaos: true},
		{ID: "a.ok", Stream: "TS-810"},
	}
	report.Summary = countChecks(report.Checks)
	if got := retryableIDs(report, checks); !slices.Equal(got, []string{"deploy.logs.beta"}) {
		t.Fatalf("retryable = %v, want only the failed non-chaos check", got)
	}

	mergeRetry(&report, []E2EResult{{ID: "deploy.logs.beta", Status: "pass", Evidence: "no error lines"}}, "r2")
	if got := report.Checks[0]; got.Status != "pass" || got.Evidence != "retried after rollout r2: no error lines" {
		t.Fatalf("retried result = %+v", got)
	}
	if got := report.Checks[1]; got.Status != "fail" || got.Evidence != "chaos" {
		t.Fatalf("chaos result changed: %+v", got)
	}
	if want := map[string]int{"pass": 2, "fail": 1, "skip": 0}; !maps.Equal(report.Summary, want) {
		t.Fatalf("summary = %v, want %v", report.Summary, want)
	}
}
