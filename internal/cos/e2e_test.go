package cos

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	testSHA = "0123456789abcdef0123456789abcdef01234567"
	oldSHA  = "fedcba9876543210fedcba9876543210fedcba98"
)

// healthyFacts is what a host reports when every check should pass.
var healthyFacts = map[string]string{
	"LoadState":      "loaded",
	"ActiveState":    "active",
	"SubState":       "running",
	"NRestarts":      "0",
	"MainPID":        "123",
	"Version":        versionJSON(testSHA, false),
	"AutoRestarts":   "0",
	"DeployRestarts": "0",
	"LogErrors":      "0",
	"LogSample":      "",
}

func versionJSON(rev string, modified bool) string {
	return fmt.Sprintf(`{"modified":%t,"revision":%q}`, modified, rev)
}

// with returns healthyFacts with the key, value pairs in kv applied.
func with(kv ...string) map[string]string {
	m := maps.Clone(healthyFacts)
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func factsText(m map[string]string) string {
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(m)) {
		fmt.Fprintf(&b, "%s=%s\n", k, m[k])
	}
	return b.String()
}

// fakeRunner answers e2eFactsScript per host from hosts and counts every call.
type fakeRunner struct {
	hosts map[string]map[string]string
	calls int
}

func (f *fakeRunner) run(_ context.Context, host, script string) (string, error) {
	f.calls++
	facts, ok := f.hosts[host]
	if !ok {
		return "", fmt.Errorf("%s: no route to host", host)
	}
	if script != e2eFactsScript {
		return "", fmt.Errorf("unexpected script for %s", host)
	}
	return factsText(facts), nil
}

func fakeMain(head e2eMain, err error) func(context.Context) (e2eMain, error) {
	return func(context.Context) (e2eMain, error) { return head, err }
}

func fakeEnv(hosts map[string]map[string]string, head e2eMain, headErr error) (*E2EEnv, *fakeRunner) {
	fr := &fakeRunner{hosts: hosts}
	return &E2EEnv{cfg: Config{LocalHost: "delta"}, run: fr.run, main: fakeMain(head, headErr)}, fr
}

func statusOf(err error) string {
	var skip e2eSkip
	switch {
	case errors.As(err, &skip):
		return "skip"
	case err != nil:
		return "fail"
	}
	return "pass"
}

func evidenceOf(got string, err error) string {
	if err != nil {
		return err.Error()
	}
	return got
}

func TestE2ERunStatusMapping(t *testing.T) {
	tests := []struct {
		name, evidence, wantStatus, wantEvidence string
		err                                      error
	}{
		{"pass", "all good", "pass", "all good", nil},
		{"fail", "", "fail", "boom", errors.New("boom")},
		{"skip", "", "skip", "TS-810: not installed", e2eSkip("TS-810: not installed")},
		{"wrapped skip", "", "skip", "TS-814: no streams", fmt.Errorf("ssh: %w", e2eSkip("TS-814: no streams"))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			check := E2ECheck{ID: "x.y", Stream: "TS-810", Run: func(context.Context, *E2EEnv) (string, error) { return tc.evidence, tc.err }}
			report := runE2E(t.Context(), &E2EEnv{cfg: Config{LocalHost: "delta"}}, []E2ECheck{check}, nil)
			got := report.Checks[0]
			if got.Status != tc.wantStatus || got.Evidence != tc.wantEvidence {
				t.Fatalf("status=%q evidence=%q, want status=%q evidence=%q", got.Status, got.Evidence, tc.wantStatus, tc.wantEvidence)
			}
		})
	}
}

func TestE2ERunSummaryAndOrder(t *testing.T) {
	mk := func(id string, err error) E2ECheck {
		return E2ECheck{ID: id, Stream: "TS-810", Run: func(context.Context, *E2EEnv) (string, error) { return "", err }}
	}
	checks := []E2ECheck{
		mk("a.one", nil),
		mk("a.two", errors.New("x")),
		mk("b.one", e2eSkip("TS-810: y")),
		mk("b.two", errors.New("z")),
	}
	report := runE2E(t.Context(), &E2EEnv{cfg: Config{LocalHost: "delta"}}, checks, nil)
	if want := map[string]int{"pass": 1, "fail": 2, "skip": 1}; !maps.Equal(report.Summary, want) {
		t.Fatalf("summary = %v, want %v", report.Summary, want)
	}
	if report.Runner != "delta" {
		t.Fatalf("runner = %q, want delta", report.Runner)
	}
	var ids []string
	for _, c := range report.Checks {
		ids = append(ids, c.ID)
	}
	if want := []string{"a.one", "a.two", "b.one", "b.two"}; !slices.Equal(ids, want) {
		t.Fatalf("check order = %v, want %v", ids, want)
	}
}

func TestE2ERunFilter(t *testing.T) {
	ids := []string{"deploy.unit.beta", "deploy", "dep", "cluster.members"}
	var checks []E2ECheck
	for _, id := range ids {
		checks = append(checks, E2ECheck{ID: id, Stream: "TS-810", Run: func(context.Context, *E2EEnv) (string, error) { return "", nil }})
	}
	tests := []struct {
		name   string
		filter []string
		want   []string
	}{
		{"no filter runs all", nil, ids},
		{"prefix matches dotted children and exact id", []string{"deploy"}, []string{"deploy.unit.beta", "deploy"}},
		{"partial segment does not match", []string{"dep"}, []string{"dep"}},
		{"exact dotted id", []string{"deploy.unit.beta"}, []string{"deploy.unit.beta"}},
		{"group prefix", []string{"cluster"}, []string{"cluster.members"}},
		{"several prefixes", []string{"deploy", "cluster.members"}, []string{"deploy.unit.beta", "deploy", "cluster.members"}},
		{"no match runs nothing", []string{"nope"}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := runE2E(t.Context(), &E2EEnv{cfg: Config{LocalHost: "delta"}}, checks, tc.filter)
			var got []string
			for _, c := range report.Checks {
				got = append(got, c.ID)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("ran %v, want %v", got, tc.want)
			}
		})
	}
}

func TestE2ERunEvidenceIsClipped(t *testing.T) {
	checks := []E2ECheck{
		{ID: "long.pass", Stream: "TS-810", Run: func(context.Context, *E2EEnv) (string, error) { return strings.Repeat("x", 700), nil }},
		{ID: "secret.fail", Stream: "TS-810", Run: func(context.Context, *E2EEnv) (string, error) { return "", errors.New("boom token=abc") }},
	}
	report := runE2E(t.Context(), &E2EEnv{cfg: Config{LocalHost: "delta"}}, checks, nil)
	if want := strings.Repeat("x", 600) + "…"; report.Checks[0].Evidence != want {
		t.Fatalf("long evidence len=%d, want clipped to 600 + ellipsis", len(report.Checks[0].Evidence))
	}
	if want := "boom token=[redacted]"; report.Checks[1].Evidence != want {
		t.Fatalf("fail evidence = %q, want %q", report.Checks[1].Evidence, want)
	}
}

func TestE2EClipRedacts(t *testing.T) {
	tests := []struct{ in, want string }{
		{"token=abc", "token=[redacted]"},
		{"password: x", "password: [redacted]"},
		{`"secret":"y"`, `"secret":[redacted]`},
		{"Passwd = hunter2", "Passwd = [redacted]"},
		{"cookie=sid123 ok", "cookie=[redacted] ok"},
		{"no secrets here", "no secrets here"},
		{"  trimmed  ", "trimmed"},
	}
	for _, tc := range tests {
		if got := e2eClip(tc.in, 600); got != tc.want {
			t.Errorf("e2eClip(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestAllHealthy(t *testing.T) {
	tests := []struct {
		name   string
		status map[string]string
		ok     bool
		want   string
	}{
		{"all ok", map[string]string{"beta": "ok", "delta": "ok", "mariogk-dev": "ok"}, true,
			"beta=ok delta=ok mariogk-dev=ok"},
		{"one catching up", map[string]string{"beta": "ok", "delta": "JetStream not in sync: consumer lagging", "mariogk-dev": "ok"}, false,
			"beta=ok delta=JetStream not in sync: consumer lagging mariogk-dev=ok"},
		{"one unreachable", map[string]string{"beta": "ok", "delta": "unreachable: timeout", "mariogk-dev": "ok"}, false,
			"beta=ok delta=unreachable: timeout mariogk-dev=ok"},
		{"no hosts", map[string]string{}, false, ""},
	}
	for _, tc := range tests {
		if ok, got := allHealthy(tc.status); ok != tc.ok || got != tc.want {
			t.Errorf("%s: allHealthy = %v %q, want %v %q", tc.name, ok, got, tc.ok, tc.want)
		}
	}
}

func TestPreSignalEvidence(t *testing.T) {
	ok := "beta=ok delta=ok mariogk-dev=ok"
	bad := "beta=ok delta=unreachable: timeout mariogk-dev=ok"
	for _, tc := range []struct {
		name     string
		ok1, ok2 bool
		ev1, ev2 string
		skip     bool
	}{
		{"both polls ok", true, true, ok, ok, false},
		{"second poll fails", true, false, ok, bad, true},
		{"first poll fails", false, true, bad, ok, true},
	} {
		got, err := preSignalEvidence(tc.ok1, tc.ev1, tc.ok2, tc.ev2)
		var skip e2eSkip
		if errors.As(err, &skip) != tc.skip {
			t.Fatalf("%s: err = %v, want skip %v", tc.name, err, tc.skip)
		}
		if tc.skip {
			if !strings.Contains(err.Error(), "poll 1: "+tc.ev1) || !strings.Contains(err.Error(), "poll 2: "+tc.ev2) {
				t.Errorf("%s: skip %q lacks both polls' status", tc.name, err)
			}
			continue
		}
		if want := "pre-signal health (2 polls, 5s apart): " + ok + "; " + ok; got != want {
			t.Errorf("%s: evidence = %q, want %q", tc.name, got, want)
		}
	}
}

func TestRolloutHold(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		r     Rollout
		holds bool
	}{
		{"no record", Rollout{}, false},
		{"gate active", Rollout{ID: "r1", Phase: phaseGate, Since: now.Add(-time.Hour)}, true},
		{"install active", Rollout{ID: "r1", Phase: phaseInstall, Since: now}, true},
		{"rollback active", Rollout{ID: "r1", Phase: phaseRollback, Since: now.Add(-time.Hour)}, true},
		{"completed 30s ago", Rollout{ID: "r1", Phase: phaseCompleted, Since: now.Add(-30 * time.Second)}, true},
		{"rolled back 119s ago", Rollout{ID: "r1", Phase: phaseRolledBack, Since: now.Add(-119 * time.Second)}, true},
		{"completed 2m ago", Rollout{ID: "r1", Phase: phaseCompleted, Since: now.Add(-rolloutSettle)}, false},
		{"rolled back long ago", Rollout{ID: "r1", Phase: phaseRolledBack, Since: now.Add(-time.Hour)}, false},
	} {
		reason := rolloutHold(tc.r, now)
		if (reason != "") != tc.holds {
			t.Errorf("%s: hold = %q, want holds %v", tc.name, reason, tc.holds)
		}
		if tc.holds && !strings.Contains(reason, "not perturbing") {
			t.Errorf("%s: reason %q lacks 'not perturbing'", tc.name, reason)
		}
	}
}

func TestStreamSummary(t *testing.T) {
	a := streamRow{line: "A", ok: true, lag: 3}
	b := streamRow{line: "B", ok: true}
	bad := streamRow{line: "C", lag: 7}
	tests := []struct {
		name    string
		rows    []streamRow
		healthy bool
		want    string
	}{
		{"all current", []streamRow{a, b}, true, "2 streams R3, all current (max lag 3)"},
		{"one failing listed first", []streamRow{a, bad, b}, false, "C; A; B"},
		{"no streams", nil, true, "0 streams R3, all current (max lag 0)"},
	}
	for _, tc := range tests {
		if got, healthy := streamSummary(tc.rows); healthy != tc.healthy || got != tc.want {
			t.Errorf("%s: streamSummary = %q %v, want %q %v", tc.name, got, healthy, tc.want, tc.healthy)
		}
	}
}

func TestE2EClipTruncates(t *testing.T) {
	if got, want := e2eClip(strings.Repeat("a", 700), 600), strings.Repeat("a", 600)+"…"; got != want {
		t.Errorf("long text clipped to len %d, want 600 + ellipsis", len(got))
	}
	if got := e2eClip("aé", 2); got != "a…" {
		t.Errorf("clip split a UTF-8 rune: %q", got)
	}
	if got := e2eClip("abcd", 4); got != "abcd" {
		t.Errorf("text of exactly n bytes = %q, want unchanged", got)
	}
}

func TestE2EClipRedactsBearerToken(t *testing.T) {
	got := e2eClip("Authorization: Bearer abc123", 600)
	if strings.Contains(got, "abc123") {
		t.Fatalf("token leaked: %q", got)
	}
}

func TestE2ECheckUnit(t *testing.T) {
	tests := []struct {
		name, want, wantEvidence string
		facts                    map[string]string
	}{
		{"active and running", "pass", "ActiveState=active", with()},
		{"failed", "fail", "ActiveState=failed", with("ActiveState", "failed", "SubState", "failed")},
		{"still starting", "fail", "SubState=start", with("ActiveState", "activating", "SubState", "start")},
		{"not installed yet", "skip", "TS-810", with("LoadState", "not-found", "ActiveState", "inactive", "SubState", "dead")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, _ := fakeEnv(map[string]map[string]string{"beta": tc.facts}, e2eMain{}, nil)
			got, err := checkUnit(t.Context(), env, "beta")
			if s := statusOf(err); s != tc.want {
				t.Fatalf("status = %s (%s), want %s", s, evidenceOf(got, err), tc.want)
			}
			if ev := evidenceOf(got, err); !strings.Contains(ev, tc.wantEvidence) {
				t.Fatalf("evidence %q does not contain %q", ev, tc.wantEvidence)
			}
		})
	}
}

func TestE2ECheckVersion(t *testing.T) {
	now := time.Now()
	fresh := e2eMain{SHA: testSHA, Time: now.Add(-time.Minute)}
	old := e2eMain{SHA: testSHA, Time: now.Add(-time.Hour)}
	tests := []struct {
		name, want, wantEvidence string
		facts                    map[string]string
		head                     e2eMain
		headErr                  error
	}{
		{"matches main", "pass", "runs 0123456789ab", with(), old, nil},
		{"stale inside deploy window", "skip", "deploy window", with("Version", versionJSON(oldSHA, false)), fresh, nil},
		{"stale after deploy window", "fail", "runs fedcba987654", with("Version", versionJSON(oldSHA, false)), old, nil},
		{"modified build at main sha", "fail", "modified=true", with("Version", versionJSON(testSHA, true)), old, nil},
		{"empty version", "fail", "no revision", with("Version", ""), old, nil},
		{"garbage version", "fail", "no revision", with("Version", "not json"), old, nil},
		{"main lookup fails", "fail", "read origin/main", with(), old, errors.New("404")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, _ := fakeEnv(map[string]map[string]string{"beta": tc.facts}, tc.head, tc.headErr)
			got, err := checkVersion(t.Context(), env, "beta")
			if s := statusOf(err); s != tc.want {
				t.Fatalf("status = %s (%s), want %s", s, evidenceOf(got, err), tc.want)
			}
			if ev := evidenceOf(got, err); !strings.Contains(ev, tc.wantEvidence) {
				t.Fatalf("evidence %q does not contain %q", ev, tc.wantEvidence)
			}
		})
	}
}

func TestE2ECheckRestarts(t *testing.T) {
	tests := []struct {
		name, want, wantEvidence string
		facts                    map[string]string
	}{
		{"no automatic restarts", "pass", "0 unattributed automatic restarts", with()},
		// Attribution reads the chaos bucket through the runner's bus; the fake env has no delta host, so it fails there.
		{"automatic restarts need the runner's bus", "fail", "delta: no route to host", with("AutoRestarts", "2")},
		{"non-numeric count", "fail", "cannot read journal restarts", with("AutoRestarts", "lots")},
		{"deploy installs are reported, not counted", "pass", "6 deploy installs", with("DeployRestarts", "6")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, _ := fakeEnv(map[string]map[string]string{"beta": tc.facts}, e2eMain{}, nil)
			got, err := checkRestarts(t.Context(), env, "beta")
			if s := statusOf(err); s != tc.want {
				t.Fatalf("status = %s (%s), want %s", s, evidenceOf(got, err), tc.want)
			}
			if ev := evidenceOf(got, err); !strings.Contains(ev, tc.wantEvidence) {
				t.Fatalf("evidence %q does not contain %q", ev, tc.wantEvidence)
			}
		})
	}
}

// TestE2ERestartAttribution feeds beta's journal from the first live e2e failure (22:52-23:00)
// plus a failed trial and a kill to the attribution the facts script runs on each host.
func TestE2ERestartAttribution(t *testing.T) {
	journal := `INFO restarting into another build action=install revision=77edc3a27f77
cos.service: Scheduled restart job, restart counter is at 6.
Started TerraScale COS.
INFO restarting into another build action=install revision=d84e2d459ea0
cos.service: Scheduled restart job, restart counter is at 7.
INFO restarting into another build action=install revision=fe8667f253fd
cos.service: Scheduled restart job, restart counter is at 8.
cos.service: start operation timed out. Terminating.
cos.service: Scheduled restart job, restart counter is at 9.
cos.service: Main process exited, code=killed, status=9/KILL
cos.service: Scheduled restart job, restart counter is at 10.
`
	cmd := exec.Command("awk", e2eRestartAwk)
	cmd.Stdin = strings.NewReader(journal)
	out, err := cmd.Output()
	if err != nil || string(out) != "AutoRestarts=2\nDeployRestarts=3\n" {
		t.Fatalf("attribution = %q, %v", out, err)
	}
}

func TestE2ECheckLogs(t *testing.T) {
	tests := []struct {
		name, want, wantEvidence string
		facts                    map[string]string
	}{
		{"no error lines", "pass", "no error or panic lines", with()},
		{"four error lines", "fail", "4 error/panic lines since start (max 17 min), first: panic: nil map", with("LogErrors", "4", "LogSample", "panic: nil map")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, _ := fakeEnv(map[string]map[string]string{"beta": tc.facts}, e2eMain{}, nil)
			got, err := checkLogs(t.Context(), env, "beta")
			if s := statusOf(err); s != tc.want {
				t.Fatalf("status = %s (%s), want %s", s, evidenceOf(got, err), tc.want)
			}
			if ev := evidenceOf(got, err); !strings.Contains(ev, tc.wantEvidence) {
				t.Fatalf("evidence %q does not contain %q", ev, tc.wantEvidence)
			}
		})
	}
}

func TestE2EHostFactsCaches(t *testing.T) {
	env, fr := fakeEnv(map[string]map[string]string{"beta": with(), "delta": with()}, e2eMain{}, nil)
	ctx := t.Context()
	for _, check := range []func(context.Context, *E2EEnv, string) (string, error){checkUnit, checkRestarts, checkLogs} {
		if _, err := check(ctx, env, "beta"); err != nil {
			t.Fatalf("beta check failed: %v", err)
		}
	}
	if fr.calls != 1 {
		t.Fatalf("three checks on beta ran the script %d times, want 1", fr.calls)
	}
	if _, err := checkUnit(ctx, env, "delta"); err != nil {
		t.Fatalf("delta check failed: %v", err)
	}
	if fr.calls != 2 {
		t.Fatalf("after delta, script ran %d times, want 2", fr.calls)
	}
	if _, err := env.hostFacts(ctx, "beta", true); err != nil {
		t.Fatalf("fresh beta facts failed: %v", err)
	}
	if fr.calls != 3 {
		t.Fatalf("fresh=true ran the script %d times total, want 3", fr.calls)
	}
}

func TestNodeCollectorBuilt(t *testing.T) {
	tests := []struct {
		name, out string
		built     bool
		wantErr   bool
	}{
		{"collector", "usage: cos nodes [<host> [1h|24h|7d]]", true, false},
		{"predates collector", `unknown command "nodes"; use cos help`, false, false},
		{"no running process", "", false, true},
		{"unexpected output", "bash: /proc/42/exe: No such file", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := 0
			env := &E2EEnv{cfg: Config{LocalHost: "delta"}, run: func(_ context.Context, host, script string) (string, error) {
				if script == e2eFactsScript {
					return factsText(with("Version", versionJSON(testSHA, false))), nil
				}
				if script != collectorProbeScript {
					return "", fmt.Errorf("unexpected script for %s", host)
				}
				probes++
				return tc.out, nil
			}}
			built, rev, err := env.nodeCollectorBuilt(t.Context(), "beta")
			if (err != nil) != tc.wantErr || built != tc.built {
				t.Fatalf("built=%t err=%v, want built=%t wantErr=%t", built, err, tc.built, tc.wantErr)
			}
			if err == nil && rev != "0123456789ab" {
				t.Fatalf("revision = %q, want the 12-character prefix", rev)
			}
			if _, _, err := env.nodeCollectorBuilt(t.Context(), "beta"); err == nil && probes != 1 {
				t.Fatalf("second call probed %d times, want the cached 1", probes)
			}
		})
	}
}

func TestE2EFactsParse(t *testing.T) {
	env := &E2EEnv{cfg: Config{LocalHost: "delta"}, run: func(context.Context, string, string) (string, error) {
		return "noise line\nLoadState=loaded\nLogSample=level=ERROR boom\n", nil
	}}
	f, err := env.hostFacts(t.Context(), "beta", false)
	if err != nil {
		t.Fatal(err)
	}
	if f["LoadState"] != "loaded" || f["LogSample"] != "level=ERROR boom" || len(f) != 2 {
		t.Fatalf("facts = %v, want LoadState and LogSample only, with '=' kept in the value", f)
	}
}

func TestE2EPartitionScript(t *testing.T) {
	v4 := netip.MustParseAddr("100.115.156.95")
	v6 := netip.MustParseAddr("fd7a:115c:a1e0::1")
	script := partitionScript([]netip.Addr{v4, v6})
	tests := []struct{ name, want string }{
		{"v4 input drop keeps ssh", "sudo iptables -w -I INPUT 1 -s 100.115.156.95 -p tcp ! --dport 22 -m comment --comment cos-e2e -j DROP\n"},
		{"v4 output drop keeps ssh", "sudo iptables -w -I OUTPUT 1 -d 100.115.156.95 -p tcp ! --sport 22 -m comment --comment cos-e2e -j DROP\n"},
		{"v6 input drop keeps ssh", "sudo ip6tables -w -I INPUT 1 -s fd7a:115c:a1e0::1 -p tcp ! --dport 22 -m comment --comment cos-e2e -j DROP\n"},
		{"v6 output drop keeps ssh", "sudo ip6tables -w -I OUTPUT 1 -d fd7a:115c:a1e0::1 -p tcp ! --sport 22 -m comment --comment cos-e2e -j DROP\n"},
		{"root timer heals in 120 s", "sudo systemd-run --quiet --collect --on-active=120 --setenv=PATH=\"$PATH\" bash /run/cos-e2e-heal.sh"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(script, tc.want) {
				t.Fatalf("script lacks %q:\n%s", tc.want, script)
			}
		})
	}
	if !strings.HasPrefix(script, "set -e\n"+e2eHeal+"\n") {
		t.Fatalf("script must start with set -e and the heal loop:\n%s", script)
	}
	if strings.Contains(script, "sudo iptables -w -I INPUT 1 -s fd7a") || strings.Contains(script, "sudo ip6tables -w -I INPUT 1 -s 100.") {
		t.Fatalf("address family mixed up:\n%s", script)
	}
}

func TestE2EPartitionTimerHealPayload(t *testing.T) {
	script := partitionScript([]netip.Addr{netip.MustParseAddr("100.115.156.95")})
	lines := strings.Split(strings.TrimSpace(script), "\n")
	timer := lines[len(lines)-1]
	if !strings.Contains(timer, "--on-active=120") {
		t.Fatalf("last line is not the root timer: %s", timer)
	}
	if strings.Contains(timer, `\n`) {
		t.Errorf("timer payload has literal \\n escapes, so bash -c gets one broken line: %s", timer)
	}
	if strings.Contains(timer, "$(") || strings.Contains(timer, "$t") {
		t.Errorf("timer payload is expanded by the outer shell, not by bash -c: %s", timer)
	}
}

func TestE2EChecksIDsAndGroups(t *testing.T) {
	// SELF-<n>: acceptance checks a self-change adds under checks/.
	streamPattern := regexp.MustCompile(`^(TS-8(1[0-9]|2[0-9])|SELF-[0-9]+)$`)
	seen := map[string]bool{}
	checks := e2eChecks()
	if len(checks) < 3*len(e2eHosts)+6 {
		t.Fatalf("got %d checks, expected at least the per-host and cluster checks", len(checks))
	}
	for _, c := range checks {
		if seen[c.ID] {
			t.Errorf("duplicate check id %q", c.ID)
		}
		seen[c.ID] = true
		if !streamPattern.MatchString(c.Stream) {
			t.Errorf("%s: stream = %q, want TS-810 to TS-829 or SELF-<n>", c.ID, c.Stream)
		}
	}
}

func TestE2ESmokeAndProtected(t *testing.T) {
	run := func(context.Context, *E2EEnv) (string, error) { return "", nil }
	checks := []E2ECheck{
		{ID: "deploy.unit.beta", Stream: "TS-810", Smoke: true, Run: run},
		{ID: "cluster.partition", Stream: "TS-810", Run: run},
		{ID: "access.lockout", Stream: "TS-810", Run: run},
	}
	report := runE2E(t.Context(), &E2EEnv{}, checks, []string{"--smoke"})
	if len(report.Checks) != 1 || report.Checks[0].ID != "deploy.unit.beta" || !report.Checks[0].Protected {
		t.Fatalf("smoke run = %+v, want only protected deploy.unit.beta", report.Checks)
	}
	report = runE2E(t.Context(), &E2EEnv{}, checks, []string{"--smoke", "cluster"})
	if len(report.Checks) != 0 {
		t.Fatalf("smoke plus cluster prefix = %+v, want none", report.Checks)
	}
	report = runE2E(t.Context(), &E2EEnv{}, checks, nil)
	for _, r := range report.Checks {
		if r.Protected != (r.Group != "cluster") {
			t.Errorf("%s protected = %t", r.ID, r.Protected)
		}
	}
}

// TestE2EProtectedChecksStay pins the protected checks: a self-change may add to them, never drop one.
func TestE2EProtectedChecksStay(t *testing.T) {
	ids := map[string]bool{}
	for _, c := range e2eChecks() {
		ids[c.ID] = true
	}
	want := []string{
		"access.login.tailnet", "access.login.public", "access.wrong-password", "access.lockout",
		"access.session-cross-node", "access.logout", "access.password-change",
		"deploy.canary", "deploy.rollback", "deploy.reconnect", "gate.self-change",
	}
	for _, h := range e2eHosts {
		want = append(want, "deploy.unit."+h, "deploy.version."+h, "deploy.restarts."+h, "deploy.logs."+h, "deploy.watchdog."+h)
	}
	for _, id := range want {
		if !ids[id] {
			t.Errorf("protected check %s is missing", id)
		}
	}
}

func TestRegisterE2E(t *testing.T) {
	saved := e2eRegistered
	t.Cleanup(func() { e2eRegistered = saved })
	run := func(context.Context, *E2EEnv) (string, error) { return "ok", nil }
	RegisterE2E(E2ECheck{ID: "accept.ts-900.chat-button", Stream: "TS-900", Run: run})
	checks := e2eChecks()
	if last := checks[len(checks)-1]; last.ID != "accept.ts-900.chat-button" {
		t.Fatalf("registered check not appended, last = %s", last.ID)
	}
	for _, bad := range []E2ECheck{
		{ID: "accept.ts-900.chat-button", Stream: "TS-900", Run: run}, // duplicate
		{ID: "deploy.unit.beta", Stream: "TS-900", Run: run},          // protected group
		{ID: "accept.ts-900", Stream: "TS-900", Run: run},             // no name
		{ID: "accept..x", Stream: "TS-900", Run: run},
		{ID: "accept.ts-900.x", Run: run},
		{ID: "accept.ts-900.y", Stream: "TS-900"},
		{ID: "accept.ts-900.z", Stream: "TS-900", Smoke: true, Chaos: true, Run: run},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("RegisterE2E(%q) did not panic", bad.ID)
				}
			}()
			RegisterE2E(bad)
		}()
	}
}

// TestE2ESmokeHasNoChaos: the fast health gate never perturbs the cluster.
func TestE2ESmokeHasNoChaos(t *testing.T) {
	chaos := map[string]bool{}
	for _, c := range e2eChecks() {
		if c.Smoke && c.Chaos {
			t.Errorf("%s is both Smoke and Chaos", c.ID)
		}
		chaos[c.ID] = c.Chaos
	}
	for _, id := range []string{"cluster.restart", "cluster.partition", "cluster.leader-freeze", "cluster.leader-kill"} {
		if !chaos[id] {
			t.Errorf("%s is not marked Chaos", id)
		}
	}
}

func TestE2ESmokeSkipsChaos(t *testing.T) {
	run := func(context.Context, *E2EEnv) (string, error) { return "", nil }
	checks := []E2ECheck{
		{ID: "deploy.unit.beta", Stream: "TS-810", Smoke: true, Run: run},
		{ID: "cluster.restart", Stream: "TS-810", Smoke: true, Chaos: true, Run: run},
	}
	report := runE2E(t.Context(), &E2EEnv{}, checks, []string{"--smoke"})
	if len(report.Checks) != 1 || report.Checks[0].ID != "deploy.unit.beta" {
		t.Fatalf("smoke run = %+v, want only deploy.unit.beta", report.Checks)
	}
}

func TestSmokeChecksScopedToHost(t *testing.T) {
	var ids []string
	for _, c := range smokeChecks(e2eChecks(), "beta") {
		ids = append(ids, c.ID)
	}
	want := []string{"access.login.tailnet", "access.login.public", "deploy.unit.beta", "deploy.version.beta", "deploy.logs.beta", "cluster.members", "cluster.jetstream", "cluster.leader", "rules.heartbeat-leader", "push.service-worker"}
	if !slices.Equal(ids, want) {
		t.Fatalf("smoke checks for beta = %v, want %v", ids, want)
	}
}

func TestE2ESmokeVersionExpectsRevision(t *testing.T) {
	env, _ := fakeEnv(map[string]map[string]string{"beta": with()}, e2eMain{SHA: "old"}, nil)
	env.main = func(context.Context) (e2eMain, error) { return e2eMain{SHA: testSHA}, nil }
	if _, err := checkVersion(t.Context(), env, "beta"); err != nil {
		t.Fatalf("matching rollout revision failed: %v", err)
	}
	env, _ = fakeEnv(map[string]map[string]string{"beta": with()}, e2eMain{SHA: "rollback"}, nil)
	if _, err := checkVersion(t.Context(), env, "beta"); err == nil || errors.As(err, new(e2eSkip)) {
		t.Fatalf("rollback revision mismatch must fail, got %v", err)
	}
}

func TestAgreedLeader(t *testing.T) {
	for _, tc := range []struct {
		name  string
		views map[string]string
		want  string
	}{
		{"all agree", map[string]string{"beta": "delta", "delta": "delta", "mariogk-dev": "delta"}, "delta"},
		{"split brain", map[string]string{"beta": "beta", "delta": "delta", "mariogk-dev": "delta"}, ""},
		{"unknown host", map[string]string{"beta": "alpha", "delta": "alpha", "mariogk-dev": "alpha"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &E2EEnv{run: func(_ context.Context, host, script string) (string, error) {
				if script != "~/.local/bin/cos cluster" {
					t.Fatalf("unexpected script %q", script)
				}
				return `{"host":"` + host + `","leader":"` + tc.views[host] + `"}`, nil
			}}
			got, _, err := env.agreedLeader(t.Context(), e2eHosts, "", 0)
			if got != tc.want || (err == nil) != (tc.want != "") {
				t.Fatalf("agreedLeader = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestE2EAccessHelpers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/login" && r.PostFormValue("password") == "right":
			http.SetCookie(rw, &http.Cookie{Name: sessionCookie, Value: "u.token"})
			http.Redirect(rw, r, "/", http.StatusSeeOther)
		case r.Method == "POST" && r.URL.Path == "/login" && r.PostFormValue("password") == "nocookie":
			http.Redirect(rw, r, "/", http.StatusSeeOther)
		case r.Method == "POST":
			http.Error(rw, "Wrong username or password.", http.StatusUnauthorized)
		case r.URL.Path == "/" && strings.Contains(r.Header.Get("Cookie"), "u.token"):
			rw.WriteHeader(http.StatusOK)
		case r.URL.Path == "/" && strings.Contains(r.Header.Get("Cookie"), "broken"):
			rw.WriteHeader(http.StatusInternalServerError)
		default:
			http.Redirect(rw, r, "/login", http.StatusSeeOther)
		}
	}))
	defer srv.Close()
	ctx := t.Context()
	if cookie, status, _, err := signIn(ctx, srv.URL, "u", "right"); err != nil || status != 303 || cookie != "u.token" {
		t.Fatalf("signIn right = %q %d %v", cookie, status, err)
	}
	if _, status, body, err := signIn(ctx, srv.URL, "u", "wrong"); err != nil || status != 401 || !strings.Contains(body, "Wrong") {
		t.Fatalf("signIn wrong = %d %v", status, err)
	}
	if _, _, _, err := signIn(ctx, srv.URL, "u", "nocookie"); err == nil {
		t.Fatal("a 303 without a session must be an error")
	}
	for cookie, want := range map[string]bool{"u.token": true, "": false, "stale": false} {
		if ok, err := signedIn(ctx, srv.URL, cookie); err != nil || ok != want {
			t.Errorf("signedIn(%q) = %t %v, want %t", cookie, ok, err, want)
		}
	}
	if _, err := signedIn(ctx, srv.URL, "broken"); err == nil {
		t.Error("HTTP 500 must be an error, not signed out")
	}
}

func TestE2ENodeURLPicksTailnet(t *testing.T) {
	env := &E2EEnv{run: func(context.Context, string, string) (string, error) {
		return "10.10.10.102/24\nfd7a:115c:a1e0::9b37:9c60/128\n100.115.156.95/32\n", nil
	}}
	if got, err := env.nodeURL(t.Context(), "beta"); err != nil || got != "http://100.115.156.95:8790" {
		t.Fatalf("nodeURL = %q %v", got, err)
	}
	env.run = func(context.Context, string, string) (string, error) { return "10.10.10.102/24\n", nil }
	if _, err := env.nodeURL(t.Context(), "beta"); err == nil {
		t.Fatal("a host without a tailnet IPv4 must fail")
	}
}

func TestE2EOnDemandRunsOnlyByExactID(t *testing.T) {
	run := func(context.Context, *E2EEnv) (string, error) { return "", nil }
	checks := []E2ECheck{{ID: "cluster.leader-freeze", Stream: "TS-810", Run: run}, {ID: "cluster.leader-kill", Stream: "TS-810", OnDemand: true, Run: run}}
	for _, tc := range []struct {
		args []string
		want int
	}{{nil, 1}, {[]string{"cluster"}, 1}, {[]string{"cluster.leader-kill"}, 1}, {[]string{"cluster", "cluster.leader-kill"}, 2}} {
		if got := len(runE2E(t.Context(), &E2EEnv{}, checks, tc.args).Checks); got != tc.want {
			t.Errorf("args %v ran %d checks, want %d", tc.args, got, tc.want)
		}
	}
}

func TestHaikuThreads(t *testing.T) {
	thread := func(id, model, title string, lineage *struct {
		Parent   string `json:"parent"`
		Relation string `json:"rel"`
	}) FleetThread {
		return FleetThread{ThreadID: id, Model: model, Title: title, Lineage: lineage}
	}
	subagent := &struct {
		Parent   string `json:"parent"`
		Relation string `json:"rel"`
	}{"parent-1", "subagent"}
	fork := &struct {
		Parent   string `json:"parent"`
		Relation string `json:"rel"`
	}{"parent-1", "fork"}
	fleets := []HostFleet{{Host: "delta", Threads: []FleetThread{
		thread("top", "claude-haiku-5-5", "top-level haiku", nil),
		thread("sub", "claude-haiku-5-5", "delegated review", subagent),
		thread("forked", "claude-haiku-5-5", "forked haiku", fork),
		thread("opus", "claude-opus-5-5", "opus thread", nil),
	}}}
	bad := haikuThreads(fleets)
	want := []string{`delta/top (claude-haiku-5-5, "top-level haiku")`, `delta/forked (claude-haiku-5-5, "forked haiku")`}
	if !slices.Equal(bad, want) {
		t.Errorf("haikuThreads = %q, want %q", bad, want)
	}
}

func TestAdvisorProblems(t *testing.T) {
	const good = `delegate_task {"providerInstanceId":"claudeAgent","model":"claude-haiku-5-5"}`
	pol := CurrentBehavior().Models
	tests := []struct {
		name, text string
		want       []string
	}{
		{"good", good, nil},
		{"launch", good + ` {"instanceId":"claudeAgent","model":"claude-haiku-5-5"}`, []string{"brief.md: launches a worker model as a thread"}},
		{"prose", good + " Then use a claude-haiku-5-5 thread.", []string{"brief.md: tells agents to open a worker-model thread"}},
		{"empty", "", []string{"brief.md: no delegate_task"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := advisorProblems("brief.md", tc.text, pol)
			if !slices.Equal(got, tc.want) {
				t.Errorf("advisorProblems = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestInboxE2EChecksRegistered: the Inbox checks run live once, never in smoke or chaos runs, and are no
// longer pending. inbox.needs-user-forward is on demand only, since it spends an Opus probe run.
func TestInboxE2EChecksRegistered(t *testing.T) {
	live := []string{"inbox.add-visible", "inbox.answer-closes", "inbox.concurrent-answer", "inbox.dedup", "inbox.needs-user-forward"}
	count := map[string]int{}
	for _, c := range e2eChecks() {
		if !slices.Contains(live, c.ID) {
			continue
		}
		count[c.ID]++
		if c.Smoke || c.Chaos || c.Stream != "TS-815" {
			t.Errorf("%s: Smoke=%t Chaos=%t Stream=%q, want a plain TS-815 check", c.ID, c.Smoke, c.Chaos, c.Stream)
		}
		if onDemand := c.ID == "inbox.needs-user-forward"; c.OnDemand != onDemand {
			t.Errorf("%s: OnDemand=%t, want %t", c.ID, c.OnDemand, onDemand)
		}
	}
	for _, id := range live {
		if count[id] != 1 {
			t.Errorf("%s registered %d times, want once", id, count[id])
		}
	}
	pendingIDs := map[string]int{}
	for _, c := range e2ePending {
		pendingIDs[c.ID]++
	}
	for _, id := range live {
		if pendingIDs[id] != 0 {
			t.Errorf("%s is still pending", id)
		}
	}
}
