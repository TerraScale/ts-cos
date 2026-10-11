package cos

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	thirdSHA  = "1111111111111111111111111111111111111111"
	fourthSHA = "2222222222222222222222222222222222222222"
	// watchdogLine is the trial revert both ExecStartPre and ExecStopPost carry.
	watchdogLine = "/bin/sh -c 'cd %h/.local/share/cos && if [ -e trial ]; then ln -sfn \"$$(readlink good)\" current.tmp; fi'"
)

// versionText is what `current/cos version` prints on a settled host, compacted as the script prints it.
func versionText(rev string) string {
	return fmt.Sprintf(`{"protocol":1,"rollout":2,"host":"delta","revision":%q,"good":%q}`, rev, rev)
}

// healthyWatchdog is the script output of a settled host with the TS-818 unit installed.
var healthyWatchdog = map[string]string{
	"Type":            "notify",
	"TimeoutStartSec": "60",
	"ExecStartPre":    watchdogLine,
	"ExecStopPost":    watchdogLine,
	"ExecStart":       "%h/.local/share/cos/current/cos serve",
	"Current":         testSHA,
	"Good":            testSHA,
	"Builds":          "2",
	"Version":         versionText(testSHA),
}

// watchdogText is the healthy script output with the key, value pairs in kv applied.
func watchdogText(kv ...string) string {
	m := maps.Clone(healthyWatchdog)
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return factsText(m)
}

func TestWatchdogProblems(t *testing.T) {
	tests := []struct {
		name        string
		text        string
		wantProblem string // empty: no problems
		wantRolling bool
	}{
		{"healthy", watchdogText(), "", false},
		{"rollout in progress", watchdogText("Trial", "yes", "Current", oldSHA), "", true},
		{"trial.next waiting", watchdogText("TrialNext", "yes"), "", true},
		{"wrong ExecStart", watchdogText("ExecStart", "/home/x/.local/bin/cos serve"), "ExecStart=", false},
		{"no revert in ExecStopPost", watchdogText("ExecStopPost", "/bin/sh -c 'true'"), "ExecStopPost has no trial revert", false},
		{"Type simple", watchdogText("Type", "simple"), "Type=simple", false},
		{"4 builds, one extra", watchdogText("Builds", "4"), "4 builds, want <= 3", false},
		{"4 builds, good pinned outside the newest", watchdogText("Builds", "4", "Current", fourthSHA, "Good", fourthSHA, "Version", versionText(fourthSHA)), "4 builds, want <= 3", false},
		{"version reports a trial", watchdogText("Version", fmt.Sprintf(`{"protocol":1,"rollout":2,"revision":%q,"good":%q,"trial":true}`, testSHA, testSHA)), "reports a trial", false},
		{"old protocol", watchdogText("Version", fmt.Sprintf(`{"protocol":0,"rollout":2,"revision":%q,"good":%q}`, testSHA, testSHA)), "protocol=0", false},
		{"old rollout controller", watchdogText("Version", fmt.Sprintf(`{"protocol":1,"rollout":1,"revision":%q,"good":%q}`, testSHA, testSHA)), "rollout=1, want >= 2", false},
		{"version is not JSON", watchdogText("Version", "cos: not found"), "no JSON", false},
		{"version is another build", watchdogText("Version", versionText(oldSHA)), "revision=", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			problems, rolling := watchdogProblems(parseWatchdog(tc.text))
			if rolling != tc.wantRolling {
				t.Fatalf("rolling = %t, want %t (problems %v)", rolling, tc.wantRolling, problems)
			}
			if tc.wantProblem == "" && len(problems) > 0 {
				t.Fatalf("problems = %v, want none", problems)
			}
			if tc.wantProblem != "" && !strings.Contains(strings.Join(problems, "; "), tc.wantProblem) {
				t.Fatalf("problems = %v, want one containing %q", problems, tc.wantProblem)
			}
		})
	}
}

func TestCheckWatchdog(t *testing.T) {
	tests := []struct {
		name, want, wantEvidence string
		text                     string
	}{
		{"healthy", "pass", "unit and watchdog as deployed", watchdogText()},
		{"rollout in progress", "skip", "rollout in progress on delta", watchdogText("Trial", "yes", "Current", oldSHA)},
		{"wrong ExecStart", "fail", "ExecStart=", watchdogText("ExecStart", "/usr/bin/cos serve")},
		{"4 builds", "fail", "4 builds", watchdogText("Builds", "4")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, fr := fakeEnv(map[string]map[string]string{"delta": with()}, e2eMain{}, nil)
			env.run = func(ctx context.Context, host, script string) (string, error) {
				if script == e2eWatchdogScript {
					return tc.text, nil
				}
				return fr.run(ctx, host, script)
			}
			got, err := checkWatchdog(t.Context(), env, "delta")
			if s := statusOf(err); s != tc.want {
				t.Fatalf("status = %s (%s), want %s", s, evidenceOf(got, err), tc.want)
			}
			if ev := evidenceOf(got, err); !strings.Contains(ev, tc.wantEvidence) {
				t.Fatalf("evidence %q does not contain %q", ev, tc.wantEvidence)
			}
		})
	}
}

func TestReleaseSignal(t *testing.T) {
	signal := "event: datastar-patch-signals\ndata: signals {\"_release\":\"" + oldSHA + "\"}\n\n"
	tests := []struct {
		name, stream, wantRev string
		wantUnknown, wantErr  bool
	}{
		{"release unknown", ": release unknown\n\n", "", true, false},
		{"release known", signal, oldSHA, false, false},
		{"no event in the window", "", "", false, true},
		{"event without data", "event: datastar-patch-signals\n\n", "", false, true},
		{"data is not JSON", "event: datastar-patch-signals\ndata: signals {nope\n\n", "", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rev, unknown, err := releaseSignal(tc.stream)
			if (err != nil) != tc.wantErr || rev != tc.wantRev || unknown != tc.wantUnknown {
				t.Fatalf("releaseSignal = (%q, %t, %v), want (%q, %t, err %t)", rev, unknown, err, tc.wantRev, tc.wantUnknown, tc.wantErr)
			}
		})
	}
}

// reconnectNode is a dashboard that answers the reconnect check: /version, a /release stream
// that ends after one signal (or "release unknown" when release is empty), and a page with releaseWatch.
func reconnectNode(host, revision, release string, versionStatus int) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/login":
			http.SetCookie(rw, &http.Cookie{Name: sessionCookie, Value: "u.token"})
			http.Redirect(rw, r, "/", http.StatusSeeOther)
		case !strings.Contains(r.Header.Get("Cookie"), "u.token"):
			http.Redirect(rw, r, "/login", http.StatusSeeOther)
		case r.URL.Path == "/version" && versionStatus != http.StatusOK:
			http.Error(rw, "not found", versionStatus)
		case r.URL.Path == "/version":
			fmt.Fprintf(rw, `{"build":{"protocol":1,"host":%q,"revision":%q},"release":{"revision":%q}}`, host, revision, release)
		case r.URL.Path == "/release" && release == "":
			fmt.Fprint(rw, ": release unknown\n\n")
		case r.URL.Path == "/release":
			fmt.Fprintf(rw, "event: datastar-patch-signals\ndata: signals {\"_release\":%q}\n\n", release)
		case r.URL.Path == "/":
			fmt.Fprint(rw, `<body data-init="@get(&#39;/release&#39;)">`)
		}
	}
}

func TestCheckReconnect(t *testing.T) {
	tests := []struct {
		name, want, wantEvidence string
		node                     func(host string) http.HandlerFunc
	}{
		{"release known", "pass", "releaseWatch rendered", func(h string) http.HandlerFunc {
			return reconnectNode(h, testSHA, oldSHA, http.StatusOK)
		}},
		{"release unknown", "pass", "release=", func(h string) http.HandlerFunc {
			return reconnectNode(h, testSHA, "", http.StatusOK)
		}},
		{"version not served yet", "skip", "not served on main yet", func(h string) http.HandlerFunc {
			return reconnectNode(h, testSHA, "", http.StatusNotFound)
		}},
		{"nodes on different builds", "skip", "rollout in progress", func(h string) http.HandlerFunc {
			if h == "delta" {
				return reconnectNode(h, oldSHA, "", http.StatusOK)
			}
			return reconnectNode(h, testSHA, "", http.StatusOK)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			urls := make([]string, len(e2eHosts))
			for i, h := range e2eHosts {
				srv := httptest.NewServer(tc.node(h))
				t.Cleanup(srv.Close)
				urls[i] = srv.URL
			}
			got, err := checkReconnect(t.Context(), urls, &e2eUser{name: "e2e-test", password: "pw"})
			if s := statusOf(err); s != tc.want {
				t.Fatalf("status = %s (%s), want %s", s, evidenceOf(got, err), tc.want)
			}
			if ev := evidenceOf(got, err); !strings.Contains(ev, tc.wantEvidence) {
				t.Fatalf("evidence %q does not contain %q", ev, tc.wantEvidence)
			}
		})
	}
}
