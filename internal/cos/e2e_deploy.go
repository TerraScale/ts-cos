package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Deploy checks read the TS-818 build store and watchdog unit: what systemd starts, which build
// is current and good, and whether a rollout is still in progress. They change nothing.

// watchdogGuard is what the unit's ExecStartPre and ExecStopPost must both contain: the revert of an unconfirmed trial.
const watchdogGuard = "if [ -e trial ]"

// e2eWatchdogScript prints the unit's directives and the build store in one ssh round trip.
// Links are printed as bare revisions. Builds counts the build store (at most keepBuilds, TS-818).
const e2eWatchdogScript = `d=$HOME/.local/share/cos
systemctl --user cat ` + e2eUnit + ` | grep -E '^(Type|TimeoutStartSec|ExecStartPre|ExecStopPost|ExecStart)='
echo "Current=$(basename "$(readlink "$d/current")")"
echo "Good=$(basename "$(readlink "$d/good")")"
echo "Trial=$([ -e "$d/trial" ] && echo yes)"
echo "TrialNext=$([ -e "$d/trial.next" ] && echo yes)"
echo "Builds=$(ls "$d/builds" 2>/dev/null | wc -l)"
echo "Version=$("$d/current/cos" version 2>&1 | tr -d '\n ')"
`

// watchdogFacts is the parsed output of e2eWatchdogScript.
type watchdogFacts struct {
	Type, TimeoutStartSec, ExecStartPre, ExecStopPost, ExecStart string
	Current, Good                                                string
	Trial, TrialNext                                             bool
	Builds                                                       int // -1 when the store cannot be listed
	Version                                                      string
}

func parseWatchdog(out string) watchdogFacts {
	f := watchdogFacts{Builds: -1}
	for line := range strings.Lines(out) {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch k {
		case "Type":
			f.Type = v
		case "TimeoutStartSec":
			f.TimeoutStartSec = v
		case "ExecStartPre":
			f.ExecStartPre = v
		case "ExecStopPost":
			f.ExecStopPost = v
		case "ExecStart":
			f.ExecStart = v
		case "Current":
			f.Current = v
		case "Good":
			f.Good = v
		case "Trial":
			f.Trial = v == "yes"
		case "TrialNext":
			f.TrialNext = v == "yes"
		case "Builds":
			if n, err := strconv.Atoi(v); err == nil {
				f.Builds = n
			}
		case "Version":
			f.Version = v
		}
	}
	return f
}

// watchdogProblems lists what must hold. The unit and the links must always hold. During a rollout
// (trial, trial.next, or current != good) the build store and the running version legitimately differ,
// so rolling is true and those are not checked.
func watchdogProblems(f watchdogFacts) (problems []string, rolling bool) {
	if f.Type != "notify" {
		problems = append(problems, "Type="+f.Type+", want notify")
	}
	if f.TimeoutStartSec != "60" && f.TimeoutStartSec != "60s" {
		problems = append(problems, "TimeoutStartSec="+f.TimeoutStartSec+", want 60")
	}
	if !strings.Contains(f.ExecStartPre, watchdogGuard) {
		problems = append(problems, "ExecStartPre has no trial revert ("+watchdogGuard+")")
	}
	if !strings.Contains(f.ExecStopPost, watchdogGuard) {
		problems = append(problems, "ExecStopPost has no trial revert ("+watchdogGuard+")")
	}
	if f.ExecStart != "%h/.local/share/cos/current/cos serve" {
		problems = append(problems, "ExecStart="+f.ExecStart+", want %h/.local/share/cos/current/cos serve")
	}
	if !validRevision(f.Current) || !validRevision(f.Good) {
		problems = append(problems, fmt.Sprintf("current=%q good=%q: links must point at builds/<40-hex>", f.Current, f.Good))
	}
	if f.Trial || f.TrialNext || f.Current != f.Good {
		return problems, true
	}
	var v Build
	if err := json.Unmarshal([]byte(f.Version), &v); err != nil {
		problems = append(problems, "cos version prints no JSON: "+e2eClip(f.Version, 80))
	} else {
		if v.Protocol < releaseProtocol {
			problems = append(problems, fmt.Sprintf("protocol=%d, want >= %d", v.Protocol, releaseProtocol))
		}
		if v.Revision != f.Good || v.Good != f.Good {
			problems = append(problems, fmt.Sprintf("cos version revision=%.12s good=%.12s, want good=%.12s", v.Revision, v.Good, f.Good))
		}
		if v.Trial {
			problems = append(problems, "cos version reports a trial with no rollout")
		}
		if v.Rollout < rolloutProtocol {
			problems = append(problems, fmt.Sprintf("rollout=%d, want >= %d", v.Rollout, rolloutProtocol))
		}
	}
	switch {
	case f.Builds < 0:
		problems = append(problems, "build store cannot be listed")
	case f.Builds > keepBuilds:
		problems = append(problems, fmt.Sprintf("%d builds, want <= %d (current, good and trial)", f.Builds, keepBuilds))
	}
	return problems, false
}

func checkWatchdog(ctx context.Context, e *E2EEnv, host string) (string, error) {
	if _, err := e.deployed(ctx, host); err != nil {
		return "", err
	}
	out, err := e.run(ctx, host, e2eWatchdogScript)
	if err != nil {
		return "", err
	}
	f := parseWatchdog(out)
	problems, rolling := watchdogProblems(f)
	switch {
	case len(problems) > 0:
		return "", fmt.Errorf("%s: %s", host, strings.Join(problems, "; "))
	case rolling:
		return "", e2eSkip(fmt.Sprintf("rollout in progress on %s: current=%.12s good=%.12s trial=%t trial.next=%t", host, f.Current, f.Good, f.Trial, f.TrialNext))
	}
	return fmt.Sprintf("%s: %s, current=good=%.12s, %d builds, unit and watchdog as deployed", host, f.Type, f.Good, f.Builds), nil
}

// e2eReleaseWatch matches the rendered form of @releaseWatch(): the page body opens GET /release.
var e2eReleaseWatch = regexp.MustCompile(`data-init="@get\((?:'|&#39;)/release(?:'|&#39;)`)

// e2eReleaseWindow is how long the /release stream is read; it never ends on its own.
const e2eReleaseWindow = 3 * time.Second

// versionReply is the part of GET /version the reconnect check reads.
type versionReply struct {
	Build struct {
		Protocol int    `json:"protocol"`
		Host     string `json:"host"`
		Revision string `json:"revision"`
		Trial    bool   `json:"trial"`
	} `json:"build"`
	Release struct {
		Revision string `json:"revision"`
	} `json:"release"`
}

// releaseSignal reads a /release chunk: the _release signal, or unknown for the ": release unknown" comment.
func releaseSignal(stream string) (rev string, unknown bool, err error) {
	if strings.Contains(stream, ": release unknown") {
		return "", true, nil
	}
	if !strings.Contains(stream, "event: datastar-patch-signals") {
		return "", false, errors.New("no datastar-patch-signals event in the first 3 s")
	}
	for line := range strings.Lines(stream) {
		data, ok := strings.CutPrefix(strings.TrimSpace(line), "data: signals ")
		if !ok {
			continue
		}
		var signals struct {
			Release string `json:"_release"`
		}
		if err := json.Unmarshal([]byte(data), &signals); err != nil {
			return "", false, fmt.Errorf("bad _release signal: %w", err)
		}
		return signals.Release, false, nil
	}
	return "", false, errors.New("datastar-patch-signals event has no data line")
}

// checkReconnect signs in on every node and checks the release endpoints a browser reconnects to.
// Until GET /version is served (not on main yet), the check skips.
func checkReconnect(ctx context.Context, urls []string, u *e2eUser) (string, error) {
	type node struct {
		host, base, cookie string
		version            versionReply
	}
	var nodes []node
	for i, base := range urls {
		host := e2eHosts[i]
		cookie, status, _, err := signIn(ctx, base, u.name, u.password)
		if err != nil || status != http.StatusSeeOther {
			return "", fmt.Errorf("login on %s: HTTP %d %v", host, status, err)
		}
		status, _, _, body, err := e2eRequest(ctx, "GET", base+"/version", cookie, nil)
		if err != nil {
			return "", err
		}
		if status == http.StatusNotFound {
			return "", e2eSkip("TS-818: GET /version is not served on main yet; reconnect and browser reload are not built")
		}
		var v versionReply
		if status != http.StatusOK || json.Unmarshal([]byte(body), &v) != nil {
			return "", fmt.Errorf("GET %s/version: HTTP %d, want 200 with JSON", host, status)
		}
		if v.Build.Host != host || v.Build.Protocol < releaseProtocol || v.Build.Revision == "" {
			return "", fmt.Errorf("%s /version: host %q protocol %d revision %q", host, v.Build.Host, v.Build.Protocol, v.Build.Revision)
		}
		nodes = append(nodes, node{host, base, cookie, v})
	}
	revision := nodes[0].version.Build.Revision
	var reports []string
	rolling := false
	for _, n := range nodes {
		reports = append(reports, fmt.Sprintf("%s=%.12s", n.host, n.version.Build.Revision))
		rolling = rolling || n.version.Build.Trial || n.version.Build.Revision != revision
	}
	if rolling {
		return "", e2eSkip("rollout in progress: " + strings.Join(reports, " "))
	}
	for _, n := range nodes {
		rctx, cancel := context.WithTimeout(ctx, e2eReleaseWindow)
		_, _, _, stream, err := e2eRequest(rctx, "GET", n.base+"/release", n.cookie, nil)
		cancel()
		if err != nil {
			return "", err
		}
		rev, unknown, err := releaseSignal(stream)
		if err != nil {
			return "", fmt.Errorf("%s /release: %w", n.host, err)
		}
		// Before any completed rollout /version has no release and /release says unknown; after one, the signal is that revision.
		want := n.version.Release.Revision
		if !(unknown && want == "" || !unknown && want != "" && rev == want) {
			return "", fmt.Errorf("%s /release: release=%q unknown=%t, want the _release signal for %q (or unknown when there is no release)", n.host, rev, unknown, want)
		}
		status, _, _, page, err := e2eRequest(ctx, "GET", n.base+"/", n.cookie, nil)
		if err != nil || status != http.StatusOK || !e2eReleaseWatch.MatchString(page) {
			return "", fmt.Errorf("%s dashboard: HTTP %d without the releaseWatch marker %v", n.host, status, err)
		}
	}
	return fmt.Sprintf("build %.12s on every node (%s); /release streams _release=%q; releaseWatch rendered on every node", revision, strings.Join(reports, " "), nodes[0].version.Release.Revision), nil
}
