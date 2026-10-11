package cos

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
)

// A full run waits for the cluster to settle after rollouts, so checks never read a node mid-restart (TS-816).
// Smoke runs skip this: the rollout gate calls E2ESmoke while a rollout is active.

const (
	e2eSettle     = 5 * time.Minute
	e2eSettlePoll = 5 * time.Second
)

// settleHost is one host's reading in a settle poll.
type settleHost struct {
	Name  string
	Build string // revision the running binary reports, "" when unknown
	Unit  string // ActiveState/SubState
	Login int    // HTTP status of GET /login, 0 when the request failed
	Err   string // why a read failed, if it did
}

func (h settleHost) ok() bool {
	return h.Err == "" && h.Unit == "active/running" && h.Build != "" && h.Login == http.StatusOK
}

func (h settleHost) String() string {
	build := cmp.Or(h.Build, "unknown")
	s := fmt.Sprintf("%s=%.12s %s login %d", h.Name, build, h.Unit, h.Login)
	if h.Err != "" {
		s += " (" + e2eClip(h.Err, 120) + ")"
	}
	return s
}

// settleState is one poll, or the last poll of a wait that never settled.
type settleState struct {
	Settled bool
	Waited  time.Duration
	Rollout string // id of the newest rollout at this poll
	Phase   string // its phase, or why it could not be read
	Hosts   []settleHost
}

func (st settleState) describe() string {
	rollout := "no rollout"
	switch {
	case st.Rollout != "":
		rollout = "rollout " + st.Rollout + " " + st.Phase
	case st.Phase != "":
		rollout = st.Phase
	}
	parts := []string{rollout}
	for _, h := range st.Hosts {
		parts = append(parts, h.String())
	}
	return strings.Join(parts, "; ")
}

// isSettled: no rollout active, and every host runs one build, active and running, answering /login with 200.
func isSettled(r Rollout, hosts []settleHost) bool {
	if r.Active() || len(hosts) != len(e2eHosts) {
		return false
	}
	for _, h := range hosts {
		if !h.ok() || h.Build != hosts[0].Build {
			return false
		}
	}
	return true
}

// lastRollout reads the newest rollout record over the runner's bus.
func (e *E2EEnv) lastRollout(ctx context.Context) (Rollout, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return Rollout{}, err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return Rollout{}, err
	}
	kv, err := deployKV(ctx, js)
	if err != nil {
		return Rollout{}, err
	}
	r, _, err := loadRollout(ctx, kv)
	return r, err
}

func (e *E2EEnv) settleHost(ctx context.Context, host string) settleHost {
	h := settleHost{Name: host}
	f, err := e.hostFacts(ctx, host, true)
	if err != nil {
		h.Err = err.Error()
		return h
	}
	var v struct {
		Revision string `json:"revision"`
	}
	_ = json.Unmarshal([]byte(f["Version"]), &v)
	h.Build, h.Unit = v.Revision, f["ActiveState"]+"/"+f["SubState"]
	base, err := e.nodeURL(ctx, host)
	if err != nil {
		h.Err = err.Error()
		return h
	}
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	h.Login, _, _, _, err = e2eRequest(call, "GET", base+"/login", "", nil)
	if err != nil {
		h.Err = err.Error()
	}
	return h
}

// settlePoll reads the newest rollout and every host once. An unreadable rollout is not settled, not an error.
func (e *E2EEnv) settlePoll(ctx context.Context) settleState {
	r, err := e.lastRollout(ctx)
	st := settleState{Rollout: r.ID, Phase: r.Phase}
	if err != nil {
		st.Phase = "unreadable: " + e2eClip(err.Error(), 120)
	}
	for _, h := range e2eHosts {
		st.Hosts = append(st.Hosts, e.settleHost(ctx, h))
	}
	st.Settled = err == nil && isSettled(r, st.Hosts)
	return st
}

// settle polls every 5 s for up to e2eSettle and returns the first settled reading, else the last one.
func (e *E2EEnv) settle(ctx context.Context) (settleState, error) {
	ctx, cancel := context.WithTimeout(ctx, e2eSettle)
	defer cancel()
	start := time.Now()
	for {
		st := e.settlePoll(ctx)
		st.Waited = time.Since(start)
		if st.Settled {
			return st, nil
		}
		select {
		case <-ctx.Done():
			return st, nil
		case <-time.After(e2eSettlePoll):
		}
	}
}

// settledResult is the cluster.settled check: it fails with the last reading when the cluster never settled.
func settledResult(st settleState, err error) E2EResult {
	r := E2EResult{ID: "cluster.settled", Group: "cluster", Stream: "TS-816", Status: "pass", DurationMS: st.Waited.Milliseconds()}
	switch {
	case err != nil:
		r.Status, r.Evidence = "fail", err.Error()
	case !st.Settled:
		r.Status, r.Evidence = "fail", "cluster not settled after "+e2eSettle.String()+": "+st.describe()
	default:
		r.Evidence = fmt.Sprintf("settled after %s: build %.12s on all nodes, no rollout in progress", st.Waited.Round(time.Second), st.Hosts[0].Build)
	}
	r.Evidence = e2eClip(r.Evidence, 600)
	return r
}

func unsettledSkip() string {
	return "cluster not settled after " + e2eSettle.String() + " (see cluster.settled)"
}

// skipChecks keeps the selected checks but makes each one a skip with reason, so none runs.
func skipChecks(checks []E2ECheck, reason string) []E2ECheck {
	out := slices.Clone(checks)
	for i := range out {
		out[i].Run = func(context.Context, *E2EEnv) (string, error) { return "", e2eSkip(reason) }
	}
	return out
}

// addSettled puts the cluster.settled result first and recounts the summary.
func addSettled(report E2EReport, settled E2EResult) E2EReport {
	report.Checks = append([]E2EResult{settled}, report.Checks...)
	report.Summary = countChecks(report.Checks)
	return report
}

func countChecks(results []E2EResult) map[string]int {
	summary := map[string]int{"pass": 0, "fail": 0, "skip": 0}
	for _, r := range results {
		summary[r.Status]++
	}
	return summary
}

// runFull is a full run (no --smoke): wait for the cluster to settle, run the checks, then retry after a mid-run rollout.
func runFull(ctx context.Context, env *E2EEnv, checks []E2ECheck, args []string) E2EReport {
	start := time.Now()
	st, err := env.settle(ctx)
	settled := settledResult(st, err)
	if settled.Status == "fail" {
		checks = skipChecks(checks, unsettledSkip())
	}
	report := addSettled(runE2E(ctx, env, checks, args), settled)
	if settled.Status == "pass" {
		retryAfterRollout(ctx, env, checks, &report, st.Rollout)
	}
	report.StartedAt, report.DurationMS = start.UTC(), time.Since(start).Milliseconds()
	return report
}

// retryableIDs lists the failed non-chaos checks: chaos perturbs the cluster, so it is never rerun.
func retryableIDs(report E2EReport, checks []E2ECheck) []string {
	var ids []string
	for _, r := range report.Checks {
		i := slices.IndexFunc(checks, func(c E2ECheck) bool { return c.ID == r.ID })
		if r.Status == "fail" && i >= 0 && !checks[i].Chaos {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

// mergeRetry replaces each retried result, prefixed with the rollout it followed, and recounts the summary.
func mergeRetry(report *E2EReport, retried []E2EResult, rollout string) {
	for _, r := range retried {
		r.Evidence = e2eClip("retried after rollout "+rollout+": "+r.Evidence, 600)
		i := slices.IndexFunc(report.Checks, func(c E2EResult) bool { return c.ID == r.ID })
		report.Checks[i] = r
	}
	report.Summary = countChecks(report.Checks)
}

// retryAfterRollout settles again when the newest rollout changed or is active, then reruns the failed checks once.
// It does nothing when no check failed, so a clean run never waits for a settle.
func retryAfterRollout(ctx context.Context, env *E2EEnv, checks []E2ECheck, report *E2EReport, seen string) {
	failed := retryableIDs(*report, checks)
	if len(failed) == 0 {
		return
	}
	after, err := env.lastRollout(ctx)
	if err == nil && after.ID == seen && !after.Active() {
		return
	}
	st, err := env.settle(ctx)
	if err != nil || !st.Settled {
		note := fmt.Sprintf("rollout %s during the run; not retried (cluster not settled)", cmp.Or(st.Rollout, after.ID))
		for i := range report.Checks {
			if slices.Contains(failed, report.Checks[i].ID) {
				report.Checks[i].Evidence = e2eClip(report.Checks[i].Evidence+"; "+note, 600)
			}
		}
		return
	}
	rerun := slices.DeleteFunc(slices.Clone(checks), func(c E2ECheck) bool { return !slices.Contains(failed, c.ID) })
	mergeRetry(report, runE2E(ctx, env, rerun, failed).Checks, st.Rollout)
}
