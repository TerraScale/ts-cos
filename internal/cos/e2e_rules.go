package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TS-813 live checks for the rules engine: heartbeat presets and the reconciler's replay of a dropped
// webhook. Each restores what it changes, also when it fails. The wake and merge paths stay skips:
// they need a sandbox PR or a real merge, which e2e must not create.

const (
	e2eIntervalWindow = 10 * time.Second
	e2eReplayWindow   = 60 * time.Second
)

// nextPreset returns the preset after current, wrapping around. An unknown current starts at the first.
func nextPreset(current int, presets []int) int {
	return presets[(slices.Index(presets, current)+1)%len(presets)]
}

// quietPRs lists the PRs whose rules decision is RECORD only: open cos/ work, CI pending or green,
// mergeable, not ready and no changes requested. Only PRs with a fingerprint qualify, because a
// dropped fingerprint is what makes the reconciler replay them.
func quietPRs(prs []PR, seen map[string]string) []PR {
	var out []PR
	for _, p := range prs {
		_, known := seen[reconcileKeyOf(p)]
		if known && strings.HasPrefix(p.Branch, "cos/") && (p.CI == "pending" || p.CI == "success") &&
			p.Mergeable && !p.Ready && !p.ChangesRequested {
			out = append(out, p)
		}
	}
	return out
}

// withoutFingerprint is a copy of the fingerprints with one PR's entry removed.
func withoutFingerprint(seen map[string]string, key string) map[string]string {
	out := maps.Clone(seen)
	delete(out, key)
	return out
}

// replayID is the event ID the reconciler gives p's current fingerprint.
func replayID(p PR) string {
	return NewEventID(SourceForgejo, "reconcile", reconcileKeyOf(p), reconcileShort(reconcileFingerprint(p)))
}

// findReplay returns the feed entry logged for event id after sequence after, the reconciler's event for one PR's current state.
func findReplay(feed []AutopilotEntry, id string, after uint64) (AutopilotEntry, bool) {
	for _, e := range feed {
		if e.Event == id && e.Seq > after {
			return e, true
		}
	}
	return AutopilotEntry{}, false
}

// autopilotKV is the COS_AUTOPILOT bucket: the heartbeat settings and the reconciler's fingerprints.
func autopilotKV(ctx context.Context, b *Bus) (jetstream.KeyValue, error) {
	js, err := b.JetStream()
	if err != nil {
		return nil, err
	}
	return js.KeyValue(ctx, autopilotSettings)
}

// nodeSession is one node's dashboard, signed in as the first admin, who owns the autopilot state.
type nodeSession struct{ host, base, cookie string }

// nodeSessions returns the owner's session for every node, so the polls below do not repeat the sign-in; end deletes it.
func nodeSessions(ctx context.Context, e *E2EEnv, urls []string) ([]nodeSession, func(), error) {
	cookie, end, err := e.ownerSession(ctx)
	if err != nil {
		return nil, nil, err
	}
	out := make([]nodeSession, 0, len(e2eHosts))
	for i, h := range e2eHosts {
		out = append(out, nodeSession{h, urls[i], cookie})
	}
	return out, end, nil
}

// nodeAutopilot reads a node's autopilot state from its /status: the node's own SQLite mirror of COS_AUTOPILOT,
// read without a Forgejo or T3 refresh, so it answers in milliseconds.
func nodeAutopilot(ctx context.Context, s nodeSession) (AutopilotState, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/status", nil)
	if err != nil {
		return AutopilotState{}, err
	}
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: s.cookie})
	res, err := e2eHTTP.Do(req)
	if err != nil {
		return AutopilotState{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return AutopilotState{}, fmt.Errorf("GET %s/status: HTTP %d", s.base, res.StatusCode)
	}
	var v struct {
		Autopilot AutopilotState `json:"autopilot"`
	}
	if err := json.NewDecoder(res.Body).Decode(&v); err != nil {
		return AutopilotState{}, fmt.Errorf("%s: /status is not JSON: %w", s.host, err)
	}
	return v.Autopilot, nil
}

// awaitNodes calls done for each node until it reports true or the window ends, and returns the hosts still waiting.
func awaitNodes(ctx context.Context, nodes []nodeSession, window time.Duration, done func(context.Context, nodeSession) bool) []string {
	deadline := time.Now().Add(window)
	waiting := nodes
	for len(waiting) > 0 {
		var next []nodeSession
		for _, s := range waiting {
			call, cancel := context.WithDeadline(ctx, deadline)
			if !done(call, s) {
				next = append(next, s)
			}
			cancel()
		}
		waiting = next
		if len(waiting) == 0 || time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	hosts := make([]string, len(waiting))
	for i, s := range waiting {
		hosts[i] = s.host
	}
	return hosts
}

// reconcilerDroppedEvent forgets one quiet PR's fingerprint, as if its webhook never arrived, and runs a
// heartbeat. The reconciler must replay it and the rules engine must log it on the leader, visible on every node.
func reconcilerDroppedEvent(ctx context.Context, e *E2EEnv, urls []string) (evidence string, err error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	settings, err := autopilotKV(ctx, bus)
	if err != nil {
		return "", err
	}
	stored, err := settings.Get(ctx, reconcileKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", e2eSkip("no reconciler baseline yet")
	}
	if err != nil {
		return "", err
	}
	seen := map[string]string{}
	if err := json.Unmarshal(stored.Value(), &seen); err != nil {
		return "", err
	}
	f := Forgejo{Config: e.cfg}
	prs, err := f.PRs(ctx)
	if err != nil {
		return "", err
	}
	var pick PR
	found := false
	for _, p := range quietPRs(prs, seen) {
		facts, err := f.prFacts(ctx, p.Repo, p.Number)
		if err != nil {
			return "", err
		}
		// A large PR would wake its worker (size rule), so only small open PRs are replayed.
		if facts.State == "open" && !facts.Merged && facts.Lines <= prLineLimit {
			pick, found = p, true
			break
		}
	}
	if !found {
		return "", e2eSkip("no quiet cos/ PR to replay")
	}
	key := reconcileKeyOf(pick)
	id := replayID(pick)
	ref := pick.Repo + "#" + strconv.Itoa(pick.Number)
	data, err := json.Marshal(withoutFingerprint(seen, key))
	if err != nil {
		return "", err
	}
	nodes, endOwner, err := nodeSessions(ctx, e, urls)
	if err != nil {
		return "", err
	}
	defer endOwner()
	// Restored when the replay fails. After a successful beat the beat writes the fingerprint back itself.
	defer func() {
		if err == nil {
			return
		}
		restore, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if _, perr := settings.Put(restore, reconcileKey, stored.Value()); perr != nil {
			err = errors.Join(err, fmt.Errorf("restore reconciler state: %w", perr))
		}
	}()
	js, err := bus.JetStream()
	if err != nil {
		return "", err
	}
	logStream, err := js.Stream(ctx, autopilotLogStream)
	if err != nil {
		return "", err
	}
	logInfo, err := logStream.Info(ctx)
	if err != nil {
		return "", err
	}
	// Only entries past this sequence are this run's replay; an earlier run or reconcile logged the same id.
	after := logInfo.State.LastSeq
	if _, err := settings.Put(ctx, reconcileKey, data); err != nil {
		return "", err
	}
	// A beat already running, or a leader handover, may have passed the reconcile step before the drop, so wait for it and start another.
	holder, runErr := runNowUntil(ctx, func(c context.Context) (string, error) { return autopilotRunNow(c, bus.conn) }, 45*time.Second, 2*time.Second)
	if errors.Is(runErr, errAutopilotBusy) {
		return "", e2eSkip("a heartbeat stayed busy for 45 s; no replay this run")
	}
	if runErr != nil {
		return "", runErr
	}
	logged := map[string]AutopilotEntry{}
	waiting := awaitNodes(ctx, nodes, e2eReplayWindow, func(call context.Context, s nodeSession) bool {
		state, err := nodeAutopilot(call, s)
		if err != nil {
			return false
		}
		entry, ok := findReplay(state.Log, id, after)
		if ok {
			logged[s.host] = entry
		}
		return ok
	})
	if len(waiting) > 0 {
		return "", fmt.Errorf("replayed event for %s is not in the feed of %s within %s", ref, strings.Join(waiting, ", "), e2eReplayWindow)
	}
	return replayEvidence(logged[holder], holder, ref, id, e2eHosts)
}

// runNowUntil calls run until it succeeds or wait elapses, so a busy beat or a leader handover clears before the check fails.
func runNowUntil(ctx context.Context, run func(context.Context) (string, error), wait, pause time.Duration) (string, error) {
	until := time.Now().Add(wait)
	for {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		holder, err := run(call)
		cancel()
		if err == nil || time.Now().After(until) || ctx.Err() != nil {
			return holder, err
		}
		select {
		case <-ctx.Done():
		case <-time.After(pause):
		}
	}
}

// replayEvidence judges the leader's feed entry for a dropped fingerprint. A "Replay dropped" entry is the
// stream's dedup proof that the state was already handled once; any other entry is a rules replay. Both must be on the leader.
func replayEvidence(entry AutopilotEntry, holder, ref, id string, hosts []string) (string, error) {
	if entry.Host != holder {
		return "", fmt.Errorf("replayed event %s was logged on %s, not on the leader %s", entry.Event, entry.Host, holder)
	}
	if entry.Action == reconcileDropped {
		return fmt.Sprintf("PR %s fingerprint dropped; leader %s re-detected it and re-published %s; the stream already held it (24 h dedup), so it is handled once; visible on %s",
			ref, holder, id, strings.Join(hosts, ", ")), nil
	}
	return fmt.Sprintf("PR %s dropped and replayed as %s; logged by leader %s (%s, %s); visible on %s",
		ref, entry.Event, holder, entry.Kind, entry.Action, strings.Join(hosts, ", ")), nil
}
