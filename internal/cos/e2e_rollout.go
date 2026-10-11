package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The rollout checks read the last canary and the last rollback from the deploy events, the
// cos_deploy records and the Inbox. They change nothing: the bucket is opened, never created.

const (
	e2eRolloutRead   = 60 * time.Second // each read a rollout check makes
	e2eDeployEvents  = 2000             // deploy events read, oldest first; the stream keeps 7 days
	e2eNoRollbackMsg = "no rollback in the 7-day event stream (last live proof: 2026-10-11)"
)

// deployRun is one rollout as its deploy events tell it.
type deployRun struct {
	ID, To string
	Hosts  []string
	From   map[string]string
	Events []Event
}

// deployData is the part of a deploy event's Data that the checks read.
type deployData struct {
	Rollout string            `json:"rollout"`
	To      string            `json:"to"`
	Hosts   []string          `json:"hosts"`
	From    map[string]string `json:"from"`
}

// deployRuns groups deploy events by rollout id, oldest rollout first, events in stream order.
func deployRuns(events []Event) []deployRun {
	var runs []deployRun
	index := map[string]int{}
	for _, ev := range events {
		var d deployData
		if json.Unmarshal(ev.Data, &d) != nil || d.Rollout == "" {
			continue
		}
		i, ok := index[d.Rollout]
		if !ok {
			i = len(runs)
			index[d.Rollout] = i
			runs = append(runs, deployRun{ID: d.Rollout, To: d.To, Hosts: d.Hosts, From: d.From})
		}
		runs[i].Events = append(runs[i].Events, ev)
	}
	return runs
}

// newestRun returns the newest run whose last event is kind.
func newestRun(runs []deployRun, kind EventKind) (deployRun, bool) {
	for _, r := range slices.Backward(runs) {
		if r.Events[len(r.Events)-1].Kind == kind {
			return r, true
		}
	}
	return deployRun{}, false
}

// canaryProblems: a completed rollout started, updated every planned host once in plan order
// (leader last), and completed, with no gate failure.
func canaryProblems(r deployRun) []string {
	if len(r.Events) == 0 {
		return []string{"no events"}
	}
	var problems []string
	if first := r.Events[0].Kind; first != KindDeployStarted {
		problems = append(problems, "first event is "+string(first)+", want deploy.started")
	}
	if last := r.Events[len(r.Events)-1].Kind; last != KindDeployCompleted {
		problems = append(problems, "last event is "+string(last)+", want deploy.completed")
	}
	var updated []string
	for _, ev := range r.Events {
		switch ev.Kind {
		case KindGateFailed, KindDeployRolledBack:
			problems = append(problems, string(ev.Kind)+" in the run")
		case KindNodeUpdated:
			updated = append(updated, ev.Host)
		}
	}
	if !slices.Equal(updated, r.Hosts) {
		problems = append(problems, fmt.Sprintf("node.updated hosts %v, want %v", updated, r.Hosts))
	}
	return problems
}

// rollbackProblems: the bad build stopped at the first host that failed its gate; hosts after it never got it.
func rollbackProblems(r deployRun) []string {
	if len(r.Events) == 0 {
		return []string{"no events"}
	}
	var problems []string
	if first := r.Events[0].Kind; first != KindDeployStarted {
		problems = append(problems, "first event is "+string(first)+", want deploy.started")
	}
	if last := r.Events[len(r.Events)-1].Kind; last != KindDeployRolledBack {
		problems = append(problems, "last event is "+string(last)+", want deploy.rolled_back")
	}
	var updated, gates []string
	var before int // node.updated events before the first gate.failed
	for _, ev := range r.Events {
		switch ev.Kind {
		case KindNodeUpdated:
			updated = append(updated, ev.Host)
		case KindGateFailed:
			if len(gates) == 0 {
				before = len(updated)
			}
			gates = append(gates, ev.Host)
		case KindDeployCompleted:
			problems = append(problems, "deploy.completed in the run")
		}
	}
	switch {
	case len(gates) != 1:
		problems = append(problems, fmt.Sprintf("%d gate.failed events, want 1", len(gates)))
	case len(updated) > before:
		problems = append(problems, "node.updated after gate.failed")
	case before >= len(r.Hosts):
		problems = append(problems, "gate.failed after every planned host")
	default:
		if !slices.Equal(updated, r.Hosts[:before]) {
			problems = append(problems, fmt.Sprintf("updated %v before the failure, want %v", updated, r.Hosts[:before]))
		}
		if gates[0] != r.Hosts[before] {
			problems = append(problems, fmt.Sprintf("gate failed on %s, want %s", gates[0], r.Hosts[before]))
		}
	}
	return problems
}

// rollbackStop returns the host whose gate failed and how many hosts updated before it.
func rollbackStop(r deployRun) (string, int) {
	k := 0
	for _, ev := range r.Events {
		switch ev.Kind {
		case KindNodeUpdated:
			k++
		case KindGateFailed:
			return ev.Host, k
		}
	}
	return "", k
}

// readDeploys returns the bus, JetStream and the deploy runs of the event stream; the caller closes bus.
func (e *E2EEnv) readDeploys(ctx context.Context) (*Bus, jetstream.JetStream, []deployRun, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	js, err := bus.JetStream()
	if err != nil {
		bus.Close()
		return nil, nil, nil, err
	}
	events, err := lastEvents(ctx, js, "cos.events.deploy.>", e2eDeployEvents)
	if err != nil {
		bus.Close()
		return nil, nil, nil, err
	}
	return bus, js, deployRuns(events), nil
}

// revisionProblems names each host in want that does not run its revision.
func (e *E2EEnv) revisionProblems(ctx context.Context, want map[string]string) ([]string, error) {
	var problems []string
	for _, h := range slices.Sorted(maps.Keys(want)) {
		f, err := e.deployed(ctx, h)
		if err != nil {
			return nil, err
		}
		var v Build
		if err := json.Unmarshal([]byte(f["Version"]), &v); err != nil {
			return nil, fmt.Errorf("%s: cos version is not JSON: %w", h, err)
		}
		if v.Revision != want[h] {
			problems = append(problems, fmt.Sprintf("%s runs %.12s, want %.12s", h, v.Revision, want[h]))
		}
	}
	return problems, nil
}

func checkCanary(ctx context.Context, e *E2EEnv) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, e2eRolloutRead)
	defer cancel()
	bus, js, runs, err := e.readDeploys(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	run, ok := newestRun(runs, KindDeployCompleted)
	if !ok {
		return "", e2eSkip("no completed rollout in the 7-day event stream")
	}
	problems := canaryProblems(run)
	evidence := fmt.Sprintf("rollout %s %.12s: %s, completed in %s", run.ID, run.To,
		strings.Join(run.Hosts, " -> "), run.Events[len(run.Events)-1].Time.Sub(run.Events[0].Time).Round(time.Second))
	kv, err := js.KeyValue(ctx, deployBucket)
	if err != nil {
		return "", err
	}
	latest, _, err := loadRollout(ctx, kv)
	if err != nil {
		return "", err
	}
	switch {
	case latest.Active():
		evidence += "; host check skipped: rollout " + latest.ID + " is " + latest.Phase
	case latest.ID == run.ID && latest.Phase == phaseCompleted:
		want := map[string]string{}
		for _, h := range e2eHosts {
			want[h] = run.To
		}
		hostProblems, err := e.revisionProblems(ctx, want)
		if err != nil {
			return "", err
		}
		problems = append(problems, hostProblems...)
		evidence += fmt.Sprintf("; all hosts on %.12s", run.To)
	}
	if len(problems) > 0 {
		return "", errors.New(strings.Join(problems, "; "))
	}
	return evidence, nil
}

func checkRollback(ctx context.Context, e *E2EEnv) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, e2eRolloutRead)
	defer cancel()
	bus, js, runs, err := e.readDeploys(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	run, ok := newestRun(runs, KindDeployRolledBack)
	if !ok {
		return "", e2eSkip(e2eNoRollbackMsg)
	}
	problems := rollbackProblems(run)
	kv, err := js.KeyValue(ctx, deployBucket)
	if err != nil {
		return "", err
	}
	if _, err := kv.Get(ctx, failedKey(run.To)); err != nil {
		problems = append(problems, failedKey(run.To)+" not recorded: "+err.Error())
	}
	in, err := OpenInbox(ctx, bus, e.LocalHost(), false)
	if err != nil {
		return "", err
	}
	entry, err := in.kv.Get(ctx, inboxID("deploy:"+run.ID))
	if err != nil {
		problems = append(problems, "Inbox item deploy:"+run.ID+" not filed: "+err.Error())
	} else {
		var item InboxItem
		if json.Unmarshal(entry.Value(), &item) != nil || item.Kind != "fyi" || item.Source != "deploy" {
			problems = append(problems, "Inbox item deploy:"+run.ID+" is not an fyi from deploy")
		}
	}
	latest, _, err := loadRollout(ctx, kv)
	if err != nil {
		return "", err
	}
	if latest.ID == run.ID && latest.Phase == phaseRolledBack {
		hostProblems, err := e.revisionProblems(ctx, run.From)
		if err != nil {
			return "", err
		}
		problems = append(problems, hostProblems...)
	}
	if len(problems) > 0 {
		return "", errors.New(strings.Join(problems, "; "))
	}
	host, k := rollbackStop(run)
	return fmt.Sprintf("rollout %s %.12s: gate failed on %s after %d of %d hosts; failed.%.12s recorded; Inbox fyi filed",
		run.ID, run.To, host, k, len(run.Hosts), run.To), nil
}
