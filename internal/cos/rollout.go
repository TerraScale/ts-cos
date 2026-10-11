package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The leader rolls every green main build out one host at a time: non-leaders first,
// then the leader once it has handed leadership to an updated node. A host must pass the
// gate before the next one starts; a failure restores every updated host to its previous
// build. The rollout lives in the replicated cos_deploy KV, so whichever node leads
// resumes it, and its compare-and-set refuses a second rollout while one is active.
const (
	deployBucket   = "cos_deploy" // "rollout": the current or last rollout; "failed.<revision>": never retried by itself
	rolloutKey     = "rollout"
	gateTimeout    = 2 * time.Minute // the watchdog's 60 s, a restart and the smoke checks
	rolloutTick    = 5 * time.Second
	mainCheckEvery = time.Minute
	// mainQuiet is how long main must stay unchanged before its newest build rolls out, so a
	// merge train lands as one rolling restart instead of one per merge.
	mainQuiet = 3 * time.Minute
)

const (
	phaseInstall    = "install"  // Hosts[Step] gets To next
	phaseGate       = "gate"     // Hosts[Step] restarted into To and must pass
	phaseRollback   = "rollback" // Pending hosts go back to From, last updated first
	phaseCompleted  = "completed"
	phaseRolledBack = "rolled_back"
	phaseSuperseded = "superseded" // main moved on: Hosts[:Step] run To, and the newest main build rolls out next
)

type Rollout struct {
	ID      string            `json:"id"`
	To      string            `json:"to"`
	Change  string            `json:"change"`
	Hosts   []string          `json:"hosts"` // update order, leader last
	Step    int               `json:"step"`
	Phase   string            `json:"phase"`
	From    map[string]string `json:"from"`              // each updated host's last-known-good before the rollout
	Pending []string          `json:"pending,omitempty"` // hosts on To, or still to restore while rolling back
	Failure string            `json:"failure,omitempty"`
	Broken  bool              `json:"broken,omitempty"` // the build itself failed (the watchdog reverted it), not the cluster
	Main    bool              `json:"main,omitempty"`   // a rollout of main; a newer main build supersedes it between hosts
	Last    string            `json:"last,omitempty"`   // latest gate observation, for the failure reason
	Healthy time.Time         `json:"healthy,omitzero"` // first of two healthy gate readings
	Started time.Time         `json:"started"`
	Since   time.Time         `json:"since"` // start of the current phase
}

func (r Rollout) Active() bool {
	return r.Phase == phaseInstall || r.Phase == phaseGate || r.Phase == phaseRollback
}

// rolloutOps is what the controller does to hosts; busOps is the NATS implementation.
type rolloutOps interface {
	health(ctx context.Context, host string) (NodeHealth, error)
	release(ctx context.Context, host, verb, revision string) error // install, commit or restore
	smoke(ctx context.Context, host, revision string) error
}

// errHandover means the next action targets the leader itself: it resigns and an updated node continues.
var errHandover = errors.New("rollout hands leadership over before acting on the leader")

type rolloutEvent struct {
	Kind   EventKind
	Host   string
	Reason string
}

// step moves an active rollout forward by at most one action.
func step(ctx context.Context, ops rolloutOps, r *Rollout, self string, now time.Time) (*rolloutEvent, error) {
	fail := func(host, reason string) *rolloutEvent {
		r.Phase, r.Failure, r.Since, r.Last = phaseRollback, fmt.Sprintf("%s: %s", host, reason), now, ""
		return &rolloutEvent{Kind: KindGateFailed, Host: host, Reason: reason}
	}
	switch r.Phase {
	case phaseInstall:
		host := r.Hosts[r.Step]
		if host == self {
			return nil, errHandover
		}
		h, err := ops.health(ctx, host)
		if err != nil {
			if now.Sub(r.Since) > gateTimeout {
				return fail(host, "unreachable before install"), nil
			}
			return nil, nil
		}
		if _, ok := r.From[host]; !ok {
			r.From[host] = h.Good
		}
		if !slices.Contains(r.Pending, host) {
			r.Pending = append(r.Pending, host)
		}
		if h.Revision != r.To {
			if err := ops.release(ctx, host, "install", r.To); err != nil {
				return fail(host, "install: "+err.Error()), nil
			}
		}
		r.Phase, r.Since, r.Last, r.Healthy = phaseGate, now, "", time.Time{}
		return nil, nil
	case phaseGate:
		host := r.Hosts[r.Step]
		h, err := ops.health(ctx, host)
		switch {
		case err != nil:
			r.Last, r.Healthy = "not answering", time.Time{}
		case h.Reverted == r.To:
			r.Broken = true
			return fail(host, "the watchdog reverted it to its last-known-good build"), nil
		case h.Revision != r.To:
			r.Last, r.Healthy = fmt.Sprintf("runs %.12s", h.Revision), time.Time{}
		case len(h.Problems) > 0:
			r.Last, r.Healthy = strings.Join(h.Problems, "; "), time.Time{}
		default:
			if r.Healthy.IsZero() || now.Sub(r.Healthy) < 5*time.Second {
				if r.Healthy.IsZero() {
					r.Healthy = now
				}
				r.Last = "healthy once; confirming"
				break
			}
			if err := ops.smoke(ctx, host, r.To); err != nil {
				r.Last = "smoke: " + err.Error()
			} else if err := ops.release(ctx, host, "commit", r.To); err != nil {
				r.Last = "commit: " + err.Error()
			} else {
				r.Step++
				r.Phase, r.Since, r.Last, r.Healthy = phaseInstall, now, "", time.Time{}
				if r.Step == len(r.Hosts) {
					r.Phase, r.Pending = phaseCompleted, nil
				}
				return &rolloutEvent{Kind: KindNodeUpdated, Host: host}, nil
			}
		}
		if now.Sub(r.Since) > gateTimeout {
			return fail(host, "not healthy within "+gateTimeout.String()+": "+r.Last), nil
		}
		return nil, nil
	case phaseRollback:
		if len(r.Pending) == 0 {
			r.Phase = phaseRolledBack
			return nil, nil
		}
		host := r.Pending[len(r.Pending)-1]
		if host == self {
			return nil, errHandover
		}
		done := func(note string) *rolloutEvent {
			r.Pending, r.Since, r.Last = r.Pending[:len(r.Pending)-1], now, ""
			if note != "" {
				r.Failure += "; " + note
			}
			return nil
		}
		from := r.From[host]
		if from == "" {
			return done(host + " had no last-known-good build to restore"), nil
		}
		h, err := ops.health(ctx, host)
		if err == nil && h.Revision == from && !h.Trial {
			return done(""), nil
		}
		if err == nil {
			if err := ops.release(ctx, host, "restore", from); err != nil {
				r.Last = "restore: " + err.Error()
			}
		}
		if now.Sub(r.Since) > gateTimeout {
			return done(fmt.Sprintf("%s was not back on %.12s within %s", host, from, gateTimeout)), nil
		}
		return nil, nil
	}
	return nil, fmt.Errorf("rollout %s is not active", r.ID)
}

// planRollout orders the hosts that are not on revision yet: non-leaders by name, the leader last.
// It refuses unless every host answers healthy and none is on trial, since restarting
// one node while another is down would cost the cluster its quorum.
func planRollout(ctx context.Context, ops rolloutOps, hosts []string, leader, revision string) ([]string, error) {
	var plan []string
	for _, host := range hosts {
		h, err := ops.health(ctx, host)
		switch {
		case err != nil:
			return nil, fmt.Errorf("%s is not answering", host)
		case len(h.Problems) > 0:
			return nil, fmt.Errorf("%s is unhealthy: %s", host, strings.Join(h.Problems, "; "))
		case h.Trial:
			return nil, fmt.Errorf("%s is on trial", host)
		case h.Good != revision || h.Revision != revision:
			plan = append(plan, host)
		}
	}
	slices.SortFunc(plan, func(a, b string) int {
		if (a == leader) != (b == leader) {
			if a == leader {
				return 1
			}
			return -1
		}
		return strings.Compare(a, b)
	})
	return plan, nil
}

func deployKV(ctx context.Context, js jetstream.JetStream) (jetstream.KeyValue, error) {
	return js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: deployBucket, History: 5, Replicas: 3})
}

func loadRollout(ctx context.Context, kv jetstream.KeyValue) (Rollout, uint64, error) {
	entry, err := kv.Get(ctx, rolloutKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Rollout{}, 0, nil
	}
	if err != nil {
		return Rollout{}, 0, err
	}
	var r Rollout
	if err := json.Unmarshal(entry.Value(), &r); err != nil {
		return Rollout{}, 0, fmt.Errorf("unreadable rollout record: %w", err)
	}
	return r, entry.Revision(), nil
}

// saveRollout writes r only if the record is still at revision; 0 means it must not exist yet.
func saveRollout(ctx context.Context, kv jetstream.KeyValue, r Rollout, revision uint64) (uint64, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	if revision == 0 {
		return kv.Create(ctx, rolloutKey, data)
	}
	return kv.Update(ctx, rolloutKey, data, revision)
}

// startRollout records a new rollout unless one is active; concurrent starts race on the KV revision.
func startRollout(ctx context.Context, kv jetstream.KeyValue, r Rollout) error {
	current, revision, err := loadRollout(ctx, kv)
	if err != nil {
		return err
	}
	if current.Active() {
		return fmt.Errorf("rollout %s of %.12s is already %s; one rollout at a time", current.ID, current.To, current.Phase)
	}
	if _, err := saveRollout(ctx, kv, r, revision); err != nil {
		return errors.New("another rollout started at the same time; one rollout at a time")
	}
	return nil
}

func deployEvent(r Rollout, e rolloutEvent, now time.Time) Event {
	summary := map[EventKind]string{
		KindDeployStarted:    "Deploy started: %s",
		KindNodeUpdated:      "Deploy: " + e.Host + " runs %s",
		KindGateFailed:       "Deploy gate failed on " + e.Host + ": %s",
		KindDeployRolledBack: "Deploy rolled back: %s",
		KindDeployCompleted:  "Deployed: %s",
	}[e.Kind]
	severity := SeverityInfo
	if e.Kind == KindGateFailed || e.Kind == KindDeployRolledBack {
		severity = SeverityWarn
	}
	data, _ := json.Marshal(map[string]any{"rollout": r.ID, "to": r.To, "change": deployLine(r.Change, 200),
		"hosts": r.Hosts, "from": r.From, "reason": deployLine(e.Reason, 1000)})
	return Event{ID: fmt.Sprintf("deploy:%s:%s:%s", r.ID, e.Kind, e.Host), Source: SourceDeploy, Kind: e.Kind,
		Time: now, Severity: severity, Host: e.Host, Data: data,
		Summary: deployLine(fmt.Sprintf(summary, deployLine(r.Change, 80)+" ("+r.To[:12]+")"), eventSummaryMax)}
}

func publishDeploy(ctx context.Context, js jetstream.JetStream, r Rollout, e rolloutEvent) {
	if _, err := PublishEvent(ctx, js, deployEvent(r, e, time.Now().UTC())); err != nil {
		slog.Warn("deploy event not published", "kind", e.Kind, "error", err)
	}
}

// deployLine keeps text on one line of at most n runes, for event summaries and data.
func deployLine(s string, n int) string {
	s = strings.Join(strings.FieldsFunc(s, func(r rune) bool { return notGraphic(r) || r == ' ' }), " ")
	if runes := []rune(s); len(runes) > n {
		return string(runes[:n-1]) + "…"
	}
	return s
}
