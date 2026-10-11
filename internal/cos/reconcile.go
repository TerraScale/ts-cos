package cos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The reconciler covers what the event stream missed (a dropped webhook): each beat diffs
// live PR state against the fingerprints of the previous beat and publishes synthetic
// events, which the rules engine handles like real ones. Thread reconciliation is deferred:
// a synthetic turn.failed would duplicate the real one under a different event ID and
// count as a second failure for same-failure escalation.
const (
	reconcileKey      = "reconcile" // COS_AUTOPILOT KV key holding the fingerprints
	reconcileBaseline = "baseline"  // set by the first beat; until then nothing is emitted
)

// reconcileEvents compares live PR state with the fingerprints seen on the previous beat and
// returns synthetic events for every change. Pure: the caller loads and stores `seen`.
func reconcileEvents(live Live, seen map[string]string, now time.Time) (events []Event, next map[string]string) {
	_, baselined := seen[reconcileBaseline]
	next = map[string]string{reconcileBaseline: "1"}
	emit := func(e Event) {
		if err := e.Validate(); err != nil {
			slog.Warn("reconciler skipped an invalid event", "id", e.ID, "error", err)
			return
		}
		events = append(events, e)
	}
	for _, p := range live.PRs {
		if !p.Cos {
			continue
		}
		key := "pr:" + p.Repo + "#" + strconv.Itoa(p.Number)
		fp := p.SHA + "|" + p.CI + "|" + strconv.FormatBool(p.Mergeable) + "|" + strconv.FormatBool(p.ChangesRequested) + "|" + strconv.FormatBool(p.BotApproved)
		next[key] = fp
		if !baselined || seen[key] == fp {
			continue
		}
		old := strings.Split(seen[key], "|") // SHA|CI|mergeable|changesRequested|botApproved
		ciChanged := len(old) < 5 || old[1] != p.CI
		reviewChanged := len(old) < 5 || old[3] != strconv.FormatBool(p.ChangesRequested) || old[4] != strconv.FormatBool(p.BotApproved)
		kind, sev := KindPRMergeable, SeverityInfo
		switch {
		case ciChanged || p.CI == "failure":
			kind = KindCIStatus
		case !p.Mergeable:
			kind = KindPRConflict
		case reviewChanged:
			kind = KindReview
		}
		if p.CI == "failure" || !p.Mergeable {
			sev = SeverityWarn
		}
		var data json.RawMessage
		if kind == KindCIStatus {
			data, _ = json.Marshal(map[string]string{"state": p.CI})
		}
		emit(Event{ID: NewEventID(SourceForgejo, "reconcile", key, reconcileShort(fp)), Source: SourceForgejo, Kind: kind,
			Time: now, Severity: sev, Repo: "TerraScale/" + p.Repo, PR: p.Number, Synthetic: true, Data: data,
			Summary: EventText(fmt.Sprintf("%s#%d reconciled: CI %s, mergeable %t, review %s", p.Repo, p.Number, p.CI, p.Mergeable, reviewLabel(p)), eventSummaryMax)})
	}
	return events, next
}

// reconcileSynthetic publishes what reconcileEvents derives and stores the fingerprints
// only after every event is in the stream, so a failed publish is retried next beat.
func reconcileSynthetic(ctx context.Context, js jetstream.JetStream, kv jetstream.KeyValue, live Live) []AutopilotEntry {
	seen := map[string]string{}
	if e, err := kv.Get(ctx, reconcileKey); err == nil {
		if err := json.Unmarshal(e.Value(), &seen); err != nil {
			return []AutopilotEntry{{Kind: "error", Action: "Read reconciler state", Reason: err.Error()}}
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return []AutopilotEntry{{Kind: "error", Action: "Read reconciler state", Reason: err.Error()}}
	}
	events, next := reconcileEvents(live, seen, time.Now().UTC())
	for _, e := range events {
		if _, err := PublishEvent(ctx, js, e); err != nil {
			return []AutopilotEntry{{Kind: "error", Action: "Publish reconciled event", Target: e.ID, Reason: err.Error()}}
		}
	}
	data, err := json.Marshal(next)
	if err == nil {
		_, err = kv.Put(ctx, reconcileKey, data)
	}
	if err != nil {
		return []AutopilotEntry{{Kind: "error", Action: "Save reconciler state", Reason: err.Error()}}
	}
	if len(events) == 0 {
		return nil
	}
	return []AutopilotEntry{{Kind: "record", Action: "Reconciler",
		Reason: fmt.Sprintf("%d synthetic events", len(events))}}
}

// reconcileHash is the fingerprint of a value: its SHA-256 in hex.
func reconcileHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// reconcileShort keeps 16 hex digits of a fingerprint for the event ID.
func reconcileShort(s string) string { return reconcileHash(s)[:16] }
