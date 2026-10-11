package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// agentDigestEvery batches escalations: escalations are batched so COS wakes once per window, not once per event.
const agentDigestEvery = 30 * time.Second

// agentDigestFetch bounds the escalations one window reads; the newest one read sets where the next window starts.
const agentDigestFetch = 500

// agentDigestSubject is the rules engine's escalation log, which the COS agent consumes.
const agentDigestSubject = "cos.autopilot.log." + ClassEscalate

// digestWake returns the escalation digest the COS agent wakes on, or "" when nothing new is worth a wake.
func (a *Agent) digestWake(ctx context.Context, st *AgentState, now time.Time) (string, error) {
	if st.DigestSince.IsZero() {
		// First start or upgrade: start from now so old escalations are never replayed.
		st.DigestSince = now
		return "", nil
	}
	if now.Sub(a.digestCheck) < agentDigestEvery {
		return "", nil
	}
	a.digestCheck = now
	since := st.DigestSince
	events, err := a.escalations(ctx, since)
	if err != nil {
		return "", err
	}
	if len(events) == 0 || !digestDue(*st, events, now) {
		return "", nil
	}
	d := BuildDigest(events, since, digestMaxBytes)
	a.recordDigestSavings(ctx, events, d, now)
	// The read is inclusive of its start time, so the last escalation is read again unless the cursor moves past it.
	st.DigestSince = events[len(events)-1].Time.Add(time.Nanosecond)
	if d.Text == "" {
		return "", nil
	}
	return fmt.Sprintf("Escalations since %s (%d events, %d merged, %d left out):\n%s", since.Format("15:04"), d.Events, d.Merged, d.Dropped, d.Text), nil
}

// agentUrgentDelay holds an urgent escalation this long, so a problem fixed meanwhile is not reported.
const agentUrgentDelay = 2 * time.Minute

// digestDue reports whether the unread escalations warrant a wake now. An urgent one is due after
// agentUrgentDelay and ignores the hourly limit; otherwise the oldest must be DigestSeconds old and fewer
// than MaxWakesPerHour digest wakes may have happened in the last hour.
func digestDue(st AgentState, events []Event, now time.Time) bool {
	for _, e := range events {
		if e.Severity == SeverityUrgent {
			if now.Sub(e.Time) >= agentUrgentDelay {
				return true
			}
			break
		}
	}
	b := CurrentBehavior().Wake
	return now.Sub(events[0].Time) >= time.Duration(b.DigestSeconds)*time.Second && st.digestWakes(now.Add(-time.Hour)) < b.MaxWakesPerHour
}

// escalations reads the escalations published after since, oldest first, up to agentDigestFetch.
func (a *Agent) escalations(ctx context.Context, since time.Time) ([]Event, error) {
	cons, err := a.js.OrderedConsumer(ctx, autopilotLogStream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{agentDigestSubject}, DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &since})
	if err != nil {
		return nil, err
	}
	out := []Event{}
	for len(out) < agentDigestFetch {
		batch, err := cons.Fetch(agentDigestFetch-len(out), jetstream.FetchMaxWait(time.Second))
		if err != nil {
			return nil, err
		}
		got, pending := 0, uint64(0)
		for m := range batch.Messages() {
			meta, err := m.Metadata()
			if err != nil {
				return nil, err
			}
			got++
			pending = meta.NumPending
			var e AutopilotEntry
			if json.Unmarshal(m.Data(), &e) == nil {
				out = append(out, escalationEvent(e, meta.Sequence.Stream, meta.Timestamp))
			}
		}
		if err := batch.Error(); err != nil {
			return nil, err
		}
		if got == 0 || pending == 0 {
			break
		}
	}
	return out, nil
}

// escalationEvent maps one rules-engine escalation onto the Event shape the digest folds.
func escalationEvent(e AutopilotEntry, seq uint64, at time.Time) Event {
	ev := Event{ID: e.Event, Kind: EventKind(e.Action), Severity: SeverityWarn, Summary: e.Reason,
		Host: e.Host, Ticket: e.Ticket, Thread: e.Target, Time: at}
	if ev.ID == "" {
		ev.ID = "seq:" + strconv.FormatUint(seq, 10)
	}
	if e.Action == "worker-question" {
		ev.Severity = SeverityUrgent
	}
	return ev
}

// recordDigestSavings counts the text COS did not have to read: duplicate lines and events past the cap.
// The bytes/4 figures are rough token estimates.
func (a *Agent) recordDigestSavings(ctx context.Context, events []Event, d WakeDigest, now time.Time) {
	if a.Savings == nil {
		return
	}
	full := 0
	for _, e := range events {
		full += len(e.Kind) + len(e.Summary) + 16
	}
	// Dropped events are costed at the average event size; the digest does not report their bytes.
	dropped := d.Dropped * full / len(events)
	if merged := full - len(d.Text) - dropped; merged > 0 {
		a.recordSaving(ctx, SavingDedup, merged/4, now)
	}
	if dropped > 0 {
		a.recordSaving(ctx, SavingDigestCap, dropped/4, now)
	}
}

func (a *Agent) recordSaving(ctx context.Context, kind string, tokens int, now time.Time) {
	if err := RecordSaving(ctx, a.Savings, kind, tokens, 0, now); err != nil {
		slog.Warn("record saving", "kind", kind, "error", err)
	}
}
