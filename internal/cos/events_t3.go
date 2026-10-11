package cos

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Each node ingests only its local T3, so an offline host never stops the others
// and no HTTP crosses nodes; the replicated stream carries events to every node.

const (
	t3BackoffMin = time.Second
	t3BackoffMax = 30 * time.Second
)

// Context usage high-water marks; each crossing emits one context.high.
var t3ContextMarks = []int{80, 90}

var t3TicketRef = regexp.MustCompile(`\b[A-Z][A-Z0-9]{1,9}-[0-9]+\b`)

type t3Tracker struct {
	host     string
	runs     map[string]string // thread -> last terminal run:status
	pending  map[string]string // thread -> pending runtime request ID
	context  map[string]int    // thread -> last context percent
	disarmed map[string]bool   // thread|run|reset -> auto-resume already turned off
}

func newT3Tracker(host string) *t3Tracker {
	return &t3Tracker{host: host, runs: map[string]string{}, pending: map[string]string{}, context: map[string]int{}, disarmed: map[string]bool{}}
}

func t3TurnKind(status string) (EventKind, Severity, bool) {
	switch status {
	case "completed":
		return KindTurnCompleted, SeverityInfo, true
	case "failed":
		return KindTurnFailed, SeverityWarn, true
	case "interrupted", "cancelled":
		return KindTurnInterrupted, SeverityInfo, true
	}
	return "", "", false
}

func (t *t3Tracker) event(row Thread, id string, kind EventKind, severity Severity, at time.Time, summary string, data map[string]any) Event {
	ticket := t3TicketRef.FindString(row.Title + " " + strings.ToUpper(row.Branch))
	if len(ticket) > 64 {
		ticket = ""
	}
	data["title"] = EventText(row.Title, 200)
	if row.ModelSelection.Model != "" {
		data["model"] = EventText(row.ModelSelection.Model, 100)
	}
	if row.Lineage.Parent != "" && len(row.Lineage.Parent) <= 256 {
		data["parent"] = row.Lineage.Parent
	}
	raw, _ := json.Marshal(data)
	thread := row.ID
	if len(thread) > 256 {
		thread = ""
	}
	return Event{ID: id, Source: SourceT3, Kind: kind, Time: at.UTC(), Severity: severity,
		Summary: EventText(EventText(row.Title, 120)+": "+summary, eventSummaryMax),
		Host:    t.host, Ticket: ticket, Thread: thread, Data: raw}
}

// observe turns one live thread row into turn and pending-request events. Rows from
// a (re)connect snapshot only emit turns that ended inside the dedup window, so a
// restart catches up on missed turns without replaying old history twice.
func (t *t3Tracker) observe(row Thread, snapshot bool, now time.Time) (events []Event, ended bool) {
	if row.ID == "" || row.ArchivedAt != nil || row.DeletedAt != nil {
		return nil, false
	}
	if kind, severity, ok := t3TurnKind(row.Status); ok && row.LatestRunID != "" && row.LatestRunDone != "" {
		key := row.LatestRunID + ":" + row.Status
		at, err := time.Parse(time.RFC3339Nano, row.LatestRunDone)
		if t.runs[row.ID] != key && err == nil {
			t.runs[row.ID] = key
			if !snapshot || now.Sub(at) < eventsDedup {
				data := map[string]any{"run": row.LatestRunID, "status": row.Status}
				if lastError := EventText(row.LastError, 500); lastError != "" {
					data["error"] = lastError
				}
				if row.LastErrorClass != "" {
					data["errorClass"] = EventText(row.LastErrorClass, 100)
				}
				if row.UsageLimitResetAt != "" {
					data["resetAt"] = EventText(row.UsageLimitResetAt, 100)
				}
				events = append(events, t.event(row, NewEventID(SourceT3, key), kind, severity, at, "turn "+row.Status, data))
				ended = true
			}
		}
	}
	if request := row.PendingRequest(); request == nil {
		delete(t.pending, row.ID)
	} else if request.ID != "" && t.pending[row.ID] != request.ID {
		t.pending[row.ID] = request.ID
		at, err := time.Parse(time.RFC3339Nano, request.CreatedAt)
		if err != nil {
			at = now
		}
		data := map[string]any{"request": request.ID, "requestKind": EventText(request.Kind, 100)}
		events = append(events, t.event(row, NewEventID(SourceT3, "request", request.ID), KindRequestPending, SeverityUrgent, at,
			"waiting for "+EventText(request.Kind, 60), data))
	}
	return events, ended
}

// disarm turns off T3's auto-resume once per (thread, run, reset). T3 resumes at Anthropic's
// reset time and ignores CLIProxyAPI cooldowns, so the resumed run would hit the limit again.
func (t *t3Tracker) disarm(ctx context.Context, row Thread, dispatch func(context.Context, map[string]any) (any, error)) {
	lr := row.LimitRecovery
	if lr == nil || !lr.AutoResume || row.ArchivedAt != nil || row.DeletedAt != nil {
		return
	}
	key := row.ID + "|" + lr.RunID + "|" + lr.ResetAt
	if t.disarmed[key] {
		return
	}
	t.disarmed[key] = true
	// T3 rejects the command if the limit changed meanwhile; it is logged, never retried.
	command := map[string]any{"type": "thread.metadata.update", "commandId": uuid(), "threadId": row.ID,
		"limitRecovery": map[string]any{"runId": lr.RunID, "resetAt": lr.ResetAt, "autoResume": false}}
	if _, err := dispatch(ctx, command); err != nil && ctx.Err() == nil {
		logErr(ctx, slog.LevelWarn, "COS T3 auto-resume disarm failed", err, "host", t.host, "thread", row.ID)
	}
}

// ended derives context.high and worker.needs_user from the projection of a thread
// whose turn just ended; one read per turn keeps the stream cheap.
func (t *t3Tracker) ended(row Thread, p Projection, now time.Time) []Event {
	var events []Event
	usage := ContextOf(p)
	previous := t.context[row.ID]
	t.context[row.ID] = usage.Pct
	for _, mark := range t3ContextMarks {
		if previous < mark && usage.Pct >= mark {
			data := map[string]any{"percent": usage.Pct, "used": usage.UsedTokens, "max": usage.MaxTokens, "mark": mark}
			events = append(events, t.event(row, NewEventID(SourceT3, row.LatestRunID, "context", strconv.Itoa(mark)), KindContextHigh, SeverityWarn, now,
				"context "+strconv.Itoa(usage.Pct)+"% used", data))
		}
	}
	var reply string
	for _, m := range p.Messages {
		// Only this run's reply can carry the ask; older runs were already reported.
		if m.Role == "assistant" && (m.RunID == "" || m.RunID == row.LatestRunID) {
			reply = m.Text
		}
	}
	for i, ask := range ParseAsks(reply) {
		if ask.Question == "" {
			continue
		}
		options := make([]string, 0, len(ask.Options))
		for _, option := range ask.Options[:min(len(ask.Options), 8)] {
			options = append(options, EventText(option, 200))
		}
		data := map[string]any{"question": EventText(ask.Question, 1000), "options": options, "why": EventText(ask.Why, 1000)}
		events = append(events, t.event(row, NewEventID(SourceT3, row.LatestRunID, "needs-user", strconv.Itoa(i)), KindNeedsUser, SeverityUrgent, now,
			"NEEDS-USER "+ask.Question, data))
	}
	return events
}

func init() { serveWorkers = append(serveWorkers, RunT3Events) }

// RunT3Events publishes the local T3 host's live thread events until ctx ends,
// reconnecting with backoff.
func RunT3Events(ctx context.Context, cfg Config, bus *Bus) {
	js, err := bus.JetStream()
	if err != nil {
		logErr(ctx, slog.LevelError, "COS T3 events unavailable", err)
		return
	}
	publish := func(ctx context.Context, e Event) error {
		_, err := PublishEvent(ctx, js, e)
		return err
	}
	retry(ctx, "COS event streams", func() error {
		// Bound each attempt: stream creation waits for a meta leader while the cluster forms.
		attempt, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		return EnsureEventStreams(attempt, js)
	})
	runT3Events(ctx, cfg.LocalHost, func(ctx context.Context) (*T3, error) { return OpenT3(ctx, cfg, cfg.LocalHost) }, publish)
}

func runT3Events(ctx context.Context, host string, open func(context.Context) (*T3, error), publish func(context.Context, Event) error) {
	tracker := newT3Tracker(host)
	backoff := t3BackoffMin
	for ctx.Err() == nil {
		started := time.Now()
		err := ingestT3(ctx, tracker, open, publish)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			backoff = t3BackoffMin
		}
		logErr(ctx, slog.LevelWarn, "COS T3 event stream reconnecting", err, "host", host, "in", backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, t3BackoffMax)
	}
}

func ingestT3(ctx context.Context, tracker *t3Tracker, open func(context.Context) (*T3, error), publish func(context.Context, Event) error) error {
	c, err := open(ctx)
	if err != nil {
		return err
	}
	return c.SubscribeShell(ctx, func(value ShellEvent) error {
		var rows []Thread
		snapshot := value.Kind == "snapshot"
		if snapshot {
			rows = value.Snapshot.Threads
		} else if value.Kind == "thread.updated" {
			rows = []Thread{*value.Thread}
		}
		for _, row := range rows {
			tracker.disarm(ctx, row, c.Dispatch)
			events, ended := tracker.observe(row, snapshot, time.Now())
			if ended && !snapshot {
				if p, err := c.Projection(ctx, row.ID); err == nil {
					events = append(events, tracker.ended(row, p, time.Now())...)
				} else if ctx.Err() == nil {
					logErr(ctx, slog.LevelWarn, "COS T3 projection failed; context and NEEDS-USER events skipped", err, "host", tracker.host, "thread", row.ID)
				}
			}
			for _, e := range events {
				if err := e.Validate(); err != nil {
					logErr(ctx, slog.LevelError, "COS T3 event dropped", err, "id", e.ID)
					continue
				}
				// Holding the T3 Ack while JetStream is unavailable keeps the event.
				retry(ctx, "COS T3 event publish", func() error { return publish(ctx, e) })
			}
		}
		return ctx.Err()
	})
}

// retry runs fn with backoff until it succeeds or ctx ends.
func retry(ctx context.Context, what string, fn func() error) {
	backoff := t3BackoffMin
	for {
		err := fn()
		if err == nil || ctx.Err() != nil {
			return
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			slog.Warn(what+" failed; retrying", "error", err, "in", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, t3BackoffMax)
	}
}
