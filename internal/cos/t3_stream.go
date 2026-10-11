package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ThreadEvent is one live change of a T3 thread. The first event of every subscription is a
// "snapshot" with the full projection; after a reconnect, resubscribe and start from it.
type ThreadEvent struct {
	Host     string `json:"host"`
	ThreadID string `json:"threadId"`
	Sequence int64  `json:"sequence"`
	// ID is T3's event ID, stable across redelivery (empty for snapshots).
	ID string `json:"id,omitempty"`
	// Type is "snapshot" or T3's event type: run.created, run.updated, message.updated,
	// runtime-request.updated, turn-item.updated, provider-thread.updated, provider-turn.updated, ...
	Type       string          `json:"type"`
	RunID      string          `json:"runId,omitempty"`
	OccurredAt string          `json:"occurredAt,omitempty"`
	Run        *ThreadRun      `json:"run,omitempty"`
	Message    *ThreadMessage  `json:"message,omitempty"`
	Request    *RuntimeRequest `json:"request,omitempty"`
	Item       *TurnItem       `json:"item,omitempty"`
	Context    *ContextUsage   `json:"context,omitempty"`
	Snapshot   *Projection     `json:"snapshot,omitempty"`
}

// ShellEvent is one change of the host's thread list: a "snapshot" first, then "thread.updated" and others.
type ShellEvent struct {
	Host     string         `json:"host"`
	Sequence int64          `json:"sequence"`
	Kind     string         `json:"kind"`
	Location string         `json:"location,omitempty"`
	Thread   *Thread        `json:"thread,omitempty"`
	Snapshot *ShellSnapshot `json:"snapshot,omitempty"`
}

// SubscribeThread streams a thread's live events until ctx ends or fn fails.
func (c *T3) SubscribeThread(ctx context.Context, id string, fn func(ThreadEvent) error) error {
	const where = "orchestration.subscribeThread"
	return c.Subscribe(ctx, where, map[string]any{"threadId": id}, func(raw json.RawMessage) error {
		e, err := c.decodeThreadEvent(where, id, raw)
		if err != nil {
			return err
		}
		return fn(e)
	})
}

func (c *T3) decodeThreadEvent(where, id string, raw json.RawMessage) (ThreadEvent, error) {
	var v struct {
		Kind             string          `json:"kind"`
		Sequence         int64           `json:"sequence"`
		SnapshotSequence int64           `json:"snapshotSequence"`
		Projection       json.RawMessage `json:"projection"`
		Event            *struct {
			ID         string          `json:"id"`
			ThreadID   string          `json:"threadId"`
			RunID      string          `json:"runId"`
			Type       string          `json:"type"`
			OccurredAt string          `json:"occurredAt"`
			Payload    json.RawMessage `json:"payload"`
		} `json:"event"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return ThreadEvent{}, c.schema(where, "stream value: %v", err)
	}
	e := ThreadEvent{Host: c.Host, ThreadID: id, Type: v.Kind}
	switch v.Kind {
	case "snapshot":
		p, err := c.decodeProjection(where, v.Projection)
		if err != nil {
			return e, err
		}
		usage := ContextOf(p)
		e.Sequence, e.Snapshot, e.Context = v.SnapshotSequence, &p, &usage
		return e, nil
	case "event":
	default:
		return e, c.schema(where, "unknown stream value kind %q", v.Kind)
	}
	if v.Event == nil || v.Event.Type == "" || v.Event.ThreadID == "" || v.Sequence == 0 {
		return e, c.schema(where, "event without type, threadId or sequence")
	}
	x := v.Event
	e.Sequence, e.ID, e.ThreadID, e.Type, e.RunID, e.OccurredAt = v.Sequence, x.ID, x.ThreadID, x.Type, x.RunID, x.OccurredAt
	decode := func(out any) error {
		if err := json.Unmarshal(x.Payload, out); err != nil {
			return c.schema(where, "%s payload: %v", x.Type, err)
		}
		return nil
	}
	var err error
	switch x.Type {
	case "run.created", "run.updated":
		e.Run = &ThreadRun{}
		if err = decode(e.Run); err == nil && (e.Run.ID == "" || e.Run.Status == "") {
			err = c.schema(where, "%s payload without id or status", x.Type)
		}
	case "message.updated":
		e.Message = &ThreadMessage{}
		if err = decode(e.Message); err == nil && e.Message.Role == "" {
			err = c.schema(where, "message.updated payload without role")
		}
	case "runtime-request.updated":
		e.Request = &RuntimeRequest{}
		if err = decode(e.Request); err == nil && (e.Request.ID == "" || e.Request.Status == "") {
			err = c.schema(where, "runtime-request.updated payload without id or status")
		}
	case "turn-item.updated":
		e.Item = &TurnItem{}
		err = decode(e.Item)
	case "provider-thread.updated", "provider-turn.updated":
		var u struct {
			ContextUsage  *ContextUsage `json:"contextUsage"`
			TokenUsage    *ContextUsage `json:"tokenUsage"`
			NativeTurnRef struct {
				Driver string `json:"driver"`
			} `json:"nativeTurnRef"`
			Driver string `json:"driver"`
		}
		if err = decode(&u); err == nil {
			e.Context = u.ContextUsage
			if u.TokenUsage != nil {
				e.Context = u.TokenUsage
			}
			if e.Context != nil {
				for _, driver := range []string{u.Driver, u.NativeTurnRef.Driver} {
					if driver != "" {
						e.Context.Provider = driver
					}
				}
				if e.Context.MaxTokens > 0 {
					e.Context.Pct = (e.Context.UsedTokens*100 + e.Context.MaxTokens/2) / e.Context.MaxTokens
				}
			}
		}
	}
	return e, err
}

// SubscribeShell streams the host's thread list until ctx ends or fn fails.
func (c *T3) SubscribeShell(ctx context.Context, fn func(ShellEvent) error) error {
	const where = "orchestration.subscribeShell"
	return c.Subscribe(ctx, where, map[string]any{}, func(raw json.RawMessage) error {
		var v struct {
			Kind     string         `json:"kind"`
			Sequence int64          `json:"sequence"`
			Location string         `json:"location"`
			Thread   *Thread        `json:"thread"`
			Snapshot *ShellSnapshot `json:"snapshot"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return c.schema(where, "stream value: %v", err)
		}
		e := ShellEvent{Host: c.Host, Sequence: v.Sequence, Kind: v.Kind, Location: v.Location, Thread: v.Thread, Snapshot: v.Snapshot}
		switch {
		case v.Kind == "":
			return c.schema(where, "stream value without kind")
		case v.Kind == "snapshot" && (v.Snapshot == nil || v.Snapshot.SchemaVersion != t3ShellSchema):
			return c.schema(where, "snapshot schemaVersion is not %d", t3ShellSchema)
		case v.Kind == "snapshot":
			e.Sequence = v.Snapshot.Sequence
		case v.Kind == "thread.updated" && (v.Thread == nil || v.Thread.ID == "" || v.Thread.Status == ""):
			return c.schema(where, "thread.updated without thread id or status")
		}
		return fn(e)
	})
}

var errThreadDone = errors.New("thread wait finished")

// waitRuns follows a thread's runs live until done reports true for them.
func (c *T3) waitRuns(ctx context.Context, id string, limit time.Duration, done func([]ThreadRun) bool) (Projection, error) {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	var runs []ThreadRun
	err := c.SubscribeThread(ctx, id, func(e ThreadEvent) error {
		switch {
		case e.Snapshot != nil:
			runs = slices.Clone(e.Snapshot.Runs)
		case e.Run != nil:
			i := slices.IndexFunc(runs, func(r ThreadRun) bool { return r.ID == e.Run.ID })
			if i < 0 {
				runs = append(runs, *e.Run)
			} else {
				runs[i] = *e.Run
			}
		default:
			return nil
		}
		if done(runs) {
			return errThreadDone
		}
		return nil
	})
	if errors.Is(err, errThreadDone) {
		return c.Projection(ctx, id)
	}
	if ctx.Err() != nil && errors.Is(err, context.DeadlineExceeded) {
		return Projection{}, OperationError(fmt.Sprintf("thread still busy after %s", limit))
	}
	if err == nil {
		err = OperationError(fmt.Sprintf("T3 %s ended the thread subscription", c.Host))
	}
	return Projection{}, err
}

// WaitIdle returns once no run of the thread is active.
func (c *T3) WaitIdle(ctx context.Context, id string, limit time.Duration) (Projection, error) {
	return c.waitRuns(ctx, id, limit, func(runs []ThreadRun) bool {
		return !slices.ContainsFunc(runs, func(r ThreadRun) bool { return active(r.Status) })
	})
}

// WaitMessage returns once the run started by messageID (a queued or immediate Send) has finished.
func (c *T3) WaitMessage(ctx context.Context, id, messageID string, limit time.Duration) (Projection, error) {
	return c.waitRuns(ctx, id, limit, func(runs []ThreadRun) bool {
		return slices.ContainsFunc(runs, func(r ThreadRun) bool {
			// Dispatched messages keep our ID; launch messages get a "message:thread:...:<id>" form.
			mine := r.UserMessageID == messageID || strings.HasSuffix(r.UserMessageID, ":"+messageID)
			return mine && r.Status != "" && !active(r.Status)
		})
	})
}
