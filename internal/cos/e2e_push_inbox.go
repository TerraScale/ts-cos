package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// checkPushInboxNotify only reads: it files no Inbox item and sends no push, it waits for the notifier to record the newest real one.
func checkPushInboxNotify(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return "", err
	}
	stream, err := js.Stream(ctx, EventsStream)
	if err != nil {
		return "", err
	}
	subject := Event{Source: SourceInbox, Kind: KindInboxItemAdded}.Subject()
	newest, err := stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return "", E2ESkip("no Inbox item has been filed yet")
	}
	if err != nil {
		return "", err
	}
	var item Event
	if err := json.Unmarshal(newest.Data, &item); err != nil {
		return "", err
	}
	// js.KeyValue, not openKV: the check must never create the bucket.
	kv, err := js.KeyValue(ctx, pushBucket)
	if err != nil {
		return "", err
	}
	var last pushLast
	var readErr error
	err = pollE2E(ctx, 90*time.Second, 3*time.Second, func(ctx context.Context) (bool, error) {
		cur, err := readPushLast(ctx, kv)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, E2ESkip("no Inbox item since the notifier started recording; next real item proves it")
		}
		readErr = err
		if err != nil {
			return false, nil
		}
		last = cur
		// A real item may arrive during the wait; the notifier must catch up to whichever is newest.
		if m, err := stream.GetLastMsgForSubject(ctx, subject); err == nil && json.Unmarshal(m.Data, &item) == nil {
			newest = m
		}
		return last.Event == item.ID, nil
	})
	// Only the poll's own timeout falls through; the suite's deadline or a cancel is reported as is.
	if err != nil && (ctx.Err() != nil || !errors.Is(err, context.DeadlineExceeded)) {
		return "", err
	}
	if readErr != nil {
		return "", fmt.Errorf("read the notifier record: %w", readErr)
	}
	if last.Event != item.ID {
		return "", fmt.Errorf("notifier has not handled the newest Inbox item %s (last handled %s at %s)", item.ID, last.Event, last.At.UTC().Format(time.RFC3339))
	}
	cons, err := js.Consumer(ctx, EventsStream, pushConsumer)
	if err != nil {
		return "", err
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return "", err
	}
	return pushInboxEvidence(last, time.Since(newest.Time), info.NumPending), nil
}

// readPushLast reads the notifier's record; a missing key comes back as jetstream.ErrKeyNotFound.
func readPushLast(ctx context.Context, kv jetstream.KeyValue) (pushLast, error) {
	var last pushLast
	entry, err := kv.Get(ctx, pushLastKey)
	if err != nil {
		return last, err
	}
	return last, json.Unmarshal(entry.Value(), &last)
}

// pushInboxEvidence words the proof. Info items are silent by design, so they are not described as pushed.
func pushInboxEvidence(last pushLast, age time.Duration, pending uint64) string {
	what := fmt.Sprintf("pushed to %d devices", last.Sent)
	if last.Severity == string(SeverityInfo) {
		what = "info item, not pushed by design"
	}
	return fmt.Sprintf("Inbox item %s (%s, filed %s ago) %s at %s; notifier pending %d", last.Item, last.Severity, age.Round(time.Second), what, last.At.UTC().Format(time.RFC3339), pending)
}
