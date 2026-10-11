package cos

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// KPIs are the dashboard tiles, folded from COS_EVENTS and COS_USAGE on every node. Today's
// counters restart at UTC midnight; the last e2e run is kept until the next one.
type KPIs struct {
	Merges    int
	E2E       E2ERun
	E2ESeen   bool
	Usage     UsageTotal
	UsageSeen bool
	Probe     string // ID of the last kpi.probe; only the liveness check reads it
}

type E2ERun struct {
	Pass, Fail, Skip int
	At               time.Time
}

// fold applies one event. Unknown kinds and payloads without the expected fields add nothing.
func (k *KPIs) fold(e Event) {
	switch e.Kind {
	case KindPRMerged:
		k.Merges++
	case KindE2EReport:
		var d struct{ Pass, Fail, Skip int }
		if json.Unmarshal(e.Data, &d) != nil {
			return
		}
		k.E2E, k.E2ESeen = E2ERun{Pass: d.Pass, Fail: d.Fail, Skip: d.Skip, At: e.Time}, true
	case KindKPIProbe:
		k.Probe = e.ID
	}
}

// KPIBoard mirrors this node's KPI tiles. Subscribers wake after every change, so sends must not block.
type KPIBoard struct {
	mu    sync.Mutex
	k     KPIs
	usage map[string]UsageRow
	subs  map[chan struct{}]struct{}
}

// Subscribe wakes the returned channel after each change. One pending wake is kept, not one per change.
func (b *KPIBoard) Subscribe() (<-chan struct{}, func()) {
	c := make(chan struct{}, 1)
	b.mu.Lock()
	if b.subs == nil {
		b.subs = map[chan struct{}]struct{}{}
	}
	b.subs[c] = struct{}{}
	b.mu.Unlock()
	return c, func() { b.mu.Lock(); delete(b.subs, c); b.mu.Unlock() }
}

// Snapshot returns the tiles, with usage totalled over the rows dated today (UTC).
func (b *KPIBoard) Snapshot() KPIs {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := b.k
	today := time.Now().UTC().Format("2006-01-02")
	var rows []UsageRow
	for _, r := range b.usage {
		if r.Day == today {
			rows = append(rows, r)
		}
	}
	k.Usage, k.UsageSeen = usageTotal("today", rows), len(rows) > 0
	return k
}

// Run folds today's merges from UTC midnight, the usage rows in COS_USAGE, and the last e2e
// report, until ctx ends. Each day replays from midnight, so a restart or the rollover at
// midnight rebuilds the merge counter.
func (b *KPIBoard) Run(ctx context.Context, bus *Bus) {
	js, err := bus.JetStream()
	if err != nil {
		slog.Error("KPI events unavailable", "error", err)
		return
	}
	e2eDone := make(chan struct{})
	go func() {
		defer close(e2eDone)
		b.follow(ctx, js, jetstream.OrderedConsumerConfig{FilterSubjects: []string{"cos.events.cos.e2e.report"}, DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy}, nil)
	}()
	usageDone := make(chan struct{})
	go func() {
		defer close(usageDone)
		b.watchUsage(ctx, js)
	}()
	for ctx.Err() == nil {
		now := time.Now().UTC()
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		day, stop := context.WithDeadline(ctx, time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC))
		b.follow(day, js, jetstream.OrderedConsumerConfig{FilterSubjects: []string{"cos.events.forgejo.pr.merged", "cos.events.cos.kpi.probe"}, DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &start}, b.resetToday)
		stop()
	}
	<-e2eDone
	<-usageDone
}

// follow applies the consumer's events until ctx ends. begin runs before each replay, so a
// restarted consumer that replays from the start does not count twice.
func (b *KPIBoard) follow(ctx context.Context, js jetstream.JetStream, cfg jetstream.OrderedConsumerConfig, begin func()) {
	for ctx.Err() == nil {
		if begin != nil {
			begin()
		}
		cons, err := js.OrderedConsumer(ctx, EventsStream, cfg)
		var cc jetstream.ConsumeContext
		if err == nil {
			cc, err = cons.Consume(b.apply)
		}
		if err == nil {
			select {
			case <-ctx.Done():
				cc.Stop()
				return
			case <-cc.Closed():
				err = errors.New("KPI consumer stopped")
			}
		}
		slog.Warn("KPI events not ready", "error", err)
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

// watchUsage mirrors COS_USAGE until ctx ends. RunUsage creates the bucket, so this retries until it exists.
func (b *KPIBoard) watchUsage(ctx context.Context, js jetstream.JetStream) {
	for ctx.Err() == nil {
		err := b.bindUsage(ctx, js)
		slog.Warn("KPI usage not ready", "error", err)
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

func (b *KPIBoard) bindUsage(ctx context.Context, js jetstream.JetStream) error {
	kv, err := js.KeyValue(ctx, usageBucket)
	if err != nil {
		return err
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		return err
	}
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-w.Updates():
			if !ok {
				return errors.New("usage watch closed")
			}
			if entry == nil {
				continue // the initial values are done; later entries are live
			}
			if entry.Operation() != jetstream.KeyValuePut {
				b.putUsage(entry.Key(), nil)
				continue
			}
			var row UsageRow
			if err := json.Unmarshal(entry.Value(), &row); err != nil {
				slog.Error("skip corrupt usage row", "key", entry.Key(), "error", err)
				continue
			}
			b.putUsage(entry.Key(), &row)
		}
	}
}

// putUsage stores one COS_USAGE row, or drops its key when row is nil.
func (b *KPIBoard) putUsage(key string, row *UsageRow) {
	b.mu.Lock()
	if row == nil {
		delete(b.usage, key)
	} else {
		if b.usage == nil {
			b.usage = map[string]UsageRow{}
		}
		b.usage[key] = *row
	}
	b.mu.Unlock()
	b.changed()
}

func (b *KPIBoard) apply(m jetstream.Msg) {
	var e Event
	if err := json.Unmarshal(m.Data(), &e); err != nil {
		slog.Error("invalid KPI event", "error", err)
		return
	}
	b.mu.Lock()
	b.k.fold(e)
	b.mu.Unlock()
	b.changed()
}

// resetToday clears the day's counters and keeps the last e2e run.
func (b *KPIBoard) resetToday() {
	b.mu.Lock()
	b.k = KPIs{E2E: b.k.E2E, E2ESeen: b.k.E2ESeen}
	b.mu.Unlock()
	b.changed()
}

func (b *KPIBoard) changed() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for c := range b.subs {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}
