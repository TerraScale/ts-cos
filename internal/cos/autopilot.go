package cos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Heartbeat presets in minutes; the dashboard offers exactly these.
var autopilotPresets = []int{10, 15, 30, 60}

const (
	autopilotDefault      = 30
	autopilotSettings     = "COS_AUTOPILOT"
	autopilotLock         = "COS_AUTOPILOT_LEADER"
	autopilotLogStream    = "COS_AUTOPILOT_LOG"
	autopilotLogSubject   = "cos.autopilot.log"
	autopilotRunSubject   = "cos.autopilot.run"
	autopilotReplicas     = 3
	autopilotLockTTL      = 45 * time.Second
	autopilotTick         = 15 * time.Second
	autopilotLogKeep      = 200
	autopilotForbiddenRun = "mariogk-desktop"
)

// AutopilotEntry is one logged heartbeat step; every action carries its reason.
type AutopilotEntry struct {
	Seq    uint64 `json:"seq"`
	At     string `json:"at"`
	Host   string `json:"host"`
	Kind   string `json:"kind"`
	Target string `json:"target,omitempty"`
	Action string `json:"action"`
	Reason string `json:"reason"`
}

// AutopilotState is the replicated heartbeat state mirrored into each host's SQLite.
type AutopilotState struct {
	Interval int              `json:"interval"`
	Leader   string           `json:"leader"`
	LastBeat string           `json:"lastBeat"`
	Error    string           `json:"error,omitempty"`
	Log      []AutopilotEntry `json:"log"`
}

func (s AutopilotState) NextBeat() string {
	last, err := time.Parse(time.RFC3339, s.LastBeat)
	if err != nil {
		return "after the leader's first check"
	}
	return last.Add(time.Duration(s.Interval) * time.Minute).UTC().Format(time.RFC3339)
}

// leaderEligible is a rule as code: COS never runs anything on the user's desktop.
func leaderEligible(host string) (bool, string) {
	if host == autopilotForbiddenRun {
		return false, "COS never runs on mariogk-desktop"
	}
	return true, ""
}

type Autopilot struct {
	Host      string
	store     *Store
	conn      *nats.Conn
	js        jetstream.JetStream
	settings  jetstream.KeyValue
	lock      jetstream.KeyValue
	reconcile func(context.Context) []AutopilotEntry

	mu       sync.Mutex
	revision uint64
	beating  bool
	mirrorMu sync.Mutex
}

// StartAutopilot runs the heartbeat machinery until ctx ends. Every host mirrors
// the replicated state; only the lock holder beats.
func StartAutopilot(ctx context.Context, host string, bus *Bus, store *Store, reconcile func(context.Context) []AutopilotEntry) (*Autopilot, error) {
	js, err := jetstream.New(bus.conn)
	if err != nil {
		return nil, err
	}
	a := &Autopilot{Host: host, store: store, conn: bus.conn, js: js, reconcile: reconcile}
	sub, err := bus.conn.Subscribe(autopilotRunSubject, a.handleRun(ctx))
	if err != nil {
		return nil, err
	}
	go func() {
		defer sub.Unsubscribe()
		a.run(ctx)
	}()
	return a, nil
}

// Reconcile refreshes live state and reports what the leader saw. The rules
// engine consumes events; this beat only catches what the event stream missed.
func Reconcile(cfg Config, bus *Bus, store *Store) func(context.Context) []AutopilotEntry {
	return func(ctx context.Context) []AutopilotEntry {
		old, err := store.Live(ctx)
		if err != nil {
			return []AutopilotEntry{{Kind: "error", Action: "Read live state", Reason: err.Error()}}
		}
		live := Refresh(ctx, cfg, bus, old)
		if err := store.SaveLive(ctx, live); err != nil {
			return []AutopilotEntry{{Kind: "error", Action: "Save live state", Reason: err.Error()}}
		}
		threads := 0
		for _, h := range live.Fleet {
			threads += len(h.Threads)
		}
		out := []AutopilotEntry{{Kind: "record", Action: "Reconciled live state",
			Reason: fmt.Sprintf("%d threads, %d PRs, %d Ku issues", threads, len(live.PRs), len(live.Issues))}}
		for _, source := range sortedKeysAny(live.Errors) {
			out = append(out, AutopilotEntry{Kind: "error", Target: source, Action: "Source refresh failed", Reason: live.Errors[source]})
		}
		return out
	}
}

func (a *Autopilot) setup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	settings, err := autopilotBucket(ctx, a.js, jetstream.KeyValueConfig{Bucket: autopilotSettings, Replicas: autopilotReplicas, Storage: jetstream.FileStorage})
	if err != nil {
		return fmt.Errorf("autopilot settings: %w", err)
	}
	lock, err := autopilotBucket(ctx, a.js, jetstream.KeyValueConfig{Bucket: autopilotLock, Replicas: autopilotReplicas, Storage: jetstream.FileStorage, TTL: autopilotLockTTL})
	if err != nil {
		return fmt.Errorf("autopilot lock: %w", err)
	}
	if _, err := a.js.Stream(ctx, autopilotLogStream); errors.Is(err, jetstream.ErrStreamNotFound) {
		_, err = a.js.CreateStream(ctx, jetstream.StreamConfig{Name: autopilotLogStream, Subjects: []string{autopilotLogSubject},
			Replicas: autopilotReplicas, Storage: jetstream.FileStorage, MaxMsgs: 1000, Discard: jetstream.DiscardOld})
		if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("autopilot log: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("autopilot log: %w", err)
	}
	a.mu.Lock()
	a.settings, a.lock = settings, lock
	a.mu.Unlock()
	return nil
}

// autopilotBucket opens a bucket another node may be creating at the same time.
func autopilotBucket(ctx context.Context, js jetstream.JetStream, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, cfg.Bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(ctx, cfg)
		if errors.Is(err, jetstream.ErrBucketExists) {
			return js.KeyValue(ctx, cfg.Bucket)
		}
	}
	return kv, err
}

func (a *Autopilot) run(ctx context.Context) {
	for {
		err := a.setup(ctx)
		if err == nil {
			break
		}
		a.mirror(ctx, func(s *AutopilotState) { s.Error = "Waiting for the COS JetStream cluster: " + err.Error() })
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	a.mirror(ctx, func(s *AutopilotState) { s.Error = "" })
	go a.follow(ctx)
	ticker := time.NewTicker(autopilotTick)
	defer ticker.Stop()
	for {
		if a.holdLock(ctx) {
			if interval, last := a.schedule(ctx); time.Since(last) >= interval {
				a.beat(ctx, "scheduled every "+interval.String())
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// holdLock acquires or renews the KV lock. Renewal needs a quorum, so a
// partitioned leader loses the lock within one TTL.
func (a *Autopilot) holdLock(ctx context.Context) bool {
	if ok, _ := leaderEligible(a.Host); !ok {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.revision != 0 {
		rev, err := a.lock.Update(ctx, "leader", []byte(a.Host), a.revision)
		if err == nil {
			a.revision = rev
			return true
		}
		a.revision = 0
	}
	rev, err := a.lock.Create(ctx, "leader", []byte(a.Host))
	if err == nil {
		a.revision = rev
		return true
	}
	// A restarted leader finds its own unexpired lock and takes it back.
	if entry, err := a.lock.Get(ctx, "leader"); err == nil && string(entry.Value()) == a.Host {
		if rev, err := a.lock.Update(ctx, "leader", []byte(a.Host), entry.Revision()); err == nil {
			a.revision = rev
			return true
		}
	}
	return false
}

func (a *Autopilot) schedule(ctx context.Context) (time.Duration, time.Time) {
	interval := autopilotDefault
	if e, err := a.settings.Get(ctx, "interval"); err == nil {
		if n, err := strconv.Atoi(string(e.Value())); err == nil && slices.Contains(autopilotPresets, n) {
			interval = n
		}
	}
	var last time.Time
	if e, err := a.settings.Get(ctx, "lastBeat"); err == nil {
		last, _ = time.Parse(time.RFC3339, string(e.Value()))
	}
	return time.Duration(interval) * time.Minute, last
}

func (a *Autopilot) beat(ctx context.Context, reason string) {
	a.mu.Lock()
	if a.beating {
		a.mu.Unlock()
		return
	}
	a.beating = true
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.beating = false; a.mu.Unlock() }()
	// Record the beat first so a failover leader does not repeat it.
	if _, err := a.settings.Put(ctx, "lastBeat", []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		slog.Error("autopilot beat", "error", err)
		return
	}
	a.publish(ctx, AutopilotEntry{Kind: "beat", Action: "Heartbeat", Reason: reason})
	for _, e := range a.reconcile(ctx) {
		a.publish(ctx, e)
	}
}

func (a *Autopilot) publish(ctx context.Context, e AutopilotEntry) {
	e.At = time.Now().UTC().Format(time.RFC3339)
	e.Host = a.Host
	data, err := json.Marshal(e)
	if err == nil {
		_, err = a.js.Publish(ctx, autopilotLogSubject, data)
	}
	if err != nil {
		slog.Error("autopilot log", "error", err, "action", e.Action)
	}
}

func (a *Autopilot) handleRun(ctx context.Context) nats.MsgHandler {
	return func(m *nats.Msg) {
		a.mu.Lock()
		leader, busy := a.revision != 0, a.beating
		a.mu.Unlock()
		if !leader {
			return // only the leader answers
		}
		reply := map[string]any{"host": a.Host, "started": !busy}
		data, _ := json.Marshal(reply)
		_ = m.Respond(data)
		if !busy {
			go a.beat(ctx, "run now from the dashboard")
		}
	}
}

// RunNow asks the current leader to beat immediately.
func (a *Autopilot) RunNow(ctx context.Context) (string, error) {
	m, err := a.conn.RequestWithContext(ctx, autopilotRunSubject, nil)
	if err != nil {
		return "", errors.New("no autopilot leader answered")
	}
	var reply struct {
		Host    string `json:"host"`
		Started bool   `json:"started"`
	}
	if err := json.Unmarshal(m.Data, &reply); err != nil {
		return "", err
	}
	if !reply.Started {
		return reply.Host, errors.New("a heartbeat is already running on " + reply.Host)
	}
	return reply.Host, nil
}

func (a *Autopilot) SetInterval(ctx context.Context, minutes int) error {
	if !slices.Contains(autopilotPresets, minutes) {
		return OperationError("interval must be one of 10, 15, 30 or 60 minutes")
	}
	a.mu.Lock()
	settings := a.settings
	a.mu.Unlock()
	if settings == nil {
		return errors.New("autopilot is waiting for the COS JetStream cluster")
	}
	_, err := settings.Put(ctx, "interval", []byte(strconv.Itoa(minutes)))
	return err
}

// follow mirrors replicated settings, the lock holder and the action log into SQLite.
func (a *Autopilot) follow(ctx context.Context) {
	go a.watch(ctx, a.settings, func(s *AutopilotState, key, value string) {
		switch key {
		case "interval":
			if n, err := strconv.Atoi(value); err == nil && slices.Contains(autopilotPresets, n) {
				s.Interval = n
			}
		case "lastBeat":
			s.LastBeat = value
		}
	})
	go a.watch(ctx, a.lock, func(s *AutopilotState, _, value string) { s.Leader = value })
	for ctx.Err() == nil {
		if err := a.followLog(ctx); err != nil && ctx.Err() == nil {
			slog.Error("autopilot log mirror", "error", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
}

func (a *Autopilot) watch(ctx context.Context, kv jetstream.KeyValue, apply func(*AutopilotState, string, string)) {
	for ctx.Err() == nil {
		w, err := kv.WatchAll(ctx)
		if err == nil {
			for e := range w.Updates() {
				if e == nil {
					continue
				}
				value := string(e.Value())
				if e.Operation() != jetstream.KeyValuePut {
					value = ""
				}
				a.mirror(ctx, func(s *AutopilotState) { apply(s, e.Key(), value) })
			}
			_ = w.Stop()
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
}

func (a *Autopilot) followLog(ctx context.Context) error {
	last, err := a.store.autopilotLogSeq(ctx)
	if err != nil {
		return err
	}
	stream, err := a.js.Stream(ctx, autopilotLogStream)
	if err != nil {
		return err
	}
	cfg := jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverAllPolicy}
	if last > 0 && stream.CachedInfo().State.LastSeq >= last {
		cfg = jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: last + 1}
	} else if last > 0 {
		// The stream was recreated: its sequence numbers restarted.
		if err := a.store.change(ctx, func(tx *sql.Tx) error { _, err := tx.ExecContext(ctx, `DELETE FROM autopilot_log`); return err }); err != nil {
			return err
		}
	}
	consumer, err := stream.OrderedConsumer(ctx, cfg)
	if err != nil {
		return err
	}
	msgs, err := consumer.Messages()
	if err != nil {
		return err
	}
	go func() { <-ctx.Done(); msgs.Stop() }()
	for {
		m, err := msgs.Next()
		if err != nil {
			return err
		}
		var e AutopilotEntry
		meta, err := m.Metadata()
		if err != nil || json.Unmarshal(m.Data(), &e) != nil {
			continue
		}
		e.Seq = meta.Sequence.Stream
		if err := a.store.addAutopilotLog(ctx, e); err != nil {
			return err
		}
	}
}

func (a *Autopilot) mirror(ctx context.Context, apply func(*AutopilotState)) {
	a.mirrorMu.Lock()
	defer a.mirrorMu.Unlock()
	if err := a.store.updateAutopilot(ctx, apply); err != nil && ctx.Err() == nil {
		slog.Error("autopilot state", "error", err)
	}
}

// Store: the autopilot row and log are a local mirror of the replicated state.

func (s *Store) Autopilot(ctx context.Context) (AutopilotState, error) {
	out := AutopilotState{Interval: autopilotDefault, Log: []AutopilotEntry{}}
	var raw string
	err := s.DB.QueryRowContext(ctx, `SELECT payload FROM autopilot WHERE id=1`).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	if err == nil {
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			return out, err
		}
	}
	out.Log = []AutopilotEntry{}
	rows, err := s.DB.QueryContext(ctx, `SELECT payload FROM autopilot_log ORDER BY seq DESC LIMIT 50`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var e AutopilotEntry
		if err := rows.Scan(&raw); err != nil {
			return out, err
		}
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return out, err
		}
		out.Log = append(out.Log, e)
	}
	return out, rows.Err()
}

func (s *Store) updateAutopilot(ctx context.Context, apply func(*AutopilotState)) error {
	old, err := s.Autopilot(ctx)
	if err != nil {
		return err
	}
	next := old
	apply(&next)
	if next.Interval == old.Interval && next.Leader == old.Leader && next.LastBeat == old.LastBeat && next.Error == old.Error {
		return nil // lock renewals rewrite the same leader every tick
	}
	next.Log = nil
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	return s.change(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO autopilot VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`, string(data))
		return err
	})
}

func (s *Store) addAutopilotLog(ctx context.Context, e AutopilotEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return s.change(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO autopilot_log VALUES(?,?)`, e.Seq, string(data)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM autopilot_log WHERE seq <= ?`, int64(e.Seq)-autopilotLogKeep)
		return err
	})
}

func (s *Store) autopilotLogSeq(ctx context.Context) (uint64, error) {
	var n sql.NullInt64
	err := s.DB.QueryRowContext(ctx, `SELECT MAX(seq) FROM autopilot_log`).Scan(&n)
	return uint64(n.Int64), err
}
