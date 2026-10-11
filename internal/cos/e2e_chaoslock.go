package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Only one e2e run may perturb the cluster at a time. A holder older than the TTL crashed without
// releasing the lock, so the next run takes it over.
const (
	e2eChaosLockKey = "lock"
	e2eChaosLockTTL = 10 * time.Minute
	// A chaos check waits this long for the lock or a rollout to clear before it skips (TS-816).
	e2eChaosWait = 5 * time.Minute
	e2eChaosPoll = 5 * time.Second
)

// chaosHeld is the error for a lock a fresh holder keeps. take waits for it rather than skipping at once.
type chaosHeld string

func (h chaosHeld) Error() string { return string(h) }

// chaosLock is the value stored under e2eChaosLockKey in the cos_e2e bucket.
type chaosLock struct {
	Host string    `json:"host"`
	PID  int       `json:"pid"`
	At   time.Time `json:"at"`
}

// chaosLockBlocks reports whether an existing lock record stops a run starting at now. No record is free,
// a record younger than the TTL is held, and an older one is stale.
func chaosLockBlocks(cur *chaosLock, now time.Time) bool {
	return cur != nil && now.Sub(cur.At) < e2eChaosLockTTL
}

// chaosLockRun takes the lock once, before the run's first chaos check, and releases it when the run ends.
type chaosLockRun struct {
	env          *E2EEnv
	budget, poll time.Duration // how long and how often take waits for a fresh holder to let go
	taken        bool
	release      func()
	err          error
}

// take returns nil when this run holds the lock. A fresh holder is waited for, up to budget, and only then
// does every chaos check skip with the holder's evidence.
func (l *chaosLockRun) take(ctx context.Context) error {
	if !l.taken {
		l.taken = true
		var held chaosHeld
		waited := waitFor(ctx, l.budget, l.poll, func() bool {
			l.release, l.err = l.env.takeChaosLock(ctx)
			return !errors.As(l.err, &held)
		})
		l.env.waited += waited
		if errors.As(l.err, &held) {
			l.err = e2eSkip(fmt.Sprintf("%s, after waiting %s", held, waited.Round(time.Second)))
		}
	}
	return l.err
}

// waitFor calls try now and then every poll until it reports done, the budget is spent or ctx ends. It returns
// the time waited, which is about 0 when try is done at once.
func waitFor(ctx context.Context, budget, poll time.Duration, try func() bool) time.Duration {
	start := time.Now()
	for !try() {
		left := budget - time.Since(start)
		if left <= 0 || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(min(poll, left)):
		}
	}
	return time.Since(start)
}

func (l *chaosLockRun) close() {
	if l.release != nil {
		l.release()
	}
}

// takeChaosLock makes this run the lock holder. A run that cannot get the lock returns an e2eSkip, so it never perturbs.
func (e *E2EEnv) takeChaosLock(ctx context.Context) (func(), error) {
	if e.chaosLock != nil {
		return e.chaosLock(ctx)
	}
	bus, err := ConnectBus(ctx, e.cfg)
	if err != nil {
		return nil, chaosLockUnavailable(err)
	}
	kv, err := chaosKV(ctx, bus)
	if err != nil {
		bus.Close()
		return nil, chaosLockUnavailable(err)
	}
	releaseKV, err := takeChaosLockKV(ctx, kv, chaosLock{Host: e.cfg.LocalHost, PID: os.Getpid(), At: time.Now().UTC()})
	if err != nil {
		bus.Close()
		return nil, err
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := releaseKV(ctx); err != nil {
			slog.Warn("release chaos lock", "error", err)
		}
		bus.Close()
	}, nil
}

// takeChaosLockKV creates the lock key, or takes over a stale one. A fresh holder gives a chaosHeld naming it.
func takeChaosLockKV(ctx context.Context, kv jetstream.KeyValue, self chaosLock) (func(context.Context) error, error) {
	val, err := json.Marshal(self)
	if err != nil {
		return nil, chaosLockUnavailable(err)
	}
	entry, err := kv.Get(ctx, e2eChaosLockKey)
	var cur *chaosLock
	if err == nil {
		cur = &chaosLock{}
		_ = json.Unmarshal(entry.Value(), cur) // a corrupt record keeps a zero At, so it reads as stale
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, chaosLockUnavailable(err)
	}
	if chaosLockBlocks(cur, self.At) {
		return nil, chaosHeld(fmt.Sprintf("another e2e run on %s (pid %d) holds the chaos lock since %s; not perturbing", cur.Host, cur.PID, cur.At.Format(time.RFC3339)))
	}
	var rev uint64
	if cur == nil {
		rev, err = kv.Create(ctx, e2eChaosLockKey, val)
	} else {
		rev, err = kv.Update(ctx, e2eChaosLockKey, val, entry.Revision())
	}
	if err != nil {
		return nil, chaosLockUnavailable(err)
	}
	return func(ctx context.Context) error {
		return kv.Delete(ctx, e2eChaosLockKey, jetstream.LastRevision(rev))
	}, nil
}

func chaosLockUnavailable(err error) error {
	return e2eSkip("chaos lock unavailable: " + err.Error() + "; not perturbing")
}
