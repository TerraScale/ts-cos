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
)

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
	env     *E2EEnv
	taken   bool
	release func()
	err     error
}

// take returns nil when this run holds the lock. Otherwise every chaos check skips with the returned error.
func (l *chaosLockRun) take(ctx context.Context) error {
	if !l.taken {
		l.taken = true
		l.release, l.err = l.env.takeChaosLock(ctx)
	}
	return l.err
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

// takeChaosLockKV creates the lock key, or takes over a stale one. A fresh holder gives an e2eSkip naming it.
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
		return nil, e2eSkip(fmt.Sprintf("another e2e run on %s (pid %d) holds the chaos lock since %s; not perturbing", cur.Host, cur.PID, cur.At.Format(time.RFC3339)))
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
