package cos

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// brainSync is how often the leader checks which users have a brain.
	brainSync = 10 * time.Second
	// brainMigrateEvery is how often the leader moves unprefixed keys of the single COS to the first admin.
	brainMigrateEvery = 10 * time.Minute
	// brainMovedRestart asks the admin's brain for a fresh thread once its agent state moved to the new workspace.
	brainMovedRestart = "COS moved to a per-user workspace"
)

// brainBuckets are the buckets every brain shares; each brain reads and writes its own keys through UserKV.
type brainBuckets struct {
	js                                          jetstream.JetStream
	auth, chat, agent, context, memory, savings jetstream.KeyValue
}

func openBrainBuckets(ctx context.Context, bus *Bus) (brainBuckets, error) {
	var b brainBuckets
	js, err := bus.JetStream()
	if err != nil {
		return b, err
	}
	b.js = js
	if b.auth, err = js.KeyValue(ctx, authBucket); err != nil {
		return b, err
	}
	if b.chat, err = openKV(ctx, bus, chatBucket); err != nil {
		return b, err
	}
	if b.agent, err = openAgentKV(ctx, bus); err != nil {
		return b, err
	}
	if b.context, err = openKV(ctx, bus, contextBucket); err != nil {
		return b, err
	}
	if b.memory, err = openKV(ctx, bus, memoryBucket); err != nil {
		return b, err
	}
	b.savings, err = openKV(ctx, bus, savingsBucket)
	return b, err
}

// brainUsers lists the users with a brain. Node T3s belong to the first admin, so only the first admin
// has one until users get their own T3 hosts.
func brainUsers(ctx context.Context, nc *nats.Conn, users jetstream.KeyValue) ([]User, error) {
	id, ok, err := firstAdmin(ctx, nc)
	if err != nil || !ok {
		return nil, err
	}
	entry, err := users.Get(ctx, "uid."+id)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec authUser
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return nil, err
	}
	if !rec.DisabledAt.IsZero() {
		return nil, nil
	}
	return []User{{ID: id, Name: rec.Name, Role: rec.Role}}, nil
}

// runBrains runs one COS brain per user while this node leads: a step loop starts for each listed user and
// stops when the user is no longer listed.
func runBrains(ctx context.Context, cfg Config, bus *Bus, chat *Chat, store *Store) {
	var buckets brainBuckets
	for {
		b, err := openBrainBuckets(ctx, bus)
		if err == nil {
			buckets = b
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	bus.Lead(ctx, func(ctx context.Context) {
		brains := map[string]context.CancelFunc{}
		defer func() {
			for _, cancel := range brains {
				cancel()
			}
		}()
		var nextMigrate time.Time
		for {
			if now := time.Now(); !now.Before(nextMigrate) {
				buckets.migrateLegacy(ctx, bus.conn)
				nextMigrate = now.Add(brainMigrateEvery)
			}
			users, err := brainUsers(ctx, bus.conn, buckets.auth)
			if err != nil {
				logErr(ctx, slog.LevelWarn, "COS brains", err)
			} else {
				for _, u := range users {
					if _, ok := brains[u.ID]; ok {
						continue
					}
					brain, err := newBrain(cfg, bus, chat, store, buckets, u)
					if err != nil {
						logErr(ctx, slog.LevelError, "COS brain", err, "user", u.Name)
						continue
					}
					brainCtx, cancel := context.WithCancel(ctx)
					brains[u.ID] = cancel
					go brain.loop(brainCtx)
				}
				for id, cancel := range brains {
					if !slices.ContainsFunc(users, func(u User) bool { return u.ID == id }) {
						cancel()
						delete(brains, id)
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(brainSync):
			}
		}
	})
}

// newBrain is the agent for one user: its state, focus and memory are the user's own keys.
func newBrain(cfg Config, bus *Bus, chat *Chat, store *Store, b brainBuckets, u User) (*Agent, error) {
	ctx := WithUser(context.Background(), u)
	kv, err := UserKV(ctx, b.agent)
	if err != nil {
		return nil, err
	}
	ctxKV, err := UserKV(ctx, b.context)
	if err != nil {
		return nil, err
	}
	memKV, err := UserKV(ctx, b.memory)
	if err != nil {
		return nil, err
	}
	a := NewAgent(cfg, chat, store, bus)
	a.User, a.js, a.KV, a.Context, a.Memory, a.Savings = u, b.js, kv, ctxKV, memKV, b.savings
	return a, nil
}

// loop steps the brain every second until ctx ends; Step waits until the chat mirror is ready.
func (a *Agent) loop(ctx context.Context) {
	for {
		if a.Chat.Ready() {
			if err := a.Step(ctx); err != nil && ctx.Err() == nil {
				logErr(ctx, slog.LevelWarn, "COS agent step", err, "user", a.User.Name)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// migrateLegacy moves the unprefixed keys of the single COS to the first admin's brain. Only the first
// admin's brain can start from them, so nothing else needs them.
func (b brainBuckets) migrateLegacy(ctx context.Context, nc *nats.Conn) {
	id, ok, err := firstAdmin(ctx, nc)
	if err != nil || !ok {
		if err != nil {
			logErr(ctx, slog.LevelWarn, "COS brain migration", err)
		}
		return
	}
	for _, m := range []struct {
		bucket string
		kv     jetstream.KeyValue
	}{{chatBucket, b.chat}, {agentBucket, b.agent}, {contextBucket, b.context}, {memoryBucket, b.memory}} {
		if m.bucket == agentBucket {
			// The admin's agent state moves to a new workspace, so its brain starts a fresh thread there. The marker
			// is written first: if it fails, the state stays put and the next run retries.
			if _, err := b.agent.Get(ctx, "state"); err == nil {
				if _, err := b.agent.Put(ctx, User{ID: id}.Key("restart"), []byte(brainMovedRestart)); err != nil {
					logErr(ctx, slog.LevelWarn, "COS brain migration", err, "bucket", m.bucket)
					return
				}
			} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
				logErr(ctx, slog.LevelWarn, "COS brain migration", err, "bucket", m.bucket)
				return
			}
		}
		moved, err := migrateToUser(ctx, m.kv, id)
		if moved > 0 {
			slog.Info("COS brain migration", "bucket", m.bucket, "keys", moved)
		}
		if err != nil {
			logErr(ctx, slog.LevelWarn, "COS brain migration", err, "bucket", m.bucket)
			return
		}
	}
}

// brainWorkspace is the per-user COS workspace: its CLAUDE.md and ./cos wrapper belong to one user.
func brainWorkspace(home string, u User) string {
	return filepath.Join(home, agentWorkspace, u.ID)
}
