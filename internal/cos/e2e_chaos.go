package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// e2eChaosBucket holds one record per perturbation the e2e run makes, so the restarts it causes are attributable.
const e2eChaosBucket = "cos_e2e"

type e2eChaos struct {
	Node   string    `json:"node"`
	Check  string    `json:"check"`
	Action string    `json:"action"`
	At     time.Time `json:"at"`
}

// chaosKV opens the chaos bucket, creating it on first use.
func chaosKV(ctx context.Context, b *Bus) (jetstream.KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	js, err := jetstream.New(b.conn)
	if err != nil {
		return nil, err
	}
	return js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: e2eChaosBucket, Replicas: 3, History: 1, TTL: 2 * time.Hour, Storage: jetstream.FileStorage})
}

// chaos records a perturbation on host before the check makes it, in the cluster store and the
// host's journal. A check that cannot record must not perturb the cluster, so it returns the error first.
func (e *E2EEnv) chaos(ctx context.Context, b *Bus, host, check, action string) error {
	kv, err := chaosKV(ctx, b)
	if err != nil {
		return fmt.Errorf("record %s on %s: %w", check, host, err)
	}
	now := time.Now().UTC()
	rec, err := json.Marshal(e2eChaos{Node: host, Check: check, Action: action, At: now})
	if err != nil {
		return err
	}
	if _, err := kv.Put(ctx, host+"."+strconv.FormatInt(now.UnixNano(), 10), rec); err != nil {
		return fmt.Errorf("record %s on %s: %w", check, host, err)
	}
	if _, err := e.run(ctx, host, fmt.Sprintf("logger -t cos-e2e 'check=%s action=%s'", check, action)); err != nil {
		return fmt.Errorf("journal %s on %s: %w", check, host, err)
	}
	return nil
}

// attributedKills counts host's sigkill records since the given time; each one explains one automatic restart.
func attributedKills(ctx context.Context, b *Bus, host string, since time.Time) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	js, err := jetstream.New(b.conn)
	if err != nil {
		return 0, err
	}
	kv, err := js.KeyValue(ctx, e2eChaosBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	keys, err := kv.ListKeysFiltered(ctx, host+".*")
	if err != nil {
		return 0, err
	}
	n := 0
	for key := range keys.Keys() {
		entry, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue // expired between listing and reading
		}
		if err != nil {
			return n, err
		}
		var rec e2eChaos
		if json.Unmarshal(entry.Value(), &rec) == nil && rec.Action == "sigkill" && rec.At.After(since) {
			n++
		}
	}
	return n, nil
}
