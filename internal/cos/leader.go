package cos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A leader refreshes its lease every leaseTTL/3 and steps down when a refresh
// fails; a dead leader's lease expires after leaseTTL, so handover takes at most ~20 s.
const (
	leaseBucket = "cos_leader"
	leaseKey    = "leader"
	leaseTTL    = 15 * time.Second
)

func leaseKV(ctx context.Context, js jetstream.JetStream, create bool) (jetstream.KeyValue, error) {
	if !create {
		return js.KeyValue(ctx, leaseBucket)
	}
	// Three replicas keep the lease available while any one host is offline.
	return js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: leaseBucket, TTL: leaseTTL, History: 1, Replicas: 3})
}

// elect campaigns for the cluster lease until ctx ends or Resign, and publishes leadership to Lead.
func (b *Bus) elect(ctx context.Context) {
	defer close(b.elected)
	js, _ := jetstream.New(b.conn)
	var kv jetstream.KeyValue
	var revision uint64
	var stop context.CancelFunc
	step := func() {
		call, cancel := context.WithTimeout(ctx, leaseTTL/4)
		defer cancel()
		var err error
		if kv == nil {
			if kv, err = leaseKV(call, js, true); err != nil {
				kv = nil
				return
			}
		}
		if stop != nil {
			if revision, err = kv.Update(call, leaseKey, []byte(b.host), revision); err != nil {
				stop()
				stop = nil
				b.setLeading(nil)
			}
			return
		}
		if revision, err = kv.Create(call, leaseKey, []byte(b.host)); err == nil {
			var leading context.Context
			leading, stop = context.WithCancel(ctx)
			b.setLeading(leading)
		}
	}
	ticker := time.NewTicker(leaseTTL / 3)
	defer ticker.Stop()
	for {
		step()
		select {
		case <-ctx.Done():
		case <-b.resign:
		case <-ticker.C:
			continue
		}
		if stop != nil {
			stop()
			b.setLeading(nil)
			// Hand over now instead of after the lease expires.
			release, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = kv.Delete(release, leaseKey, jetstream.LastRevision(revision))
			cancel()
		}
		return
	}
}

// Resign releases the lease now and stops campaigning for the rest of this
// process, so a rollout can hand leadership to an updated node first.
func (b *Bus) Resign() {
	if b.resign == nil {
		return
	}
	b.resignOnce.Do(func() { close(b.resign) })
	<-b.elected
}

func (b *Bus) setLeading(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.leading = ctx
	if b.leadChanged != nil {
		close(b.leadChanged)
	}
	b.leadChanged = make(chan struct{})
}

// Lead runs singleton work (heartbeat, agent driver) only while this node holds
// the cluster lease. work's context ends when the lease is lost or ctx ends;
// Lead runs it again the next time this node becomes leader.
func (b *Bus) Lead(ctx context.Context, work func(context.Context)) {
	for {
		b.mu.Lock()
		if b.leadChanged == nil {
			b.leadChanged = make(chan struct{})
		}
		leading, changed := b.leading, b.leadChanged
		b.mu.Unlock()
		if leading != nil && leading.Err() == nil && ctx.Err() == nil {
			run, cancel := context.WithCancel(leading)
			stop := context.AfterFunc(ctx, cancel)
			work(run)
			stop()
			cancel()
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		}
	}
}

// Leader returns the host holding the cluster lease. A direct get is answered by
// any stream replica, so it works while the JetStream meta group has no leader.
func (b *Bus) Leader(ctx context.Context) (string, error) {
	// Like the JetStream API default: a direct get routed to a vanished replica gets no reply.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	msg, err := b.conn.RequestMsgWithContext(ctx, nats.NewMsg("$JS.API.DIRECT.GET.KV_"+leaseBucket+".$KV."+leaseBucket+"."+leaseKey))
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return "", errors.New("COS cluster has not elected a leader yet")
		}
		return "", errors.New("COS cluster leader unavailable: " + err.Error())
	}
	if status := msg.Header.Get("Status"); status == "404" {
		return "", errors.New("COS cluster has no leader right now")
	} else if status != "" {
		return "", fmt.Errorf("COS cluster leader read failed: %s %s", status, msg.Header.Get("Description"))
	}
	// Deletes and TTL expiries keep a tombstone with an empty body; a live lease always holds a host name.
	if len(msg.Data) == 0 {
		return "", errors.New("COS cluster has no leader right now")
	}
	return string(msg.Data), nil
}
