package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const e2eHandover = 30 * time.Second

// leaderOf reads the cluster lease through the runner's node.
func leaderOf(ctx context.Context, b *Bus) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return b.Leader(ctx)
}

// leaderViews asks each host's deployed binary (`cos cluster`, through its own node) whom it sees as leader.
func (e *E2EEnv) leaderViews(ctx context.Context, hosts []string) (map[string]string, error) {
	views := map[string]string{}
	for _, h := range hosts {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		out, err := e.run(call, h, "~/.local/bin/cos cluster")
		if call.Err() != nil {
			err = errors.New("cos cluster timed out after 5s")
		}
		cancel()
		var v struct{ Leader string }
		if err != nil || json.Unmarshal([]byte(out), &v) != nil || v.Leader == "" {
			return views, fmt.Errorf("%s has no leader view: %v %s", h, err, e2eClip(out, 120))
		}
		views[h] = v.Leader
	}
	return views, nil
}

// agreedLeader polls until every host in hosts names the same leader, a COS host other than not.
func (e *E2EEnv) agreedLeader(ctx context.Context, hosts []string, not string, timeout time.Duration) (string, map[string]string, error) {
	start := time.Now()
	for {
		views, err := e.leaderViews(ctx, hosts)
		if err == nil {
			first := views[hosts[0]]
			agree := slices.Contains(e2eHosts, first) && first != not
			for _, l := range views {
				agree = agree && l == first
			}
			if agree {
				return first, views, nil
			}
			err = fmt.Errorf("nodes disagree on the leader: %v", views)
		}
		if time.Since(start) > timeout || ctx.Err() != nil {
			return "", views, err
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

func checkLeader(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	lease, err := leaderOf(ctx, bus)
	if err != nil {
		return "", err
	}
	leader, views, err := e.agreedLeader(ctx, e2eHosts, "", 5*time.Second)
	if err != nil {
		return "", err
	}
	if leader != lease {
		return "", fmt.Errorf("lease names %s but nodes agree on %s", lease, leader)
	}
	return fmt.Sprintf("exactly one leader %s; node views %v", leader, views), nil
}

func without(hosts []string, h string) []string {
	return slices.DeleteFunc(slices.Clone(hosts), func(x string) bool { return x == h })
}

// leaseRevision is the cluster lease entry's revision; a fresh Create after a crash changes it.
func leaseRevision(ctx context.Context, b *Bus) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	js, err := jetstream.New(b.conn)
	if err != nil {
		return 0, err
	}
	kv, err := js.KeyValue(ctx, leaseBucket)
	if err != nil {
		return 0, err
	}
	entry, err := kv.Get(ctx, leaseKey)
	if err != nil {
		return 0, err
	}
	return entry.Revision(), nil
}

func checkLeaderKill(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer func() {
		if bus != nil {
			bus.Close()
		}
	}()
	// shortcut: a fixed settle of one lease period after the earlier chaos checks; without it a kill
	// right after the partition heal took over 30 s to hand over. Replace with a JetStream meta-health
	// probe if TS-810 exposes one.
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(20 * time.Second):
	}
	if _, _, err := waitCluster(ctx, bus.conn, e2eHosts, "", 10*time.Second); err != nil {
		return "", fmt.Errorf("cluster unhealthy before killing the leader, not perturbing it: %w", err)
	}
	leader, err := leaderOf(ctx, bus)
	if err != nil {
		return "", err
	}
	revision, err := leaseRevision(ctx, bus)
	if err != nil {
		return "", err
	}
	// A crash, not a clean stop: no Resign, so the lease must expire. systemd restarts the node.
	if _, err := e.run(ctx, leader, "systemctl --user kill -s SIGKILL "+e2eUnit); err != nil {
		return "", err
	}
	start := time.Now()
	if leader == e.cfg.LocalHost {
		bus.Close()
		bus = nil
	}
	// Handover is done when the survivors agree on a live leader: another host, or the restarted
	// node holding a fresh lease (new revision) after the old one expired.
	survivors := without(e2eHosts, leader)
	var next, how string
	for next == "" {
		if time.Since(start) > e2eHandover || ctx.Err() != nil {
			views, _ := e.leaderViews(ctx, survivors)
			return "", fmt.Errorf("killed leader %s: no fresh leader within %s (survivor views %v)", leader, e2eHandover, views)
		}
		l, _, err := e.agreedLeader(ctx, survivors, "", 0)
		switch {
		case err != nil:
		case l != leader:
			next, how = l, "took over"
		default:
			if bus == nil {
				bus, _ = ConnectBus(ctx, e.cfg)
			}
			if bus != nil {
				if r, err := leaseRevision(ctx, bus); err == nil && r != revision {
					next, how = l, "was re-elected after its restart"
				}
			}
		}
		if next == "" {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	took := time.Since(start)
	if bus == nil {
		if bus, err = e.reconnect(ctx, 30*time.Second); err != nil {
			return "", fmt.Errorf("killed leader %s, %s %s in %s, but the node did not come back: %w", leader, next, how, took.Round(100*time.Millisecond), err)
		}
	}
	summary, rejoin, err := waitCluster(ctx, bus.conn, e2eHosts, "", 60*time.Second)
	if err != nil {
		return "", fmt.Errorf("killed leader %s, %s %s in %s, but the cluster did not recover: %w", leader, next, how, took.Round(100*time.Millisecond), err)
	}
	if _, views, err := e.agreedLeader(ctx, e2eHosts, "", 10*time.Second); err != nil {
		return "", fmt.Errorf("after the leader kill: %w (views %v)", err, views)
	}
	return fmt.Sprintf("killed leader %s; %s %s in %s; cluster recovered %s later (%s)",
		leader, next, how, took.Round(100*time.Millisecond), rejoin.Round(100*time.Millisecond), summary), nil
}

// reconnect waits for the runner's restarted node to accept connections again.
func (e *E2EEnv) reconnect(ctx context.Context, timeout time.Duration) (*Bus, error) {
	deadline := time.Now().Add(timeout)
	for {
		// ConnectBus bounds its own dial; the bus lives as long as ctx, so pass the run context.
		bus, err := ConnectBus(ctx, e.cfg)
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return bus, err
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

// checkHeartbeatLeader requires leader-only work to run on the cluster leader. There is one
// election (cos_leader): only the lease holder's autopilot answers run now, which starts one
// zero-token reconcile beat.
func checkHeartbeatLeader(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	leader, err := leaderOf(ctx, bus)
	if err != nil {
		return "", err
	}
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	holder, err := autopilotRunNow(call, bus.conn)
	if err != nil && !errors.Is(err, errAutopilotBusy) {
		return "", fmt.Errorf("autopilot did not answer from the cluster leader %s: %w", leader, err)
	}
	if holder != leader {
		return "", fmt.Errorf("autopilot heartbeat runs on %s but the cluster leader is %s", holder, leader)
	}
	return "autopilot heartbeat runs on the cluster leader " + leader, nil
}
