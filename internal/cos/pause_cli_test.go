package cos

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestPausedSetRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// JetStream needs a 3-node meta leader, so this runs the same mesh as the autopilot test.
	hosts := []string{"delta", "beta", "mariogk-dev"}
	ports := busTestPorts(t, 6)
	nodes := make([]*Bus, 3)
	var cfg Config
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		var node busNode
		var err error
		cfg, node = busTestConfig(t, host, ports[i], ports[i+3], peers)
		nodes[i], err = startBus(ctx, cfg, node, func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nodes[i].Close)
	}
	busTestReady(t, "full mesh", func() bool {
		return nodes[0].server.NumRoutes() == 2 && nodes[1].server.NumRoutes() == 2 && nodes[2].server.NumRoutes() == 2
	})
	js, err := nodes[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	// The first JetStream call can hang while the meta leader settles, so retry short attempts as Autopilot.setup does.
	var kv jetstream.KeyValue
	for range 10 {
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		kv, err = autopilotBucket(call, js, jetstream.KeyValueConfig{Bucket: autopilotSettings, Replicas: autopilotReplicas, Storage: jetstream.FileStorage})
		cancel()
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	if set, err := pausedSet(ctx, kv); err != nil || len(set) != 0 {
		t.Fatalf("empty bucket: %v %v, want no pauses", set, err)
	}
	if got, err := setPaused(ctx, kv, "TS-2", true); err != nil || !slices.Equal(got, []string{"TS-2"}) {
		t.Fatalf("pause TS-2 = %v %v", got, err)
	}
	if got, err := setPaused(ctx, kv, "ts-3", true); err != nil || !slices.Equal(got, []string{"TS-2", "TS-3"}) {
		t.Fatalf("pause ts-3 = %v %v, want TS-2 and TS-3", got, err)
	}
	if got, err := setPaused(ctx, kv, "TS-2", false); err != nil || !slices.Equal(got, []string{"TS-3"}) {
		t.Fatalf("resume TS-2 = %v %v, want [TS-3]", got, err)
	}
	set, err := pausedSet(ctx, kv)
	if err != nil || !set["TS-3"] || set["TS-2"] || len(set) != 1 {
		t.Fatalf("paused set = %v %v, want only TS-3", set, err)
	}
	if _, err := setPaused(ctx, kv, "foo", true); err == nil {
		t.Fatal("invalid key foo accepted")
	}

	var out bytes.Buffer
	if err := cliCommands["pause"].Run(ctx, cfg, []string{"TS-2"}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`"TS-2"`)) || !bytes.Contains(out.Bytes(), []byte(`"TS-3"`)) {
		t.Fatalf("pause output = %s, want TS-2 and TS-3 in the paused set", out.Bytes())
	}
}
