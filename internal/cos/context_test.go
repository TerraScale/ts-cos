package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestContextCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nodes, configs := chatTestCluster(t, ctx)
	// Only the JetStream meta leader creates buckets, so try every node until one succeeds.
	owner := -1
	chatTestWait(t, "context bucket created", func() bool {
		for i, bus := range nodes {
			if _, err := openKV(ctx, bus, contextBucket); err == nil {
				owner = i
				return true
			}
		}
		return false
	})
	bindKV := func(bus *Bus) jetstream.KeyValue {
		t.Helper()
		var kv jetstream.KeyValue
		chatTestWait(t, "context bucket bound", func() bool {
			var err error
			kv, err = openKV(ctx, bus, contextBucket)
			return err == nil
		})
		return kv
	}
	kv := bindKV(nodes[owner])
	replica := bindKV(nodes[(owner+1)%len(nodes)])
	run := func(kv jetstream.KeyValue, args ...string) (string, error) {
		t.Helper()
		var buf bytes.Buffer
		err := contextCommand(ctx, kv, args, &buf)
		return buf.String(), err
	}
	decode := func(out string, v any) {
		t.Helper()
		if err := json.Unmarshal([]byte(out), v); err != nil {
			t.Fatal(err, out)
		}
	}

	out, err := run(kv, "set", "user-prefs", "Prefer Haiku 5.5")
	if err != nil {
		t.Fatal(err)
	}
	var set map[string]any
	decode(out, &set)
	if set["set"] != "user-prefs" || set["bytes"] != float64(len("Prefer Haiku 5.5")) {
		t.Fatal(out)
	}

	chatTestWait(t, "context file replicated", func() bool {
		out, err = run(replica, "get", "user-prefs")
		return err == nil
	})
	var got ContextFile
	decode(out, &got)
	if got.Name != "user-prefs" || got.Text != "Prefer Haiku 5.5" || got.Bytes != len("Prefer Haiku 5.5") {
		t.Fatal(out)
	}

	out, err = run(kv, "list")
	if err != nil {
		t.Fatal(err)
	}
	var files []ContextFile
	decode(out, &files)
	if len(files) != 1 || files[0].Name != "user-prefs" || files[0].Bytes != len("Prefer Haiku 5.5") {
		t.Fatal(out)
	}
	if strings.Contains(out, "Prefer Haiku") {
		t.Fatal("list leaked file text", out)
	}

	if _, err := run(kv, "rm", "user-prefs"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(kv, "get", "user-prefs"); err == nil || err.Error() != "no context file named user-prefs" {
		t.Fatal("get after rm:", err)
	}

	if _, err := run(kv, "set", "Bad Name", "text"); err == nil {
		t.Fatal("accepted invalid name")
	}
	if _, err := run(kv, "set", "user-prefs", "   "); err == nil {
		t.Fatal("accepted empty text")
	}
	if _, err := run(kv, "set", "user-prefs", strings.Repeat("a", contextMaxText+1)); err == nil {
		t.Fatal("accepted text over 16 KiB")
	}
	if _, err := run(kv); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatal("no args:", err)
	}

	client, err := ConnectBus(ctx, configs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	if _, err := openKV(ctx, client, "COS_MISSING"); err == nil || !strings.Contains(err.Error(), "is cos serve running") {
		t.Fatal("client-only missing bucket:", err)
	}
}
