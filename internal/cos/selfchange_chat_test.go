package cos

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestPutChatShowsOnEveryNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	buses, _ := authTestCluster(t, ctx)
	// The bucket is created by the JetStream leader, so retry from each node until one succeeds.
	var posted ChatEntry
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		for _, b := range buses {
			if posted, err = PutChat(ctx, b, ChatEntry{Role: "cos", Text: "Updated: x (abc1234)"}); err == nil {
				break
			}
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("PutChat never succeeded: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	kv, err := openKV(ctx, buses[2], chatBucket)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, posted.ID)
	if err != nil {
		t.Fatalf("entry %s not readable from another node: %v", posted.ID, err)
	}
	var got ChatEntry
	if err := json.Unmarshal(entry.Value(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Text != "Updated: x (abc1234)" || got.Role != "cos" {
		t.Fatalf("read back %+v, want role cos with the posted text", got)
	}
}

func TestSelfDeployDataDecodesTS818Payload(t *testing.T) {
	raw := json.RawMessage(`{"rollout":"r1","to":"abc","hosts":["a","b"],"from":{"delta":"x"},"reason":"health"}`)
	to, reason := selfDeployData(raw)
	if to != "abc" || reason != "health" {
		t.Fatalf("selfDeployData = %q, %q; want abc, health", to, reason)
	}
}
