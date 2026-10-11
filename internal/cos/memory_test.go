package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// memoryTestBuckets starts a three-node cluster and binds each bucket on its first node.
func memoryTestBuckets(t *testing.T, ctx context.Context, buckets ...string) []jetstream.KeyValue {
	t.Helper()
	nodes, _ := chatTestCluster(t, ctx)
	kvs := make([]jetstream.KeyValue, len(buckets))
	for i, bucket := range buckets {
		// Only the JetStream meta leader creates buckets, so try every node until one succeeds.
		chatTestWait(t, bucket+" bucket created", func() bool {
			for _, bus := range nodes {
				if _, err := openKV(ctx, bus, bucket); err == nil {
					return true
				}
			}
			return false
		})
		chatTestWait(t, bucket+" bucket bound", func() bool {
			var err error
			kvs[i], err = openKV(ctx, nodes[0], bucket)
			return err == nil
		})
	}
	return kvs
}

var memorySeed = []struct{ kind, text, tag string }{
	{"decision", "deploys use canary rollout with health gates", "deploy"},
	{"preference", "user prefers terse replies", "style"},
	{"fact", "NetBird admits nodes automatically", "network"},
	{"learning", "Haiku workers handle mechanical edits", "delegation"},
	{"fact", "dashboard uses Pico.css and Datastar", "dashboard"},
	{"learning", "leader freezes the cluster during elections", "cluster"},
	{"fact", "nightly backups keep fourteen days", "backup"},
	{"decision", "rules engine reads cos.events subjects", "rules"},
}

// memoryTestSeed stores memorySeed with ascending times, so the last entry is the newest.
func memoryTestSeed(t *testing.T, ctx context.Context, mem jetstream.KeyValue) {
	t.Helper()
	base := time.Now().UTC().Add(-time.Hour)
	for i, s := range memorySeed {
		e := MemoryEntry{ID: fmt.Sprintf("t-%02d", i), Kind: s.kind, Text: s.text, Tags: []string{s.tag}, At: base.Add(time.Duration(i) * time.Minute)}
		if err := memoryPut(ctx, mem, e); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMemoryRecallRanksByRelevance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kvs := memoryTestBuckets(t, ctx, memoryBucket, contextBucket)
	memoryTestSeed(t, ctx, kvs[0])

	r, err := Recall(ctx, kvs[0], kvs[1], "how do canary deploys roll back", 3, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Entries) == 0 || r.Entries[0].Text != memorySeed[0].text {
		t.Fatalf("canary entry not first: %+v", r.Entries)
	}

	// No shared words: the most recent entries win.
	r, err = Recall(ctx, kvs[0], kvs[1], "zebra quantum", 3, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Entries) != 3 || r.Entries[0].Text != memorySeed[7].text || r.Entries[2].Text != memorySeed[5].text {
		t.Fatalf("no-match fallback: %+v", r.Entries)
	}
}

func TestMemoryRecallPinnedAlwaysIncluded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kvs := memoryTestBuckets(t, ctx, memoryBucket, contextBucket)
	mem, ctxKV := kvs[0], kvs[1]
	memoryTestSeed(t, ctx, mem)
	if err := memoryPut(ctx, mem, MemoryEntry{ID: "t-pin", Kind: "preference", Text: "always answer in English", Pinned: true, At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	focus := "Ship TS-828 before Friday.\n\n- Keep savings replicated\n- Never log tokens"
	if err := contextCommand(ctx, ctxKV, []string{"set", "focus", focus}, io.Discard); err != nil {
		t.Fatal(err)
	}

	r, err := Recall(ctx, mem, ctxKV, "canary deploys", 2, 1500)
	if err != nil {
		t.Fatal(err)
	}
	// Three focus paragraphs and one stored pin come first, unrelated to the query.
	pinned := 0
	for _, e := range r.Entries {
		if !e.Pinned {
			break
		}
		pinned++
	}
	if pinned != 4 {
		t.Fatalf("pinned entries first: got %d in %+v", pinned, r.Entries)
	}
	if r.Entries[len(r.Entries)-1].Text != memorySeed[0].text {
		t.Fatalf("canary match missing after pins: %+v", r.Entries)
	}
	if r.Entries[1].ID != "ctx.focus.0" || r.Entries[2].ID != "ctx.focus.1" {
		t.Fatalf("focus paragraph order: %+v", r.Entries)
	}
	if got := r.Text(); !strings.Contains(got, "- [fact] Ship TS-828 before Friday. (#focus)\n") {
		t.Fatalf("brief text %q", got)
	}
}

func TestMemoryRecallHardCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kvs := memoryTestBuckets(t, ctx, memoryBucket, contextBucket)
	memoryTestSeed(t, ctx, kvs[0])

	r, err := Recall(ctx, kvs[0], kvs[1], "rules nodes dashboard", 5, 40)
	if err != nil {
		t.Fatal(err)
	}
	if r.Tokens > 40 || r.Omitted == 0 || r.FullTokens <= r.Tokens {
		t.Fatalf("cap not enforced: tokens=%d omitted=%d full=%d", r.Tokens, r.Omitted, r.FullTokens)
	}
	sum := 0
	for _, e := range r.Entries {
		sum += memoryTokens(e)
	}
	if sum != r.Tokens {
		t.Fatalf("token count %d, entries sum to %d", r.Tokens, sum)
	}
}

func TestMemoryQuerySanitizer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kvs := memoryTestBuckets(t, ctx, memoryBucket, contextBucket)
	memoryTestSeed(t, ctx, kvs[0])

	if _, err := Recall(ctx, kvs[0], kvs[1], `"foo" OR bar* NEAR(`, 3, 1500); err != nil {
		t.Fatal("FTS syntax reached MATCH:", err)
	}
	if got, want := memoryMatch(`"foo" OR bar* NEAR(`), `"foo"* OR "bar"* OR "near"*`; got != want {
		t.Fatalf("memoryMatch = %s, want %s", got, want)
	}
}

func TestMemoryCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kvs := memoryTestBuckets(t, ctx, memoryBucket, contextBucket)
	mem, ctxKV := kvs[0], kvs[1]
	run := func(args ...string) (string, error) {
		t.Helper()
		var buf bytes.Buffer
		err := memoryCommand(ctx, mem, ctxKV, args, &buf)
		return buf.String(), err
	}

	out, err := run("add", "decision", "Use canary deploys with health gates", "deploy,ops")
	if err != nil {
		t.Fatal(err)
	}
	var added MemoryEntry
	if err := json.Unmarshal([]byte(out), &added); err != nil {
		t.Fatal(err, out)
	}
	if added.ID == "" || added.Kind != "decision" || len(added.Tags) != 2 {
		t.Fatal("add output:", out)
	}

	if _, err := run("pin", added.ID); err != nil {
		t.Fatal(err)
	}
	out, err = run("recall", "canary deploys")
	if err != nil {
		t.Fatal(err)
	}
	var recall MemoryRecall
	if err := json.Unmarshal([]byte(out), &recall); err != nil {
		t.Fatal(err, out)
	}
	if len(recall.Entries) == 0 || recall.Entries[0].ID != added.ID || !recall.Entries[0].Pinned {
		t.Fatal("recall output:", out)
	}

	if _, err := run("unpin", added.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := run("rm", added.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := run("rm", added.ID); err == nil {
		t.Fatal("removed twice")
	}
	if _, err := run("pin", added.ID); err == nil {
		t.Fatal("pinned a removed entry")
	}

	rejected := map[string][]string{
		"bad kind":   {"add", "gossip", "text"},
		"empty text": {"add", "fact", "   "},
		"bad tag":    {"add", "fact", "text", "Bad Tag"},
		"tag count":  {"add", "fact", "text", "a,b,c,d,e,f,g,h,i"},
		"no args":    {},
		"bad arity":  {"pin"},
	}
	for name, args := range rejected {
		if _, err := run(args...); err == nil {
			t.Errorf("%s accepted: %v", name, args)
		}
	}
}
