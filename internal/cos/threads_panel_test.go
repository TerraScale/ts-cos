package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// threadsTestLive is one host with a tab COS ("brain"), its worker and their subagents, plus strangers.
const threadsTestLive = `{"fleet":[
 {"host":"delta","threads":[
  {"threadId":"brain","title":"COS","status":"running","model":"claude-opus-5-5","updated":"2026-10-11T09:00:00Z","prs":[]},
  {"threadId":"brain-sub","title":"subagent of COS","status":"idle","updated":"2026-10-11T08:00:00Z","lineage":{"parent":"brain","rel":"subagent"},"prs":[]},
  {"threadId":"worker","title":"worker","status":"running","model":"claude-sonnet-5-5","updated":"2026-10-11T10:00:00Z","prs":[{"url":"https://git.example/terrascale/ts-cos/pulls/12","state":"open"}]},
  {"threadId":"sub","title":"subagent of worker","status":"idle","updated":"2026-10-11T11:00:00Z","lineage":{"parent":"worker","rel":"subagent"},"prs":[]},
  {"threadId":"elsewhere","title":"other tab","status":"running","updated":"2026-10-11T12:00:00Z","prs":[]},
  {"threadId":"elsewhere-sub","title":"subagent of other tab","status":"idle","updated":"2026-10-11T12:30:00Z","lineage":{"parent":"elsewhere","rel":"subagent"},"prs":[]},
  {"threadId":"loop-a","title":"cycle a","status":"idle","updated":"2026-10-11T07:00:00Z","lineage":{"parent":"loop-b","rel":"subagent"},"prs":[]},
  {"threadId":"loop-b","title":"cycle b","status":"idle","updated":"2026-10-11T07:00:00Z","lineage":{"parent":"loop-a","rel":"subagent"},"prs":[]}
 ]},
 {"host":"beta","threads":[
  {"threadId":"sub-beta","title":"subagent of worker id on beta","status":"idle","updated":"2026-10-11T13:00:00Z","lineage":{"parent":"worker","rel":"subagent"},"prs":[]}
 ]}
],"prs":[{"repo":"ts-cos","number":12,"url":"https://git.example/terrascale/ts-cos/pulls/12","ci":"failure","mergeable":false,"ready":false}]}`

func TestTabThreads(t *testing.T) {
	var live Live
	if err := json.Unmarshal([]byte(threadsTestLive), &live); err != nil {
		t.Fatal(err)
	}
	tags := []ChatTag{
		{ThreadID: "brain", Host: "delta", Tab: "t1", COS: true},
		{ThreadID: "worker", Host: "delta", Tab: "t1"},
		{ThreadID: "gone", Host: "delta", Tab: "t1"},
	}
	got := tabThreads(live, tags)
	var ids []string
	for _, p := range got {
		ids = append(ids, p.Host+"/"+p.ThreadID)
	}
	// Newest first. Hidden: the COS thread, other tabs, untagged threads, the cycle, and the same-named thread on beta (another host).
	if want := []string{"delta/sub", "delta/worker", "delta/brain-sub"}; !slices.Equal(ids, want) {
		t.Fatal(ids)
	}
	worker := got[1]
	if worker.PR == nil || worker.PR.URL != "https://git.example/terrascale/ts-cos/pulls/12" || worker.PR.Repo != "terrascale/ts-cos" || worker.PR.Number != 12 {
		t.Fatalf("PR = %+v", worker.PR)
	}
	if worker.PR.CI != "failure" || worker.PR.Mergeable || worker.PR.label() != "terrascale/ts-cos#12" {
		t.Fatalf("PR join = %+v", worker.PR)
	}
	if got[0].PR != nil || got[0].Model != "" {
		t.Fatalf("subagent row = %+v", got[0])
	}
}

func TestParsePRURL(t *testing.T) {
	if repo, n := parsePRURL("https://git.example/terrascale/ts-cos/pulls/12"); repo != "terrascale/ts-cos" || n != 12 {
		t.Fatal(repo, n)
	}
	if repo, n := parsePRURL("https://git.example/terrascale/ts-cos/issues/12"); repo != "" || n != 0 {
		t.Fatal(repo, n)
	}
}

func TestThreadPanelEscapesTitle(t *testing.T) {
	var b bytes.Buffer
	rows := []panelThread{{Host: "delta", ThreadID: "x", Title: "<script>alert(1)</script>", Status: "idle", Updated: "2026-10-11T10:00:00Z"}}
	if err := threadPanel(rows, nil).Render(context.Background(), &b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	if strings.Contains(out, "<script>") || !strings.Contains(out, "&lt;script&gt;") {
		t.Fatal(out)
	}
	b.Reset()
	if err := threadPanel(nil, nil).Render(context.Background(), &b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "No threads yet") {
		t.Fatal(b.String())
	}
}

func TestChatTagsApply(t *testing.T) {
	c := NewChat()
	tag := ChatTag{ThreadID: "thread:1%2", Host: "delta", Tab: "t1", At: time.Now().UTC()}
	raw, err := json.Marshal(tag)
	if err != nil {
		t.Fatal(err)
	}
	if k := tagKey(tag.ThreadID); len(k) != len(chatTagPrefix)+16 || strings.ContainsAny(k, ":%") {
		t.Fatal(k)
	}
	c.apply(fakeChatEntry{key: tagKey(tag.ThreadID), value: raw, op: jetstream.KeyValuePut})
	c.apply(fakeChatEntry{key: tagKey("bad"), value: []byte("{not json"), op: jetstream.KeyValuePut})
	c.apply(nil)
	if len(c.entries) != 0 {
		t.Fatal("tag became a chat entry", c.entries)
	}
	if got := c.Tags("t1"); len(got) != 1 || got[0].ThreadID != tag.ThreadID {
		t.Fatal(got)
	}
	if got := c.Tags("t2"); len(got) != 0 {
		t.Fatal(got)
	}
	c.apply(fakeChatEntry{key: tagKey(tag.ThreadID), op: jetstream.KeyValueDelete})
	if got := c.Tags("t1"); len(got) != 0 {
		t.Fatal("deleted tag still present", got)
	}
}

func TestChatDeleteTabRemovesTags(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nodes, _ := chatTestCluster(t, ctx)
	// Every node follows the chat, so whichever node leads JetStream can create the bucket.
	c := NewChat()
	go c.Run(ctx, nodes[0])
	for _, n := range nodes[1:] {
		go NewChat().Run(ctx, n)
	}
	chatTestWait(t, "chat ready", c.Ready)
	if err := c.PutTag(ctx, ChatTag{ThreadID: "w1", Host: "delta", Tab: "t1", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutTag(ctx, ChatTag{ThreadID: "cos", Host: "delta", Tab: chatMainTab, COS: true, At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	chatTestAwait(t, c, "tags replicated", func() bool { return len(c.Tags("t1")) == 1 && len(c.Tags(chatMainTab)) == 1 })
	if err := c.DeleteTab(ctx, "t1"); err != nil {
		t.Fatal(err)
	}
	chatTestAwait(t, c, "tab tags removed", func() bool { return len(c.Tags("t1")) == 0 })
	if got := c.Tags(chatMainTab); len(got) != 1 || !got[0].COS {
		t.Fatal("other tab's tags changed", got)
	}
}

func TestTagLaunch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nodes, _ := chatTestCluster(t, ctx)
	kv := agentTestOpen(t, ctx, nodes, chatBucket)
	launched := map[string]any{"host": "delta", "threadId": "w9", "resumed": false}
	if err := tagLaunch(ctx, nodes[0], "delta", "t1", launched); err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, tagKey("w9"))
	if err != nil {
		t.Fatal(err)
	}
	var got ChatTag
	if err := json.Unmarshal(entry.Value(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ThreadID != "w9" || got.Host != "delta" || got.Tab != "t1" || got.COS {
		t.Fatal(got)
	}
	if err := tagLaunch(ctx, nodes[0], "delta", "t1", map[string]any{}); err == nil {
		t.Fatal("launch without a thread ID was tagged")
	}
}
