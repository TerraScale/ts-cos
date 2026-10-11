package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// chatTestWebs starts a chat web on every node of a three-node test cluster and serves each one over HTTP.
func chatTestWebs(t *testing.T, ctx context.Context) ([]*Bus, []*Web, []string) {
	t.Helper()
	buses, _ := chatTestCluster(t, ctx)
	webs := make([]*Web, len(buses))
	for i, bus := range buses {
		store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		webs[i] = NewWeb(store, nil)
		webs[i].Chat.recheck = 100 * time.Millisecond
		// Start every node first: only the JetStream leader creates the chat bucket.
		go webs[i].Chat.Run(ctx, bus)
	}
	bases := make([]string, len(buses))
	for i, web := range webs {
		chatTestAwait(t, web.Chat, "chat ready", web.Chat.Ready)
		server := httptest.NewServer(web.routes())
		t.Cleanup(server.Close)
		bases[i] = server.URL
	}
	return buses, webs, bases
}

// TestE2EChatProbe runs the chat.non-leader-message probe on a three-node test cluster: each node serves its own chat stream.
func TestE2EChatProbe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	buses, _, bases := chatTestWebs(t, ctx)
	evidence, err := chatProbeAcross(ctx, buses[2], bases, make([]string, len(bases)))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(evidence)
}

// TestE2EChatReplyAll finds the newest COS reply and requires it in each node's first repaint; a node that never repaints it fails.
func TestE2EChatReplyAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	buses, webs, bases := chatTestWebs(t, ctx)
	cookies := make([]string, len(bases))

	if _, err := newestCOSReply(ctx, buses[0]); !errors.Is(err, errNoCOSReply) {
		t.Fatalf("empty chat: got %v, want no COS reply", err)
	}
	entry, err := PutChat(ctx, buses[2], ChatEntry{ID: chatID(time.Now().UTC()), Role: "cos", Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	for _, web := range webs {
		chatTestAwait(t, web.Chat, "reply on every node", func() bool {
			return slices.ContainsFunc(web.Chat.Entries(), func(e ChatEntry) bool { return e.ID == entry.ID })
		})
	}
	id, err := newestCOSReply(ctx, buses[1])
	if err != nil || id != entry.ID {
		t.Fatalf("newest COS reply = %q, %v; want %q", id, err, entry.ID)
	}
	evidence, err := replyRepaint(ctx, bases, cookies, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(evidence)

	// A stream that repaints history without the reply fails on that node.
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: <div id=\"messages\"></div>\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer silent.Close()
	_, err = replyRepaint(ctx, append(slices.Clone(bases), silent.URL), append(slices.Clone(cookies), ""), id)
	if err == nil {
		t.Fatal("silent node: want a missing-reply failure")
	}
	t.Log(err)
}

// TestE2EChatLiveReads round-trips the state and focus the live checks read, through a bus on another node.
func TestE2EChatLiveReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	buses, _ := chatTestCluster(t, ctx)
	started := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	kv := agentTestOpen(t, ctx, buses, agentBucket)
	data, err := json.Marshal(AgentState{Host: "delta", ThreadID: "thread-1", Started: started})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, "state", data); err != nil {
		t.Fatal(err)
	}
	st, err := liveAgentState(ctx, buses[1])
	if err != nil || st.ThreadID != "thread-1" || st.Host != "delta" || !st.Started.Equal(started) {
		t.Fatalf("liveAgentState = %+v, %v", st, err)
	}
	focusAt, err := focusUpdated(ctx, buses[1])
	if err != nil || !focusAt.IsZero() {
		t.Fatalf("focus before any write = %v, %v; want zero time", focusAt, err)
	}
	ctxKV := agentTestOpen(t, ctx, buses, contextBucket)
	if _, err := ctxKV.Put(ctx, "focus", []byte("ship TS-811")); err != nil {
		t.Fatal(err)
	}
	if focusAt, err = focusUpdated(ctx, buses[1]); err != nil || focusAt.IsZero() {
		t.Fatalf("focus after write = %v, %v; want a time", focusAt, err)
	}
}

// TestE2EChatStateChecks covers the decisions of the live state checks, each with a pass and a fail or skip case.
func TestE2EChatStateChecks(t *testing.T) {
	started := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	var skip e2eSkip
	cases := []struct {
		name string
		run  func() (string, error)
		skip bool
		fail bool
	}{
		{"spawn: thread on leader", func() (string, error) {
			return spawnThread(AgentState{Host: "delta", ThreadID: "t"}, "delta")
		}, false, false},
		{"spawn: thread on another host", func() (string, error) {
			return spawnThread(AgentState{Host: "beta", ThreadID: "t"}, "delta")
		}, false, true},
		{"spawn: no thread yet", func() (string, error) {
			return spawnThread(AgentState{Host: "delta"}, "delta")
		}, true, false},
		{"focus: no state", func() (string, error) {
			return focusRestart(AgentState{}, started.Add(time.Minute))
		}, true, false},
		{"focus: changed before the thread started", func() (string, error) {
			return focusRestart(AgentState{Started: started}, started.Add(-time.Minute))
		}, false, false},
		{"focus: changed after start, restart names it", func() (string, error) {
			return focusRestart(AgentState{Started: started, Restart: "focus changed"}, started.Add(time.Minute))
		}, false, false},
		{"focus: changed after start, focus wake already ran", func() (string, error) {
			st := AgentState{Started: started, Wakes: []WakeRecord{{At: started.Add(2 * time.Minute), Reason: "focus"}}}
			return focusRestart(st, started.Add(time.Minute))
		}, false, false},
		{"focus: changed after start, nothing pending", func() (string, error) {
			return focusRestart(AgentState{Started: started, Restart: "context reached 150000 tokens"}, started.Add(time.Minute))
		}, false, true},
		{"budget: no wakes", func() (string, error) {
			return wakeBudget(AgentState{}, 6)
		}, true, false},
		{"budget: at the limit", func() (string, error) {
			return wakeBudget(AgentState{Wakes: digests(started, 6, 0)}, 6)
		}, false, false},
		{"budget: one over the limit inside an hour", func() (string, error) {
			return wakeBudget(AgentState{Wakes: digests(started, 7, 10*time.Minute)}, 6)
		}, false, true},
		{"budget: seven digests spread over two hours", func() (string, error) {
			return wakeBudget(AgentState{Wakes: digests(started, 7, 20*time.Minute)}, 6)
		}, false, false},
		{"context: no carried context", func() (string, error) {
			return contextAgrees(AgentState{BriefTokens: 9000})
		}, false, false},
		{"context: carried over the rule", func() (string, error) {
			return contextAgrees(AgentState{BriefTokens: 9000, CarriedTokens: 130000})
		}, false, false},
		{"context: carried under the fresh minimum", func() (string, error) {
			return contextAgrees(AgentState{BriefTokens: 9000, CarriedTokens: 30000})
		}, false, true},
		{"context: compaction with no thread", func() (string, error) {
			return contextAgrees(AgentState{Compacted: "run-1"})
		}, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			evidence, err := c.run()
			switch {
			case c.skip:
				if !errors.As(err, &skip) {
					t.Fatalf("got %q, %v; want skip", evidence, err)
				}
			case c.fail:
				if err == nil {
					t.Fatalf("got %q; want failure", evidence)
				}
			case err != nil:
				t.Fatal(err)
			}
		})
	}
}

// digests returns n digest wakes starting at start, spaced gap apart.
func digests(start time.Time, n int, gap time.Duration) []WakeRecord {
	wakes := make([]WakeRecord, n)
	for i := range wakes {
		wakes[i] = WakeRecord{At: start.Add(time.Duration(i) * gap), Reason: "digest"}
	}
	return wakes
}
