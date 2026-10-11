package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// agentTestT3 records what the agent asks T3 to do and serves a canned thread projection.
type agentTestT3 struct {
	projects   []Project
	projection string
	http       []agentTestHTTP
	launches   []map[string]any
	sends      []agentTestSend
	closes     []agentTestClose
	// Failure injection for the recovery paths.
	rpcErr, launchErr error
	skipClose         bool
}

type agentTestHTTP struct {
	Method, Path string
	Input        any
}

type agentTestSend struct{ Thread, Text string }

type agentTestClose struct {
	Thread string
	Delete bool
}

type agentTestRemote struct {
	Host, Command string
	Args          []string
}

func (f *agentTestT3) Projects(context.Context) ([]Project, error) { return f.projects, nil }

func (f *agentTestT3) HTTP(_ context.Context, method, path string, input, _ any) error {
	f.http = append(f.http, agentTestHTTP{method, path, input})
	return nil
}

func (f *agentTestT3) Launch(_ context.Context, input map[string]any) (any, error) {
	if f.launchErr != nil {
		err := f.launchErr
		f.launchErr = nil
		return nil, err
	}
	f.launches = append(f.launches, input)
	return map[string]any{"threadId": fmt.Sprintf("thread-%d", len(f.launches))}, nil
}

func (f *agentTestT3) Send(_ context.Context, id, text string, _ SendMode) (SendResult, error) {
	f.sends = append(f.sends, agentTestSend{id, text})
	return SendResult{}, nil
}

func (f *agentTestT3) CloseTree(_ context.Context, id string, deleteThread bool) (TreeResult, error) {
	f.closes = append(f.closes, agentTestClose{id, deleteThread})
	if f.skipClose {
		return TreeResult{Skipped: []map[string]string{{"id": id, "reason": "active run"}}}, nil
	}
	return TreeResult{}, nil
}

// RPC decodes the canned projection the same way the agent decodes T3's reply.
func (f *agentTestT3) RPC(_ context.Context, tag string, _, output any) error {
	if tag != "orchestration.getThreadProjection" {
		return fmt.Errorf("unexpected RPC %s", tag)
	}
	if f.rpcErr != nil {
		return f.rpcErr
	}
	return json.Unmarshal([]byte(f.projection), output)
}

// agentTestThread is a projection with one COS run that has replied, a context reading, and runtime requests (a JSON array).
func agentTestThread(status string, used int, requests string) string {
	return fmt.Sprintf(`{"runs":[{"id":"r1","status":"%s"}],"turnItems":[{"type":"user_message","runId":"r1","text":"User: hi"},{"type":"dynamic_tool","runId":"r1","title":"cos fleet"},{"type":"assistant_message","runId":"r1","text":"Hello **there**"}],"runtimeRequests":%s,"providerTurns":[{"tokenUsage":{"usedTokens":%d,"maxTokens":1000000}}]}`, status, requests, used)
}

// agentTestQuestion is one pending runtime request with a single two-option question.
func agentTestQuestion(status string) string {
	return `[{"id":"q1","status":"` + status + `","createdAt":"2026-10-11T10:00:00Z","payload":{"questions":[{"id":"pick","header":"Deploy","question":"Which window?","options":[{"label":"Now"},{"label":"Tonight"}]}]}}]`
}

// agentTestCompact has a hidden /compact run after the visible reply.
const agentTestCompact = `{"runs":[{"id":"r1","status":"completed"},{"id":"r2","status":"completed"}],"turnItems":[{"type":"assistant_message","runId":"r1","text":"Hello **there**"},{"type":"user_message","runId":"r2","text":"/compact keep only the open threads"},{"type":"assistant_message","runId":"r2","text":"done"}],"runtimeRequests":[],"providerTurns":[]}`

// agentTestEnv is one 3-node cluster with the chat and agent buckets; every subtest gets a fresh agent through reset.
type agentTestEnv struct {
	t         *testing.T
	ctx       context.Context
	chat      *Chat
	chatKV    jetstream.KeyValue
	agentKV   jetstream.KeyValue
	contextKV jetstream.KeyValue
	memoryKV  jetstream.KeyValue
	savingsKV jetstream.KeyValue
	js        jetstream.JetStream
	store     *Store
	fake      *agentTestT3
	remote    []agentTestRemote
	skipTree  bool
	a         *Agent
}

func agentTestOpen(t *testing.T, ctx context.Context, nodes []*Bus, bucket string) jetstream.KeyValue {
	t.Helper()
	var kv jetstream.KeyValue
	chatTestWait(t, bucket+" bucket", func() bool {
		for _, n := range nodes {
			k, err := openKV(ctx, n, bucket)
			if err == nil {
				kv = k
				return true
			}
		}
		return false
	})
	return kv
}

func newAgentTestEnv(t *testing.T, ctx context.Context) *agentTestEnv {
	t.Helper()
	nodes, _ := chatTestCluster(t, ctx)
	e := &agentTestEnv{ctx: ctx, chat: NewChat()}
	go e.chat.Run(ctx, nodes[0])
	// Every node follows the chat: the buckets are created by whichever node leads JetStream.
	for _, n := range nodes[1:] {
		go NewChat().Run(ctx, n)
	}
	chatTestWait(t, "chat ready", e.chat.Ready)
	e.chatKV = agentTestOpen(t, ctx, nodes, chatBucket)
	e.agentKV = agentTestOpen(t, ctx, nodes, agentBucket)
	e.contextKV = agentTestOpen(t, ctx, nodes, contextBucket)
	e.memoryKV = agentTestOpen(t, ctx, nodes, memoryBucket)
	e.savingsKV = agentTestOpen(t, ctx, nodes, savingsBucket)
	js, err := nodes[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	e.js = js
	// The rules engine's escalation log, created the way the autopilot creates it.
	if err := (&Autopilot{Host: "delta", js: js}).setup(ctx); err != nil {
		t.Fatal(err)
	}
	return e
}

// reset empties the chat, agent state and store, and makes a fresh agent with a fake T3.
func (e *agentTestEnv) reset(t *testing.T) {
	t.Helper()
	e.t = t
	// A delete can be lost to a JetStream leader change, so re-issue it until the cache empties.
	deadline := time.Now().Add(2 * time.Minute)
	for len(e.chat.Entries()) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for chat cleared")
		}
		for _, old := range e.chat.Entries() {
			if err := e.chatKV.Delete(e.ctx, old.ID); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
	}
	for _, key := range []string{"state", "restart"} {
		if err := e.agentKV.Purge(e.ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e.store = store
	e.fake = &agentTestT3{projection: `{}`}
	e.remote = nil
	e.skipTree = false
	e.a = &Agent{Host: "delta", Home: t.TempDir(), Exe: "/bin/cos", Chat: e.chat, Store: store, KV: e.agentKV, Context: e.contextKV, Memory: e.memoryKV, Savings: e.savingsKV, js: e.js,
		T3: func(context.Context) (agentT3, error) { return e.fake, nil },
		Remote: func(_ context.Context, host, command string, args []string, out any) error {
			e.remote = append(e.remote, agentTestRemote{host, command, args})
			if !e.skipTree {
				return nil
			}
			data, err := json.Marshal(TreeResult{Skipped: []map[string]string{{"threadId": args[0]}}})
			if err != nil {
				return err
			}
			return json.Unmarshal(data, out)
		},
	}
	e.step() // claims COS for this node
}

// say posts a user message and waits until this node's cache holds it, so the next step sees it.
func (e *agentTestEnv) say(text string) string {
	e.t.Helper()
	entry, err := e.chat.Put(e.ctx, ChatEntry{Role: "user", Text: text})
	if err != nil {
		e.t.Fatal(err)
	}
	chatTestWait(e.t, "user message in chat", func() bool { _, ok := e.chat.Entry(entry.ID); return ok })
	return entry.ID
}

// step retries only a lost KV revision race, as production does every second; other errors fail at once.
func (e *agentTestEnv) step() {
	e.t.Helper()
	for attempt := 1; ; attempt++ {
		err := e.a.Step(e.ctx)
		var apiErr *jetstream.APIError
		if err == nil {
			return
		}
		if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence || attempt == 5 {
			e.t.Fatal(err)
		}
		time.Sleep(time.Duration(50*attempt) * time.Millisecond)
	}
}

// start posts a first message and steps, which launches the COS thread as thread-1.
func (e *agentTestEnv) start() {
	e.t.Helper()
	e.say("start")
	e.step()
	e.expectLaunches(1)
}

func (e *agentTestEnv) expectLaunches(n int) {
	e.t.Helper()
	if got := len(e.fake.launches); got != n {
		e.t.Fatalf("launches = %d, want %d", got, n)
	}
}

func (e *agentTestEnv) expectBrief(want ...string) {
	e.t.Helper()
	brief, _ := e.fake.launches[len(e.fake.launches)-1]["message"].(string)
	for _, w := range want {
		if !strings.Contains(brief, w) {
			e.t.Fatalf("brief missing %q:\n%s", w, brief)
		}
	}
}

// savingTokens is today's tokens saved for kind; tests diff it because the ledger is shared by the subtests.
func (e *agentTestEnv) savingTokens(kind string) int {
	e.t.Helper()
	days, err := SavingsReport(e.ctx, e.savingsKV, 1, time.Now())
	if err != nil {
		e.t.Fatal(err)
	}
	return days[0].Kinds[kind].Tokens
}

func (e *agentTestEnv) state() AgentState {
	e.t.Helper()
	entry, err := e.agentKV.Get(e.ctx, "state")
	if err != nil {
		e.t.Fatal(err)
	}
	var st AgentState
	if err := json.Unmarshal(entry.Value(), &st); err != nil {
		e.t.Fatal(err)
	}
	return st
}

func (e *agentTestEnv) setState(st AgentState) {
	e.t.Helper()
	data, err := json.Marshal(st)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.agentKV.Put(e.ctx, "state", data); err != nil {
		e.t.Fatal(err)
	}
}

// cosEntries returns the COS bubbles, oldest first.
func (e *agentTestEnv) cosEntries() []ChatEntry {
	out := []ChatEntry{}
	for _, entry := range e.chat.Entries() {
		if entry.Role == "cos" {
			out = append(out, entry)
		}
	}
	return out
}

// waitAsk waits for the mirror to show an ask and returns a copy of its entry.
func (e *agentTestEnv) waitAsk() ChatEntry {
	e.t.Helper()
	var ask ChatEntry
	chatTestWait(e.t, "ask in chat", func() bool {
		for _, entry := range e.chat.Entries() {
			if entry.Ask != nil {
				ask, _ = e.chat.Entry(entry.ID)
				return true
			}
		}
		return false
	})
	return ask
}

// savePRs writes a live snapshot whose open PRs are ts-cos#<number>.
func (e *agentTestEnv) savePRs(numbers ...int) {
	e.t.Helper()
	live := Live{At: "x", Errors: map[string]string{}}
	for _, n := range numbers {
		live.PRs = append(live.PRs, PR{Repo: "ts-cos", Number: n})
	}
	if err := e.store.SaveLive(e.ctx, live); err != nil {
		e.t.Fatal(err)
	}
}

func TestAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	e := newAgentTestEnv(t, ctx)

	t.Run("first message launches the COS thread", func(t *testing.T) {
		e.reset(t)
		if _, err := e.contextKV.Put(ctx, "user-prefs", []byte("Prefer Haiku 5.5")); err != nil {
			t.Fatal(err)
		}
		id := e.say("hello cos")
		e.step()
		e.expectLaunches(1)
		e.expectBrief("Fresh thread because: first start", "## Memory", "Prefer Haiku 5.5", "## Live state", "User: hello cos")
		launch := e.fake.launches[0]
		if launch["title"] != "COS" {
			t.Fatal(launch["title"])
		}
		selection, _ := launch["modelSelection"].(map[string]any)
		if selection["model"] != "claude-opus-5-5" {
			t.Fatal(selection)
		}
		if len(e.fake.http) != 1 || e.fake.http[0].Method != "POST" || e.fake.http[0].Path != "/api/projects/mutate" {
			t.Fatal(e.fake.http)
		}
		payload, _ := e.fake.http[0].Input.(map[string]any)
		root := filepath.Join(e.a.Home, ".local/share/cos/agent")
		if payload["type"] != "project.create" || payload["workspaceRoot"] != root || launch["projectId"] != payload["projectId"] {
			t.Fatal(payload, launch)
		}
		rules, err := os.ReadFile(filepath.Join(root, "CLAUDE.md"))
		if err != nil || string(rules) != CurrentBehavior().AgentBrief() {
			t.Fatal("CLAUDE.md", err)
		}
		info, err := os.Stat(filepath.Join(root, "cos"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0700 {
			t.Fatal("cos wrapper mode", info.Mode())
		}
		wrapper, _ := os.ReadFile(filepath.Join(root, "cos"))
		if !strings.Contains(string(wrapper), "exec '/bin/cos'") {
			t.Fatal(string(wrapper))
		}
		st := e.state()
		if st.ThreadID != "thread-1" || st.Host != "delta" || st.Delivered != id || st.Reason != "first start" {
			t.Fatalf("state = %+v", st)
		}
		chatTestWait(t, "fresh thread notice", func() bool {
			for _, entry := range e.chat.Entries() {
				if entry.Role == "system" && strings.Contains(entry.Text, "fresh thread") {
					return true
				}
			}
			return false
		})
	})

	t.Run("later message is sent to the running thread", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.say("second")
		e.step()
		e.expectLaunches(1)
		if want := []agentTestSend{{"thread-1", "User: second"}}; !slices.Equal(e.fake.sends, want) {
			t.Fatal(e.fake.sends)
		}
	})

	t.Run("mirrors the COS reply into chat", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("completed", 1000, "[]")
		e.say("show status")
		e.step()
		var reply ChatEntry
		chatTestWait(t, "COS reply", func() bool {
			entries := e.cosEntries()
			if len(entries) == 1 {
				reply = entries[0]
			}
			return len(entries) == 1 && !reply.Streaming
		})
		if reply.Text != "Hello **there**" || !slices.Equal(reply.Tools, []string{"cos fleet"}) {
			t.Fatalf("reply = %+v", reply)
		}

		e.fake.projection = agentTestThread("running", 1000, "[]")
		e.say("again")
		e.step()
		chatTestWait(t, "streaming reply", func() bool {
			entries := e.cosEntries()
			return len(entries) == 1 && entries[0].ID == reply.ID && entries[0].Streaming
		})

		e.fake.projection = agentTestCompact
		e.say("after")
		e.step()
		e.say("settle")
		entries := e.cosEntries()
		if len(entries) != 1 || entries[0].Text != "Hello **there**" {
			t.Fatalf("hidden /compact run changed chat: %+v", entries)
		}
	})

	t.Run("pending question becomes an ask that resolves", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("pending"))
		e.say("deploy?")
		e.step()
		var askID string
		var ask *ChatAsk
		chatTestWait(t, "pending question", func() bool {
			for _, entry := range e.chat.Entries() {
				if entry.Ask != nil {
					askID, ask = entry.ID, entry.Ask
					return true
				}
			}
			return false
		})
		if ask.Host != "delta" || ask.RequestID != "q1" || ask.Done || len(ask.Questions) != 1 {
			t.Fatalf("ask = %+v", ask)
		}
		if got := ask.Questions[0].Options; !slices.Equal(got, []string{"Now", "Tonight"}) {
			t.Fatal(got)
		}

		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("resolved"))
		e.say("thanks")
		e.step()
		chatTestWait(t, "ask marked done", func() bool {
			entry, ok := e.chat.Entry(askID)
			return ok && entry.Ask != nil && entry.Ask.Done
		})
	})

	t.Run("re-mirror after a failed save keeps one ask", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("pending"))
		e.say("deploy?")
		e.step()
		asks := func() int {
			n := 0
			for _, entry := range e.chat.Entries() {
				if entry.Ask != nil {
					n++
				}
			}
			return n
		}
		chatTestWait(t, "pending question", func() bool { return asks() == 1 })
		st := e.state()
		st.Asked = nil // the save that recorded the ask never landed
		e.setState(st)
		e.step()
		e.say("settle")
		if n := asks(); n != 1 {
			t.Fatalf("asks = %d, want 1", n)
		}
	})

	t.Run("mark done keeps the answers recorded in the ask", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("pending"))
		e.say("deploy?")
		e.step()
		ask := e.waitAsk()
		ask.Ask.Answers = map[string]string{"pick": "Now"}
		if err := e.chat.Replace(ctx, ask); err != nil {
			t.Fatal(err)
		}
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("resolved"))
		e.say("thanks")
		e.step()
		chatTestWait(t, "ask marked done", func() bool {
			entry, ok := e.chat.Entry(ask.ID)
			return ok && entry.Ask.Done
		})
		if entry, _ := e.chat.Entry(ask.ID); entry.Ask.Answers["pick"] != "Now" {
			t.Fatalf("answers = %v", entry.Ask.Answers)
		}
	})

	t.Run("mark done does not overwrite an answer written after its read", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("pending"))
		e.say("deploy?")
		e.step()
		stale := e.waitAsk()
		answered, _ := e.chat.Entry(stale.ID)
		answered.Ask.Answers = map[string]string{"pick": "Now"}
		if err := e.chat.Replace(ctx, answered); err != nil {
			t.Fatal(err)
		}
		// Once this message shows, the watcher has applied the answer, so rolling the cache back is the only stale copy left.
		e.say("settle")
		e.chat.mu.Lock()
		e.chat.entries[stale.ID] = stale
		e.chat.mu.Unlock()
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("resolved"))
		var apiErr *jetstream.APIError
		if err := e.a.Step(ctx); !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Fatal("stale mark done:", err)
		}
		kv, err := e.chatKV.Get(ctx, stale.ID)
		if err != nil {
			t.Fatal(err)
		}
		var stored ChatEntry
		if err := json.Unmarshal(kv.Value(), &stored); err != nil {
			t.Fatal(err)
		}
		if stored.Ask.Done || stored.Ask.Answers["pick"] != "Now" {
			t.Fatalf("stored ask = %+v", stored.Ask)
		}
		// The cache catches up with the stored entry, and the next step marks the ask done from it.
		e.chat.apply(kv)
		e.step()
		chatTestWait(t, "ask marked done", func() bool {
			entry, ok := e.chat.Entry(stale.ID)
			return ok && entry.Ask.Done
		})
		if entry, _ := e.chat.Entry(stale.ID); entry.Ask.Answers["pick"] != "Now" {
			t.Fatalf("answers = %v", entry.Ask.Answers)
		}
	})

	t.Run("a question known only from its turn item becomes an ask", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = `{"runs":[{"id":"r1","status":"completed"}],"turnItems":[{"type":"user_input_request","runId":"r1","requestId":"q1","questions":[{"id":"pick","header":"Deploy","question":"Which window?","options":[{"label":"Now"},{"label":"Tonight"}]}]}],"runtimeRequests":[{"id":"q1","status":"pending","createdAt":"2026-10-11T10:00:00Z","payload":null,"request":null}],"providerTurns":[]}`
		e.say("deploy?")
		e.step()
		ask := e.waitAsk()
		if ask.Ask.RequestID != "q1" || len(ask.Ask.Questions) != 1 || !slices.Equal(ask.Ask.Questions[0].Options, []string{"Now", "Tonight"}) {
			t.Fatalf("ask = %+v", ask.Ask)
		}
	})

	t.Run("failover keeps the open ask until the new thread resolves it", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("pending"))
		e.say("deploy?")
		e.step()
		ask := e.waitAsk()
		e.say("are you there?") // the wake that relaunches the thread on this host
		st := e.state()
		st.Host = "beta"
		e.setState(st)
		e.step() // claim
		e.step() // relaunch
		e.expectLaunches(2)
		if got := e.state().Asked["q1"]; got != ask.ID {
			t.Fatalf("asked = %v, want q1 -> %s", e.state().Asked, ask.ID)
		}
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("resolved"))
		e.say("status?")
		e.step()
		chatTestWait(t, "ask marked done", func() bool {
			entry, ok := e.chat.Entry(ask.ID)
			return ok && entry.Ask.Done
		})
		if _, ok := e.state().Asked["q1"]; ok {
			t.Fatal("asked still open after the new thread resolved it")
		}
	})

	t.Run("restart keeps the open ask until the fresh thread resolves it", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("pending"))
		e.say("deploy?")
		e.step()
		ask := e.waitAsk()
		if _, err := e.agentKV.Put(ctx, "restart", []byte("new plan")); err != nil {
			t.Fatal(err)
		}
		e.say("hi")
		e.step()
		e.expectLaunches(2)
		if got := e.state().Asked["q1"]; got != ask.ID {
			t.Fatalf("asked = %v, want q1 -> %s", e.state().Asked, ask.ID)
		}
		if entry, _ := e.chat.Entry(ask.ID); entry.Ask.Done {
			t.Fatal("ask closed before the fresh thread saw the request resolve")
		}
		e.fake.projection = agentTestThread("completed", 1000, agentTestQuestion("resolved"))
		e.say("status?")
		e.step()
		chatTestWait(t, "ask marked done", func() bool {
			entry, ok := e.chat.Entry(ask.ID)
			return ok && entry.Ask.Done
		})
		if _, ok := e.state().Asked["q1"]; ok {
			t.Fatal("asked still open after the fresh thread resolved it")
		}
	})

	t.Run("focus change restarts with a new brief", func(t *testing.T) {
		e.reset(t)
		if err := e.contextKV.Purge(e.ctx, "focus"); err != nil {
			t.Fatal(err)
		}
		e.start()
		if _, err := e.contextKV.Put(e.ctx, "focus", []byte("Ship the chat")); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.contextKV.Purge(e.ctx, "focus") })
		e.step()
		e.expectLaunches(2)
		e.expectBrief("Fresh thread because: focus changed", "Focus changed: Ship the chat")
		if want := []agentTestClose{{"thread-1", true}}; !slices.Equal(e.fake.closes, want) {
			t.Fatal(e.fake.closes)
		}
	})

	t.Run("context over the limit restarts an idle thread", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("completed", 150000, "[]")
		e.say("next")
		e.step()
		e.expectLaunches(2)
		e.expectBrief("Fresh thread because: context reached 150000 tokens")
	})

	t.Run("context over the limit compacts a running thread once", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.projection = agentTestThread("running", 150000, "[]")
		e.say("next")
		e.step()
		e.say("again")
		e.step()
		compacts := 0
		for _, s := range e.fake.sends {
			if strings.HasPrefix(s.Text, "/compact") {
				compacts++
			}
		}
		if compacts != 1 {
			t.Fatalf("/compact sent %d times: %+v", compacts, e.fake.sends)
		}
		e.expectLaunches(1)
	})

	t.Run("brief carries pinned and relevant memory under the cap", func(t *testing.T) {
		e.reset(t)
		if _, err := e.contextKV.Put(ctx, "focus", []byte("Ship the chat")); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.contextKV.Purge(ctx, "focus") })
		// Ten ~470-token entries overflow the cap; the unrelated fact never matches the wake, so it stays out.
		base := time.Now().UTC().Add(-time.Hour)
		for i := range 10 {
			id := fmt.Sprintf("agent-mem-%02d", i)
			text := fmt.Sprintf("canary deploy %d: %s", i, strings.Repeat("hold each rollout at a health gate before widening. ", 36))
			if err := memoryPut(ctx, e.memoryKV, MemoryEntry{ID: id, Kind: "decision", Text: text, At: base.Add(time.Duration(i) * time.Minute)}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.memoryKV.Delete(ctx, id) })
		}
		if err := memoryPut(ctx, e.memoryKV, MemoryEntry{ID: "agent-mem-unrelated", Kind: "fact", Text: "NetBird admits nodes automatically", At: base}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.memoryKV.Delete(ctx, "agent-mem-unrelated") })
		retrieved := e.savingTokens(SavingRetrieval)
		e.start()
		if got := e.savingTokens(SavingRetrieval) - retrieved; got <= 0 {
			t.Fatalf("retrieval saving = %d after a restart with memory over the cap", got)
		}
		brief, r, err := e.a.brief(ctx, Dashboard{}, "first start", "User: how do canary deploys roll back")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("memory tokens %d of %d, %d entries omitted", r.Tokens, r.FullTokens, r.Omitted)
		if r.Tokens > agentMemoryTokens || r.FullTokens <= agentMemoryTokens || r.Omitted == 0 {
			t.Fatalf("memory tokens %d of %d, omitted %d", r.Tokens, r.FullTokens, r.Omitted)
		}
		for _, want := range []string{"Ship the chat", "canary deploy 9:", fmt.Sprintf("%d more relevant entries left out to stay under %d of %d tokens", r.Omitted, r.Tokens, r.FullTokens)} {
			if !strings.Contains(brief, want) {
				t.Fatalf("brief missing %q:\n%s", want, brief)
			}
		}
		if strings.Contains(brief, "NetBird") {
			t.Fatalf("unrelated memory in brief:\n%s", brief)
		}
	})

	t.Run("idle thread restarts when a fresh brief is cheaper than its context", func(t *testing.T) {
		e.reset(t)
		e.start()
		st := e.state()
		st.BriefTokens = 10_000
		e.setState(st)
		e.fake.projection = agentTestThread("completed", 50000, "[]")
		e.say("next")
		saved := e.savingTokens(SavingFreshBrief)
		e.step()
		e.expectLaunches(2)
		e.expectBrief("Fresh thread because: context 50000 tokens vs a 10000-token fresh brief")
		if got := e.savingTokens(SavingFreshBrief) - saved; got != 50000-10000 {
			t.Fatalf("fresh-brief saving = %d, want %d", got, 50000-10000)
		}
	})

	t.Run("idle thread keeps its context when a fresh brief is not under half", func(t *testing.T) {
		e.reset(t)
		e.start()
		st := e.state()
		st.BriefTokens = 10_000
		e.setState(st)
		e.fake.projection = agentTestThread("completed", 15000, "[]")
		e.say("next")
		e.step()
		e.expectLaunches(1)
		if got := e.state().Restart; got != "" {
			t.Fatalf("restart = %q", got)
		}
	})

	t.Run("busy thread is not restarted for a fresh brief", func(t *testing.T) {
		e.reset(t)
		e.start()
		st := e.state()
		st.BriefTokens = 10_000
		e.setState(st)
		e.fake.projection = agentTestThread("running", 50000, "[]")
		e.say("next")
		e.step()
		e.expectLaunches(1)
		if got := e.state().Restart; got != "" {
			t.Fatalf("restart = %q", got)
		}
	})

	t.Run("COS asked for a restart", func(t *testing.T) {
		e.reset(t)
		e.start()
		if _, err := e.agentKV.Put(ctx, "restart", []byte("new plan")); err != nil {
			t.Fatal(err)
		}
		e.say("hi")
		e.step()
		e.expectLaunches(2)
		e.expectBrief("COS asked: new plan")
		if _, err := e.agentKV.Get(ctx, "restart"); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal("restart key not deleted:", err)
		}
	})

	t.Run("a new behavior version restarts at the next wake", func(t *testing.T) {
		e.reset(t)
		e.start()
		st := e.state()
		st.Behavior = "older-bundle"
		e.setState(st)
		e.step()
		e.expectLaunches(1)
		e.say("hi")
		e.step()
		e.expectLaunches(2)
		e.expectBrief("behavior updated to embedded")
	})

	t.Run("idle thread restarts after four hours", func(t *testing.T) {
		e.reset(t)
		e.start()
		st := e.state()
		st.LastWake = time.Now().UTC().Add(-5 * time.Hour)
		e.setState(st)
		e.say("hi")
		e.step()
		e.expectLaunches(2)
		e.expectBrief("Fresh thread because: idle for 4 hours")
	})

	t.Run("finished PR restarts with its repo and number", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.savePRs(1, 2)
		e.step()
		e.savePRs(1)
		e.step()
		if got := e.state().Restart; got != "finished ts-cos#2" {
			t.Fatalf("restart = %q", got)
		}
		e.say("status?")
		e.step()
		e.expectLaunches(2)
		e.expectBrief("Fresh thread because: finished ts-cos#2")
	})

	t.Run("a vanished thread is relaunched at the next wake", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.rpcErr = &T3Error{Host: "delta", Tag: "orchestration.getThreadProjection", Types: []string{"OrchestrationThreadNotFoundError"}}
		e.say("still there?")
		e.step()
		e.expectLaunches(2)
		e.expectBrief("COS thread vanished")
	})

	t.Run("an undeletable previous thread becomes an orphan", func(t *testing.T) {
		e.reset(t)
		e.start()
		e.fake.skipClose = true
		e.skipTree = true // the orphan retry also finds it busy
		if _, err := e.agentKV.Put(e.ctx, "restart", []byte("new plan")); err != nil {
			t.Fatal(err)
		}
		e.say("hi")
		e.step()
		e.expectLaunches(2)
		if st := e.state(); len(st.Orphans) != 1 || st.Orphans[0].ThreadID != "thread-1" {
			t.Fatalf("orphans = %+v", st.Orphans)
		}
	})

	t.Run("a deleted project is resolved again after a failed launch", func(t *testing.T) {
		e.reset(t)
		e.a.project = "stale-project"
		e.fake.launchErr = &T3Error{Host: "delta", Tag: "orchestration.launchThread", Types: []string{"ProjectNotFoundError"}}
		e.say("hi")
		if err := e.a.Step(e.ctx); err == nil {
			t.Fatal("launch error not reported")
		}
		if e.a.project != "" {
			t.Fatalf("project still cached: %q", e.a.project)
		}
		e.step()
		e.expectLaunches(1)
	})

	t.Run("failover relaunches on this host", func(t *testing.T) {
		e.reset(t)
		id := e.say("are you there?")
		e.setState(AgentState{Host: "beta", ThreadID: "old-thread", Delivered: id})
		e.step() // claim
		e.step()
		e.expectLaunches(1)
		e.expectBrief("Fresh thread because: failover from beta", "has no reply yet")
		if len(e.remote) != 1 || e.remote[0].Host != "beta" || e.remote[0].Command != "delete" || !slices.Equal(e.remote[0].Args, []string{"old-thread"}) {
			t.Fatalf("remote = %+v", e.remote)
		}
		if orphans := e.state().Orphans; len(orphans) != 0 {
			t.Fatal("deleted orphan still saved:", orphans)
		}
	})

	t.Run("skipped orphan stays for the next check", func(t *testing.T) {
		e.reset(t)
		e.skipTree = true
		id := e.say("are you there?")
		e.setState(AgentState{Host: "beta", ThreadID: "old-thread", Delivered: id})
		e.step() // claim
		e.step()
		orphans := e.state().Orphans
		if len(orphans) != 1 || orphans[0] != (AgentThread{"beta", "old-thread"}) {
			t.Fatalf("orphans = %+v", orphans)
		}
	})
}

func TestUnanswered(t *testing.T) {
	cases := []struct {
		name    string
		entries []ChatEntry
		want    bool
	}{
		{"empty", nil, false},
		{"user only", []ChatEntry{{Role: "user"}}, true},
		{"cos answered", []ChatEntry{{Role: "user"}, {Role: "cos"}}, false},
		{"newer user message", []ChatEntry{{Role: "cos"}, {Role: "user"}}, true},
		{"system notices are skipped", []ChatEntry{{Role: "user"}, {Role: "system"}}, true},
		{"only system", []ChatEntry{{Role: "system"}}, false},
		{"cos after user and system", []ChatEntry{{Role: "user"}, {Role: "cos"}, {Role: "system"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := unanswered(tc.entries); got != tc.want {
				t.Fatalf("unanswered = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestAgentCommandUsage(t *testing.T) {
	for _, args := range [][]string{{"restart"}, {"bogus", "x"}, {"restart", "a", "b"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out strings.Builder
			err := agentCommand(context.Background(), Config{Home: t.TempDir()}, args, &out)
			if err == nil || !strings.HasPrefix(err.Error(), "usage:") {
				t.Fatalf("err = %v", err)
			}
			if out.Len() != 0 {
				t.Fatal("usage error wrote output:", out.String())
			}
		})
	}
}
