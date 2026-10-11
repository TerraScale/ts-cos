package cos

// Contract tests replay real T3 responses recorded from T3 0.0.46-nightly.20261010.2935 on delta
// (throwaway Haiku 5.5 threads: launch, send, /compact, model, rename, a user_input request,
// settle, delete). After a T3 upgrade, re-record testdata/t3; schema drift must surface as
// T3SchemaError, never as silently empty data.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func t3Fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/t3/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// t3Frames returns the recorded frames of a .jsonl fixture, one JSON text per frame.
func t3Frames(t *testing.T, name string) []string {
	t.Helper()
	var out []string
	scanner := bufio.NewScanner(strings.NewReader(string(t3Fixture(t, name))))
	scanner.Buffer(nil, 16<<20)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}
	return out
}

func t3Exit(t *testing.T, name string) string {
	t.Helper()
	var exits map[string]json.RawMessage
	if err := json.Unmarshal(t3Fixture(t, "exits.json"), &exits); err != nil {
		t.Fatal(err)
	}
	return string(exits[name])
}

func t3Chunk(values ...any) string {
	data, _ := json.Marshal(map[string]any{"_tag": "Chunk", "requestId": "1", "values": values})
	return string(data)
}

func t3Success(value any) string {
	data, _ := json.Marshal(map[string]any{"_tag": "Exit", "requestId": "1", "exit": map[string]any{"_tag": "Success", "value": value}})
	return string(data)
}

// t3Replay is a fake T3 that answers each WS request with scripted frames. Like real T3 it
// sends the next frame after a Chunk only once the client acknowledges it.
type t3Replay struct {
	t      *testing.T
	shell  string
	script func(tag string, payload map[string]any) []string
	mu     sync.Mutex
	acks   int
	open   int
	server *httptest.Server
}

func newT3Replay(t *testing.T, script func(string, map[string]any) []string) *t3Replay {
	r := &t3Replay{t: t, script: script, shell: string(t3Fixture(t, "subscribe-shell.jsonl"))}
	r.server = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.server.Close)
	return r
}

func (r *t3Replay) client() *T3 {
	return &T3{Config: Config{HTTP: r.server.Client()}, Host: "delta", BaseURL: r.server.URL, token: "fake"}
}

func (r *t3Replay) serve(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/api/auth/websocket-ticket":
		_, _ = w.Write([]byte(`{"ticket":"t"}`))
	case "/api/orchestration/shell":
		var first struct {
			Values []struct {
				Snapshot json.RawMessage `json:"snapshot"`
			} `json:"values"`
		}
		_ = json.Unmarshal([]byte(strings.SplitN(r.shell, "\n", 2)[0]), &first)
		_, _ = w.Write(first.Values[0].Snapshot)
	case "/ws":
		ws, err := websocket.Accept(w, req, nil)
		if err != nil {
			r.t.Error(err)
			return
		}
		defer ws.CloseNow()
		r.mu.Lock()
		r.open++
		r.mu.Unlock()
		defer func() { r.mu.Lock(); r.open--; r.mu.Unlock() }()
		ctx := req.Context()
		_, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		var request struct {
			Tag     string         `json:"tag"`
			Payload map[string]any `json:"payload"`
		}
		if json.Unmarshal(data, &request) != nil {
			r.t.Error("invalid request")
			return
		}
		for _, frame := range r.script(request.Tag, request.Payload) {
			var m map[string]any
			if err := json.Unmarshal([]byte(frame), &m); err != nil {
				r.t.Errorf("bad scripted frame: %v", err)
				return
			}
			m["requestId"] = "1"
			out, _ := json.Marshal(m)
			if ws.Write(ctx, websocket.MessageText, out) != nil {
				return
			}
			if m["_tag"] == "Exit" {
				return
			}
			if m["_tag"] == "Chunk" {
				_, ack, err := ws.Read(ctx)
				if err != nil {
					return
				}
				if string(ack) != `{"_tag":"Ack","requestId":"1"}` {
					r.t.Errorf("expected Ack, got %s", ack)
					return
				}
				r.mu.Lock()
				r.acks++
				r.mu.Unlock()
			}
		}
		// Subscriptions stay open until the client leaves.
		_, _, _ = ws.Read(ctx)
	default:
		http.NotFound(w, req)
	}
}

func (r *t3Replay) openSockets() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.open
}

func collectThread(t *testing.T, c *T3, until func([]ThreadEvent) bool) ([]ThreadEvent, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var events []ThreadEvent
	stop := errors.New("stop")
	err := c.SubscribeThread(ctx, "thread-1", func(e ThreadEvent) error {
		events = append(events, e)
		if until(events) {
			return stop
		}
		return nil
	})
	if errors.Is(err, stop) {
		err = nil
	}
	return events, err
}

func TestT3ContractThreadLifecycle(t *testing.T) {
	frames := t3Frames(t, "subscribe-thread-lifecycle.jsonl")
	replay := newT3Replay(t, func(tag string, payload map[string]any) []string {
		if tag != "orchestration.subscribeThread" || payload["threadId"] != "thread-1" {
			t.Errorf("unexpected request %s %v", tag, payload)
		}
		return frames
	})
	events, err := collectThread(t, replay.client(), func(e []ThreadEvent) bool { return e[len(e)-1].Type == "thread.metadata-updated" })
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 72 || events[0].Type != "snapshot" || events[0].Snapshot == nil || !strings.Contains(events[0].Snapshot.Thread.Title, "TS-812 probe") {
		t.Fatalf("got %d events, first %+v", len(events), events[0])
	}
	var statuses, texts []string
	var used []int
	last := int64(0)
	for _, e := range events[1:] {
		if e.ID == "" || e.Sequence <= last || e.Host != "delta" || !strings.Contains(e.ThreadID, "08c363a5") {
			t.Fatalf("event identity lost: %+v", e)
		}
		last = e.Sequence
		if e.Run != nil {
			statuses = append(statuses, e.Run.Status)
		}
		if e.Message != nil {
			texts = append(texts, e.Message.Role+":"+e.Message.Text)
		}
		if e.Context != nil {
			used = append(used, e.Context.UsedTokens)
			if e.Context.MaxTokens != 1000000 || e.Context.Provider != "claudeAgent" {
				t.Fatalf("context usage = %+v", e.Context)
			}
		}
	}
	wantStatuses := []string{"waiting", "completed", "starting", "running", "waiting", "completed", "starting", "running", "waiting", "completed"}
	if !slices.Equal(statuses, wantStatuses) {
		t.Errorf("run statuses = %v, want %v", statuses, wantStatuses)
	}
	wantTexts := []string{"assistant:PONG", "user:Now reply with exactly PING.", "assistant:PING", "user:/compact Keep only the words PONG and PING."}
	if !slices.Equal(texts, wantTexts) {
		t.Errorf("messages = %v, want %v", texts, wantTexts)
	}
	if !slices.Contains(used, 30191) || !slices.Contains(used, 30376) || !slices.Contains(used, 4593) {
		t.Errorf("context usage values = %v, want the recorded 30191 -> 30376 -> 4593 (compaction)", used)
	}
	// The callback stopped on the last chunk, so it is the only one never acknowledged.
	if replay.acks != len(frames)-1 {
		t.Errorf("acknowledged %d chunks, want %d", replay.acks, len(frames)-1)
	}
}

func TestT3ContractPendingRequest(t *testing.T) {
	frames := t3Frames(t, "subscribe-thread-request.jsonl")
	replay := newT3Replay(t, func(string, map[string]any) []string { return frames })
	events, err := collectThread(t, replay.client(), func(e []ThreadEvent) bool { return len(e) == 29 })
	if err != nil {
		t.Fatal(err)
	}
	var requests []string
	var questions json.RawMessage
	for _, e := range events {
		if e.Request != nil {
			requests = append(requests, e.Request.Kind+":"+e.Request.Status)
		}
		if e.Item != nil && e.Item.Type == "user_input_request" {
			questions = e.Item.Questions
		}
	}
	if !slices.Equal(requests, []string{"user_input:pending", "user_input:resolved"}) {
		t.Errorf("runtime requests = %v", requests)
	}
	if !strings.Contains(string(questions), "Should I reply PONG or PING?") {
		t.Errorf("questions = %s", questions)
	}

	p, err := replay.client().decodeProjection("test", t3Fixture(t, "projection-pending.json"))
	if err != nil {
		t.Fatal(err)
	}
	pending := p.PendingRequests()
	data, _ := json.Marshal(pending)
	if len(pending) != 1 || pending[0]["kind"] != "user_input" || !strings.Contains(string(data), `"options":[{"label":"PONG"`) {
		t.Errorf("pending requests = %s", data)
	}
}

func TestT3ContractShell(t *testing.T) {
	replay := newT3Replay(t, func(tag string, _ map[string]any) []string {
		if tag != "orchestration.subscribeShell" {
			t.Errorf("unexpected %s", tag)
		}
		return t3Frames(t, "subscribe-shell.jsonl")
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var kinds []string
	stop := errors.New("stop")
	err := replay.client().SubscribeShell(ctx, func(e ShellEvent) error {
		switch {
		case e.Snapshot != nil:
			kinds = append(kinds, "snapshot:"+e.Snapshot.Projects[0].Title)
			if r := e.Snapshot.Threads[0].PendingRequest(); e.Sequence != 838500 || r == nil || r.Kind != "user_input" || r.CreatedAt == "" || r.Status != "pending" {
				t.Errorf("snapshot = %+v", e)
			}
		case e.Thread != nil:
			kinds = append(kinds, e.Thread.Status)
			if e.Thread.Status == "completed" && (e.Thread.LatestRunID == "" || e.Thread.LatestRunDone == "") {
				t.Errorf("completed thread without its latest run: %+v", e.Thread)
			}
		}
		if len(kinds) == 12 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
	want := []string{"snapshot:ts-cos", "running", "waiting", "completed", "running", "waiting", "completed", "starting", "running", "waiting", "completed", "completed"}
	if !slices.Equal(kinds, want) {
		t.Errorf("shell events = %v, want %v", kinds, want)
	}
	s, err := replay.client().Snapshot(ctx)
	if err != nil || len(s.Threads) != 1 || s.Threads[0].Title != "TS-812 probe approval (throwaway)" {
		t.Fatalf("Snapshot() = %+v, %v", s, err)
	}
}

func TestT3ContractExits(t *testing.T) {
	replay := newT3Replay(t, func(tag string, payload map[string]any) []string {
		switch {
		case tag == "orchestration.launchThread":
			return []string{t3Exit(t, "launch")}
		case tag == "orchestration.getThreadProjection":
			return []string{t3Exit(t, "projectionMissing")}
		case payload["type"] == "run.interrupt":
			return []string{t3Exit(t, "interruptRejected")}
		}
		return []string{t3Exit(t, "dispatch")}
	})
	c := replay.client()
	launched, err := c.Launch(t.Context(), map[string]any{"projectId": "p", "title": "x"})
	if err != nil || !strings.Contains(launched.(map[string]any)["threadId"].(string), "08c363a5") {
		t.Fatalf("Launch() = %v, %v", launched, err)
	}
	if out, err := c.Dispatch(t.Context(), map[string]any{"type": "thread.settle", "threadId": "x"}); err != nil || out.(map[string]any)["sequence"] == nil {
		t.Fatalf("Dispatch() = %v, %v", out, err)
	}
	_, err = c.Projection(t.Context(), "thread:does-not-exist")
	var t3err *T3Error
	if !errors.As(err, &t3err) || !t3err.NotFound() || !slices.Contains(t3err.Types, "OrchestrationV2GetThreadProjectionError") || strings.Contains(err.Error(), "Failed to load") {
		t.Fatalf("missing projection error = %v", err)
	}
	_, err = c.Dispatch(t.Context(), map[string]any{"type": "run.interrupt", "threadId": "x", "runId": "r"})
	if !errors.As(err, &t3err) || t3err.NotFound() || !strings.Contains(err.Error(), "OrchestrationV2DispatchCommandError > OrchestratorDispatchError") || strings.Contains(err.Error(), "interruptible") {
		t.Fatalf("rejected interrupt error = %v", err)
	}
}

func TestT3ContractContextUsage(t *testing.T) {
	p, err := (&T3{Host: "delta"}).decodeProjection("test", t3Fixture(t, "projection-compacted.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := ContextOf(p); got.UsedTokens != 4593 || got.MaxTokens != 1000000 || got.Provider != "claudeAgent" {
		t.Errorf("ContextOf(compacted) = %+v", got)
	}
}

// mutateFrame rewrites the first recorded frame containing marker.
func mutateFrame(frames []string, marker, old, replacement string) []string {
	out := slices.Clone(frames)
	for i, f := range out {
		if strings.Contains(f, marker) {
			out[i] = strings.Replace(f, old, replacement, 1)
			break
		}
	}
	return out
}

func TestT3SchemaDriftIsReported(t *testing.T) {
	thread := t3Frames(t, "subscribe-thread-lifecycle.jsonl")
	shell := t3Frames(t, "subscribe-shell.jsonl")
	tests := []struct {
		name   string
		tag    string
		frames []string
		detail string
	}{
		{"shell schema version", "orchestration.subscribeShell", mutateFrame(shell, `"schemaVersion":2`, `"schemaVersion":2`, `"schemaVersion":3`), "snapshot schemaVersion is not 2"},
		{"shell thread without status", "orchestration.subscribeShell", mutateFrame(shell, `"kind":"thread.updated"`, `"status":"running"`, `"state":"running"`), "thread.updated without thread id or status"},
		{"event type renamed", "orchestration.subscribeThread", mutateFrame(thread, `"type":"provider-turn.updated"`, `"type":"provider-turn.updated"`, `"eventType":"provider-turn.updated"`), "event without type"},
		{"unknown stream kind", "orchestration.subscribeThread", mutateFrame(thread, `"kind":"event"`, `"kind":"event"`, `"kind":"delta"`), `unknown stream value kind "delta"`},
		{"projection lost runs", "orchestration.subscribeThread", mutateFrame(thread, `"kind":"snapshot"`, `"runs":`, `"runList":`), `projection has no "runs"`},
		{"run status renamed", "orchestration.subscribeThread", mutateFrame(thread, `"type":"run.updated"`, `"status":"waiting"`, `"state":"waiting"`), "run.updated payload without id or status"},
		{"message role changed type", "orchestration.subscribeThread", mutateFrame(thread, `"type":"message.updated"`, `"role":"assistant"`, `"role":7`), "message.updated payload"},
		{"unknown exit", "orchestration.getThreadProjection", []string{`{"_tag":"Exit","requestId":"1","exit":{"_tag":"Interrupted"}}`}, `unknown exit "Interrupted"`},
		{"chunk on unary RPC", "orchestration.getThreadProjection", []string{t3Chunk(map[string]any{})}, "unexpected stream chunk"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replay := newT3Replay(t, func(string, map[string]any) []string { return tt.frames })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			c := replay.client()
			var err error
			switch tt.tag {
			case "orchestration.subscribeShell":
				err = c.SubscribeShell(ctx, func(ShellEvent) error { return nil })
			case "orchestration.subscribeThread":
				err = c.SubscribeThread(ctx, "thread-1", func(ThreadEvent) error { return nil })
			default:
				_, err = c.Projection(ctx, "thread-1")
			}
			var schema *T3SchemaError
			if !errors.As(err, &schema) || !strings.Contains(err.Error(), "T3 schema changed on delta: "+tt.tag) || !strings.Contains(err.Error(), tt.detail) {
				t.Fatalf("error = %v, want T3SchemaError with %q", err, tt.detail)
			}
		})
	}
	t.Run("shell HTTP schema version", func(t *testing.T) {
		replay := newT3Replay(t, nil)
		replay.shell = strings.Replace(replay.shell, `"schemaVersion":2`, `"schemaVersion":3`, 1)
		_, err := replay.client().Snapshot(t.Context())
		var schema *T3SchemaError
		if !errors.As(err, &schema) || !strings.Contains(err.Error(), "schemaVersion 3, COS understands 2") {
			t.Fatalf("error = %v", err)
		}
	})
}

// compactScript plays an idle thread, the /compact dispatch, and the run it starts.
func compactScript(t *testing.T) (func(string, map[string]any) []string, *[]map[string]any) {
	var after map[string]any
	if err := json.Unmarshal(t3Fixture(t, "projection-compacted.json"), &after); err != nil {
		t.Fatal(err)
	}
	var before map[string]any
	_ = json.Unmarshal(t3Fixture(t, "projection-compacted.json"), &before)
	turns := before["providerTurns"].([]any)
	before["providerTurns"] = turns[:len(turns)-1]
	var mu sync.Mutex
	var dispatched []map[string]any
	messageID := ""
	return func(tag string, payload map[string]any) []string {
		mu.Lock()
		defer mu.Unlock()
		switch tag {
		case "orchestration.dispatchCommand":
			dispatched = append(dispatched, payload)
			messageID, _ = payload["messageId"].(string)
			return []string{t3Exit(t, "dispatch")}
		case "orchestration.subscribeThread":
			snapshot := t3Chunk(map[string]any{"kind": "snapshot", "snapshotSequence": 1, "projection": before})
			if messageID == "" {
				return []string{snapshot}
			}
			run := func(seq int, status string) string {
				return t3Chunk(map[string]any{"kind": "event", "sequence": seq, "event": map[string]any{"id": "e" + status, "threadId": "thread-1", "type": "run.updated",
					"payload": map[string]any{"id": "run-4", "status": status, "userMessageId": messageID}}})
			}
			return []string{snapshot, run(2, "running"), run(3, "completed")}
		default:
			if messageID != "" {
				return []string{t3Success(after)}
			}
			return []string{t3Success(before)}
		}
	}, &dispatched
}

func TestT3CompactWaitsForItsRun(t *testing.T) {
	script, dispatched := compactScript(t)
	replay := newT3Replay(t, script)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	out, err := replay.client().Compact(ctx, "thread-1", "Keep the plan.")
	if err != nil {
		t.Fatal(err)
	}
	if out.Before.UsedTokens != 30376 || out.After.UsedTokens != 4593 {
		t.Errorf("Compact() = %+v, want 30376 -> 4593", out)
	}
	if len(*dispatched) != 1 || (*dispatched)[0]["text"] != "/compact Keep the plan." || (*dispatched)[0]["dispatchMode"].(map[string]any)["type"] != "start_immediately" {
		t.Errorf("dispatched = %v", *dispatched)
	}
	// The replay counts a socket closed only after its read sees the client's close, which lags the return.
	deadline := time.Now().Add(2 * time.Second)
	for replay.openSockets() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := replay.openSockets(); n != 0 {
		t.Errorf("%d T3 sockets left open", n)
	}
}

func TestT3SendModes(t *testing.T) {
	tests := []struct {
		status string
		mode   SendMode
		want   string
	}{
		{"completed", SendSteer, "start_immediately"},
		{"running", SendSteer, "steer_active"},
		{"waiting", SendSteer, "steer_active"},
		{"queued", SendSteer, "queue_after_active"},
		{"running", SendQueue, "queue_after_active"},
		{"", SendQueue, "start_immediately"},
	}
	for _, tt := range tests {
		t.Run(tt.status+"/"+string(tt.mode), func(t *testing.T) {
			var got map[string]any
			replay := newT3Replay(t, func(tag string, payload map[string]any) []string {
				if tag == "orchestration.dispatchCommand" {
					got = payload["dispatchMode"].(map[string]any)
					return []string{t3Exit(t, "dispatch")}
				}
				runs := []any{}
				if tt.status != "" {
					runs = append(runs, map[string]any{"id": "run-1", "status": tt.status})
				}
				return []string{t3Success(map[string]any{"thread": map[string]any{"id": "thread-1"}, "runs": runs, "messages": []any{}, "runtimeRequests": []any{}, "providerTurns": []any{}, "turnItems": []any{}})}
			})
			out, err := replay.client().Send(t.Context(), "thread-1", "hi", tt.mode)
			if err != nil || out.Mode != tt.want || got["type"] != tt.want || out.MessageID == "" {
				t.Fatalf("Send() = %+v, %v; dispatched %v; want %s", out, err, got, tt.want)
			}
			if tt.want == "steer_active" && got["targetRunId"] != "run-1" {
				t.Errorf("steer target = %v", got["targetRunId"])
			}
		})
	}
}

func TestT3InterruptStopsActiveRuns(t *testing.T) {
	var commands []string
	replay := newT3Replay(t, func(tag string, payload map[string]any) []string {
		if tag == "orchestration.dispatchCommand" {
			commands = append(commands, payload["type"].(string)+":"+payload["runId"].(string))
			return []string{t3Exit(t, "dispatch")}
		}
		return []string{t3Success(map[string]any{"thread": map[string]any{"id": "thread-1"}, "messages": []any{}, "runtimeRequests": []any{}, "providerTurns": []any{}, "turnItems": []any{},
			"runs": []any{map[string]any{"id": "done", "status": "completed"}, map[string]any{"id": "live", "status": "running"}, map[string]any{"id": "next", "status": "queued"}}})}
	})
	stopped, err := replay.client().Interrupt(t.Context(), "thread-1")
	if err != nil || !slices.Equal(stopped, []string{"live", "next"}) || !slices.Equal(commands, []string{"run.interrupt:live", "queued-run.cancel:next"}) {
		t.Fatalf("Interrupt() = %v, %v; commands %v", stopped, err, commands)
	}
}
