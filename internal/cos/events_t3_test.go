package cos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// testdata/t3_shell.jsonl holds real orchestration.subscribeShell frames recorded on
// delta (2026-10-11): a snapshot and TS-815's running -> waiting -> completed turn.
const t3TestThread = "thread:project:mcp%3A2553306e-4179-4b40-9cdd-cdc2bd7c3c49:"

func t3TestFrames(t *testing.T) [][]byte {
	t.Helper()
	f, err := os.Open("testdata/t3_shell.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var frames [][]byte
	scanner := bufio.NewScanner(f)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		frames = append(frames, append([]byte(nil), scanner.Bytes()...))
	}
	return frames
}

func t3TestRows(t *testing.T) (snapshot []t3ShellRow, updates []t3ShellRow) {
	t.Helper()
	for _, frame := range t3TestFrames(t) {
		var chunk struct{ Values []t3ShellValue }
		if err := json.Unmarshal(frame, &chunk); err != nil {
			t.Fatal(err)
		}
		for _, v := range chunk.Values {
			if v.Kind == "snapshot" {
				snapshot = append(snapshot, v.Snapshot.Threads...)
			} else {
				updates = append(updates, v.Thread)
			}
		}
	}
	return snapshot, updates
}

func TestT3TrackerRecordedShell(t *testing.T) {
	snapshot, updates := t3TestRows(t)
	if len(snapshot) != 4 || len(updates) != 2 {
		t.Fatalf("fixture: %d snapshot rows, %d updates", len(snapshot), len(updates))
	}
	tracker := newT3Tracker("delta")
	now := time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)
	var events []Event
	for _, row := range snapshot {
		got, ended := tracker.observe(row, true, now)
		if ended != (len(got) > 0) {
			t.Fatalf("ended=%v with %d events", ended, len(got))
		}
		events = append(events, got...)
	}
	// Catch-up keeps turns inside the dedup window and skips the 2026-10-09 completion.
	if len(events) != 2 || events[0].Kind != KindTurnInterrupted || events[1].Kind != KindTurnInterrupted ||
		events[0].Ticket != "TS-808" || events[1].Ticket != "TS-811" {
		t.Fatalf("snapshot events %#v", events)
	}
	// Reconnecting replays the snapshot; nothing is emitted twice.
	for _, row := range snapshot {
		if got, _ := tracker.observe(row, true, now); len(got) != 0 {
			t.Fatalf("replayed %#v", got)
		}
	}
	waiting, ended := tracker.observe(updates[0], false, now)
	if len(waiting) != 0 || ended {
		t.Fatalf("waiting emitted %#v", waiting)
	}
	completed, ended := tracker.observe(updates[1], false, now)
	if len(completed) != 1 || !ended {
		t.Fatalf("completed emitted %#v", completed)
	}
	e := completed[0]
	if e.Kind != KindTurnCompleted || e.Severity != SeverityInfo || e.Host != "delta" || e.Ticket != "TS-815" ||
		!strings.HasPrefix(e.Thread, t3TestThread) || e.Time != time.Date(2026, 10, 11, 0, 46, 22, 503e6, time.UTC) ||
		e.ID != "t3:"+updates[1].LatestRunID+":completed" || e.Validate() != nil {
		t.Fatalf("turn.completed %#v (%v)", e, e.Validate())
	}

	// No pending runtime request was live while recording; the row shape follows T3's
	// shell schema (projection.runtimeRequests[pending]).
	row := updates[1]
	row.PendingRuntimeRequest = &t3PendingRequest{ID: "request-1", Kind: "user-input", CreatedAt: "2026-10-11T00:50:00.000Z"}
	pending, _ := tracker.observe(row, false, now)
	if len(pending) != 1 || pending[0].Kind != KindRequestPending || pending[0].Severity != SeverityUrgent || pending[0].Validate() != nil {
		t.Fatalf("request.pending %#v", pending)
	}
	if again, _ := tracker.observe(row, false, now); len(again) != 0 {
		t.Fatalf("pending repeated %#v", again)
	}
}

func t3TestProjection(percent int) Projection {
	var p Projection
	_ = json.Unmarshal(t3TestProjectionJSON(percent), &p)
	return p
}

// t3TestProjectionJSON has every key T3's projection must carry.
func t3TestProjectionJSON(percent int) []byte {
	return []byte(`{"thread":{"id":"t"},"runs":[],"runtimeRequests":[],"turnItems":[],
		"messages":[{"role":"user","text":"go"},{"role":"assistant","text":"Done.\nNEEDS-USER: Which merge order?\nOPTIONS: A | B\nWHY: both touch web.go"}],
		"providerTurns":[{"usage":{"usedTokens":` + fmt.Sprint(percent*2000) + `,"maxTokens":200000}}]}`)
}

func TestT3TrackerEndedTurn(t *testing.T) {
	_, updates := t3TestRows(t)
	tracker := newT3Tracker("delta")
	now := time.Now()
	events := tracker.ended(updates[1], t3TestProjection(85), now)
	if len(events) != 2 || events[0].Kind != KindContextHigh || events[1].Kind != KindNeedsUser {
		t.Fatalf("events %#v", events)
	}
	var ask struct {
		Question string
		Options  []string
		Why      string
	}
	if err := json.Unmarshal(events[1].Data, &ask); err != nil || ask.Question != "Which merge order?" || len(ask.Options) != 2 || ask.Why != "both touch web.go" {
		t.Fatalf("needs_user data %s", events[1].Data)
	}
	for _, e := range events {
		if err := e.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	// 85% -> 92% crosses only the 90% mark; staying above emits nothing.
	if events := tracker.ended(updates[1], t3TestProjection(92), now); len(events) != 2 || !strings.Contains(string(events[0].Data), `"mark":90`) {
		t.Fatalf("second mark %#v", events)
	}
	if events := tracker.ended(updates[1], t3TestProjection(95), now); len(events) != 1 || events[0].Kind != KindNeedsUser {
		t.Fatalf("above marks %#v", events)
	}
}

// TestT3EventsReconnect serves the recorded frames from a fake T3, drops the first
// connection after its snapshot, and checks the turn arrives once after reconnecting.
func TestT3EventsReconnect(t *testing.T) {
	frames := t3TestFrames(t)
	var connections atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/auth/websocket-ticket", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ticket":"t"}`))
	})
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ctx := r.Context()
		_, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		var request struct{ Tag string }
		_ = json.Unmarshal(data, &request)
		if request.Tag == "orchestration.getThreadProjection" {
			var value bytes.Buffer // T3 frames are one line each
			_ = json.Compact(&value, t3TestProjectionJSON(85))
			_ = ws.Write(ctx, websocket.MessageText, []byte(`{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":`+value.String()+`}}`))
			return
		}
		send := frames
		if connections.Add(1) == 1 {
			send = frames[:1]
		}
		for _, frame := range send {
			if ws.Write(ctx, websocket.MessageText, frame) != nil {
				return
			}
			if _, ack, err := ws.Read(ctx); err != nil || string(ack) != `{"_tag":"Ack","requestId":"1"}` {
				t.Errorf("expected Ack, got %q %v", ack, err)
				return
			}
		}
		if connections.Load() > 1 {
			<-ctx.Done()
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	published := make(chan Event, 32)
	open := func(context.Context) (*T3, error) {
		return &T3{Config: Config{HTTP: server.Client()}, Host: "delta", BaseURL: server.URL, token: "t"}, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runT3Events(ctx, "delta", open, func(_ context.Context, e Event) error { published <- e; return nil })
	}()
	seen := map[string]bool{}
	var kinds []EventKind
	for len(kinds) < 3 {
		select {
		case e := <-published:
			if seen[e.ID] {
				t.Fatalf("duplicate %s", e.ID)
			}
			seen[e.ID] = true
			if strings.HasPrefix(e.Ticket, "TS-815") {
				kinds = append(kinds, e.Kind)
			}
		case <-ctx.Done():
			t.Fatalf("timeout; got %v", kinds)
		}
	}
	if kinds[0] != KindTurnCompleted || kinds[1] != KindContextHigh || kinds[2] != KindNeedsUser || connections.Load() != 2 {
		t.Fatalf("kinds %v after %d connections", kinds, connections.Load())
	}
	cancel()
	<-done
}

func TestEventIDAndText(t *testing.T) {
	if id := NewEventID(SourceT3, "run:1", "completed"); id != "t3:run:1:completed" {
		t.Fatal(id)
	}
	if id := NewEventID(SourceT3, strings.Repeat("x", 300)); len(id) != 43 || !validEventID(id, SourceT3) {
		t.Fatal(id)
	}
	if got := EventText("a\n\tb\x1b[2J  c ", 200); got != "a b [2J c" {
		t.Fatalf("%q", got)
	}
	if got := EventText("abcdef", 4); got != "abc…" {
		t.Fatalf("%q", got)
	}
}
