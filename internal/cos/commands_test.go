package cos

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
)

func TestLocalArgumentParsing(t *testing.T) {
	for name, spec := range localCommands {
		t.Run(name, func(t *testing.T) {
			args := make([]string, spec.Min)
			for i := range args {
				args[i] = "value"
			}
			if err := validateLocal(name, args); err != nil {
				t.Fatal(err)
			}
			if spec.Min > 0 {
				if err := validateLocal(name, args[:spec.Min-1]); err == nil {
					t.Fatal("accepted missing argument")
				}
			}
			for len(args) <= spec.Max {
				args = append(args, "value")
			}
			if err := validateLocal(name, args); err == nil {
				t.Fatal("accepted extra argument")
			}
		})
	}
	if err := validateLocal("send", []string{"id", "text", "queue"}); err != nil {
		t.Fatal(err)
	}
	if err := validateLocal("send", []string{"id", "text", "unknown"}); err == nil {
		t.Fatal("accepted invalid mode")
	}
	for _, n := range []string{"0", "51", "-1", "two"} {
		if err := validateLocal("thread", []string{"id", n}); err == nil {
			t.Fatal("accepted thread n", n)
		}
	}
	if err := validateLocal("thread", []string{"id", "50"}); err != nil {
		t.Fatal(err)
	}
	if _, err := objectJSON("[]"); err == nil {
		t.Fatal("accepted non-object")
	}
}

func TestLocalCommandPublishesOnlySafeLocalErrors(t *testing.T) {
	cfg := Config{Home: t.TempDir(), LocalHost: "test", HTTP: http.DefaultClient}
	_, err := LocalCommand(context.Background(), cfg, "projects", []string{"extra"})
	var public interface{ PublicMessage() string }
	if !errors.As(err, &public) || public.PublicMessage() != "usage: cos projects <host>" {
		t.Fatalf("validation error = %v, want safe public usage", err)
	}

	_, err = LocalCommand(context.Background(), cfg, "projects", nil)
	if err == nil || errors.As(err, &public) {
		t.Fatalf("missing-config error = %v, want opaque error", err)
	}

	token := "LOCAL-T3-TOKEN"
	path := filepath.Join(cfg.Home, ".config/cos/tokens", cfg.LocalHost)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("OTHER-PROVIDER-SECRET PRIVATE-BRIEF")), Header: make(http.Header)}, nil
	})}
	_, err = LocalCommand(context.Background(), cfg, "projects", nil)
	if err == nil || errors.As(err, &public) || strings.Contains(err.Error(), "OTHER-PROVIDER-SECRET") || strings.Contains(err.Error(), "PRIVATE-BRIEF") {
		t.Fatalf("upstream error = %v, want opaque error without response body", err)
	}
	cfg.HTTP = http.DefaultClient
	_, err = LocalCommand(context.Background(), cfg, "launch", []string{`{"brief":"PRIVATE-BRIEF","broken":`})
	if !errors.As(err, &public) || public.PublicMessage() != "invalid JSON object" || strings.Contains(err.Error(), "PRIVATE-BRIEF") {
		t.Fatalf("JSON validation error = %v, want safe public parser message", err)
	}
	_, err = LocalCommand(context.Background(), cfg, "watch", []string{"thread", "not-a-url PRIVATE-BRIEF"})
	if !errors.As(err, &public) || public.PublicMessage() != "not a PR URL" || strings.Contains(err.Error(), "PRIVATE-BRIEF") {
		t.Fatalf("PR URL error = %v, want safe public validation message", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestLaunchMissingProjectIsSafePublicError(t *testing.T) {
	server := newT3TestServer(t, `{"projects":[]}`, func(string, map[string]any) any { t.Error("unexpected RPC"); return nil })
	defer server.Close()
	client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL}
	_, err := client.Launch(context.Background(), map[string]any{"projectTitle": "PRIVATE-PROJECT-NAME"})
	var public interface{ PublicMessage() string }
	if !errors.As(err, &public) || public.PublicMessage() != "requested project was not found on test" || strings.Contains(err.Error(), "PRIVATE-PROJECT-NAME") {
		t.Fatalf("missing-project error = %v, want safe public domain message", err)
	}
}

func TestCloseTreeAndTidySkipActive(t *testing.T) {
	for _, command := range []string{"close", "delete", "tidy"} {
		t.Run(command, func(t *testing.T) {
			snapshot := `{"schemaVersion":2,"threads":[{"id":"root","title":"root","status":"idle"},{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"root"}},{"id":"active","title":"active","status":"running","lineage":{"parentThreadId":"root"}},{"id":"orphan","title":"orphan","status":"idle","lineage":{"parentThreadId":"absent"}}]}`
			if command == "tidy" {
				snapshot = `{"schemaVersion":2,"threads":[{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"absent"}},{"id":"active","title":"active","status":"running","lineage":{"parentThreadId":"absent"}}]}`
			}
			var got []string
			server := newT3TestServer(t, snapshot, func(tag string, payload map[string]any) any {
				if tag != "orchestration.dispatchCommand" {
					t.Errorf("unexpected RPC %s", tag)
				}
				got = append(got, payload["type"].(string)+":"+payload["threadId"].(string))
				return map[string]any{"ok": true}
			})
			defer server.Close()
			client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "fake"}
			var result TreeResult
			var err error
			if command == "tidy" {
				result, err = client.Tidy(context.Background())
			} else {
				result, err = client.CloseTree(context.Background(), "root", command == "delete")
			}
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"thread.settle:child", "thread.settle:root"}
			if command == "delete" {
				want = []string{"thread.settle:child", "thread.delete:child", "thread.settle:root", "thread.delete:root"}
			}
			if command == "tidy" {
				want = []string{"thread.settle:child"}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal(got, want)
			}
			if len(result.Skipped) != 1 || result.Skipped[0]["id"] != "active" {
				t.Fatal(result)
			}
		})
	}
}

func TestTidySettlesOrphanSubtreesSafely(t *testing.T) {
	tests := []struct {
		name, snapshot string
		failID         string
		wantClosed     []string
		wantSkipped    []map[string]string
		wantCalls      []string
	}{
		{
			name:       "reversed multi-level chain",
			snapshot:   `{"schemaVersion":2,"threads":[{"id":"grandchild","title":"grandchild","status":"idle","lineage":{"parentThreadId":"child"}},{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"parent"}},{"id":"parent","title":"parent","status":"idle","lineage":{"parentThreadId":"absent"}}]}`,
			wantClosed: []string{"parent", "child", "grandchild"},
			wantCalls:  []string{"parent", "child", "grandchild"},
		},
		{
			name:        "active middle blocks descendants",
			snapshot:    `{"schemaVersion":2,"threads":[{"id":"grandchild","title":"grandchild","status":"idle","lineage":{"parentThreadId":"middle"}},{"id":"middle","title":"middle","status":"running","lineage":{"parentThreadId":"absent"}}]}`,
			wantSkipped: []map[string]string{{"id": "middle", "reason": "active run"}},
		},
		{
			name:        "failed parent blocks descendants",
			snapshot:    `{"schemaVersion":2,"threads":[{"id":"grandchild","title":"grandchild","status":"idle","lineage":{"parentThreadId":"parent"}},{"id":"parent","title":"parent","status":"idle","lineage":{"parentThreadId":"absent"}}]}`,
			failID:      "parent",
			wantSkipped: []map[string]string{{"id": "parent", "reason": "settle failed"}},
			wantCalls:   []string{"parent"},
		},
		{
			name:     "orphan cycle is bounded",
			snapshot: `{"schemaVersion":2,"threads":[{"id":"a","title":"a","status":"idle","lineage":{"parentThreadId":"b"}},{"id":"b","title":"b","status":"idle","lineage":{"parentThreadId":"a"}}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls []string
			server := newT3TestServer(t, tt.snapshot, func(_ string, payload map[string]any) any {
				id := payload["threadId"].(string)
				calls = append(calls, id)
				if id == tt.failID {
					return rpcTestFailure("settle failed")
				}
				return map[string]any{"ok": true}
			})
			defer server.Close()
			client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "fake"}
			result, err := client.Tidy(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			wantClosed, wantSkipped, wantCalls := tt.wantClosed, tt.wantSkipped, tt.wantCalls
			if wantClosed == nil {
				wantClosed = []string{}
			}
			if wantSkipped == nil {
				wantSkipped = []map[string]string{}
			}
			if wantCalls == nil {
				wantCalls = []string{}
			}
			if calls == nil {
				calls = []string{}
			}
			if !reflect.DeepEqual(result.Closed, wantClosed) || !reflect.DeepEqual(result.Skipped, wantSkipped) || !reflect.DeepEqual(calls, wantCalls) {
				t.Fatalf("closed=%v skipped=%v calls=%v", result.Closed, result.Skipped, calls)
			}
			data, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), `"deleted"`) {
				t.Fatalf("tidy result includes deleted: %s", data)
			}
		})
	}
}

func newT3TestServer(t *testing.T, snapshot string, respond func(string, map[string]any) any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/orchestration/shell":
			_, _ = io.WriteString(w, snapshot)
		case "/api/projects":
			_, _ = io.WriteString(w, `{"projects":[]}`)
		case "/api/auth/websocket-ticket":
			_, _ = io.WriteString(w, `{"ticket":"test"}`)
		case "/ws":
			serveT3TestRPC(t, w, r, respond)
		default:
			http.NotFound(w, r)
		}
	}))
}

func serveT3TestRPC(t *testing.T, w http.ResponseWriter, r *http.Request, respond func(string, map[string]any) any) {
	t.Helper()
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer ws.CloseNow()
	_, data, err := ws.Read(r.Context())
	if err != nil {
		t.Error(err)
		return
	}
	var request struct {
		Tag     string         `json:"tag"`
		Payload map[string]any `json:"payload"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		t.Error(err)
		return
	}
	result := respond(request.Tag, request.Payload)
	if m, ok := result.(map[string]any); ok && request.Tag == "orchestration.getThreadProjection" {
		// Mocks name only the fields under test; T3 always sends the rest.
		defaults := map[string]any{"thread": map[string]any{"id": request.Payload["threadId"]}, "runs": []any{}, "messages": []any{}, "runtimeRequests": []any{}, "providerTurns": []any{}, "turnItems": []any{}}
		for k, v := range defaults {
			if _, ok := m[k]; !ok {
				m[k] = v
			}
		}
	}
	exit := map[string]any{"_tag": "Success", "value": result}
	if failure, ok := result.(rpcTestFailure); ok {
		exit = map[string]any{"_tag": "Failure", "cause": string(failure)}
	}
	body, err := json.Marshal(map[string]any{"_tag": "Exit", "requestId": "1", "exit": exit})
	if err != nil {
		t.Error(err)
		return
	}
	if err := ws.Write(r.Context(), websocket.MessageText, body); err != nil {
		t.Error(err)
	}
}

type rpcTestFailure string

func TestThreadCleanupFailuresUseFixedSkipReasons(t *testing.T) {
	const cause = "failed LOCAL-T3-TOKEN OTHER-PROVIDER-SECRET PRIVATE-BRIEF INTERNAL-FAILURE-CODE"
	tests := []struct {
		name, command, snapshot, wantReason string
		failType                            string
	}{
		{"close settle", "close", `{"schemaVersion":2,"threads":[{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"missing"}}]}`, "settle failed", "thread.settle"},
		{"delete", "delete", `{"schemaVersion":2,"threads":[{"id":"child","title":"child","status":"idle","settledAt":"now","lineage":{"parentThreadId":"missing"}}]}`, "delete failed", "thread.delete"},
		{"tidy settle", "tidy", `{"schemaVersion":2,"threads":[{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"missing"}}]}`, "settle failed", "thread.settle"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newT3TestServer(t, tt.snapshot, func(_ string, payload map[string]any) any {
				if payload["type"] == tt.failType {
					return rpcTestFailure(cause)
				}
				return map[string]any{"ok": true}
			})
			defer server.Close()
			client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "LOCAL-T3-TOKEN"}
			var result TreeResult
			var err error
			if tt.command == "tidy" {
				result, err = client.Tidy(context.Background())
			} else {
				result, err = client.CloseTree(context.Background(), "child", tt.command == "delete")
			}
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(result)
			if len(result.Skipped) != 1 || result.Skipped[0]["reason"] != tt.wantReason {
				t.Fatalf("result = %s", data)
			}
			for _, secret := range []string{"LOCAL-T3-TOKEN", "OTHER-PROVIDER-SECRET", "PRIVATE-BRIEF", "INTERNAL-FAILURE-CODE"} {
				if strings.Contains(string(data), secret) {
					t.Fatalf("result leaked %q: %s", secret, data)
				}
			}
			if strings.Contains(string(data), "[redacted]") || (tt.command == "delete" && !reflect.DeepEqual(*result.Deleted, []string{})) {
				t.Fatalf("result = %s", data)
			}
		})
	}
}

func TestEmptyThreadResultsKeepLegacyFields(t *testing.T) {
	server := newT3TestServer(t, `{"schemaVersion":2,"threads":[]}`, func(string, map[string]any) any { t.Error("unexpected mutation"); return nil })
	defer server.Close()
	client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL}
	for _, command := range []string{"close", "delete", "tidy"} {
		var result TreeResult
		var err error
		if command == "tidy" {
			result, err = client.Tidy(context.Background())
		} else {
			result, err = client.CloseTree(context.Background(), "absent", command == "delete")
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(result)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		if command == "tidy" {
			if fields["deleted"] != nil {
				t.Fatal(string(data))
			}
		} else if string(fields["deleted"]) != "[]" {
			t.Fatal(string(data))
		}
	}
}

func TestInventoryShape(t *testing.T) {
	tests := []struct{ name, snapshot, want string }{
		{
			name:     "nulls and title cut",
			snapshot: `{"schemaVersion":2,"threads":[{"id":"a","title":"` + strings.Repeat("界", 95) + `","status":"idle","settledAt":"s","archivedAt":"r","updatedAt":"2026-10-10T00:00:00Z","lineage":{"parentThreadId":"p"}},{"id":"b","title":"","status":"running"},{"id":"gone","title":"x","status":"idle","deletedAt":"d"}]}`,
			want:     `[{"id":"a","title":"` + strings.Repeat("界", 90) + `","status":"idle","settled":true,"archived":true,"parent":"p","updated":"2026-10-10T00:00:00Z"},{"id":"b","title":"","status":"running","settled":false,"archived":false,"parent":null,"updated":null}]`,
		},
		{name: "empty", snapshot: `{"schemaVersion":2,"threads":[]}`, want: `[]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newT3TestServer(t, tt.snapshot, func(string, map[string]any) any { t.Error("unexpected RPC"); return nil })
			defer server.Close()
			client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "fake"}
			got, err := client.Inventory(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != tt.want {
				t.Fatalf("inventory = %s", data)
			}
		})
	}
}

func TestPurgeKeepsActiveSubtreesAndOrder(t *testing.T) {
	snapshot := `{"schemaVersion":2,"threads":[
		{"id":"root","status":"idle","settledAt":"s"},
		{"id":"child","status":"idle","settledAt":"s","lineage":{"parentThreadId":"root"}},
		{"id":"grand","status":"idle","lineage":{"parentThreadId":"child"}},
		{"id":"busy","status":"running","settledAt":"s","lineage":{"parentThreadId":"root2"}},
		{"id":"keep-root","status":"idle","settledAt":"s"},
		{"id":"root2","status":"idle","settledAt":"s"},
		{"id":"open","status":"idle"},
		{"id":"open-child","status":"idle","settledAt":"s","lineage":{"parentThreadId":"open"}},
		{"id":"gone","status":"idle","settledAt":"s","deletedAt":"d"}]}`
	tests := []struct {
		name, failType         string
		wantCalls, wantSkipped []string
		wantDeleted            int
	}{
		{name: "clean", wantCalls: []string{"thread.settle:grand", "thread.delete:grand", "thread.delete:child", "thread.delete:root"}, wantDeleted: 3, wantSkipped: []string{"root2: active run in subtree"}},
		{name: "settle failure", failType: "thread.settle", wantCalls: []string{"thread.settle:grand", "thread.delete:child", "thread.delete:root"}, wantDeleted: 2, wantSkipped: []string{"grand: settle failed", "root2: active run in subtree"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls []string
			server := newT3TestServer(t, snapshot, func(_ string, payload map[string]any) any {
				calls = append(calls, payload["type"].(string)+":"+payload["threadId"].(string))
				if payload["type"] == tt.failType {
					return rpcTestFailure("failed LOCAL-T3-TOKEN")
				}
				return map[string]any{"ok": true}
			})
			defer server.Close()
			client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "fake"}
			result, err := client.Purge(context.Background(), []string{"keep-"})
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(result)
			if !reflect.DeepEqual(calls, tt.wantCalls) || result.Deleted != tt.wantDeleted || !reflect.DeepEqual(result.Skipped, tt.wantSkipped) {
				t.Fatalf("calls=%v result=%s", calls, data)
			}
			if strings.Contains(string(data), "LOCAL-T3-TOKEN") {
				t.Fatalf("result leaked failure text: %s", data)
			}
		})
	}
}

func TestPurgeChildrenOrdersDeepestFirst(t *testing.T) {
	snapshot := `{"schemaVersion":2,"threads":[
		{"id":"parent","status":"idle"},
		{"id":"done-child","status":"idle","lineage":{"parentThreadId":"parent"}},
		{"id":"done-grand","status":"idle","settledAt":"s","lineage":{"parentThreadId":"done-child"}},
		{"id":"live-parent","status":"running"},
		{"id":"run-child","status":"idle","lineage":{"parentThreadId":"live-parent"}},
		{"id":"run-grand","status":"idle","lineage":{"parentThreadId":"run-child"}},
		{"id":"busy-kid","status":"running","lineage":{"parentThreadId":"parent"}},
		{"id":"busy-kid-child","status":"idle","lineage":{"parentThreadId":"busy-kid"}}]}`
	var calls []string
	server := newT3TestServer(t, snapshot, func(_ string, payload map[string]any) any {
		calls = append(calls, payload["type"].(string)+":"+payload["threadId"].(string))
		return map[string]any{"ok": true}
	})
	defer server.Close()
	client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "fake"}
	result, err := client.PurgeChildren(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(result)
	want := []string{"thread.delete:done-grand", "thread.settle:done-child", "thread.delete:done-child"}
	if !reflect.DeepEqual(calls, want) || result.Deleted != 2 || string(data) != `{"deleted":2,"skipped":[]}` {
		t.Fatalf("calls=%v result=%s", calls, data)
	}
}

func TestStopInterruptsRunsAndReportsFixedFailures(t *testing.T) {
	snapshot := `{"schemaVersion":2,"threads":[
		{"id":"run-a","title":"Alpha","status":"running"},
		{"id":"queued-b","title":"","status":"queued"},
		{"id":"keep-c","title":"Keep","status":"running"},
		{"id":"idle-d","title":"Idle","status":"idle"},
		{"id":"fail-e","title":"Bad","status":"running"},
		{"id":"proj-f","title":"Proj","status":"running"},
		{"id":"gone","title":"Gone","status":"running","deletedAt":"d"}]}`
	projections := map[string]any{
		"run-a":    map[string]any{"runs": []any{map[string]any{"id": "r0", "status": "completed"}, map[string]any{"id": "r1", "status": "running"}}},
		"queued-b": map[string]any{"runs": []any{map[string]any{"id": "q1", "status": "queued"}}},
		"fail-e":   map[string]any{"runs": []any{map[string]any{"id": "f1", "status": "running"}}},
		"proj-f":   rpcTestFailure("failed LOCAL-T3-TOKEN"),
	}
	var dispatched []string
	server := newT3TestServer(t, snapshot, func(tag string, payload map[string]any) any {
		switch tag {
		case "orchestration.getThreadProjection":
			id := payload["threadId"].(string)
			projection, ok := projections[id]
			if !ok {
				t.Errorf("unexpected projection for %s", id)
			}
			return projection
		case "orchestration.dispatchCommand":
			delete(payload, "commandId")
			data, _ := json.Marshal(payload)
			dispatched = append(dispatched, string(data))
			if payload["threadId"] == "fail-e" {
				return rpcTestFailure("failed LOCAL-T3-TOKEN")
			}
			return map[string]any{"ok": true}
		}
		t.Errorf("unexpected RPC %s", tag)
		return nil
	})
	defer server.Close()
	client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "fake"}
	result, err := client.Stop(context.Background(), []string{"keep-"})
	if err != nil {
		t.Fatal(err)
	}
	wantDispatched := []string{
		`{"createdBy":"user","creationSource":"server","runId":"r1","threadId":"run-a","type":"run.interrupt"}`,
		`{"createdBy":"user","creationSource":"server","runId":"q1","threadId":"queued-b","type":"queued-run.cancel"}`,
		`{"createdBy":"user","creationSource":"server","runId":"f1","threadId":"fail-e","type":"run.interrupt"}`,
	}
	if !reflect.DeepEqual(dispatched, wantDispatched) {
		t.Fatal(dispatched)
	}
	data, _ := json.Marshal(result)
	if want := `{"stopped":["Alpha (running)","queued-b (queued)"],"failed":["Bad: interrupt failed","Proj: projection failed"]}`; string(data) != want {
		t.Fatalf("stop = %s", data)
	}
}

func TestThreadRepliesKeepOldestFirst(t *testing.T) {
	long := strings.Repeat("x", 3500)
	p := Projection{Messages: []ThreadMessage{{Role: "user", Text: "q"}, {Role: "assistant", Text: "one"}, {Role: "assistant", Text: "two"}, {Role: "assistant", Text: long}}}
	if got := lastReplies(p, 2); !reflect.DeepEqual(got, []string{"two", long}) {
		t.Fatalf("replies = %v", got)
	}
	if got := lastReplies(p, 50); !reflect.DeepEqual(got, []string{"one", "two", long}) {
		t.Fatalf("replies = %v", got)
	}
	if got := lastReplies(Projection{}, 1); got == nil || len(got) != 0 {
		t.Fatalf("replies = %#v, want empty non-nil", got)
	}
}

func TestCanceledDispatchDoesNotMutate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("request after cancellation") }))
	defer server.Close()
	client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Dispatch(ctx, map[string]any{"type": "thread.settle", "threadId": "id"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLaunchAcceptsOpusThread(t *testing.T) {
	var launches atomic.Int32
	server := newT3TestServer(t, `{"projects":[]}`, func(tag string, payload map[string]any) any {
		if tag == "orchestration.launchThread" {
			launches.Add(1)
		}
		return map[string]any{"threadId": "t", "resumed": false}
	})
	defer server.Close()
	client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL}
	input := map[string]any{"projectId": "p", "modelSelection": map[string]any{"instanceId": "claudeAgent", "model": "claude-opus-5-5", "options": []any{map[string]any{"id": "effort", "value": "high"}}}}
	if _, err := client.Launch(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if n := launches.Load(); n != 1 {
		t.Fatalf("launchThread requests = %d, want 1", n)
	}
}
