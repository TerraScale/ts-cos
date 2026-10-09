package cos

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
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
	if err := validateLocal("send", []string{"id", "text", "steer"}); err != nil {
		t.Fatal(err)
	}
	if err := validateLocal("send", []string{"id", "text", "unknown"}); err == nil {
		t.Fatal("accepted invalid mode")
	}
	if _, err := objectJSON("[]"); err == nil {
		t.Fatal("accepted non-object")
	}
}

func TestCloseTreeAndTidySkipActive(t *testing.T) {
	for _, command := range []string{"close", "delete", "tidy"} {
		t.Run(command, func(t *testing.T) {
			snapshot := `{"threads":[{"id":"root","title":"root","status":"idle"},{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"root"}},{"id":"active","title":"active","status":"running","lineage":{"parentThreadId":"root"}},{"id":"orphan","title":"orphan","status":"idle","lineage":{"parentThreadId":"absent"}}]}`
			if command == "tidy" {
				snapshot = `{"threads":[{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"absent"}},{"id":"active","title":"active","status":"running","lineage":{"parentThreadId":"absent"}}]}`
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

func newT3TestServer(t *testing.T, snapshot string, respond func(string, map[string]any) any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/orchestration/shell":
			_, _ = io.WriteString(w, snapshot)
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

func TestPartialThreadFailuresNeverExposeToken(t *testing.T) {
	for _, command := range []string{"close", "tidy"} {
		t.Run(command, func(t *testing.T) {
			snapshot := `{"threads":[{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"missing"}}]}`
			server := newT3TestServer(t, snapshot, func(string, map[string]any) any { return rpcTestFailure("failed with credential PRIVATE-T3-TOKEN") })
			defer server.Close()
			client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "PRIVATE-T3-TOKEN"}
			var result TreeResult
			var err error
			if command == "tidy" {
				result, err = client.Tidy(context.Background())
			} else {
				result, err = client.CloseTree(context.Background(), "child", false)
			}
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(result)
			if len(result.Skipped) != 1 || strings.Contains(string(data), "PRIVATE-T3-TOKEN") || !strings.Contains(string(data), "[redacted]") {
				t.Fatal(string(data))
			}
		})
	}
}

func TestEmptyThreadResultsKeepLegacyFields(t *testing.T) {
	server := newT3TestServer(t, `{"threads":[]}`, func(string, map[string]any) any { t.Error("unexpected mutation"); return nil })
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
