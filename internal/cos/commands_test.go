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
		{"close settle", "close", `{"threads":[{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"missing"}}]}`, "settle failed", "thread.settle"},
		{"delete", "delete", `{"threads":[{"id":"child","title":"child","status":"idle","settledAt":"now","lineage":{"parentThreadId":"missing"}}]}`, "delete failed", "thread.delete"},
		{"tidy settle", "tidy", `{"threads":[{"id":"child","title":"child","status":"idle","lineage":{"parentThreadId":"missing"}}]}`, "settle failed", "thread.settle"},
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
