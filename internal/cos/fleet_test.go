package cos

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestAsksKeepsHealthyThreadsAroundProjectionFailure(t *testing.T) {
	snapshot := `{"schemaVersion":2,"threads":[{"id":"before","title":"TS-1","status":"idle"},{"id":"deleted","title":"TS-2","status":"idle"},{"id":"after","title":"TS-3","status":"idle"}]}`
	visited := make(chan string, 3)
	server := newT3TestServer(t, snapshot, func(_ string, payload map[string]any) any {
		id := payload["threadId"].(string)
		visited <- id
		switch id {
		case "before":
			return map[string]any{"messages": []any{map[string]any{"role": "assistant", "text": "NEEDS-USER: Choose a window?"}}}
		case "deleted":
			return rpcTestFailure("PRIVATE-PROVIDER-TOKEN INTERNAL-DETAILS")
		default:
			return map[string]any{"runtimeRequests": []any{map[string]any{"id": "request-after", "status": "pending", "kind": "approval", "payload": map[string]any{"question": "Proceed?"}}}}
		}
	})
	defer server.Close()
	client := &T3{Config: Config{HTTP: server.Client()}, Host: "delta", BaseURL: server.URL}
	asks, err := client.Asks(t.Context())
	if err != nil || len(asks) != 3 {
		t.Fatalf("lost healthy questions: asks=%+v err=%v", asks, err)
	}
	if asks[0].ThreadID != "before" || asks[0].Question != "Choose a window?" || asks[1].ThreadID != "deleted" || asks[1].Error != "thread projection failed" || asks[2].ThreadID != "after" || asks[2].RequestID != "request-after" {
		t.Fatal(asks)
	}
	data, _ := json.Marshal(asks)
	if strings.Contains(string(data), "PRIVATE-PROVIDER-TOKEN") || strings.Contains(string(data), "INTERNAL-DETAILS") {
		t.Fatal("projection cause escaped in asks", string(data))
	}
	for _, want := range []string{"before", "deleted", "after"} {
		if got := <-visited; got != want {
			t.Fatalf("visited %s, want %s", got, want)
		}
	}
}

func TestAsksAbortsCollectionOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	snapshot := `{"schemaVersion":2,"threads":[{"id":"before","status":"idle"},{"id":"cancel","status":"idle"},{"id":"after","status":"idle"}]}`
	server := newT3TestServer(t, snapshot, func(_ string, payload map[string]any) any {
		if payload["threadId"] != "before" {
			t.Error("projection continued after cancellation", payload)
		}
		return map[string]any{"messages": []any{map[string]any{"role": "assistant", "text": "NEEDS-USER: Answer?"}}}
	})
	defer server.Close()
	tickets := 0
	transport := server.Client().Transport
	client := &T3{Host: "delta", BaseURL: server.URL, Config: Config{HTTP: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/api/auth/websocket-ticket" {
			tickets++
			if tickets == 2 {
				cancel()
				return nil, context.Canceled
			}
		}
		return transport.RoundTrip(req)
	})}}}
	asks, err := client.Asks(ctx)
	if asks != nil || !errors.Is(err, context.Canceled) || tickets != 2 {
		t.Fatalf("asks=%+v err=%v ticket calls=%d", asks, err, tickets)
	}
}

func TestParseAsks(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []Ask
	}{
		{
			name: "plain markers and trimmed options",
			text: "NEEDS-USER: Choose a host?\nOPTIONS: local | remote ||  \nWHY: Keep the work near its data.",
			want: []Ask{{Question: "Choose a host?", Options: []string{"local", "remote"}, Why: "Keep the work near its data."}},
		},
		{
			name: "bold label with colon outside",
			text: "**NEEDS-USER**: Ship now?\n**OPTIONS**: yes | wait\n**WHY**: Tests passed.",
			want: []Ask{{Question: "Ship now?", Options: []string{"yes", "wait"}, Why: "Tests passed."}},
		},
		{
			name: "bold label with colon inside and bullet",
			text: "  - **NEEDS-USER:** Ship now?  \n  - **OPTIONS:** yes | wait\n  - **WHY:** Tests passed.  ",
			want: []Ask{{Question: "Ship now?", Options: []string{"yes", "wait"}, Why: "Tests passed."}},
		},
		{
			name: "asterisk bullets and latest options replace earlier ones",
			text: "* NEEDS-USER: Which model?\n* OPTIONS: fast | precise\n* OPTIONS: cheap\n* WHY: Latency matters.",
			want: []Ask{{Question: "Which model?", Options: []string{"cheap"}, Why: "Latency matters."}},
		},
		{
			name: "options and why belong to the latest question",
			text: "OPTIONS: orphan\nWHY: orphan\nNEEDS-USER: First?\nOPTIONS: a | b\nWHY: first reason\nNEEDS-USER: Second?\nOPTIONS: c | d\nWHY: second reason",
			want: []Ask{
				{Question: "First?", Options: []string{"a", "b"}, Why: "first reason"},
				{Question: "Second?", Options: []string{"c", "d"}, Why: "second reason"},
			},
		},
		{
			name: "question without optional fields",
			text: "NEEDS-USER: Confirm?",
			want: []Ask{{Question: "Confirm?", Options: []string{}}},
		},
		{
			name: "prose and orphan metadata are ignored",
			text: "This mentions NEEDS-USER: without asking.\nOPTIONS: orphan\nWHY: orphan\nneeds-user: lowercase prose",
			want: []Ask{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseAsks(tt.text); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseAsks() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestContextOf(t *testing.T) {
	tests := []struct {
		name string
		json string
		want ContextUsage
	}{
		{
			name: "used then maximum",
			json: `{"providerTurns":[{"providerInstanceId":"codex","usage":{"usedTokens":125,"maxTokens":1000}}]}`,
			want: ContextUsage{Provider: "codex", UsedTokens: 125, MaxTokens: 1000, Pct: 13},
		},
		{
			name: "maximum then used",
			json: `{"providerTurns":[{"usage":{"maxTokens":1000,"usedTokens":125},"providerInstanceId":"codex"}]}`,
			want: ContextUsage{Provider: "codex", UsedTokens: 125, MaxTokens: 1000, Pct: 13},
		},
		{
			name: "latest provider turn wins",
			json: `{"providerTurns":[{"providerInstanceId":"old","usage":{"usedTokens":900,"maxTokens":1000}},{"providerInstanceId":"new","response":{"contextUsage":{"maxTokens":2000,"usedTokens":400}}}]}`,
			want: ContextUsage{Provider: "new", UsedTokens: 400, MaxTokens: 2000, Pct: 20},
		},
		{
			name: "latest turn without usage does not erase known usage",
			json: `{"providerTurns":[{"providerInstanceId":"codex","data":{"usage":{"usedTokens":40,"maxTokens":100}}},{"providerInstanceId":"new","result":{"other":1}}]}`,
			want: ContextUsage{Provider: "codex", UsedTokens: 40, MaxTokens: 100, Pct: 40},
		},
		{
			name: "native driver and latest nested usage",
			json: `{"providerTurns":[{"providerInstanceId":"custom-instance","nativeTurnRef":{"driver":"claude"},"result":[{"usage":{"usedTokens":10,"maxTokens":100}},{"usage":{"maxTokens":100,"usedTokens":80}}]}]}`,
			want: ContextUsage{Provider: "claude", UsedTokens: 80, MaxTokens: 100, Pct: 80},
		},
		{
			name: "zero maximum retains count without dividing",
			json: `{"providerTurns":[{"providerInstanceId":"codex","usage":{"maxTokens":0,"usedTokens":42}}]}`,
			want: ContextUsage{Provider: "codex", UsedTokens: 42, MaxTokens: 0, Pct: 0},
		},
		{
			name: "absent usage",
			json: `{"providerTurns":[{"providerInstanceId":"codex"}]}`,
			want: ContextUsage{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p Projection
			if err := json.Unmarshal([]byte(tt.json), &p); err != nil {
				t.Fatal(err)
			}
			if got := ContextOf(p); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ContextOf() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
