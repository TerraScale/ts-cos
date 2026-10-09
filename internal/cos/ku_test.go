package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const kuTestToken = "ku-test-secret"

type kuTestRequest struct {
	Method string          `json:"method"`
	ID     json.RawMessage `json:"id"`
	Params json.RawMessage `json:"params"`
}

func kuTestClient(t *testing.T, handler func(http.ResponseWriter, *http.Request, kuTestRequest)) Ku {
	t.Helper()
	home := t.TempDir()
	dir := filepath.Join(home, ".config/cos")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ku-token"), []byte(kuTestToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+kuTestToken || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json, text/event-stream" {
			t.Errorf("incorrect MCP request headers or method: %s %v", r.Method, r.Header)
		}
		var request kuTestRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		handler(w, r, request)
	}))
	t.Cleanup(server.Close)
	return Ku{Config: Config{Home: home, KuURL: server.URL + "/mcp", HTTP: server.Client()}}
}

func kuTestResult(w http.ResponseWriter, request kuTestRequest, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
}

func TestKuCallHandshakeJSONAndSSE(t *testing.T) {
	for _, mode := range []string{"JSON structured", "JSON text", "SSE structured", "SSE multiline"} {
		t.Run(mode, func(t *testing.T) {
			methods := []string{}
			protocol := "2025-03-26"
			k := kuTestClient(t, func(w http.ResponseWriter, r *http.Request, request kuTestRequest) {
				methods = append(methods, request.Method)
				if request.Method != "initialize" {
					if r.Header.Get("Mcp-Session-Id") != "test-session" || r.Header.Get("MCP-Protocol-Version") != protocol {
						t.Error("session or negotiated protocol header missing")
					}
				}
				switch request.Method {
				case "initialize":
					if r.Header.Get("Mcp-Session-Id") != "" {
						t.Error("initialize must not reuse a session")
					}
					var params map[string]any
					_ = json.Unmarshal(request.Params, &params)
					if params["protocolVersion"] != kuProtocol || string(request.ID) != "1" {
						t.Error("invalid initialization request")
					}
					w.Header().Set("Mcp-Session-Id", "test-session")
					if strings.HasPrefix(mode, "SSE") {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":%q}}\n\n", protocol)
					} else {
						kuTestResult(w, request, map[string]any{"protocolVersion": protocol})
					}
				case "notifications/initialized":
					if len(request.ID) != 0 {
						t.Error("notification must have no request ID")
					}
					w.WriteHeader(http.StatusAccepted)
				case "tools/call":
					var params struct {
						Name      string         `json:"name"`
						Arguments map[string]any `json:"arguments"`
					}
					_ = json.Unmarshal(request.Params, &params)
					if params.Name != "get_issue" || params.Arguments["identifier"] != "TS-769" || string(request.ID) != "2" {
						t.Errorf("incorrect call parameters: %s", request.Params)
					}
					result := map[string]any{"structuredContent": map[string]any{"identifier": "TS-769"}}
					switch mode {
					case "JSON structured":
						kuTestResult(w, request, result)
					case "JSON text":
						kuTestResult(w, request, map[string]any{"content": []any{map[string]any{"type": "text", "text": "first"}, map[string]any{"type": "text", "text": "second"}}})
					case "SSE structured":
						w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
						_, _ = w.Write([]byte(": keepalive\r\n\r\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\r\n\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":99,\"result\":{}}\r\n\r\n"))
						_, _ = w.Write([]byte("event: message\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"structuredContent\":{\"identifier\":\"TS-769\"}}}\r\n\r\n"))
					case "SSE multiline":
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = w.Write([]byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\ndata: \"id\":\"2\",\ndata: \"result\":{\"structuredContent\":{\"identifier\":\"TS-769\"}}}"))
					}
				default:
					t.Errorf("unexpected method: %s", request.Method)
				}
			})
			result, err := k.Call(context.Background(), "get_issue", map[string]any{"identifier": "TS-769"})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "JSON text" {
				if result != "first\nsecond" {
					t.Errorf("incorrect text content: %v", result)
				}
			} else if result.(map[string]any)["identifier"] != "TS-769" {
				t.Errorf("incorrect structured content: %v", result)
			}
			if !reflect.DeepEqual(methods, []string{"initialize", "notifications/initialized", "tools/call"}) {
				t.Errorf("incorrect handshake order: %v", methods)
			}
		})
	}
}

func TestKuCallErrorsAndRedaction(t *testing.T) {
	for _, mode := range []string{"HTTP", "initialize RPC", "tool RPC", "tool isError", "SSE RPC", "SSE no matching response", "wrong ID", "malformed", "unsupported protocol", "notification HTTP"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			k := kuTestClient(t, func(w http.ResponseWriter, r *http.Request, request kuTestRequest) {
				if mode == "HTTP" || mode == "notification HTTP" && request.Method == "notifications/initialized" {
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte(r.Header.Get("Authorization")))
					return
				}
				if mode == "initialize RPC" && request.Method == "initialize" || mode == "tool RPC" && request.Method == "tools/call" {
					_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32602, "message": "bad token " + kuTestToken}})
					return
				}
				switch request.Method {
				case "initialize":
					protocol := kuProtocol
					if mode == "unsupported protocol" {
						protocol = "2099-01-01"
					}
					kuTestResult(w, request, map[string]any{"protocolVersion": protocol})
				case "notifications/initialized":
					w.WriteHeader(http.StatusAccepted)
				case "tools/call":
					calls++
					switch mode {
					case "tool isError":
						kuTestResult(w, request, map[string]any{"isError": true, "content": []any{map[string]any{"type": "text", "text": "bad token " + kuTestToken}}})
					case "SSE RPC":
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"error\":{\"code\":-1,\"message\":%q}}\n\n", kuTestToken)
					case "SSE no matching response":
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = w.Write([]byte("data: {\"jsonrpc\":\"2.0\",\"id\":99,\"result\":{}}\n\n"))
					case "wrong ID":
						request.ID = json.RawMessage("55")
						kuTestResult(w, request, map[string]any{})
					case "malformed":
						_, _ = w.Write([]byte("not JSON " + kuTestToken))
					default:
						kuTestResult(w, request, map[string]any{})
					}
				}
			})
			_, err := k.Call(context.Background(), "get_me", nil)
			if err == nil || strings.Contains(err.Error(), kuTestToken) {
				t.Fatalf("error not handled or token exposed: %v", err)
			}
			if (mode == "initialize RPC" || mode == "unsupported protocol" || mode == "notification HTTP") && calls != 0 {
				t.Error("tool called after failed initialization")
			}
		})
	}
}

func TestKuSSEReturnsWithoutWaitingForStreamClose(t *testing.T) {
	k := kuTestClient(t, func(w http.ResponseWriter, r *http.Request, request kuTestRequest) {
		switch request.Method {
		case "initialize":
			kuTestResult(w, request, map[string]any{"protocolVersion": kuProtocol})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/call":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"structuredContent\":{\"ok\":true}}}\n\n"))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	result, err := k.Call(ctx, "get_me", nil)
	if err != nil || result.(map[string]any)["ok"] != true || time.Since(start) > time.Second {
		t.Fatalf("waited for stream close: result=%v err=%v elapsed=%s", result, err, time.Since(start))
	}
}

func TestKuIssuesPaginationAndStrictOwnership(t *testing.T) {
	pages := []int{}
	k := kuTestClient(t, func(w http.ResponseWriter, r *http.Request, request kuTestRequest) {
		switch request.Method {
		case "initialize":
			kuTestResult(w, request, map[string]any{"protocolVersion": kuProtocol})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/call":
			var params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(request.Params, &params)
			if params.Name != "list_issues" || params.Arguments["workspace"] != "terrascale" || params.Arguments["assignee"] != kuOwnerEmail || params.Arguments["per_page"] != float64(100) {
				t.Errorf("incorrect ownership query: %s", request.Params)
			}
			page := int(params.Arguments["page"].(float64))
			pages = append(pages, page)
			issues := []map[string]any{}
			switch page {
			case 1:
				issues = append(issues,
					map[string]any{"identifier": "TS-foreign", "status": "todo", "assignees": []any{map[string]any{"email": "someone@terrascale.tech", "name": "mariogk"}}},
					map[string]any{"identifier": "TS-foreign-id", "status": "todo", "creator_id": kuOwnerID, "assignee_ids": []string{"foreign-user"}},
					map[string]any{"identifier": "TS-prefix", "status": "todo", "assignees": []any{map[string]any{"email": kuOwnerEmail + ".evil"}}},
				)
			case 2:
				issues = append(issues,
					map[string]any{"identifier": "TS-769", "title": "Ship COS", "description": "connector", "status": "in_progress", "status_name": "Doing", "status_category": "started", "priority": 1, "assignee_ids": []string{"foreign-user", kuOwnerID}},
					map[string]any{"identifier": "TS-completed", "status": "todo", "status_category": "completed", "assignee_ids": []string{kuOwnerID}},
					map[string]any{"identifier": "TS-cancelled", "status": "cancelled", "assignee_ids": []string{kuOwnerID}},
					map[string]any{"identifier": "TS-archived", "status": "todo", "archived_at": "2026-10-01T00:00:00Z", "assignee_ids": []string{kuOwnerID}},
				)
			case 3:
				issues = append(issues,
					map[string]any{"identifier": "TS-770", "status": "todo", "description": nil, "assignees": []any{map[string]any{"email": "someone@terrascale.tech"}, map[string]any{"email": "MARIOGK@TERRASCALE.TECH"}}},
					map[string]any{"identifier": "TS-done", "status": "done", "assignees": []any{map[string]any{"id": kuOwnerID}}},
					map[string]any{"identifier": "TS-custom-cancelled", "status": "todo", "status_category": "cancelled", "assignee_ids": []string{kuOwnerID}},
				)
			case 4:
				issues = append(issues,
					map[string]any{"identifier": "TS-771", "status": "backlog", "url": "https://ku.mariogk.com/terrascale/issue/TS-771", "assignees": []any{map[string]any{"id": kuOwnerID, "email": "changed@example.test"}}},
					map[string]any{"identifier": "TS-reopened", "status": "done", "status_name": "Reopened", "status_category": "started", "assignee_ids": []string{kuOwnerID}},
				)
			default:
				t.Errorf("unexpected page: %d", page)
			}
			response := map[string]any{"issues": issues, "page": page, "per_page": 2, "total": 8, "has_more": page < 4}
			if page == 3 {
				text, _ := json.Marshal(response)
				kuTestResult(w, request, map[string]any{"content": []any{map[string]any{"type": "text", "text": string(text)}}})
			} else {
				kuTestResult(w, request, map[string]any{"structuredContent": response})
			}
		}
	})
	issues, err := k.Issues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pages, []int{1, 2, 3, 4}) {
		t.Errorf("missing pages: %v", pages)
	}
	ids := []string{}
	for _, issue := range issues {
		ids = append(ids, issue.Identifier)
	}
	if !reflect.DeepEqual(ids, []string{"TS-769", "TS-770", "TS-771", "TS-reopened"}) {
		t.Fatalf("incorrect owned active issues: %+v", issues)
	}
	if issues[0].Status != "Doing" || issues[0].StatusType != "started" || issues[0].Priority != 1 || issues[0].Description != "connector" || !strings.HasSuffix(issues[0].URL, "/terrascale/issue/TS-769") || issues[1].StatusType != "unstarted" {
		t.Errorf("incorrect normalized issue: %+v", issues)
	}
}

func TestKuIssuesFailOnBadOrFailedPage(t *testing.T) {
	for _, mode := range []string{"tool error", "wrong page", "missing issues"} {
		t.Run(mode, func(t *testing.T) {
			k := kuTestClient(t, func(w http.ResponseWriter, r *http.Request, request kuTestRequest) {
				switch request.Method {
				case "initialize":
					kuTestResult(w, request, map[string]any{"protocolVersion": kuProtocol})
				case "notifications/initialized":
					w.WriteHeader(http.StatusAccepted)
				case "tools/call":
					var params struct {
						Arguments struct {
							Page int `json:"page"`
						} `json:"arguments"`
					}
					_ = json.Unmarshal(request.Params, &params)
					page := params.Arguments.Page
					if page == 2 && mode == "tool error" {
						kuTestResult(w, request, map[string]any{"isError": true, "content": []any{map[string]any{"type": "text", "text": "failed"}}})
						return
					}
					response := map[string]any{"issues": []any{}, "page": page, "per_page": 100, "has_more": page == 1}
					if page == 2 && mode == "wrong page" {
						response["page"] = 1
					}
					if page == 2 && mode == "missing issues" {
						delete(response, "issues")
					}
					kuTestResult(w, request, map[string]any{"structuredContent": response})
				}
			})
			issues, err := k.Issues(context.Background())
			if err == nil || issues != nil {
				t.Fatalf("accepted incomplete issue list: issues=%v err=%v", issues, err)
			}
		})
	}
}

func TestKuIssuesFailOnMissingOwnershipMetadata(t *testing.T) {
	for _, tt := range []struct {
		name, status string
		metadata     map[string]any
	}{
		{"omitted", "todo", nil},
		{"empty", "todo", map[string]any{"assignee_ids": []string{}, "assignees": []any{}}},
		{"null", "todo", map[string]any{"assignee_ids": nil, "assignees": nil}},
		{"completed", "completed", nil},
		{"cancelled", "cancelled", nil},
		{"archived", "archived", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			initial := true
			pages := []int{}
			missing := map[string]any{"identifier": "TS-769", "status": tt.status, "creator_id": kuOwnerID}
			for key, value := range tt.metadata {
				missing[key] = value
			}
			k := kuTestClient(t, func(w http.ResponseWriter, r *http.Request, request kuTestRequest) {
				switch request.Method {
				case "initialize":
					kuTestResult(w, request, map[string]any{"protocolVersion": kuProtocol})
				case "notifications/initialized":
					w.WriteHeader(http.StatusAccepted)
				case "tools/call":
					var params struct {
						Arguments struct {
							Page int `json:"page"`
						} `json:"arguments"`
					}
					_ = json.Unmarshal(request.Params, &params)
					page := params.Arguments.Page
					pages = append(pages, page)
					issues := []any{map[string]any{"identifier": "TS-769", "status": "todo", "assignee_ids": []string{kuOwnerID}}}
					if !initial {
						issues = []any{map[string]any{"identifier": "TS-770", "status": "todo", "assignee_ids": []string{kuOwnerID}}}
						if page == 2 {
							issues = append(issues, missing)
						} else if page != 1 {
							t.Errorf("requested page after missing ownership metadata: %d", page)
						}
					}
					response := map[string]any{"issues": issues, "page": page, "per_page": 100, "has_more": !initial && page <= 2}
					kuTestResult(w, request, map[string]any{"structuredContent": response})
				}
			})
			known, err := k.Issues(t.Context())
			if err != nil || len(known) != 1 || known[0].Identifier != "TS-769" {
				t.Fatalf("initial owned snapshot: issues=%v err=%v", known, err)
			}
			initial = false
			pages = nil
			issues, err := k.Issues(t.Context())
			if err == nil || issues != nil {
				t.Fatalf("accepted partial refresh of known owned issue: issues=%v err=%v", issues, err)
			}
			if !strings.Contains(err.Error(), "assignment metadata") || !strings.Contains(err.Error(), "page 2") {
				t.Errorf("missing page/ownership diagnostic: %v", err)
			}
			if !reflect.DeepEqual(pages, []int{1, 2}) {
				t.Errorf("incorrect pages after failed refresh: %v", pages)
			}
		})
	}
}
