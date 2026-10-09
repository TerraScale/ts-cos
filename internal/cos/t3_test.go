package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestT3RPCWireProtocol(t *testing.T) {
	type frame struct {
		text string
		pong bool
	}
	tests := []struct {
		name        string
		frames      []frame
		nilOutput   bool
		closeEarly  bool
		errContains string
	}{
		{
			name:   "single success",
			frames: []frame{{text: `{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":{"answer":"ok"}}}`}},
		},
		{
			name: "batch ignores unrelated exit and answers ping",
			frames: []frame{
				{text: `[{"_tag":"Exit","requestId":"other","exit":{"_tag":"Success","value":{"answer":"wrong"}}},{"_tag":"Chunk","requestId":"1","values":[]},{"_tag":"Ping"}]`, pong: true},
				{text: `[{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":{"answer":"ok"}}}]`},
			},
		},
		{
			name: "newline frames and blank lines",
			frames: []frame{
				{text: " \n{\"_tag\":\"Ping\"}\n\n{\"_tag\":\"Exit\",\"requestId\":\"other\",\"exit\":{\"_tag\":\"Failure\",\"cause\":\"ignore this\"}}\n", pong: true},
				{text: "\n  {\"_tag\":\"Exit\",\"requestId\":\"1\",\"exit\":{\"_tag\":\"Success\",\"value\":{\"answer\":\"ok\"}}}\n"},
			},
		},
		{
			name:      "success without output",
			frames:    []frame{{text: `{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":null}}`}},
			nilOutput: true,
		},
		{
			name:        "failure exit returns cause",
			frames:      []frame{{text: `{"_tag":"Exit","requestId":"1","exit":{"_tag":"Failure","cause":{"_tag":"Fail","error":"denied"}}}`}},
			errContains: "denied",
		},
		{
			name:        "malformed batch",
			frames:      []frame{{text: `[{"_tag":"Ping"}`}},
			errContains: "invalid frame",
		},
		{
			name:        "malformed object",
			frames:      []frame{{text: `{"_tag":`}},
			errContains: "unexpected end",
		},
		{
			name:        "success output cannot be decoded",
			frames:      []frame{{text: `{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":"wrong shape"}}`}},
			errContains: "cannot unmarshal string",
		},
		{
			name:        "close before matching exit",
			frames:      []frame{{text: `{"_tag":"Exit","requestId":"other","exit":{"_tag":"Success","value":{"answer":"wrong"}}}`}},
			closeEarly:  true,
			errContains: "T3 test test.rpc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const ticket = "ticket +/?&=value"
			serverResult := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/auth/websocket-ticket":
					if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer fake" || r.Header.Get("x-t3-orchestration-protocol") != "2" || r.Header.Get("Content-Type") != "application/json" {
						t.Error("ticket request method or authentication/protocol/content headers are incorrect")
						http.Error(w, "invalid ticket request", http.StatusBadRequest)
						return
					}
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body == nil || len(body) != 0 {
						t.Errorf("ticket request body = %#v, error = %v; want empty object", body, err)
						http.Error(w, "invalid body", http.StatusBadRequest)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]string{"ticket": ticket})
				case "/ws":
					serverResult <- func() error {
						if r.URL.Query().Get("wsTicket") != ticket || r.URL.Query().Get("orchestrationProtocol") != "2" {
							return fmt.Errorf("websocket query lost the ticket escaping or protocol version")
						}
						ws, err := websocket.Accept(w, r, nil)
						if err != nil {
							return err
						}
						defer ws.CloseNow()
						kind, data, err := ws.Read(ctx)
						if err != nil {
							return err
						}
						var request map[string]any
						if err := json.Unmarshal(data, &request); err != nil {
							return err
						}
						want := map[string]any{
							"_tag": "Request", "id": "1", "tag": "test.rpc",
							"payload": map[string]any{"threadId": "thread-1"}, "headers": []any{},
						}
						if kind != websocket.MessageText || !reflect.DeepEqual(request, want) {
							return fmt.Errorf("RPC request = %#v (%v), want %#v as text", request, kind, want)
						}
						for _, frame := range tt.frames {
							if err := ws.Write(ctx, websocket.MessageText, []byte(frame.text)); err != nil {
								return err
							}
							if frame.pong {
								kind, data, err := ws.Read(ctx)
								if err != nil {
									return err
								}
								var pong map[string]any
								if err := json.Unmarshal(data, &pong); err != nil {
									return err
								}
								if kind != websocket.MessageText || !reflect.DeepEqual(pong, map[string]any{"_tag": "Pong"}) {
									return fmt.Errorf("Ping response = %#v (%v), want text Pong", pong, kind)
								}
							}
						}
						if tt.closeEarly {
							return ws.Close(websocket.StatusInternalError, "test connection ended")
						}
						return nil
					}()
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "fake"}
			var out struct {
				Answer string `json:"answer"`
			}
			var output any = &out
			if tt.nilOutput {
				output = nil
			}
			err := client.RPC(ctx, "test.rpc", map[string]any{"threadId": "thread-1"}, output)
			if tt.errContains == "" {
				if err != nil {
					t.Errorf("RPC() error = %v", err)
				}
				if !tt.nilOutput && out.Answer != "ok" {
					t.Errorf("RPC() output = %#v, want answer ok", out)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("RPC() error = %v, want containing %q", err, tt.errContains)
			}
			if tt.errContains != "" && out.Answer != "" {
				t.Errorf("failed RPC accepted unrelated success output: %#v", out)
			}
			select {
			case err := <-serverResult:
				if err != nil {
					t.Errorf("protocol server: %v", err)
				}
			case <-ctx.Done():
				t.Error("websocket protocol server did not finish")
			}
		})
	}
}

func TestT3RPCConnectionFailures(t *testing.T) {
	tests := []struct {
		name          string
		ticketStatus  int
		ticketBody    string
		wantWebsocket bool
		errContains   string
	}{
		{"ticket endpoint failure", http.StatusUnauthorized, `{"error":"unauthorized"}`, false, "HTTP 401"},
		{"missing ticket", http.StatusOK, `{}`, false, "no websocket ticket"},
		{"invalid ticket JSON", http.StatusOK, `invalid`, false, "invalid character"},
		{"websocket upgrade failure", http.StatusOK, `{"ticket":"ticket"}`, true, "websocket connection failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			websocketCalled := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/ws" {
					websocketCalled <- struct{}{}
					http.Error(w, "upgrade rejected", http.StatusForbidden)
					return
				}
				if r.URL.Path != "/api/auth/websocket-ticket" {
					http.NotFound(w, r)
					return
				}
				w.WriteHeader(tt.ticketStatus)
				_, _ = fmt.Fprint(w, tt.ticketBody)
			}))
			defer server.Close()
			client := &T3{Config: Config{HTTP: server.Client()}, Host: "test", BaseURL: server.URL, token: "fake"}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := client.RPC(ctx, "test.rpc", map[string]any{}, nil)
			if err == nil || !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("RPC() error = %v, want containing %q", err, tt.errContains)
			}
			called := false
			select {
			case <-websocketCalled:
				called = true
			default:
			}
			if called != tt.wantWebsocket {
				t.Errorf("websocket attempted = %t, want %t", called, tt.wantWebsocket)
			}
		})
	}
}
