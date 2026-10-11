package cos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/coder/websocket"
)

// t3ShellValue is one item of T3's orchestration.subscribeShell stream: a
// (possibly partial) snapshot first, then a thread.updated per change.
type t3ShellValue struct {
	Kind     string `json:"kind"`
	Snapshot struct {
		Threads []t3ShellRow `json:"threads"`
	} `json:"snapshot"`
	Thread t3ShellRow `json:"thread"`
}

// t3ShellRow holds the live shell fields that events are derived from.
type t3ShellRow struct {
	ID                    string            `json:"id"`
	Title                 string            `json:"title"`
	Status                string            `json:"status"`
	Branch                string            `json:"branch"`
	LatestRunID           string            `json:"latestRunId"`
	LatestRunCompletedAt  string            `json:"latestRunCompletedAt"`
	LastError             json.RawMessage   `json:"lastError"` // string today; raw so a shape change cannot drop the row
	PendingRuntimeRequest *t3PendingRequest `json:"pendingRuntimeRequest"`
	ArchivedAt            *string           `json:"archivedAt"`
	DeletedAt             *string           `json:"deletedAt"`
	ModelSelection        struct {
		Model string `json:"model"`
	} `json:"modelSelection"`
	Lineage struct {
		Parent string `json:"parentThreadId"`
	} `json:"lineage"`
}

type t3PendingRequest struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	CreatedAt string `json:"createdAt"`
}

// t3SubscribeShell streams the local T3 shell until ctx ends or the connection fails.
// T3 streams with backpressure: every Chunk must be acknowledged before the next.
// shortcut: duplicates TS-812's T3.SubscribeShell (#15) so either merges first; switch to it once both are on main.
func t3SubscribeShell(ctx context.Context, c *T3, handle func(t3ShellValue) error) error {
	var ticket struct {
		Ticket string `json:"ticket"`
	}
	if err := c.HTTP(ctx, "POST", "/api/auth/websocket-ticket", map[string]any{}, &ticket); err != nil {
		return err
	}
	if ticket.Ticket == "" {
		return errors.New("T3 returned no websocket ticket")
	}
	wsURL := "ws" + strings.TrimPrefix(c.BaseURL, "http") + "/ws?orchestrationProtocol=2&wsTicket=" + url.QueryEscape(ticket.Ticket)
	ws, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPClient: c.Config.HTTP})
	if err != nil {
		return fmt.Errorf("T3 %s websocket connection failed", c.Host)
	}
	defer ws.CloseNow()
	ws.SetReadLimit(64 << 20)
	request, _ := json.Marshal(map[string]any{"_tag": "Request", "id": "1", "tag": "orchestration.subscribeShell", "payload": map[string]any{}, "headers": []any{}})
	if err := ws.Write(ctx, websocket.MessageText, request); err != nil {
		return err
	}
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			return fmt.Errorf("T3 %s shell stream: %w", c.Host, err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		scanner.Buffer(make([]byte, 4096), 64<<20)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			msgs := []json.RawMessage{line}
			if line[0] == '[' && json.Unmarshal(line, &msgs) != nil {
				return errors.New("T3 invalid frame")
			}
			for _, raw := range msgs {
				var m struct {
					Tag       string            `json:"_tag"`
					RequestID string            `json:"requestId"`
					Values    []json.RawMessage `json:"values"`
				}
				if err := json.Unmarshal(raw, &m); err != nil {
					return errors.New("T3 invalid frame")
				}
				switch {
				case m.Tag == "Ping":
					err = ws.Write(ctx, websocket.MessageText, []byte(`{"_tag":"Pong"}`))
				case m.RequestID != "1":
				case m.Tag == "Exit":
					return fmt.Errorf("T3 %s shell stream ended", c.Host)
				case m.Tag == "Chunk":
					for _, rawValue := range m.Values {
						var value t3ShellValue
						// Unknown or malformed values are skipped; the reconciler covers gaps.
						if json.Unmarshal(rawValue, &value) == nil {
							if err := handle(value); err != nil {
								return err
							}
						}
					}
					err = ws.Write(ctx, websocket.MessageText, []byte(`{"_tag":"Ack","requestId":"1"}`))
				}
				if err != nil {
					return err
				}
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
}
