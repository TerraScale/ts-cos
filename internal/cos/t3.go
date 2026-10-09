package cos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// ponytail: T3's internal WS RPC is intentional parity; check it on T3 upgrades.
type T3 struct {
	Config               Config
	Host, BaseURL, token string
}

func OpenT3(ctx context.Context, cfg Config, host string) (*T3, error) {
	if !validHost(host) {
		return nil, fmt.Errorf("invalid host %q", host)
	}
	if host != cfg.LocalHost {
		return nil, fmt.Errorf("T3 is local only; use the %s NATS micro service", host)
	}
	token, err := os.ReadFile(filepath.Join(cfg.Home, ".config/cos/tokens", host))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(token)) == "" {
		return nil, fmt.Errorf("empty T3 token for %s", host)
	}
	c := &T3{Config: cfg, Host: host, token: strings.TrimSpace(string(token)), BaseURL: "http://127.0.0.1:3773"}
	return c, nil
}

func (c *T3) HTTP(ctx context.Context, method, path string, input, output any) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-t3-orchestration-protocol", "2")
	res, err := c.Config.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("T3 %s %s: connection failed", c.Host, path)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	res.Body.Close()
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("T3 %s %s: HTTP %d", c.Host, path, res.StatusCode)
	}
	return json.Unmarshal(body, output)
}

func (c *T3) RPC(ctx context.Context, tag string, payload, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
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
	data, err := json.Marshal(map[string]any{"_tag": "Request", "id": "1", "tag": tag, "payload": payload, "headers": []any{}})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ws.Write(ctx, websocket.MessageText, data); err != nil {
		return err
	}
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			return fmt.Errorf("T3 %s %s: %w", c.Host, tag, err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		scanner.Buffer(make([]byte, 4096), 64<<20)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			msgs := []json.RawMessage{}
			if line[0] == '[' {
				err = json.Unmarshal(line, &msgs)
			} else {
				msgs = append(msgs, json.RawMessage(line))
			}
			if err != nil {
				return fmt.Errorf("T3 invalid frame: %w", err)
			}
			for _, raw := range msgs {
				var m struct {
					Tag       string `json:"_tag"`
					RequestID string `json:"requestId"`
					Exit      struct {
						Tag   string          `json:"_tag"`
						Value json.RawMessage `json:"value"`
					} `json:"exit"`
				}
				if err := json.Unmarshal(raw, &m); err != nil {
					return err
				}
				if m.Tag == "Ping" {
					if err := ws.Write(ctx, websocket.MessageText, []byte(`{"_tag":"Pong"}`)); err != nil {
						return err
					}
				}
				if m.Tag != "Exit" || m.RequestID != "1" {
					continue
				}
				if m.Exit.Tag != "Success" {
					return fmt.Errorf("T3 %s RPC failed", c.Host)
				}
				if output == nil {
					return nil
				}
				return json.Unmarshal(m.Exit.Value, output)
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
}

func (c *T3) Dispatch(ctx context.Context, command map[string]any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := command["commandId"]; !ok {
		command["commandId"] = uuid()
	}
	var out any
	err := c.RPC(ctx, "orchestration.dispatchCommand", command, &out)
	return out, err
}

type Project struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Root  string `json:"root"`
}
type Thread struct {
	ID             string  `json:"id"`
	ProjectID      string  `json:"projectId"`
	Title          string  `json:"title"`
	Status         string  `json:"status"`
	Branch         string  `json:"branch"`
	WorktreePath   string  `json:"worktreePath"`
	ArchivedAt     *string `json:"archivedAt"`
	DeletedAt      *string `json:"deletedAt"`
	SettledAt      *string `json:"settledAt"`
	LastError      string  `json:"lastError"`
	ModelSelection struct {
		Model string `json:"model"`
	} `json:"modelSelection"`
	PendingRuntimeRequest json.RawMessage `json:"pendingRuntimeRequest"`
	Lineage               struct {
		Parent   string `json:"parentThreadId,omitempty"`
		Relation string `json:"relationshipToParent,omitempty"`
		Root     string `json:"rootThreadId,omitempty"`
	} `json:"lineage"`
	PullRequests []struct {
		URL      string `json:"url"`
		Snapshot struct {
			State        string `json:"state"`
			Mergeability string `json:"mergeability"`
		} `json:"snapshot"`
	} `json:"pullRequests"`
}
type ShellSnapshot struct {
	Projects []Project `json:"projects"`
	Threads  []Thread  `json:"threads"`
}
type Projection struct {
	Thread   Thread      `json:"thread"`
	Runs     []ThreadRun `json:"runs"`
	Messages []struct {
		Role string `json:"role"`
		Text string `json:"text"`
	} `json:"messages"`
	ProviderTurns   []map[string]any `json:"providerTurns"`
	RuntimeRequests []struct {
		ID      string `json:"id"`
		Status  string `json:"status"`
		Kind    string `json:"kind"`
		Payload any    `json:"payload"`
		Request any    `json:"request"`
	} `json:"runtimeRequests"`
	Subagents []any `json:"subagents"`
}
type ThreadRun struct {
	ID        string  `json:"id"`
	Status    string  `json:"status"`
	CreatedAt string  `json:"createdAt"`
	StartedAt *string `json:"startedAt"`
}

func (c *T3) Projection(ctx context.Context, id string) (Projection, error) {
	var p Projection
	err := c.RPC(ctx, "orchestration.getThreadProjection", map[string]any{"threadId": id}, &p)
	return p, err
}
func (c *T3) Snapshot(ctx context.Context) (ShellSnapshot, error) {
	var s ShellSnapshot
	err := c.HTTP(ctx, "GET", "/api/orchestration/shell", nil, &s)
	return s, err
}
func (p Projection) Run() (string, string) {
	if len(p.Runs) == 0 {
		return "", ""
	}
	r := p.Runs[len(p.Runs)-1]
	return r.ID, r.Status
}
func (c *T3) Send(ctx context.Context, id, text string, steer bool) (any, error) {
	p, err := c.Projection(ctx, id)
	if err != nil {
		return nil, err
	}
	run, status := p.Run()
	mode := map[string]any{"type": "start_immediately"}
	if active(status) {
		mode["type"] = "queue_after_active"
		if steer {
			mode["type"] = "steer_active"
			mode["targetRunId"] = run
		}
	}
	return c.Dispatch(ctx, map[string]any{"type": "message.dispatch", "createdBy": "user", "creationSource": "server", "threadId": id, "messageId": uuid(), "text": text, "attachments": []any{}, "dispatchMode": mode})
}
