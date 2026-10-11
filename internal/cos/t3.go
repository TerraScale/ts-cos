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
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// shortcut: T3's WS RPC is internal and unversioned beyond protocol 2 / shell schemaVersion 2;
// decoding checks the fields COS relies on and returns T3SchemaError when they move.
// Re-record internal/cos/testdata/t3 after a T3 upgrade.
type T3 struct {
	Config               Config
	Host, BaseURL, token string
}

const t3ShellSchema = 2

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

// T3Error is a failed T3 RPC. It carries only T3's error type names: messages can quote briefs.
type T3Error struct {
	Host, Tag string
	Types     []string
}

func (e *T3Error) Error() string {
	if len(e.Types) == 0 {
		return fmt.Sprintf("T3 %s RPC failed: %s", e.Host, e.Tag)
	}
	return fmt.Sprintf("T3 %s RPC failed: %s: %s", e.Host, e.Tag, strings.Join(e.Types, " > "))
}
func (e *T3Error) PublicMessage() string { return e.Error() }

// NotFound reports T3's "no such thread/project" failures.
func (e *T3Error) NotFound() bool {
	return slices.ContainsFunc(e.Types, func(s string) bool { return strings.Contains(s, "NotFound") })
}

// T3SchemaError means T3 answered in a shape COS does not understand, usually after a T3 upgrade.
type T3SchemaError struct{ Host, Where, Detail string }

func (e *T3SchemaError) Error() string {
	return fmt.Sprintf("T3 schema changed on %s: %s: %s (T3 upgraded? re-record internal/cos/testdata/t3 and update the T3 client)", e.Host, e.Where, e.Detail)
}
func (e *T3SchemaError) PublicMessage() string { return e.Error() }

func (c *T3) schema(where, format string, args ...any) error {
	return &T3SchemaError{c.Host, where, fmt.Sprintf(format, args...)}
}

var t3TypeName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,79}$`)

// t3FailureTypes walks a failure cause and keeps the _tag/name identifiers only.
func t3FailureTypes(v any, out []string) []string {
	switch v := v.(type) {
	case []any:
		for _, x := range v {
			out = t3FailureTypes(x, out)
		}
	case map[string]any:
		for _, key := range []string{"_tag", "name"} {
			if s, ok := v[key].(string); ok && t3TypeName.MatchString(s) && s != "Fail" && s != "Die" && !slices.Contains(out, s) {
				out = append(out, s)
			}
		}
		for _, key := range []string{"error", "cause", "defect"} {
			out = t3FailureTypes(v[key], out)
		}
	}
	return out
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
		return OperationError(fmt.Sprintf("T3 %s %s: connection failed", c.Host, path))
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

// RPC calls one unary T3 RPC with a one-minute cap.
func (c *T3) RPC(ctx context.Context, tag string, payload, output any) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	return c.call(ctx, tag, payload, nil, output)
}

// Subscribe runs a streaming T3 RPC until ctx ends, T3 ends the stream, or onValue fails.
// T3 (Effect RPC) sends the next chunk only after the client acknowledges the previous one.
func (c *T3) Subscribe(ctx context.Context, tag string, payload any, onValue func(json.RawMessage) error) error {
	return c.call(ctx, tag, payload, onValue, nil)
}

func (c *T3) call(ctx context.Context, tag string, payload any, onValue func(json.RawMessage) error, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
		return OperationError(fmt.Sprintf("T3 %s websocket connection failed", c.Host))
	}
	defer ws.CloseNow()
	ws.SetReadLimit(64 << 20)
	data, err := json.Marshal(map[string]any{"_tag": "Request", "id": "1", "tag": tag, "payload": payload, "headers": []any{}})
	if err != nil {
		return err
	}
	if err := ws.Write(ctx, websocket.MessageText, data); err != nil {
		return err
	}
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
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
					Tag       string            `json:"_tag"`
					RequestID string            `json:"requestId"`
					Values    []json.RawMessage `json:"values"`
					Exit      struct {
						Tag   string          `json:"_tag"`
						Value json.RawMessage `json:"value"`
						Cause any             `json:"cause"`
					} `json:"exit"`
				}
				if err := json.Unmarshal(raw, &m); err != nil {
					return err
				}
				switch {
				case m.Tag == "Ping":
					if err := ws.Write(ctx, websocket.MessageText, []byte(`{"_tag":"Pong"}`)); err != nil {
						return err
					}
				case m.RequestID != "1":
				case m.Tag == "Chunk":
					for _, v := range m.Values {
						if onValue == nil {
							return c.schema(tag, "unexpected stream chunk from a unary RPC")
						}
						if err := onValue(v); err != nil {
							return err
						}
					}
					if err := ws.Write(ctx, websocket.MessageText, []byte(`{"_tag":"Ack","requestId":"1"}`)); err != nil {
						return err
					}
				case m.Tag == "Defect":
					return &T3Error{Host: c.Host, Tag: tag, Types: []string{"Defect"}}
				case m.Tag != "Exit":
				case m.Exit.Tag == "Failure":
					return &T3Error{Host: c.Host, Tag: tag, Types: t3FailureTypes(m.Exit.Cause, nil)}
				case m.Exit.Tag != "Success":
					return c.schema(tag, "unknown exit %q", m.Exit.Tag)
				case output == nil:
					return nil
				default:
					return json.Unmarshal(m.Exit.Value, output)
				}
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
	LatestRunID    string  `json:"latestRunId,omitempty"`
	LatestRunDone  string  `json:"latestRunCompletedAt,omitempty"`
	UpdatedAt      string  `json:"updatedAt,omitempty"`
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
	LastErrorClass    string           `json:"lastErrorClass"`
	UsageLimitResetAt string           `json:"usageLimitResetAt"`
	LimitRecovery     *T3LimitRecovery `json:"limitRecovery"`
}

// T3LimitRecovery is T3's armed auto-resume after a usage limit (UsageLimitRecoveryWorker).
type T3LimitRecovery struct {
	RunID      string `json:"runId"`
	ResetAt    string `json:"resetAt"`
	AutoResume bool   `json:"autoResume"`
}

// NeedsInput reports a pending question or approval.
func (t Thread) NeedsInput() bool {
	return len(t.PendingRuntimeRequest) > 0 && string(t.PendingRuntimeRequest) != "null"
}

// PendingRequest decodes the shell's pendingRuntimeRequest ({id, kind, createdAt}), or nil.
func (t Thread) PendingRequest() *RuntimeRequest {
	var r RuntimeRequest
	if !t.NeedsInput() || json.Unmarshal(t.PendingRuntimeRequest, &r) != nil || r.ID == "" {
		return nil
	}
	r.Status = "pending"
	return &r
}

type ShellSnapshot struct {
	SchemaVersion int       `json:"schemaVersion"`
	Sequence      int64     `json:"snapshotSequence"`
	Projects      []Project `json:"projects"`
	Threads       []Thread  `json:"threads"`
}
type ThreadMessage struct {
	ID        string `json:"id,omitempty"`
	RunID     string `json:"runId,omitempty"`
	Role      string `json:"role"`
	Text      string `json:"text"`
	Streaming bool   `json:"streaming,omitempty"`
}
type RuntimeRequest struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Kind      string `json:"kind"`
	CreatedAt string `json:"createdAt,omitempty"`
	Payload   any    `json:"payload,omitempty"`
	Request   any    `json:"request,omitempty"`
}

// TurnItem is one timeline item; user_input_request items carry the questions of a runtime request.
type TurnItem struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Status    string          `json:"status"`
	RequestID string          `json:"requestId,omitempty"`
	Questions json.RawMessage `json:"questions,omitempty"`
	Text      string          `json:"text,omitempty"`
}
type Projection struct {
	Thread          Thread           `json:"thread"`
	Runs            []ThreadRun      `json:"runs"`
	Messages        []ThreadMessage  `json:"messages"`
	ProviderTurns   []map[string]any `json:"providerTurns,omitempty"`
	RuntimeRequests []RuntimeRequest `json:"runtimeRequests"`
	TurnItems       []TurnItem       `json:"turnItems,omitempty"`
	Subagents       []any            `json:"subagents"`
}
type ThreadRun struct {
	ID            string  `json:"id"`
	Ordinal       int     `json:"ordinal,omitempty"`
	Status        string  `json:"status"`
	UserMessageID string  `json:"userMessageId,omitempty"`
	CreatedAt     string  `json:"createdAt"`
	StartedAt     *string `json:"startedAt"`
}

// decodeProjection checks the fields every COS command reads before decoding them.
func (c *T3) decodeProjection(where string, raw json.RawMessage) (Projection, error) {
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		return Projection{}, c.schema(where, "projection is not an object")
	}
	for _, k := range []string{"thread", "runs", "messages", "runtimeRequests", "providerTurns", "turnItems"} {
		if _, ok := keys[k]; !ok {
			return Projection{}, c.schema(where, "projection has no %q", k)
		}
	}
	var p Projection
	if err := json.Unmarshal(raw, &p); err != nil {
		return Projection{}, c.schema(where, "projection fields changed type: %v", err)
	}
	if p.Thread.ID == "" {
		return Projection{}, c.schema(where, "projection thread has no id")
	}
	return p, nil
}

func (c *T3) Projection(ctx context.Context, id string) (Projection, error) {
	var raw json.RawMessage
	if err := c.RPC(ctx, "orchestration.getThreadProjection", map[string]any{"threadId": id}, &raw); err != nil {
		return Projection{}, err
	}
	return c.decodeProjection("orchestration.getThreadProjection", raw)
}
func (c *T3) Snapshot(ctx context.Context) (ShellSnapshot, error) {
	var s ShellSnapshot
	if err := c.HTTP(ctx, "GET", "/api/orchestration/shell", nil, &s); err != nil {
		return s, err
	}
	if s.SchemaVersion != t3ShellSchema {
		return s, c.schema("/api/orchestration/shell", "schemaVersion %d, COS understands %d", s.SchemaVersion, t3ShellSchema)
	}
	return s, nil
}
func (p Projection) Run() (string, string) {
	if len(p.Runs) == 0 {
		return "", ""
	}
	r := p.Runs[len(p.Runs)-1]
	return r.ID, r.Status
}

// PendingRequests lists unanswered runtime requests with their questions as payload.
func (p Projection) PendingRequests() []map[string]any {
	out := []map[string]any{}
	for _, r := range p.RuntimeRequests {
		if r.Status != "pending" {
			continue
		}
		payload := r.Payload
		if payload == nil {
			payload = r.Request
		}
		for _, item := range p.TurnItems {
			if item.RequestID == r.ID && len(item.Questions) > 0 {
				payload = map[string]any{"questions": item.Questions}
			}
		}
		out = append(out, map[string]any{"requestId": r.ID, "kind": r.Kind, "payload": payload})
	}
	return out
}

type SendMode string

const (
	// SendSteer joins a running run (the default, user 2026-10-09); queued/preparing runs cannot be steered, so those queue.
	SendSteer SendMode = "steer"
	// SendQueue runs after the active run.
	SendQueue SendMode = "queue"
)

type SendResult struct {
	Host      string `json:"host"`
	ThreadID  string `json:"threadId"`
	MessageID string `json:"messageId"`
	Mode      string `json:"dispatchMode"`
}

func (c *T3) Send(ctx context.Context, id, text string, mode SendMode) (SendResult, error) {
	p, err := c.Projection(ctx, id)
	if err != nil {
		return SendResult{}, err
	}
	run, status := p.Run()
	dispatch := map[string]any{"type": "start_immediately"}
	switch {
	case mode == SendSteer && (status == "running" || status == "waiting"):
		dispatch = map[string]any{"type": "steer_active", "targetRunId": run}
	case active(status):
		dispatch["type"] = "queue_after_active"
	}
	messageID := uuid()
	_, err = c.Dispatch(ctx, map[string]any{"type": "message.dispatch", "createdBy": "user", "creationSource": "server", "threadId": id, "messageId": messageID, "text": text, "attachments": []any{}, "dispatchMode": dispatch})
	return SendResult{c.Host, id, messageID, dispatch["type"].(string)}, err
}

// Interrupt stops every active run of a thread: queued runs are cancelled, the others interrupted.
func (c *T3) Interrupt(ctx context.Context, id string) ([]string, error) {
	p, err := c.Projection(ctx, id)
	if err != nil {
		return nil, err
	}
	stopped := []string{}
	for _, r := range p.Runs {
		if !active(r.Status) {
			continue
		}
		command := map[string]any{"type": "run.interrupt", "threadId": id, "runId": r.ID, "createdBy": "user", "creationSource": "server"}
		if r.Status == "queued" {
			command["type"] = "queued-run.cancel"
		}
		if _, err := c.Dispatch(ctx, command); err != nil {
			return stopped, err
		}
		stopped = append(stopped, r.ID)
	}
	return stopped, nil
}

type CompactResult struct {
	Host     string       `json:"host"`
	ThreadID string       `json:"threadId"`
	Before   ContextUsage `json:"before"`
	After    ContextUsage `json:"after"`
}

// Compact waits for the thread to be idle, sends Claude Code's /compact with focus, and waits for it to finish.
func (c *T3) Compact(ctx context.Context, id, focus string) (CompactResult, error) {
	p, err := c.WaitIdle(ctx, id, 20*time.Minute)
	if err != nil {
		return CompactResult{}, err
	}
	out := CompactResult{Host: c.Host, ThreadID: id, Before: ContextOf(p)}
	sent, err := c.Send(ctx, id, strings.TrimSpace("/compact "+focus), SendQueue)
	if err != nil {
		return out, err
	}
	if p, err = c.WaitMessage(ctx, id, sent.MessageID, 15*time.Minute); err != nil {
		return out, err
	}
	out.After = ContextOf(p)
	return out, nil
}
