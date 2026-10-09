package cos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const kuProtocol = "2025-06-18"
const kuOwnerEmail = "mariogk@terrascale.tech"
const kuOwnerID = "61acaf3f-3758-451d-880f-48e7d489db55"

type Ku struct{ Config Config }

type Issue struct {
	Identifier  string `json:"identifier"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Status      string `json:"status"`
	StatusType  string `json:"statusType"`
	URL         string `json:"url"`
	Priority    int    `json:"priority"`
}

type kuRPCMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func kuResponse(reader io.Reader, sse bool, id int) (json.RawMessage, error) {
	wantedID := fmt.Sprint(id)
	decode := func(data []byte) (json.RawMessage, bool, error) {
		var message kuRPCMessage
		if json.Unmarshal(data, &message) != nil || message.JSONRPC != "2.0" {
			return nil, false, fmt.Errorf("invalid MCP JSON-RPC response")
		}
		if string(message.ID) != wantedID && string(message.ID) != `"`+wantedID+`"` {
			return nil, false, nil
		}
		if message.Error != nil {
			return nil, true, fmt.Errorf("MCP JSON-RPC error %d: %s", message.Error.Code, message.Error.Message)
		}
		if len(message.Result) == 0 {
			return nil, true, fmt.Errorf("MCP response has no result")
		}
		return message.Result, true, nil
	}
	if !sse {
		var data json.RawMessage
		if json.NewDecoder(io.LimitReader(reader, 16<<20)).Decode(&data) != nil {
			return nil, fmt.Errorf("invalid MCP JSON response")
		}
		result, matched, err := decode(data)
		if err == nil && !matched {
			err = fmt.Errorf("MCP response ID does not match request")
		}
		return result, err
	}
	scanner := bufio.NewScanner(io.LimitReader(reader, 16<<20))
	scanner.Buffer(make([]byte, 4096), 16<<20)
	var data []string
	flush := func() (json.RawMessage, bool, error) {
		if len(data) == 0 {
			return nil, false, nil
		}
		result, matched, err := decode([]byte(strings.Join(data, "\n")))
		data = nil
		return result, matched, err
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			result, matched, err := flush()
			if matched || err != nil {
				return result, err
			}
		} else if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if scanner.Err() != nil {
		return nil, fmt.Errorf("could not read MCP event stream")
	}
	if result, matched, err := flush(); matched || err != nil {
		return result, err
	}
	return nil, fmt.Errorf("MCP event stream ended without a matching response")
}

func (k Ku) Call(ctx context.Context, tool string, args map[string]any) (result any, err error) {
	credential, err := os.ReadFile(filepath.Join(k.Config.Home, ".config/cos/ku-token"))
	if err != nil {
		return nil, fmt.Errorf("read Ku credentials: %w", err)
	}
	token := strings.TrimSpace(string(credential))
	if token == "" {
		return nil, fmt.Errorf("empty Ku token")
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("Ku: %s", strings.ReplaceAll(err.Error(), token, "[redacted]"))
		}
	}()
	base := k.Config.KuURL
	if base == "" {
		base = "https://ku.mariogk.com/mcp"
	}
	client := http.Client{Timeout: 60 * time.Second}
	if k.Config.HTTP != nil {
		client = *k.Config.HTTP
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	var session string
	protocol := kuProtocol
	call := func(method string, params any, id int) (json.RawMessage, error) {
		message := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
		if id != 0 {
			message["id"] = id
		}
		data, err := json.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("invalid MCP request parameters")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base, bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("invalid Ku URL")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if method != "initialize" {
			req.Header.Set("MCP-Protocol-Version", protocol)
		}
		if session != "" {
			req.Header.Set("Mcp-Session-Id", session)
		}
		res, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("%s: connection failed", method)
		}
		defer res.Body.Close()
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("%s: HTTP %d", method, res.StatusCode)
		}
		if session == "" {
			session = res.Header.Get("Mcp-Session-Id")
		}
		if id == 0 {
			return nil, nil
		}
		raw, err := kuResponse(res.Body, strings.HasPrefix(strings.ToLower(res.Header.Get("Content-Type")), "text/event-stream"), id)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", method, err)
		}
		return raw, nil
	}
	initialized, err := call("initialize", map[string]any{
		"protocolVersion": kuProtocol, "capabilities": map[string]any{},
		"clientInfo": map[string]any{"name": "cos", "version": "1"},
	}, 1)
	if err != nil {
		return nil, err
	}
	var initialization struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if json.Unmarshal(initialized, &initialization) != nil {
		return nil, fmt.Errorf("invalid MCP initialize result")
	}
	switch initialization.ProtocolVersion {
	case kuProtocol, "2025-03-26", "2024-11-05":
		protocol = initialization.ProtocolVersion
	default:
		return nil, fmt.Errorf("unsupported MCP protocol version")
	}
	if _, err := call("notifications/initialized", map[string]any{}, 0); err != nil {
		return nil, err
	}
	if args == nil {
		args = map[string]any{}
	}
	raw, err := call("tools/call", map[string]any{"name": tool, "arguments": args}, 2)
	if err != nil {
		return nil, err
	}
	var response struct {
		IsError           bool            `json:"isError"`
		StructuredContent json.RawMessage `json:"structuredContent"`
		Content           []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(raw, &response) != nil || string(raw) == "null" {
		return nil, fmt.Errorf("invalid MCP tool result")
	}
	text := []string{}
	for _, content := range response.Content {
		if content.Type == "text" {
			text = append(text, content.Text)
		}
	}
	if response.IsError {
		return nil, fmt.Errorf("tool %s: %s", tool, strings.Join(text, " "))
	}
	if len(response.StructuredContent) != 0 && string(response.StructuredContent) != "null" {
		if json.Unmarshal(response.StructuredContent, &result) != nil {
			return nil, fmt.Errorf("invalid MCP structured content")
		}
		return result, nil
	}
	return strings.Join(text, "\n"), nil
}

type kuIssue struct {
	Identifier     string   `json:"identifier"`
	Title          string   `json:"title"`
	Description    string   `json:"description"`
	Status         string   `json:"status"`
	StatusName     string   `json:"status_name"`
	StatusCategory string   `json:"status_category"`
	Priority       int      `json:"priority"`
	URL            string   `json:"url"`
	ArchivedAt     *string  `json:"archived_at"`
	AssigneeIDs    []string `json:"assignee_ids"`
	Assignees      []struct {
		ID    string `json:"id"`
		Email string `json:"email"`
	} `json:"assignees"`
}

func (i kuIssue) owned() bool {
	for _, id := range i.AssigneeIDs {
		if id == kuOwnerID {
			return true
		}
	}
	for _, user := range i.Assignees {
		if user.ID == kuOwnerID || strings.EqualFold(user.Email, kuOwnerEmail) {
			return true
		}
	}
	return false
}

func kuStatusType(status string) string {
	switch strings.ToLower(status) {
	case "backlog":
		return "backlog"
	case "todo", "unstarted":
		return "unstarted"
	case "in_progress", "in_review", "started":
		return "started"
	case "done", "completed":
		return "completed"
	case "cancelled", "canceled":
		return "cancelled"
	case "archived":
		return "archived"
	}
	return ""
}

func (k Ku) Issues(ctx context.Context) ([]Issue, error) {
	out := []Issue{}
	for page := 1; ; page++ {
		result, err := k.Call(ctx, "list_issues", map[string]any{
			"workspace": "terrascale", "assignee": kuOwnerEmail, "page": page, "per_page": 100,
		})
		if err != nil {
			return nil, err
		}
		var data []byte
		if text, ok := result.(string); ok {
			data = []byte(text)
		} else {
			data, err = json.Marshal(result)
			if err != nil {
				return nil, fmt.Errorf("invalid Ku issue result")
			}
		}
		var response struct {
			Issues  []kuIssue `json:"issues"`
			Page    int       `json:"page"`
			PerPage int       `json:"per_page"`
			Total   int       `json:"total"`
			HasMore *bool     `json:"has_more"`
		}
		if json.Unmarshal(data, &response) != nil || response.Issues == nil || response.Page != page || response.PerPage <= 0 {
			return nil, fmt.Errorf("invalid Ku list_issues page %d", page)
		}
		for _, raw := range response.Issues {
			if len(raw.AssigneeIDs) == 0 && len(raw.Assignees) == 0 {
				return nil, fmt.Errorf("Ku list_issues page %d is missing assignment metadata", page)
			}
			category := strings.ToLower(raw.StatusCategory)
			if category == "" {
				category = kuStatusType(raw.Status)
			}
			if !raw.owned() || raw.ArchivedAt != nil || category == "completed" || category == "cancelled" || category == "archived" || strings.EqualFold(raw.Status, "archived") {
				continue
			}
			status := raw.StatusName
			if status == "" {
				status = raw.Status
			}
			issueURL := raw.URL
			if issueURL == "" {
				endpoint := k.Config.KuURL
				if endpoint == "" {
					endpoint = "https://ku.mariogk.com/mcp"
				}
				base, err := url.Parse(endpoint)
				if err != nil {
					return nil, fmt.Errorf("invalid Ku URL")
				}
				issueURL = base.Scheme + "://" + base.Host + "/terrascale/issue/" + url.PathEscape(raw.Identifier)
			}
			out = append(out, Issue{Identifier: raw.Identifier, Title: raw.Title, Description: raw.Description,
				Status: status, StatusType: category, Priority: raw.Priority, URL: issueURL})
		}
		more := page*response.PerPage < response.Total
		if response.HasMore != nil {
			more = more || *response.HasMore
		}
		if !more {
			return out, nil
		}
	}
}
