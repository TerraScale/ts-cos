package cos

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type commandSpec struct {
	Min, Max int
	Usage    string
}

var localCommands = map[string]commandSpec{
	"projects": {0, 0, "projects <host>"}, "shell": {0, 0, "shell <host>"}, "asks-host": {0, 0, "asks-host <host>"}, "tidy": {0, 0, "tidy <host>"},
	"launch": {1, 1, "launch <host> '<json>'"}, "dispatch": {1, 1, "dispatch <host> '<json>'"},
	"thread": {1, 1, "thread <host> <threadId>"}, "ctx": {1, 1, "ctx <host> <threadId>"}, "settle": {1, 1, "settle <host> <threadId>"}, "archive": {1, 1, "archive <host> <threadId>"}, "close": {1, 1, "close <host> <threadId>"}, "delete": {1, 1, "delete <host> <threadId>"},
	"send": {2, 3, "send <host> <threadId> '<text>' [steer]"}, "fork": {1, 2, "fork <host> <threadId> ['<title>']"}, "watch": {2, 2, "watch <host> <threadId> <prUrl>"}, "model": {2, 2, "model <host> <threadId> '<modelSelection json>'"}, "rename": {2, 2, "rename <host> <threadId> '<title>'"},
	"handoff": {2, 3, "handoff <host> <threadId> '<modelSelection json>' ['<brief>']"}, "respond": {3, 3, "respond <host> <threadId> <requestId> '<answers json>'"},
}

func validateLocal(command string, args []string) error {
	spec, ok := localCommands[command]
	if !ok {
		return fmt.Errorf("unknown command %q", command)
	}
	if len(args) < spec.Min || len(args) > spec.Max {
		return fmt.Errorf("usage: cos %s", spec.Usage)
	}
	for _, a := range args {
		if strings.TrimSpace(a) == "" {
			return errors.New("command arguments cannot be empty")
		}
	}
	if command == "send" && len(args) == 3 && args[2] != "steer" {
		return errors.New("send mode must be steer")
	}
	return nil
}

type OperationError string

func (e OperationError) Error() string         { return string(e) }
func (e OperationError) PublicMessage() string { return string(e) }

func LocalCommand(ctx context.Context, cfg Config, command string, args []string) (out any, opErr error) {
	if command == "asks" {
		command = "asks-host"
	}
	defer func() {
		if opErr != nil {
			message := opErr.Error()
			token, err := os.ReadFile(filepath.Join(cfg.Home, ".config/cos/tokens", cfg.LocalHost))
			if err == nil && strings.TrimSpace(string(token)) != "" {
				message = strings.ReplaceAll(message, strings.TrimSpace(string(token)), "[redacted]")
			}
			opErr = OperationError(message)
		}
	}()
	if err := validateLocal(command, args); err != nil {
		return nil, err
	}
	c, err := OpenT3(ctx, cfg, cfg.LocalHost)
	if err != nil {
		return nil, err
	}
	if command == "shell" {
		return c.Shell(ctx)
	}
	if command == "asks-host" {
		a, err := c.Asks(ctx)
		return map[string]any{"host": c.Host, "asks": a}, err
	}
	if command == "projects" {
		return c.Projects(ctx)
	}
	if command == "launch" {
		input, err := objectJSON(args[0])
		if err != nil {
			return nil, err
		}
		return c.Launch(ctx, input)
	}
	if command == "dispatch" {
		input, err := objectJSON(args[0])
		if err != nil {
			return nil, err
		}
		return c.Dispatch(ctx, input)
	}
	if command == "tidy" {
		return c.Tidy(ctx)
	}
	id := args[0]
	switch command {
	case "thread", "ctx":
		p, err := c.Projection(ctx, id)
		if err != nil {
			return nil, err
		}
		if command == "ctx" {
			return ContextOf(p), nil
		}
		_, status := p.Run()
		if status == "" {
			status = "idle"
		}
		var reply any
		for _, m := range p.Messages {
			if m.Role == "assistant" {
				r := []rune(m.Text)
				if len(r) > 3000 {
					r = r[len(r)-3000:]
				}
				reply = string(r)
			}
		}
		pending := []any{}
		for _, r := range p.RuntimeRequests {
			if r.Status == "pending" {
				payload := r.Payload
				if payload == nil {
					payload = r.Request
				}
				pending = append(pending, map[string]any{"requestId": r.ID, "kind": r.Kind, "payload": payload})
			}
		}
		runs := p.Runs
		if len(runs) > 3 {
			runs = runs[len(runs)-3:]
		}
		if runs == nil {
			runs = []ThreadRun{}
		}
		return map[string]any{"host": c.Host, "title": p.Thread.Title, "branch": p.Thread.Branch, "worktreePath": p.Thread.WorktreePath, "archived": p.Thread.ArchivedAt != nil, "settled": p.Thread.SettledAt != nil, "deleted": p.Thread.DeletedAt != nil, "runStatus": status, "runs": runs, "pendingRequests": pending, "lineage": p.Thread.Lineage, "subagents": len(p.Subagents), "lastReply": reply}, nil
	case "send":
		return c.Send(ctx, id, args[1], len(args) == 3)
	case "close", "delete":
		return c.CloseTree(ctx, id, command == "delete")
	case "handoff":
		sel, err := objectJSON(args[1])
		if err != nil {
			return nil, err
		}
		brief := ""
		if len(args) == 3 {
			brief = args[2]
		}
		return c.Handoff(ctx, id, sel, brief)
	case "fork":
		p, err := c.Projection(ctx, id)
		if err != nil {
			return nil, err
		}
		target := "thread:project:" + p.Thread.ProjectID + ":" + uuid()
		cmd := map[string]any{"type": "thread.fork", "createdBy": "user", "creationSource": "server", "sourceThreadId": id, "targetThreadId": target, "sourcePoint": map[string]any{"type": "latest_stable"}}
		if len(args) == 2 {
			cmd["title"] = args[1]
		}
		if _, err := c.Dispatch(ctx, cmd); err != nil {
			return nil, err
		}
		return map[string]any{"host": c.Host, "threadId": target, "forkedFrom": id}, nil
	case "watch":
		u, err := url.Parse(args[1])
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil {
			return nil, errors.New("not a PR URL")
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(parts) != 4 || parts[2] != "pulls" {
			return nil, errors.New("not a PR URL")
		}
		number, err := strconv.Atoi(parts[3])
		if err != nil || number < 1 {
			return nil, errors.New("not a PR URL")
		}
		return c.Dispatch(ctx, map[string]any{"type": "thread.pull-request.watch", "threadId": id, "host": u.Host, "repository": strings.ToLower(parts[0] + "/" + parts[1]), "number": number, "watching": true, "link": map[string]any{"url": args[1], "source": "manual"}})
	case "model":
		sel, err := objectJSON(args[1])
		if err != nil {
			return nil, err
		}
		return c.Dispatch(ctx, map[string]any{"type": "thread.model-selection.set", "threadId": id, "modelSelection": sel})
	case "respond":
		answers, err := objectJSON(args[2])
		if err != nil {
			return nil, err
		}
		return c.Dispatch(ctx, map[string]any{"type": "runtime-request.respond", "threadId": id, "requestId": args[1], "answers": answers})
	case "rename":
		return c.Dispatch(ctx, map[string]any{"type": "thread.metadata.update", "threadId": id, "title": args[1]})
	case "settle", "archive":
		return c.Dispatch(ctx, map[string]any{"type": "thread." + command, "threadId": id})
	}
	return nil, fmt.Errorf("unknown command %q", command)
}

func (c *T3) Projects(ctx context.Context) ([]Project, error) {
	var raw struct {
		Projects []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
			Root  string `json:"workspaceRoot"`
		} `json:"projects"`
	}
	if err := c.HTTP(ctx, "GET", "/api/projects", nil, &raw); err != nil {
		return nil, err
	}
	out := []Project{}
	for _, p := range raw.Projects {
		out = append(out, Project{p.ID, p.Title, p.Root})
	}
	return out, nil
}
func (c *T3) Launch(ctx context.Context, input map[string]any) (any, error) {
	if input["projectId"] == nil || input["projectId"] == "" {
		ps, err := c.Projects(ctx)
		if err != nil {
			return nil, err
		}
		for _, p := range ps {
			if p.Title == input["projectTitle"] {
				input["projectId"] = p.ID
			}
		}
		if input["projectId"] == nil || input["projectId"] == "" {
			return nil, fmt.Errorf("no project titled %v on %s", input["projectTitle"], c.Host)
		}
	}
	payload := map[string]any{"commandId": uuid(), "creationSource": "server", "projectId": input["projectId"], "title": input["title"], "modelSelection": input["modelSelection"], "runtimeMode": "full-access", "interactionMode": "default", "workspaceStrategy": map[string]any{"type": "root"}}
	for _, k := range []string{"runtimeMode", "interactionMode", "workspaceStrategy"} {
		if input[k] != nil {
			payload[k] = input[k]
		}
	}
	if text, ok := input["message"].(string); ok && text != "" {
		payload["initialMessage"] = map[string]any{"text": text, "attachments": []any{}}
	}
	var r struct {
		ThreadID string `json:"threadId"`
		Resumed  bool   `json:"resumed"`
	}
	if err := c.RPC(ctx, "orchestration.launchThread", payload, &r); err != nil {
		return nil, err
	}
	return map[string]any{"host": c.Host, "threadId": r.ThreadID, "resumed": r.Resumed}, nil
}

type TreeResult struct {
	Closed  []string            `json:"closed"`
	Deleted *[]string           `json:"deleted,omitempty"`
	Skipped []map[string]string `json:"skipped"`
}

func (c *T3) CloseTree(ctx context.Context, id string, deleteThread bool) (TreeResult, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return TreeResult{}, err
	}
	byID := map[string]Thread{}
	children := map[string][]string{}
	for _, t := range s.Threads {
		if t.DeletedAt == nil {
			byID[t.ID] = t
			children[t.Lineage.Parent] = append(children[t.Lineage.Parent], t.ID)
		}
	}
	deleted := []string{}
	out := TreeResult{Closed: []string{}, Deleted: &deleted, Skipped: []map[string]string{}}
	tree := []string{}
	seen := map[string]bool{}
	var walk func(string)
	walk = func(id string) {
		if seen[id] {
			return
		}
		seen[id] = true
		for _, child := range children[id] {
			walk(child)
		}
		tree = append(tree, id)
	}
	walk(id)
	for _, id := range tree {
		t, ok := byID[id]
		if !ok {
			continue
		}
		if active(t.Status) {
			out.Skipped = append(out.Skipped, map[string]string{"id": id, "reason": "active run"})
			continue
		}
		if t.SettledAt == nil {
			if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.settle", "threadId": id}); err != nil {
				out.Skipped = append(out.Skipped, map[string]string{"id": id, "reason": err.Error()})
				continue
			}
		}
		out.Closed = append(out.Closed, id)
		if deleteThread {
			if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.delete", "threadId": id}); err != nil {
				out.Skipped = append(out.Skipped, map[string]string{"id": id, "reason": err.Error()})
			} else {
				*out.Deleted = append(*out.Deleted, id)
			}
		}
	}
	return out, nil
}
func (c *T3) Tidy(ctx context.Context) (TreeResult, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return TreeResult{}, err
	}
	byID := map[string]Thread{}
	for _, t := range s.Threads {
		byID[t.ID] = t
	}
	out := TreeResult{Closed: []string{}, Skipped: []map[string]string{}}
	for _, t := range s.Threads {
		if t.SettledAt != nil || t.ArchivedAt != nil || t.DeletedAt != nil || t.Lineage.Parent == "" {
			continue
		}
		p, ok := byID[t.Lineage.Parent]
		if ok && p.SettledAt == nil && p.ArchivedAt == nil && p.DeletedAt == nil {
			continue
		}
		if active(t.Status) {
			out.Skipped = append(out.Skipped, map[string]string{"id": t.ID, "reason": "active run"})
			continue
		}
		if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.settle", "threadId": t.ID}); err != nil {
			out.Skipped = append(out.Skipped, map[string]string{"id": t.ID, "reason": err.Error()})
		} else {
			out.Closed = append(out.Closed, t.Title)
		}
	}
	return out, nil
}
func (c *T3) Handoff(ctx context.Context, id string, sel map[string]any, brief string) (any, error) {
	window := 200000
	for _, key := range []string{"instanceId", "model"} {
		switch sel[key] {
		case "codex":
			window = 258400
		case "claudeAgent":
			window = 1000000
		case "claude-haiku-5-5":
			window = 200000
		}
	}
	budget := window / 2
	p, err := c.WaitIdle(ctx, id, 20*time.Minute)
	if err != nil {
		return nil, err
	}
	usage := ContextOf(p)
	steps := []any{map[string]any{"before": usage}}
	if usage.UsedTokens > 20000 {
		text := fmt.Sprintf("/compact Handing over to %v for the review-fix phase. Keep only: the PR's purpose and design, decisions made (incl. user decisions), files changed and why, open reviewer findings and their status, what was tried and failed, and the exact next step. Drop tool output, file dumps and resolved discussions.", sel["model"])
		if _, err := c.Send(ctx, id, text, false); err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
		p, err = c.WaitIdle(ctx, id, 15*time.Minute)
		if err != nil {
			return nil, err
		}
		usage = ContextOf(p)
		steps = append(steps, map[string]any{"afterCompact": usage})
	}
	if usage.UsedTokens > budget {
		return map[string]any{"action": "fresh-thread-needed", "budget": budget, "ctx": usage, "steps": steps}, nil
	}
	if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.model-selection.set", "threadId": id, "modelSelection": sel}); err != nil {
		return nil, err
	}
	if brief != "" {
		if _, err := c.Send(ctx, id, brief, false); err != nil {
			return nil, err
		}
	}
	return map[string]any{"action": "handed-off", "to": sel["model"], "budget": budget, "ctx": usage, "steps": steps}, nil
}
