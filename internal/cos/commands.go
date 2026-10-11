package cos

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"sort"
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
	"inventory": {0, 0, "inventory <host>"}, "purge-children": {0, 0, "purge-children <host>"}, "status-host": {0, 0, "status-host <host>"},
	"launch": {1, 1, "launch <host> '<json>'"}, "dispatch": {1, 1, "dispatch <host> '<json>'"},
	"thread": {1, 2, "thread <host> <threadId> [n]"}, "ctx": {1, 1, "ctx <host> <threadId>"}, "settle": {1, 1, "settle <host> <threadId>"}, "archive": {1, 1, "archive <host> <threadId>"}, "close": {1, 1, "close <host> <threadId>"}, "delete": {1, 1, "delete <host> <threadId>"},
	"send": {2, 3, "send <host> <threadId> '<text>' [queue]"}, "compact": {1, 2, "compact <host> <threadId> ['<focus>']"}, "fork": {1, 2, "fork <host> <threadId> ['<title>']"}, "watch": {2, 2, "watch <host> <threadId> <prUrl>"}, "model": {2, 2, "model <host> <threadId> '<modelSelection json>'"}, "rename": {2, 2, "rename <host> <threadId> '<title>'"},
	"handoff": {2, 3, "handoff <host> <threadId> '<modelSelection json>' ['<brief>']"}, "respond": {3, 3, "respond <host> <threadId> <requestId> '<answers json>'"},
	"purge": {0, 1, "purge <host> ['<keep id substrings csv>']"}, "stop": {0, 1, "stop <host> ['<keep id substrings csv>']"},
	"unpushed": {1, 1, "unpushed <host> <threadId>"},
}

func validateLocal(command string, args []string) error {
	spec, ok := localCommands[command]
	if !ok {
		return errors.New("unknown command")
	}
	if len(args) < spec.Min || len(args) > spec.Max {
		return fmt.Errorf("usage: cos %s", spec.Usage)
	}
	for _, a := range args {
		if strings.TrimSpace(a) == "" {
			return errors.New("command arguments cannot be empty")
		}
	}
	if command == "send" && len(args) == 3 && !sendArgPattern.MatchString(args[2]) {
		return errors.New("send mode must be queue, user, or deliver:<messageId>")
	}
	if command == "thread" && len(args) == 2 {
		if n, err := strconv.Atoi(args[1]); err != nil || n < 1 || n > 50 {
			return errors.New("n must be a positive integer up to 50")
		}
	}
	return nil
}

type OperationError string

func (e OperationError) Error() string         { return string(e) }
func (e OperationError) PublicMessage() string { return string(e) }

func localObjectJSON(raw string) (map[string]any, error) {
	value, err := objectJSON(raw)
	if err != nil {
		if err.Error() == "expected a JSON object" {
			return nil, OperationError("expected a JSON object")
		}
		return nil, OperationError("invalid JSON object")
	}
	return value, nil
}

func LocalCommand(ctx context.Context, cfg Config, command string, args []string) (any, error) {
	return localCommand(ctx, func(ctx context.Context) (*T3, error) { return OpenT3(ctx, cfg, cfg.LocalHost) }, command, args)
}

// localCommand runs one operation on the T3 that open returns; the e2e check passes a broken T3 to fail one host.
func localCommand(ctx context.Context, open func(context.Context) (*T3, error), command string, args []string) (any, error) {
	if command == "asks" {
		command = "asks-host"
	}
	if err := validateLocal(command, args); err != nil {
		return nil, OperationError(err.Error())
	}
	if err := threadModelGuard(command, args); err != nil {
		return nil, err
	}
	c, err := open(ctx)
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
		input, err := localObjectJSON(args[0])
		if err != nil {
			return nil, err
		}
		input, err = launchInput(CurrentBehavior().Models, input)
		if err != nil {
			return nil, err
		}
		return c.Launch(ctx, input)
	}
	if command == "dispatch" {
		input, err := localObjectJSON(args[0])
		if err != nil {
			return nil, err
		}
		return c.Dispatch(ctx, input)
	}
	if command == "tidy" {
		return c.Tidy(ctx)
	}
	switch command {
	case "inventory":
		return c.Inventory(ctx)
	case "purge":
		return c.Purge(ctx, keepIDs(args))
	case "purge-children":
		return c.PurgeChildren(ctx)
	case "stop":
		return c.Stop(ctx, keepIDs(args))
	case "status-host":
		return c.StatusHost(ctx)
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
		runs := p.Runs
		if len(runs) > 3 {
			runs = runs[len(runs)-3:]
		}
		if runs == nil {
			runs = []ThreadRun{}
		}
		out := map[string]any{"host": c.Host, "title": p.Thread.Title, "branch": p.Thread.Branch, "worktreePath": p.Thread.WorktreePath, "archived": p.Thread.ArchivedAt != nil, "settled": p.Thread.SettledAt != nil, "deleted": p.Thread.DeletedAt != nil, "runStatus": status, "runs": runs, "pendingRequests": p.PendingRequests(), "lineage": p.Thread.Lineage, "subagents": len(p.Subagents), "lastReply": reply}
		if len(args) == 2 {
			n, _ := strconv.Atoi(args[1])
			out["replies"] = lastReplies(p, n)
		}
		return out, nil
	case "compact":
		focus := ""
		if len(args) == 2 {
			focus = args[1]
		}
		return c.Compact(ctx, id, focus)
	case "close", "delete":
		return c.CloseTree(ctx, id, command == "delete")
	case "handoff":
		sel, err := localObjectJSON(args[1])
		if err != nil {
			return nil, err
		}
		return c.Handoff(ctx, id, sel)
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
			return nil, OperationError("not a PR URL")
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(parts) != 4 || parts[2] != "pulls" {
			return nil, OperationError("not a PR URL")
		}
		number, err := strconv.Atoi(parts[3])
		if err != nil || number < 1 {
			return nil, OperationError("not a PR URL")
		}
		return c.Dispatch(ctx, map[string]any{"type": "thread.pull-request.watch", "threadId": id, "host": u.Host, "repository": strings.ToLower(parts[0] + "/" + parts[1]), "number": number, "watching": true, "link": map[string]any{"url": args[1], "source": "manual"}})
	case "model":
		sel, err := localObjectJSON(args[1])
		if err != nil {
			return nil, err
		}
		return c.Dispatch(ctx, map[string]any{"type": "thread.model-selection.set", "threadId": id, "modelSelection": sel})
	case "respond":
		answers, err := localObjectJSON(args[2])
		if err != nil {
			return nil, err
		}
		return c.Dispatch(ctx, map[string]any{"type": "runtime-request.respond", "threadId": id, "requestId": args[1], "answers": answers})
	case "rename":
		return c.Dispatch(ctx, map[string]any{"type": "thread.metadata.update", "threadId": id, "title": args[1]})
	case "settle", "archive":
		return c.Dispatch(ctx, map[string]any{"type": "thread." + command, "threadId": id})
	case "unpushed":
		p, err := c.Projection(ctx, id)
		if err != nil {
			return nil, err
		}
		return unpushedState(ctx, c.Host, id, p.Thread.WorktreePath)
	}
	return nil, fmt.Errorf("unknown command %q", command)
}

// unpushedState reports a thread's worktree for the delete safeguard. Callers must refuse destructive actions when missing is true.
func unpushedState(ctx context.Context, host, id, path string) (map[string]any, error) {
	out := map[string]any{"host": host, "threadId": id, "worktree": path, "dirty": false, "ahead": 0}
	if path == "" {
		return out, nil
	}
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		out["missing"] = true
		return out, nil
	} else if err != nil {
		return nil, err
	}
	dirty, ahead, err := worktreeState(ctx, path)
	if err != nil {
		return nil, err
	}
	out["dirty"], out["ahead"] = dirty, ahead
	return out, nil
}

// worktreeState reports uncommitted changes and commits that no remote branch has.
func worktreeState(ctx context.Context, dir string) (dirty bool, ahead int, err error) {
	status, err := gitOutput(ctx, dir, "status", "--porcelain")
	if err != nil {
		return false, 0, err
	}
	count, err := gitOutput(ctx, dir, "rev-list", "--count", "HEAD", "--not", "--remotes")
	if err != nil {
		return false, 0, err
	}
	ahead, err = strconv.Atoi(strings.TrimSpace(count))
	if err != nil {
		return false, 0, fmt.Errorf("git rev-list --count in %s: %w", dir, err)
	}
	return status != "", ahead, nil
}

// gitOutput runs git in dir with a 10 s cap; the error names the git arguments that failed.
func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(strings.TrimSpace(string(exitErr.Stderr))) > 0 {
			return "", fmt.Errorf("git %s in %s: %w: %s", strings.Join(args, " "), dir, err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return "", fmt.Errorf("git %s in %s: %w", strings.Join(args, " "), dir, err)
	}
	return string(out), nil
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
			return nil, OperationError(fmt.Sprintf("requested project was not found on %s", c.Host))
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
				out.Skipped = append(out.Skipped, map[string]string{"id": id, "reason": "settle failed"})
				continue
			}
		}
		out.Closed = append(out.Closed, id)
		if deleteThread {
			if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.delete", "threadId": id}); err != nil {
				out.Skipped = append(out.Skipped, map[string]string{"id": id, "reason": "delete failed"})
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
	attempted := map[string]bool{}
	for pass := 0; pass < len(s.Threads); pass++ {
		progress := false
		for _, original := range s.Threads {
			t := byID[original.ID]
			if attempted[t.ID] || t.SettledAt != nil || t.ArchivedAt != nil || t.DeletedAt != nil || t.Lineage.Parent == "" {
				continue
			}
			p, ok := byID[t.Lineage.Parent]
			if ok && p.SettledAt == nil && p.ArchivedAt == nil && p.DeletedAt == nil {
				continue
			}
			attempted[t.ID] = true
			if active(t.Status) {
				out.Skipped = append(out.Skipped, map[string]string{"id": t.ID, "reason": "active run"})
				continue
			}
			if err := ctx.Err(); err != nil {
				return out, err
			}
			if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.settle", "threadId": t.ID}); err != nil {
				out.Skipped = append(out.Skipped, map[string]string{"id": t.ID, "reason": "settle failed"})
				continue
			}
			settledAt := "local"
			t.SettledAt = &settledAt
			byID[t.ID] = t
			out.Closed = append(out.Closed, t.Title)
			progress = true
		}
		if !progress {
			break
		}
	}
	return out, nil
}

// handoffCompactTokens is the context use above which a handoff compacts the thread first.
const handoffCompactTokens = 20000

// Handoff moves an idle thread to sel's model; the gate sends the brief after it.
func (c *T3) Handoff(ctx context.Context, id string, sel map[string]any) (any, error) {
	window := 200000
	for _, key := range []string{"instanceId", "model"} {
		switch sel[key] {
		case "codex":
			window = 258400
		case "claudeAgent":
			window = 1000000
		}
	}
	budget := window / 2
	p, err := c.WaitIdle(ctx, id, 20*time.Minute)
	if err != nil {
		return nil, err
	}
	usage := ContextOf(p)
	steps := []any{map[string]any{"before": usage}}
	// Every later turn re-sends the whole context; the target model only needs a summary.
	if usage.UsedTokens > handoffCompactTokens {
		focus := fmt.Sprintf("Handing over to %v for the review-fix phase. Keep only: the PR's purpose and design, decisions made (incl. user decisions), files changed and why, open reviewer findings and their status, what was tried and failed, and the exact next step. Drop tool output, file dumps and resolved discussions.", sel["model"])
		compacted, err := c.Compact(ctx, id, focus)
		if err != nil {
			return nil, err
		}
		usage = compacted.After
		steps = append(steps, map[string]any{"afterCompact": usage})
	}
	if usage.UsedTokens > budget {
		return map[string]any{"action": "fresh-thread-needed", "budget": budget, "ctx": usage, "steps": steps}, nil
	}
	if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.model-selection.set", "threadId": id, "modelSelection": sel}); err != nil {
		return nil, err
	}
	return map[string]any{"action": "handed-off", "to": sel["model"], "budget": budget, "ctx": usage, "steps": steps}, nil
}

// InventoryThread is one live thread in cleanup planning output.
type InventoryThread struct {
	ID       string  `json:"id"`
	Title    string  `json:"title"`
	Status   string  `json:"status"`
	Settled  bool    `json:"settled"`
	Archived bool    `json:"archived"`
	Parent   *string `json:"parent"`
	Updated  *string `json:"updated"`
}

type PurgeResult struct {
	Deleted int      `json:"deleted"`
	Skipped []string `json:"skipped"`
}

type StopResult struct {
	Stopped []string `json:"stopped"`
	Failed  []string `json:"failed"`
}

// threadGraph indexes the live (not deleted) threads of one snapshot.
type threadGraph struct {
	live     []Thread
	byID     map[string]Thread
	children map[string][]string
}

func newThreadGraph(s ShellSnapshot) threadGraph {
	g := threadGraph{byID: map[string]Thread{}, children: map[string][]string{}}
	for _, t := range s.Threads {
		if t.DeletedAt != nil {
			continue
		}
		g.live = append(g.live, t)
		g.byID[t.ID] = t
		g.children[t.Lineage.Parent] = append(g.children[t.Lineage.Parent], t.ID)
	}
	return g
}

// ancestors walks live parents until a missing parent or a cycle.
func (g threadGraph) ancestors(t Thread) []Thread {
	var out []Thread
	seen := map[string]bool{t.ID: true}
	for p, ok := g.byID[t.Lineage.Parent]; ok && !seen[p.ID]; p, ok = g.byID[p.Lineage.Parent] {
		seen[p.ID] = true
		out = append(out, p)
	}
	return out
}

// busy reports whether the thread or any live descendant is active.
func (g threadGraph) busy(id string, seen map[string]bool) bool {
	if seen[id] {
		return false
	}
	seen[id] = true
	if t, ok := g.byID[id]; ok && active(t.Status) {
		return true
	}
	for _, child := range g.children[id] {
		if g.busy(child, seen) {
			return true
		}
	}
	return false
}

func (c *T3) Inventory(ctx context.Context) ([]InventoryThread, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	out := []InventoryThread{}
	for _, t := range newThreadGraph(s).live {
		out = append(out, InventoryThread{ID: t.ID, Title: firstRunes(t.Title, 90), Status: t.Status, Settled: t.SettledAt != nil, Archived: t.ArchivedAt != nil, Parent: optionalString(t.Lineage.Parent), Updated: optionalString(t.UpdatedAt)})
	}
	return out, nil
}

// Purge deletes settled top-level threads with their subtrees, children first; trees with an active run are skipped.
func (c *T3) Purge(ctx context.Context, keep []string) (PurgeResult, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return PurgeResult{}, err
	}
	g := newThreadGraph(s)
	out := PurgeResult{Skipped: []string{}}
	seen := map[string]bool{}
	var tree []string
	var walk func(string)
	walk = func(id string) {
		if seen[id] {
			return
		}
		seen[id] = true
		for _, child := range g.children[id] {
			walk(child)
		}
		tree = append(tree, id)
	}
	for _, root := range g.live {
		if root.Lineage.Parent != "" || root.SettledAt == nil || active(root.Status) || keptBy(root.ID, keep) {
			continue
		}
		// Deleting a parent can cancel a running child (TS-805/TS-808): keep busy trees whole.
		if g.busy(root.ID, map[string]bool{}) {
			out.Skipped = append(out.Skipped, root.ID+": active run in subtree")
			continue
		}
		tree = nil
		walk(root.ID)
		for _, id := range tree {
			if err := ctx.Err(); err != nil {
				return out, err
			}
			c.removeThread(ctx, g.byID[id], &out)
		}
	}
	return out, nil
}

// PurgeChildren deletes finished child threads, deepest first, unless they or an ancestor are active.
func (c *T3) PurgeChildren(ctx context.Context) (PurgeResult, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return PurgeResult{}, err
	}
	g := newThreadGraph(s)
	var candidates []Thread
	depth := map[string]int{}
	for _, t := range g.live {
		if t.Lineage.Parent == "" || g.busy(t.ID, map[string]bool{}) {
			continue
		}
		ancestors := g.ancestors(t)
		if anyActive(ancestors) {
			continue
		}
		depth[t.ID] = len(ancestors)
		candidates = append(candidates, t)
	}
	sort.SliceStable(candidates, func(i, j int) bool { return depth[candidates[i].ID] > depth[candidates[j].ID] })
	out := PurgeResult{Skipped: []string{}}
	for _, t := range candidates {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		c.removeThread(ctx, t, &out)
	}
	return out, nil
}

// Stop interrupts active runs and cancels queued ones on every active thread not matched by keep.
func (c *T3) Stop(ctx context.Context, keep []string) (StopResult, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return StopResult{}, err
	}
	out := StopResult{Stopped: []string{}, Failed: []string{}}
	for _, t := range newThreadGraph(s).live {
		if !active(t.Status) || keptBy(t.ID, keep) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return out, err
		}
		label := threadTitleLabel(t)
		p, err := c.Projection(ctx, t.ID)
		if err != nil {
			out.Failed = append(out.Failed, label+": projection failed")
			continue
		}
		for _, r := range p.Runs {
			if !active(r.Status) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return out, err
			}
			command := map[string]any{"type": "run.interrupt", "threadId": t.ID, "runId": r.ID, "createdBy": "user", "creationSource": "server"}
			if r.Status == "queued" {
				command["type"] = "queued-run.cancel"
			}
			if _, err := c.Dispatch(ctx, command); err != nil {
				out.Failed = append(out.Failed, label+": interrupt failed")
				continue
			}
			out.Stopped = append(out.Stopped, label+" ("+r.Status+")")
		}
	}
	return out, nil
}

// removeThread settles then deletes one thread; errors become fixed skip strings, never upstream text.
func (c *T3) removeThread(ctx context.Context, t Thread, out *PurgeResult) {
	if t.SettledAt == nil {
		if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.settle", "threadId": t.ID}); err != nil {
			out.Skipped = append(out.Skipped, t.ID+": settle failed")
			return
		}
	}
	if _, err := c.Dispatch(ctx, map[string]any{"type": "thread.delete", "threadId": t.ID}); err != nil {
		out.Skipped = append(out.Skipped, t.ID+": delete failed")
		return
	}
	out.Deleted++
}

// keepIDs reads the optional <keep csv> argument, dropping empty entries.
func keepIDs(args []string) (keep []string) {
	if len(args) == 1 {
		for _, k := range strings.Split(args[0], ",") {
			if k != "" {
				keep = append(keep, k)
			}
		}
	}
	return keep
}

func keptBy(id string, keep []string) bool {
	for _, k := range keep {
		if strings.Contains(id, k) {
			return true
		}
	}
	return false
}

func anyActive(threads []Thread) bool {
	for _, t := range threads {
		if active(t.Status) {
			return true
		}
	}
	return false
}

// lastReplies returns the last n assistant messages in full, oldest first.
func lastReplies(p Projection, n int) []string {
	replies := []string{}
	for _, m := range p.Messages {
		if m.Role == "assistant" {
			replies = append(replies, m.Text)
		}
	}
	if len(replies) > n {
		replies = replies[len(replies)-n:]
	}
	return replies
}

func threadTitleLabel(t Thread) string {
	if t.Title == "" {
		return t.ID
	}
	return firstRunes(t.Title, 50)
}

func firstRunes(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		r = r[:n]
	}
	return string(r)
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
