package cos

import (
	"context"
	"encoding/json"
)

// handoffBrief is the data of the handoff_brief template.
type handoffBrief struct{ FromPair, FromModel, Reason, Wake, LastReply string }

// launchSuccessor starts a fresh thread on t's worktree and branch, advised by pair. brief renders the
// message from the thread's last reply. ok is false when t has no worktree to continue in.
func launchSuccessor(ctx context.Context, call func(ctx context.Context, host, cmd string, args []string, out any) error, host string, t FleetThread, pair string, brief func(lastReply string) (string, error)) (id, lastReply string, ok bool, err error) {
	var info map[string]any
	if err := call(ctx, host, "thread", []string{t.ThreadID}, &info); err != nil {
		return "", "", false, err
	}
	path, _ := info["worktreePath"].(string)
	if path == "" {
		return "", "", false, nil
	}
	title, _ := info["title"].(string)
	branch, _ := info["branch"].(string)
	lastReply, _ = info["lastReply"].(string)
	text, err := brief(lastReply)
	if err != nil {
		return "", "", false, err
	}
	pol := CurrentBehavior().Models
	input, err := json.Marshal(map[string]any{
		"projectTitle":      t.Project,
		"title":             title,
		"modelSelection":    pol.Advisor(pair, false),
		"workspaceStrategy": map[string]any{"type": "existing_worktree", "worktreePath": path, "branch": branch},
		"message":           text + "\n\n" + pol.BriefLine(pair),
	})
	if err != nil {
		return "", "", false, err
	}
	var launched map[string]any
	if err := call(ctx, host, "launch", []string{string(input)}, &launched); err != nil {
		return "", "", false, err
	}
	id, _ = launched["threadId"].(string)
	return id, lastReply, true, nil
}
