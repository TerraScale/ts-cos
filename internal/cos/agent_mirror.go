package cos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// agentProjection is the part of a T3 thread projection the chat mirrors.
type agentProjection struct {
	Runs []struct {
		ID          string    `json:"id"`
		Status      string    `json:"status"`
		RequestedAt time.Time `json:"requestedAt"`
	} `json:"runs"`
	TurnItems []struct {
		Type     string          `json:"type"`
		RunID    string          `json:"runId"`
		Text     string          `json:"text"`
		Title    string          `json:"title"`
		FileName string          `json:"fileName"`
		Input    json.RawMessage `json:"input"`
	} `json:"turnItems"`
	ProviderTurns []map[string]any `json:"providerTurns"`
}

func (p agentProjection) latest() (string, string) {
	if len(p.Runs) == 0 {
		return "", ""
	}
	r := p.Runs[len(p.Runs)-1]
	return r.ID, r.Status
}

func (p agentProjection) usedTokens() int {
	return ContextOf(Projection{ProviderTurns: p.ProviderTurns}).UsedTokens
}

// reply collects one run's assistant text and tool activity; compaction runs are hidden.
func (p agentProjection) reply(run string) (string, []string, bool) {
	texts, tools := []string{}, []string{}
	for _, item := range p.TurnItems {
		if item.RunID != run {
			continue
		}
		switch item.Type {
		case "user_message":
			if strings.HasPrefix(item.Text, "/compact") {
				return "", nil, true
			}
		case "assistant_message":
			if strings.TrimSpace(item.Text) != "" {
				texts = append(texts, item.Text)
			}
		case "reasoning", "checkpoint", "plan":
		default:
			title := item.Title
			if title == "" && item.FileName != "" {
				title = "Edit " + item.FileName
			}
			if command := ""; title == "" && json.Unmarshal(item.Input, &command) == nil {
				title = command
			}
			if title == "" {
				title = strings.ReplaceAll(item.Type, "_", " ")
			}
			tools = append(tools, truncate(title, 200))
		}
	}
	return strings.Join(texts, "\n\n"), tools, false
}

// mirror copies new COS output into the replicated chat.
func (a *Agent) mirror(ctx context.Context, st *AgentState, p agentProjection) error {
	start := 0
	for i, r := range p.Runs {
		if r.ID == st.Run {
			start = i
		}
	}
	for _, r := range p.Runs[start:] {
		text, tools, hidden := p.reply(r.ID)
		if hidden || (text == "" && len(tools) == 0) {
			continue
		}
		if r.ID != st.Run {
			st.Run, st.Reply = r.ID, replyID(r.ID, r.RequestedAt)
		}
		e := ChatEntry{ID: st.Reply, Role: "cos", Text: text, Tools: tools, Streaming: active(r.Status)}
		if err := a.show(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// show writes an entry only when its content changed, so polling does not churn JetStream.
func (a *Agent) show(ctx context.Context, e ChatEntry) error {
	data, _ := json.Marshal([]any{e.Text, e.Tools, e.Streaming})
	if a.shown[e.ID] == string(data) {
		return nil
	}
	if old, ok := a.Chat.Entry(e.ID); ok {
		e.At = old.At
	}
	if _, err := a.Chat.Put(ctx, e); err != nil {
		return err
	}
	if a.shown == nil || len(a.shown) > 100 {
		a.shown = map[string]string{}
	}
	a.shown[e.ID] = string(data)
	return nil
}

// focusOf changes with the replicated "focus" context file or the set of FULL SEND tickets.
func focusOf(focus string, v Dashboard) string {
	keys := []string{focus}
	for _, t := range v.Tasks {
		if t.Priority == "FULL SEND" {
			keys = append(keys, t.Ticket)
		}
	}
	slices.Sort(keys[1:])
	return strings.Join(keys, "|")
}

func openPRs(v Dashboard) []string {
	out := []string{}
	for _, p := range v.Live.PRs {
		out = append(out, fmt.Sprintf("%s#%d", p.Repo, p.Number))
	}
	slices.Sort(out)
	return out
}

func missingFrom(old, now []string) []string {
	out := []string{}
	for _, k := range old {
		if !slices.Contains(now, k) {
			out = append(out, k)
		}
	}
	return out
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// replyID keys a run's chat entry by the run itself, so re-mirroring a run after a failed save
// updates the same bubble instead of adding a duplicate.
func replyID(run string, requested time.Time) string {
	if requested.IsZero() {
		return chatID(time.Now())
	}
	sum := sha256.Sum256([]byte(run))
	return fmt.Sprintf("%016x-%s", requested.UnixNano(), hex.EncodeToString(sum[:4]))
}
