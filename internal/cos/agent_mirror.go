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
		Type      string          `json:"type"`
		RunID     string          `json:"runId"`
		Text      string          `json:"text"`
		Title     string          `json:"title"`
		FileName  string          `json:"fileName"`
		Input     json.RawMessage `json:"input"`
		RequestID string          `json:"requestId"`
		Questions json.RawMessage `json:"questions"`
	} `json:"turnItems"`
	RuntimeRequests []struct {
		ID        string          `json:"id"`
		Status    string          `json:"status"`
		CreatedAt time.Time       `json:"createdAt"`
		Payload   json.RawMessage `json:"payload"`
		Request   json.RawMessage `json:"request"`
	} `json:"runtimeRequests"`
	ProviderTurns []map[string]any `json:"providerTurns"`
}

func (p agentProjection) latest() (string, string) {
	if len(p.Runs) == 0 {
		return "", ""
	}
	r := p.Runs[len(p.Runs)-1]
	return r.ID, r.Status
}

// turnQuestions returns the questions of a runtime request's user_input_request item, which win over the payload.
func (p agentProjection) turnQuestions(request string) json.RawMessage {
	for _, item := range p.TurnItems {
		if item.RequestID == request && len(item.Questions) > 0 && string(item.Questions) != "null" {
			return item.Questions
		}
	}
	return nil
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

// mirror copies new COS output and pending questions into the replicated chat.
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
	pending := map[string]bool{}
	for _, r := range p.RuntimeRequests {
		if r.Status != "pending" {
			continue
		}
		pending[r.ID] = true
		if _, ok := st.Asked[r.ID]; ok {
			continue
		}
		payload := r.Payload
		if len(payload) == 0 || string(payload) == "null" {
			payload = r.Request
		}
		if q := p.turnQuestions(r.ID); q != nil {
			payload = fmt.Appendf(nil, `{"questions":%s}`, q)
		}
		var in struct {
			Questions []struct {
				ID       string `json:"id"`
				Header   string `json:"header"`
				Question string `json:"question"`
				Options  []struct {
					Label string `json:"label"`
				} `json:"options"`
			} `json:"questions"`
		}
		if json.Unmarshal(payload, &in) != nil || len(in.Questions) == 0 {
			continue
		}
		ask := &ChatAsk{Host: a.Host, ThreadID: st.ThreadID, RequestID: r.ID, Answers: map[string]string{}}
		for _, q := range in.Questions {
			cq := ChatQuestion{ID: q.ID, Header: q.Header, Text: q.Question}
			for _, o := range q.Options {
				cq.Options = append(cq.Options, o.Label)
			}
			ask.Questions = append(ask.Questions, cq)
		}
		// A re-mirror after a failed save keeps the entry, so answers already recorded in it survive.
		id := replyID(r.ID, r.CreatedAt)
		if _, ok := a.Chat.Entry(id); !ok {
			if _, err := a.Chat.Put(ctx, ChatEntry{ID: id, Role: "cos", Ask: ask}); err != nil {
				return err
			}
		}
		if st.Asked == nil {
			st.Asked = map[string]string{}
		}
		st.Asked[r.ID] = id
	}
	for request, id := range st.Asked {
		if pending[request] {
			continue
		}
		e, ok := a.Chat.Entry(id)
		if !ok {
			continue
		}
		if e.Ask != nil && !e.Ask.Done {
			e.Ask.Done = true
			if err := a.Chat.Replace(ctx, e); err != nil {
				return err
			}
		}
		delete(st.Asked, request)
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
