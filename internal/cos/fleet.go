package cos

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

type FleetThread struct {
	ThreadID   string `json:"threadId"`
	Project    string `json:"project"`
	Title      string `json:"title"`
	Status     string `json:"status"`
	Model      string `json:"model"`
	Branch     string `json:"branch"`
	NeedsInput bool   `json:"needsInput"`
	Lineage    *struct {
		Parent   string `json:"parent"`
		Relation string `json:"rel"`
	} `json:"lineage,omitempty"`
	LastError string     `json:"lastError,omitempty"`
	PRs       []ThreadPR `json:"prs"`
}
type ThreadPR struct {
	URL          string `json:"url"`
	State        string `json:"state"`
	Mergeability string `json:"mergeability"`
}
type HostFleet struct {
	Host    string        `json:"host"`
	Active  int           `json:"active"`
	Threads []FleetThread `json:"threads"`
	Error   string        `json:"error,omitempty"`
	Stale   bool          `json:"stale,omitempty"`
}
type Ask struct {
	Host        string   `json:"host,omitempty"`
	ThreadID    string   `json:"threadId,omitempty"`
	Title       string   `json:"title,omitempty"`
	Kind        string   `json:"kind,omitempty"`
	Question    string   `json:"question,omitempty"`
	Options     []string `json:"options,omitempty"`
	Why         string   `json:"why,omitempty"`
	RequestID   string   `json:"requestId,omitempty"`
	RequestKind string   `json:"requestKind,omitempty"`
	Payload     any      `json:"payload,omitempty"`
	Error       string   `json:"error,omitempty"`
	Stale       bool     `json:"stale,omitempty"`
}

func (c *T3) Shell(ctx context.Context) (HostFleet, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return HostFleet{}, err
	}
	projects := map[string]string{}
	for _, p := range s.Projects {
		projects[p.ID] = p.Title
	}
	out := HostFleet{Host: c.Host, Threads: []FleetThread{}}
	for _, t := range s.Threads {
		if t.ArchivedAt != nil || t.DeletedAt != nil || t.SettledAt != nil {
			continue
		}
		ft := FleetThread{ThreadID: t.ID, Project: projects[t.ProjectID], Title: t.Title, Status: t.Status, Model: t.ModelSelection.Model, Branch: t.Branch, NeedsInput: len(t.PendingRuntimeRequest) > 0 && string(t.PendingRuntimeRequest) != "null", LastError: t.LastError, PRs: []ThreadPR{}}
		if t.Lineage.Parent != "" {
			ft.Lineage = &struct {
				Parent   string `json:"parent"`
				Relation string `json:"rel"`
			}{t.Lineage.Parent, t.Lineage.Relation}
		}
		for _, p := range t.PullRequests {
			ft.PRs = append(ft.PRs, ThreadPR{p.URL, p.Snapshot.State, p.Snapshot.Mergeability})
		}
		out.Threads = append(out.Threads, ft)
		if active(t.Status) {
			out.Active++
		}
	}
	return out, nil
}

func Fleet(ctx context.Context, bus *Bus) ([]HostFleet, error) {
	hosts, err := bus.Hosts(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]HostFleet, len(hosts))
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Go(func() {
			err := bus.Call(ctx, host, "shell", nil, &results[i])
			if err != nil {
				results[i] = HostFleet{Host: host, Error: err.Error(), Threads: []FleetThread{}}
			}
		})
	}
	wg.Wait()
	slices.SortStableFunc(results, func(a, b HostFleet) int {
		if a.Error != "" && b.Error == "" {
			return 1
		}
		if a.Error == "" && b.Error != "" {
			return -1
		}
		return a.Active - b.Active
	})
	return results, nil
}

var askLine = regexp.MustCompile(`^\s*(?:[-*]\s*)?\**(NEEDS-USER|OPTIONS|WHY)\**:\**\s*(.*)$`)

func ParseAsks(text string) []Ask {
	out := []Ask{}
	for _, line := range strings.Split(text, "\n") {
		m := askLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if m[1] == "NEEDS-USER" {
			out = append(out, Ask{Question: strings.TrimSpace(m[2]), Options: []string{}})
			continue
		}
		if len(out) == 0 {
			continue
		}
		cur := &out[len(out)-1]
		if m[1] == "WHY" {
			cur.Why = strings.TrimSpace(m[2])
		} else {
			cur.Options = []string{}
			for _, s := range strings.Split(m[2], "|") {
				if s = strings.TrimSpace(s); s != "" {
					cur.Options = append(cur.Options, s)
				}
			}
		}
	}
	return out
}

func (c *T3) Asks(ctx context.Context) ([]Ask, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	asks := []Ask{}
	for _, t := range s.Threads {
		pending := len(t.PendingRuntimeRequest) > 0 && string(t.PendingRuntimeRequest) != "null"
		busy := active(t.Status) && t.Status != "waiting"
		recent := t.SettledAt == nil
		if t.SettledAt != nil {
			at, _ := time.Parse(time.RFC3339, *t.SettledAt)
			recent = at.After(time.Now().Add(-48 * time.Hour))
		}
		if t.ArchivedAt != nil || t.DeletedAt != nil || (!pending && (busy || !recent)) {
			continue
		}
		p, err := c.Projection(ctx, t.ID)
		if err != nil {
			return nil, err
		}
		for _, r := range p.RuntimeRequests {
			if r.Status == "pending" {
				payload := r.Payload
				if payload == nil {
					payload = r.Request
				}
				asks = append(asks, Ask{Host: c.Host, ThreadID: t.ID, Title: t.Title, Kind: "request", RequestID: r.ID, RequestKind: r.Kind, Payload: payload})
			}
		}
		if len(p.Messages) == 0 || p.Messages[len(p.Messages)-1].Role != "assistant" {
			continue
		}
		answered := -1
		for i, m := range p.Messages {
			if m.Role == "user" && (strings.Contains(m.Text, "USER ANSWER") || strings.Contains(m.Text, "COS DECISION")) {
				answered = i
			}
		}
		seen := map[string]bool{}
		for _, m := range p.Messages[answered+1:] {
			if m.Role != "assistant" {
				continue
			}
			for _, a := range ParseAsks(m.Text) {
				if seen[a.Question] {
					continue
				}
				seen[a.Question] = true
				a.Host = c.Host
				a.ThreadID = t.ID
				a.Title = t.Title
				a.Kind = "marker"
				asks = append(asks, a)
			}
		}
	}
	return asks, nil
}

func AllAsks(ctx context.Context, bus *Bus) ([]Ask, error) {
	hosts, err := bus.Hosts(ctx)
	if err != nil {
		return nil, err
	}
	byHost := make([][]Ask, len(hosts))
	var wg sync.WaitGroup
	for i, h := range hosts {
		wg.Go(func() {
			var response struct {
				Asks []Ask `json:"asks"`
			}
			err := bus.Call(ctx, h, "asks-host", nil, &response)
			byHost[i] = response.Asks
			if err != nil {
				byHost[i] = []Ask{{Host: h, Error: err.Error()}}
			}
		})
	}
	wg.Wait()
	out := []Ask{}
	for _, a := range byHost {
		out = append(out, a...)
	}
	return out, nil
}

type ContextUsage struct {
	Provider   any `json:"provider"`
	UsedTokens int `json:"usedTokens"`
	MaxTokens  int `json:"maxTokens"`
	Pct        int `json:"pct"`
}

func ContextOf(p Projection) ContextUsage {
	for _, t := range slices.Backward(p.ProviderTurns) {
		used, max, ok := findUsage(t)
		if !ok {
			continue
		}
		provider := t["providerInstanceId"]
		if n, ok := t["nativeTurnRef"].(map[string]any); ok && n["driver"] != nil {
			provider = n["driver"]
		}
		pct := 0
		if max > 0 {
			pct = int(math.Round(float64(used) * 100 / float64(max)))
		}
		return ContextUsage{provider, used, max, pct}
	}
	return ContextUsage{}
}
func findUsage(v any) (int, int, bool) {
	switch v := v.(type) {
	case map[string]any:
		used, u := v["usedTokens"].(float64)
		max, m := v["maxTokens"].(float64)
		if u && m {
			return int(used), int(max), true
		}
		for _, key := range []string{"usage", "contextUsage", "result", "response", "data", "nativeTurnRef"} {
			if u, m, ok := findUsage(v[key]); ok {
				return u, m, true
			}
		}
		for _, key := range sortedKeys(v) {
			if u, m, ok := findUsage(v[key]); ok {
				return u, m, true
			}
		}
	case []any:
		for _, v := range slices.Backward(v) {
			if u, m, ok := findUsage(v); ok {
				return u, m, true
			}
		}
	}
	return 0, 0, false
}
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func (c *T3) WaitIdle(ctx context.Context, id string, limit time.Duration) (Projection, error) {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	for {
		p, err := c.Projection(ctx, id)
		if err != nil {
			return Projection{}, err
		}
		_, status := p.Run()
		if !active(status) {
			return p, nil
		}
		select {
		case <-ctx.Done():
			return Projection{}, fmt.Errorf("thread still busy: %w", ctx.Err())
		case <-time.After(15 * time.Second):
		}
	}
}
