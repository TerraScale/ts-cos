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
	Updated    string `json:"updated,omitempty"`
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
		ft := FleetThread{ThreadID: t.ID, Project: projects[t.ProjectID], Title: t.Title, Status: t.Status, Model: t.ModelSelection.Model, Branch: t.Branch, Updated: t.UpdatedAt, NeedsInput: len(t.PendingRuntimeRequest) > 0 && string(t.PendingRuntimeRequest) != "null", LastError: t.LastError, PRs: []ThreadPR{}}
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
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			asks = append(asks, Ask{Host: c.Host, ThreadID: t.ID, Title: t.Title, Error: "thread projection failed"})
			continue
		}
		for _, r := range p.PendingRequests() {
			asks = append(asks, Ask{Host: c.Host, ThreadID: t.ID, Title: t.Title, Kind: "request", RequestID: r["requestId"].(string), RequestKind: r["kind"].(string), Payload: r["payload"]})
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

var ticketKey = regexp.MustCompile(`\bTS-\d+\b`)

type StatusThread struct {
	Host       string `json:"host"`
	ThreadID   string `json:"threadId,omitempty"`
	Key        string `json:"key,omitempty"`
	Title      string `json:"title,omitempty"`
	Status     string `json:"status,omitempty"`
	Model      string `json:"model,omitempty"`
	NeedsInput bool   `json:"needsInput"`
	// PRs is []ThreadPR from a host; Status replaces it with one summary line per PR.
	PRs    any    `json:"prs,omitempty"`
	Merged *int   `json:"merged,omitempty"`
	Last   string `json:"last,omitempty"`
	Asks   []Ask  `json:"asks,omitempty"`
	Error  string `json:"error,omitempty"`
}

// StatusHost lists this host's live ticket threads: top-level, not settled, title with a TS key.
func (c *T3) StatusHost(ctx context.Context) (map[string]any, error) {
	s, err := c.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	threads := []StatusThread{}
	for _, t := range s.Threads {
		key := ticketKey.FindString(t.Title)
		if t.ArchivedAt != nil || t.DeletedAt != nil || t.SettledAt != nil || t.Lineage.Parent != "" || key == "" {
			continue
		}
		prs := []ThreadPR{}
		for _, p := range t.PullRequests {
			prs = append(prs, ThreadPR{URL: p.URL, State: p.Snapshot.State})
		}
		row := StatusThread{Host: c.Host, ThreadID: t.ID, Key: key, Title: t.Title, Status: t.Status, Model: t.ModelSelection.Model, NeedsInput: t.NeedsInput(), PRs: prs}
		if !active(t.Status) {
			p, err := c.Projection(ctx, t.ID)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				row.Error = "thread projection failed"
			} else {
				row.Last, row.Asks = lastReplyAndAsks(p)
			}
		}
		threads = append(threads, row)
	}
	return map[string]any{"host": c.Host, "threads": threads}, nil
}

// lastReplyAndAsks returns the last assistant reply (one line, last 280 runes) and the
// NEEDS-USER blocks posted since the last "USER ANSWER" / "COS DECISION".
func lastReplyAndAsks(p Projection) (string, []Ask) {
	last, answered := "", -1
	for i, m := range p.Messages {
		if m.Role == "assistant" {
			last = m.Text
		}
		if m.Role == "user" && (strings.Contains(m.Text, "USER ANSWER") || strings.Contains(m.Text, "COS DECISION")) {
			answered = i
		}
	}
	r := []rune(strings.Join(strings.Fields(last), " "))
	if len(r) > 280 {
		r = r[len(r)-280:]
	}
	var asks []Ask
	for _, m := range p.Messages[answered+1:] {
		if m.Role == "assistant" {
			asks = append(asks, ParseAsks(m.Text)...)
		}
	}
	return string(r), asks
}

type StatusReport struct {
	Threads   []StatusThread `json:"threads"`
	OrphanPRs []string       `json:"orphanPrs"`
	PRError   string         `json:"prError,omitempty"`
}

// Status joins every host's live ticket threads with the open PRs (cos.mjs `status`).
func Status(ctx context.Context, cfg Config, bus *Bus) (StatusReport, error) {
	hosts, err := bus.Hosts(ctx)
	if err != nil {
		return StatusReport{}, err
	}
	perHost := make([][]StatusThread, len(hosts))
	var prs []PR
	var prErr error
	var wg sync.WaitGroup
	wg.Go(func() { prs, prErr = (Forgejo{Config: cfg}).PRs(ctx) })
	for i, h := range hosts {
		wg.Go(func() {
			var response struct {
				Threads []struct {
					StatusThread
					PRs []ThreadPR `json:"prs"`
				} `json:"threads"`
			}
			if err := bus.Call(ctx, h, "status-host", nil, &response); err != nil {
				perHost[i] = []StatusThread{{Host: h, Error: err.Error()}}
				return
			}
			for _, t := range response.Threads {
				t.StatusThread.PRs = t.PRs
				perHost[i] = append(perHost[i], t.StatusThread)
			}
		})
	}
	wg.Wait()
	return joinStatus(perHost, prs, prErr), nil
}

func joinStatus(perHost [][]StatusThread, prs []PR, prErr error) StatusReport {
	out := StatusReport{Threads: []StatusThread{}, OrphanPRs: []string{}}
	if prErr != nil {
		out.PRError = prErr.Error()
	}
	open := map[string]PR{}
	for _, p := range prs {
		open[strings.ToLower(p.URL)] = p
	}
	owned := map[string]bool{}
	for _, threads := range perHost {
		for _, t := range threads {
			if t.Error != "" {
				out.Threads = append(out.Threads, t)
				continue
			}
			merged, lines := 0, []string{}
			threadPRs, _ := t.PRs.([]ThreadPR)
			for _, tp := range threadPRs {
				owned[strings.ToLower(tp.URL)] = true
				p, ok := open[strings.ToLower(tp.URL)]
				switch {
				case ok:
					lines = append(lines, prLine(p))
				case tp.State == "merged":
					merged++
				default:
					parts := strings.Split(tp.URL, "/")
					lines = append(lines, strings.TrimSpace(strings.Join(parts[max(0, len(parts)-3):], "/")+" "+tp.State))
				}
			}
			t.PRs, t.Merged = lines, &merged
			out.Threads = append(out.Threads, t)
		}
	}
	for _, p := range prs {
		if p.Cos && !owned[strings.ToLower(p.URL)] {
			line := fmt.Sprintf("%s#%d %s ci=%s", p.Repo, p.Number, p.Branch, p.CI)
			if p.Ready {
				line += " READY"
			}
			out.OrphanPRs = append(out.OrphanPRs, line)
		}
	}
	return out
}

func prLine(p PR) string {
	line := fmt.Sprintf("%s#%d ci=%s", p.Repo, p.Number, p.CI)
	if len(p.Failing) > 0 {
		line += "(" + strings.Join(p.Failing, ",") + ")"
	}
	bot := "-"
	if p.BotApproved {
		bot = "ok"
	} else if p.ChangesRequested {
		bot = "changes"
	}
	merge := "CONFLICT"
	if p.Mergeable {
		merge = "ok"
	}
	line += " bot=" + bot + " merge=" + merge
	if p.Ready {
		line += " READY"
	}
	return line
}
