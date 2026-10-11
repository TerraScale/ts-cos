package cos

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"
)

// boardStages is the filter order; boardUrgency is the display order, blockers aside.
var (
	boardStages  = []string{"queued", "working", "in review", "merging", "deployed", "done"}
	boardUrgency = []string{"working", "in review", "merging", "queued", "deployed", "done"}
)

const boardRecent = 7 * 24 * time.Hour

// boardStageArg maps "in-review" or "review" to its row stage; "" means every stage.
func boardStageArg(arg string) (string, bool) {
	if arg == "review" {
		return "in review", true
	}
	stage := strings.ReplaceAll(arg, "-", " ")
	return stage, stage == "" || slices.Contains(boardStages, stage)
}

type BoardThread struct {
	Host, ThreadID, Title, Model, Status string
	NeedsInput                           bool
	LastError                            string
}
type BoardPR struct {
	Repo                           string
	Number                         int
	URL, Title, CI                 string
	Ready, Mergeable, Merged, Hold bool
}
type BoardRow struct {
	Ticket, Title, Stage string
	Threads              []BoardThread
	PRs                  []BoardPR
	Deployed             time.Time
	Since                time.Time // first activity seen; age = now - Since
	LastAt               time.Time
	LastEvent            string // "kind: summary"
	Blocker              string
}

// BoardEventSpan is one ticket's footprint in the events table.
type BoardEventSpan struct {
	First, Last           time.Time
	LastKind, LastSummary string
	Merged                []mergedPR
}
type mergedPR struct {
	Repo   string
	Number int
	Time   time.Time
}

func BuildBoard(tasks []Task, live Live, inbox []InboxItem, spans map[string]BoardEventSpan, deployedAt, now time.Time) []BoardRow {
	taskOf := map[string]*Task{}
	names := map[string]bool{}
	for i := range tasks {
		t := strings.ToUpper(tasks[i].Ticket)
		taskOf[t] = &tasks[i]
		names[t] = true
	}
	threads := map[string][]BoardThread{}
	for _, h := range live.Fleet {
		if h.Stale {
			continue
		}
		for _, th := range h.Threads {
			if !active(th.Status) {
				continue
			}
			for _, t := range ticketsIn(th.Title + " " + th.Branch) {
				threads[t] = append(threads[t], BoardThread{Host: h.Host, ThreadID: th.ThreadID, Title: th.Title,
					Model: th.Model, Status: th.Status, NeedsInput: th.NeedsInput, LastError: th.LastError})
				names[t] = true
			}
		}
	}
	prs := map[string][]PR{}
	for _, p := range live.PRs {
		for _, t := range prTickets(p) {
			prs[t] = append(prs[t], p)
			names[t] = true
		}
	}
	open := map[string]*InboxItem{}
	for _, item := range inbox {
		if t := strings.ToUpper(item.Ticket); item.Status == "open" && t != "" && open[t] == nil {
			open[t] = &item
		}
	}
	for t, span := range spans {
		if now.Sub(span.Last) <= boardRecent {
			names[t] = true
		}
	}
	delete(names, "")
	rows := make([]BoardRow, 0, len(names))
	for t := range names {
		rows = append(rows, boardRow(t, taskOf[t], threads[t], prs[t], open[t], spans[t], deployedAt))
	}
	slices.SortFunc(rows, func(a, b BoardRow) int {
		if blockedA, blockedB := a.Blocker != "", b.Blocker != ""; blockedA != blockedB {
			if blockedA {
				return -1
			}
			return 1
		}
		if d := cmp.Compare(slices.Index(boardUrgency, a.Stage), slices.Index(boardUrgency, b.Stage)); d != 0 {
			return d
		}
		if d := b.LastAt.Compare(a.LastAt); d != 0 {
			return d
		}
		return strings.Compare(a.Ticket, b.Ticket)
	})
	return rows
}

func boardRow(ticket string, task *Task, threads []BoardThread, prs []PR, item *InboxItem, span BoardEventSpan, deployedAt time.Time) BoardRow {
	row := BoardRow{Ticket: ticket, Title: ticket, Threads: threads, Since: span.First, LastAt: span.Last}
	if span.LastKind != "" {
		row.LastEvent = span.LastKind + ": " + span.LastSummary
	}
	switch {
	case task != nil && task.Outcome != "":
		row.Title = task.Outcome
	case len(threads) > 0:
		row.Title = threads[0].Title
	case len(prs) > 0:
		row.Title = prs[0].Title
	}
	openPR, ready := false, false
	for _, p := range prs {
		openPR, ready = true, ready || p.Ready
		row.PRs = append(row.PRs, BoardPR{Repo: p.Repo, Number: p.Number, URL: p.URL, Title: p.Title, CI: p.CI,
			Ready: p.Ready, Mergeable: p.Mergeable, Hold: p.Hold})
	}
	for _, m := range span.Merged {
		row.PRs = append(row.PRs, BoardPR{Repo: m.Repo, Number: m.Number, Merged: true})
	}
	mergedAt := lastMerge(span)
	deployed := !mergedAt.IsZero() && deployedAt.After(mergedAt)
	if deployed {
		row.Deployed = deployedAt
	}
	working := len(threads) > 0
	switch {
	case task != nil && completed(task.Status), deployed && !openPR && !working:
		row.Stage = "done"
	case deployed && !openPR:
		row.Stage = "deployed"
	case ready:
		row.Stage = "merging"
	case openPR:
		row.Stage = "in review"
	case working:
		row.Stage = "working"
	case !mergedAt.IsZero():
		row.Stage = "merging"
	default:
		row.Stage = "queued"
	}
	row.Blocker = boardBlocker(item, threads, prs)
	return row
}

func boardBlocker(item *InboxItem, threads []BoardThread, prs []PR) string {
	if item != nil {
		return "needs you: " + item.Title
	}
	for _, t := range threads {
		if t.NeedsInput {
			return "thread waiting for input"
		}
	}
	for _, t := range threads {
		if t.LastError != "" {
			return "thread error: " + t.LastError
		}
	}
	for _, p := range prs {
		switch {
		case !p.Mergeable:
			return "conflict with " + p.Base
		case p.CI == "failure":
			return "CI failed: " + strings.Join(p.Failing, ", ")
		case p.ChangesRequested:
			return "changes requested"
		case p.Hold:
			return "on hold"
		}
	}
	return ""
}

func lastMerge(span BoardEventSpan) time.Time {
	var at time.Time
	for _, m := range span.Merged {
		if m.Time.After(at) {
			at = m.Time
		}
	}
	return at
}

// ticketsIn returns the distinct uppercase tickets named in text, in order.
func ticketsIn(text string) []string {
	var out []string
	for _, key := range ticketPattern.FindAllString(text, -1) {
		key = strings.ToUpper(key)
		if !slices.Contains(out, key) {
			out = append(out, key)
		}
	}
	return out
}

func threadTickets(th FleetThread) []string {
	return ticketsIn(th.Title + " " + th.Branch)
}

// prTickets lists tickets from the branch, then the title, then the issues Forgejo parsed.
func prTickets(p PR) []string {
	return ticketsIn(p.Branch + " " + p.Title + " " + strings.Join(p.Issues, " "))
}

// allTasks includes completed tasks, which the board needs to mark done.
func (s *Store) allTasks(ctx context.Context) ([]Task, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT payload FROM tasks ORDER BY ticket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Task{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var t Task
		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
