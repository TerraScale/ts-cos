package cos

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	workRefresh   = 30 * time.Second // usage and event mirrors do not bump the revision
	workLastEvent = 80               // runes of the last event summary shown in the table
)

// workFilter is one button of the stage filter.
type workFilter struct {
	Label, Value string
	Count        int
	Current      bool
}

// workLine is one table row: the board row with its usage columns.
type workLine struct {
	Row   BoardRow
	Usage boardUsage
}

// workGroups splits the stage filter into segmented groups, so each group fits a phone's width.
var workGroups = [][]string{{"", "queued", "working"}, {"in review", "merging"}, {"deployed", "done"}}

// workBoard is the Work table for one stage filter.
type workBoard struct {
	Stage   string // filter in use; "" shows every row
	Filters [][]workFilter
	Lines   []workLine
	Total   int
}

// boardUsage is one ticket's usage columns.
type boardUsage struct {
	Model   string // latest advisor model and effort, e.g. "opus-5-5 · high"
	Haiku   int    // Haiku share of output tokens, percent
	Workers int    // Haiku sessions
}

// workTimeline is the timeline page for one ticket.
type workTimeline struct {
	Ticket  string
	Row     BoardRow
	OnBoard bool
	Events  []workEvent
	Threads []BoardThread
	Known   bool
}

// workEvent is one timeline entry, ready to render.
type workEvent struct {
	Ago, Exact, Tone string
	Kind, Summary    string
	PR, PRURL        string // "owner/repo#3"; PRURL is empty when the PR is no longer open
	Thread           string // "beta · 1a2b3c4d"
}

func newWorkBoard(data boardData, stage string) workBoard {
	counts := map[string]int{}
	for _, r := range data.Rows {
		counts[r.Stage]++
	}
	b := workBoard{Stage: stage, Total: len(data.Rows)}
	for _, group := range workGroups {
		var filters []workFilter
		for _, s := range group {
			if s == "" {
				filters = append(filters, workFilter{Label: "All", Count: len(data.Rows), Current: stage == ""})
				continue
			}
			filters = append(filters, workFilter{Label: s, Value: stageParam(s), Count: counts[s], Current: stage == s})
		}
		b.Filters = append(b.Filters, filters)
	}
	usage := boardUsageByTicket(data.Usage)
	for _, r := range data.Rows {
		if stage == "" || r.Stage == stage {
			b.Lines = append(b.Lines, workLine{Row: r, Usage: usage[r.Ticket]})
		}
	}
	return b
}

func newWorkTimeline(ticket string, data boardData, events []Event) workTimeline {
	t := workTimeline{Ticket: ticket, Row: BoardRow{Ticket: ticket}}
	for _, r := range data.Rows {
		if r.Ticket == ticket {
			t.Row, t.OnBoard = r, true
		}
	}
	prURL := map[string]string{}
	for _, p := range data.Live.PRs {
		prURL[prKey(p.Repo, p.Number)] = p.URL
	}
	for _, e := range events {
		t.Events = append(t.Events, workEvent{
			Ago: age(e.Time) + " ago", Exact: e.Time.UTC().Format("2006-01-02 15:04:05 UTC"), Tone: eventTone(e.Kind),
			Kind: string(e.Kind), Summary: e.Summary, PR: prLabel(e), PRURL: prURL[prKey(e.Repo, e.PR)],
			Thread: threadLabel(e),
		})
	}
	if t.OnBoard {
		t.Threads = t.Row.Threads
	}
	t.Known = t.OnBoard || len(events) > 0
	return t
}

// boardStageData is the .stage[data-stage] value; a blocker shows as blocked whatever its stage.
func boardStageData(row BoardRow) string {
	if row.Blocker != "" {
		return "blocked"
	}
	return map[string]string{"working": "running", "in review": "review", "merging": "review", "deployed": "done", "done": "done", "queued": "queued"}[row.Stage]
}

// boardUsageByTicket summarizes usage rows per ticket. Model is the latest non-Haiku row; Haiku share and workers come from SummarizeUsage.
func boardUsageByTicket(rows []UsageRow) map[string]boardUsage {
	out := map[string]boardUsage{}
	for _, t := range SummarizeUsage(rows, Live{}, "ticket") {
		out[t.Key] = boardUsage{Haiku: t.HaikuShare, Workers: t.Workers}
	}
	latest := map[string]UsageRow{}
	for _, r := range rows {
		if prev, seen := latest[r.Ticket]; r.Ticket != "" && !usageWorker(r.Model) && (!seen || r.Last.After(prev.Last)) {
			latest[r.Ticket] = r
		}
	}
	for ticket, r := range latest {
		u := out[ticket]
		u.Model = modelLabel(r.Model)
		if r.Effort != "" {
			u.Model += " · " + r.Effort
		}
		out[ticket] = u
	}
	return out
}

// modelLabel is a model ID without its vendor prefix or context hint: "opus-5-5".
func modelLabel(model string) string {
	return strings.TrimPrefix(usageModel(model), "claude-")
}

// threadsText is the thread count and the hosts running them, e.g. "2 · beta, delta".
func threadsText(threads []BoardThread) string {
	if len(threads) == 0 {
		return "–"
	}
	var hosts []string
	for _, t := range threads {
		if !slices.Contains(hosts, t.Host) {
			hosts = append(hosts, t.Host)
		}
	}
	slices.Sort(hosts)
	return fmt.Sprintf("%d · %s", len(threads), strings.Join(hosts, ", "))
}

// haikuText is the Haiku share and worker count, e.g. "38% · 2 workers".
func haikuText(u boardUsage) string {
	if u.Workers == 0 {
		return "–"
	}
	workers := "workers"
	if u.Workers == 1 {
		workers = "worker"
	}
	return fmt.Sprintf("%d%% · %d %s", u.Haiku, u.Workers, workers)
}

// eventTone colors a timeline kind (cos.css .tag[data-tone]); "" is neutral.
func eventTone(kind EventKind) string {
	switch {
	case strings.Contains(string(kind), "failed"), strings.Contains(string(kind), "conflict"), strings.Contains(string(kind), "exceeded"):
		return "danger"
	case kind == KindReview, kind == KindRequestPending:
		return "warn"
	case kind == KindPRMerged, kind == kindDeployDone:
		return "ok"
	}
	return ""
}

func prLabel(e Event) string {
	if e.PR == 0 {
		return ""
	}
	return fmt.Sprintf("%s#%d", e.Repo, e.PR)
}

func threadLabel(e Event) string {
	if e.Thread == "" {
		return ""
	}
	return e.Host + " · " + clip(e.Thread, 8)
}

// stageParam is a stage in the URL and CLI form: "in-review".
func stageParam(stage string) string { return strings.ReplaceAll(stage, " ", "-") }

func workQuery(stage string) string {
	if stage == "" {
		return ""
	}
	return "?stage=" + url.QueryEscape(stageParam(stage))
}

// workStage reads ?stage= and reports false for an unknown stage.
func workStage(r *http.Request) (string, bool) {
	return boardStageArg(r.URL.Query().Get("stage"))
}

func orDash(s string) string {
	if s == "" {
		return "–"
	}
	return s
}

func ageText(t time.Time) string {
	if t.IsZero() {
		return "–"
	}
	return age(t)
}

func agoText(t time.Time) string {
	if t.IsZero() {
		return "–"
	}
	return age(t) + " ago"
}

func pressed(on bool) string {
	if on {
		return "true"
	}
	return "false"
}

// clip shortens s to n runes and marks the cut with an ellipsis.
func clip(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

// workPage shows the Work board: a stage filter over the replicated state.
func (w *Web) workPage(rw http.ResponseWriter, r *http.Request) {
	stage, ok := workStage(r)
	if !ok {
		http.Error(rw, "Unknown stage", http.StatusBadRequest)
		return
	}
	data, err := loadBoard(r.Context(), w.Store)
	if err != nil {
		http.Error(rw, "Cannot read the board", http.StatusInternalServerError)
		return
	}
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := workPage(w.header(r.Context()), newWorkBoard(data, stage)).Render(r.Context(), rw); err != nil {
		logErr(r.Context(), slog.LevelError, "render work", err)
	}
}

// workEvents re-renders the header and table on every change and on a timer for the mirrors.
func (w *Web) workEvents(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stage, ok := workStage(r)
	if !ok {
		http.Error(rw, "Unknown stage", http.StatusBadRequest)
		return
	}
	controller := http.NewResponseController(rw)
	notify := make(chan struct{}, 1)
	w.mu.Lock()
	w.clients[notify] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.clients, notify); w.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("X-Accel-Buffering", "no")
	patch := func() error {
		data, err := loadBoard(ctx, w.Store)
		if err != nil {
			return err
		}
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := patchElements(ctx, rw, chatHead(w.header(ctx))); err != nil {
			return err
		}
		if err := patchElements(ctx, rw, workPanel(newWorkBoard(data, stage))); err != nil {
			return err
		}
		return controller.Flush()
	}
	if patch() != nil {
		return
	}
	refresh := time.NewTicker(workRefresh)
	defer refresh.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
		case <-refresh.C:
		}
		if patch() != nil {
			return
		}
	}
}

// workTicket shows one ticket's row and its timeline, newest first.
func (w *Web) workTicket(rw http.ResponseWriter, r *http.Request) {
	ticket := strings.ToUpper(r.PathValue("ticket"))
	if ticketPattern.FindString(ticket) != ticket {
		http.NotFound(rw, r)
		return
	}
	ctx := r.Context()
	data, err := loadBoard(ctx, w.Store)
	if err != nil {
		http.Error(rw, "Cannot read the board", http.StatusInternalServerError)
		return
	}
	events, err := w.Store.TicketEvents(ctx, ticket, boardTimelineMax)
	if err != nil {
		http.Error(rw, "Cannot read the timeline", http.StatusInternalServerError)
		return
	}
	t := newWorkTimeline(ticket, data, events)
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if !t.Known {
		rw.WriteHeader(http.StatusNotFound)
	}
	if err := workTicketPage(w.header(ctx), t).Render(ctx, rw); err != nil {
		logErr(ctx, slog.LevelError, "render work ticket", err)
	}
}
