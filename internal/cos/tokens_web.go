package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// tokenWindows maps each window to how many UTC days it reaches back; today is 0.
var tokenWindows = map[string]int{"today": 0, "7d": 6, "30d": 29}

// usageOpus prices what Haiku workers would have cost on the advisor model.
const usageOpus = "claude-opus-5-5"

// tokensPageData is everything the Tokens page shows for one window.
type tokensPageData struct {
	Window      string
	Empty       bool
	Total       UsageTotal
	Parallel    int
	RuleActions int
	Workers     float64
	Rules       float64
	AdvisorAvg  float64
	Threads     []UsageTotal
	Tickets     []UsageTotal
	Models      []UsageTotal
	Days        []UsageTotal
}

// tokenWindow reads ?w=, falling back to today for anything unknown.
func tokenWindow(r *http.Request) string {
	w := r.URL.Query().Get("w")
	if _, ok := tokenWindows[w]; !ok {
		return "today"
	}
	return w
}

// tokensData reads only local SQLite: usage rows, live threads and the autopilot log.
func (w *Web) tokensData(ctx context.Context, window string, now time.Time) (tokensPageData, error) {
	back := tokenWindows[window]
	since := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -back)
	rows, err := w.Store.Usage(ctx, since.Format("2006-01-02"))
	if err != nil {
		return tokensPageData{}, err
	}
	live, err := w.Store.Live(ctx)
	if err != nil {
		return tokensPageData{}, err
	}
	actions, err := w.Store.ruleActions(ctx, since)
	if err != nil {
		return tokensPageData{}, err
	}
	d := tokensPageData{Window: window, Empty: len(rows) == 0, Total: usageTotal("total", rows), RuleActions: actions}
	d.Threads = SummarizeUsage(rows, live, "thread")
	for _, t := range d.Threads {
		d.Parallel = max(d.Parallel, t.Parallel)
	}
	d.Tickets = SummarizeUsage(rows, live, "ticket")
	d.Models = SummarizeUsage(rows, live, "model")
	d.Workers, d.Rules = tokenSavings(rows, actions)
	d.AdvisorAvg = usageAdvisorAverage(rows)
	if window != "today" {
		d.Days = SummarizeUsage(rows, live, "day")
	}
	return d, nil
}

// tokensPage renders the Tokens page for ?w=today|7d|30d.
func (w *Web) tokensPage(rw http.ResponseWriter, r *http.Request) {
	d, err := w.tokensData(r.Context(), tokenWindow(r), time.Now())
	if err != nil {
		http.Error(rw, "Cannot read token usage", 500)
		return
	}
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tokensPage(w.header(r.Context()), d).Render(r.Context(), rw); err != nil {
		slog.Error("render tokens", "error", err)
	}
}

// tokensEvents repatches the Tokens page on store changes and at least once a minute,
// because usage rows do not bump the store revision.
func (w *Web) tokensEvents(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	controller := http.NewResponseController(rw)
	notify := make(chan struct{}, 1)
	w.mu.Lock()
	w.clients[notify] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.clients, notify); w.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("X-Accel-Buffering", "no")
	window := tokenWindow(r)
	patch := func() error {
		d, err := w.tokensData(ctx, window, time.Now())
		if err != nil {
			return err
		}
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := patchElements(ctx, rw, chatHead(w.header(ctx))); err != nil {
			return err
		}
		if err := patchElements(ctx, rw, tokens(d)); err != nil {
			return err
		}
		return controller.Flush()
	}
	if patch() != nil {
		return
	}
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	refresh := time.NewTicker(60 * time.Second)
	defer refresh.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
			if patch() != nil {
				return
			}
		case <-refresh.C:
			if patch() != nil {
				return
			}
		case <-keepalive.C:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := fmt.Fprint(rw, ": keepalive\n\n"); err != nil || controller.Flush() != nil {
				return
			}
		}
	}
}

// ruleActions counts rules-engine decisions since the window start that did work without a model.
// shortcut: the autopilot log keeps only its last 200 entries, so 7d and 30d count just that tail; upgrade with a per-day counter.
func (s *Store) ruleActions(ctx context.Context, since time.Time) (int, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT payload FROM autopilot_log`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	start := since.Format(time.RFC3339)
	n := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		var e AutopilotEntry
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return 0, err
		}
		if (e.Kind == ClassAct || e.Kind == ClassRecord) && e.At >= start {
			n++
		}
	}
	return n, rows.Err()
}

// tokenSavings estimates the window's savings. Workers are priced at Opus list price
// minus what Haiku cost; rule actions are priced at the average advisor message.
// Cache hits are not savings and are not counted.
func tokenSavings(rows []UsageRow, ruleActions int) (workers, rules float64) {
	var opus, actual float64
	for _, r := range rows {
		if !usageWorker(r.Model) {
			continue
		}
		priced := r
		priced.Model = usageOpus
		opus += priced.Cost()
		actual += r.Cost()
	}
	return opus - actual, float64(ruleActions) * usageAdvisorAverage(rows)
}

// usageAdvisorAverage is the mean advisor cost per message, 0 when there are none.
func usageAdvisorAverage(rows []UsageRow) float64 {
	var cost float64
	var messages int
	for _, r := range rows {
		if usageWorker(r.Model) {
			continue
		}
		cost += r.Cost()
		messages += r.Messages
	}
	if messages == 0 {
		return 0
	}
	return cost / float64(messages)
}

// shareTone is ok when Haiku wrote at least half of the output tokens.
func shareTone(t UsageTotal) string {
	switch {
	case t.Output == 0:
		return ""
	case t.HaikuShare >= 50:
		return "ok"
	}
	return "warn"
}

// humanTokens formats counts like 12.3M.
func humanTokens(n int64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	}
	return strconv.FormatInt(n, 10)
}

func money(v float64) string { return fmt.Sprintf("$%.2f", v) }
func usd(v float64) string   { return fmt.Sprintf("%.2f", v) }
