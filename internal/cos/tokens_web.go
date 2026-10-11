package cos

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"
)

// tokenWindows maps each window to how many UTC days it reaches back; today is 0.
var tokenWindows = map[string]int{"today": 0, "7d": 6, "30d": 29}

// usageOpus prices what Haiku workers would have cost on the advisor model.
const usageOpus = "claude-opus-5-5"

// Series names match the legend in tokens.templ.
const (
	seriesAdvisor = "Opus advisor"
	seriesWorker  = "Haiku workers"
)

// tokensPageData is everything the Tokens page shows for one window.
type tokensPageData struct {
	Window      string
	Empty       bool
	Total       UsageTotal
	Parallel    int
	RuleActions int
	Workers     float64
	Rules       float64
	Ledger      map[string]float64
	WallSaved   int
	SavedPct    float64
	SavedTarget int
	DaySaved    map[string]float64
	AdvisorAvg  float64
	Threads     []UsageTotal
	Tickets     []UsageTotal
	Models      []UsageTotal
	Days        []UsageTotal
	Chart       chartGeometry
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
	// The ledger is optional: without it the card shows rules and workers only.
	var ledgerDays []SavingsDay
	if kv := w.Chat.SavingsKV(); kv != nil {
		ledgerDays, err = SavingsReport(ctx, kv, back+1, now)
		if err != nil {
			logErr(ctx, slog.LevelWarn, "savings ledger unavailable", err)
		}
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
	d.addSavings(ledgerDays, rows)
	if window == "today" {
		d.Chart = barChart(threadColumns(foldThreads(d.Threads, 8)))
	} else {
		d.Days = SummarizeUsage(rows, live, "day")
		d.Chart = barChart(dayColumns(d.Days, since, back))
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
		logErr(r.Context(), slog.LevelError, "render tokens", err)
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

// addSavings fills the ledger, wall-clock saved and saved shares. days may be nil.
func (d *tokensPageData) addSavings(days []SavingsDay, rows []UsageRow) {
	d.Ledger = map[string]float64{}
	ledgerByDay := map[string]float64{}
	var ledger float64
	for _, sd := range days {
		for kind, c := range sd.Kinds {
			// Ledger tokens are context the Opus advisor did not read, so they price as Opus input.
			cost := UsageRow{Model: usageOpus, Input: int64(c.Tokens)}.Cost()
			d.Ledger[kind] += cost
			ledgerByDay[sd.Day] += cost
			ledger += cost
		}
		d.WallSaved += sd.Kinds[SavingParallel].WallSeconds
	}
	byDay := map[string][]UsageRow{}
	for _, r := range rows {
		byDay[r.Day] = append(byDay[r.Day], r)
	}
	// shortcut: per-day rule actions are only counted per window, so day shares leave out zero-token rule savings; upgrade with a per-day autopilot counter.
	d.DaySaved = map[string]float64{}
	for day, dayRows := range byDay {
		workers, _ := tokenSavings(dayRows, 0)
		d.DaySaved[day] = savedPct(workers+ledgerByDay[day], usageTotal(day, dayRows).Cost)
	}
	d.SavedPct = savedPct(d.Workers+d.Rules+ledger, d.Total.Cost)
	d.SavedTarget = savingsTargetPercent
}

// savedPct is the share of spend avoided: savings over savings plus what was spent, 0 when neither exists.
func savedPct(saved, spent float64) float64 {
	if saved+spent == 0 {
		return 0
	}
	return saved / (saved + spent) * 100
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

// foldThreads keeps the n costliest threads (totals arrive sorted by cost) and sums the rest into Other.
func foldThreads(totals []UsageTotal, n int) []UsageTotal {
	if len(totals) <= n {
		return totals
	}
	out := append([]UsageTotal{}, totals[:n]...)
	other := UsageTotal{Key: "Other", Label: "Other"}
	for _, t := range totals[n:] {
		other.AdvisorCost += t.AdvisorCost
		other.WorkerCost += t.WorkerCost
	}
	return append(out, other)
}

// dayColumns gives every UTC day of the window a column, zero days included.
// 7d labels every day; 30d labels every fifth day.
func dayColumns(days []UsageTotal, since time.Time, back int) []chartColumn {
	byDay := make(map[string]UsageTotal, len(days))
	for _, d := range days {
		byDay[d.Key] = d
	}
	every := 1
	if back > 6 {
		every = 5
	}
	cols := make([]chartColumn, 0, back+1)
	for i := 0; i <= back; i++ {
		day := since.AddDate(0, 0, i)
		name := day.Format("Jan 2")
		t := byDay[day.Format("2006-01-02")]
		col := chartColumn{Name: name, Advisor: t.AdvisorCost, Worker: t.WorkerCost}
		if i%every == 0 {
			col.Label = name
		}
		cols = append(cols, col)
	}
	return cols
}

// threadColumns labels each thread by its ticket when it has one, else a short name.
func threadColumns(threads []UsageTotal) []chartColumn {
	cols := make([]chartColumn, 0, len(threads))
	for _, t := range threads {
		cols = append(cols, chartColumn{Label: cmp.Or(t.Ticket, shortLabel(t.Label)), Name: t.Label, Advisor: t.AdvisorCost, Worker: t.WorkerCost})
	}
	return cols
}

func shortLabel(s string) string {
	r := []rune(s)
	if len(r) <= 10 {
		return s
	}
	return string(r[:9]) + "…"
}

type chartColumn struct {
	Label, Name     string
	Advisor, Worker float64
}

type chartBar struct {
	X, Y, W, H float64
	Series     string
	Title      string
}

// chartGeometry is a stacked column chart: 10 viewBox units per column, 100 units tall.
type chartGeometry struct {
	ViewBox string
	Width   float64
	Max     float64
	Labels  []string
	Bars    []chartBar
}

// barChart stacks advisor cost (series 1) at the bottom and worker cost (series 2) above it.
func barChart(cols []chartColumn) chartGeometry {
	var peak float64
	for _, c := range cols {
		peak = max(peak, c.Advisor+c.Worker)
	}
	top := niceMax(peak)
	g := chartGeometry{Width: float64(len(cols)) * 10, Max: top}
	g.ViewBox = fmt.Sprintf("0 0 %g 100", g.Width)
	for i, c := range cols {
		g.Labels = append(g.Labels, c.Label)
		base := 100.0
		for _, s := range []struct {
			series, name string
			cost         float64
		}{{"1", seriesAdvisor, c.Advisor}, {"2", seriesWorker, c.Worker}} {
			if s.cost <= 0 {
				continue
			}
			h := s.cost / top * 100
			base -= h
			g.Bars = append(g.Bars, chartBar{X: float64(i)*10 + 1, Y: base, W: 8, H: h, Series: s.series, Title: c.Name + " · " + s.name + " " + money(s.cost)})
		}
	}
	return g
}

// niceMax rounds up to 1, 2 or 5 times a power of ten.
func niceMax(v float64) float64 {
	if v <= 0 {
		return 1
	}
	exp := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 5, 10} {
		if m*exp >= v {
			return m * exp
		}
	}
	return 10 * exp
}

func tokenChartTitle(window string) string {
	if window == "today" {
		return "Cost per thread today (UTC)"
	}
	return "Cost per day (UTC)"
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

func money(v float64) string   { return fmt.Sprintf("$%.2f", v) }
func percent(v float64) string { return fmt.Sprintf("%.0f%%", v) }

// clockDuration formats seconds as h:mm.
func clockDuration(sec int) string { return fmt.Sprintf("%d:%02d", sec/3600, sec%3600/60) }
func usd(v float64) string         { return fmt.Sprintf("%.2f", v) }
func svgNum(v float64) string      { return strconv.FormatFloat(v, 'f', 2, 64) }
