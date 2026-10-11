package cos

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestNiceMax(t *testing.T) {
	for in, want := range map[float64]float64{0: 1, 0.3: 0.5, 12.4: 20, 20: 20, 51: 100, 4000: 5000} {
		if got := niceMax(in); !near(got, want) {
			t.Errorf("niceMax(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestBarChartStacks(t *testing.T) {
	g := barChart([]chartColumn{
		{Name: "Oct 9", Label: "Oct 9", Advisor: 12.4e6, Worker: 7.6e6},
		{Name: "Oct 10"},
		{Name: "Oct 11", Worker: 5e6},
	})
	if g.ViewBox != "0 0 30 100" || !near(g.Width, 30) || !near(g.Max, 20e6) {
		t.Fatalf("geometry = %q %v %v", g.ViewBox, g.Width, g.Max)
	}
	if len(g.Bars) != 3 {
		t.Fatalf("bars = %d, want 2 for day one and 1 for day three; zero day has none", len(g.Bars))
	}
	advisor, worker := g.Bars[0], g.Bars[1]
	if advisor.Series != "1" || worker.Series != "2" {
		t.Errorf("series = %s, %s; advisor must be 1 at the bottom", advisor.Series, worker.Series)
	}
	if !near(advisor.Y+advisor.H, 100) || !near(worker.Y+worker.H, advisor.Y) {
		t.Errorf("stack broken: advisor %+v, worker %+v", advisor, worker)
	}
	if !near(advisor.H+worker.H, 100) {
		t.Errorf("heights sum to %v, want 100 at the peak day", advisor.H+worker.H)
	}
	if want := "Oct 9 · Opus advisor 12.4M output"; advisor.Title != want {
		t.Errorf("title = %q, want %q", advisor.Title, want)
	}
	if got := g.Bars[2]; got.X != 21 || got.Series != "2" || !near(got.H, 25) {
		t.Errorf("third column bar = %+v", got)
	}
	if len(g.Labels) != 3 || g.Labels[1] != "" {
		t.Errorf("labels = %q", g.Labels)
	}
}

func TestFoldThreads(t *testing.T) {
	var totals []UsageTotal
	for i := 10; i >= 1; i-- {
		totals = append(totals, UsageTotal{Label: "t", AdvisorOutput: int64(i), WorkerOutput: 1})
	}
	folded := foldThreads(totals, 8)
	if len(folded) != 9 {
		t.Fatalf("folded = %d, want top 8 plus Other", len(folded))
	}
	other := folded[8]
	if other.Label != "Other" || other.AdvisorOutput != 3 || other.WorkerOutput != 2 {
		t.Errorf("Other = %+v, want the two smallest folded (2 + 1 advisor output)", other)
	}
	if len(foldThreads(totals[:3], 8)) != 3 {
		t.Error("threads under the limit must not fold")
	}
}

func TestDayColumnsFillZeroDays(t *testing.T) {
	since := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	days := []UsageTotal{{Key: "2026-10-07", AdvisorOutput: 4}}
	cols := dayColumns(days, since, 6)
	if len(cols) != 7 {
		t.Fatalf("7d columns = %d, want 7", len(cols))
	}
	if cols[0].Label != "Oct 5" || cols[0].Advisor != 0 || cols[2].Advisor != 4 {
		t.Errorf("columns = %+v", cols)
	}
	cols = dayColumns(nil, since.AddDate(0, 0, -23), 29)
	var labels []string
	for _, c := range cols {
		if c.Label != "" {
			labels = append(labels, c.Label)
		}
	}
	if len(cols) != 30 || len(labels) != 6 {
		t.Errorf("30d columns = %d with %d labels, want 30 and every fifth (6)", len(cols), len(labels))
	}
}

func TestHumanTokens(t *testing.T) {
	for in, want := range map[int64]string{999: "999", 1500: "1.5K", 12_300_000: "12.3M", 2_000_000_000: "2.0B"} {
		if got := humanTokens(in); got != want {
			t.Errorf("humanTokens(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestSummarizeUsageSortsByTokens(t *testing.T) {
	rows := []UsageRow{
		// Opus has the bigger input, but Haiku's cache writes make it the larger total: 3M tokens against 2M.
		{Model: "claude-opus-5-5", Input: 2e6, Messages: 1},
		{Model: "claude-haiku-5-5", Input: 1e6, CacheWrite5m: 2e6, Messages: 1},
	}
	totals := SummarizeUsage(rows, Live{}, "model")
	if len(totals) != 2 || totals[0].Key != "claude-haiku-5-5" || usageTokens(totals[0]) != 3e6 {
		t.Fatalf("totals = %+v, want Haiku first with 3M tokens", totals)
	}
}

func TestTokensPageRenders(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := ownerRoutes(NewWeb(store, nil))
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		return r
	}
	for _, w := range []string{"today", "7d", "30d"} {
		if r := get("/tokens?w=" + w); r.Code != http.StatusOK || !strings.Contains(r.Body.String(), "No usage yet in this window.") {
			t.Fatalf("empty %s = %d", w, r.Code)
		}
	}

	ctx := context.Background()
	now := time.Now().UTC()
	day := func(back int) string { return now.AddDate(0, 0, -back).Format("2006-01-02") }
	cos := UsageRow{Day: day(0), Host: "delta", Project: "-home-mariogk-local-share-cos-agent", Kind: "cos", Session: "cos-1",
		Model: "claude-opus-5-5", Input: 1e6, Output: 2e5, CacheWrite5m: 5e5, CacheRead: 1e6, Messages: 10, First: now, Last: now}
	worker := UsageRow{Day: day(0), Host: "delta", Project: "-home-mariogk-t3-worktrees-ts-cos-ts-826-work-tokens", Kind: "worker",
		Worktree: "ts-826-work-tokens", Branch: "cos/ts-826-work-tokens", Ticket: "TS-826", Session: "w-1",
		Model: "claude-haiku-5-5", Input: 2e6, Output: 1e6, Messages: 20, First: now.Add(-time.Hour), Last: now}
	older := cos
	older.Day, older.Session, older.First, older.Last = day(3), "cos-2", now.AddDate(0, 0, -3), now.AddDate(0, 0, -3)
	for _, r := range []UsageRow{cos, worker, older} {
		if err := store.SaveUsage(ctx, usageKey(r), r); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []AutopilotEntry{
		{Seq: 1, At: now.Format(time.RFC3339), Kind: ClassAct, Action: "Closed"},
		{Seq: 2, At: now.Format(time.RFC3339), Kind: ClassRecord, Action: "Noted"},
		{Seq: 3, At: now.Format(time.RFC3339), Kind: "beat", Action: "Heartbeat"},
	} {
		if err := store.addAutopilotLog(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	for _, w := range []string{"today", "7d", "30d"} {
		r := get("/tokens?w=" + w)
		body := r.Body.String()
		for _, want := range []string{
			`href="/tokens"`, `>Tokens</a>`, `<span class="kpi-label">Tokens</span>`, `<span class="kpi-label">Haiku share</span>`,
			`data-tone="ok"`, `<span class="kpi-label">Peak Haiku parallel</span>`, `data-series="2"`, `href="/work/TS-826"`,
			`COS agent`, `aria-pressed="true"`, `data-init="@get(&#39;/tokens/events?w=` + w,
			`Output tokens per`, `<th scope="col" class="num">Tokens</th>`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("%s missing %q", w, want)
			}
		}
		if strings.Contains(body, "$") {
			t.Errorf("%s shows a dollar figure", w)
		}
	}
	if r := get("/tokens?w=bogus"); r.Code != http.StatusOK || !strings.Contains(r.Body.String(), `value="today" aria-pressed="true"`) {
		t.Errorf("unknown window must fall back to today, got %d", r.Code)
	}
}
