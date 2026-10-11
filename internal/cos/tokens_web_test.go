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

func TestTokenSavings(t *testing.T) {
	rows := []UsageRow{
		// Haiku: 1M in at $0.10 and 1M out at $0.50 cost $0.60; at Opus prices $4 + $20.
		{Model: "claude-haiku-5-5", Input: 1e6, Output: 1e6, Messages: 1},
		// Advisor: $4 over two messages is $2 each.
		{Model: "claude-opus-5-5", Input: 1e6, Messages: 2},
	}
	workers, rules := tokenSavings(rows, 3)
	if !near(workers, 23.4) {
		t.Errorf("workers = %v, want 23.4", workers)
	}
	if !near(rules, 6) {
		t.Errorf("rules = %v, want 6 (3 actions at $2)", rules)
	}
	if w, r := tokenSavings(nil, 5); w != 0 || r != 0 {
		t.Errorf("no usage saved %v and %v, want 0 and 0", w, r)
	}
}

func TestHumanTokens(t *testing.T) {
	for in, want := range map[int64]string{999: "999", 1500: "1.5K", 12_300_000: "12.3M", 2_000_000_000: "2.0B"} {
		if got := humanTokens(in); got != want {
			t.Errorf("humanTokens(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestTokensPageRenders(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := NewWeb(store, nil).routes()
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		return r
	}
	for _, w := range []string{"today", "7d", "30d"} {
		if r := get("/tokens?w=" + w); r.Code != http.StatusOK || strings.Contains(r.Body.String(), "<table") {
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
			`href="/tokens"`, `>Tokens</a>`, `<span class="kpi-label">Cost</span>`, `<span class="kpi-label">Haiku share</span>`,
			`data-tone="ok"`, `<span class="kpi-label">Peak Haiku parallel</span>`, `href="/work/TS-826"`,
			`COS agent`, `2 rule actions`, `aria-pressed="true"`, `data-init="@get(&#39;/tokens/events?w=` + w,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("%s missing %q", w, want)
			}
		}
		i := strings.Index(body, `aria-labelledby="savings-heading"`)
		if i < 0 {
			t.Fatalf("%s has no savings card", w)
		}
		card := body[i : i+strings.Index(body[i:], "</section>")]
		if strings.Contains(strings.ToLower(card), "cache") {
			t.Errorf("%s savings mention cache: %s", w, card)
		}
	}
	if r := get("/tokens?w=bogus"); r.Code != http.StatusOK || !strings.Contains(r.Body.String(), `value="today" aria-pressed="true"`) {
		t.Errorf("unknown window must fall back to today, got %d", r.Code)
	}
}
