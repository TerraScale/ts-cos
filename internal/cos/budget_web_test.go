package cos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func budgetPostForm(t *testing.T, h http.Handler, thread, daily string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{"thread": {thread}, "daily": {daily}}
	req := httptest.NewRequest(http.MethodPost, "/settings/budgets", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestBudgetSaveStoresBothLimits(t *testing.T) {
	store := openTestStore(t)
	w := NewWeb(store, nil)
	kv := &fakeKV{data: map[string][]byte{}}
	w.Autopilot = &Autopilot{settings: kv}
	rec := budgetPostForm(t, w.routes(), " 15M ", "500k")
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/settings#budgets" {
		t.Fatalf("save = %d %q", rec.Code, rec.Header().Get("Location"))
	}
	if string(kv.data[budgetSettingsThread]) != "15000000" || string(kv.data[budgetSettingsDaily]) != "500000" {
		t.Fatalf("stored = %v", kv.data)
	}
}

func TestBudgetSaveRejectsBadInputWithoutWriting(t *testing.T) {
	store := openTestStore(t)
	w := NewWeb(store, nil)
	kv := &fakeKV{data: map[string][]byte{}}
	w.Autopilot = &Autopilot{settings: kv}
	h := w.routes()
	for _, bad := range []string{"abc", "0", "-5", "99999999999999M", "1000001M", ""} {
		rec := budgetPostForm(t, h, bad, "10M")
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `role="alert"`) || !strings.Contains(rec.Body.String(), "Per thread, per day:") {
			t.Fatalf("thread %q = %d %s", bad, rec.Code, rec.Body.String())
		}
		rec = budgetPostForm(t, h, "10M", bad)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "All threads, per day:") {
			t.Fatalf("daily %q = %d", bad, rec.Code)
		}
	}
	if len(kv.data) != 0 {
		t.Fatalf("rejected form wrote %v", kv.data)
	}
	if ok := budgetPostForm(t, h, "1000000M", "1M"); ok.Code != http.StatusSeeOther {
		t.Fatalf("1000000M at the cap = %d", ok.Code)
	}
}

func TestBudgetSaveWithoutKVIsUnavailable(t *testing.T) {
	w := NewWeb(openTestStore(t), nil)
	rec := budgetPostForm(t, w.routes(), "10M", "100M")
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), budgetUnavailable) {
		t.Fatalf("no KV = %d", rec.Code)
	}
}

func TestSettingsBudgetPanelRenders(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	today := time.Now().UTC().Format(time.DateOnly)
	rows := []UsageRow{
		{Day: today, Host: "a", Project: "cos", Kind: "cos", Model: "claude-opus-5-5", Input: 3_000_000, Last: time.Now()},
		{Day: today, Host: "a", Project: "beta", Worktree: "ts-1", Kind: "worker", Model: "claude-haiku-5-5", Input: 8_100_000, Last: time.Now()},
	}
	for i, r := range rows {
		if err := store.SaveUsage(ctx, string(rune('a'+i)), r); err != nil {
			t.Fatal(err)
		}
	}
	// The raise belongs to the worker thread; find its scope the way the page does.
	totals := SummarizeUsage(rows, Live{}, "thread")
	var worker UsageTotal
	for _, tot := range totals {
		if tot.Label == "ts-1" {
			worker = tot
		}
	}
	kv := &fakeKV{data: map[string][]byte{
		budgetSettingsThread:                           []byte("10000000"),
		budgetRaiseKey(today, budgetScope(worker.Key)): []byte("15000000"),
	}}
	w := NewWeb(store, nil)
	w.Autopilot = &Autopilot{settings: kv}
	rec := httptest.NewRecorder()
	w.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/settings", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`id="budgets"`, `name="thread" type="text" inputmode="numeric" required value="10M"`,
		`name="daily" type="text" inputmode="numeric" required value="100M"`,
		`max="100000000" low="80000000" high="100000000" optimum="0" value="11100000"`,
		`max="15000000" low="12000000" high="15000000" optimum="0" value="8100000"`,
		"8.1M / 15M", "raised today", "never paused", "Saved setting", "Default 100M from the last 7 days",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("settings missing %q", want)
		}
	}
	if strings.Contains(body, budgetUnavailable) {
		t.Error("warning shown with KV ready")
	}
}

func TestSettingsBudgetPanelWithoutKV(t *testing.T) {
	w := NewWeb(openTestStore(t), nil)
	rec := httptest.NewRecorder()
	w.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/settings", nil))
	body := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(body, budgetUnavailable) || !strings.Contains(body, `value="10M"`) || !strings.Contains(body, `value="100M"`) {
		t.Fatalf("settings without KV = %d", rec.Code)
	}
}

func TestBudgetInputRoundTrips(t *testing.T) {
	for n, want := range map[int64]string{10_000_000: "10M", 500_000: "500k", 1_500_000: "1500000", 999_999: "999999"} {
		if got := budgetInput(n); got != want {
			t.Errorf("budgetInput(%d) = %q, want %q", n, got, want)
		}
	}
}
