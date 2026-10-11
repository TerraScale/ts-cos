package cos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTokensHaikuShare(t *testing.T) {
	const kpi = `<span class="kpi-label">Haiku share</span>`
	for _, tc := range []struct {
		name    string
		body    string
		want    int
		wantErr bool
	}{
		{"ok", kpi + "\n\t\t<span class=\"kpi-value\">42%</span>", 42, false},
		{"missing kpi", `<span class="kpi-label">Tokens</span><span class="kpi-value">42%</span>`, 0, true},
		{"missing value", kpi, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tokensHaikuShare(tc.body)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("tokensHaikuShare() = %d, %v; want %d, error %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestTokensHaikuSharePageRenders(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	row := UsageRow{Day: now.Format("2006-01-02"), Host: "delta", Project: "-home-mariogk-local-share-cos-agent", Kind: "cos", Session: "cos-1",
		Model: "claude-opus-5-5", Input: 1e6, Output: 2e5, Messages: 10, First: now, Last: now}
	if err := store.SaveUsage(context.Background(), usageKey(row), row); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRecorder()
	ownerRoutes(NewWeb(store, nil)).ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/tokens?w=7d", nil))
	if r.Code != http.StatusOK {
		t.Fatalf("GET /tokens?w=7d = %d", r.Code)
	}
	if _, err := tokensHaikuShare(r.Body.String()); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerToday(t *testing.T) {
	const today = "2026-10-11"
	for _, tc := range []struct {
		name      string
		out       string
		want      int
		wantKinds int
		wantErr   bool
	}{
		{"ok", `{"days":[{"day":"2026-10-11","kinds":{"dedup":{"events":2,"tokens":300},"retrieval":{"events":1,"tokens":50}},"avoided":350,"wallSeconds":0},{"day":"2026-10-10","kinds":{},"avoided":0,"wallSeconds":0}]}`, 350, 2, false},
		{"today missing", `{"days":[{"day":"2026-10-10","kinds":{},"avoided":0,"wallSeconds":0}]}`, 0, 0, true},
		{"bad JSON", `savings: not json`, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, kinds, err := ledgerToday(tc.out, today)
			if (err != nil) != tc.wantErr || got != tc.want || kinds != tc.wantKinds {
				t.Fatalf("ledgerToday() = %d, %d, %v; want %d, %d, error %v", got, kinds, err, tc.want, tc.wantKinds, tc.wantErr)
			}
		})
	}
	if _, _, err := ledgerToday(strings.Repeat("x", 200), today); err == nil || len(err.Error()) > 200 {
		t.Fatalf("bad JSON error = %v; want a clipped message", err)
	}
}
