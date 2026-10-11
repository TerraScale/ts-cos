package cos

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	usageWorkerProject = "-home-mariogk--t3-worktrees-ts-cos-cos-ts-826-work-tokens"
	usageCOSProject    = "-home-mariogk--local-share-cos-agent"
)

func usageFixtureRows(t *testing.T) []UsageRow {
	t.Helper()
	var s usageScanner
	rows, err := s.Scan("testdata/claude", "delta", time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestUsageTranscriptCountsEachMessageOnce(t *testing.T) {
	since := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	rows, err := scanTranscript(filepath.Join("testdata/claude", usageWorkerProject, "opus-main.jsonl"), usageWorkerProject, "delta", since)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1: %+v", len(rows), rows)
	}
	r := rows[0]
	if r.Output != 100 || r.Input != 10 || r.CacheRead != 1000 || r.CacheWrite5m != 50 || r.CacheWrite1h != 0 || r.Messages != 1 {
		t.Fatalf("usage = %+v; want output 100 (max of the three lines), synthetic, old and unterminated lines ignored", r)
	}
	if r.Model != "claude-opus-5-5" || r.Effort != "high" || r.Branch != "cos/ts-826-work-tokens" || r.Ticket != "TS-826" ||
		r.Kind != "worker" || r.Worktree != "ts-cos-cos-ts-826-work-tokens" || r.Session != "opus-main" || r.Day != "2026-10-10" {
		t.Fatalf("labels = %+v", r)
	}
	wantCost := (10*4 + 100*20 + 50*5 + 1000*0.20) / 1e6
	if math.Abs(r.Cost()-wantCost) > 1e-12 {
		t.Fatalf("cost = %v, want %v", r.Cost(), wantCost)
	}
}

func TestUsageScanTotalsByKindAndModel(t *testing.T) {
	rows := usageFixtureRows(t)
	byKind := map[string]UsageTotal{}
	for _, g := range SummarizeUsage(rows, Live{}, "kind") {
		byKind[g.Key] = g
	}
	if byKind["worker"].Output != 200 || byKind["cos"].Output != 200 {
		t.Fatalf("kind totals = %+v", byKind)
	}
	byModel := map[string]UsageTotal{}
	for _, g := range SummarizeUsage(rows, Live{}, "model") {
		byModel[g.Key] = g
	}
	if byModel["claude-opus-5-5"].Output != 300 || byModel["claude-haiku-5-5"].Output != 100 {
		t.Fatalf("model totals = %+v; want dated Haiku IDs grouped under claude-haiku-5-5", byModel)
	}
}

func TestUsageSummaryByThreadUsesFleetTitleAndHaikuParallelism(t *testing.T) {
	live := Live{Fleet: []HostFleet{{Host: "delta", Threads: []FleetThread{{ThreadID: "t-826", Title: "TS-826 tokens", Branch: "cos/ts-826-work-tokens"}}}}}
	byKey := map[string]UsageTotal{}
	for _, g := range SummarizeUsage(usageFixtureRows(t), live, "thread") {
		byKey[g.Key] = g
	}
	worker := byKey["delta/"+usageWorkerProject]
	if worker.Thread != "t-826" || worker.Label != "TS-826 tokens" || worker.Ticket != "TS-826" || worker.Host != "delta" {
		t.Fatalf("worker thread = %+v", worker)
	}
	// haiku-a 10:00-10:10 and haiku-b 10:05-10:20 overlap; haiku-c runs alone at 12:00.
	if worker.Workers != 3 || worker.Parallel != 2 || worker.HaikuShare != 50 || worker.Output != 200 || worker.AdvisorOutput != 100 || worker.WorkerOutput != 100 {
		t.Fatalf("worker totals = %+v", worker)
	}
	if got := byKey["delta/"+usageCOSProject].Label; got != "COS agent" {
		t.Fatalf("cos label = %q", got)
	}
}

func TestUsageParallelEndAtStartDoesNotOverlap(t *testing.T) {
	at := time.Date(2026, 10, 10, 10, 5, 0, 0, time.UTC)
	haiku := func(session string, first, last time.Time) UsageRow {
		return UsageRow{Session: session, Model: "claude-haiku-5-5", First: first, Last: last}
	}
	rows := []UsageRow{
		haiku("a", at.Add(-5*time.Minute), at),
		haiku("b", at, at.Add(5*time.Minute)),
	}
	if got := usageTotal("x", rows).Parallel; got != 1 {
		t.Fatalf("parallel = %d, want 1 when one session ends as the other starts", got)
	}
}

func TestUsageModelAndCost(t *testing.T) {
	for in, want := range map[string]string{
		"claude-opus-5-5[1m]":       "claude-opus-5-5",
		"claude-haiku-5-5-20261001": "claude-haiku-5-5",
		"claude-haiku-5-5":          "claude-haiku-5-5",
	} {
		if got := usageModel(in); got != want {
			t.Fatalf("usageModel(%q) = %q, want %q", in, got, want)
		}
	}
	if got := (UsageRow{Model: "claude-opus-5-5[1m]", Output: 1_000_000}).Cost(); got != 20 {
		t.Fatalf("1M Opus output cost = %v, want 20", got)
	}
	if got := (UsageRow{Model: "claude-unknown", Output: 1_000_000}).Cost(); got != 0 {
		t.Fatalf("unknown model cost = %v, want 0", got)
	}
}

func TestUsageKeyUsesKVCharacters(t *testing.T) {
	key := usageKey(UsageRow{Day: "2026-10-11", Host: "mac book:x", Project: "p", Session: "s", Model: "m"})
	if !regexp.MustCompile(`^[-/_=.a-zA-Z0-9]+$`).MatchString(key) {
		t.Fatalf("key %q has characters KV does not allow", key)
	}
	if !strings.HasPrefix(key, "20261011.mac_book_x.") || len(key) != len("20261011.mac_book_x.")+16 {
		t.Fatalf("key = %q", key)
	}
}

func TestUsageScannerReusesUnchangedTranscripts(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "-home-mariogk-proj")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "s.jsonl")
	line := `{"type":"assistant","timestamp":"2026-10-10T09:00:00.000Z","sessionId":"s","message":{"id":"m","model":"claude-opus-5-5","usage":{"input_tokens":1,"output_tokens":%03d}}}` + "\n"
	if err := os.WriteFile(path, fmt.Appendf(nil, line, 100), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	var s usageScanner
	first, err := s.Scan(root, "delta", now)
	if err != nil || len(first) != 1 || first[0].Output != 100 {
		t.Fatalf("first scan = %+v, %v", first, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Same size and mtime with different content: only the cache can give the old answer.
	if err := os.WriteFile(path, fmt.Appendf(nil, line, 900), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	second, err := s.Scan(root, "delta", now)
	if err != nil || len(second) != 1 || second[0].Output != 100 {
		t.Fatalf("second scan = %+v, %v; want the cached output 100", second, err)
	}
}

func TestUsageStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	row := UsageRow{Day: "2026-10-10", Host: "delta", Project: "p", Session: "s1", Model: "claude-opus-5-5", Output: 5}
	if err := s.SaveUsage(ctx, "k1", row); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveUsage(ctx, "k2", UsageRow{Day: "2026-09-01", Host: "delta", Output: 7}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Usage(ctx, "2026-10-01")
	if err != nil || len(rows) != 1 || rows[0].Output != 5 {
		t.Fatalf("usage after save = %+v, %v", rows, err)
	}
	row.Output = 9
	if err := s.SaveUsage(ctx, "k1", row); err != nil {
		t.Fatal(err)
	}
	if err := s.PruneUsage(ctx, map[string]bool{"k1": true}); err != nil {
		t.Fatal(err)
	}
	rows, err = s.Usage(ctx, "0000-01-01")
	if err != nil || len(rows) != 1 || rows[0].Output != 9 {
		t.Fatalf("usage after update and prune = %+v, %v", rows, err)
	}
}
