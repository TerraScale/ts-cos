package cos

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The e2e check trusts transcriptTotals to agree with usageScanner, so the fixtures prove that.
func TestTranscriptTotalsMatchScanner(t *testing.T) {
	const day = "2026-10-10"
	got, err := transcriptTotals("testdata/claude", day)
	if err != nil {
		t.Fatal(err)
	}
	var want usageCount
	for _, r := range usageFixtureRows(t) {
		if r.Day == day {
			want.add(r)
		}
	}
	if got != want || got.Messages == 0 {
		t.Fatalf("transcriptTotals = %+v, scanner = %+v; want equal and non-empty", got, want)
	}
}

// todayUsage skips a file last modified before today, and inside a file it keeps only today's lines.
func TestTodayUsageSkipsOlderFiles(t *testing.T) {
	now := time.Now().UTC()
	line := func(ts time.Time, out int) string {
		return fmt.Sprintf(`{"type":"assistant","timestamp":%q,"sessionId":"s","message":{"id":"msg_%d","model":"claude-opus-5-5","usage":{"input_tokens":1,"output_tokens":%d}}}`+"\n",
			ts.Format(time.RFC3339Nano), out, out)
	}
	root := t.TempDir()
	dir := filepath.Join(root, "ts-cos")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	today := filepath.Join(dir, "today.jsonl")
	yesterday := filepath.Join(dir, "yesterday.jsonl")
	if err := os.WriteFile(today, []byte(line(now, 100)+line(now.AddDate(0, 0, -1), 900)), 0o644); err != nil {
		t.Fatal(err)
	}
	// Stamped today but last written yesterday: the mtime filter must skip it.
	if err := os.WriteFile(yesterday, []byte(line(now, 5000)), 0o644); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-30 * time.Hour)
	if err := os.Chtimes(yesterday, old, old); err != nil {
		t.Fatal(err)
	}
	rows, err := todayUsage(root, "delta", now)
	if err != nil {
		t.Fatal(err)
	}
	var got usageCount
	for _, r := range rows {
		if r.Day != now.Format(time.DateOnly) {
			t.Errorf("row for %s, want only today", r.Day)
		}
		got.add(r)
	}
	want := usageCount{Messages: 1, Tokens: 101, Output: 100}
	if got != want {
		t.Fatalf("todayUsage = %+v, want %+v", got, want)
	}
}

func TestWithinTwoPercent(t *testing.T) {
	for _, c := range []struct {
		got, want int64
		ok        bool
	}{
		{0, 0, true},      // no usage on either side
		{1, 0, false},     // usage the transcripts never saw
		{0, 1000, false},  // transcripts saw usage the bucket lacks
		{980, 1000, true}, // exactly 2% short
		{1020, 1000, true},
		{1021, 1000, false},             // 2.1% over
		{979, 1000, false},              // 2.1% under
		{12_300_000, 12_560_000, false}, // 2.07% short of 12.56M
		{12_300_000, 12_545_000, true},  // 1.95% short
	} {
		if got := withinTwoPercent(c.got, c.want); got != c.ok {
			t.Errorf("withinTwoPercent(%d, %d) = %v, want %v", c.got, c.want, got, c.ok)
		}
	}
}
