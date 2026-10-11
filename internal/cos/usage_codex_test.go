package cos

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// codexDayBase is 00:00 UTC three days ago, so the fixtures sit inside the 31-day window whenever the test runs.
func codexDayBase() time.Time {
	return time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -3)
}

// codexStamp is a timestamp at clock (HH:MM:SSZ) on base's day.
func codexStamp(base time.Time, clock string) string {
	return base.Format(time.DateOnly) + "T" + clock
}

func codexMetaLine(ts, sid, cwd, branch string) string {
	git := ""
	if branch != "" {
		git = fmt.Sprintf(`,"git":{"branch":%q}`, branch)
	}
	return fmt.Sprintf(`{"timestamp":%q,"type":"session_meta","payload":{"session_id":%q,"cwd":%q%s}}`+"\n", ts, sid, cwd, git)
}

func codexTurnLine(ts, model, effort string) string {
	return fmt.Sprintf(`{"timestamp":%q,"type":"turn_context","payload":{"model":%q,"effort":%q}}`+"\n", ts, model, effort)
}

func codexCountLine(ts string, in, cached, cacheWrite, out int64) string {
	return fmt.Sprintf(`{"timestamp":%q,"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"cache_write_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":0,"total_tokens":%d}}}}`+"\n",
		ts, in, cached, cacheWrite, out, in+out)
}

func codexNullLine(ts string) string {
	return fmt.Sprintf(`{"timestamp":%q,"type":"event_msg","payload":{"type":"token_count","info":null}}`+"\n", ts)
}

// writeRollout writes one rollout file named name under root and returns its path.
func writeRollout(t *testing.T, root, name string, lines ...string) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "")), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// codexRowFor returns the one row for day and model, and fails if there is not exactly one.
func codexRowFor(t *testing.T, rows []UsageRow, day, model string) UsageRow {
	t.Helper()
	var found []UsageRow
	for _, r := range rows {
		if r.Day == day && r.Model == model {
			found = append(found, r)
		}
	}
	if len(found) != 1 {
		t.Fatalf("rows for %s %s = %d, want 1: %+v", day, model, len(found), rows)
	}
	return found[0]
}

func TestScanCodexSessionSplitsCachedInput(t *testing.T) {
	base := codexDayBase()
	path := writeRollout(t, t.TempDir(), "a.jsonl",
		codexMetaLine(codexStamp(base, "10:00:00Z"), "s1", "/home/mariogk/.t3/worktrees/ts-cos/cos-ts-826-x", "cos/ts-826-x"),
		codexTurnLine(codexStamp(base, "10:00:01Z"), "gpt-6.1-sol", "xhigh"),
		codexCountLine(codexStamp(base, "10:00:05Z"), 1000, 400, 50, 100),
	)
	rows, err := scanCodexSession(path, "delta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.Input != 600 || r.CacheRead != 400 || r.CacheWrite5m != 50 || r.Output != 100 || r.Messages != 1 {
		t.Errorf("tokens = input %d, cacheRead %d, cacheWrite %d, output %d, messages %d; want 600, 400, 50, 100, 1",
			r.Input, r.CacheRead, r.CacheWrite5m, r.Output, r.Messages)
	}
	if r.Project != "-home-mariogk--t3-worktrees-ts-cos-cos-ts-826-x" || r.Kind != "worker" || r.Worktree != "ts-cos-cos-ts-826-x" {
		t.Errorf("project = %q, kind = %q, worktree = %q", r.Project, r.Kind, r.Worktree)
	}
	if r.Session != "s1" || r.Branch != "cos/ts-826-x" || r.Ticket != "TS-826" || r.Effort != "xhigh" || r.Host != "delta" {
		t.Errorf("session = %q, branch = %q, ticket = %q, effort = %q, host = %q", r.Session, r.Branch, r.Ticket, r.Effort, r.Host)
	}
}

func TestScanCodexSessionModelSwitch(t *testing.T) {
	base := codexDayBase()
	path := writeRollout(t, t.TempDir(), "a.jsonl",
		codexMetaLine(codexStamp(base, "10:00:00Z"), "s1", "/home/mariogk/Projects/TerraScale/ts-cos", ""),
		codexTurnLine(codexStamp(base, "10:00:01Z"), "gpt-6.1-sol", "xhigh"),
		codexCountLine(codexStamp(base, "10:00:05Z"), 1000, 0, 0, 100),
		codexTurnLine(codexStamp(base, "10:02:00Z"), "gpt-6-luna", "low"),
		codexCountLine(codexStamp(base, "10:02:05Z"), 1500, 500, 0, 150),
	)
	rows, err := scanCodexSession(path, "delta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	day := base.Format(time.DateOnly)
	sol := codexRowFor(t, rows, day, "gpt-6.1-sol")
	luna := codexRowFor(t, rows, day, "gpt-6-luna")
	if sol.Input != 1000 || sol.Output != 100 || sol.Effort != "xhigh" {
		t.Errorf("sol = input %d, output %d, effort %q; want 1000, 100, xhigh", sol.Input, sol.Output, sol.Effort)
	}
	// The second event adds 500 input, all of it cached, so the uncached input is 0.
	if luna.Input != 0 || luna.CacheRead != 500 || luna.Output != 50 || luna.Effort != "low" {
		t.Errorf("luna = input %d, cacheRead %d, output %d, effort %q; want 0, 500, 50, low", luna.Input, luna.CacheRead, luna.Output, luna.Effort)
	}
}

func TestScanCodexSessionSkipsDuplicateNullAndReset(t *testing.T) {
	base := codexDayBase()
	path := writeRollout(t, t.TempDir(), "a.jsonl",
		codexMetaLine(codexStamp(base, "10:00:00Z"), "s1", "/home/mariogk/Projects/TerraScale/ts-cos", ""),
		codexTurnLine(codexStamp(base, "10:00:01Z"), "gpt-6.1-sol", "xhigh"),
		codexCountLine(codexStamp(base, "10:00:05Z"), 1000, 0, 0, 100),
		codexCountLine(codexStamp(base, "10:00:06Z"), 1000, 0, 0, 100), // duplicate: same total
		codexNullLine(codexStamp(base, "10:00:07Z")),                   // no info: no usage
		codexCountLine(codexStamp(base, "10:01:00Z"), 1200, 0, 0, 130), // adds 200 and 30
		codexCountLine(codexStamp(base, "10:05:00Z"), 300, 0, 0, 40),   // total fell: counter reset, the total is the delta
	)
	rows, err := scanCodexSession(path, "delta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := codexRowFor(t, rows, base.Format(time.DateOnly), "gpt-6.1-sol")
	if r.Input != 1500 || r.Output != 170 || r.Messages != 3 {
		t.Errorf("tokens = input %d, output %d, messages %d; want 1500, 170, 3", r.Input, r.Output, r.Messages)
	}
}

func TestScanCodexSessionDayBoundary(t *testing.T) {
	base := codexDayBase()
	next := base.AddDate(0, 0, 1)
	path := writeRollout(t, t.TempDir(), "a.jsonl",
		codexMetaLine(codexStamp(base, "10:00:00Z"), "s1", "/home/mariogk/Projects/TerraScale/ts-cos", ""),
		codexTurnLine(codexStamp(base, "10:00:01Z"), "gpt-6.1-sol", "xhigh"),
		codexCountLine(codexStamp(base, "23:59:59Z"), 1000, 0, 0, 100),
		codexCountLine(codexStamp(next, "00:00:01Z"), 1600, 0, 0, 180),
	)
	rows, err := scanCodexSession(path, "delta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	first := codexRowFor(t, rows, base.Format(time.DateOnly), "gpt-6.1-sol")
	second := codexRowFor(t, rows, next.Format(time.DateOnly), "gpt-6.1-sol")
	if first.Input != 1000 || first.Output != 100 || second.Input != 600 || second.Output != 80 {
		t.Errorf("day one = input %d, output %d; day two = input %d, output %d; want 1000, 100, 600, 80",
			first.Input, first.Output, second.Input, second.Output)
	}

	// Since starts at the second day: the first day's total still sets the baseline, so day two counts only its own delta.
	rows, err = scanCodexSession(path, "delta", next)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows since day two = %d, want 1", len(rows))
	}
	if rows[0].Input != 600 || rows[0].Output != 80 {
		t.Errorf("day two since = input %d, output %d; want 600, 80", rows[0].Input, rows[0].Output)
	}
}

func TestCodexScannerTwoSessions(t *testing.T) {
	base := codexDayBase()
	root := t.TempDir()
	writeRollout(t, root, "a.jsonl",
		codexMetaLine(codexStamp(base, "10:00:00Z"), "A", "/home/mariogk/.t3/worktrees/ts-cos/cos-ts-826-x", "cos/ts-826-x"),
		codexTurnLine(codexStamp(base, "10:00:01Z"), "gpt-6.1-sol", "xhigh"),
		codexCountLine(codexStamp(base, "10:00:05Z"), 1000, 400, 0, 100),
		codexTurnLine(codexStamp(base, "10:02:00Z"), "gpt-6-luna", "low"),
		codexCountLine(codexStamp(base, "10:02:05Z"), 1500, 500, 0, 150),
	)
	writeRollout(t, root, "b.jsonl",
		codexMetaLine(codexStamp(base, "11:00:00Z"), "B", "/home/mariogk/Projects/TerraScale/ts-cos", ""),
		codexTurnLine(codexStamp(base, "11:00:01Z"), "gpt-6.1-sol", "high"),
		codexCountLine(codexStamp(base, "11:00:05Z"), 200, 0, 50, 10),
	)
	var s codexScanner
	rows, err := s.Scan(filepath.Join(root, "missing"), "delta", time.Now())
	if err != nil || len(rows) != 0 {
		t.Fatalf("missing root = %d rows, err %v; want none and no error", len(rows), err)
	}
	rows, err = s.Scan(root, "delta", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3 (session A sol and luna, session B sol)", len(rows))
	}
	var sol, luna, b UsageRow
	for _, r := range rows {
		switch {
		case r.Session == "A" && r.Model == "gpt-6.1-sol":
			sol = r
		case r.Session == "A" && r.Model == "gpt-6-luna":
			luna = r
		case r.Session == "B":
			b = r
		}
	}
	if sol.Input != 600 || sol.CacheRead != 400 || sol.Output != 100 || sol.Ticket != "TS-826" {
		t.Errorf("session A sol = input %d, cacheRead %d, output %d, ticket %q; want 600, 400, 100, TS-826", sol.Input, sol.CacheRead, sol.Output, sol.Ticket)
	}
	// The second event adds 500 input, 100 of it newly cached, so the uncached input is 400.
	if luna.Input != 400 || luna.CacheRead != 100 || luna.Output != 50 {
		t.Errorf("session A luna = input %d, cacheRead %d, output %d; want 400, 100, 50", luna.Input, luna.CacheRead, luna.Output)
	}
	if b.Input != 200 || b.CacheWrite5m != 50 || b.Output != 10 || b.Project != "-home-mariogk-Projects-TerraScale-ts-cos" || b.Kind != "other" {
		t.Errorf("session B = %+v, want input 200, cacheWrite 50, output 10, project -home-mariogk-Projects-TerraScale-ts-cos", b)
	}
}

func TestCodexTotalsMatchScanner(t *testing.T) {
	base := codexDayBase()
	next := base.AddDate(0, 0, 1)
	day := base.Format(time.DateOnly)
	root := t.TempDir()
	writeRollout(t, root, "a.jsonl",
		codexMetaLine(codexStamp(base, "10:00:00Z"), "A", "/home/mariogk/.t3/worktrees/ts-cos/cos-ts-826-x", "cos/ts-826-x"),
		codexTurnLine(codexStamp(base, "10:00:01Z"), "gpt-6.1-sol", "xhigh"),
		codexCountLine(codexStamp(base, "10:00:05Z"), 1000, 400, 50, 100),
		codexCountLine(codexStamp(base, "10:00:06Z"), 1000, 400, 50, 100), // duplicate
		codexNullLine(codexStamp(base, "10:00:07Z")),
		codexCountLine(codexStamp(base, "23:59:59Z"), 2000, 900, 60, 220),
		codexCountLine(codexStamp(next, "00:00:01Z"), 2600, 1000, 60, 260), // day two, not counted for day
	)
	writeRollout(t, root, "b.jsonl",
		codexMetaLine(codexStamp(base, "11:00:00Z"), "B", "/home/mariogk/Projects/TerraScale/ts-cos", ""),
		codexTurnLine(codexStamp(base, "11:00:01Z"), "gpt-6-luna", "low"),
		codexCountLine(codexStamp(base, "11:00:05Z"), 300, 0, 0, 30),
		codexCountLine(codexStamp(base, "11:00:09Z"), 100, 0, 0, 5), // reset: the total is the delta
	)
	var s codexScanner
	rows, err := s.Scan(root, "delta", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var want usageCount
	for _, r := range rows {
		if r.Day == day {
			want = want.plus(usageCount{Messages: r.Messages, Tokens: r.Input + r.CacheWrite5m + r.CacheWrite1h + r.Output, Output: r.Output, CacheRead: r.CacheRead})
		}
	}
	got, err := codexTotals(root, day)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || got.Messages == 0 {
		t.Fatalf("codexTotals = %+v, scanner = %+v; want equal and non-empty", got, want)
	}
}

// codexCallLine is a custom_tool_call, the shape of codex's exec tool, whose input is the JS source.
func codexCallLine(ts, callID, input string) string {
	return fmt.Sprintf(`{"timestamp":%q,"type":"response_item","payload":{"type":"custom_tool_call","call_id":%q,"name":"exec","input":%q}}`+"\n", ts, callID, input)
}

// codexOutputLine is a custom_tool_call_output whose output is a list of input_text blocks.
func codexOutputLine(ts, callID, text string) string {
	return fmt.Sprintf(`{"timestamp":%q,"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":%q,"output":[{"type":"input_text","text":%q}]}}`+"\n", ts, callID, text)
}

// codexReworkStart is a session's first lines. Each rework test then adds a rework call, its output and a token_count
// whose delta over this start is 300 uncached input, 50 cache writes and 60 output, so usageTokens counts 410.
func codexReworkStart(base time.Time) []string {
	return []string{
		codexMetaLine(codexStamp(base, "10:00:00Z"), "s1", "/home/mariogk/.t3/worktrees/ts-cos/cos-ts-826-x", "cos/ts-826-x"),
		codexTurnLine(codexStamp(base, "10:00:01Z"), "gpt-6.1-sol", "xhigh"),
		codexCountLine(codexStamp(base, "10:00:02Z"), 1000, 400, 0, 100),
	}
}

func TestScanCodexSessionCountsGateReaction(t *testing.T) {
	base := codexDayBase()
	lines := codexReworkStart(base)
	lines = append(lines,
		codexCallLine(codexStamp(base, "10:00:03Z"), "c1", `tools.exec_command({cmd: "sh .cos/gate.sh 12 cos/x"})`),
		codexOutputLine(codexStamp(base, "10:00:04Z"), "c1", "LOCAL GATE FAIL: go test ./internal/cos"),
		codexCountLine(codexStamp(base, "10:00:05Z"), 1500, 600, 50, 160),
	)
	rows, err := scanCodexSession(writeRollout(t, t.TempDir(), "a.jsonl", lines...), "delta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := codexRowFor(t, rows, base.Format(time.DateOnly), "gpt-6.1-sol")
	if r.Waste["gate"] != 410 || len(r.Waste) != 1 {
		t.Fatalf("waste = %v, want gate 410 only", r.Waste)
	}
}

func TestScanCodexSessionCountsRebaseReaction(t *testing.T) {
	base := codexDayBase()
	lines := codexReworkStart(base)
	lines = append(lines,
		codexCallLine(codexStamp(base, "10:00:03Z"), "c1", `tools.exec_command({cmd: "git rebase origin/main"})`),
		codexOutputLine(codexStamp(base, "10:00:04Z"), "c1", "CONFLICT (content): Merge conflict in x.go"),
		codexCountLine(codexStamp(base, "10:00:05Z"), 1500, 600, 50, 160),
	)
	rows, err := scanCodexSession(writeRollout(t, t.TempDir(), "a.jsonl", lines...), "delta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := codexRowFor(t, rows, base.Format(time.DateOnly), "gpt-6.1-sol")
	if r.Waste["rebase"] != 410 || len(r.Waste) != 1 {
		t.Fatalf("waste = %v, want rebase 410 only", r.Waste)
	}
}

func TestScanCodexSessionIgnoresPassingGate(t *testing.T) {
	base := codexDayBase()
	lines := codexReworkStart(base)
	lines = append(lines,
		codexCallLine(codexStamp(base, "10:00:03Z"), "c1", `tools.exec_command({cmd: "sh .cos/gate.sh 12 cos/x"})`),
		codexOutputLine(codexStamp(base, "10:00:04Z"), "c1", "#12 MERGED"),
		codexCountLine(codexStamp(base, "10:00:05Z"), 1500, 600, 50, 160),
	)
	rows, err := scanCodexSession(writeRollout(t, t.TempDir(), "a.jsonl", lines...), "delta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := codexRowFor(t, rows, base.Format(time.DateOnly), "gpt-6.1-sol")
	if len(r.Waste) != 0 {
		t.Fatalf("waste = %v, want none", r.Waste)
	}
}

func TestScanCodexSessionDuplicateKeepsPendingRework(t *testing.T) {
	base := codexDayBase()
	lines := codexReworkStart(base)
	lines = append(lines,
		codexCallLine(codexStamp(base, "10:00:03Z"), "c1", `tools.exec_command({cmd: "sh .cos/gate.sh 12 cos/x"})`),
		codexOutputLine(codexStamp(base, "10:00:04Z"), "c1", "NOT MERGED: ci failing"),
		codexCountLine(codexStamp(base, "10:00:04Z"), 1000, 400, 0, 100), // duplicate: zero delta
		codexCountLine(codexStamp(base, "10:00:05Z"), 1500, 600, 50, 160),
	)
	rows, err := scanCodexSession(writeRollout(t, t.TempDir(), "a.jsonl", lines...), "delta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := codexRowFor(t, rows, base.Format(time.DateOnly), "gpt-6.1-sol")
	if r.Waste["gate"] != 410 || len(r.Waste) != 1 {
		t.Fatalf("waste = %v, want gate 410 only", r.Waste)
	}
}
