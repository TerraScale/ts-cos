package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
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

func TestUsageThreadPrefersTopLevelAdvisorOverChild(t *testing.T) {
	type lineage = struct {
		Parent   string `json:"parent"`
		Relation string `json:"rel"`
	}
	row := UsageRow{Host: "beta", Branch: "cos/ts-826-x", Project: "proj"}
	child := FleetThread{ThreadID: "thread:delegated-task:1", Title: "child", Branch: "cos/ts-826-x", Lineage: &lineage{Parent: "root"}}
	root := FleetThread{ThreadID: "root", Title: "TS-826 advisor", Branch: "cos/ts-826-x"}

	// The child is listed first; the top-level advisor still owns the group.
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{child, root}}}}
	t1 := UsageTotal{Key: "beta/proj"}
	usageThread(&t1, row, live)
	if t1.Thread != "root" || t1.Label != "TS-826 advisor" {
		t.Fatalf("group thread = %q %q, want root", t1.Thread, t1.Label)
	}

	// With only the child live, the group still resolves to it.
	t2 := UsageTotal{Key: "beta/proj"}
	usageThread(&t2, row, Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{child}}}})
	if t2.Thread != child.ThreadID {
		t.Fatalf("group thread = %q, want child %q", t2.Thread, child.ThreadID)
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

func TestUsageModelNames(t *testing.T) {
	for in, want := range map[string]string{
		"claude-opus-5-5[1m]":       "claude-opus-5-5",
		"claude-haiku-5-5-20261001": "claude-haiku-5-5",
		"claude-haiku-5-5":          "claude-haiku-5-5",
	} {
		if got := usageModel(in); got != want {
			t.Fatalf("usageModel(%q) = %q, want %q", in, got, want)
		}
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

// fakeUsageKV fails Put with failErr while failPut is positive. Get reports every key
// as absent, so each scan decides only from last.
type fakeUsageKV struct {
	jetstream.KeyValue
	failPut  int
	failErr  error
	onPut    func()
	attempts map[string]int
	stored   map[string][]byte
}

func newFakeUsageKV() *fakeUsageKV {
	return &fakeUsageKV{failErr: context.DeadlineExceeded, attempts: map[string]int{}, stored: map[string][]byte{}}
}

func (f *fakeUsageKV) Get(context.Context, string) (jetstream.KeyValueEntry, error) {
	return nil, jetstream.ErrKeyNotFound
}

func (f *fakeUsageKV) Put(_ context.Context, key string, value []byte) (uint64, error) {
	f.attempts[key]++
	if f.onPut != nil {
		f.onPut()
	}
	if f.failPut > 0 {
		f.failPut--
		return 0, f.failErr
	}
	f.stored[key] = value
	return 1, nil
}

func usageTestRow(session string, output int64) UsageRow {
	return UsageRow{Day: "2026-10-11", Host: "delta", Project: "p", Session: session, Model: "claude-opus-5-5", Output: output}
}

func shortUsageBackoff(t *testing.T) {
	t.Helper()
	old := usageBackoff
	usageBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { usageBackoff = old })
}

func TestPublishUsageRetriesTransientPut(t *testing.T) {
	shortUsageBackoff(t)
	ctx := context.Background()
	kv := newFakeUsageKV()
	kv.failPut = 2
	last := map[string][]byte{}
	row := usageTestRow("a", 1)
	key := usageKey(row)
	n, err := publishUsage(ctx, kv, []UsageRow{row}, last)
	if err != nil || n != 1 || kv.attempts[key] != 3 || len(kv.stored) != 1 || len(last) != 1 {
		t.Fatalf("n=%d err=%v attempts=%d stored=%d last=%d; want the third attempt to publish in one scan", n, err, kv.attempts[key], len(kv.stored), len(last))
	}
}

func TestPublishUsageStopsScanAfterExhaustedKey(t *testing.T) {
	shortUsageBackoff(t)
	ctx := context.Background()
	kv := newFakeUsageKV()
	kv.failPut = 10
	last := map[string][]byte{}
	rows := []UsageRow{usageTestRow("a", 1), usageTestRow("b", 1), usageTestRow("c", 1)}
	keys := []string{usageKey(rows[0]), usageKey(rows[1]), usageKey(rows[2])}
	slices.Sort(keys)
	n, err := publishUsage(ctx, kv, rows, last)
	if !errors.Is(err, context.DeadlineExceeded) || n != 0 {
		t.Fatalf("n=%d err=%v; want 0 and a deadline error", n, err)
	}
	if kv.attempts[keys[0]] != len(usageBackoff)+1 || kv.attempts[keys[1]] != 0 || kv.attempts[keys[2]] != 0 {
		t.Fatalf("attempts = %v; want the first key retried and the rest untouched", kv.attempts)
	}
	if len(last) != 0 || len(kv.stored) != 0 {
		t.Fatalf("last = %v, stored = %d; want nothing recorded after a failed key", last, len(kv.stored))
	}
	kv.failPut = 0
	n, err = publishUsage(ctx, kv, rows, last)
	if err != nil || n != 3 || len(last) != 3 || len(kv.stored) != 3 {
		t.Fatalf("next scan n=%d err=%v last=%d stored=%d; want all three published", n, err, len(last), len(kv.stored))
	}
}

func TestPublishUsageNonTransientErrorIsNotRetried(t *testing.T) {
	shortUsageBackoff(t)
	kv := newFakeUsageKV()
	kv.failPut = 1
	kv.failErr = errors.New("bad row")
	last := map[string][]byte{}
	row := usageTestRow("a", 1)
	if _, err := publishUsage(context.Background(), kv, []UsageRow{row}, last); err != nil {
		t.Fatalf("err = %v; want the row error logged and skipped", err)
	}
	if got := kv.attempts[usageKey(row)]; got != 1 || len(last) != 0 {
		t.Fatalf("attempts = %d, last = %d; want one attempt and nothing recorded", got, len(last))
	}
}

// Shutdown during a Put returns the context error and logs nothing at ERROR. Logs are
// captured by swapping the default slog handler, since the error path logs via slog.
func TestPublishUsageShutdownIsNotAnError(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv := newFakeUsageKV()
	kv.failPut = 1
	kv.failErr = context.Canceled
	kv.onPut = cancel
	last := map[string][]byte{}
	n, err := publishUsage(ctx, kv, []UsageRow{usageTestRow("a", 1)}, last)
	if !errors.Is(err, context.Canceled) || n != 0 || len(last) != 0 {
		t.Fatalf("n=%d err=%v last=%d; want 0, context.Canceled and nothing recorded", n, err, len(last))
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("shutdown logged an error: %s", logs.String())
	}
}

func TestPublishUsageWritesDuplicateKeyOnce(t *testing.T) {
	ctx := context.Background()
	kv := newFakeUsageKV()
	first, second := usageTestRow("a", 1), usageTestRow("a", 2)
	if _, err := publishUsage(ctx, kv, []UsageRow{first, second}, map[string][]byte{}); err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	key := usageKey(first)
	if kv.attempts[key] != 1 || !bytes.Equal(kv.stored[key], want) {
		t.Fatalf("attempts = %d, stored = %s; want one write of the last row", kv.attempts[key], kv.stored[key])
	}
}

// usageTestMsg is an assistant line for message id with one content block.
func usageTestMsg(id string, input, output int64, content string) string {
	return fmt.Sprintf(`{"type":"assistant","timestamp":"2026-10-10T10:00:00Z","sessionId":"s1","message":{"id":%q,"model":"claude-opus-5-5","content":[%s],"usage":{"input_tokens":%d,"output_tokens":%d,"cache_read_input_tokens":1000,"cache_creation_input_tokens":0}}}`, id, content, input, output)
}

// usageTestResult is a user line with one tool_result; content is raw JSON.
func usageTestResult(id, content string) string {
	return fmt.Sprintf(`{"type":"user","timestamp":"2026-10-10T10:00:01Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":%q,"content":%s}]}}`, id, content)
}

func usageTestBash(id, command string) string {
	cmd, _ := json.Marshal(command)
	return fmt.Sprintf(`{"type":"tool_use","id":%q,"name":"Bash","input":{"command":%s}}`, id, cmd)
}

const usageTestText = `{"type":"text","text":"ok"}`

func TestUsageWasteCountsTheTurnThatReactsToRework(t *testing.T) {
	gate := usageTestBash("t1", "bash .cos/gate.sh 12 cos/x")
	cases := []struct {
		name  string
		lines []string
		want  map[string]int64
	}{
		{"gate failure", []string{
			usageTestMsg("m1", 10, 5, gate),
			usageTestResult("t1", `"LOCAL GATE FAIL: go test"`),
			usageTestMsg("m2", 30, 7, usageTestText),
		}, map[string]int64{"gate": 37}},
		{"gate failure in a text list", []string{
			usageTestMsg("m1", 10, 5, gate),
			usageTestResult("t1", `[{"type":"text","text":"NOT MERGED #12"}]`),
			usageTestMsg("m2", 30, 7, usageTestText),
		}, map[string]int64{"gate": 37}},
		{"successful gate", []string{
			usageTestMsg("m1", 10, 5, gate),
			usageTestResult("t1", `"#12 MERGED"`),
			usageTestMsg("m2", 30, 7, usageTestText),
		}, nil},
		{"reading the gate script", []string{
			usageTestMsg("m1", 10, 5, usageTestBash("t1", "cat /home/x/.cos/gate.sh")),
			usageTestResult("t1", `"LOCAL GATE FAIL: go test"`),
			usageTestMsg("m2", 30, 7, usageTestText),
		}, nil},
		{"grep output quoting a failure", []string{
			usageTestMsg("m1", 10, 5, `{"type":"tool_use","id":"t1","name":"Grep","input":{"pattern":"LOCAL GATE FAIL"}}`),
			usageTestResult("t1", `"usage.go:127: LOCAL GATE FAIL"`),
			usageTestMsg("m2", 30, 7, usageTestText),
		}, nil},
		{"gate and rebase in one command", []string{
			usageTestMsg("m1", 10, 5, usageTestBash("t1", "git rebase origin/main && bash .cos/gate.sh 12 cos/x")),
			usageTestResult("t1", `"LOCAL GATE FAIL: go test"`),
			usageTestMsg("m2", 30, 7, usageTestText),
		}, map[string]int64{"gate": 37}},
		{"git rebase", []string{
			usageTestMsg("m1", 10, 5, usageTestBash("t1", "git rebase origin/main")),
			usageTestResult("t1", `"Successfully rebased"`),
			usageTestMsg("m2", 30, 7, usageTestText),
		}, map[string]int64{"rebase": 37}},
		{"API retry", []string{
			usageTestMsg("m1", 10, 5, usageTestText),
			`{"type":"assistant","isApiErrorMessage":true,"timestamp":"2026-10-10T10:00:01Z","sessionId":"s1","message":{"id":"e1","model":"<synthetic>","content":[{"type":"text","text":"API Error"}],"usage":{"input_tokens":0,"output_tokens":0}}}`,
			usageTestMsg("m2", 30, 7, usageTestText),
		}, map[string]int64{"retry": 30}},
		{"streamed reacting message", []string{
			usageTestMsg("m1", 10, 5, gate),
			usageTestResult("t1", `"LOCAL GATE FAIL"`),
			usageTestMsg("m2", 30, 3, usageTestText),
			usageTestMsg("m2", 30, 7, usageTestText),
		}, map[string]int64{"gate": 37}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "s.jsonl")
			if err := os.WriteFile(path, []byte(strings.Join(c.lines, "\n")+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			rows, err := scanTranscript(path, usageWorkerProject, "delta", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || !maps.Equal(rows[0].Waste, c.want) {
				t.Fatalf("waste = %+v; want %v", rows, c.want)
			}
			totals := SummarizeUsage(rows, Live{}, "kind")
			if len(totals) != 1 || !maps.Equal(totals[0].Waste, c.want) {
				t.Fatalf("total waste = %+v; want %v", totals, c.want)
			}
		})
	}
}
