package cos

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

// workFixture seeds eight tickets across every stage, two of them blocked, plus usage and timelines.
func workFixture(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	for _, task := range []Task{{Ticket: "TS-801", Outcome: "Queue Haiku briefs", Status: "queued"}, {Ticket: "TS-806", Outcome: "Retire the old summary", Status: "done"}} {
		payload, err := json.Marshal(task)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.ExecContext(ctx, `INSERT INTO tasks VALUES(?,?)`, task.Ticket, string(payload)); err != nil {
			t.Fatal(err)
		}
	}
	live := Live{At: now.Format(time.RFC3339), Errors: map[string]string{}, Fleet: []HostFleet{
		{Host: "beta", Threads: []FleetThread{
			{ThreadID: "9f1c2a7e-thread", Title: "TS-802 token report", Status: "running", Model: "claude-opus-5-5", Branch: "cos/ts-802-report"},
			{ThreadID: "4b7d0e11-thread", Title: "TS-807 deploy gate", Status: "running", Model: "claude-sonnet-5-5", Branch: "cos/ts-807-gate", NeedsInput: true},
			{ThreadID: "a0d3f9c2-thread", Title: "TS-805 cleanup", Status: "running", Model: "claude-opus-5-5", Branch: "cos/ts-805-cleanup"},
		}},
		{Host: "delta", Threads: []FleetThread{
			{ThreadID: "c3e88f02-thread", Title: "TS-802 review", Status: "running", Model: "claude-opus-5-5", Branch: "cos/ts-802-report"},
		}},
	}, PRs: []PR{
		{Repo: "TerraScale/ts-cos", Number: 43, URL: "https://git.terrascale.tech/TerraScale/ts-cos/pulls/43", Title: "TS-803 review gate", Branch: "cos/ts-803-review", Base: "main", Mergeable: true, CI: "pending"},
		{Repo: "TerraScale/ts-cos", Number: 44, URL: "https://git.terrascale.tech/TerraScale/ts-cos/pulls/44", Title: "TS-804 ready to land", Branch: "cos/ts-804-land", Base: "main", Mergeable: true, CI: "success", Ready: true},
		{Repo: "TerraScale/ts-cos", Number: 45, URL: "https://git.terrascale.tech/TerraScale/ts-cos/pulls/45", Title: "TS-808 lint", Branch: "cos/ts-808-lint", Base: "main", Mergeable: true, CI: "failure", Failing: []string{"lint"}},
	}}
	if err := s.SaveLive(ctx, live); err != nil {
		t.Fatal(err)
	}
	events := []struct {
		ticket string
		event  Event
	}{
		{"TS-802", Event{ID: "t3:802-1", Source: SourceT3, Kind: KindTurnCompleted, Time: now.Add(-5 * time.Hour), Summary: "Drafted the usage report"}},
		{"TS-802", Event{ID: "t3:802-2", Source: SourceT3, Kind: KindTurnFailed, Time: now.Add(-4 * time.Hour), Summary: "Turn failed: rate limited"}},
		{"TS-802", Event{ID: "t3:802-3", Source: SourceT3, Kind: KindReview, Time: now.Add(-3 * time.Hour), Summary: "Review asked for changes"}},
		{"TS-803", Event{ID: "forgejo:803-1", Source: SourceForgejo, Kind: KindPROpened, Time: now.Add(-6 * time.Hour), Summary: "Opened the review gate", Repo: "TerraScale/ts-cos", PR: 43}},
		{"TS-805", Event{ID: "forgejo:805-1", Source: SourceForgejo, Kind: KindPRMerged, Time: now.Add(-7 * time.Hour), Summary: "Merged the cleanup", Repo: "TerraScale/ts-cos", PR: 42}},
		{"", Event{ID: "deploy:1", Source: "deploy", Kind: kindDeployDone, Time: now.Add(-time.Hour), Summary: "ts-cos"}},
	}
	for i, e := range events {
		if err := s.insertEvent(ctx, int64(i+1), e.event, e.ticket); err != nil {
			t.Fatal(err)
		}
	}
	day := now.Format("2006-01-02")
	usage := map[string]UsageRow{
		"opus":    {Day: day, Host: "beta", Session: "s-opus", Model: "claude-opus-5-5", Ticket: "TS-802", Effort: "high", Output: 1000, Last: now.Add(-time.Hour)},
		"haiku-1": {Day: day, Host: "beta", Session: "s-h1", Model: "claude-haiku-5-5", Ticket: "TS-802", Output: 2000, First: now.Add(-2 * time.Hour), Last: now.Add(-90 * time.Minute)},
		"haiku-2": {Day: day, Host: "delta", Session: "s-h2", Model: "claude-haiku-5-5", Ticket: "TS-802", Output: 1000, First: now.Add(-2 * time.Hour), Last: now.Add(-time.Hour)},
		"ts-803":  {Day: day, Host: "delta", Session: "s-h3", Model: "claude-haiku-5-5", Ticket: "TS-803", Output: 500, First: now.Add(-time.Hour), Last: now.Add(-30 * time.Minute)},
	}
	for key, row := range usage {
		if err := s.SaveUsage(ctx, key, row); err != nil {
			t.Fatal(err)
		}
	}
}

// workRowRE pulls one table row: ticket href, data-stage and stage label.
var workRowRE = regexp.MustCompile(`(?s)<th scope="row" class="mono"><a class="row-link" href="/work/([^"]+)">[^<]+</a></th>.*?<span class="stage" data-stage="([^"]+)">([^<]+)</span>`)

// workFilterRE pulls one stage filter button: value, pressed, label and count.
var workFilterRE = regexp.MustCompile(`name="stage" value="([^"]*)" aria-pressed="(true|false)">\s*([^<]+?)\s*<span class="badge">(\d+)</span>`)

func workGet(t *testing.T, web *Web, path string) (int, string) {
	t.Helper()
	server := httptest.NewServer(ownerRoutes(web))
	defer server.Close()
	response, err := server.Client().Get(server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(body)
}

func TestWorkBoardRowsAndFilters(t *testing.T) {
	s := boardTestStore(t)
	workFixture(t, s)
	web := NewWeb(s, nil)

	status, page := workGet(t, web, "/work")
	if status != http.StatusOK {
		t.Fatalf("/work status %d", status)
	}
	rows := map[string][2]string{}
	order := []string{}
	for _, m := range workRowRE.FindAllStringSubmatch(page, -1) {
		rows[m[1]] = [2]string{m[2], m[3]}
		order = append(order, m[1])
	}
	want := map[string][2]string{
		"TS-801": {"queued", "queued"}, "TS-802": {"running", "working"}, "TS-803": {"review", "in review"},
		"TS-804": {"review", "merging"}, "TS-805": {"done", "deployed"}, "TS-806": {"done", "done"},
		"TS-807": {"blocked", "working"}, "TS-808": {"blocked", "in review"},
	}
	for ticket, w := range want {
		if rows[ticket] != w {
			t.Errorf("%s stage = %v, want %v", ticket, rows[ticket], w)
		}
	}
	if len(order) != len(want) || order[0] != "TS-807" || order[1] != "TS-808" {
		t.Fatalf("row order = %v, want the two blocked rows first", order)
	}
	if !strings.Contains(page, `<span class="tag" data-tone="danger">CI failed: lint</span>`) {
		t.Fatal("blocker tag missing")
	}

	counts := map[string]string{}
	for _, m := range workFilterRE.FindAllStringSubmatch(page, -1) {
		counts[m[1]] = m[4]
		if (m[1] == "") != (m[2] == "true") {
			t.Errorf("filter %q pressed = %s", m[1], m[2])
		}
	}
	wantCounts := map[string]string{"": "8", "queued": "1", "working": "2", "in-review": "2", "merging": "1", "deployed": "1", "done": "1"}
	for value, count := range wantCounts {
		if counts[value] != count {
			t.Errorf("filter %q count = %q, want %s (all: %v)", value, counts[value], count, counts)
		}
	}

	status, filtered := workGet(t, web, "/work?stage=in-review")
	if status != http.StatusOK {
		t.Fatalf("filtered status %d", status)
	}
	var shown []string
	for _, m := range workRowRE.FindAllStringSubmatch(filtered, -1) {
		shown = append(shown, m[1])
	}
	if strings.Join(shown, ",") != "TS-808,TS-803" {
		t.Fatalf("in-review rows = %v", shown)
	}
	if !strings.Contains(filtered, `value="in-review" aria-pressed="true"`) {
		t.Fatal("in-review filter not pressed")
	}
	if status, _ := workGet(t, web, "/work?stage=nope"); status != http.StatusBadRequest {
		t.Fatalf("unknown stage status %d", status)
	}
}

func TestWorkBoardUsageColumns(t *testing.T) {
	s := boardTestStore(t)
	workFixture(t, s)
	_, page := workGet(t, NewWeb(s, nil), "/work")
	for _, want := range []string{
		`<td class="hide-sm">opus-5-5 · high</td>`,
		`<td class="num hide-sm">75% · 2 workers</td>`,
		`<td class="num hide-sm">100% · 1 worker</td>`,
		`<td>2 · beta, delta</td>`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page missing %s", want)
		}
	}
}

func TestWorkBoardEmpty(t *testing.T) {
	web := NewWeb(boardTestStore(t), nil)
	_, page := workGet(t, web, "/work")
	if !strings.Contains(page, `<p class="empty">No work yet.</p>`) {
		t.Fatal("empty board message missing")
	}
}

func TestWorkTimelineNewestFirst(t *testing.T) {
	s := boardTestStore(t)
	workFixture(t, s)
	web := NewWeb(s, nil)

	status, page := workGet(t, web, "/work/TS-802")
	if status != http.StatusOK {
		t.Fatalf("/work/TS-802 status %d", status)
	}
	tag := regexp.MustCompile(`<span class="tag" data-tone="([a-z]*)">([a-z.]+)</span>`)
	var kinds, tones []string
	for _, m := range tag.FindAllStringSubmatch(page, -1) {
		if strings.HasPrefix(m[2], "turn.") || strings.HasPrefix(m[2], "review.") {
			kinds = append(kinds, m[2])
			tones = append(tones, m[1])
		}
	}
	if strings.Join(kinds, ",") != "review.submitted,turn.failed,turn.completed" {
		t.Fatalf("timeline kinds = %v", kinds)
	}
	if strings.Join(tones, ",") != "warn,danger," {
		t.Fatalf("timeline tones = %v", tones)
	}
	if !strings.Contains(page, ` UTC">`) {
		t.Fatal("exact UTC time missing from title")
	}

	if status, _ := workGet(t, web, "/work/ts-802"); status != http.StatusOK {
		t.Fatalf("lower-case ticket status %d", status)
	}
	if status, body := workGet(t, web, "/work/TS-999"); status != http.StatusNotFound || !strings.Contains(body, "No work for") {
		t.Fatalf("unknown ticket = %d %q", status, body)
	}
	if status, _ := workGet(t, web, "/work/nope"); status != http.StatusNotFound {
		t.Fatalf("invalid ticket status %d", status)
	}
}

func TestWorkTimelinePRLinks(t *testing.T) {
	s := boardTestStore(t)
	workFixture(t, s)
	_, page := workGet(t, NewWeb(s, nil), "/work/TS-803")
	if !strings.Contains(page, `<a href="https://git.terrascale.tech/TerraScale/ts-cos/pulls/43">TerraScale/ts-cos#43</a>`) {
		t.Fatal("open PR event not linked to its URL")
	}
	_, merged := workGet(t, NewWeb(s, nil), "/work/TS-805")
	if !strings.Contains(merged, `<span>TerraScale/ts-cos#42</span>`) {
		t.Fatal("merged PR without a live URL should be plain text")
	}
}

func TestWorkEventsStreamsTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := boardTestStore(t)
	workFixture(t, s)
	server := httptest.NewServer(ownerRoutes(NewWeb(s, nil)))
	defer server.Close()
	request, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/work/events?stage=in-review", nil)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal(response.Header)
	}
	reader := bufio.NewReader(response.Body)
	readEvent := func() string {
		t.Helper()
		var event strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			event.WriteString(line)
			if line == "\n" {
				return event.String()
			}
		}
	}
	// The header patch comes first; the table patch follows.
	table := readEvent()
	if !strings.Contains(table, "datastar-patch-elements") || !strings.Contains(table, `<header id="chat-head"`) {
		t.Fatal(table)
	}
	table = readEvent()
	if !strings.Contains(table, `<section id="work"`) {
		t.Fatal(table)
	}
	if !strings.Contains(table, `value="in-review" aria-pressed="true"`) {
		t.Fatalf("stream ignored the stage filter: %s", table)
	}
	if status, _ := workGet(t, NewWeb(s, nil), "/work/events?stage=nope"); status != http.StatusBadRequest {
		t.Fatalf("unknown stage on stream status %d", status)
	}
}

func TestBoardStageData(t *testing.T) {
	cases := []struct {
		row  BoardRow
		want string
	}{
		{BoardRow{Stage: "queued"}, "queued"},
		{BoardRow{Stage: "working"}, "running"},
		{BoardRow{Stage: "in review"}, "review"},
		{BoardRow{Stage: "merging"}, "review"},
		{BoardRow{Stage: "deployed"}, "done"},
		{BoardRow{Stage: "done"}, "done"},
		{BoardRow{Stage: "queued", Blocker: "needs you: pick a model"}, "blocked"},
		{BoardRow{Stage: "in review", Blocker: "CI failed: lint"}, "blocked"},
	}
	for _, c := range cases {
		if got := boardStageData(c.row); got != c.want {
			t.Errorf("boardStageData(%s, blocker %q) = %q, want %q", c.row.Stage, c.row.Blocker, got, c.want)
		}
	}
}

func TestBoardUsageByTicket(t *testing.T) {
	now := boardNow
	rows := []UsageRow{
		{Ticket: "TS-1", Model: "claude-opus-5-5", Effort: "high", Session: "a", Output: 1000, Last: now.Add(-3 * time.Hour)},
		{Ticket: "TS-1", Model: "claude-opus-5-5[1m]", Effort: "xhigh", Session: "b", Output: 500, Last: now.Add(-time.Hour)},
		{Ticket: "TS-1", Model: "claude-haiku-5-5", Session: "h1", Output: 1500, Last: now},
		{Ticket: "TS-2", Model: "claude-haiku-5-5", Session: "h2", Output: 40, Last: now},
		{Ticket: "", Model: "claude-opus-5-5", Output: 99, Last: now},
	}
	got := boardUsageByTicket(rows)
	if want := (boardUsage{Model: "opus-5-5 · xhigh", Haiku: 50, Workers: 1}); got["TS-1"] != want {
		t.Errorf("TS-1 usage = %+v, want %+v", got["TS-1"], want)
	}
	if want := (boardUsage{Haiku: 100, Workers: 1}); got["TS-2"] != want {
		t.Errorf("TS-2 usage = %+v, want %+v", got["TS-2"], want)
	}
	if _, ok := got[""]; ok {
		t.Errorf("rows without a ticket must not get a column: %+v", got)
	}
}

func TestBoardColumnHelpers(t *testing.T) {
	if got := threadsText(nil); got != "–" {
		t.Errorf("no threads = %q", got)
	}
	if got := threadsText([]BoardThread{{Host: "delta"}, {Host: "beta"}, {Host: "delta"}}); got != "3 · beta, delta" {
		t.Errorf("threads = %q", got)
	}
	if got := haikuText(boardUsage{}); got != "–" {
		t.Errorf("no workers = %q", got)
	}
	if got := haikuText(boardUsage{Haiku: 38, Workers: 2}); got != "38% · 2 workers" {
		t.Errorf("haiku = %q", got)
	}
	if got := clip("abcdef", 4); got != "abcd…" {
		t.Errorf("clip = %q", got)
	}
}

func TestBoardStageArg(t *testing.T) {
	for arg, want := range map[string]string{"": "", "in-review": "in review", "review": "in review", "done": "done"} {
		if got, ok := boardStageArg(arg); !ok || got != want {
			t.Errorf("boardStageArg(%q) = %q, %v; want %q", arg, got, ok, want)
		}
	}
	if _, ok := boardStageArg("bogus"); ok {
		t.Error("bogus stage accepted")
	}
}
