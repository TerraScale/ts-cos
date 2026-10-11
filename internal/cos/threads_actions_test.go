package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// fakeBus answers the bus verbs and the merge seam, and records the calls in order.
type fakeBus struct {
	work     threadWorktree
	workErr  error
	skipped  []map[string]string
	mergeErr error
	calls    []string
	order    MergeOrder
}

func (f *fakeBus) bus(_ context.Context, _, command string, _ []string, out any) error {
	f.calls = append(f.calls, command)
	switch command {
	case "unpushed":
		if f.workErr != nil {
			return f.workErr
		}
		*out.(*threadWorktree) = f.work
	case "close", "delete":
		*out.(*TreeResult) = TreeResult{Skipped: f.skipped}
	}
	return nil
}

func (f *fakeBus) merge(_ context.Context, _ string, _ int, order MergeOrder) (any, error) {
	f.calls = append(f.calls, "merge")
	f.order = order
	if f.mergeErr != nil {
		return nil, f.mergeErr
	}
	return map[string]any{"sha": "abc123"}, nil
}

func openThreadPR() *panelPR {
	return &panelPR{URL: "https://git.example/terrascale/ts-cos/pulls/12", Repo: "terrascale/ts-cos", Number: 12, State: "open", Mergeable: true}
}

func TestThreadOps(t *testing.T) {
	tab := ChatTab{ID: "t1", Name: "Feature X"}
	row := func(pr *panelPR) panelThread {
		return panelThread{Host: "delta", ThreadID: "worker", Title: "worker", PR: pr}
	}
	mergedPR := openThreadPR()
	mergedPR.State = "merged"
	for _, tc := range []struct {
		name string
		op   string
		row  panelThread
		fake fakeBus
		want string // a substring of the outcome
		ok   bool
		// calls are the bus verbs and merge seam calls, in order.
		calls string
	}{
		{"settle ok", "settle", row(nil), fakeBus{}, "Settled 'worker'", true, "close"},
		{"settle while running", "settle", row(nil), fakeBus{skipped: []map[string]string{{"threadId": "x"}}}, "Not settled: 'worker' is still running", false, "close"},
		{"merge without open PR", "merge", row(nil), fakeBus{}, "Not merged: no open PR", false, ""},
		{"delete dirty", "delete", row(nil), fakeBus{work: threadWorktree{Dirty: true}}, "Not deleted: 'worker' has uncommitted changes", false, "unpushed"},
		{"delete worktree gone", "delete", row(nil), fakeBus{work: threadWorktree{Missing: true}}, "Not deleted: its worktree is gone; check its branch by hand", false, "unpushed"},
		{"delete ahead without merge", "delete", row(nil), fakeBus{work: threadWorktree{Ahead: 2}}, "Not deleted: 'worker' has 2 commits no remote has", false, "unpushed"},
		{"delete ahead with merged PR", "delete", row(mergedPR), fakeBus{work: threadWorktree{Ahead: 2}}, "Deleted 'worker'", true, "unpushed delete"},
		{"delete check fails", "delete", row(nil), fakeBus{workErr: context.DeadlineExceeded}, "Not deleted: could not check 'worker' for unpushed work", false, "unpushed"},
		{"delete ok", "delete", row(nil), fakeBus{}, "Deleted 'worker'", true, "unpushed delete"},
		{"delete while running", "delete", row(nil), fakeBus{skipped: []map[string]string{{"threadId": "x"}}}, "Not deleted: 'worker' is still running", false, "unpushed delete"},
		{"all without open PR", "all", row(nil), fakeBus{}, "Not done: no open PR", false, ""},
		{"all when not mergeable", "all", row(&panelPR{URL: "u", State: "open"}), fakeBus{}, "Not done: the PR is not mergeable", false, ""},
		{"all with unpushed commits", "all", row(openThreadPR()), fakeBus{work: threadWorktree{Ahead: 2}}, "Not done: 'worker' has 2 commits no remote has", false, "unpushed"},
		{"all happy path", "all", row(openThreadPR()), fakeBus{}, "Merged terrascale/ts-cos#12", true, "unpushed merge close delete"},
		{"all stops at failed merge", "all", row(openThreadPR()), fakeBus{mergeErr: context.Canceled}, "Not merged: context canceled", false, "unpushed merge"},
		{"all stops at running thread", "all", row(openThreadPR()), fakeBus{skipped: []map[string]string{{"threadId": "x"}}}, "; not settled: 'worker' is still running", false, "unpushed merge close"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.fake
			w := &Web{call: f.bus, merge: f.merge}
			outcome, ok := w.runThreadOp(context.Background(), tab, tc.row, tc.op)
			if !strings.Contains(outcome, tc.want) || ok != tc.ok {
				t.Fatalf("outcome %q ok=%v, want %q ok=%v", outcome, ok, tc.want, tc.ok)
			}
			if got := strings.Join(f.calls, " "); got != tc.calls {
				t.Fatalf("calls %q, want %q", got, tc.calls)
			}
		})
	}
}

func TestThreadMergeOrderIsOneLineAndShort(t *testing.T) {
	long := ChatTab{ID: "t1", Name: strings.Repeat("ü", chatTabNameMax)}
	f := fakeBus{}
	w := &Web{call: f.bus, merge: f.merge}
	row := panelThread{Host: "delta", ThreadID: "worker", Title: "worker", PR: openThreadPR()}
	w.runThreadOp(context.Background(), long, row, "merge")
	if err := f.order.Validate(); err != nil {
		t.Fatal(err)
	}
	if n := utf8.RuneCountInString(f.order.Reason); n > 200 || strings.ContainsAny(f.order.Reason, "\n\r") || !f.order.FullSend {
		t.Fatalf("merge order = %+v (%d runes)", f.order, n)
	}
}

func TestRunStepsStopsAtFirstFailure(t *testing.T) {
	var ran []string
	step := func(name string, err error) threadStep {
		return threadStep{verb: name, run: func(context.Context) (string, error) {
			ran = append(ran, name)
			if err != nil {
				return "", err
			}
			return name + " ok", nil
		}}
	}
	out, ok := runSteps(context.Background(), []threadStep{step("merged", nil), step("settled", context.Canceled), step("deleted", nil)})
	if ok || out != "Merged ok; not settled: context canceled" || strings.Join(ran, " ") != "merged settled" {
		t.Fatalf("%q %v %v", out, ok, ran)
	}
	if out, ok := runSteps(context.Background(), []threadStep{step("merged", nil), step("settled", nil)}); !ok || out != "Merged ok, settled ok" {
		t.Fatalf("%q %v", out, ok)
	}
}

func TestConfirmActionEscapesValues(t *testing.T) {
	row := panelThread{Host: "delta", ThreadID: "w1", Title: `it's "quoted" </script>`, PR: openThreadPR()}
	out := confirmAction("delete", row)
	for _, want := range []string{`$actOp="delete"`, `$actHost="delta"`, `$actThread="w1"`, `\"quoted\" \u003c/script\u003e`} {
		if !strings.Contains(out, want) {
			t.Fatal(out)
		}
	}
	if !strings.Contains(confirmAction("all", row), "Force-merge terrascale/ts-cos#12") {
		t.Fatal("all does not name the PR")
	}
}

func TestThreadPanelRowButtons(t *testing.T) {
	rows := []panelThread{
		{Host: "delta", ThreadID: "worker", Title: "worker", Status: "idle", Updated: "2026-10-11T10:00:00Z", PR: openThreadPR()},
		{Host: "delta", ThreadID: "sub", Title: "sub", Status: "idle", Updated: "2026-10-11T09:00:00Z"},
	}
	actions := []ChatEntry{{Action: true, At: time.Date(2026, 10, 11, 9, 5, 0, 0, time.UTC), Text: "Settled 'sub'"}}
	var b bytes.Buffer
	if err := threadPanel(rows, actions).Render(context.Background(), &b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for want, n := range map[string]int{"Settle</button>": 2, "Force merge</button>": 1, "All three</button>": 1, "Delete</button>": 2, "<h3>Recent actions</h3>": 1, "09:05 Settled &#39;sub&#39;": 1} {
		if got := strings.Count(out, want); got != n {
			t.Fatalf("%q appears %d times, want %d:\n%s", want, got, n, out)
		}
	}
}

func TestThreadConfirmDialogPostsToTab(t *testing.T) {
	var b bytes.Buffer
	if err := threadConfirm(ChatTab{ID: "t1"}).Render(context.Background(), &b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	if !strings.Contains(out, `id="thread-confirm"`) || !strings.Contains(out, `/tabs/t1/threads/`) || !strings.Contains(out, `name="thread"`) {
		t.Fatal(out)
	}
}

func TestRecentActionsNewestFive(t *testing.T) {
	var entries []ChatEntry
	for i := range 12 {
		entries = append(entries, ChatEntry{ID: string(rune('a' + i)), Action: i%2 == 0, Text: string(rune('a' + i))})
	}
	got := recentActions(entries)
	var texts []string
	for _, e := range got {
		texts = append(texts, e.Text)
	}
	if strings.Join(texts, "") != "kigec" {
		t.Fatal(texts)
	}
}

// TestThreadActionRoute runs the row buttons through the real route, chat and event stream on a three-node cluster.
func TestThreadActionRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nodes, _ := chatTestCluster(t, ctx)
	u := chatTestUser()
	ctx = WithUser(ctx, u)
	chat := NewChat()
	go chat.Run(ctx, nodes[0])
	// Every node follows the chat: the buckets are created by whichever node leads JetStream.
	for _, n := range nodes[1:] {
		go NewChat().Run(ctx, n)
	}
	chatTestWait(t, "chat ready", chat.Ready)
	js, err := nodes[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "event streams", func() bool { return EnsureEventStreams(ctx, js) == nil })

	store := openTestStore(t)
	var live Live
	if err := json.Unmarshal([]byte(threadsTestLive), &live); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveLive(ctx, live); err != nil {
		t.Fatal(err)
	}
	fake := &fakeBus{}
	w := NewWeb(store, nil)
	w.Bus, w.Chat, w.call, w.merge = nodes[0], chat, fake.bus, fake.merge
	tab, err := chat.CreateTab(ctx, "Feature X", "")
	if err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "tab cached", func() bool { _, ok := chat.Tab(ctx, tab.ID); return ok })
	for _, tag := range []ChatTag{{ThreadID: "brain", Host: "delta", Tab: tab.ID, COS: true}, {ThreadID: "worker", Host: "delta", Tab: tab.ID}} {
		if err := chat.PutTag(ctx, tag); err != nil {
			t.Fatal(err)
		}
	}
	chatTestWait(t, "tags cached", func() bool { return len(chat.Tags(ctx, tab.ID)) == 2 })

	post := func(op, thread string) string {
		t.Helper()
		form := url.Values{"host": {"delta"}, "thread": {thread}}
		req := httptest.NewRequest(http.MethodPost, "/tabs/"+tab.ID+"/threads/"+op, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		asUserHandler(u, w.routes()).ServeHTTP(rec, req)
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("%s %s: status %d", op, thread, rec.Code)
		}
		loc, err := url.Parse(rec.Header().Get("Location"))
		if err != nil {
			t.Fatal(err)
		}
		return loc.Query().Get("notice")
	}
	for _, tc := range []struct{ op, thread, want string }{
		{"settle", "brain", "This thread is not in this tab's panel"},
		{"settle", "worker", "Settled 'worker'"},
		{"all", "worker", "Not done: the PR is not mergeable"},
	} {
		if got := post(tc.op, tc.thread); got != tc.want {
			t.Fatalf("%s %s: notice %q, want %q", tc.op, tc.thread, got, tc.want)
		}
	}
	fake.skipped = []map[string]string{{"threadId": "sub"}}
	if got := post("settle", "worker"); got != "Not settled: 'worker' is still running" {
		t.Fatalf("running settle: notice %q", got)
	}
	if strings.Join(fake.calls, " ") != "close close" {
		t.Fatal(fake.calls)
	}

	// Every outcome, refusals included, is an Action entry in the tab's chat.
	chatTestWait(t, "four action entries", func() bool { return len(recentActions(chat.Entries(ctx, tab.ID))) == 4 })

	// Each outcome is also one valid thread.action event.
	stream, err := js.Stream(ctx, EventsStream)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.GetLastMsgForSubject(ctx, "cos.events.user.thread.action")
	if err != nil {
		t.Fatal(err)
	}
	var e Event
	if err := json.Unmarshal(msg.Data, &e); err != nil {
		t.Fatal(err)
	}
	if err := e.Validate(); err != nil || e.Kind != KindThreadAction || !strings.Contains(e.Summary, "(tab Feature X)") {
		t.Fatalf("event %+v: %v", e, err)
	}
}
