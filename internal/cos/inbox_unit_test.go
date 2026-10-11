package cos

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

var inboxTestNow = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func TestInboxUnitItemValidation(t *testing.T) {
	long := strings.Repeat("x", 201)
	tests := []struct {
		name  string
		input InboxInput
		ok    bool
		check func(*testing.T, InboxItem)
	}{
		{name: "defaults to question from cos", input: InboxInput{Title: " Pick window "}, ok: true, check: func(t *testing.T, item InboxItem) {
			if item.Kind != "question" || item.Source != "cos" || item.Title != "Pick window" || item.Status != "open" || !item.Created.Equal(inboxTestNow) {
				t.Fatal(item)
			}
		}},
		{name: "approval without options gets Approve and Reject", input: InboxInput{Kind: "approval", Title: "Merge"}, ok: true, check: func(t *testing.T, item InboxItem) {
			if !slices.Equal(item.Options, []string{"Approve", "Reject"}) {
				t.Fatal(item.Options)
			}
		}},
		{name: "question trims and de-duplicates options", input: InboxInput{Kind: "question", Title: "Pick", Options: []string{" Yes ", "Yes", "", "No"}}, ok: true, check: func(t *testing.T, item InboxItem) {
			if !slices.Equal(item.Options, []string{"Yes", "No"}) {
				t.Fatal(item.Options)
			}
		}},
		{name: "question with four options", input: InboxInput{Kind: "question", Title: "Pick", Options: []string{"a", "b", "c", "d"}}, ok: true},
		{name: "question with no options", input: InboxInput{Kind: "question", Title: "Pick"}, ok: true},
		{name: "decision without options", input: InboxInput{Kind: "decision", Title: "Chose SQLite"}, ok: true},
		{name: "blocker without options", input: InboxInput{Kind: "blocker", Title: "CI red"}, ok: true},
		{name: "source rules", input: InboxInput{Title: "Rule", Source: "rules"}, ok: true},
		{name: "source worker", input: InboxInput{Title: "Ask", Source: "worker"}, ok: true},
		{name: "unknown kind", input: InboxInput{Kind: "nope", Title: "x"}},
		{name: "unknown source", input: InboxInput{Title: "x", Source: "human"}},
		{name: "empty title", input: InboxInput{Title: "   "}},
		{name: "multi-line title", input: InboxInput{Title: "one\ntwo"}},
		{name: "title over 200 runes", input: InboxInput{Title: long}},
		{name: "context over 4000 runes", input: InboxInput{Title: "x", Context: strings.Repeat("x", 4001)}},
		{name: "ticket not a ticket", input: InboxInput{Title: "x", Ticket: "nope"}},
		{name: "ticket uppercased", input: InboxInput{Title: "x", Ticket: "ts-815"}, ok: true, check: func(t *testing.T, item InboxItem) {
			if item.Ticket != "TS-815" {
				t.Fatal(item.Ticket)
			}
		}},
		{name: "thread without host", input: InboxInput{Title: "x", Thread: "t1"}},
		{name: "host with spaces", input: InboxInput{Title: "x", Host: "bad host", Thread: "t1"}},
		{name: "thread with host", input: InboxInput{Title: "x", Host: "delta", Thread: "t1"}, ok: true},
		{name: "question with one option", input: InboxInput{Kind: "question", Title: "x", Options: []string{"Only"}}},
		{name: "question with five options", input: InboxInput{Kind: "question", Title: "x", Options: []string{"a", "b", "c", "d", "e"}}},
		{name: "approval with options", input: InboxInput{Kind: "approval", Title: "x", Options: []string{"Yes", "No"}}},
		{name: "decision with options", input: InboxInput{Kind: "decision", Title: "x", Options: []string{"Yes", "No"}}},
		{name: "blocker with options", input: InboxInput{Kind: "blocker", Title: "x", Options: []string{"Yes", "No"}}},
		{name: "ftp link", input: InboxInput{Title: "x", Links: []string{"ftp://example.com/a"}}},
		{name: "javascript link", input: InboxInput{Title: "x", Links: []string{"javascript:alert(1)"}}},
		{name: "https link without host", input: InboxInput{Title: "x", Links: []string{"https://"}}},
		{name: "https link", input: InboxInput{Title: "x", Links: []string{"https://example.com/a"}}, ok: true},
		{name: "supersedes 15 hex characters", input: InboxInput{Title: "x", Supersedes: []string{"0123456789abcde"}}},
		{name: "supersedes not hex", input: InboxInput{Title: "x", Supersedes: []string{"xyz"}}},
		{name: "supersedes 16 hex characters", input: InboxInput{Title: "x", Supersedes: []string{"0123456789abcdef"}}, ok: true},
		{name: "key over 512 bytes", input: InboxInput{Title: "x", Key: strings.Repeat("k", 513)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			item, err := tc.input.item(inboxTestNow)
			if !tc.ok {
				if err == nil {
					t.Fatalf("accepted %+v as %+v", tc.input, item)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.check != nil {
				tc.check(t, item)
			}
		})
	}
}

func TestInboxUnitDedupKey(t *testing.T) {
	base := InboxInput{Title: "Pick window", Host: "delta", Thread: "t1"}
	tests := []struct {
		name string
		a, b InboxInput
		same bool
	}{
		{name: "same thread and title differing in case and spacing", a: base, b: InboxInput{Title: "  pick   WINDOW ", Host: "delta", Thread: "t1"}, same: true},
		{name: "same title on a different thread", a: base, b: InboxInput{Title: "Pick window", Host: "delta", Thread: "t2"}},
		{name: "same title on a different host", a: base, b: InboxInput{Title: "Pick window", Host: "beta", Thread: "t1"}},
		{name: "same title on a different ticket", a: InboxInput{Title: "Pick", Ticket: "TS-815"}, b: InboxInput{Title: "Pick", Ticket: "TS-816"}},
		{name: "ticket scope differs from no scope", a: InboxInput{Title: "Pick", Ticket: "TS-815"}, b: InboxInput{Title: "Pick"}},
		{name: "explicit key overrides a different title", a: InboxInput{Title: "One", Key: "shared"}, b: InboxInput{Title: "Two", Key: "shared"}, same: true},
		{name: "explicit key differs from default key", a: InboxInput{Title: "Pick", Key: "shared"}, b: InboxInput{Title: "Pick"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := tc.a.item(inboxTestNow)
			if err != nil {
				t.Fatal(err)
			}
			b, err := tc.b.item(inboxTestNow)
			if err != nil {
				t.Fatal(err)
			}
			if (a.ID == b.ID) != tc.same {
				t.Fatalf("same=%t: %s (%q) vs %s (%q)", tc.same, a.ID, a.Key, b.ID, b.Key)
			}
		})
	}
	item, err := base.item(inboxTestNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(item.ID) != 16 || !validInboxID(item.ID) {
		t.Fatal("ID is not 16 hex characters:", item.ID)
	}
}

func TestInboxUnitItemEvent(t *testing.T) {
	open := InboxItem{ID: "0123456789abcdef", Kind: "question", Title: "Pick", Status: "open", Created: inboxTestNow, Host: "delta", Thread: "t1"}
	urgent := open
	urgent.Urgent = true
	approval := open
	approval.Kind = "approval"
	closed := open
	closed.Closed = inboxTestNow.Add(time.Hour)
	answered := closed
	answered.Status = "answered"
	answered.Answer = &AnswerData{Option: "Yes", Text: "now"}
	dismissed := closed
	dismissed.Status = "dismissed"
	superseded := closed
	superseded.Status = "superseded"
	superseded.Note = "Superseded by: newer"
	tests := []struct {
		name     string
		item     InboxItem
		source   EventSource
		kind     EventKind
		severity Severity
	}{
		{name: "open question", item: open, source: SourceInbox, kind: KindInboxItemAdded, severity: SeverityWarn},
		{name: "urgent question", item: urgent, source: SourceInbox, kind: KindInboxItemAdded, severity: SeverityUrgent},
		{name: "approval", item: approval, source: SourceInbox, kind: KindInboxItemAdded, severity: SeverityUrgent},
		{name: "answered", item: answered, source: SourceUser, kind: KindUserAnswer, severity: SeverityUrgent},
		{name: "dismissed", item: dismissed, source: SourceInbox, kind: KindInboxClosed, severity: SeverityInfo},
		{name: "superseded", item: superseded, source: SourceInbox, kind: KindInboxClosed, severity: SeverityInfo},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := itemEvent(tc.item)
			if e.Source != tc.source || e.Kind != tc.kind || e.Severity != tc.severity || e.Item != tc.item.ID {
				t.Fatalf("event %+v", e)
			}
			if err := e.Validate(); err != nil {
				t.Fatal(err)
			}
			if itemEvent(tc.item).ID != e.ID {
				t.Fatal("event ID changed for the same item state")
			}
		})
	}
	var got AnswerData
	if err := json.Unmarshal(itemEvent(answered).Data, &got); err != nil || got != *answered.Answer {
		t.Fatalf("answer data %+v err=%v", got, err)
	}
	if itemEvent(open).ID == itemEvent(answered).ID {
		t.Fatal("open and answered states share an event ID")
	}
}

func TestInboxUnitStoreOrderingAndPrune(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	at := func(d time.Duration) time.Time { return inboxTestNow.Add(d) }
	items := []InboxItem{
		{ID: "a", Kind: "question", Title: "oldest normal", Status: "open", Created: at(0)},
		{ID: "b", Kind: "question", Title: "newest normal", Status: "open", Created: at(2 * time.Hour)},
		{ID: "c", Kind: "approval", Title: "approval", Status: "open", Created: at(time.Hour)},
		{ID: "d", Kind: "question", Title: "urgent question", Urgent: true, Status: "open", Created: at(30 * time.Minute)},
		{ID: "h1", Kind: "question", Title: "answered", Status: "answered", Created: at(0), Closed: at(time.Hour)},
		{ID: "h2", Kind: "question", Title: "dismissed", Status: "dismissed", Created: at(0), Closed: at(3 * time.Hour)},
		{ID: "h3", Kind: "question", Title: "superseded", Status: "superseded", Created: at(0), Closed: at(2 * time.Hour)},
	}
	for _, item := range items {
		if err := store.SaveInboxItem(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	open, history, err := store.Inbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := inboxIDs(open); !slices.Equal(got, []string{"c", "d", "b", "a"}) {
		t.Fatal("open order", got)
	}
	if got := inboxIDs(history); !slices.Equal(got, []string{"h2", "h3", "h1"}) {
		t.Fatal("history order", got)
	}

	view, err := View(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if got := inboxIDs(view.Inbox); !slices.Equal(got, []string{"c", "d", "b", "a"}) {
		t.Fatal("view inbox", got)
	}
	if got := inboxIDs(view.History); !slices.Equal(got, []string{"h2", "h3", "h1"}) {
		t.Fatal("view history", got)
	}
	if brief := Brief(view); !strings.Contains(brief, "INBOX c approval") || !strings.Contains(brief, "INBOX a question") {
		t.Fatal(brief)
	}

	if err := store.PruneInbox(ctx, map[string]bool{"a": true, "c": true}); err != nil {
		t.Fatal(err)
	}
	open, history, err = store.Inbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := append(inboxIDs(open), inboxIDs(history)...); !slices.Equal(got, []string{"c", "a"}) {
		t.Fatal("pruned to", got)
	}
}

func inboxIDs(items []InboxItem) []string {
	ids := []string{}
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func TestInboxUnitLiveRequests(t *testing.T) {
	asks := []Ask{
		{Kind: "marker", Question: "marker"},
		{Kind: "marker", Question: "marker failed", Error: "projection failed"},
		{Kind: "request", Question: "request"},
		{Kind: "request", Question: "request failed", Error: "timeout"},
	}
	got := liveRequests(asks)
	want := []string{"marker failed", "request", "request failed"}
	if len(got) != len(want) {
		t.Fatal(got)
	}
	for i := range want {
		if got[i].Question != want[i] {
			t.Fatalf("index %d: %+v", i, got[i])
		}
	}
}

// Sign-in and cross-site rejection wrap every route in Auth.Protect.
func TestInboxUnitWebAnswerWaitsForInbox(t *testing.T) {
	h := NewWeb(openTestStore(t), nil).routes()
	for path, code := range map[string]int{"/inbox/0123456789abcdef/answer": http.StatusServiceUnavailable, "/inbox/0123456789abcdef/dismiss": http.StatusServiceUnavailable} {
		req := httptest.NewRequest("POST", path, strings.NewReader("text=hi"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rw := httptest.NewRecorder()
		h.ServeHTTP(rw, req)
		if rw.Code != code {
			t.Fatalf("%s: status=%d body=%s", path, rw.Code, rw.Body.String())
		}
	}
}

func TestInboxUnitFYI(t *testing.T) {
	item, err := InboxInput{Kind: "fyi", Title: "Deploy rolled back: ts-cos 4508c66", Source: "deploy", Key: "deploy:r1"}.item(time.Now())
	if err != nil || item.Options != nil || item.ID != inboxID("deploy:r1") {
		t.Fatalf("fyi item %+v %v", item, err)
	}
	if e := itemEvent(item); e.Kind != KindInboxItemAdded || e.Severity != SeverityInfo || e.Validate() != nil {
		t.Fatalf("fyi event %+v", e)
	}
	for _, bad := range []InboxInput{
		{Kind: "fyi", Title: "x", Urgent: true},
		{Kind: "fyi", Title: "x", Options: []string{"a", "b"}},
		{Kind: "fyi", Title: "x", Source: "cron"},
	} {
		if _, err := bad.item(time.Now()); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
}

func TestInboxUnitRegisteredCommand(t *testing.T) {
	var help strings.Builder
	if err := Run(context.Background(), Config{Home: t.TempDir()}, []string{"help"}, &help); err != nil || !strings.Contains(help.String(), "inbox add '") {
		t.Fatalf("help: %v %s", err, help.String())
	}
	err := Run(context.Background(), Config{Home: t.TempDir()}, []string{"inbox", "nope"}, io.Discard)
	if err == nil || !strings.HasPrefix(err.Error(), "usage: cos inbox") {
		t.Fatalf("dispatch: %v", err)
	}
}
