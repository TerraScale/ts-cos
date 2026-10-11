package cos

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const budgetTestDay = "2026-10-11"

var budgetTestLimits = budgetLimits{Thread: budgetThreadDefault, Daily: budgetDailyDefault, Raises: map[string]int64{}}

func budgetTestTotal(used int64) UsageTotal {
	return UsageTotal{Key: "beta/proj", Label: "TS-1 tokens", Host: "beta", Thread: "t1", Ticket: "TS-1", Input: used}
}

func budgetTestEvent(t *testing.T, kind EventKind, d budgetData) Event {
	t.Helper()
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return Event{ID: NewEventID(SourceBudget, "test", string(kind)), Source: SourceBudget, Kind: kind, Time: time.Now(),
		Severity: SeverityWarn, Summary: "TS-1 used 10M of 10M tokens today (100%, $1.00)", Host: "beta", Thread: "t1", Ticket: "TS-1", Data: data}
}

func budgetThreadEvents(events []Event, thread string) []Event {
	var out []Event
	for _, e := range events {
		if e.Thread == thread {
			out = append(out, e)
		}
	}
	return out
}

// budgetItemValid checks that the Inbox accepts the item the rules filed.
func budgetItemValid(t *testing.T, in *InboxInput) {
	t.Helper()
	if in == nil {
		t.Fatal("decision has no Inbox item")
	}
	if _, err := in.item(time.Now()); err != nil {
		t.Fatalf("Inbox rejects %+v: %v", *in, err)
	}
}

func TestBudgetCrossings(t *testing.T) {
	scope := budgetScope("beta/proj")
	cases := []struct {
		used int64
		want EventKind
	}{{7_999_999, ""}, {8_000_000, KindBudgetWarn}, {9_999_999, KindBudgetWarn}, {10_000_000, KindBudgetExceeded}}
	for _, c := range cases {
		events := budgetThreadEvents(budgetCrossings([]UsageTotal{budgetTestTotal(c.used)}, UsageTotal{}, budgetTestLimits, budgetTestDay), "t1")
		if c.want == "" {
			if len(events) != 0 {
				t.Fatalf("used %d: got %d events, want none", c.used, len(events))
			}
			continue
		}
		if len(events) != 1 || events[0].Kind != c.want {
			t.Fatalf("used %d: events = %+v, want one %s", c.used, events, c.want)
		}
		if err := events[0].Validate(); err != nil {
			t.Fatal(err)
		}
	}

	// A raise lifts the limit: 12M of 15M is 80%, and 10M of 15M is no crossing.
	raised := budgetTestLimits
	raised.Raises = map[string]int64{scope: 15_000_000}
	if events := budgetThreadEvents(budgetCrossings([]UsageTotal{budgetTestTotal(12_000_000)}, UsageTotal{}, raised, budgetTestDay), "t1"); len(events) != 1 || events[0].Kind != KindBudgetWarn {
		t.Fatalf("raised warn events = %+v", events)
	}
	if events := budgetThreadEvents(budgetCrossings([]UsageTotal{budgetTestTotal(10_000_000)}, UsageTotal{}, raised, budgetTestDay), "t1"); len(events) != 0 {
		t.Fatalf("raised events = %+v, want none", events)
	}
}

func TestBudgetCrossingsDailyListsThreadsAndCOSNeverPauses(t *testing.T) {
	cos := UsageTotal{Key: "beta/cos", Label: budgetCOSLabel, Host: "beta", Thread: "t9", Input: 10_000_000}
	worker := budgetTestTotal(1_000)
	daily := UsageTotal{Input: 100_000_000}
	events := budgetCrossings([]UsageTotal{cos, worker}, daily, budgetTestLimits, budgetTestDay)

	var fleet, cosEvent *Event
	for i := range events {
		switch {
		case events[i].Thread == "" && strings.Contains(events[i].ID, ":daily:"):
			fleet = &events[i]
		case events[i].Thread == "" && events[i].Kind == KindBudgetExceeded:
			cosEvent = &events[i]
		}
	}
	if fleet == nil || cosEvent == nil {
		t.Fatalf("events = %+v, want a fleet exceeded and a COS exceeded", events)
	}
	var d budgetData
	if err := json.Unmarshal(fleet.Data, &d); err != nil {
		t.Fatal(err)
	}
	if len(d.Threads) != 1 || d.Threads[0] != (budgetThread{Host: "beta", Thread: "t1"}) {
		t.Fatalf("fleet threads = %+v, want only the worker thread t1", d.Threads)
	}
	if cosEvent.Thread != "" {
		t.Fatalf("COS event thread = %q, want none", cosEvent.Thread)
	}
}

func TestBudgetEventIDsValidateAndDifferPerLimit(t *testing.T) {
	first := budgetCrossings([]UsageTotal{budgetTestTotal(8_000_000)}, UsageTotal{}, budgetTestLimits, budgetTestDay)
	raised := budgetTestLimits
	raised.Raises = map[string]int64{budgetScope("beta/proj"): 15_000_000}
	second := budgetCrossings([]UsageTotal{budgetTestTotal(12_000_000)}, UsageTotal{}, raised, budgetTestDay)
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("events = %+v / %+v, want one each", first, second)
	}
	if first[0].ID == second[0].ID {
		t.Fatalf("both limits share event ID %s", first[0].ID)
	}
	for _, e := range append(first, second...) {
		if err := e.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	again := budgetCrossings([]UsageTotal{budgetTestTotal(8_000_000)}, UsageTotal{}, budgetTestLimits, budgetTestDay)
	if again[0].ID != first[0].ID {
		t.Fatalf("repeat ID = %s, want %s", again[0].ID, first[0].ID)
	}
}

func TestBudgetClassify(t *testing.T) {
	live := rulesLive()
	scope := budgetScope("beta/proj")

	warn := budgetTestEvent(t, KindBudgetWarn, budgetData{Scope: scope, Day: budgetTestDay, Used: 8_000_000, Limit: 10_000_000})
	got := Classify(warn, live, nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassRecord, Rule: "budget-warn"}})
	budgetItemValid(t, got[0].Inbox)
	if got[0].Inbox.Kind != "fyi" || got[0].Inbox.Key != "budget:"+budgetTestDay+":"+scope+":80" {
		t.Fatalf("warn inbox = %+v", got[0].Inbox)
	}

	exceeded := budgetTestEvent(t, KindBudgetExceeded, budgetData{Scope: scope, Day: budgetTestDay, Used: 10_000_000, Limit: 10_000_000})
	got = Classify(exceeded, live, nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassAct, Rule: "budget-pause"}, {Class: ClassEscalate, Rule: "budget-pause"}})
	if !strings.HasPrefix(got[0].Wake, "PAUSED: budget") || got[0].Host != "beta" || got[0].Thread != "t1" {
		t.Fatalf("pause = %+v", got[0])
	}
	budgetItemValid(t, got[1].Inbox)
	if got[1].Inbox.Kind != "question" || strings.Join(got[1].Inbox.Options, "|") != strings.Join(budgetThreadOptions, "|") {
		t.Fatalf("pause inbox = %+v", got[1].Inbox)
	}

	cos := budgetTestEvent(t, KindBudgetExceeded, budgetData{Scope: scope, Day: budgetTestDay, Used: 10_000_000, Limit: 10_000_000, Label: budgetCOSLabel})
	cos.Thread = ""
	got = Classify(cos, live, nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassRecord, Rule: "budget-exceeded"}})
	budgetItemValid(t, got[0].Inbox)
	if got[0].Wake != "" {
		t.Fatalf("COS was woken: %+v", got[0])
	}

	unresolved := budgetTestEvent(t, KindBudgetExceeded, budgetData{Scope: scope, Day: budgetTestDay, Used: 10_000_000, Limit: 10_000_000})
	unresolved.Thread = "t-gone"
	got = Classify(unresolved, live, nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassRecord, Rule: "budget-exceeded"}})
	if got[0].Wake != "" {
		t.Fatalf("unresolved thread was woken: %+v", got[0])
	}

	fleet := budgetTestEvent(t, KindBudgetExceeded, budgetData{Scope: budgetDaily, Day: budgetTestDay, Used: 100_000_000, Limit: 100_000_000,
		Threads: []budgetThread{{Host: "beta", Thread: "t1"}, {Host: "beta", Thread: "t-gone"}}})
	fleet.Thread, fleet.Host = "", ""
	got = Classify(fleet, live, nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassAct, Rule: "budget-pause"}, {Class: ClassEscalate, Rule: "budget-pause"}})
	if got[0].Thread != "t1" || !strings.HasPrefix(got[0].Wake, "PAUSED: budget") {
		t.Fatalf("fleet wake = %+v, want one wake for live t1 only", got[0])
	}
	budgetItemValid(t, got[1].Inbox)
	if strings.Join(got[1].Inbox.Options, "|") != strings.Join(budgetFleetOptions, "|") {
		t.Fatalf("fleet options = %v", got[1].Inbox.Options)
	}
}

func TestBudgetHandle(t *testing.T) {
	ctx := context.Background()
	rec := &rulesRecorder{}
	r := newRulesEngine(t, rec)
	var filed []InboxInput
	r.Inbox = func(_ context.Context, in InboxInput) error {
		filed = append(filed, in)
		return nil
	}
	exceeded := budgetTestEvent(t, KindBudgetExceeded, budgetData{Scope: budgetScope("beta/proj"), Day: budgetTestDay, Used: 10_000_000, Limit: 10_000_000})
	if err := r.Handle(ctx, exceeded); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 1 || rec.calls[0].host != "beta" || rec.calls[0].command != "send" || rec.calls[0].args[0] != "t1" ||
		!strings.HasPrefix(rec.calls[0].args[1], "PAUSED: budget") {
		t.Fatalf("calls = %+v, want one PAUSED send to t1", rec.calls)
	}
	if len(filed) != 1 || filed[0].Kind != "question" {
		t.Fatalf("filed = %+v, want one question", filed)
	}

	// An Inbox failure is reported as an error entry; the wake already went out.
	r.Inbox = func(context.Context, InboxInput) error { return errors.New("inbox down") }
	rec.published = nil
	if err := r.Handle(ctx, exceeded); err != nil {
		t.Fatal(err)
	}
	if last := rec.published[len(rec.published)-1]; last.Kind != "error" || !strings.Contains(last.Reason, "inbox failed") {
		t.Fatalf("last entry = %+v, want an inbox error", last)
	}
}

func TestBudgetRaise(t *testing.T) {
	thread := InboxItem{Source: string(SourceBudget), Key: "budget:" + budgetTestDay + ":" + budgetScope("beta/proj") + ":100:10000000"}
	fleet := InboxItem{Source: string(SourceBudget), Key: "budget:" + budgetTestDay + ":daily:100:100000000"}
	cases := []struct {
		item   InboxItem
		answer AnswerData
		want   int64
		ok     bool
	}{
		{thread, AnswerData{Option: budgetResume}, 15_000_000, true},
		{thread, AnswerData{Option: budgetRaise2x}, 20_000_000, true},
		{thread, AnswerData{Option: budgetKeep}, 0, false},
		{thread, AnswerData{Text: "go ahead"}, 0, false},
		{fleet, AnswerData{Option: budgetResumeAll}, 125_000_000, true},
		{fleet, AnswerData{Option: budgetRaiseAll}, 200_000_000, true},
		{fleet, AnswerData{Option: budgetKeep}, 0, false},
		{fleet, AnswerData{Text: "more please"}, 0, false},
	}
	for _, c := range cases {
		item := c.item
		item.Answer = &c.answer
		key, value, ok := budgetRaise(item)
		if ok != c.ok || value != c.want {
			t.Errorf("%+v -> (%d, %v), want (%d, %v)", c.answer, value, ok, c.want, c.ok)
		}
		if ok && !strings.HasPrefix(key, "budget.raise."+strings.ReplaceAll(budgetTestDay, "-", "")+".") {
			t.Errorf("key %q is not a raise key for %s", key, budgetTestDay)
		}
	}
	if key, _, _ := budgetRaise(fleet); key != "" {
		t.Errorf("unanswered item raised %q", key)
	}
	if _, _, ok := budgetRaise(InboxItem{Source: "cos", Key: thread.Key, Answer: &AnswerData{Option: budgetRaise2x}}); ok {
		t.Error("non-budget item raised a limit")
	}
}

func TestBudgetStoreRaiseKeepsLargest(t *testing.T) {
	ctx := context.Background()
	kv := &fakeKV{data: map[string][]byte{}}
	key := budgetRaiseKey(budgetTestDay, budgetDaily)
	for _, c := range []struct {
		value int64
		want  bool
	}{{125_000_000, true}, {125_000_000, false}, {100_000_000, false}, {150_000_000, true}} {
		got, err := budgetStoreRaise(ctx, kv, key, c.value)
		if err != nil || got != c.want {
			t.Fatalf("store %d = %v, %v; want %v", c.value, got, err, c.want)
		}
	}
	if string(kv.data[key]) != "150000000" {
		t.Fatalf("stored raise = %s, want 150000000", kv.data[key])
	}
}

func TestBudgetLoadLimits(t *testing.T) {
	ctx := context.Background()
	scope := budgetScope("beta/proj")
	kv := &fakeKV{data: map[string][]byte{
		budgetSettingsThread:                      []byte("20000000"),
		budgetSettingsDaily:                       []byte("0"),
		budgetRaiseKey(budgetTestDay, scope):      []byte("30000000"),
		budgetRaiseKey("2026-10-10", budgetDaily): []byte("999"),
	}}
	l, err := budgetLoadLimits(ctx, kv, budgetTestDay, []string{budgetDaily, scope})
	if err != nil {
		t.Fatal(err)
	}
	if l.Thread != 20_000_000 || l.Daily != budgetDailyDefault || l.Raises[scope] != 30_000_000 || len(l.Raises) != 1 {
		t.Fatalf("limits = %+v", l)
	}
}

func TestBudgetReportSortsByPercent(t *testing.T) {
	st := budgetState{Day: budgetTestDay, Limits: budgetTestLimits, Totals: []UsageTotal{
		{Key: "beta/a", Input: 1_000_000}, {Key: "beta/b", Input: 9_000_000}, {Key: "beta/c", Input: 5_000_000},
	}}
	r := st.report()
	if r.Threads[0].Key != "beta/b" || r.Threads[2].Key != "beta/a" || r.Threads[0].Percent != 90 {
		t.Fatalf("report threads = %+v", r.Threads)
	}
}

func TestParseTokens(t *testing.T) {
	for in, want := range map[string]int64{"10M": 10_000_000, "500k": 500_000, "42": 42} {
		if got, err := parseTokens(in); err != nil || got != want {
			t.Errorf("parseTokens(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "0", "-5", "x", "M", "10MB"} {
		if _, err := parseTokens(bad); err == nil {
			t.Errorf("parseTokens(%q) accepted", bad)
		}
	}
}
