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

var budgetTestLimits = budgetLimits{Thread: budgetThreadFloor, Daily: budgetDailyFloor, Raises: map[string]int64{}}

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
		Severity: SeverityWarn, Summary: "TS-1 used 10M of 10M tokens today (100%)", Host: "beta", Thread: "t1", Ticket: "TS-1", Data: data}
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
	if got[1].Inbox.Kind != "question" || strings.Join(got[1].Inbox.Options, "|") != strings.Join(budgetThreadOptionsFor(10_000_000), "|") {
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
	unresolved.Thread = ""
	got = Classify(unresolved, live, nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassRecord, Rule: "budget-exceeded"}})
	if got[0].Wake != "" {
		t.Fatalf("unresolved thread was woken: %+v", got[0])
	}

	// The sweep resolved the thread, but the leader's snapshot has not caught up yet: still a pause.
	lagging := budgetTestEvent(t, KindBudgetExceeded, budgetData{Scope: scope, Day: budgetTestDay, Used: 10_000_000, Limit: 10_000_000})
	got = Classify(lagging, Live{}, nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassAct, Rule: "budget-pause"}, {Class: ClassEscalate, Rule: "budget-pause"}})
	if got[0].Host != "beta" || got[0].Thread != "t1" || !strings.HasPrefix(got[0].Wake, "PAUSED: budget") {
		t.Fatalf("lagging pause = %+v, want a wake for beta/t1", got[0])
	}
	budgetItemValid(t, got[1].Inbox)
	if got[1].Inbox.Kind != "question" || got[1].Inbox.Thread != "t1" {
		t.Fatalf("lagging pause inbox = %+v, want a thread question on t1", got[1].Inbox)
	}

	// COS is never paused, even when the sweep resolved a thread for it and the snapshot is empty.
	cosLagging := budgetTestEvent(t, KindBudgetExceeded, budgetData{Scope: scope, Day: budgetTestDay, Used: 10_000_000, Limit: 10_000_000, Label: budgetCOSLabel})
	got = Classify(cosLagging, Live{}, nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassRecord, Rule: "budget-exceeded"}})
	if got[0].Wake != "" {
		t.Fatalf("COS was woken on a lagging snapshot: %+v", got[0])
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
	if strings.Join(got[1].Inbox.Options, "|") != strings.Join(budgetFleetOptionsFor(100_000_000), "|") {
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
		{thread, AnswerData{Option: "Resume: allow 15M tokens today"}, 15_000_000, true},
		{thread, AnswerData{Option: "Raise to 20M tokens today"}, 20_000_000, true},
		{thread, AnswerData{Option: "Resume with +50% today"}, 15_000_000, true},
		{thread, AnswerData{Option: "Raise to 2x today"}, 20_000_000, true},
		{thread, AnswerData{Option: "Resume with +50% over today's use"}, 15_000_000, true},
		{thread, AnswerData{Option: "Raise to 2x today's use"}, 20_000_000, true},
		{thread, AnswerData{Option: budgetKeep}, 0, false},
		{thread, AnswerData{Text: "go ahead"}, 0, false},
		{fleet, AnswerData{Option: "Resume all: allow 125M tokens today"}, 125_000_000, true},
		{fleet, AnswerData{Option: "Raise today's limit to 200M tokens"}, 200_000_000, true},
		{fleet, AnswerData{Option: "Resume all with +25% today"}, 125_000_000, true},
		{fleet, AnswerData{Option: "Raise today's budget to 2x"}, 200_000_000, true},
		{fleet, AnswerData{Option: "Resume all with +25% over today's use"}, 125_000_000, true},
		{fleet, AnswerData{Option: "Raise today's budget to 2x today's use"}, 200_000_000, true},
		{fleet, AnswerData{Option: budgetKeep}, 0, false},
		{fleet, AnswerData{Text: "more please"}, 0, false},
	}
	for _, c := range cases {
		item := c.item
		item.Answer = &c.answer
		key, value, ok := budgetRaise(item, 0)
		if ok != c.ok || value != c.want {
			t.Errorf("%+v -> (%d, %v), want (%d, %v)", c.answer, value, ok, c.want, c.ok)
		}
		if ok && !strings.HasPrefix(key, "budget.raise."+strings.ReplaceAll(budgetTestDay, "-", "")+".") {
			t.Errorf("key %q is not a raise key for %s", key, budgetTestDay)
		}
	}
	if key, _, _ := budgetRaise(fleet, 0); key != "" {
		t.Errorf("unanswered item raised %q", key)
	}
	if _, _, ok := budgetRaise(InboxItem{Source: "cos", Key: thread.Key, Answer: &AnswerData{Option: "Raise to 2x today"}}, 0); ok {
		t.Error("non-budget item raised a limit")
	}
}

func TestBudgetRaiseStartsFromTodaysUse(t *testing.T) {
	fleet := InboxItem{Source: string(SourceBudget), Key: "budget:" + budgetTestDay + ":daily:100:100000000",
		Answer: &AnswerData{Option: "Resume all: allow 125M tokens today"}}
	for _, c := range []struct {
		name string
		used int64
		want int64
	}{
		{"used above limit", 139_000_000, 173_750_000},
		{"used below limit", 40_000_000, 125_000_000},
		{"no rows today", 0, 125_000_000},
	} {
		_, got, ok := budgetRaise(fleet, c.used)
		if !ok || got != c.want {
			t.Errorf("%s: raise = (%d, %v), want (%d, true)", c.name, got, ok, c.want)
		}
		if got <= c.used && c.used > 0 {
			t.Errorf("%s: raise %d does not clear used %d", c.name, got, c.used)
		}
	}

	scope := budgetScope("beta/proj")
	thread := InboxItem{Source: string(SourceBudget), Key: "budget:" + budgetTestDay + ":" + scope + ":100:10000000",
		Answer: &AnswerData{Option: "Raise to 2x today's use"}}
	if _, got, _ := budgetRaise(thread, 16_000_000); got != 32_000_000 {
		t.Errorf("thread raise 2x over 16M used = %d, want 32000000", got)
	}

	st := budgetState{Day: budgetTestDay, Daily: UsageTotal{Input: 139_000_000}, Totals: []UsageTotal{budgetTestTotal(16_000_000)}}
	if got := budgetUsedFor(st, budgetDaily); got != 139_000_000 {
		t.Errorf("daily used = %d, want 139000000", got)
	}
	if got := budgetUsedFor(st, scope); got != 16_000_000 {
		t.Errorf("thread used = %d, want 16000000", got)
	}
	if got := budgetUsedFor(st, budgetScope("beta/other")); got != 0 {
		t.Errorf("unknown scope used = %d, want 0", got)
	}
}

func TestBudgetRaiseDoesNotRecross(t *testing.T) {
	totals := []UsageTotal{budgetTestTotal(139_000_000)}
	daily := UsageTotal{Input: 139_000_000}
	item := InboxItem{Source: string(SourceBudget), Key: "budget:" + budgetTestDay + ":daily:100:100000000",
		Answer: &AnswerData{Option: "Resume all: allow 125M tokens today"}}
	_, value, ok := budgetRaise(item, budgetUsed(daily))
	if !ok {
		t.Fatal("budgetRaise rejected the answer")
	}
	raised := budgetTestLimits
	raised.Raises = map[string]int64{budgetDaily: value}
	for _, e := range budgetCrossings(totals, daily, raised, budgetTestDay) {
		var d budgetData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d.Scope == budgetDaily && e.Kind == KindBudgetExceeded {
			t.Fatalf("sweep after the raise re-crossed 100%%: %+v", e)
		}
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

func TestBudgetApplyOncePerAnswer(t *testing.T) {
	ctx := context.Background()
	kv := &fakeKV{data: map[string][]byte{}}
	item := InboxItem{ID: "12e04d92a7001d67", Source: string(SourceBudget), Status: "answered",
		Key:    "budget:" + budgetTestDay + ":" + budgetDaily + ":100:100000000",
		Answer: &AnswerData{Option: "Resume all with +25% today"}}
	st := budgetState{Day: budgetTestDay, Daily: UsageTotal{Input: 139_000_000}}
	if err := budgetApply(ctx, nil, kv, st, []InboxItem{item}); err != nil {
		t.Fatal(err)
	}
	key := budgetRaiseKey(budgetTestDay, budgetDaily)
	first := string(kv.data[key])
	if first != "173750000" || string(kv.data[budgetAppliedKey(item.ID)]) != budgetTestDay {
		t.Fatalf("first apply: raise %q, marker %q", first, kv.data[budgetAppliedKey(item.ID)])
	}
	st.Daily.Input = 180_000_000 // use keeps growing; the answer must not raise again
	if err := budgetApply(ctx, nil, kv, st, []InboxItem{item}); err != nil {
		t.Fatal(err)
	}
	if got := string(kv.data[key]); got != first {
		t.Fatalf("second sweep raised again: %s, want %s", got, first)
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
	def := budgetLimits{Thread: 30_000_000, Daily: 170_000_000}
	l, err := budgetLoadLimits(ctx, kv, budgetTestDay, []string{budgetDaily, scope}, def)
	if err != nil {
		t.Fatal(err)
	}
	if l.Thread != 20_000_000 || l.ThreadDefault || l.Daily != 170_000_000 || !l.DailyDefault || l.Raises[scope] != 30_000_000 || len(l.Raises) != 1 {
		t.Fatalf("limits = %+v", l)
	}
}

func TestBudgetDefaults(t *testing.T) {
	row := func(day, project string, input int64) UsageRow {
		return UsageRow{Day: day, Host: "beta", Project: project, Input: input}
	}
	for _, c := range []struct {
		name          string
		rows          []UsageRow
		thread, daily int64
	}{
		{"no history", nil, budgetThreadFloor, budgetDailyFloor},
		{"83M peak day doubles to 170M", []UsageRow{row("2026-10-10", "proj", 83_000_000)}, 170_000_000, 170_000_000},
		{"today is excluded", []UsageRow{row(budgetTestDay, "proj", 500_000_000)}, budgetThreadFloor, budgetDailyFloor},
		{"13.7M thread peak doubles to 30M", []UsageRow{row("2026-10-09", "proj", 13_700_000)}, 30_000_000, budgetDailyFloor},
		{"day older than 7 days is excluded", []UsageRow{row("2026-10-03", "proj", 900_000_000)}, budgetThreadFloor, budgetDailyFloor},
	} {
		thread, daily := budgetDefaults(c.rows, budgetTestDay)
		if thread != c.thread || daily != c.daily {
			t.Errorf("%s: defaults = (%d, %d), want (%d, %d)", c.name, thread, daily, c.thread, c.daily)
		}
	}
}

func TestBudgetPauseOptionsCarryTokens(t *testing.T) {
	if got := strings.Join(budgetThreadOptionsFor(120_000_000), "|"); got != "Resume: allow 180M tokens today|Raise to 240M tokens today|Keep paused until 00:00 UTC" {
		t.Errorf("thread options = %q", got)
	}
	if got := strings.Join(budgetFleetOptionsFor(120_000_000), "|"); got != "Resume all: allow 150M tokens today|Raise today's limit to 240M tokens|Keep paused until 00:00 UTC" {
		t.Errorf("fleet options = %q", got)
	}
}

func TestBudgetSummarySaysSubscriptionLimits(t *testing.T) {
	e, ok := budgetEvent(budgetData{Scope: budgetDaily, Day: budgetTestDay, Used: 139_000_000, Limit: 100_000_000,
		Label: strings.Repeat("x", 120)}, "", "", "")
	if !ok || !strings.Contains(e.Summary, "Claude subscription limits") {
		t.Errorf("summary = %q", e.Summary)
	}
}

// budgetHint is the settings-page helper, kept here only so this test still compiles until the engine slice removes it.
func budgetHint(n int64, fromHistory bool) string {
	if fromHistory {
		return "Default " + budgetHuman(n) + " from the last 7 days · e.g. 15M or 500k"
	}
	return "Saved setting · e.g. 15M or 500k"
}

func TestBudgetHint(t *testing.T) {
	if got := budgetHint(30_000_000, true); got != "Default 30M from the last 7 days · e.g. 15M or 500k" {
		t.Errorf("default hint = %q", got)
	}
	if got := budgetHint(15_000_000, false); got != "Saved setting · e.g. 15M or 500k" {
		t.Errorf("saved hint = %q", got)
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
