package cos

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// lineageOf is FleetThread.Lineage's type, named so tests can build subagents.
type lineageOf = struct {
	Parent   string `json:"parent"`
	Relation string `json:"rel"`
}

// modelUsage is a thread that is past its first hour and far below the Haiku floor.
func modelUsage(now time.Time, mod func(*UsageTotal)) (UsageTotal, map[string]threadSeen) {
	u := UsageTotal{Key: "beta/p", Host: "beta", Thread: "t1", Output: 35000, AdvisorOutput: 30000, WorkerOutput: 5000,
		HaikuShare: 14, Workers: 3, Parallel: 2, Tools: map[string]int{"build": 5, "git": 2, "read": 30, "gitread": 4, "live": 1, "ci": 2}}
	if mod != nil {
		mod(&u)
	}
	return u, map[string]threadSeen{"beta/p": {First: now.Add(-2 * time.Hour)}}
}

// noWorkersUsage makes the thread run its own advisor output and never delegate to Haiku.
func noWorkersUsage(u *UsageTotal) {
	u.Workers, u.WorkerOutput, u.Output, u.HaikuShare = 0, 0, u.AdvisorOutput, 0
}

func adviseLine(t *testing.T) string {
	t.Helper()
	line, err := CurrentBehavior().Render("advise_split", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	return line
}

func TestUsagePolicy(t *testing.T) {
	now := time.Now()
	act := func(rule string) Decision { return Decision{Class: ClassAct, Rule: rule} }
	escalate := func(rule string) Decision { return Decision{Class: ClassEscalate, Rule: rule} }
	tests := []struct {
		name  string
		usage func(*UsageTotal)
		seen  func(map[string]threadSeen)
		last  map[string]time.Time
		want  []Decision
	}{
		{name: "below the floor after the first hour", want: []Decision{act("nudge_haiku_share")}},
		{name: "within the first hour", seen: func(s map[string]threadSeen) { s["beta/p"] = threadSeen{First: now.Add(-30 * time.Minute)} }},
		{name: "under the token minimum", usage: func(u *UsageTotal) { u.Output, u.AdvisorOutput, u.WorkerOutput = 1100, 1000, 100 }},
		{name: "nudged an hour ago", last: map[string]time.Time{"share:t1": now.Add(-time.Hour)}},
		{name: "nudged three hours ago", last: map[string]time.Time{"share:t1": now.Add(-3 * time.Hour)}, want: []Decision{act("nudge_haiku_share")}},
		{name: "at the floor", usage: func(u *UsageTotal) { u.HaikuShare = 50 }},
		{name: "unactionable without a T3 thread", usage: func(u *UsageTotal) { u.Thread = "" }},
		{name: "haiku 4.5 subagent", usage: func(u *UsageTotal) { u.HaikuShare = 60 }, seen: func(s map[string]threadSeen) {
			s["beta/p"] = threadSeen{First: now.Add(-2 * time.Hour), Haiku45: true}
		}, want: []Decision{escalate("subagent-haiku-4-5"), act("nudge_haiku45")}},
		{name: "haiku 4.5 subagent nudged within the window", usage: func(u *UsageTotal) { u.HaikuShare = 60 }, seen: func(s map[string]threadSeen) {
			s["beta/p"] = threadSeen{First: now.Add(-2 * time.Hour), Haiku45: true}
		}, last: map[string]time.Time{"haiku45:t1": now.Add(-time.Hour)}},
		{name: "no workers after the first hour", usage: noWorkersUsage, want: []Decision{act("nudge_no_workers")}},
		{name: "no workers nudged within the window falls back to the share nudge", usage: noWorkersUsage,
			last: map[string]time.Time{"noworkers:t1": now.Add(-time.Hour)}, want: []Decision{act("nudge_haiku_share")}},
		{name: "no workers under the token minimum", usage: func(u *UsageTotal) { noWorkersUsage(u); u.AdvisorOutput, u.Output = 1000, 1000 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, seen := modelUsage(now, tt.usage)
			if tt.seen != nil {
				tt.seen(seen)
			}
			assertRules(t, usagePolicy([]UsageTotal{u}, seen, now, tt.last, nil), tt.want)
		})
	}
}

func TestUsagePolicyShareNudgeText(t *testing.T) {
	now := time.Now()
	u, seen := modelUsage(now, func(u *UsageTotal) {
		u.HaikuShare, u.WorkerOutput, u.AdvisorOutput, u.Output, u.Workers = 12, 4000, 29000, 33000, 7
	})
	got := usagePolicy([]UsageTotal{u}, seen, now, nil, nil)
	if len(got) != 1 {
		t.Fatalf("decisions = %+v, want one nudge", got)
	}
	for _, want := range []string{"Haiku 5.5 workers did 12% of this thread's output", "(4000 worker vs 29000 advisor tokens, 7 workers, at most 2 in parallel)",
		"5 build/test loops", "2 git rebase/push/fetch", "30 file reads", "4 git reads", "1 live ops", "2 CI calls",
		"As the Opus advisor, hand those to parallel Haiku 5.5 workers."} {
		if !strings.Contains(got[0].Wake, want) {
			t.Errorf("wake = %q, want it to contain %q", got[0].Wake, want)
		}
	}
	if !strings.HasSuffix(got[0].Wake, "\n"+adviseLine(t)) {
		t.Errorf("wake = %q, want it to end with the advise line", got[0].Wake)
	}
	if got[0].Thread != "t1" || got[0].Host != "beta" || got[0].Sig != "share" {
		t.Errorf("decision = %+v, want beta t1 with sig share", got[0])
	}
}

func TestUsagePolicyNoWorkersNudgeText(t *testing.T) {
	now := time.Now()
	u, seen := modelUsage(now, func(u *UsageTotal) { noWorkersUsage(u); u.AdvisorOutput, u.Output = 52000, 52000 })
	got := usagePolicy([]UsageTotal{u}, seen, now, nil, nil)
	if len(got) != 1 || got[0].Rule != "nudge_no_workers" {
		t.Fatalf("decisions = %+v, want one no-workers nudge", got)
	}
	if want := "COS: this thread has done 52000 Opus advisor output tokens with no Haiku 5.5 workers. As the Opus advisor, split the work and run it on parallel Haiku 5.5 workers."; !strings.HasPrefix(got[0].Wake, want) {
		t.Errorf("wake = %q, want it to start with %q", got[0].Wake, want)
	}
	if !strings.HasSuffix(got[0].Wake, "\n"+adviseLine(t)) {
		t.Errorf("wake = %q, want it to end with the advise line", got[0].Wake)
	}
	if got[0].Thread != "t1" || got[0].Host != "beta" || got[0].Sig != "noworkers" {
		t.Errorf("decision = %+v, want beta t1 with sig noworkers", got[0])
	}
}

func TestFleetModelPolicy(t *testing.T) {
	now := time.Now()
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{
		{ThreadID: "t1", Model: "claude-haiku-5-5"},
		{ThreadID: "t2", Model: "claude-opus-5-5"},
		{ThreadID: "c1", Model: "claude-haiku-5-5", Lineage: &lineageOf{Parent: "t2", Relation: "subagent"}},
		{ThreadID: "c2", Model: "claude-haiku-4-5", Lineage: &lineageOf{Parent: "t2", Relation: "subagent"}},
	}}}}
	got := fleetModelPolicy(live, now, nil, nil)
	assertRules(t, got, []Decision{{Class: ClassAct, Rule: "model-opus-thread"}})
	if got[0].Thread != "t1" || got[0].Host != "beta" || got[0].Model["model"] != "claude-opus-5-5" || got[0].Model["options"] == nil {
		t.Errorf("model decision = %+v, want t1 switched to Opus 5.5 at high effort", got[0])
	}
}

func TestHaikuWithoutAdvisor(t *testing.T) {
	now := time.Now()
	fleet := func(threads ...FleetThread) Live { return Live{Fleet: []HostFleet{{Host: "beta", Threads: threads}}} }
	child := func(id, model, parent string) FleetThread {
		return FleetThread{ThreadID: id, Model: model, Lineage: &lineageOf{Parent: parent, Relation: "subagent"}}
	}
	tests := []struct {
		name string
		live Live
		last map[string]time.Time
		want []Decision
	}{
		{"haiku worker under a sonnet parent", fleet(FleetThread{ThreadID: "p1", Model: "claude-sonnet-x"}, child("c1", "claude-haiku-5-5", "p1")),
			nil, []Decision{{Class: ClassEscalate, Rule: "haiku-without-advisor", Thread: "p1"}}},
		{"haiku worker under a haiku parent", fleet(FleetThread{ThreadID: "p1", Model: "claude-haiku-5-5"}, child("c1", "claude-haiku-5-5", "p1")),
			nil, []Decision{{Class: ClassAct, Rule: "model-opus-thread", Thread: "p1"}, {Class: ClassEscalate, Rule: "haiku-without-advisor", Thread: "p1"}}},
		{"haiku worker under an opus parent", fleet(FleetThread{ThreadID: "p1", Model: "claude-opus-5-5"}, child("c1", "claude-haiku-5-5", "p1")), nil, nil},
		{"haiku worker whose parent is not in the fleet", fleet(child("c1", "claude-haiku-5-5", "gone")), nil, nil},
		{"noadvisor escalation within the window", fleet(FleetThread{ThreadID: "p1", Model: "claude-sonnet-x"}, child("c1", "claude-haiku-5-5", "p1")),
			map[string]time.Time{"noadvisor:p1": now.Add(-time.Hour)}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertRules(t, fleetModelPolicy(tt.live, now, tt.last, nil), tt.want)
		})
	}
}

func TestHaikuWithoutAdvisorReason(t *testing.T) {
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{
		{ThreadID: "p1", Model: "claude-sonnet-x"},
		{ThreadID: "c1", Model: "claude-haiku-5-5", Lineage: &lineageOf{Parent: "p1", Relation: "subagent"}},
	}}}}
	got := fleetModelPolicy(live, time.Now(), nil, nil)
	if len(got) != 1 || got[0].Host != "beta" || got[0].Thread != "p1" || got[0].Sig != "noadvisor" ||
		got[0].Reason != "Haiku claude-haiku-5-5 worker under a claude-sonnet-x parent; every Haiku worker needs an Opus 5.5 advisor" {
		t.Fatalf("decisions = %+v, want one escalation of p1 with the advisor reason", got)
	}
}

func TestClassifyWakeEndsWithAdviseLine(t *testing.T) {
	pr := rulesPR(func(p *PRFacts) { p.CI, p.Failing, p.SHA = "failure", []string{"build"}, "aaa" })
	for _, d := range Classify(rulesCIEvent(), rulesLive(), pr, WakeMemory{}, rulesPaused) {
		if d.Rule == "ci" {
			if !strings.HasSuffix(d.Wake, "\n"+adviseLine(t)) {
				t.Fatalf("ci wake = %q, want it to end with the advise line", d.Wake)
			}
			return
		}
	}
	t.Fatal("Classify produced no ci wake")
}

// policyEngine is a rules engine whose usage is in its SQLite store and whose settings bucket is kv.
func policyEngine(t *testing.T, rec *rulesRecorder, kv jetstream.KeyValue) *RulesEngine {
	t.Helper()
	r := newRulesEngine(t, rec)
	r.JS = fakeJS{kv: kv}
	return r
}

// fakeJS opens the same bucket for every name, which is all the policy needs from JetStream.
type fakeJS struct {
	jetstream.JetStream
	kv jetstream.KeyValue
}

func (js fakeJS) KeyValue(context.Context, string) (jetstream.KeyValue, error) { return js.kv, nil }

func TestRulesEnginePolicy(t *testing.T) {
	ctx := context.Background()
	rec := &rulesRecorder{}
	kv := &fakeKV{data: map[string][]byte{}}
	r := policyEngine(t, rec, kv)
	now := time.Now()
	row := UsageRow{Day: now.UTC().Format("2006-01-02"), Host: "beta", Project: "p", Session: "s", Branch: "cos/ts-1-x",
		Model: "claude-opus-5-5", Output: 30000, First: now.Add(-2 * time.Hour), Last: now, Messages: 1}
	if err := r.Store.SaveUsage(ctx, "k1", row); err != nil {
		t.Fatal(err)
	}
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{{ThreadID: "t1", Branch: "cos/ts-1-x", Model: "claude-opus-5-5", Title: "TS-1 x"}}}}}

	r.Policy(ctx, live)
	if len(rec.calls) != 1 || rec.calls[0].host != "beta" || rec.calls[0].command != "send" ||
		rec.calls[0].args[0] != "t1" || !strings.Contains(rec.calls[0].args[1], "with no Haiku 5.5 workers") {
		t.Fatalf("calls = %+v, want one no-workers nudge to beta t1", rec.calls)
	}
	if _, ok := kv.data[nudgeKey("noworkers", "t1")]; !ok {
		t.Fatalf("nudge time not recorded: %v", kv.data)
	}

	// Inside the two-hour window the no-workers nudge falls back to the share nudge, once.
	rec.calls = nil
	r.Policy(ctx, live)
	if len(rec.calls) != 1 || !strings.Contains(rec.calls[0].args[1], "did 0% of this thread's output") {
		t.Fatalf("calls = %+v, want one share nudge to beta t1", rec.calls)
	}

	// Both kinds are now inside the window: nothing is sent.
	rec.calls = nil
	r.Policy(ctx, live)
	if len(rec.calls) != 0 {
		t.Fatalf("calls = %+v, want none within the window", rec.calls)
	}
}

func TestRulesEnginePolicySkipsUnreadableNudge(t *testing.T) {
	ctx := context.Background()
	rec := &rulesRecorder{}
	r := policyEngine(t, rec, &failingKV{fakeKV{data: map[string][]byte{}}})
	now := time.Now()
	row := UsageRow{Day: now.UTC().Format("2006-01-02"), Host: "beta", Project: "p", Session: "s", Branch: "cos/ts-1-x",
		Model: "claude-opus-5-5", Output: 30000, First: now.Add(-2 * time.Hour), Last: now, Messages: 1}
	if err := r.Store.SaveUsage(ctx, "k1", row); err != nil {
		t.Fatal(err)
	}
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{{ThreadID: "t1", Branch: "cos/ts-1-x", Model: "claude-opus-5-5"}}}}}

	r.Policy(ctx, live)
	if len(rec.calls) != 0 {
		t.Fatalf("calls = %+v, want none while the last-nudge key is unreadable", rec.calls)
	}
}

// failingKV fails every read with an error other than not-found, as a KV outage would.
type failingKV struct{ fakeKV }

func (kv *failingKV) Get(context.Context, string) (jetstream.KeyValueEntry, error) {
	return nil, errors.New("nats: timeout")
}

// unwritableKV fails every write, as a KV outage would after the reads succeeded.
type unwritableKV struct{ fakeKV }

func (kv *unwritableKV) Put(context.Context, string, []byte) (uint64, error) {
	return 0, errors.New("nats: timeout")
}

// usageRowFor is one two-hour-old usage row on branch cos/ts-1-x of beta, keyed TS-1 as usageRows sets it.
func usageRowFor(now time.Time, model string) UsageRow {
	return UsageRow{Day: now.UTC().Format("2006-01-02"), Host: "beta", Project: "p", Session: "s", Branch: "cos/ts-1-x", Ticket: "TS-1",
		Model: model, Output: 30000, First: now.Add(-2 * time.Hour), Last: now, Messages: 1}
}

func TestRulesEnginePolicySkipsPausedTicket(t *testing.T) {
	ctx := context.Background()
	rec := &rulesRecorder{}
	kv := &fakeKV{data: map[string][]byte{pausedKey: []byte(`["TS-1"]`)}}
	r := policyEngine(t, rec, kv)
	now := time.Now()
	if err := r.Store.SaveUsage(ctx, "k1", usageRowFor(now, "claude-opus-5-5")); err != nil {
		t.Fatal(err)
	}
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{
		{ThreadID: "t1", Branch: "cos/ts-1-x", Model: "claude-opus-5-5", Title: "TS-1 x"},
		{ThreadID: "h1", Model: "claude-haiku-5-5", Title: "TS-1 haiku"},
	}}}}

	r.Policy(ctx, live)
	if len(rec.calls) != 0 {
		t.Fatalf("calls = %+v, want none for a paused ticket", rec.calls)
	}

	delete(kv.data, pausedKey)
	r.Policy(ctx, live)
	if len(rec.calls) != 2 || rec.calls[0].command != "send" || rec.calls[1].command != "model" {
		t.Fatalf("calls = %+v, want the nudge to t1 and the Opus switch of h1 once unpaused", rec.calls)
	}
}

func TestRulesEnginePolicyWarnsWhenNudgeWindowNotRecorded(t *testing.T) {
	ctx := context.Background()
	rec := &rulesRecorder{}
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	r := policyEngine(t, rec, &unwritableKV{fakeKV{data: map[string][]byte{}}})
	now := time.Now()
	if err := r.Store.SaveUsage(ctx, "k1", usageRowFor(now, "claude-opus-5-5")); err != nil {
		t.Fatal(err)
	}
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{{ThreadID: "t1", Branch: "cos/ts-1-x", Model: "claude-opus-5-5", Title: "TS-1 x"}}}}}

	r.Policy(ctx, live)
	if len(rec.calls) != 1 || rec.calls[0].command != "send" {
		t.Fatalf("calls = %+v, want the nudge sent once", rec.calls)
	}
	if !strings.Contains(logs.String(), "nudge window not recorded") || !strings.Contains(logs.String(), "thread=t1") || !strings.Contains(logs.String(), "kind=noworkers") {
		t.Fatalf("log = %q, want a warning naming the thread and kind", logs.String())
	}
}

func TestRulesEnginePolicyHaiku45NudgeOnce(t *testing.T) {
	ctx := context.Background()
	rec := &rulesRecorder{}
	kv := &fakeKV{data: map[string][]byte{}}
	r := policyEngine(t, rec, kv)
	now := time.Now()
	if err := r.Store.SaveUsage(ctx, "k1", usageRowFor(now, "claude-haiku-4-5")); err != nil {
		t.Fatal(err)
	}
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{{ThreadID: "t1", Branch: "cos/ts-1-x", Model: "claude-opus-5-5", Title: "TS-1 x"}}}}}

	// A failed send leaves the window open, so the next beat retries the nudge.
	rec.sendErr = errors.New("nats: timeout")
	r.Policy(ctx, live)
	if _, ok := kv.data[nudgeKey("haiku45", "t1")]; ok {
		t.Fatalf("haiku45 window recorded after a failed send: %v", kv.data)
	}
	rec.sendErr, rec.calls = nil, nil

	r.Policy(ctx, live)
	if len(rec.calls) != 1 || !strings.Contains(rec.calls[0].args[1], "claude-haiku-4-5") {
		t.Fatalf("calls = %+v, want one haiku 4.5 nudge to t1", rec.calls)
	}
	if _, ok := kv.data[nudgeKey("haiku45", "t1")]; !ok {
		t.Fatalf("haiku45 window not recorded: %v", kv.data)
	}

	rec.calls = nil
	r.Policy(ctx, live)
	if len(rec.calls) != 0 {
		t.Fatalf("calls = %+v, want none within the window", rec.calls)
	}
}

func TestUsageThreadPrefersTopLevel(t *testing.T) {
	latest := UsageRow{Host: "beta", Branch: "cos/ts-1-x"}
	var u UsageTotal
	usageThread(&u, latest, Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{
		{ThreadID: "sub", Branch: "cos/ts-1-x", Title: "subagent", Lineage: &lineageOf{Parent: "t1", Relation: "subagent"}},
		{ThreadID: "t1", Branch: "cos/ts-1-x", Title: "TS-1 x"},
	}}}})
	if u.Thread != "t1" {
		t.Fatalf("thread = %q, want the top-level t1 and not the subagent", u.Thread)
	}

	var only UsageTotal
	usageThread(&only, latest, Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{
		{ThreadID: "sub", Branch: "cos/ts-1-x", Lineage: &lineageOf{Parent: "t1", Relation: "subagent"}},
	}}}})
	if only.Thread != "sub" || !only.Subagent {
		t.Fatalf("thread = %q subagent = %v, want the subagent labelled and never a policy target", only.Thread, only.Subagent)
	}
}

func TestForkedHaikuThreadSwitchesToOpus(t *testing.T) {
	live := Live{Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{
		{ThreadID: "t2", Model: "claude-opus-5-5"},
		{ThreadID: "f1", Model: "claude-haiku-5-5", Lineage: &lineageOf{Parent: "t2", Relation: "fork"}},
	}}}}
	got := fleetModelPolicy(live, time.Now(), nil, nil)
	assertRules(t, got, []Decision{{Class: ClassAct, Rule: "model-opus-thread"}})
	if got[0].Thread != "f1" || got[0].Model["model"] != "claude-opus-5-5" {
		t.Errorf("decision = %+v, want the fork f1 switched to Opus 5.5", got[0])
	}
}

func TestModelPolicyTemplatesAreRequired(t *testing.T) {
	files := maps.Clone(embeddedBehavior().files)
	delete(files, "templates/advise_split.tmpl")
	if _, err := ParseBehavior("test", files); err == nil || !strings.Contains(err.Error(), "advise_split") {
		t.Fatalf("bundle without advise_split accepted: %v", err)
	}
}

func TestModelPolicyTemplatesRenderOneLine(t *testing.T) {
	for _, name := range []string{"advise_split", "nudge_haiku_share", "nudge_no_workers", "nudge_haiku45"} {
		out, err := CurrentBehavior().Render(name, behaviorTemplates[name])
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if out == "" || strings.Contains(out, "\n") {
			t.Errorf("%s = %q, want one non-empty line", name, out)
		}
	}
}
