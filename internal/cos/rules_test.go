package cos

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"git.terrascale.tech/TerraScale/ts-cos/behavior"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	rulesPRURL = "https://git.terrascale.tech/TerraScale/ts-cos/pulls/7"
	rulesCISig = "ci:ts-cos#7 CI failed: build"
)

// rulesPaused is the replicated paused set the engine reads from KV.
var rulesPaused = map[string]bool{"TS-2": true}

func rulesLive() Live {
	return Live{
		Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{
			{ThreadID: "t1", Title: "TS-1 · ts-cos: x", Branch: "cos/ts-1-x", Model: "claude-opus-5-5", PRs: []ThreadPR{{URL: rulesPRURL}}},
			{ThreadID: "t2", Title: "TS-2 · ts-cos: y", Branch: "cos/ts-2-y", Model: "claude-opus-5-5"},
			{ThreadID: "t3", Title: "TS-1 old", Model: "claude-haiku-4-5"},
			{ThreadID: "sub", Title: "TS-1 sub", Branch: "cos/ts-1-x", Lineage: &struct {
				Parent   string `json:"parent"`
				Relation string `json:"rel"`
			}{Parent: "t1"}},
		}}},
	}
}

func rulesPR(mod func(*PRFacts)) *PRFacts {
	pr := &PRFacts{
		PR: PR{Repo: "ts-cos", Number: 7, URL: rulesPRURL, Branch: "cos/ts-1-x", Cos: true, Issues: []string{"TS-1"},
			Mergeable: true, CI: "pending", SHA: "aaa", Base: "main"},
		State: "open",
	}
	if mod != nil {
		mod(pr)
	}
	return pr
}

func rulesPREvent(kind EventKind, number int) Event {
	return Event{ID: "forgejo:1", Source: SourceForgejo, Kind: kind, Summary: "PR update", Repo: "TerraScale/ts-cos", PR: number}
}

func assertRules(t *testing.T, got, want []Decision) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d decisions, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Class != want[i].Class || got[i].Rule != want[i].Rule {
			t.Errorf("decision %d = %s/%s, want %s/%s (reason: %s)", i, got[i].Class, got[i].Rule, want[i].Class, want[i].Rule, got[i].Reason)
		}
	}
}

func TestClassify(t *testing.T) {
	ciFail := func(p *PRFacts) { p.CI = "failure"; p.Failing = []string{"build"} }
	tests := []struct {
		name   string
		event  Event
		pr     *PRFacts
		memory WakeMemory
		want   []Decision
		check  func(*testing.T, []Decision)
	}{
		{
			name:  "ready COS PR merges on the owner's running ticket",
			event: rulesPREvent(KindCIStatus, 7),
			pr: rulesPR(func(p *PRFacts) {
				p.Ready, p.CI = true, "success"
				p.BotApproved = true
			}),
			want: []Decision{{Class: ClassAct, Rule: "merge-ready"}},
			check: func(t *testing.T, got []Decision) {
				d := got[0]
				if d.Merge == nil || d.Merge.Number != 7 || d.Merge.URL != rulesPRURL {
					t.Errorf("Merge = %+v, want PR 7", d.Merge)
				}
				if d.Host != "beta" || d.Thread != "t1" {
					t.Errorf("target = %s %s, want beta t1", d.Host, d.Thread)
				}
			},
		},
		{
			name:  "ticket with no open thread or PR records instead of merging",
			event: Event{ID: "forgejo:1", Source: SourceForgejo, Kind: KindCIStatus, Summary: "PR update", Repo: "TerraScale/ts-cos", PR: 7, Ticket: "TS-9"},
			pr: rulesPR(func(p *PRFacts) {
				p.Ready, p.CI, p.BotApproved = true, "success", true
			}),
			want: []Decision{{Class: ClassRecord, Rule: "not-running"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Merge != nil {
					t.Errorf("Merge = %+v, want nil", got[0].Merge)
				}
				if !strings.Contains(got[0].Reason, "has no open thread or PR") {
					t.Errorf("reason = %q, want it to contain %q", got[0].Reason, "has no open thread or PR")
				}
			},
		},
		{
			name:  "paused ticket records instead of merging",
			event: Event{ID: "forgejo:1", Source: SourceForgejo, Kind: KindCIStatus, Summary: "PR update", Repo: "TerraScale/ts-cos", PR: 7, Ticket: "TS-2"},
			pr: rulesPR(func(p *PRFacts) {
				p.Ready, p.CI, p.BotApproved = true, "success", true
			}),
			want: []Decision{{Class: ClassRecord, Rule: "paused"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Merge != nil {
					t.Errorf("Merge = %+v, want nil", got[0].Merge)
				}
				if !strings.Contains(got[0].Reason, "paused") {
					t.Errorf("reason = %q, want it to contain paused", got[0].Reason)
				}
			},
		},
		{
			name:  "CI failure wakes the owner with the failing check",
			event: rulesPREvent(KindCIStatus, 7),
			pr:    rulesPR(ciFail),
			want:  []Decision{{Class: ClassAct, Rule: "ci"}},
			check: func(t *testing.T, got []Decision) {
				d := got[0]
				for _, s := range []string{"CI failed", rulesPRURL, "build"} {
					if !strings.Contains(d.Wake, s) {
						t.Errorf("Wake = %q, want it to contain %q", d.Wake, s)
					}
				}
				if d.Sig != rulesCISig {
					t.Errorf("Sig = %q, want %q", d.Sig, rulesCISig)
				}
				if d.Thread != "t1" {
					t.Errorf("Thread = %q, want t1", d.Thread)
				}
			},
		},
		{
			name:   "same failure on the same SHA records only",
			event:  rulesPREvent(KindCIStatus, 7),
			pr:     rulesPR(ciFail),
			memory: WakeMemory{Sig: rulesCISig, SHA: "aaa", Count: 1},
			want:   []Decision{{Class: ClassRecord, Rule: "ci"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Wake != "" {
					t.Errorf("Wake = %q, want none", got[0].Wake)
				}
			},
		},
		{
			name:   "same failure on a new SHA escalates the model",
			event:  rulesPREvent(KindCIStatus, 7),
			pr:     rulesPR(ciFail),
			memory: WakeMemory{Sig: rulesCISig, SHA: "old", Count: 1},
			want: []Decision{
				{Class: ClassAct, Rule: "escalate-model"},
				{Class: ClassAct, Rule: "ci"},
				{Class: ClassEscalate, Rule: "same-failure-twice"},
			},
			check: func(t *testing.T, got []Decision) {
				if got[0].Model["model"] != "claude-opus-5-5" {
					t.Errorf("Model = %v, want claude-opus-5-5 before the wake", got[0].Model)
				}
				if got[1].Wake == "" {
					t.Error("second decision has no wake text")
				}
			},
		},
		{
			name:  "conflicting PR wakes the owner to rebase",
			event: rulesPREvent(KindPRConflict, 7),
			pr:    rulesPR(func(p *PRFacts) { p.Mergeable = false }),
			want:  []Decision{{Class: ClassAct, Rule: "conflict"}},
			check: func(t *testing.T, got []Decision) {
				if !strings.Contains(got[0].Wake, "CONFLICT") {
					t.Errorf("Wake = %q, want it to contain CONFLICT", got[0].Wake)
				}
			},
		},
		{
			name:  "requested changes wakes the owner for review",
			event: rulesPREvent(KindReview, 7),
			pr:    rulesPR(func(p *PRFacts) { p.ChangesRequested = true }),
			want:  []Decision{{Class: ClassAct, Rule: "review"}},
		},
		{
			name:  "oversized PR wakes the owner to split",
			event: rulesPREvent(KindPRSynchronized, 7),
			pr:    rulesPR(func(p *PRFacts) { p.Lines = 2000 }),
			want:  []Decision{{Class: ClassAct, Rule: "size"}},
			check: func(t *testing.T, got []Decision) {
				if !strings.Contains(got[0].Wake, "2000 lines") {
					t.Errorf("Wake = %q, want it to contain 2000 lines", got[0].Wake)
				}
			},
		},
		{
			name:  "oversized conflicting PR wakes for the conflict, not the size",
			event: rulesPREvent(KindPRSynchronized, 7),
			pr:    rulesPR(func(p *PRFacts) { p.Lines = 2000; p.Mergeable = false }),
			want:  []Decision{{Class: ClassAct, Rule: "conflict"}},
		},
		{
			name:  "merged slice wakes the owner for the next slice",
			event: rulesPREvent(KindPRMerged, 7),
			pr:    rulesPR(func(p *PRFacts) { p.Merged, p.State, p.Body = true, "closed", "Part of TS-1" }),
			want:  []Decision{{Class: ClassAct, Rule: "slice"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Thread != "t1" {
					t.Errorf("Thread = %q, want t1", got[0].Thread)
				}
			},
		},
		{
			name:  "merged ticket records completion",
			event: rulesPREvent(KindPRMerged, 7),
			pr:    rulesPR(func(p *PRFacts) { p.Merged, p.State, p.Body = true, "closed", "Closes TS-1" }),
			want:  []Decision{{Class: ClassRecord, Rule: "merged"}},
		},
		{
			name:  "PR no thread owns escalates as orphan",
			event: rulesPREvent(KindCIStatus, 99),
			pr: rulesPR(func(p *PRFacts) {
				p.Number, p.URL, p.Branch = 99, "https://git.terrascale.tech/TerraScale/ts-cos/pulls/99", "cos/ts-1-other"
				ciFail(p)
			}),
			want: []Decision{{Class: ClassEscalate, Rule: "orphan"}},
		},
		{
			name:  "worker question escalates",
			event: Event{ID: "t3:req-1", Source: SourceT3, Kind: KindRequestPending, Summary: "approve the migration?", Host: "beta", Thread: "t1"},
			want:  []Decision{{Class: ClassEscalate, Rule: "worker-question"}},
		},
		{
			name:  "failed run wakes the thread",
			event: Event{ID: "t3:turn-9", Source: SourceT3, Kind: KindTurnFailed, Summary: "exit 1", Host: "beta", Thread: "t1"},
			want:  []Decision{{Class: ClassAct, Rule: "failed"}},
			check: func(t *testing.T, got []Decision) {
				if !strings.Contains(got[0].Wake, "exit 1") {
					t.Errorf("Wake = %q, want it to contain exit 1", got[0].Wake)
				}
				if got[0].Host != "beta" || got[0].Thread != "t1" {
					t.Errorf("target = %s %s, want beta t1", got[0].Host, got[0].Thread)
				}
			},
		},
		{
			name:  "real failure in a top-level thread still wakes it",
			event: Event{ID: "t3:turn-11", Source: SourceT3, Kind: KindTurnFailed, Summary: "go test failed", Host: "beta", Thread: "t1"},
			want:  []Decision{{Class: ClassAct, Rule: "failed"}},
			check: func(t *testing.T, got []Decision) {
				if !strings.Contains(got[0].Wake, "go test failed") {
					t.Errorf("Wake = %q, want it to contain go test failed", got[0].Wake)
				}
			},
		},
		{
			name:  "subagent failure is left to its parent",
			event: Event{ID: "t3:turn-12", Source: SourceT3, Kind: KindTurnFailed, Summary: "go test failed", Host: "beta", Thread: "sub"},
			want:  []Decision{{Class: ClassRecord, Rule: "child-thread"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Wake != "" || got[0].Model != nil {
					t.Errorf("Wake = %q, Model = %v, want neither", got[0].Wake, got[0].Model)
				}
			},
		},
		{
			name:  "subagent rate limit is not woken or escalated",
			event: Event{ID: "t3:turn-13", Source: SourceT3, Kind: KindTurnFailed, Summary: "Claude API rate limit reached", Host: "beta", Thread: "sub"},
			want:  []Decision{{Class: ClassRecord, Rule: "child-thread"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Wake != "" || got[0].Model != nil {
					t.Errorf("Wake = %q, Model = %v, want neither", got[0].Wake, got[0].Model)
				}
			},
		},
		{
			name:  "top-level rate limit records without waking",
			event: Event{ID: "t3:turn-14", Source: SourceT3, Kind: KindTurnFailed, Summary: "Claude API rate limit reached. Try again later.", Host: "beta", Thread: "t1"},
			want:  []Decision{{Class: ClassRecord, Rule: "transient-failure"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Wake != "" || got[0].Model != nil {
					t.Errorf("Wake = %q, Model = %v, want neither", got[0].Wake, got[0].Model)
				}
			},
		},
		{
			name:  "cancelled top-level run is recorded, never woken",
			event: Event{ID: "t3:turn-20", Source: SourceT3, Kind: KindTurnInterrupted, Summary: "turn cancelled", Host: "beta", Thread: "t1"},
			want:  []Decision{{Class: ClassRecord, Rule: "interrupted"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Wake != "" || got[0].Model != nil {
					t.Errorf("Wake = %q, Model = %v, want neither", got[0].Wake, got[0].Model)
				}
			},
		},
		{
			name:  "credential cooldown is transient",
			event: Event{ID: "t3:turn-21", Source: SourceT3, Kind: KindTurnFailed, Summary: "API Error: Server is temporarily limiting requests (not your usage limit) · All credentials for model claude-opus-5-5 are cooling down", Host: "beta", Thread: "t1"},
			want:  []Decision{{Class: ClassRecord, Rule: "transient-failure"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Wake != "" || got[0].Model != nil {
					t.Errorf("Wake = %q, Model = %v, want neither", got[0].Wake, got[0].Model)
				}
			},
		},
		{
			name:   "repeated top-level rate limit does not escalate the model",
			event:  Event{ID: "t3:turn-15", Source: SourceT3, Kind: KindTurnFailed, Summary: "Claude API rate limit reached. Try again later.", Host: "beta", Thread: "t1"},
			memory: WakeMemory{Sig: "failed:run failed: Claude API rate limit reached. Try again later.", SHA: "t3:turn-15", Count: 1},
			want:   []Decision{{Class: ClassRecord, Rule: "transient-failure"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Wake != "" || got[0].Model != nil {
					t.Errorf("Wake = %q, Model = %v, want neither", got[0].Wake, got[0].Model)
				}
			},
		},
		{
			name:  "haiku owner is moved to opus 5.5 high on completion",
			event: Event{ID: "t3:turn-10", Source: SourceT3, Kind: KindTurnCompleted, Summary: "done", Host: "beta", Thread: "t3"},
			want:  []Decision{{Class: ClassAct, Rule: "model-opus-thread"}, {Class: ClassRecord, Rule: "progress"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Model["model"] != "claude-opus-5-5" {
					t.Errorf("Model = %v, want claude-opus-5-5", got[0].Model)
				}
			},
		},
		{
			name:  "node alert files an Inbox FYI",
			event: Event{ID: "node:disk:delta:/:2026-10-11", Source: SourceNode, Kind: KindNodeAlert, Severity: SeverityWarn, Summary: "delta: disk / is 87% full (alert above 85%)", Host: "delta"},
			want:  []Decision{{Class: ClassAct, Rule: "node-alert"}},
			check: func(t *testing.T, got []Decision) {
				f := got[0].Inbox
				if f == nil || f.Kind != "fyi" || f.Key != "node:disk:delta:/:2026-10-11" || f.Host != "delta" {
					t.Fatalf("FYI = %+v, want an fyi keyed by the event on delta", f)
				}
				if _, err := f.item(time.Now()); err != nil {
					t.Errorf("FYI is not a valid Inbox input: %v", err)
				}
			},
		},
		{
			name:  "synthetic node alert only records",
			event: Event{ID: "node:missing:e2e-abc123:1", Source: SourceNode, Kind: KindNodeAlert, Severity: SeverityInfo, Summary: "e2e-abc123: no node sample", Host: "e2e-abc123", Synthetic: true},
			want:  []Decision{{Class: ClassRecord, Rule: "node-alert"}},
			check: func(t *testing.T, got []Decision) {
				if got[0].Inbox != nil {
					t.Errorf("FYI = %+v, want none", got[0].Inbox)
				}
			},
		},
		{
			name:  "user chat is left to the COS inbox",
			event: Event{ID: "user:1", Source: SourceUser, Kind: KindChatMessage, Summary: "hello"},
			want:  nil,
		},
		{
			name:  "push to main records state",
			event: Event{ID: "forgejo:push-1", Source: SourceForgejo, Kind: KindPushMain, Summary: "main moved to abc"},
			want:  []Decision{{Class: ClassRecord, Rule: "state"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.event, rulesLive(), tt.pr, tt.memory, rulesPaused)
			assertRules(t, got, tt.want)
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestRulesTicketRunning(t *testing.T) {
	tests := []struct {
		name    string
		ticket  string
		prs     []PR
		running bool
		reason  string
	}{
		{name: "empty ticket is not COS work", ticket: "", running: false, reason: "no ticket key"},
		{name: "open thread runs", ticket: "TS-1", running: true},
		{name: "open PR runs", ticket: "TS-3", prs: []PR{{Repo: "ts-cos", Number: 9, Issues: []string{"TS-3"}}}, running: true},
		{name: "paused ticket is paused", ticket: "TS-2", running: false, reason: "paused"},
		{name: "no open thread or PR does not run", ticket: "TS-9", running: false, reason: "has no open thread or PR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			live := rulesLive()
			live.PRs = tt.prs
			running, reason := ticketRunning(live, rulesPaused, tt.ticket)
			if running != tt.running {
				t.Errorf("running = %t, want %t", running, tt.running)
			}
			if !strings.Contains(reason, tt.reason) {
				t.Errorf("reason = %q, want it to contain %q", reason, tt.reason)
			}
		})
	}
}

func TestRulesPROwnerSkipsSubagent(t *testing.T) {
	host, owner := prOwner(rulesLive(), PR{Repo: "ts-cos", Number: 7, URL: rulesPRURL, Branch: "cos/ts-1-x"})
	if owner == nil {
		t.Fatal("no owner, want t1")
	}
	if owner.ThreadID != "t1" || host != "beta" {
		t.Errorf("owner = %s %s, want beta t1", host, owner.ThreadID)
	}
}

type fakeEntry struct {
	jetstream.KeyValueEntry
	value []byte
}

func (e fakeEntry) Value() []byte    { return e.value }
func (e fakeEntry) Revision() uint64 { return 1 }

type fakeKV struct {
	jetstream.KeyValue
	data map[string][]byte
}

func (kv *fakeKV) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	v, ok := kv.data[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return fakeEntry{value: v}, nil
}

func (kv *fakeKV) Put(_ context.Context, key string, value []byte) (uint64, error) {
	kv.data[key] = value
	return 1, nil
}

type rulesCall struct {
	host, command string
	args          []string
}

type rulesRecorder struct {
	sha       string
	sendErr   error
	fyiErr    error
	merges    int
	calls     []rulesCall
	fyi       []InboxInput
	published []AutopilotEntry
}

func newRulesEngine(t *testing.T, rec *rulesRecorder) *RulesEngine {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.SaveLive(context.Background(), rulesLive()); err != nil {
		t.Fatal(err)
	}
	return &RulesEngine{
		Store: store,
		PR: func(context.Context, string, int) (*PRFacts, error) {
			return rulesPR(func(p *PRFacts) { p.CI, p.Failing, p.SHA = "failure", []string{"build"}, rec.sha }), nil
		},
		Merge: func(context.Context, string, int) error { rec.merges++; return nil },
		Inbox: func(_ context.Context, in InboxInput) error {
			rec.fyi = append(rec.fyi, in)
			return rec.fyiErr
		},
		Call: func(_ context.Context, host, command string, args []string) error {
			rec.calls = append(rec.calls, rulesCall{host, command, args})
			if command == "send" {
				return rec.sendErr
			}
			return nil
		},
		Publish: func(_ context.Context, e AutopilotEntry) { rec.published = append(rec.published, e) },
		memory:  &fakeKV{data: map[string][]byte{}},
	}
}

func rulesCIEvent() Event {
	return Event{ID: "forgejo:ci-1", Source: SourceForgejo, Kind: KindCIStatus, Summary: "CI failed on PR 7", Repo: "TerraScale/ts-cos", PR: 7}
}

func TestRulesEngineHandle(t *testing.T) {
	ctx := context.Background()
	rec := &rulesRecorder{sha: "aaa"}
	r := newRulesEngine(t, rec)
	kv := r.memory.(*fakeKV)

	// First failure on a SHA wakes t1 once.
	if err := r.Handle(ctx, rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 1 || rec.calls[0].host != "beta" || rec.calls[0].command != "send" ||
		rec.calls[0].args[0] != "t1" || !strings.Contains(rec.calls[0].args[1], "CI failed") {
		t.Fatalf("calls = %+v, want one send to t1 about CI failed", rec.calls)
	}
	if len(rec.published) != 1 || rec.published[0].Kind != "act" {
		t.Fatalf("published = %+v, want one act entry", rec.published)
	}
	if n := storedWakeCount(t, kv); n != 1 {
		t.Fatalf("stored wake count = %d, want 1", n)
	}

	// A new SHA with the same failure escalates t1's model and wakes it again.
	rec.calls, rec.published, rec.sha = nil, nil, "bbb"
	if err := r.Handle(ctx, rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 2 || rec.calls[0].command != "model" || rec.calls[0].args[0] != "t1" ||
		!strings.Contains(rec.calls[0].args[1], "claude-opus-5-5") || rec.calls[1].command != "send" {
		t.Fatalf("calls = %+v, want a model change for t1 to claude-opus-5-5, then the wake", rec.calls)
	}
	if last := rec.published[len(rec.published)-1]; last.Kind != "escalate" {
		t.Fatalf("last published = %+v, want escalate", last)
	}
	if n := storedWakeCount(t, kv); n != 2 {
		t.Fatalf("stored wake count = %d, want 2", n)
	}

	// The same SHA again is already woken: no new send, no new model change.
	rec.calls, rec.published = nil, nil
	if err := r.Handle(ctx, rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("calls = %+v, want none", rec.calls)
	}
	if len(rec.published) != 1 || rec.published[0].Kind != "record" {
		t.Fatalf("published = %+v, want one record entry", rec.published)
	}
}

func TestRulesEngineHandlePausedRecordsOnly(t *testing.T) {
	rec := &rulesRecorder{sha: "aaa"}
	r := newRulesEngine(t, rec)
	r.memory.(*fakeKV).data[pausedKey] = []byte(`["TS-1"]`)
	if err := r.Handle(context.Background(), rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("calls = %+v, want none for a paused ticket", rec.calls)
	}
	if len(rec.published) != 1 || rec.published[0].Kind != ClassRecord || rec.published[0].Action != "paused" {
		t.Fatalf("published = %+v, want one paused record", rec.published)
	}
}

func TestRulesEngineHandleNodeAlert(t *testing.T) {
	ctx := context.Background()
	rec := &rulesRecorder{}
	r := newRulesEngine(t, rec)
	e := Event{ID: "node:disk:delta:/:2026-10-11", Source: SourceNode, Kind: KindNodeAlert, Severity: SeverityWarn,
		Summary: "delta: disk / is 87% full (alert above 85%)", Host: "delta"}
	if err := r.Handle(ctx, e); err != nil {
		t.Fatal(err)
	}
	if len(rec.fyi) != 1 || rec.fyi[0].Key != e.ID || len(rec.calls) != 0 {
		t.Fatalf("fyi = %+v, calls = %+v, want one FYI keyed by the event and no calls", rec.fyi, rec.calls)
	}
	if len(rec.published) != 1 || rec.published[0].Kind != ClassAct {
		t.Fatalf("published = %+v, want one act entry", rec.published)
	}
	// A failed Inbox write shows as an error in the feed (the shared Inbox executor does not redeliver).
	rec.fyiErr = errors.New("inbox unavailable")
	if err := r.Handle(ctx, e); err != nil || len(rec.published) != 2 || rec.published[1].Kind != "error" {
		t.Fatalf("Handle = %v, published = %+v, want an error entry", err, rec.published)
	}
}

func TestRulesEngineHandleWakeFailure(t *testing.T) {
	rec := &rulesRecorder{sha: "aaa", sendErr: errors.New("bus down")}
	r := newRulesEngine(t, rec)
	err := r.Handle(context.Background(), rulesCIEvent())
	if err == nil || !strings.Contains(err.Error(), "wake t1") {
		t.Fatalf("Handle error = %v, want a wake t1 error", err)
	}
}

func storedWakeCount(t *testing.T, kv *fakeKV) int {
	t.Helper()
	if len(kv.data) != 1 {
		t.Fatalf("wake memory entries = %d, want 1", len(kv.data))
	}
	var m WakeMemory
	for _, v := range kv.data {
		if err := json.Unmarshal(v, &m); err != nil {
			t.Fatal(err)
		}
	}
	return m.Count
}

func TestWakeTemplatesAreRequiredBehavior(t *testing.T) {
	files := map[string]string{}
	err := fs.WalkDir(behavior.Files, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := behavior.Files.ReadFile(name)
		files[name] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range wakeRules {
		if out, err := embeddedBehavior().Render("wake_"+rule, wakeData{PR: "u", Checks: "c", Summary: "s", Lines: 1}); err != nil || out == "" {
			t.Fatalf("wake_%s: %q %v", rule, out, err)
		}
	}
	delete(files, "templates/wake_ci.tmpl")
	if _, err := ParseBehavior("test", files); err == nil || !strings.Contains(err.Error(), "wake_ci") {
		t.Fatalf("bundle without wake_ci accepted: %v", err)
	}
	files["templates/wake_ci.tmpl"] = "line one\nline two {{.PR}}"
	if _, err := ParseBehavior("test", files); err == nil {
		t.Fatal("multi-line wake accepted")
	}
}
