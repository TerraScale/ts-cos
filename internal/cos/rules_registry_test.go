package cos

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// withRegisteredRules gives a test an empty registry and restores the real one after.
func withRegisteredRules(t *testing.T) {
	t.Helper()
	saved := registeredRules
	registeredRules = nil
	t.Cleanup(func() { registeredRules = saved })
}

func publishedAction(t *testing.T, rec *rulesRecorder, action string) AutopilotEntry {
	t.Helper()
	for _, e := range rec.published {
		if e.Action == action {
			return e
		}
	}
	t.Fatalf("no entry with action %q in %+v", action, rec.published)
	return AutopilotEntry{}
}

func TestRegisterRulePanicsOnEmptyOrDuplicateName(t *testing.T) {
	withRegisteredRules(t)
	RegisterRule(Rule{Name: "budget"})
	for _, r := range []Rule{{Name: ""}, {Name: "budget"}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("RegisterRule(%q) did not panic", r.Name)
				}
			}()
			RegisterRule(r)
		}()
	}
}

func TestRegisteredEventRuleDoGetsBusAndLogsPrefixedRule(t *testing.T) {
	withRegisteredRules(t)
	ctx := context.Background()
	rec := &rulesRecorder{sha: "aaa"}
	r := newRulesEngine(t, rec)
	r.Bus = &Bus{}
	var env RuleEnv
	RegisterRule(Rule{Name: "budget", Event: func(Event, Live) []Decision {
		return []Decision{{Class: ClassAct, Rule: "warn-80", Reason: "80% used", Do: func(_ context.Context, got RuleEnv) error {
			env = got
			return nil
		}}}
	}})
	if err := r.Handle(ctx, rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	if env.Bus != r.Bus || env.Publish == nil {
		t.Fatalf("Do got env %+v, want Bus %p and a Publish func", env, r.Bus)
	}
	entry := publishedAction(t, rec, "budget/warn-80")
	if entry.Kind != ClassAct || entry.Reason != "80% used" {
		t.Errorf("entry = %+v", entry)
	}
}

func TestPanickingEventRuleLogsErrorAndBuiltinsStillRun(t *testing.T) {
	withRegisteredRules(t)
	ctx := context.Background()
	rec := &rulesRecorder{sha: "aaa"}
	r := newRulesEngine(t, rec)
	RegisterRule(Rule{Name: "boom", Event: func(Event, Live) []Decision { panic("bad budget") }})
	if err := r.Handle(ctx, rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	entry := publishedAction(t, rec, "boom")
	if entry.Kind != "error" || !strings.Contains(entry.Reason, "bad budget") {
		t.Errorf("panic entry = %+v", entry)
	}
	if len(rec.calls) == 0 || rec.calls[0].command != "send" {
		t.Errorf("built-in CI wake did not run: %+v", rec.calls)
	}
}

func TestBeatRuleWakeCallsSend(t *testing.T) {
	withRegisteredRules(t)
	rec := &rulesRecorder{}
	r := newRulesEngine(t, rec)
	RegisterRule(Rule{Name: "nudge", Beat: func(context.Context, Live) []Decision {
		return []Decision{{Class: ClassAct, Rule: "wake", Reason: "threshold", Host: "beta", Thread: "t1", Wake: "check the budget"}}
	}})
	r.Beat(context.Background(), rulesLive())
	if len(rec.calls) != 1 || rec.calls[0].host != "beta" || rec.calls[0].command != "send" ||
		!slices.Equal(rec.calls[0].args, []string{"t1", "check the budget"}) {
		t.Fatalf("calls = %+v, want one send to beta t1", rec.calls)
	}
	entry := publishedAction(t, rec, "nudge/wake")
	if entry.Event != "beat" {
		t.Errorf("event = %q, want beat", entry.Event)
	}
}

func TestRegisteredDecisionForPausedTicketIsRecorded(t *testing.T) {
	withRegisteredRules(t)
	rec := &rulesRecorder{sha: "aaa"}
	r := newRulesEngine(t, rec)
	r.memory.(*fakeKV).data[pausedKey] = []byte(`["TS-1"]`)
	ran := false
	RegisterRule(Rule{Name: "budget", Event: func(Event, Live) []Decision {
		return []Decision{{Class: ClassAct, Ticket: "TS-1", Rule: "act", Reason: "try it", Do: func(context.Context, RuleEnv) error {
			ran = true
			return nil
		}}}
	}})
	if err := r.Handle(context.Background(), rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatal("registered Do ran for a paused ticket")
	}
	if entry := publishedAction(t, rec, "budget/act"); entry.Kind != ClassRecord {
		t.Errorf("entry kind = %q, want %q", entry.Kind, ClassRecord)
	}
}

func TestRegisteredDecisionForPausedThreadIsRecorded(t *testing.T) {
	withRegisteredRules(t)
	rec := &rulesRecorder{}
	r := newRulesEngine(t, rec)
	r.memory.(*fakeKV).data[pausedKey] = []byte(`["TS-1"]`)
	RegisterRule(Rule{Name: "nudge", Beat: func(context.Context, Live) []Decision {
		return []Decision{{Class: ClassAct, Rule: "wake", Reason: "threshold", Host: "beta", Thread: "t1", Wake: "check the budget"}}
	}})
	if err := r.Beat(context.Background(), rulesLive()); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("calls = %+v, want no wake for a paused thread", rec.calls)
	}
	if entry := publishedAction(t, rec, "nudge/wake"); entry.Kind != ClassRecord {
		t.Errorf("entry kind = %q, want %q", entry.Kind, ClassRecord)
	}
}

func TestPausedBeatRuleDoesNotAct(t *testing.T) {
	withRegisteredRules(t)
	rec := &rulesRecorder{}
	r := newRulesEngine(t, rec)
	r.memory.(*fakeKV).data[pausedKey] = []byte(`["TS-1"]`)
	ran := false
	RegisterRule(Rule{Name: "budget", Beat: func(context.Context, Live) []Decision {
		return []Decision{{Class: ClassAct, Ticket: "TS-1", Rule: "act", Reason: "try it", Do: func(context.Context, RuleEnv) error {
			ran = true
			return nil
		}}}
	}})
	if err := r.Beat(context.Background(), rulesLive()); err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatal("registered Do ran for a paused ticket")
	}
	if entry := publishedAction(t, rec, "budget/act"); entry.Kind != ClassRecord {
		t.Errorf("entry kind = %q, want %q", entry.Kind, ClassRecord)
	}
}

func TestPanickingRuleDoesNotStopTheEngine(t *testing.T) {
	withRegisteredRules(t)
	rec := &rulesRecorder{sha: "aaa"}
	r := newRulesEngine(t, rec)
	ran := false
	RegisterRule(Rule{Name: "boom", Event: func(Event, Live) []Decision {
		return []Decision{{Class: ClassAct, Rule: "act", Reason: "try it", Do: func(context.Context, RuleEnv) error {
			panic("bad action")
		}}}
	}})
	RegisterRule(Rule{Name: "after", Event: func(Event, Live) []Decision {
		return []Decision{{Class: ClassAct, Rule: "act", Reason: "try it", Do: func(context.Context, RuleEnv) error {
			ran = true
			return nil
		}}}
	}})
	if err := r.Handle(context.Background(), rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	if entry := publishedAction(t, rec, "boom/act"); entry.Kind != "error" {
		t.Errorf("panic entry = %+v, want kind error", entry)
	}
	if !ran {
		t.Fatal("rule after a panicking Do did not run")
	}

	ran = false
	RegisterRule(Rule{Name: "beat-boom", Beat: func(context.Context, Live) []Decision { panic("bad beat") }})
	RegisterRule(Rule{Name: "beat-after", Beat: func(context.Context, Live) []Decision {
		ran = true
		return nil
	}})
	if err := r.Beat(context.Background(), rulesLive()); err != nil {
		t.Fatal(err)
	}
	if entry := publishedAction(t, rec, "beat-boom"); entry.Kind != "error" {
		t.Errorf("beat panic entry = %+v, want kind error", entry)
	}
	if !ran {
		t.Fatal("beat rule after a panicking Beat did not run")
	}
}

func TestPanickingDoLogsErrorEntry(t *testing.T) {
	withRegisteredRules(t)
	rec := &rulesRecorder{sha: "aaa"}
	r := newRulesEngine(t, rec)
	RegisterRule(Rule{Name: "boom", Event: func(Event, Live) []Decision {
		return []Decision{{Class: ClassAct, Rule: "act", Reason: "try it", Do: func(context.Context, RuleEnv) error {
			panic("bad action")
		}}}
	}})
	if err := r.Handle(context.Background(), rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	entry := publishedAction(t, rec, "boom/act")
	if entry.Kind != "error" || !strings.Contains(entry.Reason, "bad action") {
		t.Errorf("entry = %+v", entry)
	}
}

func TestDoErrorLogsErrorEntry(t *testing.T) {
	withRegisteredRules(t)
	rec := &rulesRecorder{sha: "aaa"}
	r := newRulesEngine(t, rec)
	RegisterRule(Rule{Name: "fail", Event: func(Event, Live) []Decision {
		return []Decision{{Class: ClassAct, Rule: "act", Reason: "try it", Do: func(context.Context, RuleEnv) error {
			return errors.New("nats down")
		}}}
	}})
	if err := r.Handle(context.Background(), rulesCIEvent()); err != nil {
		t.Fatal(err)
	}
	entry := publishedAction(t, rec, "fail/act")
	if entry.Kind != "error" || entry.Reason != "try it; nats down" {
		t.Errorf("entry = %+v", entry)
	}
}
