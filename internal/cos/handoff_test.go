package cos

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const handoffKey = "wake.test"

// handoffRecorder fakes the bus for a handoff: the failed thread's info, each command in order,
// the launch input and what memory held when the old thread settled.
type handoffRecorder struct {
	kv             *fakeKV
	info           map[string]any
	commands       []string
	launch         map[string]any
	handedAtSettle string
	published      []AutopilotEntry
}

func (rec *handoffRecorder) call(_ context.Context, _ string, command string, args []string, out any) error {
	rec.commands = append(rec.commands, command)
	switch command {
	case "thread":
		*out.(*map[string]any) = rec.info
	case "launch":
		if err := json.Unmarshal([]byte(args[0]), &rec.launch); err != nil {
			return err
		}
		*out.(*map[string]any) = map[string]any{"threadId": "opus-2"}
	case "settle":
		var m WakeMemory
		_ = json.Unmarshal(rec.kv.data[handoffKey], &m)
		rec.handedAtSettle = m.HandedTo
	}
	return nil
}

func newHandoffRecorder(info map[string]any) *handoffRecorder {
	return &handoffRecorder{kv: &fakeKV{data: map[string][]byte{}}, info: info}
}

func handoffInfo() map[string]any {
	return map[string]any{"worktreePath": "/wt/ts-820", "title": "TS-820 · ts-cos: handoff", "branch": "cos/ts-820-handoff", "lastReply": "Pushed PR #90"}
}

func handoffLive(model string) Live {
	return Live{Fleet: []HostFleet{{Host: "delta", Threads: []FleetThread{{ThreadID: "sol-1", Project: "ts-cos", Title: "TS-820 · ts-cos: handoff", Branch: "cos/ts-820-handoff", Model: model}}}}}
}

// handoffEvent is a failed run on sol-1; handoffMemory says the same failure already woke it once.
func handoffEvent() Event {
	return Event{ID: "run-2", Source: SourceT3, Kind: KindTurnFailed, Host: "delta", Thread: "sol-1", Summary: "boom"}
}

func handoffMemory() WakeMemory {
	return WakeMemory{Sig: "failed:run failed: boom", SHA: "run-1", Count: 1}
}

func TestClassifyHandsSolFailureToLeader(t *testing.T) {
	got := Classify(handoffEvent(), handoffLive("gpt-6.1-sol"), nil, handoffMemory(), nil)
	if len(got) != 2 || got[0].Rule != "handoff-opus" || got[0].Handoff != "opus" || got[0].Model != nil {
		t.Fatalf("decisions = %+v, want a handoff to opus with no model switch", got)
	}
	if got[1].Class != ClassEscalate || got[1].Rule != "same-failure-twice" {
		t.Fatalf("second decision = %+v, want the same-failure-twice escalation", got[1])
	}
}

func TestClassifyOpusFailureEscalatesEffort(t *testing.T) {
	got := Classify(handoffEvent(), handoffLive("claude-opus-5-5"), nil, handoffMemory(), nil)
	if len(got) == 0 || got[0].Rule != "escalate-model" || got[0].Model == nil || got[0].Handoff != "" {
		t.Fatalf("decisions = %+v, want escalate-model with no handoff", got)
	}
}

func TestClassifyNeverHandsOffSubagent(t *testing.T) {
	live := handoffLive("gpt-6.1-sol")
	live.Fleet[0].Threads[0].Lineage = &struct {
		Parent   string `json:"parent"`
		Relation string `json:"rel"`
	}{Parent: "parent-1", Relation: "subagent"}
	for _, d := range Classify(handoffEvent(), live, nil, handoffMemory(), nil) {
		if d.Handoff != "" {
			t.Fatalf("subagent got a handoff: %+v", d)
		}
	}
}

func handoffDecision(t *testing.T) Decision {
	t.Helper()
	got := Classify(handoffEvent(), handoffLive("gpt-6.1-sol"), nil, handoffMemory(), nil)
	if len(got) == 0 || got[0].Handoff == "" {
		t.Fatalf("decisions = %+v, want a handoff", got)
	}
	return got[0]
}

func TestHandoffLaunchesLeaderThreadThenSettles(t *testing.T) {
	ctx := context.Background()
	rec := newHandoffRecorder(handoffInfo())
	r := &RulesEngine{Call: rec.call, memory: rec.kv, Publish: func(_ context.Context, e AutopilotEntry) { rec.published = append(rec.published, e) }}
	if err := r.execute(ctx, "run-2", handoffDecision(t), nil, handoffKey, handoffMemory()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rec.commands, ","); got != "thread,launch,settle" {
		t.Fatalf("commands = %s, want thread,launch,settle", got)
	}
	sel, _ := rec.launch["modelSelection"].(map[string]any)
	if sel["model"] != "claude-opus-5-5" {
		t.Fatalf("launch model = %v, want claude-opus-5-5", sel["model"])
	}
	msg, _ := rec.launch["message"].(string)
	for _, want := range []string{"hit the same failure twice: run failed: boom. You take over", "Pushed PR #90", CurrentBehavior().Models.BriefLine("opus")} {
		if !strings.Contains(msg, want) {
			t.Fatalf("launch message misses %q:\n%s", want, msg)
		}
	}
	if rec.handedAtSettle != "opus-2" {
		t.Fatalf("memory at settle HandedTo = %q, want opus-2", rec.handedAtSettle)
	}
	if e := rec.published[0]; e.Target != "delta opus-2" || e.Reason != "handed off to opus thread opus-2" {
		t.Fatalf("entry = %+v, want the new thread named", e)
	}
}

func TestHandoffRetrySettlesWithoutLaunch(t *testing.T) {
	ctx := context.Background()
	rec := newHandoffRecorder(handoffInfo())
	r := &RulesEngine{Call: rec.call, memory: rec.kv, Publish: func(context.Context, AutopilotEntry) {}}
	memory := handoffMemory()
	memory.HandedTo = "opus-2"
	if err := r.execute(ctx, "run-2", handoffDecision(t), nil, handoffKey, memory); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rec.commands, ","); got != "settle" {
		t.Fatalf("commands = %s, want settle only", got)
	}
}

func TestHandoffWithoutWorktreeEscalates(t *testing.T) {
	ctx := context.Background()
	rec := newHandoffRecorder(map[string]any{"title": "TS-820"})
	r := &RulesEngine{Call: rec.call, memory: rec.kv, Publish: func(_ context.Context, e AutopilotEntry) { rec.published = append(rec.published, e) }}
	if err := r.execute(ctx, "run-2", handoffDecision(t), nil, handoffKey, handoffMemory()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rec.commands, ","); got != "thread" {
		t.Fatalf("commands = %s, want thread only", got)
	}
	if e := rec.published[0]; e.Kind != ClassEscalate || !strings.Contains(e.Reason, "no worktree to hand off; needs a manual handoff") {
		t.Fatalf("entry = %+v, want an escalate for the manual handoff", e)
	}
}
