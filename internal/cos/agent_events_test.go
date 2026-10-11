package cos

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestDigestDue(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	warn := func(ago time.Duration) Event { return Event{Severity: SeverityWarn, Time: now.Add(-ago)} }
	urgent := func(ago time.Duration) Event { return Event{Severity: SeverityUrgent, Time: now.Add(-ago)} }
	fullHour := func() AgentState {
		var st AgentState
		for range CurrentBehavior().Wake.MaxWakesPerHour {
			st.Wakes = append(st.Wakes, WakeRecord{At: now.Add(-time.Minute), Reason: "digest"})
		}
		return st
	}
	cases := []struct {
		name   string
		st     AgentState
		events []Event
		want   bool
	}{
		{"urgent waits the delay", AgentState{}, []Event{urgent(time.Minute)}, false},
		{"urgent wakes after the delay", AgentState{}, []Event{urgent(agentUrgentDelay)}, true},
		{"warn waits DigestSeconds", AgentState{}, []Event{warn(time.Minute)}, false},
		{"warn wakes after DigestSeconds", AgentState{}, []Event{warn(10 * time.Minute)}, true},
		{"hourly limit holds a warn digest", fullHour(), []Event{warn(10 * time.Minute)}, false},
		{"digests older than an hour do not count", AgentState{Wakes: []WakeRecord{{At: now.Add(-2 * time.Hour), Reason: "digest"}}}, []Event{warn(10 * time.Minute)}, true},
		{"urgent bypasses the hourly limit", fullHour(), []Event{urgent(agentUrgentDelay)}, true},
	}
	for _, c := range cases {
		if got := digestDue(c.st, c.events, now); got != c.want {
			t.Errorf("%s: digestDue = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestAgentEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	e := newAgentTestEnv(t, ctx)
	chatTestWait(t, "COS event streams", func() bool { return EnsureEventStreams(ctx, e.js) == nil })
	e.reset(t)
	// The first step starts the event cursor; the answer must be published after it.
	e.step()
	data, err := json.Marshal(AnswerData{Text: "rebase on main"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PublishEvent(ctx, e.js, Event{ID: NewEventID(SourceUser, "answer-1"), Source: SourceUser, Kind: KindUserAnswer, Time: time.Now().UTC(), Severity: SeverityInfo,
		Summary: "rebase on main", Item: "inbox-1", Data: data}); err != nil {
		t.Fatal(err)
	}
	e.a.eventCheck = time.Time{}
	e.step()
	e.expectLaunches(1)
	e.expectBrief("Inbox answer on inbox-1: rebase on main")
	// The cursor moved past the answer, so the next check must not deliver it again.
	e.a.eventCheck = time.Time{}
	e.step()
	e.expectLaunches(1)
	if len(e.fake.sends) != 0 {
		t.Fatalf("answer delivered twice: %d sends", len(e.fake.sends))
	}
}
