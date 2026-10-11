package cos

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestWakeRecentNewestFirstAndCapped(t *testing.T) {
	var st AgentState
	for i := range agentMaxWakes + 5 {
		st.recordWake(WakeRecord{Items: i})
	}
	if len(st.Wakes) != agentMaxWakes || st.Wakes[0].Items != 5 {
		t.Fatalf("kept %d wakes, oldest Items %d; want %d kept, oldest 5", len(st.Wakes), st.Wakes[0].Items, agentMaxWakes)
	}
	got := st.recentWakes(3)
	if len(got) != 3 || got[0].Items != agentMaxWakes+4 || got[2].Items != agentMaxWakes+2 {
		t.Fatalf("recent = %+v, want the newest three, newest first", got)
	}
}

func TestWakeAttachRun(t *testing.T) {
	st := AgentState{Wakes: []WakeRecord{{Reason: "chat", Run: "r1"}, {Reason: "digest"}}}
	st.attachRun("r2")
	st.attachRun("r3")
	if st.Wakes[0].Run != "r1" || st.Wakes[1].Run != "r2" {
		t.Fatalf("runs = %q, %q; want r1 kept and r2 on the newest wake without one", st.Wakes[0].Run, st.Wakes[1].Run)
	}
}

// agentTestUsage is a finished run whose root turn carries token use; the other turn belongs to another node and must not count.
const agentTestUsage = `{"runs":[{"id":"r1","rootNodeId":"node:r1:root","status":"completed"}],"turnItems":[{"type":"assistant_message","runId":"r1","text":"Done"}],"runtimeRequests":[],"providerTurns":[{"nodeId":"node:r1:root","turnTokenUsage":{"inputTokens":100000,"cachedInputTokens":80000,"cacheCreationTokens":10000,"outputTokens":2000}},{"nodeId":"node:other","turnTokenUsage":{"inputTokens":999}}]}`

func TestAgentWakeMetering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	e := newAgentTestEnv(t, ctx)
	e.reset(t)
	e.start()
	e.say("hi")
	e.step()
	e.fake.projection = agentTestUsage
	e.step()
	wakes := e.state().Wakes
	last := wakes[len(wakes)-1]
	if last.Reason != "chat" || last.Run != "r1" || last.Input != 100000 || last.CachedInput != 80000 || last.CacheWrite != 10000 || last.Output != 2000 {
		t.Fatalf("wake record = %+v, want a chat wake on run r1 with 100000 input and 2000 output", last)
	}
}

func TestCosWakesSettings(t *testing.T) {
	ctx := context.Background()
	wakes := []WakeRecord{{At: time.Date(2026, 10, 11, 10, 4, 0, 0, time.UTC), Reason: "digest", Items: 3, Input: 100000, CachedInput: 80000, Output: 2000}}
	var page bytes.Buffer
	if err := settingsPage(true, AutopilotState{}, budgetPanelData{}, PushState{}, wakes).Render(ctx, &page); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`aria-labelledby="wakes-heading"`, "COS wakes", "<td>10:04</td>", "<td>100000</td>", "<td>2000</td>"} {
		if !strings.Contains(page.String(), want) {
			t.Fatalf("settings missing %q", want)
		}
	}
}
