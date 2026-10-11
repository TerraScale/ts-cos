package cos

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestKPIsFold(t *testing.T) {
	at := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
	ev := func(kind EventKind, data string) Event {
		return Event{Kind: kind, Time: at, Data: json.RawMessage(data)}
	}
	tests := []struct {
		name   string
		events []Event
		want   KPIs
	}{
		{"merges count", []Event{ev(KindPRMerged, ""), ev(KindPRMerged, "")}, KPIs{Merges: 2}},
		{"e2e replaces", []Event{ev(KindE2EReport, `{"pass":40,"fail":0,"skip":3}`), ev(KindE2EReport, `{"pass":41,"fail":1,"skip":2}`)}, KPIs{E2E: E2ERun{Pass: 41, Fail: 1, Skip: 2, At: at}, E2ESeen: true}},
		{"unknown data ignored", []Event{ev(KindE2EReport, `{"pass":"x"}`), ev(KindPROpened, `{"pass":1}`)}, KPIs{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var k KPIs
			for _, e := range tc.events {
				k.fold(e)
			}
			if k != tc.want {
				t.Fatalf("got %+v, want %+v", k, tc.want)
			}
		})
	}
}

func TestKPIBoardWakesSubscribers(t *testing.T) {
	var b KPIBoard
	wake, stop := b.Subscribe()
	defer stop()
	b.resetToday()
	select {
	case <-wake:
	default:
		t.Fatal("no wake after KPI change")
	}
}

func TestKPIBoardUsageIsToday(t *testing.T) {
	var b KPIBoard
	today := time.Now().UTC().Format("2006-01-02")
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	b.putUsage("y", &UsageRow{Day: yesterday, Model: "claude-haiku-5-5", Output: 1000000})
	b.putUsage("a", &UsageRow{Day: today, Model: "claude-haiku-5-5", Output: 300, Input: 1000})
	b.putUsage("b", &UsageRow{Day: today, Model: "claude-opus-5-5", Output: 100})
	k := b.Snapshot()
	if !k.UsageSeen || k.Usage.Output != 400 || k.Usage.HaikuShare != 75 {
		t.Fatalf("today's usage = seen %v output %d haiku %d%%, want seen, 400, 75%%", k.UsageSeen, k.Usage.Output, k.Usage.HaikuShare)
	}
	b.putUsage("a", nil)
	if k := b.Snapshot(); k.Usage.Output != 100 {
		t.Fatalf("after delete, output = %d, want 100", k.Usage.Output)
	}
}

func TestKPIBoardFoldsMergedPR(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nodes, _ := chatTestCluster(t, ctx)
	js, err := nodes[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "event streams", func() bool {
		ready, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		return EnsureEventStreams(ready, js) == nil
	})
	board := &KPIBoard{}
	go board.Run(ctx, nodes[0])
	e := Event{ID: NewEventID(SourceForgejo, "pr", "merged", "1"), Source: SourceForgejo, Kind: KindPRMerged, Time: time.Now().UTC(), Severity: SeverityInfo, Summary: "PR merged", Repo: "terrascale/ts-cos", PR: 1}
	if _, err := PublishEvent(ctx, js, e); err != nil {
		t.Fatal(err)
	}
	// Each consumer retries every 2 s while the stream settles, so this waits longer than the spec's 2 s.
	chatTestWait(t, "merge counted", func() bool { return board.Snapshot().Merges == 1 })
}
