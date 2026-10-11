package cos

import (
	"encoding/json"
	"slices"
	"testing"
	"time"
)

const (
	effWorker  = "claude-haiku-5-5"
	effAdvisor = "claude-opus-5-5"
)

func effRow(project, model, ticket, branch string, output int64) UsageRow {
	return UsageRow{Day: "2026-10-11", Host: "delta", Project: project, Session: project + model, Model: model, Ticket: ticket, Branch: branch, Output: output}
}

func TestEfficiencyOfWorkerShareAndLowShare(t *testing.T) {
	rows := []UsageRow{
		effRow("a", effAdvisor, "", "", 20000),
		effRow("a", effWorker, "", "", 5000),
		effRow("b", effWorker, "", "", 30000),
		effRow("c", effAdvisor, "", "", 9000), // under the 10,000 floor: never listed
		effRow("d", effAdvisor, "", "", 7200),
		effRow("d", effWorker, "", "", 4800),
	}
	e := EfficiencyOf("today", rows, rows, nil, 0)
	if e.Output != 76000 || e.WorkerShare != 52 {
		t.Fatalf("output %d worker share %d, want 76000 and 52", e.Output, e.WorkerShare)
	}
	want := []ThreadShare{{Thread: "delta/a", Output: 25000, WorkerShare: 20}, {Thread: "delta/d", Output: 12000, WorkerShare: 40}}
	if !slices.Equal(e.LowShare, want) {
		t.Fatalf("LowShare = %+v, want %+v", e.LowShare, want)
	}
}

func TestEfficiencyOfWasteAndSavings(t *testing.T) {
	row := effRow("a", effAdvisor, "", "", 700)
	row.CacheWrite5m = 300
	row.Waste = map[string]int64{"gate": 200, "retry": 100}
	e := EfficiencyOf("today", []UsageRow{row}, []UsageRow{row}, nil, 300)
	if e.Tokens != 1000 || e.WastePct != 30 || e.Waste["gate"] != 200 || e.Waste["retry"] != 100 {
		t.Fatalf("tokens %d waste %d%% %v, want 1000, 30%%, gate 200, retry 100", e.Tokens, e.WastePct, e.Waste)
	}
	if e.SavingsPct != 23 || e.SavingsTarget != 30 || e.Saved != 300 {
		t.Fatalf("savings %d%% of target %d, saved %d; want 23%%, 30, 300", e.SavingsPct, e.SavingsTarget, e.Saved)
	}
}

func TestEfficiencyOfMergedPRsMedian(t *testing.T) {
	inRange := effRow("a", effWorker, "TS-826", "cos/ts-826-x", 1000)
	started := effRow("a", effWorker, "TS-820", "cos/ts-820-x", 9000) // started before the range: only in allRows
	merged := []Event{
		{Kind: KindPRMerged, Repo: "terrascale/ts-cos", PR: 10, Ticket: "TS-820", Time: time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC), Data: json.RawMessage(`{"branch":"cos/ts-820-x"}`)},
		{Kind: KindPRMerged, Repo: "terrascale/ts-cos", PR: 11, Ticket: "TS-826", Time: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC), Data: json.RawMessage(`{"branch":"cos/ts-826-x"}`)},
		{Kind: KindPRMerged, Repo: "terrascale/ts-cos", PR: 12, Time: time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)}, // no branch: skipped
	}
	e := EfficiencyOf("today", []UsageRow{inRange}, []UsageRow{inRange, started}, merged, 0)
	if len(e.Merged) != 2 || e.Merged[0].PR != 11 || e.Merged[1].PR != 10 {
		t.Fatalf("Merged = %+v, want PR 11 then PR 10", e.Merged)
	}
	if e.Merged[1].Output != 9000 {
		t.Fatalf("PR 10 output %d, want 9000 from allRows", e.Merged[1].Output)
	}
	if e.OutputPerPR != 1000 {
		t.Fatalf("OutputPerPR %d, want 1000 (lower middle of 1000 and 9000)", e.OutputPerPR)
	}
}

func TestEfficiencyOfDuplicateMergeCountsOnce(t *testing.T) {
	inRange := effRow("a", effWorker, "TS-826", "cos/ts-826-x", 1000)
	started := effRow("a", effWorker, "TS-820", "cos/ts-820-x", 9000)
	merge := Event{Kind: KindPRMerged, Repo: "terrascale/ts-cos", PR: 10, Ticket: "TS-820", Time: time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC), Data: json.RawMessage(`{"branch":"cos/ts-820-x"}`)}
	other := Event{Kind: KindPRMerged, Repo: "terrascale/ts-cos", PR: 11, Ticket: "TS-826", Time: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC), Data: json.RawMessage(`{"branch":"cos/ts-826-x"}`)}
	merged := []Event{merge, other, merge}
	e := EfficiencyOf("today", []UsageRow{inRange}, []UsageRow{inRange, started}, merged, 0)
	if len(e.Merged) != 2 || e.Merged[0].PR != 11 || e.Merged[1].PR != 10 {
		t.Fatalf("Merged = %+v, want PR 11 then PR 10 once each", e.Merged)
	}
	if e.OutputPerPR != 1000 {
		t.Fatalf("OutputPerPR %d, want 1000; a duplicate PR 10 would give 9000", e.OutputPerPR)
	}
}

func TestEfficiencyOfTopSinks(t *testing.T) {
	adv := effRow("a", effAdvisor, "TS-826", "", 400)
	adv.CacheWrite5m = 200
	rows := []UsageRow{
		adv,
		effRow("b", effWorker, "TS-827", "", 300),
		effRow("c", effAdvisor, "", "", 100),
		effRow("d", effWorker, "TS-828", "", 50),
	}
	e := EfficiencyOf("today", rows, rows, nil, 0)
	if e.Tokens != 1050 || len(e.Top) != 3 {
		t.Fatalf("tokens %d, top %+v; want 1050 and 3 sinks", e.Tokens, e.Top)
	}
	want := []TokenSink{
		{Label: "TS-826 advisor", Tokens: 600, Pct: 57},
		{Label: "TS-827 workers", Tokens: 300, Pct: 29},
		{Label: "(no ticket) advisor", Tokens: 100, Pct: 10},
	}
	if !slices.Equal(e.Top, want) {
		t.Fatalf("Top = %+v, want %+v", e.Top, want)
	}
}

func TestEfficiencyOfZeroData(t *testing.T) {
	e := EfficiencyOf("today", nil, nil, nil, 0)
	if e.Tokens != 0 || e.WorkerShare != 0 || e.WastePct != 0 || e.OutputPerPR != 0 || e.SavingsPct != 0 {
		t.Fatalf("zero data gave %+v", e)
	}
	if len(e.LowShare) != 0 || len(e.Merged) != 0 || len(e.Top) != 0 || e.SavingsTarget != savingsTargetPercent {
		t.Fatalf("zero data lists or target wrong: %+v", e)
	}
}

func TestEventsOfKindFiltersKindAndTime(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	since := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	events := []Event{
		{ID: "old", Kind: KindPRMerged, Time: since.Add(-time.Hour), PR: 1},
		{ID: "merged", Kind: KindPRMerged, Time: since.Add(time.Hour), PR: 2},
		{ID: "opened", Kind: KindPROpened, Time: since.Add(2 * time.Hour), PR: 3},
	}
	for i, e := range events {
		if err := s.insertEvent(ctx, int64(i+1), e, "TS-826"); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.EventsOfKind(ctx, KindPRMerged, since)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PR != 2 {
		t.Fatalf("EventsOfKind = %+v, want only PR 2", got)
	}
}
