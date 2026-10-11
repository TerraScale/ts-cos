package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestSavingsRecordAndReport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kv := memoryTestBuckets(t, ctx, savingsBucket)[0]
	day1 := time.Date(2026, 10, 11, 15, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, -1)

	for _, rec := range []struct {
		kind         string
		tokens, wall int
		now          time.Time
	}{
		{SavingDedup, 50, 0, day1},
		{SavingParallel, 0, 120, day1},
		{SavingRetrieval, 30, 0, day2},
	} {
		if err := RecordSaving(ctx, kv, rec.kind, rec.tokens, rec.wall, rec.now); err != nil {
			t.Fatal(err)
		}
	}

	report, err := SavingsReport(ctx, kv, 3, day1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report) != 3 || report[0].Day != "2026-10-11" || report[1].Day != "2026-10-10" || report[2].Day != "2026-10-09" {
		t.Fatalf("days: %+v", report)
	}
	today := report[0]
	if today.Avoided != 50 || today.WallSeconds != 120 {
		t.Fatalf("today: %+v", today)
	}
	if today.Kinds[SavingDedup].Events != 1 || today.Kinds[SavingDedup].Tokens != 50 {
		t.Fatalf("dedup counter: %+v", today.Kinds[SavingDedup])
	}
	yesterday := report[1]
	if yesterday.Avoided != 30 {
		t.Fatalf("yesterday: %+v", yesterday)
	}
	if empty := report[2]; len(empty.Kinds) != 0 {
		t.Fatalf("empty day: %+v", empty)
	}
}

func TestSavingsRejectsBadInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kv := memoryTestBuckets(t, ctx, savingsBucket)[0]
	now := time.Now()
	if err := RecordSaving(ctx, kv, "gossip", 1, 0, now); err == nil {
		t.Fatal("accepted unknown kind")
	}
	if err := RecordSaving(ctx, kv, SavingDedup, -1, 0, now); err == nil {
		t.Fatal("accepted negative tokens")
	}
	if _, err := SavingsReport(ctx, kv, 0, now); err == nil {
		t.Fatal("SavingsReport accepted 0 days")
	}
	if _, err := SavingsReport(ctx, kv, savingsMaxDays+1, now); err == nil {
		t.Fatal("SavingsReport accepted too many days")
	}
	var buf bytes.Buffer
	if err := savingsCommand(ctx, kv, []string{"91"}, &buf); err == nil {
		t.Fatal("accepted 91 days")
	}
	if err := savingsCommand(ctx, kv, []string{"2"}, &buf); err != nil {
		t.Fatal(err)
	}
	var out struct {
		Days []SavingsDay `json:"days"`
	}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatal(err, buf.String())
	}
	if len(out.Days) != 2 {
		t.Fatal("savings output:", buf.String())
	}
}

func TestSavingsConcurrentIncrementsKeepEveryEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kv := memoryTestBuckets(t, ctx, savingsBucket)[0]
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	errs := make(chan error, 20*5)
	for range 20 {
		wg.Go(func() {
			for range 5 {
				if err := RecordSaving(ctx, kv, SavingDedup, 3, 0, now); err != nil {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal("increment lost:", err)
	}

	report, err := SavingsReport(ctx, kv, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if c := report[0].Kinds[SavingDedup]; c.Events != 100 || c.Tokens != 300 {
		t.Fatalf("counter after 100 writes: %+v", c)
	}
}
