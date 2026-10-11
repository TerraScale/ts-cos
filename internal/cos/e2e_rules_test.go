package cos

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReplayIDMatchesReconciler(t *testing.T) {
	p := PR{Repo: "ts-cos", Number: 7, Branch: "cos/ts-7", Cos: true, SHA: "abc", CI: "success", Mergeable: true}
	events, _ := reconcileEvents(Live{PRs: []PR{p}}, map[string]string{reconcileBaseline: "1"}, time.Now())
	if len(events) != 1 || events[0].ID != replayID(p) {
		t.Fatalf("reconcileEvents = %v, want one event with ID %s", events, replayID(p))
	}
}

func TestNextPreset(t *testing.T) {
	for _, tc := range []struct{ current, want int }{
		{10, 15},
		{60, 10}, // wraps around
		{30, 60},
		{7, 10}, // unknown starts at the first preset
	} {
		if got := nextPreset(tc.current, autopilotPresets); got != tc.want {
			t.Errorf("nextPreset(%d) = %d, want %d", tc.current, got, tc.want)
		}
	}
	if nextPreset(30, autopilotPresets) == 30 {
		t.Error("the replacement must differ from the current interval")
	}
}

func TestQuietPRs(t *testing.T) {
	seen := map[string]string{"pr:ts-cos#1": "fp"}
	for _, tc := range []struct {
		name string
		pr   PR
		seen map[string]string
		want bool
	}{
		{"pending CI", PR{Repo: "ts-cos", Number: 1, Branch: "cos/ts-1", CI: "pending", Mergeable: true}, seen, true},
		{"green CI", PR{Repo: "ts-cos", Number: 1, Branch: "cos/ts-1", CI: "success", Mergeable: true}, seen, true},
		{"red CI wakes the worker", PR{Repo: "ts-cos", Number: 1, Branch: "cos/ts-1", CI: "failure", Mergeable: true}, seen, false},
		{"conflict wakes the worker", PR{Repo: "ts-cos", Number: 1, Branch: "cos/ts-1", CI: "success"}, seen, false},
		{"ready merges", PR{Repo: "ts-cos", Number: 1, Branch: "cos/ts-1", CI: "success", Mergeable: true, Ready: true}, seen, false},
		{"changes requested", PR{Repo: "ts-cos", Number: 1, Branch: "cos/ts-1", CI: "pending", Mergeable: true, ChangesRequested: true}, seen, false},
		{"held cos-hold branch", PR{Repo: "ts-cos", Number: 1, Branch: "cos-hold/ts-1", CI: "pending", Mergeable: true}, seen, false},
		{"no fingerprint, nothing to drop", PR{Repo: "ts-cos", Number: 1, Branch: "cos/ts-1", CI: "pending", Mergeable: true}, map[string]string{}, false},
	} {
		got := quietPRs([]PR{tc.pr}, tc.seen)
		if (len(got) == 1) != tc.want {
			t.Errorf("%s: quietPRs kept %d, want quiet=%t", tc.name, len(got), tc.want)
		}
	}
}

func TestWithoutFingerprint(t *testing.T) {
	seen := map[string]string{reconcileBaseline: "1", "pr:ts-cos#1": "a", "pr:ts-cos#2": "b"}
	got := withoutFingerprint(seen, "pr:ts-cos#1")
	if _, ok := got["pr:ts-cos#1"]; ok {
		t.Error("the dropped PR's fingerprint is still present")
	}
	if got["pr:ts-cos#2"] != "b" || got[reconcileBaseline] != "1" {
		t.Errorf("other fingerprints changed: %v", got)
	}
	if _, ok := seen["pr:ts-cos#1"]; !ok {
		t.Error("the input map was modified; it must be a copy")
	}
}

func TestFindReplay(t *testing.T) {
	pr := PR{Repo: "ts-cos", Number: 1, Cos: true, SHA: "abc", CI: "success", Mergeable: true}
	id := replayID(pr)
	other := replayID(PR{Repo: "ts-cos", Number: 12, Cos: true, SHA: "abc", CI: "success", Mergeable: true})
	stale := replayID(PR{Repo: "ts-cos", Number: 1, Cos: true, SHA: "old", CI: "pending", Mergeable: true})
	feed := []AutopilotEntry{{Event: other, Action: "progress", Seq: 1}, {Event: id, Action: "progress", Seq: 2}}
	if e, ok := findReplay(feed, id, 0); !ok || e.Event != id {
		t.Errorf("findReplay(%s) = %v, %t; want the matching entry", id, e, ok)
	}
	if _, ok := findReplay(feed[:1], id, 0); ok {
		t.Error("#1 matched the #12 entry")
	}
	if _, ok := findReplay([]AutopilotEntry{{Event: stale, Seq: 1}}, id, 0); ok {
		t.Error("an older state of the same PR matched the current replay")
	}
}

func TestFindReplayIgnoresEntriesBeforeTheDrop(t *testing.T) {
	id := replayID(PR{Repo: "ts-cos", Number: 1, Cos: true, SHA: "abc", CI: "success", Mergeable: true})
	feed := []AutopilotEntry{{Event: id, Action: "progress", Seq: 5}}
	if _, ok := findReplay(feed, id, 5); ok {
		t.Error("an entry at the drop's sequence matched; only later entries are this run's replay")
	}
	if e, ok := findReplay(feed, id, 4); !ok || e.Seq != 5 {
		t.Errorf("findReplay after 4 = %v, %t; want the seq 5 entry", e, ok)
	}
}

func TestRunNowUntilRetriesTransientErrors(t *testing.T) {
	calls := 0
	run := func(context.Context) (string, error) {
		calls++
		if calls < 3 {
			return "", errors.New("no autopilot leader answered")
		}
		return "delta", nil
	}
	holder, err := runNowUntil(context.Background(), run, time.Second, time.Millisecond)
	if holder != "delta" || err != nil || calls != 3 {
		t.Errorf("runNowUntil = %q, %v after %d calls; want delta, nil after 3", holder, err, calls)
	}
}

func TestRunNowUntilStopsAtTheDeadline(t *testing.T) {
	run := func(context.Context) (string, error) { return "delta", errAutopilotBusy }
	holder, err := runNowUntil(context.Background(), run, 20*time.Millisecond, time.Millisecond)
	if !errors.Is(err, errAutopilotBusy) || holder != "delta" {
		t.Errorf("runNowUntil = %q, %v; want delta and errAutopilotBusy at the deadline", holder, err)
	}
}

func TestReplayEvidence(t *testing.T) {
	hosts := []string{"beta", "delta", "mariogk-dev"}
	const id = "forgejo:reconcile:pr:ts-cos#1:0123456789abcdef"
	replayed := AutopilotEntry{Host: "beta", Event: id, Kind: "record", Action: "progress"}
	dropped := AutopilotEntry{Host: "beta", Event: id, Kind: "record", Action: reconcileDropped}

	got, err := replayEvidence(replayed, "beta", "ts-cos#1", id, hosts)
	if err != nil || !strings.Contains(got, "dropped and replayed as "+id) {
		t.Errorf("rules replay: got %q, %v", got, err)
	}
	got, err = replayEvidence(dropped, "beta", "ts-cos#1", id, hosts)
	if err != nil || !strings.Contains(got, "re-published "+id) || !strings.Contains(got, "handled once") {
		t.Errorf("dedup drop: got %q, %v", got, err)
	}
	if _, err := replayEvidence(dropped, "delta", "ts-cos#1", id, hosts); err == nil {
		t.Error("a drop logged off the leader must fail")
	}
	if _, err := replayEvidence(replayed, "delta", "ts-cos#1", id, hosts); err == nil {
		t.Error("a replay logged off the leader must fail")
	}
}
