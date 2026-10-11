package cos

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var reconcileNow = time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)

func reconcilePR(ci string, mergeable bool, sha string) PR {
	return PR{Repo: "ts-cos", Number: 9, SHA: sha, CI: ci, Mergeable: mergeable, Cos: true, Ready: false}
}

func reconcileLive(prs []PR) Live {
	return Live{PRs: prs}
}

// seedReconcile is the first beat: it records fingerprints and emits nothing.
func seedReconcile(t *testing.T, live Live) map[string]string {
	t.Helper()
	events, next := reconcileEvents(live, nil, reconcileNow)
	if len(events) != 0 {
		t.Fatalf("first beat emitted %d events, want none", len(events))
	}
	return next
}

func assertValid(t *testing.T, events []Event) {
	t.Helper()
	for _, e := range events {
		if err := e.Validate(); err != nil {
			t.Fatalf("event %s fails Validate: %v", e.ID, err)
		}
	}
}

func TestReconcileFirstBeatRecordsWithoutEmitting(t *testing.T) {
	live := reconcileLive([]PR{reconcilePR("failure", false, "abc")})
	events, next := reconcileEvents(live, nil, reconcileNow)
	if len(events) != 0 {
		t.Fatalf("first beat with a failing backlog emitted %d events, want none", len(events))
	}
	if len(next) != 2 || next["pr:ts-cos#9"] == "" || next[reconcileBaseline] == "" {
		t.Fatalf("first beat next = %v, want baseline and PR fingerprints", next)
	}
}

func TestReconcileKeepsOwnerOutsideTerraScale(t *testing.T) {
	seen := seedReconcile(t, reconcileLive([]PR{{Repo: "Tools/ku", Number: 4, SHA: "abc", CI: "pending", Mergeable: true, Cos: true}}))
	live := reconcileLive([]PR{{Repo: "Tools/ku", Number: 4, SHA: "abc", CI: "failure", Mergeable: true, Cos: true}})
	events, _ := reconcileEvents(live, seen, reconcileNow)
	if len(events) != 1 || events[0].Repo != "Tools/ku" || events[0].PR != 4 {
		t.Fatalf("events = %+v, want one event for Tools/ku#4", events)
	}
	assertValid(t, events)
}

func TestReconcileCIPendingToFailure(t *testing.T) {
	seen := seedReconcile(t, reconcileLive([]PR{reconcilePR("pending", true, "abc")}))
	live := reconcileLive([]PR{reconcilePR("failure", true, "abc")})
	events, next := reconcileEvents(live, seen, reconcileNow)
	if len(events) != 1 {
		t.Fatalf("got %d events, want one ci.status", len(events))
	}
	e := events[0]
	if e.Kind != KindCIStatus || e.Source != SourceForgejo || !e.Synthetic {
		t.Fatalf("event = %+v, want synthetic forgejo ci.status", e)
	}
	if e.Repo != "TerraScale/ts-cos" || e.PR != 9 {
		t.Fatalf("event repo/PR = %q/%d, want TerraScale/ts-cos/9", e.Repo, e.PR)
	}
	if e.Severity != SeverityWarn {
		t.Fatalf("severity = %q, want warn for a failure", e.Severity)
	}
	var data struct{ State string }
	if err := json.Unmarshal(e.Data, &data); err != nil || data.State != "failure" {
		t.Fatalf("data = %s, want state failure", e.Data)
	}
	if next["pr:ts-cos#9"] == seen["pr:ts-cos#9"] {
		t.Fatal("next keeps the old fingerprint after a change")
	}
	assertValid(t, events)
}

func TestReconcileSameStateEmitsNothing(t *testing.T) {
	live := reconcileLive([]PR{reconcilePR("failure", true, "abc")})
	seen := seedReconcile(t, live)
	if events, _ := reconcileEvents(live, seen, reconcileNow); len(events) != 0 {
		t.Fatalf("unchanged state emitted %d events, want none", len(events))
	}
	if events, _ := reconcileEvents(live, seen, reconcileNow.Add(time.Minute)); len(events) != 0 {
		t.Fatalf("unchanged state a minute later emitted %d events, want none", len(events))
	}
}

func TestReconcileNewSHASameFailureGetsNewID(t *testing.T) {
	seen := seedReconcile(t, reconcileLive([]PR{reconcilePR("pending", true, "abc")}))
	failed, next := reconcileEvents(reconcileLive([]PR{reconcilePR("failure", true, "abc")}), seen, reconcileNow)
	pushed, _ := reconcileEvents(reconcileLive([]PR{reconcilePR("failure", true, "def")}), next, reconcileNow)
	if len(failed) != 1 || len(pushed) != 1 {
		t.Fatalf("got %d and %d events, want one for the failure and one for the push", len(failed), len(pushed))
	}
	if failed[0].ID == pushed[0].ID {
		t.Fatalf("a new head SHA reused the event ID %q", pushed[0].ID)
	}
	assertValid(t, pushed)
}

func TestReconcileMergeableToConflict(t *testing.T) {
	seen := seedReconcile(t, reconcileLive([]PR{reconcilePR("success", true, "abc")}))
	events, _ := reconcileEvents(reconcileLive([]PR{reconcilePR("success", false, "abc")}), seen, reconcileNow)
	if len(events) != 1 || events[0].Kind != KindPRConflict || events[0].Severity != SeverityWarn {
		t.Fatalf("events = %+v, want one warn pr.conflict", events)
	}
	assertValid(t, events)
}

func TestReconcileNonCosPRsIgnored(t *testing.T) {
	seen := seedReconcile(t, reconcileLive(nil))
	human := reconcilePR("failure", false, "abc")
	human.Cos = false
	if events, _ := reconcileEvents(reconcileLive([]PR{human}), seen, reconcileNow); len(events) != 0 {
		t.Fatalf("a PR not authored by COS emitted %d events, want none", len(events))
	}
}

func TestReconcileIDsAreDeterministic(t *testing.T) {
	seen := seedReconcile(t, reconcileLive([]PR{reconcilePR("pending", true, "abc")}))
	live := reconcileLive([]PR{reconcilePR("failure", true, "abc")})
	a, _ := reconcileEvents(live, seen, reconcileNow)
	b, _ := reconcileEvents(live, seen, reconcileNow.Add(time.Hour))
	if len(a) != 1 || len(b) != 1 || a[0].ID != b[0].ID {
		t.Fatalf("same change gave different IDs: %v vs %v", a, b)
	}
	if !strings.HasPrefix(a[0].ID, "forgejo:") {
		t.Fatalf("ID %q does not start with its source", a[0].ID)
	}
}
