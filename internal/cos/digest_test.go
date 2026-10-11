package cos

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

var digestStart = time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)

// digestAt is an event at minute min after digestStart; callers set the entity fields.
func digestAt(id string, kind EventKind, sev Severity, summary string, min int) Event {
	return Event{ID: id, Source: eventKinds[kind], Kind: kind, Severity: sev, Summary: summary, Time: digestStart.Add(time.Duration(min) * time.Minute)}
}

// digestPR puts an event on PR n of ts-cos.
func digestPR(e Event, n int) Event {
	e.Repo, e.PR = "TerraScale/ts-cos", n
	return e
}

func TestDigestIdenticalEventsFoldToOneLine(t *testing.T) {
	e := digestPR(digestAt("forgejo:ci-1", KindCIStatus, SeverityInfo, "CI running", 1), 9)
	d := BuildDigest([]Event{e, e, e}, time.Time{}, 0)
	if len(d.Lines) != 1 || d.Merged != 2 || d.Events != 3 {
		t.Fatalf("lines %d merged %d events %d, want 1 2 3", len(d.Lines), d.Merged, d.Events)
	}
}

func TestDigestCoalescesStateChangesOnOnePR(t *testing.T) {
	pending := digestPR(digestAt("forgejo:ci-1", KindCIStatus, SeverityInfo, "CI pending on test", 1), 9)
	success := digestPR(digestAt("forgejo:ci-2", KindCIStatus, SeverityInfo, "CI passed on test", 2), 9)
	d := BuildDigest([]Event{success, pending}, time.Time{}, 0)
	want := "[info] ci.status TerraScale/ts-cos#9: CI passed on test (x2)"
	if len(d.Lines) != 1 || d.Text != want {
		t.Fatalf("text %q, want %q", d.Text, want)
	}
}

func TestDigestMergedPRDropsItsCIAndSyncLines(t *testing.T) {
	events := []Event{
		digestPR(digestAt("forgejo:ci-1", KindCIStatus, SeverityInfo, "CI passed on test", 1), 9),
		digestPR(digestAt("forgejo:sync-1", KindPRSynchronized, SeverityInfo, "PR updated", 2), 9),
		digestPR(digestAt("forgejo:merged-9", KindPRMerged, SeverityInfo, "PR merged", 3), 9),
		digestPR(digestAt("forgejo:ci-10", KindCIStatus, SeverityInfo, "CI passed on test", 4), 10),
	}
	d := BuildDigest(events, time.Time{}, 0)
	if len(d.Lines) != 2 || d.Merged != 2 {
		t.Fatalf("lines %d merged %d, want 2 2", len(d.Lines), d.Merged)
	}
}

func TestDigestFoldsSimilarSummaries(t *testing.T) {
	var events []Event
	for i := 1; i <= 5; i++ {
		summary := "CI failed on run 123"
		if i%2 == 0 {
			summary = "CI failed on run 456"
		}
		events = append(events, digestPR(digestAt(fmt.Sprintf("forgejo:ci-%d", i), KindCIStatus, SeverityWarn, summary, i), i))
	}
	d := BuildDigest(events, time.Time{}, 0)
	want := "[warn] ci.status TerraScale/ts-cos#1, TerraScale/ts-cos#2, TerraScale/ts-cos#3 +2: CI failed on run 123 (x5)"
	if len(d.Lines) != 1 || d.Text != want {
		t.Fatalf("text %q, want %q", d.Text, want)
	}
}

func TestDigestOrdersByPriority(t *testing.T) {
	done := digestAt("t3:done", KindTurnCompleted, SeverityInfo, "turn finished", 4)
	done.Host, done.Thread = "mariogk-dev", "t1"
	review := digestPR(digestAt("forgejo:review", KindReview, SeverityInfo, "review submitted", 3), 9)
	failed := digestAt("t3:failed", KindTurnFailed, SeverityWarn, "turn failed", 2)
	failed.Host, failed.Thread = "delta", "t2"
	needs := digestAt("t3:needs", KindNeedsUser, SeverityUrgent, "worker needs a decision", 1)
	needs.Host, needs.Thread = "beta", "t3"
	d := BuildDigest([]Event{done, review, failed, needs}, time.Time{}, 0)
	var kinds []EventKind
	for _, l := range d.Lines {
		kinds = append(kinds, l.Kind)
	}
	want := []EventKind{KindNeedsUser, KindTurnFailed, KindReview, KindTurnCompleted}
	if !slices.Equal(kinds, want) {
		t.Fatalf("kinds %v, want %v", kinds, want)
	}
}

func TestDigestSinceExcludesOldEvents(t *testing.T) {
	since := digestStart.Add(5 * time.Minute)
	events := []Event{
		digestPR(digestAt("forgejo:old", KindCIStatus, SeverityInfo, "CI passed on test", 4), 1),
		digestPR(digestAt("forgejo:edge", KindCIStatus, SeverityInfo, "CI passed on test", 5), 2),
		digestPR(digestAt("forgejo:new", KindCIStatus, SeverityInfo, "CI passed on test", 6), 3),
	}
	d := BuildDigest(events, since, 0)
	if d.Events != 1 || len(d.Lines) != 1 || d.Lines[0].Subject != "TerraScale/ts-cos#3" {
		t.Fatalf("events %d lines %v, want 1 event on PR 3", d.Events, d.Lines)
	}
}

func TestDigestCapsBytesAndCountsDropped(t *testing.T) {
	const maxBytes = 1024
	var events []Event
	for i := 0; i < 500; i++ {
		e := digestAt(fmt.Sprintf("forgejo:ci-%d", i), KindCIStatus, SeverityInfo, "CI failed on a long run "+strings.Repeat("x", 200), i+1)
		e.Repo, e.PR = fmt.Sprintf("TerraScale/repo-%d", i), i+1
		events = append(events, e)
	}
	d := BuildDigest(events, time.Time{}, maxBytes)
	kept := 0
	for _, l := range d.Lines {
		kept += l.Count
	}
	footer := fmt.Sprintf("\n+%d more events omitted (cos events)", d.Dropped)
	switch {
	case d.Bytes > maxBytes || d.Bytes != len(d.Text):
		t.Fatalf("bytes %d, text length %d, cap %d", d.Bytes, len(d.Text), maxBytes)
	case d.Dropped == 0 || !strings.HasSuffix(d.Text, footer):
		t.Fatalf("dropped %d, want footer %q at end of %q", d.Dropped, footer, d.Text)
	case d.Dropped+kept != d.Events-d.Merged:
		t.Fatalf("dropped %d + kept %d != events %d - merged %d", d.Dropped, kept, d.Events, d.Merged)
	}
}

func TestDigestCapSmallerThanFooter(t *testing.T) {
	d := BuildDigest([]Event{digestAt("forgejo:a", KindPRMerged, SeverityInfo, "merged", 1)}, time.Time{}, 20)
	if d.Bytes > 20 || d.Dropped != 1 || len(d.Lines) != 0 {
		t.Fatalf("bytes %d dropped %d lines %d: %q", d.Bytes, d.Dropped, len(d.Lines), d.Text)
	}
}

func TestDigestEmptyInput(t *testing.T) {
	d := BuildDigest(nil, time.Time{}, 0)
	if d.Text != "" || d.Bytes != 0 || d.Events != 0 || d.Merged != 0 || d.Dropped != 0 || len(d.Lines) != 0 {
		t.Fatalf("empty digest %+v, want zero values", d)
	}
}

func TestDigestSubject(t *testing.T) {
	tests := []struct {
		name string
		e    Event
		want string
	}{
		{"pr", Event{Repo: "TerraScale/ts-cos", PR: 9, Ticket: "TS-828"}, "TerraScale/ts-cos#9"},
		{"ticket", Event{Ticket: "TS-828", Host: "delta"}, "TS-828"},
		{"host and thread", Event{Host: "delta", Thread: "t1"}, "delta/t1"},
		{"item", Event{Item: "inbox-7"}, "inbox-7"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := digestSubject(tt.e); got != tt.want {
				t.Fatalf("subject %q, want %q", got, tt.want)
			}
		})
	}
}
