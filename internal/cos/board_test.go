package cos

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

var boardNow = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

func fleetOf(threads ...FleetThread) Live {
	return Live{Fleet: []HostFleet{{Host: "beta", Threads: threads}}}
}

func boardRowFor(t *testing.T, rows []BoardRow, ticket string) BoardRow {
	t.Helper()
	for _, r := range rows {
		if r.Ticket == ticket {
			return r
		}
	}
	t.Fatalf("no board row for %s in %v", ticket, tickets(rows))
	return BoardRow{}
}

func tickets(rows []BoardRow) []string {
	out := []string{}
	for _, r := range rows {
		out = append(out, r.Ticket)
	}
	return out
}

func TestBuildBoardStages(t *testing.T) {
	ready := PR{Repo: "o/r", Number: 3, Branch: "cos/ts-1", Title: "TS-1 work", Base: "main", Mergeable: true, CI: "success", Ready: true}
	open := ready
	open.Ready = false
	working := FleetThread{ThreadID: "t1", Title: "TS-1 work", Status: "running"}
	mergedAt := boardNow.Add(-2 * time.Hour)
	merged := map[string]BoardEventSpan{"TS-1": {First: boardNow.Add(-48 * time.Hour), Last: mergedAt, LastKind: "pr.merged",
		Merged: []mergedPR{{Repo: "o/r", Number: 3, Time: mergedAt}}}}
	cases := []struct {
		name       string
		tasks      []Task
		live       Live
		spans      map[string]BoardEventSpan
		deployedAt time.Time
		want       string
	}{
		{"task completed", []Task{{Ticket: "ts-1", Status: "done"}}, Live{}, nil, time.Time{}, "done"},
		{"merged and deployed", nil, Live{}, merged, boardNow.Add(-time.Hour), "done"},
		{"deploy before merge does not count", nil, Live{}, merged, boardNow.Add(-3 * time.Hour), "merging"},
		{"deployed with a live thread", nil, fleetOf(working), merged, boardNow.Add(-time.Hour), "deployed"},
		{"merged, not deployed, nothing running", nil, Live{}, merged, time.Time{}, "merging"},
		{"merged, not deployed, live thread", nil, fleetOf(working), merged, time.Time{}, "working"},
		{"ready PR", nil, Live{PRs: []PR{ready}}, nil, time.Time{}, "merging"},
		{"open PR", nil, Live{PRs: []PR{open}}, nil, time.Time{}, "in review"},
		{"live thread", nil, fleetOf(working), nil, time.Time{}, "working"},
		{"task only", []Task{{Ticket: "TS-1"}}, Live{}, nil, time.Time{}, "queued"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows := BuildBoard(c.tasks, c.live, nil, c.spans, c.deployedAt, boardNow)
			if got := boardRowFor(t, rows, "TS-1").Stage; got != c.want {
				t.Fatalf("stage = %q, want %q", got, c.want)
			}
		})
	}
}

func TestBuildBoardBlockers(t *testing.T) {
	pr := PR{Repo: "o/r", Number: 4, Branch: "cos/ts-2", Title: "TS-2 work", Base: "main", Mergeable: true, CI: "success"}
	conflict := pr
	conflict.Mergeable = false
	failing := pr
	failing.CI, failing.Failing = "failure", []string{"unit", "lint"}
	changes := pr
	changes.ChangesRequested = true
	hold := pr
	hold.Hold = true
	thread := FleetThread{ThreadID: "t2", Title: "TS-2", Status: "running"}
	asking := thread
	asking.NeedsInput = true
	broken := thread
	broken.LastError = "boom"
	cases := []struct {
		name  string
		inbox []InboxItem
		live  Live
		want  string
	}{
		{"open inbox item", []InboxItem{{Ticket: "TS-2", Status: "open", Title: "pick a db"}}, Live{}, "needs you: pick a db"},
		{"answered inbox item ignored", []InboxItem{{Ticket: "TS-2", Status: "answered", Title: "x"}}, fleetOf(thread), ""},
		{"waiting thread", nil, fleetOf(asking), "thread waiting for input"},
		{"thread error", nil, fleetOf(broken), "thread error: boom"},
		{"conflict", nil, Live{PRs: []PR{conflict}}, "conflict with main"},
		{"CI failed", nil, Live{PRs: []PR{failing}}, "CI failed: unit, lint"},
		{"changes requested", nil, Live{PRs: []PR{changes}}, "changes requested"},
		{"on hold", nil, Live{PRs: []PR{hold}}, "on hold"},
		{"no blocker", nil, Live{PRs: []PR{pr}}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows := BuildBoard([]Task{{Ticket: "TS-2"}}, c.live, c.inbox, nil, time.Time{}, boardNow)
			if got := boardRowFor(t, rows, "TS-2").Blocker; got != c.want {
				t.Fatalf("blocker = %q, want %q", got, c.want)
			}
		})
	}
}

func TestBuildBoardSortsBlockersThenUrgency(t *testing.T) {
	live := fleetOf(FleetThread{ThreadID: "b", Title: "TS-2", Status: "running"}, FleetThread{ThreadID: "c", Title: "TS-3", Status: "running"})
	live.PRs = []PR{{Repo: "o/r", Number: 7, Branch: "cos/ts-4", Title: "TS-4", Base: "main", Mergeable: true, CI: "pending"}}
	tasks := []Task{{Ticket: "TS-1"}, {Ticket: "TS-5", Status: "done"}}
	inbox := []InboxItem{{Ticket: "TS-1", Status: "open", Title: "ask"}}
	spans := map[string]BoardEventSpan{"TS-2": {Last: boardNow.Add(-2 * time.Hour)}, "TS-3": {Last: boardNow.Add(-time.Hour)}}
	rows := BuildBoard(tasks, live, inbox, spans, time.Time{}, boardNow)
	want := []string{"TS-1", "TS-3", "TS-2", "TS-4", "TS-5"}
	if got := tickets(rows); !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestBuildBoardDiscoversTickets(t *testing.T) {
	live := fleetOf(FleetThread{ThreadID: "t", Title: "TS-11 work", Status: "running"})
	live.PRs = []PR{{Repo: "o/r", Number: 1, Branch: "cos/ts-12", Title: "x", Base: "main"}}
	spans := map[string]BoardEventSpan{
		"TS-13": {Last: boardNow.Add(-time.Hour)},
		"TS-14": {Last: boardNow.Add(-8 * 24 * time.Hour)},
	}
	rows := BuildBoard([]Task{{Ticket: "ts-10"}}, live, nil, spans, time.Time{}, boardNow)
	got := tickets(rows)
	slices.Sort(got)
	if want := []string{"TS-10", "TS-11", "TS-12", "TS-13"}; !slices.Equal(got, want) {
		t.Fatalf("tickets = %v, want %v", got, want)
	}
}

func TestEventTicket(t *testing.T) {
	live := fleetOf(FleetThread{ThreadID: "t7", Title: "TS-7 work", Status: "running"})
	prs := map[string]string{prKey("o/r", 5): "TS-5"}
	cases := []struct {
		name string
		e    Event
		want string
	}{
		{"explicit ticket", Event{Ticket: "ts-9"}, "TS-9"},
		{"via PR", Event{Repo: "o/r", PR: 5}, "TS-5"},
		{"via thread", Event{Host: "beta", Thread: "t7"}, "TS-7"},
		{"summary is not used", Event{Summary: "mentions TS-99"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := eventTicket(c.e, live, prs); got != c.want {
				t.Fatalf("ticket = %q, want %q", got, c.want)
			}
		})
	}
}

func boardTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBoardEventStore(t *testing.T) {
	ctx := context.Background()
	s := boardTestStore(t)
	t0 := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	turn := Event{ID: "t3:turn-1", Source: SourceT3, Kind: KindTurnCompleted, Time: t0, Severity: SeverityInfo, Summary: "first"}
	merged := Event{ID: "forgejo:merged-3", Source: SourceForgejo, Kind: KindPRMerged, Time: t0.Add(time.Hour), Severity: SeverityInfo, Summary: "merged", Repo: "o/r", PR: 3}
	deploy := Event{ID: "deploy:1", Source: "deploy", Kind: "deploy.completed", Time: t0.Add(2 * time.Hour), Severity: SeverityInfo, Summary: "ts-cos"}
	last := Event{ID: "t3:turn-2", Source: SourceT3, Kind: KindTurnCompleted, Time: t0.Add(3 * time.Hour), Severity: SeverityInfo, Summary: "third"}
	for seq, e := range map[int64]struct {
		e      Event
		ticket string
	}{1: {turn, "TS-801"}, 2: {merged, "TS-801"}, 3: {deploy, ""}, 4: {last, "TS-801"}} {
		if err := s.insertEvent(ctx, seq, e.e, e.ticket); err != nil {
			t.Fatal(err)
		}
	}
	events, err := s.TicketEvents(ctx, "ts-801", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Summary != "third" || events[1].Summary != "merged" || events[0].Ticket != "TS-801" {
		t.Fatalf("timeline = %+v, want newest two", events)
	}
	spans, deployedAt, err := s.LatestEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	span := spans["TS-801"]
	if !span.First.Equal(t0) || !span.Last.Equal(t0.Add(3*time.Hour)) || span.LastSummary != "third" {
		t.Fatalf("span = %+v", span)
	}
	if len(span.Merged) != 1 || span.Merged[0].Number != 3 || !span.Merged[0].Time.Equal(t0.Add(time.Hour)) {
		t.Fatalf("merged = %+v", span.Merged)
	}
	if !deployedAt.Equal(t0.Add(2 * time.Hour)) {
		t.Fatalf("deployedAt = %v", deployedAt)
	}

	revision, err := s.Revision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.insertEvent(ctx, 1, last, "TS-802"); err != nil {
		t.Fatalf("duplicate seq: %v", err)
	}
	if after, _ := s.Revision(ctx); after != revision {
		t.Fatalf("mirrored insert bumped revision %d -> %d", revision, after)
	}
	if events, _ := s.TicketEvents(ctx, "TS-802", 10); len(events) != 0 {
		t.Fatalf("duplicate seq stored: %+v", events)
	}

	old := turn
	old.ID, old.Time = "t3:old", t0.Add(-31*24*time.Hour)
	if err := s.insertEvent(ctx, 5, old, "TS-803"); err != nil {
		t.Fatal(err)
	}
	if err := s.pruneEvents(ctx, t0.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if events, _ := s.TicketEvents(ctx, "TS-803", 10); len(events) != 0 {
		t.Fatalf("old event survived prune: %+v", events)
	}
	if events, _ := s.TicketEvents(ctx, "TS-801", 10); len(events) != 3 {
		t.Fatalf("prune removed recent events: %d left", len(events))
	}
}

func TestEventMirrorResumesAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	nodes := make([]*Bus, 3)
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		cfg, busNode := busTestConfig(t, host, ports[i], ports[i+3], peers)
		node, err := startBus(ctx, cfg, busNode, func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()
		nodes[i] = node
	}
	js, err := nodes[0].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	busTestReady(t, "event streams", func() bool {
		ready, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		return EnsureEventStreams(ready, js) == nil
	})
	store := boardTestStore(t)
	publish := func(from, to int) {
		t.Helper()
		for i := from; i < to; i++ {
			e := eventsTestEvent()
			e.ID = fmt.Sprintf("forgejo:mirror-%d", i)
			if _, err := PublishEvent(ctx, js, e); err != nil {
				t.Fatal(err)
			}
		}
	}
	count := func() int {
		var n int
		if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	run := func() (stop func()) {
		runCtx, cancelRun := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); RunEventMirror(runCtx, nodes[0], store) }()
		return func() { cancelRun(); <-done }
	}
	waitRows := func(want int) {
		t.Helper()
		busTestReady(t, fmt.Sprintf("%d mirrored events", want), func() bool { return count() == want })
	}

	publish(0, 3)
	stop := run()
	waitRows(3)
	stop()

	publish(3, 4)
	stop = run()
	defer func() { stop() }()
	waitRows(4)
	if events, err := store.TicketEvents(ctx, "TS-814", 10); err != nil || len(events) != 4 {
		t.Fatalf("ticket timeline = %d events, err %v; want 4", len(events), err)
	}

	// A recreated stream restarts at seq 1, below the 4 mirrored rows; the mirror must start over.
	stop()
	if err := js.DeleteStream(ctx, EventsStream); err != nil {
		t.Fatal(err)
	}
	if err := EnsureEventStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	publish(4, 5)
	stop = run()
	waitRows(1)
	if events, err := store.TicketEvents(ctx, "TS-814", 10); err != nil || len(events) != 1 || events[0].ID != "forgejo:mirror-4" {
		t.Fatalf("after stream reset: events %v, err %v; want only forgejo:mirror-4", events, err)
	}
}
