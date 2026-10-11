package cos

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestPartialThreadAskFailureKeepsOnlyItsCachedQuestions(t *testing.T) {
	old := []Ask{
		{Host: "delta", ThreadID: "failed", Question: "Cached question"},
		{Host: "delta", ThreadID: "healthy", Question: "Outdated question"},
		{Host: "beta", ThreadID: "failed", Question: "Other host question"},
	}
	next := []Ask{
		{Host: "delta", ThreadID: "healthy", Question: "New healthy question"},
		{Host: "delta", ThreadID: "failed", Title: "TS-2", Error: "thread projection failed"},
	}
	merged := mergeAsks(old, next)
	if len(merged) != 3 || merged[0].Stale || merged[0].Question != "New healthy question" || merged[2].ThreadID != "failed" || !merged[2].Stale || merged[2].Question != "Cached question" {
		t.Fatal(merged)
	}
	brief := Brief(Dashboard{Live: Live{Asks: merged}})
	if !strings.Contains(brief, "ERROR asks delta failed: thread projection failed") || strings.Contains(brief, "Outdated question") || strings.Contains(brief, "Other host question") {
		t.Fatal(brief)
	}
	if recovered := mergeAsks(merged, []Ask{{Host: "delta", ThreadID: "failed", Question: "Fresh question"}}); len(recovered) != 1 || recovered[0].Stale {
		t.Fatal("recovery retained cached duplicate", recovered)
	}
}

func TestTaskThreadSelectionPrefersFreshHosts(t *testing.T) {
	fresh := HostFleet{Host: "beta", Threads: []FleetThread{{Title: "TS-769", Model: "gpt-6-sol", Status: "idle", Project: "current-repo", NeedsInput: true}}}
	stale := HostFleet{Host: "delta", Stale: true, Error: "offline", Threads: []FleetThread{{Title: "TS-769 old", Model: "old-model", Status: "running", Project: "old-repo", LastError: "old failure"}}}
	for _, tc := range []struct {
		name  string
		hosts []HostFleet
	}{
		{"fresh idle before stale active", []HostFleet{fresh, stale}},
		{"stale active before fresh idle", []HostFleet{stale, fresh}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ctx := context.Background()
			if err := store.SaveLive(ctx, Live{Fleet: tc.hosts}); err != nil {
				t.Fatal(err)
			}
			view, err := View(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			if len(view.Tasks) != 1 {
				t.Fatal(view.Tasks)
			}
			task := view.Tasks[0]
			if task.Host != "beta" || task.Stale || task.Model != "gpt-6-sol" || task.Status != "idle" || task.Repo != "current-repo" || task.Priority != "P2" || task.Next != "Answer the pending request" {
				t.Fatalf("stale active worker displaced fresh idle worker: %+v", task)
			}
		})
	}
}

func TestViewTasksComeFromOpenThreadsAndTSPRs(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	live := Live{
		Fleet: []HostFleet{{Host: "beta", Threads: []FleetThread{{Title: "TS-812 · ts-cos: parity", Branch: "cos/ts-812-parity", Project: "ts-cos", Status: "running", Model: "claude-opus-5-5"}}}},
		PRs: []PR{
			{Repo: "ts-cos", Number: 12, URL: "https://example.test/pulls/12", Title: "TS-9 and TS-812 parity", Issues: []string{"TS-812", "TS-9"}, CI: "success", Mergeable: true, Ready: true},
			{Repo: "ops", Number: 13, URL: "https://example.test/pulls/13", Title: "Fix CI image", Issues: []string{"OPS-4"}, CI: "failure", Mergeable: true, Failing: []string{"build"}},
		},
	}
	if err := store.SaveLive(ctx, live); err != nil {
		t.Fatal(err)
	}
	view, err := View(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Tasks) != 2 {
		t.Fatal(view.Tasks)
	}
	ticket, pr := view.Tasks[0], view.Tasks[1]
	if ticket.Ticket != "TS-812" || ticket.Outcome != "TS-812 · ts-cos: parity" || ticket.Priority != "P2" || ticket.Status != "ready" || ticket.Host != "beta" || ticket.Model != "claude-opus-5-5" || ticket.PR != "https://example.test/pulls/12" || ticket.Next != "Merge the ready PR" {
		t.Fatalf("thread task = %+v", ticket)
	}
	if pr.Ticket != "TS-9" || pr.Outcome != "TS-9 and TS-812 parity" || pr.Priority != "P2" || pr.Status != "ready" || pr.Repo != "ts-cos" || pr.PR != "https://example.test/pulls/12" || pr.Next != "Merge the ready PR" {
		t.Fatalf("PR task = %+v", pr)
	}
}
