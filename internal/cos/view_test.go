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
	stale := HostFleet{Host: "delta", Stale: true, Error: "offline", Threads: []FleetThread{{Title: "FULL SEND TS-769", Model: "old-model", Status: "running", Project: "old-repo", LastError: "old failure"}}}
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
			if err := store.SaveLive(ctx, Live{Issues: []Issue{{Identifier: "TS-769", Title: "Build COS", Priority: 1}}, Fleet: tc.hosts}); err != nil {
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
			if task.Host != "beta" || task.Stale || task.Model != "gpt-6-sol" || task.Status != "idle" || task.Repo != "current-repo" || task.Priority != "P0" || task.Next != "Answer the pending request" {
				t.Fatalf("stale active worker displaced fresh idle worker: %+v", task)
			}
		})
	}
}
