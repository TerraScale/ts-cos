package cos

import (
	"context"
	"path/filepath"
	"testing"
)

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
