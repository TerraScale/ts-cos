package cos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAutopilotLeaderOnlyAndReplicated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	stores := make([]*Store, 3)
	pilots := make([]*Autopilot, 3)
	beats := make([]atomic.Int32, 3)
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
		t.Cleanup(node.Close)
		nodes[i] = node
		stores[i], err = OpenStore(filepath.Join(cfg.Home, "cos.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { stores[i].Close() })
	}
	busTestReady(t, "full mesh", func() bool {
		return nodes[0].server.NumRoutes() == 2 && nodes[1].server.NumRoutes() == 2 && nodes[2].server.NumRoutes() == 2
	})
	for i, host := range hosts {
		var err error
		pilots[i], err = StartAutopilot(ctx, host, nodes[i], stores[i], func(context.Context) []AutopilotEntry {
			beats[i].Add(1)
			return []AutopilotEntry{{Kind: "record", Action: "Reconciled live state", Reason: "test"}}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	slow := func(description string, check func() bool) {
		t.Helper()
		deadline := time.Now().Add(40 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				s, _ := stores[0].Autopilot(ctx)
				t.Fatalf("timeout waiting for %s: %+v", description, s)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	leader := -1
	slow("first scheduled beat", func() bool {
		for i := range beats {
			if beats[i].Load() > 0 {
				leader = i
			}
		}
		return leader >= 0
	})
	state := func(i int) AutopilotState {
		s, err := stores[i].Autopilot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	slow("leader and log mirrored to every node", func() bool {
		for i := range stores {
			s := state(i)
			if s.Leader != hosts[leader] || len(s.Log) < 2 || s.LastBeat == "" {
				return false
			}
		}
		return true
	})
	follower := (leader + 1) % 3

	if err := pilots[follower].SetInterval(ctx, 7); err == nil {
		t.Fatal("non-preset interval accepted")
	}
	if err := pilots[follower].SetInterval(ctx, 10); err != nil {
		t.Fatal(err)
	}
	slow("interval replicated", func() bool {
		return state(0).Interval == 10 && state(1).Interval == 10 && state(2).Interval == 10
	})

	host, err := pilots[follower].RunNow(ctx)
	if err != nil || host != hosts[leader] {
		t.Fatalf("run now from a follower: %q %v", host, err)
	}
	slow("run now beat on the leader", func() bool { return beats[leader].Load() == 2 })
	slow("run now logged on every node", func() bool {
		for i := range stores {
			found := false
			for _, e := range state(i).Log {
				found = found || (e.Kind == "beat" && e.Reason == "run now from the dashboard" && e.Host == hosts[leader])
			}
			if !found {
				return false
			}
		}
		return true
	})
	for i := range beats {
		if i != leader && beats[i].Load() != 0 {
			t.Fatalf("%s beat without the lock", hosts[i])
		}
	}
}

func TestAutopilotNeverLeadsOnDesktop(t *testing.T) {
	if ok, reason := leaderEligible("mariogk-desktop"); ok || reason == "" {
		t.Fatal("mariogk-desktop may lead")
	}
	if ok, _ := leaderEligible("beta"); !ok {
		t.Fatal("beta may not lead")
	}
	if (&Autopilot{Host: "mariogk-desktop"}).holdLock(context.Background()) {
		t.Fatal("desktop took the lock")
	}
}

func TestAutopilotWebRequiresDatastar(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := httptest.NewServer(NewWeb(store, "test-secret").Handler())
	defer server.Close()
	post := func(path string, datastar bool) *http.Response {
		req, _ := http.NewRequest("POST", server.URL+path, nil)
		req.SetBasicAuth(dashboardUser, "test-secret")
		if datastar {
			req.Header.Set("Datastar-Request", "true")
		}
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res
	}
	if res := post("/autopilot/interval/10", false); res.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-site post: %d", res.StatusCode)
	}
	if res := post("/autopilot/run", true); res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("missing autopilot: %d", res.StatusCode)
	}
	v, err := View(context.Background(), store)
	if err != nil || v.Autopilot.Interval != autopilotDefault || v.Autopilot.NextBeat() == "" {
		t.Fatalf("default state: %+v %v", v.Autopilot, err)
	}
}

func TestAutopilotNotice(t *testing.T) {
	rec := httptest.NewRecorder()
	autopilotNotice(rec, `<script>x</script>`)
	if body := rec.Body.String(); strings.Contains(body, "<script>") || !strings.Contains(body, `id="autopilot-notice"`) {
		t.Fatal(body)
	}
}
