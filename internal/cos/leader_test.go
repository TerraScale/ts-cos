package cos

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBusLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	configs := make([]Config, 3)
	nodes := make([]busNode, 3)
	buses := make([]*Bus, 3)
	var mu sync.Mutex
	running := map[string]int{}
	start := func(i int) {
		bus, err := startBus(ctx, configs[i], nodes[i], func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		buses[i] = bus
		t.Cleanup(bus.Close)
		go bus.Lead(ctx, func(work context.Context) {
			mu.Lock()
			running[hosts[i]]++
			mu.Unlock()
			<-work.Done()
			mu.Lock()
			running[hosts[i]]--
			mu.Unlock()
		})
	}
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		configs[i], nodes[i] = busTestConfig(t, host, ports[i], ports[i+3], peers)
		start(i)
	}
	// leader waits until exactly one work function runs, on a host other than
	// not, and every live node agrees on that host.
	leader := func(within time.Duration, not int, live ...int) int {
		t.Helper()
		deadline := time.Now().Add(within)
		for time.Now().Before(deadline) {
			mu.Lock()
			active, total := "", 0
			for host, n := range running {
				total += n
				if n == 1 {
					active = host
				}
			}
			mu.Unlock()
			agreed := total == 1 && (not < 0 || active != hosts[not])
			for _, i := range live {
				host, err := buses[i].Leader(ctx)
				agreed = agreed && err == nil && host == active
			}
			if agreed {
				for i, host := range hosts {
					if host == active {
						return i
					}
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("no single agreed leader within %s: %v", within, running)
		return -1
	}
	others := func(i int) []int {
		var rest []int
		for j := range hosts {
			if j != i {
				rest = append(rest, j)
			}
		}
		return rest
	}

	first := leader(20*time.Second, -1, 0, 1, 2)

	// A crashed leader cannot delete its lease: the survivors take over when it expires.
	crashed := time.Now()
	buses[first].server.Shutdown()
	second := leader(30*time.Second, first, others(first)...)
	if time.Since(crashed) > 30*time.Second {
		t.Fatalf("handover from %s to %s took %s", hosts[first], hosts[second], time.Since(crashed))
	}
	buses[first].Close()

	// A restarted node rejoins from its JetStream store without becoming a second leader.
	start(first)
	if got := leader(10*time.Second, -1, 0, 1, 2); got != second {
		t.Fatalf("restart moved leadership from %s to %s", hosts[second], hosts[got])
	}

	// A stopping leader releases its lease at once.
	stopped := time.Now()
	buses[second].Close()
	third := leader(leaseTTL, second, others(second)...)
	// The survivors take the deleted lease at their next refresh, once the lease stream has a leader again (4–9 s re-election on a slow runner).
	if time.Since(stopped) > leaseTTL {
		t.Fatalf("graceful handover took %s", time.Since(stopped))
	}

	// A resigned leader hands over at once and never campaigns again.
	var survivor int
	for _, i := range others(second) {
		if i != third {
			survivor = i
		}
	}
	resigned := time.Now()
	buses[third].Resign()
	buses[third].Resign()
	leader(leaseTTL, third, survivor, third)
	if time.Since(resigned) > leaseTTL {
		t.Fatalf("resign handover took %s", time.Since(resigned))
	}
	buses[survivor].Resign()
	time.Sleep(leaseTTL / 2)
	mu.Lock()
	defer mu.Unlock()
	if running[hosts[third]] != 0 || running[hosts[survivor]] != 0 {
		t.Fatalf("a resigned node led again: %v", running)
	}
	// Every node resigned and the lease is deleted, so Leader reports no leader.
	if host, err := buses[third].Leader(ctx); err == nil || err.Error() != "COS cluster has no leader right now" {
		t.Fatalf("Leader after resign = %q, %v; want no leader right now", host, err)
	}
}
