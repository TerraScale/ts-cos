package cos

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// nodeFixture is one realistic sample: four cores, two mounts, two sensors and a healthy T3.
func nodeFixture(host string, at time.Time, load float64) NodeSample {
	return NodeSample{
		Host: host, Time: at, N: 1,
		Build:  NodeBuild{Revision: "0123456789abcdef0123", Time: "2026-10-10T09:00:00Z"},
		Uptime: 3*86400 + 4*3600,
		CPU:    42.5, Cores: []float64{10, 20, 30, 40},
		Load: [3]float64{load, load * 0.9, load * 0.8},
		Mem:  NodeMem{Total: 16 << 30, Available: 8 << 30, SwapTotal: 4 << 30, SwapFree: 3 << 30},
		Disks: []NodeDisk{
			{Mount: "/", Total: 100 << 30, Free: 9 << 30, ReadBps: 1024, WriteBps: 2048},
			{Mount: "/home", Total: 500 << 30, Free: 45 << 30, ReadBps: 512, WriteBps: 256},
		},
		Nets:  []NodeNet{{Iface: "eth0", RxBps: 1 << 20, TxBps: 1 << 19}},
		Temps: []NodeTemp{{Name: "cpu", Celsius: 61.5}, {Name: "nvme", Celsius: 48}},
		Proc:  NodeProc{RSS: 64 << 20, HeapAlloc: 32 << 20, Goroutines: 120},
		T3:    NodeT3{OK: true, Threads: 7, Running: 2},
	}
}

func TestNodeStateThresholds(t *testing.T) {
	for age, want := range map[time.Duration]string{
		0:                "up",
		25 * time.Second: "up",
		45 * time.Second: "degraded",
		60 * time.Second: "degraded",
		75 * time.Second: "down",
	} {
		if got := nodeState(age); got != want {
			t.Errorf("nodeState(%v) = %q, want %q", age, got, want)
		}
	}
	cards, err := NodeCards(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cards) != len(expectedHosts) {
		t.Fatalf("nil bus cards = %d, want %d", len(cards), len(expectedHosts))
	}
	for _, c := range cards {
		if c.State != "unknown" || c.known() {
			t.Errorf("%s state = %q, want unknown", c.Host, c.State)
		}
	}
}

func TestNodeMetaShowsBuildTime(t *testing.T) {
	card := NodeCard{Host: "delta", State: "up", Sample: nodeFixture("delta", time.Now(), 1)}
	if got := nodeMeta(card); got != "build 0123456789ab · Oct 10 09:00 UTC · up 3d 4h" {
		t.Fatalf("nodeMeta = %q", got)
	}
}

// nodeFixtureCards is the overview fixture: delta up, mariogk-dev down 75 s after its last sample.
func nodeFixtureCards(now time.Time) []NodeCard {
	up := nodeFixture("delta", now.Add(-5*time.Second), 1.2)
	down := nodeFixture("mariogk-dev", now.Add(-75*time.Second), 0.4)
	down.T3 = NodeT3{OK: false, Error: "connection refused"}
	return []NodeCard{
		{Host: "delta", State: "up", Sample: up, Age: 5 * time.Second, CPU: []float64{10, 90, 42}, Mem: []float64{50, 60}, Net: []float64{1, 2}},
		{Host: "mariogk-dev", State: "down", Sample: down, Age: 75 * time.Second},
	}
}

// assertCleanMarkup bans inline styles, SVG text and non-finite numbers; "Infinity" in Datastar's init is fine.
func assertCleanMarkup(t *testing.T, html string) {
	t.Helper()
	for _, banned := range []string{"<style", " style=", "<text"} {
		if strings.Contains(html, banned) {
			t.Errorf("markup contains %q", banned)
		}
	}
	if nonFinite.MatchString(html) {
		t.Error("markup contains NaN or Inf")
	}
}

var nonFinite = regexp.MustCompile(`NaN|\bInf\b`)

func TestNodesPageRender(t *testing.T) {
	var buf bytes.Buffer
	if err := nodesPage(nodeFixtureCards(time.Now())).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	html := buf.String()
	for _, want := range []string{`id="delta"`, `data-state="down"`, `id="nodes-grid"`, `aria-current="page"`} {
		if !strings.Contains(html, want) {
			t.Errorf("page missing %s", want)
		}
	}
	assertCleanMarkup(t, html)
}

func TestNodeRoutesWithoutBus(t *testing.T) {
	w := &Web{}
	mux := w.routes()
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec
	}
	if rec := get("/nodes"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `data-state="unknown"`) {
		t.Fatalf("GET /nodes = %d", rec.Code)
	}
	// The first patch lands, then the stream ends when the client goes away.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	rec := httptest.NewRecorder()
	w.nodesEvents(rec, httptest.NewRequest("GET", "/nodes/events", nil).WithContext(ctx))
	if !strings.Contains(rec.Body.String(), "event: datastar-patch-elements") || !strings.Contains(rec.Body.String(), `id="nodes-grid"`) {
		t.Fatalf("events body = %q", rec.Body.String())
	}
}

func TestNodeCardsServesFreshCacheWithoutBus(t *testing.T) {
	want := nodeFixtureCards(time.Now())
	nodeCardCache.Lock()
	nodeCardCache.at, nodeCardCache.cards = time.Now(), want
	nodeCardCache.Unlock()
	t.Cleanup(func() {
		nodeCardCache.Lock()
		nodeCardCache.at, nodeCardCache.cards = time.Time{}, nil
		nodeCardCache.Unlock()
	})
	// An empty Bus has no connection, so a fresh cache must answer without touching it.
	got, err := NodeCards(context.Background(), &Bus{})
	if err != nil || len(got) != len(want) || got[0].Host != "delta" {
		t.Fatalf("NodeCards = %v, %v", got, err)
	}
}

func TestReadHostCardsRunsHostsTogether(t *testing.T) {
	hosts := []string{"delta", "beta", "mariogk-dev"}
	var started sync.WaitGroup
	started.Add(len(hosts))
	done := make(chan []NodeCard, 1)
	go func() {
		done <- readHostCards(hosts, func(host string) NodeCard {
			started.Done()
			started.Wait() // returns only once every host is in flight
			return NodeCard{Host: host}
		})
	}()
	select {
	case cards := <-done:
		for i, c := range cards {
			if c.Host != hosts[i] {
				t.Fatalf("card %d = %s, want %s", i, c.Host, hosts[i])
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hosts were read one at a time")
	}
}
