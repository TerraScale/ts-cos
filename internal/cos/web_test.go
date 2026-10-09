package cos

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSSEStoryFromOtherProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "cos.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	web := NewWeb(store, "test-secret")
	if err := web.Watch(ctx); err != nil {
		t.Fatal(err)
	}
	defer web.Close()
	server := httptest.NewServer(web.Handler())
	defer server.Close()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/events", nil)
	req.SetBasicAuth(dashboardUser, "test-secret")
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal(response.Header)
	}
	reader := bufio.NewReader(response.Body)
	readEvent := func() string {
		t.Helper()
		var event strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			event.WriteString(line)
			if line == "\n" {
				return event.String()
			}
		}
	}
	initial := readEvent()
	if !strings.Contains(initial, "event: datastar-patch-elements\n") || !strings.Contains(initial, "selector #dashboard") || !strings.Contains(initial, "Nothing open") {
		t.Fatal(initial)
	}
	other, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.AddNote(ctx, "direction", "Ship <script>alert('x')</script> safely"); err != nil {
		t.Fatal(err)
	}
	updated := readEvent()
	if !strings.Contains(updated, "Ship &lt;script&gt;") || strings.Contains(updated, "<script>alert") {
		t.Fatal(updated)
	}
}

func TestDashboardReadOnly(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := NewWeb(store, "test-secret").Handler()
	for _, path := range []string{"/", "/status", "/events"} {
		r := httptest.NewRecorder()
		req := httptest.NewRequest("POST", path, strings.NewReader("{}"))
		req.RemoteAddr = "127.0.0.1:1234"
		req.SetBasicAuth(dashboardUser, "test-secret")
		h.ServeHTTP(r, req)
		if r.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s = %d", path, r.Code)
		}
	}
	r := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.SetBasicAuth(dashboardUser, "test-secret")
	h.ServeHTTP(r, req)
	body := r.Body.String()
	if r.Code != 200 || !strings.Contains(body, "pico.classless") || !strings.Contains(body, "data-init=\"@get('/events')\"") {
		t.Fatal(r.Code, body)
	}
	for _, heading := range []string{"Direction", "Needs you", "Decided for you", "Priorities", "What we're missing", "Suggestions", "Open PRs", "Threads by host"} {
		if !strings.Contains(body, heading) {
			t.Error("missing section", heading)
		}
	}
}

func TestDerivedTasksAndBrief(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.SetTask(ctx, map[string]any{"ticket": "TS-769", "why": "Remove hand-edited status drift", "modelWhy": "Go implementation", "next": "Review this slice"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetTask(ctx, map[string]any{"ticket": "TS-770", "status": "fake"}); err == nil {
		t.Fatal("accepted manually maintained status")
	}
	live := Live{At: "2026-10-09T12:00:00Z", Errors: map[string]string{}, Issues: []Issue{{Identifier: "TS-768", Title: "Normal urgent work", Priority: 1, Status: "In Progress"}, {Identifier: "TS-769", Title: "FULL SEND · COS app", Priority: 2, Status: "Todo"}, {Identifier: "TS-770", Title: "Finished work", StatusType: "completed"}}, Fleet: []HostFleet{{Host: "delta", Active: 1, Threads: []FleetThread{{Title: "TS-769 · ts-cos", Project: "ts-cos", Status: "running", Model: "gpt-6.1-sol"}}}}, PRs: []PR{{Repo: "ts-cos", Number: 1, URL: "https://git.terrascale.tech/TerraScale/ts-cos/pulls/1", Issues: []string{"TS-769"}, Ready: true, CI: "success", Mergeable: true}}}
	if err := store.SaveLive(ctx, live); err != nil {
		t.Fatal(err)
	}
	view, err := View(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Tasks) != 2 || view.Tasks[0].Ticket != "TS-769" {
		t.Fatal(view.Tasks)
	}
	task := view.Tasks[0]
	if task.Status != "ready" || task.Host != "delta" || task.Model != "gpt-6.1-sol" || task.Priority != "FULL SEND" || task.Next != "Review this slice" || task.PR == "" || task.Why == "" {
		t.Fatal(task)
	}
	for i := range 100 {
		view.Live.Asks = append(view.Live.Asks, Ask{Question: fmt.Sprintf("Question %d\nSecond line", i), Host: "delta", Title: "TS-769"})
	}
	brief := Brief(view)
	if lines := len(strings.Split(strings.TrimSpace(brief), "\n")); lines > 40 {
		t.Fatal("too many lines", lines)
	}
	if !strings.Contains(brief, "more items") {
		t.Fatal("no omission count")
	}
}

func TestFullSendMarker(t *testing.T) {
	for title, want := range map[string]bool{"FULL SEND · TS-769": true, " full send TS-769": true, "Build FULL SEND flow": false} {
		if got := fullSendTitle(title); got != want {
			t.Errorf("%q: %t", title, got)
		}
	}
}

func TestPartialHostFailuresKeepCachedWork(t *testing.T) {
	oldFleet := []HostFleet{{Host: "delta", Active: 1, Threads: []FleetThread{{Title: "TS-769", Status: "running"}}}, {Host: "beta", Active: 2, Threads: []FleetThread{{Title: "TS-768", Status: "running"}}}}
	next := mergeFleet(oldFleet, []HostFleet{{Host: "delta", Error: "offline"}, {Host: "beta", Active: 0, Threads: []FleetThread{}}})
	if len(next[0].Threads) != 1 || next[0].Active != 1 || !next[0].Stale || len(next[1].Threads) != 0 || next[1].Stale {
		t.Fatal(next)
	}
	oldAsks := []Ask{{Host: "delta", Question: "Choose a window", Kind: "marker"}, {Host: "beta", Question: "Already answered", Kind: "marker"}}
	asks := mergeAsks(oldAsks, []Ask{{Host: "delta", Error: "projection failed"}})
	if len(asks) != 2 || !asks[1].Stale || asks[1].Question != "Choose a window" {
		t.Fatal(asks)
	}
	brief := Brief(Dashboard{Live: Live{Fleet: next, Asks: asks}})
	if !strings.Contains(brief, "ERROR asks delta: projection failed") || !strings.Contains(brief, "STALE NEEDS-USER") {
		t.Fatal(brief)
	}
	if got := mergeAsks(asks, []Ask{}); len(got) != 0 {
		t.Fatal("successful empty refresh kept resolved asks", got)
	}
	fresh := mergeFleet(next, []HostFleet{{Host: "delta", Threads: []FleetThread{}}})
	if fresh[0].Stale || len(fresh[0].Threads) != 0 {
		t.Fatal(fresh)
	}
}
