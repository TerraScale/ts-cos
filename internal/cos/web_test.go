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

func TestChatSSEHeaderFollowsState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "cos.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	web := NewWeb(store, nil)
	if err := web.Watch(ctx); err != nil {
		t.Fatal(err)
	}
	defer web.Close()
	server := httptest.NewServer(ownerRoutes(web))
	defer server.Close()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/chat/events", nil)
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
	header := readEvent()
	if !strings.Contains(header, "event: datastar-patch-elements\n") || !strings.Contains(header, `<header id="chat-head"`) || !strings.Contains(header, "0 running") {
		t.Fatal(header)
	}
	if tiles := readEvent(); !strings.Contains(tiles, `id="kpis"`) {
		t.Fatal(tiles)
	}
	for _, badge := range []string{`id="side-inbox"`, `id="tab-inbox"`} {
		if event := readEvent(); !strings.Contains(event, badge) {
			t.Fatal(event)
		}
	}
	if nodes := readEvent(); !strings.Contains(nodes, `id="pane-nodes"`) {
		t.Fatal(nodes)
	}
	if work := readEvent(); !strings.Contains(work, `id="pane-work"`) {
		t.Fatal(work)
	}
	if inboxPane := readEvent(); !strings.Contains(inboxPane, `id="inbox"`) {
		t.Fatal(inboxPane)
	}
	if messages := readEvent(); !strings.Contains(messages, `<div id="messages"`) {
		t.Fatal(messages)
	}
	other, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	live := Live{At: "2026-10-10T12:00:00Z", Errors: map[string]string{}, Fleet: []HostFleet{{Host: "delta", Active: 2}}, PRs: []PR{{Repo: "ts-cos", Number: 1, URL: "https://github.com/TerraScale/ts-cos/pulls/1"}, {Repo: "ts-cos", Number: 2, URL: "https://github.com/TerraScale/ts-cos/pulls/2"}}}
	if err := other.SaveLive(ctx, live); err != nil {
		t.Fatal(err)
	}
	updated := readEvent()
	if !strings.Contains(updated, "2 running") || !strings.Contains(updated, "2 PRs waiting") {
		t.Fatal(updated)
	}
}

func TestChatRoutes(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := ownerRoutes(NewWeb(store, nil))
	serve := func(method, path string, datastar bool, body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.RemoteAddr = "127.0.0.1:1234"
		if datastar {
			req.Header.Set("Datastar-Request", "true")
		}
		h.ServeHTTP(r, req)
		return r
	}
	for _, path := range []string{"/status", "/chat/events"} {
		if r := serve("POST", path, false, "{}"); r.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s = %d", path, r.Code)
		}
	}
	r := serve("GET", "/", false, "")
	body := r.Body.String()
	for _, want := range []string{`data-init="@get('/chat/events', {retry: 'always'`, `data-init="@get(&#39;/release&#39;`, `id="messages"`, `href="/settings"`, `id="inbox-slot"`, `id="heartbeat-slot"`, "0 running", "0 PRs waiting", `id="kpis"`, cssPath} {
		if r.Code != http.StatusOK || !strings.Contains(body, want) {
			t.Fatal(r.Code, "missing", want)
		}
	}
	if n := strings.Count(body, `class="kpi"`); n != 6 {
		t.Fatalf("home renders %d KPI tiles, want 6", n)
	}
	for _, want := range []string{`id="pane-chat"`, `id="pane-inbox"`, `id="pane-nodes"`, `id="pane-work"`, `class="tabbar"`, `class="app-aside"`, `id="side-inbox"`, `id="tab-inbox"`} {
		if !strings.Contains(body, want) {
			t.Fatal("home missing", want)
		}
	}
	// No bus: every expected host is listed as unknown, so its figures show dashes.
	if !strings.Contains(body, `<a class="btn" href="/nodes">Open nodes</a>`) || !strings.Contains(body, `href="/nodes#`+expectedHosts[0]+`"`) || !strings.Contains(body, "CPU –") {
		t.Fatal("home missing the Nodes summary")
	}
	inboxPage := serve("GET", "/inbox", false, "")
	for _, want := range []string{`data-init="@get('/inbox/events'`, `data-init="@get(&#39;/release&#39;`, `id="inbox"`, `href="/inbox">Inbox <span class="badge badge-accent">0</span>`, "Nothing needs you"} {
		if inboxPage.Code != http.StatusOK || !strings.Contains(inboxPage.Body.String(), want) {
			t.Fatal(inboxPage.Code, "inbox page missing", want)
		}
	}
	if settings := serve("GET", "/settings", false, ""); settings.Code != http.StatusOK || !strings.Contains(settings.Body.String(), `href="/password"`) || !strings.Contains(settings.Body.String(), "Heartbeat") {
		t.Fatal("settings page", settings.Code)
	}
	if r := serve("POST", "/chat", false, `{"msg":"hi"}`); r.Code != http.StatusForbidden {
		t.Errorf("POST /chat without Datastar header = %d", r.Code)
	}
	// Send errors answer 200 with the composer notice, so Datastar applies it.
	for name, body := range map[string]string{
		"blank":    `{"msg":"   "}`,
		"not json": `not json`,
		"too long": `{"msg":"` + strings.Repeat("x", 20001) + `"}`,
	} {
		if r := serve("POST", "/chat", true, body); r.Code != http.StatusOK || !strings.Contains(r.Body.String(), `id="chat-notice"`) {
			t.Errorf("POST /chat %s = %d", name, r.Code)
		}
	}
	// NewChat has no bus, so the store is not ready and the message cannot be saved.
	if r := serve("POST", "/chat", true, `{"msg":"hi"}`); r.Code != http.StatusOK || !strings.Contains(r.Body.String(), "Chat is unavailable") {
		t.Errorf("POST /chat without store = %d", r.Code)
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
	live := Live{At: "2026-10-09T12:00:00Z", Errors: map[string]string{}, Fleet: []HostFleet{{Host: "delta", Active: 1, Threads: []FleetThread{{Title: "FULL SEND · TS-769 · COS app", Project: "ts-cos", Status: "running", Model: "gpt-6.1-sol"}, {Title: "TS-768 · Normal urgent work", Status: "idle"}}}}, PRs: []PR{{Repo: "ts-cos", Number: 1, URL: "https://github.com/TerraScale/ts-cos/pulls/1", Issues: []string{"TS-769"}, Ready: true, CI: "success", Mergeable: true}}}
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

func TestHomeRendersKPIProbe(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	w := NewWeb(store, nil)
	w.KPI.k.fold(Event{ID: "cos:kpi-probe:mariogk:1", Kind: KindKPIProbe})
	r := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	ownerRoutes(w).ServeHTTP(r, req)
	if r.Code != http.StatusOK || !strings.Contains(r.Body.String(), `data-probe="cos:kpi-probe:mariogk:1"`) {
		t.Fatalf("GET / = %d without the probe on #kpis", r.Code)
	}
}
