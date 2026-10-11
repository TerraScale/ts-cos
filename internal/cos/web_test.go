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
	server := httptest.NewServer(web.routes())
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
	if messages := readEvent(); !strings.Contains(messages, `<div id="messages"`) {
		t.Fatal(messages)
	}
	other, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	live := Live{At: "2026-10-10T12:00:00Z", Errors: map[string]string{}, Fleet: []HostFleet{{Host: "delta", Active: 2}}, PRs: []PR{{Repo: "ts-cos", Number: 1, URL: "https://git.terrascale.tech/TerraScale/ts-cos/pulls/1"}, {Repo: "ts-cos", Number: 2, URL: "https://git.terrascale.tech/TerraScale/ts-cos/pulls/2"}}}
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
	h := NewWeb(store, nil).routes()
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
	for _, want := range []string{`data-init="@get('/chat/events', {retry: 'always'`, `id="messages"`, `href="/settings"`, `id="inbox-slot"`, `id="heartbeat-slot"`, "0 running", "0 PRs waiting", cssPath} {
		if r.Code != http.StatusOK || !strings.Contains(body, want) {
			t.Fatal(r.Code, "missing", want)
		}
	}
	inboxPage := serve("GET", "/inbox", false, "")
	for _, want := range []string{`data-init="@get('/inbox/events'`, `id="inbox"`, `href="/inbox">Inbox <span class="badge badge-accent">0</span>`, "Nothing needs you"} {
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
	for name, body := range map[string]string{
		"blank":    `{"msg":"   "}`,
		"not json": `not json`,
		"too long": `{"msg":"` + strings.Repeat("x", 20001) + `"}`,
	} {
		if r := serve("POST", "/chat", true, body); r.Code != http.StatusBadRequest {
			t.Errorf("POST /chat %s = %d", name, r.Code)
		}
	}
	// NewChat has no bus, so the store is not ready and the message cannot be saved.
	if r := serve("POST", "/chat", true, `{"msg":"hi"}`); r.Code != http.StatusServiceUnavailable {
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
