package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/nats-io/nats.go/jetstream"
)

type chatHeader struct {
	Running, PRs, Inbox int
	Autopilot           AutopilotState
	Owner               bool // node-T3 counts and links show only for the first admin
}

// viewFor is the dashboard ctx may see: node-T3 state for the owner, and the caller's own Inbox for everyone.
func (w *Web) viewFor(ctx context.Context) (Dashboard, error) {
	if w.owner(ctx) {
		v, err := View(ctx, w.Store)
		v.Owner = true
		return v, err
	}
	var v Dashboard
	var err error
	if v.Inbox, v.History, err = inboxFor(ctx, w.Store); err != nil {
		return Dashboard{}, err
	}
	v.History = v.History[:min(len(v.History), inboxHistoryMax)]
	return v, nil
}

// state reads the dashboard once per patch; a failed read renders what is known, logged.
func (w *Web) state(ctx context.Context) Dashboard {
	v, err := w.viewFor(ctx)
	if err != nil {
		logErr(ctx, slog.LevelError, "read COS state", err)
	}
	return v
}

// header reads state once for pages that only need the topbar.
func (w *Web) header(ctx context.Context) chatHeader { return headerOf(w.state(ctx)) }

func headerOf(v Dashboard) chatHeader {
	return chatHeader{Running: v.Running, PRs: len(v.Live.PRs), Inbox: len(v.Inbox), Autopilot: v.Autopilot, Owner: v.Owner}
}

// kpiView is what the KPI tiles show: the header's live counts plus this node's KPI mirror.
type kpiView struct {
	Running, PRs int
	Autopilot    AutopilotState
	K            KPIs
	Owner        bool
}

func (w *Web) kpis(h chatHeader) kpiView {
	return kpiView{Running: h.Running, PRs: h.PRs, Autopilot: h.Autopilot, K: w.KPI.Snapshot(), Owner: h.Owner}
}

func e2eTone(k KPIs) string {
	switch {
	case !k.E2ESeen:
		return ""
	case k.E2E.Fail > 0:
		return "danger"
	}
	return "ok"
}

func e2eValue(k KPIs) string {
	if !k.E2ESeen {
		return "—"
	}
	return fmt.Sprintf("%d / %d / %d", k.E2E.Pass, k.E2E.Fail, k.E2E.Skip)
}

func e2eSub(k KPIs) string {
	if !k.E2ESeen {
		return "no run yet"
	}
	return age(k.E2E.At) + " ago"
}

func tokensValue(k KPIs) string {
	if !k.UsageSeen {
		return "—"
	}
	return compactTokens(usageTokens(k.Usage))
}

func tokensSub(k KPIs) string {
	if !k.UsageSeen {
		return "no usage yet today"
	}
	return fmt.Sprintf("Haiku %d%% · since 00:00 UTC", k.Usage.HaikuShare)
}

func compactTokens(n int64) string {
	switch {
	case n >= 1e6:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	}
	return strconv.FormatInt(n, 10)
}

func heartbeatValue(a AutopilotState) string {
	if a.Interval == 0 {
		return "off"
	}
	return "every " + integer(a.Interval) + " min"
}

func heartbeatTone(a AutopilotState) string {
	if a.Error != "" {
		return "danger"
	}
	return ""
}

func heartbeatSub(a AutopilotState) string {
	if a.Error != "" {
		return "last check failed"
	}
	if a.Interval == 0 {
		return "no heartbeat set"
	}
	next := a.NextBeatTime()
	if next.IsZero() {
		return "waiting for the first beat"
	}
	return "next " + clock(next)
}

// agentState reads the replicated COS state from any node; without a bus there is no state yet.
func (w *Web) agentState(ctx context.Context) (AgentState, error) {
	var st AgentState
	if w.Bus == nil {
		return st, nil
	}
	w.mu.Lock()
	kv := w.agentKV
	w.mu.Unlock()
	if kv == nil {
		var err error
		if kv, err = openKV(ctx, w.Bus, agentBucket); err != nil {
			return st, err
		}
		w.mu.Lock()
		w.agentKV = kv
		w.mu.Unlock()
	}
	e, err := kv.Get(ctx, "state")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	return st, json.Unmarshal(e.Value(), &st)
}

// settings hosts the heartbeat presets (TS-813), the model policy and the COS wakes for the owner; push and account settings are everyone's.
func (w *Web) settings(rw http.ResponseWriter, r *http.Request) {
	var m modelsPanelData
	if w.owner(r.Context()) {
		m = modelsPanelFor(CurrentBehavior().Models, "")
	}
	w.renderSettings(rw, r, http.StatusOK, m)
}

// renderSettings writes the Settings page with the Model policy section m, at status.
func (w *Web) renderSettings(rw http.ResponseWriter, r *http.Request, status int, m modelsPanelData) {
	v, err := w.viewFor(r.Context())
	if err != nil {
		http.Error(rw, "Cannot read COS state", 500)
		return
	}
	var wakes []WakeRecord
	if v.Owner {
		st, err := w.agentState(r.Context())
		if err != nil {
			slog.Error("read COS agent state", "error", err)
		}
		wakes = st.recentWakes(20)
	}
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.WriteHeader(status)
	if err := settingsPage(headerOf(v), v.Autopilot, m, w.pushState(r.Context()), wakes).Render(r.Context(), rw); err != nil {
		logErr(r.Context(), slog.LevelError, "render settings", err)
	}
}

// homeBoard reads the Work summary for the owner's home aside; a failed read is logged and reports false.
func (w *Web) homeBoard(ctx context.Context) (workBoard, bool) {
	if !w.owner(ctx) {
		return workBoard{}, false
	}
	data, err := loadBoard(ctx, w.Store)
	if err != nil {
		logErr(ctx, slog.LevelError, "read work board", err)
		return workBoard{}, false
	}
	return newWorkBoard(data, ""), true
}

// homeNodes reads the node cards for the home aside; a failed read is logged and reports false.
func (w *Web) homeNodes(ctx context.Context) ([]NodeCard, bool) {
	cards, err := NodeCards(ctx, w.Bus)
	if err != nil {
		logErr(ctx, slog.LevelError, "read node cards", err)
		return nil, false
	}
	return cards, true
}

func (w *Web) chatHome(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tab, open := w.openTab(r)
	if !open {
		tabRedirect(rw, r, "", "")
		return
	}
	v := w.state(r.Context())
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	h := headerOf(v)
	threads := tabThreads(v.Live, w.Chat.Tags(ctx, tab.ID))
	if err := chatPage(h, v, openTabs(w.Chat.Tabs(ctx)), tab, w.Chat.Entries(ctx, tab.ID), threads, r.URL.Query().Get("notice")).Render(r.Context(), rw); err != nil {
		logErr(r.Context(), slog.LevelError, "render chat", err)
	}
}

// sse writes one Datastar event; each payload line gets its own data line.
func sse(rw io.Writer, event string, fields []string, key, payload string) error {
	var b strings.Builder
	b.WriteString("event: " + event + "\n")
	for _, f := range fields {
		b.WriteString("data: " + f + "\n")
	}
	for _, line := range strings.Split(payload, "\n") {
		b.WriteString("data: " + key + " " + line + "\n")
	}
	b.WriteString("\n")
	_, err := io.WriteString(rw, b.String())
	return err
}

func patchElements(ctx context.Context, rw io.Writer, c templ.Component, fields ...string) error {
	var b bytes.Buffer
	if err := c.Render(ctx, &b); err != nil {
		return err
	}
	return sse(rw, "datastar-patch-elements", fields, "elements", b.String())
}

// chatEvents streams chat changes as element patches: new entries append, edits morph in place.
func (w *Web) chatEvents(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	controller := http.NewResponseController(rw)
	tab, _ := w.openTab(r)
	sub, stop := w.Chat.Subscribe(ctx)
	defer stop()
	notify := make(chan struct{}, 1)
	w.mu.Lock()
	w.clients[notify] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.clients, notify); w.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("X-Accel-Buffering", "no")
	sent := map[string]bool{}
	// home patches the header and Inbox from one state read.
	home := func(v Dashboard) error {
		h := headerOf(v)
		if err := patchElements(ctx, rw, chatHead(h, "chat")); err != nil {
			return err
		}
		if err := patchElements(ctx, rw, inbox(v)); err != nil {
			return err
		}
		return patchElements(ctx, rw, threadPanel(tabThreads(v.Live, w.Chat.Tags(ctx, tab.ID)), recentActions(w.Chat.Entries(ctx, tab.ID))))
	}
	// tabStrip repaints the tab strip; the current tab's name may have changed meanwhile.
	tabStrip := func() error {
		cur := tab
		if t, ok := w.Chat.Tab(ctx, tab.ID); ok {
			cur = t
		}
		return patchElements(ctx, rw, chatTabs(openTabs(w.Chat.Tabs(ctx)), cur))
	}
	// closedNotice tells the reader when the tab on screen was closed or deleted.
	closedNotice := func() error {
		if _, live := w.openTab(r); live {
			return nil
		}
		return patchElements(ctx, rw, tabClosedNotice())
	}
	repaint := func() error {
		entries := w.Chat.Entries(ctx, tab.ID)
		clear(sent)
		for _, e := range entries {
			sent[e.ID] = true
		}
		if err := home(w.state(ctx)); err != nil {
			return err
		}
		return patchElements(ctx, rw, chatMessages(entries))
	}
	// window is the IDs the log shows now; the cache keeps trimmed entries, so an edit must not re-append them.
	window := func() map[string]bool {
		ids := map[string]bool{}
		for _, e := range w.Chat.Entries(ctx, tab.ID) {
			ids[e.ID] = true
		}
		return ids
	}
	// panel repaints the Threads panel: the tab's live rows and its recent action outcomes.
	panel := func() error {
		return patchElements(ctx, rw, threadPanel(tabThreads(w.state(ctx).Live, w.Chat.Tags(ctx, tab.ID)), recentActions(w.Chat.Entries(ctx, tab.ID))))
	}
	update := func(keys []string) error {
		shown := window()
		for _, key := range keys {
			if strings.HasPrefix(key, chatTabPrefix) {
				if err := tabStrip(); err != nil {
					return err
				}
				if err := closedNotice(); err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(key, chatTagPrefix) {
				if err := panel(); err != nil {
					return err
				}
				continue
			}
			e, ok := w.Chat.Entry(ctx, key)
			if ok && e.Tab != tab.ID {
				continue
			}
			if ok && e.Action {
				if err := panel(); err != nil {
					return err
				}
			}
			switch {
			case key == "" || (ok && len(sent) == 0):
				return repaint()
			case !ok && sent[key]:
				delete(sent, key)
				if err := sse(rw, "datastar-patch-elements", []string{"selector #m-" + key, "mode remove"}, "elements", ""); err != nil {
					return err
				}
			case ok && sent[key]:
				if err := patchElements(ctx, rw, chatEntry(e)); err != nil {
					return err
				}
			case ok && shown[key]:
				sent[key] = true
				if err := patchElements(ctx, rw, chatEntry(e), "selector #messages", "mode append"); err != nil {
					return err
				}
			}
		}
		return nil
	}
	// trim removes the entries that left the window: trimming changes no key of its own, so no change would show it.
	trim := func() error {
		shown := window()
		for id := range sent {
			if shown[id] {
				continue
			}
			delete(sent, id)
			if err := sse(rw, "datastar-patch-elements", []string{"selector #m-" + id, "mode remove"}, "elements", ""); err != nil {
				return err
			}
		}
		return nil
	}
	// follow bumps chatTick after each batch; the log scrolls only while the reader is at its bottom.
	follow := func() error {
		return sse(rw, "datastar-patch-signals", nil, "signals", fmt.Sprintf(`{"chatTick":%d}`, time.Now().UnixNano()))
	}
	flush := func(err error) bool {
		if err == nil {
			err = controller.Flush()
		}
		return err == nil
	}
	_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if !flush(repaint()) {
		return
	}
	if !flush(closedNotice()) {
		return
	}
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		var err error
		select {
		case <-ctx.Done():
			return
		case <-sub.wake:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err = update(w.Chat.Changed(sub))
			if err == nil {
				err = trim()
			}
			if err == nil {
				err = follow()
			}
		case <-notify:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err = home(w.state(ctx))
		case <-keepalive.C:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = io.WriteString(rw, ": keepalive\n\n")
		}
		if !flush(err) {
			return
		}
	}
}

// chatPost stores the user's message on the replicated chat; the leader's COS agent picks it up.
func (w *Web) chatPost(rw http.ResponseWriter, r *http.Request) {
	// Cross-site requests can carry the session cookie; Datastar's header needs a CORS preflight.
	if r.Header.Get("Datastar-Request") != "true" {
		http.Error(rw, "Datastar request required", http.StatusForbidden)
		return
	}
	var in struct {
		Msg string `json:"msg"`
	}
	rw.Header().Set("Content-Type", "text/event-stream")
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil {
		chatNotice(rw, "Invalid request")
		return
	}
	tab, open := w.openTab(r)
	if !open {
		chatNotice(rw, "This chat tab is closed or unknown")
		return
	}
	text := strings.TrimSpace(in.Msg)
	if text == "" || len([]rune(text)) > chatMaxText {
		chatNotice(rw, fmt.Sprintf("Message must be 1 to %d characters", chatMaxText))
		return
	}
	if _, err := w.Chat.Put(r.Context(), ChatEntry{Role: "user", Text: text, Tab: tab.ID}); err != nil {
		logErr(r.Context(), slog.LevelError, "store chat message", err)
		chatNotice(rw, "Chat is unavailable; try again in a few seconds")
		return
	}
	chatNotice(rw, "")
	_ = sse(rw, "datastar-patch-signals", nil, "signals", `{"msg":""}`)
}

// chatNotice shows text beside the composer; empty text clears it. The stream answers 200 so Datastar applies the patch.
func chatNotice(rw http.ResponseWriter, text string) {
	_ = sse(rw, "datastar-patch-elements", nil, "elements", fmt.Sprintf(`<p id="chat-notice" class="toast" role="status">%s</p>`, html.EscapeString(text)))
}

func toolSummary(tools []string) string {
	if len(tools) == 1 {
		return "1 tool call"
	}
	return fmt.Sprintf("%d tool calls", len(tools))
}

// inboxPage shows TS-815's Inbox under the shared chat header.
func (w *Web) inboxPage(rw http.ResponseWriter, r *http.Request) {
	v, err := w.viewFor(r.Context())
	if err != nil {
		http.Error(rw, "Cannot read COS state", 500)
		return
	}
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := inboxPage(headerOf(v), v).Render(r.Context(), rw); err != nil {
		logErr(r.Context(), slog.LevelError, "render inbox", err)
	}
}

// inboxEvents repatches the Inbox and header whenever COS state changes on any node.
func (w *Web) inboxEvents(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	controller := http.NewResponseController(rw)
	notify := make(chan struct{}, 1)
	w.mu.Lock()
	w.clients[notify] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.clients, notify); w.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("X-Accel-Buffering", "no")
	patch := func() error {
		v, err := w.viewFor(ctx)
		if err != nil {
			return err
		}
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := patchElements(ctx, rw, chatHead(headerOf(v), "inbox")); err != nil {
			return err
		}
		if err := patchElements(ctx, rw, inbox(v)); err != nil {
			return err
		}
		return controller.Flush()
	}
	if patch() != nil {
		return
	}
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
			if patch() != nil {
				return
			}
		case <-keepalive.C:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := io.WriteString(rw, ": keepalive\n\n"); err != nil || controller.Flush() != nil {
				return
			}
		}
	}
}

func answerAction(entry, question string, option int) string {
	return fmt.Sprintf("@post('/chat/answer?e=%s&q=%s&o=%d')", url.QueryEscape(entry), url.QueryEscape(question), option)
}

// chatAnswer records one choice; once every question has one, it answers T3 on the COS host over NATS.
func (w *Web) chatAnswer(rw http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Datastar-Request") != "true" {
		http.Error(rw, "Datastar request required", http.StatusForbidden)
		return
	}
	query := r.URL.Query()
	// Another user's entry is not found: it does not exist for this caller.
	e, ok := w.Chat.Entry(r.Context(), query.Get("e"))
	if !ok {
		http.Error(rw, "Unknown question", http.StatusNotFound)
		return
	}
	option, err := strconv.Atoi(query.Get("o"))
	if e.Ask == nil || e.Ask.Done || err != nil {
		http.Error(rw, "This question is no longer open", http.StatusConflict)
		return
	}
	i := slices.IndexFunc(e.Ask.Questions, func(q ChatQuestion) bool { return q.ID == query.Get("q") })
	if i < 0 || option < 0 || option >= len(e.Ask.Questions[i].Options) {
		http.Error(rw, "Unknown question or option", http.StatusBadRequest)
		return
	}
	// Refuse a second answer: the mirror may not have marked the ask Done yet, so this would overwrite.
	if _, answered := e.Ask.Answers[e.Ask.Questions[i].ID]; answered {
		http.Error(rw, "Already answered", http.StatusConflict)
		return
	}
	if e.Ask.Answers == nil {
		e.Ask.Answers = map[string]string{}
	}
	question := e.Ask.Questions[i].ID
	e.Ask.Answers[question] = e.Ask.Questions[i].Options[option]
	// Fence first: only the write that wins the revision race may deliver answers to T3.
	if err := w.Chat.Replace(r.Context(), e); err != nil {
		http.Error(rw, "The question changed meanwhile; try again", http.StatusConflict)
		return
	}
	if len(e.Ask.Answers) == len(e.Ask.Questions) {
		answers, _ := json.Marshal(e.Ask.Answers)
		if err := w.Chat.Respond(r.Context(), e.Ask.Host, e.Ask.ThreadID, e.Ask.RequestID, string(answers)); err != nil {
			logErr(r.Context(), slog.LevelError, "answer COS question", err, "host", e.Ask.Host)
			// Undo the recorded answer so the question shows its buttons again.
			if cur, ok := w.Chat.Entry(r.Context(), e.ID); ok && cur.Ask != nil {
				delete(cur.Ask.Answers, question)
				if err := w.Chat.Replace(context.WithoutCancel(r.Context()), cur); err != nil {
					logErr(context.WithoutCancel(r.Context()), slog.LevelError, "roll back COS answer", err, "entry", e.ID)
				}
			}
			if errors.Is(err, errBusUnavailable) {
				http.Error(rw, "COS bus unavailable", http.StatusServiceUnavailable)
				return
			}
			http.Error(rw, "COS host did not accept the answer; try again", http.StatusBadGateway)
			return
		}
	}
	rw.WriteHeader(http.StatusNoContent)
}

// navLinks are the top bar's sections, in order; the Inbox count sits beside them.
var navLinks = []struct {
	page, href, label string
	owner             bool // node-T3 pages: only the first admin sees the link
}{
	{"chat", "/", "Chat", false}, {"metrics", "/metrics", "Metrics", true}, {"nodes", "/nodes", "Nodes", false},
	{"work", "/work", "Work", true}, {"tokens", "/tokens", "Tokens", true}, {"settings", "/settings", "Settings", false},
}

// currentPage is aria-current for a nav link: "page" on its own page, omitted elsewhere.
func currentPage(page, link string) templ.Attributes {
	if page == link {
		return templ.Attributes{"aria-current": "page"}
	}
	return nil
}
