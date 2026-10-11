package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/templ"
)

type chatHeader struct {
	Running, PRs, Inbox int
	Autopilot           AutopilotState
}

// state reads the dashboard once per patch; a failed read renders what is known, logged.
func (w *Web) state(ctx context.Context) Dashboard {
	v, err := View(ctx, w.Store)
	if err != nil {
		slog.Error("read COS state", "error", err)
	}
	return v
}

// header reads state once for pages that only need the topbar.
func (w *Web) header(ctx context.Context) chatHeader { return headerOf(w.state(ctx)) }

func headerOf(v Dashboard) chatHeader {
	return chatHeader{Running: v.Running, PRs: len(v.Live.PRs), Inbox: len(v.Inbox), Autopilot: v.Autopilot}
}

// kpiView is what the KPI tiles show: the header's live counts plus this node's KPI mirror.
type kpiView struct {
	Running, PRs int
	Autopilot    AutopilotState
	K            KPIs
}

func (w *Web) kpis(h chatHeader) kpiView {
	return kpiView{Running: h.Running, PRs: h.PRs, Autopilot: h.Autopilot, K: w.KPI.Snapshot()}
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
	return fmt.Sprintf("$%.2f", k.Usage.Cost)
}

func tokensSub(k KPIs) string {
	if !k.UsageSeen {
		return "no usage yet today"
	}
	return fmt.Sprintf("%s out · Haiku %d%% · since 00:00 UTC", compactTokens(k.Usage.Output), k.Usage.HaikuShare)
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

// settings hosts the heartbeat presets (TS-813), token budgets (TS-826); account settings join it here.
func (w *Web) settings(rw http.ResponseWriter, r *http.Request) {
	w.renderSettings(rw, r, http.StatusOK, w.budgetPanel(r.Context(), time.Now()))
}

// renderSettings writes the Settings page with the Budgets section d, at status.
func (w *Web) renderSettings(rw http.ResponseWriter, r *http.Request, status int, d budgetPanelData) {
	v, err := View(r.Context(), w.Store)
	if err != nil {
		http.Error(rw, "Cannot read COS state", 500)
		return
	}
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.WriteHeader(status)
	if err := settingsPage(v.Autopilot, d, w.pushState(r.Context(), pushUser(r))).Render(r.Context(), rw); err != nil {
		slog.Error("render settings", "error", err)
	}
}

// homeBoard reads the Work summary for the home aside; a failed read is logged and reports false.
func (w *Web) homeBoard(ctx context.Context) (workBoard, bool) {
	data, err := loadBoard(ctx, w.Store)
	if err != nil {
		slog.Error("read work board", "error", err)
		return workBoard{}, false
	}
	return newWorkBoard(data, ""), true
}

func (w *Web) chatHome(rw http.ResponseWriter, r *http.Request) {
	v := w.state(r.Context())
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	h := headerOf(v)
	b, ok := w.homeBoard(r.Context())
	if err := chatPage(h, w.kpis(h), v, w.Chat.Entries(), b, ok).Render(r.Context(), rw); err != nil {
		slog.Error("render chat", "error", err)
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
	sub, stop := w.Chat.Subscribe()
	defer stop()
	kpiWake, stopKPI := w.KPI.Subscribe()
	defer stopKPI()
	notify := make(chan struct{}, 1)
	w.mu.Lock()
	w.clients[notify] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.clients, notify); w.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("X-Accel-Buffering", "no")
	sent := map[string]bool{}
	var last chatHeader
	// home patches the header, KPI tiles, Work summary and Inbox from one state read.
	home := func(v Dashboard) error {
		h := headerOf(v)
		last = h
		if err := patchElements(ctx, rw, chatHead(h)); err != nil {
			return err
		}
		if err := patchElements(ctx, rw, kpiTiles(w.kpis(h))); err != nil {
			return err
		}
		if err := patchElements(ctx, rw, countBadge("side-inbox", "badge", h.Inbox)); err != nil {
			return err
		}
		if err := patchElements(ctx, rw, countBadge("tab-inbox", "tab-badge", h.Inbox)); err != nil {
			return err
		}
		b, ok := w.homeBoard(ctx)
		if err := patchElements(ctx, rw, workPane(b, ok)); err != nil {
			return err
		}
		return patchElements(ctx, rw, inbox(v))
	}
	repaint := func() error {
		entries := w.Chat.Entries()
		clear(sent)
		for _, e := range entries {
			sent[e.ID] = true
		}
		if err := home(w.state(ctx)); err != nil {
			return err
		}
		return patchElements(ctx, rw, chatMessages(entries))
	}
	update := func(keys []string) error {
		for _, key := range keys {
			e, ok := w.Chat.Entry(key)
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
			case ok:
				sent[key] = true
				if err := patchElements(ctx, rw, chatEntry(e), "selector #messages", "mode append"); err != nil {
					return err
				}
			}
		}
		return nil
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
		case <-notify:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err = home(w.state(ctx))
		case <-kpiWake:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err = patchElements(ctx, rw, kpiTiles(w.kpis(last)))
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
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil {
		http.Error(rw, "Invalid request", http.StatusBadRequest)
		return
	}
	text := strings.TrimSpace(in.Msg)
	if text == "" || len([]rune(text)) > chatMaxText {
		http.Error(rw, fmt.Sprintf("Message must be 1 to %d characters", chatMaxText), http.StatusBadRequest)
		return
	}
	if _, err := w.Chat.Put(r.Context(), ChatEntry{Role: "user", Text: text}); err != nil {
		slog.Error("store chat message", "error", err)
		http.Error(rw, "Chat is unavailable; try again in a few seconds", http.StatusServiceUnavailable)
		return
	}
	rw.Header().Set("Content-Type", "text/event-stream")
	_ = sse(rw, "datastar-patch-signals", nil, "signals", `{"msg":""}`)
}

func toolSummary(tools []string) string {
	if len(tools) == 1 {
		return "1 tool call"
	}
	return fmt.Sprintf("%d tool calls", len(tools))
}

// inboxPage shows TS-815's Inbox under the shared chat header.
func (w *Web) inboxPage(rw http.ResponseWriter, r *http.Request) {
	v, err := View(r.Context(), w.Store)
	if err != nil {
		http.Error(rw, "Cannot read COS state", 500)
		return
	}
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := inboxPage(headerOf(v), v).Render(r.Context(), rw); err != nil {
		slog.Error("render inbox", "error", err)
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
		v, err := View(ctx, w.Store)
		if err != nil {
			return err
		}
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := patchElements(ctx, rw, chatHead(headerOf(v))); err != nil {
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
