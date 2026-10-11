package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/a-h/templ"
)

type chatHeader struct {
	Running, PRs, Inbox int
	Autopilot           AutopilotState
}

func (w *Web) header(ctx context.Context) chatHeader {
	v, err := View(ctx, w.Store)
	if err != nil {
		slog.Error("read COS state", "error", err)
	}
	return chatHeader{Running: v.Running, PRs: len(v.Live.PRs), Inbox: len(v.Inbox), Autopilot: v.Autopilot}
}

// settings hosts the heartbeat presets (TS-813); account settings join it here.
func (w *Web) settings(rw http.ResponseWriter, r *http.Request) {
	v, err := View(r.Context(), w.Store)
	if err != nil {
		http.Error(rw, "Cannot read COS state", 500)
		return
	}
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := settingsPage(v.Autopilot).Render(r.Context(), rw); err != nil {
		slog.Error("render settings", "error", err)
	}
}

func (w *Web) chatHome(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := chatPage(w.header(r.Context()), w.Chat.Entries()).Render(r.Context(), rw); err != nil {
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
	notify := make(chan struct{}, 1)
	w.mu.Lock()
	w.clients[notify] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.clients, notify); w.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("X-Accel-Buffering", "no")
	sent := map[string]bool{}
	repaint := func() error {
		entries := w.Chat.Entries()
		clear(sent)
		for _, e := range entries {
			sent[e.ID] = true
		}
		if err := patchElements(ctx, rw, chatHead(w.header(ctx))); err != nil {
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
			err = patchElements(ctx, rw, chatHead(w.header(ctx)))
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
	if err := inboxPage(w.header(r.Context()), v).Render(r.Context(), rw); err != nil {
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
		if err := patchElements(ctx, rw, chatHead(w.header(ctx))); err != nil {
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
