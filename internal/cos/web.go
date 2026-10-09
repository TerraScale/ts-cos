package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Web struct {
	Store       *Store
	token       string
	mu          sync.Mutex
	clients     map[chan struct{}]struct{}
	watchCancel context.CancelFunc
	watchDone   chan struct{}
}

func NewWeb(s *Store, token string) *Web {
	return &Web{Store: s, token: token, clients: map[chan struct{}]struct{}{}}
}
func (w *Web) Broadcast() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for c := range w.clients {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}
func (w *Web) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(rw http.ResponseWriter, r *http.Request) {
		v, err := View(r.Context(), w.Store)
		if err != nil {
			http.Error(rw, "Cannot read COS state", 500)
			return
		}
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		rw.Header().Set("Cache-Control", "no-store")
		if err := page(v).Render(r.Context(), rw); err != nil {
			slog.Error("render dashboard", "error", err)
		}
	})
	mux.HandleFunc("GET /events", w.Events)
	mux.HandleFunc("GET /status", func(rw http.ResponseWriter, r *http.Request) {
		v, err := View(r.Context(), w.Store)
		if err != nil {
			http.Error(rw, "Cannot read COS state", 500)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(rw).Encode(v)
	})
	return dashboardAuth(w.token, mux)
}
func (w *Web) Events(rw http.ResponseWriter, r *http.Request) {
	if _, ok := rw.(http.Flusher); !ok {
		http.Error(rw, "Streaming unavailable", 500)
		return
	}
	notify := make(chan struct{}, 1)
	w.mu.Lock()
	w.clients[notify] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.clients, notify); w.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-store")
	rw.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(rw)
	patch := func() error {
		v, err := View(r.Context(), w.Store)
		if err != nil {
			return err
		}
		var b bytes.Buffer
		if err := dashboard(v).Render(r.Context(), &b); err != nil {
			return err
		}
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := fmt.Fprint(rw, "event: datastar-patch-elements\ndata: selector #dashboard\ndata: mode outer\n"); err != nil {
			return err
		}
		for _, line := range strings.Split(b.String(), "\n") {
			if _, err := fmt.Fprintf(rw, "data: elements %s\n", line); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprint(rw, "\n"); err != nil {
			return err
		}
		return controller.Flush()
	}
	if err := patch(); err != nil {
		return
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-notify:
			if err := patch(); err != nil {
				return
			}
		case <-heartbeat.C:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := fmt.Fprint(rw, ": keepalive\n\n"); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
		}
	}
}

func (w *Web) Watch(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	changes, errs, err := w.Store.Watch(ctx)
	if err != nil {
		cancel()
		return err
	}
	revision, err := w.Store.Revision(ctx)
	if err != nil {
		cancel()
		return err
	}
	w.watchCancel = cancel
	w.watchDone = make(chan struct{})
	go func() {
		defer close(w.watchDone)
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-errs:
				if !ok {
					return
				}
				slog.Error("watch state", "error", err)
			case _, ok := <-changes:
				if !ok {
					return
				}
				next, err := w.Store.Revision(ctx)
				if err != nil && ctx.Err() == nil {
					slog.Error("read revision", "error", err)
					continue
				}
				if ctx.Err() != nil {
					return
				}
				if next != revision {
					revision = next
					w.Broadcast()
				}
			}
		}
	}()
	return nil
}

func (w *Web) Close() {
	if w.watchCancel != nil {
		w.watchCancel()
		<-w.watchDone
	}
}
func (w *Web) Poll(ctx context.Context, cfg Config, bus *Bus) {
	refresh := func() {
		old, err := w.Store.Live(ctx)
		if err != nil {
			slog.Error("read live state", "error", err)
			return
		}
		live := Refresh(ctx, cfg, bus, old)
		if err := w.Store.SaveLive(ctx, live); err != nil && ctx.Err() == nil {
			slog.Error("save live state", "error", err)
		}
	}
	refresh()
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}
func integer(n int) string   { return strconv.Itoa(n) }
func join(s []string) string { return strings.Join(s, ", ") }
func notesOf(notes []Note, kind string) []Note {
	out := []Note{}
	for _, n := range notes {
		if n.Kind == kind {
			out = append(out, n)
		}
	}
	return out
}
func reviewLabel(p PR) string {
	if p.ChangesRequested {
		return "changes requested"
	}
	if p.BotApproved {
		return "approved"
	}
	if p.SkipReviewer {
		return "review bypassed"
	}
	return "waiting"
}
func mergeLabel(p PR) string {
	if p.Ready {
		return "ready"
	}
	if !p.Mergeable {
		return "conflict"
	}
	return "clean"
}
