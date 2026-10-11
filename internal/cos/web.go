package cos

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type Web struct {
	Store       *Store
	Autopilot   *Autopilot
	Hooks       http.Handler
	auth        *Auth
	mu          sync.Mutex
	clients     map[chan struct{}]struct{}
	watchCancel context.CancelFunc
	watchDone   chan struct{}
	inbox       atomic.Pointer[Inbox]
	push        atomic.Pointer[Push]
	Chat        *Chat
	KPI         *KPIBoard
	Bus         *Bus
	agentKV     jetstream.KeyValue // COS_AGENT, opened on first use
}

func NewWeb(s *Store, auth *Auth) *Web {
	return &Web{Store: s, auth: auth, Chat: NewChat(), KPI: &KPIBoard{}, clients: map[chan struct{}]struct{}{}}
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
	return w.auth.Protect(w.routes(), w.Hooks)
}

// routes are the dashboard pages behind sign-in.
func (w *Web) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", w.chatHome)
	mux.HandleFunc("GET /chat/events", w.chatEvents)
	mux.HandleFunc("POST /chat", w.chatPost)
	mux.HandleFunc("POST /chat/answer", w.chatAnswer)
	mux.HandleFunc("GET /settings", w.settings)
	mux.HandleFunc("POST /settings/models", w.modelsSave)
	mux.HandleFunc("GET /inbox", w.inboxPage)
	mux.HandleFunc("GET /inbox/events", w.inboxEvents)
	mux.HandleFunc("GET /tokens", w.tokensPage)
	mux.HandleFunc("GET /tokens/events", w.tokensEvents)
	mux.HandleFunc("GET /release", clusterRelease.events)
	mux.HandleFunc("GET /version", clusterRelease.version)
	mux.HandleFunc("GET /work", w.workPage)
	mux.HandleFunc("GET /work/events", w.workEvents)
	mux.HandleFunc("GET /work/{ticket}", w.workTicket)
	w.autopilotRoutes(mux)
	w.pushRoutes(mux)
	w.nodesRoutes(mux)
	w.nodeDetailRoutes(mux)
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
	mux.HandleFunc("GET /limits", w.limitsJSON)
	mux.HandleFunc("POST /inbox/{id}/{action}", w.InboxAction)
	return mux
}

// InboxAction answers or dismisses an item from any node; the bucket's
// compare-and-set closes it everywhere and the mirror patches every dashboard.
func (w *Web) InboxAction(rw http.ResponseWriter, r *http.Request) {
	in := w.inbox.Load()
	if in == nil {
		http.Error(rw, "Inbox is starting; retry in a few seconds", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(rw, r.Body, 16<<10)
	var err error
	switch r.PathValue("action") {
	case "answer":
		_, err = in.Answer(r.Context(), r.PathValue("id"), AnswerData{Option: r.PostFormValue("option"), Text: r.PostFormValue("text")})
	case "dismiss":
		_, err = in.Dismiss(r.Context(), r.PathValue("id"))
	default:
		http.NotFound(rw, r)
		return
	}
	var public OperationError
	if errors.As(err, &public) {
		http.Error(rw, public.PublicMessage(), http.StatusConflict)
		return
	}
	if err != nil {
		logErr(r.Context(), slog.LevelError, "inbox action", err)
		http.Error(rw, "Inbox update failed", http.StatusBadGateway)
		return
	}
	if r.Header.Get("Datastar-Request") == "true" {
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	http.Redirect(rw, r, "/inbox", http.StatusSeeOther)
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
				logErr(ctx, slog.LevelError, "watch state", err)
			case _, ok := <-changes:
				if !ok {
					return
				}
				next, err := w.Store.Revision(ctx)
				if err != nil && ctx.Err() == nil {
					logErr(ctx, slog.LevelError, "read revision", err)
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
			logErr(ctx, slog.LevelError, "read live state", err)
			return
		}
		live := Refresh(ctx, cfg, bus, old)
		if err := w.Store.SaveLive(ctx, live); err != nil && ctx.Err() == nil {
			logErr(ctx, slog.LevelError, "save live state", err)
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
func integer(n int) string { return strconv.Itoa(n) }
