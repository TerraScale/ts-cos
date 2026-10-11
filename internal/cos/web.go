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
	ownerID     atomic.Pointer[string] // the first admin's ID, cached once the bootstrap marker reads
	Chat        *Chat
	KPI         *KPIBoard
	Bus         *Bus
	Cfg         Config
	agentKV     jetstream.KeyValue // COS_AGENT, opened on first use
	// merge and call are test seams; nil means Forgejo{Config: Cfg}.Merge and Bus.Call.
	merge func(ctx context.Context, repo string, number int, order MergeOrder) (any, error)
	call  func(ctx context.Context, host, command string, args []string, output any) error
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

// routeScope says who may call a dashboard route; routes() enforces it for every route.
type routeScope int

const (
	scopeUser   routeScope = iota // the signed-in user's own data
	scopeShared                   // cluster data every signed-in user may see
	scopeOwner                    // node-T3 data: only the first admin
	scopeAdmin                    // admins only, never cos-probe; no routes yet
)

// webRoute is one dashboard route behind sign-in.
type webRoute struct {
	pattern string
	scope   routeScope
	handler http.HandlerFunc
}

// routes registers every webRoutes entry through one scope check.
func (w *Web) routes() *http.ServeMux {
	mux := http.NewServeMux()
	for _, route := range w.webRoutes() {
		mux.HandleFunc(route.pattern, w.scoped(route.scope, route.handler))
	}
	return mux
}

// webRoutes is the table of every dashboard route behind sign-in. A new route is added here with its scope.
func (w *Web) webRoutes() []webRoute {
	routes := []webRoute{
		{"GET /{$}", scopeUser, w.chatHome},
		{"GET /chat/events", scopeUser, w.chatEvents},
		{"POST /chat", scopeUser, w.chatPost},
		{"POST /chat/answer", scopeUser, w.chatAnswer},
		{"POST /tabs", scopeUser, w.createTab},
		{"POST /tabs/{id}/rename", scopeUser, w.renameTab},
		{"POST /tabs/{id}/close", scopeUser, w.closeTab},
		{"POST /tabs/{tab}/threads/{op}", scopeOwner, w.threadAction},
		{"GET /settings", scopeUser, w.settings},
		{"POST /settings/models", scopeOwner, w.modelsSave},
		{"GET /metrics", scopeOwner, w.metricsPage},
		{"GET /metrics/events", scopeOwner, w.metricsEvents},
		{"GET /inbox", scopeUser, w.inboxPage},
		{"GET /inbox/events", scopeUser, w.inboxEvents},
		{"POST /inbox/{id}/{action}", scopeUser, w.InboxAction},
		{"GET /tokens", scopeOwner, w.tokensPage},
		{"GET /tokens/events", scopeOwner, w.tokensEvents},
		{"GET /work", scopeOwner, w.workPage},
		{"GET /work/events", scopeOwner, w.workEvents},
		{"GET /work/{ticket}", scopeOwner, w.workTicket},
		{"GET /status", scopeOwner, w.status},
		{"GET /limits", scopeOwner, w.limitsJSON},
		{"GET /release", scopeShared, clusterRelease.events},
		{"GET /version", scopeShared, clusterRelease.version},
	}
	routes = append(routes, w.autopilotRoutes()...)
	routes = append(routes, w.pushRoutes()...)
	routes = append(routes, w.nodesRoutes()...)
	return append(routes, w.nodeDetailRoutes()...)
}

// scoped runs h only when the caller may use scope; otherwise the route does not exist for them.
func (w *Web) scoped(scope routeScope, h http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if !w.allowed(r.Context(), scope) {
			http.NotFound(rw, r)
			return
		}
		h(rw, r)
	}
}

// allowed is the scope check: owner routes need the first admin, admin routes need an admin other than cos-probe.
func (w *Web) allowed(ctx context.Context, scope routeScope) bool {
	switch scope {
	case scopeOwner:
		return w.owner(ctx)
	case scopeAdmin:
		u, err := UserFrom(ctx)
		return err == nil && u.Role == roleAdmin && u.ID != probeUserID
	}
	return true
}

// owner reports whether ctx is the first admin, who owns every node T3. It fails closed: before the bootstrap,
// or when the marker cannot be read, nobody is the owner.
func (w *Web) owner(ctx context.Context) bool {
	u, err := UserFrom(ctx)
	if err != nil {
		return false
	}
	if id := w.ownerID.Load(); id != nil {
		return u.ID == *id
	}
	if w.auth == nil || w.auth.nc == nil {
		return false
	}
	id, ok, err := firstAdmin(ctx, w.auth.nc)
	if err != nil {
		logErr(ctx, slog.LevelWarn, "read first admin", err)
		return false
	}
	if !ok {
		return false
	}
	w.ownerID.Store(&id)
	return u.ID == id
}

// status is the JSON view of the whole COS state, owner only.
func (w *Web) status(rw http.ResponseWriter, r *http.Request) {
	v, err := View(r.Context(), w.Store)
	if err != nil {
		http.Error(rw, "Cannot read COS state", 500)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(rw).Encode(v)
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
	if errors.Is(err, errInboxNotFound) { // another user's item, or none: nothing changed
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
