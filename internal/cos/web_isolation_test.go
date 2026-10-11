package cos

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ownerRoutes serves w's routes to the first admin, as the dashboard does after sign-in.
func ownerRoutes(w *Web) http.Handler {
	owner := User{ID: newULID(), Name: "owner", Role: roleAdmin}
	w.ownerID.Store(&owner.ID)
	return asUserHandler(owner, w.routes())
}

// isolationFixture is one COS with the first admin A, a member B and cos-probe P, and A's data seeded under markers.
type isolationFixture struct {
	web     *Web
	routes  http.Handler
	chat    *Chat
	inbox   *Inbox
	push    pushUserFixture
	a, b, p User
	entry   ChatEntry
	item    InboxItem
	device  string // A's first device endpoint
	markers []string
}

// newIsolationFixture seeds A's chat, Inbox, push devices and prefs, memory and context, and an owner PR with an ask.
func newIsolationFixture(t *testing.T) isolationFixture {
	t.Helper()
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	users, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: authBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	a, err := createUser(ctx, users, "admin-a", roleAdmin, "not-a-hash")
	if err != nil {
		t.Fatal(err)
	}
	b, err := createUser(ctx, users, "member-b", roleMember, "not-a-hash")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := users.Put(ctx, "bootstrapped", []byte(a.ID)); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	web := NewWeb(store, NewAuth(t.TempDir(), js))
	chat := NewChat()
	go chat.Run(ctx, bus)
	chatTestWait(t, "chat ready", chat.Ready)
	web.Chat = chat
	inbox := inboxTestOpen(t, bus, js)
	web.inbox.Store(inbox)
	push := newPushUserFixture(t)
	web.push.Store(push.p)
	actx := WithUser(ctx, a)

	f := isolationFixture{web: web, routes: web.routes(), chat: chat, inbox: inbox, push: push, a: a, b: b, p: probeUser}
	ask := ChatAsk{Host: "delta", ThreadID: "thread-1", RequestID: "q1", Questions: []ChatQuestion{{ID: "pick", Header: "Deploy", Text: "Which window?", Options: []string{"Now", "Tonight"}}}}
	if f.entry, err = chat.Put(actx, ChatEntry{Role: "cos", Text: markerChat, Ask: &ask}); err != nil {
		t.Fatal(err)
	}
	chatTestWait(t, "A's entry", func() bool { _, ok := chat.Entry(actx, f.entry.ID); return ok })
	if f.item, err = inbox.Add(actx, InboxInput{Title: markerInbox, Options: []string{"Yes", "No"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveInboxItem(ctx, f.item); err != nil {
		t.Fatal(err)
	}
	// Two devices, so A's settings say "2 devices subscribed" and that text marks A's push data.
	for _, path := range []string{"/a-device-1", "/a-device-2"} {
		sub, _, _, err := newPushSubscription(push.receiver.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		if err := push.p.Subscribe(actx, "marker-agent", sub); err != nil {
			t.Fatal(err)
		}
		if f.device == "" {
			f.device = sub.Endpoint
		}
	}
	if err := push.p.SetPref(actx, "chat", false); err != nil {
		t.Fatal(err)
	}
	memKV, err := openKV(ctx, bus, memoryBucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := memoryAdd(actx, mustUserKV(t, actx, memKV), MemoryEntry{Kind: "fact", Text: markerMemory}); err != nil {
		t.Fatal(err)
	}
	ctxKV, err := openKV(ctx, bus, contextBucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mustUserKV(t, actx, ctxKV).Put(ctx, "notes", []byte(markerContext)); err != nil {
		t.Fatal(err)
	}
	// Owner data: a PR whose title names a ticket becomes a Work task, and a live ask.
	live := Live{At: "2026-10-11T12:00:00Z", Errors: map[string]string{}, PRs: []PR{{Repo: "ts-cos", Number: 931, URL: "https://git.terrascale.tech/TerraScale/ts-cos/pulls/931", Title: "TS-931 " + markerPR}}, Asks: []Ask{{Host: "delta", ThreadID: "thread-931", RequestID: "req-931", Question: markerAsk, Options: []string{"Yes", "No"}}}}
	if err := store.SaveLive(ctx, live); err != nil {
		t.Fatal(err)
	}
	f.markers = []string{markerChat, markerInbox, markerMemory, markerContext, markerPR, markerAsk, "2 devices subscribed"}
	return f
}

const (
	markerChat    = "marker-chat-7f3c"
	markerInbox   = "marker-inbox-7f3c"
	markerMemory  = "marker-memory-7f3c"
	markerContext = "marker-context-7f3c"
	markerPR      = "marker-owner-pr-7f3c"
	markerAsk     = "marker-owner-ask-7f3c"
)

func mustUserKV(t *testing.T, ctx context.Context, kv jetstream.KeyValue) jetstream.KeyValue {
	t.Helper()
	view, err := UserKV(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

// isolationCase is one route's request in the walk: a concrete path with A's IDs filled in, and what A's view shows.
type isolationCase struct {
	path      string
	body      string   // JSON when it starts with "{", form otherwise
	stream    bool     // an event stream: read it for a short while, then stop
	foreign   bool     // the path names A's data: a non-owner gets 404 or 403
	own       string   // text A's own view shows
	ownerOnly []string // markup only the owner's view has
	others    []string // markup every signed-in user's view has
}

// cases is one entry per webRoutes() pattern; TestEveryRouteIsolatesUsers fails on a missing or an extra one.
func (f isolationFixture) cases() map[string]isolationCase {
	return map[string]isolationCase{
		"GET /{$}":                            {path: "/", own: markerChat, ownerOnly: []string{`id="kpis"`, `id="pane-work"`, `href="/work"`}, others: []string{`id="pane-nodes"`, `id="messages"`}},
		"GET /chat/events":                    {path: "/chat/events", stream: true, own: markerChat, ownerOnly: []string{`id="pane-work"`}},
		"POST /chat":                          {path: "/chat", body: `{"msg":"hello from B"}`},
		"POST /chat/answer":                   {path: "/chat/answer?e=" + f.entry.ID + "&q=pick&o=1", foreign: true},
		"GET /settings":                       {path: "/settings", own: "2 devices subscribed", ownerOnly: []string{`id="autopilot"`, `id="budgets"`}},
		"POST /settings/budgets":              {path: "/settings/budgets", body: "thread=10M&daily=100M"},
		"GET /inbox":                          {path: "/inbox", own: markerInbox},
		"GET /inbox/events":                   {path: "/inbox/events", stream: true, own: markerInbox},
		"POST /inbox/{id}/{action}":           {path: "/inbox/" + f.item.ID + "/dismiss", foreign: true},
		"GET /tokens":                         {path: "/tokens"},
		"GET /tokens/events":                  {path: "/tokens/events", stream: true},
		"GET /work":                           {path: "/work", own: "TS-931"},
		"GET /work/events":                    {path: "/work/events", stream: true, own: "TS-931"},
		"GET /work/{ticket}":                  {path: "/work/TS-931", own: "TS-931", foreign: true},
		"GET /status":                         {path: "/status", own: markerPR},
		"GET /release":                        {path: "/release", stream: true},
		"GET /version":                        {path: "/version"},
		"POST /autopilot/interval/{minutes}":  {path: "/autopilot/interval/15"},
		"POST /autopilot/run":                 {path: "/autopilot/run"},
		"GET /nodes":                          {path: "/nodes"},
		"GET /nodes/events":                   {path: "/nodes/events", stream: true},
		"GET /nodes/{host}":                   {path: "/nodes/delta"},
		"GET /push/key":                       {path: "/push/key"},
		"POST /push/subscribe":                {path: "/push/subscribe"},
		"POST /push/unsubscribe":              {path: "/push/unsubscribe", body: `{"endpoint":"` + f.device + `"}`},
		"POST /push/test":                     {path: "/push/test"},
		"POST /push/prefs/{category}/{state}": {path: "/push/prefs/chat/off"},
	}
}

// serve sends c as u through the routes. A stream gets a short deadline: the handler runs until it ends.
func (f isolationFixture) serve(t *testing.T, u User, method string, c isolationCase) *httptest.ResponseRecorder {
	t.Helper()
	ctx := WithUser(t.Context(), u)
	if c.stream {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
	}
	req := httptest.NewRequestWithContext(ctx, method, c.path, strings.NewReader(c.body))
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("Datastar-Request", "true")
	if strings.HasPrefix(c.body, "{") {
		req.Header.Set("Content-Type", "application/json")
	} else if c.body != "" {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	rec := httptest.NewRecorder()
	f.routes.ServeHTTP(rec, req)
	return rec
}

// walk checks one route: B and cos-probe see none of A's data and no owner route; A's own view shows A's data.
func (f isolationFixture) walk(t *testing.T, route webRoute, c isolationCase) {
	t.Helper()
	method, _, _ := strings.Cut(route.pattern, " ")
	for _, u := range []User{f.b, f.p} {
		rec := f.serve(t, u, method, c)
		body := rec.Body.String()
		for _, marker := range f.markers {
			if strings.Contains(body, marker) {
				t.Errorf("%s as %s: response contains A's %q", route.pattern, u.Name, marker)
			}
		}
		switch route.scope {
		case scopeUser:
			if c.foreign && rec.Code != http.StatusNotFound && rec.Code != http.StatusForbidden {
				t.Errorf("%s as %s on A's id: status %d, want 404 or 403", route.pattern, u.Name, rec.Code)
			}
			for _, markup := range c.ownerOnly {
				if strings.Contains(body, markup) {
					t.Errorf("%s as %s: owner-only %s is shown", route.pattern, u.Name, markup)
				}
			}
			for _, markup := range c.others {
				if !strings.Contains(body, markup) {
					t.Errorf("%s as %s: %s is missing", route.pattern, u.Name, markup)
				}
			}
		case scopeShared:
			if rec.Code != http.StatusOK {
				t.Errorf("%s as %s: status %d, want 200", route.pattern, u.Name, rec.Code)
			}
		default:
			if rec.Code != http.StatusNotFound {
				t.Errorf("%s as %s: status %d, want 404", route.pattern, u.Name, rec.Code)
			}
		}
	}
	// A's own mutating user routes (chat posts, answers, dismissals) are not replayed; owner routes are safe here.
	if method != http.MethodGet && route.scope != scopeOwner {
		return
	}
	rec := f.serve(t, f.a, method, c)
	body := rec.Body.String()
	if rec.Code == http.StatusNotFound {
		t.Errorf("%s as the first admin: 404", route.pattern)
	}
	if c.own != "" && !strings.Contains(body, c.own) {
		t.Errorf("%s as the first admin: %q is missing", route.pattern, c.own)
	}
	for _, markup := range append(c.ownerOnly, c.others...) {
		if !strings.Contains(body, markup) {
			t.Errorf("%s as the first admin: %s is missing", route.pattern, markup)
		}
	}
}

// TestEveryRouteIsolatesUsers walks every route in webRoutes() as B and cos-probe, and checks A's data after the walk.
func TestEveryRouteIsolatesUsers(t *testing.T) {
	f := newIsolationFixture(t)
	cases := f.cases()
	seen := map[string]bool{}
	for _, route := range f.web.webRoutes() {
		c, ok := cases[route.pattern]
		if !ok {
			t.Errorf("route %q has no isolation case", route.pattern)
			continue
		}
		seen[route.pattern] = true
		f.walk(t, route, c)
	}
	for pattern := range cases {
		if !seen[pattern] {
			t.Errorf("isolation case %q names no route in webRoutes()", pattern)
		}
	}
	// The requests above were refused or read-only: A's data must be exactly as seeded.
	if cur, ok := f.chat.Entry(WithUser(t.Context(), f.a), f.entry.ID); !ok || len(cur.Ask.Answers) != 0 {
		t.Errorf("A's question changed: %v", cur.Ask)
	}
	entry, err := mustUserKV(t, WithUser(t.Context(), f.a), f.inbox.kv).Get(t.Context(), f.item.ID)
	if err != nil {
		t.Fatal(err)
	}
	var item InboxItem
	if err := json.Unmarshal(entry.Value(), &item); err != nil || item.Status != "open" {
		t.Errorf("A's inbox item is %q (%v), want open", item.Status, err)
	}
	if n, err := f.push.p.Devices(WithUser(t.Context(), f.a)); err != nil || n != 2 {
		t.Errorf("A's devices = %d (%v), want 2", n, err)
	}
}

// TestRouteScopesEnforceWhoMayCall checks each scope's rule on its own, since no route is admin-only yet.
func TestRouteScopesEnforceWhoMayCall(t *testing.T) {
	w := &Web{}
	admin := User{ID: newULID(), Name: "root", Role: roleAdmin}
	adminProbe := User{ID: probeUserID, Name: "cos-probe", Role: roleAdmin}
	for _, c := range []struct {
		user  User
		scope routeScope
		want  bool
	}{
		{admin, scopeAdmin, true},
		{inboxTestUser(), scopeAdmin, false},
		{adminProbe, scopeAdmin, false},
		{inboxTestUser(), scopeUser, true},
		{inboxTestUser(), scopeShared, true},
		{inboxTestUser(), scopeOwner, false},
	} {
		if got := w.allowed(WithUser(t.Context(), c.user), c.scope); got != c.want {
			t.Errorf("%s (scope %d) allowed = %v, want %v", c.user.Name, c.scope, got, c.want)
		}
	}
}

// TestRoutesRegisterOnlyThroughTheTable fails if a route is registered outside routes(), Auth.Protect or servePWA.
func TestRoutesRegisterOnlyThroughTheTable(t *testing.T) {
	allowed := map[string]string{"web.go": "routes", "dashboard_auth.go": "Protect", "pwa.go": "servePWA"}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	registered := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			owner := ""
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner = fn.Name.Name
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || !registersRoute(call) {
					return true
				}
				registered++
				if want, ok := allowed[file]; !ok || want != owner {
					t.Errorf("%s: %s registers a route at %s", file, owner, fset.Position(call.Pos()))
				}
				return true
			})
		}
	}
	if registered == 0 {
		t.Fatal("found no route registrations to check")
	}
}

// registersRoute is true for a mux's HandleFunc, and for a Handle whose first argument is a route pattern.
func registersRoute(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) == 0 {
		return false
	}
	switch sel.Sel.Name {
	case "HandleFunc":
		return true
	case "Handle":
		_, literal := call.Args[0].(*ast.BasicLit)
		recv, isIdent := sel.X.(*ast.Ident)
		return literal || isIdent && recv.Name == "mux"
	}
	return false
}
