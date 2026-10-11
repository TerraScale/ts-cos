package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// adminTestAuth opens the auth buckets on the single test node. Auth.Start asks for three replicas, so this
// mirrors its open and seed steps with one replica.
func adminTestAuth(t *testing.T, js jetstream.JetStream) *Auth {
	t.Helper()
	ctx := t.Context()
	home := t.TempDir()
	authTestAdmin(t, home, "owner", "owner-password-1", 0600)
	auth := NewAuth(home, js)
	users, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: authBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: sessionBucket, TTL: sessionTTL, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	lockouts, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: lockoutBucket, TTL: lockFor, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	auth.seed(ctx, users)
	auth.mu.Lock()
	auth.users, auth.sessions, auth.lockouts = users, sessions, lockouts
	auth.mu.Unlock()
	return auth
}

// adminTestServer serves the admin pages and the invite flow behind the real sign-in, on one node.
func adminTestServer(t *testing.T, threads threadCloser) (*httptest.Server, *Auth, jetstream.JetStream) {
	t.Helper()
	_, js := releaseTestBus(t)
	auth := adminTestAuth(t, js)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", authTestDashboard)
	for _, route := range adminRoutes(adminPages{auth: auth, threads: threads}) {
		mux.HandleFunc(route.pattern, route.handler)
	}
	server := httptest.NewServer(auth.Protect(mux, http.NotFoundHandler()))
	t.Cleanup(server.Close)
	return server, auth, js
}

// adminTestSignIn signs in and returns the session cookie.
func adminTestSignIn(t *testing.T, server *httptest.Server, user, password string) *http.Cookie {
	t.Helper()
	response, _ := authTestHTTP(t).do(server, "POST", "/login", url.Values{"user": {user}, "password": {password}}, nil)
	cookie := sessionOf(response)
	if response.StatusCode != http.StatusSeeOther || cookie == nil {
		t.Fatalf("sign in as %s = %d", user, response.StatusCode)
	}
	return cookie
}

var inviteLinkPattern = regexp.MustCompile(`/invite/([A-Za-z0-9_-]{43})`)

// inviteTokenIn returns the one-time token from the admin page that issued it.
func inviteTokenIn(t *testing.T, body string) string {
	t.Helper()
	match := inviteLinkPattern.FindStringSubmatch(body)
	if match == nil {
		t.Fatal("no invite link on the page")
	}
	return match[1]
}

// syncBuffer collects the log output of the server's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// adminTestLogs sends the default logger to logs for the rest of the test.
func adminTestLogs(t *testing.T, logs *syncBuffer) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// adminTestData counts the keys under prefix in every per-user bucket, delete markers and old revisions included.
func adminTestData(t *testing.T, js jetstream.JetStream, prefix string) int {
	t.Helper()
	n := 0
	for _, bucket := range userBuckets {
		kv, err := js.KeyValue(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}
		w, err := kv.WatchFiltered(t.Context(), []string{prefix + ">"}, jetstream.MetaOnly())
		if err != nil {
			t.Fatal(err)
		}
		for entry := nextUserEntry(t, w); entry != nil; entry = nextUserEntry(t, w) {
			n++
		}
		w.Stop()
	}
	return n
}

// errAdminTestBus is the failure a fake bus returns when the host cannot be reached.
var errAdminTestBus = errors.New("host unreachable")

// adminTestThreads stands in for the bus. It records each close and how much of the user's data existed then.
// The server is built before the seed, so js and prefix are set once the test has them.
type adminTestThreads struct {
	t      *testing.T
	js     jetstream.JetStream
	prefix string
	result TreeResult
	err    error
	calls  []string
	seen   int
}

func (f *adminTestThreads) Call(_ context.Context, host, command string, args []string, output any) error {
	f.calls = append(f.calls, host+" "+command+" "+strings.Join(args, ","))
	f.seen = adminTestData(f.t, f.js, f.prefix)
	if out, ok := output.(*TreeResult); ok {
		*out = f.result
	}
	return f.err
}

// userNamed is the account with login name name, which the test expects to exist.
func userNamed(t *testing.T, auth *Auth, name string) User {
	t.Helper()
	u, _, _, found, err := auth.account(t.Context(), name)
	if err != nil || !found {
		t.Fatalf("account %s: found %v, err %v", name, found, err)
	}
	return u
}

// adminTestDeleteSeed makes members b and a, each with data in every per-user bucket. b also has a COS thread open.
func adminTestDeleteSeed(t *testing.T, server *httptest.Server, auth *Auth, js jetstream.JetStream) (User, User, *http.Cookie) {
	t.Helper()
	ctx := t.Context()
	users, _, _ := auth.kv()
	b := authTestUser(t, users, "e2e-b", "member-password-1")
	a := authTestUser(t, users, "e2e-a", "member-password-1")
	adminTestBuckets(t, js)
	for _, bucket := range userBuckets {
		kv, err := js.KeyValue(ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range []User{a, b} {
			for _, key := range []string{"k1", "k1", "k2"} {
				if _, err := kv.Put(ctx, u.Key(key), []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err := kv.Put(ctx, b.Key("gone"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := kv.Delete(ctx, b.Key("gone")); err != nil {
			t.Fatal(err)
		}
	}
	state, _ := json.Marshal(AgentState{Host: "delta", ThreadID: "thread-b"})
	agent, err := js.KeyValue(ctx, agentBucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Put(ctx, b.Key("state"), state); err != nil {
		t.Fatal(err)
	}
	return b, a, adminTestSignIn(t, server, "e2e-b", "member-password-1")
}

// TestAdminRoutesHideFromMembers answers 404 on every admin route to a member and to the cos-probe user.
func TestAdminRoutesHideFromMembers(t *testing.T) {
	server, auth, _ := adminTestServer(t, nil)
	users, _, _ := auth.kv()
	member := authTestUser(t, users, "e2e-m", "member-password-1")
	memberCookie := adminTestSignIn(t, server, "e2e-m", "member-password-1")
	probe, end, err := auth.ProbeSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer end()
	probeCookie := &http.Cookie{Name: sessionCookie, Value: probe}
	c := authTestHTTP(t)
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	if response, _ := c.do(server, "GET", "/admin", nil, admin); response.StatusCode != http.StatusOK {
		t.Fatalf("owner GET /admin = %d", response.StatusCode)
	}
	owner := userNamed(t, auth, "owner")
	routes := []struct {
		method, path string
		form         url.Values
	}{
		{"GET", "/admin", nil},
		{"POST", "/admin/invite", url.Values{"name": {"e2e-x"}}},
		{"POST", "/admin/users/" + member.ID + "/invite", nil},
		{"POST", "/admin/users/" + member.ID + "/disable", nil},
		{"POST", "/admin/users/" + member.ID + "/enable", nil},
		{"GET", "/admin/users/" + member.ID + "/delete", nil},
		{"POST", "/admin/users/" + member.ID + "/delete", url.Values{"confirm": {"e2e-m"}}},
		{"POST", "/admin/users/" + owner.ID + "/disable", nil},
	}
	for _, cookie := range []*http.Cookie{memberCookie, probeCookie} {
		for _, route := range routes {
			if response, _ := c.do(server, route.method, route.path, route.form, cookie); response.StatusCode != http.StatusNotFound {
				t.Errorf("%s %s = %d, want 404", route.method, route.path, response.StatusCode)
			}
		}
	}
	if _, _, found, _ := auth.accountByID(t.Context(), owner.ID); !found {
		t.Fatal("a member's request removed the owner")
	}
	if rec, _, _, _ := auth.accountByID(t.Context(), member.ID); !rec.DisabledAt.IsZero() {
		t.Fatal("a member's request disabled the member's own account")
	}
}

// TestAdminInviteFlow runs the invite end to end: the one-time link, the password rules, the sign-in, the reuse
// refusal, and a check that the token never reaches the logs.
func TestAdminInviteFlow(t *testing.T) {
	var logs syncBuffer
	adminTestLogs(t, &logs)
	server, _, _ := adminTestServer(t, nil)
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	c := authTestHTTP(t)
	response, body := c.do(server, "POST", "/admin/invite", url.Values{"name": {"e2e-b"}}, admin)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("invite = %d", response.StatusCode)
	}
	token := inviteTokenIn(t, body)
	link := "/invite/" + token
	if response, _ := c.do(server, "GET", link, nil, nil); response.StatusCode != http.StatusOK {
		t.Fatalf("link page = %d", response.StatusCode)
	}
	short := url.Values{"new": {"short-pass1"}, "confirm": {"short-pass1"}}
	if response, _ := c.do(server, "POST", link, short, nil); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("short password = %d, want 400", response.StatusCode)
	}
	set := url.Values{"new": {"e2e-password-1"}, "confirm": {"e2e-password-1"}}
	response, _ = c.do(server, "POST", link, set, nil)
	member := sessionOf(response)
	if response.StatusCode != http.StatusSeeOther || member == nil {
		t.Fatalf("set password = %d", response.StatusCode)
	}
	if _, body := c.do(server, "GET", "/", nil, member); body != "dashboard for e2e-b" {
		t.Fatalf("invited session = %q", body)
	}
	adminTestSignIn(t, server, "e2e-b", "e2e-password-1")
	if response, _ := c.do(server, "GET", link, nil, nil); response.StatusCode != http.StatusNotFound {
		t.Fatalf("used link = %d, want 404", response.StatusCode)
	}
	if response, _ := c.do(server, "POST", link, set, nil); response.StatusCode != http.StatusNotFound {
		t.Fatalf("used link POST = %d, want 404", response.StatusCode)
	}
	if _, body := c.do(server, "GET", "/admin", nil, admin); strings.Contains(body, "$2a$") {
		t.Fatal("the admin page shows a bcrypt hash")
	}
	if strings.Contains(logs.String(), token) {
		t.Fatal("the invite token reached the logs")
	}
}

// TestInviteExpires refuses a link past its expiry, with the same page as an unknown link.
func TestInviteExpires(t *testing.T) {
	server, auth, _ := adminTestServer(t, nil)
	users, _, _ := auth.kv()
	pending, err := createUser(t.Context(), users, "e2e-c", roleMember, "")
	if err != nil {
		t.Fatal(err)
	}
	token := newInviteToken()
	value, _ := json.Marshal(inviteRecord{UserID: pending.ID, Expires: time.Now().Add(-time.Minute)})
	if _, err := users.Put(t.Context(), inviteKey(token), value); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Put(t.Context(), "invitefor."+pending.ID, []byte(tokenHash(token))); err != nil {
		t.Fatal(err)
	}
	c := authTestHTTP(t)
	if response, _ := c.do(server, "GET", "/invite/"+token, nil, nil); response.StatusCode != http.StatusNotFound {
		t.Fatalf("expired link = %d, want 404", response.StatusCode)
	}
	set := url.Values{"new": {"e2e-password-1"}, "confirm": {"e2e-password-1"}}
	if response, _ := c.do(server, "POST", "/invite/"+token, set, nil); response.StatusCode != http.StatusNotFound {
		t.Fatalf("expired link POST = %d, want 404", response.StatusCode)
	}
}

// TestInviteReissueStopsOldLink makes a new link for a pending account; the old link stops working at once.
func TestInviteReissueStopsOldLink(t *testing.T) {
	server, auth, _ := adminTestServer(t, nil)
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	c := authTestHTTP(t)
	_, body := c.do(server, "POST", "/admin/invite", url.Values{"name": {"e2e-d"}}, admin)
	first := inviteTokenIn(t, body)
	id := userNamed(t, auth, "e2e-d").ID
	response, body := c.do(server, "POST", "/admin/users/"+id+"/invite", nil, admin)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("re-invite = %d", response.StatusCode)
	}
	second := inviteTokenIn(t, body)
	if response, _ := c.do(server, "GET", "/invite/"+first, nil, nil); response.StatusCode != http.StatusNotFound {
		t.Fatalf("old link = %d, want 404", response.StatusCode)
	}
	if response, _ := c.do(server, "GET", "/invite/"+second, nil, nil); response.StatusCode != http.StatusOK {
		t.Fatalf("new link = %d, want 200", response.StatusCode)
	}
}

// TestInviteLockout stops guessing: after lockAfter failed lookups from one address, even a live link is refused.
func TestInviteLockout(t *testing.T) {
	server, _, _ := adminTestServer(t, nil)
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	c := authTestHTTP(t)
	_, body := c.do(server, "POST", "/admin/invite", url.Values{"name": {"e2e-f"}}, admin)
	live := inviteTokenIn(t, body)
	for range lockAfter {
		if response, _ := c.do(server, "GET", "/invite/"+newInviteToken(), nil, nil); response.StatusCode != http.StatusNotFound {
			t.Fatalf("guess = %d, want 404", response.StatusCode)
		}
	}
	if response, _ := c.do(server, "GET", "/invite/"+live, nil, nil); response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("live link after guessing = %d, want 429", response.StatusCode)
	}
}

// TestAdminDisableRevokesSessions signs a member out at once and blocks sign-in; enable restores it.
func TestAdminDisableRevokesSessions(t *testing.T) {
	server, auth, _ := adminTestServer(t, nil)
	users, _, _ := auth.kv()
	member := authTestUser(t, users, "e2e-d", "member-password-1")
	memberCookie := adminTestSignIn(t, server, "e2e-d", "member-password-1")
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	c := authTestHTTP(t)
	if response, _ := c.do(server, "POST", "/admin/users/"+member.ID+"/disable", nil, admin); response.StatusCode != http.StatusSeeOther {
		t.Fatalf("disable = %d", response.StatusCode)
	}
	if response, _ := c.do(server, "GET", "/", nil, memberCookie); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("disabled member's session = %d, want 401", response.StatusCode)
	}
	signIn := url.Values{"user": {"e2e-d"}, "password": {"member-password-1"}}
	if response, _ := c.do(server, "POST", "/login", signIn, nil); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("disabled sign-in = %d, want 401", response.StatusCode)
	}
	if response, _ := c.do(server, "POST", "/admin/users/"+member.ID+"/enable", nil, admin); response.StatusCode != http.StatusSeeOther {
		t.Fatalf("enable = %d", response.StatusCode)
	}
	adminTestSignIn(t, server, "e2e-d", "member-password-1")
}

// TestAdminGuards refuses the signed-in admin's own account and the last active admin.
func TestAdminGuards(t *testing.T) {
	server, auth, _ := adminTestServer(t, nil)
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	owner := userNamed(t, auth, "owner")
	c := authTestHTTP(t)
	if response, _ := c.do(server, "POST", "/admin/users/"+owner.ID+"/disable", nil, admin); response.StatusCode != http.StatusConflict {
		t.Fatalf("self-disable = %d, want 409", response.StatusCode)
	}
	if response, _ := c.do(server, "POST", "/admin/users/"+owner.ID+"/delete", url.Values{"confirm": {"owner"}}, admin); response.StatusCode != http.StatusConflict {
		t.Fatalf("self-delete = %d, want 409", response.StatusCode)
	}
	other := User{ID: newULID(), Name: "other", Role: roleAdmin}
	only := []adminRow{{User: owner, Created: time.Now()}}
	if refusal := guardRemoval(other, owner, only); refusal == "" {
		t.Fatal("the last active admin was allowed to go")
	}
	both := append(slices.Clone(only), adminRow{User: other, Created: time.Now()})
	if refusal := guardRemoval(other, owner, both); refusal != "" {
		t.Fatalf("an admin with another active admin was refused: %s", refusal)
	}
}

// TestAdminDeleteNeedsTypedName changes nothing unless the typed login name matches, and lists what would go.
func TestAdminDeleteNeedsTypedName(t *testing.T) {
	threads := &adminTestThreads{t: t}
	server, auth, js := adminTestServer(t, threads)
	threads.js = js
	b, _, _ := adminTestDeleteSeed(t, server, auth, js)
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	c := authTestHTTP(t)
	before := adminTestData(t, js, b.Key(""))
	response, body := c.do(server, "GET", "/admin/users/"+b.ID+"/delete", nil, admin)
	if response.StatusCode != http.StatusOK || !strings.Contains(body, "COS_CHAT: 2 keys") {
		t.Fatalf("confirmation page = %d, want the live keys listed", response.StatusCode)
	}
	if response, _ := c.do(server, "POST", "/admin/users/"+b.ID+"/delete", url.Values{"confirm": {"e2e-x"}}, admin); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("wrong name = %d, want 400", response.StatusCode)
	}
	if after := adminTestData(t, js, b.Key("")); after != before {
		t.Fatalf("data changed from %d to %d keys", before, after)
	}
	if rec, _, found, _ := auth.accountByID(t.Context(), b.ID); !found || !rec.DisabledAt.IsZero() {
		t.Fatal("the account changed without the typed name")
	}
	if len(threads.calls) != 0 {
		t.Fatalf("a thread was closed without the typed name: %v", threads.calls)
	}
}

// TestAdminDeleteKeepsDataWhileThreadOpen stops before any data goes when the brain's thread will not close.
func TestAdminDeleteKeepsDataWhileThreadOpen(t *testing.T) {
	threads := &adminTestThreads{t: t}
	server, auth, js := adminTestServer(t, threads)
	threads.js = js
	b, a, _ := adminTestDeleteSeed(t, server, auth, js)
	threads.prefix = b.Key("")
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	before, otherBefore := adminTestData(t, js, b.Key("")), adminTestData(t, js, a.Key(""))
	c := authTestHTTP(t)
	for _, outcome := range []struct {
		name   string
		result TreeResult
		err    error
	}{
		{"bus error", TreeResult{}, errAdminTestBus},
		{"thread still open", TreeResult{Skipped: []map[string]string{{"id": "thread-b", "reason": "active run"}}}, nil},
	} {
		threads.result, threads.err, threads.calls = outcome.result, outcome.err, nil
		if response, _ := c.do(server, "POST", "/admin/users/"+b.ID+"/delete", url.Values{"confirm": {"e2e-b"}}, admin); response.StatusCode != http.StatusConflict {
			t.Fatalf("%s: delete = %d, want 409", outcome.name, response.StatusCode)
		}
		if !slices.Equal(threads.calls, []string{"delta close thread-b"}) || threads.seen != before {
			t.Fatalf("%s: close calls %v with %d keys seen, want one close with %d keys", outcome.name, threads.calls, threads.seen, before)
		}
		if after := adminTestData(t, js, b.Key("")); after != before {
			t.Fatalf("%s: data changed from %d to %d keys", outcome.name, before, after)
		}
	}
	if adminTestData(t, js, a.Key("")) != otherBefore {
		t.Fatal("the other user's data changed")
	}
	if _, _, found, _ := auth.accountByID(t.Context(), b.ID); !found {
		t.Fatal("the account was deleted while its thread was open")
	}
}

// TestAdminDeleteRemovesUserData removes one user's data, login, sessions and invite, and leaves the other user's data.
func TestAdminDeleteRemovesUserData(t *testing.T) {
	threads := &adminTestThreads{t: t}
	server, auth, js := adminTestServer(t, threads)
	threads.js = js
	b, a, bCookie := adminTestDeleteSeed(t, server, auth, js)
	threads.prefix = b.Key("")
	otherBefore := adminTestData(t, js, a.Key(""))
	admin := adminTestSignIn(t, server, "owner", "owner-password-1")
	c := authTestHTTP(t)
	_, body := c.do(server, "POST", "/admin/invite", url.Values{"name": {"e2e-c"}}, admin)
	token := inviteTokenIn(t, body)
	pending := userNamed(t, auth, "e2e-c")
	if response, _ := c.do(server, "POST", "/admin/users/"+pending.ID+"/delete", url.Values{"confirm": {"e2e-c"}}, admin); response.StatusCode != http.StatusSeeOther {
		t.Fatalf("delete pending = %d", response.StatusCode)
	}
	if response, _ := c.do(server, "GET", "/invite/"+token, nil, nil); response.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted user's invite = %d, want 404", response.StatusCode)
	}
	if response, _ := c.do(server, "POST", "/admin/users/"+b.ID+"/delete", url.Values{"confirm": {"e2e-b"}}, admin); response.StatusCode != http.StatusSeeOther {
		t.Fatalf("delete = %d, want 303", response.StatusCode)
	}
	if !slices.Equal(threads.calls, []string{"delta close thread-b"}) || threads.seen == 0 {
		t.Fatalf("close calls %v with %d keys seen; want the thread closed while its data existed", threads.calls, threads.seen)
	}
	if n := adminTestData(t, js, b.Key("")); n != 0 {
		t.Fatalf("%d keys of the deleted user remain", n)
	}
	if n := adminTestData(t, js, a.Key("")); n != otherBefore {
		t.Fatalf("the other user's data = %d keys, want %d", n, otherBefore)
	}
	if _, _, found, _ := auth.accountByID(t.Context(), b.ID); found {
		t.Fatal("the deleted account still exists")
	}
	if _, _, _, found, _ := auth.account(t.Context(), "e2e-b"); found {
		t.Fatal("the deleted login name still exists")
	}
	if response, _ := c.do(server, "GET", "/", nil, bCookie); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("deleted user's session = %d, want 401", response.StatusCode)
	}
}

// adminTestBuckets creates every per-user bucket on the test node, with history so deletes leave old revisions.
func adminTestBuckets(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	for _, bucket := range userBuckets {
		if _, err := js.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: bucket, History: 3}); err != nil {
			t.Fatal(err)
		}
	}
}

// adminTestFill writes one key for each user into every per-user bucket.
func adminTestFill(t *testing.T, js jetstream.JetStream, users ...User) {
	t.Helper()
	for _, bucket := range userBuckets {
		kv, err := js.KeyValue(t.Context(), bucket)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range users {
			if _, err := kv.Put(t.Context(), u.Key("k1"), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestAdminDeletePurgesChatMirror checks the purge reaches a node's Chat mirror, which must drop the entry from memory.
func TestAdminDeletePurgesChatMirror(t *testing.T) {
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	chat := NewChat()
	chat.recheck = 100 * time.Millisecond
	go chat.Run(ctx, bus)
	busTestReady(t, "chat ready", chat.Ready)
	kv, err := js.KeyValue(ctx, chatBucket)
	if err != nil {
		t.Fatal(err)
	}
	b := User{ID: newULID(), Name: "e2e-b", Role: roleMember}
	id := "0000000000000001-deadbeef"
	value, _ := json.Marshal(ChatEntry{Role: "user", Text: "private"})
	if _, err := kv.Put(ctx, b.Key(id), value); err != nil {
		t.Fatal(err)
	}
	bctx := WithUser(ctx, b)
	busTestReady(t, "entry on the mirror", func() bool { _, ok := chat.Entry(bctx, id); return ok })
	if err := purgeUserData(ctx, js, b.ID); err != nil {
		t.Fatal(err)
	}
	busTestReady(t, "entry dropped from the mirror", func() bool { _, ok := chat.Entry(bctx, id); return !ok })
	if _, err := kv.Get(ctx, b.Key(id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("bucket still holds the entry: %v", err)
	}
}

// TestSweepDeletedUsersPurgesOnlyOrphans purges the data of an owner with no account. A live account and the
// cos-probe keep theirs.
func TestSweepDeletedUsersPurgesOnlyOrphans(t *testing.T) {
	_, auth, js := adminTestServer(t, nil)
	users, _, _ := auth.kv()
	live := authTestUser(t, users, "e2e-live", "member-password-1")
	orphan := User{ID: newULID(), Name: "gone", Role: roleMember}
	probe := User{ID: probeUserID, Name: "cos-probe", Role: roleMember}
	adminTestBuckets(t, js)
	adminTestFill(t, js, live, orphan, probe)
	purged, err := sweepDeletedUsers(t.Context(), js, auth.nc)
	if err != nil || purged != 1 {
		t.Fatalf("sweep = %d, %v; want 1 owner purged", purged, err)
	}
	if n := adminTestData(t, js, orphan.Key("")); n != 0 {
		t.Fatalf("%d keys of the orphan remain", n)
	}
	if n := adminTestData(t, js, live.Key("")); n != len(userBuckets) {
		t.Fatalf("the live account's data = %d keys, want %d", n, len(userBuckets))
	}
	if n := adminTestData(t, js, probe.Key("")); n != len(userBuckets) {
		t.Fatalf("the probe's data = %d keys, want %d", n, len(userBuckets))
	}
}

// TestSweepDeletedUsersKeepsDataOnReadError deletes nothing while the account read fails: an error is not a
// missing account.
func TestSweepDeletedUsersKeepsDataOnReadError(t *testing.T) {
	_, auth, js := adminTestServer(t, nil)
	orphan := User{ID: newULID(), Name: "gone", Role: roleMember}
	adminTestBuckets(t, js)
	adminTestFill(t, js, orphan)
	nc, err := nats.Connect(auth.nc.ConnectedUrl())
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
	purged, err := sweepDeletedUsers(t.Context(), js, nc)
	if err == nil || purged != 0 {
		t.Fatalf("sweep with a broken account read = %d, %v; want an error and nothing purged", purged, err)
	}
	if n := adminTestData(t, js, orphan.Key("")); n != len(userBuckets) {
		t.Fatalf("the orphan's data = %d keys, want %d", n, len(userBuckets))
	}
}
