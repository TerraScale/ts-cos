package cos

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/crypto/bcrypt"
)

func TestNewULID(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		id := newULID()
		if len(id) != 26 || !ulidPattern.MatchString(id) {
			t.Fatalf("newULID() = %q, want 26 Crockford characters", id)
		}
		if seen[id] {
			t.Fatalf("newULID() repeated %q", id)
		}
		seen[id] = true
	}
	first := newULID()
	time.Sleep(2 * time.Millisecond)
	if second := newULID(); second <= first {
		t.Fatalf("%s from 2 ms later does not sort after %s", second, first)
	}
}

func TestUserScoping(t *testing.T) {
	ctx := context.Background()
	if _, err := UserFrom(ctx); !errors.Is(err, ErrNoUser) {
		t.Fatalf("UserFrom on a bare context = %v", err)
	}
	if _, err := UserKey(ctx, "notes"); !errors.Is(err, ErrNoUser) {
		t.Fatalf("UserKey on a bare context = %v", err)
	}
	bad := WithUser(ctx, User{ID: "tester", Name: "tester", Role: roleAdmin})
	if _, err := UserFrom(bad); !errors.Is(err, ErrNoUser) {
		t.Fatalf("UserFrom with an invalid ID = %v", err)
	}
	if _, err := UserKey(bad, "notes"); !errors.Is(err, ErrNoUser) {
		t.Fatalf("UserKey with an invalid ID = %v", err)
	}
	u := User{ID: newULID(), Name: "tester", Role: roleAdmin}
	if got, err := UserFrom(WithUser(ctx, u)); err != nil || got != u {
		t.Fatalf("UserFrom = %+v, %v; want %+v", got, err, u)
	}
	key, err := UserKey(WithUser(ctx, u), "k")
	if err != nil || key != "u."+u.ID+".k" || u.Key("k") != key {
		t.Fatalf("UserKey = %q, %v; Key = %q", key, err, u.Key("k"))
	}
}

// The first admin bootstraps once per cluster. Three nodes start at once with one admin.json and a
// pre-TS-831 login; they converge on one admin that keeps the legacy password. A deleted admin is
// never recreated by a later start.
func TestBootstrapRunsOncePerCluster(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buses, configs := authTestCluster(t, ctx)
	auths := make([]*Auth, len(buses))
	for i, bus := range buses {
		authTestAdmin(t, configs[i].Home, "tester", "first-password-1", 0600)
		js, err := bus.JetStream()
		if err != nil {
			t.Fatal(err)
		}
		auths[i] = NewAuth(configs[i].Home, js)
	}
	users := authTestOpen(t, ctx, auths[0])
	legacy, err := bcrypt.GenerateFromPassword([]byte("legacy-password-1"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	value, _ := json.Marshal(authUser{Bcrypt: string(legacy)})
	if _, err := users.Create(ctx, "user.tester", value); err != nil {
		t.Fatal(err)
	}
	for _, auth := range auths {
		go auth.Start(ctx)
	}
	for _, auth := range auths {
		authTestReady(t, auth)
	}

	if names := authTestKeys(t, users, "name.*", 1); names[0] != "name.tester" {
		t.Fatalf("login names = %v, want name.tester", names)
	}
	uids := authTestKeys(t, users, "uid.*", 1)
	admin, rec, _, found, err := auths[0].account(ctx, "tester")
	if err != nil || !found || rec.Role != roleAdmin || "uid."+admin.ID != uids[0] {
		t.Fatalf("tester = %+v, found %v, err %v; want the one admin", admin, found, err)
	}
	if marker, ok, err := firstAdmin(ctx, auths[0].nc); err != nil || !ok || marker != admin.ID {
		t.Fatalf("firstAdmin = %q, %v, %v; want %s", marker, ok, err, admin.ID)
	}
	server := authTestServer(t, auths[1])
	c := authTestHTTP(t)
	if response, _ := c.do(server, "POST", "/login", url.Values{"user": {"tester"}, "password": {"legacy-password-1"}}, nil); response.StatusCode != http.StatusSeeOther || sessionOf(response) == nil {
		t.Fatalf("legacy password login = %d", response.StatusCode)
	}
	if response, _ := c.do(server, "POST", "/login", url.Values{"user": {"tester"}, "password": {"first-password-1"}}, nil); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin.json password after migration = %d", response.StatusCode)
	}
	if _, _, found, err := leaderGet(ctx, auths[0].nc, authBucket, "user.tester"); err != nil || found {
		t.Fatalf("legacy key still present: found %v, err %v", found, err)
	}

	_, sessions, lockouts := auths[0].kv()
	if err := deleteUser(ctx, users, sessions, lockouts, admin); err != nil {
		t.Fatal(err)
	}
	js, err := buses[2].JetStream()
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewAuth(configs[2].Home, js)
	go restarted.Start(ctx)
	restartedUsers := authTestReady(t, restarted)
	if _, _, found, err := leaderGet(ctx, restarted.nc, authBucket, "name.tester"); err != nil || found {
		t.Fatalf("deleted admin's login name came back: found %v, err %v", found, err)
	}
	authTestKeys(t, restartedUsers, "uid.*", 0)
}

// TestUserKVScopesEveryOperation runs two users on one bucket: each sees only its own keys, under bare names.
func TestUserKVScopesEveryOperation(t *testing.T) {
	_, js := releaseTestBus(t)
	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "userkv_test", History: 5})
	if err != nil {
		t.Fatal(err)
	}
	a := User{ID: newULID(), Name: "alice", Role: roleMember}
	b := User{ID: newULID(), Name: "bob", Role: roleMember}
	if _, err := UserKV(ctx, kv); !errors.Is(err, ErrNoUser) {
		t.Fatalf("UserKV without a user = %v", err)
	}
	ua, err := UserKV(WithUser(ctx, a), kv)
	if err != nil {
		t.Fatal(err)
	}
	ub, err := UserKV(WithUser(ctx, b), kv)
	if err != nil {
		t.Fatal(err)
	}

	rev, err := ua.Put(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if entry, err := ua.Get(ctx, "k"); err != nil || entry.Key() != "k" || string(entry.Value()) != "v1" {
		t.Fatalf("alice Get = %v, %v; want bare key k with v1", entry, err)
	}
	if entry, err := ua.GetRevision(ctx, "k", rev); err != nil || entry.Key() != "k" {
		t.Fatalf("alice GetRevision = %v, %v", entry, err)
	}
	if _, err := ub.Get(ctx, "k"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("bob Get of alice's key = %v, want ErrKeyNotFound", err)
	}
	if raw, err := kv.Get(ctx, "u."+a.ID+".k"); err != nil || string(raw.Value()) != "v1" {
		t.Fatalf("stored key = %v, %v; want u.<alice>.k", raw, err)
	}
	if _, err := ua.Get(ctx, ""); !errors.Is(err, jetstream.ErrInvalidKey) {
		t.Fatalf("empty key = %v, want ErrInvalidKey", err)
	}
	if _, err := ub.Put(ctx, "k", []byte("bob")); err != nil {
		t.Fatal(err)
	}
	if _, err := ub.Put(ctx, "b1", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := ua.Put(ctx, "a2", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if entry, err := ua.Get(ctx, "k"); err != nil || string(entry.Value()) != "v1" {
		t.Fatalf("bob's write changed alice's key: %v, %v", entry, err)
	}

	// Listing and watching show only alice's bare keys.
	if keys, err := ua.Keys(ctx); err != nil || !slices.Equal(keys, []string{"a2", "k"}) {
		t.Fatalf("alice Keys = %v, %v; want [a2 k]", keys, err)
	}
	lister, err := ua.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for key := range lister.Keys() {
		listed = append(listed, key)
	}
	slices.Sort(listed)
	if !slices.Equal(listed, []string{"a2", "k"}) {
		t.Fatalf("alice ListKeys = %v, want [a2 k]", listed)
	}
	watcher, err := ua.WatchAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Stop()
	var initial []string
	for {
		entry := nextUserEntry(t, watcher)
		if entry == nil {
			break
		}
		initial = append(initial, entry.Key())
	}
	slices.Sort(initial)
	if !slices.Equal(initial, []string{"a2", "k"}) {
		t.Fatalf("alice WatchAll initial = %v, want [a2 k]", initial)
	}
	if _, err := ub.Put(ctx, "b3", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := ua.Put(ctx, "later", []byte("y")); err != nil {
		t.Fatal(err)
	}
	if entry := nextUserEntry(t, watcher); entry == nil || entry.Key() != "later" {
		t.Fatalf("alice's next update = %v, want bare key later (bob's write must not appear)", entry)
	}

	// Create, Update, Delete and History are scoped too.
	createRev, err := ua.Create(ctx, "c", []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ua.Create(ctx, "c", []byte("v")); !errors.Is(err, jetstream.ErrKeyExists) {
		t.Fatalf("second Create = %v, want ErrKeyExists", err)
	}
	if _, err := ua.Update(ctx, "c", []byte("v2"), createRev); err != nil {
		t.Fatal(err)
	}
	if err := ua.Delete(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	history, err := ua.History(ctx, "c")
	if err != nil || len(history) != 3 || history[0].Key() != "c" || string(history[1].Value()) != "v2" {
		t.Fatalf("History = %v, %v; want three bare entries for c", history, err)
	}
	if _, err := ua.Get(ctx, "c"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("deleted key Get = %v", err)
	}
	if err := ua.PurgeDeletes(ctx); !errors.Is(err, errPerUserView) {
		t.Fatalf("PurgeDeletes on a user view = %v, want errPerUserView", err)
	}
	if err := ua.Purge(ctx, "a2"); err != nil {
		t.Fatal(err)
	}
	if _, err := ub.Get(ctx, "b1"); err != nil {
		t.Fatalf("bob's key after alice's Purge = %v", err)
	}
}

// nextUserEntry returns the next watcher entry, failing after a few seconds; nil is the initial-values marker.
func nextUserEntry(t *testing.T, w jetstream.KeyWatcher) jetstream.KeyValueEntry {
	t.Helper()
	select {
	case entry, ok := <-w.Updates():
		if !ok {
			t.Fatal("watcher closed")
		}
		return entry
	case <-time.After(5 * time.Second):
		t.Fatal("no watcher update")
	}
	return nil
}

// TestMigrateToUserMovesLegacyKeys moves unscoped keys under a user, skips "u.*" keys, and is safe to rerun.
func TestMigrateToUserMovesLegacyKeys(t *testing.T) {
	_, js := releaseTestBus(t)
	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "migrate_test", History: 1})
	if err != nil {
		t.Fatal(err)
	}
	id, other := newULID(), newULID()
	// Writes are ordered, because the later write is the newer value the migration must respect.
	put := func(key, value string) {
		t.Helper()
		time.Sleep(5 * time.Millisecond)
		if _, err := kv.Put(ctx, key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	put("x1", "one")
	put("x2", "two")
	put("z", "OLD")
	put("u."+id+".z", "NEW")   // newer than the legacy z: kept
	put("u."+id+".r", "STALE") // older than the legacy r: overwritten
	put("u."+other+".keep", "kept")
	put("r", "REWRITTEN")
	if _, err := migrateToUser(ctx, kv, "not-a-ulid"); err == nil {
		t.Fatal("migration to a non-ULID id succeeded")
	}
	moved, err := migrateToUser(ctx, kv, id)
	if err != nil || moved != 4 {
		t.Fatalf("first run moved %d, %v; want 4 (x1, x2, z, r)", moved, err)
	}
	expect := map[string]string{"u." + id + ".x1": "one", "u." + id + ".x2": "two", "u." + id + ".z": "NEW", "u." + id + ".r": "REWRITTEN", "u." + other + ".keep": "kept"}
	for key, want := range expect {
		if entry, err := kv.Get(ctx, key); err != nil || string(entry.Value()) != want {
			t.Fatalf("%s = %v, %v; want %q", key, entry, err, want)
		}
	}
	for _, key := range []string{"x1", "x2", "z", "r"} {
		if _, err := kv.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("legacy key %s still present: %v", key, err)
		}
	}
	if moved, err := migrateToUser(ctx, kv, id); err != nil || moved != 0 {
		t.Fatalf("second run moved %d, %v; want 0", moved, err)
	}
}

// TestFirstAdminRejectsJunk never reports a bootstrap marker that is not a ULID as ok.
func TestFirstAdminRejectsJunk(t *testing.T) {
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: authBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := firstAdmin(ctx, bus.conn); err != nil || ok {
		t.Fatalf("before the bootstrap = %v, %v", ok, err)
	}
	if _, err := kv.Put(ctx, "bootstrapped", []byte("not-a-ulid")); err != nil {
		t.Fatal(err)
	}
	if id, ok, err := firstAdmin(ctx, bus.conn); err == nil || ok {
		t.Fatalf("junk marker = %q, %v, %v; want an error and not ok", id, ok, err)
	}
	want := newULID()
	if _, err := kv.Put(ctx, "bootstrapped", []byte(want)); err != nil {
		t.Fatal(err)
	}
	if id, ok, err := firstAdmin(ctx, bus.conn); err != nil || !ok || id != want {
		t.Fatalf("firstAdmin = %q, %v, %v; want %s", id, ok, err, want)
	}
}

// authTestOpen creates the auth buckets before Start, so a test can seed records first.
func authTestOpen(t *testing.T, ctx context.Context, auth *Auth) jetstream.KeyValue {
	t.Helper()
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		users, _, _, err := auth.open(ctx)
		if err == nil {
			return users
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
	}
}

// authTestReady waits until Start has opened the buckets, which it does only after the bootstrap.
func authTestReady(t *testing.T, auth *Auth) jetstream.KeyValue {
	t.Helper()
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if users, _, _ := auth.kv(); users != nil {
			return users
		}
	}
	t.Fatal("auth buckets were not created")
	return nil
}

// authTestKeys lists the keys under filter once the local replica holds want of them.
func authTestKeys(t *testing.T, users jetstream.KeyValue, filter string, want int) []string {
	t.Helper()
	var keys []string
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		lister, err := users.ListKeysFiltered(t.Context(), filter)
		if err != nil {
			t.Fatal(err)
		}
		keys = keys[:0]
		for key := range lister.Keys() {
			keys = append(keys, key)
		}
		if len(keys) == want {
			return keys
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s has %d keys, want %d", filter, len(keys), want)
		}
	}
}

// authTestServer serves auth's dashboard, whose handler echoes the signed-in login name.
func authTestServer(t *testing.T, auth *Auth) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(auth.Protect(http.HandlerFunc(authTestDashboard), http.NotFoundHandler()))
	t.Cleanup(server.Close)
	return server
}

// authTestHTTP returns redirects and cookies instead of following them, like the other auth tests.
func authTestHTTP(t *testing.T) authTestClient {
	return authTestClient{t, &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
