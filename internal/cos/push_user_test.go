package cos

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"git.terrascale.tech/TerraScale/ts-cos/internal/webpush"
	"github.com/nats-io/nats.go/jetstream"
)

// pushUserFixture is a one-replica push store whose receiver records the path of every push it accepts.
type pushUserFixture struct {
	p        *Push
	bus      *Bus
	receiver *httptest.Server
	got      chan string
}

func newPushUserFixture(t *testing.T) pushUserFixture {
	t.Helper()
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: pushBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := webpush.GenerateVAPID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, pushVAPIDKey, raw); err != nil {
		t.Fatal(err)
	}
	vapid, err := webpush.ParseVAPID(raw, pushSubject)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 16)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		got <- r.URL.Path
		rw.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(receiver.Close)
	client := pushClient()
	client.Transport = receiver.Client().Transport
	return pushUserFixture{p: &Push{kv: kv, js: js, host: "mariogk-dev", vapid: vapid, client: client}, bus: bus, receiver: receiver, got: got}
}

// subscribe stores a device for user under receiver/path; the same path is the same device.
func (f pushUserFixture) subscribe(t *testing.T, user User, path string) {
	t.Helper()
	sub, _, _, err := newPushSubscription(f.receiver.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.p.Subscribe(WithUser(t.Context(), user), "test", sub); err != nil {
		t.Fatal(err)
	}
}

// drain returns the paths that accepted a push since the last call, sorted. Send returns only after the receiver answers.
func (f pushUserFixture) drain() []string {
	var out []string
	for {
		select {
		case path := <-f.got:
			out = append(out, path)
		default:
			slices.Sort(out)
			return out
		}
	}
}

// TestPushUserSendReachesOnlyTheOwner: a user's notice reaches that user's devices, and an inbox notice without a user is refused.
func TestPushUserSendReachesOnlyTheOwner(t *testing.T) {
	f := newPushUserFixture(t)
	ctx := t.Context()
	alice, bob := inboxTestUser(), inboxTestUser()
	f.subscribe(t, alice, "/alice")
	f.subscribe(t, bob, "/bob")

	if sent, err := f.p.Send(ctx, Notice{Category: "inbox", User: alice.ID, Title: "Question", Tag: "inbox-a1"}); err != nil || sent != 1 {
		t.Fatalf("send to alice: %d, %v", sent, err)
	}
	if got := f.drain(); !slices.Equal(got, []string{"/alice"}) {
		t.Fatalf("alice's inbox push reached %v", got)
	}
	if _, err := f.p.Send(ctx, Notice{Category: "inbox", Title: "Question"}); err == nil {
		t.Fatal("inbox notice without a user was sent")
	}
	if got := f.drain(); len(got) != 0 {
		t.Fatalf("unowned inbox notice reached %v", got)
	}
}

// TestPushUserClusterCategoriesFollowEachUsersPrefs: done and node reach every user, each under that user's own prefs.
func TestPushUserClusterCategoriesFollowEachUsersPrefs(t *testing.T) {
	f := newPushUserFixture(t)
	ctx := t.Context()
	alice, bob := inboxTestUser(), inboxTestUser()
	f.subscribe(t, alice, "/alice")
	f.subscribe(t, bob, "/bob")

	if err := f.p.SetPref(WithUser(ctx, bob), "done", false); err != nil {
		t.Fatal(err)
	}
	if sent, err := f.p.Send(ctx, Notice{Category: "done", Title: "Updated", Tag: "deploy"}); err != nil || sent != 1 {
		t.Fatalf("deploy: %d, %v; want alice only, because bob muted done", sent, err)
	}
	if got := f.drain(); !slices.Equal(got, []string{"/alice"}) {
		t.Fatalf("deploy reached %v", got)
	}
	if sent, err := f.p.Send(ctx, Notice{Category: "node", Title: "Node alert", Tag: "node-delta"}); err != nil || sent != 2 {
		t.Fatalf("node alert: %d, %v; want both users", sent, err)
	}
	if got := f.drain(); !slices.Equal(got, []string{"/alice", "/bob"}) {
		t.Fatalf("node alert reached %v", got)
	}
	// bob's pref never changed alice's.
	if off, err := f.p.off(WithUser(ctx, alice)); err != nil || off["done"] {
		t.Fatalf("alice's prefs = %v, %v; want nothing off", off, err)
	}
}

// TestPushUserDeviceBelongsToLastSubscriber: a device subscribed by a second user leaves the first user's account.
func TestPushUserDeviceBelongsToLastSubscriber(t *testing.T) {
	f := newPushUserFixture(t)
	ctx := t.Context()
	alice, bob := inboxTestUser(), inboxTestUser()
	f.subscribe(t, alice, "/shared")
	sub, _, _, err := newPushSubscription(f.receiver.URL + "/shared")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.p.Subscribe(WithUser(ctx, bob), "test", sub); err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.kv.Get(ctx, alice.Key(pushSubKey(sub.Endpoint))); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("alice still holds the device: %v", err)
	}
	if _, err := f.p.kv.Get(ctx, bob.Key(pushSubKey(sub.Endpoint))); err != nil {
		t.Fatalf("bob does not hold the device: %v", err)
	}
	if sent, err := f.p.Send(ctx, Notice{Category: "inbox", User: alice.ID, Title: "Question", Tag: "inbox-a1"}); err != nil || sent != 0 {
		t.Fatalf("alice's push after the handover: %d, %v; want no device", sent, err)
	}
}

// TestPushUserTestGoesToSignedInUser: POST /push/test reaches the signed-in user's devices only.
func TestPushUserTestGoesToSignedInUser(t *testing.T) {
	f := newPushUserFixture(t)
	alice, bob := inboxTestUser(), inboxTestUser()
	f.subscribe(t, alice, "/alice")
	f.subscribe(t, bob, "/bob")
	w := &Web{}
	w.push.Store(f.p)
	mux := http.NewServeMux()
	w.pushRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/push/test", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req.WithContext(WithUser(req.Context(), bob)))
	if got := f.drain(); !slices.Equal(got, []string{"/bob"}) {
		t.Fatalf("test push reached %v; want bob only (status %d)", got, rw.Code)
	}
}

// TestPushMigratesLegacyKeysToFirstAdmin: legacy subscriptions and the admin's prefs move under the admin once;
// other legacy prefs go; the shared vapid and last.inbox keys stay.
func TestPushMigratesLegacyKeysToFirstAdmin(t *testing.T) {
	f := newPushUserFixture(t)
	ctx := t.Context()
	js := f.p.js
	auth, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: authBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	admin := inboxTestUser()
	account, _ := json.Marshal(authUser{Name: "tester", Role: roleAdmin, Created: time.Now().UTC()})
	if _, err := auth.Put(ctx, "uid."+admin.ID, account); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Put(ctx, "bootstrapped", []byte(admin.ID)); err != nil {
		t.Fatal(err)
	}
	sub, _, _, err := newPushSubscription(f.receiver.URL + "/legacy")
	if err != nil {
		t.Fatal(err)
	}
	legacy, _ := json.Marshal(map[string]any{"sub": sub, "user": "tester", "created": time.Now().UTC()})
	put := func(key string, value []byte) {
		t.Helper()
		if _, err := f.p.kv.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}
	}
	put(pushSubKey(sub.Endpoint), legacy)
	put(pushPrefPrefix+"tester", []byte(`{"off":["chat"]}`))
	put(pushPrefPrefix+"someone", []byte(`{"off":["done"]}`))
	put(pushLastKey, []byte(`{"item":"keep"}`))
	vapid, err := f.p.kv.Get(ctx, pushVAPIDKey)
	if err != nil {
		t.Fatal(err)
	}

	if moved, err := f.p.migrate(ctx, f.bus); err != nil || moved != 2 {
		t.Fatalf("first run moved %d, %v; want the subscription and the admin's prefs", moved, err)
	}
	if _, err := f.p.kv.Get(ctx, pushSubKey(sub.Endpoint)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("legacy subscription still there: %v", err)
	}
	if _, err := f.p.kv.Get(ctx, admin.Key(pushSubKey(sub.Endpoint))); err != nil {
		t.Fatalf("admin does not hold the subscription: %v", err)
	}
	if off, err := f.p.off(WithUser(ctx, admin)); err != nil || !off["chat"] || off["done"] {
		t.Fatalf("admin's prefs = %v, %v; want chat off only", off, err)
	}
	for _, key := range []string{pushPrefPrefix + "tester", pushPrefPrefix + "someone"} {
		if _, err := f.p.kv.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("legacy %s still there: %v", key, err)
		}
	}
	if entry, err := f.p.kv.Get(ctx, pushVAPIDKey); err != nil || string(entry.Value()) != string(vapid.Value()) {
		t.Fatalf("vapid changed: %v", err)
	}
	if _, err := f.p.kv.Get(ctx, pushLastKey); err != nil {
		t.Fatalf("last.inbox moved: %v", err)
	}
	if moved, err := f.p.migrate(ctx, f.bus); err != nil || moved != 0 {
		t.Fatalf("second run moved %d, %v; want 0", moved, err)
	}
}

// TestPushChatReplyReachesItsOwner: a COS reply is pushed to the user whose chat holds it, and never without an owner.
func TestPushChatReplyReachesItsOwner(t *testing.T) {
	f := newPushUserFixture(t)
	ctx := t.Context()
	alice, bob := inboxTestUser(), inboxTestUser()
	f.subscribe(t, alice, "/alice")
	f.subscribe(t, bob, "/bob")
	n, ok := chatReplyNotice(ChatEntry{ID: "00000000000000a1-00000000", Role: "cos", Text: "Done.", owner: alice.ID})
	if !ok {
		t.Fatal("a reply with an owner was not pushed")
	}
	if sent, err := f.p.Send(ctx, n); err != nil || sent != 1 {
		t.Fatalf("reply: %d, %v; want alice's one device", sent, err)
	}
	if got := f.drain(); !slices.Equal(got, []string{"/alice"}) {
		t.Fatalf("reply reached %v", got)
	}
	if _, ok := chatReplyNotice(ChatEntry{ID: "00000000000000a2-00000000", Role: "cos", Text: "orphan"}); ok {
		t.Fatal("a reply without an owner was pushed")
	}
}
