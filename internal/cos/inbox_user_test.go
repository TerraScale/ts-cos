package cos

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// inboxTestOpen opens the Inbox over a one-replica bucket, since the test server is not clustered.
func inboxTestOpen(t *testing.T, bus *Bus, js jetstream.JetStream) *Inbox {
	t.Helper()
	if _, err := js.CreateOrUpdateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: inboxBucket, History: 1}); err != nil {
		t.Fatal(err)
	}
	in, err := OpenInbox(t.Context(), bus, "mariogk-dev", false)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

// TestInboxUserIsolation: two users share one bucket, and B never sees, answers or dismisses A's item.
func TestInboxUserIsolation(t *testing.T) {
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	in := inboxTestOpen(t, bus, js)
	store := openTestStore(t)
	alice, bob := inboxTestUser(), inboxTestUser()
	aliceCtx, bobCtx := WithUser(ctx, alice), WithUser(ctx, bob)
	item, err := in.Add(aliceCtx, InboxInput{Title: "Alice's question", Options: []string{"Yes", "No"}})
	if err != nil {
		t.Fatal(err)
	}
	if item.Owner != alice.ID {
		t.Fatalf("owner = %q, want %q", item.Owner, alice.ID)
	}

	if _, err := in.Answer(bobCtx, item.ID, AnswerData{Option: "Yes"}); !errors.Is(err, errInboxNotFound) {
		t.Fatalf("B answered A's item: %v", err)
	}
	if _, err := in.Dismiss(bobCtx, item.ID); !errors.Is(err, errInboxNotFound) {
		t.Fatalf("B dismissed A's item: %v", err)
	}
	// Still open for A: dismissing it now is the first close.
	dismissed, err := in.Dismiss(aliceCtx, item.ID)
	if err != nil || dismissed.Status != "dismissed" {
		t.Fatalf("A's item changed under B: %+v, %v", dismissed, err)
	}

	// Each user's view of the mirror lists only their own rows.
	if err := store.SaveInboxItem(ctx, dismissed); err != nil {
		t.Fatal(err)
	}
	if open, history, err := store.Inbox(bobCtx); err != nil || len(open)+len(history) != 0 {
		t.Fatalf("B's Inbox = %v %v, %v; want nothing", inboxIDs(open), inboxIDs(history), err)
	}
	if _, _, err := store.Inbox(ctx); !errors.Is(err, ErrNoUser) {
		t.Fatalf("Inbox without a user = %v, want ErrNoUser", err)
	}
	if open, history, err := store.Inbox(aliceCtx); err != nil || len(open) != 0 || !slices.Equal(inboxIDs(history), []string{item.ID}) {
		t.Fatalf("A's Inbox = %v %v, %v", inboxIDs(open), inboxIDs(history), err)
	}
	bobView, err := UserKV(bobCtx, in.kv)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bobView.Keys(ctx); !errors.Is(err, jetstream.ErrNoKeysFound) {
		t.Fatalf("B's bucket keys = %v; want none", err)
	}
}

// TestInboxWebActionIs404ForAnotherUser: a POST for an item the caller does not own changes nothing.
func TestInboxWebActionIs404ForAnotherUser(t *testing.T) {
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	in := inboxTestOpen(t, bus, js)
	alice, bob := inboxTestUser(), inboxTestUser()
	item, err := in.Add(WithUser(ctx, alice), InboxInput{Title: "Alice's fyi", Kind: "fyi"})
	if err != nil {
		t.Fatal(err)
	}
	web := &Web{}
	web.inbox.Store(in)
	post := func(user User) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/inbox/"+item.ID+"/dismiss", nil)
		req.SetPathValue("id", item.ID)
		req.SetPathValue("action", "dismiss")
		rw := httptest.NewRecorder()
		web.InboxAction(rw, req.WithContext(WithUser(req.Context(), user)))
		return rw
	}
	if rw := post(bob); rw.Code != http.StatusNotFound {
		t.Fatalf("B's dismiss status = %d, want 404", rw.Code)
	}
	if rw := post(alice); rw.Code != http.StatusSeeOther {
		t.Fatalf("A's dismiss status = %d, want a redirect", rw.Code)
	}
}

// TestInboxEventsCarryOwner: an item's events name the user whose Inbox they are about.
func TestInboxEventsCarryOwner(t *testing.T) {
	owner := inboxTestUser()
	item, err := InboxInput{Title: "Owned", Options: []string{"Yes", "No"}}.item(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	item.Owner = owner.ID
	if e := itemEvent(item); e.User != owner.ID {
		t.Fatalf("open event user = %q, want %q", e.User, owner.ID)
	}
	item.Status, item.Answer, item.Closed = "answered", &AnswerData{Option: "Yes"}, time.Now()
	if e := itemEvent(item); e.User != owner.ID {
		t.Fatalf("answer event user = %q, want %q", e.User, owner.ID)
	}
}

// TestInboxMigratesToFirstAdmin: pre-TS-831 items wait for the bootstrap, then move under the first admin once.
func TestInboxMigratesToFirstAdmin(t *testing.T) {
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	in := inboxTestOpen(t, bus, js)
	legacy := InboxItem{ID: "0123456789abcdef", Kind: "question", Title: "legacy", Status: "open", Created: time.Now().UTC()}
	data, _ := json.Marshal(legacy)
	if _, err := in.kv.Put(ctx, legacy.ID, data); err != nil {
		t.Fatal(err)
	}
	if moved, err := in.migrate(ctx); err == nil || moved != 0 {
		t.Fatalf("before the bootstrap: moved %d, %v; want an error and nothing moved", moved, err)
	}
	auth, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: authBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	admin := inboxTestUser()
	if _, err := auth.Put(ctx, "bootstrapped", []byte(admin.ID)); err != nil {
		t.Fatal(err)
	}
	if moved, err := in.migrate(ctx); err != nil || moved != 1 {
		t.Fatalf("first run moved %d, %v; want 1", moved, err)
	}
	if _, err := in.kv.Get(ctx, legacy.ID); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("legacy key still there: %v", err)
	}
	adminView, err := UserKV(WithUser(ctx, admin), in.kv)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminView.Get(ctx, legacy.ID); err != nil {
		t.Fatalf("admin does not have the item: %v", err)
	}
	if moved, err := in.migrate(ctx); err != nil || moved != 0 {
		t.Fatalf("second run moved %d, %v; want 0", moved, err)
	}
}
