package cos

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// brainTestAuth creates the auth bucket the bootstrap would, with an admin and a member.
func brainTestAuth(t *testing.T, js jetstream.JetStream) (jetstream.KeyValue, User, User) {
	t.Helper()
	ctx := t.Context()
	users, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: authBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := createUser(ctx, users, "admin", roleAdmin, "not-a-real-hash")
	if err != nil {
		t.Fatal(err)
	}
	member, err := createUser(ctx, users, "member", roleMember, "not-a-real-hash")
	if err != nil {
		t.Fatal(err)
	}
	return users, admin, member
}

// brainTestDisable marks u's account disabled, as an admin does.
func brainTestDisable(t *testing.T, users jetstream.KeyValue, u User) {
	t.Helper()
	value, err := json.Marshal(authUser{Name: u.Name, Role: u.Role, DisabledAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := users.Put(t.Context(), "uid."+u.ID, value); err != nil {
		t.Fatal(err)
	}
}

func TestBrainUsersFirstAdminOnly(t *testing.T) {
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	users, admin, _ := brainTestAuth(t, js)
	if got, err := brainUsers(ctx, bus.conn, users); err != nil || len(got) != 0 {
		t.Fatal("before the bootstrap:", got, err)
	}
	if _, err := users.Put(ctx, "bootstrapped", []byte(admin.ID)); err != nil {
		t.Fatal(err)
	}
	if got, err := brainUsers(ctx, bus.conn, users); err != nil || len(got) != 1 || got[0].ID != admin.ID {
		t.Fatal("first admin only:", got, err)
	}
	brainTestDisable(t, users, admin)
	if got, err := brainUsers(ctx, bus.conn, users); err != nil || len(got) != 0 {
		t.Fatal("disabled first admin:", got, err)
	}
}

func TestMigrateLegacyToFirstAdmin(t *testing.T) {
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	users, admin, _ := brainTestAuth(t, js)
	if _, err := users.Put(ctx, "bootstrapped", []byte(admin.ID)); err != nil {
		t.Fatal(err)
	}
	legacy := map[string]map[string]string{
		chatBucket:    {"0000000000000001-a": `{"role":"user","text":"hi"}`},
		agentBucket:   {"state": `{"host":"delta"}`},
		contextBucket: {"focus": "Ship the chat"},
		memoryBucket:  {"agent-mem-1": `{"kind":"fact"}`},
	}
	kvs := map[string]jetstream.KeyValue{}
	for bucket, keys := range legacy {
		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, History: 1})
		if err != nil {
			t.Fatal(err)
		}
		kvs[bucket] = kv
		for key, value := range keys {
			if _, err := kv.Put(ctx, key, []byte(value)); err != nil {
				t.Fatal(err)
			}
		}
	}
	buckets := brainBuckets{auth: users, chat: kvs[chatBucket], agent: kvs[agentBucket], context: kvs[contextBucket], memory: kvs[memoryBucket]}
	buckets.migrateLegacy(ctx, bus.conn)
	for bucket, keys := range legacy {
		for key, value := range keys {
			if _, err := kvs[bucket].Get(ctx, key); err == nil {
				t.Fatalf("%s/%s still unprefixed", bucket, key)
			}
			entry, err := kvs[bucket].Get(ctx, admin.Key(key))
			if err != nil || string(entry.Value()) != value {
				t.Fatalf("%s/%s moved to %q, %v; want %q", bucket, key, entry.Value(), err, value)
			}
		}
	}
	restart, err := buckets.agent.Get(ctx, admin.Key("restart"))
	if err != nil || string(restart.Value()) != brainMovedRestart {
		t.Fatal("restart after the workspace move:", err)
	}
	// A second run finds nothing left to move and writes no marker: the legacy state is gone.
	if _, err := buckets.agent.Put(ctx, admin.Key("restart"), []byte("sentinel")); err != nil {
		t.Fatal(err)
	}
	buckets.migrateLegacy(ctx, bus.conn)
	if restart, err := buckets.agent.Get(ctx, admin.Key("restart")); err != nil || string(restart.Value()) != "sentinel" {
		t.Fatal("second run rewrote the restart marker:", err)
	}
	if moved, err := migrateToUser(ctx, buckets.chat, admin.ID); err != nil || moved != 0 {
		t.Fatal("second run moved", moved, err)
	}
}

func TestCLIUserResolution(t *testing.T) {
	bus, js := releaseTestBus(t)
	ctx := t.Context()
	users, admin, member := brainTestAuth(t, js)
	brainTestDisable(t, users, member)
	for _, ref := range []string{"admin", admin.ID} {
		if u, err := cliUser(ctx, bus.conn, ref); err != nil || u.ID != admin.ID || u.Name != "admin" {
			t.Fatalf("cliUser(%q) = %+v, %v", ref, u, err)
		}
	}
	for _, ref := range []string{"nobody", "member", newULID(), "Bad Name"} {
		if _, err := cliUser(ctx, bus.conn, ref); err == nil || err.Error() != "unknown COS user" {
			t.Fatalf("cliUser(%q) = %v; want unknown COS user", ref, err)
		}
	}
	if err := Run(ctx, Config{Home: t.TempDir()}, []string{"memory", "list"}, &bytes.Buffer{}); err == nil || err.Error() != "cos memory needs --user <name>" {
		t.Fatal("memory without --user:", err)
	}
	if err := Run(ctx, Config{Home: t.TempDir()}, []string{"--user", "admin"}, &bytes.Buffer{}); err == nil || !strings.HasPrefix(err.Error(), "usage: cos --user") {
		t.Fatal("--user without a command:", err)
	}
}

func TestMemoryListsOnlyTheUsersEntries(t *testing.T) {
	_, js := releaseTestBus(t)
	ctx := t.Context()
	mem, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: memoryBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctxKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: contextBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	a, b := User{ID: newULID(), Name: "a", Role: roleMember}, User{ID: newULID(), Name: "b", Role: roleMember}
	for _, u := range []struct {
		user User
		text string
	}{{a, "a's decision"}, {b, "b's decision"}} {
		view, err := UserKV(WithUser(ctx, u.user), mem)
		if err != nil {
			t.Fatal(err)
		}
		if err := memoryPut(ctx, view, MemoryEntry{ID: "m-" + u.user.Name, Kind: "decision", Text: u.text, At: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	actx := WithUser(ctx, a)
	mView, err := UserKV(actx, mem)
	if err != nil {
		t.Fatal(err)
	}
	cView, err := UserKV(actx, ctxKV)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := memoryCommand(actx, mView, cView, []string{"list"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "a's decision") || strings.Contains(out.String(), "b's decision") {
		t.Fatal("A's list:", out.String())
	}
}
