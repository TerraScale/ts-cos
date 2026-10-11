package cos

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

const budgetE2EScopeTest = "t-e2e0123abcd"

// The check's event and keys must be the ones the rules and Inbox accept, or the live check tests nothing.
func TestBudgetE2EEventAndKeysMatchRules(t *testing.T) {
	e, err := budgetE2EEvent(budgetE2EScopeTest, budgetTestDay, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Validate(); err != nil || !e.Synthetic {
		t.Fatalf("event invalid or not synthetic: %v, synthetic=%v", err, e.Synthetic)
	}
	key := budgetExceededKey(budgetTestDay, budgetE2EScopeTest, e2eBudgetLimit)
	if e.ID != key {
		t.Fatalf("event ID %q, want the exceeded key %q", e.ID, key)
	}
	got := Classify(e, rulesLive(), nil, WakeMemory{}, nil)
	assertRules(t, got, []Decision{{Class: ClassRecord, Rule: "budget-exceeded"}})
	budgetItemValid(t, got[0].Inbox)
	if got[0].Inbox.Kind != "fyi" || got[0].Inbox.Key != key || !strings.Contains(got[0].Inbox.Title, e2eBudgetLabel) {
		t.Fatalf("FYI = %+v, want key %q naming the check", got[0].Inbox, key)
	}

	// The pause question is filed under the exceeded key, the one the check waits on.
	pause := InboxItem{Source: string(SourceBudget), Key: key,
		Answer: &AnswerData{Option: budgetThreadOptionsFor(e2eBudgetUsed)[0]}}
	if raiseKey, value, ok := budgetRaise(pause, 0); !ok || raiseKey != budgetRaiseKey(budgetTestDay, budgetE2EScopeTest) || value != e2eBudgetLimit+e2eBudgetLimit/2 {
		t.Fatalf("pause answer raises %q to %d (%v), want the day's raise key to 1500", raiseKey, value, ok)
	}
}

func TestBudgetE2EDeleteRaiseIgnoresMissingKey(t *testing.T) {
	if err := budgetE2EDeleteRaise(missingKV{}, budgetRaiseKey(budgetTestDay, budgetE2EScopeTest))(context.Background()); err != nil {
		t.Fatalf("missing raise key failed cleanup: %v", err)
	}
	boom := errors.New("boom")
	if err := budgetE2EDeleteRaise(budgetFailingKV{err: boom}, "k")(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("real delete error = %v, want %v", err, boom)
	}
}

// missingKV reports every delete as a key that was never written.
type missingKV struct{ jetstream.KeyValue }

func (missingKV) Delete(context.Context, string, ...jetstream.KVDeleteOpt) error {
	return jetstream.ErrKeyNotFound
}

// budgetFailingKV fails every delete with err.
type budgetFailingKV struct {
	jetstream.KeyValue
	err error
}

func (f budgetFailingKV) Delete(context.Context, string, ...jetstream.KVDeleteOpt) error {
	return f.err
}

// The purge removes the answered item's applied marker with the item, and a marker that was never written is fine.
func TestBudgetE2EPurgeRemovesAppliedMarker(t *testing.T) {
	id := inboxID("k")
	body, err := json.Marshal(InboxItem{ID: id, Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	kv := &deleteKV{fakeKV{data: map[string][]byte{id: body, budgetAppliedKey(id): []byte(budgetTestDay)}}}
	if err := budgetE2EPurge(&Inbox{kv: kv}, kv, "k")(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(kv.data) != 0 {
		t.Fatalf("purge left %v behind", kv.data)
	}
	kv.data[id] = body
	if err := budgetE2EPurge(&Inbox{kv: kv}, kv, "k")(context.Background()); err != nil {
		t.Fatalf("purge without an applied marker failed: %v", err)
	}
}

// deleteKV is a fakeKV that also deletes and purges, reporting a missing key like NATS does.
type deleteKV struct{ fakeKV }

func (kv *deleteKV) Delete(_ context.Context, key string, _ ...jetstream.KVDeleteOpt) error {
	if _, ok := kv.data[key]; !ok {
		return jetstream.ErrKeyNotFound
	}
	delete(kv.data, key)
	return nil
}

func (kv *deleteKV) Purge(_ context.Context, key string, _ ...jetstream.KVDeleteOpt) error {
	delete(kv.data, key)
	return nil
}

func TestBudgetCleanupRunsEveryStep(t *testing.T) {
	var ran []string
	boom := errors.New("boom")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a cancelled or timed-out check still cleans up
	err := budgetCleanup(ctx,
		func(context.Context) error { ran = append(ran, "dismiss"); return boom },
		func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Error("cleanup step received a cancelled context")
			}
			ran = append(ran, "raise")
			return nil
		},
		func(context.Context) error { ran = append(ran, "thread"); return nil })
	if !errors.Is(err, boom) || strings.Join(ran, ",") != "dismiss,raise,thread" {
		t.Fatalf("cleanup ran %v with error %v; want every step and the first error", ran, err)
	}
}

// The stale sweep removes only keys a budget check made: fake t-e2e groups with eight hex digits.
func TestBudgetE2EStaleKeyMatchesOnlyFakeGroups(t *testing.T) {
	day := budgetTestDay
	for _, c := range []struct {
		key  string
		want bool
	}{
		{budgetExceededKey(day, budgetE2EScopeTest, e2eBudgetLimit), true},
		{budgetE2EPauseKey(day, budgetE2EScopeTest), true},
		{budgetRaiseKey(day, budgetE2EScopeTest), true},
		{budgetRaiseKey("20260101", budgetE2EScopeTest), true},
		{budgetExceededKey(day, budgetScope("beta/proj"), 1000), false},
		{budgetRaiseKey(day, budgetScope("beta/proj")), false},
		{budgetExceededKey(day, "t-e2e", 1000), false},
		{budgetExceededKey(day, "t-e2e0123abcdz", 1000), false},
		{budgetExceededKey(day, "t-e2e0123ab", 1000), false},
		{"budget:" + day + ":t-e2e0123abcd:100", false},
		{"e2e:" + budgetE2EScopeTest, false},
		{budgetAppliedKey("04230dd1ea369d48"), false},
	} {
		if got := budgetE2EStaleKey(c.key); got != c.want {
			t.Errorf("budgetE2EStaleKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}
