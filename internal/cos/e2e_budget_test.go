package cos

import (
	"context"
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
