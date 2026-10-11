package cos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The budget checks walk the TS-826 path on the live cluster. Each run uses a fresh fake scope
// that no real thread group can own, and removes every Inbox item, raise key and thread it made.

const (
	e2eBudgetUsed      = 1200
	e2eBudgetLimit     = 1000
	e2eBudgetLabel     = "e2e budget check"
	e2eBudgetFYIWait   = 60 * time.Second
	e2eBudgetLiveWait  = 75 * time.Second
	e2eBudgetPauseWait = 90 * time.Second
	e2eBudgetRaiseWait = 3 * time.Minute
	e2eBudgetPoll      = 2 * time.Second
)

// budgetE2EModel is the only model the pause check may launch: Haiku threads are rejected by design.
var budgetE2EModel = map[string]any{"instanceId": "claudeAgent", "model": "claude-opus-5-5", "options": []map[string]string{{"id": "effort", "value": "low"}}}

// budgetE2EScope is a fresh group name in the same t-<hex> shape as budgetScope.
func budgetE2EScope() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "t-e2e" + hex.EncodeToString(b[:]), nil
}

// budgetE2EPauseKey keys the check's own question in the FYI check. Its suffix differs from the
// exceeded key, which the FYI already holds for the same scope.
func budgetE2EPauseKey(day, scope string) string {
	return fmt.Sprintf("budget:%s:%s:e2e:%d", day, scope, e2eBudgetLimit)
}

// budgetE2EEvent is a synthetic budget.exceeded for scope. An empty host and thread make an unresolved group.
func budgetE2EEvent(scope, day, host, thread string) (Event, error) {
	e, ok := budgetEvent(budgetData{Scope: scope, Day: day, Used: e2eBudgetUsed, Limit: e2eBudgetLimit, Label: e2eBudgetLabel}, host, thread, "")
	if !ok {
		return e, errors.New("budget event did not cross 100% of its limit")
	}
	e.Synthetic = true
	return e, nil
}

// budgetE2EItem reads the Inbox item filed under key; ok is false while it does not exist.
func budgetE2EItem(ctx context.Context, in *Inbox, key string) (item InboxItem, ok bool, err error) {
	entry, err := in.kv.Get(ctx, inboxID(key))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return item, false, nil
	}
	if err != nil {
		return item, false, err
	}
	return item, true, json.Unmarshal(entry.Value(), &item)
}

// budgetE2EWait polls get every e2eBudgetPoll until it reports true, or fails once d has passed.
func budgetE2EWait(ctx context.Context, d time.Duration, what string, get func(context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	for {
		ok, err := get(ctx)
		if err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not seen within %s", what, d)
		case <-time.After(e2eBudgetPoll):
		}
	}
}

// budgetCleanup runs every step, even after one fails or the check was cancelled, so a
// failed run never leaves an item, key or thread behind.
func budgetCleanup(ctx context.Context, steps ...func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var errs []error
	for _, step := range steps {
		errs = append(errs, step(ctx))
	}
	return errors.Join(errs...)
}

// budgetE2EPurge removes the item under key, if the check filed it, and the applied marker an answer left
// for it, so history and the settings bucket keep no probe state.
func budgetE2EPurge(in *Inbox, settings jetstream.KeyValue, key string) func(context.Context) error {
	return func(ctx context.Context) error {
		item, ok, err := budgetE2EItem(ctx, in, key)
		if err != nil || !ok {
			return err
		}
		// Only an answered item has a marker, so a missing one is not a failure.
		err = settings.Delete(ctx, budgetAppliedKey(item.ID))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			err = nil
		}
		return errors.Join(err, in.purge(ctx, item.ID))
	}
}

// budgetE2EDeleteRaise removes the day's raise for scope, so the fake scope leaves no limit behind.
// A raise that was never written is already gone, so its missing-key error is not a failure.
func budgetE2EDeleteRaise(settings jetstream.KeyValue, key string) func(context.Context) error {
	return func(ctx context.Context) error {
		err := settings.Delete(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return err
	}
}

// checkBudgetFYI publishes an exceeded event for an unresolved group, expects its FYI, then
// answers a thread pause question and expects the day's raise. Model tokens: none.
func checkBudgetFYI(ctx context.Context, e *E2EEnv) (evidence string, err error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return "", err
	}
	in, err := OpenInbox(ctx, bus, e.cfg.LocalHost, false)
	if err != nil {
		return "", err
	}
	settings, err := js.KeyValue(ctx, autopilotSettings)
	if err != nil {
		return "", err
	}
	scope, err := budgetE2EScope()
	if err != nil {
		return "", err
	}
	// A check that crosses 00:00 UTC can fail once; the next run uses the new day.
	day := time.Now().UTC().Format(time.DateOnly)
	fyiKey, pauseKey, raiseKey := budgetExceededKey(day, scope, e2eBudgetLimit), budgetE2EPauseKey(day, scope), budgetRaiseKey(day, scope)
	defer func() {
		err = errors.Join(err, budgetCleanup(ctx, budgetE2EPurge(in, settings, fyiKey), budgetE2EPurge(in, settings, pauseKey), budgetE2EDeleteRaise(settings, raiseKey)))
	}()

	ev, err := budgetE2EEvent(scope, day, "", "")
	if err != nil {
		return "", err
	}
	published := time.Now()
	if _, err := PublishEvent(ctx, js, ev); err != nil {
		return "", err
	}
	var fyi InboxItem
	err = budgetE2EWait(ctx, e2eBudgetFYIWait, "FYI "+fyiKey, func(ctx context.Context) (bool, error) {
		item, ok, err := budgetE2EItem(ctx, in, fyiKey)
		fyi = item
		return ok, err
	})
	if err != nil {
		return "", err
	}
	if fyi.Source != string(SourceBudget) || fyi.Kind != "fyi" || !strings.Contains(fyi.Title, e2eBudgetLabel) {
		return "", fmt.Errorf("FYI item = source %q kind %q title %q, want a budget fyi naming %q", fyi.Source, fyi.Kind, fyi.Title, e2eBudgetLabel)
	}
	fyiAt := time.Since(published).Seconds()

	options := budgetThreadOptionsFor(e2eBudgetUsed)
	question, err := in.Add(ctx, InboxInput{Kind: "question", Title: "e2e budget raise check", Options: options,
		Source: string(SourceBudget), Key: pauseKey})
	if err != nil {
		return "", err
	}
	answered, err := in.Answer(ctx, question.ID, AnswerData{Option: options[0]})
	if err != nil {
		return "", err
	}
	// The fake group has no usage rows, so its raise starts from the limit.
	key, want, ok := budgetRaise(answered, 0)
	if !ok || key != raiseKey {
		return "", fmt.Errorf("answer %q does not raise %s", options[0], raiseKey)
	}
	answeredAt := time.Now()
	err = budgetE2EWait(ctx, e2eBudgetRaiseWait, "raise "+raiseKey, func(ctx context.Context) (bool, error) {
		n, ok, err := budgetGet(ctx, settings, raiseKey)
		return ok && n == want, err
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("event -> FYI in %.0fs; answer -> raise %d -> %d in %.0fs", fyiAt, e2eBudgetLimit, want, time.Since(answeredAt).Seconds()), nil
}

// checkBudgetPause launches a throwaway Opus thread, exceeds its budget, and expects the wake,
// the thread question and a resume after the answer. It spends a few Opus tokens, so it is OnDemand.
func checkBudgetPause(ctx context.Context, e *E2EEnv) (evidence string, err error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	host := e.cfg.LocalHost
	js, err := bus.JetStream()
	if err != nil {
		return "", err
	}
	in, err := OpenInbox(ctx, bus, host, false)
	if err != nil {
		return "", err
	}
	settings, err := js.KeyValue(ctx, autopilotSettings)
	if err != nil {
		return "", err
	}
	scope, err := budgetE2EScope()
	if err != nil {
		return "", err
	}
	// A check that crosses 00:00 UTC can fail once; the next run uses the new day.
	day := time.Now().UTC().Format(time.DateOnly)
	// The engine files the pause question under the exceeded key, so the check waits on that one.
	pauseKey, raiseKey := budgetExceededKey(day, scope, e2eBudgetLimit), budgetRaiseKey(day, scope)

	var launched struct {
		ThreadID string `json:"threadId"`
	}
	input, _ := json.Marshal(map[string]any{"projectTitle": "ts-cos", "title": "e2e budget pause (throwaway)", "modelSelection": budgetE2EModel})
	if err := bus.Call(ctx, host, "launch", []string{string(input)}, &launched); err != nil {
		return "", err
	}
	threadID := launched.ThreadID
	defer func() {
		steps := []func(context.Context) error{budgetE2EPurge(in, settings, pauseKey), budgetE2EDeleteRaise(settings, raiseKey),
			func(ctx context.Context) error { return bus.Call(ctx, host, "delete", []string{threadID}, nil) }}
		err = errors.Join(err, budgetCleanup(ctx, steps...))
	}()

	// A thread the rules cannot find is filed as an FYI, not paused, so wait until the fleet lists it.
	err = budgetE2EWait(ctx, e2eBudgetFYIWait, "thread "+threadID+" in the fleet", func(ctx context.Context) (bool, error) {
		fleets, err := Fleet(ctx, bus)
		for _, f := range fleets {
			for _, t := range f.Threads {
				if f.Host == host && t.ThreadID == threadID {
					return true, nil
				}
			}
		}
		return false, err
	})
	if err != nil {
		return "", err
	}
	// shortcut: waits one live-refresh period (60 s) plus margin for the leader's snapshot; upgrade when Live exposes its refresh time
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(e2eBudgetLiveWait):
	}
	ev, err := budgetE2EEvent(scope, day, host, threadID)
	if err != nil {
		return "", err
	}
	published := time.Now()
	if _, err := PublishEvent(ctx, js, ev); err != nil {
		return "", err
	}

	t3, err := OpenT3(ctx, e.cfg, host)
	if err != nil {
		return "", err
	}
	// The thread command shows only the last assistant reply, so user messages come from the projection.
	userMessage := func(ctx context.Context, prefix string) (bool, error) {
		p, err := t3.Projection(ctx, threadID)
		if err != nil {
			return false, err
		}
		for _, m := range p.Messages {
			if m.Role == "user" && strings.HasPrefix(m.Text, prefix) {
				return true, nil
			}
		}
		return false, nil
	}
	options := budgetThreadOptionsFor(e2eBudgetUsed)
	var question InboxItem
	err = budgetE2EWait(ctx, e2eBudgetPauseWait, "PAUSED message and pause question", func(ctx context.Context) (bool, error) {
		item, ok, err := budgetE2EItem(ctx, in, pauseKey)
		question = item
		if err != nil {
			return false, err
		}
		if ok && item.Kind == "fyi" {
			return false, errors.New("leader snapshot did not include the thread")
		}
		if !ok || item.Status != "open" {
			return false, nil
		}
		return userMessage(ctx, "PAUSED: budget")
	})
	if err != nil {
		return "", err
	}
	if question.Kind != "question" || question.Thread != threadID || strings.Join(question.Options, "|") != strings.Join(options, "|") {
		return "", fmt.Errorf("pause question = kind %q thread %q options %v, want a thread question on %s", question.Kind, question.Thread, question.Options, threadID)
	}
	pausedAt := time.Since(published).Seconds()

	answered, err := in.Answer(ctx, question.ID, AnswerData{Option: options[0]})
	if err != nil {
		return "", err
	}
	// The fake group has no usage rows, so its raise starts from the limit.
	_, want, ok := budgetRaise(answered, 0)
	if !ok {
		return "", fmt.Errorf("answer %q does not raise a limit", options[0])
	}
	err = budgetE2EWait(ctx, e2eBudgetRaiseWait, "raise "+raiseKey+" and USER ANSWER", func(ctx context.Context) (bool, error) {
		n, ok, err := budgetGet(ctx, settings, raiseKey)
		if err != nil || !ok || n != want {
			return false, err
		}
		return userMessage(ctx, "USER ANSWER")
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("thread %s paused in %.0fs; answer -> raise %d -> %d and USER ANSWER delivered", threadID, pausedAt, e2eBudgetLimit, want), nil
}
