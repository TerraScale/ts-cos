package cos

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// runRulesFailing runs the consumer loop: consumeFn fails n times, then blocks until ctx ends.
// progress makes each failed session ack a message first, as a healthy consumer would.
func runRulesFailing(t *testing.T, n int, progress bool) string {
	t.Helper()
	prev := rulesBackoff
	rulesBackoff = []time.Duration{time.Millisecond}
	t.Cleanup(func() { rulesBackoff = prev })
	buf := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &RulesEngine{}
	calls := 0
	blocked := make(chan struct{})
	r.consumeFn = func(ctx context.Context) error {
		calls++
		if calls <= n {
			if progress {
				r.handled.Add(1)
			}
			return errors.New("nats: no heartbeat received")
		}
		close(blocked)
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	<-blocked
	cancel()
	<-done
	return buf.String()
}

func TestRulesRunWarnsWhileRestartingWithoutError(t *testing.T) {
	logs := runRulesFailing(t, 3, false)
	if n := strings.Count(logs, "level=WARN"); n != 3 {
		t.Errorf("WARN lines = %d, want 3: %s", n, logs)
	}
	if strings.Contains(logs, "level=ERROR") {
		t.Errorf("unexpected ERROR: %s", logs)
	}
}

func TestRulesRunLogsOneErrorWhenItKeepsFailing(t *testing.T) {
	logs := runRulesFailing(t, 6, false)
	if n := strings.Count(logs, "level=ERROR"); n != 1 {
		t.Errorf("ERROR lines = %d, want 1: %s", n, logs)
	}
	if n := strings.Count(logs, "level=WARN"); n != 5 {
		t.Errorf("WARN lines = %d, want 5: %s", n, logs)
	}
}

func TestRulesRunProgressResetsFailureCount(t *testing.T) {
	logs := runRulesFailing(t, 6, true)
	if strings.Contains(logs, "level=ERROR") {
		t.Errorf("unexpected ERROR after progress: %s", logs)
	}
	if n := strings.Count(logs, "attempt=1"); n != 6 {
		t.Errorf("attempt=1 lines = %d, want 6: %s", n, logs)
	}
}

func TestRulesRunWithoutInjectedConsumerReturnsOnCancelledCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &RulesEngine{}
	r.Run(ctx)
}

func TestRulesRunShutdownLogsNothing(t *testing.T) {
	buf := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &RulesEngine{}
	r.consumeFn = func(context.Context) error {
		cancel()
		return errors.New("nats: connection closed")
	}
	r.Run(ctx)
	if buf.Len() != 0 {
		t.Errorf("shutdown logged: %s", buf.String())
	}
}
