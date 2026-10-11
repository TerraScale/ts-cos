package cos

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestBudgetBackoff(t *testing.T) {
	for i, want := range []struct {
		wait  time.Duration
		level slog.Level
	}{
		{5 * time.Second, slog.LevelWarn},
		{10 * time.Second, slog.LevelWarn},
		{20 * time.Second, slog.LevelWarn},
		{40 * time.Second, slog.LevelWarn},
		{60 * time.Second, slog.LevelError},
		{60 * time.Second, slog.LevelError},
	} {
		if wait, level := budgetBackoff(i + 1); wait != want.wait || level != want.level {
			t.Errorf("budgetBackoff(%d) = %v, %v; want %v, %v", i+1, wait, level, want.wait, want.level)
		}
	}
}

func TestBudgetSweepFailedCanceledLogsNothing(t *testing.T) {
	buf := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, err := range []error{context.DeadlineExceeded, jetstream.ErrBucketNotFound, errors.New("disk")} {
		budgetSweepFailed(ctx, err, 0)
	}
	if buf.Len() != 0 {
		t.Fatalf("canceled sweep logged %q", buf.String())
	}
}

func TestBudgetSweepFailedLevels(t *testing.T) {
	buf := captureSlog(t)
	ctx := context.Background()
	failures := 0
	for range 5 {
		failures, _ = budgetSweepFailed(ctx, jetstream.ErrBucketNotFound, failures)
	}
	if failures != 5 || strings.Count(buf.String(), "level=WARN") != 4 || strings.Count(buf.String(), "level=ERROR") != 1 {
		t.Fatalf("5 transient failures: count %d, log %q; want 4 WARN and 1 ERROR", failures, buf.String())
	}

	buf.Reset()
	failures, wait := budgetSweepFailed(ctx, errors.New("disk"), 3)
	if failures != 0 || wait != usageScanEvery || !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("non-transient failure = count %d, wait %v, log %q; want reset, normal tick, one ERROR", failures, wait, buf.String())
	}
}
