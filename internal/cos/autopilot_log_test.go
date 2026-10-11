package cos

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// captureSlog sends the default logger to a buffer for the test's duration.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestAutopilotLogRetriesThenSucceedsWithOneWarn(t *testing.T) {
	prev := autopilotLogBackoff
	autopilotLogBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { autopilotLogBackoff = prev })
	buf := captureSlog(t)
	calls := 0
	a := &Autopilot{Host: "beta", pub: func(context.Context, string, []byte) error {
		calls++
		if calls < 3 {
			return errors.New("context deadline exceeded")
		}
		return nil
	}}
	a.publish(context.Background(), AutopilotEntry{Kind: "record", Action: "Reconciled live state"})
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d, want 1: %s", n, buf.String())
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("unexpected ERROR: %s", buf.String())
	}
}

func TestAutopilotLogAlwaysFailingWarnsOnceAndErrorsOnce(t *testing.T) {
	prev := autopilotLogBackoff
	autopilotLogBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { autopilotLogBackoff = prev })
	buf := captureSlog(t)
	calls := 0
	a := &Autopilot{Host: "beta", pub: func(context.Context, string, []byte) error {
		calls++
		return errors.New("nats unavailable")
	}}
	a.publish(context.Background(), AutopilotEntry{Kind: "error", Action: "Save live state"})
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d, want 1: %s", n, buf.String())
	}
	if n := strings.Count(buf.String(), "level=ERROR"); n != 1 {
		t.Errorf("ERROR lines = %d, want 1: %s", n, buf.String())
	}
}

func TestAutopilotLogDroppedOnShutdownLogsInfoNotError(t *testing.T) {
	prev := autopilotLogBackoff
	autopilotLogBackoff = []time.Duration{time.Hour}
	t.Cleanup(func() { autopilotLogBackoff = prev })
	buf := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	a := &Autopilot{Host: "beta", pub: func(context.Context, string, []byte) error {
		calls++
		cancel()
		return errors.New("nats unavailable")
	}}
	a.publish(ctx, AutopilotEntry{Kind: "error", Action: "Save live state"})
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 0 {
		t.Errorf("WARN lines = %d, want 0: no retry runs after shutdown: %s", n, buf.String())
	}
	if n := strings.Count(buf.String(), "level=INFO msg=\"autopilot log dropped\""); n != 1 {
		t.Errorf("INFO dropped lines = %d, want 1: %s", n, buf.String())
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("unexpected ERROR: %s", buf.String())
	}
}

func TestAutopilotLogSuccessLogsNothing(t *testing.T) {
	buf := captureSlog(t)
	calls := 0
	a := &Autopilot{Host: "beta", pub: func(context.Context, string, []byte) error {
		calls++
		return nil
	}}
	a.publish(context.Background(), AutopilotEntry{Kind: "beat", Action: "Heartbeat"})
	if calls != 1 || buf.Len() != 0 {
		t.Errorf("calls = %d, log = %q; want one call and no log lines", calls, buf.String())
	}
}

func TestLiveErrorReportsOnlyWhileContextIsLive(t *testing.T) {
	e := AutopilotEntry{Kind: "error", Action: "Save live state", Reason: "context canceled"}
	if got := liveError(context.Background(), e); len(got) != 1 {
		t.Errorf("live ctx entries = %d, want 1", len(got))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := liveError(ctx, e); got != nil {
		t.Errorf("cancelled ctx entries = %v, want none", got)
	}
}

func TestAutopilotLogCanceledAtShutdownLogsInfoNotError(t *testing.T) {
	prev := autopilotLogBackoff
	autopilotLogBackoff = []time.Duration{time.Hour}
	t.Cleanup(func() { autopilotLogBackoff = prev })
	buf := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &Autopilot{Host: "beta", pub: func(context.Context, string, []byte) error {
		cancel()
		return context.Canceled
	}}
	a.publish(ctx, AutopilotEntry{Kind: "error", Action: "Save live state", Reason: "context canceled"})
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("unexpected ERROR: %s", buf.String())
	}
	if n := strings.Count(buf.String(), "level=INFO msg=\"autopilot log dropped\""); n != 1 {
		t.Errorf("INFO dropped lines = %d, want 1: %s", n, buf.String())
	}
}
