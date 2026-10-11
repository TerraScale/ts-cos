package cos

import (
	"log/slog"
	"testing"
	"time"
)

func TestMirrorBackoff(t *testing.T) {
	for _, tc := range []struct {
		failures int
		wait     time.Duration
		level    slog.Level
	}{
		{1, 5 * time.Second, slog.LevelWarn},
		{2, 10 * time.Second, slog.LevelWarn},
		{3, 20 * time.Second, slog.LevelWarn},
		{4, 30 * time.Second, slog.LevelWarn},
		{5, 30 * time.Second, slog.LevelError},
		{9, 30 * time.Second, slog.LevelError},
	} {
		if wait, level := mirrorBackoff(tc.failures); wait != tc.wait || level != tc.level {
			t.Errorf("mirrorBackoff(%d) = %v, %v; want %v, %v", tc.failures, wait, level, tc.wait, tc.level)
		}
	}
}
