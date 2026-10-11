package cos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestLogErrQuietsCancellation(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	live := context.Background()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("live ctx logs at error", func(t *testing.T) {
		buf.Reset()
		logErr(live, slog.LevelError, "read thing", errors.New("boom"))
		if out := buf.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "read thing") {
			t.Fatalf("want ERROR line, got %q", out)
		}
	})
	t.Run("cancelled ctx is silent", func(t *testing.T) {
		buf.Reset()
		logErr(cancelled, slog.LevelError, "read thing", errors.New("boom"))
		if out := buf.String(); out != "" {
			t.Fatalf("want no output, got %q", out)
		}
	})
	t.Run("wrapped context.Canceled is silent", func(t *testing.T) {
		buf.Reset()
		logErr(live, slog.LevelWarn, "read thing", fmt.Errorf("x: %w", context.Canceled))
		if out := buf.String(); out != "" {
			t.Fatalf("want no output, got %q", out)
		}
	})
}
