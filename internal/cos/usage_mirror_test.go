package cos

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The mirror counts consecutive failures and backs off like the event mirror; a shutdown returns no wait and logs nothing.
func TestUsageMirrorFailedCountsAndStopsOnShutdown(t *testing.T) {
	boom := errors.New("context deadline exceeded")
	if n, wait := usageMirrorFailed(context.Background(), boom, 0); n != 1 || wait != 5*time.Second {
		t.Fatalf("first failure = %d, %v; want 1, 5s", n, wait)
	}
	if n, wait := usageMirrorFailed(context.Background(), boom, 4); n != 5 || wait != 30*time.Second {
		t.Fatalf("fifth failure = %d, %v; want 5, 30s", n, wait)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n, wait := usageMirrorFailed(ctx, boom, 2); n != 2 || wait != 0 {
		t.Fatalf("failure on shutdown = %d, %v; want 2, 0", n, wait)
	}
}
