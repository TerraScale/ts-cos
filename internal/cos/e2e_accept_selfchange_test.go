package cos

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestE2ERetryJS(t *testing.T) {
	t.Run("retries 503 until success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		calls := 0
		v, err := e2eRetryJS(ctx, func(context.Context) (string, error) {
			calls++
			if calls <= 2 {
				return "", &jetstream.APIError{Code: 503}
			}
			return "ok", nil
		})
		if v != "ok" || err != nil || calls != 3 {
			t.Fatalf("got %q, %v after %d calls; want \"ok\", nil after 3", v, err, calls)
		}
	})
	t.Run("other errors fail at once", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		calls := 0
		_, err := e2eRetryJS(ctx, func(context.Context) (string, error) {
			calls++
			return "", errors.New("boom")
		})
		if err == nil || err.Error() != "boom" || calls != 1 {
			t.Fatalf("got %v after %d calls; want boom after 1 call", err, calls)
		}
	})
	t.Run("attempt timeout retries", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		calls := 0
		v, err := e2eRetryJS(ctx, func(context.Context) (string, error) {
			calls++
			if calls == 1 {
				return "", context.DeadlineExceeded
			}
			return "ok", nil
		})
		if v != "ok" || err != nil || calls != 2 {
			t.Fatalf("got %q, %v after %d calls; want \"ok\", nil after 2", v, err, calls)
		}
	})
	t.Run("no responders stops at context end", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := e2eRetryJS(ctx, func(context.Context) (string, error) {
			return "", nats.ErrNoResponders
		})
		if !errors.Is(err, nats.ErrNoResponders) {
			t.Fatalf("got %v; want nats.ErrNoResponders", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("retry ran %v past the 2.5 s context", elapsed)
		}
	})
}
