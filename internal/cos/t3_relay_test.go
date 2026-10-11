package cos

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// Two clustered nodes: delta relays its (replayed) T3, a client on beta follows it.
func TestT3RelayAcrossNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	thread := t3Frames(t, "subscribe-thread-lifecycle.jsonl")
	drifted := mutateFrame(thread, `"kind":"event"`, `"kind":"event"`, `"kind":"delta"`)
	replay := newT3Replay(t, func(tag string, payload map[string]any) []string {
		switch {
		case tag == "orchestration.subscribeShell":
			return t3Frames(t, "subscribe-shell.jsonl")
		case payload["threadId"] == "drifted":
			return drifted
		}
		return thread
	})
	ports := busTestPorts(t, 4)
	hosts := []string{"delta", "beta"}
	var client, delta, beta *Bus
	for i, host := range hosts {
		cfg, busNode := busTestConfig(t, host, ports[i], ports[i+2], []int{ports[3-i]})
		node, err := startBus(ctx, cfg, busNode, func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(node.Close)
		if host == "delta" {
			if err := node.ServeT3Streams(host, func(context.Context) (*T3, error) { return replay.client(), nil }); err != nil {
				t.Fatal(err)
			}
			delta = node
			continue
		}
		beta = node
		if client, err = ConnectBus(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.Close)
	}
	// Wait until beta has learned delta's endpoint over the route, so each stream below is one deterministic attempt.
	busTestReady(t, "delta's stream endpoint on beta", func() bool {
		return beta.server.GlobalAccount().SubscriptionInterest("cos.delta.stream")
	})
	// beta found delta already up, so neither node reloaded its routes (a reload drops local clients).
	if n := delta.reloads.Load() + beta.reloads.Load(); n != 0 {
		t.Fatalf("%d route reloads during startup", n)
	}
	stop := errors.New("stop")

	t.Run("thread events reach another node", func(t *testing.T) {
		var events []ThreadEvent
		err := client.StreamThread(ctx, "delta", "thread-1", func(e ThreadEvent) error {
			events = append(events, e)
			if e.Type == "thread.metadata-updated" {
				return stop
			}
			return nil
		})
		if !errors.Is(err, stop) {
			t.Fatal(err)
		}
		first := events[0]
		if len(events) != 72 || first.Snapshot == nil || first.Snapshot.ProviderTurns != nil || first.Context == nil || first.Snapshot.Thread.ID == "" {
			t.Fatalf("got %d events, first %+v", len(events), first)
		}
		var texts []string
		for _, e := range events {
			if e.Message != nil {
				texts = append(texts, e.Message.Text)
			}
		}
		if strings.Join(texts, ",") != "PONG,Now reply with exactly PING.,PING,/compact Keep only the words PONG and PING." {
			t.Errorf("messages = %v", texts)
		}
		busTestReady(t, "relay to release T3 after the client stops", func() bool { return replay.openSockets() == 0 })
	})

	t.Run("shell events reach another node", func(t *testing.T) {
		n := 0
		err := client.StreamShell(ctx, "delta", func(e ShellEvent) error {
			if n++; n == 12 {
				return stop
			}
			return nil
		})
		if !errors.Is(err, stop) {
			t.Fatal(err)
		}
	})

	t.Run("schema drift on the host reaches the client", func(t *testing.T) {
		err := client.StreamThread(ctx, "delta", "drifted", func(ThreadEvent) error { return nil })
		if err == nil || !strings.Contains(err.Error(), "T3 schema changed on delta") || !strings.Contains(err.Error(), `unknown stream value kind "delta"`) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("offline host is a clear error", func(t *testing.T) {
		err := client.StreamThread(ctx, "mariogk-dev", "thread-1", func(ThreadEvent) error { return nil })
		if err == nil || err.Error() != "COS node mariogk-dev unavailable" {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("cancelled client releases the host", func(t *testing.T) {
		streamCtx, stopStream := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- client.StreamThread(streamCtx, "delta", "thread-1", func(e ThreadEvent) error {
				if e.Type == "thread.metadata-updated" {
					stopStream()
				}
				return nil
			})
		}()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
		busTestReady(t, "relay to release T3", func() bool { return replay.openSockets() == 0 })
	})
}

func TestStreamCommandIsRegistered(t *testing.T) {
	var help strings.Builder
	if err := Run(t.Context(), Config{}, []string{"help"}, &help); err != nil || !strings.Contains(help.String(), `"stream \u003chost\u003e`) {
		t.Fatalf("help = %s, %v", help.String(), err)
	}
	for _, args := range [][]string{{"stream"}, {"stream", "-bad"}, {"stream", "delta", "a", "b"}} {
		if err := Run(t.Context(), Config{}, args, io.Discard); err == nil || err.Error() != "usage: cos stream <host> [<threadId>]" {
			t.Errorf("Run(%v) = %v", args, err)
		}
	}
}
