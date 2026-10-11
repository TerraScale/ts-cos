package cos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"
)

// checkT3Down serves a synthetic host whose T3 refuses connections. The fleet must report that host's
// failure alone, the real hosts must keep answering, and the host must disappear once it stops.
func checkT3Down(ctx context.Context, e *E2EEnv) (evidence string, err error) {
	start := time.Now()
	raw := make([]byte, 3)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	ghost := "e2e-" + hex.EncodeToString(raw)
	// Binding then closing a port gives an address nothing listens on, so every T3 call fails to connect.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	cfg := e.cfg
	open := func(context.Context) (*T3, error) {
		return &T3{Config: cfg, Host: ghost, BaseURL: fmt.Sprintf("http://127.0.0.1:%d", port), token: "e2e-dummy"}, nil
	}
	if err := bus.addService(ghost, func(ctx context.Context, command string, args []string) (any, error) {
		return localCommand(ctx, open, command, args)
	}); err != nil {
		return "", err
	}
	defer func() {
		cctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_ = bus.service.Stop()
		switch cerr := waitHost(cctx, bus, ghost, false, 15*time.Second); {
		case cerr != nil:
			evidence, err = "", cerr
		case err == nil:
			evidence += "; " + ghost + " gone after cleanup"
		}
	}()
	if err := bus.ServeT3Streams(ghost, open); err != nil {
		return "", err
	}
	if err := waitHost(ctx, bus, ghost, true, 10*time.Second); err != nil {
		return "", err
	}
	fctx, stopFleet := context.WithTimeout(ctx, 20*time.Second)
	fleet, err := Fleet(fctx, bus)
	stopFleet()
	if err != nil {
		return "", err
	}
	verdict, err := t3DownVerdict(fleet, ghost)
	if err != nil {
		return "", err
	}
	actx, stopAsks := context.WithTimeout(ctx, 20*time.Second)
	_, err = AllAsks(actx, bus)
	stopAsks()
	if err != nil {
		return "", err
	}
	sctx, stopStream := context.WithTimeout(ctx, 10*time.Second)
	err = bus.StreamThread(sctx, ghost, "x", func(ThreadEvent) error { return nil })
	stopStream()
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		return "", fmt.Errorf("stream to %s did not fail within 10 s: %v", ghost, err)
	}
	return fmt.Sprintf("%s in %d ms", verdict, time.Since(start).Milliseconds()), nil
}

// waitHost polls the discovered hosts until host is listed (want) or gone (!want), for at most wait.
func waitHost(ctx context.Context, bus *Bus, host string, want bool, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		hosts, err := bus.Hosts(ctx)
		if err != nil {
			return err
		}
		if slices.Contains(hosts, host) == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: fleet listing did not reach listed=%t", host, want)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// t3DownVerdict wants the ghost's row to name it and report the failed T3 connection without the dummy
// token or the T3 URL, and every real host to answer without an error.
func t3DownVerdict(fleet []HostFleet, ghost string) (string, error) {
	var failed string
	for _, f := range fleet {
		if f.Host == ghost {
			failed = f.Error
			continue
		}
		if f.Error != "" {
			return "", fmt.Errorf("%s: %s", f.Host, f.Error)
		}
	}
	switch {
	case !strings.Contains(failed, ghost) || !strings.Contains(failed, "connection failed"):
		return "", fmt.Errorf("%s row does not report a failed T3 connection: %q", ghost, failed)
	case strings.Contains(failed, "e2e-dummy") || strings.Contains(failed, "http://"):
		return "", fmt.Errorf("%s error leaks the T3 token or URL", ghost)
	}
	for _, h := range e2eHosts {
		if !slices.ContainsFunc(fleet, func(f HostFleet) bool { return f.Host == h }) {
			return "", fmt.Errorf("%s did not answer the fleet", h)
		}
	}
	return fmt.Sprintf("%s: T3 connection failed reported per host; %s answered", ghost, strings.Join(e2eHosts, ", ")), nil
}
