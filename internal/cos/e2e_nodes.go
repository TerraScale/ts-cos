package cos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// nodeStreamInfo reads one node stream's info. Until the collector creates the stream, the check skips.
func nodeStreamInfo(ctx context.Context, js jetstream.JetStream, name string) (*jetstream.StreamInfo, error) {
	st, err := js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, e2eSkip("TS-825: node collector not deployed yet")
	}
	if err != nil {
		return nil, err
	}
	return st.Info(ctx)
}

// checkNodeData proves a host's newest sample is fresh and complete.
func checkNodeData(host string) func(context.Context, *E2EEnv) (string, error) {
	return func(ctx context.Context, e *E2EEnv) (string, error) {
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		js, err := bus.JetStream()
		if err != nil {
			return "", err
		}
		if _, err := nodeStreamInfo(ctx, js, NodesRawStream); err != nil {
			return "", err
		}
		s, err := NodeLatest(ctx, js, host)
		if err != nil {
			return "", fmt.Errorf("%s: no node sample: %w", host, err)
		}
		age := time.Since(s.Time).Round(time.Second)
		switch {
		case age >= 30*time.Second:
			return "", fmt.Errorf("%s: newest sample is %s old, want under 30s", host, age)
		case len(s.Cores) == 0:
			return "", fmt.Errorf("%s: sample has no per-core CPU", host)
		case s.Mem.Total == 0:
			return "", fmt.Errorf("%s: sample has no memory total", host)
		case len(s.Disks) == 0:
			return "", fmt.Errorf("%s: sample has no disks", host)
		case s.Uptime <= 0:
			return "", fmt.Errorf("%s: sample has no uptime", host)
		case s.Build.Revision == "":
			return "", fmt.Errorf("%s: sample has no build revision", host)
		}
		return fmt.Sprintf("%s: cpu %.0f%%, mem %.0f%%, %d disks, sample %s old", host, s.CPU, s.Mem.UsedPercent(), len(s.Disks), age), nil
	}
}

// checkNodeDownsampling proves every host has minute-aligned 1 m roll-ups, 15 m roll-ups are
// aligned, and the raw stream keeps about 6 h of samples.
func checkNodeDownsampling(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return "", err
	}
	raw, err := nodeStreamInfo(ctx, js, NodesRawStream)
	if err != nil {
		return "", err
	}
	if _, err := nodeStreamInfo(ctx, js, Nodes1mStream); err != nil {
		return "", err
	}
	if _, err := nodeStreamInfo(ctx, js, Nodes15mStream); err != nil {
		return "", err
	}
	if raw.State.FirstTime.IsZero() {
		return "", e2eSkip("TS-825: no node samples yet")
	}
	if age := time.Since(raw.State.FirstTime); age > nodesRawAge+5*time.Minute {
		return "", fmt.Errorf("raw stream keeps a sample %s old, want at most 6h05m", age.Round(time.Second))
	}
	var warming []string
	for _, h := range e2eHosts {
		series, err := nodeRange(ctx, js, "1m", h, 10*time.Minute)
		if err != nil {
			return "", fmt.Errorf("%s: 1m samples: %w", h, err)
		}
		aligned := false
		for _, s := range series {
			aligned = aligned || s.Time.Equal(s.Time.Truncate(time.Minute)) && s.N >= 1 && s.N <= 6
		}
		if !aligned {
			// A restarted node has no closed 1 m bucket yet; it is warming up, not broken.
			raw, err := nodeRange(ctx, js, "raw", h, 10*time.Minute)
			if err != nil {
				return "", fmt.Errorf("%s: raw samples: %w", h, err)
			}
			if len(raw) == 0 {
				return "", fmt.Errorf("%s: no raw samples in the last 10 min", h)
			}
			if raw[len(raw)-1].Time.Sub(raw[0].Time) < 2*time.Minute {
				warming = append(warming, h)
				continue
			}
			return "", fmt.Errorf("%s: no minute-aligned 1m sample of 1 to 6 raw samples in the last 10 min (%d samples)", h, len(series))
		}
		last, err := nodeLast(ctx, js, "15m", h)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("%s: 15m sample: %w", h, err)
		}
		if !last.Time.Equal(last.Time.Truncate(15 * time.Minute)) {
			return "", fmt.Errorf("%s: 15m sample at %s is not 15-minute aligned", h, last.Time.Format(time.RFC3339))
		}
	}
	evidence := fmt.Sprintf("1m roll-ups minute-aligned and 15m roll-ups 15-minute aligned on %v; oldest raw sample %s old", e2eHosts, time.Since(raw.State.FirstTime).Round(time.Minute))
	if len(warming) > 0 {
		evidence += "; " + strings.Join(warming, ", ") + " warming up"
	}
	return evidence, nil
}
