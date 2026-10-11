package cos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

// collectorProbeScript runs the running binary's `nodes` with too many arguments: a build with the collector prints
// usage, an older one rejects the command. Both answer before connecting anywhere.
const collectorProbeScript = `pid=$(systemctl --user show ` + e2eUnit + ` -p MainPID --value); if [ "${pid:-0}" != 0 ]; then /proc/$pid/exe nodes probe probe probe 2>&1 | head -3; fi`

// collectorProbe is one host's answer: whether its running build has the node collector, and its revision.
type collectorProbe struct {
	built bool
	rev   string
}

// nodeCollectorBuilt probes host's running build for the node collector, once per run.
func (e *E2EEnv) nodeCollectorBuilt(ctx context.Context, host string) (bool, string, error) {
	e.mu.Lock()
	c, ok := e.collectors[host]
	e.mu.Unlock()
	if ok {
		return c.built, c.rev, nil
	}
	f, err := e.deployed(ctx, host)
	if err != nil {
		return false, "", err
	}
	var v struct {
		Revision string `json:"revision"`
	}
	_ = json.Unmarshal([]byte(f["Version"]), &v)
	c.rev = v.Revision[:min(len(v.Revision), 12)]
	out, err := e.run(ctx, host, collectorProbeScript)
	if err != nil {
		return false, "", err
	}
	switch out = strings.TrimSpace(out); {
	case out == "":
		return false, "", fmt.Errorf("%s: %s has no running process", host, e2eUnit)
	case strings.Contains(out, "usage: cos nodes"):
		c.built = true
	case strings.Contains(out, `unknown command "nodes"`):
		// An older build rejects the command: built stays false.
	default:
		return false, "", fmt.Errorf("%s: unexpected collector probe output: %q", host, e2eClip(out, 200))
	}
	e.mu.Lock()
	if e.collectors == nil {
		e.collectors = map[string]collectorProbe{}
	}
	e.collectors[host] = c
	e.mu.Unlock()
	return c.built, c.rev, nil
}

// nodeOutdated reports whether host's running build predates the node collector, and that build's revision.
func (e *E2EEnv) nodeOutdated(ctx context.Context, host string) (string, bool, error) {
	built, rev, err := e.nodeCollectorBuilt(ctx, host)
	return rev, err == nil && !built, err
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
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			rev, outdated, perr := e.nodeOutdated(ctx, host)
			if perr != nil {
				return "", perr
			}
			if outdated {
				return "", e2eSkip(fmt.Sprintf("TS-825: %s runs %s, which predates the node collector", host, rev))
			}
		}
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
	var warming, without []string
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
				rev, outdated, err := e.nodeOutdated(ctx, h)
				if err != nil {
					return "", err
				}
				if outdated {
					without = append(without, fmt.Sprintf("%s runs %s without the collector", h, rev))
					continue
				}
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
	if len(without) == len(e2eHosts) {
		return "", e2eSkip("TS-825: " + strings.Join(without, "; "))
	}
	evidence := fmt.Sprintf("1m roll-ups minute-aligned and 15m roll-ups 15-minute aligned on %v; oldest raw sample %s old", e2eHosts, time.Since(raw.State.FirstTime).Round(time.Minute))
	if len(warming) > 0 {
		evidence += "; " + strings.Join(warming, ", ") + " warming up"
	}
	if len(without) > 0 {
		evidence += "; " + strings.Join(without, "; ")
	}
	return evidence, nil
}

// checkNodeCollectorStop publishes two samples for a synthetic host, then stops; the leader should call it missing.
func checkNodeCollectorStop(ctx context.Context, e *E2EEnv) (string, error) {
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
	return publishGhostNode(ctx, e, js)
}

// checkNodeCollectorMissing waits for the leader's missing alert for the stopped synthetic host, then purges its samples.
func checkNodeCollectorMissing(ctx context.Context, e *E2EEnv) (string, error) {
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
	if e.nodeGhost == "" { // run alone by a filter: stop a host here first
		if _, err := publishGhostNode(ctx, e, js); err != nil {
			return "", err
		}
	}
	ghost, at := e.nodeGhost, e.nodeGhostAt
	defer purgeGhostNode(ctx, js, ghost)
	// Start a little early so the consumer also sees an alert published just before this check.
	start := at.Add(-5 * time.Second)
	cons, err := js.OrderedConsumer(ctx, EventsStream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{Event{Source: SourceNode, Kind: KindNodeAlert}.Subject()},
		DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
		OptStartTime:   &start,
	})
	if err != nil {
		return "", err
	}
	deadline := at.Add(100 * time.Second)
	for {
		seen, found, err := ghostMissingAlert(cons, ghost)
		if err != nil {
			return "", err
		}
		if found {
			return fmt.Sprintf("%s reported missing after %.0f s", ghost, seen.Sub(at).Seconds()), nil
		}
		if time.Now().After(deadline) {
			if why := e.leaderPredatesMain(ctx, bus); why != "" {
				return "", e2eSkip(why)
			}
			return "", fmt.Errorf("no node.alert missing event for %s within 100 s of its last sample", ghost)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// publishGhostNode writes two raw samples for a random synthetic host one second apart and records them on e.
func publishGhostNode(ctx context.Context, e *E2EEnv, js jetstream.JetStream) (string, error) {
	raw := make([]byte, 3)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	ghost := "e2e-" + hex.EncodeToString(raw)
	var last time.Time
	for i := range 2 {
		if i > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Second):
			}
		}
		last = time.Now().UTC()
		s := NodeSample{Host: ghost, Time: last, N: 1, Synthetic: true, Mem: NodeMem{Total: 1, Available: 1}}
		if err := PublishNodeSample(ctx, js, "raw", s); err != nil {
			return "", err
		}
	}
	e.nodeGhost, e.nodeGhostAt = ghost, last
	return fmt.Sprintf("published 2 samples for %s, then stopped", ghost), nil
}

// ghostMissingAlert drains the alerts delivered so far and reports when the ghost's missing alert was raised.
func ghostMissingAlert(cons jetstream.Consumer, ghost string) (time.Time, bool, error) {
	batch, err := cons.Fetch(nodesBatch, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return time.Time{}, false, err
	}
	var at time.Time
	found := false
	for m := range batch.Messages() {
		var e Event
		if json.Unmarshal(m.Data(), &e) != nil || e.Host != ghost || !e.Synthetic {
			continue
		}
		var d struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(e.Data, &d)
		if d.Type == "missing" {
			at, found = e.Time, true
		}
	}
	return at, found, batch.Error()
}

// purgeGhostNode removes a synthetic host's raw samples so no later leader sees it. Cleanup runs even when the check fails.
func purgeGhostNode(ctx context.Context, js jetstream.JetStream, ghost string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	st, err := js.Stream(ctx, NodesRawStream)
	if err == nil {
		err = st.Purge(ctx, jetstream.WithPurgeSubject(nodeSubject("raw", ghost)))
	}
	if err != nil {
		slog.Warn("e2e node cleanup", "host", ghost, "error", err)
	}
}

// leaderPredatesMain explains a missing alert by the leader still running an older build than main
// (whose watcher may predate the alerts); "" means the leader runs main and the alert really is missing.
func (e *E2EEnv) leaderPredatesMain(ctx context.Context, bus *Bus) string {
	leader, err := bus.Leader(ctx)
	if err != nil {
		return ""
	}
	head, err := e.mainSHA(ctx)
	if err != nil {
		return ""
	}
	f, err := e.hostFacts(ctx, leader, false)
	if err != nil {
		return ""
	}
	var v struct {
		Revision string `json:"revision"`
	}
	if json.Unmarshal([]byte(f["Version"]), &v) != nil || v.Revision == head.SHA {
		return ""
	}
	return fmt.Sprintf("TS-825: leader %s runs %.12s, not main %.12s; its node alert watcher may predate the alerts", leader, v.Revision, head.SHA)
}
