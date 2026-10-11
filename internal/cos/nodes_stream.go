package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Node samples live in three streams: raw every 10 s, then 1 m and 15 m roll-ups.
const (
	NodesRawStream = "COS_NODES_RAW"
	Nodes1mStream  = "COS_NODES_1M"
	Nodes15mStream = "COS_NODES_15M"

	nodesRawAge   = 6 * time.Hour
	nodes1mAge    = 7 * 24 * time.Hour
	nodes15mAge   = 90 * 24 * time.Hour
	nodesRawBytes = 64 << 20
	nodesBytes    = 128 << 20
	nodesMaxMsg   = 64 << 10
	nodesDedup    = 2 * time.Minute
	nodesBatch    = 256
)

var nodeStreams = map[string]string{"raw": NodesRawStream, "1m": Nodes1mStream, "15m": Nodes15mStream}

// NodeStreamConfigs are the fixed definitions of the three node streams.
func NodeStreamConfigs() []jetstream.StreamConfig {
	return []jetstream.StreamConfig{
		nodeStream(NodesRawStream, "cos.nodes.raw.>", nodesRawAge, nodesRawBytes),
		nodeStream(Nodes1mStream, "cos.nodes.1m.>", nodes1mAge, nodesBytes),
		nodeStream(Nodes15mStream, "cos.nodes.15m.>", nodes15mAge, nodesBytes),
	}
}

func nodeStream(name, subject string, age time.Duration, bytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: name, Subjects: []string{subject},
		Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld,
		Storage: jetstream.FileStorage, Replicas: eventsReplicas,
		MaxAge: age, MaxBytes: bytes, MaxMsgSize: nodesMaxMsg, Duplicates: nodesDedup,
	}
}

// EnsureNodeStreams creates or updates the node streams; it needs the JetStream meta leader.
func EnsureNodeStreams(ctx context.Context, js jetstream.JetStream) error {
	for _, cfg := range NodeStreamConfigs() {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("COS stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}

// PublishNodeSample stores one sample at resolution res: raw, 1m or 15m.
func PublishNodeSample(ctx context.Context, js jetstream.JetStream, res string, s NodeSample) error {
	stream, ok := nodeStreams[res]
	if !ok {
		return fmt.Errorf("unknown node resolution %q", res)
	}
	if !validNodeHost(s.Host) {
		return errors.New("invalid node host")
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(nodeSubject(res, s.Host))
	msg.Data = data
	id := fmt.Sprintf("%s:%s:%d", s.Host, res, s.Time.Unix())
	if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(id), jetstream.WithExpectStream(stream)); err != nil {
		return fmt.Errorf("COS node sample publish: %w", err)
	}
	return nil
}

// NodeLatest returns a host's newest raw sample.
func NodeLatest(ctx context.Context, js jetstream.JetStream, host string) (NodeSample, error) {
	return nodeLast(ctx, js, "raw", host)
}

// NodeSeries returns a host's samples from the last window, oldest first.
func NodeSeries(ctx context.Context, js jetstream.JetStream, host string, window time.Duration) ([]NodeSample, error) {
	if window <= 0 {
		return nil, errors.New("node window must be positive")
	}
	return nodeRange(ctx, js, nodeResolution(window), host, window)
}

// nodeResolution picks the stream that keeps a window at full detail: raw to 6 h, 1 m to 48 h, then 15 m.
func nodeResolution(window time.Duration) string {
	switch {
	case window <= 6*time.Hour:
		return "raw"
	case window <= 48*time.Hour:
		return "1m"
	}
	return "15m"
}

// nodeLast reads a host's newest sample at one resolution.
func nodeLast(ctx context.Context, js jetstream.JetStream, res, host string) (NodeSample, error) {
	if !validNodeHost(host) {
		return NodeSample{}, errors.New("invalid node host")
	}
	stream, err := js.Stream(ctx, nodeStreams[res])
	if err != nil {
		return NodeSample{}, err
	}
	msg, err := stream.GetLastMsgForSubject(ctx, nodeSubject(res, host))
	if err != nil {
		return NodeSample{}, err
	}
	var s NodeSample
	if err := json.Unmarshal(msg.Data, &s); err != nil {
		return NodeSample{}, fmt.Errorf("node sample: %w", err)
	}
	return s, nil
}

// nodeRange reads one host's samples from a resolution's stream since now-window, in stream order.
func nodeRange(ctx context.Context, js jetstream.JetStream, res, host string, window time.Duration) ([]NodeSample, error) {
	if !validNodeHost(host) {
		return nil, errors.New("invalid node host")
	}
	start := time.Now().Add(-window)
	cons, err := js.OrderedConsumer(ctx, nodeStreams[res], jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{nodeSubject(res, host)},
		DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
		OptStartTime:   &start,
	})
	if err != nil {
		return nil, err
	}
	out := []NodeSample{}
	for {
		batch, err := cons.Fetch(nodesBatch, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			return nil, err
		}
		n, pending := 0, uint64(1)
		for msg := range batch.Messages() {
			var s NodeSample
			if err := json.Unmarshal(msg.Data(), &s); err != nil {
				return nil, fmt.Errorf("node sample: %w", err)
			}
			out = append(out, s)
			n++
			if md, err := msg.Metadata(); err == nil {
				pending = md.NumPending
			}
		}
		if err := batch.Error(); err != nil {
			return nil, err
		}
		if n == 0 || pending == 0 {
			return out, nil
		}
	}
}

func nodeSubject(res, host string) string {
	return "cos.nodes." + res + "." + host
}

// validNodeHost accepts one NATS subject token: no dots, wildcards or whitespace.
func validNodeHost(host string) bool {
	return host != "" && len(host) <= 64 && strings.IndexFunc(host, func(r rune) bool {
		return r == '.' || r == '*' || r == '>' || unicode.IsSpace(r) || !unicode.IsGraphic(r)
	}) < 0
}
