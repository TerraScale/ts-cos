package cos

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	eventsDefaultCount = 20
	eventsMaxCount     = 500
	eventsReadTimeout  = 10 * time.Second
	eventsReadBatch    = 100
	eventsFilterPrefix = "cos.events."
)

var eventsUsage = errors.New("usage: cos events [<subject filter>] [<count>]")

// The COS agent and live checks read recent events on demand from any node, not only the rules engine.
func init() {
	cliCommands["events"] = cliCommand{Usage: "events [<subject filter, default cos.events.>>] [<count, default 20, max 500>]", Run: func(ctx context.Context, cfg Config, args []string, w io.Writer) error {
		filter, n, err := eventsArgs(args)
		if err != nil {
			return err
		}
		bus, err := ConnectBus(ctx, cfg)
		if err != nil {
			return err
		}
		defer bus.Close()
		js, err := bus.JetStream()
		if err != nil {
			return err
		}
		events, err := lastEvents(ctx, js, filter, n)
		if err != nil {
			return err
		}
		return output(w, events)
	}}
}

// eventsArgs accepts [<filter>] [<count>], or a lone count when it is numeric.
func eventsArgs(args []string) (string, int, error) {
	if len(args) > 2 {
		return "", 0, eventsUsage
	}
	filter, count := EventsSubjects, eventsDefaultCount
	if len(args) == 2 {
		filter = args[0]
	}
	if len(args) > 0 {
		last := args[len(args)-1]
		if _, err := strconv.Atoi(last); err == nil || len(args) == 2 {
			n, err := strconv.Atoi(last)
			if err != nil || n < 1 || n > eventsMaxCount {
				return "", 0, errors.New("count must be 1-500")
			}
			count = n
		} else {
			filter = last
		}
	}
	if !strings.HasPrefix(filter, eventsFilterPrefix) || len(filter) == len(eventsFilterPrefix) {
		return "", 0, errors.New("subject filter must start with cos.events.")
	}
	return filter, count, nil
}

// lastEvents reads the stream from the oldest message until the consumer has no pending
// messages left, keeping the newest n that decode as events.
func lastEvents(ctx context.Context, js jetstream.JetStream, filter string, n int) ([]Event, error) {
	ctx, cancel := context.WithTimeout(ctx, eventsReadTimeout)
	defer cancel()
	stream, err := js.Stream(ctx, EventsStream)
	if err != nil {
		return nil, err
	}
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{filter}, DeliverPolicy: jetstream.DeliverAllPolicy})
	if err != nil {
		return nil, err
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	pending := info.NumPending
	out := []Event{}
	for pending > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := consumer.Fetch(eventsReadBatch, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			return nil, err
		}
		for msg := range batch.Messages() {
			meta, err := msg.Metadata()
			if err != nil {
				return nil, err
			}
			pending = meta.NumPending
			var e Event
			if json.Unmarshal(msg.Data(), &e) != nil {
				continue
			}
			out = append(out, e)
			if len(out) > n {
				out = out[1:]
			}
		}
		if err := batch.Error(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
