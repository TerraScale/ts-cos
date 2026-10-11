package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// agentEventsEvery bounds how often COS reads the immediate event kinds.
const agentEventsEvery = 5 * time.Second

// agentEventsFetch bounds the events one check reads; the last one read sets where the next check starts.
const agentEventsFetch = 100

// eventWake returns one line per immediate event COS has not seen yet, and how many there were.
func (a *Agent) eventWake(ctx context.Context, st *AgentState, now time.Time) (string, int, error) {
	if st.EventsSince.IsZero() {
		// First start or upgrade: start from now so old events are never replayed.
		st.EventsSince = now
		return "", 0, nil
	}
	if now.Sub(a.eventCheck) < agentEventsEvery {
		return "", 0, nil
	}
	a.eventCheck = now
	events, err := a.immediateEvents(ctx, st.EventsSince)
	if err != nil || len(events) == 0 {
		return "", 0, err
	}
	lines := make([]string, len(events))
	for i, e := range events {
		lines[i] = eventLine(e)
	}
	// Start delivery one nanosecond past the last event: DeliverByStartTime includes that event itself.
	st.EventsSince = events[len(events)-1].Time.Add(time.Nanosecond)
	return "Events since the last wake:\n" + strings.Join(lines, "\n"), len(events), nil
}

// immediateEvents reads the COS_EVENTS kinds that wake COS at once, oldest first, from since.
// chat.message is left out: the chat store already wakes COS for it.
func (a *Agent) immediateEvents(ctx context.Context, since time.Time) ([]Event, error) {
	subjects := []string{}
	for _, kind := range CurrentBehavior().Wake.Immediate {
		if EventKind(kind) != KindChatMessage {
			subjects = append(subjects, "cos.events.*."+kind)
		}
	}
	if len(subjects) == 0 {
		return nil, nil
	}
	cons, err := a.js.OrderedConsumer(ctx, EventsStream, jetstream.OrderedConsumerConfig{
		FilterSubjects: subjects, DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &since})
	if err != nil {
		return nil, err
	}
	batch, err := cons.Fetch(agentEventsFetch, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return nil, err
	}
	out := []Event{}
	for m := range batch.Messages() {
		meta, err := m.Metadata()
		if err != nil {
			return nil, err
		}
		var e Event
		if json.Unmarshal(m.Data(), &e) == nil {
			e.Time = meta.Timestamp
			out = append(out, e)
		}
	}
	return out, batch.Error()
}

// eventLine is one immediate event as COS reads it in a wake.
func eventLine(e Event) string {
	if e.Kind == KindUserAnswer {
		return fmt.Sprintf("Inbox answer on %s: %s", e.Item, e.Summary)
	}
	return fmt.Sprintf("%s %s/%s: %s", e.Kind, e.Host, e.Thread, e.Summary)
}
