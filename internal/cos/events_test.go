package cos

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func eventsTestEvent() Event {
	return Event{ID: "forgejo:delivery-1", Source: SourceForgejo, Kind: KindCIStatus,
		Time: time.Date(2026, 10, 11, 9, 30, 0, 0, time.UTC), Severity: SeverityWarn,
		Summary: "ts-cos#5 CI failed", Ticket: "TS-814", Repo: "TerraScale/ts-cos", PR: 5,
		Data: json.RawMessage(`{"state":"failure","context":"test"}`)}
}

func TestEventRoundTrip(t *testing.T) {
	for _, e := range []Event{eventsTestEvent(), {
		ID: "user:answer-1", Source: SourceUser, Kind: KindUserAnswer, Time: time.Unix(1, 0).UTC(),
		Severity: SeverityUrgent, Summary: "answered", Item: "item-1", Thread: "thread-1", Host: "beta",
		Data: json.RawMessage(`{"option":"B"}`),
	}} {
		if err := e.Validate(); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		var got Event
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, e) {
			t.Fatalf("round trip:\n got %#v\nwant %#v", got, e)
		}
	}
	if got := eventsTestEvent().Subject(); got != "cos.events.forgejo.ci.status" {
		t.Fatalf("subject %q", got)
	}
}

func TestEventValidate(t *testing.T) {
	for kind, source := range eventKinds {
		if strings.ContainsAny(string(kind)+string(source), " *>") {
			t.Fatalf("kind %q is not a subject token", kind)
		}
	}
	for name, mutate := range map[string]func(*Event){
		"missing ID":      func(e *Event) { e.ID = "" },
		"foreign ID":      func(e *Event) { e.ID = "ku:1" },
		"wrong source":    func(e *Event) { e.Source = SourceKu; e.ID = "ku:1" },
		"unknown kind":    func(e *Event) { e.Kind = "pr.exploded" },
		"zero time":       func(e *Event) { e.Time = time.Time{} },
		"bad severity":    func(e *Event) { e.Severity = "fatal" },
		"empty summary":   func(e *Event) { e.Summary = "" },
		"multiline":       func(e *Event) { e.Summary = "a\nb" },
		"long summary":    func(e *Event) { e.Summary = strings.Repeat("x", eventSummaryMax+1) },
		"control summary": func(e *Event) { e.Summary = "a\u2028b" },
		"escape summary":  func(e *Event) { e.Summary = "a\x1b[2Jb" },
		"bad host":        func(e *Event) { e.Host = "-x" },
		"long ticket":     func(e *Event) { e.Ticket = strings.Repeat("T", 65) },
		"long thread":     func(e *Event) { e.Thread = strings.Repeat("t", 257) },
		"empty answer": func(e *Event) {
			e.ID, e.Source, e.Kind, e.Item, e.Data = "user:1", SourceUser, KindUserAnswer, "item-1", json.RawMessage(`{"text":""}`)
		},
		"PR without repo":  func(e *Event) { e.Repo = "" },
		"answer sans item": func(e *Event) { e.ID, e.Source, e.Kind = "user:1", SourceUser, KindUserAnswer },
		"invalid data":     func(e *Event) { e.Data = json.RawMessage(`{`) },
		"large data":       func(e *Event) { e.Data = json.RawMessage(`"` + strings.Repeat("x", eventDataMax) + `"`) },
	} {
		e := eventsTestEvent()
		mutate(&e)
		if e.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestEventStreamsReplicateAndDeduplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	nodes := make([]*Bus, 3)
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		cfg, busNode := busTestConfig(t, host, ports[i], ports[i+3], peers)
		node, err := startBus(ctx, cfg, busNode, func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()
		nodes[i] = node
	}
	js := make([]jetstream.JetStream, 3)
	for i, node := range nodes {
		var err error
		if js[i], err = node.JetStream(); err != nil {
			t.Fatal(err)
		}
	}
	busTestReady(t, "event streams", func() bool {
		ready, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		return EnsureEventStreams(ready, js[0]) == nil
	})
	// Idempotent on every node, as each one ensures the streams on startup.
	if err := EnsureEventStreams(ctx, js[2]); err != nil {
		t.Fatal(err)
	}
	stream, err := js[1].Stream(ctx, EventsStream)
	if err != nil {
		t.Fatal(err)
	}
	if info := stream.CachedInfo(); info.Config.Replicas != 3 || info.Cluster == nil || len(info.Cluster.Replicas) != 2 {
		t.Fatalf("unexpected replica placement: %#v", info.Cluster)
	}

	e := eventsTestEvent()
	if dup, err := PublishEvent(ctx, js[0], e); err != nil || dup {
		t.Fatalf("first publish: dup=%v err=%v", dup, err)
	}
	// A redelivered webhook arriving through another node is dropped.
	if dup, err := PublishEvent(ctx, js[1], e); err != nil || !dup {
		t.Fatalf("redelivery: dup=%v err=%v", dup, err)
	}
	bad := e
	bad.Kind = "pr.exploded"
	if _, err := PublishEvent(ctx, js[0], bad); err == nil {
		t.Fatal("invalid event published")
	}

	consumer, err := js[2].CreateOrUpdateConsumer(ctx, EventsStream, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: EventsSubjects})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(2, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var got []Event
	for msg := range batch.Messages() {
		var event Event
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			t.Fatal(err)
		}
		if msg.Subject() != e.Subject() {
			t.Fatalf("subject %q", msg.Subject())
		}
		got = append(got, event)
		_ = msg.Ack()
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], e) {
		t.Fatalf("stored events %#v", got)
	}
}
