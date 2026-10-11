package cos

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func startEventsCluster(t *testing.T, ctx context.Context) []jetstream.JetStream {
	t.Helper()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	js := make([]jetstream.JetStream, len(hosts))
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
		t.Cleanup(func() { node.Close() })
		if js[i], err = node.JetStream(); err != nil {
			t.Fatal(err)
		}
	}
	busTestReady(t, "event streams", func() bool {
		ready, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		return EnsureEventStreams(ready, js[0]) == nil
	})
	return js
}

func TestLastEventsReadsNewestInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js := startEventsCluster(t, ctx)
	at := time.Date(2026, 10, 11, 9, 30, 0, 0, time.UTC)
	var published []Event
	for i, e := range []Event{
		{ID: "forgejo:ci-1", Source: SourceForgejo, Kind: KindCIStatus, Summary: "ts-cos#1 CI passed", Repo: "TerraScale/ts-cos", PR: 1},
		{ID: "forgejo:ci-2", Source: SourceForgejo, Kind: KindCIStatus, Summary: "ts-cos#2 CI failed", Repo: "TerraScale/ts-cos", PR: 2},
		{ID: "t3:run-1:completed", Source: SourceT3, Kind: KindTurnCompleted, Summary: "run 1 done", Host: "delta"},
		{ID: "forgejo:ci-3", Source: SourceForgejo, Kind: KindCIStatus, Summary: "ts-cos#3 CI pending", Repo: "TerraScale/ts-cos", PR: 3},
		{ID: "t3:run-2:completed", Source: SourceT3, Kind: KindTurnCompleted, Summary: "run 2 done", Host: "beta"},
	} {
		e.Time, e.Severity = at.Add(time.Duration(i)*time.Minute), SeverityInfo
		if dup, err := PublishEvent(ctx, js[i%3], e); err != nil || dup {
			t.Fatalf("publish %s: dup=%v err=%v", e.ID, dup, err)
		}
		published = append(published, e)
	}

	last, err := lastEvents(ctx, js[2], EventsSubjects, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(last, published[3:]) {
		t.Fatalf("last 2:\n got %#v\nwant %#v", last, published[3:])
	}

	t3, err := lastEvents(ctx, js[2], "cos.events.t3.>", 20)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(t3, []Event{published[2], published[4]}) {
		t.Fatalf("t3 events:\n got %#v", t3)
	}

	none, err := lastEvents(ctx, js[2], "cos.events.ku.>", 20)
	if err != nil {
		t.Fatal(err)
	}
	if none == nil || len(none) != 0 {
		t.Fatalf("no matches: got %#v, want empty non-nil slice", none)
	}
}

func TestEventsArgs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		filter string
		count  int
		err    bool
	}{
		{name: "defaults", filter: "cos.events.>", count: 20},
		{name: "count only", args: []string{"5"}, filter: "cos.events.>", count: 5},
		{name: "filter only", args: []string{"cos.events.t3.>"}, filter: "cos.events.t3.>", count: 20},
		{name: "filter and count", args: []string{"cos.events.forgejo.ci.status", "7"}, filter: "cos.events.forgejo.ci.status", count: 7},
		{name: "max count", args: []string{"500"}, filter: "cos.events.>", count: 500},
		{name: "count over max", args: []string{"501"}, err: true},
		{name: "zero count", args: []string{"0"}, err: true},
		{name: "negative count", args: []string{"-1"}, err: true},
		{name: "non-numeric count", args: []string{"cos.events.>", "many"}, err: true},
		{name: "filter outside cos.events", args: []string{"ku.events.>"}, err: true},
		{name: "bare prefix", args: []string{"cos.events."}, err: true},
		{name: "filter with bad count", args: []string{"cos.events.>", "0"}, err: true},
		{name: "too many args", args: []string{"cos.events.>", "5", "extra"}, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter, count, err := eventsArgs(tc.args)
			if tc.err {
				if err == nil {
					t.Fatalf("accepted %q as filter %q count %d", tc.args, filter, count)
				}
				return
			}
			if err != nil || filter != tc.filter || count != tc.count {
				t.Fatalf("got %q %d %v, want %q %d", filter, count, err, tc.filter, tc.count)
			}
		})
	}
}
