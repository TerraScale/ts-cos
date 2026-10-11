package cos

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestForgejoRegisterHooksCreatesThenAdopts(t *testing.T) {
	var mu sync.Mutex
	hooks := map[string][]forgejoHook{"/orgs/TerraScale/hooks": {{ID: 1, Config: map[string]string{"url": "https://ts-reviewer.mariogk.com/webhook"}}}}
	var calls []string
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch r.Method {
		case http.MethodGet:
			forgejoTestJSON(w, hooks[r.URL.Path])
		case http.MethodPost, http.MethodPatch:
			var body struct {
				Type   string
				Active bool
				Events []string
				Config map[string]string
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Type != "forgejo" || !body.Active ||
				body.Config["url"] != forgejoHookURL || body.Config["secret"] != "hook-secret" || body.Config["content_type"] != "json" ||
				strings.Join(body.Events, ",") != strings.Join(forgejoHookEvents, ",") {
				t.Errorf("hook body %+v %v", body, err)
			}
			if r.Method == http.MethodPost {
				hooks[r.URL.Path] = append(hooks[r.URL.Path], forgejoHook{ID: 42, Config: map[string]string{"url": forgejoHookURL}})
			}
			forgejoTestJSON(w, map[string]any{})
		}
	})
	ctx := context.Background()
	for range 2 {
		if err := f.RegisterHooks(ctx, "hook-secret"); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"GET /orgs/TerraScale/hooks", "POST /orgs/TerraScale/hooks", "GET /repos/Tools/ku/hooks", "POST /repos/Tools/ku/hooks",
		"GET /orgs/TerraScale/hooks", "PATCH /orgs/TerraScale/hooks/42", "GET /repos/Tools/ku/hooks", "PATCH /repos/Tools/ku/hooks/42"}
	if strings.Join(calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("calls:\n%s", strings.Join(calls, "\n"))
	}
}

func TestForgejoMergeability(t *testing.T) {
	f := forgejoTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/TerraScale/ts-cos/pulls" || r.URL.Query().Get("state") != "open" {
			t.Errorf("path %s", r.URL)
		}
		conflicting, clean, foreign := forgejoTestPR(7, "mariogk", "cos/ts-814-forgejo"), forgejoTestPR(8, "mariogk", "cos/ts-811-chat"), forgejoTestPR(9, "someone", "x")
		conflicting.Mergeable, foreign.Mergeable = false, false
		forgejoTestJSON(w, []forgejoPR{conflicting, clean, foreign})
	})
	events, err := f.mergeability(context.Background(), "TerraScale/ts-cos", 8, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Kind != KindPRConflict || events[0].PR != 7 || events[0].Ticket != "TS-769" ||
		events[1].Kind != KindPRMergeable || events[1].PR != 8 || events[0].Validate() != nil || events[1].Validate() != nil {
		t.Fatalf("events %#v", events)
	}
	// Without a focus PR only conflicts are reported.
	if events, _ := f.mergeability(context.Background(), "TerraScale/ts-cos", 0, time.Now()); len(events) != 1 {
		t.Fatalf("events %#v", events)
	}
}

// testdata/forgejo holds real deliveries from org hook 42, read back from COS_INGEST
// on 2026-10-11 with their signature headers removed.
func forgejoTestDelivery(t *testing.T, name string) (string, forgejoPayload) {
	t.Helper()
	data, err := os.ReadFile("testdata/forgejo/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var delivery struct {
		Headers map[string]string
		Body    forgejoPayload
	}
	if err := json.Unmarshal(data, &delivery); err != nil {
		t.Fatal(err)
	}
	return delivery.Headers["X-Forgejo-Event"], delivery.Body
}

func TestForgejoNormalizeRecordedDeliveries(t *testing.T) {
	now := time.Date(2026, 10, 11, 2, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		kinds    []EventKind
		severity Severity
		pr       int
		ticket   string
	}{
		{"push-branch", nil, "", 0, ""},
		{"pr-opened", []EventKind{KindPROpened}, SeverityInfo, 23, "TS-817"},
		{"pr-sync-conflict", []EventKind{KindPRSynchronized}, SeverityInfo, 8, "TS-811"},
		{"pr-merged", []EventKind{KindPRMerged}, SeverityInfo, 11, "TS-816"},
		{"pr-closed", []EventKind{KindPRClosed}, SeverityInfo, 22, "TS-814"},
		{"push-main", []EventKind{KindPushMain}, SeverityInfo, 0, ""},
		{"run-pr-success", []EventKind{KindCIStatus}, SeverityInfo, 22, "TS-814"},
		{"run-pr-failure", []EventKind{KindCIStatus}, SeverityWarn, 8, "TS-811"},
		{"run-schedule-failure", []EventKind{KindCIStatus}, SeverityWarn, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event, payload := forgejoTestDelivery(t, tc.name)
			events := normalizeForgejo(event, payload, now)
			if len(events) != len(tc.kinds) {
				t.Fatalf("events %#v", events)
			}
			for i, e := range events {
				if err := e.Validate(); err != nil {
					t.Fatal(err)
				}
				if e.Kind != tc.kinds[i] || e.PR != tc.pr || e.Ticket != tc.ticket || e.Repo == "" {
					t.Fatalf("event %d: %#v", i, e)
				}
			}
			if len(events) > 0 && events[0].Severity != tc.severity {
				t.Fatalf("severity %s", events[0].Severity)
			}
			// A redelivery maps to the same IDs, so JetStream drops it.
			again := normalizeForgejo(event, payload, now.Add(time.Hour))
			for i := range events {
				if again[i].ID != events[i].ID {
					t.Fatalf("unstable ID %s vs %s", again[i].ID, events[i].ID)
				}
			}
		})
	}
	// PRs by anyone else are dropped at ingest.
	event, payload := forgejoTestDelivery(t, "pr-opened")
	payload.PullRequest.User.Login = "someone"
	if events := normalizeForgejo(event, payload, now); len(events) != 0 {
		t.Fatalf("foreign PR %#v", events)
	}
}

// TestForgejoIngestToEvents runs the normalizer on two nodes of a three-node
// cluster: a recorded delivery published twice reaches cos.events exactly once.
func TestForgejoIngestToEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	nodes := make([]*Bus, 3)
	configs := make([]Config, 3)
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		cfg, node := busTestConfig(t, host, ports[i], ports[i+3], peers)
		b, err := startBus(ctx, cfg, node, func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		nodes[i], configs[i] = b, cfg
	}
	js, _ := nodes[2].JetStream()
	busTestReady(t, "event streams", func() bool {
		ready, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		return EnsureEventStreams(ready, js) == nil
	})
	forgejoRecheckDelay = 0
	defer func() { forgejoRecheckDelay = 15 * time.Second }()
	workers, stop := context.WithCancel(ctx)
	defer stop()
	for i := range 2 {
		go RunForgejoEvents(workers, configs[i], nodes[i])
	}
	busTestReady(t, "ingest consumer", func() bool {
		_, err := js.Consumer(ctx, IngestStream, forgejoIngestConsumer)
		return err == nil
	})
	data, err := os.ReadFile("testdata/forgejo/pr-opened.json")
	if err != nil {
		t.Fatal(err)
	}
	var delivery struct {
		Headers map[string]string
		Body    json.RawMessage
	}
	_ = json.Unmarshal(data, &delivery)
	for range 2 {
		msg := nats.NewMsg(IngestForgejo)
		msg.Data = delivery.Body
		for k, v := range delivery.Headers {
			msg.Header.Set(k, v)
		}
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID("forgejo:"+delivery.Headers["X-Forgejo-Delivery"])); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, EventsStream, jetstream.ConsumerConfig{FilterSubject: EventsSubjects})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(2, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var got []Event
	for msg := range batch.Messages() {
		var e Event
		_ = json.Unmarshal(msg.Data(), &e)
		got = append(got, e)
	}
	if len(got) != 1 || got[0].Kind != KindPROpened || got[0].PR != 23 {
		t.Fatalf("events %#v", got)
	}
	busTestReady(t, "delivery acknowledged", func() bool {
		stream, err := js.Stream(ctx, IngestStream)
		return err == nil && stream.CachedInfo().State.Msgs == 0
	})
}
