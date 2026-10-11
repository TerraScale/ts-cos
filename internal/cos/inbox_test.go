package cos

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type inboxTestNode struct {
	bus   *Bus
	store *Store
	inbox *Inbox
}

func (n inboxTestNode) item(t *testing.T, id string) (InboxItem, bool) {
	t.Helper()
	open, history, err := n.store.Inbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range append(open, history...) {
		if item.ID == id {
			return item, true
		}
	}
	return InboxItem{}, false
}

func waitInbox(t *testing.T, nodes []inboxTestNode, id string, check func(InboxItem) bool) {
	t.Helper()
	for _, n := range nodes {
		busTestReady(t, "inbox item "+id+" on "+n.inbox.host, func() bool {
			item, ok := n.item(t, id)
			return ok && check(item)
		})
	}
}

func TestInboxClusterLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	var mu sync.Mutex
	var sent [][]string
	failSend := map[string]bool{"thread-retry": true}
	nodes := make([]inboxTestNode, 3)
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		cfg, node := busTestConfig(t, host, ports[i], ports[i+3], peers)
		bus, err := startBus(ctx, cfg, node, func(_ context.Context, command string, args []string) (any, error) {
			if command != "send" {
				return nil, nil
			}
			mu.Lock()
			defer mu.Unlock()
			if failSend[args[0]] {
				return nil, errors.New("T3 unavailable")
			}
			sent = append(sent, append([]string{host}, args...))
			return map[string]any{}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bus.Close)
		store, err := OpenStore(filepath.Join(cfg.Home, "cos.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		nodes[i] = inboxTestNode{bus: bus, store: store}
	}
	runCtx, stop := context.WithCancel(ctx)
	var running sync.WaitGroup
	defer func() { stop(); running.Wait() }()
	ready := make(chan *Inbox, 3)
	for i := range nodes {
		running.Go(func() { RunInbox(runCtx, nodes[i].bus, hosts[i], nodes[i].store, func(in *Inbox) { ready <- in }) })
	}
	for range nodes {
		select {
		case in := <-ready:
			for i := range nodes {
				if hosts[i] == in.host {
					nodes[i].inbox = in
				}
			}
		case <-ctx.Done():
			t.Fatal("inbox never became ready")
		}
	}
	delta, beta, dev := nodes[0], nodes[1], nodes[2]

	// Filed on one node, the item shows on every node's SQLite mirror.
	q, err := delta.inbox.Add(ctx, InboxInput{Title: "Which deploy window?", Context: "Prod deploy of ts-cos.", Options: []string{"Tonight", "Tomorrow"}, Ticket: "ts-815"})
	if err != nil {
		t.Fatal(err)
	}
	waitInbox(t, nodes, q.ID, func(i InboxItem) bool { return i.Status == "open" && i.Ticket == "TS-815" })

	// The same question again updates that item instead of filing a second one.
	again, err := beta.inbox.Add(ctx, InboxInput{Title: "which  deploy window?", Context: "Now with a rollback plan.", Options: []string{"Tonight", "Tomorrow"}, Ticket: "TS-815"})
	if err != nil || again.ID != q.ID || !again.Created.Equal(q.Created) {
		t.Fatalf("dedup: %+v %v", again, err)
	}
	waitInbox(t, nodes, q.ID, func(i InboxItem) bool { return i.Context == "Now with a rollback plan." })
	if open, _, _ := dev.store.Inbox(ctx); len(open) != 1 {
		t.Fatalf("open items after dedup: %+v", open)
	}

	// Answering on one node closes it everywhere; a second answer elsewhere loses.
	if _, err := beta.inbox.Answer(ctx, q.ID, AnswerData{Option: "Never"}); err == nil {
		t.Fatal("answer outside the options accepted")
	}
	if _, err := beta.inbox.Answer(ctx, q.ID, AnswerData{Option: "Tonight", Text: "after 22:00"}); err != nil {
		t.Fatal(err)
	}
	waitInbox(t, nodes, q.ID, func(i InboxItem) bool {
		return i.Status == "answered" && i.ClosedOn == "beta" && i.Answer != nil && i.Answer.Option == "Tonight"
	})
	if _, err := dev.inbox.Answer(ctx, q.ID, AnswerData{Text: "late"}); !errors.Is(err, errInboxClosed) {
		t.Fatalf("second answer: %v", err)
	}
	// An answered question is not asked again.
	if item, err := delta.inbox.Add(ctx, InboxInput{Title: "Which deploy window?", Ticket: "TS-815"}); err != nil || item.Status != "answered" {
		t.Fatalf("re-ask: %+v %v", item, err)
	}

	// A newer item supersedes an older one with a note.
	old, err := delta.inbox.Add(ctx, InboxInput{Kind: "approval", Title: "Spend $40 on a runner", Urgent: true})
	if err != nil || len(old.Options) != 2 {
		t.Fatalf("approval: %+v %v", old, err)
	}
	newer, err := delta.inbox.Add(ctx, InboxInput{Kind: "approval", Title: "Spend $60 on a bigger runner", Supersedes: []string{old.ID}})
	if err != nil {
		t.Fatal(err)
	}
	waitInbox(t, nodes, old.ID, func(i InboxItem) bool {
		return i.Status == "superseded" && i.Note == "Superseded by: Spend $60 on a bigger runner"
	})
	if _, err := dev.inbox.Dismiss(ctx, newer.ID); err != nil {
		t.Fatal(err)
	}

	// FYI items, such as a deploy rollback, can only be dismissed.
	fyi, err := delta.inbox.Add(ctx, InboxInput{Kind: "fyi", Title: "Deploy rolled back: ts-cos", Source: "deploy", Key: "deploy:r1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := beta.inbox.Answer(ctx, fyi.ID, AnswerData{Text: "ok"}); err == nil || errors.Is(err, errInboxClosed) {
		t.Fatalf("fyi answer: %v", err)
	}
	if _, err := beta.inbox.Dismiss(ctx, fyi.ID); err != nil {
		t.Fatal(err)
	}

	// Worker NEEDS-USER blocks become items; answers are forwarded to the worker.
	live := Live{
		At: time.Now().UTC().Format(time.RFC3339),
		Asks: []Ask{
			{Host: "delta", ThreadID: "thread-1", Title: "TS-815 worker", Kind: "marker", Question: "Pick a database?", Options: []string{"SQLite", "Postgres"}, Why: "Schema work waits."},
			{Host: "delta", ThreadID: "thread-retry", Title: "TS-815 retry", Kind: "marker", Question: "Retry me?"},
			{Host: "beta", ThreadID: "thread-busy", Title: "TS-811", Kind: "marker", Question: "Still asking?"},
			{Host: "beta", ThreadID: "thread-idle", Title: "TS-811", Kind: "marker", Question: "Gone soon?"},
		},
		Fleet: []HostFleet{
			{Host: "delta", Threads: []FleetThread{{ThreadID: "thread-1", Status: "completed"}, {ThreadID: "thread-retry", Status: "completed"}}},
			{Host: "beta", Threads: []FleetThread{{ThreadID: "thread-busy", Status: "running"}, {ThreadID: "thread-idle", Status: "completed"}}},
		},
	}
	if err := dev.inbox.SyncAsks(ctx, live, nil); err != nil {
		t.Fatal(err)
	}
	worker := InboxInput{Source: "worker", Host: "delta", Thread: "thread-1", Title: "Pick a database?"}
	w, _ := worker.item(time.Now())
	waitInbox(t, nodes, w.ID, func(i InboxItem) bool {
		return i.Source == "worker" && i.Ticket == "TS-815" && len(i.Options) == 2 && i.Context == "Schema work waits."
	})
	if _, err := delta.inbox.Answer(ctx, w.ID, AnswerData{Option: "SQLite"}); err != nil {
		t.Fatal(err)
	}
	waitInbox(t, nodes, w.ID, func(i InboxItem) bool { return i.Forwarded })
	mu.Lock()
	if len(sent) != 1 || sent[0][0] != "delta" || sent[0][1] != "thread-1" || !strings.Contains(sent[0][2], "USER ANSWER") || !strings.Contains(sent[0][2], "SQLite") {
		t.Fatalf("forwarded %q", sent)
	}
	mu.Unlock()

	// A failed forward is kept visible and retried by the reconciler.
	retry := InboxInput{Source: "worker", Host: "delta", Thread: "thread-retry", Title: "Retry me?"}
	r, _ := retry.item(time.Now())
	if item, err := beta.inbox.Answer(ctx, r.ID, AnswerData{Text: "yes"}); err != nil || item.Forwarded || item.ForwardError == "" {
		t.Fatalf("failed forward: %+v %v", item, err)
	}
	mu.Lock()
	failSend["thread-retry"] = false
	mu.Unlock()
	waitInbox(t, nodes[:1], r.ID, func(i InboxItem) bool { return !i.Forwarded && i.ForwardError != "" })
	if err := delta.inbox.reconcile(ctx, delta.store); err != nil {
		t.Fatal(err)
	}
	waitInbox(t, nodes, r.ID, func(i InboxItem) bool { return i.Forwarded && i.ForwardError == "" })

	// Questions an idle worker stopped asking are superseded; busy workers keep theirs.
	live.Asks = nil
	open, _, _ := beta.store.Inbox(ctx)
	if err := beta.inbox.SyncAsks(ctx, live, open); err != nil {
		t.Fatal(err)
	}
	idle, _ := InboxInput{Source: "worker", Host: "beta", Thread: "thread-idle", Title: "Gone soon?"}.item(time.Now())
	busy, _ := InboxInput{Source: "worker", Host: "beta", Thread: "thread-busy", Title: "Still asking?"}.item(time.Now())
	waitInbox(t, nodes, idle.ID, func(i InboxItem) bool { return i.Status == "superseded" })
	if item, _ := beta.item(t, busy.ID); item.Status != "open" {
		t.Fatalf("busy worker item %+v", item)
	}

	// Every state change reached cos.events.> exactly once.
	consumer, err := delta.inbox.js.OrderedConsumer(ctx, EventsStream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[EventKind]int{}
	var answer Event
	for {
		msg, err := consumer.Next(jetstream.FetchMaxWait(time.Second))
		if err != nil {
			break
		}
		var e Event
		if err := json.Unmarshal(msg.Data(), &e); err != nil {
			t.Fatal(err)
		}
		kinds[e.Kind]++
		if e.Kind == KindUserAnswer && e.Item == q.ID {
			answer = e
		}
	}
	// Added: q, old, newer, fyi, 4 worker asks. Answered: q, worker, retry. Closed: old, newer, fyi, idle.
	if kinds[KindInboxItemAdded] != 8 || kinds[KindUserAnswer] != 3 || kinds[KindInboxClosed] != 4 {
		t.Fatalf("event counts %v", kinds)
	}
	var data AnswerData
	if err := json.Unmarshal(answer.Data, &data); err != nil || data.Option != "Tonight" || data.Text != "after 22:00" || answer.Ticket != "TS-815" {
		t.Fatalf("answer event %+v %+v", answer, data)
	}

	// A restarted node with an empty database catches up from the bucket.
	fresh, err := OpenStore(filepath.Join(t.TempDir(), "cos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err := fresh.SaveInboxItem(ctx, InboxItem{ID: "ffffffffffffffff", Status: "open"}); err != nil {
		t.Fatal(err)
	}
	mirrorCtx, stopMirror := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- dev.inbox.Mirror(mirrorCtx, fresh) }()
	wantOpen, wantHistory, _ := delta.store.Inbox(ctx)
	busTestReady(t, "fresh mirror catch-up", func() bool {
		open, history, err := fresh.Inbox(ctx)
		return err == nil && len(open) == len(wantOpen) && len(history) == len(wantHistory)
	})
	stopMirror()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
