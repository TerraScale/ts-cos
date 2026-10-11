package cos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeGateT3 serves projections and records dispatches; a thread missing from proj is not found.
type fakeGateT3 struct {
	proj       map[string]Projection
	dispatched []map[string]any
}

func (f *fakeGateT3) Projection(_ context.Context, id string) (Projection, error) {
	p, ok := f.proj[id]
	if !ok {
		return Projection{}, &T3Error{Host: "delta", Tag: "orchestration.getThreadProjection", Types: []string{"ThreadNotFound"}}
	}
	return p, nil
}

func (f *fakeGateT3) Dispatch(_ context.Context, cmd map[string]any) (any, error) {
	f.dispatched = append(f.dispatched, cmd)
	return nil, nil
}

// modes lists the dispatch mode of every message sent so far.
func (f *fakeGateT3) modes() []string {
	var out []string
	for _, c := range f.dispatched {
		out = append(out, c["dispatchMode"].(map[string]any)["type"].(string))
	}
	return out
}

// sendTestEnv is one gate on a standalone JetStream node; the deliverer's bus calls go straight to the gate.
type sendTestEnv struct {
	t       *testing.T
	ctx     context.Context
	bus     *Bus
	gate    *Gate
	t3      *fakeGateT3
	kv      jetstream.KeyValue
	now     time.Time
	limits  LimitsState
	files   []InboxInput
	calls   [][]string
	callErr error
}

func newSendTestEnv(t *testing.T) *sendTestEnv {
	t.Helper()
	ctx := t.Context()
	bus, _ := releaseTestBus(t)
	kv, err := openSendQueue(ctx, bus)
	if err != nil {
		t.Fatal(err)
	}
	e := &sendTestEnv{t: t, ctx: ctx, bus: bus, t3: &fakeGateT3{proj: map[string]Projection{}}, kv: kv, now: time.Now().UTC()}
	e.gate = &Gate{Host: "delta",
		T3:     func(context.Context) (gateT3, error) { return e.t3, nil },
		Local:  func(context.Context, string, []string) (any, error) { return "local", nil },
		Limits: func(context.Context) (LimitsState, error) { return e.limits, nil },
		Queue:  func(context.Context) (jetstream.KeyValue, error) { return kv, nil },
		File:   func(_ context.Context, in InboxInput) error { e.files = append(e.files, in); return nil },
		Now:    func() time.Time { return e.now },
	}
	e.setHeadroom(true)
	return e
}

// setHeadroom makes the opus model free or cooling for an hour.
func (e *sendTestEnv) setHeadroom(ok bool) {
	c := LimitCredential{Provider: "claude", Account: "a", State: "active"}
	if !ok {
		c = LimitCredential{Provider: "claude", Account: "a", State: "cooling", Until: e.now.Add(time.Hour)}
	}
	e.limits = LimitsState{At: e.now, Reachable: true, Credentials: []LimitCredential{c}}
}

// advance moves the clock and keeps the CLIProxyAPI read fresh.
func (e *sendTestEnv) advance(d time.Duration) {
	e.now = e.now.Add(d)
	e.limits.At = e.now
}

func (e *sendTestEnv) addThread(id, status string) {
	var t Thread
	t.ID, t.Title = id, "Thread "+id
	t.ModelSelection.Model = "claude-opus-5-5"
	p := Projection{Thread: t}
	if status != "" {
		p.Runs = []ThreadRun{{ID: "run-1", Status: status}}
	}
	e.t3.proj[id] = p
}

// deliverer is the leader's deliverer; its bus calls reach the gate directly.
func (e *sendTestEnv) deliverer() *sendDeliverer {
	return &sendDeliverer{
		Queue:  func(context.Context) (jetstream.KeyValue, error) { return e.kv, nil },
		Limits: e.gate.Limits,
		Call: func(ctx context.Context, _, _ string, args []string, out any) error {
			e.calls = append(e.calls, args)
			if e.callErr != nil {
				return e.callErr
			}
			r, err := e.gate.Send(ctx, args[0], args[1], args[2])
			*out.(*GateResult) = r
			return err
		},
		File: e.gate.File,
		Now:  func() time.Time { return e.now },
	}
}

// drain ticks the deliverer until the queue empties: the first tick of an entry only schedules it.
func (e *sendTestEnv) drain(d *sendDeliverer) {
	e.t.Helper()
	for range 100 {
		if e.queuedCount() == 0 {
			return
		}
		if err := d.tick(e.ctx); err != nil {
			e.t.Fatal(err)
		}
		e.advance(sendTick)
	}
	e.t.Fatalf("%d entries never drained", e.queuedCount())
}

// entry returns the queue entry of thread, or false when it has none.
func (e *sendTestEnv) entry(thread string) (queuedSend, bool) {
	key, _ := sendKeys("delta", thread)
	var q queuedSend
	found, err := kvJSON(e.ctx, e.kv, key, &q)
	if err != nil {
		e.t.Fatal(err)
	}
	return q, found
}

// queuedCount counts the queue entries that exist.
func (e *sendTestEnv) queuedCount() int {
	keys, err := e.kv.Keys(e.ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return 0
	}
	if err != nil {
		e.t.Fatal(err)
	}
	n := 0
	for _, k := range keys {
		if strings.HasPrefix(k, "q.") {
			n++
		}
	}
	return n
}

func (e *sendTestEnv) mustSend(thread, text, arg string) GateResult {
	e.t.Helper()
	r, err := e.gate.Send(e.ctx, thread, text, arg)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func TestDecideSend(t *testing.T) {
	tests := []struct {
		name             string
		facts            sendFacts
		headroom, queued bool
		recent           int
		kind             string
		steer, deliver   bool
		want             string
	}{
		{"gone", sendFacts{Gone: true}, true, false, 0, "auto", true, false, "drop"},
		{"child", sendFacts{Child: true}, true, false, 0, "auto", true, false, "drop"},
		{"child answer from the user", sendFacts{Child: true}, true, false, 0, "user", true, false, "start"},
		{"no headroom", sendFacts{}, false, false, 0, "auto", true, false, "queue"},
		{"limit hold", sendFacts{LimitHold: true}, true, false, 0, "auto", true, false, "queue"},
		{"earlier message waits", sendFacts{}, true, true, 0, "auto", true, false, "queue"},
		{"deliver does not wait behind itself", sendFacts{}, true, true, 0, "auto", true, true, "start"},
		{"preparing", sendFacts{Status: "preparing"}, true, false, 0, "auto", true, false, "queue"},
		{"queued run", sendFacts{Status: "queued"}, true, false, 0, "auto", true, false, "queue"},
		{"starting run", sendFacts{Status: "starting"}, true, false, 0, "auto", true, false, "queue"},
		{"cap reached", sendFacts{}, true, false, 3, "auto", true, false, "cap"},
		{"resume counts toward cap", sendFacts{}, true, false, 3, "resume", true, false, "cap"},
		{"user answer is cap-exempt", sendFacts{}, true, false, 3, "user", true, false, "start"},
		{"running steers", sendFacts{Status: "running"}, true, false, 0, "auto", true, false, "steer"},
		{"waiting steers in deliver", sendFacts{Status: "waiting"}, true, false, 0, "auto", false, true, "steer"},
		{"running queues when asked", sendFacts{Status: "running"}, true, false, 0, "auto", false, false, "queue"},
		{"idle starts", sendFacts{}, true, false, 2, "auto", false, false, "start"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := decideSend(tt.facts, tt.headroom, tt.queued, tt.recent, tt.kind, tt.steer, tt.deliver)
			if got != tt.want {
				t.Fatalf("decideSend = %q, want %q", got, tt.want)
			}
		})
	}
}

// Burst after a mass cooldown: ten held threads get staggered slots, and each tick delivers at most one.
func TestSendBurstAfterCooldownStaggers(t *testing.T) {
	e := newSendTestEnv(t)
	e.setHeadroom(false)
	for i := range 10 {
		id := fmt.Sprintf("thread-%02d", i)
		e.addThread(id, "")
		if r := e.mustSend(id, "hello "+id, "queue"); r.Action != "queued" {
			t.Fatalf("%s: action %q, want queued", id, r.Action)
		}
		e.advance(time.Second)
	}
	e.setHeadroom(true)
	d := e.deliverer()
	if err := d.tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if len(e.calls) != 0 {
		t.Fatalf("scheduling tick delivered %d messages", len(e.calls))
	}
	var ready []time.Time
	for i := range 10 {
		q, _ := e.entry(fmt.Sprintf("thread-%02d", i))
		ready = append(ready, q.ReadyAt)
	}
	for i := 1; i < len(ready); i++ {
		if gap := ready[i].Sub(ready[i-1]); gap < sendStagger {
			t.Fatalf("ReadyAt %d is %v after the previous one, want at least %v", i, gap, sendStagger)
		}
	}
	for tick := 0; tick < 400 && e.queuedCount() > 0; tick++ {
		e.advance(sendTick)
		before := len(e.calls)
		if err := d.tick(e.ctx); err != nil {
			t.Fatal(err)
		}
		if n := len(e.calls) - before; n > 1 {
			t.Fatalf("tick %d delivered %d messages to one model", tick, n)
		}
	}
	if len(e.calls) != 10 || e.queuedCount() != 0 {
		t.Fatalf("delivered %d calls, %d entries left; want 10 and none", len(e.calls), e.queuedCount())
	}
	seen := map[string]int{}
	for _, c := range e.calls {
		seen[c[0]]++
	}
	for thread, n := range seen {
		if n != 1 {
			t.Fatalf("%s delivered %d times", thread, n)
		}
	}
}

// A running thread takes steer, never a second turn: queue and starting states hold, and queue_after_active is never sent.
func TestSendNeverStartsASecondTurn(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("running", "running")
	if r := e.mustSend("running", "more detail", ""); r.Action != "delivered" || r.Mode != "steer_active" {
		t.Fatalf("steer on a running thread = %+v", r)
	}
	if r := e.mustSend("running", "after this", "queue"); r.Action != "queued" {
		t.Fatalf("queue arg on a running thread = %+v, want queued", r)
	}
	for _, status := range []string{"preparing", "queued", "starting"} {
		e.addThread(status, status)
		if r := e.mustSend(status, "hold", ""); r.Action != "queued" {
			t.Fatalf("%s thread: action %q, want queued", status, r.Action)
		}
	}
	for _, mode := range e.t3.modes() {
		if mode == "queue_after_active" {
			t.Fatalf("gate dispatched %s", mode)
		}
	}
	if len(e.t3.dispatched) != 1 {
		t.Fatalf("dispatched %d messages, want only the steer", len(e.t3.dispatched))
	}
}

// A thread whose last run stopped on a limit gets no automatic message until CLIProxyAPI reads again.
func TestSendHeldByLimitState(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(e *sendTestEnv, th *Thread)
		want   string
		reason string
	}{
		{"cooling", func(e *sendTestEnv, _ *Thread) { e.setHeadroom(false) }, "queued", "all 1 claude credentials"},
		{"unreachable", func(e *sendTestEnv, _ *Thread) { e.limits.Reachable = false }, "queued", "CLIProxyAPI state unknown"},
		{"read from the future", func(e *sendTestEnv, _ *Thread) { e.limits.At = e.now.Add(10 * time.Minute) }, "queued", "ahead of the clock"},
		{"read older than the limit failure", func(e *sendTestEnv, th *Thread) {
			th.LastErrorClass, th.LatestRunDone = "usage_limit", e.now.Add(-time.Minute).Format(time.RFC3339Nano)
			e.limits.At = e.now.Add(-2 * time.Minute)
		}, "queued", "newer CLIProxyAPI read"},
		{"read after the limit failure", func(e *sendTestEnv, th *Thread) {
			th.LastErrorClass, th.LatestRunDone = "usage_limit", e.now.Add(-time.Minute).Format(time.RFC3339Nano)
		}, "delivered", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newSendTestEnv(t)
			e.addThread("t1", "")
			p := e.t3.proj["t1"]
			tt.setup(e, &p.Thread)
			e.t3.proj["t1"] = p
			r := e.mustSend("t1", "hi", "")
			if r.Action != tt.want {
				t.Fatalf("action %q, want %q (reason %q)", r.Action, tt.want, r.Reason)
			}
			if !strings.Contains(r.Reason, tt.reason) && tt.reason != "" {
				t.Fatalf("reason %q, want it to contain %q", r.Reason, tt.reason)
			}
			if tt.want == "queued" && len(e.t3.dispatched) != 0 {
				t.Fatalf("dispatched %d messages while held", len(e.t3.dispatched))
			}
		})
	}
}

// Deleted, settled, archived or missing threads drop the message, and the deliverer removes the entry.
func TestSendDropsGoneThreads(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(e *sendTestEnv, p *Projection)
	}{
		{"deleted", func(_ *sendTestEnv, p *Projection) { s := "2026-10-11T00:00:00Z"; p.Thread.DeletedAt = &s }},
		{"settled", func(_ *sendTestEnv, p *Projection) { s := "2026-10-11T00:00:00Z"; p.Thread.SettledAt = &s }},
		{"archived", func(_ *sendTestEnv, p *Projection) { s := "2026-10-11T00:00:00Z"; p.Thread.ArchivedAt = &s }},
		{"not found", func(e *sendTestEnv, _ *Projection) { delete(e.t3.proj, "t1") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newSendTestEnv(t)
			e.setHeadroom(false)
			e.addThread("t1", "")
			e.mustSend("t1", "held", "queue")
			e.setHeadroom(true)
			p := e.t3.proj["t1"]
			tt.mutate(e, &p)
			if _, ok := e.t3.proj["t1"]; ok {
				e.t3.proj["t1"] = p
			}
			e.drain(e.deliverer())
			if len(e.t3.dispatched) != 0 {
				t.Fatalf("dispatched %d messages to a gone thread", len(e.t3.dispatched))
			}
			if _, ok := e.entry("t1"); ok {
				t.Fatal("deliverer kept the entry of a gone thread")
			}
		})
	}
}

// Leader failover mid-queue: a fresh deliverer on the same KV delivers what the old one queued, and a stale claim keeps its message ID.
func TestSendFailoverKeepsQueueAndMessageID(t *testing.T) {
	e := newSendTestEnv(t)
	e.setHeadroom(false)
	e.addThread("t1", "")
	e.mustSend("t1", "queued by the old leader", "queue")
	old := e.deliverer()
	if err := old.tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	// The old leader claimed the entry and died before its send settled.
	id := uuid()
	q, _ := e.entry("t1")
	key, _ := sendKeys("delta", "t1")
	if err := casSend(e.ctx, e.kv, key, func(cur *queuedSend, _ bool) bool {
		*cur = q
		cur.MessageID, cur.Claimed = id, e.now.Add(-3*time.Minute)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	e.setHeadroom(true)
	if err := e.deliverer().tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if len(e.calls) != 1 || e.calls[0][2] != "deliver:"+id {
		t.Fatalf("calls = %v, want one deliver:%s", e.calls, id)
	}
	if _, ok := e.entry("t1"); ok {
		t.Fatal("entry survived its delivery")
	}
}

// Node restart: a claim whose message already reached the thread is not dispatched again.
func TestSendRestartDoesNotDuplicate(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("t1", "")
	e.setHeadroom(false)
	e.mustSend("t1", "once", "queue")
	e.setHeadroom(true)
	id := uuid()
	key, _ := sendKeys("delta", "t1")
	if err := editSend(e.ctx, e.kv, key, func(cur *queuedSend) { cur.MessageID, cur.Claimed = id, e.now.Add(-3*time.Minute) }); err != nil {
		t.Fatal(err)
	}
	p := e.t3.proj["t1"]
	p.Messages = []ThreadMessage{{ID: id, Role: "user", Text: "once"}}
	e.t3.proj["t1"] = p
	if err := e.deliverer().tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if len(e.t3.dispatched) != 0 {
		t.Fatalf("dispatched %d messages for a delivered claim", len(e.t3.dispatched))
	}
	if _, ok := e.entry("t1"); ok {
		t.Fatal("deliverer kept the entry of a delivered message")
	}
}

// A ReadyAt written by a skewed clock is reset, so the entry is not stuck for hours.
func TestSendResetsSkewedReadyAt(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("t1", "")
	e.setHeadroom(false)
	e.mustSend("t1", "hi", "queue")
	e.setHeadroom(true)
	key, _ := sendKeys("delta", "t1")
	if err := editSend(e.ctx, e.kv, key, func(cur *queuedSend) { cur.ReadyAt = e.now.Add(2 * time.Hour) }); err != nil {
		t.Fatal(err)
	}
	if err := e.deliverer().tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	q, _ := e.entry("t1")
	if d := q.ReadyAt.Sub(e.now); d <= 0 || d > sendSkewMax {
		t.Fatalf("ReadyAt is %v ahead, want within %v", d, sendSkewMax)
	}
}

// CLIProxyAPI unreachable: the send waits and the deliverer delivers nothing.
func TestSendWaitsWhileCLIProxyUnreachable(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("t1", "")
	e.limits.Reachable = false
	if r := e.mustSend("t1", "hi", ""); r.Action != "queued" {
		t.Fatalf("action %q, want queued", r.Action)
	}
	e.advance(sendTick)
	e.limits.Reachable = false
	for range 3 {
		if err := e.deliverer().tick(e.ctx); err != nil {
			t.Fatal(err)
		}
		e.advance(sendTick)
		e.limits.Reachable = false
	}
	if len(e.calls) != 0 || len(e.t3.dispatched) != 0 {
		t.Fatalf("delivered %d calls while CLIProxyAPI was unreachable", len(e.calls))
	}
	if _, ok := e.entry("t1"); !ok {
		t.Fatal("held message was dropped")
	}
}

// A worker child is dropped by the gate: only its parent advisor wakes it.
func TestSendDropsWorkerChild(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("child", "")
	p := e.t3.proj["child"]
	p.Thread.Lineage.Parent = "parent"
	e.t3.proj["child"] = p
	if r := e.mustSend("child", "wake up", ""); r.Action != "dropped" {
		t.Fatalf("action %q, want dropped", r.Action)
	}
	if len(e.t3.dispatched) != 0 {
		t.Fatal("ts-cos sent to a worker child")
	}
}

// The fourth automatic message in an hour is held as one Inbox item; answers are exempt and old history does not count.
func TestSendCapsAutomaticMessages(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("capped", "")
	for range sendCap {
		if r := e.mustSend("capped", "nudge", ""); r.Action != "delivered" {
			t.Fatalf("action %q, want delivered", r.Action)
		}
	}
	if r := e.mustSend("capped", "fourth", ""); r.Action != "held" {
		t.Fatalf("fourth automatic action %q, want held", r.Action)
	}
	if len(e.files) != 1 || e.files[0].Key != "sendcap:capped" || len(e.t3.dispatched) != sendCap {
		t.Fatalf("files %+v, dispatched %d; want one capped item and %d dispatches", e.files, len(e.t3.dispatched), sendCap)
	}
	if r := e.mustSend("capped", "answer", "user"); r.Action != "delivered" {
		t.Fatalf("user answer action %q, want delivered", r.Action)
	}
	e.advance(sendCapWindow + time.Minute)
	if r := e.mustSend("capped", "after an hour", ""); r.Action != "delivered" {
		t.Fatalf("automatic send after the window action %q, want delivered", r.Action)
	}
}

// One entry per thread: texts merge once each, and the oldest go first past the size cap.
func TestSendMergesOneEntryPerThread(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("t1", "")
	e.setHeadroom(false)
	e.mustSend("t1", "alpha", "queue")
	e.mustSend("t1", "beta", "queue")
	e.mustSend("t1", "alpha", "queue")
	if q, _ := e.entry("t1"); !slicesEqual(q.Texts, []string{"alpha", "beta"}) || e.queuedCount() != 1 {
		t.Fatalf("entry texts %v, entries %d; want [alpha beta] and one entry", q.Texts, e.queuedCount())
	}
	var q queuedSend
	mergeSend(&q, queuedSend{Host: "delta", Thread: "t1", Texts: []string{strings.Repeat("a", sendTextMax/2+1)}}, e.now)
	mergeSend(&q, queuedSend{Host: "delta", Thread: "t1", Texts: []string{strings.Repeat("b", sendTextMax/2+1)}}, e.now)
	if len(q.Texts) != 1 || !strings.HasPrefix(q.Texts[0], "b") {
		t.Fatalf("oversized merge kept %d texts, want only the newest", len(q.Texts))
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A delivery error becomes state: the entry keeps the error, one Inbox item is filed and nothing is resent.
func TestSendDeliveryErrorIsStateNotRetry(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("t1", "")
	e.setHeadroom(false)
	e.mustSend("t1", "held", "queue")
	e.setHeadroom(true)
	e.callErr = errors.New("COS node beta unavailable")
	d := e.deliverer()
	for range 4 {
		if err := d.tick(e.ctx); err != nil {
			t.Fatal(err)
		}
		e.advance(sendStagger)
	}
	if len(e.calls) != 1 {
		t.Fatalf("delivery attempts = %d, want exactly one", len(e.calls))
	}
	q, _ := e.entry("t1")
	if q.Error == "" || len(e.files) != 1 || e.files[0].Key != "sendfail:t1" {
		t.Fatalf("entry error %q, files %+v; want the error kept and one Inbox item", q.Error, e.files)
	}
}

// Launch, handoff, compact and message dispatch refuse a cooling model with the held prefix, which survives the bus.
func TestGatedCommandsRefuseCoolingModel(t *testing.T) {
	e := newSendTestEnv(t)
	e.addThread("t1", "")
	e.setHeadroom(false)
	tests := []struct {
		command string
		args    []string
	}{
		{"launch", []string{`{"modelSelection":{"model":"claude-opus-5-5"}}`}},
		{"handoff", []string{"t1", `{"model":"claude-opus-5-5"}`}},
		{"compact", []string{"t1"}},
		{"dispatch", []string{`{"type":"message.dispatch","threadId":"t1"}`}},
	}
	for _, tt := range tests {
		t.Run(tt.command, func(t *testing.T) {
			_, err := e.gate.Command(e.ctx, tt.command, tt.args)
			if !heldErr(err) {
				t.Fatalf("error %v, want a held refusal", err)
			}
			if !heldErr(errors.New(err.Error())) {
				t.Fatalf("held refusal %q lost its prefix on the bus", err)
			}
		})
	}
	if heldErr(errors.New("boom")) {
		t.Fatal("an unrelated error read as held")
	}
}

// The limit-resume is one queued entry for a top-level running ticket; children and paused tickets keep the record only.
func TestLimitFailureQueuesOneResume(t *testing.T) {
	data := `{"errorClass":"usage_limit","model":"claude-opus-5-5","title":"TS-830 gate"}`
	running := Live{Fleet: []HostFleet{{Host: "delta", Threads: []FleetThread{{ThreadID: "thread-1", Title: "TS-830 gate", Status: "running", Model: "claude-opus-5-5"}}}}}
	turn := Event{ID: "e1", Kind: KindTurnFailed, Source: SourceT3, Host: "delta", Thread: "thread-1", Ticket: "TS-830", Summary: "run failed", Data: []byte(data)}

	decisions := Classify(turn, running, nil, WakeMemory{}, map[string]bool{})
	if len(decisions) != 2 || decisions[0].Rule != "limit-failure" || decisions[1].Rule != "limit-resume" || decisions[1].Do == nil {
		t.Fatalf("decisions = %+v, want the record and one limit-resume", decisions)
	}
	for _, d := range decisions {
		if d.Wake != "" {
			t.Fatalf("%s carries a wake; a limit must never wake the thread", d.Rule)
		}
	}
	if got := Classify(turn, running, nil, WakeMemory{}, map[string]bool{"TS-830": true}); len(got) != 1 || got[0].Rule != "limit-failure" {
		t.Fatalf("paused ticket decisions = %+v, want the record only", got)
	}
	child := running
	child.Fleet = []HostFleet{{Host: "delta", Threads: []FleetThread{{ThreadID: "thread-1", Title: "TS-830 gate", Status: "running", Lineage: &struct {
		Parent   string `json:"parent"`
		Relation string `json:"rel"`
	}{Parent: "root"}}}}}
	if got := Classify(turn, child, nil, WakeMemory{}, map[string]bool{}); len(got) != 1 || got[0].Rule != "limit-failure" {
		t.Fatalf("child decisions = %+v, want the record only", got)
	}
	if got := Classify(turn, Live{}, nil, WakeMemory{}, map[string]bool{}); len(got) != 1 {
		t.Fatalf("idle thread decisions = %+v, want the record only", got)
	}

	bus, _ := releaseTestBus(t)
	for range 2 {
		if err := queueLimitResume(t.Context(), bus, "delta", "thread-1", "TS-830 gate", "claude-opus-5-5"); err != nil {
			t.Fatal(err)
		}
	}
	kv, err := openSendQueue(t.Context(), bus)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := sendKeys("delta", "thread-1")
	var q queuedSend
	if _, err := kvJSON(t.Context(), kv, key, &q); err != nil {
		t.Fatal(err)
	}
	if q.Kind != "resume" || len(q.Texts) != 1 || strings.Contains(q.Texts[0], "continue") || !strings.Contains(q.Texts[0], "claude-opus-5-5") {
		t.Fatalf("resume entry = %+v, want one resume without the word continue", q)
	}
	if d := q.NotBefore.Sub(time.Now()); d < limitsPoll || d > 3*limitsPoll {
		t.Fatalf("resume NotBefore is %v ahead, want about two limit polls", d)
	}
}

// A held compaction or launch is a record with its reason, not an error; the old thread is left alone.
func TestCompactorRecordsHeldRefusal(t *testing.T) {
	var got []AutopilotEntry
	c := &Compactor{Publish: func(_ context.Context, e AutopilotEntry) { got = append(got, e) }}
	held := OperationError(sendHeld + "no compaction on a cooling model: all 1 claude credentials")
	c.report(t.Context(), "delta", "t1", "compact", "idle at 80% context", held)
	c.report(t.Context(), "delta", "t1", "compact", "", errors.New("boom"))
	if len(got) != 2 || got[0].Kind != "record" || got[0].Reason != held.Error() || got[1].Kind != "error" {
		t.Fatalf("entries = %+v, want a held record then an error", got)
	}
}
