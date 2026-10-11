package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// routeLimits is a CLIProxyAPI read at now. The claude (Opus) and codex (Sol) credentials are free or cooling for an hour;
// haiku cools only the claude worker model.
func routeLimits(now time.Time, opus, sol, haiku bool) LimitsState {
	claude := LimitCredential{Provider: "claude", Account: "a", State: "active"}
	if !opus {
		claude.State, claude.Until = "cooling", now.Add(time.Hour)
	}
	if !haiku {
		claude.Models = []LimitModel{{Model: "claude-haiku-5-5", State: "cooling", Until: now.Add(time.Hour)}}
	}
	codex := LimitCredential{Provider: "codex", Account: "b", State: "active"}
	if !sol {
		codex.State, codex.Until = "cooling", now.Add(time.Hour)
	}
	return LimitsState{At: now, Reachable: true, Credentials: []LimitCredential{claude, codex}}
}

// setRoom sets the test env's limits: opus and sol are free or cooling; the Opus worker is free.
func (e *sendTestEnv) setRoom(opus, sol bool) {
	e.limits = routeLimits(e.now, opus, sol, true)
}

func TestRouteLaunch(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name      string
		pair      string
		ticket    string
		opus      bool
		sol       bool
		haiku     bool
		heldFor   time.Duration
		noP0      bool
		wantRoute string
		wantWhy   string
		wantHeld  bool
	}{
		{name: "sol with headroom", pair: "sol", ticket: "TS-900", opus: true, sol: true, haiku: true, wantRoute: "sol"},
		{name: "sol without headroom waits", pair: "sol", ticket: "TS-900", opus: true, sol: false, haiku: true, wantHeld: true, wantWhy: "routine work waits for Sol headroom"},
		{name: "opus with headroom", pair: "opus", ticket: "TS-900", opus: true, sol: true, haiku: true, wantRoute: "opus"},
		{name: "opus without, not P0, waits", pair: "opus", ticket: "TS-900", opus: false, sol: true, haiku: true, heldFor: 40 * time.Minute, wantHeld: true, wantWhy: "important work waits for Opus headroom"},
		{name: "opus without, P0 held 10 min, waits", pair: "opus", ticket: "TS-830", opus: false, sol: true, haiku: true, heldFor: 10 * time.Minute, wantHeld: true, wantWhy: "important work waits for Opus headroom"},
		{name: "opus without, P0 held 31 min, Sol ok, moves", pair: "opus", ticket: "TS-830", opus: false, sol: true, haiku: true, heldFor: 31 * time.Minute, wantRoute: "sol", wantWhy: "moved to Sol"},
		{name: "opus without, P0 held 31 min, Sol limited, waits", pair: "opus", ticket: "TS-830", opus: false, sol: false, haiku: true, heldFor: 31 * time.Minute, wantHeld: true, wantWhy: "important work waits for Opus headroom"},
		{name: "opus worker cooling is held", pair: "opus", ticket: "TS-900", opus: true, sol: true, haiku: false, wantHeld: true, wantWhy: "no worker spawn on a cooling model"},
		{name: "empty priority names no P0", pair: "opus", ticket: "TS-830", opus: false, sol: true, haiku: true, heldFor: 31 * time.Minute, noP0: true, wantHeld: true, wantWhy: "important work waits for Opus headroom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pol := CurrentBehavior().Models
			pol.Priority = []string{"TS-830"}
			if tt.noP0 {
				pol.Priority = nil
			}
			var heldSince time.Time
			if tt.heldFor > 0 {
				heldSince = now.Add(-tt.heldFor)
			}
			route, why, held := routeLaunch(pol, tt.pair, tt.ticket, routeLimits(now, tt.opus, tt.sol, tt.haiku), now, heldSince)
			if held != tt.wantHeld || route != tt.wantRoute {
				t.Fatalf("route %q held %v, want %q held %v (why %q)", route, held, tt.wantRoute, tt.wantHeld, why)
			}
			if !strings.Contains(why, tt.wantWhy) {
				t.Fatalf("why %q, want it to contain %q", why, tt.wantWhy)
			}
		})
	}
}

// A launch that names its pair is checked against that pair's headroom, not the model it has not yet been given.
func TestGateLaunchRefusesHeldPair(t *testing.T) {
	e := newSendTestEnv(t)
	e.setRoom(false, true)
	_, err := e.gate.Command(e.ctx, "launch", []string{`{"pair":"opus","title":"TS-900 routing"}`})
	if !heldErr(err) {
		t.Fatalf("error %v, want a held refusal", err)
	}
}

func TestGateLaunchHeldSinceIsWrittenOnceAndCleared(t *testing.T) {
	e := newSendTestEnv(t)
	e.setRoom(false, true)
	key := sendMarkerKey("held.", "TS-830")
	launch := []string{`{"pair":"opus","title":"TS-830 gate"}`}
	if _, err := e.gate.Command(e.ctx, "launch", launch); !heldErr(err) {
		t.Fatalf("first launch error %v, want a held refusal", err)
	}
	var first time.Time
	if found, _ := kvJSON(e.ctx, e.kv, key, &first); !found {
		t.Fatal("no held marker after the first refusal")
	}
	e.advance(10 * time.Minute)
	if _, err := e.gate.Command(e.ctx, "launch", launch); !heldErr(err) {
		t.Fatalf("second launch error %v, want a held refusal", err)
	}
	var second time.Time
	if _, err := kvJSON(e.ctx, e.kv, key, &second); err != nil || !second.Equal(first) {
		t.Fatalf("held marker moved from %v to %v (%v), want it written once", first, second, err)
	}
	e.setRoom(true, true)
	if _, err := e.gate.Command(e.ctx, "launch", launch); err != nil {
		t.Fatalf("launch with headroom: %v", err)
	}
	if found, _ := kvJSON(e.ctx, e.kv, key, &first); found {
		t.Fatal("held marker survived a successful launch")
	}
}

// A P0 held on Opus for over 30 min launches on Sol with the brief line and one Inbox item.
func TestGateLaunchMovesStalledP0ToSol(t *testing.T) {
	e := newSendTestEnv(t)
	pol := CurrentBehavior().Models
	pol.Priority = []string{"TS-830"}
	withPriority(t, pol)
	e.setRoom(false, true)
	launch := []string{`{"pair":"opus","title":"TS-830 gate","message":"Build the gate."}`}
	if _, err := e.gate.Command(e.ctx, "launch", launch); !heldErr(err) {
		t.Fatalf("first launch error %v, want a held refusal", err)
	}
	e.advance(31 * time.Minute)
	var got string
	e.gate.Local = func(_ context.Context, _ string, args []string) (any, error) {
		got = args[0]
		return "local", nil
	}
	if _, err := e.gate.Command(e.ctx, "launch", launch); err != nil {
		t.Fatalf("P0 launch: %v", err)
	}
	var input map[string]any
	if err := json.Unmarshal([]byte(got), &input); err != nil {
		t.Fatal(err)
	}
	routed, err := launchInput(pol, input)
	if err != nil {
		t.Fatal(err)
	}
	sel, _ := routed["modelSelection"].(map[string]any)
	if sel["model"] != "gpt-6.1-sol" {
		t.Fatalf("model %v, want the Sol advisor", sel["model"])
	}
	msg, _ := routed["message"].(string)
	for _, want := range []string{"Build the gate.", p0MovedLine, pol.BriefLine("sol")} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q lacks %q", msg, want)
		}
	}
	if n := strings.Count(msg, "ADVISOR decides"); n != 1 {
		t.Fatalf("brief line appears %d times, want once", n)
	}
	if len(e.files) != 1 || e.files[0].Key != "p0-sol:TS-830" {
		t.Fatalf("inbox %+v, want one p0-sol item", e.files)
	}
	if found, _ := kvJSON(e.ctx, e.kv, sendMarkerKey("held.", "TS-830"), new(time.Time)); found {
		t.Fatal("held marker survived the P0 move")
	}
}

// p0Calls fakes the bus calls a stall-step tick makes: thread info per thread, and the launches and settles it records.
type p0Calls struct {
	infos     map[string]map[string]any
	launchErr error // what launch returns after recording the call
	settleErr error // what settle returns after recording the call
	sends     []string
	n         int // bus calls made
	launches  []string
	settles   []string
}

func (c *p0Calls) call(_ context.Context, _, cmd string, args []string, out any) error {
	c.n++
	switch cmd {
	case "thread":
		*out.(*map[string]any) = c.infos[args[0]]
	case "launch":
		c.launches = append(c.launches, args[0])
		if c.launchErr != nil {
			return c.launchErr
		}
		*out.(*map[string]any) = map[string]any{"threadId": fmt.Sprintf("succ-%d", len(c.launches))}
	case "settle":
		c.settles = append(c.settles, args[0])
		return c.settleErr
	case "send":
		c.sends = append(c.sends, args[0])
	default:
		return errors.New("unexpected " + cmd)
	}
	return nil
}

// threadInfo is the thread command's answer for one thread; a parent makes it a child.
func threadInfo(title, branch, parent string) map[string]any {
	return map[string]any{"title": title, "branch": branch, "projectId": "proj-1", "worktreePath": "/wt/" + branch,
		"lastReply": "last reply", "lineage": map[string]any{"parentThreadId": parent}}
}

// stallEnv queues one held message each on a P0 thread, a non-P0 thread and a child of the P0, all on Opus, 31 min ago.
func stallEnv(t *testing.T) (*sendTestEnv, *p0Calls) {
	t.Helper()
	e := newSendTestEnv(t)
	pol := CurrentBehavior().Models
	pol.Priority = []string{"TS-830"}
	withPriority(t, pol)
	for _, id := range []string{"p0", "other", "child"} {
		e.addThread(id, "")
	}
	e.setTitle("p0", "TS-830 gate")
	e.setTitle("other", "TS-900 gate")
	e.setTitle("child", "TS-830 child")
	e.setRoom(false, true)
	e.mustSend("p0", "held text", "queue")
	e.mustSend("other", "other text", "queue")
	e.mustSend("child", "child text", "queue")
	e.advance(31 * time.Minute)
	return e, &p0Calls{infos: map[string]map[string]any{
		"p0":    threadInfo("TS-830 gate", "cos/ts-830", ""),
		"other": threadInfo("TS-900 gate", "cos/ts-900", ""),
		"child": threadInfo("TS-830 child", "cos/ts-830-child", "p0"),
	}}
}

// setTitle renames a test thread; a queue entry takes its title from the thread when it is queued.
func (e *sendTestEnv) setTitle(id, title string) {
	p := e.t3.proj[id]
	p.Thread.Title = title
	e.t3.proj[id] = p
}

// Cheap checks come before any bus call: a non-P0 entry, or a thread whose move marker exists, costs no call.
func TestStalledP0SkipsWithoutBusCall(t *testing.T) {
	e := newSendTestEnv(t)
	pol := CurrentBehavior().Models
	pol.Priority = []string{"TS-830"}
	withPriority(t, pol)
	e.addThread("other", "")
	e.setTitle("other", "TS-900 gate")
	e.addThread("p0", "")
	e.setTitle("p0", "TS-830 gate")
	e.setRoom(false, true)
	e.mustSend("other", "other text", "queue")
	e.mustSend("p0", "held text", "queue")
	e.advance(31 * time.Minute)
	if _, err := putOnce(e.ctx, e.kv, sendMarkerKey("p0moved.", "p0"), e.now); err != nil {
		t.Fatal(err)
	}
	calls := &p0Calls{infos: map[string]map[string]any{"other": threadInfo("TS-900 gate", "cos/ts-900", "")}}
	d := e.deliverer()
	d.Call = calls.call
	if err := d.tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if calls.n != 0 {
		t.Fatalf("%d bus calls, want none", calls.n)
	}
	if _, found := e.entry("p0"); !found {
		t.Fatal("p0 entry was removed")
	}
}

// A launch that fails after the marker is final: one Inbox item, and later ticks make no bus call.
func TestStalledP0LaunchFailureFilesOnce(t *testing.T) {
	e, calls := stallEnv(t)
	calls.launchErr = errors.New("bus timeout")
	d := e.deliverer()
	d.Call = calls.call
	if err := d.tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if len(calls.launches) != 1 || len(calls.settles) != 0 {
		t.Fatalf("launches %d settles %d, want one failed launch and no settle", len(calls.launches), len(calls.settles))
	}
	if len(e.files) != 1 || e.files[0].Key != "p0-sol-failed:TS-830" || !strings.Contains(e.files[0].Title, "bus timeout") {
		t.Fatalf("inbox %+v, want one p0-sol-failed item with the cause", e.files)
	}
	before := calls.n
	// The child carries a P0 title, so it still makes its lineage call; the p0 entry must make none.
	e.advance(sendTick)
	d = e.deliverer()
	d.Call = calls.call
	if err := d.tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if calls.n != before+1 || len(e.files) != 1 || len(calls.launches) != 1 {
		t.Fatalf("later tick made %d bus calls, %d launches and %d inbox items, want only the child's lineage call", calls.n-before, len(calls.launches), len(e.files))
	}
}

func TestStalledP0MovesOnceToSol(t *testing.T) {
	e, calls := stallEnv(t)
	d := e.deliverer()
	d.Call = calls.call
	if err := d.tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if len(calls.launches) != 1 || !slicesEqual(calls.settles, []string{"p0"}) {
		t.Fatalf("launches %d settles %v, want one Sol launch and p0 settled", len(calls.launches), calls.settles)
	}
	var in map[string]any
	if err := json.Unmarshal([]byte(calls.launches[0]), &in); err != nil {
		t.Fatal(err)
	}
	sel, _ := in["modelSelection"].(map[string]any)
	msg, _ := in["message"].(string)
	if sel["model"] != "gpt-6.1-sol" || !strings.Contains(msg, p0MovedLine) || !strings.Contains(msg, "held text") {
		t.Fatalf("successor input %v, want the Sol advisor, the move line and the held text", in)
	}
	if _, found := e.entry("p0"); found {
		t.Fatal("p0 entry survived the move")
	}
	if len(e.files) != 1 || e.files[0].Key != "p0-sol:TS-830" {
		t.Fatalf("inbox %+v, want one p0-sol item", e.files)
	}
	if _, found := e.entry("other"); !found {
		t.Fatal("non-P0 entry was touched")
	}
	if _, found := e.entry("child"); !found {
		t.Fatal("child entry was touched")
	}

	// The P0 is queued again and a fresh leader ticks: the marker stops a second successor.
	e.mustSend("p0", "more text", "queue")
	e.advance(31 * time.Minute)
	if err := e.deliverer().tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if len(calls.launches) != 1 || len(calls.settles) != 1 {
		t.Fatalf("launches %d settles %d after a second tick, want still one", len(calls.launches), len(calls.settles))
	}
}

// A settle that fails after the successor started still drops the entry and files one item; later ticks never send to the old thread.
func TestStalledP0SettleFailureDropsEntryOnce(t *testing.T) {
	e, calls := stallEnv(t)
	calls.settleErr = errors.New("settle timeout")
	d := e.deliverer()
	d.Call = calls.call
	if err := d.tick(e.ctx); err != nil {
		t.Fatal(err)
	}
	if _, found := e.entry("p0"); found {
		t.Fatal("p0 entry survived a started successor")
	}
	if len(e.files) != 1 || e.files[0].Key != "p0-sol-failed:TS-830" || !strings.Contains(e.files[0].Title, "successor succ-1 started") {
		t.Fatalf("inbox %+v, want one p0-sol-failed item naming the successor", e.files)
	}
	e.setRoom(true, true)
	for range 3 {
		e.advance(sendTick)
		d = e.deliverer()
		d.Call = calls.call
		if err := d.tick(e.ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, thread := range calls.sends {
		if thread == "p0" {
			t.Fatal("a later tick sent to the old thread")
		}
	}
	if len(e.files) != 1 {
		t.Fatalf("%d inbox items after later ticks, want one", len(e.files))
	}
}

func TestStalledP0UntouchedWhenOpusOrSolIsOpen(t *testing.T) {
	tests := []struct {
		name string
		opus bool
		sol  bool
	}{
		{name: "opus headroom is back", opus: true, sol: true},
		{name: "sol has no headroom", opus: false, sol: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, calls := stallEnv(t)
			e.setRoom(tt.opus, tt.sol)
			d := e.deliverer()
			d.Call = calls.call
			if err := d.tick(e.ctx); err != nil {
				t.Fatal(err)
			}
			if len(calls.launches) != 0 || len(calls.settles) != 0 {
				t.Fatalf("launches %v settles %v, want none", calls.launches, calls.settles)
			}
			if _, found := e.entry("p0"); !found {
				t.Fatal("p0 entry was removed")
			}
		})
	}
}
