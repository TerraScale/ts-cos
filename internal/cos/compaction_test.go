package cos

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func withLineage(t FleetThread, rel string) FleetThread {
	t.Lineage = &struct {
		Parent   string `json:"parent"`
		Relation string `json:"rel"`
	}{"parent-thread", rel}
	return t
}

func TestCompactDecision(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	idle := FleetThread{ThreadID: "t1", Status: "idle", Title: "TS-813 work"}
	ago := func(d time.Duration) compactState { return compactState{CompactedAt: now.Add(-d)} }
	cases := []struct {
		name string
		t    FleetThread
		pct  int
		st   compactState
		want string
	}{
		{"below 70", idle, 69, compactState{}, ""},
		{"idle at 72", idle, 72, compactState{}, "compact"},
		{"active", FleetThread{ThreadID: "t1", Status: "running"}, 80, compactState{}, ""},
		{"subagent", withLineage(idle, "subagent"), 80, compactState{}, ""},
		{"fork", withLineage(idle, "fork"), 72, compactState{}, "compact"},
		{"compacted 10m ago, still 55", idle, 55, ago(10 * time.Minute), "fresh"},
		{"compacted 2m ago", idle, 80, ago(2 * time.Minute), ""},
		{"compacted 10m ago, down to 40", idle, 40, ago(10 * time.Minute), ""},
		{"already fresh", idle, 55, compactState{CompactedAt: now.Add(-10 * time.Minute), Fresh: true}, ""},
		// The spec's cooldown row: a second compaction waits 30m, and a fresh-marked thread never restarts again.
		{"cooldown not passed", idle, 75, compactState{CompactedAt: now.Add(-10 * time.Minute), Fresh: true}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := compactDecision(c.t, ContextUsage{Pct: c.pct}, c.st, now)
			if got.Kind != c.want {
				t.Fatalf("kind = %q, want %q (%+v)", got.Kind, c.want, got)
			}
		})
	}
}

// fakeCompactKV is an in-memory settings bucket; methods the beat never uses panic.
type fakeCompactKV struct {
	jetstream.KeyValue
	data map[string][]byte
}

type fakeCompactEntry struct {
	jetstream.KeyValueEntry
	value []byte
}

func (e fakeCompactEntry) Value() []byte    { return e.value }
func (e fakeCompactEntry) Revision() uint64 { return 1 }

func (k *fakeCompactKV) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return fakeCompactEntry{value: v}, nil
}

func (k *fakeCompactKV) Put(_ context.Context, key string, value []byte) (uint64, error) {
	k.data[key] = value
	return 1, nil
}

type compactCall struct {
	Host, Command string
	Args          []string
}

// fakeCompact records every bus call and every log entry a beat makes.
type fakeCompact struct {
	calls   []compactCall
	usage   map[string]ContextUsage
	threads map[string]map[string]any
	failing string // thread whose /compact fails
	kv      *fakeCompactKV
	entries []AutopilotEntry
	block   chan struct{} // when set, /compact signals started and waits for block
	started chan struct{}
}

// waitPass blocks until the compactor's background pass has finished.
func waitPass(c *Compactor) { c.passes.Wait() }

func newFakeCompact() *fakeCompact {
	return &fakeCompact{usage: map[string]ContextUsage{}, threads: map[string]map[string]any{}, kv: &fakeCompactKV{data: map[string][]byte{}}}
}

func (f *fakeCompact) compactor() *Compactor {
	return &Compactor{
		Call: func(_ context.Context, host, command string, args []string, out any) error {
			f.calls = append(f.calls, compactCall{host, command, args})
			if command == "compact" && f.block != nil {
				f.started <- struct{}{}
				<-f.block
			}
			if command == "compact" && args[0] == f.failing {
				return errors.New("compact refused")
			}
			switch o := out.(type) {
			case *ContextUsage:
				*o = f.usage[args[0]]
			case *map[string]any:
				if command == "launch" {
					*o = map[string]any{"threadId": "thread-new"}
				} else {
					*o = f.threads[args[0]]
				}
			}
			return nil
		},
		KV:      func(context.Context) (jetstream.KeyValue, error) { return f.kv, nil },
		Publish: func(_ context.Context, e AutopilotEntry) { f.entries = append(f.entries, e) },
	}
}

func (f *fakeCompact) commands() []string {
	var out []string
	for _, c := range f.calls {
		out = append(out, c.Command)
	}
	return out
}

func liveWith(threads ...FleetThread) Live {
	return Live{Fleet: []HostFleet{{Host: "delta", Threads: threads}}}
}

func TestCompactBeatCompactsIdleWorker(t *testing.T) {
	f := newFakeCompact()
	f.usage["t1"] = ContextUsage{Pct: 72}
	c := f.compactor()
	thread := FleetThread{ThreadID: "t1", Status: "idle", Title: "TS-813 work"}
	c.Beat(context.Background(), liveWith(thread)) // first beat only stores the paused set
	waitPass(c)
	focus, err := CurrentBehavior().Render("compact_focus", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ctx", "compact"}
	if got := f.commands(); !slices.Equal(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	if got := f.calls[1].Args; got[0] != "t1" || got[1] != focus || !strings.HasPrefix(focus, "Handoff compaction by COS") {
		t.Fatalf("compact args = %q", got)
	}
	var st compactState
	if _, err := kvJSON(context.Background(), f.kv, compactKey("t1"), &st); err != nil || st.CompactedAt.IsZero() || st.Fresh {
		t.Fatalf("state = %+v, %v", st, err)
	}
	if len(f.entries) != 1 || f.entries[0].Kind != "act" || f.entries[0].Action != "compact" || f.entries[0].Target != "delta/t1" {
		t.Fatalf("entries = %+v", f.entries)
	}
}

func TestCompactBeatFreshThreadOnSameWorktree(t *testing.T) {
	f := newFakeCompact()
	f.usage["t1"] = ContextUsage{Pct: 55}
	f.threads["t1"] = map[string]any{"worktreePath": "/wt/ts813", "branch": "cos/ts-813", "title": "TS-813 work", "lastReply": "Pushed PR #90"}
	if err := kvPutJSON(context.Background(), f.kv, compactKey("t1"), compactState{CompactedAt: time.Now().Add(-10 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	c := f.compactor()
	c.Beat(context.Background(), liveWith(FleetThread{ThreadID: "t1", Project: "ts-cos", Status: "idle", Title: "TS-813 work"}))
	waitPass(c)
	if want := []string{"ctx", "thread", "launch", "settle"}; !slices.Equal(f.commands(), want) {
		t.Fatalf("calls = %v, want %v", f.commands(), want)
	}
	var in map[string]any
	if err := json.Unmarshal([]byte(f.calls[2].Args[0]), &in); err != nil {
		t.Fatal(err)
	}
	model := in["modelSelection"].(map[string]any)
	ws := in["workspaceStrategy"].(map[string]any)
	if in["projectTitle"] != "ts-cos" || in["title"] != "TS-813 work" || model["model"] != "claude-opus-5-5" || model["instanceId"] != "claudeAgent" ||
		ws["type"] != "existing_worktree" || ws["worktreePath"] != "/wt/ts813" || ws["branch"] != "cos/ts-813" {
		t.Fatalf("launch input = %v", in)
	}
	if msg, _ := in["message"].(string); !strings.Contains(msg, "Pushed PR #90") || !strings.Contains(msg, "fresh thread") {
		t.Fatalf("brief = %q", msg)
	}
	if f.calls[3].Args[0] != "t1" {
		t.Fatalf("settled %v, want the old thread", f.calls[3].Args)
	}
	var st compactState
	if _, err := kvJSON(context.Background(), f.kv, compactKey("t1"), &st); err != nil || !st.Fresh {
		t.Fatalf("state = %+v, %v", st, err)
	}
	if len(f.entries) != 1 || f.entries[0].Action != "fresh-thread" || !strings.Contains(f.entries[0].Reason, "thread-new") {
		t.Fatalf("entries = %+v", f.entries)
	}
}

func TestCompactBeatResumeCompactsThenWakes(t *testing.T) {
	f := newFakeCompact()
	f.usage["t1"] = ContextUsage{Pct: 10}
	if err := kvPutJSON(context.Background(), f.kv, compactPaused, []string{"TS-813"}); err != nil {
		t.Fatal(err)
	}
	c := f.compactor()
	c.Beat(context.Background(), liveWith(FleetThread{ThreadID: "t1", Status: "idle", Title: "TS-813 work"}))
	waitPass(c)
	if want := []string{"compact", "send", "ctx"}; !slices.Equal(f.commands(), want) {
		t.Fatalf("calls = %v, want %v", f.commands(), want)
	}
	if got := f.calls[1].Args; got[1] != compactResume {
		t.Fatalf("resume text = %q", got[1])
	}
	if len(f.entries) != 1 || f.entries[0].Action != "resume" {
		t.Fatalf("entries = %+v", f.entries)
	}
}

func TestCompactBeatFailureDoesNotStopBeat(t *testing.T) {
	f := newFakeCompact()
	f.failing = "t1"
	f.usage["t1"] = ContextUsage{Pct: 80}
	f.usage["t2"] = ContextUsage{Pct: 80}
	c := f.compactor()
	c.Beat(context.Background(), liveWith(
		FleetThread{ThreadID: "t1", Status: "idle"},
		FleetThread{ThreadID: "t2", Status: "idle"},
	))
	waitPass(c)
	if want := []string{"ctx", "compact", "ctx", "compact"}; !slices.Equal(f.commands(), want) {
		t.Fatalf("calls = %v, want %v", f.commands(), want)
	}
	if len(f.entries) != 2 || f.entries[0].Kind != "error" || f.entries[0].Target != "delta/t1" || f.entries[1].Kind != "act" || f.entries[1].Target != "delta/t2" {
		t.Fatalf("entries = %+v", f.entries)
	}
}

func TestCompactBeatRunsOnePassInBackground(t *testing.T) {
	f := newFakeCompact()
	f.usage["t1"] = ContextUsage{Pct: 80}
	f.block, f.started = make(chan struct{}), make(chan struct{}, 1)
	c := f.compactor()
	thread := FleetThread{ThreadID: "t1", Status: "idle"}
	c.Beat(context.Background(), liveWith(thread)) // returns while the pass is blocked in /compact
	<-f.started
	c.Beat(context.Background(), liveWith(thread)) // a pass is in flight: this beat starts nothing
	if want := []string{"ctx", "compact"}; !slices.Equal(f.commands(), want) {
		t.Fatalf("calls while blocked = %v, want %v", f.commands(), want)
	}
	close(f.block)
	waitPass(c)
	c.Beat(context.Background(), liveWith(thread)) // the next beat after release runs a new pass
	waitPass(c)
	// The compaction just ran, so the cooldown holds the second pass to a context check.
	if want := []string{"ctx", "compact", "ctx"}; !slices.Equal(f.commands(), want) {
		t.Fatalf("calls after release = %v, want %v", f.commands(), want)
	}
}

func TestCompactTemplatesRequired(t *testing.T) {
	for _, name := range []string{"compact_focus", "fresh_brief"} {
		files := behaviorTestFiles()
		delete(files, "templates/"+name+".tmpl")
		if _, err := ParseBehavior("test", files); err == nil {
			t.Errorf("bundle without %s accepted", name)
		}
	}
}
