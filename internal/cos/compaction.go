package cos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Fixed values (user decision: no knobs).
const (
	compactAtPct    = 70               // compact an idle worker at or above this context use
	freshAtPct      = 50               // still at or above this after compacting -> fresh thread
	compactSettle   = 5 * time.Minute  // wait after a compaction before judging it
	compactCooldown = 30 * time.Minute // at most one compaction per thread per window
	compactPaused   = "compact.paused" // the paused set the previous beat saw
	compactResume   = "RESUME: the ticket is running again. Continue from your handoff."
)

// compactState is per worker thread, stored under compactKey in COS_AUTOPILOT.
type compactState struct {
	CompactedAt time.Time `json:"compactedAt"`
	Fresh       bool      `json:"fresh"`
}

type compactAction struct{ Kind, Host, Thread, Reason string }

// freshBrief is the data of the fresh_brief template.
type freshBrief struct{ LastReply string }

// compactable: only idle top-level threads; subagents and running threads are never touched.
func compactable(t FleetThread) bool {
	return !active(t.Status) && (t.Lineage == nil || t.Lineage.Relation == "fork")
}

// compactDecision is pure: the thread, its context use and its stored state decide the action.
func compactDecision(t FleetThread, usage ContextUsage, st compactState, now time.Time) compactAction {
	act := compactAction{Thread: t.ThreadID}
	if !compactable(t) {
		return act
	}
	switch {
	case usage.Pct >= compactAtPct && now.Sub(st.CompactedAt) >= compactCooldown:
		act.Kind, act.Reason = "compact", fmt.Sprintf("idle at %d%% context", usage.Pct)
	case !st.CompactedAt.IsZero() && !st.Fresh && now.Sub(st.CompactedAt) >= compactSettle && usage.Pct >= freshAtPct:
		act.Kind, act.Reason = "fresh", fmt.Sprintf("still at %d%% context after compacting", usage.Pct)
	}
	return act
}

// Compactor is the worker step of each heartbeat; only the leader runs it.
type Compactor struct {
	Call    func(ctx context.Context, host, command string, args []string, out any) error
	KV      func(ctx context.Context) (jetstream.KeyValue, error)
	Savings func(ctx context.Context) (jetstream.KeyValue, error)
	Publish func(context.Context, AutopilotEntry)

	mu      sync.Mutex
	running bool
	passes  sync.WaitGroup // tests wait for the background pass
}

// Beat starts one compaction pass in the background, so a long /compact never stalls
// the heartbeat. A pass still in flight makes this beat a no-op.
func (c *Compactor) Beat(ctx context.Context, live Live) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return
	}
	c.running = true
	c.passes.Add(1)
	go func() {
		defer c.finish()
		c.pass(ctx, live)
	}()
}

func (c *Compactor) finish() {
	c.mu.Lock()
	c.running = false
	c.mu.Unlock()
	c.passes.Done()
}

// pass compacts idle workers, restarts the ones that stay full and resumes tickets
// that left the paused set. Each failure is one error entry; the pass goes on.
func (c *Compactor) pass(ctx context.Context, live Live) {
	kv, err := c.KV(ctx)
	if err != nil {
		c.Publish(ctx, AutopilotEntry{Kind: "error", Action: "compaction", Reason: err.Error()})
		return
	}
	c.resume(ctx, kv, live)
	now := time.Now()
	for _, h := range live.Fleet {
		if h.Stale {
			continue
		}
		for _, t := range h.Threads {
			if !compactable(t) {
				continue
			}
			var usage ContextUsage
			if err := c.Call(ctx, h.Host, "ctx", []string{t.ThreadID}, &usage); err != nil {
				c.report(ctx, h.Host, t.ThreadID, "context", "", err)
				continue
			}
			st, err := loadCompact(ctx, kv, t.ThreadID)
			if err != nil {
				c.report(ctx, h.Host, t.ThreadID, "compact", "", err)
				continue
			}
			switch act := compactDecision(t, usage, st, now); act.Kind {
			case "compact":
				c.report(ctx, h.Host, t.ThreadID, "compact", act.Reason, c.compact(ctx, kv, h.Host, t.ThreadID, st))
			case "fresh":
				id, err := c.fresh(ctx, kv, h.Host, t, st, usage.UsedTokens)
				if err != nil {
					c.report(ctx, h.Host, t.ThreadID, "fresh-thread", "", err)
				} else if id != "" {
					c.report(ctx, h.Host, t.ThreadID, "fresh-thread", act.Reason+"; continued in "+id, nil)
				}
			}
		}
	}
}

// compact sends /compact with the handoff focus and records when it ran.
func (c *Compactor) compact(ctx context.Context, kv jetstream.KeyValue, host, thread string, st compactState) error {
	focus, err := CurrentBehavior().Render("compact_focus", nil)
	var out CompactResult
	if err == nil {
		err = c.Call(ctx, host, "compact", []string{thread, focus}, &out)
	}
	if err != nil {
		return err
	}
	if out.Before.UsedTokens > out.After.UsedTokens {
		c.saved(ctx, out.Before.UsedTokens-out.After.UsedTokens)
	}
	st.CompactedAt = time.Now()
	return kvPutJSON(ctx, kv, compactKey(thread), st)
}

// fresh starts a new thread on the same worktree with the last reply as its brief,
// then settles the old one. carried is the context the old thread was holding.
// It returns "" when the thread has no worktree to continue in.
func (c *Compactor) fresh(ctx context.Context, kv jetstream.KeyValue, host string, t FleetThread, st compactState, carried int) (string, error) {
	var info map[string]any
	if err := c.Call(ctx, host, "thread", []string{t.ThreadID}, &info); err != nil {
		return "", err
	}
	path, _ := info["worktreePath"].(string)
	if path == "" {
		return "", nil
	}
	title, _ := info["title"].(string)
	branch, _ := info["branch"].(string)
	last, _ := info["lastReply"].(string)
	brief, err := CurrentBehavior().Render("fresh_brief", freshBrief{LastReply: last})
	if err != nil {
		return "", err
	}
	pol := CurrentBehavior().Models
	pair := pol.PairOrLeader(t.Model)
	input, err := json.Marshal(map[string]any{
		"projectTitle":      t.Project,
		"title":             title,
		"modelSelection":    pol.Advisor(pair, false),
		"workspaceStrategy": map[string]any{"type": "existing_worktree", "worktreePath": path, "branch": branch},
		"message":           brief + "\n\n" + pol.BriefLine(pair),
	})
	if err != nil {
		return "", err
	}
	var launched map[string]any
	if err := c.Call(ctx, host, "launch", []string{string(input)}, &launched); err != nil {
		return "", err
	}
	c.saved(ctx, carried-len(brief)/4)
	// Record Fresh before settling, so a failed settle never launches a second thread.
	st.Fresh = true
	if err := kvPutJSON(ctx, kv, compactKey(t.ThreadID), st); err != nil {
		return "", err
	}
	id, _ := launched["threadId"].(string)
	return id, c.Call(ctx, host, "settle", []string{t.ThreadID}, nil)
}

// resume compacts and wakes the idle threads of each ticket that left the paused set.
// The first beat only stores the set, so an existing pause never fires on upgrade.
func (c *Compactor) resume(ctx context.Context, kv jetstream.KeyValue, live Live) {
	paused, _, err := readPaused(ctx, kv)
	if err != nil {
		c.Publish(ctx, AutopilotEntry{Kind: "error", Action: "resume", Reason: err.Error()})
		return
	}
	var before []string
	found, err := kvJSON(ctx, kv, compactPaused, &before)
	if err == nil {
		err = kvPutJSON(ctx, kv, compactPaused, paused)
	}
	if err != nil {
		c.Publish(ctx, AutopilotEntry{Kind: "error", Action: "resume", Reason: err.Error()})
		return
	}
	var resumed []string
	for _, key := range before {
		if !slices.Contains(paused, key) {
			resumed = append(resumed, key)
		}
	}
	if !found || len(resumed) == 0 {
		return
	}
	for _, h := range live.Fleet {
		if h.Stale {
			continue
		}
		for _, t := range h.Threads {
			ticket := strings.ToUpper(ticketPattern.FindString(t.Title + " " + t.Branch))
			if !compactable(t) || !slices.Contains(resumed, ticket) {
				continue
			}
			st, err := loadCompact(ctx, kv, t.ThreadID)
			if err == nil {
				err = c.compact(ctx, kv, h.Host, t.ThreadID, st)
			}
			if err == nil {
				err = c.Call(ctx, h.Host, "send", []string{t.ThreadID, compactResume}, nil)
			}
			c.report(ctx, h.Host, t.ThreadID, "resume", "ticket "+ticket+" is running again", err)
		}
	}
}

// saved records context removed from a worker as fresh-brief savings. It never fails a compaction.
// A one-time lower bound: every later turn would have re-sent this context too.
func (c *Compactor) saved(ctx context.Context, tokens int) {
	if c.Savings == nil || tokens <= 0 {
		return
	}
	kv, err := c.Savings(ctx)
	if err == nil {
		err = RecordSaving(ctx, kv, SavingFreshBrief, tokens, 0, time.Now())
	}
	if err != nil {
		c.Publish(ctx, AutopilotEntry{Kind: "error", Action: "savings", Reason: err.Error()})
	}
}

// report logs one action, or its failure, against the thread it touched.
func (c *Compactor) report(ctx context.Context, host, thread, action, reason string, err error) {
	e := AutopilotEntry{Kind: "act", Target: host + "/" + thread, Action: action, Reason: reason}
	if err != nil {
		e = AutopilotEntry{Kind: "error", Target: host + "/" + thread, Action: action, Reason: err.Error()}
	}
	c.Publish(ctx, e)
}

// compactKey names one thread's state; hashing keeps thread IDs out of the key.
func compactKey(thread string) string {
	sum := sha256.Sum256([]byte(thread))
	return "compact." + hex.EncodeToString(sum[:6])
}

func loadCompact(ctx context.Context, kv jetstream.KeyValue, thread string) (compactState, error) {
	var st compactState
	_, err := kvJSON(ctx, kv, compactKey(thread), &st)
	return st, err
}

// kvJSON decodes a JSON value; found is false when the key does not exist.
func kvJSON(ctx context.Context, kv jetstream.KeyValue, key string, out any) (bool, error) {
	entry, err := kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(entry.Value(), out)
}

func kvPutJSON(ctx context.Context, kv jetstream.KeyValue, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, key, data)
	return err
}
