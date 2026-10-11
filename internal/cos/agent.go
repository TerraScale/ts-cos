package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	agentBucket = "COS_AGENT"
	// Fresh brief when idle past this context size; compact instead while a run is active.
	agentRestartTokens = 120_000
	agentFreshMin      = 40_000
	agentIdleRestart   = 4 * time.Hour
	// hard cap on the memory section of a fresh brief (the old context dump allowed 48 KiB, about 12k tokens)
	agentMemoryTokens = 4000
	agentRecentBytes  = 6 << 10
	agentWorkspace    = ".local/share/cos/agent"
)

// AgentState is the replicated record of the one COS thread, so a new leader can take over.
type AgentState struct {
	Host          string            `json:"host"`
	ThreadID      string            `json:"threadId"`
	Started       time.Time         `json:"started"`
	Reason        string            `json:"reason"`
	Behavior      string            `json:"behavior"`
	Focus         string            `json:"focus"`
	OpenPRs       []string          `json:"openPRs"`
	LastWake      time.Time         `json:"lastWake"`
	Delivered     string            `json:"delivered"`
	Restart       string            `json:"restart,omitempty"`
	Run           string            `json:"run,omitempty"`
	Reply         string            `json:"reply,omitempty"`
	Compacted     string            `json:"compacted,omitempty"`
	BriefTokens   int               `json:"briefTokens,omitempty"`
	CarriedTokens int               `json:"carriedTokens,omitempty"`
	Asked         map[string]string `json:"asked,omitempty"`
	Wakes         []WakeRecord      `json:"wakes,omitempty"`
	Orphans       []AgentThread     `json:"orphans,omitempty"`
	// DigestSince is where the next escalation digest starts; it survives a fresh COS thread.
	DigestSince time.Time `json:"digestSince,omitempty"`
	// EventsSince is where the next immediate-event read starts; it survives a fresh COS thread.
	EventsSince time.Time `json:"eventsSince,omitempty"`
}

// addReason keeps every pending restart reason, so a failover is never hidden by a later trigger.
func (st *AgentState) addReason(r string) {
	if st.Restart != "" {
		r = st.Restart + "; " + r
	}
	st.Restart = r
}

type AgentThread struct {
	Host     string `json:"host"`
	ThreadID string `json:"threadId"`
}

type agentT3 interface {
	Projects(context.Context) ([]Project, error)
	HTTP(ctx context.Context, method, path string, input, output any) error
	Launch(context.Context, map[string]any) (any, error)
	Send(context.Context, string, string, SendMode) (SendResult, error)
	CloseTree(context.Context, string, bool) (TreeResult, error)
	RPC(context.Context, string, any, any) error
}

// Agent drives the COS thread on the cluster leader. Every other node only reads its chat output.
type Agent struct {
	Host, Home, Exe string
	Chat            *Chat
	Store           *Store
	KV, Context     jetstream.KeyValue
	Memory          jetstream.KeyValue
	Savings         jetstream.KeyValue
	Limits          jetstream.KeyValue
	T3              func(context.Context) (agentT3, error)
	Remote          func(ctx context.Context, host, command string, args []string, out any) error
	js              jetstream.JetStream
	project         string
	shown           map[string]string
	orphanCheck     time.Time
	digestCheck     time.Time
	eventCheck      time.Time
	hot, nextPoll   time.Time
	saved           string
}

// The agent runs on every node as a serve worker; only the cluster leader drives COS (Run uses Bus.Lead).
func init() {
	serveWorkers = append(serveWorkers, func(ctx context.Context, cfg Config, bus *Bus) {
		store, err := stateStore(cfg)
		if err != nil {
			slog.Error("COS agent state", "error", err)
			return
		}
		defer store.Close()
		chat := NewChat()
		go chat.Run(ctx, bus)
		NewAgent(cfg, chat, store, bus).Run(ctx, bus)
	})
}

func NewAgent(cfg Config, chat *Chat, store *Store, bus *Bus) *Agent {
	exe, _ := os.Executable()
	return &Agent{Host: cfg.LocalHost, Home: cfg.Home, Exe: exe, Chat: chat, Store: store,
		T3:     func(ctx context.Context) (agentT3, error) { return OpenT3(ctx, cfg, cfg.LocalHost) },
		Remote: bus.Call}
}

// openAgentKV binds COS_AGENT with leader-only reads, so Step never acts on a stale state.
func openAgentKV(ctx context.Context, bus *Bus) (jetstream.KeyValue, error) {
	if _, err := openKV(ctx, bus, agentBucket); err != nil {
		return nil, err
	}
	js, err := bus.JetStream()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := leaderReads(ctx, js, agentBucket); err != nil {
		return nil, err
	}
	return openKV(ctx, bus, agentBucket) // re-bind: the first handle still uses direct gets
}

// Run drives COS while this node holds the cluster lease (Bus.Lead) and idles otherwise.
func (a *Agent) Run(ctx context.Context, bus *Bus) {
	for a.KV == nil {
		kv, err := openAgentKV(ctx, bus)
		if err == nil {
			a.Context, err = openKV(ctx, bus, contextBucket)
		}
		if err == nil {
			a.Memory, err = openKV(ctx, bus, memoryBucket)
		}
		if err == nil {
			a.Savings, err = openKV(ctx, bus, savingsBucket)
		}
		if err == nil {
			a.Limits, err = openLimits(ctx, bus)
		}
		if err == nil {
			a.js, err = bus.JetStream()
		}
		if err == nil {
			a.KV = kv
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	bus.Lead(ctx, func(ctx context.Context) {
		for {
			if a.Chat.Ready() {
				if err := a.Step(ctx); err != nil && ctx.Err() == nil {
					logErr(ctx, slog.LevelWarn, "COS agent step", err)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	})
}

func (a *Agent) load(ctx context.Context) (AgentState, uint64, error) {
	var st AgentState
	e, err := a.KV.Get(ctx, "state")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return st, 0, nil
	}
	if err != nil {
		return st, 0, err
	}
	a.saved = string(e.Value())
	return st, e.Revision(), json.Unmarshal(e.Value(), &st)
}

func (a *Agent) save(ctx context.Context, st AgentState, rev uint64) error {
	data, err := json.Marshal(st)
	if err != nil || string(data) == a.saved {
		return err
	}
	if rev == 0 {
		_, err = a.KV.Create(ctx, "state", data)
	} else {
		_, err = a.KV.Update(ctx, "state", data, rev)
	}
	return err
}

// Step reconciles the COS thread with the chat and live state; it is idempotent.
func (a *Agent) Step(ctx context.Context) error {
	st, rev, err := a.load(ctx)
	if err != nil {
		return fmt.Errorf("load COS state: %w", err)
	}
	now := time.Now().UTC()
	if st.Host != a.Host {
		if st.ThreadID != "" {
			st.Orphans = append(st.Orphans, AgentThread{st.Host, st.ThreadID})
			if e, ok := a.Chat.Entry(st.Reply); ok && e.Streaming {
				e.Streaming = false
				e.Text += "\n\n_Interrupted: the COS host failed over._"
				if _, err := a.Chat.Put(ctx, e); err != nil {
					return err
				}
			}
			// Asked stays: the next mirror closes asks whose requests are gone from the old thread.
			st.ThreadID, st.Run, st.Reply, st.Compacted = "", "", "", ""
			st.addReason("failover from " + st.Host)
		}
		st.Host = a.Host
		// Claim before acting: a node that loses this race must not launch a second COS thread.
		if err := a.save(ctx, st, rev); err != nil {
			return fmt.Errorf("claim COS: %w", err)
		}
		return nil
	}
	c, err := a.T3(ctx)
	if err != nil {
		return fmt.Errorf("open T3: %w", err)
	}
	v, err := View(ctx, a.Store)
	if err != nil {
		return fmt.Errorf("read COS view: %w", err)
	}
	if held, err := a.agentHeld(ctx); err != nil {
		return err
	} else if held {
		return a.mirrorOnly(ctx, c, &st, rev)
	}
	failover := st.ThreadID == "" && strings.Contains(st.Restart, "failover from")
	if b := CurrentBehavior(); b.Hash != st.Behavior {
		if st.ThreadID != "" {
			st.addReason("behavior updated to " + b.Version)
		}
		st.Behavior = b.Hash
	}
	if e, err := a.KV.Get(ctx, "restart"); err == nil {
		st.addReason("COS asked: " + string(e.Value()))
		// Fenced: a newer request written meanwhile survives for the next step.
		if err := a.KV.Delete(ctx, "restart", jetstream.LastRevision(e.Revision())); err != nil {
			return err
		}
	}
	wake := []string{}
	reasons := []string{}
	if failover && unanswered(a.Chat.Entries()) {
		wake = append(wake, "The previous COS host failed over. The last user message in Recent chat has no reply yet: answer it.")
		reasons = append(reasons, "failover")
	}
	focusText := ""
	if e, err := a.Context.Get(ctx, "focus"); err == nil {
		focusText = string(e.Value())
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if focus := focusOf(focusText, v); focus != st.Focus {
		if st.ThreadID != "" {
			st.addReason("focus changed")
			wake = append(wake, "Focus changed: "+focusText+". Re-plan around it.")
			reasons = append(reasons, "focus")
		}
		st.Focus = focus
	}
	// A unit of work finishes when one of the user's PRs leaves the open set (merged or closed).
	if v.Live.Errors["prs"] == "" && v.Live.At != "" {
		open := openPRs(v)
		if done := missingFrom(st.OpenPRs, open); st.OpenPRs != nil && len(done) > 0 {
			st.addReason("finished " + strings.Join(done, ", "))
		}
		st.OpenPRs = open
	}
	if !st.LastWake.IsZero() && now.Sub(st.LastWake) > agentIdleRestart && st.Restart == "" {
		st.addReason("idle for 4 hours")
	}
	messages := []ChatEntry{}
	for _, e := range a.Chat.Entries() {
		if e.Role == "user" && e.ID > st.Delivered {
			messages = append(messages, e)
		}
	}
	for _, m := range messages {
		wake = append(wake, "User: "+m.Text)
		st.Delivered = m.ID
	}
	items := len(messages)
	if items > 0 {
		reasons = append(reasons, "chat")
	}
	if text, err := a.digestWake(ctx, &st, now); err != nil {
		slog.Warn("escalation digest", "error", err)
	} else if text != "" {
		wake = append(wake, text)
		reasons = append(reasons, "digest")
		items++ // digestWake returns text only, so a digest counts as one item
	}
	if text, n, err := a.eventWake(ctx, &st, now); err != nil {
		slog.Warn("COS events", "error", err)
	} else if n > 0 {
		wake = append(wake, text)
		reasons = append(reasons, "event")
		items += n
	}
	busy := false
	// Poll T3 every second while COS works or a wake is due, otherwise every 30 seconds.
	if st.ThreadID != "" && (len(wake) > 0 || now.Before(a.hot) || !now.Before(a.nextPoll)) {
		a.nextPoll = now.Add(30 * time.Second)
		var p agentProjection
		var gone *T3Error
		err := c.RPC(ctx, "orchestration.getThreadProjection", map[string]any{"threadId": st.ThreadID}, &p)
		if errors.As(err, &gone) && gone.NotFound() {
			// The thread vanished (deleted, T3 data reset): forget it so the next wake relaunches.
			st.ThreadID, st.Run, st.Reply, st.Compacted = "", "", "", ""
			st.addReason("COS thread vanished")
		} else if err != nil {
			return fmt.Errorf("read COS thread: %w", err)
		} else if err := a.mirror(ctx, &st, p); err != nil {
			return fmt.Errorf("mirror COS thread: %w", err)
		}
		run, status := p.latest()
		busy = active(status)
		if busy {
			a.hot = now.Add(2 * time.Minute)
		}
		used := p.usedTokens()
		if used > agentRestartTokens {
			if busy && st.Compacted != run {
				compact := "/compact Keep only: the user's open requests, decisions made, threads and PRs you are driving and their state, and the exact next step. Drop tool output and resolved discussion."
				if _, err := c.Send(ctx, st.ThreadID, compact, SendQueue); err != nil {
					return err
				}
				st.Compacted = run
			} else if !busy && !strings.Contains(st.Restart, "context reached") {
				st.addReason(fmt.Sprintf("context reached %d tokens", used))
			}
		}
		// Every turn re-sends the carried context; restart once a fresh brief costs under half of it.
		if !busy && st.BriefTokens > 0 && used > agentFreshMin && used > 2*st.BriefTokens && st.Restart == "" {
			st.CarriedTokens = used
			st.addReason(fmt.Sprintf("context %d tokens vs a %d-token fresh brief", used, st.BriefTokens))
		}
	}
	if len(wake) > 0 {
		text := strings.Join(wake, "\n\n")
		reason := strings.Join(reasons, ", ")
		if st.ThreadID == "" || (st.Restart != "" && !busy) {
			// restart() clears st.Restart, so the reason is read first.
			reason = "first start"
			if st.Restart != "" {
				reason = st.Restart
			}
			if err := a.restart(ctx, c, &st, v, text, now); err != nil {
				return fmt.Errorf("start COS thread: %w", err)
			}
		} else if _, err := c.Send(ctx, st.ThreadID, text, SendQueue); err != nil {
			return fmt.Errorf("wake COS: %w", err)
		}
		st.LastWake = now
		a.hot = now.Add(2 * time.Minute)
		st.recordWake(WakeRecord{At: now, Reason: reason, Items: items})
	}
	if len(st.Orphans) > 0 && now.Sub(a.orphanCheck) > time.Minute {
		a.orphanCheck = now
		st.Orphans = slices.DeleteFunc(st.Orphans, func(o AgentThread) bool {
			var out TreeResult
			return a.Remote(ctx, o.Host, "delete", []string{o.ThreadID}, &out) == nil && len(out.Skipped) == 0
		})
	}
	if err := a.save(ctx, st, rev); err != nil {
		return fmt.Errorf("save COS state: %w", err)
	}
	return nil
}

// agentHeld reports whether COS's model has no headroom now.
func (a *Agent) agentHeld(ctx context.Context) (bool, error) {
	s, err := readLimits(ctx, a.Limits)
	if err != nil {
		return false, err
	}
	pol := CurrentBehavior().Models
	model, _ := pol.Advisor(pol.Leader, false)["model"].(string)
	ok, _, _ := s.Headroom(model, time.Now())
	return !ok, nil
}

// mirrorOnly reflects the COS thread's output while its model is limited; nothing is sent, launched or consumed.
func (a *Agent) mirrorOnly(ctx context.Context, c agentT3, st *AgentState, rev uint64) error {
	if st.ThreadID != "" {
		var p agentProjection
		if err := c.RPC(ctx, "orchestration.getThreadProjection", map[string]any{"threadId": st.ThreadID}, &p); err != nil {
			return fmt.Errorf("read COS thread: %w", err)
		}
		if err := a.mirror(ctx, st, p); err != nil {
			return fmt.Errorf("mirror COS thread: %w", err)
		}
	}
	return a.save(ctx, *st, rev)
}

// restart replaces the COS thread with a fresh one whose first message is a new brief.
func (a *Agent) restart(ctx context.Context, c agentT3, st *AgentState, v Dashboard, wake string, now time.Time) error {
	project, err := a.ensureProject(ctx, c)
	if err != nil {
		return err
	}
	reason := st.Restart
	if reason == "" {
		reason = "first start"
	}
	brief, r, err := a.brief(ctx, v, reason, wake)
	if err != nil {
		return err
	}
	pol := CurrentBehavior().Models
	out, err := c.Launch(ctx, map[string]any{"projectId": project, "title": "COS", "modelSelection": pol.Advisor(pol.Leader, false), "message": brief})
	var missing *T3Error
	if errors.As(err, &missing) && missing.NotFound() {
		a.project = "" // the project was deleted (T3 reset): find or recreate it next time
	}
	if err != nil {
		return err
	}
	id, _ := out.(map[string]any)["threadId"].(string)
	if id == "" {
		return errors.New("T3 launch returned no COS thread")
	}
	if st.ThreadID != "" {
		// A thread that could not be deleted (still running, T3 error) is retried with the orphans.
		if res, err := c.CloseTree(ctx, st.ThreadID, true); err != nil || len(res.Skipped) > 0 {
			st.Orphans = append(st.Orphans, AgentThread{a.Host, st.ThreadID})
		}
	}
	carried, oldBrief, replaced := st.CarriedTokens, st.BriefTokens, st.ThreadID != ""
	*st = AgentState{Host: a.Host, ThreadID: id, Started: now, Behavior: st.Behavior, Reason: reason, Focus: st.Focus, OpenPRs: st.OpenPRs, Delivered: st.Delivered, Orphans: st.Orphans, DigestSince: st.DigestSince, EventsSince: st.EventsSince, Asked: st.Asked, Wakes: st.Wakes}
	st.BriefTokens = len(brief) / 4
	if a.Savings != nil {
		// Savings are best effort: a failed write must never fail the restart that already launched.
		if r.FullTokens > r.Tokens {
			if err := RecordSaving(ctx, a.Savings, SavingRetrieval, r.FullTokens-r.Tokens, 0, now); err != nil {
				slog.Warn("record saving", "error", err)
			}
		}
		if replaced && carried > 0 {
			if err := RecordSaving(ctx, a.Savings, SavingFreshBrief, carried-oldBrief, 0, now); err != nil {
				slog.Warn("record saving", "error", err)
			}
		}
	}
	// The launch already happened: a failed notice must not make the next step launch again.
	if _, err := a.Chat.Put(ctx, ChatEntry{Role: "system", Text: "COS started a fresh thread on " + a.Host + ": " + reason + "."}); err != nil {
		slog.Warn("post COS restart notice", "error", err)
	}
	return nil
}

// ensureProject keeps the COS workspace: rules as CLAUDE.md and a ./cos wrapper bound to this node.
func (a *Agent) ensureProject(ctx context.Context, c agentT3) (string, error) {
	dir := filepath.Join(a.Home, agentWorkspace)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "CLAUDE.md"), []byte(CurrentBehavior().AgentBrief()), 0600); err != nil {
		return "", err
	}
	wrapper := fmt.Sprintf("#!/bin/sh\nHOME=%s exec %s \"$@\"\n", shellQuote(a.Home), shellQuote(a.Exe))
	if err := os.WriteFile(filepath.Join(dir, "cos"), []byte(wrapper), 0700); err != nil {
		return "", err
	}
	if a.project != "" {
		return a.project, nil
	}
	projects, err := c.Projects(ctx)
	if err != nil {
		return "", err
	}
	for _, p := range projects {
		if p.Root == dir {
			a.project = p.ID
			return p.ID, nil
		}
	}
	id := uuid()
	var out any
	if err := c.HTTP(ctx, "POST", "/api/projects/mutate", map[string]any{"type": "project.create", "commandId": uuid(), "projectId": id, "title": "cos-agent", "workspaceRoot": dir, "createWorkspaceRootIfMissing": true}, &out); err != nil {
		return "", err
	}
	a.project = id
	return id, nil
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// AgentBriefData feeds behavior/templates/brief.tmpl, the first message of every COS thread.
type AgentBriefData struct {
	At          time.Time
	Reason, Now string
	// Context and Omitted stay for behavior bundles rendered by older templates during a rollout.
	Context                                 []ContextFile
	Omitted, Live                           string
	Recent                                  []string
	Memory                                  string
	MemoryTokens, MemoryFull, MemoryOmitted int
}

// brief renders the first message of every COS thread: pinned and relevant memory, recent chat and live state.
func (a *Agent) brief(ctx context.Context, v Dashboard, reason, wake string) (string, MemoryRecall, error) {
	r, err := Recall(ctx, a.Memory, a.Context, reason+"\n"+wake, memoryRecallK, agentMemoryTokens)
	if err != nil {
		return "", MemoryRecall{}, err
	}
	data := AgentBriefData{At: time.Now().UTC(), Reason: reason, Now: wake, Live: Brief(v), Memory: r.Text(),
		MemoryTokens: r.Tokens, MemoryFull: r.FullTokens, MemoryOmitted: r.Omitted, Recent: []string{}}
	size := 0
	for _, e := range slices.Backward(a.Chat.Entries()) {
		if e.Role != "user" && e.Role != "cos" {
			continue
		}
		line := e.Role + ": " + truncate(e.Text, 800)
		if size += len(line); size > agentRecentBytes {
			break
		}
		data.Recent = append(data.Recent, line)
	}
	slices.Reverse(data.Recent)
	text, err := CurrentBehavior().Render("brief", data)
	return text, r, err
}

func init() {
	cliCommands["agent"] = cliCommand{Usage: "agent [restart '<reason>' | wakes]", Run: agentCommand}
}

// agentCommand shows the COS thread state or its last wakes, or asks the leader for a fresh brief.
func agentCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	wakes := len(args) == 1 && args[0] == "wakes"
	valid := len(args) == 0 || wakes || (len(args) == 2 && args[0] == "restart")
	if !valid {
		return errors.New("usage: cos agent [restart '<reason>' | wakes]")
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	kv, err := openAgentKV(ctx, bus)
	if err != nil {
		return err
	}
	if len(args) == 2 {
		reason := strings.TrimSpace(args[1])
		if reason == "" {
			return errors.New("restart needs a reason")
		}
		if _, err := kv.Put(ctx, "restart", []byte(truncate(reason, 200))); err != nil {
			return err
		}
		return output(w, map[string]any{"restart": reason, "note": "the leader starts a fresh COS thread at the next wake"})
	}
	e, err := kv.Get(ctx, "state")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		if wakes {
			return output(w, []WakeRecord{})
		}
		return output(w, AgentState{})
	}
	if err != nil {
		return err
	}
	if wakes {
		var st AgentState
		if err := json.Unmarshal(e.Value(), &st); err != nil {
			return err
		}
		return output(w, st.recentWakes(20))
	}
	_, err = w.Write(append(e.Value(), '\n'))
	return err
}

// unanswered reports whether the newest user or COS chat entry is from the user.
func unanswered(entries []ChatEntry) bool {
	for _, e := range slices.Backward(entries) {
		if e.Role == "user" || e.Role == "cos" {
			return e.Role == "user"
		}
	}
	return false
}
