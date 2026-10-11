package cos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The rules engine classifies every event with zero tokens: ACT (merge, wake
// the owning worker, fix its model), RECORD (state and feed only) or ESCALATE
// (judgment for the COS agent, published on cos.autopilot.log.escalate).
const (
	ClassAct      = "act"
	ClassRecord   = "record"
	ClassEscalate = "escalate"

	rulesConsumer   = "cos-rules"
	rulesMaxDeliver = 5
	prLineLimit     = 1500
)

// Wake texts are behavior files (behavior/templates/wake_<rule>.tmpl), hot-reloaded
// over NATS; a bundle missing one, or rendering more than one line, is rejected.
var wakeRules = []string{"ci", "review", "conflict", "failed", "interrupted", "size", "slice"}

type wakeData struct {
	PR, Checks, Summary string
	Lines               int
}

func init() {
	for _, rule := range wakeRules {
		behaviorTemplates["wake_"+rule] = wakeData{PR: "https://git.terrascale.tech/TerraScale/ts-cos/pulls/9", Checks: "test", Summary: "exit 1", Lines: 2000}
		behaviorLines = append(behaviorLines, "wake_"+rule)
	}
}

// Model rules (user 2026-10-10): Haiku is always 5.5; the same failure twice escalates.
var (
	modelHaiku     = map[string]any{"instanceId": "claudeAgent", "model": "claude-haiku-5-5"}
	modelOpusHigh  = map[string]any{"instanceId": "claudeAgent", "model": "claude-opus-5-5", "options": []any{map[string]any{"id": "effort", "value": "high"}}}
	modelOpusXHigh = map[string]any{"instanceId": "claudeAgent", "model": "claude-opus-5-5", "options": []any{map[string]any{"id": "effort", "value": "xhigh"}}}
)

func escalatedModel(model string) map[string]any {
	if strings.Contains(model, "haiku") {
		return modelOpusHigh
	}
	return modelOpusXHigh
}

// Decision is one classified outcome with the reason shown in the feed.
type Decision struct {
	Class  string
	Rule   string
	Reason string
	Host   string
	Thread string
	Ticket string
	// At most one action per decision.
	Wake  string         // message queued to Host/Thread
	Merge *PR            // squash-merge this READY PR
	Model map[string]any // next-run model for Host/Thread
	Sig   string         // wake signature for dedup and "same failure twice"
}

// PRFacts is the fresh state of the event's PR, fetched by the engine.
type PRFacts struct {
	PR
	Lines  int
	Merged bool
	State  string
	Body   string
}

// WakeMemory is the last wake per thread, replicated in KV across leaders.
type WakeMemory struct {
	Sig   string `json:"sig"`
	SHA   string `json:"sha"`
	Count int    `json:"count"`
}

// Classify is pure: the event, the latest live snapshot, the fresh PR (if any)
// and the owning thread's last wake decide everything.
func Classify(e Event, live Live, pr *PRFacts, memory WakeMemory, paused map[string]bool) []Decision {
	host, thread := e.Host, e.Thread
	var owner *FleetThread
	if pr != nil {
		host, owner = prOwner(live, pr.PR)
	} else if thread != "" {
		owner = findThread(live, host, thread)
	}
	if owner != nil {
		thread = owner.ThreadID
	}
	ticket := e.Ticket
	if ticket == "" && owner != nil {
		ticket = ticketPattern.FindString(owner.Title + " " + owner.Branch)
	}
	if ticket == "" && pr != nil && len(pr.Issues) > 0 {
		ticket = pr.Issues[0]
	}
	ticket = strings.ToUpper(ticket)
	d := func(class, rule, reason string) Decision {
		return Decision{Class: class, Rule: rule, Reason: reason, Host: host, Thread: thread, Ticket: ticket}
	}
	if e.Source == SourceUser || e.Source == SourceInbox {
		return nil // the COS inbox owns user messages
	}
	switch e.Kind {
	case KindRequestPending, KindNeedsUser:
		return []Decision{d(ClassEscalate, "worker-question", "a worker needs a decision: "+e.Summary)}
	case KindPushMain, KindContextHigh, KindPROpened, KindPRClosed:
		if e.Kind != KindPROpened {
			return []Decision{d(ClassRecord, "state", e.Summary)}
		}
	}
	if running, why := ticketRunning(live, paused, ticket); !running {
		rule := "not-running"
		if paused[ticket] {
			rule = "paused"
		}
		return []Decision{d(ClassRecord, rule, why)}
	}
	out := []Decision{}
	if owner != nil && strings.Contains(owner.Model, "haiku-4-5") {
		x := d(ClassAct, "model-haiku-5-5", "Haiku is always 5.5, never 4.5")
		x.Model = modelHaiku
		out = append(out, x)
	}
	wake := func(rule, reason, sha string, data wakeData) []Decision {
		if owner == nil {
			return append(out, d(ClassEscalate, "orphan", reason+"; no live thread owns it"))
		}
		text, err := CurrentBehavior().Render("wake_"+rule, data)
		if err != nil {
			return append(out, d(ClassRecord, rule, "template failed: "+err.Error()))
		}
		x := d(ClassAct, rule, reason)
		x.Wake, x.Sig = text, rule+":"+reason
		switch {
		case memory.Sig == x.Sig && memory.SHA == sha:
			return append(out, d(ClassRecord, rule, "already woken: "+reason))
		case memory.Sig == x.Sig && memory.Count >= 1:
			m := d(ClassAct, "escalate-model", "same failure twice: "+reason)
			m.Model = escalatedModel(owner.Model)
			// The model switches first, so the escalated model handles this wake.
			return append(out, m, x, d(ClassEscalate, "same-failure-twice", reason+" (twice; model escalated)"))
		}
		return append(out, x)
	}
	if pr != nil {
		ref := pr.Repo + "#" + strconv.Itoa(pr.Number)
		data := wakeData{PR: pr.URL, Lines: pr.Lines, Checks: strings.Join(pr.Failing, ", ")}
		switch {
		case pr.Merged:
			if strings.HasPrefix(strings.TrimSpace(pr.Body), "Part of") {
				return wake("slice", ref+" merged with slices left", pr.SHA, data)
			}
			return append(out, d(ClassRecord, "merged", ref+" merged; the ticket's work is complete"))
		case pr.State != "open":
			return append(out, d(ClassRecord, "closed", ref+" closed"))
		case pr.Lines > prLineLimit:
			return wake("size", fmt.Sprintf("%s changes %d lines (> %d)", ref, pr.Lines, prLineLimit), pr.SHA, data)
		case pr.Ready && pr.Cos:
			x := d(ClassAct, "merge-ready", ref+" is READY: mergeable, CI green, ts-reviewer approved")
			x.Merge = &pr.PR
			return append(out, x)
		case !pr.Mergeable:
			return wake("conflict", ref+" conflicts with "+pr.Base, pr.SHA, data)
		case pr.CI == "failure":
			return wake("ci", ref+" CI failed: "+strings.Join(pr.Failing, ", "), pr.SHA, data)
		case pr.ChangesRequested:
			return wake("review", ref+" has requested changes", pr.SHA, data)
		}
		return append(out, d(ClassRecord, "progress", ref+": CI "+pr.CI+", review "+reviewLabel(pr.PR)))
	}
	data := wakeData{Summary: e.Summary}
	switch e.Kind {
	case KindTurnFailed:
		return wake("failed", "run failed: "+e.Summary, e.ID, data)
	case KindTurnInterrupted:
		return wake("interrupted", "run interrupted: "+e.Summary, e.ID, data)
	}
	return append(out, d(ClassRecord, "progress", e.Summary))
}

// ticketRunning: work runs while its key is on an open thread or an open PR, unless the ticket is paused.
func ticketRunning(live Live, paused map[string]bool, ticket string) (bool, string) {
	if ticket == "" {
		return false, "no ticket key; not COS work"
	}
	if paused[ticket] {
		return false, ticket + " is paused (cos resume " + ticket + ")"
	}
	for _, h := range live.Fleet {
		for _, t := range h.Threads {
			if ticketIn(t.Title+" "+t.Branch, ticket) {
				return true, ""
			}
		}
	}
	for _, pr := range live.PRs {
		if slices.ContainsFunc(pr.Issues, func(key string) bool { return strings.EqualFold(key, ticket) }) {
			return true, ""
		}
	}
	return false, ticket + " has no open thread or PR"
}

func findThread(live Live, host, id string) *FleetThread {
	for _, h := range live.Fleet {
		for i, t := range h.Threads {
			if t.ThreadID == id && (host == "" || h.Host == host) {
				return &h.Threads[i]
			}
		}
	}
	return nil
}

// prOwner is the open top-level thread that links the PR, else the one on its branch.
func prOwner(live Live, pr PR) (string, *FleetThread) {
	suffix := strings.ToLower("/" + pr.Repo + "/pulls/" + strconv.Itoa(pr.Number))
	for _, match := range []func(FleetThread) bool{
		func(t FleetThread) bool {
			return slices.ContainsFunc(t.PRs, func(p ThreadPR) bool { return strings.HasSuffix(strings.ToLower(p.URL), suffix) })
		},
		func(t FleetThread) bool { return pr.Branch != "" && t.Branch == pr.Branch },
	} {
		for _, h := range live.Fleet {
			for i, t := range h.Threads {
				if t.Lineage == nil && match(t) {
					return h.Host, &h.Threads[i]
				}
			}
		}
	}
	return "", nil
}

// RulesEngine consumes cos.events.> on the leader and executes decisions.
type RulesEngine struct {
	Store   *Store
	JS      jetstream.JetStream
	Publish func(context.Context, AutopilotEntry)
	PR      func(ctx context.Context, repo string, number int) (*PRFacts, error)
	Merge   func(ctx context.Context, repo string, number int) error
	Call    func(ctx context.Context, host, command string, args []string) error
	kvMu    sync.Mutex
	memory  jetstream.KeyValue
}

// settingsKV opens the replicated COS_AUTOPILOT bucket once; consume, Handle and the
// paused-set read share it.
func (r *RulesEngine) settingsKV(ctx context.Context) (jetstream.KeyValue, error) {
	r.kvMu.Lock()
	defer r.kvMu.Unlock()
	if r.memory == nil {
		kv, err := r.JS.KeyValue(ctx, autopilotSettings)
		if err != nil {
			return nil, err
		}
		r.memory = kv
	}
	return r.memory, nil
}

// NewRulesEngine wires the real executors; StartAutopilot supplies JS and Publish.
func NewRulesEngine(cfg Config, bus *Bus, store *Store) *RulesEngine {
	f := Forgejo{Config: cfg}
	return &RulesEngine{Store: store, PR: f.prFacts,
		Merge: func(ctx context.Context, repo string, number int) error {
			_, err := f.Merge(ctx, repo, number)
			return err
		},
		Call: func(ctx context.Context, host, command string, args []string) error {
			return bus.Call(ctx, host, command, args, nil)
		}}
}

// Run consumes until ctx ends (the lease is lost); the durable consumer resumes
// where the previous leader stopped.
func (r *RulesEngine) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := r.consume(ctx); err != nil && ctx.Err() == nil {
			slog.Error("rules engine", "error", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
}

func (r *RulesEngine) consume(ctx context.Context) error {
	setup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := EnsureEventStreams(setup, r.JS); err != nil {
		return err
	}
	if _, err := r.settingsKV(setup); err != nil {
		return err
	}
	consumer, err := r.JS.CreateOrUpdateConsumer(setup, EventsStream, jetstream.ConsumerConfig{
		Durable: rulesConsumer, FilterSubject: EventsSubjects, DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 2 * time.Minute, MaxDeliver: rulesMaxDeliver,
	})
	if err != nil {
		return err
	}
	msgs, err := consumer.Messages()
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, msgs.Stop)
	defer stop()
	for {
		m, err := msgs.Next()
		if err != nil {
			return err
		}
		var e Event
		if err := json.Unmarshal(m.Data(), &e); err != nil || e.Validate() != nil {
			_ = m.Term() // malformed events never become valid
			continue
		}
		if err := r.Handle(ctx, e); err != nil {
			meta, _ := m.Metadata()
			if meta != nil && meta.NumDelivered >= rulesMaxDeliver {
				r.Publish(ctx, AutopilotEntry{Kind: "error", Event: e.ID, Target: e.Summary, Action: "Gave up on event", Reason: err.Error()})
				_ = m.Term()
			} else {
				_ = m.NakWithDelay(30 * time.Second)
			}
			continue
		}
		_ = m.Ack()
	}
}

// Handle classifies one event and executes every decision, logging each with its reason.
func (r *RulesEngine) Handle(ctx context.Context, e Event) error {
	live, err := r.Store.Live(ctx)
	if err != nil {
		return err
	}
	kv, err := r.settingsKV(ctx)
	if err != nil {
		return err // retried: a pause must never be missed
	}
	paused, err := pausedSet(ctx, kv)
	if err != nil {
		return err // retried: a pause must never be missed
	}
	var pr *PRFacts
	if e.PR > 0 && e.Repo != "" {
		_, name, _ := strings.Cut(e.Repo, "/")
		if pr, err = r.PR(ctx, name, e.PR); errors.Is(err, errNotMine) {
			r.Publish(ctx, AutopilotEntry{Kind: ClassRecord, Event: e.ID, Action: "invisible", Reason: err.Error()})
			return nil
		} else if err != nil {
			return err
		}
	}
	key := ""
	memory := WakeMemory{}
	if thread := e.Thread; thread != "" || pr != nil {
		if pr != nil {
			if _, owner := prOwner(live, pr.PR); owner != nil {
				thread = owner.ThreadID
			}
		}
		if thread != "" {
			sum := sha256.Sum256([]byte(thread))
			key = "wake." + hex.EncodeToString(sum[:12])
			if entry, err := kv.Get(ctx, key); err == nil {
				_ = json.Unmarshal(entry.Value(), &memory)
			}
		}
	}
	for _, d := range Classify(e, live, pr, memory, paused) {
		entry := AutopilotEntry{Kind: d.Class, Event: e.ID, Ticket: d.Ticket, Target: decisionTarget(d), Action: d.Rule, Reason: d.Reason}
		switch {
		case d.Merge != nil:
			if err := r.Merge(ctx, d.Merge.Repo, d.Merge.Number); err != nil {
				entry.Kind, entry.Reason = "error", d.Reason+"; merge failed: "+publicText(err)
			}
		case d.Model != nil:
			sel, _ := json.Marshal(d.Model)
			if err := r.Call(ctx, d.Host, "model", []string{d.Thread, string(sel)}); err != nil {
				entry.Kind, entry.Reason = "error", d.Reason+"; model change failed: "+publicText(err)
			}
		case d.Wake != "":
			if err := r.Call(ctx, d.Host, "send", []string{d.Thread, d.Wake}); err != nil {
				return fmt.Errorf("wake %s: %w", d.Thread, err) // retried: the worker must hear it
			}
			sha := e.ID
			if pr != nil {
				sha = pr.SHA
			}
			next := WakeMemory{Sig: d.Sig, SHA: sha, Count: 1}
			if memory.Sig == d.Sig {
				next.Count = memory.Count + 1
			}
			if data, err := json.Marshal(next); err == nil && key != "" {
				_, _ = kv.Put(ctx, key, data)
			}
		}
		r.Publish(ctx, entry)
	}
	return nil
}

func decisionTarget(d Decision) string {
	if d.Merge != nil {
		return d.Merge.URL
	}
	if d.Thread != "" {
		return d.Host + " " + d.Thread
	}
	return ""
}

func publicText(err error) string {
	var public interface{ PublicMessage() string }
	if errors.As(err, &public) {
		return public.PublicMessage()
	}
	return err.Error()
}

// Only the user's PRs are visible to COS.
var errNotMine = errors.New("not authored by mariogk")

// prFacts reads one PR authored by mariogk with its gates and size.
func (f Forgejo) prFacts(ctx context.Context, repo string, number int) (*PRFacts, error) {
	path := "/repos/" + forgejoOrg + "/" + url.PathEscape(repo) + "/pulls/" + strconv.Itoa(number)
	var raw struct {
		forgejoPR
		Additions int  `json:"additions"`
		Deletions int  `json:"deletions"`
		Merged    bool `json:"merged"`
	}
	if _, err := f.request(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return nil, err
	}
	if raw.User.Login != forgejoLogin {
		return nil, fmt.Errorf("%s#%d: %w", repo, number, errNotMine)
	}
	statuses, err := f.statuses(ctx, repo, raw.Head.SHA)
	if err != nil {
		return nil, err
	}
	reviews, err := forgejoList[forgejoReview](ctx, f, path+"/reviews")
	if err != nil {
		return nil, err
	}
	return &PRFacts{PR: forgejoState(repo, raw.forgejoPR, statuses, reviews, f.skipReviewer()),
		Lines: raw.Additions + raw.Deletions, Merged: raw.Merged, State: raw.State, Body: raw.Body}, nil
}

// reviewLabel names a PR's review gate in rules output (moved from web.go when the dashboard dropped it).
func reviewLabel(p PR) string {
	if p.ChangesRequested {
		return "changes requested"
	}
	if p.BotApproved {
		return "approved"
	}
	if p.SkipReviewer {
		return "review bypassed"
	}
	return "waiting"
}
