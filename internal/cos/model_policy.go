package cos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Model policy (user 2026-10-11): every thread is the advisor of a pair from behavior/models.json and its
// workers do the work as parallel delegate_task subagents. The leader's beat reads TS-826's usage and
// nudges the threads that run too much themselves.
const (
	shareFloor     = 50            // percent of output tokens on workers
	shareGrace     = time.Hour     // a thread's first hour is never nudged
	shareMinTokens = 20000         // too little output to judge
	nudgeEvery     = 2 * time.Hour // at most one nudge of each kind per thread per window
)

// adviseData names a thread's pair: its advisor model, its worker model and the worker's delegate_task target.
type adviseData struct{ Advisor, Worker, Target string }

// ShareData fills nudge_worker_share: a thread's worker share and what its advisor ran itself.
type ShareData struct {
	adviseData
	WorkerPct, Workers, Parallel        int
	WorkerOutput, AdvisorOutput         int64
	Build, Git, Read, GitRead, Live, CI int
}

// WorkersData fills nudge_no_workers: a thread's advisor output with no worker at all.
type WorkersData struct {
	adviseData
	AdvisorOutput int64
}

// wrongWorkerData fills nudge_wrong_worker: a worker model that is not the worker of the thread's pair.
type wrongWorkerData struct {
	adviseData
	Model string
}

func init() {
	pair := adviseData{Advisor: "claude-opus-5-5", Worker: "claude-haiku-5-5", Target: `{"providerInstanceId":"claudeAgent","model":"claude-haiku-5-5"}`}
	behaviorTemplates["nudge_worker_share"] = ShareData{adviseData: pair, WorkerPct: 20, WorkerOutput: 10000, AdvisorOutput: 40000, Workers: 3, Parallel: 2, Build: 5, Git: 2, Read: 30, GitRead: 4, Live: 1, CI: 2}
	behaviorTemplates["nudge_no_workers"] = WorkersData{adviseData: pair, AdvisorOutput: 40000}
	behaviorTemplates["nudge_wrong_worker"] = wrongWorkerData{adviseData: pair, Model: "claude-haiku-4-5"}
}

// threadSeen is what the policy needs beyond the totals: when a thread first ran, the advisor model it ran
// as, and the worker models its subagents ran.
type threadSeen struct {
	First   time.Time
	Advisor string
	Workers []string
}

func seenThreads(rows []UsageRow) map[string]threadSeen {
	pol := CurrentBehavior().Models
	out := map[string]threadSeen{}
	for _, r := range rows {
		k := usageGroupKey(r, "thread")
		s, ok := out[k]
		if !ok || r.First.Before(s.First) {
			s.First = r.First
		}
		switch {
		case pol.WorkerOnlyModel(r.Model):
			if !slices.Contains(s.Workers, r.Model) {
				s.Workers = append(s.Workers, r.Model)
			}
		case s.Advisor == "":
			s.Advisor = r.Model
		}
		out[k] = s
	}
	return out
}

// wrongWorker is a worker model a subagent of the thread ran that is not the worker of the thread's pair.
func wrongWorker(pol ModelPolicy, s threadSeen) (string, bool) {
	worker := pol.Pairs[pol.PairOrLeader(s.Advisor)].Worker.Model
	for _, m := range s.Workers {
		if normalizeModel(m) != normalizeModel(worker) {
			return m, true
		}
	}
	return "", false
}

// adviseFor names a thread's pair. A model no pair names gets the leader pair.
func adviseFor(model string) adviseData {
	pol := CurrentBehavior().Models
	pair := pol.PairOrLeader(model)
	target, _ := json.Marshal(pol.WorkerTarget(pair))
	return adviseData{Advisor: pol.Pairs[pair].Advisor.Model, Worker: pol.Pairs[pair].Worker.Model, Target: string(target)}
}

// usagePolicy is pure: per-thread usage, when each thread first ran and the last nudge of each kind ("<kind>:<threadId>") decide the nudges.
// A thread with no worker subagents gets the no-workers nudge instead of the share nudge in the same beat.
func usagePolicy(totals []UsageTotal, seen map[string]threadSeen, now time.Time, last map[string]time.Time, paused map[string]bool) []Decision {
	pol := CurrentBehavior().Models
	var out []Decision
	for _, u := range totals {
		if u.Thread == "" || u.Host == "" || u.Subagent {
			continue // only a top-level T3 thread can be woken; subagents are exempt
		}
		if paused[u.Ticket] {
			continue // a paused ticket is never woken or switched
		}
		s := seen[u.Key]
		age := now.Sub(s.First)
		pair := adviseFor(s.Advisor)
		if worker, ok := wrongWorker(pol, s); ok && now.Sub(last["wrong-worker:"+u.Thread]) >= nudgeEvery {
			// no Sig on the escalate: the paired nudge's successful send records the window
			reason := fmt.Sprintf("a subagent of this thread ran on %s, not its worker %s", worker, pair.Worker)
			out = append(out,
				Decision{Class: ClassEscalate, Rule: "subagent-wrong-worker", Reason: reason, Host: u.Host, Thread: u.Thread},
				nudge(u.Host, u.Thread, s.Advisor, "wrong-worker", "nudge_wrong_worker", reason, wrongWorkerData{adviseData: pair, Model: worker}))
		}
		switch {
		case u.Workers == 0 && u.AdvisorOutput >= shareMinTokens && age >= shareGrace && now.Sub(last["noworkers:"+u.Thread]) >= nudgeEvery:
			reason := fmt.Sprintf("no worker subagents after the first hour, %d advisor output tokens", u.AdvisorOutput)
			out = append(out, nudge(u.Host, u.Thread, s.Advisor, "noworkers", "nudge_no_workers", reason, WorkersData{adviseData: pair, AdvisorOutput: u.AdvisorOutput}))
		case u.HaikuShare < shareFloor && age >= shareGrace && u.Output >= shareMinTokens && now.Sub(last["share:"+u.Thread]) >= nudgeEvery:
			share := ShareData{adviseData: pair, WorkerPct: u.HaikuShare, WorkerOutput: u.WorkerOutput, AdvisorOutput: u.AdvisorOutput, Workers: u.Workers, Parallel: u.Parallel,
				Build: u.Tools["build"], Git: u.Tools["git"], Read: u.Tools["read"], GitRead: u.Tools["gitread"], Live: u.Tools["live"], CI: u.Tools["ci"]}
			reason := fmt.Sprintf("worker share %d%% < %d%% after the first hour", u.HaikuShare, shareFloor)
			out = append(out, nudge(u.Host, u.Thread, s.Advisor, "share", "nudge_worker_share", reason, share))
		}
	}
	return out
}

// fleetModelPolicy is pure: a top-level thread on a worker model switches to its pair's advisor, and a worker
// whose parent is not an advisor escalates the parent (once per window).
// Forks are real threads and switch like top-level ones; only delegate_task subagents run as workers.
func fleetModelPolicy(live Live, now time.Time, last map[string]time.Time, paused map[string]bool) []Decision {
	pol := CurrentBehavior().Models
	var out []Decision
	for _, h := range live.Fleet {
		byID := map[string]FleetThread{}
		for _, t := range h.Threads {
			byID[t.ThreadID] = t
		}
		for _, t := range h.Threads {
			if !pol.WorkerOnlyModel(t.Model) || pausedThread(paused, t) {
				continue
			}
			if t.Lineage == nil || t.Lineage.Relation != "subagent" {
				out = append(out, Decision{Class: ClassAct, Rule: "model-opus-thread", Reason: "threads run their pair's advisor; workers only as delegate_task subagents",
					Host: h.Host, Thread: t.ThreadID, Model: pol.Advisor(pol.PairOrLeader(t.Model), false)})
				continue
			}
			parent, known := byID[t.Lineage.Parent]
			if known && !pol.IsAdvisor(parent.Model) && !pausedThread(paused, parent) && now.Sub(last["noadvisor:"+parent.ThreadID]) >= nudgeEvery {
				out = append(out, Decision{Class: ClassEscalate, Rule: "haiku-without-advisor", Sig: "noadvisor",
					Reason: fmt.Sprintf("worker %s under a %s parent; every worker needs its pair's advisor", t.Model, parent.Model),
					Host:   h.Host, Thread: parent.ThreadID})
			}
		}
	}
	return out
}

// pausedThread reports whether a fleet thread's ticket is paused; Classify reads the ticket from the title or branch the same way.
func pausedThread(paused map[string]bool, t FleetThread) bool {
	return paused[strings.ToUpper(ticketPattern.FindString(t.Title+" "+t.Branch))]
}

// nudge wakes a thread with its rendered template, naming the thread's pair; a template that will not render is recorded instead.
func nudge(host, thread, model, kind, rule, reason string, data any) Decision {
	text, err := wakeText(rule, data, model)
	if err != nil {
		return Decision{Class: ClassRecord, Rule: rule, Reason: "template failed: " + err.Error(), Host: host, Thread: thread}
	}
	return Decision{Class: ClassAct, Rule: rule, Reason: reason, Host: host, Thread: thread, Wake: text, Sig: kind}
}

// nudgeKey is the KV key of the last nudge of one kind to one thread.
func nudgeKey(kind, thread string) string {
	sum := sha256.Sum256([]byte(kind + thread))
	return "nudge." + hex.EncodeToString(sum[:12])
}

// nudgeTimes reads the last nudge of each kind the policy may send this beat. A key that
// cannot be read counts as nudged now, so this beat skips that thread instead of repeating it.
func nudgeTimes(ctx context.Context, kv jetstream.KeyValue, now time.Time, totals []UsageTotal, live Live) map[string]time.Time {
	last := map[string]time.Time{}
	read := func(kind, thread string) {
		entry, err := kv.Get(ctx, nudgeKey(kind, thread))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return
		}
		if err != nil {
			last[kind+":"+thread] = now
			return
		}
		if at, err := time.Parse(time.RFC3339, string(entry.Value())); err == nil {
			last[kind+":"+thread] = at
		}
	}
	for _, u := range totals {
		if u.Thread != "" {
			read("share", u.Thread)
			read("noworkers", u.Thread)
			read("wrong-worker", u.Thread)
		}
	}
	for _, h := range live.Fleet {
		for _, t := range h.Threads {
			if t.Lineage != nil && CurrentBehavior().Models.WorkerOnlyModel(t.Model) {
				read("noadvisor", t.Lineage.Parent)
			}
		}
	}
	return last
}

// Policy runs on the leader's beat: it reads the usage TS-826 replicated into SQLite, applies the
// model policy and logs one AutopilotEntry per decision.
func (r *RulesEngine) Policy(ctx context.Context, live Live) {
	kv, err := r.JS.KeyValue(ctx, autopilotSettings)
	if err != nil {
		r.Publish(ctx, AutopilotEntry{Kind: "error", Action: "Open autopilot settings", Reason: publicText(err)})
		return
	}
	paused, err := pausedSet(ctx, kv)
	if err != nil {
		r.Publish(ctx, AutopilotEntry{Kind: "error", Action: "Read paused set", Reason: publicText(err)})
		return
	}
	now := time.Now()
	rows, err := r.Store.Usage(ctx, now.UTC().AddDate(0, 0, -1).Format("2006-01-02"))
	if err != nil {
		r.Publish(ctx, AutopilotEntry{Kind: "error", Action: "Read thread usage", Reason: publicText(err)})
		return
	}
	totals := SummarizeUsage(rows, live, "thread")
	last := nudgeTimes(ctx, kv, now, totals, live)
	decisions := append(usagePolicy(totals, seenThreads(rows), now, last, paused), fleetModelPolicy(live, now, last, paused)...)
	seen := map[string]bool{}
	for _, d := range decisions {
		if seen[d.Rule+d.Thread] {
			continue // usage and fleet can name the same thread; act once per beat
		}
		seen[d.Rule+d.Thread] = true
		if err := r.execute(ctx, "beat", d, nil, "", WakeMemory{}); err != nil {
			r.Publish(ctx, AutopilotEntry{Kind: "error", Event: "beat", Target: decisionTarget(d), Action: d.Rule, Reason: d.Reason + "; " + publicText(err)})
			continue
		}
		if d.Sig != "" {
			if _, err := kv.Put(ctx, nudgeKey(d.Sig, d.Thread), []byte(now.Format(time.RFC3339))); err != nil {
				slog.Warn("nudge window not recorded", "thread", d.Thread, "kind", d.Sig, "error", err)
			}
		}
	}
}
