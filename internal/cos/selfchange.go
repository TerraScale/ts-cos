package cos

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
)

// The self-change loop: the COS agent turns "X is broken / change Y" into
// `cos self change`, and the cluster leader drives it without further input:
// work item key -> branch with a failing acceptance check -> worker thread -> CI +
// e2e gate -> squash merge -> rollout -> check on every node -> "Updated" in chat.
// Records live in a replicated KV bucket, so a failover resumes where it stopped.
const (
	selfBucket          = "COS_SELF"
	selfTick            = 15 * time.Second
	selfRolloutDeadline = 45 * time.Minute
	selfVerifyDeadline  = 5 * time.Minute
	selfE2ERetry        = 10 * time.Minute
	selfMaxOpen         = 3 // self-changes that may be unfinished at once
)

// States in order; done, failed and blocked are final.
const (
	selfQueued   = "queued"   // waiting for a ticket (or, for an undo, a revert PR)
	selfTicketed = "ticketed" // writing the acceptance check
	selfChecked  = "checked"  // check fails on the live cluster; launching the worker
	selfWorking  = "working"  // worker thread is building the PR
	selfGating   = "gating"   // PR open; waiting for CI, e2e and review
	selfMerged   = "merged"   // waiting for the rollout
	selfLive     = "live"     // rolled out; running the check on every node
	selfDone     = "done"
	selfFailed   = "failed"
	selfBlocked  = "blocked" // a guardrail stopped it; needs the user
)

// Self-change workers are Opus 5.5 managers; Haiku 5.5 only runs as their delegate_task subagents.
var selfModel = map[string]any{"instanceId": "claudeAgent", "model": "claude-opus-5-5", "options": []map[string]string{{"id": "effort", "value": "high"}}}

type selfRecord struct {
	ID         string    `json:"id"`
	Request    string    `json:"request"`
	Summary    string    `json:"summary"`
	Check      selfCheck `json:"check"`
	Reverts    string    `json:"reverts,omitempty"`
	RevertedBy string    `json:"revertedBy,omitempty"`
	Ticket     string    `json:"ticket,omitempty"`
	Branch     string    `json:"branch,omitempty"`
	Host       string    `json:"host,omitempty"`
	Thread     string    `json:"thread,omitempty"`
	PR         int       `json:"pr,omitempty"`
	PRURL      string    `json:"prUrl,omitempty"`
	SHA        string    `json:"sha,omitempty"`
	E2ESHA     string    `json:"e2eSha,omitempty"` // PR head the full e2e suite last ran for
	E2EFail    string    `json:"e2eFail,omitempty"`
	E2EAt      time.Time `json:"e2eAt,omitzero"`
	State      string    `json:"state"`
	Note       string    `json:"note,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	MergedAt   time.Time `json:"mergedAt,omitzero"`
	LiveAt     time.Time `json:"liveAt,omitzero"`
	VerifiedAt time.Time `json:"verifiedAt,omitzero"`
}

func (r selfRecord) final() bool {
	return r.State == selfDone || r.State == selfFailed || r.State == selfBlocked
}

func (r selfRecord) title() string {
	if r.Reverts != "" {
		return "Revert " + r.Summary
	}
	return r.Summary
}

func (r selfRecord) note() string {
	if r.Reverts != "" {
		return "Reverted: " + r.Summary
	}
	return r.Summary
}

func (r selfRecord) entry() SelfChange {
	at := r.VerifiedAt
	if at.IsZero() {
		at = r.CreatedAt
	}
	return SelfChange{Summary: r.Summary, SHA: r.SHA, Ticket: r.Ticket, PR: r.PRURL, At: at, Reverted: r.RevertedBy != ""}
}

func selfBranch(key string) string { return "cos/" + strings.ToLower(key) }

// selfStore is the replicated changelog and queue.
type selfStore struct{ kv jetstream.KeyValue }

func openSelfStore(ctx context.Context, bus *Bus, create bool) (selfStore, error) {
	js, err := bus.JetStream()
	if err != nil {
		return selfStore{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var kv jetstream.KeyValue
	if create {
		kv, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: selfBucket, History: 8, Replicas: 3, Storage: jetstream.FileStorage})
		if err == nil {
			err = selfLeaderReads(ctx, js)
		}
		if err == nil {
			kv, err = js.KeyValue(ctx, selfBucket)
		}
	} else {
		kv, err = js.KeyValue(ctx, selfBucket)
	}
	if err != nil {
		return selfStore{}, fmt.Errorf("COS self-change store unavailable; is cos serve running? (%w)", err)
	}
	return selfStore{kv}, nil
}

// selfLeaderReads turns off direct gets on COS_SELF: a direct get may be answered by a
// lagging replica, and a stale record would repeat a step's side effects (ticket, launch).
// CreateOrUpdateKeyValue turns them back on, so the leader reapplies this on every open.
func selfLeaderReads(ctx context.Context, js jetstream.JetStream) error {
	s, err := js.Stream(ctx, "KV_"+selfBucket)
	if err != nil {
		return err
	}
	cfg := s.CachedInfo().Config
	if !cfg.AllowDirect && !cfg.MirrorDirect {
		return nil
	}
	cfg.AllowDirect, cfg.MirrorDirect = false, false
	_, err = js.UpdateStream(ctx, cfg)
	return err
}

func (s selfStore) list(ctx context.Context) ([]selfRecord, error) {
	keys, err := s.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	var out []selfRecord
	for k := range keys.Keys() {
		if k == "seq" {
			continue
		}
		e, err := s.kv.Get(ctx, k)
		var r selfRecord
		if err == nil && json.Unmarshal(e.Value(), &r) == nil {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b selfRecord) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func (s selfStore) get(ctx context.Context, id string) (selfRecord, uint64, error) {
	e, err := s.kv.Get(ctx, id)
	if err != nil {
		return selfRecord{}, 0, err
	}
	var r selfRecord
	return r, e.Revision(), json.Unmarshal(e.Value(), &r)
}

func (s selfStore) create(ctx context.Context, r selfRecord) error {
	data, _ := json.Marshal(r)
	_, err := s.kv.Create(ctx, r.ID, data)
	return err
}

// save writes r only if nobody else changed it since rev (leader failover safety).
func (s selfStore) save(ctx context.Context, r selfRecord, rev uint64) error {
	data, _ := json.Marshal(r)
	_, err := s.kv.Update(ctx, r.ID, data, rev)
	return err
}

// nextKey allocates the next self-change key (SELF-<n>) from a counter in COS_SELF;
// ts-cos tracks its own work, so there is no external ticket.
func (s selfStore) nextKey(ctx context.Context) (string, error) {
	for range 10 {
		n, rev := 0, uint64(0)
		e, err := s.kv.Get(ctx, "seq")
		switch {
		case err == nil:
			n, _ = strconv.Atoi(string(e.Value()))
			rev = e.Revision()
		case !errors.Is(err, jetstream.ErrKeyNotFound):
			return "", err
		}
		value := []byte(strconv.Itoa(n + 1))
		if rev == 0 {
			_, err = s.kv.Create(ctx, "seq", value)
		} else {
			_, err = s.kv.Update(ctx, "seq", value, rev)
		}
		if err == nil {
			return fmt.Sprintf("SELF-%d", n+1), nil
		}
	}
	return "", errors.New("self-change key allocation kept conflicting")
}

func selfID(now time.Time) string { return fmt.Sprintf("%016x", now.UnixNano()) }

// selfRequest validates what the COS agent asks for.
func selfRequest(raw string) (selfRecord, error) {
	var in struct {
		Request string    `json:"request"`
		Summary string    `json:"summary"`
		Check   selfCheck `json:"check"`
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return selfRecord{}, fmt.Errorf("self change JSON: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return selfRecord{}, errors.New("self change JSON: trailing data")
	}
	switch {
	case strings.TrimSpace(in.Request) == "" || utf8.RuneCountInString(in.Request) > 4000 || hasControl(in.Request, "\n\t"):
		return selfRecord{}, errors.New("request must be the user's words (1-4000 characters, no control characters)")
	case strings.TrimSpace(in.Summary) == "" || utf8.RuneCountInString(in.Summary) > 120 || hasControl(in.Summary, ""):
		return selfRecord{}, errors.New("summary must be one user-facing line of 1-120 characters")
	}
	if err := in.Check.validate(); err != nil {
		return selfRecord{}, err
	}
	now := time.Now().UTC()
	return selfRecord{ID: selfID(now), Request: in.Request, Summary: in.Summary, Check: in.Check, State: selfQueued, CreatedAt: now}, nil
}

// hasControl reports whether s has a control character other than those in allow.
func hasControl(s, allow string) bool {
	return strings.IndexFunc(s, func(c rune) bool { return unicode.IsControl(c) && !strings.ContainsRune(allow, c) }) >= 0
}

// selfReverted reports whether a finished revert of id exists, even if the original lost its RevertedBy.
func selfReverted(records []selfRecord, id string) bool {
	for _, r := range records {
		if r.Reverts == id && r.State == selfDone {
			return true
		}
	}
	return false
}

// selfUndo queues a revert of target (default: the newest live change not yet reverted).
func selfUndo(records []selfRecord, target string) (selfRecord, error) {
	var orig *selfRecord
	for i := len(records) - 1; i >= 0; i-- {
		r := records[i]
		if r.Reverts == "" && r.State == selfDone && r.RevertedBy == "" && !selfReverted(records, r.ID) && (target == "" || target == r.Ticket || target == r.ID) {
			orig = &records[i]
			break
		}
	}
	if orig == nil {
		return selfRecord{}, errors.New("no live self-change to undo")
	}
	for _, r := range records {
		if r.Reverts == orig.ID && !r.final() {
			return selfRecord{}, fmt.Errorf("%s is already being reverted", orig.Ticket)
		}
	}
	now := time.Now().UTC()
	// The fixed ID makes a second undo of the same change fail as "key exists" in the store.
	return selfRecord{ID: orig.ID + "-undo", Request: "undo " + orig.Ticket, Summary: orig.Summary, Check: orig.Check,
		Reverts: orig.ID, Ticket: orig.Ticket, Branch: selfBranch(orig.Ticket) + "-revert", State: selfQueued, CreatedAt: now}, nil
}

// selfDriver advances every open self-change one step per tick on the leader.
type selfDriver struct {
	cfg   Config
	bus   *Bus
	store selfStore
	f     Forgejo
	now   func() time.Time
	// External steps; replaced in tests.
	ticket  func(context.Context, selfRecord) (string, error)
	launch  func(context.Context, selfRecord, string) (host, thread string, err error)
	check   func(context.Context, selfCheck) ([]selfCheckResult, bool, error)
	rollout func(context.Context, selfRecord) (done bool, failure string, err error)
	e2e     func(ctx context.Context, skip string) (failures string)
	say     func(context.Context, string)
	finish  func(context.Context, selfRecord)
}

func newSelfDriver(cfg Config, bus *Bus, store selfStore) *selfDriver {
	d := &selfDriver{cfg: cfg, bus: bus, store: store, f: Forgejo{Config: cfg}, now: func() time.Time { return time.Now().UTC() }}
	d.launch, d.finish = d.launchWorker, d.closeOut
	d.ticket = func(ctx context.Context, _ selfRecord) (string, error) { return store.nextKey(ctx) }
	d.check = func(ctx context.Context, c selfCheck) ([]selfCheckResult, bool, error) {
		return runSelfCheck(ctx, bus, c)
	}
	d.rollout = d.rolloutDone
	d.e2e = d.runE2E
	d.say = func(ctx context.Context, text string) { selfChatPost(ctx, bus, text) }
	return d
}

// runSelfChanges drives self-changes while this node leads. It is a serve worker.
func runSelfChanges(ctx context.Context, cfg Config, bus *Bus) {
	bus.Lead(ctx, func(ctx context.Context) {
		store, err := openSelfStore(ctx, bus, true)
		if err != nil {
			slog.Warn("self-change store unavailable", "error", err)
			return
		}
		d := newSelfDriver(cfg, bus, store)
		w, err := store.kv.WatchAll(ctx, jetstream.UpdatesOnly())
		if err != nil {
			return
		}
		defer w.Stop()
		tick := time.NewTicker(selfTick)
		defer tick.Stop()
		for {
			d.step(ctx)
			select {
			case <-ctx.Done():
				return
			case <-w.Updates(): // a new request or undo starts at once
			case <-tick.C:
			}
		}
	})
}

func init() { serveWorkers = append(serveWorkers, runSelfChanges) }

func (d *selfDriver) step(ctx context.Context) {
	records, err := d.store.list(ctx)
	if err != nil {
		slog.Warn("self-change list failed", "error", err)
		return
	}
	rolling := ""
	for _, r := range records {
		if r.State == selfMerged || r.State == selfLive {
			rolling = r.Ticket
		}
	}
	for _, r := range records {
		if r.final() {
			continue
		}
		cur, rev, err := d.store.get(ctx, r.ID)
		if err != nil || cur.final() {
			continue
		}
		next, err := d.advance(ctx, cur, rolling)
		if err != nil {
			next.Note = "retrying: " + selfClip(err.Error())
			slog.Warn("self-change step failed", "id", cur.ID, "state", cur.State, "error", err)
		}
		if reflect.DeepEqual(next, cur) {
			continue
		}
		if err := d.store.save(ctx, next, rev); err != nil {
			continue
		}
		if next.State != cur.State {
			d.progress(ctx, next)
		}
		if next.State == selfMerged {
			rolling = next.Ticket
		}
		if next.final() && rolling == next.Ticket {
			rolling = ""
		}
	}
}

// advance performs one idempotent step and returns the updated record.
func (d *selfDriver) advance(ctx context.Context, r selfRecord, rolling string) (selfRecord, error) {
	now := d.now()
	switch r.State {
	case selfQueued:
		if r.Reverts != "" {
			orig, _, err := d.store.get(ctx, r.Reverts)
			if err != nil {
				return r, err
			}
			p, err := d.f.selfRevertPR(ctx, orig.SHA, orig.PR, r.Branch, r.Ticket+": "+r.title(), "Part of "+r.Ticket+". Undo requested in chat.")
			if err != nil {
				if strings.Contains(err.Error(), "changed on main") {
					return r.to(selfBlocked, "cannot revert automatically: "+err.Error()), nil
				}
				return r, err
			}
			r.PR, r.PRURL = p.Number, p.URL
			return r.to(selfGating, p.URL), nil
		}
		ticket, err := d.ticket(ctx, r)
		if err != nil {
			return r, err
		}
		r.Ticket, r.Branch = ticket, selfBranch(ticket)
		return r.to(selfTicketed, ""), nil
	case selfTicketed:
		if err := d.f.selfStartBranch(ctx, r.Branch, r.Ticket, r.Check); err != nil {
			return r, err
		}
		results, passed, err := d.check(ctx, r.Check)
		if err != nil {
			return r, err
		}
		if passed {
			return r.to(selfBlocked, "the acceptance check already passes on the live cluster; nothing to change"), nil
		}
		if !failedEverywhere(results) {
			return r, errors.New("not every node answered the acceptance check: " + selfEvidence(results))
		}
		return r.to(selfChecked, selfEvidence(results)), nil
	case selfChecked:
		host, thread, err := d.launch(ctx, r, d.brief(r))
		if err != nil {
			return r, err
		}
		r.Host, r.Thread = host, thread
		return r.to(selfWorking, host), nil
	case selfWorking:
		p, err := d.f.selfFindPR(ctx, r.Branch)
		if err != nil || p == nil {
			return r, err
		}
		r.PR, r.PRURL = p.Number, p.URL
		if r.Thread != "" {
			_ = d.bus.Call(ctx, r.Host, "watch", []string{r.Thread, p.URL}, nil)
		}
		return r.to(selfGating, p.URL), nil
	case selfGating:
		p, err := d.f.selfGetPR(ctx, r.PR)
		if err != nil {
			return r, err
		}
		if p.Merged {
			r.SHA, r.MergedAt = p.MergeSHA, now
			return r.to(selfMerged, p.MergeSHA), nil
		}
		if p.State != "open" {
			return r.to(selfFailed, "PR closed without merge"), nil
		}
		wait, block, err := d.f.selfGate(ctx, r, p)
		switch {
		case err != nil:
			return r, err
		case block != "":
			return r.to(selfBlocked, block), nil
		case wait != "":
			r.Note = wait
			return r, nil
		case rolling != "" && rolling != r.Ticket:
			r.Note = "green; queued behind " + rolling + " (one rollout at a time)"
			return r, nil
		case r.E2ESHA != p.Head.SHA || r.E2EFail != "" && now.Sub(r.E2EAt) > selfE2ERetry:
			// Full live suite; the change's own check cannot pass before it is deployed.
			r.E2ESHA, r.E2EAt, r.E2EFail = p.Head.SHA, now, d.e2e(ctx, selfE2EID(r.Ticket))
			r.Note = "e2e green"
			if r.E2EFail != "" {
				r.Note = "e2e failing (rerun in 10 min): " + selfClip(r.E2EFail)
			}
			return r, nil
		case r.E2EFail != "":
			return r, nil
		}
		sha, err := d.f.selfMerge(ctx, r, p)
		if err != nil {
			return r, err
		}
		r.SHA, r.MergedAt = sha, now
		return r.to(selfMerged, sha), nil
	case selfMerged:
		done, failure, err := d.rollout(ctx, r)
		switch {
		case err != nil:
			return r, err
		case failure != "":
			return r.to(selfFailed, "rolled back: "+failure), nil
		case !done && now.Sub(r.MergedAt) > selfRolloutDeadline:
			return r.to(selfFailed, "rollout did not finish within 45 minutes"), nil
		case !done:
			r.Note = "rolling out"
			return r, nil
		}
		r.LiveAt = now
		return r.to(selfLive, ""), nil
	case selfLive:
		results, passed, err := d.check(ctx, r.Check)
		if err != nil {
			return r, err
		}
		ok := passed
		if r.Reverts != "" {
			ok = failedEverywhere(results)
		}
		if !ok {
			if now.Sub(r.LiveAt) > selfVerifyDeadline {
				return r.to(selfFailed, "live check failed: "+selfEvidence(results)), nil
			}
			r.Note = "checking: " + selfEvidence(results)
			return r, nil
		}
		r.VerifiedAt = now
		return r.to(selfDone, ""), nil
	}
	return r, nil
}

func (r selfRecord) to(state, note string) selfRecord {
	r.State, r.Note = state, note
	return r
}

// runE2E runs the full live suite from this node and lists the failed checks.
func (d *selfDriver) runE2E(ctx context.Context, skip string) string {
	env := &E2EEnv{cfg: d.cfg, run: d.cfg.hostRun, main: func(ctx context.Context) (e2eMain, error) { return mainHead(ctx, d.cfg) }}
	checks := slices.DeleteFunc(e2eChecks(), func(c E2ECheck) bool { return c.ID == skip })
	var failed []string
	for _, c := range runE2E(ctx, env, checks, nil).Checks {
		if c.Status == "fail" {
			failed = append(failed, c.ID+": "+c.Evidence)
		}
	}
	return strings.Join(failed, "; ")
}

// progress posts one short chat line per state change, and the final Updated/Reverted line.
func (d *selfDriver) progress(ctx context.Context, r selfRecord) {
	b := CurrentBehavior()
	var line string
	var err error
	switch {
	case r.State == selfDone && r.Reverts != "":
		line, err = b.Render("reverted", r.entry())
	case r.State == selfDone:
		line, err = b.Render("updated", r.entry())
	default:
		steps := map[string]string{selfTicketed: "started", selfChecked: "acceptance check fails on main as expected",
			selfWorking: "worker started", selfGating: "PR open, waiting for CI and e2e", selfMerged: "merged, rolling out",
			selfLive: "live, checking every node", selfFailed: "stopped", selfBlocked: "needs you"}
		detail := r.Note
		if r.State == selfGating || r.State == selfMerged {
			detail = ""
		}
		line, err = b.Render("progress", SelfChangeStep{Ticket: r.Ticket, Step: steps[r.State], Detail: selfClip(detail)})
	}
	if err != nil {
		slog.Warn("self-change template failed", "error", err)
		return
	}
	d.say(ctx, line)
	if r.final() {
		d.finish(ctx, r)
	}
}

// closeOut closes the worker thread, files an Inbox item for a stopped change and links an undo to its change.
func (d *selfDriver) closeOut(ctx context.Context, r selfRecord) {
	if r.State == selfDone && r.Thread != "" {
		_ = d.bus.Call(ctx, r.Host, "close", []string{r.Thread}, nil)
	}
	if r.State == selfBlocked || r.State == selfFailed {
		in, err := OpenInbox(ctx, d.bus, d.cfg.LocalHost, false)
		if err == nil {
			_, err = in.Add(ctx, InboxInput{Kind: "blocker", Source: "cos", Key: "selfchange:" + r.ID, Ticket: r.Ticket,
				Title:   oneLine("Self-change "+r.State+": "+r.Summary, inboxTitleMax),
				Context: r.Note + "\n\nYou asked: " + r.Request, Links: slices.DeleteFunc([]string{r.PRURL}, func(s string) bool { return s == "" }),
				Options: []string{"Open a normal reviewed PR (Recommended)", "Drop it"}})
		}
		if err != nil {
			slog.Warn("self-change Inbox item failed", "error", err)
		}
	}
	if r.State == selfDone && r.Reverts != "" {
		if orig, rev, err := d.store.get(ctx, r.Reverts); err == nil {
			orig.RevertedBy = r.ID
			_ = d.store.save(ctx, orig, rev)
		}
	}
}

func (d *selfDriver) brief(r selfRecord) string {
	text, err := CurrentBehavior().Render("selfbrief", SelfBrief{Ticket: r.Ticket, Branch: r.Branch, Summary: r.Summary,
		Request: r.Request, CheckFile: selfCheckFile(r.Ticket), Check: strings.TrimSpace(selfCheckJSON(r.Check))})
	if err != nil {
		return err.Error()
	}
	return text
}

// launchWorker starts the worker on the least busy host that has the ts-cos project.
func (d *selfDriver) launchWorker(ctx context.Context, r selfRecord, brief string) (string, string, error) {
	fleet, err := Fleet(ctx, d.bus)
	if err != nil {
		return "", "", err
	}
	// A lost save leaves the worker running without the record; adopt it rather than start a second.
	for _, h := range fleet {
		for _, thread := range h.Threads {
			if h.Error == "" && strings.HasPrefix(thread.Title, r.Ticket+" · ") {
				return h.Host, thread.ThreadID, nil
			}
		}
	}
	input, _ := json.Marshal(map[string]any{
		"projectTitle": "ts-cos", "title": r.Ticket + " · ts-cos: " + r.Summary, "modelSelection": selfModel, "message": brief,
		"workspaceStrategy": map[string]any{"type": "worktree", "baseRef": r.Branch, "branch": r.Branch, "startFromOrigin": true},
	})
	var last error = errors.New("no COS host can launch a worker")
	for _, h := range fleet {
		if h.Error != "" {
			continue
		}
		var out struct {
			ThreadID string `json:"threadId"`
		}
		if last = d.bus.Call(ctx, h.Host, "launch", []string{string(input)}, &out); last == nil && out.ThreadID != "" {
			return h.Host, out.ThreadID, nil
		}
	}
	return "", "", last
}

// selfQueueUndo stores an undo record. One undo per change: a failed or blocked one is replaced, a running or finished one is kept.
func selfQueueUndo(ctx context.Context, store selfStore, r selfRecord) error {
	err := store.create(ctx, r)
	if !errors.Is(err, jetstream.ErrKeyExists) {
		return err
	}
	old, rev, gerr := store.get(ctx, r.ID)
	if gerr != nil || !(old.State == selfFailed || old.State == selfBlocked) {
		return fmt.Errorf("%s is already being reverted", r.Ticket)
	}
	return store.save(ctx, r, rev)
}

// selfCLI implements `cos self change|undo|log`; the COS agent is its main caller.
func selfCLI(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	usage := errors.New("usage: cos self change '<json: request, summary, check>' | undo [ticket] | log [all]")
	if len(args) == 0 {
		return usage
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	store, err := openSelfStore(ctx, bus, false)
	if err != nil {
		return err
	}
	switch {
	case args[0] == "change" && len(args) == 2:
		r, err := selfRequest(args[1])
		if err != nil {
			return err
		}
		records, err := store.list(ctx)
		if err != nil {
			return err
		}
		inProgress := 0
		for _, o := range records {
			if !o.final() {
				inProgress++
			}
		}
		if inProgress >= selfMaxOpen {
			return fmt.Errorf("%d self-changes are already in progress; wait for one to finish", selfMaxOpen)
		}
		for _, o := range records {
			if !o.final() && strings.TrimSpace(o.Request) == strings.TrimSpace(r.Request) {
				return fmt.Errorf("the same request is already in progress as %s", cmp.Or(o.Ticket, o.ID))
			}
		}
		if err := store.create(ctx, r); err != nil {
			return err
		}
		return output(w, map[string]any{"id": r.ID, "state": r.State, "next": "COS posts progress lines in the chat and \"Updated\" once it is live on every node"})
	case args[0] == "undo" && len(args) <= 2:
		records, err := store.list(ctx)
		if err != nil {
			return err
		}
		r, err := selfUndo(records, strings.Join(args[1:], ""))
		if err != nil {
			return err
		}
		if err := selfQueueUndo(ctx, store, r); err != nil {
			return err
		}
		return output(w, map[string]any{"id": r.ID, "reverts": r.Ticket, "state": r.State})
	case args[0] == "log" && (len(args) == 1 || len(args) == 2 && args[1] == "all"):
		records, err := store.list(ctx)
		if err != nil {
			return err
		}
		return output(w, selfLog(records, len(args) == 1, time.Now()))
	}
	return usage
}

// selfLog answers "what changed today?": every self-change with its ticket, PR, commit and verified time.
func selfLog(records []selfRecord, today bool, now time.Time) map[string]any {
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	shown, live := []selfRecord{}, []SelfChange{}
	for _, r := range records {
		if today && r.CreatedAt.Before(day) && (r.VerifiedAt.IsZero() || r.VerifiedAt.Before(day)) {
			continue
		}
		shown = append(shown, r)
		if r.State == selfDone {
			e := r.entry()
			if r.Reverts != "" {
				e.Summary = "Undo: " + e.Summary
			}
			live = append(live, e)
		}
	}
	text, err := CurrentBehavior().Render("changes", live)
	if err != nil {
		text = err.Error()
	}
	return map[string]any{"text": text, "changes": shown}
}

func init() {
	cliCommands["self"] = cliCommand{Usage: "self change '<json: request, summary, check>' | undo [ticket] | log [all]", Run: selfCLI}
}
