package cos

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Fixed values (user decision: no knobs). Every message COS sends to a thread passes Gate.Send.
const (
	sendqBucket    = "COS_SENDQ" // per-thread queue ("q.<hash>") and delivery history ("d.<hash>")
	sendCap        = 3           // automatic messages per thread per sendCapWindow
	sendCapWindow  = time.Hour
	sendStagger    = 20 * time.Second // between queued deliveries of one model once it has headroom
	sendJitter     = 15 * time.Second
	sendTick       = 5 * time.Second
	sendClaimStale = 2 * time.Minute  // a claim this old is re-checked against the thread before delivery
	sendSkewMax    = 30 * time.Minute // a ReadyAt further ahead than this was written by a skewed clock
	sendTextMax    = 8000
	sendInboxMax   = 3500
	sendLimitSlack = 5 * time.Second // a CLIProxyAPI read this close after a usage-limit failure may predate its cooldown
	sendHeld       = "COS held: "    // prefix of every refusal error; survives the bus as text
)

// solPair is the routine pair; a P0 stalled on Opus moves to it. p0Wait is how long a P0 waits for Opus headroom first.
const (
	solPair     = "sol"
	p0Wait      = 30 * time.Minute
	p0MovedLine = "COS moved this P0 from Opus to Sol: Opus had no headroom for over 30 min."
)

var sendArgPattern = regexp.MustCompile(`^(queue|user|deliver:[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12})$`)

// queuedSend is the one held entry of a thread; every message that waits merges into it.
type queuedSend struct {
	Host      string    `json:"host"`
	Thread    string    `json:"thread"`
	Title     string    `json:"title"`
	Model     string    `json:"model"`
	Kind      string    `json:"kind"` // auto | user | resume
	Texts     []string  `json:"texts"`
	Queued    time.Time `json:"queued"`
	NotBefore time.Time `json:"notBefore,omitzero"`
	ReadyAt   time.Time `json:"readyAt,omitzero"`
	Claimed   time.Time `json:"claimed,omitzero"`
	Reason    string    `json:"reason,omitempty"`
	MessageID string    `json:"messageId,omitempty"`
	Error     string    `json:"error,omitempty"`
}

// sendFacts is what the gate reads about a thread before it sends.
type sendFacts struct {
	Gone      bool   // not found, deleted, archived or settled
	Child     bool   // a delegated subagent: only its parent advisor wakes it
	Status    string // latest run status, "" when idle
	LimitHold bool   // the last run stopped on a limit and CLIProxyAPI has not read since
}

// decideSend is the gate's decision for one send. headroom is the model's headroom; queued means the thread
// already has a waiting entry; recent counts automatic sends in the cap window; steer asks to join a running
// run; deliver marks the leader's delivery of a held entry. A "queue" for headroom has an empty reason: the caller fills it.
func decideSend(f sendFacts, headroom, queued bool, recent int, kind string, steer, deliver bool) (action, reason string) {
	switch {
	case f.Gone:
		return "drop", "thread is gone or settled"
	case f.Child && kind != "user":
		return "drop", "delegated subagent: only its parent advisor wakes it"
	case !headroom:
		return "queue", ""
	case f.LimitHold:
		return "queue", "last run stopped on a limit; waiting for a newer CLIProxyAPI read"
	case queued && !deliver:
		return "queue", "an earlier message is waiting"
	case f.Status == "preparing" || f.Status == "queued" || f.Status == "starting":
		return "queue", "thread is starting a run"
	case kind != "user" && recent >= sendCap:
		return "cap", fmt.Sprintf("%d automatic messages in the last hour", recent)
	case f.Status == "running" || f.Status == "waiting":
		if steer || deliver {
			return "steer", ""
		}
		return "queue", "thread is running"
	}
	return "start", ""
}

// sendDispatch builds the message.dispatch command for one send.
func sendDispatch(thread, messageID, text string, dispatchMode map[string]any) map[string]any {
	return map[string]any{"type": "message.dispatch", "createdBy": "user", "creationSource": "server", "threadId": thread, "messageId": messageID, "text": text, "attachments": []any{}, "dispatchMode": dispatchMode}
}

// sendKeys names a thread's queue and history entries; hashing keeps thread IDs out of KV keys.
func sendKeys(host, thread string) (queue, history string) {
	sum := sha256.Sum256([]byte(host + "\x00" + thread))
	h := hex.EncodeToString(sum[:])[:24]
	return "q." + h, "d." + h
}

// gateT3 is the part of the T3 client the gate uses; tests fake it.
type gateT3 interface {
	Projection(context.Context, string) (Projection, error)
	Dispatch(context.Context, map[string]any) (any, error)
}

// gatedCommands are the commands whose messages or model changes pass the gate.
var gatedCommands = []string{"send", "launch", "handoff", "compact", "dispatch"}

// Gate is the one path for messages COS sends to threads. Its dependencies are fields, so tests run without T3, NATS or CLIProxyAPI.
type Gate struct {
	Host   string
	T3     func(context.Context) (gateT3, error)
	Local  func(ctx context.Context, command string, args []string) (any, error) // gated commands that run after the checks
	Limits func(context.Context) (LimitsState, error)
	Queue  func(context.Context) (jetstream.KeyValue, error)
	File   func(context.Context, InboxInput) error
	Now    func() time.Time

	mu    sync.Mutex
	locks map[string]*sync.Mutex // one per thread: sends to a thread are serialized on this node
}

// GateResult is what a send reports: the usual send fields plus what the gate did.
type GateResult struct {
	SendResult
	Action    string    `json:"action"` // delivered | queued | busy | held | dropped
	Reason    string    `json:"reason,omitempty"`
	Until     time.Time `json:"until,omitzero"`
	Duplicate bool      `json:"duplicate,omitempty"`
}

// newSendGate binds the gate to the local T3 and the cluster buckets.
func newSendGate(cfg Config, bus *Bus) *Gate {
	return &Gate{
		Host: cfg.LocalHost,
		T3: func(ctx context.Context) (gateT3, error) {
			c, err := OpenT3(ctx, cfg, cfg.LocalHost)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		Local: func(ctx context.Context, command string, args []string) (any, error) {
			return LocalCommand(ctx, cfg, command, args)
		},
		Limits: func(ctx context.Context) (LimitsState, error) {
			kv, err := openLimits(ctx, bus)
			if err != nil {
				return LimitsState{}, err
			}
			return readLimits(ctx, kv)
		},
		Queue: func(ctx context.Context) (jetstream.KeyValue, error) { return openSendQueue(ctx, bus) },
		File: func(ctx context.Context, in InboxInput) error {
			inbox, err := OpenInbox(ctx, bus, cfg.LocalHost, false)
			if err != nil {
				return err
			}
			_, err = inbox.Add(ctx, in)
			return err
		},
		Now: time.Now,
	}
}

// openSendQueue binds COS_SENDQ; the leader creates it on first use.
func openSendQueue(ctx context.Context, bus *Bus) (jetstream.KeyValue, error) {
	return openKVConfig(ctx, bus, jetstream.KeyValueConfig{Bucket: sendqBucket, History: 1})
}

func (g *Gate) lock(thread string) *sync.Mutex {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.locks == nil {
		g.locks = map[string]*sync.Mutex{}
	}
	m := g.locks[thread]
	if m == nil {
		m = &sync.Mutex{}
		g.locks[thread] = m
	}
	return m
}

// Send sends, queues, caps or drops one message. arg is "" (steer, automatic), "queue" (after the active run,
// automatic), "user" (steer, answer to the Inbox) or "deliver:<messageId>" (the leader's delivery of a held entry).
func (g *Gate) Send(ctx context.Context, thread, text, arg string) (GateResult, error) {
	m := g.lock(thread)
	m.Lock()
	defer m.Unlock()
	now := g.Now()
	res := GateResult{SendResult: SendResult{Host: g.Host, ThreadID: thread}}
	deliverID, deliver := strings.CutPrefix(arg, "deliver:")
	c, err := g.T3(ctx)
	if err != nil {
		return res, err
	}
	p, err := c.Projection(ctx, thread)
	var gone *T3Error
	if errors.As(err, &gone) && gone.NotFound() {
		res.Action, res.Reason = "dropped", "thread is gone or settled"
		return res, nil
	}
	if err != nil {
		return res, err
	}
	limits, err := g.Limits(ctx)
	if err != nil {
		return res, err
	}
	kv, err := g.Queue(ctx)
	if err != nil {
		return res, err
	}
	queueKey, historyKey := sendKeys(g.Host, thread)
	var entry queuedSend
	found, err := kvJSON(ctx, kv, queueKey, &entry)
	if err != nil {
		return res, err
	}
	history, err := loadSendHistory(ctx, kv, historyKey, now)
	if err != nil {
		return res, err
	}
	kind := "auto"
	switch {
	case deliver && entry.Kind != "":
		kind = entry.Kind
	case arg == "user":
		kind = "user"
	}
	if deliver && slices.ContainsFunc(p.Messages, func(msg ThreadMessage) bool { return msg.ID == deliverID }) {
		res.MessageID, res.Action, res.Duplicate = deliverID, "delivered", true
		return res, nil
	}
	t := p.Thread
	run, status := p.Run()
	ok, until, why := limits.Headroom(t.ModelSelection.Model, now)
	action, reason := decideSend(sendFacts{Gone: t.ArchivedAt != nil || t.DeletedAt != nil || t.SettledAt != nil, Child: t.Lineage.Parent != "", Status: status, LimitHold: limitHold(t, limits)},
		ok, found, len(history), kind, arg != "queue", deliver)
	if action == "queue" && !ok {
		reason = why
	}
	res.Reason = reason
	switch action {
	case "drop":
		res.Action = "dropped"
		return res, nil
	case "queue":
		if deliver {
			res.Action = "busy"
			return res, nil
		}
		res.Action, res.Until = "queued", until
		return res, casSend(ctx, kv, queueKey, func(e *queuedSend, _ bool) bool {
			mergeSend(e, queuedSend{Host: g.Host, Thread: thread, Title: threadTitleLabel(t), Model: t.ModelSelection.Model, Kind: kind, Texts: []string{text}, Reason: reason}, now)
			return true
		})
	case "cap":
		res.Action = "held"
		return res, g.File(ctx, InboxInput{Kind: "fyi", Source: "cos", Key: "sendcap:" + thread, Host: g.Host, Thread: thread,
			Title:   oneLine(fmt.Sprintf("COS held a message to %s: %d automatic messages in the last hour", threadTitleLabel(t), sendCap), inboxTitleMax),
			Context: firstRunes(text, sendInboxMax)})
	}
	dispatch := map[string]any{"type": "start_immediately"}
	if action == "steer" {
		dispatch = map[string]any{"type": "steer_active", "targetRunId": run}
	}
	messageID := deliverID
	if !deliver {
		messageID = uuid()
	}
	if _, err := c.Dispatch(ctx, sendDispatch(thread, messageID, text, dispatch)); err != nil {
		return res, err
	}
	if kind != "user" {
		if err := kvPutJSON(ctx, kv, historyKey, append(history, now)); err != nil {
			return res, err
		}
	}
	res.MessageID, res.Mode, res.Action = messageID, dispatch["type"].(string), "delivered"
	return res, nil
}

// Command runs one gated command. A send goes through Send; the others check the model's headroom and then run as before.
func (g *Gate) Command(ctx context.Context, command string, args []string) (any, error) {
	if err := validateLocal(command, args); err != nil {
		return nil, OperationError(err.Error())
	}
	switch command {
	case "send":
		arg := ""
		if len(args) == 3 {
			arg = args[2]
		}
		return g.Send(ctx, args[0], args[1], arg)
	case "handoff":
		return g.handoff(ctx, args)
	case "compact":
		if err := g.checkThread(ctx, args[0], "no compaction on a cooling model"); err != nil {
			return nil, err
		}
	case "launch":
		return g.launch(ctx, args)
	case "dispatch":
		input, err := localObjectJSON(args[0])
		if err != nil {
			return nil, err
		}
		if input["type"] == "message.dispatch" {
			thread, _ := input["threadId"].(string)
			if err := g.checkThread(ctx, thread, "no message on a cooling model"); err != nil {
				return nil, err
			}
		}
	}
	return g.Local(ctx, command, args)
}

// launch routes a launch to the pair with headroom (routeLaunch). A launch that its pair holds is refused; a P0 that has
// waited p0Wait on Opus moves to Sol with a brief, and that move is filed.
func (g *Gate) launch(ctx context.Context, args []string) (any, error) {
	input, err := localObjectJSON(args[0])
	if err != nil {
		return nil, err
	}
	pol := CurrentBehavior().Models
	pair, err := pol.PairFor(maps.Clone(input)) // PairFor removes "pair"; launchInput reads it again later
	if err != nil {
		return nil, err
	}
	limits, err := g.Limits(ctx)
	if err != nil {
		return nil, err
	}
	kv, err := g.Queue(ctx)
	if err != nil {
		return nil, err
	}
	now := g.Now()
	title, _ := input["title"].(string)
	ws, _ := input["workspaceStrategy"].(map[string]any)
	branch, _ := ws["branch"].(string)
	ticket := ticketOf(title, branch)
	heldKey := sendMarkerKey("held.", ticket)
	var heldSince time.Time
	if ticket != "" {
		if _, err := kvJSON(ctx, kv, heldKey, &heldSince); err != nil {
			return nil, err
		}
	}
	route, why, held := routeLaunch(pol, pair, ticket, limits, now, heldSince)
	if held {
		if ticket != "" {
			if _, err := putOnce(ctx, kv, heldKey, now); err != nil {
				return nil, err
			}
		}
		return nil, OperationError(sendHeld + why)
	}
	if route != pair {
		input["pair"] = route
		msg, _ := input["message"].(string)
		input["message"] = strings.TrimSpace(msg + "\n\n" + pol.BriefLine(route) + "\n" + p0MovedLine)
		raw, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		args = []string{string(raw)}
	}
	out, err := g.Local(ctx, "launch", args)
	if err != nil {
		return out, err
	}
	if !heldSince.IsZero() {
		if err := kv.Delete(ctx, heldKey); err != nil {
			logErr(ctx, slog.LevelWarn, "COS held launch marker", err, "ticket", ticket)
		}
	}
	if route != pair {
		slog.Info("COS moved a P0 to Sol", "ticket", ticket, "from", pair)
		if err := g.File(ctx, p0SolInbox(g.Host, ticket)); err != nil {
			logErr(ctx, slog.LevelWarn, "COS P0 to Sol inbox", err, "ticket", ticket)
		}
	}
	return out, nil
}

// routeLaunch picks a launch's route: its own pair when that pair's advisor and worker have headroom, else sol for a P0
// that has waited p0Wait on Opus while Sol has headroom. held means no route is open; why names the cooling model.
// Routine work never waits for Opus; important work (the leader pair) never downgrades before p0Wait.
func routeLaunch(pol ModelPolicy, pair, ticket string, limits LimitsState, now, heldSince time.Time) (route, why string, held bool) {
	ok, _, advisorWhy := limits.Headroom(pol.Pairs[pair].Advisor.Model, now)
	switch {
	case ok:
		route = pair
	case pair != pol.Leader:
		return "", "routine work waits for Sol headroom: " + advisorWhy, true
	case isP0(pol, ticket) && !heldSince.IsZero() && now.Sub(heldSince) > p0Wait && hasRoom(limits, pol.Pairs[solPair].Advisor.Model, now):
		route, why = solPair, fmt.Sprintf("P0 %s moved to Sol after 30 min without Opus headroom", ticket)
	default:
		return "", "important work waits for Opus headroom: " + advisorWhy, true
	}
	if ok, _, workerWhy := limits.Headroom(pol.Pairs[route].Worker.Model, now); !ok {
		return "", "no worker spawn on a cooling model: " + workerWhy, true
	}
	return route, why, false
}

// isP0 reports whether ticket is the first priority key. An empty priority list names no P0, and no ticket is never a P0.
func isP0(pol ModelPolicy, ticket string) bool {
	return ticket != "" && len(pol.Priority) > 0 && strings.EqualFold(pol.Priority[0], ticket)
}

// ticketOf is the ticket key in a thread's or launch's title and branch, upper-cased; "" when there is none.
func ticketOf(title, branch string) string {
	return strings.ToUpper(ticketPattern.FindString(title + " " + branch))
}

// hasRoom reports whether model has headroom now; a model no pair names has none.
func hasRoom(limits LimitsState, model string, now time.Time) bool {
	ok, _, _ := limits.Headroom(model, now)
	return model != "" && ok
}

// sendMarkerKey names a COS_SENDQ marker for one ticket or thread; hashing keeps the value out of the key, as sendKeys does.
func sendMarkerKey(prefix, s string) string {
	sum := sha256.Sum256([]byte(s))
	return prefix + hex.EncodeToString(sum[:])[:24]
}

// putOnce stores v at key unless the key exists; it reports whether it stored v.
func putOnce(ctx context.Context, kv jetstream.KeyValue, key string, v any) (bool, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	_, err = kv.Create(ctx, key, data)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return false, nil
	}
	return err == nil, err
}

// p0SolInbox is the Inbox item that files a P0 moved to Sol.
func p0SolInbox(host, ticket string) InboxInput {
	return InboxInput{Kind: "fyi", Source: "cos", Key: "p0-sol:" + ticket, Host: host, Ticket: ticket,
		Title: oneLine(fmt.Sprintf("P0 %s moved to Sol: no Opus headroom for 30 min", ticket), inboxTitleMax)}
}

// p0FailedInbox files a P0 whose move to Sol failed after its marker was stored.
func p0FailedInbox(host, thread, ticket string, cause error) InboxInput {
	return InboxInput{Kind: "fyi", Source: "cos", Key: "p0-sol-failed:" + ticket, Host: host, Thread: thread, Ticket: ticket,
		Title: oneLine(fmt.Sprintf("P0 %s could not move to Sol: %s", ticket, publicText(cause)), inboxTitleMax)}
}

// p0Moved reports whether the thread's move marker is stored. A read error counts as moved, so nothing moves on a bad read.
func p0Moved(ctx context.Context, kv jetstream.KeyValue, thread string) bool {
	_, err := kv.Get(ctx, sendMarkerKey("p0moved.", thread))
	return err == nil || !errors.Is(err, jetstream.ErrKeyNotFound)
}

// handoff refuses a cooling target, and a cooling current model when the handoff compacts first. The brief goes through Send.
func (g *Gate) handoff(ctx context.Context, args []string) (any, error) {
	sel, err := localObjectJSON(args[1])
	if err != nil {
		return nil, err
	}
	target, _ := sel["model"].(string)
	if err := g.cooling(ctx, target, "no handoff to a cooling model"); err != nil {
		return nil, err
	}
	c, err := g.T3(ctx)
	if err != nil {
		return nil, err
	}
	p, err := c.Projection(ctx, args[0])
	if err != nil {
		return nil, err
	}
	if ContextOf(p).UsedTokens > handoffCompactTokens {
		if err := g.cooling(ctx, p.Thread.ModelSelection.Model, "no compaction on a cooling model"); err != nil {
			return nil, err
		}
	}
	out, err := g.Local(ctx, "handoff", args[:2])
	if err != nil {
		return out, err
	}
	result, _ := out.(map[string]any)
	if len(args) < 3 || args[2] == "" || result["action"] != "handed-off" {
		return out, nil
	}
	if _, err := g.Send(ctx, args[0], args[2], "queue"); err != nil {
		return nil, err
	}
	return out, nil
}

// checkThread refuses when the thread's current model has no headroom.
func (g *Gate) checkThread(ctx context.Context, thread, what string) error {
	c, err := g.T3(ctx)
	if err != nil {
		return err
	}
	p, err := c.Projection(ctx, thread)
	if err != nil {
		return err
	}
	return g.cooling(ctx, p.Thread.ModelSelection.Model, what)
}

// cooling returns the held refusal when model has no headroom now.
func (g *Gate) cooling(ctx context.Context, model, what string) error {
	limits, err := g.Limits(ctx)
	if err != nil {
		return err
	}
	if ok, _, why := limits.Headroom(model, g.Now()); !ok {
		return OperationError(sendHeld + what + ": " + why)
	}
	return nil
}

// heldErr reports a gate refusal; the bus flattens errors to text, so it matches the prefix.
func heldErr(err error) bool { return err != nil && strings.Contains(err.Error(), sendHeld) }

// limitHold reports a thread whose last run stopped on a usage limit while CLIProxyAPI's read is not newer than that failure.
func limitHold(t Thread, s LimitsState) bool {
	failed, err := time.Parse(time.RFC3339Nano, cmp.Or(t.LatestRunDone, t.UpdatedAt))
	return t.LastErrorClass == "usage_limit" && err == nil && !s.At.After(failed.Add(sendLimitSlack))
}

// loadSendHistory returns the automatic sends to a thread inside the cap window.
func loadSendHistory(ctx context.Context, kv jetstream.KeyValue, key string, now time.Time) ([]time.Time, error) {
	var at []time.Time
	if _, err := kvJSON(ctx, kv, key, &at); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(at, func(t time.Time) bool { return now.Sub(t) >= sendCapWindow }), nil
}

// mergeSend folds one message into the thread's entry: texts are kept once, the oldest go first when too long,
// and a user answer makes the entry user kind. An entry whose delivery failed starts over.
func mergeSend(e *queuedSend, in queuedSend, now time.Time) {
	if e.Queued.IsZero() || e.Error != "" {
		*e = queuedSend{Host: in.Host, Thread: in.Thread, Queued: now, NotBefore: now}
	}
	if in.NotBefore.After(e.NotBefore) {
		e.NotBefore = in.NotBefore
	}
	e.Title, e.Model, e.Reason = in.Title, in.Model, in.Reason
	if in.Kind == "user" || e.Kind == "" {
		e.Kind = in.Kind
	}
	for _, t := range in.Texts {
		if !slices.Contains(e.Texts, t) {
			e.Texts = append(e.Texts, t)
		}
	}
	for len(e.Texts) > 1 && textLength(e.Texts) > sendTextMax {
		e.Texts = e.Texts[1:]
	}
}

// textLength counts the characters that a merged entry holds.
func textLength(texts []string) int {
	n := 0
	for _, t := range texts {
		n += len([]rune(t))
	}
	return n
}

// casSend applies fn to the entry at key under compare-and-swap: fn returns false to delete an existing entry or to do nothing.
// A concurrent writer makes it re-read and try again.
func casSend(ctx context.Context, kv jetstream.KeyValue, key string, fn func(e *queuedSend, found bool) bool) error {
	var err error
	for range 5 {
		var e queuedSend
		var rev uint64
		entry, gerr := kv.Get(ctx, key)
		found := gerr == nil
		switch {
		case errors.Is(gerr, jetstream.ErrKeyNotFound):
		case gerr != nil:
			return gerr
		default:
			rev = entry.Revision()
			if err := json.Unmarshal(entry.Value(), &e); err != nil {
				return err
			}
		}
		keep := fn(&e, found)
		if !keep && !found {
			return nil
		}
		if !keep {
			err = kv.Delete(ctx, key, jetstream.LastRevision(rev))
		} else {
			data, merr := json.Marshal(e)
			if merr != nil {
				return merr
			}
			if found {
				_, err = kv.Update(ctx, key, data, rev)
			} else {
				_, err = kv.Create(ctx, key, data)
			}
		}
		if err == nil {
			return nil
		}
	}
	return err
}

// editSend changes an existing entry under compare-and-swap; a missing entry is left alone.
func editSend(ctx context.Context, kv jetstream.KeyValue, key string, fn func(e *queuedSend)) error {
	return casSend(ctx, kv, key, func(e *queuedSend, found bool) bool {
		if found {
			fn(e)
		}
		return found
	})
}

// releaseClaim forgets an in-flight delivery so a later tick schedules the entry again.
func releaseClaim(e *queuedSend) {
	e.MessageID, e.Claimed, e.ReadyAt = "", time.Time{}, time.Time{}
}

// settleSend removes the texts that reached the thread; a text merged meanwhile stays for the next tick.
func settleSend(ctx context.Context, kv jetstream.KeyValue, key string, sent []string) error {
	return casSend(ctx, kv, key, func(e *queuedSend, found bool) bool {
		if !found {
			return false
		}
		e.Texts = slices.DeleteFunc(e.Texts, func(t string) bool { return slices.Contains(sent, t) })
		releaseClaim(e)
		return len(e.Texts) > 0
	})
}

// sendDeliverer delivers held messages on the cluster leader: one per model per tick, each claimed before it is sent.
type sendDeliverer struct {
	Queue  func(context.Context) (jetstream.KeyValue, error)
	Limits func(context.Context) (LimitsState, error)
	Call   func(ctx context.Context, host, command string, args []string, out any) error
	File   func(context.Context, InboxInput) error
	Now    func() time.Time
}

// heldSend is one queue entry with its key.
type heldSend struct {
	key string
	e   queuedSend
}

// runSendQueue delivers held messages while this node leads; the queue lives in KV, so a new leader resumes it.
func runSendQueue(ctx context.Context, cfg Config, bus *Bus) {
	g := newSendGate(cfg, bus)
	d := &sendDeliverer{Queue: g.Queue, Limits: g.Limits, Call: bus.Call, File: g.File, Now: time.Now}
	bus.Lead(ctx, func(ctx context.Context) {
		for {
			if err := d.tick(ctx); err != nil && ctx.Err() == nil {
				logErr(ctx, slog.LevelWarn, "COS send queue", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(sendTick):
			}
		}
	})
}

// tick schedules due entries with a stagger per model, then delivers at most one entry per model.
func (d *sendDeliverer) tick(ctx context.Context) error {
	limits, err := d.Limits(ctx)
	if err != nil {
		return err
	}
	kv, err := d.Queue(ctx)
	if err != nil {
		return err
	}
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var held []heldSend
	for _, key := range keys {
		var e queuedSend
		if !strings.HasPrefix(key, "q.") {
			continue
		}
		if found, err := kvJSON(ctx, kv, key, &e); err != nil {
			return err
		} else if found {
			held = append(held, heldSend{key, e})
		}
	}
	slices.SortFunc(held, func(a, b heldSend) int { return a.e.Queued.Compare(b.e.Queued) })
	now := d.Now()
	d.moveStalledP0(ctx, kv, held, limits, now)
	// last is the latest ReadyAt already scheduled per model, so new slots stack behind it.
	last := map[string]time.Time{}
	for _, h := range held {
		if h.e.ReadyAt.After(now) && h.e.ReadyAt.Sub(now) <= sendSkewMax && h.e.ReadyAt.After(last[h.e.Model]) {
			last[h.e.Model] = h.e.ReadyAt
		}
	}
	sent := map[string]bool{}
	for _, h := range held {
		e := h.e
		if e.Error != "" || (e.MessageID != "" && now.Sub(e.Claimed) < sendClaimStale) {
			continue
		}
		if ok, _, _ := limits.Headroom(e.Model, now); !ok {
			if !e.ReadyAt.IsZero() {
				if err := editSend(ctx, kv, h.key, func(cur *queuedSend) { cur.ReadyAt = time.Time{} }); err != nil {
					return err
				}
			}
			continue
		}
		if now.Before(e.NotBefore) {
			continue
		}
		if e.MessageID == "" && (e.ReadyAt.IsZero() || e.ReadyAt.Sub(now) > sendSkewMax) {
			at := now
			if l := last[e.Model]; l.After(now) {
				at = l.Add(sendStagger)
			}
			at = at.Add(time.Duration(rand.Int64N(int64(sendJitter) + 1)))
			last[e.Model] = at
			if err := editSend(ctx, kv, h.key, func(cur *queuedSend) { cur.ReadyAt = at }); err != nil {
				return err
			}
			continue
		}
		if now.Before(e.ReadyAt) || sent[e.Model] {
			continue
		}
		sent[e.Model] = true
		if err := d.deliver(ctx, kv, h.key, e, now); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logErr(ctx, slog.LevelWarn, "COS send queue deliver", err, "thread", e.Thread)
		}
	}
	return nil
}

// moveStalledP0 moves each P0 whose held work has waited p0Wait on Opus while Sol has headroom (moveP0). Other entries stay.
func (d *sendDeliverer) moveStalledP0(ctx context.Context, kv jetstream.KeyValue, held []heldSend, limits LimitsState, now time.Time) {
	pol := CurrentBehavior().Models
	for _, h := range held {
		e := h.e
		if pair, _ := pol.PairOf(e.Model); pair != pol.Leader || now.Sub(e.Queued) <= p0Wait || hasRoom(limits, e.Model, now) || !hasRoom(limits, pol.Pairs[solPair].Advisor.Model, now) {
			continue
		}
		// A ticket only in the branch is not seen here: the entry's title is the cheap check before the thread call.
		if !isP0(pol, ticketOf(e.Title, "")) || p0Moved(ctx, kv, e.Thread) {
			continue
		}
		if err := d.moveP0(ctx, kv, h.key, e, now); err != nil {
			logErr(ctx, slog.LevelWarn, "COS P0 to Sol", err, "thread", e.Thread)
		}
	}
}

// moveP0 starts a Sol successor on the P0's worktree with the held texts, settles the old thread and drops its entry.
// The marker is stored before the launch, so a retry or a new leader never starts a second successor.
func (d *sendDeliverer) moveP0(ctx context.Context, kv jetstream.KeyValue, key string, e queuedSend, now time.Time) error {
	pol := CurrentBehavior().Models
	var info map[string]any
	if err := d.Call(ctx, e.Host, "thread", []string{e.Thread}, &info); err != nil {
		return err
	}
	title, _ := info["title"].(string)
	branch, _ := info["branch"].(string)
	ticket := ticketOf(title, branch)
	lineage, _ := info["lineage"].(map[string]any)
	if parent, _ := lineage["parentThreadId"].(string); parent != "" || !isP0(pol, ticket) {
		return nil
	}
	if stored, err := putOnce(ctx, kv, sendMarkerKey("p0moved.", e.Thread), now); err != nil || !stored {
		return err
	}
	// The marker is stored, so a failure from here is final: it is filed once and never retried.
	fail := func(cause error) error {
		if ferr := d.File(ctx, p0FailedInbox(e.Host, e.Thread, ticket, cause)); ferr != nil {
			logErr(ctx, slog.LevelWarn, "COS P0 to Sol inbox", ferr, "ticket", ticket)
		}
		return cause
	}
	brief := func(lastReply string) (string, error) {
		return p0MovedLine + "\n\nLast reply of the Opus thread:\n" + lastReply + "\n\nHeld messages:\n" + strings.Join(e.Texts, "\n\n"), nil
	}
	id, _, ok, err := launchSuccessor(ctx, d.Call, e.Host, FleetThread{ThreadID: e.Thread, Title: title, Branch: branch}, solPair, brief)
	switch {
	case err != nil:
		return fail(err)
	case !ok:
		return fail(errors.New("the thread has no worktree"))
	case id == "":
		return fail(errors.New("the Sol successor launched without a thread id"))
	}
	// The successor is running, so the entry goes even when the settle fails: a later delivery would reach the old thread.
	settleErr := d.Call(ctx, e.Host, "settle", []string{e.Thread}, nil)
	queued := e.Queued
	if err := casSend(ctx, kv, key, func(cur *queuedSend, found bool) bool { return found && !cur.Queued.Equal(queued) }); err != nil {
		return fail(err)
	}
	if settleErr != nil {
		return fail(fmt.Errorf("successor %s started; settle the old thread by hand: %w", id, settleErr))
	}
	slog.Info("COS moved a P0 to Sol", "ticket", ticket, "thread", e.Thread, "successor", id)
	return d.File(ctx, p0SolInbox(e.Host, ticket))
}

// deliver claims one entry and sends its texts with the entry's message ID; the gate decides whether they land, wait or drop.
func (d *sendDeliverer) deliver(ctx context.Context, kv jetstream.KeyValue, key string, e queuedSend, now time.Time) error {
	id := e.MessageID
	if id == "" {
		id = uuid()
		if err := editSend(ctx, kv, key, func(cur *queuedSend) { cur.MessageID, cur.Claimed = id, now }); err != nil {
			return err
		}
	}
	text := strings.Join(e.Texts, "\n\n")
	var out GateResult
	err := d.Call(ctx, e.Host, "send", []string{e.Thread, text, "deliver:" + id}, &out)
	switch {
	case ctx.Err() != nil:
		return ctx.Err() // the claim stays; a later leader re-sends the same message ID
	case err != nil:
		return d.fail(ctx, kv, key, e, text, err)
	case out.Action == "busy" || out.Action == "queued":
		return editSend(ctx, kv, key, releaseClaim)
	}
	return settleSend(ctx, kv, key, e.Texts)
}

// fail keeps the error as the entry's state and files one Inbox item; nothing is resent.
func (d *sendDeliverer) fail(ctx context.Context, kv jetstream.KeyValue, key string, e queuedSend, text string, cause error) error {
	msg := publicText(cause)
	if err := editSend(ctx, kv, key, func(cur *queuedSend) { releaseClaim(cur); cur.Error = msg }); err != nil {
		return err
	}
	return d.File(ctx, InboxInput{Kind: "fyi", Source: "cos", Key: "sendfail:" + e.Thread, Host: e.Host, Thread: e.Thread,
		Title:   oneLine("COS could not deliver a held message to "+e.Title, inboxTitleMax),
		Context: firstRunes(msg+"\n\n"+text, sendInboxMax)})
}
