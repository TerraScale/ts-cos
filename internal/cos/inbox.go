package cos

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
)

// The user Inbox holds everything COS needs from the user. Items live in the
// replicated COS_INBOX bucket, where compare-and-set gives every item exactly one
// answer cluster-wide; every node mirrors the bucket into its SQLite state.
const (
	inboxBucket       = "COS_INBOX"
	inboxReplicas     = 3
	inboxTitleMax     = 200
	inboxTextMax      = 4000
	inboxAnswerMax    = 3500 // fits the event data limit with its JSON envelope
	inboxHistoryMax   = 50
	inboxRepublish    = time.Hour // re-emits inside the event dedup window are dropped
	inboxMigrateEvery = 10 * time.Minute
)

var inboxKinds = []string{"question", "approval", "decision", "blocker", "fyi"}

// InboxInput is what `cos inbox add`, the rules engine and worker asks file.
type InboxInput struct {
	Kind       string   `json:"kind"`
	Title      string   `json:"title"`
	Context    string   `json:"context"`
	Blocks     string   `json:"blocks"`
	Options    []string `json:"options"`
	Ticket     string   `json:"ticket"`
	Links      []string `json:"links"`
	Host       string   `json:"host"`
	Thread     string   `json:"thread"`
	Source     string   `json:"source"`
	Key        string   `json:"key"`
	Urgent     bool     `json:"urgent"`
	Supersedes []string `json:"supersedes"`
}

type InboxItem struct {
	ID           string      `json:"id"`
	Owner        string      `json:"-"` // the ULID in the item's KV key, set on every read; never stored in the payload
	Kind         string      `json:"kind"`
	Title        string      `json:"title"`
	Context      string      `json:"context,omitempty"`
	Blocks       string      `json:"blocks,omitempty"`
	Options      []string    `json:"options,omitempty"` // recommended first
	Ticket       string      `json:"ticket,omitempty"`
	Links        []string    `json:"links,omitempty"`
	Host         string      `json:"host,omitempty"`
	Thread       string      `json:"thread,omitempty"`
	Source       string      `json:"source"`
	Key          string      `json:"key"`
	Urgent       bool        `json:"urgent,omitempty"`
	Status       string      `json:"status"` // open, answered, dismissed, superseded
	Answer       *AnswerData `json:"answer,omitempty"`
	Note         string      `json:"note,omitempty"`
	ClosedOn     string      `json:"closedOn,omitempty"`
	Created      time.Time   `json:"created"`
	Closed       time.Time   `json:"closed,omitzero"`
	Forwarded    bool        `json:"forwarded,omitempty"`
	ForwardError string      `json:"forwardError,omitempty"`
}

func oneLine(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > limit {
		s = string([]rune(s)[:limit-1]) + "…"
	}
	return s
}

// inboxID is stable per dedup key, so the same question is always one item.
func inboxID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:8])
}

func validInboxID(id string) bool {
	_, err := hex.DecodeString(id)
	return len(id) == 16 && err == nil
}

func (in InboxInput) item(now time.Time) (InboxItem, error) {
	item := InboxItem{Kind: in.Kind, Title: strings.TrimSpace(in.Title), Context: strings.TrimSpace(in.Context),
		Blocks: oneLine(in.Blocks, inboxTitleMax), Ticket: strings.ToUpper(strings.TrimSpace(in.Ticket)), Host: in.Host,
		Thread: in.Thread, Source: in.Source, Key: in.Key, Urgent: in.Urgent, Status: "open", Created: now.UTC()}
	if item.Kind == "" {
		item.Kind = "question"
	}
	if item.Source == "" {
		item.Source = "cos"
	}
	switch {
	case !slices.Contains(inboxKinds, item.Kind):
		return item, errors.New("inbox kind must be question, approval, decision, blocker, or fyi")
	case !slices.Contains([]string{"cos", "rules", "worker", "deploy", "budget", "e2e"}, item.Source):
		return item, errors.New("inbox source must be cos, rules, worker, deploy, budget, or e2e")
	case item.Kind == "fyi" && item.Urgent:
		return item, errors.New("inbox fyi items are never urgent")
	case item.Title == "" || strings.ContainsAny(item.Title, "\r\n") || utf8.RuneCountInString(item.Title) > inboxTitleMax:
		return item, fmt.Errorf("inbox title must be one line of at most %d characters", inboxTitleMax)
	case utf8.RuneCountInString(item.Context) > inboxTextMax:
		return item, fmt.Errorf("inbox context must be at most %d characters", inboxTextMax)
	case item.Ticket != "" && ticketPattern.FindString(item.Ticket) != item.Ticket:
		return item, errors.New("inbox ticket must look like TS-815")
	case item.Host != "" && !validHost(item.Host), item.Thread != "" && item.Host == "":
		return item, errors.New("inbox thread requires a valid host")
	case len(in.Key) > 512:
		return item, errors.New("inbox key must be at most 512 bytes")
	}
	for _, o := range in.Options {
		if o = oneLine(o, inboxTitleMax); o != "" && !slices.Contains(item.Options, o) {
			item.Options = append(item.Options, o)
		}
	}
	switch item.Kind {
	case "question":
		if len(item.Options) == 1 || len(item.Options) > 4 {
			return item, errors.New("inbox question takes 2-4 options (recommended first) or none")
		}
	case "approval":
		if len(item.Options) != 0 {
			return item, errors.New("inbox approvals always offer Approve and Reject")
		}
		item.Options = []string{"Approve", "Reject"}
	default:
		if len(item.Options) != 0 {
			return item, fmt.Errorf("inbox %s items take no options", item.Kind)
		}
	}
	for _, link := range in.Links {
		u, err := url.Parse(link)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return item, errors.New("inbox links must be http(s) URLs")
		}
		item.Links = append(item.Links, link)
	}
	for _, id := range in.Supersedes {
		if !validInboxID(id) {
			return item, errors.New("inbox supersedes takes item IDs")
		}
	}
	if item.Key == "" {
		scope := "cos"
		if item.Thread != "" {
			scope = item.Host + "/" + item.Thread
		} else if item.Ticket != "" {
			scope = item.Ticket
		}
		item.Key = scope + "|" + strings.ToLower(oneLine(item.Title, inboxTitleMax))
	}
	item.ID = inboxID(item.Key)
	return item, nil
}

// itemEvent is the event for the item's current state; its ID is stable per state.
func itemEvent(item InboxItem) Event {
	e := Event{Source: SourceInbox, Kind: KindInboxItemAdded, Time: item.Created, Severity: SeverityWarn,
		Summary: oneLine(item.Title, eventSummaryMax), Host: item.Host, Ticket: item.Ticket, Thread: item.Thread, Item: item.ID, User: item.Owner,
		ID: fmt.Sprintf("inbox:added:%s:%d", item.ID, item.Created.UnixNano())}
	if item.Urgent || item.Kind == "approval" {
		e.Severity = SeverityUrgent
	}
	if item.Kind == "fyi" {
		e.Severity = SeverityInfo // informational: never wakes COS
	}
	switch item.Status {
	case "open":
	case "answered":
		e.Source, e.Kind, e.Severity, e.Time, e.User = SourceUser, KindUserAnswer, SeverityUrgent, item.Closed, item.Owner
		e.ID = fmt.Sprintf("user:answer:%s:%d", item.ID, item.Closed.UnixNano())
		e.Data, _ = json.Marshal(item.Answer)
	default:
		e.Kind, e.Severity, e.Time, e.User = KindInboxClosed, SeverityInfo, item.Closed, item.Owner
		e.ID = fmt.Sprintf("inbox:closed:%s:%d", item.ID, item.Closed.UnixNano())
		e.Data, _ = json.Marshal(map[string]string{"status": item.Status, "note": oneLine(item.Note, inboxTitleMax)})
	}
	return e
}

type Inbox struct {
	kv   jetstream.KeyValue
	js   jetstream.JetStream
	bus  *Bus
	host string
}

// OpenInbox binds the bucket; serving nodes also create it and the event streams.
func OpenInbox(ctx context.Context, bus *Bus, host string, create bool) (*Inbox, error) {
	js, err := bus.JetStream()
	if err != nil {
		return nil, err
	}
	in := &Inbox{js: js, bus: bus, host: host}
	if !create {
		if in.kv, err = js.KeyValue(ctx, inboxBucket); err != nil {
			return nil, fmt.Errorf("COS Inbox unavailable: %w", err)
		}
		return in, nil
	}
	if err := EnsureEventStreams(ctx, js); err != nil {
		return nil, err
	}
	in.kv, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: inboxBucket,
		Description: "COS user Inbox", History: 1, Storage: jetstream.FileStorage, Replicas: inboxReplicas})
	if err != nil {
		return nil, fmt.Errorf("COS Inbox bucket: %w", err)
	}
	return in, nil
}

// update applies fn to the caller's item under compare-and-set; fn reports whether to write.
func (in *Inbox) update(ctx context.Context, id string, fn func(item *InboxItem, exists bool) (bool, error)) (InboxItem, bool, error) {
	u, err := UserFrom(ctx)
	if err != nil {
		return InboxItem{}, false, err
	}
	kv, err := UserKV(ctx, in.kv)
	if err != nil {
		return InboxItem{}, false, err
	}
	for range 10 {
		item := InboxItem{Owner: u.ID}
		var revision uint64
		entry, err := kv.Get(ctx, id)
		if err == nil {
			revision = entry.Revision()
			if err := json.Unmarshal(entry.Value(), &item); err != nil {
				return item, false, fmt.Errorf("inbox item %s is corrupt: %w", id, err)
			}
		} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return item, false, err
		}
		write, err := fn(&item, revision != 0)
		item.Owner = u.ID // fn may replace the whole item
		if err != nil || !write {
			return item, false, err
		}
		data, err := json.Marshal(item)
		if err != nil {
			return item, false, err
		}
		if revision == 0 {
			_, err = kv.Create(ctx, id, data)
		} else {
			_, err = kv.Update(ctx, id, data, revision)
		}
		if err == nil {
			return item, true, nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return item, false, err
		}
	}
	return InboxItem{}, false, errors.New("inbox item changed concurrently; retry")
}

func (in *Inbox) publish(ctx context.Context, item InboxItem) {
	// e2e probes (source e2e, or a probe worker's "[e2e] " question) must not wake COS or push to the user's phone.
	if item.Source == "e2e" || strings.HasPrefix(item.Title, "[e2e] ") {
		return
	}
	if _, err := PublishEvent(ctx, in.js, itemEvent(item)); err != nil {
		// The reconciler re-emits recent items, so a lost publish is retried.
		slog.Warn("publish inbox event", "item", item.ID, "error", err)
	}
}

// Add files an item. The same key while open updates that item in place; an
// answered or dismissed key is not asked again; a superseded key reopens.
func (in *Inbox) Add(ctx context.Context, input InboxInput) (InboxItem, error) {
	next, err := input.item(time.Now())
	if err != nil {
		return next, err
	}
	created := false
	item, written, err := in.update(ctx, next.ID, func(cur *InboxItem, exists bool) (bool, error) {
		if !exists || cur.Status == "superseded" {
			*cur, created = next, true
			return true, nil
		}
		if cur.Status != "open" {
			return false, nil
		}
		merged := *cur
		merged.Kind, merged.Title, merged.Context, merged.Blocks, merged.Options = next.Kind, next.Title, next.Context, next.Blocks, next.Options
		merged.Ticket, merged.Links, merged.Urgent = next.Ticket, next.Links, next.Urgent
		if reflect.DeepEqual(merged, *cur) {
			return false, nil
		}
		*cur, created = merged, false
		return true, nil
	})
	if err != nil {
		return item, err
	}
	if written && created {
		in.publish(ctx, item)
	}
	for _, old := range input.Supersedes {
		if old == item.ID {
			continue
		}
		if _, err := in.close(ctx, old, "superseded", nil, "Superseded by: "+item.Title); err != nil && !errors.Is(err, errInboxClosed) {
			return item, err
		}
	}
	return item, nil
}

var (
	errInboxClosed   = OperationError("inbox item is already closed")
	errInboxNotFound = OperationError("no inbox item") // also what another user's item ID returns
)

func (in *Inbox) close(ctx context.Context, id, status string, answer *AnswerData, note string) (InboxItem, error) {
	if !validInboxID(id) {
		return InboxItem{}, OperationError("invalid inbox item ID")
	}
	item, _, err := in.update(ctx, id, func(cur *InboxItem, exists bool) (bool, error) {
		if !exists {
			return false, errInboxNotFound
		}
		if cur.Status != "open" {
			return false, fmt.Errorf("%w: %s on %s", errInboxClosed, cur.Status, cur.ClosedOn)
		}
		if answer != nil && cur.Kind == "fyi" {
			return false, OperationError("FYI items can only be dismissed")
		}
		if answer != nil && answer.Option != "" && !slices.Contains(cur.Options, answer.Option) {
			return false, OperationError("answer option is not one of the item's options")
		}
		cur.Status, cur.Answer, cur.Note, cur.ClosedOn, cur.Closed = status, answer, note, in.host, time.Now().UTC()
		return true, nil
	})
	if err != nil {
		return item, err
	}
	in.publish(ctx, item)
	return item, nil
}

// Answer records the user's answer once cluster-wide, emits user.answer and
// forwards it to the worker that asked.
func (in *Inbox) Answer(ctx context.Context, id string, answer AnswerData) (InboxItem, error) {
	answer.Option, answer.Text = strings.TrimSpace(answer.Option), strings.TrimSpace(answer.Text)
	if answer.Option == "" && answer.Text == "" {
		return InboxItem{}, OperationError("choose an option or write an answer")
	}
	if len(answer.Option)+len(answer.Text) > inboxAnswerMax {
		return InboxItem{}, OperationError(fmt.Sprintf("answer must be at most %d bytes", inboxAnswerMax))
	}
	item, err := in.close(ctx, id, "answered", &answer, "")
	if err == nil && item.Thread != "" {
		item = in.forward(ctx, item)
	}
	return item, err
}

func (in *Inbox) Dismiss(ctx context.Context, id string) (InboxItem, error) {
	return in.close(ctx, id, "dismissed", nil, "Dismissed by the user")
}

// purge removes the caller's item KV entry for good; the mirror drops it on the delete the purge leaves.
func (in *Inbox) purge(ctx context.Context, id string) error {
	kv, err := UserKV(ctx, in.kv)
	if err != nil {
		return err
	}
	return kv.Purge(ctx, id)
}

func answerText(a *AnswerData) string {
	if a == nil {
		return ""
	}
	if a.Option != "" && a.Text != "" {
		return a.Option + "\n" + a.Text
	}
	return a.Option + a.Text
}

// forward sends an answer to the asking worker. The claim makes concurrent
// reconcilers send it once; a failed send releases the claim for a retry.
// shortcut: a crash between claim and send loses the forward; record a send ID if that shows up.
func (in *Inbox) forward(ctx context.Context, item InboxItem) InboxItem {
	ctx = WithUser(ctx, User{ID: item.Owner})
	claimed, ok, err := in.update(ctx, item.ID, func(cur *InboxItem, _ bool) (bool, error) {
		if cur.Status != "answered" || cur.Forwarded || cur.Thread == "" {
			return false, nil
		}
		cur.Forwarded, cur.ForwardError = true, ""
		return true, nil
	})
	if err != nil || !ok {
		return item
	}
	text := "USER ANSWER (COS Inbox " + claimed.ID + "): " + claimed.Title + "\n" + answerText(claimed.Answer)
	sendErr := in.bus.Call(ctx, claimed.Host, "send", []string{claimed.Thread, text}, nil)
	if sendErr == nil {
		return claimed
	}
	released, _, err := in.update(context.WithoutCancel(ctx), item.ID, func(cur *InboxItem, _ bool) (bool, error) {
		cur.Forwarded, cur.ForwardError = false, oneLine(sendErr.Error(), inboxTitleMax)
		return true, nil
	})
	if err != nil {
		slog.Error("release inbox forward", "item", item.ID, "error", err)
	}
	return released
}

// askInput maps a worker NEEDS-USER block to an item; the asks poll and the
// worker.needs_user events must build identical items so they dedupe.
func askInput(a Ask) InboxInput {
	input := InboxInput{Source: "worker", Host: a.Host, Thread: a.ThreadID, Title: oneLine(a.Question, inboxTitleMax),
		Context: a.Why, Blocks: a.Title, Ticket: strings.ToUpper(ticketPattern.FindString(a.Title))}
	if input.Title != a.Question {
		input.Context = strings.TrimSpace(a.Question + "\n\n" + a.Why)
	}
	if len(a.Options) >= 2 && len(a.Options) <= 4 {
		input.Options = a.Options
	} else if len(a.Options) > 0 {
		input.Context = strings.TrimSpace(input.Context + "\n\nOPTIONS: " + strings.Join(a.Options, " | "))
	}
	input.Context = string([]rune(input.Context)[:min(utf8.RuneCountInString(input.Context), inboxTextMax)])
	return input
}

// nodeUser is the first admin, who owns everything COS produces from node T3s and COS events. It errors
// until the bootstrap has run, so nothing is ever filed unowned.
func nodeUser(ctx context.Context, bus *Bus) (User, error) {
	id, ok, err := firstAdmin(ctx, bus.conn)
	if err != nil {
		return User{}, err
	}
	if !ok {
		return User{}, errors.New("COS first admin is not bootstrapped yet")
	}
	return User{ID: id, Role: roleAdmin}, nil
}

// addNode files an item from node T3 data or a COS event, as the first admin.
func (in *Inbox) addNode(ctx context.Context, input InboxInput) (InboxItem, error) {
	admin, err := nodeUser(ctx, in.bus)
	if err != nil {
		return InboxItem{}, err
	}
	return in.Add(WithUser(ctx, admin), input)
}

// fileNeedsUser files a worker.needs_user event as soon as T3 ingestion sees it.
func (in *Inbox) fileNeedsUser(ctx context.Context, e Event) error {
	var a Ask
	if err := json.Unmarshal(e.Data, &a); err != nil || e.Kind != KindNeedsUser || e.Host == "" || e.Thread == "" || a.Question == "" {
		return fmt.Errorf("malformed worker.needs_user event %s", e.ID)
	}
	a.Host, a.ThreadID = e.Host, e.Thread
	_, err := in.addNode(ctx, askInput(a))
	return err
}

// consumeNeedsUser files worker questions from the event stream; the durable
// consumer resumes where the previous leader stopped.
// A fixed start time, unlike DeliverNew, does not depend on when the first pull happens; later leaders resume the durable.
func (in *Inbox) consumeNeedsUser(ctx context.Context) error {
	// Waiting for the first admin costs no redeliveries; the loop in RunInbox retries.
	if _, err := nodeUser(ctx, in.bus); err != nil {
		return err
	}
	consumer, err := in.js.Consumer(ctx, EventsStream, "cos-inbox-needs-user")
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		start := time.Now()
		consumer, err = in.js.CreateConsumer(ctx, EventsStream, jetstream.ConsumerConfig{Durable: "cos-inbox-needs-user",
			FilterSubject: Event{Source: SourceT3, Kind: KindNeedsUser}.Subject(), AckPolicy: jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &start, MaxDeliver: 5})
	}
	if err != nil {
		return err
	}
	consuming, err := consumer.Consume(func(msg jetstream.Msg) {
		var e Event
		ferr := json.Unmarshal(msg.Data(), &e)
		if ferr == nil {
			ferr = in.fileNeedsUser(ctx, e)
		}
		if ferr != nil {
			slog.Warn("file worker question", "error", ferr)
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	consuming.Stop()
	return nil
}

// SyncAsks turns worker NEEDS-USER blocks into items and supersedes items whose
// idle or closed worker no longer asks. Items dedupe by key.
func (in *Inbox) SyncAsks(ctx context.Context, live Live, open []InboxItem) error {
	if live.At == "" || live.Errors["asks"] != "" || live.Errors["fleet"] != "" {
		return nil // no complete snapshot yet
	}
	admin, err := nodeUser(ctx, in.bus)
	if err != nil {
		return err
	}
	asked := map[string]bool{}
	failed := map[string]bool{}
	for _, a := range live.Asks {
		if a.Error != "" {
			failed[a.Host] = failed[a.Host] || a.ThreadID == ""
			failed[a.Host+"/"+a.ThreadID] = true
			continue
		}
		if a.Kind != "marker" || a.Stale {
			continue
		}
		item, err := in.Add(WithUser(ctx, admin), askInput(a))
		if err != nil {
			return err
		}
		asked[item.ID] = true
	}
	threads := map[string]string{}
	seen := map[string]bool{}
	for _, h := range live.Fleet {
		seen[h.Host] = h.Error == "" && !h.Stale
		for _, t := range h.Threads {
			threads[h.Host+"/"+t.ThreadID] = t.Status
		}
	}
	for _, item := range open {
		thread := item.Host + "/" + item.Thread
		if item.Owner != admin.ID || item.Source != "worker" || item.Status != "open" || asked[item.ID] || !seen[item.Host] || failed[item.Host] || failed[thread] || active(threads[thread]) {
			continue
		}
		_, err := in.close(WithUser(ctx, admin), item.ID, "superseded", nil, "The worker no longer asks this (answered in T3, replaced by a newer question, or the thread closed).")
		if err != nil && !errors.Is(err, errInboxClosed) {
			return err
		}
	}
	return nil
}

// reconcile re-emits recent events, retries forwards and syncs worker asks;
// the leader runs it, and every step is idempotent across a leader handover.
func (in *Inbox) reconcile(ctx context.Context, store *Store) error {
	open, history, err := store.allInbox(ctx)
	if err != nil {
		return err
	}
	for _, item := range append(open, history...) {
		if time.Since(item.Created) < inboxRepublish || time.Since(item.Closed) < inboxRepublish {
			in.publish(ctx, item)
		}
		if item.Status == "answered" && item.Thread != "" && !item.Forwarded {
			in.forward(ctx, item)
		}
	}
	live, err := store.Live(ctx)
	if err != nil {
		return err
	}
	return in.SyncAsks(ctx, live, open)
}

// Mirror copies the bucket into SQLite until ctx ends. The watcher first replays
// every item, so a restarted or new node catches up without a manual step.
func (in *Inbox) Mirror(ctx context.Context, store *Store) error {
	w, err := in.kv.WatchAll(ctx)
	if err != nil {
		return err
	}
	defer w.Stop()
	seen := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-w.Updates():
			if !ok {
				return errors.New("inbox watch closed")
			}
			if entry == nil {
				if seen != nil {
					if err := store.PruneInbox(ctx, seen); err != nil {
						return err
					}
					seen = nil
				}
				continue
			}
			owner, id, ok := parseInboxKey(entry.Key())
			if !ok {
				continue // a pre-TS-831 key; the leader's migration moves it to the first admin
			}
			if seen != nil {
				seen[entry.Key()] = true
			}
			if entry.Operation() != jetstream.KeyValuePut {
				if err := store.DeleteInboxItem(ctx, owner, id); err != nil {
					return err
				}
				continue
			}
			var item InboxItem
			if err := json.Unmarshal(entry.Value(), &item); err != nil {
				slog.Error("skip corrupt inbox item", "key", entry.Key(), "error", err)
				continue
			}
			item.Owner = owner
			if err := store.SaveInboxItem(ctx, item); err != nil {
				return err
			}
		}
	}
}

// migrateEvery runs migrate until ctx ends: at once, then every 10 minutes, and backs off while it fails.
func migrateEvery(ctx context.Context, what string, migrate func(context.Context) (int, error)) {
	failures := inboxFailures{what: what}
	for ctx.Err() == nil {
		moved, err := migrate(ctx)
		if moved > 0 {
			slog.Info("COS migration moved keys", "what", what, "keys", moved)
		}
		wait := failures.observe(err)
		if wait == 0 {
			wait = inboxMigrateEvery
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// migrate moves items filed before per-user Inboxes under the first admin. COS_INBOX holds only item keys.
func (in *Inbox) migrate(ctx context.Context) (int, error) {
	admin, err := nodeUser(ctx, in.bus)
	if err != nil {
		return 0, err
	}
	return migrateToUser(ctx, in.kv, admin.ID)
}

// inboxFailures turns repeated errors of one background loop into one WARN and,
// only if they persist (KV/stream raft re-elections during restarts are transient),
// one ERROR; it returns the backoff before the next attempt.
type inboxFailures struct {
	what      string
	since     time.Time
	n         int
	escalated bool
}

const inboxPersist = 5 * time.Minute

func (f *inboxFailures) observe(err error) time.Duration {
	if err == nil {
		if f.n > 0 {
			slog.Info(f.what+" recovered", "attempts", f.n)
		}
		*f = inboxFailures{what: f.what}
		return 0
	}
	f.n++
	switch {
	case f.n == 1:
		f.since = time.Now()
		slog.Warn(f.what+" failed; retrying", "error", err)
	case !f.escalated && time.Since(f.since) >= inboxPersist:
		f.escalated = true
		slog.Error(f.what+" still failing", "since", f.since.UTC().Format(time.RFC3339), "attempts", f.n, "error", err)
	}
	return min(2*time.Second<<min(f.n-1, 5), time.Minute)
}

// RunInbox mirrors the bucket on every node and calls ready once it is
// reachable (the cluster needs a JetStream quorum first). The reconciler is
// singleton work, so it runs only on the cluster leader.
func RunInbox(ctx context.Context, bus *Bus, host string, store *Store, ready func(*Inbox)) {
	opening := inboxFailures{what: "COS Inbox open"}
	var in *Inbox
	for in == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		var err error
		in, err = OpenInbox(attempt, bus, host, true)
		stop()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(opening.observe(err)):
			}
		}
	}
	opening.observe(nil)
	slog.Info("COS Inbox ready", "host", host)
	ready(in)
	var lead sync.WaitGroup
	defer lead.Wait()
	lead.Go(func() {
		bus.Lead(ctx, func(ctx context.Context) {
			var loops sync.WaitGroup
			defer loops.Wait()
			loops.Go(func() { migrateEvery(ctx, "inbox migration to the first admin", in.migrate) })
			loops.Go(func() {
				questions := inboxFailures{what: "inbox worker-question consumer"}
				for ctx.Err() == nil {
					var wait time.Duration
					if err := in.consumeNeedsUser(ctx); err != nil && ctx.Err() == nil {
						wait = questions.observe(err)
					}
					select {
					case <-ctx.Done():
					case <-time.After(wait):
					}
				}
			})
			reconciling := inboxFailures{what: "inbox reconcile"}
			for {
				err := in.reconcile(ctx, store)
				if ctx.Err() != nil {
					return
				}
				wait := reconciling.observe(err)
				if wait == 0 {
					wait = time.Minute
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(wait):
				}
			}
		})
	})
	mirroring := inboxFailures{what: "inbox mirror"}
	for {
		started := time.Now()
		err := in.Mirror(ctx, store)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			mirroring.observe(nil)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(mirroring.observe(err)):
		}
	}
}

// parseInboxKey splits a COS_INBOX key "u.<ULID>.<item ID>"; any other key is pre-TS-831 and not mirrored.
func parseInboxKey(key string) (owner, id string, ok bool) {
	owner, id, ok = splitUserKey(key)
	return owner, id, ok && validInboxID(id)
}

func (s *Store) SaveInboxItem(ctx context.Context, item InboxItem) error {
	if !ulidPattern.MatchString(item.Owner) {
		return errors.New("inbox item has no owner")
	}
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return s.change(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO user_inbox VALUES(?,?,?) ON CONFLICT(owner,id) DO UPDATE SET payload=excluded.payload`, item.Owner, item.ID, string(data))
		return err
	})
}
func (s *Store) DeleteInboxItem(ctx context.Context, owner, id string) error {
	return s.change(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM user_inbox WHERE owner=? AND id=?`, owner, id)
		return err
	})
}

// PruneInbox drops local items the bucket no longer has after a catch-up; keep holds "u.<owner>.<id>" keys.
func (s *Store) PruneInbox(ctx context.Context, keep map[string]bool) error {
	open, history, err := s.allInbox(ctx)
	if err != nil {
		return err
	}
	for _, item := range append(open, history...) {
		if !keep[User{ID: item.Owner}.Key(item.ID)] {
			if err := s.DeleteInboxItem(ctx, item.Owner, item.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Inbox returns the signed-in user's open items, urgent then newest first, and their latest closed items.
// A context without a user is an error: no handler or CLI command reads another user's items.
func (s *Store) Inbox(ctx context.Context) (open, history []InboxItem, err error) {
	u, err := UserFrom(ctx)
	if err != nil {
		return []InboxItem{}, []InboxItem{}, err
	}
	return s.inbox(ctx, `SELECT owner, payload FROM user_inbox WHERE owner=?`, u.ID)
}

// inboxFor is the caller's Inbox; a context without a user (a CLI command run without --user) has none.
func inboxFor(ctx context.Context, store *Store) (open, history []InboxItem, err error) {
	if _, err := UserFrom(ctx); err != nil {
		return []InboxItem{}, []InboxItem{}, nil
	}
	return store.Inbox(ctx)
}

// allInbox is every user's items for the node loops that file, answer and republish them. Handlers use Inbox.
func (s *Store) allInbox(ctx context.Context) (open, history []InboxItem, err error) {
	return s.inbox(ctx, `SELECT owner, payload FROM user_inbox`)
}

func (s *Store) inbox(ctx context.Context, query string, args ...any) (open, history []InboxItem, err error) {
	open, history = []InboxItem{}, []InboxItem{}
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return open, history, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		var item InboxItem
		if err := rows.Scan(&item.Owner, &raw); err != nil {
			return open, history, err
		}
		if err := json.Unmarshal([]byte(raw), &item); err != nil {
			return open, history, err
		}
		if item.Status == "open" {
			open = append(open, item)
		} else {
			history = append(history, item)
		}
	}
	slices.SortFunc(open, func(a, b InboxItem) int {
		if urgent(a) != urgent(b) {
			if urgent(a) {
				return -1
			}
			return 1
		}
		return b.Created.Compare(a.Created)
	})
	slices.SortFunc(history, func(a, b InboxItem) int { return b.Closed.Compare(a.Closed) })
	return open, history, rows.Err()
}

func inboxKindLabel(item InboxItem) string {
	label := map[string]string{"question": "Question", "approval": "Approval", "decision": "Decided for you", "blocker": "FYI blocker", "fyi": "FYI"}[item.Kind]
	if urgent(item) {
		label = "Urgent " + strings.ToLower(label)
	}
	return label
}

// inboxTone colors an item's kind tag (cos.css .tag[data-tone]); "" is neutral.
func inboxTone(item InboxItem) string {
	if urgent(item) {
		return "danger"
	}
	return map[string]string{"question": "accent", "decision": "ok", "blocker": "warn"}[item.Kind]
}
func inboxTextLabel(item InboxItem) string {
	switch item.Kind {
	case "decision":
		return "Override with"
	case "blocker":
		return "Reply (optional)"
	case "approval":
		return "Note (optional, sent with Approve or Reject)"
	}
	if len(item.Options) > 0 {
		return "Other"
	}
	return "Answer"
}
func age(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// liveRequests are T3 runtime requests and failures; NEEDS-USER markers are Inbox items.
func liveRequests(asks []Ask) []Ask {
	out := []Ask{}
	for _, a := range asks {
		if a.Kind != "marker" || a.Error != "" {
			out = append(out, a)
		}
	}
	return out
}
func urgent(item InboxItem) bool { return item.Urgent || item.Kind == "approval" }
