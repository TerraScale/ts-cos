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
	inboxBucket     = "COS_INBOX"
	inboxReplicas   = 3
	inboxTitleMax   = 200
	inboxTextMax    = 4000
	inboxAnswerMax  = 3500 // fits the event data limit with its JSON envelope
	inboxHistoryMax = 50
	inboxRepublish  = time.Hour // re-emits inside the event dedup window are dropped
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
	case !slices.Contains([]string{"cos", "rules", "worker", "deploy"}, item.Source):
		return item, errors.New("inbox source must be cos, rules, worker, or deploy")
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
		Summary: oneLine(item.Title, eventSummaryMax), Host: item.Host, Ticket: item.Ticket, Thread: item.Thread, Item: item.ID,
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
		e.Source, e.Kind, e.Severity, e.Time = SourceUser, KindUserAnswer, SeverityUrgent, item.Closed
		e.ID = fmt.Sprintf("user:answer:%s:%d", item.ID, item.Closed.UnixNano())
		e.Data, _ = json.Marshal(item.Answer)
	default:
		e.Kind, e.Severity, e.Time = KindInboxClosed, SeverityInfo, item.Closed
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

// update applies fn under compare-and-set; fn reports whether to write.
func (in *Inbox) update(ctx context.Context, id string, fn func(item *InboxItem, exists bool) (bool, error)) (InboxItem, bool, error) {
	for range 10 {
		var item InboxItem
		var revision uint64
		entry, err := in.kv.Get(ctx, id)
		if err == nil {
			revision = entry.Revision()
			if err := json.Unmarshal(entry.Value(), &item); err != nil {
				return item, false, fmt.Errorf("inbox item %s is corrupt: %w", id, err)
			}
		} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return item, false, err
		}
		write, err := fn(&item, revision != 0)
		if err != nil || !write {
			return item, false, err
		}
		data, err := json.Marshal(item)
		if err != nil {
			return item, false, err
		}
		if revision == 0 {
			_, err = in.kv.Create(ctx, id, data)
		} else {
			_, err = in.kv.Update(ctx, id, data, revision)
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
	if _, err := PublishEvent(ctx, in.js, itemEvent(item)); err != nil {
		// The reconciler re-emits recent items, so a lost publish is retried.
		slog.Error("publish inbox event", "item", item.ID, "error", err)
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

var errInboxClosed = OperationError("inbox item is already closed")

func (in *Inbox) close(ctx context.Context, id, status string, answer *AnswerData, note string) (InboxItem, error) {
	if !validInboxID(id) {
		return InboxItem{}, OperationError("invalid inbox item ID")
	}
	item, _, err := in.update(ctx, id, func(cur *InboxItem, exists bool) (bool, error) {
		if !exists {
			return false, OperationError("no inbox item " + id)
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

// SyncAsks turns worker NEEDS-USER blocks into items and supersedes items whose
// idle or closed worker no longer asks. Items dedupe by key.
func (in *Inbox) SyncAsks(ctx context.Context, live Live, open []InboxItem) error {
	if live.At == "" || live.Errors["asks"] != "" || live.Errors["fleet"] != "" {
		return nil // no complete snapshot yet
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
		item, err := in.Add(ctx, input)
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
		if item.Source != "worker" || item.Status != "open" || asked[item.ID] || !seen[item.Host] || failed[item.Host] || failed[thread] || active(threads[thread]) {
			continue
		}
		_, err := in.close(ctx, item.ID, "superseded", nil, "The worker no longer asks this (answered in T3, replaced by a newer question, or the thread closed).")
		if err != nil && !errors.Is(err, errInboxClosed) {
			return err
		}
	}
	return nil
}

// reconcile re-emits recent events, retries forwards and syncs worker asks;
// the leader runs it, and every step is idempotent across a leader handover.
func (in *Inbox) reconcile(ctx context.Context, store *Store) error {
	open, history, err := store.Inbox(ctx)
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
			if seen != nil {
				seen[entry.Key()] = true
			}
			if entry.Operation() != jetstream.KeyValuePut {
				if err := store.DeleteInboxItem(ctx, entry.Key()); err != nil {
					return err
				}
				continue
			}
			var item InboxItem
			if err := json.Unmarshal(entry.Value(), &item); err != nil {
				slog.Error("skip corrupt inbox item", "key", entry.Key(), "error", err)
				continue
			}
			if err := store.SaveInboxItem(ctx, item); err != nil {
				return err
			}
		}
	}
}

// RunInbox mirrors the bucket on every node and calls ready once it is
// reachable (the cluster needs a JetStream quorum first). The reconciler is
// singleton work, so it runs only on the cluster leader.
func RunInbox(ctx context.Context, bus *Bus, host string, store *Store, ready func(*Inbox)) {
	retry := time.NewTicker(2 * time.Second)
	defer retry.Stop()
	var in *Inbox
	for in == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		var err error
		in, err = OpenInbox(attempt, bus, host, true)
		stop()
		if err != nil {
			slog.Warn("COS Inbox not ready", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-retry.C:
			}
		}
	}
	slog.Info("COS Inbox ready", "host", host)
	ready(in)
	var lead sync.WaitGroup
	defer lead.Wait()
	lead.Go(func() {
		bus.Lead(ctx, func(ctx context.Context) {
			tick := time.NewTicker(time.Minute)
			defer tick.Stop()
			for {
				if err := in.reconcile(ctx, store); err != nil && ctx.Err() == nil {
					slog.Error("reconcile inbox", "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
			}
		})
	})
	for ctx.Err() == nil {
		if err := in.Mirror(ctx, store); err != nil && ctx.Err() == nil {
			slog.Error("mirror inbox", "error", err)
		}
		select {
		case <-ctx.Done():
		case <-retry.C:
		}
	}
}

func (s *Store) SaveInboxItem(ctx context.Context, item InboxItem) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return s.change(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO inbox VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`, item.ID, string(data))
		return err
	})
}
func (s *Store) DeleteInboxItem(ctx context.Context, id string) error {
	return s.change(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM inbox WHERE id=?`, id)
		return err
	})
}

// PruneInbox drops local items the bucket no longer has after a catch-up.
func (s *Store) PruneInbox(ctx context.Context, keep map[string]bool) error {
	open, history, err := s.Inbox(ctx)
	if err != nil {
		return err
	}
	for _, item := range append(open, history...) {
		if !keep[item.ID] {
			if err := s.DeleteInboxItem(ctx, item.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Inbox returns open items, urgent then newest first, and the latest closed items.
func (s *Store) Inbox(ctx context.Context) (open, history []InboxItem, err error) {
	open, history = []InboxItem{}, []InboxItem{}
	rows, err := s.DB.QueryContext(ctx, `SELECT payload FROM inbox`)
	if err != nil {
		return open, history, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		var item InboxItem
		if err := rows.Scan(&raw); err != nil {
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
func ticketURL(v Dashboard, ticket string) string {
	for _, i := range v.Live.Issues {
		if i.Identifier == ticket {
			return i.URL
		}
	}
	return ""
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
