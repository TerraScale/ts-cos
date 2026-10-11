package cos

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

const (
	chatBucket      = "COS_CHAT"
	contextBucket   = "COS_CONTEXT"
	chatShown       = 200
	chatMaxText     = 20000
	chatMainTab     = "cos"
	chatMaxTabs     = 12
	chatTabPrefix   = "tab."
	chatTagPrefix   = "tag."
	chatTabNameMax  = 40
	chatTabFocusMax = 2000
)

// mainTab is the default chat tab, the one the main COS agent drives.
var mainTab = ChatTab{ID: chatMainTab, Name: "COS"}

// ChatEntry is one chat bubble. COS replies are rewritten in place while they stream.
type ChatEntry struct {
	ID        string    `json:"id"`
	At        time.Time `json:"at"`
	Role      string    `json:"role"` // user, cos or system
	Text      string    `json:"text"`
	Tools     []string  `json:"tools,omitempty"`
	Streaming bool      `json:"streaming,omitempty"`
	Ask       *ChatAsk  `json:"ask,omitempty"`
	// Tab is the chat tab the entry belongs to; entries written before tabs existed are the main tab.
	Tab string `json:"tab,omitempty"`
	// Action marks the outcome of a Threads panel button, shown under the panel as well as in the chat.
	Action bool `json:"action,omitempty"`
	rev    uint64
	owner  string // ULID of the user whose key this entry is stored under; not serialized
}

// ChatTab is one chat tab; the leader runs a COS thread per open tab.
type ChatTab struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Focus   string    `json:"focus,omitempty"`
	Created time.Time `json:"created"`
	Closed  bool      `json:"closed,omitempty"`
	rev     uint64
}

// ChatTag marks a thread as belonging to a chat tab; COS is true for the tab's own COS thread.
type ChatTag struct {
	ThreadID string    `json:"threadId"`
	Host     string    `json:"host"`
	Tab      string    `json:"tab"`
	COS      bool      `json:"cos,omitempty"`
	At       time.Time `json:"at"`
}

// ChatAsk is a pending T3 question from the COS thread; any node can answer it over NATS.
type ChatAsk struct {
	Host      string            `json:"host"`
	ThreadID  string            `json:"threadId"`
	RequestID string            `json:"requestId"`
	Questions []ChatQuestion    `json:"questions"`
	Answers   map[string]string `json:"answers"`
	Done      bool              `json:"done,omitempty"`
}
type ChatQuestion struct {
	ID      string   `json:"id"`
	Header  string   `json:"header"`
	Text    string   `json:"text"`
	Options []string `json:"options"`
}

// chatID sorts by creation time; KV keys allow only [-/_=.a-zA-Z0-9].
func chatID(at time.Time) string {
	return fmt.Sprintf("%016x-%s", at.UnixNano(), uuid()[:8])
}

// splitChatKey reads a stored "u.<ULID>.<chatID>" key; ok is false for legacy keys, which wait for migrateToUser.
func splitChatKey(key string) (owner, id string, ok bool) {
	rest, ok := strings.CutPrefix(key, "u.")
	if !ok {
		return "", "", false
	}
	owner, id, ok = strings.Cut(rest, ".")
	return owner, id, ok && ulidPattern.MatchString(owner) && id != ""
}

// openKV binds a replicated bucket. Only the JetStream meta leader creates it:
// concurrent creates from several nodes can wedge the stream assignment.
func openKV(ctx context.Context, b *Bus, bucket string) (jetstream.KeyValue, error) {
	return openKVConfig(ctx, b, jetstream.KeyValueConfig{Bucket: bucket})
}

// openKVConfig binds a bucket described by cfg, creating it with the cluster's replicas.
func openKVConfig(ctx context.Context, b *Bus, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	js, err := jetstream.New(b.conn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	bucket := cfg.Bucket
	kv, err := js.KeyValue(ctx, bucket)
	if b.server == nil && err != nil {
		return nil, fmt.Errorf("COS %s store unavailable; is cos serve running? (%w)", strings.ToLower(strings.TrimPrefix(bucket, "COS_")), err)
	}
	if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return kv, err
	}
	if !b.server.JetStreamIsLeader() {
		return nil, fmt.Errorf("waiting for the JetStream leader to create %s", bucket)
	}
	cfg.Replicas, cfg.Storage = 1, jetstream.FileStorage
	if b.server.JetStreamIsClustered() {
		cfg.Replicas = 3
	}
	// shortcut: buckets grow without pruning; trim old chat keys when they reach megabytes.
	return js.CreateKeyValue(ctx, cfg)
}

// Chat mirrors the replicated chat bucket in memory so every node renders the same live view.
type Chat struct {
	kv jetstream.KeyValue
	// savings is the savings ledger bucket, read by the Tokens page.
	savings jetstream.KeyValue
	bus     *Bus
	mu      sync.Mutex
	entries map[string]ChatEntry
	tabs    map[string]ChatTab
	// tags maps each tag's KV key to the tag.
	tags  map[string]ChatTag
	ready bool
	subs  map[*chatSub]struct{}
	// recheck is how often follow verifies the bucket's identity; tests shorten it.
	recheck time.Duration
}

// chatSub collects changed keys between wake-ups so a slow SSE client never misses one.
// A per-user sub sees its owner's entries by bare chatID; an all-users sub (system consumers) sees full keys.
type chatSub struct {
	wake  chan struct{}
	keys  map[string]bool
	owner string // user ULID; "" when the subscriber had no user, so it only wakes on repaints
	all   bool
}

func NewChat() *Chat {
	return &Chat{entries: map[string]ChatEntry{}, tabs: map[string]ChatTab{}, tags: map[string]ChatTag{}, subs: map[*chatSub]struct{}{}, recheck: 5 * time.Second}
}

// Run creates the chat and context buckets (retrying until JetStream has quorum) and follows every change until ctx ends.
func (c *Chat) Run(ctx context.Context, b *Bus) {
	c.mu.Lock()
	c.bus = b
	c.mu.Unlock()
	for {
		_, err := openKV(ctx, b, contextBucket)
		if err == nil {
			_, err = openKV(ctx, b, memoryBucket)
		}
		var savings jetstream.KeyValue
		if err == nil {
			savings, err = openKV(ctx, b, savingsBucket)
		}
		var kv jetstream.KeyValue
		if err == nil {
			kv, err = openKV(ctx, b, chatBucket)
		}
		if err == nil {
			var w jetstream.KeyWatcher
			if w, err = kv.WatchAll(ctx); err == nil {
				c.mu.Lock()
				c.savings = savings
				c.mu.Unlock()
				c.follow(ctx, kv, w)
				if ctx.Err() != nil {
					return
				}
				// The bucket was deleted (re-bootstrap) or the watcher ended: rebuild from a fresh replay.
				c.mu.Lock()
				c.kv, c.savings, c.ready = nil, nil, false
				clear(c.entries)
				clear(c.tabs)
				clear(c.tags)
				c.notify("")
				c.mu.Unlock()
				err = errors.New("chat watcher stopped")
			}
		}
		logErr(ctx, slog.LevelWarn, "chat store not ready", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// SavingsKV is the savings ledger bucket; nil until Run has bound it.
func (c *Chat) SavingsKV() jetstream.KeyValue {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.savings
}

func (c *Chat) follow(ctx context.Context, kv jetstream.KeyValue, w jetstream.KeyWatcher) {
	defer w.Stop()
	c.mu.Lock()
	c.kv = kv
	c.mu.Unlock()
	// The watcher survives a deleted or recreated bucket without closing; check its identity.
	created := bucketCreated(ctx, kv)
	check := time.NewTicker(c.recheck)
	defer check.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-check.C:
			if now := bucketCreated(ctx, kv); now != created && (now.IsZero() || !created.IsZero()) {
				return
			}
		case u, ok := <-w.Updates():
			if !ok {
				return
			}
			c.apply(u)
		}
	}
}

// bucketCreated identifies the bucket's backing stream; zero when it is gone or unknown.
func bucketCreated(ctx context.Context, kv jetstream.KeyValue) time.Time {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	status, err := kv.Status(ctx)
	if err != nil {
		return time.Time{}
	}
	if s, ok := status.(*jetstream.KeyValueBucketStatus); ok && s.StreamInfo() != nil {
		return s.StreamInfo().Created
	}
	return time.Time{}
}

func (c *Chat) apply(u jetstream.KeyValueEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if u == nil {
		c.ready = true
		c.notify("")
		return
	}
	owner, rest, ok := splitChatKey(u.Key())
	if !ok { // a legacy key; migrateToUser moves it under its owner
		return
	}
	// Tabs, tags and entries are all per user: "u.<ULID>.tab.<id>", "u.<ULID>.tag.<hash>", "u.<ULID>.<chatID>".
	switch {
	case strings.HasPrefix(rest, chatTabPrefix):
		id := strings.TrimPrefix(rest, chatTabPrefix)
		if u.Operation() != jetstream.KeyValuePut {
			delete(c.tabs, u.Key())
			break
		}
		var t ChatTab
		if err := json.Unmarshal(u.Value(), &t); err != nil {
			slog.Error("invalid chat tab", "key", u.Key())
			return
		}
		t.ID, t.rev = id, u.Revision()
		c.tabs[u.Key()] = t
	case strings.HasPrefix(rest, chatTagPrefix):
		if u.Operation() != jetstream.KeyValuePut {
			delete(c.tags, u.Key())
			break
		}
		var t ChatTag
		if err := json.Unmarshal(u.Value(), &t); err != nil {
			slog.Error("invalid chat tag", "key", u.Key())
			return
		}
		c.tags[u.Key()] = t
	case u.Operation() != jetstream.KeyValuePut:
		delete(c.entries, u.Key())
	default:
		var e ChatEntry
		if err := json.Unmarshal(u.Value(), &e); err != nil {
			slog.Error("invalid chat entry", "key", u.Key())
			return
		}
		e.ID, e.owner, e.rev = rest, owner, u.Revision()
		if e.Tab == "" {
			e.Tab = chatMainTab
		}
		c.entries[u.Key()] = e
	}
	if c.ready {
		c.notify(u.Key())
	}
}

// notify records a changed key ("" repaints everything) and wakes every subscriber.
func (c *Chat) notify(key string) {
	owner, _, _ := splitChatKey(key)
	for s := range c.subs {
		if key != "" && !s.all && s.owner != owner {
			continue
		}
		s.keys[key] = true
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// Subscribe wakes on changes to the user in ctx; with no user it only wakes on repaints.
func (c *Chat) Subscribe(ctx context.Context) (*chatSub, func()) {
	s := &chatSub{}
	if u, err := UserFrom(ctx); err == nil {
		s.owner = u.ID
	}
	return c.subscribe(s)
}

// SubscribeAll wakes on every user's changes; system consumers only, since it spans users.
func (c *Chat) SubscribeAll() (*chatSub, func()) {
	return c.subscribe(&chatSub{all: true})
}

func (c *Chat) subscribe(s *chatSub) (*chatSub, func()) {
	s.wake, s.keys = make(chan struct{}, 1), map[string]bool{}
	c.mu.Lock()
	c.subs[s] = struct{}{}
	c.mu.Unlock()
	return s, func() { c.mu.Lock(); delete(c.subs, s); c.mu.Unlock() }
}

// Changed returns and clears the keys changed since the last call; a per-user sub gets bare chatIDs.
func (c *Chat) Changed(s *chatSub) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := sortedKeysBool(s.keys)
	clear(s.keys)
	if s.all {
		return keys
	}
	for i, key := range keys {
		if _, id, ok := splitChatKey(key); ok {
			keys[i] = id
		}
	}
	return keys
}

func sortedKeysBool(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Entries returns the newest chatShown entries of tab for the user in ctx, oldest first; no user means none.
func (c *Chat) Entries(ctx context.Context, tab string) []ChatEntry {
	u, err := UserFrom(ctx)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ChatEntry, 0, len(c.entries))
	for key, e := range c.entries {
		if e.Tab == tab && strings.HasPrefix(key, u.Key("")) {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b ChatEntry) int { return strings.Compare(a.ID, b.ID) })
	if len(out) > chatShown {
		out = out[len(out)-chatShown:]
	}
	return out
}

// Tags returns the user's thread tags of tab, sorted by thread ID; no user means none.
func (c *Chat) Tags(ctx context.Context, tab string) []ChatTag {
	out := []ChatTag{}
	u, err := UserFrom(ctx)
	if err != nil {
		return out
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, t := range c.tags {
		if t.Tab == tab && strings.HasPrefix(key, u.Key("")) {
			out = append(out, t)
		}
	}
	slices.SortFunc(out, func(a, b ChatTag) int { return strings.Compare(a.ThreadID, b.ThreadID) })
	return out
}

// Tabs returns every stored tab of the user in ctx, closed ones included, sorted by ID; no user means none.
func (c *Chat) Tabs(ctx context.Context) []ChatTab {
	u, err := UserFrom(ctx)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ChatTab, 0, len(c.tabs))
	for key, t := range c.tabs {
		if strings.HasPrefix(key, u.Key(chatTabPrefix)) {
			out = append(out, t)
		}
	}
	slices.SortFunc(out, func(a, b ChatTab) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// Tab returns the user's tab id; another user's tab is not found.
func (c *Chat) Tab(ctx context.Context, id string) (ChatTab, bool) {
	key, err := UserKey(ctx, chatTabPrefix+id)
	if err != nil {
		return ChatTab{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tabs[key]
	return t, ok
}

// CreateTab adds a tab for the user in ctx; at most chatMaxTabs open tabs exist per user at once.
func (c *Chat) CreateTab(ctx context.Context, name, focus string) (ChatTab, error) {
	t := ChatTab{Name: strings.TrimSpace(name), Focus: strings.TrimSpace(focus)}
	if err := validTab(t); err != nil {
		return t, err
	}
	u, err := UserFrom(ctx)
	if err != nil {
		return t, err
	}
	c.mu.Lock()
	kv, ready := c.kv, c.ready
	open := 0
	for key, o := range c.tabs {
		if !o.Closed && strings.HasPrefix(key, u.Key(chatTabPrefix)) {
			open++
		}
	}
	c.mu.Unlock()
	if kv == nil || !ready {
		return t, errors.New("chat store is starting; try again in a few seconds")
	}
	// shortcut: two concurrent creates can exceed chatMaxTabs by one; the cap is a soft guard, upgrade to a CAS count if it matters.
	if open >= chatMaxTabs {
		return t, fmt.Errorf("at most %d chat tabs; close one first", chatMaxTabs)
	}
	t.Created = time.Now().UTC()
	t.ID = chatID(t.Created)
	data, err := json.Marshal(t)
	if err != nil {
		return t, err
	}
	if t.rev, err = kv.Create(ctx, u.Key(chatTabPrefix+t.ID), data); err != nil {
		return t, err
	}
	return t, nil
}

// UpdateTab writes the user's t only if it has not changed since it was read, so a rename never resurrects a closed tab.
func (c *Chat) UpdateTab(ctx context.Context, t ChatTab) error {
	if t.ID == "" || t.ID == chatMainTab {
		return fmt.Errorf("tab %q cannot be updated", t.ID)
	}
	// rev 0 would make kv.Update create the key, so a tab that was never read (or was deleted) must not be written.
	if t.rev == 0 {
		return errors.New("tab was never read or was deleted; reload it before updating")
	}
	if err := validTab(t); err != nil {
		return err
	}
	key, err := UserKey(ctx, chatTabPrefix+t.ID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	kv := c.kv
	c.mu.Unlock()
	if kv == nil {
		return errors.New("chat store is starting; try again in a few seconds")
	}
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	rev, err := kv.Update(ctx, key, data, t.rev)
	if err != nil {
		return err
	}
	t.rev = rev
	c.mu.Lock()
	c.tabs[key] = t
	c.mu.Unlock()
	return nil
}

// DeleteTab removes the user's tab: its cached entries and thread tags, then its key.
func (c *Chat) DeleteTab(ctx context.Context, id string) error {
	if id == "" || id == chatMainTab {
		return fmt.Errorf("tab %q cannot be deleted", id)
	}
	u, err := UserFrom(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	kv := c.kv
	keys := []string{}
	for key, e := range c.entries {
		if e.Tab == id && strings.HasPrefix(key, u.Key("")) {
			keys = append(keys, key)
		}
	}
	for key, t := range c.tags {
		if t.Tab == id && strings.HasPrefix(key, u.Key("")) {
			keys = append(keys, key)
		}
	}
	c.mu.Unlock()
	if kv == nil {
		return errors.New("chat store is starting; try again in a few seconds")
	}
	for _, key := range keys {
		if err := kv.Delete(ctx, key); err != nil {
			return err
		}
	}
	return kv.Delete(ctx, u.Key(chatTabPrefix+id))
}

// validTab checks a tab's name and focus; the name is one line, the focus may span lines.
func validTab(t ChatTab) error {
	if n := utf8.RuneCountInString(t.Name); n == 0 || n > chatTabNameMax || strings.IndexFunc(t.Name, notGraphic) >= 0 {
		return fmt.Errorf("tab name must be one line of 1 to %d characters", chatTabNameMax)
	}
	if utf8.RuneCountInString(t.Focus) > chatTabFocusMax {
		return fmt.Errorf("tab focus must be at most %d characters", chatTabFocusMax)
	}
	return nil
}

// Entry returns the user's entry with chatID id; another user's entry is not found.
func (c *Chat) Entry(ctx context.Context, id string) (ChatEntry, bool) {
	key, err := UserKey(ctx, id)
	if err != nil {
		return ChatEntry{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	return copyEntry(e), ok
}

// entryByKey reads an entry by its full stored key, for system consumers that span users.
func (c *Chat) entryByKey(key string) (ChatEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	return copyEntry(e), ok
}

// entriesAll is every user's entries keyed by full key, for system consumers that span users.
func (c *Chat) entriesAll() map[string]ChatEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.entries)
}

// copyEntry clones what callers edit, so they never mutate the shared cache.
func copyEntry(e ChatEntry) ChatEntry {
	if e.Ask == nil {
		return e
	}
	ask := *e.Ask
	ask.Answers = maps.Clone(ask.Answers)
	ask.Questions = slices.Clone(ask.Questions)
	for i := range ask.Questions {
		ask.Questions[i].Options = slices.Clone(ask.Questions[i].Options)
	}
	e.Ask = &ask
	return e
}

func (c *Chat) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ready
}

// Put writes the user's entry to the replicated bucket; ID and At are filled for new entries.
func (c *Chat) Put(ctx context.Context, e ChatEntry) (ChatEntry, error) {
	if _, err := UserFrom(ctx); err != nil {
		return e, err
	}
	c.mu.Lock()
	kv := c.kv
	c.mu.Unlock()
	if kv == nil {
		return e, errors.New("chat store is starting; try again in a few seconds")
	}
	return putChatEntry(ctx, kv, e)
}

// putChatEntry fills ID and time and writes e under the user in ctx; no user writes nothing.
func putChatEntry(ctx context.Context, kv jetstream.KeyValue, e ChatEntry) (ChatEntry, error) {
	scoped, err := UserKV(ctx, kv)
	if err != nil {
		return e, err
	}
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	if e.ID == "" {
		e.ID = chatID(e.At)
	}
	data, err := json.Marshal(e)
	if err != nil {
		return e, err
	}
	_, err = scoped.Put(ctx, e.ID, data)
	return e, err
}

// PutChat posts an entry for the user in ctx from any component (e.g. a leader worker) without the dashboard's Chat;
// every node's Chat.Run watcher shows it.
func PutChat(ctx context.Context, b *Bus, e ChatEntry) (ChatEntry, error) {
	kv, err := openKV(ctx, b, chatBucket)
	if err != nil {
		return e, err
	}
	return putChatEntry(ctx, kv, e)
}

// PutTag records that a thread belongs to a chat tab; every node's watcher picks it up.
func (c *Chat) PutTag(ctx context.Context, t ChatTag) error {
	c.mu.Lock()
	kv := c.kv
	c.mu.Unlock()
	if kv == nil {
		return errors.New("chat store is starting; try again in a few seconds")
	}
	return putTag(ctx, kv, t)
}

// TagThread tags a thread from any component (e.g. the CLI's launch) without the dashboard's Chat.
func TagThread(ctx context.Context, b *Bus, t ChatTag) error {
	kv, err := openKV(ctx, b, chatBucket)
	if err != nil {
		return err
	}
	return putTag(ctx, kv, t)
}

// putTag writes the tag under the user in ctx; no user writes nothing.
func putTag(ctx context.Context, kv jetstream.KeyValue, t ChatTag) error {
	if t.ThreadID == "" {
		return errors.New("tag needs a thread ID")
	}
	scoped, err := UserKV(ctx, kv)
	if err != nil {
		return err
	}
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = scoped.Put(ctx, tagKey(t.ThreadID), data)
	return err
}

// tagKey hashes the thread ID: thread IDs contain characters KV keys do not allow.
func tagKey(threadID string) string {
	sum := sha256.Sum256([]byte(threadID))
	return chatTagPrefix + hex.EncodeToString(sum[:])[:16]
}

var errBusUnavailable = errors.New("COS bus unavailable")

// Respond answers a pending T3 question on the host that owns the COS thread, over NATS.
func (c *Chat) Respond(ctx context.Context, host, thread, request, answers string) error {
	c.mu.Lock()
	b := c.bus
	c.mu.Unlock()
	if b == nil {
		return errBusUnavailable
	}
	return b.Call(ctx, host, "respond", []string{thread, request, answers}, nil)
}

// Replace writes the user's e only if nobody changed it since it was read, so concurrent answers are not lost.
func (c *Chat) Replace(ctx context.Context, e ChatEntry) error {
	u, err := UserFrom(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	kv := c.kv
	c.mu.Unlock()
	if kv == nil {
		return errors.New("chat store is starting; try again in a few seconds")
	}
	scoped, err := UserKV(ctx, kv)
	if err != nil {
		return err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	rev, err := scoped.Update(ctx, e.ID, data, e.rev)
	if err != nil {
		return err
	}
	// Cache the write now, so an answer to the next question does not fence on a stale revision.
	c.mu.Lock()
	e.rev, e.owner = rev, u.ID
	c.entries[u.Key(e.ID)] = copyEntry(e)
	c.mu.Unlock()
	return nil
}

var markdown = goldmark.New(goldmark.WithExtensions(extension.GFM),
	goldmark.WithParserOptions(parser.WithASTTransformers(util.Prioritized(linkAllowlist{}, 100))))

// linkAllowlist keeps only http(s), mailto, anchor and relative destinations; goldmark's own
// filter is a case-sensitive denylist that lets "JavaScript:" through.
type linkAllowlist struct{}

func (linkAllowlist) Transform(doc *ast.Document, reader text.Reader, _ parser.Context) {
	source := reader.Source()
	unsafe := []*ast.AutoLink{}
	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		switch n := n.(type) {
		case *ast.Link:
			n.Destination = safeDestination(n.Destination)
		case *ast.Image:
			n.Destination = safeDestination(n.Destination)
		case *ast.AutoLink:
			if entering && safeDestination(n.URL(source)) == nil {
				unsafe = append(unsafe, n)
			}
		}
		return ast.WalkContinue, nil
	})
	for _, n := range unsafe {
		n.Parent().ReplaceChild(n.Parent(), n, ast.NewString(n.Label(source)))
	}
}

func safeDestination(dst []byte) []byte {
	d := bytes.ToLower(bytes.TrimSpace(dst))
	for _, prefix := range []string{"https://", "http://", "mailto:", "#", "/"} {
		if bytes.HasPrefix(d, []byte(prefix)) {
			return dst
		}
	}
	if !bytes.Contains(d, []byte(":")) {
		return dst
	}
	return nil
}

// renderMarkdown escapes raw HTML and keeps only safe link schemes.
func renderMarkdown(text string) string {
	var b bytes.Buffer
	if err := markdown.Convert([]byte(text), &b); err != nil {
		return "<pre>" + strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(text) + "</pre>"
	}
	return b.String()
}
