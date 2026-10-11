package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

const (
	chatBucket    = "COS_CHAT"
	contextBucket = "COS_CONTEXT"
	chatShown     = 200
	chatMaxText   = 20000
)

// ChatEntry is one chat bubble. COS replies are rewritten in place while they stream.
type ChatEntry struct {
	ID        string    `json:"id"`
	At        time.Time `json:"at"`
	Role      string    `json:"role"` // user, cos or system
	Text      string    `json:"text"`
	Tools     []string  `json:"tools,omitempty"`
	Streaming bool      `json:"streaming,omitempty"`
}

// chatID sorts by creation time; KV keys allow only [-/_=.a-zA-Z0-9].
func chatID(at time.Time) string {
	return fmt.Sprintf("%016x-%s", at.UnixNano(), uuid()[:8])
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
	kv      jetstream.KeyValue
	mu      sync.Mutex
	entries map[string]ChatEntry
	ready   bool
	subs    map[*chatSub]struct{}
}

// chatSub collects changed keys between wake-ups so a slow SSE client never misses one.
type chatSub struct {
	wake chan struct{}
	keys map[string]bool
}

func NewChat() *Chat {
	return &Chat{entries: map[string]ChatEntry{}, subs: map[*chatSub]struct{}{}}
}

// Run creates the chat and context buckets (retrying until JetStream has quorum) and follows every change until ctx ends.
func (c *Chat) Run(ctx context.Context, b *Bus) {
	for {
		_, err := openKV(ctx, b, contextBucket)
		if err == nil {
			_, err = openKV(ctx, b, memoryBucket)
		}
		var kv jetstream.KeyValue
		if err == nil {
			kv, err = openKV(ctx, b, chatBucket)
		}
		if err == nil {
			var w jetstream.KeyWatcher
			if w, err = kv.WatchAll(ctx); err == nil {
				c.follow(ctx, kv, w)
				if ctx.Err() != nil {
					return
				}
				// The bucket was deleted (re-bootstrap) or the watcher ended: rebuild from a fresh replay.
				c.mu.Lock()
				c.kv, c.ready = nil, false
				clear(c.entries)
				c.mu.Unlock()
				err = errors.New("chat watcher stopped")
			}
		}
		slog.Warn("chat store not ready", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (c *Chat) follow(ctx context.Context, kv jetstream.KeyValue, w jetstream.KeyWatcher) {
	defer w.Stop()
	c.mu.Lock()
	c.kv = kv
	c.mu.Unlock()
	// The watcher survives a deleted or recreated bucket without closing; check its identity.
	created := bucketCreated(ctx, kv)
	check := time.NewTicker(5 * time.Second)
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
	if u.Operation() != jetstream.KeyValuePut {
		delete(c.entries, u.Key())
	} else {
		var e ChatEntry
		if err := json.Unmarshal(u.Value(), &e); err != nil {
			slog.Error("invalid chat entry", "key", u.Key())
			return
		}
		e.ID = u.Key()
		c.entries[e.ID] = e
	}
	if c.ready {
		c.notify(u.Key())
	}
}

// notify records a changed key ("" repaints everything) and wakes every subscriber.
func (c *Chat) notify(key string) {
	for s := range c.subs {
		s.keys[key] = true
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

func (c *Chat) Subscribe() (*chatSub, func()) {
	s := &chatSub{wake: make(chan struct{}, 1), keys: map[string]bool{}}
	c.mu.Lock()
	c.subs[s] = struct{}{}
	c.mu.Unlock()
	return s, func() { c.mu.Lock(); delete(c.subs, s); c.mu.Unlock() }
}

// Changed returns and clears the keys changed since the last call.
func (c *Chat) Changed(s *chatSub) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := sortedKeysBool(s.keys)
	clear(s.keys)
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

// Entries returns the newest chatShown entries, oldest first.
func (c *Chat) Entries() []ChatEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ChatEntry, 0, len(c.entries))
	for _, e := range c.entries {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b ChatEntry) int { return strings.Compare(a.ID, b.ID) })
	if len(out) > chatShown {
		out = out[len(out)-chatShown:]
	}
	return out
}

func (c *Chat) Entry(id string) (ChatEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[id]
	return e, ok
}

func (c *Chat) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ready
}

// Put writes an entry to the replicated bucket; ID and At are filled for new entries.
func (c *Chat) Put(ctx context.Context, e ChatEntry) (ChatEntry, error) {
	c.mu.Lock()
	kv := c.kv
	c.mu.Unlock()
	if kv == nil {
		return e, errors.New("chat store is starting; try again in a few seconds")
	}
	return putChatEntry(ctx, kv, e)
}

// putChatEntry fills ID and time and writes e to the chat bucket.
func putChatEntry(ctx context.Context, kv jetstream.KeyValue, e ChatEntry) (ChatEntry, error) {
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
	_, err = kv.Put(ctx, e.ID, data)
	return e, err
}

// PutChat posts an entry from any component (e.g. a leader worker) without the dashboard's Chat;
// every node's Chat.Run watcher shows it.
func PutChat(ctx context.Context, b *Bus, e ChatEntry) (ChatEntry, error) {
	kv, err := openKV(ctx, b, chatBucket)
	if err != nil {
		return e, err
	}
	return putChatEntry(ctx, kv, e)
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
