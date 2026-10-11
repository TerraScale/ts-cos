package cos

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/TerraScale/ts-cos/behavior"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Behavior files live in behavior/ on main. The JetStream meta leader publishes them
// to COS_BEHAVIOR when main changes; every node watches the bucket and swaps in a
// bundle only after it validates, so a bad file never replaces a working one.
const (
	behaviorBucket   = "COS_BEHAVIOR"
	behaviorKey      = "current"
	behaviorRepo     = "TerraScale/ts-cos"
	behaviorPoll     = 10 * time.Second
	behaviorReport   = time.Minute
	behaviorMaxBytes = 256 << 10
	behaviorLineMax  = 200
)

// Templates every bundle must carry; each must render its sample data. Extra templates are allowed.
var behaviorTemplates = map[string]any{
	"updated":  SelfChange{Summary: "Header label reads Fleet", SHA: "0123456789abcdef", Ticket: "TS-900", At: time.Unix(0, 0).UTC()},
	"reverted": SelfChange{Summary: "Header label reads Fleet", SHA: "0123456789abcdef", Ticket: "TS-900", At: time.Unix(0, 0).UTC(), Reverted: true},
	"progress": SelfChangeStep{Ticket: "TS-900", Step: "PR open", Detail: "CI running"},
	"digest":   Digest{Since: time.Unix(0, 0).UTC(), Items: []DigestItem{{Kind: "ci.status", Text: "ts-cos#9 failed"}}},
	"changes":  []SelfChange{{Summary: "Header label reads Fleet", SHA: "0123456789abcdef", Ticket: "TS-900", At: time.Unix(0, 0).UTC()}},
	"brief":    AgentBriefData{At: time.Unix(0, 0).UTC(), Reason: "first start", Memory: "- [fact] Ship the chat (#focus)\n", MemoryTokens: 12, MemoryFull: 40, MemoryOmitted: 1, Recent: []string{"user: hi"}, Live: "0 running", Now: "User: hi"},
	"selfbrief": SelfBrief{Ticket: "TS-900", Branch: "self/ts-900", Summary: "Header label reads Fleet", Request: "rename the header to Fleet",
		CheckFile: "checks/TS-900.json", Check: `{"summary": "Header reads Fleet", "path": "/", "contains": ["Fleet"]}`},
	"compact_focus": nil,
	"fresh_brief":   freshBrief{LastReply: "Pushed PR #90"},
	"advise_split":  adviseData{Advisor: "claude-opus-5-5", Worker: "claude-haiku-5-5", Target: `{"providerInstanceId":"claudeAgent","model":"claude-haiku-5-5"}`},
	"handoff_brief": handoffBrief{FromPair: "sol", FromModel: "gpt-6.1-sol", Reason: "run failed: boom", Wake: "COS wake", LastReply: "Pushed PR #90"},
}

// Templates that become chat status lines: one short line each.
var behaviorLines = []string{"updated", "reverted", "progress"}

var behaviorKind = regexp.MustCompile(`^[a-z]+\.[a-z_]+$`)

// SelfChange is one entry of the self-change changelog.
type SelfChange struct {
	Summary, SHA, Ticket, PR string
	At                       time.Time
	Reverted                 bool
}

// SelfBrief is the worker brief of a self-change.
type SelfBrief struct{ Ticket, Branch, Summary, Request, CheckFile, Check string }

// SelfChangeStep is one progress line of a running self-change.
type SelfChangeStep struct{ Ticket, Step, Detail string }

type Digest struct {
	Since time.Time
	Items []DigestItem
}
type DigestItem struct{ Kind, Text string }

// WakeBudget bounds how often COS is woken: listed event kinds wake it at once,
// everything else is batched into one digest per DigestSeconds, at most MaxWakesPerHour.
type WakeBudget struct {
	MaxWakesPerHour int      `json:"maxWakesPerHour"`
	DigestSeconds   int      `json:"digestSeconds"`
	Immediate       []string `json:"immediate"`
}

type Behavior struct {
	Version  string `json:"version"` // main commit the files came from, or "embedded"
	Hash     string `json:"hash"`
	Brief    string `json:"-"`
	Rules    string `json:"-"`
	Wake     WakeBudget
	Models   ModelPolicy
	files    map[string]string
	tmpl     *template.Template
	loadedAt time.Time
}

// AgentBrief is the brief and rules text COS starts every thread with.
func (b *Behavior) AgentBrief() string {
	return b.Brief + "\n" + b.Models.BriefSection() + "\n" + b.Rules
}

// behaviorRenderMax bounds one rendered template; a bundle cannot exhaust a node's memory.
const behaviorRenderMax = 64 << 10

var errRenderTooLarge = fmt.Errorf("rendered template exceeds %d bytes", behaviorRenderMax)

type cappedWriter struct{ bytes.Buffer }

func (w *cappedWriter) Write(p []byte) (int, error) {
	if w.Len()+len(p) > behaviorRenderMax {
		return 0, errRenderTooLarge
	}
	return w.Buffer.Write(p)
}

// Render executes a behavior template; status-line templates come back as one trimmed line.
func (b *Behavior) Render(name string, data any) (string, error) {
	var out cappedWriter
	if err := b.tmpl.ExecuteTemplate(&out, name, data); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

func (b *Behavior) File(name string) (string, bool) {
	text, ok := b.files[name]
	return text, ok
}

type behaviorBundle struct {
	Version string            `json:"version"`
	Files   map[string]string `json:"files"`
}

func behaviorHash(files map[string]string) string {
	h := sha256.New()
	for _, name := range sortedKeysAny(files) {
		fmt.Fprintf(h, "%s\x00%d\x00%s", name, len(files[name]), files[name])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ParseBehavior validates a complete bundle; any error rejects all of it.
func ParseBehavior(version string, files map[string]string) (*Behavior, error) {
	b := &Behavior{Version: version, files: files, Hash: behaviorHash(files)}
	size := 0
	funcs := template.FuncMap{"short": func(sha string) string { return sha[:min(7, len(sha))] }}
	tmpl := template.New("").Option("missingkey=error").Funcs(funcs)
	for name, text := range files {
		size += len(text)
		switch {
		case name == "brief.md":
			b.Brief = text
		case name == "rules.md":
			b.Rules = text
		case name == "wake.json":
			dec := json.NewDecoder(strings.NewReader(text))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&b.Wake); err != nil {
				return nil, fmt.Errorf("behavior wake.json: %w", err)
			}
		case name == "models.json":
			models, err := parseModelPolicy(text)
			if err != nil {
				return nil, err
			}
			b.Models = models
		case path.Dir(name) == "templates" && path.Ext(name) == ".tmpl":
			// Parsed alone: a {{define}} in one file must not replace another file's template.
			own, err := template.New(strings.TrimSuffix(path.Base(name), ".tmpl")).Funcs(funcs).Parse(text)
			if err != nil {
				return nil, fmt.Errorf("behavior %s: %w", name, err)
			}
			if len(own.Templates()) != 1 {
				return nil, fmt.Errorf("behavior %s must not define other templates", name)
			}
			if _, err := tmpl.AddParseTree(own.Name(), own.Tree); err != nil {
				return nil, fmt.Errorf("behavior %s: %w", name, err)
			}
		default:
			return nil, fmt.Errorf("behavior: unknown file %q", name)
		}
	}
	b.tmpl = tmpl
	if _, ok := files["models.json"]; !ok {
		return nil, errors.New("behavior models.json: missing")
	}
	switch {
	case size > behaviorMaxBytes:
		return nil, fmt.Errorf("behavior: %d bytes exceeds %d", size, behaviorMaxBytes)
	case !strings.HasPrefix(b.Brief, "# COS"):
		return nil, errors.New("behavior brief.md must start with \"# COS\"")
	case strings.TrimSpace(b.Rules) == "":
		return nil, errors.New("behavior rules.md is empty")
	case b.Wake.MaxWakesPerHour < 1 || b.Wake.MaxWakesPerHour > 60:
		return nil, errors.New("behavior wake.json: maxWakesPerHour must be 1-60")
	case b.Wake.DigestSeconds < 60 || b.Wake.DigestSeconds > 3600:
		return nil, errors.New("behavior wake.json: digestSeconds must be 60-3600")
	case !slices.Contains(b.Wake.Immediate, "chat.message"):
		return nil, errors.New("behavior wake.json: chat.message must wake COS immediately")
	}
	for _, kind := range b.Wake.Immediate {
		if !behaviorKind.MatchString(kind) {
			return nil, fmt.Errorf("behavior wake.json: invalid event kind %q", kind)
		}
	}
	for _, name := range sortedKeys(behaviorTemplates) {
		if tmpl.Lookup(name) == nil {
			return nil, fmt.Errorf("behavior: missing templates/%s.tmpl", name)
		}
		out, err := b.Render(name, behaviorTemplates[name])
		if err != nil {
			return nil, fmt.Errorf("behavior templates/%s.tmpl: %w", name, err)
		}
		if slices.Contains(behaviorLines, name) && (out == "" || strings.Contains(out, "\n") || len(out) > behaviorLineMax) {
			return nil, fmt.Errorf("behavior templates/%s.tmpl must render one line of 1-%d characters", name, behaviorLineMax)
		}
	}
	return b, nil
}

// embeddedBehavior is the copy compiled into this binary; tests keep it valid.
var embeddedBehavior = sync.OnceValue(func() *Behavior {
	files := map[string]string{}
	err := fs.WalkDir(behavior.Files, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := behavior.Files.ReadFile(name)
		files[name] = string(data)
		return err
	})
	if err != nil {
		panic(err)
	}
	b, err := ParseBehavior("embedded", files)
	if err != nil {
		panic(err)
	}
	return b
})

// BehaviorLoader holds one node's active behavior.
type BehaviorLoader struct {
	cur      atomic.Pointer[Behavior]
	rejected atomic.Pointer[string]
}

var behaviors = &BehaviorLoader{}

// CurrentBehavior is what this process runs now: the last valid published bundle, else the embedded one.
func CurrentBehavior() *Behavior { return behaviors.Current() }

func (l *BehaviorLoader) Current() *Behavior {
	if b := l.cur.Load(); b != nil {
		return b
	}
	return embeddedBehavior()
}

// Load validates raw bundle JSON and swaps it in; on error the previous behavior stays active.
func (l *BehaviorLoader) Load(raw []byte) (*Behavior, error) {
	var bundle behaviorBundle
	err := json.Unmarshal(raw, &bundle)
	var b *Behavior
	if err == nil {
		b, err = ParseBehavior(bundle.Version, bundle.Files)
	}
	if err != nil {
		msg := err.Error()
		l.rejected.Store(&msg)
		return nil, err
	}
	b.loadedAt = time.Now().UTC()
	l.cur.Store(b)
	l.rejected.Store(nil)
	return b, nil
}

// BehaviorNode is what each node reports into the bucket after every load attempt.
type BehaviorNode struct {
	Host       string    `json:"host"`
	Version    string    `json:"version"`
	Hash       string    `json:"hash"`
	LoadedAt   time.Time `json:"loadedAt,omitzero"` // zero while running the embedded copy
	ReportedAt time.Time `json:"reportedAt"`        // refreshed every behaviorReport; an old value means the node is down
	Rejected   string    `json:"rejected,omitempty"`
}

func (l *BehaviorLoader) report(ctx context.Context, kv jetstream.KeyValue, host string) {
	b := l.Current()
	node := BehaviorNode{Host: host, Version: b.Version, Hash: b.Hash, LoadedAt: b.loadedAt, ReportedAt: time.Now().UTC()}
	if r := l.rejected.Load(); r != nil {
		node.Rejected = *r
	}
	data, _ := json.Marshal(node)
	if _, err := kv.Put(ctx, "node."+host, data); err != nil {
		logErr(ctx, slog.LevelWarn, "behavior status not recorded", err)
	}
}

// behaviorBucketKV binds COS_BEHAVIOR; only the cluster leader creates it.
func behaviorBucketKV(ctx context.Context, js jetstream.JetStream, create bool) (jetstream.KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !create {
		return js.KeyValue(ctx, behaviorBucket)
	}
	return js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: behaviorBucket, History: 16, Replicas: 3, Storage: jetstream.FileStorage})
}

// RunBehavior hot-reloads behavior on this node and, while this node holds the
// cluster lease, publishes behavior/ from main. It retries until ctx ends.
func RunBehavior(ctx context.Context, cfg Config, bus *Bus, l *BehaviorLoader) {
	js, err := bus.JetStream()
	if err != nil {
		slog.Error("behavior sync unavailable", "error", err)
		return
	}
	lead := make(chan struct{})
	go func() { defer close(lead); bus.Lead(ctx, func(ctx context.Context) { publishLoop(ctx, cfg, bus, js) }) }()
	defer func() { <-lead }()
	for ctx.Err() == nil {
		if err := watchBehavior(ctx, cfg.LocalHost, js, l); err != nil && ctx.Err() == nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
			logErr(ctx, slog.LevelWarn, "behavior watch restarting", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

func watchBehavior(ctx context.Context, host string, js jetstream.JetStream, l *BehaviorLoader) error {
	kv, err := behaviorBucketKV(ctx, js, false)
	if err != nil {
		return err
	}
	w, err := kv.Watch(ctx, behaviorKey)
	if err != nil {
		return err
	}
	defer w.Stop()
	l.report(ctx, kv, host)
	report := time.NewTicker(behaviorReport)
	defer report.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-report.C:
			l.report(ctx, kv, host)
		case e, ok := <-w.Updates():
			if !ok {
				return errors.New("behavior watch closed")
			}
			if e == nil || e.Operation() != jetstream.KeyValuePut {
				continue
			}
			if b, err := l.Load(e.Value()); err != nil {
				logErr(ctx, slog.LevelError, "behavior rejected; keeping previous", err)
			} else {
				slog.Info("behavior loaded", "version", b.Version, "hash", b.Hash[:12])
			}
			l.report(ctx, kv, host)
		}
	}
}

// publishLoop runs on the leader: at once, on every push to main (TS-814 event) and every behaviorPoll.
func publishLoop(ctx context.Context, cfg Config, bus *Bus, js jetstream.JetStream) {
	push := make(chan *nats.Msg, 1)
	sub, err := bus.conn.ChanSubscribe(Event{Source: SourceForgejo, Kind: KindPushMain}.Subject(), push)
	if err == nil {
		defer sub.Unsubscribe()
	}
	tick := time.NewTimer(0)
	defer tick.Stop()
	var kv jetstream.KeyValue
	published := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-push:
		case <-tick.C:
			tick.Reset(behaviorPoll)
		}
		if kv == nil {
			if kv, err = behaviorBucketKV(ctx, js, true); err != nil {
				kv = nil
				logErr(ctx, slog.LevelWarn, "behavior bucket unavailable", err)
				continue
			}
		}
		sha, err := publishBehavior(ctx, Forgejo{Config: cfg}, kv, published)
		if err != nil {
			logErr(ctx, slog.LevelWarn, "behavior publish failed", err)
		}
		published = sha
	}
}

// publishBehavior copies behavior/ at main's head into the bucket when main moved and
// the content changed. Invalid content is never published.
func publishBehavior(ctx context.Context, f Forgejo, kv jetstream.KeyValue, last string) (string, error) {
	var branch struct {
		Commit struct {
			ID string `json:"id"`
		} `json:"commit"`
	}
	if _, err := f.request(ctx, "GET", "/repos/"+behaviorRepo+"/branches/main", nil, &branch); err != nil {
		return last, err
	}
	sha := branch.Commit.ID
	if sha == "" || sha == last {
		return last, nil
	}
	var tree struct {
		Tree []struct{ Path, Type, SHA string } `json:"tree"`
	}
	if _, err := f.request(ctx, "GET", "/repos/"+behaviorRepo+"/git/trees/"+sha+"?recursive=true&per_page=10000", nil, &tree); err != nil {
		return last, err
	}
	files := map[string]string{}
	for _, e := range tree.Tree {
		name, ok := strings.CutPrefix(e.Path, "behavior/")
		if !ok || e.Type != "blob" || path.Ext(name) == ".go" {
			continue
		}
		var blob struct{ Content string }
		if _, err := f.request(ctx, "GET", "/repos/"+behaviorRepo+"/git/blobs/"+e.SHA, nil, &blob); err != nil {
			return last, err
		}
		data, err := base64.StdEncoding.DecodeString(blob.Content)
		if err != nil {
			return last, fmt.Errorf("behavior %s: %w", name, err)
		}
		files[name] = string(data)
	}
	b, err := ParseBehavior(sha, files)
	if err != nil {
		// Main's behavior stays rejected (and visible in cos behavior) until a later merge fixes it.
		err = fmt.Errorf("main %s: %w", sha[:min(7, len(sha))], err)
		data, _ := json.Marshal(map[string]string{"version": sha, "error": err.Error()})
		if _, perr := kv.Put(ctx, "rejected", data); perr != nil {
			return last, perr
		}
		return sha, err
	}
	if err := kv.Delete(ctx, "rejected"); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return last, err
	}
	cur, err := kv.Get(ctx, behaviorKey)
	var rev uint64
	switch {
	case err == nil:
		var old behaviorBundle
		if json.Unmarshal(cur.Value(), &old) == nil && behaviorHash(old.Files) == b.Hash {
			return sha, nil
		}
		rev = cur.Revision()
	case !errors.Is(err, jetstream.ErrKeyNotFound):
		return last, err
	}
	data, _ := json.Marshal(behaviorBundle{Version: sha, Files: files})
	if rev == 0 {
		_, err = kv.Create(ctx, behaviorKey, data)
	} else {
		_, err = kv.Update(ctx, behaviorKey, data, rev)
	}
	if err != nil {
		return last, err
	}
	slog.Info("behavior published", "version", sha, "hash", b.Hash[:12])
	return sha, nil
}

// BehaviorStatus reports the published bundle and what every node has loaded.
func BehaviorStatus(ctx context.Context, bus *Bus) (map[string]any, error) {
	js, err := bus.JetStream()
	if err != nil {
		return nil, err
	}
	kv, err := behaviorBucketKV(ctx, js, false)
	if err != nil {
		return nil, fmt.Errorf("COS behavior store unavailable; is cos serve running? (%w)", err)
	}
	out := map[string]any{"published": nil, "nodes": []BehaviorNode{}}
	if e, err := kv.Get(ctx, behaviorKey); err == nil {
		var bundle behaviorBundle
		if json.Unmarshal(e.Value(), &bundle) == nil {
			out["published"] = map[string]any{"version": bundle.Version, "hash": behaviorHash(bundle.Files), "files": sortedKeysAny(bundle.Files)}
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	if e, err := kv.Get(ctx, "rejected"); err == nil {
		out["rejected"] = json.RawMessage(e.Value())
	}
	keys, err := kv.ListKeys(ctx, jetstream.MetaOnly())
	if err != nil {
		return nil, err
	}
	var nodes []BehaviorNode
	for k := range keys.Keys() {
		if !strings.HasPrefix(k, "node.") {
			continue
		}
		e, err := kv.Get(ctx, k)
		var n BehaviorNode
		if err == nil && json.Unmarshal(e.Value(), &n) == nil {
			nodes = append(nodes, n)
		}
	}
	slices.SortFunc(nodes, func(a, b BehaviorNode) int { return strings.Compare(a.Host, b.Host) })
	if nodes != nil {
		out["nodes"] = nodes
	}
	return out, nil
}
