package cos

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Token usage is read from each node's own Claude transcripts, published into the
// replicated COS_USAGE bucket, and mirrored into every node's SQLite state.
const (
	usageBucket    = "COS_USAGE"
	usageKeepDays  = 31
	usageScanEvery = 2 * time.Minute
)

// UsageRow is one day's usage for one session and model on one host.
type UsageRow struct {
	Day          string    `json:"day"`
	Host         string    `json:"host"`
	Project      string    `json:"project"`
	Kind         string    `json:"kind"`
	Worktree     string    `json:"worktree,omitempty"`
	Branch       string    `json:"branch,omitempty"`
	Ticket       string    `json:"ticket,omitempty"`
	Session      string    `json:"session"`
	Model        string    `json:"model"`
	Effort       string    `json:"effort,omitempty"`
	Input        int64     `json:"input"`
	Output       int64     `json:"output"`
	CacheWrite5m int64     `json:"cacheWrite5m"`
	CacheWrite1h int64     `json:"cacheWrite1h"`
	CacheRead    int64     `json:"cacheRead"`
	Messages     int       `json:"messages"`
	First        time.Time `json:"first"`
	Last         time.Time `json:"last"`
}

type usagePrice struct{ In, Out, Write5m, Write1h, Read float64 }

// shortcut: Haiku 5.5 prompts over 100K tokens bill at $0.50/$2.50; upgrade when transcripts carry the prompt size per request.
var usagePrices = map[string]usagePrice{
	"claude-opus-5-5":   {4, 20, 5, 8, 0.20},
	"claude-sonnet-5-5": {2, 10, 2.5, 4, 0.20},
	"claude-haiku-5-5":  {0.10, 0.50, 0.125, 0.20, 0.01},
	"claude-fable-5-1":  {10, 50, 12.5, 20, 0.25},
	"claude-opus-5":     {5, 25, 6.25, 10, 0.50},
}

var usageDateSuffix = regexp.MustCompile(`-\d{8}$`)

// usageModel drops the context hint ("[1m]") and the release date from a model ID.
func usageModel(model string) string {
	model, _, _ = strings.Cut(model, "[")
	return usageDateSuffix.ReplaceAllString(model, "")
}

// Cost is USD at list price. An unknown model costs 0 rather than a guess.
func (r UsageRow) Cost() float64 {
	p := usagePrices[usageModel(r.Model)]
	return (float64(r.Input)*p.In + float64(r.Output)*p.Out + float64(r.CacheWrite5m)*p.Write5m + float64(r.CacheWrite1h)*p.Write1h + float64(r.CacheRead)*p.Read) / 1e6
}

// usageProject reads the worker or COS agent from Claude's project dir name, which
// is the transcript path with "/" and "." replaced by "-".
func usageProject(project string) (kind, worktree string) {
	if _, after, ok := strings.Cut(project, "-t3-worktrees-"); ok {
		return "worker", after
	}
	if strings.HasSuffix(project, "-local-share-cos-agent") {
		return "cos", ""
	}
	return "other", ""
}

func usageTicket(branch, worktree string) string {
	ticket := ticketPattern.FindString(branch)
	if ticket == "" {
		ticket = ticketPattern.FindString(worktree)
	}
	return strings.ToUpper(ticket)
}

// usageWorker is a Haiku session; every other model counts as advisor work.
func usageWorker(model string) bool { return strings.Contains(model, "haiku") }

type usageEntry struct {
	Timestamp time.Time `json:"timestamp"`
	SessionID string    `json:"sessionId"`
	GitBranch string    `json:"gitBranch"`
	Effort    string    `json:"effort"`
	Message   struct {
		ID    string `json:"id"`
		Model string `json:"model"`
		Usage struct {
			Input      int64 `json:"input_tokens"`
			Output     int64 `json:"output_tokens"`
			CacheRead  int64 `json:"cache_read_input_tokens"`
			CacheWrite int64 `json:"cache_creation_input_tokens"`
			Breakdown  *struct {
				Write5m int64 `json:"ephemeral_5m_input_tokens"`
				Write1h int64 `json:"ephemeral_1h_input_tokens"`
			} `json:"cache_creation"`
		} `json:"usage"`
	} `json:"message"`
}

// scanTranscript sums one transcript. Claude Code writes one line per content block,
// each repeating the message's usage with a growing count, so lines dedupe by message
// ID and keep the per-field max.
func scanTranscript(path, project, host string, since time.Time) ([]UsageRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	kind, worktree := usageProject(project)
	msgs := map[string]*UsageRow{}
	r := bufio.NewReader(f)
	for n := 0; ; n++ {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break // a final line without '\n' is still being written
		}
		if err != nil {
			return nil, err
		}
		if !bytes.Contains(line, []byte(`"type":"assistant"`)) || !bytes.Contains(line, []byte(`"usage"`)) {
			continue
		}
		var e usageEntry
		if json.Unmarshal(line, &e) != nil || e.Timestamp.Before(since) || e.Message.Model == "<synthetic>" {
			continue
		}
		id := e.Message.ID
		if id == "" {
			id = fmt.Sprintf("line:%d", n)
		}
		ts := e.Timestamp.UTC()
		m := msgs[id]
		if m == nil {
			m = &UsageRow{Day: ts.Format("2006-01-02"), Host: host, Project: project, Kind: kind, Worktree: worktree,
				Session: e.SessionID, Model: e.Message.Model, First: ts, Last: ts, Messages: 1}
			msgs[id] = m
		}
		u := e.Message.Usage
		w5, w1 := u.CacheWrite, int64(0)
		if u.Breakdown != nil {
			w5, w1 = u.Breakdown.Write5m, u.Breakdown.Write1h
		}
		m.Branch = cmp.Or(e.GitBranch, m.Branch)
		m.Effort = cmp.Or(e.Effort, m.Effort)
		m.Input = max(m.Input, u.Input)
		m.Output = max(m.Output, u.Output)
		m.CacheRead = max(m.CacheRead, u.CacheRead)
		m.CacheWrite5m = max(m.CacheWrite5m, w5)
		m.CacheWrite1h = max(m.CacheWrite1h, w1)
		if ts.After(m.Last) {
			m.Last = ts
		}
	}
	merged := map[usageGroup]UsageRow{}
	for _, m := range msgs {
		usageAdd(merged, *m)
	}
	return usageRows(merged), nil
}

type usageGroup struct{ Day, Host, Project, Session, Model string }

// usageAdd folds r into its group. Branch and Effort follow the latest activity.
func usageAdd(m map[usageGroup]UsageRow, r UsageRow) {
	g := usageGroup{r.Day, r.Host, r.Project, r.Session, r.Model}
	cur, ok := m[g]
	if !ok {
		m[g] = r
		return
	}
	cur.Input += r.Input
	cur.Output += r.Output
	cur.CacheWrite5m += r.CacheWrite5m
	cur.CacheWrite1h += r.CacheWrite1h
	cur.CacheRead += r.CacheRead
	cur.Messages += r.Messages
	if r.First.Before(cur.First) {
		cur.First = r.First
	}
	if r.Last.After(cur.Last) {
		cur.Last = r.Last
		cur.Branch = cmp.Or(r.Branch, cur.Branch)
		cur.Effort = cmp.Or(r.Effort, cur.Effort)
	}
	m[g] = cur
}

func usageRows(m map[usageGroup]UsageRow) []UsageRow {
	rows := make([]UsageRow, 0, len(m))
	for _, r := range m {
		r.Ticket = usageTicket(r.Branch, r.Worktree)
		rows = append(rows, r)
	}
	return rows
}

// usageScanner caches each transcript's rows until its size or mtime changes, so a
// scan re-reads only the files that grew.
type usageScanner struct {
	files map[string]usageFile
}

type usageFile struct {
	modTime time.Time
	size    int64
	rows    []UsageRow
}

// Scan returns this host's rows for every transcript under root/<project>/.
func (s *usageScanner) Scan(root, host string, now time.Time) ([]UsageRow, error) {
	since := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -usageKeepDays)
	sinceDay := since.Format("2006-01-02")
	cutoff := now.AddDate(0, 0, -usageKeepDays)
	files := map[string]usageFile{}
	merged := map[usageGroup]UsageRow{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(cutoff) {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		project, _, _ := strings.Cut(rel, string(filepath.Separator))
		file, ok := s.files[path]
		if !ok || !file.modTime.Equal(info.ModTime()) || file.size != info.Size() {
			rows, err := scanTranscript(path, project, host, since)
			if err != nil {
				slog.Warn("scan usage transcript", "path", path, "error", err)
				return nil
			}
			file = usageFile{info.ModTime(), info.Size(), rows}
		}
		files[path] = file
		for _, r := range file.rows {
			if r.Day >= sinceDay {
				usageAdd(merged, r)
			}
		}
		return nil
	})
	s.files = files
	return usageRows(merged), err
}

// usageKey is the KV key for one row. Keys allow only [-/_=.a-zA-Z0-9].
func usageKey(r UsageRow) string {
	host := strings.Map(func(c rune) rune {
		if strings.ContainsRune("-/_=.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", c) {
			return c
		}
		return '_'
	}, r.Host)
	sum := sha256.Sum256([]byte(r.Project + "\x00" + r.Session + "\x00" + r.Model))
	return strings.ReplaceAll(r.Day, "-", "") + "." + host + "." + hex.EncodeToString(sum[:8])
}

// RunUsage publishes this node's transcript usage into COS_USAGE and mirrors every
// node's rows into SQLite. The bucket is the only thing nodes share.
func RunUsage(ctx context.Context, bus *Bus, host string, store *Store) {
	home, err := os.UserHomeDir()
	if err != nil {
		slog.Error("COS usage disabled", "error", err)
		return
	}
	retry := time.NewTicker(2 * time.Second)
	defer retry.Stop()
	var kv jetstream.KeyValue
	for {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		kv, err = openKVConfig(attempt, bus, jetstream.KeyValueConfig{Bucket: usageBucket, History: 1, TTL: (usageKeepDays + 4) * 24 * time.Hour})
		stop()
		if err == nil {
			break
		}
		slog.Warn("COS usage not ready", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-retry.C:
		}
	}
	slog.Info("COS usage ready", "host", host)
	var mirror sync.WaitGroup
	defer mirror.Wait()
	mirror.Go(func() {
		for ctx.Err() == nil {
			if err := mirrorUsage(ctx, kv, store); err != nil && ctx.Err() == nil {
				slog.Error("mirror usage", "error", err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
	})
	root := filepath.Join(home, ".claude", "projects")
	var scanner usageScanner
	published := map[string][]byte{}
	tick := time.NewTicker(usageScanEvery)
	defer tick.Stop()
	for {
		rows, err := scanner.Scan(root, host, time.Now())
		if err != nil {
			slog.Error("scan usage", "error", err)
		}
		publishUsage(ctx, kv, rows, published)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// publishUsage puts only rows that changed since this process last saw them. A key
// it has not seen yet is compared with the bucket, so a restart rewrites nothing.
func publishUsage(ctx context.Context, kv jetstream.KeyValue, rows []UsageRow, last map[string][]byte) {
	for _, r := range rows {
		key := usageKey(r)
		data, err := json.Marshal(r)
		if err != nil {
			slog.Error("marshal usage row", "key", key, "error", err)
			continue
		}
		prev, ok := last[key]
		if !ok {
			if entry, err := kv.Get(ctx, key); err == nil {
				prev = entry.Value()
			}
		}
		if bytes.Equal(prev, data) {
			last[key] = data
			continue
		}
		if _, err := kv.Put(ctx, key, data); err != nil {
			slog.Error("publish usage", "key", key, "error", err)
			continue
		}
		last[key] = data
	}
}

// mirrorUsage copies the bucket into SQLite until ctx ends. The initial replay is
// followed by a prune, so rows the bucket no longer has disappear locally too.
func mirrorUsage(ctx context.Context, kv jetstream.KeyValue, store *Store) error {
	w, err := kv.WatchAll(ctx)
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
				return errors.New("usage watch closed")
			}
			if entry == nil {
				if seen != nil {
					if err := store.PruneUsage(ctx, seen); err != nil {
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
				if err := store.DeleteUsage(ctx, entry.Key()); err != nil {
					return err
				}
				continue
			}
			var row UsageRow
			if err := json.Unmarshal(entry.Value(), &row); err != nil {
				slog.Error("skip corrupt usage row", "key", entry.Key(), "error", err)
				continue
			}
			if err := store.SaveUsage(ctx, entry.Key(), row); err != nil {
				return err
			}
		}
	}
}

func (s *Store) SaveUsage(ctx context.Context, key string, row UsageRow) error {
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	// Plain Exec, not s.change: usage updates must not bump the dashboard revision.
	_, err = s.DB.ExecContext(ctx, `INSERT INTO usage VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET day=excluded.day, payload=excluded.payload`, key, row.Day, string(data))
	return err
}

func (s *Store) DeleteUsage(ctx context.Context, key string) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM usage WHERE key=?`, key)
	return err
}

// PruneUsage drops local rows whose keys the bucket no longer has.
func (s *Store) PruneUsage(ctx context.Context, keep map[string]bool) error {
	rows, err := s.DB.QueryContext(ctx, `SELECT key FROM usage`)
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		if !keep[key] {
			stale = append(stale, key)
		}
	}
	err = rows.Err()
	rows.Close() // the single SQLite connection must be free before the deletes
	if err != nil {
		return err
	}
	for _, key := range stale {
		if err := s.DeleteUsage(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

// Usage returns rows on or after sinceDay (YYYY-MM-DD, UTC).
func (s *Store) Usage(ctx context.Context, sinceDay string) ([]UsageRow, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT payload FROM usage WHERE day>=?`, sinceDay)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UsageRow{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var r UsageRow
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type UsageTotal struct {
	Key           string  `json:"key"`
	Label         string  `json:"label"`
	Host          string  `json:"host,omitempty"`
	Thread        string  `json:"thread,omitempty"`
	Ticket        string  `json:"ticket,omitempty"`
	Input         int64   `json:"input"`
	Output        int64   `json:"output"`
	CacheWrite    int64   `json:"cacheWrite"`
	CacheRead     int64   `json:"cacheRead"`
	Cost          float64 `json:"cost"`
	AdvisorOutput int64   `json:"advisorOutput"`
	WorkerOutput  int64   `json:"workerOutput"`
	AdvisorCost   float64 `json:"advisorCost"`
	WorkerCost    float64 `json:"workerCost"`
	Workers       int     `json:"workers"`
	Parallel      int     `json:"parallel"`
	HaikuShare    int     `json:"haikuShare"`
}

type usageSpan struct{ first, last time.Time }

// SummarizeUsage groups rows by thread, ticket, day, model or kind. A thread takes
// its title from the live fleet when a thread on the same branch is listed there.
func SummarizeUsage(rows []UsageRow, live Live, by string) []UsageTotal {
	groups := map[string][]UsageRow{}
	for _, r := range rows {
		k := usageGroupKey(r, by)
		groups[k] = append(groups[k], r)
	}
	totals := make([]UsageTotal, 0, len(groups))
	for k, g := range groups {
		t := usageTotal(k, g)
		if by == "thread" {
			usageThread(&t, latestUsage(g), live)
		}
		totals = append(totals, t)
	}
	if by == "day" {
		slices.SortFunc(totals, func(a, b UsageTotal) int { return strings.Compare(a.Key, b.Key) })
	} else {
		slices.SortFunc(totals, func(a, b UsageTotal) int {
			return cmp.Or(cmp.Compare(b.Cost, a.Cost), strings.Compare(a.Key, b.Key))
		})
	}
	return totals
}

func usageGroupKey(r UsageRow, by string) string {
	switch by {
	case "ticket":
		return cmp.Or(r.Ticket, "(no ticket)")
	case "day":
		return r.Day
	case "model":
		return usageModel(r.Model)
	case "kind":
		return r.Kind
	}
	return r.Host + "/" + r.Project
}

// usageTotal sums rows. Workers are the distinct Haiku sessions; Parallel is the most
// of them running at one instant.
func usageTotal(key string, rows []UsageRow) UsageTotal {
	t := UsageTotal{Key: key, Label: key, Ticket: latestUsage(rows).Ticket}
	spans := map[string]usageSpan{}
	for _, r := range rows {
		cost := r.Cost()
		t.Input += r.Input
		t.Output += r.Output
		t.CacheWrite += r.CacheWrite5m + r.CacheWrite1h
		t.CacheRead += r.CacheRead
		t.Cost += cost
		if !usageWorker(r.Model) {
			t.AdvisorOutput += r.Output
			t.AdvisorCost += cost
			continue
		}
		t.WorkerOutput += r.Output
		t.WorkerCost += cost
		s, seen := spans[r.Session]
		if !seen || r.First.Before(s.first) {
			s.first = r.First
		}
		if !seen || r.Last.After(s.last) {
			s.last = r.Last
		}
		spans[r.Session] = s
	}
	if t.Output > 0 {
		t.HaikuShare = int(math.Round(100 * float64(t.WorkerOutput) / float64(t.Output)))
	}
	t.Workers = len(spans)
	t.Parallel = usageParallel(spans)
	return t
}

// usageParallel counts the most sessions running at one instant. A session ending
// as another starts does not overlap it; a single-instant session still occupies one.
func usageParallel(spans map[string]usageSpan) int {
	type edge struct {
		at    time.Time
		delta int
	}
	var edges []edge
	for _, s := range spans {
		end := s.last
		if !end.After(s.first) {
			end = s.first.Add(time.Nanosecond)
		}
		edges = append(edges, edge{s.first, 1}, edge{end, -1})
	}
	slices.SortFunc(edges, func(a, b edge) int {
		return cmp.Or(a.at.Compare(b.at), a.delta-b.delta) // ends sort before starts at one instant
	})
	open, most := 0, 0
	for _, e := range edges {
		open += e.delta
		most = max(most, open)
	}
	return most
}

func latestUsage(rows []UsageRow) UsageRow {
	var latest UsageRow
	for _, r := range rows {
		if r.Last.After(latest.Last) {
			latest = r
		}
	}
	return latest
}

func usageThread(t *UsageTotal, latest UsageRow, live Live) {
	t.Host, t.Label = latest.Host, cmp.Or(latest.Worktree, latest.Project)
	for _, h := range live.Fleet {
		for _, th := range h.Threads {
			if h.Host == latest.Host && latest.Branch != "" && th.Branch == latest.Branch && t.Thread == "" {
				t.Thread, t.Label = th.ThreadID, cmp.Or(th.Title, t.Label)
			}
		}
	}
	if latest.Kind == "cos" {
		t.Label = "COS agent"
	}
}
