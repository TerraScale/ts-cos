package cos

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// usageCount is one set of token totals. Tokens is input, cache writes and output together.
type usageCount struct {
	Messages  int   `json:"messages"`
	Tokens    int64 `json:"tokens"`
	Output    int64 `json:"output"`
	CacheRead int64 `json:"cacheRead"`
}

func (c *usageCount) add(r UsageRow) {
	c.Messages += r.Messages
	c.Tokens += r.Input + r.CacheWrite5m + r.CacheWrite1h + r.Output
	c.Output += r.Output
	c.CacheRead += r.CacheRead
}

// usageVerifyReport is the usage-verify answer: one host's three independent totals for today.
type usageVerifyReport struct {
	Host       string                `json:"host"`
	Day        string                `json:"day"`
	Transcript usageCount            `json:"transcript"`
	KV         usageCount            `json:"kv"`
	Mirror     map[string]usageCount `json:"mirror"`
}

// usageVerify republishes this host's usage from today's transcripts, then counts today three ways: straight
// from the transcripts, from this host's COS_USAGE rows and from every host's rows in this node's SQLite.
// A "mirror" arg returns only the SQLite totals, so polling the mirror does not rescan.
func usageVerify(ctx context.Context, bus *Bus, store *Store, host string, args []string) (usageVerifyReport, error) {
	now := time.Now()
	day := now.UTC().Format(time.DateOnly)
	if len(args) > 0 && args[0] == "mirror" {
		mirror, err := mirrorUsageTotals(ctx, store, day)
		if err != nil {
			return usageVerifyReport{}, err
		}
		return usageVerifyReport{Host: host, Day: day, Mirror: mirror}, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return usageVerifyReport{}, err
	}
	root := filepath.Join(home, ".claude", "projects")
	kv, err := openKVConfig(ctx, bus, usageBucketConfig)
	if err != nil {
		return usageVerifyReport{}, err
	}
	rows, err := todayUsage(root, host, now)
	if err != nil {
		return usageVerifyReport{}, err
	}
	if _, err := publishUsage(ctx, kv, rows, map[string][]byte{}); err != nil {
		return usageVerifyReport{}, fmt.Errorf("publish usage: %w", err)
	}
	transcript, err := transcriptTotals(root, day)
	if err != nil {
		return usageVerifyReport{}, err
	}
	bucket, err := kvUsageTotals(ctx, kv, host, day)
	if err != nil {
		return usageVerifyReport{}, err
	}
	mirror, err := mirrorUsageTotals(ctx, store, day)
	if err != nil {
		return usageVerifyReport{}, err
	}
	return usageVerifyReport{Host: host, Day: day, Transcript: transcript, KV: bucket, Mirror: mirror}, nil
}

// todayUsage scans only the transcripts modified since the UTC day started, and keeps only today's rows.
// It is the fresh publish: the 31-day scanner is too slow to run on every check.
func todayUsage(root, host string, now time.Time) ([]UsageRow, error) {
	dayStart := now.UTC().Truncate(24 * time.Hour)
	day := dayStart.Format(time.DateOnly)
	merged := map[usageGroup]UsageRow{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(dayStart) {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		project, _, _ := strings.Cut(rel, string(filepath.Separator))
		rows, err := scanTranscript(path, project, host, dayStart)
		if err != nil {
			slog.Warn("scan usage transcript", "path", path, "error", err)
			return nil
		}
		for _, r := range rows {
			usageAdd(merged, r)
		}
		return nil
	})
	var today []UsageRow
	for _, r := range usageRows(merged) {
		if r.Day == day {
			today = append(today, r)
		}
	}
	return today, err
}

// transcriptTotals counts one UTC day straight from the transcripts under root. It is a second, minimal
// parser on purpose: the check compares it with usageScanner, so it must not share the scanner's code.
func transcriptTotals(root, day string) (usageCount, error) {
	start, err := time.Parse(time.DateOnly, day)
	if err != nil {
		return usageCount{}, err
	}
	perMessage := map[string]verifyTokens{}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(start) {
			return nil
		}
		return countTranscript(path, day, perMessage)
	})
	if err != nil {
		return usageCount{}, err
	}
	var total usageCount
	total.Messages = len(perMessage)
	for _, t := range perMessage {
		total.Tokens += t.input + t.cacheWrite + t.output
		total.Output += t.output
		total.CacheRead += t.cacheRead
	}
	return total, nil
}

// verifyTokens is one message's largest usage seen so far. Claude Code repeats the usage on every
// content block of a message, with a growing count.
type verifyTokens struct{ input, cacheWrite, output, cacheRead int64 }

// countTranscript folds one file's assistant lines for day into perMessage. A line without a message
// ID counts as its own message. A final line without '\n' is still being written and is skipped.
func countTranscript(path, day string, perMessage map[string]verifyTokens) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for n := 0; ; n++ {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		var e map[string]any
		if json.Unmarshal(line, &e) != nil || e["type"] != "assistant" {
			continue
		}
		msg, _ := e["message"].(map[string]any)
		usage, _ := msg["usage"].(map[string]any)
		stamp, _ := e["timestamp"].(string)
		ts, err := time.Parse(time.RFC3339Nano, stamp)
		if usage == nil || msg["model"] == "<synthetic>" || err != nil || ts.UTC().Format(time.DateOnly) != day {
			continue
		}
		id, _ := msg["id"].(string)
		if id == "" {
			id = fmt.Sprintf("%s:%d", path, n)
		}
		t := perMessage[id]
		t.input = max(t.input, jsonInt(usage, "input_tokens"))
		t.cacheWrite = max(t.cacheWrite, jsonInt(usage, "cache_creation_input_tokens"))
		t.output = max(t.output, jsonInt(usage, "output_tokens"))
		t.cacheRead = max(t.cacheRead, jsonInt(usage, "cache_read_input_tokens"))
		perMessage[id] = t
	}
}

// jsonInt reads a JSON number, which Unmarshal into a map decodes as float64.
func jsonInt(m map[string]any, key string) int64 {
	f, _ := m[key].(float64)
	return int64(f)
}

// kvUsageTotals sums this host's COS_USAGE rows for day.
func kvUsageTotals(ctx context.Context, kv jetstream.KeyValue, host, day string) (usageCount, error) {
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return usageCount{}, nil
	}
	if err != nil {
		return usageCount{}, err
	}
	prefix := strings.ReplaceAll(day, "-", "") + "." + usageHostKey(host) + "."
	var total usageCount
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry, err := kv.Get(ctx, key)
		if err != nil {
			return usageCount{}, err
		}
		var r UsageRow
		if err := json.Unmarshal(entry.Value(), &r); err != nil {
			return usageCount{}, err
		}
		total.add(r)
	}
	return total, nil
}

// mirrorUsageTotals sums this node's SQLite copy of every host's rows for day.
func mirrorUsageTotals(ctx context.Context, store *Store, day string) (map[string]usageCount, error) {
	rows, err := store.Usage(ctx, day)
	if err != nil {
		return nil, err
	}
	mirror := map[string]usageCount{}
	for _, r := range rows {
		if r.Day != day {
			continue
		}
		c := mirror[r.Host]
		c.add(r)
		mirror[r.Host] = c
	}
	return mirror, nil
}

// withinTwoPercent reports whether got is within 2% of want, in integers. Zero must match zero.
func withinTwoPercent(got, want int64) bool {
	d := got - want
	if d < 0 {
		d = -d
	}
	return d*50 <= want
}
