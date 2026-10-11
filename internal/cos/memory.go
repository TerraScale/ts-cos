package cos

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	memoryBucket       = "COS_MEMORY"
	memoryMaxText      = 2000
	memoryMaxTags      = 8
	memoryRecallK      = 12
	memoryRecallTokens = 1500
)

// memoryRecencyScale is the age at which a recency boost has lost half its strength.
const memoryRecencyScale = 14 * 24 * time.Hour

// MemoryEntry is one durable decision, preference, learning or fact. Shared context files are
// read as entries at recall time and never stored here.
type MemoryEntry struct {
	ID     string    `json:"id"`
	Kind   string    `json:"kind"`
	Text   string    `json:"text"`
	Tags   []string  `json:"tags,omitempty"`
	Pinned bool      `json:"pinned,omitempty"`
	At     time.Time `json:"at"`
}

// MemoryRecall is what a brief gets: pinned entries first, then the best matches within the token cap.
type MemoryRecall struct {
	Entries    []MemoryEntry `json:"entries"`
	Tokens     int           `json:"tokens"`
	FullTokens int           `json:"fullTokens"`
	Omitted    int           `json:"omitted"`
}

var memoryKinds = []string{"decision", "preference", "learning", "fact"}

var memoryWord = regexp.MustCompile(`[\p{L}\p{N}]+`)

var memoryStopwords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true, "be": true,
	"by": true, "do": true, "for": true, "from": true, "how": true, "i": true, "in": true,
	"is": true, "it": true, "of": true, "on": true, "or": true, "that": true, "the": true,
	"this": true, "to": true, "we": true, "what": true, "when": true, "why": true, "with": true,
	"you": true,
}

func init() {
	cliCommands["memory"] = cliCommand{Usage: "memory list | add <decision|preference|learning|fact> '<text>' [tag,tag] | pin <id> | unpin <id> | rm <id> | recall '<query>'", Run: func(ctx context.Context, cfg Config, args []string, w io.Writer) error {
		bus, err := ConnectBus(ctx, cfg)
		if err != nil {
			return err
		}
		defer bus.Close()
		mem, err := openKV(ctx, bus, memoryBucket)
		if err != nil {
			return err
		}
		ctxKV, err := openKV(ctx, bus, contextBucket)
		if err != nil {
			return err
		}
		return memoryCommand(ctx, mem, ctxKV, args, w)
	}}
}

func memoryCommand(ctx context.Context, mem, ctxKV jetstream.KeyValue, args []string, w io.Writer) error {
	usage := errors.New("usage: cos memory list | add <decision|preference|learning|fact> '<text>' [tag,tag] | pin <id> | unpin <id> | rm <id> | recall '<query>'")
	if len(args) == 0 {
		return usage
	}
	switch {
	case args[0] == "list" && len(args) == 1:
		entries, err := memoryEntries(ctx, mem)
		if err != nil {
			return err
		}
		return output(w, entries)
	case args[0] == "add" && (len(args) == 3 || len(args) == 4):
		e := MemoryEntry{Kind: args[1], Text: strings.TrimSpace(args[2])}
		if len(args) == 4 {
			e.Tags = strings.Split(args[3], ",")
		}
		e, err := memoryAdd(ctx, mem, e)
		if err != nil {
			return err
		}
		return output(w, e)
	case (args[0] == "pin" || args[0] == "unpin") && len(args) == 2:
		e, err := memoryGet(ctx, mem, args[1])
		if err != nil {
			return err
		}
		e.Pinned = args[0] == "pin"
		if err := memoryPut(ctx, mem, e); err != nil {
			return err
		}
		return output(w, e)
	case args[0] == "rm" && len(args) == 2:
		if _, err := memoryGet(ctx, mem, args[1]); err != nil {
			return err
		}
		if err := mem.Delete(ctx, args[1]); err != nil {
			return err
		}
		return output(w, map[string]any{"removed": args[1]})
	case args[0] == "recall" && len(args) == 2:
		r, err := Recall(ctx, mem, ctxKV, args[1], memoryRecallK, memoryRecallTokens)
		if err != nil {
			return err
		}
		return output(w, r)
	}
	return usage
}

func validMemory(e MemoryEntry) error {
	if !slices.Contains(memoryKinds, e.Kind) {
		return errors.New("memory kind must be decision, preference, learning or fact")
	}
	if n := len(e.Text); n == 0 || n > memoryMaxText {
		return errors.New("memory text must be 1 byte to 2000 bytes")
	}
	if len(e.Tags) > memoryMaxTags {
		return errors.New("memory takes at most 8 tags")
	}
	for _, tag := range e.Tags {
		if !contextName.MatchString(tag) {
			return errors.New("memory tags must be 1-64 lowercase letters, digits or dashes")
		}
	}
	return nil
}

func memoryAdd(ctx context.Context, mem jetstream.KeyValue, e MemoryEntry) (MemoryEntry, error) {
	if err := validMemory(e); err != nil {
		return e, err
	}
	e.At = time.Now().UTC()
	e.ID = chatID(e.At)
	return e, memoryPut(ctx, mem, e)
}

func memoryPut(ctx context.Context, mem jetstream.KeyValue, e MemoryEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = mem.Put(ctx, e.ID, data)
	return err
}

// memoryRead returns the stored entry; the error is jetstream.ErrKeyNotFound when it is gone.
func memoryRead(ctx context.Context, mem jetstream.KeyValue, id string) (MemoryEntry, error) {
	entry, err := mem.Get(ctx, id)
	if err != nil {
		return MemoryEntry{}, err
	}
	var e MemoryEntry
	if err := json.Unmarshal(entry.Value(), &e); err != nil {
		return MemoryEntry{}, err
	}
	e.ID = id
	return e, nil
}

func memoryGet(ctx context.Context, mem jetstream.KeyValue, id string) (MemoryEntry, error) {
	e, err := memoryRead(ctx, mem, id)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return e, errors.New("no memory entry with id " + id)
	}
	return e, err
}

// memoryEntries returns the stored entries, newest first.
func memoryEntries(ctx context.Context, mem jetstream.KeyValue) ([]MemoryEntry, error) {
	lister, err := mem.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	defer lister.Stop()
	out := []MemoryEntry{}
	for key := range lister.Keys() {
		e, err := memoryRead(ctx, mem, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b MemoryEntry) int { return b.At.Compare(a.At) })
	return out, nil
}

// contextEntries reads every shared context file as fact entries; the focus file is pinned.
func contextEntries(ctx context.Context, kv jetstream.KeyValue) ([]MemoryEntry, error) {
	files, err := contextFiles(ctx, kv, true)
	if err != nil {
		return nil, err
	}
	var out []MemoryEntry
	for _, f := range files {
		at, err := time.Parse("2006-01-02T15:04:05Z", f.Updated)
		if err != nil {
			return nil, err
		}
		for i, text := range memoryParagraphs(f.Text) {
			out = append(out, MemoryEntry{ID: fmt.Sprintf("ctx.%s.%d", f.Name, i), Kind: "fact", Text: text, Tags: []string{f.Name}, Pinned: f.Name == "focus", At: at})
		}
	}
	return out, nil
}

// memoryParagraphs splits on blank lines; each markdown bullet is its own entry.
func memoryParagraphs(text string) []string {
	var out, cur []string
	flush := func() {
		if len(cur) > 0 {
			out = append(out, strings.Join(cur, "\n"))
			cur = nil
		}
	}
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, "- "):
			flush()
			cur = append(cur, strings.TrimPrefix(line, "- "))
		default:
			cur = append(cur, line)
		}
	}
	flush()
	return out
}

// memoryTokens estimates ~4 bytes per token, plus a fixed cost per entry for its frame.
func memoryTokens(e MemoryEntry) int {
	return (len(e.Text)+len(strings.Join(e.Tags, " "))+3)/4 + 4
}

// Recall ranks stored and shared context entries against query and fits them under tokenCap.
func Recall(ctx context.Context, mem, ctxKV jetstream.KeyValue, query string, k, tokenCap int) (MemoryRecall, error) {
	stored, err := memoryEntries(ctx, mem)
	if err != nil {
		return MemoryRecall{}, err
	}
	derived, err := contextEntries(ctx, ctxKV)
	if err != nil {
		return MemoryRecall{}, err
	}
	all := append(stored, derived...)
	full := 0
	var pinned, rest []MemoryEntry
	for _, e := range all {
		full += memoryTokens(e)
		if e.Pinned {
			pinned = append(pinned, e)
		} else {
			rest = append(rest, e)
		}
	}
	slices.SortStableFunc(pinned, func(a, b MemoryEntry) int { return b.At.Compare(a.At) })
	ranked, err := memoryRank(rest, query, k)
	if err != nil {
		return MemoryRecall{}, err
	}
	selected := append(pinned, ranked...)
	out := []MemoryEntry{}
	tokens := 0
	for _, e := range selected {
		cost := memoryTokens(e)
		if tokens+cost > tokenCap {
			break
		}
		tokens += cost
		out = append(out, e)
	}
	return MemoryRecall{Entries: out, Tokens: tokens, FullTokens: full, Omitted: len(selected) - len(out)}, nil
}

// memoryRank returns the k best BM25 matches, recency-boosted. With no match it returns the k most recent entries.
func memoryRank(entries []MemoryEntry, query string, k int) ([]MemoryEntry, error) {
	type hit struct {
		e     MemoryEntry
		score float64
	}
	// shortcut: index rebuilt per recall (sub-ms for hundreds of entries); keep a persistent index in cos.db past ~5k entries.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE VIRTUAL TABLE m USING fts5(text, tags, tokenize='porter unicode61')`); err != nil {
		return nil, err
	}
	for i, e := range entries {
		if _, err := db.Exec(`INSERT INTO m(rowid, text, tags) VALUES (?, ?, ?)`, i+1, e.Text, strings.Join(e.Tags, " ")); err != nil {
			return nil, err
		}
	}
	var hits []hit
	if match := memoryMatch(query); match != "" {
		rows, err := db.Query(`SELECT rowid, bm25(m) FROM m WHERE m MATCH ?`, match)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		now := time.Now()
		for rows.Next() {
			var id int
			var bm float64
			if err := rows.Scan(&id, &bm); err != nil {
				return nil, err
			}
			e := entries[id-1]
			age := max(now.Sub(e.At), 0)
			boost := 1 + 0.5*math.Exp(-float64(age)/float64(memoryRecencyScale))
			hits = append(hits, hit{e, -bm * boost})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	if len(hits) == 0 {
		sorted := slices.Clone(entries)
		slices.SortStableFunc(sorted, func(a, b MemoryEntry) int { return b.At.Compare(a.At) })
		return sorted[:min(k, len(sorted))], nil
	}
	slices.SortFunc(hits, func(a, b hit) int {
		return cmp.Or(cmp.Compare(b.score, a.score), b.e.At.Compare(a.e.At))
	})
	out := []MemoryEntry{}
	for _, h := range hits[:min(k, len(hits))] {
		out = append(out, h.e)
	}
	return out, nil
}

// memoryMatch turns free text into a safe FTS5 query: every word quoted, prefix-matched and OR-joined.
func memoryMatch(query string) string {
	var terms []string
	for _, w := range memoryWord.FindAllString(strings.ToLower(query), -1) {
		if utf8.RuneCountInString(w) < 2 || memoryStopwords[w] {
			continue
		}
		terms = append(terms, `"`+w+`"*`)
	}
	return strings.Join(terms, " OR ")
}

// Text renders the recall as one bullet per entry for a brief.
func (r MemoryRecall) Text() string {
	var b strings.Builder
	for _, e := range r.Entries {
		fmt.Fprintf(&b, "- [%s] %s", e.Kind, e.Text)
		if len(e.Tags) > 0 {
			fmt.Fprintf(&b, " (#%s)", strings.Join(e.Tags, " #"))
		}
		b.WriteByte('\n')
	}
	return b.String()
}
