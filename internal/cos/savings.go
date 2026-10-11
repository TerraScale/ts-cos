package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const savingsBucket = "COS_SAVINGS"

// savingsRetries bounds CAS conflicts per recorded increment before the write gives up.
const savingsRetries = 50

// savingsMaxDays bounds the window that cos savings and SavingsReport can report.
const savingsMaxDays = 90

const (
	SavingDedup      = "dedup"       // events merged in a digest
	SavingDigestCap  = "digest-cap"  // events cut by a digest's size cap
	SavingRetrieval  = "retrieval"   // memory retrieval instead of a full memory dump
	SavingFreshBrief = "fresh-brief" // restart instead of carrying context
	SavingParallel   = "parallel"    // wall-clock saved by parallel Haiku workers; their token savings are derived from usage on the tokens page
)

var savingKinds = []string{SavingDedup, SavingDigestCap, SavingRetrieval, SavingFreshBrief, SavingParallel}

// SavingCounter is one day's total for one kind.
type SavingCounter struct {
	Events      int `json:"events"`
	Tokens      int `json:"tokens"`
	WallSeconds int `json:"wallSeconds"`
}

// SavingsDay is one UTC day.
type SavingsDay struct {
	Day         string                   `json:"day"`
	Kinds       map[string]SavingCounter `json:"kinds"`
	Avoided     int                      `json:"avoided"`
	WallSeconds int                      `json:"wallSeconds"`
}

func init() {
	cliCommands["savings"] = cliCommand{Usage: "savings [days]", Run: func(ctx context.Context, cfg Config, args []string, w io.Writer) error {
		bus, err := ConnectBus(ctx, cfg)
		if err != nil {
			return err
		}
		defer bus.Close()
		kv, err := openKV(ctx, bus, savingsBucket)
		if err != nil {
			return err
		}
		return savingsCommand(ctx, kv, args, w)
	}}
}

func savingsCommand(ctx context.Context, kv jetstream.KeyValue, args []string, w io.Writer) error {
	days := 7
	if len(args) > 1 {
		return errors.New("usage: cos savings [days]")
	}
	if len(args) == 1 {
		n, err := strconv.Atoi(args[0])
		if err != nil || n < 1 || n > savingsMaxDays {
			return fmt.Errorf("savings days must be 1 to %d", savingsMaxDays)
		}
		days = n
	}
	report, err := SavingsReport(ctx, kv, days, time.Now())
	if err != nil {
		return err
	}
	return output(w, map[string]any{"days": report})
}

// RecordSaving adds one event to today's counter for kind. Concurrent writers retry on CAS conflicts, never overwrite.
func RecordSaving(ctx context.Context, kv jetstream.KeyValue, kind string, tokens, wallSeconds int, now time.Time) error {
	if !slices.Contains(savingKinds, kind) {
		return fmt.Errorf("unknown saving kind %q", kind)
	}
	if tokens < 0 || wallSeconds < 0 {
		return errors.New("saving tokens and wall seconds must not be negative")
	}
	key := now.UTC().Format("2006-01-02") + "." + kind
	var lastErr error
	for attempt := range savingsRetries {
		if attempt > 0 {
			// Jittered backoff so contending writers stop colliding in lockstep.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(rand.N(time.Duration(attempt) * 5 * time.Millisecond)):
			}
		}
		var c SavingCounter
		e, err := kv.Get(ctx, key)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}
		if err == nil {
			if err := json.Unmarshal(e.Value(), &c); err != nil {
				return err
			}
		}
		c.Events++
		c.Tokens += tokens
		c.WallSeconds += wallSeconds
		data, err := json.Marshal(c)
		if err != nil {
			return err
		}
		if e == nil {
			_, err = kv.Create(ctx, key, data)
		} else {
			_, err = kv.Update(ctx, key, data, e.Revision())
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("saving counter busy after %d tries; increment not recorded: %w", savingsRetries, lastErr)
}

// SavingsReport returns the last days UTC days, newest first.
func SavingsReport(ctx context.Context, kv jetstream.KeyValue, days int, now time.Time) ([]SavingsDay, error) {
	if days < 1 || days > savingsMaxDays {
		return nil, fmt.Errorf("savings days must be 1 to %d", savingsMaxDays)
	}
	out := make([]SavingsDay, 0, days)
	index := make(map[string]int, days)
	for i := range days {
		day := now.UTC().AddDate(0, 0, -i).Format("2006-01-02")
		index[day] = i
		out = append(out, SavingsDay{Day: day, Kinds: map[string]SavingCounter{}})
	}
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	defer lister.Stop()
	// ListKeys can repeat a key while writers are active, so collect into a set before reading.
	wanted := map[string]bool{}
	for key := range lister.Keys() {
		day, kind, _ := strings.Cut(key, ".")
		if _, ok := index[day]; ok && slices.Contains(savingKinds, kind) {
			wanted[key] = true
		}
	}
	// The key channel closes silently when ctx ends, so a short listing must fail here.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for key := range wanted {
		e, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var c SavingCounter
		if err := json.Unmarshal(e.Value(), &c); err != nil {
			return nil, err
		}
		day, kind, _ := strings.Cut(key, ".")
		d := &out[index[day]]
		d.Kinds[kind] = c
		d.WallSeconds += c.WallSeconds
		d.Avoided += c.Tokens
	}
	return out, nil
}
