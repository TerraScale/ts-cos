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
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// A token budget caps one day's tokens per worker thread and across the fleet. Input,
// cache writes and output count; cache reads are left out because they run about 20x
// the volume at 5% of the price. Cost is still shown next to every budget.
const (
	budgetThreadDefault  = 10_000_000
	budgetDailyDefault   = 100_000_000
	budgetSettingsThread = "budget.thread"
	budgetSettingsDaily  = "budget.daily"
	budgetWarnPercent    = 80
	budgetDaily          = "daily"
	budgetCOSLabel       = "COS agent"
	budgetLabelMax       = 80
	// shortcut: the daily event lists at most 50 threads to fit its 4 KB data limit; upgrade by reading usage in Handle.
	budgetPauseMax = 50
)

const (
	budgetResume    = "Resume with +50% today"
	budgetRaise2x   = "Raise to 2x today"
	budgetKeep      = "Keep paused"
	budgetResumeAll = "Resume all with +25% today"
	budgetRaiseAll  = "Raise today's budget to 2x"
)

var (
	budgetThreadOptions = []string{budgetResume, budgetRaise2x, budgetKeep}
	budgetFleetOptions  = []string{budgetResumeAll, budgetRaiseAll, budgetKeep}
)

// budgetData is the payload of budget.warn and budget.exceeded.
type budgetData struct {
	Scope   string         `json:"scope"`
	Day     string         `json:"day"`
	Used    int64          `json:"used"`
	Limit   int64          `json:"limit"`
	Percent int64          `json:"percent"`
	Cost    float64        `json:"cost"`
	Label   string         `json:"label,omitempty"`
	Threads []budgetThread `json:"threads,omitempty"`
}

type budgetThread struct {
	Host   string `json:"host"`
	Thread string `json:"thread"`
}

// budgetLimits are the base limits from settings plus today's raises, by scope.
type budgetLimits struct {
	Thread, Daily int64
	Raises        map[string]int64
}

func (l budgetLimits) limit(scope string, base int64) int64 {
	return max(base, l.Raises[scope])
}

// budgetState is today's usage per thread and fleet-wide, with the limits in force.
type budgetState struct {
	Day    string
	Totals []UsageTotal
	Daily  UsageTotal
	Limits budgetLimits
}

func budgetUsed(t UsageTotal) int64 { return t.Input + t.CacheWrite + t.Output }

func budgetPercent(used, limit int64) int64 {
	if limit <= 0 {
		return 0
	}
	return used * 100 / limit
}

// budgetMark is the highest mark used has reached: 100, 80, or 0 below both.
func budgetMark(used, limit int64) int64 {
	switch pct := budgetPercent(used, limit); {
	case pct >= 100:
		return 100
	case pct >= budgetWarnPercent:
		return budgetWarnPercent
	}
	return 0
}

// budgetScope names a thread group in KV and Inbox keys without its host or project.
func budgetScope(group string) string {
	sum := sha256.Sum256([]byte(group))
	return "t-" + hex.EncodeToString(sum[:8])
}

// budgetPausable is a worker thread that a live fleet thread owns. COS never pauses.
func budgetPausable(t UsageTotal) bool { return t.Thread != "" && t.Label != budgetCOSLabel }

func budgetRaiseKey(day, scope string) string {
	return "budget.raise." + strings.ReplaceAll(day, "-", "") + "." + scope
}

// budgetHuman formats a token count for a summary: 8.1M, 500k, 42.
func budgetHuman(n int64) string {
	v, unit := float64(n), ""
	switch {
	case n >= 1_000_000:
		v, unit = v/1e6, "M"
	case n >= 1_000:
		v, unit = v/1e3, "k"
	}
	return strings.TrimSuffix(strconv.FormatFloat(v, 'f', 1, 64), ".0") + unit
}

// budgetEvent is the warn or exceeded event for one scope, or false below 80%.
func budgetEvent(d budgetData, host, thread, ticket string) (Event, bool) {
	mark := budgetMark(d.Used, d.Limit)
	if mark == 0 {
		return Event{}, false
	}
	d.Percent = budgetPercent(d.Used, d.Limit)
	d.Label = EventText(d.Label, budgetLabelMax)
	data, _ := json.Marshal(d)
	e := Event{ID: NewEventID(SourceBudget, d.Day, d.Scope, strconv.FormatInt(mark, 10), strconv.FormatInt(d.Limit, 10)),
		Source: SourceBudget, Kind: KindBudgetWarn, Time: time.Now().UTC(), Severity: SeverityWarn,
		Summary: EventText(fmt.Sprintf("%s used %s of %s tokens today (%d%%, $%.2f)", d.Label, budgetHuman(d.Used), budgetHuman(d.Limit), d.Percent, d.Cost), eventSummaryMax),
		Host:    host, Ticket: ticket, Thread: thread, Data: data}
	if mark == 100 {
		e.Kind, e.Severity = KindBudgetExceeded, SeverityUrgent
	}
	return e, true
}

// budgetCrossings is pure: today's totals and limits decide every warn and exceeded
// event. Repeats are harmless because each event ID carries its day, scope, mark and limit.
func budgetCrossings(totals []UsageTotal, daily UsageTotal, limits budgetLimits, day string) []Event {
	var threads []budgetThread
	for _, t := range totals {
		if budgetPausable(t) && len(threads) < budgetPauseMax {
			threads = append(threads, budgetThread{Host: t.Host, Thread: t.Thread})
		}
	}
	var out []Event
	fleet := budgetData{Scope: budgetDaily, Day: day, Used: budgetUsed(daily), Limit: limits.limit(budgetDaily, limits.Daily),
		Cost: daily.Cost, Label: "All threads", Threads: threads}
	if e, ok := budgetEvent(fleet, "", "", ""); ok {
		out = append(out, e)
	}
	for _, t := range totals {
		thread := t.Thread
		if t.Label == budgetCOSLabel {
			thread = ""
		}
		scope := budgetScope(t.Key)
		d := budgetData{Scope: scope, Day: day, Used: budgetUsed(t), Limit: limits.limit(scope, limits.Thread),
			Cost: t.Cost, Label: cmp.Or(t.Label, scope)}
		if e, ok := budgetEvent(d, t.Host, thread, t.Ticket); ok {
			out = append(out, e)
		}
	}
	return out
}

// budgetDecisions acts on one budget event. The thread pause and fleet pause both carry
// a question for the user; COS and unresolved groups only get an FYI.
func budgetDecisions(e Event, live Live, owner *FleetThread) []Decision {
	var b budgetData
	if err := json.Unmarshal(e.Data, &b); err != nil {
		return []Decision{{Class: ClassRecord, Rule: "budget", Reason: "budget data unreadable: " + err.Error()}}
	}
	thread := ""
	if owner != nil {
		thread = owner.ThreadID
	}
	inbox := func(kind string, options []string, key string) *InboxInput {
		return &InboxInput{Kind: kind, Title: e.Summary, Options: options, Source: string(SourceBudget), Key: key,
			Ticket: e.Ticket, Host: e.Host, Thread: thread}
	}
	if e.Kind == KindBudgetWarn {
		return []Decision{{Class: ClassRecord, Rule: "budget-warn", Reason: e.Summary, Host: e.Host, Thread: thread, Ticket: e.Ticket,
			Inbox: inbox("fyi", nil, fmt.Sprintf("budget:%s:%s:%d", b.Day, b.Scope, budgetWarnPercent))}}
	}
	key := fmt.Sprintf("budget:%s:%s:100:%d", b.Day, b.Scope, b.Limit)
	switch {
	case b.Scope == budgetDaily:
		return budgetPauseFleet(e, b, live, key)
	case owner == nil || b.Label == budgetCOSLabel:
		return []Decision{{Class: ClassRecord, Rule: "budget-exceeded", Reason: e.Summary, Host: e.Host, Ticket: e.Ticket,
			Inbox: inbox("fyi", nil, key)}}
	}
	wake, err := CurrentBehavior().Render("wake_budget", wakeData{Summary: e.Summary})
	if err != nil {
		return []Decision{{Class: ClassRecord, Rule: "budget-pause", Reason: "template failed: " + err.Error()}}
	}
	return []Decision{
		{Class: ClassAct, Rule: "budget-pause", Reason: e.Summary, Host: e.Host, Thread: thread, Ticket: e.Ticket, Wake: wake},
		{Class: ClassEscalate, Rule: "budget-pause", Reason: e.Summary, Host: e.Host, Thread: thread, Ticket: e.Ticket,
			Inbox: inbox("question", budgetThreadOptions, key)},
	}
}

// budgetPauseFleet wakes every live thread the fleet budget named, then files one question.
func budgetPauseFleet(e Event, b budgetData, live Live, key string) []Decision {
	wake, err := CurrentBehavior().Render("wake_budget", wakeData{Summary: e.Summary})
	if err != nil {
		return []Decision{{Class: ClassRecord, Rule: "budget-pause", Reason: "template failed: " + err.Error()}}
	}
	out := []Decision{}
	for _, t := range b.Threads {
		if th := findThread(live, t.Host, t.Thread); th != nil {
			out = append(out, Decision{Class: ClassAct, Rule: "budget-pause", Reason: e.Summary, Host: t.Host, Thread: th.ThreadID, Wake: wake})
		}
	}
	return append(out, Decision{Class: ClassEscalate, Rule: "budget-pause", Reason: e.Summary,
		Inbox: &InboxInput{Kind: "question", Title: e.Summary, Options: budgetFleetOptions, Source: string(SourceBudget), Key: key}})
}

// budgetRaise maps an answered budget item to the raised limit its option grants for that
// day. Keep paused and free text grant nothing.
func budgetRaise(item InboxItem) (key string, value int64, ok bool) {
	parts := strings.Split(item.Key, ":")
	if item.Source != string(SourceBudget) || item.Answer == nil || len(parts) != 5 {
		return "", 0, false
	}
	limit, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return "", 0, false
	}
	switch item.Answer.Option {
	case budgetResume:
		value = limit + limit/2
	case budgetRaise2x, budgetRaiseAll:
		value = 2 * limit
	case budgetResumeAll:
		value = limit + limit/4
	default:
		return "", 0, false
	}
	return budgetRaiseKey(parts[1], parts[2]), value, true
}

// budgetGet reads one integer setting; ok is false when the key is absent.
func budgetGet(ctx context.Context, kv jetstream.KeyValue, key string) (n int64, ok bool, err error) {
	entry, err := kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	n, err = strconv.ParseInt(string(entry.Value()), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("budget %s: %w", key, err)
	}
	return n, true, nil
}

// budgetLoadLimits reads the base limits and the day's raises. A non-positive base is
// ignored so a bad setting never makes every percentage divide by zero.
func budgetLoadLimits(ctx context.Context, kv jetstream.KeyValue, day string, scopes []string) (budgetLimits, error) {
	l := budgetLimits{Thread: budgetThreadDefault, Daily: budgetDailyDefault, Raises: map[string]int64{}}
	for key, base := range map[string]*int64{budgetSettingsThread: &l.Thread, budgetSettingsDaily: &l.Daily} {
		n, ok, err := budgetGet(ctx, kv, key)
		if err != nil {
			return l, err
		}
		if ok && n > 0 {
			*base = n
		}
	}
	for _, scope := range scopes {
		n, ok, err := budgetGet(ctx, kv, budgetRaiseKey(day, scope))
		if err != nil {
			return l, err
		}
		if ok {
			l.Raises[scope] = n
		}
	}
	return l, nil
}

// budgetStoreRaise keeps the larger of the stored and new raise; false means nothing changed.
func budgetStoreRaise(ctx context.Context, kv jetstream.KeyValue, key string, value int64) (bool, error) {
	cur, ok, err := budgetGet(ctx, kv, key)
	if err != nil || (ok && cur >= value) {
		return false, err
	}
	_, err = kv.Put(ctx, key, []byte(strconv.FormatInt(value, 10)))
	return err == nil, err
}

// loadBudgets reads today's usage from the local mirror and the settings from KV.
func loadBudgets(ctx context.Context, store *Store, settings jetstream.KeyValue, now time.Time) (budgetState, error) {
	day := now.UTC().Format(time.DateOnly)
	rows, err := store.Usage(ctx, day) // on or after today; no rows are dated later
	if err != nil {
		return budgetState{}, err
	}
	live, err := store.Live(ctx)
	if err != nil {
		return budgetState{}, err
	}
	st := budgetState{Day: day, Totals: SummarizeUsage(rows, live, "thread"), Daily: usageTotal(budgetDaily, rows)}
	scopes := []string{budgetDaily}
	for _, t := range st.Totals {
		scopes = append(scopes, budgetScope(t.Key))
	}
	st.Limits, err = budgetLoadLimits(ctx, settings, day, scopes)
	return st, err
}

// runBudgets is leader-only: it publishes today's budget events and applies answers.
func runBudgets(ctx context.Context, bus *Bus, store *Store) {
	tick := time.NewTicker(usageScanEvery)
	defer tick.Stop()
	for {
		if err := budgetSweep(ctx, bus, store); err != nil && ctx.Err() == nil {
			slog.Error("token budgets", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func budgetSweep(ctx context.Context, bus *Bus, store *Store) error {
	js, err := bus.JetStream()
	if err != nil {
		return err
	}
	settings, err := js.KeyValue(ctx, autopilotSettings)
	if err != nil {
		return err
	}
	st, err := loadBudgets(ctx, store, settings, time.Now())
	if err != nil {
		return err
	}
	for _, e := range budgetCrossings(st.Totals, st.Daily, st.Limits, st.Day) {
		if _, err := PublishEvent(ctx, js, e); err != nil {
			return err
		}
	}
	_, history, err := store.Inbox(ctx)
	if err != nil {
		return err
	}
	return budgetApply(ctx, bus, settings, st, history)
}

// budgetApply stores each answered raise once. A fleet answer that raises the limit also
// resumes every paused thread; a repeat stores nothing, so nothing is resumed twice.
func budgetApply(ctx context.Context, bus *Bus, settings jetstream.KeyValue, st budgetState, history []InboxItem) error {
	for _, item := range history {
		key, value, ok := budgetRaise(item)
		if !ok || !strings.HasPrefix(key, budgetRaiseKey(st.Day, "")) {
			continue
		}
		raised, err := budgetStoreRaise(ctx, settings, key, value)
		if err != nil {
			return err
		}
		if !raised || item.Thread != "" {
			continue
		}
		text := fmt.Sprintf("RESUMED: budget raised to %d tokens today. Continue where you stopped.", value)
		for _, t := range st.Totals {
			if !budgetPausable(t) {
				continue
			}
			if err := bus.Call(ctx, t.Host, "send", []string{t.Thread, text}, nil); err != nil {
				slog.Error("resume paused thread", "host", t.Host, "thread", t.Thread, "error", err)
			}
		}
	}
	return nil
}

// parseTokens reads a positive token count with an optional k or M suffix: 10M, 500k, 42.
func parseTokens(s string) (int64, error) {
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "M"):
		mult, s = 1_000_000, strings.TrimSuffix(s, "M")
	case strings.HasSuffix(s, "k"):
		mult, s = 1_000, strings.TrimSuffix(s, "k")
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n > (1<<62)/mult {
		return 0, OperationError("budget must be a positive token count such as 10M or 500k")
	}
	return n * mult, nil
}
