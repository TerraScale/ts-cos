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
	// Floors for the defaults; the defaults grow past them from the last 7 days of use.
	budgetThreadFloor    = 10_000_000
	budgetDailyFloor     = 100_000_000
	budgetHistoryDays    = 7
	budgetSettingsThread = "budget.thread"
	budgetSettingsDaily  = "budget.daily"
	budgetWarnPercent    = 80
	budgetMaxTokens      = 1_000_000_000_000
	budgetDaily          = "daily"
	budgetCOSLabel       = "COS agent"
	budgetLabelMax       = 40
	// shortcut: the daily event lists at most 50 threads to fit its 4 KB data limit; upgrade by reading usage in Handle.
	budgetPauseMax = 50
)

const budgetKeep = "Keep paused until 00:00 UTC"

// budgetThreadOptionsFor and budgetFleetOptionsFor are the answers a pause question offers,
// worked out from today's use when it is filed. budgetRaise matches them by their prefix.
func budgetThreadOptionsFor(used int64) []string {
	return []string{"Resume: allow " + budgetHuman(used+used/2) + " tokens today",
		"Raise to " + budgetHuman(2*used) + " tokens today", budgetKeep}
}

func budgetFleetOptionsFor(used int64) []string {
	return []string{"Resume all: allow " + budgetHuman(used+used/4) + " tokens today",
		"Raise today's limit to " + budgetHuman(2*used) + " tokens", budgetKeep}
}

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

// budgetLimits are the base limits from settings plus today's raises, by scope. A base that
// no setting holds is a default from the last 7 days, and its flag says so.
type budgetLimits struct {
	Thread, Daily               int64
	ThreadDefault, DailyDefault bool
	Raises                      map[string]int64
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

// budgetAppliedKey marks an answered budget item whose raise is stored. Inbox IDs are hex.
func budgetAppliedKey(id string) string { return "budget.applied." + id }

// budgetExceededKey is the Inbox key of a scope's exceeded item; budgetRaise reads its day, scope and limit back.
func budgetExceededKey(day, scope string, limit int64) string {
	return fmt.Sprintf("budget:%s:%s:100:%d", day, scope, limit)
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
		Summary: EventText(fmt.Sprintf("%s used %s of %s tokens today (%d%%). Tokens count against your Claude subscription limits; $%.2f is the API list-price equivalent, not a bill.",
			d.Label, budgetHuman(d.Used), budgetHuman(d.Limit), d.Percent, d.Cost), eventSummaryMax),
		Host: host, Ticket: ticket, Thread: thread, Data: data}
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
// a question for the user; COS and events with no thread only get an FYI. The sweep
// resolves the thread, so a live snapshot that lags behind a new thread does not block the pause.
func budgetDecisions(e Event, live Live, owner *FleetThread) []Decision {
	var b budgetData
	if err := json.Unmarshal(e.Data, &b); err != nil {
		return []Decision{{Class: ClassRecord, Rule: "budget", Reason: "budget data unreadable: " + err.Error()}}
	}
	thread := e.Thread
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
	key := budgetExceededKey(b.Day, b.Scope, b.Limit)
	switch {
	case b.Scope == budgetDaily:
		return budgetPauseFleet(e, b, live, key)
	case thread == "" || e.Host == "" || b.Label == budgetCOSLabel:
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
			Inbox: inbox("question", budgetThreadOptionsFor(b.Used), key)},
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
		Inbox: &InboxInput{Kind: "question", Title: e.Summary, Options: budgetFleetOptionsFor(b.Used), Source: string(SourceBudget), Key: key}})
}

// budgetItemScope is the scope a budget item key names, or "" when the key is not one.
func budgetItemScope(item InboxItem) string {
	parts := strings.Split(item.Key, ":")
	if len(parts) != 5 {
		return ""
	}
	return parts[2]
}

// budgetUsedFor is today's use for one scope; a scope with no rows today has used nothing.
func budgetUsedFor(st budgetState, scope string) int64 {
	if scope == budgetDaily {
		return budgetUsed(st.Daily)
	}
	for _, t := range st.Totals {
		if budgetScope(t.Key) == scope {
			return budgetUsed(t)
		}
	}
	return 0
}

// budgetRaise maps an answered budget item to the raised limit its option grants for that
// day. The raise starts from the larger of the crossed limit and used, so it always clears
// today's use. Keep paused and free text grant nothing.
func budgetRaise(item InboxItem, used int64) (key string, value int64, ok bool) {
	parts := strings.Split(item.Key, ":")
	if item.Source != string(SourceBudget) || item.Answer == nil || len(parts) != 5 {
		return "", 0, false
	}
	limit, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return "", 0, false
	}
	grow, ok := budgetGrowth(item.Answer.Option)
	if !ok {
		return "", 0, false
	}
	return budgetRaiseKey(parts[1], parts[2]), grow(max(limit, used)), true
}

// budgetGrowth maps an answer to its factor by prefix. The first two cases of each pair are
// the labels filed now; the rest are the labels filed before the limits were worked out from
// today's use, which items still carry. Keep paused matches none.
func budgetGrowth(option string) (func(int64) int64, bool) {
	switch {
	case strings.HasPrefix(option, "Resume all:"), strings.HasPrefix(option, "Resume all with +25%"):
		return func(n int64) int64 { return n + n/4 }, true
	case strings.HasPrefix(option, "Resume:"), strings.HasPrefix(option, "Resume with +50%"):
		return func(n int64) int64 { return n + n/2 }, true
	case strings.HasPrefix(option, "Raise to "), strings.HasPrefix(option, "Raise today's "):
		return func(n int64) int64 { return 2 * n }, true
	}
	return nil, false
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

// budgetSetLimit stores one base limit; the CLI and the Settings form both write through it.
func budgetSetLimit(ctx context.Context, kv jetstream.KeyValue, setting string, tokens int64) error {
	_, err := kv.Put(ctx, setting, []byte(strconv.FormatInt(tokens, 10)))
	return err
}

// budgetLoadLimits reads the base limits and the day's raises. def supplies each base that no
// setting holds. A non-positive base is ignored so a bad setting never makes every percentage
// divide by zero.
func budgetLoadLimits(ctx context.Context, kv jetstream.KeyValue, day string, scopes []string, def budgetLimits) (budgetLimits, error) {
	l := budgetLimits{Thread: def.Thread, Daily: def.Daily, ThreadDefault: true, DailyDefault: true, Raises: map[string]int64{}}
	thread, ok, err := budgetGet(ctx, kv, budgetSettingsThread)
	if err != nil {
		return l, err
	}
	if ok && thread > 0 {
		l.Thread, l.ThreadDefault = thread, false
	}
	daily, ok, err := budgetGet(ctx, kv, budgetSettingsDaily)
	if err != nil {
		return l, err
	}
	if ok && daily > 0 {
		l.Daily, l.DailyDefault = daily, false
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
	st, scopes, err := budgetUsage(ctx, store, now)
	if err != nil {
		return st, err
	}
	st.Limits, err = budgetLoadLimits(ctx, settings, st.Day, scopes, st.Limits)
	return st, err
}

// budgetHistoryStart is the first of the budgetHistoryDays complete days before today.
func budgetHistoryStart(today string) string {
	t, err := time.Parse(time.DateOnly, today)
	if err != nil {
		return today
	}
	return t.AddDate(0, 0, -budgetHistoryDays).Format(time.DateOnly)
}

// budgetDefaults sizes the base limits from the complete days before today, so a runaway
// today cannot raise its own limit. Each default is twice the peak and never below its floor.
func budgetDefaults(rows []UsageRow, today string) (thread, daily int64) {
	start := budgetHistoryStart(today)
	byDay := map[string][]UsageRow{}
	byThread := map[[3]string][]UsageRow{}
	for _, r := range rows {
		if r.Day < start || r.Day >= today {
			continue
		}
		byDay[r.Day] = append(byDay[r.Day], r)
		k := [3]string{r.Day, r.Host, r.Project}
		byThread[k] = append(byThread[k], r)
	}
	var peakDay, peakThread int64
	for _, rs := range byDay {
		peakDay = max(peakDay, budgetUsed(usageTotal(budgetDaily, rs)))
	}
	for _, rs := range byThread {
		peakThread = max(peakThread, budgetUsed(usageTotal("thread", rs)))
	}
	return max(budgetThreadFloor, budgetRoundUp(2*peakThread, 5_000_000)),
		max(budgetDailyFloor, budgetRoundUp(2*peakDay, 10_000_000))
}

func budgetRoundUp(n, step int64) int64 { return (n + step - 1) / step * step }

// budgetUsage reads today's totals, the scopes they name, and the defaults from the history
// window; the setting-backed limits are left for budgetLoadLimits.
func budgetUsage(ctx context.Context, store *Store, now time.Time) (budgetState, []string, error) {
	day := now.UTC().Format(time.DateOnly)
	rows, err := store.Usage(ctx, budgetHistoryStart(day)) // history and today; no rows are dated later
	if err != nil {
		return budgetState{}, nil, err
	}
	var today []UsageRow
	for _, r := range rows {
		if r.Day >= day {
			today = append(today, r)
		}
	}
	live, err := store.Live(ctx)
	if err != nil {
		return budgetState{}, nil, err
	}
	st := budgetState{Day: day, Totals: SummarizeUsage(today, live, "thread"), Daily: usageTotal(budgetDaily, today)}
	st.Limits = budgetLimits{ThreadDefault: true, DailyDefault: true}
	st.Limits.Thread, st.Limits.Daily = budgetDefaults(rows, day)
	scopes := []string{budgetDaily}
	for _, t := range st.Totals {
		scopes = append(scopes, budgetScope(t.Key))
	}
	return st, scopes, nil
}

// runBudgets is leader-only: it publishes today's budget events and applies answers.
func runBudgets(ctx context.Context, bus *Bus, store *Store) {
	failures := 0
	for {
		wait := usageScanEvery
		if err := budgetSweep(ctx, bus, store); err == nil {
			failures = 0
		} else {
			failures, wait = budgetSweepFailed(ctx, err, failures)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// budgetRetry starts the wait after a transient sweep failure; it doubles to budgetRetryMax.
const (
	budgetRetry      = 5 * time.Second
	budgetRetryMax   = 60 * time.Second
	budgetErrorAfter = 5
)

// budgetBackoff is the wait after the nth consecutive transient failure and how loudly to log it.
func budgetBackoff(failures int) (time.Duration, slog.Level) {
	wait := min(budgetRetry<<min(max(failures-1, 0), 4), budgetRetryMax)
	if failures < budgetErrorAfter {
		return wait, slog.LevelWarn
	}
	return wait, slog.LevelError
}

// budgetTransient is a sweep failure that clears on its own: NATS is not answering yet, or the
// settings bucket is not there yet right after a restart.
func budgetTransient(err error) bool {
	return transientUsageErr(err) || errors.Is(err, jetstream.ErrBucketNotFound)
}

// budgetSweepFailed logs a failed sweep and returns the consecutive transient failure count and
// the wait before the next sweep. A shutdown logs nothing, and a non-transient error is logged at once.
func budgetSweepFailed(ctx context.Context, err error, failures int) (int, time.Duration) {
	switch {
	case ctx.Err() != nil:
		return failures, 0
	case !budgetTransient(err):
		logErr(ctx, slog.LevelError, "token budgets", err)
		return 0, usageScanEvery
	}
	failures++
	wait, level := budgetBackoff(failures)
	slog.Log(ctx, level, "token budgets not ready; retrying", "error", err, "in", wait, "failures", failures)
	return failures, wait
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
		key, value, ok := budgetRaise(item, budgetUsedFor(st, budgetItemScope(item)))
		if !ok || !strings.HasPrefix(key, budgetRaiseKey(st.Day, "")) {
			continue
		}
		// Each answer applies once: the raise follows today's use, so re-applying it every sweep
		// would keep raising the limit and re-send RESUMED to every thread.
		applied := budgetAppliedKey(item.ID)
		if _, err := settings.Get(ctx, applied); err == nil {
			continue
		} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}
		raised, err := budgetStoreRaise(ctx, settings, key, value)
		if err != nil {
			return err
		}
		if _, err := settings.Put(ctx, applied, []byte(st.Day)); err != nil {
			return err
		}
		// Only the fleet raise resumes every thread; a thread-less scope raise must not message live work.
		if !raised || item.Thread != "" || key != budgetRaiseKey(st.Day, budgetDaily) {
			continue
		}
		text := fmt.Sprintf("RESUMED: budget raised to %d tokens today. Continue where you stopped.", value)
		for _, t := range st.Totals {
			if !budgetPausable(t) {
				continue
			}
			if err := bus.Call(ctx, t.Host, "send", []string{t.Thread, text}, nil); err != nil {
				logErr(ctx, slog.LevelError, "resume paused thread", err, "host", t.Host, "thread", t.Thread)
			}
		}
	}
	return nil
}

// parseTokens reads a positive token count with an optional k or M suffix: 10M, 500k, 42.
// Anything above budgetMaxTokens is rejected, which also keeps n*mult from overflowing.
func parseTokens(s string) (int64, error) {
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "M"):
		mult, s = 1_000_000, strings.TrimSuffix(s, "M")
	case strings.HasSuffix(s, "k"):
		mult, s = 1_000, strings.TrimSuffix(s, "k")
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n > budgetMaxTokens/mult {
		return 0, OperationError("budget must be a positive token count such as 10M or 500k, up to 1000000M")
	}
	return n * mult, nil
}
