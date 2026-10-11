package cos

import (
	"cmp"
	"context"
	"errors"
	"io"
	"slices"
	"strconv"
	"time"
)

func init() {
	cliCommands["budget"] = cliCommand{Usage: "budget | budget thread <tokens> | budget daily <tokens>, e.g. 10M or 500k", Run: budgetCommand}
}

type budgetRow struct {
	Key     string `json:"key"`
	Label   string `json:"label"`
	Thread  string `json:"thread,omitempty"`
	Used    int64  `json:"used"`
	Limit   int64  `json:"limit"`
	Percent int64  `json:"percent"`
}

type budgetReport struct {
	Day         string           `json:"day"`
	ThreadLimit int64            `json:"threadLimit"`
	DailyLimit  int64            `json:"dailyLimit"`
	Raises      map[string]int64 `json:"raises"`
	DailyUsed   int64            `json:"dailyUsed"`
	Threads     []budgetRow      `json:"threads"`
}

func budgetCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	usage := errors.New("usage: cos budget | cos budget thread <tokens> | cos budget daily <tokens>")
	setting := ""
	var tokens int64
	switch {
	case len(args) == 0:
	case len(args) == 2 && args[0] == "thread":
		setting = budgetSettingsThread
	case len(args) == 2 && args[0] == "daily":
		setting = budgetSettingsDaily
	default:
		return usage
	}
	if setting != "" {
		var err error
		if tokens, err = parseTokens(args[1]); err != nil {
			return err
		}
	}
	s, err := stateStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return err
	}
	settings, err := js.KeyValue(ctx, autopilotSettings)
	if err != nil {
		return err
	}
	if setting != "" {
		if _, err := settings.Put(ctx, setting, []byte(strconv.FormatInt(tokens, 10))); err != nil {
			return err
		}
	}
	st, err := loadBudgets(ctx, s, settings, time.Now())
	if err != nil {
		return err
	}
	return output(w, st.report())
}

// report lists threads most used first, so the group nearest its limit is on top.
func (st budgetState) report() budgetReport {
	r := budgetReport{Day: st.Day, ThreadLimit: st.Limits.Thread, DailyLimit: st.Limits.limit(budgetDaily, st.Limits.Daily),
		Raises: st.Limits.Raises, DailyUsed: budgetUsed(st.Daily), Threads: []budgetRow{}}
	for _, t := range st.Totals {
		used := budgetUsed(t)
		limit := st.Limits.limit(budgetScope(t.Key), st.Limits.Thread)
		r.Threads = append(r.Threads, budgetRow{Key: t.Key, Label: t.Label, Thread: t.Thread, Used: used, Limit: limit, Percent: budgetPercent(used, limit)})
	}
	slices.SortFunc(r.Threads, func(a, b budgetRow) int { return cmp.Compare(b.Percent, a.Percent) })
	return r
}
