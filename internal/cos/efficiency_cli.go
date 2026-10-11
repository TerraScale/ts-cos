package cos

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"
)

func init() {
	cliCommands["efficiency"] = cliCommand{Usage: "efficiency [today|7d|30d]", Run: efficiencyCommand}
}

func efficiencyCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	usage := errors.New("usage: cos efficiency [today|7d|30d]")
	window := "today"
	if len(args) > 1 {
		return usage
	}
	if len(args) == 1 {
		window = args[0]
	}
	back, ok := tokenWindows[window]
	if !ok {
		return usage
	}
	s, err := stateStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	now := time.Now().UTC()
	today := now.Truncate(24 * time.Hour)
	start := today.AddDate(0, 0, -back)
	rows, err := s.Usage(ctx, start.Format("2006-01-02"))
	if err != nil {
		return err
	}
	allRows, err := s.Usage(ctx, today.AddDate(0, 0, -(usageKeepDays-1)).Format("2006-01-02"))
	if err != nil {
		return err
	}
	merged, err := s.EventsOfKind(ctx, KindPRMerged, start)
	if err != nil {
		return err
	}
	return output(w, EfficiencyOf(window, rows, allRows, merged, efficiencySaved(ctx, cfg, back+1, now)))
}

// efficiencySaved sums the savings ledger over days. The ledger lives on the bus, so
// when the bus or bucket is unreachable the view still prints, with Saved at 0.
func efficiencySaved(ctx context.Context, cfg Config, days int, now time.Time) int64 {
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		slog.Warn("efficiency: savings unavailable", "err", err)
		return 0
	}
	defer bus.Close()
	kv, err := openKV(ctx, bus, savingsBucket)
	if err != nil {
		slog.Warn("efficiency: savings unavailable", "err", err)
		return 0
	}
	report, err := SavingsReport(ctx, kv, days, now)
	if err != nil {
		slog.Warn("efficiency: savings unavailable", "err", err)
		return 0
	}
	var saved int64
	for _, d := range report {
		saved += int64(d.Avoided)
	}
	return saved
}
