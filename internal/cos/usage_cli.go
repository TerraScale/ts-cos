package cos

import (
	"context"
	"errors"
	"io"
	"slices"
	"time"
)

func init() {
	cliCommands["usage"] = cliCommand{Usage: "usage [today|7d|30d] [thread|ticket|day|model|kind]", Run: usageCommand}
}

func usageCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	usage := errors.New("usage: cos usage [today|7d|30d] [thread|ticket|day|model|kind]")
	window, by := "today", "thread"
	if len(args) > 2 {
		return usage
	}
	if len(args) > 0 {
		window = args[0]
	}
	if len(args) > 1 {
		by = args[1]
	}
	back, ok := tokenWindows[window]
	if !ok || !slices.Contains([]string{"thread", "ticket", "day", "model", "kind"}, by) {
		return usage
	}
	s, err := stateStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	since := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -back).Format("2006-01-02")
	rows, err := s.Usage(ctx, since)
	if err != nil {
		return err
	}
	live, err := s.Live(ctx)
	if err != nil {
		return err
	}
	return output(w, struct {
		Window string       `json:"window"`
		By     string       `json:"by"`
		Total  UsageTotal   `json:"total"`
		Rows   []UsageTotal `json:"rows"`
	}{window, by, usageTotal("total", rows), SummarizeUsage(rows, live, by)})
}
