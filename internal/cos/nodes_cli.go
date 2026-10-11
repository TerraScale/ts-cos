package cos

import (
	"context"
	"errors"
	"io"
	"time"
)

// nodeMissing is a known host with no readable sample, so the list still names every node.
type nodeMissing struct {
	Host  string `json:"host"`
	Error string `json:"error"`
}

var nodeWindows = map[string]time.Duration{"1h": time.Hour, "24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour}

func init() {
	cliCommands["nodes"] = cliCommand{Usage: "nodes [<host> [1h|24h|7d]]", Run: nodesCommand}
}

func nodesCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	usage := errors.New("usage: cos nodes [<host> [1h|24h|7d]]")
	window := time.Hour
	if len(args) > 2 {
		return usage
	}
	if len(args) == 2 {
		d, ok := nodeWindows[args[1]]
		if !ok {
			return usage
		}
		window = d
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return err
	}
	if len(args) > 0 {
		series, err := NodeSeries(ctx, js, args[0], window)
		if err != nil {
			return err
		}
		return output(w, series)
	}
	hosts, err := bus.Hosts(ctx)
	if err != nil {
		return err
	}
	rows := make([]any, 0, len(hosts))
	for _, h := range hosts {
		s, err := NodeLatest(ctx, js, h)
		if err != nil {
			rows = append(rows, nodeMissing{Host: h, Error: err.Error()})
			continue
		}
		rows = append(rows, s)
	}
	return output(w, rows)
}
