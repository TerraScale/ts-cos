package cos

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
)

func init() {
	cliCommands["stream"] = cliCommand{"stream <host> [<threadId>] (live JSON events, one per line)", streamCommand}
	serveWorkers = append(serveWorkers, func(ctx context.Context, cfg Config, bus *Bus) {
		if err := bus.ServeT3Streams(cfg.LocalHost, func(ctx context.Context) (*T3, error) { return OpenT3(ctx, cfg, cfg.LocalHost) }); err != nil {
			slog.Error("T3 stream relay not started", "error", err)
		}
	})
}

// streamCommand prints a host's live thread-list events, or one thread's, until interrupted.
func streamCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	if len(args) < 1 || len(args) > 2 || !validHost(args[0]) {
		return errors.New("usage: cos stream <host> [<threadId>]")
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	enc := json.NewEncoder(w)
	if len(args) == 1 {
		return bus.StreamShell(ctx, args[0], func(e ShellEvent) error { return enc.Encode(e) })
	}
	return bus.StreamThread(ctx, args[0], args[1], func(e ThreadEvent) error { return enc.Encode(e) })
}
