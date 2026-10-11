package cos

import (
	"context"
	"errors"
	"io"
)

func init() {
	cliCommands["behavior"] = cliCommand{Usage: "behavior", Run: func(ctx context.Context, cfg Config, args []string, w io.Writer) error {
		if len(args) != 0 {
			return errors.New("usage: cos behavior")
		}
		bus, err := ConnectBus(ctx, cfg)
		if err != nil {
			return err
		}
		defer bus.Close()
		v, err := BehaviorStatus(ctx, bus)
		if err != nil {
			return err
		}
		return output(w, v)
	}}
}
