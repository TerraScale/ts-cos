package cos

import (
	"context"
	"errors"
	"io"
)

func init() {
	cliCommands["status"] = cliCommand{"status [--brief]", statusCommand}
}

// statusCommand prints the cos.mjs status; --brief refreshes the dashboard state and prints it in at most 40 lines.
func statusCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	if len(args) > 1 || (len(args) == 1 && args[0] != "--brief") {
		return errors.New("usage: cos status [--brief]")
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	if len(args) == 0 {
		v, err := Status(ctx, cfg, bus)
		if err != nil {
			return err
		}
		return output(w, v)
	}
	s, err := stateStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	old, err := s.Live(ctx)
	if err != nil {
		return err
	}
	if err := s.SaveLive(ctx, Refresh(ctx, cfg, bus, old)); err != nil {
		return err
	}
	v, err := View(ctx, s)
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, Brief(v))
	return err
}
