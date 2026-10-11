package cos

import (
	"context"
	"errors"
	"io"
)

func init() {
	cliCommands["e2e"] = cliCommand{Usage: "e2e [--smoke] [check-id-prefix...]", Run: E2E}
	cliCommands["version"] = cliCommand{Usage: "version", Run: func(_ context.Context, _ Config, args []string, w io.Writer) error {
		if len(args) != 0 {
			return errors.New("usage: cos version")
		}
		return versionCommand(w)
	}}
}
