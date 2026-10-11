package cos

import (
	"context"
	"io"
)

// cliCommand is a host-less command a feature registers from its own file in init(),
// so adding a command never edits cli.go. Run validates its own arguments.
type cliCommand struct {
	Usage string
	Run   func(ctx context.Context, cfg Config, args []string, w io.Writer) error
}

var cliCommands = map[string]cliCommand{}
