package cos

import (
	"context"
	"errors"
	"io"
	"regexp"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// Shared context files: durable user decisions, preferences and learnings, replicated on every node.
type ContextFile struct {
	Name    string `json:"name"`
	Text    string `json:"text,omitempty"`
	Bytes   int    `json:"bytes"`
	Updated string `json:"updated"`
}

var contextName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

const contextMaxText = 16 << 10

func init() {
	cliCommands["context"] = cliCommand{Usage: "context list | get <name> | set <name> '<markdown>' | rm <name>", Run: func(ctx context.Context, cfg Config, args []string, w io.Writer) error {
		bus, err := ConnectBus(ctx, cfg)
		if err != nil {
			return err
		}
		defer bus.Close()
		kv, err := openKV(ctx, bus, contextBucket)
		if err != nil {
			return err
		}
		return contextCommand(ctx, kv, args, w)
	}}
}

func contextFiles(ctx context.Context, kv jetstream.KeyValue, withText bool) ([]ContextFile, error) {
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	defer lister.Stop()
	out := []ContextFile{}
	for key := range lister.Keys() {
		e, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		f := ContextFile{Name: key, Bytes: len(e.Value()), Updated: e.Created().UTC().Format("2006-01-02T15:04:05Z")}
		if withText {
			f.Text = string(e.Value())
		}
		out = append(out, f)
	}
	// ListKeys ends the channel silently when ctx ends; a short list must not pass as complete.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func contextCommand(ctx context.Context, kv jetstream.KeyValue, args []string, w io.Writer) error {
	usage := errors.New("usage: cos context list | get <name> | set <name> '<markdown>' | rm <name>")
	if len(args) == 0 {
		return usage
	}
	if args[0] == "list" && len(args) == 1 {
		files, err := contextFiles(ctx, kv, false)
		if err != nil {
			return err
		}
		return output(w, files)
	}
	if len(args) < 2 || !contextName.MatchString(args[1]) {
		if len(args) >= 2 {
			return errors.New("context name must be 1-64 lowercase letters, digits or dashes")
		}
		return usage
	}
	name := args[1]
	switch {
	case args[0] == "get" && len(args) == 2:
		e, err := kv.Get(ctx, name)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return errors.New("no context file named " + name)
		}
		if err != nil {
			return err
		}
		return output(w, ContextFile{Name: name, Text: string(e.Value()), Bytes: len(e.Value()), Updated: e.Created().UTC().Format("2006-01-02T15:04:05Z")})
	case args[0] == "set" && len(args) == 3:
		text := strings.TrimSpace(args[2])
		if text == "" || len(text) > contextMaxText {
			return errors.New("context text must be 1 byte to 16 KiB")
		}
		if _, err := kv.Put(ctx, name, []byte(text)); err != nil {
			return err
		}
		return output(w, map[string]any{"set": name, "bytes": len(text)})
	case args[0] == "rm" && len(args) == 2:
		if err := kv.Delete(ctx, name); err != nil {
			return err
		}
		return output(w, map[string]any{"removed": name})
	}
	return usage
}
