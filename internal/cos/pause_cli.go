package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// The paused set lives in the replicated autopilot settings bucket, so every node
// sees the same pauses and the rules engine on the leader reads them.
const pausedKey = "paused"

var ticketKeyPattern = regexp.MustCompile(`^[A-Z][A-Z0-9]*-\d+$`)

func init() {
	cliCommands["pause"] = cliCommand{Usage: "pause <TS-n>... (pause tickets: rules never wake their threads)", Run: pauseCommand(true)}
	cliCommands["resume"] = cliCommand{Usage: "resume <TS-n>...", Run: pauseCommand(false)}
}

func pauseCommand(paused bool) func(context.Context, Config, []string, io.Writer) error {
	verb := "resume"
	if paused {
		verb = "pause"
	}
	return func(ctx context.Context, cfg Config, args []string, w io.Writer) error {
		if len(args) == 0 {
			return fmt.Errorf("usage: cos %s <TS-n>...", verb)
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
		kv, err := js.KeyValue(ctx, autopilotSettings)
		if err != nil {
			return err
		}
		var set []string
		for _, key := range args {
			if set, err = setPaused(ctx, kv, key, paused); err != nil {
				return err
			}
		}
		return output(w, map[string][]string{"paused": set})
	}
}

// readPaused returns the paused keys and the entry revision (0 when the key is missing).
func readPaused(ctx context.Context, kv jetstream.KeyValue) ([]string, uint64, error) {
	entry, err := kv.Get(ctx, pausedKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var keys []string
	if err := json.Unmarshal(entry.Value(), &keys); err != nil {
		return nil, 0, fmt.Errorf("paused set: %w", err)
	}
	return keys, entry.Revision(), nil
}

func pausedSet(ctx context.Context, kv jetstream.KeyValue) (map[string]bool, error) {
	keys, _, err := readPaused(ctx, kv)
	if err != nil {
		return nil, err
	}
	paused := make(map[string]bool, len(keys))
	for _, key := range keys {
		paused[key] = true
	}
	return paused, nil
}

// setPaused edits one ticket in the paused set and returns the sorted result.
func setPaused(ctx context.Context, kv jetstream.KeyValue, key string, paused bool) ([]string, error) {
	key = strings.ToUpper(key)
	if !ticketKeyPattern.MatchString(key) {
		return nil, OperationError(fmt.Sprintf("%q is not a ticket key; use TS-<n>", key))
	}
	for range 3 {
		keys, revision, err := readPaused(ctx, kv)
		if err != nil {
			return nil, err
		}
		set := make(map[string]bool, len(keys)+1)
		for _, k := range keys {
			set[k] = true
		}
		if paused {
			set[key] = true
		} else {
			delete(set, key)
		}
		next := slices.Sorted(maps.Keys(set))
		if next == nil {
			next = []string{}
		}
		data, err := json.Marshal(next)
		if err != nil {
			return nil, err
		}
		if revision == 0 {
			_, err = kv.Create(ctx, pausedKey, data)
		} else {
			_, err = kv.Update(ctx, pausedKey, data, revision)
		}
		if err == nil {
			return next, nil
		}
		if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) && !errors.Is(err, jetstream.ErrKeyExists) {
			return nil, err
		}
	}
	return nil, errors.New("paused set kept changing; try again")
}
