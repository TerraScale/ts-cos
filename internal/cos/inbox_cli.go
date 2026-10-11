package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

func init() {
	cliCommands["inbox"] = cliCommand{Usage: "inbox add '<json: kind, title, context, blocks, options, ticket, links, host, thread, source, key, urgent, supersedes>' | list | history | answer <id> '<text>' | dismiss <id>", Run: inboxCommand}
}

func inboxCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	usage := errors.New("usage: cos inbox add '<json>' | list | history | answer <id> '<text>' | dismiss <id>")
	if len(args) == 0 {
		return usage
	}
	if args[0] == "list" || args[0] == "history" {
		if len(args) != 1 {
			return usage
		}
		s, err := stateStore(cfg)
		if err != nil {
			return err
		}
		defer s.Close()
		open, history, err := s.Inbox(ctx)
		if err != nil {
			return err
		}
		if args[0] == "list" {
			return output(w, open)
		}
		return output(w, history[:min(len(history), inboxHistoryMax)])
	}
	if want := map[string]int{"add": 2, "dismiss": 2, "answer": 3}[args[0]]; len(args) != want {
		return usage
	}
	var input InboxInput
	if args[0] == "add" {
		dec := json.NewDecoder(strings.NewReader(args[1]))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&input); err != nil {
			return fmt.Errorf("inbox item JSON: %w", err)
		}
		if _, err := input.item(time.Now()); err != nil {
			return err
		}
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	in, err := OpenInbox(ctx, bus, cfg.LocalHost, false)
	if err != nil {
		return err
	}
	var item InboxItem
	switch args[0] {
	case "add":
		item, err = in.Add(ctx, input)
	case "dismiss":
		item, err = in.Dismiss(ctx, args[1])
	case "answer":
		answer := AnswerData{Text: args[2]}
		if entry, getErr := in.kv.Get(ctx, args[1]); getErr == nil {
			var cur InboxItem
			if json.Unmarshal(entry.Value(), &cur) == nil && slices.Contains(cur.Options, strings.TrimSpace(args[2])) {
				answer = AnswerData{Option: strings.TrimSpace(args[2])}
			}
		}
		item, err = in.Answer(ctx, args[1], answer)
	default:
		return usage
	}
	if err != nil {
		return err
	}
	return output(w, item)
}
