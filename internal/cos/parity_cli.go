package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func init() {
	cliCommands["prs"] = cliCommand{"prs", prsCommand}
	cliCommands["merge"] = cliCommand{"merge <repo|owner/repo> <number> [--full-send|--force] [--adopt] --reason '<user order>'", mergeCommand}
}

func prsCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: cos prs")
	}
	v, err := (Forgejo{Config: cfg}).PRs(ctx)
	if err != nil {
		return err
	}
	return output(w, v)
}

// mergeCommand squashes a PR; any flag is a user order past the gate and is recorded.
func mergeCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	const usage = "usage: cos merge <repo> <number> [--full-send|--force] [--adopt] --reason '<user order>'"
	if len(args) < 2 {
		return errors.New(usage)
	}
	n, err := strconv.Atoi(args[1])
	if err != nil || n < 1 {
		return errors.New("PR number must be positive")
	}
	var order MergeOrder
	seen := map[string]bool{}
	for i := 2; i < len(args); i++ {
		flag := args[i]
		if seen[flag] {
			return fmt.Errorf("duplicate %s; %s", flag, usage)
		}
		seen[flag] = true
		switch flag {
		case "--full-send":
			order.FullSend = true
		case "--force":
			order.Force = true
		case "--adopt":
			order.Adopt = true
		case "--reason":
			if i+1 >= len(args) {
				return errors.New("--reason needs a value")
			}
			i++
			order.Reason = strings.TrimSpace(args[i])
		default:
			return fmt.Errorf("unknown flag %q; %s", flag, usage)
		}
	}
	if order.Reason != "" && !order.FullSend && !order.Force && !order.Adopt {
		return errors.New("--reason needs --full-send, --force or --adopt")
	}
	if err := order.Validate(); err != nil {
		return err
	}
	v, err := (Forgejo{Config: cfg}).Merge(ctx, args[0], n, order)
	if err != nil {
		return err
	}
	if order == (MergeOrder{}) {
		return output(w, v)
	}
	result := v.(map[string]any)
	if err := recordMergeOrder(ctx, cfg, args[0], n, result["sha"].(string), order); err != nil {
		// The merge already happened; report the gap instead of failing the command.
		result["recordError"] = publicText(err)
	}
	return output(w, result)
}

// recordMergeOrder leaves a user merge order in the event stream and the chat feed.
func recordMergeOrder(ctx context.Context, cfg Config, repo string, number int, sha string, order MergeOrder) error {
	full := forgejoFullName(repo)
	var names, flags []string
	if order.FullSend {
		names, flags = append(names, "full-send"), append(flags, "--full-send")
	}
	if order.Force {
		names, flags = append(names, "force"), append(flags, "--force")
	}
	if order.Adopt {
		names, flags = append(names, "adopt"), append(flags, "--adopt")
	}
	reason := strings.TrimSpace(order.Reason)
	now := time.Now().UTC()
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return err
	}
	data, err := json.Marshal(map[string]any{"orders": names, "reason": reason, "sha": sha})
	if err != nil {
		return err
	}
	summary := fmt.Sprintf("%s#%d merged with %s: %s", full, number, strings.Join(names, "+"), reason)
	summary = string([]rune(summary)[:min(utf8.RuneCountInString(summary), eventSummaryMax)])
	_, err = PublishEvent(ctx, js, Event{ID: fmt.Sprintf("user:merge-order:%s#%d@%s", full, number, sha), Source: SourceUser,
		Kind: KindMergeOrder, Time: now, Severity: SeverityWarn, Summary: summary, Repo: full, PR: number, Data: data})
	if err != nil {
		return err
	}
	kv, err := openKV(ctx, bus, chatBucket)
	if err != nil {
		return err
	}
	entry := ChatEntry{ID: chatID(now), At: now, Role: "system",
		Text: fmt.Sprintf("Merged %s#%d with %s: %s", full, number, strings.Join(flags, " "), reason)}
	entryData, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, entry.ID, entryData)
	return err
}
