package cos

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
)

func init() {
	cliCommands["models"] = cliCommand{Usage: "models | models set '<json merge patch>', e.g. '{\"priority\":[\"TS-819\"]}'", Run: modelsCommand}
}

// decodePatch checks that a merge patch is a JSON object before anything else reads it.
func decodePatch(patch []byte) (map[string]any, error) {
	var p map[string]any
	if err := json.Unmarshal(patch, &p); err != nil || p == nil {
		return nil, errors.New("models patch must be a JSON object")
	}
	return p, nil
}

// mergePatch applies RFC 7386: objects merge key by key, null deletes a key, anything else replaces.
func mergePatch(target, patch any) any {
	obj, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	t, _ := target.(map[string]any)
	if t == nil {
		t = map[string]any{}
	}
	for k, v := range obj {
		if v == nil {
			delete(t, k)
			continue
		}
		t[k] = mergePatch(t[k], v)
	}
	return t
}

// applyModelsPatch merges patch into cur and returns the validated policy and its models.json bytes.
// A patch that changes nothing is refused.
func applyModelsPatch(cur ModelPolicy, patch []byte) (ModelPolicy, []byte, error) {
	p, err := decodePatch(patch)
	if err != nil {
		return cur, nil, err
	}
	base, err := json.Marshal(cur)
	if err != nil {
		return cur, nil, err
	}
	var doc map[string]any
	if err := json.Unmarshal(base, &doc); err != nil {
		return cur, nil, err
	}
	merged, err := json.Marshal(mergePatch(doc, p))
	if err != nil {
		return cur, nil, err
	}
	var next ModelPolicy
	dec := json.NewDecoder(bytes.NewReader(merged))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&next); err != nil {
		return cur, nil, fmt.Errorf("models patch: %w", err)
	}
	for i, key := range next.Priority {
		next.Priority[i] = strings.ToUpper(strings.TrimSpace(key))
	}
	if err := next.validate(); err != nil {
		return cur, nil, err
	}
	after, err := json.Marshal(next)
	if err != nil {
		return cur, nil, err
	}
	if bytes.Equal(base, after) {
		return cur, nil, errors.New("models patch changes nothing")
	}
	file, err := json.MarshalIndent(next, "", "  ")
	return next, append(file, '\n'), err
}

// modelsSummary is the one line a model-policy change names: leader, each pair's advisor and worker, and priority.
func modelsSummary(p ModelPolicy) string {
	parts := []string{"leader " + p.Leader}
	for _, k := range slices.Sorted(maps.Keys(p.Pairs)) {
		pair := p.Pairs[k]
		advisor := k + " " + pair.Advisor.Model
		for _, o := range pair.Advisor.Options {
			if slices.Contains(effortOptions, o.ID) {
				advisor += " (" + o.Value + ")"
			}
		}
		parts = append(parts, advisor+" + "+pair.Worker.Model)
	}
	priority := "(none)"
	if len(p.Priority) > 0 {
		priority = strings.Join(p.Priority, ", ")
	}
	return strings.Join(append(parts, "priority "+priority), " · ")
}

// modelsHash fingerprints a policy, so Settings and the live check show when a route-only edit lands.
func modelsHash(p ModelPolicy) string {
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:8]
}

// modelsChange queues the replacement of behavior/models.json as a self-change, so it goes live through the normal path.
func modelsChange(ctx context.Context, store selfStore, cur ModelPolicy, patch []byte) (selfRecord, error) {
	next, file, err := applyModelsPatch(cur, patch)
	if err != nil {
		return selfRecord{}, err
	}
	fields, _ := decodePatch(patch)
	keys := slices.Sorted(maps.Keys(fields))
	raw, err := json.Marshal(map[string]any{
		"request": "Replace the whole content of behavior/models.json with exactly the JSON below. It is already validated. Change no other file.\n```json\n" + string(file) + "```",
		"summary": "Model policy: " + strings.Join(keys, ", ") + " changed",
		"pair":    "sol",
		"check":   selfCheck{Summary: "Settings shows the new model policy", Path: "/settings", Contains: []string{modelsSummary(next), "policy " + modelsHash(next)}},
	})
	if err != nil {
		return selfRecord{}, err
	}
	r, err := selfRequest(string(raw))
	if err != nil {
		return selfRecord{}, err
	}
	return r, selfEnqueue(ctx, store, r)
}

// IsP0 reports whether a ticket is P0: a FULL SEND title, or the first key in models.json priority.
func IsP0(ticket, title string) bool {
	p := CurrentBehavior().Models.Priority
	return fullSendTitle(title) || len(p) > 0 && strings.ToUpper(ticket) == p[0]
}

// compareTasks orders tasks by priority, then by their place in the priority list, then by ticket.
func compareTasks(a, b Task, priority []string) int {
	if d := priorityRank(a.Priority) - priorityRank(b.Priority); d != 0 {
		return d
	}
	if d := priorityIndex(priority, a.Ticket) - priorityIndex(priority, b.Ticket); d != 0 {
		return d
	}
	return cmp.Compare(a.Ticket, b.Ticket)
}

// priorityIndex is a ticket's place in the priority list; unlisted tickets come after every listed one.
func priorityIndex(priority []string, ticket string) int {
	if i := slices.Index(priority, ticket); i >= 0 {
		return i
	}
	return len(priority)
}

// liveModelPolicy reads models.json from the published behavior bundle, the file every node runs.
func liveModelPolicy(ctx context.Context, bus *Bus) (ModelPolicy, string, error) {
	js, err := bus.JetStream()
	if err != nil {
		return ModelPolicy{}, "", err
	}
	kv, err := behaviorBucketKV(ctx, js, false)
	if err != nil {
		return ModelPolicy{}, "", fmt.Errorf("COS behavior store unavailable; is cos serve running? (%w)", err)
	}
	e, err := kv.Get(ctx, behaviorKey)
	if err != nil {
		return ModelPolicy{}, "", fmt.Errorf("no behavior published yet: %w", err)
	}
	var bundle behaviorBundle
	if err := json.Unmarshal(e.Value(), &bundle); err != nil {
		return ModelPolicy{}, "", err
	}
	raw, ok := bundle.Files["models.json"]
	if !ok {
		return ModelPolicy{}, "", errors.New("published behavior has no models.json")
	}
	p, err := parseModelPolicy(raw)
	return p, bundle.Version, err
}

// modelsCommand implements `cos models` (read the live policy) and `cos models set '<patch>'` (queue a change).
func modelsCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	usage := errors.New("usage: cos models | cos models set '<json merge patch>'")
	if len(args) == 2 && args[0] == "set" {
		if _, err := decodePatch([]byte(args[1])); err != nil {
			return err
		}
	} else if len(args) != 0 {
		return usage
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	live, version, err := liveModelPolicy(ctx, bus)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return output(w, map[string]any{"summary": modelsSummary(live), "version": version, "leader": live.Leader, "route": live.Route, "pairs": live.Pairs, "priority": live.Priority})
	}
	store, err := openSelfStore(ctx, bus, false)
	if err != nil {
		return err
	}
	r, err := modelsChange(ctx, store, live, []byte(args[1]))
	if err != nil {
		return err
	}
	return output(w, map[string]any{"id": r.ID, "state": r.State, "summary": r.Summary, "next": "COS posts progress in the chat and Updated once live"})
}
