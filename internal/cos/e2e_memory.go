package cos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// memoryProbeText carries a per-run nonce, so a recall of it proves this run's probe and nothing else.
const memoryProbeText = "e2e memory probe %s canary rollback health gate"

// checkMemoryRecall writes one tagged probe into COS_MEMORY, then requires every host to recall it within the token cap.
func checkMemoryRecall(ctx context.Context, e *E2EEnv) (evidence string, err error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	js, err := jetstream.New(bus.conn)
	if err != nil {
		return "", err
	}
	mem, err := e2eRetryJS(ctx, func(ctx context.Context) (jetstream.KeyValue, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return js.KeyValue(ctx, memoryBucket)
	})
	if err != nil {
		return "", err
	}
	raw := make([]byte, 4)
	rand.Read(raw)
	nonce := hex.EncodeToString(raw)
	put, cancel := context.WithTimeout(ctx, 5*time.Second)
	probe, err := memoryAdd(put, mem, MemoryEntry{Kind: "fact", Text: fmt.Sprintf(memoryProbeText, nonce), Tags: []string{"e2e"}})
	cancel()
	// A failed put can still have stored the probe, so delete by ID whenever one was assigned.
	defer func() {
		if probe.ID == "" {
			return
		}
		call, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if derr := mem.Delete(call, probe.ID); derr != nil && err == nil {
			err = fmt.Errorf("remove probe %s: %w", probe.ID, derr)
		}
	}()
	if err != nil {
		return "", err
	}
	query := "e2e probe " + nonce + " rollback"
	var first MemoryRecall
	for i, host := range e2eHosts {
		if r, err := memoryRecallOnHost(ctx, e, host, query, probe.ID); err != nil {
			return "", fmt.Errorf("%s: %w", host, err)
		} else if i == 0 {
			first = r
		}
	}
	saved := 0
	if first.FullTokens > 0 {
		saved = (first.FullTokens - first.Tokens) * 100 / first.FullTokens
	}
	return fmt.Sprintf("recalled probe on %d/%d hosts; tokens %d of %d full (%d%% saved), cap %d", len(e2eHosts), len(e2eHosts), first.Tokens, first.FullTokens, saved, memoryRecallTokens), nil
}

// memoryRecallOnHost runs the deployed `cos memory recall` on one host, retrying for up to 10 s while replication catches up.
func memoryRecallOnHost(ctx context.Context, e *E2EEnv, host, query, id string) (MemoryRecall, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		var r MemoryRecall
		out, err := e.Host(ctx, host, "~/.local/bin/cos memory recall '"+query+"'")
		if err == nil {
			r, err = memoryProbeResult(out, id)
		}
		if err == nil || time.Now().After(deadline) {
			return r, err
		}
		select {
		case <-ctx.Done():
			return r, err
		case <-time.After(time.Second):
		}
	}
}

// memoryProbeResult validates one host's recall output: the probe must be among the entries and the recall must stay within the token cap.
func memoryProbeResult(out, id string) (MemoryRecall, error) {
	var r MemoryRecall
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &r); err != nil {
		return r, fmt.Errorf("recall prints no JSON: %s", e2eClip(out, 80))
	}
	if !slices.ContainsFunc(r.Entries, func(e MemoryEntry) bool { return e.ID == id }) {
		return r, fmt.Errorf("probe %s missing from %d recalled entries", id, len(r.Entries))
	}
	if r.Tokens > memoryRecallTokens {
		return r, fmt.Errorf("recall spends %d tokens, over the %d cap", r.Tokens, memoryRecallTokens)
	}
	if r.FullTokens < r.Tokens {
		return r, fmt.Errorf("full tokens %d below recalled tokens %d", r.FullTokens, r.Tokens)
	}
	return r, nil
}
