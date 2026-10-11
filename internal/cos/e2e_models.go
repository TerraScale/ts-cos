package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"strings"
)

// checkModelsSettings reads the live policy from the published behavior and checks every node's Settings shows it.
func checkModelsSettings(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	live, version, err := liveModelPolicy(ctx, bus)
	if err != nil {
		return "", err
	}
	urls, err := e.nodeURLs(ctx)
	if err != nil {
		return "", err
	}
	forms := make([]string, len(urls))
	for i, base := range urls {
		cookie, err := e.session(ctx, base)
		if err != nil {
			return "", err
		}
		status, _, _, body, err := e2eRequest(ctx, "GET", base+"/settings", cookie, nil)
		if err != nil {
			return "", err
		}
		if status != http.StatusOK {
			return "", fmt.Errorf("GET %s/settings: HTTP %d, want 200", base, status)
		}
		if forms[i], err = modelsPageShows(body, live); err != nil {
			return "", fmt.Errorf("GET %s/settings: %w", base, err)
		}
	}
	return fmt.Sprintf("bundle %s on %s: settings show %q and policy %s (%s)", version, strings.Join(e2eHosts, ", "), modelsSummary(live), modelsHash(live), strings.Join(forms, ", ")), nil
}

// modelsPageShows checks a Settings page carries the policy summary and its hash, raw or HTML-escaped,
// and names which form matched.
func modelsPageShows(body string, p ModelPolicy) (string, error) {
	summary, hash := modelsSummary(p), "policy "+modelsHash(p)
	for _, form := range []struct{ name, text string }{{"raw", summary}, {"escaped", html.EscapeString(summary)}} {
		if strings.Contains(body, form.text) && strings.Contains(body, hash) {
			return form.name, nil
		}
	}
	return "", errors.New("does not show the live model policy " + summary + " · " + hash)
}

// checkModelsLaunchGuard sends three launches that must all be refused; an accepted launch fails loudly with its thread id.
func checkModelsLaunchGuard(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	probes := []struct {
		name, want string
		input      map[string]any
	}{
		{"no pair and no modelSelection", "state the pair", map[string]any{}},
		{"worker-only model gpt-6-luna", "worker-only", map[string]any{"modelSelection": map[string]any{"instanceId": "codex", "model": "gpt-6-luna"}}},
		{"codex gpt-6.1-sol on fast tier", "Standard tier", map[string]any{"modelSelection": map[string]any{"instanceId": "codex", "model": "gpt-6.1-sol", "options": []any{map[string]any{"id": "serviceTier", "value": "fast"}}}}},
	}
	host := e2eHosts[0]
	var refusals []string
	for _, p := range probes {
		p.input["projectTitle"], p.input["title"], p.input["message"] = "ts-cos", "TS-820 e2e guard probe (must be refused)", "refused probe"
		input, _ := json.Marshal(p.input)
		var launched struct {
			ThreadID string `json:"threadId"`
		}
		err := bus.Call(ctx, host, "launch", []string{string(input)}, &launched)
		if err == nil {
			return "", fmt.Errorf("%s: launch on %s was ACCEPTED as thread %s; a human must delete it", p.name, host, launched.ThreadID)
		}
		if err := guardRefusal(err, p.want); err != nil {
			return "", fmt.Errorf("%s on %s: %w", p.name, host, err)
		}
		refusals = append(refusals, fmt.Sprintf("%s: %q", p.name, err.Error()))
	}
	return fmt.Sprintf("all 3 launches refused on %s: %s", host, strings.Join(refusals, "; ")), nil
}

// guardRefusal passes a launch refusal that names want; any other error is reported as is.
func guardRefusal(err error, want string) error {
	if strings.Contains(err.Error(), want) {
		return nil
	}
	return fmt.Errorf("refusal %q does not contain %q", err.Error(), want)
}
