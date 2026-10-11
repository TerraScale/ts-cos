package cos

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// ModelPolicy is behavior/models.json: which advisor and worker models COS runs, and when each pair is used.
// A pair is one advisor model (a thread) and its worker model (delegate_task subagents).
type ModelPolicy struct {
	Doc        string               `json:"_doc,omitempty"`
	Route      map[string]string    `json:"route"`
	Leader     string               `json:"leader"`
	Pairs      map[string]ModelPair `json:"pairs"`
	WorkerOnly []string             `json:"workerOnly"`
	Priority   []string             `json:"priority"`
}

type ModelPair struct {
	Advisor          AdvisorModel `json:"advisor"`
	HighStakesEffort string       `json:"highStakesEffort"`
	Worker           WorkerModel  `json:"worker"`
}

type AdvisorModel struct {
	InstanceID string        `json:"instanceId"`
	Model      string        `json:"model"`
	Options    []ModelOption `json:"options,omitempty"`
}

type ModelOption struct{ ID, Value string } // json "id","value"

type WorkerModel struct {
	ProviderInstanceID string            `json:"providerInstanceId"`
	Model              string            `json:"model"`
	Options            map[string]string `json:"options,omitempty"`
}

// effortOptions are the option ids that carry the effort level.
var effortOptions = []string{"effort", "reasoningEffort"}

// parseModelPolicy decodes and validates behavior/models.json.
func parseModelPolicy(text string) (ModelPolicy, error) {
	var p ModelPolicy
	dec := json.NewDecoder(strings.NewReader(text))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("behavior models.json: %w", err)
	}
	return p, p.validate()
}

func (p ModelPolicy) validate() error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("behavior models.json: "+format, args...)
	}
	if !slices.Equal(slices.Sorted(maps.Keys(p.Route)), slices.Sorted(maps.Keys(p.Pairs))) {
		return fail("route and pairs must name the same pairs")
	}
	for _, k := range slices.Sorted(maps.Keys(p.Route)) {
		if strings.TrimSpace(p.Route[k]) == "" {
			return fail("route %s needs text", k)
		}
	}
	if _, ok := p.Pairs[p.Leader]; !ok {
		return fail("leader %q is not a pair", p.Leader)
	}
	for _, k := range slices.Sorted(maps.Keys(p.Pairs)) {
		pair := p.Pairs[k]
		switch {
		case pair.Advisor.InstanceID == "" || pair.Advisor.Model == "":
			return fail("pair %s advisor needs instanceId and model", k)
		case p.WorkerOnlyModel(pair.Advisor.Model):
			return fail("pair %s advisor %s is worker-only", k, pair.Advisor.Model)
		case pair.Worker.ProviderInstanceID == "" || pair.Worker.Model == "":
			return fail("pair %s worker needs providerInstanceId and model", k)
		case !p.WorkerOnlyModel(pair.Worker.Model):
			return fail("pair %s worker %s must be listed in workerOnly", k, pair.Worker.Model)
		case pair.HighStakesEffort == "":
			return fail("pair %s needs highStakesEffort", k)
		case pair.Advisor.InstanceID == "codex" && !hasDefaultTier(pair.Advisor.Options):
			return fail("pair %s advisor on codex must set serviceTier default", k)
		case pair.Worker.ProviderInstanceID == "codex" && pair.Worker.Options["serviceTier"] != "default":
			return fail("pair %s worker on codex must set serviceTier default", k)
		}
	}
	seen := map[string]bool{}
	for _, key := range p.Priority {
		if !ticketKeyPattern.MatchString(key) {
			return fail("priority %q is not a ticket key", key)
		}
		if seen[key] {
			return fail("priority %s is listed twice", key)
		}
		seen[key] = true
	}
	return nil
}

func hasDefaultTier(opts []ModelOption) bool {
	return slices.Contains(opts, ModelOption{ID: "serviceTier", Value: "default"})
}

// normalizeModel lowercases a model name and drops a "[1m]"-style suffix.
func normalizeModel(model string) string {
	m := strings.ToLower(strings.TrimSpace(model))
	if i := strings.Index(m, "["); i >= 0 {
		m = strings.TrimSpace(m[:i])
	}
	return m
}

// Advisor is the T3 modelSelection of a pair's advisor; highStakes swaps in the high-stakes effort.
func (p ModelPolicy) Advisor(pair string, highStakes bool) map[string]any {
	m := p.Pairs[pair]
	opts := make([]any, 0, len(m.Advisor.Options))
	for _, o := range m.Advisor.Options {
		value := o.Value
		if highStakes && slices.Contains(effortOptions, o.ID) {
			value = m.HighStakesEffort
		}
		opts = append(opts, map[string]any{"id": o.ID, "value": value})
	}
	return map[string]any{"instanceId": m.Advisor.InstanceID, "model": m.Advisor.Model, "options": opts}
}

// WorkerTarget is the delegate_task target of a pair's worker.
func (p ModelPolicy) WorkerTarget(pair string) map[string]any {
	w := p.Pairs[pair].Worker
	target := map[string]any{"providerInstanceId": w.ProviderInstanceID, "model": w.Model}
	if len(w.Options) > 0 {
		opts := make(map[string]any, len(w.Options))
		for k, v := range w.Options {
			opts[k] = v
		}
		target["options"] = opts
	}
	return target
}

// PairOf finds the pair whose advisor or worker model is model.
func (p ModelPolicy) PairOf(model string) (string, bool) {
	m := normalizeModel(model)
	for _, k := range slices.Sorted(maps.Keys(p.Pairs)) {
		pair := p.Pairs[k]
		if normalizeModel(pair.Advisor.Model) == m || normalizeModel(pair.Worker.Model) == m {
			return k, true
		}
	}
	return "", false
}

// PairOrLeader is PairOf, falling back to the leader for a model no pair names.
func (p ModelPolicy) PairOrLeader(model string) string {
	if k, ok := p.PairOf(model); ok {
		return k
	}
	return p.Leader
}

// IsAdvisor reports whether model is a pair's advisor model.
func (p ModelPolicy) IsAdvisor(model string) bool {
	m := normalizeModel(model)
	return slices.ContainsFunc(p.advisorModels(), func(a string) bool { return normalizeModel(a) == m })
}

// WorkerOnlyModel reports whether model is a worker-only model, or a version of one.
func (p ModelPolicy) WorkerOnlyModel(model string) bool {
	m := normalizeModel(model)
	for _, w := range p.WorkerOnly {
		if w = normalizeModel(w); w != "" && strings.HasPrefix(m, w) {
			return true
		}
	}
	return false
}

// advisorModels lists every pair's advisor model, in pair order.
func (p ModelPolicy) advisorModels() []string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(p.Pairs)) {
		out = append(out, p.Pairs[k].Advisor.Model)
	}
	return out
}

// PairFor picks the pair of a launch: its "pair" key (removed, so T3 never sees it), else the pair of
// the model it names. Anything else is refused with the routes, so the caller states a pair.
func (p ModelPolicy) PairFor(input map[string]any) (string, error) {
	if v, ok := input["pair"]; ok {
		delete(input, "pair")
		if pair, _ := v.(string); p.Pairs[pair].Advisor.Model != "" {
			return pair, nil
		}
	} else if sel, ok := input["modelSelection"].(map[string]any); ok {
		model, _ := sel["model"].(string)
		if pair, ok := p.PairOf(model); ok {
			return pair, nil
		}
	}
	return "", OperationError("state the pair: " + p.routeChoices(`"pair": `))
}

// routeChoices lists each pair with its route text, sorted by pair; prefix goes before each pair name.
func (p ModelPolicy) routeChoices(prefix string) string {
	var choices []string
	for _, k := range slices.Sorted(maps.Keys(p.Route)) {
		choices = append(choices, fmt.Sprintf("%s%q (%s)", prefix, k, p.Route[k]))
	}
	return strings.Join(choices, " or ")
}

// BriefLine tells a thread who advises and who works, so it never runs a worker model as a thread.
func (p ModelPolicy) BriefLine(pair string) string {
	m := p.Pairs[pair]
	label := m.Advisor.Model
	for _, o := range m.Advisor.Options {
		if slices.Contains(effortOptions, o.ID) {
			label += " (" + o.Value + ")"
		}
	}
	target, _ := json.Marshal(p.WorkerTarget(pair))
	return fmt.Sprintf("ADVISOR decides, WORKERS execute: you are the advisor on %s; your workers are %s via delegate_task target %s. Never run a worker model as a thread.", label, m.Worker.Model, target)
}

// launchInput sets a launch's pair and brief: a launch that names a pair gets its advisor as the model,
// and every launch's message says who advises and who works, unless it already does.
func launchInput(pol ModelPolicy, input map[string]any) (map[string]any, error) {
	_, byPair := input["pair"]
	pair, err := pol.PairFor(input)
	if err != nil {
		return nil, err
	}
	if byPair {
		input["modelSelection"] = pol.Advisor(pair, false)
	}
	msg, _ := input["message"].(string)
	switch {
	case strings.Contains(msg, "ADVISOR decides"):
	case msg == "":
		input["message"] = pol.BriefLine(pair)
	default:
		input["message"] = msg + "\n\n" + pol.BriefLine(pair)
	}
	return input, nil
}
