package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// checkAdvisorPrinciple reads the deployed behavior bundle and requires its briefs to keep "ADVISOR decides, WORKERS execute".
func checkAdvisorPrinciple(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	js, err := jetstream.New(bus.conn)
	if err != nil {
		return "", err
	}
	entry, err := e2eRetryJS(ctx, func(ctx context.Context) (jetstream.KeyValueEntry, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		kv, err := js.KeyValue(ctx, behaviorBucket)
		if err != nil {
			return nil, err
		}
		return kv.Get(ctx, behaviorKey)
	})
	if err != nil {
		return "", err
	}
	var bundle behaviorBundle
	if err := json.Unmarshal(entry.Value(), &bundle); err != nil {
		return "", err
	}
	b, err := ParseBehavior(bundle.Version, bundle.Files)
	if err != nil {
		return "", fmt.Errorf("deployed behavior %s does not load: %w", bundle.Version, err)
	}
	problems := briefProblems(b.AgentBrief(), b.Models)
	problems = append(problems, advisorProblems("brief.md", b.AgentBrief(), b.Models)...)
	problems = append(problems, advisorProblems("templates/selfbrief.tmpl", bundle.Files["templates/selfbrief.tmpl"], b.Models)...)
	if len(problems) > 0 {
		return "", fmt.Errorf("deployed behavior %s breaks \"ADVISOR decides, WORKERS execute\": %s", bundle.Version, strings.Join(problems, "; "))
	}
	return "deployed behavior " + bundle.Version + ": brief names every pair's advisor and worker, selfbrief delegates to workers, no worker threads", nil
}

// workerNames is an alternation of the worker-only models, for matching a launch or prose that runs one as a thread.
func workerNames(pol ModelPolicy) string {
	names := make([]string, 0, len(pol.WorkerOnly))
	for _, m := range pol.WorkerOnly {
		names = append(names, regexp.QuoteMeta(m))
	}
	return strings.Join(names, "|")
}

// briefProblems lists what the brief must say: the principle, and each pair's advisor and worker model.
func briefProblems(content string, pol ModelPolicy) []string {
	var problems []string
	if !strings.Contains(content, "ADVISOR decides, WORKERS execute") {
		problems = append(problems, "brief: no \"ADVISOR decides, WORKERS execute\"")
	}
	for _, k := range slices.Sorted(maps.Keys(pol.Pairs)) {
		for _, m := range []string{pol.Pairs[k].Advisor.Model, pol.Pairs[k].Worker.Model} {
			if !strings.Contains(content, m) {
				problems = append(problems, "brief: no "+m+" in pair "+k)
			}
		}
	}
	return problems
}

// advisorProblems lists how one behavior file breaks the advisor principle. A missing file has empty content and fails the same checks.
func advisorProblems(name, content string, pol ModelPolicy) []string {
	var problems []string
	if !strings.Contains(content, "delegate_task") {
		problems = append(problems, name+": no delegate_task")
	}
	launch := regexp.MustCompile(`(?i)"instanceId"\s*:\s*"[^"]*"\s*,\s*"model"\s*:\s*"(` + workerNames(pol) + `)"`)
	if launch.MatchString(content) {
		problems = append(problems, name+": launches a worker model as a thread")
	}
	prose := regexp.MustCompile(`(?i)(` + workerNames(pol) + `) thread`)
	if prose.MatchString(content) {
		problems = append(problems, name+": tells agents to open a worker-model thread")
	}
	return problems
}
