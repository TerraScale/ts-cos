package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// haikuLaunch matches a thread launch that selects Haiku. delegate_task targets use providerInstanceId, so they never match.
var haikuLaunch = regexp.MustCompile(`(?i)"instanceId"\s*:\s*"claudeAgent"\s*,\s*"model"\s*:\s*"claude-haiku`)

// haikuThread matches prose that tells an agent to open a Haiku 5.5 thread.
var haikuThread = regexp.MustCompile(`(?i)haiku 5\.5 thread`)

// checkAdvisorPrinciple reads the deployed behavior bundle and requires its briefs to keep "Opus advises, Haiku executes".
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
	var problems []string
	for _, name := range []string{"brief.md", "templates/selfbrief.tmpl"} {
		problems = append(problems, advisorProblems(name, bundle.Files[name])...)
	}
	if len(problems) > 0 {
		return "", fmt.Errorf("deployed behavior %s breaks \"Opus advises, Haiku executes\": %s", bundle.Version, strings.Join(problems, "; "))
	}
	return "deployed behavior " + bundle.Version + ": brief.md and selfbrief carry Opus advisor + Haiku delegate_task subagents, no Haiku threads", nil
}

// advisorProblems lists how one behavior file breaks the advisor principle. A missing file has empty content and fails the same checks.
func advisorProblems(name, content string) []string {
	var problems []string
	if !strings.Contains(content, "Opus 5.5") && !strings.Contains(content, "claude-opus-5-5") {
		problems = append(problems, name+": no Opus 5.5 advisor")
	}
	if !strings.Contains(content, "claude-haiku-5-5") {
		problems = append(problems, name+": no claude-haiku-5-5 delegate")
	}
	if !strings.Contains(content, "delegate_task") {
		problems = append(problems, name+": no delegate_task")
	}
	if haikuLaunch.MatchString(content) {
		problems = append(problems, name+": launches a Haiku thread")
	}
	if haikuThread.MatchString(content) {
		problems = append(problems, name+": tells agents to use a Haiku 5.5 thread")
	}
	return problems
}
