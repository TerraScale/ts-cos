package cos

import (
	"context"
	"fmt"
	"strings"
)

// checkNoHaikuThreads requires every open T3 thread to run on a non-Haiku model. Haiku is allowed
// only as delegate_task subagents, so a top-level or forked Haiku thread is a policy breach.
func checkNoHaikuThreads(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	fleets, err := Fleet(ctx, bus)
	if err != nil {
		return "", err
	}
	var unreachable []string
	total := 0
	for _, f := range fleets {
		if f.Error != "" {
			unreachable = append(unreachable, f.Host)
			continue
		}
		total += len(f.Threads)
	}
	if bad := haikuThreads(fleets); len(bad) > 0 {
		return "", fmt.Errorf("%d thread(s) run on a Haiku model (Haiku is allowed only as delegate_task subagents): %s", len(bad), strings.Join(bad, "; "))
	}
	evidence := fmt.Sprintf("no Haiku threads among %d open threads on %d hosts", total, len(fleets))
	for _, h := range unreachable {
		evidence += fmt.Sprintf("; host %s unreachable", h)
	}
	return evidence, nil
}

// haikuThreads lists each thread that runs on Haiku without being a delegate_task subagent.
func haikuThreads(fleets []HostFleet) []string {
	var bad []string
	for _, f := range fleets {
		for _, t := range f.Threads {
			if !strings.Contains(strings.ToLower(t.Model), "haiku") {
				continue
			}
			if t.Lineage != nil && t.Lineage.Relation == "subagent" {
				continue
			}
			bad = append(bad, fmt.Sprintf("%s/%s (%s, %q)", f.Host, t.ThreadID, t.Model, e2eClip(t.Title, 60)))
		}
	}
	return bad
}
