package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"git.terrascale.tech/TerraScale/ts-cos/checks"
)

// Every committed self-change acceptance check is also a live `cos e2e` check, so a
// later change that breaks an earlier request fails the suite.
func init() {
	files, _ := fs.Glob(checks.Files, "*.json")
	for _, name := range files {
		data, _ := checks.Files.ReadFile(name)
		var c selfCheck
		if json.Unmarshal(data, &c) != nil || c.validate() != nil {
			continue // TestRepoAcceptanceChecks fails such a file in CI
		}
		ticket := strings.TrimSuffix(name, ".json")
		RegisterE2E(E2ECheck{ID: selfE2EID(ticket), Stream: ticket, Run: func(ctx context.Context, e *E2EEnv) (string, error) {
			bus, err := e.bus(ctx)
			if err != nil {
				return "", err
			}
			defer bus.Close()
			results, passed, err := runSelfCheck(ctx, bus, c)
			if err != nil {
				return "", err
			}
			if !passed {
				return "", errors.New(c.Summary + ": " + selfEvidence(results))
			}
			return c.Summary + ": " + selfEvidence(results), nil
		}})
	}
}

func selfE2EID(ticket string) string { return "accept." + strings.ToLower(ticket) + ".live" }

// checkSelfGate (protected): the self-change store is replicated and the guardrail
// refuses every protected path, so no self-change can disable login, deploy or this gate.
func checkSelfGate(ctx context.Context, e *E2EEnv) (string, error) {
	for _, path := range []string{"internal/cos/dashboard_auth.go", "internal/cos/login.templ", ".forgejo/workflows/ci.yml", "internal/cos/e2e.go", "internal/cos/selfchange.go", "deploy/cos.service"} {
		if selfGuard([]string{path}, "TS-0") == "" {
			return "", fmt.Errorf("self-change guard allows %s", path)
		}
	}
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return "", err
	}
	stream, err := js.Stream(ctx, "KV_"+selfBucket)
	if err != nil {
		return "", E2ESkip("no self-change has been requested yet (" + selfBucket + " not created)")
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return "", err
	}
	if info.Config.Replicas != 3 || info.Cluster == nil || len(info.Cluster.Replicas) != 2 {
		return "", fmt.Errorf("%s is not on 3 current replicas (configured %d)", selfBucket, info.Config.Replicas)
	}
	return selfBucket + " on 3 replicas; guard refuses login, deploy, CI, e2e and self-change paths", nil
}
