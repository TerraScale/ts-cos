package cos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// cos e2e runs live checks against the deployed COS cluster. It is idempotent,
// cleans up what it creates and never touches the user's real work or credentials.

var e2eHosts = []string{"beta", "delta", "mariogk-dev"}

const (
	e2eUnit          = "cos.service"
	e2eDeadline      = 4*time.Minute + 30*time.Second
	e2eSmokeDeadline = 25 * time.Second
)

// e2eProtected groups guard login, deploy/rollback and the e2e gate itself. A self-change
// (TS-817) must never break them, so the gate fails any change where one of them fails or vanishes.
var e2eProtected = []string{"access", "deploy", "gate"}

type E2EResult struct {
	ID         string `json:"id"`
	Group      string `json:"group"`
	Status     string `json:"status"` // pass, fail or skip (owning feature not built yet)
	Evidence   string `json:"evidence"`
	Stream     string `json:"stream"`
	Protected  bool   `json:"protected,omitempty"`
	DurationMS int64  `json:"durationMs"`
}

type E2EReport struct {
	StartedAt  time.Time      `json:"startedAt"`
	DurationMS int64          `json:"durationMs"`
	Runner     string         `json:"runner"`
	Version    string         `json:"version"`
	Summary    map[string]int `json:"summary"`
	Checks     []E2EResult    `json:"checks"`
}

// e2eSkip marks a check whose feature is not deployed yet; it is reported as "skip", never "fail".
type e2eSkip string

func (e e2eSkip) Error() string { return string(e) }

// E2ESkip reports a check whose feature is not deployed yet.
func E2ESkip(reason string) error { return e2eSkip(reason) }

// E2ECheck is one live check. Its group is the ID's first dot segment.
type E2ECheck struct {
	ID     string
	Stream string // owning ticket, reported with every result
	Smoke  bool   // also part of the fast health gate (cos e2e --smoke, < 30 s, TS-818)
	Run    func(ctx context.Context, env *E2EEnv) (evidence string, err error)
}

var e2eRegistered []E2ECheck

// RegisterE2E is the stable API for per-request acceptance checks (TS-817): call it from an
// init function in an e2e_accept_*.go file. IDs must be "accept.<slug>.<name>" and unique, so a
// registered check can never replace or impersonate a protected check. Run must be idempotent,
// finish well inside the suite's 5 minute budget, clean up what it creates and touch no real work.
func RegisterE2E(c E2ECheck) {
	parts := strings.Split(c.ID, ".")
	if len(parts) < 3 || parts[0] != "accept" || slices.Contains(parts, "") || c.Stream == "" || c.Run == nil {
		panic("RegisterE2E: need ID accept.<slug>.<name>, Stream and Run: " + c.ID)
	}
	for _, r := range e2eRegistered {
		if r.ID == c.ID {
			panic("RegisterE2E: duplicate ID " + c.ID)
		}
	}
	e2eRegistered = append(e2eRegistered, c)
}

func e2eGroup(id string) string {
	group, _, _ := strings.Cut(id, ".")
	return group
}

// E2EEnv is shared by one run. Runner and main are replaceable in tests.
type E2EEnv struct {
	cfg  Config
	run  func(ctx context.Context, host, script string) (string, error)
	main func(ctx context.Context) (e2eMain, error)

	once     sync.Once
	mainHead e2eMain
	mainErr  error
	mu       sync.Mutex
	facts    map[string]e2eFacts
}

// Host runs a bash script on a COS host (locally or over ssh) and returns its combined output.
func (e *E2EEnv) Host(ctx context.Context, host, script string) (string, error) {
	return e.run(ctx, host, script)
}

// LocalHost is the host running the suite.
func (e *E2EEnv) LocalHost() string { return e.cfg.LocalHost }

func (e *E2EEnv) mainSHA(ctx context.Context) (e2eMain, error) {
	e.once.Do(func() { e.mainHead, e.mainErr = e.main(ctx) })
	return e.mainHead, e.mainErr
}

// hostRun executes a bash script on host: locally for the runner's own host, otherwise over ssh.
// The script travels on stdin, so neither the remote login shell nor quoting can alter it.
func (cfg Config) hostRun(ctx context.Context, host, script string) (string, error) {
	var cmd *exec.Cmd
	if host == cfg.LocalHost {
		cmd = exec.CommandContext(ctx, "bash", "-s")
	} else {
		cmd = exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, "bash", "-s")
	}
	cmd.Stdin = strings.NewReader(script)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	text := strings.TrimSpace(out.String())
	if err != nil {
		return text, fmt.Errorf("%s: %w: %s", host, err, e2eClip(text, 200))
	}
	return text, nil
}

var e2eSecret = regexp.MustCompile(`(?i)(token|password|passwd|secret|seed|bcrypt|authorization|cookie)(["']?\s*[=:]\s*)(?:(?:bearer|basic|token)\s+)?\S+`)

// e2eClip keeps evidence short and redacts anything that looks like a credential.
func e2eClip(s string, n int) string {
	s = e2eSecret.ReplaceAllString(strings.TrimSpace(s), "$1$2[redacted]")
	if len(s) > n {
		for n > 0 && !utf8.RuneStart(s[n]) {
			n--
		}
		return s[:n] + "…"
	}
	return s
}

func versionCommand(w io.Writer) error {
	return output(w, map[string]any{"revision": strings.TrimSuffix(Version, "-dirty"), "modified": strings.HasSuffix(Version, "-dirty")})
}

func E2E(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	env := &E2EEnv{cfg: cfg, run: cfg.hostRun, main: func(ctx context.Context) (e2eMain, error) { return mainHead(ctx, cfg) }}
	report := runE2E(ctx, env, e2eChecks(), args)
	if err := output(w, report); err != nil {
		return err
	}
	if n := report.Summary["fail"]; n > 0 {
		return fmt.Errorf("%d e2e checks failed", n)
	}
	return nil
}

// E2ESmoke is the rollout health gate (TS-818): the smoke checks for host plus the cluster-wide
// smoke checks, with deploy.version expecting revision (the build being rolled out or rolled back
// to) instead of origin/main. It passes when Summary["fail"] is 0.
func E2ESmoke(ctx context.Context, cfg Config, host, revision string) E2EReport {
	env := &E2EEnv{cfg: cfg, run: cfg.hostRun, main: func(context.Context) (e2eMain, error) { return e2eMain{SHA: revision}, nil }}
	return runE2E(ctx, env, smokeChecks(e2eChecks(), host), []string{"--smoke"})
}

func smokeChecks(checks []E2ECheck, host string) []E2ECheck {
	return slices.DeleteFunc(slices.Clone(checks), func(c E2ECheck) bool {
		h := c.ID[strings.LastIndex(c.ID, ".")+1:]
		return !c.Smoke || slices.Contains(e2eHosts, h) && h != host
	})
}

// runE2E runs checks in order. "--smoke" selects the fast health-gate subset; other args are
// ID prefixes (for example "deploy" or "cluster.partition").
func runE2E(ctx context.Context, env *E2EEnv, checks []E2ECheck, args []string) E2EReport {
	smoke := slices.Contains(args, "--smoke")
	filter := slices.DeleteFunc(slices.Clone(args), func(a string) bool { return a == "--smoke" })
	deadline := e2eDeadline
	if smoke {
		deadline = e2eSmokeDeadline
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	report := E2EReport{StartedAt: time.Now().UTC(), Runner: env.cfg.LocalHost, Version: Version, Summary: map[string]int{"pass": 0, "fail": 0, "skip": 0}, Checks: []E2EResult{}}
	for _, c := range checks {
		if smoke && !c.Smoke || len(filter) > 0 && !slicesHasPrefix(filter, c.ID) {
			continue
		}
		start := time.Now()
		r := E2EResult{ID: c.ID, Group: e2eGroup(c.ID), Stream: c.Stream, Status: "pass", Protected: slices.Contains(e2eProtected, e2eGroup(c.ID))}
		evidence, err := c.Run(ctx, env)
		var skip e2eSkip
		switch {
		case errors.As(err, &skip):
			r.Status, evidence = "skip", skip.Error()
		case err != nil:
			r.Status, evidence = "fail", err.Error()
		}
		r.Evidence = e2eClip(evidence, 600)
		r.DurationMS = time.Since(start).Milliseconds()
		report.Summary[r.Status]++
		report.Checks = append(report.Checks, r)
	}
	report.DurationMS = time.Since(report.StartedAt).Milliseconds()
	return report
}

func slicesHasPrefix(prefixes []string, id string) bool {
	for _, p := range prefixes {
		if id == p || strings.HasPrefix(id, p+".") {
			return true
		}
	}
	return false
}

func e2eChecks() []E2ECheck {
	var checks []E2ECheck
	for _, h := range e2eHosts {
		checks = append(checks,
			E2ECheck{ID: "deploy.unit." + h, Stream: "TS-810", Smoke: true, Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkUnit(ctx, e, h) }},
			E2ECheck{ID: "deploy.version." + h, Stream: "TS-810", Smoke: true, Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkVersion(ctx, e, h) }},
			E2ECheck{ID: "deploy.restarts." + h, Stream: "TS-810", Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkRestarts(ctx, e, h) }},
			E2ECheck{ID: "deploy.logs." + h, Stream: "TS-810", Smoke: true, Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkLogs(ctx, e, h) }},
		)
	}
	checks = append(checks,
		E2ECheck{ID: "cluster.members", Stream: "TS-810", Smoke: true, Run: checkMembers},
		E2ECheck{ID: "cluster.jetstream", Stream: "TS-810", Smoke: true, Run: checkJetStream},
		E2ECheck{ID: "cluster.leader", Stream: "TS-810", Smoke: true, Run: checkLeader},
		E2ECheck{ID: "cluster.restart", Stream: "TS-810", Run: checkRestartCatchUp},
		E2ECheck{ID: "cluster.partition", Stream: "TS-810", Run: checkPartition},
		E2ECheck{ID: "cluster.leader-kill", Stream: "TS-810", Run: checkLeaderKill},
		E2ECheck{ID: "rules.heartbeat-leader", Stream: "TS-813", Smoke: true, Run: checkHeartbeatLeader},
	)
	return append(append(checks, e2ePending...), e2eRegistered...)
}

func pending(reason string) func(context.Context, *E2EEnv) (string, error) {
	return func(context.Context, *E2EEnv) (string, error) { return "", e2eSkip(reason) }
}
