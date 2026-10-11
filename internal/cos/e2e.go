package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
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

// e2eReportData is the e2e.report payload the dashboard KPI tiles fold.
type e2eReportData struct {
	Pass   int    `json:"pass"`
	Fail   int    `json:"fail"`
	Skip   int    `json:"skip"`
	Runner string `json:"runner"`
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
	Chaos  bool   // perturbs the cluster (kill, restart, partition, freeze); never part of --smoke
	// OnDemand checks run only when named by their exact ID (for example a real crash).
	OnDemand bool
	Run      func(ctx context.Context, env *E2EEnv) (evidence string, err error)
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
	if c.Smoke && c.Chaos {
		panic("RegisterE2E: a check cannot be both Smoke and Chaos: " + c.ID)
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

	// chaosLock takes the cluster-wide chaos lock; nil takes the real one. Replaceable in tests.
	chaosLock func(ctx context.Context) (func(), error)

	// waited is the time the current chaos check spent waiting for the lock or a rollout. chaosWaited is
	// the lock and rollout wait all chaos checks of this run share (TS-816).
	waited, chaosWaited time.Duration

	// newUser creates the run's shared user and returns its cleanup; nil uses the bus. Replaceable in tests.
	newUser func(ctx context.Context) (*e2eUser, func(), error)
	// sessMu guards sess: the run's shared user and its session cookie per node URL (see session).
	sessMu sync.Mutex
	sess   *e2eSession

	once       sync.Once
	mainHead   e2eMain
	mainErr    error
	mu         sync.Mutex
	facts      map[string]e2eFacts
	collectors map[string]collectorProbe

	// Set by nodes.collector-stop: the synthetic host it stopped and its last sample time.
	nodeGhost   string
	nodeGhostAt time.Time
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

func E2E(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	env := &E2EEnv{cfg: cfg, run: cfg.hostRun, main: func(ctx context.Context) (e2eMain, error) { return mainHead(ctx, cfg) }}
	var report E2EReport
	if slices.Contains(args, "--smoke") {
		report = runE2E(ctx, env, e2eChecks(), args)
	} else {
		report = runFull(ctx, env, e2eChecks(), args)
	}
	if len(args) == 0 {
		publishE2EReport(ctx, cfg, report)
	}
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

// publishE2EReport records a full run on COS_EVENTS for the KPI tiles. Best effort: a failure is
// logged and never changes the run's result.
func publishE2EReport(ctx context.Context, cfg Config, report E2EReport) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		slog.Warn("publish e2e report", "error", err)
		return
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		slog.Warn("publish e2e report", "error", err)
		return
	}
	data, err := json.Marshal(e2eReportData{Pass: report.Summary["pass"], Fail: report.Summary["fail"], Skip: report.Summary["skip"], Runner: report.Runner})
	if err != nil {
		slog.Warn("publish e2e report", "error", err)
		return
	}
	severity := SeverityInfo
	if report.Summary["fail"] > 0 {
		severity = SeverityWarn
	}
	e := Event{
		ID:       NewEventID(SourceCOS, "e2e", report.Runner, strconv.FormatInt(report.StartedAt.UnixNano(), 10)),
		Source:   SourceCOS,
		Kind:     KindE2EReport,
		Time:     report.StartedAt,
		Severity: severity,
		Summary:  fmt.Sprintf("e2e %d pass, %d fail, %d skip", report.Summary["pass"], report.Summary["fail"], report.Summary["skip"]),
		Host:     report.Runner,
		Data:     data,
	}
	if _, err := PublishEvent(ctx, js, e); err != nil {
		slog.Warn("publish e2e report", "error", err)
	}
}

func smokeChecks(checks []E2ECheck, host string) []E2ECheck {
	return slices.DeleteFunc(slices.Clone(checks), func(c E2ECheck) bool {
		h := c.ID[strings.LastIndex(c.ID, ".")+1:]
		return !c.Smoke || slices.Contains(e2eHosts, h) && h != host
	})
}

// runE2E runs checks in order. "--smoke" selects the fast health-gate subset, which never includes
// chaos; other args are ID prefixes (for example "deploy" or "cluster.partition").
func runE2E(ctx context.Context, env *E2EEnv, checks []E2ECheck, args []string) E2EReport {
	smoke := slices.Contains(args, "--smoke")
	filter := slices.DeleteFunc(slices.Clone(args), func(a string) bool { return a == "--smoke" })
	// Chaos perturbs the cluster and may wait for the lock, so it runs after the checks that need a quiet cluster and a full deadline.
	checks = slices.Clone(checks)
	slices.SortStableFunc(checks, func(a, b E2ECheck) int {
		switch {
		case a.Chaos == b.Chaos:
			return 0
		case a.Chaos:
			return 1
		}
		return -1
	})
	// Chaos may wait up to e2eChaosWait for the lock or a rollout; the checks themselves still fit e2eDeadline.
	deadline := e2eDeadline + e2eChaosWait
	if smoke {
		deadline = e2eSmokeDeadline
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	report := E2EReport{StartedAt: time.Now().UTC(), Runner: env.cfg.LocalHost, Version: Version, Summary: map[string]int{"pass": 0, "fail": 0, "skip": 0}, Checks: []E2EResult{}}
	chaos := &chaosLockRun{env: env, budget: e2eChaosWait, poll: e2eChaosPoll}
	defer chaos.close()
	defer env.closeSession()
	for _, c := range checks {
		if smoke && (!c.Smoke || c.Chaos) || len(filter) > 0 && !slicesHasPrefix(filter, c.ID) || c.OnDemand && !slices.Contains(filter, c.ID) {
			continue
		}
		start := time.Now()
		r := E2EResult{ID: c.ID, Group: e2eGroup(c.ID), Stream: c.Stream, Status: "pass", Protected: slices.Contains(e2eProtected, e2eGroup(c.ID))}
		var evidence string
		var err error
		if c.Chaos {
			env.waited = 0
			err = chaos.take(ctx)
		}
		if err == nil {
			evidence, err = c.Run(ctx, env)
		}
		if c.Chaos && err == nil && env.waited.Round(time.Second) > 0 {
			evidence += fmt.Sprintf("; waited %s for lock/rollout", env.waited.Round(time.Second))
		}
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
	// First, so the leader's 60 s missing window overlaps the rest of the suite.
	checks := []E2ECheck{{ID: "nodes.collector-stop", Stream: "TS-825", Run: checkNodeCollectorStop}}
	// The login checks run first: their sign-ins fill the run's shared session, which the checks below reuse.
	checks = append(checks,
		E2ECheck{ID: "access.login.tailnet", Stream: "TS-810", Smoke: true, Run: nodesCheck(checkLoginTailnet)},
		E2ECheck{ID: "access.login.public", Stream: "TS-810", Smoke: true, Run: checkLoginPublic},
	)
	for _, h := range e2eHosts {
		checks = append(checks,
			E2ECheck{ID: "deploy.unit." + h, Stream: "TS-810", Smoke: true, Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkUnit(ctx, e, h) }},
			E2ECheck{ID: "deploy.version." + h, Stream: "TS-810", Smoke: true, Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkVersion(ctx, e, h) }},
			E2ECheck{ID: "deploy.restarts." + h, Stream: "TS-810", Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkRestarts(ctx, e, h) }},
			E2ECheck{ID: "deploy.logs." + h, Stream: "TS-810", Smoke: true, Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkLogs(ctx, e, h) }},
			E2ECheck{ID: "deploy.watchdog." + h, Stream: "TS-818", Run: func(ctx context.Context, e *E2EEnv) (string, error) { return checkWatchdog(ctx, e, h) }},
			E2ECheck{ID: "home.kpis." + h, Stream: "TS-824", Run: nodesCheck(func(ctx context.Context, e *E2EEnv, urls []string) (string, error) {
				return checkHomeKPIs(ctx, e, urls[slices.Index(e2eHosts, h)])
			})},
			E2ECheck{ID: "home.kpis.live." + h, Stream: "TS-824", Run: nodesCheck(func(ctx context.Context, e *E2EEnv, urls []string) (string, error) {
				return checkKPIsLive(ctx, e, h, urls[slices.Index(e2eHosts, h)])
			})},
			E2ECheck{ID: "tokens.savings." + h, Stream: "TS-828", Run: nodesCheck(func(ctx context.Context, e *E2EEnv, urls []string) (string, error) {
				return checkTokensSavings(ctx, e, h, urls[slices.Index(e2eHosts, h)])
			})},
		)
	}
	for _, h := range e2eHosts {
		checks = append(checks, E2ECheck{ID: "nodes.data." + h, Stream: "TS-825", Run: checkNodeData(h)})
		checks = append(checks, E2ECheck{ID: "tokens.accounting." + h, Stream: "TS-826", Run: checkTokenAccounting(h)})
	}
	checks = append(checks,
		E2ECheck{ID: "nodes.downsampling", Stream: "TS-825", Run: checkNodeDownsampling},
		E2ECheck{ID: "cluster.members", Stream: "TS-810", Smoke: true, Run: checkMembers},
		E2ECheck{ID: "cluster.jetstream", Stream: "TS-810", Smoke: true, Run: checkJetStream},
		E2ECheck{ID: "cluster.leader", Stream: "TS-810", Smoke: true, Run: checkLeader},
		// Restarting cos.service restarts its embedded NATS server too (covers failure.nats-restart).
		E2ECheck{ID: "cluster.restart", Stream: "TS-810", Chaos: true, Run: checkRestartCatchUp},
		E2ECheck{ID: "cluster.partition", Stream: "TS-810", Chaos: true, Run: checkPartition},
		E2ECheck{ID: "cluster.leader-freeze", Stream: "TS-810", Chaos: true, Run: checkLeaderFreeze},
		// SIGKILL proves a real crash, systemd restart and rejoin from the store, but each run counts
		// as an automatic restart, so it runs only on demand: cos e2e cluster.leader-kill.
		E2ECheck{ID: "cluster.leader-kill", Stream: "TS-810", Chaos: true, OnDemand: true, Run: checkLeaderKill},
		E2ECheck{ID: "rules.heartbeat-leader", Stream: "TS-813", Smoke: true, Run: checkHeartbeatLeader},
		E2ECheck{ID: "rules.heartbeat-presets", Stream: "TS-813", Run: checkHeartbeatPresets},
		E2ECheck{ID: "rules.reconciler-dropped-event", Stream: "TS-813", Run: nodesCheck(reconcilerDroppedEvent)},
		E2ECheck{ID: "rules.red-ci-wake", Stream: "TS-813", Run: pending("TS-813: needs a sandbox PR with red CI and a throwaway thread; covered by TestClassify/TestRulesEngineHandle and the 2026-10-11 sandbox live check (PR #19)")},
		E2ECheck{ID: "rules.ready-merge-once", Stream: "TS-813", Run: pending("TS-813: a real merge cannot run inside e2e; covered by TestClassify merge-ready and gate.sh merges")},
		E2ECheck{ID: "threads.no-haiku", Stream: "TS-812", Run: checkNoHaikuThreads},
		E2ECheck{ID: "failure.t3-schema-change", Stream: "TS-812", Run: checkT3Schema},
		E2ECheck{ID: "failure.forgejo-5xx", Stream: "TS-812", Run: checkForgejo5xx},
		E2ECheck{ID: "failure.t3-down", Stream: "TS-812", Run: checkT3Down},
		E2ECheck{ID: "behavior.advisor-principle", Stream: "TS-817", Run: checkAdvisorPrinciple},
		E2ECheck{ID: "memory.recall", Stream: "TS-828", Run: checkMemoryRecall},
		// These change auth state (failures, lockout, logout, password), so each gets its own throwaway user.
		E2ECheck{ID: "access.wrong-password", Stream: "TS-810", Run: accessCheck(checkWrongPassword)},
		E2ECheck{ID: "access.lockout", Stream: "TS-810", Run: accessCheck(checkLockout)},
		E2ECheck{ID: "access.session-cross-node", Stream: "TS-810", Run: nodesCheck(checkSessionCrossNode)},
		E2ECheck{ID: "access.logout", Stream: "TS-810", Run: accessCheck(checkLogout)},
		E2ECheck{ID: "access.password-change", Stream: "TS-810", Run: accessCheck(checkPasswordChange)},
		E2ECheck{ID: "deploy.reconnect", Stream: "TS-818", Run: nodesCheck(checkReconnect)},
		E2ECheck{ID: "deploy.canary", Stream: "TS-818", Run: checkCanary},
		E2ECheck{ID: "deploy.rollback", Stream: "TS-818", Run: checkRollback},
		E2ECheck{ID: "gate.self-change", Stream: "TS-817", Run: checkSelfGate},
		E2ECheck{ID: "inbox.add-visible", Stream: "TS-815", Run: inboxCheck(checkInboxAddVisible)},
		E2ECheck{ID: "inbox.answer-closes", Stream: "TS-815", Run: inboxCheck(checkInboxAnswerCloses)},
		E2ECheck{ID: "inbox.concurrent-answer", Stream: "TS-815", Run: inboxCheck(checkInboxConcurrentAnswer)},
		E2ECheck{ID: "inbox.dedup", Stream: "TS-815", Run: inboxCheck(checkInboxDedup)},
		E2ECheck{ID: "events.forgejo.valid", Stream: "TS-814", Run: checkForgejoValid},
		E2ECheck{ID: "events.forgejo.bad-signature", Stream: "TS-814", Run: checkForgejoBadSignature},
		E2ECheck{ID: "events.forgejo.replay", Stream: "TS-814", Run: checkForgejoReplay},
		E2ECheck{ID: "events.forgejo.foreign-author", Stream: "TS-814", Run: checkForgejoForeignAuthor},
		E2ECheck{ID: "tokens.budget-fyi", Stream: "TS-826", Run: checkBudgetFYI},
		// The pause spends a few Opus tokens on a throwaway thread, so it runs only when named.
		E2ECheck{ID: "tokens.budget-pause", Stream: "TS-826", OnDemand: true, Run: checkBudgetPause},
		// Last, so the 100 s wait only starts after the rest of the suite has run.
		E2ECheck{ID: "nodes.collector-missing", Stream: "TS-825", Run: checkNodeCollectorMissing},
		// Runs only when named, because it spends an Opus probe run.
		E2ECheck{ID: "inbox.needs-user-forward", Stream: "TS-815", OnDemand: true, Run: inboxCheck(checkInboxNeedsUserForward)},
	)
	return append(append(checks, e2ePending...), e2eRegistered...)
}

func pending(reason string) func(context.Context, *E2EEnv) (string, error) {
	return func(context.Context, *E2EEnv) (string, error) { return "", e2eSkip(reason) }
}
