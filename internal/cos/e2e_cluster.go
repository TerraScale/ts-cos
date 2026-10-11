package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

type e2eMain struct {
	SHA  string
	Time time.Time
}

func mainHead(ctx context.Context, cfg Config) (e2eMain, error) {
	var branch struct {
		Commit struct {
			ID        string    `json:"id"`
			Timestamp time.Time `json:"timestamp"`
		} `json:"commit"`
	}
	if _, err := (Forgejo{Config: cfg}).request(ctx, "GET", "/repos/TerraScale/ts-cos/branches/main", nil, &branch); err != nil {
		return e2eMain{}, err
	}
	return e2eMain{branch.Commit.ID, branch.Commit.Timestamp}, nil
}

// e2eRestartAwk splits systemd's automatic restarts into deploy installs (the node logged
// "restarting into another build" and exited for systemd to start the new build) and the
// rest: crashes, kills, failed trials.
const e2eRestartAwk = `/restarting into another build/ { deploy = 1 }
/Scheduled restart job/ { if (deploy) installs++; else unexpected++; deploy = 0 }
END { print "AutoRestarts=" unexpected + 0; print "DeployRestarts=" installs + 0 }`

// e2eFacts is one ssh round trip per host: systemd state, the running binary's revision and journal counts.
type e2eFacts map[string]string

const e2eFactsScript = `unit=` + e2eUnit + `
systemctl --user show "$unit" -p LoadState -p ActiveState -p SubState -p NRestarts -p MainPID
pid=$(systemctl --user show "$unit" -p MainPID --value)
if [ "${pid:-0}" != 0 ]; then echo "Version=$(/proc/$pid/exe version 2>&1 | tr -d '\n ')"; fi
journalctl --user -u "$unit" --since -25min -o cat --no-pager 2>/dev/null | awk '` + e2eRestartAwk + `'
# Errors count since the later of 17 min ago and this process's start: 17 min (not the 20 min run
# period) leaves out transient errors from the previous run's own chaos, and the process start
# keeps a rollback from failing on the replaced build's lines (crash loops show in AutoRestarts).
since=$(( $(date +%s) - 1020 ))
start=$(systemctl --user show "$unit" -p ExecMainStartTimestamp --value --timestamp=unix); start=${start#@}
[ "${start:-0}" -gt "$since" ] 2>/dev/null && since=$start
errors=$(journalctl --user -u "$unit" --since "@$since" -o cat --no-pager 2>/dev/null | grep -E 'panic:|fatal error:|\bERROR\b|level=ERROR|^\{"error":')
echo "LogErrors=$(printf '%s' "$errors" | grep -c .)"
echo "LogSample=$(printf '%s' "$errors" | head -1 | cut -c1-200)"
`

func (e *E2EEnv) hostFacts(ctx context.Context, host string, fresh bool) (e2eFacts, error) {
	e.mu.Lock()
	f, ok := e.facts[host]
	e.mu.Unlock()
	if ok && !fresh {
		return f, nil
	}
	out, err := e.run(ctx, host, e2eFactsScript)
	if err != nil {
		return nil, err
	}
	f = e2eFacts{}
	for line := range strings.Lines(out) {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			f[k] = v
		}
	}
	e.mu.Lock()
	if e.facts == nil {
		e.facts = map[string]e2eFacts{}
	}
	e.facts[host] = f
	e.mu.Unlock()
	return f, nil
}

// deployed returns the host's facts, or a skip while TS-810 has not installed the unit there.
func (e *E2EEnv) deployed(ctx context.Context, host string) (e2eFacts, error) {
	f, err := e.hostFacts(ctx, host, false)
	if err != nil {
		return nil, err
	}
	if f["LoadState"] != "loaded" {
		return nil, e2eSkip(fmt.Sprintf("TS-810: %s is not installed on %s (LoadState=%s)", e2eUnit, host, f["LoadState"]))
	}
	return f, nil
}

func checkUnit(ctx context.Context, e *E2EEnv, host string) (string, error) {
	f, err := e.deployed(ctx, host)
	if err != nil {
		return "", err
	}
	state := fmt.Sprintf("%s ActiveState=%s SubState=%s MainPID=%s", host, f["ActiveState"], f["SubState"], f["MainPID"])
	if f["ActiveState"] != "active" || f["SubState"] != "running" {
		return "", errors.New(state)
	}
	return state, nil
}

// mainDeployWindow is how long auto-deploy may take after a push to main before a stale host fails.
const mainDeployWindow = 15 * time.Minute

func checkVersion(ctx context.Context, e *E2EEnv, host string) (string, error) {
	f, err := e.deployed(ctx, host)
	if err != nil {
		return "", err
	}
	head, err := e.mainSHA(ctx)
	if err != nil {
		return "", fmt.Errorf("read origin/main: %w", err)
	}
	var v struct {
		Revision string `json:"revision"`
		Modified bool   `json:"modified"`
	}
	if json.Unmarshal([]byte(f["Version"]), &v) != nil || v.Revision == "" {
		return "", fmt.Errorf("%s: running binary reports no revision: %q", host, f["Version"])
	}
	state := fmt.Sprintf("%s runs %.12s (modified=%t); want %.12s", host, v.Revision, v.Modified, head.SHA)
	switch {
	case v.Revision == head.SHA && !v.Modified:
		return state, nil
	case v.Revision != head.SHA && time.Since(head.Time) < mainDeployWindow:
		return "", e2eSkip(fmt.Sprintf("deploy window: %s, pushed %s ago", state, time.Since(head.Time).Round(time.Second)))
	}
	return "", errors.New(state)
}

// Every automatic restart in the last 25 min must be explained by an e2e sigkill record; deploy
// installs are reported, not counted.
const maxUnattributedRestarts = 0

func checkRestarts(ctx context.Context, e *E2EEnv, host string) (string, error) {
	f, err := e.deployed(ctx, host)
	if err != nil {
		return "", err
	}
	n, err := strconv.Atoi(f["AutoRestarts"])
	if err != nil {
		return "", fmt.Errorf("%s: cannot read journal restarts: %q", host, f["AutoRestarts"])
	}
	attributed := 0
	if n > 0 {
		bus, err := e.bus(ctx)
		if err != nil {
			return "", err
		}
		defer bus.Close()
		if attributed, err = attributedKills(ctx, bus, host, time.Now().Add(-25*time.Minute)); err != nil {
			return "", err
		}
	}
	unattributed := max(0, n-attributed)
	state := fmt.Sprintf("%s: %d unattributed automatic restarts in 25 min (%d from e2e chaos, %s deploy installs), NRestarts=%s", host, unattributed, attributed, f["DeployRestarts"], f["NRestarts"])
	if unattributed > maxUnattributedRestarts {
		return "", errors.New(state)
	}
	return state, nil
}

func checkLogs(ctx context.Context, e *E2EEnv, host string) (string, error) {
	f, err := e.deployed(ctx, host)
	if err != nil {
		return "", err
	}
	if f["LogErrors"] != "0" {
		return "", fmt.Errorf("%s: %s error/panic lines since start (max 17 min), first: %s", host, f["LogErrors"], f["LogSample"])
	}
	return host + ": no error or panic lines since start (max 17 min)", nil
}

// e2eBus connects to the runner's local node once the unit is deployed there.
func (e *E2EEnv) bus(ctx context.Context) (*Bus, error) {
	if _, err := e.deployed(ctx, e.cfg.LocalHost); err != nil {
		return nil, err
	}
	return ConnectBus(ctx, e.cfg)
}

// pingHosts lists the hosts whose cos micro service answers through the local node's routes.
func pingHosts(ctx context.Context, conn *nats.Conn) ([]string, error) {
	inbox := nats.NewInbox()
	sub, err := conn.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	subject, _ := micro.ControlSubject(micro.PingVerb, "cos", "")
	if err := conn.PublishRequest(subject, inbox, nil); err != nil {
		return nil, err
	}
	wait, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var hosts []string
	for {
		msg, err := sub.NextMsgWithContext(wait)
		if err != nil {
			break
		}
		var ping micro.Ping
		if json.Unmarshal(msg.Data, &ping) == nil && ping.Name == "cos" && !slices.Contains(hosts, ping.Metadata["host"]) {
			hosts = append(hosts, ping.Metadata["host"])
		}
	}
	slices.Sort(hosts)
	return hosts, ctx.Err()
}

func checkMembers(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	hosts, err := pingHosts(ctx, bus.conn)
	if err != nil {
		return "", err
	}
	if !slices.Equal(hosts, e2eHosts) {
		return "", fmt.Errorf("micro ping via %s answered by %v, want %v", e.cfg.LocalHost, hosts, e2eHosts)
	}
	return fmt.Sprintf("micro ping via %s answered by %v", e.cfg.LocalHost, hosts), nil
}

// streamRow is one stream's replica state as streamHealth reads it.
type streamRow struct {
	line string // the per-stream detail, listed only when the stream is not ok
	ok   bool
	lag  uint64
}

// streamSummary lists failing streams first; when every stream is ok it is one compact line.
func streamSummary(rows []streamRow) (string, bool) {
	var failing, passing []string
	var maxLag uint64
	healthy := true
	for _, r := range rows {
		maxLag = max(maxLag, r.lag)
		healthy = healthy && r.ok
		if r.ok {
			passing = append(passing, r.line)
		} else {
			failing = append(failing, r.line)
		}
	}
	if healthy {
		return fmt.Sprintf("%d streams R3, all current (max lag %d)", len(rows), maxLag), true
	}
	return strings.Join(append(failing, passing...), "; "), false
}

// streamHealth reports every stream's replica state; healthy means R3, a leader, and all peers current.
// With down set, that host's replica is ignored and must not lead (the majority side of a partition).
func streamHealth(ctx context.Context, conn *nats.Conn, down string) (summary string, healthy bool, count int, err error) {
	js, err := jetstream.New(conn)
	if err != nil {
		return "", false, 0, err
	}
	lister := js.ListStreams(ctx)
	var rows []streamRow
	for info := range lister.Info() {
		c := info.Cluster
		ok := info.Config.Replicas == 3 && c != nil && c.Leader != "" && c.Leader != down && len(c.Replicas) == 2
		var lag uint64
		if c != nil {
			for _, p := range c.Replicas {
				ok = ok && (p.Name == down || p.Current && !p.Offline)
				lag = max(lag, p.Lag)
			}
		}
		leader := ""
		if c != nil {
			leader = c.Leader
		}
		rows = append(rows, streamRow{
			line: fmt.Sprintf("%s R%d leader=%s maxLag=%d ok=%t", info.Config.Name, info.Config.Replicas, leader, lag, ok),
			ok:   ok,
			lag:  lag,
		})
	}
	if err := lister.Err(); err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
		return "", false, len(rows), err
	}
	summary, healthy = streamSummary(rows)
	return summary, healthy, len(rows), nil
}

func checkJetStream(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	summary, healthy, n, err := streamHealth(ctx, bus.conn, "")
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", e2eSkip("TS-814: no JetStream streams on main yet")
	}
	if !healthy {
		return "", errors.New(summary)
	}
	return summary, nil
}

// chaosTarget is a deployed host other than the runner, so the runner's own connection survives.
// With wantLeader it prefers the leader (when another host leads), otherwise it avoids the leader.
func (e *E2EEnv) chaosTarget(ctx context.Context, bus *Bus, wantLeader bool) (string, error) {
	var others []string
	for _, h := range e2eHosts {
		if h == e.cfg.LocalHost {
			continue
		}
		if _, err := e.deployed(ctx, h); err != nil {
			return "", err
		}
		others = append(others, h)
	}
	if len(others) == 0 {
		return "", errors.New("runner is not one of the COS hosts")
	}
	leader, _ := leaderOf(ctx, bus)
	for _, h := range others {
		if (h == leader) == wantLeader {
			return h, nil
		}
	}
	return others[0], nil
}

// waitCluster polls until exactly the want hosts answer and every stream is healthy without down, or the timeout elapses.
func waitCluster(ctx context.Context, conn *nats.Conn, want []string, down string, timeout time.Duration) (string, time.Duration, error) {
	start := time.Now()
	for {
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		hosts, err := pingHosts(call, conn)
		summary, healthy, _, serr := streamHealth(call, conn, down)
		cancel()
		if err == nil && serr == nil && healthy && slices.Equal(hosts, want) {
			return summary, time.Since(start), nil
		}
		if time.Since(start) > timeout || ctx.Err() != nil {
			return "", time.Since(start), fmt.Errorf("after %s: hosts %v (want %v), streams: %s %v", time.Since(start).Round(time.Second), hosts, want, summary, errors.Join(err, serr))
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

// nodeStatus asks one host's own release.health endpoint: "ok" when it reports no problems, else why not.
func nodeStatus(ctx context.Context, conn *nats.Conn, host string) string {
	msg, err := conn.RequestWithContext(ctx, "cos."+host+".release.health", nil)
	switch {
	case err != nil:
		return "unreachable: " + err.Error()
	case msg.Header.Get("Nats-Service-Error") != "":
		return msg.Header.Get("Nats-Service-Error")
	}
	var health NodeHealth
	if err := json.Unmarshal(msg.Data, &health); err != nil {
		return "bad reply: " + err.Error()
	}
	if len(health.Problems) > 0 {
		return strings.Join(health.Problems, "; ")
	}
	return "ok"
}

// allHealthy reports whether every host is "ok" and joins the per-host statuses in host order for evidence.
func allHealthy(status map[string]string) (bool, string) {
	ok := len(status) > 0
	var parts []string
	for _, h := range slices.Sorted(maps.Keys(status)) {
		ok = ok && status[h] == "ok"
		parts = append(parts, h+"="+status[h])
	}
	return ok, strings.Join(parts, " ")
}

// hostStatus reads every host's health once and reports whether all are ok, with the per-host evidence.
func (e *E2EEnv) hostStatus(ctx context.Context, bus *Bus) (bool, string) {
	status := make(map[string]string, len(e2eHosts))
	for _, h := range e2eHosts {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		status[h] = nodeStatus(call, bus.conn, h)
		cancel()
	}
	return allHealthy(status)
}

// rolloutSettle is how long after a rollout finishes its last gate still counts as fresh (TS-816).
const rolloutSettle = 2 * time.Minute

// rolloutHold returns why the last rollout stops a perturbation now, or "" when none does. A rollout
// restarts nodes while it is active, and right after it ends the cluster may still be settling.
func rolloutHold(r Rollout, now time.Time) string {
	if r.ID == "" {
		return ""
	}
	age := now.Sub(r.Since).Round(time.Second)
	terminal := r.Phase == phaseCompleted || r.Phase == phaseRolledBack || r.Phase == phaseSuperseded
	if terminal && age >= rolloutSettle {
		return ""
	}
	return fmt.Sprintf("rollout %s %s (%s ago), not perturbing", r.ID, r.Phase, age)
}

// rolloutGuard reads the last rollout, the one that decides whether a chaos check may perturb now.
func rolloutGuard(ctx context.Context, bus *Bus) (Rollout, error) {
	js, err := bus.JetStream()
	if err != nil {
		return Rollout{}, err
	}
	kv, err := deployKV(ctx, js)
	if err != nil {
		return Rollout{}, err
	}
	r, _, err := loadRollout(ctx, kv)
	return r, err
}

// noRollout skips the check while the last rollout is active or only just finished (TS-816). The pre-signal
// polls use it, so a rollout that starts right before a signal still skips rather than waits.
func (e *E2EEnv) noRollout(ctx context.Context, bus *Bus) error {
	r, err := rolloutGuard(ctx, bus)
	if err != nil {
		return err
	}
	if reason := rolloutHold(r, time.Now()); reason != "" {
		return e2eSkip(reason)
	}
	return nil
}

// waitRollout re-reads the last rollout every poll while it holds a chaos check, for up to budget. It returns
// the rollout last read, the time waited and a read error.
func waitRollout(ctx context.Context, budget, poll time.Duration, look func() (Rollout, error), now func() time.Time) (Rollout, time.Duration, error) {
	var r Rollout
	var err error
	waited := waitFor(ctx, budget, poll, func() bool {
		r, err = look()
		return err != nil || rolloutHold(r, now()) == ""
	})
	return r, waited, err
}

// waitNoRollout is noRollout at the start of a chaos check: it waits for the rollout to finish and settle,
// within the budget the run shares, and skips only when that runs out (TS-816).
func (e *E2EEnv) waitNoRollout(ctx context.Context, bus *Bus) error {
	look := func() (Rollout, error) { return rolloutGuard(ctx, bus) }
	r, waited, err := waitRollout(ctx, e2eChaosWait-e.chaosWaited, e2eChaosPoll, look, time.Now)
	e.chaosWaited += waited
	e.waited += waited
	if err != nil {
		return err
	}
	if rolloutHold(r, time.Now()) != "" {
		return e2eSkip(fmt.Sprintf("rollout %s %s still blocking after waiting %s", r.ID, r.Phase, waited.Round(time.Second)))
	}
	return nil
}

// preSignalEvidence decides the two pre-signal polls: every node must be ok at both, else skip with
// both readings. A single good poll can land between two restarts, so one is not enough (TS-816).
func preSignalEvidence(ok1 bool, ev1 string, ok2 bool, ev2 string) (string, error) {
	if !ok1 || !ok2 {
		return "", e2eSkip(fmt.Sprintf("JetStream not fully current on every node at both pre-signal polls, not perturbing: poll 1: %s; poll 2: %s",
			e2eClip(ev1, 300), e2eClip(ev2, 300)))
	}
	return fmt.Sprintf("pre-signal health (2 polls, 5s apart): %s; %s", ev1, ev2), nil
}

// preSignal is the last look before a perturbation: no rollout, and every node current on two polls
// 5 s apart. It returns the health evidence for the check's pass line.
func (e *E2EEnv) preSignal(ctx context.Context, bus *Bus) (string, error) {
	if err := e.noRollout(ctx, bus); err != nil {
		return "", err
	}
	ok1, ev1 := e.hostStatus(ctx, bus)
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(5 * time.Second):
	}
	if err := e.noRollout(ctx, bus); err != nil {
		return "", err
	}
	ok2, ev2 := e.hostStatus(ctx, bus)
	return preSignalEvidence(ok1, ev1, ok2, ev2)
}

// allCurrent waits until every node reports JetStream current on its own, so a perturbation never
// lands on a cluster still catching up from the last one (TS-816). It skips, not fails, on timeout.
// Call it before choosing the target: a leader picked earlier can move while this waits.
func (e *E2EEnv) allCurrent(ctx context.Context, bus *Bus, step string) (time.Duration, error) {
	start := time.Now()
	deadline := start.Add(120 * time.Second)
	for {
		ok, evidence := e.hostStatus(ctx, bus)
		if ok {
			return time.Since(start), nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return time.Since(start), e2eSkip(fmt.Sprintf("JetStream not fully current on every node before %s after %s, not perturbing: %s",
				step, time.Since(start).Round(time.Second), e2eClip(evidence, 300)))
		}
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

// gateEvidence is the health gate's wait, appended to a check's pass evidence.
func gateEvidence(waited time.Duration) string {
	return "health gate waited " + waited.Round(100*time.Millisecond).String()
}

func checkRestartCatchUp(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	if err := e.waitNoRollout(ctx, bus); err != nil {
		return "", err
	}
	gate, err := e.allCurrent(ctx, bus, "cluster.restart")
	if err != nil {
		return "", err
	}
	target, err := e.chaosTarget(ctx, bus, false)
	if err != nil {
		return "", err
	}
	if _, _, err := waitCluster(ctx, bus.conn, e2eHosts, "", 10*time.Second); err != nil {
		return "", fmt.Errorf("cluster unhealthy before restart, not perturbing it: %w", err)
	}
	pre, err := e.preSignal(ctx, bus)
	if err != nil {
		return "", err
	}
	if err := e.chaos(ctx, bus, target, "cluster.restart", "restart"); err != nil {
		return "", err
	}
	if _, err := e.run(ctx, target, "systemctl --user restart "+e2eUnit); err != nil {
		return "", err
	}
	summary, took, err := waitCluster(ctx, bus.conn, e2eHosts, "", 60*time.Second)
	if err != nil {
		return "", fmt.Errorf("%s restarted but did not catch up: %w", target, err)
	}
	return fmt.Sprintf("%s restarted and caught up in %s; %s; %s; %s", target, took.Round(100*time.Millisecond), summary, gateEvidence(gate), pre), nil
}

// e2eHeal removes every cos-e2e firewall rule; it is idempotent and also runs from a root timer.
const e2eHeal = `for t in iptables ip6tables; do for c in INPUT OUTPUT; do
while r=$(sudo $t -w -S $c | grep -m1 -- '--comment cos-e2e'); do sudo $t -w $(echo "$r" | sed 's/^-A /-D /') || break; done
done; done`

// partitionScript blackholes all TCP between host and peers except ssh, with a 120 s root
// timer that heals the partition even if this runner dies. The timer runs the heal script from a
// root-owned file because systemd-run would expand its $variables inline.
func partitionScript(peers []netip.Addr) string {
	var b strings.Builder
	b.WriteString("set -e\n" + e2eHeal + "\n")
	for _, ip := range peers {
		t := "iptables"
		if ip.Is6() {
			t = "ip6tables"
		}
		fmt.Fprintf(&b, "sudo %[1]s -w -I INPUT 1 -s %[2]s -p tcp ! --dport 22 -m comment --comment cos-e2e -j DROP\n", t, ip)
		fmt.Fprintf(&b, "sudo %[1]s -w -I OUTPUT 1 -d %[2]s -p tcp ! --sport 22 -m comment --comment cos-e2e -j DROP\n", t, ip)
	}
	b.WriteString("sudo tee /run/cos-e2e-heal.sh >/dev/null <<'COS_E2E_HEAL'\n" + e2eHeal + "\nCOS_E2E_HEAL\n")
	// NixOS services get a minimal PATH; reuse the caller's so sudo, grep and iptables resolve.
	b.WriteString("sudo systemd-run --quiet --collect --on-active=120 --setenv=PATH=\"$PATH\" bash /run/cos-e2e-heal.sh\n")
	return b.String()
}

// hostAddrs lists a host's own unicast addresses (LAN, tailnet, NetBird) as the host reports them.
func (e *E2EEnv) hostAddrs(ctx context.Context, host string) ([]netip.Addr, error) {
	out, err := e.run(ctx, host, "ip -o addr show scope global | awk '{print $4}'")
	if err != nil {
		return nil, err
	}
	var addrs []netip.Addr
	for _, f := range strings.Fields(out) {
		if p, err := netip.ParsePrefix(f); err == nil && !p.Addr().IsLoopback() && !p.Addr().IsLinkLocalUnicast() {
			addrs = append(addrs, p.Addr())
		}
	}
	return addrs, nil
}

func checkPartition(ctx context.Context, e *E2EEnv) (string, error) {
	bus, err := e.bus(ctx)
	if err != nil {
		return "", err
	}
	defer bus.Close()
	if err := e.waitNoRollout(ctx, bus); err != nil {
		return "", err
	}
	gate, err := e.allCurrent(ctx, bus, "cluster.partition")
	if err != nil {
		return "", err
	}
	// Partition the leader when another host leads: the majority must elect a new one and the
	// old leader must step down (no split brain).
	target, err := e.chaosTarget(ctx, bus, true)
	if err != nil {
		return "", err
	}
	leader, err := leaderOf(ctx, bus)
	if err != nil {
		return "", fmt.Errorf("no leader before partition, not perturbing it: %w", err)
	}
	if _, _, err := waitCluster(ctx, bus.conn, e2eHosts, "", 10*time.Second); err != nil {
		return "", fmt.Errorf("cluster unhealthy before partition, not perturbing it: %w", err)
	}
	// Block every address the peers own, whichever network the routes use; skip addresses the
	// target shares (such as a default container bridge) so its local traffic keeps working.
	own, err := e.hostAddrs(ctx, target)
	if err != nil {
		return "", err
	}
	var peers []netip.Addr
	var rest []string
	for _, h := range e2eHosts {
		if h == target {
			continue
		}
		rest = append(rest, h)
		addrs, err := e.hostAddrs(ctx, h)
		if err != nil {
			return "", err
		}
		for _, a := range addrs {
			if !slices.Contains(own, a) && !slices.Contains(peers, a) {
				peers = append(peers, a)
			}
		}
	}
	pre, err := e.preSignal(ctx, bus)
	if err != nil {
		return "", err
	}
	if err := e.chaos(ctx, bus, target, "cluster.partition", "partition"); err != nil {
		return "", err
	}
	// Heal with a fresh context so a cancelled run never leaves the partition in place.
	defer func() {
		heal, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = e.run(heal, target, e2eHeal)
	}()
	if _, err := e.run(ctx, target, partitionScript(peers)); err != nil {
		return "", fmt.Errorf("partition %s: %w", target, err)
	}
	// Majority side: the target drops out and every stream keeps (or re-elects) a leader.
	split, tookSplit, err := waitCluster(ctx, bus.conn, rest, target, 30*time.Second)
	if err != nil {
		return "", fmt.Errorf("majority did not keep serving with %s partitioned: %w", target, err)
	}
	lead := "leader " + leader + " kept the lease"
	if leader == target {
		start := time.Now()
		next, _, err := e.agreedLeader(ctx, rest, target, e2eHandover)
		took := time.Since(start)
		if err != nil {
			return "", fmt.Errorf("majority did not replace partitioned leader %s: %w", target, err)
		}
		lead = fmt.Sprintf("%s took over from partitioned leader %s in %s", next, target, took.Round(100*time.Millisecond))
	}
	if _, err := e.run(ctx, target, e2eHeal); err != nil {
		return "", fmt.Errorf("heal %s: %w", target, err)
	}
	summary, tookHeal, err := waitCluster(ctx, bus.conn, e2eHosts, "", 60*time.Second)
	if err != nil {
		return "", fmt.Errorf("%s did not rejoin after the partition healed: %w", target, err)
	}
	// Split brain: after healing, every node, the old leader included, must name one leader.
	if _, views, err := e.agreedLeader(ctx, e2eHosts, "", 10*time.Second); err != nil {
		return "", fmt.Errorf("split brain after healing %s: %w (views %v)", target, err, views)
	}
	return fmt.Sprintf("partitioned %s: %v served after %s (%s); %s; rejoined in %s (%s); one leader after heal; %s; %s",
		target, rest, tookSplit.Round(100*time.Millisecond), split, lead, tookHeal.Round(100*time.Millisecond), summary, gateEvidence(gate), pre), nil
}
