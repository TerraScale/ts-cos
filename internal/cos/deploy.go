package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

const (
	sourceRepo = "ssh://git@git.terrascale.tech/TerraScale/ts-cos.git"
	cosModule  = "git.terrascale.tech/TerraScale/ts-cos"
)

// busOps drives hosts through their cos.<host>.release.* endpoints.
type busOps struct {
	b   *Bus
	cfg Config
}

func (b *Bus) releaseCall(ctx context.Context, host, verb, revision string, out any) error {
	timeout := 2 * time.Minute // install and restore fetch and check a build
	if verb == "health" || verb == "version" || verb == "commit" {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	data, _ := json.Marshal(releaseRequest{Revision: revision})
	msg, err := b.conn.RequestWithContext(ctx, "cos."+host+".release."+verb, data)
	if err != nil {
		return fmt.Errorf("%s is not answering", host)
	}
	if e := msg.Header.Get(micro.ErrorHeader); e != "" {
		return errors.New(e)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(msg.Data, out)
}

func (o busOps) health(ctx context.Context, host string) (NodeHealth, error) {
	var h NodeHealth
	err := o.b.releaseCall(ctx, host, "health", "", &h)
	return h, err
}

func (o busOps) release(ctx context.Context, host, verb, revision string) error {
	return o.b.releaseCall(ctx, host, verb, revision, nil)
}

// smoke checks the host serves COS operations from the new build (its micro service
// answers discovery with that revision), then runs TS-816's smoke subset for it.
func (o busOps) smoke(ctx context.Context, host, revision string) error {
	if err := o.servesOperations(ctx, host, revision); err != nil {
		return err
	}
	// Only the host's own checks gate: a cluster-wide check that already failed before
	// this build would block every deploy, including its fix. Cluster membership and
	// JetStream sync are part of the host's health.
	return hostSmokeFailure(E2ESmoke(ctx, o.cfg, host, revision), host)
}

func hostSmokeFailure(report E2EReport, host string) error {
	for _, c := range report.Checks {
		// The log scan fails on the one startup ERROR other components print while
		// JetStream warms up, which would roll back every build; the e2e suite keeps it.
		if c.Status == "fail" && strings.HasSuffix(c.ID, "."+host) && !strings.HasPrefix(c.ID, "deploy.logs.") {
			return fmt.Errorf("%s: %s", c.ID, c.Evidence)
		}
	}
	return nil
}

func (o busOps) servesOperations(ctx context.Context, host, revision string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	inbox := nats.NewInbox()
	sub, err := o.b.conn.SubscribeSync(inbox)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	subject, _ := micro.ControlSubject(micro.PingVerb, "cos", "")
	if err := o.b.conn.PublishRequest(subject, inbox, nil); err != nil {
		return err
	}
	for {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			return errors.New("COS operations service is not registered")
		}
		var ping micro.Ping
		if json.Unmarshal(msg.Data, &ping) == nil && ping.Metadata["host"] == host {
			if v := ping.Metadata["version"]; v != revision {
				return fmt.Errorf("COS operations service runs %.12s", v)
			}
			return nil
		}
	}
}

// source runs git and go in this host's build checkout of ts-cos.
type source struct {
	cfg Config
	dir string
}

func sourceOf(cfg Config) source { return source{cfg, filepath.Join(cfg.Home, ".local/share/cos/src")} }

func (s source) run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = s.dir
	cmd.Env = append(os.Environ(), "GIT_SSH_COMMAND=ssh -o BatchMode=yes", "GIT_TERMINAL_PROMPT=0", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %v: %s", name, args[0], err, deployLine(string(out), 300))
	}
	return strings.TrimSpace(string(out)), nil
}

// resolve fetches every branch and returns ref's revision and commit subject.
func (s source) resolve(ctx context.Context, ref string) (revision, change string, err error) {
	if _, err := os.Stat(filepath.Join(s.dir, ".git")); err != nil {
		if err := os.MkdirAll(s.dir, 0o700); err != nil {
			return "", "", err
		}
		if _, err := s.run(ctx, "git", "init", "-q"); err != nil {
			return "", "", err
		}
		if _, err := s.run(ctx, "git", "remote", "add", "origin", sourceRepo); err != nil {
			return "", "", err
		}
	}
	if _, err := s.run(ctx, "git", "fetch", "-q", "--prune", "origin", "+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return "", "", err
	}
	if revision, err = s.run(ctx, "git", "rev-parse", "--verify", "-q", "origin/"+ref+"^{commit}"); err != nil {
		if revision, err = s.run(ctx, "git", "rev-parse", "--verify", "-q", ref+"^{commit}"); err != nil {
			return "", "", fmt.Errorf("unknown branch or commit %q", ref)
		}
	}
	subject, err := s.run(ctx, "git", "log", "-1", "--format=%s", revision)
	if err != nil {
		return "", "", err
	}
	note, err := s.run(ctx, "git", "log", "-1", "--format=%(trailers:key=Update-Note,valueonly,separator=%x20)", revision)
	return revision, changeText(note, subject), err
}

// build compiles a resolved revision from a clean checkout into the cos_builds object
// store, unless it is there already.
func (s source) build(ctx context.Context, js jetstream.JetStream, revision, change string) error {
	store, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: buildsBucket, Replicas: 3, Storage: jetstream.FileStorage})
	if err != nil {
		return fmt.Errorf("build store unavailable: %w", err)
	}
	if _, err := store.GetInfo(ctx, revision); err == nil {
		return nil
	}
	for _, args := range [][]string{{"checkout", "-q", "--force", "--detach", revision}, {"clean", "-q", "-fdx"}} {
		if _, err := s.run(ctx, "git", args...); err != nil {
			return err
		}
	}
	out := filepath.Join(s.cfg.Home, ".local/share/cos/build.tmp")
	defer os.Remove(out)
	stamp := "-X " + cosModule + "/internal/cos.buildTime=" + time.Now().UTC().Format(time.RFC3339)
	if _, err := s.run(ctx, "nice", "-n", "19", "go", "build", "-trimpath", "-ldflags", stamp, "-o", out, "."); err != nil {
		return err
	}
	if err := preflight(ctx, out, revision); err != nil {
		return err
	}
	if err := canRollOut(ctx, out); err != nil {
		return fmt.Errorf("build %.12s %w", revision, err)
	}
	f, err := os.Open(out)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := store.Put(ctx, jetstream.ObjectMeta{Name: revision, Description: change}, f); err != nil {
		return fmt.Errorf("storing build: %w", err)
	}
	pruneBuildStore(ctx, store)
	return nil
}

// rolloutProtocol is the rollout controller's level; bump it when a controller change
// must not be lost again. 2: walks main to the newest green commit and gates without
// the startup log scan.
const rolloutProtocol = 2

// canRollOut refuses a build whose rollout controller is older than this one: once every
// node ran it, later merges could stop reaching the nodes (2026-10-11: a build without a
// controller stranded the cluster).
func canRollOut(ctx context.Context, bin string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "version").Output()
	var v Build
	if err != nil || json.Unmarshal(out, &v) != nil {
		return errors.New("does not start")
	}
	if v.Rollout < rolloutProtocol {
		return fmt.Errorf("has rollout controller level %d, this cluster needs %d; deploying it would stop automatic deploys", v.Rollout, rolloutProtocol)
	}
	return nil
}

// pruneBuildStore keeps the newest keepBuilds builds; hosts keep their own copies of what they run.
func pruneBuildStore(ctx context.Context, store jetstream.ObjectStore) {
	objects, err := store.List(ctx)
	if err != nil {
		return
	}
	slices.SortFunc(objects, func(a, b *jetstream.ObjectInfo) int { return b.ModTime.Compare(a.ModTime) })
	for _, o := range objects[min(keepBuilds, len(objects)):] {
		_ = store.Delete(ctx, o.Name)
	}
}

// newestGreenMain returns the newest commit on main whose CI is green and that never
// failed a rollout. During a merge train main's head is nearly always still in CI, so
// waiting for the head alone would never deploy. The walk stops at current, the build
// the cluster already runs, so it never picks anything older.
func newestGreenMain(ctx context.Context, cfg Config, current string, failed func(string) bool) (string, error) {
	f := Forgejo{Config: cfg}
	var commits []struct {
		SHA string `json:"sha"`
	}
	if _, err := f.request(ctx, http.MethodGet, "/repos/"+forgejoOrg+"/ts-cos/commits?sha=main&limit=30&stat=false&verification=false&files=false", nil, &commits); err != nil {
		return "", err
	}
	for _, c := range commits {
		if c.SHA == current {
			return current, nil
		}
		if !validRevision(c.SHA) || failed(c.SHA) {
			continue
		}
		var status struct {
			State string `json:"state"`
			Total int    `json:"total_count"`
		}
		if _, err := f.request(ctx, http.MethodGet, "/repos/"+forgejoOrg+"/ts-cos/commits/"+c.SHA+"/status", nil, &status); err != nil {
			return "", err
		}
		if status.State == "success" && status.Total > 0 {
			return c.SHA, nil
		}
	}
	return "", errors.New("no green main commit to deploy yet")
}

func newRollout(revision, change string, hosts []string, now time.Time) Rollout {
	return Rollout{ID: uuid(), To: revision, Change: change, Hosts: hosts, Phase: phaseInstall,
		From: map[string]string{}, Started: now, Since: now}
}

// beginRollout plans, builds and records a rollout of ref to every host not on it yet.
func beginRollout(ctx context.Context, cfg Config, bus *Bus, kv jetstream.KeyValue, ref, leader string) (Rollout, error) {
	js, err := bus.JetStream()
	if err != nil {
		return Rollout{}, err
	}
	current, _, err := loadRollout(ctx, kv)
	if err != nil {
		return Rollout{}, err
	}
	if current.Active() {
		return Rollout{}, fmt.Errorf("rollout %s of %.12s is already %s; one rollout at a time", current.ID, current.To, current.Phase)
	}
	src := sourceOf(cfg)
	revision, change, err := src.resolve(ctx, ref)
	if err != nil {
		return Rollout{}, err
	}
	hosts, err := bus.Hosts(ctx)
	if err != nil {
		return Rollout{}, err
	}
	plan, err := planRollout(ctx, busOps{bus, cfg}, hosts, leader, revision)
	if err != nil {
		return Rollout{}, err
	}
	if len(plan) == 0 {
		return Rollout{}, fmt.Errorf("every host already runs %.12s", revision)
	}
	if err := src.build(ctx, js, revision, change); err != nil {
		return Rollout{}, buildError{err}
	}
	r := newRollout(revision, change, plan, time.Now().UTC())
	if err := startRollout(ctx, kv, r); err != nil {
		return Rollout{}, err
	}
	publishDeploy(ctx, js, r, rolloutEvent{Kind: KindDeployStarted})
	return r, nil
}

// rolledBackFYI tells the user in the Inbox; nobody has to act. Its key makes a resumed leader's repeat a no-op.
func rolledBackFYI(ctx context.Context, bus *Bus, host string, r Rollout) error {
	inbox, err := OpenInbox(ctx, bus, host, true)
	if err != nil {
		return err
	}
	var restored []string
	for h, revision := range r.From {
		restored = append(restored, fmt.Sprintf("%s on %.12s", h, revision))
	}
	slices.Sort(restored)
	_, err = inbox.Add(ctx, InboxInput{Kind: "fyi", Source: "deploy", Key: "deploy:" + r.ID,
		Title: deployLine("Deploy rolled back: "+r.Change, inboxTitleMax),
		Context: fmt.Sprintf("Build %.12s failed its health gate: %s.\nRestored: %s. Nothing to do: a newer green main build deploys by itself.",
			r.To, deployLine(r.Failure, 1000), strings.Join(restored, ", "))})
	return err
}

func failedKey(revision string) string { return "failed." + revision }

// buildError marks a revision that cannot be built or does not start, as opposed to a cluster not ready for a rollout.
type buildError struct{ error }

func (e buildError) Unwrap() error { return e.error }

// RunRollouts is the leader's deploy loop (run it through Bus.Lead): it drives the
// active rollout and starts one when main has a new green build.
func RunRollouts(ctx context.Context, cfg Config, bus *Bus) {
	js, err := bus.JetStream()
	if err != nil {
		return
	}
	var kv jetstream.KeyValue
	var checked time.Time
	tick := time.NewTicker(rolloutTick)
	defer tick.Stop()
	for {
		// A leader on trial drives too: the gate trusts each node's own health report
		// anyway, and waiting would stall the rollout until its trial expires.
		if kv == nil && EnsureEventStreams(ctx, js) == nil {
			kv, _ = deployKV(ctx, js)
		}
		if kv != nil {
			err := driveRollout(ctx, cfg, bus, js, kv, &checked)
			if errors.Is(err, errHandover) {
				slog.Info("handing leadership to an updated node before updating this one")
				bus.Resign()
				return
			}
			if err != nil && ctx.Err() == nil {
				slog.Warn("rollout step failed", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func driveRollout(ctx context.Context, cfg Config, bus *Bus, js jetstream.JetStream, kv jetstream.KeyValue, checked *time.Time) error {
	r, revision, err := loadRollout(ctx, kv)
	if err != nil {
		return err
	}
	if !r.Active() {
		if time.Since(*checked) < mainCheckEvery {
			return nil
		}
		*checked = time.Now()
		current := ""
		if r.Phase == phaseCompleted {
			current = r.To
		}
		head, err := newestGreenMain(ctx, cfg, current, func(revision string) bool {
			_, err := kv.Get(ctx, failedKey(revision))
			return err == nil
		})
		if err != nil || head == current {
			return nil
		}
		hosts, err := bus.Hosts(ctx)
		if err != nil {
			return err
		}
		if plan, err := planRollout(ctx, busOps{bus, cfg}, hosts, cfg.LocalHost, head); err != nil || len(plan) == 0 {
			return err
		}
		started, err := beginRollout(ctx, cfg, bus, kv, head, cfg.LocalHost)
		if errors.As(err, new(buildError)) {
			// A build that does not compile or start is never retried by itself either.
			_, _ = kv.Put(ctx, failedKey(head), []byte(err.Error()))
		}
		if err != nil {
			return err
		}
		slog.Info("rollout started", "id", started.ID, "revision", started.To, "hosts", started.Hosts)
		return nil
	}
	event, err := step(ctx, busOps{bus, cfg}, &r, cfg.LocalHost, time.Now().UTC())
	if err != nil {
		return err
	}
	if _, err := saveRollout(ctx, kv, r, revision); err != nil {
		return fmt.Errorf("rollout record changed underneath; reloading: %w", err)
	}
	if event != nil {
		publishDeploy(ctx, js, r, *event)
	}
	switch r.Phase {
	case phaseCompleted:
		// Open tabs reload into the new build once every node runs it.
		release, _ := json.Marshal(Release{Revision: r.To, Change: r.Change})
		if _, err := kv.Put(ctx, releaseKey, release); err != nil {
			slog.Warn("release not recorded", "error", err)
		}
		publishDeploy(ctx, js, r, rolloutEvent{Kind: KindDeployCompleted})
	case phaseRolledBack:
		// Only a newer main build, or `cos deploy`, tries again.
		_, _ = kv.Put(ctx, failedKey(r.To), []byte(r.Failure))
		publishDeploy(ctx, js, r, rolloutEvent{Kind: KindDeployRolledBack, Reason: r.Failure})
		if err := rolledBackFYI(ctx, bus, cfg.LocalHost, r); err != nil {
			slog.Warn("deploy rollback not added to the Inbox", "error", err)
		}
	}
	return nil
}

func init() {
	cliCommands["deploy"] = cliCommand{Usage: "deploy [<branch|commit>|status]", Run: deployCommand}
}

// deployCommand: `cos deploy [<branch|commit>]` rolls a build out now; `cos deploy status` shows the last rollout.
func deployCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	if len(args) > 1 {
		return errors.New("usage: cos deploy [<branch|commit>|status]")
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return err
	}
	kv, err := deployKV(ctx, js)
	if err != nil {
		return err
	}
	if len(args) == 1 && args[0] == "status" {
		r, _, err := loadRollout(ctx, kv)
		if err != nil {
			return err
		}
		return output(w, r)
	}
	ref := "main"
	if len(args) == 1 {
		ref = args[0]
	}
	leader, err := bus.Leader(ctx)
	if err != nil {
		return err
	}
	r, err := beginRollout(ctx, cfg, bus, kv, ref, leader)
	if err != nil {
		return err
	}
	return output(w, r)
}
