package cos

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Each node classifies its own journal and stores the result under its host key in restartBucket.
const (
	restartBucket   = "cos_node_restarts"
	restartWindow   = 7 * 24 * time.Hour
	restartInterval = 5 * time.Minute
	restartKeep     = 50
	restartE2E      = 3 * time.Minute // an e2e kill is followed by its automatic restart within this long
	journalTimeout  = 10 * time.Second
)

// NodeRestart is one start of the COS service and what caused it.
type NodeRestart struct {
	Time   time.Time `json:"time"`
	Kind   string    `json:"kind"`   // deploy | e2e | unexpected | manual
	Detail string    `json:"detail"` // e.g. revision, "check=cluster.restart action=restart", systemd's restart message
}

// journalLine is one journal entry; Text keeps the host and identifier so sources can be told apart.
type journalLine struct {
	Time time.Time
	Text string
}

var restartRevision = regexp.MustCompile(`revision=(\S+)`)

// classifyRestarts names every start of the service, oldest first, keeping the newest restartKeep.
// A start is attributed to what happened since the previous start: a deploy first, then an e2e kill
// within restartE2E, then a crash ("Scheduled restart job"), else manual (systemctl restart or host boot).
func classifyRestarts(lines []journalLine) []NodeRestart {
	slices.SortStableFunc(lines, func(a, b journalLine) int { return a.Time.Compare(b.Time) })
	var restarts []NodeRestart
	var deploy, e2e, crash *journalLine
	for i := range lines {
		l := &lines[i]
		switch {
		case strings.Contains(l.Text, "restarting into another build"):
			deploy = l
		case strings.Contains(l.Text, "cos-e2e"):
			e2e = l
		case strings.Contains(l.Text, "Scheduled restart job"):
			crash = l
		case serviceStarted(l.Text):
			restarts = append(restarts, classifyStart(l, deploy, e2e, crash))
			deploy, e2e, crash = nil, nil, nil
		}
	}
	if len(restarts) > restartKeep {
		restarts = restarts[len(restarts)-restartKeep:]
	}
	return restarts
}

// classifyStart names one start from the lines seen since the previous start; nil lines were not seen.
func classifyStart(start, deploy, e2e, crash *journalLine) NodeRestart {
	r := NodeRestart{Time: start.Time}
	switch {
	case deploy != nil:
		r.Kind = "deploy"
		if m := restartRevision.FindStringSubmatch(deploy.Text); m != nil {
			r.Detail = m[1]
		}
	case e2e != nil && start.Time.Sub(e2e.Time) <= restartE2E:
		r.Kind, r.Detail = "e2e", journalMessage(e2e.Text)
	case crash != nil:
		r.Kind, r.Detail = "unexpected", journalMessage(crash.Text)
	default:
		r.Kind = "manual"
	}
	return r
}

// serviceStarted matches systemd's start line for the unit, which names it by its description.
func serviceStarted(text string) bool {
	return strings.Contains(text, "]: Started TerraScale COS") || strings.Contains(text, "]: Started cos.service")
}

// journalMessage is the message after "identifier[pid]: ".
func journalMessage(text string) string {
	_, msg, _ := strings.Cut(text, ": ")
	return msg
}

// parseJournal reads `journalctl -o short-unix` lines: "<epoch seconds> <host> <identifier>[pid]: <message>".
func parseJournal(out string) []journalLine {
	var lines []journalLine
	for _, raw := range strings.Split(out, "\n") {
		stamp, text, ok := strings.Cut(raw, " ")
		secs, err := strconv.ParseFloat(stamp, 64)
		if !ok || err != nil {
			continue // "-- No entries --" and blank lines
		}
		lines = append(lines, journalLine{Time: time.UnixMicro(int64(secs * 1e6)), Text: text})
	}
	return lines
}

// readJournal reads the service's journal and the e2e tag's journal for restartWindow.
func readJournal(ctx context.Context) ([]journalLine, error) {
	service, err := journalctl(ctx, "--user", "-u", "cos.service", "--since", "-7d", "-o", "short-unix", "--no-pager")
	if err != nil {
		return nil, err
	}
	e2e, err := journalctl(ctx, "--user", "-t", "cos-e2e", "--since", "-7d", "-o", "short-unix", "--no-pager")
	if err != nil {
		return nil, err
	}
	return append(parseJournal(service), parseJournal(e2e)...), nil
}

// journalctl runs one foreground query with its own timeout.
func journalctl(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, journalTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "journalctl", args...).Output()
	return string(out), err
}

// restartKV opens the restart bucket; the writer creates it.
func restartKV(ctx context.Context, js jetstream.JetStream, create bool) (jetstream.KeyValue, error) {
	if !create {
		return js.KeyValue(ctx, restartBucket)
	}
	return js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: restartBucket, History: 1, Replicas: 3, Storage: jetstream.FileStorage})
}

// refreshRestarts replaces this host's record with its classified journal. A failed read keeps the old record.
func refreshRestarts(ctx context.Context, js jetstream.JetStream, host string) error {
	lines, err := readJournal(ctx)
	if err != nil {
		return err
	}
	value, err := json.Marshal(classifyRestarts(lines))
	if err != nil {
		return err
	}
	kv, err := restartKV(ctx, js, true)
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, host, value)
	return err
}

// runRestarts refreshes this node's restart history now and every restartInterval until ctx ends.
// Like publishNode, it logs the first failure and again only after a recovery.
func runRestarts(ctx context.Context, js jetstream.JetStream, host string) {
	failing := false
	ticker := time.NewTicker(restartInterval)
	defer ticker.Stop()
	for {
		err := refreshRestarts(ctx, js, host)
		if err != nil && ctx.Err() == nil && !failing {
			slog.Warn("COS node restarts unavailable", "host", host, "error", err)
		}
		failing = err != nil
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// NodeRestarts returns a host's stored restarts, oldest first. A host with no record, or no bucket yet, has none.
func NodeRestarts(ctx context.Context, js jetstream.JetStream, host string) ([]NodeRestart, error) {
	kv, err := restartKV(ctx, js, false)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entry, err := kv.Get(ctx, host)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var restarts []NodeRestart
	return restarts, json.Unmarshal(entry.Value(), &restarts)
}

// recentRestarts reads a host's restarts from the last restartWindow. A failed read is flagged, not fatal.
func recentRestarts(ctx context.Context, js jetstream.JetStream, host string, now time.Time) ([]NodeRestart, bool) {
	stored, err := NodeRestarts(ctx, js, host)
	if err != nil {
		return nil, true
	}
	var recent []NodeRestart
	for _, r := range stored {
		if !r.Time.Before(now.Add(-restartWindow)) {
			recent = append(recent, r)
		}
	}
	return recent, false
}

// restartTally counts restarts by kind.
type restartTally struct{ deploy, e2e, unexpected, manual int }

func tallyRestarts(restarts []NodeRestart) restartTally {
	var n restartTally
	for _, r := range restarts {
		switch r.Kind {
		case "deploy":
			n.deploy++
		case "e2e":
			n.e2e++
		case "unexpected":
			n.unexpected++
		case "manual":
			n.manual++
		}
	}
	return n
}

// unexpectedRestarts is the overview's flag: unexpected restarts in seven days, none when the record is unreadable.
func (c NodeCard) unexpectedRestarts() int {
	if c.restartsErr {
		return 0
	}
	return tallyRestarts(c.Restarts).unexpected
}

// restartStage maps a restart kind to its stage badge state.
func restartStage(kind string) string {
	switch kind {
	case "deploy":
		return "done"
	case "e2e":
		return "review"
	case "unexpected":
		return "failed"
	}
	return "queued"
}
