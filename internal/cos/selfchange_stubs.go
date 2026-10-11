package cos

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Minimal versions of other streams' pieces the self-change loop needs before they
// are on main. Replace each with the real API when its stream merges.

// selfChatPost posts a COS line in the replicated chat (TS-811).
func selfChatPost(ctx context.Context, bus *Bus, text string) {
	slog.Info("self-change", "chat", text)
	if _, err := PutChat(ctx, bus, ChatEntry{Role: "cos", Text: text}); err != nil {
		slog.Warn("self-change chat post failed", "error", err)
	}
}

// TS-818 rollout events (cos-hold/ts-818-rollout, not on main yet): data.to is the revision on all 3
// nodes; data.reason explains a rollback.
const (
	selfDeployCompleted  = "cos.events.deploy.deploy.completed"
	selfDeployRolledBack = "cos.events.deploy.deploy.rolled_back"
)

// rolloutDone reports whether r's merge commit is live. Behavior-only changes are
// live when every node runs the published bundle; code waits for TS-818's rollout.
func (d *selfDriver) rolloutDone(ctx context.Context, r selfRecord) (bool, string, error) {
	var files []struct{ Filename string }
	if _, err := d.f.request(ctx, http.MethodGet, selfPath("/pulls/"+strconv.Itoa(r.PR)+"/files?limit=300"), nil, &files); err != nil {
		return false, "", err
	}
	behaviorOnly := len(files) > 0
	for _, f := range files {
		behaviorOnly = behaviorOnly && (strings.HasPrefix(f.Filename, "behavior/") || strings.HasPrefix(f.Filename, "checks/")) && !strings.HasSuffix(f.Filename, ".go")
	}
	js, err := d.bus.JetStream()
	if err != nil {
		return false, "", err
	}
	if behaviorOnly {
		return selfBehaviorLive(ctx, js, r)
	}
	stream, err := js.Stream(ctx, EventsStream)
	if err != nil {
		return false, "", err
	}
	last := func(subject string) (to, reason string, ok bool) {
		m, err := stream.GetLastMsgForSubject(ctx, subject)
		var e Event
		if err != nil || json.Unmarshal(m.Data, &e) != nil || e.Time.Before(r.MergedAt) {
			return "", "", false
		}
		to, reason = selfDeployData(e.Data)
		return to, reason, true
	}
	if _, reason, ok := last(selfDeployRolledBack); ok {
		return false, cmp.Or(reason, "rollout rolled back"), nil
	}
	to, _, ok := last(selfDeployCompleted)
	if !ok {
		return false, "", nil
	}
	if to == r.SHA {
		return true, "", nil
	}
	// A later main commit rolled out; it includes r when r is among its recent ancestors.
	var commits []struct{ SHA string }
	if _, err := d.f.request(ctx, http.MethodGet, selfPath("/commits?sha="+to+"&limit=50&stat=false&files=false&verification=false"), nil, &commits); err != nil {
		return false, "", err
	}
	for _, c := range commits {
		if c.SHA == r.SHA {
			return true, "", nil
		}
	}
	return false, "", nil
}

// selfDeployData reads a TS-818 deploy payload. Its hosts (array) and from (object) fields are
// ignored: decoding them into strings would fail the whole event.
func selfDeployData(raw json.RawMessage) (to, reason string) {
	var data struct {
		To     string `json:"to"`
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(raw, &data)
	return data.To, data.Reason
}

// selfBehaviorLive: the bundle published after the merge is loaded on every node.
func selfBehaviorLive(ctx context.Context, js jetstream.JetStream, r selfRecord) (bool, string, error) {
	kv, err := behaviorBucketKV(ctx, js, false)
	if err != nil {
		return false, "", err
	}
	if e, err := kv.Get(ctx, "rejected"); err == nil {
		var rej struct{ Version, Error string }
		if json.Unmarshal(e.Value(), &rej) == nil && rej.Version == r.SHA {
			return false, "behavior rejected: " + rej.Error, nil
		}
	}
	cur, err := kv.Get(ctx, behaviorKey)
	if err != nil {
		return false, "", nil
	}
	var bundle behaviorBundle
	if json.Unmarshal(cur.Value(), &bundle) != nil || (bundle.Version != r.SHA && cur.Created().Before(r.MergedAt)) {
		return false, "", nil
	}
	hash := behaviorHash(bundle.Files)
	nodes := 0
	keys, err := kv.ListKeys(ctx)
	if err != nil {
		return false, "", err
	}
	for k := range keys.Keys() {
		if !strings.HasPrefix(k, "node.") {
			continue
		}
		e, err := kv.Get(ctx, k)
		var n BehaviorNode
		if err != nil || json.Unmarshal(e.Value(), &n) != nil {
			continue
		}
		if time.Since(n.ReportedAt) > 3*behaviorReport {
			continue // a node gone for good does not hold every rollout
		}
		if n.Hash != hash {
			return false, "", nil
		}
		nodes++
	}
	if nodes < selfNodes {
		return false, "", errors.New("fewer than 3 nodes report behavior status")
	}
	return true, "", nil
}
