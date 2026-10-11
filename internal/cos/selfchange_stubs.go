package cos

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Minimal versions of other streams' pieces the self-change loop needs before they
// are on main. Replace each with the real API when its stream merges.

// selfChatPost writes a COS line into TS-811's replicated chat bucket.
// shortcut: copies ChatEntry's JSON shape and key format; use Chat.Put once TS-811 merges.
func selfChatPost(ctx context.Context, bus *Bus, text string) {
	slog.Info("self-change", "chat", text)
	js, err := bus.JetStream()
	if err != nil {
		return
	}
	kv, err := js.KeyValue(ctx, "COS_CHAT")
	if err != nil {
		return // chat not deployed yet; the line is in the log
	}
	at := time.Now().UTC()
	id := fmt.Sprintf("%016x-%s", at.UnixNano(), uuid()[:8])
	data, _ := json.Marshal(map[string]any{"id": id, "at": at, "role": "cos", "text": text})
	if _, err := kv.Put(ctx, id, data); err != nil {
		slog.Warn("self-change chat post failed", "error", err)
	}
}

// TS-818 rollout events (contract from stream I): data.to is the revision now on all 3
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
	last := func(subject string) (Event, map[string]string, bool) {
		m, err := stream.GetLastMsgForSubject(ctx, subject)
		var e Event
		var data map[string]string
		if err != nil || json.Unmarshal(m.Data, &e) != nil || e.Time.Before(r.MergedAt) {
			return e, nil, false
		}
		_ = json.Unmarshal(e.Data, &data)
		return e, data, true
	}
	if _, data, ok := last(selfDeployRolledBack); ok {
		return false, cmp.Or(data["reason"], "rollout rolled back"), nil
	}
	_, data, ok := last(selfDeployCompleted)
	if !ok {
		return false, "", nil
	}
	if data["to"] == r.SHA {
		return true, "", nil
	}
	// A later main commit rolled out; it includes r when r is among its recent ancestors.
	var commits []struct{ SHA string }
	if _, err := d.f.request(ctx, http.MethodGet, selfPath("/commits?sha="+data["to"]+"&limit=50&stat=false&files=false&verification=false"), nil, &commits); err != nil {
		return false, "", err
	}
	for _, c := range commits {
		if c.SHA == r.SHA {
			return true, "", nil
		}
	}
	return false, "", nil
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
