package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// A tab reloads once when the cluster's release changes, which is only when a rollout
// completes, so it never flaps between nodes on different builds mid-rollout or after
// a rollback. The release is "release" in the cos_deploy KV.
const releaseKey = "release"

type Release struct {
	Revision string `json:"revision"`
	Change   string `json:"change"` // what the user sees in "Updated: …"
}

type releaseFeed struct {
	mu      sync.Mutex
	cfg     Config
	current Release
	clients map[chan struct{}]struct{}
}

var clusterRelease = &releaseFeed{clients: map[chan struct{}]struct{}{}}

func (f *releaseFeed) get() Release {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *releaseFeed) set(r Release) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.current = r
	for c := range f.clients {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

// watch follows the release key until ctx ends, reopening the watch when the cluster is unavailable.
func (f *releaseFeed) watch(ctx context.Context, cfg Config, js jetstream.JetStream) {
	f.mu.Lock()
	f.cfg = cfg
	f.mu.Unlock()
	for ctx.Err() == nil {
		if kv, err := deployKV(ctx, js); err == nil {
			if w, err := kv.Watch(ctx, releaseKey); err == nil {
				for entry := range w.Updates() {
					var r Release
					if entry != nil && entry.Operation() == jetstream.KeyValuePut && json.Unmarshal(entry.Value(), &r) == nil {
						f.set(r)
					}
				}
				_ = w.Stop()
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
}

// events streams the release as the _release Datastar signal; nothing is sent until it is known.
func (f *releaseFeed) events(rw http.ResponseWriter, r *http.Request) {
	controller := http.NewResponseController(rw)
	notify := make(chan struct{}, 1)
	f.mu.Lock()
	f.clients[notify] = struct{}{}
	f.mu.Unlock()
	defer func() { f.mu.Lock(); delete(f.clients, notify); f.mu.Unlock() }()
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-store")
	rw.Header().Set("X-Accel-Buffering", "no")
	send := func() error {
		if release := f.get(); release.Revision != "" {
			signals, _ := json.Marshal(map[string]string{"_release": release.Revision})
			_, err := fmt.Fprintf(rw, "event: datastar-patch-signals\ndata: signals %s\n\n", signals)
			if err != nil {
				return err
			}
		} else if _, err := fmt.Fprint(rw, ": release unknown\n\n"); err != nil {
			return err
		}
		return controller.Flush()
	}
	if send() != nil {
		return
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-notify:
		case <-heartbeat.C:
		}
		if send() != nil {
			return
		}
	}
}

// version answers GET /version: this node's build and the cluster release.
func (f *releaseFeed) version(rw http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	cfg := f.cfg
	f.mu.Unlock()
	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(rw).Encode(map[string]any{"build": CurrentBuild(cfg), "release": f.get()})
}

// sseGet opens a long-lived Datastar SSE stream that reconnects after a clean end or an
// error page too, so a tab follows a restarting node to another one during a deploy.
func sseGet(path string) string {
	return "@get('" + path + "', {retry: 'always', retryMaxCount: 1000})"
}

// The tab remembers the first release it sees; a later different one marks the
// reload as an update and reloads.
const releaseEffect = `$_release && ($_releaseSeen ? $_release != $_releaseSeen && (sessionStorage.cosUpdated = '1', location.reload()) : ($_releaseSeen = $_release))`

// After the reload the new page shows the change for a few seconds.
const updatedInit = `sessionStorage.cosUpdated && (delete sessionStorage.cosUpdated, $_updated = true, setTimeout(() => $_updated = false, 8000))`

func releaseSignals() string {
	data, _ := json.Marshal(map[string]any{"_release": "", "_releaseSeen": clusterRelease.get().Revision, "_updated": false})
	return string(data)
}

var ticketPrefix = regexp.MustCompile(`^[A-Z][A-Z0-9]*-\d+:\s*|\s*\(#\d+\)$`)

// changeText is the deployed commit's Update-Note trailer (TS-817 sets it), else its
// subject without the ticket key and PR number.
func changeText(note, subject string) string {
	if note = strings.TrimSpace(note); note != "" {
		return deployLine(note, 200)
	}
	return deployLine(ticketPrefix.ReplaceAllString(strings.TrimSpace(subject), ""), 200)
}
