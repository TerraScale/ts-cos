package cos

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/singleflight"
)

// clusterUpAfter is how recently a meta peer must have been seen to count as up.
const clusterUpAfter = 10 * time.Second

// ClusterView is the NATS cluster as this node sees it; Known is false without an embedded server.
type ClusterView struct {
	Known      bool // false without an embedded server (CLI / tests)
	COSLeader  string
	MetaLeader string
	Members    []ClusterMember // every meta peer incl. this node, sorted by name
	Streams    []ClusterStream // sorted by name
}

// ClusterMember is one meta peer.
type ClusterMember struct {
	Name    string
	Current bool
	Active  time.Duration
}

// Up reports whether the member is current and was seen within clusterUpAfter.
func (m ClusterMember) Up() bool { return m.Current && m.Active < clusterUpAfter }

// ClusterStream is one JetStream stream's replica placement; InSync counts the leader and current replicas.
type ClusterStream struct {
	Name, Leader     string
	Replicas, InSync int
}

// clusterCache shares one read of the cluster across all open dashboards, like nodeCardCache.
var clusterCache struct {
	sync.Mutex
	at   time.Time
	view ClusterView
}

// clusterRefresh lets concurrent cache misses share one cluster read.
var clusterRefresh singleflight.Group

// ClusterHealth reads the meta group and every stream from the embedded server. Without one it is not Known.
func ClusterHealth(ctx context.Context, bus *Bus) (ClusterView, error) {
	if bus == nil || bus.server == nil {
		return ClusterView{}, nil
	}
	clusterCache.Lock()
	view, at := clusterCache.view, clusterCache.at
	clusterCache.Unlock()
	if view.Known && time.Since(at) < nodeCardTTL {
		return view, nil
	}
	shared, err, _ := clusterRefresh.Do("cluster", func() (any, error) {
		// Detached from this request: one closed tab must not cancel the read every dashboard shares.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nodeReadTimeout)
		defer cancel()
		view, err := readCluster(ctx, bus)
		if err != nil {
			return nil, err
		}
		clusterCache.Lock()
		clusterCache.at, clusterCache.view = time.Now(), view
		clusterCache.Unlock()
		return view, nil
	})
	if err == nil {
		return shared.(ClusterView), nil
	}
	// A failed refresh serves the last read only while it is young enough to show a true state.
	clusterCache.Lock()
	defer clusterCache.Unlock()
	if clusterCache.view.Known && time.Since(clusterCache.at) < nodeDownAfter {
		return clusterCache.view, nil
	}
	return ClusterView{}, err
}

// clusterOrEmpty is the cluster for a page: a failed read is logged and shows as unavailable.
func clusterOrEmpty(ctx context.Context, bus *Bus) ClusterView {
	view, err := ClusterHealth(ctx, bus)
	if err != nil {
		slog.Error("read NATS cluster", "error", err)
	}
	return view
}

func readCluster(ctx context.Context, bus *Bus) (ClusterView, error) {
	stats, err := bus.server.Jsz(&server.JSzOptions{})
	if err != nil {
		return ClusterView{}, err
	}
	// This node is always current; the meta replicas list the other peers.
	view := ClusterView{Known: true, Members: []ClusterMember{{Name: bus.host, Current: true}}}
	if stats.Meta != nil {
		view.MetaLeader = stats.Meta.Leader
		for _, p := range stats.Meta.Replicas {
			if p.Name != bus.host {
				view.Members = append(view.Members, ClusterMember{Name: p.Name, Current: p.Current, Active: p.Active})
			}
		}
	}
	slices.SortFunc(view.Members, func(a, b ClusterMember) int { return strings.Compare(a.Name, b.Name) })
	// Without a lease yet the COS leader is simply unknown.
	view.COSLeader, _ = bus.Leader(ctx)

	js, err := bus.JetStream()
	if err != nil {
		return ClusterView{}, err
	}
	lister := js.ListStreams(ctx)
	for si := range lister.Info() {
		view.Streams = append(view.Streams, clusterStream(si))
	}
	if err := lister.Err(); err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
		return ClusterView{}, err
	}
	slices.SortFunc(view.Streams, func(a, b ClusterStream) int { return strings.Compare(a.Name, b.Name) })
	return view, nil
}

// clusterStream reads one stream's placement. A stream without a leader has nothing in sync.
func clusterStream(info *jetstream.StreamInfo) ClusterStream {
	s := ClusterStream{Name: info.Config.Name, Replicas: info.Config.Replicas}
	if c := info.Cluster; c != nil && c.Leader != "" {
		s.Leader, s.InSync = c.Leader, 1
		for _, p := range c.Replicas {
			if p.Current && !p.Offline {
				s.InSync++
			}
		}
	}
	return s
}

// membersUp counts the members seen within clusterUpAfter.
func (v ClusterView) membersUp() int {
	n := 0
	for _, m := range v.Members {
		if m.Up() {
			n++
		}
	}
	return n
}

// streamsInSync counts the streams whose every replica is in sync.
func (v ClusterView) streamsInSync() int {
	n := 0
	for _, s := range v.Streams {
		if s.InSync >= s.Replicas {
			n++
		}
	}
	return n
}

// membersTone is danger below two members up, where the meta group loses quorum, and warn while any member is missing.
func (v ClusterView) membersTone() string {
	switch up := v.membersUp(); {
	case up < 2:
		return "danger"
	case up < len(v.Members):
		return "warn"
	}
	return ""
}

// streamsTone warns while any stream has a replica out of sync.
func (v ClusterView) streamsTone() string {
	if v.streamsInSync() < len(v.Streams) {
		return "warn"
	}
	return ""
}

// state is the tile's dot: unknown without a cluster, then down, degraded or up by the members tone.
func (v ClusterView) state() string {
	switch {
	case !v.Known:
		return "unknown"
	case v.membersTone() == "danger":
		return "down"
	case v.membersTone() == "warn":
		return "degraded"
	}
	return "up"
}

// buildDiffers reports whether a known node runs another revision than the COS leader's.
func buildDiffers(cards []NodeCard, leader string, c NodeCard) bool {
	for _, l := range cards {
		if l.Host == leader && l.known() && l.Sample.Build.Revision != "" {
			return c.known() && c.Sample.Build.Revision != l.Sample.Build.Revision
		}
	}
	return false
}

// buildRevisionText is the node's 12-character revision, or a dash when unknown.
func buildRevisionText(c NodeCard) string {
	if !c.known() {
		return noValue
	}
	return orDash(shortRevision(c.Sample.Build.Revision))
}

// buildTimeText is the node's build time, or a dash when unknown.
func buildTimeText(c NodeCard) string {
	if !c.known() {
		return noValue
	}
	return orDash(formatBuildTime(c.Sample.Build.Time))
}
