package cos

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// clusterFixture is a three-member meta group with one member down, and twelve streams with one at 2/3.
func clusterFixture() ClusterView {
	v := ClusterView{
		Known: true, COSLeader: "delta", MetaLeader: "beta",
		Members: []ClusterMember{
			{Name: "beta", Current: true, Active: 2 * time.Second},
			{Name: "delta", Current: true},
			{Name: "mariogk-dev", Current: false, Active: 40 * time.Second},
		},
	}
	for i := range 12 {
		v.Streams = append(v.Streams, ClusterStream{Name: fmt.Sprintf("COS_%02d", i), Leader: "delta", Replicas: 3, InSync: 3})
	}
	v.Streams[4].InSync = 2
	return v
}

func TestClusterMemberUp(t *testing.T) {
	for _, tt := range []struct {
		m    ClusterMember
		want bool
	}{
		{ClusterMember{Current: true, Active: 9 * time.Second}, true},
		{ClusterMember{Current: true, Active: 10 * time.Second}, false},
		{ClusterMember{Current: false, Active: time.Second}, false},
	} {
		if got := tt.m.Up(); got != tt.want {
			t.Errorf("Up(%+v) = %v, want %v", tt.m, got, tt.want)
		}
	}
}

func TestClusterTones(t *testing.T) {
	healthy := clusterFixture()
	healthy.Members[2].Current, healthy.Members[2].Active = true, time.Second
	healthy.Streams[4].InSync = 3
	oneUp := clusterFixture()
	oneUp.Members[1].Current, oneUp.Members[2].Current = false, false
	for _, tt := range []struct {
		name            string
		view            ClusterView
		members, stream string
		state           string
	}{
		{"fixture two of three up, stream lagging", clusterFixture(), "warn", "warn", "degraded"},
		{"all members and streams healthy", healthy, "", "", "up"},
		{"one of three up", oneUp, "danger", "warn", "down"},
		{"no embedded server", ClusterView{}, "danger", "", "unknown"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.view.membersTone(); got != tt.members {
				t.Errorf("membersTone = %q, want %q", got, tt.members)
			}
			if got := tt.view.streamsTone(); got != tt.stream {
				t.Errorf("streamsTone = %q, want %q", got, tt.stream)
			}
			if got := tt.view.state(); got != tt.state {
				t.Errorf("state = %q, want %q", got, tt.state)
			}
		})
	}
}

func TestClusterHealthWithoutServer(t *testing.T) {
	for _, bus := range []*Bus{nil, {}} {
		v, err := ClusterHealth(context.Background(), bus)
		if err != nil || v.Known {
			t.Errorf("ClusterHealth(%v) = %+v, %v; want unknown without error", bus, v, err)
		}
	}
}

func TestClusterHealthServesFreshCache(t *testing.T) {
	want := clusterFixture()
	clusterCache.Lock()
	clusterCache.at, clusterCache.view = time.Now(), want
	clusterCache.Unlock()
	t.Cleanup(func() {
		clusterCache.Lock()
		clusterCache.at, clusterCache.view = time.Time{}, ClusterView{}
		clusterCache.Unlock()
	})
	// A bare server has no meta group or JetStream, so any bus read would fail: a fresh cache must answer without one.
	got, err := ClusterHealth(context.Background(), &Bus{server: &server.Server{}})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("ClusterHealth = %+v, %v; want the fresh cache", got, err)
	}
}

func TestNodesPageClusterRender(t *testing.T) {
	now := time.Now()
	cards := nodeFixtureCards(now)
	cards[1].Sample.Build.Revision = "fedcba9876543210ffff" // differs from the COS leader, delta
	var buf bytes.Buffer
	if err := nodesPage(chatHeader{}, clusterFixture(), cards).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	html := buf.String()
	for _, want := range []string{
		`id="cluster"`, `id="delta"`, `id="nodes-grid"`, `NATS cluster`,
		`2/3 members · 11/12 streams in sync`, `COS leader delta · meta leader beta`,
		`<details class="disclosure">`, `Streams and builds`, `data-state="degraded"`,
		`class="badge">differs</span>`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("page missing %s", want)
		}
	}
	if n := strings.Count(html, `class="badge">differs</span>`); n != 1 {
		t.Errorf("differs badges = %d, want 1 (mariogk-dev only)", n)
	}
	assertCleanMarkup(t, html)

	buf.Reset()
	if err := nodesPage(chatHeader{}, ClusterView{}, cards).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "cluster unknown") || !strings.Contains(buf.String(), `id="delta"`) {
		t.Error("an unknown cluster keeps the cards and says it is unknown")
	}
}
