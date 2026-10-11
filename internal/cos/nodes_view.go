package cos

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/singleflight"
)

// nodeStaleAfter and nodeDownAfter match the missing-node alert: 30 s without a sample is degraded, 60 s is down.
const (
	nodeStaleAfter  = 30 * time.Second
	nodeDownAfter   = 60 * time.Second
	nodeCardTTL     = 5 * time.Second
	nodeReadTimeout = 5 * time.Second
)

// NodeCard is one host's current state for the overview, K's home panel and the detail header.
type NodeCard struct {
	Host        string
	State       string     // up (< 30 s), degraded (30–60 s), down (> 60 s), unknown (no sample)
	Sample      NodeSample // newest raw sample (zero when unknown)
	Age         time.Duration
	CPU         []float64     // last hour of CPU from the 1m stream, for the tile's sparkline
	Restarts    []NodeRestart // last seven days of service starts
	restartsErr bool          // the restart record could not be read
}

// nodeCardCache shares one read of every card across all open dashboards.
var nodeCardCache struct {
	sync.Mutex
	at    time.Time
	cards []NodeCard
}

// nodeCardRefresh lets concurrent cache misses share one bus read.
var nodeCardRefresh singleflight.Group

// NodeCards returns every node's card, sorted by host. Results are shared for nodeCardTTL.
func NodeCards(ctx context.Context, bus *Bus) ([]NodeCard, error) {
	if bus == nil {
		return unknownCards(), nil
	}
	nodeCardCache.Lock()
	cards, at := nodeCardCache.cards, nodeCardCache.at
	nodeCardCache.Unlock()
	if cards != nil && time.Since(at) < nodeCardTTL {
		return cards, nil
	}
	shared, err, _ := nodeCardRefresh.Do("cards", func() (any, error) {
		// Detached from this request: one closed tab must not cancel the read every dashboard shares.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nodeReadTimeout)
		defer cancel()
		cards, err := readNodeCards(ctx, bus)
		if err != nil {
			return nil, err
		}
		nodeCardCache.Lock()
		nodeCardCache.at, nodeCardCache.cards = time.Now(), cards
		nodeCardCache.Unlock()
		return cards, nil
	})
	if err == nil {
		return shared.([]NodeCard), nil
	}
	// A failed refresh serves the last read only while it is young enough to show a true state.
	nodeCardCache.Lock()
	defer nodeCardCache.Unlock()
	if nodeCardCache.cards != nil && time.Since(nodeCardCache.at) < nodeDownAfter {
		return nodeCardCache.cards, nil
	}
	return nil, err
}

// unknownCards is the overview without a bus: the expected hosts, all unknown.
func unknownCards() []NodeCard {
	cards := make([]NodeCard, 0, len(expectedHosts))
	for _, h := range expectedHosts {
		cards = append(cards, NodeCard{Host: h, State: "unknown"})
	}
	return cards
}

func readNodeCards(ctx context.Context, bus *Bus) ([]NodeCard, error) {
	js, err := bus.JetStream()
	if err != nil {
		return nil, err
	}
	hosts, err := bus.Hosts(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	all := readHostCards(hosts, func(host string) NodeCard {
		return readNodeCard(ctx, js, host, now)
	})
	cards := make([]NodeCard, 0, len(all))
	for _, c := range all {
		if !c.Sample.Synthetic {
			cards = append(cards, c)
		}
	}
	return cards, nil
}

// readHostCards reads all hosts at once, so the refresh takes as long as the slowest host, not the sum. Output keeps the input order.
func readHostCards(hosts []string, read func(host string) NodeCard) []NodeCard {
	cards := make([]NodeCard, len(hosts))
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cards[i] = read(host)
		}()
	}
	wg.Wait()
	return cards
}

// readNodeCard reads a host's newest sample and its last hour. A host without a readable sample is unknown, not an error.
func readNodeCard(ctx context.Context, js jetstream.JetStream, host string, now time.Time) NodeCard {
	c := NodeCard{Host: host, State: "unknown"}
	c.Restarts, c.restartsErr = recentRestarts(ctx, js, host, now)
	s, err := NodeLatest(ctx, js, host)
	if err != nil {
		return c
	}
	c.Sample, c.Age = s, now.Sub(s.Time)
	c.State = nodeState(c.Age)
	hour, err := nodeRange(ctx, js, "1m", host, time.Hour)
	if err != nil {
		return c
	}
	for _, m := range hour {
		c.CPU = append(c.CPU, m.CPU)
	}
	return c
}

// nodeState classifies a node by the age of its newest sample.
func nodeState(age time.Duration) string {
	switch {
	case age < nodeStaleAfter:
		return "up"
	case age <= nodeDownAfter:
		return "degraded"
	}
	return "down"
}

// netBps is the node's total traffic, received plus sent, in bytes per second.
func netBps(s NodeSample) float64 {
	var total float64
	for _, n := range s.Nets {
		total += n.RxBps + n.TxBps
	}
	return total
}

// known reports whether the card has a sample; an unknown node shows dashes, never zeros.
func (c NodeCard) known() bool { return c.State != "unknown" }

// text is a KPI value for a known node, or a dash.
func (c NodeCard) text(formatted string) string {
	if !c.known() {
		return noValue
	}
	return formatted
}

// unit is a KPI unit for a known node, or nothing.
func (c NodeCard) unit(u string) string {
	if !c.known() {
		return ""
	}
	return u
}

// fullestDisk is the mount closest to full, or the zero NodeDisk when the node reports none.
func (c NodeCard) fullestDisk() NodeDisk {
	var top NodeDisk
	for _, d := range c.Sample.Disks {
		if top.Total == 0 || d.UsedPercent() > top.UsedPercent() {
			top = d
		}
	}
	return top
}

// hasTemps reports whether the node has any temperature sensor.
func (c NodeCard) hasTemps() bool { return len(c.Sample.Temps) > 0 }

// hottest is the warmest sensor's reading in °C; it is only meaningful when hasTemps.
func (c NodeCard) hottest() float64 {
	var top float64
	for i, t := range c.Sample.Temps {
		if i == 0 || t.Celsius > top {
			top = t.Celsius
		}
	}
	return top
}

// summary is the card's one muted line: load, network, T3 and hottest sensor. Unknown parts are left out.
func (c NodeCard) summary() string {
	if !c.known() {
		return ""
	}
	parts := []string{"load " + formatFixed(c.Sample.Load[0], 2), "net " + formatRate(netBps(c.Sample))}
	if c.Sample.T3.OK {
		parts = append(parts, "T3 "+integer(c.Sample.T3.Running)+"/"+integer(c.Sample.T3.Threads))
	} else {
		parts = append(parts, "T3 down")
	}
	if c.hasTemps() {
		parts = append(parts, formatTemp(c.hottest()))
	}
	return strings.Join(parts, " · ")
}

// nodeMeta is the muted text beside a node's name: uptime and the short build revision.
func nodeMeta(c NodeCard) string {
	if !c.known() {
		return "no samples yet"
	}
	revision := shortRevision(c.Sample.Build.Revision)
	if revision == "" {
		revision = "unknown"
	}
	return "up " + formatUptime(c.Sample.Uptime) + " · " + revision
}

// sparkLine and sparkArea are the two paths of a KPI's sparkline, scaled to ymax (≤ 0 means the series max).
func sparkLine(values []float64, ymax float64) string {
	line, _ := sparkPaths(values, ymax)
	return line
}

func sparkArea(values []float64, ymax float64) string {
	_, area := sparkPaths(values, ymax)
	return area
}
