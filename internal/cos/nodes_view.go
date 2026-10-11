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
	Host          string
	State         string     // up (< 30 s), degraded (30–60 s), down (> 60 s), unknown (no sample)
	Sample        NodeSample // newest raw sample (zero when unknown)
	Age           time.Duration
	CPU, Mem, Net []float64 // last hour from the 1m stream, for sparklines (Net = rx+tx bytes/s)
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
		c.Mem = append(c.Mem, m.Mem.UsedPercent())
		c.Net = append(c.Net, netBps(m))
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

// SwapPercent is the share of swap in use.
func (m NodeMem) SwapPercent() float64 {
	if m.SwapTotal == 0 {
		return 0
	}
	return 100 * float64(m.SwapTotal-m.SwapFree) / float64(m.SwapTotal)
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

func (c NodeCard) cpuTone() string {
	if c.known() && c.Sample.CPU >= 90 {
		return "warn"
	}
	return ""
}

func (c NodeCard) memTone() string {
	switch used := c.Sample.Mem.UsedPercent(); {
	case !c.known():
		return ""
	case used > 90:
		return "danger"
	case used > 80:
		return "warn"
	}
	return ""
}

func (c NodeCard) t3Tone() string {
	if c.known() && !c.Sample.T3.OK {
		return "danger"
	}
	return ""
}

// t3Label names the T3 KPI, with the thread count when the node has one.
func (c NodeCard) t3Label() string {
	if !c.known() {
		return "T3"
	}
	return "T3 · " + integer(c.Sample.T3.Threads) + " threads"
}

// hasTemps reports whether the node has any temperature sensor.
func (c NodeCard) hasTemps() bool { return len(c.Sample.Temps) > 0 }

// hottest names the hottest sensor, for the one-line temperature summary.
func (c NodeCard) hottest() string {
	var top NodeTemp
	for i, t := range c.Sample.Temps {
		if i == 0 || t.Celsius > top.Celsius {
			top = t
		}
	}
	return "Hottest " + top.Name + " " + formatTemp(top.Celsius)
}

// nodeMeta is the muted line under a node's name: build, build time and uptime, plus the age of the last sample when the node is quiet.
func nodeMeta(c NodeCard) string {
	if !c.known() {
		return "no samples yet"
	}
	revision := shortRevision(c.Sample.Build.Revision)
	if revision == "" {
		revision = "unknown"
	}
	parts := []string{"build " + revision}
	if c.Sample.Build.Time != "" {
		parts = append(parts, formatBuildTime(c.Sample.Build.Time))
	}
	parts = append(parts, "up "+formatUptime(c.Sample.Uptime))
	if c.State == "degraded" || c.State == "down" {
		parts = append(parts, "last sample "+formatUptime(c.Age.Seconds())+" ago")
	}
	return strings.Join(parts, " · ")
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
