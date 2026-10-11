package cos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Alert thresholds are fixed; a host past them is worth an Inbox item or a push.
const (
	nodeDiskAlert    = 85.0             // percent used, any mount
	nodeMemAlert     = 90.0             // percent used (MemAvailable based)
	nodeMissingAfter = 60 * time.Second // no raw sample for this long
	nodeAlertEvery   = 10 * time.Second
)

func init() { serveWorkers = append(serveWorkers, runNodeAlerts) }

// runNodeAlerts watches node samples on the leader only, so each alert is published once per cluster.
func runNodeAlerts(ctx context.Context, _ Config, bus *Bus) {
	bus.Lead(ctx, func(ctx context.Context) { watchNodes(ctx, bus) })
}

// watchNodes keeps each host's newest sample and publishes the alerts every nodeAlertEvery.
func watchNodes(ctx context.Context, bus *Bus) {
	js, err := bus.JetStream()
	if err != nil {
		logErr(ctx, slog.LevelError, "COS node alerts unavailable", err)
		return
	}
	var mu sync.Mutex
	latest := map[string]NodeSample{}
	// Subscribe before seeding, so a sample published in between is not lost.
	sub, err := bus.conn.Subscribe("cos.nodes.raw.*", func(m *nats.Msg) {
		var s NodeSample
		if json.Unmarshal(m.Data, &s) != nil || !validNodeHost(s.Host) {
			return
		}
		mu.Lock()
		latest[s.Host] = s
		mu.Unlock()
	})
	if err != nil {
		logErr(ctx, slog.LevelError, "COS node alert subscription", err)
		return
	}
	defer sub.Unsubscribe()
	// A host with no sample counts as last seen when this leader started, which gives it the grace period.
	start := time.Now()
	hosts, err := bus.Hosts(ctx)
	if err != nil {
		logErr(ctx, slog.LevelWarn, "COS node alert hosts", err)
	}
	for host, s := range seedNodeLatest(ctx, js, hosts, start) {
		mu.Lock()
		if _, seen := latest[host]; !seen {
			latest[host] = s
		}
		mu.Unlock()
	}
	// alerted holds the IDs published this lead term; the stream dedupes them for 24 h anyway.
	alerted := map[string]bool{}
	ticker := time.NewTicker(nodeAlertEvery)
	defer ticker.Stop()
	for {
		var now time.Time
		select {
		case <-ctx.Done():
			return
		case now = <-ticker.C:
		}
		mu.Lock()
		snapshot := maps.Clone(latest)
		mu.Unlock()
		for _, e := range nodeAlerts(now, snapshot) {
			if alerted[e.ID] {
				continue
			}
			if _, err := PublishEvent(ctx, js, e); err != nil {
				logErr(ctx, slog.LevelError, "COS node alert publish", err, "event", e.ID)
				continue
			}
			alerted[e.ID] = true
			// A synthetic host never reports again once its missing alert is out.
			var d struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(e.Data, &d)
			if e.Synthetic && d.Type == "missing" {
				mu.Lock()
				delete(latest, e.Host)
				mu.Unlock()
			}
		}
	}
}

// seedNodeLatest starts the watcher from what is stored: every host with a raw sample, plus the expected hosts.
// A host with no stored sample counts as last seen at start, so a silent host still gets its missing alert.
func seedNodeLatest(ctx context.Context, js jetstream.JetStream, hosts []string, start time.Time) map[string]NodeSample {
	stored, err := nodeRawHosts(ctx, js)
	if err != nil {
		logErr(ctx, slog.LevelWarn, "COS node alert stored hosts", err)
	}
	known := map[string]bool{}
	for _, host := range append(stored, hosts...) {
		known[host] = true
	}
	out := make(map[string]NodeSample, len(known))
	for host := range known {
		s, err := NodeLatest(ctx, js, host)
		if err != nil {
			s = NodeSample{Host: host, Time: start}
		}
		out[host] = s
	}
	return out
}

// nodeRawHosts lists every host with a subject in the raw node stream, so a host the bus does not know still counts.
func nodeRawHosts(ctx context.Context, js jetstream.JetStream) ([]string, error) {
	stream, err := js.Stream(ctx, NodesRawStream)
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(nodeSubject("raw", "*")))
	if err != nil {
		return nil, err
	}
	var hosts []string
	for subject := range info.State.Subjects {
		if host, ok := strings.CutPrefix(subject, nodeSubject("raw", "")); ok && validNodeHost(host) {
			hosts = append(hosts, host)
		}
	}
	return hosts, nil
}

// nodeAlerts evaluates the rules over each host's newest sample; latest[host].Time is when the host was last seen.
func nodeAlerts(now time.Time, latest map[string]NodeSample) []Event {
	day := now.UTC().Format("2006-01-02")
	var out []Event
	for _, host := range slices.Sorted(maps.Keys(latest)) {
		s := latest[host]
		alert := func(sev Severity, id, summary string, data map[string]any) {
			if s.Synthetic {
				sev = SeverityInfo // e2e hosts never page anyone
			}
			raw, _ := json.Marshal(data)
			out = append(out, Event{ID: id, Source: SourceNode, Kind: KindNodeAlert, Time: now, Severity: sev,
				Summary: EventText(summary, eventSummaryMax), Host: host, Synthetic: s.Synthetic, Data: raw})
		}
		if age := now.Sub(s.Time); age > nodeMissingAfter {
			// The disk and memory numbers of a silent host are stale, so only the missing alert fires.
			alert(SeverityUrgent, NewEventID(SourceNode, "missing", host, strconv.FormatInt(s.Time.Unix(), 10)),
				fmt.Sprintf("%s: no node sample for %.0f s (last %s UTC)", host, age.Seconds(), s.Time.UTC().Format("15:04:05")),
				map[string]any{"type": "missing", "lastSeen": s.Time.UTC().Format(time.RFC3339), "after": int(nodeMissingAfter / time.Second)})
			continue
		}
		for _, d := range s.Disks {
			if p := d.UsedPercent(); p > nodeDiskAlert {
				alert(SeverityWarn, NewEventID(SourceNode, "disk", host, d.Mount, day),
					fmt.Sprintf("%s: disk %s is %.0f%% full (alert above %.0f%%)", host, d.Mount, p, nodeDiskAlert),
					map[string]any{"type": "disk", "mount": d.Mount, "percent": round1(p), "threshold": nodeDiskAlert})
			}
		}
		if p := s.Mem.UsedPercent(); p > nodeMemAlert {
			alert(SeverityWarn, NewEventID(SourceNode, "mem", host, day),
				fmt.Sprintf("%s: memory %.0f%% used (alert above %.0f%%)", host, p, nodeMemAlert),
				map[string]any{"type": "mem", "percent": round1(p), "threshold": nodeMemAlert})
		}
	}
	return out
}
