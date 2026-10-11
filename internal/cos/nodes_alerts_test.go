package cos

import (
	"encoding/json"
	"slices"
	"strconv"
	"testing"
	"time"
)

var nodeAlertNow = time.Date(2026, 10, 11, 3, 13, 4, 0, time.UTC)

// nodeAlertDisk is a disk that is used percent full.
func nodeAlertDisk(mount string, used uint64) NodeDisk {
	return NodeDisk{Mount: mount, Total: 100, Free: 100 - used}
}

func TestNodeAlerts(t *testing.T) {
	day := "2026-10-11"
	fresh := nodeAlertNow.Add(-5 * time.Second)
	stale := nodeAlertNow.Add(-75 * time.Second)
	okMem := NodeMem{Total: 100, Available: 50}
	tests := []struct {
		name      string
		latest    map[string]NodeSample
		want      []string // expected event IDs, in order
		severity  Severity
		synthetic bool
	}{
		{
			name:   "healthy host raises nothing",
			latest: map[string]NodeSample{"beta": {Host: "beta", Time: fresh, Mem: okMem, Disks: []NodeDisk{nodeAlertDisk("/", 40)}}},
		},
		{
			name:     "disk over 85 percent is a warn per mount per day",
			latest:   map[string]NodeSample{"delta": {Host: "delta", Time: fresh, Mem: okMem, Disks: []NodeDisk{nodeAlertDisk("/", 90), nodeAlertDisk("/data", 10)}}},
			want:     []string{NewEventID(SourceNode, "disk", "delta", "/", day)},
			severity: SeverityWarn,
		},
		{
			name:     "memory over 90 percent is a warn",
			latest:   map[string]NodeSample{"beta": {Host: "beta", Time: fresh, Mem: NodeMem{Total: 100, Available: 7}}},
			want:     []string{NewEventID(SourceNode, "mem", "beta", day)},
			severity: SeverityWarn,
		},
		{
			name:     "disk and memory on one host are two events",
			latest:   map[string]NodeSample{"beta": {Host: "beta", Time: fresh, Mem: NodeMem{Total: 100, Available: 5}, Disks: []NodeDisk{nodeAlertDisk("/", 86)}}},
			want:     []string{NewEventID(SourceNode, "disk", "beta", "/", day), NewEventID(SourceNode, "mem", "beta", day)},
			severity: SeverityWarn,
		},
		{
			name: "silent host is urgent missing and its stale numbers are ignored",
			latest: map[string]NodeSample{"mariogk-dev": {Host: "mariogk-dev", Time: stale,
				Mem: NodeMem{Total: 100, Available: 1}, Disks: []NodeDisk{nodeAlertDisk("/", 99)}}},
			want:     []string{NewEventID(SourceNode, "missing", "mariogk-dev", strconv.FormatInt(stale.Unix(), 10))},
			severity: SeverityUrgent,
		},
		{
			name: "synthetic silent host is info and synthetic",
			latest: map[string]NodeSample{"e2e-abc123": {Host: "e2e-abc123", Time: stale, Synthetic: true,
				Mem: okMem}},
			want:      []string{NewEventID(SourceNode, "missing", "e2e-abc123", strconv.FormatInt(stale.Unix(), 10))},
			severity:  SeverityInfo,
			synthetic: true,
		},
		{
			name: "synthetic disk over threshold is info and synthetic",
			latest: map[string]NodeSample{"e2e-def456": {Host: "e2e-def456", Time: fresh, Synthetic: true,
				Mem: okMem, Disks: []NodeDisk{nodeAlertDisk("/", 90)}}},
			want:      []string{NewEventID(SourceNode, "disk", "e2e-def456", "/", day)},
			severity:  SeverityInfo,
			synthetic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nodeAlerts(nodeAlertNow, tt.latest)
			var ids []string
			for _, e := range got {
				if err := e.Validate(); err != nil {
					t.Fatalf("event %s is invalid: %v", e.ID, err)
				}
				if e.Source != SourceNode || e.Kind != KindNodeAlert || !e.Time.Equal(nodeAlertNow) {
					t.Errorf("event %s = %s %s at %v, want node node.alert at now", e.ID, e.Source, e.Kind, e.Time)
				}
				if e.Severity != tt.severity || e.Synthetic != tt.synthetic {
					t.Errorf("event %s severity %s synthetic %v, want %s synthetic %v", e.ID, e.Severity, e.Synthetic, tt.severity, tt.synthetic)
				}
				ids = append(ids, e.ID)
			}
			if !slices.Equal(ids, tt.want) {
				t.Fatalf("IDs = %v, want %v", ids, tt.want)
			}
		})
	}
}

func TestNodeAlertData(t *testing.T) {
	stale := nodeAlertNow.Add(-75 * time.Second)
	got := nodeAlerts(nodeAlertNow, map[string]NodeSample{
		"delta":       {Host: "delta", Time: nodeAlertNow, Mem: NodeMem{Total: 100, Available: 50}, Disks: []NodeDisk{nodeAlertDisk("/", 87)}},
		"mariogk-dev": {Host: "mariogk-dev", Time: stale},
	})
	if len(got) != 2 {
		t.Fatalf("got %d events, want disk and missing", len(got))
	}
	disk, missing := got[0], got[1]
	if want := "delta: disk / is 87% full (alert above 85%)"; disk.Summary != want {
		t.Errorf("disk summary = %q, want %q", disk.Summary, want)
	}
	var diskData map[string]any
	if err := json.Unmarshal(disk.Data, &diskData); err != nil {
		t.Fatal(err)
	}
	if diskData["type"] != "disk" || diskData["mount"] != "/" || diskData["percent"] != 87.0 || diskData["threshold"] != 85.0 {
		t.Errorf("disk data = %v", diskData)
	}
	if want := "mariogk-dev: no node sample for 75 s (last 03:11:49 UTC)"; missing.Summary != want {
		t.Errorf("missing summary = %q, want %q", missing.Summary, want)
	}
	var missingData map[string]any
	if err := json.Unmarshal(missing.Data, &missingData); err != nil {
		t.Fatal(err)
	}
	if missingData["type"] != "missing" || missingData["lastSeen"] != "2026-10-11T03:11:49Z" || missingData["after"] != 60.0 {
		t.Errorf("missing data = %v", missingData)
	}
}
