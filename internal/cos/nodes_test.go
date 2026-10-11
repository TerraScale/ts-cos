package cos

import (
	"context"
	"reflect"
	"testing"
	"time"
)

const nodeStatBefore = `cpu  100 0 50 800 50 0 0 0 0 0
cpu0 50 0 25 400 25 0 0 0 0 0
cpu1 50 0 25 400 25 0 0 0 0 0
intr 12345 0 0
`

const nodeStatAfter = `cpu  200 0 100 900 100 0 0 0 0 0
cpu0 100 0 25 450 25 0 0 0 0 0
cpu1 100 0 50 400 50 0 0 0 0 0
intr 22345 0 0
`

const nodeMeminfo = `MemTotal:       16318032 kB
MemFree:         1234567 kB
MemAvailable:    8000000 kB
SwapTotal:       4194300 kB
SwapFree:        4000000 kB
`

const nodeNetDev = `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:    1000      10    0    0    0     0          0         0     1000      10    0    0    0     0       0          0
  eth0:  500000     400    0    0    0     0          0         0   250000     300    0    0    0     0       0          0
vethab12:      10       1    0    0    0     0          0         0       20       1    0    0    0     0       0          0
`

const nodeDiskstats = `   8       0 sda 1000 10 2048 300 500 20 4096 400 0 500 700
 259       0 nvme0n1 5000 0 40960 1000 3000 0 81920 2000 0 1500 3000
 254       0 dm-0 100 0 800 50 200 0 1600 80 0 60 130
`

const nodeMounts = `/dev/nvme0n1p2 / btrfs rw,subvol=/root 0 0
/dev/nvme0n1p2 /nix/store btrfs rw,subvol=/nix/store 0 0
/dev/nvme0n1p2 /home btrfs rw,subvol=/home 0 0
tmpfs /run tmpfs rw,nosuid 0 0
/dev/nvme0n1p1 /boot vfat rw 0 0
/dev/mapper/cryptroot /var/lib ext4 rw 0 0
`

func TestNodeCPUPercent(t *testing.T) {
	before, after := parseCPUStat(nodeStatBefore), parseCPUStat(nodeStatAfter)
	for key, want := range map[string]float64{"cpu": 50, "cpu0": 50, "cpu1": 75} {
		if got := cpuPercent(before, after, key); got != want {
			t.Errorf("%s busy = %v, want %v", key, got, want)
		}
	}
	if got := cpuPercent(nil, after, "cpu"); got != 0 {
		t.Errorf("first snapshot reported %v, want 0", got)
	}
	if got := busyPercent(cpuTicks{idle: 10, total: 100}, cpuTicks{idle: 5, total: 200}); got != 0 {
		t.Errorf("counter that went backwards reported %v, want 0", got)
	}
}

func TestNodeParsers(t *testing.T) {
	if got := parseMeminfo(nodeMeminfo); got != (NodeMem{Total: 16318032 << 10, Available: 8000000 << 10, SwapTotal: 4194300 << 10, SwapFree: 4000000 << 10}) {
		t.Errorf("meminfo = %+v", got)
	}
	if got := parseLoadavg("0.52 0.58 0.59 2/1234 56789"); got != [3]float64{0.52, 0.58, 0.59} {
		t.Errorf("loadavg = %v", got)
	}
	if got := parseUptime("12345.67 98765.43"); got != 12345.67 {
		t.Errorf("uptime = %v", got)
	}
	if got := parseStatm("1500 300 200 10 0 400 0"); got != 300 {
		t.Errorf("statm resident = %v", got)
	}
	if got := parseNetDev(nodeNetDev); !reflect.DeepEqual(got, map[string]counter{"lo": {1000, 1000}, "eth0": {500000, 250000}, "vethab12": {10, 20}}) {
		t.Errorf("net dev = %v", got)
	}
	if got := parseDiskstats(nodeDiskstats); !reflect.DeepEqual(got, map[string]counter{"sda": {2048, 4096}, "nvme0n1": {40960, 81920}, "dm-0": {800, 1600}}) {
		t.Errorf("diskstats = %v", got)
	}
	want := []mountPoint{{"/dev/nvme0n1p2", "/"}, {"/dev/nvme0n1p1", "/boot"}, {"/dev/mapper/cryptroot", "/var/lib"}}
	if got := parseMounts(nodeMounts); !reflect.DeepEqual(got, want) {
		t.Errorf("mounts = %v, want %v", got, want)
	}
}

func TestNodeKeepNet(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    counter
		keep bool
	}{{"lo", counter{1, 1}, false}, {"vethab12", counter{1, 1}, false}, {"eth0", counter{1, 0}, true}, {"eth1", counter{}, false}} {
		if got := keepNet(tc.name, tc.c); got != tc.keep {
			t.Errorf("keepNet(%s, %v) = %v, want %v", tc.name, tc.c, got, tc.keep)
		}
	}
}

func TestNodeRateAndPercent(t *testing.T) {
	if got := rate(150, 100, 10, 512); got != 2560 {
		t.Errorf("rate = %v, want 2560", got)
	}
	if got := rate(5, 100, 10, 1); got != 0 {
		t.Errorf("reset counter rate = %v, want 0", got)
	}
	if got := rate(150, 100, 0, 1); got != 0 {
		t.Errorf("zero-interval rate = %v, want 0", got)
	}
	if got := (NodeMem{Total: 100, Available: 25}).UsedPercent(); got != 75 {
		t.Errorf("mem used = %v, want 75", got)
	}
	if got := (NodeDisk{Total: 200, Free: 50}).UsedPercent(); got != 75 {
		t.Errorf("disk used = %v, want 75", got)
	}
	if got := (NodeDisk{}).UsedPercent(); got != 0 {
		t.Errorf("empty disk used = %v, want 0", got)
	}
}

func TestNodeDownsample(t *testing.T) {
	start := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
	var in []NodeSample
	for i := range 3 {
		f := float64(i + 1)
		in = append(in, NodeSample{
			Host: "beta", Time: start.Add(time.Duration(i) * 10 * time.Second), N: 1,
			Build: NodeBuild{Revision: "abc", Time: "t1"}, Uptime: 100 + 10*f,
			CPU: 10 * f, Cores: []float64{10 * f, 20 * f}, Load: [3]float64{f, f + 1, f + 2},
			Mem:   NodeMem{Total: 100, Available: uint64(50 * f)},
			Disks: []NodeDisk{{Mount: "/", Total: 1000, Free: uint64(1000 - 100*f), ReadBps: 10 * f, WriteBps: 20 * f}},
			Nets:  []NodeNet{{Iface: "eth0", RxBps: 100 * f, TxBps: 200 * f}},
			Temps: []NodeTemp{{Name: "cpu", Celsius: 30 + 10*f}},
			Proc:  NodeProc{RSS: uint64(100 * f), Goroutines: int(10 * f), NumGC: uint32(f)},
			T3:    NodeT3{OK: true, Threads: []int{1, 2, 2}[i], Running: 0},
		})
	}
	in[2].Build = NodeBuild{Revision: "def", Time: "t3"}
	in[2].T3 = NodeT3{OK: false, Error: "boom", Threads: 2}
	got := downsample("beta", start, in)
	want := NodeSample{
		Host: "beta", Time: start, N: 3, Build: NodeBuild{Revision: "def", Time: "t3"}, Uptime: 130,
		CPU: 20, Cores: []float64{20, 40}, Load: [3]float64{2, 3, 4},
		Mem:   NodeMem{Total: 100, Available: 100},
		Disks: []NodeDisk{{Mount: "/", Total: 1000, Free: 800, ReadBps: 20, WriteBps: 40}},
		Nets:  []NodeNet{{Iface: "eth0", RxBps: 200, TxBps: 400}},
		Temps: []NodeTemp{{Name: "cpu", Celsius: 50}},
		Proc:  NodeProc{RSS: 200, Goroutines: 20, NumGC: 2},
		T3:    NodeT3{OK: false, Error: "boom", Threads: 2, Running: 0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("downsample:\n got %+v\nwant %+v", got, want)
	}
}

func TestNodeRound1(t *testing.T) {
	if got := round1(12.345); got != 12.3 {
		t.Errorf("round1(12.345) = %v, want 12.3", got)
	}
	if got := round1(99.96); got != 100 {
		t.Errorf("round1(99.96) = %v, want 100", got)
	}
}

func TestNodeResolution(t *testing.T) {
	for window, want := range map[time.Duration]string{time.Hour: "raw", 6 * time.Hour: "raw", 7 * time.Hour: "1m", 48 * time.Hour: "1m", 49 * time.Hour: "15m"} {
		if got := nodeResolution(window); got != want {
			t.Errorf("nodeResolution(%v) = %s, want %s", window, got, want)
		}
	}
}

func TestNodeHostsRejected(t *testing.T) {
	ctx := context.Background()
	for _, host := range []string{"", "a.b", "*", ">", "a b", "a\tb"} {
		if validNodeHost(host) {
			t.Errorf("%q accepted as a node host", host)
		}
		// Validation runs before JetStream is touched, so a nil API is safe here.
		if _, err := NodeLatest(ctx, nil, host); err == nil {
			t.Errorf("NodeLatest(%q) accepted", host)
		}
		if _, err := NodeSeries(ctx, nil, host, time.Hour); err == nil {
			t.Errorf("NodeSeries(%q) accepted", host)
		}
	}
}

func TestNodeStreamsRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js := busTestJetStream(t, ctx)
	busTestReady(t, "node streams", func() bool {
		ready, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		return EnsureNodeStreams(ready, js[0]) == nil
	})
	if err := EnsureNodeStreams(ctx, js[2]); err != nil {
		t.Fatal(err)
	}
	// Samples go in through different nodes, so replication and ordering both matter.
	base := time.Now().UTC().Truncate(time.Second)
	for i := range 3 {
		s := NodeSample{Host: "beta", Time: base.Add(time.Duration(i) * 10 * time.Second), N: 1, CPU: float64(i)}
		if err := PublishNodeSample(ctx, js[i], "raw", s); err != nil {
			t.Fatal(err)
		}
	}
	latest, err := NodeLatest(ctx, js[1], "beta")
	if err != nil {
		t.Fatal(err)
	}
	if latest.CPU != 2 {
		t.Fatalf("latest CPU = %v, want the newest sample (2)", latest.CPU)
	}
	series, err := NodeSeries(ctx, js[1], "beta", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 3 || series[0].CPU != 0 || series[1].CPU != 1 || series[2].CPU != 2 {
		t.Fatalf("series out of order or incomplete: %+v", series)
	}
	other, err := NodeSeries(ctx, js[1], "delta", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("delta series holds %d samples of beta", len(other))
	}
}
