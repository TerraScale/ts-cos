package cos

import (
	"context"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Every node samples its host, its COS process and its local T3 every nodeInterval,
// publishes the raw sample and rolls samples up into 1 minute and 15 minute buckets.
const (
	nodeInterval = 10 * time.Second
	nodeT3Time   = 3 * time.Second
)

// NodeSample is one host's metrics; Time is the sample time (raw) or bucket start (1m/15m).
type NodeSample struct {
	Host      string     `json:"host"`
	Time      time.Time  `json:"time"`
	N         int        `json:"n"`                   // raw samples merged (1 for raw)
	Synthetic bool       `json:"synthetic,omitempty"` // e2e-only, never shown or alerted on
	Build     NodeBuild  `json:"build"`
	Uptime    float64    `json:"uptime"` // seconds, /proc/uptime
	CPU       float64    `json:"cpu"`    // percent busy over the interval, all cores
	Cores     []float64  `json:"cores"`  // percent busy per core
	Load      [3]float64 `json:"load"`
	Mem       NodeMem    `json:"mem"`
	Disks     []NodeDisk `json:"disks"`
	Nets      []NodeNet  `json:"nets"`
	Temps     []NodeTemp `json:"temps,omitempty"`
	Proc      NodeProc   `json:"proc"`
	T3        NodeT3     `json:"t3"`
}

type NodeBuild struct {
	Revision string `json:"revision"`
	Time     string `json:"time"`
}

// NodeMem holds bytes from /proc/meminfo.
type NodeMem struct {
	Total     uint64 `json:"total"`
	Available uint64 `json:"available"`
	SwapTotal uint64 `json:"swapTotal"`
	SwapFree  uint64 `json:"swapFree"`
}

type NodeDisk struct {
	Mount    string  `json:"mount"`
	Total    uint64  `json:"total"`
	Free     uint64  `json:"free"`
	ReadBps  float64 `json:"readBps"`
	WriteBps float64 `json:"writeBps"`
}

type NodeNet struct {
	Iface string  `json:"iface"`
	RxBps float64 `json:"rxBps"`
	TxBps float64 `json:"txBps"`
}

type NodeTemp struct {
	Name    string  `json:"name"`
	Celsius float64 `json:"celsius"`
}

// NodeProc describes the COS process itself.
type NodeProc struct {
	RSS        uint64 `json:"rss"`
	HeapAlloc  uint64 `json:"heapAlloc"`
	GCPauseNs  uint64 `json:"gcPauseNs"`
	Goroutines int    `json:"goroutines"`
	NumGC      uint32 `json:"numGC"`
}

type NodeT3 struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Threads int    `json:"threads"`
	Running int    `json:"running"`
}

// UsedPercent is the share of memory in use, not counting reclaimable cache.
func (m NodeMem) UsedPercent() float64 {
	if m.Total == 0 {
		return 0
	}
	return 100 * float64(m.Total-m.Available) / float64(m.Total)
}

// SwapPercent is the share of swap in use; zero when the host has no swap.
func (m NodeMem) SwapPercent() float64 {
	if m.SwapTotal == 0 {
		return 0
	}
	return 100 * float64(m.SwapTotal-m.SwapFree) / float64(m.SwapTotal)
}

func (d NodeDisk) UsedPercent() float64 {
	if d.Total == 0 {
		return 0
	}
	return 100 * float64(d.Total-d.Free) / float64(d.Total)
}

// cpuTicks are cumulative jiffies. idle includes iowait; total skips guest time, which user already counts.
type cpuTicks struct{ idle, total uint64 }

// counter is a cumulative pair: disk read and written sectors, or net received and sent bytes.
type counter struct{ in, out uint64 }

// mountPoint is one filesystem worth reporting, with its device as the kernel names it.
type mountPoint struct{ Device, Path string }

var diskFSTypes = map[string]bool{"ext4": true, "xfs": true, "btrfs": true, "zfs": true, "f2fs": true, "bcachefs": true, "vfat": true}

func parseCPUStat(s string) map[string]cpuTicks {
	out := map[string]cpuTicks{}
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 5 || !strings.HasPrefix(f[0], "cpu") {
			continue
		}
		var t cpuTicks
		for i, field := range f[1:min(len(f), 9)] {
			n, err := strconv.ParseUint(field, 10, 64)
			if err != nil {
				break
			}
			t.total += n
			if i == 3 || i == 4 {
				t.idle += n
			}
		}
		out[f[0]] = t
	}
	return out
}

// busyPercent is the non-idle share between two snapshots; zero if a counter went backwards.
func busyPercent(prev, cur cpuTicks) float64 {
	if cur.total <= prev.total || cur.idle < prev.idle {
		return 0
	}
	return 100 * (1 - float64(cur.idle-prev.idle)/float64(cur.total-prev.total))
}

// cpuPercent needs a previous snapshot; the first sample has none and reports zero.
func cpuPercent(prev, cur map[string]cpuTicks, key string) float64 {
	p, ok := prev[key]
	if !ok {
		return 0
	}
	return busyPercent(p, cur[key])
}

func parseMeminfo(s string) NodeMem {
	var m NodeMem
	for _, line := range strings.Split(s, "\n") {
		key, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			continue
		}
		kb, err := strconv.ParseUint(f[0], 10, 64)
		if err != nil {
			continue
		}
		bytes := kb << 10
		switch key {
		case "MemTotal":
			m.Total = bytes
		case "MemAvailable":
			m.Available = bytes
		case "SwapTotal":
			m.SwapTotal = bytes
		case "SwapFree":
			m.SwapFree = bytes
		}
	}
	return m
}

func parseLoadavg(s string) [3]float64 {
	var load [3]float64
	f := strings.Fields(s)
	for i := 0; i < len(load) && i < len(f); i++ {
		load[i], _ = strconv.ParseFloat(f[i], 64)
	}
	return load
}

func parseUptime(s string) float64 {
	f := strings.Fields(s)
	if len(f) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(f[0], 64)
	return v
}

// parseStatm returns resident pages from /proc/self/statm.
func parseStatm(s string) uint64 {
	f := strings.Fields(s)
	if len(f) < 2 {
		return 0
	}
	n, _ := strconv.ParseUint(f[1], 10, 64)
	return n
}

// parseNetDev reads "iface: rx_bytes ... tx_bytes", where tx_bytes is the ninth field after the colon.
func parseNetDev(s string) map[string]counter {
	out := map[string]counter{}
	for _, line := range strings.Split(s, "\n") {
		name, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) < 9 {
			continue
		}
		rx, err1 := strconv.ParseUint(f[0], 10, 64)
		tx, err2 := strconv.ParseUint(f[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out[strings.TrimSpace(name)] = counter{rx, tx}
	}
	return out
}

// parseDiskstats reads "major minor name ..." with sectors read as field 6 and written as field 10.
func parseDiskstats(s string) map[string]counter {
	out := map[string]counter{}
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 10 {
			continue
		}
		rd, err1 := strconv.ParseUint(f[5], 10, 64)
		wr, err2 := strconv.ParseUint(f[9], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out[f[2]] = counter{rd, wr}
	}
	return out
}

// parseMounts keeps one mount per device, the shortest path: NixOS btrfs subvolumes share a device.
func parseMounts(s string) []mountPoint {
	var out []mountPoint
	seen := map[string]int{}
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 3 || !diskFSTypes[f[2]] {
			continue
		}
		m := mountPoint{Device: f[0], Path: f[1]}
		if i, ok := seen[m.Device]; ok {
			if len(m.Path) < len(out[i].Path) {
				out[i] = m
			}
			continue
		}
		seen[m.Device] = len(out)
		out = append(out, m)
	}
	return out
}

// diskName maps a device to its /proc/diskstats name; /dev/mapper/* resolves to dm-N.
func diskName(device string) string {
	if real, err := filepath.EvalSymlinks(device); err == nil {
		device = real
	}
	return filepath.Base(device)
}

// keepNet drops loopback, container veth pairs and interfaces that never carried a byte.
func keepNet(name string, c counter) bool {
	return name != "lo" && !strings.HasPrefix(name, "veth") && c != (counter{})
}

// rate is a cumulative counter's growth per second, zero after a reset.
func rate(cur, prev uint64, seconds, unit float64) float64 {
	if seconds <= 0 || cur < prev {
		return 0
	}
	return float64(cur-prev) * unit / seconds
}

// readTemps lists hwmon sensors, or thermal zones when the host has no hwmon temperature.
func readTemps() []NodeTemp {
	var temps []NodeTemp
	inputs, _ := filepath.Glob("/sys/class/hwmon/hwmon*/temp*_input")
	for _, input := range inputs {
		dir, sensor := filepath.Dir(input), strings.TrimSuffix(filepath.Base(input), "_input")
		label := strings.TrimSpace(readText(filepath.Join(dir, sensor+"_label")))
		if label == "" {
			label = sensor
		}
		if t, ok := readTemp(input, strings.TrimSpace(readText(filepath.Join(dir, "name")))+" "+label); ok {
			temps = append(temps, t)
		}
	}
	if len(temps) > 0 {
		return temps
	}
	zones, _ := filepath.Glob("/sys/class/thermal/thermal_zone*/temp")
	for _, zone := range zones {
		if t, ok := readTemp(zone, readText(filepath.Join(filepath.Dir(zone), "type"))); ok {
			temps = append(temps, t)
		}
	}
	return temps
}

// readTemp converts a sysfs file in millidegrees to Celsius.
func readTemp(path, name string) (NodeTemp, bool) {
	milli, err := strconv.ParseFloat(strings.TrimSpace(readText(path)), 64)
	if err != nil {
		return NodeTemp{}, false
	}
	return NodeTemp{Name: strings.TrimSpace(name), Celsius: milli / 1000}, true
}

// readText returns "" for a missing file: absent sensors and counters are not errors.
func readText(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
}

// nodeCollector keeps the previous counters that rates and CPU percentages come from.
type nodeCollector struct {
	cfg   Config
	build NodeBuild
	t3    *T3
	at    time.Time
	cpu   map[string]cpuTicks
	disk  map[string]counter
	net   map[string]counter
}

// sample reads every metric at now. The first sample only sets the baselines.
func (c *nodeCollector) sample(ctx context.Context, now time.Time) NodeSample {
	sec := now.Sub(c.at).Seconds()
	s := NodeSample{Host: c.cfg.LocalHost, Time: now.UTC(), N: 1, Build: c.build}
	s.Uptime = parseUptime(readText("/proc/uptime"))
	s.Load = parseLoadavg(readText("/proc/loadavg"))
	s.Mem = parseMeminfo(readText("/proc/meminfo"))

	cpu := parseCPUStat(readText("/proc/stat"))
	s.CPU = round1(cpuPercent(c.cpu, cpu, "cpu"))
	for i := 0; ; i++ {
		key := "cpu" + strconv.Itoa(i)
		if _, ok := cpu[key]; !ok {
			break
		}
		s.Cores = append(s.Cores, round1(cpuPercent(c.cpu, cpu, key)))
	}
	c.cpu = cpu

	disks := parseDiskstats(readText("/proc/diskstats"))
	for _, m := range parseMounts(readText("/proc/self/mounts")) {
		var st syscall.Statfs_t
		if syscall.Statfs(m.Path, &st) != nil {
			continue
		}
		d := NodeDisk{Mount: m.Path, Total: st.Blocks * uint64(st.Bsize), Free: st.Bavail * uint64(st.Bsize)}
		name := diskName(m.Device)
		if p, ok := c.disk[name]; ok {
			d.ReadBps = math.Round(rate(disks[name].in, p.in, sec, 512))
			d.WriteBps = math.Round(rate(disks[name].out, p.out, sec, 512))
		}
		s.Disks = append(s.Disks, d)
	}
	c.disk = disks

	nets := parseNetDev(readText("/proc/net/dev"))
	for _, name := range slices.Sorted(maps.Keys(nets)) {
		cur := nets[name]
		if !keepNet(name, cur) {
			continue
		}
		n := NodeNet{Iface: name}
		if p, ok := c.net[name]; ok {
			n.RxBps = math.Round(rate(cur.in, p.in, sec, 1))
			n.TxBps = math.Round(rate(cur.out, p.out, sec, 1))
		}
		s.Nets = append(s.Nets, n)
	}
	c.net = nets

	s.Temps = readTemps()
	for i := range s.Temps {
		s.Temps[i].Celsius = round1(s.Temps[i].Celsius)
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.Proc = NodeProc{
		RSS:        parseStatm(readText("/proc/self/statm")) * uint64(os.Getpagesize()),
		HeapAlloc:  ms.HeapAlloc,
		GCPauseNs:  ms.PauseNs[(ms.NumGC+255)%256],
		Goroutines: runtime.NumGoroutine(),
		NumGC:      ms.NumGC,
	}
	s.T3 = c.t3Health(ctx)
	c.at = now
	return s
}

// t3Health pings the local T3 once per tick; the client is opened on the first tick that succeeds.
func (c *nodeCollector) t3Health(ctx context.Context) NodeT3 {
	if c.t3 == nil {
		t, err := OpenT3(ctx, c.cfg, c.cfg.LocalHost)
		if err != nil {
			return NodeT3{Error: publicText(err)}
		}
		c.t3 = t
	}
	ctx, cancel := context.WithTimeout(ctx, nodeT3Time)
	defer cancel()
	fleet, err := c.t3.Shell(ctx)
	if err != nil {
		return NodeT3{Error: publicText(err)}
	}
	return NodeT3{OK: true, Threads: len(fleet.Threads), Running: fleet.Active}
}

func init() { serveWorkers = append(serveWorkers, runNodes) }

// runNodes publishes this node's samples and their 1 m and 15 m roll-ups until ctx ends.
func runNodes(ctx context.Context, cfg Config, bus *Bus) {
	js, err := bus.JetStream()
	if err != nil {
		logErr(ctx, slog.LevelError, "COS node samples unavailable", err)
		return
	}
	retry(ctx, "COS node streams", func() error {
		// Bound each attempt: stream creation waits for a meta leader while the cluster forms.
		attempt, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		return EnsureNodeStreams(attempt, js)
	})
	c := &nodeCollector{cfg: cfg, build: NodeBuild{Revision: Version, Time: CurrentBuild(cfg).Built}}
	minute, quarter := nodeBucket{width: time.Minute}, nodeBucket{width: 15 * time.Minute}
	failing := map[string]bool{}
	ticker := time.NewTicker(nodeInterval)
	defer ticker.Stop()
	for first := true; ; first = false {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s := c.sample(ctx, now)
			if first {
				continue
			}
			publishNode(ctx, js, "raw", s, failing)
			done, start := minute.add(s)
			if len(done) == 0 {
				continue
			}
			m := downsample(cfg.LocalHost, start, done)
			publishNode(ctx, js, "1m", m, failing)
			if done, start := quarter.add(m); len(done) > 0 {
				publishNode(ctx, js, "15m", downsample(cfg.LocalHost, start, done), failing)
			}
		}
	}
}

// publishNode stores one sample. Each resolution logs its first failure and again only after it recovers.
func publishNode(ctx context.Context, js jetstream.JetStream, res string, s NodeSample, failing map[string]bool) {
	err := PublishNodeSample(ctx, js, res, s)
	if err == nil {
		delete(failing, res)
		return
	}
	if !failing[res] {
		failing[res] = true
		logErr(ctx, slog.LevelWarn, "COS node sample publish failed", err, "resolution", res)
	}
}

// nodeBucket gathers the samples of one time bucket. shortcut: a partial bucket is lost when the node restarts; rebuild from the raw stream if gaps matter.
type nodeBucket struct {
	width   time.Duration
	start   time.Time
	samples []NodeSample
}

// add returns the finished bucket when s starts a new one.
func (b *nodeBucket) add(s NodeSample) ([]NodeSample, time.Time) {
	start := s.Time.Truncate(b.width)
	var done []NodeSample
	var doneStart time.Time
	if !start.Equal(b.start) {
		done, doneStart = b.samples, b.start
		b.samples = nil
		b.start = start
	}
	b.samples = append(b.samples, s)
	return done, doneStart
}

// downsample averages a bucket. Build, uptime and disk capacity come from the last sample.
func downsample(host string, start time.Time, in []NodeSample) NodeSample {
	last := in[len(in)-1]
	out := NodeSample{Host: host, Time: start, Build: last.Build, Uptime: last.Uptime, T3: NodeT3{OK: true, Error: last.T3.Error}}
	for _, s := range in {
		out.N += s.N
		out.T3.OK = out.T3.OK && s.T3.OK
	}
	out.CPU = round1(mean(in, func(s NodeSample) float64 { return s.CPU }))
	out.Cores = make([]float64, len(last.Cores))
	for i := range out.Cores {
		out.Cores[i] = round1(mean(in, func(s NodeSample) float64 {
			if i < len(s.Cores) {
				return s.Cores[i]
			}
			return 0
		}))
	}
	for i := range out.Load {
		out.Load[i] = mean(in, func(s NodeSample) float64 { return s.Load[i] })
	}
	out.Mem = NodeMem{
		Total:     mean(in, func(s NodeSample) uint64 { return s.Mem.Total }),
		Available: mean(in, func(s NodeSample) uint64 { return s.Mem.Available }),
		SwapTotal: mean(in, func(s NodeSample) uint64 { return s.Mem.SwapTotal }),
		SwapFree:  mean(in, func(s NodeSample) uint64 { return s.Mem.SwapFree }),
	}
	for _, d := range last.Disks {
		m, n := NodeDisk{Mount: d.Mount, Total: d.Total}, 0.0
		for _, s := range in {
			for _, x := range s.Disks {
				if x.Mount == d.Mount {
					m.Free += x.Free
					m.ReadBps += x.ReadBps
					m.WriteBps += x.WriteBps
					n++
				}
			}
		}
		m.Free, m.ReadBps, m.WriteBps = uint64(float64(m.Free)/n), math.Round(m.ReadBps/n), math.Round(m.WriteBps/n)
		out.Disks = append(out.Disks, m)
	}
	for _, x := range last.Nets {
		m, n := NodeNet{Iface: x.Iface}, 0.0
		for _, s := range in {
			for _, y := range s.Nets {
				if y.Iface == x.Iface {
					m.RxBps += y.RxBps
					m.TxBps += y.TxBps
					n++
				}
			}
		}
		m.RxBps, m.TxBps = math.Round(m.RxBps/n), math.Round(m.TxBps/n)
		out.Nets = append(out.Nets, m)
	}
	for _, t := range last.Temps {
		m, n := NodeTemp{Name: t.Name}, 0.0
		for _, s := range in {
			for _, y := range s.Temps {
				if y.Name == t.Name {
					m.Celsius += y.Celsius
					n++
				}
			}
		}
		m.Celsius = round1(m.Celsius / n)
		out.Temps = append(out.Temps, m)
	}
	out.Proc = NodeProc{
		RSS:        mean(in, func(s NodeSample) uint64 { return s.Proc.RSS }),
		HeapAlloc:  mean(in, func(s NodeSample) uint64 { return s.Proc.HeapAlloc }),
		GCPauseNs:  mean(in, func(s NodeSample) uint64 { return s.Proc.GCPauseNs }),
		Goroutines: mean(in, func(s NodeSample) int { return s.Proc.Goroutines }),
		NumGC:      mean(in, func(s NodeSample) uint32 { return s.Proc.NumGC }),
	}
	out.T3.Threads = int(math.Round(mean(in, func(s NodeSample) float64 { return float64(s.T3.Threads) })))
	out.T3.Running = int(math.Round(mean(in, func(s NodeSample) float64 { return float64(s.T3.Running) })))
	return out
}

// round1 keeps one decimal so samples stay small on the wire.
func round1(x float64) float64 { return math.Round(x*10) / 10 }

// mean averages get over the samples; integers truncate.
func mean[T ~float64 | ~uint64 | ~int | ~uint32](in []NodeSample, get func(NodeSample) T) T {
	var sum T
	for _, s := range in {
		sum += get(s)
	}
	return sum / T(len(in))
}
