package cos

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// nodeDetailRoutes serves one page per node.
func (w *Web) nodeDetailRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /nodes/{host}", w.nodeDetail)
}

// nodeDetail shows one node. Unknown or synthetic hosts are 404; a web without a bus renders every node as unknown.
func (w *Web) nodeDetail(rw http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if !validNodeHost(host) {
		http.NotFound(rw, r)
		return
	}
	window, span := nodeWindowFor(r.URL.Query().Get("range"))
	ctx := r.Context()
	now := time.Now()
	card := NodeCard{Host: host, State: "unknown"}
	var series []NodeSample
	if w.Bus != nil {
		js, err := w.Bus.JetStream()
		if err != nil {
			http.Error(rw, "Nodes are unavailable; retry in a few seconds", http.StatusServiceUnavailable)
			return
		}
		card = readNodeCard(ctx, js, host, now)
		if !card.known() || card.Sample.Synthetic {
			http.NotFound(rw, r)
			return
		}
		series, err = nodeDetail(ctx, js, host, span)
		if err != nil {
			slog.Error("read node series", "host", host, "error", err)
			http.Error(rw, "Node history is unavailable; retry in a few seconds", http.StatusServiceUnavailable)
			return
		}
	}
	charts := nodeCharts(series, now.Add(-span), now, window)
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := nodeDetailPage(w.header(ctx), card, window, charts).Render(ctx, rw); err != nil {
		slog.Error("render node", "error", err)
	}
}

// nodeWindowNames is the detail page's range control, shortest first.
var nodeWindowNames = []string{"1h", "24h", "7d"}

const nodeMaxPoints = 360

// nodeDetail returns a host's samples over window, averaged down to at most nodeMaxPoints.
func nodeDetail(ctx context.Context, js jetstream.JetStream, host string, window time.Duration) ([]NodeSample, error) {
	in, err := NodeSeries(ctx, js, host, window)
	if err != nil {
		return nil, err
	}
	return thinSamples(host, in, nodeMaxPoints), nil
}

// thinSamples averages runs of consecutive samples with downsample, so detail charts match the roll-ups.
func thinSamples(host string, in []NodeSample, limit int) []NodeSample {
	if len(in) <= limit {
		return in
	}
	step := (len(in) + limit - 1) / limit
	out := make([]NodeSample, 0, (len(in)+step-1)/step)
	for i := 0; i < len(in); i += step {
		out = append(out, downsample(host, in[i].Time, in[i:min(i+step, len(in))]))
	}
	return out
}

// nodeWindowFor maps the range query to a window name and duration; anything unknown is one hour.
func nodeWindowFor(name string) (string, time.Duration) {
	if d, ok := nodeWindows[name]; ok {
		return name, d
	}
	return "1h", time.Hour
}

// nodeChart is one figure on the detail page. Values hold NaN where a sample has no value.
type nodeChart struct {
	Title      string
	Times      []time.Time
	Lines      []chartLine
	YMax       float64
	YTicks     []string
	XTicks     []string
	Start, End time.Time
	Format     func(float64) string
}

// chartLine is one series; Color is its data-series 1..5, fixed by entity so colors do not shift.
type chartLine struct {
	Name   string
	Color  int
	Values []float64
}

func (c nodeChart) empty() bool { return len(c.Times) == 0 }

// nodeCharts builds the ten detail charts from samples over [start, end].
func nodeCharts(in []NodeSample, start, end time.Time, window string) []nodeChart {
	layout := "15:04"
	if window != "1h" {
		layout = "Jan 2 15:04"
	}
	times := make([]time.Time, len(in))
	for i, s := range in {
		times[i] = s.Time
	}
	pick := func(f func(NodeSample) float64) []float64 {
		out := make([]float64, len(in))
		for i, s := range in {
			out[i] = f(s)
		}
		return out
	}
	build := func(title string, format func(float64) string, ymax float64, lines ...chartLine) nodeChart {
		if ymax <= 0 {
			ymax = axisMax(peakOf(lines))
		}
		return nodeChart{Title: title, Times: times, Lines: lines, YMax: ymax, YTicks: yTicks(ymax, format),
			XTicks: xTicks(start, end, layout), Start: start, End: end, Format: format}
	}
	byteChart := func(title string, format func(float64) string, lines ...chartLine) nodeChart {
		return build(title, format, byteAxisMax(peakOf(lines)), lines...)
	}
	var disks []chartLine
	if len(in) > 0 {
		for i, mount := range sortedMounts(in[len(in)-1]) {
			if i == 5 {
				break
			}
			disks = append(disks, chartLine{Name: mount, Color: i + 1, Values: pick(func(s NodeSample) float64 { return diskUsed(s, mount) })})
		}
	}
	var temps []chartLine
	for i, name := range hottestSensors(in) {
		temps = append(temps, chartLine{Name: name, Color: i + 1, Values: pick(func(s NodeSample) float64 { return tempAt(s, name) })})
	}
	return []nodeChart{
		build("CPU", formatPercent, 100, chartLine{"CPU", 1, pick(func(s NodeSample) float64 { return s.CPU })}),
		build("Load", formatNumber, 0,
			chartLine{"1 min", 1, pick(func(s NodeSample) float64 { return s.Load[0] })},
			chartLine{"5 min", 2, pick(func(s NodeSample) float64 { return s.Load[1] })},
			chartLine{"15 min", 3, pick(func(s NodeSample) float64 { return s.Load[2] })}),
		build("Memory", formatPercent, 100,
			chartLine{"Memory", 1, pick(func(s NodeSample) float64 { return s.Mem.UsedPercent() })},
			chartLine{"Swap", 2, pick(func(s NodeSample) float64 { return s.Mem.SwapPercent() })}),
		build("Disks used", formatPercent, 100, disks...),
		byteChart("Disk IO", formatRate,
			chartLine{"Read", 1, pick(func(s NodeSample) float64 { return sumDisks(s, func(d NodeDisk) float64 { return d.ReadBps }) })},
			chartLine{"Write", 2, pick(func(s NodeSample) float64 { return sumDisks(s, func(d NodeDisk) float64 { return d.WriteBps }) })}),
		byteChart("Network", formatRate,
			chartLine{"Received", 1, pick(func(s NodeSample) float64 { return sumNets(s, func(n NodeNet) float64 { return n.RxBps }) })},
			chartLine{"Sent", 2, pick(func(s NodeSample) float64 { return sumNets(s, func(n NodeNet) float64 { return n.TxBps }) })}),
		build("Temperatures", formatTemp, 0, temps...),
		byteChart("COS process", formatBytes,
			chartLine{"Resident", 1, pick(func(s NodeSample) float64 { return float64(s.Proc.RSS) })},
			chartLine{"Heap", 2, pick(func(s NodeSample) float64 { return float64(s.Proc.HeapAlloc) })}),
		build("Goroutines", formatNumber, 0,
			chartLine{"Goroutines", 1, pick(func(s NodeSample) float64 { return float64(s.Proc.Goroutines) })}),
		build("T3 threads", formatNumber, 0,
			chartLine{"Running", 1, pick(func(s NodeSample) float64 { return float64(s.T3.Running) })},
			chartLine{"Open", 2, pick(func(s NodeSample) float64 { return float64(s.T3.Threads) })}),
	}
}

// peakOf is the largest finite value across the lines, the axis top when a chart has no fixed scale.
func peakOf(lines []chartLine) float64 {
	peak := 0.0
	for _, l := range lines {
		for _, v := range l.Values {
			if isFinite(v) {
				peak = math.Max(peak, v)
			}
		}
	}
	return peak
}

// sortedMounts lists a sample's disk mounts by path.
func sortedMounts(s NodeSample) []string {
	mounts := make([]string, 0, len(s.Disks))
	for _, d := range s.Disks {
		mounts = append(mounts, d.Mount)
	}
	slices.Sort(mounts)
	return mounts
}

func diskUsed(s NodeSample, mount string) float64 {
	for _, d := range s.Disks {
		if d.Mount == mount {
			return d.UsedPercent()
		}
	}
	return math.NaN()
}

func sumDisks(s NodeSample, get func(NodeDisk) float64) float64 {
	var total float64
	for _, d := range s.Disks {
		total += get(d)
	}
	return total
}

func sumNets(s NodeSample, get func(NodeNet) float64) float64 {
	var total float64
	for _, n := range s.Nets {
		total += get(n)
	}
	return total
}

// hottestSensors picks the five sensors with the highest peak, then sorts them by name so colors follow names.
func hottestSensors(in []NodeSample) []string {
	peak := map[string]float64{}
	for _, s := range in {
		for _, t := range s.Temps {
			if v, ok := peak[t.Name]; !ok || t.Celsius > v {
				peak[t.Name] = t.Celsius
			}
		}
	}
	names := slices.Collect(maps.Keys(peak))
	slices.SortFunc(names, func(a, b string) int {
		return cmp.Or(cmp.Compare(peak[b], peak[a]), strings.Compare(a, b))
	})
	names = names[:min(len(names), 5)]
	slices.Sort(names)
	return names
}

func tempAt(s NodeSample, name string) float64 {
	for _, t := range s.Temps {
		if t.Name == name {
			return t.Celsius
		}
	}
	return math.NaN()
}

// tableRows picks at most 24 evenly spaced rows for a chart's data table.
func tableRows(n int) []int {
	step := max(1, (n+23)/24)
	var rows []int
	for i := 0; i < n; i += step {
		rows = append(rows, i)
	}
	return rows
}

// chartPaths draws values against time in a 100 × 100 box scaled to ymax. A missing value, or a gap longer than 3 times the median step, starts a new line.
func chartPaths(times []time.Time, values []float64, t0, t1 time.Time, ymax float64) string {
	n := min(len(times), len(values))
	span := t1.Sub(t0)
	if n == 0 || span <= 0 || !(ymax > 0) {
		return ""
	}
	limit := 3 * medianStep(times[:n])
	broken := svgPoint{X: math.NaN(), Y: math.NaN()}
	var pts []svgPoint
	for i := range n {
		if i > 0 && limit > 0 && times[i].Sub(times[i-1]) > limit {
			pts = append(pts, broken)
		}
		if !isFinite(values[i]) {
			pts = append(pts, broken)
			continue
		}
		x := 100 * float64(times[i].Sub(t0)) / float64(span)
		y := 100 - 100*values[i]/ymax
		pts = append(pts, svgPoint{X: clamp(x), Y: clamp(y)})
	}
	return lineD(pathRuns(pts))
}

// medianStep is the typical time between samples.
func medianStep(times []time.Time) time.Duration {
	if len(times) < 2 {
		return 0
	}
	steps := make([]time.Duration, 0, len(times)-1)
	for i := 1; i < len(times); i++ {
		steps = append(steps, times[i].Sub(times[i-1]))
	}
	slices.Sort(steps)
	return steps[len(steps)/2]
}

func clamp(v float64) float64 { return math.Max(0, math.Min(100, v)) }

// niceMax rounds v up to 1, 2, 2.5 or 5 times a power of ten, so axes end on round numbers.
func nodeNiceMax(v float64) float64 {
	if !isFinite(v) || v <= 0 {
		return 1
	}
	base := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if m*base >= v {
			return m * base
		}
	}
	return 10 * base
}

// axisMax is the top of a five-gridline axis: four nice steps, so every tick lands on a round number.
func axisMax(peak float64) float64 { return 4 * nodeNiceMax(peak/4) }

// byteAxisMax is axisMax in the binary unit formatBytes shows, so byte ticks read 2.0 MiB, 1.5 MiB, not 1.9 MiB, 1.4 MiB.
func byteAxisMax(peak float64) float64 {
	unit := 1.0
	for peak/unit >= 1024 && unit < 1<<40 {
		unit *= 1024
	}
	return unit * axisMax(peak/unit)
}

// yTicks labels the five gridlines from the top of the axis down to zero, rounded so float noise never shows.
func yTicks(ymax float64, format func(float64) string) []string {
	out := make([]string, 5)
	for i := range out {
		out[i] = format(math.Round(ymax*float64(4-i)/4*1e9) / 1e9)
	}
	return out
}

// xTicks labels five instants from start to end, left to right, in UTC.
func xTicks(start, end time.Time, layout string) []string {
	out := make([]string, 5)
	for i := range out {
		out[i] = start.Add(end.Sub(start) * time.Duration(i) / 4).UTC().Format(layout)
	}
	return out
}
