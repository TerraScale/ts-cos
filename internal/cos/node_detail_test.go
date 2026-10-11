package cos

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestChartPaths(t *testing.T) {
	t0 := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	if got := chartPaths(nil, nil, t0, t1, 100); got != "" {
		t.Fatalf("empty = %q", got)
	}
	if got := chartPaths([]time.Time{at(30 * time.Second)}, []float64{50}, t0, t1, 100); got != "M50.00 50.00 L50.00 50.00" {
		t.Fatalf("single = %q", got)
	}
	if got := chartPaths([]time.Time{at(0), at(30 * time.Second)}, []float64{0, 0}, t0, t1, 100); got != "M0.00 100.00 L50.00 100.00" {
		t.Fatalf("flat zero = %q", got)
	}
	// Four 1-minute steps, then a 17-minute gap: the median step is 1 minute, so the gap starts a second line.
	times := []time.Time{at(0), at(time.Minute), at(2 * time.Minute), at(3 * time.Minute), at(20 * time.Minute)}
	if got := chartPaths(times, []float64{50, 50, 50, 50, 50}, t0, t0.Add(30*time.Minute), 100); strings.Count(got, "M") != 2 {
		t.Fatalf("gap should start a second line: %q", got)
	}
}

func TestNodeNiceMax(t *testing.T) {
	for in, want := range map[float64]float64{0: 1, -3: 1, math.NaN(): 1, 0.3: 0.5, 1: 1, 1.1: 2, 2.2: 2.5, 4: 5, 5.1: 10, 97: 100, 1234: 2000} {
		if got := nodeNiceMax(in); got != want {
			t.Errorf("nodeNiceMax(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestAxisMax(t *testing.T) {
	// Each step is niceMax(peak/4), so the top is 4 steps and every tick is a round number.
	for in, want := range map[float64]float64{0: 4, 2.21: 4, 4: 4, 4.1: 8, 97: 100, 1234: 2000} {
		if got := axisMax(in); got != want {
			t.Errorf("axisMax(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestAxisTicks(t *testing.T) {
	if got := strings.Join(yTicks(100, formatPercent), " "); got != "100% 75% 50% 25% 0%" {
		t.Fatalf("y ticks = %q", got)
	}
	if got := strings.Join(yTicks(axisMax(2.21), formatNumber), " "); got != "4 3 2 1 0" {
		t.Fatalf("load ticks = %q", got)
	}
	if got := strings.Join(yTicks(byteAxisMax(1.9e6), formatRate), " "); got != "2.0 MiB/s 1.5 MiB/s 1.0 MiB/s 512.0 KiB/s 0 B/s" {
		t.Fatalf("byte ticks = %q", got)
	}
	if got := byteAxisMax(100e6); got != 100<<20 {
		t.Fatalf("byteAxisMax(100e6) = %v, want 100 MiB", got)
	}
	// 0.4 / 4 · 3 is 0.30000000000000004 in floating point; the label must read 0.3.
	if got := strings.Join(yTicks(0.4, formatNumber), " "); got != "0.4 0.3 0.2 0.1 0" {
		t.Fatalf("fractional ticks = %q", got)
	}
	start := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	if got := strings.Join(xTicks(start, start.Add(7*24*time.Hour), "Jan 2"), " "); got != "Oct 4 Oct 6 Oct 8 Oct 9 Oct 11" {
		t.Fatalf("7d x ticks = %q", got)
	}
}

func TestThinSamplesKeepsOrderAndLimit(t *testing.T) {
	start := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	in := make([]NodeSample, 1441)
	for i := range in {
		in[i] = nodeFixture("delta", start.Add(time.Duration(i)*time.Minute), 1)
	}
	out := thinSamples("delta", in, nodeMaxPoints)
	if len(out) == 0 || len(out) > nodeMaxPoints {
		t.Fatalf("thinned to %d points, want 1..%d", len(out), nodeMaxPoints)
	}
	for i := 1; i < len(out); i++ {
		if !out[i].Time.After(out[i-1].Time) {
			t.Fatalf("time order broken at %d", i)
		}
	}
	if short := thinSamples("delta", in[:100], nodeMaxPoints); len(short) != 100 {
		t.Fatalf("short series changed to %d points", len(short))
	}
}

func TestNodeWindowFor(t *testing.T) {
	for in, want := range map[string]string{"": "1h", "bogus": "1h", "1h": "1h", "24h": "24h", "7d": "7d"} {
		if got, _ := nodeWindowFor(in); got != want {
			t.Errorf("nodeWindowFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNodeChartsRuleAndTables(t *testing.T) {
	start := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	in := make([]NodeSample, 30)
	for i := range in {
		in[i] = nodeFixture("delta", start.Add(time.Duration(i)*time.Minute), float64(i))
	}
	charts := nodeCharts(in, start, start.Add(time.Hour), "1h")
	if len(charts) != 10 {
		t.Fatalf("charts = %d, want 10", len(charts))
	}
	for _, c := range charts {
		if len(c.Lines) > 5 {
			t.Errorf("%s has %d series, max is 5", c.Title, len(c.Lines))
		}
		if len(tableRows(len(c.Times))) > 24 {
			t.Errorf("%s table exceeds 24 rows", c.Title)
		}
	}
	empty := nodeCharts(nil, start, start.Add(time.Hour), "1h")
	if !empty[0].empty() {
		t.Fatal("no samples should give an empty chart")
	}
	if got := nodeCharts(in, start, start.Add(24*time.Hour), "24h")[0].XTicks; got[0] != "Oct 10 00:00" || got[4] != "Oct 11 00:00" {
		t.Fatalf("24h x ticks = %q, want dates on both ends", got)
	}
	if got := nodeCharts(in, start, start.Add(time.Hour), "1h")[0].XTicks; got[0] != "00:00" {
		t.Fatalf("1h x ticks = %q, want clock times", got)
	}
}

func TestNodeChartEmptyStates(t *testing.T) {
	now := time.Now()
	var buf bytes.Buffer
	// Samples but no lines, as a VM with no disks or sensors: the chart has data to plot, just no series.
	noLines := nodeChart{Title: "Disks used", Times: []time.Time{now}, YMax: 100}
	if noLines.empty() {
		t.Fatal("samples without lines should not count as empty")
	}
	if err := nodeChartView(noLines).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `<p class="chart-empty">No data</p>`) {
		t.Errorf("no-lines chart should say No data: %s", buf.String())
	}
	buf.Reset()
	noSamples := nodeChart{Title: "Disks used", YMax: 100}
	if !noSamples.empty() {
		t.Fatal("no samples should count as empty")
	}
	if err := nodeChartView(noSamples).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "No samples in this window") {
		t.Errorf("no-samples chart should say No samples in this window: %s", buf.String())
	}
}

func TestNodeDetailRender(t *testing.T) {
	now := time.Now()
	series := make([]NodeSample, 30)
	for i := range series {
		series[i] = nodeFixture("delta", now.Add(-time.Duration(30-i)*time.Minute), float64(i%5))
	}
	card := nodeFixtureCards(now)[0]
	var buf bytes.Buffer
	if err := nodeDetailPage(card, "24h", nodeCharts(series, now.Add(-24*time.Hour), now, "24h")).Render(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	html := buf.String()
	if n := strings.Count(html, "<figure"); n < 10 {
		t.Errorf("figures = %d, want at least 10", n)
	}
	if !strings.Contains(html, `<details class="disclosure">`) || !strings.Contains(html, `class="legend"`) {
		t.Error("multi-series charts need a legend and a data table")
	}
	if !strings.Contains(html, `value="24h" aria-pressed="true"`) {
		t.Error("range control does not mark 24h as pressed")
	}
	assertCleanMarkup(t, html)
}

func TestNodeDetailRoutesWithoutBus(t *testing.T) {
	w := &Web{}
	mux := w.routes()
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec
	}
	if rec := get("/nodes/beta?range=bogus"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "no samples yet") {
		t.Fatalf("GET /nodes/beta = %d", rec.Code)
	}
	if rec := get("/nodes/bad.host"); rec.Code != http.StatusNotFound {
		t.Fatalf("invalid host = %d, want 404", rec.Code)
	}
}
