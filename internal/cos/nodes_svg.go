package cos

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// noValue stands in for a missing or non-finite number, so NaN and Inf never reach markup.
const noValue = "–"

// svgPoint is one vertex in a 0..100 box; a NaN coordinate ends the current line.
type svgPoint struct{ X, Y float64 }

func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// sparkPaths draws values as a line and its filled area in a 100 × 24 box, scaled to ymax, or to the largest value when ymax ≤ 0.
func sparkPaths(values []float64, ymax float64) (line, area string) {
	if len(values) == 0 {
		return "", ""
	}
	if len(values) == 1 {
		values = []float64{values[0], values[0]} // one sample draws a flat line
	}
	top := ymax
	if top <= 0 {
		top = 0
		for _, v := range values {
			if isFinite(v) {
				top = math.Max(top, v)
			}
		}
	}
	pts := make([]svgPoint, len(values))
	for i, v := range values {
		pts[i] = svgPoint{X: 100 * float64(i) / float64(len(values)-1), Y: 24}
		switch {
		case !isFinite(v):
			pts[i].Y = math.NaN()
		case top > 0:
			pts[i].Y = 24 - 24*v/top
		}
	}
	runs := pathRuns(pts)
	return lineD(runs), areaD(runs)
}

// pathRuns splits points wherever one is not finite, so a gap shows as a gap, not a slope.
func pathRuns(pts []svgPoint) [][]svgPoint {
	var runs [][]svgPoint
	var run []svgPoint
	for _, p := range pts {
		if !isFinite(p.X) || !isFinite(p.Y) {
			if len(run) > 0 {
				runs = append(runs, run)
			}
			run = nil
			continue
		}
		run = append(run, p)
	}
	if len(run) > 0 {
		runs = append(runs, run)
	}
	return runs
}

// lineD writes runs as SVG path data; a lone point becomes a zero-length segment so it still draws a dot.
func lineD(runs [][]svgPoint) string {
	var b strings.Builder
	for _, run := range runs {
		if len(run) == 1 {
			run = append(run, run[0])
		}
		for i, p := range run {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			cmd := "L"
			if i == 0 {
				cmd = "M"
			}
			b.WriteString(cmd + num(p.X) + " " + num(p.Y))
		}
	}
	return b.String()
}

// areaD closes each run down to the sparkline baseline at y = 24.
func areaD(runs [][]svgPoint) string {
	var b strings.Builder
	for _, run := range runs {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString("M" + num(run[0].X) + " 24")
		for _, p := range run {
			b.WriteString(" L" + num(p.X) + " " + num(p.Y))
		}
		b.WriteString(" L" + num(run[len(run)-1].X) + " 24 Z")
	}
	return b.String()
}

func num(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }

// formatBytes writes a byte count in binary units, one decimal above bytes.
func formatBytes(n float64) string {
	if !isFinite(n) {
		return noValue
	}
	if math.Abs(n) < 1024 {
		return fmt.Sprintf("%.0f B", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	i := -1
	for math.Abs(n) >= 1024 && i < len(units)-1 {
		n /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %s", n, units[i])
}

func formatRate(n float64) string {
	if !isFinite(n) {
		return noValue
	}
	return formatBytes(n) + "/s"
}

func formatPercent(n float64) string {
	if !isFinite(n) {
		return noValue
	}
	return fmt.Sprintf("%.0f%%", n)
}

func formatTemp(n float64) string {
	if !isFinite(n) {
		return noValue
	}
	return fmt.Sprintf("%.0f°C", n)
}

func formatNumber(n float64) string {
	if !isFinite(n) {
		return noValue
	}
	return strconv.FormatFloat(n, 'f', -1, 64)
}

func formatFixed(n float64, decimals int) string {
	if !isFinite(n) {
		return noValue
	}
	return strconv.FormatFloat(n, 'f', decimals, 64)
}

// formatUptime writes seconds as "45s", "12m", "5h 12m" or "3d 4h".
func formatUptime(sec float64) string {
	if !isFinite(sec) || sec < 0 {
		return noValue
	}
	d := time.Duration(sec * float64(time.Second))
	switch {
	case d >= 24*time.Hour:
		return fmt.Sprintf("%dd %dh", d/(24*time.Hour), d%(24*time.Hour)/time.Hour)
	case d >= time.Hour:
		return fmt.Sprintf("%dh %dm", d/time.Hour, d%time.Hour/time.Minute)
	case d >= time.Minute:
		return fmt.Sprintf("%dm", d/time.Minute)
	}
	return fmt.Sprintf("%ds", d/time.Second)
}

// shortRevision is the first 12 characters of a build revision, the length the dashboard shows.
func shortRevision(rev string) string {
	if len(rev) > 12 {
		return rev[:12]
	}
	return rev
}

// formatBuildTime writes an RFC 3339 build time as "Oct 10 09:00 UTC", or returns the raw text when it does not parse.
func formatBuildTime(raw string) string {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return raw
	}
	return t.UTC().Format("Jan 2 15:04 MST")
}
