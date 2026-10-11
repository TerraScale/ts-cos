package cos

import (
	"math"
	"testing"
)

func TestSparkPaths(t *testing.T) {
	if line, area := sparkPaths(nil, 0); line != "" || area != "" {
		t.Fatalf("empty = %q %q", line, area)
	}
	line, area := sparkPaths([]float64{5}, 0)
	if line != "M0.00 0.00 L100.00 0.00" || area != "M0.00 24 L0.00 0.00 L100.00 0.00 L100.00 24 Z" {
		t.Fatalf("single = %q %q", line, area)
	}
	if line, _ := sparkPaths([]float64{0, 0, 0}, 0); line != "M0.00 24.00 L50.00 24.00 L100.00 24.00" {
		t.Fatalf("flat zero = %q", line)
	}
	// The NaN splits the line in two: a missing sample is a gap, not a slope.
	if line, _ := sparkPaths([]float64{1, math.NaN(), 2}, 0); line != "M0.00 12.00 L0.00 12.00 M100.00 0.00 L100.00 0.00" {
		t.Fatalf("gap = %q", line)
	}
	// A steady 52 % on a 0–100 scale sits low in the box, not at the top as it would on its own max.
	if line, _ := sparkPaths([]float64{52, 52}, 100); line != "M0.00 11.52 L100.00 11.52" {
		t.Fatalf("percent scale = %q", line)
	}
}

func TestFormatHelpers(t *testing.T) {
	inf := math.Inf(1)
	cases := map[string]struct{ got, want string }{
		"bytes 0":      {formatBytes(0), "0 B"},
		"bytes KiB":    {formatBytes(1536), "1.5 KiB"},
		"bytes -KiB":   {formatBytes(-2048), "-2.0 KiB"},
		"bytes GiB":    {formatBytes(3 << 30), "3.0 GiB"},
		"bytes NaN":    {formatBytes(math.NaN()), "–"},
		"rate":         {formatRate(2048), "2.0 KiB/s"},
		"percent":      {formatPercent(42.4), "42%"},
		"temp":         {formatTemp(61.6), "62°C"},
		"uptime 45s":   {formatUptime(45), "45s"},
		"uptime 5h":    {formatUptime(5*3600 + 12*60), "5h 12m"},
		"uptime 3d":    {formatUptime(3*86400 + 4*3600), "3d 4h"},
		"revision":     {shortRevision("0123456789abcdef"), "0123456789ab"},
		"build time":   {formatBuildTime("2026-10-10T09:00:00Z"), "Oct 10 09:00 UTC"},
		"build offset": {formatBuildTime("2026-10-10T11:00:00+02:00"), "Oct 10 09:00 UTC"},
		"build raw":    {formatBuildTime("yesterday"), "yesterday"},
		"number Inf":   {formatNumber(inf), "–"},
		"number small": {formatNumber(2.5), "2.5"},
	}
	for name, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", name, c.got, c.want)
		}
	}
}
