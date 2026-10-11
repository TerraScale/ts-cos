package charts

import (
	"math"
	"time"
)

// TimeTicks keeps UTC axis labels short and avoids repeated labels when a
// provider returns fewer than five buckets. Tooltips retain full timestamps.
func TimeTicks(starts []time.Time, rangeKey string) []string {
	ticks := make([]string, 5)
	if len(starts) == 0 {
		return ticks
	}
	seen := make(map[int]bool)
	format := "15:04"
	if rangeKey == "7d" || rangeKey == "30d" {
		format = "Jan 2 15:04"
	}
	for step := range 5 {
		index := int(math.Round(float64(step) * float64(len(starts)-1) / 4))
		if len(starts) > 1 && index == len(starts)-1 && step != 4 {
			continue
		}
		if !seen[index] {
			ticks[step] = starts[index].UTC().Format(format)
			seen[index] = true
		}
	}
	return ticks
}
