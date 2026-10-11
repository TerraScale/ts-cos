package charts

import (
	"testing"
	"time"
)

func TestTimeTicksKeepSparseUTCLabelsDistinct(t *testing.T) {
	start := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	for _, count := range []int{0, 1, 2, 3, 4, 5, 60} {
		starts := make([]time.Time, count)
		for i := range starts {
			starts[i] = start.Add(time.Duration(i) * time.Minute)
		}
		ticks := TimeTicks(starts, "1h")
		seen := map[string]bool{}
		for _, tick := range ticks {
			if tick == "" {
				continue
			}
			if seen[tick] {
				t.Fatalf("%d buckets repeat %q", count, tick)
			}
			seen[tick] = true
		}
		if count > 1 && ticks[4] != starts[count-1].Format("15:04") {
			t.Fatalf("last tick=%q", ticks[4])
		}
	}
}
