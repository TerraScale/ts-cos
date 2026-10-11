package charts

import (
	"bytes"
	"strings"
	"testing"
)

func TestLegendNamesEverySeriesWithItsSwatch(t *testing.T) {
	data := NewLine("CPU", nil, []string{"a", "b"}, 120, Series{Name: "delta", Values: []int64{1, 2}}, Series{Name: "beta", Values: []int64{2, 1}})
	var html bytes.Buffer
	if err := Legend(data).Render(t.Context(), &html); err != nil {
		t.Fatal(err)
	}
	out := html.String()
	for _, want := range []string{`class="ts-chart-legend"`, `class="ts-chart-legend-item"`, `class="ts-chart-swatch `, ">delta<", ">beta<"} {
		if !strings.Contains(out, want) {
			t.Fatalf("legend lacks %q: %s", want, out)
		}
	}
	if got := strings.Count(out, `class="ts-chart-legend-item"`); got != 2 {
		t.Fatalf("legend items=%d, want 2", got)
	}
	if strings.Contains(out, "style=") {
		t.Fatalf("legend has inline style: %s", out)
	}
}
