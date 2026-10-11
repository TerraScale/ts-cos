package charts

import (
	"bytes"
	"strings"
	"testing"
)

func TestRingAndBarRenderWithoutInlineStyles(t *testing.T) {
	var ring, bar bytes.Buffer
	if err := Ring(NewRing("Disk", 30, 100)).Render(t.Context(), &ring); err != nil {
		t.Fatal(err)
	}
	if err := Bar(NewBar("CPU", 42, "8 cores")).Render(t.Context(), &bar); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`class="ts-chart-meter"`, `class="ts-ring ts-ring-success"`, `class="ts-ring-fill"`,
		`stroke-dasharray="30 100"`, `aria-label="Disk: 30% of capacity"`, `<strong>30%</strong>`,
	} {
		if !strings.Contains(ring.String(), want) {
			t.Errorf("ring missing %s in %s", want, ring.String())
		}
	}
	for _, want := range []string{
		`class="ts-chart-meter"`, `class="ts-svg-sharebar-fill"`, `width="42"`,
		`aria-label="CPU: 42 percent of 8 cores"`, `<strong>42%</strong>`,
	} {
		if !strings.Contains(bar.String(), want) {
			t.Errorf("bar missing %s in %s", want, bar.String())
		}
	}
	for _, html := range []string{ring.String(), bar.String()} {
		if strings.Contains(html, "style=") {
			t.Errorf("meter emits inline style: %s", html)
		}
	}
}

func TestNewRingEdgeCases(t *testing.T) {
	// Zero capacity is shown as unavailable, not as 0% or a division by zero.
	if got := NewRing("Disk", 10, 0); got.Value != "Capacity unavailable" || got.Tone != "success" || got.Dash != "0 100" || got.Percent != 0 {
		t.Errorf("zero capacity: got %+v", got)
	}
	// Usage above capacity keeps the real percent but clamps the arc to a full ring.
	if got := NewRing("Disk", 150, 100); got.Value != "150%" || got.Tone != "danger" || got.Dash != "100 100" || got.Percent != 150 {
		t.Errorf("used over capacity: got %+v", got)
	}
}
