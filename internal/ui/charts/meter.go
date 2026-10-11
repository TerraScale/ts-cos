// Package charts prepares accessible, server-rendered SVG chart geometry.
package charts

import (
	"fmt"
	"math"
	"strconv"
)

type RingData struct {
	Name    string
	Value   string
	Label   string
	Tone    string
	Dash    string
	Percent int
}

type BarData struct {
	Name  string
	Value string
	Label string
	Width string
}

// NewBar keeps progress geometry in SVG attributes, which work with a strict
// style-src CSP and remain readable without scripts. The tooltip shows name
// and percent; of completes the accessible label ("name: n percent of …").
func NewBar(name string, percent int, of string) BarData {
	return BarData{
		Name: name, Value: strconv.Itoa(percent) + "%",
		Label: fmt.Sprintf("%s: %d percent of %s", name, percent, of),
		Width: strconv.Itoa(min(100, max(0, percent))),
	}
}

func NewRing(label string, used, capacity int64) RingData {
	if capacity <= 0 {
		return RingData{Name: label, Value: "Capacity unavailable", Label: label + ": capacity unavailable", Tone: "success", Dash: "0 100"}
	}
	percentage := int(math.Round(float64(used) / float64(capacity) * 100))
	tone := "success"
	if percentage >= 100 {
		tone = "danger"
	} else if percentage >= 80 {
		tone = "warning"
	}
	return RingData{
		Name: label, Value: strconv.Itoa(percentage) + "%",
		Label: fmt.Sprintf("%s: %d%% of capacity", label, percentage),
		Tone:  tone, Dash: strconv.Itoa(min(100, max(0, percentage))) + " 100", Percent: percentage,
	}
}
