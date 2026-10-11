package cos

import (
	"bufio"
	"strings"
	"testing"
)

func TestScanForStopsAtMarker(t *testing.T) {
	stream := "event: datastar-patch-elements\ndata: elements <section id=\"kpis\" data-probe=\"\">\n" +
		"event: datastar-patch-elements\ndata: elements <section id=\"kpis\" data-probe=\"cos:kpi-probe:a:1\">\n"
	sc := bufio.NewScanner(strings.NewReader(stream))
	if !scanFor(sc, `id="kpis"`) {
		t.Fatal("kpis line not found")
	}
	if !scanFor(sc, `data-probe="cos:kpi-probe:a:1"`) {
		t.Fatal("probe line not found after the kpis line")
	}
	if scanFor(sc, `data-probe="cos:kpi-probe:a:2"`) {
		t.Fatal("matched a probe that never arrived")
	}
}
