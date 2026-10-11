package cos

import (
	"errors"
	"html"
	"strings"
	"testing"
)

func TestModelsPageShows(t *testing.T) {
	p := freshPolicy(t)
	summary, hash := modelsSummary(p), "policy "+modelsHash(p)
	if form, err := modelsPageShows("<p>"+summary+" · "+hash+"</p>", p); err != nil || form != "raw" {
		t.Fatalf("raw page: form %q, err %v", form, err)
	}
	// The summary has no characters html escapes, so the escaped form equals the raw one; the check still accepts either.
	if form, err := modelsPageShows("<p>"+html.EscapeString(summary)+" · "+hash+"</p>", p); err != nil || form == "" {
		t.Fatalf("escaped page: form %q, err %v", form, err)
	}
	if _, err := modelsPageShows("<p>"+summary+" · policy 00000000</p>", p); err == nil {
		t.Fatal("a stale hash passed")
	}
	if _, err := modelsPageShows("<p>policy "+modelsHash(p)+"</p>", p); err == nil {
		t.Fatal("a page without the summary passed")
	}
}

func TestGuardRefusal(t *testing.T) {
	if err := guardRefusal(OperationError("gpt-6-luna is a worker-only model; threads run on an advisor model"), "worker-only"); err != nil {
		t.Fatalf("matching refusal: %v", err)
	}
	if err := guardRefusal(errors.New("timeout"), "worker-only"); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("unrelated error passed or lost its text: %v", err)
	}
}
