package cos

import (
	"strings"
	"testing"
)

func TestCheckForgejo5xx(t *testing.T) {
	got, err := checkForgejo5xx(t.Context(), &E2EEnv{cfg: Config{LocalHost: "delta"}})
	if err != nil || !strings.HasPrefix(got, "503:") {
		t.Fatalf("checkForgejo5xx = %q, %v; want a 503 evidence line", got, err)
	}
	// A Forgejo that answers 200 from the start must fail the check, or it proves nothing.
	if got, err := forgejo5xxScenario(t.Context(), false); err == nil {
		t.Fatalf("healthy Forgejo passed the 5xx check with %q", got)
	}
}
