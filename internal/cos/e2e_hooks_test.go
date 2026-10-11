package cos

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestForgejoProbeIsDroppedByTheFilter(t *testing.T) {
	var p forgejoPayload
	if err := json.Unmarshal(forgejoProbeBody("0123abcd", time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)), &p); err != nil {
		t.Fatal(err)
	}
	if got := normalizeForgejo("pull_request", p, time.Now()); got != nil {
		t.Fatalf("foreign-author probe normalized to %d events, want none", len(got))
	}
}

func TestForgejoProbeSignatureMatchesHooks(t *testing.T) {
	p := newForgejoProbe(time.Now())
	header := http.Header{}
	header.Set("X-Forgejo-Signature", signHookBody("s3cret", p.body))
	if !validHookSignature("s3cret", header, p.body) {
		t.Fatal("hooks.go rejects a probe signed with the secret")
	}
	if validHookSignature("other", header, p.body) {
		t.Fatal("hooks.go accepts a probe signed with another secret")
	}
	if validHookSignature("s3cret", header, append(p.body, ' ')) {
		t.Fatal("hooks.go accepts a tampered body")
	}
}

func TestForgejoProbeFreshDelivery(t *testing.T) {
	a, b := newForgejoProbe(time.Now()), newForgejoProbe(time.Now())
	if !strings.HasPrefix(a.delivery, "e2e-") || len(a.delivery) != len("e2e-")+32 {
		t.Fatalf("delivery %q is not e2e-<32 hex>", a.delivery)
	}
	if a.delivery == b.delivery {
		t.Fatal("two probes share a delivery ID")
	}
}

func TestIngestAdded(t *testing.T) {
	for _, tc := range []struct {
		before, after uint64
		want          int64
	}{{5, 6, 1}, {6, 6, 0}, {7, 6, -1}} {
		if got := ingestAdded(tc.before, tc.after); got != tc.want {
			t.Errorf("ingestAdded(%d, %d) = %d, want %d", tc.before, tc.after, got, tc.want)
		}
	}
}
