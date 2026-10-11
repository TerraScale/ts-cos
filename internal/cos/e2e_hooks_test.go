package cos

import (
	"encoding/json"
	"errors"
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

func TestWithHookRetry(t *testing.T) {
	plain := errors.New("status 401")
	for _, tc := range []struct {
		name    string
		errs    []error
		want    string
		wantErr error
		calls   int
	}{
		{"traffic twice then ok", []error{errHookTraffic, errHookTraffic, nil}, "ok (attempt 3", nil, 3},
		{"traffic every time", []error{errHookTraffic, errHookTraffic, errHookTraffic}, "", errHookTraffic, 3},
		{"plain error stops at once", []error{plain}, "", plain, 1},
		{"ok first has no suffix", []error{nil}, "ok", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			got, err := withHookRetry(func() (string, error) {
				calls++
				if calls > len(tc.errs) {
					t.Fatalf("probe called %d times, want %d", calls, tc.calls)
				}
				if tc.errs[calls-1] != nil {
					return "", tc.errs[calls-1]
				}
				return "ok", nil
			})
			if calls != tc.calls {
				t.Errorf("probe called %d times, want %d", calls, tc.calls)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && !strings.HasPrefix(got, tc.want) {
				t.Errorf("output %q, want prefix %q", got, tc.want)
			}
			if tc.want == "ok" && got != "ok" {
				t.Errorf("output %q gained an attempt suffix on the first try", got)
			}
		})
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
