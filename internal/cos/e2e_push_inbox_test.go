package cos

import (
	"testing"
	"time"
)

func TestPushInboxEvidence(t *testing.T) {
	at := time.Date(2026, 10, 11, 9, 30, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		last pushLast
		age  time.Duration
		want string
	}{
		{"pushed", pushLast{Item: "a1", Severity: "warn", Sent: 2, At: at}, 90 * time.Second,
			"Inbox item a1 (warn, filed 1m30s ago) pushed to 2 devices at 2026-10-11T09:30:00Z; notifier pending 0"},
		{"info", pushLast{Item: "a2", Severity: "info", At: at}, 5 * time.Second,
			"Inbox item a2 (info, filed 5s ago) info item, not pushed by design at 2026-10-11T09:30:00Z; notifier pending 0"},
	} {
		if got := pushInboxEvidence(tc.last, tc.age, 0); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
