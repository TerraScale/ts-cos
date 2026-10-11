package cos

import (
	"context"
	"errors"
	"testing"
)

func TestAutopilotHoldsLeaseOnlyForItself(t *testing.T) {
	cases := []struct {
		name   string
		holder string
		err    error
		want   bool
	}{
		{"own lease", "beta", nil, true},
		{"lease moved", "mariogk-dev", nil, false},
		{"lease unreadable", "beta", errors.New("COS cluster has no leader right now"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := &Autopilot{Host: "beta", leaseHolder: func(context.Context) (string, error) { return c.holder, c.err }}
			if got := a.holdsLease(context.Background()); got != c.want {
				t.Fatalf("holdsLease = %v, want %v", got, c.want)
			}
		})
	}
}
