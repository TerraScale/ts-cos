package cos

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestT3DownVerdict(t *testing.T) {
	const ghost = "e2e-ab12cd"
	ghostDown := HostFleet{Host: ghost, Error: "T3 e2e-ab12cd /api/orchestration/shell: connection failed"}
	realHosts := []HostFleet{{Host: "beta"}, {Host: "delta"}, {Host: "mariogk-dev"}}
	tests := []struct {
		name    string
		fleet   []HostFleet
		wantErr string // empty: the ghost is reported and every real host answers
	}{
		{"ghost reported, real hosts answer", append([]HostFleet{ghostDown}, realHosts...), ""},
		{"ghost answers instead of failing", append([]HostFleet{{Host: ghost}}, realHosts...), "does not report a failed T3 connection"},
		{"ghost error leaks the token", append([]HostFleet{{Host: ghost, Error: "T3 e2e-ab12cd http://127.0.0.1:1 e2e-dummy: connection failed"}}, realHosts...), "leaks the T3 token or URL"},
		{"real host fails", []HostFleet{ghostDown, {Host: "beta"}, {Host: "delta", Error: "COS request failed; local node unavailable"}, {Host: "mariogk-dev"}}, "delta: COS request failed"},
		{"real host missing", append([]HostFleet{ghostDown}, HostFleet{Host: "beta"}, HostFleet{Host: "delta"}), "mariogk-dev did not answer"},
		{"ghost missing", realHosts, "does not report a failed T3 connection"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := t3DownVerdict(tc.fleet, ghost)
			if tc.wantErr == "" {
				if err != nil || !strings.HasPrefix(got, ghost+": T3 connection failed reported per host") {
					t.Fatalf("t3DownVerdict = %q, %v; want the ghost reported", got, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("t3DownVerdict error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

// A client bus that serves a host must answer its operations without wedging the bus.
func TestClientBusServesHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ports := busTestPorts(t, 2)
	cfg, node := busTestConfig(t, "delta", ports[0], ports[1], nil)
	server, err := startBus(ctx, cfg, node, func(context.Context, string, []string) (any, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()
	cfg.HTTP = e2eHTTP
	open := func(context.Context) (*T3, error) {
		return &T3{Config: cfg, Host: "e2e-ab12cd", BaseURL: "http://127.0.0.1:1", token: "e2e-dummy"}, nil
	}
	if err := bus.addService("e2e-ab12cd", func(ctx context.Context, command string, args []string) (any, error) {
		return localCommand(ctx, open, command, args)
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		err := bus.Call(ctx, "e2e-ab12cd", "shell", nil, nil)
		if err == nil || !strings.Contains(err.Error(), "connection failed") {
			t.Fatalf("Call = %v, want the T3 connection failure", err)
		}
	}
}
