package cos

import (
	"context"
	"slices"
	"strings"
	"syscall"
	"testing"
)

func TestTunnelCommandKeepsTokenOffTheCommandLine(t *testing.T) {
	cmd := tunnelCommand(context.Background(), "PRIVATE-TUNNEL-TOKEN")
	if strings.Contains(strings.Join(cmd.Args, " "), "PRIVATE") || !slices.Contains(cmd.Env, "TUNNEL_TOKEN=PRIVATE-TUNNEL-TOKEN") {
		t.Fatalf("token placement: args %q", cmd.Args)
	}
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Fatal("cloudflared would outlive a dead node and keep receiving traffic")
	}
}
