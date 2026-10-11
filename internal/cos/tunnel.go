package cos

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// Cloudflare Tunnel "cos" (88533e25-445e-4353-9739-2e4a5509fab6) routes
// cos.terrascale.tech to http://localhost:8790. Every node runs a connector, so
// Cloudflare fails over to the others when a node stops.
const tunnelSecret = "cos-tunnel"

// runTunnel keeps this node's cloudflared connector running until ctx ends.
func runTunnel(ctx context.Context) {
	for {
		err := tunnelOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		slog.Error("COS tunnel connector stopped; restarting", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func tunnelOnce(ctx context.Context) error {
	api, err := newOCIVault()
	if err != nil {
		return err
	}
	token, err := api.read(ctx, tunnelSecret)
	if err != nil {
		return err
	}
	cmd := tunnelCommand(ctx, string(token))
	clear(token)
	if err := cmd.Run(); err != nil {
		return errors.New("cloudflared exited: " + err.Error())
	}
	return errors.New("cloudflared exited")
}

// tunnelCommand passes the token in the environment, never on the command line,
// and ties cloudflared's life to this process so a dead node stops serving.
func tunnelCommand(ctx context.Context, token string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "cloudflared", "tunnel", "--no-autoupdate", "run")
	cmd.Env = append(os.Environ(), "TUNNEL_TOKEN="+token)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	return cmd
}
