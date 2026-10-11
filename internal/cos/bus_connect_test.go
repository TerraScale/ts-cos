package cos

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// busAuthTestServer starts a loopback NATS server that accepts the COS client user with password.
func busAuthTestServer(t *testing.T, password string) (*server.Server, *server.Options, int) {
	t.Helper()
	port := busTestPorts(t, 1)[0]
	opts := &server.Options{Host: "127.0.0.1", Port: port, Username: busClientUser, Password: password, NoLog: true, NoSigs: true}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	busTestReady(t, "auth server", func() bool { return srv.ReadyForConnections(20 * time.Millisecond) })
	return srv, opts, port
}

// busAuthAttempts counts every connection the server has accepted, including rejected logins.
func busAuthAttempts(t *testing.T, srv *server.Server) uint64 {
	t.Helper()
	varz, err := srv.Varz(&server.VarzOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return varz.TotalConnections
}

func TestConnectBusRetriesWrongPasswordForTwoSeconds(t *testing.T) {
	srv, _, port := busAuthTestServer(t, "right")
	before := busAuthAttempts(t, srv)
	started := time.Now()
	conn, err := connectBus(t.Context(), busCredential{Port: port, Password: "wrong"})
	elapsed := time.Since(started)
	if conn != nil || err == nil {
		t.Fatal("connectBus accepted a wrong password")
	}
	if got := err.Error(); got != "COS local NATS connection failed; check node availability and authentication" {
		t.Fatalf("connectBus error = %q", got)
	}
	if elapsed < 2*time.Second || elapsed > 3*time.Second {
		t.Fatalf("connectBus gave up after %v, want about 2s of retries", elapsed)
	}
	if attempts := busAuthAttempts(t, srv) - before; attempts < 3 {
		t.Fatalf("server saw %d login attempts, want several", attempts)
	}
}

func TestConnectBusSucceedsAfterRouteReloadRejection(t *testing.T) {
	srv, opts, port := busAuthTestServer(t, "old")
	before := busAuthAttempts(t, srv)
	type result struct {
		conn *nats.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := connectBus(t.Context(), busCredential{Port: port, Password: "new"})
		done <- result{conn, err}
	}()
	// The second attempt starts only after the first was rejected, so reload now.
	busTestReady(t, "first rejected login", func() bool { return busAuthAttempts(t, srv) >= before+2 })
	next := opts.Clone()
	next.Password = "new"
	if err := srv.ReloadOptions(next); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatal(r.err)
		}
		r.conn.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("connectBus did not succeed after the reload")
	}
}

func TestConnectBusCancelStopsRetriesPromptly(t *testing.T) {
	srv, _, port := busAuthTestServer(t, "right")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	before := busAuthAttempts(t, srv)
	done := make(chan error, 1)
	go func() {
		_, err := connectBus(ctx, busCredential{Port: port, Password: "wrong"})
		done <- err
	}()
	busTestReady(t, "retry", func() bool { return busAuthAttempts(t, srv) >= before+2 })
	cancelled := time.Now()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("connectBus succeeded with a wrong password")
		}
		if elapsed := time.Since(cancelled); elapsed > 200*time.Millisecond {
			t.Fatalf("connectBus returned %v after cancel, want under 200ms", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connectBus kept retrying after cancel")
	}
}
