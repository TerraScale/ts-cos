package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nkeys"
)

func busTestPorts(t *testing.T, count int) []int {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()
	ports := make([]int, count)
	for i := range ports {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		ports[i] = listener.Addr().(*net.TCPAddr).Port
	}
	return ports
}

func busTestConfig(t *testing.T, host string, port, routePort int, peers []int) (Config, string) {
	t.Helper()
	cfg := Config{Home: t.TempDir(), LocalHost: host}
	var routes []string
	for _, peer := range peers {
		routes = append(routes, fmt.Sprintf("nats-route://cos-route:route-test-secret@127.0.0.1:%d", peer))
	}
	text := fmt.Sprintf(`server_name: %q
listen: "127.0.0.1:%d"
authorization { user: cos-client, password: client-test-secret }
jetstream {
  store_dir: %q
  max_memory_store: 32MB
  max_file_store: 32MB
}
cluster {
  name: COS
  listen: "127.0.0.1:%d"
  pool_size: -1
  authorization { user: cos-route, password: route-test-secret }
  routes: [%s]
}
`, host, port, filepath.Join(cfg.Home, "jetstream"), routePort, strings.Join(routes, ", "))
	busTestWriteConfig(t, cfg, text)
	return cfg, text
}

func busTestWriteConfig(t *testing.T, cfg Config, text string) {
	t.Helper()
	dir := filepath.Join(cfg.Home, ".config/cos")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nats.conf"), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
}

func busTestReady(t *testing.T, description string, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s", description)
		case <-ticker.C:
		}
	}
}

type busTestResult struct {
	Host, Command string
	Args          []string
}

type busTestPublicError struct{}

func (busTestPublicError) Error() string         { return "PRIVATE-T3-TOKEN client-test-secret" }
func (busTestPublicError) PublicMessage() string { return "no project titled 'missing'" }

func TestBusCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	nodes := make([]*Bus, 3)
	configs := make([]Config, 3)
	handlers := make([]OperationHandler, 3)
	accepted := make(chan context.Context, 1)
	finished := make(chan struct{}, 1)
	var mutated atomic.Bool
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		cfg, _ := busTestConfig(t, host, ports[i], ports[i+3], peers)
		configs[i] = cfg
		handlers[i] = func(ctx context.Context, command string, args []string) (any, error) {
			if len(args) > 0 {
				switch args[0] {
				case "cancel-handoff":
					accepted <- ctx
					<-ctx.Done()
					defer func() { finished <- struct{}{} }()
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					mutated.Store(true)
				case "pending-cancel":
					mutated.Store(true)
				case "fail":
					return nil, errors.New("client-test-secret route-test-secret PRIVATE-T3-TOKEN")
				case "public":
					return nil, busTestPublicError{}
				case "wrapped-public":
					return nil, fmt.Errorf("PRIVATE-T3-TOKEN: %w", busTestPublicError{})
				case "panic":
					panic("client-test-secret PRIVATE-T3-TOKEN")
				case "encode":
					return make(chan int), nil
				case "wait":
					<-ctx.Done()
					return nil, ctx.Err()
				}
			}
			return busTestResult{host, command, args}, nil
		}
		node, err := StartBus(ctx, cfg, handlers[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node
		t.Cleanup(node.Close)
	}
	busTestReady(t, "full mesh of authenticated routes", func() bool {
		return nodes[0].server.NumRoutes() == 2 && nodes[1].server.NumRoutes() == 2 && nodes[2].server.NumRoutes() == 2
	})

	clients := make([]*Bus, 3)
	for i, cfg := range configs {
		client, err := ConnectBus(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = client
		t.Cleanup(client.Close)
	}

	t.Run("anonymous denied", func(t *testing.T) {
		for _, port := range ports[:3] {
			conn, err := nats.Connect("nats://127.0.0.1:"+strconv.Itoa(port), nats.Timeout(time.Second), nats.NoReconnect())
			if conn != nil {
				conn.Close()
			}
			if !errors.Is(err, nats.ErrAuthorization) {
				t.Fatalf("anonymous connection returned %v", err)
			}
		}
	})

	t.Run("clients cross routes and all endpoints preserve arguments", func(t *testing.T) {
		for i, client := range clients {
			for j, host := range hosts {
				if i == j {
					continue
				}
				busTestReady(t, "remote micro endpoint", func() bool {
					var out busTestResult
					return client.Call(ctx, host, "projects", nil, &out) == nil && out.Host == host
				})
			}
		}
		args := []string{"thread-id", "--steer", "message with spaces", "--host", "literal-value"}
		for _, command := range busCommands {
			var out busTestResult
			if err := clients[0].Call(ctx, "beta", command, args, &out); err != nil {
				t.Fatal(err)
			}
			if out.Host != "beta" || out.Command != command || !slices.Equal(out.Args, args) {
				t.Fatalf("%s returned %#v", command, out)
			}
		}
	})

	t.Run("named micro discovery", func(t *testing.T) {
		inbox := nats.NewInbox()
		sub, err := clients[0].conn.SubscribeSync(inbox)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		subject, err := micro.ControlSubject(micro.InfoVerb, "cos", "")
		if err != nil {
			t.Fatal(err)
		}
		if err := clients[0].conn.PublishRequest(subject, inbox, nil); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for len(seen) < 3 {
			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var info micro.Info
			if err := json.Unmarshal(msg.Data, &info); err != nil {
				t.Fatal(err)
			}
			host := info.Metadata["host"]
			if info.Name != "cos" || !slices.Contains(hosts, host) || len(info.Endpoints) != len(busCommands)+1 {
				t.Fatalf("unexpected micro info: %#v", info)
			}
			for _, endpoint := range info.Endpoints {
				if endpoint.Subject != "cos."+host+"."+endpoint.Name {
					t.Fatalf("unexpected endpoint: %#v", endpoint)
				}
			}
			seen[host] = true
		}
		extra, err := micro.AddService(nodes[2].conn, micro.Config{Name: "cos", Version: "0.1.0", Metadata: map[string]string{"host": "extra-host"}})
		if err != nil {
			t.Fatal(err)
		}
		defer extra.Stop()
		// A new subscription must propagate across routes before discovery sees it.
		busTestReady(t, "additional micro service discovery", func() bool {
			got, err := clients[0].Hosts(ctx)
			return err == nil && slices.Equal(got, []string{"beta", "delta", "extra-host", "mariogk-dev"})
		})
	})

	t.Run("micro errors redact secrets", func(t *testing.T) {
		for _, arg := range []string{"fail", "panic", "encode"} {
			err := clients[0].Call(ctx, "beta", "send", []string{arg}, nil)
			if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "PRIVATE-T3-TOKEN") {
				t.Fatalf("unsafe or missing error: %v", err)
			}
		}
		for _, arg := range []string{"public", "wrapped-public"} {
			err := clients[0].Call(ctx, "beta", "launch", []string{arg}, nil)
			if err == nil || err.Error() != "no project titled 'missing'" {
				t.Fatalf("public error message lost: %v", err)
			}
		}
		msg, err := clients[0].conn.Request("cos.beta.send", []byte(`["fail"]`), time.Second)
		if err != nil || msg.Header.Get(micro.ErrorCodeHeader) != "500" || strings.Contains(msg.Header.Get(micro.ErrorHeader), "secret") {
			t.Fatalf("unsafe micro error response: %v, %v", msg, err)
		}
		msg, err = clients[0].conn.Request("cos.beta.send", []byte(`{"token":"PRIVATE-T3-TOKEN"}`), time.Second)
		if err != nil || msg.Header.Get(micro.ErrorCodeHeader) != "400" || strings.Contains(msg.Header.Get(micro.ErrorHeader), "PRIVATE-T3-TOKEN") {
			t.Fatalf("invalid request response: %v, %v", msg, err)
		}
	})

	t.Run("absent node and cancelled calls are bounded", func(t *testing.T) {
		start := time.Now()
		err := clients[0].Call(ctx, "offline-host", "projects", nil, nil)
		if !errors.Is(err, nats.ErrNoResponders) || time.Since(start) > time.Second {
			t.Fatalf("absent node: %v after %s", err, time.Since(start))
		}
		short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
		defer stop()
		if err := clients[0].Call(short, "beta", "watch", []string{"wait"}, nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cancelled call: %v", err)
		}
		if err := clients[0].Call(ctx, "beta", "*", nil, nil); err == nil {
			t.Fatal("wildcard command accepted")
		}
	})

	t.Run("accepted handoff cancellation stops later mutations", func(t *testing.T) {
		for _, closeClient := range []bool{false, true} {
			t.Run(fmt.Sprintf("client lifetime cancelled=%t", closeClient), func(t *testing.T) {
				callCtx, stop := context.WithCancel(context.Background())
				defer stop()
				if _, expires := callCtx.Deadline(); expires {
					t.Fatal("caller cancellation test must have no deadline")
				}
				client := clients[0]
				if closeClient {
					var err error
					client, err = ConnectBus(callCtx, configs[0])
					if err != nil {
						t.Fatal(err)
					}
					defer client.Close()
				}
				result := make(chan error, 1)
				go func() {
					result <- client.Call(callCtx, "beta", "handoff", []string{"cancel-handoff"}, nil)
				}()
				var handlerCtx context.Context
				select {
				case handlerCtx = <-accepted:
				case <-time.After(3 * time.Second):
					t.Fatal("handoff was not accepted")
				}
				stop()
				select {
				case err := <-result:
					if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "unconfirmed") {
						t.Fatalf("remote cancellation not acknowledged: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("caller cancellation was not bounded")
				}
				select {
				case <-handlerCtx.Done():
					if handlerCtx.Err() != context.Canceled {
						t.Fatalf("handler ended by expiry instead of cancellation: %v", handlerCtx.Err())
					}
				case <-time.After(time.Second):
					t.Fatal("accepted remote handler was not cancelled")
				}
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("remote handler did not exit")
				}
				if mutated.Load() {
					t.Fatal("subsequent mutation ran after cancellation")
				}
			})
		}
	})

	t.Run("cancel before acceptance is consumed and cleaned up", func(t *testing.T) {
		id := uuid()
		request := nats.NewMsg("cos.beta.cancel")
		request.Header.Set(busRequestHeader, id)
		ack, err := clients[0].conn.RequestMsg(request, time.Second)
		if err != nil || string(ack.Data) != "cancelled" {
			t.Fatalf("pending cancellation not acknowledged: %v, %v", ack, err)
		}
		request.Subject = "cos.beta.handoff"
		request.Data = []byte(`["pending-cancel"]`)
		response, err := clients[0].conn.RequestMsg(request, time.Second)
		if err != nil || response.Header.Get(micro.ErrorCodeHeader) != "499" || mutated.Load() {
			t.Fatalf("pre-cancelled operation ran: %v, %v", response, err)
		}
		busTestReady(t, "request context and pending cancellation cleanup", func() bool {
			nodes[1].mu.Lock()
			defer nodes[1].mu.Unlock()
			return nodes[1].active[id] == nil && nodes[1].pending[id] == nil
		})
		request.Subject = "cos.beta.cancel"
		request.Data = nil
		if _, err := clients[0].conn.RequestMsg(request, time.Second); err != nil {
			t.Fatal(err)
		}
		nodes[1].mu.Lock()
		timer := nodes[1].pending[id]
		if timer != nil {
			// Exercise the real expiry callback without waiting forty minutes.
			timer.Reset(time.Millisecond)
		}
		nodes[1].mu.Unlock()
		if timer == nil {
			t.Fatal("cancellation was not retained before acceptance")
		}
		busTestReady(t, "unused pending cancellation expiry", func() bool {
			nodes[1].mu.Lock()
			defer nodes[1].mu.Unlock()
			return nodes[1].pending[id] == nil
		})
	})

	t.Run("normal completion sends no remote cancellation", func(t *testing.T) {
		sub, err := clients[0].conn.SubscribeSync("cos.beta.cancel")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if err := clients[0].conn.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		callCtx, stop := context.WithCancel(ctx)
		defer stop()
		if err := clients[0].Call(callCtx, "beta", "projects", nil, nil); err != nil {
			t.Fatal(err)
		}
		stop()
		if _, err := sub.NextMsg(50 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("completed call sent a cancellation: %v", err)
		}
	})

	t.Run("request IDs reject malformed and repeated headers", func(t *testing.T) {
		for _, subject := range []string{"cos.beta.send", "cos.beta.cancel"} {
			for _, values := range [][]string{{""}, {"PRIVATE-T3-TOKEN"}, {uuid(), uuid()}} {
				request := nats.NewMsg(subject)
				request.Header[busRequestHeader] = values
				request.Data = []byte(`["pending-cancel"]`)
				response, err := clients[0].conn.RequestMsg(request, time.Second)
				if err != nil || response.Header.Get(micro.ErrorCodeHeader) != "400" || strings.Contains(response.Header.Get(micro.ErrorHeader), "PRIVATE-T3-TOKEN") || mutated.Load() {
					t.Fatalf("invalid request ID was accepted or leaked: %v, %v", response, err)
				}
			}
		}
	})

	t.Run("failed cancellation acknowledgement is explicit", func(t *testing.T) {
		client, err := ConnectBus(ctx, configs[0])
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		callCtx, stop := context.WithCancel(context.Background())
		defer stop()
		result := make(chan error, 1)
		go func() {
			result <- client.Call(callCtx, "beta", "handoff", []string{"cancel-handoff"}, nil)
		}()
		var handlerCtx context.Context
		select {
		case handlerCtx = <-accepted:
		case <-time.After(3 * time.Second):
			t.Fatal("handoff was not accepted")
		}
		// Remove the cancellation endpoint while the accepted handler is still waiting.
		if err := nodes[1].service.Stop(); err != nil {
			t.Fatal(err)
		}
		busTestReady(t, "cancellation endpoint removal across routes", func() bool {
			_, err := client.conn.Request("cos.beta.cancel", nil, 100*time.Millisecond)
			return errors.Is(err, nats.ErrNoResponders)
		})
		stop()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "remote cancellation unconfirmed") {
				t.Fatalf("unconfirmed cancellation was hidden: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("failed cancellation acknowledgement was not bounded")
		}
		// A lost acknowledgement cannot tell whether the remote context was cancelled.
		nodes[1].mu.Lock()
		for _, cancel := range nodes[1].active {
			cancel()
		}
		nodes[1].mu.Unlock()
		select {
		case <-handlerCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("test handler cleanup failed")
		}
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("test handler did not exit")
		}
	})

	t.Run("JetStream three replicas and fanout", func(t *testing.T) {
		js, err := clients[0].conn.JetStream(nats.MaxWait(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		busTestReady(t, "JetStream cluster leader", func() bool {
			_, err := js.AccountInfo()
			return err == nil
		})
		info, err := js.AddStream(&nats.StreamConfig{Name: "COS_PROBE", Subjects: []string{"cos.probe"}, Replicas: 3, Storage: nats.FileStorage, MaxBytes: 1 << 20, MaxAge: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		defer js.DeleteStream("COS_PROBE")
		if info.Config.Replicas != 3 || info.Cluster == nil || len(info.Cluster.Replicas) != 2 {
			t.Fatalf("unexpected replica placement: %#v", info.Cluster)
		}
		subs := make([]*nats.Subscription, 2)
		for i := range subs {
			subs[i], err = clients[i+1].conn.SubscribeSync("cos.probe")
			if err != nil {
				t.Fatal(err)
			}
			defer subs[i].Unsubscribe()
			if err := clients[i+1].conn.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
		}
		busTestReady(t, "cross-route fanout subscriptions", func() bool {
			if err := clients[0].conn.Publish("cos.probe", []byte("ready")); err != nil {
				return false
			}
			for _, sub := range subs {
				if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
					return false
				}
			}
			return true
		})
		ack, err := js.Publish("cos.probe", []byte("fanout"))
		if err != nil {
			t.Fatal(err)
		}
		stored, err := js.GetMsg("COS_PROBE", ack.Sequence)
		if err != nil || string(stored.Data) != "fanout" {
			t.Fatalf("stored message: %v, %v", stored, err)
		}
		for _, sub := range subs {
			for {
				msg, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if string(msg.Data) == "fanout" {
					break
				}
			}
		}
	})

	t.Run("client reconnects only to its restarted local node", func(t *testing.T) {
		nodes[1].Close()
		restarted, err := StartBus(ctx, configs[1], handlers[1])
		if err != nil {
			t.Fatal(err)
		}
		nodes[1] = restarted
		t.Cleanup(restarted.Close)
		busTestReady(t, "local client reconnect", func() bool {
			if !clients[1].conn.IsConnected() {
				return false
			}
			var out busTestResult
			return clients[1].Call(ctx, "beta", "projects", nil, &out) == nil && out.Host == "beta"
		})
		if clients[1].conn.ConnectedUrl() != "nats://127.0.0.1:"+strconv.Itoa(ports[1]) {
			t.Fatal("client reconnected to a remote node")
		}
		busTestReady(t, "restarted remote service", func() bool {
			return clients[0].Call(ctx, "beta", "projects", nil, nil) == nil
		})
	})

	t.Run("Close releases listeners and services", func(t *testing.T) {
		request := nats.NewMsg("cos.delta.cancel")
		request.Header.Set(busRequestHeader, uuid())
		if _, err := clients[0].conn.RequestMsg(request, time.Second); err != nil {
			t.Fatal(err)
		}
		for i, node := range nodes {
			clients[i].Close()
			clients[i].Close()
			node.Close()
			node.Close()
			if !clients[i].conn.IsClosed() || !node.conn.IsClosed() || !node.service.Stopped() || node.server.Running() {
				t.Fatal("Close left a client, service, or server running")
			}
			node.mu.Lock()
			pending := len(node.pending)
			node.mu.Unlock()
			if pending != 0 {
				t.Fatal("Close left pending cancellation timers")
			}
			for _, port := range []int{ports[i], ports[i+3]} {
				listener, err := net.Listen("tcp4", "127.0.0.1:"+strconv.Itoa(port))
				if err != nil {
					t.Fatalf("Close left port %d bound: %v", port, err)
				}
				listener.Close()
			}
		}
	})
}

func TestBusConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		edit func(string) string
		mode os.FileMode
	}{
		{"public config", nil, 0644},
		{"read-only config", nil, 0400},
		{"missing authentication", func(s string) string {
			return strings.ReplaceAll(s, "authorization { user: cos-client, password: client-test-secret }", "")
		}, 0600},
		{"empty password", func(s string) string { return strings.ReplaceAll(s, "password: client-test-secret", `password: ""`) }, 0600},
		{"anonymous fallback", func(s string) string { return s + "\nno_auth_user: cos-client\n" }, 0600},
		{"JetStream disabled", func(s string) string {
			start := strings.Index(s, "jetstream {")
			end := strings.Index(s[start:], "}")
			return s[:start] + "jetstream: false" + s[start+end+1:]
		}, 0600},
		{"production cluster", func(s string) string { return strings.ReplaceAll(s, "name: COS", "name: production") }, 0600},
		{"production COS cluster", func(s string) string { return strings.ReplaceAll(s, "name: COS", "name: COS-production") }, 0600},
		{"missing route auth", func(s string) string {
			return strings.ReplaceAll(s, "authorization { user: cos-route, password: route-test-secret }", "")
		}, 0600},
		{"public route", func(s string) string {
			return strings.ReplaceAll(s, "routes: []", "routes: [nats-route://cos-route:route-test-secret@production.example.com:6222]")
		}, 0600},
		{"invalid syntax containing secret", func(s string) string { return s + "\nunknown-secret-setting: PRIVATE-T3-TOKEN\n" }, 0600},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg, text := busTestConfig(t, "test-node", 4223, 6223, nil)
			if test.edit != nil {
				busTestWriteConfig(t, cfg, test.edit(text))
			}
			if err := os.Chmod(filepath.Join(cfg.Home, ".config/cos/nats.conf"), test.mode); err != nil {
				t.Fatal(err)
			}
			for _, start := range []bool{false, true} {
				var bus *Bus
				var err error
				if start {
					bus, err = StartBus(context.Background(), cfg, func(context.Context, string, []string) (any, error) { return nil, nil })
				} else {
					bus, err = ConnectBus(context.Background(), cfg)
				}
				if bus != nil {
					bus.Close()
				}
				if err == nil || strings.Contains(err.Error(), "test-secret") || strings.Contains(err.Error(), "PRIVATE-T3-TOKEN") {
					t.Fatalf("unsafe or missing configuration error: %v", err)
				}
			}
		})
	}
	t.Run("symlink is not a private regular config", func(t *testing.T) {
		cfg := Config{Home: t.TempDir(), LocalHost: "test-node"}
		dir := filepath.Join(cfg.Home, ".config/cos")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "target"), []byte("PRIVATE-T3-TOKEN"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("target", filepath.Join(dir, "nats.conf")); err != nil {
			t.Fatal(err)
		}
		if _, err := ConnectBus(context.Background(), cfg); err == nil {
			t.Fatal("symlink accepted")
		}
	})
}

func TestBusNkeyAndContextCleanup(t *testing.T) {
	ports := busTestPorts(t, 3)
	cfg, text := busTestConfig(t, "nkey-node", ports[0], ports[1], ports[2:])
	key, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer key.Wipe()
	public, err := key.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := key.Seed()
	if err != nil {
		t.Fatal(err)
	}
	seedPath := filepath.Join(cfg.Home, ".config/cos/nats.seed")
	if err := os.WriteFile(seedPath, seed, 0600); err != nil {
		t.Fatal(err)
	}
	text = strings.ReplaceAll(text, "authorization { user: cos-client, password: client-test-secret }", fmt.Sprintf("authorization { users: [{nkey: %q}] }", public))
	busTestWriteConfig(t, cfg, text)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := StartBus(ctx, cfg, func(ctx context.Context, command string, args []string) (any, error) {
		if command == "handoff" || command == "send" {
			deadline, _ := ctx.Deadline()
			return deadline, nil
		}
		return "nkey-authenticated", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	client, err := ConnectBus(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var out string
	if err := client.Call(ctx, "nkey-node", "projects", nil, &out); err != nil || out != "nkey-authenticated" {
		t.Fatalf("nkey call: %q, %v", out, err)
	}
	for _, test := range []struct {
		command string
		limit   time.Duration
	}{{"send", time.Minute}, {"handoff", 40 * time.Minute}} {
		var deadline time.Time
		if err := client.Call(ctx, "nkey-node", test.command, nil, &deadline); err != nil {
			t.Fatal(err)
		}
		remaining := time.Until(deadline)
		if remaining > test.limit || remaining < test.limit-time.Second {
			t.Fatalf("%s deadline = %s, want approximately %s", test.command, remaining, test.limit)
		}
	}
	short, stopShort := context.WithTimeout(ctx, 500*time.Millisecond)
	defer stopShort()
	var deadline time.Time
	if err := client.Call(short, "nkey-node", "handoff", nil, &deadline); err != nil {
		t.Fatal(err)
	}
	callerDeadline, _ := short.Deadline()
	if !deadline.Equal(callerDeadline) {
		t.Fatalf("caller deadline lost: %s != %s", deadline, callerDeadline)
	}
	hosts, err := client.Hosts(ctx)
	if err != nil || !slices.Equal(hosts, []string{"beta", "delta", "mariogk-dev", "nkey-node"}) {
		t.Fatalf("offline expected nodes lost: %v, %v", hosts, err)
	}
	if err := os.Chmod(seedPath, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ConnectBus(ctx, cfg); err == nil || strings.Contains(err.Error(), string(seed)) {
		t.Fatalf("unsafe seed permissions accepted or leaked: %v", err)
	}
	cancel()
	for _, bus := range []*Bus{client, node} {
		select {
		case <-bus.done:
		case <-time.After(3 * time.Second):
			t.Fatal("context cancellation did not close bus")
		}
	}
	if node.server.Running() || !client.conn.IsClosed() {
		t.Fatal("context cancellation left bus running")
	}
}

func TestBusExampleConfig(t *testing.T) {
	text, err := os.ReadFile("../../examples/nats.conf")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Home: t.TempDir(), LocalHost: "delta"}
	busTestWriteConfig(t, cfg, string(text))
	opts, _, _, err := loadBusConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.StoreDir != filepath.Join(cfg.Home, ".local/share/cos/nats") || len(opts.Routes) != 2 {
		t.Fatal("example lost durable storage or COS peer routes")
	}
}
