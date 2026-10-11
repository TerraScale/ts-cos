package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
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

// busTestJetStream starts three peered COS buses and returns each node's JetStream API.
func busTestJetStream(t *testing.T, ctx context.Context) []jetstream.JetStream {
	t.Helper()
	ports := busTestPorts(t, 6)
	hosts := []string{"delta", "beta", "mariogk-dev"}
	js := make([]jetstream.JetStream, len(hosts))
	for i, host := range hosts {
		var peers []int
		for j := range hosts {
			if i != j {
				peers = append(peers, ports[j+3])
			}
		}
		cfg, busNode := busTestConfig(t, host, ports[i], ports[i+3], peers)
		node, err := startBus(ctx, cfg, busNode, func(context.Context, string, []string) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(node.Close)
		if js[i], err = node.JetStream(); err != nil {
			t.Fatal(err)
		}
	}
	return js
}

var busTestMaster = []byte("0123456789abcdef0123456789abcdef")

func busTestConfig(t *testing.T, host string, port, routePort int, peers []int) (Config, busNode) {
	t.Helper()
	cfg := Config{Home: t.TempDir(), Runtime: t.TempDir(), LocalHost: host}
	var addresses []string
	for _, peer := range peers {
		addresses = append(addresses, "127.0.0.1:"+strconv.Itoa(peer))
	}
	return cfg, busNode{master: busTestMaster, clientPort: port, routeHost: "127.0.0.1", routePort: routePort,
		peers: func(context.Context) ([]string, error) { return addresses, nil }}
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
	busNodes := make([]busNode, 3)
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
		cfg, node := busTestConfig(t, host, ports[i], ports[i+3], peers)
		configs[i], busNodes[i] = cfg, node
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
		bus, err := startBus(ctx, cfg, busNodes[i], handlers[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = bus
		t.Cleanup(bus.Close)
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
		restarted, err := startBus(ctx, configs[1], busNodes[1], handlers[1])
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

func TestBusAdmission(t *testing.T) {
	t.Run("probe sends nothing and accepts only COS route listeners", func(t *testing.T) {
		for banner, want := range map[string]bool{
			"INFO {\"cluster\":\"COS\",\"name\":\"beta\"}\r\n": true,
			"INFO {\"cluster\":\"production\"}\r\n":            false,
			"-ERR 'not nats'\r\n":                              false,
			"":                                                 false,
		} {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			received := make(chan int, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					received <- -1
					return
				}
				_, _ = conn.Write([]byte(banner))
				_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				n, _ := io.Copy(io.Discard, conn)
				conn.Close()
				received <- int(n)
			}()
			if banner == "" {
				go func() { time.Sleep(100 * time.Millisecond); listener.Close() }()
			}
			if got := cosRoute(context.Background(), listener.Addr().String()); got != want {
				t.Fatalf("banner %q accepted = %v", banner, got)
			}
			if n := <-received; n > 0 {
				t.Fatalf("probe sent %d bytes to %q", n, banner)
			}
			listener.Close()
		}
	})

	t.Run("nodes with another master secret never join", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ports := busTestPorts(t, 4)
		handler := func(context.Context, string, []string) (any, error) { return nil, nil }
		cfg, node := busTestConfig(t, "delta", ports[0], ports[1], nil)
		first, err := startBus(ctx, cfg, node, handler)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Close()
		cfg, node = busTestConfig(t, "beta", ports[2], ports[3], []int{ports[1]})
		node.master = []byte("another-master-secret-0123456789")
		second, err := startBus(ctx, cfg, node, handler)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close()
		time.Sleep(time.Second)
		if first.server.NumRoutes() != 0 || second.server.NumRoutes() != 0 {
			t.Fatal("a node with another master secret joined the cluster")
		}
	})

	t.Run("CLI requires the service credential", func(t *testing.T) {
		cfg := Config{Home: t.TempDir(), Runtime: t.TempDir(), LocalHost: "delta"}
		if _, err := ConnectBus(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "not running") {
			t.Fatalf("missing credential: %v", err)
		}
		if err := writeBusCredential(cfg, busCredential{Port: 1, Password: "PRIVATE-T3-TOKEN"}); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(busCredentialPath(cfg), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := ConnectBus(context.Background(), cfg); err == nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("public credential accepted or leaked: %v", err)
		}
	})
}

func TestTailnetPeers(t *testing.T) {
	peer := tailnetPeer{TailscaleIPs: []string{"fd7a:115c:a1e0::1", "100.64.0.9"}}
	if tailnetIPv4(peer) != "100.64.0.9" || tailnetIPv4(tailnetPeer{TailscaleIPs: []string{"fd7a::1"}}) != "" {
		t.Fatal("tailnet IPv4 selection")
	}
}

func TestBusContextCleanup(t *testing.T) {
	ports := busTestPorts(t, 3)
	cfg, node := busTestConfig(t, "solo-node", ports[0], ports[1], ports[2:])
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus, err := startBus(ctx, cfg, node, func(ctx context.Context, command string, args []string) (any, error) {
		if command == "handoff" || command == "send" {
			deadline, _ := ctx.Deadline()
			return deadline, nil
		}
		return "authenticated", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()
	client, err := ConnectBus(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var out string
	if err := client.Call(ctx, "solo-node", "projects", nil, &out); err != nil || out != "authenticated" {
		t.Fatalf("call: %q, %v", out, err)
	}
	for _, test := range []struct {
		command string
		limit   time.Duration
	}{{"send", time.Minute}, {"handoff", 40 * time.Minute}} {
		var deadline time.Time
		if err := client.Call(ctx, "solo-node", test.command, nil, &deadline); err != nil {
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
	if err := client.Call(short, "solo-node", "handoff", nil, &deadline); err != nil {
		t.Fatal(err)
	}
	callerDeadline, _ := short.Deadline()
	if !deadline.Equal(callerDeadline) {
		t.Fatalf("caller deadline lost: %s != %s", deadline, callerDeadline)
	}
	hosts, err := client.Hosts(ctx)
	if err != nil || !slices.Equal(hosts, []string{"beta", "delta", "mariogk-dev", "solo-node"}) {
		t.Fatalf("offline expected nodes lost: %v, %v", hosts, err)
	}
	cancel()
	for _, b := range []*Bus{client, bus} {
		select {
		case <-b.done:
		case <-time.After(5 * time.Second):
			t.Fatal("context cancellation did not close bus")
		}
	}
	if bus.server.Running() || !client.conn.IsClosed() {
		t.Fatal("context cancellation left bus running")
	}
}
