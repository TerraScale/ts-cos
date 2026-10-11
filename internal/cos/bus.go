package cos

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

type OperationHandler func(context.Context, string, []string) (any, error)

var busCommands = []string{
	"projects", "launch", "thread", "send", "handoff", "asks-host", "ctx", "shell",
	"archive", "fork", "watch", "model", "settle", "rename", "tidy", "close",
	"delete", "dispatch", "respond", "asks", "compact", "usage-verify", "inventory", "purge", "purge-children", "stop", "status-host",
}

const (
	busDeadlineHeader = "Cos-Deadline"
	busRequestHeader  = "Cos-Request-ID"
	busCancelTimeout  = 2 * time.Second
	busPendingLimit   = 1024
)

func validBusRequestID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	return err == nil && len(decoded) == 16
}

func busOperationTimeout(command string) time.Duration {
	if command == "handoff" || command == "compact" {
		return 40 * time.Minute
	}
	return time.Minute
}

// Bus is a local client, optionally owning the embedded COS server and service.
type Bus struct {
	conn    *nats.Conn
	server  *server.Server
	service micro.Service
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	host    string

	mu      sync.Mutex
	closing bool
	calls   sync.WaitGroup
	tasks   sync.WaitGroup // discovery and election use the connection until Close
	active  map[string]context.CancelFunc
	pending map[string]*time.Timer

	leading     context.Context // non-nil while this node holds the cluster lease
	leadChanged chan struct{}   // closed whenever leading changes
	resign      chan struct{}   // closed by Resign
	resignOnce  sync.Once
	elected     chan struct{} // closed when elect returns
	hookSecrets map[string]string
	reloads     atomic.Int64 // route reloads applied; tests read it
}

// Fixed COS node settings; the ports stay clear of dev dashboards near 8790 and NATS defaults.
const (
	busClusterName = "COS"
	busClientPort  = 7790
	busRoutePort   = 7791
	busClientUser  = "cos"
	busRouteUser   = "cos-route"
	busDiscovery   = 15 * time.Second
)

// busNode is what a node needs beyond Config; StartBus fills it from OCI Vault and Tailscale.
type busNode struct {
	master     []byte
	clientPort int
	routeHost  string
	routePort  int
	peers      func(context.Context) ([]string, error)
}

type busCredential struct {
	Port     int    `json:"port"`
	Password string `json:"password"`
}

func busRoute(password, address string) *url.URL {
	return &url.URL{Scheme: "nats-route", User: url.UserPassword(busRouteUser, password), Host: address}
}

func busOptions(cfg Config, node busNode) *server.Options {
	routePassword := derive(node.master, "nats route")
	return &server.Options{
		ServerName: cfg.LocalHost, Host: "127.0.0.1", Port: node.clientPort,
		Username: busClientUser, Password: derive(node.master, "nats client"),
		JetStream: true, StoreDir: filepath.Join(cfg.Home, ".local/share/cos/nats"),
		JetStreamMaxMemory: 256 << 20, JetStreamMaxStore: 1 << 30,
		Cluster: server.ClusterOpts{
			Name: busClusterName, Host: node.routeHost, Port: node.routePort,
			Username: busRouteUser, Password: routePassword, PoolSize: -1, // one route connection per peer
		},
		// Clustered JetStream needs a configured route before peers are found; NATS ignores a route to itself.
		Routes: []*url.URL{busRoute(routePassword, net.JoinHostPort(node.routeHost, strconv.Itoa(node.routePort)))},
		NoLog:  true, NoSigs: true,
	}
}

// natsLog routes embedded NATS logs to slog; NoLog only stops the server's own logger.
type natsLog struct{ secrets []string }

func (l natsLog) line(format string, v ...any) string {
	msg := fmt.Sprintf(format, v...)
	for _, secret := range l.secrets {
		if secret != "" {
			msg = strings.ReplaceAll(msg, secret, "[REDACTED]")
		}
	}
	return "nats: " + msg
}

func (l natsLog) Noticef(format string, v ...any) { slog.Info(l.line(format, v...)) }
func (l natsLog) Warnf(format string, v ...any)   { slog.Warn(l.line(format, v...)) }

// Route and connection errors are routine while a peer restarts; ERROR lines fail the e2e deploy.logs check.
func (l natsLog) Errorf(format string, v ...any) { slog.Warn(l.line(format, v...)) }
func (l natsLog) Fatalf(format string, v ...any) { slog.Error(l.line(format, v...)) }
func (l natsLog) Debugf(string, ...any)          {}
func (l natsLog) Tracef(string, ...any)          {}

func busCredentialPath(cfg Config) string {
	return filepath.Join(cfg.Runtime, "cos", "nats.json")
}

// writeBusCredential hands the local client password to CLI processes of the same user.
func writeBusCredential(cfg Config, credential busCredential) error {
	path := busCredentialPath(cfg)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return errors.New("COS runtime directory unavailable")
	}
	data, _ := json.Marshal(credential)
	f, err := os.CreateTemp(filepath.Dir(path), ".nats-*")
	if err != nil {
		return errors.New("COS runtime credential could not be written")
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		return errors.New("COS runtime credential could not be written")
	}
	return nil
}

func readBusCredential(cfg Config) (busCredential, error) {
	var credential busCredential
	path := busCredentialPath(cfg)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return credential, errors.New("COS service is not running on this host; start it with systemctl --user start cos")
	}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &credential) != nil || credential.Port < 1 || credential.Password == "" {
		return credential, errors.New("COS runtime credential is unreadable")
	}
	return credential, nil
}

type tailnetPeer struct {
	UserID       int64
	OS           string
	Online       bool
	TailscaleIPs []string
}

// tailnet returns this host's Tailscale IPv4 and the IPv4 of every online Linux
// device of the same owner: tailnet membership is COS admission (zero config).
func tailnet(ctx context.Context) (string, []string, error) {
	client := http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", "/run/tailscale/tailscaled.sock")
		},
	}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://local-tailscaled.sock/localapi/v0/status", nil)
	resp, err := client.Do(req)
	if err != nil {
		return "", nil, errors.New("Tailscale is unavailable; COS discovers peers on the tailnet")
	}
	defer resp.Body.Close()
	var status struct {
		Self tailnetPeer
		Peer map[string]tailnetPeer
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&status) != nil {
		return "", nil, errors.New("Tailscale status is unreadable")
	}
	self, peers := tailnetIPv4(status.Self), []string{}
	for _, peer := range status.Peer {
		if ip := tailnetIPv4(peer); ip != "" && peer.Online && peer.OS == "linux" && peer.UserID == status.Self.UserID {
			peers = append(peers, ip)
		}
	}
	if self == "" {
		return "", nil, errors.New("Tailscale has no IPv4 address for this host")
	}
	slices.Sort(peers)
	return self, peers, nil
}

func tailnetIPv4(peer tailnetPeer) string {
	for _, raw := range peer.TailscaleIPs {
		if ip, err := netip.ParseAddr(raw); err == nil && ip.Is4() {
			return ip.String()
		}
	}
	return ""
}

// cosRoute reports the server name of address when it is a COS route listener.
// NATS sends INFO before credentials, so route passwords only ever reach COS nodes.
func cosRoute(ctx context.Context, address string) (string, bool) {
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return "", false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	line, err := bufio.NewReader(io.LimitReader(conn, 64<<10)).ReadString('\n')
	info, ok := strings.CutPrefix(line, "INFO ")
	var fields struct {
		Cluster    string `json:"cluster"`
		ServerName string `json:"server_name"`
	}
	if err != nil || !ok || json.Unmarshal([]byte(info), &fields) != nil || fields.Cluster != busClusterName {
		return "", false
	}
	return fields.ServerName, true
}

// routeConnected reports whether name is this node or already routed through gossip.
func (b *Bus) routeConnected(name string) bool {
	if name == b.server.Name() {
		return true
	}
	routez, err := b.server.Routez(&server.RoutezOptions{})
	if err != nil {
		return false
	}
	for _, route := range routez.Routes {
		if route.RemoteName == name {
			return true
		}
	}
	return false
}

// discover adds a route to every COS node found; NATS gossip completes the mesh
// and retries configured routes while a peer restarts. A reload briefly rejects
// client auth, so peers already connected through gossip are not reloaded for.
func (b *Bus) discover(ctx context.Context, opts *server.Options, peers func(context.Context) ([]string, error)) {
	routed := map[string]bool{}
	for {
		candidates, _ := peers(ctx)
		added := false
		for _, peer := range candidates {
			if routed[peer] {
				continue
			}
			name, ok := cosRoute(ctx, peer)
			if !ok {
				continue
			}
			routed[peer] = true
			added = added || !b.routeConnected(name)
		}
		if added {
			next := opts.Clone()
			next.Routes = next.Routes[:1]
			for _, peer := range slices.Sorted(maps.Keys(routed)) {
				next.Routes = append(next.Routes, busRoute(opts.Cluster.Password, peer))
			}
			if err := b.server.ReloadOptions(next); err != nil {
				slog.Error("COS route update failed", "error", err)
				clear(routed)
			} else {
				b.reloads.Add(1)
				opts = next
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(busDiscovery):
		}
	}
}

func connectBus(ctx context.Context, credential busCredential) (*nats.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	timeout := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	address := "nats://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(credential.Port))
	var conn *nats.Conn
	var err error
	// A route reload in discover briefly rejects valid credentials, so retry those logins.
	for started := time.Now(); ; {
		conn, err = nats.Connect(address, nats.UserInfo(busClientUser, credential.Password), nats.Name("cos"), nats.Timeout(timeout),
			nats.MaxReconnects(-1), nats.ReconnectWait(500*time.Millisecond),
			nats.PingInterval(15*time.Second), nats.MaxPingsOutstanding(2),
			nats.ReconnectBufSize(-1), nats.IgnoreDiscoveredServers(),
			nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
		if !errors.Is(err, nats.ErrAuthorization) || time.Since(started) >= 2*time.Second {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
		if ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return nil, errors.New("COS local NATS connection failed; check node availability and authentication")
	}
	if err := ctx.Err(); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (b *Bus) watch(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			b.Close()
		case <-b.done:
		}
	}()
}

// ConnectBus connects only to the local node, with the credential its service wrote.
func ConnectBus(ctx context.Context, cfg Config) (*Bus, error) {
	credential, err := readBusCredential(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &Bus{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	b.conn, err = connectBus(ctx, credential)
	if err != nil {
		b.Close()
		return nil, err
	}
	b.watch(ctx)
	return b, nil
}

// StartBus embeds the COS JetStream node, joins every COS node on the tailnet
// with credentials derived from the OCI Vault master secret, and registers
// its host-specific micro endpoints.
func StartBus(ctx context.Context, cfg Config, handler OperationHandler) (*Bus, error) {
	api, err := newOCIVault()
	if err != nil {
		return nil, err
	}
	master, err := vaultMaster(ctx, api)
	if err != nil {
		return nil, err
	}
	defer clear(master)
	self, _, err := tailnet(ctx)
	if err != nil {
		return nil, err
	}
	return startBus(ctx, cfg, busNode{master: master, clientPort: busClientPort, routeHost: self, routePort: busRoutePort,
		peers: func(ctx context.Context) ([]string, error) {
			_, peers, err := tailnet(ctx)
			for i, peer := range peers {
				peers[i] = net.JoinHostPort(peer, strconv.Itoa(busRoutePort))
			}
			return peers, err
		}}, handler)
}

func startBus(ctx context.Context, cfg Config, node busNode, handler OperationHandler) (*Bus, error) {
	if !validHost(cfg.LocalHost) || handler == nil {
		return nil, errors.New("COS bus requires a valid local host and operation handler")
	}
	opts := busOptions(cfg, node)
	credential := busCredential{Port: node.clientPort, Password: opts.Password}
	hookSecrets := map[string]string{}
	for source := range hookSources {
		hookSecrets[source] = derive(node.master, "hook "+source)
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &Bus{ctx: ctx, cancel: cancel, done: make(chan struct{}), host: cfg.LocalHost,
		active: make(map[string]context.CancelFunc), pending: make(map[string]*time.Timer),
		resign: make(chan struct{}), elected: make(chan struct{}), hookSecrets: hookSecrets}
	var err error
	b.server, err = server.NewServer(opts)
	if err != nil {
		b.Close()
		return nil, errors.New("COS embedded NATS configuration rejected")
	}
	b.server.SetLoggerV2(natsLog{secrets: []string{opts.Password, opts.Cluster.Password}}, false, false, false)
	go b.server.Start()
	ready, stopReady := context.WithTimeout(ctx, 30*time.Second)
	defer stopReady()
	for !b.server.ReadyForConnections(50 * time.Millisecond) {
		if err := ready.Err(); err != nil {
			b.Close()
			return nil, fmt.Errorf("COS embedded NATS did not become ready: %w", err)
		}
	}
	b.conn, err = connectBus(ctx, credential)
	if err != nil {
		b.Close()
		return nil, err
	}
	b.service, err = micro.AddService(b.conn, micro.Config{
		Name: "cos", Version: "0.1.0", Metadata: map[string]string{"host": cfg.LocalHost, "version": Version},
		Description: "COS host operations", QueueGroup: "cos-" + cfg.LocalHost,
	})
	if err == nil {
		err = b.service.AddEndpoint("cancel", micro.HandlerFunc(b.handleCancel),
			micro.WithEndpointSubject("cos."+cfg.LocalHost+".cancel"))
	}
	if err == nil {
		for _, command := range busCommands {
			err = b.service.AddEndpoint(command, micro.HandlerFunc(func(req micro.Request) {
				b.handle(ctx, handler, command, req)
			}), micro.WithEndpointSubject("cos."+cfg.LocalHost+"."+command))
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		err = b.conn.FlushWithContext(ready)
	}
	if err != nil {
		b.Close()
		return nil, errors.New("COS micro service registration failed")
	}
	if err := writeBusCredential(cfg, credential); err != nil {
		b.Close()
		return nil, err
	}
	next := opts.Clone()
	b.tasks.Go(func() { b.discover(ctx, next, node.peers) })
	b.tasks.Go(func() { b.elect(ctx) })
	b.watch(ctx)
	return b, nil
}

func (b *Bus) handle(ctx context.Context, handler OperationHandler, command string, req micro.Request) {
	defer func() {
		if recover() != nil {
			_ = req.Error("500", "COS operation failed", nil)
		}
	}()
	id := req.Headers().Get(busRequestHeader)
	values := req.Headers().Values(busRequestHeader)
	if len(values) > 1 || (len(values) == 1 && !validBusRequestID(id)) {
		_ = req.Error("400", "invalid COS request ID", nil)
		return
	}
	var args []string
	if err := json.Unmarshal(req.Data(), &args); err != nil {
		_ = req.Error("400", "COS arguments must be a JSON array of strings", nil)
		return
	}
	deadline := time.Now().Add(busOperationTimeout(command))
	if value := req.Headers().Get(busDeadlineHeader); value != "" {
		ns, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			_ = req.Error("400", "invalid COS request deadline", nil)
			return
		}
		if requested := time.Unix(0, ns); requested.Before(deadline) {
			deadline = requested
		}
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if id != "" {
		b.mu.Lock()
		if _, exists := b.active[id]; exists {
			b.mu.Unlock()
			_ = req.Error("409", "duplicate COS request ID", nil)
			return
		}
		b.active[id] = cancel
		if timer := b.pending[id]; timer != nil {
			timer.Stop()
			delete(b.pending, id)
			cancel()
		}
		b.mu.Unlock()
		defer func() {
			b.mu.Lock()
			delete(b.active, id)
			b.mu.Unlock()
		}()
	}
	if ctx.Err() != nil {
		_ = req.Error("499", "COS operation cancelled", nil)
		return
	}
	result, err := handler(ctx, command, args)
	if err != nil {
		// Upstream errors may contain T3 credentials or private command arguments.
		message := "COS operation failed"
		var public interface{ PublicMessage() string }
		if errors.As(err, &public) {
			if safe := public.PublicMessage(); safe != "" {
				message = safe
			}
		}
		_ = req.Error("500", message, nil)
		return
	}
	if err := req.RespondJSON(result); err != nil {
		_ = req.Error("500", "COS response could not be encoded or sent", nil)
	}
}

func (b *Bus) handleCancel(req micro.Request) {
	id := req.Headers().Get(busRequestHeader)
	if len(req.Headers().Values(busRequestHeader)) != 1 || !validBusRequestID(id) {
		_ = req.Error("400", "invalid COS request ID", nil)
		return
	}
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		_ = req.Error("503", "COS service stopping", nil)
		return
	}
	if cancel := b.active[id]; cancel != nil {
		cancel()
	} else if b.pending[id] == nil {
		if len(b.pending) >= busPendingLimit {
			b.mu.Unlock()
			_ = req.Error("503", "COS cancellation tracking unavailable", nil)
			return
		}
		// Remember cancellations racing acceptance, no longer than any operation can run.
		var timer *time.Timer
		timer = time.AfterFunc(busOperationTimeout("handoff"), func() {
			b.mu.Lock()
			if b.pending[id] == timer {
				delete(b.pending, id)
			}
			b.mu.Unlock()
		})
		b.pending[id] = timer
	}
	b.mu.Unlock()
	// This acknowledges context cancellation or its recording, not rollback or completion.
	_ = req.Respond([]byte("cancelled"))
}

// Close cancels handlers, stops the service, and shuts down an owned server.
func (b *Bus) Close() {
	if b == nil {
		return
	}
	b.once.Do(func() {
		b.mu.Lock()
		b.closing = true
		b.mu.Unlock()
		b.cancel()
		// Keep NATS available until callers have attempted cancellation acknowledgement.
		b.calls.Wait()
		b.tasks.Wait()
		if b.service != nil {
			_ = b.service.Stop()
		}
		if b.conn != nil {
			b.conn.Close()
		}
		if b.server != nil {
			b.server.Shutdown()
			b.server.WaitForShutdown()
		}
		b.mu.Lock()
		for id, timer := range b.pending {
			timer.Stop()
			delete(b.pending, id)
		}
		b.mu.Unlock()
		close(b.done)
	})
}

// Call caps operations at one minute, or forty minutes for handoff, respecting earlier caller deadlines.
func (b *Bus) Call(ctx context.Context, host, command string, args []string, output any) error {
	if !validHost(host) || !slices.Contains(busCommands, command) {
		return errors.New("invalid COS host or command")
	}
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("COS bus closed")
	}
	b.calls.Add(1)
	b.mu.Unlock()
	defer b.calls.Done()
	ctx, cancel := context.WithTimeout(ctx, busOperationTimeout(command))
	defer cancel()
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(args)
	if err != nil {
		return errors.New("COS arguments could not be encoded")
	}
	msg := nats.NewMsg("cos." + host + "." + command)
	msg.Data = data
	id := uuid()
	msg.Header.Set(busRequestHeader, id)
	deadline, _ := ctx.Deadline()
	msg.Header.Set(busDeadlineHeader, strconv.FormatInt(deadline.UnixNano(), 10))
	response, err := b.conn.RequestMsgWithContext(ctx, msg)
	if err != nil {
		if ctx.Err() != nil {
			ackCtx, stopAck := context.WithTimeout(context.Background(), busCancelTimeout)
			defer stopAck()
			request := nats.NewMsg("cos." + host + ".cancel")
			request.Header.Set(busRequestHeader, id)
			ack, ackErr := b.conn.RequestMsgWithContext(ackCtx, request)
			if ackErr != nil || ack.Header.Get(micro.ErrorCodeHeader) != "" || ack.Header.Get(micro.ErrorHeader) != "" || string(ack.Data) != "cancelled" {
				return fmt.Errorf("remote cancellation unconfirmed: %w", ctx.Err())
			}
			return ctx.Err()
		}
		if errors.Is(err, nats.ErrNoResponders) {
			return fmt.Errorf("COS node unavailable: %w", nats.ErrNoResponders)
		}
		return errors.New("COS request failed; local node unavailable")
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	if response.Header.Get(micro.ErrorHeader) != "" || response.Header.Get(micro.ErrorCodeHeader) != "" {
		if message := response.Header.Get(micro.ErrorHeader); message != "" {
			return errors.New(message)
		}
		return errors.New("COS remote operation failed")
	}
	if output == nil {
		return nil
	}
	if err := json.Unmarshal(response.Data, output); err != nil {
		return errors.New("COS response could not be decoded")
	}
	return nil
}

// Hosts includes expected COS nodes even while offline, plus nodes discovered by micro PING.
func (b *Bus) Hosts(ctx context.Context) ([]string, error) {
	hosts := map[string]bool{"delta": true, "beta": true, "mariogk-dev": true}
	discovery, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	inbox := nats.NewInbox()
	sub, err := b.conn.SubscribeSync(inbox)
	if err != nil {
		return nil, errors.New("COS discovery subscription failed")
	}
	defer sub.Unsubscribe()
	subject, _ := micro.ControlSubject(micro.PingVerb, "cos", "")
	if err := b.conn.PublishRequest(subject, inbox, nil); err != nil {
		return nil, errors.New("COS discovery request failed")
	}
	if err := b.conn.FlushWithContext(discovery); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("COS discovery could not reach the local node")
	}
	for {
		msg, err := sub.NextMsgWithContext(discovery)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return nil, errors.New("COS discovery interrupted")
		}
		var ping micro.Ping
		if json.Unmarshal(msg.Data, &ping) == nil && ping.Name == "cos" && validHost(ping.Metadata["host"]) {
			hosts[ping.Metadata["host"]] = true
		}
	}
	result := make([]string, 0, len(hosts))
	for host := range hosts {
		result = append(result, host)
	}
	slices.Sort(result)
	return result, nil
}
