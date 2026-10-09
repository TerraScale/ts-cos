package cos

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nkeys"
)

type OperationHandler func(context.Context, string, []string) (any, error)

var busCommands = []string{
	"projects", "launch", "thread", "send", "handoff", "asks-host", "ctx", "shell",
	"archive", "fork", "watch", "model", "settle", "rename", "tidy", "close",
	"delete", "dispatch", "respond", "asks",
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
	if command == "handoff" {
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
	key     nkeys.KeyPair

	mu      sync.Mutex
	closing bool
	calls   sync.WaitGroup
	active  map[string]context.CancelFunc
	pending map[string]*time.Timer
}

func privateBusFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("COS %s is missing or unreadable", filepath.Base(path))
	}
	if !info.Mode().IsRegular() || info.Mode() != 0600 {
		return fmt.Errorf("COS %s must be a regular file with mode 0600", filepath.Base(path))
	}
	return nil
}

func loadBusConfig(cfg Config) (*server.Options, nats.Option, nkeys.KeyPair, error) {
	path := filepath.Join(cfg.Home, ".config/cos/nats.conf")
	if err := privateBusFile(path); err != nil {
		return nil, nil, nil, err
	}
	opts, err := server.ProcessConfigFile(path)
	if err != nil {
		// NATS parser errors can quote configuration values, including passwords.
		return nil, nil, nil, errors.New("invalid COS nats.conf; check NATS configuration syntax")
	}
	if !opts.JetStream {
		return nil, nil, nil, errors.New("COS nats.conf must enable JetStream")
	}
	name := strings.ToUpper(opts.Cluster.Name)
	if (name != "COS" && !strings.HasPrefix(name, "COS-")) || strings.Contains(name, "PROD") {
		return nil, nil, nil, errors.New("COS requires a dedicated COS cluster; production clusters are forbidden")
	}
	if opts.Cluster.Port == 0 || opts.Cluster.Username == "" || opts.Cluster.Password == "" {
		return nil, nil, nil, errors.New("COS cluster requires a route listener and user/password authentication")
	}
	if len(opts.Gateway.Gateways) != 0 || opts.Gateway.Port != 0 || len(opts.LeafNode.Remotes) != 0 || opts.LeafNode.Port != 0 {
		return nil, nil, nil, errors.New("COS cluster must not bridge to gateways or leaf nodes")
	}
	for _, route := range opts.Routes {
		if route.Scheme != "nats-route" || !cosRouteHost(route.Hostname()) {
			return nil, nil, nil, errors.New("COS routes must use COS hosts over Tailscale or loopback")
		}
	}
	if opts.Port == 0 {
		opts.Port = nats.DefaultPort
	}
	if opts.Port < 1 || opts.Port > 65535 {
		return nil, nil, nil, errors.New("COS client listener requires a fixed port between 1 and 65535")
	}
	if opts.NoAuthUser != "" || opts.Authorization != "" || len(opts.Accounts) != 0 || opts.AuthCallout != nil || len(opts.TrustedOperators) != 0 {
		return nil, nil, nil, errors.New("COS requires user/password or nkey authentication without anonymous access")
	}
	if opts.StoreDir == "" {
		opts.StoreDir = filepath.Join(cfg.Home, ".local/share/cos/nats")
	}
	opts.NoLog, opts.NoSigs = true, true
	opts.Debug, opts.Trace, opts.TraceVerbose = false, false, false
	if opts.ServerName == "" {
		opts.ServerName = cfg.LocalHost
	}
	username, password := opts.Username, opts.Password
	if len(opts.Users) == 1 {
		username, password = opts.Users[0].Username, opts.Users[0].Password
	}
	if len(opts.Nkeys) == 0 && len(opts.Users) <= 1 && username != "" && password != "" && !strings.HasPrefix(password, "$2") {
		return opts, nats.UserInfo(username, password), nil, nil
	}
	if len(opts.Nkeys) == 0 || len(opts.Users) != 0 || username != "" || password != "" {
		return nil, nil, nil, errors.New("COS requires one plaintext user/password or nkey users with a local nats.seed")
	}
	seedPath := filepath.Join(cfg.Home, ".config/cos/nats.seed")
	if err := privateBusFile(seedPath); err != nil {
		return nil, nil, nil, err
	}
	seed, err := os.ReadFile(seedPath)
	if err != nil {
		return nil, nil, nil, errors.New("COS nats.seed is unreadable")
	}
	key, err := nkeys.FromSeed([]byte(strings.TrimSpace(string(seed))))
	clear(seed)
	if err != nil {
		return nil, nil, nil, errors.New("invalid COS nats.seed")
	}
	public, err := key.PublicKey()
	if err == nil && nkeys.IsValidPublicUserKey(public) {
		for _, user := range opts.Nkeys {
			if user.Nkey == public {
				return opts, nats.Nkey(public, key.Sign), key, nil
			}
		}
	}
	key.Wipe()
	return nil, nil, nil, errors.New("COS nats.seed must match an authorized user nkey")
}

func cosRouteHost(host string) bool {
	host = strings.ToLower(host)
	if host == "localhost" || host == "delta" || host == "beta" || host == "mariogk-dev" {
		return true
	}
	if strings.HasSuffix(host, ".ts.net") {
		first, _, _ := strings.Cut(host, ".")
		return first == "delta" || first == "beta" || first == "mariogk-dev"
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && (ip.IsLoopback() || netip.MustParsePrefix("100.64.0.0/10").Contains(ip) || netip.MustParsePrefix("fd7a:115c:a1e0::/48").Contains(ip))
}

func connectBus(ctx context.Context, opts *server.Options, auth nats.Option) (*nats.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	timeout := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	address := "nats://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(opts.Port))
	conn, err := nats.Connect(address, auth, nats.Name("cos"), nats.Timeout(timeout),
		nats.MaxReconnects(-1), nats.ReconnectWait(500*time.Millisecond),
		nats.PingInterval(15*time.Second), nats.MaxPingsOutstanding(2),
		nats.ReconnectBufSize(-1), nats.IgnoreDiscoveredServers(),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
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

// ConnectBus connects only to the local node configured in ~/.config/cos/nats.conf.
func ConnectBus(ctx context.Context, cfg Config) (*Bus, error) {
	opts, auth, key, err := loadBusConfig(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &Bus{ctx: ctx, cancel: cancel, done: make(chan struct{}), key: key}
	b.conn, err = connectBus(ctx, opts, auth)
	if err != nil {
		b.Close()
		return nil, err
	}
	b.watch(ctx)
	return b, nil
}

// StartBus embeds the COS JetStream node and registers its host-specific micro endpoints.
func StartBus(ctx context.Context, cfg Config, handler OperationHandler) (*Bus, error) {
	if !validHost(cfg.LocalHost) || handler == nil {
		return nil, errors.New("COS bus requires a valid local host and operation handler")
	}
	opts, auth, key, err := loadBusConfig(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &Bus{ctx: ctx, cancel: cancel, done: make(chan struct{}), key: key,
		active: make(map[string]context.CancelFunc), pending: make(map[string]*time.Timer)}
	b.server, err = server.NewServer(opts)
	if err != nil {
		b.Close()
		return nil, errors.New("COS embedded NATS configuration rejected")
	}
	go b.server.Start()
	ready, stopReady := context.WithTimeout(ctx, 5*time.Second)
	defer stopReady()
	for !b.server.ReadyForConnections(50 * time.Millisecond) {
		if err := ready.Err(); err != nil {
			b.Close()
			return nil, fmt.Errorf("COS embedded NATS did not become ready: %w", err)
		}
	}
	b.conn, err = connectBus(ctx, opts, auth)
	if err != nil {
		b.Close()
		return nil, err
	}
	b.service, err = micro.AddService(b.conn, micro.Config{
		Name: "cos", Version: "0.1.0", Metadata: map[string]string{"host": cfg.LocalHost},
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
		if b.key != nil {
			b.key.Wipe()
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
