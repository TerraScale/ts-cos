package cos

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"time"
)

func output(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
func stateStore(cfg Config) (*Store, error) {
	return OpenStore(filepath.Join(cfg.Home, ".local/share/cos/cos.db"))
}

func Run(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: cos <command> ...; use cos help")
	}
	command, args := args[0], args[1:]
	if command == "help" || command == "--help" || command == "-h" {
		commands := []string{"fleet", "task set '<json: ticket, why, modelWhy, next>' | rm <ticket> | list", "note <direction|missing|suggest|need|decide> '<text>' | --rm <id> | --clear", "serve [--port 8790]", "cluster"}
		for _, k := range sortedCommandNames() {
			commands = append(commands, localCommands[k].Usage)
		}
		for _, k := range slices.Sorted(maps.Keys(cliCommands)) {
			commands = append(commands, cliCommands[k].Usage)
		}
		return output(w, map[string]any{"commands": commands})
	}
	if c, ok := cliCommands[command]; ok {
		return c.Run(ctx, cfg, args, w)
	}
	if command == "serve" {
		return Serve(ctx, cfg, args)
	}
	if command == "task" || command == "note" {
		s, err := stateStore(cfg)
		if err != nil {
			return err
		}
		defer s.Close()
		if command == "task" {
			return taskCommand(ctx, s, args, w)
		}
		return noteCommand(ctx, s, args, w)
	}
	if command == "cluster" {
		if len(args) != 0 {
			return errors.New("usage: cos cluster")
		}
		bus, err := ConnectBus(ctx, cfg)
		if err != nil {
			return err
		}
		defer bus.Close()
		leader, err := bus.Leader(ctx)
		if err != nil {
			return err
		}
		return output(w, map[string]string{"host": cfg.LocalHost, "leader": leader, "version": Version})
	}
	if command == "fleet" || command == "asks" {
		if len(args) != 0 {
			return fmt.Errorf("usage: cos %s", command)
		}
		bus, err := ConnectBus(ctx, cfg)
		if err != nil {
			return err
		}
		defer bus.Close()
		if command == "fleet" {
			v, err := Fleet(ctx, bus)
			if err != nil {
				return err
			}
			return output(w, v)
		}
		v, err := AllAsks(ctx, bus)
		if err != nil {
			return err
		}
		return output(w, v)
	}
	if _, ok := localCommands[command]; !ok {
		return fmt.Errorf("unknown command %q; use cos help", command)
	}
	if len(args) < 1 {
		return fmt.Errorf("usage: cos %s", localCommands[command].Usage)
	}
	host := args[0]
	if !validHost(host) {
		return errors.New("invalid host")
	}
	if err := validateLocal(command, args[1:]); err != nil {
		return err
	}
	bus, err := ConnectBus(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()
	var v any
	if err := bus.Call(ctx, host, command, args[1:], &v); err != nil {
		return err
	}
	if tab := os.Getenv("COS_TAB"); command == "launch" && tab != "" {
		if err := tagLaunch(ctx, bus, host, tab, v); err != nil {
			fmt.Fprintln(os.Stderr, "warning: launched thread is not tagged for its chat tab:", err)
		}
	}
	return output(w, v)
}

// tagLaunch tags the thread a launch returned, so the tab's Threads panel lists it.
func tagLaunch(ctx context.Context, bus *Bus, host, tab string, out any) error {
	launched, _ := out.(map[string]any)
	id, _ := launched["threadId"].(string)
	if id == "" {
		return errors.New("launch returned no thread ID")
	}
	return TagThread(ctx, bus, ChatTag{ThreadID: id, Host: host, Tab: tab, At: time.Now().UTC()})
}
func sortedCommandNames() []string {
	m := map[string]any{}
	for k := range localCommands {
		m[k] = nil
	}
	return sortedKeys(m)
}

func taskCommand(ctx context.Context, s *Store, args []string, w io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: cos task set '<json>' | rm <ticket> | list")
	}
	switch args[0] {
	case "set":
		if len(args) != 2 {
			return errors.New("usage: cos task set '<json: ticket, why, modelWhy, next>'")
		}
		patch, err := objectJSON(args[1])
		if err != nil {
			return err
		}
		_, err = s.SetTask(ctx, patch)
		if err != nil {
			return err
		}
		return output(w, patch)
	case "rm":
		if len(args) != 2 {
			return errors.New("usage: cos task rm <ticket>")
		}
		if err := s.RemoveTask(ctx, args[1]); err != nil {
			return err
		}
		return output(w, map[string]any{"removed": args[1]})
	case "list":
		if len(args) != 1 {
			return errors.New("usage: cos task list")
		}
		v, err := View(ctx, s)
		if err != nil {
			return err
		}
		return output(w, v.Tasks)
	}
	return errors.New("task action must be set, rm, or list")
}
func noteCommand(ctx context.Context, s *Store, args []string, w io.Writer) error {
	if len(args) < 2 || len(args) > 3 {
		return errors.New("usage: cos note <direction|missing|suggest|need|decide> '<text>' | --rm <id> | --clear")
	}
	kind := args[0]
	if args[1] == "--clear" {
		if len(args) != 2 {
			return errors.New("--clear takes no value")
		}
		if err := s.ClearNotes(ctx, kind); err != nil {
			return err
		}
		return output(w, map[string]any{"cleared": kind})
	}
	if args[1] == "--rm" {
		if len(args) != 3 {
			return errors.New("--rm requires a note ID")
		}
		id, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil || id < 1 {
			return errors.New("note ID must be positive")
		}
		if err := s.RemoveNote(ctx, kind, id); err != nil {
			return err
		}
		return output(w, map[string]any{"removed": id})
	}
	if len(args) != 2 {
		return errors.New("note text must be one quoted argument")
	}
	v, err := s.AddNote(ctx, kind, args[1])
	if err != nil {
		return err
	}
	return output(w, v)
}

// serveWorkers run beside the dashboard on every node; features register them
// from their own files with init() so Serve stays unchanged.
var serveWorkers []func(context.Context, Config, *Bus)

func Serve(ctx context.Context, cfg Config, args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	port := flags.Int("port", 8790, "dashboard port")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *port < 1 || *port > 65535 {
		return errors.New("usage: cos serve [--port 8790]")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s, err := stateStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	var bus *Bus
	var gate atomic.Pointer[Gate]
	bus, err = StartBus(ctx, cfg, func(ctx context.Context, cmd string, args []string) (any, error) {
		if cmd == "usage-verify" {
			return usageVerify(ctx, bus, s, cfg.LocalHost, args)
		}
		if slices.Contains(gatedCommands, cmd) {
			g := gate.Load()
			if g == nil {
				return nil, OperationError("COS is starting; try again")
			}
			return g.Command(ctx, cmd, args)
		}
		return LocalCommand(ctx, cfg, cmd, args)
	})
	if err != nil {
		return err
	}
	gate.Store(newSendGate(cfg, bus))
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return err
	}
	auth := NewAuth(cfg.Home, js)
	go auth.Start(ctx)
	w := NewWeb(s, auth)
	w.Bus = bus
	hooks := NewHooks(bus)
	go hooks.Start(ctx)
	w.Hooks = hooks
	probe := func(ctx context.Context) []string { return dashboardProbe(ctx, *port, auth) }
	if err := bus.serveReleases(ctx, cfg, probe, cancel); err != nil {
		return err
	}
	go bus.Lead(ctx, func(ctx context.Context) { RunRollouts(ctx, cfg, bus) })
	go clusterRelease.watch(ctx, cfg, js)
	w.Autopilot, err = StartAutopilot(ctx, cfg.LocalHost, bus, s, Reconcile(cfg, bus, s), NewRulesEngine(cfg, bus, s))
	if err != nil {
		return err
	}
	if err := w.Watch(ctx); err != nil {
		return err
	}
	defer w.Close()
	inboxDone := make(chan struct{})
	go func() { defer close(inboxDone); RunInbox(ctx, bus, cfg.LocalHost, s, w.inbox.Store) }()
	usageDone := make(chan struct{})
	go func() { defer close(usageDone); RunUsage(ctx, bus, cfg.LocalHost, s) }()
	mirrorDone := make(chan struct{})
	go func() { defer close(mirrorDone); RunEventMirror(ctx, bus, s) }()
	pushDone := make(chan struct{})
	go func() { defer close(pushDone); RunPush(ctx, bus, w.Chat, w.push.Store) }()
	defer func() { cancel(); <-inboxDone; <-usageDone; <-mirrorDone; <-pushDone }()
	l, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: w.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	pollDone, chatDone, kpiDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { defer close(pollDone); w.Poll(ctx, cfg, bus) }()
	behaviorDone := make(chan struct{})
	go func() { defer close(behaviorDone); RunBehavior(ctx, cfg, bus, behaviors) }()
	defer func() { cancel(); <-behaviorDone }()
	go func() {
		// Public traffic reaches this node only once it serves signed-in users; until its
		// sessions bucket opens it would answer 401 to everyone Cloudflare sends here.
		for len(probe(ctx)) > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		runTunnel(ctx)
	}()
	for _, worker := range serveWorkers {
		go worker(ctx, cfg, bus)
	}
	go func() { defer close(chatDone); w.Chat.Run(ctx, bus) }()
	go func() { defer close(kpiDone); w.KPI.Run(ctx, bus) }()
	defer func() { cancel(); <-pollDone; <-chatDone; <-kpiDone }()
	go releasesOf(cfg).watchdog(ctx, CurrentBuild(cfg).Revision, trialTimeout,
		func(ctx context.Context) []string { return bus.health(ctx, probe) }, cancel)
	slog.Info("COS dashboard listening", "address", l.Addr().String())
	err = serveDashboard(ctx, server, l, cancel)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func serveDashboard(ctx context.Context, server *http.Server, listener net.Listener, cancel context.CancelFunc) error {
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
	}()
	err := server.Serve(listener)
	cancel()
	<-shutdownDone
	return err
}
