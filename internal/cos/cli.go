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
	"path/filepath"
	"slices"
	"strconv"
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
		commands := []string{"fleet", "prs", "merge <repo> <number> (COS_FULL_SEND=1, COS_ADOPTED=1)", "status [--brief]", "task set '<json: ticket, why, modelWhy, next>' | rm <ticket> | list", "note <direction|missing|suggest|need|decide> '<text>' | --rm <id> | --clear", "serve [--port 8790]", "cluster"}
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
	if command == "prs" {
		if len(args) != 0 {
			return errors.New("usage: cos prs")
		}
		v, err := (Forgejo{Config: cfg}).PRs(ctx)
		if err != nil {
			return err
		}
		return output(w, v)
	}
	if command == "merge" {
		if len(args) != 2 {
			return errors.New("usage: cos merge <repo> <number>")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil || n < 1 {
			return errors.New("PR number must be positive")
		}
		v, err := (Forgejo{Config: cfg}).Merge(ctx, args[0], n)
		if err != nil {
			return err
		}
		return output(w, v)
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
	if command == "status" {
		if len(args) > 1 || (len(args) == 1 && args[0] != "--brief") {
			return errors.New("usage: cos status [--brief]")
		}
		s, err := stateStore(cfg)
		if err != nil {
			return err
		}
		defer s.Close()
		bus, err := ConnectBus(ctx, cfg)
		if err != nil {
			return err
		}
		defer bus.Close()
		old, err := s.Live(ctx)
		if err != nil {
			return err
		}
		if err := s.SaveLive(ctx, Refresh(ctx, cfg, bus, old)); err != nil {
			return err
		}
		v, err := View(ctx, s)
		if err != nil {
			return err
		}
		if len(args) == 1 {
			_, err = io.WriteString(w, Brief(v))
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
	return output(w, v)
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
	bus, err := StartBus(ctx, cfg, func(ctx context.Context, cmd string, args []string) (any, error) {
		return LocalCommand(ctx, cfg, cmd, args)
	})
	if err != nil {
		return err
	}
	defer bus.Close()
	js, err := bus.JetStream()
	if err != nil {
		return err
	}
	auth := NewAuth(cfg.Home, js)
	go auth.Start(ctx)
	w := NewWeb(s, auth)
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
	defer func() { cancel(); <-inboxDone; <-usageDone; <-mirrorDone }()
	l, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: w.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	pollDone, chatDone := make(chan struct{}), make(chan struct{})
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
	defer func() { cancel(); <-pollDone; <-chatDone }()
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
