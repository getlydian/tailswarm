// Command tailswarm is the daemon entry point.
//
// Run-time wiring lives in run() so the tests in main_test.go can drive
// the same code path with fakes. main() does nothing but parse argv and
// delegate.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/getlydian/tailswarm/internal/tailswarm"
)

const (
	defaultWorkerCount    = 8
	defaultQueueBuffer    = 256
	defaultEventChanDepth = 256
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr, nil); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "tailswarm:", err)
		os.Exit(1)
	}
}

// runDeps lets tests inject a fake Docker + Controller without going
// through the real SDK. Production callers pass nil and run() builds the
// real implementations from config.
type runDeps struct {
	Docker     tailswarm.DockerClient
	Events     tailswarm.EventStream
	Controller tailswarm.Controller
	// Started fires after Resync completes and the watcher loop is
	// running. Tests use it to know when it's safe to inject events.
	Started chan<- struct{}
}

// run is the testable wrapper around the daemon. It loads config,
// builds the dependency graph, runs the resync, and then drives the
// watcher → queue → reconciler pipeline until ctx is cancelled.
func run(ctx context.Context, args []string, env func(string) string, stdout, stderr io.Writer, deps *runDeps) error {
	if env == nil {
		env = os.Getenv
	}

	fs := flag.NewFlagSet("tailswarm", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", env("TAILSWARM_CONFIG"), "path to YAML config file (env: TAILSWARM_CONFIG)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := tailswarm.Load(*configPath, env)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger = logger.With("component", "tailswarm")
	slog.SetDefault(logger)

	// Wire dependencies. Tests pass deps; production builds the real
	// docker + headscale clients.
	var (
		dockerClient tailswarm.DockerClient
		eventStream  tailswarm.EventStream
		ctrl         tailswarm.Controller
		closeFns     []func() error
	)
	if deps != nil {
		dockerClient = deps.Docker
		eventStream = deps.Events
		ctrl = deps.Controller
	}
	if dockerClient == nil || eventStream == nil {
		d, err := tailswarm.NewDocker()
		if err != nil {
			return fmt.Errorf("docker client: %w", err)
		}
		closeFns = append(closeFns, d.Close)
		if dockerClient == nil {
			dockerClient = d
		}
		if eventStream == nil {
			eventStream = d
		}
	}
	if ctrl == nil {
		ctrl = &tailswarm.Headscale{
			BaseURL: cfg.Headscale.URL,
			APIKey:  cfg.Headscale.APIKey,
		}
	}
	defer func() {
		for _, fn := range closeFns {
			if err := fn(); err != nil {
				logger.Warn("close on shutdown", "err", err)
			}
		}
	}()

	store := tailswarm.NewStore()
	reconciler := tailswarm.NewReconciler(dockerClient, ctrl, store, cfg)
	reconciler.Log = logger.With("subcomponent", "reconciler")

	// DESIGN.md §7: rebuild state from authoritative sources before we
	// start consuming events, so a tailswarm restart doesn't double-mint
	// keys or leave orphan sidecars from a previous crash.
	if err := reconciler.Resync(ctx); err != nil {
		// A failed resync isn't fatal — Headscale or Docker may be
		// transiently unavailable on boot — but it's worth shouting.
		logger.Warn("startup resync failed; continuing", "err", err)
	}

	queue := tailswarm.NewQueue(defaultWorkerCount, defaultQueueBuffer)
	events := make(chan string, defaultEventChanDepth)

	watcher := &tailswarm.Watcher{
		Docker:         dockerClient,
		Events:         eventStream,
		Out:            events,
		FullResync:     cfg.Reconcile.FullResyncInterval,
		LabelNamespace: cfg.LabelNamespace,
		Log:            logger.With("subcomponent", "watcher"),
	}

	var wg sync.WaitGroup

	// Watcher → queue feeder. A tiny shim so the watcher's typed channel
	// drives the queue's Enqueue method, with ctx-aware shutdown.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case id, ok := <-events:
				if !ok {
					return
				}
				queue.Enqueue(id)
			}
		}
	}()

	// Reconcile workers (sharded inside the queue).
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Run(ctx, func(ctx context.Context, serviceID string) {
			if err := reconciler.Reconcile(ctx, serviceID); err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn("reconcile failed", "service_id", serviceID, "err", err)
			}
		})
	}()

	// Watcher loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := watcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("watcher exited", "err", err)
		}
	}()

	if deps != nil && deps.Started != nil {
		close(deps.Started)
	}

	logger.Info("tailswarm started",
		"label_namespace", cfg.LabelNamespace,
		"headscale_url", cfg.Headscale.URL,
		"resync_interval", cfg.Reconcile.FullResyncInterval)

	<-ctx.Done()
	logger.Info("shutdown requested; draining workers")
	wg.Wait()
	return nil
}
