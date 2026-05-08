package tailswarm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"

	"tailscale.com/tsnet"
)

// ProxyConfig is the input to StartProxy. Hostname becomes the tailnet
// node name; Target is the DNS name (and the per-port suffix) to dial on
// the shared overlay.
type ProxyConfig struct {
	Hostname string
	Target   string
	AuthKey  string
	Ports    []Port
	StateDir string
	LoginURL string
	Tags     []string
}

// Proxy owns one tsnet.Server and the goroutines forwarding TCP traffic
// from each tailnet listener to the target service on the overlay
// network. Close shuts everything down and removes the tsnet identity
// from memory; on-disk state survives so the next start re-uses the
// same Headscale node.
type Proxy struct {
	cfg    ProxyConfig
	srv    tsnetServer
	cancel context.CancelFunc
	wg     sync.WaitGroup
	log    *slog.Logger
}

// tsnetServer is the subset of *tsnet.Server we actually use, extracted
// so unit tests can swap in a loopback fake without spinning up a real
// node.
type tsnetServer interface {
	Listen(network, addr string) (net.Listener, error)
	Close() error
}

// ProxyFactory is the seam between the reconciler and tsnet. Production
// uses NewTsnetProxy; tests inject a fake.
type ProxyFactory func(ctx context.Context, cfg ProxyConfig, log *slog.Logger) (*Proxy, error)

// NewTsnetProxy is the production ProxyFactory. It creates a real
// tsnet.Server, starts it (which blocks on Headscale registration the
// first time), opens one listener per configured TCP port, and starts
// forwarding to cfg.Target.
func NewTsnetProxy(ctx context.Context, cfg ProxyConfig, log *slog.Logger) (*Proxy, error) {
	if cfg.Hostname == "" {
		return nil, errors.New("tailswarm: ProxyConfig.Hostname is empty")
	}
	if cfg.Target == "" {
		return nil, errors.New("tailswarm: ProxyConfig.Target is empty")
	}
	if len(cfg.Ports) == 0 {
		return nil, errors.New("tailswarm: ProxyConfig.Ports is empty")
	}
	if cfg.StateDir == "" {
		return nil, errors.New("tailswarm: ProxyConfig.StateDir is empty")
	}

	srv := &tsnet.Server{
		Hostname:  cfg.Hostname,
		AuthKey:   cfg.AuthKey,
		Dir:       filepath.Join(cfg.StateDir, cfg.Hostname),
		Ephemeral: true,
		Logf:      tsnetLogf(log),
	}
	if cfg.LoginURL != "" {
		srv.ControlURL = cfg.LoginURL
	}

	if err := srv.Start(); err != nil {
		if log != nil {
			log.Error("tsnet start failed", "hostname", cfg.Hostname, "err", err)
		}
		return nil, fmt.Errorf("tsnet start %s: %w", cfg.Hostname, err)
	}

	return startProxyOn(ctx, srv, cfg, log)
}

// startProxyOn is the inner constructor shared by NewTsnetProxy and the
// fake-backed tests. Splitting it out keeps Listen/forwardTCP wiring in
// one place.
func startProxyOn(ctx context.Context, srv tsnetServer, cfg ProxyConfig, log *slog.Logger) (*Proxy, error) {
	if log == nil {
		log = slog.Default()
	}
	pctx, cancel := context.WithCancel(ctx)
	p := &Proxy{cfg: cfg, srv: srv, cancel: cancel, log: log}

	for _, port := range cfg.Ports {
		ln, err := srv.Listen("tcp", fmt.Sprintf(":%d", port.Target))
		if err != nil {
			cancel()
			_ = srv.Close()
			return nil, fmt.Errorf("listen %d: %w", port.Target, err)
		}
		dst := fmt.Sprintf("%s:%d", cfg.Target, port.Target)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			acceptLoop(pctx, ln, dst, log)
		}()
	}
	return p, nil
}

// Close cancels the proxy's context, shuts down the tsnet.Server (which
// closes every listener and unblocks acceptLoop), and waits for all
// forwarding goroutines to finish.
func (p *Proxy) Close() error {
	p.cancel()
	err := p.srv.Close()
	p.wg.Wait()
	return err
}

// Hostname returns the tailnet hostname this proxy was started with.
// Useful for log fields and tests.
func (p *Proxy) Hostname() string { return p.cfg.Hostname }

// acceptLoop accepts incoming tailnet connections on ln and spawns a
// forwarding goroutine for each. Exits when ctx is cancelled or the
// listener is closed.
func acceptLoop(ctx context.Context, ln net.Listener, target string, log *slog.Logger) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Listener closed without context cancel — tsnet shutting
			// down. Either way, exit.
			return
		}
		go forwardTCP(ctx, conn, target, log)
	}
}

// forwardTCP dials target and pumps bytes between conn and the upstream
// connection. Closes both ends on either context cancel or upstream
// failure.
func forwardTCP(ctx context.Context, conn net.Conn, target string, log *slog.Logger) {
	defer func() { _ = conn.Close() }()

	var d net.Dialer
	upstream, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		log.Warn("dial upstream", "target", target, "err", err)
		return
	}
	defer func() { _ = upstream.Close() }()

	// Cancel both halves when ctx fires so a shutdown unblocks the
	// io.Copy pumps that would otherwise wait on Read forever.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
		_ = upstream.Close()
	}()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, conn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, upstream); done <- struct{}{} }()
	<-done
}

// tsnetLogf adapts a slog.Logger into the tsnet logf signature so we get
// structured tsnet logs alongside the rest of tailswarm. tsnet is chatty
// at info level, so we route everything to Debug.
func tsnetLogf(log *slog.Logger) func(string, ...any) {
	if log == nil {
		log = slog.Default()
	}
	return func(format string, args ...any) {
		log.Debug(fmt.Sprintf(format, args...))
	}
}
