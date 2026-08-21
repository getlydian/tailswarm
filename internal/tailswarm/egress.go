package tailswarm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sync"

	"tailscale.com/tsnet"
)

// EgressTarget is one MagicDNS destination an egress gateway dials out to
// over the tailnet. The app reaches it by the real Host name on the
// overlay (Docker DNS resolves Host to the gateway, which holds it as an
// overlay alias); the gateway forwards each connection to Host:Port on
// the tailnet via tsnet.Server.Dial.
type EgressTarget struct {
	Host string
	Port uint32
}

func (t EgressTarget) addr() string {
	return fmt.Sprintf("%s:%d", t.Host, t.Port)
}

// EgressConfig is the input to the egress data plane (gateway mode). It
// is the mirror of ProxyConfig: the gateway owns a tsnet.Server for its
// tailnet identity, but listens on the overlay and dials out the tailnet.
type EgressConfig struct {
	Hostname string
	AuthKey  string
	Targets  []EgressTarget
	StateDir string
	LoginURL string
	Tags     []string
}

// EgressProxy owns one tsnet.Server and the goroutines forwarding TCP
// traffic from each overlay listener out to a MagicDNS target on the
// tailnet. It is the egress mirror of Proxy: overlay listener →
// srv.Dial, where Proxy is tailnet listener → net.Dial.
type EgressProxy struct {
	cfg    EgressConfig
	srv    tsnetDialer
	cancel context.CancelFunc
	wg     sync.WaitGroup
	log    *slog.Logger
}

// tsnetDialer is the subset of *tsnet.Server the egress path uses. It
// adds Dial (the outbound tailnet primitive) to the Close that Proxy also
// needs. Listen is absent: the gateway listens on the overlay with the
// host's net.Listen, not on the tailnet.
type tsnetDialer interface {
	Dial(ctx context.Context, network, addr string) (net.Conn, error)
	Close() error
}

// EgressFactory is the seam between the gateway entrypoint and tsnet.
// Production uses NewTsnetEgressProxy; tests inject a fake.
type EgressFactory func(ctx context.Context, cfg EgressConfig, log *slog.Logger) (*EgressProxy, error)

// NewTsnetEgressProxy is the production EgressFactory. It creates a real
// tsnet.Server, starts it (blocking on Headscale registration the first
// time), opens one overlay listener per target, and forwards each out the
// tailnet via srv.Dial.
func NewTsnetEgressProxy(ctx context.Context, cfg EgressConfig, log *slog.Logger) (*EgressProxy, error) {
	if cfg.Hostname == "" {
		return nil, errors.New("tailswarm: EgressConfig.Hostname is empty")
	}
	if len(cfg.Targets) == 0 {
		return nil, errors.New("tailswarm: EgressConfig.Targets is empty")
	}
	if cfg.StateDir == "" {
		return nil, errors.New("tailswarm: EgressConfig.StateDir is empty")
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

	if err := startTsnetBounded(ctx, srv, cfg.Hostname, tsnetStartTimeout, log); err != nil {
		if log != nil {
			log.Error("tsnet start failed", "hostname", cfg.Hostname, "err", err)
		}
		return nil, err
	}

	return startEgressProxyOn(ctx, srv, cfg, log)
}

// startEgressProxyOn is the inner constructor shared by
// NewTsnetEgressProxy and the fake-backed tests. The overlay listener is
// opened with the host's net package (reachable by the app on the
// overlay); the upstream dialer is backed by srv.Dial (out the tailnet).
func startEgressProxyOn(ctx context.Context, srv tsnetDialer, cfg EgressConfig, log *slog.Logger) (*EgressProxy, error) {
	return startEgressProxyWith(ctx, srv, cfg, listenOverlay, log)
}

// listenOverlay opens an overlay-side listener for an egress target. In
// production this binds the target port on the gateway container so the
// app can reach it by the target's overlay-aliased name. Tests override
// it to bind an ephemeral loopback port.
func listenOverlay(t EgressTarget) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf(":%d", t.Port))
}

// startEgressProxyWith is the testable core: it takes the overlay-listen
// function as a seam so tests can bind ephemeral ports instead of the
// fixed target port.
func startEgressProxyWith(ctx context.Context, srv tsnetDialer, cfg EgressConfig, listen func(EgressTarget) (net.Listener, error), log *slog.Logger) (*EgressProxy, error) {
	if log == nil {
		log = slog.Default()
	}
	pctx, cancel := context.WithCancel(ctx)
	p := &EgressProxy{cfg: cfg, srv: srv, cancel: cancel, log: log}

	for _, t := range cfg.Targets {
		ln, err := listen(t)
		if err != nil {
			cancel()
			_ = srv.Close()
			return nil, fmt.Errorf("listen overlay %s: %w", t.addr(), err)
		}
		dst := t.addr()
		dial := tailnetDialer(srv, dst)
		// Unlike ingress (where srv.Close closes the tailnet listeners),
		// these overlay listeners are ours to close — do it on cancel so
		// acceptLoop's blocking Accept unblocks.
		go func() {
			<-pctx.Done()
			_ = ln.Close()
		}()
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			acceptLoop(pctx, ln, dst, dial, log)
		}()
	}
	return p, nil
}

// tailnetDialer returns a dialFunc that dials addr over the tailnet via
// the gateway's tsnet.Server. This is the egress upstream path — the
// mirror of overlayDialer.
func tailnetDialer(srv tsnetDialer, addr string) dialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		return srv.Dial(ctx, "tcp", addr)
	}
}

// Close cancels the proxy's context, shuts down the tsnet.Server (which
// unblocks any in-flight srv.Dial), and waits for all forwarding
// goroutines to finish. The overlay listeners are closed by the context
// cancel unblocking acceptLoop; tsnet.Close handles the tailnet side.
func (p *EgressProxy) Close() error {
	p.cancel()
	err := p.srv.Close()
	p.wg.Wait()
	return err
}

// Hostname returns the tailnet hostname this gateway registered under.
func (p *EgressProxy) Hostname() string { return p.cfg.Hostname }
