package tailswarm

import (
	"context"
	"fmt"
	"log/slog"
)

// RunGateway is the egress data-plane entrypoint: the tailswarm binary in
// "gateway" mode. The control plane (reconcileEgress) deploys a companion
// service running tailswarm's own image with `gateway` as its argument and
// the TAILSWARM_GATEWAY_* env contract set on the spec (see gatewaySpec).
// This reads that contract, brings up one tsnet.Server under the app's
// egress tag, and forwards each overlay listener out the tailnet via
// srv.Dial — the mirror of the daemon's ingress proxies.
//
// It blocks until ctx is cancelled (SIGINT/SIGTERM), then shuts the proxy
// down cleanly. env defaults to os.Getenv when nil so production callers
// need not pass it; tests inject a map-backed lookup.
func RunGateway(ctx context.Context, env func(string) string, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}

	targets, err := parseEgressTargets(env(envGatewayTargets))
	if err != nil {
		return fmt.Errorf("gateway: %w", err)
	}
	if len(targets) == 0 {
		return fmt.Errorf("gateway: %s is empty", envGatewayTargets)
	}

	tag := env(envGatewayTag)
	cfg := EgressConfig{
		Hostname: env(envGatewayHostname),
		AuthKey:  env(envGatewayAuthKey),
		Targets:  targets,
		StateDir: env(envStateDir),
		LoginURL: env(envHeadscaleURL),
	}
	if tag != "" {
		cfg.Tags = []string{tag}
	}

	log.Info("gateway starting",
		"hostname", cfg.Hostname,
		"tag", tag,
		"targets", encodeTargets(targets))

	proxy, err := NewTsnetEgressProxy(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("gateway: %w", err)
	}

	<-ctx.Done()
	log.Info("gateway shutdown requested", "hostname", proxy.Hostname())
	if err := proxy.Close(); err != nil {
		log.Warn("gateway close", "hostname", proxy.Hostname(), "err", err)
	}
	return nil
}
