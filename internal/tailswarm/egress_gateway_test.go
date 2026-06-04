package tailswarm

import (
	"slices"
	"strings"
	"testing"
)

// gatewaySpec must produce a ContainerSpec whose entrypoint is the
// image's absolute binary path. The distroless image (ENTRYPOINT
// ["/tailswarm"]) has no shell and no $PATH lookup, so a bare
// "tailswarm" command fails at runtime with "executable file not found
// in $PATH". This test is the regression guard for that bug.
func TestGatewaySpecRunsAbsoluteEntrypoint(t *testing.T) {
	eg := &EgressSpec{
		Hostname: "phpmyadmin-egress",
		Tag:      "tag:svc-reporting",
		Network:  defaultOverlay,
		Targets:  []EgressTarget{{Host: "db-mysql", Port: 3306}},
	}
	cfg := Config{
		Headscale: HeadscaleConfig{URL: "https://hs"},
		Tsnet:     TsnetConfig{StateDir: "/var/lib/tailswarm"},
	}

	spec := gatewaySpec(eg, eg.Targets[0], "app-service-id", cfg, "ghcr.io/getlydian/tailswarm:test", "authkey")

	cs := spec.TaskTemplate.ContainerSpec
	if cs == nil {
		t.Fatal("ContainerSpec is nil")
	}
	if want := []string{"/tailswarm"}; !slices.Equal(cs.Command, want) {
		t.Errorf("Command = %v, want %v (must be the absolute ENTRYPOINT path, not a bare name)", cs.Command, want)
	}
	if want := []string{"gateway"}; !slices.Equal(cs.Args, want) {
		t.Errorf("Args = %v, want %v", cs.Args, want)
	}
}

// gatewaySpec wires one target through into the env contract, network
// alias, marker label, and image the gateway needs to run. There is one
// gateway per target: its spec carries the single target's addr and the
// single host as an alias, under a per-target hostname.
func TestGatewaySpecContract(t *testing.T) {
	eg := &EgressSpec{
		Hostname: "thanos-query-lydian-egress",
		Tag:      "tag:svc-thanos-query-ops-lydian",
		Network:  defaultOverlay,
		Targets: []EgressTarget{
			{Host: "thanos-store-lydian", Port: 10901},
			{Host: "thanos-query-kulmin", Port: 10901},
		},
	}
	cfg := Config{
		Headscale: HeadscaleConfig{URL: "https://headscale.lyops.ee"},
		Tsnet:     TsnetConfig{StateDir: "/var/lib/tailswarm"},
	}

	target := eg.Targets[0]
	spec := gatewaySpec(eg, target, "query-svc-id", cfg, "img:tag", "secret-key")

	// Image + marker label.
	if got := spec.TaskTemplate.ContainerSpec.Image; got != "img:tag" {
		t.Errorf("Image = %q, want img:tag", got)
	}
	if got := spec.Labels[gatewayForLabel]; got != "query-svc-id" {
		t.Errorf("marker label = %q, want query-svc-id", got)
	}
	// A gateway must never be picked up as an ingress target.
	if _, isApp := spec.Labels["tailswarm.enable"]; isApp {
		t.Error("gateway must not carry tailswarm.enable")
	}

	// Per-target service name and hostname disambiguate sibling gateways.
	if want := "tsgw-thanos-query-lydian-egress-thanos-store-lydian"; spec.Name != want {
		t.Errorf("service name = %q, want %q", spec.Name, want)
	}

	// One network attachment, on the egress overlay, aliasing only this
	// gateway's single target host so overlay DNS resolves it here.
	nets := spec.TaskTemplate.Networks
	if len(nets) != 1 || nets[0].Target != defaultOverlay {
		t.Fatalf("networks = %+v, want one on %s", nets, defaultOverlay)
	}
	if want := []string{"thanos-store-lydian"}; !slices.Equal(nets[0].Aliases, want) {
		t.Errorf("aliases = %v, want %v (one per gateway)", nets[0].Aliases, want)
	}

	// Env contract the gateway entrypoint (RunGateway) reads back. Targets
	// is the single target addr; hostname is the per-target name.
	env := parseEnvList(spec.TaskTemplate.ContainerSpec.Env)
	for k, want := range map[string]string{
		envHeadscaleURL:    "https://headscale.lyops.ee",
		envStateDir:        "/var/lib/tailswarm",
		envGatewayHostname: "thanos-query-lydian-egress-thanos-store-lydian",
		envGatewayTag:      "tag:svc-thanos-query-ops-lydian",
		envGatewayTargets:  "thanos-store-lydian:10901",
		envGatewayAuthKey:  "secret-key",
	} {
		if env[k] != want {
			t.Errorf("env %s = %q, want %q", k, env[k], want)
		}
	}
}

// An empty authKey (the hash-only spec build) must not emit an auth-key
// env entry.
func TestGatewaySpecOmitsEmptyAuthKey(t *testing.T) {
	eg := &EgressSpec{
		Hostname: "app-egress",
		Network:  defaultOverlay,
		Targets:  []EgressTarget{{Host: "db", Port: 5432}},
	}
	spec := gatewaySpec(eg, eg.Targets[0], "id", Config{}, "img", "")
	if _, ok := parseEnvList(spec.TaskTemplate.ContainerSpec.Env)[envGatewayAuthKey]; ok {
		t.Error("empty authKey must not set the auth-key env var")
	}
}

// gatewayHostname keeps within tailscale's length cap, falling back to a
// truncated prefix plus a stable hash suffix for very long target names so
// sibling gateways still get distinct hostnames.
func TestGatewayHostnameLengthCap(t *testing.T) {
	eg := &EgressSpec{Hostname: strings.Repeat("a", 50)}
	long := EgressTarget{Host: strings.Repeat("b", 50), Port: 10901}
	got := gatewayHostname(eg, long)
	if len(got) > maxTailnetHostname {
		t.Errorf("hostname %q len %d exceeds cap %d", got, len(got), maxTailnetHostname)
	}
	// Distinct targets must still yield distinct hostnames.
	other := EgressTarget{Host: strings.Repeat("c", 50), Port: 10901}
	if gatewayHostname(eg, other) == got {
		t.Error("distinct long targets collapsed to the same hostname")
	}
}

// parseEnvList turns a ["K=V", ...] env slice into a map for assertions.
// (Distinct from envMap in gateway_run_test.go, which builds the inverse
// — a lookup func from a map.)
func parseEnvList(env []string) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		if k, v, ok := strings.Cut(e, "="); ok {
			m[k] = v
		}
	}
	return m
}
