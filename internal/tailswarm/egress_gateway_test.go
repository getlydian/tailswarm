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

	spec := gatewaySpec(eg, "app-service-id", cfg, "ghcr.io/getlydian/tailswarm:test", "authkey")

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

// gatewaySpec wires the EgressSpec through into the env contract,
// network alias, marker label, and image the gateway needs to run.
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

	spec := gatewaySpec(eg, "query-svc-id", cfg, "img:tag", "secret-key")

	// Image + marker label.
	if got := spec.TaskTemplate.ContainerSpec.Image; got != "img:tag" {
		t.Errorf("Image = %q, want img:tag", got)
	}
	if got := spec.Annotations.Labels[gatewayForLabel]; got != "query-svc-id" {
		t.Errorf("marker label = %q, want query-svc-id", got)
	}
	// A gateway must never be picked up as an ingress target.
	if _, isApp := spec.Annotations.Labels["tailswarm.enable"]; isApp {
		t.Error("gateway must not carry tailswarm.enable")
	}

	// One network attachment, on the egress overlay, aliasing every
	// target host so overlay DNS resolves the real remote name here.
	nets := spec.TaskTemplate.Networks
	if len(nets) != 1 || nets[0].Target != defaultOverlay {
		t.Fatalf("networks = %+v, want one on %s", nets, defaultOverlay)
	}
	wantAliases := []string{"thanos-store-lydian", "thanos-query-kulmin"}
	if !slices.Equal(nets[0].Aliases, wantAliases) {
		t.Errorf("aliases = %v, want %v", nets[0].Aliases, wantAliases)
	}

	// Env contract the gateway entrypoint (RunGateway) reads back.
	env := parseEnvList(spec.TaskTemplate.ContainerSpec.Env)
	for k, want := range map[string]string{
		envHeadscaleURL:    "https://headscale.lyops.ee",
		envStateDir:        "/var/lib/tailswarm",
		envGatewayHostname: "thanos-query-lydian-egress",
		envGatewayTag:      "tag:svc-thanos-query-ops-lydian",
		envGatewayTargets:  "thanos-store-lydian:10901,thanos-query-kulmin:10901",
		envGatewayAuthKey:  "secret-key",
	} {
		if env[k] != want {
			t.Errorf("env %s = %q, want %q", k, env[k], want)
		}
	}
}

// gatewaySpec deduplicates aliases when two targets share a host (e.g.
// two ports on the same remote), since a network alias is per-host.
func TestGatewaySpecDedupesAliases(t *testing.T) {
	eg := &EgressSpec{
		Hostname: "app-egress",
		Network:  defaultOverlay,
		Targets: []EgressTarget{
			{Host: "db", Port: 5432},
			{Host: "db", Port: 5433},
		},
	}
	spec := gatewaySpec(eg, "id", Config{}, "img", "")
	aliases := spec.TaskTemplate.Networks[0].Aliases
	if want := []string{"db"}; !slices.Equal(aliases, want) {
		t.Errorf("aliases = %v, want %v (one per host)", aliases, want)
	}
	// No auth key → no auth-key env entry.
	if _, ok := parseEnvList(spec.TaskTemplate.ContainerSpec.Env)[envGatewayAuthKey]; ok {
		t.Error("empty authKey must not set the auth-key env var")
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
