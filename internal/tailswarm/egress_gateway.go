package tailswarm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/docker/docker/api/types/swarm"
)

// Gateway-mode environment variables. The control plane sets these on the
// gateway service spec; the gateway entrypoint (tailswarm in "gateway"
// mode) reads them to build its EgressConfig. They are the contract
// between the control plane (this file) and the data plane (egress.go).
const (
	envGatewayHostname = "TAILSWARM_GATEWAY_HOSTNAME"
	envGatewayTag      = "TAILSWARM_GATEWAY_TAG"
	envGatewayTargets  = "TAILSWARM_GATEWAY_TARGETS"
	envGatewayAuthKey  = "TAILSWARM_GATEWAY_AUTH_KEY"
	// The gateway reuses the daemon's headscale URL / state dir env names
	// so the same Config.applyEnv path populates them in gateway mode.
	envHeadscaleURL = "TAILSWARM_HEADSCALE_URL"
	envStateDir     = "TAILSWARM_TSNET_STATE_DIR"
)

// gatewaySpec is the pure translation of a single egress target (plus the
// owning EgressSpec for tag/network/base-hostname, the app it fronts, the
// daemon Config, the gateway image, and a freshly minted auth key) into the
// Swarm service spec for that target's egress gateway companion.
//
// There is one gateway per target, not one per app: each target needs its
// own Swarm service so it gets its own VIP/task IP. A single shared gateway
// holding every target Host as an alias collapses them to one VIP/one
// container IP (Docker assigns one VIP per service per network, regardless
// of alias count), so multiple same-port targets would both collide on
// bind and be indistinguishable at L4. Per-target services sidestep both.
//
// The gateway:
//   - runs tailswarm's own image (the daemon's own, discovered at startup)
//     in "gateway" mode;
//   - joins the egress overlay carrying its single target Host as a network
//     alias, so Docker DNS resolves the real remote name to it;
//   - registers on the tailnet under the app's egress.Tag (ACL source
//     identity) using the supplied ephemeral auth key, with a per-target
//     hostname so its tsnet state dir does not collide with sibling gateways;
//   - carries the gatewayForLabel marker (= app service ID) and never
//     tailswarm.enable, so Reconcile and the watcher skip it.
//
// authKey may be empty when building the spec only to hash it (the auth
// key rotates and is excluded from gatewayHash).
func gatewaySpec(eg *EgressSpec, t EgressTarget, appServiceID string, cfg Config, image, authKey string) swarm.ServiceSpec {
	hostname := gatewayHostname(eg, t)

	env := []string{
		envHeadscaleURL + "=" + cfg.Headscale.URL,
		envStateDir + "=" + cfg.Tsnet.StateDir,
		envGatewayHostname + "=" + hostname,
		envGatewayTag + "=" + eg.Tag,
		envGatewayTargets + "=" + t.addr(),
	}
	if authKey != "" {
		env = append(env, envGatewayAuthKey+"="+authKey)
	}

	return swarm.ServiceSpec{
		Annotations: swarm.Annotations{
			Name: gatewayServiceName(hostname),
			Labels: map[string]string{
				gatewayForLabel: appServiceID,
			},
		},
		TaskTemplate: swarm.TaskSpec{
			ContainerSpec: &swarm.ContainerSpec{
				Image: image,
				// Absolute path — must match the image's ENTRYPOINT
				// (/tailswarm). ContainerSpec.Command is the exec form
				// (argv[0]); the distroless image has no shell and no
				// $PATH lookup, so a bare "tailswarm" fails with
				// "executable file not found in $PATH".
				Command: []string{"/tailswarm"},
				Args:    []string{"gateway"},
				Env:     env,
			},
			Networks: []swarm.NetworkAttachmentConfig{
				{Target: eg.Network, Aliases: []string{t.Host}},
			},
		},
	}
}

// maxTailnetHostname is tailscale's hostname length cap. A label that would
// exceed it is truncated and disambiguated with a short hash of the target
// addr so sibling gateways still get distinct, stable hostnames.
const maxTailnetHostname = 63

// gatewayHostname derives a per-target gateway hostname from the app's
// egress base hostname and the target Host. Each gateway needs a distinct
// hostname because tsnet keys its state dir on hostname (sibling gateways
// sharing one hostname would clobber each other's state — the original
// duplicate-hostname collision). The target Host is already a DNS name, so
// the join is normally DNS-safe; only the length cap needs guarding.
func gatewayHostname(eg *EgressSpec, t EgressTarget) string {
	name := eg.Hostname + "-" + t.Host
	if len(name) <= maxTailnetHostname {
		return name
	}
	// Too long: keep a readable prefix and append a stable short hash of the
	// full target addr to preserve uniqueness across targets.
	sum := sha256.Sum256([]byte(t.addr()))
	suffix := "-" + hex.EncodeToString(sum[:])[:8]
	keep := maxTailnetHostname - len(suffix)
	if keep < 0 {
		keep = 0
	}
	return name[:keep] + suffix
}

// gatewayServiceName derives the Swarm service name for a gateway from its
// tailnet hostname. Hostnames already encode the stack/service, so a
// "tsgw-" (tailswarm gateway) prefix keeps gateways grep-able and distinct
// from the apps they front.
func gatewayServiceName(hostname string) string {
	return "tsgw-" + hostname
}

// encodeTargets renders an EgressTarget list as the comma-separated
// "host:port" form the egress label parser already understands, so the
// gateway entrypoint can reuse parseEgressTargets verbatim.
func encodeTargets(targets []EgressTarget) string {
	parts := make([]string, len(targets))
	for i, t := range targets {
		parts[i] = t.addr()
	}
	return strings.Join(parts, ",")
}

// gatewayHash is the stable hash over the diff-relevant subset of a
// gateway's desired spec — its identity, target set, overlay, and image.
// The auth key is excluded (it rotates every reconcile). It mirrors
// proxyHash: a matching hash means the live gateway already matches
// desired and no Docker write is needed.
func gatewayHash(eg *EgressSpec, image string) string {
	targets := make([]string, len(eg.Targets))
	for i, t := range eg.Targets {
		targets[i] = t.addr()
	}
	sort.Strings(targets)

	payload := struct {
		Hostname string
		Tag      string
		Network  string
		Targets  []string
		Image    string
	}{
		Hostname: eg.Hostname,
		Tag:      eg.Tag,
		Network:  eg.Network,
		Targets:  targets,
		Image:    image,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// errGatewayImageUnset is returned when a service opts into egress but the
// gateway image was never resolved — startup image discovery failed (see
// DiscoverGatewayImage). Surfaced as a reconcile error rather than a
// teardown so the operator sees the misconfiguration in logs.
var errGatewayImageUnset = fmt.Errorf("tailswarm: egress requested but the gateway image was not discovered (is the daemon running as a Swarm service?)")
