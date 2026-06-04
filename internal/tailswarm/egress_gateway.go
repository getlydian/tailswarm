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

// gatewaySpec is the pure translation from a parsed EgressSpec (plus the
// app it fronts, the daemon Config, the gateway image, and a freshly
// minted auth key) into the Swarm service spec for the egress gateway
// companion.
//
// The gateway:
//   - runs tailswarm's own image (the daemon's own, discovered at startup)
//     in "gateway" mode;
//   - joins the egress overlay carrying every target Host as a network
//     alias, so Docker DNS resolves the real remote name to the gateway;
//   - registers on the tailnet under the app's egress.Tag (ACL source
//     identity) using the supplied ephemeral auth key;
//   - carries the gatewayForLabel marker (= app service ID) and never
//     tailswarm.enable, so Reconcile and the watcher skip it.
//
// authKey may be empty when building the spec only to hash it (the auth
// key rotates every reconcile and is excluded from gatewayHash).
func gatewaySpec(eg *EgressSpec, appServiceID string, cfg Config, image, authKey string) swarm.ServiceSpec {
	aliases := make([]string, 0, len(eg.Targets))
	seen := make(map[string]struct{}, len(eg.Targets))
	for _, t := range eg.Targets {
		if _, dup := seen[t.Host]; dup {
			continue
		}
		seen[t.Host] = struct{}{}
		aliases = append(aliases, t.Host)
	}

	env := []string{
		envHeadscaleURL + "=" + cfg.Headscale.URL,
		envStateDir + "=" + cfg.Tsnet.StateDir,
		envGatewayHostname + "=" + eg.Hostname,
		envGatewayTag + "=" + eg.Tag,
		envGatewayTargets + "=" + encodeTargets(eg.Targets),
	}
	if authKey != "" {
		env = append(env, envGatewayAuthKey+"="+authKey)
	}

	return swarm.ServiceSpec{
		Annotations: swarm.Annotations{
			Name: gatewayServiceName(eg.Hostname),
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
				{Target: eg.Network, Aliases: aliases},
			},
		},
	}
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
