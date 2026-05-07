package tailswarm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// PlannerConfig is the static, deployment-wide input to Plan: things that
// come from tailswarm's own configuration rather than from the target
// service's labels.
type PlannerConfig struct {
	// Image is the fully-qualified tailscale image, e.g.
	// "tailscale/tailscale:v1.78".
	Image string

	// NetworkID is the Docker network ID for Target.Network. The planner
	// is pure and does not resolve names; the caller (reconciler) looks
	// the ID up before calling Plan.
	NetworkID string

	// HeadscaleURL is propagated to the sidecar via TS_EXTRA_ARGS as
	// --login-server. Empty omits the flag (useful for SaaS Tailscale
	// later).
	HeadscaleURL string
}

// DeviceMapping describes a host-to-container device passthrough. Mirrors
// the shape of Docker's container.DeviceMapping so the reconciler can
// translate one-to-one without further plumbing.
type DeviceMapping struct {
	PathOnHost        string
	PathInContainer   string
	CgroupPermissions string
}

// SidecarSpec is the planner's pure description of the desired sidecar
// service. The reconciler turns this into a Swarm ServiceSpec.
type SidecarSpec struct {
	Name      string
	Image     string
	NetworkID string
	Hostname  string
	Env       map[string]string
	CapAdd    []string
	Devices   []DeviceMapping
	Labels    map[string]string
	Replicas  uint64
}

// Owner labels written onto the sidecar so it can be matched back to its
// target on resync.
const (
	ownerLabelManaged = "tailswarm.managed"
	ownerLabelTarget  = "tailswarm.target-service"
	ownerLabelVersion = "tailswarm.target-version"
)

// servicePrefixLen is how much of the target service ID is folded into
// the sidecar's name to keep names collision-free across stacks.
const servicePrefixLen = 12

// Plan is the pure (Target, Config, key) → SidecarSpec translation. It
// performs no I/O and is safe to call concurrently.
func Plan(t Target, cfg PlannerConfig, key string) SidecarSpec {
	return SidecarSpec{
		Name:      sidecarName(t),
		Image:     cfg.Image,
		NetworkID: cfg.NetworkID,
		Hostname:  t.Hostname,
		Env:       buildEnv(t, cfg, key),
		CapAdd:    []string{"NET_ADMIN", "SYS_MODULE"},
		Devices: []DeviceMapping{{
			PathOnHost:        "/dev/net/tun",
			PathInContainer:   "/dev/net/tun",
			CgroupPermissions: "rwm",
		}},
		Labels: map[string]string{
			ownerLabelManaged: "true",
			ownerLabelTarget:  t.ServiceID,
			ownerLabelVersion: strconv.FormatUint(t.SpecVersion, 10),
		},
		Replicas: 1,
	}
}

func sidecarName(t Target) string {
	prefix := t.ServiceID
	if len(prefix) > servicePrefixLen {
		prefix = prefix[:servicePrefixLen]
	}
	return "tailswarm_" + prefix + "_" + t.ServiceName
}

func buildEnv(t Target, cfg PlannerConfig, key string) map[string]string {
	env := map[string]string{
		"TS_AUTHKEY":    key,
		"TS_HOSTNAME":   t.Hostname,
		"TS_EXTRA_ARGS": buildExtraArgs(t, cfg),
		"TS_STATE_DIR":  "/var/lib/tailscale",
		"TS_USERSPACE":  "false",
	}
	return env
}

func buildExtraArgs(t Target, cfg PlannerConfig) string {
	var parts []string
	if cfg.HeadscaleURL != "" {
		parts = append(parts, "--login-server="+cfg.HeadscaleURL)
	}
	if t.Tag != "" {
		parts = append(parts, "--advertise-tags="+t.Tag)
	}
	if len(t.AdvertiseRoutes) > 0 {
		parts = append(parts, "--advertise-routes="+strings.Join(t.AdvertiseRoutes, ","))
	}
	return strings.Join(parts, " ")
}

// SpecHash is a stable hash over the diff-relevant subset of a
// SidecarSpec — everything except the rotating auth key. Map iteration
// order does not affect the result.
func SpecHash(s SidecarSpec) string {
	envCopy := make(map[string]string, len(s.Env))
	for k, v := range s.Env {
		if k == "TS_AUTHKEY" {
			continue
		}
		envCopy[k] = v
	}

	payload := struct {
		Name      string
		Image     string
		NetworkID string
		Hostname  string
		Env       []kv
		CapAdd    []string
		Devices   []DeviceMapping
		Labels    []kv
		Replicas  uint64
	}{
		Name:      s.Name,
		Image:     s.Image,
		NetworkID: s.NetworkID,
		Hostname:  s.Hostname,
		Env:       sortedKV(envCopy),
		CapAdd:    sortedCopy(s.CapAdd),
		Devices:   s.Devices,
		Labels:    sortedKV(s.Labels),
		Replicas:  s.Replicas,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		// json.Marshal of strings/maps/slices of strings cannot fail.
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type kv struct{ K, V string }

func sortedKV(m map[string]string) []kv {
	out := make([]kv, 0, len(m))
	for k, v := range m {
		out = append(out, kv{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].K < out[j].K })
	return out
}

func sortedCopy(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	sort.Strings(out)
	return out
}
