// Package tailswarm contains the daemon that reconciles Docker Swarm
// services opted into a tailnet against a Headscale controller.
package tailswarm

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/docker/docker/api/types/swarm"
)

// stackLabel is the conventional Docker Swarm label set by `docker stack
// deploy` to identify the stack a service belongs to.
const stackLabel = "com.docker.stack.namespace"

// defaultNamespace is the label namespace used when Labels.Namespace is
// the zero value.
const defaultNamespace = "tailswarm"

// Target captures everything tailswarm needs to know about a labeled
// Swarm service to plan its sidecar.
type Target struct {
	ServiceID       string
	ServiceName     string
	Stack           string
	Network         string
	Hostname        string
	Tag             string
	AdvertiseRoutes []string
	SpecVersion     uint64
}

// Labels parses tailswarm.* deploy labels off a Swarm service.
//
// Namespace defaults to "tailswarm". Set it to e.g. "tailswarm-stage" to
// run a second tailswarm instance side-by-side reading
// "tailswarm-stage.enable" labels.
//
// AllowedTagPrefixes is the allowlist used to validate user-supplied
// tailswarm.tag overrides. A user-supplied tag must start with one of
// these prefixes (after the "tag:" prefix is stripped). The default
// derived tag (tag:swarm-<service>) is always permitted regardless of
// this allowlist.
type Labels struct {
	Namespace          string
	AllowedTagPrefixes []string
}

// ErrAmbiguousNetwork is returned when a service is attached to more
// than one user overlay and tailswarm.network is not set.
var ErrAmbiguousNetwork = errors.New("tailswarm: service is attached to multiple overlays; tailswarm.network is required")

// ErrUnknownNetwork is returned when tailswarm.network references a
// network the service is not attached to.
var ErrUnknownNetwork = errors.New("tailswarm: tailswarm.network does not match any of the service's networks")

// ErrNoNetwork is returned when a service has no user overlay attached.
var ErrNoNetwork = errors.New("tailswarm: service is not attached to any user overlay")

// ErrTagNotAllowed is returned when tailswarm.tag is set to a value
// outside the configured allowlist.
var ErrTagNotAllowed = errors.New("tailswarm: tailswarm.tag is not in the allowed prefix list")

func (l Labels) namespace() string {
	if l.Namespace == "" {
		return defaultNamespace
	}
	return l.Namespace
}

func (l Labels) key(suffix string) string {
	return l.namespace() + "." + suffix
}

// Parse extracts a Target from a Swarm service's deploy labels.
//
// The second return value reports whether the service is opted in
// (tailswarm.enable=true). When false, the Target value is the zero
// value and err is nil. An error means the service IS opted in but its
// labels are malformed.
//
// `networks` should be the full list of swarm networks in the cluster,
// used to map network IDs (which is what Swarm puts on the service) to
// human-readable names (which is what tailswarm.network references).
func (l Labels) Parse(svc swarm.Service, networks []swarm.Network) (Target, bool, error) {
	labels := svc.Spec.Annotations.Labels

	enabled, hasEnable := labels[l.key("enable")]
	if !hasEnable || !isTrue(enabled) {
		return Target{}, false, nil
	}

	stack := labels[stackLabel]
	serviceName := svc.Spec.Annotations.Name
	shortName := strings.TrimPrefix(serviceName, stack+"_")

	overlays := userOverlays(svc, networks)
	network, err := selectNetwork(labels[l.key("network")], overlays)
	if err != nil {
		return Target{}, true, err
	}

	hostname := labels[l.key("hostname")]
	if hostname == "" {
		if stack != "" {
			hostname = stack + "-" + shortName
		} else {
			hostname = shortName
		}
	}

	derivedTag := "tag:swarm-" + shortName
	tag := derivedTag
	if override, ok := labels[l.key("tag")]; ok && override != "" {
		if !tagAllowed(override, derivedTag, l.AllowedTagPrefixes) {
			return Target{}, true, fmt.Errorf("%w: %q", ErrTagNotAllowed, override)
		}
		tag = override
	}

	routes, err := parseRoutes(labels[l.key("advertise-routes")])
	if err != nil {
		return Target{}, true, err
	}

	return Target{
		ServiceID:       svc.ID,
		ServiceName:     serviceName,
		Stack:           stack,
		Network:         network,
		Hostname:        hostname,
		Tag:             tag,
		AdvertiseRoutes: routes,
		SpecVersion:     svc.Version.Index,
	}, true, nil
}

func isTrue(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1", "yes", "on":
		return true
	}
	return false
}

// userOverlays returns the names of overlay networks the service is
// attached to, excluding swarm-managed networks like "ingress".
func userOverlays(svc swarm.Service, networks []swarm.Network) []string {
	byID := make(map[string]swarm.Network, len(networks))
	byName := make(map[string]swarm.Network, len(networks))
	for _, n := range networks {
		byID[n.ID] = n
		byName[n.Spec.Annotations.Name] = n
	}

	attached := svc.Spec.TaskTemplate.Networks
	if len(attached) == 0 {
		attached = svc.Spec.Networks
	}

	seen := make(map[string]struct{}, len(attached))
	var out []string
	for _, a := range attached {
		n, ok := byID[a.Target]
		if !ok {
			n, ok = byName[a.Target]
		}
		if !ok {
			continue
		}
		if n.Spec.Ingress {
			continue
		}
		if n.DriverState.Name != "" && n.DriverState.Name != "overlay" {
			continue
		}
		name := n.Spec.Annotations.Name
		if name == "" {
			name = a.Target
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func selectNetwork(requested string, overlays []string) (string, error) {
	if requested != "" {
		for _, n := range overlays {
			if n == requested {
				return requested, nil
			}
		}
		return "", fmt.Errorf("%w: %q", ErrUnknownNetwork, requested)
	}
	switch len(overlays) {
	case 0:
		return "", ErrNoNetwork
	case 1:
		return overlays[0], nil
	default:
		return "", ErrAmbiguousNetwork
	}
}

func tagAllowed(tag, derived string, allowedPrefixes []string) bool {
	if tag == derived {
		return true
	}
	for _, p := range allowedPrefixes {
		if strings.HasPrefix(tag, p) {
			return true
		}
	}
	return false
}

func parseRoutes(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, err := netip.ParsePrefix(p); err != nil {
			return nil, fmt.Errorf("tailswarm: invalid CIDR in advertise-routes %q: %w", p, err)
		}
		out = append(out, p)
	}
	return out, nil
}
