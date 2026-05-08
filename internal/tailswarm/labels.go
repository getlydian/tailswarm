// Package tailswarm contains the daemon that reconciles Docker Swarm
// services opted into a tailnet. tailswarm runs an in-process tsnet
// server per opted-in service and TCP-forwards over a shared overlay
// network — there are no per-service sidecar containers.
package tailswarm

import (
	"errors"
	"fmt"
	"strings"

	"github.com/docker/docker/api/types/swarm"
)

// stackLabel is the conventional Docker Swarm label set by `docker stack
// deploy` to identify the stack a service belongs to.
const stackLabel = "com.docker.stack.namespace"

// defaultNamespace is the label namespace used when Labels.Namespace is
// the zero value.
const defaultNamespace = "tailswarm"

// defaultOverlay is the shared overlay tailswarm and managed services
// join by default. Operators may override per-service via
// tailswarm.network.
const defaultOverlay = "tailswarm-overlay"

// Port is one TCP port pulled off a service's EndpointSpec. UDP and
// other protocols are out of scope for v1.
type Port struct {
	Target uint32
}

// Target captures everything tailswarm needs to know about a labeled
// Swarm service to bring up its tsnet proxy.
type Target struct {
	ServiceID   string
	ServiceName string
	Stack       string
	Network     string
	Hostname    string
	Tag         string
	Ports       []Port
	SpecVersion uint64
}

// Labels parses tailswarm.* deploy labels off a Swarm service.
//
// Namespace defaults to "tailswarm". Set it to e.g. "tailswarm-stage" to
// run a second tailswarm instance side-by-side reading
// "tailswarm-stage.enable" labels.
//
// AllowedTagPrefixes is the allowlist used to validate user-supplied
// tailswarm.tag overrides. The default derived tag (tag:swarm-<service>)
// is always permitted regardless of this allowlist.
//
// DefaultNetwork overrides the built-in "tailswarm-overlay" default. The
// reconciler injects the configured shared overlay name here.
type Labels struct {
	Namespace          string
	AllowedTagPrefixes []string
	DefaultNetwork     string
}

var (
	ErrUnknownNetwork = errors.New("tailswarm: tailswarm.network does not match any of the service's networks")
	ErrTagNotAllowed  = errors.New("tailswarm: tailswarm.tag is not in the allowed prefix list")
	ErrNoTCPPorts     = errors.New("tailswarm: service has no TCP ports in its endpoint spec")
)

func (l Labels) namespace() string {
	if l.Namespace == "" {
		return defaultNamespace
	}
	return l.Namespace
}

func (l Labels) defaultNetwork() string {
	if l.DefaultNetwork == "" {
		return defaultOverlay
	}
	return l.DefaultNetwork
}

func (l Labels) key(suffix string) string {
	return l.namespace() + "." + suffix
}

// Parse extracts a Target from a Swarm service's deploy labels.
//
// The second return value reports whether the service is opted in
// (tailswarm.enable=true). When false, the Target value is the zero
// value and err is nil. An error means the service IS opted in but its
// labels or ports are malformed.
//
// `networks` is the full list of swarm networks in the cluster, used
// only to validate that an explicit tailswarm.network override matches
// a network the service is actually attached to.
func (l Labels) Parse(svc swarm.Service, networks []swarm.Network) (Target, bool, error) {
	labels := svc.Spec.Labels

	enabled, hasEnable := labels[l.key("enable")]
	if !hasEnable || !isTrue(enabled) {
		return Target{}, false, nil
	}

	stack := labels[stackLabel]
	serviceName := svc.Spec.Name
	shortName := strings.TrimPrefix(serviceName, stack+"_")

	network := l.defaultNetwork()
	if override, ok := labels[l.key("network")]; ok && override != "" {
		if !serviceAttachedTo(svc, networks, override) {
			return Target{}, true, fmt.Errorf("%w: %q", ErrUnknownNetwork, override)
		}
		network = override
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

	ports := tcpPorts(svc)
	if len(ports) == 0 {
		return Target{}, true, ErrNoTCPPorts
	}

	return Target{
		ServiceID:   svc.ID,
		ServiceName: serviceName,
		Stack:       stack,
		Network:     network,
		Hostname:    hostname,
		Tag:         tag,
		Ports:       ports,
		SpecVersion: svc.Version.Index,
	}, true, nil
}

func isTrue(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1", "yes", "on":
		return true
	}
	return false
}

// serviceAttachedTo reports whether svc is attached to a network with
// the given name. Used only to validate explicit tailswarm.network
// overrides; the default shared overlay is trusted to be reachable from
// tailswarm itself, which is the only side that needs it.
func serviceAttachedTo(svc swarm.Service, networks []swarm.Network, name string) bool {
	byID := make(map[string]string, len(networks))
	for _, n := range networks {
		byID[n.ID] = n.Spec.Name
	}

	attached := svc.Spec.TaskTemplate.Networks
	if len(attached) == 0 {
		attached = svc.Spec.Networks //nolint:staticcheck // back-compat with pre-v1.44 services
	}
	for _, a := range attached {
		if a.Target == name {
			return true
		}
		if n, ok := byID[a.Target]; ok && n == name {
			return true
		}
	}
	return false
}

// tcpPorts returns every TCP TargetPort declared in the service's
// EndpointSpec, deduplicated and in declaration order.
func tcpPorts(svc swarm.Service) []Port {
	if svc.Spec.EndpointSpec == nil {
		return nil
	}
	seen := make(map[uint32]struct{}, len(svc.Spec.EndpointSpec.Ports))
	out := make([]Port, 0, len(svc.Spec.EndpointSpec.Ports))
	for _, p := range svc.Spec.EndpointSpec.Ports {
		if p.Protocol != "" && p.Protocol != swarm.PortConfigProtocolTCP {
			continue
		}
		if p.TargetPort == 0 {
			continue
		}
		if _, dup := seen[p.TargetPort]; dup {
			continue
		}
		seen[p.TargetPort] = struct{}{}
		out = append(out, Port{Target: p.TargetPort})
	}
	return out
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
