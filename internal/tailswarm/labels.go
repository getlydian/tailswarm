// Package tailswarm contains the daemon that reconciles Docker Swarm
// services opted into a tailnet. tailswarm runs an in-process tsnet
// server per opted-in service and TCP-forwards over a shared overlay
// network — there are no per-service sidecar containers.
package tailswarm

import (
	"errors"
	"fmt"
	"strconv"
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

// gatewayForLabel marks an egress gateway service tailswarm itself
// created, recording the app service ID it fronts. It is intentionally
// NOT namespaced under the configurable label prefix and never carries
// "<namespace>.enable", so the watcher's full-list filter and the
// reconciler skip it rather than treating it as an opted-in app.
const gatewayForLabel = "tailswarm.gateway.for"

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

	// Ingress reports whether the service opted into inbound proxying
	// (tailswarm.enable=true). When false, Hostname/Tag/Ports are not
	// populated for ingress and no ingress Proxy is started.
	Ingress bool

	// Egress is populated when the service opted into outbound proxying
	// (tailswarm.egress.enable=true). When nil, the service does not
	// egress. A service may be ingress-only, egress-only, or both.
	Egress *EgressSpec
}

// EgressSpec is the parsed egress label set for a service. Hostname is
// the gateway's tailnet node name; Tag is its tailnet identity (ACL
// source); Targets is the set of MagicDNS destinations it dials out to.
type EgressSpec struct {
	Hostname string
	Tag      string
	Network  string
	Targets  []EgressTarget
}

// Labels parses tailswarm.* deploy labels off a Swarm service.
//
// Namespace defaults to "tailswarm". Set it to e.g. "tailswarm-stage" to
// run a second tailswarm instance side-by-side reading
// "tailswarm-stage.enable" labels.
//
// AllowedTags is the allowlist used to validate user-supplied
// tailswarm.tag overrides. Each entry is a whole-tag glob: `*` matches
// any run of characters, anchored at both ends. The default derived tag
// (tag:swarm-<service>) is always permitted regardless of this
// allowlist.
//
// DefaultNetwork overrides the built-in "tailswarm-overlay" default. The
// reconciler injects the configured shared overlay name here.
type Labels struct {
	Namespace      string
	AllowedTags    []string
	DefaultNetwork string
}

var (
	ErrUnknownNetwork    = errors.New("tailswarm: tailswarm.network does not match any of the service's networks")
	ErrTagNotAllowed     = errors.New("tailswarm: tailswarm.tag does not match any allowed_tags pattern")
	ErrNoTCPPorts        = errors.New("tailswarm: service has no TCP ports in its endpoint spec")
	ErrNoEgressTargets   = errors.New("tailswarm: tailswarm.egress.enable is set but tailswarm.egress.targets is empty")
	ErrBadEgressTarget   = errors.New("tailswarm: tailswarm.egress.targets entry is not host:port")
	ErrEgressTagNotAllow = errors.New("tailswarm: tailswarm.egress.tag does not match any allowed_tags pattern")
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
// The second return value reports whether the service is opted in to
// anything — ingress (tailswarm.enable=true), egress
// (tailswarm.egress.enable=true), or both. When false, the Target value
// is the zero value and err is nil. An error means the service IS opted
// in but its labels or ports are malformed.
//
// Ingress and egress are independent: an egress-only service needs no
// EndpointSpec.Ports, and an ingress-only service needs no egress labels.
//
// `networks` is the full list of swarm networks in the cluster, used
// only to validate that an explicit tailswarm.network override matches
// a network the service is actually attached to.
func (l Labels) Parse(svc swarm.Service, networks []swarm.Network) (Target, bool, error) {
	labels := svc.Spec.Labels

	ingressOn := isTrue(labels[l.key("enable")])
	egressOn := isTrue(labels[l.key("egress.enable")])
	if !ingressOn && !egressOn {
		return Target{}, false, nil
	}

	stack := labels[stackLabel]
	serviceName := svc.Spec.Name
	shortName := strings.TrimPrefix(serviceName, stack+"_")
	derivedTag := "tag:swarm-" + shortName

	tgt := Target{
		ServiceID:   svc.ID,
		ServiceName: serviceName,
		Stack:       stack,
		SpecVersion: svc.Version.Index,
	}

	if ingressOn {
		if err := l.parseIngress(&tgt, svc, networks, shortName, derivedTag); err != nil {
			return Target{}, true, err
		}
	}

	if egressOn {
		eg, err := l.parseEgress(svc, networks, stack, shortName, derivedTag)
		if err != nil {
			return Target{}, true, err
		}
		tgt.Egress = eg
	}

	return tgt, true, nil
}

// parseIngress fills the inbound-proxy fields on tgt from the
// tailswarm.* labels. Mutates tgt in place; returns an error if the
// service opted into ingress but its network/tag/ports are malformed.
func (l Labels) parseIngress(tgt *Target, svc swarm.Service, networks []swarm.Network, shortName, derivedTag string) error {
	labels := svc.Spec.Labels

	network := l.defaultNetwork()
	if override, ok := labels[l.key("network")]; ok && override != "" {
		if !serviceAttachedTo(svc, networks, override) {
			return fmt.Errorf("%w: %q", ErrUnknownNetwork, override)
		}
		network = override
	}

	hostname := labels[l.key("hostname")]
	if hostname == "" {
		if tgt.Stack != "" {
			hostname = tgt.Stack + "-" + shortName
		} else {
			hostname = shortName
		}
	}

	tag := derivedTag
	if override, ok := labels[l.key("tag")]; ok && override != "" {
		if !tagAllowed(override, derivedTag, l.AllowedTags) {
			return fmt.Errorf("%w: %q", ErrTagNotAllowed, override)
		}
		tag = override
	}

	ports := tcpPorts(svc)
	if len(ports) == 0 {
		return ErrNoTCPPorts
	}

	tgt.Ingress = true
	tgt.Network = network
	tgt.Hostname = hostname
	tgt.Tag = tag
	tgt.Ports = ports
	return nil
}

// parseEgress builds the EgressSpec from the tailswarm.egress.* labels.
// The gateway hostname defaults to "<ingress-hostname>-egress" so it is
// distinct from the ingress node; the tag defaults to the derived tag and
// may be overridden within allowed_tags. Targets is a comma-separated
// host:port list with no local-port suffix — the app dials the real name.
func (l Labels) parseEgress(svc swarm.Service, networks []swarm.Network, stack, shortName, derivedTag string) (*EgressSpec, error) {
	labels := svc.Spec.Labels

	targets, err := parseEgressTargets(labels[l.key("egress.targets")])
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, ErrNoEgressTargets
	}

	network := l.defaultNetwork()
	if override, ok := labels[l.key("egress.network")]; ok && override != "" {
		if !serviceAttachedTo(svc, networks, override) {
			return nil, fmt.Errorf("%w: %q", ErrUnknownNetwork, override)
		}
		network = override
	}

	hostname := labels[l.key("egress.hostname")]
	if hostname == "" {
		base := shortName
		if stack != "" {
			base = stack + "-" + shortName
		}
		hostname = base + "-egress"
	}

	tag := derivedTag
	if override, ok := labels[l.key("egress.tag")]; ok && override != "" {
		if !tagAllowed(override, derivedTag, l.AllowedTags) {
			return nil, fmt.Errorf("%w: %q", ErrEgressTagNotAllow, override)
		}
		tag = override
	}

	return &EgressSpec{
		Hostname: hostname,
		Tag:      tag,
		Network:  network,
		Targets:  targets,
	}, nil
}

// parseEgressTargets parses a comma-separated "host:port, host:port" list
// into EgressTargets, deduplicated and in declaration order. Blank
// entries (e.g. trailing commas) are skipped; a malformed entry is an
// error.
func parseEgressTargets(raw string) ([]EgressTarget, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	seen := make(map[string]struct{})
	out := make([]EgressTarget, 0)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		host, portStr, ok := strings.Cut(entry, ":")
		host = strings.TrimSpace(host)
		portStr = strings.TrimSpace(portStr)
		if !ok || host == "" || portStr == "" {
			return nil, fmt.Errorf("%w: %q", ErrBadEgressTarget, entry)
		}
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil || port == 0 {
			return nil, fmt.Errorf("%w: %q", ErrBadEgressTarget, entry)
		}
		if _, dup := seen[entry]; dup {
			continue
		}
		seen[entry] = struct{}{}
		out = append(out, EgressTarget{Host: host, Port: uint32(port)})
	}
	return out, nil
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

func tagAllowed(tag, derived string, allowedTags []string) bool {
	if tag == derived {
		return true
	}
	for _, p := range allowedTags {
		if matchGlob(p, tag) {
			return true
		}
	}
	return false
}

// matchGlob reports whether s matches pattern. The only metacharacter is
// `*`, which matches any (possibly empty) run of characters. The match is
// anchored at both ends, so the pattern must consume the whole string.
func matchGlob(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == s
	}
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(s, mid)
		if i < 0 {
			return false
		}
		s = s[i+len(mid):]
	}
	return strings.HasSuffix(s, last)
}
