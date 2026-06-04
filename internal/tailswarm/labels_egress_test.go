package tailswarm

import (
	"errors"
	"testing"

	"github.com/docker/docker/api/types/swarm"
)

// egressSvc builds a service opted into egress with the given labels
// merged over the enable label. No EndpointSpec — egress needs no ports.
func egressSvc(name string, labels map[string]string) swarm.Service {
	merged := map[string]string{"tailswarm.egress.enable": "true"}
	for k, v := range labels {
		merged[k] = v
	}
	return swarm.Service{
		ID: "svc-egress",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Name: name, Labels: merged},
		},
	}
}

func TestParseEgressOnlyDefaults(t *testing.T) {
	svc := egressSvc("admin_phpmyadmin", map[string]string{
		stackLabel:                 "admin",
		"tailswarm.egress.targets": "db-mysql:3306",
	})
	tgt, enabled, err := Labels{}.Parse(svc, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !enabled {
		t.Fatal("expected enabled")
	}
	if tgt.Ingress {
		t.Error("egress-only service should not be marked Ingress")
	}
	if tgt.Egress == nil {
		t.Fatal("Egress spec is nil")
	}
	eg := tgt.Egress
	if eg.Hostname != "admin-phpmyadmin-egress" {
		t.Errorf("egress hostname: got %q want admin-phpmyadmin-egress", eg.Hostname)
	}
	if eg.Tag != "tag:swarm-phpmyadmin" {
		t.Errorf("egress tag: got %q want tag:swarm-phpmyadmin", eg.Tag)
	}
	if eg.Network != defaultOverlay {
		t.Errorf("egress network: got %q want %q", eg.Network, defaultOverlay)
	}
	if len(eg.Targets) != 1 || eg.Targets[0] != (EgressTarget{Host: "db-mysql", Port: 3306}) {
		t.Errorf("targets: got %+v", eg.Targets)
	}
}

func TestParseEgressMultipleTargetsAndWhitespace(t *testing.T) {
	svc := egressSvc("admin_phpmyadmin", map[string]string{
		"tailswarm.egress.targets": " db-mysql:3306, analytics-mysql:3306 , ",
	})
	tgt, _, err := Labels{}.Parse(svc, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []EgressTarget{
		{Host: "db-mysql", Port: 3306},
		{Host: "analytics-mysql", Port: 3306},
	}
	if len(tgt.Egress.Targets) != len(want) {
		t.Fatalf("targets: got %+v want %+v", tgt.Egress.Targets, want)
	}
	for i, w := range want {
		if tgt.Egress.Targets[i] != w {
			t.Errorf("target %d: got %+v want %+v", i, tgt.Egress.Targets[i], w)
		}
	}
}

func TestParseEgressDedupes(t *testing.T) {
	svc := egressSvc("admin_phpmyadmin", map[string]string{
		"tailswarm.egress.targets": "db-mysql:3306, db-mysql:3306",
	})
	tgt, _, err := Labels{}.Parse(svc, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(tgt.Egress.Targets) != 1 {
		t.Fatalf("got %d targets want 1: %+v", len(tgt.Egress.Targets), tgt.Egress.Targets)
	}
}

func TestParseEgressNoTargets(t *testing.T) {
	svc := egressSvc("admin_phpmyadmin", map[string]string{
		"tailswarm.egress.targets": "  ",
	})
	_, _, err := Labels{}.Parse(svc, nil)
	if !errors.Is(err, ErrNoEgressTargets) {
		t.Fatalf("got %v want ErrNoEgressTargets", err)
	}
}

func TestParseEgressMissingTargetsLabel(t *testing.T) {
	svc := egressSvc("admin_phpmyadmin", nil)
	_, _, err := Labels{}.Parse(svc, nil)
	if !errors.Is(err, ErrNoEgressTargets) {
		t.Fatalf("got %v want ErrNoEgressTargets", err)
	}
}

func TestParseEgressBadTarget(t *testing.T) {
	cases := []string{
		"db-mysql",          // no port
		"db-mysql:",         // empty port
		":3306",             // empty host
		"db-mysql:notaport", // non-numeric
		"db-mysql:0",        // zero port
		"db-mysql:99999",    // out of uint16 range
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			svc := egressSvc("admin_phpmyadmin", map[string]string{
				"tailswarm.egress.targets": raw,
			})
			_, _, err := Labels{}.Parse(svc, nil)
			if !errors.Is(err, ErrBadEgressTarget) {
				t.Fatalf("got %v want ErrBadEgressTarget", err)
			}
		})
	}
}

func TestParseEgressTagAllowlist(t *testing.T) {
	svc := egressSvc("admin_phpmyadmin", map[string]string{
		"tailswarm.egress.targets": "db-mysql:3306",
		"tailswarm.egress.tag":     "tag:other",
	})
	_, _, err := Labels{}.Parse(svc, nil)
	if !errors.Is(err, ErrEgressTagNotAllow) {
		t.Fatalf("got %v want ErrEgressTagNotAllow", err)
	}

	// Within the allowlist it parses.
	svc.Spec.Labels["tailswarm.egress.tag"] = "tag:svc-phpmyadmin-admin"
	l := Labels{AllowedTags: []string{"tag:svc-*-admin"}}
	tgt, _, err := l.Parse(svc, nil)
	if err != nil {
		t.Fatalf("parse with allowed tag: %v", err)
	}
	if tgt.Egress.Tag != "tag:svc-phpmyadmin-admin" {
		t.Errorf("egress tag: got %q", tgt.Egress.Tag)
	}
}

func TestParseIngressAndEgressTogether(t *testing.T) {
	svc := swarm.Service{
		ID: "svc-both",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: "ops_thanos",
				Labels: map[string]string{
					stackLabel:                 "ops",
					"tailswarm.enable":         "true",
					"tailswarm.egress.enable":  "true",
					"tailswarm.egress.targets": "store-lydian:10901",
				},
			},
			EndpointSpec: &swarm.EndpointSpec{
				Ports: []swarm.PortConfig{{Protocol: swarm.PortConfigProtocolTCP, TargetPort: 9090}},
			},
		},
	}
	tgt, enabled, err := Labels{}.Parse(svc, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !enabled {
		t.Fatal("expected enabled")
	}
	if !tgt.Ingress {
		t.Error("expected Ingress true")
	}
	if len(tgt.Ports) != 1 || tgt.Ports[0].Target != 9090 {
		t.Errorf("ingress ports: got %+v", tgt.Ports)
	}
	if tgt.Egress == nil || len(tgt.Egress.Targets) != 1 {
		t.Fatalf("egress: got %+v", tgt.Egress)
	}
	// Ingress and egress hostnames must differ so they are distinct nodes.
	if tgt.Hostname == tgt.Egress.Hostname {
		t.Errorf("ingress and egress hostnames collide: %q", tgt.Hostname)
	}
}

func TestParseEgressOnlyNeedsNoPorts(t *testing.T) {
	// An egress-only service with no EndpointSpec must NOT trip
	// ErrNoTCPPorts — ports are an ingress concern only.
	svc := egressSvc("admin_phpmyadmin", map[string]string{
		"tailswarm.egress.targets": "db-mysql:3306",
	})
	if _, _, err := (Labels{}).Parse(svc, nil); err != nil {
		t.Fatalf("egress-only parse should succeed, got %v", err)
	}
}
