package tailswarm

import (
	"errors"
	"testing"

	"github.com/docker/docker/api/types/swarm"
)

func TestLabelsParseDisabled(t *testing.T) {
	l := Labels{}
	svc := swarm.Service{Spec: swarm.ServiceSpec{
		Annotations: swarm.Annotations{Labels: map[string]string{}},
	}}
	tgt, enabled, err := l.Parse(svc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if enabled {
		t.Fatalf("expected disabled, got enabled with target %+v", tgt)
	}
}

func TestLabelsParseDefaults(t *testing.T) {
	l := Labels{}
	svc := swarm.Service{
		ID: "svc1",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: "billing_api",
				Labels: map[string]string{
					stackLabel:         "billing",
					"tailswarm.enable": "true",
				},
			},
			EndpointSpec: &swarm.EndpointSpec{
				Ports: []swarm.PortConfig{
					{Protocol: swarm.PortConfigProtocolTCP, TargetPort: 8080},
				},
			},
		},
	}
	tgt, enabled, err := l.Parse(svc, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !enabled {
		t.Fatal("expected enabled")
	}
	if tgt.Hostname != "billing-api" {
		t.Errorf("hostname: got %q want billing-api", tgt.Hostname)
	}
	if tgt.Tag != "tag:swarm-api" {
		t.Errorf("tag: got %q want tag:swarm-api", tgt.Tag)
	}
	if tgt.Network != defaultOverlay {
		t.Errorf("network: got %q want %q", tgt.Network, defaultOverlay)
	}
	if len(tgt.Ports) != 1 || tgt.Ports[0].Target != 8080 {
		t.Errorf("ports: got %+v", tgt.Ports)
	}
}

func TestLabelsParseTagAllowlist(t *testing.T) {
	l := Labels{AllowedTagPrefixes: []string{"tag:billing"}}
	svc := swarm.Service{
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: "api",
				Labels: map[string]string{
					"tailswarm.enable": "true",
					"tailswarm.tag":    "tag:other",
				},
			},
			EndpointSpec: &swarm.EndpointSpec{
				Ports: []swarm.PortConfig{{Protocol: swarm.PortConfigProtocolTCP, TargetPort: 80}},
			},
		},
	}
	_, _, err := l.Parse(svc, nil)
	if !errors.Is(err, ErrTagNotAllowed) {
		t.Fatalf("got %v want ErrTagNotAllowed", err)
	}
}

func TestLabelsParseNetworkOverrideMustMatchAttachment(t *testing.T) {
	netA := swarm.Network{ID: "net-a", Spec: swarm.NetworkSpec{Annotations: swarm.Annotations{Name: "app"}}}
	svc := swarm.Service{
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: "api",
				Labels: map[string]string{
					"tailswarm.enable":  "true",
					"tailswarm.network": "missing",
				},
			},
			TaskTemplate: swarm.TaskSpec{
				Networks: []swarm.NetworkAttachmentConfig{{Target: "net-a"}},
			},
			EndpointSpec: &swarm.EndpointSpec{
				Ports: []swarm.PortConfig{{Protocol: swarm.PortConfigProtocolTCP, TargetPort: 80}},
			},
		},
	}
	_, _, err := Labels{}.Parse(svc, []swarm.Network{netA})
	if !errors.Is(err, ErrUnknownNetwork) {
		t.Fatalf("got %v want ErrUnknownNetwork", err)
	}
}

func TestLabelsParseNetworkOverrideMatchByID(t *testing.T) {
	netA := swarm.Network{ID: "net-a", Spec: swarm.NetworkSpec{Annotations: swarm.Annotations{Name: "app"}}}
	svc := swarm.Service{
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: "api",
				Labels: map[string]string{
					"tailswarm.enable":  "true",
					"tailswarm.network": "app",
				},
			},
			TaskTemplate: swarm.TaskSpec{
				Networks: []swarm.NetworkAttachmentConfig{{Target: "net-a"}},
			},
			EndpointSpec: &swarm.EndpointSpec{
				Ports: []swarm.PortConfig{{Protocol: swarm.PortConfigProtocolTCP, TargetPort: 80}},
			},
		},
	}
	tgt, _, err := Labels{}.Parse(svc, []swarm.Network{netA})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tgt.Network != "app" {
		t.Fatalf("network: got %q want app", tgt.Network)
	}
}

func TestLabelsParseNoTCPPorts(t *testing.T) {
	svc := swarm.Service{
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name:   "api",
				Labels: map[string]string{"tailswarm.enable": "true"},
			},
			EndpointSpec: &swarm.EndpointSpec{
				Ports: []swarm.PortConfig{{Protocol: "udp", TargetPort: 53}},
			},
		},
	}
	_, _, err := Labels{}.Parse(svc, nil)
	if !errors.Is(err, ErrNoTCPPorts) {
		t.Fatalf("got %v want ErrNoTCPPorts", err)
	}
}

func TestLabelsParseDedupesPorts(t *testing.T) {
	svc := swarm.Service{
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name:   "api",
				Labels: map[string]string{"tailswarm.enable": "true"},
			},
			EndpointSpec: &swarm.EndpointSpec{
				Ports: []swarm.PortConfig{
					{Protocol: swarm.PortConfigProtocolTCP, TargetPort: 80},
					{Protocol: swarm.PortConfigProtocolTCP, TargetPort: 80},
					{Protocol: swarm.PortConfigProtocolTCP, TargetPort: 443},
				},
			},
		},
	}
	tgt, _, err := Labels{}.Parse(svc, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(tgt.Ports) != 2 {
		t.Fatalf("got %d ports want 2: %+v", len(tgt.Ports), tgt.Ports)
	}
}
