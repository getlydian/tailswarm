package tailswarm

import (
	"errors"
	"reflect"
	"testing"

	"github.com/docker/docker/api/types/swarm"
)

// makeService builds a swarm.Service with the given labels and networks
// (referenced by ID).
func makeService(name, stack string, labels map[string]string, networkIDs ...string) swarm.Service {
	if labels == nil {
		labels = map[string]string{}
	}
	if stack != "" {
		labels[stackLabel] = stack
	}
	attachments := make([]swarm.NetworkAttachmentConfig, 0, len(networkIDs))
	for _, id := range networkIDs {
		attachments = append(attachments, swarm.NetworkAttachmentConfig{Target: id})
	}
	return swarm.Service{
		ID: "svc-id-1",
		Meta: swarm.Meta{
			Version: swarm.Version{Index: 42},
		},
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name:   name,
				Labels: labels,
			},
			TaskTemplate: swarm.TaskSpec{
				Networks: attachments,
			},
		},
	}
}

func makeNetwork(id, name string, ingress bool) swarm.Network {
	return swarm.Network{
		ID: id,
		Spec: swarm.NetworkSpec{
			Annotations: swarm.Annotations{Name: name},
			Ingress:     ingress,
		},
		DriverState: swarm.Driver{Name: "overlay"},
	}
}

func TestParse_NotEnabled(t *testing.T) {
	t.Parallel()

	cases := map[string]map[string]string{
		"missing":    nil,
		"empty":      {"tailswarm.enable": ""},
		"false":      {"tailswarm.enable": "false"},
		"non-bool":   {"tailswarm.enable": "maybe"},
		"zero":       {"tailswarm.enable": "0"},
	}

	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			svc := makeService("foo_api", "foo", labels)
			got, enabled, err := Labels{}.Parse(svc, nil)
			if err != nil {
				t.Fatalf("expected nil err, got %v", err)
			}
			if enabled {
				t.Fatalf("expected enabled=false")
			}
			if !reflect.DeepEqual(got, Target{}) {
				t.Fatalf("expected zero Target, got %+v", got)
			}
		})
	}
}

func TestParse_SingleOverlayAutoSelect(t *testing.T) {
	t.Parallel()

	svc := makeService("foo_api", "foo", map[string]string{
		"tailswarm.enable": "true",
	}, "net-app-id")
	networks := []swarm.Network{
		makeNetwork("net-app-id", "app", false),
		makeNetwork("ingress-id", "ingress", true),
	}

	got, enabled, err := Labels{}.Parse(svc, networks)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !enabled {
		t.Fatalf("expected enabled")
	}
	if got.Network != "app" {
		t.Fatalf("expected auto-selected network 'app', got %q", got.Network)
	}
}

func TestParse_MultiOverlayRequiresLabel(t *testing.T) {
	t.Parallel()

	svc := makeService("foo_api", "foo", map[string]string{
		"tailswarm.enable": "true",
	}, "net-a", "net-b")
	networks := []swarm.Network{
		makeNetwork("net-a", "alpha", false),
		makeNetwork("net-b", "beta", false),
	}

	_, _, err := Labels{}.Parse(svc, networks)
	if !errors.Is(err, ErrAmbiguousNetwork) {
		t.Fatalf("expected ErrAmbiguousNetwork, got %v", err)
	}

	// Now disambiguate via label.
	svc.Spec.Annotations.Labels["tailswarm.network"] = "beta"
	got, enabled, err := Labels{}.Parse(svc, networks)
	if err != nil || !enabled {
		t.Fatalf("expected enabled with no error, got enabled=%v err=%v", enabled, err)
	}
	if got.Network != "beta" {
		t.Fatalf("expected network 'beta', got %q", got.Network)
	}
}

func TestParse_UnknownNetworkLabel(t *testing.T) {
	t.Parallel()

	svc := makeService("foo_api", "foo", map[string]string{
		"tailswarm.enable":  "true",
		"tailswarm.network": "does-not-exist",
	}, "net-a")
	networks := []swarm.Network{makeNetwork("net-a", "alpha", false)}

	_, _, err := Labels{}.Parse(svc, networks)
	if !errors.Is(err, ErrUnknownNetwork) {
		t.Fatalf("expected ErrUnknownNetwork, got %v", err)
	}
}

func TestParse_NoUserOverlay(t *testing.T) {
	t.Parallel()

	svc := makeService("foo_api", "foo", map[string]string{"tailswarm.enable": "true"}, "ingress-id")
	networks := []swarm.Network{makeNetwork("ingress-id", "ingress", true)}

	_, _, err := Labels{}.Parse(svc, networks)
	if !errors.Is(err, ErrNoNetwork) {
		t.Fatalf("expected ErrNoNetwork, got %v", err)
	}
}

func TestParse_HostnameDefaultAndOverride(t *testing.T) {
	t.Parallel()

	svc := makeService("foo_api", "foo", map[string]string{"tailswarm.enable": "true"}, "net-a")
	networks := []swarm.Network{makeNetwork("net-a", "app", false)}

	got, _, err := Labels{}.Parse(svc, networks)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got.Hostname != "foo-api" {
		t.Fatalf("expected default hostname 'foo-api', got %q", got.Hostname)
	}

	svc.Spec.Annotations.Labels["tailswarm.hostname"] = "billing"
	got, _, err = Labels{}.Parse(svc, networks)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got.Hostname != "billing" {
		t.Fatalf("expected override hostname 'billing', got %q", got.Hostname)
	}
}

func TestParse_HostnameDefault_NoStack(t *testing.T) {
	t.Parallel()

	// Service deployed without a stack: name has no stack prefix.
	svc := makeService("standalone", "", map[string]string{"tailswarm.enable": "true"}, "net-a")
	networks := []swarm.Network{makeNetwork("net-a", "app", false)}

	got, _, err := Labels{}.Parse(svc, networks)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got.Hostname != "standalone" {
		t.Fatalf("expected hostname 'standalone', got %q", got.Hostname)
	}
}

func TestParse_TagDefaultAndOverride(t *testing.T) {
	t.Parallel()

	networks := []swarm.Network{makeNetwork("net-a", "app", false)}

	t.Run("default derived tag", func(t *testing.T) {
		svc := makeService("foo_api", "foo", map[string]string{"tailswarm.enable": "true"}, "net-a")
		got, _, err := Labels{}.Parse(svc, networks)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if got.Tag != "tag:swarm-api" {
			t.Fatalf("expected default tag 'tag:swarm-api', got %q", got.Tag)
		}
	})

	t.Run("override allowed by prefix", func(t *testing.T) {
		svc := makeService("foo_api", "foo", map[string]string{
			"tailswarm.enable": "true",
			"tailswarm.tag":    "tag:billing",
		}, "net-a")
		l := Labels{AllowedTagPrefixes: []string{"tag:billing", "tag:internal"}}
		got, _, err := l.Parse(svc, networks)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if got.Tag != "tag:billing" {
			t.Fatalf("expected tag 'tag:billing', got %q", got.Tag)
		}
	})

	t.Run("override rejected", func(t *testing.T) {
		svc := makeService("foo_api", "foo", map[string]string{
			"tailswarm.enable": "true",
			"tailswarm.tag":    "tag:admin",
		}, "net-a")
		l := Labels{AllowedTagPrefixes: []string{"tag:billing"}}
		_, _, err := l.Parse(svc, networks)
		if !errors.Is(err, ErrTagNotAllowed) {
			t.Fatalf("expected ErrTagNotAllowed, got %v", err)
		}
	})

	t.Run("override matches derived", func(t *testing.T) {
		// Setting the override to exactly the derived value is always
		// allowed, even with an empty allowlist.
		svc := makeService("foo_api", "foo", map[string]string{
			"tailswarm.enable": "true",
			"tailswarm.tag":    "tag:swarm-api",
		}, "net-a")
		got, _, err := Labels{}.Parse(svc, networks)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if got.Tag != "tag:swarm-api" {
			t.Fatalf("got %q", got.Tag)
		}
	})
}

func TestParse_AdvertiseRoutes(t *testing.T) {
	t.Parallel()

	networks := []swarm.Network{makeNetwork("net-a", "app", false)}

	t.Run("multi CIDR with whitespace", func(t *testing.T) {
		svc := makeService("foo_api", "foo", map[string]string{
			"tailswarm.enable":           "true",
			"tailswarm.advertise-routes": "10.0.5.0/24, 192.168.1.0/24,2001:db8::/64",
		}, "net-a")
		got, _, err := Labels{}.Parse(svc, networks)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		want := []string{"10.0.5.0/24", "192.168.1.0/24", "2001:db8::/64"}
		if !reflect.DeepEqual(got.AdvertiseRoutes, want) {
			t.Fatalf("routes: got %v, want %v", got.AdvertiseRoutes, want)
		}
	})

	t.Run("invalid CIDR rejected", func(t *testing.T) {
		svc := makeService("foo_api", "foo", map[string]string{
			"tailswarm.enable":           "true",
			"tailswarm.advertise-routes": "10.0.5.0/24,not-a-cidr",
		}, "net-a")
		_, _, err := Labels{}.Parse(svc, networks)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})

	t.Run("empty label", func(t *testing.T) {
		svc := makeService("foo_api", "foo", map[string]string{
			"tailswarm.enable":           "true",
			"tailswarm.advertise-routes": "  ",
		}, "net-a")
		got, _, err := Labels{}.Parse(svc, networks)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(got.AdvertiseRoutes) != 0 {
			t.Fatalf("expected no routes, got %v", got.AdvertiseRoutes)
		}
	})
}

func TestParse_NamespaceSwitch(t *testing.T) {
	t.Parallel()

	svc := makeService("foo_api", "foo", map[string]string{
		// Default namespace is ignored.
		"tailswarm.enable":       "true",
		"tailswarm-stage.enable": "true",
		"tailswarm-stage.tag":    "tag:swarm-api",
	}, "net-a")
	networks := []swarm.Network{makeNetwork("net-a", "app", false)}

	// Default namespace sees its own enable label.
	if _, enabled, err := (Labels{}).Parse(svc, networks); err != nil || !enabled {
		t.Fatalf("default namespace: enabled=%v err=%v", enabled, err)
	}

	// Stage namespace also sees enable=true.
	got, enabled, err := Labels{Namespace: "tailswarm-stage"}.Parse(svc, networks)
	if err != nil {
		t.Fatalf("stage namespace err: %v", err)
	}
	if !enabled {
		t.Fatalf("expected stage to be enabled")
	}
	if got.Tag != "tag:swarm-api" {
		t.Fatalf("expected stage tag override, got %q", got.Tag)
	}

	// A namespace whose enable label is absent reports not enabled.
	if _, enabled, err := (Labels{Namespace: "tailswarm-prod"}).Parse(svc, networks); err != nil || enabled {
		t.Fatalf("prod namespace: enabled=%v err=%v", enabled, err)
	}
}

func TestParse_ServiceNetworksFallback(t *testing.T) {
	t.Parallel()

	// Some services use the deprecated Spec.Networks field instead of
	// Spec.TaskTemplate.Networks. Parse must handle both.
	svc := swarm.Service{
		ID:   "svc-id-1",
		Meta: swarm.Meta{Version: swarm.Version{Index: 7}},
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: "foo_api",
				Labels: map[string]string{
					"tailswarm.enable": "true",
					stackLabel:         "foo",
				},
			},
			Networks: []swarm.NetworkAttachmentConfig{
				{Target: "net-a"},
			},
		},
	}
	networks := []swarm.Network{makeNetwork("net-a", "app", false)}

	got, enabled, err := Labels{}.Parse(svc, networks)
	if err != nil || !enabled {
		t.Fatalf("enabled=%v err=%v", enabled, err)
	}
	if got.Network != "app" {
		t.Fatalf("expected 'app', got %q", got.Network)
	}
	if got.SpecVersion != 7 {
		t.Fatalf("SpecVersion: got %d, want 7", got.SpecVersion)
	}
}

func TestParse_TargetCoreFields(t *testing.T) {
	t.Parallel()

	svc := makeService("billing_api", "billing", map[string]string{
		"tailswarm.enable":           "true",
		"tailswarm.hostname":         "billing-api",
		"tailswarm.advertise-routes": "10.0.0.0/24",
	}, "net-a")
	svc.ID = "abc123"
	networks := []swarm.Network{makeNetwork("net-a", "app", false)}

	got, enabled, err := Labels{}.Parse(svc, networks)
	if err != nil || !enabled {
		t.Fatalf("enabled=%v err=%v", enabled, err)
	}

	want := Target{
		ServiceID:       "abc123",
		ServiceName:     "billing_api",
		Stack:           "billing",
		Network:         "app",
		Hostname:        "billing-api",
		Tag:             "tag:swarm-api",
		AdvertiseRoutes: []string{"10.0.0.0/24"},
		SpecVersion:     42,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Target mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}
