package tailswarm

import (
	"reflect"
	"strings"
	"testing"
)

func sampleTarget() Target {
	return Target{
		ServiceID:       "abc123def456ghi789",
		ServiceName:     "billing_api",
		Stack:           "billing",
		Network:         "app",
		Hostname:        "billing-api",
		Tag:             "tag:billing",
		AdvertiseRoutes: []string{"10.0.0.0/24", "192.168.1.0/24"},
		SpecVersion:     42,
	}
}

func sampleConfig() PlannerConfig {
	return PlannerConfig{
		Image:        "tailscale/tailscale:v1.78",
		NetworkID:    "net-app-id",
		HeadscaleURL: "https://headscale.internal",
	}
}

func TestPlan_Golden(t *testing.T) {
	t.Parallel()

	got := Plan(sampleTarget(), sampleConfig(), "tskey-auth-XYZ")

	want := SidecarSpec{
		Name:      "tailswarm_abc123def456_billing_api",
		Image:     "tailscale/tailscale:v1.78",
		NetworkID: "net-app-id",
		Hostname:  "billing-api",
		Env: map[string]string{
			"TS_AUTHKEY":    "tskey-auth-XYZ",
			"TS_HOSTNAME":   "billing-api",
			"TS_EXTRA_ARGS": "--login-server=https://headscale.internal --advertise-tags=tag:billing --advertise-routes=10.0.0.0/24,192.168.1.0/24",
			"TS_STATE_DIR":  "/var/lib/tailscale",
			"TS_USERSPACE":  "false",
		},
		CapAdd: []string{"NET_ADMIN", "SYS_MODULE"},
		Devices: []DeviceMapping{{
			PathOnHost:        "/dev/net/tun",
			PathInContainer:   "/dev/net/tun",
			CgroupPermissions: "rwm",
		}},
		Labels: map[string]string{
			"tailswarm.managed":        "true",
			"tailswarm.target-service": "abc123def456ghi789",
			"tailswarm.target-version": "42",
		},
		Replicas: 1,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Plan mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestPlan_NameUsesIDPrefix(t *testing.T) {
	t.Parallel()

	t.Run("long ID truncated", func(t *testing.T) {
		tgt := sampleTarget()
		tgt.ServiceID = "abcdefghijklmnopqrstuvwxyz"
		tgt.ServiceName = "svc"
		got := Plan(tgt, sampleConfig(), "k")
		if got.Name != "tailswarm_abcdefghijkl_svc" {
			t.Fatalf("name: %q", got.Name)
		}
	})

	t.Run("short ID kept verbatim", func(t *testing.T) {
		tgt := sampleTarget()
		tgt.ServiceID = "abc"
		tgt.ServiceName = "svc"
		got := Plan(tgt, sampleConfig(), "k")
		if got.Name != "tailswarm_abc_svc" {
			t.Fatalf("name: %q", got.Name)
		}
	})
}

func TestPlan_ExtraArgs(t *testing.T) {
	t.Parallel()

	t.Run("no headscale url omits login-server", func(t *testing.T) {
		cfg := sampleConfig()
		cfg.HeadscaleURL = ""
		got := Plan(sampleTarget(), cfg, "k")
		if strings.Contains(got.Env["TS_EXTRA_ARGS"], "login-server") {
			t.Fatalf("unexpected login-server: %q", got.Env["TS_EXTRA_ARGS"])
		}
	})

	t.Run("no routes omits flag", func(t *testing.T) {
		tgt := sampleTarget()
		tgt.AdvertiseRoutes = nil
		got := Plan(tgt, sampleConfig(), "k")
		if strings.Contains(got.Env["TS_EXTRA_ARGS"], "advertise-routes") {
			t.Fatalf("unexpected advertise-routes: %q", got.Env["TS_EXTRA_ARGS"])
		}
		if !strings.Contains(got.Env["TS_EXTRA_ARGS"], "advertise-tags=tag:billing") {
			t.Fatalf("missing tag arg: %q", got.Env["TS_EXTRA_ARGS"])
		}
	})

	t.Run("empty tag omits flag", func(t *testing.T) {
		tgt := sampleTarget()
		tgt.Tag = ""
		got := Plan(tgt, sampleConfig(), "k")
		if strings.Contains(got.Env["TS_EXTRA_ARGS"], "advertise-tags") {
			t.Fatalf("unexpected advertise-tags: %q", got.Env["TS_EXTRA_ARGS"])
		}
	})
}

func TestSpecHash_StableAcrossKeyRotation(t *testing.T) {
	t.Parallel()

	a := Plan(sampleTarget(), sampleConfig(), "tskey-auth-AAA")
	b := Plan(sampleTarget(), sampleConfig(), "tskey-auth-BBB")

	if SpecHash(a) != SpecHash(b) {
		t.Fatalf("hash changed when only auth key rotated:\n a=%s\n b=%s",
			SpecHash(a), SpecHash(b))
	}
}

func TestSpecHash_StableAcrossMapOrder(t *testing.T) {
	t.Parallel()

	// Run many times — Go intentionally randomizes map iteration, so if
	// the hash was order-dependent at least one of these would differ.
	base := Plan(sampleTarget(), sampleConfig(), "k")
	want := SpecHash(base)
	for i := 0; i < 256; i++ {
		got := SpecHash(Plan(sampleTarget(), sampleConfig(), "k"))
		if got != want {
			t.Fatalf("hash unstable across runs: got %s, want %s", got, want)
		}
	}
}

func TestSpecHash_SpecVersionChangesHash(t *testing.T) {
	t.Parallel()

	a := Plan(sampleTarget(), sampleConfig(), "k")

	tgt := sampleTarget()
	tgt.SpecVersion = 43
	b := Plan(tgt, sampleConfig(), "k")

	if SpecHash(a) == SpecHash(b) {
		t.Fatalf("hash unchanged after SpecVersion bump: %s", SpecHash(a))
	}
}

func TestSpecHash_FieldChangesAreDetected(t *testing.T) {
	t.Parallel()

	base := Plan(sampleTarget(), sampleConfig(), "k")
	baseHash := SpecHash(base)

	cases := map[string]func(Target) Target{
		"hostname": func(t Target) Target { t.Hostname = "other"; return t },
		"tag":      func(t Target) Target { t.Tag = "tag:other"; return t },
		"routes":   func(t Target) Target { t.AdvertiseRoutes = []string{"172.16.0.0/24"}; return t },
		"name":     func(t Target) Target { t.ServiceName = "renamed"; return t },
		"id":       func(t Target) Target { t.ServiceID = "differentidvalue"; return t },
	}

	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			h := SpecHash(Plan(mut(sampleTarget()), sampleConfig(), "k"))
			if h == baseHash {
				t.Fatalf("hash unchanged after mutating %s", name)
			}
		})
	}
}

func TestSpecHash_ConfigChangesAreDetected(t *testing.T) {
	t.Parallel()

	base := Plan(sampleTarget(), sampleConfig(), "k")
	baseHash := SpecHash(base)

	cases := map[string]func(PlannerConfig) PlannerConfig{
		"image":     func(c PlannerConfig) PlannerConfig { c.Image = "tailscale/tailscale:v1.99"; return c },
		"network":   func(c PlannerConfig) PlannerConfig { c.NetworkID = "different-net-id"; return c },
		"login-url": func(c PlannerConfig) PlannerConfig { c.HeadscaleURL = "https://other"; return c },
	}

	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			h := SpecHash(Plan(sampleTarget(), mut(sampleConfig()), "k"))
			if h == baseHash {
				t.Fatalf("hash unchanged after mutating %s", name)
			}
		})
	}
}
