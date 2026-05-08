package tailswarm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func envFunc(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaultsAndEnv(t *testing.T) {
	cfg, err := Load("", envFunc(map[string]string{
		"TAILSWARM_HEADSCALE_URL":     "https://hs.example",
		"TAILSWARM_HEADSCALE_USER":    "swarm",
		"TAILSWARM_HEADSCALE_API_KEY": "secret",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Network != defaultOverlay {
		t.Errorf("Network default: got %q want %q", cfg.Network, defaultOverlay)
	}
	if cfg.Tsnet.StateDir == "" {
		t.Errorf("StateDir default missing")
	}
	if cfg.Reconcile.FullResyncInterval == 0 {
		t.Errorf("FullResyncInterval default missing")
	}
}

func TestLoadYAMLOverlayedByEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tailswarm.yml")
	body := strings.Join([]string{
		"headscale:",
		"  url: https://from-yaml",
		"  user: yaml-user",
		"  key_expiration: 1m",
		"reconcile:",
		"  full_resync_interval: 10s",
		"  rate_limit_rps: 2",
		"network: yaml-net",
		"tsnet:",
		"  state_dir: /yaml-state",
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, envFunc(map[string]string{
		"TAILSWARM_HEADSCALE_API_KEY": "k",
		"TAILSWARM_NETWORK":           "env-net",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Headscale.URL != "https://from-yaml" {
		t.Errorf("yaml url: %q", cfg.Headscale.URL)
	}
	if cfg.Network != "env-net" {
		t.Errorf("env should win: got %q", cfg.Network)
	}
	if cfg.Tsnet.StateDir != "/yaml-state" {
		t.Errorf("yaml state_dir: %q", cfg.Tsnet.StateDir)
	}
	if cfg.Headscale.KeyExpiration != time.Minute {
		t.Errorf("yaml key_expiration: %s", cfg.Headscale.KeyExpiration)
	}
}

func TestLoadValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "missing url",
			env:  map[string]string{"TAILSWARM_HEADSCALE_USER": "u", "TAILSWARM_HEADSCALE_API_KEY": "k"},
			want: "headscale.url",
		},
		{
			name: "bad url",
			env:  map[string]string{"TAILSWARM_HEADSCALE_URL": "not-a-url", "TAILSWARM_HEADSCALE_USER": "u", "TAILSWARM_HEADSCALE_API_KEY": "k"},
			want: "is not a valid absolute URL",
		},
		{
			name: "missing user",
			env:  map[string]string{"TAILSWARM_HEADSCALE_URL": "https://x", "TAILSWARM_HEADSCALE_API_KEY": "k"},
			want: "headscale.user",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load("", envFunc(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v want substring %q", err, tc.want)
			}
		})
	}
}

func TestLoadAPIKeyFile(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	if err := os.WriteFile(keyPath, []byte("secret-from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load("", envFunc(map[string]string{
		"TAILSWARM_HEADSCALE_URL":          "https://hs",
		"TAILSWARM_HEADSCALE_USER":         "u",
		"TAILSWARM_HEADSCALE_API_KEY_FILE": keyPath,
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Headscale.APIKey != "secret-from-file" {
		t.Errorf("api key: %q", cfg.Headscale.APIKey)
	}
}
