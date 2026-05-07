package tailswarm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// nilEnv is a stand-in env lookup for tests that explicitly want nothing
// in the environment to leak into the config under test.
func nilEnv(string) string { return "" }

// envMap turns a map into the env-lookup signature Load expects.
func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestLoad_YAMLRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	keyPath := writeFile(t, dir, "api.key", "secret-key-123\n")
	cfgPath := writeFile(t, dir, "tailswarm.yml", `
headscale:
  url: https://headscale.internal
  api_key_file: `+keyPath+`
  user: swarm
  key_expiration: 2m
sidecar:
  image: tailscale/tailscale:v1.78
reconcile:
  full_resync_interval: 45s
  rate_limit_rps: 7
label_namespace: tailswarm
allowed_tag_prefixes:
  - tag:swarm-
  - tag:billing-
`)

	cfg, err := Load(cfgPath, nilEnv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Headscale.URL != "https://headscale.internal" {
		t.Errorf("URL = %q", cfg.Headscale.URL)
	}
	if cfg.Headscale.APIKey != "secret-key-123" {
		t.Errorf("APIKey = %q (api_key_file should be read and trimmed)", cfg.Headscale.APIKey)
	}
	if cfg.Headscale.User != "swarm" {
		t.Errorf("User = %q", cfg.Headscale.User)
	}
	if cfg.Headscale.KeyExpiration != 2*time.Minute {
		t.Errorf("KeyExpiration = %s", cfg.Headscale.KeyExpiration)
	}
	if cfg.Sidecar.Image != "tailscale/tailscale:v1.78" {
		t.Errorf("Image = %q", cfg.Sidecar.Image)
	}
	if cfg.Reconcile.FullResyncInterval != 45*time.Second {
		t.Errorf("FullResyncInterval = %s", cfg.Reconcile.FullResyncInterval)
	}
	if cfg.Reconcile.RateLimitRPS != 7 {
		t.Errorf("RateLimitRPS = %v", cfg.Reconcile.RateLimitRPS)
	}
	if cfg.LabelNamespace != "tailswarm" {
		t.Errorf("LabelNamespace = %q", cfg.LabelNamespace)
	}
	if got, want := cfg.AllowedTagPrefixes, []string{"tag:swarm-", "tag:billing-"}; !equalStrings(got, want) {
		t.Errorf("AllowedTagPrefixes = %v, want %v", got, want)
	}
}

func TestLoad_DefaultsApply(t *testing.T) {
	t.Parallel()

	// Minimal YAML — everything else should fall back to defaults.
	dir := t.TempDir()
	cfgPath := writeFile(t, dir, "minimal.yml", `
headscale:
  url: https://hs.example
  user: swarm
sidecar:
  image: tailscale/tailscale:v1.78
`)

	cfg, err := Load(cfgPath, nilEnv)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Headscale.KeyExpiration != 5*time.Minute {
		t.Errorf("default KeyExpiration = %s, want 5m", cfg.Headscale.KeyExpiration)
	}
	if cfg.Reconcile.FullResyncInterval != 30*time.Second {
		t.Errorf("default FullResyncInterval = %s, want 30s", cfg.Reconcile.FullResyncInterval)
	}
	if cfg.Reconcile.RateLimitRPS != 5 {
		t.Errorf("default RateLimitRPS = %v, want 5", cfg.Reconcile.RateLimitRPS)
	}
	if cfg.LabelNamespace != "tailswarm" {
		t.Errorf("default LabelNamespace = %q", cfg.LabelNamespace)
	}
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfgPath := writeFile(t, dir, "tailswarm.yml", `
headscale:
  url: https://headscale-from-yaml
  user: swarm
  key_expiration: 5m
sidecar:
  image: tailscale/tailscale:v1.78
reconcile:
  full_resync_interval: 30s
  rate_limit_rps: 5
`)

	env := envMap(map[string]string{
		"TAILSWARM_HEADSCALE_URL":                  "https://headscale-from-env",
		"TAILSWARM_HEADSCALE_API_KEY":              "env-key",
		"TAILSWARM_HEADSCALE_USER":                 "envuser",
		"TAILSWARM_HEADSCALE_KEY_EXPIRATION":       "90s",
		"TAILSWARM_SIDECAR_IMAGE":                  "tailscale/tailscale:v1.80",
		"TAILSWARM_RECONCILE_FULL_RESYNC_INTERVAL": "1m",
		"TAILSWARM_RECONCILE_RATE_LIMIT_RPS":       "10",
		"TAILSWARM_LABEL_NAMESPACE":                "tailswarm-stage",
		"TAILSWARM_ALLOWED_TAG_PREFIXES":           "tag:swarm-, tag:edge-",
	})

	cfg, err := Load(cfgPath, env)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Headscale.URL != "https://headscale-from-env" {
		t.Errorf("env should override URL, got %q", cfg.Headscale.URL)
	}
	if cfg.Headscale.APIKey != "env-key" {
		t.Errorf("APIKey from env = %q", cfg.Headscale.APIKey)
	}
	if cfg.Headscale.User != "envuser" {
		t.Errorf("User from env = %q", cfg.Headscale.User)
	}
	if cfg.Headscale.KeyExpiration != 90*time.Second {
		t.Errorf("KeyExpiration from env = %s", cfg.Headscale.KeyExpiration)
	}
	if cfg.Sidecar.Image != "tailscale/tailscale:v1.80" {
		t.Errorf("Image from env = %q", cfg.Sidecar.Image)
	}
	if cfg.Reconcile.FullResyncInterval != time.Minute {
		t.Errorf("FullResyncInterval from env = %s", cfg.Reconcile.FullResyncInterval)
	}
	if cfg.Reconcile.RateLimitRPS != 10 {
		t.Errorf("RateLimitRPS from env = %v", cfg.Reconcile.RateLimitRPS)
	}
	if cfg.LabelNamespace != "tailswarm-stage" {
		t.Errorf("LabelNamespace from env = %q", cfg.LabelNamespace)
	}
	if got, want := cfg.AllowedTagPrefixes, []string{"tag:swarm-", "tag:edge-"}; !equalStrings(got, want) {
		t.Errorf("AllowedTagPrefixes from env = %v, want %v", got, want)
	}
}

func TestLoad_APIKeyEnvBeatsFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	keyPath := writeFile(t, dir, "api.key", "from-file")
	cfgPath := writeFile(t, dir, "c.yml", `
headscale:
  url: https://hs.example
  api_key_file: `+keyPath+`
  user: swarm
sidecar:
  image: tailscale/tailscale:v1.78
`)

	env := envMap(map[string]string{
		"TAILSWARM_HEADSCALE_API_KEY": "from-env",
	})
	cfg, err := Load(cfgPath, env)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Headscale.APIKey != "from-env" {
		t.Errorf("env should win over api_key_file, got %q", cfg.Headscale.APIKey)
	}
}

func TestLoad_NoYAMLFile(t *testing.T) {
	t.Parallel()

	env := envMap(map[string]string{
		"TAILSWARM_HEADSCALE_URL":  "https://hs.example",
		"TAILSWARM_HEADSCALE_USER": "swarm",
		"TAILSWARM_SIDECAR_IMAGE":  "tailscale/tailscale:v1.78",
	})
	cfg, err := Load("", env)
	if err != nil {
		t.Fatalf("Load with empty path: %v", err)
	}
	if cfg.Headscale.URL != "https://hs.example" {
		t.Errorf("URL = %q", cfg.Headscale.URL)
	}
	// Defaults still apply.
	if cfg.Reconcile.FullResyncInterval != 30*time.Second {
		t.Errorf("default FullResyncInterval = %s", cfg.Reconcile.FullResyncInterval)
	}
}

func TestLoad_ValidationErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		yaml    string
		wantSub string
	}{
		{
			name: "missing url",
			yaml: `
headscale:
  user: swarm
sidecar:
  image: tailscale/tailscale:v1.78
`,
			wantSub: "headscale.url is required",
		},
		{
			name: "bad url",
			yaml: `
headscale:
  url: "not a url"
  user: swarm
sidecar:
  image: tailscale/tailscale:v1.78
`,
			wantSub: "not a valid absolute URL",
		},
		{
			name: "missing user",
			yaml: `
headscale:
  url: https://hs.example
sidecar:
  image: tailscale/tailscale:v1.78
`,
			wantSub: "headscale.user is required",
		},
		{
			name: "missing image",
			yaml: `
headscale:
  url: https://hs.example
  user: swarm
`,
			wantSub: "sidecar.image is required",
		},
		{
			name: "non-positive interval",
			yaml: `
headscale:
  url: https://hs.example
  user: swarm
sidecar:
  image: tailscale/tailscale:v1.78
reconcile:
  full_resync_interval: -1s
`,
			wantSub: "full_resync_interval must be positive",
		},
		{
			name: "bad label namespace",
			yaml: `
headscale:
  url: https://hs.example
  user: swarm
sidecar:
  image: tailscale/tailscale:v1.78
label_namespace: "Not Valid"
`,
			wantSub: "label_namespace",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := writeFile(t, dir, "c.yml", tc.yaml)
			_, err := Load(path, nilEnv)
			if err == nil {
				t.Fatalf("Load: expected error containing %q, got nil", tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("Load error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestLoad_RejectsUnknownYAMLFields(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := writeFile(t, dir, "c.yml", `
headscale:
  url: https://hs.example
  user: swarm
sidecar:
  image: tailscale/tailscale:v1.78
totally_unknown_field: 42
`)
	_, err := Load(path, nilEnv)
	if err == nil {
		t.Fatalf("expected error for unknown YAML field")
	}
}

func TestLoad_BadDurationEnv(t *testing.T) {
	t.Parallel()

	env := envMap(map[string]string{
		"TAILSWARM_HEADSCALE_URL":            "https://hs.example",
		"TAILSWARM_HEADSCALE_USER":           "swarm",
		"TAILSWARM_SIDECAR_IMAGE":            "tailscale/tailscale:v1.78",
		"TAILSWARM_HEADSCALE_KEY_EXPIRATION": "not-a-duration",
	})
	_, err := Load("", env)
	if err == nil || !strings.Contains(err.Error(), "TAILSWARM_HEADSCALE_KEY_EXPIRATION") {
		t.Fatalf("expected duration parse error, got %v", err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
