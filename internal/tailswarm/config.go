package tailswarm

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the merged YAML + env configuration for tailswarm.
type Config struct {
	Headscale          HeadscaleConfig `yaml:"headscale"`
	Tsnet              TsnetConfig     `yaml:"tsnet"`
	Reconcile          ReconcileConfig `yaml:"reconcile"`
	LabelNamespace string          `yaml:"label_namespace"`
	AllowedTags    []string        `yaml:"allowed_tags"`

	// Network is the shared overlay tailswarm and managed services join.
	// Defaults to "tailswarm-overlay". Per-service tailswarm.network
	// labels override this for the edge case where a service can't be
	// moved onto the shared overlay.
	Network string `yaml:"network"`
}

// HeadscaleConfig groups the controller-side knobs.
type HeadscaleConfig struct {
	URL           string        `yaml:"url"`
	APIKey        string        `yaml:"-"`
	APIKeyFile    string        `yaml:"api_key_file"`
	User          string        `yaml:"user"`
	KeyExpiration time.Duration `yaml:"key_expiration"`
}

// TsnetConfig groups in-process tailnet knobs.
type TsnetConfig struct {
	// StateDir is the on-disk root under which each tsnet server
	// persists its node identity (one subdirectory per hostname).
	// Should map to a named volume so identities survive restarts.
	StateDir string `yaml:"state_dir"`
}

type ReconcileConfig struct {
	FullResyncInterval time.Duration `yaml:"full_resync_interval"`
	RateLimitRPS       float64       `yaml:"rate_limit_rps"`
}

var labelNamespacePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// Load reads tailswarm configuration from a YAML file and overlays
// environment variables. env wins over YAML.
func Load(path string, env func(string) string) (Config, error) {
	if env == nil {
		env = os.Getenv
	}

	cfg := defaultConfig()

	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config %s: %w", path, err)
		}
		var fromFile Config
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true)
		if err := dec.Decode(&fromFile); err != nil {
			return Config{}, fmt.Errorf("parse config %s: %w", path, err)
		}
		cfg = mergeConfig(cfg, fromFile)
	}

	if err := applyEnv(&cfg, env); err != nil {
		return Config{}, err
	}

	if cfg.Headscale.APIKey == "" && cfg.Headscale.APIKeyFile != "" {
		key, err := os.ReadFile(cfg.Headscale.APIKeyFile)
		if err != nil {
			return Config{}, fmt.Errorf("read headscale api_key_file %s: %w", cfg.Headscale.APIKeyFile, err)
		}
		cfg.Headscale.APIKey = strings.TrimSpace(string(key))
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		Headscale: HeadscaleConfig{
			KeyExpiration: 5 * time.Minute,
		},
		Tsnet: TsnetConfig{
			StateDir: "/var/lib/tailswarm",
		},
		Reconcile: ReconcileConfig{
			FullResyncInterval: 30 * time.Second,
			RateLimitRPS:       5,
		},
		LabelNamespace: defaultNamespace,
		Network:        defaultOverlay,
	}
}

func mergeConfig(dst, src Config) Config {
	if src.Headscale.URL != "" {
		dst.Headscale.URL = src.Headscale.URL
	}
	if src.Headscale.APIKeyFile != "" {
		dst.Headscale.APIKeyFile = src.Headscale.APIKeyFile
	}
	if src.Headscale.User != "" {
		dst.Headscale.User = src.Headscale.User
	}
	if src.Headscale.KeyExpiration != 0 {
		dst.Headscale.KeyExpiration = src.Headscale.KeyExpiration
	}
	if src.Tsnet.StateDir != "" {
		dst.Tsnet.StateDir = src.Tsnet.StateDir
	}
	if src.Reconcile.FullResyncInterval != 0 {
		dst.Reconcile.FullResyncInterval = src.Reconcile.FullResyncInterval
	}
	if src.Reconcile.RateLimitRPS != 0 {
		dst.Reconcile.RateLimitRPS = src.Reconcile.RateLimitRPS
	}
	if src.LabelNamespace != "" {
		dst.LabelNamespace = src.LabelNamespace
	}
	if src.AllowedTags != nil {
		dst.AllowedTags = src.AllowedTags
	}
	if src.Network != "" {
		dst.Network = src.Network
	}
	return dst
}

func applyEnv(cfg *Config, env func(string) string) error {
	if v := env("TAILSWARM_HEADSCALE_URL"); v != "" {
		cfg.Headscale.URL = v
	}
	if v := env("TAILSWARM_HEADSCALE_API_KEY"); v != "" {
		cfg.Headscale.APIKey = v
	}
	if v := env("TAILSWARM_HEADSCALE_API_KEY_FILE"); v != "" {
		cfg.Headscale.APIKeyFile = v
	}
	if v := env("TAILSWARM_HEADSCALE_USER"); v != "" {
		cfg.Headscale.User = v
	}
	if v := env("TAILSWARM_HEADSCALE_KEY_EXPIRATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("TAILSWARM_HEADSCALE_KEY_EXPIRATION: %w", err)
		}
		cfg.Headscale.KeyExpiration = d
	}
	if v := env("TAILSWARM_TSNET_STATE_DIR"); v != "" {
		cfg.Tsnet.StateDir = v
	}
	if v := env("TAILSWARM_RECONCILE_FULL_RESYNC_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("TAILSWARM_RECONCILE_FULL_RESYNC_INTERVAL: %w", err)
		}
		cfg.Reconcile.FullResyncInterval = d
	}
	if v := env("TAILSWARM_RECONCILE_RATE_LIMIT_RPS"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("TAILSWARM_RECONCILE_RATE_LIMIT_RPS: %w", err)
		}
		cfg.Reconcile.RateLimitRPS = f
	}
	if v := env("TAILSWARM_LABEL_NAMESPACE"); v != "" {
		cfg.LabelNamespace = v
	}
	if v := env("TAILSWARM_ALLOWED_TAGS"); v != "" {
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		cfg.AllowedTags = out
	}
	if v := env("TAILSWARM_NETWORK"); v != "" {
		cfg.Network = v
	}
	return nil
}

func (c Config) validate() error {
	if c.Headscale.URL == "" {
		return fmt.Errorf("tailswarm: headscale.url is required")
	}
	u, err := url.Parse(c.Headscale.URL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("tailswarm: headscale.url %q is not a valid absolute URL", c.Headscale.URL)
	}
	if c.Headscale.User == "" {
		return fmt.Errorf("tailswarm: headscale.user is required")
	}
	if c.Tsnet.StateDir == "" {
		return fmt.Errorf("tailswarm: tsnet.state_dir is required")
	}
	if c.Headscale.KeyExpiration <= 0 {
		return fmt.Errorf("tailswarm: headscale.key_expiration must be positive, got %s", c.Headscale.KeyExpiration)
	}
	if c.Reconcile.FullResyncInterval <= 0 {
		return fmt.Errorf("tailswarm: reconcile.full_resync_interval must be positive, got %s", c.Reconcile.FullResyncInterval)
	}
	if c.Reconcile.RateLimitRPS <= 0 {
		return fmt.Errorf("tailswarm: reconcile.rate_limit_rps must be positive, got %v", c.Reconcile.RateLimitRPS)
	}
	if !labelNamespacePattern.MatchString(c.LabelNamespace) {
		return fmt.Errorf("tailswarm: label_namespace %q must match %s", c.LabelNamespace, labelNamespacePattern)
	}
	if c.Network == "" {
		return fmt.Errorf("tailswarm: network is required")
	}
	return nil
}
