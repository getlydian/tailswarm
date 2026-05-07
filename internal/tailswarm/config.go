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

// Config is the merged YAML + env configuration for tailswarm. The
// loader populates it from a YAML file, overlays environment variables
// (env wins, per DESIGN.md §6), and then validates.
type Config struct {
	Headscale          HeadscaleConfig  `yaml:"headscale"`
	Sidecar            SidecarConfig    `yaml:"sidecar"`
	Reconcile          ReconcileConfig  `yaml:"reconcile"`
	LabelNamespace     string           `yaml:"label_namespace"`
	AllowedTagPrefixes []string         `yaml:"allowed_tag_prefixes"`
}

// HeadscaleConfig groups the controller-side knobs.
//
// APIKey is never read from YAML directly; operators provide either
// APIKeyFile (typically a Docker secret mount) which is read once at
// load time, or the TAILSWARM_HEADSCALE_API_KEY env var.
type HeadscaleConfig struct {
	URL           string        `yaml:"url"`
	APIKey        string        `yaml:"-"`
	APIKeyFile    string        `yaml:"api_key_file"`
	User          string        `yaml:"user"`
	KeyExpiration time.Duration `yaml:"key_expiration"`
}

// SidecarConfig groups sidecar-image knobs.
type SidecarConfig struct {
	Image string `yaml:"image"`
}

// ReconcileConfig groups loop-tuning knobs.
type ReconcileConfig struct {
	FullResyncInterval time.Duration `yaml:"full_resync_interval"`
	RateLimitRPS       float64       `yaml:"rate_limit_rps"`
}

// labelNamespacePattern matches the allowed shape for label_namespace —
// the part of "<ns>.enable" before the dot. Lower-case alnum and dashes,
// at least one character.
var labelNamespacePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// Load reads tailswarm configuration from a YAML file and overlays
// environment variables on top. env is injected (rather than calling
// os.Getenv directly) so tests can provide an isolated environment.
//
// path may be empty, in which case the YAML step is skipped and the
// config comes entirely from env + defaults.
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
		// Decode into a fresh Config so explicitly-set zero values from
		// YAML don't get clobbered by the defaults — then merge.
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
		Reconcile: ReconcileConfig{
			FullResyncInterval: 30 * time.Second,
			RateLimitRPS:       5,
		},
		LabelNamespace: defaultNamespace,
	}
}

// mergeConfig overlays src onto dst: any non-zero field in src replaces
// the corresponding field in dst. Slices are replaced wholesale rather
// than appended, so an operator who explicitly sets allowed_tag_prefixes
// to [] gets exactly that.
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
	if src.Sidecar.Image != "" {
		dst.Sidecar.Image = src.Sidecar.Image
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
	if src.AllowedTagPrefixes != nil {
		dst.AllowedTagPrefixes = src.AllowedTagPrefixes
	}
	return dst
}

// applyEnv overlays TAILSWARM_* environment variables onto cfg. env wins
// over YAML, which is what operators expect from twelve-factor-style
// config and what DESIGN.md §6 specifies.
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
	if v := env("TAILSWARM_SIDECAR_IMAGE"); v != "" {
		cfg.Sidecar.Image = v
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
	if v := env("TAILSWARM_ALLOWED_TAG_PREFIXES"); v != "" {
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		cfg.AllowedTagPrefixes = out
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
	if c.Sidecar.Image == "" {
		return fmt.Errorf("tailswarm: sidecar.image is required")
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
	return nil
}
