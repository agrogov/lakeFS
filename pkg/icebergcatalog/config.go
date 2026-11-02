package icebergcatalog

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/viper"
)

const (
	// ConfigKey is the YAML namespace of the iceberg catalog configuration: the "iceberg:"
	// block, absorbed by pkg/config/oss_config.go so that viper.UnmarshalExact accepts it.
	ConfigKey = "iceberg"

	// CatalogPrefix is the only supported value of iceberg.prefix. It is the value of the
	// prefix override returned by GET /v1/config, and clients prepend it to every identifier
	// they send back (ICEBERG_CATALOG_PLAN.md §5, Q-I3). The catalog addresses tables by
	// repository, branch, namespace and table name, so there is nothing a different prefix
	// could mean: it is a constant, and LoadConfig rejects any other value instead of
	// silently serving a prefix no read path implements.
	CatalogPrefix = "lakefs"

	// DefaultTokenTTL is the default lifetime of an OAuth token minted by
	// POST /v1/oauth/tokens (ICEBERG_CATALOG_PLAN.md §7.1): 1 hour.
	DefaultTokenTTL = time.Hour

	// DefaultMaxRequestBody is the default of the iceberg.max_request_body setting: the
	// largest catalog request body the server reads, 8 MiB. A commit request carries the
	// requirements and updates of one metadata.json edit and is far below it; the limit
	// exists so that no request can make the server buffer an arbitrary amount of memory.
	DefaultMaxRequestBody int64 = 8 << 20

	// DefaultUncommittedGCGrace is the default of the iceberg.uncommitted_gc_grace setting:
	// how long an object under the table location is protected from the uncommitted GC, 24h.
	// Clients write data, manifest and metadata files before the catalog commit registers them
	// as lakeFS entries, so the GC must not delete a fresh client-written object
	// (ICEBERG_CATALOG_PLAN.md §6.3).
	DefaultUncommittedGCGrace = 24 * time.Hour

	// UncommittedGCGraceProtectAll is the value of iceberg.uncommitted_gc_grace that switches the
	// grace off in favour of maximum protection: every object under <storage_namespace>/_iceberg/
	// stays out of the uncommitted-GC report, however old it is (gc.go UncommittedGCProtected,
	// spike S-I2 §6). It is the escape hatch for installations that keep staged branches for
	// weeks; the cost is that a table file no commit ever staged is never collected, so the area
	// grows until the value is lowered.
	//
	// Zero is only reachable when the key is set explicitly: an absent key means "use
	// DefaultUncommittedGCGrace", which setDefaults applies.
	UncommittedGCGraceProtectAll time.Duration = 0

	// DefaultIgnoreCompactionCommits is the default of the iceberg.ignore_compaction_commits
	// setting: a commit that only re-points metadata at rewritten (compacted) data files is
	// merged into a concurrent change instead of conflicting with it (§6.4, phase I-5).
	DefaultIgnoreCompactionCommits = true
)

// ErrInvalidConfig is wrapped by every configuration validation error, so callers can tell a
// bad iceberg section from an I/O or programming error.
var ErrInvalidConfig = errors.New("invalid iceberg catalog configuration")

// Config is the "iceberg:" section of the lakeFS configuration (ICEBERG_CATALOG_PLAN.md §10).
// It is populated by LoadConfig from viper and never by the core config unmarshalling, which
// only absorbs the key.
type Config struct {
	// Enabled mounts the catalog at /iceberg/api and /iceberg/relative_to. When false the
	// wrapper binary mounts nothing and the 501 placeholders stay in place (§4.1).
	Enabled bool `mapstructure:"enabled"`

	// Prefix is the prefix override of GET /v1/config. Must be CatalogPrefix; see above.
	Prefix string `mapstructure:"prefix"`

	// TokenTTL is the lifetime of a token minted by POST /v1/oauth/tokens (§7.1).
	TokenTTL time.Duration `mapstructure:"token_ttl"`

	// MaxRequestBody is the largest catalog request body the server reads, in bytes. It bounds
	// http.MaxBytesReader in front of the request decoder, so a client cannot make the server
	// buffer an arbitrary amount of memory.
	MaxRequestBody int64 `mapstructure:"max_request_body"`

	// UncommittedGCGrace protects client-written objects under the table location from the
	// uncommitted GC for this long, until the catalog commit stages them as entries (§6.3).
	// UncommittedGCGraceProtectAll (0) protects them regardless of age; a negative value is
	// rejected by Validate.
	UncommittedGCGrace time.Duration `mapstructure:"uncommitted_gc_grace"`

	// gcGraceExplicit reports that uncommitted_gc_grace was set by the installation, so that an
	// explicit 0 - which means UncommittedGCGraceProtectAll - is not replaced by the 24h default.
	// It is unexported: the distinction is a property of how the configuration was read, not a
	// setting an operator can see or write.
	gcGraceExplicit bool

	// IgnoreCompactionCommits enables the compaction auto-resolve of §6.4 (phase I-5).
	IgnoreCompactionCommits bool `mapstructure:"ignore_compaction_commits"`
}

// LoadConfig reads and validates the "iceberg:" configuration section through viper. It is
// called from the wrapper binary's routes builder, so an invalid configuration fails startup
// before the catalog is mounted.
func LoadConfig() (*Config, error) {
	cfg := &Config{}
	if err := viper.UnmarshalKey(ConfigKey, cfg); err != nil {
		return nil, fmt.Errorf("%w: cannot parse the %q section: %w", ErrInvalidConfig, ConfigKey, err)
	}
	cfg.applyViperOverrides()
	cfg.setDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyViperOverrides re-reads every scalar key through viper. viper.UnmarshalKey decodes only
// the config-file map, so per-key environment variables such as LAKEFS_ICEBERG_ENABLED
// (ICEBERG_CATALOG_PLAN.md §10) would otherwise be silently ignored, leaving Enabled false.
func (c *Config) applyViperOverrides() {
	const (
		enabledKey                 = ConfigKey + ".enabled"
		prefixKey                  = ConfigKey + ".prefix"
		tokenTTLKey                = ConfigKey + ".token_ttl"
		maxRequestBodyKey          = ConfigKey + ".max_request_body"
		uncommittedGCGraceKey      = ConfigKey + ".uncommitted_gc_grace"
		ignoreCompactionCommitsKey = ConfigKey + ".ignore_compaction_commits"
	)
	if viper.IsSet(enabledKey) {
		c.Enabled = viper.GetBool(enabledKey)
	}
	if viper.IsSet(prefixKey) {
		c.Prefix = viper.GetString(prefixKey)
	}
	if viper.IsSet(tokenTTLKey) {
		c.TokenTTL = viper.GetDuration(tokenTTLKey)
	}
	if viper.IsSet(maxRequestBodyKey) {
		c.MaxRequestBody = viper.GetInt64(maxRequestBodyKey)
	}
	if viper.IsSet(uncommittedGCGraceKey) {
		c.UncommittedGCGrace = viper.GetDuration(uncommittedGCGraceKey)
		// An explicit 0 is UncommittedGCGraceProtectAll, not "unset": setDefaults must not
		// replace it with the 24h default.
		c.gcGraceExplicit = true
	}
	// The default is true, so the zero value cannot mean "unset": viper.IsSet is what
	// distinguishes an explicit `ignore_compaction_commits: false` from an absent key.
	if viper.IsSet(ignoreCompactionCommitsKey) {
		c.IgnoreCompactionCommits = viper.GetBool(ignoreCompactionCommitsKey)
	} else {
		c.IgnoreCompactionCommits = DefaultIgnoreCompactionCommits
	}
}

// setDefaults fills every unset field with its documented default. Zero values mean "unset"
// here: a value that is set but not positive is rejected by Validate instead of defaulted.
func (c *Config) setDefaults() {
	if c.Prefix == "" {
		c.Prefix = CatalogPrefix
	}
	if c.TokenTTL == 0 {
		c.TokenTTL = DefaultTokenTTL
	}
	if c.MaxRequestBody == 0 {
		c.MaxRequestBody = DefaultMaxRequestBody
	}
	// Zero means "unset" only for a key the installation did not write; an explicit 0 is the
	// protect-everything value of gc.go.
	if c.UncommittedGCGrace == 0 && !c.gcGraceExplicit {
		c.UncommittedGCGrace = DefaultUncommittedGCGrace
	}
}

// Validate checks the catalog configuration (ICEBERG_CATALOG_PLAN.md §10). Unlike the datasets
// section there is no key that only matters when the service is enabled, so every check runs
// regardless of Enabled: a disabled catalog with an unusable prefix or a non-positive token
// lifetime is a configuration bug, and failing startup is cheaper than discovering it after the
// catalog is switched on.
func (c *Config) Validate() error {
	if c.Prefix != CatalogPrefix {
		return fmt.Errorf("%w: prefix %q is not supported, only %q is; the prefix is the constant of GET /v1/config overrides",
			ErrInvalidConfig, c.Prefix, CatalogPrefix)
	}
	if c.TokenTTL <= 0 {
		return fmt.Errorf("%w: token_ttl must be positive, got %s", ErrInvalidConfig, c.TokenTTL)
	}
	if c.MaxRequestBody < 0 {
		return fmt.Errorf("%w: max_request_body must not be negative, got %d", ErrInvalidConfig, c.MaxRequestBody)
	}
	// Negative is a configuration bug: a negative window would protect nothing under any reading,
	// and silently treating it as "protect everything" (what gc.go does, because the fail-safe
	// direction is to protect) would hide the typo. Zero is the documented protect-everything
	// value; absent is 24h.
	if c.UncommittedGCGrace < 0 {
		return fmt.Errorf("%w: uncommitted_gc_grace must be positive, or %s to protect every %s/ object, got %s",
			ErrInvalidConfig, UncommittedGCGraceProtectAll, DataPrefix, c.UncommittedGCGrace)
	}
	return nil
}
