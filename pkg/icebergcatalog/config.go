package icebergcatalog

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
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

	// DefaultVendedSessionDuration is the default session_duration of vended credentials: one
	// hour (§8 "STS vended credentials"). It is the lifetime of the STS session the catalog
	// hands a client that asked for vended-credentials.
	DefaultVendedSessionDuration = time.Hour

	// MinimumVendedSessionDuration is the shortest session_duration STS accepts (15 minutes),
	// which §8 repeats as the lower bound of the setting. A shorter session would expire while a
	// client is still writing, so it is rejected at startup rather than vended.
	MinimumVendedSessionDuration = 15 * time.Minute

	// MaximumVendedSessionDuration is the longest session_duration STS accepts (12 hours).
	MaximumVendedSessionDuration = 12 * time.Hour

	// CredentialsVendingSectionKey is where the vended-credentials setting lives: the
	// blockstore.s3 section of the lakeFS configuration, as the public lakeFS Iceberg
	// documentation spells it (§8). It is read through viper (see Config.CredentialsVending)
	// rather than through the core config unmarshalling, which knows nothing about it.
	CredentialsVendingSectionKey = "blockstore.s3.credentials_vending" //nolint:gosec // a configuration key, not a credential

	// credentialsVendingKey is the same setting inside the iceberg section, the one a YAML file
	// can carry while the core configuration has no blockstore.s3.credentials_vending field (see
	// CredentialsVendingConfig).
	credentialsVendingKey = "credentials_vending"
)

// ErrInvalidConfig is wrapped by every configuration validation error, so callers can tell a
// bad iceberg section from an I/O or programming error.
var ErrInvalidConfig = errors.New("invalid iceberg catalog configuration")

// CredentialsVendingConfig is the vended-credentials setting of §8 (phase I-4): the role the
// catalog assumes to hand a client a session scoped to one table's prefix.
//
// The keys are the ones the public lakeFS Iceberg documentation uses,
// blockstore.s3.credentials_vending.{role_arn, session_duration, external_id, endpoint}, and
// they are read through viper by LoadConfig (see applyCredentialsVending). The section is also
// accepted as iceberg.credentials_vending, because viper.UnmarshalExact rejects an unknown
// blockstore.s3 key: the OSS core configuration has no credentials_vending field, so the
// iceberg section is the one place a YAML file can carry the setting until the core struct
// grows the field. Environment variables work on both paths
// (LAKEFS_BLOCKSTORE_S3_CREDENTIALS_VENDING_ROLE_ARN or
// LAKEFS_ICEBERG_CREDENTIALS_VENDING_ROLE_ARN).
type CredentialsVendingConfig struct {
	// RoleARN is the role the catalog assumes. Vending is off while it is empty (§8: an
	// installation that does not configure a role keeps the default mode, the client's own
	// credentials).
	RoleARN string `mapstructure:"role_arn"`

	// SessionDuration is the lifetime of the session, one hour by default and at least 15
	// minutes (§8).
	SessionDuration time.Duration `mapstructure:"session_duration"`

	// ExternalID is the ExternalId of the AssumeRole call, for a role whose trust policy
	// requires one. Optional.
	ExternalID string `mapstructure:"external_id"`

	// Endpoint overrides the STS endpoint, for an S3-compatible service that implements STS
	// itself (MinIO, LocalStack) or a private STS endpoint. Optional; empty means the AWS
	// default endpoint of the region the server runs in.
	Endpoint string `mapstructure:"endpoint"`
}

// Configured reports whether vended credentials were asked for at all, i.e. whether the
// operator named a role to assume. It is the setting-side half of §8's mode table: while it is
// false the catalog advertises no credentials endpoint and every client keeps using its own
// storage credentials.
func (c CredentialsVendingConfig) Configured() bool {
	return c.RoleARN != ""
}

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

	// CredentialsVending is the vended-credentials setting of §8 (phase I-4). It is populated
	// from blockstore.s3.credentials_vending, the key the public lakeFS documentation names, and
	// from iceberg.credentials_vending; see CredentialsVendingConfig.
	CredentialsVending CredentialsVendingConfig `mapstructure:"credentials_vending"`
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
	cfg.applyCredentialsVending()
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

// applyCredentialsVending reads the vended-credentials setting (§8) from both keys it is
// accepted under, so that neither spelling silently does nothing.
//
// The keys are read one by one through viper rather than only by the UnmarshalKey decode above,
// because UnmarshalKey sees the config file alone: reading them individually is what makes
// LAKEFS_ICEBERG_CREDENTIALS_VENDING_ROLE_ARN and
// LAKEFS_BLOCKSTORE_S3_CREDENTIALS_VENDING_ROLE_ARN work, exactly like applyViperOverrides does
// for the rest of the section. blockstore.s3.credentials_vending is applied last, so the key the
// public lakeFS Iceberg documentation names wins over the iceberg-section spelling of the same
// setting, and a key that only one of the two sections sets keeps its value.
func (c *Config) applyCredentialsVending() {
	readKeys := func(section string, dst *CredentialsVendingConfig) {
		if viper.IsSet(section + ".role_arn") {
			dst.RoleARN = viper.GetString(section + ".role_arn")
		}
		if viper.IsSet(section + ".session_duration") {
			dst.SessionDuration = viper.GetDuration(section + ".session_duration")
		}
		if viper.IsSet(section + ".external_id") {
			dst.ExternalID = viper.GetString(section + ".external_id")
		}
		if viper.IsSet(section + ".endpoint") {
			dst.Endpoint = viper.GetString(section + ".endpoint")
		}
	}
	readKeys(ConfigKey+"."+credentialsVendingKey, &c.CredentialsVending)
	readKeys(CredentialsVendingSectionKey, &c.CredentialsVending)
}

// IgnoresCompactionCommits reports whether a table-pointer conflict that a compaction commit
// alone caused is auto-resolved instead of reported (§6.4, phase I-5). A nil *Config answers with
// the documented default, DefaultIgnoreCompactionCommits, so a caller that has no configuration in
// hand still gets the behaviour of the public lakeFS Iceberg documentation; the decision itself is
// Store.ResolvePointerConflict (commit.go).
func (c *Config) IgnoresCompactionCommits() bool {
	if c == nil {
		return DefaultIgnoreCompactionCommits
	}
	return c.IgnoreCompactionCommits
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
	// One hour is the documented default session_duration (§8). A value is only defaulted while
	// vending is configured: left alone when it is not, so that "session_duration without
	// role_arn" stays visible to Validate instead of becoming a silently ignored 1h.
	if c.CredentialsVending.Configured() && c.CredentialsVending.SessionDuration == 0 {
		c.CredentialsVending.SessionDuration = DefaultVendedSessionDuration
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
	return c.CredentialsVending.Validate()
}

// Validate checks the vended-credentials setting of §8. It runs even when vending is off, so
// that a half-written section is a startup error rather than a setting that silently does
// nothing: session_duration, external_id and endpoint all describe a role to assume, and
// without role_arn there is no role.
func (c CredentialsVendingConfig) Validate() error {
	if !c.Configured() {
		if c.SessionDuration != 0 || c.ExternalID != "" || c.Endpoint != "" {
			return fmt.Errorf("%w: %s requires role_arn: session_duration, external_id and endpoint describe the role the catalog assumes",
				ErrInvalidConfig, CredentialsVendingSectionKey)
		}
		return nil
	}
	// The scheme prefix is the one property of an ARN this setting can check without knowing the
	// partition; the value is passed to STS verbatim, which validates the rest.
	if !strings.HasPrefix(c.RoleARN, "arn:") {
		return fmt.Errorf("%w: %s.role_arn %q is not an ARN",
			ErrInvalidConfig, CredentialsVendingSectionKey, c.RoleARN)
	}
	if c.SessionDuration < MinimumVendedSessionDuration || c.SessionDuration > MaximumVendedSessionDuration {
		return fmt.Errorf("%w: %s.session_duration must be between %s and %s, got %s",
			ErrInvalidConfig, CredentialsVendingSectionKey, MinimumVendedSessionDuration,
			MaximumVendedSessionDuration, c.SessionDuration)
	}
	if c.Endpoint != "" {
		endpoint, err := url.Parse(c.Endpoint)
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
			return fmt.Errorf("%w: %s.endpoint %q is not an http(s) URL",
				ErrInvalidConfig, CredentialsVendingSectionKey, c.Endpoint)
		}
	}
	return nil
}
