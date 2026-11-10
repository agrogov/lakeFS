// Package mds is the Metadata Search indexer: the process behind `lakefs-sso mds run`
// (METADATA_SEARCH_DESIGN.md §4.1, §7, §9). It is a REST client of lakeFS and of the Iceberg
// REST catalog, so it imports neither pkg/catalog nor pkg/graveler: it reads the configuration
// of the top-level metadata_search section, ticks every period and reports its state on
// /_health and /metrics.
//
// Clean-room note: upstream Metadata Search is a lakeFS Team/Enterprise feature. This is an
// independent OSS implementation written only from the public guide
// (https://docs.lakefs.io/guides/metadata-search/), the Apache Iceberg specs and this
// repository's code.
package mds

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// ConfigKey is the top-level configuration section the indexer reads (§7). It is the
	// upstream-compatible name and deliberately not nested under "iceberg": the server reads
	// only iceberg.metadata_search.enabled, and the indexer reads only this section.
	ConfigKey = "metadata_search"

	// deprecatedEndpointSuffix is the suffix the deprecated lakefs_mds_endpoint has to end in
	// (guide G10): that key named the separate Metadata Search catalog, which the guide retired in
	// favor of the lakeFS catalog at <lakefs_endpoint>/iceberg/api. A value without the suffix is
	// a base URL, which is what lakefs_endpoint is, so it is refused with a message that names the
	// rename instead of being guessed.
	deprecatedEndpointSuffix = "/mds/iceberg/api"
)

// The documented defaults of the metadata_search section that the guide's key list still has
// (G10, G11). Every other setting this package used to read is now fixed behaviour: the keys are
// gone from the guide, so there is nothing left to validate or default (X6).
const (
	// DefaultListenAddress is where /_health, /metrics and /_pprof/ are served (§7, §9).
	DefaultListenAddress = "0.0.0.0:8080"

	// DefaultPeriod is the interval between two ticks of the indexer (§4.1).
	DefaultPeriod = 60 * time.Second

	// DefaultConcurrency is how many branches an indexer processes in parallel (§4.1).
	DefaultConcurrency = 25

	// DefaultMaxCommits bounds the number of source commits one branch applies per tick (§4.2).
	DefaultMaxCommits = 1000
)

// ErrInvalidConfig is wrapped by every configuration error, so a caller can tell a bad
// metadata_search section from an I/O or programming error. Validation reports one error per
// problem, joined into a single error (§7).
var ErrInvalidConfig = errors.New("invalid metadata_search configuration")

// The static errors of the branch-selector warning. They are reported as warnings, never as
// startup failures: the guide's validation list has no branch rule, and a selector that matches
// nothing is not a configuration error the process can decide about.
var (
	// errEmptyBranchSelector reports a metadata_search.repositories.<R>.branches entry that is
	// the empty string, which matches no branch.
	errEmptyBranchSelector = errors.New("a branch selector must not be empty")

	// errBranchGlobStar reports a "*" that is not the trailing character. Only a trailing glob
	// is supported (§4.1).
	errBranchGlobStar = errors.New(`only a single trailing "*" is supported in a branch pattern`)
)

// RepositoryConfig is one entry of metadata_search.repositories (§7): a data repository to index
// and the branches of it that are mirrored into R-metadata.
type RepositoryConfig struct {
	// Branches are the branches to index: exact names, or a prefix followed by a single
	// trailing "*" glob (§4.1). An empty list indexes nothing and is not a startup error (X8).
	Branches []string `mapstructure:"branches"`

	// StorageNamespace overrides the storage namespace of R-metadata (§3.3). Empty means
	// "-metadata" is appended to R's own namespace, in every case: a bucket root
	// (s3://bucket -> s3://bucket-metadata) and an Azure container root
	// (https://acct.blob.core.windows.net/c -> https://acct.blob.core.windows.net/c-metadata)
	// included, exactly as the guide says (X10).
	StorageNamespace string `mapstructure:"storage_namespace"`
}

// Config is the top-level "metadata_search:" section of the lakeFS configuration (§7). It is
// populated by LoadConfig from viper and never by the server's own configuration unmarshalling,
// which knows nothing about this section.
//
// The key list is the guide's (G10, G11): lakefs_endpoint, the deprecated lakefs_mds_endpoint,
// the optional access_key_id/secret_key pair, listen_address, period, concurrency, max_commits,
// since and repositories. The object-store credentials the indexer writes with are the standard
// blockstore.* settings of the process, not a key of this section (X6).
type Config struct {
	// LakeFSEndpoint is the base URL of lakeFS: the API lives at /api/v1 and the Iceberg catalog
	// at /iceberg, so writes go to <LakeFSEndpoint>/iceberg/api (§4.1, G10). Required, unless the
	// deprecated lakefs_mds_endpoint is the only endpoint set. LoadConfig stores the resolved base
	// URL here, so this field is always the effective endpoint of a loaded configuration.
	LakeFSEndpoint string `mapstructure:"lakefs_endpoint"`

	// LakeFSMDSEndpoint is the deprecated spelling of LakeFSEndpoint (G10). It named the separate
	// Metadata Search catalog, so it is only accepted when it ends in /mds/iceberg/api, which is
	// stripped with a warning. Our own configurations used it as a base URL; that form fails
	// validation with a message that names the rename.
	LakeFSMDSEndpoint string `mapstructure:"lakefs_mds_endpoint"`

	// AccessKeyID is the access key of the principal the indexer writes as (§8). Optional: it is
	// set together with SecretKey or not at all (G11, X7), so an installation that provisions the
	// service user itself leaves both empty.
	AccessKeyID string `mapstructure:"access_key_id"`

	// SecretKey is the secret key of that principal. Optional, and set together with AccessKeyID.
	SecretKey string `mapstructure:"secret_key"` //nolint:gosec // a configuration field, not a credential value

	// ListenAddress is the address of the indexer's own HTTP server: /_health, /metrics and
	// /_pprof/ (§9).
	ListenAddress string `mapstructure:"listen_address"`

	// Period is the interval between two ticks (§4.1). Must be positive.
	Period time.Duration `mapstructure:"period"`

	// Concurrency is how many branches one tick processes in parallel (§4.1). Must be at least 1.
	Concurrency int `mapstructure:"concurrency"`

	// MaxCommits bounds the number of source commits one branch applies per tick (§4.2). Must be
	// at least 1.
	MaxCommits int `mapstructure:"max_commits"`

	// Since is the RFC 3339 timestamp that bounds a backfill of a branch: only commits created
	// at or after it are listed (§4.5 step 2). It is kept as the text the operator wrote so that
	// a bad value can be reported with the value in it; SinceTime returns the parsed form. It is
	// read by applyViperOverrides rather than by the mapstructure decode, because a bare YAML
	// timestamp arrives as a time.Time and the text is what this setting is.
	Since string `mapstructure:"-"`

	// Repositories are the data repositories to index, by repository name (§7). An empty map
	// means Metadata Search is disabled: it is not a startup error (X8).
	Repositories map[string]RepositoryConfig `mapstructure:"repositories"`

	// since is the parsed form of Since; nil when the key is absent or empty. It is unexported
	// because the text is the setting: the parsed value is what the indexer uses.
	since *time.Time
}

// LoadConfig reads and validates the "metadata_search:" configuration section through viper. It
// is called by `lakefs-sso mds run` before the first tick, so an invalid section fails startup
// with one error per problem instead of surfacing after the indexer has already written rows.
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
// the config-file map, so per-key environment variables such as
// LAKEFS_METADATA_SEARCH_SECRET_KEY (§7) would otherwise be silently ignored. Nested values
// (repositories) are read by the UnmarshalKey decode above.
func (c *Config) applyViperOverrides() {
	if viper.IsSet(ConfigKey + ".lakefs_endpoint") {
		c.LakeFSEndpoint = viper.GetString(ConfigKey + ".lakefs_endpoint")
	}
	if viper.IsSet(ConfigKey + ".lakefs_mds_endpoint") {
		c.LakeFSMDSEndpoint = viper.GetString(ConfigKey + ".lakefs_mds_endpoint")
	}
	if viper.IsSet(ConfigKey + ".access_key_id") {
		c.AccessKeyID = viper.GetString(ConfigKey + ".access_key_id")
	}
	if viper.IsSet(ConfigKey + ".secret_key") {
		c.SecretKey = viper.GetString(ConfigKey + ".secret_key")
	}
	if viper.IsSet(ConfigKey + ".listen_address") {
		c.ListenAddress = viper.GetString(ConfigKey + ".listen_address")
	}
	if viper.IsSet(ConfigKey + ".period") {
		c.Period = viper.GetDuration(ConfigKey + ".period")
	}
	if viper.IsSet(ConfigKey + ".concurrency") {
		c.Concurrency = viper.GetInt(ConfigKey + ".concurrency")
	}
	if viper.IsSet(ConfigKey + ".max_commits") {
		c.MaxCommits = viper.GetInt(ConfigKey + ".max_commits")
	}
	if since, ok := viperTimeString(ConfigKey + ".since"); ok {
		c.Since = since
	}
}

// setDefaults fills every absent setting with its documented default (§7). A key the
// installation wrote is never replaced, so an explicit 0 stays visible to Validate instead of
// being silently turned into the default: viper.IsSet is what distinguishes the two.
func (c *Config) setDefaults() {
	set := func(key string) bool { return viper.IsSet(ConfigKey + "." + key) }
	if !set("listen_address") || c.ListenAddress == "" {
		c.ListenAddress = DefaultListenAddress
	}
	if !set("period") {
		c.Period = DefaultPeriod
	}
	if !set("concurrency") {
		c.Concurrency = DefaultConcurrency
	}
	if !set("max_commits") {
		c.MaxCommits = DefaultMaxCommits
	}
}

// Validate checks the section against the guide's validation list (G11): an endpoint is set, the
// optional credential pair is complete, period > 0, concurrency >= 1 and max_commits >= 1.
// Nothing else fails startup: a repository name, an unparsable `since` or an unsupported branch
// selector is a warning, and an empty repositories or branches list disables work instead of
// stopping the process (X7, X8, X9).
//
// It reports one error per problem: the returned error joins every finding, so an operator fixes
// the whole section in one pass instead of rediscovering the next problem on the next start.
func (c *Config) Validate() error {
	var problems []error
	invalid := func(format string, args ...any) {
		problems = append(problems, fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...)))
	}

	c.resolveEndpoint(invalid)
	// The guide's optional credential pair: both or neither (G11, X7).
	hasAccessKey := strings.TrimSpace(c.AccessKeyID) != ""
	hasSecretKey := strings.TrimSpace(c.SecretKey) != ""
	if hasAccessKey != hasSecretKey {
		invalid("%s.access_key_id and %s.secret_key must be set together, or both left empty",
			ConfigKey, ConfigKey)
	}
	if c.Period <= 0 {
		invalid("%s.period must be positive, got %s", ConfigKey, c.Period)
	}
	if c.Concurrency < 1 {
		invalid("%s.concurrency must be at least 1, got %d", ConfigKey, c.Concurrency)
	}
	if c.MaxCommits < 1 {
		invalid("%s.max_commits must be at least 1, got %d", ConfigKey, c.MaxCommits)
	}

	c.resolveSince()
	c.warnRepositories()
	if len(c.Repositories) == 0 {
		logging.ContextUnavailable().Warnf(
			"mds: %s.repositories is empty, so Metadata Search is disabled and nothing is indexed", ConfigKey)
	}
	return errors.Join(problems...)
}

// resolveEndpoint sets LakeFSEndpoint to the lakeFS base URL the indexer writes to. lakefs_endpoint
// is the guide's key (G10); lakefs_mds_endpoint is the deprecated spelling and only accepted when
// it ends in /mds/iceberg/api, whose suffix is stripped with a warning. A lakefs_mds_endpoint that
// is a base URL - which is what our own configurations (hr2) carry - is refused with a message
// that names the rename, instead of being guessed into the wrong catalog.
func (c *Config) resolveEndpoint(invalid func(format string, args ...any)) {
	log := logging.ContextUnavailable()
	endpoint := strings.TrimRight(strings.TrimSpace(c.LakeFSEndpoint), "/")
	deprecated := strings.TrimRight(strings.TrimSpace(c.LakeFSMDSEndpoint), "/")
	switch {
	case endpoint != "":
		c.LakeFSEndpoint = endpoint
		if deprecated != "" {
			log.WithField("deprecated_key", ConfigKey+".lakefs_mds_endpoint").
				Warn("mds: both lakefs_endpoint and its deprecated spelling lakefs_mds_endpoint are set, using lakefs_endpoint")
		}
	case deprecated == "":
		invalid("%s.lakefs_endpoint is required: the lakeFS base URL, e.g. http://lakefs:8000", ConfigKey)
	case strings.HasSuffix(deprecated, deprecatedEndpointSuffix):
		stripped := strings.TrimSuffix(deprecated, deprecatedEndpointSuffix)
		if stripped == "" {
			invalid("%s.lakefs_mds_endpoint %q is not a lakeFS base URL: set %s.lakefs_endpoint to the base URL instead",
				ConfigKey, c.LakeFSMDSEndpoint, ConfigKey)
			return
		}
		c.LakeFSEndpoint = stripped
		log.WithFields(logging.Fields{
			"deprecated_key": ConfigKey + ".lakefs_mds_endpoint",
			"endpoint":       stripped,
		}).Warnf("mds: %s.lakefs_mds_endpoint is deprecated in favor of %s.lakefs_endpoint, using %q",
			ConfigKey, ConfigKey, stripped)
	default:
		invalid("%s.lakefs_mds_endpoint is deprecated and is only accepted when it ends in %q, got %q: "+
			"that is a lakeFS base URL, so rename the key to %s.lakefs_endpoint",
			ConfigKey, deprecatedEndpointSuffix, c.LakeFSMDSEndpoint, ConfigKey)
	}
}

// resolveSince parses the optional since bound. A value that is not RFC 3339 does not fail startup
// (X6: the guide's validation list has no since rule); it is reported as a warning and dropped, so
// the branch is captured "from the time the branch was created", which is the guide's default
// (X11), instead of failing the process on a value an operator can fix in a running deployment.
func (c *Config) resolveSince() {
	c.since = nil
	if c.Since == "" {
		return
	}
	parsed, err := time.Parse(time.RFC3339, c.Since)
	if err != nil {
		logging.ContextUnavailable().WithField("since", c.Since).Warn(
			"mds: metadata_search.since is not an RFC 3339 timestamp, ignoring it; metadata is captured from the time the branch was created")
		return
	}
	c.since = &parsed
}

// warnRepositories reports the repository settings a data repository may carry that would
// otherwise silently index nothing (§4.1): a branch selector that no branch can match. It is a
// warning and never an error: the guide has no startup rule about repositories (X9).
func (c *Config) warnRepositories() {
	log := logging.ContextUnavailable()
	for _, name := range c.RepositoryNames() {
		repository := c.Repositories[name]
		if len(repository.Branches) == 0 {
			log.WithField("repository", name).Warnf(
				"mds: %s.repositories.%s.branches is empty, so no branch of %q is indexed", ConfigKey, name, name)
			continue
		}
		for _, selector := range repository.Branches {
			if err := validateBranchGlob(selector); err != nil {
				log.WithFields(logging.Fields{
					"repository": name,
					"selector":   selector,
				}).Warnf("mds: %s.repositories.%s.branches: %v, so no branch matches it",
					ConfigKey, name, err)
			}
		}
	}
}

// validateBranchGlob checks the branch selector of §4.1: an exact branch name, or a prefix
// followed by a single trailing "*". It is a warning, not a startup failure: a "*" anywhere else
// is reported rather than silently treated as a pattern nothing matches.
func validateBranchGlob(glob string) error {
	if glob == "" {
		return errEmptyBranchSelector
	}
	if star := strings.IndexByte(glob, '*'); star >= 0 && star != len(glob)-1 {
		return fmt.Errorf("%w, got %q", errBranchGlobStar, glob)
	}
	return nil
}

// RepositoryNames returns the configured repository names in sorted order, so that the tick and
// the health report iterate deterministically.
func (c *Config) RepositoryNames() []string {
	if c == nil {
		return nil
	}
	names := make([]string, 0, len(c.Repositories))
	for name := range c.Repositories {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// viperTimeString reads a timestamp setting the way viper holds it: a quoted YAML value or an
// environment variable arrives as a string, while an unquoted RFC 3339 value arrives as a
// time.Time, because YAML resolves bare timestamps. Either way the result is the text in RFC 3339
// form, so a warning can quote it back and the indexer can parse it.
func viperTimeString(key string) (string, bool) {
	switch value := viper.Get(key).(type) {
	case nil:
		return "", false
	case string:
		return value, true
	case time.Time:
		return value.Format(time.RFC3339Nano), true
	default:
		return fmt.Sprint(value), true
	}
}

// SinceTime returns the parsed metadata_search.since timestamp, and false when the key is absent,
// empty, or not an RFC 3339 timestamp.
func (c *Config) SinceTime() (time.Time, bool) {
	if c == nil || c.since == nil {
		return time.Time{}, false
	}
	return *c.since, true
}
