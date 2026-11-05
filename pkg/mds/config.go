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
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

const (
	// ConfigKey is the top-level configuration section the indexer reads (§7). It is the
	// upstream-compatible name and deliberately not nested under "iceberg": the server reads
	// only iceberg.metadata_search.enabled, and the indexer reads only this section.
	ConfigKey = "metadata_search"

	// BackfillHead indexes only the head of a branch when it is first indexed: the objects of
	// the head commit are written as a single full listing (§4.5 step 2). It is the default.
	BackfillHead = "head"

	// BackfillHistory starts at the branch's oldest first-parent commit, like upstream's
	// "branch creation" mode (§4.5 step 2).
	BackfillHistory = "history"

	// maxRepositoryIDLength is the longest lakeFS repository ID:
	// validator.ReValidRepositoryID accepts 3 to 63 characters.
	maxRepositoryIDLength = 63
)

// MaxRepositoryNameLength is the longest data repository name the indexer accepts: the companion
// repository is R + icebergcatalog.MetadataRepositorySuffix, and a lakeFS repository ID is at
// most 63 characters, so R may be at most 54 (§3.3, §7).
const MaxRepositoryNameLength = maxRepositoryIDLength - len(icebergcatalog.MetadataRepositorySuffix)

// The documented defaults of the metadata_search section (§7). A key that is written with a
// value that Validate rejects (for example period: 0) is not defaulted: the default applies only
// when the key is absent.
const (
	// DefaultListenAddress is where /_health, /metrics and /_pprof/ are served (§7, §9).
	DefaultListenAddress = "0.0.0.0:8080"

	// DefaultPeriod is the interval between two ticks of the indexer (§4.1).
	DefaultPeriod = 60 * time.Second

	// DefaultRequestTimeout bounds one HTTP request of the indexer's lakeFS API client and of its
	// Iceberg REST catalog client (§4.1, §7).
	DefaultRequestTimeout = 60 * time.Second

	// DefaultConcurrency is how many branches an indexer processes in parallel (§4.1).
	DefaultConcurrency = 25

	// DefaultMaxCommits bounds the number of source commits one branch applies per tick (§4.2).
	DefaultMaxCommits = 1000

	// DefaultSeedLookback bounds how far back the first indexing of a branch looks for an
	// indexed ancestor to seed from (§4.5 step 1).
	DefaultSeedLookback = 1000

	// DefaultRebuildThreshold is the change-set size above which a commit is indexed by
	// rebuilding the branch's table from a full listing instead of applying a delta (§4.3).
	DefaultRebuildThreshold = 100_000

	// DefaultDeleteBranches is whether deleting a matching branch of the data repository also
	// deletes the mirrored branch of R-metadata (§4.8). Its tags stay.
	DefaultDeleteBranches = true

	// DefaultTagRetention is how long mds-* tags are kept; 0 keeps them forever (§4.9).
	DefaultTagRetention time.Duration = 0

	// DefaultCompactionMaxDeleteFiles is the number of position-delete files that triggers a
	// compaction of a branch's table (§4.9).
	DefaultCompactionMaxDeleteFiles = 200

	// DefaultCompactionEveryCommits is the number of indexed commits after which a branch's
	// table is compacted (§4.9).
	DefaultCompactionEveryCommits = 500
)

// ErrInvalidConfig is wrapped by every configuration error, so a caller can tell a bad
// metadata_search section from an I/O or programming error. Validation reports one error per
// problem, joined into a single error (§7).
var ErrInvalidConfig = errors.New("invalid metadata_search configuration")

// The static errors of the branch-selector rule of §4.1. Validate wraps them with the offending
// selector, so an operator sees the value and errors.Is can still tell the rule apart.
var (
	// errEmptyBranchSelector reports a metadata_search.repositories.<R>.branches entry that is
	// the empty string, which matches no branch.
	errEmptyBranchSelector = errors.New("a branch selector must not be empty")

	// errBranchGlobStar reports a "*" that is not the trailing character. Only a trailing glob
	// is supported (§4.1).
	errBranchGlobStar = errors.New(`only a single trailing "*" is supported in a branch pattern`)
)

// CompactionConfig is the metadata_search.compaction section (§4.9): when a branch's table is
// rewritten to drop the position-delete files that the merge-on-read delta writes accumulate.
//
// A threshold of 0 disables its trigger and a negative value is refused by Validate: 0 is the
// documented way to turn a trigger off, so it is never replaced by the default here (IO-6). The
// defaults apply only when the key is absent from the configuration.
type CompactionConfig struct {
	// MaxDeleteFiles is the number of delete files that triggers a compaction; 0 disables the
	// trigger, the default when the key is absent is 200.
	MaxDeleteFiles int `mapstructure:"max_delete_files"`

	// EveryCommits is the number of indexed commits after which a compaction runs; 0 disables the
	// trigger, the default when the key is absent is 500.
	EveryCommits int `mapstructure:"every_commits"`
}

// RepositoryConfig is one entry of metadata_search.repositories (§7): a data repository to index
// and the branches of it that are mirrored into R-metadata.
type RepositoryConfig struct {
	// Branches are the branches to index: exact names, or a prefix followed by a single
	// trailing "*" glob (§4.1). At least one is required.
	Branches []string `mapstructure:"branches"`

	// StorageNamespace overrides the storage namespace of R-metadata (§3.3). Empty means the
	// namespace is derived from R's: <ns>-metadata when R's namespace has a path component, and
	// a bucket-root namespace requires this key to be set.
	StorageNamespace string `mapstructure:"storage_namespace"`
}

// Config is the top-level "metadata_search:" section of the lakeFS configuration (§7). It is
// populated by LoadConfig from viper and never by the server's own configuration unmarshalling,
// which knows nothing about this section.
type Config struct {
	// LakeFSMDSEndpoint is the base URL of lakeFS: the API lives at /api/v1 and the Iceberg
	// catalog at /iceberg (§4.1). Required.
	LakeFSMDSEndpoint string `mapstructure:"lakefs_mds_endpoint"`

	// AccessKeyID is the access key of the mds-indexer principal (§8). Required.
	AccessKeyID string `mapstructure:"access_key_id"`

	// SecretKey is the secret key of the mds-indexer principal (§8). Required.
	SecretKey string `mapstructure:"secret_key"` //nolint:gosec // a configuration field, not a credential value

	// ListenAddress is the address of the indexer's own HTTP server: /_health, /metrics and
	// /_pprof/ (§9).
	ListenAddress string `mapstructure:"listen_address"`

	// Period is the interval between two ticks (§4.1). Must be positive.
	Period time.Duration `mapstructure:"period"`

	// RequestTimeout bounds every HTTP request the indexer sends, on the lakeFS API client and on
	// the Iceberg REST catalog client alike (§4.1). It is what keeps one connection that neither
	// answers nor resets from stalling the tick loop while /_health stays 200 (§9). Must be
	// positive.
	RequestTimeout time.Duration `mapstructure:"request_timeout"`

	// Concurrency is how many branches one tick processes in parallel (§4.1). Must be positive.
	Concurrency int `mapstructure:"concurrency"`

	// MaxCommits bounds the number of source commits one branch applies per tick (§4.2). Must
	// be positive.
	MaxCommits int `mapstructure:"max_commits"`

	// Since is the RFC 3339 timestamp that bounds a backfill of a branch: only commits created
	// at or after it are listed (§4.5 step 2). It is kept as the text the operator wrote so
	// that Validate can report a bad value with the value in it; SinceTime returns the parsed
	// form. It is read by applyViperOverrides rather than by the mapstructure decode, because a
	// bare YAML timestamp arrives as a time.Time and the text is what this setting is.
	Since string `mapstructure:"-"`

	// Backfill is BackfillHead (default) or BackfillHistory (§4.5 step 2).
	Backfill string `mapstructure:"backfill"`

	// SeedLookback is how many first-parent commits of a branch the first indexing walks back
	// looking for an indexed ancestor to seed from (§4.5 step 1). Must be positive.
	SeedLookback int `mapstructure:"seed_lookback"`

	// RebuildThreshold is the number of upserts plus deletes above which a commit is indexed by
	// a full rebuild of the branch's table instead of a delta (§4.3 step 2). Must be positive.
	RebuildThreshold int `mapstructure:"rebuild_threshold"`

	// DeleteBranches mirrors the deletion of a matching branch to R-metadata (§4.8). Default
	// true.
	DeleteBranches bool `mapstructure:"delete_branches"`

	// TagRetention is how long mds-* tags are kept, 0 keeping them forever (§4.9). A negative
	// duration is refused by Validate.
	TagRetention time.Duration `mapstructure:"tag_retention"`

	// Compaction configures the table maintenance of §4.9.
	Compaction CompactionConfig `mapstructure:"compaction"`

	// Storage holds the iceberg-go IO properties (s3.endpoint, s3.access-key-id, ...) used when
	// the catalog does not vend write credentials (§4.6). Optional.
	Storage map[string]string `mapstructure:"storage"`

	// Repositories are the data repositories to index, by repository name (§7). At least one is
	// required.
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
// (repositories, storage, compaction) are read by the UnmarshalKey decode above.
func (c *Config) applyViperOverrides() {
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
	if viper.IsSet(ConfigKey + ".request_timeout") {
		c.RequestTimeout = viper.GetDuration(ConfigKey + ".request_timeout")
	}
	if viper.IsSet(ConfigKey + ".concurrency") {
		c.Concurrency = viper.GetInt(ConfigKey + ".concurrency")
	}
	if viper.IsSet(ConfigKey + ".max_commits") {
		c.MaxCommits = viper.GetInt(ConfigKey + ".max_commits")
	}
	if viper.IsSet(ConfigKey + ".backfill") {
		c.Backfill = viper.GetString(ConfigKey + ".backfill")
	}
	if since, ok := viperTimeString(ConfigKey + ".since"); ok {
		c.Since = since
	}
	if viper.IsSet(ConfigKey + ".seed_lookback") {
		c.SeedLookback = viper.GetInt(ConfigKey + ".seed_lookback")
	}
	if viper.IsSet(ConfigKey + ".rebuild_threshold") {
		c.RebuildThreshold = viper.GetInt(ConfigKey + ".rebuild_threshold")
	}
	if viper.IsSet(ConfigKey + ".delete_branches") {
		c.DeleteBranches = viper.GetBool(ConfigKey + ".delete_branches")
	}
	if viper.IsSet(ConfigKey + ".tag_retention") {
		c.TagRetention = viper.GetDuration(ConfigKey + ".tag_retention")
	}
	if viper.IsSet(ConfigKey + ".compaction.max_delete_files") {
		c.Compaction.MaxDeleteFiles = viper.GetInt(ConfigKey + ".compaction.max_delete_files")
	}
	if viper.IsSet(ConfigKey + ".compaction.every_commits") {
		c.Compaction.EveryCommits = viper.GetInt(ConfigKey + ".compaction.every_commits")
	}
}

// setDefaults fills every absent setting with its documented default (§7). A key the
// installation wrote is never replaced, so an explicit 0 or false stays visible to Validate
// instead of being silently turned into the default: viper.IsSet is what distinguishes the two.
func (c *Config) setDefaults() {
	set := func(key string) bool { return viper.IsSet(ConfigKey + "." + key) }
	if !set("listen_address") || c.ListenAddress == "" {
		c.ListenAddress = DefaultListenAddress
	}
	if !set("period") {
		c.Period = DefaultPeriod
	}
	if !set("request_timeout") {
		c.RequestTimeout = DefaultRequestTimeout
	}
	if !set("concurrency") {
		c.Concurrency = DefaultConcurrency
	}
	if !set("max_commits") {
		c.MaxCommits = DefaultMaxCommits
	}
	if !set("backfill") {
		c.Backfill = BackfillHead
	}
	if !set("seed_lookback") {
		c.SeedLookback = DefaultSeedLookback
	}
	if !set("rebuild_threshold") {
		c.RebuildThreshold = DefaultRebuildThreshold
	}
	if !set("delete_branches") {
		c.DeleteBranches = DefaultDeleteBranches
	}
	if !set("tag_retention") {
		c.TagRetention = DefaultTagRetention
	}
	if !set("compaction.max_delete_files") {
		c.Compaction.MaxDeleteFiles = DefaultCompactionMaxDeleteFiles
	}
	if !set("compaction.every_commits") {
		c.Compaction.EveryCommits = DefaultCompactionEveryCommits
	}
}

// Validate checks the metadata_search section (§7) and reports one error per problem: the
// returned error joins every finding, so an operator fixes the whole section in one pass instead
// of rediscovering the next problem on the next start. It also parses Since, which SinceTime
// then returns.
func (c *Config) Validate() error {
	var problems []error
	invalid := func(format string, args ...any) {
		problems = append(problems, fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...)))
	}

	if strings.TrimSpace(c.LakeFSMDSEndpoint) == "" {
		invalid("%s.lakefs_mds_endpoint is required", ConfigKey)
	}
	if strings.TrimSpace(c.AccessKeyID) == "" {
		invalid("%s.access_key_id is required", ConfigKey)
	}
	if strings.TrimSpace(c.SecretKey) == "" {
		invalid("%s.secret_key is required", ConfigKey)
	}
	if c.Period <= 0 {
		invalid("%s.period must be positive, got %s", ConfigKey, c.Period)
	}
	if c.RequestTimeout <= 0 {
		invalid("%s.request_timeout must be positive, got %s", ConfigKey, c.RequestTimeout)
	}
	if c.Concurrency <= 0 {
		invalid("%s.concurrency must be positive, got %d", ConfigKey, c.Concurrency)
	}
	if c.MaxCommits <= 0 {
		invalid("%s.max_commits must be positive, got %d", ConfigKey, c.MaxCommits)
	}
	if c.SeedLookback <= 0 {
		invalid("%s.seed_lookback must be positive, got %d", ConfigKey, c.SeedLookback)
	}
	if c.RebuildThreshold <= 0 {
		invalid("%s.rebuild_threshold must be positive, got %d", ConfigKey, c.RebuildThreshold)
	}
	// The maintenance settings are the two places where 0 is a documented value: it turns a
	// compaction trigger off, and it keeps every mds-* tag (§4.9). Negative values are not.
	if c.TagRetention < 0 {
		invalid("%s.tag_retention must not be negative, got %s; 0 keeps every mds-* tag", ConfigKey, c.TagRetention)
	}
	if c.Compaction.MaxDeleteFiles < 0 {
		invalid("%s.compaction.max_delete_files must not be negative, got %d; 0 disables the trigger",
			ConfigKey, c.Compaction.MaxDeleteFiles)
	}
	if c.Compaction.EveryCommits < 0 {
		invalid("%s.compaction.every_commits must not be negative, got %d; 0 disables the trigger",
			ConfigKey, c.Compaction.EveryCommits)
	}
	switch c.Backfill {
	case BackfillHead, BackfillHistory:
	default:
		invalid("%s.backfill must be %q or %q, got %q", ConfigKey, BackfillHead, BackfillHistory, c.Backfill)
	}
	c.validateRepositories(invalid)

	c.since = nil
	if c.Since != "" {
		parsed, err := time.Parse(time.RFC3339, c.Since)
		if err != nil {
			invalid("%s.since %q is not an RFC 3339 timestamp: %v", ConfigKey, c.Since, err)
		} else {
			c.since = &parsed
		}
	}
	return errors.Join(problems...)
}

// validateRepositories checks every metadata_search.repositories entry: the repository name rules
// of §3.3 (it must leave room for the -metadata suffix and must not itself look like a companion
// repository), the requirement that a repository lists branches, and the branch-glob rule of §4.1
// (only a single trailing "*" is supported).
func (c *Config) validateRepositories(invalid func(format string, args ...any)) {
	if len(c.Repositories) == 0 {
		invalid("%s.repositories must list at least one repository", ConfigKey)
		return
	}
	for _, name := range c.RepositoryNames() {
		repo := c.Repositories[name]
		if strings.HasSuffix(name, icebergcatalog.MetadataRepositorySuffix) {
			invalid("%s.repositories.%s: a data repository name must not end in %q, that is the companion repository suffix",
				ConfigKey, name, icebergcatalog.MetadataRepositorySuffix)
		}
		if len(name) > MaxRepositoryNameLength {
			invalid("%s.repositories.%s: a data repository name must not be longer than %d characters, because %q is appended to it",
				ConfigKey, name, MaxRepositoryNameLength, icebergcatalog.MetadataRepositorySuffix)
		}
		if len(repo.Branches) == 0 {
			invalid("%s.repositories.%s.branches must list at least one branch", ConfigKey, name)
			continue
		}
		for i, glob := range repo.Branches {
			if err := validateBranchGlob(glob); err != nil {
				invalid("%s.repositories.%s.branches[%d]: %v", ConfigKey, name, i, err)
			}
		}
	}
}

// validateBranchGlob checks the branch selector of §4.1: an exact branch name, or a prefix
// followed by a single trailing "*". A "*" anywhere else is refused rather than treated as a
// pattern nothing matches, because a branch that silently stays unindexed is exactly the failure
// an operator cannot see.
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
// form, so Validate can quote it back and the indexer can parse it.
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

// SinceTime returns the parsed metadata_search.since timestamp, and false when the key is absent
// or empty.
func (c *Config) SinceTime() (time.Time, bool) {
	if c == nil || c.since == nil {
		return time.Time{}, false
	}
	return *c.since, true
}
