// Package datasets implements the lakeFS Datasets control plane: named, versioned,
// read-only collections of data curated from one or more repositories. A dataset is
// materialised as a read-only repository with the same name, so every existing lakeFS
// read path keeps working unchanged (see DATASETS_DESIGN.md).
//
// This package is fork-owned. Nothing outside the fork-owned entry points
// (cmd/lakefs-sso, cmd/lakectl-datasets and the cmd/lakefs hook) may import it
// (DATASETS_DESIGN.md §4.6).
package datasets

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	// ConfigKey is the YAML namespace of the datasets configuration: the "datasets:" block,
	// absorbed by pkg/config/oss_config.go so that viper.UnmarshalExact accepts it.
	ConfigKey = "datasets"

	// AccessModelOpen grants every base-group member data-plane read access to every dataset;
	// dataset grants still govern the control plane (DATASETS_DESIGN.md §9.6).
	AccessModelOpen = "open"
	// AccessModelStrict would isolate data-plane reads per dataset. Not implemented yet; it is
	// accepted by the config schema only so that a typo-free future value is recognised.
	AccessModelStrict = "strict"

	DefaultMaxItems              = 1000
	DefaultSyncPublishMaxObjects = 100_000
	DefaultMergeFanIn            = 64
	DefaultPublishTimeout        = 30 * time.Minute
	DefaultReconcileInterval     = 10 * time.Minute
)

// ErrInvalidConfig is wrapped by every configuration validation error, so callers can tell a
// bad datasets section from an I/O or programming error.
var ErrInvalidConfig = errors.New("invalid datasets configuration")

// MetadataConfig controls how dataset metadata is validated (DATASETS_DESIGN.md §5.5).
type MetadataConfig struct {
	// Strict rejects metadata keys that were not declared as metadata keys.
	Strict bool `mapstructure:"strict"`
}

// Config is the "datasets:" section of the lakeFS configuration (DATASETS_DESIGN.md §13).
// It is populated by LoadConfig from viper and never by the core config unmarshalling, which
// only absorbs the key.
type Config struct {
	// Enabled turns the control plane on. A disabled service still mounts a 501 handler under
	// /api/v1/datasets, so clients get a clear answer instead of the invalid-endpoint catch-all.
	Enabled bool `mapstructure:"enabled"`

	// StorageID is the blockstore that holds dataset repositories. It must equal the storage ID
	// of every source repository; sources are referenced by full physical address and read back
	// through this blockstore (DATASETS_DESIGN.md §6.3).
	StorageID string `mapstructure:"storage_id"`

	// StorageNamespacePrefix is the namespace under which dataset repositories are created:
	// <prefix>/<name>-<ulid>. Required when Enabled is true.
	StorageNamespacePrefix string `mapstructure:"storage_namespace_prefix"`

	// MaxItems bounds the number of data items in a definition.
	MaxItems int `mapstructure:"max_items"`

	// AllowEmptyItems permits prefix items that currently match no object.
	AllowEmptyItems bool `mapstructure:"allow_empty_items"`

	// SyncPublishMaxObjects is the object count above which a publish runs asynchronously
	// (202 + task) instead of in the request.
	SyncPublishMaxObjects int `mapstructure:"sync_publish_max_objects"`

	// MergeFanIn is the maximum number of per-item iterators merged in one pass.
	MergeFanIn int `mapstructure:"merge_fan_in"`

	// PublishTimeout bounds how long a dataset may stay in a transient state before the
	// reconciler rolls the publish back.
	PublishTimeout time.Duration `mapstructure:"publish_timeout"`

	// ReconcileInterval is the period of the background repair loop.
	ReconcileInterval time.Duration `mapstructure:"reconcile_interval"`

	// AccessModel is "open" or "strict" (DATASETS_DESIGN.md §9.6).
	AccessModel string `mapstructure:"access_model"`

	// ImmutableGuardGroups lists extra groups that get the per-dataset immutability deny policy.
	ImmutableGuardGroups []string `mapstructure:"immutable_guard_groups"`

	Metadata MetadataConfig `mapstructure:"metadata"`
}

// LoadConfig reads and validates the "datasets:" configuration section through viper. It is
// called from RoutesBuilder, so an invalid configuration with enabled: true fails startup: the
// hook loop in cmd/lakefs/cmd/run.go logs the returned error as fatal before the server listens.
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
// the config-file map, so per-key environment variables such as LAKEFS_DATASETS_ENABLED
// (DATASETS_DESIGN.md §13) would otherwise be silently ignored, leaving Enabled false.
func (c *Config) applyViperOverrides() {
	const (
		enabledKey              = ConfigKey + ".enabled"
		storageIDKey            = ConfigKey + ".storage_id"
		namespacePrefixKey      = ConfigKey + ".storage_namespace_prefix"
		maxItemsKey             = ConfigKey + ".max_items"
		allowEmptyItemsKey      = ConfigKey + ".allow_empty_items"
		syncPublishObjectsKey   = ConfigKey + ".sync_publish_max_objects"
		mergeFanInKey           = ConfigKey + ".merge_fan_in"
		publishTimeoutKey       = ConfigKey + ".publish_timeout"
		reconcileIntervalKey    = ConfigKey + ".reconcile_interval"
		accessModelKey          = ConfigKey + ".access_model"
		immutableGuardGroupsKey = ConfigKey + ".immutable_guard_groups"
		metadataStrictKey       = ConfigKey + ".metadata.strict"
	)
	if viper.IsSet(enabledKey) {
		c.Enabled = viper.GetBool(enabledKey)
	}
	if viper.IsSet(storageIDKey) {
		c.StorageID = viper.GetString(storageIDKey)
	}
	if viper.IsSet(namespacePrefixKey) {
		c.StorageNamespacePrefix = viper.GetString(namespacePrefixKey)
	}
	if viper.IsSet(maxItemsKey) {
		c.MaxItems = viper.GetInt(maxItemsKey)
	}
	if viper.IsSet(allowEmptyItemsKey) {
		c.AllowEmptyItems = viper.GetBool(allowEmptyItemsKey)
	}
	if viper.IsSet(syncPublishObjectsKey) {
		c.SyncPublishMaxObjects = viper.GetInt(syncPublishObjectsKey)
	}
	if viper.IsSet(mergeFanInKey) {
		c.MergeFanIn = viper.GetInt(mergeFanInKey)
	}
	if viper.IsSet(publishTimeoutKey) {
		c.PublishTimeout = viper.GetDuration(publishTimeoutKey)
	}
	if viper.IsSet(reconcileIntervalKey) {
		c.ReconcileInterval = viper.GetDuration(reconcileIntervalKey)
	}
	if viper.IsSet(accessModelKey) {
		c.AccessModel = viper.GetString(accessModelKey)
	}
	if viper.IsSet(immutableGuardGroupsKey) {
		c.ImmutableGuardGroups = viper.GetStringSlice(immutableGuardGroupsKey)
	}
	if viper.IsSet(metadataStrictKey) {
		c.Metadata.Strict = viper.GetBool(metadataStrictKey)
	}
}

// setDefaults fills every unset field with its documented default. Zero values mean "unset"
// here: a value that is set but not positive is rejected by Validate instead of defaulted.
func (c *Config) setDefaults() {
	if c.MaxItems == 0 {
		c.MaxItems = DefaultMaxItems
	}
	if c.SyncPublishMaxObjects == 0 {
		c.SyncPublishMaxObjects = DefaultSyncPublishMaxObjects
	}
	if c.MergeFanIn == 0 {
		c.MergeFanIn = DefaultMergeFanIn
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = DefaultPublishTimeout
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = DefaultReconcileInterval
	}
	if c.AccessModel == "" {
		c.AccessModel = AccessModelOpen
	}
}

// Validate checks the dataset configuration (DATASETS_DESIGN.md §13). Storage and namespace
// checks apply only when the service is enabled: a disabled section is inert.
func (c *Config) Validate() error {
	if c.PublishTimeout <= 0 {
		return fmt.Errorf("%w: publish_timeout must be positive, got %s", ErrInvalidConfig, c.PublishTimeout)
	}
	if c.ReconcileInterval <= 0 {
		return fmt.Errorf("%w: reconcile_interval must be positive, got %s", ErrInvalidConfig, c.ReconcileInterval)
	}
	if c.MaxItems <= 0 {
		return fmt.Errorf("%w: max_items must be positive, got %d", ErrInvalidConfig, c.MaxItems)
	}
	if c.SyncPublishMaxObjects < 0 {
		return fmt.Errorf("%w: sync_publish_max_objects must not be negative, got %d", ErrInvalidConfig, c.SyncPublishMaxObjects)
	}
	if c.MergeFanIn <= 0 {
		return fmt.Errorf("%w: merge_fan_in must be positive, got %d", ErrInvalidConfig, c.MergeFanIn)
	}
	switch c.AccessModel {
	case AccessModelOpen:
	case AccessModelStrict:
		return fmt.Errorf("%w: access_model %q is not implemented yet, use %q", ErrInvalidConfig, AccessModelStrict, AccessModelOpen)
	default:
		return fmt.Errorf("%w: access_model must be %q or %q, got %q", ErrInvalidConfig, AccessModelOpen, AccessModelStrict, c.AccessModel)
	}
	if !c.Enabled {
		return nil
	}
	if c.StorageNamespacePrefix == "" {
		return fmt.Errorf("%w: storage_namespace_prefix is required when datasets are enabled", ErrInvalidConfig)
	}
	if err := c.validateStorageNamespacePrefix(); err != nil {
		return err
	}
	if c.StorageID != config.SingleBlockstoreID {
		return fmt.Errorf("%w: storage_id %q does not exist; this build has a single blockstore, addressed by the empty storage ID",
			ErrInvalidConfig, c.StorageID)
	}
	return nil
}

// validateStorageNamespacePrefix checks that the prefix is a namespace the blockstore can parse:
// a known storage scheme, matching the configured blockstore type when one is set.
func (c *Config) validateStorageNamespacePrefix() error {
	qk, err := block.DefaultResolveNamespace(c.StorageNamespacePrefix, "", block.IdentifierTypeRelative)
	if err != nil {
		return fmt.Errorf("%w: storage_namespace_prefix %q is not a valid blockstore namespace: %w",
			ErrInvalidConfig, c.StorageNamespacePrefix, err)
	}
	// Not configured in tests and in installations that rely on the blockstore defaults.
	if blockstoreType := viper.GetString(config.BlockstoreTypeKey); blockstoreType != "" {
		if got := qk.GetStorageType().BlockstoreType(); got != blockstoreType {
			return fmt.Errorf("%w: storage_namespace_prefix %q addresses a %s blockstore, but blockstore.type is %q",
				ErrInvalidConfig, c.StorageNamespacePrefix, got, blockstoreType)
		}
	}
	return nil
}
