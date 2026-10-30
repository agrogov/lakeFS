package datasets

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/config"
)

// useConfig installs a viper state backed by the given YAML document, so that LoadConfig reads
// exactly what a lakeFS installation would read from lakefs.yaml.
func useConfig(t *testing.T, yaml string) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.SetConfigType("yaml")
	require.NoError(t, viper.ReadConfig(strings.NewReader(yaml)))
}

func TestLoadConfigDefaults(t *testing.T) {
	// No datasets: section at all - every field falls back to its documented default.
	useConfig(t, "{}\n")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
	require.Empty(t, cfg.StorageID)
	require.Empty(t, cfg.StorageNamespacePrefix)
	require.Equal(t, DefaultMaxItems, cfg.MaxItems)
	require.False(t, cfg.AllowEmptyItems)
	require.Equal(t, DefaultSyncPublishMaxObjects, cfg.SyncPublishMaxObjects)
	require.Equal(t, DefaultMergeFanIn, cfg.MergeFanIn)
	require.Equal(t, DefaultPublishTimeout, cfg.PublishTimeout)
	require.Equal(t, DefaultReconcileInterval, cfg.ReconcileInterval)
	require.Equal(t, AccessModelOpen, cfg.AccessModel)
	require.Empty(t, cfg.ImmutableGuardGroups)
	require.False(t, cfg.Metadata.Strict)
}

func TestLoadConfigValid(t *testing.T) {
	useConfig(t, `
datasets:
  enabled: true
  storage_id: ""
  storage_namespace_prefix: "s3://my-bucket/lakefs-datasets"
  max_items: 5
  allow_empty_items: true
  sync_publish_max_objects: 7
  merge_fan_in: 3
  publish_timeout: 5m
  reconcile_interval: 1m
  access_model: open
  immutable_guard_groups:
    - DatasetAdmins
    - DatasetPublishers
  metadata:
    strict: true
`)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Empty(t, cfg.StorageID)
	require.Equal(t, "s3://my-bucket/lakefs-datasets", cfg.StorageNamespacePrefix)
	require.Equal(t, 5, cfg.MaxItems)
	require.True(t, cfg.AllowEmptyItems)
	require.Equal(t, 7, cfg.SyncPublishMaxObjects)
	require.Equal(t, 3, cfg.MergeFanIn)
	require.Equal(t, 5*time.Minute, cfg.PublishTimeout)
	require.Equal(t, time.Minute, cfg.ReconcileInterval)
	require.Equal(t, AccessModelOpen, cfg.AccessModel)
	require.Equal(t, []string{"DatasetAdmins", "DatasetPublishers"}, cfg.ImmutableGuardGroups)
	require.True(t, cfg.Metadata.Strict)
}

// TestLoadConfigDisabledIsInert checks that the storage checks only apply when the service is
// enabled: a disabled section must not fail startup because a placeholder prefix is missing.
func TestLoadConfigDisabledIsInert(t *testing.T) {
	useConfig(t, "datasets:\n  enabled: false\n  storage_id: not-a-storage\n")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
}

func TestLoadConfigEnvironmentOverrides(t *testing.T) {
	// viper.UnmarshalKey only decodes the file map; the env vars must still win (design §13).
	useConfig(t, "datasets:\n  enabled: false\n  max_items: 7\n")
	t.Setenv("LAKEFS_DATASETS_ENABLED", "true")
	t.Setenv("LAKEFS_DATASETS_STORAGE_NAMESPACE_PREFIX", "s3://env-bucket/datasets")
	t.Setenv("LAKEFS_DATASETS_MAX_ITEMS", "42")
	t.Setenv("LAKEFS_DATASETS_PUBLISH_TIMEOUT", "90s")
	t.Setenv("LAKEFS_DATASETS_METADATA_STRICT", "true")
	viper.SetEnvPrefix("LAKEFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, "s3://env-bucket/datasets", cfg.StorageNamespacePrefix)
	require.Equal(t, 42, cfg.MaxItems)
	require.Equal(t, 90*time.Second, cfg.PublishTimeout)
	require.True(t, cfg.Metadata.Strict)
}

func TestLoadConfigValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		prepare func(t *testing.T)
		wantErr string
	}{
		{
			name:    "enabled without storage_namespace_prefix",
			yaml:    "datasets:\n  enabled: true\n",
			wantErr: "storage_namespace_prefix is required",
		},
		{
			name:    "storage_namespace_prefix without a storage scheme",
			yaml:    "datasets:\n  enabled: true\n  storage_namespace_prefix: \"my-bucket/datasets\"\n",
			wantErr: "is not a valid blockstore namespace",
		},
		{
			name:    "storage_namespace_prefix with an unknown scheme",
			yaml:    "datasets:\n  enabled: true\n  storage_namespace_prefix: \"ftp://my-bucket/datasets\"\n",
			wantErr: "is not a valid blockstore namespace",
		},
		{
			name: "storage_namespace_prefix does not match the blockstore",
			yaml: "datasets:\n  enabled: true\n  storage_namespace_prefix: \"s3://my-bucket/datasets\"\n",
			prepare: func(t *testing.T) {
				t.Helper()
				viper.Set(config.BlockstoreTypeKey, "local")
			},
			wantErr: `addresses a s3 blockstore, but blockstore.type is "local"`,
		},
		{
			name:    "unknown storage_id",
			yaml:    "datasets:\n  enabled: true\n  storage_id: warm\n  storage_namespace_prefix: \"s3://my-bucket/datasets\"\n",
			wantErr: `storage_id "warm" does not exist`,
		},
		{
			name:    "non-positive publish_timeout",
			yaml:    "datasets:\n  publish_timeout: -1m\n",
			wantErr: "publish_timeout must be positive",
		},
		{
			name:    "non-positive reconcile_interval",
			yaml:    "datasets:\n  reconcile_interval: -10m\n",
			wantErr: "reconcile_interval must be positive",
		},
		{
			name:    "non-positive max_items",
			yaml:    "datasets:\n  max_items: -1\n",
			wantErr: "max_items must be positive",
		},
		{
			name:    "negative sync_publish_max_objects",
			yaml:    "datasets:\n  sync_publish_max_objects: -1\n",
			wantErr: "sync_publish_max_objects must not be negative",
		},
		{
			name:    "non-positive merge_fan_in",
			yaml:    "datasets:\n  merge_fan_in: -64\n",
			wantErr: "merge_fan_in must be positive",
		},
		{
			name:    "unknown access_model",
			yaml:    "datasets:\n  access_model: openish\n",
			wantErr: `access_model must be "open" or "strict"`,
		},
		{
			name:    "strict access_model is not implemented yet",
			yaml:    "datasets:\n  access_model: strict\n",
			wantErr: "is not implemented yet",
		},
		{
			name:    "malformed section",
			yaml:    "datasets: 3\n",
			wantErr: "cannot parse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useConfig(t, tt.yaml)
			if tt.prepare != nil {
				tt.prepare(t)
			}
			cfg, err := LoadConfig()
			require.Error(t, err)
			require.Nil(t, cfg)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
