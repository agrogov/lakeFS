package icebergcatalog

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
	// No iceberg: section at all - every field falls back to its documented default.
	useConfig(t, "{}\n")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.False(t, cfg.Enabled, "the catalog is off unless it is switched on")
	require.Equal(t, CatalogPrefix, cfg.Prefix)
	require.Equal(t, DefaultTokenTTL, cfg.TokenTTL)
	require.Equal(t, DefaultMaxRequestBody, cfg.MaxRequestBody, "8 MiB by default")
	require.Equal(t, DefaultUncommittedGCGrace, cfg.UncommittedGCGrace, "24h of grace for client-written files")
	require.True(t, cfg.IgnoreCompactionCommits)

	// The documented defaults of ICEBERG_CATALOG_PLAN.md section 10, spelled out.
	require.Equal(t, "lakefs", cfg.Prefix)
	require.Equal(t, time.Hour, cfg.TokenTTL)
	require.Equal(t, int64(8<<20), cfg.MaxRequestBody)
	require.Equal(t, 24*time.Hour, cfg.UncommittedGCGrace)
}

func TestLoadConfigValid(t *testing.T) {
	useConfig(t, `
iceberg:
  enabled: true
  prefix: lakefs
  token_ttl: 15m
  max_request_body: 1048576
  uncommitted_gc_grace: 90m
  ignore_compaction_commits: false
`)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, "lakefs", cfg.Prefix)
	require.Equal(t, 15*time.Minute, cfg.TokenTTL)
	require.Equal(t, int64(1<<20), cfg.MaxRequestBody)
	require.Equal(t, 90*time.Minute, cfg.UncommittedGCGrace)
	require.False(t, cfg.IgnoreCompactionCommits, "an explicit false must not be replaced by the default")
}

// TestLoadConfigIgnoreCompactionCommits pins the one default that is true: the zero value of a
// bool is false, so the parsed config must distinguish "absent" from "set to false".
func TestLoadConfigIgnoreCompactionCommits(t *testing.T) {
	t.Run("default is true", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n")
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.True(t, cfg.IgnoreCompactionCommits)
	})

	t.Run("explicit true", func(t *testing.T) {
		useConfig(t, "iceberg:\n  ignore_compaction_commits: true\n")
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.True(t, cfg.IgnoreCompactionCommits)
	})

	t.Run("explicit false", func(t *testing.T) {
		useConfig(t, "iceberg:\n  ignore_compaction_commits: false\n")
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.False(t, cfg.IgnoreCompactionCommits)
	})
}

func TestLoadConfigEnvironmentOverrides(t *testing.T) {
	// viper.UnmarshalKey only decodes the file map; the env vars must still win (design §10).
	useConfig(t, "iceberg:\n  enabled: false\n  token_ttl: 1h\n")
	t.Setenv("LAKEFS_ICEBERG_ENABLED", "true")
	t.Setenv("LAKEFS_ICEBERG_TOKEN_TTL", "90s")
	t.Setenv("LAKEFS_ICEBERG_MAX_REQUEST_BODY", "1024")
	t.Setenv("LAKEFS_ICEBERG_UNCOMMITTED_GC_GRACE", "48h")
	t.Setenv("LAKEFS_ICEBERG_IGNORE_COMPACTION_COMMITS", "false")
	viper.SetEnvPrefix("LAKEFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, 90*time.Second, cfg.TokenTTL)
	require.Equal(t, int64(1024), cfg.MaxRequestBody)
	require.Equal(t, 48*time.Hour, cfg.UncommittedGCGrace)
	require.False(t, cfg.IgnoreCompactionCommits)
}

func TestLoadConfigValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "unsupported prefix",
			yaml:    "iceberg:\n  enabled: true\n  prefix: mds\n",
			wantErr: `prefix "mds" is not supported, only "lakefs" is`,
		},
		{
			name:    "unsupported prefix with a different case",
			yaml:    "iceberg:\n  prefix: LakeFS\n",
			wantErr: "is not supported",
		},
		{
			name:    "unsupported prefix while disabled",
			yaml:    "iceberg:\n  enabled: false\n  prefix: another\n",
			wantErr: "is not supported",
		},
		{
			name:    "non-positive token_ttl",
			yaml:    "iceberg:\n  token_ttl: -1m\n",
			wantErr: "token_ttl must be positive",
		},
		{
			name:    "negative max_request_body",
			yaml:    "iceberg:\n  max_request_body: -1\n",
			wantErr: "max_request_body must not be negative",
		},
		{
			name:    "non-positive uncommitted_gc_grace",
			yaml:    "iceberg:\n  uncommitted_gc_grace: -1h\n",
			wantErr: "uncommitted_gc_grace must be positive",
		},
		{
			name:    "malformed section",
			yaml:    "iceberg: 3\n",
			wantErr: "cannot parse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useConfig(t, tt.yaml)
			cfg, err := LoadConfig()
			require.Error(t, err)
			require.Nil(t, cfg)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestLoadConfigPrefixIsAConstant is Q-I3: Enterprise treats the prefix as the constant
// "lakefs", and the open-source catalog has no read path that could honour another value, so a
// different prefix is rejected at load time instead of served and then failing on every lookup.
func TestLoadConfigPrefixIsAConstant(t *testing.T) {
	useConfig(t, "iceberg:\n  prefix: lakefs\n")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "lakefs", cfg.Prefix)

	useConfig(t, "iceberg:\n  prefix: \"\"\n")
	cfg, err = LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "lakefs", cfg.Prefix, "an empty prefix falls back to the constant")
}

// TestConfigImplAbsorbsIcebergSection pins the one-field change to pkg/config/oss_config.go:
// without the Iceberg field, viper.UnmarshalExact rejects an "iceberg:" block as an unknown key,
// which would make every installation with an iceberg section fail to start. The section is
// parsed by this package, not by pkg/config.
func TestConfigImplAbsorbsIcebergSection(t *testing.T) {
	useConfig(t, `
iceberg:
  enabled: true
  prefix: lakefs
  some_future_key: 7
`)

	c := &config.ConfigImpl{}
	require.NoError(t, config.Unmarshal(c), "the iceberg: section must not fail UnmarshalExact")

	section, ok := c.Iceberg["enabled"]
	require.True(t, ok, "iceberg.enabled should be absorbed into ConfigImpl.Iceberg")
	require.Equal(t, true, section)
	_, ok = c.Iceberg["some_future_key"]
	require.True(t, ok, "the section is kept verbatim, so new keys do not need a pkg/config change")
}
