package config_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/config"
)

// TestConfigImplAbsorbsMetadataSearchSection pins the one-field change to oss_config.go: without
// the MetadataSearch field, viper.UnmarshalExact rejects the top-level "metadata_search:" block as
// an unknown key, which would make every installation that shares one configuration file with
// `lakefs-sso mds run` fail to start (METADATA_SEARCH_DESIGN.md §7). The section is parsed by
// pkg/mds, not by pkg/config, and it holds secrets, so it must be masked when the config is
// logged exactly like the sso section.
func TestConfigImplAbsorbsMetadataSearchSection(t *testing.T) {
	const (
		secretKey        = "mds-secret-key-must-not-be-logged"
		storageAccessKey = "storage-access-key-must-not-be-logged"
		storageSecretKey = "storage-secret-key-must-not-be-logged"
	)
	// A valid lakeFS configuration (the keys of testdata/valid_config.yaml) that also carries
	// every metadata_search key of §7: the one file the server and the indexer both read.
	document := `
logging:
  format: text
  level: NONE
  output: "-"

database:
  type: postgres
  postgres:
    connection_string: test:///dev/null
    max_open_connections: 12
    max_idle_connections: 13
    connection_max_lifetime: 2s

auth:
  encrypt:
    secret_key: "required in config"

blockstore:
  type: local
  local:
    path: /tmp

gateways:
  s3:
    domain_name:
      - s3.example.com
    region: us-east-1

listen_address: "0.0.0.0:8005"

metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000
  access_key_id: AKIAEXAMPLE
  secret_key: ` + secretKey + `
  listen_address: 0.0.0.0:8080
  period: 60s
  concurrency: 25
  max_commits: 1000
  since: 2026-01-01T00:00:00Z
  backfill: head
  seed_lookback: 1000
  rebuild_threshold: 100000
  delete_branches: true
  tag_retention: 0s
  compaction:
    max_delete_files: 200
    every_commits: 500
  storage:
    s3.endpoint: http://seaweedfs:8333
    s3.region: us-east-1
    s3.access-key-id: ` + storageAccessKey + `
    s3.secret-access-key: ` + storageSecretKey + `
    s3.path-style-access: "true"
  repositories:
    my-repo:
      branches: [main, "dev-*"]
      storage_namespace: s3://bucket/metadata/my-repo
`
	fn := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(fn, []byte(document), 0o600))

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.SetConfigFile(fn)
	require.NoError(t, viper.ReadInConfig())

	cfg, err := config.BuildConfig("")
	require.NoError(t, err, "the metadata_search: section must not fail UnmarshalExact")

	impl, ok := cfg.(*config.ConfigImpl)
	require.True(t, ok, "BuildConfig must return *config.ConfigImpl")
	require.Equal(t, "http://lakefs:8000", impl.MetadataSearch["lakefs_mds_endpoint"])
	require.Equal(t, "AKIAEXAMPLE", impl.MetadataSearch["access_key_id"])
	require.Equal(t, secretKey, impl.MetadataSearch["secret_key"])
	require.Equal(t, "head", impl.MetadataSearch["backfill"])
	require.Contains(t, impl.MetadataSearch, "storage", "the section is kept verbatim for pkg/mds")
	require.Contains(t, impl.MetadataSearch, "repositories", "the section is kept verbatim for pkg/mds")

	// The section holds secrets: it is masked as a whole, exactly like sso.
	fields := config.MapLoggingFields(cfg)
	require.Equal(t, config.FieldMaskedValue, fields["metadata_search"],
		"metadata_search must be masked like sso")
	for key, value := range fields {
		for _, secret := range []string{secretKey, storageAccessKey, storageSecretKey} {
			require.NotContains(t, fmt.Sprint(value), secret,
				"logged config field %q leaks a metadata_search secret", key)
		}
	}
}

// TestMapLoggingFieldsMasksMetadataSearchLikeSSO checks the reason MetadataSearch uses
// SecretSection rather than a plain map: MapLoggingFields masks SecretSection values, and it must
// treat metadata_search the same way it treats the existing sso section.
func TestMapLoggingFieldsMasksMetadataSearchLikeSSO(t *testing.T) {
	t.Parallel()
	value := struct {
		SSO            config.SecretSection `mapstructure:"sso"`
		MetadataSearch config.SecretSection `mapstructure:"metadata_search"`
		Empty          config.SecretSection `mapstructure:"empty"`
	}{
		SSO:            config.SecretSection{"client_secret": "sso-secret"},
		MetadataSearch: config.SecretSection{"secret_key": "mds-secret"},
	}
	fields := config.MapLoggingFields(value)
	require.Equal(t, config.FieldMaskedValue, fields["sso"])
	require.Equal(t, config.FieldMaskedValue, fields["metadata_search"])
	require.Equal(t, config.FieldMaskedNoValue, fields["empty"])
}
