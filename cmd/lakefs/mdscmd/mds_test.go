package mdscmd

// mds_test.go pins the two pieces of `mds run` that are not the whole wiring - opening the database
// the server uses, and translating blockstore.* into the IO properties of a table write. The
// end-to-end RA4b test runs the command through the server binary's root command, in
// cmd/lakefs/mds_test.go.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	testAccessKeyID     = "AKIAIOSFODNN7EXAMPLE"
	testSecretAccessKey = "mds-run-test-secret-access-key-0123456789"
)

// TestOpenMDSCatalogFailsOnAnUnusableDatabase pins the startup rule: the indexer opens the
// database from database.*, so a database it cannot open fails startup the way it fails `run`
// instead of leaving a process that ticks and never indexes.
func TestOpenMDSCatalogFailsOnAnUnusableDatabase(t *testing.T) {
	notADirectory := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(notADirectory, []byte("x"), 0o600))

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)
	viper.Set("blockstore.local.path", t.TempDir())
	viper.Set("database.type", "local")
	viper.Set("database.local.path", notADirectory)
	viper.Set("logging.level", "NONE")
	viper.Set("logging.output", "-")

	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err, "build the lakeFS configuration")

	store, cat, err := openMDSCatalog(t.Context(), cfg)
	require.Error(t, err, "an unusable database fails startup")
	require.Nil(t, store)
	require.Nil(t, cat)
	require.Contains(t, err.Error(), "open the KV store from database.*")
}

// TestBlockstoreIOProperties pins the IO switch of a table write: the iceberg-go properties come
// from the standard blockstore.* configuration, which replaced the removed metadata_search.storage
// and metadata_search.azure keys (X6, RA4b).
func TestBlockstoreIOProperties(t *testing.T) {
	tests := []struct {
		name  string
		viper map[string]any
		want  map[string]string
	}{
		{
			name: "s3 with static credentials",
			viper: map[string]any{
				config.BlockstoreTypeKey:                      block.BlockstoreTypeS3,
				"blockstore.s3.region":                        "us-east-1",
				"blockstore.s3.endpoint":                      "http://seaweedfs:8333",
				"blockstore.s3.force_path_style":              true,
				"blockstore.s3.credentials.access_key_id":     testAccessKeyID,
				"blockstore.s3.credentials.secret_access_key": testSecretAccessKey,
				"blockstore.s3.credentials.session_token":     "session",
			},
			want: map[string]string{
				"client.region":               "us-east-1",
				"s3.region":                   "us-east-1",
				"s3.endpoint":                 "http://seaweedfs:8333",
				"s3.access-key-id":            testAccessKeyID,
				"s3.secret-access-key":        testSecretAccessKey,
				"s3.session-token":            "session",
				"s3.force-virtual-addressing": "false",
			},
		},
		{
			name: "azure shared key",
			viper: map[string]any{
				config.BlockstoreTypeKey:              block.BlockstoreTypeAzure,
				"blockstore.azure.storage_account":    "acct",
				"blockstore.azure.storage_access_key": testSecretAccessKey,
			},
			want: map[string]string{
				"adls.auth.shared-key.account.name": "acct",
				"adls.auth.shared-key.account.key":  testSecretAccessKey,
			},
		},
		{
			name: "local has no static IO properties",
			viper: map[string]any{
				config.BlockstoreTypeKey: block.BlockstoreTypeLocal,
				"blockstore.local.path":  t.TempDir(),
			},
			want: nil,
		},
		{
			name: "s3 without static credentials leaves the default chain",
			viper: map[string]any{
				config.BlockstoreTypeKey: block.BlockstoreTypeS3,
				"blockstore.s3.region":   "us-east-1",
			},
			want: map[string]string{
				"client.region": "us-east-1",
				"s3.region":     "us-east-1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			viper.Set("logging.level", "NONE")
			viper.Set("logging.output", "-")
			for key, value := range test.viper {
				viper.Set(key, value)
			}
			cfg := &config.ConfigImpl{}
			_, err := config.NewConfig("", cfg)
			require.NoError(t, err, "build the lakeFS configuration")

			props, err := blockstoreIOProperties(cfg.GetBaseConfig())
			require.NoError(t, err)
			require.Equal(t, test.want, map[string]string(props))
		})
	}
}
