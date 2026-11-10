package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/config"
)

func TestRepositoryDeletionStorageDefaults(t *testing.T) {
	cfg := &config.ConfigImpl{}
	baseCfg, err := config.NewConfig("", cfg)
	require.NoError(t, err)

	storage := baseCfg.RepositoryDeletion.Storage
	require.True(t, storage.Enabled, "storage deletion is enabled unless configured otherwise")
	require.Equal(t, 30*time.Second, storage.Interval)
	require.Equal(t, 1000, storage.BatchSize)
	require.Equal(t, 10, storage.MaxAttempts)
}
