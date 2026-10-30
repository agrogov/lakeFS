package config_test

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/config"
)

// TestConfigImplAbsorbsDatasetsSection pins the one-field change to oss_config.go: without the
// Datasets field, viper.UnmarshalExact rejects a "datasets:" block as an unknown key, which
// would make every installation with a datasets section fail to start
// (DATASETS_DESIGN.md §4.4, §13). The section is parsed by pkg/datasets, not by pkg/config.
func TestConfigImplAbsorbsDatasetsSection(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.SetConfigType("yaml")
	require.NoError(t, viper.ReadConfig(strings.NewReader(`
datasets:
  enabled: true
  storage_namespace_prefix: "s3://my-bucket/lakefs-datasets"
  some_future_key: 7
`)))

	c := &config.ConfigImpl{}
	require.NoError(t, config.Unmarshal(c), "the datasets: section must not fail UnmarshalExact")

	section, ok := c.Datasets["enabled"]
	require.True(t, ok, "datasets.enabled should be absorbed into ConfigImpl.Datasets")
	require.Equal(t, true, section)
	_, ok = c.Datasets["some_future_key"]
	require.True(t, ok, "the section is kept verbatim, so new keys do not need a pkg/config change")
}
