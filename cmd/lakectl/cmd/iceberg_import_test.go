package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/icebergcatalog/importer"
)

// TestIcebergImportCommandIsRegistered pins the command tree the feature adds: `lakectl iceberg
// import` and every flag the documented invocation names.
func TestIcebergImportCommandIsRegistered(t *testing.T) {
	found := false
	for _, command := range icebergCmd.Commands() {
		if command.Name() == "import" {
			found = true
			break
		}
	}
	require.True(t, found, "iceberg import is a subcommand of iceberg")

	for _, flag := range []string{
		icebergImportFlagSourceURI,
		icebergImportFlagSourceCredential,
		icebergImportFlagSourceToken,
		icebergImportFlagSourceWarehouse,
		icebergImportFlagSourceProp,
		icebergImportFlagRepository,
		icebergImportFlagBranch,
		icebergImportFlagNamespace,
		icebergImportFlagAll,
		icebergImportFlagTargetNamespacePrefix,
		icebergImportFlagOverwrite,
		icebergImportFlagDryRun,
		icebergImportFlagCopy,
	} {
		require.NotNil(t, icebergImportCmd.Flags().Lookup(flag), "flag --%s is registered", flag)
	}
}

func TestParseIcebergImportProps(t *testing.T) {
	props, err := parseIcebergImportProps([]string{
		"s3.region=us-east-1",
		"oauth2-server-uri=https://idp.example/token?audience=catalog",
		"prefix = lakefs",
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"s3.region":         "us-east-1",
		"oauth2-server-uri": "https://idp.example/token?audience=catalog",
		"prefix":            "lakefs",
	}, props)

	props, err = parseIcebergImportProps(nil)
	require.NoError(t, err)
	require.Empty(t, props)

	for _, invalid := range []string{"", "no-equals", "=value"} {
		_, err := parseIcebergImportProps([]string{invalid})
		require.ErrorIs(t, err, importer.ErrInvalidOptions, invalid)
	}
}

func TestParseIcebergImportNamespaces(t *testing.T) {
	namespaces, all, err := parseIcebergImportNamespaces([]string{"sales", "sales.eu", "a\x1fb\x1fc"}, false)
	require.NoError(t, err)
	require.False(t, all)
	require.Equal(t, []importer.Namespace{{"sales"}, {"sales", "eu"}, {"a", "b", "c"}}, namespaces)

	namespaces, all, err = parseIcebergImportNamespaces([]string{"sales"}, true)
	require.ErrorIs(t, err, importer.ErrInvalidOptions)
	require.False(t, all)

	namespaces, all, err = parseIcebergImportNamespaces(nil, false)
	require.ErrorIs(t, err, importer.ErrInvalidOptions)
	require.Nil(t, namespaces)

	namespaces, all, err = parseIcebergImportNamespaces(nil, true)
	require.NoError(t, err)
	require.True(t, all)
	require.Empty(t, namespaces)

	for _, invalid := range []string{"sales..eu", ".sales", "sales.", "sales...eu"} {
		_, _, err := parseIcebergImportNamespaces([]string{invalid}, false)
		require.ErrorIs(t, err, importer.ErrInvalidOptions, invalid)
	}
}

func TestParseIcebergImportTargetPrefix(t *testing.T) {
	prefix, err := parseIcebergImportNamespace("imported.2024")
	require.NoError(t, err)
	require.Equal(t, importer.Namespace{"imported", "2024"}, prefix)

	prefix, err = parseIcebergImportNamespace("")
	require.NoError(t, err)
	require.Nil(t, prefix)

	_, err = parseIcebergImportNamespace("a..b")
	require.ErrorIs(t, err, importer.ErrInvalidOptions)
}

func TestLakeFSCatalogURL(t *testing.T) {
	for endpoint, want := range map[string]string{
		"http://lakefs:8000":                   "http://lakefs:8000/iceberg/api",
		"http://lakefs:8000/":                  "http://lakefs:8000/iceberg/api",
		"http://lakefs:8000/api/v1":            "http://lakefs:8000/iceberg/api",
		"https://lakefs.example/prefix/api/v1": "https://lakefs.example/prefix/iceberg/api",
	} {
		got, err := lakeFSCatalogURL(endpoint)
		require.NoError(t, err, endpoint)
		require.Equal(t, want, got, endpoint)
	}

	for _, invalid := range []string{"", "lakefs:8000", "/api/v1"} {
		_, err := lakeFSCatalogURL(invalid)
		require.ErrorIs(t, err, importer.ErrInvalidOptions, invalid)
	}
}
