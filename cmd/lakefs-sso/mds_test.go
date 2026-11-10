package main

// mds_test.go keeps the assertion that the lakefs-sso binary exposes `mds run`.
//
// The command is registered once, on the root command in cmd/lakefs/mdscmd/mds.go, which both
// binaries import: lakefs-sso adds no copy of its own. The RA4b behaviour test runs the command
// through that root command in cmd/lakefs/mds_test.go.

import (
	"testing"

	"github.com/stretchr/testify/require"
	lakefsCmd "github.com/treeverse/lakefs/cmd/lakefs/cmd"
)

func TestMDSRunIsExposedByLakeFSSSO(t *testing.T) {
	root := lakefsCmd.GetRoot()

	cmd, remaining, err := root.Find([]string{"mds", "run"})
	require.NoError(t, err, "the lakefs-sso binary resolves `mds run`")
	require.Empty(t, remaining)
	require.Equal(t, "run", cmd.Name())

	group := cmd.Parent()
	require.NotNil(t, group, "`mds run` is a subcommand of mds")
	require.Equal(t, "mds", group.Name())
	require.Same(t, root, group.Parent(), "lakefs-sso runs the server binary's own mds command")

	// `mds run` reads the root's persistent --config, the flag the official chart passes.
	configFlag := root.PersistentFlags().Lookup("config")
	require.NotNil(t, configFlag, "the root command keeps --config")
	require.Contains(t, configFlag.Usage, "config file")
}
