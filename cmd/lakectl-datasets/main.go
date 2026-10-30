// Command lakectl-datasets is the lakectl plugin for lakeFS datasets (DATASETS_DESIGN.md §12.1).
//
// lakectl discovers plugins as `lakectl-<name>` executables on PATH, so this binary is invoked as
// `lakectl-datasets <args...>` when a user runs `lakectl datasets <args...>`, and receives only
// the arguments after `datasets`.
//
// The command tree is registered on lakectl's root command and the command name is put back in
// front of the arguments, so a plugin invocation runs through exactly the same plumbing as
// `lakectl datasets ...`: the configuration file ~/.lakectl.yaml (or --config/LAKECTL_*), the
// credentials providers, the retry policy, the output helpers and the error formatting. The
// datasets control plane itself is called through the transport lakectl configured, see
// cmd/lakectl/cmd.APIClientTransport.
package main

import (
	"os"

	datasetscmd "github.com/treeverse/lakefs/cmd/lakectl-datasets/cmd"
	lakectlcmd "github.com/treeverse/lakefs/cmd/lakectl/cmd"
)

func main() {
	root := lakectlcmd.GetRoot()
	root.AddCommand(datasetscmd.NewRoot())
	// `lakectl datasets list` runs `lakectl-datasets list`, which must behave like
	// `lakectl datasets list`. Cobra parses the same tree either way once the command name is
	// back in front of the arguments.
	root.SetArgs(append([]string{datasetscmd.CommandName}, os.Args[1:]...))
	lakectlcmd.Execute()
}
