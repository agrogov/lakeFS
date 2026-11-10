package main

import (
	"github.com/treeverse/lakefs/cmd/lakefs/cmd"
	_ "github.com/treeverse/lakefs/cmd/lakefs/mdscmd" // registers the "mds" command on the root command
)

func main() {
	cmd.Execute()
}
