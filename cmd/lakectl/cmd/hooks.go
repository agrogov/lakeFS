package cmd

import "github.com/spf13/cobra"

// GetRoot returns the root command so wrapper binaries (e.g. lakectl-sso) can
// inject additional subcommands before calling Execute().
func GetRoot() *cobra.Command { return rootCmd }
