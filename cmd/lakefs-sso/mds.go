package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds"
)

// newMDSCommand returns the "mds" command group: the Metadata Search indexer
// (METADATA_SEARCH_DESIGN.md §4.1). It is a separate process that needs only HTTP access to
// lakeFS (and, without credential vending, write access to the object store), which is why it is
// a subcommand of the server binary and not a mode of `lakefs-sso run`. Run one replica.
func newMDSCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mds",
		Short: "Metadata Search indexer",
		Long: `Commands of the Metadata Search indexer.

The indexer keeps the system.object_metadata table of every configured data repository R in its
companion repository R-metadata in sync with the commits of R, so that Iceberg engines can query
R.<ref>.system.object_metadata (METADATA_SEARCH_DESIGN.md).`,
	}
	cmd.AddCommand(newMDSRunCommand())
	return cmd
}

// newMDSRunCommand returns "mds run". It reads the top-level metadata_search section of the file
// given with --config, serves /_health, /metrics and /_pprof/ on metadata_search.listen_address,
// and runs until SIGTERM or SIGINT (§4.1, §9).
func newMDSRunCommand() *cobra.Command {
	var cfgFile string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the Metadata Search indexer",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := configureMDSViper(cfgFile); err != nil {
				return err
			}
			cfg, err := mds.LoadConfig()
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			return mds.NewService(cfg, logging.ContextUnavailable()).Run(ctx)
		},
	}
	cmd.Flags().StringVarP(&cfgFile, "config", "c", "",
		"path to the lakeFS configuration file that holds the metadata_search section")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

// configureMDSViper points the global viper at the indexer's configuration file, with the lakeFS
// environment conventions of §7: LAKEFS_METADATA_SEARCH_SECRET_KEY and friends override the file,
// because every key is read with viper.Get* in pkg/mds. Only the metadata_search section is read;
// the file may be a full lakeFS configuration.
func configureMDSViper(cfgFile string) error {
	viper.SetConfigFile(cfgFile)
	viper.SetConfigType("yaml")
	viper.SetEnvPrefix("LAKEFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("read config %s: %w", cfgFile, err)
	}
	return nil
}
