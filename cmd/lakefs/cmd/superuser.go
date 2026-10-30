package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/version"
)

// errExternalAuth is returned when the effective auth mode is a genuinely external auth
// service: its users are created there, not by this command.
var errExternalAuth = errors.New("external auth service")

// superuserCmd represents the init command
var superuserCmd = &cobra.Command{
	Use:   "superuser",
	Short: "Create additional user with admin credentials",
	Long: `Create additional user with admin credentials.
This command can be used to import an admin user when moving from lakeFS version
with previously configured users to a lakeFS with basic auth version.
To do that provide the user name as well as the access key ID to import.
If the wrong user or credentials were chosen it is possible to delete the user and perform the action again.
`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := LoadConfig()
		baseConfig := cfg.GetBaseConfig()

		userName, err := cmd.Flags().GetString("user-name")
		if err != nil {
			fmt.Printf("user-name: %s\n", err)
			os.Exit(1)
		}
		accessKeyID, err := cmd.Flags().GetString("access-key-id")
		if err != nil {
			fmt.Printf("access-key-id: %s\n", err)
			os.Exit(1)
		}
		secretAccessKey, err := cmd.Flags().GetString("secret-access-key")
		if err != nil {
			fmt.Printf("secret-access-key: %s\n", err)
			os.Exit(1)
		}

		logger := logging.ContextUnavailable()
		ctx := cmd.Context()
		kvParams, err := kvparams.NewConfig(&baseConfig.Database)
		if err != nil {
			fmt.Printf("KV params: %s\n", err)
			os.Exit(1)
		}
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Printf("Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		authMetadataManager := auth.NewKVMetadataManager(version.Version, baseConfig.Installation.FixedID, baseConfig.Database.Type, kvStore)
		metadata := initStatsMetadata(ctx, logger, authMetadataManager, cfg)

		credentials, err := createSuperuser(ctx, cfg, logger, kvStore, authMetadataManager, userName, accessKeyID, secretAccessKey)
		if err != nil {
			if errors.Is(err, errExternalAuth) {
				fmt.Printf("Can't create additional admin while using external auth API - auth.api.endpoint is configured.\n")
				os.Exit(1)
			}
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(ctx)
		collector := stats.NewBufferedCollector(metadata.InstallationID, stats.Config(baseConfig.Stats),
			stats.WithLogger(logger.WithField("service", "stats_collector")))
		collector.Start(ctx)
		defer collector.Close()

		collector.CollectMetadata(metadata)
		collector.CollectEvent(stats.Event{Class: "global", Name: "superuser"})

		fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
			credentials.AccessKeyID, credentials.SecretAccessKey)

		cancelFn()
	},
}

// createSuperuser is the core of the superuser command: it resolves the effective auth mode,
// builds the configured auth service and creates the user with credentials. It is separate from
// the cobra Run so that the mode-dependent behaviour is testable without os.Exit.
func createSuperuser(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, authMetadataManager *auth.KVMetadataManager, userName, accessKeyID, secretAccessKey string) (*model.Credential, error) {
	// Resolve the effective mode before deciding anything from it: under SSO "none" runs as
	// "simplified", so the user must join Admins (GAP-03), and "external" is not supported and
	// runs as "simplified" as well (GAP-04).
	mode := sso.ResolveAuthMode(cfg, logger)
	if mode == config.AuthRBACExternal {
		return nil, errExternalAuth
	}
	authService := authServiceBuilder(ctx, cfg, logger, kvStore, authMetadataManager)
	return setup.AddAdminUser(ctx, authService, &model.SuperuserConfiguration{
		User: model.User{
			CreatedAt: time.Now(),
			Username:  userName,
		},
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}, mode != config.AuthRBACNone)
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(superuserCmd)
	f := superuserCmd.Flags()
	f.String("user-name", "", "an identifier for the user (e.g. \"jane.doe\")")
	f.String("access-key-id", "", "create this access key ID for the user (for ease of integration)")
	f.String("secret-access-key", "", "use this access key secret (potentially insecure, use carefully for ease of integration)")

	_ = superuserCmd.MarkFlagRequired("user-name")
}
