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

// setupCmd initial lakeFS system setup - build the database, load initial data and create first superuser
var setupCmd = &cobra.Command{
	Use:     "setup",
	Aliases: []string{"init"},
	Short:   "Setup a new lakeFS instance with initial credentials",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := LoadConfig()
		baseConfig := cfg.GetBaseConfig()

		ctx := cmd.Context()
		logger := logging.FromContext(ctx)
		kvParams, err := kvparams.NewConfig(&baseConfig.Database)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		migrator := kv.NewDatabaseMigrator(kvParams)

		err = migrator.Migrate(ctx)
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}

		if !setupCreatesLocalAdmin(cfg, logger) {
			// nothing to do - users are managed by an external auth service
			return
		}

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

		noCheck, err := cmd.Flags().GetBool("no-check")
		if err != nil {
			fmt.Printf("no-check: %s\n", err)
			os.Exit(1)
		}

		var authService auth.Service
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Printf("Failed to connect to DB: %s", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		authMetadataManager := auth.NewKVMetadataManager(version.Version, baseConfig.Installation.FixedID, baseConfig.Database.Type, kvStore)
		authService = authServiceBuilder(ctx, cfg, logger, kvStore, authMetadataManager)
		metadata := initStatsMetadata(ctx, logger, authMetadataManager, cfg)

		credentials, err := setupLakeFS(ctx, cfg, authMetadataManager, authService, userName, accessKeyID, secretAccessKey, noCheck)
		if err != nil {
			fmt.Printf("Setup failed: %s\n", err)
			os.Exit(1)
		}
		if credentials == nil {
			fmt.Printf("Setup is already complete.\n")
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(ctx)
		collector := stats.NewBufferedCollector(metadata.InstallationID, stats.Config(baseConfig.Stats),
			stats.WithLogger(logger.WithField("service", "stats_collector")))
		collector.Start(ctx)
		defer collector.Close()

		collector.CollectMetadata(metadata)
		collector.CollectEvent(stats.Event{Class: "global", Name: "init"})

		fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
			credentials.AccessKeyID, credentials.SecretAccessKey)

		cancelFn()
	},
}

// setupCreatesLocalAdmin reports whether `setup` should create the initial admin user. A
// genuinely external auth service manages its users elsewhere, so there is nothing to set up.
// The mode is resolved first: with SSO enabled "external" is not supported and the installation
// runs in-process as "simplified" (RBAC_DESIGN.md §4.1), which does need a local initial admin
// (GAP-04).
func setupCreatesLocalAdmin(cfg config.Config, logger logging.Logger) bool {
	return sso.ResolveAuthMode(cfg, logger) != config.AuthRBACExternal
}

func setupLakeFS(ctx context.Context, cfg config.Config, metadataManager auth.MetadataManager, authService auth.Service, userName string, accessKeyID string, secretAccessKey string, noSetupCheck bool) (*model.Credential, error) {
	var (
		err            error
		isCommPrefsSet = false
	)
	if noSetupCheck {
		// check if we already set comm preferences, we like to skip reset in case we already set it
		isCommPrefsSet, err = metadataManager.IsCommPrefsSet(ctx)
		if err != nil && !errors.Is(err, auth.ErrNotFound) {
			return nil, fmt.Errorf("check comm prefs: %w", err)
		}
	} else {
		// check if already initialized
		initialized, err := metadataManager.IsInitialized(ctx)
		if err != nil || initialized {
			// we return nil credentials to indicate setup is already complete
			return nil, err
		}
	}

	if !isCommPrefsSet {
		// mark comm prefs was not provided
		_, err := metadataManager.UpdateCommPrefs(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("update comm prefs: %w", err)
		}
	}

	// The auth-service builder runs before this setup and, when SSO is enabled, it seeds the
	// optional break-glass admin from installation.*. On a fresh KV, run then asks for exactly
	// that admin and access key, and creating them again would fail with "already exists". If
	// that user and key already exist the installation is complete: record the setup time and
	// return the credentials instead of failing. Any other pre-existing user or key still takes
	// the normal path, so the "already exists" error is preserved for genuine conflicts.
	if accessKeyID != "" {
		if _, credErr := authService.GetCredentialsForUser(ctx, userName, accessKeyID); credErr == nil {
			if err := authService.AddUserToGroup(ctx, userName, setup.AdminsGroup); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
				return nil, fmt.Errorf("add user to group - %w", err)
			}
			if err := metadataManager.UpdateSetupTimestamp(ctx, time.Now(), cfg.AuthConfig().GetAuthUIConfig().RBAC); err != nil {
				return nil, fmt.Errorf("update setup timestamp: %w", err)
			}
			return &model.Credential{
				Username: userName,
				BaseCredential: model.BaseCredential{
					AccessKeyID:     accessKeyID,
					SecretAccessKey: secretAccessKey,
				},
			}, nil
		} else if !errors.Is(credErr, auth.ErrNotFound) {
			return nil, fmt.Errorf("check initial admin credentials: %w", credErr)
		}
	}

	// populate initial data and create admin user
	credentials, err := setup.CreateInitialAdminUserWithKeys(ctx, authService, cfg, metadataManager, userName, &accessKeyID, &secretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("create initial admin user: %w", err)
	}
	return credentials, nil
}

const internalErrorCode = 2

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(setupCmd)
	f := setupCmd.Flags()
	f.String("user-name", "", "an identifier for the user (e.g. \"jane.doe\")")
	f.String("access-key-id", "", "AWS-format access key ID to create for that user (for integration)")
	f.String("secret-access-key", "", "AWS-format secret access key to create for that user (for integration)")
	f.Bool("no-check", false, "skip checking if setup is already complete and do anyway")
	if err := f.MarkHidden("access-key-id"); err != nil {
		// (internal error)
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
	if err := f.MarkHidden("secret-access-key"); err != nil {
		// (internal error)
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
	_ = setupCmd.MarkFlagRequired("user-name")
}
