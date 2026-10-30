// Package main is the entry point for the lakefs-sso binary.
// It is identical to the upstream lakefs binary except that it:
//
//  1. Uses acl.AuthService (RBAC "simplified" mode) for multi-user + group support,
//     which is required for Azure Entra ID SSO.
//  2. Will register a NativeOIDCService as the authentication.Service in Phase 1.
//  3. Adds an "sso-migrate" subcommand to migrate an existing single-admin
//     installation from BasicAuthService to acl.AuthService.
//  4. Adds an "rbac-migrate" subcommand to migrate simplified ACL groups to
//     policy-based RBAC groups.
//
// Merge-friendliness: the upstream cmd/lakefs directory is untouched except for
// cmd/lakefs/cmd/hooks.go (new file) and 2 lines in cmd/lakefs/cmd/run.go.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	lakefsCmd "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/datasets"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	logging "github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
)

func main() {
	// Override both service factories with SSO-aware implementations.
	lakefsCmd.SetAuthServiceBuilder(sso.BuildAuthService)
	lakefsCmd.SetAuthenticationServiceBuilder(sso.BuildAuthenticationService)

	// Register the sso-migrate and rbac-migrate subcommands.
	lakefsCmd.GetRoot().AddCommand(newSSOmigrateCmd())
	lakefsCmd.GetRoot().AddCommand(newRBACMigrateCmd())

	// Mount the datasets control plane on /api/v1/datasets (DATASETS_DESIGN.md §4.4). The
	// builder mounts a 501 handler when datasets.enabled is false.
	lakefsCmd.AddExtraRoutesBuilder(datasets.RoutesBuilder)

	lakefsCmd.Execute()
}

// newSSOmigrateCmd returns the "sso-migrate" cobra command that copies the existing
// admin user from the BasicAuthService KV partition into the ACL auth partition.
func newSSOmigrateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sso-migrate",
		Short: "Migrate single-admin install from BasicAuth to ACL auth service",
		Long: `Copies the existing admin user and credentials from the BasicAuthService
KV partition ("basicAuth") to the ACL auth service partition ("auth").
The user is placed in the "Admins" ACL group.
Safe to run multiple times (idempotent).`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := context.Background()
			cfg := lakefsCmd.LoadConfig()
			logger := logging.ContextUnavailable()

			baseCfg := cfg.GetBaseConfig()
			baseAuthCfg := cfg.AuthConfig().GetBaseAuthConfig()
			secretStore := crypt.NewSecretStore([]byte(baseAuthCfg.Encrypt.SecretKey))

			kvParams, err := kvparams.NewConfig(&baseCfg.Database)
			if err != nil {
				return fmt.Errorf("build KV params: %w", err)
			}
			kvStore, err := kv.Open(ctx, kvParams)
			if err != nil {
				return fmt.Errorf("open KV store: %w", err)
			}
			defer kvStore.Close()

			if err = sso.MigrateBasicToACL(ctx, kvStore, secretStore, logger); err != nil {
				return fmt.Errorf("migration: %w", err)
			}
			fmt.Fprintln(os.Stdout, "Migration completed successfully.")
			return nil
		},
	}
}

// newRBACMigrateCmd returns the "rbac-migrate" cobra command that moves an ACL
// (simplified) installation to policy-based RBAC (internal). It is a dry run unless
// --yes is provided and is safe to run repeatedly.
func newRBACMigrateCmd() *cobra.Command {
	var (
		apply   bool
		cleanup bool
	)
	cmd := &cobra.Command{
		Use:   "rbac-migrate",
		Short: "Migrate simplified ACL groups to policy-based RBAC",
		Long: `Migrates an existing simplified (ACL) installation to policy-based RBAC:

  Admins  -> Admins (attach RBAC policies, detach the ACL policy)
  Supers  -> SuperUsers
  Writers -> Developers
  Readers -> Viewers

Members are copied to the RBAC groups; custom groups are left untouched. The command
is a dry run by default. Pass --yes to apply it and --cleanup to delete the emptied
legacy groups and their ACL policies. It is safe to run multiple times.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := context.Background()
			cfg := lakefsCmd.LoadConfig()

			baseCfg := cfg.GetBaseConfig()
			baseAuthCfg := cfg.AuthConfig().GetBaseAuthConfig()
			secretStore := crypt.NewSecretStore([]byte(baseAuthCfg.Encrypt.SecretKey))

			kvParams, err := kvparams.NewConfig(&baseCfg.Database)
			if err != nil {
				return fmt.Errorf("build KV params: %w", err)
			}
			kvStore, err := kv.Open(ctx, kvParams)
			if err != nil {
				return fmt.Errorf("open KV store: %w", err)
			}
			defer kvStore.Close()

			svc := acl.NewAuthService(kvStore, secretStore, authparams.ServiceCache{Enabled: false})
			if err := sso.MigrateACLToRBAC(ctx, svc, sso.RBACMigrationOptions{Apply: apply, Cleanup: cleanup}, cmd.OutOrStdout()); err != nil {
				return fmt.Errorf("rbac migration: %w", err)
			}
			if !apply {
				fmt.Fprintln(cmd.OutOrStdout(), "Dry run only. Re-run with --yes to apply.")
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&apply, "yes", "y", false, "apply the migration (default is a dry run)")
	cmd.Flags().BoolVar(&cleanup, "cleanup", false, "delete emptied legacy groups and their ACL policies")
	return cmd
}
