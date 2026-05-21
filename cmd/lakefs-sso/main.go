// Package main is the entry point for the lakefs-sso binary.
// It is identical to the upstream lakefs binary except that it:
//
//  1. Uses acl.AuthService (RBAC "simplified" mode) for multi-user + group support,
//     which is required for Azure Entra ID SSO.
//  2. Will register a NativeOIDCService as the authentication.Service in Phase 1.
//  3. Adds an "sso-migrate" subcommand to migrate an existing single-admin
//     installation from BasicAuthService to acl.AuthService.
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
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	logging "github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
)

func main() {
	// Override the auth service factory with SSO-aware ACL implementation.
	lakefsCmd.SetAuthServiceBuilder(sso.BuildAuthService)
	// Phase 1: SetAuthenticationServiceBuilder will be wired when NativeOIDCService is ready.

	// Register the sso-migrate subcommand.
	lakefsCmd.GetRoot().AddCommand(newSSOmigrateCmd())

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
