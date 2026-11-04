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
//  5. Mounts the datasets control plane (/api/v1/datasets) and the Apache Iceberg
//     REST catalog (/iceberg/api, /iceberg/relative_to) by registering extra route
//     builders, each of which is disabled unless its config section is enabled, and
//     installs the Iceberg catalog's process-wide controls (the _lakefs_iceberg/ deny
//     policy, the uncommitted-GC grace, and the table-pointer merge-conflict hook)
//     with a third builder that mounts no route, and creates the Metadata Search
//     principals and policies (METADATA_SEARCH_DESIGN.md §8) with a fourth builder that
//     mounts no route either and does nothing unless iceberg.metadata_search.enabled is
//     true.
//
// Merge-friendliness: the upstream cmd/lakefs directory is untouched except for
// cmd/lakefs/cmd/hooks.go (new file) and 2 lines in cmd/lakefs/cmd/run.go.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	lakefsCmd "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/datasets"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	logging "github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/bootstrap"
	"github.com/treeverse/lakefs/pkg/mds/refmap"
	"github.com/treeverse/lakefs/pkg/sso"
)

func main() {
	// Override both service factories with SSO-aware implementations.
	lakefsCmd.SetAuthServiceBuilder(buildAuthService)
	lakefsCmd.SetAuthenticationServiceBuilder(sso.BuildAuthenticationService)

	// Register the sso-migrate, rbac-migrate and mds (Metadata Search indexer) subcommands.
	lakefsCmd.GetRoot().AddCommand(newSSOmigrateCmd())
	lakefsCmd.GetRoot().AddCommand(newRBACMigrateCmd())
	lakefsCmd.GetRoot().AddCommand(newMDSCommand())

	// Mount the datasets control plane on /api/v1/datasets (DATASETS_DESIGN.md §4.4). The
	// builder mounts a 501 handler when datasets.enabled is false.
	lakefsCmd.AddExtraRoutesBuilder(datasets.RoutesBuilder)

	// Mount the Apache Iceberg REST catalog on /iceberg/api (and the relative_to form on
	// /iceberg/relative_to) (ICEBERG_CATALOG_PLAN.md §4.1). The builder mounts the catalog's own
	// 501 handler when iceberg.enabled is false, so the prefixes stay answered in the Iceberg
	// error envelope in either case.
	lakefsCmd.AddExtraRoutesBuilder(icebergcatalog.RoutesBuilder)

	// Install the installation-wide controls that do not live on a route, before the first GC
	// prepare run, the first catalog request and the first merge: the uncommitted-GC grace over the
	// client-written _iceberg/ area, the iceberg-reserved-prefix-protect deny policy over
	// _lakefs_iceberg/, and - when iceberg.ignore_compaction_commits is on - the merge-conflict hook
	// that lets a merge auto-resolve a table-pointer conflict caused only by compactions
	// (ICEBERG_CATALOG_PLAN.md §6.1, §6.3, §6.4). The builder mounts nothing and is a no-op while
	// iceberg.enabled is false, so the plain lakefs binary keeps the upstream GC defaults and
	// reports every merge conflict exactly as upstream does.
	lakefsCmd.AddExtraRoutesBuilder(func(ctx context.Context, deps lakefsCmd.ServerDeps, _ *chi.Mux) error {
		return icebergcatalog.Bootstrap(ctx, deps, guardGroups())
	})

	// Create the Metadata Search principals and policies (METADATA_SEARCH_DESIGN.md §8) when
	// iceberg.metadata_search.enabled is true. The builder mounts no route: it exists so the
	// mds-indexer user, the MetadataSearchIndexers group and the two policies are in place before the
	// first indexer request is authorized.
	lakefsCmd.AddExtraRoutesBuilder(bootstrap.RoutesBuilder)

	// Map the refs of a data repository to the companion repository's refs, so a tag or a commit read
	// through the virtual table resolves to the snapshot the indexer recorded for it
	// (METADATA_SEARCH_DESIGN.md §6).
	icebergcatalog.MetadataSearchRefMapper = refmap.New

	lakefsCmd.Execute()
}

// buildAuthService is sso.BuildAuthService plus, when datasets are enabled, the SEC-2
// immutability guard: an authorizer-level deny of every write action on dataset repositories,
// independent of the groups and policies a principal holds.
func buildAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, mm *auth.KVMetadataManager) auth.Service {
	svc := sso.BuildAuthService(ctx, cfg, logger, kvStore, mm)
	if dcfg, err := datasets.LoadConfig(); err == nil && dcfg.Enabled {
		return datasets.NewImmutabilityAuthService(svc, datasets.NewDatasetRepoSet(kvStore))
	}
	return svc
}

// guardGroups returns the groups that must carry the installation-wide
// iceberg-reserved-prefix-protect deny policy: the base groups of both group templates plus
// datasets.immutable_guard_groups, which is the same set datasets-pins-protect covers. The deny
// keeps a principal that holds fs:* from writing the catalog's own _lakefs_iceberg/ keys through the
// object API and thereby forging catalog state (ICEBERG_CATALOG_PLAN.md §6.1).
//
// It lives here, and not in pkg/icebergcatalog, because pkg/datasets imports that package
// (materialize.go) and the dependency cannot be reversed.
func guardGroups() []string {
	if dcfg, err := datasets.LoadConfig(); err == nil {
		return datasets.PinsProtectGroups(dcfg.ImmutableGuardGroups)
	}
	return datasets.PinsProtectGroups(nil)
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
