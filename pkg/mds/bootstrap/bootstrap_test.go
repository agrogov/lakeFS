package bootstrap

// Tests for the principals and policies of METADATA_SEARCH_DESIGN.md §8: the exact statement
// lists of the design, the create/repair/idempotent behaviour of EnsurePrincipals, the one degraded
// startup it accepts (an auth service without policies, exactly like
// icebergcatalog.EnsureReservedPrefixProtectPolicy), and the ARNs the two policies do *not* decide
// anything about: the data repositories, their branches and their tables (the review's MDS-SEC-2,
// TestMDSPoliciesDoNotReachDataRepositories).
//
// The behaviour is exercised through a real auth service - the simplified-ACL implementation the
// lakefs-sso binary uses - and not a mock, because the questions are "what does a policy written by
// this bootstrap allow" and "is the deny in force", not "which methods were called". The deny is
// checked against the same service that would evaluate it: a Deny statement only protects a
// companion repository if the policy machinery gives it precedence over the administrator's fs:*
// allow, and that is a property of the service, not of this package.
//
// The tests are not parallel: they read the process's viper state (RoutesBuilder), redirect the
// package logger, and share the ACL fixture pattern of pkg/icebergcatalog's tests.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	lakefscatalog "github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/datasets"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/upload"
)

// policyTestRepositories are the repositories the two statement-list tests build their policies
// from: two data repositories, one of them repeated, so the tests pin the order (sorted by companion
// repository) and the deduplication as well as the statements themselves.
var policyTestRepositories = []string{"orders", "analytics", "analytics"}

func TestIndexerPolicyStatements(t *testing.T) {
	t.Parallel()

	// The §8 statement list, spelled as literals rather than as calls to this package's constants,
	// so the design line and the built policy are pinned to each other. The resources of a companion
	// repository are exact ARNs, not the "*\u002dmetadata" patterns of the design: pkg/auth/wildcard
	// lets "*" span "/", so those patterns also matched data-repository ARNs (MDS-SEC-2). The literal
	// repository name in front of a trailing "*" is what makes the subtree ARN safe.
	want := model.Statements{
		{
			Effect: model.StatementEffectAllow,
			Action: []string{
				"fs:ReadRepository", "fs:ListObjects", "fs:ReadObject", "fs:ListBranches",
				"fs:ReadBranch", "fs:ListCommits", "fs:ReadCommit",
			},
			Resource: "*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*"},
			Resource: "arn:lakefs:fs:::repository/analytics-metadata",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*"},
			Resource: "arn:lakefs:fs:::repository/analytics-metadata/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:CreateRepository"},
			Resource: "arn:lakefs:fs:::repository/analytics-metadata",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::namespace/analytics-metadata/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::table/analytics-metadata/*/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::view/analytics-metadata/*/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*"},
			Resource: "arn:lakefs:fs:::repository/orders-metadata",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*"},
			Resource: "arn:lakefs:fs:::repository/orders-metadata/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:CreateRepository"},
			Resource: "arn:lakefs:fs:::repository/orders-metadata",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::namespace/orders-metadata/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::table/orders-metadata/*/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::view/orders-metadata/*/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:AttachStorageNamespace"},
			Resource: "arn:lakefs:fs:::namespace/*-metadata",
		},
	}
	policy := IndexerPolicy(policyTestRepositories, time.Unix(0, 0))
	require.Equal(t, IndexerPolicyName, policy.DisplayName)
	require.True(t, statementsEqual(want, policy.Statement),
		"the mds-indexer policy is the statement list of §8; got %+v", policy.Statement)

	// A server that knows no repository yet grants the reads and no companion-repository write: it
	// must not fall back to a pattern.
	empty := IndexerPolicy(nil, time.Unix(0, 0))
	require.Len(t, empty.Statement, 2, "no repository, no companion statement: %+v", empty.Statement)
	require.Equal(t, "*", empty.Statement[0].Resource)
	require.Equal(t, MetadataNamespacePattern, empty.Statement[1].Resource)
}

func TestMetadataRepoProtectPolicyStatements(t *testing.T) {
	t.Parallel()

	// The deny statement list of §8: fs:Write*/Delete*/Create*/Revert*/Update*/Import* and the catalog
	// writes on the companion repositories. Reads are deliberately not denied. fs:Update* is the
	// review's MDS-SEC-5: fs:UpdateRepository writes a repository's metadata and matched none of the
	// other four groups. fs:Import* is the import pair of pkg/api/controller.go: ImportCancel is
	// authorized on a branch ARN of the companion repository, so the subtree statement denies it;
	// ImportFromStorage is authorized on a source storage namespace and is therefore not reached by
	// these repository statements (see metadataRepoProtectFSDeniedActions). The resources are the
	// exact ARNs of the companion repositories of policyTestRepositories (MDS-SEC-2).
	fsActions := []string{"fs:Write*", "fs:Delete*", "fs:Create*", "fs:Revert*", "fs:Update*", "fs:Import*"}
	catalogActions := []string{"catalog:Create*", "catalog:Update*", "catalog:Delete*", "catalog:WriteTableData"}
	want := model.Statements{
		{
			Effect:   model.StatementEffectDeny,
			Action:   fsActions,
			Resource: "arn:lakefs:fs:::repository/analytics-metadata",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   fsActions,
			Resource: "arn:lakefs:fs:::repository/analytics-metadata/*",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   catalogActions,
			Resource: "arn:lakefs:catalog:::namespace/analytics-metadata/*",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   catalogActions,
			Resource: "arn:lakefs:catalog:::table/analytics-metadata/*/*",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   catalogActions,
			Resource: "arn:lakefs:catalog:::view/analytics-metadata/*/*",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   fsActions,
			Resource: "arn:lakefs:fs:::repository/orders-metadata",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   fsActions,
			Resource: "arn:lakefs:fs:::repository/orders-metadata/*",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   catalogActions,
			Resource: "arn:lakefs:catalog:::namespace/orders-metadata/*",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   catalogActions,
			Resource: "arn:lakefs:catalog:::table/orders-metadata/*/*",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   catalogActions,
			Resource: "arn:lakefs:catalog:::view/orders-metadata/*/*",
		},
	}
	policy := MetadataRepoProtectPolicy(policyTestRepositories, time.Unix(0, 0))
	require.Equal(t, MetadataRepoProtectPolicyName, policy.DisplayName)
	require.True(t, statementsEqual(want, policy.Statement),
		"the mds-metadata-repo-protect policy is the deny list of §8; got %+v", policy.Statement)

	require.Empty(t, MetadataRepoProtectPolicy(nil, time.Unix(0, 0)).Statement,
		"no repository the server knows, nothing to deny: the deny never falls back to a pattern")
}

// bootstrapTS is a fixed creation timestamp, so the idempotency assertions compare exact values.
var bootstrapTS = time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC)

func TestEnsurePrincipals(t *testing.T) {
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err, "one group of the deny's list exists in this installation")

	inForce, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), policyTestRepositories)
	require.NoError(t, err)
	require.True(t, inForce, "a backend that stores policies has the principals in force")

	// The user, its membership and the group.
	_, err = svc.GetUser(t.Context(), UserName)
	require.NoError(t, err, "EnsurePrincipals creates the mds-indexer user")
	groups, _, err := svc.ListUserGroups(t.Context(), UserName, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.Contains(t, groupNames(groups), GroupName, "mds-indexer is a member of MetadataSearchIndexers")

	// Both policies exist with exactly the built-in statements.
	for _, built := range []*model.Policy{
		IndexerPolicy(policyTestRepositories, bootstrapTS),
		MetadataRepoProtectPolicy(policyTestRepositories, bootstrapTS),
	} {
		stored, err := svc.GetPolicy(t.Context(), built.DisplayName)
		require.NoErrorf(t, err, "policy %s exists", built.DisplayName)
		require.Truef(t, statementsEqual(built.Statement, stored.Statement),
			"policy %s has the built-in statements; got %+v", built.DisplayName, stored.Statement)
	}

	// mds-indexer is attached to its group and nowhere else, in particular not to a base group.
	indexerPolicies := policyNames(t, svc, GroupName)
	require.ElementsMatch(t, []string{IndexerPolicyName}, indexerPolicies,
		"MetadataSearchIndexers carries mds-indexer and only mds-indexer")

	// The deny is attached to the base group this installation has...
	require.Contains(t, policyNames(t, svc, "Admins"), MetadataRepoProtectPolicyName,
		"the deny is attached to the base groups, as §8 requires")
	// ... and never to the indexer group: attaching it there would disarm the indexer.
	require.NotContains(t, indexerPolicies, MetadataRepoProtectPolicyName,
		"the deny must never be attached to MetadataSearchIndexers")
}

// TestEnsurePrincipalsConfiguredGuardGroups is the regression test for the guard list coming from
// cmd/lakefs-sso: guardGroups() is datasets.PinsProtectGroups(datasets.immutable_guard_groups), the
// same list icebergcatalog.Bootstrap attaches iceberg-reserved-prefix-protect to (§8). The bootstrap
// used to recompute datasets.PinsProtectGroups(nil) itself, so an operator's configured extra group
// got the reserved-prefix deny but no metadata-repo deny at all.
func TestEnsurePrincipalsConfiguredGuardGroups(t *testing.T) {
	svc := newACLService(t)
	const extraGuardGroup = "metadata-operators"
	for _, group := range []string{"Admins", extraGuardGroup, GroupName} {
		_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: group, CreatedAt: bootstrapTS})
		require.NoError(t, err)
	}

	// Exactly what cmd/lakefs-sso/main.go passes after guardGroups() read
	// datasets.immutable_guard_groups: [metadata-operators]. MetadataSearchIndexers is appended to
	// prove the bootstrap filters it out even when an operator lists it there.
	groups := datasets.PinsProtectGroups([]string{extraGuardGroup, GroupName})

	readLog := capturePackageLog(t)
	inForce, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, groups, nil)
	require.NoError(t, err)
	require.True(t, inForce)

	require.Contains(t, policyNames(t, svc, extraGuardGroup), MetadataRepoProtectPolicyName,
		"a configured extra guard group gets the deny exactly like a base group")
	require.Contains(t, policyNames(t, svc, "Admins"), MetadataRepoProtectPolicyName)
	require.NotContains(t, policyNames(t, svc, GroupName), MetadataRepoProtectPolicyName,
		"the deny is never attached to MetadataSearchIndexers, even when the guard list names it")

	output := readLog()
	require.Contains(t, output, "level=warning", "dropping the indexer group from the guard list is logged")
	require.Contains(t, output, GroupName)
}

// TestEnsurePrincipalsAuthorization checks what the two policies actually decide, through the same
// ACL engine the server uses: the indexer writes companion repositories and only reads data
// repositories, and an administrator's fs:* does not write a companion repository.
func TestEnsurePrincipalsAuthorization(t *testing.T) {
	const dataRepo = "analytics"
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	// analytics is a repository the server knows: its companion repository is what the policies name.
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), []string{dataRepo})
	require.NoError(t, err)

	// An administrator: a member of Admins (so the deny applies) with an installation-wide allow,
	// the §8 "wins over every allow" case.
	operator := newOperatorPrincipal(t, svc)

	const (
		metaRepo   = dataRepo + icebergcatalog.MetadataRepositorySuffix
		objectKey  = "_iceberg/table-uuid/data/part-00000.parquet"
		tableARN   = "arn:lakefs:catalog:::table/" + metaRepo + "/system/object_metadata"
		metadataRN = "arn:lakefs:catalog:::table/" + dataRepo + "/system/object_metadata"
	)
	tests := []struct {
		name     string
		username string
		action   string
		resource string
		want     bool
	}{
		{
			name:     "indexer reads a data repository",
			username: UserName, action: permissions.ReadObjectAction,
			resource: permissions.ObjectArn(dataRepo, "a.parquet"), want: true,
		},
		{
			name:     "indexer does not write a data repository",
			username: UserName, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "a.parquet"), want: false,
		},
		{
			name:     "indexer writes a companion repository object",
			username: UserName, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(metaRepo, objectKey), want: true,
		},
		{
			name:     "indexer creates a commit on a companion repository branch",
			username: UserName, action: permissions.CreateCommitAction,
			resource: permissions.BranchArn(metaRepo, "main"), want: true,
		},
		{
			name:     "indexer creates the companion repository",
			username: UserName, action: permissions.CreateRepositoryAction,
			resource: permissions.RepoArn(metaRepo), want: true,
		},
		{
			name:     "indexer attaches the derived companion storage namespace",
			username: UserName, action: permissions.AttachStorageNamespaceAction,
			resource: permissions.StorageNamespace("s3://bucket/" + dataRepo + "-metadata"), want: true,
		},
		{
			name:     "indexer does not attach a data repository storage namespace",
			username: UserName, action: permissions.AttachStorageNamespaceAction,
			resource: permissions.StorageNamespace("s3://bucket/" + dataRepo), want: false,
		},
		{
			name:     "indexer does not create a data repository",
			username: UserName, action: permissions.CreateRepositoryAction,
			resource: permissions.RepoArn(dataRepo), want: false,
		},
		{
			name:     "indexer creates a table in the companion repository",
			username: UserName, action: permissions.CatalogCreateTableAction,
			resource: tableARN, want: true,
		},
		{
			name:     "indexer does not create a table in a data repository",
			username: UserName, action: permissions.CatalogCreateTableAction,
			resource: metadataRN, want: false,
		},
		{
			name:     "an administrator still writes a data repository",
			username: operator, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "a.parquet"), want: true,
		},
		{
			name:     "the deny beats an administrator's fs:* on a companion repository",
			username: operator, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(metaRepo, objectKey), want: false,
		},
		{
			name:     "the deny beats an administrator's catalog:* on a companion repository",
			username: operator, action: permissions.CatalogCreateTableAction,
			resource: tableARN, want: false,
		},
		{
			name:     "the deny does not block reads of a companion repository",
			username: operator, action: permissions.ReadObjectAction,
			resource: permissions.ObjectArn(metaRepo, objectKey), want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := svc.Authorize(t.Context(), &auth.AuthorizationRequest{
				Username: tt.username,
				RequiredPermissions: permissions.Node{Permission: permissions.Permission{
					Action:   tt.action,
					Resource: tt.resource,
				}},
			})
			require.NoError(t, err)
			require.Equal(t, tt.want, resp.Allowed,
				"%s on %s for %s", tt.action, tt.resource, tt.username)
		})
	}
}

// newOperatorPrincipal creates the §8 "an administrator's fs:* does not write a companion
// repository" principal: a member of the guard group Admins (so the metadata-repo deny is attached to
// it) with an installation-wide fs:* and catalog:* allow of its own.
func newOperatorPrincipal(t *testing.T, svc auth.Service) string {
	t.Helper()
	const username = "operator"
	_, err := svc.CreateUser(t.Context(), &model.User{Username: username, CreatedAt: bootstrapTS})
	require.NoError(t, err)
	require.NoError(t, svc.AddUserToGroup(t.Context(), username, "Admins"))
	require.NoError(t, svc.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: "operator-fs-all",
		Statement: model.Statements{
			{Effect: model.StatementEffectAllow, Action: []string{"fs:*"}, Resource: permissions.All},
			{Effect: model.StatementEffectAllow, Action: []string{"catalog:*"}, Resource: permissions.All},
		},
	}, false))
	require.NoError(t, svc.AttachPolicyToUser(t.Context(), "operator-fs-all", username))
	return username
}

// TestMDSPoliciesDoNotReachDataRepositories is the regression test for MDS-SEC-2: the two §8
// policies name companion repositories, and must not decide anything about a data repository, a
// branch of one, or a table in one - including the adversarial ARNs whose key, branch name or
// namespace ends in "-metadata" or carries "-metadata/" in the middle.
//
// The two policy patterns of the design leaned on "*" spanning "/" (pkg/auth/wildcard), so
// repository/<R>/object/-metadata matched repository/*-metadata and
// repository/<R>/object/a/b-metadata/object/c matched repository/*-metadata/object/*: the indexer
// (which §8 restricts to reads on data repositories) could write and delete data-repository objects,
// and the deny made an ordinary foo-metadata/data.yml object of a data repository unwritable for a
// guard-group member. Every ARN below is checked against the real ACL service the server uses, for
// both principals the policies are about.
func TestMDSPoliciesDoNotReachDataRepositories(t *testing.T) {
	const (
		dataRepo = "analytics"
		metaRepo = dataRepo + icebergcatalog.MetadataRepositorySuffix
	)
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	// analytics is a repository the server knows: the policies are built from it.
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), []string{dataRepo})
	require.NoError(t, err)
	operator := newOperatorPrincipal(t, svc)

	tests := []struct {
		name     string
		username string
		action   string
		resource string
		want     bool
	}{
		// mds-indexer: §8 gives it reads on every data repository and writes on companion
		// repositories only, so the adversarial data-repository ARNs must all be refused.
		{
			name:     "indexer does not write an object named -metadata in a data repository",
			username: UserName, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "-metadata"), want: false,
		},
		{
			name:     "indexer does not delete an object named -metadata in a data repository",
			username: UserName, action: permissions.DeleteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "-metadata"), want: false,
		},
		{
			name:     "indexer does not write a foo-metadata key in a data repository",
			username: UserName, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "foo-metadata/data.yml"), want: false,
		},
		{
			name:     "indexer does not delete a foo-metadata key in a data repository",
			username: UserName, action: permissions.DeleteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "foo-metadata/data.yml"), want: false,
		},
		{
			name:     "indexer does not write a key with -metadata in the middle in a data repository",
			username: UserName, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "a/b-metadata/object/c"), want: false,
		},
		{
			name:     "indexer does not delete a branch named -metadata in a data repository",
			username: UserName, action: permissions.DeleteBranchAction,
			resource: permissions.BranchArn(dataRepo, "x-metadata"), want: false,
		},
		{
			name:     "indexer does not create a table in a -metadata namespace of a data repository",
			username: UserName, action: permissions.CatalogCreateTableAction,
			resource: permissions.CatalogTableArn(dataRepo, "foo-metadata", "orders"), want: false,
		},
		{
			name:     "indexer does not drop a table in a -metadata namespace of a data repository",
			username: UserName, action: permissions.CatalogDeleteTableAction,
			resource: permissions.CatalogTableArn(dataRepo, dataRepo+"-metadata", "orders"), want: false,
		},
		{
			name:     "indexer still reads an object named -metadata in a data repository",
			username: UserName, action: permissions.ReadObjectAction,
			resource: permissions.ObjectArn(dataRepo, "-metadata"), want: true,
		},

		// mds-indexer on the companion repository itself: unaffected by the fix.
		{
			name:     "indexer writes a companion repository object",
			username: UserName, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(metaRepo, "_iceberg/uuid/data/part-00000.parquet"), want: true,
		},
		{
			name:     "indexer deletes a companion repository branch named -metadata",
			username: UserName, action: permissions.DeleteBranchAction,
			resource: permissions.BranchArn(metaRepo, "x-metadata"), want: true,
		},
		{
			name:     "indexer creates a table in a -metadata namespace of the companion repository",
			username: UserName, action: permissions.CatalogCreateTableAction,
			resource: permissions.CatalogTableArn(metaRepo, "system-metadata", "orders"), want: true,
		},
		{
			name:     "indexer updates the companion repository",
			username: UserName, action: permissions.UpdateRepositoryAction,
			resource: permissions.RepoArn(metaRepo), want: true,
		},

		// A guard-group member with an installation-wide fs:* and catalog:* allow: the deny must not
		// take an ordinary data repository away from it (MDS-SEC-2).
		{
			name:     "the deny does not block an object named -metadata in a data repository",
			username: operator, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "-metadata"), want: true,
		},
		{
			name:     "the deny does not block deleting an object named -metadata in a data repository",
			username: operator, action: permissions.DeleteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "-metadata"), want: true,
		},
		{
			name:     "the deny does not block a foo-metadata key in a data repository",
			username: operator, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "foo-metadata/data.yml"), want: true,
		},
		{
			name:     "the deny does not block a key with -metadata in the middle in a data repository",
			username: operator, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "a/b-metadata/object/c"), want: true,
		},
		{
			name:     "the deny does not block a branch named -metadata in a data repository",
			username: operator, action: permissions.DeleteBranchAction,
			resource: permissions.BranchArn(dataRepo, "x-metadata"), want: true,
		},
		{
			name:     "the deny does not block a table in a -metadata namespace of a data repository",
			username: operator, action: permissions.CatalogCreateTableAction,
			resource: permissions.CatalogTableArn(dataRepo, "foo-metadata", "orders"), want: true,
		},
		{
			name:     "the deny does not block a table in a namespace named after the companion repository",
			username: operator, action: permissions.CatalogDeleteTableAction,
			resource: permissions.CatalogTableArn(dataRepo, metaRepo, "orders"), want: true,
		},
		{
			name:     "the deny does not block updating a data repository",
			username: operator, action: permissions.UpdateRepositoryAction,
			resource: permissions.RepoArn(dataRepo), want: true,
		},

		// The companion repository is still covered by the deny (MDS-SEC-5 adds
		// fs:UpdateRepository to it).
		{
			name:     "the deny blocks a companion repository object",
			username: operator, action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(metaRepo, "forged/row.txt"), want: false,
		},
		{
			name:     "the deny blocks deleting a companion repository object",
			username: operator, action: permissions.DeleteObjectAction,
			resource: permissions.ObjectArn(metaRepo, "forged/row.txt"), want: false,
		},
		{
			name:     "the deny blocks a companion repository branch",
			username: operator, action: permissions.DeleteBranchAction,
			resource: permissions.BranchArn(metaRepo, "main"), want: false,
		},
		{
			name:     "the deny blocks a table in a companion repository namespace",
			username: operator, action: permissions.CatalogCreateTableAction,
			resource: permissions.CatalogTableArn(metaRepo, "system", "object_metadata"), want: false,
		},
		{
			name:     "the deny blocks a view in a companion repository",
			username: operator, action: permissions.CatalogCreateViewAction,
			resource: permissions.CatalogViewArn(metaRepo, "system", "forged"), want: false,
		},
		{
			name:     "the deny blocks a namespace in a companion repository",
			username: operator, action: permissions.CatalogCreateNamespaceAction,
			resource: permissions.CatalogNamespaceArn(metaRepo, "forged"), want: false,
		},
		{
			name:     "the deny blocks updating the companion repository (MDS-SEC-5)",
			username: operator, action: permissions.UpdateRepositoryAction,
			resource: permissions.RepoArn(metaRepo), want: false,
		},
		{
			name:     "the deny still allows reading a companion repository",
			username: operator, action: permissions.ReadObjectAction,
			resource: permissions.ObjectArn(metaRepo, "system/object_metadata/data.parquet"), want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := svc.Authorize(t.Context(), &auth.AuthorizationRequest{
				Username: tt.username,
				RequiredPermissions: permissions.Node{Permission: permissions.Permission{
					Action:   tt.action,
					Resource: tt.resource,
				}},
			})
			require.NoError(t, err)
			require.Equal(t, tt.want, resp.Allowed,
				"%s on %s for %s", tt.action, tt.resource, tt.username)
		})
	}
}

// TestMetadataRepoProtectCoversImportActions pins the import pair through the real ACL service.
// ImportStart (pkg/api/controller.go) authorizes fs:WriteObject and fs:CreateCommit on the
// destination branch plus, per source, fs:ImportFromStorage on the source storage namespace and
// fs:WriteObject on the destination object; ImportCancel authorizes fs:ImportCancel on the
// destination branch. Before fs:Import* joined the deny list a guard-group member could cancel an
// import on a companion branch, because fs:ImportCancel matched none of Write*/Delete*/Create*/
// Revert*/Update*. Import into a companion branch was already denied - its destination permissions
// are fs:Write* and fs:Create* - and both actions stay allowed on an ordinary data repository and for
// the indexer on its own companion repositories.
//
// The ImportFromStorage half of the decision is pinned here as well: an import whose source is a
// companion repository's storage namespace but whose destination is a data repository stays allowed,
// because it writes the data repository and only copies files out of the companion one - the read
// that the deny deliberately leaves open (metadataRepoProtectFSDeniedActions explains why a namespace
// statement is neither needed nor exact).
func TestMetadataRepoProtectCoversImportActions(t *testing.T) {
	const (
		dataRepo           = "analytics"
		metaRepo           = dataRepo + icebergcatalog.MetadataRepositorySuffix
		companionNamespace = "s3://bucket/" + metaRepo
		branch             = "main"
	)
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	// analytics is a repository the server knows: the policies name analytics-metadata.
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), []string{dataRepo})
	require.NoError(t, err)
	operator := newOperatorPrincipal(t, svc)

	sources := []importSource{{path: "s3://bucket/import", destination: "imported/a.parquet"}}

	// ImportCancel on a companion branch is the case the new fs:Import* statement covers.
	requireAuthorization(t, svc, operator, permissions.ImportCancelAction, permissions.BranchArn(metaRepo, branch), false,
		"a guard-group member must not cancel an import on a companion branch")
	requireAuthorization(t, svc, operator, permissions.ImportCancelAction, permissions.BranchArn(dataRepo, branch), true,
		"an ordinary data repository is outside the deny")
	requireAuthorization(t, svc, UserName, permissions.ImportCancelAction, permissions.BranchArn(metaRepo, branch), true,
		"the deny must never reach the indexer")

	// ImportStart's node, exactly as pkg/api/controller.go builds it.
	requireNodeAuthorization(t, svc, operator, importPermissionNode(metaRepo, branch, sources), false,
		"a guard-group member must not import into a companion branch")
	requireNodeAuthorization(t, svc, operator, importPermissionNode(dataRepo, branch, sources), true,
		"import into an ordinary data repository is outside the deny")

	// The indexer keeps the companion-side permissions an import into its own companion repository
	// needs: it is in no guard group and carries fs:* on the companion repository.
	for _, permission := range []permissions.Permission{
		{Action: permissions.WriteObjectAction, Resource: permissions.BranchArn(metaRepo, branch)},
		{Action: permissions.CreateCommitAction, Resource: permissions.BranchArn(metaRepo, branch)},
		{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn(metaRepo, "imported/a.parquet")},
		{Action: permissions.ImportCancelAction, Resource: permissions.BranchArn(metaRepo, branch)},
	} {
		requireNodeAuthorization(t, svc, UserName, permissions.Node{Permission: permission}, true,
			"the deny must not take the indexer's own companion repository away from it")
	}

	// The decision about fs:ImportFromStorage: it is authorized on the source storage namespace, a
	// resource outside the companion repository ARNs, and the copy-out it performs is the read §8's
	// deny does not take away. A destination that *is* a companion repository stays denied.
	fromCompanion := []importSource{{path: companionNamespace, destination: "copied/a.parquet"}}
	requireAuthorization(t, svc, operator, permissions.ImportFromStorageAction,
		permissions.StorageNamespace(companionNamespace), true,
		"fs:ImportFromStorage is authorized on the source namespace, not on a companion repository ARN")
	requireNodeAuthorization(t, svc, operator, importPermissionNode(dataRepo, branch, fromCompanion), true,
		"importing a companion repository's files into a data repository writes only the data repository")
	requireNodeAuthorization(t, svc, operator, importPermissionNode(metaRepo, branch, fromCompanion), false,
		"an import into a companion repository stays denied whatever its source")
}

func TestEnsurePrincipalsIdempotent(t *testing.T) {
	svc := newACLService(t)

	_, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), nil)
	require.NoError(t, err)
	first, err := svc.GetPolicy(t.Context(), IndexerPolicyName)
	require.NoError(t, err)

	// A second startup, one hour later: nothing is re-created, so the policy keeps its original
	// creation timestamp and its statements.
	later := bootstrapTS.Add(time.Hour)
	inForce, err := EnsurePrincipals(t.Context(), svc, later, defaultGuardGroups(), nil)
	require.NoError(t, err, "a rerun is a success, not an already-exists error")
	require.True(t, inForce)
	second, err := svc.GetPolicy(t.Context(), IndexerPolicyName)
	require.NoError(t, err)
	require.Equal(t, first.CreatedAt, second.CreatedAt, "an unchanged policy is not rewritten")
	require.True(t, statementsEqual(first.Statement, second.Statement))

	// The attachments were not duplicated either (the ACL service stores one entry per pair, and a
	// duplicate would fail the assertion through a repeated name).
	require.ElementsMatch(t, []string{IndexerPolicyName}, policyNames(t, svc, GroupName))
}

func TestEnsurePrincipalsRepairsDriftedPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy *model.Policy
	}{
		{
			name: "allow policy with a wildcard grant",
			policy: &model.Policy{
				CreatedAt:   bootstrapTS,
				DisplayName: IndexerPolicyName,
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:*"},
					Resource: permissions.All,
				}},
			},
		},
		{
			name: "deny policy with no statements",
			policy: &model.Policy{
				CreatedAt:   bootstrapTS,
				DisplayName: MetadataRepoProtectPolicyName,
				Statement:   model.Statements{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newACLService(t)
			require.NoError(t, svc.WritePolicy(t.Context(), tt.policy, false /* create */),
				"a predictable policy name can be pre-created")

			readLog := capturePackageLog(t)
			inForce, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), policyTestRepositories)
			require.NoError(t, err)
			require.True(t, inForce)

			want := IndexerPolicy(policyTestRepositories, bootstrapTS)
			if tt.policy.DisplayName == MetadataRepoProtectPolicyName {
				want = MetadataRepoProtectPolicy(policyTestRepositories, bootstrapTS)
			}
			stored, err := svc.GetPolicy(t.Context(), tt.policy.DisplayName)
			require.NoError(t, err)
			require.True(t, statementsEqual(want.Statement, stored.Statement),
				"a drifted policy is rewritten to the built-in statements; got %+v", stored.Statement)

			output := readLog()
			require.Contains(t, output, "level=warning", "the repair is logged, not silent")
			require.Contains(t, output, "rewrote a metadata search policy")
			require.Contains(t, output, tt.policy.DisplayName)
		})
	}
}

// TestEnsurePrincipalsDegradesWithoutPolicyBackend drives the documented default auth mode: a
// BasicAuthService cannot store policies, so Metadata Search's principals cannot be created. The
// startup must continue with a warning naming the consequence - and must not rename the
// administrator, which BasicAuthService.CreateUser would do if it were called.
func TestEnsurePrincipalsDegradesWithoutPolicyBackend(t *testing.T) {
	store := kvtest.GetStore(t.Context(), t)
	basic := auth.NewBasicAuthService(store, crypt.NewSecretStore([]byte("mds-bootstrap-basic-secret")),
		authparams.ServiceCache{Enabled: false}, logging.DummyLogger{})
	_, err := basic.CreateUser(t.Context(), &model.User{Username: "admin", CreatedAt: bootstrapTS})
	require.NoError(t, err)

	readLog := capturePackageLog(t)
	inForce, err := EnsurePrincipals(t.Context(), basic, bootstrapTS, defaultGuardGroups(), nil)
	require.NoError(t, err, "a service that cannot store policies must not refuse to start Metadata Search")
	require.False(t, inForce, "nothing is in force on a backend without policies")

	// The administrator is untouched: the probe ran before CreateUser.
	admin, err := basic.GetUser(t.Context(), "admin")
	require.NoError(t, err)
	require.Equal(t, "admin", admin.Username, "the installation's administrator was not renamed")
	_, err = basic.GetUser(t.Context(), UserName)
	require.Error(t, err, "mds-indexer is not created on a backend that cannot authorize it")

	output := readLog()
	require.Contains(t, output, "level=warning", "the degradation is logged loudly, not silently")
	require.Contains(t, output, "NOT created", "the warning says which principals were not created")
	require.Contains(t, output, UserName)
	require.Contains(t, output, GroupName)
	require.Contains(t, output, IndexerPolicyName)
	require.Contains(t, output, MetadataRepoProtectPolicyName)
	require.Contains(t, output, auth.ErrNotImplemented.Error(), "the warning carries the reason")
}

// policyStub is an auth.Service whose only implemented method is the one EnsurePrincipals calls
// first: GetPolicy. The embedded nil interface is deliberate - a method the bootstrap does not
// expect to reach panics loudly instead of pretending to work.
type policyStub struct {
	auth.Service
	policyErr error
}

func (s policyStub) GetPolicy(context.Context, string) (*model.Policy, error) {
	return nil, s.policyErr
}

// TestEnsurePrincipalsPolicyErrorIsFatal covers the other half of the degradation contract: a
// backend that does store policies and failed must still fail startup.
func TestEnsurePrincipalsPolicyErrorIsFatal(t *testing.T) {
	genuine := errors.New("kv: connection refused")
	_, err := EnsurePrincipals(t.Context(), policyStub{policyErr: fmt.Errorf("get policy: %w", genuine)}, bootstrapTS, defaultGuardGroups(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, genuine, "the real cause is preserved")
	require.EqualError(t, err, fmt.Sprintf("mds bootstrap: read policy %s: get policy: %v", IndexerPolicyName, genuine),
		"a real error is not softened into a warning")
}

func TestEnsurePrincipalsNoAuthService(t *testing.T) {
	_, err := EnsurePrincipals(t.Context(), nil, bootstrapTS, defaultGuardGroups(), nil)
	require.ErrorIs(t, err, ErrNoAuthorizer)
}

// TestRoutesBuilder covers the builder cmd/lakefs-sso registers: it runs only while
// iceberg.metadata_search.enabled is true, mounts nothing, and degrades like EnsurePrincipals.
func TestRoutesBuilder(t *testing.T) {
	t.Run("disabled section does nothing", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n")
		svc := newACLService(t)
		require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: svc, Logger: logging.DummyLogger{}}, defaultGuardGroups()),
			"a disabled Metadata Search section is not an error")
		_, err := svc.GetUser(t.Context(), UserName)
		require.ErrorIs(t, err, auth.ErrNotFound, "no principal is created while Metadata Search is off")
		_, err = svc.GetPolicy(t.Context(), IndexerPolicyName)
		require.ErrorIs(t, err, auth.ErrNotFound, "no policy is created while Metadata Search is off")
	})

	t.Run("explicitly disabled section does nothing", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: false\n")
		svc := newACLService(t)
		require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: svc, Logger: logging.DummyLogger{}}, defaultGuardGroups()))
		_, err := svc.GetUser(t.Context(), UserName)
		require.ErrorIs(t, err, auth.ErrNotFound)
	})

	t.Run("enabled section bootstraps the principals", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
		svc := newACLService(t)
		cat := newBootstrapCatalog(t, "analytics")
		_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
		require.NoError(t, err)
		require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{
			AuthService: svc, Logger: logging.DummyLogger{}, Catalog: cat,
		}, defaultGuardGroups()))
		_, err = svc.GetUser(t.Context(), UserName)
		require.NoError(t, err)
		_, err = svc.GetPolicy(t.Context(), MetadataRepoProtectPolicyName)
		require.NoError(t, err, "both policies are created")
		require.Contains(t, policyNames(t, svc, "Admins"), MetadataRepoProtectPolicyName,
			"the builder forwards the guard group list it is handed, as cmd/lakefs-sso does")

		// The repository the catalog holds is the one the policies name.
		deny, err := svc.GetPolicy(t.Context(), MetadataRepoProtectPolicyName)
		require.NoError(t, err)
		require.True(t, policyNamesRepository(deny, "analytics"+icebergcatalog.MetadataRepositorySuffix),
			"the deny names the companion repository of the repository the server knows; got %+v", deny.Statement)
	})

	t.Run("enabled section without dependencies is an error", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
		require.ErrorIs(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{Logger: logging.DummyLogger{}}, defaultGuardGroups()),
			ErrMissingDeps, "a missing auth service must not be skipped silently")
		require.ErrorIs(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: newACLService(t)}, defaultGuardGroups()),
			ErrMissingDeps, "a missing logger must not be skipped silently")
		require.ErrorIs(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: newACLService(t), Logger: logging.DummyLogger{}}, defaultGuardGroups()),
			ErrMissingDeps, "a missing catalog must not be skipped silently: the policies would name no companion repository")
	})

	t.Run("enabled section on a backend without policies degrades", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
		store := kvtest.GetStore(t.Context(), t)
		basic := auth.NewBasicAuthService(store, crypt.NewSecretStore([]byte("mds-bootstrap-basic-secret")),
			authparams.ServiceCache{Enabled: false}, logging.DummyLogger{})
		require.NoError(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{
				AuthService: basic, Logger: logging.DummyLogger{}, Catalog: newBootstrapCatalog(t),
			}, defaultGuardGroups()),
			"the documented default auth mode must still start the server")
	})

	t.Run("invalid configuration is fatal", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  prefix: not-lakefs\n  metadata_search:\n    enabled: true\n")
		require.Error(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{
			AuthService: newACLService(t), Logger: logging.DummyLogger{},
		}, defaultGuardGroups()),
			"a configuration error is reported, not hidden behind a disabled builder")
	})
}

// TestKnownDataRepositories pins how the policies learn which companion repositories to name: the
// repositories the catalog holds (analytics, legacy-metadata) plus the ones this file's
// metadata_search.repositories section lists (events, and a "bad/name" entry that is not a
// repository ID and must never become an ARN).
func TestKnownDataRepositories(t *testing.T) {
	useConfig(t, `iceberg:
  enabled: true
  metadata_search:
    enabled: true
metadata_search:
  repositories:
    events:
      branches: [main]
    "bad/name":
      branches: [main]
`)
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	cat := newBootstrapCatalog(t, "analytics", "legacy-metadata")

	dataRepositories, err := knownDataRepositories(t.Context(), cat)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"analytics", "events", "legacy-metadata"}, dataRepositories)

	// legacy-metadata is a companion repository by §3.3, not a data repository whose companion
	// would be legacy-metadata-metadata; bad/name is dropped because its companion would not be a
	// valid repository ID.
	require.Equal(t, []string{"analytics-metadata", "events-metadata", "legacy-metadata"},
		companionRepositories(dataRepositories))

	// End to end: the deny of a guard-group member names both the catalog's and the configured
	// repository, and the indexer may write the companion repository of either.
	require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{
		AuthService: svc, Logger: logging.DummyLogger{}, Catalog: cat,
	}, defaultGuardGroups()))
	operator := newOperatorPrincipal(t, svc)

	for _, companion := range []string{"analytics-metadata", "events-metadata"} {
		requireAuthorization(t, svc, operator, permissions.UpdateRepositoryAction, permissions.RepoArn(companion), false,
			"the deny must cover %s: it is a companion repository the server knows", companion)
		requireAuthorization(t, svc, UserName, permissions.WriteObjectAction,
			permissions.ObjectArn(companion, "_iceberg/uuid/data/part-00000.parquet"), true,
			"the indexer must be able to write %s", companion)
	}
	requireAuthorization(t, svc, operator, permissions.WriteObjectAction,
		permissions.ObjectArn("analytics", "foo-metadata/data.yml"), true,
		"a data repository object stays writable whatever its key")
}

// TestPolicyRefresher adds a repository after the policies were built, the way an operator creates
// one in a running installation: the refresher must bring both policies up to date without a server
// restart, and stop when its context is cancelled.
func TestPolicyRefresher(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
	svc := newACLService(t)
	cat := newBootstrapCatalog(t)
	_, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), nil)
	require.NoError(t, err)

	deny, err := svc.GetPolicy(t.Context(), MetadataRepoProtectPolicyName)
	require.NoError(t, err)
	require.Empty(t, deny.Statement, "no repository is known yet, so the deny names none")

	ctx, cancel := context.WithCancel(t.Context())
	done := startPolicyRefresher(ctx, cmdpkg.ServerDeps{
		AuthService: svc, Logger: logging.DummyLogger{}, Catalog: cat,
	}, 10*time.Millisecond)

	_, err = cat.CreateRepository(t.Context(), "analytics", "", "mem://analytics", "main", false)
	require.NoError(t, err, "create the data repository after the policies were built")

	require.Eventually(t, func() bool {
		deny, err := svc.GetPolicy(ctx, MetadataRepoProtectPolicyName)
		if err != nil {
			return false
		}
		indexer, err := svc.GetPolicy(ctx, IndexerPolicyName)
		if err != nil {
			return false
		}
		return policyNamesRepository(deny, "analytics-metadata") && policyNamesRepository(indexer, "analytics-metadata")
	}, 5*time.Second, 10*time.Millisecond,
		"the refresher must add a repository created after startup to both policies")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the policy refresher did not stop when its context was cancelled")
	}
}

// defaultGuardGroups is the group list cmd/lakefs-sso/main.go passes to both installation-wide
// denies when datasets.immutable_guard_groups is unset: the base groups of both group templates. A
// test that configures extra guard groups passes datasets.PinsProtectGroups(extras) instead, exactly
// as guardGroups() builds it.
func defaultGuardGroups() []string {
	return datasets.PinsProtectGroups(nil)
}

// newBootstrapCatalog builds the catalog RoutesBuilder lists the repositories from: a mem block
// store over a mem KV store, the fixture pkg/api's controller tests use, with the given repositories
// created on branch "main".
func newBootstrapCatalog(t *testing.T, repositories ...string) *lakefscatalog.Catalog {
	t.Helper()
	ctx := t.Context()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err, "build the lakeFS configuration")
	cfg.Committed.LocalCache.Dir = filepath.Join(t.TempDir(), "cache")
	cat, err := lakefscatalog.New(context.WithoutCancel(ctx), lakefscatalog.Config{
		Config:       cfg,
		KVStore:      kvtest.GetStore(ctx, t),
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err, "build the lakeFS catalog")
	t.Cleanup(func() { _ = cat.Close() })
	for _, name := range repositories {
		_, err := cat.CreateRepository(ctx, name, "", "mem://"+name, "main", false)
		require.NoErrorf(t, err, "create repository %s", name)
	}
	return cat
}

// policyNamesRepository reports whether a policy carries a statement below repo: its repository ARN
// itself or an ARN of the form repository/<repo>/.... The exact statements this package builds are
// pinned by the two statement-list tests; this predicate is for the tests that care about which
// repositories a policy names, not about its exact text.
func policyNamesRepository(policy *model.Policy, repo string) bool {
	prefix := permissions.RepoArn(repo)
	for _, statement := range policy.Statement {
		if statement.Resource == prefix || strings.HasPrefix(statement.Resource, prefix+"/") {
			return true
		}
	}
	return false
}

// importSource is one paths entry of an import request: the source path (a storage namespace prefix)
// and the destination key in the target repository.
type importSource struct {
	path        string
	destination string
}

// importPermissionNode is the authorization node pkg/api/controller.go's ImportStart builds for one
// import request: fs:WriteObject and fs:CreateCommit on the destination branch, plus
// fs:ImportFromStorage on the source's storage namespace and fs:WriteObject on the destination object
// for every path of the request. The tests import this shape rather than checking one permission at
// a time, so the deny is shown to stop the request the server actually authorizes.
func importPermissionNode(repository, branch string, sources []importSource) permissions.Node {
	node := permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{Permission: permissions.Permission{
				Action:   permissions.WriteObjectAction,
				Resource: permissions.BranchArn(repository, branch),
			}},
			{Permission: permissions.Permission{
				Action:   permissions.CreateCommitAction,
				Resource: permissions.BranchArn(repository, branch),
			}},
		},
	}
	for _, source := range sources {
		node.Nodes = append(node.Nodes,
			permissions.Node{Permission: permissions.Permission{
				Action:   permissions.ImportFromStorageAction,
				Resource: permissions.StorageNamespace(source.path),
			}},
			permissions.Node{Permission: permissions.Permission{
				Action:   permissions.WriteObjectAction,
				Resource: permissions.ObjectArn(repository, source.destination),
			}})
	}
	return node
}

// requireNodeAuthorization asserts the decision the ACL service gives username for one authorization
// node, as the API's authorize helper would.
func requireNodeAuthorization(t *testing.T, svc auth.Service, username string, node permissions.Node, want bool, msg string) {
	t.Helper()
	resp, err := svc.Authorize(t.Context(), &auth.AuthorizationRequest{
		Username:            username,
		RequiredPermissions: node,
	})
	require.NoError(t, err)
	require.Equalf(t, want, resp.Allowed, "%s: %+v for %s", msg, node, username)
}

// requireAuthorization asserts the decision the ACL service gives username for one action on one
// resource.
func requireAuthorization(t *testing.T, svc auth.Service, username, action, resource string, want bool, msgAndArgs ...any) {
	t.Helper()
	resp, err := svc.Authorize(t.Context(), &auth.AuthorizationRequest{
		Username: username,
		RequiredPermissions: permissions.Node{Permission: permissions.Permission{
			Action:   action,
			Resource: resource,
		}},
	})
	require.NoError(t, err)
	require.Equalf(t, want, resp.Allowed, "%s on %s for %s: %v", action, resource, username, msgAndArgs)
}

// newACLService returns the simplified-ACL auth service the lakefs-sso binary uses, on an
// in-memory KV store.
func newACLService(t *testing.T) *acl.AuthService {
	t.Helper()
	store := kvtest.GetStore(t.Context(), t)
	return acl.NewAuthService(store, crypt.NewSecretStore([]byte("mds-bootstrap-test-secret")),
		authparams.ServiceCache{Enabled: false})
}

// policyNames lists the policies attached to group.
func policyNames(t *testing.T, svc auth.Service, group string) []string {
	t.Helper()
	policies, _, err := svc.ListGroupPolicies(t.Context(), group, &model.PaginationParams{Amount: -1})
	require.NoErrorf(t, err, "list policies of group %s", group)
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.DisplayName)
	}
	return names
}

// groupNames lists the display names of groups.
func groupNames(groups []*model.Group) []string {
	names := make([]string, 0, len(groups))
	for _, group := range groups {
		names = append(names, group.DisplayName)
	}
	return names
}

// useConfig installs a viper state backed by the given YAML document, so that
// icebergcatalog.LoadConfig in RoutesBuilder reads exactly what a lakeFS installation would read
// from lakefs.yaml.
func useConfig(t *testing.T, yaml string) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.SetConfigType("yaml")
	require.NoError(t, viper.ReadConfig(strings.NewReader(yaml)))
}

// capturePackageLog redirects the package logger (the one logging.FromContext returns, where
// EnsurePrincipals writes its warnings) to a file and returns a reader for it.
func capturePackageLog(t *testing.T) func() string {
	t.Helper()
	logFile := filepath.Join(t.TempDir(), "mds-bootstrap.log")
	require.NoError(t, logging.SetOutputs([]string{logFile}, 0, 0), "redirect the package logger")
	t.Cleanup(func() {
		_ = logging.CloseWriters()
		require.NoError(t, logging.SetOutputs([]string{"-"}, 0, 0), "restore the package logger")
	})
	return func() string {
		require.NoError(t, logging.CloseWriters(), "flush the captured log")
		data, err := os.ReadFile(logFile)
		require.NoError(t, err, "read the captured log")
		return string(data)
	}
}
