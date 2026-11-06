package bootstrap

// Tests for the principals and policies of METADATA_SEARCH_DESIGN.md §8: the exact statement
// lists of the design, the create/repair/idempotent behaviour of EnsurePrincipals, the one degraded
// startup it accepts (an auth service without policies, exactly like
// icebergcatalog.EnsureReservedPrefixProtectPolicy), and the ARNs the policy set does *not* decide
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
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	"google.golang.org/protobuf/proto"
)

// policyTestRepositories are the repositories the statement-list tests build their policies from:
// two data repositories, one of them repeated, so the tests pin the order (sorted by companion
// repository) and the deduplication as well as the statements themselves.
var policyTestRepositories = []repositoryInfo{
	{Name: "orders"}, {Name: "analytics"}, {Name: "analytics"},
}

func TestIndexerPolicyStatements(t *testing.T) {
	t.Parallel()

	// The §8 statement list, spelled as literals rather than as calls to this package's constants,
	// so the design line and the built policy are pinned to each other. The resources of a companion
	// repository are exact ARNs, not the "*\u002dmetadata" patterns of the design: pkg/auth/wildcard
	// lets "*" span "/", so those patterns also matched data-repository ARNs (MDS-SEC-2). The literal
	// repository name in front of a trailing "*" is what makes the subtree ARN safe.
	policies := IndexerPolicies(policyTestRepositories, time.Unix(0, 0))
	require.Len(t, policies, 2, "one base policy plus one chunk for two companion repositories")
	base := policies[0]
	require.Equal(t, IndexerPolicyName, base.DisplayName)
	require.True(t, statementsEqual(model.Statements{
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
			Action:   []string{"fs:AttachStorageNamespace"},
			Resource: "arn:lakefs:fs:::namespace/*-metadata",
		},
	}, base.Statement), "the base policy is the repository-independent part of §8; got %+v", base.Statement)

	chunk := policies[1]
	require.Equal(t, chunkPolicyName(IndexerPolicyName, 1), chunk.DisplayName)
	want := model.Statements{
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
	}
	require.True(t, statementsEqual(want, chunk.Statement),
		"the first chunk is the companion statement list of §8; got %+v", chunk.Statement)

	// A server that knows no repository yet grants the reads and no companion-repository write: it
	// must not fall back to a pattern, and it writes no chunk.
	empty := IndexerPolicies(nil, time.Unix(0, 0))
	require.Len(t, empty, 1, "no repository, no companion chunk: %+v", empty)
	require.Len(t, empty[0].Statement, 2)
	require.Equal(t, "*", empty[0].Statement[0].Resource)
	require.Equal(t, MetadataNamespacePattern, empty[0].Statement[1].Resource)
}

func TestMetadataRepoProtectPolicyStatements(t *testing.T) {
	t.Parallel()

	// The deny statement list of §8: fs:Write*/Delete*/Create*/Revert*/Update*/Import* and the catalog
	// writes on the companion repositories. Reads are deliberately not denied. fs:Update* is the
	// review's MDS-SEC-5: fs:UpdateRepository writes a repository's metadata and matched none of the
	// other four groups. fs:Import* is the import pair of pkg/api/controller.go: ImportCancel is
	// authorized on a branch ARN of the companion repository, so the subtree statement denies it;
	// ImportFromStorage is authorized on a source storage namespace and is therefore not reached by
	// these repository statements (see metadataRepoProtectFSDeniedActions). branches:
	// SetBranchProtectionRules is denied on the repository ARN (R2S-7); retention actions stay
	// allowed because deleting the live index still needs fs:DeleteBranch, which is denied. The
	// resources are the exact ARNs of the companion repositories of policyTestRepositories
	// (MDS-SEC-2).
	fsActions := []string{"fs:Write*", "fs:Delete*", "fs:Create*", "fs:Revert*", "fs:Update*", "fs:Import*"}
	repositoryActions := append(append([]string(nil), fsActions...), "branches:SetBranchProtectionRules")
	catalogActions := []string{"catalog:Create*", "catalog:Update*", "catalog:Delete*", "catalog:WriteTableData"}
	policies := MetadataRepoProtectPolicies(policyTestRepositories, time.Unix(0, 0))
	require.Len(t, policies, 1, "one chunk for two companion repositories; the deny has no base policy")
	require.NotEqual(t, MetadataRepoProtectPolicyName, policies[0].DisplayName,
		"R3S-2: the deny is never written as a statement-less base policy")

	want := model.Statements{
		{
			Effect:   model.StatementEffectDeny,
			Action:   repositoryActions,
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
			Action:   repositoryActions,
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
	chunk := policies[0]
	require.Equal(t, chunkPolicyName(MetadataRepoProtectPolicyName, 1), chunk.DisplayName)
	require.True(t, statementsEqual(want, chunk.Statement),
		"the first chunk is the deny list of §8; got %+v", chunk.Statement)

	empty := MetadataRepoProtectPolicies(nil, time.Unix(0, 0))
	require.Empty(t, empty,
		"no repository the server knows, nothing to deny, and no statement-less policy: the deny never falls back to a pattern")
}

// TestIsMetadataRepoProtectChunk pins the exported chunk-name predicate the esti suite uses. It
// accepts exactly the names chunkPolicyName writes for a positive chunk number, and rejects the
// base name, a zero or zero-padded chunk, and every loose-prefix neighbour, so the
// suite cannot pass on a policy the bootstrap does not own (R3S-2).
func TestIsMetadataRepoProtectChunk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
		want   bool
	}{
		{name: "first chunk", policy: chunkPolicyName(MetadataRepoProtectPolicyName, 1), want: true},
		{name: "later chunk", policy: chunkPolicyName(MetadataRepoProtectPolicyName, 42), want: true},
		{name: "base", policy: MetadataRepoProtectPolicyName, want: false},
		{name: "zero chunk", policy: MetadataRepoProtectPolicyName + "-0", want: false},
		{name: "zero padded chunk", policy: MetadataRepoProtectPolicyName + "-01", want: false},
		{name: "negative chunk", policy: MetadataRepoProtectPolicyName + "--1", want: false},
		{name: "empty chunk", policy: MetadataRepoProtectPolicyName + "-", want: false},
		{name: "trailing text", policy: MetadataRepoProtectPolicyName + "-1-extra", want: false},
		{name: "non-numeric chunk", policy: MetadataRepoProtectPolicyName + "-extra", want: false},
		{name: "lookalike base", policy: MetadataRepoProtectPolicyName + "x-1", want: false},
		{name: "indexer chunk", policy: chunkPolicyName(IndexerPolicyName, 1), want: false},
		{name: "empty", policy: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equalf(t, tt.want, IsMetadataRepoProtectChunk(tt.policy),
				"IsMetadataRepoProtectChunk(%q)", tt.policy)
		})
	}
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

	// Every policy exists with exactly the built-in statements.
	built := append(IndexerPolicies(policyTestRepositories, bootstrapTS), MetadataRepoProtectPolicies(policyTestRepositories, bootstrapTS)...)
	for _, policy := range built {
		stored, err := svc.GetPolicy(t.Context(), policy.DisplayName)
		require.NoErrorf(t, err, "policy %s exists", policy.DisplayName)
		require.Truef(t, statementsEqual(policy.Statement, stored.Statement),
			"policy %s has the built-in statements; got %+v", policy.DisplayName, stored.Statement)
	}

	// mds-indexer and its chunks are attached to their group and nowhere else, in particular not to
	// a base group.
	indexerPolicies := policyNames(t, svc, GroupName)
	indexerNames := make([]string, 0, len(IndexerPolicies(policyTestRepositories, bootstrapTS)))
	for _, policy := range IndexerPolicies(policyTestRepositories, bootstrapTS) {
		indexerNames = append(indexerNames, policy.DisplayName)
	}
	require.ElementsMatch(t, indexerNames, indexerPolicies,
		"MetadataSearchIndexers carries mds-indexer and its chunks, and nothing else")

	// The deny policies are attached to the base group this installation has...
	protectNames := make([]string, 0, len(MetadataRepoProtectPolicies(policyTestRepositories, bootstrapTS)))
	for _, policy := range MetadataRepoProtectPolicies(policyTestRepositories, bootstrapTS) {
		protectNames = append(protectNames, policy.DisplayName)
	}
	adminPolicies := policyNames(t, svc, "Admins")
	for _, name := range protectNames {
		require.Contains(t, adminPolicies, name,
			"every deny chunk is attached to the base groups, as §8 requires")
	}
	// ... and never to the indexer group: attaching them there would disarm the indexer.
	for _, name := range protectNames {
		require.NotContains(t, indexerPolicies, name,
			"the deny must never be attached to MetadataSearchIndexers")
	}
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
	inForce, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, groups, repositoryInfos("analytics"))
	require.NoError(t, err)
	require.True(t, inForce)

	// The deny has no repository-independent statement, so the deny policy set is its chunks and
	// nothing else (R3S-2): no statement-less base policy is written to any of these groups.
	denyChunk := chunkPolicyName(MetadataRepoProtectPolicyName, 1)
	require.Contains(t, policyNames(t, svc, extraGuardGroup), denyChunk,
		"a configured extra guard group gets the deny exactly like a base group")
	require.Contains(t, policyNames(t, svc, "Admins"), denyChunk)
	require.NotContains(t, policyNames(t, svc, GroupName), denyChunk,
		"the deny is never attached to MetadataSearchIndexers, even when the guard list names it")
	for _, group := range []string{"Admins", extraGuardGroup, GroupName} {
		for _, name := range policyNames(t, svc, group) {
			require.NotEqual(t, MetadataRepoProtectPolicyName, name,
				"the statement-less base deny is never written, let alone attached (R3S-2)")
		}
	}

	output := readLog()
	require.Contains(t, output, "level=warning", "dropping the indexer group from the guard list is logged")
	require.Contains(t, output, GroupName)
}

// TestEnsurePrincipalsAuthorization checks what the policy set actually decides, through the same
// ACL engine the server uses: the indexer writes companion repositories and only reads data
// repositories, and an administrator's fs:* does not write a companion repository.
func TestEnsurePrincipalsAuthorization(t *testing.T) {
	const dataRepo = "analytics"
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	// analytics is a repository the server knows: its companion repository is what the policies name.
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), repositoryInfos(dataRepo))
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
// it) with the installation-wide administrative allows of the RBAC template (fs:*, catalog:*,
// branches:*, retention:*), so a deny test is about the deny and not about a missing allow.
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
			{Effect: model.StatementEffectAllow, Action: []string{"fs:*", "catalog:*", "branches:*", "retention:*"}, Resource: permissions.All},
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
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), repositoryInfos(dataRepo))
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

		// R2S-7: branch protection on a companion repository is a write that can stop the indexer
		// permanently. Retention stays allowed because deleting the live index still needs
		// fs:DeleteBranch, which the deny covers.
		{
			name:     "the deny blocks setting branch protection on a companion repository (R2S-7)",
			username: operator, action: permissions.SetBranchProtectionRulesAction,
			resource: permissions.RepoArn(metaRepo), want: false,
		},
		{
			name:     "the deny does not block setting branch protection on a data repository",
			username: operator, action: permissions.SetBranchProtectionRulesAction,
			resource: permissions.RepoArn(dataRepo), want: true,
		},
		{
			name:     "the deny leaves retention garbage collection rules allowed on a companion repository",
			username: operator, action: permissions.SetGarbageCollectionRulesAction,
			resource: permissions.RepoArn(metaRepo), want: true,
		},
		{
			name:     "the deny leaves retention GC preparation allowed on a companion repository",
			username: operator, action: permissions.PrepareGarbageCollectionCommitsAction,
			resource: permissions.RepoArn(metaRepo), want: true,
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
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), repositoryInfos(dataRepo))
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
			name: "deny chunk policy with a blanket allow",
			policy: &model.Policy{
				CreatedAt:   bootstrapTS,
				DisplayName: chunkPolicyName(MetadataRepoProtectPolicyName, 1),
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:*"},
					Resource: permissions.All,
				}},
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

			var want *model.Policy
			for _, built := range append(IndexerPolicies(policyTestRepositories, bootstrapTS),
				MetadataRepoProtectPolicies(policyTestRepositories, bootstrapTS)...) {
				if built.DisplayName == tt.policy.DisplayName {
					want = built
					break
				}
			}
			require.NotNil(t, want)
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

// TestEnsurePrincipalsRemovesStatementLessDenyPolicy is R3S-2 on the stored side: an earlier version
// wrote an mds-metadata-repo-protect base policy with no statements (and a hand-edit can leave one
// behind). The bootstrap no longer writes one, so it removes the stored one as a stale owned name
// instead of trusting or rewriting it. A policy whose statement array is empty is rejected by a
// server that validates api/authorization.yml (minItems: 1).
func TestEnsurePrincipalsRemovesStatementLessDenyPolicy(t *testing.T) {
	svc := newACLService(t)
	// The ACL service accepts the empty statement list, exactly as the in-process tests always
	// assumed; the API server on top of it does not (TestEnsurePrincipalsOverAuthAPI).
	require.NoError(t, svc.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: MetadataRepoProtectPolicyName,
		Statement:   model.Statements{},
	}, false))

	_, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), policyTestRepositories)
	require.NoError(t, err)

	_, err = svc.GetPolicy(t.Context(), MetadataRepoProtectPolicyName)
	require.ErrorIs(t, err, auth.ErrNotFound,
		"the statement-less base deny is removed, and no empty policy replaces it")
	require.NotEmpty(t, ownedPolicyNames(t, svc, MetadataRepoProtectPolicyName, 10),
		"the chunk policies that do carry statements are still there")
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

// TestEnsurePrincipalsOverAuthAPI is R3S-2 and R3S-5 over the wire, not over the in-process ACL
// service: contrib/auth/acl.Serve validates every request body against api/authorization.yml, and
// pkg/auth.APIAuthService is what the lakefs-sso binary uses when auth is external.
//
// R3S-2: Policy.statement has minItems: 1, so the statement-less base deny policy the bootstrap used
// to write is answered with 400 (auth.ErrInvalidRequest), which EnsurePrincipals returned and
// cmd/lakefs/cmd/run.go's extra-routes hook turned into Fatal. The deny set is now chunks only, so
// nothing statement-less is ever sent.
//
// R3S-5: the ACL HTTP controller answered ListPolicies and DeletePolicy with 501
// (auth.ErrUnexpectedStatusCode through APIAuthService), and every refresh calls both to remove a
// stale chunk. A seeded stale chunk makes the refresh call DeletePolicy and proves it is gone.
func TestEnsurePrincipalsOverAuthAPI(t *testing.T) {
	aclService := newACLService(t)
	_, err := aclService.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)

	staleChunk := chunkPolicyName(MetadataRepoProtectPolicyName, 7)
	require.NoError(t, aclService.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: staleChunk,
		Statement: model.Statements{{
			Effect:   model.StatementEffectDeny,
			Action:   []string{"fs:Delete*"},
			Resource: permissions.RepoArn("gone-metadata"),
		}},
	}, false /* create */), "seed a stale chunk an earlier refresh left behind")

	server := httptest.NewServer(acl.Serve(aclService, logging.DummyLogger{}))
	t.Cleanup(server.Close)
	apiService, err := auth.NewAPIAuthService(
		server.URL+acl.BaseURL+"/", "", false, false,
		crypt.NewSecretStore([]byte("mds-bootstrap-api-test-secret")),
		authparams.ServiceCache{Enabled: false}, logging.DummyLogger{})
	require.NoError(t, err, "build the auth API client the binary uses for external auth")

	inForce, err := EnsurePrincipals(t.Context(), apiService, bootstrapTS, defaultGuardGroups(), repositoryInfos("analytics"))
	require.NoError(t, err,
		"the principals must install over the auth API: a statement-less policy is rejected with 400 and a 501 is fatal")
	require.True(t, inForce)

	// The deny is in force through the same HTTP service: the guard group carries the chunk that
	// names the companion repository of the repository the server knows.
	denyChunk := chunkPolicyName(MetadataRepoProtectPolicyName, 1)
	require.Contains(t, policyNames(t, apiService, "Admins"), denyChunk)
	chunk, err := apiService.GetPolicy(t.Context(), denyChunk)
	require.NoError(t, err)
	require.NotEmpty(t, chunk.Statement, "a stored deny policy always carries statements")

	_, err = apiService.GetPolicy(t.Context(), staleChunk)
	require.ErrorIs(t, err, auth.ErrNotFound, "the stale chunk is deleted over the API (DeletePolicy, R3S-5)")

	// A second startup over the API is idempotent: reading the stored chunks back (ListPolicies) and
	// planning from them rebuilds the same policies, so nothing is rewritten.
	before, err := apiService.GetPolicy(t.Context(), denyChunk)
	require.NoError(t, err)
	_, err = EnsurePrincipals(t.Context(), apiService, bootstrapTS.Add(time.Hour), defaultGuardGroups(), repositoryInfos("analytics"))
	require.NoError(t, err)
	after, err := apiService.GetPolicy(t.Context(), denyChunk)
	require.NoError(t, err)
	require.Equal(t, before.CreatedAt, after.CreatedAt, "an unchanged refresh does not rewrite a chunk")
	require.True(t, statementsEqual(before.Statement, after.Statement))
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
		denyChunk := chunkPolicyName(MetadataRepoProtectPolicyName, 1)
		_, err = svc.GetPolicy(t.Context(), denyChunk)
		require.NoError(t, err, "the policy set is created")
		require.Contains(t, policyNames(t, svc, "Admins"), denyChunk,
			"the builder forwards the guard group list it is handed, as cmd/lakefs-sso does")

		// The repository the catalog holds is the one the policies name.
		require.True(t, policySetNamesRepository(t, svc, MetadataRepoProtectPolicyName,
			"analytics"+icebergcatalog.MetadataRepositorySuffix),
			"the deny names the companion repository of the repository the server knows")
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
// repository ID and must never become an ARN). legacy-metadata is an ordinary repository because
// its data repository "legacy" does not exist (R2S-4): it must stay writable for its users and must
// not be granted to the indexer.
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

	repositories, err := knownRepositories(t.Context(), cat)
	require.NoError(t, err)
	names := make([]string, 0, len(repositories))
	for _, repository := range repositories {
		names = append(names, repository.Name)
	}
	require.ElementsMatch(t, []string{"analytics", "events", "legacy-metadata"}, names)

	// analytics-metadata is the companion of a repository that exists; events-metadata is the
	// companion of a configured repository that does not exist yet. legacy-metadata is not a
	// companion because "legacy" does not exist; bad/name is dropped because its companion would not
	// be a valid repository ID.
	require.Equal(t, []string{"analytics-metadata", "events-metadata"},
		companionNames(companionRepositories(repositories)))

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

	// The ordinary *-metadata repository keeps its users' writes and gets no indexer grant (R2S-4).
	requireAuthorization(t, svc, operator, permissions.WriteObjectAction,
		permissions.ObjectArn("legacy-metadata", "data.yml"), true,
		"an ordinary *-metadata repository stays writable for its users")
	requireAuthorization(t, svc, operator, permissions.DeleteRepositoryAction,
		permissions.RepoArn("legacy-metadata"), true,
		"an ordinary *-metadata repository stays deletable for its users")
	requireAuthorization(t, svc, UserName, permissions.WriteObjectAction,
		permissions.ObjectArn("legacy-metadata", "data.yml"), false,
		"the indexer gets no grant on an ordinary *-metadata repository")
	requireAuthorization(t, svc, UserName, permissions.DeleteRepositoryAction,
		permissions.RepoArn("legacy-metadata"), false,
		"the indexer gets no repository grant on an ordinary *-metadata repository")
}

// TestPolicyRefresher adds a repository after the policies were built, the way an operator creates
// one in a running installation: the refresher must bring the policy set up to date without a server
// restart, and stop when its context is cancelled.
func TestPolicyRefresher(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
	svc := newACLService(t)
	cat := newBootstrapCatalog(t)
	_, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), nil)
	require.NoError(t, err)

	_, err = svc.GetPolicy(t.Context(), MetadataRepoProtectPolicyName)
	require.ErrorIs(t, err, auth.ErrNotFound,
		"no repository is known yet, so no deny policy exists, and no statement-less one is written (R3S-2)")

	ctx, cancel := context.WithCancel(t.Context())
	done := startPolicyRefresher(ctx, cmdpkg.ServerDeps{
		AuthService: svc, Logger: logging.DummyLogger{}, Catalog: cat,
	}, defaultGuardGroups(), 10*time.Millisecond)

	_, err = cat.CreateRepository(t.Context(), "analytics", "", "mem://analytics", "main", false)
	require.NoError(t, err, "create the data repository after the policies were built")

	require.Eventually(t, func() bool {
		return policySetNamesRepository(t, svc, MetadataRepoProtectPolicyName, "analytics-metadata") &&
			policySetNamesRepository(t, svc, IndexerPolicyName, "analytics-metadata")
	}, 5*time.Second, 10*time.Millisecond,
		"the refresher must add a repository created after startup to the policy set")
	require.NotEmpty(t, ownedPolicyNames(t, svc, MetadataRepoProtectPolicyName, 10),
		"the deny is the chunk policy that now names the companion repository")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the policy refresher did not stop when its context was cancelled")
	}
}

// TestPolicySizeAtScale is R2S-2: the statements of 1000 companion repositories are split across
// chunk policies, so every stored policy stays below the documented cap (and therefore well below
// DynamoDB's 400 KiB item limit) instead of growing into one item that the server cannot write.
func TestPolicySizeAtScale(t *testing.T) {
	const repositories = 1000
	known := make([]repositoryInfo, 0, repositories*2)
	for i := 0; i < repositories; i++ {
		// A 54-character data repository ID is the longest whose -metadata companion is still a
		// valid 63-character lakeFS repository ID, so this is the widest policy a real name can build.
		name := fmt.Sprintf("r%053d", i)
		known = append(known,
			repositoryInfo{Name: name, StorageNamespace: "s3://bucket/" + name},
			repositoryInfo{
				Name:             icebergcatalog.MetadataRepository(name),
				StorageNamespace: "s3://bucket/" + icebergcatalog.MetadataRepository(name),
			})
	}

	svc := newACLService(t)
	_, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, nil, known)
	require.NoError(t, err)
	policies, _, err := svc.ListPolicies(t.Context(), &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.Greater(t, len(policies), 2, "1000 repositories must be split into chunk policies")
	var largest int
	for _, policy := range policies {
		encoded, err := proto.Marshal(model.ProtoFromPolicy(policy))
		require.NoErrorf(t, err, "encode policy %s", policy.DisplayName)
		largest = max(largest, len(encoded))
		require.LessOrEqualf(t, len(encoded), MaxPolicyStoredBytes,
			"policy %s is %d bytes, above the documented cap %d; chunking must bound every item",
			policy.DisplayName, len(encoded), MaxPolicyStoredBytes)
	}
	t.Logf("1000 repositories: %d policies, largest %d bytes (cap %d)", len(policies), largest, MaxPolicyStoredBytes)
}

// TestPolicyChunksShrink is the other half of R2S-2: when the repository set shrinks, the chunks
// that are no longer needed are deleted, so a removed repository's grant and deny do not survive.
func TestPolicyChunksShrink(t *testing.T) {
	svc := newACLService(t)
	big := make([]repositoryInfo, 0, companionChunkSize*2+1)
	for i := 0; i < companionChunkSize*2+1; i++ {
		big = append(big, repositoryInfo{Name: fmt.Sprintf("repo-%03d", i)})
	}
	_, err := EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), big)
	require.NoError(t, err)
	require.NotEmpty(t, ownedPolicyNames(t, svc, MetadataRepoProtectPolicyName, 3),
		"the large repository set must create more than one deny chunk")

	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS.Add(time.Hour), defaultGuardGroups(), big[:1])
	require.NoError(t, err)
	require.ElementsMatch(t, []string{IndexerPolicyName, chunkPolicyName(IndexerPolicyName, 1)},
		ownedPolicyNames(t, svc, IndexerPolicyName, 10),
		"a shrunk repository set leaves the indexer base and the one chunk that still names a companion")
	require.ElementsMatch(t, []string{chunkPolicyName(MetadataRepoProtectPolicyName, 1)},
		ownedPolicyNames(t, svc, MetadataRepoProtectPolicyName, 10),
		"a shrunk repository set leaves only the deny chunk that still names a companion (no base deny)")
}

// TestCompanionStorageNamespaceDeny is R2S-3: a guard-group member cannot create another repository
// on a companion repository's existing storage namespace, while an ordinary namespace stays allowed
// and the indexer's namespace/*-metadata grant stays in place.
func TestCompanionStorageNamespaceDeny(t *testing.T) {
	const (
		dataRepo           = "analytics"
		metaRepo           = dataRepo + icebergcatalog.MetadataRepositorySuffix
		companionNamespace = "mem://" + metaRepo
	)
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	cat := newBootstrapCatalog(t, dataRepo, metaRepo)
	repositories, err := knownRepositories(t.Context(), cat)
	require.NoError(t, err)
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), repositories)
	require.NoError(t, err)
	operator := newOperatorPrincipal(t, svc)

	// The exact CreateRepository node of pkg/api/controller.go on the companion's namespace.
	requireNodeAuthorization(t, svc, operator, permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{Permission: permissions.Permission{
				Action:   permissions.CreateRepositoryAction,
				Resource: permissions.RepoArn("evil"),
			}},
			{Permission: permissions.Permission{
				Action:   permissions.AttachStorageNamespaceAction,
				Resource: permissions.StorageNamespace(companionNamespace),
			}},
		},
	}, false, "a guard-group member must not create a repository on a companion storage namespace")

	requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction,
		permissions.StorageNamespace(companionNamespace), false,
		"the exact companion storage namespace is denied")
	requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction,
		permissions.StorageNamespace("mem://ordinary"), true,
		"an ordinary storage namespace stays allowed")
	requireAuthorization(t, svc, UserName, permissions.AttachStorageNamespaceAction,
		permissions.StorageNamespace("s3://bucket/"+metaRepo), true,
		"the indexer keeps the namespace/*-metadata pattern it creates companions with")
}

// TestCompanionStorageNamespaceDenySpellings is R3S-3: CreateRepository authorizes
// fs:AttachStorageNamespace on the raw storage_namespace string the client sent
// (pkg/api/controller.go, before it looks at the --bare flag), and the block layer resolves a
// trailing "/" away, so the same physical prefix has more than one spelling the server accepts. The
// deny is built from the namespace the catalog stored, so an exact ARN of that one spelling would
// leave every other spelling to a guard-group member with fs:*: they could create a bare repository
// on a companion's prefix and let its uncommitted GC sweep the index files.
//
// The test drives the exact authorization node CreateRepository evaluates, for both stored spellings
// and both accepted spellings of the same physical prefix. The node is the 403: the controller
// authorizes before any other work, and a denial there is the API's 403 response.
func TestCompanionStorageNamespaceDenySpellings(t *testing.T) {
	const (
		dataRepo = "analytics"
		metaRepo = dataRepo + icebergcatalog.MetadataRepositorySuffix
	)
	// The catalog stores whatever spelling the repository was created with, so the companion's own
	// namespace may itself end in "/". Both stored spellings must deny both accepted spellings.
	for _, stored := range []string{"mem://" + metaRepo, "mem://" + metaRepo + "/"} {
		t.Run(stored, func(t *testing.T) {
			svc := newACLService(t)
			_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
			require.NoError(t, err)
			repositories := []repositoryInfo{
				{Name: dataRepo, StorageNamespace: "mem://" + dataRepo},
				{Name: metaRepo, StorageNamespace: stored},
			}
			_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), repositories)
			require.NoError(t, err)
			operator := newOperatorPrincipal(t, svc)

			for _, spelling := range []string{"mem://" + metaRepo, "mem://" + metaRepo + "/"} {
				// The CreateRepository node of pkg/api/controller.go: fs:CreateRepository on the
				// repository ARN (which the operator's fs:* allows) and fs:AttachStorageNamespace on
				// the namespace as the client spelled it. The bare flag is not read before this node,
				// so a bare repository is decided by the same two permissions.
				requireNodeAuthorization(t, svc, operator, permissions.Node{
					Type: permissions.NodeTypeAnd,
					Nodes: []permissions.Node{
						{Permission: permissions.Permission{
							Action:   permissions.CreateRepositoryAction,
							Resource: permissions.RepoArn("evil"),
						}},
						{Permission: permissions.Permission{
							Action:   permissions.AttachStorageNamespaceAction,
							Resource: permissions.StorageNamespace(spelling),
						}},
					},
				}, false, fmt.Sprintf("a guard-group member must not create a repository (bare or not) on the companion storage namespace spelled %q", spelling))

				requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction,
					permissions.StorageNamespace(spelling), false,
					"the companion storage namespace is denied in the spelling %q", spelling)
			}

			// A different physical prefix is still allowed: the deny names the companion's prefix and
			// nothing else.
			requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction,
				permissions.StorageNamespace("mem://ordinary"), true,
				"an ordinary storage namespace stays allowed")
		})
	}

	// A bucket-root namespace (a companion with an explicit storage_namespace in another bucket) has
	// one more accepted spelling: pkg/block.resolveRelative trims one "/", so "mem://index-bucket//"
	// resolves to the bucket root like "mem://index-bucket".
	t.Run("bucket root", func(t *testing.T) {
		const root = "mem://index-bucket"
		svc := newACLService(t)
		_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
		require.NoError(t, err)
		repositories := []repositoryInfo{
			{Name: dataRepo, StorageNamespace: "mem://data-bucket/" + dataRepo},
			{Name: metaRepo, StorageNamespace: root},
		}
		_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), repositories)
		require.NoError(t, err)
		operator := newOperatorPrincipal(t, svc)

		for _, spelling := range []string{root, root + "/", root + "//"} {
			requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction,
				permissions.StorageNamespace(spelling), false,
				"the bucket-root companion namespace is denied in the spelling %q", spelling)
		}
	})
}

// TestStorageNamespaceDenyResources pins the exact ARN list of the AttachStorageNamespace deny
// resource by resource (R3S-3, METADATA_SEARCH_AZURE D4). The S3, GS and mem lists are the ones the
// function produced before the Azure work, so the Azure rules cannot change them silently; the Azure
// lists add the other scheme's spelling of every spelling and give the container root - not the
// host - the "//" spelling.
func TestStorageNamespaceDenyResources(t *testing.T) {
	arns := func(spellings ...string) []string {
		resources := make([]string, 0, len(spellings))
		for _, spelling := range spellings {
			resources = append(resources, permissions.StorageNamespace(spelling))
		}
		return resources
	}
	tests := []struct {
		name      string
		namespace string
		want      []string
	}{
		{
			name:      "s3 prefix",
			namespace: "s3://bucket/analytics-metadata",
			want: arns(
				"s3://bucket/analytics-metadata",
				"s3://bucket/analytics-metadata/"),
		},
		{
			name:      "s3 prefix, stored with a trailing slash",
			namespace: "s3://bucket/analytics-metadata/",
			want: arns(
				"s3://bucket/analytics-metadata/",
				"s3://bucket/analytics-metadata"),
		},
		{
			name:      "s3 bucket root",
			namespace: "s3://index-bucket",
			want: arns(
				"s3://index-bucket",
				"s3://index-bucket/",
				"s3://index-bucket//"),
		},
		{
			name:      "gs prefix",
			namespace: "gs://bucket/analytics-metadata",
			want: arns(
				"gs://bucket/analytics-metadata",
				"gs://bucket/analytics-metadata/"),
		},
		{
			name:      "gs bucket root",
			namespace: "gs://bucket",
			want: arns(
				"gs://bucket",
				"gs://bucket/",
				"gs://bucket//"),
		},
		{
			name:      "mem bucket root",
			namespace: "mem://analytics-metadata",
			want: arns(
				"mem://analytics-metadata",
				"mem://analytics-metadata/",
				"mem://analytics-metadata//"),
		},
		{
			name:      "azure prefix, https stored",
			namespace: "https://acct.blob.core.windows.net/data/analytics-metadata",
			want: arns(
				"https://acct.blob.core.windows.net/data/analytics-metadata",
				"https://acct.blob.core.windows.net/data/analytics-metadata/",
				"http://acct.blob.core.windows.net/data/analytics-metadata",
				"http://acct.blob.core.windows.net/data/analytics-metadata/"),
		},
		{
			name:      "azure prefix, http stored",
			namespace: "http://acct.blob.core.windows.net/data/analytics-metadata",
			want: arns(
				"http://acct.blob.core.windows.net/data/analytics-metadata",
				"http://acct.blob.core.windows.net/data/analytics-metadata/",
				"https://acct.blob.core.windows.net/data/analytics-metadata",
				"https://acct.blob.core.windows.net/data/analytics-metadata/"),
		},
		{
			name:      "azure container root, https stored",
			namespace: "https://acct.blob.core.windows.net/index",
			want: arns(
				"https://acct.blob.core.windows.net/index",
				"https://acct.blob.core.windows.net/index/",
				"https://acct.blob.core.windows.net/index//",
				"http://acct.blob.core.windows.net/index",
				"http://acct.blob.core.windows.net/index/",
				"http://acct.blob.core.windows.net/index//"),
		},
		{
			name:      "azure container root, http stored",
			namespace: "http://acct.azurite.test/index",
			want: arns(
				"http://acct.azurite.test/index",
				"http://acct.azurite.test/index/",
				"http://acct.azurite.test/index//",
				"https://acct.azurite.test/index",
				"https://acct.azurite.test/index/",
				"https://acct.azurite.test/index//"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, storageNamespaceDenyResources(test.namespace))
		})
	}
}

// TestOrdinaryMetadataSuffixRepository is R2S-4: a repository named x-metadata is a companion only
// when its data repository x exists. An ordinary x-metadata repository stays writable for its users
// and gets no indexer grant.
func TestOrdinaryMetadataSuffixRepository(t *testing.T) {
	const ordinary = "team-metadata"
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	repositories := []repositoryInfo{{Name: ordinary, StorageNamespace: "mem://" + ordinary}}
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), repositories)
	require.NoError(t, err)
	operator := newOperatorPrincipal(t, svc)

	requireAuthorization(t, svc, operator, permissions.WriteObjectAction,
		permissions.ObjectArn(ordinary, "data.yml"), true,
		"an ordinary x-metadata repository stays writable for its users")
	requireAuthorization(t, svc, operator, permissions.CreateCommitAction,
		permissions.BranchArn(ordinary, "main"), true,
		"an ordinary x-metadata repository stays committable for its users")
	requireAuthorization(t, svc, operator, permissions.DeleteRepositoryAction,
		permissions.RepoArn(ordinary), true,
		"an ordinary x-metadata repository stays deletable for its users")
	requireAuthorization(t, svc, UserName, permissions.WriteObjectAction,
		permissions.ObjectArn(ordinary, "data.yml"), false,
		"the indexer gets no write grant on an ordinary x-metadata repository")
	requireAuthorization(t, svc, UserName, permissions.DeleteRepositoryAction,
		permissions.RepoArn(ordinary), false,
		"the indexer gets no repository grant on an ordinary x-metadata repository")
}

// TestOperatorPolicySurvivesRefresher is R2S-6 with the chosen fix: the operator puts the extra
// grant for an explicit non-metadata storage namespace in a policy of its own, attached to
// MetadataSearchIndexers. RoutesBuilder and a later refresh never touch that policy.
func TestOperatorPolicySurvivesRefresher(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
	const (
		policyName = "mds-indexer-extra-storage-namespace"
		namespace  = "s3://bucket/explicit-metadata"
	)
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	cat := newBootstrapCatalog(t, "analytics")
	operatorPolicy := &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: policyName,
		Statement: model.Statements{
			{Effect: model.StatementEffectAllow, Action: []string{permissions.CreateRepositoryAction},
				Resource: permissions.RepoArn("explicit-metadata")},
			{Effect: model.StatementEffectAllow, Action: []string{permissions.AttachStorageNamespaceAction},
				Resource: permissions.StorageNamespace(namespace)},
		},
	}
	_, err = svc.CreateGroup(t.Context(), &model.Group{DisplayName: GroupName, CreatedAt: bootstrapTS})
	require.NoError(t, err)
	require.NoError(t, svc.WritePolicy(t.Context(), operatorPolicy, false))
	require.NoError(t, svc.AttachPolicyToGroup(t.Context(), policyName, GroupName))

	require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{
		AuthService: svc, Logger: logging.DummyLogger{}, Catalog: cat,
	}, defaultGuardGroups()))
	require.Contains(t, policyNames(t, svc, GroupName), policyName,
		"RoutesBuilder does not detach an operator's extra policy")

	// A refresh that adds a repository and rewrites the chunks must leave the operator policy alone.
	inForce, _, err := refreshPolicies(t.Context(), svc, repositoryInfos("analytics", "orders"), defaultGuardGroups(), bootstrapTS.Add(time.Hour))
	require.NoError(t, err)
	require.True(t, inForce)
	stored, err := svc.GetPolicy(t.Context(), policyName)
	require.NoError(t, err, "the refresher must not delete a policy it does not own")
	require.True(t, statementsEqual(operatorPolicy.Statement, stored.Statement))
	require.Contains(t, policyNames(t, svc, GroupName), policyName,
		"the refresher does not detach an operator's extra policy")
	requireAuthorization(t, svc, UserName, permissions.AttachStorageNamespaceAction,
		permissions.StorageNamespace(namespace), true,
		"the operator's extra grant is still in force for the indexer after a refresh")
}

// TestRoutesBuilderStartsPolicyRefresher is R2S-8: the production wiring, not just
// startPolicyRefresher, must cover a repository created after RoutesBuilder returned.
func TestRoutesBuilderStartsPolicyRefresher(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
	svc := newACLService(t)
	cat := newBootstrapCatalog(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)

	oldInterval := policyRefreshInterval
	policyRefreshInterval = 10 * time.Millisecond
	t.Cleanup(func() { policyRefreshInterval = oldInterval })

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	require.NoError(t, RoutesBuilder(ctx, cmdpkg.ServerDeps{
		AuthService: svc, Logger: logging.DummyLogger{}, Catalog: cat,
	}, defaultGuardGroups()))

	_, err = cat.CreateRepository(t.Context(), "late", "", "mem://late", "main", false)
	require.NoError(t, err, "create a repository after RoutesBuilder returned")
	require.Eventually(t, func() bool {
		return policySetNamesRepository(t, svc, MetadataRepoProtectPolicyName, "late-metadata") &&
			policySetNamesRepository(t, svc, IndexerPolicyName, "late-metadata")
	}, 5*time.Second, 10*time.Millisecond,
		"RoutesBuilder must start the refresher that covers a repository created after startup")
}

// cachingPolicyService is an auth.Service with the cache behaviour of pkg/auth.APIAuthService:
// WritePolicy stores the policy but does not purge this service's effective-policy cache. The
// underlying ACL service is the real one, so authorization decisions are real.
type cachingPolicyService struct {
	*acl.AuthService
	cache *auth.LRUCache
}

func newCachingPolicyService(t *testing.T) *cachingPolicyService {
	t.Helper()
	store := kvtest.GetStore(t.Context(), t)
	return &cachingPolicyService{
		AuthService: acl.NewAuthService(store, crypt.NewSecretStore([]byte("mds-bootstrap-cache-test-secret")),
			authparams.ServiceCache{Enabled: false}),
		cache: auth.NewLRUCache(1000, time.Minute, 0),
	}
}

func (s *cachingPolicyService) Cache() auth.Cache {
	return s.cache
}

func (s *cachingPolicyService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	policies, err := s.cache.GetUserPolicies(req.Username, func() ([]*model.Policy, error) {
		policies, _, err := s.AuthService.ListEffectivePolicies(ctx, req.Username, &model.PaginationParams{Amount: -1})
		return policies, err
	})
	if err != nil {
		return nil, err
	}
	audit := &auth.MissingPermissions{}
	if auth.CheckPermissions(ctx, req.RequiredPermissions, req.Username, policies, audit) != auth.CheckAllow {
		return &auth.AuthorizationResponse{Allowed: false, Error: fmt.Errorf("%w: %s", auth.ErrInsufficientPermissions, audit.String())}, nil
	}
	return &auth.AuthorizationResponse{Allowed: true}, nil
}

// nilCachePolicyService is an auth.Service with no cache at all: the bootstrap must keep working.
type nilCachePolicyService struct {
	*acl.AuthService
}

func (s *nilCachePolicyService) Cache() auth.Cache {
	return nil
}

// TestRefreshPurgesPolicyCache is R2S-9: after a rewrite the refresher purges the auth service's
// effective-policy cache, so the new deny is effective on the next Authorize instead of after
// auth.cache.ttl. A service without a cache keeps working.
func TestRefreshPurgesPolicyCache(t *testing.T) {
	svc := newCachingPolicyService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), repositoryInfosWithCompanionNamespaces("analytics"))
	require.NoError(t, err)
	operator := newOperatorPrincipal(t, svc)

	ordersNamespace := permissions.StorageNamespace("s3://bucket/orders-metadata")
	requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction, ordersNamespace, true,
		"before orders exists its companion namespace is not denied; this also warms the cache")

	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS.Add(time.Hour), defaultGuardGroups(),
		repositoryInfosWithCompanionNamespaces("analytics", "orders"))
	require.NoError(t, err)
	requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction, ordersNamespace, false,
		"the refresh must purge the cached effective policies so the new deny applies at once")

	// The same rewrite on a service that has no cache at all must not panic.
	store := kvtest.GetStore(t.Context(), t)
	noCache := &nilCachePolicyService{AuthService: acl.NewAuthService(store,
		crypt.NewSecretStore([]byte("mds-bootstrap-nocache-test-secret")), authparams.ServiceCache{Enabled: false})}
	_, err = EnsurePrincipals(t.Context(), noCache, bootstrapTS, defaultGuardGroups(), repositoryInfosWithCompanionNamespaces("analytics"))
	require.NoError(t, err)
	_, err = EnsurePrincipals(t.Context(), noCache, bootstrapTS.Add(time.Hour), defaultGuardGroups(),
		repositoryInfosWithCompanionNamespaces("analytics", "orders"))
	require.NoError(t, err, "a service without a policy cache keeps working")
}

// refillOnAttachService is the cachingPolicyService with the window R3S-6 is about: a request that
// arrives between a policy write and its attachment refills the effective-policy cache without the
// policy that is about to be attached. Purging after the write is not enough - that purge happens
// before the attach - so without a purge after the attach the next Authorize keeps serving the stale
// entry for up to auth.cache.ttl. The refill runs before delegating each attach, which is exactly a
// request in that window.
type refillOnAttachService struct {
	*cachingPolicyService
	refill func(ctx context.Context) error
}

func (s *refillOnAttachService) AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupID string) error {
	if s.refill != nil {
		if err := s.refill(ctx); err != nil {
			return err
		}
	}
	return s.cachingPolicyService.AttachPolicyToGroup(ctx, policyDisplayName, groupID)
}

// companionRepositoriesNamed builds the repository list of count data repositories plus their
// companion repositories, each with the companion's derived storage namespace. The names sort after
// prefix "aaa" and before prefix "repo", which the insertion test relies on.
func companionRepositoriesNamed(prefix string, count int) []repositoryInfo {
	repositories := make([]repositoryInfo, 0, count*2)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("%s-%03d", prefix, i)
		companion := icebergcatalog.MetadataRepository(name)
		repositories = append(repositories,
			repositoryInfo{Name: name, StorageNamespace: "s3://bucket/" + name},
			repositoryInfo{Name: companion, StorageNamespace: "s3://bucket/" + companion})
	}
	return repositories
}

// TestRefreshPurgesCacheAfterAttach is R3S-6: a chunk the refresh *creates and attaches* must be
// visible to the next Authorize at once, even when a request in the window between its write and
// its attachment refilled the cache. Only a new chunk shows the gap: a rewritten chunk purges after
// its own write. The deny is the dangerous half - it fails open on a new companion repository.
func TestRefreshPurgesCacheAfterAttach(t *testing.T) {
	svc := newCachingPolicyService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)

	// companionChunkSize companions fill chunk 1 exactly. The late companion sorts before all of
	// them but does not move any of them: it is a new member, so it lands in a new chunk 2.
	initial := companionRepositoriesNamed("repo", companionChunkSize)
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), initial)
	require.NoError(t, err)
	operator := newOperatorPrincipal(t, svc)

	lateNamespace := permissions.StorageNamespace("s3://bucket/late-metadata")
	requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction, lateNamespace, true,
		"before the late companion exists its namespace is not denied; this warms the cache too")

	refilling := &refillOnAttachService{cachingPolicyService: svc}
	refilling.refill = func(ctx context.Context) error {
		// A request in the window between the new chunk's write and its attachment: it caches the
		// effective policies of the moment, which do not include the chunk yet.
		_, err := refilling.Authorize(ctx, &auth.AuthorizationRequest{
			Username: operator,
			RequiredPermissions: permissions.Node{Permission: permissions.Permission{
				Action:   permissions.AttachStorageNamespaceAction,
				Resource: lateNamespace,
			}},
		})
		return err
	}
	late := []repositoryInfo{
		{Name: "late", StorageNamespace: "s3://bucket/late"},
		{Name: "late-metadata", StorageNamespace: "s3://bucket/late-metadata"},
	}
	_, _, err = refreshPolicies(t.Context(), refilling, append(initial, late...), defaultGuardGroups(), bootstrapTS.Add(time.Hour))
	require.NoError(t, err)

	require.Contains(t, policyNames(t, svc, "Admins"), chunkPolicyName(MetadataRepoProtectPolicyName, 2),
		"the new chunk was attached to the guard group")
	requireAuthorization(t, svc, operator, permissions.AttachStorageNamespaceAction, lateNamespace, false,
		"the purge after the attach makes the new chunk effective on the very next Authorize (R3S-6)")
}

// failNthProtectChunkWriteService fails the nth WritePolicy of a deny chunk, after the earlier chunks
// have been written. It is how the test reproduces a crash or an error in the middle of a refresh.
type failNthProtectChunkWriteService struct {
	*acl.AuthService
	mu     sync.Mutex
	nth    int
	writes int
}

func (s *failNthProtectChunkWriteService) WritePolicy(ctx context.Context, policy *model.Policy, update bool) error {
	if chunk, ok := ownedChunkNumber(MetadataRepoProtectPolicyName, policy.DisplayName); ok && chunk > 0 {
		s.mu.Lock()
		s.writes++
		write := s.writes
		s.mu.Unlock()
		if write == s.nth {
			return errors.New("injected policy write failure")
		}
	}
	return s.AuthService.WritePolicy(ctx, policy, update)
}

// TestRefreshKeepsCompanionCoverageAcrossWrites is R3S-7: a companion must never lose its grant or
// its deny between two policy writes of one refresh. Chunk membership is positional in a fresh plan,
// so inserting companions that sort first shifts every existing companion up and drops the last
// member of chunk 1 into chunk 2 - which is only written later. Failing that later write used to
// leave the shifted companion with neither its indexer grant nor its guard-group deny until the next
// refresh. The plan keeps every companion in the chunk that already carries it, so a refresh that
// only adds companions never rewrites a chunk that already covers one.
func TestRefreshKeepsCompanionCoverageAcrossWrites(t *testing.T) {
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)

	// 130 companions: chunks 1 and 2 are full, chunk 3 holds the last two.
	initial := companionRepositoriesNamed("repo", companionChunkSize*2+2)
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS, defaultGuardGroups(), initial)
	require.NoError(t, err)
	operator := newOperatorPrincipal(t, svc)

	// The last member of chunk 1. Any index object below it stands in for the indexer's writes.
	const survivor = "repo-063-metadata"
	indexObject := permissions.ObjectArn(survivor, "_iceberg/table-uuid/data/part-00000.parquet")
	requireAuthorization(t, svc, operator, permissions.WriteObjectAction, indexObject, false,
		"the deny covers the survivor before the refresh")
	requireAuthorization(t, svc, UserName, permissions.WriteObjectAction, indexObject, true,
		"the indexer may write the survivor before the refresh")

	// 63 companions that sort before every existing one: the insertion that used to shift the set.
	inserted := companionRepositoriesNamed("aaa", 63)
	failing := &failNthProtectChunkWriteService{AuthService: svc, nth: 2}
	_, _, err = refreshPolicies(t.Context(), failing, append(inserted, initial...), defaultGuardGroups(), bootstrapTS.Add(time.Hour))
	require.Error(t, err, "the injected write failure is reported")
	require.ErrorContains(t, err, "injected policy write failure")

	requireAuthorization(t, svc, operator, permissions.WriteObjectAction, indexObject, false,
		"the survivor keeps its deny when a later policy write of the refresh fails (R3S-7)")
	requireAuthorization(t, svc, UserName, permissions.WriteObjectAction, indexObject, true,
		"the survivor keeps its indexer grant when a later policy write of the refresh fails (R3S-7)")
}

// TestExistingCompanionChunksIgnoresChunkNumberGaps pins the read side of the stable plan: a stored
// chunk policy numbered far above the number of chunks must not make the plan allocate that many
// chunks. Policy names are predictable, so a principal allowed to write policies can create one.
func TestExistingCompanionChunksIgnoresChunkNumberGaps(t *testing.T) {
	svc := newACLService(t)
	policy := func(chunk int, companion string) *model.Policy {
		return &model.Policy{
			CreatedAt:   bootstrapTS,
			DisplayName: chunkPolicyName(MetadataRepoProtectPolicyName, chunk),
			Statement: model.Statements{{
				Effect:   model.StatementEffectDeny,
				Action:   []string{"fs:Delete*"},
				Resource: permissions.RepoArn(companion),
			}},
		}
	}
	require.NoError(t, svc.WritePolicy(t.Context(), policy(2, "orders-metadata"), false))
	require.NoError(t, svc.WritePolicy(t.Context(), policy(1<<20, "analytics-metadata"), false))

	chunks, err := existingCompanionChunks(t.Context(), svc, MetadataRepoProtectPolicyName)
	require.NoError(t, err)
	require.Len(t, chunks, 2, "one entry per chunk that exists, not one per chunk number")
	require.Equal(t, []string{"orders-metadata"}, chunks[0])
	require.Equal(t, []string{"analytics-metadata"}, chunks[1])
}

// TestPolicyRefresherAttachesNewChunks is the attachment half of R2S-2: a chunk created by the
// refresher after the repository set grows past a chunk boundary is attached to the same groups as
// the base policies, not left orphaned.
func TestPolicyRefresherAttachesNewChunks(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
	svc := newACLService(t)
	cat := newBootstrapCatalog(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)

	oldInterval := policyRefreshInterval
	policyRefreshInterval = 10 * time.Millisecond
	t.Cleanup(func() { policyRefreshInterval = oldInterval })
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	require.NoError(t, RoutesBuilder(ctx, cmdpkg.ServerDeps{
		AuthService: svc, Logger: logging.DummyLogger{}, Catalog: cat,
	}, defaultGuardGroups()))

	for i := 0; i <= companionChunkSize; i++ {
		name := fmt.Sprintf("chunk-repo-%03d", i)
		_, err := cat.CreateRepository(t.Context(), name, "", "mem://"+name, "main", false)
		require.NoErrorf(t, err, "create repository %s", name)
	}

	indexerChunk := chunkPolicyName(IndexerPolicyName, 2)
	protectChunk := chunkPolicyName(MetadataRepoProtectPolicyName, 2)
	require.Eventually(t, func() bool {
		return groupHasPolicy(svc, GroupName, indexerChunk) && groupHasPolicy(svc, "Admins", protectChunk)
	}, 10*time.Second, 10*time.Millisecond,
		"the refresher must attach a new chunk to the indexer group and to the guard groups")
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

// repositoryInfos builds the repository list the policy builders take when a test does not need
// storage namespaces.
func repositoryInfos(names ...string) []repositoryInfo {
	repositories := make([]repositoryInfo, len(names))
	for i, name := range names {
		repositories[i] = repositoryInfo{Name: name}
	}
	return repositories
}

// repositoryInfosWithCompanionNamespaces builds a repository list in which every data repository
// and its companion exist, with the companion's derived storage namespace.
func repositoryInfosWithCompanionNamespaces(names ...string) []repositoryInfo {
	repositories := make([]repositoryInfo, 0, len(names)*2)
	for _, name := range names {
		companion := icebergcatalog.MetadataRepository(name)
		repositories = append(repositories,
			repositoryInfo{Name: name, StorageNamespace: "s3://bucket/" + name},
			repositoryInfo{Name: companion, StorageNamespace: "s3://bucket/" + companion})
	}
	return repositories
}

// groupHasPolicy reports whether group carries policy, for the Eventually loops that wait for the
// refresher. A transient list error is just "not yet".
func groupHasPolicy(svc auth.Service, group, policyName string) bool {
	policies, _, err := svc.ListGroupPolicies(context.Background(), group, &model.PaginationParams{Amount: -1})
	if err != nil {
		return false
	}
	for _, policy := range policies {
		if policy.DisplayName == policyName {
			return true
		}
	}
	return false
}

// ownedPolicyNames lists the base/chunk policies of base, at most limit of them (for diagnostics).
func ownedPolicyNames(t *testing.T, svc auth.Service, base string, limit int) []string {
	t.Helper()
	policies, _, err := svc.ListPolicies(t.Context(), &model.PaginationParams{Prefix: base, Amount: limit})
	require.NoErrorf(t, err, "list policies with prefix %s", base)
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		if isOwnedPolicyName(base, policy.DisplayName) {
			names = append(names, policy.DisplayName)
		}
	}
	return names
}

// policySetNamesRepository reports whether any policy of the base/chunk set names repo.
func policySetNamesRepository(t *testing.T, svc auth.Service, base, repo string) bool {
	t.Helper()
	policies, _, err := svc.ListPolicies(t.Context(), &model.PaginationParams{Prefix: base, Amount: -1})
	require.NoErrorf(t, err, "list policies with prefix %s", base)
	for _, policy := range policies {
		if isOwnedPolicyName(base, policy.DisplayName) && policyNamesRepository(policy, repo) {
			return true
		}
	}
	return false
}

// policyNamesRepository reports whether a policy carries a statement below repo: its repository ARN
// itself or an ARN of the form repository/<repo>/.... The exact statements this package builds are
// pinned by the statement-list tests; this predicate is for the tests that care about which
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
