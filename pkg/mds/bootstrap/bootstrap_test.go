package bootstrap

// Tests for the principals and policies of METADATA_SEARCH_DESIGN.md §8: the exact statement
// lists of the design, the create/repair/idempotent behaviour of EnsurePrincipals, and the one
// degraded startup it accepts (an auth service without policies, exactly like
// icebergcatalog.EnsureReservedPrefixProtectPolicy).
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
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

func TestIndexerPolicyStatements(t *testing.T) {
	t.Parallel()

	// The §8 statement list, spelled as literals rather than as calls to this package's constants,
	// so the design line and the built policy are pinned to each other.
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
			Resource: "arn:lakefs:fs:::repository/*-metadata",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*"},
			Resource: "arn:lakefs:fs:::repository/*-metadata/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::*/*-metadata/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:CreateRepository"},
			Resource: "arn:lakefs:fs:::repository/*-metadata",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:AttachStorageNamespace"},
			Resource: "arn:lakefs:fs:::namespace/*-metadata",
		},
	}
	policy := IndexerPolicy(time.Unix(0, 0))
	require.Equal(t, IndexerPolicyName, policy.DisplayName)
	require.True(t, statementsEqual(want, policy.Statement),
		"the mds-indexer policy is the statement list of §8; got %+v", policy.Statement)
}

func TestMetadataRepoProtectPolicyStatements(t *testing.T) {
	t.Parallel()

	// The deny statement list of §8: fs:Write*/Delete*/Create*/Revert* and the catalog writes on the
	// *-metadata ARNs. Reads are deliberately not denied.
	fsActions := []string{"fs:Write*", "fs:Delete*", "fs:Create*", "fs:Revert*"}
	catalogActions := []string{"catalog:Create*", "catalog:Update*", "catalog:Delete*", "catalog:WriteTableData"}
	want := model.Statements{
		{
			Effect:   model.StatementEffectDeny,
			Action:   fsActions,
			Resource: "arn:lakefs:fs:::repository/*-metadata",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   fsActions,
			Resource: "arn:lakefs:fs:::repository/*-metadata/*",
		},
		{
			Effect:   model.StatementEffectDeny,
			Action:   catalogActions,
			Resource: "arn:lakefs:catalog:::*/*-metadata/*",
		},
	}
	policy := MetadataRepoProtectPolicy(time.Unix(0, 0))
	require.Equal(t, MetadataRepoProtectPolicyName, policy.DisplayName)
	require.True(t, statementsEqual(want, policy.Statement),
		"the mds-metadata-repo-protect policy is the deny list of §8; got %+v", policy.Statement)
}

// bootstrapTS is a fixed creation timestamp, so the idempotency assertions compare exact values.
var bootstrapTS = time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC)

func TestEnsurePrincipals(t *testing.T) {
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err, "one group of the deny's list exists in this installation")

	inForce, err := EnsurePrincipals(t.Context(), svc, bootstrapTS)
	require.NoError(t, err)
	require.True(t, inForce, "a backend that stores policies has the principals in force")

	// The user, its membership and the group.
	_, err = svc.GetUser(t.Context(), UserName)
	require.NoError(t, err, "EnsurePrincipals creates the mds-indexer user")
	groups, _, err := svc.ListUserGroups(t.Context(), UserName, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.Contains(t, groupNames(groups), GroupName, "mds-indexer is a member of MetadataSearchIndexers")

	// Both policies exist with exactly the built-in statements.
	for _, built := range []*model.Policy{IndexerPolicy(bootstrapTS), MetadataRepoProtectPolicy(bootstrapTS)} {
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

// TestEnsurePrincipalsAuthorization checks what the two policies actually decide, through the same
// ACL engine the server uses: the indexer writes companion repositories and only reads data
// repositories, and an administrator's fs:* does not write a companion repository.
func TestEnsurePrincipalsAuthorization(t *testing.T) {
	svc := newACLService(t)
	_, err := svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	_, err = EnsurePrincipals(t.Context(), svc, bootstrapTS)
	require.NoError(t, err)

	// An administrator: a member of Admins (so the deny applies) with an installation-wide allow,
	// the §8 "wins over every allow" case.
	const operator = "operator"
	_, err = svc.CreateUser(t.Context(), &model.User{Username: operator, CreatedAt: bootstrapTS})
	require.NoError(t, err)
	require.NoError(t, svc.AddUserToGroup(t.Context(), operator, "Admins"))
	require.NoError(t, svc.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: "operator-fs-all",
		Statement: model.Statements{
			{Effect: model.StatementEffectAllow, Action: []string{"fs:*"}, Resource: permissions.All},
			{Effect: model.StatementEffectAllow, Action: []string{"catalog:*"}, Resource: permissions.All},
		},
	}, false))
	require.NoError(t, svc.AttachPolicyToUser(t.Context(), "operator-fs-all", operator))

	const (
		dataRepo   = "analytics"
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

func TestEnsurePrincipalsIdempotent(t *testing.T) {
	svc := newACLService(t)

	_, err := EnsurePrincipals(t.Context(), svc, bootstrapTS)
	require.NoError(t, err)
	first, err := svc.GetPolicy(t.Context(), IndexerPolicyName)
	require.NoError(t, err)

	// A second startup, one hour later: nothing is re-created, so the policy keeps its original
	// creation timestamp and its statements.
	later := bootstrapTS.Add(time.Hour)
	inForce, err := EnsurePrincipals(t.Context(), svc, later)
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
			inForce, err := EnsurePrincipals(t.Context(), svc, bootstrapTS)
			require.NoError(t, err)
			require.True(t, inForce)

			want := IndexerPolicy(bootstrapTS)
			if tt.policy.DisplayName == MetadataRepoProtectPolicyName {
				want = MetadataRepoProtectPolicy(bootstrapTS)
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
	inForce, err := EnsurePrincipals(t.Context(), basic, bootstrapTS)
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
	_, err := EnsurePrincipals(t.Context(), policyStub{policyErr: fmt.Errorf("get policy: %w", genuine)}, bootstrapTS)
	require.Error(t, err)
	require.ErrorIs(t, err, genuine, "the real cause is preserved")
	require.EqualError(t, err, fmt.Sprintf("mds bootstrap: read policy %s: get policy: %v", IndexerPolicyName, genuine),
		"a real error is not softened into a warning")
}

func TestEnsurePrincipalsNoAuthService(t *testing.T) {
	_, err := EnsurePrincipals(t.Context(), nil, bootstrapTS)
	require.ErrorIs(t, err, ErrNoAuthorizer)
}

// TestRoutesBuilder covers the builder cmd/lakefs-sso registers: it runs only while
// iceberg.metadata_search.enabled is true, mounts nothing, and degrades like EnsurePrincipals.
func TestRoutesBuilder(t *testing.T) {
	t.Run("disabled section does nothing", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n")
		svc := newACLService(t)
		require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: svc, Logger: logging.DummyLogger{}}, nil),
			"a disabled Metadata Search section is not an error")
		_, err := svc.GetUser(t.Context(), UserName)
		require.ErrorIs(t, err, auth.ErrNotFound, "no principal is created while Metadata Search is off")
		_, err = svc.GetPolicy(t.Context(), IndexerPolicyName)
		require.ErrorIs(t, err, auth.ErrNotFound, "no policy is created while Metadata Search is off")
	})

	t.Run("explicitly disabled section does nothing", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: false\n")
		svc := newACLService(t)
		require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: svc, Logger: logging.DummyLogger{}}, nil))
		_, err := svc.GetUser(t.Context(), UserName)
		require.ErrorIs(t, err, auth.ErrNotFound)
	})

	t.Run("enabled section bootstraps the principals", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
		svc := newACLService(t)
		require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: svc, Logger: logging.DummyLogger{}}, nil))
		_, err := svc.GetUser(t.Context(), UserName)
		require.NoError(t, err)
		_, err = svc.GetPolicy(t.Context(), MetadataRepoProtectPolicyName)
		require.NoError(t, err, "both policies are created")
	})

	t.Run("enabled section without dependencies is an error", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
		require.ErrorIs(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{Logger: logging.DummyLogger{}}, nil),
			ErrMissingDeps, "a missing auth service must not be skipped silently")
		require.ErrorIs(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: newACLService(t)}, nil),
			ErrMissingDeps, "a missing logger must not be skipped silently")
	})

	t.Run("enabled section on a backend without policies degrades", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")
		store := kvtest.GetStore(t.Context(), t)
		basic := auth.NewBasicAuthService(store, crypt.NewSecretStore([]byte("mds-bootstrap-basic-secret")),
			authparams.ServiceCache{Enabled: false}, logging.DummyLogger{})
		require.NoError(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: basic, Logger: logging.DummyLogger{}}, nil),
			"the documented default auth mode must still start the server")
	})

	t.Run("invalid configuration is fatal", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  prefix: not-lakefs\n  metadata_search:\n    enabled: true\n")
		require.Error(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: newACLService(t), Logger: logging.DummyLogger{}}, nil),
			"a configuration error is reported, not hidden behind a disabled builder")
	})
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
