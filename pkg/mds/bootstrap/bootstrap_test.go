package bootstrap

// Tests for the sync identity of the current guide (METADATA_SEARCH_REALIGN.md RA5, G12, X1–X4):
// the exact MDSCatalogAccess statements, create/repair/idempotent behaviour, the removal of every
// principal and policy the earlier design installed, the one degraded startup an auth service
// without policies produces, and what the policy actually allows through the real ACL engine the
// lakefs-sso binary uses.
//
// The service is the simplified-ACL implementation, not a mock: the questions are "what does a
// policy written by this bootstrap allow" and "did the legacy principal really disappear", not
// "which methods were called".

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/internal/mdstest"
	"github.com/treeverse/lakefs/pkg/mds/table"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// bootstrapTS is a fixed creation timestamp, so the idempotency assertions compare exact values.
var bootstrapTS = time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC)

func TestCatalogAccessPolicyStatements(t *testing.T) {
	t.Parallel()

	policy := CatalogAccessPolicy(bootstrapTS)
	require.Equal(t, PolicyName, policy.DisplayName)
	require.Equal(t, bootstrapTS, policy.CreatedAt)
	require.Len(t, policy.Statement, 1)

	statement := policy.Statement[0]
	require.Equal(t, model.StatementEffectAllow, statement.Effect)
	require.Equal(t, []string{"catalog:*"}, statement.Action)
	require.Equal(t, CatalogAccessResource, statement.Resource)
	require.Equal(t, "arn:lakefs:catalog:::*", statement.Resource,
		"the policy grants catalog permissions, installation-wide and on nothing else")
}

func TestEnsureSyncIdentity(t *testing.T) {
	svc := newACLService(t)

	inForce, err := EnsureSyncIdentity(t.Context(), svc, bootstrapTS)
	require.NoError(t, err)
	require.True(t, inForce, "a backend that stores policies has the identity in force")

	user, err := svc.GetUser(t.Context(), UserName)
	require.NoError(t, err, "mds-service-user is created")
	require.Equal(t, UserName, user.Username)

	policy, err := svc.GetPolicy(t.Context(), PolicyName)
	require.NoError(t, err, "MDSCatalogAccess is created")
	require.True(t, statementsEqual(CatalogAccessPolicy(bootstrapTS).Statement, policy.Statement),
		"the policy carries the built-in statements; got %+v", policy.Statement)

	attached, err := listUserPolicies(t.Context(), svc, UserName)
	require.NoError(t, err)
	require.Len(t, attached, 1, "the user carries exactly the one policy the guide names")
	require.Equal(t, PolicyName, attached[0].DisplayName)

	// Nothing of the earlier design exists on a fresh installation.
	_, err = svc.GetUser(t.Context(), LegacyUserName)
	require.ErrorIs(t, err, auth.ErrNotFound, "the legacy user is absent")
	_, err = svc.GetGroup(t.Context(), LegacyGroupName)
	require.ErrorIs(t, err, auth.ErrNotFound, "the legacy group is absent")
	for _, legacy := range []string{LegacyPolicyName, LegacyMetadataRepoProtectPolicyName} {
		_, err = svc.GetPolicy(t.Context(), legacy)
		require.ErrorIs(t, err, auth.ErrNotFound, "the legacy policy %s is absent", legacy)
	}

	// A second start, one hour later: nothing is re-created, so the user and the policy keep their
	// original creation timestamps.
	later := bootstrapTS.Add(time.Hour)
	inForce, err = EnsureSyncIdentity(t.Context(), svc, later)
	require.NoError(t, err, "a rerun is a success, not an already-exists error")
	require.True(t, inForce)

	again, err := svc.GetUser(t.Context(), UserName)
	require.NoError(t, err)
	require.Equal(t, user.CreatedAt, again.CreatedAt, "an existing user is not re-created")
	policyAgain, err := svc.GetPolicy(t.Context(), PolicyName)
	require.NoError(t, err)
	require.Equal(t, policy.CreatedAt, policyAgain.CreatedAt, "an unchanged policy is not rewritten")
	attached, err = listUserPolicies(t.Context(), svc, UserName)
	require.NoError(t, err)
	require.Len(t, attached, 1, "the attachment is not duplicated")
}

func TestEnsureSyncIdentityRepairsDriftedPolicy(t *testing.T) {
	svc := newACLService(t)
	require.NoError(t, svc.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: PolicyName,
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*", "catalog:*"},
			Resource: permissions.All,
		}},
	}, false /* create */), "a predictable policy name can be pre-created")

	readLog := mdstest.CapturePackageLog(t)
	inForce, err := EnsureSyncIdentity(t.Context(), svc, bootstrapTS)
	require.NoError(t, err)
	require.True(t, inForce)

	stored, err := svc.GetPolicy(t.Context(), PolicyName)
	require.NoError(t, err)
	require.True(t, statementsEqual(CatalogAccessPolicy(bootstrapTS).Statement, stored.Statement),
		"a drifted policy is rewritten to the built-in statements; got %+v", stored.Statement)

	output := readLog()
	require.Contains(t, output, "level=warning", "the repair is logged, not silent")
	require.Contains(t, output, "rewrote a metadata search policy")
	require.Contains(t, output, PolicyName)
}

// TestEnsureSyncIdentityRemovesLegacyPrincipals is X1–X3: every principal and policy the earlier
// design installed is removed, while an operator policy that merely shares a prefix survives, and
// the read-only MDSFSReadAccess policy is detached from mds-service-user but not deleted.
func TestEnsureSyncIdentityRemovesLegacyPrincipals(t *testing.T) {
	svc := newACLService(t)
	_, err := EnsureSyncIdentity(t.Context(), svc, bootstrapTS)
	require.NoError(t, err)

	// The operator's own extra policy: the old design told operators to put an extra storage
	// namespace grant here. It is not a name this package writes, so it must survive.
	const operatorPolicy = "mds-indexer-extra-storage-namespace"
	require.NoError(t, svc.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: operatorPolicy,
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.AttachStorageNamespaceAction},
			Resource: permissions.StorageNamespace("s3://bucket/explicit-metadata"),
		}},
	}, false))

	// The old principals and policies, exactly as an older server left them.
	_, err = svc.CreateUser(t.Context(), &model.User{Username: LegacyUserName, CreatedAt: bootstrapTS})
	require.NoError(t, err)
	_, err = svc.AddCredentials(t.Context(), LegacyUserName, "AKIAOLDINDEXER000001", "old-indexer-secret")
	require.NoError(t, err)
	_, err = svc.CreateGroup(t.Context(), &model.Group{DisplayName: LegacyGroupName, CreatedAt: bootstrapTS})
	require.NoError(t, err)
	require.NoError(t, svc.AddUserToGroup(t.Context(), LegacyUserName, LegacyGroupName))

	legacyPolicies := []*model.Policy{
		{
			CreatedAt:   bootstrapTS,
			DisplayName: LegacyPolicyName,
			Statement: model.Statements{{
				Effect: model.StatementEffectAllow, Action: []string{"fs:ReadRepository"}, Resource: permissions.All,
			}},
		},
		{
			CreatedAt:   bootstrapTS,
			DisplayName: LegacyPolicyName + "-1",
			Statement: model.Statements{{
				Effect: model.StatementEffectAllow, Action: []string{"fs:*"}, Resource: permissions.RepoArn("analytics-metadata"),
			}},
		},
		{
			CreatedAt:   bootstrapTS,
			DisplayName: LegacyMetadataRepoProtectPolicyName,
			Statement: model.Statements{{
				Effect: model.StatementEffectDeny, Action: []string{"fs:Write*"}, Resource: permissions.RepoArn("analytics-metadata"),
			}},
		},
		{
			CreatedAt:   bootstrapTS,
			DisplayName: LegacyMetadataRepoProtectPolicyName + "-1",
			Statement: model.Statements{{
				Effect: model.StatementEffectDeny, Action: []string{"fs:Write*"}, Resource: permissions.RepoArn("orders-metadata"),
			}},
		},
	}
	for _, policy := range legacyPolicies {
		require.NoErrorf(t, svc.WritePolicy(t.Context(), policy, false), "seed legacy policy %s", policy.DisplayName)
	}
	require.NoError(t, svc.AttachPolicyToGroup(t.Context(), LegacyPolicyName, LegacyGroupName))
	_, err = svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: bootstrapTS})
	require.NoError(t, err)
	require.NoError(t, svc.AttachPolicyToGroup(t.Context(), LegacyMetadataRepoProtectPolicyName+"-1", "Admins"))

	// The read-only policy the guide detaches: create it and attach it to the service user.
	require.NoError(t, svc.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: FSReadPolicyName,
		Statement: model.Statements{{
			Effect: model.StatementEffectAllow, Action: []string{"fs:ReadRepository"}, Resource: permissions.All,
		}},
	}, false))
	require.NoError(t, svc.AttachPolicyToUser(t.Context(), FSReadPolicyName, UserName))

	inForce, err := EnsureSyncIdentity(t.Context(), svc, bootstrapTS.Add(time.Hour))
	require.NoError(t, err)
	require.True(t, inForce)

	// The legacy user, its credentials and the group are gone.
	_, err = svc.GetUser(t.Context(), LegacyUserName)
	require.ErrorIs(t, err, auth.ErrNotFound, "mds-indexer is removed")
	_, err = svc.GetGroup(t.Context(), LegacyGroupName)
	require.ErrorIs(t, err, auth.ErrNotFound, "MetadataSearchIndexers is removed")

	// Every policy of both old sets is gone, chunks included.
	for _, policy := range legacyPolicies {
		_, err = svc.GetPolicy(t.Context(), policy.DisplayName)
		require.ErrorIsf(t, err, auth.ErrNotFound, "legacy policy %s is removed", policy.DisplayName)
	}
	// ... and an operator's look-alike policy is not.
	_, err = svc.GetPolicy(t.Context(), operatorPolicy)
	require.NoError(t, err, "the bootstrap removes only names it owns")

	// MDSFSReadAccess is detached from the service user, and the policy itself survives.
	attached, err := listUserPolicies(t.Context(), svc, UserName)
	require.NoError(t, err)
	require.Len(t, attached, 1)
	require.Equal(t, PolicyName, attached[0].DisplayName, "only MDSCatalogAccess stays attached")
	_, err = svc.GetPolicy(t.Context(), FSReadPolicyName)
	require.NoError(t, err, "the policy is detached, not deleted")

	// A second run is a no-op.
	inForce, err = EnsureSyncIdentity(t.Context(), svc, bootstrapTS.Add(2*time.Hour))
	require.NoError(t, err)
	require.True(t, inForce)
}

// TestRemoveLegacyPrincipalsOverAuthAPI exercises the external-auth path for the removals:
// pkg/auth.APIAuthService is what the lakefs-sso binary uses when auth is served remotely, and the
// bootstrap's ListPolicies and DeletePolicy calls (R3S-5) have to work over contrib/auth/acl's HTTP
// controller, not only over the in-process service. The identity's own user-policy attachment is not
// part of this test: contrib/auth/acl's HTTP controller does not implement it, so a deployment that
// serves that controller has to move to a backend that does (the in-process ACL service of the
// default lakefs-sso deployment and the Enterprise auth service both do).
func TestRemoveLegacyPrincipalsOverAuthAPI(t *testing.T) {
	aclService := newACLService(t)
	stale := LegacyMetadataRepoProtectPolicyName + "-7"
	require.NoError(t, aclService.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: stale,
		Statement: model.Statements{{
			Effect:   model.StatementEffectDeny,
			Action:   []string{"fs:Delete*"},
			Resource: permissions.RepoArn("gone-metadata"),
		}},
	}, false /* create */), "seed a stale legacy chunk")
	require.NoError(t, aclService.WritePolicy(t.Context(), &model.Policy{
		CreatedAt:   bootstrapTS,
		DisplayName: LegacyPolicyName,
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:ReadRepository"},
			Resource: permissions.All,
		}},
	}, false))

	server := httptest.NewServer(acl.Serve(aclService, logging.DummyLogger{}))
	t.Cleanup(server.Close)
	apiService, err := auth.NewAPIAuthService(
		server.URL+acl.BaseURL+"/", "", false, false,
		crypt.NewSecretStore([]byte("mds-bootstrap-api-test-secret")),
		authparams.ServiceCache{Enabled: false}, logging.DummyLogger{})
	require.NoError(t, err, "build the auth API client the binary uses for external auth")

	require.NoError(t, removeLegacyPrincipals(t.Context(), apiService))

	for _, policy := range []string{stale, LegacyPolicyName} {
		_, err = apiService.GetPolicy(t.Context(), policy)
		require.ErrorIsf(t, err, auth.ErrNotFound, "the legacy policy %s is deleted over the API (DeletePolicy)", policy)
	}
}

// TestEnsureSyncIdentityDegradesWithoutPolicyBackend drives the documented default auth mode: a
// BasicAuthService cannot store policies, so the sync identity cannot be created. The startup must
// continue with a warning naming the consequence - and must not rename the administrator, which
// BasicAuthService.CreateUser would do if it were called.
func TestEnsureSyncIdentityDegradesWithoutPolicyBackend(t *testing.T) {
	store := kvtest.GetStore(t.Context(), t)
	basic := auth.NewBasicAuthService(store, crypt.NewSecretStore([]byte("mds-bootstrap-basic-secret")),
		authparams.ServiceCache{Enabled: false}, logging.DummyLogger{})
	_, err := basic.CreateUser(t.Context(), &model.User{Username: "admin", CreatedAt: bootstrapTS})
	require.NoError(t, err)

	readLog := mdstest.CapturePackageLog(t)
	inForce, err := EnsureSyncIdentity(t.Context(), basic, bootstrapTS)
	require.NoError(t, err, "a service that cannot store policies must not refuse to start Metadata Search")
	require.False(t, inForce, "nothing is in force on a backend without policies")

	admin, err := basic.GetUser(t.Context(), "admin")
	require.NoError(t, err)
	require.Equal(t, "admin", admin.Username, "the installation's administrator was not renamed")
	_, err = basic.GetUser(t.Context(), UserName)
	require.Error(t, err, "mds-service-user is not created on a backend that cannot authorize it")

	output := readLog()
	require.Contains(t, output, "level=warning", "the degradation is logged loudly, not silently")
	require.Contains(t, output, "NOT created", "the warning says which principals were not created")
	require.Contains(t, output, UserName)
	require.Contains(t, output, PolicyName)
	require.Contains(t, output, auth.ErrNotImplemented.Error(), "the warning carries the reason")
}

// policyStub is an auth.Service whose only implemented method is the one EnsureSyncIdentity calls
// first: GetPolicy. The embedded nil interface is deliberate - a method the bootstrap does not
// expect to reach panics loudly instead of pretending to work.
type policyStub struct {
	auth.Service
	policyErr error
}

func (s policyStub) GetPolicy(context.Context, string) (*model.Policy, error) {
	return nil, s.policyErr
}

// TestEnsureSyncIdentityPolicyErrorIsFatal covers the other half of the degradation contract: a
// backend that does store policies and failed must still fail startup.
func TestEnsureSyncIdentityPolicyErrorIsFatal(t *testing.T) {
	genuine := errors.New("kv: connection refused")
	_, err := EnsureSyncIdentity(t.Context(), policyStub{policyErr: fmt.Errorf("get policy: %w", genuine)}, bootstrapTS)
	require.Error(t, err)
	require.ErrorIs(t, err, genuine, "the real cause is preserved")
	require.EqualError(t, err, fmt.Sprintf("mds bootstrap: read policy %s: get policy: %v", PolicyName, genuine),
		"a real error is not softened into a warning")
}

func TestEnsureSyncIdentityNoAuthService(t *testing.T) {
	_, err := EnsureSyncIdentity(t.Context(), nil, bootstrapTS)
	require.ErrorIs(t, err, ErrNoAuthorizer)
}

// TestCatalogAccessAuthorizesTheSync pins what MDSCatalogAccess decides through the same ACL engine
// the server uses: the sync may operate the Iceberg catalog of any repository (its companion
// repositories are created after the policy exists), and holds no fs: permission at all.
func TestCatalogAccessAuthorizesTheSync(t *testing.T) {
	svc := newACLService(t)
	_, err := EnsureSyncIdentity(t.Context(), svc, bootstrapTS)
	require.NoError(t, err)

	const dataRepo = "analytics"
	metaRepo := dataRepo + "-metadata"
	tests := []struct {
		name     string
		action   string
		resource string
		want     bool
	}{
		{
			name: "creates the namespace of a companion repository",
			action: permissions.CatalogCreateNamespaceAction,
			resource: permissions.CatalogNamespaceArn(metaRepo, "system"), want: true,
		},
		{
			name: "creates the metadata table of a companion repository",
			action: permissions.CatalogCreateTableAction,
			resource: permissions.CatalogTableArn(metaRepo, "system", "object_metadata"), want: true,
		},
		{
			name: "commits to the metadata table",
			action: permissions.CatalogUpdateTableAction,
			resource: permissions.CatalogTableArn(metaRepo, "system", "object_metadata"), want: true,
		},
		{
			name: "uses the data-plane table action of credential vending",
			action: permissions.CatalogWriteTableDataAction,
			resource: permissions.CatalogTableArn(metaRepo, "system", "object_metadata"), want: true,
		},
		{
			name: "does not get an fs: grant on a data repository object",
			action: permissions.WriteObjectAction,
			resource: permissions.ObjectArn(dataRepo, "orders/a.parquet"), want: false,
		},
		{
			name: "does not get an fs: grant to create a repository",
			action: permissions.CreateRepositoryAction,
			resource: permissions.RepoArn(metaRepo), want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := svc.Authorize(t.Context(), &auth.AuthorizationRequest{
				Username: UserName,
				RequiredPermissions: permissions.Node{Permission: permissions.Permission{
					Action:   tt.action,
					Resource: tt.resource,
				}},
			})
			require.NoError(t, err)
			require.Equal(t, tt.want, resp.Allowed, "%s on %s", tt.action, tt.resource)
		})
	}
}

// TestCatalogAccessForAnotherUser is the guide's "to run the sync as another user instead ... attach
// the MDSCatalogAccess policy to it": the policy is a named, attachable policy, and attaching it is
// all the other user needs for the catalog writes.
func TestCatalogAccessForAnotherUser(t *testing.T) {
	svc := newACLService(t)
	_, err := EnsureSyncIdentity(t.Context(), svc, bootstrapTS)
	require.NoError(t, err)

	const other = "sync-runner"
	_, err = svc.CreateUser(t.Context(), &model.User{Username: other, CreatedAt: bootstrapTS})
	require.NoError(t, err)
	require.NoError(t, svc.AttachPolicyToUser(t.Context(), PolicyName, other))

	resp, err := svc.Authorize(t.Context(), &auth.AuthorizationRequest{
		Username: other,
		RequiredPermissions: permissions.Node{Permission: permissions.Permission{
			Action:   permissions.CatalogCreateTableAction,
			Resource: permissions.CatalogTableArn("analytics-metadata", "system", "object_metadata"),
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed, "MDSCatalogAccess attached to another user authorizes the sync's writes")
}

func TestRoutesBuilder(t *testing.T) {
	t.Run("provisions the identity", func(t *testing.T) {
		svc := newACLService(t)
		require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{
			AuthService: svc, Logger: logging.DummyLogger{},
		}))
		_, err := svc.GetUser(t.Context(), UserName)
		require.NoError(t, err)
		_, err = svc.GetPolicy(t.Context(), PolicyName)
		require.NoError(t, err)
	})

	t.Run("missing dependencies are an error", func(t *testing.T) {
		require.ErrorIs(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{Logger: logging.DummyLogger{}}),
			ErrMissingDeps, "a missing auth service must not be skipped silently")
		require.ErrorIs(t,
			RoutesBuilder(t.Context(), cmdpkg.ServerDeps{AuthService: newACLService(t)}),
			ErrMissingDeps, "a missing logger must not be skipped silently")
	})

	t.Run("a backend without policies degrades", func(t *testing.T) {
		store := kvtest.GetStore(t.Context(), t)
		basic := auth.NewBasicAuthService(store, crypt.NewSecretStore([]byte("mds-bootstrap-basic-secret")),
			authparams.ServiceCache{Enabled: false}, logging.DummyLogger{})
		require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{
			AuthService: basic, Logger: logging.DummyLogger{},
		}), "the documented default auth mode must still start the server")
	})
}

// TestSignServiceLoginTokenIsTheSyncIdentity is here as well as in pkg/mds/table because the
// bootstrap's startup check signs the same token: it must be a token auth.VerifyToken accepts, for
// mds-service-user, with an expiry.
func TestSignServiceLoginTokenIsTheSyncIdentity(t *testing.T) {
	const secret = "mds-bootstrap-token-signing-secret"
	now := time.Now()
	token, err := table.SignServiceLoginToken(secret, now)
	require.NoError(t, err)
	claims, err := auth.VerifyToken([]byte(secret), token)
	require.NoError(t, err, "the catalog verifies the token with the server's key")
	require.Equal(t, table.ServiceUserName, claims.Subject)
	require.Equal(t, auth.LoginAudience, claims.Audience)
	require.Equal(t, now.Add(table.ServiceLoginTokenTTL).Unix(), claims.ExpiresAt.Unix(),
		"the token carries the service TTL's expiry")
}

// newACLService returns the simplified-ACL auth service the lakefs-sso binary uses, on an in-memory
// KV store.
func newACLService(t *testing.T) *acl.AuthService {
	t.Helper()
	store := kvtest.GetStore(t.Context(), t)
	return acl.NewAuthService(store, crypt.NewSecretStore([]byte("mds-bootstrap-test-secret")),
		authparams.ServiceCache{Enabled: false})
}

