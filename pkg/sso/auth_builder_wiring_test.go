package sso

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// The tests in this file pin the §4.2 wiring of BuildAuthService (RBAC_DESIGN.md) that the
// conformance review found untested (DESIGN_CONFORMANCE.md GAP-02). Each one is written so that
// removing or falsifying one wiring step makes it fail:
//
//   - rbacAuthService.IsAdvancedAuth() -> false, or dropping the rbacAuthService wrap:
//     TestBuildAuthService_InternalBootstrapAndWrappers asserts IsAdvancedAuth through all wrappers.
//   - removing the EnsureRBACBase call: the same test asserts the RBAC base groups/policies exist.
//   - removing the ReconcileACLPolicies call: the same test stages a stale ACL-derived policy.
//   - dropping the authServiceWithProfiles wrapper: the same test asserts profileUpdater is reachable.

const builderTestSecret = "builder-test-secret-key"

func builderTestACLService(t *testing.T, store kv.Store) *acl.AuthService {
	t.Helper()
	return acl.NewAuthService(store, crypt.NewSecretStore([]byte(builderTestSecret)), authparams.ServiceCache{Enabled: false})
}

func newBuilderTestConfig(mode string) *config.ConfigImpl {
	cfg := &config.ConfigImpl{}
	cfg.Database.Type = "mem"
	cfg.Auth.GetAuthUIConfig().RBAC = mode
	cfg.Auth.GetBaseAuthConfig().Encrypt.SecretKey = builderTestSecret
	return cfg
}

func setBuilderBreakGlass(t *testing.T, user, keyID, secret string) {
	t.Helper()
	for k, v := range map[string]string{
		"installation.user_name":         user,
		"installation.access_key_id":     keyID,
		"installation.secret_access_key": secret,
	} {
		viper.Set(k, v)
	}
	t.Cleanup(func() {
		for _, k := range []string{"installation.user_name", "installation.access_key_id", "installation.secret_access_key"} {
			viper.Set(k, "")
		}
	})
}

// withoutEmptyConditions normalizes nil and empty condition maps, which is how KV protobuf
// encoding round-trips a statement with no conditions.
func withoutEmptyConditions(statements model.Statements) model.Statements {
	normalized := make(model.Statements, len(statements))
	for i, statement := range statements {
		if len(statement.Condition) == 0 {
			statement.Condition = nil
		}
		normalized[i] = statement
	}
	return normalized
}

func sortedPolicyNames(policies []*model.Policy) []string {
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.DisplayName)
	}
	sort.Strings(names)
	return names
}

func sortedGroupNames(groups []*model.Group) []string {
	names := make([]string, 0, len(groups))
	for _, group := range groups {
		names = append(names, group.DisplayName)
	}
	sort.Strings(names)
	return names
}

func requireACLBaseGroups(t *testing.T, ctx context.Context, svc *acl.AuthService) {
	t.Helper()
	for _, group := range []string{acl.AdminsGroup, acl.SupersGroup, acl.WritersGroup, acl.ReadersGroup} {
		_, err := svc.GetGroup(ctx, group)
		require.NoErrorf(t, err, "ACL base group %s", group)
	}
}

// TestBuildAuthService_InternalBootstrapAndWrappers is the GAP-02 regression test for the internal
// mode bootstrap order and effects: ACL groups -> ACL reconcile -> RBAC base -> break-glass ->
// monitored/profile wrappers.
func TestBuildAuthService_InternalBootstrapAndWrappers(t *testing.T) {
	setSSOEnabled(t, true)
	setBuilderBreakGlass(t, "admin", "AKIABUILDER00001", "builder-secret")

	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	cfg := newBuilderTestConfig(config.AuthRBACInternal)

	// Stage an installation whose ACL base groups exist but whose ACL-derived Writers policy is
	// stale, and whose ACL template version was never recorded, so the builder must reconcile it.
	setupSvc := builderTestACLService(t, store)
	require.NoError(t, acl.CreateACLBaseGroups(ctx, setupSvc, time.Now()))
	stale := &model.Policy{
		CreatedAt:   time.Now(),
		DisplayName: acl.PolicyName(acl.WritersGroup),
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadObjectAction},
			Resource: permissions.All,
		}},
		ACL: model.ACL{Permission: acl.WritePermission},
	}
	require.NoError(t, setupSvc.WritePolicy(ctx, stale, true))

	metadataManager := auth.NewKVMetadataManager("test", "install-id", "mem", store)
	svc := BuildAuthService(ctx, cfg, logging.Dummy(), store, metadataManager)
	require.NotNil(t, svc)

	// IsAdvancedAuth() must survive every wrapper. Dropping the rbacAuthService wrap or making
	// its IsAdvancedAuth return false fails here.
	require.True(t, svc.IsAdvancedAuth(), "internal mode must report advanced auth through the returned service")
	withProfiles, ok := svc.(authServiceWithProfiles)
	require.True(t, ok, "authServiceWithProfiles wrapper was dropped: the profile API is unreachable")
	monitored, ok := withProfiles.Service.(*auth.MonitoredService)
	require.True(t, ok, "MonitoredService wrapper was dropped")
	require.True(t, monitored.Wrapped.IsAdvancedAuth(), "IsAdvancedAuth was lost below the monitored wrapper")
	_, ok = monitored.Wrapped.(rbacAuthService)
	require.True(t, ok, "rbacAuthService wrap was dropped below the monitored wrapper")

	// The profile-update API that oidc_provider asserts the built service to must be reachable.
	_, ok = svc.(profileUpdater)
	require.True(t, ok, "profileUpdater is not reachable through the built service")

	aclSvc := builderTestACLService(t, store)

	// ACL base groups were bootstrapped (idempotently) on the fresh KV.
	requireACLBaseGroups(t, ctx, aclSvc)

	// ReconcileACLPolicies ran: the stale template was rewritten from the current ACL statements.
	wantStatements, err := acl.ACLToStatement(model.ACL{Permission: acl.WritePermission})
	require.NoError(t, err)
	gotPolicy, err := aclSvc.GetPolicy(ctx, acl.PolicyName(acl.WritersGroup))
	require.NoError(t, err)
	require.Equal(t, withoutEmptyConditions(wantStatements), withoutEmptyConditions(gotPolicy.Statement),
		"stale ACL policy was not reconciled")

	// EnsureRBACBase ran: the RBAC base groups, policies and attachments exist.
	for _, group := range []string{"Admins", "SuperUsers", "Developers", "Viewers"} {
		_, err := aclSvc.GetGroup(ctx, group)
		require.NoErrorf(t, err, "RBAC base group %s", group)
	}
	for _, policy := range []string{"FSFullAccess", "CatalogReadWriteAll", "AuthFullAccess", "RepoManagementFullAccess"} {
		_, err := aclSvc.GetPolicy(ctx, policy)
		require.NoErrorf(t, err, "RBAC base policy %s", policy)
	}
	adminPolicies, _, err := aclSvc.ListGroupPolicies(ctx, "Admins", &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.Equal(t,
		[]string{"ACL(_-_)Admins", "AuthFullAccess", "CatalogReadWriteAll", "FSFullAccess", "RepoManagementFullAccess"},
		sortedPolicyNames(adminPolicies),
		"Admins group does not hold the ACL admin policy plus the RBAC admin policies")

	// The break-glass admin is seeded after the groups it joins exist, joins Admins only, and
	// receives the effective admin policies.
	_, err = aclSvc.GetUser(ctx, "admin")
	require.NoError(t, err, "break-glass admin was not created")
	groups, _, err := aclSvc.ListUserGroups(ctx, "admin", &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.Equal(t, []string{"Admins"}, sortedGroupNames(groups), "break-glass admin must be in Admins only")

	effective, _, err := aclSvc.ListEffectivePolicies(ctx, "admin", &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.Equal(t,
		[]string{"ACL(_-_)Admins", "AuthFullAccess", "CatalogReadWriteAll", "FSFullAccess", "RepoManagementFullAccess"},
		sortedPolicyNames(effective),
		"break-glass admin does not hold the expected effective policies")
}

// TestBuildAuthService_ModeMatrix pins the normalized mode -> service-capability mapping, and the
// RBAC bootstrap being internal-only.
func TestBuildAuthService_ModeMatrix(t *testing.T) {
	setSSOEnabled(t, true)

	tests := []struct {
		name     string
		in       string
		wantMode string
		advanced bool
	}{
		{name: "empty", in: "", wantMode: config.AuthRBACSimplified, advanced: false},
		{name: "none", in: config.AuthRBACNone, wantMode: config.AuthRBACSimplified, advanced: false},
		{name: "simplified", in: config.AuthRBACSimplified, wantMode: config.AuthRBACSimplified, advanced: false},
		{name: "internal", in: config.AuthRBACInternal, wantMode: config.AuthRBACInternal, advanced: true},
		{name: "external", in: config.AuthRBACExternal, wantMode: config.AuthRBACSimplified, advanced: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := kvtest.GetStore(ctx, t)
			cfg := newBuilderTestConfig(tt.in)
			metadataManager := auth.NewKVMetadataManager("test", "install-id", "mem", store)

			svc := BuildAuthService(ctx, cfg, logging.Dummy(), store, metadataManager)
			require.NotNil(t, svc)
			require.Equal(t, tt.advanced, svc.IsAdvancedAuth())
			// The config is shared by pointer; setup and the API controller must see the mode the
			// service actually runs with.
			require.Equal(t, tt.wantMode, cfg.Auth.GetAuthUIConfig().RBAC)

			aclSvc := builderTestACLService(t, store)
			requireACLBaseGroups(t, ctx, aclSvc)

			_, err := aclSvc.GetGroup(ctx, "SuperUsers")
			if tt.advanced {
				require.NoError(t, err, "internal mode must bootstrap the RBAC base groups")
				_, err = aclSvc.GetPolicy(ctx, "FSFullAccess")
				require.NoError(t, err, "internal mode must bootstrap the RBAC base policies")
			} else {
				require.ErrorIs(t, err, auth.ErrNotFound, "non-internal modes must not bootstrap the RBAC base groups")
			}
		})
	}
}
