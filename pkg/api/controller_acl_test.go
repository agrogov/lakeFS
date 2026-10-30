package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authacl "github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/testutil"
)

// This file covers the controller contracts that only hold with a real contrib/auth/acl service
// (DESIGN_CONFORMANCE GAP-10, rows R18/R52, R80 and R82). The other controller tests keep using
// the policyAuthService fake, which stubs out the parts of the ACL service these tests exercise:
// policy persistence, effective-policy evaluation and the group-ACL derivation.
//
// Deliberately not covered here:
//   - the webui half of R52 (the "Custom (policies)" badge), which lives in webui/;
//   - wiring setup through authServiceBuilder end to end (that is GAP-02, owned by pkg/sso).
//
// Policy conditions are not re-tested against the ACL service: they are evaluated by
// auth.CheckPermissions from the request's condition context, and that plumbing is pinned by
// TestListBranchesPolicyConditions, TestListRepositoriesPolicyConditions and
// TestPresignPolicyConditions above.

// newACLAuthService returns a real ACL auth service over a fresh in-memory KV store. The cache is
// disabled so every authorization reads the store, which keeps these tests independent of cache
// invalidation (covered by contrib/auth/acl).
func newACLAuthService(t testing.TB) *authacl.AuthService {
	t.Helper()
	return authacl.NewAuthService(
		kvtest.GetStore(t.Context(), t),
		crypt.NewSecretStore([]byte("some secret")),
		authparams.ServiceCache{Enabled: false},
	)
}

// internalACLController builds a controller wired the way internal RBAC mode wires it: an
// RBAC-internal auth config and a real acl.AuthService as the auth service.
func internalACLController(t testing.TB, deps *dependencies, authService auth.Service) *api.Controller {
	t.Helper()
	return &api.Controller{
		Config: &config.ConfigImpl{Auth: config.Auth{
			AuthUIConfig: config.AuthUIConfig{RBAC: config.AuthRBACInternal},
		}},
		Catalog:      deps.catalog,
		BlockAdapter: deps.catalog.BlockAdapter,
		Auth:         authService,
		Logger:       logging.FromContext(t.Context()),
		Collector:    deps.collector,
	}
}

// adminStatements grants every fs: and auth: action on every resource - the equivalent of the
// built-in admin policies, written literally so the test does not depend on the setup package.
func adminStatements() model.Statements {
	return model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*"},
			Resource: permissions.All,
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"auth:*"},
			Resource: permissions.All,
		},
	}
}

// createUserWithPolicy creates username and attaches a single policy with statements to it.
func createUserWithPolicy(t testing.TB, authService auth.Service, username, policyName string, statements model.Statements) {
	t.Helper()
	ctx := t.Context()
	_, err := authService.CreateUser(ctx, &model.User{Username: username, CreatedAt: time.Now()})
	require.NoError(t, err)
	require.NoError(t, authService.WritePolicy(ctx, &model.Policy{
		CreatedAt:   time.Now(),
		DisplayName: policyName,
		Statement:   statements,
	}, false))
	require.NoError(t, authService.AttachPolicyToUser(ctx, policyName, username))
}

// TestController_PolicyInternalACLService pins R80: in internal mode with a real acl.AuthService the
// policy endpoints are implemented, so policy CRUD returns 2xx instead of the 501 that the legacy
// BasicAuthService returns (that side stays covered by TestController_Policy).
func TestController_PolicyInternalACLService(t *testing.T) {
	_, deps := setupHandler(t)
	ctx := t.Context()

	authService := newACLAuthService(t)
	const username = "internal-policy-admin"
	createUserWithPolicy(t, authService, username, "InternalPolicyAdmin", adminStatements())
	controller := internalACLController(t, deps, authService)

	newRequest := func(method string) *http.Request {
		req := httptest.NewRequest(method, "/auth/policies", nil)
		return req.WithContext(auth.WithUser(req.Context(), &model.User{Username: username}))
	}
	const policyID = "TestInternalPolicy"
	statements := func(effect string) []apigen.Statement {
		return []apigen.Statement{{
			Effect:   effect,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: "*",
		}}
	}

	t.Run("create", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		controller.CreatePolicy(recorder, newRequest(http.MethodPost), apigen.CreatePolicyJSONRequestBody{
			Id:        policyID,
			Statement: statements("allow"),
		})
		require.Equal(t, http.StatusCreated, recorder.Code)

		// The policy is persisted through the ACL service, not just acknowledged.
		policy, err := authService.GetPolicy(ctx, policyID)
		require.NoError(t, err)
		require.Equal(t, policyID, policy.DisplayName)
	})

	t.Run("get", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		controller.GetPolicy(recorder, newRequest(http.MethodGet), policyID)
		require.Equal(t, http.StatusOK, recorder.Code)
		var response apigen.Policy
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		require.Equal(t, policyID, response.Id)
		require.Len(t, response.Statement, 1)
		require.Equal(t, []string{permissions.ReadRepositoryAction}, response.Statement[0].Action)
	})

	t.Run("update", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		controller.UpdatePolicy(recorder, newRequest(http.MethodPut), apigen.UpdatePolicyJSONRequestBody{
			Id:        policyID,
			Statement: statements("deny"),
		}, policyID)
		require.Equal(t, http.StatusOK, recorder.Code)

		policy, err := authService.GetPolicy(ctx, policyID)
		require.NoError(t, err)
		require.Len(t, policy.Statement, 1)
		require.Equal(t, model.StatementEffectDeny, policy.Statement[0].Effect)
	})

	t.Run("delete", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		controller.DeletePolicy(recorder, newRequest(http.MethodDelete), policyID)
		require.Equal(t, http.StatusNoContent, recorder.Code)

		_, err := authService.GetPolicy(ctx, policyID)
		require.ErrorIs(t, err, auth.ErrNotFound)
	})
}

// TestListRepositoriesRepoScopedACLService pins R82: a user whose policy grants access to one
// repository sees only that repository when listing, and is rejected on another repository's
// object. It uses a real acl.AuthService, so the effective policies, the list filter and the
// object authorization all run against the production implementation.
func TestListRepositoriesRepoScopedACLService(t *testing.T) {
	_, deps := setupHandler(t)
	ctx := t.Context()

	allowedRepo := testUniqueRepoName() + "-allowed"
	deniedRepo := testUniqueRepoName() + "-denied"
	for i, name := range []string{allowedRepo, deniedRepo} {
		_, err := deps.catalog.CreateRepository(ctx, name, config.SingleBlockstoreID, onBlock(deps, fmt.Sprintf("acl-scoped-%d", i)), "main", false)
		testutil.Must(t, err)
	}
	testutil.Must(t, deps.catalog.CreateEntry(ctx, allowedRepo, "main", catalog.DBEntry{
		Path:            "file",
		PhysicalAddress: "file-address-" + allowedRepo,
		CreationDate:    time.Now(),
		Size:            1,
		Checksum:        "checksum",
	}))

	authService := newACLAuthService(t)
	const username = "repo-scoped-user"
	createUserWithPolicy(t, authService, username, "RepoScopedPolicy", model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ListRepositoriesAction},
			Resource: permissions.RepoArn(allowedRepo),
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadObjectAction},
			Resource: permissions.ObjectArn(allowedRepo, "*"),
		},
	})
	controller := internalACLController(t, deps, authService)

	newRequest := func(target string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		return req.WithContext(auth.WithUser(req.Context(), &model.User{Username: username}))
	}
	statObject := func(repository string) int {
		recorder := httptest.NewRecorder()
		controller.StatObject(recorder, newRequest("/repositories/"+repository+"/refs/main/objects"), repository, "main", apigen.StatObjectParams{
			Path: "file",
		})
		return recorder.Code
	}

	t.Run("sees only the granted repository", func(t *testing.T) {
		status, repos := listRepositoriesAsUser(t, controller, username)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, []string{allowedRepo}, repos)
	})

	t.Run("reads the granted repository's object", func(t *testing.T) {
		require.Equal(t, http.StatusOK, statObject(allowedRepo))
	})

	t.Run("another repository's object is unauthorized", func(t *testing.T) {
		require.Equal(t, http.StatusUnauthorized, statObject(deniedRepo))
	})
}

// TestGetGroupACL pins the no_acl contract of GetGroupACL (R18, and the api half of R52): the
// endpoint derives a group's ACL from its single ACL policy, so a group with exactly one policy
// returns that ACL, while a group with more than one attached policy is ambiguous and returns 404
// with no_acl: true - which is how the simplified UI shows "Custom (policies)".
func TestGetGroupACL(t *testing.T) {
	_, deps := setupHandler(t)
	ctx := t.Context()

	authService := newACLAuthService(t)
	const caller = "group-acl-admin"
	createUserWithPolicy(t, authService, caller, "GroupACLAdmin", adminStatements())
	controller := internalACLController(t, deps, authService)

	const (
		singlePolicyGroup   = "single-acl-policy-group"
		multiplePolicyGroup = "multiple-policy-group"
		extraPolicyName     = "group-extra-policy"
	)
	for _, group := range []string{singlePolicyGroup, multiplePolicyGroup} {
		_, err := authService.CreateGroup(ctx, &model.Group{DisplayName: group, CreatedAt: time.Now()})
		testutil.Must(t, err)
	}

	// WriteGroupACL writes the group's ACL policy and attaches it - the single-policy state.
	for _, group := range []string{singlePolicyGroup, multiplePolicyGroup} {
		require.NoError(t, authacl.WriteGroupACL(ctx, authService, group, model.ACL{
			Permission: authacl.WritePermission,
		}, time.Now(), false))
	}
	// A second attached policy makes the group's ACL ambiguous.
	require.NoError(t, authService.WritePolicy(ctx, &model.Policy{
		CreatedAt:   time.Now(),
		DisplayName: extraPolicyName,
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: permissions.RepoArn("some-repo"),
		}},
	}, false))
	require.NoError(t, authService.AttachPolicyToGroup(ctx, extraPolicyName, multiplePolicyGroup))

	getGroupACL := func(groupID string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/auth/groups/"+groupID+"/acl", nil)
		req = req.WithContext(auth.WithUser(req.Context(), &model.User{Username: caller}))
		recorder := httptest.NewRecorder()
		controller.GetGroupACL(recorder, req, groupID)
		return recorder
	}

	t.Run("single ACL policy is returned", func(t *testing.T) {
		recorder := getGroupACL(singlePolicyGroup)
		require.Equal(t, http.StatusOK, recorder.Code)
		var response apigen.ACL
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		require.Equal(t, string(authacl.WritePermission), response.Permission)
	})

	t.Run("multiple policies return no_acl", func(t *testing.T) {
		recorder := getGroupACL(multiplePolicyGroup)
		require.Equal(t, http.StatusNotFound, recorder.Code)
		var response apigen.NotFoundOrNoACL
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		require.NotNil(t, response.NoAcl, "the 404 body must set no_acl")
		require.True(t, *response.NoAcl)
		require.Contains(t, response.Message, "Multiple policies")
	})
}
