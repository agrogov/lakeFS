package setup_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authtestutil "github.com/treeverse/lakefs/contrib/auth/acl/testutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

func SetupService(t *testing.T, secret string) (*auth.BasicAuthService, kv.Store) {
	t.Helper()
	kvStore := kvtest.GetStore(t.Context(), t)
	return auth.NewBasicAuthService(kvStore, crypt.NewSecretStore([]byte(secret)), authparams.ServiceCache{
		Enabled: false,
	}, logging.ContextUnavailable()), kvStore
}

func TestEnsureRBACBaseGroups(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	authService, _ := authtestutil.SetupService(t, ctx, []byte("secret"))
	ts := time.Now()

	require.NoError(t, setup.EnsureRBACBaseGroups(ctx, authService, ts))

	// Edit a built-in policy to make sure a repeated bootstrap keeps admin changes.
	editedFSReadAll := &model.Policy{
		CreatedAt:   ts.Add(time.Minute),
		DisplayName: "FSReadAll",
		Statement: model.Statements{
			{
				Action:    []string{"fs:ReadObject"},
				Resource:  "arn:lakefs:fs:::repository/edited",
				Effect:    model.StatementEffectAllow,
				Condition: map[string]map[string][]string{},
			},
		},
	}
	require.NoError(t, authService.WritePolicy(ctx, editedFSReadAll, true))

	// Running the bootstrap again must not change existing policies or fail.
	require.NoError(t, setup.EnsureRBACBaseGroups(ctx, authService, ts.Add(time.Hour)))

	gotFSReadAll, err := authService.GetPolicy(ctx, "FSReadAll")
	require.NoError(t, err)
	require.Equal(t, editedFSReadAll.DisplayName, gotFSReadAll.DisplayName)
	require.True(t, editedFSReadAll.CreatedAt.Equal(gotFSReadAll.CreatedAt))
	require.Equal(t, editedFSReadAll.Statement, gotFSReadAll.Statement)

	expectedAttachments := map[string][]string{
		setup.AdminsGroup:     {"FSFullAccess", "CatalogReadWriteAll", "AuthFullAccess", "RepoManagementFullAccess"},
		setup.SuperUsersGroup: {"FSFullAccess", "PRReadWriteAll", "CatalogReadWriteAll", "AuthManageOwnCredentials", "RepoManagementReadAll"},
		setup.DevelopersGroup: {"FSReadWriteAll", "PRReadWriteAll", "CatalogReadWriteAll", "AuthManageOwnCredentials", "RepoManagementReadAll"},
		setup.ViewersGroup:    {"FSReadAll", "CatalogReadAll", "AuthManageOwnCredentials"},
	}
	for groupName, expectedPolicies := range expectedAttachments {
		_, err := authService.GetGroup(ctx, groupName)
		require.NoError(t, err)

		policies, _, err := authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		actualPolicies := make([]string, 0, len(policies))
		for _, policy := range policies {
			actualPolicies = append(actualPolicies, policy.DisplayName)
		}
		require.ElementsMatch(t, expectedPolicies, actualPolicies)
	}
}

// TestEnsureRBACBaseGroupsDatasetsDisabled checks the default: without WithDatasetsEnabled the
// datasets policies are not created at all, so they cannot show up unattached in the Policies UI.
func TestEnsureRBACBaseGroupsDatasetsDisabled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	authService, _ := authtestutil.SetupService(t, ctx, []byte("secret"))

	require.NoError(t, setup.EnsureRBACBaseGroups(ctx, authService, time.Now()))

	requireNoDatasetsPolicies(t, ctx, authService)

	// An explicit off must behave the same as the default.
	authServiceOff, _ := authtestutil.SetupService(t, ctx, []byte("secret"))
	require.NoError(t, setup.EnsureRBACBaseGroups(ctx, authServiceOff, time.Now(), setup.WithDatasetsEnabled(false)))
	requireNoDatasetsPolicies(t, ctx, authServiceOff)
}

// TestEnsureRBACBasePoliciesDatasetsDisabled pins the same default on the policy-creation entry
// point, which is the one that used to create the datasets policies unconditionally.
func TestEnsureRBACBasePoliciesDatasetsDisabled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	authService, _ := authtestutil.SetupService(t, ctx, []byte("secret"))

	require.NoError(t, setup.CreateRBACBasePolicies(ctx, authService, time.Now()))

	requireNoDatasetsPolicies(t, ctx, authService)
}

func requireNoDatasetsPolicies(t *testing.T, ctx context.Context, authService auth.Service) {
	t.Helper()

	for _, name := range []string{setup.DatasetsFullAccessPolicy, setup.DatasetsReadAllPolicy} {
		_, err := authService.GetPolicy(ctx, name)
		require.ErrorIs(t, err, auth.ErrNotFound, "policy %s must not be created when the datasets feature is off", name)
	}

	policies, _, err := authService.ListPolicies(ctx, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	for _, policy := range policies {
		require.NotEqual(t, setup.DatasetsFullAccessPolicy, policy.DisplayName)
		require.NotEqual(t, setup.DatasetsReadAllPolicy, policy.DisplayName)
	}

	for _, groupName := range []string{setup.AdminsGroup, setup.SuperUsersGroup, setup.DevelopersGroup, setup.ViewersGroup} {
		attached, _, err := authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		for _, policy := range attached {
			require.NotEqual(t, setup.DatasetsFullAccessPolicy, policy.DisplayName)
			require.NotEqual(t, setup.DatasetsReadAllPolicy, policy.DisplayName)
		}
	}
}

func TestEnsureRBACBaseGroupsDatasetsEnabled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	authService, _ := authtestutil.SetupService(t, ctx, []byte("secret"))
	ts := time.Now()

	require.NoError(t, setup.EnsureRBACBaseGroups(ctx, authService, ts, setup.WithDatasetsEnabled(true)))

	// Policies are created with the datasets actions and the ARN service prefix is plural.
	full, err := authService.GetPolicy(ctx, setup.DatasetsFullAccessPolicy)
	require.NoError(t, err)
	require.Equal(t, model.Statements{{
		Action:    []string{"datasets:*"},
		Resource:  permissions.All,
		Effect:    model.StatementEffectAllow,
		Condition: map[string]map[string][]string{},
	}}, full.Statement)

	read, err := authService.GetPolicy(ctx, setup.DatasetsReadAllPolicy)
	require.NoError(t, err)
	require.Equal(t, model.Statements{{
		Action:    []string{"datasets:List*", "datasets:Read*"},
		Resource:  permissions.All,
		Effect:    model.StatementEffectAllow,
		Condition: map[string]map[string][]string{},
	}}, read.Statement)

	expectedAttachments := map[string][]string{
		setup.AdminsGroup:     {"FSFullAccess", "CatalogReadWriteAll", "AuthFullAccess", "RepoManagementFullAccess", setup.DatasetsFullAccessPolicy},
		setup.SuperUsersGroup: {"FSFullAccess", "PRReadWriteAll", "CatalogReadWriteAll", "AuthManageOwnCredentials", "RepoManagementReadAll", setup.DatasetsFullAccessPolicy},
		setup.DevelopersGroup: {"FSReadWriteAll", "PRReadWriteAll", "CatalogReadWriteAll", "AuthManageOwnCredentials", "RepoManagementReadAll", setup.DatasetsReadAllPolicy},
		setup.ViewersGroup:    {"FSReadAll", "CatalogReadAll", "AuthManageOwnCredentials", setup.DatasetsReadAllPolicy},
	}
	for groupName, expectedPolicies := range expectedAttachments {
		policies, _, err := authService.ListGroupPolicies(ctx, groupName, &model.PaginationParams{Amount: 100})
		require.NoError(t, err)
		actualPolicies := make([]string, 0, len(policies))
		for _, policy := range policies {
			actualPolicies = append(actualPolicies, policy.DisplayName)
		}
		require.ElementsMatch(t, expectedPolicies, actualPolicies)
	}

	// A second run is a no-op.
	require.NoError(t, setup.EnsureRBACBaseGroups(ctx, authService, ts.Add(time.Hour), setup.WithDatasetsEnabled(true)))
	policies, _, err := authService.ListGroupPolicies(ctx, setup.AdminsGroup, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	require.Len(t, policies, len(expectedAttachments[setup.AdminsGroup]))
}

// TestEnsureRBACBasePoliciesDatasetsEnabled checks that the policy-creation entry point honours
// the same option as the group attachments.
func TestEnsureRBACBasePoliciesDatasetsEnabled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	authService, _ := authtestutil.SetupService(t, ctx, []byte("secret"))

	require.NoError(t, setup.CreateRBACBasePolicies(ctx, authService, time.Now(), setup.WithDatasetsEnabled(true)))

	_, err := authService.GetPolicy(ctx, setup.DatasetsFullAccessPolicy)
	require.NoError(t, err)
	_, err = authService.GetPolicy(ctx, setup.DatasetsReadAllPolicy)
	require.NoError(t, err)
}

func TestAddAdminUser(t *testing.T) {
	t.Parallel()
	authService, _ := SetupService(t, "secret")
	ctx := t.Context()

	// setup admin user
	superuser := &model.SuperuserConfiguration{
		User: model.User{
			CreatedAt: time.Now(),
			Username:  "testuser",
		},
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	}
	_, err := setup.AddAdminUser(ctx, authService, superuser, false)
	if err != nil {
		t.Fatal("failed to add admin user:", err)
	}

	// try to add another admin user - should fail
	superuser2 := &model.SuperuserConfiguration{
		User: model.User{
			CreatedAt: time.Now(),
			Username:  "testuser2",
		},
		AccessKeyID:     "key2",
		SecretAccessKey: "secret2",
	}
	_, err = setup.AddAdminUser(ctx, authService, superuser2, false)
	expectedErr := auth.ErrAlreadyExists
	if !errors.Is(err, expectedErr) {
		t.Fatalf("adding another admin. err:%v, expected:%v", err, expectedErr)
	}

	// delete admin user should work
	err = authService.DeleteUser(ctx, superuser.Username)
	if err != nil {
		t.Fatal("failed to delete admin user:", err)
	}

	// setup admin user again without secret access key should fail
	superuser.SecretAccessKey = ""
	_, err = setup.AddAdminUser(ctx, authService, superuser, false)
	expectedErr = auth.ErrNotFound
	if !errors.Is(err, expectedErr) {
		t.Fatalf("adding admin without secret access. key err:%v, expected:%v", err, expectedErr)
	}

	// we should be able to re-add the admin user
	superuser.SecretAccessKey = "new-secret"
	_, err = setup.AddAdminUser(ctx, authService, superuser, false)
	if err != nil {
		t.Fatal("failed to re-add admin user:", err)
	}
}
