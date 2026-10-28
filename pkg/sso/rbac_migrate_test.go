package sso_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/sso"
)

const (
	migrationSuperUser  = "super-user"
	migrationWriter     = "writer-user"
	migrationReader     = "reader-user"
	migrationTeamACL    = "TeamACL"
	migrationTeamNoACL  = "TeamNoACL"
	migrationTeamMember = "team-acl-user"
)

func createMigrationUser(t *testing.T, ctx context.Context, svc auth.Service, username string) {
	t.Helper()
	_, err := svc.CreateUser(ctx, &model.User{Username: username, CreatedAt: time.Now().UTC()})
	require.NoError(t, err)
}

func addMigrationUser(t *testing.T, ctx context.Context, svc auth.Service, username, group string) {
	t.Helper()
	require.NoError(t, svc.AddUserToGroup(ctx, username, group))
}

func groupPolicyNames(t *testing.T, ctx context.Context, svc auth.Service, group string) []string {
	t.Helper()
	policies, _, err := svc.ListGroupPolicies(ctx, group, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.DisplayName)
	}
	return names
}

func requireGroupPolicy(t *testing.T, ctx context.Context, svc auth.Service, group, policy string) {
	t.Helper()
	require.Contains(t, groupPolicyNames(t, ctx, svc, group), policy)
}

func requireUserInGroup(t *testing.T, ctx context.Context, svc auth.Service, username, group string) {
	t.Helper()
	groups, _, err := svc.ListUserGroups(ctx, username, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	for _, g := range groups {
		if g.DisplayName == group {
			return
		}
	}
	t.Fatalf("user %s is not a member of group %s", username, group)
}

// setupLegacyMigrationState creates the ACL groups and users a simplified installation
// would have, plus one custom group with an ACL and one custom group without an ACL.
func setupLegacyMigrationState(t *testing.T, ctx context.Context, svc *acl.AuthService) {
	t.Helper()
	require.NoError(t, sso.EnsureACLBaseGroups(ctx, svc))

	for _, username := range []string{migrationSuperUser, migrationWriter, migrationReader, migrationTeamMember} {
		createMigrationUser(t, ctx, svc, username)
	}
	addMigrationUser(t, ctx, svc, migrationSuperUser, acl.SupersGroup)
	addMigrationUser(t, ctx, svc, migrationWriter, acl.WritersGroup)
	addMigrationUser(t, ctx, svc, migrationReader, acl.ReadersGroup)

	_, err := svc.CreateGroup(ctx, &model.Group{DisplayName: migrationTeamACL})
	require.NoError(t, err)
	require.NoError(t, acl.WriteGroupACL(ctx, svc, migrationTeamACL, model.ACL{Permission: acl.ReadPermission}, time.Now().UTC(), false))
	addMigrationUser(t, ctx, svc, migrationTeamMember, migrationTeamACL)

	_, err = svc.CreateGroup(ctx, &model.Group{DisplayName: migrationTeamNoACL})
	require.NoError(t, err)
}

func TestRBACMigrate_DryRunMakesNoChanges(t *testing.T) {
	ctx, _, _, svc := newACLService(t)
	setupLegacyMigrationState(t, ctx, svc)

	var out bytes.Buffer
	err := sso.MigrateACLToRBAC(ctx, svc, sso.RBACMigrationOptions{}, &out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "dry run")
	require.Contains(t, out.String(), "Writers -> Developers")
	require.Contains(t, out.String(), "rolling back")

	// Dry run must not create the RBAC target groups, attach policies or detach the ACL.
	_, err = svc.GetGroup(ctx, setup.SuperUsersGroup)
	require.ErrorIs(t, err, auth.ErrNotFound)
	requireGroupPolicy(t, ctx, svc, acl.AdminsGroup, acl.PolicyName(acl.AdminsGroup))

	plan, err := sso.PlanRBACMigration(ctx, svc, false)
	require.NoError(t, err)
	require.Equal(t, []string{
		"FSFullAccess", "CatalogReadWriteAll", "AuthFullAccess", "RepoManagementFullAccess", "PRReadWriteAll",
	}, plan.AttachAdminPolicies)
	require.Equal(t, []string{acl.PolicyName(acl.AdminsGroup)}, plan.DetachAdminPolicies)
	require.Len(t, plan.MemberCopies, 3)
}

func TestRBACMigrate_ApplyAndRerun(t *testing.T) {
	ctx, _, _, svc := newACLService(t)
	setupLegacyMigrationState(t, ctx, svc)

	var out bytes.Buffer
	err := sso.MigrateACLToRBAC(ctx, svc, sso.RBACMigrationOptions{Apply: true}, &out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "RBAC migration applied")
	require.Contains(t, out.String(), "Supers -> SuperUsers")
	require.Contains(t, out.String(), "Writers -> Developers")
	require.Contains(t, out.String(), "Readers -> Viewers")
	require.Contains(t, out.String(), "rolling back")

	// RBAC base groups exist and members were copied.
	for _, group := range []string{setup.AdminsGroup, setup.SuperUsersGroup, setup.DevelopersGroup, setup.ViewersGroup} {
		_, err := svc.GetGroup(ctx, group)
		require.NoError(t, err)
	}
	requireUserInGroup(t, ctx, svc, migrationSuperUser, setup.SuperUsersGroup)
	requireUserInGroup(t, ctx, svc, migrationWriter, setup.DevelopersGroup)
	requireUserInGroup(t, ctx, svc, migrationReader, setup.ViewersGroup)

	// Legacy memberships remain (so rollback stays lossless without --cleanup).
	requireUserInGroup(t, ctx, svc, migrationSuperUser, acl.SupersGroup)
	requireUserInGroup(t, ctx, svc, migrationWriter, acl.WritersGroup)
	requireUserInGroup(t, ctx, svc, migrationReader, acl.ReadersGroup)

	// Admins has every RBAC policy and no longer has the ACL policy.
	adminPolicies := groupPolicyNames(t, ctx, svc, setup.AdminsGroup)
	for _, policy := range []string{
		"FSFullAccess", "CatalogReadWriteAll", "AuthFullAccess", "RepoManagementFullAccess", "PRReadWriteAll",
	} {
		require.Contains(t, adminPolicies, policy)
	}
	require.NotContains(t, adminPolicies, acl.PolicyName(acl.AdminsGroup))

	// Custom groups are untouched.
	_, err = svc.GetPolicy(ctx, acl.PolicyName(migrationTeamACL))
	require.NoError(t, err)
	requireGroupPolicy(t, ctx, svc, migrationTeamACL, acl.PolicyName(migrationTeamACL))
	requireUserInGroup(t, ctx, svc, migrationTeamMember, migrationTeamACL)
	_, err = svc.GetGroup(ctx, migrationTeamNoACL)
	require.NoError(t, err)

	// The second run is a no-op.
	plan, err := sso.PlanRBACMigration(ctx, svc, false)
	require.NoError(t, err)
	require.True(t, plan.Empty(), "second plan should be empty: %+v", plan)

	out.Reset()
	require.NoError(t, sso.MigrateACLToRBAC(ctx, svc, sso.RBACMigrationOptions{Apply: true}, &out))
	require.Contains(t, out.String(), "no authorization changes")
	require.Equal(t, adminPolicies, groupPolicyNames(t, ctx, svc, setup.AdminsGroup))
	requireUserInGroup(t, ctx, svc, migrationSuperUser, setup.SuperUsersGroup)
}

func TestRBACMigrate_CleanupDeletesLegacyGroupsAndPolicies(t *testing.T) {
	ctx, _, _, svc := newACLService(t)
	setupLegacyMigrationState(t, ctx, svc)

	require.NoError(t, sso.MigrateACLToRBAC(ctx, svc, sso.RBACMigrationOptions{Apply: true}, &bytes.Buffer{}))

	// Cleanup deletes the legacy groups even though group sync has not removed their
	// members yet. DeleteGroup removes the remaining legacy memberships, and the
	// RBAC copies made by the migration are preserved.
	var out bytes.Buffer
	err := sso.MigrateACLToRBAC(ctx, svc, sso.RBACMigrationOptions{Apply: true, Cleanup: true}, &out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "delete legacy group Supers")
	require.Contains(t, out.String(), "delete legacy group Writers")
	require.Contains(t, out.String(), "delete legacy group Readers")

	for _, group := range []string{acl.SupersGroup, acl.WritersGroup, acl.ReadersGroup} {
		_, err := svc.GetGroup(ctx, group)
		require.ErrorIs(t, err, auth.ErrNotFound)
		_, err = svc.GetPolicy(ctx, acl.PolicyName(group))
		require.ErrorIs(t, err, auth.ErrNotFound)
	}
	requireUserInGroup(t, ctx, svc, migrationSuperUser, setup.SuperUsersGroup)
	requireUserInGroup(t, ctx, svc, migrationWriter, setup.DevelopersGroup)
	requireUserInGroup(t, ctx, svc, migrationReader, setup.ViewersGroup)

	// Custom ACL policies survive cleanup.
	_, err = svc.GetPolicy(ctx, acl.PolicyName(migrationTeamACL))
	require.NoError(t, err)

	// A further run is a no-op.
	plan, err := sso.PlanRBACMigration(ctx, svc, true)
	require.NoError(t, err)
	require.True(t, plan.Empty(), "plan after cleanup should be empty: %+v", plan)

	out.Reset()
	require.NoError(t, sso.MigrateACLToRBAC(ctx, svc, sso.RBACMigrationOptions{Apply: true, Cleanup: true}, &out))
	require.Contains(t, out.String(), "no authorization changes")
	_, err = svc.GetGroup(ctx, acl.ReadersGroup)
	require.ErrorIs(t, err, auth.ErrNotFound)
}
