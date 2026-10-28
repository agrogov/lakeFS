package acl_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authacl "github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/permissions"
)

func TestACLToStatementActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		acl    model.ACL
		action []string
	}{
		{
			name: "Read",
			acl:  model.ACL{Permission: authacl.ReadPermission},
			action: []string{
				"fs:List*",
				"fs:Read*",
				"fs:ReadConfig",
				permissions.CreateCredentialsAction,
				permissions.DeleteCredentialsAction,
				permissions.ListCredentialsAction,
				permissions.ReadCredentialsAction,
				permissions.CatalogListNamespacesAction,
				permissions.CatalogGetNamespaceAction,
				permissions.CatalogListTablesAction,
				permissions.CatalogReadTableAction,
				permissions.CatalogListViewsAction,
				permissions.CatalogReadViewAction,
			},
		},
		{
			name: "Write",
			acl:  model.ACL{Permission: authacl.WritePermission},
			action: []string{
				"fs:Read*",
				"fs:List*",
				permissions.WriteObjectAction,
				permissions.DeleteObjectAction,
				permissions.RevertBranchAction,
				permissions.CreateBranchAction,
				permissions.CreateTagAction,
				permissions.DeleteBranchAction,
				permissions.DeleteTagAction,
				permissions.CreateCommitAction,
				permissions.CreateCredentialsAction,
				permissions.DeleteCredentialsAction,
				permissions.ListCredentialsAction,
				permissions.ReadCredentialsAction,
				"ci:Read*",
				"retention:Get*",
				"branches:Get*",
				"pr:Read*",
				"pr:List*",
				"fs:ReadConfig",
				"pr:*",
				"catalog:*",
			},
		},
		{
			name: "Super",
			acl:  model.ACL{Permission: authacl.SuperPermission},
			action: []string{
				"fs:*",
				permissions.CreateCredentialsAction,
				permissions.DeleteCredentialsAction,
				permissions.ListCredentialsAction,
				permissions.ReadCredentialsAction,
				"ci:Read*",
				"retention:Get*",
				"branches:Get*",
				"pr:Read*",
				"pr:List*",
				"fs:ReadConfig",
				"pr:*",
				"catalog:*",
			},
		},
		{
			name: "Admin",
			acl:  model.ACL{Permission: authacl.AdminPermission},
			action: []string{
				"fs:*",
				"auth:*",
				"ci:*",
				"retention:*",
				"branches:*",
				"pr:*",
				"catalog:*",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statements, err := authacl.ACLToStatement(tc.acl)
			require.NoError(t, err)

			got := sortedActions(statements)
			want := slices.Clone(tc.action)
			slices.Sort(want)
			require.Equal(t, want, got)
		})
	}
}

func TestACLToStatementRBACParity(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	svc := newRBACRecordingService()
	require.NoError(t, setup.CreateRBACBaseGroups(ctx, svc, time.Now()))

	tests := []struct {
		name            string
		acl             model.ACL
		group           string
		extraPolicyType []string
	}{
		{
			name:            "Read covers Viewers",
			acl:             model.ACL{Permission: authacl.ReadPermission},
			group:           setup.ViewersGroup,
			extraPolicyType: []string{"FSReadConfig"},
		},
		{
			name:  "Write covers Developers",
			acl:   model.ACL{Permission: authacl.WritePermission},
			group: setup.DevelopersGroup,
		},
		{
			name:            "Super covers SuperUsers",
			acl:             model.ACL{Permission: authacl.SuperPermission},
			group:           setup.SuperUsersGroup,
			extraPolicyType: []string{"PRReadWrite"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statements, err := authacl.ACLToStatement(tc.acl)
			require.NoError(t, err)

			want := actionsForRBACGroup(t, svc, tc.group)
			for _, policyType := range tc.extraPolicyType {
				actions, err := auth.GetActionsForPolicyType(policyType)
				require.NoError(t, err)
				for _, action := range actions {
					want[action] = struct{}{}
				}
			}

			require.Equal(t, want, actionSet(statements))
		})
	}
}

// rbacRecordingService records the policies and attachments created by the RBAC base setup.
type rbacRecordingService struct {
	auth.Service
	policies    map[string]model.Statements
	attachments map[string][]string
}

func newRBACRecordingService() *rbacRecordingService {
	return &rbacRecordingService{
		policies:    make(map[string]model.Statements),
		attachments: make(map[string][]string),
	}
}

func (s *rbacRecordingService) CreateGroup(_ context.Context, group *model.Group) (*model.Group, error) {
	return group, nil
}

func (s *rbacRecordingService) WritePolicy(_ context.Context, policy *model.Policy, _ bool) error {
	s.policies[policy.DisplayName] = policy.Statement
	return nil
}

func (s *rbacRecordingService) AttachPolicyToGroup(_ context.Context, policyDisplayName, groupID string) error {
	s.attachments[groupID] = append(s.attachments[groupID], policyDisplayName)
	return nil
}

func actionsForRBACGroup(t *testing.T, svc *rbacRecordingService, group string) map[string]struct{} {
	t.Helper()

	policyNames, ok := svc.attachments[group]
	require.True(t, ok, "RBAC group %q has no attached policies", group)

	actions := make(map[string]struct{})
	for _, policyName := range policyNames {
		statements, ok := svc.policies[policyName]
		require.True(t, ok, "RBAC policy %q was not recorded", policyName)
		for action := range actionSet(statements) {
			actions[action] = struct{}{}
		}
	}
	return actions
}

func actionSet(statements model.Statements) map[string]struct{} {
	actions := make(map[string]struct{})
	for _, statement := range statements {
		for _, action := range statement.Action {
			actions[action] = struct{}{}
		}
	}
	return actions
}

func sortedActions(statements model.Statements) []string {
	actions := make([]string, 0)
	for action := range actionSet(statements) {
		actions = append(actions, action)
	}
	slices.Sort(actions)
	return actions
}
