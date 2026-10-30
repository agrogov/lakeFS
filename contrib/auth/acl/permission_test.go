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
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
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

// aclRBACDifference is one action set that the ACL template and the RBAC base group do not share,
// together with the reason the difference is accepted. It is the only way to make an ACL/RBAC pair
// pass without matching, every entry must name its reason, and the reason is printed in the failure
// message.
type aclRBACDifference struct {
	// policyType is the built-in policy type whose actions make up the difference.
	policyType string
	// rbacGranted is true when only the RBAC group grants these actions, false when only the ACL
	// template does.
	rbacGranted bool
	// reason explains why the difference is acceptable; it is shown when the test fails.
	reason string
}

func (d aclRBACDifference) String() string {
	side := "ACL template only"
	if d.rbacGranted {
		side = "RBAC group only"
	}
	return side + " " + d.policyType + ": " + d.reason
}

// TestACLToStatementRBACParity asserts that the ACL templates and the RBAC base groups are two
// encodings of the same rights: for each ACL permission, the effective action set of the ACL
// statement equals the union of the actions of the policies attached to the equivalent RBAC group.
//
// A pair may only differ through aclRBACDifference, which documents and prints every leftover. The
// Super/PRReadWrite difference must never go back into that table: allow-listing it here is how the
// fact that RBAC SuperUsers lacked pr:WritePullRequest - so `rbac-migrate` silently downgraded every
// Supers member (DESIGN_CONFORMANCE.md GAP-06) - stayed invisible to this test.
func TestACLToStatementRBACParity(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	svc := newRBACRecordingService()
	require.NoError(t, setup.CreateRBACBaseGroups(ctx, svc, time.Now()))

	tests := []struct {
		name        string
		acl         model.ACL
		group       string
		differences []aclRBACDifference
	}{
		{
			name:  "Read covers Viewers",
			acl:   model.ACL{Permission: authacl.ReadPermission},
			group: setup.ViewersGroup,
			// Not a Super PR-write leftover, and not introduced by that fix: RBAC Viewers has no
			// FSReadConfig policy, so a Viewer cannot call the storage-config endpoints that the ACL
			// Read template allows. Kept as an explicit, reasoned exception instead of a silent
			// allow-list; see RBAC_DESIGN.md §4.5 #1.
			differences: []aclRBACDifference{{
				policyType:  "FSReadConfig",
				rbacGranted: false,
				reason: "RBAC Viewers does not attach FSReadConfig, while ACL Read grants fs:ReadConfig " +
					"(RBAC_DESIGN.md §4.5 #1 records this as a known remaining difference)",
			}},
		},
		{
			name:  "Write covers Developers",
			acl:   model.ACL{Permission: authacl.WritePermission},
			group: setup.DevelopersGroup,
		},
		{
			name:  "Super covers SuperUsers",
			acl:   model.ACL{Permission: authacl.SuperPermission},
			group: setup.SuperUsersGroup,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statements, err := authacl.ACLToStatement(tc.acl)
			require.NoError(t, err)

			got := actionSet(statements)
			want := actionsForRBACGroup(t, svc, tc.group)
			allowed := make([]string, 0, len(tc.differences))
			for _, difference := range tc.differences {
				allowed = append(allowed, difference.String())
				if difference.rbacGranted {
					removeDocumentedDifference(t, want, difference)
				} else {
					removeDocumentedDifference(t, got, difference)
				}
			}

			require.Equalf(t, want, got, "ACL template and RBAC group %s differ; documented differences: %v", tc.group, allowed)
		})
	}
}

// TestRBACSuperUsersGrantsPRWrite is the regression test for GAP-06: ACL Super grants
// pr:WritePullRequest, so RBAC SuperUsers - the group `rbac-migrate` moves Supers members into -
// must grant it too, or the migration silently takes pull-request write away from every Super.
//
// Both sides are checked with wildcard matching, the way Authorize evaluates an action, so `pr:*`
// counts as granting `pr:WritePullRequest` (a literal set comparison would not).
func TestRBACSuperUsersGrantsPRWrite(t *testing.T) {
	t.Parallel()

	svc := newRBACRecordingService()
	require.NoError(t, setup.CreateRBACBaseGroups(t.Context(), svc, time.Now()))

	aclStatements, err := authacl.ACLToStatement(model.ACL{Permission: authacl.SuperPermission})
	require.NoError(t, err)
	require.True(t, grantsAction(actionSet(aclStatements), permissions.WritePullRequestAction),
		"precondition: ACL Super grants pr:WritePullRequest")
	require.True(t, grantsAction(actionsForRBACGroup(t, svc, setup.SuperUsersGroup), permissions.WritePullRequestAction),
		"RBAC SuperUsers must grant pr:WritePullRequest (GAP-06)")
}

// grantsAction reports whether an action set, as collected from policy statements, grants action:
// entries may be wildcards (`pr:*`), exactly as Authorize evaluates them.
func grantsAction(actions map[string]struct{}, action string) bool {
	for granted := range actions {
		if wildcard.Match(granted, action) {
			return true
		}
	}
	return false
}

// removeDocumentedDifference deletes the actions of diff from side and fails when one of them is not
// granted there, so an exception that no longer applies has to be deleted from the table instead of
// staying behind as dead weight.
func removeDocumentedDifference(t *testing.T, side map[string]struct{}, diff aclRBACDifference) {
	t.Helper()

	actions, err := auth.GetActionsForPolicyType(diff.policyType)
	require.NoError(t, err)
	for _, action := range actions {
		require.Containsf(t, side, action, "stale documented difference (%s): %q is not granted on that side", diff, action)
		delete(side, action)
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
