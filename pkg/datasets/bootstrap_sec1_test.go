// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// bootstrap_sec1_test.go covers SEC-1 of DATASETS_REVIEW.md against a fresh KV store in `internal`
// (policy) RBAC mode, which is the mode the review found unprotected:
//
//   - the bootstrap really runs on server start (RoutesBuilder calls it), so the installation-wide
//     datasets-pins-protect deny policy and the built-in groups exist;
//   - that policy is attached to the groups the RBAC template ships (Developers and Admins), so a
//     plain member of Developers can neither delete a pin branch nor squat on its name;
//   - a repair pass reaches its pin repair on a fresh install instead of failing on the groups the
//     ACL template would have.
//
// The end-to-end version of the same scenario - a running lakefs-sso on a throw-away database -
// lives in esti/datasets_test.go (testDatasetsPinProtection).
package datasets

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// internalRBACFixture is a fresh KV store with the `internal` RBAC template applied - the groups
// and policies pkg/sso.EnsureRBACBase creates on a real installation - and the datasets bootstrap
// run the way RoutesBuilder runs it on every start.
type internalRBACFixture struct {
	svc   *acl.AuthService
	authz *Authorizer
}

func newInternalRBACFixture(t *testing.T) *internalRBACFixture {
	t.Helper()
	ctx := t.Context()
	store := kvtest.GetStore(ctx, t)
	svc := acl.NewAuthService(store, crypt.NewSecretStore([]byte("datasets-sec1-test-secret")), authparams.ServiceCache{Enabled: false})
	ts := time.Now()
	require.NoError(t, setup.EnsureRBACBaseGroups(ctx, svc, ts))
	require.NoError(t, Bootstrap(ctx, svc, ts), "the bootstrap RoutesBuilder runs on every start")
	return &internalRBACFixture{svc: svc, authz: NewAuthorizer(svc)}
}

func (f *internalRBACFixture) policyNames(t *testing.T) []string {
	t.Helper()
	policies, _, err := f.svc.ListPolicies(t.Context(), &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.DisplayName)
	}
	return names
}

func (f *internalRBACFixture) groupPolicyNames(t *testing.T, group string) []string {
	t.Helper()
	policies, _, err := f.svc.ListGroupPolicies(t.Context(), group, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.DisplayName)
	}
	return names
}

// user creates a user in the RBAC group the RBAC template ships.
func (f *internalRBACFixture) user(t *testing.T, username, group string) Caller {
	t.Helper()
	ctx := t.Context()
	_, err := f.svc.CreateUser(ctx, &model.User{Username: username, CreatedAt: time.Now()})
	require.NoError(t, err)
	require.NoError(t, f.svc.AddUserToGroup(ctx, username, group))
	return Caller{Username: username}
}

// TestBootstrapOnFreshInternalRBACProtectsPins is SEC-1: on a fresh `internal` RBAC installation the
// datasets-pins-protect policy exists and is attached to Developers and Admins, and a plain member
// of Developers can neither delete a pin branch nor create one.
func TestBootstrapOnFreshInternalRBACProtectsPins(t *testing.T) {
	t.Parallel()
	f := newInternalRBACFixture(t)

	require.Contains(t, f.policyNames(t), DatasetsPinsProtectPolicyName,
		"the bootstrap must create the installation-wide pin protection")
	for _, group := range []string{setup.DevelopersGroup, setup.AdminsGroup} {
		require.Containsf(t, f.groupPolicyNames(t, group), DatasetsPinsProtectPolicyName,
			"datasets-pins-protect must be attached to %s", group)
	}

	developer := f.user(t, "developer", setup.DevelopersGroup)
	ctx := t.Context()

	// §8.2: deleting a pin branch silently disarms the protection of every dataset version pinned
	// to that commit, so GC would later collect data a published version still serves.
	deletePin := permissions.Node{Permission: permissions.Permission{
		Action:   permissions.DeleteBranchAction,
		Resource: permissions.BranchArn("source-repo", "_lakefs_ds_pin_30f9eb1c"),
	}}
	err := f.authz.Check(ctx, developer, deletePin, "")
	require.ErrorIs(t, err, ErrPermissionDenied, "a Developers user must not delete a pin branch")
	var permErr *PermissionError
	require.ErrorAs(t, err, &permErr)
	require.Equal(t, http.StatusForbidden, permErr.HTTPStatusCode())

	// Squatting: a pre-created pin branch makes every publish that pins the commit fail with
	// ErrPinBranchConflict, so creating one is denied as well (SEC-1 bullet 3).
	createPin := permissions.Node{Permission: permissions.Permission{
		Action:   permissions.CreateBranchAction,
		Resource: permissions.BranchArn("source-repo", "_lakefs_ds_pin_30f9eb1c"),
	}}
	require.ErrorIs(t, f.authz.Check(ctx, developer, createPin, ""), ErrPermissionDenied,
		"a Developers user must not be able to squat on a pin branch name")

	// Ordinary branches stay usable by the same user through the ordinary Developers grant: the
	// deny is scoped to pin branches.
	createFeature := permissions.Node{Permission: permissions.Permission{
		Action:   permissions.CreateBranchAction,
		Resource: permissions.BranchArn("source-repo", "feature-branch"),
	}}
	require.NoError(t, f.authz.Check(ctx, developer, createFeature, ""),
		"datasets-pins-protect must not affect ordinary branches")
}

// TestReconcileFreshInstallReturnsCleanPass is the third SEC-1 test: a repair pass on a fresh
// installation must converge with no errors, instead of failing on the groups the installation's
// group template does not have - which is what kept it from reaching its pin repair.
func TestReconcileFreshInstallReturnsCleanPass(t *testing.T) {
	f := newServiceFixture(t)

	// Nothing exists yet: no dataset, so the pass has only the installation-wide work to do.
	report := f.reconcile(t)
	require.Zero(t, report.Errors, "a fresh install must reconcile cleanly: %+v", report)
	require.Equal(t, 0, report.Datasets)

	// The managed policies were already created by the bootstrap; the pass must not have needed to
	// repair them, and it must have re-attached nothing in a loop.
	require.Zero(t, report.PoliciesReattached)
}

// TestReconcileToleratesGroupsMissingFromTheTemplate is the direct regression test for the
// ErrNotFound handling of attachDatasetPolicies: with the ACL template's groups gone (an `internal`
// RBAC installation has Developers/Viewers/SuperUsers, not Writers/Readers/Supers), the pass must
// still succeed instead of reporting an error every interval forever.
func TestReconcileToleratesGroupsMissingFromTheTemplate(t *testing.T) {
	f := newServiceFixture(t)
	report := f.reconcile(t)
	require.Zero(t, report.Errors, "reconcile pass must tolerate groups the template does not have: %+v", report)

	// Deleting a group the reconciler attaches managed policies to must not fail the pass either.
	require.NoError(t, f.authService.DeleteGroup(t.Context(), DatasetConsumersGroup))
	report = f.reconcile(t)
	require.Zero(t, report.Errors, "a missing DatasetConsumers group must be skipped, not reported: %+v", report)
}

// TestRoutesBuilderBootstrapsInstallationPolicies is the wiring half of SEC-1: RoutesBuilder calls
// Bootstrap on every start, so anything a previous run created - or an operator deleted - is
// restored when the server starts, without a separate bootstrap command.
func TestRoutesBuilderBootstrapsInstallationPolicies(t *testing.T) {
	useConfig(t, "datasets:\n  enabled: true\n  storage_namespace_prefix: \"mem://datasets\"\n")
	ctx := t.Context()
	deps := testDeps(t)

	// Take the installation-wide deny policy and the built-in admins group away, as a fresh
	// database or an operator's cleanup would.
	require.NoError(t, deps.AuthService.DeletePolicy(ctx, DatasetsPinsProtectPolicyName))
	require.NoError(t, deps.AuthService.DeleteGroup(ctx, DatasetAdminsGroup))

	require.NoError(t, RoutesBuilder(ctx, deps, coreRouter()))

	_, err := deps.AuthService.GetPolicy(ctx, DatasetsPinsProtectPolicyName)
	require.NoError(t, err, "RoutesBuilder must bootstrap the pin protection on start")

	groups, _, err := deps.AuthService.ListGroups(ctx, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	names := make([]string, 0, len(groups))
	for _, group := range groups {
		names = append(names, group.DisplayName)
	}
	require.Contains(t, names, DatasetAdminsGroup, "RoutesBuilder must bootstrap the built-in groups")

	// attachPinsProtectPolicy covers the base groups of both templates, so the ACL Admins group of
	// this fixture (pkg/auth/setup ACL template) carries the deny policy.
	policies, _, err := deps.AuthService.ListGroupPolicies(ctx, acl.AdminsGroup, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	attached := make([]string, 0, len(policies))
	for _, policy := range policies {
		attached = append(attached, policy.DisplayName)
	}
	require.Contains(t, attached, DatasetsPinsProtectPolicyName,
		"datasets-pins-protect must be attached to the base groups that exist")
}
