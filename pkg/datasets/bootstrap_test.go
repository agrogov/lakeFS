// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// bootstrap_test.go covers the §9.5 bootstrap - the four built-in policies, the three groups, the
// managed datasets-all-read policy and their attachments - against a real acl.AuthService on an
// in-memory KV store (§15.1).
package datasets

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// policyNames lists the display names of every policy in the auth service, sorted.
func (f *authzFixture) policyNames(t *testing.T) []string {
	t.Helper()
	policies, _, err := f.svc.ListPolicies(t.Context(), &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.DisplayName)
	}
	sort.Strings(names)
	return names
}

// groupNames lists the display names of every group in the auth service, sorted.
func (f *authzFixture) groupNames(t *testing.T) []string {
	t.Helper()
	groups, _, err := f.svc.ListGroups(t.Context(), &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	names := make([]string, 0, len(groups))
	for _, group := range groups {
		names = append(names, group.DisplayName)
	}
	sort.Strings(names)
	return names
}

// groupPolicyNames lists the display names of the policies attached to group, sorted.
func (f *authzFixture) groupPolicyNames(t *testing.T, group string) []string {
	t.Helper()
	policies, _, err := f.svc.ListGroupPolicies(t.Context(), group, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.DisplayName)
	}
	sort.Strings(names)
	return names
}

func TestBootstrapCreatesTheBuiltins(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)

	require.NoError(t, Bootstrap(t.Context(), f.svc, ts))

	// §9.5: four built-in policies plus the managed all-read policy, and the installation-wide
	// §8.2 datasets-pins-protect deny policy. policyNames sorts with sort.Strings, so upper-case
	// names come first.
	require.Equal(t, []string{
		DatasetsFullAccessPolicyName,
		DatasetsManageMetadataKeysPolicyName,
		DatasetsReadAllPolicyName,
		DatasetsReadWriteAllPolicyName,
		DatasetsAllReadPolicyName,
		DatasetsPinsProtectPolicyName,
	}, f.policyNames(t))

	// §9.5: three groups.
	require.Equal(t, []string{
		DatasetAdminsGroup,
		DatasetConsumersGroup,
		DatasetPublishersGroup,
	}, f.groupNames(t))

	// §9.5 note: only the full-access and read-all groups get the data-plane read policy.
	require.Equal(t, []string{DatasetsFullAccessPolicyName, DatasetsAllReadPolicyName},
		f.groupPolicyNames(t, DatasetAdminsGroup))
	require.Equal(t, []string{DatasetsReadWriteAllPolicyName},
		f.groupPolicyNames(t, DatasetPublishersGroup))
	require.Equal(t, []string{DatasetsReadAllPolicyName, DatasetsAllReadPolicyName},
		f.groupPolicyNames(t, DatasetConsumersGroup))

	// The four built-in policy bodies are exactly the §9.5 statements; the all-read policy with no
	// known datasets is the list statement alone.
	tests := map[string]model.Statements{
		DatasetsFullAccessPolicyName: {
			{Effect: model.StatementEffectAllow, Action: []string{"dataset:*"}, Resource: DatasetArn("*")},
		},
		DatasetsReadWriteAllPolicyName: {
			{Effect: model.StatementEffectAllow, Resource: DatasetArn("*"), Action: []string{
				"dataset:ListDatasets", "dataset:ReadDataset", "dataset:CreateDataset",
				"dataset:UpdateDataset",
				"dataset:ListMetadataKeys", "dataset:ReadMetadataKey",
			}},
		},
		DatasetsReadAllPolicyName: {
			{Effect: model.StatementEffectAllow, Resource: DatasetArn("*"), Action: []string{
				"dataset:ListDatasets", "dataset:ReadDataset",
				"dataset:ListMetadataKeys", "dataset:ReadMetadataKey",
			}},
		},
		DatasetsManageMetadataKeysPolicyName: {
			{Effect: model.StatementEffectAllow, Resource: DatasetArn("*"), Action: []string{
				"dataset:ListMetadataKeys", "dataset:ReadMetadataKey", "dataset:CreateMetadataKey",
				"dataset:UpdateMetadataKey", "dataset:DeleteMetadataKey",
			}},
		},
		DatasetsAllReadPolicyName: {
			{Effect: model.StatementEffectAllow, Resource: permissions.All, Action: []string{
				"fs:ListRepositories", "dataset:ListDatasets",
			}},
		},
		DatasetsPinsProtectPolicyName: {
			{
				Effect: model.StatementEffectDeny,
				Action: []string{
					permissions.DeleteBranchAction, permissions.CreateBranchAction,
					permissions.CreateCommitAction, permissions.RevertBranchAction,
				},
				Resource: "arn:lakefs:fs:::repository/*/branch/_lakefs_ds_pin_*",
			},
		},
	}
	for name, want := range tests {
		policy, err := f.svc.GetPolicy(t.Context(), name)
		require.NoErrorf(t, err, "policy %s", name)
		require.Truef(t, statementsEqual(want, policy.Statement), "policy %s statements: got %+v, want %+v", name, policy.Statement, want)
		require.Equal(t, ts, policy.CreatedAt, "policy %s must carry the bootstrap timestamp", name)
	}

	// §9.5: ReadWriteAll must not hand out access management or metadata-key management, and
	// DatasetsReadAll must not hand out publishing.
	for _, denied := range []string{
		ActionManageAccess,
		ActionCreateMetadataKey, ActionUpdateMetadataKey, ActionDeleteMetadataKey,
	} {
		require.NotContains(t, actionSet(t, f, DatasetsReadWriteAllPolicyName), denied)
	}
	require.NotContains(t, actionSet(t, f, DatasetsReadAllPolicyName), ActionUpdateDataset)
}

// TestBootstrapIsIdempotent is the §15.1 idempotency test: a second run creates no duplicate
// policy, group or attachment and leaves every object untouched.
func TestBootstrapIsIdempotent(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	ctx := t.Context()

	require.NoError(t, Bootstrap(ctx, f.svc, ts, WithBootstrapDatasets([]string{"customer-360", "events"})))
	firstPolicies := f.policyNames(t)
	firstGroups := f.groupNames(t)
	first := make(map[string][]string, len(firstGroups))
	for _, group := range firstGroups {
		first[group] = f.groupPolicyNames(t, group)
	}

	// A later run of the same code path, at a different wall-clock time and with the same
	// datasets: nothing may change, and in particular nothing may be duplicated.
	later := ts.Add(24 * time.Hour)
	require.NoError(t, Bootstrap(ctx, f.svc, later, WithBootstrapDatasets([]string{"events", "customer-360", "events"})))

	require.Equal(t, firstPolicies, f.policyNames(t), "a re-run must not duplicate policies")
	require.Equal(t, firstGroups, f.groupNames(t), "a re-run must not duplicate groups")
	for _, group := range firstGroups {
		require.Equal(t, first[group], f.groupPolicyNames(t, group), "a re-run must not duplicate attachments of %s", group)
	}

	// The built-in policies keep their original creation timestamp: create-if-absent, never
	// rewrite.
	policy, err := f.svc.GetPolicy(ctx, DatasetsReadAllPolicyName)
	require.NoError(t, err)
	require.Equal(t, ts, policy.CreatedAt)
}

// TestBootstrapRewritesEditedPolicies is SEC-6: a managed policy whose statements do not match
// the built-in ones is rewritten, because its name is predictable and a pre-created or edited
// policy would otherwise silently grant (or remove) rights on every dataset.
func TestBootstrapRewritesEditedPolicies(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	ctx := t.Context()
	require.NoError(t, Bootstrap(ctx, f.svc, ts))

	// Someone with auth:WritePolicy but no datasets rights pre-created or edited the read-all
	// policy into an fs:* grant.
	tampered := &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsReadAllPolicyName,
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*"},
			Resource: permissions.All,
		}},
	}
	require.NoError(t, f.svc.WritePolicy(ctx, tampered, true))

	require.NoError(t, Bootstrap(ctx, f.svc, ts.Add(time.Hour)))

	got, err := f.svc.GetPolicy(ctx, DatasetsReadAllPolicyName)
	require.NoError(t, err)
	require.True(t, statementsEqual(datasetsReadAllPolicy(ts).Statement, got.Statement),
		"bootstrap must rewrite a managed policy whose statements do not match")
	require.NotContains(t, statementActions(got.Statement), "fs:*")

	// A second run is a no-op: the managed statements already match.
	before, _, err := f.svc.ListPolicies(ctx, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.NoError(t, Bootstrap(ctx, f.svc, ts.Add(2*time.Hour)))
	after, _, err := f.svc.ListPolicies(ctx, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.Equal(t, len(before), len(after))
}

// statementActions flattens the actions of statements, for assertions that a tampered action
// is gone.
func statementActions(statements model.Statements) []string {
	var actions []string
	for _, statement := range statements {
		actions = append(actions, statement.Action...)
	}
	return actions
}

// TestDatasetsAllReadPolicy pins the managed policy of the §9.5 note: three data-plane statements
// per dataset plus one installation-wide list statement.
func TestDatasetsAllReadPolicy(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	policy := DatasetsAllReadPolicy([]string{"events", "customer-360", "events", ""}, ts)

	require.Equal(t, "datasets-all-read", policy.DisplayName)
	require.NoError(t, acl.ValidatePolicy(policy))
	require.Len(t, policy.Statement, 2*3+1, "three statements per dataset plus the list statement")

	// Sorted and deduplicated, so the statement list is stable for a given dataset set.
	require.Equal(t, "arn:lakefs:fs:::repository/customer-360", policy.Statement[0].Resource)
	require.Equal(t, "arn:lakefs:fs:::repository/customer-360/object/*", policy.Statement[1].Resource)
	require.Equal(t, "arn:lakefs:dataset:::dataset/customer-360", policy.Statement[2].Resource)
	require.Equal(t, "arn:lakefs:fs:::repository/events", policy.Statement[3].Resource)
	require.Equal(t, "arn:lakefs:fs:::repository/events/object/*", policy.Statement[4].Resource)
	require.Equal(t, "arn:lakefs:dataset:::dataset/events", policy.Statement[5].Resource)

	for i := 0; i < 6; i++ {
		require.Equal(t, model.StatementEffectAllow, policy.Statement[i].Effect)
	}
	require.Equal(t, datasetReadActions, policy.Statement[0].Action, "the per-dataset grant reuses the §9.2 read actions")
	require.Equal(t, datasetObjectReadActions, policy.Statement[1].Action)
	require.Equal(t, []string{ActionReadDataset}, policy.Statement[2].Action)

	last := policy.Statement[len(policy.Statement)-1]
	require.Equal(t, permissions.All, last.Resource)
	require.Equal(t, []string{permissions.ListRepositoriesAction, ActionListDatasets}, last.Action,
		"the managed policy must keep the installation-wide listing statement")

	// No dataset known: the policy grants no data access at all.
	empty := DatasetsAllReadPolicy(nil, ts)
	require.Len(t, empty.Statement, 1)
	require.Equal(t, permissions.All, empty.Statement[0].Resource)
}

// TestEnsureDatasetsAllReadPolicy covers the create/update machinery the service and the
// reconciler use: create once, no-op when unchanged, rewrite when the dataset set changes.
func TestEnsureDatasetsAllReadPolicy(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ctx := t.Context()
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)

	// Created.
	changed, err := EnsureDatasetsAllReadPolicy(ctx, f.svc, []string{"customer-360"}, ts)
	require.NoError(t, err)
	require.True(t, changed)
	policy, err := f.svc.GetPolicy(ctx, DatasetsAllReadPolicyName)
	require.NoError(t, err)
	require.Len(t, policy.Statement, 4)

	// Idempotent.
	changed, err = EnsureDatasetsAllReadPolicy(ctx, f.svc, []string{"customer-360"}, ts)
	require.NoError(t, err)
	require.False(t, changed, "an unchanged dataset set must not rewrite the policy")
	require.False(t, f.drift(t, []string{"customer-360"}, ts))

	// A new dataset is added...
	changed, err = EnsureDatasetsAllReadPolicy(ctx, f.svc, []string{"customer-360", "events"}, ts)
	require.NoError(t, err)
	require.True(t, changed)
	policy, err = f.svc.GetPolicy(ctx, DatasetsAllReadPolicyName)
	require.NoError(t, err)
	require.Len(t, policy.Statement, 7)
	require.False(t, f.drift(t, []string{"customer-360", "events"}, ts))

	// ...and a deleted one is removed again; the managed policy shrinks back.
	changed, err = EnsureDatasetsAllReadPolicy(ctx, f.svc, []string{"customer-360"}, ts)
	require.NoError(t, err)
	require.True(t, changed)
	policy, err = f.svc.GetPolicy(ctx, DatasetsAllReadPolicyName)
	require.NoError(t, err)
	require.Len(t, policy.Statement, 4)

	// Drift detection is read-only.
	require.True(t, f.drift(t, []string{"customer-360", "events"}, ts))
	require.False(t, f.drift(t, []string{"customer-360"}, ts),
		"a deleted dataset that is not in the list is not drift")
}

// TestBootstrapAllReadGrantsDataPlaneRead evaluates the managed policy: a member of
// DatasetConsumers can read the data of a dataset that exists at bootstrap time, and not the data
// of any other repository.
func TestBootstrapAllReadGrantsDataPlaneRead(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	ctx := t.Context()

	f.user(t, "consumer")
	require.NoError(t, Bootstrap(ctx, f.svc, ts, WithBootstrapDatasets([]string{"customer-360"})))
	f.addToGroup(t, "consumer", DatasetConsumersGroup)

	consumer := Caller{Username: "consumer"}
	objectRead := func(repo, path string) permissions.Node {
		return permissions.Node{Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repo, path),
		}}
	}
	require.NoError(t, f.authz.Check(ctx, consumer, objectRead("customer-360", "customers/p0.parquet"), ""),
		"a consumer must be able to read the data of an existing dataset")
	require.NoError(t, f.authz.CheckDataset(ctx, consumer, ActionReadDataset, "customer-360"))
	// The consumer grant is data-plane only: it does not publish, delete or manage access.
	for _, action := range []string{ActionUpdateDataset, ActionDeleteDataset, ActionManageAccess} {
		require.Error(t, f.authz.CheckDataset(ctx, consumer, action, "customer-360"),
			"a consumer must not be allowed to %s", action)
	}
	// A repository that is not a dataset stays out of reach.
	require.Error(t, f.authz.Check(ctx, consumer, objectRead("lakehouse-repo", "data/customers/c1.parquet"), ""))
}

// TestBootstrapPinsProtectDeniesPinBranchDeletion pins the §8.2 mitigation end to end: bootstrap
// creates the deny policy, attaches it to the base groups that exist in the installation, and hides
// none of it behind a group the installation does not have. A member of a base group can no longer
// delete a pin branch, in any repository, while ordinary branches stay deletable and re-creatable.
func TestBootstrapPinsProtectDeniesPinBranchDeletion(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	ctx := t.Context()

	// The base group this installation has (the simplified ACL template ships Writers).
	f.group(t, "Writers")
	f.group(t, "custom-operators")
	f.user(t, "writer")
	f.user(t, "operator")
	f.addToGroup(t, "writer", "Writers")
	f.addToGroup(t, "operator", "custom-operators")

	require.NoError(t, Bootstrap(ctx, f.svc, ts, WithPinsProtectGroups([]string{"custom-operators", "missing-group"})))

	// The policy is attached to the existing base group and to the configured extra group; the
	// unknown group is skipped instead of failing the bootstrap.
	require.Contains(t, f.groupPolicyNames(t, "Writers"), DatasetsPinsProtectPolicyName)
	require.Contains(t, f.groupPolicyNames(t, "custom-operators"), DatasetsPinsProtectPolicyName)

	deleteBranch := func(repo, branch string) permissions.Node {
		return permissions.Node{Permission: permissions.Permission{
			Action:   permissions.DeleteBranchAction,
			Resource: permissions.BranchArn(repo, branch),
		}}
	}
	createBranch := func(repo, branch string) permissions.Node {
		return permissions.Node{Permission: permissions.Permission{
			Action:   permissions.CreateBranchAction,
			Resource: permissions.BranchArn(repo, branch),
		}}
	}
	for _, user := range []string{"writer", "operator"} {
		caller := Caller{Username: user}
		require.ErrorIs(t, f.authz.Check(ctx, caller, deleteBranch("lakehouse-repo", "_lakefs_ds_pin_30f9eb1c"), ""),
			ErrPermissionDenied, "%s must not be able to delete a pin branch", user)
		require.ErrorIs(t, f.authz.Check(ctx, caller, deleteBranch("ref-data", "_lakefs_ds_pin_abc"), ""),
			ErrPermissionDenied, "%s must not be able to delete a pin branch in another repository", user)
		require.ErrorIs(t, f.authz.Check(ctx, caller, createBranch("lakehouse-repo", "_lakefs_ds_pin_squat"), ""),
			ErrPermissionDenied, "%s must not be able to pre-create a pin branch", user)
	}

	// The deny is scoped to pin branches: with the default deny-all base policy, an ordinary branch
	// delete is still decided by the ordinary allow rules, not by this policy.
	f.policy(t, "allow-delete-feature", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.DeleteBranchAction},
		Resource: permissions.BranchArn("lakehouse-repo", "feature"),
	}})
	f.attachToGroup(t, "allow-delete-feature", "Writers")
	require.NoError(t, f.authz.Check(ctx, Caller{Username: "writer"}, deleteBranch("lakehouse-repo", "feature"), ""),
		"datasets-pins-protect must not affect ordinary branches")
}

// TestMigrateLegacyManagedPolicies pins the DOC-1 migration: legacy plural datasets actions and
// ARNs in the managed policies are rewritten to the singular dataset namespace, coarse
// ManageMetadataKeys expands to the five documented actions, and the migration is idempotent.
func TestMigrateLegacyManagedPolicies(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ctx := t.Context()
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)

	legacyRead := &model.Policy{
		CreatedAt:   ts,
		DisplayName: "dataset-customer-360-read",
		Statement: model.Statements{
			{Effect: model.StatementEffectAllow, Action: []string{permissions.ReadRepositoryAction}, Resource: permissions.RepoArn("customer-360")},
			{Effect: model.StatementEffectAllow, Action: []string{"datasets:ReadDataset"}, Resource: "arn:lakefs:datasets:::dataset/customer-360"},
			{Effect: model.StatementEffectAllow, Action: []string{permissions.ListRepositoriesAction, "datasets:ListDatasets"}, Resource: permissions.All},
		},
	}
	require.NoError(t, f.svc.WritePolicy(ctx, legacyRead, false))
	require.NoError(t, f.svc.WritePolicy(ctx, &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsFullAccessPolicyName,
		Statement:   model.Statements{{Effect: model.StatementEffectAllow, Action: []string{"datasets:*"}, Resource: permissions.All}},
	}, false))
	require.NoError(t, f.svc.WritePolicy(ctx, &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsManageMetadataKeysPolicyName,
		Statement:   model.Statements{{Effect: model.StatementEffectAllow, Action: []string{"datasets:ManageMetadataKeys"}, Resource: permissions.All}},
	}, false))
	require.NoError(t, f.svc.WritePolicy(ctx, &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsPinsProtectPolicyName,
		Statement:   model.Statements{{Effect: model.StatementEffectDeny, Action: []string{permissions.DeleteBranchAction}, Resource: PinBranchResourcePattern}},
	}, false))

	changed, err := MigrateLegacyManagedPolicies(ctx, f.svc)
	require.NoError(t, err)
	require.True(t, changed)

	read, err := f.svc.GetPolicy(ctx, "dataset-customer-360-read")
	require.NoError(t, err)
	require.Equal(t, model.Statements{
		{Effect: model.StatementEffectAllow, Action: []string{permissions.ReadRepositoryAction}, Resource: permissions.RepoArn("customer-360"), Condition: map[string]map[string][]string{}},
		{Effect: model.StatementEffectAllow, Action: []string{"dataset:ReadDataset"}, Resource: "arn:lakefs:dataset:::dataset/customer-360", Condition: map[string]map[string][]string{}},
		{Effect: model.StatementEffectAllow, Action: []string{permissions.ListRepositoriesAction, "dataset:ListDatasets"}, Resource: permissions.All, Condition: map[string]map[string][]string{}},
	}, read.Statement)

	full, err := f.svc.GetPolicy(ctx, DatasetsFullAccessPolicyName)
	require.NoError(t, err)
	require.Equal(t, []string{"dataset:*"}, full.Statement[0].Action)

	keys, err := f.svc.GetPolicy(ctx, DatasetsManageMetadataKeysPolicyName)
	require.NoError(t, err)
	require.Equal(t, []string{
		ActionListMetadataKeys, ActionReadMetadataKey, ActionCreateMetadataKey,
		ActionUpdateMetadataKey, ActionDeleteMetadataKey,
	}, keys.Statement[0].Action)

	// The pins-protect policy had no legacy token, so it was not rewritten.
	pins, err := f.svc.GetPolicy(ctx, DatasetsPinsProtectPolicyName)
	require.NoError(t, err)
	require.Equal(t, ts, pins.CreatedAt)

	// A second run changes nothing.
	changed, err = MigrateLegacyManagedPolicies(ctx, f.svc)
	require.NoError(t, err)
	require.False(t, changed)
}

// TestBootstrapWithoutServiceFailsClosed pins that a missing auth service is an error, not a
// silent success.
func TestBootstrapWithoutServiceFailsClosed(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, Bootstrap(t.Context(), nil, time.Now()), ErrNoAuthorizer)
	_, err := EnsureDatasetsAllReadPolicy(t.Context(), nil, nil, time.Now())
	require.ErrorIs(t, err, ErrNoAuthorizer)
	_, err = DetectDatasetsAllReadDrift(t.Context(), nil, nil, time.Now())
	require.ErrorIs(t, err, ErrNoAuthorizer)
}

// drift is a test helper around DetectDatasetsAllReadDrift.
func (f *authzFixture) drift(t *testing.T, datasets []string, ts time.Time) bool {
	t.Helper()
	drift, err := DetectDatasetsAllReadDrift(t.Context(), f.svc, datasets, ts)
	require.NoError(t, err)
	return drift
}

// actionSet returns the union of the actions of the named policy.
func actionSet(t *testing.T, f *authzFixture, policyName string) []string {
	t.Helper()
	policy, err := f.svc.GetPolicy(t.Context(), policyName)
	require.NoError(t, err)
	var actions []string
	for _, stmt := range policy.Statement {
		actions = append(actions, stmt.Action...)
	}
	return actions
}

// TestBootstrapFreshRBACInstall pins SEC-1: on a fresh RBAC installation (Admins, SuperUsers,
// Developers and Viewers exist) Bootstrap creates datasets-pins-protect and attaches it to
// Developers and Admins, attaches the built-ins by the documented group mapping, and the
// reconciler pass that follows reports no error.
func TestBootstrapFreshRBACInstall(t *testing.T) {
	t.Parallel()
	f := newServiceFixture(t)
	ctx := f.ctx
	for _, name := range []string{"Admins", "SuperUsers", "Developers", "Viewers"} {
		_, err := f.authService.CreateGroup(ctx, &model.Group{CreatedAt: f.ts, DisplayName: name})
		if !errors.Is(err, auth.ErrAlreadyExists) {
			require.NoError(t, err)
		}
	}
	require.NoError(t, Bootstrap(ctx, f.authService, f.ts))
	require.NoError(t, Bootstrap(ctx, f.authService, f.ts), "bootstrap must be idempotent")

	groupPolicies := func(group string) []string {
		policies, _, err := f.authService.ListGroupPolicies(ctx, group, &model.PaginationParams{Amount: -1})
		require.NoError(t, err)
		names := make([]string, 0, len(policies))
		for _, policy := range policies {
			names = append(names, policy.DisplayName)
		}
		return names
	}
	require.Contains(t, groupPolicies("Developers"), DatasetsPinsProtectPolicyName)
	require.Contains(t, groupPolicies("Admins"), DatasetsPinsProtectPolicyName)

	report, err := f.reconcilerFor().Reconcile(ctx)
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Zero(t, report.Errors)
}

// TestPinsProtectDeniesDevelopers checks that a member of Developers can neither delete nor
// pre-create a pin branch (SEC-9) while an ordinary allow still works elsewhere.
func TestPinsProtectDeniesDevelopers(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ctx := t.Context()
	f.group(t, "Developers")
	f.user(t, "dev")
	f.addToGroup(t, "dev", "Developers")
	require.NoError(t, Bootstrap(ctx, f.svc, time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)))
	require.Contains(t, f.groupPolicyNames(t, "Developers"), DatasetsPinsProtectPolicyName)

	for _, action := range []string{permissions.DeleteBranchAction, permissions.CreateBranchAction} {
		node := permissions.Node{Permission: permissions.Permission{
			Action:   action,
			Resource: permissions.BranchArn("src", "_lakefs_ds_pin_x"),
		}}
		require.ErrorIs(t, f.authz.Check(ctx, Caller{Username: "dev"}, node, ""), ErrPermissionDenied, action)
	}
}

func TestDatasetsReadWriteAllDeniesDelete(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	ctx := t.Context()

	f.user(t, "publisher")
	require.NoError(t, Bootstrap(ctx, f.svc, ts))
	f.addToGroup(t, "publisher", DatasetPublishersGroup)

	caller := Caller{Username: "publisher"}
	require.NoError(t, f.authz.CheckDataset(ctx, caller, ActionUpdateDataset, "customer-360"))
	require.Error(t, f.authz.CheckDataset(ctx, caller, ActionDeleteDataset, "customer-360"),
		"DatasetsReadWriteAll must not grant dataset:DeleteDataset")
	for _, a := range datasetsReadWriteAllPolicy(ts).Statement[0].Action {
		require.NotEqual(t, ActionDeleteDataset, a)
	}
}

func TestMigrateLegacyActionsDeduplicates(t *testing.T) {
	t.Parallel()
	got := migrateLegacyActions([]string{
		legacyActionPrefix + "PublishVersion",
		legacyActionPrefix + "CreateProposal",
		ActionUpdateDataset,
	})
	require.Equal(t, []string{ActionUpdateDataset}, got)
}
