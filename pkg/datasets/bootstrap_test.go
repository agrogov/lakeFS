// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// bootstrap_test.go covers the §9.5 bootstrap - the four built-in policies, the three groups, the
// managed datasets-all-read policy and their attachments - against a real acl.AuthService on an
// in-memory KV store (§15.1).
package datasets

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
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

	// §9.5: four built-in policies plus the managed all-read policy. policyNames sorts with
	// sort.Strings, so upper-case names come first.
	require.Equal(t, []string{
		DatasetsFullAccessPolicyName,
		DatasetsManageMetadataKeysPolicyName,
		DatasetsReadAllPolicyName,
		DatasetsReadWriteAllPolicyName,
		DatasetsAllReadPolicyName,
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
			{Effect: model.StatementEffectAllow, Action: []string{"datasets:*"}, Resource: permissions.All},
		},
		DatasetsReadWriteAllPolicyName: {
			{Effect: model.StatementEffectAllow, Resource: permissions.All, Action: []string{
				"datasets:ListDatasets", "datasets:CreateDataset", "datasets:ReadDataset",
				"datasets:PublishVersion", "datasets:CreateProposal",
			}},
		},
		DatasetsReadAllPolicyName: {
			{Effect: model.StatementEffectAllow, Resource: permissions.All, Action: []string{
				"datasets:ListDatasets", "datasets:ReadDataset",
			}},
		},
		DatasetsManageMetadataKeysPolicyName: {
			{Effect: model.StatementEffectAllow, Resource: permissions.All, Action: []string{
				"datasets:ManageMetadataKeys",
			}},
		},
		DatasetsAllReadPolicyName: {
			{Effect: model.StatementEffectAllow, Resource: permissions.All, Action: []string{
				"fs:ListRepositories", "datasets:ListDatasets",
			}},
		},
	}
	for name, want := range tests {
		policy, err := f.svc.GetPolicy(t.Context(), name)
		require.NoErrorf(t, err, "policy %s", name)
		require.Truef(t, statementsEqual(want, policy.Statement), "policy %s statements: got %+v, want %+v", name, policy.Statement, want)
		require.Equal(t, ts, policy.CreatedAt, "policy %s must carry the bootstrap timestamp", name)
	}

	// §9.5: ReadWriteAll must not hand out deletion, access management or metadata-key
	// management, and DatasetsReadAll must not hand out publishing.
	for _, denied := range []string{
		ActionDeleteDataset, ActionManageAccess, ActionManageMetadataKeys,
	} {
		require.NotContains(t, actionSet(t, f, DatasetsReadWriteAllPolicyName), denied)
	}
	require.NotContains(t, actionSet(t, f, DatasetsReadAllPolicyName), ActionPublishVersion)
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

// TestBootstrapDoesNotOverwriteEditedPolicies pins the RBAC bootstrap promise (pkg/auth/setup):
// an edited built-in policy is left alone, so a restart does not undo an administrator's change.
func TestBootstrapDoesNotOverwriteEditedPolicies(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	ctx := t.Context()
	require.NoError(t, Bootstrap(ctx, f.svc, ts))

	edited := &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsReadAllPolicyName,
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{ActionListDatasets},
			Resource: permissions.All,
		}},
	}
	require.NoError(t, f.svc.WritePolicy(ctx, edited, true))

	require.NoError(t, Bootstrap(ctx, f.svc, ts.Add(time.Hour)))

	got, err := f.svc.GetPolicy(ctx, DatasetsReadAllPolicyName)
	require.NoError(t, err)
	require.True(t, statementsEqual(edited.Statement, got.Statement), "bootstrap must not overwrite an edited built-in policy")
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
	require.Equal(t, "arn:lakefs:datasets:::dataset/customer-360", policy.Statement[2].Resource)
	require.Equal(t, "arn:lakefs:fs:::repository/events", policy.Statement[3].Resource)
	require.Equal(t, "arn:lakefs:fs:::repository/events/object/*", policy.Statement[4].Resource)
	require.Equal(t, "arn:lakefs:datasets:::dataset/events", policy.Statement[5].Resource)

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
	for _, action := range []string{ActionPublishVersion, ActionDeleteDataset, ActionManageAccess} {
		require.Error(t, f.authz.CheckDataset(ctx, consumer, action, "customer-360"),
			"a consumer must not be allowed to %s", action)
	}
	// A repository that is not a dataset stays out of reach.
	require.Error(t, f.authz.Check(ctx, consumer, objectRead("lakehouse-repo", "data/customers/c1.parquet"), ""))
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
