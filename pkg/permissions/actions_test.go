package permissions_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/permissions"
)

func TestIsValidActionDatasets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		action  string
		wantErr error
	}{
		{name: "list", action: "dataset:ListDatasets"},
		{name: "read", action: "dataset:ReadDataset"},
		{name: "update", action: "dataset:UpdateDataset"},
		{name: "manage access", action: "dataset:ManageAccess"},
		{name: "list metadata keys", action: "dataset:ListMetadataKeys"},
		{name: "read metadata key", action: "dataset:ReadMetadataKey"},
		{name: "create metadata key", action: "dataset:CreateMetadataKey"},
		{name: "update metadata key", action: "dataset:UpdateMetadataKey"},
		{name: "delete metadata key", action: "dataset:DeleteMetadataKey"},
		{name: "wildcard", action: "dataset:*"},
		// The plural is a deprecated alias kept so old policies still validate.
		{name: "deprecated plural alias", action: "datasets:ReadDataset"},
		{name: "unknown service", action: "unknown:ReadDataset", wantErr: permissions.ErrInvalidServiceName},
		{name: "no service", action: "ReadDataset", wantErr: permissions.ErrInvalidAction},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := permissions.IsValidAction(tc.action)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// TestDatasetsActionsNotInPicker records that the dataset action constants live in
// pkg/datasets/authz.go rather than actions.go: the service is validated by prefix, but no
// dataset:* action is listed in permissions.Actions, so the policy-editor action picker does
// not offer them. actions.gen.go is generated from the constants in actions.go only.
func TestDatasetsActionsNotInPicker(t *testing.T) {
	t.Parallel()

	require.NoError(t, permissions.IsValidAction("dataset:ListDatasets"))
	for _, action := range permissions.Actions {
		service := strings.SplitN(action, ":", 2)[0]
		require.NotEqual(t, "dataset", service,
			"permissions.Actions must not offer dataset actions until the constants move to actions.go; got %q", action)
		require.NotEqual(t, "datasets", service,
			"permissions.Actions must not offer deprecated datasets actions; got %q", action)
	}
}

// TestCatalogTableDataActions covers the data-plane catalog actions added for the Iceberg REST
// catalog (ICEBERG_CATALOG_PLAN.md §7.2): they must be valid, offered by the policy-editor action
// picker, and granted by the built-in policies that imply data access.
func TestCatalogTableDataActions(t *testing.T) {
	t.Parallel()

	for _, action := range []string{
		permissions.CatalogReadTableDataAction,
		permissions.CatalogWriteTableDataAction,
	} {
		require.NoError(t, permissions.IsValidAction(action))
		require.Contains(t, permissions.Actions, action)
	}

	require.Equal(t, "catalog:ReadTableData", permissions.CatalogReadTableDataAction)
	require.Equal(t, "catalog:WriteTableData", permissions.CatalogWriteTableDataAction)

	catalogRead := policyActions(t, "CatalogRead")
	require.Contains(t, catalogRead, permissions.CatalogReadTableDataAction,
		"CatalogRead must allow reading table data (credential vending / remote signing)")
	require.NotContains(t, catalogRead, permissions.CatalogWriteTableDataAction,
		"CatalogRead must not allow writing table data")

	// CatalogReadWrite grants catalog:* - a prefix wildcard that already matches the data actions;
	// assert the wildcard rather than the expanded action list.
	catalogReadWrite := policyActions(t, "CatalogReadWrite")
	require.Contains(t, catalogReadWrite, "catalog:*")
}

// policyActions returns the action patterns of a built-in policy.
func policyActions(t *testing.T, name string) []string {
	t.Helper()
	statements, err := auth.MakeStatementForPolicyType(name, []string{permissions.All})
	require.NoError(t, err)
	require.Len(t, statements, 1)
	return statements[0].Action
}

func TestAllActions(t *testing.T) {
	t.Parallel()
	actions := permissions.Actions

	if !slices.Contains(actions, permissions.ReadUserAction) {
		t.Errorf("Expected actions %v to include %s", actions, permissions.ReadUserAction)
	}

	if !slices.Contains(actions, permissions.ReadActionsAction) {
		t.Errorf("Expected actions %v to include %s", actions, permissions.ReadActionsAction)
	}

	if slices.Contains(actions, "IsValidAction") {
		t.Errorf("Expected actions %v not to include IsValidAction", actions)
	}
}
