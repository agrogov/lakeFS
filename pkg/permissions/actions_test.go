package permissions_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/permissions"
)

func TestIsValidActionDatasets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		action  string
		wantErr error
	}{
		{name: "list", action: "datasets:ListDatasets"},
		{name: "read", action: "datasets:ReadDataset"},
		{name: "publish", action: "datasets:PublishVersion"},
		{name: "manage access", action: "datasets:ManageAccess"},
		{name: "manage metadata keys", action: "datasets:ManageMetadataKeys"},
		{name: "wildcard", action: "datasets:*"},
		// The ARN service prefix is plural (DATASETS_DESIGN.md §9.1); the enterprise singular is not a service.
		{name: "singular service", action: "dataset:ReadDataset", wantErr: permissions.ErrInvalidServiceName},
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
