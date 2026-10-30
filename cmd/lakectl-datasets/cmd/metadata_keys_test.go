package cmd

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// testMetadataKeyBody is a metadata key response (§5.5, §10.2).
const testMetadataKeyBody = `{
  "name": "tier",
  "type": "enum",
  "required": true,
  "allow_multiple": false,
  "allowed_values": ["bronze", "silver", "gold"],
  "description": "medallion tier",
  "created_at": 1767225600000,
  "created_by": "admin"
}`

// TestMetadataKeysListCommand pins the metadata key list request (§10.1).
func TestMetadataKeysListCommand(t *testing.T) {
	listBody := `{"results": [` + testMetadataKeyBody + `], "pagination": ` + emptyPagination + `}`
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: listBody})

	out := mustExecute(t, "metadata-keys", "list", "--amount", "50")
	request := fake.OnlyRequest()
	require.Equal(t, http.MethodGet, request.Method)
	require.Equal(t, controlPlanePath+"/metadata-keys", request.Path)
	require.Equal(t, []string{"50"}, request.Query["amount"])
	require.Contains(t, out, "tier")
	require.Contains(t, out, "enum")
	require.Contains(t, out, "bronze")
}

// TestMetadataKeysCreateCommand pins the metadata key create request (§5.5, §10.1).
func TestMetadataKeysCreateCommand(t *testing.T) {
	t.Run("enum key", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testMetadataKeyBody})
		out := mustExecute(t, "metadata-keys", "create", "tier",
			"--type", "enum", "--allowed-values", "bronze,silver,gold",
			"--required", "--description", "medallion tier")

		request := fake.OnlyRequest()
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, controlPlanePath+"/metadata-keys", request.Path)
		requireJSONBody(t, request.Body, `{
		  "name": "tier",
		  "type": "enum",
		  "required": true,
		  "allow_multiple": false,
		  "allowed_values": ["bronze", "silver", "gold"],
		  "description": "medallion tier"
		}`)
		require.Contains(t, out, "Key: tier")
	})

	t.Run("string key with multiple values", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: `{
		  "name": "owner", "type": "string", "required": false, "allow_multiple": true,
		  "created_at": 1767225600000, "created_by": "admin"
		}`})
		mustExecute(t, "metadata-keys", "create", "owner", "--type", "string", "--allow-multiple")
		requireJSONBody(t, fake.OnlyRequest().Body, `{
		  "name": "owner", "type": "string", "required": false, "allow_multiple": true
		}`)
	})

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing type",
			args:    []string{"metadata-keys", "create", "tier"},
			wantErr: "requires --type",
		},
		{
			name:    "unknown type",
			args:    []string{"metadata-keys", "create", "tier", "--type", "int"},
			wantErr: "invalid --type",
		},
		{
			name:    "enum without allowed values",
			args:    []string{"metadata-keys", "create", "tier", "--type", "enum"},
			wantErr: "requires at least one --allowed-values",
		},
		{
			name:    "allowed values on a non-enum key",
			args:    []string{"metadata-keys", "create", "owner", "--type", "string", "--allowed-values", "a"},
			wantErr: "only allowed for \"enum\" keys",
		},
		{
			name:    "missing key name",
			args:    []string{"metadata-keys", "create", "--type", "string"},
			wantErr: "accepts 1 arg(s)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testMetadataKeyBody})
			_, err := executeCommand(t, tt.args...)
			require.ErrorContains(t, err, tt.wantErr)
			require.Empty(t, fake.Requests())
		})
	}
}

// TestMetadataKeysDeleteCommand pins the metadata key delete request (§10.1).
func TestMetadataKeysDeleteCommand(t *testing.T) {
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusNoContent})
	out := mustExecute(t, "metadata-keys", "delete", "tier")

	request := fake.OnlyRequest()
	require.Equal(t, http.MethodDelete, request.Method)
	require.Equal(t, controlPlanePath+"/metadata-keys/tier", request.Path)
	require.Contains(t, out, `Metadata key "tier" deleted`)
}
