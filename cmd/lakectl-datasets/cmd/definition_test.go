package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// TestParseDefinitionDocument pins the two document shapes -f accepts (§5.1): a bare definition
// and the portable definition envelope.
func TestParseDefinitionDocument(t *testing.T) {
	tests := []struct {
		name      string
		document  string
		wantName  string
		wantDef   apigen.DatasetDefinition
		wantError string
	}{
		{
			name:     "bare definition",
			document: testDefinition,
			wantDef: apigen.DatasetDefinition{
				FormatVersion: 1,
				Description:   ptr("Curated customer view"),
				Metadata:      &apigen.DatasetMetadata{"tier": "gold"},
				Data: []apigen.DataItem{{
					Target:     "customers",
					Repository: "lakehouse-repo",
					Ref:        apigen.SourceRef{Type: "commit", Id: "30f9eb1c"},
					Type:       "prefix",
					Prefix:     &apigen.PathSource{Path: "data/customers/"},
				}},
			},
		},
		{
			name:     "definition envelope",
			document: `{"name": "customer-360", "definition": ` + testDefinition + `}`,
			wantName: "customer-360",
			wantDef: apigen.DatasetDefinition{
				FormatVersion: 1,
				Description:   ptr("Curated customer view"),
				Metadata:      &apigen.DatasetMetadata{"tier": "gold"},
				Data: []apigen.DataItem{{
					Target:     "customers",
					Repository: "lakehouse-repo",
					Ref:        apigen.SourceRef{Type: "commit", Id: "30f9eb1c"},
					Type:       "prefix",
					Prefix:     &apigen.PathSource{Path: "data/customers/"},
				}},
			},
		},
		{
			name:     "envelope without a name",
			document: `{"definition": {"format_version": 1, "data": []}}`,
			wantDef:  apigen.DatasetDefinition{FormatVersion: 1, Data: []apigen.DataItem{}},
		},
		{
			name:      "invalid JSON",
			document:  `{"format_version": 1,`,
			wantError: "invalid JSON",
		},
		{
			name:      "neither definition nor envelope",
			document:  `{"name": "customer-360"}`,
			wantError: `expected a dataset definition`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDefinitionDocument([]byte(tt.document))
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantName, got.Name)
			require.Equal(t, tt.wantDef, got.Definition)
		})
	}
}

// TestReadDefinitionFile checks the -f plumbing, including the error for a missing file.
func TestReadDefinitionFile(t *testing.T) {
	path := writeDefinitionFile(t, testDefinition)
	loaded, err := readDefinitionFile(path)
	require.NoError(t, err)
	require.Equal(t, 1, loaded.Definition.FormatVersion)
	require.Empty(t, loaded.Name)

	_, err = readDefinitionFile(path + ".missing")
	require.ErrorContains(t, err, "open definition file")
}

// TestParseVersionRef pins the accepted dataset refs (§10.1, §10.3).
func TestParseVersionRef(t *testing.T) {
	tests := []struct {
		value   string
		wantErr bool
	}{
		{value: "latest"},
		{value: "v1"},
		{value: "v42"},
		{value: "3"},
		{value: "proposal-01J8Z3Q4X5"},
		{value: "", wantErr: true},
		{value: "V3", wantErr: true},
		{value: "main", wantErr: true},
		{value: "v", wantErr: true},
		{value: "proposal-", wantErr: true},
		{value: "v1 ", wantErr: true},
	}
	for _, tt := range tests {
		t.Run("value="+tt.value, func(t *testing.T) {
			got, err := parseVersionRef(tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.value, got)
		})
	}
}

// TestParseExpectedVersion pins the optimistic concurrency flag of §5.4.
func TestParseExpectedVersion(t *testing.T) {
	tests := []struct {
		value   string
		want    *int
		wantErr string
	}{
		{value: "", want: nil},
		{value: "0", want: ptr(0)},
		{value: "7", want: ptr(7)},
		{value: "two", wantErr: "expects a version number"},
		{value: "-1", wantErr: "must not be negative"},
	}
	for _, tt := range tests {
		t.Run("value="+tt.value, func(t *testing.T) {
			got, err := parseExpectedVersion(tt.value)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// ptr returns a pointer to value, for the table expectations above.
func ptr[T any](value T) *T { return &value }
