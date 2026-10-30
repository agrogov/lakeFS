package datasets

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// enterpriseExample is the example document of DATASETS_DESIGN.md §5.1, verbatim
// (including the abbreviated commit IDs).
const enterpriseExample = `{
  "name": "customer-360",
  "definition": {
    "format_version": 1,
    "description": "Curated customer view for analytics",
    "metadata": { "classification": "internal", "owner": "data-platform", "tier": "gold", "retention": "1y" },
    "data": [
      {
        "target": "customers",
        "repository": "lakehouse-repo",
        "ref": { "type": "commit", "id": "30f9eb1c..." },
        "type": "prefix",
        "prefix": { "path": "data/customers/" }
      },
      {
        "target": "reference/countries.csv",
        "repository": "ref-data",
        "ref": { "type": "commit", "id": "a1b2c3..." },
        "type": "object",
        "object": { "path": "geo/countries.csv" }
      }
    ]
  },
  "publish": { "message": "Initial dataset with customer objects" }
}`

// objectItem builds an "object" item for tests.
func objectItem(target, path string) DataItem {
	return DataItem{
		Target:     target,
		Repository: "src",
		Ref:        SourceRef{Type: RefTypeCommit, ID: "abc123"},
		Type:       ItemTypeObject,
		Object:     &PathSource{Path: path},
	}
}

// prefixItem builds a "prefix" item for tests.
func prefixItem(target, path string) DataItem {
	return DataItem{
		Target:     target,
		Repository: "src",
		Ref:        SourceRef{Type: RefTypeCommit, ID: "abc123"},
		Type:       ItemTypePrefix,
		Prefix:     &PathSource{Path: path},
	}
}

// canonicalJSON round-trips raw through encoding/json, so that maps are printed
// with sorted keys and whitespace is dropped: the comparable form of a document
// "modulo key order".
func canonicalJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal(raw, &v))
	out, err := json.Marshal(v)
	require.NoError(t, err)
	return out
}

// problemPaths extracts the problem paths in order.
func problemPaths(problems []Problem) []string {
	var paths []string
	for _, p := range problems {
		paths = append(paths, p.Path)
	}
	return paths
}

// TestEnterpriseExampleRoundTrip checks that the §5.1 document survives a parse and
// re-marshal without losing or inventing anything, and that it is already in
// canonical form so that a stored definition is byte-stable modulo key order.
func TestEnterpriseExampleRoundTrip(t *testing.T) {
	var env Envelope
	require.NoError(t, json.Unmarshal([]byte(enterpriseExample), &env))

	require.Equal(t, "customer-360", env.Name)
	require.Equal(t, FormatVersion, env.Definition.FormatVersion)
	require.Equal(t, "Curated customer view for analytics", env.Definition.Description)
	require.Equal(t, map[string]any{
		"classification": "internal",
		"owner":          "data-platform",
		"tier":           "gold",
		"retention":      "1y",
	}, env.Definition.Metadata)
	require.Len(t, env.Definition.Data, 2)

	first := env.Definition.Data[0]
	require.Equal(t, "customers", first.Target)
	require.Equal(t, "lakehouse-repo", first.Repository)
	require.Equal(t, SourceRef{Type: RefTypeCommit, ID: "30f9eb1c..."}, first.Ref)
	require.Equal(t, ItemTypePrefix, first.Type)
	require.Equal(t, &PathSource{Path: "data/customers/"}, first.Prefix)
	require.Nil(t, first.Object)
	require.Nil(t, first.Table)
	require.Nil(t, first.Namespace)

	second := env.Definition.Data[1]
	require.Equal(t, "reference/countries.csv", second.Target)
	require.Equal(t, ItemTypeObject, second.Type)
	require.Equal(t, &PathSource{Path: "geo/countries.csv"}, second.Object)
	require.Nil(t, second.Prefix)

	require.NotNil(t, env.Publish)
	require.Equal(t, "Initial dataset with customer objects", env.Publish.Message)

	// Re-marshalling is byte-stable: the canonical form of the document is exactly
	// what the parsed document marshals to, so no key, value or nesting level is
	// lost (unmarshalling into any would not catch a dropped "omitempty" field) and
	// no field is invented.
	marshalled, err := json.Marshal(env)
	require.NoError(t, err)
	require.Equal(t, string(canonicalJSON(t, []byte(enterpriseExample))), string(canonicalJSON(t, marshalled)))

	// Marshalling twice is stable, which is what "byte-stable" means for storage.
	var reparsed Envelope
	require.NoError(t, json.Unmarshal(marshalled, &reparsed))
	require.Equal(t, env, reparsed)

	// The example is already canonical, so normalising it changes nothing.
	require.Empty(t, env.Definition.Normalise())
	normalised, err := json.Marshal(env)
	require.NoError(t, err)
	require.Equal(t, string(marshalled), string(normalised))
}

// TestEnvelopeDocumentShape locks the JSON member names of the §5.1 document, so
// that a definition file written by Enterprise parses and one written here is
// accepted there.
func TestEnvelopeDocumentShape(t *testing.T) {
	t.Run("minimal", func(t *testing.T) {
		// description, metadata and publish are optional; data is not.
		raw := `{"name":"abc","definition":{"format_version":1,"data":[]}}`
		var env Envelope
		require.NoError(t, json.Unmarshal([]byte(raw), &env))
		require.Equal(t, "abc", env.Name)
		require.Nil(t, env.Publish)
		require.NotNil(t, env.Definition.Data) // "data": [] parses as an empty, non-nil slice

		out, err := json.Marshal(env)
		require.NoError(t, err)
		// No "description", "metadata" or "publish" for absent values.
		require.JSONEq(t, raw, string(out))
	})

	t.Run("reserved iceberg item types parse", func(t *testing.T) {
		raw := `{
			"target": "t",
			"repository": "src",
			"ref": {"type": "commit", "id": "abc"},
			"type": "table",
			"table": {"namespace": "ns", "name": "orders"}
		}`
		var item DataItem
		require.NoError(t, json.Unmarshal([]byte(raw), &item))
		require.Equal(t, ItemTypeTable, item.Type)
		require.Equal(t, &TableSrc{Namespace: "ns", Name: "orders"}, item.Table)

		out, err := json.Marshal(item)
		require.NoError(t, err)
		require.JSONEq(t, raw, string(out))

		var ns DataItem
		require.NoError(t, json.Unmarshal([]byte(`{"type":"namespace","namespace":{"name":"ns"}}`), &ns))
		require.Equal(t, &NsSrc{Name: "ns"}, ns.Namespace)
	})
}

// TestNormalise covers every normalisation rule of §5.2, one case per rule.
func TestNormalise(t *testing.T) {
	tests := []struct {
		name         string
		in           []DataItem
		want         []DataItem
		wantProblems []string
	}{
		{
			name: "prefix source path gains its trailing slash",
			in:   []DataItem{prefixItem("customers", "data/customers")},
			want: []DataItem{prefixItem("customers", "data/customers/")},
		},
		{
			name: "prefix source path keeps a single trailing slash",
			in:   []DataItem{prefixItem("customers", "data/customers///")},
			want: []DataItem{prefixItem("customers", "data/customers/")},
		},
		{
			name: "prefix source path loses its leading slash",
			in:   []DataItem{prefixItem("customers", "/data/customers")},
			want: []DataItem{prefixItem("customers", "data/customers/")},
		},
		{
			name: "empty prefix source path means the whole repository",
			in:   []DataItem{prefixItem("all", "")},
			want: []DataItem{prefixItem("all", "")},
		},
		{
			name: "a prefix source path of slashes is the whole repository",
			in:   []DataItem{prefixItem("all", "/")},
			want: []DataItem{prefixItem("all", "")},
		},
		{
			name: "object source path loses its leading slash",
			in:   []DataItem{objectItem("countries.csv", "/geo/countries.csv")},
			want: []DataItem{objectItem("countries.csv", "geo/countries.csv")},
		},
		{
			name: "empty object source path is left for validation",
			in:   []DataItem{objectItem("countries.csv", "")},
			want: []DataItem{objectItem("countries.csv", "")},
		},
		{
			name: "target loses its leading slash",
			in:   []DataItem{objectItem("/reference/countries.csv", "geo/countries.csv")},
			want: []DataItem{objectItem("reference/countries.csv", "geo/countries.csv")},
		},
		{
			name: "object target has no trailing slash",
			in:   []DataItem{objectItem("reference/countries.csv/", "geo/countries.csv")},
			want: []DataItem{objectItem("reference/countries.csv", "geo/countries.csv")},
		},
		{
			name: "prefix target has its trailing slash stripped",
			in:   []DataItem{prefixItem("customers/", "data/customers/")},
			want: []DataItem{prefixItem("customers", "data/customers/")},
		},
		{
			name: "empty prefix target is the repository root",
			in:   []DataItem{prefixItem("/", "")},
			want: []DataItem{prefixItem("", "")},
		},
		{
			name: "doubled slashes in a target collapse",
			in:   []DataItem{objectItem("reference//countries.csv", "geo/countries.csv")},
			want: []DataItem{objectItem("reference/countries.csv", "geo/countries.csv")},
		},
		{
			name:         "a dot segment in a target is rejected",
			in:           []DataItem{objectItem("reference/./countries.csv", "geo/countries.csv")},
			want:         []DataItem{objectItem("reference/./countries.csv", "geo/countries.csv")},
			wantProblems: []string{"data[0].target"},
		},
		{
			name:         "a dotdot segment in a target is rejected",
			in:           []DataItem{objectItem("reference/../countries.csv", "geo/countries.csv")},
			want:         []DataItem{objectItem("reference/../countries.csv", "geo/countries.csv")},
			wantProblems: []string{"data[0].target"},
		},
		{
			name:         "a target inside the reserved _dataset/ directory is rejected",
			in:           []DataItem{objectItem("_dataset/definition.json", "x.json")},
			want:         []DataItem{objectItem("_dataset/definition.json", "x.json")},
			wantProblems: []string{"data[0].target"},
		},
		{
			name:         "the reserved _dataset target itself is rejected",
			in:           []DataItem{objectItem("_dataset", "x.json")},
			want:         []DataItem{objectItem("_dataset", "x.json")},
			wantProblems: []string{"data[0].target"},
		},
		{
			name: "targets that merely start with _dataset are allowed",
			in:   []DataItem{objectItem("_datasetx/definition.json", "x.json")},
			want: []DataItem{objectItem("_datasetx/definition.json", "x.json")},
		},
		{
			name: "ref type is lower-cased and trimmed",
			in: []DataItem{{
				Target: "t", Repository: " src ", Ref: SourceRef{Type: " COMMIT ", ID: " abc "},
				Type: " Prefix ", Prefix: &PathSource{Path: "p/"},
			}},
			want: []DataItem{{
				Target: "t", Repository: "src", Ref: SourceRef{Type: RefTypeCommit, ID: "abc"},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "p/"},
			}},
		},
		{
			name: "branch and tag refs are kept for the service to resolve",
			in: []DataItem{{
				Target: "t", Repository: "src", Ref: SourceRef{Type: "Tag", ID: "v1"},
				Type: ItemTypeObject, Object: &PathSource{Path: "f"},
			}},
			want: []DataItem{{
				Target: "t", Repository: "src", Ref: SourceRef{Type: RefTypeTag, ID: "v1"},
				Type: ItemTypeObject, Object: &PathSource{Path: "f"},
			}},
		},
		{
			// "a-b" sorts between "a" and "a/b" ('-' 0x2d < '/' 0x2f): sorting is
			// by target string, not by path segments.
			name: "items are sorted by target",
			in: []DataItem{
				prefixItem("a/b", "src-b/"),
				objectItem("a-b", "f"),
				prefixItem("a", "src-a/"),
			},
			want: []DataItem{
				prefixItem("a", "src-a/"),
				objectItem("a-b", "f"),
				prefixItem("a/b", "src-b/"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := Definition{FormatVersion: FormatVersion, Data: tt.in}
			problems := def.Normalise()
			require.Equal(t, tt.wantProblems, problemPaths(problems), "problem paths")
			require.Equal(t, tt.want, def.Data, "normalised data")

			// Normalising again changes nothing; rules that were rejected keep being
			// reported, because the item is deliberately left as submitted.
			if tt.wantProblems == nil {
				require.Empty(t, def.Normalise(), "reported problems on the normalised definition")
				require.Equal(t, tt.want, def.Data, "data changed on the second pass")
				return
			}
			require.Equal(t, tt.wantProblems, problemPaths(def.Normalise()), "problems on the second pass")
			require.Equal(t, tt.want, def.Data, "data changed on the second pass")
		})
	}
}

// TestNormaliseProblemsAreDescriptive checks the wording of the normalisation
// problems, which is what the API returns to a user (§10.4).
func TestNormaliseProblemsAreDescriptive(t *testing.T) {
	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{
		objectItem("a/../b", "f"),
		objectItem("_dataset/x", "f"),
	}}
	problems := def.Normalise()
	require.Equal(t, []Problem{
		{Path: "data[0].target", Message: `must not contain "." or ".." segments`},
		{Path: "data[1].target", Message: `must not be inside the reserved "_dataset" directory`},
	}, problems)
}

// TestNormaliseNilDefinition makes sure Normalise tolerates a nil receiver.
func TestNormaliseNilDefinition(t *testing.T) {
	var def *Definition
	require.Empty(t, def.Normalise())
}
