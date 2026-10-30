package datasets

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// validDefinition is the smallest definition that passes Validate.
func validDefinition() *Definition {
	return &Definition{FormatVersion: FormatVersion, Data: []DataItem{objectItem("countries.csv", "geo/countries.csv")}}
}

// validatePaths runs Validate and returns the problem paths, failing when the
// error is not a *ValidationError.
func validatePaths(t *testing.T, def *Definition, opts ValidateOptions) []string {
	t.Helper()
	err := Validate(def, opts)
	if err == nil {
		return nil
	}
	var verr *ValidationError
	require.ErrorAs(t, err, &verr)
	return problemPaths(verr.Problems)
}

// TestValidateValid makes sure a well-formed definition passes.
func TestValidateValid(t *testing.T) {
	require.NoError(t, Validate(validDefinition(), ValidateOptions{}))
	require.NoError(t, Validate(&Definition{
		FormatVersion: FormatVersion,
		Description:   "desc",
		Metadata:      map[string]any{"tier": "gold", "replicas": 2, "public": true, "tags": []string{"a", "b"}},
		Data:          []DataItem{prefixItem("customers", "data/customers/"), objectItem("a-b", "f")},
	}, ValidateOptions{}))
	// A whole-repository prefix item with the root target is legal on its own.
	require.NoError(t, Validate(&Definition{
		FormatVersion: FormatVersion,
		Data:          []DataItem{prefixItem("", "")},
	}, ValidateOptions{}))
}

// TestValidateName checks §5.2 rule 1 both ways: the pattern boundaries and the
// reserved names.
func TestValidateName(t *testing.T) {
	tests := []struct {
		name         string
		wantProblems int
		wantReserved bool
	}{
		{name: "abc", wantProblems: 0},
		{name: "customer-360", wantProblems: 0},
		{name: "1abc", wantProblems: 0}, // first character may be a digit
		{name: "abc-", wantProblems: 0}, // the pattern allows a trailing hyphen
		{name: strings.Repeat("a", 63), wantProblems: 0},
		{name: strings.Repeat("a", 62) + "1", wantProblems: 0},
		{name: strings.Repeat("a", 64), wantProblems: 1},
		{name: "ab", wantProblems: 1},
		{name: "a", wantProblems: 1},
		{name: "", wantProblems: 1},
		{name: "-abc", wantProblems: 1},
		{name: "a-bc", wantProblems: 0},
		{name: "ABC", wantProblems: 1},
		{name: "a_bc", wantProblems: 1},
		{name: "a.bc", wantProblems: 1},
		{name: "a bc", wantProblems: 1},
		{name: "abç", wantProblems: 1},
		// Reserved names (§5.2 rule 1, §10.1): they match the pattern but would
		// collide with control-plane routes or internal names.
		{name: "config", wantProblems: 1, wantReserved: true},
		{name: "metadata-keys", wantProblems: 1, wantReserved: true},
		// _datasets does not match the pattern either, so it gets two problems.
		{name: "_datasets", wantProblems: 2, wantReserved: true},
		{name: "datasets", wantProblems: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := ValidateName(tt.name)
			require.Len(t, problems, tt.wantProblems)
			for _, p := range problems {
				require.Equal(t, "name", p.Path)
			}

			reservedReported := false
			for _, p := range problems {
				if strings.Contains(p.Message, "is reserved") {
					reservedReported = true
				}
			}
			require.Equal(t, tt.wantReserved, reservedReported, "reserved-name problem")

			// The name check is wired into ValidateEnvelope.
			err := ValidateEnvelope(&Envelope{Name: tt.name, Definition: *validDefinition()}, ValidateOptions{})
			require.Equal(t, tt.wantProblems == 0, err == nil)
		})
	}
}

// TestValidateTargetOverlap checks §5.2 rule 3. Overlaps are reported on the
// later, canonically sorted item, which is the definition the service stores.
func TestValidateTargetOverlap(t *testing.T) {
	tests := []struct {
		name        string
		targets     []string
		wantPaths   []string
		wantMessage []string
	}{
		{
			name:        "a and a/b overlap",
			targets:     []string{"a", "a/b"},
			wantPaths:   []string{"data[1].target"},
			wantMessage: []string{"overlaps data[0]"},
		},
		{
			name:        "submission order does not matter",
			targets:     []string{"a/b", "a"},
			wantPaths:   []string{"data[1].target"},
			wantMessage: []string{"overlaps data[0]"},
		},
		{
			name:    "a and a-b do not overlap",
			targets: []string{"a", "a-b"},
		},
		{
			name:        "a and a/b/c overlap across the sorted order",
			targets:     []string{"a", "a/b/c"},
			wantPaths:   []string{"data[1].target"},
			wantMessage: []string{"overlaps data[0]"},
		},
		{
			// Sorted: a, a-b, a/b/c — the overlap is not between adjacent entries.
			name:        "a, a-b and a/b/c: only the descendant overlaps",
			targets:     []string{"a", "a-b", "a/b/c"},
			wantPaths:   []string{"data[2].target"},
			wantMessage: []string{"overlaps data[0]"},
		},
		{
			name:        "every descendant of a shared ancestor is reported",
			targets:     []string{"a", "a/b", "a/b/c"},
			wantPaths:   []string{"data[1].target", "data[2].target"},
			wantMessage: []string{"overlaps data[0]", "overlaps data[1]"},
		},
		{
			name:    "sibling prefixes and a hyphenated target do not overlap",
			targets: []string{"a/b", "a/c", "a-b"},
		},
		{
			name:    "a/b and ab do not overlap",
			targets: []string{"a/b", "ab"},
		},
		{
			name:        "equal targets overlap",
			targets:     []string{"a", "a"},
			wantPaths:   []string{"data[1].target"},
			wantMessage: []string{"duplicates data[0]"},
		},
		{
			name:        "the root target overlaps everything",
			targets:     []string{"", "a"},
			wantPaths:   []string{"data[1].target"},
			wantMessage: []string{"overlaps data[0]"},
		},
		{
			name:    "the root target alone is fine",
			targets: []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := &Definition{FormatVersion: FormatVersion}
			for _, target := range tt.targets {
				def.Data = append(def.Data, prefixItem(target, "src/"))
			}

			err := Validate(def, ValidateOptions{})
			if len(tt.wantPaths) == 0 {
				require.NoError(t, err)
				return
			}
			var verr *ValidationError
			require.ErrorAs(t, err, &verr)
			require.Equal(t, tt.wantPaths, problemPaths(verr.Problems))
			messages := make([]string, 0, len(verr.Problems))
			for _, p := range verr.Problems {
				messages = append(messages, p.Message)
			}
			require.Equal(t, tt.wantMessage, messages)
		})
	}
}

// TestValidateOverlapIgnoresItemType makes sure the rule is about targets only: an
// object and a prefix item conflict exactly like two prefixes.
func TestValidateOverlapIgnoresItemType(t *testing.T) {
	p := validatePaths(t, &Definition{
		FormatVersion: FormatVersion,
		Data:          []DataItem{objectItem("a", "f"), prefixItem("a/b", "src/")},
	}, ValidateOptions{})
	require.Equal(t, []string{"data[1].target"}, p)

	// Overlaps reported by the API are the §10.4 shape.
	err := Validate(&Definition{
		FormatVersion: FormatVersion,
		Data:          []DataItem{prefixItem("a", "src/"), prefixItem("a/b", "src/"), prefixItem("a/b/c", "src/")},
	}, ValidateOptions{})
	var verr *ValidationError
	require.ErrorAs(t, err, &verr)
	out, marshalErr := json.Marshal(verr.Problems)
	require.NoError(t, marshalErr)
	require.JSONEq(t, `[{"path":"data[1].target","message":"overlaps data[0]"},
		{"path":"data[2].target","message":"overlaps data[1]"}]`, string(out))
}

// TestValidateReservedTargets checks that the reserved _dataset/ prefix is
// rejected wherever the target appears (§6.4).
func TestValidateReservedTargets(t *testing.T) {
	for _, target := range []string{"_dataset", "_dataset/definition.json", "/_dataset/x", "_dataset//x"} {
		t.Run(target, func(t *testing.T) {
			paths := validatePaths(t, &Definition{
				FormatVersion: FormatVersion,
				Data:          []DataItem{objectItem(target, "f")},
			}, ValidateOptions{})
			require.Equal(t, []string{"data[0].target"}, paths)
		})
	}

	// A prefix item whose target lands in the reserved directory is rejected too.
	paths := validatePaths(t, &Definition{
		FormatVersion: FormatVersion,
		Data:          []DataItem{prefixItem("_dataset/defs", "src/")},
	}, ValidateOptions{})
	require.Equal(t, []string{"data[0].target"}, paths)

	// Similar-looking targets are fine.
	require.NoError(t, Validate(&Definition{
		FormatVersion: FormatVersion,
		Data:          []DataItem{objectItem("_datasetx/definition.json", "f"), objectItem("data/_dataset/x", "g")},
	}, ValidateOptions{}))
}

// TestValidateFormatVersion checks the format_version rule.
func TestValidateFormatVersion(t *testing.T) {
	for _, version := range []int{0, 2, -1} {
		def := validDefinition()
		def.FormatVersion = version
		paths := validatePaths(t, def, ValidateOptions{})
		require.Equal(t, []string{"format_version"}, paths)
	}

	def := validDefinition()
	def.FormatVersion = 2
	err := Validate(def, ValidateOptions{})
	var verr *ValidationError
	require.ErrorAs(t, err, &verr)
	require.Equal(t, "must be 1, got 2", verr.Problems[0].Message)
}

// TestValidateDataLength checks "non-empty data" and the max_items limit.
func TestValidateDataLength(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		paths := validatePaths(t, &Definition{FormatVersion: FormatVersion}, ValidateOptions{})
		require.Equal(t, []string{"data"}, paths)
		paths = validatePaths(t, &Definition{FormatVersion: FormatVersion, Data: []DataItem{}}, ValidateOptions{})
		require.Equal(t, []string{"data"}, paths)
	})

	t.Run("nil definition", func(t *testing.T) {
		var verr *ValidationError
		require.ErrorAs(t, Validate(nil, ValidateOptions{}), &verr)
		require.Equal(t, []Problem{{Message: "definition is missing"}}, verr.Problems)
		require.ErrorAs(t, ValidateEnvelope(nil, ValidateOptions{}), &verr)
		require.Equal(t, []Problem{{Message: "document is missing"}}, verr.Problems)
	})

	t.Run("max_items", func(t *testing.T) {
		three := func() *Definition {
			return &Definition{FormatVersion: FormatVersion, Data: []DataItem{
				objectItem("a", "f"), objectItem("b", "f"), objectItem("c", "f"),
			}}
		}

		// opts.MaxItems wins over the default.
		err := Validate(three(), ValidateOptions{MaxItems: 2})
		var verr *ValidationError
		require.ErrorAs(t, err, &verr)
		require.Equal(t, []string{"data"}, problemPaths(verr.Problems))
		require.Equal(t, "must contain at most 2 items, got 3", verr.Problems[0].Message)

		require.NoError(t, Validate(three(), ValidateOptions{MaxItems: 3}))
		// Zero and negative mean the default, not "no limit".
		require.NoError(t, Validate(three(), ValidateOptions{}))
		require.NoError(t, Validate(three(), ValidateOptions{MaxItems: -1}))

		// The default limit itself.
		tooMany := &Definition{FormatVersion: FormatVersion, Data: make([]DataItem, 0, DefaultMaxItems+1)}
		for i := 0; i <= DefaultMaxItems; i++ {
			tooMany.Data = append(tooMany.Data, objectItem(fmt.Sprintf("t-%d", i), "f"))
		}
		err = Validate(tooMany, ValidateOptions{})
		require.ErrorAs(t, err, &verr)
		require.Equal(t, []string{"data"}, problemPaths(verr.Problems))
	})
}

// TestValidateItemStructure checks the per-item structural rules.
func TestValidateItemStructure(t *testing.T) {
	tests := []struct {
		name      string
		item      DataItem
		wantPaths []string
	}{
		{
			name:      "repository is required",
			item:      DataItem{Target: "t", Ref: SourceRef{Type: RefTypeCommit, ID: "c"}, Type: ItemTypeObject, Object: &PathSource{Path: "f"}},
			wantPaths: []string{"data[0].repository"},
		},
		{
			name:      "ref type must be commit, branch or tag",
			item:      DataItem{Target: "t", Repository: "r", Ref: SourceRef{Type: "sha1", ID: "c"}, Type: ItemTypeObject, Object: &PathSource{Path: "f"}},
			wantPaths: []string{"data[0].ref.type"},
		},
		{
			name:      "ref id is required",
			item:      DataItem{Target: "t", Repository: "r", Ref: SourceRef{Type: RefTypeBranch}, Type: ItemTypeObject, Object: &PathSource{Path: "f"}},
			wantPaths: []string{"data[0].ref.id"},
		},
		{
			name:      "item type must be object or prefix",
			item:      DataItem{Target: "t", Repository: "r", Ref: SourceRef{Type: RefTypeCommit, ID: "c"}, Type: "file"},
			wantPaths: []string{"data[0].type"},
		},
		{
			name:      "object item needs a target",
			item:      DataItem{Repository: "r", Ref: SourceRef{Type: RefTypeCommit, ID: "c"}, Type: ItemTypeObject, Object: &PathSource{Path: "f"}},
			wantPaths: []string{"data[0].target"},
		},
		{
			name:      "object item needs an object path",
			item:      DataItem{Target: "t", Repository: "r", Ref: SourceRef{Type: RefTypeCommit, ID: "c"}, Type: ItemTypeObject, Object: &PathSource{}},
			wantPaths: []string{"data[0].object.path"},
		},
		{
			name:      "object item needs its source descriptor",
			item:      DataItem{Target: "t", Repository: "r", Ref: SourceRef{Type: RefTypeCommit, ID: "c"}, Type: ItemTypeObject},
			wantPaths: []string{"data[0].object"},
		},
		{
			name: "the source descriptor must match the item type",
			item: DataItem{
				Target: "t", Repository: "r", Ref: SourceRef{Type: RefTypeCommit, ID: "c"},
				Type: ItemTypeObject, Object: &PathSource{Path: "f"}, Prefix: &PathSource{Path: "p/"},
			},
			wantPaths: []string{"data[0].prefix"},
		},
		{
			name: "a prefix item with an object descriptor",
			item: DataItem{
				Target: "t", Repository: "r", Ref: SourceRef{Type: RefTypeCommit, ID: "c"},
				Type: ItemTypePrefix, Object: &PathSource{Path: "f"},
			},
			wantPaths: []string{"data[0].object", "data[0].prefix"},
		},
		{
			name:      "table items are not supported in the MVP",
			item:      DataItem{Target: "t", Repository: "r", Ref: SourceRef{Type: RefTypeCommit, ID: "c"}, Type: ItemTypeTable, Table: &TableSrc{Namespace: "ns", Name: "t"}},
			wantPaths: []string{"data[0].type"},
		},
		{
			name:      "namespace items are not supported in the MVP",
			item:      DataItem{Target: "t", Repository: "r", Ref: SourceRef{Type: RefTypeCommit, ID: "c"}, Type: ItemTypeNamespace, Namespace: &NsSrc{Name: "ns"}},
			wantPaths: []string{"data[0].type"},
		},
		{
			name:      "everything missing at once is reported",
			item:      DataItem{Type: ItemTypeObject},
			wantPaths: []string{"data[0].repository", "data[0].ref.type", "data[0].ref.id", "data[0].object", "data[0].target"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paths := validatePaths(t, &Definition{FormatVersion: FormatVersion, Data: []DataItem{tt.item}}, ValidateOptions{})
			require.Equal(t, tt.wantPaths, paths)
		})
	}
}

// TestValidateReportsEveryProblem locks the multi-error contract: a single call
// reports every problem, each with the per-item path the API returns (§10.4), and
// indices refer to the canonically sorted definition.
func TestValidateReportsEveryProblem(t *testing.T) {
	env := &Envelope{
		Name: "ab", // too short
		Definition: Definition{
			FormatVersion: 7, // unsupported
			Data: []DataItem{
				{
					Target: "customers",
					// repository, ref type and ref id are missing
					Ref:  SourceRef{Type: "nope"},
					Type: ItemTypeObject, // the object descriptor is missing
				},
				{
					Target:     "customers/orders", // overlaps data[0]
					Repository: "src",
					Ref:        SourceRef{Type: RefTypeCommit, ID: "abc"},
					Type:       ItemTypePrefix,
					Prefix:     &PathSource{Path: "orders/"},
					Object:     &PathSource{Path: "x"}, // contradicts the type
				},
				{
					Target:     "t",
					Repository: "src",
					Ref:        SourceRef{Type: RefTypeCommit, ID: "abc"},
					Type:       ItemTypeTable,
					Table:      &TableSrc{Namespace: "ns", Name: "t"}, // unsupported type
				},
			},
		},
	}

	err := ValidateEnvelope(env, ValidateOptions{})
	var verr *ValidationError
	require.ErrorAs(t, err, &verr)

	require.Equal(t, []string{
		"name",               // name pattern
		"format_version",     // format_version != 1
		"data[0].repository", // missing repository
		"data[0].ref.type",   // unknown ref type
		"data[0].ref.id",     // missing ref id
		"data[0].object",     // missing source descriptor
		"data[1].object",     // source descriptor contradicts the type
		"data[1].target",     // overlap
		"data[2].type",       // table is unsupported
	}, problemPaths(verr.Problems))
	require.Equal(t, "overlaps data[0]", verr.Problems[7].Message)

	// The error message mentions all problems, so a log line is useful on its own.
	require.Contains(t, err.Error(), "9 problem(s)")
	require.Contains(t, err.Error(), "data[1].target: overlaps data[0]")

	// Every problem is JSON-serialisable in the §10.4 shape.
	out, marshalErr := json.Marshal(verr.Problems)
	require.NoError(t, marshalErr)
	var decoded []map[string]string
	require.NoError(t, json.Unmarshal(out, &decoded))
	require.Len(t, decoded, 9)
	for _, p := range decoded {
		require.Equal(t, []string{"message", "path"}, mapKeys(p))
	}
}

// mapKeys returns the sorted keys of a string map.
func mapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for _, k := range []string{"message", "path"} {
		if _, ok := m[k]; ok {
			keys = append(keys, k)
		}
	}
	return keys
}

// TestValidateMetadataHook checks the DS-03 extension point: metadata is untouched
// unless a validator is supplied, and its problems are merged into the result.
func TestValidateMetadataHook(t *testing.T) {
	def := validDefinition()
	def.Metadata = map[string]any{"tier": "gold", "owners": []string{"data-platform"}}

	require.NoError(t, Validate(def, ValidateOptions{}), "metadata is not validated without a hook")

	var called bool
	opts := ValidateOptions{Metadata: func(d *Definition) []Problem {
		called = true
		require.Equal(t, def, d)
		return []Problem{
			{Path: "metadata.tier", Message: "invalid value"},
			{Path: "metadata.classification", Message: "required metadata key is missing"},
		}
	}}

	err := Validate(def, opts)
	require.True(t, called)
	var verr *ValidationError
	require.ErrorAs(t, err, &verr)
	require.Equal(t, []string{"metadata.tier", "metadata.classification"}, problemPaths(verr.Problems))
}

// TestValidateNormalisesBeforeChecking makes sure a caller never has to normalise
// by hand: Validate normalises in place and then judges the canonical form.
func TestValidateNormalisesBeforeChecking(t *testing.T) {
	def := &Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{
			objectItem("/b", "/f"),
			objectItem("a/", "g"), // "a" after normalisation
		},
	}
	require.NoError(t, Validate(def, ValidateOptions{}))
	require.Equal(t, []DataItem{objectItem("a", "g"), objectItem("b", "f")}, def.Data)
}
