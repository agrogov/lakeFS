package datasets

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strKey(name string) MetadataKey {
	return MetadataKey{Name: name, Type: MetadataTypeString}
}

func enumKey(name string, allowed ...string) MetadataKey {
	return MetadataKey{Name: name, Type: MetadataTypeEnum, AllowedValues: allowed}
}

// problemKey returns the metadata key a problem is about, decoded from the §10.4
// path ("metadata.<key>").
func problemKey(problem Problem) string {
	return strings.TrimPrefix(problem.Path, MetadataPathPrefix)
}

// problemCode returns the stable metadata problem code, which the merged Problem
// carries as its message prefix ("<code>: <detail>").
func problemCode(problem Problem) string {
	code, _ := splitProblemMessage(problem.Message)
	return code
}

func problemsOfCode(problems []Problem, code string) []Problem {
	var out []Problem
	for _, problem := range problems {
		if problemCode(problem) == code {
			out = append(out, problem)
		}
	}
	return out
}

func requireSingleProblem(t *testing.T, problems []Problem, key, code string) Problem {
	t.Helper()
	require.Len(t, problems, 1, "problems: %v", problems)
	assert.Equal(t, MetadataPathPrefix+key, problems[0].Path)
	assert.Equal(t, code, problemCode(problems[0]))
	assert.NotEmpty(t, problems[0].Message)
	return problems[0]
}

func TestValidateMetadataKey(t *testing.T) {
	t.Run("valid_string", func(t *testing.T) {
		assert.Empty(t, ValidateMetadataKey(strKey("owner")))
	})

	t.Run("valid_enum", func(t *testing.T) {
		assert.Empty(t, ValidateMetadataKey(enumKey("tier", "gold", "silver")))
	})

	t.Run("valid_all_types", func(t *testing.T) {
		for _, typ := range sortedMetadataTypes() {
			key := MetadataKey{Name: "some_key", Type: typ}
			if typ == MetadataTypeEnum {
				key.AllowedValues = []string{"a"}
			}
			assert.Empty(t, ValidateMetadataKey(key), "type %q", typ)
		}
	})

	t.Run("name_boundaries", func(t *testing.T) {
		tests := []struct {
			name    string
			valid   bool
			comment string
		}{
			{"a", true, "single lower-case letter is the minimum"},
			{"ab", true, "two chars"},
			{"a1", true, "digits allowed after the first char"},
			{"a_1", true, "underscore allowed after the first char"},
			{strings.Repeat("a", 63), true, "63 chars is the maximum"},
			{strings.Repeat("a", 64), false, "64 chars is one too many"},
			{"", false, "empty"},
			{"1a", false, "must start with a letter"},
			{"_a", false, "must not start with an underscore"},
			{"A", false, "upper-case"},
			{"aB", false, "upper-case inside"},
			{"a-b", false, "hyphen not allowed"},
			{"a b", false, "space not allowed"},
			{"a.b", false, "dot not allowed"},
		}
		for _, tt := range tests {
			t.Run(tt.name+"_"+tt.comment, func(t *testing.T) {
				assert.Equal(t, tt.valid, IsValidMetadataKeyName(tt.name))
				problems := ValidateMetadataKey(strKey(tt.name))
				if tt.valid {
					assert.Empty(t, problems)
				} else {
					requireSingleProblem(t, problems, tt.name, ProblemInvalidKeyName)
				}
			})
		}
	})

	t.Run("invalid_type", func(t *testing.T) {
		problems := ValidateMetadataKey(MetadataKey{Name: "owner", Type: "text"})
		problem := requireSingleProblem(t, problems, "owner", ProblemInvalidKeyType)
		assert.Contains(t, problem.Message, `got "text"`)
	})

	t.Run("enum_requires_allowed_values", func(t *testing.T) {
		for _, allowed := range [][]string{nil, {}} {
			problems := ValidateMetadataKey(enumKey("tier", allowed...))
			requireSingleProblem(t, problems, "tier", ProblemAllowedValuesRequired)
		}
	})

	t.Run("allowed_values_only_for_enum", func(t *testing.T) {
		for _, typ := range []string{MetadataTypeString, MetadataTypeNumber, MetadataTypeBoolean, MetadataTypeDate} {
			key := MetadataKey{Name: "tier", Type: typ, AllowedValues: []string{"a"}}
			problems := ValidateMetadataKey(key)
			requireSingleProblem(t, problems, "tier", ProblemAllowedValuesNotAllowed)
		}
	})

	t.Run("problems_are_sorted", func(t *testing.T) {
		// Bad name, bad type and stray allowed_values: two problems, sorted.
		problems := ValidateMetadataKey(MetadataKey{Name: "Bad-Name", Type: "text", AllowedValues: []string{"a"}})
		require.Len(t, problems, 2)
		assert.Equal(t, ProblemInvalidKeyName, problemCode(problems[0]))
		assert.Equal(t, ProblemInvalidKeyType, problemCode(problems[1]))
	})
}

func TestValidateMetadataTypes(t *testing.T) {
	tests := []struct {
		name    string
		key     MetadataKey
		valid   []any
		invalid map[string]any // description -> value
	}{
		{
			name:  "string",
			key:   strKey("owner"),
			valid: []any{"data-platform", ""},
			invalid: map[string]any{
				"number":  float64(1),
				"boolean": true,
				"array":   []any{"a"},
				"object":  map[string]any{"a": "b"},
			},
		},
		{
			name:  "enum",
			key:   enumKey("tier", "gold", "silver"),
			valid: []any{"gold", "silver"},
			invalid: map[string]any{
				"not_member": "bronze",
				"number":     float64(1),
				"boolean":    true,
			},
		},
		{
			name: "number",
			key:  MetadataKey{Name: "retention_days", Type: MetadataTypeNumber},
			valid: []any{
				float64(30),
				int(30),
				int64(-2),
				uint8(7),
				json.Number("1.5"),
			},
			invalid: map[string]any{
				"string":  "30",
				"boolean": true,
				"nan":     math.NaN(),
				"inf":     math.Inf(1),
				"badjson": json.Number("not-a-number"),
			},
		},
		{
			name:  "boolean",
			key:   MetadataKey{Name: "pii", Type: MetadataTypeBoolean},
			valid: []any{true, false},
			invalid: map[string]any{
				"string": "true",
				"number": float64(1),
			},
		},
		{
			name: "date",
			key:  MetadataKey{Name: "review_date", Type: MetadataTypeDate},
			valid: []any{
				"2024-02-29", // leap day
				"1970-01-01",
				"2024-12-31",
			},
			invalid: map[string]any{
				"not_a_leap_year": "2023-02-29",
				"single_digits":   "2024-2-9",
				"timestamp":       "2024-02-09T00:00:00Z",
				"compact":         "20240209",
				"month_13":        "2024-13-01",
				"empty":           "",
				"number":          float64(20240209),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, value := range tt.valid {
				problems := ValidateMetadata(map[string]any{tt.key.Name: value}, []MetadataKey{tt.key}, false)
				assert.Empty(t, problems, "value %#v should be valid for type %q", value, tt.key.Type)
			}
			for description, value := range tt.invalid {
				t.Run("invalid_"+description, func(t *testing.T) {
					problems := ValidateMetadata(map[string]any{tt.key.Name: value}, []MetadataKey{tt.key}, false)
					require.NotEmpty(t, problems, "value %#v should be invalid for type %q", value, tt.key.Type)
					assert.Equal(t, MetadataPathPrefix+tt.key.Name, problems[0].Path)

					wantCode := ProblemTypeMismatch
					switch tt.key.Type {
					case MetadataTypeEnum:
						if _, isString := value.(string); isString {
							wantCode = ProblemEnumValueNotAllowed
						}
					case MetadataTypeDate:
						if _, isString := value.(string); isString {
							wantCode = ProblemInvalidDate
						}
					}
					assert.Equal(t, wantCode, problemCode(problems[0]), "value %#v", value)
				})
			}
		})
	}

	t.Run("enum_message_lists_allowed_values", func(t *testing.T) {
		problems := ValidateMetadata(
			map[string]any{"tier": "bronze"},
			[]MetadataKey{enumKey("tier", "gold", "silver")},
			false,
		)
		problem := requireSingleProblem(t, problems, "tier", ProblemEnumValueNotAllowed)
		assert.Contains(t, problem.Message, "gold")
		assert.Contains(t, problem.Message, "silver")
	})
}

func TestValidateMetadataAllowMultiple(t *testing.T) {
	keys := func(key MetadataKey) []MetadataKey { return []MetadataKey{key} }

	t.Run("valid_string_array", func(t *testing.T) {
		key := strKey("tags")
		key.AllowMultiple = true
		problems := ValidateMetadata(map[string]any{"tags": []any{"a", "b"}}, keys(key), false)
		assert.Empty(t, problems)
	})

	t.Run("valid_typed_slice", func(t *testing.T) {
		key := strKey("tags")
		key.AllowMultiple = true
		problems := ValidateMetadata(map[string]any{"tags": []string{"a"}}, keys(key), false)
		assert.Empty(t, problems)
	})

	t.Run("valid_enum_array", func(t *testing.T) {
		key := enumKey("tiers", "gold", "silver")
		key.AllowMultiple = true
		problems := ValidateMetadata(map[string]any{"tiers": []any{"gold", "silver"}}, keys(key), false)
		assert.Empty(t, problems)
	})

	t.Run("scalar_rejected", func(t *testing.T) {
		key := strKey("tags")
		key.AllowMultiple = true
		problems := ValidateMetadata(map[string]any{"tags": "a"}, keys(key), false)
		problem := requireSingleProblem(t, problems, "tags", ProblemTypeMismatch)
		assert.Contains(t, problem.Message, "array")
	})

	t.Run("empty_array_rejected", func(t *testing.T) {
		key := strKey("tags")
		key.AllowMultiple = true
		for _, value := range []any{[]any{}, []string{}} {
			problems := ValidateMetadata(map[string]any{"tags": value}, keys(key), false)
			requireSingleProblem(t, problems, "tags", ProblemEmptyArray)
		}
	})

	t.Run("elements_are_validated_in_order", func(t *testing.T) {
		key := MetadataKey{Name: "retention_days", Type: MetadataTypeNumber, AllowMultiple: true}
		problems := ValidateMetadata(
			map[string]any{"retention_days": []any{float64(1), "two", true, float64(4), "five"}},
			keys(key),
			false,
		)
		require.Len(t, problems, 3)
		assert.Contains(t, problems[0].Message, "element 1")
		assert.Contains(t, problems[1].Message, "element 2")
		assert.Contains(t, problems[2].Message, "element 4")
		for _, problem := range problems {
			assert.Equal(t, ProblemTypeMismatch, problemCode(problem))
		}
	})

	t.Run("enum_elements_are_checked", func(t *testing.T) {
		key := enumKey("tiers", "gold", "silver")
		key.AllowMultiple = true
		problems := ValidateMetadata(map[string]any{"tiers": []any{"gold", "bronze"}}, keys(key), false)
		problem := requireSingleProblem(t, problems, "tiers", ProblemEnumValueNotAllowed)
		assert.Contains(t, problem.Message, "element 1")
	})

	t.Run("array_of_arrays_rejected", func(t *testing.T) {
		key := strKey("tags")
		key.AllowMultiple = true
		problems := ValidateMetadata(map[string]any{"tags": []any{[]string{"a"}}}, keys(key), false)
		problem := requireSingleProblem(t, problems, "tags", ProblemTypeMismatch)
		assert.Contains(t, problem.Message, "element 0")
	})
}

func TestValidateMetadataRequired(t *testing.T) {
	key := strKey("owner")
	key.Required = true
	optional := strKey("retention")
	keys := []MetadataKey{key, optional}

	tests := []struct {
		name      string
		values    map[string]any
		wantCode  string // "" means valid
		wantEmpty bool
	}{
		{name: "present", values: map[string]any{"owner": "data-platform"}, wantEmpty: true},
		{name: "present_with_optional", values: map[string]any{"owner": "data-platform", "retention": "1y"}, wantEmpty: true},
		{name: "absent", values: map[string]any{}, wantCode: ProblemRequiredMissing},
		{name: "null_counts_as_absent", values: map[string]any{"owner": nil}, wantCode: ProblemRequiredMissing},
		{name: "optional_absent_is_fine", values: map[string]any{"owner": "x"}, wantEmpty: true},
		{name: "optional_null_is_fine", values: map[string]any{"owner": "x", "retention": nil}, wantEmpty: true},
		{name: "wrong_type", values: map[string]any{"owner": float64(1)}, wantCode: ProblemTypeMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := ValidateMetadata(tt.values, keys, false)
			if tt.wantEmpty {
				assert.Empty(t, problems)
				return
			}
			requireSingleProblem(t, problems, "owner", tt.wantCode)
		})
	}

	t.Run("required_and_wrong_type_both_reported", func(t *testing.T) {
		keys := []MetadataKey{
			{Name: "owner", Type: MetadataTypeString, Required: true},
			{Name: "pii", Type: MetadataTypeBoolean, Required: true},
		}
		problems := ValidateMetadata(map[string]any{"pii": "yes"}, keys, false)
		// Same key can carry both codes: absent is not possible with a value, so
		// the required problem belongs to "owner" and the type problem to "pii".
		require.Len(t, problems, 2)
		assert.Equal(t, ProblemRequiredMissing, problemCode(problemsOfCode(problems, ProblemRequiredMissing)[0]))
		assert.Equal(t, "owner", problemKey(problemsOfCode(problems, ProblemRequiredMissing)[0]))
		assert.Equal(t, "pii", problemKey(problemsOfCode(problems, ProblemTypeMismatch)[0]))
	})

	t.Run("duplicate_key_definitions_report_once", func(t *testing.T) {
		// A repeated definition must not duplicate the required_missing problem.
		problems := ValidateMetadata(map[string]any{}, []MetadataKey{key, key}, false)
		requireSingleProblem(t, problems, "owner", ProblemRequiredMissing)
	})

	t.Run("required_multi_needs_non_empty_array", func(t *testing.T) {
		key := strKey("tags")
		key.Required = true
		key.AllowMultiple = true
		requireSingleProblem(t, ValidateMetadata(map[string]any{}, []MetadataKey{key}, false), "tags", ProblemRequiredMissing)
		requireSingleProblem(t, ValidateMetadata(map[string]any{"tags": []any{}}, []MetadataKey{key}, false), "tags", ProblemEmptyArray)
		assert.Empty(t, ValidateMetadata(map[string]any{"tags": []any{"a"}}, []MetadataKey{key}, false))
	})
}

// TestValidateMetadataMatrix is the required/enum/multi/strict matrix: each case
// lists the exact set of (key, code) problems expected, in sorted order.
func TestValidateMetadataMatrix(t *testing.T) {
	owner := MetadataKey{Name: "owner", Type: MetadataTypeString, Required: true}
	tier := MetadataKey{Name: "tier", Type: MetadataTypeEnum, AllowedValues: []string{"gold", "silver"}}
	tags := MetadataKey{Name: "tags", Type: MetadataTypeString, AllowMultiple: true}
	keys := []MetadataKey{owner, tier, tags}

	type want struct {
		key  string
		code string
	}
	tests := []struct {
		name   string
		values map[string]any
		strict bool
		want   []want
	}{
		{
			name:   "all_good",
			values: map[string]any{"owner": "data-platform", "tier": "gold", "tags": []any{"a"}},
		},
		{
			name:   "required_missing_plus_enum_and_multi",
			values: map[string]any{"tier": "bronze", "tags": []any{"a", float64(2)}},
			want: []want{
				{"owner", ProblemRequiredMissing},
				{"tags", ProblemTypeMismatch},
				{"tier", ProblemEnumValueNotAllowed},
			},
		},
		{
			name:   "required_only_when_write_omits_it",
			values: map[string]any{"owner": nil, "tier": "silver", "tags": []any{}},
			want: []want{
				{"owner", ProblemRequiredMissing},
				{"tags", ProblemEmptyArray},
			},
		},
		{
			name:   "unknown_key_non_strict_is_free_form",
			values: map[string]any{"owner": "x", "retention": "1y", "custom": float64(7)},
		},
		{
			name:   "unknown_key_strict_is_rejected",
			values: map[string]any{"owner": "x", "retention": "1y", "custom": float64(7)},
			strict: true,
			want: []want{
				{"custom", ProblemUnknownKey},
				{"retention", ProblemUnknownKey},
			},
		},
		{
			name:   "strict_does_not_hide_other_problems",
			values: map[string]any{"tier": "bronze", "retention": "1y"},
			strict: true,
			want: []want{
				{"owner", ProblemRequiredMissing},
				{"retention", ProblemUnknownKey},
				{"tier", ProblemEnumValueNotAllowed},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := ValidateMetadata(tt.values, keys, tt.strict)
			got := make([]want, len(problems))
			for i, problem := range problems {
				got[i] = want{key: problemKey(problem), code: problemCode(problem)}
			}
			if tt.want == nil {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateMetadataUnknownKeys(t *testing.T) {
	keys := []MetadataKey{strKey("owner")}
	values := map[string]any{"owner": "data-platform", "retention": "1y"}

	t.Run("non_strict_allows_free_form_strings", func(t *testing.T) {
		assert.Empty(t, ValidateMetadata(values, keys, false))
	})

	t.Run("non_strict_allows_free_form_values_of_any_json_type", func(t *testing.T) {
		// Undefined keys are free-form: no value validation is applied.
		freeForm := map[string]any{
			"owner":                "data-platform",
			"retention":            "1y",
			"classification_level": float64(3),
			"contains_pii":         true,
			"tags":                 []any{"a", "b"},
		}
		assert.Empty(t, ValidateMetadata(freeForm, keys, false))
	})

	t.Run("strict_rejects_unknown_keys", func(t *testing.T) {
		problems := ValidateMetadata(values, keys, true)
		problem := requireSingleProblem(t, problems, "retention", ProblemUnknownKey)
		assert.Contains(t, problem.Message, "strict")
	})

	t.Run("strict_allows_defined_keys", func(t *testing.T) {
		assert.Empty(t, ValidateMetadata(map[string]any{"owner": "x"}, keys, true))
	})

	t.Run("strict_still_reports_unknown_key_alongside_other_problems", func(t *testing.T) {
		problems := ValidateMetadata(
			map[string]any{"owner": float64(1), "retention": "1y"},
			keys,
			true,
		)
		require.Len(t, problems, 2)
		// Sorted by key: "owner" < "retention".
		assert.Equal(t, "owner", problemKey(problems[0]))
		assert.Equal(t, ProblemTypeMismatch, problemCode(problems[0]))
		assert.Equal(t, "retention", problemKey(problems[1]))
		assert.Equal(t, ProblemUnknownKey, problemCode(problems[1]))
	})

	t.Run("strict_without_any_values_and_without_required_keys", func(t *testing.T) {
		assert.Empty(t, ValidateMetadata(map[string]any{}, keys, true))
		assert.Empty(t, ValidateMetadata(nil, keys, true))
	})
}

// TestValidateMetadataNewRequiredKeyOnlyOnNextEdit pins the design §5.5 rule:
// adding a required metadata key is enforced only for the definition being
// written; already published versions are never re-validated. The service layer
// calls ValidateMetadata only on create/publish/proposal definitions, never on
// stored VersionRecords.
func TestValidateMetadataNewRequiredKeyOnlyOnNextEdit(t *testing.T) {
	keysV1 := []MetadataKey{strKey("owner")}
	versionV1Values := map[string]any{"owner": "data-platform"}

	// Published as v1: valid, and the version keeps these values forever.
	require.Empty(t, ValidateMetadata(versionV1Values, keysV1, false))

	// An admin now adds "tier" as a required key.
	keysV2 := []MetadataKey{
		strKey("owner"),
		{Name: "tier", Type: MetadataTypeEnum, Required: true, AllowedValues: []string{"gold", "silver"}},
	}

	// The published v1 metadata is not re-validated: it is neither mutated nor
	// re-checked, so the stored version stays readable. (Only a write path calls
	// ValidateMetadata, and no write happens here.)
	assert.Equal(t, map[string]any{"owner": "data-platform"}, versionV1Values)

	// Re-validating v1's values against the new key set would fail - which is
	// exactly why the service never does it for existing versions.
	problems := ValidateMetadata(versionV1Values, keysV2, false)
	requireSingleProblem(t, problems, "tier", ProblemRequiredMissing)

	// The next edit must supply the new required key.
	nextEdit := map[string]any{"owner": "data-platform", "tier": "gold"}
	assert.Empty(t, ValidateMetadata(nextEdit, keysV2, false))

	// And a next edit that still omits it is rejected.
	delete(nextEdit, "tier")
	requireSingleProblem(t, ValidateMetadata(nextEdit, keysV2, false), "tier", ProblemRequiredMissing)
}

func TestValidateMetadataDeterministicOrder(t *testing.T) {
	keys := []MetadataKey{
		{Name: "zeta", Type: MetadataTypeBoolean},
		{Name: "alpha", Type: MetadataTypeNumber},
		{Name: "mike", Type: MetadataTypeDate},
	}
	values := map[string]any{
		"zeta":       "yes",
		"alpha":      "not-a-number",
		"mike":       "09/02/2024",
		"undeclared": 42,
	}

	want := []string{"alpha", "mike", "undeclared", "zeta"}

	first := ValidateMetadata(values, keys, true)
	require.Len(t, first, 4)
	got := make([]string, len(first))
	for i, problem := range first {
		got[i] = problemKey(problem)
	}
	assert.Equal(t, want, got)
	assert.Equal(t, ProblemUnknownKey, problemCode(first[2]))

	// Re-running must produce byte-identical problems (no map iteration leakage).
	for i := 0; i < 50; i++ {
		assert.Equal(t, first, ValidateMetadata(values, keys, true))
	}

	// Without strict mode the unknown key disappears and the rest keeps its order.
	nonStrict := ValidateMetadata(values, keys, false)
	require.Len(t, nonStrict, 3)
	assert.Equal(t, []string{"alpha", "mike", "zeta"}, []string{problemKey(nonStrict[0]), problemKey(nonStrict[1]), problemKey(nonStrict[2])})
}

func TestSortProblems(t *testing.T) {
	problems := []Problem{
		metadataProblem("b", ProblemTypeMismatch, ""),
		metadataProblem("a", ProblemUnknownKey, ""),
		metadataProblem("a", ProblemRequiredMissing, ""),
		metadataProblem("a", ProblemRequiredMissing, ""),
	}
	SortProblems(problems)
	assert.Equal(t, []Problem{
		metadataProblem("a", ProblemRequiredMissing, ""),
		metadataProblem("a", ProblemRequiredMissing, ""),
		metadataProblem("a", ProblemUnknownKey, ""),
		metadataProblem("b", ProblemTypeMismatch, ""),
	}, problems)
}

// TestProblemShape pins the merged §10.4 problem shape: a metadata problem has
// exactly the two fields of the API error, with the key in the path and the stable
// metadata code folded into the message.
func TestProblemShape(t *testing.T) {
	problem := metadataProblem("owner", ProblemTypeMismatch, "expected a string, got number")
	assert.Equal(t, "metadata.owner", problem.Path)
	assert.Equal(t, "type_mismatch: expected a string, got number", problem.Message)

	encoded, err := json.Marshal(problem)
	require.NoError(t, err)
	assert.JSONEq(t, `{"path":"metadata.owner","message":"type_mismatch: expected a string, got number"}`, string(encoded))
}

func TestValidateKeyUpdate(t *testing.T) {
	base := MetadataKey{Name: "tier", Type: MetadataTypeEnum, AllowedValues: []string{"gold"}, Required: true}

	t.Run("allows_allowed_values_change", func(t *testing.T) {
		updated := base
		updated.AllowedValues = []string{"gold", "silver"}
		assert.NoError(t, ValidateKeyUpdate(base, updated))
	})

	t.Run("allows_required_change", func(t *testing.T) {
		updated := base
		updated.Required = false
		assert.NoError(t, ValidateKeyUpdate(base, updated))
	})

	t.Run("allows_allow_multiple_and_description_change", func(t *testing.T) {
		updated := base
		updated.AllowMultiple = true
		updated.Description = "data tier"
		assert.NoError(t, ValidateKeyUpdate(base, updated))
	})

	t.Run("allows_noop", func(t *testing.T) {
		assert.NoError(t, ValidateKeyUpdate(base, base))
	})

	t.Run("forbids_type_change", func(t *testing.T) {
		for _, newType := range []string{MetadataTypeString, MetadataTypeNumber, MetadataTypeBoolean, MetadataTypeDate} {
			updated := base
			updated.Type = newType
			updated.AllowedValues = nil // even with a valid definition for the new type
			err := ValidateKeyUpdate(base, updated)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrMetadataKeyTypeChange)
			assert.Contains(t, err.Error(), "delete and recreate")
		}
	})

	t.Run("type_change_wins_over_other_validation", func(t *testing.T) {
		updated := MetadataKey{Name: "tier", Type: MetadataTypeString, AllowedValues: []string{"gold"}}
		err := ValidateKeyUpdate(base, updated)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrMetadataKeyTypeChange)
	})

	t.Run("forbids_rename", func(t *testing.T) {
		updated := base
		updated.Name = "tiers"
		err := ValidateKeyUpdate(base, updated)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrMetadataKeyRename)
	})

	t.Run("rejects_invalid_new_definition", func(t *testing.T) {
		// Same name and type, but the enum loses its allowed_values: the update
		// itself passes the type rule, yet the new definition is invalid.
		updated := base
		updated.AllowedValues = nil
		err := ValidateKeyUpdate(base, updated)
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrMetadataKeyTypeChange)
		assert.NotErrorIs(t, err, ErrMetadataKeyRename)
		assert.Contains(t, err.Error(), "non-empty allowed_values")
	})

	t.Run("rename_checked_before_definition_validity", func(t *testing.T) {
		newKey := MetadataKey{Name: "Tier", Type: MetadataTypeEnum, AllowedValues: []string{"gold"}}
		err := ValidateKeyUpdate(base, newKey)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrMetadataKeyRename)
	})
}

func TestValidateMetadataEmptyInputs(t *testing.T) {
	assert.Empty(t, ValidateMetadata(nil, nil, false))
	assert.Empty(t, ValidateMetadata(map[string]any{}, nil, true))
	assert.Empty(t, ValidateMetadata(nil, []MetadataKey{strKey("owner")}, true)) // nothing required
}

func TestValidateMetadataKeyRetainsJSONRoundTrip(t *testing.T) {
	key := MetadataKey{
		Name:          "tier",
		Type:          MetadataTypeEnum,
		Required:      true,
		AllowedValues: []string{"gold", "silver"},
		AllowMultiple: true,
		Description:   "data tier",
		CreatedAt:     time.Date(2024, time.February, 9, 12, 30, 0, 0, time.UTC),
		CreatedBy:     "admin",
	}
	encoded, err := json.Marshal(key)
	require.NoError(t, err)
	// §5.5 field names are part of the Enterprise-compatible wire format.
	for _, field := range []string{`"name":`, `"type":`, `"required":`, `"allowed_values":`, `"allow_multiple":`, `"created_at":`, `"created_by":`} {
		assert.Contains(t, string(encoded), field)
	}

	var decoded MetadataKey
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, key, decoded)
	assert.Empty(t, ValidateMetadataKey(decoded))

	// An Enterprise-style definition with free-form metadata validates cleanly
	// against these keys.
	values := map[string]any{"tier": []any{"gold"}, "retention": "1y", "contains_pii": false}
	assert.Empty(t, ValidateMetadata(values, []MetadataKey{decoded}, false))
}

func TestValidateMetadataDateEdgeCases(t *testing.T) {
	key := MetadataKey{Name: "d", Type: MetadataTypeDate}
	for _, value := range []string{"2024-01-01", "1999-12-31", "2000-02-29"} {
		assert.True(t, isFullDate(value), value)
	}
	for _, value := range []string{"2024-00-01", "2024-01-00", "2024-01-32", "0000-01-01x", " 2024-01-01"} {
		assert.False(t, isFullDate(value), value)
		problems := ValidateMetadata(map[string]any{"d": value}, []MetadataKey{key}, false)
		requireSingleProblem(t, problems, "d", ProblemInvalidDate)
	}
}

func TestProblemsError(t *testing.T) {
	assert.NoError(t, problemsError(nil))
	err := problemsError([]Problem{{Message: "first"}, {Message: "second"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMetadataKey)
	assert.Equal(t, "invalid metadata key: first; second", err.Error())
	assert.NotErrorIs(t, err, ErrMetadataKeyTypeChange)
}

// TestMetadataKeysValidator pins the adapter that folds this card into the merged
// validator: the caller supplies the registered keys and datasets.metadata.strict,
// and ValidateOptions.Metadata reports the metadata problems with the merged §10.4
// paths, next to (not instead of) the structural problems.
func TestMetadataKeysValidator(t *testing.T) {
	keys := []MetadataKey{
		{Name: "owner", Type: MetadataTypeString, Required: true},
		enumKey("tier", "gold", "silver"),
	}
	def := validDefinition() // structurally valid: one object item
	def.Metadata = map[string]any{"tier": "bronze", "retention": "1y"}

	// Without the hook metadata stays free-form: the merged default is unchanged.
	require.NoError(t, Validate(def, ValidateOptions{}))

	t.Run("non_strict", func(t *testing.T) {
		var verr *ValidationError
		require.ErrorAs(t, Validate(def, ValidateOptions{Metadata: MetadataKeysValidator(keys, false)}), &verr)
		// Sorted by path: metadata.owner < metadata.tier.
		require.Equal(t, []string{"metadata.owner", "metadata.tier"}, problemPaths(verr.Problems))
		assert.Equal(t, ProblemRequiredMissing, problemCode(verr.Problems[0]))
		assert.Equal(t, ProblemEnumValueNotAllowed, problemCode(verr.Problems[1]))
	})

	t.Run("strict_rejects_the_free_form_key_too", func(t *testing.T) {
		var verr *ValidationError
		require.ErrorAs(t, Validate(def, ValidateOptions{Metadata: MetadataKeysValidator(keys, true)}), &verr)
		require.Equal(t, []string{"metadata.owner", "metadata.retention", "metadata.tier"}, problemPaths(verr.Problems))
		assert.Equal(t, ProblemUnknownKey, problemCode(verr.Problems[1]))
	})

	t.Run("valid_metadata_passes", func(t *testing.T) {
		good := validDefinition()
		good.Metadata = map[string]any{"owner": "data-platform", "tier": "gold", "retention": "1y"}
		// Strict rejects only undefined keys; it accepts every defined, valid one.
		require.NoError(t, Validate(good, ValidateOptions{Metadata: MetadataKeysValidator(keys, false)}))
		delete(good.Metadata, "retention")
		require.NoError(t, Validate(good, ValidateOptions{Metadata: MetadataKeysValidator(keys, true)}))
	})

	t.Run("nil_definition_is_left_to_validate", func(t *testing.T) {
		// The adapter itself is silent; Validate reports the missing definition.
		assert.Empty(t, MetadataKeysValidator(keys, true)(nil))
		var verr *ValidationError
		require.ErrorAs(t, Validate(nil, ValidateOptions{Metadata: MetadataKeysValidator(keys, true)}), &verr)
		require.Equal(t, []Problem{{Message: "definition is missing"}}, verr.Problems)
	})
}

// TestValidateMetadataReportedOnce pins that the metadata hook runs exactly once
// per validation, so a metadata problem is never reported twice: once by the hook
// and once by another path.
func TestValidateMetadataReportedOnce(t *testing.T) {
	keys := []MetadataKey{{Name: "owner", Type: MetadataTypeString, Required: true}}
	def := validDefinition() // no metadata: "owner" is missing

	var calls int
	hook := func(d *Definition) []Problem {
		calls++
		return ValidateMetadata(d.Metadata, keys, false)
	}

	var verr *ValidationError
	require.ErrorAs(t, Validate(def, ValidateOptions{Metadata: hook}), &verr)
	require.Equal(t, 1, calls, "Validate must call the metadata hook exactly once")
	require.Equal(t, []Problem{metadataProblem("owner", ProblemRequiredMissing, "required metadata key is missing")}, verr.Problems)

	// ValidateEnvelope adds the name check but still runs the hook once, because
	// metadata belongs to the definition and is validated only there.
	calls = 0
	err := ValidateEnvelope(&Envelope{Name: "customer-360", Definition: *def}, ValidateOptions{Metadata: hook})
	require.ErrorAs(t, err, &verr)
	require.Equal(t, 1, calls, "ValidateEnvelope must call the metadata hook exactly once")
	require.Equal(t, []string{"metadata.owner"}, problemPaths(verr.Problems))
}
