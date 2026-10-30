package datasets

// Metadata value validation (design §5.5). metadata_keys.go holds the metadata
// key model and key-definition validation; this file validates the metadata map
// of a definition that is being written against those keys.
//
// Problems use the merged §10.4 shape of validate.go: {path, message}, with
// path = "metadata.<key>" for every metadata problem. The stable problem codes
// of the metadata card are kept as the message prefix ("<code>: <detail>"), so
// API clients can still switch on the code while the wire shape stays the two
// fields of §10.4.

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

// MetadataPathPrefix is the path prefix of every metadata problem in the §10.4
// error shape: the problem for metadata key "tier" has path "metadata.tier",
// just as structural problems use paths such as "data[2].target".
const MetadataPathPrefix = "metadata."

// Metadata problem codes. They are stable strings so the REST API can surface
// them (as the message prefix) and clients can switch on them.
const (
	// ProblemInvalidKeyName reports a metadata key name that does not match
	// ^[a-z][a-z0-9_]{0,62}$.
	ProblemInvalidKeyName = "invalid_key_name"
	// ProblemInvalidKeyType reports a key type outside the supported set.
	ProblemInvalidKeyType = "invalid_key_type"
	// ProblemAllowedValuesRequired reports an enum key without allowed_values.
	ProblemAllowedValuesRequired = "allowed_values_required"
	// ProblemAllowedValuesNotAllowed reports allowed_values on a non-enum key.
	ProblemAllowedValuesNotAllowed = "allowed_values_not_allowed"
	// ProblemRequiredMissing reports a required key absent from the values.
	ProblemRequiredMissing = "required_missing"
	// ProblemUnknownKey reports an undefined key while metadata.strict is set.
	ProblemUnknownKey = "unknown_key"
	// ProblemTypeMismatch reports a value that is not of the key's type.
	ProblemTypeMismatch = "type_mismatch"
	// ProblemEnumValueNotAllowed reports a value outside allowed_values.
	ProblemEnumValueNotAllowed = "enum_value_not_allowed"
	// ProblemEmptyArray reports an empty value for an allow_multiple key.
	ProblemEmptyArray = "empty_array"
	// ProblemInvalidDate reports a date value that is not a valid YYYY-MM-DD.
	ProblemInvalidDate = "invalid_date"
)

// metadataFullDateRe guards the date layout: RFC 3339 full-date is exactly ten
// bytes, YYYY-MM-DD, so no other layout time.Parse might accept can slip in.
var metadataFullDateRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// metadataProblem builds a metadata Problem in the merged §10.4 shape: the path
// names the metadata key and the message starts with the stable problem code.
// An empty message leaves the bare code as the message.
func metadataProblem(key, code, message string) Problem {
	if message == "" {
		return Problem{Path: MetadataPathPrefix + key, Message: code}
	}
	return Problem{Path: MetadataPathPrefix + key, Message: code + ": " + message}
}

// splitProblemMessage splits a metadata problem message into its stable code and
// human-readable detail. It is the inverse of metadataProblem for the element
// prefixing of allow_multiple arrays.
func splitProblemMessage(message string) (code, detail string) {
	if code, detail, ok := strings.Cut(message, ": "); ok {
		return code, detail
	}
	return message, ""
}

// SortProblems orders problems deterministically: by path, then by message, which
// starts with the problem code, so it is also ordered by code. Ties keep their
// insertion order (stable sort), so per-element array problems stay in index
// order. Every metadata validation function returns problems in this order so API
// output and tests are stable; callers combining several problem lists should run
// the combined slice through SortProblems again.
func SortProblems(problems []Problem) {
	sort.SliceStable(problems, func(i, j int) bool {
		if problems[i].Path != problems[j].Path {
			return problems[i].Path < problems[j].Path
		}
		return problems[i].Message < problems[j].Message
	})
}

// MetadataKeysValidator returns the MetadataValidator adapter of validate.go: it
// pulls Definition.Metadata, checks it against the registered metadata keys and
// reports the result as metadata problems. It is the hook ValidateOptions.Metadata
// expects, so a caller that has loaded the key definitions writes:
//
//	def := &Definition{...}
//	err := Validate(def, ValidateOptions{
//		MaxItems: cfg.MaxItems,
//		Metadata: MetadataKeysValidator(keys, cfg.Metadata.Strict),
//	})
//
// or simply cfg.ValidateOptions(keys) (config.go). A nil Definition produces no
// problems: Validate reports the missing definition itself.
func MetadataKeysValidator(keys []MetadataKey, strict bool) MetadataValidator {
	return func(def *Definition) []Problem {
		if def == nil {
			return nil
		}
		return ValidateMetadata(def.Metadata, keys, strict)
	}
}

// ValidateMetadata validates the metadata values of a definition that is being
// written against the dataset's metadata key definitions.
//
// Rules (design §5.5):
//   - A value for a defined key is type checked: string, number (JSON number),
//     boolean, date (RFC 3339 full-date, YYYY-MM-DD) or enum (a string from
//     allowed_values). With allow_multiple the value must be a non-empty JSON
//     array of that scalar type and every element is validated.
//   - Every key with required=true must be present. A JSON null counts as absent,
//     as does a missing map entry.
//   - Keys that are not defined are free-form: accepted without value validation,
//     the usual case being free-form strings such as "retention". When strict is
//     true (config datasets.metadata.strict) they are rejected with
//     ProblemUnknownKey instead.
//
// Existing versions are never re-validated. This function is called only for a
// definition that is being written (create, publish, proposal), so adding a
// required key affects only the next edit; versions published earlier keep the
// metadata they were published with and remain readable.
//
// Problems are returned sorted by path (that is, by key), then by code; an empty
// result means valid.
func ValidateMetadata(values map[string]any, keys []MetadataKey, strict bool) []Problem {
	var problems []Problem

	defined := make(map[string]MetadataKey, len(keys))
	for _, key := range keys {
		// A duplicate name keeps the last definition. Malformed definitions are
		// reported by ValidateMetadataKey, not here.
		defined[key.Name] = key
	}

	// Required keys are enforced only for the definition being written. Iterating
	// the de-duplicated map keeps a repeated key definition from reporting the
	// same missing key twice; SortProblems restores a stable order.
	for name, key := range defined {
		if !key.Required {
			continue
		}
		if _, ok := metadataValue(values, name); !ok {
			problems = append(problems, metadataProblem(name, ProblemRequiredMissing, "required metadata key is missing"))
		}
	}

	// Iterate values in sorted name order; the final SortProblems pass makes the
	// result independent of Go map iteration order.
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		value, ok := metadataValue(values, name)
		if !ok {
			continue // absent or JSON null
		}
		key, isDefined := defined[name]
		if !isDefined {
			if strict {
				problems = append(problems, metadataProblem(name, ProblemUnknownKey,
					"metadata key is not defined and datasets.metadata.strict is enabled"))
			}
			// Non-strict: free-form value, deliberately not validated.
			continue
		}
		problems = append(problems, validateMetadataValue(key, value)...)
	}

	SortProblems(problems)
	return problems
}

// metadataValue returns the value for name and reports whether it is present.
// A JSON null counts as absent, so a required key cannot be satisfied by null and
// optional nulls are simply ignored.
func metadataValue(values map[string]any, name string) (any, bool) {
	value, ok := values[name]
	if !ok || value == nil {
		return nil, false
	}
	return value, true
}

// validateMetadataValue validates one present value (scalar, or array when the key
// allows multiple values).
func validateMetadataValue(key MetadataKey, value any) []Problem {
	if !key.AllowMultiple {
		return validateMetadataScalar(key, value)
	}

	elems, ok := metadataArrayElements(value)
	if !ok {
		return []Problem{metadataProblem(key.Name, ProblemTypeMismatch,
			fmt.Sprintf("expected an array of %s values, got %s",
				metadataTypeDescription(key.Type), describeJSONValue(value)))}
	}
	if len(elems) == 0 {
		return []Problem{metadataProblem(key.Name, ProblemEmptyArray,
			"an allow_multiple key requires a non-empty array")}
	}

	var problems []Problem
	for i, elem := range elems {
		for _, problem := range validateMetadataScalar(key, elem) {
			// Prefixed after the code, so ["type_mismatch: element 0: ..."] stays
			// sorted by code first.
			code, detail := splitProblemMessage(problem.Message)
			problem.Message = code + ": " + fmt.Sprintf("element %d: %s", i, detail)
			problems = append(problems, problem)
		}
	}
	return problems
}

// validateMetadataScalar validates one scalar value against key.
func validateMetadataScalar(key MetadataKey, value any) []Problem {
	switch key.Type {
	case MetadataTypeString:
		if _, ok := value.(string); !ok {
			return metadataTypeMismatch(key, value, "a string")
		}

	case MetadataTypeEnum:
		str, ok := value.(string)
		if !ok {
			return metadataTypeMismatch(key, value, "a string from allowed_values")
		}
		for _, allowed := range key.AllowedValues {
			if str == allowed {
				return nil
			}
		}
		return []Problem{metadataProblem(key.Name, ProblemEnumValueNotAllowed,
			fmt.Sprintf("value %q is not one of the allowed values [%s]", str, strings.Join(key.AllowedValues, ", ")))}

	case MetadataTypeNumber:
		if !isJSONNumber(value) {
			return metadataTypeMismatch(key, value, "a number")
		}

	case MetadataTypeBoolean:
		if _, ok := value.(bool); !ok {
			return metadataTypeMismatch(key, value, "a boolean")
		}

	case MetadataTypeDate:
		str, ok := value.(string)
		if !ok {
			return metadataTypeMismatch(key, value, "a date (RFC 3339 full-date, YYYY-MM-DD)")
		}
		if !isFullDate(str) {
			return []Problem{metadataProblem(key.Name, ProblemInvalidDate,
				fmt.Sprintf("value %q is not a valid RFC 3339 full-date (YYYY-MM-DD)", str))}
		}

	default:
		// Unknown type: ValidateMetadataKey reports it, and there is nothing to
		// type check against.
	}
	return nil
}

func metadataTypeMismatch(key MetadataKey, value any, expected string) []Problem {
	return []Problem{metadataProblem(key.Name, ProblemTypeMismatch,
		fmt.Sprintf("expected %s, got %s", expected, describeJSONValue(value)))}
}

// metadataArrayElements flattens a value that is a JSON array (or any Go
// slice/array, so typed slices such as []string work too) into []any.
func metadataArrayElements(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	elems := make([]any, rv.Len())
	for i := range elems {
		elems[i] = rv.Index(i).Interface()
	}
	return elems, true
}

// isJSONNumber reports whether value is a finite JSON number. json.Number (the
// decoder output with UseNumber) is accepted when it parses; NaN and infinities
// are not valid JSON numbers.
func isJSONNumber(value any) bool {
	if num, ok := value.(json.Number); ok {
		_, err := num.Float64()
		return err == nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		return !math.IsNaN(f) && !math.IsInf(f, 0)
	}
	return false
}

// isFullDate reports whether s is a valid RFC 3339 full-date: exactly YYYY-MM-DD
// and a real calendar day (2024-02-29 yes, 2023-02-29 no).
func isFullDate(s string) bool {
	if !metadataFullDateRe.MatchString(s) {
		return false
	}
	_, err := time.Parse(MetadataTypeDateLayout, s)
	return err == nil
}

// describeJSONValue names the JSON type of value for error messages.
func describeJSONValue(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "boolean"
	}
	if isJSONNumber(value) {
		return "number"
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Map, reflect.Struct:
		return "object"
	}
	return fmt.Sprintf("%T", value)
}

// metadataTypeDescription renders a metadata type for error messages.
func metadataTypeDescription(typ string) string {
	switch typ {
	case MetadataTypeDate:
		return "date (YYYY-MM-DD)"
	case MetadataTypeEnum:
		return "enum"
	}
	return typ
}

// problemsError folds problems into one ErrInvalidMetadataKey error for the CRUD
// layer.
func problemsError(problems []Problem) error {
	if len(problems) == 0 {
		return nil
	}
	messages := make([]string, len(problems))
	for i, problem := range problems {
		messages[i] = problem.Message
	}
	return fmt.Errorf("%w: %s", ErrInvalidMetadataKey, strings.Join(messages, "; "))
}
