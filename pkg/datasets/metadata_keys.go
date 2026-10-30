// Package datasets implements the lakeFS datasets control plane described in
// DATASETS_DESIGN.md.
//
// This file holds the metadata key model (design §5.5). Metadata value
// validation lives in metadata_validate.go.
package datasets

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Metadata value types (design §5.5). The constants are untyped strings on
// purpose: MetadataKey.Type is a plain string in the wire format, so callers can
// assign these constants (and values decoded from JSON) without conversions.
const (
	MetadataTypeString  = "string"
	MetadataTypeEnum    = "enum"
	MetadataTypeNumber  = "number"
	MetadataTypeBoolean = "boolean"
	MetadataTypeDate    = "date"
)

// MetadataTypeDateLayout is the only accepted representation of a "date"
// metadata value: an RFC 3339 full-date, YYYY-MM-DD.
const MetadataTypeDateLayout = time.DateOnly

var (
	// metadataKeyNameRe is the metadata key name rule from design §5.5.
	metadataKeyNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

	validMetadataTypes = map[string]struct{}{
		MetadataTypeString:  {},
		MetadataTypeEnum:    {},
		MetadataTypeNumber:  {},
		MetadataTypeBoolean: {},
		MetadataTypeDate:    {},
	}
)

// MetadataKey is the definition of one metadata key (design §5.5). Keys are
// stored individually in KV (metadata_keys/<key>) and apply to the metadata of
// every definition of every dataset.
type MetadataKey struct {
	Name          string    `json:"name"`                     // ^[a-z][a-z0-9_]{0,62}$
	Type          string    `json:"type"`                     // string | enum | number | boolean | date
	Required      bool      `json:"required"`                 // must be present on every write
	AllowedValues []string  `json:"allowed_values,omitempty"` // enum only, non-empty
	AllowMultiple bool      `json:"allow_multiple"`           // value is a JSON array of Type
	Description   string    `json:"description,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	CreatedBy     string    `json:"created_by"`
}

// ErrInvalidMetadataKey reports a metadata key definition (or key update) that
// failed validation; the wrapped message carries the problems.
var ErrInvalidMetadataKey = errors.New("invalid metadata key")

// ErrMetadataKeyRename reports an attempt to rename a metadata key. The name is
// the KV key, so a rename is a delete plus a create.
var ErrMetadataKeyRename = errors.New("metadata key name cannot be changed")

// ErrMetadataKeyTypeChange reports an attempt to change a key's type. Design
// §5.5 forbids it: delete the key and recreate it instead. Existing metadata
// values were validated against the old type and are never re-validated, so a
// type change would silently reinterpret them.
var ErrMetadataKeyTypeChange = errors.New("metadata key type cannot be changed")

// IsValidMetadataKeyName reports whether name matches ^[a-z][a-z0-9_]{0,62}$.
func IsValidMetadataKeyName(name string) bool {
	return metadataKeyNameRe.MatchString(name)
}

// IsValidMetadataType reports whether typ is one of the supported metadata types.
func IsValidMetadataType(typ string) bool {
	_, ok := validMetadataTypes[typ]
	return ok
}

// ValidateMetadataKey checks a metadata key definition itself: its name, its type,
// and the allowed_values rules (required and non-empty for enum, rejected for
// every other type). It reports all problems, sorted by path then code.
//
// The metadata key CRUD layer must run this before storing a key.
// ValidateMetadata assumes its keys have already been validated.
func ValidateMetadataKey(key MetadataKey) []Problem {
	var problems []Problem

	if !IsValidMetadataKeyName(key.Name) {
		problems = append(problems, metadataProblem(key.Name, ProblemInvalidKeyName,
			fmt.Sprintf("metadata key name must match %s", metadataKeyNameRe.String())))
	}

	switch {
	case !IsValidMetadataType(key.Type):
		problems = append(problems, metadataProblem(key.Name, ProblemInvalidKeyType,
			fmt.Sprintf("type must be one of %s, got %q", strings.Join(sortedMetadataTypes(), ", "), key.Type)))
	case key.Type == MetadataTypeEnum && len(key.AllowedValues) == 0:
		problems = append(problems, metadataProblem(key.Name, ProblemAllowedValuesRequired,
			"an enum key requires a non-empty allowed_values list"))
	case key.Type != MetadataTypeEnum && len(key.AllowedValues) > 0:
		problems = append(problems, metadataProblem(key.Name, ProblemAllowedValuesNotAllowed,
			fmt.Sprintf("allowed_values is only valid for enum keys, got type %q", key.Type)))
	}

	SortProblems(problems)
	return problems
}

// ValidateKeyUpdate checks whether oldKey may be updated to newKey (design §5.5).
//
// Forbidden:
//   - renaming the key (the name is the KV key: delete + create instead);
//   - changing the type (delete + recreate instead).
//
// Allowed: changing allowed_values, required, allow_multiple and description.
// newKey must also be a valid definition on its own.
func ValidateKeyUpdate(oldKey, newKey MetadataKey) error {
	if oldKey.Name != newKey.Name {
		return fmt.Errorf("%w: %q -> %q: delete the old key and create a new one",
			ErrMetadataKeyRename, oldKey.Name, newKey.Name)
	}
	if oldKey.Type != newKey.Type {
		return fmt.Errorf("%w for key %q: %q -> %q: delete and recreate the key instead",
			ErrMetadataKeyTypeChange, oldKey.Name, oldKey.Type, newKey.Type)
	}
	if problems := ValidateMetadataKey(newKey); len(problems) > 0 {
		return fmt.Errorf("cannot update metadata key %q: %w", newKey.Name, problemsError(problems))
	}
	return nil
}

// sortedMetadataTypes returns the supported types in a stable order for messages.
func sortedMetadataTypes() []string {
	types := make([]string, 0, len(validMetadataTypes))
	for typ := range validMetadataTypes {
		types = append(types, typ)
	}
	sort.Strings(types)
	return types
}
