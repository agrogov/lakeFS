package datasets

import (
	"fmt"
	"strings"
)

// DefaultMaxItems is the default of the datasets.max_items setting (§13).
const DefaultMaxItems = 1000

// Problem is a single validation finding. It is rendered as one entry of the
// "errors" array of a 400 response (§10.4):
//
//	{"path": "data[2].target", "message": "overlaps data[0]"}
//
// Path names the offending part of the document: a top-level field ("name",
// "format_version", "data"), an item field ("data[2].target") or a nested item
// field ("data[2].object.path"). It is empty only when the document itself is
// missing.
type Problem struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// ValidationError is the error returned by Validate and ValidateEnvelope. It
// always carries every problem that was found, never just the first one, because
// the API returns them all at once (§10.4).
type ValidationError struct {
	Problems []Problem
}

// Error implements error.
func (e *ValidationError) Error() string {
	if len(e.Problems) == 0 {
		return "invalid dataset definition"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "invalid dataset definition: %d problem(s)", len(e.Problems))
	for _, p := range e.Problems {
		if p.Path == "" {
			fmt.Fprintf(&b, "; %s", p.Message)
			continue
		}
		fmt.Fprintf(&b, "; %s: %s", p.Path, p.Message)
	}
	return b.String()
}

// MetadataValidator checks Definition.Metadata against the registered metadata
// keys (§5.2 rule 6, §5.5). It reports per-key problems such as
//
//	{Path: "metadata.tier", Message: `unknown metadata key`}
//
// so they can be returned alongside the structural problems.
//
// This is the extension point for the metadata-vs-keys validation owned by DS-03.
// Structural validation in this package never inspects metadata values: it only
// calls the hook when one is supplied.
type MetadataValidator func(def *Definition) []Problem

// ValidateOptions tunes Validate and ValidateEnvelope. The zero value is usable
// and means DefaultMaxItems with no metadata validation.
type ValidateOptions struct {
	// MaxItems is datasets.max_items (§13). Values <= 0 mean DefaultMaxItems.
	MaxItems int

	// Metadata is the optional metadata-vs-keys validator (§5.2 rule 6). When nil,
	// metadata is not validated at all, which is the behaviour of this card.
	Metadata MetadataValidator
}

// maxItems returns the effective item limit.
func (o ValidateOptions) maxItems() int {
	if o.MaxItems <= 0 {
		return DefaultMaxItems
	}
	return o.MaxItems
}

// ValidateName checks §5.2 rule 1: the DatasetNamePattern a dataset name must
// match, and the names reserved for control-plane routes and internal use.
func ValidateName(name string) []Problem {
	var problems []Problem
	if !datasetNameRegexp.MatchString(name) {
		problems = append(problems, Problem{
			Path:    "name",
			Message: fmt.Sprintf("must match %s", DatasetNamePattern),
		})
	}
	if IsReservedName(name) {
		problems = append(problems, Problem{
			Path:    "name",
			Message: fmt.Sprintf("%q is reserved", name),
		})
	}
	return problems
}

// ValidateEnvelope validates a whole §5.1 document: the dataset name (§5.2 rule 1)
// and the definition (see Validate).
func ValidateEnvelope(env *Envelope, opts ValidateOptions) error {
	if env == nil {
		return &ValidationError{Problems: []Problem{{Message: "document is missing"}}}
	}
	problems := ValidateName(env.Name)
	problems = append(problems, definitionProblems(&env.Definition, opts)...)
	return newValidationError(problems)
}

// Validate checks a definition against the structural rules of §5.2 that need no
// catalog access:
//
//   - rule 2: format_version is FormatVersion, and 1 <= len(data) <= opts.MaxItems;
//   - rule 3: targets are well formed and do not overlap;
//   - the per-item shape every later stage relies on: a repository, a ref and
//     exactly one source descriptor that matches the item type;
//   - the reserved _dataset/ target prefix of §6.4.
//
// Rule 1 (the name) is not part of a Definition: use ValidateName, or
// ValidateEnvelope for a whole document.
//
// Validate normalises d in place first (§5.2, "applied before validation and
// storage"), so callers do not need to call Normalise themselves. Every problem is
// reported, each with a per-item path (§10.4); item indices refer to the
// normalised, target-sorted definition.
//
// Rules 4 and 5 need the catalog and are checked by the service: whether the source
// repository exists, is not itself a dataset and shares the dataset's StorageID,
// and whether the referenced source keys exist (and a prefix is non-empty unless
// allow_empty_items). Rule 6 (metadata against metadata keys) is delegated to
// opts.Metadata, which is owned by DS-03.
func Validate(d *Definition, opts ValidateOptions) error {
	return newValidationError(definitionProblems(d, opts))
}

// definitionProblems returns every structural problem of d, normalising it first.
func definitionProblems(d *Definition, opts ValidateOptions) []Problem {
	if d == nil {
		return []Problem{{Message: "definition is missing"}}
	}

	problems := d.Normalise()

	if d.FormatVersion != FormatVersion {
		problems = append(problems, Problem{
			Path:    "format_version",
			Message: fmt.Sprintf("must be %d, got %d", FormatVersion, d.FormatVersion),
		})
	}

	maxItems := opts.maxItems()
	switch {
	case len(d.Data) == 0:
		problems = append(problems, Problem{
			Path:    "data",
			Message: "must contain at least one item",
		})
	case len(d.Data) > maxItems:
		problems = append(problems, Problem{
			Path:    "data",
			Message: fmt.Sprintf("must contain at most %d items, got %d", maxItems, len(d.Data)),
		})
	}

	problems = append(problems, validateItems(d)...)

	if opts.Metadata != nil {
		problems = append(problems, opts.Metadata(d)...)
	}
	return problems
}

// newValidationError converts problems into the returned error, or nil.
func newValidationError(problems []Problem) error {
	if len(problems) == 0 {
		return nil
	}
	return &ValidationError{Problems: problems}
}

// validateItems checks each item's structure and then its target collisions, so
// that all problems of one item are reported together, in document order.
func validateItems(d *Definition) []Problem {
	first := firstTargetIndex(d)
	var problems []Problem
	for i := range d.Data {
		item := &d.Data[i]
		if item.Repository == "" {
			problems = append(problems, Problem{Path: itemPath(i, "repository"), Message: "required"})
		}
		switch item.Ref.Type {
		case RefTypeCommit, RefTypeBranch, RefTypeTag:
		default:
			problems = append(problems, Problem{
				Path:    itemPath(i, "ref.type"),
				Message: `must be one of "commit", "branch", "tag"`,
			})
		}
		if item.Ref.ID == "" {
			problems = append(problems, Problem{Path: itemPath(i, "ref.id"), Message: "required"})
		}

		problems = append(problems, validateSourceDescriptors(i, item)...)

		switch item.Type {
		case ItemTypeObject:
			// An object target is the exact output key, so it cannot be empty;
			// for a prefix item the empty target means the repository root (§5.2).
			if item.Target == RootTarget {
				problems = append(problems, Problem{
					Path:    itemPath(i, "target"),
					Message: `required for item type "object"`,
				})
			}
			if item.Object != nil && item.Object.Path == "" {
				problems = append(problems, Problem{Path: itemPath(i, "object.path"), Message: "required"})
			}
		case ItemTypePrefix:
			// Both the target and the prefix source path may be empty here:
			// they mean "repository root" and "whole source repository".
		case ItemTypeTable, ItemTypeNamespace:
			problems = append(problems, Problem{
				Path:    itemPath(i, "type"),
				Message: fmt.Sprintf("item type %q is not supported (501)", item.Type),
			})
		default:
			problems = append(problems, Problem{
				Path:    itemPath(i, "type"),
				Message: `must be one of "object", "prefix"`,
			})
		}

		problems = append(problems, targetProblems(i, d, first)...)
	}
	return problems
}

// validateSourceDescriptors enforces that exactly one source descriptor is set and
// that it matches the item type: object for "object", prefix for "prefix", table
// for "table", namespace for "namespace".
func validateSourceDescriptors(i int, item *DataItem) []Problem {
	var problems []Problem
	for _, f := range []struct {
		name string
		set  bool
	}{
		{"object", item.Object != nil},
		{"prefix", item.Prefix != nil},
		{"table", item.Table != nil},
		{"namespace", item.Namespace != nil},
	} {
		want := string(item.Type) == f.name
		switch {
		case f.set && !want:
			problems = append(problems, Problem{
				Path:    itemPath(i, f.name),
				Message: fmt.Sprintf("must not be set for item type %q", item.Type),
			})
		case !f.set && want:
			problems = append(problems, Problem{
				Path:    itemPath(i, f.name),
				Message: fmt.Sprintf("required for item type %q", item.Type),
			})
		}
	}
	return problems
}

// firstTargetIndex maps every normalised target to the index of the first item
// that uses it.
func firstTargetIndex(d *Definition) map[string]int {
	first := make(map[string]int, len(d.Data))
	for i := range d.Data {
		target := d.Data[i].Target
		if _, ok := first[target]; !ok {
			first[target] = i
		}
	}
	return first
}

// targetProblems reports the target collisions of item i (§5.2 rule 3). Two items
// collide when their targets are equal, or when one target is a path prefix of the
// other on a "/" boundary: "a" vs "a/b" and "a" vs "a/b/c" are errors while "a"
// vs "a-b" and "a/b" vs "a/c" are fine. This guarantees that every key of the
// dataset has exactly one source, which is what makes materialisation a pure
// k-way merge (§6.4). An item with the root target ("") covers every key, so it
// collides with every other item.
//
// Walked per item rather than over the sorted slice, because a real prefix need
// not be adjacent to its descendant in sorted order: "a", "a-b", "a/b" sorts as
// "a", "a-b", "a/b" ('-' 0x2d < '/' 0x2f), so only the ancestor lookup finds the
// "a" / "a/b" collision.
func targetProblems(i int, d *Definition, first map[string]int) []Problem {
	target := d.Data[i].Target
	if j := first[target]; j < i {
		return []Problem{{
			Path:    itemPath(i, "target"),
			Message: fmt.Sprintf("duplicates data[%d]", j),
		}}
	}

	// Deepest ancestor first, so the reported conflict is the most specific one.
	ancestors := ancestorTargets(target)
	for k := len(ancestors) - 1; k >= 0; k-- {
		if j, ok := first[ancestors[k]]; ok {
			return []Problem{{
				Path:    itemPath(i, "target"),
				Message: fmt.Sprintf("overlaps data[%d]", j),
			}}
		}
	}
	return nil
}

// ancestorTargets returns the targets whose keys would also be produced by target,
// i.e. the targets it overlaps when they exist: the root target (the empty prefix
// is a prefix of every path) followed by every "/" boundary prefix, shallowest
// first.
func ancestorTargets(target string) []string {
	if target == RootTarget {
		return nil
	}
	ancestors := make([]string, 0)
	ancestors = append(ancestors, RootTarget)
	for i := 0; i < len(target); i++ {
		if target[i] == '/' && i > 0 {
			ancestors = append(ancestors, target[:i])
		}
	}
	return ancestors
}

// itemPath renders the per-item problem path of §10.4, for example
// "data[2].target".
func itemPath(i int, field string) string {
	if field == "" {
		return fmt.Sprintf("data[%d]", i)
	}
	return fmt.Sprintf("data[%d].%s", i, field)
}
