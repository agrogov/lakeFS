package datasets

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/spf13/viper"
)

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
//	{Path: "metadata.tier", Message: "unknown_key: ..."}
//
// so they can be returned alongside the structural problems. MetadataKeysValidator
// (metadata_validate.go) is the ready-made adapter: it pulls def.Metadata, runs
// ValidateMetadata against the keys and applies datasets.metadata.strict.
// Structural validation in this package never inspects metadata values: it only
// calls the hook when one is supplied.
type MetadataValidator func(def *Definition) []Problem

// IcebergEnabledKey is the lakeFS setting that mounts the Iceberg REST catalog. It is
// the key pkg/icebergcatalog reads as icebergcatalog.ConfigKey + ".enabled"; the
// datasets section has no switch of its own, because a table or namespace item can only
// be materialised - and the dataset it produces only read back through /iceberg/api -
// when that catalog is served (ICEBERG_CATALOG_PLAN.md §9).
const IcebergEnabledKey = "iceberg.enabled"

// IcebergEnabled reports whether the reserved Iceberg item types (table, namespace) are
// accepted (§1.2, ICEBERG_CATALOG_PLAN.md §9).
//
// It is the single gate every validation entry point goes through: while iceberg.enabled is
// false a table or namespace item keeps answering with the 501 of §10.4, because nothing
// could materialise it and the resulting dataset could not be read through a catalog.
//
// It is a variable so that a caller holding an already-loaded catalog configuration, and a
// test, can pin the answer instead of mutating the process-wide viper instance. It is the
// only configuration read in this file: the rest of the package stays pure data code.
var IcebergEnabled = func() bool {
	return viper.GetBool(IcebergEnabledKey)
}

// ValidateOptions tunes Validate and ValidateEnvelope. The zero value is usable
// and means DefaultMaxItems with free-form metadata (no key validation).
type ValidateOptions struct {
	// MaxItems is datasets.max_items (§13). Values <= 0 mean DefaultMaxItems.
	MaxItems int

	// Metadata is the optional metadata-vs-keys validator (§5.2 rule 6). When nil,
	// metadata is not validated at all; Config.ValidateOptions sets it whenever the
	// caller has metadata keys to enforce or datasets.metadata.strict is on.
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
// opts.Metadata; MetadataKeysValidator is the adapter that implements it.
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

	// This is the only call site of the metadata hook: metadata is validated here,
	// once, so a metadata problem is never reported twice (see
	// TestValidateMetadataReportedOnce).
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
			if item.Object != nil {
				problems = append(problems, wildcardProblems(itemPath(i, "object.path"), item.Object.Path)...)
			}
		case ItemTypePrefix:
			// Both the target and the prefix source path may be empty here:
			// they mean "repository root" and "whole source repository".
			if item.Prefix != nil {
				problems = append(problems, wildcardProblems(itemPath(i, "prefix.path"), item.Prefix.Path)...)
			}
		case ItemTypeTable, ItemTypeNamespace:
			// ICEBERG_CATALOG_PLAN.md §9: the reserved types are accepted only when the catalog
			// that materialises and serves them is enabled. "(501)" is what the service turns
			// into ErrUnsupported (§10.4), so the disabled answer is unchanged.
			if !IcebergEnabled() {
				problems = append(problems, Problem{
					Path:    itemPath(i, "type"),
					Message: fmt.Sprintf("item type %q is not supported (501)", item.Type),
				})
				break
			}
			problems = append(problems, icebergItemProblems(i, item)...)
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

// wildcardProblems rejects the policy wildcards "*" and "?" in a source path (SEC-3). Source paths
// are literal object keys, but they are also spliced into resource ARNs that are matched as
// patterns, so a wildcard would let an item widen what the authorization check looks at.
func wildcardProblems(path, value string) []Problem {
	if strings.ContainsAny(value, "*?") {
		return []Problem{{Path: path, Message: `must not contain the wildcard characters "*" or "?"`}}
	}
	return nil
}

// icebergNamespaceSeparator is the character a definition document uses to join the levels of a
// multi-level Iceberg namespace. It is not the catalog's wire separator - that is 0x1F, see
// pkg/icebergcatalog NamespaceSeparator, because a URL-encoded control byte cannot collide with
// a namespace name - but the dotted notation lakeFS uses for a namespace outside a URL
// (Namespace.Dotted, the form a catalog ARN carries and the form diff.go compares). A
// definition is JSON, not a URL, and it has one string field for the namespace, so it follows
// the dotted form.
const icebergNamespaceSeparator = "."

// icebergNamespaceSeparatorByte is the catalog's own separator, spelled here so that a submitted
// namespace carrying it is rejected: it would be one namespace level on the wire and a literal
// control byte in a lakeFS key.
const icebergNamespaceSeparatorByte = "\x1f"

// icebergMaxNamespacePathLength is the exclusive upper bound of a namespace path in characters,
// the lakeFS rule the catalog enforces (pkg/icebergcatalog MaxNamespacePathLength): a namespace
// that fails it cannot be addressed through /iceberg/api, so a definition cannot use it either.
const icebergMaxNamespacePathLength = 255

// icebergAddress is the catalog address a table or namespace item materialises (§9).
//
// The reserved Iceberg layout is repository-wide: a table lives at
// _lakefs_iceberg/tables/<namespace levels>/<table>/.pointer and its files at
// _lakefs_iceberg/files/<table-uuid>/..., so a dataset that copies those keys unchanged answers
// /iceberg/api at <dataset>.<ref>.<namespace>.<table>. The address is therefore derived from the
// item's source descriptor alone (the namespace and name it names), never from a free-form
// target.
type icebergAddress struct {
	// path is the catalog path of the item: "<namespace levels>/<table>" for a table item and
	// "<namespace levels>" for a namespace item. It is also the item's required Target, which
	// is what keeps the §5.2 rule-3 overlap check meaningful for these items: two items overlap
	// exactly when one would materialise the other's keys.
	path string

	// namespace holds the namespace levels, outermost first. A namespace item materialises this
	// namespace and every namespace below it; a table item materialises the table in it.
	namespace []string

	// table is the table name of a table item; empty for a namespace item.
	table string
}

// icebergItemProblems validates the shape of a table or namespace item once the catalog is
// enabled (§9): the source descriptor must name a usable namespace and, for a table, a usable
// name, and the target must be the catalog path the item materialises at.
func icebergItemProblems(i int, item *DataItem) []Problem {
	_, problems := icebergItemAddress(i, item)
	return problems
}

// icebergItemAddress derives the catalog address of a table or namespace item and reports every
// problem that prevents it.
//
// A missing source descriptor is not reported here: validateSourceDescriptors owns that answer,
// so it is reported exactly once. The address is returned only when there is no problem with it,
// which lets a caller use path without re-checking.
//
// The target rule is the interesting one. The keys a reserved item produces are fixed by the
// catalog layout, so a target that disagrees with the catalog path could not be honoured;
// silently ignoring it would make the definition say one thing and the dataset do another, and
// would also let two items collide in the key space while passing the §5.2 rule-3 check. The
// target is therefore required to be exactly the catalog path.
func icebergItemAddress(i int, item *DataItem) (icebergAddress, []Problem) {
	var addr icebergAddress
	switch item.Type {
	case ItemTypeTable:
		if item.Table == nil {
			return icebergAddress{}, nil
		}
		levels, levelProblems := icebergNamespaceLevels(itemPath(i, "table.namespace"), item.Table.Namespace)
		if len(levelProblems) > 0 {
			return icebergAddress{}, levelProblems
		}
		if item.Table.Name == "" {
			return icebergAddress{}, []Problem{{Path: itemPath(i, "table.name"), Message: "required"}}
		}
		if p := icebergSegmentProblem(itemPath(i, "table.name"), item.Table.Name); p != nil {
			return icebergAddress{}, []Problem{*p}
		}
		addr.namespace = levels
		addr.table = item.Table.Name
		addr.path = strings.Join(levels, "/") + "/" + item.Table.Name
	case ItemTypeNamespace:
		if item.Namespace == nil {
			return icebergAddress{}, nil
		}
		levels, levelProblems := icebergNamespaceLevels(itemPath(i, "namespace"), *item.Namespace)
		if len(levelProblems) > 0 {
			return icebergAddress{}, levelProblems
		}
		addr.namespace = levels
		addr.path = strings.Join(levels, "/")
	default:
		return icebergAddress{}, nil
	}
	if item.Target != addr.path {
		return icebergAddress{}, []Problem{{
			Path:    itemPath(i, "target"),
			Message: fmt.Sprintf("must be %q, the catalog path this item materialises at", addr.path),
		}}
	}
	return addr, nil
}

// icebergNamespaceLevels splits a dotted namespace into its levels and validates each one. Every
// problem names path, the field the namespace came from ("data[2].table.namespace" or
// "data[2].namespace").
func icebergNamespaceLevels(path, namespace string) ([]string, []Problem) {
	if namespace == "" {
		return nil, []Problem{{Path: path, Message: "required"}}
	}
	if n := utf8.RuneCountInString(namespace); n >= icebergMaxNamespacePathLength {
		return nil, []Problem{{Path: path, Message: fmt.Sprintf(
			"must be shorter than %d characters, got %d", icebergMaxNamespacePathLength, n)}}
	}
	levels := strings.Split(namespace, icebergNamespaceSeparator)
	for _, level := range levels {
		if level == "" {
			return nil, []Problem{{Path: path, Message: "must not contain an empty namespace level"}}
		}
		if p := icebergSegmentProblem(path, level); p != nil {
			return nil, []Problem{*p}
		}
		for _, r := range level {
			if !icebergNamespaceRune(r) {
				return nil, []Problem{{Path: path, Message: fmt.Sprintf(
					"namespace level %q contains %q; only letters, digits, underscores and hyphens are allowed",
					level, r)}}
			}
		}
	}
	return levels, nil
}

// icebergNamespaceRune reports whether r may appear in one level of a namespace. Dots are absent
// on purpose: they separate levels, so a level cannot contain one. The set is the catalog's own
// (pkg/icebergcatalog validNamespaceRune, minus dots), because a level the catalog cannot address
// would make the table unreachable through /iceberg/api.
func icebergNamespaceRune(r rune) bool {
	if r == utf8.RuneError {
		return false
	}
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-'
}

// icebergSegmentProblem rejects a value that cannot be one component of the reserved
// _lakefs_iceberg/ layout: a path or namespace separator would let an item reach keys outside
// the table it names, so this is a security rule as much as a shape rule. It mirrors
// pkg/icebergcatalog validateLayoutSegment, which the catalog applies when it builds the same
// key, and it is applied to table names too - a table name is a path component there.
func icebergSegmentProblem(path, segment string) *Problem {
	switch {
	case segment == "." || segment == "..":
		return &Problem{Path: path, Message: fmt.Sprintf("%q is reserved as a path component", segment)}
	case strings.ContainsAny(segment, `/\`):
		return &Problem{Path: path, Message: "must not contain a path separator"}
	case strings.Contains(segment, icebergNamespaceSeparatorByte):
		return &Problem{Path: path, Message: "must not contain the iceberg namespace separator"}
	}
	return nil
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
