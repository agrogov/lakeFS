// Package datasets implements the lakeFS OSS Datasets control plane described in
// DATASETS_DESIGN.md.
//
// definition.go holds the Enterprise-compatible definition format (§5.1) and the
// normalisation rules (§5.2); validate.go holds the structural validation (§5.2).
// Both are pure data code: they depend on the standard library only, with no KV
// store, catalog or HTTP in the picture.
//
// Clean-room note (§2): everything here is written from DATASETS_DESIGN.md and the
// public lakeFS documentation. No code, SQL, protobuf or UI assets from lakeFS
// Enterprise, and nothing from upstream lakeFS >= 1.87, were used.
package datasets

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Definition-format constants (§5.1, §5.2, §6.4).
const (
	// FormatVersion is the only supported value of Definition.FormatVersion.
	FormatVersion = 1

	// ReservedTargetPrefix is the dataset-repository directory owned by the
	// service (§6.4): every version stores <ReservedTargetPrefix>/definition.json
	// there, so user targets may not live under it.
	ReservedTargetPrefix = "_dataset"

	// RootTarget is the canonical target of an item that lands at the top of the
	// dataset repository: the empty target of a prefix item (§5.2, "empty prefix
	// target = repo root").
	RootTarget = ""
)

// DatasetNamePattern is the dataset name rule of §5.2 rule 1. A dataset is
// materialised as a repository, so the pattern is the repository-ID one: a
// lower-case letter or digit followed by 2..62 lower-case letters, digits or
// hyphens.
const DatasetNamePattern = `^[a-z0-9][a-z0-9-]{2,62}$`

var datasetNameRegexp = regexp.MustCompile(DatasetNamePattern)

// reservedNames are names that pass DatasetNamePattern but may not be used as
// dataset names (§5.2 rule 1). "config" and "metadata-keys" collide with the
// control-plane routes of §10.1, which are siblings of /datasets/{ds}; "_datasets"
// is the KV partition of §5.3 and "_dataset" the reserved in-repository directory
// of §6.4. The two underscore names can never match DatasetNamePattern today;
// they are listed so that the reservation survives a future loosening of the
// pattern.
var reservedNames = map[string]struct{}{
	"config":        {},
	"metadata-keys": {},
	"_datasets":     {},
	"_dataset":      {},
}

// IsReservedName reports whether name is reserved and therefore may not be used
// as a dataset name, regardless of whether it matches DatasetNamePattern.
func IsReservedName(name string) bool {
	_, ok := reservedNames[name]
	return ok
}

// RefType is the kind of ref a data item is pinned to (§5.1). Only RefTypeCommit
// is ever stored: branch and tag refs are accepted on input and resolved to a
// commit ID at write time (§5.2).
type RefType string

const (
	RefTypeCommit RefType = "commit"
	RefTypeBranch RefType = "branch"
	RefTypeTag    RefType = "tag"
)

// SourceRef pins an item to a ref of its source repository (§5.1).
type SourceRef struct {
	Type RefType `json:"type"`
	ID   string  `json:"id"`
}

// ItemType is the kind of source a data item maps into the dataset (§5.1).
type ItemType string

const (
	ItemTypeObject    ItemType = "object"
	ItemTypePrefix    ItemType = "prefix"
	ItemTypeTable     ItemType = "table"     // reserved: Iceberg, needs a REST catalog (§1.2)
	ItemTypeNamespace ItemType = "namespace" // reserved: Iceberg, needs a REST catalog (§1.2)
)

// PathSource is the source descriptor of an "object" or "prefix" item (§5.1).
// For a prefix item the path always ends with "/" after normalisation, except for
// the empty path, which means the whole source repository.
type PathSource struct {
	Path string `json:"path"`
}

// TableSrc is the reserved source descriptor of a "table" item (Iceberg). The
// item type is rejected by validation in the MVP (§1.2) because OSS has no
// Iceberg REST catalog; the field exists so that Enterprise definition documents
// parse and round-trip.
type TableSrc struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// NsSrc is the reserved source descriptor of a "namespace" item (Iceberg), for
// the same reason as TableSrc.
type NsSrc struct {
	Name string `json:"name"`
}

// DataItem maps a source location into the dataset repository (§5.1). Exactly one
// of Object, Prefix, Table and Namespace is set, matching Type.
type DataItem struct {
	Target     string      `json:"target"`
	Repository string      `json:"repository"`
	Ref        SourceRef   `json:"ref"`
	Type       ItemType    `json:"type"`
	Object     *PathSource `json:"object,omitempty"`
	Prefix     *PathSource `json:"prefix,omitempty"`
	Table      *TableSrc   `json:"table,omitempty"`     // reserved
	Namespace  *NsSrc      `json:"namespace,omitempty"` // reserved
}

// Definition is the Enterprise-compatible definition body (§5.1). It carries no
// name: the name lives in Envelope/URL and in the dataset record.
type Definition struct {
	FormatVersion int            `json:"format_version"`
	Description   string         `json:"description,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"` // string | number | bool | []string
	Data          []DataItem     `json:"data"`
}

// Envelope is the §5.1 document shape: a dataset name, its definition and the
// optional publish instruction applied by POST /datasets (§10.1) and by
// `lakectl datasets create -f definition.json` (§12.1).
type Envelope struct {
	Name       string              `json:"name"`
	Definition Definition          `json:"definition"`
	Publish    *PublishInstruction `json:"publish,omitempty"`
}

// PublishInstruction is the optional "publish" member of Envelope (§5.1).
type PublishInstruction struct {
	Message string `json:"message,omitempty"`
}

// Normalise rewrites d into the canonical definition form of §5.2:
//
//   - ref types are lower-cased and trimmed, and so are repository names and ref
//     IDs;
//   - prefix source paths end with "/" (appended when missing); an empty prefix
//     source path means the whole repository and stays empty;
//   - targets lose their leading and trailing slashes and their empty segments, so
//     "no leading /", "no trailing /" and single separators hold for both item
//     types. For an "object" item the target is the exact output key; for a
//     "prefix" item the trailing "/" is stripped here and re-added by
//     materialisation, and an empty target means the repository root;
//   - items are sorted by target, which is the order in which they are stored.
//
// Rules that cannot be satisfied by rewriting are reported as Problems with a
// per-item path and the offending item is otherwise left untouched, so that
// Validate (which calls Normalise first) reports each problem exactly once:
// "." and ".." target segments, and targets under the reserved _dataset/ prefix
// (§6.4).
//
// Normalise is idempotent for every value it can normalise: rewriting a
// normalised definition changes nothing. Items whose problems are reported keep
// their submitted target, so a second call reports the same problems again;
// callers treat reported problems as fatal rather than retrying.
//
// Item indices in the returned Problems refer to the canonical (sorted)
// definition, because sorting is part of normalisation; a caller that needs to
// map them back to the submitted document should match on target.
func (d *Definition) Normalise() []Problem {
	if d == nil {
		return nil
	}
	var problems []Problem
	for i := range d.Data {
		item := &d.Data[i]
		item.Repository = strings.TrimSpace(item.Repository)
		item.Type = ItemType(strings.ToLower(strings.TrimSpace(string(item.Type))))
		item.Ref.Type = RefType(strings.ToLower(strings.TrimSpace(string(item.Ref.Type))))
		item.Ref.ID = strings.TrimSpace(item.Ref.ID)

		if p := normaliseTarget(i, item); p != nil {
			problems = append(problems, *p)
		}
		normaliseSource(item)
	}
	sort.SliceStable(d.Data, func(i, j int) bool { return d.Data[i].Target < d.Data[j].Target })
	return problems
}

// normaliseTarget canonicalises item.Target in place. A nil return means the item
// was normalised; a non-nil return is the problem that prevented it, in which case
// Target is left as submitted.
func normaliseTarget(i int, item *DataItem) *Problem {
	if hasDotSegment(item.Target) {
		return &Problem{
			Path:    itemPath(i, "target"),
			Message: `must not contain "." or ".." segments`,
		}
	}
	target := joinPathSegments(splitPathSegments(item.Target))
	if isReservedTarget(target) {
		return &Problem{
			Path:    itemPath(i, "target"),
			Message: fmt.Sprintf("must not be inside the reserved %q directory", ReservedTargetPrefix),
		}
	}
	item.Target = target
	return nil
}

// normaliseSource canonicalises the source descriptor that matches item.Type in
// place. Descriptors that do not match the item type are left untouched for
// Validate to report.
func normaliseSource(item *DataItem) {
	switch item.Type {
	case ItemTypeObject:
		if item.Object != nil {
			// lakeFS object keys are always relative to the repository root.
			item.Object.Path = strings.TrimLeft(item.Object.Path, "/")
		}
	case ItemTypePrefix:
		if item.Prefix != nil {
			p := strings.Trim(item.Prefix.Path, "/")
			if p == "" {
				// Empty prefix source = the whole repository. It stays empty
				// rather than becoming "/" so that it is a usable List prefix
				// (§5.2, §6.3).
				item.Prefix.Path = ""
			} else {
				item.Prefix.Path = p + "/"
			}
		}
	}
}

// isReservedTarget reports whether target is the reserved _dataset directory
// itself or anywhere below it (§6.4).
func isReservedTarget(target string) bool {
	if target == RootTarget {
		return false
	}
	return target == ReservedTargetPrefix || strings.HasPrefix(target, ReservedTargetPrefix+"/")
}

// splitPathSegments splits a slash-separated path and drops empty segments, which
// is how leading, trailing and doubled slashes appear in a submitted target.
func splitPathSegments(p string) []string {
	if p == "" {
		return nil
	}
	segments := make([]string, 0)
	for _, s := range strings.Split(p, "/") {
		if s != "" {
			segments = append(segments, s)
		}
	}
	return segments
}

// joinPathSegments is the inverse of splitPathSegments.
func joinPathSegments(segments []string) string {
	return strings.Join(segments, "/")
}

// hasDotSegment reports whether p contains a "." or ".." segment. Such a target
// cannot be normalised away — ".." in particular would change which keys an item
// maps — so it is rejected instead.
func hasDotSegment(p string) bool {
	for _, s := range strings.Split(p, "/") {
		if s == "." || s == ".." {
			return true
		}
	}
	return false
}
