// Package datasets implements the lakeFS OSS Datasets control plane described in
// DATASETS_DESIGN.md.
//
// diff.go implements the version diff of §10.3, the one endpoint that compares two
// references of the same dataset:
//
//   - the *definition diff* is pure data code: two resolved definitions (§5.1) are
//     matched by item target, and the description, the metadata map and every item that
//     was added, removed or changed are reported. Output is deterministic (items sorted
//     by target, metadata by key), so the API answer, the UI and the tests are stable and
//     two calls with the same inputs are byte-for-byte identical.
//   - the *data diff* is the native graveler diff of the dataset repository between the
//     two refs (§4.2: a dataset is a materialised repository, so a version diff is a
//     ref diff). It is paginated like the core API diff and filters out the synthetic
//     _dataset/ entries of §6.4, which are bookkeeping, not data.
//
// Ref forms (§10.3): `vN` (also accepted as plain `N` and `VN`, matching the pattern of
// api/datasets.yml), `latest` and `proposal-<id>`. A version resolves to the version
// tag `vN` in the dataset repository and its VersionRecord definition; `latest`
// resolves to the dataset branch DefaultBranch; a proposal resolves to its hidden
// preview branch `_proposal-<id>` (§5.6) and the resolved definition stored in the
// proposal record. Ref resolution is catalog-free for definition diffs and needs the
// catalog only for a version whose definition is stored behind DefinitionRef (§5.3).
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS
// documentation only. No code, SQL, protobuf or UI asset from lakeFS Enterprise, and
// nothing from upstream lakeFS >= 1.87, was used.
package datasets

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/kv"
)

// ErrRefNotFound reports a dataset ref that does not exist: an unknown version, a
// `latest` on a dataset without versions, or an unknown proposal (§10.4, 404). It is a
// distinct sentinel from ErrDatasetNotFound because the dataset does exist and the
// client's fix is different (pick another ref versus another dataset).
var ErrRefNotFound = errors.New("dataset ref not found")

// RefNameLatest is the "latest" ref form of §10.3: the newest published version.
const RefNameLatest = "latest"

// Ref-form prefixes. A proposal ref is `proposal-<id>`; the hidden branch the proposal
// is materialised on (and therefore the graveler ref a data diff of a proposal uses) is
// `_proposal-<id>` (§5.6, §10.3).
const (
	ProposalRefPrefix           = "proposal-"
	proposalPreviewBranchPrefix = "_proposal-"
)

// ProposalPreviewBranch returns the hidden dataset-repository branch a proposal is
// materialised on (§5.6). It is also what ProposalRecord.PreviewBranch holds.
func ProposalPreviewBranch(id string) string {
	return proposalPreviewBranchPrefix + id
}

// ChangeType is the classification of one entry of a diff (§10.2): "added", "removed" or
// "changed". The same three values classify definition items, metadata entries and data
// paths, which is why the core API diff and DefinitionDiff share the vocabulary.
type ChangeType string

const (
	ChangeAdded   ChangeType = "added"
	ChangeRemoved ChangeType = "removed"
	ChangeChanged ChangeType = "changed"
)

// DescriptionDiff is the description member of a definition diff: the description before
// and after. It is present only when the description actually differs.
type DescriptionDiff struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// MetadataDiff is one metadata entry of a definition diff (§10.2 MetadataChange). From is
// absent for an added entry and To for a removed one; both are set when a key's value
// changed. They are pointers to interface{} so that a metadata value that is itself JSON
// null still round-trips as an explicit member.
type MetadataDiff struct {
	Key    string     `json:"key"`
	Change ChangeType `json:"change"`
	From   *any       `json:"from,omitempty"`
	To     *any       `json:"to,omitempty"`
}

// ItemDiff is one data item of a definition diff (§10.2 ItemChange): the item as it was in
// the left definition, as it is in the right one, or both when it changed.
type ItemDiff struct {
	Target string     `json:"target"`
	Change ChangeType `json:"change"`
	From   *DataItem  `json:"from,omitempty"`
	To     *DataItem  `json:"to,omitempty"`
}

// DefinitionDiff is the structural diff of two resolved definitions (§10.3). Items is
// always present (possibly empty); Description is omitted when unchanged and Metadata when
// no entry changed.
type DefinitionDiff struct {
	Description *DescriptionDiff `json:"description,omitempty"`
	Metadata    []MetadataDiff   `json:"metadata,omitempty"`
	Items       []ItemDiff       `json:"items"`
}

// Empty reports whether the diff changes nothing: no item, no metadata key and no
// description. The service uses it to answer "no changes" without making the caller
// compare empty lists.
func (d *DefinitionDiff) Empty() bool {
	return d == nil || (d.Description == nil && len(d.Metadata) == 0 && len(d.Items) == 0)
}

// DiffDefinitions returns the definition diff between left and right (§10.3).
//
// Both sides are normalised first (Normalise, §5.2: canonical targets and source paths,
// items sorted by target), so the result does not depend on the order in which the two
// documents happen to store their items, and a definition that differs only in, say, a
// trailing slash on a prefix source path is not reported as a change. Normalisation
// problems are ignored here: a diff is a read, and a stored definition was validated when
// it was written.
//
// The left definition is the "from" side and the right one the "to" side. A nil
// definition is treated as an empty one (no items, no metadata, no description), which is
// what a dataset without a published version looks like.
//
// Ordering: Items are sorted by target and Metadata by key, so the answer is
// deterministic. Targets are unique within a validated definition (§5.2 rule 3), which is
// what makes "matched by target" total.
func DiffDefinitions(left, right *Definition) *DefinitionDiff {
	leftDef := normaliseForDiff(left)
	rightDef := normaliseForDiff(right)

	diff := &DefinitionDiff{Items: diffItems(leftDef.Data, rightDef.Data)}
	if leftDef.Description != rightDef.Description {
		diff.Description = &DescriptionDiff{From: leftDef.Description, To: rightDef.Description}
	}
	diff.Metadata = diffMetadata(leftDef.Metadata, rightDef.Metadata)
	return diff
}

// normaliseForDiff returns a normalised copy of def, so DiffDefinitions never edits the
// caller's document (a service may hold the same Definition for a version record and a
// proposal).
func normaliseForDiff(def *Definition) Definition {
	if def == nil {
		return Definition{FormatVersion: FormatVersion}
	}
	out := cloneDefinition(*def)
	// Normalise reports problems for targets it cannot canonicalise; a diff has nothing
	// better to do with them than to report the target as it was stored.
	_ = out.Normalise()
	return out
}

// diffItems matches two target-sorted item slices by target (§10.3). Both inputs come
// from normaliseForDiff, so they are sorted and the walk is a linear merge.
func diffItems(left, right []DataItem) []ItemDiff {
	items := make([]ItemDiff, 0, len(left)+len(right))
	for i, j := 0, 0; i < len(left) || j < len(right); {
		switch {
		case j >= len(right) || (i < len(left) && left[i].Target < right[j].Target):
			removed := left[i]
			items = append(items, ItemDiff{Target: removed.Target, Change: ChangeRemoved, From: &removed})
			i++
		case i >= len(left) || right[j].Target < left[i].Target:
			added := right[j]
			items = append(items, ItemDiff{Target: added.Target, Change: ChangeAdded, To: &added})
			j++
		default:
			if !dataItemsEqual(left[i], right[j]) {
				from, to := left[i], right[j]
				items = append(items, ItemDiff{Target: from.Target, Change: ChangeChanged, From: &from, To: &to})
			}
			i++
			j++
		}
	}
	if len(items) == 0 {
		return []ItemDiff{}
	}
	return items
}

// dataItemsEqual reports whether two items with the same target are the same pin. Per
// §10.3 an item is "changed" when its repository, commit, item type or source path
// differs; the ref type is compared too, which costs nothing and keeps a hand-built
// definition with an unresolved ref honest. Both sides of a diff are normally resolved
// (§5.2: stored and proposal definitions only hold `type: commit`).
func dataItemsEqual(a, b DataItem) bool {
	return a.Repository == b.Repository &&
		a.Ref.Type == b.Ref.Type &&
		a.Ref.ID == b.Ref.ID &&
		a.Type == b.Type &&
		itemSourceKey(a) == itemSourceKey(b)
}

// itemSourceKey returns the source location an item reads, as one comparable string: the
// object key, the normalised prefix, or the reserved Iceberg identifiers. Items of
// different types never compare equal anyway, because ItemType is part of
// dataItemsEqual. Unlike authz.go's itemSourcePath it also covers the reserved `table` and
// `namespace` types, so a definition that round-trips through Enterprise stays diffable.
func itemSourceKey(item DataItem) string {
	switch item.Type {
	case ItemTypeObject:
		if item.Object != nil {
			return item.Object.Path
		}
	case ItemTypePrefix:
		if item.Prefix != nil {
			return item.Prefix.Path
		}
	case ItemTypeTable:
		if item.Table != nil {
			return item.Table.Namespace + "." + item.Table.Name
		}
	case ItemTypeNamespace:
		if item.Namespace != nil {
			return item.Namespace.Name
		}
	}
	return ""
}

// diffMetadata diffs two metadata maps by key (§10.3), in key order. It returns nil when no
// key changed, so the member is omitted from the JSON answer.
func diffMetadata(left, right map[string]any) []MetadataDiff {
	keys := sortedMetadataKeys(left, right)
	changes := make([]MetadataDiff, 0, len(keys))
	for _, key := range keys {
		leftValue, inLeft := left[key]
		rightValue, inRight := right[key]
		switch {
		case inLeft && !inRight:
			from := leftValue
			changes = append(changes, MetadataDiff{Key: key, Change: ChangeRemoved, From: &from})
		case !inLeft && inRight:
			to := rightValue
			changes = append(changes, MetadataDiff{Key: key, Change: ChangeAdded, To: &to})
		case !metadataValuesEqual(leftValue, rightValue):
			from, to := leftValue, rightValue
			changes = append(changes, MetadataDiff{Key: key, Change: ChangeChanged, From: &from, To: &to})
		}
	}
	if len(changes) == 0 {
		return nil
	}
	return changes
}

// sortedMetadataKeys returns the union of the keys of both maps in lexical order.
func sortedMetadataKeys(left, right map[string]any) []string {
	seen := make(map[string]struct{}, len(left)+len(right))
	keys := make([]string, 0, len(left)+len(right))
	for _, metadata := range []map[string]any{left, right} {
		for key := range metadata {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// metadataValuesEqual compares two metadata values as JSON documents.
//
// A metadata value is JSON (string, number, bool or []string, §5.1), so comparing its
// canonical encoding is both the right equality and a stable one: 1 (int, from a
// hand-built test) and 1 (float64, from a decoded body) are the same value, a nil and an
// absent value are not confused, and encoding/json sorts object keys, so two equal
// documents always encode identically. Values that cannot be encoded fall back to
// reflect.DeepEqual rather than reporting a spurious change.
func metadataValuesEqual(a, b any) bool {
	encodedA, errA := json.Marshal(a)
	encodedB, errB := json.Marshal(b)
	if errA != nil || errB != nil {
		return reflect.DeepEqual(a, b)
	}
	return bytes.Equal(encodedA, encodedB)
}

// DataDiffEntry is one path of a data diff (§10.2 DataDiffEntry): the same shape as the
// core API diff, so a client that renders `lakectl diff` renders this too.
type DataDiffEntry struct {
	Path      string     `json:"path"`
	Type      ChangeType `json:"type"`
	SizeBytes int64      `json:"size_bytes"`
}

// DataDiff is one page of a data diff (§10.2 DataDiffList). NextOffset is the path to pass
// as `after` to fetch the next page; it is empty when HasMore is false.
type DataDiff struct {
	Entries    []DataDiffEntry `json:"results"`
	HasMore    bool            `json:"has_more"`
	NextOffset string          `json:"next_offset,omitempty"`
	MaxPerPage int             `json:"max_per_page"`
}

// DataDiffRequest is the input of Differ.Diff: the dataset, the two graveler refs to
// compare and the pagination of §10.3.
type DataDiffRequest struct {
	Dataset string
	// Left and Right are graveler refs in the dataset repository: a version tag `vN`,
	// the dataset branch DefaultBranch, or a proposal preview branch.
	Left  string
	Right string
	// Prefix restricts the diff to dataset paths starting with it.
	Prefix string
	// After continues a previous page: the last path of that page.
	After string
	// Amount is the requested page size; 0 or less means DefaultListAmount, more than
	// MaxListAmount is capped.
	Amount int
}

// Differ computes data diffs of dataset repositories (§10.3). It is stateless and can be
// reused across requests.
type Differ struct {
	// Catalog is the lakeFS catalog. Required.
	Catalog *catalog.Catalog
}

// Diff returns one page of the data diff between req.Left and req.Right on the dataset
// repository (§10.3): the native graveler diff, so it is a metarange comparison and not an
// object listing.
//
// The synthetic _dataset/ entry every version carries (§6.4: the definition document) is
// filtered out, because it is dataset bookkeeping rather than data; so is anything else
// under the reserved ReservedTargetPrefix, which user targets cannot use (§5.2). Filtering
// happens after the catalog page, so a page can hold fewer entries than requested while
// more pages remain; the loop therefore keeps reading until it has Amount entries or the
// diff is exhausted, and HasMore/NextOffset describe the filtered result rather than the
// raw one.
//
// Pagination is path-based like the core diff: `after` is the last path the client saw and
// the catalog skips straight to it in the metarange, so paging is cheap and stable.
func (d *Differ) Diff(ctx context.Context, req DataDiffRequest) (*DataDiff, error) {
	if d == nil || d.Catalog == nil {
		return nil, fmt.Errorf("%w: differ has no catalog", ErrIncompleteService)
	}
	amount := paginationAmount(req.Amount)
	result := &DataDiff{
		Entries:    make([]DataDiffEntry, 0, amount),
		MaxPerPage: amount,
	}

	after := req.After
	for {
		page, more, err := d.Catalog.Diff(ctx, req.Dataset, req.Left, req.Right, catalog.DiffParams{
			Prefix: req.Prefix,
			After:  after,
			Limit:  amount - len(result.Entries),
		})
		if err != nil {
			if isNotFound(err) {
				return nil, fmt.Errorf("%w: dataset %q: %w", ErrRefNotFound, req.Dataset, err)
			}
			return nil, fmt.Errorf("datasets: diff %q %s..%s: %w", req.Dataset, req.Left, req.Right, err)
		}

		overflowed := false
		for _, diff := range page {
			// Advance the cursor over every entry the catalog returned, including the
			// ones filtered out below, so the next page never re-reads them.
			after = diff.Path
			if isReservedTarget(diff.Path) {
				continue
			}
			change, ok := dataDiffChange(diff.Type)
			if !ok {
				continue
			}
			if len(result.Entries) >= amount {
				// An eligible entry beyond the page: the diff continues even though this
				// catalog page might have been the last one.
				overflowed = true
				break
			}
			result.Entries = append(result.Entries, DataDiffEntry{
				Path:      diff.Path,
				Type:      change,
				SizeBytes: diff.Size,
			})
		}

		switch {
		case overflowed:
			result.HasMore = true
		case len(result.Entries) >= amount:
			result.HasMore = more
		case !more || len(page) == 0:
			// Nothing left. The len(page)==0 guard keeps a catalog that reports more
			// without producing anything from looping forever.
			result.HasMore = false
		default:
			continue
		}
		break
	}

	if result.HasMore && len(result.Entries) > 0 {
		result.NextOffset = result.Entries[len(result.Entries)-1].Path
	}
	return result, nil
}

// dataDiffChange maps a graveler diff type onto the three-value vocabulary of §10.2. The
// bool is false for DifferenceTypeNone, which is not a change and never reaches a diff.
//
// A two-way graveler diff only produces added, removed and changed. Conflict and
// PrefixChanged are mapped to "changed" instead of being dropped: the first cannot appear
// in a ref-to-ref diff but is a change if it ever does, and the second is how a delimiter
// diff reports a common prefix (pkg/catalog: a common prefix is always "changed").
func dataDiffChange(typ catalog.DifferenceType) (ChangeType, bool) {
	switch typ {
	case catalog.DifferenceTypeAdded:
		return ChangeAdded, true
	case catalog.DifferenceTypeRemoved:
		return ChangeRemoved, true
	case catalog.DifferenceTypeChanged, catalog.DifferenceTypeConflict, catalog.DifferenceTypePrefixChanged:
		return ChangeChanged, true
	default:
		return "", false
	}
}

// paginationAmount applies the list pagination defaults of §10.1 to a diff amount: a
// missing or non-positive amount is DefaultListAmount and anything larger than
// MaxListAmount is capped, so a client cannot ask for an unbounded diff page.
func paginationAmount(amount int) int {
	switch {
	case amount <= 0:
		return DefaultListAmount
	case amount > MaxListAmount:
		return MaxListAmount
	default:
		return amount
	}
}

// diffRef is one side of a dataset diff after ref resolution (§10.3): the ref the client
// sent, the version or proposal it named, the graveler ref to compare in the dataset
// repository, and the resolved definition of that side. It is internal to the package: a
// ref is resolved only as part of Service.Diff, which is where the caller is authorized.
type diffRef struct {
	// Ref is the ref as the client wrote it, for error messages: "v3", "latest" or
	// "proposal-<id>".
	Ref string
	// Version is the version number the ref names, 0 when it names a proposal.
	Version int
	// ProposalID is the proposal the ref names, empty for a version ref.
	ProposalID string
	// GravelerRef is the ref to use in the dataset repository: tag "vN" for a version,
	// DefaultBranch for latest, and the proposal preview branch for a proposal.
	GravelerRef string
	// Definition is the resolved definition of this side (§4.3 "resolved definitions").
	Definition *Definition
}

// refTarget is a parsed ref form before it is resolved against the dataset (§10.3).
type refTarget struct {
	latest     bool
	version    int
	proposalID string
}

// parseRefTarget parses one of the three ref forms of §10.3. The accepted grammar matches
// the leftRef/rightRef pattern of api/datasets.yml: `latest`, an optional `v`/`V` followed
// by 1..9 digits, or `proposal-` followed by alphanumerics.
//
// A ref that does not parse is a client problem and is reported as a *ValidationError with
// the given field path (left_ref/right_ref), which the API answers with 400 (§10.4); a ref
// that parses but names nothing is ErrRefNotFound (404).
func parseRefTarget(field, ref string) (refTarget, error) {
	invalid := func(message string) (refTarget, error) {
		return refTarget{}, &ValidationError{Problems: []Problem{{Path: field, Message: message}}}
	}
	switch {
	case ref == "":
		return invalid("must not be empty")
	case ref == RefNameLatest:
		return refTarget{latest: true}, nil
	case strings.HasPrefix(ref, ProposalRefPrefix):
		id := strings.TrimPrefix(ref, ProposalRefPrefix)
		if !isAlphanumeric(id) {
			return invalid(fmt.Sprintf("must be %q followed by the proposal id", ProposalRefPrefix))
		}
		return refTarget{proposalID: id}, nil
	}

	digits := ref
	if len(digits) > 0 && (digits[0] == 'v' || digits[0] == 'V') {
		digits = digits[1:]
	}
	if !isDigits(digits) {
		return invalid(`must be "latest", a version ("v3", "3") or a proposal ("proposal-<id>")`)
	}
	version, err := strconv.Atoi(digits)
	if err != nil || version <= 0 {
		return invalid("version must be a positive number")
	}
	return refTarget{version: version}, nil
}

// isDigits reports whether s is a non-empty run of decimal digits.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// isAlphanumeric reports whether s is a non-empty run of ASCII letters and digits. It is
// the id part of the ref grammar of api/datasets.yml.
func isAlphanumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		default:
			return false
		}
	}
	return true
}

// resolveRef resolves ref (a ref of *this* dataset, §10.3) against the dataset record of
// name, returning the resolved side of a diff. It performs no authorization: Service.Diff
// authorizes the caller once, before resolving either side (§9.1, §4.6). field names the
// request member the ref came from, so a malformed left or right ref is reported against
// the field the client sent.
func (s *Service) resolveRef(ctx context.Context, name, field, ref string) (*diffRef, error) {
	target, err := parseRefTarget(field, ref)
	if err != nil {
		return nil, err
	}
	rec, err := s.store.GetDataset(ctx, name)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, fmt.Errorf("%w: %q", ErrDatasetNotFound, name)
	}
	if err != nil {
		return nil, err
	}
	if target.proposalID != "" {
		return s.resolveProposalRef(ctx, rec, target.proposalID)
	}
	version := target.version
	refName := VersionTag(version)
	gravelerRef := refName
	if target.latest {
		if rec.LatestVersion <= 0 {
			return nil, fmt.Errorf("%w: dataset %q has no published version", ErrRefNotFound, name)
		}
		version = rec.LatestVersion
		refName = RefNameLatest
		// "latest" is the dataset branch (§6.1): a data diff against latest sees the
		// branch head, which is what a reader of the dataset sees.
		gravelerRef = DefaultBranch
	}
	return s.resolveVersionRef(ctx, rec, version, refName, gravelerRef)
}

// resolveVersionRef loads the VersionRecord of version and the definition it carries
// (§5.3), and reports ErrRefNotFound when the dataset has no such version.
func (s *Service) resolveVersionRef(ctx context.Context, rec *DatasetRecord, version int, refName, gravelerRef string) (*diffRef, error) {
	versionRec, err := s.store.GetVersion(ctx, rec.Name, version)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, fmt.Errorf("%w: dataset %q version %d", ErrRefNotFound, rec.Name, version)
	}
	if err != nil {
		return nil, err
	}
	def, err := s.versionDefinition(ctx, versionRec)
	if err != nil {
		return nil, err
	}
	return &diffRef{
		Ref:         refName,
		Version:     version,
		GravelerRef: gravelerRef,
		Definition:  def,
	}, nil
}

// resolveProposalRef loads a proposal record and resolves it to its preview branch
// (§5.6): the hidden branch the proposal's definition was materialised on, which is what
// makes a data diff of a proposal a plain ref diff against latest.
func (s *Service) resolveProposalRef(ctx context.Context, rec *DatasetRecord, id string) (*diffRef, error) {
	proposal, err := s.store.GetProposal(ctx, rec.Name, id)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, fmt.Errorf("%w: dataset %q has no proposal %q", ErrRefNotFound, rec.Name, id)
	}
	if err != nil {
		return nil, err
	}
	branch := proposal.PreviewBranch
	if branch == "" {
		branch = ProposalPreviewBranch(proposal.ID)
	}
	def := cloneDefinition(proposal.Definition)
	return &diffRef{
		Ref:         ProposalRefPrefix + proposal.ID,
		ProposalID:  proposal.ID,
		GravelerRef: branch,
		Definition:  &def,
	}, nil
}

// versionDefinition returns the resolved definition of a version: from the record when the
// definition is inline, and from the version's synthetic _dataset/definition.json entry
// when the record carries a DefinitionRef because the definition was too large to inline
// (§5.3). The definition document written by materialisation is self-describing (§6.4), so
// reading it back is exactly the recovery path the reconciler uses (§12.3).
func (s *Service) versionDefinition(ctx context.Context, rec *VersionRecord) (*Definition, error) {
	if rec.HasInlineDefinition() {
		def := cloneDefinition(rec.Definition)
		return &def, nil
	}
	ref := rec.DefinitionRef
	path := ref.Path
	if path == "" {
		path = DefinitionPath
	}
	doc, err := s.readDefinitionDocument(ctx, rec.Dataset, ref.CommitID, path)
	if err != nil {
		return nil, err
	}
	def := cloneDefinition(doc.Definition)
	return &def, nil
}

// readDefinitionDocument reads and decodes one _dataset/definition.json object of the
// dataset repository at commitID/path. The entry keeps a relative address into the
// dataset's own namespace (§6.4), so the read goes through the dataset repository record's
// storage namespace and address type.
func (s *Service) readDefinitionDocument(ctx context.Context, dataset, commitID, path string) (*definitionDocument, error) {
	repo, err := s.catalog.GetRepository(ctx, dataset)
	if err != nil {
		return nil, fmt.Errorf("datasets: repository %q: %w", dataset, err)
	}
	entry, err := s.catalog.GetEntry(ctx, dataset, commitID, path, catalog.GetEntryParams{})
	if err != nil {
		return nil, fmt.Errorf("datasets: definition of %q at %s: %w", dataset, commitID, err)
	}
	reader, err := s.catalog.BlockAdapter.Get(ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       entry.PhysicalAddress,
		IdentifierType:   entry.AddressType.ToIdentifierType(),
	})
	if err != nil {
		return nil, fmt.Errorf("datasets: read definition of %q at %s: %w", dataset, commitID, err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("datasets: read definition of %q at %s: %w", dataset, commitID, err)
	}
	var doc definitionDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("datasets: decode definition of %q at %s: %w", dataset, commitID, err)
	}
	return &doc, nil
}

// Diff kinds of §10.3, as accepted by Service.Diff.
const (
	DiffTypeDefinition = "definition"
	DiffTypeData       = "data"
)

// DiffRequest is the input of Service.Diff: GET
// /datasets/{ds}/refs/{leftRef}/diff/{rightRef}?type=definition|data (§10.3).
type DiffRequest struct {
	Dataset  string
	LeftRef  string
	RightRef string
	// Type is "" (definition), DiffTypeDefinition or DiffTypeData.
	Type string
	// Prefix, After and Amount paginate a data diff; they are ignored by a definition
	// diff, which is small (at most datasets.max_items items) and returned whole.
	Prefix string
	After  string
	Amount int
}

// DiffResult is the answer of Service.Diff: exactly one of Definition and Data is set,
// matching Type ("definition" or "data").
type DiffResult struct {
	Type       string
	Definition *DefinitionDiff
	Data       *DataDiff
}

// Diff computes the definition or data diff of two refs of one dataset (§10.3).
//
// Reading a dataset is gated by ActionReadDataset on the dataset itself (§9.1), not on its
// sources: a consumer with a dataset grant can diff it without access to the repositories
// it was curated from. Both sides are resolved through the same resolver, so `vN`,
// `latest` and `proposal-<id>` behave identically on either side.
func (s *Service) Diff(ctx context.Context, caller Caller, req DiffRequest) (*DiffResult, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionReadDataset, req.Dataset); err != nil {
		return nil, err
	}

	kind := strings.ToLower(strings.TrimSpace(req.Type))
	switch kind {
	case "":
		kind = DiffTypeDefinition
	case DiffTypeDefinition, DiffTypeData:
	default:
		return nil, &ValidationError{Problems: []Problem{{
			Path:    "type",
			Message: fmt.Sprintf("must be %q or %q", DiffTypeDefinition, DiffTypeData),
		}}}
	}

	left, err := s.resolveRef(ctx, req.Dataset, "left_ref", req.LeftRef)
	if err != nil {
		return nil, err
	}
	right, err := s.resolveRef(ctx, req.Dataset, "right_ref", req.RightRef)
	if err != nil {
		return nil, err
	}

	if kind == DiffTypeDefinition {
		return &DiffResult{
			Type:       kind,
			Definition: DiffDefinitions(left.Definition, right.Definition),
		}, nil
	}

	data, err := (&Differ{Catalog: s.catalog}).Diff(ctx, DataDiffRequest{
		Dataset: req.Dataset,
		Left:    left.GravelerRef,
		Right:   right.GravelerRef,
		Prefix:  req.Prefix,
		After:   req.After,
		Amount:  req.Amount,
	})
	if err != nil {
		return nil, err
	}
	return &DiffResult{Type: kind, Data: data}, nil
}
