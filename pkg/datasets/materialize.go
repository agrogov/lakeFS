// Package datasets implements the lakeFS Datasets control plane described in
// DATASETS_DESIGN.md.
//
// materialize.go implements §6.2 steps 6-10, §6.3 and §6.4: turning a resolved
// definition into one version of the dataset repository — the per-item source
// iterators, their address conversion to full identifiers, the k-way merge the
// metarange writer needs, the synthetic _dataset/definition.json entry, and the
// write-metarange / commit / tag sequence that makes a version.
//
// Nothing here talks HTTP and nothing writes KV: the service owns validation,
// authorisation, pins, the publish lock of §5.4 and the VersionRecord (§6.2 steps
// 1-5 and 11-12). This is the primitive the service calls once it holds the lock.
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS
// documentation only.
package datasets

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/md5" //nolint:gosec // lakeFS object ETags are MD5 hashes (see block/mem, block/local)
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// CommitMetadataVersionKey, CommitMetadataProposalKey and CommitMetadataPublisherKey
	// are the commit metadata of a dataset version (§6.2 step 9, §12.4). They make
	// history greppable and are how the reconciler recognises a publish it already
	// performed (§12.3).
	CommitMetadataVersionKey   = "dataset.version"
	CommitMetadataProposalKey  = "dataset.proposal"
	CommitMetadataPublisherKey = "dataset.publisher"

	// The dataset-repository key of the synthetic per-version definition entry is
	// DefinitionPath (store.go): it lives under the reserved ReservedTargetPrefix, so
	// a user target can never collide with it (§5.2) and the data diff filters it out
	// (§10.3). Materialisation feeds exactly that key into the merge, never a second
	// copy of the literal, so the entry and the DefinitionRef of a VersionRecord
	// (§5.3) always name the same path.

	// definitionObjectPrefix is where the definition objects live inside the dataset
	// repository's own storage namespace (§6.4). It is deliberately *not* the same
	// string as DefinitionPath: that is the logical key inside the metarange, this is
	// the physical location of the bytes. The _lakefs_ prefix keeps bookkeeping
	// objects out of the namespace range the service may later shard.
	definitionObjectPrefix = "_lakefs_dataset"

	// DefinitionContentType is the content type of the uploaded definition object.
	DefinitionContentType = "application/json"

	// DefaultBranch is the dataset repository branch versions are committed on
	// (§6.1, §7.1).
	DefaultBranch = "latest"
)

// Merge and publish errors. All of them fail the publish; the API maps ErrNoChanges
// to 409 (§6.2 step 8, §10.4) and the rest to 500 unless they wrap a source error.
var (
	// ErrDuplicateKey reports two sources producing the same dataset key, or a source
	// whose entries are not strictly increasing. Targets are validated not to overlap
	// (§5.2 rule 3), so this can only be a bug or a corrupted source, and it fails the
	// publish instead of silently dropping an entry (§6.4).
	ErrDuplicateKey = errors.New("duplicate or out-of-order key in materialised dataset")

	// ErrNoChanges reports a publish that would reproduce the previous version
	// (§6.2 step 8).
	ErrNoChanges = errors.New("dataset version has no changes")

	// ErrSourceKeyNotFound reports an object item whose source key does not exist at
	// the pinned commit. §5.2 rule 5 checks this in the service too; materialisation
	// cannot invent the entry, so it refuses rather than publish a version that is
	// missing a target the definition promises.
	ErrSourceKeyNotFound = errors.New("source object not found at the committed ref")

	// ErrUnsupportedItemType reports a data item type that materialisation cannot
	// map (table and namespace items are reserved, §1.2).
	ErrUnsupportedItemType = errors.New("unsupported data item type")

	// ErrInvalidPublishRequest reports a request that the service should never send:
	// a non-positive version, an unresolved ref, or a missing source descriptor. The
	// service validates definitions before it calls Publish, so this is a programming
	// error rather than a user error.
	ErrInvalidPublishRequest = errors.New("invalid publish request")

	// ErrInvalidSourceEntry reports a source entry that cannot be materialised: a
	// listing record without a value, or an address type this build does not know.
	// It fails the publish, because skipping the entry would publish a version that is
	// silently missing data.
	ErrInvalidSourceEntry = errors.New("invalid source entry")

	// ErrUnknownAddressType reports an entry address type outside the three of §6.3.
	ErrUnknownAddressType = errors.New("unknown entry address type")
)

// Materializer is the publish primitive: it turns one resolved definition into one
// version of the dataset repository. Its zero options are usable.
type Materializer struct {
	// Catalog is the lakeFS catalog. It also carries the block adapter (address
	// resolution and the definition upload) and the graveler store (§4.6).
	Catalog *catalog.Catalog

	// BatchSize is the Store.List batch size used to enumerate sources; 0 leaves the
	// graveler default (§6.5).
	BatchSize int

	// ValidateOptions tunes the definition validation Publish performs before it opens
	// any source (§6.2 step 1). Its Metadata hook is how §5.2 rule 6 / §5.5 is enforced
	// on publish: the service builds it with Config.ValidateOptions(keys), so a
	// definition that lacks a required metadata key fails the publish with a
	// *ValidationError instead of becoming a version.
	ValidateOptions ValidateOptions
}

// PublishRequest is everything one publish needs. The definition must already be
// normalised and validated (§5.2) and carry only `type: commit` refs — that is,
// it is the output of Resolver.Resolve (§6.2 step 2).
type PublishRequest struct {
	// Dataset is the dataset name, which is also the dataset repository name (§4.2).
	Dataset string

	// Branch is the dataset repository branch to commit on. Empty means DefaultBranch.
	Branch string

	// DefinitionScope, when set, places the synthetic definition object of §6.4 in a
	// namespace of its own inside the dataset storage namespace:
	// _lakefs_dataset/<scope>/v<N>/definition.json. A proposal preview sets it to its
	// preview branch, so materialising a preview can neither overwrite nor be
	// overwritten by the definition object of the version it would produce. Empty means
	// the version namespace of §6.4.
	DefinitionScope string

	// Version is the version number this publish creates, N+1 in §5.4.
	Version int

	// Definition is the resolved definition that becomes the version.
	Definition Definition

	// Message, Publisher and ProposalID become the commit message, committer and the
	// dataset.* commit metadata (§6.2 step 9, §12.4). ProposalID is empty for a
	// direct publish.
	Message    string
	Publisher  string
	ProposalID string

	// Previous describes the version this publish supersedes, for the no-op check of
	// §6.2 step 8. Leave it zero when publishing the first version.
	Previous PreviousVersion
}

// PreviousVersion is the version a publish supersedes. Callers that cannot supply
// the previous definition (for example a reconciler resuming a crashed publish,
// §12.3) may supply only the metarange ID.
type PreviousVersion struct {
	// Number is the previous version number; 0 means "no previous version".
	Number int

	// MetaRangeID is the previous version's metarange, compared after the new one is
	// written (§6.2 step 8).
	MetaRangeID graveler.MetaRangeID

	// Definition is the previous version's resolved definition, compared before
	// anything is written.
	Definition *Definition
}

// PublishResult is what the service needs to write the VersionRecord (§5.3).
type PublishResult struct {
	// Version is the version this publish created.
	Version int

	// Tag is the dataset repository tag holding the version's commit ("v<N>", §6.1).
	Tag string

	// CommitID and MetaRangeID identify the version inside the dataset repository.
	CommitID    graveler.CommitID
	MetaRangeID graveler.MetaRangeID

	// Definition is the normalised definition that was materialised. It is the value
	// the service stores in the VersionRecord and returns from a version read.
	Definition Definition

	// Stats is the store's ItemStats shape (§5.3 VersionRecord.Stats): totals plus one
	// ItemStat per data item, in definition (target) order. Publishing it through the
	// record builder below is what keeps the primitive and the KV record in agreement.
	Stats ItemStats

	// Objects and Bytes count the materialised data entries, excluding the synthetic
	// _dataset/ definition entry. They equal Stats.Objects and Stats.Bytes and feed the
	// sync/async decision of §6.5.
	Objects int64
	Bytes   int64
}

// VersionRecord builds the immutable store record of this publish (§5.3), so the
// service can hand the result to Store.PutVersion without re-deriving the identity
// fields. The record uses the store's VersionRecord type and its field layout: commit
// and metarange IDs as strings, the resolved definition and the ItemStats built while
// materialising. Writing the record and advancing the DatasetRecord remain the
// service's job (§6.2 steps 11-12); this builder only removes the risk of the two
// layers disagreeing about the record shape.
func (r *PublishResult) VersionRecord(req PublishRequest, publishedAt time.Time) *VersionRecord {
	return &VersionRecord{
		Dataset:     req.Dataset,
		Version:     r.Version,
		Definition:  r.Definition,
		Message:     req.Message,
		Publisher:   req.Publisher,
		PublishedAt: publishedAt,
		CommitID:    r.CommitID.String(),
		MetaRangeID: r.MetaRangeID.String(),
		ProposalID:  req.ProposalID,
		Stats:       r.Stats,
	}
}

// PreviousVersionFromRecord turns the store's VersionRecord of the version a publish
// supersedes into the PreviousVersion the no-op check of §6.2 step 8 needs. It exists
// so a caller that already loaded the record (the service, or the reconciler of §12.3)
// does not rebuild the version-number and metarange-ID conversions by hand. A record
// whose definition lives behind DefinitionRef (§5.3 large definitions) yields a
// PreviousVersion with no Definition: the metarange comparison still applies.
func PreviousVersionFromRecord(rec *VersionRecord) PreviousVersion {
	if rec == nil || rec.Version <= 0 {
		return PreviousVersion{}
	}
	previous := PreviousVersion{
		Number:      rec.Version,
		MetaRangeID: graveler.MetaRangeID(rec.MetaRangeID),
	}
	if rec.HasInlineDefinition() {
		def := rec.Definition
		previous.Definition = &def
	}
	return previous
}

// VersionTag returns the dataset repository tag name of a version (§6.1, §7.1).
func VersionTag(version int) string {
	return "v" + strconv.Itoa(version)
}

// definitionObjectKey is the physical key of a version's definition object inside the
// dataset namespace (§6.4). It is version-scoped on purpose: entries store *relative*
// addresses into that shared namespace, so a fixed path would make every older
// version's definition.json point at the newest document, and a version would stop
// being self-describing for disaster recovery (§5.4, §12.3). scope adds one level for a
// caller outside the version namespace (a proposal preview, see
// PublishRequest.DefinitionScope); an empty scope is the version namespace itself.
func definitionObjectKey(scope string, version int) string {
	if scope == "" {
		return fmt.Sprintf("%s/v%d/definition.json", definitionObjectPrefix, version)
	}
	return fmt.Sprintf("%s/%s/v%d/definition.json", definitionObjectPrefix, scope, version)
}

// definitionDocument is the self-describing payload of the synthetic
// _dataset/definition.json entry (§6.4): the resolved definition plus who published
// which version, which is what lets the reconciler rebuild a lost VersionRecord from
// the dataset repository alone (§5.4, §12.3). It carries no timestamp, so publishing
// the same definition twice produces the same bytes and the metarange IDs of two
// retries of the same publish match (§6.2).
type definitionDocument struct {
	Dataset    string     `json:"dataset"`
	Version    int        `json:"version"`
	Definition Definition `json:"definition"`
	Publisher  string     `json:"publisher"`
	Message    string     `json:"message,omitempty"`
}

// Publish materialises req.Definition into a new version of the dataset repository
// (§6.2 steps 6-10):
//
//  1. refuse a publish whose data cannot differ from the previous version's
//     (ErrNoChanges, §6.2 step 8);
//  2. open the per-item source iterators of §6.3 and write the synthetic
//     _dataset/definition.json entry of §6.4;
//  3. merge them into one key-sorted iterator and write the metarange with
//     graveler.WithForce(true), because a dataset repository is read-only (§6.1);
//  4. compare the fresh metarange with the previous version's (ErrNoChanges again,
//     §6.2 step 8);
//  5. commit on the dataset branch and create tag v<N> with force (§6.2 steps 9-10).
//
// The commit and the tag are the version's source of truth (§5.4); the caller writes
// the VersionRecord and the DatasetRecord afterwards. A tag that already points at the
// commit this call produced is accepted instead of failing, but a retry that lands on a
// different commit for the same version is a conflict: deciding whether an interrupted
// publish is resumed or rolled back belongs to the reconciler (§12.3), not here.
func (m *Materializer) Publish(ctx context.Context, req PublishRequest) (*PublishResult, error) {
	branch := req.Branch
	if branch == "" {
		branch = DefaultBranch
	}
	result, err := m.materialize(ctx, req, branch)
	if err != nil {
		return nil, err
	}

	// §6.2 step 10: the tag is what turns the commit into a version ref (§7.1). Only a
	// version is tagged; MaterializePreview is the untagged form used by a proposal.
	tag := VersionTag(req.Version)
	if err := m.tag(ctx, req.Dataset, tag, result.CommitID); err != nil {
		return nil, err
	}
	result.Tag = tag
	return result, nil
}

// MaterializePreview materialises req on branch without creating a version tag (§5.6): a
// proposal preview is a branch a reviewer can browse (`lakefs://<ds>/_proposal-<id>/...`),
// not a version. Everything else is the publish path of §6.2 steps 1-9, so a preview can
// never show data a merge would not produce, and there is no second materialisation code
// path to keep in step with this one.
//
// The metarange is a complete snapshot of the definition, so committing it on a branch
// whose head predates the current `latest` still serves exactly the definition's data: only
// the commit's ancestry differs, and nothing diffs ancestry. The caller creates the branch
// first, hidden and forced (a dataset repository is read-only, §6.1).
func (m *Materializer) MaterializePreview(ctx context.Context, req PublishRequest, branch string) (*PublishResult, error) {
	if branch == "" {
		return nil, fmt.Errorf("%w: preview branch must not be empty", ErrInvalidPublishRequest)
	}
	return m.materialize(ctx, req, branch)
}

// materialize performs §6.2 steps 1-9 on branch: validate, write the definition object and
// the data metarange, and commit it. It is the shared body of Publish, which then tags the
// commit as a version, and of MaterializePreview, which leaves it as a branch head.
func (m *Materializer) materialize(ctx context.Context, req PublishRequest, branch string) (*PublishResult, error) {
	if req.Version <= 0 {
		return nil, fmt.Errorf("%w: version must be positive, got %d", ErrInvalidPublishRequest, req.Version)
	}

	// §6.2 step 1, the half that does not need the catalog: normalise and validate the
	// document, including the metadata hook (§5.2 rule 6, §5.5). The definition is
	// cloned first because Normalise edits it in place and the caller keeps its own
	// document. Validating here means a required metadata key is enforced on every
	// publish, not only on create, and before anything is written.
	def := cloneDefinition(req.Definition)
	if err := Validate(&def, m.ValidateOptions); err != nil {
		return nil, fmt.Errorf("datasets: %s version %d: %w", req.Dataset, req.Version, err)
	}
	req.Definition = def

	dsRepo, err := m.Catalog.Store.GetRepository(ctx, graveler.RepositoryID(req.Dataset))
	if err != nil {
		return nil, fmt.Errorf("datasets: dataset repository %q: %w", req.Dataset, err)
	}

	// §6.2 step 8, first half: a publish whose resolved definition is identical to the
	// previous version's cannot change any data entry, so it is not a version.
	//
	// The comparison is on the definition rather than on the metarange ID, because the
	// two IDs can never match for a re-publish: the synthetic _dataset/definition.json
	// entry of §6.4 carries the version number, so a v2 document differs from v1's even
	// when the data does not. Comparing definitions is the same predicate as "identical
	// data metarange", and it is also what makes a repeat with a different message a
	// no-op instead of a version that only changed its commit message. Rejecting here,
	// before anything is written, keeps the failed publish from leaving a definition
	// object and a metarange behind.
	if req.Previous.Number > 0 && req.Previous.Definition != nil &&
		reflect.DeepEqual(*req.Previous.Definition, req.Definition) {
		return nil, fmt.Errorf("datasets: %s version %d: %w", req.Dataset, req.Version, ErrNoChanges)
	}

	// §6.2 step 6: open the per-item streams first, so a bad source fails the publish
	// before it writes anything, then add the synthetic definition entry of §6.4.
	items, err := m.openItems(ctx, req.Definition)
	if err != nil {
		return nil, err
	}
	definitionStream, err := m.putDefinition(ctx, dsRepo, req)
	if err != nil {
		closeItemIterators(items)
		return nil, err
	}

	streams := make([]graveler.ValueIterator, 0, len(items)+1)
	for _, item := range items {
		streams = append(streams, item)
	}
	streams = append(streams, definitionStream)
	merged := newMergeIterator(streams)
	defer merged.Close()

	// §6.2 step 7.
	metaRangeID, err := m.Catalog.Store.WriteMetaRangeByIterator(ctx, dsRepo, merged, graveler.WithForce(true))
	if err != nil {
		return nil, fmt.Errorf("datasets: write metarange for %s version %d: %w", req.Dataset, req.Version, err)
	}

	// §6.2 step 8, second half: the literal "identical metarange" check, for a caller
	// that knows the previous metarange ID but not the previous definition (the
	// reconciler resuming a publish, §12.3). A publish that merely repeats the previous
	// definition was already rejected above.
	if req.Previous.MetaRangeID != "" && req.Previous.MetaRangeID == *metaRangeID {
		return nil, fmt.Errorf("datasets: %s version %d: %w", req.Dataset, req.Version, ErrNoChanges)
	}

	// §6.2 step 9.
	commitID, err := m.commit(ctx, req, branch, *metaRangeID)
	if err != nil {
		return nil, err
	}

	result := &PublishResult{
		Version:     req.Version,
		CommitID:    commitID,
		MetaRangeID: *metaRangeID,
		Definition:  req.Definition,
	}
	result.Stats.Items = make([]ItemStat, 0, len(items))
	for _, item := range items {
		result.Objects += item.objects
		result.Bytes += item.bytes
		result.Stats.Items = append(result.Stats.Items, ItemStat{
			Target:  item.stream.target,
			Objects: item.objects,
			Bytes:   item.bytes,
		})
	}
	result.Stats.Objects = result.Objects
	result.Stats.Bytes = result.Bytes
	return result, nil
}

// commit commits the new metarange on the dataset branch (§6.2 step 9).
func (m *Materializer) commit(ctx context.Context, req PublishRequest, branch string, metaRangeID graveler.MetaRangeID) (graveler.CommitID, error) {
	metadata := catalog.Metadata{
		CommitMetadataVersionKey:   strconv.Itoa(req.Version),
		CommitMetadataPublisherKey: req.Publisher,
	}
	if req.ProposalID != "" {
		metadata[CommitMetadataProposalKey] = req.ProposalID
	}

	mr := metaRangeID.String()
	log, err := m.Catalog.Commit(ctx, req.Dataset, branch, req.Message, req.Publisher, metadata, nil, &mr, false,
		graveler.WithForce(true))
	if err != nil {
		return "", fmt.Errorf("datasets: commit %s version %d: %w", req.Dataset, req.Version, err)
	}
	return graveler.CommitID(log.Reference), nil
}

// tag creates the version tag (§6.2 step 10). A tag that already exists because an
// earlier attempt of the same publish got this far points at this very commit, so it
// is accepted: that is what makes the publish resumable. A tag at another commit is
// a real conflict and is returned as is.
func (m *Materializer) tag(ctx context.Context, dataset, tag string, commitID graveler.CommitID) error {
	_, err := m.Catalog.CreateTag(ctx, dataset, tag, commitID.String(), graveler.WithForce(true))
	if err == nil {
		return nil
	}
	if !errors.Is(err, graveler.ErrTagAlreadyExists) {
		return fmt.Errorf("datasets: create tag %q on %s: %w", tag, dataset, err)
	}
	existing, getErr := m.Catalog.GetTag(ctx, dataset, tag)
	if getErr == nil && existing == commitID.String() {
		return nil
	}
	return fmt.Errorf("datasets: create tag %q on %s: %w", tag, dataset, err)
}

// putDefinition uploads the resolved definition document to the dataset's own storage
// namespace and returns the synthetic _dataset/definition.json stream for the merge
// (§6.4). The entry keeps a *relative* address — the object lives in the dataset
// namespace, unlike the data entries, which point into the source namespaces (§6.3).
func (m *Materializer) putDefinition(ctx context.Context, dsRepo *graveler.RepositoryRecord, req PublishRequest) (*singleRecordIterator, error) {
	doc := definitionDocument{
		Dataset:    req.Dataset,
		Version:    req.Version,
		Definition: req.Definition,
		Publisher:  req.Publisher,
		Message:    req.Message,
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("datasets: marshal definition of %s version %d: %w", req.Dataset, req.Version, err)
	}

	objectKey := definitionObjectKey(req.DefinitionScope, req.Version)
	pointer := block.ObjectPointer{
		StorageID:        dsRepo.StorageID.String(),
		StorageNamespace: dsRepo.StorageNamespace.String(),
		Identifier:       objectKey,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	putResponse, err := m.Catalog.BlockAdapter.Put(ctx, pointer, int64(len(data)), bytes.NewReader(data), block.PutOpts{})
	if err != nil {
		return nil, fmt.Errorf("datasets: upload definition of %s version %d: %w", req.Dataset, req.Version, err)
	}

	// An entry's identity is (size, ETag, metadata, content type), so taking the ETag from
	// the object store keeps the definition entry describing exactly the bytes that were
	// uploaded. Adapters that do not report properties fall back to the MD5 the lakeFS
	// block adapters use for single-part objects.
	lastModified := putResponse.GetMtime()
	etag := ""
	if props, propsErr := m.Catalog.BlockAdapter.GetProperties(ctx, pointer); propsErr == nil {
		etag = props.ETag
		if !props.LastModified.IsZero() {
			lastModified = props.LastModified
		}
	}
	if etag == "" {
		sum := md5.Sum(data) //nolint:gosec
		etag = hex.EncodeToString(sum[:])
	}

	entry := &catalog.Entry{
		Address:      objectKey,
		AddressType:  catalog.Entry_RELATIVE,
		Size:         int64(len(data)),
		ETag:         etag,
		ContentType:  DefinitionContentType,
		LastModified: timestamppb.New(lastModified.UTC()),
	}
	value, err := catalog.EntryToValue(entry)
	if err != nil {
		return nil, fmt.Errorf("datasets: encode definition entry of %s version %d: %w", req.Dataset, req.Version, err)
	}
	return &singleRecordIterator{key: graveler.Key(DefinitionPath), value: value}, nil
}

// openItems opens one source iterator per data item, in definition order (§6.3). On
// error, the iterators that were already opened are closed again.
//
// §6.5 limits how many per-item iterators may be open at once (datasets.merge_fan_in,
// default 64) and prescribes merging in two passes with WriteRange above that. This
// implementation opens all of them for the duration of the merge: the iterators are
// metadata-only, and datasets.max_items bounds the count at 1000 by default. The two-pass
// variant is a later performance step, not a correctness one.
func (m *Materializer) openItems(ctx context.Context, def Definition) ([]*itemIterator, error) {
	items := make([]*itemIterator, 0, len(def.Data))
	for i := range def.Data {
		item, err := m.openItem(ctx, i, &def.Data[i])
		if err != nil {
			closeItemIterators(items)
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

// openItem builds the iterator of a single data item.
func (m *Materializer) openItem(ctx context.Context, index int, item *DataItem) (*itemIterator, error) {
	stream := &itemStream{
		index:  index,
		target: item.Target,
	}
	switch item.Type {
	case ItemTypeObject:
		if item.Object == nil {
			return nil, fmt.Errorf("%w: %s: required for item type %q",
				ErrInvalidPublishRequest, itemPath(index, "object"), item.Type)
		}
		stream.object = true
		stream.sourceKey = item.Object.Path
	case ItemTypePrefix:
		if item.Prefix == nil {
			return nil, fmt.Errorf("%w: %s: required for item type %q",
				ErrInvalidPublishRequest, itemPath(index, "prefix"), item.Type)
		}
		stream.sourceKey = item.Prefix.Path
	default:
		return nil, fmt.Errorf("%s: %w: %q", itemPath(index, "type"), ErrUnsupportedItemType, item.Type)
	}
	if item.Ref.Type != RefTypeCommit {
		return nil, fmt.Errorf("%w: %s: ref type %q is not resolved to a commit",
			ErrInvalidPublishRequest, itemPath(index, "ref.type"), item.Ref.Type)
	}
	stream.commitID = graveler.CommitID(item.Ref.ID)

	repo, err := m.Catalog.Store.GetRepository(ctx, graveler.RepositoryID(item.Repository))
	if err != nil {
		return nil, fmt.Errorf("%s: source repository %q: %w", itemPath(index, "repository"), item.Repository, err)
	}
	stream.repository = repo

	src, err := m.openSource(ctx, stream)
	if err != nil {
		return nil, err
	}
	return &itemIterator{stream: stream, src: src, adapter: m.Catalog.BlockAdapter}, nil
}

// openSource opens the source-key-space iterator of one item: a single-key get for an
// object item, a prefix-bounded Store.List for a prefix item (§6.3).
func (m *Materializer) openSource(ctx context.Context, stream *itemStream) (graveler.ValueIterator, error) {
	if stream.object {
		value, err := m.Catalog.Store.Get(ctx, stream.repository, graveler.Ref(stream.commitID), graveler.Key(stream.sourceKey))
		if err != nil {
			if errors.Is(err, graveler.ErrNotFound) {
				return nil, &SourceError{
					Item:       stream.index,
					Repository: stream.repository.RepositoryID.String(),
					Ref:        SourceRef{Type: RefTypeCommit, ID: stream.commitID.String()},
					Err:        ErrSourceKeyNotFound,
				}
			}
			return nil, fmt.Errorf("datasets: get source object %q at %s/%s: %w",
				stream.sourceKey, stream.repository.RepositoryID, stream.commitID, err)
		}
		return &singleRecordIterator{key: graveler.Key(stream.sourceKey), value: value}, nil
	}

	it, err := m.Catalog.Store.List(ctx, stream.repository, graveler.Ref(stream.commitID), m.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("datasets: list %q at %s/%s: %w",
			stream.sourceKey, stream.repository.RepositoryID, stream.commitID, err)
	}
	if stream.sourceKey != "" {
		// An empty prefix is the whole repository (§5.2), which is where List starts.
		it.SeekGE(graveler.Key(stream.sourceKey))
	}
	return &prefixIterator{it: it, prefix: stream.sourceKey}, nil
}

// closeItemIterators closes the iterators of items that were opened before a failure.
func closeItemIterators(items []*itemIterator) {
	for _, item := range items {
		item.Close()
	}
}

// itemStream is the static description of one data item's contribution to the merge:
// where its keys come from, which repository namespace they live in, and how a source
// key maps into a dataset key (§5.2, §6.3).
type itemStream struct {
	// index is the item's position in the definition, used in error paths.
	index int

	// target is the normalised dataset target of the item. The empty target is the
	// repository root (§5.2).
	target string

	// sourceKey is the exact source key of an object item, or the source prefix of a
	// prefix item (always "/"-terminated after normalisation; empty means the whole
	// source repository).
	sourceKey string

	// object reports whether the item maps exactly one key.
	object bool

	repository *graveler.RepositoryRecord
	commitID   graveler.CommitID
}

// datasetKey maps a source key into the dataset key space (§5.2). For a prefix item it
// replaces the source prefix with the target; for an object item the single source key
// becomes the target. The mapping is monotone in the source key, which is what makes
// each item's stream sorted and the k-way merge of §6.4 valid.
func (s *itemStream) datasetKey(sourceKey string) string {
	if s.object {
		return s.target
	}
	rel := strings.TrimPrefix(sourceKey, s.sourceKey)
	if s.target == RootTarget {
		return rel
	}
	return s.target + "/" + rel
}

// sourceSeekKey maps a dataset key back into the item's source key space, for
// SeekGE. It is conservative: a key outside the item's own target span seeks to the
// start of the span, so the merge — which owns the final ordering — never misses an
// entry. Over-seeking would silently drop data.
func (s *itemStream) sourceSeekKey(datasetKey string) string {
	if s.object {
		return s.sourceKey
	}
	if s.target == RootTarget {
		return datasetKey
	}
	rel, ok := strings.CutPrefix(datasetKey, s.target+"/")
	if !ok {
		return s.sourceKey
	}
	return s.sourceKey + rel
}

// itemIterator maps one item's source entries into the dataset key space and converts
// every entry's address (§6.3). Its output is sorted because the mapping is monotone.
type itemIterator struct {
	stream  *itemStream
	src     graveler.ValueIterator
	adapter block.Adapter

	current *graveler.ValueRecord
	err     error

	// objects and bytes count what this item contributed to the version, for
	// PublishResult and §5.3 ItemStats.
	objects int64
	bytes   int64
}

// Next implements graveler.ValueIterator.
func (it *itemIterator) Next() bool {
	it.current = nil
	if it.err != nil {
		return false
	}
	if !it.src.Next() {
		it.err = it.src.Err()
		return false
	}
	record := it.src.Value()
	if record == nil {
		it.err = fmt.Errorf("%w: %s: source iterator returned no value",
			ErrInvalidSourceEntry, itemPath(it.stream.index, ""))
		return false
	}
	entry, err := catalog.ValueToEntry(record.Value)
	if err != nil {
		it.err = fmt.Errorf("datasets: %s: decode source entry %q: %w", itemPath(it.stream.index, ""), record.Key, err)
		return false
	}
	if entry == nil {
		it.err = fmt.Errorf("%w: %s: source entry %q has no value",
			ErrInvalidSourceEntry, itemPath(it.stream.index, ""), record.Key)
		return false
	}

	// §6.3: only the address changes. ETag, Size, LastModified, ContentType and the user
	// Metadata are carried over untouched, which is what keeps diffs between versions
	// meaningful (EntryToValue's identity is size+ETag+metadata+content type).
	if err := convertEntryAddress(it.adapter, it.stream.repository, entry); err != nil {
		it.err = fmt.Errorf("datasets: %s: %w", itemPath(it.stream.index, ""), err)
		return false
	}
	value, err := catalog.EntryToValue(entry)
	if err != nil {
		it.err = fmt.Errorf("datasets: %s: encode entry %q: %w", itemPath(it.stream.index, ""), record.Key, err)
		return false
	}

	it.current = &graveler.ValueRecord{
		Key:   graveler.Key(it.stream.datasetKey(string(record.Key))),
		Value: value,
	}
	it.objects++
	it.bytes += entry.Size
	return true
}

// SeekGE implements graveler.ValueIterator.
func (it *itemIterator) SeekGE(id graveler.Key) {
	it.current = nil
	it.src.SeekGE(graveler.Key(it.stream.sourceSeekKey(string(id))))
}

// Value implements graveler.ValueIterator.
func (it *itemIterator) Value() *graveler.ValueRecord {
	if it.err != nil {
		return nil
	}
	return it.current
}

// Err implements graveler.ValueIterator.
func (it *itemIterator) Err() error { return it.err }

// Close implements graveler.ValueIterator.
func (it *itemIterator) Close() { it.src.Close() }

var _ graveler.ValueIterator = (*itemIterator)(nil)

// convertEntryAddress rewrites a source entry's address into a full identifier (§6.3).
//
// A full address is kept verbatim: it already names the object store URI and
// resolving it again would double the namespace on adapters whose QualifiedKey.Format
// prefixes their own root (see the local-adapter caveat pinned by Spike S4). Relative
// (and the deprecated by-prefix) addresses are resolved against the *source*
// repository's namespace, which is why the source and the dataset must share a
// storage ID (§5.2 rule 4).
func convertEntryAddress(adapter block.Adapter, repo *graveler.RepositoryRecord, entry *catalog.Entry) error {
	addressType := catalog.AddressType(entry.AddressType)
	switch addressType {
	case catalog.AddressTypeFull:
		return nil
	case catalog.AddressTypeRelative, catalog.AddressTypeByPrefixDeprecated: //nolint:staticcheck // old repositories still hold by-prefix entries
		qk, err := adapter.ResolveNamespace(
			repo.StorageID.String(),
			repo.StorageNamespace.String(),
			entry.Address,
			addressType.ToIdentifierType())
		if err != nil {
			return fmt.Errorf("resolve source address %q in %s: %w", entry.Address, repo.StorageNamespace, err)
		}
		entry.Address = qk.Format()
		entry.AddressType = catalog.Entry_FULL
		return nil
	default:
		return fmt.Errorf("%w %d for %q", ErrUnknownAddressType, entry.AddressType, entry.Address)
	}
}

// singleRecordIterator is a graveler.ValueIterator over one record, used for object
// items and for the synthetic definition entry.
type singleRecordIterator struct {
	key     graveler.Key
	value   *graveler.Value
	yielded bool
	skip    bool
}

// Next implements graveler.ValueIterator.
func (it *singleRecordIterator) Next() bool {
	if it.yielded || it.skip {
		return false
	}
	it.yielded = true
	return true
}

// SeekGE implements graveler.ValueIterator.
func (it *singleRecordIterator) SeekGE(id graveler.Key) {
	it.yielded = false
	it.skip = bytes.Compare(it.key, id) < 0
}

// Value implements graveler.ValueIterator.
func (it *singleRecordIterator) Value() *graveler.ValueRecord {
	if it.skip || !it.yielded {
		return nil
	}
	return &graveler.ValueRecord{Key: it.key, Value: it.value}
}

// Err implements graveler.ValueIterator.
func (it *singleRecordIterator) Err() error { return nil }

// Close implements graveler.ValueIterator.
func (it *singleRecordIterator) Close() {}

var _ graveler.ValueIterator = (*singleRecordIterator)(nil)

// prefixIterator bounds a Store.List iterator to its prefix. Everything the source
// repository holds under the prefix belongs to the item; the first key outside it ends
// the stream (§6.3).
type prefixIterator struct {
	it     graveler.ValueIterator
	prefix string
	done   bool
}

// Next implements graveler.ValueIterator.
func (it *prefixIterator) Next() bool {
	if it.done {
		return false
	}
	if !it.it.Next() {
		it.done = true
		return false
	}
	if !strings.HasPrefix(string(it.it.Value().Key), it.prefix) {
		it.done = true
		return false
	}
	return true
}

// SeekGE implements graveler.ValueIterator.
func (it *prefixIterator) SeekGE(id graveler.Key) {
	it.done = false
	it.it.SeekGE(id)
}

// Value implements graveler.ValueIterator.
func (it *prefixIterator) Value() *graveler.ValueRecord {
	if it.done {
		return nil
	}
	return it.it.Value()
}

// Err implements graveler.ValueIterator.
func (it *prefixIterator) Err() error { return it.it.Err() }

// Close implements graveler.ValueIterator.
func (it *prefixIterator) Close() { it.it.Close() }

var _ graveler.ValueIterator = (*prefixIterator)(nil)

// mergeIterator is the k-way merge of §6.4.
//
// Concatenating the per-item streams in target order is not enough: target "a" (a
// prefix producing "a/x") and target "a-b" (an object producing "a-b") sort as "a",
// "a-b", so concatenation would emit "a/x" before "a-b" even though "a-b" < "a/x"
// ('-' is 0x2d, '/' is 0x2f). A heap over the streams fixes that, and the strict
// key-increase assertion turns an overlap that validation should have caught into a
// failed publish instead of a metarange with a duplicate key.
type mergeIterator struct {
	streams []graveler.ValueIterator
	heap    streamHeap

	started bool
	hasLast bool
	last    graveler.Key
	seekTo  graveler.Key

	current *graveler.ValueRecord
	err     error
}

// newMergeIterator merges the given streams. They are owned by the merge iterator:
// Close closes all of them.
func newMergeIterator(streams []graveler.ValueIterator) *mergeIterator {
	return &mergeIterator{
		streams: streams,
		heap:    make(streamHeap, 0, len(streams)),
	}
}

// Next implements graveler.ValueIterator.
func (m *mergeIterator) Next() bool {
	m.current = nil
	if m.err != nil {
		return false
	}
	if !m.started {
		m.started = true
		m.start()
	}
	for m.err == nil {
		if m.heap.Len() == 0 {
			return false
		}
		top := m.heap[0]
		record := top.Value()
		if record == nil {
			m.err = fmt.Errorf("%w: source stream returned no value", ErrInvalidSourceEntry)
			return false
		}
		// Advance the stream before handing the record out: the record belongs to the
		// stream and is only valid until its next Next.
		key := append(graveler.Key(nil), record.Key...)
		value := record.Value
		if top.Next() {
			heap.Fix(&m.heap, 0)
		} else {
			if err := top.Err(); err != nil {
				m.err = err
				return false
			}
			heap.Pop(&m.heap)
		}

		if m.seekTo != nil && bytes.Compare(key, m.seekTo) < 0 {
			continue
		}
		if m.hasLast && bytes.Compare(key, m.last) <= 0 {
			m.err = fmt.Errorf("%w: key %q follows %q", ErrDuplicateKey, key, m.last)
			return false
		}
		m.last = key
		m.hasLast = true
		m.current = &graveler.ValueRecord{Key: key, Value: value}
		return true
	}
	return false
}

// start primes the heap with the first record of every stream.
func (m *mergeIterator) start() {
	for _, stream := range m.streams {
		if stream.Next() {
			heap.Push(&m.heap, stream)
			continue
		}
		if err := stream.Err(); err != nil {
			m.err = err
			return
		}
	}
}

// SeekGE implements graveler.ValueIterator. The streams are asked to position
// themselves conservatively, and Next filters whatever is still before id, so the
// first value returned is the first key >= id.
func (m *mergeIterator) SeekGE(id graveler.Key) {
	m.current = nil
	m.hasLast = false
	m.last = nil
	m.seekTo = append(graveler.Key(nil), id...)
	m.heap = m.heap[:0]
	m.started = false
	for _, stream := range m.streams {
		stream.SeekGE(id)
	}
}

// Value implements graveler.ValueIterator.
func (m *mergeIterator) Value() *graveler.ValueRecord {
	if m.err != nil {
		return nil
	}
	return m.current
}

// Err implements graveler.ValueIterator.
func (m *mergeIterator) Err() error { return m.err }

// Close implements graveler.ValueIterator.
func (m *mergeIterator) Close() {
	for _, stream := range m.streams {
		stream.Close()
	}
}

var _ graveler.ValueIterator = (*mergeIterator)(nil)

// streamHeap is a min-heap of value iterators ordered by their current key.
type streamHeap []graveler.ValueIterator

func (h streamHeap) Len() int { return len(h) }

func (h streamHeap) Less(i, j int) bool {
	// A stream whose Value is nil is a broken iterator; ordering it first lets Next report
	// the problem instead of panicking inside the heap.
	left, right := h[i].Value(), h[j].Value()
	switch {
	case left == nil:
		return right != nil
	case right == nil:
		return false
	}
	return bytes.Compare(left.Key, right.Key) < 0
}

func (h streamHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *streamHeap) Push(x any) { *h = append(*h, x.(graveler.ValueIterator)) }

func (h *streamHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
