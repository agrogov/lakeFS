package icebergcatalog

// View state on the branch, and the Iceberg view commit (ICEBERG_CATALOG_PLAN.md §6.1 "Layout on
// the branch", §6.2 "Atomic commit"; Apache Iceberg view spec and REST OpenAPI spec).
//
// An Iceberg *view* is a named, versioned SQL query. It is not a table: a view has no snapshots,
// no manifests and no data files. It has a metadata document of its own (the view spec's "View
// Metadata": format-version, view-uuid, location, current-version-id, versions, schemas,
// version-log, properties) and the catalog stores it exactly the way it stores a table's:
//
//	_lakefs_iceberg/
//	  views/<ns1>/<ns2>/<view>/.pointer      view pointer, entry user metadata:
//	                                           iceberg.metadata-location = physical URI of metadata.json
//	                                           iceberg.view-uuid        = view UUID
//	                                           iceberg.format-version   = 1
//	  files/<view-uuid>/<relative path>      the view's metadata.json entries (§6.3): the initial one a
//	                                           create wrote, and every one a replace wrote
//
// The layout deliberately mirrors the tables': a pointer is the one entry that makes a view exist,
// its `iceberg.metadata-location` is the compare-and-swap field of a replace, the metadata
// documents live under `<storage_namespace>/_iceberg/<view-uuid>/metadata/` (location.go), and the
// document a pointer names is staged as its own lakeFS entry so that the committed GC protects it
// and `lakectl diff` and merge see it (files.go, §6.3). Everything that follows from that
// mirroring is also inherited rather than re-implemented:
//
//   - **Staged, not committed.** A create, a replace and a drop write the branch's staging area
//     through catalog.CreateEntry without graveler.WithForce, so branch protection, repository
//     immutability, the installation-wide deny on `_lakefs_iceberg/` and the explicit lakeFS
//     commit all apply unchanged (§6.2 "why stage instead of auto-commit", §14 Q-I2).
//   - **Compare-and-swap.** A replace conditions the pointer write on the base pointer's
//     metadata-location (graveler.WithCondition), so exactly one of two concurrent replaces wins,
//     the loser gets graveler.ErrPreconditionFailed and its metadata.json is left unreferenced for
//     GC - the same orphan a lost table commit leaves (§6.2 step 6).
//   - **Any-ref reads.** GetView resolves the pointer on a branch, a tag or a commit, staging area
//     first, so a view created through the catalog is readable before its lakeFS commit and a view
//     captured by a tag stays readable after the branch moves on (§1.1 goal 1).
//   - **Reversible drops.** A drop removes the pointer and the staged metadata entries, and no
//     object: reverting the lakeFS commit that carries the drop brings the view back, and a
//     branch or tag whose pointer still names the document keeps it readable (§14 Q-I8).
//
// What is *not* shared is the metadata document itself. Views use iceberg-go's `view` package
// (view.Metadata, view.MetadataBuilder, view.Updates, view.Requirements), because the view spec's
// update and requirement language is not the table one: the only requirement is
// `assert-view-uuid`, and the eight update actions are assign-uuid, upgrade-format-version,
// add-schema, set-location, set-properties, remove-properties, add-view-version and
// set-current-view-version. A table commit's apply path (table.UpdateTableMetadata) cannot read
// them, so the replace path here is its own - and, because views are metadata-only, it has no
// file-registration step for data files: one metadata.json per commit is the whole write.
//
// set-location is served for views, unlike for tables. The table restriction (§6.2 step 3) exists
// because a table's location roots the staged data files and the GC window; a view has neither, so
// the update is applied - but the new location is still validated against the catalog's table area
// (location.go) before anything is written, because the catalog writes and stages the metadata
// file there with its own credentials.
//
// Rename is not served, deliberately: `POST /v1/{prefix}/views/rename` stays a route the catalog
// refuses to advertise (§1.2 non-goals keep it out of GET /v1/config.endpoints), and a client that
// calls it is answered 404 by the router like any other unserved path.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, listViews /
// createView / loadView / replaceView / dropView / viewExists and components.schemas.ViewMetadata,
// CreateViewRequest, LoadViewResult, CommitViewRequest) and the Apache Iceberg view spec
// (https://iceberg.apache.org/view-spec/) only. No code from lakeFS Enterprise or from upstream
// lakeFS >= 1.87 (BSL) was used; the view metadata model itself is the pinned Apache-2.0
// github.com/apache/iceberg-go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	// ViewsDirectory is the subdirectory of the reserved prefix that holds one .pointer entry per
	// view:
	//
	//	_lakefs_iceberg/views/<ns1>/<ns2>/<view>/.pointer
	//
	// It is the views' counterpart of TablesDirectory (store.go). It is declared here rather than
	// in the leaf package iceberglayout because nothing outside this package builds a view key:
	// pkg/datasets materialises tables and namespaces only, and a view is not a datasets item.
	ViewsDirectory = "views"

	// ViewPointerName is the file name of a view pointer. A pointer is a pointer whatever it
	// points at, so the name is the table one: a reader looking for the entry that makes a named
	// object exist looks for the same file name in both directories.
	ViewPointerName = TablePointerName

	// ViewUUIDKey is the view pointer's user-metadata key that holds the view UUID, i.e. the
	// value the view metadata document carries as `view-uuid` and the one the Iceberg REST
	// spec's `assert-view-uuid` requirement compares. It is a key of its own and not
	// TableUUIDKey: a view is not a table, and a pointer that mixed them up would be unreadable
	// in a way that looks like corruption instead of a bug.
	ViewUUIDKey = "iceberg.view-uuid"

	// SupportedViewFormatVersion is the only Iceberg view format version the catalog serves. The
	// view spec defines exactly one (1); a document that names another is refused rather than
	// served by code that does not implement it.
	SupportedViewFormatVersion = 1
)

// NoSuchViewException is returned when the view pointer does not exist on the requested ref. The
// HTTP layer maps it to 404 NoSuchViewException.
type NoSuchViewException struct {
	Repo      string
	Ref       string
	Namespace Namespace
	View      string
}

func (e *NoSuchViewException) Error() string {
	return fmt.Sprintf("no such view %q in namespace %q of %s@%s",
		e.View, e.Namespace.Dotted(), e.Repo, e.Ref)
}

// ViewPointer is the decoded user metadata of a _lakefs_iceberg/views/<ns>/<view>/.pointer entry.
// It is the view's TablePointer: the location of the current metadata document, the view UUID and
// the view format version.
type ViewPointer struct {
	// MetadataLocation is the physical URI of the current view metadata.json. It is the
	// compare-and-swap field of a replace.
	MetadataLocation string

	// ViewUUID is the view UUID that the metadata document also carries as `view-uuid`.
	ViewUUID string

	// FormatVersion is the Iceberg view format version, 1.
	FormatVersion int
}

// View is a resolved view: its pointer and the metadata document the pointer references.
type View struct {
	// Address is the catalog address the view was read from, including the ref.
	Address Address

	// Pointer is the decoded pointer entry.
	Pointer ViewPointer

	// Metadata is the view metadata document parsed by iceberg-go's view package.
	Metadata view.Metadata

	// MetadataJSON is the exact bytes of the document, so a load response does not have to
	// re-marshal the parsed model.
	MetadataJSON []byte
}

// DecodeViewPointer decodes the user metadata of a view pointer entry. It validates every field
// EncodeViewPointer writes, so a pointer this catalog could not have written is reported as
// ErrInvalidPointer instead of being half-used.
func DecodeViewPointer(metadata map[string]string) (ViewPointer, error) {
	location := metadata[MetadataLocationKey]
	if location == "" {
		return ViewPointer{}, fmt.Errorf("%w: user metadata %q is missing or empty",
			ErrInvalidPointer, MetadataLocationKey)
	}
	viewUUID := metadata[ViewUUIDKey]
	if viewUUID == "" {
		return ViewPointer{}, fmt.Errorf("%w: user metadata %q is missing or empty",
			ErrInvalidPointer, ViewUUIDKey)
	}
	rawVersion := metadata[FormatVersionKey]
	if rawVersion == "" {
		return ViewPointer{}, fmt.Errorf("%w: user metadata %q is missing or empty",
			ErrInvalidPointer, FormatVersionKey)
	}
	formatVersion, err := strconv.Atoi(rawVersion)
	if err != nil {
		return ViewPointer{}, fmt.Errorf("%w: %q is %q, not an integer: %w",
			ErrInvalidPointer, FormatVersionKey, rawVersion, err)
	}
	if formatVersion != SupportedViewFormatVersion {
		return ViewPointer{}, fmt.Errorf("%w: %q is %d, and the catalog serves view format version %d",
			ErrInvalidPointer, FormatVersionKey, formatVersion, SupportedViewFormatVersion)
	}
	return ViewPointer{
		MetadataLocation: location,
		ViewUUID:         viewUUID,
		FormatVersion:    formatVersion,
	}, nil
}

// EncodeViewPointer turns a pointer into the entry user metadata of a view pointer. It validates
// the same fields DecodeViewPointer validates, so a pointer this catalog would refuse to read can
// never be written.
func EncodeViewPointer(pointer ViewPointer) (map[string]string, error) {
	if pointer.MetadataLocation == "" {
		return nil, fmt.Errorf("%w: %q must not be empty", ErrInvalidPointer, MetadataLocationKey)
	}
	if pointer.ViewUUID == "" {
		return nil, fmt.Errorf("%w: %q must not be empty", ErrInvalidPointer, ViewUUIDKey)
	}
	if pointer.FormatVersion != SupportedViewFormatVersion {
		return nil, fmt.Errorf("%w: %q is %d, and the catalog serves view format version %d",
			ErrInvalidPointer, FormatVersionKey, pointer.FormatVersion, SupportedViewFormatVersion)
	}
	return map[string]string{
		MetadataLocationKey: pointer.MetadataLocation,
		ViewUUIDKey:         pointer.ViewUUID,
		FormatVersionKey:    strconv.Itoa(pointer.FormatVersion),
	}, nil
}

// GetView reads the pointer of addr on addr.Ref, decodes it and loads the metadata document it
// names. A missing pointer is NoSuchViewException; a pointer or a document this catalog cannot
// parse is ErrInvalidPointer.
//
// Staged data wins: catalog.GetEntry resolves the branch's staging area before the committed
// metadata range, so a view created or replaced but not yet committed by lakeFS is already
// readable, while a tag or a commit has no staging area and answers the immutable state it
// captured.
func (s *Store) GetView(ctx context.Context, addr Address) (*View, error) {
	pointerPath, err := viewPointerPath(addr)
	if err != nil {
		return nil, err
	}
	entry, err := s.getEntry(ctx, addr, pointerPath)
	if err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			return nil, &NoSuchViewException{
				Repo:      addr.Repo,
				Ref:       addr.Ref,
				Namespace: addr.Namespace,
				View:      addr.View,
			}
		}
		return nil, err
	}
	pointer, err := DecodeViewPointer(entry.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode view pointer %q: %w", pointerPath, err)
	}
	metadata, document, err := s.loadViewMetadata(ctx, addr.Repo, pointer.MetadataLocation)
	if err != nil {
		return nil, err
	}
	return &View{
		Address:      addr,
		Pointer:      pointer,
		Metadata:     metadata,
		MetadataJSON: document,
	}, nil
}

// CreateView stages a new view pointer on addr.Ref with assert-create semantics: a view whose name
// is already taken is ErrAlreadyExists, never silently replaced. It stages the pointer and nothing
// else; the create path of the HTTP layer uses CreateViewWithMetadata, which registers the initial
// metadata document first, for the same reason the table create does (§6.3: a pointer names its
// document from user metadata, so nothing else would make the document a lakeFS entry).
func (s *Store) CreateView(ctx context.Context, addr Address, pointer ViewPointer) error {
	pointerPath, err := viewPointerPath(addr)
	if err != nil {
		return err
	}
	metadata, err := EncodeViewPointer(pointer)
	if err != nil {
		return err
	}
	err = s.stage(ctx, addr, pointerPath, metadata, func(current *catalog.Entry) error {
		if current != nil {
			return fmt.Errorf("%w: view %q in namespace %q already exists on %s@%s",
				ErrAlreadyExists, addr.View, addr.Namespace.Dotted(), addr.Repo, addr.Ref)
		}
		return nil
	})
	return asAlreadyExists(err, "view", addr.Namespace.Dotted()+"."+addr.View, addr)
}

// CreateViewWithMetadata stages a new view on addr.Ref: the lakeFS entry of the initial metadata
// document (StageViewMetadata), and then the view pointer (CreateView). location is the view's base
// location, i.e. the `location` of the document the pointer names: StageViewMetadata refuses a
// document outside it, exactly as the commit path does.
//
// The order is the table create's (store.go CreateTableWithMetadata): the file entry is staged
// first, so a failure of file registration never leaves a pointer naming an unregistered document,
// and a duplicate create still loses at the pointer's compare-and-swap. The loser's entries are
// keyed by its own fresh view UUID, so they cannot shadow the winner's, and the uncommitted-GC
// grace protects them until they are collected (gc.go).
func (s *Store) CreateViewWithMetadata(ctx context.Context, addr Address, pointer ViewPointer, location string, metadataSize int64) error {
	viewUUID, err := uuid.Parse(pointer.ViewUUID)
	if err != nil {
		return fmt.Errorf("%w: %q is not a view uuid: %w", ErrInvalidPointer, pointer.ViewUUID, err)
	}
	if err := s.StageViewMetadata(ctx, addr, viewUUID, location, []TableMetadataFile{
		{Location: pointer.MetadataLocation, Size: metadataSize},
	}); err != nil {
		return err
	}
	return s.CreateView(ctx, addr, pointer)
}

// UpdateViewPointer swaps the view pointer from base to next on addr.Ref: the check-and-put of a
// replace.
//
// The condition compares the current value's iceberg.metadata-location with base's - the field
// every replace changes and the only one that has to - and a mismatch, including a pointer that is
// gone, is graveler.ErrPreconditionFailed. The caller maps it to 409 CommitFailedException and the
// new metadata document becomes an unreferenced orphan that GC collects.
func (s *Store) UpdateViewPointer(ctx context.Context, addr Address, base, next ViewPointer) error {
	pointerPath, err := viewPointerPath(addr)
	if err != nil {
		return err
	}
	if base.MetadataLocation == "" {
		return fmt.Errorf("%w: the base view pointer has no %q to condition on",
			ErrInvalidPointer, MetadataLocationKey)
	}
	metadata, err := EncodeViewPointer(next)
	if err != nil {
		return err
	}
	return s.stage(ctx, addr, pointerPath, metadata, func(current *catalog.Entry) error {
		if current == nil || current.Metadata[MetadataLocationKey] != base.MetadataLocation {
			return graveler.ErrPreconditionFailed
		}
		return nil
	})
}

// StageViewMetadata registers the metadata documents of one view write as lakeFS entries on
// addr.Ref, under the view's own directory of the staged-file area (§6.3):
//
//	_lakefs_iceberg/files/<view-uuid>/<path relative to the view root>
//
// It is the view half of StageTableFiles (files.go) and it deliberately shares that file's
// deterministic key builder (TableFilePath) and its validation: a relative path that escapes the
// view root, and a write that would stage more files than iceberg.max_staged_files, are refused
// before anything is written. The rule is the same because the property is the same - a metadata
// document that is not an entry is a document the committed GC may collect, and a view has no
// other kind of file.
//
// root is the view's base location (the `location` of its metadata document). A document outside
// it is a *ForeignTableFileError, i.e. the 400 of §13: the catalog would otherwise publish an entry
// pointing outside the storage it is allowed to write.
func (s *Store) StageViewMetadata(ctx context.Context, addr Address, viewUUID uuid.UUID, root string, files []TableMetadataFile) error {
	if len(files) == 0 {
		return nil
	}
	if len(files) > s.maxStagedFiles {
		return &TableFileLimitError{Count: len(files), Max: s.maxStagedFiles}
	}
	// The physical address of an entry is the location in the block adapter's own spelling (D2,
	// location.go blockIdentifier), which needs the repository's storage namespace: on an Azure
	// blockstore the document's location is abfs[s]:// and the adapter reads https://.
	repo, err := s.catalog.GetRepository(ctx, addr.Repo)
	if err != nil {
		return fmt.Errorf("%w: resolve repository %q: %w", ErrTableFileRegistration, addr.Repo, err)
	}
	trimmedRoot := strings.TrimRight(root, "/") + "/"
	now := time.Now().UTC()
	planned := make([]TableFile, 0, len(files))
	for _, file := range files {
		relative, ok := strings.CutPrefix(file.Location, trimmedRoot)
		if !ok {
			return &ForeignTableFileError{
				Kind:     TableFileKindMetadata,
				Location: file.Location,
				Root:     strings.TrimRight(root, "/"),
				Err:      ErrTableLocationOutsideArea,
			}
		}
		entryPath, err := TableFilePath(viewUUID, relative)
		if err != nil {
			return err
		}
		planned = append(planned, TableFile{
			Kind:            TableFileKindMetadata,
			Path:            entryPath,
			PhysicalAddress: blockIdentifier(repo.StorageNamespace, file.Location),
			Size:            file.Size,
			// A view metadata document carries no last-updated-ms, so the write time is the
			// closest thing to when the object appeared: the uncommitted GC ages staged entries
			// by it (gc.go UncommittedGCProtected).
			LastModified: now,
		})
	}
	return s.stageTableFiles(ctx, addr, planned, DefaultStageWorkers)
}

// releaseViewMetadata stages the removal of every staged-file entry of one view: the reverse of
// StageViewMetadata and the second half of a drop.
//
// The caller must have removed the pointer first, exactly as the table drop does: a failure between
// the two steps then leaves entries that still protect their objects rather than a view whose
// documents are no longer protected (files.go releaseTableFiles, tables.go dropTable).
func releaseViewMetadata(ctx context.Context, lister TableLister, addr Address, viewUUID uuid.UUID, workers int) (int, error) {
	return releaseTableFiles(ctx, lister, addr, viewUUID, workers)
}

// ViewLocation resolves the base location of a new view: the client's `location` when the create
// requested one, and <storage_namespace>/_iceberg/<view-uuid> otherwise. The rules, the validation
// and the reason are the tables' (location.go TableLocation, §14 Q-I5): a location is accepted only
// inside the catalog's table area, and the managed one carries the UUID so a drop-and-recreate
// under the same name gets a fresh directory.
func (s *Store) ViewLocation(ctx context.Context, repositoryID, requested, viewUUID string) (string, error) {
	return s.TableLocation(ctx, repositoryID, requested, viewUUID)
}

// viewRoot is ViewLocation for an existing view's document: the location must be given (the view
// spec requires it) and must be inside the catalog's table area. It is what a replace validates
// before writing the next document, so that a set-location update cannot make the catalog write -
// or stage an entry - outside the area its GC, grace window and deny policy cover.
func (s *Store) viewRoot(ctx context.Context, repositoryID, location string) (string, error) {
	if strings.TrimSpace(location) == "" {
		return "", fmt.Errorf("%w: the view metadata has no location to write a metadata file under",
			ErrInvalidTableLocation)
	}
	repository, err := s.catalog.GetRepository(ctx, repositoryID)
	if err != nil {
		return "", fmt.Errorf("resolve repository %q: %w", repositoryID, err)
	}
	return normalizeTableLocation(repository.StorageNamespace, location)
}

// NewViewMetadataFileLocation returns the fully qualified location of the next metadata document
// of a view:
//
//	<view-location>/metadata/<seq>-<uuid>.metadata.json
//
// It is NewMetadataFileLocation for the view spec, and it keeps the same two properties for the
// same two reasons: the sequence continues the file it replaces (so a listing is chronological),
// and the trailing UUID is fresh per write (so two replaces racing on one base cannot overwrite each
// other's document, which matters because the object store has no conditional put).
func NewViewMetadataFileLocation(viewLocation, baseLocation string, meta view.Metadata) (string, error) {
	if strings.TrimSpace(viewLocation) == "" {
		return "", fmt.Errorf("%w: the view metadata has no location to write a metadata file under",
			ErrInvalidPointer)
	}
	sequence := nextViewMetadataSequence(baseLocation, meta)
	fileUUID, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("generate a view metadata file name: %w", err)
	}
	return strings.TrimRight(viewLocation, "/") + "/" + MetadataDirectory + "/" +
		metadataFileName(sequence, fileUUID), nil
}

// nextViewMetadataSequence is the sequence number of the view metadata file that replaces
// baseLocation: the base file's sequence plus one when this catalog wrote it, and otherwise the
// number of version-log entries - the view spec's own record of how many versions the document
// remembers.
func nextViewMetadataSequence(baseLocation string, meta view.Metadata) int {
	if sequence, ok := metadataFileSequence(baseLocation); ok {
		return sequence + 1
	}
	return len(meta.VersionLog()) + 1
}

// WriteViewMetadata serializes a view metadata document and writes it at location, returning the
// exact bytes written.
func (s *Store) WriteViewMetadata(ctx context.Context, repositoryID, location string, meta view.Metadata) ([]byte, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal view metadata for %q: %w", location, err)
	}
	if err := s.writeDocument(ctx, repositoryID, location, data); err != nil {
		return nil, err
	}
	return data, nil
}

// loadViewMetadata reads and parses the view metadata document at a physical URI through the block
// adapter. The URI is fully qualified by design, so it is resolved as a full identifier against the
// repository's storage namespace - exactly like loadMetadata does for a table - and in the block
// adapter's own spelling (D2, location.go blockIdentifier), because the adapter cannot read
// abfs[s]:// on an Azure blockstore.
func (s *Store) loadViewMetadata(ctx context.Context, repositoryID, location string) (view.Metadata, []byte, error) {
	repo, err := s.catalog.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve repository %q: %w", repositoryID, err)
	}
	reader, err := s.block.Get(ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       blockIdentifier(repo.StorageNamespace, location),
		IdentifierType:   block.IdentifierTypeFull,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("read iceberg view metadata %q: %w", location, err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("read iceberg view metadata %q: %w", location, err)
	}
	metadata, err := view.ParseMetadataBytes(data)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse iceberg view metadata %q: %w", ErrInvalidPointer, location, err)
	}
	return metadata, data, nil
}

// ViewCommitRequest is the body of a view replace (spec: components.schemas.CommitViewRequest): the
// requirements the view must still satisfy, and the updates to apply. Both are decoded by the
// iceberg-go view package, so a payload this catalog cannot model fails at decode time rather than
// half-way through a replace.
//
// identifier repeats the address the URL already carries and is therefore ignored, like the table
// commit's identifier.
type ViewCommitRequest struct {
	Requirements view.Requirements `json:"requirements"`
	Updates      view.Updates      `json:"updates"`
}

// ViewCommitResult is the outcome of a successful replace: the new pointer, the parsed metadata and
// the exact bytes written, i.e. everything a LoadViewResult needs.
type ViewCommitResult struct {
	// Address is the view the replace was addressed to, unchanged.
	Address Address

	// Pointer is the new view pointer, already staged on Address.Ref.
	Pointer ViewPointer

	// Metadata is the new metadata document, parsed.
	Metadata view.Metadata

	// MetadataJSON is the exact bytes of the new metadata document.
	MetadataJSON []byte
}

// CommitView applies one Iceberg view commit (a replace) to the view at addr.
//
// The steps are the table commit's (commit.go Commit) with the view model in place of the table
// one:
//
//  1. resolve the base pointer and load the document it names (GetView). A missing view is 404
//     NoSuchViewException; a corrupt pointer or document is a server-side data error;
//  2. validate every requirement of the request against the base document; a failed requirement is
//     a *CommitFailedError, i.e. 409 CommitFailedException - the view moved under the client;
//  3. apply the updates through iceberg-go's view.MetadataBuilder. An update the current document
//     forbids, an unsupported format version and a location outside the catalog's area are a
//     *BadRequestError (400): retrying the same request cannot help;
//  4. write the result to a fresh metadata file (never a reused name) at the view's location;
//  5. stage that document as a lakeFS entry under the view's UUID directory (§6.3);
//  6. swap the pointer with a condition on the base metadata-location, so exactly one of two
//     concurrent replaces wins; a lost race is a *CommitFailedError, and a failure after the
//     document was written is a *CommitStateUnknownError (500) because the client cannot know
//     whether it landed;
//  7. return the new pointer and document, i.e. the body of a LoadViewResult.
//
// Nothing is committed to lakeFS: the writes are staged on addr.Ref and become part of the
// repository on an explicit commit, exactly like table changes.
func (s *Store) CommitView(ctx context.Context, addr Address, request ViewCommitRequest) (*ViewCommitResult, error) {
	// Step 1: the base pointer and its document.
	base, err := s.GetView(ctx, addr)
	if err != nil {
		return nil, err
	}

	// Step 2: the requirements, in request order, so the reported failure is the one the client
	// sent first.
	if err := validateViewRequirements(request.Requirements, base.Metadata); err != nil {
		return nil, err
	}

	// Step 3: apply the updates.
	next, err := applyViewUpdates(base.Metadata, request.Updates)
	if err != nil {
		return nil, err
	}

	// The format version is validated here rather than trusted to the builder: the builder refuses
	// an unsupported version in Build, and naming it as a 400 with the reason is the difference
	// between a client that knows what happened and a generic rejection.
	if version := next.FormatVersion(); version != SupportedViewFormatVersion {
		return nil, &BadRequestError{
			Message: fmt.Sprintf("view format version %d is not supported: this catalog serves view format version %d",
				version, SupportedViewFormatVersion),
		}
	}

	// The document's location roots the file this replace writes and the entry it stages, so it is
	// validated before either exists. set-location is a served update for views, but only inside
	// the catalog's table area.
	root, err := s.viewRoot(ctx, addr.Repo, next.Location())
	if err != nil {
		if errors.Is(err, ErrInvalidTableLocation) {
			return nil, &BadRequestError{Message: err.Error(), Err: err}
		}
		return nil, err
	}

	// The uuid names the directory the document's entry is staged under, so it is read from the
	// document rather than remembered from the base: an assign-uuid update is a change of identity,
	// and the pointer and the staged entry must agree on the uuid a later drop will read.
	viewUUID := next.ViewUUID()
	location, err := NewViewMetadataFileLocation(root, base.Pointer.MetadataLocation, next)
	if err != nil {
		return nil, err
	}
	document, err := s.WriteViewMetadata(ctx, addr.Repo, location, next)
	if err != nil {
		return nil, err
	}

	// Step 5: the document becomes a lakeFS entry, so the committed GC protects it and a diff or a
	// merge sees it (§6.3).
	if err := s.StageViewMetadata(ctx, addr, viewUUID, root, []TableMetadataFile{
		{Location: location, Size: int64(len(document))},
	}); err != nil {
		if isRefusedTableFiles(err) {
			return nil, &BadRequestError{
				Message: "the view metadata file cannot be registered",
				Err:     err,
			}
		}
		return nil, &CommitStateUnknownError{MetadataLocation: location, Err: err}
	}

	// Step 6: the compare-and-swap.
	pointer := ViewPointer{
		MetadataLocation: location,
		ViewUUID:         next.ViewUUID().String(),
		FormatVersion:    next.FormatVersion(),
	}
	if err := s.UpdateViewPointer(ctx, addr, base.Pointer, pointer); err != nil {
		if errors.Is(err, graveler.ErrPreconditionFailed) {
			return nil, &CommitFailedError{Err: err}
		}
		return nil, &CommitStateUnknownError{MetadataLocation: location, Err: err}
	}

	return &ViewCommitResult{
		Address:      addr,
		Pointer:      pointer,
		Metadata:     next,
		MetadataJSON: document,
	}, nil
}

// validateViewRequirements runs Requirement.Validate for every requirement of a view replace
// against the base document. Like the table path it maps a failure to *CommitFailedError (409)
// here rather than matching a cause: iceberg-go's Requirement.Validate returns a plain error.
func validateViewRequirements(requirements view.Requirements, base view.Metadata) error {
	for _, requirement := range requirements {
		if err := requirement.Validate(base); err != nil {
			return &CommitFailedError{Requirement: requirement.GetType(), Err: err}
		}
	}
	return nil
}

// applyViewUpdates applies the update actions of a view replace to the base document through
// iceberg-go's builder, in request order. The first update that cannot be applied - an unknown
// schema, a version that names no schema, a dialect a replacement may not drop - is a
// *BadRequestError: the request cannot be served as sent, and retrying it unchanged cannot help
// (§2.1).
func applyViewUpdates(base view.Metadata, updates view.Updates) (view.Metadata, error) {
	builder, err := view.MetadataBuilderFromBase(base)
	if err != nil {
		return nil, &BadRequestError{
			Message: "the current view metadata cannot be modified",
			Err:     err,
		}
	}
	for _, update := range updates {
		if err := update.Apply(builder); err != nil {
			return nil, &BadRequestError{
				Message: fmt.Sprintf("the %q update cannot be applied to the current view", update.Action()),
				Err:     err,
			}
		}
	}
	result, err := builder.Build()
	if err != nil {
		return nil, &BadRequestError{
			Message: "the updates cannot be applied to the current view",
			Err:     err,
		}
	}
	return result.Metadata, nil
}

// viewPointerPath builds the reserved key of a view pointer.
func viewPointerPath(addr Address) (string, error) {
	if !addr.HasView() {
		return "", fmt.Errorf("%w: the address does not name a view", ErrInvalidIdentifier)
	}
	if len(addr.Namespace) == 0 {
		return "", fmt.Errorf("%w: the namespace is empty", ErrInvalidIdentifier)
	}
	parts := make([]string, len(addr.Namespace))
	for i, level := range addr.Namespace {
		if err := validateLayoutSegment(level); err != nil {
			return "", fmt.Errorf("namespace level %d: %w", i+1, err)
		}
		parts[i] = level
	}
	if err := validateLayoutSegment(addr.View); err != nil {
		return "", fmt.Errorf("view name: %w", err)
	}
	return ReservedPrefix + "/" + ViewsDirectory + "/" + strings.Join(parts, "/") + "/" +
		addr.View + "/" + ViewPointerName, nil
}

// viewEntryPrefix is the reserved key prefix of the pointers below a namespace:
// `_lakefs_iceberg/views/<ns levels…>/`. The trailing slash is what keeps a namespace from matching
// a sibling whose name it is a prefix of.
func viewEntryPrefix(namespace Namespace) string {
	return ReservedPrefix + "/" + ViewsDirectory + "/" + strings.Join(namespace, "/") + "/"
}
