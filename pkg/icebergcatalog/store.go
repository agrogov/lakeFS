package icebergcatalog

// Catalog state as lakeFS entries (ICEBERG_CATALOG_PLAN.md §6.1 "Layout on the branch",
// §4 "store.go").
//
// The catalog stores everything it knows on the lakeFS branch, so diff, merge, revert, commit
// and GC work on it for free:
//
//	_lakefs_iceberg/
//	  ns/<ns1>/<ns2>/.namespace              namespace marker, entry user metadata = properties
//	  tables/<ns1>/<ns2>/<table>/.pointer    table pointer, entry user metadata:
//	                                           iceberg.metadata-location = physical URI of metadata.json
//	                                           iceberg.table-uuid       = table UUID
//	                                           iceberg.format-version   = 1 | 2
//	  views/<ns1>/<ns2>/<view>/.pointer      view pointer, entry user metadata (views.go):
//	                                           iceberg.metadata-location = physical URI of metadata.json
//	                                           iceberg.view-uuid        = view UUID
//	                                           iceberg.format-version   = 1
//	  files/<uuid>/<relative path>           one entry per staged file (§6.3, files.go): the table files
//	                                           a commit added, the initial metadata.json a create wrote,
//	                                           and the view metadata documents a create or replace wrote
//
// The read half resolves the marker and the pointer on a branch, tag or commit, decodes the
// pointer's user metadata and loads the referenced metadata.json through the block adapter.
//
// The write half creates and updates those entries with a check-and-put: every marker and pointer
// write is a catalog.CreateEntry gated by graveler.WithCondition (§6.2 steps 5-6), so it is:
//
//   - a compare-and-swap. A table pointer write is conditioned on the pointer's
//     iceberg.metadata-location, the field §6.2 step 6 swaps. A lost race returns
//     graveler.ErrPreconditionFailed, never a silent overwrite, and that is what keeps GC safe:
//     the metadata.json a losing client wrote is unreachable and collectable, while the pointer
//     still names the winner's file.
//   - a create on an absent value for create namespace / create table. A nil current value is
//     the If-None-Match: * semantics of pkg/api/apiutil/conditions.go:16-36, so one create wins
//     and the other learns that the name is taken (ErrAlreadyExists).
//
// No write passes graveler.SetOptions.Force. A protected branch therefore rejects a catalog write
// with graveler.ErrWriteToProtectedBranch and a read-only (datasets-immutable) repository rejects
// it with graveler.ErrReadOnlyRepository - the same answers an fs: write gets (§6.2 "why stage
// instead of auto-commit"). That includes the table file entries of §6.3 (files.go): they are
// staged through the same catalog.CreateEntry without Force, so branch protection, repository
// immutability and the reserved-prefix policy apply to them unchanged. A file entry is not
// conditioned, because it is not a compare-and-swap target: its key is derived from the file's own
// path, so a retried write overwrites it instead of racing another writer.
//
// Writes are staged, never committed: a namespace or table created through the catalog becomes
// visible on the branch immediately (staged reads, above) and becomes part of the repository only
// on an explicit lakeFS commit. That is the documented "staged on the target branch" behaviour
// (§14 Q-I2), and it is why no method here calls catalog.Commit.
//
// Reads go through catalog.GetEntry without GetEntryParams.StageOnly, so graveler answers with the
// staged value first and falls back to the committed one. That is what makes a catalog write
// visible before its lakeFS commit (§6.2 "staging, not auto-commit") and what lets the same code
// read an immutable tag or commit.
//
// The table pointer is a single entry. Its metadata-location is the physical URI of the current
// metadata.json, which is where a client (or a merge, see §6.4) can see a change as an ordinary
// file change. The pointer and the metadata file are separate on purpose: the pointer is the
// compare-and-swap target of a commit, and metadata.json is never overwritten.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/iceberglayout"

	"github.com/apache/iceberg-go/table"
)

// The reserved-prefix layout constants are declared in the leaf package iceberglayout, which
// pkg/datasets also imports: it materialises catalog workspaces and must build these exact keys,
// and it cannot import this package without turning this package's tests - which import
// pkg/datasets to check the §7.2 authorization matrix against a real auth service - into an
// import cycle. They are aliased here, under the names this package has always exported, so the
// surface every caller and test uses is unchanged; iceberglayout documents each one.
const (
	// ReservedPrefix is the reserved key prefix that holds all catalog state on the branch.
	ReservedPrefix = iceberglayout.ReservedPrefix

	// NamespaceDirectory holds one .namespace marker per namespace.
	NamespaceDirectory = iceberglayout.NamespaceDirectory

	// TablesDirectory holds one .pointer entry per table.
	TablesDirectory = iceberglayout.TablesDirectory

	// NamespaceMarkerName is the file name of a namespace marker.
	NamespaceMarkerName = iceberglayout.NamespaceMarkerName

	// TablePointerName is the file name of a table pointer.
	TablePointerName = iceberglayout.TablePointerName

	// MetadataLocationKey is the pointer user-metadata key that holds the physical URI of the
	// current metadata.json, i.e. the compare-and-swap field of a commit (§6.2 step 6).
	MetadataLocationKey = iceberglayout.MetadataLocationKey

	// TableUUIDKey is the pointer user-metadata key that holds the table UUID that metadata.json
	// also carries as table-uuid.
	TableUUIDKey = iceberglayout.TableUUIDKey

	// FormatVersionKey is the pointer user-metadata key that holds the Iceberg format version,
	// 1 or 2 (§1.2 rules out serving version 3).
	FormatVersionKey = iceberglayout.FormatVersionKey
)

// ErrInvalidPointer is wrapped when a table pointer, a view pointer or the namespace marker exists
// but its content is unusable: a missing or empty field, a format version this catalog does not
// serve, or a metadata.json that does not parse. It is a server-side data error, not a missing
// object: the caller of GetTable or GetView already established that the pointer entry exists.
var ErrInvalidPointer = errors.New("invalid iceberg catalog entry")

// ErrAlreadyExists is wrapped when a create-only write finds a value where it required an absent
// one: the namespace or the table name is taken, in the staging area or in the committed data.
// The HTTP layer maps it to 409 NamespaceAlreadyExistsException or TableAlreadyExistsException
// (spec: components.responses.CreateNamespaceResponse / CreateTableResponse, task I2-2 and I2-4).
var ErrAlreadyExists = errors.New("iceberg catalog entry already exists")

// ErrInvalidNamespaceProperties is wrapped when namespace properties cannot be stored as the user
// metadata of a marker, i.e. a property has an empty name.
var ErrInvalidNamespaceProperties = errors.New("invalid iceberg namespace properties")

// NoSuchNamespaceException is returned when the namespace marker does not exist on the requested
// ref. The HTTP layer maps it to 404 NoSuchNamespaceException (ICEBERG_CATALOG_PLAN.md §5).
type NoSuchNamespaceException struct {
	Repo      string
	Ref       string
	Namespace Namespace
}

func (e *NoSuchNamespaceException) Error() string {
	return fmt.Sprintf("no such namespace %q in %s@%s", e.Namespace.Dotted(), e.Repo, e.Ref)
}

// NoSuchTableException is returned when the table pointer does not exist on the requested ref.
// The HTTP layer maps it to 404 NoSuchTableException (ICEBERG_CATALOG_PLAN.md §5).
type NoSuchTableException struct {
	Repo      string
	Ref       string
	Namespace Namespace
	Table     string
}

func (e *NoSuchTableException) Error() string {
	return fmt.Sprintf("no such table %q in namespace %q of %s@%s",
		e.Table, e.Namespace.Dotted(), e.Repo, e.Ref)
}

// NamespaceMarker is a decoded _lakefs_iceberg/ns/<ns>/.namespace entry. Properties is the entry
// user metadata, i.e. the namespace properties of the Iceberg REST namespace object (§6.1).
// It is never nil.
type NamespaceMarker struct {
	Properties map[string]string
}

// TablePointer is the decoded user metadata of a _lakefs_iceberg/tables/<ns>/<table>/.pointer
// entry (ICEBERG_CATALOG_PLAN.md §6.1).
type TablePointer struct {
	// MetadataLocation is the physical URI of the current metadata.json.
	MetadataLocation string

	// TableUUID is the table UUID that metadata.json also carries.
	TableUUID string

	// FormatVersion is the Iceberg format version of the table, 1 or 2.
	FormatVersion int
}

// Table is a resolved table: its pointer and the metadata.json the pointer references.
type Table struct {
	// Address is the catalog address the table was read from, including the ref. It is useful
	// to report which commit or tag answered a read.
	Address Address

	// Pointer is the decoded pointer entry.
	Pointer TablePointer

	// Metadata is metadata.json parsed by iceberg-go.
	Metadata table.Metadata

	// MetadataJSON is the exact bytes of metadata.json, so an ETag or a pass-through response
	// does not have to re-marshal the parsed model.
	MetadataJSON []byte
}

// CatalogReader is the part of catalog.Catalog the store reads through. It is an interface so
// tests can drive resolution without a full graveler, and so the write half stays explicit about
// what it needs beyond reads.
type CatalogReader interface {
	GetRepository(ctx context.Context, repository string) (*catalog.Repository, error)
	GetEntry(ctx context.Context, repositoryID string, reference string, path string, params catalog.GetEntryParams) (*catalog.DBEntry, error)
}

// CatalogWriter is the part of catalog.Catalog the store writes through. CreateEntry is the only
// write verb the catalog needs: graveler's SetOptionsFunc carries the condition, and leaving
// graveler.WithForce out of it is what lets branch protection and repository immutability apply.
type CatalogWriter interface {
	CreateEntry(ctx context.Context, repositoryID string, branch string, entry catalog.DBEntry, opts ...graveler.SetOptionsFunc) error
}

// Catalog is what Store needs of catalog.Catalog: the any-ref reads of §6.1 and the conditional
// staging writes of §6.2.
type Catalog interface {
	CatalogReader
	CatalogWriter
}

// Store resolves catalog state on lakeFS refs and stages catalog writes on a branch.
type Store struct {
	catalog Catalog
	block   block.Adapter

	// resolver is the registered table-resolution hook (resolver.go). It is nil by default, which
	// is what makes every read of a catalog without the hook exactly the read it always was.
	resolver TableResolver

	// maxStagedFiles is the cap on the number of table-file entries one commit may stage, i.e.
	// iceberg.max_staged_files (§10). It is enforced by StageTableFiles before anything is
	// written, and it is a Store field rather than a parameter of Commit because the commit body
	// is the client's: a client must not be able to raise its own cap.
	maxStagedFiles int
}

// StoreOption configures a Store at construction.
type StoreOption func(*Store)

// WithMaxStagedFiles sets the cap on the table files of one commit, the iceberg.max_staged_files
// setting (§10, DefaultMaxStagedFiles). The mount passes the configured value:
//
//	store := NewStore(deps.Catalog, deps.BlockAdapter, WithMaxStagedFiles(cfg.MaxStagedFiles))
//
// A non-positive value is ignored - the default stays in force - because the cap exists to refuse
// an oversized commit, and a cap of zero would refuse every commit instead of protecting it.
func WithMaxStagedFiles(maxFiles int) StoreOption {
	return func(s *Store) {
		if maxFiles > 0 {
			s.maxStagedFiles = maxFiles
		}
	}
}

// WithTableResolver registers the table-resolution hook (resolver.go, METADATA_SEARCH_DESIGN.md
// §5.1). The hook is what makes a virtual table such as the Metadata Search table of §5.2
// addressable, and it is off by default: without this option every catalog read is exactly the
// read it was before the hook existed.
//
// The mount registers it only while the feature that owns the resolver is enabled
// (iceberg.metadata_search.enabled, §5.5); a nil resolver is ignored, so a caller that has no
// resolver to register cannot accidentally clear one.
func WithTableResolver(resolver TableResolver) StoreOption {
	return func(s *Store) {
		if resolver != nil {
			s.resolver = resolver
		}
	}
}

// NewStore returns a catalog store over the given catalog and block adapter. The catalog must be
// able to read entries on any ref and to stage writes on a branch; *catalog.Catalog does both.
func NewStore(catalogStore Catalog, blockAdapter block.Adapter, opts ...StoreOption) *Store {
	store := &Store{
		catalog:        catalogStore,
		block:          blockAdapter,
		maxStagedFiles: DefaultMaxStagedFiles,
	}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

// MaxStagedFiles is the cap this store enforces on the table files of one commit.
func (s *Store) MaxStagedFiles() int { return s.maxStagedFiles }

// GetNamespace reads the _lakefs_iceberg/ns/<ns>/.namespace marker of addr.Namespace on
// addr.Ref. A missing marker is NoSuchNamespaceException.
//
// A missing repository or ref is returned as the underlying lakeFS error, not as a missing
// namespace: the caller can then distinguish "this warehouse/repository does not exist"
// (NoSuchWarehouseException) from "this repository has no such namespace".
func (s *Store) GetNamespace(ctx context.Context, addr Address) (*NamespaceMarker, error) {
	markerPath, err := namespaceMarkerPath(addr.Namespace)
	if err != nil {
		return nil, err
	}
	entry, err := s.getEntry(ctx, addr, markerPath)
	if err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			return nil, &NoSuchNamespaceException{Repo: addr.Repo, Ref: addr.Ref, Namespace: addr.Namespace}
		}
		return nil, err
	}
	properties := make(map[string]string, len(entry.Metadata))
	for key, value := range entry.Metadata {
		properties[key] = value
	}
	return &NamespaceMarker{Properties: properties}, nil
}

// GetTable reads the _lakefs_iceberg/tables/<ns>/<table>/.pointer entry of addr on addr.Ref,
// decodes the pointer, and loads the referenced metadata.json through the block adapter. A
// missing pointer is NoSuchTableException.
//
// Staged data wins: catalog.GetEntry resolves the branch's staging area before the committed
// metadata range, so a create or commit that is staged but not yet committed is already readable.
// On a tag or a commit there is no staging area, which is exactly the read-only any-ref behaviour
// the catalog promises (ICEBERG_CATALOG_PLAN.md §1.1 goal 1).
func (s *Store) GetTable(ctx context.Context, addr Address) (*Table, error) {
	pointerPath, err := tablePointerPath(addr)
	if err != nil {
		return nil, err
	}
	entry, err := s.getEntry(ctx, addr, pointerPath)
	if err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			return nil, &NoSuchTableException{
				Repo:      addr.Repo,
				Ref:       addr.Ref,
				Namespace: addr.Namespace,
				Table:     addr.Table,
			}
		}
		return nil, err
	}

	pointer, err := DecodeTablePointer(entry.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode pointer %q: %w", pointerPath, err)
	}

	metadata, metadataJSON, err := s.loadMetadata(ctx, addr.Repo, pointer.MetadataLocation)
	if err != nil {
		return nil, err
	}
	return &Table{
		Address:      addr,
		Pointer:      pointer,
		Metadata:     metadata,
		MetadataJSON: metadataJSON,
	}, nil
}

// DecodeTablePointer decodes the user metadata of a table pointer entry. It is exported because
// EncodeTablePointer, the write half, must produce exactly these fields, and because a test can pin
// the wire names.
func DecodeTablePointer(metadata map[string]string) (TablePointer, error) {
	location := metadata[MetadataLocationKey]
	if location == "" {
		return TablePointer{}, fmt.Errorf("%w: user metadata %q is missing or empty",
			ErrInvalidPointer, MetadataLocationKey)
	}
	tableUUID := metadata[TableUUIDKey]
	if tableUUID == "" {
		return TablePointer{}, fmt.Errorf("%w: user metadata %q is missing or empty",
			ErrInvalidPointer, TableUUIDKey)
	}
	rawVersion := metadata[FormatVersionKey]
	if rawVersion == "" {
		return TablePointer{}, fmt.Errorf("%w: user metadata %q is missing or empty",
			ErrInvalidPointer, FormatVersionKey)
	}
	formatVersion, err := strconv.Atoi(rawVersion)
	if err != nil {
		return TablePointer{}, fmt.Errorf("%w: %q is %q, not an integer: %w",
			ErrInvalidPointer, FormatVersionKey, rawVersion, err)
	}
	// v1 and v2 are what the catalog serves; v3 is an explicit non-goal of v1 (§1.2).
	if formatVersion != 1 && formatVersion != 2 {
		return TablePointer{}, fmt.Errorf("%w: %q is %d, and only Iceberg format versions 1 and 2 are served",
			ErrInvalidPointer, FormatVersionKey, formatVersion)
	}
	return TablePointer{
		MetadataLocation: location,
		TableUUID:        tableUUID,
		FormatVersion:    formatVersion,
	}, nil
}

// EncodeNamespaceProperties turns namespace properties into the entry user metadata of a
// namespace marker. The marker carries nothing else, because §6.1 defines the namespace properties
// as exactly that user metadata. A nil or empty map is a marker with no properties, which is still
// a marker: creating it must not fail.
func EncodeNamespaceProperties(properties map[string]string) (map[string]string, error) {
	metadata := make(map[string]string, len(properties))
	for key, value := range properties {
		if key == "" {
			return nil, fmt.Errorf("%w: a namespace property has an empty name", ErrInvalidNamespaceProperties)
		}
		metadata[key] = value
	}
	return metadata, nil
}

// EncodeTablePointer turns a pointer into the entry user metadata of a table pointer (§6.1).
//
// It validates the same fields DecodeTablePointer validates, so a pointer that this catalog would
// refuse to read can never be written: a commit that produced one would be unrecoverable without
// another commit, and one of the three fields (metadata-location) is the compare-and-swap target.
func EncodeTablePointer(pointer TablePointer) (map[string]string, error) {
	if pointer.MetadataLocation == "" {
		return nil, fmt.Errorf("%w: %q must not be empty", ErrInvalidPointer, MetadataLocationKey)
	}
	if pointer.TableUUID == "" {
		return nil, fmt.Errorf("%w: %q must not be empty", ErrInvalidPointer, TableUUIDKey)
	}
	// v1 and v2 are what the catalog serves; v3 is an explicit non-goal of v1 (§1.2).
	if pointer.FormatVersion != 1 && pointer.FormatVersion != 2 {
		return nil, fmt.Errorf("%w: %q is %d, and only Iceberg format versions 1 and 2 are served",
			ErrInvalidPointer, FormatVersionKey, pointer.FormatVersion)
	}
	return map[string]string{
		MetadataLocationKey: pointer.MetadataLocation,
		TableUUIDKey:        pointer.TableUUID,
		FormatVersionKey:    strconv.Itoa(pointer.FormatVersion),
	}, nil
}

// CreateNamespace stages a new namespace marker with the given properties on addr.Ref.
//
// It is a create on an absent value: the condition fails when a marker already exists in the
// branch staging area or in the committed data, so a concurrent or earlier create wins and this
// call returns ErrAlreadyExists rather than overwriting someone else's properties. That mirrors
// If-None-Match: * (pkg/api/apiutil/conditions.go:16-36), which the object API uses for the same
// reason: the caller must not silently replace an existing namespace.
func (s *Store) CreateNamespace(ctx context.Context, addr Address, properties map[string]string) error {
	markerPath, err := namespaceMarkerPath(addr.Namespace)
	if err != nil {
		return err
	}
	metadata, err := EncodeNamespaceProperties(properties)
	if err != nil {
		return err
	}
	name := addr.Namespace.Dotted()
	err = s.stage(ctx, addr, markerPath, metadata, func(current *catalog.Entry) error {
		if current != nil {
			return fmt.Errorf("%w: namespace %q already exists on %s@%s",
				ErrAlreadyExists, name, addr.Repo, addr.Ref)
		}
		return nil
	})
	return asAlreadyExists(err, "namespace", name, addr)
}

// UpdateNamespace replaces the properties of an existing namespace marker on addr.Ref.
//
// Unlike CreateNamespace it requires the marker to exist, so a namespace that was dropped (or
// never created) is not resurrected by a properties update: the condition returns
// NoSuchNamespaceException, and the caller maps it to 404. The properties are written as the whole
// set, not merged; a merge is a read-modify-write that belongs to the caller that has to report
// the merge result (task I2-2).
func (s *Store) UpdateNamespace(ctx context.Context, addr Address, properties map[string]string) error {
	markerPath, err := namespaceMarkerPath(addr.Namespace)
	if err != nil {
		return err
	}
	metadata, err := EncodeNamespaceProperties(properties)
	if err != nil {
		return err
	}
	return s.stage(ctx, addr, markerPath, metadata, func(current *catalog.Entry) error {
		if current == nil {
			return &NoSuchNamespaceException{Repo: addr.Repo, Ref: addr.Ref, Namespace: addr.Namespace}
		}
		return nil
	})
}

// CreateTable stages a new table pointer on addr.Ref.
//
// Like CreateNamespace it is a create on an absent value (If-None-Match: *), so a create whose name
// is already taken returns ErrAlreadyExists instead of replacing an existing table's pointer and
// orphaning the metadata.json it named. Metadata files are never overwritten; a pointer write is
// the only way to change which one is current (§6.1).
//
// This call stages the pointer and nothing else. The create path of the catalog uses
// CreateTableWithMetadata, which first registers the initial metadata.json as a lakeFS entry, so a
// table that is created and never appended to still has a referenced metadata file (§6.3).
func (s *Store) CreateTable(ctx context.Context, addr Address, pointer TablePointer) error {
	pointerPath, err := tablePointerPath(addr)
	if err != nil {
		return err
	}
	metadata, err := EncodeTablePointer(pointer)
	if err != nil {
		return err
	}
	err = s.stage(ctx, addr, pointerPath, metadata, func(current *catalog.Entry) error {
		if current != nil {
			return fmt.Errorf("%w: table %q in namespace %q already exists on %s@%s",
				ErrAlreadyExists, addr.Table, addr.Namespace.Dotted(), addr.Repo, addr.Ref)
		}
		return nil
	})
	return asAlreadyExists(err, "table", addr.Namespace.Dotted()+"."+addr.Table, addr)
}

// CreateTableWithMetadata stages a new table on addr.Ref: the lakeFS entry of the initial
// metadata.json the create path wrote (§6.3), and then the table pointer (CreateTable).
//
// The initial metadata file needs its own entry for the reason every table file does (§6.3): the
// pointer names it from entry *user metadata*, not from an entry address, so nothing else makes it
// a lakeFS entry. A table that is created and never appended to would therefore have a metadata
// file no entry and no commit references - collectable by the committed GC sweep, and invisible to
// `lakectl diff` and to a merge. The commit path registers the metadata file it writes with
// StageTableFiles (§6.2 step 5); a create is the same situation with no base metadata and no
// snapshots, so it goes through the same call, which is what makes the rules identical rather than
// a second, weaker copy of them:
//
//   - the foreign-location check against the table's own root, so a create can never register a
//     file outside <storage_namespace>/_iceberg/ (§13);
//   - the iceberg.max_staged_files cap (this store's configured value), refused before anything is
//     staged;
//   - the deterministic entry key _lakefs_iceberg/files/<table-uuid>/metadata/<file name>, the one
//     the commit path uses for the metadata file it writes, so the create and a later commit of the
//     same table address one directory and a retried request overwrites the same key.
//
// metadata is the document pointer names and metadataSize is its length in bytes, i.e. what the
// create path just wrote through WriteMetadata (§6.2 step 4); a create has exactly one such file.
//
// Order matters and is the commit path's: the file entry is staged first, so a failure of file
// registration leaves no pointer that names an unregistered metadata file, and the create answers
// the same error it would answer without this call - a failure after the metadata write, which the
// HTTP layer maps as it did before. The pointer write keeps the create-on-absent-value semantics of
// CreateTable, so a duplicate create is still ErrAlreadyExists; the losing request's entries are
// keyed by its own fresh table uuid, so they cannot shadow the winner's and GC collects them.
func (s *Store) CreateTableWithMetadata(ctx context.Context, addr Address, pointer TablePointer, metadata table.Metadata, metadataSize int64) error {
	if _, err := s.StageTableFiles(ctx, addr, StageTableFilesRequest{
		// A create has no base metadata (Base stays nil: every snapshot of Next would count as
		// new) and a newly initialized metadata.json has no snapshots at all, so the plan is
		// exactly the one metadata file.
		Next:          metadata,
		MetadataFiles: []TableMetadataFile{{Location: pointer.MetadataLocation, Size: metadataSize}},
		MaxFiles:      s.maxStagedFiles,
	}); err != nil {
		return err
	}
	return s.CreateTable(ctx, addr, pointer)
}

// UpdateTablePointer swaps the table pointer from base to next on addr.Ref: the check-and-put of
// §6.2 step 6.
//
// The condition compares the current value's iceberg.metadata-location with base's. That field is
// the commit's linearization point: every commit writes a new metadata.json and stages its URI
// here, so "the pointer still names the metadata.json I based this commit on" is exactly "nobody
// committed since I read". A mismatch - including a pointer that is gone - returns
// graveler.ErrPreconditionFailed, which the caller maps to 409 CommitFailedException; the caller's
// metadata.json stays in the object store unreferenced and GC collects it.
//
// The comparison is deliberately on the location and not on base's uuid or format version: the
// location changes on every commit, while a table that was dropped and recreated can legitimately
// carry a new uuid at a new location (which also fails the comparison).
func (s *Store) UpdateTablePointer(ctx context.Context, addr Address, base, next TablePointer) error {
	pointerPath, err := tablePointerPath(addr)
	if err != nil {
		return err
	}
	if base.MetadataLocation == "" {
		return fmt.Errorf("%w: the base pointer has no %q to condition on",
			ErrInvalidPointer, MetadataLocationKey)
	}
	metadata, err := EncodeTablePointer(next)
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

// stage writes one catalog entry on the branch addr.Ref, gated by condition.
//
// The condition runs inside graveler's check-and-put against the value a reader would see - the
// branch staging area first, then the committed data (graveler.handleUpdate) - so it is evaluated
// against the same state GetEntry resolves and a concurrent writer cannot slip between the check
// and the write.
//
// The entry has no object behind it: a marker and a pointer are metadata-only entries whose content
// is their user metadata, so both the path and the relative address are the reserved key. Nothing
// is written to object storage here; the metadata.json a pointer names is written by the commit
// path (I2-3) before it stages the pointer, and the entry does not reference it by address (the
// pointer's user metadata does) because §6.3 stages table files as their own entries.
func (s *Store) stage(ctx context.Context, addr Address, entryPath string, metadata map[string]string, condition func(current *catalog.Entry) error) error {
	if addr.Repo == "" {
		return fmt.Errorf("%w: the repository is empty", ErrInvalidIdentifier)
	}
	if addr.Ref == "" {
		return fmt.Errorf("%w: the ref is empty", ErrInvalidIdentifier)
	}
	entry := catalog.DBEntry{
		Path:            entryPath,
		PhysicalAddress: entryPath,
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    time.Now().UTC(),
		Metadata:        metadata,
	}
	// No graveler.WithForce: a protected branch or a read-only repository must reject this write.
	return s.catalog.CreateEntry(ctx, addr.Repo, addr.Ref, entry,
		graveler.WithCondition(catalog.EntryCondition(condition)))
}

// asAlreadyExists reports a create-only write that lost, in either of the two ways it can lose: the
// condition saw an existing value and returned ErrAlreadyExists, or a concurrent writer inserted
// the key between the condition's read and the write and graveler's kv predicate failed. Both mean
// the same thing to the caller - the name is taken - so both leave here as ErrAlreadyExists.
func asAlreadyExists(err error, kind, name string, addr Address) error {
	if err == nil || errors.Is(err, ErrAlreadyExists) {
		return err
	}
	if errors.Is(err, graveler.ErrPreconditionFailed) {
		return fmt.Errorf("%w: %s %q already exists on %s@%s",
			ErrAlreadyExists, kind, name, addr.Repo, addr.Ref)
	}
	return err
}

// getEntry resolves one catalog entry on addr.Ref. GetEntryParams{} deliberately leaves
// StageOnly false: graveler tries the branch staging area first and then the committed data.
func (s *Store) getEntry(ctx context.Context, addr Address, path string) (*catalog.DBEntry, error) {
	if addr.Repo == "" {
		return nil, fmt.Errorf("%w: the repository is empty", ErrInvalidIdentifier)
	}
	if addr.Ref == "" {
		return nil, fmt.Errorf("%w: the ref is empty", ErrInvalidIdentifier)
	}
	return s.catalog.GetEntry(ctx, addr.Repo, addr.Ref, path, catalog.GetEntryParams{})
}

// loadMetadata reads and parses metadata.json at a physical URI through the block adapter. The
// URI is fully qualified by design (§6.1, Q-I5), so it is resolved as a full identifier against
// the repository's storage namespace.
func (s *Store) loadMetadata(ctx context.Context, repositoryID, location string) (table.Metadata, []byte, error) {
	repo, err := s.catalog.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve repository %q: %w", repositoryID, err)
	}
	reader, err := s.block.Get(ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("read iceberg metadata %q: %w", location, err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("read iceberg metadata %q: %w", location, err)
	}
	metadata, err := table.ParseMetadataBytes(data)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse iceberg metadata %q: %w", ErrInvalidPointer, location, err)
	}
	return metadata, data, nil
}

// namespaceMarkerPath builds the reserved key of a namespace marker. The client namespace is
// validated so that a level can never be "." or ".." or contain a path separator and thereby
// escape the reserved prefix or reach a sibling layout (ident.go applies the same rules before a
// path parameter becomes an Address; this is the defence in depth for callers inside the
// package).
func namespaceMarkerPath(namespace Namespace) (string, error) {
	if len(namespace) == 0 {
		return "", fmt.Errorf("%w: the namespace is empty", ErrInvalidIdentifier)
	}
	parts := make([]string, len(namespace))
	for i, level := range namespace {
		if err := validateLayoutSegment(level); err != nil {
			return "", fmt.Errorf("namespace level %d: %w", i+1, err)
		}
		parts[i] = level
	}
	return ReservedPrefix + "/" + NamespaceDirectory + "/" + strings.Join(parts, "/") + "/" + NamespaceMarkerName, nil
}

// tablePointerPath builds the reserved key of a table pointer.
func tablePointerPath(addr Address) (string, error) {
	if !addr.HasTable() {
		return "", fmt.Errorf("%w: the address does not name a table", ErrInvalidIdentifier)
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
	if err := validateLayoutSegment(addr.Table); err != nil {
		return "", fmt.Errorf("table name: %w", err)
	}
	return ReservedPrefix + "/" + TablesDirectory + "/" + strings.Join(parts, "/") + "/" + addr.Table + "/" + TablePointerName, nil
}

// validateLayoutSegment rejects the values that would make a level ambiguous as a lakeFS key or
// as a path component. Empty levels are rejected because they would produce "//" in the key.
func validateLayoutSegment(segment string) error {
	if segment == "" {
		return fmt.Errorf("%w: the level is empty", ErrInvalidIdentifier)
	}
	if segment == "." || segment == ".." {
		return fmt.Errorf("%w: %q is reserved as a path component", ErrInvalidIdentifier, segment)
	}
	if strings.ContainsAny(segment, "/\\") {
		return fmt.Errorf("%w: %q contains a path separator", ErrInvalidIdentifier, segment)
	}
	if strings.Contains(segment, NamespaceSeparator) {
		return fmt.Errorf("%w: %q contains a namespace separator", ErrInvalidIdentifier, segment)
	}
	return nil
}
