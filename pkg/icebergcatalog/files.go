package icebergcatalog

// Registering table files as lakeFS entries (ICEBERG_CATALOG_PLAN.md §6.3 "Table files become
// lakeFS entries (GC safety)", §13 risk "Client writes outside the table location", §14 Q-I8,
// §9 datasets, spike ICEBERG_SPIKE_S-I2.md).
//
// An Iceberg client writes data, manifest and metadata files straight to the object store, under
// the table's location:
//
//	<storage_namespace>/_iceberg/<table-uuid>/metadata/<seq>-<uuid>.metadata.json
//	<storage_namespace>/_iceberg/<table-uuid>/metadata/snap-<id>-1-<uuid>.avro     (manifest list)
//	<storage_namespace>/_iceberg/<table-uuid>/metadata/<uuid>-m0.avro             (manifest)
//	<storage_namespace>/_iceberg/<table-uuid>/data/00000-0-<uuid>.parquet         (data file)
//
// Those objects are inside the repository's storage namespace but no lakeFS entry points at them,
// and that has three consequences (§6.3):
//
//   - GC is unsafe. Committed GC deletes namespace objects that no retained commit references, and
//     the catalog commit is staged rather than auto-committed (§6.2), so files that are not
//     entries are collectable. Spike S-I2 §4 measured it: PrepareGCUncommitted lists an object
//     only when a staged entry points at it - 0 of 200 raw client writes were listed, 100% of the
//     staged ones were.
//   - `lakectl diff` and merge only see the pointer change, not the contents of the table.
//   - datasets cannot materialise table contents (§9).
//
// So a commit that adds a snapshot registers the files that snapshot added as lakeFS entries:
//
//	_lakefs_iceberg/files/<table-uuid>/<path relative to the table root>
//
// each with AddressType=Full and the physical URI the client wrote, exactly as elsewhere in the
// reserved area (§6.1). The reserved prefix is the same one the pointer and the namespace markers
// live under, so a single lakeFS commit publishes the files and the pointer together, diff shows
// both, and a dataset materialisation copies the entries and keeps the absolute paths inside
// metadata valid (§9, task I6-1).
//
// # What a drop releases
//
// The other half of the same idea is releaseTableFiles: the drop stages the removal of every entry
// under _lakefs_iceberg/files/<table-uuid>/ in the same commit that removes the table's pointer
// (tables.go dropTable). §6.3 stages the files so that the committed GC protects exactly the live
// files, and the committed GC's live set is the set of addresses reachable from the active commits;
// for a table that no longer exists, an entry left behind protects the opposite of a live file - it
// keeps the dropped table's objects alive for as long as the ref keeps the commits that carry it.
// The release is the exact reverse of the registration above (same uuid, same directory, nothing
// else), and it deletes no object: the objects are shared by every commit and every branch that
// references them, and only the committed GC may decide that none does (§14 Q-I8).
//
// # What is registered
//
// For every snapshot that the commit added (i.e. every snapshot of the new metadata that the base
// metadata does not have, which is exactly what an `add-snapshot` update produces):
//
//  1. the metadata.json the commit wrote (passed in by the caller: the commit path knows the file
//     it wrote in §6.2 step 4, and registering it is not optional - the table pointer references it
//     from entry *user metadata*, not from an entry address, so nothing else makes the committed GC
//     retain it);
//  2. the snapshot's manifest list;
//  3. every manifest that this snapshot added (a manifest-list entry whose added-snapshot-id is
//     this snapshot, or whose added-snapshot-id is unknown - the null case of a v1 manifest list);
//  4. every file those manifests add: status ADDED manifest entries, data files and v2 position or
//     equality delete files alike.
//
// Manifests and data files an earlier commit already registered are deliberately *not* re-staged:
// the cost of a commit is proportional to what it added (spike S-I2 §5.1 item 1), and the entries
// are deterministic, so a retried commit overwrites the same keys (item 5). Entries whose status is
// EXISTING are skipped for the same reason, and DELETED entries are skipped because v1 keeps files
// on expire: Q-I8 answers "no" to file tombstoning, because Enterprise does not support expire
// either, so a `remove-snapshots` commit stages no tombstones at all. Expire therefore leaves
// entries behind; they point at objects that only the committed GC can collect, which is the
// conservative direction.
//
// # The foreign-location check is a security control
//
// Every path a new snapshot references - its manifest list, every manifest in the list (not only
// the added ones) and every file those manifests add - must live under the table's root, i.e.
// under the location metadata.json declares:
//
//	<storage_namespace>/_iceberg/<table-uuid>/        (the default location, Q-I5)
//	<storage_namespace>/_iceberg/<client-chosen>/     (a location the create request carried)
//
// and that root must itself be inside <storage_namespace>/_iceberg/, the catalog's table area. A
// snapshot that references anything else is refused with 400 before anything is staged (§13
// "Client writes outside the table location"). This is not an optimisation. The catalog reads
// manifest lists and manifests through the server's block adapter, i.e. with the server's own
// credentials, so without the check a client could commit a snapshot whose manifest list points at
// `s3://another-team/secret/...` and make this server read another bucket. It also keeps a table's
// files in one place, which is what the staging keys, the GC window and the merge semantics of
// §6.4 assume.
//
// The root is the metadata's location and not a value derived from the table UUID, because the
// create path lets a client choose a location inside the table area (location.go
// normalizeTableLocation). Deriving the root from the UUID instead would refuse to commit every
// table whose location is not the default one, even though its files are exactly where the catalog
// put them.
//
// Tables created by this catalog always satisfy the check: the create path places a table inside
// <storage_namespace>/_iceberg/ - the managed <storage_namespace>/_iceberg/<table-uuid> (Q-I5,
// task I2-4) unless the request carried a location of its own - and set-location is refused
// (commit.go §6.2 step 3). A table registered from another catalog may not; such a table can be
// read, but a commit that adds a snapshot to it is refused until registration supports external
// locations (out of scope for v1, §13).
//
// # Cost and batching
//
// Spike S-I2 §5.1 is the source of the numbers and of the recommendations implemented here:
//
//   - staging is one catalog.CreateEntry per file, in a bounded worker pool (16-32 goroutines
//     measured at 4.2-4.8 k entries/s: a 10 k-file append stages in ~2.3 s instead of the ~76 s a
//     sequential loop takes, because each CreateEntry pays graveler's 3 ms max_batch_delay);
//   - the batched WriteRange + Store.Import path is *not* used: it creates its own commit and takes
//     no graveler.WithCondition, so it would bypass the pointer compare-and-swap of §6.2 step 6 and
//     break the single-commit and 409-on-race guarantees (S-I2 §2.4);
//   - entries carry the file's size and the timestamp of the snapshot that added it, and never a
//     HEAD: the creation date is what the GC job ages staged objects by (gc.go's
//     UncommittedGCProtected), and an existence or metadata check per file would cost one request
//     per file (S-I2 §5.1 items 3-4);
//   - a hard cap on the files of one commit (DefaultMaxStagedFiles) refuses an oversized commit
//     with 400 instead of silently skipping files, because a skipped file is a GC-deletable file
//     (S-I2 §5.1 item 4). The cap belongs to config as iceberg.max_staged_files (§10) and reaches
//     this code as StageTableFilesRequest.MaxFiles, which the commit path fills from the Store's
//     configured cap (store.go WithMaxStagedFiles).
//
// # Wiring
//
// StageTableFiles is the whole of §6.2 step 5 and nothing else, so the commit path calls it between
// the metadata write (step 4) and the pointer swap (step 6), with the cap the Store was built with:
//
//	files, err := s.StageTableFiles(ctx, addr, StageTableFilesRequest{
//	    Base:          base.Metadata,
//	    Next:          next,
//	    MetadataFiles: []TableMetadataFile{{Location: location, Size: int64(len(metadataJSON))}},
//	    MaxFiles:      s.maxStagedFiles,
//	})
//
// and reports a *ForeignTableFileError or a *TableFileLimitError as the 400 it is, and any other
// failure as CommitStateUnknownError, because it happens after metadata.json was written and before
// the pointer swap (§6.2 step 6). The call lives in commit.go (Store.Commit).
//
// Licensing: this file is a clean-room implementation from the Apache Iceberg table spec
// (Apache-2.0), https://iceberg.apache.org/spec/ - "Table Metadata", "Snapshots", "Manifests" and
// "Manifest Lists" - and the Apache Iceberg REST OpenAPI spec (Apache-2.0),
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml. Task I3-1. No
// code is taken from lakeFS Enterprise or from upstream lakeFS >= 1.87 (BUSL 1.1).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/iceberglayout"
	"golang.org/x/sync/errgroup"
)

const (
	// FilesDirectory is the subdirectory of the reserved prefix that holds one entry per table
	// file (ICEBERG_CATALOG_PLAN.md §6.3):
	//
	//	_lakefs_iceberg/files/<table-uuid>/<path relative to the table root>
	//
	// It is the third directory of the reserved area, next to NamespaceDirectory ("ns") and
	// TablesDirectory ("tables"). Like them it is declared in the leaf package iceberglayout,
	// which pkg/datasets imports as well, and aliased here (see the note on store.go's layout
	// const block).
	FilesDirectory = iceberglayout.FilesDirectory

	// DefaultMaxStagedFiles is the default cap on the number of entries one commit may stage:
	// 50 000 files is ~6 s of staging at 32 workers over a KV with a 1 ms round trip and ~12 s at
	// the in-process 4.5 k entries/s (spike S-I2 §5.1 item 4). §10 spells the knob as
	// iceberg.max_staged_files; the configured value reaches this code through
	// StageTableFilesRequest.MaxFiles.
	DefaultMaxStagedFiles = 50_000

	// DefaultStageWorkers is the default number of concurrent catalog.CreateEntry calls: 32 is
	// the measured optimum of spike S-I2 §2.2 (4.2-4.8 k entries/s, 8 workers already give 1 k/s,
	// and 32 fills graveler's 3 ms batching windows without flooding the KV).
	DefaultStageWorkers = 32

	// unknownAddedSnapshotID is the added-snapshot-id a v1 manifest list carries when the field is
	// absent: the spec allows it to be null, and iceberg-go decodes the absence as -1. A manifest
	// whose added-snapshot-id is unknown is treated as added by the snapshot whose list it is in,
	// because a v1 list has no other way to say so.
	unknownAddedSnapshotID = -1
)

// TableFileKind is what kind of Iceberg file an entry registers. It is the vocabulary of §6.3
// ("one entry per metadata, manifest-list, manifest and data file") and it is carried on every
// TableFile and on the error a refused path produces, so a rejection says which file of the
// snapshot was outside the table.
type TableFileKind string

const (
	// TableFileKindMetadata is a table metadata file (metadata.json).
	TableFileKindMetadata TableFileKind = "metadata"

	// TableFileKindManifestList is a snapshot's manifest list (the Avro file the snapshot's
	// manifest-list field names, spec: "Manifest Lists").
	TableFileKindManifestList TableFileKind = "manifest-list"

	// TableFileKindManifest is a manifest file (spec: "Manifests").
	TableFileKindManifest TableFileKind = "manifest"

	// TableFileKindData is a file a manifest entry points at: a data file, or a v2 position or
	// equality delete file. Iceberg calls both "data files" in the manifest-entry sense ("Data
	// File" in the spec), so they share one kind.
	TableFileKindData TableFileKind = "data"
)

var (
	// ErrForeignTableFile is reported by ForeignTableFileError: a snapshot references a file that
	// is not under the table's root. It is a 400 BadRequestException - the security control of
	// §13, not an optimisation - and it is always raised during planning, so a refused snapshot
	// stages nothing.
	ErrForeignTableFile = errors.New("table file outside the table location")

	// ErrTooManyTableFiles is reported by TableFileLimitError: the commit would stage more files
	// than the cap allows. It is a 400 BadRequestException: retrying the same commit cannot help,
	// and silently staging a subset would leave the rest collectable by GC (spike S-I2 §5.1
	// item 4).
	ErrTooManyTableFiles = errors.New("too many table files in one commit")

	// ErrTableFileRegistration is wrapped by every other failure of this file: a repository that
	// cannot be resolved, a manifest list or manifest that cannot be read or parsed. Nothing on
	// this path is the client's fault, so it is a 5xx; because it happens after metadata.json was
	// written and before the pointer swap, the commit path reports it as
	// CommitStateUnknownException (§6.2 step 6).
	ErrTableFileRegistration = errors.New("iceberg table file registration failed")

	// ErrTableFileRelease is wrapped by every failure of the release half of this file: the file
	// entries of a table that is being dropped could not be listed or removed. It is a 5xx - nothing
	// on this path is the client's fault - and it is reported rather than skipped, because a drop
	// that silently removed only some of the entries would leave exactly the half-released table
	// this path exists to avoid.
	ErrTableFileRelease = errors.New("iceberg table file release failed")

	// ErrTableLocationOutsideArea is attached to a *ForeignTableFileError when the table's own
	// location is outside <storage_namespace>/_iceberg/. Such a table can be read, but a commit
	// that would stage its files is refused with 400 (§13: registration of external locations is
	// out of scope for v1).
	ErrTableLocationOutsideArea = errors.New("the table location is outside the catalog's table area")
)

// TableFile is one lakeFS entry to stage: a file of a table snapshot, promoted to an entry under
// the reserved prefix.
type TableFile struct {
	// Kind is what kind of Iceberg file this is.
	Kind TableFileKind

	// Path is the lakeFS entry key under the reserved prefix:
	// _lakefs_iceberg/files/<table-uuid>/<path relative to the table root>. It is deterministic,
	// so a retried commit overwrites the same key instead of creating a second entry (S-I2 §5.1
	// item 5).
	Path string

	// PhysicalAddress is the fully qualified URI the client wrote, i.e. the object the entry
	// points at. AddressType is Full: the object is shared by every branch, which is what makes a
	// merge of two branches' pointers correct (§6.3).
	PhysicalAddress string

	// Size is the object's size in bytes: the value the manifest list carries for a manifest, the
	// manifest entry for a data file, and the bytes written for a metadata file.
	Size int64

	// LastModified is when the file was added: the timestamp of the snapshot that added it, or the
	// metadata's last-updated-ms for a metadata file. The uncommitted GC ages staged objects by
	// this value (gc.go, UncommittedGCProtected), so it is read from the metadata and never from a
	// HEAD (S-I2 §5.1 item 3).
	LastModified time.Time
}

// TableMetadataFile is one metadata.json a commit wrote, which §6.3 registers like any other table
// file. The commit path knows both fields: it wrote the file and serialized the bytes (§6.2
// step 4).
type TableMetadataFile struct {
	// Location is the fully qualified URI of the metadata file.
	Location string

	// Size is the number of bytes written.
	Size int64
}

// StageTableFilesRequest is the input of file registration: the metadata the commit started from,
// the metadata it produced, and the files it wrote. Everything else is read from the object store.
type StageTableFilesRequest struct {
	// Base is the table metadata the commit read in §6.2 step 1. It is only used to tell which
	// snapshots the commit added; it may be nil for a commit that creates the table, in which case
	// every snapshot of Next counts as new.
	Base table.Metadata

	// Next is the table metadata after the updates were applied (§6.2 step 4). Its table-uuid and
	// last-updated-ms are used, and its new snapshots are what gets registered.
	Next table.Metadata

	// MetadataFiles are the metadata files the commit wrote, in write order. A commit writes one;
	// the slice exists because that is a property of the commit path, not of this function.
	MetadataFiles []TableMetadataFile

	// MaxFiles caps the number of entries this commit may stage. Zero means DefaultMaxStagedFiles.
	// A commit above the cap is refused with TableFileLimitError before anything is staged.
	MaxFiles int

	// Workers bounds the concurrent catalog.CreateEntry calls. Zero means DefaultStageWorkers.
	Workers int
}

// TableFileSet is the outcome of one file-registration step: the entries the snapshot's files
// become, in a deterministic order.
type TableFileSet struct {
	// TableRoot is the physical root every registered file lives under: the location metadata.json
	// declares, <storage_namespace>/_iceberg/<table-uuid> for a table created without one.
	TableRoot string

	// SnapshotIDs are the snapshots of this commit that were registered, ascending. It is empty
	// for a commit that added no snapshot - a properties change, say - which stages the metadata
	// files and nothing else.
	SnapshotIDs []int64

	// Files are the entries to stage, sorted by Path and deduplicated. It is the plan: it is
	// complete before the first write, so a refused plan stages nothing.
	Files []TableFile

	// ManifestsSkipped is the number of manifests in the new snapshots' manifest lists that
	// another snapshot added, i.e. that an earlier commit already registered.
	ManifestsSkipped int

	// EntriesSkipped is the number of manifest entries that are not ADDED: files the snapshot
	// kept (already registered) or removed (Q-I8 keeps files, so nothing is tombstoned).
	EntriesSkipped int

	// Staged is the number of entries StageTableFiles actually wrote; a plan has 0.
	Staged int
}

// ForeignTableFileError is a snapshot that references a file outside its table's physical root.
// It is the §13 security control: the catalog reads manifest lists and manifests with the server's
// credentials, so a path outside the table root would make it read another bucket, and a staged
// entry pointing outside the namespace would be invisible to the uncommitted GC (spike S-I2 §4).
// It is also how a table whose own location is outside <storage_namespace>/_iceberg/ is refused
// when a commit would stage its files.
//
// It is a 400 BadRequestException. The reference counts even when the file would not have been
// read: every manifest of the new snapshot's manifest list is checked, not only the added ones.
type ForeignTableFileError struct {
	// Kind is what kind of file was outside the root.
	Kind TableFileKind

	// Location is the path the snapshot referenced, verbatim.
	Location string

	// Root is the table's physical root: the location metadata.json declares, or
	// <storage_namespace>/_iceberg/<table-uuid> for a table created without one.
	Root string

	// Err is set when the path is under the root but is not a usable entry path - a segmented path
	// that climbs out with "..", an empty segment, a control character - and nil when it is simply
	// outside the root.
	Err error
}

// Error implements error.
func (e *ForeignTableFileError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s %q under %q: %v", ErrForeignTableFile, e.Kind, e.Location, e.Root, e.Err)
	}
	return fmt.Sprintf("%s: %s %q is not under %q", ErrForeignTableFile, e.Kind, e.Location, e.Root)
}

// Unwrap reports ErrForeignTableFile, ErrCommitRejected (this is a refused commit) and the
// underlying reason when there is one.
func (e *ForeignTableFileError) Unwrap() error {
	return errors.Join(ErrForeignTableFile, ErrCommitRejected, e.Err)
}

// HTTPStatusCode is the status of the refusal, 400.
func (e *ForeignTableFileError) HTTPStatusCode() int { return http.StatusBadRequest }

// ErrorType is the Iceberg error type of the refusal.
func (e *ForeignTableFileError) ErrorType() string { return ErrTypeBadRequest }

// TableFileLimitError is a commit that would stage more files than the cap allows. It is a 400
// BadRequestException: the cap is a configuration limit, not a race, so retrying the same commit
// cannot succeed (spike S-I2 §5.1 item 4).
type TableFileLimitError struct {
	// Count is the number of files the commit tried to register.
	Count int

	// Max is the configured cap (iceberg.max_staged_files).
	Max int
}

// Error implements error.
func (e *TableFileLimitError) Error() string {
	return fmt.Sprintf("%s: %d files, and the limit is %d (iceberg.max_staged_files)",
		ErrTooManyTableFiles, e.Count, e.Max)
}

// Unwrap reports ErrTooManyTableFiles and ErrCommitRejected.
func (e *TableFileLimitError) Unwrap() error {
	return errors.Join(ErrTooManyTableFiles, ErrCommitRejected)
}

// HTTPStatusCode is the status of the refusal, 400.
func (e *TableFileLimitError) HTTPStatusCode() int { return http.StatusBadRequest }

// ErrorType is the Iceberg error type of the refusal.
func (e *TableFileLimitError) ErrorType() string { return ErrTypeBadRequest }

// TableRoot returns the default physical root of a table's files (ICEBERG_CATALOG_PLAN.md §6.3,
// §10 iceberg.uncommitted_gc_grace):
//
//	<storage_namespace>/_iceberg/<table-uuid>
//
// with <storage_namespace> in the form the catalog hands out - the Iceberg abfs[s] form on an Azure
// blockstore (azure_location.go D1), the namespace itself everywhere else - so that it names the
// same directory as the location metadata.json declares.
//
// It is the location this catalog gives a table that the create request did not place itself
// (Q-I5). File registration uses the root the table itself declares (tableRootForMetadata), which
// is this value for a defaulted table and the client's location for a table created with one.
// The storage namespace is used without a trailing slash, because that is the form the location of
// a defaulted table has.
func TableRoot(storageNamespace string, tableUUID uuid.UUID) (string, error) {
	namespace := strings.TrimRight(storageNamespace, "/")
	if namespace == "" {
		return "", fmt.Errorf("%w: the repository has no storage namespace to stage table files under",
			ErrTableFileRegistration)
	}
	if tableUUID == uuid.Nil {
		return "", fmt.Errorf("%w: the table has no UUID to stage table files under",
			ErrTableFileRegistration)
	}
	return tableAreaNamespace(namespace) + "/" + DataPrefix + "/" + tableUUID.String(), nil
}

// TableFilePath returns the lakeFS entry key of a table file:
//
//	_lakefs_iceberg/files/<table-uuid>/<relativePath>
//
// relativePath is the file's path relative to TableRoot, and it is validated here rather than
// trusted: the key is built from a client-controlled manifest, so a segment that is "..", empty or
// carries a control character must not be able to reach outside the table's directory in the
// reserved area.
func TableFilePath(tableUUID uuid.UUID, relativePath string) (string, error) {
	if tableUUID == uuid.Nil {
		return "", fmt.Errorf("%w: the table has no UUID to name a table file entry after",
			ErrTableFileRegistration)
	}
	if err := validateTableRelativePath(relativePath); err != nil {
		return "", err
	}
	key := ReservedPrefix + "/" + FilesDirectory + "/" + tableUUID.String() + "/" + relativePath
	if err := catalog.ValidatePath(catalog.Path(key)); err != nil {
		return "", fmt.Errorf("%w: %q is not a usable entry key: %w", ErrTableFileRegistration, key, err)
	}
	return key, nil
}

// tableFilesPrefix returns the reserved key prefix that holds every file entry of one table:
//
//	_lakefs_iceberg/files/<table-uuid>/
//
// It is the parent of every key TableFilePath builds for the table, and it is spelled from the same
// uuid and the same constants, so the directory a drop empties (releaseTableFiles) is the directory
// every commit of that table staged into by construction rather than by convention. The trailing
// slash is what keeps one table's uuid from being a prefix of another's.
func tableFilesPrefix(tableUUID uuid.UUID) (string, error) {
	if tableUUID == uuid.Nil {
		return "", fmt.Errorf("%w: the table has no UUID to release the table files of", ErrTableFileRelease)
	}
	return ReservedPrefix + "/" + FilesDirectory + "/" + tableUUID.String() + "/", nil
}

// tableRootForMetadata resolves the physical root every file of a commit must live under: the
// table's own location when metadata.json declares one, and the managed
// <storage_namespace>/_iceberg/<table-uuid> when it does not.
//
// The location is checked against the catalog's table area (<storage_namespace>/_iceberg/) rather
// than trusted, because it comes from a document a client wrote. A table whose location is outside
// the area - one registered from another catalog, say - is readable, but a commit that would stage
// its files is refused with a *ForeignTableFileError: registering files under a root the catalog
// does not own would stage entries pointing outside the repository's storage namespace, which the
// uncommitted GC cannot see (spike S-I2 §4) and which §13 forbids.
func tableRootForMetadata(storageNamespace string, meta table.Metadata) (string, error) {
	defaultRoot, err := TableRoot(storageNamespace, meta.TableUUID())
	if err != nil {
		return "", err
	}
	location := strings.TrimRight(meta.Location(), "/")
	if location == "" || location == defaultRoot {
		return defaultRoot, nil
	}
	area := TableAreaLocation(storageNamespace)
	relative, ok := strings.CutPrefix(location, area)
	if !ok {
		// An Azure table created before the location translation keeps its files under the
		// namespace spelling of the area (D2's read/commit tolerance): its location is accepted so
		// that a commit can still register its files, and the root stays in that spelling so the
		// manifest entries under it are matched by the collector.
		if legacyArea := legacyTableAreaLocation(storageNamespace); legacyArea != "" {
			relative, ok = strings.CutPrefix(location, legacyArea)
		}
	}
	if !ok || relative == "" {
		return "", &ForeignTableFileError{
			Kind:     TableFileKindMetadata,
			Location: location,
			Root:     strings.TrimRight(area, "/"),
			Err:      ErrTableLocationOutsideArea,
		}
	}
	if err := validateTableRelativePath(strings.TrimRight(relative, "/")); err != nil {
		return "", &ForeignTableFileError{
			Kind:     TableFileKindMetadata,
			Location: location,
			Root:     strings.TrimRight(area, "/"),
			Err:      err,
		}
	}
	return location, nil
}

// PlanTableFiles collects the entries a commit's new snapshots need, without writing anything: it
// reads the manifest lists and manifests through the block adapter and returns the plan in a
// deterministic order.
//
// Planning is separate from staging so that every rejection - a foreign location, an oversized
// commit, an unreadable manifest - happens before the first entry is written (§6.3 step 2 of the
// ordered proposal in spike S-I2 §5.1), and so an operator tool or a test can see what a commit
// would register.
func (s *Store) PlanTableFiles(ctx context.Context, addr Address, request StageTableFilesRequest) (*TableFileSet, error) {
	if addr.Repo == "" {
		return nil, fmt.Errorf("%w: the repository is empty", ErrInvalidIdentifier)
	}
	if addr.Ref == "" {
		return nil, fmt.Errorf("%w: the ref is empty", ErrInvalidIdentifier)
	}
	if request.Next == nil {
		return nil, fmt.Errorf("%w: the commit has no table metadata to register files for",
			ErrTableFileRegistration)
	}
	repo, err := s.catalog.GetRepository(ctx, addr.Repo)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve repository %q: %w", ErrTableFileRegistration, addr.Repo, err)
	}
	tableUUID := request.Next.TableUUID()
	root, err := tableRootForMetadata(repo.StorageNamespace, request.Next)
	if err != nil {
		return nil, err
	}

	maxFiles := request.MaxFiles
	if maxFiles <= 0 {
		maxFiles = DefaultMaxStagedFiles
	}
	collector := &tableFileCollector{
		tableUUID:        tableUUID,
		root:             root,
		storageNamespace: repo.StorageNamespace,
		maxFiles:         maxFiles,
		files:            make(map[string]TableFile),
	}
	set := &TableFileSet{TableRoot: root}

	// Step 1 of §6.3: the metadata.json the commit wrote. Its last-updated-ms is the moment the
	// file was written, which is the closest thing to an object write time we have without a HEAD.
	metadataTime := time.UnixMilli(request.Next.LastUpdatedMillis()).UTC()
	for _, metadataFile := range request.MetadataFiles {
		if err := collector.add(TableFileKindMetadata, metadataFile.Location, metadataFile.Size, metadataTime); err != nil {
			return nil, err
		}
	}

	// Steps 2-3: the snapshots this commit added, ascending by id so the plan is stable.
	for _, snapshot := range newTableSnapshots(request.Base, request.Next) {
		set.SnapshotIDs = append(set.SnapshotIDs, snapshot.SnapshotID)
		if err := s.collectSnapshotFiles(ctx, repo, request.Next, snapshot, collector); err != nil {
			return nil, err
		}
	}

	set.Files = collector.sorted()
	set.ManifestsSkipped = collector.manifestsSkipped
	set.EntriesSkipped = collector.entriesSkipped
	return set, nil
}

// StageTableFiles registers the files of a commit's new snapshots as lakeFS entries on addr.Ref,
// implementing §6.2 step 5 and the whole of §6.3: plan first (PlanTableFiles), then stage every
// planned entry, then return.
//
// The writes are ordinary catalog.CreateEntry calls in a bounded worker pool, with no
// graveler.WithForce, so a protected branch or a read-only (datasets) repository rejects them
// exactly like any other catalog write. They are staged, never committed: one lakeFS commit on the
// branch publishes the files and the table pointer together (§6.2 "why stage instead of
// auto-commit").
//
// Errors: a *ForeignTableFileError or a *TableFileLimitError is the 400 it says it is, and both are
// raised before anything is staged. Anything else is a wrapped ErrTableFileRegistration and happens
// after metadata.json was written and before the pointer swap, so the commit path reports it as
// CommitStateUnknownError (§6.2 step 6).
func (s *Store) StageTableFiles(ctx context.Context, addr Address, request StageTableFilesRequest) (*TableFileSet, error) {
	set, err := s.PlanTableFiles(ctx, addr, request)
	if err != nil {
		return nil, err
	}
	if err := s.stageTableFiles(ctx, addr, set.Files, request.Workers); err != nil {
		return nil, err
	}
	set.Staged = len(set.Files)
	return set, nil
}

// stageTableFiles writes the planned entries in a bounded worker pool (spike S-I2 §2.2: the
// per-call cost is graveler's batching window, not the KV, so concurrency is what makes a 10 k-file
// append take seconds instead of a minute).
//
// A failure cancels the remaining writes: a partially staged commit is harmless, because the
// entries it did write are deterministic - a retry overwrites them - and the ones it did not are
// either staged by the retry or collected by the uncommitted GC (spike S-I2 §5.1 item 5).
func (s *Store) stageTableFiles(ctx context.Context, addr Address, files []TableFile, workers int) error {
	if len(files) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = DefaultStageWorkers
	}
	if workers > len(files) {
		workers = len(files)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(workers)
	for _, file := range files {
		group.Go(func() error {
			entry := catalog.DBEntry{
				Path:            file.Path,
				PhysicalAddress: file.PhysicalAddress,
				// Full, not Relative: the object is the one the client wrote, under the storage
				// namespace, and its address is stable across branches (§6.3).
				AddressType: catalog.AddressTypeFull,
				// The snapshot timestamp stands in for the object's write time: the uncommitted GC
				// ages staged entries by it, and a HEAD per file is not affordable (S-I2 §5.1
				// item 3).
				CreationDate: file.LastModified,
				Size:         file.Size,
				// No Checksum: an Iceberg manifest carries no ETag, and computing one would mean
				// reading every file.
			}
			// No graveler.WithForce: branch protection and repository immutability apply.
			if err := s.catalog.CreateEntry(groupCtx, addr.Repo, addr.Ref, entry); err != nil {
				return fmt.Errorf("%w: stage %s %q: %w", ErrTableFileRegistration, file.Kind, file.Path, err)
			}
			return nil
		})
	}
	// Errgroup.SetLimit bounds the goroutines, so this only waits for the writes in flight.
	if err := group.Wait(); err != nil {
		return err
	}
	return nil
}

// releaseTableFiles stages the removal of every table-file entry of one table from addr.Ref: the
// reverse of StageTableFiles, and the half of the drop that hands the table's objects back to the
// committed GC (files.go file comment "What a drop releases", tables.go dropTable, RESULTS.md N-3).
//
// It walks exactly the table's own directory, _lakefs_iceberg/files/<table-uuid>/ (§6.3), one page
// at a time: the keys are the ones the table's own commits staged, so a sibling table, the namespace
// markers and the table pointers are never in reach, and no object is deleted - the entries carry
// AddressType=Full, and the objects they point at stay readable through every commit, tag and
// branch that still references them (§14 Q-I8).
//
// The caller must already have removed the pointer (tables.go dropTable): the pointer is what makes
// the table exist, so a failure between the two steps has to leave files that are still protected
// rather than a table whose files are no longer protected.
//
// A listed entry that is already gone is not an error - another writer, or an earlier attempt of
// this same drop, released it, and the outcome the release asked for is the entry being absent.
func releaseTableFiles(ctx context.Context, lister TableLister, addr Address, tableUUID uuid.UUID, workers int) (int, error) {
	prefix, err := tableFilesPrefix(tableUUID)
	if err != nil {
		return 0, err
	}
	released := 0
	after := ""
	for {
		// A page, cursored by the last path listed rather than by an offset: the staging writes below
		// remove entries, so the only stable cursor is "the last key I saw". The staging area is read
		// first (TableLister.ListEntries), so an entry this drop already staged as deleted is either
		// gone from the next page or tolerated below.
		entries, hasMore, err := lister.ListEntries(ctx, addr.Repo, addr.Ref, prefix, after, "",
			catalog.ListEntriesLimitMax)
		if err != nil {
			return released, fmt.Errorf("%w: list the table files of %s@%s under %q: %w",
				ErrTableFileRelease, addr.Repo, addr.Ref, prefix, err)
		}
		if len(entries) == 0 {
			return released, nil
		}
		count, err := deleteTableFileEntries(ctx, lister, addr, entries, workers)
		released += count
		if err != nil {
			return released, err
		}
		if !hasMore {
			return released, nil
		}
		after = entries[len(entries)-1].Path
	}
}

// deleteTableFileEntries stages the removal of one page of table-file entries in a bounded worker
// pool, the same way and for the same reason as stageTableFiles: a catalog.DeleteEntry pays
// graveler's batching window (3 ms), so a table whose commits staged thousands of files needs the
// pool to be released in seconds rather than minutes (spike S-I2 §2.2).
//
// It returns how many entries it removed, which is what releaseTableFiles reports, so a test or a
// log can tell a released table from a table that never staged anything.
func deleteTableFileEntries(ctx context.Context, lister TableLister, addr Address, entries []*catalog.DBEntry, workers int) (int, error) {
	if workers <= 0 {
		workers = DefaultStageWorkers
	}
	if workers > len(entries) {
		workers = len(entries)
	}
	var released atomic.Int64
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(workers)
	for _, entry := range entries {
		group.Go(func() error {
			if entry.CommonLevel {
				// A common prefix is a directory of the listing, not an entry a delete could remove.
				// The release lists without a delimiter, so this is a guard rather than a case.
				return nil
			}
			if err := lister.DeleteEntry(groupCtx, addr.Repo, addr.Ref, entry.Path); err != nil {
				if errors.Is(err, graveler.ErrNotFound) {
					return nil
				}
				return fmt.Errorf("%w: release %q: %w", ErrTableFileRelease, entry.Path, err)
			}
			released.Add(1)
			return nil
		})
	}
	// A failure cancels the remaining deletes, exactly like a failed staging write: the entries it did
	// not remove still protect their objects, which is the conservative direction, and the failure is
	// reported rather than swallowed.
	if err := group.Wait(); err != nil {
		return int(released.Load()), err
	}
	return int(released.Load()), nil
}

// collectSnapshotFiles adds the manifest list, the added manifests and their added files of one new
// snapshot (§6.3 steps 1-3).
func (s *Store) collectSnapshotFiles(ctx context.Context, repo *catalog.Repository, meta table.Metadata, snapshot table.Snapshot, collector *tableFileCollector) error {
	// The snapshot's timestamp is when its files were written, which is the creation date the
	// entries get; a manifest carries no per-file write time (S-I2 §5.1 item 3).
	modified := time.UnixMilli(snapshot.TimestampMs).UTC()

	manifests, err := s.snapshotManifests(ctx, repo, meta, snapshot, collector, modified)
	if err != nil {
		return err
	}

	// Every manifest the snapshot's list names must be under the table root, whether or not this
	// commit added it and whether or not it will be read: the list is client-supplied, and a
	// manifest outside the root is the §13 attack even before anything is read.
	for _, manifest := range manifests {
		if err := collector.validate(TableFileKindManifest, manifest.FilePath()); err != nil {
			return err
		}
	}

	for _, manifest := range manifests {
		if !manifestAddedBySnapshot(manifest, snapshot.SnapshotID) {
			// An earlier commit added it, and an earlier commit registered its files.
			collector.manifestsSkipped++
			continue
		}
		if err := s.collectManifestFiles(ctx, repo, manifest, collector, modified); err != nil {
			return err
		}
	}
	return nil
}

// snapshotManifests resolves the manifest list of a snapshot and returns the manifests it names,
// registering the manifest-list file itself (§6.3 step 1).
//
// Two forms exist and both are served (§14 Q-I8 and §1.2: format versions 1 and 2):
//
//   - a manifest list named by the snapshot's manifest-list field. It may be in a manifest of its
//     own or not; either way it is checked against the table root *before* it is read, so a foreign
//     list is never fetched with the server's credentials.
//   - the v1 form, where the snapshot carries the manifest locations inline (the deprecated
//     "manifests" field). There is no manifest-list object then, and no manifest-list entry to
//     stage; the locations are wrapped in a minimal v1 ManifestFile so that the same reader code
//     serves both forms. Such a list carries no partition-spec id, which the manifest reader only
//     uses to label entries, so the table's default spec id is used.
func (s *Store) snapshotManifests(ctx context.Context, repo *catalog.Repository, meta table.Metadata, snapshot table.Snapshot, collector *tableFileCollector, modified time.Time) ([]iceberg.ManifestFile, error) {
	if snapshot.ManifestList == "" {
		// No object is read here, and collectSnapshotFiles validates every one of these locations
		// before it reads anything, so wrapping them is all this branch has to do.
		specID, err := partitionSpecID(meta.DefaultPartitionSpec())
		if err != nil {
			return nil, err
		}
		manifests := make([]iceberg.ManifestFile, 0, len(snapshot.ManifestLocations))
		for _, location := range snapshot.ManifestLocations {
			manifests = append(manifests, iceberg.NewManifestFile(
				1, location, 0, specID, snapshot.SnapshotID).Build())
		}
		return manifests, nil
	}

	// Validate before reading: the check is the reason a foreign path is never fetched.
	if err := collector.validate(TableFileKindManifestList, snapshot.ManifestList); err != nil {
		return nil, err
	}
	data, err := s.readTableObject(ctx, repo, snapshot.ManifestList)
	if err != nil {
		return nil, err
	}
	if err := collector.add(TableFileKindManifestList, snapshot.ManifestList, int64(len(data)), modified); err != nil {
		return nil, err
	}
	manifests, err := iceberg.ReadManifestList(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: parse manifest list %q: %w", ErrTableFileRegistration, snapshot.ManifestList, err)
	}
	return manifests, nil
}

// collectManifestFiles registers one manifest and the files it adds (§6.3 steps 2-3). Entries that
// are not ADDED are skipped: EXISTING files were registered by the commit that added them, and
// DELETED files are not tombstoned because v1 keeps files on expire (Q-I8).
func (s *Store) collectManifestFiles(ctx context.Context, repo *catalog.Repository, manifest iceberg.ManifestFile, collector *tableFileCollector, modified time.Time) error {
	location := manifest.FilePath()
	// Validate before reading, as for the manifest list.
	if err := collector.validate(TableFileKindManifest, location); err != nil {
		return err
	}
	data, err := s.readTableObject(ctx, repo, location)
	if err != nil {
		return err
	}
	size := manifest.Length()
	if size <= 0 {
		// A v1 inline manifest has no length in the metadata; the bytes we just read are the size.
		size = int64(len(data))
	}
	if err := collector.add(TableFileKindManifest, location, size, modified); err != nil {
		return err
	}

	reader, err := iceberg.NewManifestReader(manifest, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("%w: read manifest %q: %w", ErrTableFileRegistration, location, err)
	}
	defer func() { _ = reader.Close() }()

	for {
		entry, err := reader.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("%w: read an entry of manifest %q: %w", ErrTableFileRegistration, location, err)
		}
		if entry.Status() != iceberg.EntryStatusADDED {
			collector.entriesSkipped++
			continue
		}
		dataFile := entry.DataFile()
		// Delete files are data files in the manifest-entry sense (spec: "Data File"), and a
		// snapshot that adds one needs it registered just as much as a data file.
		if err := collector.add(TableFileKindData, dataFile.FilePath(), dataFile.FileSizeBytes(), modified); err != nil {
			return err
		}
	}
	return nil
}

// readTableObject reads one object of the table through the block adapter, as a fully qualified
// identifier against the repository's storage namespace - the same resolution the pointer's
// metadata.json gets (store.go loadMetadata).
//
// The caller has already checked that the location is under the table root, which is what keeps
// this from reading another bucket with the server's credentials (§13). The identifier is the
// location in the block adapter's own spelling (D2, location.go blockIdentifier): on an Azure
// blockstore the location is abfs[s]:// and the adapter reads https://.
func (s *Store) readTableObject(ctx context.Context, repo *catalog.Repository, location string) ([]byte, error) {
	reader, err := s.block.Get(ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       blockIdentifier(repo.StorageNamespace, location),
		IdentifierType:   block.IdentifierTypeFull,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: read %q: %w", ErrTableFileRegistration, location, err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: read %q: %w", ErrTableFileRegistration, location, err)
	}
	return data, nil
}

// tableFileCollector accumulates the plan: one entry per file, deduplicated by entry key, capped,
// and with the two counters that explain what was not registered.
type tableFileCollector struct {
	tableUUID uuid.UUID
	root      string
	// storageNamespace is the repository's namespace in the block adapter's spelling; add turns a
	// validated Iceberg location into the physical address the adapter can read (D2).
	storageNamespace string
	maxFiles         int
	files            map[string]TableFile

	manifestsSkipped int
	entriesSkipped   int
}

// validate reports whether a path a snapshot references may be registered: it must be under the
// table root and its relative path must be usable as a lakeFS key (§13).
func (c *tableFileCollector) validate(kind TableFileKind, location string) error {
	if _, err := c.relativePath(kind, location); err != nil {
		return err
	}
	return nil
}

// relativePath returns the path of location relative to the table root, or a
// *ForeignTableFileError.
func (c *tableFileCollector) relativePath(kind TableFileKind, location string) (string, error) {
	prefix := strings.TrimRight(c.root, "/") + "/"
	if !strings.HasPrefix(location, prefix) {
		return "", &ForeignTableFileError{Kind: kind, Location: location, Root: c.root}
	}
	relative := location[len(prefix):]
	if err := validateTableRelativePath(relative); err != nil {
		// The path carries the root as a string prefix but does not resolve to a file under it - a
		// ".." segment climbs out, an empty segment re-enters - so it is refused like an outside
		// path, with the reason attached.
		return "", &ForeignTableFileError{Kind: kind, Location: location, Root: c.root, Err: err}
	}
	return relative, nil
}

// add registers one file in the plan: it validates the location, deduplicates the entry key and
// enforces the cap. A file that is already planned with the same address is the same object seen
// twice (the same data file in two manifests of one snapshot, say) and is a no-op; the same key with
// a different address cannot happen for a fixed root and is refused rather than silently preferred.
func (c *tableFileCollector) add(kind TableFileKind, location string, size int64, modified time.Time) error {
	relative, err := c.relativePath(kind, location)
	if err != nil {
		return err
	}
	entryPath, err := TableFilePath(c.tableUUID, relative)
	if err != nil {
		return err
	}
	if _, ok := c.files[entryPath]; ok {
		// The same file seen twice - in two manifests of one snapshot, say. For a fixed root the
		// mapping from location to entry key is one-to-one (the key is the location minus the root),
		// so the same key is necessarily the same object and the same entry.
		return nil
	}
	if len(c.files) >= c.maxFiles {
		// Fail before staging anything: a file silently left out is a file GC may delete (§13,
		// S-I2 §5.1 item 4).
		return &TableFileLimitError{Count: len(c.files) + 1, Max: c.maxFiles}
	}
	c.files[entryPath] = TableFile{
		Kind:            kind,
		Path:            entryPath,
		PhysicalAddress: blockIdentifier(c.storageNamespace, location),
		Size:            size,
		LastModified:    modified,
	}
	return nil
}

// sorted returns the plan ordered by entry key, so two runs over the same snapshot produce the same
// plan and a test (or a log) can pin it.
func (c *tableFileCollector) sorted() []TableFile {
	files := make([]TableFile, 0, len(c.files))
	for _, file := range c.files {
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files
}

// newTableSnapshots returns the snapshots of next that base does not have, ascending by id. That is
// exactly the set an `add-snapshot` update produced, without having to type-assert the unexported
// update structs of iceberg-go; a nil base (create table) makes every snapshot new.
func newTableSnapshots(base, next table.Metadata) []table.Snapshot {
	known := make(map[int64]struct{})
	if base != nil {
		for _, snapshot := range base.Snapshots() {
			known[snapshot.SnapshotID] = struct{}{}
		}
	}
	added := make([]table.Snapshot, 0)
	for _, snapshot := range next.Snapshots() {
		if _, ok := known[snapshot.SnapshotID]; !ok {
			added = append(added, snapshot)
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].SnapshotID < added[j].SnapshotID })
	return added
}

// manifestAddedBySnapshot reports whether manifest was added by the snapshot with snapshotID, i.e.
// whether the commit that created that snapshot wrote it (spec: "Manifest Lists", added-snapshot-id).
// A v1 manifest list may leave the field null, which iceberg-go decodes as -1; such a manifest is
// treated as added by the snapshot whose list names it, because that is the only reading a v1 list
// supports.
func manifestAddedBySnapshot(manifest iceberg.ManifestFile, snapshotID int64) bool {
	added := manifest.SnapshotID()
	return added == snapshotID || added == unknownAddedSnapshotID
}

// validateTableRelativePath checks that a path relative to a table root is usable as the leaf of a
// lakeFS entry key: non-empty, no empty, "." or ".." segment, no path separator other than "/", no
// control characters, and already in its clean form. Everything it rejects is a path that could
// escape the table's directory in the reserved area or name a key the catalog would not read back.
func validateTableRelativePath(relativePath string) error {
	if relativePath == "" {
		return fmt.Errorf("%w: the path is empty", ErrTableFileRegistration)
	}
	if path.Clean(relativePath) != relativePath {
		return fmt.Errorf("%w: %q is not a clean path", ErrTableFileRegistration, relativePath)
	}
	if strings.HasPrefix(relativePath, "/") || strings.HasSuffix(relativePath, "/") {
		return fmt.Errorf("%w: %q has an empty first or last segment", ErrTableFileRegistration, relativePath)
	}
	for _, segment := range strings.Split(relativePath, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("%w: %q has an empty, %q or %q segment", ErrTableFileRegistration, relativePath, ".", "..")
		}
		if strings.ContainsRune(segment, '\\') {
			return fmt.Errorf("%w: %q contains a path separator", ErrTableFileRegistration, relativePath)
		}
		if strings.IndexFunc(segment, unicode.IsControl) >= 0 {
			return fmt.Errorf("%w: %q contains a control character", ErrTableFileRegistration, relativePath)
		}
	}
	return nil
}

// partitionSpecID narrows the metadata's default partition-spec id to the int32 a synthesized
// ManifestFile takes. The id is a metadata field, and a value that does not fit is corrupt metadata
// rather than something this function could serve (manifests are identified by their spec id, so
// silently truncating it would mislabel entries).
func partitionSpecID(id int) (int32, error) {
	if id < 0 || id > math.MaxInt32 {
		return 0, fmt.Errorf("%w: partition spec id %d does not fit an Iceberg spec id", ErrTableFileRegistration, id)
	}
	return int32(id), nil
}
