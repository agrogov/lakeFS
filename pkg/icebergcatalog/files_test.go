package icebergcatalog

// Tests for table-file registration (ICEBERG_CATALOG_PLAN.md §6.3, §13, §14 Q-I8, §12 "unit
// tests"). The fixture is the real catalog store of store_test.go - graveler, committed ranges and
// an in-memory block adapter - so "the entries are staged on the branch, not committed", "the
// physical address is the object the client wrote" and "the plan is refused before anything is
// staged" are facts of the code the server runs.
//
// The Avro is written by iceberg-go itself: the manifests and manifest lists below are produced by
// iceberg.WriteManifest and iceberg.WriteManifestList, the same writers the dependency offers, and
// read back by the same readers files.go uses. That is deliberate - a hand-rolled Avro fixture would
// test the fixture rather than the manifest-list and manifest decoding of §6.3 steps 1-2.
//
// The metadata documents follow the examples of the Apache Iceberg table spec (Apache-2.0),
// https://iceberg.apache.org/spec/#iceberg-table-spec, including the v1 inline-manifest form of
// "Snapshots" so both manifest-list forms this package serves are covered.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	// filesTestSnapshotID is the snapshot a commit under test adds; filesTestParentSnapshotID is
	// the one it builds on, so that a manifest entry of an earlier snapshot can be written without
	// being mistaken for something this commit added.
	filesTestSnapshotID       = int64(3055729675574597004)
	filesTestParentSnapshotID = int64(3051729675574597004)

	// filesTestSequenceNumber is the sequence number of the added snapshot, i.e. the commit's
	// position in the table's history.
	filesTestSequenceNumber = int64(34)

	// filesTestSnapshotTimestampMs is the snapshot's timestamp: the creation date the staged
	// entries must carry (spike S-I2 §5.1 item 3).
	filesTestSnapshotTimestampMs = int64(1555100955770)

	// filesTestMetadataTimestampMs is the last-updated-ms of the metadata documents, i.e. the
	// creation date of the staged metadata.json.
	filesTestMetadataTimestampMs = int64(1602638573590)

	// filesTestDataSize and filesTestDataCount are the size and record count of the data files the
	// manifests point at.
	filesTestDataSize  = int64(1024)
	filesTestDataCount = int64(100)
)

// filesTestFiles is a committed snapshot's file layout in the object store: the manifest list, the
// manifest and the data files, all under the table root. Every name is deterministic so a test can
// name the entry it expects.
var (
	filesTestManifestListKey = "metadata/snap-" + fmt.Sprint(filesTestSnapshotID) + "-1-" + storeTestUUID + ".avro"
	filesTestManifestKey     = "metadata/" + storeTestUUID + "-m0.avro"
	filesTestMetadataKey     = "metadata/00002-5f8f9f7e-0000-4000-8000-000000000001.metadata.json"
	filesTestDataKeys        = []string{
		"data/00000-0-" + storeTestUUID + ".parquet",
		"data/00001-0-" + storeTestUUID + ".parquet",
	}
)

// getRecordingAdapter is the fixture's block adapter with a log of every object that was read. It
// is how TestForeignLocationRejected proves that a path outside the table root is refused *before*
// it is fetched with the server's credentials, which is the whole point of the check (§13).
type getRecordingAdapter struct {
	block.Adapter

	mu   sync.Mutex
	gets []string
}

func (a *getRecordingAdapter) Get(ctx context.Context, obj block.ObjectPointer) (io.ReadCloser, error) {
	a.mu.Lock()
	a.gets = append(a.gets, obj.Identifier)
	a.mu.Unlock()
	return a.Adapter.Get(ctx, obj)
}

// readPaths is the identifiers read since the last reset.
func (a *getRecordingAdapter) readPaths() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.gets...)
}

func (a *getRecordingAdapter) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.gets = nil
}

// entryRecordingCatalog is the fixture's catalog with a log of every entry it was asked to write.
// file registration succeeds or fails as a whole, so counting the writes is how a test distinguishes
// "refused while planning" from "refused half-way through staging".
type entryRecordingCatalog struct {
	Catalog

	mu      sync.Mutex
	created []catalog.DBEntry
}

func (c *entryRecordingCatalog) CreateEntry(ctx context.Context, repositoryID, branch string, entry catalog.DBEntry, opts ...graveler.SetOptionsFunc) error {
	c.mu.Lock()
	c.created = append(c.created, entry)
	c.mu.Unlock()
	return c.Catalog.CreateEntry(ctx, repositoryID, branch, entry, opts...)
}

func (c *entryRecordingCatalog) createdEntries() []catalog.DBEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]catalog.DBEntry(nil), c.created...)
}

func (c *entryRecordingCatalog) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created = nil
}

// filesFixture is a store whose reads and writes are recorded, plus the address and the physical
// root of the table the commits under test address.
type filesFixture struct {
	*storeFixture

	addr    Address
	root    string
	reader  *getRecordingAdapter
	created *entryRecordingCatalog
}

func newFilesFixture(t *testing.T) *filesFixture {
	t.Helper()

	f := newStoreFixture(t)
	reader := &getRecordingAdapter{Adapter: f.catalog.BlockAdapter}
	created := &entryRecordingCatalog{Catalog: f.catalog}
	f.store = NewStore(created, reader)

	tableUUID := uuid.MustParse(storeTestUUID)
	root, err := TableRoot("mem://"+storeTestRepo+"-ns", tableUUID)
	require.NoError(t, err, "TableRoot")

	return &filesFixture{
		storeFixture: f,
		addr: Address{
			Repo:      storeTestRepo,
			Ref:       "main",
			Namespace: Namespace{"inventory", "books"},
			Table:     "orders",
		},
		root:    root,
		reader:  reader,
		created: created,
	}
}

// objectKey is the key of a table file relative to the repository storage namespace, i.e. the form
// storeFixture.putMetadata takes.
func (f *filesFixture) objectKey(t *testing.T, relativePath string) string {
	t.Helper()
	return strings.TrimPrefix(f.root+"/"+relativePath, "mem://"+storeTestRepo+"-ns/")
}

// location is the fully qualified URI of a table file.
func (f *filesFixture) location(relativePath string) string {
	return f.root + "/" + relativePath
}

// putObject writes one object of the table and returns its fully qualified URI, the way a client
// (or iceberg-go on the client's behalf) writes it.
func (f *filesFixture) putObject(t *testing.T, relativePath string, content []byte) string {
	t.Helper()
	return f.putMetadata(t, f.objectKey(t, relativePath), string(content))
}

// entryPath is the reserved key a table file must be registered under.
func (f *filesFixture) entryPath(t *testing.T, relativePath string) string {
	t.Helper()
	path, err := TableFilePath(uuid.MustParse(storeTestUUID), relativePath)
	require.NoError(t, err, "TableFilePath(%q)", relativePath)
	return path
}

// writeManifest writes a v2 data manifest with the given entries under relativePath and returns the
// manifest file (its length included) and its URI.
func (f *filesFixture) writeManifest(t *testing.T, relativePath string, entries []iceberg.ManifestEntry) (iceberg.ManifestFile, string) {
	t.Helper()
	var buffer bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		f.location(relativePath), &buffer, 2, *iceberg.UnpartitionedSpec, filesTestSchema(),
		filesTestSnapshotID, entries)
	require.NoError(t, err, "write manifest %q", relativePath)
	f.putObject(t, relativePath, buffer.Bytes())
	return manifest, f.location(relativePath)
}

// writeManifestList writes a v2 manifest list of one snapshot under relativePath and returns its
// URI.
func (f *filesFixture) writeManifestList(t *testing.T, relativePath string, manifests ...iceberg.ManifestFile) string {
	t.Helper()
	var buffer bytes.Buffer
	sequenceNumber := filesTestSequenceNumber
	parent := int64(filesTestParentSnapshotID)
	require.NoError(t, iceberg.WriteManifestList(
		2, &buffer, filesTestSnapshotID, &parent, &sequenceNumber, 0, manifests),
		"write manifest list %q", relativePath)
	return f.putObject(t, relativePath, buffer.Bytes())
}

// dataFile builds one data file of the table.
func (f *filesFixture) dataFile(t *testing.T, relativePath string, size, count int64) iceberg.DataFile {
	t.Helper()
	file, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData, f.location(relativePath),
		iceberg.ParquetFile, nil, nil, nil, count, size)
	require.NoError(t, err, "build data file %q", relativePath)
	return file.Build()
}

// commitFiles plans and stages the files of one commit whose new snapshot is described by next.
func (f *filesFixture) commitFiles(t *testing.T, base, next table.Metadata, metadataFiles []TableMetadataFile, options ...func(*StageTableFilesRequest)) (*TableFileSet, error) {
	t.Helper()
	request := StageTableFilesRequest{Base: base, Next: next, MetadataFiles: metadataFiles}
	for _, option := range options {
		option(&request)
	}
	return f.store.StageTableFiles(f.ctx, f.addr, request)
}

// filesTestSchema is the schema the manifests are written with: one required long column, which is
// all a manifest needs to be valid.
func filesTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
}

// filesTestSnapshotDocument renders one v2 snapshot of the spec's example, with the manifest list
// the test wrote.
func filesTestSnapshotDocument(manifestList string) string {
	return fmt.Sprintf(`{
		"snapshot-id": %d,
		"parent-snapshot-id": %d,
		"timestamp-ms": %d,
		"sequence-number": %d,
		"summary": {"operation": "append"},
		"manifest-list": %q
	}`, filesTestSnapshotID, filesTestParentSnapshotID, filesTestSnapshotTimestampMs, filesTestSequenceNumber, manifestList)
}

// filesTestMetadataDocument is a v2 metadata document with the given snapshots and current
// snapshot. It follows the Iceberg spec's example: the table location is the physical root the
// catalog defaults to (Q-I5), and the snapshots are the ones the commit produced.
func filesTestMetadataDocument(location string, snapshots []string, currentSnapshotID int64) string {
	return fmt.Sprintf(`{
		"format-version": 2,
		"table-uuid": %q,
		"location": %q,
		"last-sequence-number": %d,
		"last-updated-ms": %d,
		"last-column-id": 1,
		"current-schema-id": 0,
		"schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"last-partition-id": 999,
		"default-sort-order-id": 0,
		"sort-orders": [{"order-id": 0, "fields": []}],
		"properties": {},
		"current-snapshot-id": %d,
		"snapshots": [%s],
		"snapshot-log": [],
		"metadata-log": []
	}`, storeTestUUID, location, filesTestSequenceNumber, filesTestMetadataTimestampMs,
		currentSnapshotID, strings.Join(snapshots, ","))
}

// filesTestMetadata parses a metadata document, the way the commit path parses the file it loads.
func filesTestMetadata(t *testing.T, document string) table.Metadata {
	t.Helper()
	metadata, err := table.ParseMetadataBytes([]byte(document))
	require.NoError(t, err, "parse metadata document")
	return metadata
}

// stagedEntry reads an entry back from the branch, i.e. through the same staged-then-committed
// resolution a reader gets.
func (f *filesFixture) stagedEntry(t *testing.T, entryPath string) *catalog.DBEntry {
	t.Helper()
	entry, err := f.entryAt(t, "main", entryPath)
	require.NoError(t, err, "read entry %s", entryPath)
	return entry
}

// TestStageTableFiles covers §6.3 end to end: a commit that adds a snapshot stages one entry per
// metadata, manifest-list, manifest and data file, with the physical address of the object the
// client wrote and a deterministic entry key; a commit that adds no snapshot stages only its
// metadata file; the cap refuses an oversized commit before anything is staged; and both manifest
// list forms of the spec (v2 manifest-list file and v1 inline manifests) are served.
func TestStageTableFiles(t *testing.T) {
	t.Run("stages the files an append added", func(t *testing.T) {
		f := newFilesFixture(t)
		headBefore := f.headRef(t)

		// An earlier commit registered the files of the parent snapshot. This commit's manifest adds
		// two files, keeps one (EXISTING) and removes one (DELETED, which Q-I8 does not tombstone).
		manifest, _ := f.writeManifest(t, filesTestManifestKey, []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil, f.dataFile(t, filesTestDataKeys[0], filesTestDataSize, filesTestDataCount)),
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil, f.dataFile(t, filesTestDataKeys[1], 2*filesTestDataSize, 2*filesTestDataCount)),
			iceberg.NewManifestEntry(iceberg.EntryStatusEXISTING, pointerTo(filesTestParentSnapshotID), pointerTo(filesTestSequenceNumber-1), pointerTo(filesTestSequenceNumber-1), f.dataFile(t, "data/kept.parquet", filesTestDataSize, filesTestDataCount)),
			iceberg.NewManifestEntry(iceberg.EntryStatusDELETED, pointerTo(filesTestParentSnapshotID), pointerTo(filesTestSequenceNumber-1), pointerTo(filesTestSequenceNumber-1), f.dataFile(t, "data/removed.parquet", filesTestDataSize, filesTestDataCount)),
		})
		manifestList := f.writeManifestList(t, filesTestManifestListKey, manifest)

		base := filesTestMetadata(t, filesTestMetadataDocument(f.root, nil, -1))
		metadataDocument := filesTestMetadataDocument(f.root, []string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, metadataDocument)
		metadataLocation := f.putObject(t, filesTestMetadataKey, []byte(metadataDocument))

		set, err := f.commitFiles(t, base, next, []TableMetadataFile{
			{Location: metadataLocation, Size: int64(len(metadataDocument))},
		})
		require.NoError(t, err, "stage the files of the append")

		// One snapshot added, and it is the one the commit names.
		require.Equal(t, []int64{filesTestSnapshotID}, set.SnapshotIDs)
		require.Equal(t, f.root, set.TableRoot)

		// One entry per metadata, manifest-list, manifest and data file - and nothing for the files
		// the snapshot kept or removed.
		require.Equal(t, []string{
			f.entryPath(t, filesTestDataKeys[0]),
			f.entryPath(t, filesTestDataKeys[1]),
			f.entryPath(t, filesTestMetadataKey),
			f.entryPath(t, filesTestManifestKey),
			f.entryPath(t, filesTestManifestListKey),
		}, planPaths(set.Files), "the plan is sorted by entry key and holds exactly the added files")
		require.Equal(t, 2, set.EntriesSkipped, "EXISTING and DELETED entries are not registered")
		require.Equal(t, 0, set.ManifestsSkipped)
		require.Equal(t, len(set.Files), set.Staged, "a stage reports what it staged")

		// The layout of §6.3, spelled out: _lakefs_iceberg/files/<table-uuid>/<relative path>.
		require.Equal(t, "_lakefs_iceberg/files/"+storeTestUUID+"/"+filesTestDataKeys[0],
			set.Files[0].Path)

		snapshotTime := time.UnixMilli(filesTestSnapshotTimestampMs).UTC()
		metadataTime := time.UnixMilli(filesTestMetadataTimestampMs).UTC()
		for _, file := range set.Files {
			entry := f.stagedEntry(t, file.Path)
			require.Equal(t, catalog.AddressTypeFull, entry.AddressType,
				"%s points at the object the client wrote", file.Path)
			require.Equal(t, file.PhysicalAddress, entry.PhysicalAddress)
			require.Equal(t, file.Size, entry.Size, "size of %s", file.Path)
			require.Equal(t, file.Kind, kindOf(t, set, file.Path), "kind of %s", file.Path)
			if file.Kind == TableFileKindMetadata {
				require.Equal(t, metadataTime, entry.CreationDate.UTC(),
					"a metadata file's creation date is the metadata's last-updated-ms")
			} else {
				require.Equal(t, snapshotTime, entry.CreationDate.UTC(),
					"a snapshot file's creation date is the snapshot's timestamp")
			}
		}

		// Sizes come from the metadata the client wrote, not from a HEAD: the manifest list's from
		// the bytes we read, the manifest's from the manifest list, the data files' from the
		// manifest entries.
		byPath := map[string]TableFile{}
		for _, file := range set.Files {
			byPath[file.Path] = file
		}
		require.Equal(t, filesTestDataSize, byPath[f.entryPath(t, filesTestDataKeys[0])].Size)
		require.Equal(t, 2*filesTestDataSize, byPath[f.entryPath(t, filesTestDataKeys[1])].Size)
		require.NotZero(t, byPath[f.entryPath(t, filesTestManifestKey)].Size)
		require.NotZero(t, byPath[f.entryPath(t, filesTestManifestListKey)].Size)
		require.Equal(t, int64(len(metadataDocument)), byPath[f.entryPath(t, filesTestMetadataKey)].Size)

		// Staged, not committed: file registration is §6.2 step 5 and the lakeFS commit is the
		// client's separate action.
		require.Equal(t, headBefore, f.headRef(t), "registering table files must not commit the branch")

		// A retried commit overwrites the same keys instead of adding entries (S-I2 §5.1 item 5).
		f.created.reset()
		again, err := f.commitFiles(t, base, next, []TableMetadataFile{
			{Location: metadataLocation, Size: int64(len(metadataDocument))},
		})
		require.NoError(t, err, "re-stage the same commit")
		require.Equal(t, planPaths(set.Files), planPaths(again.Files), "a retry plans the same keys")
		require.Len(t, f.created.createdEntries(), len(set.Files), "a retry rewrites the same keys")
	})

	t.Run("the same file in two manifests is one entry", func(t *testing.T) {
		f := newFilesFixture(t)

		// One data file described by two manifests of the same snapshot: the plan is a set of keys,
		// so it is one entry, and planning writes nothing at all.
		dataFile := f.dataFile(t, filesTestDataKeys[0], filesTestDataSize, filesTestDataCount)
		first, _ := f.writeManifest(t, filesTestManifestKey, []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil, dataFile),
		})
		second, _ := f.writeManifest(t, "metadata/"+storeTestUUID+"-m1.avro", []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil, dataFile),
		})
		manifestList := f.writeManifestList(t, filesTestManifestListKey, first, second)

		document := filesTestMetadataDocument(f.root, []string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, document)

		plan, err := f.store.PlanTableFiles(f.ctx, f.addr, StageTableFilesRequest{
			Next:          next,
			MetadataFiles: []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}},
		})
		require.NoError(t, err, "plan the files of the two manifests")
		require.Equal(t, 5, len(plan.Files), "two manifests, one manifest list, one data file, one metadata file")
		require.Zero(t, plan.Staged, "a plan stages nothing")
		require.Empty(t, f.created.createdEntries(), "a plan writes nothing")

		set, err := f.commitFiles(t, nil, next, []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}})
		require.NoError(t, err, "stage the files of the two manifests")
		require.Equal(t, 5, set.Staged)
		require.Equal(t, 1, countDataEntries(set), "the duplicated data file is one entry")
	})

	t.Run("a commit without a new snapshot stages only its metadata file", func(t *testing.T) {
		f := newFilesFixture(t)

		// The table already has a snapshot, and its manifest list is not even an object this
		// fixture wrote: nothing may read it, because the commit added no snapshot (§6.3 step 1 is
		// per added snapshot).
		document := filesTestMetadataDocument(f.root, []string{
			filesTestSnapshotDocument(f.location(filesTestManifestListKey)),
		}, filesTestSnapshotID)
		base := filesTestMetadata(t, document)
		metadataLocation := f.putObject(t, filesTestMetadataKey, []byte(document))

		set, err := f.commitFiles(t, base, base, []TableMetadataFile{
			{Location: metadataLocation, Size: int64(len(document))},
		})
		require.NoError(t, err, "stage a properties-only commit")

		require.Empty(t, set.SnapshotIDs, "no snapshot was added")
		require.Equal(t, []string{f.entryPath(t, filesTestMetadataKey)}, planPaths(set.Files))
		require.Empty(t, f.reader.readPaths(), "no manifest list of an existing snapshot is read")
	})

	t.Run("the cap refuses an oversized commit before staging anything", func(t *testing.T) {
		f := newFilesFixture(t)

		entries := make([]iceberg.ManifestEntry, 0, 4)
		for i := range 4 {
			relativePath := fmt.Sprintf("data/part-%05d.parquet", i)
			entries = append(entries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil,
				f.dataFile(t, relativePath, filesTestDataSize, filesTestDataCount)))
		}
		manifest, _ := f.writeManifest(t, filesTestManifestKey, entries)
		manifestList := f.writeManifestList(t, filesTestManifestListKey, manifest)

		document := filesTestMetadataDocument(f.root, []string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, document)

		_, err := f.commitFiles(t, nil, next, []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}},
			func(request *StageTableFilesRequest) { request.MaxFiles = 3 })
		require.Error(t, err, "a commit above the cap is refused")

		var limit *TableFileLimitError
		require.ErrorAs(t, err, &limit)
		require.Equal(t, 4, limit.Count, "the count names the file that crossed the cap")
		require.Equal(t, 3, limit.Max)
		require.ErrorIs(t, err, ErrTooManyTableFiles)
		require.ErrorIs(t, err, ErrCommitRejected)
		require.Equal(t, 400, limit.HTTPStatusCode())
		require.Equal(t, ErrTypeBadRequest, limit.ErrorType())
		require.Empty(t, f.created.createdEntries(), "a refused plan must not stage anything")
	})

	t.Run("stages many files through the worker pool", func(t *testing.T) {
		f := newFilesFixture(t)

		entries := make([]iceberg.ManifestEntry, 0, 40)
		expected := make([]string, 0, 40)
		for i := range 40 {
			relativePath := fmt.Sprintf("data/part-%05d.parquet", i)
			expected = append(expected, f.entryPath(t, relativePath))
			entries = append(entries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil,
				f.dataFile(t, relativePath, filesTestDataSize, filesTestDataCount)))
		}
		manifest, _ := f.writeManifest(t, filesTestManifestKey, entries)
		manifestList := f.writeManifestList(t, filesTestManifestListKey, manifest)

		document := filesTestMetadataDocument(f.root, []string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, document)

		set, err := f.commitFiles(t, nil, next, []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}},
			func(request *StageTableFilesRequest) { request.Workers = 8 })
		require.NoError(t, err, "stage 40 files with 8 workers")

		require.Equal(t, 40+3, set.Staged, "40 data files, one manifest, one manifest list, one metadata file")
		for _, entryPath := range expected {
			require.NotNil(t, f.stagedEntry(t, entryPath), "every data file is staged")
		}
	})

	t.Run("serves the v1 inline-manifest form", func(t *testing.T) {
		f := newFilesFixture(t)

		// v1 snapshots may carry their manifest locations inline instead of naming a manifest list
		// (spec: "Snapshots", the deprecated manifests field of a v1 snapshot).
		var buffer bytes.Buffer
		manifest, err := iceberg.WriteManifest(
			f.location(filesTestManifestKey), &buffer, 1, *iceberg.UnpartitionedSpec, filesTestSchema(),
			filesTestSnapshotID, []iceberg.ManifestEntry{
				iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil,
					f.dataFile(t, filesTestDataKeys[0], filesTestDataSize, filesTestDataCount)),
			})
		require.NoError(t, err, "write a v1 manifest")
		f.putObject(t, filesTestManifestKey, buffer.Bytes())

		document := fmt.Sprintf(`{
			"format-version": 1,
			"table-uuid": %q,
			"location": %q,
			"last-updated-ms": %d,
			"last-column-id": 1,
			"schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
			"partition-spec": [],
			"properties": {},
			"current-snapshot-id": %d,
			"snapshots": [{
				"snapshot-id": %d,
				"timestamp-ms": %d,
				"sequence-number": 0,
				"summary": {"operation": "append"},
				"manifests": [%q]
			}],
			"snapshot-log": [],
			"metadata-log": []
		}`, storeTestUUID, f.root, filesTestMetadataTimestampMs, filesTestSnapshotID,
			filesTestSnapshotID, filesTestSnapshotTimestampMs, manifest.FilePath())
		next := filesTestMetadata(t, document)

		set, err := f.commitFiles(t, nil, next, []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}})
		require.NoError(t, err, "stage a v1 table")

		require.Equal(t, []int64{filesTestSnapshotID}, set.SnapshotIDs)
		require.Equal(t, []string{
			f.entryPath(t, filesTestDataKeys[0]),
			f.entryPath(t, filesTestMetadataKey),
			f.entryPath(t, filesTestManifestKey),
		}, planPaths(set.Files), "a v1 snapshot has no manifest-list object to register")
		for _, file := range set.Files {
			if file.Kind == TableFileKindManifest {
				require.NotZero(t, file.Size,
					"a v1 inline manifest has no length in the metadata, so its size is the bytes read")
			}
		}
	})
}

// TestForeignLocationRejected covers the §13 security control: a snapshot that references anything
// outside <storage_namespace>/_iceberg/<table-uuid>/ is a 400, and it is refused *before* the
// object is read - the catalog reads manifests with the server's credentials, so reading first and
// checking afterwards would already have read another bucket.
//
// Every case also asserts that nothing was staged, because a rejection that leaves half the files
// registered would be a GC hazard rather than a refusal.
func TestForeignLocationRejected(t *testing.T) {
	const foreignRoot = "s3://another-team/secret"

	t.Run("manifest list outside the table root", func(t *testing.T) {
		f := newFilesFixture(t)

		foreignList := foreignRoot + "/metadata/snap-1.avro"
		document := filesTestMetadataDocument(f.root, []string{filesTestSnapshotDocument(foreignList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, document)

		_, err := f.commitFiles(t, nil, next, []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}})
		require.Error(t, err, "a foreign manifest list is refused")

		var foreign *ForeignTableFileError
		require.ErrorAs(t, err, &foreign)
		require.Equal(t, TableFileKindManifestList, foreign.Kind)
		require.Equal(t, foreignList, foreign.Location)
		require.Equal(t, f.root, foreign.Root)
		require.ErrorIs(t, err, ErrForeignTableFile)
		require.ErrorIs(t, err, ErrCommitRejected)
		require.Equal(t, 400, foreign.HTTPStatusCode())
		require.Equal(t, ErrTypeBadRequest, foreign.ErrorType())

		require.Empty(t, f.reader.readPaths(), "a foreign path is never read with the server's credentials")
		require.Empty(t, f.created.createdEntries(), "a refused plan stages nothing")
	})

	t.Run("manifest outside the table root", func(t *testing.T) {
		f := newFilesFixture(t)

		// The manifest list itself is inside the table, and names a manifest outside it. Only the
		// list may be read; the foreign manifest must never be fetched.
		foreignManifest := foreignRoot + "/metadata/m0.avro"
		var buffer bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(2, &buffer, filesTestSnapshotID, nil,
			pointerTo(filesTestSequenceNumber), 0,
			[]iceberg.ManifestFile{iceberg.NewManifestFile(2, foreignManifest, 4096, 0, filesTestSnapshotID).Build()}),
			"write a manifest list that names a foreign manifest")
		manifestList := f.putObject(t, filesTestManifestListKey, buffer.Bytes())

		document := filesTestMetadataDocument(f.root, []string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, document)

		_, err := f.commitFiles(t, nil, next, []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}})
		require.Error(t, err, "a foreign manifest is refused")

		var foreign *ForeignTableFileError
		require.ErrorAs(t, err, &foreign)
		require.Equal(t, TableFileKindManifest, foreign.Kind)
		require.Equal(t, foreignManifest, foreign.Location)
		require.Equal(t, []string{manifestList}, f.reader.readPaths(),
			"the list is read, and the foreign manifest is not")
		require.Empty(t, f.created.createdEntries(), "a refused plan stages nothing")
	})

	t.Run("data file outside the table root", func(t *testing.T) {
		f := newFilesFixture(t)

		foreignData := foreignRoot + "/data/part-0.parquet"
		dataFile, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentData, foreignData,
			iceberg.ParquetFile, nil, nil, nil, filesTestDataCount, filesTestDataSize)
		require.NoError(t, err, "build a foreign data file")
		manifest, _ := f.writeManifest(t, filesTestManifestKey, []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil, dataFile.Build()),
		})
		manifestList := f.writeManifestList(t, filesTestManifestListKey, manifest)

		document := filesTestMetadataDocument(f.root, []string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, document)

		_, err = f.commitFiles(t, nil, next, []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}})
		require.Error(t, err, "a foreign data file is refused")

		var foreign *ForeignTableFileError
		require.ErrorAs(t, err, &foreign)
		require.Equal(t, TableFileKindData, foreign.Kind)
		require.Equal(t, foreignData, foreign.Location)
		require.NotContains(t, f.reader.readPaths(), foreignData, "a data file is never read")
		require.Empty(t, f.created.createdEntries(), "a refused plan stages nothing")
	})

	t.Run("a path that climbs out of the table root", func(t *testing.T) {
		f := newFilesFixture(t)

		// The address carries the root as a string prefix, so a prefix test alone would accept it;
		// the ".." segment is what makes it a different directory.
		climbing := f.root + "/../another-table/data/part-0.parquet"
		dataFile, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentData, climbing,
			iceberg.ParquetFile, nil, nil, nil, filesTestDataCount, filesTestDataSize)
		require.NoError(t, err, "build a climbing data file")
		manifest, _ := f.writeManifest(t, filesTestManifestKey, []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil, dataFile.Build()),
		})
		manifestList := f.writeManifestList(t, filesTestManifestListKey, manifest)

		document := filesTestMetadataDocument(f.root, []string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, document)

		_, err = f.commitFiles(t, nil, next, []TableMetadataFile{{Location: f.location(filesTestMetadataKey)}})
		require.Error(t, err, "a path that climbs out of the root is refused")

		var foreign *ForeignTableFileError
		require.ErrorAs(t, err, &foreign)
		require.Equal(t, climbing, foreign.Location)
		require.Error(t, foreign.Err, "the reason the relative path is unusable is attached")
		require.Empty(t, f.created.createdEntries(), "a refused plan stages nothing")
	})

	t.Run("metadata file outside the table root", func(t *testing.T) {
		f := newFilesFixture(t)

		// A table registered from another catalog may have its own location; the catalog can read
		// it, but §13 does not let a commit register files under it.
		foreignMetadata := foreignRoot + "/metadata/00002.metadata.json"
		document := filesTestMetadataDocument(f.root, nil, -1)
		next := filesTestMetadata(t, document)

		_, err := f.commitFiles(t, nil, next, []TableMetadataFile{{Location: foreignMetadata, Size: 42}})
		require.Error(t, err, "a foreign metadata file is refused")

		var foreign *ForeignTableFileError
		require.ErrorAs(t, err, &foreign)
		require.Equal(t, TableFileKindMetadata, foreign.Kind)
		require.Equal(t, foreignMetadata, foreign.Location)
		require.Empty(t, f.created.createdEntries(), "a refused plan stages nothing")
	})

	t.Run("entry keys cannot escape the reserved directory", func(t *testing.T) {
		// The key builder is the last line of defence: even a relative path that got past the
		// snapshot checks cannot name an entry outside _lakefs_iceberg/files/<uuid>/.
		for _, relativePath := range []string{
			"", "/data/x.parquet", "data/", "data//x.parquet", "../data/x.parquet",
			"data/../../x.parquet", "./data/x.parquet", "data\\x.parquet", "data/\x00x.parquet",
		} {
			_, err := TableFilePath(uuid.MustParse(storeTestUUID), relativePath)
			require.Error(t, err, "relative path %q must not become an entry key", relativePath)
		}

		key, err := TableFilePath(uuid.MustParse(storeTestUUID), filesTestDataKeys[0])
		require.NoError(t, err)
		require.Equal(t, "_lakefs_iceberg/files/"+storeTestUUID+"/"+filesTestDataKeys[0], key)
	})
}

// TestTableRoot covers where a commit's files must live (ICEBERG_CATALOG_PLAN.md §6.3): the
// location the table's own metadata.json declares. For a table created without a location that is
// the managed <storage_namespace>/_iceberg/<table-uuid>; for a table the create request placed
// inside the table area it is that location, which is why the root is read from the metadata and
// not derived from the table UUID. A table whose location is outside the table area can be read
// but not committed: its files are outside the one place the catalog registers and the GC watches.
func TestTableRoot(t *testing.T) {
	t.Run("a table created with its own location registers under that location", func(t *testing.T) {
		f := newFilesFixture(t)
		customRoot := "mem://" + storeTestRepo + "-ns/_iceberg/team-a/orders"

		// The snapshot's manifest list, written where the client wrote it: under the table's own
		// location, not under the managed location derived from the table UUID.
		var buffer bytes.Buffer
		sequenceNumber := int64(filesTestSequenceNumber)
		parent := int64(filesTestParentSnapshotID)
		require.NoError(t, iceberg.WriteManifestList(2, &buffer, filesTestSnapshotID, &parent, &sequenceNumber, 0, nil))
		manifestList := customRoot + "/" + filesTestManifestListKey
		f.putMetadata(t, strings.TrimPrefix(manifestList, "mem://"+storeTestRepo+"-ns/"), buffer.String())

		base := filesTestMetadata(t, filesTestMetadataDocument(customRoot, nil, -1))
		document := filesTestMetadataDocument(customRoot, []string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, document)
		metadataLocation := f.putMetadata(t,
			strings.TrimPrefix(customRoot+"/"+filesTestMetadataKey, "mem://"+storeTestRepo+"-ns/"), document)

		set, err := f.commitFiles(t, base, next, []TableMetadataFile{{Location: metadataLocation, Size: int64(len(document))}})
		require.NoError(t, err, "a table inside the table area registers its files")
		require.Equal(t, customRoot, set.TableRoot, "the root is the location metadata.json declares")
		require.Equal(t, []string{
			f.entryPath(t, filesTestMetadataKey),
			f.entryPath(t, filesTestManifestListKey),
		}, planPaths(set.Files), "the keys stay relative to the table root, under the table UUID")
	})

	t.Run("a table outside the table area is refused", func(t *testing.T) {
		f := newFilesFixture(t)

		// A table registered from another catalog may point anywhere; the catalog can read it, but
		// a commit must not register files the uncommitted GC cannot see (§13, spike S-I2 §4).
		const outsideRoot = "s3://another-team/books"
		base := filesTestMetadata(t, filesTestMetadataDocument(outsideRoot, nil, -1))
		next := filesTestMetadata(t, filesTestMetadataDocument(outsideRoot, nil, -1))

		_, err := f.commitFiles(t, base, next, nil)
		require.Error(t, err)

		var foreign *ForeignTableFileError
		require.ErrorAs(t, err, &foreign)
		require.ErrorIs(t, err, ErrForeignTableFile)
		require.ErrorIs(t, err, ErrTableLocationOutsideArea)
		require.ErrorIs(t, err, ErrCommitRejected)
		require.Equal(t, 400, foreign.HTTPStatusCode())
		require.Equal(t, outsideRoot, foreign.Location)
		require.Empty(t, f.created.createdEntries(), "a refused plan stages nothing")
		require.Empty(t, f.reader.readPaths(), "a table outside the area is never read")
	})
}

// planPaths is the entry keys of a plan, in plan order.
func planPaths(files []TableFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	return paths
}

// kindOf is the kind the plan records for an entry path.
func kindOf(t *testing.T, set *TableFileSet, entryPath string) TableFileKind {
	t.Helper()
	for _, file := range set.Files {
		if file.Path == entryPath {
			return file.Kind
		}
	}
	require.FailNowf(t, "entry not in the plan", "path %q", entryPath)
	return ""
}

// countDataEntries is the number of data-file entries in a plan.
func countDataEntries(set *TableFileSet) int {
	count := 0
	for _, file := range set.Files {
		if file.Kind == TableFileKindData {
			count++
		}
	}
	return count
}

// pointerTo is a pointer to a copy of value, for the optional manifest-entry fields.
func pointerTo[T any](value T) *T {
	return &value
}

// TestReleaseTableFiles covers the other half of §6.3: what a drop does to the entries a commit
// staged (files.go "What a drop releases", tables.go dropTable). The registration above exists so
// that the committed GC protects exactly the live files; the release is the reverse of it, so that a
// dropped table's files stop being protected and the GC can reclaim them.
//
// What the release must be, and what these subtests therefore pin:
//   - it removes every entry under the table's own directory _lakefs_iceberg/files/<table-uuid>/,
//     and only those: a sibling table's entries and the reserved area outside the directory survive;
//   - it deletes no object: the entries are AddressType=Full, so the files themselves stay readable
//     and a commit, tag or branch that still references them keeps working (§14 Q-I8);
//   - it is a staged removal like every other catalog write, and it is idempotent: a table whose
//     entries are already gone releases nothing and answers success, because the outcome it was
//     asked for - the entries being absent - is what it finds.
func TestReleaseTableFiles(t *testing.T) {
	// stageAppendFiles registers the files a commit that adds one snapshot stages: a metadata file,
	// a manifest list, a manifest and two data files under the table's uuid.
	stageAppendFiles := func(t *testing.T, f *filesFixture) []string {
		t.Helper()
		manifest, _ := f.writeManifest(t, filesTestManifestKey, []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil,
				f.dataFile(t, filesTestDataKeys[0], filesTestDataSize, filesTestDataCount)),
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil,
				f.dataFile(t, filesTestDataKeys[1], 2*filesTestDataSize, 2*filesTestDataCount)),
		})
		manifestList := f.writeManifestList(t, filesTestManifestListKey, manifest)

		base := filesTestMetadata(t, filesTestMetadataDocument(f.root, nil, -1))
		metadataDocument := filesTestMetadataDocument(f.root,
			[]string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		next := filesTestMetadata(t, metadataDocument)
		metadataLocation := f.putObject(t, filesTestMetadataKey, []byte(metadataDocument))

		set, err := f.commitFiles(t, base, next, []TableMetadataFile{
			{Location: metadataLocation, Size: int64(len(metadataDocument))},
		})
		require.NoError(t, err, "stage the files of the append")
		require.Len(t, set.Files, 5, "metadata, manifest list, manifest and two data files")
		return planPaths(set.Files)
	}

	t.Run("releases the table's entries and nothing else", func(t *testing.T) {
		f := newFilesFixture(t)
		staged := stageAppendFiles(t, f)

		// A sibling table's entries, under its own uuid directory, staged the same way a commit of
		// that table would stage them. The release must not be able to reach them.
		siblingPath, err := TableFilePath(uuid.MustParse(storeAltUUID), "data/00000-0-parquet")
		require.NoError(t, err)
		require.NoError(t, f.catalog.CreateEntry(f.ctx, storeTestRepo, "main", catalog.DBEntry{
			Path:            siblingPath,
			PhysicalAddress: "mem://" + storeTestRepo + "-ns/_iceberg/" + storeAltUUID + "/data/00000-0-parquet",
			AddressType:     catalog.AddressTypeFull,
			CreationDate:    time.UnixMilli(filesTestSnapshotTimestampMs).UTC(),
			Size:            filesTestDataSize,
		}), "stage the sibling table's file entry")
		before := f.listReservedArea(t)

		released, err := releaseTableFiles(f.ctx, f.catalog, f.addr, uuid.MustParse(storeTestUUID), 0)
		require.NoError(t, err, "release the table's files")
		require.Equal(t, len(staged), released, "every staged entry of the table is released")

		// The entries are gone - from the staging area a reader resolves first - and the objects they
		// pointed at are untouched: a release stages entry removals, it never deletes a file.
		for _, entryPath := range staged {
			_, err := f.entryAt(t, "main", entryPath)
			require.ErrorIs(t, err, graveler.ErrNotFound, "entry %s must be released", entryPath)
		}
		// (The data files are referenced by the manifests but never written by this fixture; the
		// metadata file, the manifest list and the manifest are the objects that exist.)
		for _, relativePath := range []string{filesTestMetadataKey, filesTestManifestListKey, filesTestManifestKey} {
			require.NotEmpty(t, f.storedObject(t, f.location(relativePath)),
				"the release must not delete the object at %s", f.location(relativePath))
		}

		// Exactly the table's own directory was emptied: the sibling entry and every other key of
		// the reserved area are still there.
		after := f.listReservedArea(t)
		require.Equal(t, sortedDifference(before, after), sortedPaths(staged),
			"the release removes the table's entries and nothing else")
		require.Contains(t, after, siblingPath, "a sibling table's entries are not the release's")
	})

	t.Run("a table whose entries are already gone releases nothing", func(t *testing.T) {
		f := newFilesFixture(t)
		staged := stageAppendFiles(t, f)

		released, err := releaseTableFiles(f.ctx, f.catalog, f.addr, uuid.MustParse(storeTestUUID), 0)
		require.NoError(t, err)
		require.Equal(t, len(staged), released)

		// A second release of the same table - a retried drop, or the same directory released twice -
		// finds nothing to remove and is a success: the entries it was asked to remove are absent,
		// which is the outcome it asked for.
		released, err = releaseTableFiles(f.ctx, f.catalog, f.addr, uuid.MustParse(storeTestUUID), 0)
		require.NoError(t, err, "releasing an empty directory is not a failure")
		require.Zero(t, released)
	})

	t.Run("pages over a table with more files than one listing returns", func(t *testing.T) {
		// The real listing page is catalog.ListEntriesLimitMax, so a fake lister is the only way to
		// exercise the cursor of the release loop: a table whose files do not fit in one page must be
		// released completely, and no key may be skipped or revisited.
		prefix := ReservedPrefix + "/" + FilesDirectory + "/" + storeTestUUID + "/"
		lister := &releaseLister{pageSize: 1}
		for i := range 4 {
			lister.add(prefix + "data/0000" + strconv.Itoa(i) + ".parquet")
		}

		released, err := releaseTableFiles(context.Background(), lister, filesTestAddr(), uuid.MustParse(storeTestUUID), 1)
		require.NoError(t, err)
		require.Equal(t, 4, released)
		require.Equal(t, []string{
			prefix + "data/00000.parquet",
			prefix + "data/00001.parquet",
			prefix + "data/00002.parquet",
			prefix + "data/00003.parquet",
		}, lister.deletedPaths())
		require.GreaterOrEqual(t, lister.listCalls(), 4, "a page of one entry means at least one call per entry")
	})

	t.Run("an entry that is already gone is not a failure", func(t *testing.T) {
		// Two writers release the same table's directory; the listing may still name an entry the
		// other writer already removed. A delete that finds nothing removed what was asked for.
		prefix := ReservedPrefix + "/" + FilesDirectory + "/" + storeTestUUID + "/"
		lister := &releaseLister{pageSize: 10, missing: map[string]bool{prefix + "data/00000.parquet": true}}
		lister.add(prefix + "data/00000.parquet")
		lister.add(prefix + "data/00001.parquet")

		released, err := releaseTableFiles(context.Background(), lister, filesTestAddr(), uuid.MustParse(storeTestUUID), 2)
		require.NoError(t, err)
		require.Equal(t, 1, released, "only the entry that was there to remove counts as released")
		require.Equal(t, []string{prefix + "data/00001.parquet"}, lister.deletedPaths())
	})

	t.Run("a table without a uuid cannot be released", func(t *testing.T) {
		// The directory name *is* the table uuid, so a release without one has no directory to walk.
		// It is refused rather than answered with "released nothing", which a caller could mistake for
		// a released table.
		_, err := releaseTableFiles(context.Background(), &releaseLister{}, filesTestAddr(), uuid.Nil, 0)
		require.ErrorIs(t, err, ErrTableFileRelease)
	})
}

// filesTestAddr is the address the release tests act on: the table newFilesFixture describes.
func filesTestAddr() Address {
	return Address{
		Repo:      storeTestRepo,
		Ref:       "main",
		Namespace: Namespace{"inventory", "books"},
		Table:     "orders",
	}
}

// listReservedArea lists every key of the reserved _lakefs_iceberg/ area on the fixture's branch, so
// a test can assert which keys a write added or removed rather than only the ones it knows about.
func (f *filesFixture) listReservedArea(t *testing.T) []string {
	t.Helper()
	entries, _, err := f.catalog.ListEntries(f.ctx, storeTestRepo, "main",
		ReservedPrefix+"/", "", "", catalog.ListEntriesLimitMax)
	require.NoError(t, err, "list the reserved area")
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, entry.Path)
	}
	return paths
}

// storedObject reads one object of the fixture repository through the block adapter, i.e. the way a
// client (or the committed GC) reads a file the entries point at.
func (f *filesFixture) storedObject(t *testing.T, location string) string {
	t.Helper()
	repo, err := f.catalog.GetRepository(f.ctx, storeTestRepo)
	require.NoError(t, err, "get the repository")
	reader, err := f.catalog.BlockAdapter.Get(f.ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	})
	require.NoError(t, err, "read the object at %s", location)
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	require.NoError(t, err, "read the object at %s", location)
	return string(data)
}

// sortedPaths is paths sorted, for a set comparison of two listings.
func sortedPaths(paths []string) []string {
	sorted := append([]string(nil), paths...)
	sort.Strings(sorted)
	return sorted
}

// sortedDifference returns the keys of before that are not in after, sorted.
func sortedDifference(before, after []string) []string {
	kept := make(map[string]struct{}, len(after))
	for _, path := range after {
		kept[path] = struct{}{}
	}
	removed := make([]string, 0, len(before))
	for _, path := range before {
		if _, ok := kept[path]; !ok {
			removed = append(removed, path)
		}
	}
	return sortedPaths(removed)
}

// releaseLister is a TableLister whose listing is served in fixed pages and whose deletes are
// recorded, so a release can be paged over a table with more entries than one listing returns
// without staging thousands of entries through the real store.
type releaseLister struct {
	mu       sync.Mutex
	paths    []string
	pageSize int
	missing  map[string]bool
	deleted  []string
	calls    int
}

// add puts one key in the lister's listing.
func (l *releaseLister) add(path string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.paths = append(l.paths, path)
	sort.Strings(l.paths)
}

// deletedPaths is the keys the release staged a removal of, in the order the actions are recorded.
func (l *releaseLister) deletedPaths() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.deleted...)
}

// listCalls is how many listings the release made.
func (l *releaseLister) listCalls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (l *releaseLister) GetBranchReference(context.Context, string, string) (string, error) {
	return "", graveler.ErrNotFound
}

// GetEntry is not part of the release path; the drop reads the pointer through the real lister.
func (l *releaseLister) GetEntry(context.Context, string, string, string, catalog.GetEntryParams) (*catalog.DBEntry, error) {
	return nil, graveler.ErrNotFound
}

// ListEntries serves the release's listing in pages of pageSize entries, cursor and hasMore
// included, exactly like catalog.ListEntries does at its maximum page size.
func (l *releaseLister) ListEntries(_ context.Context, _, _, prefix, after, _ string, _ int) ([]*catalog.DBEntry, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	entries := make([]*catalog.DBEntry, 0, l.pageSize)
	for _, path := range l.paths {
		if path <= after || !strings.HasPrefix(path, prefix) {
			continue
		}
		entries = append(entries, &catalog.DBEntry{Path: path, PhysicalAddress: path})
		if len(entries) == l.pageSize {
			break
		}
	}
	hasMore := len(entries) > 0 && entries[len(entries)-1].Path < l.paths[len(l.paths)-1]
	return entries, hasMore, nil
}

// DeleteEntry stages the removal of one entry, and answers ErrNotFound for the keys the test marks
// as already gone.
func (l *releaseLister) DeleteEntry(_ context.Context, _, _, path string, _ ...graveler.SetOptionsFunc) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.missing[path] {
		return graveler.ErrNotFound
	}
	l.deleted = append(l.deleted, path)
	return nil
}
