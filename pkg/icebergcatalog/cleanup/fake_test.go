package cleanup

// fake_test.go is an in-memory lakeFS: branches, tags, commits, staged entries and the
// physical objects behind them, implementing the API interface of api.go. It exists so the
// tests can exercise a whole expire+mark+sweep run - including pagination and the retention
// walk - without a server, a blockstore or a clock.
//
// It follows the same rules the real server does where the phases can observe them:
//
//   - an entry at a ref is resolvable only at that ref, so "read the table pointer at this
//     commit" behaves like an immutable ref;
//   - GetObject resolves the entry to its physical address, so a catalog file entry whose
//     object lives under the storage namespace is readable without any blockstore on this
//     side;
//   - UploadObject writes the object at <storage-namespace>/<path> and stages an entry at
//     that path, which is what the expire phase relies on;
//   - listing paginates at fakePageSize entries, so the phases' cursor loops are exercised;
//   - deleting removes the entry (a staged tombstone, as far as the phases can tell).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

const (
	fakeRepository    = "repo"
	fakeNamespace     = "s3://bucket/ns"
	fakeDefaultBranch = "main"
	fakePageSize      = 2
	testTableUUIDText = "11111111-2222-3333-4444-555555555555"
)

// fakeAPI is the in-memory implementation of API.
type fakeAPI struct {
	repo     Repository
	branches []string
	tags     []Tag

	rules    GCRules
	rulesErr error

	// entries maps a ref (branch, tag name or commit ID) to its visible entries.
	entries map[string]map[string]ObjectInfo

	// chains maps a branch name or commit ID to its first-parent commit chain, newest
	// first, so ListCommits can answer for a branch and for a tag's commit alike.
	chains map[string][]CommitInfo

	// staged maps a branch to its uncommitted changes.
	staged map[string][]DiffInfo

	// objects maps a normalized physical address to the bytes stored there.
	objects map[string][]byte

	deleted []deletedEntry
	uploads []string

	// beforeStat, when set, is called at the start of every StatObject. It lets a test
	// simulate a concurrent writer moving an entry between planning and applying.
	beforeStat func(ref, path string)
}

type deletedEntry struct {
	branch string
	path   string
}

// fakeHeadCommit is the commit the default branch points at in a fresh fake repository. It
// is recent, so it is the branch's head and the only GC-retained commit.
const fakeHeadCommit = "head-commit"

// newFakeAPI returns an empty repository with one default branch and one recent commit.
func newFakeAPI() *fakeAPI {
	head := CommitInfo{ID: fakeHeadCommit, CreationDate: time.Now().UTC()}
	return &fakeAPI{
		repo: Repository{
			Name:             fakeRepository,
			StorageNamespace: fakeNamespace,
			DefaultBranch:    fakeDefaultBranch,
		},
		branches: []string{fakeDefaultBranch},
		rules:    GCRules{DefaultRetentionDays: 14, Branches: map[string]int{}},
		entries: map[string]map[string]ObjectInfo{
			fakeDefaultBranch: {},
			fakeHeadCommit:    {},
		},
		chains: map[string][]CommitInfo{
			fakeDefaultBranch: {head},
			fakeHeadCommit:    {head},
		},
		staged:  map[string][]DiffInfo{},
		objects: map[string][]byte{},
	}
}

// addBranch creates a branch that starts from the default branch's entries.
func (f *fakeAPI) addBranch(name string) {
	f.branches = append(f.branches, name)
	f.entries[name] = copyEntries(f.entries[fakeDefaultBranch])
	f.chains[name] = f.chains[fakeDefaultBranch]
	sort.Strings(f.branches)
}

// addTag creates a tag over a commit and publishes that commit's entries at the tag ref.
func (f *fakeAPI) addTag(name, commitID string) {
	f.tags = append(f.tags, Tag{Name: name, CommitID: commitID})
	f.entries[name] = copyEntries(f.entries[commitID])
	sort.Slice(f.tags, func(i, j int) bool { return f.tags[i].Name < f.tags[j].Name })
}

// setChain sets a ref's first-parent chain and publishes the entries of every commit in it.
func (f *fakeAPI) setChain(ref string, rootEntries map[string]ObjectInfo, commits ...CommitInfo) {
	f.chains[ref] = commits
	for _, commit := range commits {
		f.chains[commit.ID] = commits
		if _, exists := f.entries[commit.ID]; !exists {
			f.entries[commit.ID] = copyEntries(rootEntries)
		}
	}
}

// stageEntry puts an entry on a ref, the way the catalog's CreateEntry does.
func (f *fakeAPI) stageEntry(ref string, entry ObjectInfo) {
	if f.entries[ref] == nil {
		f.entries[ref] = map[string]ObjectInfo{}
	}
	f.entries[ref][entry.Path] = entry
}

// stageDiff records an uncommitted change.
func (f *fakeAPI) stageDiff(branch, path, changeType string) {
	f.staged[branch] = append(f.staged[branch], DiffInfo{Path: path, Type: changeType})
}

// putObject stores bytes at a physical address.
func (f *fakeAPI) putObject(address string, data []byte) {
	f.objects[normalizeAddress(address)] = append([]byte(nil), data...)
}

// objectAt returns the bytes stored at a physical address.
func (f *fakeAPI) objectAt(address string) ([]byte, bool) {
	data, ok := f.objects[normalizeAddress(address)]
	return data, ok
}

// entry returns one entry of a ref.
func (f *fakeAPI) entry(ref, path string) (ObjectInfo, bool) {
	entry, ok := f.entries[ref][path]
	return entry, ok
}

func copyEntries(entries map[string]ObjectInfo) map[string]ObjectInfo {
	copied := make(map[string]ObjectInfo, len(entries))
	for path, entry := range entries {
		copied[path] = entry
	}
	return copied
}

// GetRepository implements API.
func (f *fakeAPI) GetRepository(context.Context, string) (Repository, error) {
	return f.repo, nil
}

// ListBranches implements API.
func (f *fakeAPI) ListBranches(context.Context, string) ([]string, error) {
	return append([]string(nil), f.branches...), nil
}

// ListTags implements API.
func (f *fakeAPI) ListTags(context.Context, string) ([]Tag, error) {
	return append([]Tag(nil), f.tags...), nil
}

// GetGCRules implements API.
func (f *fakeAPI) GetGCRules(context.Context, string) (GCRules, error) {
	if f.rulesErr != nil {
		return GCRules{}, f.rulesErr
	}
	return f.rules, nil
}

// ListCommits implements API.
func (f *fakeAPI) ListCommits(_ context.Context, _, ref, after string) ([]CommitInfo, bool, error) {
	chain, ok := f.chains[ref]
	if !ok {
		return nil, false, fmt.Errorf("ref %q: %w", ref, ErrNotFound)
	}
	return paginate(chain, after, func(commit CommitInfo) string { return commit.ID })
}

// ListObjects implements API.
func (f *fakeAPI) ListObjects(_ context.Context, _, ref, prefix, after string) ([]ObjectInfo, bool, error) {
	entries := f.entries[ref]
	paths := make([]string, 0, len(entries))
	for path := range entries {
		if strings.HasPrefix(path, prefix) {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	objects := make([]ObjectInfo, 0, len(paths))
	for _, path := range paths {
		objects = append(objects, entries[path])
	}
	return paginate(objects, after, func(object ObjectInfo) string { return object.Path })
}

// StatObject implements API.
func (f *fakeAPI) StatObject(_ context.Context, _, ref, path string) (ObjectInfo, error) {
	if f.beforeStat != nil {
		f.beforeStat(ref, path)
	}
	entry, ok := f.entries[ref][path]
	if !ok {
		return ObjectInfo{}, fmt.Errorf("%q at %q: %w", path, ref, ErrNotFound)
	}
	return entry, nil
}

// GetObject implements API.
func (f *fakeAPI) GetObject(_ context.Context, _, ref, path string) ([]byte, error) {
	entry, ok := f.entries[ref][path]
	if !ok {
		return nil, fmt.Errorf("%q at %q: %w", path, ref, ErrNotFound)
	}
	data, ok := f.objectAt(entry.PhysicalAddress)
	if !ok {
		return nil, fmt.Errorf("object %q behind %q is gone", entry.PhysicalAddress, path)
	}
	return data, nil
}

// UploadObject implements API.
func (f *fakeAPI) UploadObject(_ context.Context, _, branch, path string, data []byte) (ObjectInfo, error) {
	address := f.repo.StorageNamespace + "/" + path
	f.putObject(address, data)
	entry := ObjectInfo{
		Path:            path,
		PhysicalAddress: address,
		Checksum:        fmt.Sprintf("etag-%d", len(f.uploads)),
		SizeBytes:       int64(len(data)),
		Mtime:           time.Unix(1700000000, 0).UTC(),
	}
	f.uploads = append(f.uploads, path)
	f.stageEntry(branch, entry)
	return entry, nil
}

// StageObject implements API.
func (f *fakeAPI) StageObject(_ context.Context, _, branch, path string, object StagedObject) error {
	f.stageEntry(branch, ObjectInfo{
		Path:            path,
		PhysicalAddress: object.PhysicalAddress,
		Checksum:        object.Checksum,
		SizeBytes:       object.SizeBytes,
		Mtime:           object.Mtime,
		Metadata:        object.Metadata,
	})
	return nil
}

// DeleteObject implements API.
func (f *fakeAPI) DeleteObject(_ context.Context, _, branch, path string) error {
	f.deleted = append(f.deleted, deletedEntry{branch: branch, path: path})
	delete(f.entries[branch], path)
	return nil
}

// DiffBranch implements API.
func (f *fakeAPI) DiffBranch(_ context.Context, _, branch, prefix, after string) ([]DiffInfo, bool, error) {
	changes := make([]DiffInfo, 0, len(f.staged[branch]))
	for _, change := range f.staged[branch] {
		if strings.HasPrefix(change.Path, prefix) {
			changes = append(changes, change)
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	return paginate(changes, after, func(change DiffInfo) string { return change.Path })
}

// paginate returns one page of an ordered slice and whether another page exists. after is
// an opaque cursor: the key of the last item of the previous page, exactly like the
// server's PaginationAfter (which is a commit ID for a commit log, not a sort key: a commit
// log is ordered by ancestry, so it must not be paginated by comparing IDs).
func paginate[T any](items []T, after string, key func(T) string) ([]T, bool, error) {
	start := 0
	if after != "" {
		found := false
		for index := range items {
			if key(items[index]) == after {
				start = index + 1
				found = true
				break
			}
		}
		if !found {
			for start < len(items) && key(items[start]) <= after {
				start++
			}
		}
	}
	end := start + fakePageSize
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], end < len(items), nil
}

// testSnapshot is one snapshot of a fixture table: its ID and timestamp.
type testSnapshot struct {
	id          int64
	timestampMs int64
}

// testSnapshotDocument is one snapshot's rendered metadata document.
type testSnapshotDocument struct {
	id           int64
	timestampMs  int64
	manifestList string
	parentID     *int64
}

// testTable is one fixture table and the physical files it references.
type testTable struct {
	api         *fakeAPI
	ref         string
	uuid        uuid.UUID
	namespace   string
	name        string
	root        string
	dataFiles   []string
	metaFile    string
	pointerPath string
}

// pointerPathOf returns the reserved pointer path of a table.
func pointerPathOf(namespace, name string) string {
	return icebergcatalog.ReservedPrefix + "/" + icebergcatalog.TablesDirectory + "/" +
		namespace + "/" + name + "/" + icebergcatalog.TablePointerName
}

// newTestTable writes a table on a ref with the given snapshots, each backed by a real v2
// manifest and manifest list, so the sweep's manifest walk reads genuine Avro. snapshots
// are given oldest first and each one adds one data file.
func newTestTable(t *testing.T, api *fakeAPI, ref, namespace, name string, properties map[string]string, snapshots []testSnapshot) *testTable {
	t.Helper()
	return newTestTableWithUUID(t, api, ref, namespace, name, uuid.MustParse(testTableUUIDText), properties, snapshots)
}

// newTestTableWithUUID is newTestTable with an explicit table UUID, so one test can hold
// several distinct tables on one branch.
func newTestTableWithUUID(t *testing.T, api *fakeAPI, ref, namespace, name string, tableUUID uuid.UUID, properties map[string]string, snapshots []testSnapshot) *testTable {
	t.Helper()
	root := api.repo.StorageNamespace + "/" + icebergcatalog.DataPrefix + "/" + tableUUID.String()
	fixture := &testTable{
		api:         api,
		ref:         ref,
		uuid:        tableUUID,
		namespace:   namespace,
		name:        name,
		root:        root,
		pointerPath: pointerPathOf(namespace, name),
	}
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	documents := make([]testSnapshotDocument, 0, len(snapshots))
	var parentID *int64
	for index, snapshot := range snapshots {
		dataLocation := fmt.Sprintf("%s/data/%05d.parquet", root, index)
		fixture.dataFiles = append(fixture.dataFiles, dataLocation)
		api.putObject(dataLocation, []byte("data"))
		dataFile, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentData, dataLocation,
			iceberg.ParquetFile, nil, nil, nil, 1, int64(len("data")))
		require.NoError(t, err)

		sequenceNumber := int64(index + 1)
		snapshotID := snapshot.id
		manifestLocation := fmt.Sprintf("%s/metadata/manifest-%d.avro", root, snapshot.id)
		var manifestBuffer bytes.Buffer
		manifest, err := iceberg.WriteManifest(manifestLocation, &manifestBuffer, 2, *iceberg.UnpartitionedSpec,
			schema, snapshot.id, []iceberg.ManifestEntry{
				iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &sequenceNumber, &sequenceNumber, dataFile.Build()),
			})
		require.NoError(t, err, "write manifest %d", snapshot.id)
		api.putObject(manifestLocation, manifestBuffer.Bytes())
		registerTableFile(t, api, ref, fixture, manifestLocation, manifestBuffer.Bytes())

		manifestListLocation := fmt.Sprintf("%s/metadata/snap-%d.avro", root, snapshot.id)
		var listBuffer bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(2, &listBuffer, snapshot.id, parentID, &sequenceNumber, 0,
			[]iceberg.ManifestFile{manifest}))
		api.putObject(manifestListLocation, listBuffer.Bytes())
		registerTableFile(t, api, ref, fixture, manifestListLocation, listBuffer.Bytes())

		id := snapshot.id
		documents = append(documents, testSnapshotDocument{
			id:           snapshot.id,
			timestampMs:  snapshot.timestampMs,
			manifestList: manifestListLocation,
			parentID:     parentID,
		})
		parentID = &id
	}
	lastID := int64(0)
	if len(documents) > 0 {
		lastID = documents[len(documents)-1].id
	}
	document := metadataDocument(t, fixture, properties, documents, lastID, nil)
	metadataLocation := fmt.Sprintf("%s/metadata/00000-%s.metadata.json", root, tableUUID)
	fixture.metaFile = metadataLocation
	api.putObject(metadataLocation, document)
	registerMetadataFile(t, api, ref, fixture, metadataLocation, document)
	stagePointer(t, api, ref, fixture, metadataLocation, document)
	return fixture
}

// registerMetadataFile stages the catalog's file registration of one metadata file.
func registerMetadataFile(t *testing.T, api *fakeAPI, ref string, fixture *testTable, location string, data []byte) {
	t.Helper()
	registerTableFile(t, api, ref, fixture, location, data)
}

// registerTableFile stages the catalog's registration of any table file.
func registerTableFile(t *testing.T, api *fakeAPI, ref string, fixture *testTable, location string, data []byte) {
	t.Helper()
	entryPath, err := fileEntryPath(fixture.uuid, fixture.root, location)
	require.NoError(t, err)
	api.stageEntry(ref, ObjectInfo{
		Path:            entryPath,
		PhysicalAddress: location,
		Checksum:        "etag-metadata",
		SizeBytes:       int64(len(data)),
		Mtime:           time.Unix(1700000000, 0).UTC(),
	})
}

// stagePointer stages the table pointer of a fixture table.
func stagePointer(t *testing.T, api *fakeAPI, ref string, fixture *testTable, location string, data []byte) {
	t.Helper()
	pointerMetadata, err := icebergcatalog.EncodeTablePointer(icebergcatalog.TablePointer{
		MetadataLocation: location,
		TableUUID:        fixture.uuid.String(),
		FormatVersion:    2,
	})
	require.NoError(t, err)
	api.stageEntry(ref, ObjectInfo{
		Path:            fixture.pointerPath,
		PhysicalAddress: location,
		Checksum:        "etag-metadata",
		SizeBytes:       int64(len(data)),
		Mtime:           time.Unix(1700000000, 0).UTC(),
		Metadata:        pointerMetadata,
	})
}

// metadataDocument renders a v2 metadata document for the fixture.
func metadataDocument(t *testing.T, fixture *testTable, properties map[string]string, snapshots []testSnapshotDocument, currentSnapshotID int64, metadataLog []string) []byte {
	t.Helper()
	if properties == nil {
		properties = map[string]string{}
	}
	snapshotDocuments := make([]map[string]any, 0, len(snapshots))
	snapshotLog := make([]map[string]any, 0, len(snapshots))
	lastUpdated := int64(1700000000000)
	for index, snapshot := range snapshots {
		document := map[string]any{
			"snapshot-id":     snapshot.id,
			"timestamp-ms":    snapshot.timestampMs,
			"sequence-number": int64(index + 1),
			"summary":         map[string]any{"operation": "append"},
			"manifest-list":   snapshot.manifestList,
		}
		if snapshot.parentID != nil {
			document["parent-snapshot-id"] = *snapshot.parentID
		}
		snapshotDocuments = append(snapshotDocuments, document)
		snapshotLog = append(snapshotLog, map[string]any{"snapshot-id": snapshot.id, "timestamp-ms": snapshot.timestampMs})
		if snapshot.timestampMs >= lastUpdated {
			lastUpdated = snapshot.timestampMs + 1
		}
	}
	// The spec requires snapshot-log to be sorted by timestamp, and last-updated-ms to be
	// at least the newest log entry; the fixtures are written in that order so
	// table.ParseMetadataBytes accepts them.
	sort.SliceStable(snapshotLog, func(i, j int) bool {
		return snapshotLog[i]["timestamp-ms"].(int64) < snapshotLog[j]["timestamp-ms"].(int64)
	})
	document := map[string]any{
		"format-version":        2,
		"table-uuid":            fixture.uuid.String(),
		"location":              fixture.root,
		"last-sequence-number":  int64(len(snapshots)),
		"last-updated-ms":       lastUpdated,
		"last-column-id":        1,
		"current-schema-id":     0,
		"schemas":               []any{mustJSON(t, `{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}`)},
		"default-spec-id":       0,
		"partition-specs":       []any{map[string]any{"spec-id": 0, "fields": []any{}}},
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders":           []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties":            properties,
		"current-snapshot-id":   currentSnapshotID,
		"snapshots":             snapshotDocuments,
		"snapshot-log":          snapshotLog,
		"metadata-log":          metadataLogEntries(metadataLog),
	}
	data, err := json.Marshal(document)
	require.NoError(t, err)
	if _, err := table.ParseMetadataBytes(data); err != nil {
		t.Fatalf("fixture metadata does not parse: %v\n%s", err, data)
	}
	return data
}

// metadataLogEntries renders a metadata-log for the given previous files.
func metadataLogEntries(locations []string) []any {
	entries := make([]any, 0, len(locations))
	for index, location := range locations {
		entries = append(entries, map[string]any{
			"metadata-file": location,
			"timestamp-ms":  int64(1600000000000 + index),
		})
	}
	return entries
}

// metadataV1Document renders a v1 metadata document whose snapshots embed their manifests
// (the deprecated "manifests" field), which the expire phase must refuse.
func metadataV1Document(t *testing.T, fixture *testTable, manifests []string) []byte {
	t.Helper()
	snapshots := make([]map[string]any, 0, len(manifests))
	for index, manifest := range manifests {
		snapshots = append(snapshots, map[string]any{
			"snapshot-id":     int64(index + 1),
			"timestamp-ms":    int64(1600000000000 + index),
			"sequence-number": int64(index + 1),
			"summary":         map[string]any{"operation": "append"},
			"manifests":       []string{manifest},
		})
	}
	document := map[string]any{
		"format-version":        1,
		"table-uuid":            fixture.uuid.String(),
		"location":              fixture.root,
		"last-updated-ms":       int64(1700000000000),
		"last-column-id":        1,
		"current-schema-id":     0,
		"schemas":               []any{mustJSON(t, `{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}`)},
		"default-spec-id":       0,
		"partition-specs":       []any{map[string]any{"spec-id": 0, "fields": []any{}}},
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders":           []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties":            map[string]string{},
		"current-snapshot-id":   int64(len(manifests)),
		"snapshots":             snapshots,
		"snapshot-log":          []any{},
		"metadata-log":          []any{},
	}
	data, err := json.Marshal(document)
	require.NoError(t, err)
	if _, err := table.ParseMetadataBytes(data); err != nil {
		t.Fatalf("fixture v1 metadata does not parse: %v\n%s", err, data)
	}
	return data
}

// parseMetadata parses a metadata document the way the phases do.
func parseMetadata(t *testing.T, data []byte) table.Metadata {
	t.Helper()
	metadata, err := table.ParseMetadataBytes(data)
	require.NoError(t, err)
	return metadata
}

func mustJSON(t *testing.T, text string) any {
	t.Helper()
	var value any
	require.NoError(t, json.Unmarshal([]byte(text), &value))
	return value
}

// writeVersion writes one metadata version of a fixture table on ref - one snapshot whose
// manifest references dataFile - and stages its file registration and pointer there. It is
// the fixture the mark/sweep tests use to create several distinct live table states.
func (f *testTable) writeVersion(t *testing.T, ref string, version int, snapshotID int64, timestampMs int64, dataFile string) string {
	t.Helper()
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	data, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData, dataFile,
		iceberg.ParquetFile, nil, nil, nil, 1, int64(len("data")))
	require.NoError(t, err)
	sequenceNumber := int64(1)
	snapshot := snapshotID
	manifestLocation := fmt.Sprintf("%s/metadata/manifest-%d.avro", f.root, snapshotID)
	var manifestBuffer bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestLocation, &manifestBuffer, 2, *iceberg.UnpartitionedSpec,
		schema, snapshotID, []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshot, &sequenceNumber, &sequenceNumber, data.Build()),
		})
	require.NoError(t, err)
	f.api.putObject(manifestLocation, manifestBuffer.Bytes())
	registerTableFile(t, f.api, ref, f, manifestLocation, manifestBuffer.Bytes())
	manifestListLocation := fmt.Sprintf("%s/metadata/snap-%d-%d.avro", f.root, snapshotID, version)
	var listBuffer bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(2, &listBuffer, snapshotID, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest}))
	f.api.putObject(manifestListLocation, listBuffer.Bytes())
	registerTableFile(t, f.api, ref, f, manifestListLocation, listBuffer.Bytes())

	documents := []testSnapshotDocument{{
		id:           snapshotID,
		timestampMs:  timestampMs,
		manifestList: manifestListLocation,
	}}
	var metadataLog []string
	if entry, ok := f.api.entry(ref, f.pointerPath); ok {
		if previous, err := icebergcatalog.DecodeTablePointer(entry.Metadata); err == nil {
			metadataLog = []string{previous.MetadataLocation}
		}
	}
	document := metadataDocument(t, f, nil, documents, snapshotID, metadataLog)
	metadataLocation := fmt.Sprintf("%s/metadata/%05d-%s.metadata.json", f.root, version, f.uuid)
	f.api.putObject(metadataLocation, document)
	registerMetadataFile(t, f.api, ref, f, metadataLocation, document)
	stagePointer(t, f.api, ref, f, metadataLocation, document)
	return metadataLocation
}

// addRegistration stages one catalog file registration for a fixture table.
func (f *testTable) addRegistration(t *testing.T, ref, relativePath, address string, modified time.Time) string {
	t.Helper()
	entryPath, err := icebergcatalog.TableFilePath(f.uuid, relativePath)
	require.NoError(t, err)
	f.api.stageEntry(ref, ObjectInfo{
		Path:            entryPath,
		PhysicalAddress: address,
		Checksum:        "etag-registration",
		SizeBytes:       int64(len("data")),
		Mtime:           modified,
	})
	return entryPath
}

// addTableFileEntry stages one catalog file registration for an arbitrary table UUID, which
// is how the unreadable-table tests get file registrations without a readable metadata.
func addTableFileEntry(t *testing.T, api *fakeAPI, ref string, tableUUID uuid.UUID, relativePath, address string, modified time.Time) string {
	t.Helper()
	entryPath, err := icebergcatalog.TableFilePath(tableUUID, relativePath)
	require.NoError(t, err)
	api.stageEntry(ref, ObjectInfo{
		Path:            entryPath,
		PhysicalAddress: address,
		Checksum:        "etag-registration",
		SizeBytes:       int64(len("data")),
		Mtime:           modified,
	})
	return entryPath
}
