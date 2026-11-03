package icebergcatalog

// Tests for the commit path (ICEBERG_CATALOG_PLAN.md §6.2, §12 "requirement and update
// application (table-driven from spec examples)"). The fixture is the real catalog store of
// store_test.go (graveler + committed ranges on an in-memory KV store, in-memory block adapter),
// so "staged, not committed", "the pointer swap is a compare-and-swap" and "the metadata file is
// a real object" are facts of the code the server runs.
//
// The payloads are written from the per-action schemas of the Apache Iceberg REST OpenAPI spec
// (Apache-2.0),
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml — the same
// source updates_coverage_test.go uses for the I0-1 gate.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// specRequirementTypes lists every member of components.schemas.TableRequirement in the Iceberg
// REST OpenAPI spec, in spec order. TestRequirements drives its coverage assertion from this list,
// so a requirement type the spec adds without a case here fails the test.
var specRequirementTypes = []string{
	"assert-create",
	"assert-table-uuid",
	"assert-ref-snapshot-id",
	"assert-last-assigned-field-id",
	"assert-current-schema-id",
	"assert-last-assigned-partition-id",
	"assert-default-spec-id",
	"assert-default-sort-order-id",
}

// specUpdateActions lists every member of components.schemas.TableUpdate.anyOf in the spec, in
// spec order. TestUpdates drives its coverage assertion from it.
var specUpdateActions = []string{
	"assign-uuid",
	"upgrade-format-version",
	"add-schema",
	"set-current-schema",
	"add-spec",
	"set-default-spec",
	"add-sort-order",
	"set-default-sort-order",
	"add-snapshot",
	"set-snapshot-ref",
	"remove-snapshots",
	"remove-snapshot-ref",
	"set-location",
	"set-properties",
	"remove-properties",
	"set-statistics",
	"remove-statistics",
	"set-partition-statistics",
	"remove-partition-statistics",
	"remove-partition-specs",
	"remove-schemas",
	"add-encryption-key",
	"remove-encryption-key",
}

// recordingAdapter is the fixture's block adapter with a log of every object it was asked to
// write. A commit writes exactly one object - the new metadata.json - so counting the writes is
// how the tests distinguish "refused before writing" from "wrote an orphan".
type recordingAdapter struct {
	block.Adapter

	mu   sync.Mutex
	puts []string
}

func (a *recordingAdapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) (*block.PutResponse, error) {
	a.mu.Lock()
	a.puts = append(a.puts, obj.Identifier)
	a.mu.Unlock()
	return a.Adapter.Put(ctx, obj, sizeBytes, reader, opts)
}

// reset forgets the writes so a subtest starts from zero.
func (a *recordingAdapter) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.puts = nil
}

// putCount is the number of objects written since the last reset.
func (a *recordingAdapter) putCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.puts)
}

func (a *recordingAdapter) clone() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.puts...)
}

// failingCatalog is the fixture's catalog with a CreateEntry that fails for the table pointer,
// which is how a test forces the pointer swap of a commit to lose its compare-and-swap (or to fail
// for a reason that is not a precondition failure). Every other entry - the table files §6.2
// step 5 stages, which go through the same CreateEntry - succeeds, so the failure under test is
// the swap and not the staging that precedes it.
type failingCatalog struct {
	Catalog
	err error
}

func (c *failingCatalog) CreateEntry(ctx context.Context, repositoryID, branch string, entry catalog.DBEntry, opts ...graveler.SetOptionsFunc) error {
	if strings.HasSuffix(entry.Path, "/"+TablePointerName) {
		return c.err
	}
	return c.Catalog.CreateEntry(ctx, repositoryID, branch, entry, opts...)
}

// stagingFailureCatalog is the fixture's catalog with a CreateEntry that fails for staged table
// files and succeeds for everything else. It is how a test reaches the one case that is neither a
// 400 nor a 409: metadata.json is written, some file entries may be staged, and the pointer has
// not moved, so the client cannot know whether the commit landed (§2.1).
type stagingFailureCatalog struct {
	Catalog
	err error
}

func (c *stagingFailureCatalog) CreateEntry(ctx context.Context, repositoryID, branch string, entry catalog.DBEntry, opts ...graveler.SetOptionsFunc) error {
	if strings.Contains(entry.Path, "/"+FilesDirectory+"/") {
		return c.err
	}
	return c.Catalog.CreateEntry(ctx, repositoryID, branch, entry, opts...)
}

// commitFixture is a storeFixture with a recording block adapter and one table on main, whose
// pointer names a real metadata.json. That is the state a client commits against.
type commitFixture struct {
	*storeFixture

	addr          Address
	tableLocation string
	baseLocation  string
	baseMetadata  table.Metadata
	writes        *recordingAdapter
}

func newCommitFixture(t *testing.T) *commitFixture {
	t.Helper()

	f := newStoreFixture(t)
	writes := &recordingAdapter{Adapter: f.catalog.BlockAdapter}
	f.store = NewStore(f.catalog, writes)

	tableLocation := "mem://" + storeTestRepo + "-ns/_iceberg/" + storeTestUUID
	baseLocation := f.putMetadata(t,
		"_iceberg/"+storeTestUUID+"/metadata/00001.metadata.json",
		storeTestMetadata(t, storeTestUUID, tableLocation))
	addr := Address{
		Repo:      storeTestRepo,
		Ref:       "main",
		Namespace: Namespace{"inventory", "books"},
		Table:     "orders",
	}
	require.NoError(t, f.store.CreateTable(f.ctx, addr, TablePointer{
		MetadataLocation: baseLocation,
		TableUUID:        storeTestUUID,
		FormatVersion:    2,
	}), "create the table the commits are built on")

	base, err := f.store.GetTable(f.ctx, addr)
	require.NoError(t, err, "read the base table")

	// The successful cases of TestUpdates add snapshots whose manifest lists the commit path reads
	// while staging files (§6.2 step 5). Write them, empty, under the table root, so those cases
	// test the update they are about rather than the registration that follows it.
	for _, snapshotID := range []string{"5555555555555555555", "6666666666666666666"} {
		f.writeEmptyManifestList(t, snapshotID)
	}

	writes.reset()
	return &commitFixture{
		storeFixture:  f,
		addr:          addr,
		tableLocation: tableLocation,
		baseLocation:  baseLocation,
		baseMetadata:  base.Metadata,
		writes:        writes,
	}
}

// commit decodes and runs one commit body, exactly as the HTTP handler will: Decode then Commit.
func (f *commitFixture) commit(t *testing.T, body string) (*CommitResult, error) {
	t.Helper()
	request, err := DecodeTableCommitRequest([]byte(body))
	require.NoError(t, err, "decode commit request %s", body)
	return f.store.Commit(f.ctx, f.addr, request)
}

// writeEmptyManifestList writes a v2 manifest list with no manifests for snapshotID under the
// table root and returns its URI: the smallest real object a snapshot can name, so a test of the
// commit path does not have to model a manifest to exercise file registration.
func (f *storeFixture) writeEmptyManifestList(t *testing.T, snapshotID string) string {
	t.Helper()
	var buffer bytes.Buffer
	parsedID, err := strconv.ParseInt(snapshotID, 10, 64)
	require.NoError(t, err, "parse snapshot id %q", snapshotID)
	sequenceNumber := int64(35)
	require.NoError(t, iceberg.WriteManifestList(2, &buffer, parsedID, nil, &sequenceNumber, 0, nil),
		"write an empty manifest list for snapshot %s", snapshotID)
	return f.putMetadata(t, "_iceberg/"+storeTestUUID+"/metadata/snap-"+snapshotID+".avro", buffer.String())
}

// mustCommit runs a commit body and requires it to succeed.
func (f *commitFixture) mustCommit(t *testing.T, body string) *CommitResult {
	t.Helper()
	result, err := f.commit(t, body)
	require.NoError(t, err, "commit %s", body)
	require.NotNil(t, result)
	return result
}

// currentPointer reads the pointer the branch answers with.
func (f *commitFixture) currentPointer(t *testing.T) TablePointer {
	t.Helper()
	got, err := f.store.GetTable(f.ctx, f.addr)
	require.NoError(t, err, "read the table")
	return got.Pointer
}

// objectExists reports whether the block adapter holds an object at a fully qualified location.
func (f *commitFixture) objectExists(t *testing.T, location string) bool {
	t.Helper()
	repo, err := f.catalog.GetRepository(f.ctx, storeTestRepo)
	require.NoError(t, err, "get repository")
	exists, err := f.catalog.BlockAdapter.Exists(f.ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	})
	require.NoError(t, err, "stat %s", location)
	return exists
}

// readObject reads an object at a fully qualified location.
func (f *commitFixture) readObject(t *testing.T, location string) string {
	t.Helper()
	repo, err := f.catalog.GetRepository(f.ctx, storeTestRepo)
	require.NoError(t, err, "get repository")
	reader, err := f.catalog.BlockAdapter.Get(f.ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	})
	require.NoError(t, err, "read %s", location)
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	require.NoError(t, err, "read %s", location)
	return string(data)
}

// lastMetadataLogEntry returns the last metadata-log entry of meta.
func lastMetadataLogEntry(t *testing.T, meta table.Metadata) table.MetadataLogEntry {
	t.Helper()
	var last table.MetadataLogEntry
	found := false
	for entry := range meta.PreviousFiles() {
		last = entry
		found = true
	}
	require.True(t, found, "metadata-log is empty, the commit did not log the file it replaced")
	return last
}

// noChange asserts that a refused commit touched nothing: no new metadata.json, no pointer move.
func (f *commitFixture) noChange(t *testing.T, before TablePointer) {
	t.Helper()
	require.Equal(t, 0, f.writes.putCount(), "a refused commit must not write a metadata file")
	require.Equal(t, before, f.currentPointer(t), "a refused commit must not move the pointer")
}

// TestCommit covers the end-to-end path of §6.2: apply, write, swap, and every error class the
// commit endpoint can answer with.
func TestCommit(t *testing.T) {
	t.Run("applies updates, writes metadata.json and stages the pointer", func(t *testing.T) {
		f := newCommitFixture(t)
		headBefore := f.headRef(t)

		result := f.mustCommit(t, `{"requirements":[],"updates":[
			{"action":"set-properties","updates":{"owner":"lakefs","write.metadata.compression-codec":"gzip"}}]}`)

		require.Equal(t, 1, f.writes.putCount(), "a commit writes exactly one object, the new metadata.json")
		require.Equal(t, storeTestUUID, result.Pointer.TableUUID)
		require.Equal(t, 2, result.Pointer.FormatVersion)
		require.NotEqual(t, f.baseLocation, result.Pointer.MetadataLocation,
			"a commit must never reuse the metadata file it replaces")
		require.Contains(t, result.Pointer.MetadataLocation, f.tableLocation+"/"+MetadataDirectory+"/",
			"the new file is written under the table location")
		require.Equal(t, "lakefs", result.Metadata.Properties()["owner"])

		// The file exists, holds exactly the bytes the result reports, and parses back.
		require.True(t, f.objectExists(t, result.Pointer.MetadataLocation))
		require.Equal(t, string(result.MetadataJSON), f.readObject(t, result.Pointer.MetadataLocation))
		reparsed, err := table.ParseMetadataBytes(result.MetadataJSON)
		require.NoError(t, err, "the written metadata.json must be what our own reader accepts")
		require.Equal(t, storeTestUUID, reparsed.TableUUID().String())
		require.Equal(t, "lakefs", reparsed.Properties()["owner"])

		// metadata-log names the file this commit replaced, with the base's last-updated-ms. This
		// is the I0-1 correction 2: the builder takes the *previous* location, not the new one.
		entry := lastMetadataLogEntry(t, result.Metadata)
		require.Equal(t, f.baseLocation, entry.MetadataFile)
		require.Equal(t, f.baseMetadata.LastUpdatedMillis(), entry.TimestampMs)

		// Staged, not committed: the branch head did not move.
		require.Equal(t, headBefore, f.headRef(t), "a catalog commit must not create a lakeFS commit")

		// The read half resolves the new pointer and the new metadata.
		require.Equal(t, result.Pointer, f.currentPointer(t))
	})

	t.Run("a second commit builds on the staged pointer", func(t *testing.T) {
		f := newCommitFixture(t)

		first := f.mustCommit(t, `{"requirements":[{"type":"assert-table-uuid","uuid":"`+storeTestUUID+`"}],
			"updates":[{"action":"set-properties","updates":{"step":"1"}}]}`)
		second := f.mustCommit(t, `{"requirements":[{"type":"assert-current-schema-id","current-schema-id":1}],
			"updates":[{"action":"set-properties","updates":{"step":"2"}}]}`)

		require.NotEqual(t, first.Pointer.MetadataLocation, second.Pointer.MetadataLocation)
		require.Equal(t, 2, f.writes.putCount())
		require.Equal(t, first.Pointer.MetadataLocation, lastMetadataLogEntry(t, second.Metadata).MetadataFile,
			"the second commit chained onto the first staged commit, not onto the base")

		names := f.writes.clone()
		require.Contains(t, names[0], "/metadata/00002-", "the sequence continues the base file's")
		require.Contains(t, names[1], "/metadata/00003-")
		require.Equal(t, second.Pointer, f.currentPointer(t))
	})

	t.Run("a stale requirement is a 409 and writes nothing", func(t *testing.T) {
		f := newCommitFixture(t)
		before := f.currentPointer(t)

		result, err := f.commit(t, `{"requirements":[{"type":"assert-table-uuid","uuid":"`+storeAltUUID+`"}],
			"updates":[{"action":"set-properties","updates":{"owner":"someone"}}]}`)
		require.Nil(t, result)

		var failed *CommitFailedError
		require.ErrorAs(t, err, &failed)
		require.Equal(t, "assert-table-uuid", failed.Requirement)
		require.ErrorIs(t, err, ErrCommitFailed)
		require.Equal(t, 409, failed.HTTPStatusCode())
		require.Equal(t, ErrTypeCommitFailed, failed.ErrorType())
		require.NotErrorIs(t, err, ErrCommitStateUnknown, "a requirement failure is not an unknown outcome")

		f.noChange(t, before)
	})

	t.Run("a lost compare-and-swap is a 409 and leaves an orphan metadata file", func(t *testing.T) {
		f := newCommitFixture(t)
		before := f.currentPointer(t)

		// A concurrent commit moved the pointer between our read and our swap: the condition sees
		// a metadata-location that is not the base's and the compare-and-swap is refused.
		f.store = NewStore(&failingCatalog{Catalog: f.catalog, err: graveler.ErrPreconditionFailed}, f.writes)

		result, err := f.commit(t, `{"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"loser"}}]}`)
		require.Nil(t, result)

		var failed *CommitFailedError
		require.ErrorAs(t, err, &failed)
		require.ErrorIs(t, err, graveler.ErrPreconditionFailed, "the caller can tell a lost race from a bad request")
		require.Equal(t, 409, failed.HTTPStatusCode())
		require.NotErrorIs(t, err, ErrCommitStateUnknown)

		// §6.2: the metadata file is written before the swap, and on a lost swap it stays
		// unreferenced on purpose. GC collects it.
		require.Equal(t, 1, f.writes.putCount())
		orphan := f.writes.clone()[0]
		require.True(t, f.objectExists(t, orphan), "the orphan metadata file is intentional")
		require.NotEqual(t, orphan, before.MetadataLocation)
		require.Equal(t, before, f.currentPointer(t), "the winner's pointer must survive")
	})

	t.Run("any other swap failure is a 500 CommitStateUnknownException", func(t *testing.T) {
		f := newCommitFixture(t)
		before := f.currentPointer(t)

		swapErr := errors.New("kv unavailable")
		f.store = NewStore(&failingCatalog{Catalog: f.catalog, err: swapErr}, f.writes)

		result, err := f.commit(t, `{"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"maybe"}}]}`)
		require.Nil(t, result)

		var unknown *CommitStateUnknownError
		require.ErrorAs(t, err, &unknown)
		require.ErrorIs(t, err, ErrCommitStateUnknown)
		require.ErrorIs(t, err, swapErr)
		require.Equal(t, 500, unknown.HTTPStatusCode())
		require.Equal(t, ErrorTypeCommitStateUnknown, unknown.ErrorType())
		require.Equal(t, ErrorTypeCommitStateUnknown, "CommitStateUnknownException")

		// This is the case §2.1 forbids reporting as 409: the commit may have landed, so the client
		// must re-read the table. The file it named is the one that would have been current.
		require.NotErrorIs(t, err, ErrCommitFailed)
		require.NotEmpty(t, unknown.MetadataLocation)
		require.True(t, f.objectExists(t, unknown.MetadataLocation))
		require.Equal(t, before, f.currentPointer(t))
	})

	t.Run("a graveler refusal of the swap is not a 409", func(t *testing.T) {
		f := newCommitFixture(t)
		before := f.currentPointer(t)

		// The swap is an ordinary catalog.CreateEntry without Force (I2-1), so graveler's own
		// refusals reach it: a protected branch (this error), a read-only repository, a KV failure.
		// The metadata file is already written when they happen, so the commit is reported as an
		// unknown outcome rather than as the 409 that would tell the client to retry the same
		// doomed request (§2.1). The graveler refusal is preserved for the log and for errors.Is.
		f.store = NewStore(&failingCatalog{Catalog: f.catalog, err: graveler.ErrWriteToProtectedBranch}, f.writes)

		_, err := f.commit(t, `{"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"blocked"}}]}`)
		var unknown *CommitStateUnknownError
		require.ErrorAs(t, err, &unknown)
		require.ErrorIs(t, err, graveler.ErrWriteToProtectedBranch)
		require.NotErrorIs(t, err, ErrCommitFailed)
		require.Equal(t, before, f.currentPointer(t))
	})

	t.Run("a missing table is the read half's 404", func(t *testing.T) {
		f := newCommitFixture(t)
		f.addr.Table = "invoices"

		_, err := f.commit(t, `{"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"nobody"}}]}`)
		var missing *NoSuchTableException
		require.ErrorAs(t, err, &missing)
		require.Equal(t, 0, f.writes.putCount(), "a commit to a missing table writes nothing")
	})

	t.Run("set-location is refused with a 400", func(t *testing.T) {
		f := newCommitFixture(t)
		before := f.currentPointer(t)

		result, err := f.commit(t, `{"requirements":[],"updates":[{"action":"set-location","location":"s3://elsewhere/table"}]}`)
		require.Nil(t, result)

		var bad *BadRequestError
		require.ErrorAs(t, err, &bad)
		require.ErrorIs(t, err, ErrSetLocationUnsupported)
		require.Equal(t, 400, bad.HTTPStatusCode())
		require.Equal(t, ErrTypeBadRequest, bad.ErrorType())
		require.NotErrorIs(t, err, ErrCommitFailed)

		f.noChange(t, before)
	})

	t.Run("a malformed body is a 400", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			body string
		}{
			{"unknown action", `{"requirements":[],"updates":[{"action":"rewrite-everything"}]}`},
			{"missing action", `{"requirements":[],"updates":[{"uuid":"` + storeTestUUID + `"}]}`},
			{"missing required field", `{"requirements":[],"updates":[{"action":"upgrade-format-version"}]}`},
			{"unknown requirement type", `{"requirements":[{"type":"assert-nothing"}],"updates":[]}`},
			{"not an object", `[]`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := newCommitFixture(t)
				before := f.currentPointer(t)

				request, err := DecodeTableCommitRequest([]byte(tc.body))
				require.Empty(t, request)
				var bad *BadRequestError
				require.ErrorAs(t, err, &bad)
				require.Equal(t, 400, bad.HTTPStatusCode())

				f.noChange(t, before)
			})
		}
	})
}

// TestCommitStagesTableFiles covers §6.2 step 5 wired into the commit path: a commit that adds a
// snapshot stages the new metadata file and the files that snapshot references as lakeFS entries on
// the branch, between the metadata write and the pointer swap. It also covers the two refusals that
// step introduces - a file outside the table's root (§13) and a commit above
// iceberg.max_staged_files (§10) - and the unknown outcome of a staging failure (§2.1).
func TestCommitStagesTableFiles(t *testing.T) {
	// addSnapshotBody is a commit whose only update adds one snapshot naming manifestList.
	addSnapshotBody := func(snapshotID int64, manifestList string) string {
		return fmt.Sprintf(`{"requirements":[],"updates":[{"action":"add-snapshot","snapshot":{
			"snapshot-id":%d,
			"parent-snapshot-id":3055729675574597004,
			"sequence-number":35,
			"timestamp-ms":1602638574000,
			"manifest-list":%q,
			"summary":{"operation":"append"}}}]}`, snapshotID, manifestList)
	}

	t.Run("a commit that adds no snapshot stages only its metadata file", func(t *testing.T) {
		f := newCommitFixture(t)
		recorded := &entryRecordingCatalog{Catalog: f.catalog}
		f.store = NewStore(recorded, f.writes)

		result := f.mustCommit(t, `{"requirements":[],"updates":[{"action":"set-properties",
			"updates":{"owner":"lakefs"}}]}`)

		// The metadata.json is registered even though no snapshot was added: the pointer names it
		// from user metadata, not from an entry address, so nothing else would make the committed
		// GC retain it (§6.3 step 1).
		staged := fileEntries(recorded.createdEntries())
		require.Len(t, staged, 1)
		relative := strings.TrimPrefix(result.Pointer.MetadataLocation, f.tableLocation+"/")
		want, err := TableFilePath(uuid.MustParse(storeTestUUID), relative)
		require.NoError(t, err)
		require.Equal(t, want, staged[0].Path)
		require.Equal(t, result.Pointer.MetadataLocation, staged[0].PhysicalAddress)
		require.EqualValues(t, len(result.MetadataJSON), staged[0].Size)
	})

	t.Run("stages the metadata file and the snapshot's manifest list", func(t *testing.T) {
		f := newCommitFixture(t)
		recorded := &entryRecordingCatalog{Catalog: f.catalog}
		f.store = NewStore(recorded, f.writes)
		headBefore := f.headRef(t)

		const snapshotID = int64(1234567890123456789)
		manifestList := f.writeEmptyManifestList(t, strconv.FormatInt(snapshotID, 10))
		result := f.mustCommit(t, addSnapshotBody(snapshotID, manifestList))

		// The keys are deterministic: the table UUID, then the file's path relative to the table
		// root (§6.3), so a retried commit overwrites the same entries.
		tableUUID := uuid.MustParse(storeTestUUID)
		metadataRelative := strings.TrimPrefix(result.Pointer.MetadataLocation, f.tableLocation+"/")
		require.True(t, strings.HasPrefix(metadataRelative, MetadataDirectory+"/"),
			"the metadata file is written under the table root, got %q", metadataRelative)
		wantMetadataEntry, err := TableFilePath(tableUUID, metadataRelative)
		require.NoError(t, err)
		wantManifestListEntry, err := TableFilePath(tableUUID, "metadata/snap-"+strconv.FormatInt(snapshotID, 10)+".avro")
		require.NoError(t, err)

		staged := fileEntries(recorded.createdEntries())
		byPath := make(map[string]catalog.DBEntry, len(staged))
		for _, entry := range staged {
			byPath[entry.Path] = entry
		}
		require.Len(t, staged, 2, "the metadata file and the manifest list, and nothing else")
		require.Contains(t, byPath, wantMetadataEntry)
		require.Contains(t, byPath, wantManifestListEntry)

		// Each entry is a full address pointing at the object that was written, not a copy.
		require.Equal(t, catalog.AddressTypeFull, byPath[wantMetadataEntry].AddressType)
		require.Equal(t, result.Pointer.MetadataLocation, byPath[wantMetadataEntry].PhysicalAddress)
		require.EqualValues(t, len(result.MetadataJSON), byPath[wantMetadataEntry].Size)
		require.Equal(t, catalog.AddressTypeFull, byPath[wantManifestListEntry].AddressType)
		require.Equal(t, manifestList, byPath[wantManifestListEntry].PhysicalAddress)
		// The creation date is the snapshot's timestamp, which is what the uncommitted GC ages a
		// staged entry by (spike S-I2 §5.1 item 3).
		require.Equal(t, time.UnixMilli(1602638574000).UTC(), byPath[wantManifestListEntry].CreationDate)

		// Staged, not committed: the entry is readable on the branch and the branch head did not move.
		entry, err := f.entryAt(t, "main", wantManifestListEntry)
		require.NoError(t, err, "the staged table file must be readable on the branch")
		require.Equal(t, manifestList, entry.PhysicalAddress)
		require.Equal(t, headBefore, f.headRef(t), "a catalog commit stages; it never commits lakeFS")
	})

	t.Run("a file outside the table root is a 400 and stages nothing", func(t *testing.T) {
		f := newCommitFixture(t)
		recorded := &entryRecordingCatalog{Catalog: f.catalog}
		f.store = NewStore(recorded, f.writes)
		before := f.currentPointer(t)

		_, err := f.commit(t, addSnapshotBody(1234567890123456790,
			"s3://another-team/secret/metadata/snap-1.avro"))
		require.Error(t, err)

		var bad *BadRequestError
		require.ErrorAs(t, err, &bad)
		require.ErrorIs(t, err, ErrForeignTableFile,
			"the client can tell a foreign path from a server failure")
		require.Equal(t, 400, bad.HTTPStatusCode())
		require.Equal(t, ErrTypeBadRequest, bad.ErrorType())
		require.NotErrorIs(t, err, ErrCommitStateUnknown)
		require.NotErrorIs(t, err, ErrCommitFailed)

		// The plan is refused before the first write, so nothing is staged. The metadata file of
		// step 4 was written; it is unreferenced and GC collects it.
		require.Empty(t, fileEntries(recorded.createdEntries()), "a refused plan stages nothing")
		require.Equal(t, before, f.currentPointer(t), "a refused commit must not move the pointer")
	})

	t.Run("a commit above iceberg.max_staged_files is a 400", func(t *testing.T) {
		f := newCommitFixture(t)
		recorded := &entryRecordingCatalog{Catalog: f.catalog}
		f.store = NewStore(recorded, f.writes, WithMaxStagedFiles(1))
		before := f.currentPointer(t)

		const snapshotID = int64(1234567890123456791)
		manifestList := f.writeEmptyManifestList(t, strconv.FormatInt(snapshotID, 10))
		_, err := f.commit(t, addSnapshotBody(snapshotID, manifestList))

		var limit *TableFileLimitError
		require.ErrorAs(t, err, &limit)
		require.ErrorIs(t, err, ErrTooManyTableFiles)
		require.Equal(t, 1, limit.Max, "the cap is the one the store was built with")
		require.Equal(t, 400, limit.HTTPStatusCode())
		var bad *BadRequestError
		require.ErrorAs(t, err, &bad)
		require.Equal(t, 400, bad.HTTPStatusCode())
		require.NotErrorIs(t, err, ErrCommitStateUnknown)
		require.Empty(t, fileEntries(recorded.createdEntries()))
		require.Equal(t, before, f.currentPointer(t))
	})

	t.Run("the default cap is the documented 50000", func(t *testing.T) {
		f := newCommitFixture(t)
		require.Equal(t, DefaultMaxStagedFiles, f.store.MaxStagedFiles())
		require.Equal(t, 50_000, DefaultMaxStagedFiles, "iceberg.max_staged_files defaults to 50000")
	})

	t.Run("a staging failure is a 500 CommitStateUnknownException", func(t *testing.T) {
		f := newCommitFixture(t)
		stagingErr := errors.New("kv unavailable while staging")
		f.store = NewStore(&stagingFailureCatalog{Catalog: f.catalog, err: stagingErr}, f.writes)
		before := f.currentPointer(t)

		const snapshotID = int64(1234567890123456792)
		manifestList := f.writeEmptyManifestList(t, strconv.FormatInt(snapshotID, 10))
		_, err := f.commit(t, addSnapshotBody(snapshotID, manifestList))

		var unknown *CommitStateUnknownError
		require.ErrorAs(t, err, &unknown)
		require.ErrorIs(t, err, stagingErr)
		require.Equal(t, 500, unknown.HTTPStatusCode())
		require.Equal(t, ErrorTypeCommitStateUnknown, unknown.ErrorType())
		require.NotErrorIs(t, err, ErrCommitFailed, "a staging failure is not a conflict")
		require.Equal(t, before, f.currentPointer(t), "the pointer did not move")
	})
}

// fileEntries keeps the entries a catalog was asked to create that are staged table files (§6.3),
// dropping the pointer writes of the same commit.
func fileEntries(entries []catalog.DBEntry) []catalog.DBEntry {
	prefix := ReservedPrefix + "/" + FilesDirectory + "/"
	files := make([]catalog.DBEntry, 0, len(entries))
	for _, entry := range entries {
		if strings.HasPrefix(entry.Path, prefix) {
			files = append(files, entry)
		}
	}
	return files
}

// TestRequirements is the §12 "requirement application" table: every requirement type of the REST
// spec, once satisfied and once failing, driven through a real commit so the metadata under test
// is the metadata.json the read half loaded, not an in-memory fixture.
func TestRequirements(t *testing.T) {
	tests := []struct {
		name        string
		requirement string // one components.schemas.TableRequirement payload
		satisfied   bool   // against the fixture table
	}{
		{
			// assert-create is the mirror image: it only holds when no table exists, so against an
			// existing table it must fail. The passing direction (a nil metadata) is covered by
			// TestRequirements/assert-create-on-a-missing-table and belongs to the staged-create
			// path, I2-4.
			name:        "assert-create/table-exists",
			requirement: `{"type":"assert-create"}`,
			satisfied:   false,
		},
		{
			name:        "assert-table-uuid/matching",
			requirement: `{"type":"assert-table-uuid","uuid":"` + storeTestUUID + `"}`,
			satisfied:   true,
		},
		{
			name:        "assert-table-uuid/different",
			requirement: `{"type":"assert-table-uuid","uuid":"` + storeAltUUID + `"}`,
			satisfied:   false,
		},
		{
			// The fixture's tag "test" points at snapshot 3051729675574597004.
			name:        "assert-ref-snapshot-id/tag-points-there",
			requirement: `{"type":"assert-ref-snapshot-id","ref":"test","snapshot-id":3051729675574597004}`,
			satisfied:   true,
		},
		{
			name:        "assert-ref-snapshot-id/tag-points-elsewhere",
			requirement: `{"type":"assert-ref-snapshot-id","ref":"test","snapshot-id":42}`,
			satisfied:   false,
		},
		{
			// snapshot-id null means "this ref must not exist", and the base has no "audit" ref.
			// (Not "main": a table with a current snapshot has an implicit main ref even when refs
			// does not list it, which is the v1 behaviour iceberg-go models.)
			name:        "assert-ref-snapshot-id/absent-ref",
			requirement: `{"type":"assert-ref-snapshot-id","ref":"audit","snapshot-id":null}`,
			satisfied:   true,
		},
		{
			name:        "assert-ref-snapshot-id/ref-must-not-exist",
			requirement: `{"type":"assert-ref-snapshot-id","ref":"test","snapshot-id":null}`,
			satisfied:   false,
		},
		{
			name:        "assert-last-assigned-field-id/matching",
			requirement: `{"type":"assert-last-assigned-field-id","last-assigned-field-id":3}`,
			satisfied:   true,
		},
		{
			name:        "assert-last-assigned-field-id/ahead",
			requirement: `{"type":"assert-last-assigned-field-id","last-assigned-field-id":4}`,
			satisfied:   false,
		},
		{
			name:        "assert-current-schema-id/matching",
			requirement: `{"type":"assert-current-schema-id","current-schema-id":1}`,
			satisfied:   true,
		},
		{
			name:        "assert-current-schema-id/different",
			requirement: `{"type":"assert-current-schema-id","current-schema-id":0}`,
			satisfied:   false,
		},
		{
			name:        "assert-last-assigned-partition-id/matching",
			requirement: `{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":1000}`,
			satisfied:   true,
		},
		{
			name:        "assert-last-assigned-partition-id/different",
			requirement: `{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":999}`,
			satisfied:   false,
		},
		{
			name:        "assert-default-spec-id/matching",
			requirement: `{"type":"assert-default-spec-id","default-spec-id":0}`,
			satisfied:   true,
		},
		{
			name:        "assert-default-spec-id/different",
			requirement: `{"type":"assert-default-spec-id","default-spec-id":1}`,
			satisfied:   false,
		},
		{
			name:        "assert-default-sort-order-id/matching",
			requirement: `{"type":"assert-default-sort-order-id","default-sort-order-id":3}`,
			satisfied:   true,
		},
		{
			name:        "assert-default-sort-order-id/different",
			requirement: `{"type":"assert-default-sort-order-id","default-sort-order-id":0}`,
			satisfied:   false,
		},
	}

	covered := make(map[string]bool)
	for _, tc := range tests {
		covered[strings.SplitN(tc.name, "/", 2)[0]] = true
	}
	for _, reqType := range specRequirementTypes {
		require.True(t, covered[reqType], "spec requirement type %q has no case", reqType)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommitFixture(t)
			before := f.currentPointer(t)
			body := `{"requirements":[` + tc.requirement + `],"updates":[
				{"action":"set-properties","updates":{"requirement":"` + tc.name + `"}}]}`

			result, err := f.commit(t, body)
			if tc.satisfied {
				require.NoError(t, err, "%s must be satisfied by the fixture table", tc.requirement)
				require.Equal(t, tc.name, result.Metadata.Properties()["requirement"])
				require.Equal(t, 1, f.writes.putCount())
				return
			}

			require.Nil(t, result)
			var failed *CommitFailedError
			require.ErrorAs(t, err, &failed, "%s must fail the commit, got %v", tc.requirement, err)
			require.Equal(t, strings.SplitN(tc.name, "/", 2)[0], failed.Requirement)
			require.Equal(t, 409, failed.HTTPStatusCode())
			require.NotErrorIs(t, err, ErrCommitStateUnknown)
			f.noChange(t, before)
		})
	}

	t.Run("assert-create-on-a-missing-table", func(t *testing.T) {
		// Requirement.Validate(nil) is "the table does not exist". Only assert-create accepts it;
		// the staged create that pairs it with updates is the create-table path (I2-4).
		var requirements table.Requirements
		require.NoError(t, json.Unmarshal([]byte(`[{"type":"assert-create"}]`), &requirements))
		require.NoError(t, validateRequirements(requirements, nil))

		require.NoError(t, json.Unmarshal([]byte(`[{"type":"assert-table-uuid","uuid":"`+storeTestUUID+`"}]`), &requirements))
		var failed *CommitFailedError
		require.ErrorAs(t, validateRequirements(requirements, nil), &failed)
	})
}

// TestUpdates is the §12 "update application" table: one case per TableUpdate action of the REST
// spec, plus the actions this catalog refuses. Every case goes through a real commit, so it
// asserts what iceberg-go applies *and* that the result survives the metadata.json write.
func TestUpdates(t *testing.T) {
	// covered records the spec actions this test has a case for, successful or refused.
	covered := map[string]bool{}

	tests := []struct {
		name    string
		actions []string // the actions this case covers
		updates string   // the request's "updates" array
		check   func(t *testing.T, result *CommitResult)
	}{
		{
			name:    "assign-uuid",
			actions: []string{"assign-uuid"},
			updates: `[{"action":"assign-uuid","uuid":"` + storeTestUUID + `"}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Equal(t, storeTestUUID, result.Metadata.TableUUID().String())
			},
		},
		{
			name:    "upgrade-format-version",
			actions: []string{"upgrade-format-version"},
			updates: `[{"action":"upgrade-format-version","format-version":2}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Equal(t, 2, result.Metadata.Version())
			},
		},
		{
			name:    "add-schema-and-set-current-schema",
			actions: []string{"add-schema", "set-current-schema"},
			updates: `[
				{"action":"add-schema","schema":{"type":"struct","schema-id":2,"fields":[
					{"id":1,"name":"x","required":true,"type":"long"},
					{"id":2,"name":"y","required":true,"type":"long"},
					{"id":3,"name":"z","required":true,"type":"long"},
					{"id":4,"name":"w","required":false,"type":"string"}]},"last-column-id":4},
				{"action":"set-current-schema","schema-id":-1}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Len(t, result.Metadata.Schemas(), 3)
				require.Equal(t, 2, result.Metadata.CurrentSchema().ID)
				require.Equal(t, 4, result.Metadata.LastColumnID())
			},
		},
		{
			name:    "add-spec-and-set-default-spec",
			actions: []string{"add-spec", "set-default-spec"},
			updates: `[
				{"action":"add-spec","spec":{"spec-id":1,"fields":[
					{"name":"z_bucket","transform":"bucket[16]","source-id":3,"field-id":1001}]}},
				{"action":"set-default-spec","spec-id":-1}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Equal(t, 1, result.Metadata.DefaultPartitionSpec())
				require.NotNil(t, result.Metadata.PartitionSpecByID(1))
			},
		},
		{
			name:    "add-sort-order-and-set-default-sort-order",
			actions: []string{"add-sort-order", "set-default-sort-order"},
			updates: `[
				{"action":"add-sort-order","sort-order":{"order-id":4,"fields":[
					{"transform":"identity","source-id":1,"direction":"asc","null-order":"nulls-first"}]}},
				{"action":"set-default-sort-order","sort-order-id":-1}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Equal(t, 4, result.Metadata.DefaultSortOrder())
			},
		},
		{
			name:    "add-snapshot",
			actions: []string{"add-snapshot"},
			updates: `[{"action":"add-snapshot","snapshot":{
				"snapshot-id":5555555555555555555,
				"parent-snapshot-id":3055729675574597004,
				"sequence-number":35,
				"timestamp-ms":1602638574000,
				"manifest-list":"mem://repo-ns/_iceberg/` + storeTestUUID + `/metadata/snap-5555555555555555555.avro",
				"summary":{"operation":"append","added-data-files":"1"},
				"schema-id":1}}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Len(t, result.Metadata.Snapshots(), 3)
				require.NotNil(t, result.Metadata.SnapshotByID(5555555555555555555))
				require.EqualValues(t, 35, result.Metadata.LastSequenceNumber())
			},
		},
		{
			name:    "add-snapshot-and-set-snapshot-ref",
			actions: []string{"add-snapshot", "set-snapshot-ref"},
			updates: `[
				{"action":"add-snapshot","snapshot":{
					"snapshot-id":6666666666666666666,
					"parent-snapshot-id":3055729675574597004,
					"sequence-number":35,
					"timestamp-ms":1602638574000,
					"manifest-list":"mem://repo-ns/_iceberg/` + storeTestUUID + `/metadata/snap-6666666666666666666.avro",
					"summary":{"operation":"append"}}},
				{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":6666666666666666666,
					"max-ref-age-ms":86400000,"min-snapshots-to-keep":2}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.NotNil(t, result.Metadata.SnapshotByName("main"))
				require.Equal(t, int64(6666666666666666666), result.Metadata.SnapshotByName("main").SnapshotID)
			},
		},
		{
			// The base's tag "test" references this snapshot, so drop the ref first: removing a
			// snapshot a ref still points at is the state precondition the spec defines.
			name:    "remove-snapshot-ref-and-remove-snapshots",
			actions: []string{"remove-snapshot-ref", "remove-snapshots"},
			updates: `[
				{"action":"remove-snapshot-ref","ref-name":"test"},
				{"action":"remove-snapshots","snapshot-ids":[3051729675574597004]}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Nil(t, result.Metadata.SnapshotByID(3051729675574597004))
				require.Nil(t, result.Metadata.SnapshotByName("test"))
				require.Len(t, result.Metadata.Snapshots(), 1)
			},
		},
		{
			name:    "set-properties",
			actions: []string{"set-properties"},
			updates: `[{"action":"set-properties","updates":{"owner":"lakefs","custom.key":"custom-value"}}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Equal(t, "lakefs", result.Metadata.Properties()["owner"])
				require.Equal(t, "custom-value", result.Metadata.Properties()["custom.key"])
			},
		},
		{
			name:    "remove-properties",
			actions: []string{"remove-properties"},
			updates: `[{"action":"remove-properties","removals":["read.split.target.size"]}]`,
			check: func(t *testing.T, result *CommitResult) {
				_, ok := result.Metadata.Properties()["read.split.target.size"]
				require.False(t, ok)
			},
		},
		{
			name:    "set-statistics",
			actions: []string{"set-statistics"},
			updates: `[{"action":"set-statistics","snapshot-id":3055729675574597004,"statistics":{
				"snapshot-id":3055729675574597004,
				"statistics-path":"mem://repo-ns/_iceberg/` + storeTestUUID + `/metadata/statistic-3055729675574597004.puffin",
				"file-size-in-bytes":1300,
				"file-footer-size-in-bytes":100,
				"blob-metadata":[{"type":"apache-datasketches-theta-v1","snapshot-id":3055729675574597004,
					"sequence-number":1,"fields":[1],"properties":{"ndv":"5"}}]}}]`,
			check: func(t *testing.T, result *CommitResult) {
				count := 0
				for range result.Metadata.Statistics() {
					count++
				}
				require.Equal(t, 1, count)
			},
		},
		{
			name:    "remove-statistics",
			actions: []string{"remove-statistics"},
			updates: `[{"action":"remove-statistics","snapshot-id":3055729675574597004}]`,
			check: func(t *testing.T, result *CommitResult) {
				count := 0
				for range result.Metadata.Statistics() {
					count++
				}
				require.Equal(t, 0, count, "removing statistics the table does not have is a no-op")
			},
		},
		{
			name:    "set-partition-statistics",
			actions: []string{"set-partition-statistics"},
			updates: `[{"action":"set-partition-statistics","partition-statistics":{
				"snapshot-id":3055729675574597004,
				"statistics-path":"mem://repo-ns/_iceberg/` + storeTestUUID + `/metadata/partition-3055729675574597004.parquet",
				"file-size-in-bytes":1000}}]`,
			check: func(t *testing.T, result *CommitResult) {
				count := 0
				for range result.Metadata.PartitionStatistics() {
					count++
				}
				require.Equal(t, 1, count)
			},
		},
		{
			name:    "remove-partition-statistics",
			actions: []string{"remove-partition-statistics"},
			updates: `[{"action":"remove-partition-statistics","snapshot-id":3055729675574597004}]`,
			check: func(t *testing.T, result *CommitResult) {
				count := 0
				for range result.Metadata.PartitionStatistics() {
					count++
				}
				require.Equal(t, 0, count)
			},
		},
		{
			name:    "remove-partition-specs",
			actions: []string{"remove-partition-specs"},
			updates: `[
				{"action":"add-spec","spec":{"spec-id":1,"fields":[
					{"name":"z_bucket","transform":"bucket[16]","source-id":3,"field-id":1001}]}},
				{"action":"set-default-spec","spec-id":-1},
				{"action":"remove-partition-specs","spec-ids":[0]}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Nil(t, result.Metadata.PartitionSpecByID(0))
				require.Equal(t, 1, result.Metadata.DefaultPartitionSpec())
			},
		},
		{
			name:    "remove-schemas",
			actions: []string{"remove-schemas"},
			// The added schema must differ from both schemas the base already has: iceberg-go reuses
			// a structurally identical schema instead of adding a new one.
			updates: `[
				{"action":"add-schema","schema":{"type":"struct","schema-id":2,"fields":[
					{"id":1,"name":"x","required":true,"type":"long"},
					{"id":2,"name":"y","required":true,"type":"long"},
					{"id":3,"name":"z","required":true,"type":"long"},
					{"id":4,"name":"w","required":false,"type":"string"}]},"last-column-id":4},
				{"action":"set-current-schema","schema-id":-1},
				{"action":"remove-schemas","schema-ids":[0]}]`,
			check: func(t *testing.T, result *CommitResult) {
				require.Len(t, result.Metadata.Schemas(), 2)
				for _, schema := range result.Metadata.Schemas() {
					require.NotEqual(t, 0, schema.ID)
				}
			},
		},
		{
			name:    "remove-encryption-key",
			actions: []string{"remove-encryption-key"},
			updates: `[{"action":"remove-encryption-key","key-id":"key-1"}]`,
			check: func(t *testing.T, result *CommitResult) {
				count := 0
				for range result.Metadata.EncryptionKeys() {
					count++
				}
				require.Equal(t, 0, count, "a v2 table has no encryption keys; removing one is a no-op")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommitFixture(t)
			result := f.mustCommit(t, `{"requirements":[],"updates":`+tc.updates+`}`)

			require.Equal(t, 1, f.writes.putCount())
			require.Equal(t, result.Pointer, f.currentPointer(t))
			reparsed, err := table.ParseMetadataBytes(result.MetadataJSON)
			require.NoError(t, err, "the written metadata.json must re-parse")
			require.True(t, reparsed.Equals(result.Metadata), "the file and the model must agree")

			tc.check(t, result)
			for _, action := range tc.actions {
				covered[action] = true
			}
		})
	}

	// Refused updates. Each one must be a 400 that wrote nothing - never a 409, which would tell
	// the client to retry a request the table can never accept (§2.1).
	refused := []struct {
		name    string
		actions []string
		updates string
		cause   error // sentinel the refusal must wrap, nil when iceberg-go wraps none
	}{
		{
			name:    "set-location-is-an-enterprise-limitation",
			actions: []string{"set-location"},
			updates: `[{"action":"set-location","location":"s3://elsewhere/table"}]`,
			cause:   ErrSetLocationUnsupported,
		},
		{
			// Format version 3, including what only exists there. §1.2 makes v3 a non-goal, so the
			// action decodes and is then refused with a reason instead of surfacing as a 500.
			name:    "upgrade-format-version-3-is-unsupported",
			actions: []string{"upgrade-format-version"},
			updates: `[{"action":"upgrade-format-version","format-version":3}]`,
		},
		{
			name:    "add-encryption-key-needs-format-version-3",
			actions: []string{"add-encryption-key"},
			updates: `[{"action":"add-encryption-key","encryption-key":{
				"key-id":"key-1","encrypted-key-metadata":"AAAA","encrypted-by-id":"key-0",
				"properties":{"kms.key-arn":"arn:aws:kms:us-east-1:111122223333:key/abc"}}}]`,
		},
		{
			name:    "set-current-schema-with-nothing-added",
			actions: []string{"set-current-schema"},
			updates: `[{"action":"set-current-schema","schema-id":-1}]`,
		},
		{
			name:    "set-current-schema-to-an-unknown-id",
			actions: []string{"set-current-schema"},
			updates: `[{"action":"set-current-schema","schema-id":999}]`,
		},
		{
			// The trap I0-1 documented: iceberg-go's stale-snapshot-timestamp refusal wraps no
			// sentinel, so a cause-based classifier would answer 500. The commit path classifies
			// every apply failure as a bad request instead.
			name:    "add-snapshot-with-a-stale-timestamp",
			actions: []string{"add-snapshot"},
			updates: `[{"action":"add-snapshot","snapshot":{
				"snapshot-id":7777777777777777777,
				"parent-snapshot-id":3055729675574597004,
				"sequence-number":35,
				"timestamp-ms":1555100955811,
				"manifest-list":"mem://repo-ns/_iceberg/` + storeTestUUID + `/metadata/snap-7777777777777777777.avro",
				"summary":{"operation":"append"}}}]`,
		},
	}

	for _, tc := range refused {
		t.Run("refused/"+tc.name, func(t *testing.T) {
			f := newCommitFixture(t)
			before := f.currentPointer(t)

			result, err := f.commit(t, `{"requirements":[],"updates":`+tc.updates+`}`)
			require.Nil(t, result)

			var bad *BadRequestError
			require.ErrorAs(t, err, &bad, "a refused update must be a 400, got %v", err)
			require.Equal(t, 400, bad.HTTPStatusCode())
			require.Equal(t, ErrTypeBadRequest, bad.ErrorType())
			require.NotErrorIs(t, err, ErrCommitFailed, "an unappliable update is not a conflict")
			require.NotErrorIs(t, err, ErrCommitStateUnknown, "nothing was written, so the outcome is known")
			if tc.cause != nil {
				require.ErrorIs(t, err, tc.cause)
			}

			f.noChange(t, before)
			for _, action := range tc.actions {
				covered[action] = true
			}
		})
	}

	// Both halves together must cover the spec exactly: every action has a case, and no case names
	// an action the spec does not have.
	for _, action := range specUpdateActions {
		require.True(t, covered[action], "spec update action %q has no case", action)
	}
	spec := make(map[string]struct{}, len(specUpdateActions))
	for _, action := range specUpdateActions {
		spec[action] = struct{}{}
	}
	for action := range covered {
		_, ok := spec[action]
		require.True(t, ok, "case names %q, which is not a TableUpdate action in the REST spec", action)
	}
}

// forkSide commits body on a new branch forked from main's committed head, and returns the pointer
// that branch answers with: the state of one side of a merge, built by the same Commit the server
// runs. The main branch must be committed first, because a new branch starts from a commit, not
// from main's staging area.
func (f *commitFixture) forkSide(t *testing.T, branch, body string) TablePointer {
	t.Helper()
	_, err := f.catalog.CreateBranch(f.ctx, storeTestRepo, branch, f.headRef(t))
	require.NoError(t, err, "create branch %s", branch)

	addr := f.addr
	addr.Ref = branch
	request, err := DecodeTableCommitRequest([]byte(body))
	require.NoError(t, err, "decode commit request %s", body)
	result, err := f.store.Commit(f.ctx, addr, request)
	require.NoError(t, err, "commit on branch %s", branch)
	return result.Pointer
}

// compactionBody is a commit of the shape a compaction makes: it moves the pointer (and would
// rewrite data files), without changing the table's logical contents. What the resolver reads is
// the metadata-log chain, so the payload need not model rewritten files - and must not: the point
// of §6.4 is that no code inspects snapshots to resolve a conflict.
const compactionBody = `{"requirements":[],"updates":[
	{"action":"set-properties","updates":{"compaction":"rewritten"}}]}`

// logicalBody is a commit that changes the table logically: an append with its properties.
const logicalBody = `{"requirements":[],"updates":[
	{"action":"set-properties","updates":{"owner":"analytics"}}]}`

// detachedPointer writes a metadata.json that does not descend from any other file - what a table
// dropped and recreated, or reset to a third history, would name - and returns the pointer. distinct
// keeps two of them apart, so that a test with two detached sides is a real two-value conflict.
func (f *commitFixture) detachedPointer(t *testing.T, distinct string) TablePointer {
	t.Helper()
	location := f.putMetadata(t,
		"_iceberg/"+storeTestUUID+"/metadata/00001-detached-"+distinct+".metadata.json",
		storeTestMetadata(t, storeTestUUID, f.tableLocation))
	return TablePointer{MetadataLocation: location, TableUUID: storeTestUUID, FormatVersion: 2}
}

// TestCompactionAutoResolve covers the compaction auto-resolve of §6.4 (phase I-5): the commit
// message detection, the three-way decision on a table pointer, the iceberg.ignore_compaction_commits
// default and its switch-off, and the ancestry guard that stops the resolver from guessing.
func TestCompactionAutoResolve(t *testing.T) {
	const (
		compactionMessage = "compaction of inventory.books.orders\nlakefs.compaction.operation-id=4f0c1a2e"
		logicalMessage    = "append 12 rows to inventory.books.orders"
	)

	t.Run("a message carrying the marker is a compaction commit", func(t *testing.T) {
		tests := []struct {
			name    string
			message string
			want    bool
		}{
			{name: "operation id as a key=value", message: "lakefs.compaction.operation-id=abc123", want: true},
			{name: "marker inside a multi-line message", message: "compaction\nlakefs.compaction.operation-id: abc123\n", want: true},
			{name: "an ordinary commit", message: logicalMessage, want: false},
			{name: "an empty message", message: "", want: false},
			{name: "a message that merely mentions compaction", message: "compaction of orders", want: false},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.want, IsCompactionCommit(tc.message))
			})
		}
	})

	// newThreeWay builds one table on main, commits it, and returns the merge-base pointer plus
	// one pointer per side: feature changes the table logically, compaction only compacts it.
	newThreeWay := func(t *testing.T) (f *commitFixture, base, feature, compaction TablePointer) {
		t.Helper()
		f = newCommitFixture(t)
		f.storeFixture.commit(t, "create the table")
		base = f.currentPointer(t)
		feature = f.forkSide(t, "feature", logicalBody)
		compaction = f.forkSide(t, "compaction", compactionBody)
		require.NotEqual(t, base, feature)
		require.NotEqual(t, base, compaction)
		require.NotEqual(t, feature, compaction)
		return f, base, feature, compaction
	}

	t.Run("a compaction commit on the source branch is ignored", func(t *testing.T) {
		f, base, feature, compaction := newThreeWay(t)

		resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
			Base:        base,
			Destination: PointerRevision{Pointer: feature, CommitMessage: logicalMessage},
			Source:      PointerRevision{Pointer: compaction, CommitMessage: compactionMessage},
		}, nil)
		require.NoError(t, err)
		require.NotNil(t, resolution, "a conflict caused only by a compaction commit is auto-resolved")
		require.Equal(t, feature, resolution.Pointer, "the logical change wins; the compaction is a no-op")
		require.Equal(t, compaction, resolution.IgnoredCompaction)

		// The kept pointer is a readable table that still carries the logical change.
		keptAddress := f.addr
		keptAddress.Ref = "feature"
		kept, err := f.store.GetTable(f.ctx, keptAddress)
		require.NoError(t, err)
		require.Equal(t, feature, kept.Pointer, "the kept pointer is the destination's own pointer, unchanged")
		require.Equal(t, "analytics", kept.Metadata.Properties()["owner"])
	})

	t.Run("a compaction commit on the destination branch is ignored", func(t *testing.T) {
		f, base, feature, compaction := newThreeWay(t)

		resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
			Base:        base,
			Destination: PointerRevision{Pointer: compaction, CommitMessage: compactionMessage},
			Source:      PointerRevision{Pointer: feature, CommitMessage: logicalMessage},
		}, nil)
		require.NoError(t, err)
		require.NotNil(t, resolution)
		require.Equal(t, feature, resolution.Pointer, "the logical change wins whichever side it is on")
		require.Equal(t, compaction, resolution.IgnoredCompaction)
	})

	t.Run("two logical changes are a conflict", func(t *testing.T) {
		f := newCommitFixture(t)
		f.storeFixture.commit(t, "create the table")
		base := f.currentPointer(t)
		destination := f.forkSide(t, "feature", logicalBody)
		source := f.forkSide(t, "hotfix", `{"requirements":[],"updates":[
			{"action":"set-properties","updates":{"owner":"finance"}}]}`)

		resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
			Base:        base,
			Destination: PointerRevision{Pointer: destination, CommitMessage: logicalMessage},
			Source:      PointerRevision{Pointer: source, CommitMessage: "fix a typo in a property"},
		}, nil)
		require.NoError(t, err)
		require.Nil(t, resolution, "neither side is a compaction commit, so the conflict is reported")
	})

	t.Run("with iceberg.ignore_compaction_commits off the conflict is reported", func(t *testing.T) {
		f, base, feature, compaction := newThreeWay(t)

		resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
			Base:        base,
			Destination: PointerRevision{Pointer: feature, CommitMessage: logicalMessage},
			Source:      PointerRevision{Pointer: compaction, CommitMessage: compactionMessage},
		}, &Config{IgnoreCompactionCommits: false})
		require.NoError(t, err)
		require.Nil(t, resolution, "the flag off means the documented behaviour: report the conflict")
	})

	t.Run("both sides compacted: the destination is kept", func(t *testing.T) {
		f, base, destination, source := newThreeWay(t)

		resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
			Base:        base,
			Destination: PointerRevision{Pointer: destination, CommitMessage: compactionMessage},
			Source:      PointerRevision{Pointer: source, CommitMessage: compactionMessage + " (other op)"},
		}, nil)
		require.NoError(t, err)
		require.NotNil(t, resolution, "two compactions are two logical no-ops, so there is nothing to report")
		require.Equal(t, destination, resolution.Pointer)
	})

	t.Run("one side did not move: the moved side wins without ignoring anything", func(t *testing.T) {
		f, base, feature, _ := newThreeWay(t)

		resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
			Base:        base,
			Destination: PointerRevision{Pointer: base},
			Source:      PointerRevision{Pointer: feature, CommitMessage: logicalMessage},
		}, &Config{IgnoreCompactionCommits: false})
		require.NoError(t, err)
		require.NotNil(t, resolution, "a fast-forward is not a conflict, whatever the flag says")
		require.Equal(t, feature, resolution.Pointer)
		require.Equal(t, TablePointer{}, resolution.IgnoredCompaction, "nothing was ignored")
	})

	// The three-way guard: a compaction commit is only ignorable while the merge base is still an
	// ancestor of both sides. A destructively rewritten side is not "base plus a compaction".
	t.Run("three-way: a side that no longer descends from the base is not resolved", func(t *testing.T) {
		t.Run("the compaction side was reset", func(t *testing.T) {
			f := newCommitFixture(t)
			f.storeFixture.commit(t, "create the table")
			base := f.currentPointer(t)
			destination := f.forkSide(t, "feature", logicalBody)

			resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
				Base:        base,
				Destination: PointerRevision{Pointer: destination, CommitMessage: logicalMessage},
				Source:      PointerRevision{Pointer: f.detachedPointer(t, "source"), CommitMessage: compactionMessage},
			}, nil)
			require.NoError(t, err)
			require.Nil(t, resolution,
				"a pointer whose metadata-log does not reach the base is not a compaction of it")
		})

		t.Run("the kept side was reset", func(t *testing.T) {
			f := newCommitFixture(t)
			f.storeFixture.commit(t, "create the table")
			base := f.currentPointer(t)
			source := f.forkSide(t, "compaction", compactionBody)

			resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
				Base:        base,
				Destination: PointerRevision{Pointer: f.detachedPointer(t, "destination"), CommitMessage: logicalMessage},
				Source:      PointerRevision{Pointer: source, CommitMessage: compactionMessage},
			}, nil)
			require.NoError(t, err)
			require.Nil(t, resolution,
				"the side that would win does not descend from the base either, so nothing is proven")
		})

		t.Run("a new table on both sides is a conflict", func(t *testing.T) {
			f := newCommitFixture(t)
			// No commit and no fork from a base table: base is the zero pointer.
			resolution, err := f.store.ResolvePointerConflict(f.ctx, storeTestRepo, PointerConflict{
				Base:        TablePointer{},
				Destination: PointerRevision{Pointer: f.detachedPointer(t, "ours"), CommitMessage: logicalMessage},
				Source:      PointerRevision{Pointer: f.detachedPointer(t, "theirs"), CommitMessage: compactionMessage},
			}, nil)
			require.NoError(t, err)
			require.Nil(t, resolution, "both branches created the table: there is no base to descend from")
		})
	})
}
