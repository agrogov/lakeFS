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
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/table"
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

// failingCatalog is the fixture's catalog with a CreateEntry that always fails, which is how a
// test forces the pointer swap of a commit to lose its compare-and-swap (or to fail for a reason
// that is not a precondition failure).
type failingCatalog struct {
	Catalog
	err error
}

func (c *failingCatalog) CreateEntry(context.Context, string, string, catalog.DBEntry, ...graveler.SetOptionsFunc) error {
	return c.err
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
