package icebergcatalog

// Tests for the register operation: POST /v1/{prefix}/namespaces/{namespace}/register
// (ICEBERG_CATALOG_PLAN.md §6.1, §6.2, §6.3, §13, §7.2, §11 I-2 task I2-6).
//
// The fixture is the tables fixture of tables_test.go - a real catalog store (graveler, committed
// ranges) on an in-memory KV store, the ACL auth service, the production middleware stack - with
// the register service mounted next to the table surface and the block adapter instrumented so a
// test can prove that a foreign metadata-location is refused *before* it is fetched with the
// server's credentials (§13).
//
// The metadata documents are written by the fixture with the same block adapter the catalog reads,
// which is the situation register exists for: an Iceberg client (PyIceberg, Spark, another
// catalog) wrote the files itself and the catalog adopts the pointer. The Avro manifests of the
// "stages the files of the adopted snapshots" case are produced by iceberg-go's own writers
// (iceberg.WriteManifest / iceberg.WriteManifestList), like files_test.go, so the manifest
// decoding under test is the dependency's and not a fixture's.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// registerTestTableUUID is the table UUID of the adopted table. It is a UUID no other fixture in
// this package uses, so the entries its snapshots stage cannot collide with another table's.
const registerTestTableUUID = "55555555-5555-5555-5555-555555555555"

// registerTestMetadataTemplate is a table metadata document of the Apache Iceberg table spec
// ("Table Metadata") with a parameterized format version, table UUID, location and snapshots.
// The format version is a parameter so that one template covers the v1/v2 documents register
// adopts and the v3 document it refuses (§1.2).
const registerTestMetadataTemplate = `{
  "format-version": %d,
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
}`

// registerTestMetadata renders registerTestMetadataTemplate for one table. currentSnapshotID is -1
// for a table that has no snapshot, which is the state a freshly written table is in.
func registerTestMetadata(formatVersion int, tableUUID, location string, snapshots []string, currentSnapshotID int64) string {
	return fmt.Sprintf(registerTestMetadataTemplate, formatVersion, tableUUID, location,
		filesTestSequenceNumber, filesTestMetadataTimestampMs, currentSnapshotID, strings.Join(snapshots, ","))
}

// registerFixture is the register stack: the tables fixture with the register service mounted next
// to the table surface, and the block adapter instrumented for the §13 assertions.
type registerFixture struct {
	*tablesFixture

	// reader records every object read through the block adapter, so a test can show that a
	// refused location was never fetched.
	reader *getRecordingAdapter
}

// newRegisterFixture builds the fixture and mounts the register route next to the table routes, so
// a test can register a table and then load it through the same router - staged, without a lakeFS
// commit - the way a client would.
func newRegisterFixture(t *testing.T) *registerFixture {
	t.Helper()
	base := newTablesFixture(t)

	// The store is rebuilt over a recording adapter: every read of the catalog - the register
	// handler's included - now goes through it. Nothing else about the fixture changes.
	reader := &getRecordingAdapter{Adapter: base.catalog.BlockAdapter}
	store := NewStore(base.catalog, reader)
	base.store = store

	cfg := &Config{Prefix: CatalogPrefix}
	// Register needs the same services the server hands it (§4.1): the store, the §7.2 authorizer
	// and the lister the write path resolves the ref and the pointer with.
	service := NewRegisterService(store, base.authz.authz, base.catalog, cfg)
	tableService := NewTableService(store, base.authz.authz, base.catalog, cfg)

	routes := append(tableService.Routes(), service.Routes()...)
	router, err := newRouter(tablesUser, cfg, catalogServerWith(cfg, routes...))
	require.NoError(t, err, "build the catalog router")
	base.router = router

	return &registerFixture{tablesFixture: base, reader: reader}
}

// registerTarget is the register URL of a namespace on ref, written the way a client writes it:
// the namespace path parameter carries the repository, the ref and the client namespace levels
// joined by %1F (§5).
func registerTarget(ref string, namespace Namespace) string {
	levels := append([]string{tablesTestRepo, ref}, namespace...)
	return "/v1/" + CatalogPrefix + "/namespaces/" +
		url.PathEscape(strings.Join(levels, NamespaceSeparator)) + "/register"
}

// namespaceParam is the {namespace} path parameter of a namespaced route on the main branch.
func namespaceParam(namespace Namespace) string {
	levels := append([]string{tablesTestRepo, tablesTestRef}, namespace...)
	return url.PathEscape(strings.Join(levels, NamespaceSeparator))
}

// post sends one register request as user.
func (f *registerFixture) post(t *testing.T, target, user string, body registerTableRequest) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err, "marshal the register request")
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(payload))
	req.Header.Set("X-Test-User", user)
	req.Header.Set("Content-Type", contentTypeJSON)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// entry reads one catalog entry of the fixture repository - not of storeTestRepo, which the store
// fixture created first and which no catalog write of this file touches - through the same
// staged-then-committed resolution a reader gets.
func (f *registerFixture) entry(t *testing.T, ref, entryPath string) (*catalog.DBEntry, error) {
	t.Helper()
	return f.catalog.GetEntry(f.ctx, tablesTestRepo, ref, entryPath, catalog.GetEntryParams{})
}

// branchHead is the commit the fixture branch points at, i.e. what an explicit lakeFS commit last
// wrote. Register must not move it: catalog writes are staged, never committed (§6.2).
func (f *registerFixture) branchHead(t *testing.T) string {
	t.Helper()
	ref, err := f.catalog.GetBranchReference(f.ctx, tablesTestRepo, tablesTestRef)
	require.NoError(t, err, "get the %s reference", tablesTestRef)
	return ref
}

// tableLocation is the managed base location of the adopted table (Q-I5):
// <storage_namespace>/_iceberg/<table-uuid>.
func (f *registerFixture) tableLocation(tableUUID string) string {
	return f.storageNamespace + "/" + DataPrefix + "/" + tableUUID
}

// putMetadataDocument writes an ordinary (no snapshot) metadata document for the adopted table
// under its managed location and returns its fully qualified URI.
func (f *registerFixture) putMetadataDocument(t *testing.T, tableUUID, document string) string {
	t.Helper()
	return f.putTableMetadata(t,
		DataPrefix+"/"+tableUUID+"/"+MetadataDirectory+"/00001-"+tableUUID+".metadata.json", document)
}

// writeSnapshotFiles writes one snapshot's manifest list and manifest - with one ADDED data file -
// under the table's location and returns the manifest-list URI, the way an Iceberg client writes
// them before the catalog adopts the table.
func (f *registerFixture) writeSnapshotFiles(t *testing.T, tableLocation string) string {
	t.Helper()

	dataFile, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		tableLocation+"/data/00000-0-"+registerTestTableUUID+".parquet",
		iceberg.ParquetFile, nil, nil, nil, filesTestDataCount, filesTestDataSize)
	require.NoError(t, err, "build the data file")

	var manifestBuffer bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		tableLocation+"/metadata/"+registerTestTableUUID+"-m0.avro", &manifestBuffer, 2,
		*iceberg.UnpartitionedSpec, filesTestSchema(), filesTestSnapshotID,
		[]iceberg.ManifestEntry{iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, nil, nil, dataFile.Build())})
	require.NoError(t, err, "write the manifest")
	f.putTableMetadata(t, strings.TrimPrefix(
		tableLocation+"/metadata/"+registerTestTableUUID+"-m0.avro", f.storageNamespace+"/"), manifestBuffer.String())

	var listBuffer bytes.Buffer
	sequenceNumber := filesTestSequenceNumber
	parent := int64(filesTestParentSnapshotID)
	require.NoError(t, iceberg.WriteManifestList(
		2, &listBuffer, filesTestSnapshotID, &parent, &sequenceNumber, 0, []iceberg.ManifestFile{manifest}),
		"write the manifest list")
	manifestListKey := "metadata/snap-" + fmt.Sprint(filesTestSnapshotID) + "-1-" + registerTestTableUUID + ".avro"
	return f.putTableMetadata(t, strings.TrimPrefix(tableLocation+"/"+manifestListKey, f.storageNamespace+"/"),
		listBuffer.String())
}

// fileEntryPath is the reserved key a table file is registered under (§6.3):
// _lakefs_iceberg/files/<table-uuid>/<path relative to the table root>.
func fileEntryPath(relativePath string) string {
	return ReservedPrefix + "/" + FilesDirectory + "/" + registerTestTableUUID + "/" + relativePath
}

// TestRegisterTable covers the register operation end to end (§11 I-2 "register"): the pointer is
// staged with the assert-create condition, the adopted metadata.json is registered as a lakeFS
// entry, the table is readable before its lakeFS commit, and every refusal - a taken name, a
// foreign location, an unsupported format version, a missing namespace, a read-only ref, a missing
// grant, and a table whose files are outside the catalog's table area - answers the status the
// client expects.
func TestRegisterTable(t *testing.T) {
	namespace := Namespace{"inventory", "books"}

	// writer is the principal that holds the write half of §7.2 (authzWriteStatements); alice of
	// the fixture holds the read half only, which is the "missing grant" case.
	const writer = "carol"

	t.Run("adopts a metadata document and stages its pointer", func(t *testing.T) {
		f := newRegisterFixture(t)
		headBefore := f.branchHead(t)

		document := registerTestMetadata(2, registerTestTableUUID, f.tableLocation(registerTestTableUUID), nil, -1)
		metadataLocation := f.putMetadataDocument(t, registerTestTableUUID, document)

		rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, registerTableRequest{
			Name:             "adopted",
			MetadataLocation: metadataLocation,
		})
		body := decodeLoadTable(t, rec)

		// The response adopts the client's file: the location is the request's, unchanged, and the
		// document is the one that was read rather than a re-encoding of it.
		require.Equal(t, metadataLocation, body.MetadataLocation,
			"register adopts the metadata.json it was given")
		var tableUUID string
		require.NoError(t, json.Unmarshal(body.Metadata["table-uuid"], &tableUUID))
		require.Equal(t, registerTestTableUUID, tableUUID)

		// The pointer is staged on the branch with the fields of §6.1.
		addr := Address{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: namespace, Table: "adopted"}
		table, err := f.store.GetTable(f.ctx, addr)
		require.NoError(t, err, "the registered table is readable")
		require.Equal(t, TablePointer{
			MetadataLocation: metadataLocation,
			TableUUID:        registerTestTableUUID,
			FormatVersion:    2,
		}, table.Pointer)

		// And through the HTTP surface, before any lakeFS commit: a catalog write is staged, never
		// auto-committed (§6.2, §14 Q-I2).
		load := decodeLoadTable(t, f.request(t, http.MethodGet,
			tablesLoadTarget(namespaceParam(namespace), "adopted", nil), writer))
		require.Equal(t, metadataLocation, load.MetadataLocation)
		require.Equal(t, headBefore, f.branchHead(t), "register stages; the branch head is unchanged")

		// §6.3: the adopted metadata.json is a lakeFS entry under the reserved prefix, pointing at
		// the object the client wrote. Nothing else is registered for a table without snapshots.
		entry, err := f.entry(t, tablesTestRef, fileEntryPath(MetadataDirectory+"/00001-"+registerTestTableUUID+".metadata.json"))
		require.NoError(t, err, "the metadata file is staged as an entry")
		require.Equal(t, catalog.AddressTypeFull, entry.AddressType)
		require.Equal(t, metadataLocation, entry.PhysicalAddress)
		require.Equal(t, int64(len(document)), entry.Size)
	})

	t.Run("stages the files of the adopted snapshot", func(t *testing.T) {
		f := newRegisterFixture(t)
		root := f.tableLocation(registerTestTableUUID)
		manifestList := f.writeSnapshotFiles(t, root)
		document := registerTestMetadata(2, registerTestTableUUID, root,
			[]string{filesTestSnapshotDocument(manifestList)}, filesTestSnapshotID)
		metadataLocation := f.putMetadataDocument(t, registerTestTableUUID, document)

		rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, registerTableRequest{
			Name:             "adopted",
			MetadataLocation: metadataLocation,
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		// One entry per metadata file, manifest list, manifest and the data file the manifest adds.
		for _, relativePath := range []string{
			"data/00000-0-" + registerTestTableUUID + ".parquet",
			"metadata/" + registerTestTableUUID + "-m0.avro",
			"metadata/snap-" + fmt.Sprint(filesTestSnapshotID) + "-1-" + registerTestTableUUID + ".avro",
			"metadata/00001-" + registerTestTableUUID + ".metadata.json",
		} {
			entry, err := f.entry(t, tablesTestRef, fileEntryPath(relativePath))
			require.NoErrorf(t, err, "%s must be staged", relativePath)
			require.Equal(t, root+"/"+relativePath, entry.PhysicalAddress,
				"%s points at the object the client wrote", relativePath)
		}
	})

	t.Run("refuses a name that is taken with 409 and stages nothing", func(t *testing.T) {
		f := newRegisterFixture(t)
		existing := Address{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: namespace, Table: "orders"}
		before, err := f.store.GetTable(f.ctx, existing)
		require.NoError(t, err, "the fixture's orders table")

		document := registerTestMetadata(2, registerTestTableUUID, f.tableLocation(registerTestTableUUID), nil, -1)
		metadataLocation := f.putMetadataDocument(t, registerTestTableUUID, document)

		for name, request := range map[string]registerTableRequest{
			"without overwrite": {Name: "orders", MetadataLocation: metadataLocation},
			"with overwrite":    {Name: "orders", MetadataLocation: metadataLocation, Overwrite: true},
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, request)
				apiError := requireEnvelope(t, rec, http.StatusConflict, ErrTypeAlreadyExists)
				require.NotContains(t, rec.Body.String(), "CommitFailedException",
					"a taken name is not a lost commit: the client must not retry it as-is")
				if request.Overwrite {
					require.Contains(t, apiError.Message, "overwrite is not supported",
						"the message says why the flag had no effect")
				}
			})
		}

		// Nothing of the loser is visible: the pointer still names the fixture's document, and no
		// file entry of the rejected table was staged.
		after, err := f.store.GetTable(f.ctx, existing)
		require.NoError(t, err)
		require.Equal(t, before.Pointer, after.Pointer, "an existing table is never replaced")
		_, err = f.entry(t, tablesTestRef, fileEntryPath(MetadataDirectory+"/00001-"+registerTestTableUUID+".metadata.json"))
		require.ErrorIs(t, err, graveler.ErrNotFound, "a refused register stages nothing")
	})

	t.Run("refuses a foreign metadata-location with 400 and never reads it", func(t *testing.T) {
		f := newRegisterFixture(t)
		// Inside no repository of this catalog: reading it would mean fetching another bucket with
		// this server's credentials, which is the §13 control register applies.
		foreign := "mem://another-bucket/" + DataPrefix + "/" + registerTestTableUUID + "/metadata/00001.metadata.json"
		f.reader.reset()

		rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, registerTableRequest{
			Name:             "foreign",
			MetadataLocation: foreign,
		})
		apiError := requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		require.Contains(t, apiError.Message, "storage namespace")
		require.NotContains(t, f.reader.readPaths(), foreign,
			"a foreign location must not be fetched with the server's credentials")

		_, err := f.store.GetTable(f.ctx, Address{
			Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: namespace, Table: "foreign",
		})
		require.ErrorAs(t, err, new(*NoSuchTableException), "a refused register creates no table")
	})

	t.Run("refuses a metadata-location that climbs out of the storage namespace", func(t *testing.T) {
		f := newRegisterFixture(t)
		// It carries the storage namespace as a string prefix but does not resolve to a file under
		// it, so it is refused like a foreign location.
		escaping := f.storageNamespace + "/../another-bucket/" + DataPrefix + "/x/metadata/00001.metadata.json"

		rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, registerTableRequest{
			Name:             "escaping",
			MetadataLocation: escaping,
		})
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("refuses format version 3 with 400", func(t *testing.T) {
		f := newRegisterFixture(t)
		// A v3 document carries next-row-id, the field iceberg-go requires of that version: the
		// point of this case is that the catalog refuses the version, not that the document is
		// unparseable.
		document := strings.Replace(
			registerTestMetadata(3, registerTestTableUUID, f.tableLocation(registerTestTableUUID), nil, -1),
			`"format-version": 3,`, `"format-version": 3, "next-row-id": 0,`, 1)
		metadataLocation := f.putMetadataDocument(t, registerTestTableUUID, document)

		rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, registerTableRequest{
			Name:             "v3",
			MetadataLocation: metadataLocation,
		})
		apiError := requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		require.Contains(t, apiError.Message, "format version 3", "the refusal names the version (§1.2)")
	})

	t.Run("refuses a document without a table-uuid", func(t *testing.T) {
		f := newRegisterFixture(t)
		// The pointer carries the table UUID metadata.json also carries (§6.1), and a v1 document
		// may omit it (format spec, "Table Metadata"). It is the client's document, so it is
		// refused as a bad request instead of surfacing as a pointer that cannot be encoded.
		root := f.tableLocation(registerTestTableUUID)
		document := strings.Replace(strings.Replace(tablesTestNoRefsMetadata,
			`"location": "mem://repo-ns/_iceberg/legacy-events"`, `"location": "`+root+`"`, 1),
			`"table-uuid": "f0a2c2c8-5f2b-4d4c-9b1f-1bbbbbbbbbbb",`, ``, 1)
		metadataLocation := f.putMetadataDocument(t, registerTestTableUUID, document)

		rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, registerTableRequest{
			Name:             "uuidless",
			MetadataLocation: metadataLocation,
		})
		apiError := requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		require.Contains(t, apiError.Message, "table-uuid", "the refusal names what the document lacks")
	})

	t.Run("refuses an unreadable metadata-location with 400", func(t *testing.T) {
		f := newRegisterFixture(t)
		missing := f.tableLocation(registerTestTableUUID) + "/metadata/there-is-no-such-file.metadata.json"

		rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, registerTableRequest{
			Name:             "missing",
			MetadataLocation: missing,
		})
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("requires the namespace to exist", func(t *testing.T) {
		f := newRegisterFixture(t)
		document := registerTestMetadata(2, registerTestTableUUID, f.tableLocation(registerTestTableUUID), nil, -1)
		metadataLocation := f.putMetadataDocument(t, registerTestTableUUID, document)

		rec := f.post(t, registerTarget(tablesTestRef, Namespace{"inventory", "no-such-namespace"}), writer,
			registerTableRequest{Name: "adopted", MetadataLocation: metadataLocation})
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("writes need a branch", func(t *testing.T) {
		f := newRegisterFixture(t)
		document := registerTestMetadata(2, registerTestTableUUID, f.tableLocation(registerTestTableUUID), nil, -1)
		metadataLocation := f.putMetadataDocument(t, registerTestTableUUID, document)

		// The tag points at the commit the fixture made, so the namespace exists there and only the
		// Q-I4 rule can refuse the write.
		rec := f.post(t, registerTarget(tablesTestTag, namespace), writer, registerTableRequest{
			Name:             "adopted",
			MetadataLocation: metadataLocation,
		})
		apiError := requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
		require.Equal(t, "ref is read-only", apiError.Message)
	})

	t.Run("requires the create grant", func(t *testing.T) {
		f := newRegisterFixture(t)
		document := registerTestMetadata(2, registerTestTableUUID, f.tableLocation(registerTestTableUUID), nil, -1)
		metadataLocation := f.putMetadataDocument(t, registerTestTableUUID, document)

		// alice holds the read half of §7.2 only: catalog:CreateTable is missing.
		rec := f.post(t, registerTarget(tablesTestRef, namespace), "alice", registerTableRequest{
			Name:             "adopted",
			MetadataLocation: metadataLocation,
		})
		requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("refuses a table whose files are outside the catalog's table area", func(t *testing.T) {
		f := newRegisterFixture(t)
		// Inside the repository's storage namespace (so the register location check passes) but
		// outside <storage_namespace>/_iceberg/, where this catalog stages files: §6.3 refuses it
		// while planning, so nothing is staged and the answer is 400.
		outsideRoot := f.storageNamespace + "/warehouse/adopted"
		document := registerTestMetadata(2, registerTestTableUUID, outsideRoot, nil, -1)
		metadataLocation := f.putTableMetadata(t, "warehouse/adopted/metadata/00001.metadata.json", document)

		rec := f.post(t, registerTarget(tablesTestRef, namespace), writer, registerTableRequest{
			Name:             "adopted",
			MetadataLocation: metadataLocation,
		})
		apiError := requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		require.Contains(t, apiError.Message, "outside the catalog's table area")

		_, err := f.store.GetTable(f.ctx, Address{
			Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: namespace, Table: "adopted",
		})
		require.ErrorAs(t, err, new(*NoSuchTableException), "a refused register creates no table")
	})
}
