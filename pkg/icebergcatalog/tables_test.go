package icebergcatalog

// Tests for the table read endpoints (ICEBERG_CATALOG_PLAN.md §2.1 protocol facts, §5 "Addressing
// rules", §7.2 "Authorization" and §11 I-1 "tables list/load/head on any ref"). Every test name
// starts with TestTablesRead so that the task's validation command,
// `go test -run TestTablesRead ./pkg/icebergcatalog/`, runs exactly this file.
//
// The fixture is the real read path end to end: a catalog store (graveler + committed ranges) on
// an in-memory KV store, an in-memory block adapter, the simplified-ACL auth service the
// lakefs-sso binary uses, and the production middleware stack built by newRouter. Pointers are
// staged with the store's own create path and metadata.json files are written through the block
// adapter, so the entries and the documents the endpoints read are the ones the write path of I2
// will produce.
//
// The metadata document below is deliberately not the spec example: it carries three snapshots of
// which only two are referenced (by the implicit main branch and by a tag), and its snapshot IDs
// are larger than 2^53, so the refs-mode assertions fail if an implementation decodes a snapshot
// ID into a float or returns every snapshot regardless of the query parameter.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// tablesTestRepo is the repository the table tree of this file lives in, tablesTestRef the
	// branch it is committed to and tablesTestTag a tag created on that commit, so "a table is
	// readable on any ref" is tested against a real tag.
	tablesTestRepo = authzRepo
	tablesTestRef  = "main"
	tablesTestTag  = "release"

	// tablesTestUUID is the table UUID of the orders table, whose metadata document has the
	// snapshots this file asserts on.
	tablesTestUUID = storeTestUUID

	// The snapshot IDs of tablesTestMetadata: a tag points at tablesTestTagSnapshot, the main
	// branch (current-snapshot-id) at tablesTestCurrentSnapshot, and
	// tablesTestUnreferencedSnapshot is in the snapshots list but referenced by no ref. The
	// values exceed 2^53 on purpose - see the file comment.
	tablesTestTagSnapshot          int64 = 3051729675574597004
	tablesTestUnreferencedSnapshot int64 = 1234567890123456789
	tablesTestCurrentSnapshot      int64 = 3055729675574597004
)

// tablesTestMetadataTemplate is a v2 table metadata document with three snapshots. The first is
// tagged, the third is the current snapshot of the main branch, and the second is only history.
// location is %q, the table uuid is %q.
const tablesTestMetadataTemplate = `{
  "format-version": 2,
  "table-uuid": %q,
  "location": %q,
  "last-sequence-number": 3,
  "last-updated-ms": 1602638573590,
  "last-column-id": 1,
  "current-schema-id": 0,
  "schemas": [
    {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {"owner": "data-team"},
  "current-snapshot-id": %d,
  "snapshots": [
    {
      "snapshot-id": %d,
      "timestamp-ms": 1515100955770,
      "sequence-number": 0,
      "summary": {"operation": "append"},
      "manifest-list": "mem://repo-ns/_iceberg/metadata/1.avro"
    },
    {
      "snapshot-id": %d,
      "parent-snapshot-id": %d,
      "timestamp-ms": 1525100955770,
      "sequence-number": 1,
      "summary": {"operation": "overwrite"},
      "manifest-list": "mem://repo-ns/_iceberg/metadata/2.avro"
    },
    {
      "snapshot-id": %d,
      "parent-snapshot-id": %d,
      "timestamp-ms": 1555100955770,
      "sequence-number": 2,
      "summary": {"operation": "append"},
      "manifest-list": "mem://repo-ns/_iceberg/metadata/3.avro"
    }
  ],
  "snapshot-log": [
    {"snapshot-id": %d, "timestamp-ms": 1515100955770},
    {"snapshot-id": %d, "timestamp-ms": 1525100955770},
    {"snapshot-id": %d, "timestamp-ms": 1555100955770}
  ],
  "metadata-log": [{"metadata-file": "mem://repo-ns/_iceberg/metadata/0.metadata.json", "timestamp-ms": 1515100}],
  "refs": {
    "main": {"snapshot-id": %d, "type": "branch"},
    "v1": {"snapshot-id": %d, "type": "tag"}
  }
}`

// tablesTestMetadata renders tablesTestMetadataTemplate for one table.
func tablesTestMetadata(tableUUID, tableLocation string) string {
	return fmt.Sprintf(tablesTestMetadataTemplate, tableUUID, tableLocation,
		tablesTestCurrentSnapshot,
		tablesTestTagSnapshot,
		tablesTestUnreferencedSnapshot, tablesTestTagSnapshot,
		tablesTestCurrentSnapshot, tablesTestUnreferencedSnapshot,
		tablesTestTagSnapshot,
		tablesTestUnreferencedSnapshot,
		tablesTestCurrentSnapshot,
		tablesTestCurrentSnapshot,
		tablesTestTagSnapshot,
	)
}

// tablesTestNoRefsMetadata is a v1-shaped metadata document: no `refs` map at all, which the
// format spec allows ("there is always a main branch reference pointing to the
// current-snapshot-id even if the refs map is null"). refs mode must still return the current
// snapshot of such a table.
const tablesTestNoRefsMetadata = `{
  "format-version": 1,
  "table-uuid": "f0a2c2c8-5f2b-4d4c-9b1f-1bbbbbbbbbbb",
  "location": "mem://repo-ns/_iceberg/legacy-events",
  "last-updated-ms": 1602638573590,
  "last-column-id": 1,
  "schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
  "current-schema-id": 0,
  "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
  "partition-spec": [],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": 4055729675574597004,
  "snapshots": [
    {
      "snapshot-id": 4055729675574597004,
      "timestamp-ms": 1555100955770,
      "summary": {"operation": "append"},
      "manifest-list": "mem://repo-ns/_iceberg/metadata/legacy.avro"
    }
  ],
  "snapshot-log": [{"snapshot-id": 4055729675574597004, "timestamp-ms": 1555100955770}]
}`

// tablesFixtureSeq numbers the fixture repositories in this process. Each fixture gets its own
// storage namespace so that two fixtures - and the local caches and in-memory object store they
// build on that namespace - cannot interfere.
var tablesFixtureSeq atomic.Int64

// tablesFixture is the table read stack: the catalog store, the ACL auth service, the table
// service and the router a client talks to.
type tablesFixture struct {
	*storeFixture
	authz  *authzFixture
	serve  *tableService
	router http.Handler

	// storageNamespace is the storage namespace of tablesTestRepo, i.e. the prefix of every
	// metadata location this file asserts on (§6.1 writes them fully qualified).
	storageNamespace string
}

// tablesReadStatements grants the read half of §7.2 on every catalog ARN plus the
// fs:ReadRepository every catalog request needs, scoped to the fixture's repository.
func tablesReadStatements() model.Statements {
	return model.Statements{
		{
			Effect: model.StatementEffectAllow,
			Action: []string{
				permissions.CatalogListNamespacesAction,
				permissions.CatalogGetNamespaceAction,
				permissions.CatalogListTablesAction,
				permissions.CatalogReadTableAction,
				permissions.CatalogReadTableDataAction,
			},
			Resource: "arn:lakefs:catalog:::*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: permissions.RepoArn(tablesTestRepo),
		},
	}
}

// tablesOrdersOnlyStatements grants catalog:ListTables on one namespace conditional on
// catalog:TableName, the per-table filter of §7.2, so a listing can be shown to hide a table the
// caller may not see.
func tablesOrdersOnlyStatements() model.Statements {
	return model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.CatalogListTablesAction},
			Resource: permissions.CatalogNamespaceArn(tablesTestRepo, "inventory.books"),
			Condition: map[string]map[string][]string{
				"StringLike": {ConditionFieldTableName: {"ord*"}},
			},
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: permissions.RepoArn(tablesTestRepo),
		},
	}
}

// tablesUser is the authentication stand-in the router is built with: it authenticates every
// request as the user named by X-Test-User (alice by default), so a test can switch between a
// granted caller and one without policies.
func tablesUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username := r.Header.Get("X-Test-User")
		if username == "" {
			username = "alice"
		}
		next.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), &model.User{Username: username})))
	})
}

// newTablesFixture builds the fixture and materializes a small table tree on main:
//
//	inventory/books          orders   (tablesTestMetadata: three snapshots, two referenced)
//	                         titles   (the spec example metadata)
//	inventory/books/archive  backfill (a nested namespace's table: not listed under inventory/books)
//	inventory                summary
//	sales                    (marker only, no table)
//	legacy                   events   (a v1-shaped metadata document without refs)
//
// main is then committed and release tags that commit.
func newTablesFixture(t *testing.T) *tablesFixture {
	t.Helper()
	base := newStoreFixture(t)
	ctx := base.ctx

	// A repository of this fixture's own, with a storage namespace no other fixture shares.
	storageNamespace := fmt.Sprintf("mem://iceberg-tables-%d-ns", tablesFixtureSeq.Add(1))
	_, err := base.catalog.CreateRepository(ctx, tablesTestRepo, "", storageNamespace, tablesTestRef, false)
	require.NoError(t, err, "create catalog repository")

	authz := newAuthzUser(t, tablesReadStatements())
	authz.user(t, "bob")
	authz.user(t, "dana")
	authz.attachStatements(t, "orders-only", "dana", tablesOrdersOnlyStatements())

	store := NewStore(base.catalog, base.catalog.BlockAdapter)
	service := NewTableService(store, authz.authz, base.catalog, &Config{Prefix: CatalogPrefix})

	cfg := &Config{Prefix: CatalogPrefix}
	router, err := newRouter(tablesUser, cfg, catalogServerWith(cfg, service.Routes()...))
	require.NoError(t, err, "build catalog router")

	f := &tablesFixture{
		storeFixture:     base,
		authz:            authz,
		serve:            service,
		router:           router,
		storageNamespace: storageNamespace,
	}

	for _, namespace := range []Namespace{
		{"inventory"},
		{"inventory", "books"},
		{"inventory", "books", "archive"},
		{"sales"},
		{"legacy"},
	} {
		f.createNamespace(t, namespace)
	}

	f.createTableWithMetadata(t, Namespace{"inventory", "books"}, "orders", tablesTestUUID,
		tablesTestMetadata(tablesTestUUID, f.storageNamespace+"/_iceberg/"+tablesTestUUID))
	f.createTableWithMetadata(t, Namespace{"inventory", "books"}, "titles", storeAltUUID,
		storeTestMetadata(t, storeAltUUID, f.storageNamespace+"/_iceberg/"+storeAltUUID))
	f.createTableWithMetadata(t, Namespace{"inventory", "books", "archive"}, "backfill",
		"22222222-2222-2222-2222-222222222222",
		storeTestMetadata(t, "22222222-2222-2222-2222-222222222222",
			f.storageNamespace+"/_iceberg/22222222-2222-2222-2222-222222222222"))
	f.createTableWithMetadata(t, Namespace{"inventory"}, "summary",
		"33333333-3333-3333-3333-333333333333",
		storeTestMetadata(t, "33333333-3333-3333-3333-333333333333",
			f.storageNamespace+"/_iceberg/33333333-3333-3333-3333-333333333333"))
	f.createTableWithMetadata(t, Namespace{"legacy"}, "events",
		"44444444-4444-4444-4444-444444444444", tablesTestNoRefsMetadata)

	ref := f.commitCatalog(t)
	_, err = base.catalog.CreateTag(ctx, tablesTestRepo, tablesTestTag, ref)
	require.NoError(t, err, "create tag %s", tablesTestTag)
	return f
}

// catalogServerWith builds the catalog surface the router mounts: GET /v1/config plus routes, so
// a test can assert both that the routes are served and that they are advertised (§2.1).
func catalogServerWith(cfg *Config, routes ...route) *server {
	s := &server{cfg: cfg}
	s.routes = append([]route{{method: http.MethodGet, pattern: configPath, handler: s.handleConfig}}, routes...)
	s.endpoints = advertisedEndpoints(s.routes)
	return s
}

// createNamespace stages a namespace marker with no properties on main.
func (f *tablesFixture) createNamespace(t *testing.T, namespace Namespace) {
	t.Helper()
	require.NoError(t, f.store.CreateNamespace(f.ctx, Address{
		Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: namespace,
	}, nil), "create namespace %s", namespace.Dotted())
}

// createTableWithMetadata writes document at a fresh metadata.json key and stages the pointer of
// the named table through the store's own create path. It returns the pointer.
func (f *tablesFixture) createTableWithMetadata(t *testing.T, namespace Namespace, table, tableUUID, document string) TablePointer {
	t.Helper()
	location := f.putTableMetadata(t, "_iceberg/"+tableUUID+"/metadata/00001.metadata.json", document)
	pointer := TablePointer{MetadataLocation: location, TableUUID: tableUUID, FormatVersion: 2}
	require.NoError(t, f.store.CreateTable(f.ctx, Address{
		Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: namespace, Table: table,
	}, pointer), "create table %s.%s", namespace.Dotted(), table)
	return pointer
}

// putTableMetadata writes content at key under the fixture repository's storage namespace and
// returns the full URI, the way a client (or the commit path of I2) puts a metadata.json.
func (f *tablesFixture) putTableMetadata(t *testing.T, key, content string) string {
	t.Helper()
	repo, err := f.catalog.GetRepository(f.ctx, tablesTestRepo)
	require.NoError(t, err, "get repository")
	location := strings.TrimRight(repo.StorageNamespace, "/") + "/" + key
	_, err = f.catalog.BlockAdapter.Put(f.ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	}, int64(len(content)), strings.NewReader(content), block.PutOpts{})
	require.NoError(t, err, "put metadata at %s", location)
	return location
}

// commitCatalog commits main and returns the new commit ID.
func (f *tablesFixture) commitCatalog(t *testing.T) string {
	t.Helper()
	log, err := f.catalog.Commit(f.ctx, tablesTestRepo, tablesTestRef, "iceberg tables", "tables-test",
		nil, nil, nil, false)
	require.NoError(t, err, "commit catalog state")
	return log.Reference
}

// request sends one catalog request as user.
func (f *tablesFixture) request(t *testing.T, method, target, user string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-Test-User", user)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// tablesListTarget is the list URL of the namespace named by levels (the bare namespace parameter,
// without the client namespace levels, is built with nodes()).
func tablesListTarget(namespaceParam string, query url.Values) string {
	target := "/v1/" + CatalogPrefix + "/namespaces/" + namespaceParam + "/tables"
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	return target
}

// tablesLoadTarget is the load/head URL of one table.
func tablesLoadTarget(namespaceParam, table string, query url.Values) string {
	target := "/v1/" + CatalogPrefix + "/namespaces/" + namespaceParam + "/tables/" + url.PathEscape(table)
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	return target
}

// loadTableBody is the part of LoadTableResult these tests assert on: the metadata document is
// kept raw so a test can read one field of it without modelling the whole spec.
type loadTableBody struct {
	MetadataLocation string                     `json:"metadata-location"`
	Metadata         map[string]json.RawMessage `json:"metadata"`
	Config           map[string]string          `json:"config"`
}

// decodeLoadTable asserts a successful load and decodes it.
func decodeLoadTable(t *testing.T, rec *httptest.ResponseRecorder) loadTableBody {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var body loadTableBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "body: %s", rec.Body.String())
	require.NotEmpty(t, body.Metadata, "the metadata document is required by LoadTableResult")
	return body
}

// decodeListTables asserts a successful listing and decodes it.
func decodeListTables(t *testing.T, rec *httptest.ResponseRecorder) listTablesResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response listTablesResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return response
}

// tableNamesOf returns the names of a listing, in order, so a test can compare them with a slice.
func tableNamesOf(response listTablesResponse) []string {
	names := make([]string, 0, len(response.Identifiers))
	for _, identifier := range response.Identifiers {
		names = append(names, identifier.Name)
	}
	return names
}

// snapshotIDsOf decodes the snapshot-id of every snapshot a metadata document carries, in order.
// A snapshot-id larger than 2^53 survives only if it was decoded as an integer.
func snapshotIDsOf(t *testing.T, metadata map[string]json.RawMessage) []int64 {
	t.Helper()
	raw, ok := metadata["snapshots"]
	require.True(t, ok, "the metadata document must carry a snapshots field")
	var snapshots []struct {
		SnapshotID int64 `json:"snapshot-id"`
	}
	require.NoError(t, json.Unmarshal(raw, &snapshots), "snapshots: %s", string(raw))
	ids := make([]int64, 0, len(snapshots))
	for _, snapshot := range snapshots {
		ids = append(ids, snapshot.SnapshotID)
	}
	return ids
}

// TestTablesRead covers the list/load/head trio of §11 I-1: the listing levels, pageToken and
// pageSize, the LoadTableResult of a branch, a tag and a staged change, the snapshots query
// parameter, and the error paths of §5, §7.2 and §2.1.
func TestTablesRead(t *testing.T) {
	f := newTablesFixture(t)

	t.Run("list returns the tables of one namespace", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), nil), "alice")
		response := decodeListTables(t, rec)
		require.Equal(t, []string{"orders", "titles"}, tableNamesOf(response))
		require.Empty(t, response.NextPageToken)
		for _, identifier := range response.Identifiers {
			require.Equal(t, Namespace{tablesTestRepo, tablesTestRef, "inventory", "books"}, identifier.Namespace,
				"an identifier carries the same namespace a client sent, macro levels included")
		}
	})

	t.Run("list does not descend into a nested namespace", func(t *testing.T) {
		// tables/inventory/books/archive/backfill/.pointer is not a table of inventory/books.
		rec := f.request(t, http.MethodGet,
			tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), nil), "alice")
		require.NotContains(t, tableNamesOf(decodeListTables(t, rec)), "backfill")

		nested := f.request(t, http.MethodGet,
			tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books", "archive"), nil), "alice")
		require.Equal(t, []string{"backfill"}, tableNamesOf(decodeListTables(t, nested)))
	})

	t.Run("list of a namespace with no table is an empty array", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "sales"), nil), "alice")
		response := decodeListTables(t, rec)
		require.Empty(t, response.Identifiers)
		require.NotNil(t, response.Identifiers, "an empty listing is [], not null")
	})

	t.Run("pageSize and pageToken walk the whole listing", func(t *testing.T) {
		target := tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"),
			url.Values{"pageSize": {"1"}})
		first := decodeListTables(t, f.request(t, http.MethodGet, target, "alice"))
		require.Len(t, first.Identifiers, 1)
		require.NotEmpty(t, first.NextPageToken, "a full page must carry a next-page-token")

		next := tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), url.Values{
			"pageSize":  {"1"},
			"pageToken": {first.NextPageToken},
		})
		second := decodeListTables(t, f.request(t, http.MethodGet, next, "alice"))
		require.Len(t, second.Identifiers, 1)
		require.Empty(t, second.NextPageToken, "the last page carries no token")
		require.Equal(t, []string{"orders", "titles"},
			append(tableNamesOf(first), tableNamesOf(second)...))
	})

	t.Run("load returns the metadata, its physical location and the config", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "orders", nil), "alice")
		body := decodeLoadTable(t, rec)

		require.True(t, strings.HasPrefix(body.MetadataLocation, f.storageNamespace+"/_iceberg/"),
			"metadata-location is the pointer's physical URI, got %q", body.MetadataLocation)
		require.NotContains(t, body.MetadataLocation, ReservedPrefix,
			"metadata-location must not be a lakeFS path")

		var tableUUID, location string
		require.NoError(t, json.Unmarshal(body.Metadata["table-uuid"], &tableUUID))
		require.NoError(t, json.Unmarshal(body.Metadata["location"], &location))
		require.Equal(t, tablesTestUUID, tableUUID)
		require.Equal(t, f.storageNamespace+"/_iceberg/"+tablesTestUUID, location)
		require.Equal(t, "2", string(body.Metadata["format-version"]))

		require.NotNil(t, body.Config, "config is a map, not null")
		require.Empty(t, body.Config, "v1 vends no table configuration: that is phase I-4")
	})

	t.Run("snapshots defaults to all and honours snapshots=all", func(t *testing.T) {
		want := []int64{tablesTestTagSnapshot, tablesTestUnreferencedSnapshot, tablesTestCurrentSnapshot}
		for name, query := range map[string]url.Values{
			"absent": nil,
			"all":    {"snapshots": {SnapshotsAll}},
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet,
					tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "orders", query),
					"alice")
				require.Equal(t, want, snapshotIDsOf(t, decodeLoadTable(t, rec).Metadata))
			})
		}
	})

	t.Run("snapshots=refs keeps only the referenced snapshots", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "orders",
				url.Values{"snapshots": {SnapshotsRefs}}), "alice")
		body := decodeLoadTable(t, rec)

		// The tag points at the first snapshot and main at the last; the middle one is history
		// that no ref names, so refs mode drops it and keeps the original order.
		require.Equal(t, []int64{tablesTestTagSnapshot, tablesTestCurrentSnapshot},
			snapshotIDsOf(t, body.Metadata))

		// Only the snapshots field is rewritten: the rest of the document is what was written.
		require.Equal(t, "2", string(body.Metadata["format-version"]))
		var refs map[string]struct {
			SnapshotID int64 `json:"snapshot-id"`
		}
		require.NoError(t, json.Unmarshal(body.Metadata["refs"], &refs))
		require.Equal(t, tablesTestCurrentSnapshot, refs["main"].SnapshotID)
		require.Equal(t, tablesTestTagSnapshot, refs["v1"].SnapshotID)

		var log []struct {
			SnapshotID int64 `json:"snapshot-id"`
		}
		require.NoError(t, json.Unmarshal(body.Metadata["snapshot-log"], &log))
		require.Len(t, log, 3, "snapshot-log is passed through: the parameter names the snapshots field")
	})

	t.Run("snapshots=refs keeps the current snapshot when refs is absent", func(t *testing.T) {
		// The format spec defines current-snapshot-id as the main branch even when refs is null,
		// so a v1-shaped document must not come back with an empty snapshots list.
		rec := f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "legacy"), "events",
				url.Values{"snapshots": {SnapshotsRefs}}), "alice")
		require.Equal(t, []int64{4055729675574597004}, snapshotIDsOf(t, decodeLoadTable(t, rec).Metadata))
	})

	t.Run("an invalid snapshots value is 400", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "orders",
				url.Values{"snapshots": {"latest"}}), "alice")
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("head answers 204 without a body for an existing table", func(t *testing.T) {
		rec := f.request(t, http.MethodHead,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "orders", nil), "alice")
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String())
	})

	t.Run("a table is readable on a tag", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestTag, "inventory", "books"), "orders", nil), "alice")
		body := decodeLoadTable(t, rec)
		var tableUUID string
		require.NoError(t, json.Unmarshal(body.Metadata["table-uuid"], &tableUUID))
		require.Equal(t, tablesTestUUID, tableUUID)

		head := f.request(t, http.MethodHead,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestTag, "inventory", "books"), "orders", nil), "alice")
		require.Equal(t, http.StatusNoContent, head.Code, "body: %s", head.Body.String())
	})

	t.Run("a missing table is 404 NoSuchTableException", func(t *testing.T) {
		target := tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "missing", nil)
		apiError := requireEnvelope(t, f.request(t, http.MethodGet, target, "alice"),
			http.StatusNotFound, ErrTypeNoSuchTable)
		require.NotEmpty(t, apiError.Message)
		require.Equal(t, http.StatusNotFound, apiError.Code)

		requireEnvelope(t, f.request(t, http.MethodHead, target, "alice"),
			http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("a pointer whose metadata file is gone is a server error, not a missing table", func(t *testing.T) {
		g := newTablesFixture(t)
		addr := Address{Repo: tablesTestRepo, Ref: tablesTestRef,
			Namespace: Namespace{"inventory", "books"}, Table: "titles"}
		pointerPath, err := tablePointerPath(addr)
		require.NoError(t, err)
		g.stageEntryOn(t, tablesTestRepo, tablesTestRef, pointerPath,
			pointerMetadata(g.storageNamespace+"/_iceberg/gone/metadata.json", storeAltUUID, 2))

		rec := g.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "titles", nil), "alice")
		requireEnvelope(t, rec, http.StatusInternalServerError, ErrTypeInternalServerError)
	})

	t.Run("a prefix other than the catalog prefix is 404 NoSuchWarehouseException", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			"/v1/not-the-prefix/namespaces/"+nodes(tablesTestRepo, tablesTestRef, "inventory", "books")+"/tables", "alice")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("a listing address without a namespace level is 400", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			tablesListTarget(nodes(tablesTestRepo, tablesTestRef), nil), "alice")
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("a table address without a namespace level is 400", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef), "orders", nil), "alice")
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("a malformed page token or page size is 400", func(t *testing.T) {
		base := nodes(tablesTestRepo, tablesTestRef, "inventory", "books")
		requireEnvelope(t, f.request(t, http.MethodGet,
			tablesListTarget(base, url.Values{"pageToken": {"!!!not-a-token!!!"}}), "alice"),
			http.StatusBadRequest, ErrTypeBadRequest)
		requireEnvelope(t, f.request(t, http.MethodGet,
			tablesListTarget(base, url.Values{"pageSize": {"one"}}), "alice"),
			http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("a caller without grants cannot list, load or head a table", func(t *testing.T) {
		list := f.request(t, http.MethodGet,
			tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), nil), "bob")
		requireEnvelope(t, list, http.StatusForbidden, ErrTypeForbidden)

		target := tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "orders", nil)
		requireEnvelope(t, f.request(t, http.MethodGet, target, "bob"), http.StatusForbidden, ErrTypeForbidden)
		requireEnvelope(t, f.request(t, http.MethodHead, target, "bob"), http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("a forbidden table cannot be told apart from a missing one", func(t *testing.T) {
		// bob has no grant: a missing table and an existing one answer identically.
		existing := f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "orders", nil), "bob")
		missing := f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "missing", nil), "bob")
		requireEnvelope(t, existing, http.StatusForbidden, ErrTypeForbidden)
		require.Equal(t, existing.Body.String(), missing.Body.String())
	})

	t.Run("a listing hides the tables the caller may not see", func(t *testing.T) {
		// dana may list inventory/books but only tables whose name matches ord*.
		rec := f.request(t, http.MethodGet,
			tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), nil), "dana")
		require.Equal(t, []string{"orders"}, tableNamesOf(decodeListTables(t, rec)))

		// A namespace in which nothing matches is a denial, not an empty page: otherwise dana
		// could tell a namespace without matching tables from one she may not list.
		denied := f.request(t, http.MethodGet,
			tablesListTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books", "archive"), nil), "dana")
		requireEnvelope(t, denied, http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("a staged pointer is visible before its commit and the tag is not", func(t *testing.T) {
		g := newTablesFixture(t)
		addr := Address{Repo: tablesTestRepo, Ref: tablesTestRef,
			Namespace: Namespace{"inventory", "books"}, Table: "orders"}
		newUUID := "55555555-5555-5555-5555-555555555555"
		location := g.putTableMetadata(t, "_iceberg/"+newUUID+"/metadata/00002.metadata.json",
			storeTestMetadata(t, newUUID, g.storageNamespace+"/_iceberg/"+newUUID))
		require.NoError(t, g.store.UpdateTablePointer(g.ctx, addr,
			TablePointer{MetadataLocation: g.tablePointer(t, addr).MetadataLocation,
				TableUUID: tablesTestUUID, FormatVersion: 2},
			TablePointer{MetadataLocation: location, TableUUID: newUUID, FormatVersion: 2}))

		staged := decodeLoadTable(t, g.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"), "orders", nil), "alice"))
		require.Equal(t, location, staged.MetadataLocation, "the branch serves its staging area first")
		var stagedUUID string
		require.NoError(t, json.Unmarshal(staged.Metadata["table-uuid"], &stagedUUID))
		require.Equal(t, newUUID, stagedUUID)

		tagged := decodeLoadTable(t, g.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestTag, "inventory", "books"), "orders", nil), "alice"))
		require.NotEqual(t, location, tagged.MetadataLocation, "a tag is immutable")
		var taggedUUID string
		require.NoError(t, json.Unmarshal(tagged.Metadata["table-uuid"], &taggedUUID))
		require.Equal(t, tablesTestUUID, taggedUUID)
	})

	t.Run("config advertises the table endpoints that are mounted", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, configPath, "alice")
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, []string{listTablesEndpoint, loadTableEndpoint, tableExistsEndpoint},
			response.Endpoints, "the advertised list is the route table of this file")
	})
}

// stageEntryOn stages one entry on an explicit repository and ref: the read tests stage a pointer
// directly to describe a table the store's create path would refuse to write.
func (f *storeFixture) stageEntryOn(t *testing.T, repo, ref, entryPath string, metadata map[string]string) {
	t.Helper()
	require.NoError(t, f.catalog.CreateEntry(f.ctx, repo, ref, catalog.DBEntry{
		Path:            entryPath,
		PhysicalAddress: entryPath,
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    time.Now(),
		Metadata:        metadata,
	}), "stage entry %s", entryPath)
}

// tablePointer returns the pointer currently stored at addr, so a test can swap it.
func (f *tablesFixture) tablePointer(t *testing.T, addr Address) TablePointer {
	t.Helper()
	table, err := f.store.GetTable(f.ctx, addr)
	require.NoError(t, err, "read the table pointer")
	return table.Pointer
}
