package icebergcatalog

// Tests for the table endpoints: the read trio of ICEBERG_CATALOG_PLAN.md §11 I-1 (list, load and
// head on any ref), the staged create of §11 I-2 / task I2-4 (§2.1 protocol facts, §5
// "Addressing rules", §6.2 "Atomic commit", §7.2 "Authorization"), and the drop and metrics sink
// of task I2-5 (§11 I-2, §14 Q-I8). The read tests are named TestTablesRead, the create tests
// TestCreateTable, the create's file registration and commit behaviour
// TestCreateTableStagesMetadataFile (§6.3), and the drop and metrics tests TestDropTable,
// TestDropTableReleasesTableFiles and TestReportMetrics, so each task's validation command (`go test
// -run TestTablesRead`, `-run TestCreateTable`, `-run TestDropTable`) runs the tests it names.
//
// The fixture is the real read path end to end: a catalog store (graveler + committed ranges) on
// an in-memory KV store, an in-memory block adapter, the simplified-ACL auth service the
// lakefs-sso binary uses, and the production middleware stack built by newRouter. Pointers are
// staged with the store's own create path and metadata.json files are written through the block
// adapter, so the entries and the documents the endpoints read are the ones the write path of I2
// will produce. The read tree is built with store.CreateTable directly - it stages a pointer and
// nothing else - while the create tests go through the endpoint, whose create also registers the
// initial metadata file (§6.3, TestCreateTableStagesMetadataFile).
//
// The metadata document below is deliberately not the spec example: it carries three snapshots of
// which only two are referenced (by the implicit main branch and by a tag), and its snapshot IDs
// are larger than 2^53, so the refs-mode assertions fail if an implementation decodes a snapshot
// ID into a float or returns every snapshot regardless of the query parameter.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
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

// tablesReadStatements grants the read half of §7.2 on the catalog ARNs of the fixture's
// repository. The repository scope is in the catalog ARNs themselves: a catalog request needs no
// fs: action (the public Iceberg documentation: "Iceberg catalog permissions supersede any
// related object permissions"), so scoping the catalog ARNs is what scopes the grant.
func tablesReadStatements() model.Statements {
	return model.Statements{
		{
			Effect: model.StatementEffectAllow,
			Action: []string{
				permissions.CatalogListNamespacesAction,
				permissions.CatalogGetNamespaceAction,
				permissions.CatalogListTablesAction,
				permissions.CatalogListViewsAction,
			},
			Resource: permissions.CatalogNamespaceArn(tablesTestRepo, "*"),
		},
		{
			Effect: model.StatementEffectAllow,
			Action: []string{
				permissions.CatalogReadTableAction,
				permissions.CatalogReadTableDataAction,
			},
			Resource: permissions.CatalogTableArn(tablesTestRepo, "*", "*"),
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.CatalogReadViewAction},
			Resource: permissions.CatalogViewArn(tablesTestRepo, "*", "*"),
		},
	}
}

// tablesOrdersOnlyStatements grants catalog:ListTables on one namespace conditional on
// catalog:TableName, the per-table filter of §7.2, so a listing can be shown to hide a table the
// caller may not see. It needs no fs: grant: the catalog action is the whole permission.
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
	// carol holds the write half of §7.2 (authzWriteStatements): the catalog write actions on the
	// catalog ARNs. The create tests use her, so "a caller without the write grant" is alice, who
	// holds the read half only.
	authz.user(t, "carol")
	authz.attachStatements(t, "writer", "carol", authzWriteStatements())

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
		require.Equal(t, []string{
			listTablesEndpoint, loadTableEndpoint, tableExistsEndpoint, createTableEndpoint,
			commitTableEndpoint, dropTableEndpoint, reportMetricsEndpoint,
		}, response.Endpoints, "the advertised list is the route table of this file")
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

// ---------------------------------------------------------------------------------------------
// Create table (§11 I-2, task I2-4)
// ---------------------------------------------------------------------------------------------
//
// The create tests run on the same fixture as the read tests - the same router, store, block
// adapter and auth service - so a create goes through the production middleware stack and the §7.2
// authorization, and its result is read back through the read endpoints. `sales` is the namespace
// newTablesFixture creates with a marker and no table, which is why the creates below happen there:
// nothing they do can be confused with the table tree the read tests assert on.
//
// The schema IDs below are the client's own numbering, as a REST client sends them: the create path
// hands an unbound partition spec and sort order to iceberg-go, which resolves those placeholders
// against this schema and assigns the table's fresh IDs.

const createTableSchema = `{"type": "struct", "schema-id": 0, "fields": [
	{"id": 10, "name": "id", "required": true, "type": "long"},
	{"id": 20, "name": "name", "required": false, "type": "string"}
]}`

// createTableBody builds a CreateTableRequest body: a name, a schema and the members a test adds
// verbatim (a "location", a "partition-spec", "properties", …).
func createTableBody(name, schema string, members ...string) string {
	parts := []string{`"name": ` + strconv.Quote(name), `"schema": ` + schema}
	parts = append(parts, members...)
	return "{" + strings.Join(parts, ", ") + "}"
}

// tablesCreateTarget is the create URL of a namespace: createTable and listTables share one path,
// and only the method tells them apart.
func tablesCreateTarget(namespaceParam string) string {
	return tablesListTarget(namespaceParam, nil)
}

// postTable sends one create request as user.
func (f *tablesFixture) postTable(t *testing.T, namespaceParam, user, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, tablesCreateTarget(namespaceParam), strings.NewReader(body))
	req.Header.Set("X-Test-User", user)
	req.Header.Set("Content-Type", contentTypeJSON)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// tablesNamespaceParam is the namespace path parameter of a namespace of the fixture repository on
// ref: the macro levels (repository, ref) followed by the client namespace levels (§5).
func tablesNamespaceParam(ref string, namespace Namespace) string {
	return nodes(append([]string{tablesTestRepo, ref}, namespace...)...)
}

// tableAddressOf is the address a create test creates.
func tableAddressOf(namespace Namespace, table string) Address {
	return Address{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: namespace, Table: table}
}

// entryOn reads one entry of the fixture repository on ref. It is the fixture's own repository, not
// the storeFixture's, which is why it does not go through storeFixture.entryAt.
func (f *tablesFixture) entryOn(t *testing.T, ref, entryPath string) (*catalog.DBEntry, error) {
	t.Helper()
	return f.catalog.GetEntry(f.ctx, tablesTestRepo, ref, entryPath, catalog.GetEntryParams{})
}

// stagedPointerOf reads the pointer entry the create staged on the ref, i.e. the entry a reader
// resolves before any lakeFS commit.
func (f *tablesFixture) stagedPointerOf(t *testing.T, addr Address) map[string]string {
	t.Helper()
	pointerPath, err := tablePointerPath(addr)
	require.NoError(t, err, "build the pointer path")
	entry, err := f.entryOn(t, addr.Ref, pointerPath)
	require.NoError(t, err, "read the staged pointer %s", pointerPath)
	return entry.Metadata
}

// tableObject reads the object at a fully qualified URI through the block adapter, the way a client
// (or Store.loadMetadata) reads a table file.
func (f *tablesFixture) tableObject(t *testing.T, location string) string {
	t.Helper()
	repo, err := f.catalog.GetRepository(f.ctx, tablesTestRepo)
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

// currentSchemaFieldIDs maps the field names of the table's current schema to the IDs the catalog
// assigned, so a test can assert that a request's placeholder source IDs were resolved instead of
// hard-coding the fresh numbering.
func currentSchemaFieldIDs(t *testing.T, metadata map[string]json.RawMessage) map[string]int {
	t.Helper()
	var schemas []struct {
		Fields []struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		} `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(metadata["schemas"], &schemas), "decode schemas")
	require.NotEmpty(t, schemas, "the metadata of a table carries a schema")
	ids := make(map[string]int, len(schemas[0].Fields))
	for _, field := range schemas[0].Fields {
		ids[field.Name] = field.ID
	}
	return ids
}

// TestCreateTable covers the create of §11 I-2 end to end: the managed default location of §14
// Q-I5, an explicit location inside the table area, the initial metadata.json, the placeholder
// source IDs of a create request, the assert-create condition (a duplicate is 409), and the error
// paths a client can hit - a malformed body, a missing namespace, a tag ref, a missing grant.
func TestCreateTable(t *testing.T) {
	f := newTablesFixture(t)
	sales := Namespace{"sales"}
	salesParam := tablesNamespaceParam(tablesTestRef, sales)

	t.Run("stages the pointer with the managed default location", func(t *testing.T) {
		head := f.headRef(t)

		rec := f.postTable(t, salesParam, "carol", createTableBody("create_default", createTableSchema))
		body := decodeLoadTable(t, rec)

		// §14 Q-I5: <storage_namespace>/_iceberg/<table-uuid>, and the location never carries the
		// ref, so the same table is the same files on every branch (§6.1, §13 rejected alternative).
		var tableUUID, location string
		require.NoError(t, json.Unmarshal(body.Metadata["table-uuid"], &tableUUID))
		require.NoError(t, json.Unmarshal(body.Metadata["location"], &location))
		// The location is the whole of <storage_namespace>/_iceberg/<table-uuid>: nothing in it names
		// the ref, which is what makes it the same on every branch.
		require.Equal(t, f.storageNamespace+"/_iceberg/"+tableUUID, location)

		// The response names a metadata.json under the table's location, and that file is the
		// document the response carries (§6.2 step 4).
		require.True(t, strings.HasPrefix(body.MetadataLocation, location+"/metadata/"),
			"metadata-location %q is not under %q", body.MetadataLocation, location+"/metadata/")
		returned, err := json.Marshal(body.Metadata)
		require.NoError(t, err, "re-encode the returned metadata")
		require.JSONEq(t, string(returned), f.tableObject(t, body.MetadataLocation),
			"the returned metadata is the document that was written")

		var formatVersion int
		require.NoError(t, json.Unmarshal(body.Metadata["format-version"], &formatVersion))
		require.Equal(t, 2, formatVersion, "a create without format-version makes a v2 table")
		if raw, ok := body.Metadata["snapshots"]; ok {
			var snapshots []json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &snapshots))
			require.Empty(t, snapshots, "a new table has no snapshots")
		}

		// The pointer is staged on the branch, with assert-create semantics, and the branch head did
		// not move: create is staged, not auto-committed (§14 Q-I2).
		require.Equal(t, head, f.headRef(t), "create must not commit the branch")
		addr := tableAddressOf(sales, "create_default")
		pointer := f.stagedPointerOf(t, addr)
		require.Equal(t, body.MetadataLocation, pointer[MetadataLocationKey])
		require.Equal(t, tableUUID, pointer[TableUUIDKey])
		require.Equal(t, "2", pointer[FormatVersionKey])

		// And the read path serves it immediately, before any commit.
		loaded := decodeLoadTable(t, f.request(t, http.MethodGet,
			tablesLoadTarget(salesParam, "create_default", nil), "alice"))
		require.Equal(t, body.MetadataLocation, loaded.MetadataLocation)
		require.Equal(t, http.StatusNoContent, f.request(t, http.MethodHead,
			tablesLoadTarget(salesParam, "create_default", nil), "alice").Code)
	})

	t.Run("honours an explicit location inside the table area", func(t *testing.T) {
		location := f.storageNamespace + "/_iceberg/66666666-6666-6666-6666-666666666666"
		rec := f.postTable(t, salesParam, "carol", createTableBody("create_explicit", createTableSchema,
			`"location": `+strconv.Quote(location+"/")))
		body := decodeLoadTable(t, rec)

		var got string
		require.NoError(t, json.Unmarshal(body.Metadata["location"], &got))
		require.Equal(t, location, got, "a trailing slash is normalized away")
		require.True(t, strings.HasPrefix(body.MetadataLocation, location+"/metadata/"),
			"metadata-location %q is not under the requested location", body.MetadataLocation)
		// The location is the client's; the table uuid is still the one the catalog generated, and it
		// is the same in metadata.json and in the pointer.
		var tableUUID string
		require.NoError(t, json.Unmarshal(body.Metadata["table-uuid"], &tableUUID))
		require.Equal(t, tableUUID,
			f.stagedPointerOf(t, tableAddressOf(sales, "create_explicit"))[TableUUIDKey])
		require.NotEmpty(t, f.tableObject(t, body.MetadataLocation), "the metadata file is written there")
	})

	t.Run("refuses a location outside the table area", func(t *testing.T) {
		for name, location := range map[string]string{
			"another bucket":  "mem://some-other-bucket/tables/x",
			"the area itself": f.storageNamespace + "/_iceberg",
			"dot dot":         f.storageNamespace + "/_iceberg/uuid/../../elsewhere",
			"empty segment":   f.storageNamespace + "/_iceberg//uuid",
			"plain path":      "/tmp/tables/x",
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.postTable(t, salesParam, "carol", createTableBody("create_outside", createTableSchema,
					`"location": `+strconv.Quote(location)))
				requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
			})
		}
		// Nothing was staged by any of the refused requests.
		refusedPath, err := tablePointerPath(tableAddressOf(sales, "create_outside"))
		require.NoError(t, err)
		_, err = f.entryOn(t, tablesTestRef, refusedPath)
		require.Error(t, err, "a refused create must not stage a pointer")
	})

	t.Run("carries the request's properties and format version", func(t *testing.T) {
		rec := f.postTable(t, salesParam, "carol", createTableBody("create_v1", createTableSchema,
			`"properties": {"owner": "data-team", "format-version": "1"}`))
		body := decodeLoadTable(t, rec)

		var version int
		require.NoError(t, json.Unmarshal(body.Metadata["format-version"], &version))
		require.Equal(t, 1, version)
		var properties map[string]string
		require.NoError(t, json.Unmarshal(body.Metadata["properties"], &properties))
		require.Equal(t, "data-team", properties["owner"])
		require.NotContains(t, properties, "format-version",
			"format-version is a field of the metadata, not a property of the table")

		pointer := f.stagedPointerOf(t, tableAddressOf(sales, "create_v1"))
		require.Equal(t, "1", pointer[FormatVersionKey], "the pointer advertises the table's own version")
	})

	t.Run("resolves the request's placeholder source IDs", func(t *testing.T) {
		// A create request carries the client's source IDs (10 and 20 here, as a client numbers its
		// own schema), not field IDs of a table that does not exist yet. Decoding them as bound IDs
		// would reject the request; the table's metadata must carry the resolved IDs instead.
		rec := f.postTable(t, salesParam, "carol", createTableBody("create_partitioned", createTableSchema,
			`"partition-spec": {"spec-id": 0, "fields": [
				{"name": "id_bucket", "transform": "bucket[4]", "source-id": 10, "field-id": 1000}
			]}`,
			`"write-order": {"order-id": 1, "fields": [
				{"transform": "identity", "source-id": 20, "direction": "asc", "null-order": "nulls-first"}
			]}`))
		body := decodeLoadTable(t, rec)

		ids := currentSchemaFieldIDs(t, body.Metadata)
		require.Equal(t, []string{"id", "name"}, sortedFieldNames(ids), "the schema keeps its fields")
		require.NotEqual(t, 10, ids["id"], "the catalog assigns the table's own field IDs")

		var specs []struct {
			Fields []struct {
				Name      string `json:"name"`
				Transform string `json:"transform"`
				SourceID  int    `json:"source-id"`
			} `json:"fields"`
		}
		require.NoError(t, json.Unmarshal(body.Metadata["partition-specs"], &specs))
		require.Len(t, specs, 1)
		require.Len(t, specs[0].Fields, 1)
		require.Equal(t, "id_bucket", specs[0].Fields[0].Name)
		require.Equal(t, "bucket[4]", specs[0].Fields[0].Transform)
		require.Equal(t, ids["id"], specs[0].Fields[0].SourceID,
			"the placeholder source ID was resolved to the table's own field ID")

		var orders []struct {
			Fields []struct {
				SourceID int `json:"source-id"`
			} `json:"fields"`
		}
		require.NoError(t, json.Unmarshal(body.Metadata["sort-orders"], &orders))
		require.NotEmpty(t, orders)
		require.Len(t, orders[0].Fields, 1)
		require.Equal(t, ids["name"], orders[0].Fields[0].SourceID)
	})

	t.Run("a duplicate create is 409 AlreadyExistsException", func(t *testing.T) {
		first := decodeLoadTable(t, f.postTable(t, salesParam, "carol",
			createTableBody("create_duplicate", createTableSchema)))
		before := f.stagedPointerOf(t, tableAddressOf(sales, "create_duplicate"))
		require.Equal(t, first.MetadataLocation, before[MetadataLocationKey])

		rec := f.postTable(t, salesParam, "carol", createTableBody("create_duplicate", createTableSchema))
		requireEnvelope(t, rec, http.StatusConflict, ErrTypeAlreadyExists)

		after := f.stagedPointerOf(t, tableAddressOf(sales, "create_duplicate"))
		require.Equal(t, before, after, "the losing create must not replace the pointer")

		// The table the first create made is still the one the read path serves, and it is still the
		// only one under that name.
		require.Equal(t, first.MetadataLocation, decodeLoadTable(t, f.request(t, http.MethodGet,
			tablesLoadTarget(salesParam, "create_duplicate", nil), "alice")).MetadataLocation)
	})

	t.Run("a malformed create body is 400", func(t *testing.T) {
		for name, body := range map[string]string{
			"no schema":         createTableBody("create_bad", "null"),
			"no name":           "{" + `"schema": ` + createTableSchema + "}",
			"empty body":        "",
			"truncated json":    `{"name": "create_bad", "schema":`,
			"schema not struct": createTableBody("create_bad", `{"type": "list"}`),
			"format version 3":  createTableBody("create_bad", createTableSchema, `"properties": {"format-version": "3"}`),
			"unknown transform": createTableBody("create_bad", createTableSchema,
				`"partition-spec": {"spec-id": 0, "fields": [{"name": "x", "transform": "nope", "source-id": 1, "field-id": 1000}]}`),
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.postTable(t, salesParam, "carol", body)
				requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
			})
		}
	})

	t.Run("a namespace that does not exist is 404 NoSuchNamespaceException", func(t *testing.T) {
		missing := tablesNamespaceParam(tablesTestRef, Namespace{"missing_ns"})
		rec := f.postTable(t, missing, "carol", createTableBody("t", createTableSchema))
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("a tag ref is 403 ForbiddenException", func(t *testing.T) {
		tagged := tablesNamespaceParam(tablesTestTag, sales)
		rec := f.postTable(t, tagged, "carol", createTableBody("create_on_tag", createTableSchema))
		requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("a caller without catalog:CreateTable is refused", func(t *testing.T) {
		// alice holds the read half of §7.2 only.
		rec := f.postTable(t, salesParam, "alice", createTableBody("create_denied", createTableSchema))
		requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)

		// bob holds nothing at all, and the answer is the same 403 - the refusal does not say
		// whether the table or the namespace exists.
		rec = f.postTable(t, salesParam, "bob", createTableBody("create_denied", createTableSchema))
		requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("a prefix other than the catalog prefix is 404 NoSuchWarehouseException", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost,
			"/v1/not-the-prefix/namespaces/"+salesParam+"/tables",
			strings.NewReader(createTableBody("t", createTableSchema)))
		req.Header.Set("X-Test-User", "carol")
		rec := httptest.NewRecorder()
		f.router.ServeHTTP(rec, req)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("stage-create initializes the metadata without creating the table", func(t *testing.T) {
		rec := f.postTable(t, salesParam, "carol", createTableBody("create_staged", createTableSchema,
			`"stage-create": true`))
		body := decodeLoadTable(t, rec)

		// The spec's stage-create: "the table is not created, but table metadata is initialized and
		// returned". No pointer is staged, so the table does not exist on the branch.
		require.NotEmpty(t, f.tableObject(t, body.MetadataLocation),
			"the initialized metadata.json is written")
		requireEnvelope(t, f.request(t, http.MethodGet,
			tablesLoadTarget(salesParam, "create_staged", nil), "alice"),
			http.StatusNotFound, ErrTypeNoSuchTable)
		stagedPath, err := tablePointerPath(tableAddressOf(sales, "create_staged"))
		require.NoError(t, err)
		_, err = f.entryOn(t, tablesTestRef, stagedPath)
		require.Error(t, err, "a staged create stages no pointer")
	})

	t.Run("a created table is listed and an uncreated one is not", func(t *testing.T) { // A table is listed exactly when its pointer exists (§6.1, I1-9), which is the other half of
		// "create stages a pointer": the listing sees a staged create, and a stage-create that staged
		// nothing stays invisible.
		require.Equal(t, http.StatusOK, f.postTable(t, salesParam, "carol",
			createTableBody("create_listed", createTableSchema)).Code)
		require.Equal(t, http.StatusOK, f.postTable(t, salesParam, "carol", createTableBody("create_unlisted",
			createTableSchema, `"stage-create": true`)).Code)

		names := tableNamesOf(decodeListTables(t, f.request(t, http.MethodGet,
			tablesListTarget(salesParam, nil), "alice")))
		require.Contains(t, names, "create_listed")
		require.NotContains(t, names, "create_unlisted", "an uncreated staged table is not a table")
	})

	t.Run("the production router mounts and advertises the create endpoint", func(t *testing.T) {
		// RoutesBuilder is what the lakefs-sso binary mounts, so this is the wiring test: the tables
		// service has to be one of its route sources, or the create endpoint would be served by no
		// router at all. An advertised endpoint is a promise (§2.1), and advertisedEndpoints is
		// derived from the mounted routes, so the config response proves both halves at once.
		useConfig(t, "iceberg:\n  enabled: true\n")
		deps := newServerDeps(t)
		deps.Catalog, deps.BlockAdapter = f.catalog, f.catalog.BlockAdapter

		mux := coreRouter()
		require.NoError(t, RoutesBuilder(t.Context(), deps, mux), "mount the catalog")

		rec := do(t, mux, http.MethodGet, APIBasePath+configPath, testAccessKeyID, testSecretAccessKey)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
		for _, endpoint := range []string{createTableEndpoint, listTablesEndpoint, loadTableEndpoint, tableExistsEndpoint} {
			require.Contains(t, response.Endpoints, endpoint)
		}
	})
}

// TestCreateTableStagesMetadataFile covers the one thing the create of TestCreateTable cannot show
// on its own: a created table's initial metadata.json is a lakeFS entry (§6.3), so a table that is
// created and never appended to survives a lakeFS commit and the committed GC that follows it, and
// diff, merge and a dataset materialisation see the file and not only the pointer.
//
// The commit half is what makes it a test of the fix rather than of a staged entry: the pointer and
// the file entry are staged together, one lakeFS commit publishes both, and the metadata file then
// belongs to a retained commit instead of to no one.
func TestCreateTableStagesMetadataFile(t *testing.T) {
	t.Run("the created table's metadata file is an entry a lakeFS commit keeps", func(t *testing.T) {
		f := newTablesFixture(t)
		sales := Namespace{"sales"}
		salesParam := tablesNamespaceParam(tablesTestRef, sales)
		head := f.headRef(t)

		created := decodeLoadTable(t, f.postTable(t, salesParam, "carol",
			createTableBody("staged_metadata", createTableSchema)))
		var tableUUID string
		require.NoError(t, json.Unmarshal(created.Metadata["table-uuid"], &tableUUID))

		// The entry key is the commit path's (§6.3): _lakefs_iceberg/files/<table-uuid>/<path relative
		// to the table root>, built with the production helper so the test cannot pin a key the code
		// does not produce.
		entryPath := metadataFileEntryPath(t, tableUUID, created.MetadataLocation)
		staged, err := f.entryOn(t, tablesTestRef, entryPath)
		require.NoError(t, err, "the initial metadata file must be a lakeFS entry")
		require.Equal(t, created.MetadataLocation, staged.PhysicalAddress,
			"the entry points at the metadata.json the pointer names")
		require.Equal(t, catalog.AddressTypeFull, staged.AddressType)
		require.Equal(t, int64(len(f.tableObject(t, created.MetadataLocation))), staged.Size)

		// Staged, not committed, like every other catalog write (§14 Q-I2).
		require.Equal(t, head, f.headRef(t), "create must not commit the branch")
		_, err = f.entryOn(t, head, entryPath)
		require.ErrorIs(t, err, graveler.ErrNotFound, "the entry is staged, not committed")
		require.Equal(t, created.MetadataLocation, decodeLoadTable(t, f.request(t, http.MethodGet,
			tablesLoadTarget(salesParam, "staged_metadata", nil), "alice")).MetadataLocation)

		// One lakeFS commit publishes the pointer and the file entry together: the point of the fix
		// is that the metadata file is referenced by a retained commit, so it is no longer only a
		// client-written object that nothing protects.
		ref := f.commitCatalog(t)
		committed, err := f.entryOn(t, ref, entryPath)
		require.NoError(t, err, "the metadata entry must survive the lakeFS commit")
		require.Equal(t, staged.PhysicalAddress, committed.PhysicalAddress)
		require.Equal(t, created.MetadataLocation, decodeLoadTable(t, f.request(t, http.MethodGet,
			tablesLoadTarget(salesParam, "staged_metadata", nil), "alice")).MetadataLocation,
			"the table is still readable on the branch after the commit")

		// And it is readable at the commit itself, next to the pointer it belongs to.
		pointerPath, err := tablePointerPath(tableAddressOf(sales, "staged_metadata"))
		require.NoError(t, err)
		_, err = f.entryOn(t, ref, pointerPath)
		require.NoError(t, err, "the pointer is committed next to the file entry")
	})

	t.Run("stage-create stages no metadata file entry", func(t *testing.T) {
		f := newTablesFixture(t)
		salesParam := tablesNamespaceParam(tablesTestRef, Namespace{"sales"})

		created := decodeLoadTable(t, f.postTable(t, salesParam, "carol", createTableBody("staged_only",
			createTableSchema, `"stage-create": true`)))
		var tableUUID string
		require.NoError(t, json.Unmarshal(created.Metadata["table-uuid"], &tableUUID))

		// stage-create initializes the metadata and creates no table, so there is no pointer for a
		// file entry to belong to: an entry without a table would be an uncommitted entry nobody can
		// read and nothing can commit into a table. (The phase-two commit such a client sends is not
		// served yet - see the create's file comment - so a table created that way registers its
		// metadata file when the table itself is created.)
		_, err := f.entryOn(t, tablesTestRef, metadataFileEntryPath(t, tableUUID, created.MetadataLocation))
		require.ErrorIs(t, err, graveler.ErrNotFound, "a staged create stages no table file")
		require.Empty(t, f.stagedFileEntries(t, tablesTestRepo, tablesTestRef, tableUUID),
			"nothing is staged under the table's uuid")
	})

	t.Run("a duplicate create leaves the winner's metadata entry alone", func(t *testing.T) {
		f := newTablesFixture(t)
		salesParam := tablesNamespaceParam(tablesTestRef, Namespace{"sales"})

		first := decodeLoadTable(t, f.postTable(t, salesParam, "carol",
			createTableBody("created_twice", createTableSchema)))
		var tableUUID string
		require.NoError(t, json.Unmarshal(first.Metadata["table-uuid"], &tableUUID))
		winnerPath := metadataFileEntryPath(t, tableUUID, first.MetadataLocation)
		winner, err := f.entryOn(t, tablesTestRef, winnerPath)
		require.NoError(t, err, "the winner registered its metadata file")

		// The losing request writes and registers its own metadata under its own fresh table uuid
		// before the assert-create of the pointer refuses it:
		requireEnvelope(t, f.postTable(t, salesParam, "carol",
			createTableBody("created_twice", createTableSchema)), http.StatusConflict, ErrTypeAlreadyExists)

		// The winner's entry and pointer are untouched, and the table still reads what it read before.
		after, err := f.entryOn(t, tablesTestRef, winnerPath)
		require.NoError(t, err)
		require.Equal(t, winner.PhysicalAddress, after.PhysicalAddress)
		require.Equal(t, first.MetadataLocation, decodeLoadTable(t, f.request(t, http.MethodGet,
			tablesLoadTarget(salesParam, "created_twice", nil), "alice")).MetadataLocation)
	})

	t.Run("a failure after the metadata write keeps the create path's mapping", func(t *testing.T) {
		f := newTablesFixture(t)
		salesParam := tablesNamespaceParam(tablesTestRef, Namespace{"sales"})
		cfg := &Config{Prefix: CatalogPrefix}

		// The registration step runs after metadata.json was written and before the pointer: its
		// failures must surface exactly as a create failure always did, not as a new status. The
		// staging write is redirected so that this stays a test of the mapping rather than of
		// graveler's branch protection (whose rules the fixture's own writes have already cached):
		// a write the repository refuses is still 403 and anything unexpected is still the server's
		// internal error.
		for name, tc := range map[string]struct {
			stageErr   error
			wantStatus int
			wantType   string
		}{
			"a staging write the repository refuses": {
				stageErr:   graveler.ErrWriteToProtectedBranch,
				wantStatus: http.StatusForbidden,
				wantType:   ErrTypeForbidden,
			},
			"an unexpected staging failure": {
				stageErr:   fmt.Errorf("%w: the KV store is unavailable", ErrTableFileRegistration),
				wantStatus: http.StatusInternalServerError,
				wantType:   ErrTypeInternalServerError,
			},
		} {
			t.Run(name, func(t *testing.T) {
				service := NewTableService(
					NewStore(failingStageCatalog{Catalog: f.catalog, err: tc.stageErr}, f.catalog.BlockAdapter),
					f.authz.authz, f.catalog, cfg)
				router, err := newRouter(tablesUser, cfg, catalogServerWith(cfg, service.Routes()...))
				require.NoError(t, err, "build the catalog router")

				req := httptest.NewRequest(http.MethodPost, tablesCreateTarget(salesParam),
					strings.NewReader(createTableBody("refused_create", createTableSchema)))
				req.Header.Set("X-Test-User", "carol")
				req.Header.Set("Content-Type", contentTypeJSON)
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				requireEnvelope(t, rec, tc.wantStatus, tc.wantType)

				// The pointer was never staged, so the table does not exist whatever the failure was.
				pointerPath, err := tablePointerPath(tableAddressOf(Namespace{"sales"}, "refused_create"))
				require.NoError(t, err)
				_, err = f.entryOn(t, tablesTestRef, pointerPath)
				require.ErrorIs(t, err, graveler.ErrNotFound,
					"a create whose file registration failed must not stage a pointer")
			})
		}
	})
}

// failingStageCatalog delegates to a catalog but fails the staging write of table file entries, so
// a test can exercise what the create path answers when the registration that runs after the
// metadata write fails. Everything else - reads, the pointer write, the namespace marker - is the
// fixture's real catalog.
type failingStageCatalog struct {
	Catalog
	err error
}

func (c failingStageCatalog) CreateEntry(ctx context.Context, repositoryID, branch string, entry catalog.DBEntry,
	opts ...graveler.SetOptionsFunc) error {
	if strings.HasPrefix(entry.Path, ReservedPrefix+"/"+FilesDirectory+"/") {
		return c.err
	}
	return c.Catalog.CreateEntry(ctx, repositoryID, branch, entry, opts...)
}

// metadataFileEntryPath builds the entry key of a created table's initial metadata file (§6.3) with
// the production helper: _lakefs_iceberg/files/<table-uuid>/<path relative to the table root>.
func metadataFileEntryPath(t *testing.T, tableUUID, metadataLocation string) string {
	t.Helper()
	value, err := uuid.Parse(tableUUID)
	require.NoError(t, err, "the create response carries a table uuid")
	entryPath, err := TableFilePath(value, path.Join(MetadataDirectory, path.Base(metadataLocation)))
	require.NoError(t, err, "build the table file entry key")
	return entryPath
}

// sortedFieldNames returns the field names of currentSchemaFieldIDs in lexical order, so a test can
// assert the set of fields the catalog kept without depending on map iteration order or on the IDs
// it assigned.
func sortedFieldNames(m map[string]int) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ---------------------------------------------------------------------------------------------
// Drop table and report metrics (§11 I-2, task I2-5)
// ---------------------------------------------------------------------------------------------
//
// The drop and metrics tests run on the same fixture as the read and create tests - the same router,
// store, block adapter and auth service - so both go through the production middleware stack and the
// §7.2 authorization, and their effect is read back through the read endpoints.

// tablesCatalogRef is the commit tablesTestRef points at, i.e. what an explicit lakeFS commit last
// wrote to the fixture's repository. storeFixture.headRef answers the same question for the store
// fixture's own repository ("repo"), which is not the repository these tables live in ("lakehouse").
func (f *tablesFixture) tablesCatalogRef(t *testing.T) string {
	t.Helper()
	ref, err := f.catalog.GetBranchReference(f.ctx, tablesTestRepo, tablesTestRef)
	require.NoError(t, err, "get the %s branch reference", tablesTestRef)
	return ref
}

// tableMetricsTarget is the report-metrics URL of one table.
func tableMetricsTarget(namespaceParam, table string) string {
	return tablesLoadTarget(namespaceParam, table, nil) + "/metrics"
}

// deleteTable sends one drop request as user. The optional query parameters are the ones a client
// adds to a drop - `purgeRequested` - so a test can show they change nothing.
func (f *tablesFixture) deleteTable(t *testing.T, namespaceParam, table, user string, query url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, tablesLoadTarget(namespaceParam, table, query), nil)
	req.Header.Set("X-Test-User", user)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// postTableMetrics sends one report-metrics request as user.
func (f *tablesFixture) postTableMetrics(t *testing.T, namespaceParam, table, user, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, tableMetricsTarget(namespaceParam, table), strings.NewReader(body))
	req.Header.Set("X-Test-User", user)
	req.Header.Set("Content-Type", contentTypeJSON)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// deleteNamespace sends one namespace drop request as user through router. The namespace parameter
// is the same path parameter a table request uses, macro levels included (§5).
func deleteNamespace(t *testing.T, router http.Handler, namespaceParam, user string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, "/v1/"+CatalogPrefix+"/namespaces/"+namespaceParam, nil)
	req.Header.Set("X-Test-User", user)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// pointerEntryGone asserts that the table pointer of addr is no longer an entry of addr.Ref, i.e.
// that the drop staged a removal rather than leaving the pointer behind.
func (f *tablesFixture) pointerEntryGone(t *testing.T, addr Address) {
	t.Helper()
	pointerPath, err := tablePointerPath(addr)
	require.NoError(t, err, "build the pointer path")
	_, err = f.entryOn(t, addr.Ref, pointerPath)
	require.ErrorIs(t, err, graveler.ErrNotFound, "the pointer entry %s must be gone", pointerPath)
}

// TestDropTable covers the drop of §11 I-2 end to end: the pointer removal, the release of the
// table's file entries (its own test, TestDropTableReleasesTableFiles), the data-plane objects that
// stay until GC (§14 Q-I8), the any-ref behaviour of a staged drop, the namespace that becomes
// droppable once its last table is, and the error paths a client can hit.
//
// What a drop must be, and what these subtests therefore pin:
//   - the pointer removal is what ends the table: it stops existing on the ref it was dropped on and
//     keeps existing on every other ref whose pointer still names its metadata - the release tag of
//     the fixture proves the any-ref half. The file entries §6.3 staged for the table go with the
//     pointer (TestDropTableReleasesTableFiles); the tables of this fixture are created with
//     store.CreateTable, which stages a pointer and no file entry, so for them the pointer is the
//     whole of the drop;
//   - it deletes no data-plane object: the metadata.json the dropped pointer named is still readable
//     from the object store afterwards, which is the Q-I8 rule of §14 and the reason a drop is a
//     staged, revertible lakeFS change rather than a destructive purge;
//   - the spec's `purgeRequested` query parameter changes nothing, whatever its value;
//   - a table that does not exist is 404 NoSuchTableException, a tag ref is the Q-I4 403, and a
//     caller without catalog:DeleteTable is refused with the same 403.
func TestDropTable(t *testing.T) {
	f := newTablesFixture(t)
	books := Namespace{"inventory", "books"}
	booksParam := tablesNamespaceParam(tablesTestRef, books)
	ordersAddr := tableAddressOf(books, "orders")
	titlesAddr := tableAddressOf(books, "titles")

	// The pointers and the document of the table the first subtest drops, read before the drop.
	ordersPointer := f.tablePointer(t, ordersAddr)
	titlesPointer := f.tablePointer(t, titlesAddr)
	ordersDocument := f.tableObject(t, ordersPointer.MetadataLocation)

	t.Run("drops the pointer and leaves the data-plane objects in place", func(t *testing.T) {
		head := f.tablesCatalogRef(t)

		rec := f.deleteTable(t, booksParam, "orders", "carol", nil)
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String(), "a drop has no response body")

		// The table is gone from every read of the branch: load, head and the listing agree.
		requireEnvelope(t, f.request(t, http.MethodGet, tablesLoadTarget(booksParam, "orders", nil), "alice"),
			http.StatusNotFound, ErrTypeNoSuchTable)
		requireEnvelope(t, f.request(t, http.MethodHead, tablesLoadTarget(booksParam, "orders", nil), "alice"),
			http.StatusNotFound, ErrTypeNoSuchTable)
		names := tableNamesOf(decodeListTables(t, f.request(t, http.MethodGet,
			tablesListTarget(booksParam, nil), "alice")))
		require.NotContains(t, names, "orders")
		require.Contains(t, names, "titles", "a drop must not touch a sibling table")

		f.pointerEntryGone(t, ordersAddr)
		require.Equal(t, titlesPointer, f.tablePointer(t, titlesAddr), "the sibling pointer is unchanged")

		// §14 Q-I8: the drop removed the pointer, not the objects the table's files are. The
		// metadata.json of the dropped table is still in the object store, and so are the data/manifest
		// files it references; releasing the entries that protected them (a table of this fixture has
		// none) hands the objects to GC instead of deleting them (§6.3, phase I-3).
		require.Equal(t, ordersDocument, f.tableObject(t, ordersPointer.MetadataLocation),
			"the dropped table's metadata.json must stay until GC")

		// The drop is staged like every other catalog write: the branch head did not move, and
		// nothing was committed (§6.2, §14 Q-I2).
		require.Equal(t, head, f.tablesCatalogRef(t), "a drop must not commit the branch")
	})

	t.Run("a table dropped on a branch is still readable on a tag", func(t *testing.T) {
		// release tagged the commit the fixture made before any drop, so the tag's pointer still
		// names the dropped table's metadata. That is the any-ref read of §1.1 goal 1, and the reason
		// a drop may only touch the ref it is staged on: the tag is its own commit, so neither the
		// pointer removal nor the file-entry release can reach it.
		tagged := tablesNamespaceParam(tablesTestTag, books)
		body := decodeLoadTable(t, f.request(t, http.MethodGet, tablesLoadTarget(tagged, "orders", nil), "alice"))
		require.Equal(t, ordersPointer.MetadataLocation, body.MetadataLocation,
			"the tag still names the metadata of the table dropped on the branch")
		require.Equal(t, http.StatusNoContent, f.request(t, http.MethodHead,
			tablesLoadTarget(tagged, "orders", nil), "alice").Code)
	})

	t.Run("a commit carries the drop, and the previous commit still has the table", func(t *testing.T) {
		before := f.tablesCatalogRef(t)
		require.Equal(t, http.StatusNoContent, f.deleteTable(t, booksParam, "titles", "carol", nil).Code)

		// Before the commit the drop is only in the staging area (previous subtest's observation);
		// the commit is what makes it part of the repository.
		after := f.commitCatalog(t)
		require.NotEqual(t, before, after, "the commit wrote a new commit")

		requireEnvelope(t, f.request(t, http.MethodGet, tablesLoadTarget(booksParam, "titles", nil), "alice"),
			http.StatusNotFound, ErrTypeNoSuchTable)
		// ... and the commit it superseded still serves the table: a drop is revertible because only
		// entries were removed - no object was touched - so reverting the commit restores the pointer
		// and the entries together.
		previous := tablesNamespaceParam(before, books)
		require.Equal(t, titlesPointer.MetadataLocation,
			decodeLoadTable(t, f.request(t, http.MethodGet,
				tablesLoadTarget(previous, "titles", nil), "alice")).MetadataLocation)
	})

	t.Run("purgeRequested is accepted and purges nothing", func(t *testing.T) {
		// iceberg-go sends purgeRequested=false for DropTable and true for PurgeTable. v1 answers
		// both with the same drop: no object is deleted, because the files are GC's to reclaim and one
		// catalog request must not delete files another ref may still reference (§14 Q-I8). The
		// entries that protected them are released with the pointer (§6.3,
		// TestDropTableReleasesTableFiles).
		const uuid = "77777777-7777-7777-7777-777777777777"
		sales := Namespace{"sales"}
		salesParam := tablesNamespaceParam(tablesTestRef, sales)

		for _, purge := range []string{"false", "true"} {
			t.Run("purgeRequested="+purge, func(t *testing.T) {
				pointer := f.createTableWithMetadata(t, sales, "purged", uuid,
					storeTestMetadata(t, uuid, f.storageNamespace+"/_iceberg/"+uuid))

				rec := f.deleteTable(t, salesParam, "purged", "carol", url.Values{"purgeRequested": {purge}})
				require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())

				f.pointerEntryGone(t, tableAddressOf(sales, "purged"))
				require.NotEmpty(t, f.tableObject(t, pointer.MetadataLocation),
					"purgeRequested=%s must not delete the table's files", purge)
			})
		}
	})

	t.Run("an unknown table is 404 NoSuchTableException", func(t *testing.T) {
		// A table that never existed, a table dropped twice, and a table in a namespace that does
		// not exist are the same answer: the pointer is what makes a table exist (§6.1).
		for name, namespaceParam := range map[string]string{
			"never created": booksParam,
			"missing ns":    tablesNamespaceParam(tablesTestRef, Namespace{"missing_ns"}),
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.deleteTable(t, namespaceParam, "no_such_table", "carol", nil)
				requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchTable)
			})
		}

		// orders was dropped by the first subtest, so dropping it again is the same 404.
		rec := f.deleteTable(t, booksParam, "orders", "carol", nil)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("a namespace becomes droppable once its last table is dropped", func(t *testing.T) {
		// The last half of "a drop no longer keeps the table alive": the namespace emptiness check of
		// I2-2 walks the same `tables/<ns>/` prefix a drop cleans up, so a table that is dropped
		// stops keeping its namespace alive, and nothing else has to be deleted.
		//
		// This fixture's router carries the table service's routes alone (the read tests assert the
		// exact advertised list), so the namespace endpoints of I2-2 are mounted here explicitly.
		cfg := &Config{Prefix: CatalogPrefix}
		router, err := newRouter(tablesUser, cfg,
			newServer(cfg, f.serve, NewNamespaceService(f.store, f.authz.authz, f.catalog, cfg)))
		require.NoError(t, err, "mount the namespace and table services together")

		sales := Namespace{"sales"}
		salesParam := tablesNamespaceParam(tablesTestRef, sales)
		require.Equal(t, http.StatusOK, f.postTable(t, salesParam, "carol",
			createTableBody("last_table", createTableSchema)).Code)

		rec := deleteNamespace(t, router, salesParam, "carol")
		requireEnvelope(t, rec, http.StatusConflict, ErrTypeNamespaceNotEmpty)

		require.Equal(t, http.StatusNoContent, f.deleteTable(t, salesParam, "last_table", "carol", nil).Code)
		rec = deleteNamespace(t, router, salesParam, "carol")
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("a tag ref is 403 ForbiddenException", func(t *testing.T) {
		tagged := tablesNamespaceParam(tablesTestTag, books)
		rec := f.deleteTable(t, tagged, "orders", "carol", nil)
		requireForbiddenReadOnly(t, rec)
	})

	t.Run("a caller without catalog:DeleteTable is refused", func(t *testing.T) {
		// alice holds the read half of §7.2 only, bob holds nothing at all: both get the same 403,
		// and neither refusal says whether the table exists.
		inventory := Namespace{"inventory"}
		inventoryParam := tablesNamespaceParam(tablesTestRef, inventory)
		for _, user := range []string{"alice", "bob"} {
			rec := f.deleteTable(t, inventoryParam, "summary", user, nil)
			requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
		}
		// The refused requests changed nothing: the table is still there.
		require.Equal(t, http.StatusNoContent, f.request(t, http.MethodHead,
			tablesLoadTarget(inventoryParam, "summary", nil), "alice").Code)
	})

	t.Run("a prefix other than the catalog prefix is 404 NoSuchWarehouseException", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete,
			"/v1/not-the-prefix/namespaces/"+booksParam+"/tables/titles", nil)
		req.Header.Set("X-Test-User", "carol")
		rec := httptest.NewRecorder()
		f.router.ServeHTTP(rec, req)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("the production router mounts and advertises the drop endpoint", func(t *testing.T) {
		// An advertised endpoint is a promise (§2.1), and advertisedEndpoints is derived from the
		// mounted routes, so the config response of the production router proves both halves at
		// once: the tables service contributes dropTable and reportMetrics.
		useConfig(t, "iceberg:\n  enabled: true\n")
		deps := newServerDeps(t)
		deps.Catalog, deps.BlockAdapter = f.catalog, f.catalog.BlockAdapter

		mux := coreRouter()
		require.NoError(t, RoutesBuilder(t.Context(), deps, mux), "mount the catalog")

		rec := do(t, mux, http.MethodGet, APIBasePath+configPath, testAccessKeyID, testSecretAccessKey)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
		require.Contains(t, response.Endpoints, dropTableEndpoint)
		require.Contains(t, response.Endpoints, reportMetricsEndpoint)
	})
}

// TestReportMetrics covers the metrics sink of §11 I-2: the endpoint accepts a client's scan or
// commit report, stores nothing, and answers 204 - while still refusing a caller without the
// catalog:UpdateTable action and a report for a table this catalog does not have.
func TestReportMetrics(t *testing.T) {
	f := newTablesFixture(t)
	books := Namespace{"inventory", "books"}
	booksParam := tablesNamespaceParam(tablesTestRef, books)

	// The two report shapes of the spec's ReportMetricsRequest, flattened around the report-type
	// discriminator. The body is discarded, so these are here to show the endpoint accepts the
	// documents a real client sends - not to have the fields asserted on.
	const (
		scanReport = `{"report-type": "scan-report", "table-name": "lakehouse.main.inventory.books.orders",
			"snapshot-id": 3055729675574597004, "filter": {"type": "true"}, "schema-id": 0,
			"projected-field-ids": [1], "projected-field-names": ["x"], "metadata": {}, "metrics": {}}`
		commitReport = `{"report-type": "commit-report", "table-name": "lakehouse.main.inventory.books.orders",
			"snapshot-id": 3055729675574597004, "sequence-number": 1, "operation": "append",
			"commit-metrics": {"total-data-files": 1, "added-data-files": 1},
			"commit-report": {"duration-ms": 12, "manifests-created": 1, "metadata-json-result": {}}}`
	)

	t.Run("a scan report is accepted and discarded", func(t *testing.T) {
		head := f.tablesCatalogRef(t)
		before := f.stagedPointerOf(t, tableAddressOf(books, "orders"))

		rec := f.postTableMetrics(t, booksParam, "orders", "carol", scanReport)
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String(), "reportMetrics has no response body")

		// Nothing was written: the branch head did not move and the pointer is the same entry.
		require.Equal(t, head, f.tablesCatalogRef(t), "reporting metrics must not commit the branch")
		require.Equal(t, before, f.stagedPointerOf(t, tableAddressOf(books, "orders")))
	})

	t.Run("a commit report is accepted and discarded", func(t *testing.T) {
		rec := f.postTableMetrics(t, booksParam, "orders", "carol", commitReport)
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String())
	})

	t.Run("the payload is not decoded", func(t *testing.T) {
		// A report is telemetry: the catalog has nowhere to put it (v1 stores nothing) and a client
		// only checks whether the request failed, so the shape of a payload the server throws away
		// must not turn into a 400. An empty body and an unknown report type are both accepted.
		for name, body := range map[string]string{
			"empty body":          "",
			"unknown report type": `{"report-type": "some-future-report"}`,
			"not json at all":     "not json",
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.postTableMetrics(t, booksParam, "orders", "carol", body)
				require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
			})
		}
		// The table is still exactly what it was: metrics reporting is not a state change.
		require.Equal(t, http.StatusOK, f.request(t, http.MethodGet,
			tablesLoadTarget(booksParam, "orders", nil), "alice").Code)
	})

	t.Run("a report for a table that does not exist is 404 NoSuchTableException", func(t *testing.T) {
		rec := f.postTableMetrics(t, booksParam, "no_such_table", "carol", scanReport)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("a report for a tag ref is 403 ForbiddenException", func(t *testing.T) {
		// §7.2 has no row of its own for metrics; authz.go assigns the operation to
		// catalog:UpdateTable, which is a write of the §7.2 matrix and so needs a branch (Q-I4).
		// Reporting a scan of a tag is therefore refused as a read-only ref, like every other write
		// the catalog refuses there.
		tagged := tablesNamespaceParam(tablesTestTag, books)
		rec := f.postTableMetrics(t, tagged, "orders", "carol", scanReport)
		requireForbiddenReadOnly(t, rec)
	})

	t.Run("a caller without catalog:UpdateTable is refused", func(t *testing.T) {
		// alice holds the read half of §7.2 only, bob holds nothing at all.
		for _, user := range []string{"alice", "bob"} {
			rec := f.postTableMetrics(t, booksParam, "orders", user, scanReport)
			requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
		}
	})

	t.Run("a prefix other than the catalog prefix is 404 NoSuchWarehouseException", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost,
			"/v1/not-the-prefix/namespaces/"+booksParam+"/tables/orders/metrics",
			strings.NewReader(scanReport))
		req.Header.Set("X-Test-User", "carol")
		rec := httptest.NewRecorder()
		f.router.ServeHTTP(rec, req)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("a table dropped on the branch takes its metrics endpoint with it", func(t *testing.T) {
		// Existence is the pointer (§6.1), so a drop ends the table's metrics endpoint too.
		require.Equal(t, http.StatusNoContent, f.deleteTable(t, booksParam, "orders", "carol", nil).Code)
		rec := f.postTableMetrics(t, booksParam, "orders", "carol", scanReport)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("config advertises report metrics", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, configPath, "alice")
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Contains(t, response.Endpoints, reportMetricsEndpoint)
	})
}

// ---------------------------------------------------------------------------------------------
// Table-load entity tags (§2.1 "ETag / If-None-Match", §11 I-5)
// ---------------------------------------------------------------------------------------------
//
// A client that re-reads a table it already holds should pay for a 304 and not for the metadata
// document. The one requirement the header must not get wrong is that the tag varies with the
// `snapshots` query parameter: `all` and `refs` are different representations of the same table, so
// a tag shared between them would let a cache answer a `refs` request with the `all` document (or
// the reverse) and never be caught - the response is a 2xx with valid JSON, just the wrong one.

// TestETagTableLoad covers the validator of a table load: the tag on a 200, the 304 of a matching
// If-None-Match, the tag that must differ between snapshots=all and snapshots=refs, a weak or listed
// tag, and the tag that changes when the table does.
func TestETagTableLoad(t *testing.T) {
	f := newTablesFixture(t)
	books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})

	// load issues one table load, optionally conditional.
	load := func(t *testing.T, query url.Values, ifNoneMatch string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, tablesLoadTarget(books, "orders", query), nil)
		req.Header.Set("X-Test-User", "alice")
		if ifNoneMatch != "" {
			req.Header.Set("If-None-Match", ifNoneMatch)
		}
		rec := httptest.NewRecorder()
		f.router.ServeHTTP(rec, req)
		return rec
	}
	// etagOf loads the table and returns its ETag, asserting the 200 it came with.
	etagOf := func(t *testing.T, query url.Values) string {
		t.Helper()
		rec := load(t, query, "")
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		etag := rec.Header().Get("ETag")
		require.NotEmpty(t, etag, "a load returns an entity tag")
		require.True(t, strings.HasPrefix(etag, `"`) && strings.HasSuffix(etag, `"`),
			"an ETag header value is a quoted string, got %q", etag)
		return etag
	}

	allTag := etagOf(t, nil)
	refsTag := etagOf(t, url.Values{"snapshots": {SnapshotsRefs}})

	t.Run("a matching If-None-Match is 304 with no body", func(t *testing.T) {
		rec := load(t, nil, allTag)
		require.Equal(t, http.StatusNotModified, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String(), "a 304 carries no body")
		require.Equal(t, allTag, rec.Header().Get("ETag"), "the validator is repeated on the 304")
	})

	t.Run("absent and snapshots=all are the same representation", func(t *testing.T) {
		require.Equal(t, allTag, etagOf(t, url.Values{"snapshots": {SnapshotsAll}}),
			"the default and the explicit value select the same document, so they share a tag")
	})

	t.Run("snapshots=refs has its own tag and refuses the all tag", func(t *testing.T) {
		require.NotEqual(t, allTag, refsTag,
			"all and refs are different representations: a shared tag would serve the wrong one")

		rec := load(t, url.Values{"snapshots": {SnapshotsRefs}}, allTag)
		require.Equal(t, http.StatusOK, rec.Code,
			"the tag of another representation must not match, body: %s", rec.Body.String())
		require.Equal(t, refsTag, rec.Header().Get("ETag"))
		require.NotEmpty(t, rec.Body.String())

		refsMatch := load(t, url.Values{"snapshots": {SnapshotsRefs}}, refsTag)
		require.Equal(t, http.StatusNotModified, refsMatch.Code, "body: %s", refsMatch.Body.String())
	})

	t.Run("a weak tag, a tag list and * are honoured", func(t *testing.T) {
		for name, header := range map[string]string{
			"weak":      `W/` + allTag,
			"in a list": `"not-the-tag", ` + allTag,
			"star":      `*`,
		} {
			t.Run(name, func(t *testing.T) {
				rec := load(t, nil, header)
				require.Equal(t, http.StatusNotModified, rec.Code, "body: %s", rec.Body.String())
			})
		}

		rec := load(t, nil, `"not-the-tag"`)
		require.Equal(t, http.StatusOK, rec.Code, "an unrelated tag is a full response")
	})

	t.Run("a client that sends no header is unaffected", func(t *testing.T) {
		rec := load(t, nil, "")
		require.Equal(t, http.StatusOK, rec.Code)
		require.NotEmpty(t, rec.Body.String(), "no conditional request, no empty response")
	})

	t.Run("the tag changes when the table does", func(t *testing.T) {
		// A commit stages a new metadata.json, i.e. a different document for the same table.
		require.Equal(t, http.StatusOK, f.commitTable(t, books, "orders", "carol",
			commitTableBody(`[]`, setPropertiesUpdate("etag-team"))).Code)

		rec := load(t, nil, allTag)
		require.Equal(t, http.StatusOK, rec.Code,
			"the old tag must not match the committed table, body: %s", rec.Body.String())
		require.NotEqual(t, allTag, rec.Header().Get("ETag"), "the new representation has a new tag")
	})
}

// TestDropTableReleasesTableFiles is the release half of the drop (files.go "What a drop releases",
// RESULTS.md N-3): a drop removes the table's file entries together with its pointer, in one lakeFS
// commit, so the committed GC can reclaim the dropped table's objects instead of protecting them for
// as long as the ref keeps the commits that carry them.
//
// The measurement this pins is a diff, not a status code. §6.3 stages _lakefs_iceberg/files/
// <table-uuid>/… so that the GC protects exactly the live files, and the defect was that the drop
// commit removed the pointer and no other path - which protected exactly the dead ones. So the test
// builds a table the way the write path does (a create through the endpoint, whose metadata file the
// production create path registers, plus the file entries a later append commit stages), commits it,
// drops it, and then asserts the two diffs: the branch's uncommitted diff after the drop and the
// drop commit's own diff are both exactly the pointer plus every file entry, and nothing else.
func TestDropTableReleasesTableFiles(t *testing.T) {
	sales := Namespace{"sales"}
	salesParam := tablesNamespaceParam(tablesTestRef, sales)

	t.Run("the drop commit removes the pointer and every file entry", func(t *testing.T) {
		f := newTablesFixture(t)

		// Created through the endpoint: the create path registers the initial metadata.json under
		// the table's uuid (§6.3 step 1, TestCreateTableStagesMetadataFile).
		created := decodeLoadTable(t, f.postTable(t, salesParam, "carol",
			createTableBody("released", createTableSchema)))
		var tableUUID string
		require.NoError(t, json.Unmarshal(created.Metadata["table-uuid"], &tableUUID))
		tableUUIDValue, err := uuid.Parse(tableUUID)
		require.NoError(t, err, "the create response carries a table uuid")
		tableRoot := f.storageNamespace + "/_iceberg/" + tableUUID

		// The entries an append commit of the same table stages: a manifest list, a manifest and two
		// data files. The commit path registers exactly these keys (TestCommitStagesTableFiles); here
		// they stand for "the table has files from an earlier commit", which is the state the drop has
		// to release.
		fileEntries := []string{metadataFileEntryPath(t, tableUUID, created.MetadataLocation)}
		for index, relativePath := range []string{
			"metadata/snap-1-1-" + tableUUID + ".avro",
			"metadata/" + tableUUID + "-m0.avro",
			"data/00000-0-" + tableUUID + ".parquet",
			"data/00001-0-" + tableUUID + ".parquet",
		} {
			entryPath, err := TableFilePath(tableUUIDValue, relativePath)
			require.NoError(t, err, "build the entry key of %s", relativePath)
			fileEntries = append(fileEntries, entryPath)
			require.NoError(t, f.catalog.CreateEntry(f.ctx, tablesTestRepo, tablesTestRef, catalog.DBEntry{
				Path:            entryPath,
				PhysicalAddress: tableRoot + "/" + relativePath,
				AddressType:     catalog.AddressTypeFull,
				CreationDate:    time.UnixMilli(1602638574000).UTC(),
				Size:            int64(index + 1),
			}), "stage the table file %s", relativePath)
		}
		pointerPath, err := tablePointerPath(tableAddressOf(sales, "released"))
		require.NoError(t, err, "build the pointer path")

		// One commit publishes the pointer and the table's files together - the create's and the
		// append's - which is the state that keeps the table's objects alive (§6.3).
		before := f.commitCatalog(t)
		for _, entryPath := range append(append([]string{}, fileEntries...), pointerPath) {
			_, err := f.entryOn(t, before, entryPath)
			require.NoError(t, err, "entry %s must be part of the commit", entryPath)
		}
		wantRemoved := append(append([]string{}, fileEntries...), pointerPath)
		sort.Strings(wantRemoved)

		rec := f.deleteTable(t, salesParam, "released", "carol", nil)
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())

		// Before any commit the drop is a staged removal of exactly those paths: the pointer and every
		// file entry, and nothing else on the branch.
		uncommitted, _, err := f.catalog.DiffUncommitted(f.ctx, tablesTestRepo, tablesTestRef, "", "", -1, "")
		require.NoError(t, err, "read the branch's uncommitted diff")
		require.Equal(t, wantRemoved, removedPaths(t, uncommitted),
			"the drop stages the removal of the pointer and of every file entry")
		for _, entryPath := range fileEntries {
			_, err := f.entryOn(t, tablesTestRef, entryPath)
			require.ErrorIs(t, err, graveler.ErrNotFound, "the file entry %s must be released", entryPath)
		}

		// No object was deleted: the drop releases entries, and the committed GC is what decides that
		// an object is unreferenced (§14 Q-I8).
		require.NotEmpty(t, f.tableObject(t, created.MetadataLocation),
			"a drop must not delete the metadata.json it released the entry of")

		// And the commit a client makes next carries the whole release.
		after := f.commitCatalog(t)
		require.NotEqual(t, before, after, "the drop was committed")
		differences, _, err := f.catalog.Diff(f.ctx, tablesTestRepo, before, after, catalog.DiffParams{Limit: -1})
		require.NoError(t, err, "diff the drop commit")
		require.Equal(t, wantRemoved, removedPaths(t, differences),
			"the drop commit removes the pointer and every file entry of the table")

		// The table is gone, and the ref it was dropped on still serves its neighbours.
		requireEnvelope(t, f.request(t, http.MethodGet, tablesLoadTarget(salesParam, "released", nil), "alice"),
			http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("a drop releases the files of its own table only", func(t *testing.T) {
		// Two tables in one namespace, each with a file entry of its own: dropping one must leave the
		// other's entries and the pointer of the other table in place. This is the "no half release"
		// half of the ordering - the release walks one directory, not the files area.
		f := newTablesFixture(t)
		created := map[string]string{}
		for _, table := range []string{"kept", "dropped"} {
			body := decodeLoadTable(t, f.postTable(t, salesParam, "carol", createTableBody(table, createTableSchema)))
			var tableUUID string
			require.NoError(t, json.Unmarshal(body.Metadata["table-uuid"], &tableUUID))
			created[table] = metadataFileEntryPath(t, tableUUID, body.MetadataLocation)
		}
		f.commitCatalog(t)

		require.Equal(t, http.StatusNoContent, f.deleteTable(t, salesParam, "dropped", "carol", nil).Code)

		_, err := f.entryOn(t, tablesTestRef, created["dropped"])
		require.ErrorIs(t, err, graveler.ErrNotFound, "the dropped table's file entry is released")
		_, err = f.entryOn(t, tablesTestRef, created["kept"])
		require.NoError(t, err, "the other table's file entry is untouched")
		_, err = f.entryOn(t, tablesTestRef,
			mustPointerPath(t, tableAddressOf(sales, "kept")))
		require.NoError(t, err, "the other table's pointer is untouched")
	})

	t.Run("a pointer that carries no table uuid is not dropped halfway", func(t *testing.T) {
		// The drop reads the table uuid out of the pointer's own user metadata, because that uuid -
		// not metadata.json - names the directory the files are released from (dropTable). A pointer
		// this catalog could not have written cannot say which directory to release, and a drop that
		// removed only its pointer would leave exactly the leak the release exists to close. It is
		// therefore refused as the corrupt pointer it is, *before* the pointer is removed: the client
		// gets a 5xx and nothing is staged.
		f := newTablesFixture(t)
		addr := tableAddressOf(sales, "corrupt")
		pointerPath := mustPointerPath(t, addr)
		f.stageEntryOn(t, tablesTestRepo, tablesTestRef, pointerPath, map[string]string{
			MetadataLocationKey: f.storageNamespace + "/_iceberg/" + tableUUIDOrNone + "/metadata/00001.metadata.json",
			FormatVersionKey:    "2",
		})

		rec := f.deleteTable(t, salesParam, "corrupt", "carol", nil)
		requireEnvelope(t, rec, http.StatusInternalServerError, ErrTypeInternalServerError)

		_, err := f.entryOn(t, tablesTestRef, pointerPath)
		require.NoError(t, err, "a refused drop must not remove the pointer")
	})
}

// tableUUIDOrNone is the table uuid of the corrupt pointer above: a valid uuid, so that the only
// thing wrong with that pointer is the missing table-uuid field.
const tableUUIDOrNone = "88888888-8888-8888-8888-888888888888"

// mustPointerPath is the pointer key of addr, for a test that would otherwise repeat the error
// handling of tablePointerPath.
func mustPointerPath(t *testing.T, addr Address) string {
	t.Helper()
	pointerPath, err := tablePointerPath(addr)
	require.NoError(t, err, "build the pointer path of %s", addr.Table)
	return pointerPath
}

// removedPaths is the sorted paths a diff removes, and fails the test on any other kind of change:
// the drop's diffs are removals only, so an addition or a change is a defect of its own.
func removedPaths(t *testing.T, differences catalog.Differences) []string {
	t.Helper()
	removed := make([]string, 0, len(differences))
	for _, difference := range differences {
		require.Equal(t, catalog.DifferenceTypeRemoved, difference.Type,
			"the drop changes only removals, got %s", difference)
		removed = append(removed, difference.Path)
	}
	sort.Strings(removed)
	return removed
}
