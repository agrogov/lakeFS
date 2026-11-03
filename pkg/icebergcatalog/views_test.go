package icebergcatalog

// Tests for the view surface: the six operations of views_http.go and the state of views.go.
//
// The fixture is the real read and write path end to end, like the table tests: a catalog store
// (graveler + committed ranges) on an in-memory KV store, an in-memory block adapter, the
// simplified-ACL auth service the lakefs-sso binary uses, and the production middleware stack built
// by newRouter. Views are created through the store's own create path (write the document, stage its
// entry, stage the pointer), so the entries and the documents the endpoints read are the ones the
// create endpoint produces - and the create endpoint itself is exercised through the router.
//
// What each test names:
//
//   - TestViewsRead: ListViews, LoadView and ViewExists - listing levels, pagination, the
//     catalog:ViewName filter, any-ref reads (branch, staged, tag) and the error paths (§5, §7.2,
//     §2.1);
//   - TestCreateView: the staged create, the duplicate 409, the validation 400s, the missing
//     namespace 404, the Q-I4 read-only-ref 403 and the initial metadata entry (§6.3);
//   - TestReplaceView: the commit - assert-view-uuid (409 on a mismatch), set-properties,
//     add-view-version + set-current-view-version, set-location inside and outside the catalog's
//     area, upgrade-format-version 2 (400), and a lost compare-and-swap (409);
//   - TestDropView: the pointer and the metadata entries removed in one commit, the staged drop,
//     the 404 on a second drop;
//   - TestViewsNamespaceEmptiness: dropping a namespace that holds a view is 409
//     NamespaceNotEmptyException;
//   - TestViewsConfig: GET /v1/config advertises exactly the six operations, and the mount builder
//     mounts them;
//   - TestViewsRelativeTo: a view created, listed and loaded through the relative_to mount
//     round-trips in the client's own terms;
//   - TestViewsStore: the store-level facts the endpoints rest on - pointer encode/decode, the
//     metadata file sequence and the compare-and-swap.

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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/go-chi/chi/v5"
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
	// viewsTestRepo is the repository the view tree of this file lives in, viewsTestRef the
	// branch, and viewsTestTag a tag on the committed state, so "a view is readable on any ref"
	// is tested against a real tag.
	viewsTestRepo = authzRepo
	viewsTestRef  = "main"
	viewsTestTag  = "release"

	// viewsTestUUID is the UUID of the committed view orders_view.
	viewsTestUUID = "9c12d441-03fe-4693-9a96-a0705ddf69c1"

	// viewsTestSQL is the SQL of the committed view. It is not a real query over a real table -
	// nothing in the catalog interprets it - so its only job is to be recognisable in a response.
	viewsTestSQL = "SELECT id, name FROM inventory.books.orders"

	// viewsTestVersionedUUID is the UUID of the view the replace tests update a second time.
	viewsTestVersionedUUID = "7f5a0e4c-1f3e-4a51-9a3e-2b0c1d9e8f70"
)

// viewsFixtureSeq keeps fixture storage namespaces unique across the process, like
// tablesFixtureSeq does for the table tests.
var viewsFixtureSeq atomic.Int64

// viewsTestSchema is the schema of the fixture views: one required long column and one optional
// string, with schema-id 0 (the id the create path normalizes the initial schema to).
func viewsTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
}

// viewsTestVersion builds a view version with one SQL representation.
func viewsTestVersion(t *testing.T, id int64, sql string, defaultNamespace table.Identifier) *view.Version {
	t.Helper()
	version, err := view.NewVersion(id, 0, view.Representations{view.NewRepresentation(sql, "trino")}, defaultNamespace)
	require.NoError(t, err, "build view version %d", id)
	return version
}

type viewsFixture struct {
	*storeFixture
	authz  *authzFixture
	serve  *viewService
	router http.Handler
	// mux is the production mount: the catalog router at APIBasePath and the relative_to form at
	// RelativeToBasePath (the two mounts RoutesBuilder builds), so a test can drive the
	// relative_to addressing form end to end.
	mux http.Handler

	// storageNamespace is the storage namespace of viewsTestRepo, i.e. the prefix of every
	// metadata location this file asserts on.
	storageNamespace string
}

// viewsFixtureStatements grants the view and namespace halves of §7.2 (reads and writes) on every
// catalog ARN, plus fs:ReadRepository on the fixture's repository and fs:WriteObject on the
// reserved area every catalog write needs.
func viewsFixtureStatements() model.Statements {
	return append(authzWriteStatements(),
		model.Statement{
			Effect: model.StatementEffectAllow,
			Action: []string{
				permissions.CatalogListNamespacesAction,
				permissions.CatalogGetNamespaceAction,
			},
			Resource: "arn:lakefs:catalog:::*",
		},
		model.Statement{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.WriteObjectAction},
			Resource: authzWriteObjectArn,
		},
	)
}

// newViewsFixture builds the fixture and materializes a small view tree on main:
//
//	inventory/books          orders_view   (committed, viewsTestUUID)
//	                         stale_view    (committed, viewsTestVersionedUUID)
//	inventory/books/archive  backfill_view (a nested namespace's view: not listed under books)
//	inventory                summary_view  (committed)
//	sales                    (marker only)
//
// main is then committed, release tags that commit, and staged_view is created after the commit so
// that "a staged create is already readable" is tested against a state that is genuinely uncommitted.
func newViewsFixture(t *testing.T) *viewsFixture {
	t.Helper()
	base := newStoreFixture(t)
	ctx := base.ctx

	storageNamespace := fmt.Sprintf("mem://iceberg-views-%d-ns", viewsFixtureSeq.Add(1))
	_, err := base.catalog.CreateRepository(ctx, viewsTestRepo, "", storageNamespace, viewsTestRef, false)
	require.NoError(t, err, "create catalog repository")

	authz := newAuthzUser(t, viewsFixtureStatements())
	// carol holds the write half of §7.2 (viewsFixtureStatements is authzWriteStatements plus the
	// namespace reads), bob holds nothing: every refusal below is asserted against one of them.
	authz.user(t, "carol")
	authz.attachStatements(t, "views-writer", "carol", viewsFixtureStatements())
	authz.user(t, "bob")

	store := NewStore(base.catalog, base.catalog.BlockAdapter)
	cfg := &Config{Prefix: CatalogPrefix}
	service := NewViewService(store, authz.authz, base.catalog, cfg)
	namespaceService := NewNamespaceService(store, authz.authz, base.catalog, cfg)
	router, err := newRouter(viewsUser, cfg,
		catalogServerWith(cfg, append(namespaceService.Routes(), service.Routes()...)...))
	require.NoError(t, err, "build catalog router")

	// The two production mounts, built the way RoutesBuilder builds them: the catalog at
	// APIBasePath, and the warehouse form at RelativeToBasePath/{warehouse}/api. Both wrap the same
	// router, so the relative_to form reaches the very handlers the direct tests drive.
	mux := chi.NewRouter()
	mux.Mount(APIBasePath, RelativeTo(router))
	relativeTo := chi.NewRouter()
	relativeTo.NotFound(func(w http.ResponseWriter, req *http.Request) {
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
			fmt.Sprintf("%s %s does not name a warehouse", req.Method, req.URL.Path))
	})
	relativeTo.Mount(relativeToPattern, RelativeTo(router))
	mux.Mount(RelativeToBasePath, relativeTo)

	f := &viewsFixture{
		storeFixture:     base,
		authz:            authz,
		serve:            service,
		router:           router,
		mux:              mux,
		storageNamespace: storageNamespace,
	}

	for _, namespace := range []Namespace{
		{"inventory"},
		{"inventory", "books"},
		{"inventory", "books", "archive"},
		{"sales"},
	} {
		f.createNamespace(t, namespace)
	}

	f.createViewWithDocument(t, Namespace{"inventory", "books"}, "orders_view", viewsTestUUID, viewsTestSQL)
	f.createViewWithDocument(t, Namespace{"inventory", "books"}, "stale_view", viewsTestVersionedUUID,
		"SELECT id FROM inventory.books.orders")
	f.createViewWithDocument(t, Namespace{"inventory", "books", "archive"}, "backfill_view",
		"22222222-2222-2222-2222-222222222222", "SELECT id FROM inventory.books.archive.backfill")
	f.createViewWithDocument(t, Namespace{"inventory"}, "summary_view",
		"33333333-3333-3333-3333-333333333333", "SELECT count(*) FROM inventory.books.orders")

	ref := f.commitCatalog(t)
	_, err = base.catalog.CreateTag(ctx, viewsTestRepo, viewsTestTag, ref)
	require.NoError(t, err, "create tag %s", viewsTestTag)

	// After the commit: a staged create, i.e. a view that exists on the branch and in no commit.
	f.createViewWithDocument(t, Namespace{"inventory", "books"}, "staged_view",
		"44444444-4444-4444-4444-444444444444", "SELECT name FROM inventory.books.orders")
	return f
}

// viewsUser is the authentication stand-in the router is built with.
func viewsUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username := r.Header.Get("X-Test-User")
		if username == "" {
			username = "alice"
		}
		next.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), &model.User{Username: username})))
	})
}

// createNamespace stages a namespace marker with no properties on main.
func (f *viewsFixture) createNamespace(t *testing.T, namespace Namespace) {
	t.Helper()
	require.NoError(t, f.store.CreateNamespace(f.ctx, Address{
		Repo: viewsTestRepo, Ref: viewsTestRef, Namespace: namespace,
	}, nil), "create namespace %s", namespace.Dotted())
}

// createViewWithDocument writes a view metadata document at a fresh metadata key and stages its
// entry and the pointer through the store's own create path. It returns the pointer.
func (f *viewsFixture) createViewWithDocument(t *testing.T, namespace Namespace, name, viewUUID, sql string) ViewPointer {
	t.Helper()
	location := strings.TrimRight(f.storageNamespace, "/") + "/" + DataPrefix + "/" + viewUUID
	metadata, err := view.NewMetadataWithUUID(
		viewsTestVersion(t, view.InitialVersionID, sql, table.Identifier(namespace)),
		viewsTestSchema(), location, iceberg.Properties{"owner": "data-team"}, uuid.MustParse(viewUUID))
	require.NoError(t, err, "build view metadata for %s", name)
	metadataLocation, err := NewViewMetadataFileLocation(location, "", metadata)
	require.NoError(t, err, "build the metadata location of %s", name)
	document, err := f.store.WriteViewMetadata(f.ctx, viewsTestRepo, metadataLocation, metadata)
	require.NoError(t, err, "write the metadata of %s", name)

	pointer := ViewPointer{
		MetadataLocation: metadataLocation,
		ViewUUID:         viewUUID,
		FormatVersion:    SupportedViewFormatVersion,
	}
	require.NoError(t, f.store.CreateViewWithMetadata(f.ctx, Address{
		Repo: viewsTestRepo, Ref: viewsTestRef, Namespace: namespace, View: name,
	}, pointer, location, int64(len(document))), "create view %s.%s", namespace.Dotted(), name)
	return pointer
}

// commitCatalog commits main and returns the new commit ID.
func (f *viewsFixture) commitCatalog(t *testing.T) string {
	t.Helper()
	log, err := f.catalog.Commit(f.ctx, viewsTestRepo, viewsTestRef, "iceberg views", "views-test",
		nil, nil, nil, false)
	require.NoError(t, err, "commit catalog state")
	return log.Reference
}

// request sends one catalog request as user.
func (f *viewsFixture) request(t *testing.T, method, target, user string) *httptest.ResponseRecorder {
	t.Helper()
	return f.requestBody(t, method, target, user, "")
}

// requestBody is request with a JSON body.
func (f *viewsFixture) requestBody(t *testing.T, method, target, user, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	req.Header.Set("X-Test-User", user)
	if body != "" {
		req.Header.Set("Content-Type", contentTypeJSON)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// viewsListTarget is the ListViews URL of one namespace address.
func viewsListTarget(namespaceParam string, query url.Values) string {
	target := "/v1/" + CatalogPrefix + "/namespaces/" + namespaceParam + "/views"
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	return target
}

// viewsLoadTarget is the load/head/replace/drop URL of one view.
func viewsLoadTarget(namespaceParam, name string) string {
	return "/v1/" + CatalogPrefix + "/namespaces/" + namespaceParam + "/views/" + url.PathEscape(name)
}

// decodeLoadView asserts a successful load and decodes it.
func decodeLoadView(t *testing.T, rec *httptest.ResponseRecorder) loadViewResult {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var body loadViewResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "body: %s", rec.Body.String())
	require.NotEmpty(t, body.Metadata, "the metadata document is required by LoadViewResult")
	return body
}

// decodeListViewTables asserts a successful listing and decodes it.
func decodeListViews(t *testing.T, rec *httptest.ResponseRecorder) listViewsResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response listViewsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return response
}

// viewNamesOf returns the names of a listing, in order.
func viewNamesOf(response listViewsResponse) []string {
	names := make([]string, 0, len(response.Identifiers))
	for _, identifier := range response.Identifiers {
		names = append(names, identifier.Name)
	}
	return names
}

// viewMetadataField reads one string field of a metadata document, so a test can assert on it
// without modelling the whole view document.
func viewMetadataField(t *testing.T, metadata json.RawMessage, field string) string {
	t.Helper()
	var value string
	require.NoError(t, json.Unmarshal(rawMetadataField(t, metadata, field), &value),
		"field %q: %s", field, string(metadata))
	return value
}

// viewMetadataNumber reads one integer field of a metadata document.
func viewMetadataNumber(t *testing.T, metadata json.RawMessage, field string) int64 {
	t.Helper()
	var value int64
	require.NoError(t, json.Unmarshal(rawMetadataField(t, metadata, field), &value),
		"field %q: %s", field, string(metadata))
	return value
}

// rawMetadataField returns one field of a metadata document verbatim.
func rawMetadataField(t *testing.T, metadata json.RawMessage, field string) json.RawMessage {
	t.Helper()
	var document map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadata, &document), "metadata: %s", string(metadata))
	raw, ok := document[field]
	require.Truef(t, ok, "the metadata document carries no %q field: %s", field, string(metadata))
	return raw
}

// viewProperty reads one entry of the metadata document's properties map.
func viewProperty(t *testing.T, metadata json.RawMessage, key string) string {
	t.Helper()
	var document map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadata, &document), "metadata: %s", string(metadata))
	var properties map[string]string
	require.NoError(t, json.Unmarshal(document["properties"], &properties), "metadata: %s", string(metadata))
	return properties[key]
}

// TestViewsRead covers list/load/head: the listing levels, pagination, the catalog:ViewName filter,
// reads on a branch, of a staged create and on a tag, and the error paths.
func TestViewsRead(t *testing.T) {
	f := newViewsFixture(t)
	books := nodes(viewsTestRepo, viewsTestRef, "inventory", "books")

	t.Run("the listing reports the views of exactly one namespace level", func(t *testing.T) {
		response := decodeListViews(t, f.request(t, http.MethodGet, viewsListTarget(books, nil), "alice"))
		require.Equal(t, []string{"orders_view", "staged_view", "stale_view"}, viewNamesOf(response),
			"the listing is sorted by path and carries exactly one namespace level: "+
				"archive.backfill_view is a nested namespace's view and is not listed here")
		for _, identifier := range response.Identifiers {
			require.Equal(t, []string{viewsTestRepo, viewsTestRef, "inventory", "books"},
				[]string(identifier.Namespace),
				"an identifier carries the full lakeFS address, so a client can load it back")
		}
	})

	t.Run("the listing of the parent namespace does not include the child's views", func(t *testing.T) {
		response := decodeListViews(t, f.request(t, http.MethodGet,
			viewsListTarget(nodes(viewsTestRepo, viewsTestRef, "inventory"), nil), "alice"))
		require.Equal(t, []string{"summary_view"}, viewNamesOf(response))
	})

	t.Run("the nested namespace lists its own view", func(t *testing.T) {
		response := decodeListViews(t, f.request(t, http.MethodGet,
			viewsListTarget(nodes(viewsTestRepo, viewsTestRef, "inventory", "books", "archive"), nil), "alice"))
		require.Equal(t, []string{"backfill_view"}, viewNamesOf(response))
	})

	t.Run("a namespace with no views is an empty listing, not a 404", func(t *testing.T) {
		response := decodeListViews(t, f.request(t, http.MethodGet,
			viewsListTarget(nodes(viewsTestRepo, viewsTestRef, "sales"), nil), "alice"))
		require.Empty(t, viewNamesOf(response))
	})

	t.Run("a namespace that does not exist is 404 NoSuchNamespaceException", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			viewsListTarget(nodes(viewsTestRepo, viewsTestRef, "missing"), nil), "alice")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("pageSize and pageToken page the listing", func(t *testing.T) {
		first := decodeListViews(t, f.request(t, http.MethodGet,
			viewsListTarget(books, url.Values{"pageSize": {"2"}}), "alice"))
		require.Equal(t, []string{"orders_view", "staged_view"}, viewNamesOf(first))
		require.NotEmpty(t, first.NextPageToken, "a truncated listing carries a next-page-token")

		second := decodeListViews(t, f.request(t, http.MethodGet,
			viewsListTarget(books, url.Values{"pageSize": {"2"}, "pageToken": {first.NextPageToken}}), "alice"))
		require.Equal(t, []string{"stale_view"}, viewNamesOf(second))
		require.Empty(t, second.NextPageToken)
	})

	t.Run("a malformed page token is 400", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, viewsListTarget(books, url.Values{"pageToken": {"nope"}}), "alice")
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("the catalog:ViewName condition filters the listing", func(t *testing.T) {
		// dana may list only views whose name starts with "stale-", which is the per-view filter of
		// §7.2 evaluated by FilterViews.
		f.authz.user(t, "dana")
		f.authz.attachStatements(t, "stale-only", "dana", model.Statements{
			{
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.CatalogListViewsAction},
				Resource: permissions.CatalogNamespaceArn(viewsTestRepo, "inventory.books"),
				Condition: map[string]map[string][]string{
					"StringLike": {ConditionFieldViewName: {"stale*"}},
				},
			},
			{
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.ReadRepositoryAction},
				Resource: permissions.RepoArn(viewsTestRepo),
			},
		})
		response := decodeListViews(t, f.request(t, http.MethodGet, viewsListTarget(books, nil), "dana"))
		require.Equal(t, []string{"stale_view"}, viewNamesOf(response))
	})

	t.Run("a caller without the grant is refused rather than shown an empty page", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, viewsListTarget(books, nil), "bob")
		requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("the listing does not leak tables into the views listing", func(t *testing.T) {
		// Nothing in this fixture creates a table, so the assertion is about the directory the
		// listing reads: a table pointer under tables/ is not a view.
		f.stageEntry(t, ReservedPrefix+"/"+TablesDirectory+"/inventory/books/orders/.pointer",
			map[string]string{MetadataLocationKey: "mem://nowhere", TableUUIDKey: viewsTestUUID, FormatVersionKey: "2"})
		response := decodeListViews(t, f.request(t, http.MethodGet, viewsListTarget(books, nil), "alice"))
		require.NotContains(t, viewNamesOf(response), "orders")
	})

	t.Run("load returns the metadata location and the document", func(t *testing.T) {
		want := f.viewPointer(t, Namespace{"inventory", "books"}, "orders_view")
		body := decodeLoadView(t, f.request(t, http.MethodGet,
			viewsLoadTarget(books, "orders_view"), "alice"))
		require.Equal(t, want.MetadataLocation, body.MetadataLocation,
			"the load serves the physical URI the pointer names, not a lakeFS path")
		require.Equal(t, viewsTestUUID, viewMetadataField(t, body.Metadata, "view-uuid"))
		require.Equal(t, "data-team", viewProperty(t, body.Metadata, "owner"))
		require.Equal(t, map[string]string{}, body.Config,
			"config is an empty map in v1, not null: credential vending is phase I-4")
	})

	t.Run("a staged create is readable before its commit", func(t *testing.T) {
		body := decodeLoadView(t, f.request(t, http.MethodGet,
			viewsLoadTarget(books, "staged_view"), "alice"))
		require.Equal(t, "44444444-4444-4444-4444-444444444444",
			viewMetadataField(t, body.Metadata, "view-uuid"))
	})

	t.Run("a view is readable on a tag", func(t *testing.T) {
		tagBooks := nodes(viewsTestRepo, viewsTestTag, "inventory", "books")
		// The tag was cut before staged_view existed on the branch, so it must not be listed there.
		response := decodeListViews(t, f.request(t, http.MethodGet, viewsListTarget(tagBooks, nil), "alice"))
		require.Equal(t, []string{"orders_view", "stale_view"}, viewNamesOf(response),
			"a tag reads the state it captured, not the branch's later staged create")
		require.Equal(t, http.StatusOK,
			f.request(t, http.MethodGet, viewsLoadTarget(tagBooks, "orders_view"), "alice").Code)
		requireEnvelope(t, f.request(t, http.MethodGet, viewsLoadTarget(tagBooks, "staged_view"), "alice"),
			http.StatusNotFound, ErrTypeNoSuchView)
	})

	t.Run("a missing view is 404 NoSuchViewException", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, viewsLoadTarget(books, "missing_view"), "alice")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchView)
	})

	t.Run("head is 204 for a view and 404 for a missing one", func(t *testing.T) {
		rec := f.request(t, http.MethodHead, viewsLoadTarget(books, "orders_view"), "alice")
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String())

		requireEnvelope(t, f.request(t, http.MethodHead, viewsLoadTarget(books, "missing_view"), "alice"),
			http.StatusNotFound, ErrTypeNoSuchView)
	})

	t.Run("a caller without the grant cannot load or head the view", func(t *testing.T) {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			rec := f.request(t, method, viewsLoadTarget(books, "orders_view"), "bob")
			requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
		}
	})

	t.Run("a wrong prefix is 404 NoSuchWarehouseException", func(t *testing.T) {
		target := "/v1/notlakefs/namespaces/" + books + "/views/orders_view"
		requireEnvelope(t, f.request(t, http.MethodGet, target, "alice"),
			http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("a namespace-address listing without a namespace level is 400", func(t *testing.T) {
		// "repo<sep>ref" is the repository.ref level of the namespace listing, not a view listing
		// address: a view always lives in a namespace.
		rec := f.request(t, http.MethodGet,
			viewsListTarget(nodes(viewsTestRepo, viewsTestRef), nil), "alice")
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})
}

// TestCreateView covers the create: the staged pointer and document entry, the duplicate refusal,
// the validation errors, the namespace requirement and the read-only-ref rule of Q-I4.
func TestCreateView(t *testing.T) {
	f := newViewsFixture(t)
	books := nodes(viewsTestRepo, viewsTestRef, "inventory", "books")

	t.Run("a created view is listed, loadable and staged with its metadata entry", func(t *testing.T) {
		rec := f.requestBody(t, http.MethodPost, viewsListTarget(books, nil), "carol",
			viewsCreateBody(t, "new_view", "SELECT 1 AS id"))
		body := decodeLoadView(t, rec)
		require.True(t, strings.HasPrefix(body.MetadataLocation,
			strings.TrimRight(f.storageNamespace, "/")+"/"+DataPrefix+"/"),
			"a view without a requested location lands in the managed table area: %q", body.MetadataLocation)
		viewUUID := viewMetadataField(t, body.Metadata, "view-uuid")

		// The pointer is staged with the uuid the document carries, and the document is staged as
		// its own lakeFS entry so the committed GC can see it (§6.3).
		pointer := f.viewPointer(t, Namespace{"inventory", "books"}, "new_view")
		require.Equal(t, viewUUID, pointer.ViewUUID)
		require.Equal(t, body.MetadataLocation, pointer.MetadataLocation)
		require.Equal(t, SupportedViewFormatVersion, pointer.FormatVersion)

		entries := f.listEntries(t, ReservedPrefix+"/"+FilesDirectory+"/"+viewUUID+"/")
		require.Len(t, entries, 1, "the initial metadata document is one entry: %v", entryPaths(entries))
		require.Equal(t, body.MetadataLocation, entries[0].PhysicalAddress,
			"the entry addresses the object the catalog wrote")

		response := decodeListViews(t, f.request(t, http.MethodGet, viewsListTarget(books, nil), "alice"))
		require.Contains(t, viewNamesOf(response), "new_view")
	})

	t.Run("a view may share a name with a table", func(t *testing.T) {
		// The two live in different directories of the reserved area and are addressed by different
		// endpoints, so neither can shadow the other.
		f.stageEntry(t, ReservedPrefix+"/"+TablesDirectory+"/inventory/books/orders_view/.pointer",
			map[string]string{
				MetadataLocationKey: "mem://nowhere/1.metadata.json",
				TableUUIDKey:        viewsTestUUID,
				FormatVersionKey:    "2",
			})
		require.Equal(t, http.StatusOK,
			f.request(t, http.MethodGet, viewsLoadTarget(books, "orders_view"), "alice").Code,
			"the view is unaffected by a table of the same name")
	})

	t.Run("a duplicate create is 409 AlreadyExistsException", func(t *testing.T) {
		rec := f.requestBody(t, http.MethodPost, viewsListTarget(books, nil), "carol",
			viewsCreateBody(t, "orders_view", "SELECT 2"))
		requireEnvelope(t, rec, http.StatusConflict, ErrTypeAlreadyExists)
	})

	t.Run("a create in a namespace that does not exist is 404 NoSuchNamespaceException", func(t *testing.T) {
		rec := f.requestBody(t, http.MethodPost,
			viewsListTarget(nodes(viewsTestRepo, viewsTestRef, "missing"), nil), "carol",
			viewsCreateBody(t, "new_view", "SELECT 1"))
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("a body without a schema is 400", func(t *testing.T) {
		body, err := json.Marshal(createViewRequest{
			Name:        "no_schema",
			ViewVersion: viewsTestVersion(t, 1, "SELECT 1", table.Identifier{"inventory", "books"}),
		})
		require.NoError(t, err)
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsListTarget(books, nil), "carol", string(body)),
			http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("a body without a view version is 400", func(t *testing.T) {
		body, err := json.Marshal(createViewRequest{Name: "no_version", Schema: viewsTestSchema()})
		require.NoError(t, err)
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsListTarget(books, nil), "carol", string(body)),
			http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("a view version with no representation is 400", func(t *testing.T) {
		body := fmt.Sprintf(`{"name":"no_repr","schema":%s,"view-version":{"version-id":1,"schema-id":0,
			"timestamp-ms":1600000000000,"summary":{"engine-name":"test"},"representations":[],
			"default-namespace":["inventory","books"]}}`, viewsSchemaJSON)
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsListTarget(books, nil), "carol", body),
			http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("a malformed body is 400", func(t *testing.T) {
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsListTarget(books, nil), "carol", "{"),
			http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("a create on a tag is 403 ref is read-only", func(t *testing.T) {
		tagBooks := nodes(viewsTestRepo, viewsTestTag, "inventory", "books")
		rec := f.requestBody(t, http.MethodPost, viewsListTarget(tagBooks, nil), "carol",
			viewsCreateBody(t, "tag_view", "SELECT 1"))
		apiError := requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
		require.Equal(t, "ref is read-only", apiError.Message)
	})

	t.Run("a caller without the write grant cannot create", func(t *testing.T) {
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsListTarget(books, nil), "bob",
			viewsCreateBody(t, "unauthorized_view", "SELECT 1")),
			http.StatusForbidden, ErrTypeForbidden)
	})
}

// TestReplaceView covers the commit: requirements, the eight update actions the task names, the
// format-version gate, the location check and the compare-and-swap.
func TestReplaceView(t *testing.T) {
	f := newViewsFixture(t)
	books := nodes(viewsTestRepo, viewsTestRef, "inventory", "books")
	ordersView := f.viewPointer(t, Namespace{"inventory", "books"}, "orders_view")

	t.Run("assert-view-uuid holds and a properties update is applied", func(t *testing.T) {
		body := fmt.Sprintf(`{"requirements":[{"type":"assert-view-uuid","uuid":%q}],
			"updates":[{"action":"set-properties","updates":{"stage":"replaced"}}]}`, viewsTestUUID)
		parent := f.viewPointer(t, Namespace{"inventory", "books"}, "orders_view")
		rec := f.requestBody(t, http.MethodPost, viewsLoadTarget(books, "orders_view"), "carol", body)
		loaded := decodeLoadView(t, rec)

		require.Equal(t, viewsTestUUID, viewMetadataField(t, loaded.Metadata, "view-uuid"),
			"the view uuid survives a replace")
		require.Equal(t, "replaced", viewProperty(t, loaded.Metadata, "stage"))
		require.Equal(t, "data-team", viewProperty(t, loaded.Metadata, "owner"),
			"set-properties merges: an untouched property stays")
		require.NotEqual(t, parent.MetadataLocation, loaded.MetadataLocation,
			"a replace writes a new metadata document")

		// The new document is a lakeFS entry as well, so the committed GC protects it.
		entries := f.listEntries(t, ReservedPrefix+"/"+FilesDirectory+"/"+viewsTestUUID+"/")
		addresses := make([]string, 0, len(entries))
		for _, entry := range entries {
			addresses = append(addresses, entry.PhysicalAddress)
		}
		require.Contains(t, addresses, loaded.MetadataLocation,
			"the replaced document must be staged: %v", addresses)
		require.Contains(t, addresses, parent.MetadataLocation,
			"the document it replaced stays where it was: metadata files are never deleted by a commit")
	})

	t.Run("assert-view-uuid that does not hold is 409 CommitFailedException", func(t *testing.T) {
		body := `{"requirements":[{"type":"assert-view-uuid","uuid":"11111111-1111-1111-1111-111111111111"}],
			"updates":[{"action":"set-properties","updates":{"stage":"never"}}]}`
		rec := f.requestBody(t, http.MethodPost, viewsLoadTarget(books, "orders_view"), "carol", body)
		requireEnvelope(t, rec, http.StatusConflict, ErrTypeCommitFailed)
	})

	t.Run("an unknown requirement type is 400", func(t *testing.T) {
		body := `{"requirements":[{"type":"assert-table-uuid","uuid":"11111111-1111-1111-1111-111111111111"}],
			"updates":[]}`
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsLoadTarget(books, "orders_view"), "carol", body),
			http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("add-view-version and set-current-view-version replace the current version", func(t *testing.T) {
		version := viewsTestVersion(t, 2, "SELECT id, upper(name) FROM inventory.books.orders",
			table.Identifier{"inventory", "books"})
		// The task's update language names the field "view-version" for add-view-version and
		// "view-version-id" for set-current-view-version (rest-catalog-open-api.yaml,
		// components.schemas.AddViewVersionUpdate / SetCurrentViewVersionUpdate).
		body := fmt.Sprintf(`{"requirements":[{"type":"assert-view-uuid","uuid":%q}],
			"updates":[{"action":"add-view-version","view-version":%s},
			           {"action":"set-current-view-version","view-version-id":2}]}`,
			viewsTestUUID, viewsVersionJSON(t, version))
		loaded := decodeLoadView(t, f.requestBody(t, http.MethodPost,
			viewsLoadTarget(books, "orders_view"), "carol", body))
		require.Equal(t, int64(2), viewMetadataNumber(t, loaded.Metadata, "current-version-id"))

		var document struct {
			Versions []struct {
				VersionID       int64 `json:"version-id"`
				Representations []struct {
					SQL string `json:"sql"`
				} `json:"representations"`
			} `json:"versions"`
		}
		require.NoError(t, json.Unmarshal(loaded.Metadata, &document))
		require.Len(t, document.Versions, 2, "the new version is added beside the existing one")
		require.Contains(t, document.Versions[1].Representations[0].SQL, "upper(name)")
	})

	t.Run("set-location inside the catalog's area is applied", func(t *testing.T) {
		target := strings.TrimRight(f.storageNamespace, "/") + "/" + DataPrefix + "/moved-orders-view"
		body := fmt.Sprintf(`{"updates":[{"action":"set-location","location":%q},
			{"action":"set-properties","updates":{"moved":"yes"}}]}`, target)
		loaded := decodeLoadView(t, f.requestBody(t, http.MethodPost,
			viewsLoadTarget(books, "orders_view"), "carol", body))
		require.Equal(t, target, viewMetadataField(t, loaded.Metadata, "location"))
		require.True(t, strings.HasPrefix(loaded.MetadataLocation, target+"/"+MetadataDirectory+"/"),
			"the next document is written under the new location: %q", loaded.MetadataLocation)
		require.Equal(t, "yes", viewProperty(t, loaded.Metadata, "moved"))
	})

	t.Run("set-location outside the catalog's area is 400", func(t *testing.T) {
		body := `{"updates":[{"action":"set-location","location":"s3://another-bucket/warehouse/view"}]}`
		blog := requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsLoadTarget(books, "stale_view"), "carol", body),
			http.StatusBadRequest, ErrTypeBadRequest)
		require.Contains(t, blog.Message, "table area",
			"the refusal names the area the catalog owns")
	})

	t.Run("upgrade-format-version to an unsupported version is 400", func(t *testing.T) {
		body := `{"updates":[{"action":"upgrade-format-version","format-version":2}]}`
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsLoadTarget(books, "stale_view"), "carol", body),
			http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("an unknown update action is 400", func(t *testing.T) {
		body := `{"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":1}}]}`
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsLoadTarget(books, "stale_view"), "carol", body),
			http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("an empty commit is answered with the current view", func(t *testing.T) {
		// No requirements and no updates: the view is unchanged, and the response is the
		// LoadViewResult a client expects, not an error.
		loaded := decodeLoadView(t, f.requestBody(t, http.MethodPost,
			viewsLoadTarget(books, "stale_view"), "carol", `{"updates":[]}`))
		require.Equal(t, viewsTestVersionedUUID, viewMetadataField(t, loaded.Metadata, "view-uuid"))
	})

	t.Run("a replace of a missing view is 404 NoSuchViewException", func(t *testing.T) {
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsLoadTarget(books, "missing_view"), "carol",
			`{"updates":[{"action":"set-properties","updates":{"a":"b"}}]}`),
			http.StatusNotFound, ErrTypeNoSuchView)
	})

	t.Run("a replace on a tag is 403 ref is read-only", func(t *testing.T) {
		tagBooks := nodes(viewsTestRepo, viewsTestTag, "inventory", "books")
		apiError := requireEnvelope(t, f.requestBody(t, http.MethodPost,
			viewsLoadTarget(tagBooks, "orders_view"), "carol",
			`{"updates":[{"action":"set-properties","updates":{"a":"b"}}]}`),
			http.StatusForbidden, ErrTypeForbidden)
		require.Equal(t, "ref is read-only", apiError.Message)
	})

	t.Run("a caller without catalog:UpdateView cannot replace", func(t *testing.T) {
		requireEnvelope(t, f.requestBody(t, http.MethodPost, viewsLoadTarget(books, "orders_view"), "bob",
			`{"updates":[]}`), http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("a lost compare-and-swap is 409 CommitFailedException", func(t *testing.T) {
		// A competing writer moves the pointer between the base read and the write: the condition
		// on the base metadata-location then fails inside graveler, and the client is told to
		// refresh and retry rather than being handed a silent overwrite.
		f2 := newViewsFixture(t)
		competing := newRacingViewPointerCatalog(f2.catalog, func() error {
			addr := Address{Repo: viewsTestRepo, Ref: viewsTestRef, Namespace: Namespace{"inventory", "books"}, View: "orders_view"}
			pointerPath, err := viewPointerPath(addr)
			if err != nil {
				return err
			}
			winner := ViewPointer{
				MetadataLocation: strings.TrimRight(f2.storageNamespace, "/") + "/_iceberg/winner/metadata/00001.metadata.json",
				ViewUUID:         viewsTestUUID,
				FormatVersion:    SupportedViewFormatVersion,
			}
			metadata, err := EncodeViewPointer(winner)
			if err != nil {
				return err
			}
			return f2.catalog.CreateEntry(f2.ctx, viewsTestRepo, viewsTestRef, catalog.DBEntry{
				Path:            pointerPath,
				PhysicalAddress: pointerPath,
				AddressType:     catalog.AddressTypeRelative,
				Metadata:        metadata,
			})
		})
		store := NewStore(competing, f2.catalog.BlockAdapter)
		service := NewViewService(store, f2.authz.authz, competing, &Config{Prefix: CatalogPrefix})
		router, err := newRouter(viewsUser, &Config{Prefix: CatalogPrefix}, catalogServerWith(&Config{Prefix: CatalogPrefix}, service.Routes()...))
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost,
			viewsLoadTarget(books, "orders_view"),
			strings.NewReader(`{"updates":[{"action":"set-properties","updates":{"a":"b"}}]}`))
		req.Header.Set("X-Test-User", "carol")
		req.Header.Set("Content-Type", contentTypeJSON)
		router.ServeHTTP(rec, req)
		requireEnvelope(t, rec, http.StatusConflict, ErrTypeCommitFailed)
	})

	t.Run("the untouched view still reads", func(t *testing.T) {
		require.Equal(t, ordersView.ViewUUID,
			f.viewPointer(t, Namespace{"inventory", "books"}, "orders_view").ViewUUID)
	})
}

// TestDropView covers the drop: the pointer and the staged metadata entries are removed together,
// the drop is visible before its lakeFS commit, and a second drop is 404.
func TestDropView(t *testing.T) {
	f := newViewsFixture(t)
	books := nodes(viewsTestRepo, viewsTestRef, "inventory", "books")
	pointer := f.viewPointer(t, Namespace{"inventory", "books"}, "orders_view")

	t.Run("a drop removes the pointer and stages the removal of the metadata entries", func(t *testing.T) {
		entriesBefore := f.listEntries(t, ReservedPrefix+"/"+FilesDirectory+"/"+viewsTestUUID+"/")
		require.NotEmpty(t, entriesBefore, "the view has staged metadata entries before the drop")

		rec := f.request(t, http.MethodDelete, viewsLoadTarget(books, "orders_view"), "carol")
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String())

		// The pointer is gone: the view stops loading, stops being listed and stops existing.
		requireEnvelope(t, f.request(t, http.MethodGet, viewsLoadTarget(books, "orders_view"), "alice"),
			http.StatusNotFound, ErrTypeNoSuchView)
		require.NotContains(t,
			viewNamesOf(decodeListViews(t, f.request(t, http.MethodGet, viewsListTarget(books, nil), "alice"))),
			"orders_view")

		// The entries that referenced the documents are gone from the branch as well, which is what
		// hands the objects to the committed GC (§6.3).
		require.Empty(t, f.listEntries(t, ReservedPrefix+"/"+FilesDirectory+"/"+viewsTestUUID+"/"),
			"a drop releases the view's staged metadata entries")

		// The document itself is not deleted: v1 never purges (§14 Q-I8).
		var document []byte
		require.NoError(t, f.readObject(f.ctx, pointer.MetadataLocation, &document),
			"a drop deletes no object: the metadata document is still in the bucket")
	})

	t.Run("a second drop is 404 NoSuchViewException", func(t *testing.T) {
		requireEnvelope(t, f.request(t, http.MethodDelete, viewsLoadTarget(books, "orders_view"), "carol"),
			http.StatusNotFound, ErrTypeNoSuchView)
	})

	t.Run("a drop on a tag is 403 ref is read-only", func(t *testing.T) {
		f2 := newViewsFixture(t)
		tagBooks := nodes(viewsTestRepo, viewsTestTag, "inventory", "books")
		apiError := requireEnvelope(t, f2.request(t, http.MethodDelete,
			viewsLoadTarget(tagBooks, "orders_view"), "carol"), http.StatusForbidden, ErrTypeForbidden)
		require.Equal(t, "ref is read-only", apiError.Message)
	})

	t.Run("a caller without catalog:DeleteView cannot drop", func(t *testing.T) {
		f2 := newViewsFixture(t)
		requireEnvelope(t, f2.request(t, http.MethodDelete,
			viewsLoadTarget(books, "orders_view"), "bob"), http.StatusForbidden, ErrTypeForbidden)
		require.Equal(t, http.StatusOK,
			f2.request(t, http.MethodGet, viewsLoadTarget(books, "orders_view"), "alice").Code,
			"the refused drop must not have removed the view")
	})
}

// TestViewsNamespaceEmptiness: a namespace that holds a view is not empty, so dropping it is 409
// NamespaceNotEmptyException rather than a drop that orphans the view's pointer.
func TestViewsNamespaceEmptiness(t *testing.T) {
	f := newViewsFixture(t)
	books := nodes(viewsTestRepo, viewsTestRef, "inventory", "books")

	required := append(viewsFixtureStatements(), model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.CatalogDeleteNamespaceAction},
		Resource: "arn:lakefs:catalog:::*",
	})
	f.authz.attachStatements(t, "namespace-deleter", "carol", required)

	rec := f.request(t, http.MethodDelete, "/v1/"+CatalogPrefix+"/namespaces/"+books, "carol")
	requireEnvelope(t, rec, http.StatusConflict, ErrTypeNamespaceNotEmpty)

	// The view is still there: a refused drop of the namespace must not have removed anything.
	require.Equal(t, http.StatusOK,
		f.request(t, http.MethodGet, viewsLoadTarget(books, "orders_view"), "alice").Code)

	// A namespace with no view, no table and no child namespace is still droppable.
	require.Equal(t, http.StatusNoContent,
		f.request(t, http.MethodDelete, "/v1/"+CatalogPrefix+"/namespaces/"+
			nodes(viewsTestRepo, viewsTestRef, "sales"), "carol").Code,
		"a namespace with no views and no tables is still droppable")
}

// TestViewsConfig: the six operations are advertised, and the production mount mounts them.
func TestViewsConfig(t *testing.T) {
	f := newViewsFixture(t)

	rec := f.request(t, http.MethodGet, configPath, "alice")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var config CatalogConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &config))

	for _, endpoint := range []string{
		listViewsEndpoint,
		loadViewEndpoint,
		viewExistsEndpoint,
		createViewEndpoint,
		replaceViewEndpoint,
		dropViewEndpoint,
	} {
		require.Contains(t, config.Endpoints, endpoint,
			"GET /v1/config must advertise every view operation the catalog serves")
	}
	require.NotContains(t, config.Endpoints, "POST /v1/{prefix}/views/rename",
		"view rename stays a v1 non-goal and must never be advertised")
}

// TestViewsRelativeTo drives a view through the mounted catalog's relative_to form: a client names
// the warehouse in the URL, so the namespace levels it sends - and reads back - are its own short
// names, never [repo, ref, ns…].
func TestViewsRelativeTo(t *testing.T) {
	f := newViewsFixture(t)

	warehouse := func(catalogPath string) string {
		return RelativeToBasePath + "/" + viewsTestRepo + "." + viewsTestRef + "/api" + catalogPath
	}
	// relativeRequest drives the mounted catalog the way a client does: through RelativeToBasePath.
	relativeRequest := func(t *testing.T, method, target, user, body string) *httptest.ResponseRecorder {
		t.Helper()
		var reader io.Reader
		if body != "" {
			reader = strings.NewReader(body)
		}
		req := httptest.NewRequest(method, target, reader)
		req.Header.Set("X-Test-User", user)
		if body != "" {
			req.Header.Set("Content-Type", contentTypeJSON)
		}
		rec := httptest.NewRecorder()
		f.mux.ServeHTTP(rec, req)
		return rec
	}
	namespace := "analytics"
	// The fixture's users authenticate the relative_to fixture's way only in relative_test.go, so
	// this test uses the policy of a user that holds the view writes on this repository.
	f.authz.attachStatements(t, "relative-writer", "carol", append(viewsFixtureStatements(),
		model.Statement{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.CatalogCreateNamespaceAction},
			Resource: "arn:lakefs:catalog:::*",
		}))

	// Create the namespace through the catalog on the identifier form (the relative_to form's
	// namespace create is covered by relative_test.go), then create the view through relative_to.
	create, err := json.Marshal(createNamespaceRequest{Namespace: []string{viewsTestRepo, viewsTestRef, namespace}})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK,
		f.requestBody(t, http.MethodPost, "/v1/"+CatalogPrefix+"/namespaces", "carol", string(create)).Code)

	rec := relativeRequest(t, http.MethodPost,
		warehouse("/v1/"+CatalogPrefix+"/namespaces/"+url.PathEscape(namespace)+"/views"),
		"carol", viewsCreateBody(t, "relative_view", "SELECT 1"))
	_ = decodeLoadView(t, rec)

	// Listing and loading through the same mount answers the client's own terms.
	listed := decodeListViews(t, relativeRequest(t, http.MethodGet,
		warehouse("/v1/"+CatalogPrefix+"/namespaces/"+url.PathEscape(namespace)+"/views"), "alice", ""))
	require.Equal(t, []string{"relative_view"}, viewNamesOf(listed))
	require.Equal(t, []string{namespace}, []string(listed.Identifiers[0].Namespace),
		"a relative_to client must read back the namespace it sent, not the levels its URL fixed")

	loaded := decodeLoadView(t, relativeRequest(t, http.MethodGet,
		warehouse("/v1/"+CatalogPrefix+"/namespaces/"+url.PathEscape(namespace)+"/views/relative_view"), "alice", ""))
	require.NotEmpty(t, viewMetadataField(t, loaded.Metadata, "view-uuid"),
		"the load through relative_to serves the view the client named")
	require.Equal(t, http.StatusNoContent, relativeRequest(t, http.MethodHead,
		warehouse("/v1/"+CatalogPrefix+"/namespaces/"+url.PathEscape(namespace)+"/views/relative_view"), "alice", "").Code)

	// And the identifier form sees the same view at the full address.
	require.Equal(t, http.StatusOK, f.request(t, http.MethodGet,
		viewsLoadTarget(nodes(viewsTestRepo, viewsTestRef, namespace), "relative_view"), "alice").Code)
}

// TestViewsThroughRoutesBuilder is the wiring test: it drives the catalog the lakefs-sso binary
// mounts - RoutesBuilder, with the catalog's own authentication and both addressing forms - and
// performs a create, a list and a load of a view through the relative_to form. A view service that
// exists but is not mounted, or that is mounted without its routes being advertised, fails here
// rather than in a deployment.
func TestViewsThroughRoutesBuilder(t *testing.T) {
	f := newRelativeToFixture(t)

	// GET /v1/config through the production mount advertises the six operations.
	rec := f.request(t, http.MethodGet, APIBasePath+configPath, "alice")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var config CatalogConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &config))
	for _, endpoint := range []string{
		listViewsEndpoint, loadViewEndpoint, viewExistsEndpoint,
		createViewEndpoint, replaceViewEndpoint, dropViewEndpoint,
	} {
		require.Contains(t, config.Endpoints, endpoint,
			"RoutesBuilder must advertise %q", endpoint)
	}

	// A client of the warehouse form creates the view with its own short namespace, and reads it
	// back in the same terms.
	warehouse := RelativeToBasePath + "/" + nsTestRepo + "." + nsTestRef + "/api"
	viewsTarget := warehouse + "/v1/" + CatalogPrefix + "/namespaces/inventory/views"
	require.Equal(t, http.StatusOK,
		f.requestBody(t, http.MethodPost, viewsTarget, "carol", viewsCreateBody(t, "wired_view", "SELECT 1")).Code)

	listed := decodeListViews(t, f.request(t, http.MethodGet, viewsTarget, "alice"))
	require.Equal(t, []string{"wired_view"}, viewNamesOf(listed))
	require.Equal(t, []string{"inventory"}, []string(listed.Identifiers[0].Namespace),
		"a relative_to client reads back the namespace it sent")

	loaded := decodeLoadView(t, f.request(t, http.MethodGet, viewsTarget+"/wired_view", "alice"))
	require.NotEmpty(t, viewMetadataField(t, loaded.Metadata, "view-uuid"))
}

// TestViewsStore covers the store-level facts the endpoints rest on.
func TestViewsStore(t *testing.T) {
	f := newViewsFixture(t)
	addr := Address{Repo: viewsTestRepo, Ref: viewsTestRef, Namespace: Namespace{"inventory", "books"}, View: "orders_view"}

	t.Run("a pointer round-trips through its entry user metadata", func(t *testing.T) {
		pointer := ViewPointer{
			MetadataLocation: "mem://ns/_iceberg/uuid/metadata/00001.metadata.json",
			ViewUUID:         viewsTestUUID,
			FormatVersion:    SupportedViewFormatVersion,
		}
		metadata, err := EncodeViewPointer(pointer)
		require.NoError(t, err)
		require.Equal(t, pointer.MetadataLocation, metadata[MetadataLocationKey])
		require.Equal(t, viewsTestUUID, metadata[ViewUUIDKey])
		require.Equal(t, "1", metadata[FormatVersionKey])

		decoded, err := DecodeViewPointer(metadata)
		require.NoError(t, err)
		require.Equal(t, pointer, decoded)
	})

	t.Run("a pointer this catalog could not have written is refused", func(t *testing.T) {
		cases := map[string]map[string]string{
			"no location": {ViewUUIDKey: viewsTestUUID, FormatVersionKey: "1"},
			"no uuid":     {MetadataLocationKey: "mem://x", FormatVersionKey: "1"},
			"no version":  {MetadataLocationKey: "mem://x", ViewUUIDKey: viewsTestUUID},
			"bad version": {MetadataLocationKey: "mem://x", ViewUUIDKey: viewsTestUUID, FormatVersionKey: "2"},
			"not a number": {MetadataLocationKey: "mem://x", ViewUUIDKey: viewsTestUUID,
				FormatVersionKey: "one"},
		}
		for name, metadata := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := DecodeViewPointer(metadata)
				require.ErrorIs(t, err, ErrInvalidPointer)
			})
		}
		_, err := EncodeViewPointer(ViewPointer{MetadataLocation: "mem://x", ViewUUID: viewsTestUUID, FormatVersion: 2})
		require.ErrorIs(t, err, ErrInvalidPointer)
	})

	t.Run("the pointer path is the reserved views layout", func(t *testing.T) {
		path, err := viewPointerPath(addr)
		require.NoError(t, err)
		require.Equal(t, ReservedPrefix+"/"+ViewsDirectory+"/inventory/books/orders_view/.pointer", path)
		require.Equal(t, ReservedPrefix+"/"+ViewsDirectory+"/inventory/books/",
			viewEntryPrefix(Namespace{"inventory", "books"}))
	})

	t.Run("the metadata file sequence continues the file it replaces", func(t *testing.T) {
		metadata, err := f.store.GetView(f.ctx, addr)
		require.NoError(t, err)
		base := metadata.Pointer.MetadataLocation
		next, err := NewViewMetadataFileLocation(metadata.Metadata.Location(), base, metadata.Metadata)
		require.NoError(t, err)
		require.NotEqual(t, base, next, "a new document never reuses the name it replaces")
		baseSequence, err := strconv.Atoi(strings.SplitN(path.Base(base), "-", 2)[0])
		require.NoError(t, err, "the fixture's metadata file is named <seq>-<uuid>.metadata.json")
		nextSequence, err := strconv.Atoi(strings.SplitN(path.Base(next), "-", 2)[0])
		require.NoError(t, err)
		require.Equal(t, baseSequence+1, nextSequence,
			"the sequence continues the file it replaces: %s -> %s", path.Base(base), path.Base(next))
	})

	t.Run("an update conditioned on a stale pointer loses the compare-and-swap", func(t *testing.T) {
		base, err := f.store.GetView(f.ctx, addr)
		require.NoError(t, err)
		next := ViewPointer{
			MetadataLocation: base.Pointer.MetadataLocation + ".next",
			ViewUUID:         base.Pointer.ViewUUID,
			FormatVersion:    SupportedViewFormatVersion,
		}
		require.NoError(t, f.store.UpdateViewPointer(f.ctx, addr, base.Pointer, next),
			"the first writer wins")
		err = f.store.UpdateViewPointer(f.ctx, addr, base.Pointer, ViewPointer{
			MetadataLocation: base.Pointer.MetadataLocation + ".loser",
			ViewUUID:         base.Pointer.ViewUUID,
			FormatVersion:    SupportedViewFormatVersion,
		})
		require.ErrorIs(t, err, graveler.ErrPreconditionFailed,
			"a second writer conditioned on the same base must lose")
	})

	t.Run("a create on an existing view loses the assert-create condition", func(t *testing.T) {
		err := f.store.CreateView(f.ctx, addr, ViewPointer{
			MetadataLocation: "mem://x/metadata/00001.metadata.json",
			ViewUUID:         viewsTestUUID,
			FormatVersion:    SupportedViewFormatVersion,
		})
		require.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("a replaced view keeps its uuid across commits", func(t *testing.T) {
		replaced := Address{Repo: viewsTestRepo, Ref: viewsTestRef, Namespace: Namespace{"inventory", "books"}, View: "stale_view"}
		base, err := f.store.GetView(f.ctx, replaced)
		require.NoError(t, err)
		result, err := f.store.CommitView(f.ctx, replaced, ViewCommitRequest{
			Requirements: view.Requirements{view.AssertViewUUID(uuid.MustParse(base.Pointer.ViewUUID))},
		})
		require.NoError(t, err)
		require.Equal(t, base.Pointer.ViewUUID, result.Pointer.ViewUUID)
		require.NotEqual(t, base.Pointer.MetadataLocation, result.Pointer.MetadataLocation)

		// A commit with no pointer move at all is still committed: the returned metadata is the
		// document a subsequent load reads.
		loaded, err := f.store.GetView(f.ctx, replaced)
		require.NoError(t, err)
		require.Equal(t, result.Pointer.MetadataLocation, loaded.Pointer.MetadataLocation)
	})
}

// TestViewMergeConflictIsALogicalConflict pins that a view pointer is an ordinary entry at merge:
// two branches that replaced the same view conflict, exactly like a table pointer (§6.4), because
// only *table* pointers have a compaction resolver.
func TestViewMergeConflictIsALogicalConflict(t *testing.T) {
	addr := Address{Repo: viewsTestRepo, Ref: viewsTestRef, Namespace: Namespace{"inventory", "books"}, View: "orders_view"}
	require.False(t, IsTablePointerKey(graveler.Key(mustViewPointerPath(t, addr))),
		"a view pointer is not a table pointer: the compaction resolver must not resolve it")
	require.True(t, IsTablePointerKey(graveler.Key(ReservedPrefix+"/"+TablesDirectory+"/inventory/books/t/.pointer")))
}

// --- helpers ---------------------------------------------------------------------------------

// mustViewPointerPath is viewPointerPath that fails the test instead of returning an error.
func mustViewPointerPath(t *testing.T, addr Address) string {
	t.Helper()
	path, err := viewPointerPath(addr)
	require.NoError(t, err)
	return path
}

// viewPointer returns the pointer the store reads for a view.
func (f *viewsFixture) viewPointer(t *testing.T, namespace Namespace, name string) ViewPointer {
	t.Helper()
	loaded, err := f.store.GetView(f.ctx, Address{
		Repo: viewsTestRepo, Ref: viewsTestRef, Namespace: namespace, View: name,
	})
	require.NoErrorf(t, err, "get view %s.%s", namespace.Dotted(), name)
	return loaded.Pointer
}

// listEntries lists every entry below prefix at the fixture's branch head.
func (f *viewsFixture) listEntries(t *testing.T, prefix string) []*catalog.DBEntry {
	t.Helper()
	entries, _, err := f.catalog.ListEntries(f.ctx, viewsTestRepo, viewsTestRef, prefix, "", "", catalog.ListEntriesLimitMax)
	require.NoErrorf(t, err, "list entries below %q", prefix)
	return entries
}

// readObject reads a physical object through the block adapter.
func (f *viewsFixture) readObject(ctx context.Context, location string, into *[]byte) error {
	repo, err := f.catalog.GetRepository(ctx, viewsTestRepo)
	if err != nil {
		return err
	}
	reader, err := f.catalog.BlockAdapter.Get(ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	})
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	*into, err = io.ReadAll(reader)
	return err
}

// entryPaths renders entry paths for an assertion message.
func entryPaths(entries []*catalog.DBEntry) []string {
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, entry.Path)
	}
	sort.Strings(paths)
	return paths
}

// viewsSchemaJSON is the wire form of viewsTestSchema, so one create test can carry a hand-written
// body instead of re-marshalling the struct the handler decodes (which would hide a field name).
const viewsSchemaJSON = `{"type":"struct","schema-id":0,"fields":[
	{"id":1,"name":"id","required":true,"type":"long"},
	{"id":2,"name":"name","required":false,"type":"string"}]}`

// viewsCreateBody builds a CreateViewRequest body.
func viewsCreateBody(t *testing.T, name, sql string) string {
	t.Helper()
	version := viewsTestVersion(t, 1, sql, table.Identifier{"inventory", "books"})
	body, err := json.Marshal(createViewRequest{
		Name:        name,
		Schema:      viewsTestSchema(),
		Properties:  iceberg.Properties{"owner": "data-team"},
		ViewVersion: version,
	})
	require.NoError(t, err, "marshal the create request")
	return string(body)
}

// viewsVersionJSON is the wire form of one view version, for the add-view-version update.
func viewsVersionJSON(t *testing.T, version *view.Version) string {
	t.Helper()
	body, err := json.Marshal(version)
	require.NoError(t, err, "marshal view version")
	return string(body)
}

// racingViewPointerCatalog is a catalog.Catalog that stages a competing view pointer immediately
// before the write under test reaches graveler, which is how a lost compare-and-swap is produced
// deterministically instead of by a race.
type racingViewPointerCatalog struct {
	*catalog.Catalog
	before func() error
	fired  bool
}

func newRacingViewPointerCatalog(inner *catalog.Catalog, before func() error) *racingViewPointerCatalog {
	return &racingViewPointerCatalog{Catalog: inner, before: before}
}

func (c *racingViewPointerCatalog) CreateEntry(ctx context.Context, repositoryID, branch string, entry catalog.DBEntry, opts ...graveler.SetOptionsFunc) error {
	if !c.fired && strings.HasPrefix(entry.Path, ReservedPrefix+"/"+ViewsDirectory+"/") &&
		strings.HasSuffix(entry.Path, "/"+ViewPointerName) {
		c.fired = true
		if err := c.before(); err != nil {
			return err
		}
	}
	return c.Catalog.CreateEntry(ctx, repositoryID, branch, entry, opts...)
}
