package icebergcatalog

// Tests for the namespace read endpoints (ICEBERG_CATALOG_PLAN.md §5 "Addressing rules", §7.2 and
// §11 I-1 "namespaces list/get/head (repo/ref levels)"). Every test name starts with
// TestNamespacesRead so that the task's validation command,
// `go test -run TestNamespacesRead ./pkg/icebergcatalog/`, runs exactly this file.
//
// The fixture is the real read path end to end: a catalog store (graveler + committed ranges) on
// an in-memory KV store, an in-memory block adapter, the simplified-ACL auth service the
// lakefs-sso binary uses, and the production middleware stack built by newRouter. Namespace
// markers are written with the store's own create path, so the entries the endpoints read are
// exactly the entries the write path of I2-2 will produce.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	// nsTestRepo is the repository the namespace tree of this file lives in. It is authzRepo
	// ("lakehouse"), so the §7.2 read policy of authz_test.go applies unchanged.
	nsTestRepo = authzRepo

	// nsTestRef is the branch the markers are created on and committed to.
	nsTestRef = "main"

	// nsTestTag is a tag created on the committed catalog state, so "a namespace is readable on
	// any ref" is tested against a real tag and not only a branch.
	nsTestTag = "release"
)

// namespacesFixture is the namespace read stack: the catalog store, the ACL auth service, the
// namespace service and the router a client talks to.
type namespacesFixture struct {
	*storeFixture
	authz  *authzFixture
	serve  *namespaceService
	router http.Handler
}

// namespacesReadStatements grants the read half of §7.2 plus the repository-list permission the
// root level of §5 needs. No fs: action is granted: a catalog request needs its catalog action
// only (authz.go), so this is the whole grant a catalog client has to hold.
func namespacesReadStatements() model.Statements {
	statements := authzReadStatements()
	return append(statements, model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ListRepositoriesAction},
		Resource: permissions.All,
	})
}

// namespacesUser is the authentication stand-in the router is built with: it authenticates every
// request as the user named by X-Test-User (alice by default), so a test can switch between a
// granted caller and one without policies.
func namespacesUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username := r.Header.Get("X-Test-User")
		if username == "" {
			username = "alice"
		}
		next.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), &model.User{Username: username})))
	})
}

// namespacesAllRepositoriesStatements grants catalog:* on every catalog ARN plus the
// repository-list permission, so a repository that does not exist is reached by the lookup instead
// of being refused at the authorization gate.
func namespacesAllRepositoriesStatements() model.Statements {
	return model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ListRepositoriesAction},
			Resource: permissions.All,
		},
	}
}

// newNamespacesFixture builds the fixture and materializes a small namespace tree on main:
//
//	inventory/.namespace          properties {"owner": "data-team"}
//	inventory/books/.namespace    properties {"owner": "data-team", "comment": "books"}
//	inventory/authors/.namespace  no properties
//	other/.namespace              no properties
//
// main is then committed and release tags that commit; dev stays at the empty initial commit, so
// the branch-level listing has two branches while only one carries catalog state.
func newNamespacesFixture(t *testing.T) *namespacesFixture {
	t.Helper()
	cat := newNamespacesCatalog(t)
	base := &storeFixture{ctx: t.Context(), catalog: cat, store: NewStore(cat, cat.BlockAdapter)}
	ctx := base.ctx

	_, err := base.catalog.CreateRepository(ctx, nsTestRepo, "", "mem://iceberg-namespaces-ns", nsTestRef, false)
	require.NoError(t, err, "create catalog repository")
	_, err = base.catalog.CreateBranch(ctx, nsTestRepo, "dev", nsTestRef)
	require.NoError(t, err, "create dev branch")

	store := NewStore(base.catalog, base.catalog.BlockAdapter)
	authz := newAuthzUser(t, namespacesReadStatements())
	authz.user(t, "bob")
	authz.user(t, "dana")
	authz.attachStatements(t, "all-repositories", "dana", namespacesAllRepositoriesStatements())

	service := NewNamespaceService(store, authz.authz, base.catalog, &Config{Prefix: CatalogPrefix})
	router, err := newRouter(namespacesUser, &Config{}, testServer(&Config{Prefix: CatalogPrefix}, service.Routes()...))
	require.NoError(t, err, "build catalog router")

	f := &namespacesFixture{storeFixture: base, authz: authz, serve: service, router: router}
	f.createNamespace(t, Namespace{"inventory"}, map[string]string{"owner": "data-team"})
	f.createNamespace(t, Namespace{"inventory", "books"},
		map[string]string{"owner": "data-team", "comment": "books"})
	f.createNamespace(t, Namespace{"inventory", "authors"}, nil)
	f.createNamespace(t, Namespace{"other"}, nil)

	ref := f.commitCatalog(t)
	_, err = base.catalog.CreateTag(ctx, nsTestRepo, nsTestTag, ref)
	require.NoError(t, err, "create tag %s", nsTestTag)
	return f
}

// newNamespacesCatalog builds the fixture's catalog with a local cache directory of its own.
//
// The package's shared fixture (store_test.go) uses the configured cache directory, which by
// default lives outside any temporary directory, so consecutive runs of the package can meet each
// other's cached meta-ranges and fail inside a commit with "no such file" on a workspace file -
// the store tests hit the same problem when they are run shuffled. A per-test directory makes the
// namespace tests hermetic; it changes nothing about what they assert.
func newNamespacesCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	ctx := t.Context()
	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open mem KV store")
	t.Cleanup(kvStore.Close)

	cfg := storeTestConfig(t)
	cfg.Committed.LocalCache.Dir = t.TempDir()

	cat, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err, "build catalog")
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// createNamespace stages a marker on main through the store's own create path.
func (f *namespacesFixture) createNamespace(t *testing.T, namespace Namespace, properties map[string]string) {
	t.Helper()
	require.NoError(t, f.store.CreateNamespace(f.ctx, Address{
		Repo: nsTestRepo, Ref: nsTestRef, Namespace: namespace,
	}, properties), "create namespace %s", namespace.Dotted())
}

// commitCatalog commits main and returns the new commit ID.
func (f *namespacesFixture) commitCatalog(t *testing.T) string {
	t.Helper()
	log, err := f.catalog.Commit(f.ctx, nsTestRepo, nsTestRef, "iceberg namespaces", "namespaces-test",
		nil, nil, nil, false)
	require.NoError(t, err, "commit catalog state")
	return log.Reference
}

// request sends one catalog request as user.
func (f *namespacesFixture) request(t *testing.T, method, target, user string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-Test-User", user)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// nodes renders the client namespace path parameter of the /iceberg/api form (§5): the levels
// joined by the spec's unit separator and percent-encoded once.
func nodes(levels ...string) string {
	return url.PathEscape(strings.Join(levels, NamespaceSeparator))
}

// namespacesTarget is the get/head URL of one namespace address.
func namespacesTarget(levels ...string) string {
	return "/v1/" + CatalogPrefix + "/namespaces/" + nodes(levels...)
}

// listTarget is the list URL of an optional parent and optional extra query parameters.
func listTarget(parent []string, query url.Values) string {
	values := url.Values{}
	if len(parent) > 0 {
		values.Set("parent", strings.Join(parent, NamespaceSeparator))
	}
	for key, v := range query {
		values[key] = v
	}
	target := "/v1/" + CatalogPrefix + "/namespaces"
	if encoded := values.Encode(); encoded != "" {
		target += "?" + encoded
	}
	return target
}

// decodeListNamespaces asserts a successful listing and decodes it.
func decodeListNamespaces(t *testing.T, rec *httptest.ResponseRecorder) listNamespacesResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response listNamespacesResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return response
}

// decodeGetNamespace asserts a successful namespace read and decodes it.
func decodeGetNamespace(t *testing.T, rec *httptest.ResponseRecorder) getNamespaceResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response getNamespaceResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return response
}

// TestNamespacesRead covers the three listing levels, get/head with properties, any-ref reads,
// pagination and the error paths of §5 and §7.2.
func TestNamespacesRead(t *testing.T) {
	f := newNamespacesFixture(t)

	t.Run("root lists only readable repositories", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, listTarget(nil, nil), "alice")
		response := decodeListNamespaces(t, rec)
		require.Equal(t, []Namespace{{nsTestRepo}}, response.Namespaces)
		require.Empty(t, response.NextPageToken)
	})

	t.Run("repository level lists branches", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, listTarget([]string{nsTestRepo}, nil), "alice")
		response := decodeListNamespaces(t, rec)
		require.Equal(t, []Namespace{
			{nsTestRepo, "dev"},
			{nsTestRepo, nsTestRef},
		}, response.Namespaces)
	})

	t.Run("branch level lists real namespaces", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, listTarget([]string{nsTestRepo, nsTestRef}, nil), "alice")
		response := decodeListNamespaces(t, rec)
		require.Equal(t, []Namespace{
			{nsTestRepo, nsTestRef, "inventory"},
			{nsTestRepo, nsTestRef, "other"},
		}, response.Namespaces)
	})

	t.Run("namespace level lists immediate children only", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			listTarget([]string{nsTestRepo, nsTestRef, "inventory"}, nil), "alice")
		response := decodeListNamespaces(t, rec)
		require.Equal(t, []Namespace{
			{nsTestRepo, nsTestRef, "inventory", "authors"},
			{nsTestRepo, nsTestRef, "inventory", "books"},
		}, response.Namespaces)
	})

	t.Run("a branch without catalog state lists no namespaces", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, listTarget([]string{nsTestRepo, "dev"}, nil), "alice")
		response := decodeListNamespaces(t, rec)
		require.Empty(t, response.Namespaces)
		require.NotNil(t, response.Namespaces, "an empty listing is [], not null")
	})

	t.Run("pageSize and pageToken walk the whole listing", func(t *testing.T) {
		target := listTarget([]string{nsTestRepo, nsTestRef}, url.Values{"pageSize": {"1"}})
		first := decodeListNamespaces(t, f.request(t, http.MethodGet, target, "alice"))
		require.Len(t, first.Namespaces, 1)
		require.NotEmpty(t, first.NextPageToken, "a full page must carry a next-page-token")

		next := listTarget([]string{nsTestRepo, nsTestRef}, url.Values{
			"pageSize":  {"1"},
			"pageToken": {first.NextPageToken},
		})
		second := decodeListNamespaces(t, f.request(t, http.MethodGet, next, "alice"))
		require.Len(t, second.Namespaces, 1)
		require.Empty(t, second.NextPageToken, "the last page carries no token")

		require.Equal(t, []Namespace{
			{nsTestRepo, nsTestRef, "inventory"},
			{nsTestRepo, nsTestRef, "other"},
		}, append(first.Namespaces, second.Namespaces...))
	})

	t.Run("get returns the namespace and its properties", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory", "books"), "alice")
		response := decodeGetNamespace(t, rec)
		require.Equal(t, Namespace{nsTestRepo, nsTestRef, "inventory", "books"}, response.Namespace)
		require.Equal(t, map[string]string{"owner": "data-team", "comment": "books"}, response.Properties)
	})

	t.Run("get returns an empty property map for a marker without properties", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "other"), "alice")
		response := decodeGetNamespace(t, rec)
		require.Equal(t, Namespace{nsTestRepo, nsTestRef, "other"}, response.Namespace)
		require.NotNil(t, response.Properties)
		require.Empty(t, response.Properties)
	})

	t.Run("head answers 204 for an existing namespace", func(t *testing.T) {
		rec := f.request(t, http.MethodHead,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory"), "alice")
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String())
	})

	t.Run("a namespace is readable on a tag", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestTag, "inventory", "books"), "alice")
		response := decodeGetNamespace(t, rec)
		require.Equal(t, map[string]string{"owner": "data-team", "comment": "books"}, response.Properties)

		head := f.request(t, http.MethodHead,
			namespacesTarget(nsTestRepo, nsTestTag, "inventory"), "alice")
		require.Equal(t, http.StatusNoContent, head.Code, "body: %s", head.Body.String())
	})

	t.Run("a missing namespace is 404 NoSuchNamespaceException", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory", "missing"), "alice")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)

		head := f.request(t, http.MethodHead,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory", "missing"), "alice")
		requireEnvelope(t, head, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("an unknown repository is 404 NoSuchWarehouseException", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, listTarget([]string{"missing-repo"}, nil), "dana")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("an unknown branch is 404 NoSuchWarehouseException", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			listTarget([]string{nsTestRepo, "missing-branch"}, nil), "alice")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("a prefix other than the catalog prefix is 404 NoSuchWarehouseException", func(t *testing.T) {
		rec := f.request(t, http.MethodGet,
			"/v1/not-the-prefix/namespaces/"+nodes(nsTestRepo, nsTestRef, "inventory"), "alice")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)

		list := f.request(t, http.MethodGet, "/v1/not-the-prefix/namespaces", "alice")
		requireEnvelope(t, list, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("a namespace address without a namespace level is 400", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, namespacesTarget(nsTestRepo, nsTestRef), "alice")
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("a caller without grants is refused on every level", func(t *testing.T) {
		root := f.request(t, http.MethodGet, listTarget(nil, nil), "bob")
		requireEnvelope(t, root, http.StatusForbidden, ErrTypeForbidden)

		get := f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory"), "bob")
		requireEnvelope(t, get, http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("a malformed page token or page size is 400", func(t *testing.T) {
		token := f.request(t, http.MethodGet, listTarget([]string{nsTestRepo, nsTestRef},
			url.Values{"pageToken": {"!!!not-a-token!!!"}}), "alice")
		requireEnvelope(t, token, http.StatusBadRequest, ErrTypeBadRequest)

		size := f.request(t, http.MethodGet, listTarget([]string{nsTestRepo, nsTestRef},
			url.Values{"pageSize": {"one"}}), "alice")
		requireEnvelope(t, size, http.StatusBadRequest, ErrTypeBadRequest)
	})
}

// Tests for the namespace write endpoints of I2-2 (ICEBERG_CATALOG_PLAN.md §6.1 "Layout on the
// branch", §7.2 "Authorization" and §11 I-2 "namespaces create/update-properties/drop"). Every
// test name starts with TestNamespacesWrite so that the task's validation command,
// `go test -run TestNamespacesWrite ./pkg/icebergcatalog/`, runs exactly this file's write half.
//
// The fixture is the read fixture of this file: the real catalog store on an in-memory KV store,
// the ACL auth service, the production middleware stack and the router a client talks to. The
// namespace markers are written by the endpoints themselves, and the table pointer a drop has to
// find is staged through the store's own create path (I2-1), so "a namespace that still holds
// tables" is the state the catalog (not the client) has to detect.

const (
	// nsWriteCreateTarget is POST /v1/{prefix}/namespaces, the create endpoint.
	nsWriteCreateTarget = "/v1/" + CatalogPrefix + "/namespaces"
)

// writeRequest sends one catalog write as user, with an optional JSON body.
func (f *namespacesFixture) writeRequest(t *testing.T, method, target, user, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", contentTypeJSON)
	}
	req.Header.Set("X-Test-User", user)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// namespacePropertiesTarget is the properties URL of one namespace address.
func namespacePropertiesTarget(levels ...string) string {
	return namespacesTarget(levels...) + "/properties"
}

// jsonBody marshals a request body.
func jsonBody(t *testing.T, body any) string {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	return string(encoded)
}

// decodeCreateNamespace asserts a successful create and decodes it.
func decodeCreateNamespace(t *testing.T, rec *httptest.ResponseRecorder) createNamespaceResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response createNamespaceResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return response
}

// decodeUpdateNamespaceProperties asserts a successful properties update and decodes it.
func decodeUpdateNamespaceProperties(t *testing.T, rec *httptest.ResponseRecorder) updateNamespacePropertiesResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response updateNamespacePropertiesResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return response
}

// requireForbiddenReadOnly asserts the Q-I4 refusal: 403 ForbiddenException whose message is
// exactly "ref is read-only", the message the plan fixes for a write on a tag or a commit.
func requireForbiddenReadOnly(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	apiError := requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	require.Equal(t, ErrRefReadOnly.Error(), apiError.Message)
}

// writeFixture is the read fixture (which already carries a committed inventory/books/authors/other
// tree and a tag) plus the write half of the §7.2 policy: the catalog write actions, on catalog
// ARNs only (authz.go). Without them every write endpoint would answer 403, which is the read
// fixture's whole point.
func writeFixture(t *testing.T) *namespacesFixture {
	t.Helper()
	f := newNamespacesFixture(t)
	f.authz.attachStatements(t, "write-grants", "alice", append(
		authzWriteStatements(),
		model.Statement{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ListRepositoriesAction},
			Resource: permissions.All,
		},
	))
	// dana may write on every repository (catalog:* on every catalog ARN), so a missing repository
	// is reached by the lookup instead of being refused at the authorization gate.
	f.authz.attachStatements(t, "write-all-repositories", "dana", namespacesAllRepositoriesStatements())
	return f
}

// TestNamespacesWrite covers create, update-properties and drop, the statuses of §11 I-2 and the
// endpoint list GET /v1/config advertises once they are mounted.
//
// The subtests share one fixture - and so one catalog instance - on purpose: each fixture builds a
// catalog with its own local pyramid cache over the same cache root, and a run that builds many
// catalogs in one process can lose a meta range to the cache's eviction (the store tests hit the
// same thing when they are run shuffled). Sharing the fixture keeps one catalog alive for the
// whole file. The subtests are independent in what they assert: each works on namespaces the
// others do not use, except that the update-properties subtests build on each other in order.
func TestNamespacesWrite(t *testing.T) {
	f := writeFixture(t)
	t.Run("create", func(t *testing.T) { testNamespacesWriteCreate(t, f) })
	t.Run("update properties", func(t *testing.T) { testNamespacesWriteUpdateProperties(t, f) })
	t.Run("drop", func(t *testing.T) { testNamespacesWriteDrop(t, f) })
	t.Run("advertised endpoints", func(t *testing.T) { testNamespacesWriteAdvertisedEndpoints(t, f) })
}

func testNamespacesWriteCreate(t *testing.T, f *namespacesFixture) {

	t.Run("create stages the namespace and its properties", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
			Namespace:  []string{nsTestRepo, nsTestRef, "sales"},
			Properties: map[string]string{"owner": "sales-team"},
		}))
		response := decodeCreateNamespace(t, rec)
		require.Equal(t, Namespace{nsTestRepo, nsTestRef, "sales"}, response.Namespace)
		require.Equal(t, map[string]string{"owner": "sales-team"}, response.Properties)

		// The marker is readable immediately: a catalog write is staged on the branch (§6.2).
		get := decodeGetNamespace(t, f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "sales"), "alice"))
		require.Equal(t, map[string]string{"owner": "sales-team"}, get.Properties)

		head := f.request(t, http.MethodHead,
			namespacesTarget(nsTestRepo, nsTestRef, "sales"), "alice")
		require.Equal(t, http.StatusNoContent, head.Code, "body: %s", head.Body.String())

		// ...and not committed: the tag over the previous commit does not see it. Staging, not
		// auto-commit, is the documented behaviour (§6.2 "why stage instead of auto-commit").
		tag := f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestTag, "sales"), "alice")
		requireEnvelope(t, tag, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("create without properties answers an empty property object", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
			Namespace: []string{nsTestRepo, nsTestRef, "no-properties"},
		}))
		response := decodeCreateNamespace(t, rec)
		require.NotNil(t, response.Properties)
		require.Empty(t, response.Properties)
	})

	t.Run("create on an existing namespace is 409 and keeps the properties", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
			Namespace:  []string{nsTestRepo, nsTestRef, "inventory"},
			Properties: map[string]string{"owner": "someone-else"},
		}))
		requireEnvelope(t, rec, http.StatusConflict, ErrTypeNamespaceAlreadyExists)

		get := decodeGetNamespace(t, f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory"), "alice"))
		require.Equal(t, map[string]string{"owner": "data-team"}, get.Properties,
			"a losing create must not overwrite the existing properties")
	})

	t.Run("create of the repository or ref level is 400", func(t *testing.T) {
		for _, levels := range [][]string{
			{nsTestRepo},
			{nsTestRepo, nsTestRef},
		} {
			rec := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
				Namespace: levels,
			}))
			requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		}
	})

	t.Run("create with an empty namespace or property name is 400", func(t *testing.T) {
		empty := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{}))
		requireEnvelope(t, empty, http.StatusBadRequest, ErrTypeBadRequest)

		emptyProperty := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
			Namespace:  []string{nsTestRepo, nsTestRef, "bad-property"},
			Properties: map[string]string{"": "value"},
		}))
		requireEnvelope(t, emptyProperty, http.StatusBadRequest, ErrTypeBadRequest)
	})

	t.Run("create on a tag is 403 ref is read-only", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
			Namespace: []string{nsTestRepo, nsTestTag, "on-a-tag"},
		}))
		requireForbiddenReadOnly(t, rec)

		// A commit ID is as read-only as a tag (§5, Q-I4): both are refs a write cannot move.
		commit, err := f.catalog.GetBranchReference(f.ctx, nsTestRepo, nsTestRef)
		require.NoError(t, err, "read the main branch reference")
		onCommit := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
			Namespace: []string{nsTestRepo, commit, "on-a-commit"},
		}))
		requireForbiddenReadOnly(t, onCommit)
	})

	t.Run("create on a missing branch or repository is 404 NoSuchWarehouse", func(t *testing.T) {
		missingBranch := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
			Namespace: []string{nsTestRepo, "missing-branch", "ns"},
		}))
		requireEnvelope(t, missingBranch, http.StatusNotFound, ErrTypeNoSuchWarehouse)

		missingRepo := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "dana", jsonBody(t, createNamespaceRequest{
			Namespace: []string{"missing-repo", "main", "ns"},
		}))
		requireEnvelope(t, missingRepo, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("a caller without grants is refused", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "bob", jsonBody(t, createNamespaceRequest{
			Namespace: []string{nsTestRepo, nsTestRef, "not-allowed"},
		}))
		requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	})

	t.Run("a malformed body is 400", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", "{not json")
		requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
	})
}

func testNamespacesWriteUpdateProperties(t *testing.T, f *namespacesFixture) {

	t.Run("properties are merged, not replaced", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost,
			namespacePropertiesTarget(nsTestRepo, nsTestRef, "inventory"), "alice",
			jsonBody(t, updateNamespacePropertiesRequest{
				Updates:  map[string]string{"team": "core", "owner": "analytics"},
				Removals: []string{},
			}))
		response := decodeUpdateNamespaceProperties(t, rec)
		require.Equal(t, []string{"owner", "team"}, response.Updated)
		require.Empty(t, response.Removed)
		require.Empty(t, response.Missing)

		get := decodeGetNamespace(t, f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory"), "alice"))
		require.Equal(t, map[string]string{"owner": "analytics", "team": "core"}, get.Properties,
			"a property that was not mentioned must survive the update")
	})

	t.Run("removals are applied and the rest is kept", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost,
			namespacePropertiesTarget(nsTestRepo, nsTestRef, "inventory"), "alice",
			jsonBody(t, updateNamespacePropertiesRequest{Removals: []string{"owner"}}))
		response := decodeUpdateNamespaceProperties(t, rec)
		require.Equal(t, []string{"owner"}, response.Removed)
		require.Empty(t, response.Updated)
		require.Empty(t, response.Missing)

		get := decodeGetNamespace(t, f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory"), "alice"))
		require.Equal(t, map[string]string{"team": "core"}, get.Properties)
	})

	t.Run("a removal of a property that is not there is a partial success", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost,
			namespacePropertiesTarget(nsTestRepo, nsTestRef, "inventory"), "alice",
			jsonBody(t, updateNamespacePropertiesRequest{Removals: []string{"not-there"}}))
		response := decodeUpdateNamespaceProperties(t, rec)
		require.Equal(t, []string{"not-there"}, response.Missing)
		require.Empty(t, response.Updated)
		require.Empty(t, response.Removed)
		require.Contains(t, rec.Body.String(), `"updated":[]`,
			"updated and removed are required arrays of the response model")
	})

	t.Run("an unknown namespace is 404", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost,
			namespacePropertiesTarget(nsTestRepo, nsTestRef, "inventory", "missing"), "alice",
			jsonBody(t, updateNamespacePropertiesRequest{Updates: map[string]string{"a": "b"}}))
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("a key in both updates and removals is 422", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost,
			namespacePropertiesTarget(nsTestRepo, nsTestRef, "inventory"), "alice",
			jsonBody(t, updateNamespacePropertiesRequest{
				Updates:  map[string]string{"team": "other"},
				Removals: []string{"team"},
			}))
		requireEnvelope(t, rec, http.StatusUnprocessableEntity, ErrTypeUnprocessableEntity)

		get := decodeGetNamespace(t, f.request(t, http.MethodGet,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory"), "alice"))
		require.Equal(t, map[string]string{"team": "core"}, get.Properties,
			"a refused update must not change anything")
	})

	t.Run("an update on a tag is 403 ref is read-only", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost,
			namespacePropertiesTarget(nsTestRepo, nsTestTag, "inventory"), "alice",
			jsonBody(t, updateNamespacePropertiesRequest{Updates: map[string]string{"a": "b"}}))
		requireForbiddenReadOnly(t, rec)
	})

	t.Run("a caller without grants is refused", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodPost,
			namespacePropertiesTarget(nsTestRepo, nsTestRef, "inventory"), "bob",
			jsonBody(t, updateNamespacePropertiesRequest{Updates: map[string]string{"a": "b"}}))
		requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	})
}

func testNamespacesWriteDrop(t *testing.T, f *namespacesFixture) {

	t.Run("an empty namespace is dropped", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodDelete,
			namespacesTarget(nsTestRepo, nsTestRef, "other"), "alice", "")
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String())

		get := f.request(t, http.MethodGet, namespacesTarget(nsTestRepo, nsTestRef, "other"), "alice")
		requireEnvelope(t, get, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("a namespace that still holds a table is 409", func(t *testing.T) {
		create := f.writeRequest(t, http.MethodPost, nsWriteCreateTarget, "alice", jsonBody(t, createNamespaceRequest{
			Namespace: []string{nsTestRepo, nsTestRef, "with-a-table"},
		}))
		decodeCreateNamespace(t, create)

		// The table is staged through the store's own create path: the catalog, not the client,
		// has to notice that the namespace is not empty (§11 I-2).
		require.NoError(t, f.store.CreateTable(f.ctx, Address{
			Repo: nsTestRepo, Ref: nsTestRef, Namespace: Namespace{"with-a-table"}, Table: "orders",
		}, TablePointer{
			MetadataLocation: "mem://iceberg-namespaces-ns/_iceberg/table-uuid/metadata/1.json",
			TableUUID:        "table-uuid",
			FormatVersion:    2,
		}), "stage a table pointer under the namespace")

		rec := f.writeRequest(t, http.MethodDelete,
			namespacesTarget(nsTestRepo, nsTestRef, "with-a-table"), "alice", "")
		requireEnvelope(t, rec, http.StatusConflict, ErrTypeNamespaceNotEmpty)

		head := f.request(t, http.MethodHead, namespacesTarget(nsTestRepo, nsTestRef, "with-a-table"), "alice")
		require.Equal(t, http.StatusNoContent, head.Code, "body: %s", head.Body.String())
	})

	t.Run("a namespace with child namespaces is 409", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodDelete,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory"), "alice", "")
		requireEnvelope(t, rec, http.StatusConflict, ErrTypeNamespaceNotEmpty)
	})

	t.Run("an absent namespace is 404", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodDelete,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory", "missing"), "alice", "")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("a drop on a tag is 403 ref is read-only", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodDelete,
			namespacesTarget(nsTestRepo, nsTestTag, "inventory"), "alice", "")
		requireForbiddenReadOnly(t, rec)
	})

	t.Run("a drop on a missing branch is 404 NoSuchWarehouse", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodDelete,
			namespacesTarget(nsTestRepo, "missing-branch", "inventory"), "alice", "")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("a caller without grants is refused", func(t *testing.T) {
		rec := f.writeRequest(t, http.MethodDelete,
			namespacesTarget(nsTestRepo, nsTestRef, "inventory"), "bob", "")
		requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	})
}

// testNamespacesWriteAdvertisedEndpoints pins the §2.1 negotiation promise: once the write
// endpoints are mounted, GET /v1/config advertises them, and it advertises nothing else.
func testNamespacesWriteAdvertisedEndpoints(t *testing.T, f *namespacesFixture) {

	// The read fixture's router is built from the service's route table alone, so this subtest
	// builds the catalog surface the way the production router does: GET /v1/config plus the
	// namespace service's operations.
	router, err := newRouter(namespacesUser, &Config{},
		newServer(&Config{Prefix: CatalogPrefix}, f.serve))
	require.NoError(t, err, "build catalog router")
	req := httptest.NewRequest(http.MethodGet, configPath, nil)
	req.Header.Set("X-Test-User", "alice")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response CatalogConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())

	require.Equal(t, []string{
		listNamespacesEndpoint,
		getNamespaceEndpoint,
		headNamespaceEndpoint,
		createNamespaceEndpoint,
		updateNamespacePropertiesEndpoint,
		dropNamespaceEndpoint,
	}, response.Endpoints, "the advertised list is the mounted route table")
	require.Contains(t, response.Endpoints, createNamespaceEndpoint)
	require.Contains(t, response.Endpoints, updateNamespacePropertiesEndpoint)
	require.Contains(t, response.Endpoints, dropNamespaceEndpoint)
}
