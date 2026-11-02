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
	"github.com/treeverse/lakefs/pkg/permissions"
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
// root level of §5 needs. The fs:ReadRepository grant is scoped to authzRepoArn, which is what
// makes the root listing hide every other repository in the fixture.
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

// namespacesAllRepositoriesStatements grants the catalog read actions and fs:ReadRepository on
// every repository, so a repository that does not exist is reached by the lookup instead of being
// refused at the authorization gate.
func namespacesAllRepositoriesStatements() model.Statements {
	return model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction, permissions.ListRepositoriesAction},
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
	base := newStoreFixture(t)
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
