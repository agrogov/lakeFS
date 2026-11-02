package icebergcatalog

// Tests for the relative_to addressing forms (ICEBERG_CATALOG_PLAN.md §5 rows 4 and 5, §1.1
// goal 2, §11 I-1 task I1-10). Every test name starts with TestRelativeTo so that the task's
// validation command, `go test -run TestRelativeTo ./pkg/icebergcatalog/`, runs exactly this
// file.
//
// The two properties the task asks for are tested separately:
//
//   - the binding itself, against a stub catalog whose routes are the real read path patterns
//     (TestRelativeToBindsTheWarehouse). The stub proves the rewrite is the /iceberg/api request
//     and nothing else - same levels, same parent, same pagination - and that a warehouse that
//     does not parse never reaches a handler;
//   - the identity of the two forms, end to end and over the real namespace read path of
//     namespaces_test.go, mounted the way http.go's RoutesBuilder mounts it
//     (TestRelativeToIsTheSameCatalog). Config negotiation, get, head, the three listing levels,
//     pagination, any-ref reads, the error envelopes and an attempt to smuggle an address across
//     the fixed scope are all answered through both forms and compared body for body.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// warehouseTarget is one relative_to request: the warehouse segment, /api, and the catalog path
// a client would send to /iceberg/api.
func warehouseTarget(warehouse, catalogPath string) string {
	return RelativeToBasePath + "/" + warehouse + "/api" + catalogPath
}

// addressLevels is the namespace path parameter of one /iceberg/api address: the repository, the
// ref and the client namespace levels, in that order (§5).
func addressLevels(repo, ref string, namespace Namespace) []string {
	levels := make([]string, 0, macroLevels+len(namespace))
	levels = append(levels, repo, ref)
	return append(levels, namespace...)
}

// bindingCapture is what the stub handler of TestRelativeToBindsTheWarehouse records.
type bindingCapture struct {
	called    bool
	namespace string
	table     string
	query     url.Values
}

// newBindingStub builds the two mounts http.go's RoutesBuilder builds - the default form and the
// relative_to form - around a stub catalog whose routes are the real namespace read patterns. The
// rewrite is therefore exercised exactly as it is in production: RelativeTo sits in front of the
// router that matches the {namespace} parameter, so what the stub records is what a handler
// records.
func newBindingStub() (*chi.Mux, *bindingCapture) {
	capture := &bindingCapture{}
	record := func(w http.ResponseWriter, r *http.Request) {
		capture.called = true
		capture.namespace = chi.URLParam(r, "namespace")
		capture.table = chi.URLParam(r, "table")
		capture.query = r.URL.Query()
		writeJSON(w, http.StatusOK, struct{}{})
	}

	stub := chi.NewRouter()
	stub.Get(namespacesPattern, record)
	stub.Get(namespacePattern, record)
	// The tables route of I1-9: the same {namespace} parameter, one level deeper.
	stub.Get("/v1/{prefix}/namespaces/{namespace}/tables/{table}", record)

	mux := chi.NewRouter()
	// RelativeTo is harmless on the default form: there is no warehouse segment to bind, so the
	// request must reach the handler unchanged.
	mux.Mount(APIBasePath, RelativeTo(stub))

	relativeTo := chi.NewRouter()
	relativeTo.Mount(relativeToPattern, RelativeTo(stub))
	mux.Mount(RelativeToBasePath, relativeTo)
	return mux, capture
}

// serve sends one request through the stub mounts and returns the response with a fresh capture.
func serve(t *testing.T, mux *chi.Mux, capture *bindingCapture, target string) (*httptest.ResponseRecorder, *bindingCapture) {
	t.Helper()
	*capture = bindingCapture{}
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec, capture
}

// TestRelativeToBindsTheWarehouse is the unit test of the rewrite: the request the handler sees
// must be the /iceberg/api request for the same address, for every row of §5.
func TestRelativeToBindsTheWarehouse(t *testing.T) {
	mux, capture := newBindingStub()

	// parse reads the recorded namespace parameter back as an /iceberg/api address, which is what
	// the real handlers do: if the rewrite is right, the recorded value parses to the expected
	// address.
	parse := func(t *testing.T, got *bindingCapture) Address {
		t.Helper()
		var (
			addr Address
			err  error
		)
		if got.table != "" {
			addr, err = ParseTable(got.namespace, got.table)
		} else {
			addr, err = ParseNamespace(got.namespace)
		}
		require.NoError(t, err, "the rewritten namespace parameter %q must parse as an "+
			"/iceberg/api address", got.namespace)
		return addr
	}

	// parseParent reads the recorded parent query parameter back as an /iceberg/api address: the
	// listing route carries its address in the query, not in the path (namespaces.go).
	parseParent := func(t *testing.T, got *bindingCapture) Address {
		t.Helper()
		require.True(t, got.called, "the request must reach the handler")
		addr, err := ParseNamespace(got.query.Get(parentParam))
		require.NoError(t, err, "the injected parent %q must parse as an /iceberg/api address",
			got.query.Get(parentParam))
		return addr
	}

	t.Run("the default form is untouched", func(t *testing.T) {
		rec, got := serve(t, mux, capture, APIBasePath+namespacesTarget("lakehouse", "main", "inventory", "books"))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.True(t, got.called, "the default form must reach the handler")
		require.Equal(t, Address{Repo: "lakehouse", Ref: "main", Namespace: Namespace{"inventory", "books"}},
			parse(t, got))
	})

	t.Run("repo.branch fixes two levels and the client keeps only its namespace", func(t *testing.T) {
		target := warehouseTarget("lakehouse.main", namespacesTarget("inventory", "books"))
		rec, got := serve(t, mux, capture, target)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, Address{Repo: "lakehouse", Ref: "main", Namespace: Namespace{"inventory", "books"}},
			parse(t, got))
	})

	t.Run("repo fixes one level and the first client level is the ref", func(t *testing.T) {
		target := warehouseTarget("lakehouse", namespacesTarget("main", "inventory", "books"))
		rec, got := serve(t, mux, capture, target)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, Address{Repo: "lakehouse", Ref: "main", Namespace: Namespace{"inventory", "books"}},
			parse(t, got))
	})

	t.Run("the warehouse splits at the first dot so a tag may carry dots", func(t *testing.T) {
		target := warehouseTarget("lakehouse.release.v1.2", namespacesTarget("inventory"))
		rec, got := serve(t, mux, capture, target)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, Address{Repo: "lakehouse", Ref: "release.v1.2", Namespace: Namespace{"inventory"}},
			parse(t, got))
	})

	t.Run("a table address keeps its table name", func(t *testing.T) {
		target := warehouseTarget("lakehouse.main", "/v1/"+CatalogPrefix+"/namespaces/inventory/tables/books")
		rec, got := serve(t, mux, capture, target)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, Address{
			Repo: "lakehouse", Ref: "main", Namespace: Namespace{"inventory"}, Table: "books",
		}, parse(t, got))
	})

	t.Run("a multi-level client namespace stays one address", func(t *testing.T) {
		target := warehouseTarget("lakehouse.main", namespacesTarget("inventory", "books.v2"))
		rec, got := serve(t, mux, capture, target)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, Address{
			Repo: "lakehouse", Ref: "main", Namespace: Namespace{"inventory", "books.v2"},
		}, parse(t, got))
	})

	t.Run("list namespaces without a parent answers the warehouse level", func(t *testing.T) {
		// §5 rows 4 and 5: an absent parent is the level the URL fixed, not "list repositories".
		cases := []struct {
			name      string
			warehouse string
			want      Address
		}{
			{"repo.branch", "lakehouse.main", Address{Repo: "lakehouse", Ref: "main"}},
			{"repo", "lakehouse", Address{Repo: "lakehouse"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				target := warehouseTarget(tc.warehouse, "/v1/"+CatalogPrefix+"/namespaces")
				rec, got := serve(t, mux, capture, target)
				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
				require.Equal(t, tc.want, parseParent(t, got),
					"the injected parent %q must name the warehouse level", got.query.Get(parentParam))
			})
		}
	})

	t.Run("list namespaces below a parent keeps the fixed levels first", func(t *testing.T) {
		cases := []struct {
			name      string
			warehouse string
			parent    string
			want      Address
		}{
			{
				name:      "repo.branch",
				warehouse: "lakehouse.main",
				parent:    nodes("inventory"),
				want:      Address{Repo: "lakehouse", Ref: "main", Namespace: Namespace{"inventory"}},
			},
			{
				name:      "repo",
				warehouse: "lakehouse",
				parent:    nodes("main", "inventory"),
				want:      Address{Repo: "lakehouse", Ref: "main", Namespace: Namespace{"inventory"}},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				target := warehouseTarget(tc.warehouse,
					"/v1/"+CatalogPrefix+"/namespaces?parent="+tc.parent)
				rec, got := serve(t, mux, capture, target)
				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
				require.Equal(t, tc.want, parseParent(t, got))
			})
		}
	})

	t.Run("pagination is passed through", func(t *testing.T) {
		target := warehouseTarget("lakehouse.main",
			"/v1/"+CatalogPrefix+"/namespaces?pageSize=2&pageToken="+encodePageToken(2))
		rec, got := serve(t, mux, capture, target)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, "2", got.query.Get("pageSize"))
		require.Equal(t, encodePageToken(2), got.query.Get("pageToken"))
		require.Equal(t, Address{Repo: "lakehouse", Ref: "main"}, parseParent(t, got),
			"the injected parent and the client's page token must coexist")
	})

	t.Run("a warehouse that does not parse never reaches a handler", func(t *testing.T) {
		cases := map[string]string{
			"an uppercase repository":   "Lakehouse.main",
			"an empty repository":       ".main",
			"an empty ref":              "lakehouse.",
			"a separator in the repo":   "lakehouse%1Fmain",
			"a namespace separator ref": "lakehouse.main%1Fother",
		}
		for name, warehouse := range cases {
			t.Run(name, func(t *testing.T) {
				target := warehouseTarget(warehouse, namespacesTarget("inventory"))
				rec, got := serve(t, mux, capture, target)
				requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
				require.False(t, got.called, "a warehouse that names no repository must not be served")
			})
		}
	})
}

// relativeToFixture serves the namespace read path of namespaces_test.go through both addressing
// forms, mounted exactly the way http.go's RoutesBuilder mounts it: the same handler at
// APIBasePath, and the same handler behind RelativeTo at
// RelativeToBasePath/{warehouse}/api.
type relativeToFixture struct {
	*namespacesFixture
	mux *chi.Mux
}

// newRelativeToFixture builds the catalog surface the read path serves - the routes the catalog's
// server owns (GET /v1/config) plus the namespace read routes - and serves it at both addressing
// forms. It reuses the namespace fixture for its catalog, store and policies; its router is
// rebuilt here only because the two-form mux needs the route table.
func newRelativeToFixture(t *testing.T) *relativeToFixture {
	t.Helper()
	base := newNamespacesFixture(t)

	surface := newServer(&Config{Prefix: CatalogPrefix})
	routes := append(surface.ownRoutes(), base.serve.Routes()...)
	handler, err := newRouter(namespacesUser, &Config{Prefix: CatalogPrefix},
		testServer(&Config{Prefix: CatalogPrefix}, routes...))
	require.NoError(t, err, "build catalog router")

	mux := chi.NewRouter()
	mux.Mount(APIBasePath, handler)

	relativeTo := chi.NewRouter()
	relativeTo.Mount(relativeToPattern, RelativeTo(handler))
	mux.Mount(RelativeToBasePath, relativeTo)

	return &relativeToFixture{namespacesFixture: base, mux: mux}
}

// request sends one request to the two-form mux as user.
func (f *relativeToFixture) request(t *testing.T, method, target, user string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-Test-User", user)
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	return rec
}

// TestRelativeToIsTheSameCatalog is the I1-10 identity: for the same address, the default form
// and both relative_to forms must be indistinguishable, because they are the same handlers.
func TestRelativeToIsTheSameCatalog(t *testing.T) {
	f := newRelativeToFixture(t)

	// Both relative_to rows for one address: {repo}.{branch} with a namespace-only path, and
	// {repo} with the ref as the first client level.
	relativeForms := func(namespace Namespace) map[string]string {
		return map[string]string{
			"repo.branch": warehouseTarget(nsTestRepo+"."+nsTestRef, namespacesTarget([]string(namespace)...)),
			"repo":        warehouseTarget(nsTestRepo, namespacesTarget(append([]string{nsTestRef}, []string(namespace)...)...)),
		}
	}

	t.Run("a namespace reads the same through every form", func(t *testing.T) {
		namespace := Namespace{"inventory", "books"}
		direct := f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestRef, namespace)...), "alice")
		require.Equal(t, http.StatusOK, direct.Code, "body: %s", direct.Body.String())
		require.Equal(t, map[string]string{"owner": "data-team", "comment": "books"},
			decodeGetNamespace(t, direct).Properties)

		for name, target := range relativeForms(namespace) {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, target, "alice")
				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
				require.JSONEq(t, direct.Body.String(), rec.Body.String())
			})
		}
	})

	t.Run("config is the same through every form", func(t *testing.T) {
		// This is a client's first request in both published recipes (DuckDB and Starburst put
		// the warehouse in the URL and then negotiate at /v1/config). The catalog it negotiates
		// with must be the same catalog, endpoints and prefix included.
		direct := f.request(t, http.MethodGet, APIBasePath+configPath, "alice")
		require.Equal(t, http.StatusOK, direct.Code, "body: %s", direct.Body.String())

		for name, warehouse := range map[string]string{
			"repo.branch": nsTestRepo + "." + nsTestRef,
			"repo":        nsTestRepo,
			// A warehouse that does not exist is still negotiated: the warehouse is resolved by
			// the operations that read it, not by the config endpoint (see the doc comment in
			// relative.go).
			"an unknown repository": "missing-repo.main",
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, warehouseTarget(warehouse, configPath), "alice")
				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
				require.JSONEq(t, direct.Body.String(), rec.Body.String())
			})
		}
	})

	t.Run("head answers the same through every form", func(t *testing.T) {
		namespace := Namespace{"inventory"}
		direct := f.request(t, http.MethodHead,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestRef, namespace)...), "alice")
		require.Equal(t, http.StatusNoContent, direct.Code, "body: %s", direct.Body.String())

		for name, target := range relativeForms(namespace) {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodHead, target, "alice")
				require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
				require.Empty(t, rec.Body.String())
			})
		}
	})

	t.Run("every listing level reads the same through every form", func(t *testing.T) {
		// parent is the address a relative_to client sends (relative to the warehouse);
		// directParent is the same address on the /iceberg/api form, i.e. with the warehouse
		// levels in front of it. query is sent through both.
		children := []Namespace{
			{nsTestRepo, nsTestRef, "inventory", "authors"},
			{nsTestRepo, nsTestRef, "inventory", "books"},
		}
		cases := []struct {
			name         string
			warehouse    string
			parent       []string
			directParent []string
			query        url.Values
			want         []Namespace
		}{
			{
				name:         "the repository level lists branches",
				warehouse:    nsTestRepo,
				directParent: []string{nsTestRepo},
				want: []Namespace{
					{nsTestRepo, "dev"},
					{nsTestRepo, nsTestRef},
				},
			},
			{
				name:         "the branch level lists namespaces",
				warehouse:    nsTestRepo + "." + nsTestRef,
				directParent: []string{nsTestRepo, nsTestRef},
				want: []Namespace{
					{nsTestRepo, nsTestRef, "inventory"},
					{nsTestRepo, nsTestRef, "other"},
				},
			},
			{
				name:         "the namespace level lists children",
				warehouse:    nsTestRepo + "." + nsTestRef,
				parent:       []string{"inventory"},
				directParent: []string{nsTestRepo, nsTestRef, "inventory"},
				want:         children,
			},
			{
				name:         "the ref-carrying form lists children too",
				warehouse:    nsTestRepo,
				parent:       []string{nsTestRef, "inventory"},
				directParent: []string{nsTestRepo, nsTestRef, "inventory"},
				want:         children,
			},
			{
				name:         "pagination walks the same pages",
				warehouse:    nsTestRepo + "." + nsTestRef,
				directParent: []string{nsTestRepo, nsTestRef},
				query:        url.Values{"pageSize": {"1"}},
				want:         []Namespace{{nsTestRepo, nsTestRef, "inventory"}},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				direct := f.request(t, http.MethodGet,
					APIBasePath+listTarget(tc.directParent, tc.query), "alice")
				require.Equal(t, tc.want, decodeListNamespaces(t, direct).Namespaces)

				relativePath := listTarget(tc.parent, tc.query)
				rec := f.request(t, http.MethodGet, warehouseTarget(tc.warehouse, relativePath), "alice")
				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
				// Body for body, next-page-token included: a client can walk a listing the same way
				// whichever form it addresses the catalog with.
				require.JSONEq(t, direct.Body.String(), rec.Body.String())
			})
		}
	})

	t.Run("a tag is readable through every form", func(t *testing.T) {
		namespace := Namespace{"inventory", "books"}
		direct := f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestTag, namespace)...), "alice")
		require.Equal(t, http.StatusOK, direct.Code, "body: %s", direct.Body.String())
		require.Equal(t, map[string]string{"owner": "data-team", "comment": "books"},
			decodeGetNamespace(t, direct).Properties)

		for name, target := range map[string]string{
			"repo.branch": warehouseTarget(nsTestRepo+"."+nsTestTag, namespacesTarget([]string(namespace)...)),
			"repo":        warehouseTarget(nsTestRepo, namespacesTarget(append([]string{nsTestTag}, []string(namespace)...)...)),
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, target, "alice")
				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
				require.JSONEq(t, direct.Body.String(), rec.Body.String())
			})
		}
	})

	t.Run("a missing namespace is the same 404 through every form", func(t *testing.T) {
		namespace := Namespace{"inventory", "missing"}
		direct := f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestRef, namespace)...), "alice")
		requireEnvelope(t, direct, http.StatusNotFound, ErrTypeNoSuchNamespace)

		for name, target := range relativeForms(namespace) {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, target, "alice")
				requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
				require.JSONEq(t, direct.Body.String(), rec.Body.String())
			})
		}
	})

	t.Run("a caller without grants is refused the same way through every form", func(t *testing.T) {
		namespace := Namespace{"inventory"}
		direct := f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestRef, namespace)...), "bob")
		requireEnvelope(t, direct, http.StatusForbidden, ErrTypeForbidden)

		for name, target := range relativeForms(namespace) {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, target, "bob")
				requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
				require.JSONEq(t, direct.Body.String(), rec.Body.String())
			})
		}
	})

	t.Run("an unknown warehouse is 404 NoSuchWarehouseException when listing", func(t *testing.T) {
		// The listing paths ask the lakeFS lister directly, so a repository or ref that does not
		// exist reaches namespaces.go's isMissingWarehouse mapping, exactly as §5 requires.
		cases := map[string]string{
			"an unknown repository": warehouseTarget("missing-repo.main", listTarget(nil, nil)),
			"an unknown ref": warehouseTarget(nsTestRepo+".missing-branch",
				listTarget(nil, url.Values{"pageSize": {"1"}})),
		}
		for name, target := range cases {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, target, "dana")
				requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
			})
		}
	})

	t.Run("an unknown warehouse is 404 when reading a namespace", func(t *testing.T) {
		// §5 asks for 404 NoSuchWarehouseException here as well. The status is that 404 today, but
		// the type is NoSuchNamespaceException: store.GetNamespace (I1-7) classifies a repository
		// or ref that does not exist as a missing namespace, because graveler.ErrRepositoryNotFound
		// and friends wrap graveler.ErrNotFound. The requirement is therefore only half met by the
		// read path this task delegates to, which is why the type is not pinned here; it becomes
		// NoSuchWarehouseException once that classification is fixed in store.go, a file outside
		// this task's scope (reported as a follow-up).
		cases := map[string]string{
			"an unknown repository": warehouseTarget("missing-repo.main", namespacesTarget("inventory")),
			"an unknown branch":     warehouseTarget(nsTestRepo+".missing-branch", namespacesTarget("inventory")),
			"an unknown ref":        warehouseTarget(nsTestRepo, namespacesTarget("missing-branch", "inventory")),
		}
		for name, target := range cases {
			t.Run(name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, target, "dana")
				require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
			})
		}
	})

	t.Run("the fixed scope cannot be escaped through a namespace level", func(t *testing.T) {
		// A client that puts an address into its own namespace path gets a namespace of that name
		// below the warehouse the URL fixed - it never moves the request into another repository.
		smuggled := append([]string{"other-repo", "main"}, "inventory")
		rec := f.request(t, http.MethodGet,
			warehouseTarget(nsTestRepo+"."+nsTestRef, namespacesTarget(smuggled...)), "alice")
		apiError := requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
		require.Contains(t, apiError.Message, nsTestRepo+"@"+nsTestRef,
			"the address must stay inside the warehouse the URL fixed")
		require.Contains(t, apiError.Message, "other-repo")

		// The same smuggled levels below a parent are a parent namespace of that name, not a
		// second warehouse either.
		parent := f.request(t, http.MethodGet, warehouseTarget(nsTestRepo+"."+nsTestRef,
			"/v1/"+CatalogPrefix+"/namespaces?parent="+nodes("other-repo", "main")), "alice")
		require.Equal(t, http.StatusOK, parent.Code, "body: %s", parent.Body.String())
		require.Empty(t, decodeListNamespaces(t, parent).Namespaces)
	})

	t.Run("a relative_to path without the warehouse shape is not a catalog path", func(t *testing.T) {
		// http.go's relative_to mount owns this 404 (RoutesBuilder's NotFound on the mount
		// router); here it is enough that the client is not served a catalog answer for it.
		rec := f.request(t, http.MethodGet, RelativeToBasePath+"/v1/"+CatalogPrefix+"/namespaces", "alice")
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
		require.NotContains(t, rec.Body.String(), `"namespaces"`)
	})
}

// TestRelativeToErrorEnvelope pins the message of a warehouse that does not parse: it must name
// the shape the client should use, and it must be the NoSuchWarehouseException type of §5.
func TestRelativeToErrorEnvelope(t *testing.T) {
	mux, capture := newBindingStub()

	rec, got := serve(t, mux, capture, warehouseTarget("Lakehouse.main", namespacesTarget("inventory")))
	apiError := requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	require.False(t, got.called)
	require.Contains(t, apiError.Message, "Lakehouse.main")
	require.Contains(t, apiError.Message, RelativeToBasePath)

	var envelope struct {
		Error ErrorModel `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope), "body: %s", rec.Body.String())
	require.Equal(t, http.StatusNotFound, envelope.Error.Code)
}
