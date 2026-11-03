package icebergcatalog

// Tests for the relative_to addressing forms (ICEBERG_CATALOG_PLAN.md §5 rows 4 and 5, §1.1
// goal 2, §11 I-1 task I1-10, and D-1, D-2 and D-4 of conformance/RESULTS.md §10). Every test name
// starts with TestRelativeTo so that the task's validation command,
// `go test -run TestRelativeTo ./pkg/icebergcatalog/`, runs exactly this file.
//
// Three properties are tested, at three levels:
//
//   - the binding itself, against a stub catalog whose routes are the real read path patterns
//     (TestRelativeToBindsTheWarehouse). The stub proves the rewrite is the /iceberg/api request
//     and nothing else - same levels, same parent, same pagination - and that a warehouse that
//     does not parse never reaches a handler;
//   - the identity of the two forms, end to end and over the real namespace read path of
//     namespaces_test.go, through the router http.go's RoutesBuilder builds
//     (TestRelativeToIsTheSameCatalog). Config negotiation, get, head, the three listing levels,
//     pagination, any-ref reads, the error envelopes and an attempt to smuggle an address across
//     the fixed scope are all answered through both forms. The forms answer the same *address*,
//     not the same bytes: a relative_to client reads back the levels it sent, because the
//     repository and the ref its URL fixed are not its to name (D-4);
//   - the client's round trip through the production mount
//     (TestRelativeToRoundTripThroughTheMountedCatalog): create a namespace, list it, read it,
//     create a table, list it, load it, and drop both, all through
//     /iceberg/relative_to/<warehouse>/api and all in the client's own terms.
//
// The last two drive the routes RoutesBuilder mounts rather than a fixture-built pair of mounts.
// That distinction is the reason this file could be green while D-1 shipped: the fixture used to
// mount RelativeTo itself, so nothing ever asserted that http.go's mount does.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
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

// newBindingStub builds the two mounts RelativeTo has to serve - the default form and the
// relative_to form - around a stub catalog whose routes are the real namespace read patterns. The
// rewrite is therefore exercised the way it works in production: RelativeTo sits in front of the
// router that matches the {namespace} parameter, so what the stub records is what a handler
// records. That the production mount really wraps the handler is asserted separately, against the
// router RoutesBuilder builds (TestRelativeToIsTheSameCatalog, and the round trip test at the end
// of this file): a stub that mounts RelativeTo itself cannot see a missing mount in http.go.
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

// relativeToFixture is the catalog the way a client reaches it: the router RoutesBuilder mounts -
// the same function cmd/lakefs-sso registers through cmd.AddExtraRoutesBuilder - over a real ACL
// auth service, a real catalog and the production middleware stack. Both addressing forms are
// therefore mounted exactly as they are in the binary, which is the point: the previous fixture
// built the two mounts itself, and so it could not see that http.go mounted the bare handler
// instead of RelativeTo(handler) (defect D-1 in conformance/RESULTS.md §10).
type relativeToFixture struct {
	*namespacesFixture
	mux         *chi.Mux
	credentials map[string]relativeToCredential
}

// relativeToCredential is one test principal's access key pair: RoutesBuilder mounts the catalog's
// own authentication (basic or bearer, §7.1), so a test authenticates the way a client does.
type relativeToCredential struct {
	accessKeyID     string
	secretAccessKey string
}

// relativeToUsers are the principals the fixture creates credentials for. alice holds the read half
// of §7.2 (namespacesReadStatements), bob holds nothing, dana may list every repository
// (namespacesAllRepositoriesStatements), and carol holds the write half (authzWriteStatements), so
// the round trip of TestRelativeToRoundTripThroughTheMountedCatalog can create what it then reads.
var relativeToUsers = []string{"alice", "bob", "dana", "carol"}

// newRelativeToFixture builds the namespace read path of namespaces_test.go - the repository, its
// markers and a tag - and mounts the production router over it.
func newRelativeToFixture(t *testing.T) *relativeToFixture {
	t.Helper()
	base := newNamespacesFixture(t)

	base.authz.user(t, "carol")
	base.authz.attachStatements(t, "writer", "carol", authzWriteStatements())

	// RoutesBuilder reads iceberg.enabled through LoadConfig, and a disabled catalog mounts its 501
	// answer instead of the routes.
	useConfig(t, "iceberg:\n  enabled: true\n")

	credentials := make(map[string]relativeToCredential, len(relativeToUsers))
	for i, username := range relativeToUsers {
		credential := relativeToCredential{
			accessKeyID:     fmt.Sprintf("AKIARELATIVETO%04d", i),
			secretAccessKey: fmt.Sprintf("relative-to-test-secret-0000-0000-%04d", i),
		}
		_, err := base.authz.svc.AddCredentials(t.Context(), username,
			credential.accessKeyID, credential.secretAccessKey)
		require.NoErrorf(t, err, "add credentials for %s", username)
		credentials[username] = credential
	}

	kvStore, err := kv.Open(t.Context(), kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open the server's KV store")
	t.Cleanup(kvStore.Close)

	deps := cmdpkg.ServerDeps{
		Config:        &config.ConfigImpl{},
		AuthService:   base.authz.svc,
		Authenticator: auth.NewBuiltinAuthenticator(base.authz.svc),
		Catalog:       base.catalog,
		BlockAdapter:  base.catalog.BlockAdapter,
		KVStore:       kvStore,
		Logger:        logging.ContextUnavailable(),
	}

	mux := coreRouter()
	require.NoError(t, RoutesBuilder(t.Context(), deps, mux), "mount the catalog")

	return &relativeToFixture{namespacesFixture: base, mux: mux, credentials: credentials}
}

// request sends one request to the mounted catalog as user.
func (f *relativeToFixture) request(t *testing.T, method, target, user string) *httptest.ResponseRecorder {
	t.Helper()
	return f.requestBody(t, method, target, user, "")
}

// requestBody is request with a JSON body, which the create of a namespace and of a table need.
func (f *relativeToFixture) requestBody(t *testing.T, method, target, user, body string) *httptest.ResponseRecorder {
	t.Helper()
	credential, ok := f.credentials[user]
	require.Truef(t, ok, "no credentials for user %q", user)

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	req.SetBasicAuth(credential.accessKeyID, credential.secretAccessKey)
	if body != "" {
		req.Header.Set("Content-Type", contentTypeJSON)
	}
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	return rec
}

// clientLevels is the namespace a relative_to client names for a full catalog address: everything
// below the warehouse its URL fixed. It is the un-prefixing the catalog has to apply on the way out
// (defect D-4), written here from ParseWarehouse and the fixed levels rather than by calling the
// production helper, so the test checks the behaviour instead of re-running the code under test.
func clientLevels(t *testing.T, warehouse string, full Namespace) Namespace {
	t.Helper()
	scope, err := ParseWarehouse(warehouse)
	require.NoErrorf(t, err, "parse warehouse %q", warehouse)
	fixed := 1
	if scope.Ref != "" {
		fixed = 2
	}
	require.GreaterOrEqualf(t, len(full), fixed, "address %v is not below %q", full, warehouse)
	require.Equal(t, scope.Repo, full[0], "address %v is not below %q", full, warehouse)
	if scope.Ref != "" {
		require.Equal(t, scope.Ref, full[1], "address %v is not below %q", full, warehouse)
	}
	return full[fixed:]
}

// relativeForm is one of the two relative_to rows of §5 for a client namespace: {repo}.{branch}
// with a namespace-only path, and {repo} with the ref as the first client level.
type relativeForm struct {
	name      string
	warehouse string
	target    string
}

// relativeForms names both relative_to rows of §5 for one client namespace.
func relativeForms(namespace Namespace) []relativeForm {
	return []relativeForm{
		{
			name:      "repo.branch",
			warehouse: nsTestRepo + "." + nsTestRef,
			target: warehouseTarget(nsTestRepo+"."+nsTestRef,
				namespacesTarget(namespace...)),
		},
		{
			name:      "repo",
			warehouse: nsTestRepo,
			target: warehouseTarget(nsTestRepo,
				namespacesTarget(append([]string{nsTestRef}, namespace...)...)),
		},
	}
}

// TestRelativeToIsTheSameCatalog is the I1-10 identity, driven through the router RoutesBuilder
// mounts: both forms reach the same catalog state and answer the same address - in the client's own
// terms, which means the full [repo, ref, ns…] path on /iceberg/api and the levels below the fixed
// warehouse on a relative_to form (D-4).
func TestRelativeToIsTheSameCatalog(t *testing.T) {
	f := newRelativeToFixture(t)

	t.Run("a namespace reads the same through every form", func(t *testing.T) {
		namespace := Namespace{"inventory", "books"}
		direct := f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestRef, namespace)...), "alice")
		directBody := decodeGetNamespace(t, direct)
		require.Equal(t, append([]string{nsTestRepo, nsTestRef}, namespace...), []string(directBody.Namespace),
			"the identifier form answers the full address (RESULTS.md §5, unchanged)")
		require.Equal(t, map[string]string{"owner": "data-team", "comment": "books"}, directBody.Properties)

		for _, form := range relativeForms(namespace) {
			t.Run(form.name, func(t *testing.T) {
				body := decodeGetNamespace(t, f.request(t, http.MethodGet, form.target, "alice"))
				// The client reads back the address it sent: for the {repo} form that address carries
				// the ref as its first level, because the client had to name the ref itself.
				require.Equal(t, []string(clientLevels(t, form.warehouse, directBody.Namespace)),
					[]string(body.Namespace),
					"the client must read back the address it sent, never the levels its URL fixed")
				require.Equal(t, directBody.Properties, body.Properties)
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

		for _, form := range relativeForms(namespace) {
			t.Run(form.name, func(t *testing.T) {
				rec := f.request(t, http.MethodHead, form.target, "alice")
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
				directBody := decodeListNamespaces(t, direct)
				require.Equal(t, tc.want, directBody.Namespaces,
					"the identifier form answers the full addresses (RESULTS.md §5, unchanged)")

				// The same listing, in the terms of a client whose URL fixed the warehouse.
				want := make([]Namespace, 0, len(tc.want))
				for _, full := range tc.want {
					want = append(want, clientLevels(t, tc.warehouse, full))
				}

				rec := f.request(t, http.MethodGet,
					warehouseTarget(tc.warehouse, listTarget(tc.parent, tc.query)), "alice")
				body := decodeListNamespaces(t, rec)
				require.Equal(t, want, body.Namespaces,
					"a relative_to client reads the levels it sent, not the ones its URL fixed")
				require.Equal(t, directBody.NextPageToken, body.NextPageToken,
					"the two forms walk the same listing, so the next page token is the same")
			})
		}
	})

	t.Run("a tag is readable through every form", func(t *testing.T) {
		namespace := Namespace{"inventory", "books"}
		direct := f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestTag, namespace)...), "alice")
		directBody := decodeGetNamespace(t, direct)
		require.Equal(t, []string{nsTestRepo, nsTestTag, "inventory", "books"}, []string(directBody.Namespace))
		require.Equal(t, map[string]string{"owner": "data-team", "comment": "books"}, directBody.Properties)

		for _, form := range []relativeForm{
			{
				name:      "repo.branch",
				warehouse: nsTestRepo + "." + nsTestTag,
				target: warehouseTarget(nsTestRepo+"."+nsTestTag,
					namespacesTarget(namespace...)),
			},
			{
				name:      "repo",
				warehouse: nsTestRepo,
				target: warehouseTarget(nsTestRepo,
					namespacesTarget(append([]string{nsTestTag}, namespace...)...)),
			},
		} {
			t.Run(form.name, func(t *testing.T) {
				body := decodeGetNamespace(t, f.request(t, http.MethodGet, form.target, "alice"))
				require.Equal(t, []string(clientLevels(t, form.warehouse, directBody.Namespace)),
					[]string(body.Namespace))
				require.Equal(t, directBody.Properties, body.Properties)
			})
		}
	})

	t.Run("a missing namespace is the same 404 through every form", func(t *testing.T) {
		namespace := Namespace{"inventory", "missing"}
		direct := f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestRef, namespace)...), "alice")
		requireEnvelope(t, direct, http.StatusNotFound, ErrTypeNoSuchNamespace)

		for _, form := range relativeForms(namespace) {
			t.Run(form.name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, form.target, "alice")
				requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchNamespace)
				// The message names the namespace in the client's own terms on both forms (it is
				// built from the address's namespace levels, never from the macro levels), so the
				// envelopes are identical.
				require.JSONEq(t, direct.Body.String(), rec.Body.String())
			})
		}
	})

	t.Run("a caller without grants is refused the same way through every form", func(t *testing.T) {
		namespace := Namespace{"inventory"}
		direct := f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestRef, namespace)...), "bob")
		requireEnvelope(t, direct, http.StatusForbidden, ErrTypeForbidden)

		for _, form := range relativeForms(namespace) {
			t.Run(form.name, func(t *testing.T) {
				rec := f.request(t, http.MethodGet, form.target, "bob")
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

	t.Run("the warehouse segment is bound, not captured and ignored", func(t *testing.T) {
		// A warehouse that names no repository can only be answered by RelativeTo, which runs in
		// front of the catalog: the bare handler would answer ErrTypeNotFound like any other
		// unknown path. This is the wiring assertion of D-1, and it is why the fixture mounts the
		// production router instead of building the two mounts itself.
		rec := f.request(t, http.MethodGet,
			warehouseTarget("Lakehouse.main", namespacesTarget("inventory")), "alice")
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})
}

// TestRelativeToRoundTripThroughTheMountedCatalog is the acceptance test of D-1, D-2 and D-4: it
// drives the router RoutesBuilder mounts, and performs - through /iceberg/relative_to only - the
// calls conformance/RESULTS.md §4 measured failing 18 of 18 times and §6.2 recorded failing for
// PyIceberg: create a namespace, list it, read it, create a table in it, list the table, load the
// table, update and read its properties, and drop both again.
//
// Every assertion is made from the client's side of the wire, so an answer that carried the
// catalog's internal [repo, ref, ns] address where the client sent [ns] fails the round trip (D-4),
// and every request goes through the relative_to mount, so a catalog that ignores the warehouse
// segment fails at the first call (D-1). The namespace of the create travels in the JSON body, so a
// catalog that rewrites only the path and the `parent` query fails there (D-2).
func TestRelativeToRoundTripThroughTheMountedCatalog(t *testing.T) {
	f := newRelativeToFixture(t)

	cases := []struct {
		name      string
		warehouse string
		namespace []string
		// parent is the listing that names the created namespace's parent in the client's terms:
		// none at all when the URL fixed the ref (it is the parent), and the ref when it did not.
		parent []string
	}{
		{name: "repo.branch", warehouse: nsTestRepo + "." + nsTestRef, namespace: []string{"round-trip-branch"}},
		{name: "repo", warehouse: nsTestRepo, namespace: []string{nsTestRef, "round-trip-repo"},
			parent: []string{nsTestRef}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			warehouse := func(catalogPath string) string {
				return warehouseTarget(tc.warehouse, catalogPath)
			}
			namespace := Namespace(tc.namespace)
			table := "orders"

			// A relative_to client's namespace parameter and table targets, in its own terms.
			namespacePath := func() string { return namespacesTarget(tc.namespace...) }
			listTablesPath := func() string { return tablesListTarget(nodes(tc.namespace...), nil) }
			loadTablePath := func() string {
				return tablesLoadTarget(nodes(tc.namespace...), table, nil)
			}

			// 1. CreateNamespace, with the address in the body (D-2).
			createNamespace, err := json.Marshal(createNamespaceRequest{
				Namespace:  tc.namespace,
				Properties: map[string]string{"owner": "round-trip"},
			})
			require.NoError(t, err, "encode the create request")
			rec := f.requestBody(t, http.MethodPost, warehouse("/v1/"+CatalogPrefix+"/namespaces"),
				"carol", string(createNamespace))
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			var created createNamespaceResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created), "body: %s", rec.Body.String())
			require.Equal(t, []string(namespace), []string(created.Namespace),
				"the create answers the namespace the client sent, not [repo, ref, ns…]")
			require.Equal(t, map[string]string{"owner": "round-trip"}, created.Properties)

			// 2. ListNamespaces: the listing below the parent the client named contains the client's
			// own name, and no entry carries a level the URL fixed (this is D-1's "no parent must
			// list the ref's namespaces, not the repositories" half too).
			listed := decodeListNamespaces(t, f.request(t, http.MethodGet,
				warehouse(listTarget(tc.parent, nil)), "alice"))
			require.Contains(t, listed.Namespaces, namespace,
				"the namespace the client created must be listed under the name it used")
			for _, entry := range listed.Namespaces {
				require.NotEmpty(t, entry)
				require.NotEqual(t, nsTestRepo, entry[0],
					"the repository is fixed by the URL, so no listing entry may carry it: %v", entry)
			}

			// 3. GetNamespace and NamespaceExists.
			read := decodeGetNamespace(t, f.request(t, http.MethodGet, warehouse(namespacePath()), "alice"))
			require.Equal(t, []string(namespace), []string(read.Namespace))
			require.Equal(t, map[string]string{"owner": "round-trip"}, read.Properties)
			require.Equal(t, http.StatusNoContent,
				f.request(t, http.MethodHead, warehouse(namespacePath()), "alice").Code,
				"the namespace exists")

			// 4. UpdateProperties, and the update is visible on the next read.
			properties, err := json.Marshal(updateNamespacePropertiesRequest{
				Updates: map[string]string{"stage": "round-trip"},
			})
			require.NoError(t, err, "encode the properties request")
			rec = f.requestBody(t, http.MethodPost,
				warehouse(namespacePath()+"/properties"), "carol", string(properties))
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			require.Equal(t, map[string]string{"owner": "round-trip", "stage": "round-trip"},
				decodeGetNamespace(t, f.request(t, http.MethodGet, warehouse(namespacePath()), "alice")).Properties)

			// 5. CreateTable: the table is named in the body, the namespace in the client's path.
			rec = f.requestBody(t, http.MethodPost, warehouse(listTablesPath()), "carol",
				createTableBody(table, createTableSchema))
			createdTable := decodeLoadTable(t, rec)
			require.NotEmpty(t, createdTable.MetadataLocation)

			// 6. ListTables and LoadTable: the identifier the client sent comes back unchanged.
			tables := decodeListTables(t, f.request(t, http.MethodGet, warehouse(listTablesPath()), "alice"))
			require.Equal(t, []string{table}, tableNamesOf(tables))
			for _, identifier := range tables.Identifiers {
				require.Equal(t, []string(namespace), []string(identifier.Namespace),
					"a table identifier carries the client's namespace, not [repo, ref, ns…]")
			}
			loaded := decodeLoadTable(t, f.request(t, http.MethodGet, warehouse(loadTablePath()), "alice"))
			require.Equal(t, createdTable.MetadataLocation, loaded.MetadataLocation,
				"the table loaded through the relative_to URI is the table that was created through it")

			// 7. DropNamespace refuses while the table is there - it addresses the same namespace -
			// and both drops then succeed, which is the cleanup a client performs for itself.
			requireEnvelope(t, f.request(t, http.MethodDelete, warehouse(namespacePath()), "carol"),
				http.StatusConflict, ErrTypeNamespaceNotEmpty)
			require.Equal(t, http.StatusNoContent,
				f.request(t, http.MethodDelete, warehouse(loadTablePath()), "carol").Code,
				"drop the table")
			require.Equal(t, http.StatusNoContent,
				f.request(t, http.MethodDelete, warehouse(namespacePath()), "carol").Code,
				"drop the namespace")
			requireEnvelope(t, f.request(t, http.MethodGet, warehouse(namespacePath()), "alice"),
				http.StatusNotFound, ErrTypeNoSuchNamespace)
		})
	}

	// The identifier form is measured working in conformance/RESULTS.md §5 and must not regress:
	// it still carries the repository and the ref in the client's own path, and answers with them.
	t.Run("the identifier form still answers with the full address", func(t *testing.T) {
		namespace := Namespace{"inventory", "books"}
		read := decodeGetNamespace(t, f.request(t, http.MethodGet,
			APIBasePath+namespacesTarget(addressLevels(nsTestRepo, nsTestRef, namespace)...), "alice"))
		require.Equal(t, []string{nsTestRepo, nsTestRef, "inventory", "books"}, []string(read.Namespace))

		listed := decodeListNamespaces(t, f.request(t, http.MethodGet,
			APIBasePath+listTarget([]string{nsTestRepo, nsTestRef}, nil), "alice"))
		require.NotEmpty(t, listed.Namespaces)
		for _, entry := range listed.Namespaces {
			require.Equal(t, []string{nsTestRepo, nsTestRef}, []string(entry[:2]),
				"the identifier form lists full [repo, ref, ns…] addresses")
		}

		tables := decodeListTables(t, f.request(t, http.MethodGet,
			APIBasePath+tablesListTarget(nodes(nsTestRepo, nsTestRef, "inventory", "books"), nil), "alice"))
		for _, identifier := range tables.Identifiers {
			require.Equal(t, []string{nsTestRepo, nsTestRef, "inventory", "books"},
				[]string(identifier.Namespace))
		}
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
