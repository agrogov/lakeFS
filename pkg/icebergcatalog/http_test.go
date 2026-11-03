package icebergcatalog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
)

// The tests of this file pin the two things every other I-1/I-2 handler task builds on: the
// order of the middleware stack (authentication before any request or body validation, §12 and
// I1-3) and the Iceberg error envelope with the status mapping of §2.1.

const (
	testAccessKeyID     = "AKIAICEBERGTEST0001"
	testSecretAccessKey = "iceberg-test-secret-access-key-0123456789"
	testUsername        = "iceberg-tester"
)

// authenticated is an authentication middleware stand-in: it authenticates every request as
// testUsername, so a test can reach the handlers without a full auth service. The real
// middleware, and the envelope of its rejections, are covered by TestRoutesBuilderEnabled.
func authenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), &model.User{Username: testUsername})))
	})
}

// testServer builds a catalog surface around an explicit route table, the way newServer does
// for the real one. It is how a test puts an operation that decodes a body - or one that
// advertises a §1.2 non-goal - in front of the production middleware stack.
func testServer(cfg *Config, routes ...route) *server {
	c := &server{cfg: cfg, routes: routes}
	c.endpoints = advertisedEndpoints(routes)
	return c
}

// echoRequest is the body the test route decodes.
type echoRequest struct {
	Name string `json:"name"`
}

// echoHandler is a write-shaped handler: it reads the body, so it is where an oversized body
// (413) and a malformed one (400) are observed.
func echoHandler(w http.ResponseWriter, r *http.Request) {
	var body echoRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}
	writeJSON(w, http.StatusOK, body)
}

// requireEnvelope asserts that rec carries the Iceberg error envelope of wantStatus and
// wantType: the status, the JSON content type, and a body whose type and code match the status.
func requireEnvelope(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantType string) ErrorModel {
	t.Helper()
	require.Equal(t, wantStatus, rec.Code, "body: %s", rec.Body.String())
	require.Equal(t, contentTypeJSON, rec.Header().Get("Content-Type"))
	var response ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	require.Equal(t, wantType, response.Error.Type, "body: %s", rec.Body.String())
	require.Equal(t, wantStatus, response.Error.Code)
	require.NotEmpty(t, response.Error.Message)
	return response.Error
}

// TestErrorEnvelopeMappings pins the status-to-type mapping of §2.1 and §12. Every status the
// catalog can answer with renders the same envelope, with its code equal to the HTTP status: 400
// and 413 for a bad request, 401 and 403 for an authentication or authorization failure, 404 for
// an unknown operation or resource, 409 for a lost commit, 501 for a disabled catalog, and 500
// for anything unexpected.
func TestErrorEnvelopeMappings(t *testing.T) {
	for status, wantType := range map[int]string{
		http.StatusBadRequest:            ErrTypeBadRequest,
		http.StatusUnauthorized:          ErrTypeNotAuthorized,
		http.StatusForbidden:             ErrTypeForbidden,
		http.StatusNotFound:              ErrTypeNotFound,
		http.StatusConflict:              ErrTypeCommitFailed,
		http.StatusRequestEntityTooLarge: ErrTypeRequestTooLarge,
		http.StatusNotImplemented:        ErrTypeNotImplemented,
		http.StatusInternalServerError:   ErrTypeInternalServerError,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeIcebergError(rec, status, "", "")
			requireEnvelope(t, rec, status, wantType)
		})
	}

	t.Run("an explicit type wins over the default", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeIcebergError(rec, http.StatusNotFound, ErrTypeNoSuchWarehouse, "no such warehouse")
		apiError := requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
		require.Equal(t, "no such warehouse", apiError.Message)
	})
}

// TestRouterAuthenticatesBeforeReadingTheBody is the middleware-order requirement of I1-3: a
// request that carries no principal is answered 401 even when its body is over the configured
// limit and malformed, so the server never parses a body it will reject. The core API builds the
// opposite stack (pkg/api/serve.go: the spec validator runs before AuthMiddleware); the catalog
// does not.
func TestRouterAuthenticatesBeforeReadingTheBody(t *testing.T) {
	cfg := &Config{MaxRequestBody: 16}
	router, err := newRouter(nil, cfg, testServer(cfg,
		route{method: http.MethodPost, pattern: "/v1/echo", handler: echoHandler}))
	require.NoError(t, err)

	oversized := `{"name":"` + strings.Repeat("x", 1024) + `"}`

	t.Run("oversized body is not read", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/echo", strings.NewReader(oversized))
		router.ServeHTTP(rec, req)
		requireEnvelope(t, rec, http.StatusUnauthorized, ErrTypeNotAuthorized)
		require.NotContains(t, rec.Body.String(), "limit", "the body was measured before the caller was authenticated")
	})

	t.Run("malformed body is not parsed", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/echo", strings.NewReader(`{"name":`))
		router.ServeHTTP(rec, req)
		requireEnvelope(t, rec, http.StatusUnauthorized, ErrTypeNotAuthorized)
		require.NotContains(t, rec.Body.String(), "malformed", "the body was parsed before the caller was authenticated")
	})
}

// TestRouterRejectsOversizedBody covers the 413 mapping of §12: once the caller is
// authenticated, a body over iceberg.max_request_body is refused with RequestTooLargeException
// and the configured limit in the message.
func TestRouterRejectsOversizedBody(t *testing.T) {
	cfg := &Config{MaxRequestBody: 16}
	router, err := newRouter(authenticated, cfg, testServer(cfg,
		route{method: http.MethodPost, pattern: "/v1/echo", handler: echoHandler}))
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/echo",
		strings.NewReader(`{"name":"`+strings.Repeat("x", 1024)+`"}`))
	router.ServeHTTP(rec, req)

	apiError := requireEnvelope(t, rec, http.StatusRequestEntityTooLarge, ErrTypeRequestTooLarge)
	require.Contains(t, apiError.Message, "16", "the message names the configured limit")
}

// TestRouterRejectsMalformedBody covers the 400 mapping once the caller is authenticated.
func TestRouterRejectsMalformedBody(t *testing.T) {
	cfg := &Config{MaxRequestBody: 1024}
	router, err := newRouter(authenticated, cfg, testServer(cfg,
		route{method: http.MethodPost, pattern: "/v1/echo", handler: echoHandler}))
	require.NoError(t, err)

	for name, body := range map[string]string{
		"truncated JSON": `{"name":`,
		"empty body":     "",
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/echo", strings.NewReader(body))
			router.ServeHTTP(rec, req)
			requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		})
	}
}

// TestRouterEnvelopesUnknownPathAndMethod pins the 404 mapping: chi's plain-text "404 page not
// found" and its bare 405 are not parseable by an Iceberg client, so the catalog answers both in
// the envelope. Routing is request validation, so both answers come after authentication: an
// anonymous caller learns nothing about which operations exist (401), which is the same order
// the rest of the stack keeps.
func TestRouterEnvelopesUnknownPathAndMethod(t *testing.T) {
	cfg := &Config{MaxRequestBody: 1024}
	router, err := newRouter(authenticated, cfg, newServer(cfg))
	require.NoError(t, err)

	t.Run("an anonymous caller is refused before routing", func(t *testing.T) {
		anonymous, err := newRouter(nil, cfg, newServer(cfg))
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		anonymous.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil))
		requireEnvelope(t, rec, http.StatusUnauthorized, ErrTypeNotAuthorized)
		require.NotContains(t, rec.Body.String(), "not an Apache Iceberg")
	})

	t.Run("unknown path", func(t *testing.T) {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil))
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNotFound)
	})

	t.Run("unknown method on a served path", func(t *testing.T) {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, configPath, nil))
		requireEnvelope(t, rec, http.StatusMethodNotAllowed, ErrTypeMethodNotAllowed)
	})
}

// TestRouterRefusesToAdvertiseNonGoals is the §1.2 compatibility rule: an advertised endpoint is
// a promise (§2.1), so a route that would advertise views, table rename, the multi-table
// transaction commit or server-side scan planning fails the build of the router instead of
// quietly appearing in GET /v1/config.
func TestRouterRefusesToAdvertiseNonGoals(t *testing.T) {
	cfg := &Config{MaxRequestBody: 1024}
	for name, endpoint := range map[string]string{
		"table rename":       "POST /v1/{prefix}/tables/rename",
		"view load":          "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
		"view rename":        "POST /v1/{prefix}/views/rename",
		"transaction commit": "POST /v1/{prefix}/transactions/commit",
		"scan planning":      "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
		"scan task fetch":    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
	} {
		t.Run(name, func(t *testing.T) {
			c := testServer(cfg, route{
				method:   http.MethodPost,
				pattern:  "/v1/non-goal",
				handler:  echoHandler,
				endpoint: endpoint,
			})
			_, err := newRouter(authenticated, cfg, c)
			require.ErrorIs(t, err, errNonGoalEndpoint)
			require.Contains(t, err.Error(), endpoint)
		})
	}

	t.Run("an implemented operation is accepted", func(t *testing.T) {
		c := testServer(cfg, route{
			method:   http.MethodGet,
			pattern:  "/v1/namespaces",
			handler:  echoHandler,
			endpoint: "GET /v1/{prefix}/namespaces",
		})
		_, err := newRouter(authenticated, cfg, c)
		require.NoError(t, err)
	})
}

// TestAdvertisedEndpoints lists only the operations the route table serves, and always returns
// an array rather than nil: the endpoint list is what tells a client which operations exist.
func TestAdvertisedEndpoints(t *testing.T) {
	t.Run("the config endpoint is served but never advertised", func(t *testing.T) {
		c := newServer(&Config{})
		require.Equal(t, []string{}, c.endpoints)
		require.NotNil(t, c.endpoints, "the endpoints list is always an array")
	})

	t.Run("advertised endpoints follow the route table order", func(t *testing.T) {
		c := testServer(&Config{},
			route{method: http.MethodGet, pattern: configPath, handler: echoHandler},
			route{method: http.MethodGet, pattern: "/v1/namespaces", handler: echoHandler, endpoint: "GET /v1/{prefix}/namespaces"},
			route{method: http.MethodPost, pattern: "/v1/namespaces", handler: echoHandler, endpoint: "POST /v1/{prefix}/namespaces"},
		)
		require.Equal(t, []string{"GET /v1/{prefix}/namespaces", "POST /v1/{prefix}/namespaces"}, c.endpoints)
	})
}

// TestConfigEndpoint is GET /v1/config (§2.1): the defaults, the prefix override and an explicit
// endpoints array. The array must be present even while empty, because an absent list means the
// default set - which includes rename and transactions.
func TestConfigEndpoint(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, MaxRequestBody: DefaultMaxRequestBody}

	t.Run("an empty catalog still sends an explicit endpoints array", func(t *testing.T) {
		router, err := newRouter(authenticated, cfg, newServer(cfg))
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, configPath, nil))

		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, contentTypeJSON, rec.Header().Get("Content-Type"))
		require.Contains(t, rec.Body.String(), `"endpoints":[]`,
			"an absent endpoint list would mean the default set, which includes rename and transactions")

		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.NotNil(t, response.Defaults)
		require.Empty(t, response.Defaults, "the catalog imposes no property on the client")
		require.Equal(t, map[string]string{"prefix": "lakefs"}, response.Overrides)
		require.Empty(t, response.Endpoints)
	})

	t.Run("an implemented operation is served and advertised", func(t *testing.T) {
		c := &server{cfg: cfg}
		c.routes = []route{
			{method: http.MethodGet, pattern: configPath, handler: c.handleConfig},
			{method: http.MethodGet, pattern: "/v1/{prefix}/namespaces", handler: echoHandler,
				endpoint: "GET /v1/{prefix}/namespaces"},
		}
		c.endpoints = advertisedEndpoints(c.routes)
		router, err := newRouter(authenticated, cfg, c)
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, configPath, nil))

		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, []string{"GET /v1/{prefix}/namespaces"}, response.Endpoints,
			"the advertised list is the route table, so config cannot promise an unmounted operation")
	})
}

// TestAuthMiddlewareFailuresUseTheEnvelope pins §7.1 and the §12 security test: the core
// authentication middleware the catalog reuses answers a rejection with the core
// {"message": ...} body, and the catalog replaces it with its own envelope without leaking the
// reason. A client reads the reason of an error out of {"error": {...}}.
func TestAuthMiddlewareFailuresUseTheEnvelope(t *testing.T) {
	const internalDetail = "access key does not exist"

	rejecting := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Exactly what api.GenericAuthMiddleware writes, through the same helper.
			httputil.WriteAPIError(w, r, http.StatusUnauthorized, internalDetail)
		})
	}
	cfg := &Config{MaxRequestBody: 1024}
	router, err := newRouter(rejecting, cfg, newServer(cfg))
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, configPath, nil))

	apiError := requireEnvelope(t, rec, http.StatusUnauthorized, ErrTypeNotAuthorized)
	require.NotContains(t, apiError.Message, internalDetail, "the reason for the rejection must not leak")
	require.Equal(t, authFailureMessage(http.StatusUnauthorized), apiError.Message)
	require.NotContains(t, rec.Body.String(), internalDetail)
}

// TestAuthErrorWriterPassesCatalogAnswersThrough: the envelope replacement covers the
// authentication middleware's own answers only. A 401 (requireUser) or 403 (authz, I1-6) written
// by a catalog handler must reach the client as written.
func TestAuthErrorWriterPassesCatalogAnswersThrough(t *testing.T) {
	rejecting := func(next http.Handler) http.Handler { return next }
	handler := authenticate(rejecting, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeIcebergError(w, http.StatusForbidden, ErrTypeForbidden, "ref is read-only")
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, configPath, nil))
	requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
	require.Contains(t, rec.Body.String(), "ref is read-only")
}

// coreRouter mirrors the part of api.Serve that matters for the mount: the /api/v1 catch-all
// that answers every unmatched path. RoutesBuilder mounts onto the same *chi.Mux in run.go.
func coreRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Mount(apiutil.BaseURL, http.HandlerFunc(api.InvalidAPIEndpointHandler))
	return r
}

// newServerDeps builds the server dependencies RoutesBuilder needs: an ACL auth service over the
// in-memory KV store, and one user whose credentials the tests authenticate with. No policy is
// attached - this task is about authentication and mounting, and authorization lands in I1-6.
func newServerDeps(t *testing.T) cmdpkg.ServerDeps {
	t.Helper()
	ctx := t.Context()
	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err)
	t.Cleanup(kvStore.Close)

	authService := acl.NewAuthService(kvStore,
		crypt.NewSecretStore([]byte("iceberg-catalog-test-secret")),
		authparams.ServiceCache{Enabled: false})
	_, err = authService.CreateUser(ctx, &model.User{Username: testUsername})
	require.NoError(t, err)
	_, err = authService.AddCredentials(ctx, testUsername, testAccessKeyID, testSecretAccessKey)
	require.NoError(t, err)

	return cmdpkg.ServerDeps{
		Config:        &config.ConfigImpl{},
		AuthService:   authService,
		Authenticator: auth.NewBuiltinAuthenticator(authService),
		KVStore:       kvStore,
		Logger:        logging.ContextUnavailable(),
	}
}

// do serves one request through handler, optionally with basic credentials.
func do(t *testing.T, handler http.Handler, method, path, accessKey, secret string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if accessKey != "" {
		req.SetBasicAuth(accessKey, secret)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestRoutesBuilderDisabled pins the effect of iceberg.enabled=false (§10): the prefixes are
// still claimed, and the answer is the catalog's 501 in its own envelope. It also re-proves the
// I0-3 mount fix from the catalog's side: the core placeholder must neither panic nor shadow the
// hook's mount.
func TestRoutesBuilderDisabled(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: false\n")

	mux := coreRouter()
	require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{}, mux))

	for _, path := range []string{
		APIBasePath + configPath,
		RelativeToBasePath + "/repo.branch/api" + configPath,
	} {
		rec := do(t, mux, http.MethodGet, path, "", "")
		apiError := requireEnvelope(t, rec, http.StatusNotImplemented, ErrTypeNotImplemented)
		require.Equal(t, DisabledMessage, apiError.Message)
	}

	// I0-3: the placeholders the hook did not claim stay 501, and the claimed prefixes keep
	// answering as the catalog does.
	require.NotPanics(t, func() { api.MountNotImplementedServices(mux) })
	rec := do(t, mux, http.MethodGet, APIBasePath+configPath, "", "")
	requireEnvelope(t, rec, http.StatusNotImplemented, ErrTypeNotImplemented)
	rec = do(t, mux, http.MethodGet, "/mds/iceberg/api/v1/config", "", "")
	require.Equal(t, http.StatusNotImplemented, rec.Code, "the metadata search prefix stays a core 501")
}

// TestRoutesBuilderNeedsDeps: an enabled catalog without server dependencies cannot be served,
// and startup must fail rather than mount a catalog that answers 500 to everything.
func TestRoutesBuilderNeedsDeps(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: true\n")
	require.ErrorIs(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{}, coreRouter()), ErrMissingDeps)
}

// TestRoutesBuilderEnabled is the end-to-end mounting test: both addressing forms land on the
// same catalog, behind real authentication, over the core catch-all.
func TestRoutesBuilderEnabled(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: true\n  max_request_body: 2048\n")

	deps := newServerDeps(t)
	mux := coreRouter()
	require.NoError(t, RoutesBuilder(t.Context(), deps, mux))

	t.Run("an anonymous caller is refused in the envelope", func(t *testing.T) {
		rec := do(t, mux, http.MethodGet, APIBasePath+configPath, "", "")
		requireEnvelope(t, rec, http.StatusUnauthorized, ErrTypeNotAuthorized)
		require.NotContains(t, rec.Body.String(), api.ErrInvalidAPIEndpoint.Error())
	})

	t.Run("unknown credentials are refused in the envelope and leak nothing", func(t *testing.T) {
		rec := do(t, mux, http.MethodGet, APIBasePath+configPath, "AKIAUNKNOWNKEY0001", "unknown-secret")
		apiError := requireEnvelope(t, rec, http.StatusUnauthorized, ErrTypeNotAuthorized)
		require.Equal(t, authFailureMessage(http.StatusUnauthorized), apiError.Message)
	})

	t.Run("the config endpoint is served behind auth", func(t *testing.T) {
		rec := do(t, mux, http.MethodGet, APIBasePath+configPath, testAccessKeyID, testSecretAccessKey)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, map[string]string{"prefix": CatalogPrefix}, response.Overrides)
		require.NotNil(t, response.Endpoints)
	})

	t.Run("relative_to answers exactly like the default form", func(t *testing.T) {
		direct := do(t, mux, http.MethodGet, APIBasePath+configPath, testAccessKeyID, testSecretAccessKey)
		relative := do(t, mux, http.MethodGet, RelativeToBasePath+"/repo.branch/api"+configPath,
			testAccessKeyID, testSecretAccessKey)
		require.Equal(t, http.StatusOK, relative.Code, "body: %s", relative.Body.String())
		require.JSONEq(t, direct.Body.String(), relative.Body.String())
	})

	t.Run("a relative_to path without a warehouse is a catalog 404", func(t *testing.T) {
		rec := do(t, mux, http.MethodGet, RelativeToBasePath+configPath, testAccessKeyID, testSecretAccessKey)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("the core catch-all still answers unrelated paths", func(t *testing.T) {
		rec := do(t, mux, http.MethodGet, apiutil.BaseURL+"/definitely-not-a-catalog-route", "", "")
		require.Equal(t, http.StatusNotFound, rec.Code)
		require.Contains(t, rec.Body.String(), api.ErrInvalidAPIEndpoint.Error())
	})

	t.Run("the core placeholders do not shadow the mounted catalog", func(t *testing.T) {
		require.NotPanics(t, func() { api.MountNotImplementedServices(mux) })
		rec := do(t, mux, http.MethodGet, APIBasePath+configPath, testAccessKeyID, testSecretAccessKey)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})
}

// dataPlaneCredentialsPath and dataPlaneSignPath are the two §8 data-plane operations as a client
// addresses them. The namespace segment is written the way a client writes it (%1F between the
// levels); it is never decoded here, because these requests only have to route.
const (
	dataPlaneCredentialsPath = APIBasePath + "/v1/" + CatalogPrefix +
		"/namespaces/repo%1Fmain%1Fns/tables/titles/credentials"
	dataPlaneSignPath = APIBasePath + "/v1/" + CatalogPrefix +
		"/namespaces/repo%1Fmain%1Fns/tables/titles/sign"
)

// mountedEndpoints returns the endpoints GET /v1/config advertises on a mounted catalog, read with
// the fixture's basic credentials.
func mountedEndpoints(t *testing.T, handler http.Handler) []string {
	t.Helper()
	rec := do(t, handler, http.MethodGet, APIBasePath+configPath, testAccessKeyID, testSecretAccessKey)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response CatalogConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	return response.Endpoints
}

// requireMethodNotAllowed proves an operation is *mounted*: the path is served, only not with this
// method, which chi answers 405 for. An operation that is not mounted answers 404 instead, so the
// two are distinguishable without invoking a service.
func requireMethodNotAllowed(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	requireEnvelope(t, rec, http.StatusMethodNotAllowed, ErrTypeMethodNotAllowed)
}

// TestRoutesBuilderMountsDataPlaneServices is the wiring test of §8: the remote-signing and
// credential-vending services of sign.go and creds.go are contributed to the route table by
// RoutesBuilder, so each is mounted and advertised exactly when its own precondition holds. A
// disabled service contributes no route, so GET /v1/config cannot promise a data-plane operation
// the router does not serve (§2.1), and the endpoint answers 404 rather than sitting half-wired.
func TestRoutesBuilderMountsDataPlaneServices(t *testing.T) {
	// A real catalog, so deps.Catalog is the shape the server hands a service (§4.1). Nothing in
	// this test reaches the catalog: mounting and advertisement are decided at startup.
	fixture := newStoreFixture(t)

	build := func(t *testing.T, yaml string, adapter block.Adapter) http.Handler {
		t.Helper()
		useConfig(t, yaml)
		deps := newServerDeps(t)
		deps.Catalog = fixture.catalog
		deps.BlockAdapter = adapter
		mux := coreRouter()
		require.NoError(t, RoutesBuilder(t.Context(), deps, mux))
		return mux
	}

	t.Run("no data-plane service is mounted by default", func(t *testing.T) {
		// The catalog's own mem blockstore: it can neither presign nor vend, and no role is
		// configured.
		mux := build(t, "iceberg:\n  enabled: true\n", fixture.catalog.BlockAdapter)

		endpoints := mountedEndpoints(t, mux)
		require.NotContains(t, endpoints, remoteSigningEndpoint)
		require.NotContains(t, endpoints, credentialsEndpoint)

		requireEnvelope(t, do(t, mux, http.MethodGet, dataPlaneCredentialsPath, testAccessKeyID, testSecretAccessKey),
			http.StatusNotFound, ErrTypeNotFound)
		requireEnvelope(t, do(t, mux, http.MethodPost, dataPlaneSignPath, testAccessKeyID, testSecretAccessKey),
			http.StatusNotFound, ErrTypeNotFound)
	})

	t.Run("remote signing is mounted when the blockstore can sign", func(t *testing.T) {
		// An S3 blockstore with a working pre-signer and no credentials_vending role: the signing
		// endpoint exists, the credentials endpoint does not.
		mux := build(t, "iceberg:\n  enabled: true\n", signingEnabledAdapter())

		endpoints := mountedEndpoints(t, mux)
		require.Contains(t, endpoints, remoteSigningEndpoint)
		require.NotContains(t, endpoints, credentialsEndpoint)

		requireMethodNotAllowed(t, do(t, mux, http.MethodGet, dataPlaneSignPath, testAccessKeyID, testSecretAccessKey))
		requireEnvelope(t, do(t, mux, http.MethodGet, dataPlaneCredentialsPath, testAccessKeyID, testSecretAccessKey),
			http.StatusNotFound, ErrTypeNotFound)
	})

	t.Run("credential vending is mounted when a role is configured", func(t *testing.T) {
		// An S3 blockstore whose pre-signer is off, and a configured role: vending is advertised
		// and remote signing is not, so the two are decided independently.
		adapter := &signingAdapter{blockstoreType: block.BlockstoreTypeS3, preSignSupported: false}
		mux := build(t, "iceberg:\n  enabled: true\nblockstore:\n  s3:\n    credentials_vending:\n      role_arn: arn:aws:iam::123456789012:role/lakefs-iceberg\n", adapter)

		endpoints := mountedEndpoints(t, mux)
		require.Contains(t, endpoints, credentialsEndpoint)
		require.NotContains(t, endpoints, remoteSigningEndpoint)

		requireMethodNotAllowed(t, do(t, mux, http.MethodPost, dataPlaneCredentialsPath, testAccessKeyID, testSecretAccessKey))
		requireEnvelope(t, do(t, mux, http.MethodGet, dataPlaneSignPath, testAccessKeyID, testSecretAccessKey),
			http.StatusNotFound, ErrTypeNotFound)
	})

	t.Run("both are mounted when both preconditions hold", func(t *testing.T) {
		mux := build(t, "iceberg:\n  enabled: true\nblockstore:\n  s3:\n    credentials_vending:\n      role_arn: arn:aws:iam::123456789012:role/lakefs-iceberg\n", signingEnabledAdapter())

		endpoints := mountedEndpoints(t, mux)
		require.Contains(t, endpoints, remoteSigningEndpoint)
		require.Contains(t, endpoints, credentialsEndpoint)

		requireMethodNotAllowed(t, do(t, mux, http.MethodGet, dataPlaneSignPath, testAccessKeyID, testSecretAccessKey))
		requireMethodNotAllowed(t, do(t, mux, http.MethodPost, dataPlaneCredentialsPath, testAccessKeyID, testSecretAccessKey))
	})
}

// registerPath is the register operation as a client addresses it (spec: registerTable). The
// namespace segment carries the repository and the ref, as every catalog namespace does (§5).
const registerPath = APIBasePath + "/v1/" + CatalogPrefix + "/namespaces/repo%1Fmain%1Fns/register"

// TestRoutesBuilderMountsRegister is the wiring test of the register operation (register.go): it is
// contributed to the route table by RoutesBuilder - so it is mounted and advertised in
// GET /v1/config - and it is its own service, whose route table cannot be mounted without also
// appearing in the endpoint list.
func TestRoutesBuilderMountsRegister(t *testing.T) {
	// A real catalog, so deps.Catalog is the shape the server hands a service (§4.1). Nothing in
	// this test reaches the catalog: mounting and advertisement are decided at startup.
	fixture := newStoreFixture(t)

	useConfig(t, "iceberg:\n  enabled: true\n")
	deps := newServerDeps(t)
	deps.Catalog = fixture.catalog
	deps.BlockAdapter = fixture.catalog.BlockAdapter
	mux := coreRouter()
	require.NoError(t, RoutesBuilder(t.Context(), deps, mux))

	require.Contains(t, mountedEndpoints(t, mux), registerTableEndpoint,
		"register is advertised because it is mounted (§2.1)")

	// Mounted, not merely advertised: the path is served, and only not with GET, which chi answers
	// 405 for. An operation that is missing from the route table answers 404 instead.
	requireMethodNotAllowed(t, do(t, mux, http.MethodGet, registerPath, testAccessKeyID, testSecretAccessKey))
}
