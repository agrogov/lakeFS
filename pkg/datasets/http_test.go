package datasets

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

// coreRouter mirrors the part of api.Serve() that matters for Spike S1: the /api/v1 mount
// whose InvalidAPIEndpointHandler answers every unmatched path. RoutesBuilder mounts onto the
// very same *chi.Mux afterwards, from the hook loop in cmd/lakefs/cmd/run.go.
func coreRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Mount(apiutil.BaseURL, http.HandlerFunc(api.InvalidAPIEndpointHandler))
	return r
}

func doGet(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func testDeps(t *testing.T) cmdpkg.ServerDeps {
	t.Helper()
	ctx := t.Context()
	store := kvtest.GetStore(ctx, t)
	authService := auth.NewBasicAuthService(
		store,
		crypt.NewSecretStore([]byte("datasets-test-secret")),
		authparams.ServiceCache{Enabled: false},
		logging.ContextUnavailable(),
	)
	return cmdpkg.ServerDeps{
		Config:        &config.ConfigImpl{},
		AuthService:   authService,
		Authenticator: auth.NewBuiltinAuthenticator(authService),
		KVStore:       store,
		Logger:        logging.ContextUnavailable(),
	}
}

// TestRoutesBuilderEnabledWinsOverCoreCatchAll is Spike S1 (DATASETS_DESIGN.md §4.5, §18):
// after api.Serve() mounts /api/v1, mounting the datasets router on the same chi router must
// route a datasets request to the datasets router, not to InvalidAPIEndpointHandler. The
// fallback prefix (/api/v1/x/datasets) is therefore not needed.
func TestRoutesBuilderEnabledWinsOverCoreCatchAll(t *testing.T) {
	useConfig(t, "datasets:\n  enabled: true\n  storage_namespace_prefix: \"mem://datasets\"\n")

	mux := coreRouter()
	require.NoError(t, RoutesBuilder(t.Context(), testDeps(t), mux))

	t.Run("datasets path reaches the datasets router", func(t *testing.T) {
		rec := doGet(t, mux, APIBasePath+"/anything")
		// "anything" matches the /{dataset} route of the generated control plane, so the
		// request is answered inside the datasets router: 401 with the datasets error shape,
		// never the core {"message":"invalid API endpoint"} catch-all.
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.NotContains(t, rec.Body.String(), api.ErrInvalidAPIEndpoint.Error())
		require.NotContains(t, rec.Body.String(), "404 page not found")
		require.JSONEq(t, `{"message":"`+ErrAuthenticationRequired.Error()+`"}`, rec.Body.String())
	})

	t.Run("collection root reaches the datasets router", func(t *testing.T) {
		// The mount point itself is served by the generated "GET /" operation.
		rec := doGet(t, mux, APIBasePath)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.NotContains(t, rec.Body.String(), api.ErrInvalidAPIEndpoint.Error())
	})

	t.Run("unrelated path still reaches the core handler", func(t *testing.T) {
		rec := doGet(t, mux, "/api/v1/definitely-not-a-datasets-route")
		require.Equal(t, http.StatusNotFound, rec.Code)
		require.Contains(t, rec.Body.String(), api.ErrInvalidAPIEndpoint.Error())
	})
}

// TestRoutesBuilderEnabledWrapsRouterWithAuth checks that the enabled router sits behind
// api.GenericAuthMiddleware: a request that presents unknown credentials is rejected by the
// middleware before any handler runs (DATASETS_DESIGN.md §4.5).
func TestRoutesBuilderEnabledWrapsRouterWithAuth(t *testing.T) {
	useConfig(t, "datasets:\n  enabled: true\n  storage_namespace_prefix: \"mem://datasets\"\n")

	mux := coreRouter()
	require.NoError(t, RoutesBuilder(t.Context(), testDeps(t), mux))

	req := httptest.NewRequest(http.MethodGet, APIBasePath+"/anything", nil)
	req.SetBasicAuth("unknown-access-key", "unknown-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.NotContains(t, rec.Body.String(), api.ErrInvalidAPIEndpoint.Error())
}

// TestRoutesBuilderDisabled mounts the 501 answer. It also re-proves the mount precedence for
// the disabled path, since datasets.enabled defaults to false.
func TestRoutesBuilderDisabled(t *testing.T) {
	useConfig(t, "{}\n")

	mux := coreRouter()
	require.NoError(t, RoutesBuilder(t.Context(), cmdpkg.ServerDeps{}, mux))

	rec := doGet(t, mux, APIBasePath+"/anything")
	require.Equal(t, http.StatusNotImplemented, rec.Code)
	require.JSONEq(t, `{"message":"`+DisabledMessage+`"}`, rec.Body.String())
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	rec = doGet(t, mux, "/api/v1/not-a-datasets-route")
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), api.ErrInvalidAPIEndpoint.Error())
}

// TestRoutesBuilderInvalidConfig: an invalid datasets section with enabled true fails startup.
// RoutesBuilder is what turns the configuration error into the fatal error in run.go.
func TestRoutesBuilderInvalidConfig(t *testing.T) {
	useConfig(t, "datasets:\n  enabled: true\n")

	mux := coreRouter()
	err := RoutesBuilder(t.Context(), cmdpkg.ServerDeps{}, mux)
	require.ErrorIs(t, err, ErrInvalidConfig)

	// Nothing was mounted: the core catch-all still answers.
	rec := doGet(t, mux, APIBasePath+"/anything")
	require.Contains(t, rec.Body.String(), api.ErrInvalidAPIEndpoint.Error())
}

// TestRoutesBuilderEnabledNeedsDeps: an enabled control plane without server dependencies is a
// wiring bug, reported as an error instead of a nil-pointer panic during startup.
func TestRoutesBuilderEnabledNeedsDeps(t *testing.T) {
	useConfig(t, "datasets:\n  enabled: true\n  storage_namespace_prefix: \"mem://datasets\"\n")

	err := RoutesBuilder(t.Context(), cmdpkg.ServerDeps{}, coreRouter())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMissingDeps)
}
