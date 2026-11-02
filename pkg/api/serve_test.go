package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	ServerTimeout = 30 * time.Second
)

type dependencies struct {
	blocks      block.Adapter
	catalog     *catalog.Catalog
	authService auth.Service
	collector   *memCollector
	server      *httptest.Server
}

// memCollector in-memory collector stores events and metadata sent
type memCollector struct {
	Metrics        []*stats.Metric
	Metadata       []*stats.Metadata
	InstallationID string
	mu             sync.Mutex
}

func (m *memCollector) CollectEvents(ev stats.Event, count uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Metrics = append(m.Metrics, &stats.Metric{Event: ev, Value: count})
}

func (m *memCollector) CollectEvent(ev stats.Event) {
	m.CollectEvents(ev, 1)
}

func (m *memCollector) CollectMetadata(accountMetadata *stats.Metadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Metadata = append(m.Metadata, accountMetadata)
}

func (m *memCollector) SetInstallationID(installationID string) {
	m.InstallationID = installationID
}

func (m *memCollector) CollectCommPrefs(_ stats.CommPrefs) {
}

func (m *memCollector) Close() {}

func createDefaultAdminUser(t testing.TB, clt apigen.ClientWithResponsesInterface) *authmodel.BaseCredential {
	t.Helper()
	res, err := clt.SetupWithResponse(t.Context(), apigen.SetupJSONRequestBody{
		Username: "admin",
	})
	testutil.Must(t, err)
	if res.JSON200 == nil {
		t.Fatal("Failed run setup env", res.HTTPResponse.StatusCode, res.HTTPResponse.Status)
	}
	return &authmodel.BaseCredential{
		IssuedDate:      time.Unix(res.JSON200.CreationDate, 0),
		AccessKeyID:     res.JSON200.AccessKeyId,
		SecretAccessKey: res.JSON200.SecretAccessKey,
	}
}

func setupHandler(t testing.TB) (http.Handler, *dependencies) {
	t.Helper()
	ctx := t.Context()

	if viper.Get(config.BlockstoreTypeKey) == nil {
		viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	}
	viper.Set("database.type", mem.DriverName)
	// Add endpoint so that 'IsAdvancedAuth' will be in effect
	viper.Set("auth.api.endpoint", config.DefaultListenAddress)

	viper.Set("committed.local_cache.size_bytes", 24*1024*1024)
	viper.Set("committed.sstable.memory.cache_size_bytes", 2*1024*1024)

	collector := &memCollector{}
	cfg := &config.ConfigImpl{}
	baseCfg, err := config.NewConfig("", cfg)
	testutil.MustDo(t, "config", err)
	cfg.Committed.LocalCache.Dir = path.Join(t.TempDir(), "cache")
	kvStore := kvtest.GetStore(ctx, t)
	actionsStore := actions.NewActionsKVStore(kvStore)
	idGen := &actions.DecreasingIDGenerator{}
	authService := auth.NewBasicAuthService(kvStore, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{
		Enabled: false,
	}, logging.FromContext(ctx))
	meta := auth.NewKVMetadataManager("serve_test", baseCfg.Installation.FixedID, baseCfg.Database.Type, kvStore)

	// Use context.WithoutCancel to prevent test timeout from cancelling catalog's background goroutines
	// This preserves context values (like logging) while preventing cancellation propagation
	catalogCtx := context.WithoutCancel(ctx)
	// Do not validate invalid config (missing required fields).
	c, err := catalog.New(catalogCtx, catalog.Config{
		Config:                cfg,
		KVStore:               kvStore,
		SettingsManagerOption: settings.WithCache(cache.NoCache),
		PathProvider:          upload.DefaultPathProvider,
	})
	testutil.MustDo(t, "build catalog", err)

	// wire actions
	actionsConfig := actions.Config{Enabled: true}
	actionsConfig.Lua.NetHTTPEnabled = true
	actionsService := actions.NewService(
		ctx,
		actionsStore,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
		idGen,
		collector,
		actionsConfig,
		"",
	)

	c.SetHooksHandler(actionsService)

	authenticator := auth.NewBuiltinAuthenticator(authService)
	kvParams, err := kvparams.NewConfig(&baseCfg.Database)
	testutil.Must(t, err)
	migrator := kv.NewDatabaseMigrator(kvParams)

	t.Cleanup(func() {
		actionsService.Stop()
		_ = c.Close()
	})

	auditChecker := version.NewDefaultAuditChecker(baseCfg.Security.AuditCheckURL, "", nil)

	authenticationService := authentication.NewDummyService()
	logger := logging.FromContext(ctx)
	handler := api.Serve(
		cfg,
		c,
		authenticator,
		authService,
		authenticationService,
		c.BlockAdapter,
		meta,
		migrator,
		collector,
		actionsService,
		auditChecker,
		logger,
		nil,
		nil,
		upload.DefaultPathProvider,
		&stats.NopUsageReporter{},
	)

	// register placeholder routes for services not available in OSS
	api.MountNotImplementedServices(handler)

	return handler, &dependencies{
		blocks:      c.BlockAdapter,
		authService: authService,
		catalog:     c,
		collector:   collector,
	}
}

func setupClientByEndpoint(t testing.TB, endpointURL string, accessKeyID, secretAccessKey string, opts ...apigen.ClientOption) apigen.ClientWithResponsesInterface {
	t.Helper()

	if accessKeyID != "" {
		basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(accessKeyID, secretAccessKey)
		if err != nil {
			t.Fatal("basic auth security provider", err)
		}
		opts = append(opts, apigen.WithRequestEditorFn(basicAuthProvider.Intercept))
	}
	clt, err := apigen.NewClientWithResponses(endpointURL+apiutil.BaseURL, opts...)
	if err != nil {
		t.Fatal("failed to create lakefs api client:", err)
	}
	return clt
}

func setupServer(t testing.TB, handler http.Handler) *httptest.Server {
	t.Helper()
	if shouldUseServerTimeout() {
		handler = http.TimeoutHandler(handler, ServerTimeout, `{"error": "timeout"}`)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func shouldUseServerTimeout() bool {
	withServerTimeoutVal := os.Getenv("TEST_WITH_SERVER_TIMEOUT")
	if withServerTimeoutVal == "" {
		return true // default
	}
	withServerTimeout, err := strconv.ParseBool(withServerTimeoutVal)
	if err != nil {
		panic(fmt.Errorf("invalid TEST_WITH_SERVER_TIMEOUT value: %w", err))
	}
	return withServerTimeout
}

func setupClientWithAdmin(t testing.TB) (apigen.ClientWithResponsesInterface, *dependencies) {
	t.Helper()
	handler, deps := setupHandler(t)
	server := setupServer(t, handler)
	deps.server = server
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)
	clt = setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey)
	return clt, deps
}

func TestInvalidRoute(t *testing.T) {
	handler, _ := setupHandler(t)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)

	// setup client with invalid endpoint base url
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(cred.AccessKeyID, cred.SecretAccessKey)
	if err != nil {
		t.Fatal("basic auth security provider", err)
	}
	clt, err = apigen.NewClientWithResponses(server.URL+apiutil.BaseURL+"//", apigen.WithRequestEditorFn(basicAuthProvider.Intercept))
	if err != nil {
		t.Fatal("failed to create api client:", err)
	}

	ctx := t.Context()
	resp, err := clt.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{})
	if err != nil {
		t.Fatalf("failed to get lakefs server version")
	}
	if resp.JSONDefault == nil {
		t.Fatalf("client api call expected default error, got nil")
	}
	expectedErrMsg := api.ErrInvalidAPIEndpoint.Error()
	errMsg := resp.JSONDefault.Message
	if errMsg != expectedErrMsg {
		t.Fatalf("client response error message: %s, expected: %s", errMsg, expectedErrMsg)
	}
	if resp.StatusCode() != http.StatusNotFound {
		t.Fatalf("expected status code %d, got %d for invalid API endpoint", http.StatusNotFound, resp.StatusCode())
	}
}

func TestNotImplementedAPI(t *testing.T) {
	handler, _ := setupHandler(t)
	server := setupServer(t, handler)

	// verify that for specific APIs, we get a 405 Not Implemented
	routes := []string{"/iceberg/api/v1/config", "/iceberg/relative_to/v1/config", "/mds/iceberg/api/v1/config"}
	for _, route := range routes {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+route, nil)
		if err != nil {
			t.Fatalf("failed to make http request '%s': %s", route, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to perform http request '%s': %s", route, err)
		}
		if resp.StatusCode != http.StatusNotImplemented {
			t.Fatalf("expected status code %d, got %d for '%s'", http.StatusNotImplemented, resp.StatusCode, route)
		}
	}
}

// TestNotImplementedServicesKeepsHookRoutes is Spike S-I3 (ICEBERG_CATALOG_PLAN.md §4.1).
// MountNotImplementedServices runs after the extra-routes builders, so a wrapper binary
// (lakefs-sso) may already have mounted /iceberg/api/ and must keep it: chi v5.2.4 panics
// when Mount() targets a pattern that is already mounted, and a second mount would shadow
// the hook's handler. The placeholders the hook did not claim must stay 501.
func TestNotImplementedServicesKeepsHookRoutes(t *testing.T) {
	const (
		icebergPrefix = "/iceberg/api/"
		hookHeader    = "X-LakeFS-Hook"
	)
	hookHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(hookHeader, "iceberg")
		w.WriteHeader(http.StatusOK)
	})
	subRouter := func(register func(r chi.Router)) http.Handler {
		r := chi.NewRouter()
		register(r)
		return r
	}

	tests := []struct {
		name     string
		hook     http.Handler
		hookPath string
	}{
		{
			name:     "plain handler mounted at the prefix",
			hook:     hookHandler,
			hookPath: icebergPrefix + "v1/config",
		},
		{
			name: "sub-router serving the path the probe uses",
			hook: subRouter(func(r chi.Router) {
				r.Get("/probe", hookHandler)
				r.Get("/v1/config", hookHandler)
			}),
			hookPath: icebergPrefix + "v1/config",
		},
		{
			// The probe inside MountNotImplementedServices cannot report a hit here: a
			// sub-router answers only for the paths it registers. The mount itself must
			// still be detected, or Mount() panics.
			name: "sub-router serving none of the probed paths",
			hook: subRouter(func(r chi.Router) {
				r.Get("/v1/namespaces", hookHandler)
			}),
			hookPath: icebergPrefix + "v1/namespaces",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := chi.NewRouter()
			router.Mount(icebergPrefix, tt.hook)

			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("MountNotImplementedServices panicked over a hook-mounted %q: %v", icebergPrefix, r)
					}
				}()
				api.MountNotImplementedServices(router)
				// Mounting twice is a no-op: the placeholders this call registered are
				// detected on the second call exactly like a hook's mount is.
				api.MountNotImplementedServices(router)
			}()

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.hookPath, nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("hook route %q: expected status code %d, got %d", tt.hookPath, http.StatusOK, rec.Code)
			}
			if got := rec.Header().Get(hookHeader); got != "iceberg" {
				t.Fatalf("hook route %q did not answer: header %q = %q", tt.hookPath, hookHeader, got)
			}

			// The hook claimed only /iceberg/api/, so the other placeholders stay mounted.
			for _, route := range []string{"/iceberg/relative_to/v1/config", "/mds/iceberg/api/v1/config"} {
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, route, nil))
				if rec.Code != http.StatusNotImplemented {
					t.Fatalf("expected status code %d for %q, got %d", http.StatusNotImplemented, route, rec.Code)
				}
			}
		})
	}
}

// TestNotImplementedServicesWithCoreCatchAll pins the other half of the detection: the core
// router ends with a catch-all that answers every path, so a bare Match() reports every path
// as taken. The placeholders must still be mounted on such a router, or an unmatched iceberg
// request falls through to the catch-all instead of answering 501.
func TestNotImplementedServicesWithCoreCatchAll(t *testing.T) {
	router := chi.NewRouter()
	router.Mount("/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	api.MountNotImplementedServices(router)

	routes := []string{"/iceberg/api/v1/config", "/iceberg/relative_to/v1/config", "/mds/iceberg/api/v1/config"}
	for _, route := range routes {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, route, nil))
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("expected status code %d for %q, got %d", http.StatusNotImplemented, route, rec.Code)
		}
	}
}
