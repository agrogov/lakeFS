package table_test

// The in-process lakeFS server the package's integration tests run against: a real catalog, a
// real block adapter and the production catalog routes, over HTTP, so iceberg-go sees exactly what
// a deployed indexer sees (METADATA_SEARCH_DESIGN.md §4.3 step 3, §4.6; METADATA_SEARCH_PLAN.md
// task M3).
//
// The block adapter is the local one: iceberg-go writes its data, Avro and metadata files
// directly, and this test binary registers a "local" IO scheme that maps the adapter's
// local:///abs/path locations to the filesystem. A production indexer writes to S3 through the
// s3 IO scheme catalog.go registers; the mapping between a lakeFS storage namespace and the IO
// that serves it is iceberg-go's, so the test changes only the scheme registration.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	icebergcataloggo "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	lakefscatalog "github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/table"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	testAccessKeyID     = "AKIAMDSTEST00000001"
	testSecretAccessKey = "mds-test-secret-access-key-0123456789"
	testUser            = "mds-indexer"

	// testBranch is the branch of the companion repository the fixture indexes.
	testBranch = "main"
)

var localIOScheme sync.Once

// localIO is the iceberg-go IO for the local block adapter's namespace (local:///abs/path).
type localIO struct{}

func localPath(name string) string {
	return strings.TrimPrefix(name, "local://")
}

func (localIO) Open(name string) (icebergio.File, error) {
	return icebergio.LocalFS{}.Open(localPath(name))
}

func (localIO) Remove(name string) error {
	return icebergio.LocalFS{}.Remove(localPath(name))
}

func (localIO) ReadFile(name string) ([]byte, error) {
	return icebergio.LocalFS{}.ReadFile(localPath(name))
}

func (localIO) Create(name string) (icebergio.FileWriter, error) {
	return icebergio.LocalFS{}.Create(localPath(name))
}

func (localIO) WriteFile(name string, content []byte) error {
	return icebergio.LocalFS{}.WriteFile(localPath(name), content)
}

// registerLocalIO installs the local scheme once per test binary. iceberg-go panics on a
// duplicate registration, so every fixture funnels through sync.Once.
func registerLocalIO() {
	localIOScheme.Do(func() {
		icebergio.Register("local", func(_ context.Context, _ *url.URL, _ map[string]string) (icebergio.IO, error) {
			return localIO{}, nil
		})
	})
}

// testEnv is one in-process lakeFS server, the companion repository the fixture creates, and the
// iceberg-go client of that repository's branch.
type testEnv struct {
	repo   string
	server *httptest.Server
	rest   *rest.Catalog
	// clientConfig is the configuration the client catalog was built with, so that a test can
	// assert the mode it runs in (vended_test.go asserts that metadata_search.storage is empty).
	clientConfig table.CatalogConfig
	// cat is the lakeFS catalog the fixture serves. A test that asserts on the catalog's own
	// entries - the table pointer, the staged table files and their physical addresses - reads
	// them through it, the way the server wrote them, rather than through the catalog API.
	cat *lakefscatalog.Catalog
}

// testEnvOptions are the fixture knobs beyond the local-blockstore default. Every test but the
// vended-credentials test of vended_test.go uses the defaults.
type testEnvOptions struct {
	// viper is set before the lakeFS configuration is built. The vended test uses it for the
	// blockstore.s3.* keys and for iceberg.credentials_vending.*.
	viper map[string]any
	// storageNamespace overrides the companion repository's storage namespace. A nil function
	// keeps the local adapter's namespace under the fixture's temporary directory.
	storageNamespace func(root, repo string) string
	// storage is metadata_search.storage, the client's static IO properties. Nil is the vended
	// mode of design §4.6: the client has no object-store credentials of its own.
	storage iceberg.Properties
	// wrapHandler wraps the catalog's HTTP handler, so a test can observe the requests iceberg-go
	// sends (the vended test records the data-access delegation header).
	wrapHandler func(http.Handler) http.Handler
}

// newTestEnv builds the fixture with the default: a local block adapter rooted in a temporary
// directory and a client with no static IO properties.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	return newTestEnvWith(t, testEnvOptions{})
}

// newTestEnvWith builds the fixture: a block adapter, a mem KV store, a basic auth service with one
// principal, a catalog, the production catalog routes mounted over a real HTTP server, the
// companion repository and its system namespace.
func newTestEnvWith(t *testing.T, opts testEnvOptions) *testEnv {
	t.Helper()
	ctx := t.Context()
	registerLocalIO()

	root := t.TempDir()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)
	viper.Set("blockstore.local.path", root)
	for key, value := range opts.viper {
		viper.Set(key, value)
	}
	viper.Set("iceberg.enabled", true)
	viper.Set("iceberg.metadata_search.enabled", true)
	viper.Set("committed.local_cache.size_bytes", 24*1024*1024)
	viper.Set("committed.sstable.memory.cache_size_bytes", 2*1024*1024)

	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err, "build the lakeFS configuration")
	cfg.Committed.LocalCache.Dir = filepath.Join(root, "cache")

	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open the mem KV store")
	t.Cleanup(kvStore.Close)

	authService := auth.NewBasicAuthService(kvStore, crypt.NewSecretStore([]byte("mds-table-test-secret")),
		authparams.ServiceCache{Enabled: false}, logging.FromContext(ctx))
	_, err = authService.CreateUser(ctx, &authmodel.User{Username: testUser})
	require.NoError(t, err, "create the test principal")
	_, err = authService.AddCredentials(ctx, testUser, testAccessKeyID, testSecretAccessKey)
	require.NoError(t, err, "add the test principal's credentials")

	cat, err := lakefscatalog.New(context.WithoutCancel(ctx), lakefscatalog.Config{
		Config:       cfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err, "build the lakeFS catalog")
	t.Cleanup(func() { _ = cat.Close() })

	env := &testEnv{repo: "data-repo-metadata"}

	storageNamespace := "local://" + filepath.Join(root, "repos", env.repo)
	if opts.storageNamespace != nil {
		storageNamespace = opts.storageNamespace(root, env.repo)
	}
	_, err = cat.CreateRepository(ctx, env.repo, "", storageNamespace, testBranch, false)
	require.NoError(t, err, "create the companion repository")
	env.cat = cat

	mux := chi.NewRouter()
	mux.Handle("/*", http.NotFoundHandler())
	require.NoError(t, icebergcatalog.RoutesBuilder(ctx, cmdpkg.ServerDeps{
		Config:        cfg,
		AuthService:   authService,
		Authenticator: auth.NewBuiltinAuthenticator(authService),
		Catalog:       cat,
		BlockAdapter:  cat.BlockAdapter,
		KVStore:       kvStore,
		Logger:        logging.FromContext(ctx),
	}, mux), "mount the Iceberg catalog")
	api.MountNotImplementedServices(mux)
	var handler http.Handler = mux
	if opts.wrapHandler != nil {
		handler = opts.wrapHandler(handler)
	}
	env.server = httptest.NewServer(handler)
	t.Cleanup(env.server.Close)

	env.clientConfig = table.CatalogConfig{
		Endpoint:        env.server.URL,
		Repository:      env.repo,
		Branch:          testBranch,
		AccessKeyID:     testAccessKeyID,
		SecretAccessKey: testSecretAccessKey,
		Storage:         opts.storage,
	}
	env.rest, err = table.NewCatalog(ctx, env.clientConfig)
	require.NoError(t, err, "build the iceberg-go REST catalog")

	err = env.rest.CreateNamespace(ctx, icebergcataloggo.ToIdentifier(table.Namespace), nil)
	require.NoError(t, err, "create the system namespace")

	return env
}

// catalogConfig is the configuration of the fixture's branch with the given storage properties.
func (e *testEnv) catalogConfig(storage iceberg.Properties) table.CatalogConfig {
	return table.CatalogConfig{
		Endpoint:        e.server.URL,
		Repository:      e.repo,
		Branch:          testBranch,
		AccessKeyID:     testAccessKeyID,
		SecretAccessKey: testSecretAccessKey,
		Storage:         storage,
	}
}

// createTable creates system.object_metadata through the catalog and returns the created table.
func (e *testEnv) createTable(t *testing.T) *icebergtable.Table {
	t.Helper()
	created, err := table.CreateTable(t.Context(), e.rest, e.dataRepository(), nil)
	require.NoError(t, err, "create the object_metadata table")
	return created
}

// dataRepository is the data repository name the table describes. The fixture has no data
// repository of its own: the rows are synthetic and only the name matters.
func (e *testEnv) dataRepository() string { return "data-repo" }

// loadTable loads system.object_metadata from the catalog, i.e. the table as a reader sees it.
func (e *testEnv) loadTable(t *testing.T) *icebergtable.Table {
	t.Helper()
	tbl, err := e.rest.LoadTable(t.Context(), table.Identifier())
	require.NoError(t, err, "load the object_metadata table")
	return tbl
}
