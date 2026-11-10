package indexer_test

// server_test.go is the integration test the end-to-end run was missing: it drives one indexer
// tick against a real in-process lakeFS - the production API and Iceberg catalog routes, a real
// catalog and the local block adapter - rather than against the fakes of fake_test.go. It covers
// the whole write path of design §4.3 (catalog staging, the lakeFS commit of R-metadata, the
// mds-<C> tags and the rows a reader sees) and is the regression test for the "commit: no changes"
// defect: with the recorded position returned by the log API's inclusive stop_at, the indexer used
// to apply the last indexed commit again, stage nothing and fail the lakeFS commit.

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	lakefscatalog "github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/reader"
	"github.com/treeverse/lakefs/pkg/mds/table"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	serverAccessKeyID     = "AKIAMDSINDEXERTEST01"
	serverSecretAccessKey = "mds-indexer-test-secret-key-0123456789"
	serverIndexerUser     = "mds-indexer"
	serverDataRepository  = "server-data"
)

var mdsLocalIOScheme sync.Once

// mdsLocalIO serves iceberg-go's local:// locations from the filesystem, exactly as
// pkg/mds/table's server fixture does for the local block adapter.
type mdsLocalIO struct{}

func mdsLocalPath(name string) string { return strings.TrimPrefix(name, "local://") }

func (mdsLocalIO) Open(name string) (icebergio.File, error) {
	return icebergio.LocalFS{}.Open(mdsLocalPath(name))
}
func (mdsLocalIO) Remove(name string) error { return icebergio.LocalFS{}.Remove(mdsLocalPath(name)) }
func (mdsLocalIO) ReadFile(name string) ([]byte, error) {
	return icebergio.LocalFS{}.ReadFile(mdsLocalPath(name))
}
func (mdsLocalIO) Create(name string) (icebergio.FileWriter, error) {
	return icebergio.LocalFS{}.Create(mdsLocalPath(name))
}
func (mdsLocalIO) WriteFile(name string, content []byte) error {
	return icebergio.LocalFS{}.WriteFile(mdsLocalPath(name), content)
}

func registerMDSLocalIO() {
	mdsLocalIOScheme.Do(func() {
		icebergio.Register("local", func(_ context.Context, _ *url.URL, _ map[string]string) (icebergio.IO, error) {
			return mdsLocalIO{}, nil
		})
	})
}

// mdsServerEnv is one in-process lakeFS server with the API and catalog mounted as the server
// mounts them, a data repository, and the catalog client a reader would use.
type mdsServerEnv struct {
	url           string
	catalog       *lakefscatalog.Catalog
	dataRepo      string
	metadataRepo  string
	dataNamespace string
	accessKeyID   string
	secretKey     string
}

func newMDSServerEnv(t *testing.T) *mdsServerEnv {
	t.Helper()
	ctx := t.Context()
	registerMDSLocalIO()

	root := t.TempDir()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)
	viper.Set("blockstore.local.path", root)
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

	authService := auth.NewBasicAuthService(kvStore, crypt.NewSecretStore([]byte("mds-indexer-server-test")),
		authparams.ServiceCache{Enabled: false}, logging.FromContext(ctx))
	_, err = authService.CreateUser(ctx, &authmodel.User{Username: serverIndexerUser})
	require.NoError(t, err, "create the indexer principal")
	_, err = authService.AddCredentials(ctx, serverIndexerUser, serverAccessKeyID, serverSecretAccessKey)
	require.NoError(t, err, "add the indexer principal's credentials")

	cat, err := lakefscatalog.New(context.WithoutCancel(ctx), lakefscatalog.Config{
		Config:       cfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err, "build the lakeFS catalog")
	t.Cleanup(func() { _ = cat.Close() })

	dataRepo := serverDataRepository
	metadataRepo := dataRepo + icebergcatalog.MetadataRepositorySuffix
	dataNamespace := "local://" + filepath.Join(root, "repos", dataRepo)
	_, err = cat.CreateRepository(ctx, dataRepo, "", dataNamespace, "main", false)
	require.NoError(t, err, "create the data repository")

	authenticator := auth.NewBuiltinAuthenticator(authService)
	collector := &mdsNopCollector{}
	handler := api.Serve(
		cfg,
		cat,
		authenticator,
		authService,
		authentication.NewDummyService(),
		cat.BlockAdapter,
		auth.NewKVMetadataManager("mds_indexer_server_test", cfg.Installation.FixedID, cfg.Database.Type, kvStore),
		nil,
		collector,
		nil,
		nil,
		logging.FromContext(ctx),
		nil,
		nil,
		upload.DefaultPathProvider,
		&stats.NopUsageReporter{},
	)
	deps := cmdpkg.ServerDeps{
		Config:        cfg,
		AuthService:   authService,
		Authenticator: authenticator,
		Catalog:       cat,
		BlockAdapter:  cat.BlockAdapter,
		KVStore:       kvStore,
		Logger:        logging.FromContext(ctx),
	}
	require.NoError(t, icebergcatalog.RoutesBuilder(ctx, deps, handler), "mount the Iceberg catalog")
	api.MountNotImplementedServices(handler)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return &mdsServerEnv{
		url:           server.URL,
		catalog:       cat,
		dataRepo:      dataRepo,
		metadataRepo:  metadataRepo,
		dataNamespace: dataNamespace,
		accessKeyID:   serverAccessKeyID,
		secretKey:     serverSecretAccessKey,
	}
}

// mdsNopCollector is the stats sink of the test server.
type mdsNopCollector struct{}

func (*mdsNopCollector) CollectEvent(stats.Event)          {}
func (*mdsNopCollector) CollectEvents(stats.Event, uint64) {}
func (*mdsNopCollector) CollectMetadata(*stats.Metadata)   {}
func (*mdsNopCollector) CollectCommPrefs(stats.CommPrefs)  {}
func (*mdsNopCollector) SetInstallationID(string)          {}
func (*mdsNopCollector) Close()                            {}

// entry is one object of the data repository's first commit.
func (e *mdsServerEnv) entry(t *testing.T, path string, size int64, etag string) lakefscatalog.DBEntry {
	t.Helper()
	return lakefscatalog.DBEntry{
		Path:            path,
		PhysicalAddress: "local://" + filepath.Join(t.TempDir(), path),
		CreationDate:    time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		Size:            size,
		Checksum:        etag,
		Metadata:        lakefscatalog.Metadata{"X-Amz-Meta-Owner": "alice"},
		ContentType:     "text/plain",
	}
}

// TestIndexerTickAgainstALakeFSServer is the end-to-end regression of design §4.3: two commits
// (one deletes an object), one tick that indexes the branch from its oldest first-parent commit,
// and a real lakeFS commit and tag for every indexed source commit.
func TestIndexerTickAgainstALakeFSServer(t *testing.T) {
	ctx := t.Context()
	env := newMDSServerEnv(t)

	// First commit: a and b.
	require.NoError(t, env.catalog.CreateEntry(ctx, env.dataRepo, "main", env.entry(t, "a", 10, "etag-a")))
	require.NoError(t, env.catalog.CreateEntry(ctx, env.dataRepo, "main", env.entry(t, "b", 20, "etag-b")))
	first, err := env.catalog.Commit(ctx, env.dataRepo, "main", "add a and b", "tester", nil, nil, nil, false)
	require.NoError(t, err)

	// Second commit: b is deleted and c is added.
	require.NoError(t, env.catalog.DeleteEntry(ctx, env.dataRepo, "main", "b"))
	require.NoError(t, env.catalog.CreateEntry(ctx, env.dataRepo, "main", env.entry(t, "c", 30, "etag-c")))
	second, err := env.catalog.Commit(ctx, env.dataRepo, "main", "delete b and add c", "tester", nil, nil, nil, false)
	require.NoError(t, err)

	idx, err := indexer.New(indexer.Options{
		// RA4b: the indexer runs on the server's KV store and block adapter, so it reads and
		// writes the repositories through an in-process reader over the same catalog the test
		// server serves. Only the table files go over REST, with the principal's credentials.
		LakeFS:      reader.New(env.catalog, 0),
		Endpoint:    env.url,
		AccessKeyID: env.accessKeyID,
		SecretKey:   env.secretKey,
		Repositories: []indexer.Repository{{
			Name:     env.dataRepo,
			Branches: []string{"main"},
		}},
		// Index the branch from its oldest first-parent commit so that one tick applies both
		// source commits. This is also the path on which the inclusive stop_at used to make the
		// tick apply the just-indexed position again.
		Backfill: indexer.BackfillHistory,
		Log:      logging.FromContext(ctx),
	})
	require.NoError(t, err, "build the indexer")
	require.NoError(t, idx.Tick(ctx), "one indexer tick")

	// The companion repository is the indexer's: non-bare, with an initialisation commit as its
	// first non-root commit.
	metadata, err := env.catalog.GetRepository(ctx, env.metadataRepo)
	require.NoError(t, err, "the indexer created R-metadata")
	require.Equal(t, env.dataNamespace+icebergcatalog.MetadataRepositorySuffix,
		metadata.StorageNamespace, "the derived metadata namespace")

	commits, _, err := env.catalog.ListCommits(ctx, env.metadataRepo, "main", lakefscatalog.LogParams{Amount: 100})
	require.NoError(t, err)
	bySource := make(map[string]*lakefscatalog.CommitLog)
	initCommits := 0
	for _, commit := range commits {
		if commit.Metadata[indexer.MetadataKeyInit] == indexer.MetadataKeyInitValue {
			initCommits++
		}
		if source := commit.Metadata[indexer.MetadataKeySourceCommit]; source != "" {
			bySource[source] = commit
		}
	}
	require.Equal(t, 1, initCommits, "exactly one initialisation commit")
	for _, source := range []*lakefscatalog.CommitLog{first, second} {
		recorded, ok := bySource[source.Reference]
		require.True(t, ok, "source commit %s has a lakeFS commit on R-metadata", source.Reference)
		require.Equal(t, fmt.Sprintf(indexer.IndexCommitMessage, env.dataRepo, source.Reference), recorded.Message)
		require.Equal(t, env.dataRepo, recorded.Metadata[indexer.MetadataKeySourceRepository])
		tag, err := env.catalog.GetTag(ctx, env.metadataRepo, indexer.TagPrefix+source.Reference)
		require.NoError(t, err, "tag mds-%s exists", source.Reference)
		require.Equal(t, recorded.Reference, tag, "the tag points at the commit that recorded the source commit")
	}

	head, err := env.catalog.GetCommit(ctx, env.metadataRepo, "main")
	require.NoError(t, err)
	require.Equal(t, second.Reference, head.Metadata[indexer.MetadataKeySourceCommit],
		"the branch head records the last indexed source commit")

	// The table a reader sees through the catalog: a remains from the first commit, b is gone,
	// c came in with the second.
	readCatalog, err := table.NewCatalog(ctx, table.CatalogConfig{
		Endpoint:        env.url,
		Repository:      env.metadataRepo,
		Branch:          "main",
		AccessKeyID:     env.accessKeyID,
		SecretAccessKey: env.secretKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = readCatalog.Close() })
	tbl, err := readCatalog.LoadTable(ctx, table.Identifier())
	require.NoError(t, err, "load system.object_metadata")
	require.Equal(t, second.Reference, tbl.Properties()[table.PropertySourceCommit])

	rows := scanMDSRows(t, tbl)
	paths := make([]string, 0, len(rows))
	for _, row := range rows {
		paths = append(paths, row.Path)
	}
	slices.Sort(paths)
	require.Equal(t, []string{"a", "c"}, paths)
	byPath := make(map[string]table.Row, len(rows))
	for _, row := range rows {
		byPath[row.Path] = row
	}
	require.Equal(t, first.Reference, byPath["a"].CommitID, "a was last changed by the first commit")
	require.Equal(t, second.Reference, byPath["c"].CommitID, "c was last changed by the second commit")
	require.Equal(t, int64(10), byPath["a"].SizeBytes)
	require.Equal(t, "etag-a", byPath["a"].ETag)
	require.Equal(t, "text/plain", *byPath["a"].ContentType)
	require.Equal(t, map[string]string{"X-Amz-Meta-Owner": "alice"}, byPath["a"].UserMetadata)
	_, exists := byPath["b"]
	require.False(t, exists, "the delete of b removed its row")
}

// scanMDSRows returns every row of a table, ordered by path. The column layout is the fixed schema
// of design §3.1.
func scanMDSRows(t *testing.T, tbl *icebergtable.Table) []table.Row {
	t.Helper()
	scan := tbl.Scan()
	defer func() { _ = scan.Close() }()
	_, batches, err := scan.ToArrowRecords(t.Context())
	require.NoError(t, err, "scan the table")

	var rows []table.Row
	for batch, err := range batches {
		require.NoError(t, err)
		for i := range int(batch.NumRows()) {
			rows = append(rows, decodeMDSRow(batch, i))
		}
		batch.Release()
	}
	slices.SortFunc(rows, func(a, b table.Row) int { return strings.Compare(a.Path, b.Path) })
	return rows
}

func decodeMDSRow(batch arrow.RecordBatch, i int) table.Row {
	return table.Row{
		Repository:   batch.Column(0).(*array.String).Value(i),
		Path:         batch.Column(1).(*array.String).Value(i),
		CommitID:     batch.Column(2).(*array.String).Value(i),
		SizeBytes:    batch.Column(3).(*array.Int64).Value(i),
		LastModified: time.UnixMicro(int64(batch.Column(4).(*array.Timestamp).Value(i))).UTC(),
		ETag:         batch.Column(5).(*array.String).Value(i),
		UserMetadata: mdsUserMetadata(batch.Column(6).(*array.Map), i),
		Committer:    mdsOptionalString(batch.Column(7).(*array.String), i),
		ContentType:  mdsOptionalString(batch.Column(8).(*array.String), i),
		CreationDate: mdsOptionalTimestamp(batch.Column(9).(*array.Timestamp), i),
	}
}

func mdsUserMetadata(values *array.Map, i int) map[string]string {
	if values.IsNull(i) {
		return nil
	}
	start, end := values.ValueOffsets(i)
	keys := values.Keys().(*array.String)
	items := values.Items().(*array.String)
	metadata := make(map[string]string, end-start)
	for j := start; j < end; j++ {
		metadata[keys.Value(int(j))] = items.Value(int(j))
	}
	return metadata
}

func mdsOptionalString(values *array.String, i int) *string {
	if values.IsNull(i) {
		return nil
	}
	value := values.Value(i)
	return &value
}

func mdsOptionalTimestamp(values *array.Timestamp, i int) *time.Time {
	if values.IsNull(i) {
		return nil
	}
	value := time.UnixMicro(int64(values.Value(i))).UTC()
	return &value
}
