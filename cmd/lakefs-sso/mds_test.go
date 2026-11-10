package main

// mds_test.go is the RA4b test: `mds run` against the same KV store and blockstore as an
// in-process lakeFS server.
//
// The fixture is a real lakeFS catalog over a local (in-memory) KV store and the local block
// adapter, the production Iceberg catalog routes over HTTP, a data repository with one commit, and
// a configuration file that names the same database, blockstore and auth key the server uses. The
// command is then executed the way the official chart invokes it - `mds run --config <file>`
// through the server binary's root command - so the test covers the whole wiring: the server
// configuration loader, the KV store opened from database.*, the catalog built over blockstore.*,
// the in-process repository reads and the REST table writes.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
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
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/table"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	testSecretKey       = "mds-run-test-encrypt-secret-key-0123456789"
	testAccessKeyID     = "AKIAMDSRUNTEST0000001"
	testSecretAccessKey = "mds-run-test-secret-access-key-0123456789"
	testPrincipal       = "mds-indexer"
	testDataRepository  = "data"
	testBranch          = "main"
)

var mdsRunLocalIOScheme sync.Once

// mdsRunLocalIO serves iceberg-go's local:// locations from the filesystem: the local block
// adapter is the test's blockstore, and pkg/mds/table registers only the production cloud IO
// schemes. The production mapping from blockstore.* to IO properties is pinned separately in
// TestBlockstoreIOProperties.
type mdsRunLocalIO struct{}

func mdsRunLocalPath(name string) string { return strings.TrimPrefix(name, "local://") }

func (mdsRunLocalIO) Open(name string) (io.File, error) {
	return io.LocalFS{}.Open(mdsRunLocalPath(name))
}
func (mdsRunLocalIO) Remove(name string) error { return io.LocalFS{}.Remove(mdsRunLocalPath(name)) }
func (mdsRunLocalIO) ReadFile(name string) ([]byte, error) {
	return io.LocalFS{}.ReadFile(mdsRunLocalPath(name))
}
func (mdsRunLocalIO) Create(name string) (io.FileWriter, error) {
	return io.LocalFS{}.Create(mdsRunLocalPath(name))
}
func (mdsRunLocalIO) WriteFile(name string, content []byte) error {
	return io.LocalFS{}.WriteFile(mdsRunLocalPath(name), content)
}

func registerMDSRunLocalIO() {
	mdsRunLocalIOScheme.Do(func() {
		io.Register("local", func(_ context.Context, _ *url.URL, _ map[string]string) (io.IO, error) {
			return mdsRunLocalIO{}, nil
		})
	})
}

// serverFixture is the in-process lakeFS the indexer shares its KV store and blockstore with.
type serverFixture struct {
	t        *testing.T
	root     string
	kvPath   string
	cfg      *config.ConfigImpl
	cat      *lakefscatalog.Catalog
	server   *httptest.Server
	head     *lakefscatalog.CommitLog
	dataNS   string
	metaRepo string
}

// newServerFixture builds the catalog and mounts the production Iceberg catalog routes over HTTP.
func newServerFixture(t *testing.T) *serverFixture {
	t.Helper()
	ctx := t.Context()
	registerMDSRunLocalIO()

	root := t.TempDir()
	kvPath := fmt.Sprintf("memory:mds-run-%d", time.Now().UnixNano())

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)
	viper.Set("blockstore.local.path", root)
	viper.Set("database.type", "local")
	viper.Set("database.local.path", kvPath)
	viper.Set("auth.encrypt.secret_key", testSecretKey)
	viper.Set("iceberg.enabled", true)
	viper.Set("iceberg.metadata_search.enabled", true)
	viper.Set("committed.local_cache.size_bytes", 24*1024*1024)
	viper.Set("committed.sstable.memory.cache_size_bytes", 2*1024*1024)
	viper.Set("logging.format", "text")
	viper.Set("logging.level", "NONE")
	viper.Set("logging.output", "-")

	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err, "build the lakeFS configuration")
	cfg.Committed.LocalCache.Dir = filepath.Join(root, "server-cache")

	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "local", Local: &kvparams.Local{Path: kvPath}})
	require.NoError(t, err, "open the shared KV store")
	t.Cleanup(kvStore.Close)

	authService := auth.NewBasicAuthService(kvStore, crypt.NewSecretStore([]byte(testSecretKey)),
		authparams.ServiceCache{Enabled: false}, logging.FromContext(ctx))
	_, err = authService.CreateUser(ctx, &authmodel.User{Username: testPrincipal})
	require.NoError(t, err, "create the indexer principal")
	_, err = authService.AddCredentials(ctx, testPrincipal, testAccessKeyID, testSecretAccessKey)
	require.NoError(t, err, "add the indexer principal's credentials")

	cat, err := lakefscatalog.New(context.WithoutCancel(ctx), lakefscatalog.Config{
		Config:       cfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err, "build the lakeFS catalog")
	t.Cleanup(func() { _ = cat.Close() })

	dataNS := "local://" + filepath.Join(root, "repos", testDataRepository)
	_, err = cat.CreateRepository(ctx, testDataRepository, "", dataNS, testBranch, false)
	require.NoError(t, err, "create the data repository")

	for _, path := range []string{"orders/a.parquet", "orders/b.parquet"} {
		require.NoError(t, cat.CreateEntry(ctx, testDataRepository, testBranch, dataEntry(t, path)))
	}
	head, err := cat.Commit(ctx, testDataRepository, testBranch, "add two objects", "tester", nil, nil, nil, false)
	require.NoError(t, err, "commit the data repository")

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
	}, mux), "mount the Iceberg catalog routes")

	return &serverFixture{
		t:        t,
		root:     root,
		kvPath:   kvPath,
		cfg:      cfg,
		cat:      cat,
		server:   httptest.NewServer(mux),
		head:     head,
		dataNS:   dataNS,
		metaRepo: icebergcatalog.MetadataRepository(testDataRepository),
	}
}

// dataEntry is one object of the data repository's commit. The indexer never reads the object
// body: the row comes from the commit's entry metadata.
func dataEntry(t *testing.T, path string) lakefscatalog.DBEntry {
	t.Helper()
	return lakefscatalog.DBEntry{
		Path:            path,
		PhysicalAddress: "local://" + filepath.Join(t.TempDir(), path),
		CreationDate:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Size:            int64(len(path)),
		Checksum:        "etag-" + path,
		Metadata:        lakefscatalog.Metadata{"X-Amz-Meta-Owner": "alice"},
		ContentType:     "application/octet-stream",
	}
}

// configFile writes the server configuration plus the metadata_search section the indexer reads.
// It names the server's own database, blockstore and auth key, which is what the official chart
// gives the MDS pod.
func (f *serverFixture) configFile() string {
	f.t.Helper()
	document := fmt.Sprintf(`logging:
  format: text
  level: NONE
  output: "-"

database:
  type: local
  local:
    path: %s

auth:
  encrypt:
    secret_key: %s

blockstore:
  type: local
  local:
    path: %s

committed:
  local_cache:
    dir: %s
    size_bytes: 25165824
  sstable:
    memory:
      cache_size_bytes: 2097152

iceberg:
  enabled: true
  metadata_search:
    enabled: true

metadata_search:
  lakefs_endpoint: %s
  access_key_id: %s
  secret_key: %s
  listen_address: 127.0.0.1:0
  period: 50ms
  concurrency: 1
  max_commits: 100
  repositories:
    %s:
      branches: [%s]
`, f.kvPath, testSecretKey, f.root, filepath.Join(f.root, "mds-cache"), f.server.URL,
		testAccessKeyID, testSecretAccessKey, testDataRepository, testBranch)

	path := filepath.Join(f.t.TempDir(), "config.yaml")
	require.NoError(f.t, os.WriteFile(path, []byte(document), 0o600), "write the mds configuration file")

	// Read the file into viper here as well: `lakefsCmd.LoadConfig` does that once per process,
	// and the test asserts on the configuration file, so a repeated run (-count>1) must not fall
	// back to the previous run's viper state.
	viper.SetConfigFile(path)
	viper.SetEnvPrefix("LAKEFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	require.NoError(f.t, viper.ReadInConfig(), "read the mds configuration file")
	return path
}

// TestMDSRunAgainstAnInProcessServer runs the command as the chart invokes it and asserts that the
// indexer read the data repository from the shared KV store, wrote the table through the REST
// catalog into the shared blockstore, and committed the indexed position to the shared KV store.
func TestMDSRunAgainstAnInProcessServer(t *testing.T) {
	f := newServerFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	root := cmdpkg.GetRoot()
	root.AddCommand(newMDSCommand())
	root.SetArgs([]string{"mds", "run", "--config", f.configFile()})
	// cobra keeps the context of the command it executed, so a repeated run of this test in one
	// process would execute with the previous run's (cancelled) context. Clear it: the child picks
	// up this execution's context from the root.
	clearCommandContexts(root)

	done := make(chan error, 1)
	go func() {
		done <- root.ExecuteContext(ctx)
	}()

	// The indexer creates the companion repository and commits one indexed source commit; the tag
	// mds-<C> is written after the commit.
	isIndexed := func() bool {
		ctx := context.WithoutCancel(t.Context())
		commit, err := f.cat.GetCommit(ctx, f.metaRepo, testBranch)
		if err != nil || commit.Metadata[indexer.MetadataKeySourceCommit] != f.head.Reference {
			return false
		}
		// finishCommit writes the commit and then the mds-<C> tag; the tick is done with the
		// branch only when both are there.
		tag, err := f.cat.GetTag(ctx, f.metaRepo, indexer.TagPrefix+f.head.Reference)
		return err == nil && tag == commit.Reference
	}
	deadline := time.Now().Add(30 * time.Second)
	for !isIndexed() {
		select {
		case err := <-done:
			t.Fatalf("mds run returned before indexing: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("mds run did not index the data repository's head")
		}
	}

	// The companion repository is the indexer's own, in the namespace the guide derives for it.
	metadata, err := f.cat.GetRepository(t.Context(), f.metaRepo)
	require.NoError(t, err)
	require.Equal(t, f.dataNS+icebergcatalog.MetadataRepositorySuffix, metadata.StorageNamespace,
		"the companion namespace is the data namespace with the suffix appended")
	require.False(t, metadata.ReadOnly)

	// The commits were made in process, by the Metadata Search principal: the same identity the
	// server-side ownership proof checks.
	commits, _, err := f.cat.ListCommits(t.Context(), f.metaRepo, testBranch, lakefscatalog.LogParams{Amount: 100})
	require.NoError(t, err)
	var initCommit *lakefscatalog.CommitLog
	var indexed *lakefscatalog.CommitLog
	for _, commit := range commits {
		if len(commit.Parents) > 0 {
			// The repository's root commit is lakeFS's own; every commit the indexer made names
			// the Metadata Search principal.
			require.Equal(t, icebergcatalog.MetadataSearchIndexerPrincipal, commit.Committer,
				"the indexer commits as the Metadata Search principal")
		}
		if commit.Metadata[indexer.MetadataKeyInit] == indexer.MetadataKeyInitValue {
			initCommit = commit
		}
		if commit.Metadata[indexer.MetadataKeySourceCommit] == f.head.Reference {
			indexed = commit
		}
	}
	require.NotNil(t, initCommit, "the companion repository has an initialisation commit")
	require.NotNil(t, indexed, "the head of the data repository was indexed")
	require.Equal(t, testDataRepository, indexed.Metadata[indexer.MetadataKeySourceRepository])
	require.Equal(t, fmt.Sprintf(indexer.IndexCommitMessage, testDataRepository, f.head.Reference), indexed.Message)

	tag, err := f.cat.GetTag(t.Context(), f.metaRepo, indexer.TagPrefix+f.head.Reference)
	require.NoError(t, err, "tag mds-<C> records the indexed source commit")
	require.Equal(t, indexed.Reference, tag)

	// The table a reader sees through the catalog: the rows of the data repository's commit, read
	// back from the blockstore the indexer wrote them to.
	readCatalog, err := table.NewCatalog(t.Context(), table.CatalogConfig{
		Endpoint:        f.server.URL,
		Repository:      f.metaRepo,
		Branch:          testBranch,
		AccessKeyID:     testAccessKeyID,
		SecretAccessKey: testSecretAccessKey,
	})
	require.NoError(t, err, "build the reading catalog")
	t.Cleanup(func() { _ = readCatalog.Close() })

	tbl, err := readCatalog.LoadTable(t.Context(), table.Identifier())
	require.NoError(t, err, "load system.object_metadata")
	require.Equal(t, f.head.Reference, tbl.Properties()[table.PropertySourceCommit])

	rows := scanMDSRunRows(t, tbl)
	require.Len(t, rows, 2)
	paths := []string{rows[0].Path, rows[1].Path}
	slices.Sort(paths)
	require.Equal(t, []string{"orders/a.parquet", "orders/b.parquet"}, paths)
	for _, row := range rows {
		require.Equal(t, f.head.Reference, row.CommitID)
		require.Equal(t, testDataRepository, row.Repository)
		require.NotNil(t, row.Committer)
		require.Equal(t, "tester", *row.Committer)
	}

	// The command stops gracefully when its context is cancelled, which is what SIGTERM does.
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "a cancelled context is a graceful stop")
	case <-time.After(30 * time.Second):
		t.Fatal("mds run did not return after its context was cancelled")
	}
}

// clearCommandContexts clears the context cobra stored on every command, so the next
// ExecuteContext hands the executed command the context of that execution.
func clearCommandContexts(cmd *cobra.Command) {
	cmd.SetContext(nil)
	for _, child := range cmd.Commands() {
		clearCommandContexts(child)
	}
}

// TestOpenMDSCatalogFailsOnAnUnusableDatabase pins the startup rule: the indexer opens the
// database from database.*, so a database it cannot open fails startup the way it fails `run`
// instead of leaving a process that ticks and never indexes.
func TestOpenMDSCatalogFailsOnAnUnusableDatabase(t *testing.T) {
	notADirectory := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(notADirectory, []byte("x"), 0o600))

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)
	viper.Set("blockstore.local.path", t.TempDir())
	viper.Set("database.type", "local")
	viper.Set("database.local.path", notADirectory)
	viper.Set("logging.level", "NONE")
	viper.Set("logging.output", "-")

	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err, "build the lakeFS configuration")

	store, cat, err := openMDSCatalog(t.Context(), cfg)
	require.Error(t, err, "an unusable database fails startup")
	require.Nil(t, store)
	require.Nil(t, cat)
	require.Contains(t, err.Error(), "open the KV store from database.*")
}

// TestBlockstoreIOProperties pins the IO switch of a table write: the iceberg-go properties come
// from the standard blockstore.* configuration, which replaced the removed metadata_search.storage
// and metadata_search.azure keys (X6, RA4b).
func TestBlockstoreIOProperties(t *testing.T) {
	tests := []struct {
		name  string
		viper map[string]any
		want  map[string]string
	}{
		{
			name: "s3 with static credentials",
			viper: map[string]any{
				config.BlockstoreTypeKey:                      block.BlockstoreTypeS3,
				"blockstore.s3.region":                        "us-east-1",
				"blockstore.s3.endpoint":                      "http://seaweedfs:8333",
				"blockstore.s3.force_path_style":              true,
				"blockstore.s3.credentials.access_key_id":     testAccessKeyID,
				"blockstore.s3.credentials.secret_access_key": testSecretAccessKey,
				"blockstore.s3.credentials.session_token":     "session",
			},
			want: map[string]string{
				"client.region":               "us-east-1",
				"s3.region":                   "us-east-1",
				"s3.endpoint":                 "http://seaweedfs:8333",
				"s3.access-key-id":            testAccessKeyID,
				"s3.secret-access-key":        testSecretAccessKey,
				"s3.session-token":            "session",
				"s3.force-virtual-addressing": "false",
			},
		},
		{
			name: "azure shared key",
			viper: map[string]any{
				config.BlockstoreTypeKey:              block.BlockstoreTypeAzure,
				"blockstore.azure.storage_account":    "acct",
				"blockstore.azure.storage_access_key": testSecretAccessKey,
			},
			want: map[string]string{
				"adls.auth.shared-key.account.name": "acct",
				"adls.auth.shared-key.account.key":  testSecretAccessKey,
			},
		},
		{
			name: "local has no static IO properties",
			viper: map[string]any{
				config.BlockstoreTypeKey: block.BlockstoreTypeLocal,
				"blockstore.local.path":  t.TempDir(),
			},
			want: nil,
		},
		{
			name: "s3 without static credentials leaves the default chain",
			viper: map[string]any{
				config.BlockstoreTypeKey: block.BlockstoreTypeS3,
				"blockstore.s3.region":   "us-east-1",
			},
			want: map[string]string{
				"client.region": "us-east-1",
				"s3.region":     "us-east-1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			viper.Set("logging.level", "NONE")
			viper.Set("logging.output", "-")
			for key, value := range test.viper {
				viper.Set(key, value)
			}
			cfg := &config.ConfigImpl{}
			_, err := config.NewConfig("", cfg)
			require.NoError(t, err, "build the lakeFS configuration")

			props, err := blockstoreIOProperties(cfg.GetBaseConfig())
			require.NoError(t, err)
			require.Equal(t, test.want, map[string]string(props))
		})
	}
}

// scanMDSRunRows returns every row of the metadata table, ordered by path. The column layout is the
// fixed schema of design §3.1.
func scanMDSRunRows(t *testing.T, tbl *icebergtable.Table) []table.Row {
	t.Helper()
	scan := tbl.Scan()
	defer func() { _ = scan.Close() }()
	_, batches, err := scan.ToArrowRecords(t.Context())
	require.NoError(t, err, "scan the table")

	var rows []table.Row
	for batch, err := range batches {
		require.NoError(t, err)
		for i := range int(batch.NumRows()) {
			rows = append(rows, decodeMDSRunRow(batch, i))
		}
		batch.Release()
	}
	slices.SortFunc(rows, func(a, b table.Row) int { return strings.Compare(a.Path, b.Path) })
	return rows
}

func decodeMDSRunRow(batch arrow.RecordBatch, i int) table.Row {
	return table.Row{
		Repository:   batch.Column(0).(*array.String).Value(i),
		Path:         batch.Column(1).(*array.String).Value(i),
		CommitID:     batch.Column(2).(*array.String).Value(i),
		SizeBytes:    batch.Column(3).(*array.Int64).Value(i),
		LastModified: time.UnixMicro(int64(batch.Column(4).(*array.Timestamp).Value(i))).UTC(),
		ETag:         batch.Column(5).(*array.String).Value(i),
		UserMetadata: mdsRunUserMetadata(batch.Column(6).(*array.Map), i),
		Committer:    mdsRunOptionalString(batch.Column(7).(*array.String), i),
		ContentType:  mdsRunOptionalString(batch.Column(8).(*array.String), i),
		CreationDate: mdsRunOptionalTimestamp(batch.Column(9).(*array.Timestamp), i),
	}
}

func mdsRunUserMetadata(values *array.Map, i int) map[string]string {
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

func mdsRunOptionalString(values *array.String, i int) *string {
	if values.IsNull(i) {
		return nil
	}
	value := values.Value(i)
	return &value
}

func mdsRunOptionalTimestamp(values *array.Timestamp, i int) *time.Time {
	if values.IsNull(i) {
		return nil
	}
	value := time.UnixMicro(int64(values.Value(i))).UTC()
	return &value
}
