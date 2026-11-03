package importer

// End-to-end tests of the import against the real lakeFS Iceberg REST catalog.
//
// The target is not a fake: it is pkg/icebergcatalog mounted on an httptest server over an
// in-memory graveler catalog and block adapter, with the production router and the register,
// namespace and table services. So a successful copy test proves that the rewritten document,
// the rewritten manifests and the uploaded objects are what the catalog's own register operation
// accepts, stages and serves back - including the manifest_length bookkeeping, the
// _lakefs_iceberg/files staging and the foreign-location control.
//
// The source is an httptest server that speaks the Iceberg REST protocol and serves the table's
// files over http, which is exactly the shape of a catalog fronting a web-served warehouse. Its
// manifests are written by iceberg-go, the same library the catalog reads.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem" // register the "mem" KV driver
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	importTestRepo     = "analytics"
	importTestBranch   = "main"
	importTestTable    = "orders"
	importTestSourceNS = "sales"
)

var (
	importTestConfigOnce sync.Once
	importTestConfigBase config.ConfigImpl
	importTestConfigErr  error
)

// importTestConfig builds the process configuration the catalog needs, once.
func importTestConfig(t *testing.T) *config.ConfigImpl {
	t.Helper()
	importTestConfigOnce.Do(func() {
		viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
		_, importTestConfigErr = config.NewConfig("", &importTestConfigBase)
	})
	require.NoError(t, importTestConfigErr)
	cfg := importTestConfigBase
	cfg.Committed.LocalCache.Dir = path.Join(t.TempDir(), "cache")
	cfg.Committed.LocalCache.SizeBytes = 24 << 20
	cfg.Committed.SSTable.Memory.CacheSizeBytes = 2 << 20
	return &cfg
}

// allowAllAuthorizer grants every catalog permission. The tests are about the import's
// interaction with the catalog, not about the authorization matrix, which pkg/icebergcatalog tests
// on its own.
type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(context.Context, *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{Allowed: true}, nil
}

// importTestCatalog is the real catalog plus the state a test asserts on.
type importTestCatalog struct {
	ctx              context.Context
	store            *icebergcatalog.Store
	catalog          *catalog.Catalog
	server           *httptest.Server
	baseURL          string
	storageNamespace string
}

// newImportTestCatalog builds a lakeFS Iceberg catalog on an in-memory KV and block store.
func newImportTestCatalog(t *testing.T) *importTestCatalog {
	t.Helper()
	ctx := t.Context()

	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err)
	t.Cleanup(kvStore.Close)

	cat, err := catalog.New(ctx, catalog.Config{
		Config:       importTestConfig(t),
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })

	storageNamespace := "mem://import-target"
	_, err = cat.CreateRepository(ctx, importTestRepo, "", storageNamespace, importTestBranch, false)
	require.NoError(t, err)

	store := icebergcatalog.NewStore(cat, cat.BlockAdapter)
	authorizer := icebergcatalog.NewAuthorizer(allowAllAuthorizer{})
	cfg := &icebergcatalog.Config{Prefix: "lakefs"}
	router, err := icebergcatalog.NewRouter(injectImporterUser, cfg,
		icebergcatalog.NewNamespaceService(store, authorizer, cat, cfg),
		icebergcatalog.NewTableService(store, authorizer, cat, cfg),
		icebergcatalog.NewRegisterService(store, authorizer, cat, cfg))
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.Handle("/iceberg/api/", http.StripPrefix("/iceberg/api", router))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return &importTestCatalog{
		ctx:              ctx,
		store:            store,
		catalog:          cat,
		server:           server,
		baseURL:          server.URL + "/iceberg/api",
		storageNamespace: storageNamespace,
	}
}

// injectImporterUser authenticates every catalog request as one test principal, replacing the
// catalog's own credential middleware.
func injectImporterUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := &model.User{Username: "importer"}
		next.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), user)))
	})
}

// importTestSource is the source catalog: a REST surface and a file surface on one httptest
// server, with the table's files generated by iceberg-go.
type importSourceCatalog struct {
	server           *httptest.Server
	metadataLocation string
	metadata         []byte
	files            map[string][]byte
	dataContent      []byte
	snapshotID       int64
}

// newImportTestSource builds a one-namespace, one-table source catalog.
func newImportTestSource(t *testing.T, formatVersion int) *importSourceCatalog {
	t.Helper()
	source := &importSourceCatalog{
		files:       map[string][]byte{},
		dataContent: []byte("not-really-a-parquet-file"),
		snapshotID:  3055729675574597004,
	}
	mux := http.NewServeMux()
	source.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if data, ok := source.files[r.URL.Path]; ok {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			_, _ = w.Write(data)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(source.server.Close)

	sourceRoot := source.server.URL + "/warehouse/sales/orders"
	fixture := buildIcebergTestFixture(t, sourceRoot)
	source.snapshotID = fixture.snapshotID
	source.files["/warehouse/sales/orders/metadata/snap-1.avro"] = fixture.list
	source.files["/warehouse/sales/orders/metadata/m0.avro"] = fixture.manifest
	source.files["/warehouse/sales/orders/data/00000-0-abc.parquet"] = source.dataContent

	metadata := map[string]any{
		"format-version":        formatVersion,
		"table-uuid":            "9c12d441-03fe-4693-9a96-a0705ddf69c1",
		"location":              sourceRoot,
		"last-sequence-number":  1,
		"last-updated-ms":       1602638573590,
		"last-column-id":        1,
		"current-schema-id":     0,
		"schemas":               []any{map[string]any{"type": "struct", "schema-id": 0, "fields": []any{map[string]any{"id": 1, "name": "id", "required": true, "type": "long"}}}},
		"default-spec-id":       0,
		"partition-specs":       []any{map[string]any{"spec-id": 0, "fields": []any{}}},
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders":           []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties":            map[string]string{"owner": "data-team"},
		"current-snapshot-id":   fixture.snapshotID,
		"snapshots":             []any{map[string]any{"snapshot-id": fixture.snapshotID, "timestamp-ms": 1555100955770, "sequence-number": 1, "summary": map[string]string{"operation": "append"}, "manifest-list": fixture.manifestList}},
		"snapshot-log":          []any{map[string]any{"snapshot-id": fixture.snapshotID, "timestamp-ms": 1555100955770}},
		"metadata-log":          []any{},
		"refs":                  map[string]any{"main": map[string]any{"snapshot-id": fixture.snapshotID, "type": "branch"}},
	}
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)

	source.metadata = metadataJSON
	source.metadataLocation = sourceRoot + "/metadata/00001.metadata.json"
	source.files["/warehouse/sales/orders/metadata/00001.metadata.json"] = metadataJSON

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, map[string]any{"overrides": map[string]string{"prefix": "source"}})
	})
	mux.HandleFunc("/v1/source/namespaces", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("parent") == "" {
			writeTestJSON(t, w, map[string]any{"namespaces": [][]string{{importTestSourceNS}}})
			return
		}
		writeTestJSON(t, w, map[string]any{"namespaces": [][]string{}})
	})
	mux.HandleFunc("/v1/source/namespaces/sales/tables", func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, map[string]any{"identifiers": []map[string]any{
			{"namespace": []string{importTestSourceNS}, "name": importTestTable},
		}})
	})
	mux.HandleFunc("/v1/source/namespaces/sales/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, map[string]any{
			"metadata-location": source.metadataLocation,
			"metadata":          json.RawMessage(source.metadata),
		})
	})
	return source
}

// countingUploader wraps the block-adapter uploader and counts uploads.
type countingUploader struct {
	inner blockUploader
	count atomic.Int64
}

func (u *countingUploader) Upload(ctx context.Context, repository, branch, key string, size int64, body io.Reader) error {
	u.count.Add(1)
	return u.inner.Upload(ctx, repository, branch, key, size, body)
}

// blockUploader stands in for the lakeFS object API: it writes the key under the repository's
// storage namespace with the catalog's block adapter, which is the physical effect the object API
// has and the location the rewritten metadata points at.
type blockUploader struct {
	adapter          block.Adapter
	storageNamespace string
}

func (u blockUploader) Upload(ctx context.Context, _, _ string, key string, size int64, body io.Reader) error {
	location := joinLocation(u.storageNamespace, key)
	_, err := u.adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: u.storageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	}, size, body, block.PutOpts{})
	return err
}

// newImportOptions builds the options of a copy import of the source's one table.
func newImportOptions(t *testing.T, source *importSourceCatalog, target *importTestCatalog, uploader Uploader, copyMode bool) Options {
	t.Helper()
	options := Options{
		Source:           mustSource(t, source),
		Target:           mustTarget(t, target),
		Repository:       importTestRepo,
		Branch:           importTestBranch,
		StorageNamespace: target.storageNamespace,
		Namespaces:       []Namespace{{importTestSourceNS}},
		Copy:             copyMode,
		Uploader:         uploader,
		FileReader:       NewFileReader(nil, http.DefaultClient),
	}
	return options
}

func mustSource(t *testing.T, source *importSourceCatalog) *Source {
	t.Helper()
	parsed, err := NewSource(SourceConfig{URI: source.server.URL, HTTPClient: http.DefaultClient})
	require.NoError(t, err)
	return parsed
}

func mustTarget(t *testing.T, target *importTestCatalog) *Target {
	t.Helper()
	parsed, err := NewTarget(TargetConfig{BaseURL: target.baseURL, HTTPClient: http.DefaultClient})
	require.NoError(t, err)
	return parsed
}

// TestImportCopyModeAgainstRealCatalog imports a table from an http-served source into the real
// catalog and checks the result end to end: the register succeeds, the table loads back with
// rewritten locations, the copied documents carry the new paths, and the catalog staged the files
// as _lakefs_iceberg entries.
func TestImportCopyModeAgainstRealCatalog(t *testing.T) {
	source := newImportTestSource(t, 2)
	target := newImportTestCatalog(t)
	uploader := &countingUploader{inner: blockUploader{adapter: target.catalog.BlockAdapter, storageNamespace: target.storageNamespace}}

	refBefore, err := target.catalog.GetBranchReference(target.ctx, importTestRepo, importTestBranch)
	require.NoError(t, err)

	summary, err := Import(target.ctx, newImportOptions(t, source, target, uploader, true))
	require.NoError(t, err)
	require.Equal(t, 1, summary.Imported)
	require.Equal(t, 0, summary.Failed)
	require.NotZero(t, uploader.count.Load(), "the copy uploaded the table's files")

	result := summary.Results[0]
	require.Equal(t, StatusImported, result.Status)
	require.Equal(t, "sales.orders", result.Source)
	require.True(t, strings.HasPrefix(result.MetadataLocation, target.storageNamespace+"/_iceberg/"),
		"the registered metadata lives in the managed table area: %s", result.MetadataLocation)

	loaded := loadTargetTable(t, target)
	require.True(t, strings.HasPrefix(loaded.MetadataLocation, target.storageNamespace+"/_iceberg/"))
	var document map[string]any
	require.NoError(t, json.Unmarshal(loaded.Metadata, &document))
	require.True(t, strings.HasPrefix(document["location"].(string), target.storageNamespace+"/_iceberg/"))
	require.NotEqual(t, "9c12d441-03fe-4693-9a96-a0705ddf69c1", document["table-uuid"],
		"the copy gets a table uuid of its own")
	require.NotContains(t, document, "metadata-log")

	// The copied manifest list and manifest are in the target storage namespace and carry the
	// rewritten paths: the target catalog can only have registered them if its own
	// iceberg-go reader accepted them and found every file under the table root.
	manifests, err := target.catalog.BlockAdapter.Get(target.ctx, block.ObjectPointer{
		StorageNamespace: target.storageNamespace,
		Identifier:       document["snapshots"].([]any)[0].(map[string]any)["manifest-list"].(string),
		IdentifierType:   block.IdentifierTypeFull,
	})
	require.NoError(t, err)
	manifestListBytes, err := io.ReadAll(manifests)
	require.NoError(t, err)
	_ = manifests.Close()
	manifestFiles, err := iceberg.ReadManifestList(bytes.NewReader(manifestListBytes))
	require.NoError(t, err)
	require.Len(t, manifestFiles, 1)
	require.True(t, strings.HasPrefix(manifestFiles[0].FilePath(), target.storageNamespace+"/_iceberg/"),
		"the copied manifest lives under the new table root: %s", manifestFiles[0].FilePath())

	// The catalog staged the table's files under _lakefs_iceberg/files/<uuid>/ (files.go §6.3),
	// which is what makes the copied table GC-safe and visible to diff.
	entries, _, err := target.catalog.ListEntries(target.ctx, importTestRepo, importTestBranch,
		"_lakefs_iceberg/files/", "", "", -1)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	// Nothing was committed: the import stages on the branch, and a commit publishes it.
	refAfter, err := target.catalog.GetBranchReference(target.ctx, importTestRepo, importTestBranch)
	require.NoError(t, err)
	require.Equal(t, refBefore, refAfter, "an import must not create a commit")
}

// TestImportInPlaceAgainstRealCatalog is the default mode's success path: the table's files are
// already inside the repository's storage namespace, so register adopts them where they are, with
// no copy and no upload.
func TestImportInPlaceAgainstRealCatalog(t *testing.T) {
	target := newImportTestCatalog(t)
	tableUUID := "22222222-2222-2222-2222-222222222222"
	sourceRoot := joinLocation(target.storageNamespace, "_iceberg/"+tableUUID)
	fixture := buildIcebergTestFixture(t, sourceRoot)
	target.putObject(t, fixture.manifestList, fixture.list)
	target.putObject(t, fixture.manifestPath, fixture.manifest)
	target.putObject(t, fixture.dataPath, []byte("data"))

	metadataLocation := sourceRoot + "/metadata/00001.metadata.json"
	metadata := inPlaceTestMetadata(fixture, sourceRoot)
	target.putObject(t, metadataLocation, metadata)
	sourceServer := startSourceCatalog(t, metadataLocation, metadata)

	source, err := NewSource(SourceConfig{URI: sourceServer.URL, HTTPClient: http.DefaultClient})
	require.NoError(t, err)
	summary, err := Import(target.ctx, Options{
		Source:           source,
		Target:           mustTarget(t, target),
		Repository:       importTestRepo,
		Branch:           importTestBranch,
		StorageNamespace: target.storageNamespace,
		Namespaces:       []Namespace{{importTestSourceNS}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, summary.Imported)
	require.Equal(t, metadataLocation, summary.Results[0].MetadataLocation, "in-place mode adopts the source document")

	loaded := loadTargetTable(t, target)
	require.Equal(t, metadataLocation, loaded.MetadataLocation)
	require.JSONEq(t, string(metadata), string(loaded.Metadata))
}

// inPlaceTestMetadata builds the metadata document of a table whose files already live under
// sourceRoot; the table uuid is the one the catalog will stage the files under.
func inPlaceTestMetadata(fixture icebergTestFixture, sourceRoot string) []byte {
	const tableUUID = "22222222-2222-2222-2222-222222222222"
	document := map[string]any{
		"format-version":        2,
		"table-uuid":            tableUUID,
		"location":              sourceRoot,
		"last-sequence-number":  1,
		"last-updated-ms":       1602638573590,
		"last-column-id":        1,
		"current-schema-id":     0,
		"schemas":               []any{map[string]any{"type": "struct", "schema-id": 0, "fields": []any{map[string]any{"id": 1, "name": "id", "required": true, "type": "long"}}}},
		"default-spec-id":       0,
		"partition-specs":       []any{map[string]any{"spec-id": 0, "fields": []any{}}},
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders":           []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties":            map[string]string{"owner": "data-team"},
		"current-snapshot-id":   fixture.snapshotID,
		"snapshots":             []any{map[string]any{"snapshot-id": fixture.snapshotID, "timestamp-ms": 1555100955770, "sequence-number": 1, "summary": map[string]string{"operation": "append"}, "manifest-list": fixture.manifestList}},
		"snapshot-log":          []any{map[string]any{"snapshot-id": fixture.snapshotID, "timestamp-ms": 1555100955770}},
		"refs":                  map[string]any{"main": map[string]any{"snapshot-id": fixture.snapshotID, "type": "branch"}},
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return encoded
}

// putObject writes one object under the target repository's storage namespace, the way a client
// that owns the warehouse would have.
func (c *importTestCatalog) putObject(t *testing.T, location string, content []byte) {
	t.Helper()
	_, err := c.catalog.BlockAdapter.Put(c.ctx, block.ObjectPointer{
		StorageNamespace: c.storageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	}, int64(len(content)), bytes.NewReader(content), block.PutOpts{})
	require.NoError(t, err)
}

// startSourceCatalog serves a source REST catalog for one namespace and one table.
func startSourceCatalog(t *testing.T, metadataLocation string, metadata []byte) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, map[string]any{"overrides": map[string]string{"prefix": "source"}})
	})
	mux.HandleFunc("/v1/source/namespaces", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("parent") == "" {
			writeTestJSON(t, w, map[string]any{"namespaces": [][]string{{importTestSourceNS}}})
			return
		}
		writeTestJSON(t, w, map[string]any{"namespaces": [][]string{}})
	})
	mux.HandleFunc("/v1/source/namespaces/sales/tables", func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, map[string]any{"identifiers": []map[string]any{
			{"namespace": []string{importTestSourceNS}, "name": importTestTable},
		}})
	})
	mux.HandleFunc("/v1/source/namespaces/sales/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, map[string]any{
			"metadata-location": metadataLocation,
			"metadata":          json.RawMessage(metadata),
		})
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

// TestImportIsResumable skips a table that already exists, and replaces it with --overwrite.
func TestImportIsResumable(t *testing.T) {
	source := newImportTestSource(t, 2)
	target := newImportTestCatalog(t)
	uploader := blockUploader{adapter: target.catalog.BlockAdapter, storageNamespace: target.storageNamespace}

	summary, err := Import(target.ctx, newImportOptions(t, source, target, uploader, true))
	require.NoError(t, err)
	require.Equal(t, 1, summary.Imported)

	options := newImportOptions(t, source, target, uploader, true)
	summary, err = Import(target.ctx, options)
	require.NoError(t, err)
	require.Equal(t, 0, summary.Imported)
	require.Equal(t, 1, summary.Skipped)
	require.Contains(t, summary.Results[0].Detail, "already exists")

	options.Overwrite = true
	summary, err = Import(target.ctx, options)
	require.NoError(t, err)
	require.Equal(t, 1, summary.Imported)
}

// TestImportRefusesForeignLocationWithoutCopy is the default mode's security answer: a table whose
// files are not in the repository storage namespace is reported, not registered, and the report
// names --copy.
func TestImportRefusesForeignLocationWithoutCopy(t *testing.T) {
	source := newImportTestSource(t, 2)
	target := newImportTestCatalog(t)
	options := newImportOptions(t, source, target, nil, false)

	summary, err := Import(target.ctx, options)
	require.NoError(t, err)
	require.Equal(t, 1, summary.Failed)
	require.Equal(t, StatusFailed, summary.Results[0].Status)
	require.Contains(t, summary.Results[0].Detail, "--copy")

	require.Equal(t, http.StatusNotFound, targetTableStatus(t, target))
}

// TestImportRefusesFormatVersion3 pins the v3 refusal: it is per table, with the reason.
func TestImportRefusesFormatVersion3(t *testing.T) {
	source := newImportTestSource(t, 3)
	target := newImportTestCatalog(t)
	uploader := blockUploader{adapter: target.catalog.BlockAdapter, storageNamespace: target.storageNamespace}

	summary, err := Import(target.ctx, newImportOptions(t, source, target, uploader, true))
	require.NoError(t, err)
	require.Equal(t, 1, summary.Failed)
	require.Contains(t, summary.Results[0].Detail, "format version 3")
}

// TestImportDryRunWritesNothing plans without uploading or registering.
func TestImportDryRunWritesNothing(t *testing.T) {
	source := newImportTestSource(t, 2)
	target := newImportTestCatalog(t)
	uploader := &countingUploader{inner: blockUploader{adapter: target.catalog.BlockAdapter, storageNamespace: target.storageNamespace}}

	options := newImportOptions(t, source, target, uploader, true)
	options.DryRun = true
	summary, err := Import(target.ctx, options)
	require.NoError(t, err)
	require.Equal(t, 1, summary.Planned)
	require.Zero(t, uploader.count.Load())
	require.Equal(t, http.StatusNotFound, targetTableStatus(t, target))
}

// TestImportCreatesTheTargetNamespace checks the namespace create that precedes register.
func TestImportCreatesTheTargetNamespace(t *testing.T) {
	source := newImportTestSource(t, 2)
	target := newImportTestCatalog(t)
	uploader := blockUploader{adapter: target.catalog.BlockAdapter, storageNamespace: target.storageNamespace}

	options := newImportOptions(t, source, target, uploader, true)
	options.TargetPrefix = []string{"imported"}
	summary, err := Import(target.ctx, options)
	require.NoError(t, err)
	require.Equal(t, 1, summary.Imported)
	require.Equal(t, StatusImported, summary.Results[0].Status)

	_, err = target.catalog.GetEntry(target.ctx, importTestRepo, importTestBranch,
		"_lakefs_iceberg/ns/imported/sales/.namespace", catalog.GetEntryParams{})
	require.NoError(t, err, "the target namespace marker is staged")
}

// targetTableStatus returns the status of a LoadTable of the imported table on the target.
func targetTableStatus(t *testing.T, target *importTestCatalog) int {
	t.Helper()
	url := fmt.Sprintf("%s/v1/lakefs/namespaces/%s%%1F%s%%1F%s/tables/%s",
		target.baseURL, importTestRepo, importTestBranch, importTestSourceNS, importTestTable)
	resp, err := http.Get(url) //nolint:gosec // a test server URL
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// loadTargetTable loads the imported table through the catalog and decodes it.
func loadTargetTable(t *testing.T, target *importTestCatalog) LoadedTable {
	t.Helper()
	url := fmt.Sprintf("%s/v1/lakefs/namespaces/%s%%1F%s%%1F%s/tables/%s",
		target.baseURL, importTestRepo, importTestBranch, importTestSourceNS, importTestTable)
	resp, err := http.Get(url) //nolint:gosec // a test server URL
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var loaded LoadedTable
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&loaded))
	return loaded
}
