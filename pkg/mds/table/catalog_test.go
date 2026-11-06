package table_test

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// TestCreateTableSchema pins the table design of §3.1: the columns, their Iceberg types, their
// requiredness, the identifier field and the sort order. The field IDs of the created table are
// pinned separately, in TestCreateTableFieldIDs.
func TestCreateTableSchema(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	schema := tbl.Schema()

	require.Equal(t, 2, tbl.Metadata().Version(), "the table is Iceberg format v2")
	require.Equal(t, []int{2}, schema.IdentifierFieldIDs, "path is the identifier field")
	require.Len(t, schema.Fields(), 10)

	want := []struct {
		name     string
		iceType  string
		required bool
	}{
		{table.ColumnRepository, "string", true},
		{table.ColumnPath, "string", true},
		{table.ColumnCommitID, "string", true},
		{table.ColumnSizeBytes, "long", true},
		{table.ColumnLastModified, "timestamptz", true},
		{table.ColumnETag, "string", true},
		{table.ColumnUserMetadata, "map", true},
		{table.ColumnCommitter, "string", false},
		{table.ColumnContentType, "string", false},
		{table.ColumnCreationDate, "timestamptz", false},
	}
	for i, field := range schema.Fields() {
		require.Equal(t, want[i].name, field.Name, "column %d", i)
		require.Equal(t, want[i].iceType, field.Type.Type(), "column %s", field.Name)
		require.Equal(t, want[i].required, field.Required, "column %s requiredness", field.Name)
	}

	userMetadata, ok := schema.FindFieldByName(table.ColumnUserMetadata)
	require.True(t, ok, "user_metadata is a column")
	mapType, ok := userMetadata.Type.(*iceberg.MapType)
	require.True(t, ok, "user_metadata is a map")
	require.Equal(t, iceberg.StringType{}, mapType.KeyType)
	require.Equal(t, iceberg.StringType{}, mapType.ValueType)
	// Design §3.1 declares the map value optional: iceberg-go reads a parquet map column with
	// nullable items, so a required value breaks its own copy-on-write rewrite and compaction.
	// The indexer still never writes a null value, and an empty map - never a null - is an
	// object without user metadata.
	require.False(t, mapType.ValueRequired, "the map value is optional (§3.1)")
	require.NotZero(t, mapType.KeyID, "the map key has an ID")
	require.NotZero(t, mapType.ValueID, "the map value has an ID")

	path, ok := schema.FindFieldByName(table.ColumnPath)
	require.True(t, ok)
	order := tbl.SortOrder()
	require.Equal(t, 1, order.Len(), "one sort field")
	field := order.Field(0)
	require.Equal(t, path.ID, field.SourceID())
	require.Equal(t, "asc", string(field.Direction))
}

// TestCreateTableFieldIDs pins every field ID of a created table (§3.1): 1-7 for the columns up to
// and including user_metadata, 8 and 9 for the map's key and value (they sit inside user_metadata,
// so the catalog's pre-order renumbering gives them the IDs right after the map field), and 10-12
// for the trailing optional columns. The IDs are written into every data and delete file, so they
// are part of the table's compatibility surface: a change to them is a schema change, not a
// refactoring.
//
// The test also pins the two ways those numbers enter the code: the constants of schema.go, and
// the create request Schema returns. The catalog renumbers the request in pre-order
// (iceberg-go's NewMetadataWithUUID calls AssignFreshSchemaIDs), so a request built in the created
// table's pre-order keeps its IDs. Readers and the writer still take them from the loaded table
// (Table.Schema()), never from a constant; this test only says the two agree at creation.
func TestCreateTableFieldIDs(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	schema := tbl.Schema()

	want := []struct {
		name string
		id   int
	}{
		{table.ColumnRepository, table.FieldRepository},
		{table.ColumnPath, table.FieldPath},
		{table.ColumnCommitID, table.FieldCommitID},
		{table.ColumnSizeBytes, table.FieldSizeBytes},
		{table.ColumnLastModified, table.FieldLastModified},
		{table.ColumnETag, table.FieldETag},
		{table.ColumnUserMetadata, table.FieldUserMetadata},
		{table.ColumnCommitter, table.FieldCommitter},
		{table.ColumnContentType, table.FieldContentType},
		{table.ColumnCreationDate, table.FieldCreationDate},
	}
	// The literal IDs of §3.1's table, spelled out so that the constants cannot drift silently.
	literal := []int{1, 2, 3, 4, 5, 6, 7, 10, 11, 12}
	require.Len(t, schema.Fields(), len(want))
	for i, field := range schema.Fields() {
		require.Equal(t, want[i].name, field.Name, "column %d", i)
		require.Equal(t, literal[i], field.ID, "the created ID of column %s", field.Name)
		require.Equal(t, want[i].id, field.ID, "the constant for column %s is the created ID", field.Name)
	}

	userMetadata, ok := schema.FindFieldByName(table.ColumnUserMetadata)
	require.True(t, ok, "user_metadata is a column")
	mapType, ok := userMetadata.Type.(*iceberg.MapType)
	require.True(t, ok, "user_metadata is a map")
	require.Equal(t, 8, mapType.KeyID, "the created ID of the map key")
	require.Equal(t, 9, mapType.ValueID, "the created ID of the map value")
	require.Equal(t, table.FieldUserMetadataKey, mapType.KeyID, "the constant FieldUserMetadataKey is the created key ID")
	require.Equal(t, table.FieldUserMetadataValue, mapType.ValueID, "the constant FieldUserMetadataValue is the created value ID")

	// The create request carries the created table's IDs, so the catalog's renumbering is a no-op.
	request := table.Schema()
	require.Len(t, request.Fields(), len(want))
	for i, field := range request.Fields() {
		require.Equal(t, schema.Fields()[i].ID, field.ID, "the create request ID of column %s", field.Name)
	}
	requestMap, ok := request.Fields()[6].Type.(*iceberg.MapType)
	require.True(t, ok, "the create request's user_metadata is a map")
	require.Equal(t, mapType.KeyID, requestMap.KeyID, "the create request's map key ID")
	require.Equal(t, mapType.ValueID, requestMap.ValueID, "the create request's map value ID")

	// Identifier ids are rewritten by the same pass; path keeps ID 2.
	require.Equal(t, []int{2}, schema.IdentifierFieldIDs, "the identifier field is path (ID 2)")
}

// TestCreateTableProperties pins the table properties of §3.1.
func TestCreateTableProperties(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	props := tbl.Properties()

	require.Equal(t, table.WriteModeMergeOnRead, props[table.PropertyWriteDeleteMode])
	require.Equal(t, table.WriteModeMergeOnRead, props[table.PropertyWriteUpdateMode])
	require.Equal(t, table.WriteCompressionCodecZSTD, props[table.PropertyWriteCompressionCodec])
	require.Equal(t, table.SchemaVersion, props[table.PropertySchemaVersion])
	require.Equal(t, env.dataRepository(), props[table.PropertyRepository])
}

// TestCreateTableKeepsFixedProperties: a caller may add properties but not drop the fixed ones.
func TestCreateTableKeepsFixedProperties(t *testing.T) {
	env := newTestEnv(t)
	tbl, err := table.CreateTable(t.Context(), env.rest, env.dataRepository(), iceberg.Properties{
		"custom.property":             "yes",
		table.PropertyWriteDeleteMode: "copy-on-write",
	})
	require.NoError(t, err)
	require.Equal(t, "yes", tbl.Properties()["custom.property"])
	require.Equal(t, table.WriteModeMergeOnRead, tbl.Properties()[table.PropertyWriteDeleteMode])
}

// TestCatalogConfigURI pins the relative_to address of §4.3 step 3, including a branch name with a
// slash in it: the warehouse is one URL path segment.
func TestCatalogConfigURI(t *testing.T) {
	tests := []struct {
		name     string
		cfg      table.CatalogConfig
		want     string
		wantErrs bool
	}{
		{
			name: "plain",
			cfg: table.CatalogConfig{
				Endpoint: "http://lakefs:8000", Repository: "repo-metadata", Branch: "main",
				AccessKeyID: "key", SecretAccessKey: "secret",
			},
			want: "http://lakefs:8000/iceberg/relative_to/repo-metadata.main/api",
		},
		{
			name: "trailing slash and a branch with a slash",
			cfg: table.CatalogConfig{
				Endpoint: "https://lakefs.example.com/", Repository: "repo-metadata", Branch: "feature/x",
				AccessKeyID: "key", SecretAccessKey: "secret",
			},
			want: "https://lakefs.example.com/iceberg/relative_to/repo-metadata.feature%2Fx/api",
		},
		{
			name:     "no endpoint",
			cfg:      table.CatalogConfig{Repository: "repo-metadata", Branch: "main", AccessKeyID: "key", SecretAccessKey: "secret"},
			wantErrs: true,
		},
		{
			name:     "relative endpoint",
			cfg:      table.CatalogConfig{Endpoint: "lakefs:8000", Repository: "repo-metadata", Branch: "main", AccessKeyID: "key", SecretAccessKey: "secret"},
			wantErrs: true,
		},
		{
			name:     "no credentials",
			cfg:      table.CatalogConfig{Endpoint: "http://lakefs:8000", Repository: "repo-metadata", Branch: "main"},
			wantErrs: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.cfg.URI()
			if tc.wantErrs {
				require.ErrorIs(t, err, table.ErrInvalidCatalogConfig)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestAzureSchemeRegistered pins the Azure backend import of METADATA_SEARCH_AZURE.md D3. Without
// it iceberg-go's IO registry answers io.ErrIOSchemeNotFound ("io scheme not registered") for the
// abfss:// location of a table on an Azure blockstore, so the indexer would fail every branch of
// every repository on an Azure installation before writing one row.
//
// The credentials are fake and no request is made: io.LoadFS builds the Azure container client (the
// shared key only has to be valid base64), so this test asserts the scheme resolution of the
// registry, not a reachable account.
func TestAzureSchemeRegistered(t *testing.T) {
	props := map[string]string{
		icebergio.ADLSSharedKeyAccountName: "acct",
		icebergio.ADLSSharedKeyAccountKey:  base64.StdEncoding.EncodeToString([]byte("not-a-real-key")),
		icebergio.ADLSEndpoint:             "azurite.test:10000",
		icebergio.ADLSProtocol:             "http",
	}

	iofs, err := icebergio.LoadFS(t.Context(), props,
		"abfss://container@acct.azurite.test/tables/t/metadata/v1.metadata.json")
	require.NoError(t, err, "the abfss scheme must be registered, not refused as unsupported")
	require.False(t, errors.Is(err, icebergio.ErrIOSchemeNotFound))
	require.NotNil(t, iofs, "io.LoadFS reached the Azure backend")

	// The backend registers every scheme of the ADLS URI pattern, so neither an abfs:// location (the
	// non-TLS form, D1) nor one of the deprecated wasb[s]// spellings a client may have written is
	// refused.
	for _, scheme := range []string{"abfs", "abfss", "wasb", "wasbs"} {
		t.Run(scheme, func(t *testing.T) {
			iofs, err := icebergio.LoadFS(t.Context(), props, scheme+"://container@acct.azurite.test/tables/t")
			require.NoError(t, err)
			require.NotNil(t, iofs)
		})
	}
}

// TestS3SchemeStillRegistered pins the other half of the import: adding the Azure backend next to the
// S3 one leaves the S3 resolution of iceberg-go's registry unchanged.
func TestS3SchemeStillRegistered(t *testing.T) {
	iofs, err := icebergio.LoadFS(t.Context(), map[string]string{
		"s3.endpoint":          "http://seaweedfs:8333",
		"s3.access-key-id":     "AKIAEXAMPLE",
		"s3.secret-access-key": "secret",
		"s3.region":            "us-east-1",
	}, "s3://bucket/_iceberg/table/metadata/v1.metadata.json")
	require.NoError(t, err)
	require.NotNil(t, iofs)
}

// TestNewCatalogConcurrentSharedStorage builds catalogs concurrently from one Storage map, as the
// indexer does for the branches of a repository. iceberg-go writes the server's config
// properties into the map it is given, so a shared map crashed the indexer with "concurrent map
// writes"; the caller's map must also stay unchanged.
func TestNewCatalogConcurrentSharedStorage(t *testing.T) {
	env := newTestEnv(t)
	storage := iceberg.Properties{"s3.region": "us-east-1"}

	const catalogs = 8
	var wg sync.WaitGroup
	errs := make(chan error, catalogs)
	for range catalogs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := table.NewCatalog(t.Context(), env.catalogConfig(storage))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, iceberg.Properties{"s3.region": "us-east-1"}, storage, "the caller's Storage map is not modified")
}

// TestCatalogConfigAuthorization: the credentials are sent as HTTP basic, which the catalog accepts
// on every route.
func TestCatalogConfigAuthorization(t *testing.T) {
	cfg := table.CatalogConfig{AccessKeyID: "AKIA", SecretAccessKey: "secret"}
	require.Equal(t, "Basic QUtJQTpzZWNyZXQ=", cfg.Authorization())
}

// TestNewCatalogRequestTimeout pins the bound of design §4.1 that iceberg-go does not have: it
// builds its own http.Client with no Timeout (catalog/rest/rest.go createSession), so a connection
// that accepts the request and never answers must fail on CatalogConfig.RequestTimeout instead of
// blocking the caller - for the indexer that caller is a branch tick that holds the whole tick
// loop (MDS_REVIEW.md IX-2, TB-3).
//
// Both exchanges are covered because they run on differently built clients in iceberg-go: the
// GET /v1/config of NewCatalog (fetchConfig) and every later table call (createSession).
func TestNewCatalogRequestTimeout(t *testing.T) {
	const requestTimeout = 200 * time.Millisecond

	// hangingCatalog answers GET /v1/config and never answers anything else, so a subtest can
	// choose the exchange it stalls. The handler returns when the client gives up, so closing the
	// server cannot block.
	hangingCatalog := func(t *testing.T, answerConfig bool) *httptest.Server {
		t.Helper()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if answerConfig && strings.HasSuffix(r.URL.Path, "/v1/config") {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{}`))
				return
			}
			<-r.Context().Done()
		}))
		t.Cleanup(server.Close)
		return server
	}

	catalogConfig := func(endpoint string) table.CatalogConfig {
		return table.CatalogConfig{
			Endpoint:        endpoint,
			Repository:      "data-repo-metadata",
			Branch:          "main",
			AccessKeyID:     "AKIAEXAMPLE",
			SecretAccessKey: "secret",
			RequestTimeout:  requestTimeout,
		}
	}

	t.Run("config exchange", func(t *testing.T) {
		server := hangingCatalog(t, false)
		start := time.Now()
		cat, err := table.NewCatalog(t.Context(), catalogConfig(server.URL))
		elapsed := time.Since(start)
		require.Error(t, err, "NewCatalog must not hang on GET /v1/config")
		require.Nil(t, cat)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.GreaterOrEqual(t, elapsed, requestTimeout,
			"the failure waited for the request bound, so it is the timeout and not a refused connection")
	})

	t.Run("table call", func(t *testing.T) {
		server := hangingCatalog(t, true)
		cat, err := table.NewCatalog(t.Context(), catalogConfig(server.URL))
		require.NoError(t, err, "GET /v1/config answers")
		t.Cleanup(func() { _ = cat.Close() })

		start := time.Now()
		_, err = cat.LoadTable(t.Context(), table.Identifier())
		elapsed := time.Since(start)
		require.Error(t, err, "LoadTable must not hang on a catalog that never answers")
		require.True(t, errors.Is(err, context.DeadlineExceeded), "the error is the request timeout, got %v", err)
		require.GreaterOrEqual(t, elapsed, requestTimeout)
	})
}
