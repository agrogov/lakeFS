package table_test

import (
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// TestCreateTableSchema pins the table design of §3.1: the columns, their Iceberg types, their
// requiredness, the identifier field and the sort order.
//
// The field IDs are not the literal ones of the design's table. This catalog assigns field IDs
// itself when it applies a create request (iceberg-go's NewMetadataWithUUID reassigns every ID in
// pre-order), so user_metadata's key and value take the IDs after the map field and the trailing
// optional columns follow them. The column set, the types, the requiredness and the identifier
// are exactly the design's, and once created the IDs are stable, which is what schema evolution
// and the fixed schema require. See Schema's documentation.
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
	// Design §3.1 says the map value is required. It is optional here for the reason Schema
	// documents: iceberg-go cannot rewrite a table whose map value it reads back as nullable.
	require.False(t, mapType.ValueRequired, "the map value is optional; see Schema")
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
