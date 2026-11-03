package importer

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelativeToIsStrict(t *testing.T) {
	root := "s3://bucket/warehouse/orders/"
	relative, err := relativeTo(root, "s3://bucket/warehouse/orders/data/00000.parquet")
	require.NoError(t, err)
	require.Equal(t, "data/00000.parquet", relative)

	for _, location := range []string{
		"s3://bucket/warehouse/orders-archive/data/00000.parquet", // a sibling with a shared prefix
		"s3://bucket/warehouse/orders",                            // the root itself
		"s3://bucket/warehouse/orders/",                           // the root with a trailing slash
		"s3://bucket/warehouse/orders//data.parquet",              // an empty segment
		"s3://bucket/warehouse/orders/../secret.parquet",          // a reserved segment
		"s3://bucket/warehouse/orders/./data.parquet",
		"",
	} {
		_, err := relativeTo(root, location)
		require.ErrorIs(t, err, ErrTableCopy, location)
	}

	require.True(t, inside("s3://bucket/warehouse/orders", "s3://bucket/warehouse/orders/data"))
	require.False(t, inside("s3://bucket/warehouse/orders", "s3://bucket/warehouse/orders"))
	require.False(t, inside("s3://bucket/warehouse/orders", "s3://bucket/warehouse/ordersx"))
}

func TestJoinLocation(t *testing.T) {
	require.Equal(t, "s3://bucket/t/data.parquet", joinLocation("s3://bucket/t/", "/data.parquet"))
	require.Equal(t, "s3://bucket/t/data.parquet", joinLocation(" s3://bucket/t ", "data.parquet"))
}

func TestNormalizeLocation(t *testing.T) {
	require.Equal(t, "s3://bucket/t", normalizeLocation("  s3://bucket/t/  "))
}

// TestCopyTableUUIDIsStableAndDistinct pins the two properties the copy relies on: a retry of the
// same import lands on the same location, and two target tables of one source table do not share
// a uuid (and therefore do not share the catalog's _lakefs_iceberg/files/<uuid>/ entries).
func TestCopyTableUUIDIsStableAndDistinct(t *testing.T) {
	const metadataLocation = "s3://source/warehouse/orders/metadata/00001.metadata.json"

	first := copyTableUUID(metadataLocation, []string{"sales"}, "orders")
	require.Equal(t, first, copyTableUUID(metadataLocation, []string{"sales"}, "orders"), "a retry reuses the location")
	require.NotEqual(t, first, copyTableUUID(metadataLocation, []string{"imported", "sales"}, "orders"),
		"a different target namespace gets a different uuid")
	require.NotEqual(t, first, copyTableUUID(metadataLocation, []string{"sales"}, "orders_copy"),
		"a different table name gets a different uuid")
	require.NotEqual(t, first, copyTableUUID(metadataLocation+"-v2", []string{"sales"}, "orders"),
		"a different source version gets a different uuid")
}

func TestUploadCopyPlanRejectsATargetOutsideTheStorageNamespace(t *testing.T) {
	plan := &copyPlan{files: []copiedFile{{target: "s3://other-bucket/_iceberg/u/data.parquet", content: []byte("x")}}}
	uploader := &recordingUploader{}
	err := uploadCopyPlan(t.Context(), uploader, nil, "repo", "main", "s3://bucket/repo", plan)
	require.ErrorIs(t, err, ErrTableCopy)
	require.Empty(t, uploader.keys)
}

type recordingUploader struct {
	keys []string
}

func (u *recordingUploader) Upload(_ context.Context, _, _, key string, _ int64, _ io.Reader) error {
	u.keys = append(u.keys, key)
	return nil
}
