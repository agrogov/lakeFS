package cleanup

// layout_test.go pins the path vocabulary: the mapping from a physical file address to the
// catalog's registration entry and back, the namespace checks that decide "external
// metadata", and the address normalization the sweep's set membership depends on.

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

func TestNormalizeAddress(t *testing.T) {
	for _, testCase := range []struct {
		in   string
		want string
	}{
		{in: "s3://bucket/ns/_iceberg/u/data/f.parquet", want: "s3://bucket/ns/_iceberg/u/data/f.parquet"},
		{in: "s3://bucket/ns//_iceberg/./u/data/f.parquet", want: "s3://bucket/ns/_iceberg/u/data/f.parquet"},
		{in: " s3://bucket/ns/x/ ", want: "s3://bucket/ns/x"},
		{in: "relative/path/", want: "relative/path"},
		{in: "", want: ""},
	} {
		require.Equal(t, testCase.want, normalizeAddress(testCase.in), "normalize %q", testCase.in)
	}
	// Two spellings of the same object must compare equal; two objects must not.
	require.Equal(t, normalizeAddress("s3://b/ns//x"), normalizeAddress("s3://b/ns/x"))
	require.NotEqual(t, normalizeAddress("s3://b/ns/x"), normalizeAddress("s3://other/ns/x"))
}

func TestStorageKey(t *testing.T) {
	namespace := "s3://bucket/prefix"
	key, err := storageKey(namespace, namespace+"/_iceberg/u/metadata/00001-x.metadata.json")
	require.NoError(t, err)
	require.Equal(t, "_iceberg/u/metadata/00001-x.metadata.json", key)

	// A trailing slash on the namespace is tolerated by trimTrailingSlashes in the Runner;
	// storageKey itself requires the location to be under the (already trimmed) namespace.
	_, err = storageKey(namespace+"/", namespace+"/_iceberg/u/x")
	require.NoError(t, err)

	for _, outside := range []string{
		"s3://other-bucket/ns/x",
		"s3://bucket/prefix2/x",
		"s3://bucket/prefix",
	} {
		_, err := storageKey(namespace, outside)
		require.Error(t, err, "storageKey(%q)", outside)
	}
}

func TestInTableArea(t *testing.T) {
	namespace := "s3://bucket/prefix"
	for _, testCase := range []struct {
		location string
		want     bool
	}{
		{location: "s3://bucket/prefix/_iceberg/u", want: true},
		{location: "s3://bucket/prefix/_iceberg", want: true},
		{location: "s3://bucket/prefix/_iceberg/u/metadata/x", want: true},
		{location: "s3://bucket/prefix/other/u", want: false},
		{location: "s3://bucket/prefix/_icebergx/u", want: false},
		{location: "s3://other-bucket/prefix/_iceberg/u", want: false},
	} {
		require.Equal(t, testCase.want, inTableArea(namespace, testCase.location), "inTableArea(%q)", testCase.location)
	}
}

func TestFileEntryPathRoundTrip(t *testing.T) {
	tableUUID := uuid.MustParse(testTableUUIDText)
	root := "s3://bucket/prefix/_iceberg/" + tableUUID.String()

	entryPath, err := fileEntryPath(tableUUID, root, root+"/data/part-0.parquet")
	require.NoError(t, err)
	require.Equal(t, icebergcatalog.ReservedPrefix+"/"+icebergcatalog.FilesDirectory+"/"+tableUUID.String()+"/data/part-0.parquet", entryPath)

	parsed, ok := tableFileEntryUUID(entryPath)
	require.True(t, ok)
	require.Equal(t, tableUUID, parsed)

	// A file outside the root is refused rather than registered under a confusing key.
	_, err = fileEntryPath(tableUUID, root, "s3://bucket/prefix/_iceberg/other/data/f.parquet")
	require.Error(t, err)

	// A path that climbs out of the root with ".." is refused by TableFilePath.
	_, err = fileEntryPath(tableUUID, root, root+"/data/../../escape.parquet")
	require.Error(t, err)

	// A stray entry under the reserved prefix is not attributed to a table.
	for _, stray := range []string{
		icebergcatalog.ReservedPrefix + "/files/",
		icebergcatalog.ReservedPrefix + "/files/not-a-uuid/data/f.parquet",
		icebergcatalog.ReservedPrefix + "/tables/ns/t/.pointer",
		"_lakefs_iceberg/filesX/" + tableUUID.String() + "/data/f.parquet",
	} {
		_, ok := tableFileEntryUUID(stray)
		require.False(t, ok, "tableFileEntryUUID(%q)", stray)
	}
}

func TestIsPointerPath(t *testing.T) {
	require.True(t, isPointerPath(pointerPathOf("ns", "table")))
	require.False(t, isPointerPath(icebergcatalog.ReservedPrefix+"/files/"+testTableUUIDText+"/data/f.parquet"))
	require.False(t, isPointerPath(icebergcatalog.ReservedPrefix+"/tables/ns/table/other"))
}

func TestMetadataRootCandidates(t *testing.T) {
	require.Equal(t, []string{
		"s3://bucket/ns/_iceberg/u",
		"s3://bucket/ns/_iceberg/u/metadata",
	}, metadataRootCandidates("s3://bucket/ns/_iceberg/u/metadata/00001-x.metadata.json"))

	// A metadata file whose parent is not called "metadata" still gets both guesses: the
	// file may sit in the table root itself, or one directory below it.
	require.Equal(t, []string{
		"s3://bucket/ns/_iceberg/u",
		"s3://bucket/ns/_iceberg/u/metadata",
	}, metadataRootCandidates("s3://bucket/ns/_iceberg/u/metadata/x.json"))

	require.Empty(t, metadataRootCandidates(""))
}

func TestReportMarshalling(t *testing.T) {
	objects := []MarkObject{
		{
			Address:      "s3://bucket/ns/_iceberg/u/data/a.parquet",
			TableUUID:    testTableUUIDText,
			SizeBytes:    12,
			LastModified: time.Unix(1700000000, 0).UTC(),
			Registrations: []MarkRegistration{
				{Branch: "main", Path: "_lakefs_iceberg/files/u/data/a.parquet"},
			},
		},
	}
	data, err := marshalJSONLines(objects)
	require.NoError(t, err)
	parsed, err := unmarshalMarkObjects(data)
	require.NoError(t, err)
	require.Equal(t, objects, parsed)

	_, err = unmarshalMarkObjects([]byte("{\"address\":\"\"}\n"))
	require.Error(t, err, "an object without an address must not be accepted")

	_, err = unmarshalMarkObjects([]byte("not json\n"))
	require.Error(t, err)
}

func TestCheckStatus(t *testing.T) {
	require.NoError(t, checkStatus(http.StatusOK, true, nil, "op", "subject"))
	require.ErrorIs(t, checkStatus(http.StatusNotFound, false, nil, "op", "subject"), ErrNotFound)
	require.ErrorIs(t, checkStatus(http.StatusForbidden, false, []byte(`{"message":"no"}`), "op", "subject"), ErrForbidden)
	err := checkStatus(http.StatusInternalServerError, false, []byte(`{"message":"boom"}`), "op", "subject")
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
	require.Contains(t, err.Error(), "500")
}
