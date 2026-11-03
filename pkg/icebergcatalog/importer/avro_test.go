package importer

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// icebergTestFixture is a real v2 manifest and manifest list written by iceberg-go, the library
// the lakeFS catalog itself reads them with. Using the real writer (and the real reader in the
// assertions) is what makes the rewrite test meaningful: a synthetic Avro schema would prove only
// that the importer agrees with itself.
type icebergTestFixture struct {
	sourceRoot   string
	dataPath     string
	manifestPath string
	manifestList string
	manifest     []byte
	list         []byte
	snapshotID   int64
}

// buildIcebergTestFixture writes one unpartitioned v2 manifest with one ADDED data file and the
// manifest list that points at it.
func buildIcebergTestFixture(t *testing.T, sourceRoot string) icebergTestFixture {
	t.Helper()
	const snapshotID = int64(3055729675574597004)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true})
	spec := iceberg.NewPartitionSpec()

	fixture := icebergTestFixture{
		sourceRoot:   sourceRoot,
		dataPath:     sourceRoot + "/data/00000-0-abc.parquet",
		manifestPath: sourceRoot + "/metadata/m0.avro",
		snapshotID:   snapshotID,
	}

	dataFileBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, fixture.dataPath,
		iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	dataFile := dataFileBuilder.Build()
	snapshotIDPtr := snapshotID
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotIDPtr, nil, nil, dataFile)

	var manifestBuffer bytes.Buffer
	manifestWriter, err := iceberg.NewManifestWriter(2, &manifestBuffer, spec, schema, snapshotID)
	require.NoError(t, err)
	require.NoError(t, manifestWriter.Add(entry))
	require.NoError(t, manifestWriter.Close())
	manifestLength := int64(manifestBuffer.Len())
	manifestFile, err := manifestWriter.ToManifestFile(fixture.manifestPath, manifestLength)
	require.NoError(t, err)
	fixture.manifest = manifestBuffer.Bytes()

	var listBuffer bytes.Buffer
	listWriter, err := iceberg.NewManifestListWriterV2(&listBuffer, snapshotID, 1, nil)
	require.NoError(t, err)
	require.NoError(t, listWriter.AddManifests([]iceberg.ManifestFile{manifestFile}))
	require.NoError(t, listWriter.Close())
	fixture.list = listBuffer.Bytes()
	fixture.manifestList = sourceRoot + "/metadata/snap-1.avro"
	return fixture
}

// TestRewriteManifestAndManifestList rewrites a real Iceberg manifest and manifest list and reads
// them back with iceberg-go: the data file and manifest paths move to the new root, the entries'
// data are otherwise untouched, and the manifest-list record's manifest_length is the re-encoded
// manifest's size.
func TestRewriteManifestAndManifestList(t *testing.T) {
	fixture := buildIcebergTestFixture(t, "s3://source/warehouse/orders")
	targetRoot := "mem://repo-ns/_iceberg/11111111-1111-1111-1111-111111111111"
	rewrite := func(location string) (string, error) {
		relative, err := relativeTo(fixture.sourceRoot, location)
		require.NoError(t, err)
		return joinLocation(targetRoot, relative), nil
	}

	rewrittenManifest, refs, err := rewriteManifest(fixture.manifest, rewrite)
	require.NoError(t, err)
	require.Equal(t, []dataFileRef{{Location: fixture.dataPath, Status: manifestEntryStatusAdded}}, refs)
	require.NotEqual(t, len(fixture.manifest), len(rewrittenManifest), "the fixture's length assertion must be meaningful")

	rewrittenList, err := rewriteManifestList(fixture.list, rewrite, func(manifest string) (int64, error) {
		require.Equal(t, fixture.manifestPath, manifest)
		return int64(len(rewrittenManifest)), nil
	})
	require.NoError(t, err)

	manifests, err := iceberg.ReadManifestList(bytes.NewReader(rewrittenList))
	require.NoError(t, err)
	require.Len(t, manifests, 1)
	require.Equal(t, targetRoot+"/metadata/m0.avro", manifests[0].FilePath())
	require.Equal(t, int64(len(rewrittenManifest)), manifests[0].Length())
	require.Equal(t, fixture.snapshotID, manifests[0].SnapshotID())

	reader, err := iceberg.NewManifestReader(manifests[0], bytes.NewReader(rewrittenManifest))
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	readEntry, err := reader.ReadEntry()
	require.NoError(t, err)
	require.Equal(t, iceberg.EntryStatusADDED, readEntry.Status())
	require.Equal(t, targetRoot+"/data/00000-0-abc.parquet", readEntry.DataFile().FilePath())
	require.Equal(t, int64(128), readEntry.DataFile().FileSizeBytes())

	_, err = reader.ReadEntry()
	require.True(t, errors.Is(err, io.EOF))
}

// TestRewriteManifestRefusesForeignFile pins the copy's per-table refusal: a data file outside the
// source table root has no well-defined relative path, so it is never silently placed somewhere.
func TestRewriteManifestRefusesForeignFile(t *testing.T) {
	fixture := buildIcebergTestFixture(t, "s3://source/warehouse/orders")
	rewrite := func(location string) (string, error) {
		_, err := relativeTo(fixture.sourceRoot, "s3://another-team/secret/file.parquet")
		return "", err
	}
	_, _, err := rewriteManifest(fixture.manifest, rewrite)
	require.ErrorIs(t, err, ErrTableCopy)
}

func TestAvroCodecIsPreserved(t *testing.T) {
	codec, err := avroCodec(map[string][]byte{"avro.codec": []byte("deflate")})
	require.NoError(t, err)
	require.Equal(t, "deflate", codec.Name())

	_, err = avroCodec(map[string][]byte{"avro.codec": []byte("bzip2")})
	require.ErrorIs(t, err, ErrTableCopy)
}
