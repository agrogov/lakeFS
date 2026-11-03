package importer

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

const metadataTestDocument = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://source/warehouse/sales/orders",
  "last-sequence-number": 3,
  "last-updated-ms": 1602638573590,
  "last-column-id": 1,
  "current-schema-id": 0,
  "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {
    "owner": "data-team",
    "write.data.path": "s3://source/warehouse/sales/orders/data",
    "write.metadata.path": "s3://elsewhere/metadata",
    "write.metadata.statistics.path": "s3://source/warehouse/sales/orders/metadata"
  },
  "current-snapshot-id": 3055729675574597004,
  "snapshots": [
    {"snapshot-id": 3055729675574597004, "sequence-number": 1, "timestamp-ms": 1555100955770,
     "summary": {"operation": "append"},
     "manifest-list": "s3://source/warehouse/sales/orders/metadata/snap-1.avro"},
    {"snapshot-id": 3055729675574597005, "sequence-number": 2, "timestamp-ms": 1555100955771,
     "summary": {"operation": "append"},
     "manifest-list": "s3://source/warehouse/sales/orders/metadata/snap-1.avro",
     "manifests": ["s3://source/warehouse/sales/orders/metadata/old-m.avro", "s3://source/warehouse/sales/orders/metadata/old-m.avro"]}
  ],
  "snapshot-log": [{"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}],
  "metadata-log": [{"metadata-file": "s3://source/warehouse/sales/orders/metadata/0.metadata.json", "timestamp-ms": 1515100}],
  "statistics": [{"snapshot-id": 3055729675574597004, "statistics-path": "s3://source/warehouse/sales/orders/metadata/s.stats", "file-size-in-bytes": 10, "file-footer-size-in-bytes": 1, "blob-metadata": []}],
  "partition-statistics": [{"snapshot-id": 3055729675574597004, "statistics-path": "s3://source/warehouse/sales/orders/metadata/p.stats", "file-size-in-bytes": 10, "file-footer-size-in-bytes": 1}],
  "unknown-future-field": {"kept": true}
}`

func TestParseMetadataReadsTheFieldsTheImportUses(t *testing.T) {
	metadata, err := ParseMetadata([]byte(metadataTestDocument))
	require.NoError(t, err)

	version, err := metadata.FormatVersion()
	require.NoError(t, err)
	require.Equal(t, 2, version)
	require.Equal(t, "s3://source/warehouse/sales/orders", metadata.Location())
	require.Equal(t, "9c12d441-03fe-4693-9a96-a0705ddf69c1", metadata.TableUUID())

	require.Equal(t, []string{
		"s3://source/warehouse/sales/orders/metadata/snap-1.avro",
	}, metadata.ManifestListLocations())
	require.Equal(t, []string{
		"s3://source/warehouse/sales/orders/metadata/old-m.avro",
	}, metadata.InlineManifestLocations())
	require.Equal(t, metadataTestDocument, string(metadata.Original()), "the original bytes are what register adopts")
}

func TestMetadataRewriteForCopy(t *testing.T) {
	metadata, err := ParseMetadata([]byte(metadataTestDocument))
	require.NoError(t, err)

	sourceRoot := "s3://source/warehouse/sales/orders"
	targetRoot := "mem://repo-ns/_iceberg/11111111-1111-1111-1111-111111111111"
	rewrite := func(location string) (string, error) {
		relative, err := relativeTo(sourceRoot, location)
		require.NoError(t, err)
		return joinLocation(targetRoot, relative), nil
	}
	require.NoError(t, metadata.RewriteForCopy(sourceRoot, targetRoot, "11111111-1111-1111-1111-111111111111", rewrite))

	encoded, err := metadata.JSON()
	require.NoError(t, err)

	var document map[string]any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&document))

	require.Equal(t, targetRoot, document["location"])
	require.Equal(t, "11111111-1111-1111-1111-111111111111", document["table-uuid"])
	require.Contains(t, document, "unknown-future-field", "unknown fields survive the rewrite")
	require.NotContains(t, document, "metadata-log")
	require.NotContains(t, document, "statistics")
	require.NotContains(t, document, "partition-statistics")

	snapshots := document["snapshots"].([]any)
	first := snapshots[0].(map[string]any)
	require.Equal(t, targetRoot+"/metadata/snap-1.avro", first["manifest-list"])
	second := snapshots[1].(map[string]any)
	manifests := second["manifests"].([]any)
	require.Equal(t, []any{
		targetRoot + "/metadata/old-m.avro",
		targetRoot + "/metadata/old-m.avro",
	}, manifests)

	properties := document["properties"].(map[string]any)
	require.Equal(t, targetRoot+"/data", properties["write.data.path"])
	require.Equal(t, "s3://elsewhere/metadata", properties["write.metadata.path"], "a property outside the table is left alone")
	require.Equal(t, targetRoot+"/metadata", properties["write.metadata.statistics.path"])

	require.Contains(t, string(encoded), `"current-snapshot-id":3055729675574597004`,
		"a number above 2^53 must not be rounded through float64")
}

func TestMetadataRefusesWhatItCannotUse(t *testing.T) {
	for name, document := range map[string]string{
		"not json":         `{"format-version":`,
		"no version":       `{"location":"s3://b/t"}`,
		"string version":   `{"format-version":"2"}`,
		"fraction version": `{"format-version":2.5}`,
		"trailing json":    `{"format-version":2} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			metadata, err := ParseMetadata([]byte(document))
			if err != nil {
				require.ErrorIs(t, err, ErrInvalidMetadata)
				return
			}
			_, err = metadata.FormatVersion()
			require.ErrorIs(t, err, ErrInvalidMetadata)
		})
	}
}
