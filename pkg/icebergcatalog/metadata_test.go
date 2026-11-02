package icebergcatalog

// Tests for the metadata.json half of the commit path (ICEBERG_CATALOG_PLAN.md §6.2 steps 3-4):
// naming the file a commit writes, continuing the version sequence, never reusing a location, and
// writing the bytes through the block adapter. The naming rules are what make "a commit never
// overwrites a metadata file" true, so they are tested on their own as well as end to end in
// commit_test.go.

import (
	"strconv"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// metadataSequenceOf parses the <seq> prefix of a file name this catalog wrote.
func metadataSequenceOf(t *testing.T, location string) int {
	t.Helper()
	name := location[strings.LastIndexByte(location, '/')+1:]
	name = strings.TrimSuffix(name, metadataFileSuffix)
	dash := strings.IndexByte(name, '-')
	require.GreaterOrEqual(t, dash, 0, "%q is not a <seq>-<uuid>.metadata.json name", location)
	sequence, err := strconv.Atoi(name[:dash])
	require.NoError(t, err, "sequence of %q", location)
	return sequence
}

// TestCommitMetadataFileName covers NewMetadataFileLocation: the sequence continues the file being
// replaced, an unrecognised base falls back to metadata-log, every call is unique, and the file
// always lands under <table-location>/metadata/.
func TestCommitMetadataFileName(t *testing.T) {
	const tableLocation = "mem://repo-ns/_iceberg/" + storeTestUUID
	meta, err := table.ParseMetadataBytes([]byte(storeTestMetadata(t, storeTestUUID, tableLocation)))
	require.NoError(t, err)
	require.Equal(t, 1, metadataLogLength(meta), "the fixture's metadata-log has one entry")

	tests := []struct {
		name       string
		base       string
		wantSeq    int
		wantPrefix string
	}{
		{
			name:       "continues a name this catalog wrote",
			base:       tableLocation + "/metadata/00007-" + storeTestUUID + ".metadata.json",
			wantSeq:    8,
			wantPrefix: "00008-",
		},
		{
			name:       "continues a bare sequence number",
			base:       tableLocation + "/metadata/17.metadata.json",
			wantSeq:    18,
			wantPrefix: "00018-",
		},
		{
			// A file written by another catalog: the name carries no sequence, so the count of
			// metadata-log entries is the number of versions the document remembers.
			name:       "falls back to metadata-log for a foreign name",
			base:       tableLocation + "/metadata/v3.metadata.json",
			wantSeq:    2,
			wantPrefix: "00002-",
		},
		{
			name:       "falls back to metadata-log with no base at all",
			base:       "",
			wantSeq:    2,
			wantPrefix: "00002-",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			location, err := NewMetadataFileLocation(tableLocation, tc.base, meta)
			require.NoError(t, err)

			require.True(t, strings.HasPrefix(location, tableLocation+"/"+MetadataDirectory+"/"),
				"%q must be under the table location's metadata directory", location)
			require.True(t, strings.HasSuffix(location, metadataFileSuffix))
			require.Equal(t, tc.wantSeq, metadataSequenceOf(t, location))
			require.True(t, strings.HasPrefix(location[strings.LastIndexByte(location, '/')+1:], tc.wantPrefix))
			require.NotEqual(t, tc.base, location, "a commit must never reuse the file it replaces")

			// The trailing UUID is freshly generated and a valid UUID, not the table's.
			name := location[strings.LastIndexByte(location, '/')+1:]
			fileUUID := strings.TrimSuffix(strings.TrimPrefix(name, tc.wantPrefix), metadataFileSuffix)
			_, err = uuid.Parse(fileUUID)
			require.NoError(t, err, "%q must end in a UUID", name)
		})
	}

	t.Run("a table location with a trailing slash", func(t *testing.T) {
		location, err := NewMetadataFileLocation(tableLocation+"/", "", meta)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(location, tableLocation+"/"+MetadataDirectory+"/"),
			"the join must not double the slash: %q", location)
	})

	t.Run("a table with no location", func(t *testing.T) {
		_, err := NewMetadataFileLocation("", "", meta)
		require.ErrorIs(t, err, ErrInvalidPointer)
	})

	t.Run("every call names a different file", func(t *testing.T) {
		seen := make(map[string]struct{}, 100)
		for range 100 {
			location, err := NewMetadataFileLocation(tableLocation, tableLocation+"/metadata/00001.metadata.json", meta)
			require.NoError(t, err)
			require.Equal(t, 2, metadataSequenceOf(t, location), "two racing commits compute the same sequence")
			_, duplicate := seen[location]
			require.False(t, duplicate, "%q was handed out twice", location)
			seen[location] = struct{}{}
		}
	})
}

// TestCommitMetadataWrite covers WriteMetadata: the bytes are written through the block adapter at
// the fully qualified location, they are exactly what the function returns, and they parse back
// into the model that produced them.
func TestCommitMetadataWrite(t *testing.T) {
	f := newCommitFixture(t)
	base := f.baseMetadata

	location, err := NewMetadataFileLocation(f.tableLocation, f.baseLocation, base)
	require.NoError(t, err)

	data, err := f.store.WriteMetadata(f.ctx, storeTestRepo, location, base)
	require.NoError(t, err)
	require.True(t, f.objectExists(t, location), "the metadata file must be in the object store")
	require.Equal(t, string(data), f.readObject(t, location))

	reparsed, err := table.ParseMetadataBytes(data)
	require.NoError(t, err)
	require.Equal(t, base.TableUUID(), reparsed.TableUUID())
	require.Equal(t, base.Location(), reparsed.Location())
	require.Equal(t, base.Version(), reparsed.Version())
	require.True(t, base.Equals(reparsed), "the written document must be the model that produced it")

	t.Run("an unknown repository", func(t *testing.T) {
		_, err := f.store.WriteMetadata(f.ctx, "no-such-repository", location, base)
		require.Error(t, err)
	})
}
