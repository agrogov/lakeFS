package table

import (
	"fmt"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// TestChunks: the IN lists of one delta are at most DefaultDeleteChunkSize paths each, and more
// than ten thousand paths split instead of becoming one predicate (design §4.3 step 3).
func TestChunks(t *testing.T) {
	paths := make([]string, 0, DefaultDeleteChunkSize+1)
	for i := range DefaultDeleteChunkSize + 1 {
		paths = append(paths, fmt.Sprintf("path-%06d", i))
	}

	got := chunks(paths, DefaultDeleteChunkSize)
	require.Len(t, got, 2)
	require.Len(t, got[0], DefaultDeleteChunkSize)
	require.Len(t, got[1], 1)
	require.Equal(t, paths, slices.Concat(got...), "chunking must not drop or reorder a path")

	require.Empty(t, chunks(nil, DefaultDeleteChunkSize))
	require.Equal(t, [][]string{{"a"}}, chunks([]string{"a"}, DefaultDeleteChunkSize))
}

// TestPathFilter: the copy-on-write fallback names the same paths as the merge-on-read chunks, one
// IN list per chunk joined with OR, and an empty path set is AlwaysFalse - never AlwaysTrue, which
// would replace the whole table.
func TestPathFilter(t *testing.T) {
	require.IsType(t, iceberg.AlwaysFalse{}, pathFilter(nil, DefaultDeleteChunkSize))

	// iceberg.IsIn reduces a one-value list to EQUAL, which names the same row.
	single := pathFilter([]string{"a"}, DefaultDeleteChunkSize)
	require.NotNil(t, single)
	require.Contains(t, single.String(), "path")
	require.NotContains(t, single.String(), "OR")

	joined := pathFilter([]string{"a", "b", "c", "d", "e"}, 2)
	require.NotNil(t, joined)
	require.Contains(t, joined.String(), "Or(")
	for _, path := range []string{"a", "b", "c", "d", "e"} {
		require.Contains(t, joined.String(), path)
	}
}

// TestChangedPaths: one predicate covers the upserts and the deletes, sorted and deduplicated, so
// a path that is both replaced and deleted appears once and a retried commit sends the same bytes.
func TestChangedPaths(t *testing.T) {
	rows := []Row{{Path: "b"}, {Path: "a"}, {Path: "b"}}
	require.Equal(t, []string{"a", "b", "c"}, changedPaths(rows, []string{"c", "a"}))
	require.Empty(t, changedPaths(nil, nil))
}
