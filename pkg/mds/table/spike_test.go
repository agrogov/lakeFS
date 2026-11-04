package table_test

// SPIKE (METADATA_SEARCH_PLAN.md task M3, design §3.1, §4.3 step 3, §10 decision 2): prove, before
// building on it, that the lakeFS Iceberg catalog accepts the transaction the indexer wants to
// send:
//
//	Delete(In("path", …)) with write.delete.mode=merge-on-read + Append + SetProperties
//
// committed through iceberg-go's catalog/rest against /iceberg/relative_to/<repo>.<branch>/api,
// and read back with a scan.
//
// The catalog stages every file a new snapshot references (pkg/icebergcatalog/files.go), delete
// files included, so this test is the end-to-end proof of that claim and of the row semantics the
// writer depends on. If it fails at the commit, the fallback is the copy-on-write path that
// TestApplyDeltaCopyOnWrite covers (design §13 risk "Delete files and catalog staging").

import (
	"fmt"
	"testing"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// row builds a test row with the fields the assertions read. The optional columns are set so that
// a scan returning null where a value was written fails the comparison.
func row(t *testing.T, path, commitID string, size int64, modified time.Time, userMetadata map[string]string) table.Row {
	t.Helper()
	committer := "committer-" + commitID
	contentType := "application/octet-stream"
	creation := modified.Add(-time.Minute)
	if userMetadata == nil {
		userMetadata = map[string]string{}
	}
	return table.Row{
		Repository:   "data-repo",
		Path:         path,
		CommitID:     commitID,
		SizeBytes:    size,
		LastModified: modified,
		ETag:         "etag-" + path + "-" + commitID,
		UserMetadata: userMetadata,
		Committer:    &committer,
		ContentType:  &contentType,
		CreationDate: &creation,
	}
}

// TestSpikeMergeOnReadDelta is the spike: one transaction with Delete (merge-on-read), Append and
// SetProperties, committed through the catalog and read back with a scan.
func TestSpikeMergeOnReadDelta(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)

	first := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)
	second := first.Add(time.Hour)

	// Create three rows at commit c1.
	created, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, first, map[string]string{"k": "1"}),
		row(t, "b", "c1", 2, first, nil),
		row(t, "c", "c1", 3, first, map[string]string{"k": "3"}),
	}, nil, "c1")
	require.NoError(t, err, "apply the first delta")
	require.Equal(t, "c1", created.CurrentSnapshot().Summary.Properties[table.PropertySourceCommit])
	require.Equal(t, "c1", created.Properties()[table.PropertySourceCommit])

	// One transaction: delete a and b (merge-on-read position deletes), append the new a, and
	// move the source-commit property to c2.
	updated, err := table.ApplyDelta(t.Context(), created, []table.Row{
		row(t, "a", "c2", 10, second, map[string]string{"k": "10"}),
	}, []string{"b"}, "c2")
	require.NoError(t, err, "apply the merge-on-read delta")

	// The spike's finding: the catalog staged position delete files, so the snapshot counts them.
	require.Positive(t, positionDeleteCount(t, updated.CurrentSnapshot()),
		"merge-on-read must have written position deletes")
	require.Equal(t, "c2", updated.CurrentSnapshot().Summary.Properties[table.PropertySourceCommit])
	require.Equal(t, env.dataRepository(),
		updated.CurrentSnapshot().Summary.Properties[table.PropertySourceRepository])
	require.Equal(t, "c2", updated.Properties()[table.PropertySourceCommit])

	// And the rows are exactly what a reader must see: a updated, b gone, c untouched.
	requireScanRows(t, env.loadTable(t), []table.Row{
		row(t, "a", "c2", 10, second, map[string]string{"k": "10"}),
		row(t, "c", "c1", 3, first, map[string]string{"k": "3"}),
	})
}

// positionDeleteCount returns how many position deletes the snapshot's summary reports, and how
// many delete files carry them. A snapshot may not carry the summary counters (the field is
// optional), in which case the answer is 0.
func positionDeleteCount(t *testing.T, snapshot *icebergtable.Snapshot) int {
	t.Helper()
	require.NotNil(t, snapshot, "the write added no snapshot")
	if snapshot.Summary == nil {
		return 0
	}
	require.Positive(t, parseInt(t, snapshot.Summary.Properties["total-delete-files"]),
		"a position delete must be carried by a delete file")
	return parseInt(t, snapshot.Summary.Properties["total-position-deletes"])
}

func parseInt(t *testing.T, value string) int {
	t.Helper()
	if value == "" {
		return 0
	}
	var number int
	_, err := fmt.Sscanf(value, "%d", &number)
	require.NoError(t, err, "parse %q", value)
	return number
}
