package table_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// rowSeq is a row source over a slice, the shape Rebuild reads.
func rowSeq(rows []table.Row) table.Rows {
	return func(yield func(table.Row, error) bool) {
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

// snapshotCount returns how many snapshots the table metadata carries.
func snapshotCount(tbl *icebergtable.Table) int {
	return len(tbl.Metadata().Snapshots())
}

// summaryInt returns an integer of the current snapshot's summary, or 0 when the key is absent.
func summaryInt(t *testing.T, tbl *icebergtable.Table, key string) int {
	t.Helper()
	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot, "the write added no snapshot")
	if snapshot.Summary == nil {
		return 0
	}
	return parseInt(t, snapshot.Summary.Properties[key])
}

// TestApplyDeltaMergeOnRead covers the delta path of §4.3 step 3 with the default (and table's)
// merge-on-read delete mode: an upsert replaces a row, a delete removes one, and an untouched row
// stays.
func TestApplyDeltaMergeOnRead(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	first := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)
	second := first.Add(time.Hour)

	created, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, first, map[string]string{"k": "1"}),
		row(t, "b", "c1", 2, first, nil),
		row(t, "c", "c1", 3, first, map[string]string{"k": "3"}),
	}, nil, "c1")
	require.NoError(t, err)
	require.Zero(t, summaryInt(t, created, "total-delete-files"), "an append writes no delete files")

	updated, err := table.ApplyDelta(t.Context(), created, []table.Row{
		row(t, "a", "c2", 10, second, map[string]string{"k": "10", "z": "z"}),
	}, []string{"b"}, "c2")
	require.NoError(t, err)
	require.Positive(t, summaryInt(t, updated, "total-position-deletes"))
	require.Equal(t, "c2", updated.Properties()[table.PropertySourceCommit])
	require.Equal(t, "c2", updated.CurrentSnapshot().Summary.Properties[table.PropertySourceCommit])
	require.Equal(t, env.dataRepository(),
		updated.CurrentSnapshot().Summary.Properties[table.PropertySourceRepository])

	requireScanRows(t, env.loadTable(t), []table.Row{
		row(t, "a", "c2", 10, second, map[string]string{"k": "10", "z": "z"}),
		row(t, "c", "c1", 3, first, map[string]string{"k": "3"}),
	})
}

// TestApplyDeltaCopyOnWrite is the fallback of design §13's risk list: the same delta written with
// Overwrite and an IN filter instead of position deletes. Nothing is deleted logically - the
// affected data files are rewritten - and the rows are the same as the merge-on-read path's.
func TestApplyDeltaCopyOnWrite(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	first := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	second := first.Add(time.Hour)

	created, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, first, map[string]string{"k": "1"}),
		row(t, "b", "c1", 2, first, nil),
		row(t, "c", "c1", 3, first, nil),
	}, nil, "c1")
	require.NoError(t, err)

	updated, err := table.ApplyDelta(t.Context(), created, []table.Row{
		row(t, "a", "c2", 10, second, nil),
	}, []string{"b"}, "c2", table.WithDeleteMode(table.WriteModeCopyOnWrite))
	require.NoError(t, err)
	require.Zero(t, summaryInt(t, updated, "total-delete-files"),
		"copy-on-write must not leave delete files behind")
	require.Equal(t, "c2", updated.CurrentSnapshot().Summary.Properties[table.PropertySourceCommit])

	requireScanRows(t, env.loadTable(t), []table.Row{
		row(t, "a", "c2", 10, second, nil),
		row(t, "c", "c1", 3, first, nil),
	})
}

// TestApplyDeltaEmptyDelta: a commit that changed no object still moves the table property (§4.3
// step 3), which is what makes the indexing position recoverable (§4.4), and adds no snapshot.
func TestApplyDeltaEmptyDelta(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	created, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, nil),
	}, nil, "c1")
	require.NoError(t, err)
	before := snapshotCount(created)

	updated, err := table.ApplyDelta(t.Context(), created, nil, nil, "c2")
	require.NoError(t, err)
	require.Equal(t, "c2", updated.Properties()[table.PropertySourceCommit])
	require.Equal(t, before, snapshotCount(updated), "an empty delta adds no snapshot")

	requireScanRows(t, env.loadTable(t), []table.Row{row(t, "a", "c1", 1, when, nil)})
}

// TestApplyDeltaChunkedDeletes proves the IN list is chunked inside one transaction (§4.3 step 3):
// five paths with a chunk size of two are three Delete calls, i.e. three snapshots in one commit.
func TestApplyDeltaChunkedDeletes(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	rows := make([]table.Row, 0, 5)
	paths := make([]string, 0, 5)
	for _, path := range []string{"a", "b", "c", "d", "e"} {
		rows = append(rows, row(t, path, "c1", 1, when, nil))
		paths = append(paths, path)
	}
	created, err := table.ApplyDelta(t.Context(), tbl, rows, nil, "c1")
	require.NoError(t, err)
	before := snapshotCount(created)

	deleted, err := table.ApplyDelta(t.Context(), created, nil, paths, "c2",
		table.WithDeleteChunkSize(2))
	require.NoError(t, err)
	require.Equal(t, before+3, snapshotCount(deleted), "five paths at two per chunk are three deletes")
	require.Equal(t, 5, summaryInt(t, deleted, "total-position-deletes"))
	require.Empty(t, scanRows(t, env.loadTable(t)))
}

// TestApplyDeltaManyDeletesChunksTheINList is the >10 000-path case of §4.3 step 3 on the default
// chunk size: 10 001 deleted paths are two Delete calls in one transaction, so the commit carries
// two snapshots and no predicate names more than DefaultDeleteChunkSize paths. The table holds one
// unrelated row, which must survive.
func TestApplyDeltaManyDeletesChunksTheINList(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	created, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "kept", "c1", 1, when, nil),
	}, nil, "c1")
	require.NoError(t, err)
	before := snapshotCount(created)

	paths := make([]string, 0, table.DefaultDeleteChunkSize+1)
	for i := range table.DefaultDeleteChunkSize + 1 {
		paths = append(paths, fmt.Sprintf("gone/%06d", i))
	}
	deleted, err := table.ApplyDelta(t.Context(), created, nil, paths, "c2")
	require.NoError(t, err)
	require.Equal(t, before+2, snapshotCount(deleted), "10 001 paths are two delete calls")
	require.Equal(t, "c2", deleted.Properties()[table.PropertySourceCommit])
	requireScanRows(t, env.loadTable(t), []table.Row{row(t, "kept", "c1", 1, when, nil)})
}

// TestApplyDeltaCopyOnWriteChunked is the same chunking on the copy-on-write path, where the
// chunks are joined with OR into one filter: one overwrite, so no rows are duplicated.
func TestApplyDeltaCopyOnWriteChunked(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	rows := make([]table.Row, 0, 5)
	paths := make([]string, 0, 5)
	for _, path := range []string{"a", "b", "c", "d", "e"} {
		rows = append(rows, row(t, path, "c1", 1, when, nil))
		paths = append(paths, path)
	}
	created, err := table.ApplyDelta(t.Context(), tbl, rows, nil, "c1")
	require.NoError(t, err)

	_, err = table.ApplyDelta(t.Context(), created, nil, paths, "c2",
		table.WithDeleteMode(table.WriteModeCopyOnWrite), table.WithDeleteChunkSize(2))
	require.NoError(t, err)
	require.Empty(t, scanRows(t, env.loadTable(t)))
}

// TestRebuild replaces the whole table and streams its rows: a rebuild is the full-listing path of
// §4.3 step 2 and §4.5.
func TestRebuild(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	firstRows := []table.Row{
		row(t, "a", "c1", 1, when, nil),
		row(t, "b", "c1", 2, when, map[string]string{"k": "2"}),
		row(t, "c", "c1", 3, when, nil),
	}
	rebuilt, err := table.Rebuild(t.Context(), tbl, rowSeq(firstRows), "c1")
	require.NoError(t, err)
	require.Equal(t, "c1", rebuilt.Properties()[table.PropertySourceCommit])
	require.Equal(t, "c1", rebuilt.CurrentSnapshot().Summary.Properties[table.PropertySourceCommit])
	requireScanRows(t, env.loadTable(t), firstRows)

	// A second rebuild replaces the contents and streams through several record batches, so the
	// pull-based reader and its flush at the end are exercised.
	secondRows := []table.Row{
		row(t, "d", "c2", 4, when, nil),
		row(t, "e", "c2", 5, when, nil),
	}
	replaced, err := table.Rebuild(t.Context(), rebuilt, rowSeq(secondRows), "c2",
		table.WithRecordBatchSize(1))
	require.NoError(t, err)
	require.Equal(t, "c2", replaced.Properties()[table.PropertySourceCommit])
	requireScanRows(t, env.loadTable(t), secondRows)

	// A rebuild after a merge-on-read delta replaces a table that has delete files, too.
	deleted, err := table.ApplyDelta(t.Context(), replaced, nil, []string{"d"}, "c3")
	require.NoError(t, err)
	require.Positive(t, summaryInt(t, deleted, "total-delete-files"))
	finalRows := []table.Row{row(t, "f", "c4", 6, when, nil)}
	afterDeleteFiles, err := table.Rebuild(t.Context(), deleted, rowSeq(finalRows), "c4")
	require.NoError(t, err)
	requireScanRows(t, env.loadTable(t), finalRows)
	require.Equal(t, "c4", afterDeleteFiles.Properties()[table.PropertySourceCommit])
}

// TestRebuildEmpty: a rebuild with no rows empties the table - the first indexing of a branch with
// no objects (§4.5) - and still records the commit.
func TestRebuildEmpty(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	filled, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, nil),
	}, nil, "c1")
	require.NoError(t, err)

	emptied, err := table.Rebuild(t.Context(), filled, rowSeq(nil), "c2")
	require.NoError(t, err)
	require.Equal(t, "c2", emptied.Properties()[table.PropertySourceCommit])
	require.Empty(t, scanRows(t, env.loadTable(t)))
}

// TestRebuildSourceError: an error of the row source fails the rebuild and commits nothing.
func TestRebuildSourceError(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	filled, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, nil),
	}, nil, "c1")
	require.NoError(t, err)

	sourceErr := errors.New("the listing failed")
	_, err = table.Rebuild(t.Context(), filled, func(yield func(table.Row, error) bool) {
		yield(row(t, "b", "c2", 2, when, nil), nil)
		yield(table.Row{}, sourceErr)
	}, "c2")
	require.ErrorIs(t, err, sourceErr)

	// The failed rebuild left the table as it was.
	requireScanRows(t, env.loadTable(t), []table.Row{row(t, "a", "c1", 1, when, nil)})
}

// TestCompact folds the position delete files of a table away (§4.9): the rows stay, the delete
// files and the source commit do not change.
func TestCompact(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	created, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, map[string]string{"k": "1"}),
		row(t, "b", "c1", 2, when, nil),
		row(t, "c", "c1", 3, when, nil),
	}, nil, "c1")
	require.NoError(t, err)
	updated, err := table.ApplyDelta(t.Context(), created, []table.Row{
		row(t, "a", "c2", 10, when, map[string]string{"k": "10"}),
	}, []string{"b"}, "c2")
	require.NoError(t, err)
	require.Positive(t, summaryInt(t, updated, "total-delete-files"))

	want := []table.Row{
		row(t, "a", "c2", 10, when, map[string]string{"k": "10"}),
		row(t, "c", "c1", 3, when, nil),
	}
	compacted, err := table.Compact(t.Context(), updated, "c2")
	require.NoError(t, err)
	require.Zero(t, summaryInt(t, compacted, "total-delete-files"),
		"compaction must fold the delete files away")
	require.Equal(t, "c2", compacted.Properties()[table.PropertySourceCommit])
	requireScanRows(t, env.loadTable(t), want)

	// The table keeps working after a compaction: the next delta applies to the rewritten files.
	next := time.Date(2024, 1, 2, 5, 6, 7, 0, time.UTC)
	continued, err := table.ApplyDelta(t.Context(), compacted, []table.Row{
		row(t, "c", "c3", 30, next, nil),
	}, []string{"a"}, "c3")
	require.NoError(t, err)
	require.Equal(t, "c3", continued.Properties()[table.PropertySourceCommit])
	requireScanRows(t, env.loadTable(t), []table.Row{row(t, "c", "c3", 30, next, nil)})
}

// TestApplyDeltaValidation pins the errors of a malformed write.
func TestApplyDeltaValidation(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	_, err := table.ApplyDelta(t.Context(), tbl, nil, nil, "")
	require.ErrorIs(t, err, table.ErrNoSourceCommit)

	_, err = table.ApplyDelta(t.Context(), tbl, []table.Row{row(t, "a", "c1", 1, when, nil)}, nil, "c1",
		table.WithDeleteMode("nonsense"))
	require.ErrorIs(t, err, table.ErrUnknownDeleteMode)

	// A table that this package did not create (no lakefs.mds.repository) cannot be written: the
	// snapshot properties would lose the data repository name.
	require.NoError(t, env.rest.DropTable(t.Context(), table.Identifier()))
	bare, err := env.rest.CreateTable(t.Context(), table.Identifier(), table.Schema())
	require.NoError(t, err)
	_, err = table.ApplyDelta(t.Context(), bare, []table.Row{row(t, "a", "c1", 1, when, nil)}, nil, "c1")
	require.ErrorIs(t, err, table.ErrNoRepository)

	// And the same for a rebuild.
	_, err = table.Rebuild(t.Context(), bare, rowSeq(nil), "c1")
	require.ErrorIs(t, err, table.ErrNoRepository)

	_, err = table.Compact(t.Context(), bare, "c1")
	require.ErrorIs(t, err, table.ErrNoRepository)

	// No snapshot was added by any of the refusals.
	require.Zero(t, snapshotCount(bare))
}

// TestApplyDeltaStaleTable: the transaction asserts the branch's snapshot ID, so a table handle
// from before another commit is refused instead of overwriting that commit (§4.3 step 3, §4.4).
func TestApplyDeltaStaleTable(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	_, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, nil),
	}, nil, "c1")
	require.NoError(t, err)

	_, err = table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "b", "c2", 2, when, nil),
	}, nil, "c2")
	require.Error(t, err, "a stale table handle must not commit")
	require.ErrorContains(t, err, "assert-ref-snapshot-id")
}

// TestApplyDeltaSourceRepository: lakefs.mds.source-repository is the data repository of the table
// property, even when the upsert rows were built before a table reload.
func TestApplyDeltaSourceRepository(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	rows := []table.Row{row(t, "a", "c1", 1, when, nil)}
	rows[0].Repository = "not-the-table-property"
	created, err := table.ApplyDelta(t.Context(), tbl, rows, nil, "c1")
	require.NoError(t, err)
	require.Equal(t, env.dataRepository(),
		created.CurrentSnapshot().Summary.Properties[table.PropertySourceRepository])
}
