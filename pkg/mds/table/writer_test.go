package table_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icebergcataloggo "github.com/apache/iceberg-go/catalog"
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

// scanRowsInWriteOrder returns every row of the table at the branch head in the order its data
// files carry it, unlike scanRows, which sorts by path. It is how a test sees the physical order
// a compaction or a rebuild wrote: the table declares sort order path ASC (§3.1), so that order is
// part of what the writer promises, and a path-sorted read cannot check it (MDS_REVIEW.md TB-2).
func scanRowsInWriteOrder(t *testing.T, tbl *icebergtable.Table) []table.Row {
	t.Helper()
	scan := tbl.Scan()
	defer func() { _ = scan.Close() }()
	_, batches, err := scan.ToArrowRecords(t.Context())
	require.NoError(t, err, "scan the table")

	var rows []table.Row
	for batch, err := range batches {
		require.NoError(t, err)
		for i := range int(batch.NumRows()) {
			rows = append(rows, decodeRow(batch, i))
		}
		batch.Release()
	}
	return rows
}

// pathsOf returns the paths of rows in the order given.
func pathsOf(rows []table.Row) []string {
	paths := make([]string, 0, len(rows))
	for _, row := range rows {
		paths = append(paths, row.Path)
	}
	return paths
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

	// A rebuild after merge-on-read deltas writes no delete file of its own: the two deletes below
	// leave two position delete files, and a rebuild that keeps them would keep the snapshot
	// summary's total-delete-files above zero forever (§4.9's delete-file trigger reads it).
	deleted, err := table.ApplyDelta(t.Context(), replaced, nil, []string{"d"}, "c3")
	require.NoError(t, err)
	twiceDeleted, err := table.ApplyDelta(t.Context(), deleted, nil, []string{"e"}, "c4")
	require.NoError(t, err)
	require.Equal(t, 2, summaryInt(t, twiceDeleted, "total-delete-files"),
		"two merge-on-read deletes write two position delete files")
	finalRows := []table.Row{row(t, "f", "c5", 6, when, nil)}
	afterDeleteFiles, err := table.Rebuild(t.Context(), twiceDeleted, rowSeq(finalRows), "c5")
	require.NoError(t, err)
	requireScanRows(t, env.loadTable(t), finalRows)
	require.Equal(t, "c5", afterDeleteFiles.Properties()[table.PropertySourceCommit])
	// A rebuild replaces the contents of the table, so no delete file can outlive it: the rows it
	// replaces are the rows those delete files referred to, and a leftover file is referenced by
	// the live snapshot (so no cleanup can reclaim it) and counted by the compaction trigger.
	require.Zero(t, summaryInt(t, afterDeleteFiles, "total-delete-files"),
		"a rebuild must leave no delete file behind")
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

	// Two merge-on-read deletes, a rebuild and then a compaction (TB-1), on a table of their own so
	// that exactly two delete files are live: the delete files the rebuild orphaned must be gone -
	// and stay gone - because compaction is what the indexer runs when the summary's
	// total-delete-files crosses compaction.max_delete_files (§4.9).
	orphanEnv := newTestEnv(t)
	orphanTable := orphanEnv.createTable(t)
	withDeletes, err := table.ApplyDelta(t.Context(), orphanTable, []table.Row{
		row(t, "x", "c1", 1, next, nil),
		row(t, "y", "c1", 2, next, nil),
	}, nil, "c1")
	require.NoError(t, err)
	withDeletes, err = table.ApplyDelta(t.Context(), withDeletes, nil, []string{"x"}, "c2")
	require.NoError(t, err)
	withDeletes, err = table.ApplyDelta(t.Context(), withDeletes, nil, []string{"y"}, "c3")
	require.NoError(t, err)
	require.Equal(t, 2, summaryInt(t, withDeletes, "total-delete-files"),
		"two merge-on-read deletes write two position delete files")

	rebuiltRows := []table.Row{
		row(t, "w", "c4", 3, next, nil),
		row(t, "z", "c4", 4, next, nil),
	}
	rebuilt, err := table.Rebuild(t.Context(), withDeletes, rowSeq(rebuiltRows), "c4")
	require.NoError(t, err)
	require.Zero(t, summaryInt(t, rebuilt, "total-delete-files"),
		"a rebuild must leave no delete file behind")
	recompacted, err := table.Compact(t.Context(), rebuilt, "c4")
	require.NoError(t, err)
	require.Zero(t, summaryInt(t, recompacted, "total-delete-files"),
		"compaction must not keep a delete file the rebuild orphaned")
	requireScanRows(t, orphanEnv.loadTable(t), rebuiltRows)

	// A delete file orphaned without a rebuild: a copy-on-write delta rewrites the data file a
	// position delete file belongs to and keeps the delete file (§4.9's reason for ReplaceFiles),
	// so no scan task carries it. Compaction removes it by enumerating the snapshot's manifests.
	morDelete, err := table.ApplyDelta(t.Context(), recompacted, nil, []string{"z"}, "c5")
	require.NoError(t, err)
	require.Equal(t, 1, summaryInt(t, morDelete, "total-delete-files"),
		"a delete of one row of a two-row data file writes a position delete file")
	cowRewritten, err := table.ApplyDelta(t.Context(), morDelete, []table.Row{
		row(t, "z", "c6", 30, next, nil),
	}, nil, "c6", table.WithDeleteMode(table.WriteModeCopyOnWrite))
	require.NoError(t, err)
	require.Equal(t, 1, summaryInt(t, cowRewritten, "total-delete-files"),
		"copy-on-write keeps the position delete file of the data file it rewrote")
	orphansRemoved, err := table.Compact(t.Context(), cowRewritten, "c6")
	require.NoError(t, err)
	require.Zero(t, summaryInt(t, orphansRemoved, "total-delete-files"),
		"compaction must remove a delete file no scan task carries")
	requireScanRows(t, orphanEnv.loadTable(t), []table.Row{
		row(t, "w", "c4", 3, next, nil),
		row(t, "z", "c6", 30, next, nil),
	})
}

// TestCompactWritesRowsInPathOrder is MDS_REVIEW.md TB-2: the table declares sort order path ASC
// (§3.1), so a compaction must write its files in that order. The scan plans the table's data files
// in the order the commits that wrote them left them, so two commits of interleaved paths - data
// files [a c] then [b d] - used to compact into a file of [b d a c], a table whose declared sort
// order no longer describes its files. The rows are read back in the order the written files carry
// them (scanRowsInWriteOrder): a path-sorted read cannot see the difference.
//
// The compaction writes two record batches here (CompactRecordBatchSize(2)), which is the case the
// bug is about: iceberg-go sorts each batch it is given on its own, so a compaction that streams
// the scan's batches back (one batch per data file) writes [b d][a c]. Sorting the rows before they
// are batched is what makes the written order the declared one.
func TestCompactWritesRowsInPathOrder(t *testing.T) {
	restore := table.CompactRecordBatchSize(2)
	t.Cleanup(restore)

	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	first, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, nil),
		row(t, "c", "c1", 3, when, nil),
	}, nil, "c1")
	require.NoError(t, err)
	second, err := table.ApplyDelta(t.Context(), first, []table.Row{
		row(t, "b", "c2", 2, when, nil),
		row(t, "d", "c2", 4, when, nil),
	}, nil, "c2")
	require.NoError(t, err)
	plan := second.Scan()
	tasks, err := plan.PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2, "the two commits wrote two data files")
	require.NoError(t, plan.Close())

	compacted, err := table.Compact(t.Context(), second, "c2")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d"},
		pathsOf(scanRowsInWriteOrder(t, env.loadTable(t))),
		"the compacted file carries the rows in the declared path ASC order")
	require.Equal(t, first.SortOrder().OrderID(), compacted.SortOrder().OrderID(),
		"compaction keeps the sort order it now obeys")
}

// TestRebuildRefusesRowsOutOfOrder is the rebuild half of MDS_REVIEW.md TB-2: a rebuild streams its
// source (design §4.3 step 2), so it cannot sort it, and a source that does not yield the table's
// declared path ASC order would write files that contradict the declared order. Such a source is
// refused and the table is left as it was, rather than being rewritten unsorted.
func TestRebuildRefusesRowsOutOfOrder(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	filled, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "m", "c1", 1, when, nil),
	}, nil, "c1")
	require.NoError(t, err)

	_, err = table.Rebuild(t.Context(), filled, rowSeq([]table.Row{
		row(t, "z", "c2", 2, when, nil),
		row(t, "a", "c2", 3, when, nil),
	}), "c2")
	require.ErrorIs(t, err, table.ErrRowsOutOfOrder)

	// Nothing was written: the failed rebuild committed no snapshot and the table still holds the
	// row it had, in path order.
	requireScanRows(t, env.loadTable(t), []table.Row{row(t, "m", "c1", 1, when, nil)})
	require.Equal(t, []string{"m"}, pathsOf(scanRowsInWriteOrder(t, env.loadTable(t))))

	// A duplicate path is out of order too: path is the identifier field, so a row appears once.
	_, err = table.Rebuild(t.Context(), filled, rowSeq([]table.Row{
		row(t, "b", "c2", 2, when, nil),
		row(t, "b", "c2", 2, when, nil),
	}), "c2")
	require.ErrorIs(t, err, table.ErrRowsOutOfOrder)

	// A sorted source is written in the order it yields, which is the declared order, so the
	// written files are sorted too.
	rebuilt, err := table.Rebuild(t.Context(), filled, rowSeq([]table.Row{
		row(t, "a", "c3", 2, when, nil),
		row(t, "b", "c3", 3, when, nil),
	}), "c3")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, pathsOf(scanRowsInWriteOrder(t, env.loadTable(t))))
	require.Equal(t, 1, rebuilt.SortOrder().OrderID(), "the declared sort order is unchanged")
}

// TestCompactTakesNoOptions pins MDS_REVIEW.md TB-4: Compact used to accept the write options of
// ApplyDelta and discard all of them. It has none now, so the options it used to pretend to honour
// - delete mode, delete chunk size and record batch size - cannot be passed to it at all.
// The compile-time check is the call below: Compact takes ctx, a table and a source commit.
func TestCompactTakesNoOptions(t *testing.T) {
	env := newTestEnv(t)
	tbl := env.createTable(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	filled, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, nil),
	}, nil, "c1")
	require.NoError(t, err)

	var compact func(context.Context, *icebergtable.Table, string) (*icebergtable.Table, error) = table.Compact
	compacted, err := compact(t.Context(), filled, "c1")
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, pathsOf(scanRowsInWriteOrder(t, compacted)))
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

// ---------------------------------------------------------------------------------------------
// R2T-1, R2T-2 and R2T-3 of MDS_REVIEW_2.md
// ---------------------------------------------------------------------------------------------

// createTableWith creates system.object_metadata with the given schema and extra properties, the
// way CreateTable does: the fixed properties of the package are written last.
func (e *testEnv) createTableWith(t *testing.T, sc *iceberg.Schema, extra iceberg.Properties) *icebergtable.Table {
	t.Helper()
	order, err := table.SortOrder()
	require.NoError(t, err)
	props := iceberg.Properties{}
	maps.Copy(props, extra)
	maps.Copy(props, table.Properties(e.dataRepository()))
	created, err := e.rest.CreateTable(t.Context(), table.Identifier(), sc,
		icebergcataloggo.WithProperties(props), icebergcataloggo.WithSortOrder(order))
	require.NoError(t, err, "create the object_metadata table")
	return created
}

// evolvesWithExtraColumn appends the optional column a later schema version would add (design §3.1:
// "schema changes only add optional fields with new IDs") and returns the evolved table.
func evolvesWithExtraColumn(t *testing.T, tbl *icebergtable.Table) *icebergtable.Table {
	t.Helper()
	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(false, false).
		AddColumn([]string{table.ExtraColumn}, iceberg.StringType{}, "a column this writer does not know", false, nil).
		Commit())
	evolved, err := txn.Commit(t.Context())
	require.NoError(t, err, "append the extra column")
	require.Len(t, evolved.Schema().Fields(), 11)
	return evolved
}

// appendsWithExtraColumn appends rows through iceberg-go, with a value for the extra column of the
// evolved schema: a writer that knows a column this package's Row does not.
func appendsWithExtraColumn(t *testing.T, env *testEnv, tbl *icebergtable.Table, rows []table.Row, extra string, sourceCommit string) *icebergtable.Table {
	t.Helper()
	extras := make([]string, len(rows))
	for i := range extras {
		extras[i] = extra
	}
	reader, err := table.NewExtraColumnRecordReader(tbl.Schema(), rows, extras)
	require.NoError(t, err)
	defer reader.Release()
	txn := tbl.NewTransaction()
	require.NoError(t, txn.Append(t.Context(), reader, table.SnapshotProperties(env.dataRepository(), sourceCommit)))
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.PropertySourceCommit: sourceCommit}))
	appended, err := txn.Commit(t.Context())
	require.NoError(t, err, "append the row with the extra column")
	return appended
}

// extraColumnValues reads the extra column of every row of the table, keyed by path. A nil value is
// a null cell.
func extraColumnValues(t *testing.T, tbl *icebergtable.Table) map[string]*string {
	t.Helper()
	scan := tbl.Scan(icebergtable.WithSelectedFields(table.ColumnPath, table.ExtraColumn))
	defer func() { _ = scan.Close() }()
	schema, batches, err := scan.ToArrowRecords(t.Context())
	require.NoError(t, err, "scan the path and extra columns")
	pathIndex := schema.FieldIndices(table.ColumnPath)
	extraIndex := schema.FieldIndices(table.ExtraColumn)
	require.NotEmpty(t, pathIndex)
	require.NotEmpty(t, extraIndex)
	values := map[string]*string{}
	for batch, err := range batches {
		require.NoError(t, err)
		paths := batch.Column(pathIndex[0]).(*array.String)
		extras := batch.Column(extraIndex[0]).(*array.String)
		for i := range int(batch.NumRows()) {
			path := paths.Value(i)
			if extras.IsNull(i) {
				values[path] = nil
				continue
			}
			value := extras.Value(i)
			values[path] = &value
		}
		batch.Release()
	}
	return values
}

// TestCompactKeepsAColumnTheWriterDoesNotKnow is the probe of MDS_REVIEW_2.md R2T-1. Design §3.1
// allows a later schema version to append an optional column with a new ID, and a foreign writer
// fills it. Compact used to decode every row into the ten fields of Row and rebuild them, so the
// unknown column was appended no value while the others got one, and arrow's record builder raised
// "some fields have excessive number of rows" inside iceberg-go's writer goroutine: no recover could
// reach it, and the indexer process died. The compaction now re-emits the scan's own columns, so the
// column keeps its value.
func TestCompactKeepsAColumnTheWriterDoesNotKnow(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	tbl := env.createTable(t)
	seeded, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, nil),
		row(t, "c", "c1", 3, when, nil),
	}, nil, "c1")
	require.NoError(t, err)
	seeded, err = table.ApplyDelta(t.Context(), seeded, []table.Row{
		row(t, "b", "c2", 2, when, nil),
		row(t, "d", "c2", 4, when, nil),
	}, nil, "c2")
	require.NoError(t, err)

	evolved := evolvesWithExtraColumn(t, seeded)
	appended := appendsWithExtraColumn(t, env, evolved, []table.Row{
		row(t, "e", "c3", 5, when, nil),
	}, "keep-me", "c3")

	// R3T-8 of MDS_REVIEW_3.md: a streamed run left the reader's schema nil until its first batch
	// arrived, so the "no task" branch - the projection, whose fields carry the table's field IDs -
	// was the common path, and the schema the reader declared was not the schema its batches carry.
	// The reader now takes the schema of the file's scan at open, so the declaration is known before
	// the first batch and describes every batch of the merge.
	// A two-row output batch makes the merge build batches of several runs as well as slices of one,
	// so the assertion covers both ways buildMergedBatch produces a batch.
	restoreBatchSize := table.CompactRecordBatchSize(2)
	defer restoreBatchSize()
	declared, batchSchemas, err := table.CompactReaderSchemas(t.Context(), appended)
	require.NoError(t, err)
	require.NotNil(t, declared, "the reader declares a schema before its first batch")
	require.NotEmpty(t, batchSchemas, "the reader yielded the table's rows")
	for _, batchSchema := range batchSchemas {
		require.True(t, declared.Equal(batchSchema),
			"the schema the reader declares is the schema its batches carry")
	}

	compacted, err := table.Compact(t.Context(), appended, "c3")
	require.NoError(t, err, "a compaction must not panic on a column it does not know")

	values := extraColumnValues(t, env.loadTable(t))
	require.Len(t, values, 5)
	require.NotNil(t, values["e"], "the extra column of the foreign writer's row survives")
	require.Equal(t, "keep-me", *values["e"])
	for _, path := range []string{"a", "b", "c", "d"} {
		require.Nil(t, values[path], "path %s was written before the column existed", path)
	}
	requireScanRows(t, env.loadTable(t), []table.Row{
		row(t, "a", "c1", 1, when, nil),
		row(t, "b", "c2", 2, when, nil),
		row(t, "c", "c1", 3, when, nil),
		row(t, "d", "c2", 4, when, nil),
		row(t, "e", "c3", 5, when, nil),
	})
	require.Equal(t, "c3", compacted.Properties()[table.PropertySourceCommit])
}

// TestWritesRefuseAColumnTheWriterDoesNotKnow is the other half of R2T-1: a write that has to build
// rows from Row's ten fields cannot fill a column it was not given, so it refuses the table with
// ErrUnknownSchema instead of panicking (or silently writing nulls over data another writer owns).
// A delete-only delta writes no row and still applies.
func TestWritesRefuseAColumnTheWriterDoesNotKnow(t *testing.T) {
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// newTable builds the evolved table: two rows of this writer, then the extra column, then one
	// row of a writer that fills it.
	newTable := func(t *testing.T) (*testEnv, *icebergtable.Table) {
		t.Helper()
		env := newTestEnv(t)
		tbl := env.createTable(t)
		seeded, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
			row(t, "a", "c1", 1, when, nil),
			row(t, "b", "c1", 2, when, nil),
		}, nil, "c1")
		require.NoError(t, err)
		evolved := evolvesWithExtraColumn(t, seeded)
		return env, appendsWithExtraColumn(t, env, evolved, []table.Row{
			row(t, "e", "c2", 5, when, nil),
		}, "keep-me", "c2")
	}

	t.Run("a rebuild refuses the table before it writes", func(t *testing.T) {
		env, tbl := newTable(t)
		before := snapshotCount(tbl)
		_, err := table.Rebuild(t.Context(), tbl, rowSeq([]table.Row{row(t, "z", "c3", 6, when, nil)}), "c3")
		require.ErrorIs(t, err, table.ErrUnknownSchema)
		require.Equal(t, before, snapshotCount(env.loadTable(t)), "the refused rebuild committed nothing")
	})

	t.Run("an upsert delta refuses the table before it writes", func(t *testing.T) {
		env, tbl := newTable(t)
		before := snapshotCount(tbl)
		_, err := table.ApplyDelta(t.Context(), tbl, []table.Row{row(t, "z", "c3", 6, when, nil)}, nil, "c3")
		require.ErrorIs(t, err, table.ErrUnknownSchema)
		require.Equal(t, before, snapshotCount(env.loadTable(t)), "the refused delta committed nothing")
	})

	t.Run("a delete-only delta still applies", func(t *testing.T) {
		env, tbl := newTable(t)
		updated, err := table.ApplyDelta(t.Context(), tbl, nil, []string{"a"}, "c3")
		require.NoError(t, err, "a delta that writes no row does not need to build one")
		require.Equal(t, "c3", updated.Properties()[table.PropertySourceCommit])
		requireScanRows(t, env.loadTable(t), []table.Row{
			row(t, "b", "c1", 2, when, nil),
			row(t, "e", "c2", 5, when, nil),
		})
		values := extraColumnValues(t, env.loadTable(t))
		require.NotNil(t, values["e"], "a delete must not touch the column it does not know")
		require.Equal(t, "keep-me", *values["e"])
	})
}

// TestCompactSortsAFileWrittenBeforeTheFix is R2T-2's legacy case. A compaction before commit
// 2daeca114 streamed the scan's batches straight back into Append, and iceberg-go sorts each record
// batch on its own, so one data file could carry [a c b d] for the rows [a b c d]. The k-way merge
// treats each file as a sorted stream, so such a file must be detected and sorted on its own: the
// compaction succeeds, keeps the declared order, and loses no row. (Streaming the file as a run
// would fail its in-order check instead of writing the wrong order.)
func TestCompactSortsAFileWrittenBeforeTheFix(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tbl := env.createTable(t)

	arrowSchema, err := table.ArrowSchema(tbl.Schema())
	require.NoError(t, err)
	legacyFile := []arrow.RecordBatch{
		rowBatchOf(t, tbl, 2, row(t, "c", "c1", 3, when, nil), row(t, "a", "c1", 1, when, nil)),
		rowBatchOf(t, tbl, 2, row(t, "d", "c1", 4, when, nil), row(t, "b", "c1", 2, when, nil)),
	}
	txn := tbl.NewTransaction()
	// iceberg-go sorts each batch on its own, so the file is [a c][b d]: ascending inside a batch,
	// not across the file, which is exactly the shape the old compaction wrote.
	require.NoError(t, txn.Append(t.Context(),
		table.NewBatchesRecordReader(arrowSchema, batchesOf(legacyFile)),
		table.SnapshotProperties(env.dataRepository(), "c1")))
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.PropertySourceCommit: "c1"}))
	_, err = txn.Commit(t.Context())
	require.NoError(t, err)

	require.Equal(t, []string{"a", "c", "b", "d"}, pathsOf(scanRowsInWriteOrder(t, env.loadTable(t))),
		"the data file is not in the table's declared order, as a file written before 2daeca114 was")

	compacted, err := table.Compact(t.Context(), env.loadTable(t), "c1")
	require.NoError(t, err, "a file written before the fix must be handled, not refused")
	require.Equal(t, []string{"a", "b", "c", "d"}, pathsOf(scanRowsInWriteOrder(t, compacted)),
		"the compaction writes the declared path ASC order")
	requireScanRows(t, env.loadTable(t), []table.Row{
		row(t, "a", "c1", 1, when, nil),
		row(t, "b", "c1", 2, when, nil),
		row(t, "c", "c1", 3, when, nil),
		row(t, "d", "c1", 4, when, nil),
	})
}

// rowBatchOf returns one record batch of the rows, in the order given, through NewRecordReader: the
// batches are what a scan (or the writer of a pre-2daeca114 compaction) hands to a transaction.
func rowBatchOf(t *testing.T, tbl *icebergtable.Table, batchSize int, rows ...table.Row) arrow.RecordBatch {
	t.Helper()
	reader, err := table.NewRecordReader(tbl.Schema(), rows, batchSize)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	batch := reader.RecordBatch()
	batch.Retain()
	return batch
}

// batchesOf is a batch source over a slice, the shape NewBatchesRecordReader reads.
func batchesOf(batches []arrow.RecordBatch) func(yield func(arrow.RecordBatch, error) bool) {
	return func(yield func(arrow.RecordBatch, error) bool) {
		for _, batch := range batches {
			if !yield(batch, nil) {
				return
			}
		}
	}
}

// TestCompactHoldsABoundedNumberOfRows is the probe of MDS_REVIEW_2.md R2T-2. The compaction used to
// collect every row of the table (a Row plus its strings and maps, ~340 B, and ~1.5 KiB/row for the
// whole scan+rows+write path) and hand them back one batch at a time, so its peak followed the size
// of the table. The merge holds one read batch per data file and one batch of output, and the
// per-file read batch is the merge's row budget divided by the number of files, so the number of
// rows it holds is the same for a table of 2 000 rows and one of 20 000.
func TestCompactHoldsABoundedNumberOfRows(t *testing.T) {
	const (
		mergeBudget  = 200
		outputBatch  = 64
		writeBatches = 200
	)
	restoreBudget := table.CompactMergeRowBudget(mergeBudget)
	t.Cleanup(restoreBudget)
	restoreBatch := table.CompactRecordBatchSize(outputBatch)
	t.Cleanup(restoreBatch)

	peak := func(rows int) int {
		env := newTestEnv(t)
		tbl := env.createTable(t)
		when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
		upserts := make([]table.Row, 0, rows)
		for i := range rows {
			upserts = append(upserts, row(t, fmt.Sprintf("path-%08d", i), "c1", int64(i), when, nil))
		}
		seeded, err := table.ApplyDelta(t.Context(), tbl, upserts, nil, "c1",
			table.WithRecordBatchSize(writeBatches))
		require.NoError(t, err)
		scan := seeded.Scan()
		tasks, err := scan.PlanFiles(t.Context())
		require.NoError(t, err)
		require.NoError(t, scan.Close())
		require.Len(t, tasks, 1, "the table is one data file, so the merge holds one file's batch")

		var highest atomic.Int64
		unobserve := table.MergeRowsHeld(func(held int) {
			for {
				was := highest.Load()
				if int64(held) <= was || highest.CompareAndSwap(was, int64(held)) {
					return
				}
			}
		})
		defer unobserve()
		_, err = table.Compact(t.Context(), seeded, "c1")
		require.NoError(t, err)
		return int(highest.Load())
	}

	small := peak(2_000)
	large := peak(20_000)
	require.Equal(t, small, large,
		"the merge holds the same number of rows for 2 000 and 20 000 rows: %d", small)
	require.LessOrEqual(t, large, mergeBudget+outputBatch,
		"the merge holds one read batch of the one file plus one batch of output")
}

// ---------------------------------------------------------------------------------------------
// MDS_REVIEW_3.md R3T-1, R3T-4, R3T-7 and R3T-8: the compaction's file list and its read path
// ---------------------------------------------------------------------------------------------

// TestCompactReplacesASplitFileOnce is R3T-1 of MDS_REVIEW_3.md. iceberg-go splits a Parquet file
// above read.split.target-size into several scan tasks that all carry the same File, each with its
// own Start and Length (table/scan_splits.go:55-60 in iceberg-go v0.7.0), and ReplaceFiles refuses
// a delete list in which a path appears twice (table/transaction.go:1843: "delete data file paths
// must be unique for ReplaceFiles"). Compact removed the split file once per task, so the first
// compaction of a table holding such a file failed, and because the compaction trigger is derived
// from the snapshot it stayed due and failed again on every tick.
//
// A split target of one byte and a row group limit of two rows make the writer produce one file of
// four row groups that the scan plans as several tasks, which is the shape a file above the 128 MiB
// default reaches on a real table.
func TestCompactReplacesASplitFileOnce(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tbl := env.createTableWith(t, table.Schema(), iceberg.Properties{
		icebergtable.ReadSplitTargetSizeKey:  "1",
		icebergtable.ParquetRowGroupLimitKey: "2",
	})

	upserts := make([]table.Row, 0, 7)
	for i := range 7 {
		upserts = append(upserts, row(t, fmt.Sprintf("path-%02d", i), "c1", int64(i), when, nil))
	}
	seeded, err := table.ApplyDelta(t.Context(), tbl, upserts, nil, "c1")
	require.NoError(t, err)

	scan := seeded.Scan()
	tasks, err := scan.PlanFiles(t.Context())
	require.NoError(t, err)
	require.NoError(t, scan.Close())
	paths := map[string]struct{}{}
	for _, task := range tasks {
		paths[task.File.FilePath()] = struct{}{}
	}
	require.Len(t, paths, 1, "the delta wrote one data file")
	require.Greater(t, len(tasks), len(paths),
		"the scan plans more tasks than data files, so a file is split into ranges")

	compacted, err := table.Compact(t.Context(), seeded, "c1")
	require.NoError(t, err, "a split data file must be replaced, not listed twice")
	require.Equal(t, "c1", compacted.Properties()[table.PropertySourceCommit])
	requireScanRows(t, env.loadTable(t), upserts)
}

// TestCompactDividesTheMergeBudgetAcrossFiles is the second half of MDS_REVIEW_3.md R3T-4: the merge
// budget is divided by the number of files, and the batch a file's stream actually reads is that
// division rather than the floor. TestCompactHoldsABoundedNumberOfRows cannot see this - it
// compacts one file with a budget below the floor, so it observes max(256, budget) and a sum - so
// this test compacts four real data files and requires the row count of the batches the streams
// pull, not the total the merge holds.
func TestCompactDividesTheMergeBudgetAcrossFiles(t *testing.T) {
	const (
		files       = 4
		rowsPerFile = 1_000
		mergeBudget = 3_000
		// compactMergeRowBudget/files, which is above the 256-row floor, so the budget decides what
		// one file's read batch is.
		wantBatchRows = mergeBudget / files
		floorRows     = 256
	)
	restoreBudget := table.CompactMergeRowBudget(mergeBudget)
	t.Cleanup(restoreBudget)

	env := newTestEnv(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tbl := env.createTable(t)

	// One delta per file: each commit appends one data file, so the table has four of them.
	var want []table.Row
	seeded := tbl
	for file := range files {
		upserts := make([]table.Row, 0, rowsPerFile)
		for i := range rowsPerFile {
			upserts = append(upserts, row(t, fmt.Sprintf("file-%d-path-%06d", file, i), "c1",
				int64(i), when, nil))
		}
		var err error
		seeded, err = table.ApplyDelta(t.Context(), seeded, upserts, nil, "c1")
		require.NoError(t, err)
		want = append(want, upserts...)
	}

	scan := seeded.Scan()
	tasks, err := scan.PlanFiles(t.Context())
	require.NoError(t, err)
	require.NoError(t, scan.Close())
	require.Len(t, tasks, files, "the table is one data file per delta")

	var highest, batches atomic.Int64
	unobserve := table.StreamBatchRows(func(rows int) {
		batches.Add(1)
		for {
			was := highest.Load()
			if int64(rows) <= was || highest.CompareAndSwap(was, int64(rows)) {
				return
			}
		}
	})
	defer unobserve()

	compacted, err := table.Compact(t.Context(), seeded, "c1")
	require.NoError(t, err)
	unobserve()

	require.GreaterOrEqual(t, batches.Load(), int64(files),
		"every file's stream pulled at least one batch")
	require.LessOrEqual(t, highest.Load(), int64(wantBatchRows),
		"a stream's read batch is the budget divided by the files")
	require.Greater(t, highest.Load(), int64(floorRows),
		"the division, not the floor, decided a stream's read batch")
	requireScanRows(t, env.loadTable(t), want)
	require.Equal(t, "c1", compacted.Properties()[table.PropertySourceCommit])
}

// TestCompactReleasesEveryBatchItReads is R3T-7 of MDS_REVIEW_3.md. The compaction's streams were
// released only by the merge's own defer, and iter.Pull2 does not run a body that was never started,
// while Compact opens the reader before ReplaceFiles and Append - either of which can fail before the
// first batch is pulled. Under memory.DefaultAllocator the garbage collector hides that; under a
// memory.CheckedAllocator every batch that is not released is still allocated when the compaction
// returns. The scan's batches and the batches the merge builds both go through the checked allocator
// here, so an unreleased batch of either kind fails the test.
func TestCompactReleasesEveryBatchItReads(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tbl := env.createTable(t)

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	restoreAllocator := table.CompactionAllocator(alloc)
	t.Cleanup(restoreAllocator)
	ctx := compute.WithAllocator(t.Context(), alloc)

	// Three data files, so the merge concatenates groups of rows from different batches and the
	// batches it builds itself are allocated as well.
	var want []table.Row
	seeded := tbl
	for file := range 3 {
		upserts := make([]table.Row, 0, 50)
		for i := range 50 {
			upserts = append(upserts, row(t, fmt.Sprintf("file-%d-path-%03d", file, i), "c1",
				int64(i), when, nil))
		}
		var err error
		seeded, err = table.ApplyDelta(ctx, seeded, upserts, nil, "c1")
		require.NoError(t, err)
		want = append(want, upserts...)
	}

	compacted, err := table.Compact(ctx, seeded, "c1")
	require.NoError(t, err)
	require.Equal(t, "c1", compacted.Properties()[table.PropertySourceCommit])
	requireScanRows(t, env.loadTable(t), want)
}
