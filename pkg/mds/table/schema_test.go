package table_test

// R2T-3 of MDS_REVIEW_2.md: every rewrite is non-replayable, not only one that removes a delete
// file.
//
// iceberg-go marks a commit non-replayable only when its snapshot removes a delete file
// (table/snapshot_producers.go sets txn.noReplay from deletedDeleteFiles/deletedDVsByRef), and how
// many times a conflicting commit is replayed comes from the table property
// commit.retry.num-retries (table/table.go doCommit reads it before the commit, default 0). So the
// rebuild/compaction guarantee design §4.3 step 2 states - "a concurrent writer is refused with a
// conflict instead of having the removal replayed" - held only while the table happened to hold a
// delete file and while nobody raised the property. This package now pins the property to 0 at
// creation, re-asserts it on every write, and repairs a table created before the change with one
// metadata-only commit before the write, so the stale-rewrite conflict is terminal for every write
// of every table.

import (
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// raiseCommitRetries sets commit.retry.num-retries to 1, the state an operator can create and the
// state every table created before this package pinned the property is in. It also relaxes the
// client-side overwrite isolation to "snapshot", which is the property that makes a replayed
// rewrite actually destructive in the pinned iceberg-go: with the default serializable isolation a
// replay of a rewrite is refused by iceberg-go's conflict validation, so without this the defect
// this test is about cannot be observed at all (probe on 902839f67: the stale rebuild replayed and
// left two rows for path "a").
func raiseCommitRetries(t *testing.T, tbl *icebergtable.Table) *icebergtable.Table {
	t.Helper()
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{
		table.PropertyCommitRetries: "1",
		// iceberg-go's write.update.isolation-level, spelled here so the test does not depend on an
		// unexported name. "snapshot" is the relaxed level an operator can set.
		"write.update.isolation-level": "snapshot",
	}))
	legacy, err := txn.Commit(t.Context())
	require.NoError(t, err, "raise the retry budget")
	require.Equal(t, "1", legacy.Properties()[table.PropertyCommitRetries])
	return legacy
}

// TestCreateTablePinsNoReplay: the table carries the no-replay property from its creation, and a
// caller's properties cannot drop it - the fixed properties are written after the caller's.
func TestCreateTablePinsNoReplay(t *testing.T) {
	require.Equal(t, icebergtable.CommitNumRetriesKey, table.PropertyCommitRetries,
		"the property name is iceberg-go's own key")
	require.Equal(t, "0", table.CommitRetriesNone)

	env := newTestEnv(t)
	tbl := env.createTable(t)
	require.Equal(t, table.CommitRetriesNone, tbl.Properties()[table.PropertyCommitRetries],
		"a table this package creates can never replay a commit")

	// A caller that asks for a retry budget does not get one: replaying a whole-table rewrite
	// duplicates rows, so it is not a table option.
	otherEnv := newTestEnv(t)
	guarded, err := table.CreateTable(t.Context(), otherEnv.rest, otherEnv.dataRepository(), iceberg.Properties{
		table.PropertyCommitRetries: "5",
	})
	require.NoError(t, err)
	require.Equal(t, table.CommitRetriesNone, guarded.Properties()[table.PropertyCommitRetries])
}

// TestEveryWriteRepairsTheCommitRetryProperty: a table created before the property existed (or one
// an operator raised) is repaired on its next write, before the write commits, and the repair is a
// metadata-only commit - it adds no snapshot. The stale-handle test below proves that repairing
// before the write is what makes the write itself non-replayable.
func TestEveryWriteRepairsTheCommitRetryProperty(t *testing.T) {
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	writes := []struct {
		name string
		// write runs one write of the package and returns the committed table.
		write func(t *testing.T, env *testEnv, tbl *icebergtable.Table) *icebergtable.Table
		// snapshots is how many snapshots the write itself adds.
		snapshots int
	}{
		{
			name: "delta",
			write: func(t *testing.T, _ *testEnv, tbl *icebergtable.Table) *icebergtable.Table {
				// An empty delta still writes the table property (§4.3 step 3) and adds no snapshot,
				// so the only commit it can add is the repair's.
				updated, err := table.ApplyDelta(t.Context(), tbl, nil, nil, "c2")
				require.NoError(t, err)
				return updated
			},
			snapshots: 0,
		},
		{
			name: "upsert delta",
			write: func(t *testing.T, _ *testEnv, tbl *icebergtable.Table) *icebergtable.Table {
				updated, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
					row(t, "c", "c2", 3, when, nil),
				}, nil, "c2")
				require.NoError(t, err)
				return updated
			},
			snapshots: 2,
		},
		{
			name: "rebuild",
			write: func(t *testing.T, _ *testEnv, tbl *icebergtable.Table) *icebergtable.Table {
				rebuilt, err := table.Rebuild(t.Context(), tbl, rowSeq([]table.Row{
					row(t, "a", "c2", 1, when, nil),
					row(t, "c", "c2", 3, when, nil),
				}), "c2")
				require.NoError(t, err)
				return rebuilt
			},
			snapshots: 2,
		},
		{
			name: "compaction",
			write: func(t *testing.T, _ *testEnv, tbl *icebergtable.Table) *icebergtable.Table {
				compacted, err := table.Compact(t.Context(), tbl, "c2")
				require.NoError(t, err)
				return compacted
			},
			snapshots: 2,
		},
	}
	for _, tc := range writes {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t)
			tbl := env.createTable(t)
			seeded, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
				row(t, "a", "c1", 1, when, nil),
				row(t, "b", "c1", 2, when, nil),
			}, nil, "c1")
			require.NoError(t, err)
			legacy := raiseCommitRetries(t, seeded)
			before := snapshotCount(legacy)

			updated := tc.write(t, env, legacy)

			require.Equal(t, table.CommitRetriesNone, updated.Properties()[table.PropertyCommitRetries],
				"the write pins the table to no replay, for every later writer too")
			require.Equal(t, before+tc.snapshots, snapshotCount(updated),
				"the repair is metadata-only: the write's own snapshots are the only ones added")
		})
	}
}

// TestAStaleRewriteIsRefusedNotReplayed is the probe of R2T-3's defect. A whole-table rewrite stages
// a ReplaceFiles of the snapshot's data files and an Append of the rows it read. On a table whose
// retry budget is 1 - and whose snapshot holds no delete file, so iceberg-go does not mark the
// commit non-replayable - the lost optimistic-concurrency check made iceberg-go refresh the parent
// and replay the rewrite: the concurrent writer's data file was never in the captured removal set,
// so it survived next to the replayed rows and the same path appeared twice.
//
// With every write pinning commit.retry.num-retries to 0, the stale rewrite is refused with the
// conflict the indexer retries (§4.4), and the table holds exactly the concurrent writer's state.
func TestAStaleRewriteIsRefusedNotReplayed(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tbl := env.createTable(t)
	seeded, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
		row(t, "a", "c1", 1, when, nil),
		row(t, "b", "c1", 2, when, nil),
	}, nil, "c1")
	require.NoError(t, err)
	legacy := raiseCommitRetries(t, seeded)

	// Two writers of the same snapshot: one commits a new version of a, the other tries a rebuild
	// of the whole table from the snapshot it read.
	stale := legacy
	concurrent, err := table.ApplyDelta(t.Context(), legacy, []table.Row{
		row(t, "a", "c2", 10, when, nil),
	}, nil, "c2")
	require.NoError(t, err)

	_, err = table.Rebuild(t.Context(), stale, rowSeq([]table.Row{
		row(t, "a", "c1", 1, when, nil),
		row(t, "b", "c1", 2, when, nil),
		row(t, "d", "c3", 4, when, nil),
	}), "c3")
	require.Error(t, err, "a stale rewrite must be refused, not replayed")
	require.ErrorIs(t, err, table.ErrStaleTable)

	// Nothing of the refused rebuild is in the table, and the concurrent writer's row appears once.
	rows := scanRows(t, env.loadTable(t))
	require.Len(t, rows, 2)
	require.Equal(t, []string{"a", "b"}, pathsOf(rows))
	require.Equal(t, "c2", rows[0].CommitID, "the concurrent writer's version of a survives")
	require.Equal(t, table.CommitRetriesNone, env.loadTable(t).Properties()[table.PropertyCommitRetries])
	require.Equal(t, "c2", concurrent.Properties()[table.PropertySourceCommit])
}
