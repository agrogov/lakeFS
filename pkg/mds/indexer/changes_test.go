package indexer_test

// changes_test.go covers design §4.3 steps 1 and 2: how a commit of the data repository becomes a
// change set, how a right-side stat that the experimental include_right_stats did not fill in falls
// back to statObject, and when a change set is written as a full rebuild instead of a delta.

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/reader"
)

// TestChangeSetFromDiff pins §4.3 step 1: added and changed entries become upserts, removed entries
// become deletes, and the rows carry the commit's committer and creation date.
func TestChangeSetFromDiff(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil,
		[]reader.ObjectStat{object("a", 10), object("b", 20), object("keep", 30)})
	e.tick()

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []reader.Change{
		added("c", 40),
		{Path: "a", Type: reader.ChangeChanged, SizeBytes: 11, Right: &reader.ObjectStat{
			Path: "a", SizeBytes: 11, ETag: "etag-a2", Mtime: object("a", 11).Mtime,
		}},
		removed("b"),
	})
	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 1, e.fake.applyCalls)
	require.Empty(t, e.fake.statCalls, "a right-side stat that is complete is used as it is")

	state, _ := e.fake.tableState("main")
	require.Equal(t, []string{"a", "c", "keep"}, state.paths())
	e.requireRow("main", "c", "s2", 40)
	e.requireRow("main", "keep", "s1", 30)
	a := e.requireRow("main", "a", "s2", 11)
	require.Equal(t, "etag-a2", a.ETag)
}

// TestChangeSetFallsBackToStatObject pins the plan's risk entry: include_right_stats is
// experimental, so an entry the server did not describe - or described without a checksum or an
// mtime - is read with statObject instead (§4.3 step 1).
func TestChangeSetFallsBackToStatObject(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
	e.tick()

	empty := reader.ObjectStat{Path: "b"} // no checksum, no mtime: not a usable stat
	e.fake.addCommit("repo", "main", "s2", []string{"s1"},
		[]reader.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
	e.fake.setDiff("repo", "s1", "s2", []reader.Change{
		{Path: "b", Type: reader.ChangeAdded, SizeBytes: 20, Right: &empty},
		{Path: "c", Type: reader.ChangeAdded, SizeBytes: 30}, // no right side at all
	})
	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, []string{"b", "c"}, e.fake.statCalls)

	b := e.requireRow("main", "b", "s2", 20)
	require.Equal(t, "etag-b", b.ETag, "the row carries the stats statObject returned")
	require.Equal(t, object("b", 20).Mtime, b.LastModified)
	require.Equal(t, map[string]string{"X-Amz-Meta-Owner": "alice"}, b.UserMetadata)
	e.requireRow("main", "c", "s2", 30)
}

// TestStatFallbackCoversEveryNeededField pins §4.3 step 1's rule that a needed field missing from the
// right-side stat means one statObject call - not just an empty checksum or a zero mtime. The right
// side of a diff is used as it is only when it carries the checksum, the modification time and the
// size; content_type and user metadata may legitimately be empty, so they never trigger the
// fallback.
func TestStatFallbackCoversEveryNeededField(t *testing.T) {
	mtime := object("b", 20).Mtime
	right := func(mutate func(*reader.ObjectStat)) *reader.ObjectStat {
		stat := reader.ObjectStat{Path: "b", SizeBytes: 20, ETag: "etag-b", Mtime: mtime}
		if mutate != nil {
			mutate(&stat)
		}
		return &stat
	}

	cases := []struct {
		name     string
		change   reader.Change
		wantStat bool
	}{
		{
			name:   "complete right side",
			change: reader.Change{Path: "b", Type: reader.ChangeAdded, SizeBytes: 20, Right: right(nil)},
		},
		{
			// include_right_stats is not honoured at all: the whole right-side stat is absent.
			name:     "no right side",
			change:   reader.Change{Path: "b", Type: reader.ChangeAdded, SizeBytes: 20},
			wantStat: true,
		},
		{
			name: "no checksum",
			change: reader.Change{Path: "b", Type: reader.ChangeAdded, SizeBytes: 20,
				Right: right(func(stat *reader.ObjectStat) { stat.ETag = "" })},
			wantStat: true,
		},
		{
			name: "no modification time",
			change: reader.Change{Path: "b", Type: reader.ChangeChanged, SizeBytes: 20,
				Right: right(func(stat *reader.ObjectStat) { stat.Mtime = time.Time{} })},
			wantStat: true,
		},
		{
			// The size is on the diff entry, not inside the right-side stat, and a size the
			// server did not report arrives as zero.
			name:     "no size",
			change:   reader.Change{Path: "b", Type: reader.ChangeAdded, Right: right(nil)},
			wantStat: true,
		},
		{
			name: "empty content type and metadata are not missing fields",
			change: reader.Change{Path: "b", Type: reader.ChangeAdded, SizeBytes: 20,
				Right: right(func(stat *reader.ObjectStat) { stat.ContentType = ""; stat.UserMetadata = nil })},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
			e.tick()

			e.fake.addCommit("repo", "main", "s2", []string{"s1"},
				[]reader.ObjectStat{object("a", 10), object("b", 20)})
			e.fake.setDiff("repo", "s1", "s2", []reader.Change{tc.change})
			e.tick()

			require.NoError(t, e.health.repositoryError("repo"))
			if tc.wantStat {
				require.Equal(t, []string{"b"}, e.fake.statCalls, "a needed field is missing, so the entry is statted")
			} else {
				require.Empty(t, e.fake.statCalls, "the right-side stat is complete and used as it is")
			}

			b := e.requireRow("main", "b", "s2", 20)
			require.Equal(t, "etag-b", b.ETag)
			require.Equal(t, mtime, b.LastModified)
		})
	}
}

// TestChangeSetStopsAtRebuildThreshold pins the bound §4.3 step 2 is about: the change set of one
// commit is bounded by rebuild_threshold, not by the size of its diff. The indexer stops paging the
// diff at the first entry above the threshold and rebuilds the table at the commit, so it neither
// reads the rest of the diff nor statts any of its entries.
func TestChangeSetStopsAtRebuildThreshold(t *testing.T) {
	const (
		threshold = 2
		entries   = 10
	)
	e := newEnv(t, func(opts *indexer.Options) { opts.RebuildThreshold = threshold })
	// Two entries per page: a full read of the diff below takes five DiffRefs calls.
	e.fake.pageSize = 2
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
	e.tick()
	require.Equal(t, 1, e.fake.rebuildCalls)
	require.Zero(t, e.fake.diffRefsCalls(), "the first indexing is a rebuild, not a diff")

	// No right-side stats at all, so a change set that was read to the end would stat every entry.
	objects := make([]reader.ObjectStat, 0, entries)
	upserts := make([]reader.Change, 0, entries)
	for i := range entries {
		path := fmt.Sprintf("p%02d", i)
		size := int64(10 * (i + 1))
		objects = append(objects, object(path, size))
		upserts = append(upserts, reader.Change{Path: path, Type: reader.ChangeAdded, SizeBytes: size})
	}
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, objects)
	e.fake.setDiff("repo", "s1", "s2", upserts)

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 2, e.fake.diffRefsCalls(),
		"the diff is read only up to the first entry above rebuild_threshold, so two of five pages")
	require.Empty(t, e.fake.statCalls, "a commit that is rebuilt costs no statObject call")
	require.Zero(t, e.fake.applyCalls, "the cut-off change set is never applied as a delta")
	require.Equal(t, 2, e.fake.rebuildCalls, "the commit is written as a full listing instead")
	// One page for the backfill listing of s1, and five for the rebuild listing of ten objects in
	// pages of two.
	require.Equal(t, 6, e.fake.listObjectCalls(), "the rebuild reads the full listing of the commit")

	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Len(t, state.rows, entries)
	for _, path := range state.paths() {
		require.Equal(t, "s2", state.rows[path].CommitID)
	}
}

// TestUnknownChangeTypeIsRefused pins the strictness of §4.3 step 1: a diff type the indexer does
// not know stops the branch instead of silently losing a change.
func TestUnknownChangeTypeIsRefused(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
	e.tick()

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []reader.Change{{Path: "a", Type: reader.ChangeType("renamed")}})
	e.tick()

	err := e.health.branch("repo", "main").err
	require.ErrorIs(t, err, indexer.ErrUnknownChangeType)
	require.Contains(t, e.metrics.errors, "repo/branch")
	require.Equal(t, "s1", e.health.branch("repo", "main").lastIndexed, "the position did not move")
}

// TestRebuildThreshold pins §4.3 step 2: a change set larger than rebuild_threshold is written as a
// full listing of the commit, so every row carries that commit, and the delete predicate of the
// delta is never built.
func TestRebuildThreshold(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.RebuildThreshold = 1
	})
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
	e.tick()
	require.Equal(t, 1, e.fake.rebuildCalls)

	e.fake.addCommit("repo", "main", "s2", []string{"s1"},
		[]reader.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
	e.fake.setDiff("repo", "s1", "s2", []reader.Change{added("b", 20), added("c", 30)})
	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Zero(t, e.fake.applyCalls, "the delta is not written at all")
	require.Equal(t, 2, e.fake.rebuildCalls)
	require.Equal(t, 2, e.metrics.rebuilds)

	state, _ := e.fake.tableState("main")
	require.Equal(t, []string{"a", "b", "c"}, state.paths())
	for _, path := range state.paths() {
		require.Equal(t, "s2", state.rows[path].CommitID,
			"a rebuild writes every row with the commit it rebuilt at (the documented loss of per-object history)")
	}
}

// TestRowColumnsFromTheCommit pins §3.1's optional columns: committer and creation_date come from
// the commit (and are null when the commit has none), content_type is null when the object has none.
func TestRowColumnsFromTheCommit(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{{
		Path:      "a",
		SizeBytes: 10,
		Mtime:     object("a", 10).Mtime,
		ETag:      "etag-a",
	}})
	e.fake.setCommit("repo", reader.Commit{ID: "s1", Message: "no committer, no date"})
	e.tick()

	row := e.requireRow("main", "a", "s1", 10)
	require.Nil(t, row.Committer, "no committer is null, not an empty string")
	require.Nil(t, row.CreationDate, "no creation date is null")
	require.Nil(t, row.ContentType, "no content type is null")
	require.Empty(t, row.UserMetadata, "user_metadata is an empty map, never null")
}
