package indexer_test

// changes_test.go covers design §4.3 steps 1 and 2: how a commit of the data repository becomes a
// change set, how a right-side stat that the experimental include_right_stats did not fill in falls
// back to statObject, and when a change set is written as a full rebuild instead of a delta.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/client"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
)

// TestChangeSetFromDiff pins §4.3 step 1: added and changed entries become upserts, removed entries
// become deletes, and the rows carry the commit's committer and creation date.
func TestChangeSetFromDiff(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil,
		[]client.ObjectStat{object("a", 10), object("b", 20), object("keep", 30)})
	e.tick()

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{
		added("c", 40),
		{Path: "a", Type: client.ChangeChanged, SizeBytes: 11, Right: &client.ObjectStat{
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
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()

	empty := client.ObjectStat{Path: "b"} // no checksum, no mtime: not a usable stat
	e.fake.addCommit("repo", "main", "s2", []string{"s1"},
		[]client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
	e.fake.setDiff("repo", "s1", "s2", []client.Change{
		{Path: "b", Type: client.ChangeAdded, SizeBytes: 20, Right: &empty},
		{Path: "c", Type: client.ChangeAdded, SizeBytes: 30}, // no right side at all
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

// TestUnknownChangeTypeIsRefused pins the strictness of §4.3 step 1: a diff type the indexer does
// not know stops the branch instead of silently losing a change.
func TestUnknownChangeTypeIsRefused(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{{Path: "a", Type: client.ChangeType("renamed")}})
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
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	require.Equal(t, 1, e.fake.rebuildCalls)

	e.fake.addCommit("repo", "main", "s2", []string{"s1"},
		[]client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20), added("c", 30)})
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
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{{
		Path:      "a",
		SizeBytes: 10,
		Mtime:     object("a", 10).Mtime,
		ETag:      "etag-a",
	}})
	e.fake.setCommit("repo", client.Commit{ID: "s1", Message: "no committer, no date"})
	e.tick()

	row := e.requireRow("main", "a", "s1", 10)
	require.Nil(t, row.Committer, "no committer is null, not an empty string")
	require.Nil(t, row.CreationDate, "no creation date is null")
	require.Nil(t, row.ContentType, "no content type is null")
	require.Empty(t, row.UserMetadata, "user_metadata is an empty map, never null")
}
