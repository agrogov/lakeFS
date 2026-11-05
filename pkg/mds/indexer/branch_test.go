package indexer_test

// branch_test.go covers the per-branch state machine of design §4.2 and everything it recovers
// from: the crash table of §4.4, the seeding and full listing of §4.5, the first-parent merge rule
// of §3.2, the history rewrite of §4.7 and the branch lifecycle of §4.8. Every case drives the real
// code through one or more ticks of indexer.Tick against the fakes of fake_test.go.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/client"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// rows returns the committed rows of a branch of the metadata repository, by path.
func (e *env) rows(branch string) map[string]table.Row {
	e.t.Helper()
	state, ok := e.fake.tableState(branch)
	require.True(e.t, ok, "branch %q has no table", branch)
	return state.rows
}

// paths returns the sorted paths of a branch's committed rows.
func (e *env) paths(branch string) []string {
	e.t.Helper()
	state, ok := e.fake.tableState(branch)
	require.True(e.t, ok, "branch %q has no table", branch)
	return state.paths()
}

// requireRow asserts one row: the path exists, and its commit and size are what §3.1 says they
// should be.
func (e *env) requireRow(branch, path, commitID string, size int64) table.Row {
	e.t.Helper()
	row, ok := e.rows(branch)[path]
	require.True(e.t, ok, "path %q is indexed on %q", path, branch)
	require.Equal(e.t, commitID, row.CommitID, "commit_id of %q", path)
	require.Equal(e.t, size, row.SizeBytes, "size_bytes of %q", path)
	require.Equal(e.t, "repo", row.Repository)
	return row
}

// TestBackfillHead pins §4.5 step 2 with the default backfill: head. A branch that has never been
// indexed is written from one full listing of the head commit, all rows carry that commit, and the
// lakeFS commit and the mds-<commit> tag make the position addressable.
func TestBackfillHead(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10), object("b", 20)})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Zero(t, e.fake.applyCalls, "the first indexing is a rebuild, not a delta")
	require.Equal(t, 1, e.fake.rebuildCalls)

	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, "s1", state.sourceCommit, "the table records the source commit")
	require.Equal(t, []string{"a", "b"}, state.paths())

	row := e.requireRow("main", "a", "s1", 10)
	commit := e.fake.commits["repo"]["s1"]
	require.Equal(t, commit.Committer, *row.Committer, "committer comes from the commit")
	require.Equal(t, commit.CreationDate, *row.CreationDate, "creation_date comes from the commit")
	require.Equal(t, object("a", 10).Mtime, row.LastModified)
	require.Equal(t, "etag-a", row.ETag)
	require.Equal(t, "text/plain", *row.ContentType)
	require.Equal(t, map[string]string{"X-Amz-Meta-Owner": "alice"}, row.UserMetadata)

	head, ok := e.fake.branchHead("repo-metadata", "main")
	require.True(t, ok)
	tagCommit, ok := e.fake.tagCommit("repo-metadata", "mds-s1")
	require.True(t, ok, "a tag mds-<commit> makes the commit addressable (§3.3)")
	require.Equal(t, head, tagCommit)

	health := e.health.branch("repo", "main")
	require.Equal(t, "s1", health.lastIndexed)
	require.Equal(t, "s1", health.head)
	require.Zero(t, health.lag)
	require.NoError(t, health.err)
	require.Equal(t, 1, e.metrics.indexed["repo"])
	require.EqualValues(t, 2, e.metrics.rows)

	// Nothing changed, so a second tick writes no commit at all.
	before := len(e.fake.madeCommits())
	e.tick()
	require.Len(t, e.fake.madeCommits(), before)
}

// TestBackfillSeedsFromAnAncestorTag pins §4.5 step 1: a new branch whose history contains an
// indexed commit is created from that commit's tag and only the commits after it are applied, so a
// feature branch does not pay for a full listing.
func TestBackfillSeedsFromAnAncestorTag(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Repositories[0].Branches = []string{"main", "feature-*"}
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10), object("b", 20)})
	e.tick()
	require.Equal(t, 1, e.fake.rebuildCalls)

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 25)})
	// R@feature-x branches from s1 and has one commit of its own, s2.
	e.fake.setBranchHead("repo", "feature-x", "s2")

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Len(t, e.fake.createdBranches, 1)
	require.Equal(t, createdBranch{repository: "repo-metadata", branch: "feature-x", source: "mds-s1"},
		e.fake.createdBranches[0])
	require.Equal(t, 1, e.fake.rebuildCalls, "a seeded branch is not rebuilt")

	// The seeded table starts from s1, and the one commit after it is applied as a delta.
	e.requireRow("feature-x", "a", "s1", 10)
	b := e.requireRow("feature-x", "b", "s2", 25)
	require.Equal(t, e.fake.commits["repo"]["s2"].Committer, *b.Committer)

	_, ok := e.fake.tagCommit("repo-metadata", "mds-s2")
	require.True(t, ok, "the applied commit is tagged")
}

// TestBackfillHistory pins §4.5 step 2 with backfill: history: the branch is written from its
// oldest first-parent commit and every commit after it is applied in order.
func TestBackfillHistory(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Backfill = indexer.BackfillHistory
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
	e.fake.setDiff("repo", "s2", "s3", []client.Change{removed("a")})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 1, e.fake.rebuildCalls, "one rebuild at the oldest commit")
	require.Equal(t, 2, e.fake.applyCalls, "then one delta per commit after it")

	state, _ := e.fake.tableState("main")
	require.Equal(t, "s3", state.sourceCommit)
	require.Equal(t, []string{"b"}, state.paths(), "a was removed by s3")
	e.requireRow("main", "b", "s2", 20)
	require.Equal(t, 3, e.metrics.indexed["repo"])
	for _, commit := range []string{"s1", "s2", "s3"} {
		_, ok := e.fake.tagCommit("repo-metadata", "mds-"+commit)
		require.True(t, ok, "tag mds-%s", commit)
	}
}

// TestBackfillHistorySince pins the since bound of §4.5 step 2: the backfill starts at the oldest
// first-parent commit created at or after since, so older objects are not indexed at all.
func TestBackfillHistorySince(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	since := base.Add(24 * time.Hour)
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Backfill = indexer.BackfillHistory
		opts.Since = &since
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setCommitDate("repo", "s1", base)
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, []client.ObjectStat{object("a", 10), object("b", 20)})
	e.fake.setCommitDate("repo", "s2", since)
	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, []client.ObjectStat{object("b", 20)})
	e.fake.setCommitDate("repo", "s3", since.Add(time.Hour))
	e.fake.setDiff("repo", "s2", "s3", []client.Change{removed("a")})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 2, e.metrics.indexed["repo"], "s2 and s3, not s1")
	_, ok := e.fake.tagCommit("repo-metadata", "mds-s1")
	require.False(t, ok, "the commit before since is not indexed")
	_, ok = e.fake.tagCommit("repo-metadata", "mds-s2")
	require.True(t, ok)
	require.Equal(t, []string{"b"}, e.paths("main"))
}

// TestMaxCommitsBoundsATick pins §4.2 step 5 and the lag of §9: a branch with more commits than
// max_commits applies the oldest max_commits of them, reports the rest as lag, and the next tick
// continues exactly where this one stopped.
func TestMaxCommitsBoundsATick(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.MaxCommits = 2
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()

	paths := []string{"b", "c", "d", "e"}
	parent := "s1"
	for i, path := range paths {
		commit := fmt.Sprintf("t%d", i+2)
		e.fake.addCommit("repo", "main", commit, []string{parent}, nil)
		e.fake.setDiff("repo", parent, commit, []client.Change{added(path, int64(10*(i+2)))})
		parent = commit
	}

	e.tick()
	require.Equal(t, []string{"a", "b", "c"}, e.paths("main"), "max_commits commits, oldest first")
	health := e.health.branch("repo", "main")
	require.Equal(t, "t3", health.lastIndexed, "the position is the last applied commit")
	require.Equal(t, "t5", health.head)
	require.EqualValues(t, 2, health.lag, "the lag counts every commit that is not indexed")
	require.EqualValues(t, 2, e.metrics.lags["repo@main"])

	e.tick()
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, e.paths("main"))
	require.Zero(t, e.health.branch("repo", "main").lag)
}

// TestMergeUsesTheFirstParent pins §3.2: commits are walked along the first-parent chain, so a
// merge commit is diffed against its first parent and the objects it brings in get the merge
// commit's ID, committer and date. The second parent's commits are not indexed.
func TestMergeUsesTheFirstParent(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Backfill = indexer.BackfillHistory
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	// A side branch that is never on the first-parent chain.
	e.fake.addCommit("repo", "", "side", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "side", []client.Change{added("x", 99)})
	// The merge: first parent s2, second parent side, and it brings c in.
	merge := e.fake.addCommit("repo", "main", "m", []string{"s2", "side"}, nil)
	e.fake.setDiff("repo", "s2", "m", []client.Change{added("c", 30)})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	state, _ := e.fake.tableState("main")
	require.Equal(t, []string{"a", "b", "c"}, state.paths(), "the side commit is not on the first-parent chain")
	e.requireRow("main", "a", "s1", 10)
	e.requireRow("main", "b", "s2", 20)
	c := e.requireRow("main", "c", "m", 30)
	require.Equal(t, merge.Committer, *c.Committer, "a merge writes its own committer")
	require.Equal(t, merge.CreationDate, *c.CreationDate, "a merge writes its own creation_date")
	require.Equal(t, 1, e.fake.rebuildCalls, "one rebuild at s1; s2 and the merge are deltas")
	require.Empty(t, e.fake.statCalls)
}

// TestRecovery pins every row of §4.4's crash table by injecting the state a crash leaves behind.
func TestRecovery(t *testing.T) {
	// indexed is an environment where main is indexed at s1 and s2 is a pending commit that adds b.
	indexed := func(t *testing.T) *env {
		t.Helper()
		e := newEnv(t)
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.tick()
		e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
		e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
		return e
	}
	// indexedOnce is an environment where main is indexed at s1 and nothing else is pending.
	indexedOnce := func(t *testing.T) *env {
		t.Helper()
		e := newEnv(t)
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.tick()
		return e
	}
	// stagedAtS2 is the table state that a crashed step 3 leaves behind: s1's rows, plus b.
	stagedAtS2 := func(e *env) *fakeState {
		state, ok := e.fake.tableState("main")
		require.True(t, ok)
		staged := state.clone()
		staged.sourceCommit = "s2"
		staged.rows["b"] = table.Row{
			Repository:   "repo",
			Path:         "b",
			CommitID:     "s2",
			SizeBytes:    20,
			LastModified: time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
			ETag:         "etag-b",
			UserMetadata: map[string]string{"X-Amz-Meta-Owner": "alice"},
		}
		return staged
	}

	t.Run("crashed after the iceberg commit", func(t *testing.T) {
		e := indexed(t)
		e.fake.stage("main", stagedAtS2(e))
		e.fake.applyCalls = 0

		e.tick()

		require.Zero(t, e.fake.applyCalls, "the staged table is not written again")
		require.Empty(t, e.fake.resetCalls(), "a staged commit at the next commit is completed, not discarded")
		require.Equal(t, 2, e.metrics.indexed["repo"])
		made := e.fake.madeCommits()
		require.Equal(t, "s2", made[len(made)-1].Metadata[indexer.MetadataKeySourceCommit])
		state, _ := e.fake.tableState("main")
		require.Equal(t, "s2", state.sourceCommit)
		require.Equal(t, []string{"a", "b"}, state.paths())
		tagCommit, ok := e.fake.tagCommit("repo-metadata", "mds-s2")
		require.True(t, ok)
		head, _ := e.fake.branchHead("repo-metadata", "main")
		require.Equal(t, head, tagCommit)
		require.Zero(t, e.health.branch("repo", "main").lag)
	})

	t.Run("crashed after the iceberg commit with a foreign staged property", func(t *testing.T) {
		e := indexed(t)
		staged := stagedAtS2(e)
		staged.sourceCommit = "not-a-commit-of-this-branch"
		e.fake.stage("main", staged)

		e.tick()

		require.Equal(t, []string{"main"}, e.fake.resetCalls(), "the staging is discarded")
		state, _ := e.fake.tableState("main")
		require.Equal(t, "s2", state.sourceCommit, "and the commit is indexed normally")
		require.Equal(t, []string{"a", "b"}, state.paths())
		_, ok := e.fake.tagCommit("repo-metadata", "mds-s2")
		require.True(t, ok)
	})

	t.Run("foreign staged changes with the recorded position", func(t *testing.T) {
		e := indexed(t)
		committed, ok := e.fake.tableState("main")
		require.True(t, ok)
		staged := committed.clone()
		// The property still records the indexed position, so the old recovery could not tell
		// this foreign staged write from an empty staging.
		staged.sourceCommit = "s1"
		staged.rows["stale"] = table.Row{
			Repository: "repo",
			Path:       "stale",
			CommitID:   "s1",
			SizeBytes:  999,
			ETag:       "etag-stale",
		}
		e.fake.stage("main", staged)

		e.tick()

		require.Equal(t, []string{"main"}, e.fake.resetCalls(), "the staging is discarded despite the matching property")
		require.Equal(t, []string{"a", "b"}, e.paths("main"))
		require.NotContains(t, e.rows("main"), "stale")
	})

	t.Run("crashed after the lakefs commit", func(t *testing.T) {
		e := indexedOnce(t)
		delete(e.fake.tags["repo-metadata"], "mds-s1")
		before := len(e.fake.madeCommits())

		e.tick()

		require.Len(t, e.fake.madeCommits(), before, "the lakeFS commit exists, so only the tag is missing")
		head, _ := e.fake.branchHead("repo-metadata", "main")
		tagCommit, ok := e.fake.tagCommit("repo-metadata", "mds-s1")
		require.True(t, ok, "the tag is repaired")
		require.Equal(t, head, tagCommit)
	})

	t.Run("crashed after the lakefs commit with a mispointed tag", func(t *testing.T) {
		e := indexedOnce(t)
		initCommit := e.fake.initCommitID()
		require.NotEmpty(t, initCommit)
		e.fake.tags["repo-metadata"]["mds-s1"] = initCommit

		e.tick()

		head, _ := e.fake.branchHead("repo-metadata", "main")
		tagCommit, ok := e.fake.tagCommit("repo-metadata", "mds-s1")
		require.True(t, ok)
		require.Equal(t, head, tagCommit, "the tag is forced onto the recorded position")
	})

	t.Run("crashed during the iceberg commit", func(t *testing.T) {
		e := indexed(t)
		// Data files written, no staged pointer: the next tick finds a clean branch and indexes
		// the pending commit normally. The orphan files are lakectl iceberg cleanup's problem.
		e.tick()

		require.Empty(t, e.fake.resetCalls())
		state, _ := e.fake.tableState("main")
		require.Equal(t, []string{"a", "b"}, state.paths())
		require.Equal(t, 2, e.metrics.indexed["repo"])
	})

	t.Run("a concurrent writer moves the table", func(t *testing.T) {
		e := indexed(t)
		e.fake.applyErr = fmt.Errorf("another writer: %w", indexer.ErrConflict)

		e.tick()

		require.NoError(t, e.health.branch("repo", "main").err, "the branch tick restarts instead of failing")
		state, _ := e.fake.tableState("main")
		require.Equal(t, "s2", state.sourceCommit)
		require.Equal(t, []string{"a", "b"}, state.paths())
		require.Equal(t, 2, e.metrics.indexed["repo"])
	})

	t.Run("two consecutive conflicts", func(t *testing.T) {
		e := indexed(t)
		conflict := fmt.Errorf("another writer: %w", indexer.ErrConflict)
		e.fake.applyErrors = []error{conflict, conflict}

		e.tick()

		require.NoError(t, e.health.branch("repo", "main").err, "three retries absorb two consecutive 409s")
		require.Equal(t, 3, e.fake.applyCalls)
		state, _ := e.fake.tableState("main")
		require.Equal(t, "s2", state.sourceCommit)
		require.Equal(t, []string{"a", "b"}, state.paths())
	})
}

// TestHardResetRewriteWithTag pins §4.7 with a tag at the merge base: the metadata branch is hard
// reset to mds-<merge base>, so the table goes back to the rows of that commit.
func TestHardResetRewriteWithTag(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Backfill = indexer.BackfillHistory
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
	e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
	e.tick()
	require.Equal(t, []string{"a", "b", "c"}, e.paths("main"))

	// A hard reset of the data branch from s3 back to s2.
	e.fake.setBranchHead("repo", "main", "s2")

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: "mds-s2"})
	state, _ := e.fake.tableState("main")
	require.Equal(t, "s2", state.sourceCommit)
	require.Equal(t, []string{"a", "b"}, state.paths(), "the rows of s2")
	health := e.health.branch("repo", "main")
	require.Equal(t, "s2", health.lastIndexed)
	require.Equal(t, "s2", health.head)
	require.Zero(t, health.lag)
}

// TestHardResetRewriteWithoutTag pins the other half of §4.7: when the merge base has no tag, the
// branch is reset to the initialisation commit and rebuilt from a full listing at the head.
func TestHardResetRewriteWithoutTag(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Backfill = indexer.BackfillHistory
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, []client.ObjectStat{object("a", 10), object("b", 20)})
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, []client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
	e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
	e.tick()

	initCommit := e.fake.initCommitID()
	require.NotEmpty(t, initCommit)
	delete(e.fake.tags["repo-metadata"], "mds-s2")
	e.fake.setBranchHead("repo", "main", "s2")
	rebuilds := e.fake.rebuildCalls

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: initCommit})
	require.Equal(t, rebuilds+1, e.fake.rebuildCalls, "the branch is rebuilt from a full listing")
	state, _ := e.fake.tableState("main")
	require.Equal(t, "s2", state.sourceCommit)
	require.Equal(t, []string{"a", "b"}, state.paths())
	_, ok := e.fake.tagCommit("repo-metadata", "mds-s2")
	require.True(t, ok, "the rebuilt position is tagged again")
}

// TestRewriteWithStagedChanges pins the §4.4 rule that a rewrite discards staging first. Real
// lakeFS ResetHard keeps staging (pkg/graveler/graveler.go ResetHard has a TODO and only checks the
// sealed tokens), so the old order - hard reset, then recovery - left a crashed step 3 pointer in
// place and ran the rewrite on top of it. Both rewrite paths are covered: the tag of the merge base
// and the initialisation-commit fallback.
func TestRewriteWithStagedChanges(t *testing.T) {
	setup := func(t *testing.T, withTag bool) *env {
		t.Helper()
		e := newEnv(t, func(opts *indexer.Options) {
			opts.Backfill = indexer.BackfillHistory
		})
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.fake.addCommit("repo", "main", "s2", []string{"s1"}, []client.ObjectStat{object("a", 10), object("b", 20)})
		e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
		e.fake.addCommit("repo", "main", "s3", []string{"s2"}, []client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
		e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
		e.tick()
		require.Equal(t, []string{"a", "b", "c"}, e.paths("main"))

		committed, ok := e.fake.tableState("main")
		require.True(t, ok)
		staged := committed.clone()
		// The staged pointer records the commit the rewrite is about to move to, so recovery
		// cannot tell it apart from an empty staging by the property alone.
		staged.sourceCommit = "s2"
		staged.rows["stale"] = table.Row{
			Repository: "repo",
			Path:       "stale",
			CommitID:   "s3",
			SizeBytes:  999,
			ETag:       "etag-stale",
		}
		e.fake.stage("main", staged)
		if !withTag {
			delete(e.fake.tags["repo-metadata"], "mds-s2")
		}
		e.fake.setBranchHead("repo", "main", "s2")
		return e
	}
	requireResetBeforeHardReset := func(t *testing.T, e *env) {
		t.Helper()
		reset, hardReset := -1, -1
		for i, event := range e.fake.events {
			if event == "reset:main" && reset == -1 {
				reset = i
			}
			if len(event) >= len("hard-reset:main") && event[:len("hard-reset:main")] == "hard-reset:main" && hardReset == -1 {
				hardReset = i
			}
		}
		require.NotEqual(t, -1, reset, "the rewrite discards staging")
		require.NotEqual(t, -1, hardReset, "the rewrite moves the branch")
		require.Less(t, reset, hardReset, "staging is discarded before the history rewrite")
	}

	t.Run("tagged merge base", func(t *testing.T) {
		e := setup(t, true)

		e.tick()

		require.NoError(t, e.health.repositoryError("repo"))
		requireResetBeforeHardReset(t, e)
		require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: "mds-s2"})
		state, _ := e.fake.tableState("main")
		require.Equal(t, "s2", state.sourceCommit)
		require.Equal(t, []string{"a", "b"}, state.paths())
		require.NotContains(t, e.rows("main"), "stale")
	})

	t.Run("fallback without a tag", func(t *testing.T) {
		e := setup(t, false)
		initCommit := e.fake.initCommitID()
		require.NotEmpty(t, initCommit)

		e.tick()

		require.NoError(t, e.health.repositoryError("repo"))
		requireResetBeforeHardReset(t, e)
		require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: initCommit})
		state, _ := e.fake.tableState("main")
		require.Equal(t, "s2", state.sourceCommit)
		require.Equal(t, []string{"a", "b"}, state.paths())
		require.NotContains(t, e.rows("main"), "stale")
	})
}

// TestUnrelatedHistoriesRewrite pins §4.7 when R@b is force-moved to a commit with no common
// ancestor. graveler answers ErrNoMergeBase and pkg/api returns a 500; the client types it and the
// indexer takes the fallback instead of failing the branch on every tick.
func TestUnrelatedHistoriesRewrite(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Backfill = indexer.BackfillHistory
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()

	e.fake.addCommit("repo", "", "other", nil, []client.ObjectStat{object("z", 99)})
	e.fake.setBranchHead("repo", "main", "other")
	initCommit := e.fake.initCommitID()
	require.NotEmpty(t, initCommit)

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.NoError(t, e.health.branch("repo", "main").err)
	require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: initCommit})
	state, _ := e.fake.tableState("main")
	require.Equal(t, "other", state.sourceCommit)
	require.Equal(t, []string{"z"}, state.paths())
	e.requireRow("main", "z", "other", 99)
	_, ok := e.fake.tagCommit("repo-metadata", "mds-other")
	require.True(t, ok, "the rebuilt position is tagged")
}

// TestPositionReachableOnlyThroughSecondParent pins §4.7 when last is an ancestor of head only
// through a merge's second parent. findMergeBase(last, head) answers last, so the ancestry check
// passes, but pending walks the first-parent chain and never meets it. Continuing from last used to
// replay the whole chain from the root on top of its table and keep foreign rows; the fix detects
// the absent stop commit and rebuilds from a full listing at head.
func TestPositionReachableOnlyThroughSecondParent(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Backfill = indexer.BackfillHistory
		opts.Repositories[0].Branches = []string{"main", "side"}
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	// The side commit is reachable from the merge only through its second parent.
	e.fake.addCommit("repo", "side", "side1", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "side1", []client.Change{added("x", 99)})
	e.fake.addCommit("repo", "main", "m", []string{"s2", "side1"}, []client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
	e.fake.setDiff("repo", "s2", "m", []client.Change{added("c", 30)})
	e.tick()
	require.Equal(t, []string{"a", "b", "c"}, e.paths("main"))

	// Record the side commit as the position of main, as a previously indexed branch that was
	// then force-moved onto the merge would have.
	e.fake.branches["repo-metadata"]["main"] = e.fake.branches["repo-metadata"]["side"]
	initCommit := e.fake.initCommitID()
	require.NotEmpty(t, initCommit)
	rebuilds := e.fake.rebuildCalls

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.NoError(t, e.health.branch("repo", "main").err)
	require.Equal(t, rebuilds+1, e.fake.rebuildCalls, "the unfollowable position forces a full listing at head")
	require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: initCommit})
	state, _ := e.fake.tableState("main")
	require.Equal(t, "m", state.sourceCommit)
	require.Equal(t, []string{"a", "b", "c"}, state.paths(), "the side row x is not on main's first-parent chain")
	e.requireRow("main", "c", "m", 30)
}

// TestBranchDelete pins §4.8: deleting a matching branch deletes its metadata branch (its tags stay),
// and delete_branches: false leaves it alone.
func TestBranchDelete(t *testing.T) {
	setup := func(t *testing.T, deleteBranches bool) *env {
		t.Helper()
		e := newEnv(t, func(opts *indexer.Options) {
			opts.Repositories[0].Branches = []string{"main", "feature"}
			opts.DeleteBranches = deleteBranches
		})
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.fake.setBranchHead("repo", "feature", "s1")
		e.tick()
		require.Len(t, e.fake.createdBranches, 1, "feature is seeded from mds-s1")
		return e
	}

	t.Run("delete_branches: true", func(t *testing.T) {
		e := setup(t, true)
		e.fake.deleteBranch("repo", "feature")

		e.tick()

		require.Equal(t, []string{"feature"}, e.fake.deletedBranches)
		_, ok := e.fake.branchHead("repo-metadata", "feature")
		require.False(t, ok)
		_, ok = e.fake.tagCommit("repo-metadata", "mds-s1")
		require.True(t, ok, "its tags stay, so the history is still addressable (§4.8)")
		require.Contains(t, e.metrics.cleared, "repo@feature")
	})

	t.Run("delete_branches: false", func(t *testing.T) {
		e := setup(t, false)
		e.fake.deleteBranch("repo", "feature")

		e.tick()

		require.Empty(t, e.fake.deletedBranches)
		_, ok := e.fake.branchHead("repo-metadata", "feature")
		require.True(t, ok, "the metadata branch is left as it is")
	})
}

// TestBranchStopsMatchingKeepsItsTable pins §4.8's "branch stops matching the config" row: nothing
// happens - the mirror is neither advanced nor deleted.
func TestBranchStopsMatchingKeepsItsTable(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setBranchHead("repo", "feature", "s1")
	e.tick()

	// A metadata branch that the configuration no longer matches, created through the same client
	// the indexer uses.
	initCommit := e.fake.initCommitID()
	require.NoError(t, e.fake.CreateBranch(t.Context(), "repo-metadata", "feature", initCommit))
	// R@feature moves on while the selector says "main" only.
	e.fake.addCommit("repo", "feature", "s2", []string{"s1"}, []client.ObjectStat{object("a", 10), object("b", 20)})
	before, _ := e.fake.branchHead("repo-metadata", "feature")

	e.tick()

	after, ok := e.fake.branchHead("repo-metadata", "feature")
	require.True(t, ok)
	require.Equal(t, before, after, "an unmatched branch is left where it is")
	require.Empty(t, e.fake.deletedBranches, "and it is not deleted: it still exists in R")
}

// loadHookStore wraps the fake's table store and runs a hook around selected LoadTable calls, so a
// test can act "between" two steps of one tick.
type loadHookStore struct {
	indexer.TableStore
	calls  int
	onLoad func(int)
}

func (s *loadHookStore) LoadTable(ctx context.Context, repository, branch string) (indexer.Table, error) {
	s.calls++
	if s.onLoad != nil {
		s.onLoad(s.calls)
	}
	return s.TableStore.LoadTable(ctx, repository, branch)
}

// TestReloadTableBeforeApplyingCommit pins §4.4's "before step 3 the indexer reloads the table and
// skips to step 4 if the source commit already equals C". The handle loaded at the top of the tick
// is stale if another writer stages C before step 3; without the reload the indexer applies the
// delta again instead of only committing the staged table.
func TestReloadTableBeforeApplyingCommit(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})

	store := &loadHookStore{TableStore: e.fake}
	store.onLoad = func(call int) {
		if call != 2 {
			return
		}
		// Another writer stages s2 after the tick's first LoadTable and before step 3.
		state, ok := e.fake.tableState("main")
		require.True(t, ok)
		staged := state.clone()
		staged.sourceCommit = "s2"
		staged.rows["b"] = table.Row{
			Repository:   "repo",
			Path:         "b",
			CommitID:     "s2",
			SizeBytes:    20,
			LastModified: time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
			ETag:         "etag-b",
			UserMetadata: map[string]string{"X-Amz-Meta-Owner": "alice"},
		}
		e.fake.stage("main", staged)
	}
	idx, err := indexer.New(indexer.Options{
		LakeFS:       e.fake,
		Tables:       store,
		Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
		Health:       e.health,
		Metrics:      e.metrics,
	})
	require.NoError(t, err)

	require.NoError(t, idx.Tick(t.Context()))

	require.Zero(t, e.fake.applyCalls, "the reloaded table already records s2, so step 3 is skipped")
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, "s2", state.sourceCommit)
	require.Equal(t, []string{"a", "b"}, state.paths())
}
