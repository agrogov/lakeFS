package indexer_test

// branch_test.go covers the per-branch state machine of design §4.2 and everything it recovers
// from: the crash table of §4.4, the seeding and full listing of §4.5, the first-parent merge rule
// of §3.2, the history rewrite of §4.7 and the branch lifecycle of §4.8. Every case drives the real
// code through one or more ticks of indexer.Tick against the fakes of fake_test.go.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
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

// TestPendingPagesTheLog pins the paged pending walk of §4.2 step 5, which no other case exercises:
// the fake answered every log read in one page until now (a real lakeFS pages at page_size), so the
// cursor handling, the "met the position" check across pages and the inclusive stop_at rule were
// unverified (IX-5). The window a page walk keeps is asserted through the requests it makes and the
// commits it reads: the todo is the oldest commits after the position, so a bounded walk reads
// towards the position and never past it.
func TestPendingPagesTheLog(t *testing.T) {
	t.Run("every commit of the backlog is applied", func(t *testing.T) {
		e := newEnv(t)
		e.fake.pageSize = 2
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.tick()
		calls, read := e.fake.logCommitsCalls("repo"), e.fake.logCommitsRead("repo")

		// Five pending commits on top of s1, one page of two at a time.
		parent := "s1"
		for i := 2; i <= 6; i++ {
			id, path := fmt.Sprintf("s%d", i), fmt.Sprintf("p%d", i)
			e.fake.addCommit("repo", "main", id, []string{parent}, nil)
			e.fake.setDiff("repo", parent, id, []client.Change{added(path, int64(10*i))})
			parent = id
		}

		e.tick()

		require.NoError(t, e.health.branch("repo", "main").err)
		require.Equal(t, 3, e.fake.logCommitsCalls("repo")-calls,
			"five pending commits plus the stop commit are three pages of two")
		require.Equal(t, 6, e.fake.logCommitsRead("repo")-read, "no page is read twice")
		require.Equal(t, []string{"a", "p2", "p3", "p4", "p5", "p6"}, e.paths("main"))
		health := e.health.branch("repo", "main")
		require.Equal(t, "s6", health.lastIndexed)
		require.Zero(t, health.lag, "the walk met the position, so the lag is exact and 0")
	})

	t.Run("max_commits applies the oldest pending commit", func(t *testing.T) {
		e := newEnv(t, func(opts *indexer.Options) {
			opts.MaxCommits = 1
		})
		e.fake.pageSize = 2
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.tick()
		e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
		e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
		e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
		e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
		calls := e.fake.logCommitsCalls("repo")

		e.tick()

		require.NoError(t, e.health.branch("repo", "main").err)
		require.Equal(t, 2, e.fake.logCommitsCalls("repo")-calls,
			"the position is on the second page, and the walk stops there")
		require.Equal(t, []string{"a", "b"}, e.paths("main"), "the oldest pending commit, not the newest")
		health := e.health.branch("repo", "main")
		require.Equal(t, "s2", health.lastIndexed)
		require.EqualValues(t, 1, health.lag)

		e.tick()
		require.Equal(t, []string{"a", "b", "c"}, e.paths("main"))
		require.Zero(t, e.health.branch("repo", "main").lag)
	})

	t.Run("a position off the first-parent chain is found across pages", func(t *testing.T) {
		e := newEnv(t, func(opts *indexer.Options) {
			opts.Backfill = indexer.BackfillHistory
			opts.Repositories[0].Branches = []string{"main", "side"}
		})
		e.fake.pageSize = 1
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
		e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
		// side1 is reachable from the merge only through its second parent.
		e.fake.addCommit("repo", "side", "side1", []string{"s1"}, nil)
		e.fake.setDiff("repo", "s1", "side1", []client.Change{added("x", 99)})
		e.fake.addCommit("repo", "main", "m", []string{"s2", "side1"},
			[]client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
		e.fake.setDiff("repo", "s2", "m", []client.Change{added("c", 30)})
		e.tick()
		require.Equal(t, []string{"a", "b", "c"}, e.paths("main"))

		// Record the side commit as main's position, as an indexed branch that was then
		// force-moved onto the merge would have.
		e.fake.branches["repo-metadata"]["main"] = e.fake.branches["repo-metadata"]["side"]
		initCommit := e.fake.initCommitID()
		require.NotEmpty(t, initCommit)
		calls := e.fake.logCommitsCalls("repo")

		e.tick()

		require.NoError(t, e.health.branch("repo", "main").err)
		require.Equal(t, 3, e.fake.logCommitsCalls("repo")-calls,
			"one page per commit: m, s2, and s1 where the walk runs out of first-parent history")
		require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: initCommit},
			"the position is not on the chain across pages either, so the branch is rebuilt at head")
		require.Equal(t, []string{"a", "b", "c"}, e.paths("main"))
	})
}

// TestFarBehindBranchIsRebuiltInsteadOfWalked pins the bound §4.2 step 5 puts on one tick: the
// commits to apply are the oldest ones after the recorded position, so catching up used to read the
// whole remaining log on every tick, hold it in memory and do it again next tick (O(lag) memory and
// O(lag/page) requests for max_commits applied commits). A tick now reads at most max_commits plus
// one page of the log; a position deeper than that is not continued from but rebuilt at the head
// from a full listing (§4.7), which costs the listing once instead of the backlog every tick.
func TestFarBehindBranchIsRebuiltInsteadOfWalked(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.MaxCommits = 1
	})
	e.fake.pageSize = 2
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	require.Equal(t, 1, e.fake.rebuildCalls)
	initCommit := e.fake.initCommitID()
	require.NotEmpty(t, initCommit)

	// 40 pending commits: far deeper than the commits one tick may read.
	objects := []client.ObjectStat{object("a", 10)}
	parent := "s1"
	for i := 2; i <= 41; i++ {
		id, path := fmt.Sprintf("s%02d", i), fmt.Sprintf("p%02d", i)
		e.fake.addCommit("repo", "main", id, []string{parent}, nil)
		e.fake.setDiff("repo", parent, id, []client.Change{added(path, int64(i))})
		objects = append(objects, object(path, int64(i)))
		parent = id
	}
	e.fake.setObjects("repo", parent, objects)
	calls, read := e.fake.logCommitsCalls("repo"), e.fake.logCommitsRead("repo")

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.NoError(t, e.health.branch("repo", "main").err)
	require.Equal(t, 2, e.fake.logCommitsCalls("repo")-calls,
		"one page more than max_commits: two requests, not the 21 a walk to the position costs")
	require.Equal(t, 4, e.fake.logCommitsRead("repo")-read,
		"and four commits read in total, so a tick cannot retain more than one page of them")

	require.Equal(t, 2, e.fake.rebuildCalls, "the branch it cannot walk to is rebuilt at the head")
	require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: initCommit})
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, "s41", state.sourceCommit, "the position is the head that was just rebuilt")
	require.Len(t, state.rows, len(objects))
	health := e.health.branch("repo", "main")
	require.Equal(t, "s41", health.lastIndexed)
	require.Zero(t, health.lag, "the lag of a rebuilt branch is 0, exact, and not a lower bound")
	_, ok = e.fake.tagCommit("repo-metadata", "mds-s41")
	require.True(t, ok, "the rebuilt position is tagged like any other (§4.3 step 5)")

	// A second tick has nothing left behind it: it reads no log for R and writes no commit.
	calls, read = e.fake.logCommitsCalls("repo"), e.fake.logCommitsRead("repo")
	made := len(e.fake.madeCommits())
	e.tick()
	require.Zero(t, e.fake.logCommitsCalls("repo")-calls)
	require.Zero(t, e.fake.logCommitsRead("repo")-read)
	require.Len(t, e.fake.madeCommits(), made)
	require.Equal(t, 2, e.fake.rebuildCalls)
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
	for _, request := range e.fake.logRequests {
		require.True(t, request.opts.FirstParent,
			"§3.2 walks the first-parent chain, so every log read asks for it (repository %q)", request.repository)
	}
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
		// The transaction wrote data files and never staged a pointer: the next tick finds a clean
		// branch and indexes the pending commit normally. The orphan files are unreferenced and
		// are lakectl iceberg cleanup's problem (§4.4 row 5, §9); the fake has no object store to
		// leave them in, so what is pinned here is that nothing staged is neither replayed nor reset.
		e.tick()

		require.Empty(t, e.fake.resetCalls())
		state, _ := e.fake.tableState("main")
		require.Equal(t, []string{"a", "b"}, state.paths())
		require.Equal(t, 2, e.metrics.indexed["repo"])
	})

	t.Run("crashed during the iceberg commit with a partial staging", func(t *testing.T) {
		e := indexed(t)
		// The crash left a partial write behind: the transaction staged rows but never moved the
		// table pointer, so the staged lakefs.mds.source-commit is empty and cannot describe s2.
		// An indexer that treats any staging as "already applied" would commit the partial state
		// (and record no position); the correct recovery discards it and applies s2 as a delta.
		committed, ok := e.fake.tableState("main")
		require.True(t, ok)
		partial := committed.clone()
		partial.sourceCommit = ""
		partial.rows["b"] = table.Row{
			Repository:   "repo",
			Path:         "b",
			CommitID:     "s2",
			SizeBytes:    999, // the partial value: the delta must replace it
			LastModified: time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
			ETag:         "etag-partial",
		}
		e.fake.stage("main", partial)
		e.fake.applyCalls = 0

		e.tick()

		require.Equal(t, []string{"main"}, e.fake.resetCalls(), "the partial staging is discarded")
		require.Equal(t, 1, e.fake.applyCalls, "and s2 is applied as a normal delta")
		state, ok := e.fake.tableState("main")
		require.True(t, ok)
		require.Equal(t, "s2", state.sourceCommit)
		e.requireRow("main", "b", "s2", 20)
		require.Equal(t, []string{"a", "b"}, state.paths())
		_, ok = e.fake.tagCommit("repo-metadata", "mds-s2")
		require.True(t, ok, "the recovered commit is tagged")
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

// TestCollectedPositionIsRebuilt pins §4.7 for a recorded position that no longer exists in R: a
// force-moved branch whose old commits committed GC collected while the indexer was down, or R
// deleted and created again under the same name (§4.8). Nothing keeps the source commit of a
// position alive in R, so findMergeBase dereferences it and answers 404. Returning that error failed
// the branch on every tick forever: /_health stayed 503 and the lag never cleared. The 404 is a
// position that is gone, so the tick takes the full-rebuild fallback §4.7 already has.
func TestCollectedPositionIsRebuilt(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, []client.ObjectStat{object("a", 10), object("b", 20)})
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.tick()
	require.Equal(t, []string{"a", "b"}, e.paths("main"))
	require.Equal(t, "s2", e.health.branch("repo", "main").lastIndexed)

	// Committed GC collects s2 (no branch refers to it any more) and R@main is re-created on an
	// unrelated root, which is what deleting and re-creating R looks like to the indexer.
	e.fake.deleteCommit("repo", "s2")
	e.fake.addCommit("repo", "", "fresh", nil, []client.ObjectStat{object("z", 99)})
	e.fake.setBranchHead("repo", "main", "fresh")
	initCommit := e.fake.initCommitID()
	require.NotEmpty(t, initCommit)
	rebuilds := e.fake.rebuildCalls

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.NoError(t, e.health.branch("repo", "main").err, "a collected position is a rewrite, not a failure")
	require.Equal(t, rebuilds+1, e.fake.rebuildCalls, "the branch is rebuilt from a full listing at the head")
	require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: initCommit})
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, "fresh", state.sourceCommit)
	require.Equal(t, []string{"z"}, state.paths(), "the rows of the re-created repository, not the old ones")
	_, ok = e.fake.tagCommit("repo-metadata", "mds-fresh")
	require.True(t, ok, "the rebuilt position is tagged")
	health := e.health.branch("repo", "main")
	require.Equal(t, "fresh", health.lastIndexed)
	require.Zero(t, health.lag)

	// The next tick has nothing to do, and above all no error of the same kind.
	made := len(e.fake.madeCommits())
	e.tick()
	require.NoError(t, e.health.repositoryError("repo"))
	require.NoError(t, e.health.branch("repo", "main").err)
	require.Len(t, e.fake.madeCommits(), made)
}

// TestCollectedPositionDuringTheWalkIsRebuilt pins the same rule for the stop_at dereference of the
// pending walk. The position can be collected between §4.2 step 4 and step 5, and pkg/catalog
// dereferences stop_at before it walks, so the log answers 404 there too (the fake models that).
func TestCollectedPositionDuringTheWalkIsRebuilt(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.fake.addCommit("repo", "main", "s3", []string{"s2"},
		[]client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
	e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
	initCommit := e.fake.initCommitID()
	require.NotEmpty(t, initCommit)
	rebuilds := e.fake.rebuildCalls

	// The recorded position is collected after the merge-base answer and before the pending walk.
	e.fake.afterFindMergeBase = func() { e.fake.deleteCommit("repo", "s1") }

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.NoError(t, e.health.branch("repo", "main").err)
	require.Equal(t, rebuilds+1, e.fake.rebuildCalls)
	require.Contains(t, e.fake.hardResets, hardReset{repository: "repo-metadata", branch: "main", ref: initCommit})
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, "s3", state.sourceCommit)
	require.Equal(t, []string{"a", "b", "c"}, state.paths())
	_, ok = e.fake.tagCommit("repo-metadata", "mds-s3")
	require.True(t, ok)
}

// TestNotFoundThatIsNotThePositionIsAFailure pins the other half of the rule: only a 404 about the
// recorded position means it is gone. A 404 about anything else - the head, most of all, which the
// branch listing promised - is a transient server answer and must fail this tick, not reset the
// branch to the initialisation commit and rebuild it.
func TestNotFoundThatIsNotThePositionIsAFailure(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, []client.ObjectStat{object("a", 10), object("b", 20)})
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	rebuilds := e.fake.rebuildCalls
	e.fake.findMergeBaseErr = &client.NotFoundError{Operation: "find merge base", Subject: "s2"}

	e.tick()

	err := e.health.branch("repo", "main").err
	require.Error(t, err, "a 404 that is not about the recorded position is still an error")
	require.ErrorIs(t, err, client.ErrNotFound, "and it is not swallowed")
	require.Empty(t, e.fake.hardResets, "the recorded position exists, so nothing is rebuilt")
	require.Equal(t, rebuilds, e.fake.rebuildCalls)
	require.Equal(t, []string{"a"}, e.paths("main"), "the table is left at the recorded position")

	// The next tick, with the transient answer gone, catches up normally.
	e.tick()
	require.NoError(t, e.health.branch("repo", "main").err)
	require.Equal(t, []string{"a", "b"}, e.paths("main"))
	require.Equal(t, "s2", e.health.branch("repo", "main").lastIndexed)
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
		require.False(t, e.health.hasBranch("repo", "feature"), "a deleted branch leaves /_health")
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

// TestMetadataDefaultBranchIsNeverRetired pins the §4.8 branch-deletion mirror against R-metadata's
// own default branch. R may rename its default branch and delete the old name; R-metadata keeps the
// old name as its default branch, and lakeFS refuses to delete a default branch
// (pkg/graveler/graveler.go DeleteBranch returns ErrDeleteDefaultBranch). Trying anyway failed the
// repository tick every time and stopped tag retention after it.
func TestMetadataDefaultBranchIsNeverRetired(t *testing.T) {
	now := time.Now().UTC()
	logger := &recordingLogger{}
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Repositories[0].Branches = []string{"*"}
		opts.TagRetention = time.Hour
		opts.DeleteBranches = true
		opts.Log = logger
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setCommitDate("repo", "s1", now.Add(-72*time.Hour))
	e.tick()

	// A tag that retention may retire, to prove it still runs after the default-branch skip.
	e.fake.addCommit("repo", "", "old-source", nil, nil)
	e.fake.setCommitDate("repo", "old-source", now.Add(-72*time.Hour))
	e.fake.setCommit("repo-metadata", client.Commit{
		ID: "old-index",
		Metadata: map[string]string{
			indexer.MetadataKeySourceCommit:     "old-source",
			indexer.MetadataKeySourceRepository: "repo",
		},
	})
	e.fake.addTag("repo-metadata", "mds-old-source", "old-index")

	// R renames its default branch to trunk and deletes main. R-metadata keeps main as its own
	// default branch.
	e.fake.branches["repo"]["trunk"] = "s1"
	e.fake.repositories["repo"] = client.Repository{
		Name:             "repo",
		StorageNamespace: "s3://bucket/data",
		DefaultBranch:    "trunk",
		StorageID:        "storage-repo",
	}
	delete(e.fake.branches["repo"], "main")

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	_, ok := e.fake.branchHead("repo-metadata", "main")
	require.True(t, ok, "R-metadata's default branch is never deleted")
	require.NotContains(t, e.fake.deletedBranches, "main")
	require.Contains(t, e.fake.deletedTags, "mds-old-source", "tag retention still runs after the skip")
	require.Equal(t, 1, logger.countInfos("keeping the metadata default branch"), "the skip is logged once")

	// A later tick takes the same path but does not log it again.
	e.tick()
	require.Equal(t, 1, logger.countInfos("keeping the metadata default branch"), "the skip is logged once per process")
}

// TestBranchStopsMatchingDropsItsHealth pins the other half of §4.8's "branch stops matching" row:
// the table is left as it is, but /_health must not keep reporting a branch the indexer no longer
// follows.
func TestBranchStopsMatchingDropsItsHealth(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Repositories[0].Branches = []string{"main", "feature"}
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setBranchHead("repo", "feature", "s1")
	e.tick()
	require.True(t, e.health.hasBranch("repo", "feature"))

	idx, err := indexer.New(indexer.Options{
		LakeFS:       e.fake,
		Tables:       e.fake,
		Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
		Health:       e.health,
		Metrics:      e.metrics,
	})
	require.NoError(t, err)
	require.NoError(t, idx.Tick(t.Context()))

	require.False(t, e.health.hasBranch("repo", "feature"), "a branch that stops matching leaves /_health")
	_, ok := e.fake.branchHead("repo-metadata", "feature")
	require.True(t, ok, "its table is left as it is (§4.8)")
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

// reloadHookStore wraps the fake's table store and runs a hook before the first reload of the
// handle it serves, so a test can act "between" two steps of one tick.
type reloadHookStore struct {
	indexer.TableStore
	onReload func()
}

func (s *reloadHookStore) LoadTable(ctx context.Context, repository, branch string) (indexer.Table, error) {
	tbl, err := s.TableStore.LoadTable(ctx, repository, branch)
	if err != nil {
		return nil, err
	}
	return &reloadHookTable{Table: tbl, store: s}, nil
}

// reloadHookTable runs the store's hook before the table re-reads itself. The reload goes through
// the handle (§4.4, Table.Refresh), not through another LoadTable, so the hook belongs here.
type reloadHookTable struct {
	indexer.Table
	store *reloadHookStore
}

func (t *reloadHookTable) Refresh(ctx context.Context) error {
	if hook := t.store.onReload; hook != nil {
		t.store.onReload = nil
		hook()
	}
	return t.Table.Refresh(ctx)
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

	store := &reloadHookStore{TableStore: e.fake}
	store.onReload = func() {
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

// TestBackfillHeadSince pins §4.5 step 2's since bound with the default backfill: head. since wins
// over backfill: head - the branch starts at the oldest first-parent commit created at or after
// since, so objects that only exist before it are never indexed.
func TestBackfillHeadSince(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	since := base.Add(24 * time.Hour)
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Since = &since
		// Backfill is left at its default, head.
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setCommitDate("repo", "s1", base)
	e.fake.addCommit("repo", "main", "s2", []string{"s1"},
		[]client.ObjectStat{object("a", 10), object("b", 20)})
	e.fake.setCommitDate("repo", "s2", base.Add(48*time.Hour))
	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, []client.ObjectStat{object("b", 20)})
	e.fake.setCommitDate("repo", "s3", since.Add(time.Hour))
	e.fake.setDiff("repo", "s2", "s3", []client.Change{removed("a")})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 1, e.fake.rebuildCalls, "one full listing, at the oldest commit inside the since window")
	require.Equal(t, 1, e.fake.applyCalls, "the commits after it are deltas")
	require.Equal(t, 2, e.metrics.indexed["repo"], "s2 and s3, not the commit before since")
	_, ok := e.fake.tagCommit("repo-metadata", "mds-s1")
	require.False(t, ok, "the commit before since is not indexed")
	require.Equal(t, []string{"b"}, e.paths("main"))
	e.requireRow("main", "b", "s2", 20)
}

// TestSeedLookbackBoundsTheSeedingWalk pins §4.5 step 1: the first indexing of a branch looks for
// an indexed ancestor only within seed_lookback commits of its head. A tag further back is not used;
// the branch is written from a full listing of its head instead.
func TestSeedLookbackBoundsTheSeedingWalk(t *testing.T) {
	// seededChain leaves main indexed at s1 - the only commit of the history with an mds- tag - and
	// R@feature at s4, three commits behind that tag.
	seededChain := func(t *testing.T) *env {
		t.Helper()
		e := newEnv(t, func(opts *indexer.Options) {
			opts.Repositories[0].Branches = []string{"main"}
		})
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.tick()
		require.Equal(t, 1, e.fake.rebuildCalls)

		e.fake.addCommit("repo", "main", "s2", []string{"s1"},
			[]client.ObjectStat{object("a", 10), object("b", 20)})
		e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
		e.fake.addCommit("repo", "main", "s3", []string{"s2"},
			[]client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
		e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
		e.fake.addCommit("repo", "main", "s4", []string{"s3"},
			[]client.ObjectStat{object("a", 10), object("b", 20), object("c", 30), object("d", 40)})
		e.fake.setDiff("repo", "s3", "s4", []client.Change{added("d", 40)})

		e.fake.setBranchHead("repo", "feature", "s4")
		return e
	}
	// indexFeature runs one tick that follows only R@feature, with the given seed_lookback.
	indexFeature := func(t *testing.T, e *env, lookback int) {
		t.Helper()
		idx, err := indexer.New(indexer.Options{
			LakeFS:       e.fake,
			Tables:       e.fake,
			Health:       e.health,
			Metrics:      e.metrics,
			Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"feature"}}},
			SeedLookback: lookback,
		})
		require.NoError(t, err)
		require.NoError(t, idx.Tick(t.Context()))
	}

	t.Run("within the bound", func(t *testing.T) {
		e := seededChain(t)
		indexFeature(t, e, 4)

		require.NoError(t, e.health.repositoryError("repo"))
		require.Len(t, e.fake.createdBranches, 1)
		require.Equal(t, createdBranch{repository: "repo-metadata", branch: "feature", source: "mds-s1"},
			e.fake.createdBranches[0], "the tag inside seed_lookback seeds the branch")
		require.Equal(t, 1, e.fake.rebuildCalls, "a seeded branch is not rebuilt")
		require.Equal(t, 3, e.fake.applyCalls, "s2, s3 and s4 are applied as deltas")
		e.requireRow("feature", "a", "s1", 10)
		e.requireRow("feature", "d", "s4", 40)
	})

	t.Run("beyond the bound", func(t *testing.T) {
		e := seededChain(t)
		indexFeature(t, e, 3)

		require.NoError(t, e.health.repositoryError("repo"))
		require.Len(t, e.fake.createdBranches, 1)
		require.Equal(t, e.fake.initCommitID(), e.fake.createdBranches[0].source,
			"a tag beyond seed_lookback is not used")
		require.Equal(t, 2, e.fake.rebuildCalls, "the branch is written from a full listing of its head")
		require.Zero(t, e.fake.applyCalls)
		require.Empty(t, e.fake.statCalls)
		state, ok := e.fake.tableState("feature")
		require.True(t, ok)
		require.Equal(t, []string{"a", "b", "c", "d"}, state.paths())
		for path, size := range map[string]int64{"a": 10, "b": 20, "c": 30, "d": 40} {
			e.requireRow("feature", path, "s4", size)
		}
	})
}

// probeTimeout bounds how long a branch tick waits for another one to start, so an indexer that
// runs its branches one at a time fails the assertion of the concurrency test instead of hanging.
const probeTimeout = 3 * time.Second

// inFlightProbe measures how many branch ticks of one indexer run at the same time (§4.1). A tick
// counts as in flight while the fake answers the head read it starts with; enter blocks until want
// ticks are in flight together, or until probeTimeout.
type inFlightProbe struct {
	want    int
	mu      sync.Mutex
	in      int
	peak    int
	reached chan struct{}
	once    sync.Once
}

func newInFlightProbe(want int) *inFlightProbe {
	return &inFlightProbe{want: want, reached: make(chan struct{})}
}

func (p *inFlightProbe) enter(string) func() {
	p.mu.Lock()
	p.in++
	if p.in > p.peak {
		p.peak = p.in
	}
	enough := p.in >= p.want
	p.mu.Unlock()
	if enough {
		p.once.Do(func() { close(p.reached) })
	}
	select {
	case <-p.reached:
	case <-time.After(probeTimeout):
		// The indexer did not run want branches at once. Let the tick finish so that the test
		// reports the peak it measured instead of timing out.
	}
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.in--
	}
}

// maxInFlight returns the largest number of branch ticks the probe saw at once.
func (p *inFlightProbe) maxInFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// TestConcurrencyBoundsBranchesInFlight pins §4.1: one repository tick processes up to `concurrency`
// branches in parallel and never more, and it does use the parallelism it is configured with.
func TestConcurrencyBoundsBranchesInFlight(t *testing.T) {
	const (
		branches    = 4
		concurrency = 2
	)
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Concurrency = concurrency
		opts.Repositories[0].Branches = []string{"b*"}
	})
	for i := 1; i <= branches; i++ {
		branch := fmt.Sprintf("b%d", i)
		e.fake.addCommit("repo", branch, "s1-"+branch, nil, []client.ObjectStat{object("a", 10)})
	}
	e.tick()
	require.Equal(t, branches, e.fake.rebuildCalls, "every branch is backfilled")

	// One more commit per branch, so the next tick has work to do on all of them at once.
	probe := newInFlightProbe(concurrency)
	e.fake.setDataBranchHook(probe.enter)
	for i := 1; i <= branches; i++ {
		branch := fmt.Sprintf("b%d", i)
		commit := "s2-" + branch
		e.fake.addCommit("repo", branch, commit, []string{"s1-" + branch},
			[]client.ObjectStat{object("a", 10), object("b", 20)})
		e.fake.setDiff("repo", "s1-"+branch, commit, []client.Change{added("b", 20)})
	}

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, concurrency, probe.maxInFlight(),
		"indexRepository processes concurrency branches at a time, and not more than that")
	for i := 1; i <= branches; i++ {
		branch := fmt.Sprintf("b%d", i)
		health := e.health.branch("repo", branch)
		require.NoError(t, health.err, "branch %q", branch)
		require.Zero(t, health.lag, "branch %q", branch)
		e.requireRow(branch, "b", "s2-"+branch, 20)
	}
}

// deleteMetadataBranchBeforeCommit wraps the fake lakeFS and deletes one metadata branch just
// before the indexer commits to it. It is the probe TS-3 describes: a branch (or its repository)
// that disappears while a tick is writing is a 404 in real lakeFS, not a silent success.
type deleteMetadataBranchBeforeCommit struct {
	client.LakeFS
	repository string
	branch     string
	once       sync.Once
}

func (w *deleteMetadataBranchBeforeCommit) Commit(ctx context.Context, repository, branch string, opts client.CommitOptions) (client.Commit, error) {
	if repository == w.repository && branch == w.branch {
		w.once.Do(func() { _ = w.LakeFS.DeleteBranch(ctx, w.repository, w.branch) })
	}
	return w.LakeFS.Commit(ctx, repository, branch, opts)
}

// TestMetadataBranchDeletedDuringATickFails pins §4.2 step 6 and §4.4 for the one write real lakeFS
// refuses with a 404 that the tick must surface: the metadata branch deleted between the tick's load
// and its commit. The old fake re-created the branch under an indexing commit - a history lakeFS
// cannot produce - and reported success (TS-3). The next tick finds no mirror, creates it again
// (seeded from the mds-s1 tag) and catches up, so the failure is not permanent.
func TestMetadataBranchDeletedDuringATickFails(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Repositories[0].Branches = []string{"main", "feature"}
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setBranchHead("repo", "feature", "s1")
	e.tick()
	require.Len(t, e.fake.createdBranches, 1, "the feature mirror is created")

	e.fake.addCommit("repo", "feature", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})

	wrapped := &deleteMetadataBranchBeforeCommit{
		LakeFS:     e.fake,
		repository: "repo-metadata",
		branch:     "feature",
	}
	idx, err := indexer.New(indexer.Options{
		LakeFS:       wrapped,
		Tables:       e.fake,
		Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main", "feature"}}},
		Health:       e.health,
		Metrics:      e.metrics,
	})
	require.NoError(t, err)

	require.NoError(t, idx.Tick(t.Context()))

	err = e.health.branch("repo", "feature").err
	require.Error(t, err, "a commit to a branch that is gone is not a success")
	require.ErrorIs(t, err, client.ErrNotFound, "it is the 404 lakeFS answers")
	require.NoError(t, e.health.branch("repo", "main").err, "the other branch is unaffected")
	_, ok := e.fake.branchHead("repo-metadata", "feature")
	require.False(t, ok, "the fake does not re-create the branch under an indexing commit")

	// §4.5 again on the next tick: the mirror is gone, so it is seeded from the indexed ancestor.
	e.tick()

	require.NoError(t, e.health.branch("repo", "feature").err)
	require.Equal(t, []string{"a", "b"}, e.paths("feature"))
	require.Equal(t, "s2", e.health.branch("repo", "feature").lastIndexed)
}

// failingReloadStore makes the reload of every table it serves fail, so a test can force the error
// paths between the steps of a tick (IX-6).
type failingReloadStore struct {
	indexer.TableStore
	err error
}

func (s *failingReloadStore) LoadTable(ctx context.Context, repository, branch string) (indexer.Table, error) {
	tbl, err := s.TableStore.LoadTable(ctx, repository, branch)
	if err != nil {
		return nil, err
	}
	return &failingReloadTable{Table: tbl, err: s.err}, nil
}

type failingReloadTable struct {
	indexer.Table
	err error
}

func (t *failingReloadTable) Refresh(context.Context) error { return t.err }

// TestErrorMessagesNameTheRepositoryBeforeTheBranch pins IX-6: every message that identifies a
// metadata table names repository@branch, like the rest of the package, not branch@repository.
func TestErrorMessagesNameTheRepositoryBeforeTheBranch(t *testing.T) {
	reloadErr := errors.New("the catalog is unreachable")
	requireMessage := func(t *testing.T, err error, phrase string) {
		t.Helper()
		require.ErrorIs(t, err, reloadErr)
		require.Contains(t, err.Error(), phrase)
		require.Contains(t, err.Error(), "repo@main", "the repository comes before the branch")
		require.NotContains(t, err.Error(), "main@repo")
	}

	t.Run("before applying a commit", func(t *testing.T) {
		e := newEnv(t)
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.tick()
		e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
		e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})

		idx, err := indexer.New(indexer.Options{
			LakeFS:       e.fake,
			Tables:       &failingReloadStore{TableStore: e.fake, err: reloadErr},
			Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
			Health:       e.health,
			Metrics:      e.metrics,
		})
		require.NoError(t, err)
		require.NoError(t, idx.Tick(t.Context()))

		requireMessage(t, e.health.branch("repo", "main").err, "before applying s2")
	})

	t.Run("after discarding staging", func(t *testing.T) {
		e := newEnv(t)
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.tick()
		committed, ok := e.fake.tableState("main")
		require.True(t, ok)
		staged := committed.clone()
		staged.sourceCommit = "not-a-commit-of-this-branch"
		e.fake.stage("main", staged)

		idx, err := indexer.New(indexer.Options{
			LakeFS:       e.fake,
			Tables:       &failingReloadStore{TableStore: e.fake, err: reloadErr},
			Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
			Health:       e.health,
			Metrics:      e.metrics,
		})
		require.NoError(t, err)
		require.NoError(t, idx.Tick(t.Context()))

		requireMessage(t, e.health.branch("repo", "main").err, "after a reset")
	})
}

// TestReloadReusesTheBranchCatalog pins IX-3: the reload before step 3 goes through the branch's
// catalog (Table.Refresh), so a tick that applies two commits fetches the catalog's /v1/config once
// instead of once per commit. The old path built a new rest.Catalog per reload, and rest.NewCatalog
// eagerly fetches /v1/config; it also closed the previous handle, dropping its connection pool.
func TestReloadReusesTheBranchCatalog(t *testing.T) {
	ctx := t.Context()
	env := newMDSServerEnv(t)

	// The first tick backfills the branch; the counted tick applies two deltas.
	require.NoError(t, env.catalog.CreateEntry(ctx, env.dataRepo, "main", env.entry(t, "a", 10, "etag-a")))
	_, err := env.catalog.Commit(ctx, env.dataRepo, "main", "add a", "tester", nil, nil, nil, false)
	require.NoError(t, err)
	buildIndexer := func(endpoint string) *indexer.Indexer {
		t.Helper()
		idx, err := indexer.New(indexer.Options{
			Endpoint:    endpoint,
			AccessKeyID: env.accessKeyID,
			SecretKey:   env.secretKey,
			Repositories: []indexer.Repository{{
				Name:     env.dataRepo,
				Branches: []string{"main"},
			}},
			// The fixture's object addresses are outside the blockstore root, so a full listing
			// of a commit with objects is not available; starting from the root commit keeps the
			// tick to deltas, which is all this test needs.
			Backfill: indexer.BackfillHistory,
			Log:      logging.FromContext(ctx),
		})
		require.NoError(t, err)
		return idx
	}
	require.NoError(t, buildIndexer(env.url).Tick(ctx), "backfill the branch")

	require.NoError(t, env.catalog.CreateEntry(ctx, env.dataRepo, "main", env.entry(t, "b", 20, "etag-b")))
	_, err = env.catalog.Commit(ctx, env.dataRepo, "main", "add b", "tester", nil, nil, nil, false)
	require.NoError(t, err)
	require.NoError(t, env.catalog.CreateEntry(ctx, env.dataRepo, "main", env.entry(t, "c", 30, "etag-c")))
	third, err := env.catalog.Commit(ctx, env.dataRepo, "main", "add c", "tester", nil, nil, nil, false)
	require.NoError(t, err)

	target, err := url.Parse(env.url)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var configCalls atomic.Int64
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/v1/config") {
			configCalls.Add(1)
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)

	require.NoError(t, buildIndexer(front.URL).Tick(ctx), "one tick, two pending commits")
	require.EqualValues(t, 1, configCalls.Load(),
		"one catalog per branch tick: the reloads reuse it instead of fetching /v1/config again")

	// And both commits really were applied by that tick.
	readCatalog, err := table.NewCatalog(ctx, table.CatalogConfig{
		Endpoint:        env.url,
		Repository:      env.metadataRepo,
		Branch:          "main",
		AccessKeyID:     env.accessKeyID,
		SecretAccessKey: env.secretKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = readCatalog.Close() })
	tbl, err := readCatalog.LoadTable(ctx, table.Identifier())
	require.NoError(t, err)
	require.Equal(t, third.Reference, tbl.Properties()[table.PropertySourceCommit])
	paths := make([]string, 0, 3)
	for _, row := range scanMDSRows(t, tbl) {
		paths = append(paths, row.Path)
	}
	require.Equal(t, []string{"a", "b", "c"}, paths)
}
