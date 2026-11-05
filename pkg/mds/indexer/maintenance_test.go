package indexer_test

// maintenance_test.go covers design §4.9: compaction by delete files and by the indexed commits
// since the last compaction, the lakeFS commit it leaves behind (without a new tag), and tag
// retention. The every_commits count is durable state, so the tests here also restart the indexer
// over the same lakeFS and expect the same commit to trigger it.

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/client"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
)

// TestCompactionByDeleteFiles pins the max_delete_files trigger: once the table carries more delete
// files than the limit, the branch is rewritten from its own rows and the compaction is recorded in
// a lakeFS commit that moves no tag.
func TestCompactionByDeleteFiles(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Compaction = indexer.Compaction{MaxDeleteFiles: 1, EveryCommits: 1000}
	})
	e.fake.addCommit("repo", "main", "s1", nil,
		[]client.ObjectStat{object("a", 10), object("b", 20), object("c", 30)})
	e.tick()

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{removed("a")})
	e.tick()
	require.Zero(t, e.fake.compactCalls, "one delete file is within the limit")

	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
	e.fake.setDiff("repo", "s2", "s3", []client.Change{removed("b")})
	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 1, e.fake.compactCalls)
	require.Equal(t, 1, e.metrics.compactions)

	state, _ := e.fake.tableState("main")
	require.Equal(t, []string{"c"}, state.paths(), "compaction keeps the rows")
	require.Equal(t, "s3", state.sourceCommit, "and the position")
	require.Zero(t, state.deleteFiles, "and drops the delete files")

	last := e.fake.madeCommits()[len(e.fake.madeCommits())-1]
	require.Equal(t, "mds: compact repo@s3", last.Message)
	require.Equal(t, "s3", last.Metadata[indexer.MetadataKeySourceCommit])
	_, tagged := e.fake.tagCommit("repo-metadata", "mds-s3")
	require.True(t, tagged, "the tag stays on the pre-compaction commit")
	tagCommit, _ := e.fake.tagCommit("repo-metadata", "mds-s3")
	head, _ := e.fake.branchHead("repo-metadata", "main")
	require.NotEqual(t, head, tagCommit, "the compaction moved the branch and moved no tag")

	// §4.4's tag repair runs at the start of every tick, and after a compaction the branch head is
	// the compaction commit. It must leave the tag on the pre-compaction commit, whose rows are the
	// same, instead of forcing it onto the head.
	beforeCommits, beforeTags := len(e.fake.madeCommits()), len(e.fake.tagsMade)
	e.tick()
	require.Len(t, e.fake.madeCommits(), beforeCommits, "an idle compacted branch writes no commit")
	require.Len(t, e.fake.tagsMade, beforeTags, "and creates no tag")
	tagAfter, ok := e.fake.tagCommit("repo-metadata", "mds-s3")
	require.True(t, ok)
	require.Equal(t, tagCommit, tagAfter, "the tag does not move on the tick after the compaction")
	head, _ = e.fake.branchHead("repo-metadata", "main")
	require.NotEqual(t, head, tagAfter, "the tag stays on the indexing commit")
}

// TestCompactionEveryCommits pins the every_commits trigger: it counts indexed commits, not
// snapshots, so a compaction does not push the next one into the following tick.
func TestCompactionEveryCommits(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Compaction = indexer.Compaction{MaxDeleteFiles: 1000, EveryCommits: 2}
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	require.Zero(t, e.fake.compactCalls, "one indexed commit is below every_commits")

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.tick()
	require.Equal(t, 1, e.fake.compactCalls, "two indexed commits trigger it")

	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
	e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
	e.tick()
	require.Equal(t, 1, e.fake.compactCalls, "the count restarts at the compaction commit")

	e.fake.addCommit("repo", "main", "s4", []string{"s3"}, nil)
	e.fake.setDiff("repo", "s3", "s4", []client.Change{added("d", 40)})
	e.tick()
	require.Equal(t, 2, e.fake.compactCalls)

	state, _ := e.fake.tableState("main")
	require.Equal(t, []string{"a", "b", "c", "d"}, state.paths())
}

// TestCompactionEveryCommitsSurvivesRestart pins that the every_commits count is durable state and
// not a tally of the process: a fresh indexer over the same lakeFS keeps counting the commits the
// previous process indexed, so it compacts at the commit that reaches every_commits and not
// every_commits commits later.
func TestCompactionEveryCommitsSurvivesRestart(t *testing.T) {
	configure := func(opts *indexer.Options) {
		opts.Compaction = indexer.Compaction{MaxDeleteFiles: 1000, EveryCommits: 3}
	}
	e := newEnv(t, configure)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.tick()
	require.Zero(t, e.fake.compactCalls, "two of three indexed commits are below every_commits")

	// A restart is a new indexer over the same lakeFS: the two commits the previous process indexed
	// have to come back from R-metadata, which is what the count is read from.
	restarted := restart(t, e, configure)
	require.NoError(t, restarted.Tick(t.Context()))
	require.Zero(t, e.fake.compactCalls, "a restart does not count more commits than were indexed")

	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
	e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
	require.NoError(t, restarted.Tick(t.Context()))

	require.Equal(t, 1, e.fake.compactCalls, "the third indexed commit triggers it, restart or not")
	made := e.fake.madeCommits()
	require.Equal(t, "mds: compact repo@s3", made[len(made)-1].Message,
		"the compaction happens at the commit that reached every_commits")
	state, _ := e.fake.tableState("main")
	require.Equal(t, []string{"a", "b", "c"}, state.paths(), "compaction keeps the rows")
	require.Equal(t, "s3", state.sourceCommit, "and the position")
	require.Zero(t, state.deleteFiles)

	// The compaction commit is where the durable count restarts, so neither the next tick nor one
	// commit after it triggers a second compaction.
	require.NoError(t, restarted.Tick(t.Context()))
	require.Equal(t, 1, e.fake.compactCalls, "a compacted branch is quiet until every_commits again")
	e.fake.addCommit("repo", "main", "s4", []string{"s3"}, nil)
	e.fake.setDiff("repo", "s3", "s4", []client.Change{added("d", 40)})
	require.NoError(t, restarted.Tick(t.Context()))
	require.Equal(t, 1, e.fake.compactCalls, "one indexed commit after the compaction is below it")
}

// TestCompactionEveryCommitsPagesTheLog pins that the count reads the commit log in pages: three
// indexed commits do not fit in a page of one.
func TestCompactionEveryCommitsPagesTheLog(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Compaction = indexer.Compaction{MaxDeleteFiles: 1000, EveryCommits: 3}
	})
	e.fake.pageSize = 1
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.tick()
	require.Zero(t, e.fake.compactCalls)

	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
	e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
	e.tick()
	require.Equal(t, 1, e.fake.compactCalls, "the count crosses the page boundaries")
}

// restart builds a second indexer over the same fake lakeFS with the same options: what a process
// restart looks like to §4.9, where no tally of the previous process is left.
func restart(t *testing.T, e *env, configure ...func(*indexer.Options)) *indexer.Indexer {
	t.Helper()
	opts := indexer.Options{
		LakeFS:       e.fake,
		Tables:       e.fake,
		Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
		Health:       e.health,
		Metrics:      e.metrics,
		Log:          logging.ContextUnavailable(),
	}
	for _, apply := range configure {
		apply(&opts)
	}
	idx, err := indexer.New(opts)
	require.NoError(t, err)
	return idx
}

// TestCompactionDisabledByZero pins the other half of IO-6: a threshold of 0 turns its trigger
// off for good. Before the fix indexer.applyDefaults rewrote 0 to the documented default, so an
// operator could not express "never compact" at all, although the sample configuration and
// docs/metadata-search.md document 0 as exactly that.
func TestCompactionDisabledByZero(t *testing.T) {
	// indexCommits indexes a branch of n + 1 commits that each delete the object "a", so the
	// branch ends with n delete files and n indexed commits: each threshold is crossed by a wide
	// margin, and neither default would leave it alone.
	indexCommits := func(t *testing.T, e *env, n int) {
		t.Helper()
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		parent := "s1"
		for i := 2; i <= n+1; i++ {
			commit := fmt.Sprintf("s%d", i)
			e.fake.addCommit("repo", "main", commit, []string{parent}, nil)
			e.fake.setDiff("repo", parent, commit, []client.Change{removed("a")})
			parent = commit
		}
		e.tick()
		require.NoError(t, e.health.repositoryError("repo"))
		require.NoError(t, e.health.branch("repo", "main").err)
	}

	t.Run("max_delete_files: 0", func(t *testing.T) {
		e := newEnv(t, func(opts *indexer.Options) {
			opts.Backfill = indexer.BackfillHistory
			opts.Compaction = indexer.Compaction{MaxDeleteFiles: 0, EveryCommits: 1000}
		})
		indexCommits(t, e, 201) // one more than the default of 200 that used to replace the 0

		state, _ := e.fake.tableState("main")
		require.Equal(t, 201, state.deleteFiles, "the branch is above every delete-file limit")
		require.Zero(t, e.fake.compactCalls, "max_delete_files: 0 disables the delete-file trigger")
		require.Zero(t, e.metrics.compactions)
	})

	t.Run("every_commits: 0", func(t *testing.T) {
		e := newEnv(t, func(opts *indexer.Options) {
			opts.Backfill = indexer.BackfillHistory
			opts.Compaction = indexer.Compaction{MaxDeleteFiles: 1000, EveryCommits: 0}
		})
		indexCommits(t, e, 501) // one more than the default of 500 that used to replace the 0

		require.Zero(t, e.fake.compactCalls, "every_commits: 0 disables the commit-count trigger")
		require.Zero(t, e.metrics.compactions)
	})
}

// TestTagRetention pins §4.9's tag_retention: mds-* tags whose source commit is older than the
// retention are deleted, younger ones are kept, and 0 keeps everything.
func TestTagRetention(t *testing.T) {
	now := time.Now().UTC()
	setup := func(t *testing.T, retention time.Duration) *env {
		t.Helper()
		e := newEnv(t, func(opts *indexer.Options) {
			opts.Backfill = indexer.BackfillHistory
			opts.TagRetention = retention
		})
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
		e.fake.setCommitDate("repo", "s1", now.Add(-72*time.Hour))
		e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
		e.fake.setCommitDate("repo", "s2", now)
		e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
		e.tick()
		return e
	}

	t.Run("retires old tags", func(t *testing.T) {
		e := setup(t, 24*time.Hour)

		require.NoError(t, e.health.repositoryError("repo"))
		require.Equal(t, []string{"mds-s1"}, e.fake.deletedTags)
		_, ok := e.fake.tagCommit("repo-metadata", "mds-s1")
		require.False(t, ok, "an old tag is gone")
		_, ok = e.fake.tagCommit("repo-metadata", "mds-s2")
		require.True(t, ok, "a recent tag stays")
	})

	t.Run("zero keeps every tag", func(t *testing.T) {
		e := setup(t, 0)

		require.NoError(t, e.health.repositoryError("repo"))
		require.Empty(t, e.fake.deletedTags)
		for _, commit := range []string{"s1", "s2"} {
			_, ok := e.fake.tagCommit("repo-metadata", "mds-"+commit)
			require.True(t, ok, "tag mds-%s", commit)
		}
	})
}

// TestTagRetentionKeepsCurrentPosition pins the interaction between §4.9's tag_retention and §4.4's
// tag repair: the tag of an idle branch records the branch's current position, so retention must
// leave it alone however old its source commit is. Deleting it made the next tick recreate it and
// the tick after that delete it again.
func TestTagRetentionKeepsCurrentPosition(t *testing.T) {
	now := time.Now().UTC()
	e := newEnv(t, func(opts *indexer.Options) {
		opts.TagRetention = 24 * time.Hour
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setCommitDate("repo", "s1", now.Add(-72*time.Hour))

	e.tick()

	_, ok := e.fake.tagCommit("repo-metadata", "mds-s1")
	require.True(t, ok, "the tag of the current position is kept although its commit is old")
	require.Empty(t, e.fake.deletedTags, "retention does not delete the current position")
	tagsMade := len(e.fake.tagsMade)

	e.tick()

	require.Empty(t, e.fake.deletedTags, "the second tick deletes nothing")
	require.Len(t, e.fake.tagsMade, tagsMade, "and the tag repair creates nothing")
	_, ok = e.fake.tagCommit("repo-metadata", "mds-s1")
	require.True(t, ok)
}

// TestTagRetentionRetiresUnreadableTags pins the other half of tagExpired: a tag whose source commit
// is not there any more cannot be resolved through the catalog (§6), so it is retired too.
func TestTagRetentionRetiresUnreadableTags(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.TagRetention = time.Hour
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setCommitDate("repo", "s1", time.Now().UTC())
	e.tick()

	// A tag whose source commit is not in R any more: the metadata commit records it, but a read of
	// R cannot resolve it, so the tag is not addressable through the catalog (§6).
	head, ok := e.fake.branchHead("repo-metadata", "main")
	require.True(t, ok)
	dangling := e.fake.commits["repo-metadata"][head]
	dangling.ID = "dangling"
	dangling.Metadata = map[string]string{
		indexer.MetadataKeySourceCommit:     "gone",
		indexer.MetadataKeySourceRepository: "repo",
	}
	e.fake.setCommit("repo-metadata", dangling)
	e.fake.addTag("repo-metadata", "mds-gone", "dangling")

	e.tick()

	require.Contains(t, e.fake.deletedTags, "mds-gone")
	_, ok = e.fake.tagCommit("repo-metadata", "mds-s1")
	require.True(t, ok, "a tag whose source commit exists and is recent stays")
}
