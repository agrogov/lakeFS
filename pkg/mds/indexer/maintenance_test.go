package indexer_test

// maintenance_test.go covers design §4.9: compaction by delete files and by the commit counter, the
// lakeFS commit it leaves behind (without a new tag), and tag retention.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	require.Equal(t, 1, e.fake.compactCalls, "the counter restarts after a compaction")

	e.fake.addCommit("repo", "main", "s4", []string{"s3"}, nil)
	e.fake.setDiff("repo", "s3", "s4", []client.Change{added("d", 40)})
	e.tick()
	require.Equal(t, 2, e.fake.compactCalls)

	state, _ := e.fake.tableState("main")
	require.Equal(t, []string{"a", "b", "c", "d"}, state.paths())
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
