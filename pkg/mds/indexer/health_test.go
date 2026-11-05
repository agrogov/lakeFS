package indexer_test

// health_test.go covers what the indexer reports on /_health and /metrics (§9) when a tick does
// not end in the plain success case: the state of a branch that is no longer followed (IO-3), of a
// branch that failed before it knew where it stood (IO-4), and of a repository whose tick was
// cancelled (IO-5). The rest of the fake-driven tests live next to their subject; these are here
// because the reported state is the subject.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/client"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
)

// TestDeletedBranchLeavesHealthWithoutDeletingTheMirror pins IO-3: the /_health entry and the lag
// gauge of a deleted branch must go whichever delete_branches says. The cleanup used to be a side
// effect of deleting the metadata branch, so with delete_branches: false one failed attempt of a
// branch that was deleted afterwards kept /_health at 503 until the process restarted (§4.8).
func TestDeletedBranchLeavesHealthWithoutDeletingTheMirror(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Repositories[0].Branches = []string{"main", "feature"}
		opts.DeleteBranches = false
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setBranchHead("repo", "feature", "s1")
	e.tick()
	require.NoError(t, e.health.branch("repo", "feature").err, "feature is indexed")

	// One commit on feature that cannot be indexed: its diff is missing, so §4.2 step 6 fails the
	// branch with one commit pending.
	e.fake.addCommit("repo", "feature", "s2", []string{"s1"}, nil)
	e.tick()
	failed := e.health.branch("repo", "feature")
	require.Error(t, failed.err, "the last attempt of feature failed")
	require.EqualValues(t, 1, failed.lag, "and it is one commit behind")

	// The branch is deleted in R, and delete_branches: false keeps its metadata branch.
	e.fake.deleteBranch("repo", "feature")
	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.False(t, e.health.hasBranch("repo", "feature"),
		"the /_health entry of a deleted branch is removed even when the mirror is kept")
	require.Contains(t, e.metrics.cleared, "repo@feature", "and its lag gauge is cleared")
	require.Empty(t, e.fake.deletedBranches)
	_, ok := e.fake.branchHead("repo-metadata", "feature")
	require.True(t, ok, "delete_branches: false leaves the metadata branch as it is (§4.8)")
}

// TestBranchStopsMatchingClearsItsLagGauge pins the second half of IO-3: a branch that still
// exists in R but no longer matches the configuration already lost its /_health entry, but its
// lakefs_mds_lag_commits series kept its last value forever.
func TestBranchStopsMatchingClearsItsLagGauge(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Repositories[0].Branches = []string{"main", "feature"}
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.fake.setBranchHead("repo", "feature", "s1")
	e.tick()
	require.Contains(t, e.metrics.lags, "repo@feature", "the followed branch reports a lag")

	// The operator narrows the selector, which is a new indexer over the same sinks: the branch is
	// still a branch of R, it is simply no longer followed.
	idx, err := indexer.New(indexer.Options{
		LakeFS:       e.fake,
		Tables:       e.fake,
		Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
		Health:       e.health,
		Metrics:      e.metrics,
	})
	require.NoError(t, err)
	require.NoError(t, idx.Tick(t.Context()))

	require.Contains(t, e.metrics.cleared, "repo@feature", "the lag gauge of a branch that stops matching is cleared")
	require.False(t, e.health.hasBranch("repo", "feature"), "and so is its /_health entry")
}

// TestFailedBranchReportsTheRecordedPosition pins IO-4: a branch that fails before it counts the
// pending commits reports the position its table is at and an unknown lag, not last_indexed: ""
// with lag_commits: 0. The latter is the answer an alert on lag_commits > 0 has to fire on, and it
// was reported for the failure mode the alert exists for (§9).
func TestFailedBranchReportsTheRecordedPosition(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	require.Equal(t, branchRecord{lastIndexed: "s1", head: "s1"}, e.health.branch("repo", "main"))

	// Two commits pending, and the merge-base read of §4.2 step 4 fails like a transient 500: the
	// tick fails before it can walk the log.
	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
	e.fake.setDiff("repo", "s2", "s3", []client.Change{added("c", 30)})
	e.fake.findMergeBaseErr = errors.New("find merge base: unexpected lakeFS response")

	e.tick()

	failed := e.health.branch("repo", "main")
	require.Error(t, failed.err)
	require.Equal(t, "s1", failed.lastIndexed,
		"the position the table is at is known and must be reported")
	require.Equal(t, "s3", failed.head)
	require.Equal(t, indexer.LagUnknown, failed.lag,
		"the tick never counted the pending commits, so the lag is unknown and not 0")
	require.Equal(t, indexer.LagUnknown, e.metrics.lags["repo@main"],
		"the lag gauge says unknown too, instead of claiming the branch is caught up")
}

// TestFailedBranchReportsThePositionItAdvancedTo pins the other half of IO-4: a tick that applied
// some commits before it failed reports the position it reached and the exact number of commits
// left, so an operator sees how far the backlog was worked off.
func TestFailedBranchReportsThePositionItAdvancedTo(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()

	e.fake.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	e.fake.addCommit("repo", "main", "s3", []string{"s2"}, nil)
	// No diff for s2..s3: changing s3 fails after s2 was applied and committed.
	e.tick()

	failed := e.health.branch("repo", "main")
	require.Error(t, failed.err)
	require.Equal(t, "s2", failed.lastIndexed, "the commit that was applied and committed is the position")
	require.Equal(t, "s3", failed.head)
	require.EqualValues(t, 1, failed.lag, "one commit is still to apply")
}

// cancelRecordingHealth is the fake's Health sink plus the optional indexer.TickCanceller part:
// the fake_test.go type is shared with tests that do not care about cancellation, so the extra
// method lives here.
type cancelRecordingHealth struct {
	*fakeHealth

	mu      sync.Mutex
	cancels []string
}

func (h *cancelRecordingHealth) CancelTick(repository string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cancels = append(h.cancels, repository)
}

func (h *cancelRecordingHealth) cancelled() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.cancels...)
}

// TestCancelledTickIsNotLeftInProgress pins the second half of IO-5: the tick of a repository that
// is cancelled mid-flight must end, and it must end without an outcome - the previous one stays.
// It used to return before FinishTick, so the repository was reported as in progress for the life
// of the process.
func TestCancelledTickIsNotLeftInProgress(t *testing.T) {
	health := &cancelRecordingHealth{fakeHealth: newFakeHealth()}
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Health = health
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	ctx, cancel := context.WithCancel(t.Context())
	e.fake.setDataBranchHook(func(string) func() {
		cancel()
		return nil
	})

	require.NoError(t, e.indexer.Tick(ctx))

	require.Equal(t, []string{"repo"}, health.cancelled(),
		"the cancelled tick ends the in-progress state of the repository")
	require.Empty(t, health.finishes, "a cancelled tick is not an outcome, so nothing is finished with")
}
