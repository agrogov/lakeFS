package indexer_test

// repo_test.go covers design §3.3 and §4.8 on the companion repository: the storage namespace
// derivation and its refusals, the creation of R-metadata and its initialisation commit, and the
// refusal of a repository the indexer did not create.

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/client"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
)

// TestMetadataNamespace pins the namespace rules of §3.3 through the effect they have on the
// repository the indexer creates: the derived name, the explicit override, and the two refusals -
// a bucket-root namespace that cannot be derived, and a namespace that overlaps R's (R's
// uncommitted GC would sweep it).
func TestMetadataNamespace(t *testing.T) {
	tests := []struct {
		name          string
		dataNamespace string
		configured    string
		want          string
		wantErr       error
	}{
		{
			name:          "derived from a namespace with a path",
			dataNamespace: "s3://bucket/data",
			want:          "s3://bucket/data-metadata",
		},
		{
			name:          "derived, trailing slash removed",
			dataNamespace: "s3://bucket/data/",
			want:          "s3://bucket/data-metadata",
		},
		{
			name:          "explicit namespace",
			dataNamespace: "s3://bucket/data",
			configured:    "s3://other/metadata/repo",
			want:          "s3://other/metadata/repo",
		},
		{
			name:          "explicit namespace, trailing slash removed",
			dataNamespace: "s3://bucket/data",
			configured:    "s3://other/metadata/",
			want:          "s3://other/metadata",
		},
		{
			name:          "bucket root cannot be derived",
			dataNamespace: "s3://bucket",
			wantErr:       indexer.ErrBucketRootNamespace,
		},
		{
			name:          "bucket root with a slash cannot be derived",
			dataNamespace: "s3://bucket/",
			wantErr:       indexer.ErrBucketRootNamespace,
		},
		{
			name:          "bucket root with an explicit namespace is fine",
			dataNamespace: "s3://bucket",
			configured:    "s3://bucket-data/metadata",
			want:          "s3://bucket-data/metadata",
		},
		{
			name:          "explicit namespace inside the data namespace",
			dataNamespace: "s3://bucket/data",
			configured:    "s3://bucket/data/metadata",
			wantErr:       indexer.ErrOverlappingNamespace,
		},
		{
			name:          "explicit namespace that is the data namespace",
			dataNamespace: "s3://bucket/data",
			configured:    "s3://bucket/data",
			wantErr:       indexer.ErrOverlappingNamespace,
		},
		{
			name:          "explicit namespace that contains the data namespace",
			dataNamespace: "s3://bucket/data/repo",
			configured:    "s3://bucket/data",
			wantErr:       indexer.ErrOverlappingNamespace,
		},
		{
			name:          "a sibling with a shared prefix does not overlap",
			dataNamespace: "s3://bucket/data",
			configured:    "s3://bucket/data-metadata",
			want:          "s3://bucket/data-metadata",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := newEnv(t, func(opts *indexer.Options) {
				opts.Repositories[0].StorageNamespace = test.configured
			})
			e.fake.repositories["repo"] = client.Repository{
				Name:             "repo",
				StorageNamespace: test.dataNamespace,
				DefaultBranch:    "main",
				StorageID:        "storage-repo",
			}
			e.fake.addCommit("repo", "main", "s1", nil, nil)

			e.tick()

			if test.wantErr != nil {
				require.ErrorIs(t, e.health.repositoryError("repo"), test.wantErr)
				require.Empty(t, e.fake.createdRepositories, "a refused repository creates nothing")
				return
			}
			require.NoError(t, e.health.repositoryError("repo"))
			require.Len(t, e.fake.createdRepositories, 1)
			require.Equal(t, test.want, e.fake.createdRepositories[0].StorageNamespace)
		})
	}
}

// TestExistingMetadataNamespaceOverlapIsRefused pins §3.3's namespace rule on the path that never
// creates R-metadata: the overlap check used to run only in createMetadataRepository, so an existing
// companion repository whose namespace overlaps R's was used as it is. R's uncommitted GC tests
// every object address against R's normalized namespace prefix
// (pkg/catalog/gc_write_uncommitted.go) and would delete the table's files; the repository is now
// refused on every tick, before anything is written, and marked failed as §7 and §4.8 say.
func TestExistingMetadataNamespaceOverlapIsRefused(t *testing.T) {
	tests := []struct {
		name              string
		dataNamespace     string
		metadataNamespace string
	}{
		{
			name:              "metadata namespace inside the data namespace",
			dataNamespace:     "s3://bucket/data",
			metadataNamespace: "s3://bucket/data/metadata",
		},
		{
			name:              "metadata namespace is the data namespace",
			dataNamespace:     "s3://bucket/data",
			metadataNamespace: "s3://bucket/data",
		},
		{
			name:              "metadata namespace contains the data namespace",
			dataNamespace:     "s3://bucket/data/repo",
			metadataNamespace: "s3://bucket/data",
		},
		{
			name:              "data namespace moved on top of an existing metadata namespace",
			dataNamespace:     "s3://bucket",
			metadataNamespace: "s3://bucket/data-metadata",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := newEnv(t)
			e.fake.repositories["repo"] = client.Repository{
				Name:             "repo",
				StorageNamespace: test.dataNamespace,
				DefaultBranch:    "main",
				StorageID:        "storage-repo",
			}
			e.fake.addBareMetadataRepository(test.metadataNamespace)
			e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

			e.tick()

			require.ErrorIs(t, e.health.repositoryError("repo"), indexer.ErrOverlappingNamespace)
			require.Contains(t, e.metrics.errors, "repo/"+indexer.StageRepository,
				"the repository is marked failed in metrics (§7, §9)")
			require.Empty(t, e.fake.madeCommits(), "nothing is written into a repository R's uncommitted GC would sweep")
			require.Zero(t, e.fake.applyCalls)
			require.Zero(t, e.fake.rebuildCalls)
		})
	}
}

// TestExistingBareMetadataRepositoryIsAccepted pins the acceptance row of §4.8 that only a bare
// companion repository produces: the lakefs.mds.init marker is on the root commit, not on the first
// commit after lakeFS's automatic "Repository created" root. Such a repository is the indexer's, so
// the tick takes its initialisation commit from it and writes nothing but the indexing commit.
func TestExistingBareMetadataRepositoryIsAccepted(t *testing.T) {
	e := newEnv(t)
	init := e.fake.addBareMetadataRepository("s3://bucket/data-metadata")
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Empty(t, e.fake.createdRepositories, "an existing initialised repository is reused")
	require.Equal(t, init, e.fake.initCommitID(), "the root stays the initialisation commit")
	commits := e.fake.madeCommits()
	require.Len(t, commits, 1, "only the indexing commit is written")
	require.Equal(t, "mds: index repo@s1", commits[0].Message)
	head, _ := e.fake.branchHead("repo-metadata", "main")
	require.Equal(t, []string{init}, e.fake.commits["repo-metadata"][head].Parents)
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, []string{"a"}, state.paths())
	_, ok = e.fake.tagCommit("repo-metadata", "mds-s1")
	require.True(t, ok, "the indexed commit is tagged for the ref mapper (§6)")

	// A second tick finds the repository initialised and writes nothing.
	e.tick()
	require.Len(t, e.fake.madeCommits(), 1)
	require.Empty(t, e.fake.resetCalls())
}

// TestCreateMetadataRepository pins §3.3 and §4.8: R-metadata is created with R's default branch,
// storage ID and not read-only; its namespace and table are created on that branch; and the
// initialisation commit carries lakefs.mds.init before any indexing commit.
func TestCreateMetadataRepository(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	require.Len(t, e.fake.createdRepositories, 1)
	created := e.fake.createdRepositories[0]
	require.Equal(t, "repo-metadata", created.Name)
	require.Equal(t, "s3://bucket/data-metadata", created.StorageNamespace)
	require.Equal(t, "main", created.DefaultBranch)
	require.Equal(t, "storage-repo", created.StorageID)
	require.False(t, created.ReadOnly, "the indexer must be able to write, so the repository is not read-only")

	commits := e.fake.madeCommits()
	require.Len(t, commits, 2)
	require.Equal(t, indexer.InitCommitMessage, commits[0].Message)
	require.Equal(t, indexer.MetadataKeyInitValue, commits[0].Metadata[indexer.MetadataKeyInit])
	require.Equal(t, "repo", commits[0].Metadata[indexer.MetadataKeySourceRepository])
	require.Equal(t, "mds: index repo@s1", commits[1].Message)
	require.Equal(t, "s1", commits[1].Metadata[indexer.MetadataKeySourceCommit])

	initCommit := e.fake.initCommitID()
	require.NotEmpty(t, initCommit)
	init := e.fake.commits["repo-metadata"][initCommit]
	require.Equal(t, initCommit, init.ID)
	require.Len(t, init.Parents, 1, "a non-bare repository has its automatic creation root beneath the initialisation")
	root := e.fake.commits["repo-metadata"][init.Parents[0]]
	require.Equal(t, "Repository created", root.Message)
	require.Empty(t, root.Parents, "the automatic creation commit is the root")

	// A second tick finds the repository initialised and writes nothing.
	before := len(e.fake.madeCommits())
	e.tick()
	require.Len(t, e.fake.madeCommits(), before)
	require.Len(t, e.fake.createdRepositories, 1)
}

// TestInterruptedInitialisationIsCompleted pins the crash window of §4.8: R-metadata exists but has
// no commit at all, so the initialisation is completed instead of refused.
func TestInterruptedInitialisationIsCompleted(t *testing.T) {
	e := newEnv(t)
	e.fake.addRepository("repo-metadata", "s3://bucket/data-metadata", "main")
	e.fake.setBranchHead("repo-metadata", "main", "")
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	commits := e.fake.madeCommits()
	require.Len(t, commits, 2, "the initialisation commit and the indexing commit")
	require.Equal(t, indexer.InitCommitMessage, commits[0].Message)
	require.NotEmpty(t, e.fake.initCommitID())
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, []string{"a"}, state.paths())
}

// TestInitialiseOnTheAutomaticCreationCommit pins the non-bare form of §4.8: the indexer's
// CreateRepository leaves lakeFS's automatic "Repository created" root on the branch, and the
// initialisation is completed on top of that root instead of being refused as foreign.
func TestInitialiseOnTheAutomaticCreationCommit(t *testing.T) {
	e := newEnv(t)
	e.fake.addRepository("repo-metadata", "s3://bucket/data-metadata", "main")
	root := e.fake.addCommit("repo-metadata", "main", "created", nil, nil)
	root.Message = "Repository created"
	e.fake.setCommit("repo-metadata", root)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	initCommit := e.fake.initCommitID()
	require.NotEmpty(t, initCommit)
	init := e.fake.commits["repo-metadata"][initCommit]
	require.Equal(t, []string{"created"}, init.Parents, "the initialisation commit is the first non-root commit")
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, []string{"a"}, state.paths())
}

// TestRefuseForeignRepository pins §4.8: a companion repository that was not created by the
// indexer (it has no initialisation commit) is refused and never written to.
func TestRefuseForeignRepository(t *testing.T) {
	e := newEnv(t)
	e.fake.addRepository("repo-metadata", "s3://bucket/foreign", "main")
	e.fake.addCommit("repo-metadata", "main", "foreign", nil, nil)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	err := e.health.repositoryError("repo")
	require.ErrorIs(t, err, indexer.ErrForeignRepository)
	require.Empty(t, e.fake.madeCommits(), "nothing is written into a foreign repository")
	require.Zero(t, e.fake.applyCalls)
	require.Zero(t, e.fake.rebuildCalls)
}

// TestRefuseForeignRepositoryAfterAutomaticRoot pins the other half of §4.8's acceptance rule: a
// non-bare repository whose automatic root is followed by a commit without the initialisation
// marker is not the indexer's, even though the root itself is the shape lakeFS creates.
func TestRefuseForeignRepositoryAfterAutomaticRoot(t *testing.T) {
	e := newEnv(t)
	e.fake.addRepository("repo-metadata", "s3://bucket/foreign", "main")
	root := e.fake.addCommit("repo-metadata", "main", "created", nil, nil)
	root.Message = "Repository created"
	e.fake.setCommit("repo-metadata", root)
	e.fake.addCommit("repo-metadata", "main", "foreign", []string{"created"}, nil)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	require.ErrorIs(t, e.health.repositoryError("repo"), indexer.ErrForeignRepository)
	require.Empty(t, e.fake.madeCommits(), "nothing is written into a foreign repository")
}

// TestDataRepositoryNamedMetadataIsRefused pins the other half of §3.3's naming rule: a configured
// data repository whose own name ends in "-metadata" cannot be indexed.
func TestDataRepositoryNamedMetadataIsRefused(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Repositories = []indexer.Repository{{Name: "acme-metadata", Branches: []string{"main"}}}
	})
	e.fake.addRepository("acme-metadata", "s3://bucket/data", "main")
	e.fake.addRepository("acme-metadata-metadata", "s3://bucket/data-metadata", "main")
	e.fake.addCommit("acme-metadata", "main", "s1", nil, nil)

	e.tick()

	require.ErrorIs(t, e.health.repositoryError("acme-metadata"), indexer.ErrDataRepositoryNamedMetadata)
	require.Empty(t, e.fake.madeCommits())
}

// TestMissingDataRepositoryIsReported pins §4.8's "repository deleted" row: the repository is
// skipped and reported, and the other repositories of the configuration keep indexing.
func TestMissingDataRepositoryIsReported(t *testing.T) {
	e := newEnv(t, func(opts *indexer.Options) {
		opts.Repositories = []indexer.Repository{
			{Name: "gone", Branches: []string{"main"}},
			{Name: "repo", Branches: []string{"main"}},
		}
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	require.ErrorIs(t, e.health.repositoryError("gone"), client.ErrNotFound)
	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 1, e.metrics.indexed["repo"])
}

// TestConcurrentInitialisationCommitsOnce pins IO-1: two indexer processes that meet on a freshly
// created R-metadata must both end up with an initialised repository. Before the fix both committed
// `mds: init` (AllowEmpty: true), the loser on top of the winner, and the next verifyInitCommit
// refused the head as a foreign repository - for every later process, with nothing to repair it but
// deleting R-metadata and losing the mirror.
//
// The competing replica is a wrapper (below) and not a part of fake_test.go: it is the interleaving
// of two processes, not a behaviour of lakeFS.
func TestConcurrentInitialisationCommitsOnce(t *testing.T) {
	var replica *competingReplica
	e := newEnv(t, func(opts *indexer.Options) {
		replica = &competingReplica{LakeFS: opts.LakeFS}
		opts.LakeFS = replica
	})
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	require.True(t, replica.competed, "the competing initialisation ran")
	require.NoError(t, e.health.repositoryError("repo"))
	require.Len(t, initCommits(e.fake), 1, "the initialisation that lost the race must not commit a second mds: init")
	require.Equal(t, 1, e.metrics.indexed["repo"], "the indexer that lost the race still indexes the branch")
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, []string{"a"}, state.paths())

	// The next process of a rolling restart, over the state the two initialisations left: it must
	// recognise the repository instead of answering ErrForeignRepository.
	next := newEnvOn(t, e.fake)
	next.tick()
	require.NoError(t, next.health.repositoryError("repo"))
	require.Len(t, initCommits(e.fake), 1)
}

// TestDuplicateInitCommitsAboveTheAutomaticRootAreAccepted pins the acceptance rule §4.8 needs for
// the history an indexer before the IO-1 fix left behind: two initialisations that both committed,
// so the branch is [mds: init, mds: init, Repository created]. The second commit carries no change
// (lakeFS accepts an empty commit only under AllowEmpty, pkg/graveler/graveler.go:2355), so the run
// is harmless; refusing it made the repository unusable for ever.
func TestDuplicateInitCommitsAboveTheAutomaticRootAreAccepted(t *testing.T) {
	e := newEnv(t)
	root := e.fake.addNonBareMetadataRepository()
	init1 := e.fake.addInitCommit("init-1", root)
	init2 := e.fake.addInitCommit("init-2", init1)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 1, e.metrics.indexed["repo"])
	require.Len(t, initCommits(e.fake), 2, "the fixture, not the tick, made the duplicate")
	state, ok := e.fake.tableState("main")
	require.True(t, ok)
	require.Equal(t, []string{"a"}, state.paths())
	commits := e.fake.madeCommits()
	require.Len(t, commits, 1, "only the indexing commit is written")
	require.Equal(t, "mds: index repo@s1", commits[0].Message)
	head, ok := e.fake.branchHead("repo-metadata", "main")
	require.True(t, ok)
	require.Equal(t, []string{init2}, e.fake.commits["repo-metadata"][head].Parents,
		"the tick indexes on top of the branch head, duplicate initialisation commits included")

	// The run is accepted on every later tick too, from the head the indexing commit left.
	e.tick()
	require.NoError(t, e.health.repositoryError("repo"))
	require.Len(t, initCommits(e.fake), 2)
}

// TestInitMarkerOutsideTheRunIsStillRefused pins the other half of the run rule: the markers must be
// contiguous and must end at lakeFS's automatic root. Either property is what tells a duplicate
// initialisation (harmless, committed nothing) from a repository the indexer did not create.
func TestInitMarkerOutsideTheRunIsStillRefused(t *testing.T) {
	t.Run("a marker below a commit without one", func(t *testing.T) {
		e := newEnv(t)
		root := e.fake.addNonBareMetadataRepository()
		init1 := e.fake.addInitCommit("init-1", root)
		e.fake.addCommit("repo-metadata", "main", "foreign", []string{init1}, nil)
		e.fake.addInitCommit("init-2", "foreign")
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

		e.tick()

		require.ErrorIs(t, e.health.repositoryError("repo"), indexer.ErrForeignRepository)
		require.Empty(t, e.fake.madeCommits(), "nothing is written into a foreign repository")
	})

	t.Run("a marker above a root lakeFS did not create", func(t *testing.T) {
		e := newEnv(t)
		e.fake.addRepository("repo-metadata", "s3://bucket/data-metadata", "main")
		root := e.fake.addCommit("repo-metadata", "main", "created", nil, nil)
		root.Message = "created by hand"
		e.fake.setCommit("repo-metadata", root)
		e.fake.addInitCommit("init-1", root.ID)
		e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

		e.tick()

		require.ErrorIs(t, e.health.repositoryError("repo"), indexer.ErrForeignRepository)
		require.Empty(t, e.fake.madeCommits(), "nothing is written into a foreign repository")
	})
}

// TestInitialisationCheckStopsAtTheAnswer pins TS-5's bounded walk on the shape that settles early:
// a commit that carries the initialisation marker but sits below a commit without one is not a run,
// so the repository is refused as soon as the second commit below the marker is read. The check used
// to read the whole first-parent history - one API call per page - before it looked at any of it, so
// the refusal cost a page per 1000 commits the repository ever indexed.
func TestInitialisationCheckStopsAtTheAnswer(t *testing.T) {
	const history = 400 // 8 pages at the fake's page size of 50

	e := newEnv(t)
	root := e.fake.addNonBareMetadataRepository()
	// A long first-parent history below the marker, as a repository that indexed a lot has.
	parent := root
	for i := range history {
		parent = e.fake.addCommit("repo-metadata", "main", fmt.Sprintf("indexed-%03d", i), []string{parent}, nil).ID
	}
	// The marker sits at the head of the log, above two commits without one: a marker below a gap is
	// part of no run, so the refusal is known two commits after it, however long the history below.
	e.fake.addInitCommit("init-1", parent)
	e.fake.addCommit("repo-metadata", "main", "foreign-1", []string{"init-1"}, nil)
	e.fake.addCommit("repo-metadata", "main", "foreign-2", []string{"foreign-1"}, nil)
	e.fake.pageSize = 50
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	// No branch matches, so the tick is the repository stage alone: the pages counted below are the
	// initialisation check's, not a branch tick's or the compaction count's.
	var counter *countingLakeFS
	next := newEnvOn(t, e.fake, func(opts *indexer.Options) {
		opts.Repositories = []indexer.Repository{{Name: "repo", Branches: []string{"no-such-branch"}}}
		counter = &countingLakeFS{LakeFS: opts.LakeFS}
		opts.LakeFS = counter
	})
	next.tick()

	require.ErrorIs(t, next.health.repositoryError("repo"), indexer.ErrForeignRepository)
	require.Equal(t, 1, counter.pages, "the walk stops when the answer is known instead of reading the whole history")
}

// TestInitialisationCheckKeepsTheHistoryOutOfMemory pins the other half of TS-5: the walk streams the
// first-parent log of R-metadata instead of collecting it. R-metadata gets one indexing commit per
// indexed source commit, so a repository that indexed a million commits kept every one of them - a
// few hundred megabytes - resident before its first branch could be indexed. The walk now holds one
// page, so the live heap stays flat however long the history is.
//
// The heap is sampled after every page rather than after the call: what the call keeps while it runs
// is the defect, and the history it collected is unreachable - and so invisible to a measurement -
// once it returns.
func TestInitialisationCheckKeepsTheHistoryOutOfMemory(t *testing.T) {
	const (
		history = 100_000 // ~10MB of client.Commit values if the walk collects them
		// The walk holds one page of 1000 commits plus the tick's own state.
		maxPeak = 4 << 20
	)

	e := newEnv(t)
	root := e.fake.addNonBareMetadataRepository()
	parent := e.fake.addInitCommit("init-1", root)
	for i := range history {
		parent = e.fake.addCommit("repo-metadata", "main", fmt.Sprintf("indexed-%06d", i), []string{parent}, nil).ID
	}
	e.fake.pageSize = 1_000
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})

	// No branch matches, so the tick is the repository stage alone and the metadata log is read by
	// the initialisation check only.
	var sampler *heapSamplingLakeFS
	next := newEnvOn(t, e.fake, func(opts *indexer.Options) {
		opts.Repositories = []indexer.Repository{{Name: "repo", Branches: []string{"no-such-branch"}}}
		sampler = &heapSamplingLakeFS{LakeFS: opts.LakeFS}
		opts.LakeFS = sampler
	})
	baseline := liveHeap()
	next.tick()
	growth := int64(sampler.peak) - int64(baseline)

	require.NoError(t, next.health.repositoryError("repo"))
	require.NotZero(t, sampler.samples, "the history is long enough to page")
	require.Less(t, growth, int64(maxPeak),
		"the initialisation check must not keep the %d commits of R-metadata's history in memory (peak growth %d bytes)",
		history, growth)
}

// heapSamplingLakeFS records the largest live heap seen while the metadata log is read, i.e. what the
// walk keeps while it runs.
type heapSamplingLakeFS struct {
	client.LakeFS
	peak    uint64
	samples int
}

func (h *heapSamplingLakeFS) LogCommits(ctx context.Context, repository, ref string, opts client.LogCommitsOptions) ([]client.Commit, bool, error) {
	page, more, err := h.LakeFS.LogCommits(ctx, repository, ref, opts)
	if repository == "repo-metadata" {
		h.samples++
		h.peak = max(h.peak, liveHeap())
	}
	return page, more, err
}

// liveHeap returns the live heap after a collection, so a test measures what is kept and not what
// was allocated on the way.
func liveHeap() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// competingReplica is the second indexer process of design §4.1 that meets the first one on a
// freshly created companion repository (IO-1). lakeFS keeps one staging area per branch, so the
// table creation the first process staged can be committed by either process and by only one of
// them: this wrapper commits it first, as the other replica would, and the process under test then
// finds nothing staged. The commit is made with AllowEmpty: true because that is what the indexer
// did before the fix, and lakeFS accepts the empty change set under that flag
// (pkg/graveler/graveler.go:2355); an indexer that does not pass it commits nothing and adopts the
// commit instead.
type competingReplica struct {
	client.LakeFS
	competed bool
}

func (r *competingReplica) Commit(ctx context.Context, repository, branch string, opts client.CommitOptions) (client.Commit, error) {
	if !r.competed && repository == "repo-metadata" && opts.Message == indexer.InitCommitMessage {
		r.competed = true
		if _, err := r.LakeFS.Commit(ctx, repository, branch, client.CommitOptions{
			Message:    indexer.InitCommitMessage,
			Metadata:   opts.Metadata,
			AllowEmpty: true,
		}); err != nil {
			return client.Commit{}, err
		}
	}
	return r.LakeFS.Commit(ctx, repository, branch, opts)
}

// countingLakeFS counts the pages of the first-parent log of R-metadata a run reads.
type countingLakeFS struct {
	client.LakeFS
	pages int
}

func (c *countingLakeFS) LogCommits(ctx context.Context, repository, ref string, opts client.LogCommitsOptions) ([]client.Commit, bool, error) {
	if repository == "repo-metadata" {
		c.pages++
	}
	return c.LakeFS.LogCommits(ctx, repository, ref, opts)
}

// newEnvOn builds an indexer over an existing fake lakeFS: a second process started on the state a
// first one left, which newEnv cannot express because it makes a fresh fake.
func newEnvOn(t *testing.T, f *fake, configure ...func(*indexer.Options)) *env {
	t.Helper()
	e := &env{t: t, fake: f, health: newFakeHealth(), metrics: newFakeMetrics()}
	opts := indexer.Options{
		LakeFS:       f,
		Tables:       f,
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
	e.indexer = idx
	return e
}

// initCommits returns the mds: init commits the fake holds for its metadata repository.
func initCommits(f *fake) []client.Commit {
	f.mu.Lock()
	defer f.mu.Unlock()
	var commits []client.Commit
	for _, commit := range f.commits[f.metadataRepo] {
		if commit.Message == indexer.InitCommitMessage {
			commits = append(commits, commit)
		}
	}
	return commits
}

// addNonBareMetadataRepository makes R-metadata exist as the indexer's CreateRepository leaves it:
// non-bare, so its default branch starts with lakeFS's automatic "Repository created" root. It
// returns that root's commit ID.
func (f *fake) addNonBareMetadataRepository() string {
	f.addRepository("repo-metadata", "s3://bucket/data-metadata", "main")
	root := f.addCommit("repo-metadata", "main", "created", nil, nil)
	root.Message = "Repository created"
	f.setCommit("repo-metadata", root)
	return root.ID
}

// addInitCommit adds an mds: init commit of §4.8 on top of parent, with the empty table state the
// initialisation committed, and returns its ID. A second one on top of the first is the history an
// indexer before the IO-1 fix left behind: lakeFS accepts an empty commit under AllowEmpty, so the
// loser of the race wrote a marker commit that carried no change.
func (f *fake) addInitCommit(id, parent string) string {
	commit := f.addCommit("repo-metadata", "main", id, []string{parent}, nil)
	commit.Message = indexer.InitCommitMessage
	commit.Metadata = map[string]string{
		indexer.MetadataKeyInit:             indexer.MetadataKeyInitValue,
		indexer.MetadataKeySourceRepository: "repo",
	}
	f.setCommit("repo-metadata", commit)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tableExists = true
	f.tableAt[id] = newState()
	return id
}

// addBareMetadataRepository makes R-metadata exist as an older indexer left it: created bare, so its
// initialisation commit - message mds: init, metadata lakefs.mds.init=true - is the root of its
// default branch and that commit carries the empty system.object_metadata table (§3.3, §4.8).
//
// The fake's CreateRepository cannot produce this shape because real lakeFS creates a non-bare
// repository with the automatic "Repository created" root (pkg/catalog/catalog.go,
// CreateBareRepository for the bare form), so the state is built here instead. It returns the
// initialisation commit ID.
func (f *fake) addBareMetadataRepository(namespace string) string {
	// addRepository registers the name as the fake's companion repository, which is what its
	// TableStore methods address.
	f.addRepository("repo-metadata", namespace, "main")
	init := f.addCommit("repo-metadata", "main", "init", nil, nil)
	init.Message = indexer.InitCommitMessage
	init.Metadata = map[string]string{
		indexer.MetadataKeyInit:             indexer.MetadataKeyInitValue,
		indexer.MetadataKeySourceRepository: "repo",
	}
	f.setCommit("repo-metadata", init)
	f.mu.Lock()
	defer f.mu.Unlock()
	// The initialisation commit created the table: it exists on the branch and holds no rows (§4.8).
	f.tableExists = true
	f.tableAt[init.ID] = newState()
	return init.ID
}

// TestReadOnlyMetadataRepositoryFailsTheRepository pins TS-4: a companion repository lakeFS reports
// as read-only cannot take the indexing commit or a reset (§4.4 steps 4-5, §4.7), so every branch of
// it would report the same 403 on every tick and /_health would stay 503. The repository is refused
// once per tick, before any branch is touched, with a message that names the cause and the fix.
func TestReadOnlyMetadataRepositoryFailsTheRepository(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []client.ObjectStat{object("a", 10)})
	e.tick()
	require.NoError(t, e.health.repositoryError("repo"))

	// R-metadata created with read_only: true, or flagged later by an operator.
	e.fake.setReadOnly("repo-metadata", true)
	e.fake.addCommit("repo", "main", "s2", []string{"s1"},
		[]client.ObjectStat{object("a", 10), object("b", 20)})
	e.fake.setDiff("repo", "s1", "s2", []client.Change{added("b", 20)})
	made := len(e.fake.madeCommits())

	e.tick()

	err := e.health.repositoryError("repo")
	require.ErrorIs(t, err, indexer.ErrReadOnlyMetadataRepository)
	require.Contains(t, err.Error(), "read_only=false", "the error names the reason and the fix")
	require.Contains(t, err.Error(), "COMMIT", "and the protection rule that would do the same")
	require.NoError(t, e.health.branch("repo", "main").err, "no branch collects a 403 of its own")
	require.Len(t, e.fake.madeCommits(), made, "nothing is written into a read-only repository")
	require.Equal(t, []string{"repo/" + indexer.StageRepository}, e.metrics.errors,
		"one repository error per tick, not one per branch")
	require.Empty(t, e.fake.resetCalls())

	// Clearing the flag lets the next tick continue from the recorded position.
	e.fake.setReadOnly("repo-metadata", false)
	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.NoError(t, e.health.branch("repo", "main").err)
	require.Equal(t, []string{"a", "b"}, e.paths("main"))
	require.Equal(t, "s2", e.health.branch("repo", "main").lastIndexed)
}
