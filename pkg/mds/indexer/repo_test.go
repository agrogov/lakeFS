package indexer_test

// repo_test.go covers design §3.3 and §4.8 on the companion repository: the storage namespace
// derivation and its refusals, the creation of R-metadata and its initialisation commit, and the
// refusal of a repository the indexer did not create.

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	lakefscatalog "github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/reader"
	"github.com/treeverse/lakefs/pkg/mds/table"
	"github.com/treeverse/lakefs/pkg/upload"
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
			name:          "bucket root appends to the bucket name",
			dataNamespace: "s3://bucket",
			want:          "s3://bucket-metadata",
		},
		{
			name:          "bucket root with a slash appends to the bucket name",
			dataNamespace: "s3://bucket/",
			want:          "s3://bucket-metadata",
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
		{
			name:          "azure path derives the companion in the same container",
			dataNamespace: "https://acct.blob.core.windows.net/data/repo",
			want:          "https://acct.blob.core.windows.net/data/repo-metadata",
		},
		{
			name:          "azure container root appends to the container name",
			dataNamespace: "https://acct.blob.core.windows.net/data",
			want:          "https://acct.blob.core.windows.net/data-metadata",
		},
		{
			name:          "azure container root with the http scheme appends to the container name",
			dataNamespace: "http://acct.azurite.test/data",
			want:          "http://acct.azurite.test/data-metadata",
		},
		{
			name:          "a data repository without a namespace has nothing to append to",
			dataNamespace: "",
			wantErr:       indexer.ErrNoStorageNamespace,
		},
		{
			name:          "azure container root with an explicit namespace is fine",
			dataNamespace: "https://acct.blob.core.windows.net/data",
			configured:    "https://acct.blob.core.windows.net/index/repo-metadata",
			want:          "https://acct.blob.core.windows.net/index/repo-metadata",
		},
		{
			name:          "azure companion that contains the data namespace overlaps",
			dataNamespace: "https://acct.blob.core.windows.net/data/repo",
			configured:    "https://acct.blob.core.windows.net/data",
			wantErr:       indexer.ErrOverlappingNamespace,
		},
		{
			name:          "azure companion on the other scheme of the data prefix overlaps",
			dataNamespace: "https://acct.blob.core.windows.net/data/repo",
			configured:    "http://acct.blob.core.windows.net/data/repo/metadata",
			wantErr:       indexer.ErrOverlappingNamespace,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := newEnv(t, func(opts *indexer.Options) {
				opts.Repositories[0].StorageNamespace = test.configured
			})
			e.fake.repositories["repo"] = reader.Repository{
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
			e.fake.repositories["repo"] = reader.Repository{
				Name:             "repo",
				StorageNamespace: test.dataNamespace,
				DefaultBranch:    "main",
				StorageID:        "storage-repo",
			}
			e.fake.addBareMetadataRepository(test.metadataNamespace)
			e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Empty(t, e.fake.createdRepositories, "an existing initialised repository is reused")
	require.Equal(t, init, e.fake.initCommitID(), "the root stays the initialisation commit")
	root := e.fake.commits["repo-metadata"][init]
	require.Empty(t, root.Parents, "the bare companion's initialisation commit is its root")
	// The shape the catalog's ownership proof reads (pkg/icebergcatalog/resolver.go,
	// findInitCommit): a root commit that carries the marker. The committer is not part of the
	// proof (X4: the sync may run as another user), so only the marker and the position matter.
	require.Equal(t, indexer.MetadataKeyInitValue, root.Metadata[indexer.MetadataKeyInit])
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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

	e.tick()

	require.ErrorIs(t, e.health.repositoryError("gone"), reader.ErrNotFound)
	require.NoError(t, e.health.repositoryError("repo"))
	require.Equal(t, 1, e.metrics.indexed["repo"])
}

// TestPersistentBadRequestOnTheInitialisationCommitDoesNotRecurse pins R2I-1: a 400 from the
// initialisation commit is "lost the race" only when re-verification adopts another replica's
// commit. A branch that still shows only lakeFS's automatic root has nothing to adopt, so the 400
// must be returned as it is instead of re-entering initialise, which recursed without a bound and
// re-wrapped the same error at every level.
type persistentInitRejection struct {
	reader.LakeFS
	attempts int
}

func (r *persistentInitRejection) Commit(ctx context.Context, repository, branch string, opts reader.CommitOptions) (reader.Commit, error) {
	if repository == "repo-metadata" && opts.Message == indexer.InitCommitMessage {
		r.attempts++
		if r.attempts > 25 {
			// Bound the probe so the pre-fix recursion ends with a visible failure instead of a
			// stack or memory exhaustion.
			return reader.Commit{}, fmt.Errorf("probe: gave up after %d initialisation attempts", r.attempts)
		}
		return reader.Commit{}, fmt.Errorf("commit %q@%s: block: write failed: %w", repository, branch, reader.ErrBadRequest)
	}
	return r.LakeFS.Commit(ctx, repository, branch, opts)
}

func TestPersistentBadRequestOnTheInitialisationCommitDoesNotRecurse(t *testing.T) {
	var rejecter *persistentInitRejection
	e := newEnv(t, func(opts *indexer.Options) {
		rejecter = &persistentInitRejection{LakeFS: opts.LakeFS}
		opts.LakeFS = rejecter
	})
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

	e.tick()

	err := e.health.repositoryError("repo")
	require.ErrorIs(t, err, reader.ErrBadRequest)
	require.Contains(t, err.Error(), "block: write failed", "the original 400 is reported")
	require.Equal(t, 1, rejecter.attempts, "a 400 with nothing to adopt is not retried")
	require.Equal(t, 1, e.fake.createTableCalls, "initialise does not re-enter CreateTable")
	require.Empty(t, initCommits(e.fake), "no initialisation commit is written")
}

// TestInitialisationRefusesAForeignStagedTable pins R2I-4: the ErrTableExists arm used to commit
// whatever table was staged, so a principal that staged system.object_metadata on a fresh
// R-metadata supplied the location, schema and properties the indexer then sealed with mds: init.
// The staged table must record lakefs.mds.repository = R (or it is refused as a foreign
// repository, exactly as a foreign history is, §4.8).
func TestInitialisationRefusesAForeignStagedTable(t *testing.T) {
	tests := []struct {
		name       string
		repository string
	}{
		{name: "another data repository", repository: "someone-else"},
		{name: "no repository property", repository: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := newEnv(t)
			e.fake.addNonBareMetadataRepository()
			// The table is staged, which is what ErrTableExists reports: CreateTable finds it and the
			// initialisation must load and inspect it instead of replacing it.
			e.fake.tableExists = true
			foreign := newState()
			foreign.repository = test.repository
			foreign.rows["planted"] = table.Row{Repository: test.repository, Path: "planted", CommitID: "planted"}
			e.fake.stage("main", foreign)
			e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

			e.tick()

			require.ErrorIs(t, e.health.repositoryError("repo"), indexer.ErrForeignRepository)
			require.Empty(t, initCommits(e.fake), "the mds: init marker is not sealed on a foreign table")
			require.Empty(t, e.fake.madeCommits(), "nothing is committed for a foreign table")
		})
	}
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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
		e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
		e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
		history = 100_000 // ~10MB of reader.Commit values if the walk collects them
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
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

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
	reader.LakeFS
	peak    uint64
	samples int
}

func (h *heapSamplingLakeFS) LogCommits(ctx context.Context, repository, ref string, opts reader.LogCommitsOptions) ([]reader.Commit, bool, error) {
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
	reader.LakeFS
	competed bool
}

func (r *competingReplica) Commit(ctx context.Context, repository, branch string, opts reader.CommitOptions) (reader.Commit, error) {
	if !r.competed && repository == "repo-metadata" && opts.Message == indexer.InitCommitMessage {
		r.competed = true
		if _, err := r.LakeFS.Commit(ctx, repository, branch, reader.CommitOptions{
			Message:    indexer.InitCommitMessage,
			Metadata:   opts.Metadata,
			AllowEmpty: true,
		}); err != nil {
			return reader.Commit{}, err
		}
	}
	return r.LakeFS.Commit(ctx, repository, branch, opts)
}

// countingLakeFS counts the pages of the first-parent log of R-metadata a run reads.
type countingLakeFS struct {
	reader.LakeFS
	pages int
}

func (c *countingLakeFS) LogCommits(ctx context.Context, repository, ref string, opts reader.LogCommitsOptions) ([]reader.Commit, bool, error) {
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
func initCommits(f *fake) []reader.Commit {
	f.mu.Lock()
	defer f.mu.Unlock()
	var commits []reader.Commit
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
	commit.Committer = fakeIndexerPrincipal
	commit.Metadata = map[string]string{
		indexer.MetadataKeyInit:             indexer.MetadataKeyInitValue,
		indexer.MetadataKeySourceRepository: "repo",
	}
	f.setCommit("repo-metadata", commit)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tableExists = true
	state := newState()
	state.repository = "repo"
	f.tableAt[id] = state
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
	init.Committer = fakeIndexerPrincipal
	init.Metadata = map[string]string{
		indexer.MetadataKeyInit:             indexer.MetadataKeyInitValue,
		indexer.MetadataKeySourceRepository: "repo",
	}
	f.setCommit("repo-metadata", init)
	f.mu.Lock()
	defer f.mu.Unlock()
	// The initialisation commit created the table: it exists on the branch and holds no rows (§4.8).
	f.tableExists = true
	state := newState()
	state.repository = "repo"
	f.tableAt[init.ID] = state
	return init.ID
}

// TestProtectedMetadataBranchFailsTheRepository pins R2I-3: a COMMIT- or STAGING_WRITE-protected
// companion branch is refused like a read-only companion repository. The repository record does not
// carry the rule, so the refusal is detected at the first write lakeFS rejects and answered with
// the ErrReadOnlyMetadataRepository family, naming branch protection instead of leaving the raw 403
// on every tick (design §3.3, §7, TS-4).
func TestProtectedMetadataBranchFailsTheRepository(t *testing.T) {
	t.Run("commit protection", func(t *testing.T) {
		e := newEnv(t)
		e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
		e.tick()
		require.NoError(t, e.health.repositoryError("repo"))

		e.fake.protectBranch("repo-metadata", "main", true, false)
		e.fake.addCommit("repo", "main", "s2", []string{"s1"},
			[]reader.ObjectStat{object("a", 10), object("b", 20)})
		e.fake.setDiff("repo", "s1", "s2", []reader.Change{added("b", 20)})
		made := len(e.fake.madeCommits())

		e.tick()

		err := e.health.branch("repo", "main").err
		require.ErrorIs(t, err, indexer.ErrReadOnlyMetadataRepository)
		require.Contains(t, err.Error(), "branch protection")
		require.Contains(t, err.Error(), "COMMIT")
		require.Contains(t, err.Error(), "cannot commit to protected branch", "the 403 stays in the chain")
		require.Len(t, e.fake.madeCommits(), made, "the protected branch takes no indexing commit")
	})

	t.Run("staging write protection", func(t *testing.T) {
		e := newEnv(t)
		e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
		e.tick()
		require.NoError(t, e.health.repositoryError("repo"))

		e.fake.protectBranch("repo-metadata", "main", false, true)
		e.fake.addCommit("repo", "main", "s2", []string{"s1"},
			[]reader.ObjectStat{object("a", 10), object("b", 20)})
		e.fake.setDiff("repo", "s1", "s2", []reader.Change{added("b", 20)})

		e.tick()

		err := e.health.branch("repo", "main").err
		require.ErrorIs(t, err, indexer.ErrReadOnlyMetadataRepository)
		require.Contains(t, err.Error(), "branch protection")
		require.Contains(t, err.Error(), "STAGING_WRITE")
		require.Contains(t, err.Error(), "cannot write to protected branch", "the 403 stays in the chain")
		require.ErrorIs(t, err, rest.ErrForbidden,
			"the refusal reaches the indexer as the catalog's sentinel, not as the lakeFS API's (R3I-5)")
	})

	t.Run("staging write protection before the initialisation", func(t *testing.T) {
		// §3.3, §4.8: a STAGING_WRITE rule present before the first tick is met at the catalog's
		// CreateTable, one step before the initialisation commit, and used to escape the family
		// (R3I-2). The rule blocks only staging, which the COMMIT-protection case above never meets.
		e := newEnv(t)
		e.fake.addNonBareMetadataRepository()
		e.fake.protectBranch("repo-metadata", "main", false, true)
		e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

		e.tick()

		err := e.health.repositoryError("repo")
		require.ErrorIs(t, err, indexer.ErrReadOnlyMetadataRepository,
			"the create refusal is a repository-level error, not a bare 403")
		require.Contains(t, err.Error(), "branch protection")
		require.Contains(t, err.Error(), "STAGING_WRITE")
		require.Contains(t, err.Error(), "create the metadata table", "the refusal names the write that met it")
		require.Contains(t, err.Error(), "cannot write to protected branch", "the 403 stays in the chain")
		require.False(t, e.fake.tableExists, "no table pointer is staged")
		require.Empty(t, e.health.branch("repo", "main").head, "no branch of the repository is touched")
		require.Empty(t, e.fake.madeCommits(), "a protected repository is refused before a commit")
	})

	t.Run("before the initialisation commit", func(t *testing.T) {
		e := newEnv(t)
		e.fake.addNonBareMetadataRepository()
		e.fake.protectBranch("repo-metadata", "main", true, false)
		e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

		e.tick()

		err := e.health.repositoryError("repo")
		require.ErrorIs(t, err, indexer.ErrReadOnlyMetadataRepository)
		require.Contains(t, err.Error(), "branch protection")
		require.Empty(t, e.fake.madeCommits(), "a protected repository is refused before a commit")
		require.Empty(t, e.health.branch("repo", "main").head, "no branch of the repository is touched")
	})
}

// TestForbiddenMetadataBranchWriteIsNotBranchProtection pins R3I-1 at the branch tick: an
// authorization refusal answers the same forbidden family as a protection rule (pkg/mds/reader
// maps the protection refusals into reader.ErrForbidden), and the catalog answers 403 for a
// permission denial as well. The branch must still
// report the ErrReadOnlyMetadataRepository family - a companion repository it cannot write is not a
// transient branch error - but the message must not claim branch protection, because that sends an
// operator looking for a rule that does not exist instead of at the indexer principal's policy
// (design §8: the expected state while the policy refresher has not covered a new repository).
func TestForbiddenMetadataBranchWriteIsNotBranchProtection(t *testing.T) {
	t.Run("the lakeFS API's 401 authorization denial", func(t *testing.T) {
		e := newEnv(t)
		e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
		e.tick()
		require.NoError(t, e.health.repositoryError("repo"))

		e.fake.addCommit("repo", "main", "s2", []string{"s1"},
			[]reader.ObjectStat{object("a", 10), object("b", 20)})
		e.fake.setDiff("repo", "s1", "s2", []reader.Change{added("b", 20)})
		e.fake.applyErr = fmt.Errorf("apply repo@s2: %w: permission denied: no policy grants fs:WriteObject", reader.ErrForbidden)

		e.tick()

		err := e.health.branch("repo", "main").err
		require.ErrorIs(t, err, indexer.ErrReadOnlyMetadataRepository, "the refusal still fails the write")
		require.ErrorIs(t, err, reader.ErrForbidden)
		require.NotContains(t, err.Error(), "a branch protection rule blocks",
			"branch protection is not what lakeFS answered")
		require.NotContains(t, err.Error(), "remove the rule")
		require.NotContains(t, err.Error(), "read_only=false", "and the destructive remedy is not offered")
		require.Contains(t, err.Error(), "branch protection", "the neutral text still names the candidates")
		require.Contains(t, err.Error(), "read_only")
		require.Contains(t, err.Error(), "permissions")
		require.Contains(t, err.Error(), "no policy grants fs:WriteObject", "the cause is quoted")
	})

	t.Run("the catalog's 403 permission refusal", func(t *testing.T) {
		e := newEnv(t)
		e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
		e.tick()

		e.fake.addCommit("repo", "main", "s2", []string{"s1"},
			[]reader.ObjectStat{object("a", 10), object("b", 20)})
		e.fake.setDiff("repo", "s1", "s2", []reader.Change{added("b", 20)})
		e.fake.applyErr = fmt.Errorf("apply repo@s2: %w: ForbiddenException: permission denied for catalog:WriteTableData", rest.ErrForbidden)

		e.tick()

		err := e.health.branch("repo", "main").err
		require.ErrorIs(t, err, indexer.ErrReadOnlyMetadataRepository)
		require.ErrorIs(t, err, rest.ErrForbidden)
		require.NotContains(t, err.Error(), "a branch protection rule blocks")
		require.NotContains(t, err.Error(), "remove the rule")
		require.Contains(t, err.Error(), "catalog:WriteTableData")
	})
}

// catalogCreateRefusal answers CreateTable with a fixed error, the way the production store reaches
// the indexer: through the Iceberg REST catalog, so a refused create arrives as rest.ErrForbidden
// (iceberg-go catalog/rest/rest.go).
type catalogCreateRefusal struct {
	indexer.TableStore
	err error
}

func (s catalogCreateRefusal) CreateTable(context.Context, string, string) (indexer.Table, error) {
	return nil, s.err
}

// TestInitialisationCreateRefusalIsTheReadOnlyFamily pins R3I-2: the catalog's CreateTable is a
// staging write, so it meets a protection rule before the initialisation commit does. It used to
// return a bare 403 out of initialise, outside the ErrReadOnlyMetadataRepository family: the
// documented pre-first-tick state of design §3.3 and §4.8 for a STAGING_WRITE rule was reported as
// an unknown failure while the COMMIT-rule shape was diagnosed.
func TestInitialisationCreateRefusalIsTheReadOnlyFamily(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantClaim bool
	}{
		{
			name:      "a protected branch",
			body:      "ForbiddenException: cannot write to protected branch",
			wantClaim: true,
		},
		{
			name:      "an authorization refusal",
			body:      "ForbiddenException: permission denied for catalog:CreateTable",
			wantClaim: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := newEnv(t, func(opts *indexer.Options) {
				opts.Tables = catalogCreateRefusal{
					TableStore: opts.Tables,
					err:        fmt.Errorf("create %s.%s: %w: %s", table.Namespace, table.Name, rest.ErrForbidden, test.body),
				}
			})
			e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})

			e.tick()

			err := e.health.repositoryError("repo")
			require.ErrorIs(t, err, indexer.ErrReadOnlyMetadataRepository, "the create refusal is in the family")
			require.ErrorIs(t, err, rest.ErrForbidden, "the catalog's cause stays in the chain")
			require.Contains(t, err.Error(), "create the metadata table", "the refusal names the write that met it")
			if test.wantClaim {
				require.Contains(t, err.Error(), "a branch protection rule blocks")
				require.Contains(t, err.Error(), "STAGING_WRITE")
			} else {
				require.NotContains(t, err.Error(), "a branch protection rule blocks")
				require.Contains(t, err.Error(), "permissions")
			}
			require.Empty(t, e.fake.madeCommits(), "the initialisation is refused before its commit")
		})
	}
}

// TestReadOnlyMetadataRepositoryFailsTheRepository pins TS-4: a companion repository lakeFS reports
// as read-only cannot take the indexing commit or a reset (§4.4 steps 4-5, §4.7), so every branch of
// it would report the same 403 on every tick and /_health would stay 503. The repository is refused
// once per tick, before any branch is touched, with a message that names the cause and the fix.
func TestReadOnlyMetadataRepositoryFailsTheRepository(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
	e.tick()
	require.NoError(t, e.health.repositoryError("repo"))

	// R-metadata created with read_only: true, or flagged later by an operator.
	e.fake.setReadOnly("repo-metadata", true)
	e.fake.addCommit("repo", "main", "s2", []string{"s1"},
		[]reader.ObjectStat{object("a", 10), object("b", 20)})
	e.fake.setDiff("repo", "s1", "s2", []reader.Change{added("b", 20)})
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

// TestRecreatedCompanionGetsItsInitialisationCommitAgain pins the stale initialisation cache of
// design §6: deleting R and R-metadata with delete_storage and re-creating both under the same names
// inside one process used to leave the per-process `initialised` flag set, so the second companion
// was never initialised and failed the catalog's init-commit proof. The fix resets the flag whenever
// a missing companion is about to be created.
func TestRecreatedCompanionGetsItsInitialisationCommitAgain(t *testing.T) {
	e := newEnv(t)
	e.fake.addCommit("repo", "main", "s1", nil, []reader.ObjectStat{object("a", 10)})
	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	first := e.fake.initCommitID()
	require.NotEmpty(t, first, "the first companion is initialised")
	require.Len(t, e.fake.createdRepositories, 1)

	// The cascade deleted both repositories, and both are re-created under the same names before
	// this process restarts: the second companion is a fresh, empty repository.
	e.fake.deleteRepository("repo-metadata")
	e.fake.deleteRepository("repo")
	e.fake.addRepository("repo", "s3://bucket/data", "main")
	e.fake.addCommit("repo", "main", "s2", nil, []reader.ObjectStat{object("b", 20)})

	e.tick()

	require.NoError(t, e.health.repositoryError("repo"))
	require.Len(t, e.fake.createdRepositories, 2, "the companion is created again")
	second := e.fake.initCommitID()
	require.NotEmpty(t, second, "the re-created companion is initialised again, whatever the process remembers")
	require.NotEqual(t, first, second)
	init := e.fake.commits["repo-metadata"][second]
	require.Equal(t, indexer.MetadataKeyInitValue, init.Metadata[indexer.MetadataKeyInit])
	require.Equal(t, "repo", init.Metadata[indexer.MetadataKeySourceRepository])
	root := e.fake.commits["repo-metadata"][init.Parents[0]]
	require.Empty(t, root.Parents, "the initialisation sits directly above the automatic creation root")
}

// TestCompanionCreateIsRefusedWhileItsDeletionIsPending pins design §6's indexer interaction and R16
// through the indexer's own client (the in-process reader): a tick that read R just before R and its
// companion were deleted with storage tries to create R-metadata again while that name's deletion
// record is pending, and the catalog refuses the create.
func TestCompanionCreateIsRefusedWhileItsDeletionIsPending(t *testing.T) {
	ctx := t.Context()
	cat := newIndexerCatalog(t)

	const (
		dataRepository      = "repo"
		companionRepository = "repo-metadata"
		dataNamespace       = "mem://pending/data"
		companionNamespace  = "mem://pending/data-metadata"
	)
	_, err := cat.CreateRepository(ctx, dataRepository, "", dataNamespace, "main", false)
	require.NoError(t, err, "create the data repository")
	_, err = cat.CreateRepository(ctx, companionRepository, "", companionNamespace, "main", false)
	require.NoError(t, err, "create the companion repository before its deletion")

	// The cascade deleted R-metadata's metadata and left its deletion record pending: the exact
	// window in which a tick that already read R still tries to create the companion.
	require.NoError(t, cat.CreateRepositoryDeletion(ctx, &lakefscatalog.RepositoryDeletion{
		Repository:       companionRepository,
		StorageNamespace: companionNamespace,
		State:            lakefscatalog.RepositoryDeletionState_DELETING,
		Step:             lakefscatalog.RepositoryDeletionStep_OBJECTS,
	}))
	require.NoError(t, cat.DeleteRepository(ctx, companionRepository, graveler.WithForce(true)))

	health := newFakeHealth()
	idx, err := indexer.New(indexer.Options{
		LakeFS:       reader.New(cat, 10),
		Tables:       newFake(),
		Repositories: []indexer.Repository{{Name: dataRepository, Branches: []string{"main"}}},
		Health:       health,
		Log:          logging.ContextUnavailable(),
	})
	require.NoError(t, err, "build the indexer over the in-process reader")

	require.NoError(t, idx.Tick(ctx))
	err = health.repositoryError(dataRepository)
	require.ErrorIs(t, err, lakefscatalog.ErrDeletionInProgress,
		"the companion create is refused while R's deletion is pending (R16)")
	require.Contains(t, err.Error(), companionRepository, "the refusal names the repository whose deletion is pending")
}

// newIndexerCatalog builds the lakeFS catalog the in-process reader of a test runs on: a mem KV store
// and the mem block adapter, with no repository cache so a deleted repository is gone immediately.
func newIndexerCatalog(t *testing.T) *lakefscatalog.Catalog {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	viper.Set("graveler.repository_cache.size", 0)

	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err, "build the lakeFS configuration")
	cfg.Committed.LocalCache.Dir = t.TempDir()

	kvStore, err := kv.Open(t.Context(), kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open the mem KV store")
	t.Cleanup(kvStore.Close)

	cat, err := lakefscatalog.New(context.WithoutCancel(t.Context()), lakefscatalog.Config{
		Config:       cfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err, "build the lakeFS catalog")
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}
