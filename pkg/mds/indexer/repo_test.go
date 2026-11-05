package indexer_test

// repo_test.go covers design §3.3 and §4.8 on the companion repository: the storage namespace
// derivation and its refusals, the creation of R-metadata and its initialisation commit, and the
// refusal of a repository the indexer did not create.

import (
	"testing"

	"github.com/stretchr/testify/require"
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
