package reader_test

// catalog_test.go drives the in-process implementation of the seam over a real *catalog.Catalog:
// a mem KV store and the local block adapter, the same pieces `mds run` builds from the server
// configuration (RA4b). It pins the mapping from catalog types to the seam types - paging cursors,
// two-dot diffs with right-side stats, tag and commit reads - and the error mapping the indexer
// reacts to (not found, forbidden, conflict, no changes, no merge base).

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	lakefscatalog "github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/mds/reader"
	"github.com/treeverse/lakefs/pkg/mds/table"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	testRepository = "data"
	testBranch     = "main"
)

// env is one lakeFS catalog over a mem KV store and the local block adapter, and the reader over
// it.
type env struct {
	t    *testing.T
	cat  *lakefscatalog.Catalog
	read *reader.Catalog
	// namespace is the data repository's storage namespace, as it was created.
	namespace string
	// pageSize is the reader's page size: 1 makes every listing page through its cursor.
	pageSize int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := t.Context()

	root := t.TempDir()
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)
	viper.Set("blockstore.local.path", root)
	viper.Set("committed.local_cache.size_bytes", 24*1024*1024)
	viper.Set("committed.sstable.memory.cache_size_bytes", 2*1024*1024)

	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err, "build the lakeFS configuration")
	cfg.Committed.LocalCache.Dir = filepath.Join(root, "cache")

	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open the mem KV store")
	t.Cleanup(kvStore.Close)

	cat, err := lakefscatalog.New(context.WithoutCancel(ctx), lakefscatalog.Config{
		Config:       cfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err, "build the lakeFS catalog")
	t.Cleanup(func() { _ = cat.Close() })

	namespace := "local://" + filepath.Join(root, "repos", testRepository)
	_, err = cat.CreateRepository(ctx, testRepository, "", namespace, testBranch, false)
	require.NoError(t, err, "create the data repository")

	return &env{t: t, cat: cat, read: reader.New(cat, 1), namespace: namespace, pageSize: 1}
}

// entry is one object of a commit. The table writer never reads the object, so a synthetic
// physical address is enough.
func (e *env) entry(path string, size int64, etag string) lakefscatalog.DBEntry {
	return lakefscatalog.DBEntry{
		Path:            path,
		PhysicalAddress: "local://" + filepath.Join(e.t.TempDir(), path),
		CreationDate:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Size:            size,
		Checksum:        etag,
		Metadata:        lakefscatalog.Metadata{"X-Amz-Meta-Owner": "alice"},
		ContentType:     "text/plain",
	}
}

// log reads every page of a commit log, so a test asserts on the whole walk whatever the reader's
// page size is.
func (e *env) log(ref string, opts reader.LogCommitsOptions) []reader.Commit {
	e.t.Helper()
	var commits []reader.Commit
	for {
		page, hasMore, err := e.read.LogCommits(e.t.Context(), testRepository, ref, opts)
		require.NoError(e.t, err)
		commits = append(commits, page...)
		if !hasMore {
			return commits
		}
		require.NotEmpty(e.t, page)
		opts.After = page[len(page)-1].ID
	}
}

// commit stages the entries on the branch (deleting the delete paths first) and commits them.
func (e *env) commit(message string, entries []lakefscatalog.DBEntry, deletes ...string) *lakefscatalog.CommitLog {
	e.t.Helper()
	ctx := e.t.Context()
	for _, path := range deletes {
		require.NoError(e.t, e.cat.DeleteEntry(ctx, testRepository, testBranch, path))
	}
	for _, entry := range entries {
		require.NoError(e.t, e.cat.CreateEntry(ctx, testRepository, testBranch, entry))
	}
	commit, err := e.cat.Commit(ctx, testRepository, testBranch, message, "tester", nil, nil, nil, false)
	require.NoError(e.t, err)
	return commit
}

func TestRepositoryBranchesAndCommits(t *testing.T) {
	ctx := t.Context()
	e := newEnv(t)

	repo, err := e.read.GetRepository(ctx, testRepository)
	require.NoError(t, err)
	require.Equal(t, testRepository, repo.Name)
	require.Equal(t, testBranch, repo.DefaultBranch)
	require.Equal(t, e.namespace, repo.StorageNamespace)
	require.False(t, repo.ReadOnly)

	first := e.commit("add a", []lakefscatalog.DBEntry{e.entry("a", 10, "etag-a")})
	second := e.commit("add b", []lakefscatalog.DBEntry{e.entry("b", 20, "etag-b")})

	// Branches page through the cursor one at a time.
	head, err := e.read.GetCommit(ctx, testRepository, testBranch)
	require.NoError(t, err)
	require.Equal(t, second.Reference, head.ID)
	require.Equal(t, []string{first.Reference}, head.Parents)

	branches, hasMore, err := e.read.ListBranches(ctx, testRepository, "")
	require.NoError(t, err)
	require.Equal(t, []reader.Branch{{Name: testBranch, CommitID: second.Reference}}, branches)
	require.False(t, hasMore, "one branch fits in one page")

	// The log is newest first; after and stop_at are the API's cursors.
	commits := e.log(testBranch, reader.LogCommitsOptions{FirstParent: true})
	require.Len(t, commits, 3, "two commits and the repository's root commit")
	require.Equal(t, second.Reference, commits[0].ID)
	require.Equal(t, first.Reference, commits[1].ID)
	require.Equal(t, "tester", commits[1].Committer)
	require.Equal(t, second.CreationDate, commits[0].CreationDate)

	// The walk is newest first, so the page after the first commit is the root commit - the cursor
	// the indexer's pending walk keeps as it pages towards the recorded position.
	commits = e.log(testBranch, reader.LogCommitsOptions{FirstParent: true, After: first.Reference})
	require.Len(t, commits, 1)
	require.Empty(t, commits[0].Parents)
	require.Equal(t, "Repository created", commits[0].Message)

	commits = e.log(testBranch, reader.LogCommitsOptions{FirstParent: true, StopAt: first.Reference})
	require.Len(t, commits, 2, "stop_at is inclusive, as the REST API's is")
	require.Equal(t, first.Reference, commits[1].ID)

	// A commit ID is a ref: GetCommit resolves it too.
	again, err := e.read.GetCommit(ctx, testRepository, first.Reference)
	require.NoError(t, err)
	require.Equal(t, first.Reference, again.ID)
}

func TestBranchesTagsAndMergeBase(t *testing.T) {
	ctx := t.Context()
	e := newEnv(t)

	first := e.commit("add a", []lakefscatalog.DBEntry{e.entry("a", 10, "etag-a")})
	require.NoError(t, e.read.CreateBranch(ctx, testRepository, "feature", testBranch))
	require.ErrorIs(t, e.read.CreateBranch(ctx, testRepository, "feature", testBranch), reader.ErrConflict,
		"an existing branch is a conflict")

	var branches []reader.Branch
	after := ""
	for {
		page, hasMore, err := e.read.ListBranches(ctx, testRepository, after)
		require.NoError(t, err)
		branches = append(branches, page...)
		if !hasMore {
			break
		}
		require.NotEmpty(t, page)
		after = page[len(page)-1].Name
	}
	require.Len(t, branches, 2)
	require.Equal(t, "feature", branches[0].Name, "branches are listed in name order")
	require.Equal(t, testBranch, branches[1].Name)

	base, err := e.read.FindMergeBase(ctx, testRepository, first.Reference, "feature")
	require.NoError(t, err)
	require.Equal(t, first.Reference, base)

	tag, err := e.read.CreateTag(ctx, testRepository, "v1", first.Reference, false)
	require.NoError(t, err)
	require.Equal(t, reader.Tag{Name: "v1", CommitID: first.Reference}, tag)
	_, err = e.read.CreateTag(ctx, testRepository, "v1", first.Reference, false)
	require.ErrorIs(t, err, reader.ErrConflict, "lakeFS never overwrites a tag")

	got, err := e.read.GetTag(ctx, testRepository, "v1")
	require.NoError(t, err)
	require.Equal(t, first.Reference, got.CommitID)

	tags, hasMore, err := e.read.ListTags(ctx, testRepository, "")
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, []reader.Tag{{Name: "v1", CommitID: first.Reference}}, tags)

	require.NoError(t, e.read.DeleteTag(ctx, testRepository, "v1"))
	_, err = e.read.GetTag(ctx, testRepository, "v1")
	require.ErrorIs(t, err, reader.ErrNotFound)

	require.NoError(t, e.read.DeleteBranch(ctx, testRepository, "feature"))
	_, err = e.read.GetCommit(ctx, testRepository, "feature")
	require.ErrorIs(t, err, reader.ErrNotFound)

	// A hard reset is how §4.7 rewrites the metadata branch; the branch resolves the ref.
	require.NoError(t, e.read.HardResetBranch(ctx, testRepository, testBranch, first.Reference))
	head, err := e.read.GetCommit(ctx, testRepository, testBranch)
	require.NoError(t, err)
	require.Equal(t, first.Reference, head.ID)
}

func TestObjectsDiffsAndStaging(t *testing.T) {
	ctx := t.Context()
	e := newEnv(t)

	first := e.commit("add a and b", []lakefscatalog.DBEntry{
		e.entry("a", 10, "etag-a"),
		e.entry("b", 20, "etag-b"),
	})
	second := e.commit("delete b, change a, add c", []lakefscatalog.DBEntry{
		e.entry("a", 11, "etag-a2"),
		e.entry("c", 30, "etag-c"),
	}, "b")

	// ListObjects pages through the cursor; the entry's metadata and content type are part of the
	// answer, because every row of the table carries them.
	var paths []string
	after := ""
	for {
		objects, hasMore, err := e.read.ListObjects(ctx, testRepository, second.Reference, after)
		require.NoError(t, err)
		for _, object := range objects {
			paths = append(paths, object.Path)
			require.Equal(t, "text/plain", object.ContentType)
			require.Equal(t, map[string]string{"X-Amz-Meta-Owner": "alice"}, object.UserMetadata)
		}
		if !hasMore {
			break
		}
		require.Len(t, objects, e.pageSize, "a full page means the cursor advances by its last path")
		after = objects[len(objects)-1].Path
	}
	require.Equal(t, []string{"a", "c"}, paths)

	stat, err := e.read.StatObject(ctx, testRepository, second.Reference, "a")
	require.NoError(t, err)
	require.Equal(t, reader.ObjectStat{
		Path:         "a",
		SizeBytes:    11,
		Mtime:        time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		ETag:         "etag-a2",
		ContentType:  "text/plain",
		UserMetadata: map[string]string{"X-Amz-Meta-Owner": "alice"},
	}, stat)
	_, err = e.read.StatObject(ctx, testRepository, second.Reference, "missing")
	require.ErrorIs(t, err, reader.ErrNotFound)

	// A two-dot diff of the two commits: a changed, b removed, c added - with a right-side stat
	// for the entries that have one and none for the removed one.
	var changes []reader.Change
	after = ""
	for {
		page, hasMore, err := e.read.DiffRefs(ctx, testRepository, first.Reference, second.Reference, after)
		require.NoError(t, err)
		changes = append(changes, page...)
		if !hasMore {
			break
		}
		require.NotEmpty(t, page)
		after = page[len(page)-1].Path
	}
	require.Len(t, changes, 3)
	require.Equal(t, reader.ChangeChanged, changes[0].Type)
	require.Equal(t, "a", changes[0].Path)
	require.NotNil(t, changes[0].Right)
	require.Equal(t, int64(11), changes[0].SizeBytes)
	require.Equal(t, "etag-a2", changes[0].Right.ETag)
	require.Equal(t, reader.ChangeRemoved, changes[1].Type)
	require.Equal(t, "b", changes[1].Path)
	require.Nil(t, changes[1].Right, "a removed entry has no right side")
	require.Equal(t, reader.ChangeAdded, changes[2].Type)
	require.Equal(t, "c", changes[2].Path)

	// A staged, uncommitted entry is a branch diff, which §4.4 reads to tell an empty staging
	// from a foreign staged write.
	require.NoError(t, e.cat.CreateEntry(ctx, testRepository, testBranch, e.entry("d", 40, "etag-d")))
	staged, _, err := e.read.DiffBranch(ctx, testRepository, testBranch, "")
	require.NoError(t, err)
	require.Len(t, staged, 1)
	require.Equal(t, "d", staged[0].Path)
	require.Equal(t, reader.ChangeAdded, staged[0].Type)
}

func TestCommitAsTheIndexerPrincipal(t *testing.T) {
	ctx := t.Context()
	e := newEnv(t)

	metaRepo := testRepository + icebergcatalog.MetadataRepositorySuffix
	_, err := e.read.CreateRepository(ctx, reader.RepositoryOptions{
		Name:             metaRepo,
		StorageNamespace: "local://" + filepath.Join(t.TempDir(), metaRepo),
		DefaultBranch:    testBranch,
	})
	require.NoError(t, err)

	// A duplicate create is a conflict, never an overwrite.
	_, err = e.read.CreateRepository(ctx, reader.RepositoryOptions{Name: metaRepo, StorageNamespace: "local://elsewhere"})
	require.ErrorIs(t, err, reader.ErrConflict)
	repo, err := e.read.GetRepository(ctx, metaRepo)
	require.NoError(t, err)
	require.NotEqual(t, "local://elsewhere", repo.StorageNamespace, "the conflict left the repository alone")

	require.NoError(t, e.cat.CreateEntry(ctx, metaRepo, testBranch, e.entry("_lakefs_iceberg/table", 1, "etag")))
	commit, err := e.read.Commit(ctx, metaRepo, testBranch, reader.CommitOptions{
		Message:    "mds: init",
		Metadata:   map[string]string{"lakefs.mds.init": "true"},
		AllowEmpty: false,
	})
	require.NoError(t, err)
	require.Equal(t, table.ServiceUserName, commit.Committer,
		"the in-process commit names the sync identity")
	require.Equal(t, map[string]string{"lakefs.mds.init": "true"}, commit.Metadata)
	require.NotEmpty(t, commit.ID)

	// An empty commit with allow_empty false is the bad request §4.3 step 4 relies on.
	_, err = e.read.Commit(ctx, metaRepo, testBranch, reader.CommitOptions{Message: "nothing", AllowEmpty: false})
	require.ErrorIs(t, err, reader.ErrBadRequest)
}

func TestForbiddenAndNotFound(t *testing.T) {
	ctx := t.Context()
	e := newEnv(t)

	_, err := e.read.GetRepository(ctx, "missing")
	require.ErrorIs(t, err, reader.ErrNotFound)
	var notFound *reader.NotFoundError
	require.True(t, errors.As(err, &notFound))
	require.Equal(t, "get repository", notFound.Operation)
	require.Equal(t, "missing", notFound.Subject)

	// Deleting the default branch is a forbidden write, not a bad request (the REST API answers
	// 403 too).
	require.ErrorIs(t, e.read.DeleteBranch(ctx, testRepository, testBranch), reader.ErrForbidden)

	// A read-only repository refuses every write with the same sentinel.
	readOnly := fmt.Sprintf("%s-readonly", testRepository)
	_, err = e.read.CreateRepository(ctx, reader.RepositoryOptions{
		Name:             readOnly,
		StorageNamespace: "local://" + filepath.Join(t.TempDir(), readOnly),
		DefaultBranch:    testBranch,
		ReadOnly:         true,
	})
	require.NoError(t, err)
	require.ErrorIs(t, e.read.CreateBranch(ctx, readOnly, "feature", testBranch), reader.ErrForbidden)
	_, err = e.read.Commit(ctx, readOnly, testBranch, reader.CommitOptions{Message: "x", AllowEmpty: true})
	require.ErrorIs(t, err, reader.ErrForbidden)
}
