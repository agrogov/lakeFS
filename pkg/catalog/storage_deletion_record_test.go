package catalog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/upload"
)

// newStorageDeletionTestCatalog builds a catalog on the mem KV store and the mem block adapter,
// with a context that survives the test so Catalog.Close is the only way background goroutines
// stop.
func newStorageDeletionTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	ctx := context.WithoutCancel(t.Context())
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err)
	cfg.Committed.LocalCache.Dir = t.TempDir()

	c, err := New(ctx, Config{
		Config:       cfg,
		KVStore:      kvtest.GetStore(t.Context(), t),
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// setRepositoryState changes a repository record's state in KV, so that tests can put a
// repository in IN_DELETION without running the metadata delete.
func setRepositoryState(t *testing.T, store kv.Store, repository string, state graveler.RepositoryState) {
	t.Helper()
	ctx := t.Context()
	data := &graveler.RepositoryData{}
	predicate, err := kv.GetMsg(ctx, store, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(graveler.RepositoryID(repository))), data)
	require.NoError(t, err)
	data.State = state
	require.NoError(t, kv.SetMsgIf(ctx, store, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(graveler.RepositoryID(repository))), data, predicate))
}

func TestRepositoryDeletion_CreateDuplicateAndList(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionTestCatalog(t)

	first := &RepositoryDeletion{
		Repository:       "repo1",
		StorageID:        "sid",
		StorageNamespace: "mem://bucket/repo1",
		State:            RepositoryDeletionState_PENDING,
		Step:             RepositoryDeletionStep_OBJECTS,
		Marker:           "prefix/last",
		ObjectsDeleted:   7,
		RequestedBy:      "tester",
		Force:            true,
		CascadeOf:        "data-repo",
	}
	require.NoError(t, c.CreateRepositoryDeletion(ctx, first))

	got, err := c.GetRepositoryDeletion(ctx, "repo1")
	require.NoError(t, err)
	require.Equal(t, "repo1", got.Repository)
	require.Equal(t, "sid", got.StorageID)
	require.Equal(t, "mem://bucket/repo1", got.StorageNamespace)
	require.Equal(t, RepositoryDeletionState_PENDING, got.State)
	require.Equal(t, RepositoryDeletionStep_OBJECTS, got.Step)
	require.Equal(t, "prefix/last", got.Marker)
	require.Equal(t, int64(7), got.ObjectsDeleted)
	require.Equal(t, "tester", got.RequestedBy)
	require.True(t, got.Force)
	require.Equal(t, "data-repo", got.CascadeOf)
	require.False(t, got.RequestedAt.IsZero(), "requested_at is set on create")
	require.False(t, got.UpdatedAt.IsZero(), "updated_at is set on create")
	require.True(t, got.MetadataDeletedAt.IsZero(), "metadata_deleted_at is unset until the metadata step succeeds")
	require.True(t, got.NextAttemptAt.IsZero(), "a new record is due immediately")

	// A second create for the same name fails on the nil predicate, and changes nothing (R11).
	duplicate := &RepositoryDeletion{
		Repository:       "repo1",
		StorageNamespace: "mem://bucket/other",
		State:            RepositoryDeletionState_PENDING,
	}
	err = c.CreateRepositoryDeletion(ctx, duplicate)
	require.ErrorIs(t, err, ErrDeletionInProgress)
	still, err := c.GetRepositoryDeletion(ctx, "repo1")
	require.NoError(t, err)
	require.Equal(t, "mem://bucket/repo1", still.StorageNamespace, "a duplicate create must not overwrite the record")

	// Records are listed in repository name order.
	second := &RepositoryDeletion{
		Repository:       "repo2",
		StorageNamespace: "mem://bucket/repo2",
		State:            RepositoryDeletionState_DONE,
	}
	require.NoError(t, c.CreateRepositoryDeletion(ctx, second))
	records, err := c.ListRepositoryDeletions(ctx)
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, "repo1", records[0].Repository)
	require.Equal(t, "repo2", records[1].Repository)

	_, err = c.GetRepositoryDeletion(ctx, "missing")
	require.ErrorIs(t, err, ErrRepositoryDeletionNotFound)
	require.ErrorIs(t, err, graveler.ErrNotFound, "a missing record matches a missing repository, so callers map both to 404")
}

func TestRepositoryDeletion_RetryOfFailedKeepsStepAndMarker(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionTestCatalog(t)

	record := &RepositoryDeletion{
		Repository:       "repo1",
		StorageNamespace: "mem://bucket/repo1",
		State:            RepositoryDeletionState_PENDING,
	}
	require.NoError(t, c.CreateRepositoryDeletion(ctx, record))

	// Move the record to FAILED as the worker would after max_attempts.
	failed, predicate, err := c.getRepositoryDeletionWithPredicate(ctx, "repo1")
	require.NoError(t, err)
	failed.State = RepositoryDeletionState_FAILED
	failed.Step = RepositoryDeletionStep_VERIFY
	failed.Marker = "prefix/checked/up/to/here"
	failed.ObjectsDeleted = 42
	failed.Attempts = 10
	failed.LastError = "AccessDenied"
	failed.NextAttemptAt = time.Now().Add(time.Hour)
	require.NoError(t, c.UpdateRepositoryDeletion(ctx, failed, predicate))

	// The worker update of a stale read is dropped: this is what makes ownership safe (§3.2).
	stale, stalePredicate, err := c.getRepositoryDeletionWithPredicate(ctx, "repo1")
	require.NoError(t, err)
	require.NoError(t, c.UpdateRepositoryDeletion(ctx, stale, stalePredicate))
	stale.LastError = "a write from a stale read must fail"
	err = c.UpdateRepositoryDeletion(ctx, stale, predicate)
	require.ErrorIs(t, err, kv.ErrPredicateFailed)

	// A repeated delete request resets the FAILED record, keeping its progress (R15).
	current, predicate, err := c.getRepositoryDeletionWithPredicate(ctx, "repo1")
	require.NoError(t, err)
	require.Equal(t, RepositoryDeletionState_FAILED, current.State)
	reset, err := c.ResetRepositoryDeletionForRetry(ctx, current, predicate)
	require.NoError(t, err)
	require.Equal(t, RepositoryDeletionState_PENDING, reset.State)
	require.Equal(t, int32(0), reset.Attempts)
	require.Equal(t, RepositoryDeletionStep_VERIFY, reset.Step, "retry keeps the step")
	require.Equal(t, "prefix/checked/up/to/here", reset.Marker, "retry keeps the marker")
	require.Equal(t, int64(42), reset.ObjectsDeleted)
	require.True(t, reset.NextAttemptAt.IsZero(), "a retried record is due immediately")
	require.Equal(t, "AccessDenied", reset.LastError, "the retry keeps the error for the status reader")

	stored, err := c.GetRepositoryDeletion(ctx, "repo1")
	require.NoError(t, err)
	require.Equal(t, RepositoryDeletionState_PENDING, stored.State)
	require.Equal(t, RepositoryDeletionStep_VERIFY, stored.Step)
	require.Equal(t, "prefix/checked/up/to/here", stored.Marker)

	// Only a FAILED record may be retried.
	_, predicate, err = c.getRepositoryDeletionWithPredicate(ctx, "repo1")
	require.NoError(t, err)
	_, err = c.ResetRepositoryDeletionForRetry(ctx, stored, predicate)
	require.ErrorIs(t, err, ErrDeletionNotFailed)
}

func TestRepositoryDeletion_DeleteRecord(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionTestCatalog(t)

	record := &RepositoryDeletion{
		Repository:       "repo1",
		StorageNamespace: "mem://bucket/repo1",
		State:            RepositoryDeletionState_DONE,
	}
	require.NoError(t, c.CreateRepositoryDeletion(ctx, record))
	require.NoError(t, c.DeleteRepositoryDeletion(ctx, "repo1"))
	_, err := c.GetRepositoryDeletion(ctx, "repo1")
	require.True(t, errors.Is(err, ErrRepositoryDeletionNotFound))
}
