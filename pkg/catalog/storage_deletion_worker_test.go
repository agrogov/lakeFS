package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

const (
	happyPathRepository = "repo1"
	happyPathNamespace  = "mem://deletion/repo1"
	happyPathSiblingNS  = "mem://deletion/repo1-metadata"
	happyPathSibling    = happyPathSiblingNS + "/keep"
)

// objectKeyFor distributes the objects over the three kinds a namespace holds: committed data,
// staged data and the "_lakefs/" ranges, metaranges and the dummy marker.  The deletion must not
// care which is which (R12).
func objectKeyFor(i int) string {
	switch i % 3 {
	case 0:
		return fmt.Sprintf("data/branch-main/file-%05d", i)
	case 1:
		return fmt.Sprintf("staged/file-%05d", i)
	default:
		return fmt.Sprintf("_lakefs/ranges/file-%05d", i)
	}
}

// TestRepositoryDeletionWorker_HappyPath deletes 3000 objects of every kind, ends DONE, empties the
// namespace and leaves the repository answering 404 (R3, R12, R21, A1 on mem).
func TestRepositoryDeletionWorker_HappyPath(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)

	_, err := c.CreateRepository(ctx, happyPathRepository, "", happyPathNamespace, "main", false)
	require.NoError(t, err)

	const objectCount = 3000
	for i := range objectCount {
		putNamespaceObject(t, c, "", happyPathNamespace, objectKeyFor(i))
	}
	putNamespaceObject(t, c, "", happyPathNamespace, repositoryDeletionDummyKey)
	putFullObject(t, c, happyPathSibling)

	deletedObjectsBefore := promtestutil.ToFloat64(repositoryStorageDeletedObjects)

	records, err := c.DeleteRepositoryWithStorage(ctx, happyPathRepository, DeleteStorageOptions{RequestedBy: "tester"})
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, RepositoryDeletionState_DELETING, record.State)
	require.Equal(t, RepositoryDeletionStep_SETTLE, record.Step)
	require.False(t, record.MetadataDeletedAt.IsZero())
	require.Equal(t, "tester", record.RequestedBy)

	// R3: the metadata is deleted inline, so the repository is not readable as soon as the request
	// returns.
	requireRepositoryGone(t, c, happyPathRepository)

	startRepositoryDeletionWorker(t, c)
	done := waitForDeletionState(t, c, happyPathRepository, RepositoryDeletionState_DONE, 60*time.Second)
	require.Equal(t, RepositoryDeletionStep_FINISHED, done.Step)
	require.Equal(t, int64(objectCount+1), done.ObjectsDeleted, "every object and the marker are counted once")
	require.Empty(t, done.Marker)
	require.Empty(t, done.LastError)
	require.Equal(t, int32(0), done.Attempts)

	require.Empty(t, namespaceObjects(t, c, happyPathNamespace), "the namespace lists empty")
	require.Equal(t, []string{happyPathSibling}, namespaceObjects(t, c, happyPathSiblingNS),
		"the sibling namespace is not touched (A2)")

	require.GreaterOrEqual(t, promtestutil.ToFloat64(repositoryStorageDeletedObjects), deletedObjectsBefore+objectCount+1,
		"the deleted-objects counter grows by every deleted object (R21)")
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(repositoryStorageDeletions.WithLabelValues("done")) >= 1
	}, 30*time.Second, 5*time.Millisecond, "the by-state gauge reports the DONE record (R21)")
}

// TestRepositoryDeletionWorker_CrashAndResume cancels the worker in the middle of the object pass,
// after the first page was checkpointed, and proves a new worker on the same KV continues from the
// marker and finishes (R13, A3).
func TestRepositoryDeletionWorker_CrashAndResume(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)

	const (
		repository  = "resumed"
		namespace   = "mem://resume/ns"
		objectCount = 500
	)
	_, err := c.CreateRepository(ctx, repository, "", namespace, "main", false)
	require.NoError(t, err)
	for i := range objectCount {
		putNamespaceObject(t, c, "", namespace, fmt.Sprintf("obj/%05d", i))
	}
	putNamespaceObject(t, c, "", namespace, repositoryDeletionDummyKey)

	pausing := &pausingDeleteAdapter{Adapter: c.BlockAdapter, paused: make(chan struct{})}
	c.BlockAdapter = pausing

	_, err = c.DeleteRepositoryWithStorage(ctx, repository, DeleteStorageOptions{})
	require.NoError(t, err)

	firstWorkerCtx, firstWorkerCancel := contextWithCancelWithoutTestContext(t)
	c.StartRepositoryDeletionWorker(firstWorkerCtx,
		WithRepositoryDeletionSettle(time.Millisecond),
		WithRepositoryDeletionOwnershipIntervals(5*time.Millisecond, 50*time.Millisecond))

	// The adapter blocks the second page, so the first page's marker is already committed.
	select {
	case <-pausing.paused:
	case <-time.After(30 * time.Second):
		t.Fatal("the worker did not reach the second page")
	}
	interrupted, err := c.GetRepositoryDeletion(ctx, repository)
	require.NoError(t, err)
	require.NotEmpty(t, interrupted.Marker, "the first page's marker is checkpointed")
	require.Positive(t, interrupted.ObjectsDeleted)
	require.Less(t, interrupted.ObjectsDeleted, int64(objectCount+1), "the deletion stopped in the middle")

	firstWorkerCancel()

	// A new worker on the same KV and adapter: the same "process" restarted.
	startRepositoryDeletionWorker(t, c)
	done := waitForDeletionState(t, c, repository, RepositoryDeletionState_DONE, 60*time.Second)
	require.Equal(t, int64(objectCount+1), done.ObjectsDeleted)
	require.Empty(t, namespaceObjects(t, c, namespace))
}

// TestRepositoryDeletionWorker_TwoWorkers runs two workers on one KV store and one adapter, as two
// replicas would, and proves the deletion completes and every object is deleted once (A4).
func TestRepositoryDeletionWorker_TwoWorkers(t *testing.T) {
	ctx := t.Context()
	store := kvtest.GetStore(ctx, t)
	first := newStorageDeletionWorkerCatalog(t, store)
	second := newStorageDeletionWorkerCatalog(t, store)
	// One object store for both replicas.
	second.BlockAdapter = first.BlockAdapter

	const (
		repository  = "shared"
		namespace   = "mem://shared/ns"
		objectCount = 500
	)
	_, err := first.CreateRepository(ctx, repository, "", namespace, "main", false)
	require.NoError(t, err)
	for i := range objectCount {
		putNamespaceObject(t, first, "", namespace, fmt.Sprintf("obj/%05d", i))
	}
	putNamespaceObject(t, first, "", namespace, repositoryDeletionDummyKey)

	_, err = first.DeleteRepositoryWithStorage(ctx, repository, DeleteStorageOptions{})
	require.NoError(t, err)

	startRepositoryDeletionWorker(t, first)
	startRepositoryDeletionWorker(t, second)

	done := waitForDeletionState(t, first, repository, RepositoryDeletionState_DONE, 60*time.Second)
	// Deletes are idempotent, so a duplicated page can at worst count an object twice; it can never
	// leave one behind or delete outside the namespace.
	require.GreaterOrEqual(t, done.ObjectsDeleted, int64(objectCount+1))
	require.Empty(t, namespaceObjects(t, first, namespace))

	records, err := first.ListRepositoryDeletions(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1, "one record for the repository, however many replicas ran")
}

// TestRepositoryDeletionWorker_LateWriteRemovedByVerify writes an object behind the OBJECTS marker,
// the way a replica with a stale repository cache would, and proves VERIFY removes it.
func TestRepositoryDeletionWorker_LateWriteRemovedByVerify(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)

	const (
		repository  = "late"
		namespace   = "mem://late/ns"
		objectCount = 500
	)
	_, err := c.CreateRepository(ctx, repository, "", namespace, "main", false)
	require.NoError(t, err)
	for i := range objectCount {
		putNamespaceObject(t, c, "", namespace, fmt.Sprintf("obj/%05d", i))
	}
	putNamespaceObject(t, c, "", namespace, repositoryDeletionDummyKey)

	// "obj/00050-late" sorts before the first page's last key ("obj/00099"), so OBJECTS, which
	// continues after the marker, never lists it.
	c.BlockAdapter = &lateWriteDeleteAdapter{Adapter: c.BlockAdapter, t: t, lateKey: "obj/00050-late", lateSize: 4}

	_, err = c.DeleteRepositoryWithStorage(ctx, repository, DeleteStorageOptions{})
	require.NoError(t, err)

	startRepositoryDeletionWorker(t, c)
	done := waitForDeletionState(t, c, repository, RepositoryDeletionState_DONE, 60*time.Second)
	require.Equal(t, int64(objectCount+1+1), done.ObjectsDeleted, "the late write is counted too")
	require.Empty(t, namespaceObjects(t, c, namespace), "VERIFY removed the late write before the marker step")
}

// TestRepositoryDeletionWorker_FailsAndResumesAfterReRequest makes every adapter call fail, proves
// the record backs off and ends FAILED with the provider's error, then clears the error and proves a
// repeated request resumes the very same record and completes (R15).
func TestRepositoryDeletionWorker_FailsAndResumesAfterReRequest(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)

	const (
		repository  = "failing"
		namespace   = "mem://failing/ns"
		objectCount = 10
	)
	_, err := c.CreateRepository(ctx, repository, "", namespace, "main", false)
	require.NoError(t, err)
	for i := range objectCount {
		putNamespaceObject(t, c, "", namespace, fmt.Sprintf("obj/%05d", i))
	}
	putNamespaceObject(t, c, "", namespace, repositoryDeletionDummyKey)

	errorsBefore := promtestutil.ToFloat64(repositoryStorageDeletionErrors.WithLabelValues(RepositoryDeletionStep_OBJECTS.String()))

	providerError := errors.New("access denied: the credentials may not delete objects")
	failing := &failingDeleteAdapter{Adapter: c.BlockAdapter, err: providerError}
	c.BlockAdapter = failing

	_, err = c.DeleteRepositoryWithStorage(ctx, repository, DeleteStorageOptions{})
	require.NoError(t, err)

	startRepositoryDeletionWorker(t, c)
	failed := waitForDeletionState(t, c, repository, RepositoryDeletionState_FAILED, 60*time.Second)
	require.Equal(t, int32(3), failed.Attempts, "FAILED at max_attempts")
	require.Contains(t, failed.LastError, "access denied")
	require.Equal(t, RepositoryDeletionStep_OBJECTS, failed.Step)
	require.True(t, failed.NextAttemptAt.After(failed.UpdatedAt), "the retry backs off")
	require.GreaterOrEqual(t, promtestutil.ToFloat64(repositoryStorageDeletionErrors.WithLabelValues(RepositoryDeletionStep_OBJECTS.String())),
		errorsBefore+3, "every failed attempt counts by step (R21)")

	// The error is transient for the test: clear it and repeat the request (R15).
	failing.setError(nil)
	records, err := c.DeleteRepositoryWithStorage(ctx, repository, DeleteStorageOptions{})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, RepositoryDeletionState_DELETING, records[0].State)
	require.Equal(t, int32(0), records[0].Attempts, "a re-request resets the attempt budget")
	require.Empty(t, records[0].LastError)

	done := waitForDeletionState(t, c, repository, RepositoryDeletionState_DONE, 60*time.Second)
	require.Equal(t, int64(objectCount+1), done.ObjectsDeleted)
	require.Empty(t, namespaceObjects(t, c, namespace))
}

// TestRepositoryDeletionWorker_ExpiresDoneRecords proves a DONE record older than 24 hours is
// removed by the worker, and a fresher one is kept (R14).
func TestRepositoryDeletionWorker_ExpiresDoneRecords(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)

	require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
		Repository:       "old",
		StorageNamespace: "mem://expire/old",
		State:            RepositoryDeletionState_DONE,
		Step:             RepositoryDeletionStep_FINISHED,
	}))
	overwriteDeletionRecordUpdatedAt(t, c, "old", time.Now().UTC().Add(-25*time.Hour))

	require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
		Repository:       "fresh",
		StorageNamespace: "mem://expire/fresh",
		State:            RepositoryDeletionState_DONE,
		Step:             RepositoryDeletionStep_FINISHED,
	}))

	startRepositoryDeletionWorker(t, c)

	require.Eventually(t, func() bool {
		_, err := c.GetRepositoryDeletion(ctx, "old")
		return errors.Is(err, ErrRepositoryDeletionNotFound)
	}, 30*time.Second, 5*time.Millisecond, "a DONE record older than 24 hours is removed")

	kept, err := c.GetRepositoryDeletion(ctx, "fresh")
	require.NoError(t, err)
	require.Equal(t, RepositoryDeletionState_DONE, kept.State)
}

func contextWithCancelWithoutTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	t.Cleanup(cancel)
	return ctx, cancel
}
