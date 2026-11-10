package catalog

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/upload"
)

// newStorageDeletionWorkerCatalog builds a catalog on the mem blockstore and, when store is
// non-nil, on a caller-provided KV store so two catalogs can share one deletion partition.
func newStorageDeletionWorkerCatalog(t *testing.T, store kv.Store) *Catalog {
	t.Helper()
	ctx := context.WithoutCancel(t.Context())
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	// No repository cache: the metadata delete must make GET answer 404 immediately (R3), and a
	// repeated request must see the deletion record rather than a cached repository.
	viper.Set("graveler.repository_cache.size", 0)
	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	require.NoError(t, err)
	cfg.Committed.LocalCache.Dir = t.TempDir()

	if store == nil {
		store = kvtest.GetStore(t.Context(), t)
	}
	c, err := New(ctx, Config{
		Config:       cfg,
		KVStore:      store,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Small values keep the worker tests in milliseconds: one page per 100 objects, three attempts,
	// and no wait between the metadata delete and the object pass.
	c.RepositoryDeletionStorage = config.RepositoryDeletionStorage{
		Enabled:     true,
		Interval:    5 * time.Millisecond,
		BatchSize:   100,
		MaxAttempts: 3,
	}
	return c
}

// startRepositoryDeletionWorker starts a worker on its own cancellable context and stops it at the
// end of the test.
func startRepositoryDeletionWorker(t *testing.T, c *Catalog) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	c.StartRepositoryDeletionWorker(ctx,
		WithRepositoryDeletionSettle(time.Millisecond),
		WithRepositoryDeletionOwnershipIntervals(5*time.Millisecond, 50*time.Millisecond))
	t.Cleanup(cancel)
	return cancel
}

// putNamespaceObject writes one object under a storage namespace with a relative key.
func putNamespaceObject(t *testing.T, c *Catalog, storageID, storageNamespace, key string) {
	t.Helper()
	_, err := c.BlockAdapter.Put(t.Context(), block.ObjectPointer{
		StorageID:        storageID,
		StorageNamespace: storageNamespace,
		Identifier:       key,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(key)), strings.NewReader(key), block.PutOpts{})
	require.NoError(t, err)
}

// putFullObject writes one object addressed by a full key, used for sibling namespaces.
func putFullObject(t *testing.T, c *Catalog, fullKey string) {
	t.Helper()
	_, err := c.BlockAdapter.Put(t.Context(), block.ObjectPointer{
		Identifier:     fullKey,
		IdentifierType: block.IdentifierTypeFull,
	}, int64(len(fullKey)), strings.NewReader(fullKey), block.PutOpts{})
	require.NoError(t, err)
}

// namespaceObjectPrefix returns the fully qualified prefix of a namespace's objects, with exactly
// one trailing slash.
func namespaceObjectPrefix(t *testing.T, c *Catalog, storageNamespace string) string {
	t.Helper()
	qk, err := c.BlockAdapter.ResolveNamespace("", storageNamespace, "", block.IdentifierTypeRelative)
	require.NoError(t, err)
	prefix := qk.Format()
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

// namespaceObjects lists every object under a namespace, sorted.  It never modifies the namespace,
// unlike DeleteNamespacePage.
func namespaceObjects(t *testing.T, c *Catalog, storageNamespace string) []string {
	t.Helper()
	u, err := url.Parse(storageNamespace)
	require.NoError(t, err)
	walker, err := c.BlockAdapter.GetWalker("", block.WalkerOptions{StorageURI: u})
	require.NoError(t, err)

	prefix := namespaceObjectPrefix(t, c, storageNamespace)
	keys := make([]string, 0)
	require.NoError(t, walker.Walk(t.Context(), u, block.WalkOptions{}, func(entry block.ObjectStoreEntry) error {
		if strings.HasPrefix(entry.Address, prefix) {
			keys = append(keys, entry.Address)
		}
		return nil
	}))
	sort.Strings(keys)
	return keys
}

// waitForDeletionState polls the record until it reaches state or the timeout expires.
func waitForDeletionState(t *testing.T, c *Catalog, repository string, state RepositoryDeletionState, timeout time.Duration) *RepositoryDeletion {
	t.Helper()
	var last *RepositoryDeletion
	require.Eventually(t, func() bool {
		record, err := c.GetRepositoryDeletion(t.Context(), repository)
		if err != nil {
			return false
		}
		last = record
		return record.State == state
	}, timeout, 5*time.Millisecond)
	require.NotNil(t, last)
	return last
}

// deletionSnapshot is the whole state a refusal must leave byte-identical.
type deletionSnapshot struct {
	Objects      []string
	Records      []*RepositoryDeletion
	Repositories []string
}

func snapshotDeletionState(t *testing.T, c *Catalog, namespaces ...string) deletionSnapshot {
	t.Helper()
	ctx := t.Context()
	snapshot := deletionSnapshot{Objects: make([]string, 0)}
	for _, namespace := range namespaces {
		snapshot.Objects = append(snapshot.Objects, namespaceObjects(t, c, namespace)...)
	}
	sort.Strings(snapshot.Objects)

	records, err := c.ListRepositoryDeletions(ctx)
	require.NoError(t, err)
	snapshot.Records = records

	it, err := c.Store.ListRepositories(ctx)
	require.NoError(t, err)
	defer it.Close()
	for it.Next() {
		repo := it.Value()
		snapshot.Repositories = append(snapshot.Repositories, fmt.Sprintf("%s|%s|%s|%t",
			repo.RepositoryID, repo.StorageNamespace, repo.State, repo.ReadOnly))
	}
	require.NoError(t, it.Err())
	sort.Strings(snapshot.Repositories)
	return snapshot
}

// overwriteDeletionRecordUpdatedAt makes a record look older than it is, for the expiry test.
func overwriteDeletionRecordUpdatedAt(t *testing.T, c *Catalog, repository string, updatedAt time.Time) {
	t.Helper()
	ctx := t.Context()
	record, predicate, err := c.getRepositoryDeletionWithPredicate(ctx, repository)
	require.NoError(t, err)
	data := record.toProto()
	data.UpdatedAt = timestamppb.New(updatedAt)
	require.NoError(t, kv.SetMsgIf(ctx, c.KVStore, RepositoryDeletionsPartition, RepositoryDeletionKey(repository), data, predicate))
}

// --- adapter wrappers used by the worker tests ---

// failingDeleteAdapter fails every DeleteNamespacePage call with err until the error is cleared.
type failingDeleteAdapter struct {
	block.Adapter
	mu  sync.Mutex
	err error
}

func (a *failingDeleteAdapter) setError(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.err = err
}

func (a *failingDeleteAdapter) DeleteNamespacePage(ctx context.Context, storageID, storageNamespace string, opts block.DeleteNamespaceOpts) (*block.DeleteNamespaceResult, error) {
	a.mu.Lock()
	err := a.err
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return a.Adapter.DeleteNamespacePage(ctx, storageID, storageNamespace, opts)
}

// pausingDeleteAdapter serves the first page and then blocks the second one until the context is
// cancelled, which simulates a crash after the first checkpoint.
type pausingDeleteAdapter struct {
	block.Adapter
	calls  atomic.Int64
	paused chan struct{}
}

func (a *pausingDeleteAdapter) DeleteNamespacePage(ctx context.Context, storageID, storageNamespace string, opts block.DeleteNamespaceOpts) (*block.DeleteNamespaceResult, error) {
	if a.calls.Add(1) == 2 {
		close(a.paused)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return a.Adapter.DeleteNamespacePage(ctx, storageID, storageNamespace, opts)
}

// lateWriteDeleteAdapter writes one object behind the OBJECTS marker after its first page, as a
// replica with a stale repository cache would.
type lateWriteDeleteAdapter struct {
	block.Adapter
	once     sync.Once
	t        *testing.T
	lateKey  string
	lateSize int
}

func (a *lateWriteDeleteAdapter) DeleteNamespacePage(ctx context.Context, storageID, storageNamespace string, opts block.DeleteNamespaceOpts) (*block.DeleteNamespaceResult, error) {
	result, err := a.Adapter.DeleteNamespacePage(ctx, storageID, storageNamespace, opts)
	if err == nil {
		a.once.Do(func() {
			_, putErr := a.Adapter.Put(ctx, block.ObjectPointer{
				StorageID:        storageID,
				StorageNamespace: storageNamespace,
				Identifier:       a.lateKey,
				IdentifierType:   block.IdentifierTypeRelative,
			}, int64(a.lateSize), strings.NewReader("late"), block.PutOpts{})
			if putErr != nil {
				a.t.Errorf("late write adapter: put %q: %v", a.lateKey, putErr)
			}
		})
	}
	return result, err
}

// noNamespaceDeleteSupportAdapter reports a blockstore that cannot delete namespaces (R6).
type noNamespaceDeleteSupportAdapter struct {
	block.Adapter
}

func (a *noNamespaceDeleteSupportAdapter) GetStorageNamespaceInfo(storageID string) *block.StorageNamespaceInfo {
	info := a.Adapter.GetStorageNamespaceInfo(storageID)
	if info == nil {
		return &block.StorageNamespaceInfo{}
	}
	unsupported := *info
	unsupported.DeleteNamespaceSupport = false
	return &unsupported
}

// requireRepositoryGone asserts the GET of a deleted repository answers "not found".
func requireRepositoryGone(t *testing.T, c *Catalog, repository string) {
	t.Helper()
	_, err := c.GetRepository(t.Context(), repository)
	require.ErrorIs(t, err, graveler.ErrNotFound)
}

// --- request path refusals (R6-R11) ---

// TestDeleteRepositoryWithStorage_Refusals pins R6-R11: a refusal changes nothing at all, neither in
// the KV store (repository and deletion records) nor in the object store.  The object store is
// compared through the namespaces the case touches, because the mem adapter's walker is
// non-destructive.
func TestDeleteRepositoryWithStorage_Refusals(t *testing.T) {
	ctx := t.Context()

	t.Run("R6 blockstore cannot delete a namespace", func(t *testing.T) {
		c := newStorageDeletionWorkerCatalog(t, nil)
		c.BlockAdapter = &noNamespaceDeleteSupportAdapter{Adapter: c.BlockAdapter}
		_, err := c.CreateRepository(ctx, "victim", "", "mem://refuse/unsupported", "main", false)
		require.NoError(t, err)
		putNamespaceObject(t, c, "", "mem://refuse/unsupported", "keep")

		before := snapshotDeletionState(t, c, "mem://refuse/unsupported")
		_, err = c.DeleteRepositoryWithStorage(ctx, "victim", DeleteStorageOptions{})
		require.ErrorIs(t, err, ErrStorageDeletionNotSupported)
		require.Equal(t, before, snapshotDeletionState(t, c, "mem://refuse/unsupported"))
	})

	t.Run("R7 storage deletion disabled", func(t *testing.T) {
		c := newStorageDeletionWorkerCatalog(t, nil)
		c.RepositoryDeletionStorage.Enabled = false
		_, err := c.CreateRepository(ctx, "victim", "", "mem://refuse/disabled", "main", false)
		require.NoError(t, err)
		putNamespaceObject(t, c, "", "mem://refuse/disabled", "keep")

		before := snapshotDeletionState(t, c, "mem://refuse/disabled")
		_, err = c.DeleteRepositoryWithStorage(ctx, "victim", DeleteStorageOptions{})
		require.ErrorIs(t, err, ErrStorageDeletionNotSupported)
		require.Equal(t, before, snapshotDeletionState(t, c, "mem://refuse/disabled"))
	})

	t.Run("R8 read-only repository even with force", func(t *testing.T) {
		c := newStorageDeletionWorkerCatalog(t, nil)
		_, err := c.CreateRepository(ctx, "victim", "", "mem://refuse/readonly", "main", true)
		require.NoError(t, err)
		putNamespaceObject(t, c, "", "mem://refuse/readonly", "keep")

		before := snapshotDeletionState(t, c, "mem://refuse/readonly")
		_, err = c.DeleteRepositoryWithStorage(ctx, "victim", DeleteStorageOptions{Force: true})
		require.ErrorIs(t, err, graveler.ErrReadOnlyRepository)
		require.Equal(t, before, snapshotDeletionState(t, c, "mem://refuse/readonly"))
	})

	t.Run("R9 namespace is a storage root", func(t *testing.T) {
		c := newStorageDeletionWorkerCatalog(t, nil)
		_, err := c.CreateRepository(ctx, "victim", "", "mem://", "main", false)
		require.NoError(t, err)
		putNamespaceObject(t, c, "", "mem://keep", "keep")

		before := snapshotDeletionState(t, c, "mem://keep")
		_, err = c.DeleteRepositoryWithStorage(ctx, "victim", DeleteStorageOptions{})
		require.ErrorIs(t, err, block.ErrNamespaceIsRoot)
		require.Equal(t, before, snapshotDeletionState(t, c, "mem://keep"))
	})

	t.Run("R10 namespace overlaps another repository", func(t *testing.T) {
		c := newStorageDeletionWorkerCatalog(t, nil)
		_, err := c.CreateRepository(ctx, "other", "", "mem://refuse/shared", "main", false)
		require.NoError(t, err)
		_, err = c.CreateRepository(ctx, "victim", "", "mem://refuse/shared/child", "main", false)
		require.NoError(t, err)
		putNamespaceObject(t, c, "", "mem://refuse/shared", "other-object")
		putNamespaceObject(t, c, "", "mem://refuse/shared/child", "victim-object")

		before := snapshotDeletionState(t, c, "mem://refuse/shared", "mem://refuse/shared/child")
		_, err = c.DeleteRepositoryWithStorage(ctx, "victim", DeleteStorageOptions{})
		require.ErrorIs(t, err, ErrNamespaceShared)
		require.Equal(t, before, snapshotDeletionState(t, c, "mem://refuse/shared", "mem://refuse/shared/child"))
	})

	t.Run("R11 a deletion is already pending", func(t *testing.T) {
		c := newStorageDeletionWorkerCatalog(t, nil)
		_, err := c.CreateRepository(ctx, "victim", "", "mem://refuse/pending", "main", false)
		require.NoError(t, err)
		putNamespaceObject(t, c, "", "mem://refuse/pending", "keep")
		require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
			Repository:       "victim",
			StorageNamespace: "mem://refuse/pending",
			State:            RepositoryDeletionState_DELETING,
			Step:             RepositoryDeletionStep_OBJECTS,
			Marker:           "keep",
		}))

		before := snapshotDeletionState(t, c, "mem://refuse/pending")
		_, err = c.DeleteRepositoryWithStorage(ctx, "victim", DeleteStorageOptions{})
		require.ErrorIs(t, err, ErrDeletionInProgress)
		require.Equal(t, before, snapshotDeletionState(t, c, "mem://refuse/pending"))
	})
}

// TestDeleteRepositoryWithStorage_ResumeFailedRecord covers the second half of R15: repeating the
// request for a repository that is already gone resets a FAILED record, keeping its step and marker.
func TestDeleteRepositoryWithStorage_ResumeFailedRecord(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)

	const (
		repository = "resumed"
		namespace  = "mem://resume/ns"
	)
	require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
		Repository:       repository,
		StorageNamespace: namespace,
		State:            RepositoryDeletionState_FAILED,
		Step:             RepositoryDeletionStep_OBJECTS,
		Marker:           "last/page",
		ObjectsDeleted:   42,
		Attempts:         3,
		LastError:        "access denied",
		NextAttemptAt:    time.Now().UTC().Add(time.Hour),
	}))

	records, err := c.DeleteRepositoryWithStorage(ctx, repository, DeleteStorageOptions{RequestedBy: "retrier"})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, RepositoryDeletionState_DELETING, records[0].State)
	require.Equal(t, RepositoryDeletionStep_SETTLE, records[0].Step)
	require.Equal(t, "last/page", records[0].Marker, "the marker is kept, so the deletion resumes where it stopped")
	require.Equal(t, int64(42), records[0].ObjectsDeleted)
	require.Equal(t, int32(0), records[0].Attempts)
	require.Empty(t, records[0].LastError)
	require.Equal(t, "retrier", records[0].RequestedBy)

	// A record that is not FAILED or DONE is still a conflict (R11).
	_, err = c.DeleteRepositoryWithStorage(ctx, repository, DeleteStorageOptions{})
	require.ErrorIs(t, err, ErrDeletionInProgress)

	// The same retry shape applies to a repository that still exists: its FAILED record is reset and
	// the inline metadata step runs, keeping the object marker.
	const liveRepository = "live-failed"
	_, err = c.CreateRepository(ctx, liveRepository, "", "mem://resume/live", "main", false)
	require.NoError(t, err)
	require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
		Repository:       liveRepository,
		StorageNamespace: "mem://resume/live",
		State:            RepositoryDeletionState_FAILED,
		Step:             RepositoryDeletionStep_OBJECTS,
		Marker:           "obj/last",
		ObjectsDeleted:   7,
		Attempts:         2,
		LastError:        "access denied",
	}))
	records, err = c.DeleteRepositoryWithStorage(ctx, liveRepository, DeleteStorageOptions{})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, RepositoryDeletionState_DELETING, records[0].State)
	require.Equal(t, RepositoryDeletionStep_SETTLE, records[0].Step)
	require.Equal(t, "obj/last", records[0].Marker)
	require.Equal(t, int64(7), records[0].ObjectsDeleted)
	require.Equal(t, int32(0), records[0].Attempts)
	require.Empty(t, records[0].LastError)
	requireRepositoryGone(t, c, liveRepository)
}
