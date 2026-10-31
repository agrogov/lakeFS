package datasets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
)

// etagStore wraps a kv.Store the way CosmosDB behaves: Get returns an opaque
// predicate (an ETag) and SetIf accepts only such a token. A predicate built from
// record bytes never matches, so any caller that rebuilds the predicate fails here.
type etagStore struct {
	kv.Store
	mu sync.Mutex
}

func opaqueETag(value []byte) kv.Predicate {
	sum := sha256.Sum256(value)
	return "etag:" + hex.EncodeToString(sum[:])
}

func (e *etagStore) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	res, err := e.Store.Get(ctx, partitionKey, key)
	if err != nil {
		return nil, err
	}
	return &kv.ValueWithPredicate{Value: res.Value, Predicate: opaqueETag(res.Value)}, nil
}

func (e *etagStore) SetIf(ctx context.Context, partitionKey, key, value []byte, predicate kv.Predicate) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if predicate == nil {
		return e.Store.SetIf(ctx, partitionKey, key, value, nil)
	}
	cur, err := e.Store.Get(ctx, partitionKey, key)
	if errors.Is(err, kv.ErrNotFound) {
		return kv.ErrPredicateFailed
	}
	if err != nil {
		return err
	}
	if predicate != opaqueETag(cur.Value) {
		return kv.ErrPredicateFailed
	}
	return e.Store.SetIf(ctx, partitionKey, key, value, cur.Predicate)
}

func newETagStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	_, kvs, ctx := newTestStore(t)
	return NewStore(&etagStore{Store: kvs}), ctx
}

// TestStoreCASWithOpaquePredicate is BUG-1: every CAS must use the predicate that
// kv.Get returned, so it works on a store (CosmosDB) whose predicate is not the value.
func TestStoreCASWithOpaquePredicate(t *testing.T) {
	store, ctx := newETagStore(t)

	require.NoError(t, store.CreateDataset(ctx, newDatasetRecord()))
	current, predicate, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)
	require.NotNil(t, predicate)

	next := *current
	next.State = DatasetStatePublishing
	next.PendingVersion = current.LatestVersion + 1
	require.NoError(t, store.PutDatasetIf(ctx, &next, current, predicate), "PutDatasetIf after GetDataset")

	// The predicate now guards the record as it was before the write: a second write
	// conditioned on it is a stale read and must lose.
	stale := *current
	stale.State = DatasetStateDeleting
	require.ErrorIs(t, store.PutDatasetIf(ctx, &stale, current, predicate), kv.ErrPredicateFailed)

	fresh, freshPredicate, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)
	require.Equal(t, DatasetStatePublishing, fresh.State)

	require.ErrorIs(t, store.DeleteDataset(ctx, "customer-360", current, predicate), kv.ErrPredicateFailed)
	require.NoError(t, store.DeleteDataset(ctx, "customer-360", fresh, freshPredicate))
	_, _, err = store.GetDataset(ctx, "customer-360")
	require.ErrorIs(t, err, kv.ErrNotFound)
}

// TestStorePinCASWithOpaquePredicate is the pin half of BUG-1 on the BUG-15 layout: a pin is
// one record per reference, and the conditional writes on a reference record must apply the
// predicate the store returned for it, not a re-encoding of the value.
func TestStorePinCASWithOpaquePredicate(t *testing.T) {
	store, ctx := newETagStore(t)
	const (
		repository = "lakehouse-repo"
		commitID   = "30f9eb1c"
	)
	ref := PinReference("customer-360", 2)

	pin, err := store.AddPinReference(ctx, repository, commitID, ref)
	require.NoError(t, err, "creating a pin needs a nil predicate")
	require.Equal(t, []string{ref}, pin.References)

	read, predicate, err := store.GetPin(ctx, repository, commitID)
	require.NoError(t, err)
	require.Equal(t, pin, read)
	require.Nil(t, predicate, "a pin has no single record a predicate could guard (BUG-15)")

	// The reference's own record carries the predicate: the store rejects a create over it
	// and accepts a write conditioned on the value as read, even though the predicate is an
	// opaque ETag here.
	key, err := pinReferenceKey(repository, commitID, ref)
	require.NoError(t, err)
	stored, err := store.store.Get(ctx, []byte(PartitionKey), key)
	require.NoError(t, err)
	require.ErrorIs(t, store.store.SetIf(ctx, []byte(PartitionKey), key, stored.Value, nil), kv.ErrPredicateFailed)
	require.NoError(t, store.store.SetIf(ctx, []byte(PartitionKey), key, stored.Value, stored.Predicate))

	// A second reference is another record, so the two do not contend for one predicate.
	added, err := store.AddPinReference(ctx, repository, commitID, PinReference("customer-360", 3))
	require.NoError(t, err)
	require.Equal(t, 2, len(added.References))

	deleted, err := store.RemovePinReference(ctx, repository, commitID, ref)
	require.NoError(t, err)
	require.False(t, deleted)
	deleted, err = store.RemovePinReference(ctx, repository, commitID, PinReference("customer-360", 3))
	require.NoError(t, err)
	require.True(t, deleted, "the last reference deletes the pin")
	_, _, err = store.GetPin(ctx, repository, commitID)
	require.ErrorIs(t, err, kv.ErrNotFound)
}

// TestStoreLeaseCASWithOpaquePredicate is the lease half of BUG-1: taking and renewing the
// reconcile lease must use the predicate of the lease record as read.
func TestStoreLeaseCASWithOpaquePredicate(t *testing.T) {
	store, ctx := newETagStore(t)

	acquired, err := store.AcquireLease(ctx, "node-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	before, stalePredicate, err := store.GetLease(ctx)
	require.NoError(t, err)
	require.Equal(t, "node-a", before.Owner)
	require.NotNil(t, stalePredicate)

	// Renewal by the same owner is a CAS on the record it already holds.
	acquired, err = store.AcquireLease(ctx, "node-a", 2*time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	lease, _, err := store.GetLease(ctx)
	require.NoError(t, err)
	require.Equal(t, "node-a", lease.Owner)
	require.True(t, lease.ExpiresAt.After(before.ExpiresAt), "the renewal extended the lease")

	acquired, err = store.AcquireLease(ctx, "node-b", time.Minute)
	require.NoError(t, err)
	require.False(t, acquired, "a live lease owned by another node is not taken")

	// The predicate from before the renewal no longer matches: the lease is not overwritten.
	leaseValue, err := json.Marshal(leaseRecord{Owner: "node-b", ExpiresAt: lease.ExpiresAt})
	require.NoError(t, err)
	require.ErrorIs(t, store.store.SetIf(ctx, []byte(PartitionKey), []byte(ReconcileLeaseKey),
		leaseValue, stalePredicate), kv.ErrPredicateFailed)

	// Releasing with a stale predicate leaves the lease in place.
	require.ErrorIs(t, store.deleteIf(ctx, []byte(ReconcileLeaseKey), stalePredicate), kv.ErrPredicateFailed)
	still, _, err := store.GetLease(ctx)
	require.NoError(t, err)
	require.Equal(t, "node-a", still.Owner)

	require.NoError(t, store.ReleaseLease(ctx, "node-a"))
	_, _, err = store.GetLease(ctx)
	require.ErrorIs(t, err, kv.ErrNotFound)
	require.NoError(t, store.ReleaseLease(ctx, "node-a"), "releasing a lease that is gone is a no-op")
}

// TestPinsAcquireReleaseWithOpaquePredicate drives the pin manager end to end on a store
// whose predicate is opaque: acquiring a second reference and releasing them again are
// compare-and-swaps on the predicate of the pin record as read.
func TestPinsAcquireReleaseWithOpaquePredicate(t *testing.T) {
	f := newPinFixture(t)
	store := NewStore(&etagStore{Store: f.kvStore})
	pins := NewPins(store, f.cat)

	commit := f.sourceRepo(t, "source-opaque", "data/a/customers.parquet")
	src := PinnedSource{Repository: "source-opaque", CommitID: commit.String()}
	def := definitionOf(src)

	_, err := pins.Acquire(f.ctx, def, PinReference("customer-360", 1))
	require.NoError(t, err)
	rec, _, err := store.GetPin(f.ctx, src.Repository, src.CommitID)
	require.NoError(t, err)
	require.Equal(t, []string{PinReference("customer-360", 1)}, rec.References)

	// A second dataset version shares the pin: the CAS is on the predicate just read.
	_, err = pins.Acquire(f.ctx, def, PinReference("events", 1))
	require.NoError(t, err)
	rec, _, err = store.GetPin(f.ctx, src.Repository, src.CommitID)
	require.NoError(t, err)
	require.Equal(t, []string{PinReference("customer-360", 1), PinReference("events", 1)}, rec.References)

	// Releasing one reference keeps the pin; releasing the last removes it.
	require.NoError(t, pins.Release(f.ctx, def, PinReference("customer-360", 1)))
	rec, _, err = store.GetPin(f.ctx, src.Repository, src.CommitID)
	require.NoError(t, err)
	require.Equal(t, []string{PinReference("events", 1)}, rec.References)
	require.NoError(t, pins.Release(f.ctx, def, PinReference("events", 1)))
	_, _, err = store.GetPin(f.ctx, src.Repository, src.CommitID)
	require.ErrorIs(t, err, kv.ErrNotFound)
}

// TestStoreProposalCAS is BUG-8 (proposal half): a write based on a stale read must not
// overwrite a concurrent change, and exactly one of two racing writers wins.
func TestStoreProposalCAS(t *testing.T) {
	for name, open := range map[string]func(t *testing.T) (*Store, context.Context){
		"mem": func(t *testing.T) (*Store, context.Context) {
			s, _, ctx := newTestStore(t)
			return s, ctx
		},
		"opaque predicate": newETagStore,
	} {
		t.Run(name, func(t *testing.T) {
			store, ctx := open(t)
			first := &ProposalRecord{ID: "01J9Z6Q0K8", Dataset: "customer-360", Status: ProposalStatusOpen,
				Title: "t", CreatedAt: testTime()}
			require.NoError(t, store.PutProposalIf(ctx, first, nil))
			require.ErrorIs(t, store.PutProposalIf(ctx, first, nil), kv.ErrPredicateFailed, "create only if absent")

			read, err := store.GetProposal(ctx, "customer-360", "01J9Z6Q0K8")
			require.NoError(t, err)

			closed := *read
			closed.Status = ProposalStatusClosed
			merged := *read
			merged.Status = ProposalStatusMerged
			merged.MergedVersion = 3

			var wg sync.WaitGroup
			errs := make([]error, 2)
			for i, rec := range []*ProposalRecord{&closed, &merged} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = store.PutProposalIf(ctx, rec, read)
				}()
			}
			wg.Wait()
			wins := 0
			for _, err := range errs {
				if err == nil {
					wins++
				} else {
					require.ErrorIs(t, err, kv.ErrPredicateFailed)
				}
			}
			require.Equal(t, 1, wins, "exactly one of close and merge wins")

			// A write based on the original read now fails and leaves the winner.
			winner, err := store.GetProposal(ctx, "customer-360", "01J9Z6Q0K8")
			require.NoError(t, err)
			late := *read
			late.Title = "late"
			require.ErrorIs(t, store.PutProposalIf(ctx, &late, read), kv.ErrPredicateFailed)
			after, err := store.GetProposal(ctx, "customer-360", "01J9Z6Q0K8")
			require.NoError(t, err)
			require.Equal(t, winner, after)

			// A proposal removed in between is a lost race, not a resurrection.
			require.NoError(t, store.DeleteProposal(ctx, "customer-360", "01J9Z6Q0K8"))
			require.ErrorIs(t, store.PutProposalIf(ctx, &late, winner), kv.ErrPredicateFailed)
		})
	}
}

// TestStoreCASCanonicalizesExpected: the expected side is compared in canonical form, so
// a previous record carrying a monotonic-clock time and an empty (non-nil) slice still
// matches the stored encoding.
func TestStoreCASCanonicalizesExpected(t *testing.T) {
	store, _, ctx := newTestStore(t)

	rec := newDatasetRecord()
	rec.CreatedAt = time.Now()
	rec.UpdatedAt = time.Now()
	require.NoError(t, store.CreateDataset(ctx, rec))
	next := *rec
	next.State = DatasetStatePublishing
	next.PendingVersion = rec.LatestVersion + 1
	require.NoError(t, store.PutDatasetIf(ctx, &next, rec, nil), "previous holds a monotonic time.Now()")

	key := &MetadataKey{Name: "owner", Type: "string", AllowedValues: []string{}, CreatedAt: time.Now(), CreatedBy: "alice"}
	require.NoError(t, store.PutMetadataKey(ctx, *key, nil))
	updated := *key
	updated.Description = "d"
	require.NoError(t, store.PutMetadataKey(ctx, updated, key), "previous holds an empty non-nil slice")
	require.NoError(t, store.DeleteMetadataKey(ctx, "owner", &updated))
}
