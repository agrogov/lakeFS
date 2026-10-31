package datasets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	store, kvs, ctx := newTestStore(t)
	_ = store
	return NewStore(&etagStore{Store: kvs}), ctx
}

// TestStoreCASWithOpaquePredicate is BUG-1: every CAS must use the predicate that
// kv.Get returned, so it works on a store (CosmosDB) whose predicate is not the value.
func TestStoreCASWithOpaquePredicate(t *testing.T) {
	store, ctx := newETagStore(t)

	require.NoError(t, store.CreateDataset(ctx, newDatasetRecord()))
	current, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)

	next := *current
	next.State = DatasetStatePublishing
	next.PendingVersion = current.LatestVersion + 1
	require.NoError(t, store.PutDatasetIf(ctx, &next, current), "PutDatasetIf after GetDataset")

	stale := *current
	stale.State = DatasetStateDeleting
	require.ErrorIs(t, store.PutDatasetIf(ctx, &stale, current), kv.ErrPredicateFailed)

	require.ErrorIs(t, store.DeleteDataset(ctx, "customer-360", current), kv.ErrPredicateFailed)
	require.NoError(t, store.DeleteDataset(ctx, "customer-360", &next))

	_, err = store.AddPinReference(ctx, "lakehouse-repo", "30f9eb1c", PinReference("customer-360", 2))
	require.NoError(t, err)
	_, err = store.AddPinReference(ctx, "lakehouse-repo", "30f9eb1c", PinReference("customer-360", 3))
	require.NoError(t, err)
	_, err = store.RemovePinReference(ctx, "lakehouse-repo", "30f9eb1c", PinReference("customer-360", 2))
	require.NoError(t, err)
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
	require.NoError(t, store.PutDatasetIf(ctx, &next, rec), "previous holds a monotonic time.Now()")

	key := &MetadataKey{Name: "owner", Type: "string", AllowedValues: []string{}, CreatedAt: time.Now(), CreatedBy: "alice"}
	require.NoError(t, store.PutMetadataKey(ctx, *key, nil))
	updated := *key
	updated.Description = "d"
	require.NoError(t, store.PutMetadataKey(ctx, updated, key), "previous holds an empty non-nil slice")
	require.NoError(t, store.DeleteMetadataKey(ctx, "owner", &updated))
}
