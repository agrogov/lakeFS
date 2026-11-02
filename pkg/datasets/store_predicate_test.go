package datasets

// Tests of the §5.4 compare-and-swap contract against a backend whose predicate is not the
// stored value (BUG-1).
//
// postgres, DynamoDB, local and mem all return the stored bytes as the predicate, so a
// predicate synthesized by re-encoding a record - or by comparing the stored bytes with a
// re-encoded record - works on them and the bug is invisible. CosmosDB returns the
// document's ETag instead (pkg/kv/cosmosdb/store.go), so every such write fails there. The
// in-memory kv.Store cannot express that, so these tests drive the Store with opaqueKV: a
// fake kv.Store that hands out an opaque, rotating token as the predicate and whose SetIf
// accepts nothing but that token.

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
)

// opaquePredicate is the kv.Predicate opaqueKV hands out. It is deliberately not []byte: a
// predicate a caller builds itself - by re-encoding a record, as the bug did - can never be
// confused with one the store issued, which is exactly the CosmosDB ETag situation.
type opaquePredicate struct {
	token string
}

// opaqueItem is one stored entry: the value plus the opaque token that is currently the only
// predicate able to replace or delete it.
type opaqueItem struct {
	value []byte
	token string
}

// opaqueKV is a kv.Store whose predicates are opaque tokens rotated on every write, like a
// CosmosDB ETag. SetIf compares the presented predicate with the stored token and nothing
// else, so a write conditioned on a value-derived predicate always fails.
type opaqueKV struct {
	mu    sync.Mutex
	items map[opaqueKey]opaqueItem
	tags  int
}

type opaqueKey struct {
	partition string
	key       string
}

func newOpaqueKV() *opaqueKV {
	return &opaqueKV{items: make(map[opaqueKey]opaqueItem)}
}

// write stores value and rotates the entry's token, returning the new predicate. Rotating on
// every write is what makes a stale predicate stale even when the value did not change.
func (s *opaqueKV) write(k opaqueKey, value []byte) opaquePredicate {
	s.tags++
	item := opaqueItem{value: bytes.Clone(value), token: fmt.Sprintf("etag-%d", s.tags)}
	s.items[k] = item
	return opaquePredicate{token: item.token}
}

func (s *opaqueKV) Get(_ context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[opaqueKey{partition: string(partitionKey), key: string(key)}]
	if !ok {
		return nil, kv.ErrNotFound
	}
	return &kv.ValueWithPredicate{
		Value:     bytes.Clone(item.value),
		Predicate: opaquePredicate{token: item.token},
	}, nil
}

func (s *opaqueKV) Set(_ context.Context, partitionKey, key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.write(opaqueKey{partition: string(partitionKey), key: string(key)}, value)
	return nil
}

func (s *opaqueKV) SetIf(_ context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := opaqueKey{partition: string(partitionKey), key: string(key)}
	stored, exists := s.items[k]
	switch predicate := valuePredicate.(type) {
	case nil:
		// nil means "the key must not exist yet", as on every other backend.
		if exists {
			return kv.ErrPredicateFailed
		}
	case opaquePredicate:
		if !exists || predicate.token != stored.token {
			return kv.ErrPredicateFailed
		}
	default:
		// Any other predicate type can never match: this is the CosmosDB behaviour that a
		// value-derived predicate runs into.
		return kv.ErrPredicateFailed
	}
	s.write(k, value)
	return nil
}

func (s *opaqueKV) Delete(_ context.Context, partitionKey, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, opaqueKey{partition: string(partitionKey), key: string(key)})
	return nil
}

func (s *opaqueKV) Scan(_ context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]kv.Entry, 0, len(s.items))
	for k, item := range s.items {
		if k.partition != string(partitionKey) {
			continue
		}
		key := []byte(k.key)
		if len(options.KeyStart) > 0 && bytes.Compare(key, options.KeyStart) < 0 {
			continue
		}
		entries = append(entries, kv.Entry{
			PartitionKey: []byte(k.partition),
			Key:          key,
			Value:        bytes.Clone(item.value),
		})
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	return &opaqueIterator{entries: entries, pos: -1}, nil
}

func (s *opaqueKV) Close() {}

// opaqueIterator is the Scan iterator of opaqueKV over a snapshot of one partition.
type opaqueIterator struct {
	entries []kv.Entry
	pos     int
}

func (it *opaqueIterator) Next() bool {
	it.pos++
	return it.pos >= 0 && it.pos < len(it.entries)
}

func (it *opaqueIterator) SeekGE(key []byte) {
	it.pos = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].Key, key) >= 0
	}) - 1 // Next moves onto the first entry at or after key
}

func (it *opaqueIterator) Entry() *kv.Entry {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return nil
	}
	entry := it.entries[it.pos]
	return &entry
}

func (it *opaqueIterator) Err() error { return nil }

func (it *opaqueIterator) Close() {}

// newOpaqueStore returns a Store over a fresh opaqueKV.
func newOpaqueStore(t *testing.T) (*Store, *opaqueKV, context.Context) {
	t.Helper()
	kvs := newOpaqueKV()
	return NewStore(kvs), kvs, t.Context()
}

// TestStorePutDatasetIfOpaquePredicate is BUG-1 for the dataset head: PutDatasetIf after
// GetDataset must succeed on a backend whose predicate is not the record's bytes, and the
// predicate of a re-encoded record must be rejected.
func TestStorePutDatasetIfOpaquePredicate(t *testing.T) {
	store, _, ctx := newOpaqueStore(t)

	require.NoError(t, store.CreateDataset(ctx, newDatasetRecord()), "create with a nil predicate")

	// GetDataset hands back the backend's opaque predicate, not the record's bytes.
	current, predicate, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)
	require.NotNil(t, predicate)

	// The bug: a predicate re-encoded from the record the caller read. It matches the stored
	// value byte for byte and still fails, because the backend's predicate is its own token.
	reencoded, err := datasetValue(current)
	require.NoError(t, err)
	publishing := *current
	publishing.State = DatasetStatePublishing
	publishing.PendingVersion = current.LatestVersion + 1
	err = store.PutDatasetIf(ctx, &publishing, current, reencoded)
	require.ErrorIs(t, err, kv.ErrPredicateFailed, "a predicate derived from the record must not be accepted")

	// The fix: the predicate GetDataset returned.
	require.NoError(t, store.PutDatasetIf(ctx, &publishing, current, predicate), "PutDatasetIf after GetDataset must succeed")

	// The next CAS re-reads the record and presents the predicate of that read.
	_, publishingPredicate, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)
	active := publishing
	active.State = DatasetStateActive
	active.PendingVersion = 0
	active.LatestVersion = publishing.PendingVersion
	require.NoError(t, store.PutDatasetIf(ctx, &active, &publishing, publishingPredicate))

	stored, storedPredicate, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)
	require.Equal(t, DatasetStateActive, stored.State)
	require.Equal(t, publishing.PendingVersion, stored.LatestVersion)

	// A conditional delete is bounded by a predicate too: a stale one is refused, the current
	// one deletes.
	require.ErrorIs(t, store.DeleteDataset(ctx, "customer-360", &publishing, publishingPredicate), kv.ErrPredicateFailed)
	require.NoError(t, store.DeleteDataset(ctx, "customer-360", stored, storedPredicate))
	_, _, err = store.GetDataset(ctx, "customer-360")
	require.ErrorIs(t, err, kv.ErrNotFound)
}

// TestStorePinReferencesOpaquePredicate is BUG-1 for the GC pins: the reference
// compare-and-swap of AddPinReference, RemovePinReference and RemovePinReferences must
// condition on the predicate the store returned, or no reference is ever recorded.
func TestStorePinReferencesOpaquePredicate(t *testing.T) {
	store, _, ctx := newOpaqueStore(t)
	const repository, commitID = "lakehouse-repo", "30f9eb1c"

	// The first reference creates the pin record.
	rec, err := store.AddPinReference(ctx, repository, commitID, PinReference("orders", 1))
	require.NoError(t, err)
	require.Equal(t, []string{"orders@v1"}, rec.References)

	// The second reference is a CAS on the record that is there; it fails if the update is
	// conditioned on anything but the store's predicate.
	rec, err = store.AddPinReference(ctx, repository, commitID, PinReference("orders", 2))
	require.NoError(t, err)
	require.Equal(t, []string{"orders@v1", "orders@v2"}, rec.References)

	// Adding an existing reference is still a no-op.
	rec, err = store.AddPinReference(ctx, repository, commitID, PinReference("orders", 2))
	require.NoError(t, err)
	require.Equal(t, []string{"orders@v1", "orders@v2"}, rec.References)

	// A pin is one record per reference, so GetPin has no single predicate to hand out.
	stored, _, err := store.GetPin(ctx, repository, commitID)
	require.NoError(t, err)
	require.Equal(t, rec.References, stored.References)

	// Removing one reference CASes the record down to a non-empty set.
	deleted, err := store.RemovePinReference(ctx, repository, commitID, PinReference("orders", 1))
	require.NoError(t, err)
	require.False(t, deleted, "a pin with a remaining reference is not an orphan")
	stored, _, err = store.GetPin(ctx, repository, commitID)
	require.NoError(t, err)
	require.Equal(t, []string{"orders@v2"}, stored.References)

	// The bulk form (used by a dataset delete) is a CAS too.
	deleted, err = store.RemovePinReferences(ctx, repository, commitID, []string{PinReference("orders", 2)})
	require.NoError(t, err)
	require.True(t, deleted, "the pin became an orphan and the branch must go")
	_, _, err = store.GetPin(ctx, repository, commitID)
	require.ErrorIs(t, err, kv.ErrNotFound, "an orphaned pin record is deleted")
}

// TestServicePublishOnOpaquePredicateStore runs the whole publish state machine of §5.4
// (create -> publishing -> active, then publishing -> active again) with the dataset records
// on the opaque-predicate backend: not just the first CAS but every one in the chain - the
// create, the CAS to "publishing", the CAS to "active", and the pin reference CAS - has to
// present a predicate the store issued, or no publish can ever be activated.
func TestServicePublishOnOpaquePredicateStore(t *testing.T) {
	// The datasets control plane runs on the opaque backend; the catalog, the auth service and
	// the block adapter keep the fixture's mem store.
	f := newServiceFixtureOnStore(t, newOpaqueKV())

	commit := f.sourceRepo(t, "src", map[string]string{
		"data/customers/a.parquet": "alpha",
		"data/customers/b.parquet": "bravo",
		"data/other/c.parquet":     "charlie",
	})
	def := prefixDefinition("src", commit.String(), "customers", "data/customers/")

	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       "customer-360",
		Definition: def,
		Message:    "v1",
	})
	require.NoError(t, err, "create with publish must complete on an opaque-predicate store")
	require.Equal(t, DatasetStateActive, view.Record.State)
	require.Equal(t, 1, view.Record.LatestVersion)

	// A wider item over the same source commit: the data differs, so §6.2 step 8's no-op check
	// accepts it, and the pin of that commit takes a second reference through a second CAS.
	wider := prefixDefinition("src", commit.String(), "all", "data/")
	published, err := f.svc.PublishVersion(f.ctx, f.caller(), "customer-360", PublishVersionRequest{
		Definition: wider,
		Message:    "v2",
	})
	require.NoError(t, err, "the second publish CASes on the predicate of the previous one")
	require.NotNil(t, published.Version)
	require.Equal(t, 2, published.Version.Version)

	record, _, err := f.svc.store.GetDataset(f.ctx, "customer-360")
	require.NoError(t, err)
	require.Equal(t, 2, record.LatestVersion)
	require.Equal(t, DatasetStateActive, record.State)
	require.Zero(t, record.PendingVersion)

	// The pin of the source commit carries both versions: the reference CAS ran twice on the
	// opaque backend and lost nothing.
	pin, _, err := f.svc.store.GetPin(f.ctx, "src", commit.String())
	require.NoError(t, err)
	require.Equal(t, []string{PinReference("customer-360", 1), PinReference("customer-360", 2)}, pin.References)
}

// TestStoreLeaseOpaquePredicate is BUG-1 for the reconcile lease: taking over, re-acquiring
// and releasing the lease must all condition on the predicate the store returned, so a
// second reconcile node cannot run a pass concurrently on CosmosDB.
func TestStoreLeaseOpaquePredicate(t *testing.T) {
	store, kvs, ctx := newOpaqueStore(t)

	// No lease yet: it is created with a nil predicate.
	held, err := store.AcquireLease(ctx, "node-a", time.Minute)
	require.NoError(t, err)
	require.True(t, held)

	// A live lease held by another owner is left alone.
	held, err = store.AcquireLease(ctx, "node-b", time.Minute)
	require.NoError(t, err)
	require.False(t, held)

	// Re-acquiring as the owner rewrites the record, conditioned on the predicate of the
	// record that is there.
	held, err = store.AcquireLease(ctx, "node-a", time.Minute)
	require.NoError(t, err)
	require.True(t, held, "the owner may renew its own lease")

	// An expired lease is taken over the same way: this is the path a crashed node leaves and
	// the one that silently stopped all repairs when the predicate was re-encoded.
	expired := []byte(`{"owner":"node-a","expires_at":"2000-01-01T00:00:00Z"}`)
	require.NoError(t, kvs.Set(ctx, []byte(PartitionKey), []byte(ReconcileLeaseKey), expired))
	held, err = store.AcquireLease(ctx, "node-b", time.Minute)
	require.NoError(t, err)
	require.True(t, held, "an expired lease must be takeable")

	// Releasing is a no-op for a lease owned by somebody else, and it keeps the lease.
	require.NoError(t, store.ReleaseLease(ctx, "node-a"))
	current, _, err := store.GetLease(ctx)
	require.NoError(t, err)
	require.Equal(t, "node-b", current.Owner)

	// The owner releases it, conditioned on the predicate it read, and the next node wins it.
	require.NoError(t, store.ReleaseLease(ctx, "node-b"))
	_, _, err = store.GetLease(ctx)
	require.ErrorIs(t, err, kv.ErrNotFound)
	held, err = store.AcquireLease(ctx, "node-a", time.Minute)
	require.NoError(t, err)
	require.True(t, held)
}
