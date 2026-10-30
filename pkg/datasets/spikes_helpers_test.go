package datasets

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem" // register the "mem" KV driver
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
)

// spikeFixture is a real catalog.Store (graveler + committed ranges) on an in-memory KV store
// and an in-memory block adapter, as DATASETS_DESIGN.md section 15.1 prescribes. It also exposes
// a ref.Manager over the same KV store so the spikes can call the ref-manager entry points that
// the GC job uses (GCBranchIterator) and inspect branch hidden-ness.
type spikeFixture struct {
	ctx     context.Context
	cat     *catalog.Catalog
	refMgr  *ref.Manager
	kvStore kv.Store
}

func newSpikeFixture(t *testing.T) *spikeFixture {
	t.Helper()
	ctx := t.Context()

	// The catalog builds its block adapter from the process configuration; force the mem adapter.
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	testutil.MustDo(t, "build config", err)
	cfg.Committed.LocalCache.Dir = t.TempDir()
	// Disable the repository and commit caches: they are wall-clock caches (default expiry is
	// seconds), so a create-after-delete assertion would race with them. This is a test-only
	// knob, like the settings cache override below. The pin manager reads repository state
	// through the catalog.
	cfg.Graveler.RepositoryCache.Size = 0
	cfg.Graveler.CommitCache.Size = 0

	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open mem kv store")
	t.Cleanup(func() { store.Close() })

	cat, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      store,
		PathProvider: upload.DefaultPathProvider,
		// Repository settings (branch protection rules) are cached for ~3s by default, which would
		// make a protection test race with the cache. Shorten it to 1ms; this is a test-only knob.
		SettingsManagerOption: settings.WithCache(cache.NewCache(1000, time.Millisecond, cache.NewJitterFn(0))),
	})
	require.NoError(t, err, "build catalog")
	t.Cleanup(func() { _ = cat.Close() })

	refMgr := ref.NewRefManager(ref.ManagerConfig{
		Executor:        batch.NopExecutor(),
		KVStore:         store,
		KVStoreLimited:  store,
		AddressProvider: ident.NewHexAddressProvider(),
	}, cfg.StorageConfig())

	return &spikeFixture{
		ctx:     ctx,
		cat:     cat,
		refMgr:  refMgr,
		kvStore: store,
	}
}

// createRepository creates a repository whose storage namespace is unique within the fixture.
func (f *spikeFixture) createRepository(t *testing.T, name, defaultBranch string, readOnly bool) {
	t.Helper()
	_, err := f.cat.CreateRepository(f.ctx, name, "", "mem://"+name+"-ns", defaultBranch, readOnly)
	require.NoError(t, err, "create repository %s", name)
}

// repositoryRecord returns the graveler repository record (with its InstanceUID) as the catalog
// itself resolves it, so partitions match.
func (f *spikeFixture) repositoryRecord(t *testing.T, name string) *graveler.RepositoryRecord {
	t.Helper()
	rec, err := f.refMgr.GetRepository(f.ctx, graveler.RepositoryID(name))
	require.NoError(t, err, "get repository record %s", name)
	return rec
}

// commitEntry stages one entry and commits it, returning the new commit ID.
func (f *spikeFixture) commitEntry(t *testing.T, repo, branch, entryPath, physicalAddress string, addressType catalog.AddressType, opts ...graveler.SetOptionsFunc) graveler.CommitID {
	t.Helper()
	err := f.cat.CreateEntry(f.ctx, repo, branch, catalog.DBEntry{
		Path:            entryPath,
		PhysicalAddress: physicalAddress,
		AddressType:     addressType,
		CreationDate:    time.Now(),
		Size:            42,
		Checksum:        "deadbeef",
	}, opts...)
	require.NoError(t, err, "create entry %s", entryPath)

	log, err := f.cat.Commit(f.ctx, repo, branch, "commit "+entryPath, "spike", nil, nil, nil, false, opts...)
	require.NoError(t, err, "commit %s", entryPath)
	return graveler.CommitID(log.Reference)
}

// branchNames lists branch names as the default (non-hidden) listing returns them.
func (f *spikeFixture) branchNames(t *testing.T, repo string) []string {
	t.Helper()
	branches, _, err := f.cat.ListBranches(f.ctx, repo, "", -1, "")
	require.NoError(t, err, "list branches of %s", repo)
	names := make([]string, len(branches))
	for i, b := range branches {
		names[i] = b.Name
	}
	return names
}

// sliceValueIterator is a minimal strictly-key-sorted graveler.ValueIterator, used to feed
// WriteMetaRangeByIterator the way the publish path does (DATASETS_DESIGN.md section 6.2 step 7).
type sliceValueIterator struct {
	records []*graveler.ValueRecord
	idx     int
}

func newSliceValueIterator(records []*graveler.ValueRecord) *sliceValueIterator {
	return &sliceValueIterator{records: records, idx: -1}
}

func (it *sliceValueIterator) Next() bool {
	it.idx++
	return it.idx < len(it.records)
}

func (it *sliceValueIterator) SeekGE(id graveler.Key) {
	it.idx = -1
	for i, rec := range it.records {
		if string(rec.Key) >= string(id) {
			it.idx = i - 1
			return
		}
	}
	it.idx = len(it.records) - 1
}

func (it *sliceValueIterator) Value() *graveler.ValueRecord {
	if it.idx < 0 || it.idx >= len(it.records) {
		return nil
	}
	return it.records[it.idx]
}

func (it *sliceValueIterator) Err() error { return nil }

func (it *sliceValueIterator) Close() {}

var _ graveler.ValueIterator = (*sliceValueIterator)(nil)
