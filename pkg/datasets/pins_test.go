// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// pins_test.go covers the GC pin manager of design §8.2 against a real catalog (mem block adapter)
// and a real KV store (§15.1): pin branch naming, source deduplication, the "pins before the data
// commit" ordering, the reference count shared by several datasets, release on dataset delete,
// reconciler-side recreation, and degraded detection when a source repository disappears.
//
// The end-to-end GC regression test - the one that proves a deleted source branch cannot collect
// pinned data - lives in pins_gc_test.go.
package datasets

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// pinFixture is the spike fixture (catalog + ref manager on mem KV) plus the datasets store and the
// pin manager built on them.
type pinFixture struct {
	*spikeFixture
	store *Store
	pins  *Pins
}

func newPinFixture(t *testing.T) *pinFixture {
	t.Helper()
	f := newSpikeFixture(t)
	store := NewStore(f.kvStore)
	return &pinFixture{spikeFixture: f, store: store, pins: NewPins(store, f.cat)}
}

// sourceRepo creates a source repository with one commit on its main branch and returns that
// commit, i.e. the commit a dataset version would pin.
func (f *pinFixture) sourceRepo(t *testing.T, repo, key string) graveler.CommitID {
	t.Helper()
	f.createRepository(t, repo, "main", false)
	return f.commitEntry(t, repo, "main", key, key, catalog.AddressTypeRelative)
}

// nextCommit adds another commit on branch and returns it.
func (f *pinFixture) nextCommit(t *testing.T, repo, branch, key string) graveler.CommitID {
	t.Helper()
	return f.commitEntry(t, repo, branch, key, key, catalog.AddressTypeRelative)
}

// pinBranchAt returns the commit a pin branch points at, and whether the branch exists.
func (f *pinFixture) pinBranchAt(t *testing.T, repo, commitID string) (string, bool) {
	t.Helper()
	got, err := f.cat.GetBranchReference(f.ctx, repo, PinBranch(commitID))
	if errors.Is(err, graveler.ErrBranchNotFound) {
		return "", false
	}
	require.NoError(t, err, "get pin branch %s/%s", repo, PinBranch(commitID))
	return got, true
}

// requirePinBranch asserts that repo has a hidden pin branch at commitID.
func (f *pinFixture) requirePinBranch(t *testing.T, repo, commitID string) {
	t.Helper()
	got, ok := f.pinBranchAt(t, repo, commitID)
	require.True(t, ok, "pin branch %s/%s must exist", repo, PinBranch(commitID))
	require.Equal(t, commitID, got, "pin branch must point at the pinned commit")

	rec := f.repositoryRecord(t, repo)
	branch, err := f.refMgr.GetBranch(f.ctx, rec, graveler.BranchID(PinBranch(commitID)))
	require.NoError(t, err)
	require.True(t, branch.Hidden, "pin branch must be hidden (§8.2)")

	// Hidden from the default listing, so users do not see pins in the UI or lakectl.
	require.NotContains(t, f.branchNames(t, repo), PinBranch(commitID))
}

// definitionOf builds a definition whose items are pinned to the given (repository, commit) pairs.
func definitionOf(pairs ...PinnedSource) Definition {
	data := make([]DataItem, 0, len(pairs))
	for i, src := range pairs {
		name := fmt.Sprintf("item%c", 'a'+i)
		data = append(data, DataItem{
			Target:     name,
			Repository: src.Repository,
			Ref:        SourceRef{Type: RefTypeCommit, ID: src.CommitID},
			Type:       ItemTypePrefix,
			Prefix:     &PathSource{Path: "data/" + name + "/"},
		})
	}
	return Definition{FormatVersion: FormatVersion, Data: data}
}

// pinRefs returns the reference set of the pin of (repo, commit).
func (f *pinFixture) pinRefs(t *testing.T, repo, commitID string) []string {
	t.Helper()
	rec, err := f.store.GetPin(f.ctx, repo, commitID)
	require.NoError(t, err, "pin record %s/%s", repo, commitID)
	return rec.References
}

func TestPinBranchNames(t *testing.T) {
	t.Parallel()
	const commitID = "30f9eb1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f90"

	require.Equal(t, "_lakefs_ds_pin_"+commitID, PinBranch(commitID))
	require.True(t, IsPinBranch(PinBranch(commitID)))
	require.False(t, IsPinBranch("main"))
	require.False(t, IsPinBranch(PinBranchPrefix), "the empty suffix is not a pin")

	got, ok := PinCommitID(PinBranch(commitID))
	require.True(t, ok)
	require.Equal(t, commitID, got)

	_, ok = PinCommitID("_lakefs_ds_pin")
	require.False(t, ok)
	_, ok = PinCommitID("feature/_lakefs_ds_pin_" + commitID)
	require.False(t, ok, "a pin branch is at the top level of the source repository")

	// The branch name must be a legal branch ID (design §8.2 names the branch after the commit).
	require.NoError(t, graveler.ValidateBranchID(graveler.BranchID(PinBranch(commitID))))
}

func TestPinSources(t *testing.T) {
	t.Parallel()

	// Two items from one commit and one from another: the distinct pairs are the pins (§8.2).
	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{
		{Target: "b", Repository: "source-two", Ref: SourceRef{Type: RefTypeCommit, ID: "bb"}, Type: ItemTypeObject, Object: &PathSource{Path: "b"}},
		{Target: "a", Repository: "source-one", Ref: SourceRef{Type: RefTypeCommit, ID: "aa"}, Type: ItemTypePrefix, Prefix: &PathSource{Path: "a/"}},
		{Target: "c", Repository: "source-one", Ref: SourceRef{Type: RefTypeCommit, ID: "aa"}, Type: ItemTypeObject, Object: &PathSource{Path: "c"}},
	}}
	sources, err := PinSources(def)
	require.NoError(t, err)
	require.Equal(t, []PinnedSource{
		{Repository: "source-one", CommitID: "aa"},
		{Repository: "source-two", CommitID: "bb"},
	}, sources, "one pin per distinct (repository, commit), sorted")

	// A definition that is not resolved yet must fail closed, not pin nothing (§5.2, §8.2).
	for name, item := range map[string]DataItem{
		"branch ref": {Target: "a", Repository: "source-one", Ref: SourceRef{Type: RefTypeBranch, ID: "main"}},
		"tag ref":    {Target: "a", Repository: "source-one", Ref: SourceRef{Type: RefTypeTag, ID: "v1"}},
		"empty ref":  {Target: "a", Repository: "source-one", Ref: SourceRef{Type: RefTypeCommit}},
		"no repo":    {Target: "a", Ref: SourceRef{Type: RefTypeCommit, ID: "aa"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := PinSources(Definition{FormatVersion: FormatVersion, Data: []DataItem{item}})
			require.ErrorIs(t, err, ErrUnresolvedPinSource)
		})
	}

	// A definition with no items yields no pins.
	sources, err = PinSources(Definition{FormatVersion: FormatVersion})
	require.NoError(t, err)
	require.Empty(t, sources)
}

// recordingPinCatalog records the branch operations it forwards, so a test can assert the order of
// pin acquisition relative to the data commit (§6.2 steps 5 and 9).
type recordingPinCatalog struct {
	PinCatalog
	ops *[]string
}

func (r *recordingPinCatalog) CreateBranch(ctx context.Context, repositoryID, branch, sourceRef string, opts ...graveler.SetOptionsFunc) (*catalog.CommitLog, error) {
	*r.ops = append(*r.ops, "create_branch:"+repositoryID+"/"+branch)
	return r.PinCatalog.CreateBranch(ctx, repositoryID, branch, sourceRef, opts...)
}

func (r *recordingPinCatalog) DeleteBranch(ctx context.Context, repositoryID, branch string, opts ...graveler.SetOptionsFunc) error {
	*r.ops = append(*r.ops, "delete_branch:"+repositoryID+"/"+branch)
	return r.PinCatalog.DeleteBranch(ctx, repositoryID, branch, opts...)
}

// TestAcquirePinsBeforeCommit pins the ordering rule of §6.2/§14: at the moment the version's data
// is committed, every pin branch already exists. Never under-retain.
func TestAcquirePinsBeforeCommit(t *testing.T) {
	const (
		dataset = "customer-360"
		ref     = "customer-360@v1"
	)
	f := newPinFixture(t)
	ctx := f.ctx

	first := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	f.createRepository(t, "source-two", "main", false)
	second := f.commitEntry(t, "source-two", "main", "geo/countries.csv", "geo/countries.csv", catalog.AddressTypeRelative)

	def := definitionOf(
		PinnedSource{Repository: "source-one", CommitID: first.String()},
		PinnedSource{Repository: "source-two", CommitID: second.String()},
	)

	var ops []string
	pins := NewPins(f.store, &recordingPinCatalog{PinCatalog: f.cat, ops: &ops})

	// §6.2 step 5: pins first.
	acquired, err := pins.Acquire(ctx, def, ref)
	require.NoError(t, err)
	require.Len(t, acquired, 2)

	// At this instant - before anything is committed to the dataset - the pins must already exist,
	// hold the reference, and point at the pinned commits.
	for _, src := range acquired {
		f.requirePinBranch(t, src.Repository, src.CommitID)
		require.Equal(t, []string{ref}, f.pinRefs(t, src.Repository, src.CommitID))
	}

	// §6.2 steps 7-10 (metarange, commit, tag) run only now.
	ops = append(ops, "commit")
	require.Equal(t, []string{
		"create_branch:source-one/" + PinBranch(first.String()),
		"create_branch:source-two/" + PinBranch(second.String()),
		"commit",
	}, ops, "every pin branch must be created before the data commit")
}

// TestPinRefcountSharedAcrossDatasets covers the reference count of §8.2: one pin, several dataset
// versions, and the branch disappearing exactly when the last reference goes.
func TestPinRefcountSharedAcrossDatasets(t *testing.T) {
	f := newPinFixture(t)
	ctx := f.ctx
	commit := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	src := PinnedSource{Repository: "source-one", CommitID: commit.String()}
	def := definitionOf(src)

	// Dataset customer-360 pins it at v1 and v2, dataset events at v1: three references, one branch.
	for _, ref := range []string{PinReference("customer-360", 1), PinReference("customer-360", 2), PinReference("events", 1)} {
		_, err := f.pins.Acquire(ctx, def, ref)
		require.NoError(t, err)
	}
	require.Equal(t, []string{"customer-360@v1", "customer-360@v2", "events@v1"},
		f.pinRefs(t, src.Repository, src.CommitID), "references are a sorted set")

	// Acquiring the same reference twice is a no-op.
	_, err := f.pins.Acquire(ctx, def, PinReference("customer-360", 1))
	require.NoError(t, err)
	require.Len(t, f.pinRefs(t, src.Repository, src.CommitID), 3)

	// Releasing one dataset version keeps the branch for the others.
	require.NoError(t, f.pins.Release(ctx, def, PinReference("customer-360", 1)))
	require.Equal(t, []string{"customer-360@v2", "events@v1"}, f.pinRefs(t, src.Repository, src.CommitID))
	f.requirePinBranch(t, src.Repository, src.CommitID)

	// Releasing a reference that is not there is a no-op, so a retried delete converges (§6.6).
	require.NoError(t, f.pins.Release(ctx, def, PinReference("customer-360", 1)))
	f.requirePinBranch(t, src.Repository, src.CommitID)

	// The last reference removes the record and deletes the branch (§8.2).
	require.NoError(t, f.pins.Release(ctx, def, PinReference("customer-360", 2)))
	_, err = f.store.GetPin(ctx, src.Repository, src.CommitID)
	require.NoError(t, err, "the pin still has the events@v1 reference")
	require.NoError(t, f.pins.Release(ctx, def, PinReference("events", 1)))

	_, err = f.store.GetPin(ctx, src.Repository, src.CommitID)
	require.ErrorIs(t, err, kv.ErrNotFound, "the pin record must be gone at refcount zero")
	_, ok := f.pinBranchAt(t, src.Repository, src.CommitID)
	require.False(t, ok, "the pin branch must be deleted at refcount zero")
}

// TestReleaseDatasetDeletesOrphanPins covers §6.6 step 3: deleting a dataset releases every pin of
// every version, and only the pins that become orphans lose their branch.
func TestReleaseDatasetDeletesOrphanPins(t *testing.T) {
	f := newPinFixture(t)
	ctx := f.ctx
	commit := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	src := PinnedSource{Repository: "source-one", CommitID: commit.String()}
	def := definitionOf(src)

	// customer-360 v1 and v2 both reference the same commit; events v1 shares a different one.
	for _, ref := range []string{PinReference("customer-360", 1), PinReference("customer-360", 2)} {
		_, err := f.pins.Acquire(ctx, def, ref)
		require.NoError(t, err)
	}
	otherCommit := f.nextCommit(t, "source-one", "main", "data/b/orders.parquet")
	_, err := f.pins.Acquire(ctx, definitionOf(PinnedSource{Repository: "source-one", CommitID: otherCommit.String()}),
		PinReference("events", 1))
	require.NoError(t, err)

	// Deleting customer-360 releases both of its versions at once.
	require.NoError(t, f.pins.ReleaseDataset(ctx, "customer-360"))
	_, err = f.store.GetPin(ctx, src.Repository, src.CommitID)
	require.ErrorIs(t, err, kv.ErrNotFound)
	_, ok := f.pinBranchAt(t, src.Repository, src.CommitID)
	require.False(t, ok)

	// The other dataset's pin is untouched.
	require.Equal(t, []string{"events@v1"}, f.pinRefs(t, src.Repository, otherCommit.String()))
	f.requirePinBranch(t, src.Repository, otherCommit.String())

	// A dataset with no pins releases cleanly (and the deleted pin list is empty).
	require.NoError(t, f.pins.ReleaseDataset(ctx, "no-such-dataset"))
}

// TestListDatasetPins covers the KV lookup the delete path and the degraded check use: pins are
// keyed by source, so the dataset's pins have to be filtered out of the partition.
func TestListDatasetPins(t *testing.T) {
	f := newPinFixture(t)
	ctx := f.ctx
	first := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	second := f.nextCommit(t, "source-one", "main", "data/b/orders.parquet")
	third := f.nextCommit(t, "source-one", "main", "data/c/events.parquet")

	// customer-360 and a name that shares its prefix, to make sure the filter is exact.
	for _, c := range []graveler.CommitID{first, second} {
		_, err := f.pins.Acquire(ctx, definitionOf(PinnedSource{Repository: "source-one", CommitID: c.String()}),
			PinReference("customer-360", 1))
		require.NoError(t, err)
	}
	_, err := f.pins.Acquire(ctx, definitionOf(PinnedSource{Repository: "source-one", CommitID: third.String()}),
		PinReference("customer-3600", 1))
	require.NoError(t, err)

	pins, err := f.store.ListDatasetPins(ctx, "customer-360")
	require.NoError(t, err)
	require.Len(t, pins, 2, "only the pins that carry a customer-360 reference")
	for _, pin := range pins {
		require.Equal(t, []string{"customer-360@v1"}, pin.References)
	}

	all, err := f.store.ListDatasetPins(ctx, "customer-3600")
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, third.String(), all[0].CommitID)
}

// TestEnsureRecreatesMissingPin covers §12.3 item 2: an administrator (or a mistake) deleted a pin
// branch, and the reconciler's repair hook recreates it, hidden and pointing at the pinned commit,
// without touching the reference set.
func TestEnsureRecreatesMissingPin(t *testing.T) {
	const dataset = "customer-360"
	f := newPinFixture(t)
	ctx := f.ctx
	commit := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	src := PinnedSource{Repository: "source-one", CommitID: commit.String()}
	def := definitionOf(src)
	ref := PinReference(dataset, 1)

	_, err := f.pins.Acquire(ctx, def, ref)
	require.NoError(t, err)

	// A no-op Ensure on a healthy pin reports nothing recreated.
	recreated, missing, err := f.pins.Ensure(ctx, def, ref)
	require.NoError(t, err)
	require.Empty(t, recreated)
	require.Empty(t, missing)

	// Simulate the manual deletion the deny policy of §8.2 is meant to prevent.
	require.NoError(t, f.cat.DeleteBranch(ctx, src.Repository, PinBranch(src.CommitID), graveler.WithForce(true)))
	_, ok := f.pinBranchAt(t, src.Repository, src.CommitID)
	require.False(t, ok)

	// The reference is still recorded: the pin record is what says the commit must stay protected.
	require.Equal(t, []string{ref}, f.pinRefs(t, src.Repository, src.CommitID))

	// The reconciler hook recreates the branch from the definition.
	recreated, missing, err = f.pins.Ensure(ctx, def, ref)
	require.NoError(t, err)
	require.Equal(t, []PinnedSource{src}, recreated)
	require.Empty(t, missing)
	f.requirePinBranch(t, src.Repository, src.CommitID)
	require.Equal(t, []string{ref}, f.pinRefs(t, src.Repository, src.CommitID))

	// The definition-free form (used when a version's definition is stored as a DefinitionRef) does
	// the same from the pin records alone.
	require.NoError(t, f.cat.DeleteBranch(ctx, src.Repository, PinBranch(src.CommitID), graveler.WithForce(true)))
	recreated, missing, err = f.pins.EnsureDatasetPins(ctx, dataset)
	require.NoError(t, err)
	require.Equal(t, []PinnedSource{src}, recreated)
	require.Empty(t, missing)
	f.requirePinBranch(t, src.Repository, src.CommitID)
}

// TestEnsureDetectsMovedPinBranch covers the other half of the repair hook: a pin branch that exists
// but no longer points at the pinned commit is a conflict, not a healthy pin. Silently accepting it
// would let the next GC run in the source repository collect the pinned commit.
func TestEnsureDetectsMovedPinBranch(t *testing.T) {
	f := newPinFixture(t)
	ctx := f.ctx
	commit := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	src := PinnedSource{Repository: "source-one", CommitID: commit.String()}
	def := definitionOf(src)

	_, err := f.pins.Acquire(ctx, def, PinReference("customer-360", 1))
	require.NoError(t, err)
	elsewhere := f.nextCommit(t, "source-one", "main", "data/b/orders.parquet")

	// Move the pin branch off the pinned commit (only possible for a caller that can delete and
	// create branches in the source repository).
	require.NoError(t, f.cat.DeleteBranch(ctx, src.Repository, PinBranch(src.CommitID), graveler.WithForce(true)))
	_, err = f.cat.CreateBranch(ctx, src.Repository, PinBranch(src.CommitID), elsewhere.String(),
		graveler.WithHidden(true), graveler.WithForce(true))
	require.NoError(t, err)

	_, _, err = f.pins.Ensure(ctx, def, PinReference("customer-360", 1))
	require.ErrorIs(t, err, ErrPinBranchConflict)

	status, err := f.pins.Status(ctx, src)
	require.NoError(t, err)
	require.True(t, status.RepositoryExists)
	require.True(t, status.PinBranchExists)
	require.Equal(t, elsewhere.String(), status.PinBranchCommit)
	require.False(t, status.Healthy())
}

// TestDegradedDetection covers §8.2 risks and §12.3 step 4: a deleted source repository makes the
// dataset (and the version that references it) degraded, and the check is idempotent and read-only
// for healthy datasets.
func TestDegradedDetection(t *testing.T) {
	const dataset = "customer-360"
	f := newPinFixture(t)
	ctx := f.ctx
	commit := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	src := PinnedSource{Repository: "source-one", CommitID: commit.String()}
	def := definitionOf(src)
	ref := PinReference(dataset, 1)

	_, err := f.pins.Acquire(ctx, def, ref)
	require.NoError(t, err)

	now := time.Now().UTC()
	require.NoError(t, f.store.CreateDataset(ctx, &DatasetRecord{
		Name: dataset, CreatedBy: "publisher", CreatedAt: now, UpdatedAt: now,
		State: DatasetStateActive, LatestVersion: 1,
	}))
	_, err = f.store.PutVersion(ctx, VersionRecord{
		Dataset: dataset, Version: 1, Definition: def, Publisher: "publisher",
		PublishedAt: now, CommitID: "dataset-commit", MetaRangeID: "metarange",
	})
	require.NoError(t, err)

	// Healthy: no missing source, no state change.
	status, err := f.pins.Status(ctx, src)
	require.NoError(t, err)
	require.True(t, status.Healthy())

	missing, err := f.pins.CheckVersionDegraded(ctx, dataset, 1)
	require.NoError(t, err)
	require.Empty(t, missing)

	missing, marked, err := f.pins.CheckDatasetDegraded(ctx, dataset)
	require.NoError(t, err)
	require.Empty(t, missing)
	require.False(t, marked)
	rec, err := f.store.GetDataset(ctx, dataset)
	require.NoError(t, err)
	require.Equal(t, DatasetStateActive, rec.State)

	// The source repository is deleted: its pins went with it (§8.2 risks).
	require.NoError(t, f.cat.DeleteRepository(ctx, "source-one", graveler.WithForce(true)))

	status, err = f.pins.Status(ctx, src)
	require.NoError(t, err, "a deleted source repository is a state, not an error")
	require.False(t, status.RepositoryExists)
	require.False(t, status.PinBranchExists)

	missing, err = f.pins.DegradedSources(ctx, []PinnedSource{src})
	require.NoError(t, err)
	require.Equal(t, []PinnedSource{src}, missing)

	missing, err = f.pins.CheckVersionDegraded(ctx, dataset, 1)
	require.NoError(t, err)
	require.Equal(t, []PinnedSource{src}, missing, "version 1 references the deleted repository")

	// No pin reference of that version: not degraded.
	missing, err = f.pins.CheckVersionDegraded(ctx, dataset, 2)
	require.NoError(t, err)
	require.Empty(t, missing)

	missing, marked, err = f.pins.CheckDatasetDegraded(ctx, dataset)
	require.NoError(t, err)
	require.Equal(t, []PinnedSource{src}, missing)
	require.True(t, marked, "the dataset must be marked degraded")
	rec, err = f.store.GetDataset(ctx, dataset)
	require.NoError(t, err)
	require.Equal(t, DatasetStateDegraded, rec.State)

	// Idempotent: marking again reports no change.
	_, marked, err = f.pins.CheckDatasetDegraded(ctx, dataset)
	require.NoError(t, err)
	require.False(t, marked)

	// The reconciler's repair hook reports the missing source instead of failing (§12.3 item 2).
	recreated, missing, err := f.pins.Ensure(ctx, def, ref)
	require.NoError(t, err)
	require.Empty(t, recreated)
	require.Equal(t, []PinnedSource{src}, missing)

	// Releasing after the source repository is gone still succeeds: the pin records go away and the
	// branch delete is tolerated (§6.6 "every step tolerates not found").
	require.NoError(t, f.pins.ReleaseDataset(ctx, dataset))
	_, err = f.store.GetPin(ctx, src.Repository, src.CommitID)
	require.ErrorIs(t, err, kv.ErrNotFound)
}

// TestDegradedDoesNotTouchTransientStates pins that the degraded check leaves a publish or delete in
// flight to the reconciler's first step (§12.3 item 1), instead of overwriting its state.
func TestDegradedDoesNotTouchTransientStates(t *testing.T) {
	const dataset = "customer-360"
	f := newPinFixture(t)
	ctx := f.ctx
	commit := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	src := PinnedSource{Repository: "source-one", CommitID: commit.String()}
	_, err := f.pins.Acquire(ctx, definitionOf(src), PinReference(dataset, 1))
	require.NoError(t, err)

	now := time.Now().UTC()
	require.NoError(t, f.store.CreateDataset(ctx, &DatasetRecord{
		Name: dataset, CreatedBy: "publisher", CreatedAt: now, UpdatedAt: now,
		State: DatasetStatePublishing, LatestVersion: 1, PendingVersion: 2,
	}))
	require.NoError(t, f.cat.DeleteRepository(ctx, "source-one", graveler.WithForce(true)))

	missing, marked, err := f.pins.CheckDatasetDegraded(ctx, dataset)
	require.NoError(t, err)
	require.Equal(t, []PinnedSource{src}, missing)
	require.False(t, marked, "a transient state belongs to the reconciler's rollback step")

	rec, err := f.store.GetDataset(ctx, dataset)
	require.NoError(t, err)
	require.Equal(t, DatasetStatePublishing, rec.State)
	require.Equal(t, 2, rec.PendingVersion)
}

// TestPinsFailClosed pins that a pin manager without its dependencies refuses to work instead of
// silently protecting nothing (§8.2: skipping a pin can lose data).
func TestPinsFailClosed(t *testing.T) {
	t.Parallel()
	def := definitionOf(PinnedSource{Repository: "source-one", CommitID: "aa"})
	for name, pins := range map[string]*Pins{
		"nil manager": nil,
		"no store":    NewPins(nil, nil),
		"no catalog":  NewPins(NewStore(nil), nil),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_, err := pins.Acquire(ctx, def, "customer-360@v1")
			require.ErrorIs(t, err, ErrPinsUnavailable)
			require.ErrorIs(t, pins.Release(ctx, def, "customer-360@v1"), ErrPinsUnavailable)
			require.ErrorIs(t, pins.ReleaseDataset(ctx, "customer-360"), ErrPinsUnavailable)
			_, _, err = pins.Ensure(ctx, def, "customer-360@v1")
			require.ErrorIs(t, err, ErrPinsUnavailable)
			_, _, err = pins.EnsureDatasetPins(ctx, "customer-360")
			require.ErrorIs(t, err, ErrPinsUnavailable)
			_, err = pins.CheckVersionDegraded(ctx, "customer-360", 1)
			require.ErrorIs(t, err, ErrPinsUnavailable)
			_, _, err = pins.CheckDatasetDegraded(ctx, "customer-360")
			require.ErrorIs(t, err, ErrPinsUnavailable)
			_, err = pins.Status(ctx, PinnedSource{Repository: "source-one", CommitID: "aa"})
			require.ErrorIs(t, err, ErrPinsUnavailable)
		})
	}
}

// failingPinCatalog fails every branch creation in one repository, to exercise the rollback path of
// Acquire.
type failingPinCatalog struct {
	PinCatalog
	repository string
}

func (r *failingPinCatalog) CreateBranch(ctx context.Context, repositoryID, branch, sourceRef string, opts ...graveler.SetOptionsFunc) (*catalog.CommitLog, error) {
	if repositoryID == r.repository {
		return nil, errPinSourceUnavailable
	}
	return r.PinCatalog.CreateBranch(ctx, repositoryID, branch, sourceRef, opts...)
}

var errPinSourceUnavailable = errors.New("source repository unavailable")

// TestAcquireRollsBackOnlyWhatItAdded pins the retry safety of Acquire: a partial failure releases the
// references this call added and nothing else, so an interrupted publish that is retried cannot strip
// the pin protection of a version record that already exists.
func TestAcquireRollsBackOnlyWhatItAdded(t *testing.T) {
	f := newPinFixture(t)
	ctx := f.ctx
	good := f.sourceRepo(t, "source-one", "data/a/customers.parquet")
	f.createRepository(t, "source-two", "main", false)
	bad := f.commitEntry(t, "source-two", "main", "geo/countries.csv", "geo/countries.csv", catalog.AddressTypeRelative)
	goodSource := PinnedSource{Repository: "source-one", CommitID: good.String()}
	badSource := PinnedSource{Repository: "source-two", CommitID: bad.String()}
	def := definitionOf(badSource, goodSource) // sorted by target, so source-one is pinned first
	ref := PinReference("customer-360", 2)
	broken := NewPins(f.store, &failingPinCatalog{PinCatalog: f.cat, repository: badSource.Repository})

	// First attempt: the first source is pinned, the second fails, and the first is released again.
	_, err := broken.Acquire(ctx, def, ref)
	require.ErrorIs(t, err, errPinSourceUnavailable)
	_, err = f.store.GetPin(ctx, goodSource.Repository, goodSource.CommitID)
	require.ErrorIs(t, err, kv.ErrNotFound, "the reference added by this call must be rolled back")
	_, ok := f.pinBranchAt(t, goodSource.Repository, goodSource.CommitID)
	require.False(t, ok, "the branch goes with the only reference that was ever taken")

	// Retry: this time the first source's reference already exists (an earlier attempt recorded it),
	// so a later failure must leave it in place.
	_, err = f.pins.Acquire(ctx, definitionOf(goodSource), ref)
	require.NoError(t, err)
	_, err = broken.Acquire(ctx, def, ref)
	require.ErrorIs(t, err, errPinSourceUnavailable)
	require.Equal(t, []string{ref}, f.pinRefs(t, goodSource.Repository, goodSource.CommitID),
		"a reference that was already held must survive the rollback of a failed retry")
	f.requirePinBranch(t, goodSource.Repository, goodSource.CommitID)
}
