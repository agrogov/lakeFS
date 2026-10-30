// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// pins_gc_test.go is the GC regression test of design §8.2/§8.3 and the centrepiece of the pins
// card: a dataset version is published from a commit that is reachable only from a source branch,
// the source branch is deleted, the source repository's GC commits are prepared, and the dataset
// must still be readable - because the pin branch made the pinned commit a GC starting point.
//
// The contrast case is the regression the design calls out in §8.3: had the dataset used a *tag* in
// the source repository instead of a pin branch, the commit would not be a GC starting point and
// its data would be collectable. Tags are not starting points (retention.GCStartingPointIterator
// iterates branch heads and dangling commits only), so the same GC run that retains the pinned
// commit drops the tagged one.
//
// "PrepareGCCommits" (§15.1) is catalog.PrepareExpiredCommits in this fork: it runs
// retention.GetGarbageCollectionCommits and writes the active-commit set as a CSV in the repository
// namespace. The test parses that CSV - the artifact the GC job consumes - instead of re-implementing
// the reachability logic, so it asserts exactly what garbage collection will retain.
package datasets

import (
	"bytes"
	"encoding/csv"
	"io"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// commitEntryAt stages one object and commits it with an explicit commit date, so that a GC
// retention window measured from the commit's creation date is already expired for it.
//
// The GC preparation only treats a dangling commit as active while it is inside the retention
// window; an unpinned dangling commit older than the window is expired. Commits are therefore
// created two days in the past, which keeps the test independent of the wall clock.
func commitEntryAt(t *testing.T, f *pinFixture, repo, branch, key string, created time.Time) graveler.CommitID {
	t.Helper()
	err := f.cat.CreateEntry(f.ctx, repo, branch, catalog.DBEntry{
		Path:            key,
		PhysicalAddress: key,
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    created,
		Size:            42,
		Checksum:        "deadbeef",
	})
	require.NoError(t, err, "create entry %s", key)
	unix := created.Unix()
	log, err := f.cat.Commit(f.ctx, repo, branch, "commit "+key, "spike", nil, &unix, nil, false)
	require.NoError(t, err, "commit %s", key)
	return graveler.CommitID(log.Reference)
}

// activeCommits runs the source repository's GC commit preparation and returns the commit IDs the
// GC job will treat as active (it retains their metaranges).
func activeCommits(t *testing.T, f *pinFixture, repo string) map[string]struct{} {
	t.Helper()
	rec := f.repositoryRecord(t, repo)
	meta, err := f.cat.PrepareExpiredCommits(f.ctx, repo)
	require.NoError(t, err, "prepare GC commits for %s", repo)

	reader, err := f.cat.BlockAdapter.Get(f.ctx, block.ObjectPointer{
		StorageID:        rec.StorageID.String(),
		StorageNamespace: rec.StorageNamespace.String(),
		Identifier:       meta.CommitsCSVLocation,
		IdentifierType:   block.IdentifierTypeFull,
	})
	require.NoError(t, err, "read the active-commits CSV")
	defer reader.Close()

	records, err := csv.NewReader(reader).ReadAll()
	require.NoError(t, err, "parse the active-commits CSV")
	require.NotEmpty(t, records)
	require.Equal(t, []string{"commit_id", "expired", "metarange_id"}, records[0],
		"the CSV header is the GC job's contract")

	active := make(map[string]struct{}, len(records)-1)
	for _, row := range records[1:] {
		require.Len(t, row, 3)
		require.Equal(t, "false", row[1], "the preparation step lists active commits")
		active[row[0]] = struct{}{}
	}
	return active
}

// materialiseDatasetVersion writes the dataset repository version that publish (§6.2) writes: one
// entry per source object, pointing at the source's full physical address (zero-copy, §6.3), then a
// commit and the version tag.
func materialiseDatasetVersion(t *testing.T, f *pinFixture, dsRepo, tag, target, fullAddress string, content []byte) graveler.CommitID {
	t.Helper()
	ds := f.repositoryRecord(t, dsRepo)
	entry := &catalog.Entry{
		Address:      fullAddress,
		AddressType:  catalog.Entry_FULL,
		Size:         int64(len(content)),
		ETag:         "etag-of-source",
		ContentType:  "application/octet-stream",
		LastModified: timestamppb.New(time.Now()),
	}
	value, err := catalog.EntryToValue(entry)
	require.NoError(t, err)
	it := newSliceValueIterator([]*graveler.ValueRecord{{Key: graveler.Key(target), Value: value}})
	metaRangeID, err := f.cat.Store.WriteMetaRangeByIterator(f.ctx, ds, it, graveler.WithForce(true))
	require.NoError(t, err, "write the dataset metarange")

	commit, err := f.cat.Commit(f.ctx, dsRepo, "latest", "publish "+tag, "publisher", nil, nil,
		swag.String(metaRangeID.String()), false, graveler.WithForce(true))
	require.NoError(t, err, "commit the dataset version")
	_, err = f.cat.CreateTag(f.ctx, dsRepo, tag, commit.Reference, graveler.WithForce(true))
	require.NoError(t, err, "tag the dataset version")
	return graveler.CommitID(commit.Reference)
}

// readDatasetObject reads one object of a dataset version the way the S3 gateway does (§6.3, spike
// S4): look the entry up in the dataset repository, then read its full physical address through the
// block adapter.
func readDatasetObject(t *testing.T, f *pinFixture, dsRepo, ref, path string) []byte {
	t.Helper()
	ds := f.repositoryRecord(t, dsRepo)
	entry, err := f.cat.GetEntry(f.ctx, dsRepo, ref, path, catalog.GetEntryParams{})
	require.NoError(t, err, "get dataset entry %s@%s/%s", dsRepo, ref, path)
	require.Equal(t, catalog.AddressTypeFull, entry.AddressType)

	reader, err := f.cat.BlockAdapter.Get(f.ctx, block.ObjectPointer{
		StorageID:        ds.StorageID.String(),
		StorageNamespace: ds.StorageNamespace.String(),
		Identifier:       entry.PhysicalAddress,
		IdentifierType:   block.IdentifierTypeFull,
	})
	require.NoError(t, err, "read the dataset object")
	defer reader.Close()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	return content
}

// TestGCPinnedCommitSurvivesSourceBranchDeletion is the GC regression test of §8.2/§8.3.
//
// Sequence:
//  1. source repository: main has one commit; the data the dataset wants is committed on a feature
//     branch and the feature branch is then deleted, so the pinned commit is reachable from nothing;
//  2. dataset v1 is published from that commit (pin acquired first, §6.2 step 5) and the dataset
//     entry points at the source object's full physical address;
//  3. GC commits are prepared on the source repository with an expired retention window;
//  4. the pinned commit is in the active set, and the dataset reads back the source bytes;
//  5. contrast: the same commit referenced by a *tag* instead of a pin branch is not in the active
//     set - the design's §8.3 regression, i.e. why tags cannot be used for this.
func TestGCPinnedCommitSurvivesSourceBranchDeletion(t *testing.T) {
	const (
		srcRepo    = "lakehouse-repo"
		dsRepo     = "customer-360"
		srcKey     = "data/customers/c1.parquet"
		target     = "customers/c1.parquet"
		datasetRef = "customer-360@v1"
	)
	content := []byte("parquet-bytes-of-c1")
	// GC prepares its commit graph in a local KV store at ./gc.db. Run the test in a temporary
	// working directory so the artifact (a multi-gigabyte sparse file) does not land in the repo.
	t.Chdir(t.TempDir())
	f := newPinFixture(t)
	ctx := f.ctx

	// --- 1. Source repository: the pinned commit is only reachable from a branch we delete. ------
	f.createRepository(t, srcRepo, "main", false)
	f.createRepository(t, dsRepo, "latest", true) // datasets are read-only repositories (§6.1)
	src := f.repositoryRecord(t, srcRepo)

	_, err := f.cat.BlockAdapter.Put(ctx, block.ObjectPointer{
		StorageID:        src.StorageID.String(),
		StorageNamespace: src.StorageNamespace.String(),
		Identifier:       srcKey,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(content)), bytes.NewReader(content), block.PutOpts{})
	require.NoError(t, err, "put the source object")

	old := time.Now().Add(-48 * time.Hour)
	base := commitEntryAt(t, f, srcRepo, "main", "data/other/keep.parquet", old)
	_, err = f.cat.CreateBranch(ctx, srcRepo, "feature", base.String())
	require.NoError(t, err)
	pinned := commitEntryAt(t, f, srcRepo, "feature", srcKey, old)

	// The full physical address of the source object, exactly as materialisation computes it (§6.3).
	srcEntry, err := f.cat.GetEntry(ctx, srcRepo, pinned.String(), srcKey, catalog.GetEntryParams{})
	require.NoError(t, err)
	require.Equal(t, catalog.AddressTypeRelative, srcEntry.AddressType)
	qk, err := f.cat.BlockAdapter.ResolveNamespace(src.StorageID.String(), src.StorageNamespace.String(),
		srcEntry.PhysicalAddress, srcEntry.AddressType.ToIdentifierType())
	require.NoError(t, err)
	fullAddress := qk.Format()
	ds := f.repositoryRecord(t, dsRepo)
	require.NotContains(t, fullAddress, ds.StorageNamespace.String(),
		"the dataset must reference the source namespace, not its own")

	// --- 2. Publish: pins first (§6.2 step 5), then the dataset version. -------------------------
	pinnedSource := PinnedSource{Repository: srcRepo, CommitID: pinned.String()}
	definition := definitionOf(pinnedSource)
	acquired, err := f.pins.Acquire(ctx, definition, datasetRef)
	require.NoError(t, err)
	require.Equal(t, []PinnedSource{pinnedSource}, acquired)
	f.requirePinBranch(t, srcRepo, pinned.String())
	require.Equal(t, []string{datasetRef}, f.pinRefs(t, srcRepo, pinned.String()))

	// §6.2 steps 7-10: metarange, commit, tag.
	materialiseDatasetVersion(t, f, dsRepo, "v1", target, fullAddress, content)

	// The source branch now goes away: the pinned commit is nobody's HEAD any more.
	require.NoError(t, f.cat.DeleteBranch(ctx, srcRepo, "feature"))

	// --- 3. GC with an expired retention window. -------------------------------------------------
	require.NoError(t, f.cat.SetGarbageCollectionRules(ctx, srcRepo, &graveler.GarbageCollectionRules{
		// The commits are two days old, so a one-day window is already expired for them: only GC
		// starting points survive. (All-zero rules would marshal to zero bytes and read back as
		// "no rules at all".)
		DefaultRetentionDays: 1,
		BranchRetentionDays:  map[string]int32{},
	}))
	active := activeCommits(t, f, srcRepo)

	// The main branch head is retained, which shows the preparation really ran.
	_, baseActive := active[base.String()]
	require.True(t, baseActive, "the source repository's default branch head must be active")

	// --- 4. The pin did its job. -----------------------------------------------------------------
	_, pinnedActive := active[pinned.String()]
	require.True(t, pinnedActive,
		"§8.2: the pinned commit must be in the GC active set after its source branch was deleted")

	require.Equal(t, content, readDatasetObject(t, f, dsRepo, "v1", target),
		"the dataset must read the source bytes through the pinned commit's full address")

	// --- 5. Contrast: a tag is not a GC starting point (§8.3). -----------------------------------
	// Remove the pin - the state a tag-only implementation is in - and reference the same commit by
	// a tag instead.
	require.NoError(t, f.pins.Release(ctx, definition, datasetRef))
	_, ok := f.pinBranchAt(t, srcRepo, pinned.String())
	require.False(t, ok, "the pin branch must be gone at refcount zero")
	_, err = f.cat.CreateTag(ctx, srcRepo, "pinned-tag", pinned.String())
	require.NoError(t, err, "a tag on the pinned commit is allowed; it just does not protect the data")
	tagged, err := f.cat.GetTag(ctx, srcRepo, "pinned-tag")
	require.NoError(t, err, "the tag really points at the pinned commit")
	require.Equal(t, pinned.String(), tagged)

	taggedActive := activeCommits(t, f, srcRepo)
	require.NotContains(t, taggedActive, pinned.String(),
		"§8.3 REGRESSION: a tag in the source repository is not a GC starting point, so the commit is expired")
	_, baseStillActive := taggedActive[base.String()]
	require.True(t, baseStillActive, "the default branch head stays active, so the run is otherwise unchanged")

	// Reading still works here because this test never deletes objects - deleting the expired
	// commits' objects is the GC job's next step. The point of the assertion above is that the
	// commit is no longer in the retain set, so those objects would be collected.
	require.Equal(t, content, readDatasetObject(t, f, dsRepo, "v1", target))

	// Pins really are the difference: re-acquiring brings the commit back into the active set.
	_, err = f.pins.Acquire(ctx, definition, datasetRef)
	require.NoError(t, err)
	require.Contains(t, activeCommits(t, f, srcRepo), pinned.String(),
		"re-pinning must put the commit back in the GC active set")
}

// TestGCPinSharedPinKeepsBothDatasetsReadable covers the shared-pin half of §8.2: two datasets
// pinned to the same commit, one dataset deleted, and the other still retained and readable.
func TestGCPinSharedPinKeepsBothDatasetsReadable(t *testing.T) {
	const (
		srcRepo = "lakehouse-repo"
		srcKey  = "data/customers/c1.parquet"
	)
	content := []byte("shared-bytes")
	t.Chdir(t.TempDir()) // GC's local commit-graph store is created in the working directory
	f := newPinFixture(t)
	ctx := f.ctx

	f.createRepository(t, srcRepo, "main", false)
	f.createRepository(t, "customer-360", "latest", true)
	f.createRepository(t, "events", "latest", true)
	src := f.repositoryRecord(t, srcRepo)

	_, err := f.cat.BlockAdapter.Put(ctx, block.ObjectPointer{
		StorageID:        src.StorageID.String(),
		StorageNamespace: src.StorageNamespace.String(),
		Identifier:       srcKey,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(content)), bytes.NewReader(content), block.PutOpts{})
	require.NoError(t, err)

	old := time.Now().Add(-48 * time.Hour)
	base := commitEntryAt(t, f, srcRepo, "main", "data/other/keep.parquet", old)
	_, err = f.cat.CreateBranch(ctx, srcRepo, "feature", base.String())
	require.NoError(t, err)
	pinned := commitEntryAt(t, f, srcRepo, "feature", srcKey, old)
	srcEntry, err := f.cat.GetEntry(ctx, srcRepo, pinned.String(), srcKey, catalog.GetEntryParams{})
	require.NoError(t, err)
	qk, err := f.cat.BlockAdapter.ResolveNamespace(src.StorageID.String(), src.StorageNamespace.String(),
		srcEntry.PhysicalAddress, srcEntry.AddressType.ToIdentifierType())
	require.NoError(t, err)

	source := PinnedSource{Repository: srcRepo, CommitID: pinned.String()}
	definition := definitionOf(source)
	for _, ref := range []string{PinReference("customer-360", 1), PinReference("events", 1)} {
		_, err := f.pins.Acquire(ctx, definition, ref)
		require.NoError(t, err)
	}
	require.Equal(t, []string{"customer-360@v1", "events@v1"}, f.pinRefs(t, srcRepo, pinned.String()))

	materialiseDatasetVersion(t, f, "customer-360", "v1", "customers/c1.parquet", qk.Format(), content)
	materialiseDatasetVersion(t, f, "events", "v1", "customers/c1.parquet", qk.Format(), content)
	require.NoError(t, f.cat.DeleteBranch(ctx, srcRepo, "feature"))
	require.NoError(t, f.cat.SetGarbageCollectionRules(ctx, srcRepo, &graveler.GarbageCollectionRules{
		DefaultRetentionDays: 1, // the commits are two days old: the window is expired
		BranchRetentionDays:  map[string]int32{},
	}))

	// Deleting one dataset releases only its reference: the pin branch stays for the other.
	require.NoError(t, f.pins.ReleaseDataset(ctx, "customer-360"))
	require.Equal(t, []string{"events@v1"}, f.pinRefs(t, srcRepo, pinned.String()))
	f.requirePinBranch(t, srcRepo, pinned.String())

	require.Contains(t, activeCommits(t, f, srcRepo), pinned.String())
	require.Equal(t, content, readDatasetObject(t, f, "events", "v1", "customers/c1.parquet"))

	// Deleting the second dataset releases the last reference and removes the branch.
	require.NoError(t, f.pins.ReleaseDataset(ctx, "events"))
	_, err = f.store.GetPin(ctx, srcRepo, pinned.String())
	require.ErrorIs(t, err, kv.ErrNotFound)
	_, ok := f.pinBranchAt(t, srcRepo, pinned.String())
	require.False(t, ok)
	require.NotContains(t, activeCommits(t, f, srcRepo), pinned.String(),
		"with no dataset referencing the commit, GC no longer retains it")
}
