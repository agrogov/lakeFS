package datasets

// Spike S4 (DATASETS_DESIGN.md sections 6.3, 7.2 and 18) — reading IdentifierTypeFull addresses
// that live outside the reading repository's namespace. This is the whole basis of zero-copy
// datasets: a dataset entry's PhysicalAddress points into the *source* repository's namespace, and
// the dataset repository's own namespace holds none of that data.
//
//	S4: Do adapters read IdentifierTypeFull addresses outside the repository namespace?
//	    ANSWER: YES. This file verifies it end to end for mem (through a real catalog) and for the local
//	    adapter, including its allowed_external_prefixes restriction; the shared adapter suite verifies it
//	    for every adapter that runs it, including the docker-backed ones (see "Adapter-level conformance"
//	    below). Concretely: an entry whose PhysicalAddress is a full URI into the source repository's
//	    namespace resolves and reads through the very code path the S3 gateway uses
//	    (ObjectPointer{IdentifierType: Full}), while the dataset repository's own namespace stays empty.
//	    The local adapter additionally requires the
//	    source path to be inside its configured root directory, or listed in
//	    blockstore.local.allowed_external_prefixes — with the default configuration a source namespace
//	    outside the adapter root is rejected with block.ErrForbidden. Required configuration when the
//	    source namespaces live outside the local adapter root (lakefs.yaml, or the env var
//	    LAKEFS_BLOCKSTORE_LOCAL_ALLOWED_EXTERNAL_PREFIXES as a comma separated list):
//
//	        blockstore:
//	          type: local
//	          local:
//	            allowed_external_prefixes:
//	              - /mnt/lakefs-datasets     # the adapter root is always allowed
//	              - /mnt/source-namespaces   # only needed for paths outside the root
//	    Caveat for local (see the test): its QualifiedKey.Format() always prefixes the adapter root, so a
//	    full address must be kept verbatim and never round-tripped through ResolveNamespace, exactly as
//	    design section 6.3 specifies (only RELATIVE addresses are converted).
//	    The local adapter is exercised at the adapter level, because that is where its restriction lives;
//	    the catalog-level test uses mem and covers the adapter-independent part (entry round trip + read).
//
//	    Adapter-level conformance (S4 for all backends): testAdapterGetFullAddressInOtherNamespace in
//	    pkg/block/blocktest/basic_suite.go, wired into AdapterBasicObjectTest and therefore into
//	    AdapterTest, runs for all five adapters: mem, local, s3, gs and azure. It writes a source object
//	    with a RELATIVE identifier into namespace A, then reads it from a DIFFERENT namespace B with
//	    IdentifierTypeFull and an address pointing into A — an object this adapter instance never wrote
//	    through that address, and one that never existed in B, which is exactly the shape of a dataset
//	    read (design sections 4.2, 6.3). It also asserts the object is NOT visible through a relative
//	    identifier in B, so the successful read provably goes through the full address instead of a
//	    lucky namespace fallback. B is allocated as a sibling of A (same bucket/container/account, or a
//	    sibling directory under the local adapter root), which keeps the case inside each backend's
//	    addressing rules and inside local's allowed paths — so no adapter needs a skip.
//	    Executed: mem, local, gs (fake-gcs-server) and azure (azurite) all pass. S3 runs the same case
//	    but could not be executed in the environment where this was validated: its dockertest container
//	    needs minio/minio:RELEASE.2023-06-09T07-32-12Z, a pinned tag that is not pullable there (Docker
//	    Hub no longer serves old minio tags), so only the s3 package was compiled, not run. The S3 read
//	    path is resolveFull -> adapter.Get -> SDK call with no namespace restriction, so no restriction
//	    beyond local's is expected — but that stays an expectation until the pinned image is reachable.
//	    The end-to-end datasets-on-cloud-blockstore case (dataset repository + publish + gateway read
//	    against a real cloud storage account) is not covered here: it needs cloud credentials and a
//	    deployed lakeFS, and is tracked by a separate esti card.

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/local"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestSpikeS4_FullAddressReadInMemAdapter pins S4 for the mem adapter, end to end through a real
// catalog: source repo -> full address -> dataset repo metarange -> commit -> GetEntry -> adapter Get.
func TestSpikeS4_FullAddressReadInMemAdapter(t *testing.T) {
	const (
		srcRepo    = "src-unique"
		dsRepo     = "ds-unique"
		srcKey     = "data/customers/c1.parquet"
		targetPath = "customers/c1.parquet"
	)
	content := []byte("parquet-bytes-of-c1")
	f := newSpikeFixture(t)
	f.createRepository(t, srcRepo, "main", false)
	f.createRepository(t, dsRepo, "latest", true) // datasets are read-only repositories

	src := f.repositoryRecord(t, srcRepo)
	ds := f.repositoryRecord(t, dsRepo)

	// The source object lives in the source repository's namespace, nowhere else.
	_, err := f.cat.BlockAdapter.Put(f.ctx, block.ObjectPointer{
		StorageID:        src.StorageID.String(),
		StorageNamespace: src.StorageNamespace.String(),
		Identifier:       srcKey,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(content)), bytes.NewReader(content), block.PutOpts{})
	require.NoError(t, err, "put source object")

	// A committed source entry, as the publish path would find it.
	srcCommit := f.commitEntry(t, srcRepo, "main", srcKey, srcKey, catalog.AddressTypeRelative)
	srcEntry, err := f.cat.GetEntry(f.ctx, srcRepo, srcCommit.String(), srcKey, catalog.GetEntryParams{})
	require.NoError(t, err)
	require.Equal(t, catalog.AddressTypeRelative, srcEntry.AddressType, "source entry address type")

	// Address conversion (design section 6.3): relative -> full URI in the SOURCE namespace.
	qk, err := f.cat.BlockAdapter.ResolveNamespace(src.StorageID.String(), src.StorageNamespace.String(),
		srcEntry.PhysicalAddress, srcEntry.AddressType.ToIdentifierType())
	require.NoError(t, err, "resolve source namespace")
	fullAddress := qk.Format()
	require.Equal(t, "mem://"+srcRepo+"-ns/"+srcKey, fullAddress, "full address must point into the source namespace")
	require.NotContains(t, fullAddress, ds.StorageNamespace.String(), "the full address must be outside the dataset namespace")

	// Write the dataset version the way publish does: metarange by iterator, then commit with it.
	entry := &catalog.Entry{
		Address:      fullAddress,
		AddressType:  catalog.Entry_FULL,
		Size:         int64(len(content)),
		ETag:         "etag-of-c1",
		ContentType:  "application/octet-stream",
		LastModified: timestamppb.New(time.Now()),
	}
	value, err := catalog.EntryToValue(entry)
	require.NoError(t, err)
	it := newSliceValueIterator([]*graveler.ValueRecord{{Key: graveler.Key(targetPath), Value: value}})
	metaRangeID, err := f.cat.Store.WriteMetaRangeByIterator(f.ctx, ds, it, graveler.WithForce(true))
	require.NoError(t, err, "write dataset metarange")
	_, err = f.cat.Commit(f.ctx, dsRepo, "latest", "publish v1", "spike", nil, nil,
		swag.String(metaRangeID.String()), false, graveler.WithForce(true))
	require.NoError(t, err, "commit dataset version")

	// The stored entry keeps AddressTypeFull through the range/SSTable round trip.
	dsEntry, err := f.cat.GetEntry(f.ctx, dsRepo, "latest", targetPath, catalog.GetEntryParams{})
	require.NoError(t, err)
	require.Equal(t, catalog.AddressTypeFull, dsEntry.AddressType, "dataset entry must stay a FULL address after commit")
	require.Equal(t, fullAddress, dsEntry.PhysicalAddress)
	require.Equal(t, entry.ETag, dsEntry.Checksum, "ETag must be preserved so diffs stay meaningful")
	require.Equal(t, entry.Size, dsEntry.Size)

	// Read it exactly like the S3 gateway does (gateway/operations/getobject.go).
	reader, err := f.cat.BlockAdapter.Get(f.ctx, block.ObjectPointer{
		StorageID:        ds.StorageID.String(),
		StorageNamespace: ds.StorageNamespace.String(),
		Identifier:       dsEntry.PhysicalAddress,
		IdentifierType:   block.IdentifierTypeFull,
	})
	require.NoError(t, err, "S4 ANSWER: a full address outside the reading repository's namespace resolves and reads")
	defer reader.Close()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, content, got)

	// Contrast: reading the same entry as a relative address looks inside the (empty) dataset
	// namespace and fails. That is why publish must convert relative -> full.
	_, err = f.cat.BlockAdapter.Get(f.ctx, block.ObjectPointer{
		StorageID:        ds.StorageID.String(),
		StorageNamespace: ds.StorageNamespace.String(),
		Identifier:       targetPath,
		IdentifierType:   block.IdentifierTypeRelative,
	})
	require.ErrorIs(t, err, block.ErrDataNotFound, "a relative address reads from the dataset namespace, which holds no data")
}

// TestSpikeS4_LocalAdapterFullAddress pins S4 for the local adapter, including its
// allowed_external_prefixes restriction (design section 18, S4): the same rule applies to source
// and dataset repositories living under different roots.
func TestSpikeS4_LocalAdapterFullAddress(t *testing.T) {
	const (
		srcKey  = "data/customers/c1.parquet"
		content = "parquet-bytes-of-c1"
	)
	root := filepath.Join(t.TempDir(), "adapter-root")
	outsideRoot := filepath.Join(t.TempDir(), "other-disk")

	adapter, err := local.NewAdapter(root)
	require.NoError(t, err)

	// Source repository namespace: a sibling directory of the dataset namespace, both under the
	// adapter root. That is the normal single-blockstore layout.
	srcNS := "local://src-repo-ns"
	dsNS := "local://ds-repo-ns"
	_, err = adapter.Put(t.Context(), block.ObjectPointer{
		StorageNamespace: srcNS,
		Identifier:       srcKey,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(content)), bytes.NewBufferString(content), block.PutOpts{})
	require.NoError(t, err, "put source object")

	qk, err := adapter.ResolveNamespace("", srcNS, srcKey, block.IdentifierTypeRelative)
	require.NoError(t, err)
	fullAddress := qk.Format()
	require.Equal(t, "local://"+filepath.Join(root, "src-repo-ns", srcKey), fullAddress)

	readFull := func(t *testing.T, a *local.Adapter, address string) (string, error) {
		t.Helper()
		reader, err := a.Get(t.Context(), block.ObjectPointer{
			StorageNamespace: dsNS,
			Identifier:       address,
			IdentifierType:   block.IdentifierTypeFull,
		})
		if err != nil {
			return "", err
		}
		defer reader.Close()
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		return string(data), nil
	}

	got, err := readFull(t, adapter, fullAddress)
	require.NoError(t, err, "S4 ANSWER: local reads a full address outside the reading repository's namespace when it is under the adapter root")
	require.Equal(t, content, got)

	// A source namespace outside the adapter root needs blockstore.local.allowed_external_prefixes.
	outsideFile := filepath.Join(outsideRoot, "src-repo-ns", srcKey)
	require.NoError(t, os.MkdirAll(filepath.Dir(outsideFile), 0o750))
	require.NoError(t, os.WriteFile(outsideFile, []byte(content), 0o600))
	outsideAddress := "local://" + outsideFile

	_, err = readFull(t, adapter, outsideAddress)
	require.ErrorIs(t, err, block.ErrForbidden,
		"local adapter rejects full addresses outside its root unless they are allowed")

	// The restriction also bites at materialisation time: publish converts relative -> full with
	// ResolveNamespace, which validates the path as well.
	_, err = adapter.ResolveNamespace("", dsNS, outsideAddress, block.IdentifierTypeFull)
	require.ErrorIs(t, err, block.ErrForbidden, "ResolveNamespace enforces the same allowed prefixes")

	permissive, err := local.NewAdapter(root, local.WithAllowedExternalPrefixes([]string{outsideRoot}))
	require.NoError(t, err)
	got, err = readFull(t, permissive, outsideAddress)
	require.NoError(t, err, "with allowed_external_prefixes the outside address reads")
	require.Equal(t, content, got)
	_, err = permissive.ResolveNamespace("", dsNS, outsideAddress, block.IdentifierTypeFull)
	require.NoError(t, err, "with allowed_external_prefixes ResolveNamespace accepts the outside address too")

	// Adapter quirk, worth knowing for publish: the local adapter's QualifiedKey.Format() always
	// prefixes its root path, so formatting a *full* address through ResolveNamespace yields a doubled
	// path ("local://<root><root>/..."). DATASETS_DESIGN.md section 6.3 keeps FULL addresses verbatim
	// and only converts RELATIVE ones, so publish must never round-trip a full address like this.
	resolved, err := adapter.ResolveNamespace("", dsNS, fullAddress, block.IdentifierTypeFull)
	require.NoError(t, err)
	require.NotEqual(t, fullAddress, resolved.Format(), "local QualifiedKey.Format must not be used on an already-full address")
}
