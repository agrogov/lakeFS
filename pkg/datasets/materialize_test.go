package datasets

// materialize_test.go covers §6.3 and §6.4 on a real catalog: the per-item iterators,
// the address conversion, and the k-way merge that WriteMetaRangeByIterator requires.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// mergedItems opens the item streams of def the way Publish does and returns the merged
// iterator plus the item iterators, so tests can inspect counters as well.
func mergedItems(t *testing.T, f *datasetFixture, def Definition) (*mergeIterator, []*itemIterator) {
	t.Helper()
	m := &Materializer{Catalog: f.cat}
	items, err := m.openItems(f.ctx, def)
	require.NoError(t, err, "open item streams")
	streams := make([]graveler.ValueIterator, 0, len(items))
	for _, item := range items {
		streams = append(streams, item)
	}
	merged := newMergeIterator(streams)
	t.Cleanup(merged.Close)
	return merged, items
}

// drainKeys consumes a value iterator and returns its keys plus its final error.
func drainKeys(t *testing.T, it graveler.ValueIterator) ([]string, error) {
	t.Helper()
	keys := make([]string, 0)
	for it.Next() {
		record := it.Value()
		require.NotNil(t, record, "iterator returned true with a nil record")
		keys = append(keys, string(record.Key))
	}
	return keys, it.Err()
}

// TestMergeIterator_SortedAcrossTargets is the §6.4 regression: targets "a" (prefix,
// producing a/x) and "a-b" (object) concatenate as a/x, a-b — but '-' (0x2d) sorts
// before '/' (0x2f), so "a-b" must come first. Conciatenating per-item streams is
// exactly what the merge exists to avoid, and an unsorted iterator fails the metarange
// write, so this test guards the publish path.
func TestMergeIterator_SortedAcrossTargets(t *testing.T) {
	f := newDatasetFixture(t)
	f.createRepository(t, "src-merge", "main", false)
	commit := f.commitObjects(t, "src-merge", "main",
		testObject{key: "data/a/x.parquet", content: []byte("x")},
		testObject{key: "data/a/y.parquet", content: []byte("y")},
		testObject{key: "data/ab.parquet", content: []byte("ab")},
	)

	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{
		{
			Target: "a", Repository: "src-merge",
			Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/a/"},
		},
		{
			Target: "a-b", Repository: "src-merge",
			Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypeObject, Object: &PathSource{Path: "data/ab.parquet"},
		},
	}}
	require.NoError(t, Validate(&def, ValidateOptions{}), "targets a and a-b do not overlap")

	merged, _ := mergedItems(t, f, def)
	keys, err := drainKeys(t, merged)
	require.NoError(t, err)
	require.Equal(t, []string{"a-b", "a/x.parquet", "a/y.parquet"}, keys,
		"the merge must emit keys in byte order, not in target order")
}

// TestMergeIterator_SeekGE pins the SeekGE contract the metarange writer's callers rely
// on: after a seek, Next returns the first key >= the target, across all streams.
func TestMergeIterator_SeekGE(t *testing.T) {
	f := newDatasetFixture(t)
	f.createRepository(t, "src-seek", "main", false)
	commit := f.commitObjects(t, "src-seek", "main",
		testObject{key: "data/a/x.parquet", content: []byte("x")},
		testObject{key: "data/a/y.parquet", content: []byte("y")},
		testObject{key: "data/ab.parquet", content: []byte("ab")},
	)

	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{
		{
			Target: "a", Repository: "src-seek",
			Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/a/"},
		},
		{
			Target: "a-b", Repository: "src-seek",
			Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypeObject, Object: &PathSource{Path: "data/ab.parquet"},
		},
	}}

	for _, tc := range []struct {
		name   string
		seek   string
		expect []string
	}{
		{name: "before everything", seek: "0", expect: []string{"a-b", "a/x.parquet", "a/y.parquet"}},
		{name: "between object and prefix keys", seek: "a-x", expect: []string{"a/x.parquet", "a/y.parquet"}},
		{name: "exactly at a key", seek: "a/x.parquet", expect: []string{"a/x.parquet", "a/y.parquet"}},
		{name: "at the object key", seek: "a-b", expect: []string{"a-b", "a/x.parquet", "a/y.parquet"}},
		{name: "after everything", seek: "zzz", expect: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged, _ := mergedItems(t, f, def)
			merged.SeekGE(graveler.Key(tc.seek))
			keys, err := drainKeys(t, merged)
			require.NoError(t, err)
			require.Equal(t, tc.expect, keys)
		})
	}
}

// TestMergeIterator_RejectsDuplicateKeys pins the strict key-increase assertion of §6.4:
// overlapping targets (which validation rejects, but a corrupted or hand-built
// definition can still produce) fail the publish instead of silently dropping an entry.
func TestMergeIterator_RejectsDuplicateKeys(t *testing.T) {
	f := newDatasetFixture(t)
	f.createRepository(t, "src-dup", "main", false)
	commit := f.commitObjects(t, "src-dup", "main",
		testObject{key: "one/k.parquet", content: []byte("one")},
		testObject{key: "two/k.parquet", content: []byte("two")},
	)

	// Both items target "same", so both produce "same/k.parquet".
	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{
		{
			Target: "same", Repository: "src-dup",
			Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "one/"},
		},
		{
			Target: "same", Repository: "src-dup",
			Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "two/"},
		},
	}}
	require.Error(t, Validate(&def, ValidateOptions{}), "validation rejects overlapping targets")

	merged, _ := mergedItems(t, f, def)
	_, err := drainKeys(t, merged)
	require.ErrorIs(t, err, ErrDuplicateKey, "the merge must fail on a duplicate key")
}

// TestMergeIterator_ManyItems pins that merging many object items produces exactly the
// union of their keys, in order (§6.4: the heap also has to be right for more than two
// streams).
func TestMergeIterator_ManyItems(t *testing.T) {
	const items = 12
	f := newDatasetFixture(t)
	f.createRepository(t, "src-many-items", "main", false)

	objects := make([]testObject, 0, items)
	for i := range items {
		objects = append(objects, testObject{key: "src/" + string(rune('a'+i)) + ".parquet", content: []byte{byte(i)}})
	}
	commit := f.commitObjects(t, "src-many-items", "main", objects...)

	def := Definition{FormatVersion: FormatVersion}
	for i := range items {
		key := "src/" + string(rune('a'+i)) + ".parquet"
		def.Data = append(def.Data, DataItem{
			Target: "target-" + string(rune('a'+i)), Repository: "src-many-items",
			Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypeObject, Object: &PathSource{Path: key},
		})
	}
	require.NoError(t, Validate(&def, ValidateOptions{}))

	merged, itemIterators := mergedItems(t, f, def)
	keys, err := drainKeys(t, merged)
	require.NoError(t, err)
	require.Equal(t, []string{
		"target-a", "target-b", "target-c", "target-d", "target-e", "target-f",
		"target-g", "target-h", "target-i", "target-j", "target-k", "target-l",
	}, keys)

	var counted int64
	for _, item := range itemIterators {
		counted += item.objects
	}
	require.Equal(t, int64(items), counted, "every item contributes exactly one object")
}

// TestItemIterator_ConvertsAddresses is the §6.3 unit test. Relative source entries are
// resolved against the *source* repository namespace and become full identifiers; a
// full address is passed through untouched (resolving it again would double the
// namespace on adapters whose QualifiedKey.Format prefixes their root — Spike S4). Every
// other field is preserved so that diffs between versions stay meaningful.
func TestItemIterator_ConvertsAddresses(t *testing.T) {
	f := newDatasetFixture(t)
	f.createRepository(t, "src-addr", "main", false)

	relativeContent := []byte("relative-source-bytes")
	f.putObject(t, "src-addr", "rel/data.parquet", relativeContent)
	fullContent := []byte("full-source-bytes")
	fullCommit := f.commitFullAddressEntry(t, "src-addr", "main", "abs/data.parquet", fullContent)

	// Stage the relative entry on a second commit so both live in the same commit we
	// materialise from.
	f.putObject(t, "src-addr", "rel/data.parquet", relativeContent)
	require.NoError(t, f.cat.CreateEntry(f.ctx, "src-addr", "main", catalog.DBEntry{
		Path:            "rel/data.parquet",
		PhysicalAddress: "rel/data.parquet",
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    testTime(),
		Size:            int64(len(relativeContent)),
		Checksum:        etagOf(relativeContent),
		ContentType:     "application/x-parquet",
		Metadata:        catalog.Metadata{"owner": "data-platform"},
	}))
	log, err := f.cat.Commit(f.ctx, "src-addr", "main", "add relative entry", "tester", nil, nil, nil, false)
	require.NoError(t, err)
	commit := graveler.CommitID(log.Reference)

	fullEntryBefore := f.datasetEntry(t, "src-addr", fullCommit.String(), "abs/data.parquet")

	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{
		{
			Target: "relative", Repository: "src-addr", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypeObject, Object: &PathSource{Path: "rel/data.parquet"},
		},
		{
			Target: "kept-full", Repository: "src-addr", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypeObject, Object: &PathSource{Path: "abs/data.parquet"},
		},
	}}
	require.NoError(t, Validate(&def, ValidateOptions{}))

	merged, _ := mergedItems(t, f, def)
	entries := make(map[string]*catalog.Entry)
	for merged.Next() {
		entry, err := catalog.ValueToEntry(merged.Value().Value)
		require.NoError(t, err)
		entries[string(merged.Value().Key)] = entry
	}
	require.NoError(t, merged.Err())
	require.Len(t, entries, 2)

	relative := entries["relative"]
	require.Equal(t, catalog.Entry_FULL, relative.AddressType, "a relative address becomes full")
	require.Equal(t, "mem://src-addr-ns/rel/data.parquet", relative.Address,
		"the full address points into the SOURCE namespace")
	require.Equal(t, int64(len(relativeContent)), relative.Size)
	require.Equal(t, etagOf(relativeContent), relative.ETag, "the ETag is preserved")
	require.Equal(t, "application/x-parquet", relative.ContentType)
	require.Equal(t, map[string]string{"owner": "data-platform"}, relative.Metadata)
	require.Equal(t, testTime().Unix(), relative.LastModified.AsTime().Unix())

	full := entries["kept-full"]
	require.Equal(t, catalog.Entry_FULL, full.AddressType)
	require.Equal(t, fullEntryBefore.PhysicalAddress, full.Address, "a full address is passed through verbatim")
	require.Equal(t, etagOf(fullContent), full.ETag)
}

// TestMaterialize_RejectsSourceKeyOnReservedDefinitionPath is the §6.4 regression for m7 of
// CODE_REVIEW_DATASETS.md. A prefix item with the root target maps every source key into the
// dataset key space unchanged, so a source repository that happens to hold the reserved
// _dataset/definition.json key produces the same dataset key as the synthetic definition
// entry of §6.4. The merge would fail such a publish with ErrDuplicateKey ("duplicate or
// out-of-order key in materialised dataset"), which reads like an internal bug and says
// nothing about the source key: the item must instead be rejected as a *ValidationError that
// names the item and the reserved path, before the definition object or any metarange entry
// is written.
func TestMaterialize_RejectsSourceKeyOnReservedDefinitionPath(t *testing.T) {
	const source = "reserved-key-source"
	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)

	reserved := []byte(`{"dataset":"not-a-definition"}`)
	// The first commit holds the reserved key at the repository root next to ordinary data; the
	// second adds the same key below a source prefix. Commits are cumulative, so a whole-repo
	// item over either commit sees a reserved root key.
	rootCommit := f.commitObjects(t, source, "main",
		testObject{key: DefinitionPath, content: reserved},
		testObject{key: "customers/c1.parquet", content: []byte("c1")},
	)
	subCommit := f.commitObjects(t, source, "main",
		testObject{key: "sub/" + DefinitionPath, content: reserved},
		testObject{key: "sub/customers/c2.parquet", content: []byte("c2")},
	)

	prefixItem := func(commit graveler.CommitID, prefix string) DataItem {
		return DataItem{
			Repository: source,
			Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type:       ItemTypePrefix, Prefix: &PathSource{Path: prefix},
		}
	}

	failing := []struct {
		name    string
		dataset string
		item    DataItem
		// wantSourceKey is the source key the message must name.
		wantSourceKey string
	}{
		{
			// The empty prefix is the whole repository (§5.2), so the reserved key maps onto
			// itself.
			name:          "whole source repository at the root target",
			dataset:       "reserved-root",
			item:          prefixItem(rootCommit, ""),
			wantSourceKey: DefinitionPath,
		},
		{
			// A non-empty prefix still maps into the root target unchanged, so the colliding
			// source key is the prefix plus the reserved path.
			name:          "source prefix that holds the reserved key",
			dataset:       "reserved-prefix",
			item:          prefixItem(subCommit, "sub/"),
			wantSourceKey: "sub/" + DefinitionPath,
		},
	}
	for _, tc := range failing {
		t.Run(tc.name, func(t *testing.T) {
			f.createRepository(t, tc.dataset, DefaultBranch, true)
			// The repository's creation commit, which a rejected publish must leave alone.
			headBefore, err := f.cat.GetBranchReference(f.ctx, tc.dataset, DefaultBranch)
			require.NoError(t, err)
			def := resolvedDefinition(t, f, Definition{
				FormatVersion: FormatVersion,
				Data:          []DataItem{tc.item},
			})

			_, err = (&Materializer{Catalog: f.cat}).Publish(f.ctx, PublishRequest{
				Dataset:    tc.dataset,
				Version:    1,
				Definition: def,
				Message:    "must be rejected",
				Publisher:  "tester",
			})
			var validation *ValidationError
			require.ErrorAs(t, err, &validation, "a reserved source key is a definition problem, not a merge failure")
			require.NotErrorIs(t, err, ErrDuplicateKey,
				"the collision must not surface as the merge's duplicate-key error")
			require.Len(t, validation.Problems, 1)
			problem := validation.Problems[0]
			require.Equal(t, itemPath(0, ""), problem.Path, "the problem names the offending item")
			require.Contains(t, problem.Message, DefinitionPath, "the problem names the reserved path")
			require.Contains(t, problem.Message, tc.wantSourceKey, "the problem names the offending source key")

			// Nothing was written: no version tag, no commit on the dataset branch and no
			// definition object in the dataset namespace (§6.2 step 6).
			_, err = f.cat.GetTag(f.ctx, tc.dataset, VersionTag(1))
			require.Error(t, err, "a rejected publish creates no version tag")
			headAfter, err := f.cat.GetBranchReference(f.ctx, tc.dataset, DefaultBranch)
			require.NoError(t, err)
			require.Equal(t, headBefore, headAfter, "a rejected publish commits nothing on the dataset branch")
			dsRec := f.repositoryRecord(t, tc.dataset)
			_, err = f.cat.BlockAdapter.Get(f.ctx, block.ObjectPointer{
				StorageID:        dsRec.StorageID.String(),
				StorageNamespace: dsRec.StorageNamespace.String(),
				Identifier:       definitionObjectKey("", 1),
				IdentifierType:   block.IdentifierTypeRelative,
			})
			require.ErrorIs(t, err, mem.ErrNoDataForKey,
				"the rejected publish must not upload the definition object")
		})
	}

	// The check must not over-reject: a root-target prefix item over a source that holds no
	// reserved key publishes fine, and a reserved-looking key below the item's source prefix
	// is just data.
	t.Run("source without the reserved key", func(t *testing.T) {
		const dataset = "reserved-clean"
		const cleanSource = "reserved-key-clean-source"
		f.createRepository(t, cleanSource, "main", false)
		f.createRepository(t, dataset, DefaultBranch, true)
		cleanRepoCommit := f.commitObjects(t, cleanSource, "main",
			testObject{key: "other/c3.parquet", content: []byte("c3")},
		)
		def := resolvedDefinition(t, f, Definition{
			FormatVersion: FormatVersion,
			Data: []DataItem{{
				Repository: cleanSource,
				Ref:        SourceRef{Type: RefTypeCommit, ID: cleanRepoCommit.String()},
				Type:       ItemTypePrefix, Prefix: &PathSource{Path: ""},
			}},
		})
		publish(t, f, PublishRequest{
			Dataset:    dataset,
			Version:    1,
			Definition: def,
			Message:    "no reserved key in the source",
			Publisher:  "tester",
		})
		require.Equal(t, []string{DefinitionPath, "other/c3.parquet"}, f.listPaths(t, dataset, DefaultBranch))
	})

	t.Run("reserved path below a source prefix is just data", func(t *testing.T) {
		const dataset = "reserved-subtree"
		f.createRepository(t, dataset, DefaultBranch, true)
		item := prefixItem(subCommit, "sub/")
		item.Target = "data"
		def := resolvedDefinition(t, f, Definition{
			FormatVersion: FormatVersion,
			Data:          []DataItem{item},
		})
		publish(t, f, PublishRequest{
			Dataset:    dataset,
			Version:    1,
			Definition: def,
			Message:    "the source prefix is not the repository root",
			Publisher:  "tester",
		})
		require.Equal(t, []string{
			DefinitionPath,
			"data/" + DefinitionPath,
			"data/customers/c2.parquet",
		}, f.listPaths(t, dataset, DefaultBranch))
	})
}

// TestOpenItems_RejectsObjectItemOnReservedDefinitionPath pins the primitive's own guard: an
// object item's only output key is its target, so a target on the reserved path would replace
// the synthetic definition entry of §6.4. Validation rejects that target first (§5.2), so this
// test drives openItems directly, the way a caller that skipped validation would.
func TestOpenItems_RejectsObjectItemOnReservedDefinitionPath(t *testing.T) {
	const source = "reserved-object-source"
	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)
	commit := f.commitObjects(t, source, "main",
		testObject{key: "customers/c1.parquet", content: []byte("c1")},
	)

	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{{
		Target:     DefinitionPath,
		Repository: source,
		Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
		Type:       ItemTypeObject, Object: &PathSource{Path: "customers/c1.parquet"},
	}}}
	require.Error(t, Validate(&def, ValidateOptions{}), "validation rejects the reserved target first")

	items, err := (&Materializer{Catalog: f.cat}).openItems(f.ctx, def)
	require.Nil(t, items)
	var validation *ValidationError
	require.ErrorAs(t, err, &validation)
	require.Len(t, validation.Problems, 1)
	require.Equal(t, itemPath(0, ""), validation.Problems[0].Path, "the problem names the offending item")
	require.Contains(t, validation.Problems[0].Message, DefinitionPath, "the problem names the reserved path")
}

// TestConvertEntryAddress is the §6.3 address table, including the deprecated by-prefix
// form old repositories still hold and the full-address passthrough that the local
// adapter's QualifiedKey.Format forces (Spike S4).
func TestConvertEntryAddress(t *testing.T) {
	repo := &graveler.RepositoryRecord{
		RepositoryID: "src-addr-table",
		Repository: &graveler.Repository{
			StorageNamespace: "mem://src-addr-table-ns",
		},
	}
	adapter := mem.New(t.Context())

	for _, tc := range []struct {
		name        string
		addressType catalog.Entry_AddressType
		address     string
		wantAddress string
		wantType    catalog.Entry_AddressType
	}{
		{
			name:        "relative becomes full in the source namespace",
			addressType: catalog.Entry_RELATIVE,
			address:     "objects/001/data.parquet",
			wantAddress: "mem://src-addr-table-ns/objects/001/data.parquet",
			wantType:    catalog.Entry_FULL,
		},
		{
			name:        "deprecated by-prefix address is resolved as well",
			addressType: catalog.Entry_BY_PREFIX_DEPRECATED,
			address:     "objects/002/data.parquet",
			wantAddress: "mem://src-addr-table-ns/objects/002/data.parquet",
			wantType:    catalog.Entry_FULL,
		},
		{
			name:        "full address is left alone",
			addressType: catalog.Entry_FULL,
			address:     "mem://other-namespace/objects/003/data.parquet",
			wantAddress: "mem://other-namespace/objects/003/data.parquet",
			wantType:    catalog.Entry_FULL,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := &catalog.Entry{Address: tc.address, AddressType: tc.addressType, Size: 7, ETag: "etag"}
			require.NoError(t, convertEntryAddress(adapter, repo, entry))
			require.Equal(t, tc.wantAddress, entry.Address)
			require.Equal(t, tc.wantType, entry.AddressType)
			require.Equal(t, int64(7), entry.Size, "conversion must not touch the other entry fields")
			require.Equal(t, "etag", entry.ETag, "conversion must not touch the other entry fields")
		})
	}
}
