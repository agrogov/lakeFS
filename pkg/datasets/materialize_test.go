package datasets

// materialize_test.go covers §6.3 and §6.4 on a real catalog: the per-item iterators,
// the address conversion, and the k-way merge that WriteMetaRangeByIterator requires.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
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

// Iceberg table and namespace items (ICEBERG_CATALOG_PLAN.md §6.1, §6.3, §9).
//
// The fixture below writes the entries the Iceberg catalog's own write path stages on a branch
// (pkg/icebergcatalog store.go and files.go): namespace markers, one table pointer per table, and
// one entry per staged file. Materialisation copies exactly those entries, so the assertions are
// about placement, metadata and addresses - and about the fact that the objects themselves are
// never copied (goal G3).

// withIcebergEnabled pins the §9 gate for the duration of a test, the way a process configured with
// iceberg.enabled: true would see it.
func withIcebergEnabled(t *testing.T) {
	t.Helper()
	previous := IcebergEnabled
	IcebergEnabled = func() bool { return true }
	t.Cleanup(func() { IcebergEnabled = previous })
}

// icebergTestFile is one object staged under _lakefs_iceberg/files/<uuid>/: a metadata file, a
// manifest list, a manifest or a data file.
type icebergTestFile struct {
	// path is the path relative to the table root, for example "data/part-0.parquet" or
	// "metadata/v1.json". §6.3 gives it the same shape under the reserved prefix.
	path    string
	content []byte
}

// icebergTestTable is one table of a source repository, as the catalog would have written it.
type icebergTestTable struct {
	// namespace holds the namespace levels, outermost first.
	namespace []string
	name      string
	// physicalRoot is the table's physical directory inside the source storage namespace: the
	// root the client wrote to, which the catalog gives every table its own of (Q-I5).
	physicalRoot string
	// tableUUID is the table identity, the path component of every staged file (§6.3).
	tableUUID string
	files     []icebergTestFile
	// metadataDocument, when set, is written at <physicalRoot>/metadata/v1.json and named by the
	// pointer. A test that reads the table back through the catalog needs it; the placement tests
	// only need the entries.
	metadataDocument []byte
}

// metadataLocation is the physical URI of the table's current metadata.json: the value the pointer
// carries, and the one the Iceberg metadata inside it keeps naming (§6.1, §9).
func (f *datasetFixture) metadataLocation(t *testing.T, repo string, tbl icebergTestTable) string {
	t.Helper()
	rec := f.repositoryRecord(t, repo)
	return rec.StorageNamespace.String() + "/" + tbl.physicalRoot + "/metadata/v1.json"
}

// stageIcebergTable stages the entries of one table on branch: the markers of its namespace, its
// pointer and its files. Nothing is committed, so a caller can stage several tables and commit once
// - which is how a real branch ends up holding a whole warehouse.
func (f *datasetFixture) stageIcebergTable(t *testing.T, repo, branch string, tbl icebergTestTable) {
	t.Helper()
	rec := f.repositoryRecord(t, repo)

	// One marker per namespace level, so the dataset can list the namespace afterwards.
	for i := 1; i <= len(tbl.namespace); i++ {
		markerPath := icebergNamespaceMarkerKey(strings.Join(tbl.namespace[:i], "/"))
		require.NoError(t, f.cat.CreateEntry(f.ctx, repo, branch, catalog.DBEntry{
			Path:            markerPath,
			PhysicalAddress: markerPath,
			AddressType:     catalog.AddressTypeRelative,
			CreationDate:    testTime(),
			Metadata:        catalog.Metadata{"owner": "data-platform"},
		}), "stage namespace marker %s", markerPath)
	}

	// The metadata document lives at the table's physical root, where the client wrote it. The
	// pointer names it by absolute URI and never by entry address (pkg/icebergcatalog store.go).
	metadataLocation := f.metadataLocation(t, repo, tbl)
	if tbl.metadataDocument != nil {
		f.putObject(t, repo, tbl.physicalRoot+"/metadata/v1.json", tbl.metadataDocument)
	}

	pointerPath := icebergTablePointerKey(strings.Join(tbl.namespace, "/") + "/" + tbl.name)
	require.NoError(t, f.cat.CreateEntry(f.ctx, repo, branch, catalog.DBEntry{
		Path:            pointerPath,
		PhysicalAddress: pointerPath,
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    testTime(),
		Metadata: catalog.Metadata{
			icebergcatalog.MetadataLocationKey: metadataLocation,
			icebergcatalog.TableUUIDKey:        tbl.tableUUID,
			icebergcatalog.FormatVersionKey:    "2",
		},
	}), "stage table pointer %s", pointerPath)

	// Table files are staged with the full physical address the client wrote (§6.3), so the
	// dataset copies that address and never touches the object.
	for _, file := range tbl.files {
		key := icebergFilesPrefix(tbl.tableUUID) + file.path
		f.putObject(t, repo, key, file.content)
		qk, err := f.cat.BlockAdapter.ResolveNamespace(rec.StorageID.String(), rec.StorageNamespace.String(), key, block.IdentifierTypeRelative)
		require.NoError(t, err, "resolve %s", key)
		require.NoError(t, f.cat.CreateEntry(f.ctx, repo, branch, catalog.DBEntry{
			Path:            key,
			PhysicalAddress: qk.Format(),
			AddressType:     catalog.AddressTypeFull,
			CreationDate:    testTime(),
			Size:            int64(len(file.content)),
			Checksum:        etagOf(file.content),
		}), "stage table file %s", key)
	}
}

// commitIcebergTables stages tables and commits them, returning the commit their dataset items pin.
func (f *datasetFixture) commitIcebergTables(t *testing.T, repo, branch string, tables ...icebergTestTable) graveler.CommitID {
	t.Helper()
	for _, tbl := range tables {
		f.stageIcebergTable(t, repo, branch, tbl)
	}
	log, err := f.cat.Commit(f.ctx, repo, branch, "commit "+repo+"/"+branch, "tester", nil, nil, nil, false)
	require.NoError(t, err, "commit %s/%s", repo, branch)
	return graveler.CommitID(log.Reference)
}

// testOrdersUUID is the table uuid of the standard fixture table.
const testOrdersUUID = "11111111-2222-3333-4444-555555555555"

// testOrdersTable is the standard fixture table: namespace analytics, name orders, four staged
// files - the ones §6.3 stages for a snapshot.
func testOrdersTable() icebergTestTable {
	return icebergTestTable{
		namespace:    []string{"analytics"},
		name:         "orders",
		physicalRoot: "_iceberg/" + testOrdersUUID,
		tableUUID:    testOrdersUUID,
		files: []icebergTestFile{
			{path: "metadata/v1.json", content: []byte(`{"format-version":2}`)},
			{path: "metadata/snap-1.avro", content: []byte("manifest-list")},
			{path: "metadata/manifest-1.avro", content: []byte("manifest")},
			{path: "data/part-0.parquet", content: []byte("parquet-bytes")},
		},
	}
}

// TestPublishTableItem is the §9 payoff: a table item materialises the pointer of the table at the
// pinned commit and every _lakefs_iceberg/files/<uuid>/... entry the snapshot staged, at the same
// keys, so the absolute paths inside the Iceberg metadata stay valid. The objects themselves are
// shared with the source repository - the dataset entry keeps the source's full physical address,
// which is what makes the copy zero-cost (goal G3).
func TestPublishTableItem(t *testing.T) {
	const (
		dataset = "analytics-tables"
		source  = "lakehouse-orders"
	)
	withIcebergEnabled(t)

	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)

	table := testOrdersTable()
	commit := f.commitIcebergTables(t, source, "main", table)

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target:     "analytics/orders",
			Repository: source,
			Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type:       ItemTypeTable,
			Table:      &TableSrc{Namespace: "analytics", Name: "orders"},
		}},
	})

	result := publish(t, f, PublishRequest{
		Dataset:    dataset,
		Version:    1,
		Definition: def,
		Message:    "publish a table",
		Publisher:  "tester",
	})
	require.Equal(t, VersionTag(1), result.Tag)

	// The reserved layout of the source is reproduced verbatim, next to the synthetic definition
	// entry and nothing else: exactly the pointer and the table's staged files - the two things
	// §9 lists. The namespace marker is not copied by a table item, so a dataset of sibling tables
	// cannot produce it twice (see the sibling-tables test).
	require.Equal(t, []string{
		DefinitionPath,
		icebergFilesPrefix(testOrdersUUID) + "data/part-0.parquet",
		icebergFilesPrefix(testOrdersUUID) + "metadata/manifest-1.avro",
		icebergFilesPrefix(testOrdersUUID) + "metadata/snap-1.avro",
		icebergFilesPrefix(testOrdersUUID) + "metadata/v1.json",
		"_lakefs_iceberg/tables/analytics/orders/.pointer",
	}, f.listPaths(t, dataset, VersionTag(1)))

	// The pointer is what the catalog reads: it must decode as a pointer of the same table, with
	// the metadata location untouched (the URI is absolute, §6.1).
	pointer := f.datasetEntry(t, dataset, VersionTag(1), icebergTablePointerKey("analytics/orders"))
	decoded, err := icebergcatalog.DecodeTablePointer(pointer.Metadata)
	require.NoError(t, err, "the copied pointer must decode like the catalog wrote it")
	require.Equal(t, f.metadataLocation(t, source, table), decoded.MetadataLocation)
	require.Equal(t, table.tableUUID, decoded.TableUUID)
	require.Equal(t, 2, decoded.FormatVersion)

	// Every copied file keeps the source's full physical address, so the dataset serves the very
	// bytes the client wrote without copying an object.
	for _, file := range table.files {
		key := icebergFilesPrefix(table.tableUUID) + file.path
		entry := f.datasetEntry(t, dataset, VersionTag(1), key)
		require.Equal(t, catalog.AddressTypeFull, catalog.AddressType(entry.AddressType), "%s keeps a full address", key)
		require.Equal(t, "mem://"+source+"-ns/"+key, entry.PhysicalAddress,
			"%s points into the SOURCE namespace", key)
		require.Equal(t, file.content, f.readDatasetObject(t, dataset, VersionTag(1), key))
	}

	// The stats count what was materialised: the pointer and the four files.
	require.Equal(t, int64(5), result.Stats.Objects)
	require.Len(t, result.Stats.Items, 1)
	require.Equal(t, "analytics/orders", result.Stats.Items[0].Target)
}

// TestPublishSiblingTableItems is the regression for the marker-ownership rule of
// openIcebergItem: two tables of one namespace are a valid definition (they do not overlap,
// §5.2 rule 3), and both of their namespaces' markers would be the same key. Because a table item
// does not copy markers, the publish succeeds and both tables are materialised.
func TestPublishSiblingTableItems(t *testing.T) {
	const (
		dataset = "sibling-tables"
		source  = "lakehouse-siblings"
	)
	withIcebergEnabled(t)

	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)

	orders := testOrdersTable()
	customersUUID := "22222222-3333-4444-5555-666666666666"
	customers := icebergTestTable{
		namespace:    []string{"analytics"},
		name:         "customers",
		physicalRoot: "_iceberg/" + customersUUID,
		tableUUID:    customersUUID,
		files: []icebergTestFile{
			{path: "data/part-0.parquet", content: []byte("customers-parquet")},
		},
	}
	commit := f.commitIcebergTables(t, source, "main", orders, customers)

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{
			{
				Target:     "analytics/orders",
				Repository: source,
				Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type:       ItemTypeTable,
				Table:      &TableSrc{Namespace: "analytics", Name: "orders"},
			},
			{
				Target:     "analytics/customers",
				Repository: source,
				Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type:       ItemTypeTable,
				Table:      &TableSrc{Namespace: "analytics", Name: "customers"},
			},
		},
	})
	result := publish(t, f, PublishRequest{
		Dataset:    dataset,
		Version:    1,
		Definition: def,
		Message:    "two tables of one namespace",
		Publisher:  "tester",
	})
	require.Len(t, result.Stats.Items, 2)

	require.Equal(t, []string{
		DefinitionPath,
		icebergFilesPrefix(testOrdersUUID) + "data/part-0.parquet",
		icebergFilesPrefix(testOrdersUUID) + "metadata/manifest-1.avro",
		icebergFilesPrefix(testOrdersUUID) + "metadata/snap-1.avro",
		icebergFilesPrefix(testOrdersUUID) + "metadata/v1.json",
		icebergFilesPrefix(customersUUID) + "data/part-0.parquet",
		"_lakefs_iceberg/tables/analytics/customers/.pointer",
		"_lakefs_iceberg/tables/analytics/orders/.pointer",
	}, f.listPaths(t, dataset, VersionTag(1)))
}

// TestPublishNamespaceItem pins the namespace item of §9: the marker, every marker below it, every
// table pointer below it, and the files of every one of those tables.
func TestPublishNamespaceItem(t *testing.T) {
	const (
		dataset = "analytics-warehouse"
		source  = "lakehouse-warehouse"
	)
	withIcebergEnabled(t)

	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)

	orders := testOrdersTable()
	nestedUUID := "99999999-8888-7777-6666-555555555555"
	nested := icebergTestTable{
		namespace:    []string{"analytics", "books"},
		name:         "titles",
		physicalRoot: "_iceberg/" + nestedUUID,
		tableUUID:    nestedUUID,
		files: []icebergTestFile{
			{path: "data/part-0.parquet", content: []byte("titles-parquet")},
		},
	}
	// "sales" is a sibling namespace and must not be dragged in by the analytics item.
	other := icebergTestTable{
		namespace:    []string{"sales"},
		name:         "invoices",
		physicalRoot: "_iceberg/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		tableUUID:    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	commit := f.commitIcebergTables(t, source, "main", orders, nested, other)

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target:     "analytics",
			Repository: source,
			Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type:       ItemTypeNamespace,
			Namespace:  ptr("analytics"),
		}},
	})
	publish(t, f, PublishRequest{
		Dataset:    dataset,
		Version:    1,
		Definition: def,
		Message:    "publish a namespace",
		Publisher:  "tester",
	})

	require.Equal(t, []string{
		DefinitionPath,
		"_lakefs_iceberg/files/11111111-2222-3333-4444-555555555555/data/part-0.parquet",
		"_lakefs_iceberg/files/11111111-2222-3333-4444-555555555555/metadata/manifest-1.avro",
		"_lakefs_iceberg/files/11111111-2222-3333-4444-555555555555/metadata/snap-1.avro",
		"_lakefs_iceberg/files/11111111-2222-3333-4444-555555555555/metadata/v1.json",
		"_lakefs_iceberg/files/99999999-8888-7777-6666-555555555555/data/part-0.parquet",
		"_lakefs_iceberg/ns/analytics/.namespace",
		"_lakefs_iceberg/ns/analytics/books/.namespace",
		"_lakefs_iceberg/tables/analytics/books/titles/.pointer",
		"_lakefs_iceberg/tables/analytics/orders/.pointer",
	}, f.listPaths(t, dataset, VersionTag(1)), "the whole analytics subtree, and only it")

	// A namespace item owns the markers, so the dataset answers the catalog's namespace reads for
	// the namespace and for every namespace below it.
	store := icebergcatalog.NewStore(f.cat, f.cat.BlockAdapter)
	for _, namespace := range []icebergcatalog.Namespace{{"analytics"}, {"analytics", "books"}} {
		marker, err := store.GetNamespace(f.ctx, icebergcatalog.Address{
			Repo:      dataset,
			Ref:       VersionTag(1),
			Namespace: namespace,
		})
		require.NoError(t, err, "the dataset must answer the catalog's namespace read for %v", namespace)
		require.Equal(t, "data-platform", marker.Properties["owner"])
	}
}

// TestPublishTableItemMissingSource pins the answer for a table that does not exist at the pinned
// commit: it is a definition problem naming the item (like a missing object key, §5.2 rule 5), and
// the publish writes nothing at all.
func TestPublishTableItemMissingSource(t *testing.T) {
	const (
		dataset = "missing-table"
		source  = "lakehouse-missing"
	)
	withIcebergEnabled(t)

	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)
	// A table in another namespace exists, so the fixture is a real warehouse and only the pinned
	// address is wrong.
	commit := f.commitIcebergTables(t, source, "main", testOrdersTable())
	f.createRepository(t, dataset, DefaultBranch, true)
	headBefore, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target:     "analytics/customers",
			Repository: source,
			Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type:       ItemTypeTable,
			Table:      &TableSrc{Namespace: "analytics", Name: "customers"},
		}},
	})
	_, err = (&Materializer{Catalog: f.cat}).Publish(f.ctx, PublishRequest{
		Dataset:    dataset,
		Version:    1,
		Definition: def,
		Message:    "must be rejected",
		Publisher:  "tester",
	})
	var validation *ValidationError
	require.ErrorAs(t, err, &validation, "a missing table is a definition problem, not a server error")
	require.Len(t, validation.Problems, 1)
	require.Equal(t, "data[0].table", validation.Problems[0].Path)
	require.Contains(t, validation.Problems[0].Message, "analytics.customers")

	_, err = f.cat.GetTag(f.ctx, dataset, VersionTag(1))
	require.Error(t, err, "a rejected publish creates no version tag")
	headAfter, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)
	require.Equal(t, headBefore, headAfter, "a rejected publish commits nothing")
}

// TestPublishTableItemCorruptPointer pins that a pointer without a table uuid fails the publish
// instead of silently publishing a table without its files: the uuid is what names the staged-file
// prefix (§6.3), so a table without one cannot be materialised.
func TestPublishTableItemCorruptPointer(t *testing.T) {
	const (
		dataset = "corrupt-pointer"
		source  = "lakehouse-corrupt"
	)
	withIcebergEnabled(t)

	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)

	pointerPath := icebergTablePointerKey("analytics/orders")
	require.NoError(t, f.cat.CreateEntry(f.ctx, source, "main", catalog.DBEntry{
		Path:            pointerPath,
		PhysicalAddress: pointerPath,
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    testTime(),
		Metadata:        catalog.Metadata{icebergcatalog.MetadataLocationKey: "mem://lakehouse-corrupt-ns/x.json"},
	}))
	log, err := f.cat.Commit(f.ctx, source, "main", "pointer without a uuid", "tester", nil, nil, nil, false)
	require.NoError(t, err)

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target:     "analytics/orders",
			Repository: source,
			Ref:        SourceRef{Type: RefTypeCommit, ID: graveler.CommitID(log.Reference).String()},
			Type:       ItemTypeTable,
			Table:      &TableSrc{Namespace: "analytics", Name: "orders"},
		}},
	})
	_, err = (&Materializer{Catalog: f.cat}).Publish(f.ctx, PublishRequest{
		Dataset:    dataset,
		Version:    1,
		Definition: def,
		Message:    "must be rejected",
		Publisher:  "tester",
	})
	require.ErrorIs(t, err, ErrInvalidSourceEntry)
	require.Contains(t, err.Error(), icebergcatalog.TableUUIDKey)
}

// TestOpenItemsTableItemRequiresIceberg pins the gate at the materialisation boundary, for a caller
// that skipped validation: with the catalog disabled a table item cannot be opened at all, which is
// the same answer the definition gets from Validate (§10.4).
func TestOpenItemsTableItemRequiresIceberg(t *testing.T) {
	require.False(t, IcebergEnabled(), "the fixture must run with the catalog disabled")
	f := newDatasetFixture(t)
	f.createRepository(t, "src-no-iceberg", "main", false)

	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{{
		Target:     "analytics/orders",
		Repository: "src-no-iceberg",
		Ref:        SourceRef{Type: RefTypeCommit, ID: "abc"},
		Type:       ItemTypeTable,
		Table:      &TableSrc{Namespace: "analytics", Name: "orders"},
	}}}
	items, err := (&Materializer{Catalog: f.cat}).openItems(f.ctx, def)
	require.Nil(t, items)
	require.ErrorIs(t, err, ErrUnsupportedItemType)
}

// TestOpenItemsTableItemRejectsForeignTarget pins the primitive's own guard for the target rule of
// §9: the keys a reserved item produces are fixed by the layout, so a definition whose target
// disagrees cannot be honoured and must not be guessed at. Validation rejects it first; this covers
// a caller that skipped validation.
func TestOpenItemsTableItemRejectsForeignTarget(t *testing.T) {
	withIcebergEnabled(t)
	f := newDatasetFixture(t)
	f.createRepository(t, "src-foreign-target", "main", false)
	commit := f.commitIcebergTables(t, "src-foreign-target", "main", testOrdersTable())

	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{{
		Target:     "elsewhere",
		Repository: "src-foreign-target",
		Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
		Type:       ItemTypeTable,
		Table:      &TableSrc{Namespace: "analytics", Name: "orders"},
	}}}
	items, err := (&Materializer{Catalog: f.cat}).openItems(f.ctx, def)
	require.Nil(t, items)
	var validation *ValidationError
	require.ErrorAs(t, err, &validation)
	require.Equal(t, []string{"data[0].target"}, problemPaths(validation.Problems))
}

// TestTableItemConflictingItemsFailTheMerge pins that two items claiming the same table fail the
// publish through the §6.4 merge rather than producing a metarange with two values for one key.
// Validation rejects the overlap first (§5.2 rule 3); this drives the primitive directly.
func TestTableItemConflictingItemsFailTheMerge(t *testing.T) {
	withIcebergEnabled(t)
	f := newDatasetFixture(t)
	f.createRepository(t, "src-conflict", "main", false)
	commit := f.commitIcebergTables(t, "src-conflict", "main", testOrdersTable())

	tableItem := DataItem{
		Target:     "analytics/orders",
		Repository: "src-conflict",
		Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
		Type:       ItemTypeTable,
		Table:      &TableSrc{Namespace: "analytics", Name: "orders"},
	}
	namespaceItem := DataItem{
		Target:     "analytics",
		Repository: "src-conflict",
		Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
		Type:       ItemTypeNamespace,
		Namespace:  ptr("analytics"),
	}
	// The namespace item already covers the table, so the two together materialise its keys twice.
	def := Definition{FormatVersion: FormatVersion, Data: []DataItem{tableItem, namespaceItem}}
	require.Error(t, Validate(&def, ValidateOptions{}), "validation rejects the overlap first")

	merged, _ := mergedItems(t, f, def)
	_, err := drainKeys(t, merged)
	require.ErrorIs(t, err, ErrDuplicateKey)
}

// TestRecordsIterator pins the iterator the reserved items are built on: sorted output, and a SeekGE
// that positions on the first key >= the target, which is what the metarange writer's seek contract
// needs.
func TestRecordsIterator(t *testing.T) {
	makeRecord := func(key string) *graveler.ValueRecord {
		return &graveler.ValueRecord{Key: graveler.Key(key), Value: &graveler.Value{Data: []byte(key)}}
	}
	records := []*graveler.ValueRecord{
		makeRecord("b"),
		makeRecord("a"),
		makeRecord("c"),
	}
	it := newRecordsIterator(records)
	keys, err := drainKeys(t, it)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, keys, "records are sorted on construction")

	for _, tc := range []struct {
		seek   string
		expect []string
	}{
		{seek: "", expect: []string{"a", "b", "c"}},
		{seek: "a", expect: []string{"a", "b", "c"}},
		{seek: "aa", expect: []string{"b", "c"}},
		{seek: "c", expect: []string{"c"}},
		{seek: "d", expect: []string{}},
	} {
		it := newRecordsIterator(records)
		it.SeekGE(graveler.Key(tc.seek))
		keys, err := drainKeys(t, it)
		require.NoError(t, err)
		require.Equal(t, tc.expect, keys, "seek %q", tc.seek)
	}
}

// icebergMetadataDocument renders a v2 table metadata document at location, in the shape the
// Iceberg spec defines ("Table Metadata" in
// https://iceberg.apache.org/spec/#table-metadata) and iceberg-go parses.
func icebergMetadataDocument(tableUUID, location string) []byte {
	return []byte(fmt.Sprintf(`{
		"format-version": 2,
		"table-uuid": %q,
		"location": %q,
		"last-sequence-number": 1,
		"last-updated-ms": 1700000000000,
		"last-column-id": 1,
		"current-schema-id": 0,
		"schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"last-partition-id": 999,
		"default-sort-order-id": 0,
		"sort-orders": [{"order-id": 0, "fields": []}],
		"properties": {},
		"current-snapshot-id": -1,
		"snapshots": [],
		"snapshot-log": [],
		"metadata-log": []
	}`, tableUUID, location))
}

// TestPublishedTableReadableThroughCatalog is the §9 read path end to end: after a table item is
// published, the *catalog's own reader* finds the table in the dataset repository at the version
// tag and loads its metadata. Nothing in this test recomputes the reserved keys, so it is what
// proves the placement - not just the copy - is the one pkg/icebergcatalog addresses.
func TestPublishedTableReadableThroughCatalog(t *testing.T) {
	const (
		dataset = "catalog-readable"
		source  = "lakehouse-readable"
	)
	withIcebergEnabled(t)

	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)

	table := testOrdersTable()
	table.metadataDocument = icebergMetadataDocument(table.tableUUID, "mem://"+source+"-ns/"+table.physicalRoot)
	commit := f.commitIcebergTables(t, source, "main", table)

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target:     "analytics/orders",
			Repository: source,
			Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type:       ItemTypeTable,
			Table:      &TableSrc{Namespace: "analytics", Name: "orders"},
		}},
	})
	publish(t, f, PublishRequest{
		Dataset:    dataset,
		Version:    1,
		Definition: def,
		Message:    "publish a table",
		Publisher:  "tester",
	})

	store := icebergcatalog.NewStore(f.cat, f.cat.BlockAdapter)
	addr := icebergcatalog.Address{
		Repo:      dataset,
		Ref:       VersionTag(1),
		Namespace: icebergcatalog.Namespace{"analytics"},
		Table:     "orders",
	}

	// The table loads: pointer, metadata location and the metadata document itself, which the
	// catalog reads from the source namespace through the pointer's absolute URI.
	loaded, err := store.GetTable(f.ctx, addr)
	require.NoError(t, err, "the dataset must answer the catalog's table read")
	require.Equal(t, f.metadataLocation(t, source, table), loaded.Pointer.MetadataLocation)
	require.Equal(t, table.tableUUID, loaded.Pointer.TableUUID)
	require.Equal(t, table.metadataDocument, loaded.MetadataJSON,
		"the metadata document is read from where the pointer names it, unmodified")
}
