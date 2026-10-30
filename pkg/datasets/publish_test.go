package datasets

// publish_test.go covers the publish primitive end to end on kv/mem + block/mem with a
// real catalog (§15.1): §6.2 steps 6-10, the zero-copy read-back of goal G3, and the
// no-op detection of §6.2 step 8.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// publish calls the publish primitive on a resolved definition and fails on error.
func publish(t *testing.T, f *datasetFixture, req PublishRequest) *PublishResult {
	t.Helper()
	result, err := (&Materializer{Catalog: f.cat}).Publish(f.ctx, req)
	require.NoError(t, err, "publish %s version %d", req.Dataset, req.Version)
	return result
}

// resolvedDefinition validates and resolves def, the two steps the service performs
// before it calls the publish primitive (§6.2 steps 1-2).
func resolvedDefinition(t *testing.T, f *datasetFixture, def Definition) Definition {
	t.Helper()
	require.NoError(t, Validate(&def, ValidateOptions{}), "validate definition")
	resolved, err := (&Resolver{Catalog: f.cat}).Resolve(f.ctx, &def)
	require.NoError(t, err, "resolve definition")
	return *resolved
}

// TestPublish_FirstVersionEndToEnd is the v1 publish of §6.2 steps 6-10: metarange,
// commit, tag, commit metadata, the synthetic definition entry, and a read back through
// catalog.GetEntry + BlockAdapter.Get that returns the source bytes untouched (goal G3).
func TestPublish_FirstVersionEndToEnd(t *testing.T) {
	const (
		dataset = "customer360"
		source  = "lakehouse-repo"
	)
	f := newDatasetFixture(t)
	f.createRepository(t, source, "main", false)
	f.createRepository(t, dataset, DefaultBranch, true) // a dataset is a read-only repository (§6.1)

	customers := []testObject{
		{key: "data/customers/c1.parquet", content: []byte("c1-bytes")},
		{key: "data/customers/c2.parquet", content: []byte("c2-bytes")},
		{key: "data/customers/c3.parquet", content: []byte("c3-bytes")},
	}
	countries := testObject{key: "geo/countries.csv", content: []byte("code,name\nIL,Israel\n")}
	objects := append(append([]testObject{}, customers...), countries)
	commit := f.commitObjects(t, source, "main", objects...)
	_, err := f.cat.CreateTag(f.ctx, source, "release-1", commit.String())
	require.NoError(t, err, "create tag release-1")

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Description:   "curated customer view",
		Metadata:      map[string]any{"owner": "data-platform"},
		Data: []DataItem{
			{
				Target: "customers", Repository: source,
				Ref:  SourceRef{Type: RefTypeBranch, ID: "main"},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/customers/"},
			},
			{
				Target: "reference/countries.csv", Repository: source,
				Ref:  SourceRef{Type: RefTypeTag, ID: "release-1"},
				Type: ItemTypeObject, Object: &PathSource{Path: "geo/countries.csv"},
			},
		},
	})

	result := publish(t, f, PublishRequest{
		Dataset:    dataset,
		Version:    1,
		Definition: def,
		Message:    "initial dataset with customer objects",
		Publisher:  "tester",
	})
	require.Equal(t, 1, result.Version)
	require.Equal(t, VersionTag(1), result.Tag)
	require.NotEmpty(t, result.CommitID.String())
	require.NotEmpty(t, result.MetaRangeID.String())
	require.Equal(t, int64(len(objects)), result.Objects)
	require.Equal(t, totalBytes(objects), result.Bytes)
	require.Equal(t, ItemStats{
		Objects: int64(len(objects)),
		Bytes:   totalBytes(objects),
		Items: []ItemStat{
			{Target: "customers", Objects: int64(len(customers)), Bytes: totalBytes(customers)},
			{Target: "reference/countries.csv", Objects: 1, Bytes: int64(len(countries.content))},
		},
	}, result.Stats, "the result carries the store's ItemStats shape, per item (§5.3)")

	// §6.2 step 10: the tag and the branch head both point at the version commit (§5.4).
	tagCommit, err := f.cat.GetTag(f.ctx, dataset, VersionTag(1))
	require.NoError(t, err, "tag v1")
	require.Equal(t, result.CommitID.String(), tagCommit)
	head, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err, "branch latest")
	require.Equal(t, result.CommitID.String(), head)

	// §6.2 step 9: the commit metadata the reconciler and history grep for (§12.4).
	log, err := f.cat.GetCommit(f.ctx, dataset, result.CommitID.String())
	require.NoError(t, err)
	require.Equal(t, CommitMetadataVersionKey, "dataset.version")
	require.Equal(t, "1", log.Metadata[CommitMetadataVersionKey])
	require.Equal(t, "tester", log.Metadata[CommitMetadataPublisherKey])
	require.NotContains(t, log.Metadata, CommitMetadataProposalKey, "a direct publish has no proposal")
	require.Equal(t, "initial dataset with customer objects", log.Message)
	require.Equal(t, result.MetaRangeID.String(), log.MetaRangeID)

	// The definition entry sorts with the data and is the only entry outside the targets.
	require.Equal(t, []string{
		DefinitionPath,
		"customers/c1.parquet",
		"customers/c2.parquet",
		"customers/c3.parquet",
		"reference/countries.csv",
	}, f.listPaths(t, dataset, DefaultBranch), "the metarange is key-sorted (§6.4)")

	// §6.4: the version is self-describing.
	doc := f.readDefinitionDocument(t, dataset, DefaultBranch)
	require.Equal(t, dataset, doc.Dataset)
	require.Equal(t, 1, doc.Version)
	require.Equal(t, "tester", doc.Publisher)
	require.Equal(t, "initial dataset with customer objects", doc.Message)
	require.Equal(t, def, doc.Definition)
	require.Equal(t, RefTypeCommit, doc.Definition.Data[0].Ref.Type, "the stored definition holds commit refs only")

	// Goal G3: zero-copy. Every data entry is a full address into the source namespace,
	// and reading it through the dataset repository returns the source bytes.
	for _, object := range objects {
		path := map[string]string{
			"data/customers/c1.parquet": "customers/c1.parquet",
			"data/customers/c2.parquet": "customers/c2.parquet",
			"data/customers/c3.parquet": "customers/c3.parquet",
			"geo/countries.csv":         "reference/countries.csv",
		}[object.key]
		entry := f.datasetEntry(t, dataset, DefaultBranch, path)
		require.Equal(t, catalog.AddressTypeFull, entry.AddressType, "%s must keep a full address", path)
		require.Equal(t, "mem://"+source+"-ns/"+f.addressOf(t, source, object.key), entry.PhysicalAddress,
			"%s must point into the source repository namespace", path)
		require.Equal(t, etagOf(object.content), entry.Checksum, "%s ETag must be preserved", path)
		require.Equal(t, int64(len(object.content)), entry.Size, "%s size must be preserved", path)
		require.Equal(t, catalog.Metadata{"origin": source}, entry.Metadata, "%s metadata must be preserved", path)
		require.Equal(t, object.content, f.readDatasetObject(t, dataset, DefaultBranch, path),
			"%s must read back the source bytes", path)
	}
}

// TestPublish_SortedAcrossPrefixAndObjectTargets runs the §6.4 a / a-b / a/x case through
// the real publish: a naive concatenation of the item streams would hand
// WriteMetaRangeByIterator an unsorted iterator and fail the publish.
func TestPublish_SortedAcrossPrefixAndObjectTargets(t *testing.T) {
	const dataset = "sortedness"
	f := newDatasetFixture(t)
	f.createRepository(t, "src-sorted", "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)
	commit := f.commitObjects(t, "src-sorted", "main",
		testObject{key: "data/a/x.parquet", content: []byte("x")},
		testObject{key: "data/a/y.parquet", content: []byte("y")},
		testObject{key: "data/ab.parquet", content: []byte("ab")},
	)

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{
			{
				Target: "a", Repository: "src-sorted", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/a/"},
			},
			{
				Target: "a-b", Repository: "src-sorted", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type: ItemTypeObject, Object: &PathSource{Path: "data/ab.parquet"},
			},
		},
	})
	publish(t, f, PublishRequest{Dataset: dataset, Version: 1, Definition: def, Message: "sorted", Publisher: "tester"})

	require.Equal(t, []string{DefinitionPath, "a-b", "a/x.parquet", "a/y.parquet"},
		f.listPaths(t, dataset, DefaultBranch))
	require.Equal(t, []byte("ab"), f.readDatasetObject(t, dataset, DefaultBranch, "a-b"))
	require.Equal(t, []byte("x"), f.readDatasetObject(t, dataset, DefaultBranch, "a/x.parquet"))
}

// TestPublish_ManyObjectsTwoItems pins the merge over a realistic item count: two prefix
// items with many objects each, published in one version.
func TestPublish_ManyObjectsTwoItems(t *testing.T) {
	const (
		dataset   = "many-objects"
		perItem   = 25
		targetOne = "one"
		targetTwo = "two"
	)
	f := newDatasetFixture(t)
	f.createRepository(t, "src-many", "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)

	objects := make([]testObject, 0, 2*perItem)
	for i := range perItem {
		suffix := string(rune('a'+i%26)) + "-" + twoDigits(i) + ".parquet"
		objects = append(objects, testObject{
			key:     "left/" + suffix,
			content: []byte("left-" + twoDigits(i)),
		}, testObject{
			key:     "right/" + suffix,
			content: []byte("right-" + twoDigits(i)),
		})
	}
	commit := f.commitObjects(t, "src-many", "main", objects...)

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{
			{
				Target: targetOne, Repository: "src-many", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "left/"},
			},
			{
				Target: targetTwo, Repository: "src-many", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "right/"},
			},
		},
	})
	result := publish(t, f, PublishRequest{Dataset: dataset, Version: 1, Definition: def, Message: "many", Publisher: "tester"})
	require.Equal(t, int64(len(objects)), result.Objects)
	require.Equal(t, totalBytes(objects), result.Bytes)

	paths := f.listPaths(t, dataset, DefaultBranch)
	require.Len(t, paths, len(objects)+1, "every object plus the definition entry")
	require.Equal(t, DefinitionPath, paths[0])
	require.Equal(t, targetOne+"/a-00.parquet", paths[1])
	for i := 1; i < len(paths); i++ {
		require.Less(t, paths[i-1], paths[i], "the metarange must be strictly increasing")
	}

	// Spot-check one object from each item, including its bytes.
	lastSuffix := string(rune('a'+(perItem-1)%26)) + "-" + twoDigits(perItem-1) + ".parquet"
	require.Equal(t, []byte("left-"+twoDigits(perItem-1)), f.readDatasetObject(t, dataset, DefaultBranch, targetOne+"/"+lastSuffix))
	require.Equal(t, []byte("right-"+twoDigits(perItem-1)), f.readDatasetObject(t, dataset, DefaultBranch, targetTwo+"/"+lastSuffix))
}

// TestPublish_SecondVersionKeepsFirstReadable pins versioning semantics: v2 gets its own
// commit and tag, latest moves, and v1 keeps reading the old data because its metarange
// points at the old source commit (§5.4, §7.1).
func TestPublish_SecondVersionKeepsFirstReadable(t *testing.T) {
	const dataset = "versioned"
	f := newDatasetFixture(t)
	f.createRepository(t, "src-versioned", "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)

	firstCommit := f.commitObjects(t, "src-versioned", "main",
		testObject{key: "data/a.parquet", content: []byte("v1-bytes")})
	secondCommit := f.commitObjects(t, "src-versioned", "main",
		testObject{key: "data/a.parquet", content: []byte("v2-bytes")})

	prefixItem := func(commit graveler.CommitID) DataItem {
		return DataItem{
			Target: "data", Repository: "src-versioned", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
		}
	}

	v1Definition := resolvedDefinition(t, f, Definition{FormatVersion: FormatVersion, Data: []DataItem{prefixItem(firstCommit)}})
	v1 := publish(t, f, PublishRequest{
		Dataset: dataset, Version: 1, Definition: v1Definition, Message: "v1", Publisher: "tester",
	})

	v2Definition := resolvedDefinition(t, f, Definition{FormatVersion: FormatVersion, Data: []DataItem{prefixItem(secondCommit)}})
	v2 := publish(t, f, PublishRequest{
		Dataset: dataset, Version: 2, Definition: v2Definition, Message: "v2", Publisher: "tester",
		Previous: PreviousVersion{Number: 1, MetaRangeID: v1.MetaRangeID, Definition: &v1Definition},
	})

	require.NotEqual(t, v1.CommitID, v2.CommitID, "each version is its own commit")
	require.NotEqual(t, v1.MetaRangeID, v2.MetaRangeID, "each version is its own metarange")

	tagOne, err := f.cat.GetTag(f.ctx, dataset, VersionTag(1))
	require.NoError(t, err)
	require.Equal(t, v1.CommitID.String(), tagOne, "v1 keeps pointing at the first commit")
	tagTwo, err := f.cat.GetTag(f.ctx, dataset, VersionTag(2))
	require.NoError(t, err)
	require.Equal(t, v2.CommitID.String(), tagTwo)

	head, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)
	require.Equal(t, v2.CommitID.String(), head, "latest moves to the new version")

	require.Equal(t, []byte("v1-bytes"), f.readDatasetObject(t, dataset, VersionTag(1), "data/a.parquet"))
	require.Equal(t, []byte("v2-bytes"), f.readDatasetObject(t, dataset, VersionTag(2), "data/a.parquet"))
	require.Equal(t, []byte("v2-bytes"), f.readDatasetObject(t, dataset, DefaultBranch, "data/a.parquet"))

	// Each version carries its own definition document (§6.4).
	require.Equal(t, 1, f.readDefinitionDocument(t, dataset, VersionTag(1)).Version)
	require.Equal(t, 2, f.readDefinitionDocument(t, dataset, VersionTag(2)).Version)
}

// TestPublish_NoChanges pins §6.2 step 8: republishing the same definition is not a
// version, and it must not leave a commit or a tag behind.
func TestPublish_NoChanges(t *testing.T) {
	const dataset = "no-changes"
	f := newDatasetFixture(t)
	f.createRepository(t, "src-nochanges", "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)
	commit := f.commitObjects(t, "src-nochanges", "main",
		testObject{key: "data/a.parquet", content: []byte("a")})

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target: "a.parquet", Repository: "src-nochanges", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
		}},
	})
	first := publish(t, f, PublishRequest{Dataset: dataset, Version: 1, Definition: def, Message: "v1", Publisher: "tester"})

	_, err := (&Materializer{Catalog: f.cat}).Publish(f.ctx, PublishRequest{
		Dataset: dataset, Version: 2, Definition: def, Message: "v2", Publisher: "tester",
		Previous: PreviousVersion{Number: 1, MetaRangeID: first.MetaRangeID, Definition: &def},
	})
	require.ErrorIs(t, err, ErrNoChanges, "an identical definition is not a new version")

	// Nothing was committed: latest is still v1 and no v2 tag exists.
	head, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)
	require.Equal(t, first.CommitID.String(), head)
	_, err = f.cat.GetTag(f.ctx, dataset, VersionTag(2))
	require.ErrorIs(t, err, graveler.ErrNotFound, "a rejected publish must not create its tag")
}

// TestPublish_SourceObjectNotFound pins that materialisation refuses to publish a
// version that silently misses an object item's target (§5.2 rule 5); it is a typed
// error so the API can report which item is broken.
func TestPublish_SourceObjectNotFound(t *testing.T) {
	const dataset = "missing-source"
	f := newDatasetFixture(t)
	f.createRepository(t, "src-missing", "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)
	commit := f.commitObjects(t, "src-missing", "main",
		testObject{key: "data/a.parquet", content: []byte("a")})

	def := Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target: "gone.parquet", Repository: "src-missing", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypeObject, Object: &PathSource{Path: "data/gone.parquet"},
		}},
	}
	require.NoError(t, Validate(&def, ValidateOptions{}))

	headBefore, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)

	_, err = (&Materializer{Catalog: f.cat}).Publish(f.ctx, PublishRequest{
		Dataset: dataset, Version: 1, Definition: def, Message: "missing", Publisher: "tester",
	})
	require.ErrorIs(t, err, ErrSourceKeyNotFound)

	var sourceErr *SourceError
	require.ErrorAs(t, err, &sourceErr)
	require.Equal(t, "src-missing", sourceErr.Repository)

	// A failed publish leaves no version behind.
	_, err = f.cat.GetTag(f.ctx, dataset, VersionTag(1))
	require.ErrorIs(t, err, graveler.ErrNotFound)
	head, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)
	require.Equal(t, headBefore, head, "a failed publish must not move latest")
}

// TestPublish_RejectsNonResolvedRef keeps the primitive honest about its contract: a
// definition that still carries a branch ref has to go through Resolver first.
func TestPublish_RejectsNonResolvedRef(t *testing.T) {
	const dataset = "unresolved"
	f := newDatasetFixture(t)
	f.createRepository(t, "src-unresolved", "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)
	f.commitObjects(t, "src-unresolved", "main", testObject{key: "data/a.parquet", content: []byte("a")})

	_, err := (&Materializer{Catalog: f.cat}).Publish(f.ctx, PublishRequest{
		Dataset: dataset, Version: 1, Message: "unresolved", Publisher: "tester",
		Definition: Definition{
			FormatVersion: FormatVersion,
			Data: []DataItem{{
				Target: "a.parquet", Repository: "src-unresolved", Ref: SourceRef{Type: RefTypeBranch, ID: "main"},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
			}},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not resolved to a commit")
}

// TestPublish_EnforcesMetadataRequiredKeys wires the validate.go MetadataValidator hook
// into the publish path: §5.5 says required metadata keys are enforced whenever a
// definition is written, publish included, so a definition that lacks one must not
// become a version.
func TestPublish_EnforcesMetadataRequiredKeys(t *testing.T) {
	const dataset = "metadata-gate"
	f := newDatasetFixture(t)
	f.createRepository(t, "src-metadata", "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)
	commit := f.commitObjects(t, "src-metadata", "main",
		testObject{key: "data/a.parquet", content: []byte("a")})

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target: "a.parquet", Repository: "src-metadata", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
		}},
	})
	headBefore, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)
	mat := &Materializer{
		Catalog: f.cat,
		ValidateOptions: ValidateOptions{Metadata: MetadataKeysValidator([]MetadataKey{
			{Name: "owner", Type: MetadataTypeString, Required: true},
		}, false)},
	}

	_, err = mat.Publish(f.ctx, PublishRequest{
		Dataset: dataset, Version: 1, Definition: def, Message: "missing owner", Publisher: "tester",
	})
	require.Error(t, err, "a required metadata key must fail the publish")
	var validationErr *ValidationError
	require.ErrorAs(t, err, &validationErr)
	require.Contains(t, err.Error(), "owner")

	// A rejected publish writes nothing: no v1 tag, and latest did not move.
	_, err = f.cat.GetTag(f.ctx, dataset, VersionTag(1))
	require.ErrorIs(t, err, graveler.ErrNotFound)
	headAfter, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)
	require.Equal(t, headBefore, headAfter)

	// Supplying the required key makes the very same definition publishable.
	def.Metadata = map[string]any{"owner": "data-platform"}
	result, err := mat.Publish(f.ctx, PublishRequest{
		Dataset: dataset, Version: 1, Definition: def, Message: "with owner", Publisher: "tester",
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.Version)
}

// TestPublish_VersionRecordMatchesStore pins the record-shape agreement with store.go:
// the result builds a store.VersionRecord that Store.PutVersion/GetVersion accept
// unchanged, and PreviousVersionFromRecord turns that record back into the
// PreviousVersion the next publish's no-op check consumes.
func TestPublish_VersionRecordMatchesStore(t *testing.T) {
	const dataset = "recorded"
	f := newDatasetFixture(t)
	f.createRepository(t, "src-recorded", "main", false)
	f.createRepository(t, dataset, DefaultBranch, true)
	commit := f.commitObjects(t, "src-recorded", "main",
		testObject{key: "data/a.parquet", content: []byte("a")},
		testObject{key: "data/b.parquet", content: []byte("bb")})

	def := resolvedDefinition(t, f, Definition{
		FormatVersion: FormatVersion,
		Metadata:      map[string]any{"owner": "data-platform"},
		Data: []DataItem{{
			Target: "data", Repository: "src-recorded", Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
		}},
	})
	req := PublishRequest{
		Dataset: dataset, Version: 1, Definition: def, Message: "recorded", Publisher: "tester",
		ProposalID: "01Jproposalrecord",
	}
	result := publish(t, f, req)

	record := result.VersionRecord(req, testTime())
	require.Equal(t, dataset, record.Dataset)
	require.Equal(t, 1, record.Version)
	require.Equal(t, result.CommitID.String(), record.CommitID)
	require.Equal(t, result.MetaRangeID.String(), record.MetaRangeID)
	require.Equal(t, req.Message, record.Message)
	require.Equal(t, req.Publisher, record.Publisher)
	require.Equal(t, req.ProposalID, record.ProposalID)
	require.Equal(t, testTime(), record.PublishedAt)
	require.Equal(t, result.Definition, record.Definition)
	require.Equal(t, result.Stats, record.Stats)
	require.True(t, record.HasInlineDefinition())

	// The store accepts the record as is: same key layout, same field shape (§5.3).
	store, _, ctx := newTestStore(t)
	_, err := store.PutVersion(ctx, *record)
	require.NoError(t, err, "PutVersion accepts the record the publish result builds")
	stored, err := store.GetVersion(ctx, dataset, 1)
	require.NoError(t, err)
	requireJSONEq(t, record, stored)

	previous := PreviousVersionFromRecord(stored)
	require.Equal(t, 1, previous.Number)
	require.Equal(t, result.MetaRangeID, previous.MetaRangeID)
	require.NotNil(t, previous.Definition)
	require.Equal(t, result.Definition, *previous.Definition)

	// A record behind DefinitionRef (§5.3 large definitions) still yields the metarange.
	stored.Definition = Definition{}
	stored.DefinitionRef = &DefinitionRef{CommitID: stored.CommitID, Path: DefinitionPath}
	require.Nil(t, PreviousVersionFromRecord(stored).Definition)
	require.Equal(t, PreviousVersion{}, PreviousVersionFromRecord(nil))
}

// twoDigits renders a zero-padded two-digit number for stable, sortable test keys.
func twoDigits(i int) string {
	if i < 10 {
		return "0" + string(rune('0'+i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}
