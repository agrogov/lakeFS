package datasets

// diff_test.go covers the version diff of DATASETS_DESIGN.md §10.3 (and the diff row of
// §10.1): the definition diff on two resolved definitions, its ordering guarantees, and
// the data diff on a real catalog (kv/mem + block/mem, §15.1) between a dataset's v1 and
// v2, including _dataset/ filtering, pagination and the three ref forms of §10.3.
//
// The data fixtures publish real versions through the service, so every assertion is about
// what a client actually sees: real commits, real tags, real metaranges.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// anyPtr returns a pointer to v, for the From/To members of a MetadataDiff (§10.2
// MetadataChange).
func anyPtr(v any) *any { return &v }

// --- definition diff: pure data code (§10.3) ---

// TestDiffDefinitionsItemsMetadataAndDescription is the structural diff of §10.3: items are
// matched by target and classified added/removed/changed, and the description and metadata
// maps are diffed alongside them.
func TestDiffDefinitionsItemsMetadataAndDescription(t *testing.T) {
	leftItems := []DataItem{
		prefixItem("customers", "data/customers/"),
		objectItem("reference/countries.csv", "geo/countries.csv"),
		objectItem("reference/unchanged.csv", "geo/unchanged.csv"),
	}
	rightItems := []DataItem{
		prefixItem("customers", "data/customers/"),
		objectItem("reference/regions.csv", "geo/regions.csv"),
		objectItem("reference/unchanged.csv", "geo/unchanged.csv"),
	}
	// The prefix item keeps its target and source path but pins another commit: §10.3 calls
	// that "changed".
	rightItems[0].Ref = SourceRef{Type: RefTypeCommit, ID: "def456"}

	left := &Definition{
		FormatVersion: FormatVersion,
		Description:   "curated customer view",
		Metadata: map[string]any{
			"owner":     "data-platform",
			"tier":      "gold",
			"retention": "1y",
			"obsolete":  "yes",
		},
		Data: leftItems,
	}
	right := &Definition{
		FormatVersion: FormatVersion,
		Description:   "curated customer view, second cut",
		Metadata: map[string]any{
			"owner": "data-platform",
			"tier":  "silver",
			"fresh": "no",
		},
		Data: rightItems,
	}

	diff := DiffDefinitions(left, right)
	require.False(t, diff.Empty())

	require.Equal(t, &DescriptionDiff{
		From: "curated customer view",
		To:   "curated customer view, second cut",
	}, diff.Description)

	// Metadata entries are reported in key order, and an unchanged key is not reported.
	expectedMetadata := []MetadataDiff{
		{Key: "fresh", Change: ChangeAdded, To: anyPtr("no")},
		{Key: "obsolete", Change: ChangeRemoved, From: anyPtr("yes")},
		{Key: "retention", Change: ChangeRemoved, From: anyPtr("1y")},
		{Key: "tier", Change: ChangeChanged, From: anyPtr("gold"), To: anyPtr("silver")},
	}
	require.Equal(t, expectedMetadata, diff.Metadata)

	changedFrom, changedTo := leftItems[0], rightItems[0]
	removed := leftItems[1]
	added := rightItems[1]
	require.Equal(t, []ItemDiff{
		{Target: "customers", Change: ChangeChanged, From: &changedFrom, To: &changedTo},
		{Target: "reference/countries.csv", Change: ChangeRemoved, From: &removed},
		{Target: "reference/regions.csv", Change: ChangeAdded, To: &added},
	}, diff.Items, "items are matched by target and sorted by target")

	// The inputs were not edited by the diff (both sides may be shared with callers).
	require.Equal(t, "gold", left.Metadata["tier"])
	require.Equal(t, "abc123", left.Data[0].Ref.ID)

	t.Run("identical definitions change nothing", func(t *testing.T) {
		empty := DiffDefinitions(left, left)
		require.True(t, empty.Empty())
		require.Nil(t, empty.Description, "an unchanged description is omitted")
		require.Nil(t, empty.Metadata, "an unchanged metadata map is omitted")
		require.Empty(t, empty.Items)
		require.NotNil(t, empty.Items, "items is always present, empty rather than null")
	})

	t.Run("nil definitions are empty definitions", func(t *testing.T) {
		diff := DiffDefinitions(nil, &Definition{FormatVersion: FormatVersion})
		require.True(t, diff.Empty())
		require.Empty(t, diff.Items)
	})
}

// TestDiffDefinitionsOrdering is the determinism guarantee of §10.3: whatever order the two
// documents store their items and metadata in, the answer is sorted by target and then by
// metadata key, and repeated runs are identical.
func TestDiffDefinitionsOrdering(t *testing.T) {
	// Submitted in reverse target order, with metadata keys in map (random) order.
	left := &Definition{
		FormatVersion: FormatVersion,
		Metadata:      map[string]any{"zeta": "1", "alpha": "2", "mu": "3"},
		Data: []DataItem{
			objectItem("zeta/path", "z"),
			objectItem("mu/path", "m"),
			objectItem("alpha/path", "a"),
		},
	}
	right := &Definition{
		FormatVersion: FormatVersion,
		Metadata:      map[string]any{"zeta": "1x", "alpha": "2x", "omega": "4"},
		Data: []DataItem{
			objectItem("zeta/path2", "z2"),
			objectItem("beta/path", "b"),
			objectItem("alpha/path2", "a2"),
		},
	}

	diff := DiffDefinitions(left, right)

	targets := make([]string, 0, len(diff.Items))
	for _, item := range diff.Items {
		targets = append(targets, item.Target)
	}
	require.Equal(t, []string{
		"alpha/path", "alpha/path2", "beta/path", "mu/path", "zeta/path", "zeta/path2",
	}, targets, "every change is reported in target order")

	keys := make([]string, 0, len(diff.Metadata))
	for _, change := range diff.Metadata {
		keys = append(keys, change.Key)
	}
	require.Equal(t, []string{"alpha", "mu", "omega", "zeta"}, keys)

	// Same inputs, same answer - ten times. Items order and metadata order must not depend
	// on Go's randomised map iteration.
	for i := 0; i < 10; i++ {
		require.Equal(t, diff, DiffDefinitions(left, right), "run %d", i)
	}

	// Sorting is normalisation, not a diff artefact: two definitions that differ only in
	// item order are the same definition, so they produce no diff at all.
	reordered := &Definition{
		FormatVersion: FormatVersion,
		Metadata:      left.Metadata,
		Data:          []DataItem{left.Data[2], left.Data[0], left.Data[1]},
	}
	require.True(t, DiffDefinitions(left, reordered).Empty())
}

// TestDiffDefinitionsNormalisation checks that the diff compares canonical forms (§5.2), so
// a purely syntactic difference is not reported as a change.
func TestDiffDefinitionsNormalisation(t *testing.T) {
	stored := &Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{
			prefixItem("customers", "data/customers/"),
			objectItem("reference/countries.csv", "geo/countries.csv"),
		},
	}
	submitted := &Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{
			prefixItem("/customers/", "/data/customers"),
			objectItem("reference/countries.csv", "/geo/countries.csv"),
		},
	}
	require.True(t, DiffDefinitions(stored, submitted).Empty(),
		"trailing and leading slashes are normalised away before diffing")

	t.Run("metadata values compare as JSON", func(t *testing.T) {
		numbers := &Definition{FormatVersion: FormatVersion, Metadata: map[string]any{"count": 1}}
		decoded := &Definition{FormatVersion: FormatVersion, Metadata: map[string]any{"count": float64(1)}}
		require.True(t, DiffDefinitions(numbers, decoded).Empty(),
			"an int built in Go and the same number decoded from JSON are equal")

		multi := &Definition{FormatVersion: FormatVersion, Metadata: map[string]any{"tags": []string{"a", "b"}}}
		multiDecoded := &Definition{FormatVersion: FormatVersion, Metadata: map[string]any{"tags": []any{"a", "b"}}}
		require.True(t, DiffDefinitions(multi, multiDecoded).Empty(), "multi-value metadata compares by value")

		other := &Definition{FormatVersion: FormatVersion, Metadata: map[string]any{"count": 2}}
		diff := DiffDefinitions(numbers, other)
		require.Equal(t, []MetadataDiff{{
			Key:    "count",
			Change: ChangeChanged,
			From:   anyPtr(1),
			To:     anyPtr(2),
		}}, diff.Metadata, "values are reported as they are, never rewritten")
	})
}

// TestParseRefTarget covers the ref grammar of §10.3 / api/datasets.yml: `vN` (and the `N`
// and `VN` forms the spec pattern allows), `latest` and `proposal-<id>`.
func TestParseRefTarget(t *testing.T) {
	valid := []struct {
		ref      string
		expected refTarget
	}{
		{"latest", refTarget{latest: true}},
		{"v1", refTarget{version: 1}},
		{"v42", refTarget{version: 42}},
		{"V7", refTarget{version: 7}},
		{"3", refTarget{version: 3}},
		{"proposal-01J8Z6Q0K8", refTarget{proposalID: "01J8Z6Q0K8"}},
	}
	for _, tc := range valid {
		t.Run(tc.ref, func(t *testing.T) {
			target, err := parseRefTarget("left_ref", tc.ref)
			require.NoError(t, err)
			require.Equal(t, tc.expected, target)
		})
	}

	invalid := []string{"", "v0", "0", "v", "version1", "main", "proposal-", "proposal-a/b", "-1", "v-1", "v1.5"}
	for _, ref := range invalid {
		t.Run("invalid "+ref, func(t *testing.T) {
			_, err := parseRefTarget("left_ref", ref)
			var validation *ValidationError
			require.ErrorAs(t, err, &validation)
			require.Len(t, validation.Problems, 1)
			require.Equal(t, "left_ref", validation.Problems[0].Path)
		})
	}
}

// --- data diff on a real catalog (§15.1) ---

const (
	diffDataset = "customer360"
	diffSource  = "lakehouse-repo"
)

// diffFixture is a service fixture with a two-version dataset: the source repository has a
// first commit, the dataset v1 is published on it, the source gets a second commit (one
// object deleted, one rewritten, one added) and the dataset v2 pins that.
type diffFixture struct {
	*serviceFixture
	// v1Commit and v2Commit are commits of the *source* repository, the ones the two
	// definitions pin.
	v1Commit graveler.CommitID
	v2Commit graveler.CommitID
	// datasetV2Commit is the commit v2 materialised in the *dataset* repository, which is
	// where a proposal preview branch is forked from.
	datasetV2Commit string
	// v1 and v2 are the definitions as stored in the version records (resolved and
	// normalised), which is what a diff reads.
	v1, v2 Definition
}

func newDiffFixture(t *testing.T) *diffFixture {
	t.Helper()
	f := newServiceFixture(t)
	f.createRepository(t, diffSource, "main", false)

	// v1 of the source: two customer objects and one reference object.
	v1Commit := f.commitObjects(t, diffSource, "main",
		testObject{key: "data/customers/c1.parquet", content: []byte("c1-bytes")},
		testObject{key: "data/customers/c2.parquet", content: []byte("c2-v1-bytes")},
		testObject{key: "geo/countries.csv", content: []byte("countries-v1")},
	)

	// v2 of the source: c1 removed, c2 rewritten (a new address, so graveler sees a
	// changed entry), c4 and regions.csv added.
	require.NoError(t, f.cat.DeleteEntry(f.ctx, diffSource, "main", "data/customers/c1.parquet"))
	v2Commit := f.commitObjects(t, diffSource, "main",
		testObject{key: "data/customers/c2.parquet", content: []byte("c2-v2-bytes")},
		testObject{key: "data/customers/c4.parquet", content: []byte("c4-v2-bytes")},
		testObject{key: "geo/regions.csv", content: []byte("regions-v2")},
	)

	v1Def := Definition{
		FormatVersion: FormatVersion,
		Description:   "curated customer view",
		Metadata: map[string]any{
			"owner": "data-platform",
			"tier":  "gold",
		},
		Data: []DataItem{
			{
				Target: "customers", Repository: diffSource,
				Ref:  SourceRef{Type: RefTypeCommit, ID: v1Commit.String()},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/customers/"},
			},
			{
				Target: "reference/countries.csv", Repository: diffSource,
				Ref:  SourceRef{Type: RefTypeCommit, ID: v1Commit.String()},
				Type: ItemTypeObject, Object: &PathSource{Path: "geo/countries.csv"},
			},
		},
	}
	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       diffDataset,
		Definition: v1Def,
		Publish:    true,
		Message:    "v1",
	})
	require.NoError(t, err)
	require.Equal(t, 1, view.Version.Version)

	v2Def := Definition{
		FormatVersion: FormatVersion,
		Description:   "curated customer view v2",
		Metadata: map[string]any{
			"owner":     "data-platform",
			"tier":      "silver",
			"retention": "1y",
		},
		Data: []DataItem{
			{
				Target: "customers", Repository: diffSource,
				Ref:  SourceRef{Type: RefTypeCommit, ID: v2Commit.String()},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/customers/"},
			},
			{
				Target: "reference/regions.csv", Repository: diffSource,
				Ref:  SourceRef{Type: RefTypeCommit, ID: v2Commit.String()},
				Type: ItemTypeObject, Object: &PathSource{Path: "geo/regions.csv"},
			},
		},
	}
	v2 := f.publishNextVersion(t, diffDataset, v2Def, "v2")
	require.Equal(t, 2, v2.Version)

	return &diffFixture{
		serviceFixture:  f,
		v1Commit:        v1Commit,
		v2Commit:        v2Commit,
		datasetV2Commit: v2.CommitID,
		v1:              view.Version.Definition,
		v2:              v2.Definition,
	}
}

// publishNextVersion publishes def as the next version of name, performing the same steps
// the service performs for a create-with-publish (§6.2): validate, resolve, check sources,
// estimate stats, materialise, then advance the dataset record. Service.Publish is a later
// card; the diff tests need two versions now, so they drive the same internals.
func (f *serviceFixture) publishNextVersion(t *testing.T, name string, def Definition, message string) *VersionRecord {
	t.Helper()
	ctx := f.ctx
	previous, err := f.svc.store.GetDataset(ctx, name)
	require.NoError(t, err)
	require.NoError(t, f.svc.validateDefinition(ctx, &def))
	resolved, err := f.svc.resolver.Resolve(ctx, &def)
	require.NoError(t, err)
	require.NoError(t, f.svc.authorizer.CheckSources(ctx, f.caller(), resolved.Data))
	_, err = f.svc.sourceStats(ctx, resolved)
	require.NoError(t, err)

	version, err := f.svc.publish(ctx, previous, *resolved, message, testServiceUser, "")
	require.NoError(t, err)

	next := *previous
	next.LatestVersion = version.Version
	next.UpdatedAt = f.svc.now()
	require.NoError(t, f.svc.store.PutDatasetIf(ctx, &next, previous))
	return version
}

// TestServiceDiffDefinitionVersions diffs the two stored definitions of a dataset: a
// changed item, a removed one, an added one, a changed description and changed, added and
// removed metadata keys.
func TestServiceDiffDefinitionVersions(t *testing.T) {
	f := newDiffFixture(t)

	result, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
		Dataset:  diffDataset,
		LeftRef:  "v1",
		RightRef: "v2",
	})
	require.NoError(t, err)
	require.Equal(t, DiffTypeDefinition, result.Type, "the default type is definition (§10.1)")
	require.Nil(t, result.Data)

	diff := result.Definition
	require.Equal(t, &DescriptionDiff{From: f.v1.Description, To: f.v2.Description}, diff.Description)
	require.Equal(t, []MetadataDiff{
		{Key: "retention", Change: ChangeAdded, To: anyPtr("1y")},
		{Key: "tier", Change: ChangeChanged, From: anyPtr("gold"), To: anyPtr("silver")},
	}, diff.Metadata, "owner did not change, so it is not in the diff")

	fromCustomers, toCustomers := f.v1.Data[0], f.v2.Data[0]
	require.Equal(t, "customers", fromCustomers.Target)
	removedCountries := f.v1.Data[1]
	addedRegions := f.v2.Data[1]
	require.Equal(t, []ItemDiff{
		{Target: "customers", Change: ChangeChanged, From: &fromCustomers, To: &toCustomers},
		{Target: "reference/countries.csv", Change: ChangeRemoved, From: &removedCountries},
		{Target: "reference/regions.csv", Change: ChangeAdded, To: &addedRegions},
	}, diff.Items)
	require.Equal(t, f.v2Commit.String(), toCustomers.Ref.ID,
		"the changed item names the new commit")

	t.Run("explicit type", func(t *testing.T) {
		explicit, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: "v2",
			Type:     DiffTypeDefinition,
		})
		require.NoError(t, err)
		require.Equal(t, diff, explicit.Definition)
	})

	t.Run("same ref has no changes", func(t *testing.T) {
		same, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: "v1",
		})
		require.NoError(t, err)
		require.True(t, same.Definition.Empty())
	})

	t.Run("unknown diff type is a validation problem", func(t *testing.T) {
		_, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: "v2",
			Type:     "semantic",
		})
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "type", validation.Problems[0].Path)
	})

	t.Run("malformed ref names the request field", func(t *testing.T) {
		_, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "main",
			RightRef: "v2",
		})
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "left_ref", validation.Problems[0].Path)

		_, err = f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: "tail",
		})
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "right_ref", validation.Problems[0].Path)
	})
}

// TestServiceDiffDataVersions is the data diff of §10.3: the native graveler diff between
// the v1 and v2 tags of the dataset repository, with the _dataset/ definition entry
// filtered out.
func TestServiceDiffDataVersions(t *testing.T) {
	f := newDiffFixture(t)

	result, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
		Dataset:  diffDataset,
		LeftRef:  "v1",
		RightRef: "v2",
		Type:     DiffTypeData,
	})
	require.NoError(t, err)
	require.Equal(t, DiffTypeData, result.Type)
	require.Nil(t, result.Definition)

	expected := []DataDiffEntry{
		{Path: "customers/c1.parquet", Type: ChangeRemoved, SizeBytes: int64(len("c1-bytes"))},
		{Path: "customers/c2.parquet", Type: ChangeChanged, SizeBytes: int64(len("c2-v2-bytes"))},
		{Path: "customers/c4.parquet", Type: ChangeAdded, SizeBytes: int64(len("c4-v2-bytes"))},
		{Path: "reference/countries.csv", Type: ChangeRemoved, SizeBytes: int64(len("countries-v1"))},
		{Path: "reference/regions.csv", Type: ChangeAdded, SizeBytes: int64(len("regions-v2"))},
	}
	diff := result.Data
	require.Equal(t, expected, diff.Entries, "added, removed and changed paths with their sizes")
	require.False(t, diff.HasMore)
	require.Empty(t, diff.NextOffset)
	require.Equal(t, DefaultListAmount, diff.MaxPerPage)

	for _, entry := range diff.Entries {
		require.NotEqual(t, DefinitionPath, entry.Path, "the definition entry is bookkeeping, not data")
		require.False(t, isReservedTarget(entry.Path), "reserved _dataset/ paths are filtered out")
	}

	t.Run("latest is the branch head", func(t *testing.T) {
		latest, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: RefNameLatest,
			Type:     DiffTypeData,
		})
		require.NoError(t, err)
		require.Equal(t, expected, latest.Data.Entries)
	})

	t.Run("reverse direction swaps added and removed", func(t *testing.T) {
		reverse, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v2",
			RightRef: "v1",
			Type:     DiffTypeData,
		})
		require.NoError(t, err)
		reversed := make([]DataDiffEntry, 0, len(reverse.Data.Entries))
		for _, entry := range reverse.Data.Entries {
			reversed = append(reversed, DataDiffEntry{
				Path:      entry.Path,
				Type:      map[ChangeType]ChangeType{ChangeAdded: ChangeRemoved, ChangeRemoved: ChangeAdded, ChangeChanged: ChangeChanged}[entry.Type],
				SizeBytes: entry.SizeBytes,
			})
		}
		require.Equal(t, expected, sortedDataDiffEntries(reversed))
	})

	t.Run("prefix filter", func(t *testing.T) {
		prefix, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: "v2",
			Type:     DiffTypeData,
			Prefix:   "reference/",
		})
		require.NoError(t, err)
		require.Equal(t, []DataDiffEntry{
			{Path: "reference/countries.csv", Type: ChangeRemoved, SizeBytes: int64(len("countries-v1"))},
			{Path: "reference/regions.csv", Type: ChangeAdded, SizeBytes: int64(len("regions-v2"))},
		}, prefix.Data.Entries)
	})

	t.Run("same ref has no data changes", func(t *testing.T) {
		same, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v2",
			RightRef: "v2",
			Type:     DiffTypeData,
		})
		require.NoError(t, err)
		require.Empty(t, same.Data.Entries)
		require.False(t, same.Data.HasMore)
	})
}

// sortedDataDiffEntries sorts a page by path, so a reverse-direction assertion can be
// compared with the forward expectation.
func sortedDataDiffEntries(entries []DataDiffEntry) []DataDiffEntry {
	sorted := make([]DataDiffEntry, len(entries))
	copy(sorted, entries)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].Path < sorted[j-1].Path; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	return sorted
}

// TestServiceDiffDataPagination walks the whole data diff in pages of two and checks that
// pagination is path-based, stable and lossless even though each catalog page is filtered:
// the first page starts with entries that are not the first ones in the metarange.
func TestServiceDiffDataPagination(t *testing.T) {
	f := newDiffFixture(t)

	const amount = 2
	var (
		got      []DataDiffEntry
		after    string
		pages    int
		maxPaths = 10
	)
	for {
		result, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: "v2",
			Type:     DiffTypeData,
			After:    after,
			Amount:   amount,
		})
		require.NoError(t, err)
		pages++
		require.LessOrEqual(t, len(result.Data.Entries), amount, "page %d respects amount", pages)
		require.Equal(t, amount, result.Data.MaxPerPage)
		for _, entry := range result.Data.Entries {
			require.False(t, isReservedTarget(entry.Path), "page %d leaked a _dataset/ entry", pages)
		}
		got = append(got, result.Data.Entries...)

		if !result.Data.HasMore {
			require.Empty(t, result.Data.NextOffset)
			break
		}
		require.NotEmpty(t, result.Data.NextOffset, "a page with more results carries a cursor")
		after = result.Data.NextOffset
		require.Less(t, pages, maxPaths, "pagination did not terminate")
	}

	require.Equal(t, 3, pages, "five entries in pages of two")
	require.Equal(t, []DataDiffEntry{
		{Path: "customers/c1.parquet", Type: ChangeRemoved, SizeBytes: int64(len("c1-bytes"))},
		{Path: "customers/c2.parquet", Type: ChangeChanged, SizeBytes: int64(len("c2-v2-bytes"))},
		{Path: "customers/c4.parquet", Type: ChangeAdded, SizeBytes: int64(len("c4-v2-bytes"))},
		{Path: "reference/countries.csv", Type: ChangeRemoved, SizeBytes: int64(len("countries-v1"))},
		{Path: "reference/regions.csv", Type: ChangeAdded, SizeBytes: int64(len("regions-v2"))},
	}, got, "the pages concatenate to the unpaginated diff")

	t.Run("the _dataset/ entry does not consume a page slot", func(t *testing.T) {
		// The definition entry changes between v1 and v2 and sorts first, so an
		// unfiltered page of one would return it and nothing else. The first page of a
		// filtered diff of amount 1 must be real data.
		page, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: "v2",
			Type:     DiffTypeData,
			Amount:   1,
		})
		require.NoError(t, err)
		require.Len(t, page.Data.Entries, 1)
		require.Equal(t, "customers/c1.parquet", page.Data.Entries[0].Path)
		require.True(t, page.Data.HasMore)
	})

	t.Run("amount is capped", func(t *testing.T) {
		page, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
			Dataset:  diffDataset,
			LeftRef:  "v1",
			RightRef: "v2",
			Type:     DiffTypeData,
			Amount:   MaxListAmount * 2,
		})
		require.NoError(t, err)
		require.Equal(t, MaxListAmount, page.Data.MaxPerPage)
	})
}

// TestServiceResolveRef covers §10.3 ref resolution for every accepted form, including a
// proposal preview branch, and the 404/400 split for refs that name nothing.
func TestServiceResolveRef(t *testing.T) {
	f := newDiffFixture(t)

	t.Run("version", func(t *testing.T) {
		ref, err := f.svc.resolveRef(f.ctx, diffDataset, "ref", "v1")
		require.NoError(t, err)
		require.Equal(t, VersionTag(1), ref.Ref)
		require.Equal(t, 1, ref.Version)
		require.Empty(t, ref.ProposalID)
		require.Equal(t, "v1", ref.GravelerRef, "a version is diffed by its tag (§5.4)")
		require.Equal(t, f.v1, *ref.Definition)
	})

	t.Run("version accepts N and VN", func(t *testing.T) {
		for _, form := range []string{"2", "V2"} {
			ref, err := f.svc.resolveRef(f.ctx, diffDataset, "ref", form)
			require.NoError(t, err, "ref %q", form)
			require.Equal(t, 2, ref.Version)
			require.Equal(t, "v2", ref.GravelerRef)
			require.Equal(t, f.v2, *ref.Definition)
		}
	})

	t.Run("latest", func(t *testing.T) {
		ref, err := f.svc.resolveRef(f.ctx, diffDataset, "ref", RefNameLatest)
		require.NoError(t, err)
		require.Equal(t, RefNameLatest, ref.Ref, "the ref is reported as the client wrote it")
		require.Equal(t, 2, ref.Version)
		require.Equal(t, DefaultBranch, ref.GravelerRef, "latest is the dataset branch (§6.1)")
		require.Equal(t, f.v2, *ref.Definition)
	})

	t.Run("proposal preview branch", func(t *testing.T) {
		id := "01J8Z6Q0K8PROPOSAL000000"
		proposalDef := Definition{
			FormatVersion: FormatVersion,
			Description:   "proposal: bring countries back",
			Metadata: map[string]any{
				"owner":     "data-platform",
				"tier":      "platinum",
				"retention": "1y",
			},
			Data: []DataItem{
				{
					Target: "customers", Repository: diffSource,
					Ref:  SourceRef{Type: RefTypeCommit, ID: f.v2Commit.String()},
					Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/customers/"},
				},
				{
					Target: "reference/regions.csv", Repository: diffSource,
					Ref:  SourceRef{Type: RefTypeCommit, ID: f.v2Commit.String()},
					Type: ItemTypeObject, Object: &PathSource{Path: "geo/regions.csv"},
				},
				{
					Target: "reference/countries.csv", Repository: diffSource,
					Ref:  SourceRef{Type: RefTypeCommit, ID: f.v2Commit.String()},
					Type: ItemTypeObject, Object: &PathSource{Path: "geo/countries.csv"},
				},
			},
		}
		resolved := resolvedDefinition(t, f.datasetFixture, proposalDef)

		// §5.6: a proposal is materialised on a hidden preview branch, so reviewers can
		// browse the data it would publish and a data diff of it is a plain ref diff.
		previewBranch := ProposalPreviewBranch(id)
		_, err := f.cat.CreateBranch(f.ctx, diffDataset, previewBranch, f.datasetV2Commit, graveler.WithForce(true))
		require.NoError(t, err)
		previewResult := publish(t, f.datasetFixture, PublishRequest{
			Dataset:    diffDataset,
			Version:    3,
			Definition: resolved,
			Message:    "proposal preview",
			Publisher:  testServiceUser,
			Branch:     previewBranch,
		})
		require.Equal(t, previewBranch, ProposalPreviewBranch(id))

		require.NoError(t, f.svc.store.PutProposal(f.ctx, &ProposalRecord{
			ID:            id,
			Dataset:       diffDataset,
			BaseVersion:   2,
			Definition:    resolved,
			Title:         "bring countries back",
			Description:   "the countries reference is used again",
			Author:        testServiceUser,
			Status:        "open",
			CreatedAt:     f.svc.now(),
			PreviewBranch: previewBranch,
		}))

		ref, err := f.svc.resolveRef(f.ctx, diffDataset, "ref", ProposalRefPrefix+id)
		require.NoError(t, err)
		require.Equal(t, ProposalRefPrefix+id, ref.Ref)
		require.Equal(t, 0, ref.Version)
		require.Equal(t, id, ref.ProposalID)
		require.Equal(t, previewBranch, ref.GravelerRef)
		require.Equal(t, resolved, *ref.Definition)

		t.Run("definition diff against latest", func(t *testing.T) {
			result, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
				Dataset:  diffDataset,
				LeftRef:  RefNameLatest,
				RightRef: ProposalRefPrefix + id,
			})
			require.NoError(t, err)
			diff := result.Definition
			require.Equal(t, &DescriptionDiff{From: f.v2.Description, To: resolved.Description}, diff.Description)
			require.Equal(t, []MetadataDiff{
				{Key: "tier", Change: ChangeChanged, From: anyPtr("silver"), To: anyPtr("platinum")},
			}, diff.Metadata)
			require.Len(t, diff.Items, 1)
			require.Equal(t, ChangeAdded, diff.Items[0].Change)
			require.Equal(t, "reference/countries.csv", diff.Items[0].Target)
		})

		t.Run("data diff against latest", func(t *testing.T) {
			result, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
				Dataset:  diffDataset,
				LeftRef:  RefNameLatest,
				RightRef: ProposalRefPrefix + id,
				Type:     DiffTypeData,
			})
			require.NoError(t, err)
			require.Equal(t, []DataDiffEntry{{
				Path:      "reference/countries.csv",
				Type:      ChangeAdded,
				SizeBytes: int64(len("countries-v1")),
			}}, result.Data.Entries, "the preview branch adds exactly the countries object")
			require.False(t, result.Data.HasMore)
		})

		t.Run("the preview branch is what the commit was written to", func(t *testing.T) {
			head, err := f.cat.GetBranchReference(f.ctx, diffDataset, previewBranch)
			require.NoError(t, err)
			require.Equal(t, previewResult.CommitID.String(), head)
			latest, err := f.cat.GetBranchReference(f.ctx, diffDataset, DefaultBranch)
			require.NoError(t, err)
			require.NotEqual(t, latest, head, "the proposal did not move the latest branch")
		})
	})

	t.Run("unknown version", func(t *testing.T) {
		_, err := f.svc.resolveRef(f.ctx, diffDataset, "ref", "v99")
		require.ErrorIs(t, err, ErrRefNotFound)
	})

	t.Run("unknown proposal", func(t *testing.T) {
		_, err := f.svc.resolveRef(f.ctx, diffDataset, "ref", "proposal-01J8Z6Q0K8MISSING")
		require.ErrorIs(t, err, ErrRefNotFound)
	})

	t.Run("unknown dataset", func(t *testing.T) {
		_, err := f.svc.resolveRef(f.ctx, "no-such-dataset", "ref", "v1")
		require.ErrorIs(t, err, ErrDatasetNotFound)
	})

	t.Run("latest without a published version", func(t *testing.T) {
		commit := f.sourceRepo(t, "src-for-empty", map[string]string{"a": "a"})
		_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
			Name:       "empty-dataset",
			Definition: objectDefinition("src-for-empty", commit.String(), "a", "a"),
		})
		require.NoError(t, err)
		_, err = f.svc.resolveRef(f.ctx, "empty-dataset", "ref", RefNameLatest)
		require.ErrorIs(t, err, ErrRefNotFound)
	})
}

// TestServiceDiffLargeDefinition checks the DefinitionRef path of §5.3: a version record can
// carry only a pointer to the definition object, and a diff then reads the definition back
// from the dataset repository instead of comparing an empty definition.
func TestServiceDiffLargeDefinition(t *testing.T) {
	f := newDiffFixture(t)

	// A record whose definition was moved behind DefinitionRef (§5.3 large definitions),
	// pointing at the definition object of v2. Version 10 keeps it clear of v1/v2.
	source, err := f.svc.store.GetVersion(f.ctx, diffDataset, 2)
	require.NoError(t, err)
	require.True(t, source.HasInlineDefinition())

	stored, err := f.svc.store.PutVersion(f.ctx, VersionRecord{
		Dataset: diffDataset,
		Version: 10,
		DefinitionRef: &DefinitionRef{
			CommitID: source.CommitID,
			Path:     DefinitionPath,
		},
		Message:     "stored by reference",
		Publisher:   testServiceUser,
		PublishedAt: f.svc.now(),
		CommitID:    source.CommitID,
		MetaRangeID: source.MetaRangeID,
	})
	require.NoError(t, err)
	require.False(t, stored.HasInlineDefinition(), "the record points at the definition object")
	require.Equal(t, DefinitionPath, stored.DefinitionRef.Path)

	ref, err := f.svc.resolveRef(f.ctx, diffDataset, "ref", "v10")
	require.NoError(t, err)
	require.Equal(t, f.v2, *ref.Definition, "the definition was read back from the dataset repository")

	result, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
		Dataset:  diffDataset,
		LeftRef:  "v2",
		RightRef: "v10",
	})
	require.NoError(t, err)
	require.True(t, result.Definition.Empty(), "same definition, so no changes")
}

// TestServiceDiffAuthorization pins §9.1/§4.6: a diff is gated on the dataset, not on the
// repositories the dataset was curated from.
func TestServiceDiffAuthorization(t *testing.T) {
	f := newDiffFixture(t)

	// A principal with no datasets permission at all cannot diff the dataset, even though
	// the data itself is readable.
	stranger := "diff-stranger"
	f.addUser(t, stranger)
	_, err := f.svc.Diff(f.ctx, f.callerFor(stranger), DiffRequest{
		Dataset:  diffDataset,
		LeftRef:  "v1",
		RightRef: "v2",
	})
	var denied *PermissionError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, http.StatusForbidden, denied.HTTPStatusCode())

	// The dataset owner (DatasetAdmins + the ACL base grants) can diff without holding any
	// grant on the source repository's individual objects beyond the base read grant.
	result, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
		Dataset:  diffDataset,
		LeftRef:  "v1",
		RightRef: "v2",
		Type:     DiffTypeData,
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Data.Entries)
}

// TestDatasetsHTTPDiff covers the wire contract of §10.3: the DefinitionDiff and
// DataDiffList bodies, the `type` query parameter, and the 404/400 mappings.
func TestDatasetsHTTPDiff(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")
	rec := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "handler-diff", def, true), accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	t.Run("definition diff", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/handler-diff/refs/v1/diff/v1", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var diff apigenDefinitionDiff
		require.NoError(t, decodeJSONBody(t, rec.Body.Bytes(), &diff))
		require.Empty(t, diff.Items)
		require.Nil(t, diff.Description)
		require.Nil(t, diff.Metadata)
	})

	t.Run("definition diff of latest against a version", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/handler-diff/refs/latest/diff/v1", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("data diff", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/handler-diff/refs/v1/diff/latest?type=data", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var body dataDiffBody
		require.NoError(t, decodeJSONBody(t, rec.Body.Bytes(), &body))
		require.Empty(t, body.Results)
		require.False(t, body.Pagination.HasMore)
		require.Equal(t, DefaultListAmount, body.Pagination.MaxPerPage)
	})

	t.Run("unknown version is 404", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/handler-diff/refs/v1/diff/v9", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
		require.Contains(t, decodeError(t, rec).Message, "dataset ref not found")
	})

	t.Run("unknown dataset is 404", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/missing-diff/refs/v1/diff/v2", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("malformed ref is 400", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/handler-diff/refs/v0/diff/v1", "", accessKey, secret)
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, []string{"left_ref"}, pathsOf(decodeError(t, rec).Errors))
	})

	t.Run("unknown type is rejected by the validator", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/handler-diff/refs/v1/diff/v1?type=semantic", "", accessKey, secret)
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
	})
}

// apigenDefinitionDiff mirrors the DefinitionDiff schema of §10.2 closely enough to assert
// the wire shape, without pulling the generated types into the JSON decoding path.
type apigenDefinitionDiff struct {
	Description *struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"description,omitempty"`
	Metadata []struct {
		Key    string `json:"key"`
		Change string `json:"change"`
	} `json:"metadata,omitempty"`
	Items []struct {
		Target string `json:"target"`
		Change string `json:"change"`
	} `json:"items"`
}

// dataDiffBody mirrors the DataDiffList schema of §10.2.
type dataDiffBody struct {
	Results []struct {
		Path      string `json:"path"`
		Type      string `json:"type"`
		SizeBytes *int64 `json:"size_bytes,omitempty"`
	} `json:"results"`
	Pagination struct {
		HasMore    bool   `json:"has_more"`
		NextOffset string `json:"next_offset"`
		Results    int    `json:"results"`
		MaxPerPage int    `json:"max_per_page"`
	} `json:"pagination"`
}

// decodeJSONBody decodes a response body, reporting the raw body on failure.
func decodeJSONBody(t *testing.T, body []byte, out any) error {
	t.Helper()
	err := json.Unmarshal(body, out)
	if err != nil {
		t.Logf("body: %s", body)
	}
	return err
}
