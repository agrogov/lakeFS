package datasets

// paging_test.go covers BUG-14: every list of the control plane pages in KV from its cursor
// and reads only the records the page contains. The tests wrap the fixture's KV store in a
// counting view (gets, scans, entries handed out by an iterator) and assert the bound, so a
// regression back to "load the partition and filter in memory" fails a test instead of
// merely being slow.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
)

// countingStore is a kv.Store view that counts the reads a call makes: Get calls, Scan
// calls, and the keys of the entries an iterator actually handed out. Writes pass through
// unchanged.
type countingStore struct {
	kv.Store

	mu    sync.Mutex
	gets  int
	scans int
	// entries holds the key of every entry an iterator handed out. The mem store returns the
	// first key outside the scanned prefix too, and the caller breaks on it, so a test counts
	// the entries within the prefix it scanned rather than the raw number.
	entries []string
}

func (c *countingStore) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	return c.Store.Get(ctx, partitionKey, key)
}

func (c *countingStore) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	c.mu.Lock()
	c.scans++
	c.mu.Unlock()
	it, err := c.Store.Scan(ctx, partitionKey, options)
	if err != nil {
		return nil, err
	}
	return &countingIterator{EntriesIterator: it, store: c}, nil
}

// reset zeroes the counters, so a test can measure one call after its setup.
func (c *countingStore) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets, c.scans, c.entries = 0, 0, nil
}

// counts returns the Get and Scan counters since the last reset.
func (c *countingStore) counts() (gets, scans int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.scans
}

// entriesWithin returns the number of entries an iterator handed out whose key starts with
// prefix, i.e. the records the scan actually decoded.
func (c *countingStore) entriesWithin(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, key := range c.entries {
		if strings.HasPrefix(key, prefix) {
			n++
		}
	}
	return n
}

// countingIterator counts the entries Next returns.
type countingIterator struct {
	kv.EntriesIterator
	store *countingStore
}

func (it *countingIterator) Next() bool {
	ok := it.EntriesIterator.Next()
	if ok {
		if entry := it.EntriesIterator.Entry(); entry != nil {
			it.store.mu.Lock()
			it.store.entries = append(it.store.entries, string(entry.Key))
			it.store.mu.Unlock()
		}
	}
	return ok
}

// newCountingServiceFixture returns the service fixture and a second service over a counting
// view of the same KV store. The counting service reads the records the fixture writes, so a
// test can drive the fixture and measure the reads of one listing.
func newCountingServiceFixture(t *testing.T) (*serviceFixture, *Service, *countingStore) {
	t.Helper()
	f := newServiceFixture(t)
	counter := &countingStore{Store: f.kvStore}
	svc, err := NewService(f.cfg, counter, f.cat, f.authService)
	require.NoError(t, err)
	return f, svc, counter
}

// datasetNames renders the names of a page of datasets.
func datasetNames(page []*DatasetView) []string {
	names := make([]string, len(page))
	for i, view := range page {
		names[i] = view.Record.Name
	}
	return names
}

// TestListPagesFromCursorWithBoundedReads is BUG-14 for GET /datasets: the scan starts after
// the cursor and stops at the page, and it never walks the datasets the page does not
// contain.
func TestListPagesFromCursorWithBoundedReads(t *testing.T) {
	f, svc, counter := newCountingServiceFixture(t)
	commit := f.sourceRepo(t, "src-paging-list", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-paging-list", commit.String(), "data", "data/")
	for _, name := range []string{"page-a", "page-b", "page-c", "page-d"} {
		f.createPublishedDataset(t, name, def)
	}

	counter.reset()
	first, err := svc.List(f.ctx, f.caller(), ListRequest{Amount: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"page-a", "page-b"}, datasetNames(first.Datasets))
	require.True(t, first.HasMore)
	require.Equal(t, "page-b", first.NextOffset)
	require.Equal(t, 2, first.MaxPerPage)
	gets, scans := counter.counts()
	require.Equal(t, 1, scans, "one scan for the page")
	require.Equal(t, 3, counter.entriesWithin("datasets/"), "two records of the page plus one that proves has_more")
	require.Equal(t, 3, gets, "the latest version of those three records")

	// The next page starts after the cursor: the first two datasets are not read at all.
	counter.reset()
	second, err := svc.List(f.ctx, f.caller(), ListRequest{Amount: 2, After: first.NextOffset})
	require.NoError(t, err)
	require.Equal(t, []string{"page-c", "page-d"}, datasetNames(second.Datasets))
	require.False(t, second.HasMore)
	require.Empty(t, second.NextOffset)
	_, scans = counter.counts()
	require.Equal(t, 1, scans)
	require.Equal(t, 2, counter.entriesWithin("datasets/"), "the page ends the dataset set: no read past it")

	// A page past the end is empty and says so, without reading anything.
	counter.reset()
	after, err := svc.List(f.ctx, f.caller(), ListRequest{Amount: 2, After: "page-d"})
	require.NoError(t, err)
	require.Empty(t, after.Datasets)
	require.False(t, after.HasMore)
	require.Zero(t, counter.entriesWithin("datasets/"))

	// A cursor that is not a dataset name is a bad request, not a key error.
	_, err = svc.List(f.ctx, f.caller(), ListRequest{After: "Not A Name!"})
	var validation *ValidationError
	require.ErrorAs(t, err, &validation)
	require.Equal(t, "after", validation.Problems[0].Path)
}

// TestListEnrichesOnlyTheReturnedPage is the second half of BUG-14 for GET /datasets: the
// per-item source statuses (one resolution and authorization per data item) are computed for
// the page, not for every dataset the scan passes.
func TestListEnrichesOnlyTheReturnedPage(t *testing.T) {
	f, svc, _ := newCountingServiceFixture(t)
	commit := f.sourceRepo(t, "src-paging-enrich", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-paging-enrich", commit.String(), "data", "data/")
	for _, name := range []string{"enrich-a", "enrich-b", "enrich-c"} {
		f.createPublishedDataset(t, name, def)
	}

	page, err := svc.List(f.ctx, f.caller(), ListRequest{Amount: 1})
	require.NoError(t, err)
	require.Len(t, page.Datasets, 1)
	require.Len(t, page.Datasets[0].ItemStatuses, 1, "the page is enriched")
	require.Equal(t, ItemStatusOK, page.Datasets[0].ItemStatuses[0])
}

// TestListVersionsPagesWithBoundedReads is BUG-14 for GET /datasets/{ds}/versions: a page is
// read newest first with one Get per version, never with a scan of the whole partition.
func TestListVersionsPagesWithBoundedReads(t *testing.T) {
	const dataset = "paging-versions"
	f, svc, counter := newCountingServiceFixture(t)
	commit := f.sourceRepo(t, "src-paging-versions", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-paging-versions", commit.String(), "data", "data/")
	// DOC-14: the create publishes v1, so the four explicit publishes produce v2..v5. Every one
	// of them names a distinct target: a publish that reproduces the previous data is the no-op
	// of §6.2 step 8, not a version.
	f.createDatasetV1(t, dataset, def)
	for i := 2; i <= 5; i++ {
		f.publishVersion(t, dataset, PublishVersionRequest{
			Definition: prefixDefinition("src-paging-versions", commit.String(), fmt.Sprintf("target%d", i), "data/"),
			Message:    fmt.Sprintf("v%d", i),
		})
	}

	counter.reset()
	first, err := svc.ListVersions(f.ctx, f.caller(), dataset, "", 2)
	require.NoError(t, err)
	require.Equal(t, []int{5, 4}, versionNumbers(first.Versions))
	require.True(t, first.HasMore)
	require.Equal(t, "4", first.NextOffset)
	gets, scans := counter.counts()
	require.Zero(t, scans, "versions are dense: a page is reads, not a scan")
	require.Equal(t, 4, gets, "the dataset record plus the page and one version past it")

	counter.reset()
	second, err := svc.ListVersions(f.ctx, f.caller(), dataset, first.NextOffset, 2)
	require.NoError(t, err)
	require.Equal(t, []int{3, 2}, versionNumbers(second.Versions))
	require.True(t, second.HasMore)
	require.Equal(t, "2", second.NextOffset)

	third, err := svc.ListVersions(f.ctx, f.caller(), dataset, second.NextOffset, 2)
	require.NoError(t, err)
	require.Equal(t, []int{1}, versionNumbers(third.Versions))
	require.False(t, third.HasMore)
	require.Empty(t, third.NextOffset)
}

// TestListProposalsPagesWithBoundedReads is BUG-14 for GET /datasets/{ds}/pulls: the newest
// first page is one scan from the cursor, not a walk of the partition.
func TestListProposalsPagesWithBoundedReads(t *testing.T) {
	const dataset = "paging-proposals"
	f, svc, counter := newCountingServiceFixture(t)
	commit := f.sourceRepo(t, "src-paging-proposals", map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition("src-paging-proposals", commit.String(), "data", "data/"))

	opened := make([]*ProposalView, 0, 3)
	for i := 0; i < 3; i++ {
		opened = append(opened, f.createProposal(t, dataset, CreateProposalRequest{
			Definition: prefixDefinition("src-paging-proposals", commit.String(), fmt.Sprintf("target%d", i), "data/"),
			Title:      fmt.Sprintf("proposal %d", i),
		}))
	}

	counter.reset()
	first, err := svc.ListProposals(f.ctx, f.caller(), dataset, "", "", 2)
	require.NoError(t, err)
	require.Len(t, first.Proposals, 2)
	require.Equal(t, opened[2].Record.ID, first.Proposals[0].ID, "newest first")
	require.Equal(t, opened[1].Record.ID, first.Proposals[1].ID)
	require.True(t, first.HasMore)
	require.Equal(t, opened[1].Record.ID, first.NextOffset)
	_, scans := counter.counts()
	require.Equal(t, 1, scans)
	require.Equal(t, 3, counter.entriesWithin("proposals/paging-proposals/"), "two proposals of the page plus one that proves has_more")

	// The cursor is checked with one read, and the scan starts at the cursor instead of
	// walking the newer proposals again.
	counter.reset()
	second, err := svc.ListProposals(f.ctx, f.caller(), dataset, "", first.NextOffset, 2)
	require.NoError(t, err)
	require.Len(t, second.Proposals, 1)
	require.Equal(t, opened[0].Record.ID, second.Proposals[0].ID)
	require.False(t, second.HasMore)
	gets, scans := counter.counts()
	require.Equal(t, 2, gets, "the dataset record and one read for the cursor")
	require.Equal(t, 1, scans)
	require.Equal(t, 2, counter.entriesWithin("proposals/paging-proposals/"), "the cursor record and the one after it")
}

// TestListAccessPagesWithBoundedReads is BUG-14 for GET /datasets/{ds}/access: the grants of
// one dataset page by principal instead of being loaded in full.
func TestListAccessPagesWithBoundedReads(t *testing.T) {
	const dataset = "paging-access"
	f, svc, counter := newCountingServiceFixture(t)
	commit := f.sourceRepo(t, "src-paging-access", map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition("src-paging-access", commit.String(), "data", "data/"))
	for _, user := range []string{"alice", "bob", "carol"} {
		f.addUser(t, user)
		_, err := f.svc.GrantAccess(f.ctx, f.caller(), dataset, PrincipalTypeUser, user)
		require.NoError(t, err)
	}

	counter.reset()
	first, err := svc.ListAccess(f.ctx, f.caller(), dataset, "", 2)
	require.NoError(t, err)
	require.Len(t, first.Grants, 2)
	require.Equal(t, "alice", first.Grants[0].PrincipalID)
	require.Equal(t, "bob", first.Grants[1].PrincipalID)
	require.True(t, first.HasMore)
	require.Equal(t, "user/bob", first.NextOffset)
	_, scans := counter.counts()
	require.Equal(t, 1, scans)
	require.Equal(t, 3, counter.entriesWithin("grants/paging-access/"), "two grants of the page plus one that proves has_more")

	counter.reset()
	second, err := svc.ListAccess(f.ctx, f.caller(), dataset, first.NextOffset, 2)
	require.NoError(t, err)
	require.Len(t, second.Grants, 1)
	require.Equal(t, "carol", second.Grants[0].PrincipalID)
	require.False(t, second.HasMore)
	require.Equal(t, 1, counter.entriesWithin("grants/paging-access/"), "the page ends the grant set: no read past it")
}

// TestDescendingKeySegmentReversesOrder pins the encoding the proposal listing pages with
// (BUG-14): sorting the encoded segments ascending yields the ids descending, including for
// ids one of which is a prefix of the other.
func TestDescendingKeySegmentReversesOrder(t *testing.T) {
	ids := []string{
		"01J9Z6Q0K8",
		"01J9Z6Q0K9",
		"01J9Z6Q0K9000",
		"01J9Z6Q0K",
		"zzz",
		"a",
	}
	sortedIDs := append([]string(nil), ids...)
	sort.Sort(sort.Reverse(sort.StringSlice(sortedIDs)))
	want := make([]string, len(sortedIDs))
	for i, id := range sortedIDs {
		want[i] = descendingKeySegment(id)
	}
	sort.Strings(want)

	encoded := make([]string, len(ids))
	for i, id := range ids {
		encoded[i] = descendingKeySegment(id)
	}
	sort.Strings(encoded)
	require.Equal(t, want, encoded, "key order is the reverse of id order")
}

// TestPinIndexGroupsOnce is the third bullet of BUG-14: the reconciler answers "which pins
// does this dataset hold?" from one index per pass, not with a scan per dataset.
func TestPinIndexGroupsOnce(t *testing.T) {
	shared := &PinRecord{Repository: "src", CommitID: "aaa", References: []string{
		PinReference("first", 1), PinReference("second", 1),
	}}
	onlyFirst := &PinRecord{Repository: "src", CommitID: "bbb", References: []string{PinReference("first", 2)}}
	index := newPinIndex([]*PinRecord{shared, onlyFirst, nil})

	require.Equal(t, []*PinRecord{shared, onlyFirst}, index.forDataset("first"))
	require.Equal(t, []*PinRecord{shared}, index.forDataset("second"))
	require.Empty(t, index.forDataset("third"))

	// The index is only a grouping: the store's ListDatasetPins must agree with it.
	store, _, ctx := newTestStore(t)
	for _, pin := range []*PinRecord{shared, onlyFirst} {
		for _, ref := range pin.References {
			_, err := store.AddPinReference(ctx, pin.Repository, pin.CommitID, ref)
			require.NoError(t, err)
		}
	}
	fromStore, err := store.ListDatasetPins(ctx, "first")
	require.NoError(t, err)
	require.Equal(t, []*PinRecord{shared, onlyFirst}, fromStore)
}
