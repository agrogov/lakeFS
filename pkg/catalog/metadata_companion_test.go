package catalog

// Tests for the Metadata Search companion-repository filter of the repository listing
// (METADATA_SEARCH_REALIGN.md G9, task RA6; metadata_companion.go). They pin three things:
//
//   - what "companion" means: the indexer's initialisation commit in one of the two accepted
//     positions, never the `-metadata` suffix alone, so a repository of the user's own that ends in
//     "-metadata" is not hidden;
//   - the cost of the proof per listed repository - RA6's open check - measured on a listing of a
//     large number of repositories: nothing for a name that cannot be a companion, one bounded
//     history walk for a candidate, one page for a listing that changed nothing, and one page for a
//     candidate that gained commits;
//   - the cache bound: a listing of more repositories than the bound does not grow the cache.
//
// The package's own listing loop is exercised end to end in pkg/api (REST) and
// pkg/icebergcatalog (the catalog root); this file drives the per-repository decision those loops
// make, with a commit lister that counts its calls, so the cost model is an assertion and not a
// claim about wall-clock time.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeCompanionLister is a commit-log page source over hand-built histories, keyed by repository,
// newest commit first, the order Catalog.ListCommits answers in. calls is the observable cost: one
// call is one page of the first-parent log the proof read.
type fakeCompanionLister struct {
	histories map[string][]*CommitLog
	calls     int
}

// ListCommits implements metadataCompanionCommitLister.
func (f *fakeCompanionLister) ListCommits(_ context.Context, repositoryID, ref string, params LogParams) ([]*CommitLog, bool, error) {
	f.calls++
	history, ok := f.histories[repositoryID]
	if !ok {
		return nil, false, fmt.Errorf("no branch %q@%s", repositoryID, ref)
	}
	start := 0
	if params.FromReference != "" {
		start = len(history)
		for i, commit := range history {
			if commit.Reference == params.FromReference {
				start = i + 1
				break
			}
		}
	}
	start = min(start, len(history))
	amount := params.Amount
	if amount <= 0 {
		amount = len(history)
	}
	end := min(start+amount, len(history))
	return history[start:end], end < len(history), nil
}

// companionHistory builds a first-parent log of n commits, newest first, for one repository: the
// automatic "Repository created" root, then - when init is true - the indexer's initialisation commit
// directly above it, then ordinary indexing commits. With init false it is the history of a
// repository the user created and then committed to, which is ::what a user's own `foo-metadata`
// looks like.
func companionHistory(t *testing.T, n int, init bool) []*CommitLog {
	t.Helper()
	require.Greater(t, n, 1)
	history := make([]*CommitLog, 0, n)
	history = append(history, &CommitLog{
		Reference: "c000-root",
		Committer: "lakefs",
		Message:   "Repository created",
		Metadata:  Metadata{},
	})
	next := 1
	if init {
		history = append(history, initCommitLog("c001-init", "c000-root"))
		next = 2
	}
	for i := next; i < n; i++ {
		reference := fmt.Sprintf("commit-%04d", i)
		history = append(history, &CommitLog{
			Reference: reference,
			Committer: "mds-service-user",
			Message:   fmt.Sprintf("indexing %d", i),
			Metadata:  Metadata{},
			Parents:   []string{history[len(history)-1].Reference},
		})
	}
	// Reverse into the order the catalog's commit log answers: newest first.
	for i, j := 0, len(history)-1; i < j; i, j = i+1, j-1 {
		history[i], history[j] = history[j], history[i]
	}
	return history
}

// initCommitLog is the indexer's initialisation commit (§4.8) on top of parentID, or a branch's root
// commit when parentID is empty.
func initCommitLog(reference, parentID string) *CommitLog {
	commit := &CommitLog{
		Reference: reference,
		Committer: "mds-service-user",
		Message:   metadataSearchInitCommitMessage,
		Metadata:  Metadata{metadataSearchInitProperty: metadataSearchInitValue},
	}
	if parentID != "" {
		commit.Parents = []string{parentID}
	}
	return commit
}

// appendCommit adds one commit on top of history (which is newest first) and returns the new head.
func appendCommit(history []*CommitLog) ([]*CommitLog, string) {
	head := "commit-next"
	commit := &CommitLog{
		Reference: head,
		Committer: "mds-service-user",
		Message:   "indexing another source commit",
		Metadata:  Metadata{},
		Parents:   []string{history[0].Reference},
	}
	return append([]*CommitLog{commit}, history...), head
}

// TestMetadataCompanionProof pins what is and is not a companion: the marker alone, in one of the two
// positions the indexer writes it, with the message that goes with it.
func TestMetadataCompanionProof(t *testing.T) {
	tests := []struct {
		name      string
		history   []*CommitLog
		companion bool
	}{
		{
			name: "init commit directly above the automatic root",
			history: []*CommitLog{
				{Reference: "head", Parents: []string{"c001-init"}, Message: "indexing"},
				initCommitLog("c001-init", "c000-root"),
				{Reference: "c000-root", Message: "Repository created"},
			},
			companion: true,
		},
		{
			name: "init commit is the root of a branch that starts with it",
			history: []*CommitLog{
				{Reference: "head", Parents: []string{"c001-init"}, Message: "indexing"},
				initCommitLog("c001-init", ""),
			},
			companion: true,
		},
		{
			name: "the initialisation commit is the head itself",
			history: []*CommitLog{
				func() *CommitLog { c := initCommitLog("c001-init", "c000-root"); return c }(),
				{Reference: "c000-root", Message: "Repository created"},
			},
			companion: true,
		},
		{
			name: "a user's own -metadata repository, no marker",
			history: []*CommitLog{
				{Reference: "head", Parents: []string{"c000-root"}, Message: "my data"},
				{Reference: "c000-root", Message: "Repository created"},
			},
		},
		{
			name: "the marker two commits above the root is not the indexer's position",
			history: []*CommitLog{
				{Reference: "head", Parents: []string{"c001-init"}, Message: "indexing"},
				initCommitLog("c001-init", "middle"),
				{Reference: "middle", Parents: []string{"c000-root"}, Message: "something else"},
				{Reference: "c000-root", Message: "Repository created"},
			},
		},
		{
			name: "the message without the marker is not the indexer's commit",
			history: []*CommitLog{
				{Reference: "head", Parents: []string{"c001-init"}, Message: "indexing"},
				{
					Reference: "c001-init",
					Message:   metadataSearchInitCommitMessage,
					Parents:   []string{"c000-root"},
				},
				{Reference: "c000-root", Message: "Repository created"},
			},
		},
		{
			name: "the marker without the message is not the indexer's commit",
			history: []*CommitLog{
				{Reference: "head", Parents: []string{"c001-init"}, Message: "indexing"},
				{
					Reference: "c001-init",
					Message:   "something else",
					Metadata:  Metadata{metadataSearchInitProperty: metadataSearchInitValue},
					Parents:   []string{"c000-root"},
				},
				{Reference: "c000-root", Message: "Repository created"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := &metadataCompanionCache{}
			lister := &fakeCompanionLister{histories: map[string][]*CommitLog{"repo-metadata": tt.history}}
			companion, err := cache.hidden(t.Context(), lister, "repo-metadata", "main")
			require.NoError(t, err)
			require.Equal(t, tt.companion, companion)
		})
	}
}

// TestMetadataCompanionName pins the cheap pre-filter: only a name that can be a companion reaches
// the proof at all, and the proof itself decides.
func TestMetadataCompanionName(t *testing.T) {
	for _, tt := range []struct {
		name     string
		candidate bool
	}{
		{name: "orders-metadata", candidate: true},
		{name: "orders", candidate: false},
		{name: "-metadata", candidate: false},
		{name: "metadata", candidate: false},
		{name: "orders_metadata", candidate: false},
	} {
		require.Equalf(t, tt.candidate, isMetadataCompanionName(tt.name), "name %q", tt.name)
	}
}

// TestMetadataCompanionHiddenSkipsTheWalkForOtherNames pins that the listing pays nothing for the
// repositories that cannot be companions: no lakeFS call at all.
func TestMetadataCompanionHiddenSkipsTheWalkForOtherNames(t *testing.T) {
	cache := &metadataCompanionCache{}
	lister := &fakeCompanionLister{}
	for _, name := range []string{"orders", "orders_metadata", "metadata", "-metadata", ""} {
		companion, err := cache.hidden(t.Context(), lister, name, "main")
		require.NoErrorf(t, err, "name %q", name)
		require.Falsef(t, companion, "name %q", name)
	}
	require.Zero(t, lister.calls, "a name that cannot be a companion costs no history read")
	size, _ := companionCacheSize(t, cache)
	require.Zero(t, size, "and is not cached")
}

// TestMetadataCompanionProofCost is RA6's open check, measured: the cost of the proof per listed
// repository on a listing of a large number of repositories.
//
// The listing loop of Catalog.ListRepositories makes exactly this call per repository, so the count
// of commit-log pages it performs is the listing's cost:
//
//   - the 998 repositories whose names cannot be companions cost no call at all;
//   - the two `-metadata` candidates cost one history walk each on the first listing: 16 pages for a
//     history of 4096 commits at the 256-commit page size;
//   - a second listing of the same, unchanged repositories costs one page per candidate, and finds
//     the answer in the cache;
//   - a candidate that gained a commit costs one page again, because the page reaches the head the
//     answer was proved at.
func TestMetadataCompanionProofCost(t *testing.T) {
	const (
		listingSize = 1000
		historySize = 4096 // 16 pages at metadataCompanionPageSize
		pages       = historySize / metadataCompanionPageSize
	)
	cache := &metadataCompanionCache{}
	lister := &fakeCompanionLister{histories: map[string][]*CommitLog{}}

	// The repository list of one page of a large installation: 998 ordinary repositories and the two
	// repositories whose names end in "-metadata".
	const (
		companionRepo = "orders-metadata"
		userRepo      = "reports-metadata"
	)
	lister.histories[companionRepo] = companionHistory(t, historySize, true)
	lister.histories[userRepo] = companionHistory(t, historySize, false)
	repositories := make([]string, 0, listingSize)
	for i := 0; i < listingSize-2; i++ {
		repositories = append(repositories, fmt.Sprintf("repo-%04d", i))
	}
	repositories = append(repositories, companionRepo, userRepo)

	list := func(t *testing.T) (hidden []string, pages int) {
		t.Helper()
		lister.calls = 0
		for _, repository := range repositories {
			companion, err := cache.hidden(t.Context(), lister, repository, "main")
			require.NoErrorf(t, err, "repository %q", repository)
			if companion {
				hidden = append(hidden, repository)
			}
		}
		return hidden, lister.calls
	}

	hidden, cost := list(t)
	require.Equal(t, []string{companionRepo}, hidden,
		"the companion is hidden; the user's own -metadata repository is not")
	require.Equal(t, 2*pages, cost,
		"the first listing walks the two candidates to their roots and pays nothing for the rest")

	hidden, cost = list(t)
	require.Equal(t, []string{companionRepo}, hidden)
	require.Equal(t, 2, cost, "an unchanged listing reads one page per candidate and finds the answer cached")

	lister.histories[companionRepo], _ = appendCommit(lister.histories[companionRepo])
	lister.histories[userRepo], _ = appendCommit(lister.histories[userRepo])
	hidden, cost = list(t)
	require.Equal(t, []string{companionRepo}, hidden)
	require.Equal(t, 2, cost, "a candidate that gained commits reads only the commits above the proved head")

	// The negative answer is a cached fact too, and it stays right on a longer history of the same
	// branch: the walk reached the root and found no initialisation commit at the only two positions.
	_, entries := companionCacheSize(t, cache)
	require.Equal(t, metadataCompanionEntry{head: lister.histories[userRepo][0].Reference, companion: false},
		entries[userRepo])
}

// TestMetadataCompanionCacheBound pins the cache bound: listing more repositories than the bound
// leaves at most the bound in the cache, evicting the oldest entries.
func TestMetadataCompanionCacheBound(t *testing.T) {
	cache := &metadataCompanionCache{}
	lister := &fakeCompanionLister{histories: map[string][]*CommitLog{}}
	total := metadataCompanionCacheMaxRepositories + 17
	for i := 0; i < total; i++ {
		repository := fmt.Sprintf("repo-%04d-metadata", i)
		lister.histories[repository] = companionHistory(t, 3, true)
		companion, err := cache.hidden(t.Context(), lister, repository, "main")
		require.NoErrorf(t, err, "repository %q", repository)
		require.Truef(t, companion, "repository %q", repository)
	}
	size, entries := companionCacheSize(t, cache)
	require.LessOrEqual(t, size, metadataCompanionCacheMaxRepositories,
		"the cache does not grow without bound")
	require.Len(t, cache.order, size, "the eviction order holds exactly the cached repositories")
	require.NotContains(t, entries, "repo-0000-metadata", "the oldest entries were evicted")
	require.Contains(t, entries, fmt.Sprintf("repo-%04d-metadata", total-1),
		"the newest entry is cached")
}

// TestMetadataCompanionReadErrorListsTheRepository pins the direction of a history that cannot be
// read (an empty repository has no branch to walk): the listing shows the repository and the error is
// reported to the caller, which is what Catalog.ListRepositories swallows into "not hidden".
func TestMetadataCompanionReadErrorListsTheRepository(t *testing.T) {
	cache := &metadataCompanionCache{}
	lister := &fakeCompanionLister{histories: map[string][]*CommitLog{}}
	companion, err := cache.hidden(t.Context(), lister, "orders-metadata", "main")
	require.Error(t, err, "a history that cannot be read is reported, not guessed")
	require.False(t, companion)
	size, _ := companionCacheSize(t, cache)
	require.Zero(t, size, "an unreadable history is not cached")
}

// companionCacheSize reads the cache's size and entries under its lock. The listing paths that use
// them are covered by pkg/api and pkg/icebergcatalog; this is the in-package read the bound test
// needs.
func companionCacheSize(t *testing.T, c *metadataCompanionCache) (int, map[string]metadataCompanionEntry) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries), c.entries
}
