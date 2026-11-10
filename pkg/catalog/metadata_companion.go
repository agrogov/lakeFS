package catalog

// Hidden Metadata Search companion repositories (METADATA_SEARCH_REALIGN.md G9, task RA6).
//
// The guide says: "Metadata repositories are hidden from repository listings, but remain reachable
// by name." The listing this file filters is the catalog's own repository listing
// (Catalog.ListRepositories), which is the source of both listings a user meets: the REST endpoint
// `/repositories` (lakectl, the Web UI and every API client) and the root level of the Iceberg
// catalog's namespace tree. A repository read by name does not go through it (GetRepository, and the
// catalog's own address resolution), so hiding a companion here leaves it reachable.
//
// # What "companion" means
//
// A companion is a repository the Metadata Search sync created - `R-metadata` for a data repository
// R - and not any repository whose name ends in `-metadata`: a user may create `foo-metadata`
// themselves, and that repository is the user's own. The suffix is therefore only a cheap
// pre-filter (a string comparison, no I/O) for the candidate set; the proof is the indexer's
// initialisation commit (METADATA_SEARCH_DESIGN.md §4.8), the same fact the catalog's table
// resolver requires before it serves a companion table:
//
//   - the repository's default branch carries an `mds: init` commit - message "mds: init" and
//     metadata lakefs.mds.init=true - positioned as the first commit directly above the
//     repository's root commit, or as that root commit itself for a companion whose branch starts
//     with the initialisation (the two shapes pkg/mds/indexer/repo.go accepts, and the ones
//     pkg/icebergcatalog/resolver.go serves).
//
// The committer is deliberately not part of the proof (METADATA_SEARCH_REALIGN.md X4): RA5 removed
// the committer check because the guide lets the sync run "as another user instead" with
// MDSCatalogAccess, so a check pinned to one committer name would not hide a legitimate companion.
// The cost of that reading is stated where it is paid: a repository named `-metadata` whose owner
// writes the marker and the message above the root is hidden too, exactly as the catalog would
// serve its table as a companion.
//
// The literals are restated here rather than imported: the indexer writes them
// (pkg/mds/indexer/repo.go MetadataKeyInit, pkg/mds/indexer/indexer.go InitCommitMessage) and the
// catalog serves them (pkg/icebergcatalog/resolver.go MetadataSearchInit*); pkg/catalog is imported
// by pkg/icebergcatalog, so neither direction of import exists. The design §4.8 pins the spellings.
//
// # The cost of the proof, and its cache
//
// The open check RA6 asks for: what does the proof cost per listed repository? For a repository
// whose name does not end in `-metadata` it costs nothing at all - the candidate is rejected by a
// string comparison and no lakeFS call is made. For a candidate it costs one first-parent history
// walk: the init commit sits at the bottom of the companion's history (directly above the root, or
// at the root), so the walk reads the history from the head down to the root, one page of
// metadataCompanionPageSize commits per request. The companion is by construction the repository
// with the longest history in the system - the indexer writes one commit per indexed source commit
// - so that walk is bounded by the length of the mirrored history, which is what makes a per-listing
// walk unacceptable.
//
// The answer is therefore cached per repository, the way the catalog's initialisation-commit cache
// is (pkg/icebergcatalog/resolver.go, metadataSearchInitCacheMaxRepositories): an entry is the head
// the answer was proved (or refuted) at, and the answer itself. A listing of an unchanged companion
// reads one page, finds the cached head and stops; a companion that gained commits since the last
// listing reads only the commits above the cached head, because reaching that head - a history
// already known to contain (or to lack) the init commit - is the same proof. The cache is bounded
// and evicts its oldest entry, so a process that lists many repositories does not grow without
// bound.
//
// A negative answer is cached too, and for the same reason: a user's own `foo-metadata` with a long
// history is walked once per head, not once per listing. It cannot go stale: the walk that produced
// it read the whole history down to the root, and a repository's history can only gain a different
// answer by changing the head (any rewrite - a hard reset, a new commit record - gives a new head
// commit ID, because commits are content-addressed), which invalidates the entry.
//
// A history that cannot be read at all (an empty repository has no branch to walk) proves nothing,
// and the repository is listed: hiding is a listing nicety, never worth failing the listing or
// making a repository of the user's own disappear.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// metadataRepositorySuffix is the suffix that names the companion repository of a data
	// repository R: R-metadata (METADATA_SEARCH_DESIGN.md §3.2). It is the one naming convention
	// the catalog, the indexer and this filter share; see pkg/icebergcatalog/resolver.go
	// MetadataRepositorySuffix for the catalog's copy of the same literal.
	metadataRepositorySuffix = "-metadata"

	// metadataSearchInitProperty, metadataSearchInitValue and metadataSearchInitCommitMessage are
	// the indexer's initialisation marker (METADATA_SEARCH_DESIGN.md §4.8), restated from
	// pkg/icebergcatalog/resolver.go MetadataSearchInitProperty/Value/CommitMessage.
	metadataSearchInitProperty      = "lakefs.mds.init"
	metadataSearchInitValue         = "true"
	metadataSearchInitCommitMessage = "mds: init"

	// metadataCompanionPageSize is the page size of the first-parent walk. It is the page size the
	// catalog's initialisation-commit proof uses (metadataSearchInitPageSize): the walk's cost is
	// the number of requests, so a page has to be large enough that a long history is a handful of
	// requests and small enough that one page is not a large response.
	metadataCompanionPageSize = 256

	// metadataCompanionCacheMaxRepositories bounds the cache. An entry is one repository whose
	// answer was proved (or refuted) at one head; the oldest entry is evicted when the cache is
	// full, so a process that lists many repositories does not grow without bound.
	metadataCompanionCacheMaxRepositories = 1024
)

// errMetadataCompanionWalk is returned when a candidate's commit log does not advance. It is a
// catalog-level inconsistency (a bug), never a property of the request, and it is reported as "not
// proved" - the repository is listed - like any other history that cannot be read.
var errMetadataCompanionWalk = errors.New("metadata companion history walk did not advance")

// metadataCompanionCommitLister is the commit-history read the proof needs: one page of the branch's
// first-parent log, newest first, starting after params.FromReference. *Catalog implements it, and
// a test can hand in a chain of commits instead.
type metadataCompanionCommitLister interface {
	ListCommits(ctx context.Context, repositoryID string, ref string, params LogParams) ([]*CommitLog, bool, error)
}

// IsMetadataCompanion reports whether repository is a Metadata Search companion repository: one the
// indexer created as R-metadata, not any repository whose name ends in "-metadata" (see the file
// comment). repository and defaultBranch are the ones the caller already holds, so the answer costs
// no repository read.
//
// It is exported because two callers outside this file need the same proof: the repository listing
// hides the companions it proves (isHiddenMetadataCompanion, below), and the repository deletion
// cascade deletes the companion it proves (REPO_DELETE_STORAGE_DESIGN.md section 6, R17). Sharing
// one function is what keeps those two from disagreeing - a repository that a listing hides must be
// exactly one that a deletion cascades into, never the user's own "foo-metadata".
//
// The error is the reason the history could not be read; the caller treats it as "not proved" (see
// the file comment).
func (c *Catalog) IsMetadataCompanion(ctx context.Context, repository, defaultBranch string) (bool, error) {
	return c.metadataCompanions.hidden(ctx, c, repository, defaultBranch)
}

// isHiddenMetadataCompanion reports whether one listed repository is a Metadata Search companion
// repository, which a repository listing leaves out (G9). It is the listing's name for
// IsMetadataCompanion, and deliberately only that name: two proofs that could disagree would hide
// one repository from a listing while a deletion cascaded into another.
func (c *Catalog) isHiddenMetadataCompanion(ctx context.Context, repository, defaultBranch string) (bool, error) {
	return c.IsMetadataCompanion(ctx, repository, defaultBranch)
}

// isMetadataCompanionName reports whether repository's name can name a companion at all. It is the
// cheap pre-filter that keeps the proof off every repository of a large installation, and it decides
// nothing: a name that passes it still has to carry the initialisation commit to be hidden.
func isMetadataCompanionName(repository string) bool {
	// The suffix alone is not a name: "-metadata" and "" are not repositories of a data
	// repository.
	return strings.HasSuffix(repository, metadataRepositorySuffix) &&
		len(repository) > len(metadataRepositorySuffix)
}

// metadataCompanionCache is the bounded per-repository answer cache described in the file comment.
// The zero value is usable and safe for concurrent listings; only the mutex-protected methods touch
// the map, and the history walk itself runs outside the lock.
type metadataCompanionCache struct {
	mu      sync.Mutex
	entries map[string]metadataCompanionEntry
	order   []string
}

// metadataCompanionEntry is one repository's answer at one head.
type metadataCompanionEntry struct {
	// head is the branch head the answer was proved (or refuted) at.
	head string

	// companion is the answer at that head.
	companion bool
}

// hidden is the whole per-repository decision: the name pre-filter and, for a candidate, the proof.
func (c *metadataCompanionCache) hidden(ctx context.Context, lister metadataCompanionCommitLister, repository, defaultBranch string) (bool, error) {
	if !isMetadataCompanionName(repository) {
		return false, nil
	}
	return c.isCompanion(ctx, lister, repository, defaultBranch)
}

// isCompanion walks (or reads from the cache) the first-parent history of repository's default
// branch and answers whether it carries the indexer's initialisation commit in one of the two
// accepted positions.
func (c *metadataCompanionCache) isCompanion(ctx context.Context, lister metadataCompanionCommitLister, repository, branch string) (bool, error) {
	cached, cachedOK := c.entry(repository)

	var (
		after    string
		head     string
		previous *CommitLog
	)
	for {
		page, hasMore, err := lister.ListCommits(ctx, repository, branch, LogParams{
			FirstParent:   true,
			Amount:        metadataCompanionPageSize,
			FromReference: after,
		})
		if err != nil {
			return false, fmt.Errorf("read the first-parent log of %q@%s: %w", repository, branch, err)
		}
		if len(page) == 0 {
			// A branch with no commits: nothing was initialised here.
			return false, nil
		}
		if head == "" {
			head = page[0].Reference
			if cachedOK && head == cached.head {
				// The branch has not moved since the answer was proved: no walk, no new commits.
				return cached.companion, nil
			}
		}
		for _, commit := range page {
			if len(commit.Parents) == 0 {
				// The root. A companion created bare has its initialisation commit at the root; a
				// companion created the ordinary way has it directly above, and no ancestor can
				// carry the marker (§4.8).
				switch {
				case isMetadataCompanionInitCommit(commit):
					c.remember(repository, head, true)
					return true, nil
				case previous != nil && len(previous.Parents) == 1 &&
					previous.Parents[0] == commit.Reference && isMetadataCompanionInitCommit(previous):
					c.remember(repository, head, true)
					return true, nil
				default:
					c.remember(repository, head, false)
					return false, nil
				}
			}
			if cachedOK && commit.Reference == cached.head {
				// An incremental walk reached the head a previous answer was proved (or refuted)
				// at: that history is the same one, so the answer for the new head is the same.
				c.remember(repository, head, cached.companion)
				return cached.companion, nil
			}
			previous = commit
		}
		if !hasMore {
			// A log that ends without a root: nothing provable either way, and nothing to cache.
			return false, nil
		}
		last := page[len(page)-1].Reference
		if last == "" || last == after {
			return false, fmt.Errorf("%w: %q@%s", errMetadataCompanionWalk, repository, branch)
		}
		after = last
	}
}

// isMetadataCompanionInitCommit reports whether commit carries the indexer's initialisation marker.
// Both the message and the metadata marker are required, so a commit that merely copied one of them
// is not mistaken for an initialisation; the committer is not read (X4).
func isMetadataCompanionInitCommit(commit *CommitLog) bool {
	return commit != nil &&
		commit.Message == metadataSearchInitCommitMessage &&
		commit.Metadata[metadataSearchInitProperty] == metadataSearchInitValue
}

// entry returns the cached answer for one repository.
func (c *metadataCompanionCache) entry(repository string) (metadataCompanionEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[repository]
	return entry, ok
}

// remember records one repository's answer at head, evicting the oldest entry when the cache is full.
func (c *metadataCompanionCache) remember(repository, head string, companion bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]metadataCompanionEntry)
	}
	if _, exists := c.entries[repository]; !exists {
		c.evictLocked()
		c.order = append(c.order, repository)
	}
	c.entries[repository] = metadataCompanionEntry{head: head, companion: companion}
}

// evictLocked drops the oldest entries until one more fits. Callers hold c.mu.
func (c *metadataCompanionCache) evictLocked() {
	for len(c.entries) >= metadataCompanionCacheMaxRepositories && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, oldest)
	}
}
