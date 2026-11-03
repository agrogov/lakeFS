package cleanup

// refs.go resolves what the sweep must protect. It is the heart of the safety argument
// (doc.go "Safety argument"), and it is deliberately the most conservative code in the
// package: whenever a reference cannot be resolved, it protects more, never less.
//
// Four kinds of references are enumerated:
//
//  1. branch heads. Every branch's visible table pointers are resolved - committed and
//     staged alike - and every file their metadata references is protected. A branch head
//     is what a client reads, so losing a file it references is data loss by definition.
//
//  2. tags. A tag is immutable, so the commit it names is retained by lakeFS forever, and
//     so is every table pointer in it. Tags are resolved like branch heads, at the tag's
//     commit.
//
//  3. GC-retained commits. lakeFS GC keeps, per branch, the first-parent chain from the
//     branch head down to and including the first commit older than the branch's retention
//     period (pkg/graveler/retention/active_commits.go: the parent of a commit is added to
//     the active set when the commit itself is newer than the threshold). This file
//     reproduces exactly that walk from the API's commit log, so the commits protected
//     here are the commits lakeFS GC will not remove. When the repository has no GC rules,
//     lakeFS GC cannot run at all, so no commit is ever removed and every reachable commit
//     is protected (the safe answer, and the honest one: without GC rules, history is
//     retained forever).
//
//  4. staged changes. An uncommitted table pointer is a table state a client can already
//     read through the catalog, and an uncommitted file registration is a file the catalog
//     has already committed to protect. Both are resolved/protected.
//
// The result is a set of normalized physical addresses plus a set of table UUIDs whose live
// set could not be read. Both matter to the sweep: an address in the set must not be
// deleted, and *any* file of an unreadable table must not be deleted.

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// refSet is the resolved reference set of a repository.
type refSet struct {
	branches   []string
	tags       []Tag
	retained   []string
	allHistory bool
}

// protectedView is the set of physical files the sweep must not delete, with the reason
// each was protected, plus the tables whose live set is unknown.
type protectedView struct {
	// addresses maps a normalized physical address to the reason it is protected.
	addresses map[string]string

	// unknownTables holds the table UUIDs whose metadata could not be fully read. Every
	// registration of such a table is protected (ReasonUnknownTable).
	unknownTables map[uuid.UUID]string

	// unknownAll is set when a reference could not be enumerated at all. It protects every
	// candidate: a sweep with an incomplete reference set would be unsafe by construction.
	unknownAll string

	// collected deduplicates metadata documents: the same metadata location is collected
	// once, however many refs point at it.
	collected map[string]struct{}

	// tableStates counts the distinct table pointers resolved, and stagedBranches the
	// branches that had staged catalog changes.
	tableStates    int
	stagedBranches int
}

// newProtectedView returns an empty view.
func newProtectedView() *protectedView {
	return &protectedView{
		addresses:     make(map[string]string),
		unknownTables: make(map[uuid.UUID]string),
		collected:     make(map[string]struct{}),
	}
}

// add protects one physical address.
func (v *protectedView) add(address, reason string) {
	key := normalizeAddress(address)
	if key == "" {
		return
	}
	if _, exists := v.addresses[key]; !exists {
		v.addresses[key] = reason
	}
}

// protect reports whether an address is protected.
func (v *protectedView) protect(address string) (string, bool) {
	if v.unknownAll != "" {
		return ReasonUnknownTable, true
	}
	reason, ok := v.addresses[normalizeAddress(address)]
	return reason, ok
}

// protectTable reports whether every file of a table is protected because its live set is
// unknown.
func (v *protectedView) protectTable(tableUUID uuid.UUID) (string, bool) {
	reason, ok := v.unknownTables[tableUUID]
	return reason, ok
}

// computeProtected resolves every reference of the repository.
func (r *Runner) computeProtected(ctx context.Context) (*protectedView, *refSet, error) {
	refs, err := r.discoverRefs(ctx)
	if err != nil {
		return nil, nil, err
	}
	view := newProtectedView()
	for _, branch := range refs.branches {
		if err := r.collectTableStates(ctx, branch, ReasonReferencedByBranch, view); err != nil {
			return nil, nil, err
		}
		if err := r.collectStaged(ctx, branch, view); err != nil {
			return nil, nil, err
		}
	}
	for _, tag := range refs.tags {
		if err := r.collectTableStates(ctx, tag.CommitID, ReasonReferencedByTag, view); err != nil {
			return nil, nil, err
		}
	}
	for _, commit := range refs.retained {
		if err := r.collectTableStates(ctx, commit, ReasonReferencedByCommit, view); err != nil {
			return nil, nil, err
		}
	}
	return view, refs, nil
}

// discoverRefs enumerates the branches, tags and GC-retained commits of the repository.
func (r *Runner) discoverRefs(ctx context.Context) (*refSet, error) {
	branches, err := r.api.ListBranches(ctx, r.repo.Name)
	if err != nil {
		return nil, err
	}
	sort.Strings(branches)
	tags, err := r.api.ListTags(ctx, r.repo.Name)
	if err != nil {
		return nil, err
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i].Name < tags[j].Name })

	rules, err := r.api.GetGCRules(ctx, r.repo.Name)
	allHistory := false
	switch {
	case errors.Is(err, ErrGCRulesNotFound):
		// No rules means PrepareGarbageCollectionCommits cannot run, which means no
		// commit is ever collected. Treating history as retained is the only safe answer.
		allHistory = true
		rules = GCRules{}
	case err != nil:
		return nil, err
	}

	retained := make(map[string]struct{})
	var dangling []string
	for _, branch := range branches {
		days := rules.DefaultRetentionDays
		if branchDays, ok := rules.Branches[branch]; ok {
			days = branchDays
		}
		chain, extraRoots, err := r.retainedChain(ctx, branch, days, allHistory)
		if err != nil {
			return nil, err
		}
		for _, commit := range chain {
			retained[commit] = struct{}{}
		}
		dangling = append(dangling, extraRoots...)
	}
	for _, tag := range tags {
		// A tag's commit is a GC starting point without a branch: the GC walk gives it the
		// default retention period (pkg/graveler/retention/active_commits.go), so this walk
		// does too.
		chain, extraRoots, err := r.retainedChain(ctx, tag.CommitID, rules.DefaultRetentionDays, allHistory)
		if err != nil {
			return nil, err
		}
		for _, commit := range chain {
			retained[commit] = struct{}{}
		}
		dangling = append(dangling, extraRoots...)
	}

	// A non-first parent of a merge commit is a "dangling" commit: lakeFS GC treats every
	// commit that is nobody's first parent as a starting point of its own
	// (ref.GCCommitIterator), so the table state it carries is retained and must be
	// protected. Walking each such root with the default retention - the rule the GC walk
	// applies to a starting point that is not a branch - closes that gap. The queue is a
	// work list rather than recursion so a long chain of merges cannot exhaust the stack.
	visitedRoots := make(map[string]struct{}, len(dangling))
	for len(dangling) > 0 {
		root := dangling[0]
		dangling = dangling[1:]
		if _, seen := visitedRoots[root]; seen {
			continue
		}
		visitedRoots[root] = struct{}{}
		chain, extraRoots, err := r.retainedChain(ctx, root, rules.DefaultRetentionDays, allHistory)
		if err != nil {
			return nil, err
		}
		for _, commit := range chain {
			retained[commit] = struct{}{}
		}
		dangling = append(dangling, extraRoots...)
	}

	retainedCommits := make([]string, 0, len(retained))
	for commit := range retained {
		retainedCommits = append(retainedCommits, commit)
	}
	sort.Strings(retainedCommits)
	return &refSet{
		branches:   branches,
		tags:       tags,
		retained:   retainedCommits,
		allHistory: allHistory,
	}, nil
}

// initialCommitChain is the initial capacity of a retained commit chain. Retention periods
// are measured in days, so a branch has many commits; 8 is only the growth hint.
const initialCommitChain = 8

// retainedChain reproduces the lakeFS GC retention walk for one starting point (a branch
// head, a tag's commit or a dangling commit): the starting point is active, and a commit's
// parent is active while the commit itself is newer than the retention threshold. The walk
// therefore ends at (and includes) the first commit older than the threshold.
//
// The second result is the non-first parents of the commits the walk visited: each one is a
// merge side branch, and lakeFS GC retains such a commit as a starting point of its own
// (ref.GCCommitIterator keeps every commit that is nobody's first parent). The caller walks
// those roots as well, so a table a merge side branch still references is protected.
//
// allHistory walks the entire chain. It is used when the repository has no GC rules.
func (r *Runner) retainedChain(ctx context.Context, ref string, retentionDays int, allHistory bool) ([]string, []string, error) {
	threshold := r.now.AddDate(0, 0, -retentionDays)
	chain := make([]string, 0, initialCommitChain)
	var dangling []string
	after := ""
	head := true
	for {
		commits, hasMore, err := r.api.ListCommits(ctx, r.repo.Name, ref, after)
		if err != nil {
			return nil, nil, fmt.Errorf("walk the commit chain of %q: %w", ref, err)
		}
		stop := false
		for index := range commits {
			commit := commits[index]
			if len(commit.Parents) > 1 {
				dangling = append(dangling, commit.Parents[1:]...)
			}
			if head {
				// The starting point itself is always active: a branch head and a tag's
				// commit are never collected.
				chain = append(chain, commit.ID)
				head = false
				if !allHistory && !commit.CreationDate.After(threshold) {
					// An old head keeps only itself, exactly like the GC walk.
					stop = true
					break
				}
				continue
			}
			chain = append(chain, commit.ID)
			if !allHistory && !commit.CreationDate.After(threshold) {
				// The first commit at or below the threshold is included and ends the walk.
				stop = true
				break
			}
		}
		if stop || !hasMore || len(commits) == 0 {
			return chain, dangling, nil
		}
		after = commits[len(commits)-1].ID
	}
}

// collectTableStates resolves every table pointer visible at ref and protects every file
// its metadata references. The ref can be a branch (staged pointers included), a tag or a
// commit.
func (r *Runner) collectTableStates(ctx context.Context, ref, reason string, view *protectedView) error {
	pointers, err := r.listTablePointers(ctx, ref)
	if err != nil {
		return fmt.Errorf("list the table pointers at %q: %w", ref, err)
	}
	for _, entry := range pointers {
		pointer, err := r.pointerFor(ctx, ref, entry)
		if err != nil {
			// A pointer this tool cannot decode means the catalog state of the repository
			// cannot be read with confidence, so no file may be swept at all.
			view.unknownAll = fmt.Sprintf("a table pointer at %q is unreadable: %v", ref, err)
			return nil
		}
		if !inTableArea(r.repo.StorageNamespace, pointer.MetadataLocation) {
			// External metadata: its files are outside the storage namespace, so they are
			// never sweep candidates and need no protection.
			continue
		}
		tableUUID, err := uuid.Parse(pointer.TableUUID)
		if err != nil {
			view.unknownAll = fmt.Sprintf("a table pointer at %q carries UUID %q: %v", ref, pointer.TableUUID, err)
			return nil
		}
		view.tableStates++

		key := normalizeAddress(pointer.MetadataLocation)
		if _, done := view.collected[key]; done {
			continue
		}
		metadata, root, err := r.readTableMetadata(ctx, ref, tableUUID, pointer.MetadataLocation)
		if err != nil {
			// Unreadable metadata: protect everything the table registered.
			if _, exists := view.unknownTables[tableUUID]; !exists {
				view.unknownTables[tableUUID] = fmt.Sprintf("%s: %v", ReasonUnknownTable, err)
			}
			continue
		}
		if err := r.collectMetadataFiles(ctx, ref, tableUUID, root, pointer.MetadataLocation, metadata, reason, view); err != nil {
			if _, exists := view.unknownTables[tableUUID]; !exists {
				view.unknownTables[tableUUID] = fmt.Sprintf("%s: %v", ReasonUnknownTable, err)
			}
			continue
		}
		view.collected[key] = struct{}{}
	}
	return nil
}

// collectMetadataFiles protects every file of one metadata document.
func (r *Runner) collectMetadataFiles(ctx context.Context, ref string, tableUUID uuid.UUID, root, metadataLocation string, metadata table.Metadata, reason string, view *protectedView) error {
	return r.index.collect(ctx, ref, tableUUID, root, metadataLocation, metadata, func(address string) {
		view.add(address, reason)
	})
}

// collectStaged protects the staged changes of one branch: uncommitted table files
// (registrations the catalog staged for a snapshot that is not committed yet) and
// uncommitted table pointers (a table state a client can already read).
func (r *Runner) collectStaged(ctx context.Context, branch string, view *protectedView) error {
	changes, err := r.listStaged(ctx, branch)
	if err != nil {
		return fmt.Errorf("list the staged changes of %q: %w", branch, err)
	}
	if len(changes) == 0 {
		return nil
	}
	view.stagedBranches++
	for _, change := range changes {
		if change.Type == "removed" {
			// A staged removal is not a reference; the file is on its way out.
			continue
		}
		switch {
		case underPrefix(change.Path, filesPrefix):
			entry, err := r.api.StatObject(ctx, r.repo.Name, branch, change.Path)
			if err != nil {
				if isNotFound(err) {
					continue
				}
				return fmt.Errorf("stat the staged table file %q on %q: %w", change.Path, branch, err)
			}
			view.add(entry.PhysicalAddress, ReasonReferencedByStaged)
		case isPointerPath(change.Path):
			pointer, err := r.pointerFor(ctx, branch, ObjectInfo{Path: change.Path})
			if err != nil {
				view.unknownAll = fmt.Sprintf("a staged table pointer on %q is unreadable: %v", branch, err)
				return nil
			}
			if !inTableArea(r.repo.StorageNamespace, pointer.MetadataLocation) {
				continue
			}
			tableUUID, err := uuid.Parse(pointer.TableUUID)
			if err != nil {
				view.unknownAll = fmt.Sprintf("a staged table pointer on %q carries UUID %q: %v", branch, pointer.TableUUID, err)
				return nil
			}
			metadata, root, err := r.readTableMetadata(ctx, branch, tableUUID, pointer.MetadataLocation)
			if err != nil {
				if _, exists := view.unknownTables[tableUUID]; !exists {
					view.unknownTables[tableUUID] = fmt.Sprintf("%s: %v", ReasonUnknownTable, err)
				}
				continue
			}
			if err := r.collectMetadataFiles(ctx, branch, tableUUID, root, pointer.MetadataLocation, metadata, ReasonReferencedByStaged, view); err != nil {
				if _, exists := view.unknownTables[tableUUID]; !exists {
					view.unknownTables[tableUUID] = fmt.Sprintf("%s: %v", ReasonUnknownTable, err)
				}
			}
		}
	}
	return nil
}

// listStaged returns every staged change of a branch under the reserved catalog prefix.
func (r *Runner) listStaged(ctx context.Context, branch string) ([]DiffInfo, error) {
	var changes []DiffInfo
	after := ""
	for {
		page, hasMore, err := r.api.DiffBranch(ctx, r.repo.Name, branch, reservedPrefix, after)
		if err != nil {
			return nil, err
		}
		changes = append(changes, page...)
		if !hasMore {
			return changes, nil
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("%w: diff of %q: more results without a page", ErrBadPagination, branch)
		}
		after = page[len(page)-1].Path
	}
}
