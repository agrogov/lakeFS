package catalog

// A merge extension point for conflicts no strategy resolves.
//
// Merge is a three-way merge of entries: graveler compares the merge base, the destination branch
// and the source ref, and writes a new metarange. Two sides that both changed the same key are a
// conflict. graveler resolves those in order, through the ConflictResolvers the committed manager
// was built with (graveler/committed/merge.go, handleConflict):
//
//   - StrategyConflictResolver first, so strategy "dest-wins" / "source-wins" behaves exactly as
//     before and every other merge is untouched;
//   - then the resolver below, which consults the hook installed with SetMergeConflictResolver.
//     With no hook installed it answers "not resolved" for every key, which is what the upstream
//     behaviour is: the merge fails with graveler.ErrConflictFound.
//
// The hook exists for catalog formats whose conflicts are resolvable from the two values alone
// (the Iceberg catalog auto-resolves a table-pointer conflict caused only by a compaction, see
// pkg/icebergcatalog/commit.go and ICEBERG_CATALOG_PLAN.md §6.4). It is deliberately narrow: it is
// asked about one key at a time, and it may answer with one of the two sides' own values or with
// "unresolved" - never with a value of its own making. A merge that rewrites a value is a merge
// that can lose data silently, so that is refused here rather than trusted to the implementation.
//
// What the hook is told about the merge is what a resolver needs and nothing more: the repository,
// the key, the strategy, the two sides' values and the value at the merge base. The merge base is
// looked up lazily - the first time a conflict actually reaches the hook - so a merge that has no
// conflicts, or whose conflicts a strategy resolves, pays nothing for having a hook installed.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	stdatomic "sync/atomic"

	"github.com/treeverse/lakefs/pkg/graveler"
)

// MergeConflict is one key of a three-way conflict, as handed to the installed
// MergeConflictResolver: the key both sides changed, the value each side holds, and the value at
// the merge base.
type MergeConflict struct {
	// RepositoryID is the repository the merge runs on, which is what a resolver needs to read
	// further state (pkg/icebergcatalog reads metadata.json through it).
	RepositoryID string

	// Key is the conflicting entry key.
	Key graveler.Key

	// Strategy is the merge strategy the caller asked for. A strategy that resolves conflicts
	// has already run (StrategyConflictResolver) and the hook is therefore not called for it;
	// the field is here so a resolver can tell a plain merge from a forced one.
	Strategy graveler.MergeStrategy

	// Base is the merge base's value for Key, or nil when Key does not exist at the merge base -
	// which is the "both sides added the same key" conflict.
	Base *graveler.ValueRecord

	// Source is the value the merged-from ref holds.
	Source *graveler.ValueRecord

	// Destination is the value the destination branch holds.
	Destination *graveler.ValueRecord
}

// MergeConflictResolver resolves a merge conflict, or declines to. A nil result means "this is a
// real conflict": the merge reports graveler.ErrConflictFound exactly as it would with no resolver
// installed. A non-nil result must be the value of the source or of the destination side, unchanged
// (mergeConflictHookResolver verifies that); returning an error aborts the merge with that error.
//
// The hook must be safe for concurrent use: one process serves many merges at once, and the hook is
// shared by all of them.
type MergeConflictResolver func(ctx context.Context, conflict MergeConflict) (*graveler.ValueRecord, error)

// mergeConflictResolver holds the resolver installed by SetMergeConflictResolver. It is read on
// every unresolved conflict of every merge and written once at startup, so it is an atomic pointer
// rather than a plain variable - the same shape as the uncommitted-GC protector
// (gc_write_uncommitted.go).
var mergeConflictResolver stdatomic.Pointer[MergeConflictResolver]

// ErrMergeConflictResolution is wrapped when a resolver returns a value that is not one of the two
// sides of the conflict it was asked about. A merge may choose between the two sides; it may not
// write a third value, so the merge fails instead of trusting the resolver.
var ErrMergeConflictResolution = errors.New("merge conflict resolver returned a value that is not a side of the conflict")

// SetMergeConflictResolver installs resolver, or clears it when resolver is nil. It is called once
// at startup by the binary that owns a catalog with resolvable conflicts; a nil resolver keeps the
// upstream behaviour of reporting every unresolved conflict.
func SetMergeConflictResolver(resolver MergeConflictResolver) {
	if resolver == nil {
		mergeConflictResolver.Store(nil)
		return
	}
	mergeConflictResolver.Store(&resolver)
}

// mergeConflictScopeKey is the context key that carries the per-merge state a resolver cannot get
// from its arguments. The value is a *mergeConflictScope, put there by Catalog.Merge.
type mergeConflictScopeKey struct{}

// mergeConflictScope is what Catalog.Merge knows about a merge and a single conflicting key does
// not: the repository and the two refs, from which the merge base - and so the base value of any
// conflicting key - follows.
//
// The merge base is resolved at most once per merge, on the first conflict the hook is asked
// about, and only when a hook is installed: a merge with no conflicting pointer pays nothing.
type mergeConflictScope struct {
	catalog     *Catalog
	repository  *graveler.RepositoryRecord
	destination graveler.Ref
	source      graveler.Ref

	baseOnce   sync.Once
	baseCommit graveler.CommitID
	baseErr    error
}

func withMergeConflictScope(ctx context.Context, scope *mergeConflictScope) context.Context {
	return context.WithValue(ctx, mergeConflictScopeKey{}, scope)
}

func mergeConflictScopeFrom(ctx context.Context) *mergeConflictScope {
	scope, _ := ctx.Value(mergeConflictScopeKey{}).(*mergeConflictScope)
	return scope
}

// base returns the merge base's value for key, or nil when the key does not exist there. It fails
// when the merge base itself cannot be found: a merge that cannot read its own base must not have a
// conflict resolved against it.
func (s *mergeConflictScope) base(ctx context.Context, key graveler.Key) (*graveler.ValueRecord, error) {
	s.baseOnce.Do(func() {
		_, _, baseCommit, err := s.catalog.Store.FindMergeBase(ctx, s.repository, s.destination, s.source)
		if err != nil {
			s.baseErr = err
			return
		}
		// A graveler merge base is the commit itself; its address is its content address, the
		// same conversion Catalog.FindMergeBase makes.
		s.baseCommit = graveler.CommitID(s.catalog.addressProvider.ContentAddress(baseCommit))
	})
	if s.baseErr != nil {
		return nil, s.baseErr
	}
	value, err := s.catalog.Store.GetByCommitID(ctx, s.repository, s.baseCommit, key)
	switch {
	case errors.Is(err, graveler.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return &graveler.ValueRecord{Key: key, Value: value}, nil
}

// mergeConflictHookResolver adapts the installed MergeConflictResolver to graveler's
// ConflictResolver. It is a member of every committed manager's resolver list, so it is called for
// every conflict of every merge - and answers "not resolved" immediately when no hook is installed
// or when the context does not carry a merge scope (a merge that did not go through Catalog.Merge,
// e.g. graveler's own API or a test).
type mergeConflictHookResolver struct{}

// ResolveConflict implements graveler.ConflictResolver.
func (mergeConflictHookResolver) ResolveConflict(ctx context.Context, _ graveler.StorageContext, strategy graveler.MergeStrategy, sourceValue, destValue *graveler.ValueRecord) (*graveler.ValueRecord, error) {
	resolver := mergeConflictResolver.Load()
	scope := mergeConflictScopeFrom(ctx)
	if resolver == nil || scope == nil || sourceValue == nil || destValue == nil {
		return nil, nil
	}
	baseValue, err := scope.base(ctx, destValue.Key)
	if err != nil {
		return nil, fmt.Errorf("merge conflict on key %q: find merge base: %w", destValue.Key, err)
	}
	resolved, err := (*resolver)(ctx, MergeConflict{
		RepositoryID: scope.repository.RepositoryID.String(),
		Key:          destValue.Key,
		Strategy:     strategy,
		Base:         baseValue,
		Source:       sourceValue,
		Destination:  destValue,
	})
	if err != nil {
		return nil, fmt.Errorf("merge conflict on key %q: %w", destValue.Key, err)
	}
	if resolved == nil {
		return nil, nil
	}
	if !sameMergeSide(resolved, sourceValue) && !sameMergeSide(resolved, destValue) {
		return nil, fmt.Errorf("merge conflict on key %q: %w", destValue.Key, ErrMergeConflictResolution)
	}
	return resolved, nil
}

// sameMergeSide reports whether candidate is side: the same record, or a record with the same key
// and the same identity (the checksum that graveler compares, so an equal identity is the same
// value). It is the guard that keeps the hook from synthesizing a third value into a merge.
func sameMergeSide(candidate, side *graveler.ValueRecord) bool {
	if candidate == side {
		return true
	}
	if candidate == nil || side == nil || !bytes.Equal(candidate.Key, side.Key) {
		return false
	}
	if candidate.Value == nil || side.Value == nil {
		return candidate.Value == nil && side.Value == nil
	}
	return bytes.Equal(candidate.Identity, side.Identity)
}
