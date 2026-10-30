// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// pins.go holds the garbage-collection safety mechanism of design §8.2: one hidden pin branch
// per distinct (source repository, commit) pair referenced by a dataset version, and the
// reference-counted KV records that keep those branches alive. It also holds the degraded-source
// detection of §8.2/§12.3 step 4, which tells the reconciler that a source repository was deleted
// and the dataset can no longer be served in full.
//
// Why pin branches (and not tags): the GC job builds its starting points from branch heads plus
// dangling commits (retention.GCStartingPointIterator). A branch head is retained regardless of
// the retention rules; a tag is not a GC starting point at all. Spike S2 (spikes_pins_test.go)
// answers the two questions the design left open: a hidden pin branch can be created even when
// branch protection matches "*" (protection gates staging writes and commits, not branch
// creation), and auth.ArnMatch wildcard-matches the middle of an ARN resource, so the
// datasets-pins-protect deny policy of §8.2 (authz.go) works.
//
// Ordering rule (never under-retain, §14 "Node crashes mid-publish"): pins are acquired before the
// data commit is written (service: §6.2 step 5 runs before steps 7-10), and inside one pin the
// hidden branch is created before the KV reference is recorded. A crash can therefore leave a pin
// branch without a reference (over-retention, cleaned up by the reconciler, §12.3 item 2) but never
// a reference that claims protection while the branch is missing.
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS documentation only.
// No code, SQL, protobuf or UI asset from lakeFS Enterprise, and nothing from upstream lakeFS
// >= 1.87, was used.
package datasets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// PinBranchPrefix is the name prefix of a GC pin branch in a source repository (§8.2). The rest
// of the name is the pinned commit ID, which is what lets many datasets share one pin and keeps
// dataset names out of the source repository.
const PinBranchPrefix = "_lakefs_ds_pin_"

var (
	// ErrUnresolvedPinSource reports a definition that still carries a branch or tag ref. Only
	// resolved definitions are stored (§5.2), so pinning one that is not resolved would pin the
	// wrong thing; the service resolves refs before it calls the pin manager.
	ErrUnresolvedPinSource = errors.New("dataset source is not resolved to a commit")

	// ErrPinBranchConflict reports a branch named like a pin that points at a different commit
	// than its name says. It is not a pin: the branch was moved (or created by hand) and the
	// commit the datasets reference is no longer protected by it.
	ErrPinBranchConflict = errors.New("pin branch points at another commit")

	// ErrPinsUnavailable reports a Pins manager built without a store or without a catalog. Like
	// ErrNoAuthorizer it is a programming error, never a reason to skip pinning: skipping would
	// let the source repository's GC delete objects a dataset serves.
	ErrPinsUnavailable = errors.New("datasets: pin manager requires a store and a catalog")
)

// PinBranch returns the pin branch name of commitID, §8.2: _lakefs_ds_pin_<commit-id>.
//
// The commit ID is hex (§18 S2a), which the branch-ID rule (^\w[-\w]*$) accepts, so the branch
// name needs no escaping.
func PinBranch(commitID string) string {
	return PinBranchPrefix + commitID
}

// IsPinBranch reports whether name is a pin branch name (§8.2). It is what a listing filter or a
// reconciler guard needs: pin branches are service-owned and must never be committed to.
func IsPinBranch(name string) bool {
	return strings.HasPrefix(name, PinBranchPrefix) && len(name) > len(PinBranchPrefix)
}

// PinCommitID returns the commit ID a pin branch name refers to, and whether name is a pin branch
// name at all. It is the inverse of PinBranch.
func PinCommitID(name string) (string, bool) {
	if !IsPinBranch(name) {
		return "", false
	}
	return strings.TrimPrefix(name, PinBranchPrefix), true
}

// PinnedSource is one distinct (source repository, commit) pair referenced by a dataset version.
// It is the unit of GC protection of §8.2: one hidden branch and one pin record per pair, shared
// by every dataset version that references it.
type PinnedSource struct {
	// Repository is the source repository ID.
	Repository string
	// CommitID is the commit the data items of the version are pinned to.
	CommitID string
}

// String implements fmt.Stringer, rendering "<repo>@<commit>".
func (s PinnedSource) String() string {
	return s.Repository + "@" + s.CommitID
}

// PinSources returns the distinct sources of def, sorted by repository and then commit (§8.2: "for
// every distinct (source repo, commit) referenced by any dataset version"). A definition with ten
// items from the same commit yields one source; two items from the same repository at different
// commits yield two.
//
// Every item must carry a resolved ref (type "commit", §5.2). A definition that still has a
// branch or tag ref is rejected with ErrUnresolvedPinSource instead of silently pinning nothing:
// the caller resolves refs before it publishes, and failing closed is what keeps the data safe.
func PinSources(def Definition) ([]PinnedSource, error) {
	seen := make(map[PinnedSource]struct{}, len(def.Data))
	sources := make([]PinnedSource, 0, len(def.Data))
	for i, item := range def.Data {
		if item.Repository == "" {
			return nil, fmt.Errorf("%w: data[%d] %q has no repository", ErrUnresolvedPinSource, i, item.Target)
		}
		if item.Ref.Type != RefTypeCommit {
			return nil, fmt.Errorf("%w: data[%d] %q is pinned to %s %q, not to a commit",
				ErrUnresolvedPinSource, i, item.Target, item.Ref.Type, item.Ref.ID)
		}
		if item.Ref.ID == "" {
			return nil, fmt.Errorf("%w: data[%d] %q has an empty commit id", ErrUnresolvedPinSource, i, item.Target)
		}
		src := PinnedSource{Repository: item.Repository, CommitID: item.Ref.ID}
		if _, ok := seen[src]; ok {
			continue
		}
		seen[src] = struct{}{}
		sources = append(sources, src)
	}
	sortPinnedSources(sources)
	return sources, nil
}

// sortPinnedSources sorts sources by repository, then commit, so that every caller (and every test)
// sees the same order regardless of the order of the items in the definition.
func sortPinnedSources(sources []PinnedSource) {
	sort.SliceStable(sources, func(i, j int) bool {
		if sources[i].Repository != sources[j].Repository {
			return sources[i].Repository < sources[j].Repository
		}
		return sources[i].CommitID < sources[j].CommitID
	})
}

// PinCatalog is the catalog surface the pin manager needs: create and delete a branch, resolve a
// branch head, and look a repository up. *catalog.Catalog implements it directly.
type PinCatalog interface {
	CreateBranch(ctx context.Context, repositoryID string, branch string, sourceRef string, opts ...graveler.SetOptionsFunc) (*catalog.CommitLog, error)
	DeleteBranch(ctx context.Context, repositoryID string, branch string, opts ...graveler.SetOptionsFunc) error
	GetBranchReference(ctx context.Context, repositoryID string, branch string) (string, error)
	GetRepository(ctx context.Context, repository string) (*catalog.Repository, error)
}

// Pins is the GC pin manager of §8.2. It owns the pin branches of every source repository and the
// reference-counted pin records in the datasets KV partition.
//
// The zero Pins is unusable: build one with NewPins.
type Pins struct {
	store   *Store
	catalog PinCatalog
}

// NewPins returns a pin manager over the datasets KV store and the catalog. The manager does not
// own either.
func NewPins(store *Store, cat PinCatalog) *Pins {
	return &Pins{store: store, catalog: cat}
}

// available reports whether the manager is usable. Every public method checks it and fails closed
// with ErrPinsUnavailable: a pin manager that silently does nothing would let GC delete data.
func (p *Pins) available() error {
	if p == nil || p.store == nil || p.catalog == nil {
		return ErrPinsUnavailable
	}
	return nil
}

// Acquire pins every distinct source of def for ref and returns the sources it pinned. ref is the
// pin reference of the version being published, "<dataset>@v<n>" (PinReference).
//
// It must be called BEFORE the version's data is committed (§6.2 step 5): from the moment the
// dataset serves an object, the source repository's GC must already treat the pinned commit as a
// GC starting point, otherwise a GC run that overlaps a publish can collect data the new version
// points at (§14 "never under-retain").
//
// Acquiring is idempotent: a pin shared by several datasets keeps one branch and accumulates one
// reference per dataset version. A partial failure releases the references this call added (a
// branch that other references still need is kept); anything left behind only over-retains, which
// the reconciler cleans up (§12.3 item 2).
func (p *Pins) Acquire(ctx context.Context, def Definition, ref string) ([]PinnedSource, error) {
	if err := p.available(); err != nil {
		return nil, err
	}
	if err := checkSegment("pin reference", ref); err != nil {
		return nil, err
	}
	sources, err := PinSources(def)
	if err != nil {
		return nil, err
	}
	added := make([]PinnedSource, 0, len(sources))
	for _, src := range sources {
		fresh, err := p.pin(ctx, src, ref)
		if err != nil {
			// Roll back only the references this call added: a retried publish finds its own
			// reference already in place, and removing that would strip the protection of a version
			// record that already exists.
			if rollbackErr := p.releaseSources(ctx, added, ref); rollbackErr != nil {
				logging.FromContext(ctx).WithError(rollbackErr).
					WithField("dataset_reference", ref).
					Warn("datasets: could not roll back partly acquired pins; they over-retain until the reconciler runs")
			}
			return nil, err
		}
		if fresh {
			added = append(added, src)
		}
	}
	return sources, nil
}

// Release drops ref from every distinct source of def (§8.2: "the branch is deleted when the set
// becomes empty"). A pin that another dataset version still references keeps its branch.
//
// Releasing is what a dataset delete does per version (§6.6 step 3); releasing a reference that is
// not there is a no-op, so a retried delete converges. A source repository that has been deleted
// already is tolerated: its pins died with it, and a delete must not fail because the source is
// gone (§6.6 "Every step tolerates not found").
func (p *Pins) Release(ctx context.Context, def Definition, ref string) error {
	if err := p.available(); err != nil {
		return err
	}
	if err := checkSegment("pin reference", ref); err != nil {
		return err
	}
	sources, err := PinSources(def)
	if err != nil {
		return err
	}
	return p.releaseSources(ctx, sources, ref)
}

// ReleaseDataset releases every pin reference of dataset, across all of its versions (§6.6 step 3),
// and deletes each pin branch whose reference set becomes empty.
//
// It works from the pin records (pins/<repo>/<commit>, §5.3) rather than from the stored
// definitions, so it also releases pins of a version whose definition is only stored as a
// DefinitionRef, and it still finds them when a definition can no longer be read.
func (p *Pins) ReleaseDataset(ctx context.Context, dataset string) error {
	if err := p.available(); err != nil {
		return err
	}
	records, err := p.store.ListDatasetPins(ctx, dataset)
	if err != nil {
		return err
	}
	prefix := dataset + "@"
	var errs []error
	for _, rec := range records {
		refs := referencesWithPrefix(rec.References, prefix)
		// Release all of the dataset's references in one compare-and-swap, so a concurrent reader
		// never sees a half-released pin and a pin that also serves another dataset is not written
		// once per version.
		deleteBranch, err := p.store.RemovePinReferences(ctx, rec.Repository, rec.CommitID, refs)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if deleteBranch {
			if err := p.deletePinBranch(ctx, PinnedSource{Repository: rec.Repository, CommitID: rec.CommitID}); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// Ensure re-establishes the pins of def for ref: it re-creates a pin branch that was deleted by
// hand and re-adds ref to the pin's reference set (§12.3 item 2). It reports the sources whose
// branch it re-created and the sources whose repository no longer exists, so the caller can mark
// the dataset degraded (§12.3 step 4). Ensure is idempotent and is safe to run on every cycle.
func (p *Pins) Ensure(ctx context.Context, def Definition, ref string) (recreated, missing []PinnedSource, err error) {
	if err := p.available(); err != nil {
		return nil, nil, err
	}
	if err := checkSegment("pin reference", ref); err != nil {
		return nil, nil, err
	}
	sources, err := PinSources(def)
	if err != nil {
		return nil, nil, err
	}
	return p.ensureSources(ctx, sources, ref)
}

// EnsureDatasetPins is the reconciler-side form of Ensure (§12.3 item 2): it walks the pin records
// that reference dataset, re-creates every missing pin branch and reports the sources whose
// repository has been deleted. It needs no definition, so it also repairs pins of a version whose
// definition is stored as a DefinitionRef, and it is the hook a repair pass calls.
func (p *Pins) EnsureDatasetPins(ctx context.Context, dataset string) (recreated, missing []PinnedSource, err error) {
	if err := p.available(); err != nil {
		return nil, nil, err
	}
	records, err := p.store.ListDatasetPins(ctx, dataset)
	if err != nil {
		return nil, nil, err
	}
	return p.ensureSources(ctx, sourcesOfPins(records), "" /* references are already recorded */)
}

// SourceStatus is the reachability of one pinned source, as Status reports it.
type SourceStatus struct {
	// PinnedSource is the (repository, commit) pair that was inspected.
	PinnedSource PinnedSource
	// RepositoryExists is false when the source repository has been deleted (§8.2 risks).
	RepositoryExists bool
	// PinBranchExists reports whether the hidden pin branch is present.
	PinBranchExists bool
	// PinBranchCommit is the commit the pin branch points at. It differs from PinnedSource.CommitID
	// when the branch was moved, which makes the pin unhealthy and is reported as
	// ErrPinBranchConflict by the repair hook (Ensure).
	PinBranchCommit string
}

// Healthy reports whether the pin is intact: the repository exists and the pin branch is present
// and points at the pinned commit.
func (s SourceStatus) Healthy() bool {
	return s.RepositoryExists && s.PinBranchExists && s.PinBranchCommit == s.PinnedSource.CommitID
}

// Status inspects one pinned source: does the repository still exist, and is the pin branch still
// there and still pointing at the pinned commit?
//
// A missing repository is not an error - it is the degraded case of §8.2 that the reconciler
// reports - while a pin branch that exists but points elsewhere is returned as
// ErrPinBranchConflict by the repair hook (Ensure), because moving it back would be the only
// repair and Status itself never writes.
func (p *Pins) Status(ctx context.Context, src PinnedSource) (SourceStatus, error) {
	if err := p.available(); err != nil {
		return SourceStatus{PinnedSource: src}, err
	}
	status := SourceStatus{PinnedSource: src}
	if _, err := p.catalog.GetRepository(ctx, src.Repository); err != nil {
		// A repository that is being deleted is as unavailable as a deleted one: its pins are about
		// to go with it (§8.2 risks), so both are reported as "does not exist" rather than as an
		// error the reconciler would have to special-case.
		if errors.Is(err, graveler.ErrRepositoryNotFound) || errors.Is(err, graveler.ErrRepositoryInDeletion) {
			return status, nil
		}
		return status, fmt.Errorf("datasets: get source repository %s: %w", src.Repository, err)
	}
	status.RepositoryExists = true
	commitID, err := p.catalog.GetBranchReference(ctx, src.Repository, PinBranch(src.CommitID))
	switch {
	case errors.Is(err, graveler.ErrBranchNotFound):
		return status, nil
	case err != nil:
		return status, fmt.Errorf("datasets: get pin branch %s/%s: %w", src.Repository, PinBranch(src.CommitID), err)
	}
	status.PinBranchExists = true
	status.PinBranchCommit = commitID
	return status, nil
}

// DegradedSources returns the sources among sources whose repository has been deleted (§8.2
// risks). It is the pure check the reconciler uses to report a dataset it cannot serve fully.
func (p *Pins) DegradedSources(ctx context.Context, sources []PinnedSource) ([]PinnedSource, error) {
	if err := p.available(); err != nil {
		return nil, err
	}
	missing := make([]PinnedSource, 0)
	for _, src := range sources {
		status, err := p.Status(ctx, src)
		if err != nil {
			return missing, err
		}
		if !status.RepositoryExists {
			missing = append(missing, src)
		}
	}
	return missing, nil
}

// CheckVersionDegraded reports the sources of one dataset version whose repository has been
// deleted (§8.2 risks).
//
// It works from the version's pin references rather than from its definition, so it covers a
// version whose definition is stored as a DefinitionRef (§5.3) as well, and it never reads a
// source repository's data.
func (p *Pins) CheckVersionDegraded(ctx context.Context, dataset string, version int) ([]PinnedSource, error) {
	if err := p.available(); err != nil {
		return nil, err
	}
	ref := PinReference(dataset, version)
	records, err := p.store.ListDatasetPins(ctx, dataset)
	if err != nil {
		return nil, err
	}
	versionSources := make([]PinnedSource, 0, len(records))
	for _, rec := range records {
		if !containsReference(rec.References, ref) {
			continue
		}
		versionSources = append(versionSources, PinnedSource{Repository: rec.Repository, CommitID: rec.CommitID})
	}
	return p.DegradedSources(ctx, versionSources)
}

// CheckDatasetDegraded is the reconciler entry point of §12.3 step 4: it checks every pinned source
// of dataset against the catalog and, when at least one source repository has been deleted, marks
// the dataset record degraded (§10.2 Dataset.state "degraded"). It reports the missing sources and
// whether the record was written.
//
// Only an "active" record is marked: a transient state (creating/publishing/deleting) belongs to the
// reconciler's first step (§12.3 item 1), which resumes or rolls the operation back before the
// degraded check makes sense. Reads of a degraded dataset keep working until somebody removes the
// source storage namespace (§8.2), which is why the state is a warning and not a tombstone.
func (p *Pins) CheckDatasetDegraded(ctx context.Context, dataset string) (missing []PinnedSource, marked bool, err error) {
	if err := p.available(); err != nil {
		return nil, false, err
	}
	records, err := p.store.ListDatasetPins(ctx, dataset)
	if err != nil {
		return nil, false, err
	}
	missing, err = p.DegradedSources(ctx, sourcesOfPins(records))
	if err != nil {
		return missing, false, err
	}
	if len(missing) == 0 {
		return nil, false, nil
	}
	marked, err = p.markDegraded(ctx, dataset)
	return missing, marked, err
}

// markDegraded compare-and-swaps the dataset record to the degraded state (§5.4 CAS discipline).
// Losing the race is retried a bounded number of times, like the pin reference updates: the
// reconciler will come back anyway, but a lost update here would hide the warning for a whole
// reconcile interval.
func (p *Pins) markDegraded(ctx context.Context, dataset string) (bool, error) {
	for attempt := 0; attempt < pinCASRetries; attempt++ {
		rec, err := p.store.GetDataset(ctx, dataset)
		if errors.Is(err, kv.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if rec.State != DatasetStateActive {
			if rec.State == DatasetStateDegraded {
				return false, nil // already marked
			}
			return false, nil // transient state: §12.3 item 1 owns it
		}
		next := *rec
		next.State = DatasetStateDegraded
		next.UpdatedAt = time.Now().UTC()
		err = p.store.PutDatasetIf(ctx, &next, rec)
		if errors.Is(err, kv.ErrPredicateFailed) {
			continue // the record changed under us; re-read and retry
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, kv.ErrPredicateFailed
}

// pin acquires one source: the hidden branch first, then the reference. It reports whether the
// reference is new, so Acquire can roll back exactly what it added.
//
// The "was it already there" read is safe against concurrent publishers because the §5.4 CAS lets
// only one publish of a given dataset version run at a time, and the reconciler only re-adds
// references that are already recorded.
func (p *Pins) pin(ctx context.Context, src PinnedSource, ref string) (bool, error) {
	if _, err := p.ensureBranch(ctx, src); err != nil {
		return false, err
	}
	current, err := p.store.GetPin(ctx, src.Repository, src.CommitID)
	switch {
	case err == nil && containsReference(current.References, ref):
		return false, nil // already protected by an earlier (possibly interrupted) attempt
	case err != nil && !errors.Is(err, kv.ErrNotFound):
		return false, fmt.Errorf("datasets: read pin %s: %w", src, err)
	}
	if _, err := p.store.AddPinReference(ctx, src.Repository, src.CommitID, ref); err != nil {
		return false, fmt.Errorf("datasets: reference %s in pin %s: %w", ref, src, err)
	}
	return true, nil
}

// ensureSources re-establishes a list of pins, collecting the sources it re-created and the sources
// whose repository is gone. A non-empty ref is added to each pin's reference set.
func (p *Pins) ensureSources(ctx context.Context, sources []PinnedSource, ref string) (recreated, missing []PinnedSource, err error) {
	for _, src := range sources {
		created, err := p.ensureBranch(ctx, src)
		switch {
		case errors.Is(err, graveler.ErrRepositoryNotFound), errors.Is(err, graveler.ErrRepositoryInDeletion):
			// The source repository was deleted (or is being deleted, §8.2 risks): the reconciler
			// reports it instead of repairing it, and marking the dataset degraded is the caller's
			// next step.
			missing = append(missing, src)
			continue
		case err != nil:
			return recreated, missing, err
		}
		if created {
			recreated = append(recreated, src)
			logging.FromContext(ctx).WithFields(logging.Fields{
				"repository": src.Repository,
				"commit_id":  src.CommitID,
				"pin_branch": PinBranch(src.CommitID),
			}).Warn("datasets: re-created a missing GC pin branch")
		}
		if ref == "" {
			continue
		}
		if _, err := p.store.AddPinReference(ctx, src.Repository, src.CommitID, ref); err != nil {
			return recreated, missing, fmt.Errorf("datasets: reference %s in pin %s: %w", ref, src, err)
		}
	}
	return recreated, missing, nil
}

// ensureBranch creates the hidden pin branch of src if it does not exist yet, and reports whether it
// created it. A pin branch that exists but points at another commit is an error
// (ErrPinBranchConflict): the pin is not doing its job, and silently accepting it would let GC
// collect the pinned commit.
func (p *Pins) ensureBranch(ctx context.Context, src PinnedSource) (bool, error) {
	branch := PinBranch(src.CommitID)
	// Hidden: pin branches must not show up in the UI or in lakectl (§8.2). Force: §6.2 - the
	// service writes even to read-only, protected source repositories, and branch protection does
	// not gate branch creation at all (spike S2a).
	_, err := p.catalog.CreateBranch(ctx, src.Repository, branch, src.CommitID,
		graveler.WithHidden(true), graveler.WithForce(true))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, graveler.ErrBranchExists):
		return false, p.verifyPinBranch(ctx, src)
	default:
		return false, fmt.Errorf("datasets: create pin branch %s/%s: %w", src.Repository, branch, err)
	}
}

// verifyPinBranch checks that an existing pin branch still points at the pinned commit.
func (p *Pins) verifyPinBranch(ctx context.Context, src PinnedSource) error {
	branch := PinBranch(src.CommitID)
	commitID, err := p.catalog.GetBranchReference(ctx, src.Repository, branch)
	switch {
	case errors.Is(err, graveler.ErrBranchNotFound):
		// The branch disappeared between the create attempt and this read (a concurrent delete).
		// The reference set still says the commit must be protected, and the next Ensure recreates
		// the branch, so this is not a conflict.
		return nil
	case err != nil:
		return fmt.Errorf("datasets: get pin branch %s/%s: %w", src.Repository, branch, err)
	case commitID != src.CommitID:
		return fmt.Errorf("%w: %s/%s points at %s, expected %s",
			ErrPinBranchConflict, src.Repository, branch, commitID, src.CommitID)
	}
	return nil
}

// releaseSources removes ref from each pin in sources, deleting the pin branch whenever the
// reference set becomes empty (§8.2).
func (p *Pins) releaseSources(ctx context.Context, sources []PinnedSource, ref string) error {
	var errs []error
	for _, src := range sources {
		if err := p.releaseSource(ctx, src, ref); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// releaseSource removes one reference and deletes the branch when the pin becomes an orphan.
func (p *Pins) releaseSource(ctx context.Context, src PinnedSource, ref string) error {
	orphaned, err := p.store.RemovePinReference(ctx, src.Repository, src.CommitID, ref)
	if err != nil {
		return fmt.Errorf("datasets: release %s from pin %s: %w", ref, src, err)
	}
	if !orphaned {
		return nil
	}
	return p.deletePinBranch(ctx, src)
}

// deletePinBranch removes a pin branch whose reference set is empty. A branch or repository that is
// already gone is not an error: deletes and release must converge (§6.6).
func (p *Pins) deletePinBranch(ctx context.Context, src PinnedSource) error {
	err := p.catalog.DeleteBranch(ctx, src.Repository, PinBranch(src.CommitID), graveler.WithForce(true))
	switch {
	case err == nil:
		logging.FromContext(ctx).WithFields(logging.Fields{
			"repository": src.Repository,
			"commit_id":  src.CommitID,
			"pin_branch": PinBranch(src.CommitID),
		}).Info("datasets: deleted orphaned GC pin branch")
		return nil
	case errors.Is(err, graveler.ErrBranchNotFound), errors.Is(err, graveler.ErrRepositoryNotFound),
		errors.Is(err, graveler.ErrRepositoryInDeletion):
		return nil
	default:
		return fmt.Errorf("datasets: delete pin branch %s/%s: %w", src.Repository, PinBranch(src.CommitID), err)
	}
}

// sourcesOfPins maps pin records to the sources they protect, sorted and deduplicated.
func sourcesOfPins(records []*PinRecord) []PinnedSource {
	seen := make(map[PinnedSource]struct{}, len(records))
	sources := make([]PinnedSource, 0, len(records))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		src := PinnedSource{Repository: rec.Repository, CommitID: rec.CommitID}
		if _, ok := seen[src]; ok {
			continue
		}
		seen[src] = struct{}{}
		sources = append(sources, src)
	}
	sortPinnedSources(sources)
	return sources
}

// referencesWithPrefix returns the references of refs that start with prefix, i.e. the versions of
// one dataset.
func referencesWithPrefix(refs []string, prefix string) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if strings.HasPrefix(ref, prefix) {
			out = append(out, ref)
		}
	}
	return out
}

// ----------------------------------------------------------------------------
// Pin records of one dataset (KV side of §8.2)
// ----------------------------------------------------------------------------

// ListDatasetPins returns every pin record that carries at least one reference of dataset, in key
// order (pins/<repo>/<commit>, §5.3).
//
// A pin is keyed by the source (repository, commit), not by the dataset, so answering "which pins
// does this dataset hold?" means scanning the pin prefix. That is what a dataset delete (§6.6 step
// 3) and the degraded check (§12.3 step 4) need, and they run rarely; a per-dataset index would be a
// second source of truth for the same fact.
func (s *Store) ListDatasetPins(ctx context.Context, dataset string) ([]*PinRecord, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	pins, err := s.listAllPins(ctx)
	if err != nil {
		return nil, err
	}
	prefix := dataset + "@" // PinReference(dataset, n), and "@" cannot continue a dataset name
	mine := make([]*PinRecord, 0, len(pins))
	for _, pin := range pins {
		if len(referencesWithPrefix(pin.References, prefix)) == 0 {
			continue
		}
		mine = append(mine, pin)
	}
	return mine, nil
}

// listAllPins returns every pin record of the partition, in key order.
func (s *Store) listAllPins(ctx context.Context) ([]*PinRecord, error) {
	pins := make([]*PinRecord, 0)
	prefix := []byte(pinKeyPrefix + kv.PathDelimiter)
	err := s.scanPrefix(ctx, prefix, func(key, value []byte) error {
		rec, err := decodeRecord[PinRecord]("pin record", key, value)
		if err != nil {
			return err
		}
		pins = append(pins, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pins, nil
}

// RemovePinReferences removes several references from one pin in a single compare-and-swap and
// reports whether the pin became an orphan, i.e. whether the caller must delete the pin branch.
//
// It is the bulk form of RemovePinReference, used by ReleaseDataset: removing references one at a
// time would delete the branch on the first removal (an empty set) and then fail to recreate it for
// the next one.
func (s *Store) RemovePinReferences(ctx context.Context, repository, commitID string, refs []string) (orphaned bool, err error) {
	if len(refs) == 0 {
		return false, nil
	}
	for _, ref := range refs {
		if err := checkSegment("pin reference", ref); err != nil {
			return false, err
		}
	}
	key, err := pinKey(repository, commitID)
	if err != nil {
		return false, err
	}
	drop := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		drop[ref] = struct{}{}
	}
	for attempt := 0; attempt < pinCASRetries; attempt++ {
		current, predicate, err := s.readPin(ctx, key)
		if err != nil {
			return false, err
		}
		if current == nil {
			return false, nil
		}
		remaining := make([]string, 0, len(current.References))
		for _, existing := range current.References {
			if _, drop := drop[existing]; drop {
				continue
			}
			remaining = append(remaining, existing)
		}
		if len(remaining) == len(current.References) {
			return false, nil
		}
		next := *current
		next.References = remaining
		value, err := json.Marshal(next)
		if err != nil {
			return false, fmt.Errorf("datasets: encode pin record %s/%s: %w", repository, commitID, err)
		}
		err = s.store.SetIf(ctx, []byte(PartitionKey), key, value, predicate)
		if errors.Is(err, kv.ErrPredicateFailed) {
			continue // another writer changed the pin; re-read and retry
		}
		if err != nil {
			return false, err
		}
		if len(remaining) > 0 {
			return false, nil
		}
		if err := s.delete(ctx, key); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, kv.ErrPredicateFailed
}
