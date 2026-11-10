package catalog

import (
	"context"
	"errors"
	"fmt"
	stdatomic "sync/atomic"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/validator"
)

// DeletionCascade names the Metadata Search companion repository whose storage deletion a request
// for repository should also request (REPO_DELETE_STORAGE_SPEC.md R17). repository is the data
// repository's public record, as Catalog.GetRepository returns it.
//
// companion is the repository to cascade into, and it is only ever a name a proof named: the
// installer returns "<repository.Name>-metadata" when that repository exists and passes the
// init-commit proof (Catalog.IsMetadataCompanion, metadata_companion.go), so a user's own
// "foo-metadata" without the init commit is never cascaded. An empty companion, or ok false, means
// "no companion": the request deletes exactly the repository it named. err aborts the request
// before anything is written.
//
// The hook must be safe for concurrent use: one process serves many deletion requests at once.
type DeletionCascade func(ctx context.Context, repository *Repository) (companion string, ok bool, err error)

// deletionCascade holds the cascade installed by SetDeletionCascade. It is read on every request
// that asks to delete a repository's storage and written once at startup, so it is an atomic pointer
// rather than a plain variable - the same shape as the merge-conflict resolver
// (merge_conflict.go) and the uncommitted-GC protector (gc_write_uncommitted.go).
var deletionCascade stdatomic.Pointer[DeletionCascade]

// SetDeletionCascade installs f, or clears the cascade when f is nil. It is called once at startup
// by the binary that owns a catalog whose data repositories have Metadata Search companions
// (pkg/icebergcatalog.Bootstrap, installed from cmd/lakefs-sso/main.go), and it is cleared again -
// not left installed but inert - when Metadata Search is not enabled.
//
// A nil cascade is the upstream behaviour: a request deletes only the repository it named.
func SetDeletionCascade(f DeletionCascade) {
	if f == nil {
		deletionCascade.Store(nil)
		return
	}
	deletionCascade.Store(&f)
}

// SetDeletionCascade is the catalog-shaped spelling of the package function of the same name
// (REPO_DELETE_STORAGE_DESIGN.md section 6). The hook is process-wide - it is installed once, before
// the first request, and read by every catalog of the process - so the receiver is only the handle a
// caller such as pkg/icebergcatalog.Bootstrap already holds.
func (c *Catalog) SetDeletionCascade(f DeletionCascade) {
	SetDeletionCascade(f)
}

// DeleteStorageOptions are the request-path options of DeleteRepositoryWithStorage
// (REPO_DELETE_STORAGE_DESIGN.md section 4.2).
type DeleteStorageOptions struct {
	// Force is recorded on the deletion record and passed to retries of the metadata step.  It does
	// not affect the R8 refusal: a read-only repository is refused with or without it (R4, R8).
	Force bool
	// RequestedBy is the user that asked for the deletion, reported by the status endpoints (R14).
	RequestedBy string
}

var (
	// ErrStorageDeletionNotSupported is returned when the blockstore cannot delete a namespace, or
	// when repository deletion with storage is disabled in the configuration (R6, R7).
	ErrStorageDeletionNotSupported = errors.New("storage deletion is not supported")

	// ErrNamespaceShared is returned when another repository, or another pending storage deletion,
	// owns a namespace that overlaps the target's (R10).
	ErrNamespaceShared = errors.New("storage namespace is shared with another repository")

	// ErrNamespaceDeleteNoProgress is returned by the worker when a page neither advanced the marker
	// nor reached the end of the namespace.  Without it a broken adapter would make the worker spin.
	ErrNamespaceDeleteNoProgress = errors.New("namespace deletion made no progress")
)

// DeleteRepositoryWithStorage is the request path of repository deletion with storage
// (REPO_DELETE_STORAGE_DESIGN.md section 4.2).  It resolves the targets, runs every refusal check for
// all of them before any write (R6-R11), writes one deletion record per target and then runs the
// metadata step inline, so the repository is unreadable when the request returns (R3).
//
// The object deletion itself is the worker's job (storage_deletion_worker.go): this call returns as
// soon as the metadata is gone, whatever the size of the namespace (G2).
func (c *Catalog) DeleteRepositoryWithStorage(ctx context.Context, repository string, opts DeleteStorageOptions) ([]*RepositoryDeletion, error) {
	repositoryID := graveler.RepositoryID(repository)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}

	repo, err := c.getRepository(ctx, repository)
	if err != nil {
		// The repository record is gone (a repeated request, R15) or already being deleted: resume
		// by name from the deletion record, which outlives the repository's metadata (section 3.3).
		if errors.Is(err, graveler.ErrNotFound) || errors.Is(err, graveler.ErrRepositoryInDeletion) {
			return c.resumeRepositoryDeletion(ctx, repository, opts)
		}
		return nil, err
	}

	// The targets are R plus, when a cascade is installed and proves one, R's Metadata Search
	// companion (R17).  Both are resolved here and checked together below, so a refusal of either
	// target changes nothing at all (R19).
	targets := []repositoryDeletionTarget{{
		RepositoryDeletionTarget: RepositoryDeletionTarget{
			Repository:       repository,
			StorageID:        repo.StorageID.String(),
			StorageNamespace: repo.StorageNamespace.String(),
		},
		ReadOnly: repo.ReadOnly,
	}}
	companion, err := c.deletionCascadeTarget(ctx, repository, repo)
	if err != nil {
		return nil, err
	}
	if companion != nil {
		targets = append(targets, *companion)
	}

	// Every refusal check runs for every target before the first write (R19): a refusal changes
	// nothing at all.
	if err := c.checkRepositoryDeletionTargets(ctx, targets); err != nil {
		return nil, err
	}

	// One record per target.  If a later write fails, the earlier records stay and the worker
	// finishes them: each target is valid on its own because it passed its checks.
	records := make([]*RepositoryDeletion, 0, len(targets))
	for _, target := range targets {
		record := &RepositoryDeletion{
			Repository:       target.Repository,
			StorageID:        target.StorageID,
			StorageNamespace: target.StorageNamespace,
			State:            RepositoryDeletionState_PENDING,
			Step:             RepositoryDeletionStep_METADATA,
			RequestedBy:      opts.RequestedBy,
			Force:            opts.Force,
			CascadeOf:        target.CascadeOf,
		}
		stored, err := c.createOrReplaceRepositoryDeletion(ctx, record)
		if err != nil {
			return nil, err
		}
		records = append(records, stored)
	}

	// The metadata step runs inline, so the repository answers 404 when the request returns (R3).
	// A failure here still answers 202: the worker retries the step and the error is on the record.
	for i, record := range records {
		updated, err := c.runRepositoryDeletionMetadataStep(ctx, record)
		if err != nil {
			return nil, err
		}
		records[i] = updated
	}
	return records, nil
}

// repositoryDeletionTarget is a target of one request: the public overlap-scan view plus the
// repository record fields the refusal checks need.
type repositoryDeletionTarget struct {
	RepositoryDeletionTarget
	ReadOnly bool
	// CascadeOf is the data repository whose deletion requested this target's deletion, or empty
	// for the repository the request named.  It is recorded on the companion's deletion record
	// (R17, design section 3.1).
	CascadeOf string
}

// deletionCascadeTarget resolves the Metadata Search companion the installed cascade names for
// repository (R17).  It returns nil when no cascade is installed, or when the data repository has no
// proven companion - the ordinary case, and the only one for a plain lakeFS installation.
//
// The companion's storage namespace is read from the companion's own repository record and never
// derived from R's: the indexer lets an operator override it
// (metadata_search.repositories.<R>.storage_namespace, pkg/mds/indexer/repo.go metadataNamespace), so
// "R's namespace with -metadata appended" would name the wrong prefix - and delete the wrong
// prefix - for such a companion.
func (c *Catalog) deletionCascadeTarget(ctx context.Context, repository string, repo *graveler.RepositoryRecord) (*repositoryDeletionTarget, error) {
	cascade := deletionCascade.Load()
	if cascade == nil {
		return nil, nil
	}
	companion, ok, err := (*cascade)(ctx, &Repository{
		Name:             repository,
		StorageID:        repo.StorageID.String(),
		StorageNamespace: repo.StorageNamespace.String(),
		DefaultBranch:    repo.DefaultBranchID.String(),
		CreationDate:     repo.CreationDate,
		ReadOnly:         repo.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve the metadata companion of repository %s: %w", repository, err)
	}
	if !ok || companion == "" || companion == repository {
		return nil, nil
	}
	companionRepo, err := c.getRepository(ctx, companion)
	if err != nil {
		return nil, fmt.Errorf("read metadata companion repository %s of %s: %w", companion, repository, err)
	}
	return &repositoryDeletionTarget{
		RepositoryDeletionTarget: RepositoryDeletionTarget{
			Repository:       companion,
			StorageID:        companionRepo.StorageID.String(),
			StorageNamespace: companionRepo.StorageNamespace.String(),
		},
		ReadOnly:  companionRepo.ReadOnly,
		CascadeOf: repository,
	}, nil
}

// resumeRepositoryDeletion serves a request for a repository that is already gone (R15): the record
// is the only trace left, and a FAILED record is reset to PENDING so the worker picks it up again.
func (c *Catalog) resumeRepositoryDeletion(ctx context.Context, repository string, opts DeleteStorageOptions) ([]*RepositoryDeletion, error) {
	if !c.RepositoryDeletionStorage.Enabled {
		return nil, fmt.Errorf("repository_deletion.storage.enabled is false: %w", ErrStorageDeletionNotSupported)
	}
	record, predicate, err := c.getRepositoryDeletionWithPredicate(ctx, repository)
	if err != nil {
		return nil, err
	}
	switch record.State {
	case RepositoryDeletionState_DONE:
		// The deletion is already finished; the status stays readable until it expires (R14), and a
		// repeated request is a no-op.
		return []*RepositoryDeletion{record}, nil
	case RepositoryDeletionState_FAILED:
		// R15: the retry keeps the step and the marker, so the deletion continues where it stopped.
		record.RequestedBy = opts.RequestedBy
		record.Force = opts.Force
		record, err = c.ResetRepositoryDeletionForRetry(ctx, record, predicate)
		if err != nil {
			return nil, err
		}
		updated, err := c.runRepositoryDeletionMetadataStep(ctx, record)
		if err != nil {
			return nil, err
		}
		return []*RepositoryDeletion{updated}, nil
	default:
		return nil, fmt.Errorf("repository %s: %w", repository, ErrDeletionInProgress)
	}
}

// checkRepositoryDeletionTargets runs R6-R11 for every target.  It never writes: the first refusal
// wins and the caller changes nothing.
func (c *Catalog) checkRepositoryDeletionTargets(ctx context.Context, targets []repositoryDeletionTarget) error {
	if !c.RepositoryDeletionStorage.Enabled {
		return fmt.Errorf("repository_deletion.storage.enabled is false: %w", ErrStorageDeletionNotSupported)
	}

	plainTargets := make([]RepositoryDeletionTarget, 0, len(targets))
	for _, target := range targets {
		// R6: the blockstore must be able to delete a namespace (transient cannot).
		info := c.BlockAdapter.GetStorageNamespaceInfo(target.StorageID)
		if info == nil || !info.DeleteNamespaceSupport {
			return fmt.Errorf("blockstore %s cannot delete a storage namespace: %w", c.BlockAdapter.BlockstoreType(), ErrStorageDeletionNotSupported)
		}
		// R8: a read-only repository never wrote its own marker, so its namespace may hold data that
		// lakeFS does not own.  force does not bypass this (D3, R4).
		if target.ReadOnly {
			return fmt.Errorf("repository %s is read-only: %w", target.Repository, graveler.ErrReadOnlyRepository)
		}
		// R9: a namespace that is the root of a bucket, container, or the local/mem scheme has no
		// namespace-only prefix to delete under.
		if _, _, err := block.NamespaceDeletePrefix(target.StorageNamespace); err != nil {
			return fmt.Errorf("storage namespace %s: %w", target.StorageNamespace, err)
		}
		plainTargets = append(plainTargets, target.RepositoryDeletionTarget)
	}

	// R10: every live repository (every state, IN_DELETION included) and every non-DONE deletion
	// record owns its namespace.  The targets of this request are excluded from each other.
	conflict, err := c.FindNamespaceConflict(ctx, plainTargets)
	if err != nil {
		return err
	}
	if conflict != nil {
		return fmt.Errorf("repository %s: %w", conflict.Repository, ErrNamespaceShared)
	}

	// R11: an existing record that is neither FAILED nor DONE means a deletion is already pending.
	// A FAILED record is allowed because the request is its retry, and a DONE record because the name
	// may have been re-created since (R16).
	for _, target := range targets {
		record, err := c.GetRepositoryDeletion(ctx, target.Repository)
		if err != nil {
			if errors.Is(err, ErrRepositoryDeletionNotFound) {
				continue
			}
			return err
		}
		if record.State != RepositoryDeletionState_FAILED && record.State != RepositoryDeletionState_DONE {
			return fmt.Errorf("repository %s: %w", target.Repository, ErrDeletionInProgress)
		}
	}
	return nil
}

// createOrReplaceRepositoryDeletion writes the new record and returns the one the request will use.
// With no record present the write uses a nil predicate, so a concurrent request loses the race with
// kv.ErrPredicateFailed (R11).
//
// An existing FAILED record is reset, keeping its step and marker, because this request is its retry
// (R15).  A DONE record is only a status: the same repository name may have been re-created after it
// (R16), and deleting the new repository replaces it.  Anything else means a deletion is pending.
func (c *Catalog) createOrReplaceRepositoryDeletion(ctx context.Context, record *RepositoryDeletion) (*RepositoryDeletion, error) {
	existing, predicate, err := c.getRepositoryDeletionWithPredicate(ctx, record.Repository)
	if err != nil {
		if errors.Is(err, ErrRepositoryDeletionNotFound) {
			return record, c.CreateRepositoryDeletion(ctx, record)
		}
		return nil, err
	}
	switch existing.State {
	case RepositoryDeletionState_DONE:
		record.RequestedAt = time.Now().UTC()
		if err := c.UpdateRepositoryDeletion(ctx, record, predicate); err != nil {
			if errors.Is(err, kv.ErrPredicateFailed) {
				return nil, fmt.Errorf("repository %s: %w", record.Repository, ErrDeletionInProgress)
			}
			return nil, err
		}
		return record, nil
	case RepositoryDeletionState_FAILED:
		existing.RequestedBy = record.RequestedBy
		existing.Force = record.Force
		reset, err := c.ResetRepositoryDeletionForRetry(ctx, existing, predicate)
		if err != nil {
			return nil, err
		}
		return reset, nil
	default:
		return nil, fmt.Errorf("repository %s: %w", record.Repository, ErrDeletionInProgress)
	}
}

// runRepositoryDeletionMetadataStep deletes the repository's metadata now (design section 4.2 step
// 4).  force is safe: R8 already refused read-only repositories, and the request's force is kept on
// the record.  graveler.ErrNotFound is success, so the step is idempotent across restarts and
// retries.  On any other error the record keeps the error for the worker and the caller still
// answers 202.
func (c *Catalog) runRepositoryDeletionMetadataStep(ctx context.Context, record *RepositoryDeletion) (*RepositoryDeletion, error) {
	_, predicate, err := c.getRepositoryDeletionWithPredicate(ctx, record.Repository)
	if err != nil {
		return nil, err
	}

	record.State = RepositoryDeletionState_DELETING
	err = c.Store.DeleteRepository(ctx, graveler.RepositoryID(record.Repository), graveler.WithForce(true))
	if err != nil && !errors.Is(err, graveler.ErrNotFound) {
		record.LastError = err.Error()
		if updateErr := c.UpdateRepositoryDeletion(ctx, record, predicate); updateErr != nil {
			return nil, updateErr
		}
		return record, nil
	}

	record.LastError = ""
	record.Step = RepositoryDeletionStep_SETTLE
	if record.MetadataDeletedAt.IsZero() {
		record.MetadataDeletedAt = time.Now().UTC()
	}
	if err := c.UpdateRepositoryDeletion(ctx, record, predicate); err != nil {
		return nil, err
	}
	return record, nil
}
