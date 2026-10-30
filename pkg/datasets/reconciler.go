// Package datasets implements the lakeFS Datasets control plane (see DATASETS_DESIGN.md).
//
// reconciler.go is §12.3: the background repair loop, the KV lease that keeps exactly one
// node per interval running a pass, and the pass itself. A pass is a pure function of the
// catalog and the KV records, so it can be run by the ticker, by an operator or by a test;
// it never authorizes a caller and it never invents state, it only moves a dataset to the
// state §5.4 and §6.6 say it should already be in:
//
//  1. a dataset stuck in creating/publishing/deleting past datasets.publish_timeout is
//     resumed from the dataset repository (tag vN, or the latest HEAD commit carrying
//     dataset.version=N) or rolled back (pins released, active again);
//  2. every (repository, commit) a version references has its pin branch, and pins that
//     no version references are removed;
//  3. the immutability guard and the managed read policies exist and are attached;
//  4. a dataset whose pinned source repository disappeared is marked degraded, and the
//     mark is cleared when every source is reachable again.
//
// Every repair step tolerates "not found", so a crashed operation is finished rather than
// retried forever (§6.6).
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS documentation
// only. No code, SQL, protobuf or UI asset from lakeFS Enterprise, and nothing from upstream
// lakeFS >= 1.87, was used.
package datasets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// ReconcileLeaseKey is the KV key of the reconcile lease (§12.3): "datasets/_lease" in the
// datasets partition. A dataset name may not contain "_" (§5.2 rule 1), so the key can never
// collide with the record of a dataset called "_lease".
const ReconcileLeaseKey = datasetKeyPrefix + kv.PathDelimiter + "_lease"

// reconcileOwnerPrefix namespaces the owner strings a Reconciler writes into the lease.
const reconcileOwnerPrefix = "datasets-reconciler"

// DefaultReconcileLeaseTTL is the lease expiry used when the configuration yields none. The
// TTL is leaseIntervalFactor reconcile intervals, so a pass that overruns one tick still holds
// the lease for the next one, while a node that crashes does not block the others for longer
// than that many intervals.
const (
	DefaultReconcileLeaseTTL = 2 * time.Minute
	leaseIntervalFactor      = 2
)

// leaseRecord is the value at ReconcileLeaseKey. ExpiresAt is an absolute wall-clock time:
// every node compares it with its own clock, so a slightly skewed node can only start a pass
// a little early, and the pass is idempotent.
type leaseRecord struct {
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expires_at"`
}

// AcquireLease tries to take the reconcile lease for owner for ttl. It reports whether this
// caller now holds it.
//
// A missing or expired lease is written with SetIf, so two nodes racing for it cannot both
// win: the loser gets kv.ErrPredicateFailed, re-reads the record and finds a live owner. A
// live lease held by another owner is left alone.
func (s *Store) AcquireLease(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	if owner == "" {
		return false, fmt.Errorf("%w: reconcile lease owner is required", ErrInvalidRecordKey)
	}
	if ttl <= 0 {
		ttl = DefaultReconcileLeaseTTL
	}
	key := []byte(ReconcileLeaseKey)
	for attempt := 0; attempt < pinCASRetries; attempt++ {
		now := time.Now().UTC()
		var predicate kv.Predicate
		res, err := s.store.Get(ctx, []byte(PartitionKey), key)
		switch {
		case errors.Is(err, kv.ErrNotFound):
			// No lease record yet: SetIf with a nil predicate creates it only if it is
			// still absent.
		case err != nil:
			return false, err
		default:
			var current leaseRecord
			if decodeErr := json.Unmarshal(res.Value, &current); decodeErr == nil &&
				current.ExpiresAt.After(now) && current.Owner != owner {
				return false, nil // a live lease owned by somebody else
			}
			predicate = res.Predicate
		}
		value, err := json.Marshal(leaseRecord{Owner: owner, ExpiresAt: now.Add(ttl)})
		if err != nil {
			return false, fmt.Errorf("datasets: encode reconcile lease: %w", err)
		}
		err = s.store.SetIf(ctx, []byte(PartitionKey), key, value, predicate)
		switch {
		case errors.Is(err, kv.ErrPredicateFailed):
			continue // lost the race; re-read and retry
		case err != nil:
			return false, err
		}
		return true, nil
	}
	return false, kv.ErrPredicateFailed
}

// ReleaseLease drops the lease when owner still holds it, so the next node can run a pass
// immediately instead of waiting for the expiry. It is a no-op when the lease is already gone
// or owned by someone else (a crashed node's lease expired and was taken over): releasing
// another owner's lease would let two nodes run concurrently.
func (s *Store) ReleaseLease(ctx context.Context, owner string) error {
	key := []byte(ReconcileLeaseKey)
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	switch {
	case errors.Is(err, kv.ErrNotFound):
		return nil
	case err != nil:
		return err
	}
	var current leaseRecord
	if err := json.Unmarshal(res.Value, &current); err != nil {
		// A corrupted lease is left to expire rather than deleted: it may belong to an
		// owner this node cannot identify.
		return nil
	}
	if current.Owner != owner {
		return nil
	}
	return s.store.Delete(ctx, []byte(PartitionKey), key)
}

// Reconciler runs the §12.3 repair pass. One instance per process; it holds no per-pass
// state, so passes never interfere.
//
// The zero Reconciler is unusable: build one with NewReconciler.
type Reconciler struct {
	svc     *Service
	store   *Store
	pins    *Pins
	catalog *catalog.Catalog

	log logging.Logger

	// interval is datasets.reconcile_interval and timeout is datasets.publish_timeout;
	// leaseTTL bounds how long a crashed node keeps the lease.
	interval time.Duration
	timeout  time.Duration
	leaseTTL time.Duration

	// owner identifies this reconciler instance in the lease. It is unique per process so
	// two nodes (or two instances in one process, as in the tests) cannot both believe
	// they hold the lease.
	owner string

	// now is the clock used for staleness and record timestamps; production uses the wall
	// clock and tests replace it.
	now func() time.Time

	// onPass is a test hook invoked once a pass owns the lease and before it repairs
	// anything. It is nil in production.
	onPass func()
}

// NewReconciler builds the reconciler of svc, using the service's configuration, store,
// catalog, auth service and clock. It does not start anything: RoutesBuilder runs it (Run),
// and Reconcile runs a single pass on demand.
func NewReconciler(svc *Service) *Reconciler {
	interval := DefaultReconcileInterval
	timeout := DefaultPublishTimeout
	if svc != nil && svc.cfg != nil {
		if svc.cfg.ReconcileInterval > 0 {
			interval = svc.cfg.ReconcileInterval
		}
		if svc.cfg.PublishTimeout > 0 {
			timeout = svc.cfg.PublishTimeout
		}
	}
	owner := reconcileOwnerPrefix
	if id, err := newULID(time.Now()); err == nil {
		owner += "-" + id
	} else {
		owner += "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	r := &Reconciler{
		svc:      svc,
		store:    svc.store,
		pins:     svc.pins,
		catalog:  svc.catalog,
		log:      logging.ContextUnavailable(),
		interval: interval,
		timeout:  timeout,
		leaseTTL: leaseIntervalFactor * interval,
		owner:    owner,
		now:      svc.now,
	}
	if r.leaseTTL < DefaultReconcileLeaseTTL {
		r.leaseTTL = DefaultReconcileLeaseTTL
	}
	return r
}

// Run is the background loop started by RoutesBuilder. It runs a pass every
// datasets.reconcile_interval until ctx is cancelled, then returns. A pass that fails is
// logged and counted; the loop keeps going, because a transient KV or catalog error must not
// stop the repair forever.
func (r *Reconciler) Run(ctx context.Context) {
	if ctx == nil {
		return
	}
	log := r.log.WithField("interval", r.interval)
	log.Info("datasets: reconciler started")
	defer log.Info("datasets: reconciler stopped")

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report, err := r.Reconcile(ctx)
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				return
			case err != nil:
				log.WithError(err).Warn("datasets: reconcile pass failed")
			case report == nil:
				log.Debug("datasets: another node holds the reconcile lease; skipping this pass")
			default:
				report.log(log)
			}
		}
	}
}

// Reconcile runs one repair pass (§12.3) after taking the KV lease, and releases it when the
// pass is done. It returns a nil report and a nil error when another node holds the lease: in
// that case this node does not run a pass at all.
//
// Reconcile is the directly callable entry point of the reconciler; the ticker in Run is only
// one caller.
func (r *Reconciler) Reconcile(ctx context.Context) (*ReconcileReport, error) {
	if r == nil || r.store == nil || r.svc == nil {
		return nil, fmt.Errorf("%w: reconciler", ErrIncompleteService)
	}
	acquired, err := r.store.AcquireLease(ctx, r.owner, r.leaseTTL)
	if err != nil {
		return nil, fmt.Errorf("datasets: acquire reconcile lease: %w", err)
	}
	if !acquired {
		return nil, nil
	}
	// Release with a context that survives cancellation: a pass interrupted by shutdown must
	// still hand the lease back, or the next node waits for the expiry.
	releaseCtx := context.WithoutCancel(ctx)
	defer func() {
		if err := r.store.ReleaseLease(releaseCtx, r.owner); err != nil {
			r.log.WithError(err).Warn("datasets: could not release the reconcile lease; it will expire")
		}
	}()

	if r.onPass != nil {
		r.onPass()
	}
	return r.repair(releaseCtx), nil
}

// ReconcileReport summarises one repair pass. Every counter is diagnostic; nothing in the
// control plane depends on them.
type ReconcileReport struct {
	// Datasets is the number of dataset records seen by the pass.
	Datasets int
	// StatesRepaired counts datasets whose transient state was resumed, rolled back or
	// finished.
	StatesRepaired int
	// VersionsRebuilt counts VersionRecords rebuilt from the dataset repository.
	VersionsRebuilt int
	// PinsRecreated counts pin branches re-created.
	PinsRecreated int
	// PinsRemoved counts orphan pin records (and their branches) removed.
	PinsRemoved int
	// PoliciesReattached counts managed policies written or attached again.
	PoliciesReattached int
	// Degraded counts datasets newly marked degraded.
	Degraded int
	// Recovered counts datasets whose degraded mark was cleared.
	Recovered int
	// Errors counts per-dataset (or per-pin) repairs that failed. The pass continues after
	// a failure; the next pass retries.
	Errors int
}

// log emits the pass summary at info level.
func (r *ReconcileReport) log(log logging.Logger) {
	log.WithFields(logging.Fields{
		"datasets":            r.Datasets,
		"states_repaired":     r.StatesRepaired,
		"versions_rebuilt":    r.VersionsRebuilt,
		"pins_recreated":      r.PinsRecreated,
		"pins_removed":        r.PinsRemoved,
		"policies_reattached": r.PoliciesReattached,
		"degraded":            r.Degraded,
		"recovered":           r.Recovered,
		"errors":              r.Errors,
	}).Info("datasets: reconcile pass complete")
}

// repair is the pass itself, without the lease. The caller must hold the lease.
func (r *Reconciler) repair(ctx context.Context) *ReconcileReport {
	report := &ReconcileReport{}
	records, err := r.store.ListDatasets(ctx)
	if err != nil {
		report.Errors++
		dsMetrics.reconcileErrors.Inc()
		r.log.WithError(err).Warn("datasets: cannot list datasets for a reconcile pass")
		return report
	}
	report.Datasets = len(records)
	dsMetrics.datasetsTotal.Set(float64(len(records)))

	names := make([]string, 0, len(records))
	for _, rec := range records {
		if ctx.Err() != nil {
			return report
		}
		if rec == nil {
			continue
		}
		names = append(names, rec.Name)
		if err := r.repairDataset(ctx, rec, report); err != nil {
			report.Errors++
			dsMetrics.reconcileErrors.Inc()
			r.log.WithError(err).WithField("dataset", rec.Name).
				Warn("datasets: could not repair a dataset; the next pass will retry")
		}
	}

	// §9.5 note: the installation-wide managed policies must follow the dataset set.
	if err := r.ensureInstallationPolicies(ctx, names, report); err != nil {
		report.Errors++
		dsMetrics.reconcileErrors.Inc()
		r.log.WithError(err).Warn("datasets: could not repair the installation-wide policies")
	}

	if err := r.removeOrphanPins(ctx, records, report); err != nil {
		report.Errors++
		dsMetrics.reconcileErrors.Inc()
		r.log.WithError(err).Warn("datasets: could not remove orphan pins")
	}
	return report
}

// repairDataset repairs one dataset record (§12.3 items 1-4).
func (r *Reconciler) repairDataset(ctx context.Context, rec *DatasetRecord, report *ReconcileReport) error {
	switch rec.State {
	case DatasetStateCreating, DatasetStatePublishing, DatasetStateDeleting:
		if !r.stale(rec) {
			// An operation is genuinely in flight; its own writer owns the record and the
			// reconciler must not race it (§5.4).
			return nil
		}
		if rec.State == DatasetStateDeleting {
			return r.finishDelete(ctx, rec, report)
		}
		if err := r.repairInterrupted(ctx, rec, report); err != nil {
			return err
		}
		// Re-read: the repair advanced the record to active (or removed it).
		repaired, err := r.store.GetDataset(ctx, rec.Name)
		if errors.Is(err, kv.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if repaired.State != DatasetStateActive && repaired.State != DatasetStateDegraded {
			return nil
		}
		rec = repaired
	case DatasetStateActive, DatasetStateDegraded:
		// Nothing to resume; fall through to the idempotent repairs below.
	default:
		r.log.WithFields(logging.Fields{"dataset": rec.Name, "state": rec.State}).
			Warn("datasets: dataset record has an unknown state; leaving it alone")
		return nil
	}

	// §12.3 item 2: every pinned source has its pin branch.
	recreated, missing, err := r.pins.EnsureDatasetPins(ctx, rec.Name)
	if len(recreated) > 0 {
		report.PinsRecreated += len(recreated)
		dsMetrics.pinsRecreated.Add(float64(len(recreated)))
		r.log.WithFields(logging.Fields{
			"dataset": rec.Name,
			"pins":    len(recreated),
		}).Warn("datasets: re-created missing GC pin branches")
	}
	if err != nil {
		return fmt.Errorf("datasets: repair pins of %q: %w", rec.Name, err)
	}

	// §12.3 item 3: the per-dataset managed policies exist and are attached.
	if err := r.ensureDatasetPolicies(ctx, rec.Name, report); err != nil {
		return fmt.Errorf("datasets: repair policies of %q: %w", rec.Name, err)
	}

	// §12.3 item 4: degraded marking follows source reachability.
	return r.reconcileDegraded(ctx, rec, len(missing) > 0, report)
}

// stale reports whether a transient record has been left alone for longer than
// datasets.publish_timeout (§5.4 step 2, §12.3 item 1).
func (r *Reconciler) stale(rec *DatasetRecord) bool {
	timeout := r.timeout
	if timeout <= 0 {
		timeout = DefaultPublishTimeout
	}
	return r.now().Sub(rec.UpdatedAt) > timeout
}

// repairInterrupted finishes or rolls back an interrupted create/publish (§12.3 item 1).
//
// The order is the one §5.4 makes authoritative: the tag vN first, then the latest HEAD
// commit carrying dataset.version=N, and only then the rollback.
func (r *Reconciler) repairInterrupted(ctx context.Context, rec *DatasetRecord, report *ReconcileReport) error {
	dataset := rec.Name
	version := rec.PendingVersion
	if version <= 0 {
		version = rec.LatestVersion + 1
	}
	tag := VersionTag(version)

	// (a) the tag exists: the version was committed, only the KV record is missing.
	tagCommit, err := r.catalog.GetTag(ctx, dataset, tag)
	switch {
	case err == nil:
		return r.completeVersion(ctx, rec, version, tagCommit, report)
	case !isNotFound(err):
		return fmt.Errorf("datasets: read tag %s of %q: %w", tag, dataset, err)
	}

	// (b) the commit exists and carries dataset.version=N: create the missing tag.
	headCommit, err := r.catalog.GetBranchReference(ctx, dataset, DefaultBranch)
	switch {
	case err == nil:
		commitLog, getErr := r.catalog.GetCommit(ctx, dataset, headCommit)
		if getErr != nil {
			return fmt.Errorf("datasets: read commit %s of %q: %w", headCommit, dataset, getErr)
		}
		if commitLog.Metadata[CommitMetadataVersionKey] == strconv.Itoa(version) {
			if _, tagErr := r.catalog.CreateTag(ctx, dataset, tag, headCommit, graveler.WithForce(true)); tagErr != nil &&
				!errors.Is(tagErr, graveler.ErrTagAlreadyExists) {
				return fmt.Errorf("datasets: create tag %s on %q: %w", tag, dataset, tagErr)
			}
			r.log.WithFields(logging.Fields{
				"dataset": dataset,
				"version": version,
				"commit":  headCommit,
			}).Warn("datasets: created the missing tag of an interrupted publish")
			return r.completeVersion(ctx, rec, version, headCommit, report)
		}
	case !isNotFound(err):
		return fmt.Errorf("datasets: read %s/%s: %w", dataset, DefaultBranch, err)
	}

	// (c) nothing was written: roll back. §12.3 item 1 releases the pins acquired for N and
	// returns the dataset to active; a create that never got as far as its repository is
	// removed instead of being promoted to a dataset with no repository.
	if rec.State == DatasetStateCreating {
		if _, err := r.catalog.GetRepository(ctx, dataset); isNotFound(err) {
			if err := r.store.DeleteDataset(ctx, dataset, rec); err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
				return err
			}
			report.StatesRepaired++
			r.log.WithField("dataset", dataset).Warn("datasets: removed the record of an interrupted create")
			return nil
		}
	}
	if err := r.pins.ReleaseVersion(ctx, dataset, version); err != nil {
		return fmt.Errorf("datasets: release pins of %s@v%d: %w", dataset, version, err)
	}
	next := *rec
	next.State = DatasetStateActive
	next.PendingVersion = 0
	next.UpdatedAt = r.now()
	if err := r.store.PutDatasetIf(ctx, &next, rec); err != nil {
		return fmt.Errorf("datasets: roll %q back to active: %w", dataset, err)
	}
	report.StatesRepaired++
	r.log.WithFields(logging.Fields{
		"dataset": dataset,
		"version": version,
	}).Warn("datasets: rolled an interrupted publish back to active")
	return nil
}

// completeVersion rebuilds the missing VersionRecord of a committed version from the
// version's _dataset/definition.json and marks the dataset active (§5.4: the tag and the
// definition object are authoritative, the KV record is derived).
func (r *Reconciler) completeVersion(ctx context.Context, rec *DatasetRecord, version int, commitID string, report *ReconcileReport) error {
	dataset := rec.Name
	_, err := r.store.GetVersion(ctx, dataset, version)
	switch {
	case err == nil:
		// The record exists; only the head needs to catch up.
	case errors.Is(err, kv.ErrNotFound):
		doc, docErr := r.svc.readDefinitionDocument(ctx, dataset, commitID, DefinitionPath)
		if docErr != nil {
			return fmt.Errorf("datasets: rebuild version %d of %q: %w", version, dataset, docErr)
		}
		commitLog, commitErr := r.catalog.GetCommit(ctx, dataset, commitID)
		if commitErr != nil {
			return fmt.Errorf("datasets: read commit %s of %q: %w", commitID, dataset, commitErr)
		}
		stats, statsErr := r.rebuildStats(ctx, dataset, commitID, doc.Definition)
		if statsErr != nil {
			return fmt.Errorf("datasets: rebuild stats of %s@v%d: %w", dataset, version, statsErr)
		}
		rebuilt := VersionRecord{
			Dataset:     dataset,
			Version:     version,
			Definition:  doc.Definition,
			Message:     doc.Message,
			Publisher:   doc.Publisher,
			PublishedAt: commitLog.CreationDate,
			CommitID:    commitID,
			MetaRangeID: commitLog.MetaRangeID,
			ProposalID:  commitLog.Metadata[CommitMetadataProposalKey],
			Stats:       stats,
		}
		if _, err := r.store.PutVersion(ctx, rebuilt); err != nil {
			return fmt.Errorf("datasets: write rebuilt version %d of %q: %w", version, dataset, err)
		}
		report.VersionsRebuilt++
		r.log.WithFields(logging.Fields{
			"dataset": dataset,
			"version": version,
			"commit":  commitID,
		}).Warn("datasets: rebuilt a missing version record from the dataset repository")
	default:
		return err
	}

	next := *rec
	next.State = DatasetStateActive
	next.PendingVersion = 0
	if next.LatestVersion < version {
		next.LatestVersion = version
	}
	next.UpdatedAt = r.now()
	if err := r.store.PutDatasetIf(ctx, &next, rec); err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			// Somebody else moved the record; the next pass converges on the new value.
			return nil
		}
		return fmt.Errorf("datasets: activate %q after rebuilding version %d: %w", dataset, version, err)
	}
	report.StatesRepaired++
	return nil
}

// finishDelete completes a delete that was interrupted after the record was CASed to
// "deleting" (§6.6, §12.3 item 1). Every step tolerates not found, so it converges whether
// the repository, the pins or the records were already removed.
func (r *Reconciler) finishDelete(ctx context.Context, rec *DatasetRecord, report *ReconcileReport) error {
	dataset := rec.Name
	versions, err := r.store.ListVersions(ctx, dataset)
	if err != nil && !errors.Is(err, kv.ErrNotFound) {
		return err
	}

	// §6.6 step 2: the repository. A crashed delete usually got this far.
	if err := r.catalog.DeleteRepository(ctx, dataset, graveler.WithForce(true)); err != nil && !isNotFound(err) {
		return fmt.Errorf("datasets: finish delete of repository %q: %w", dataset, err)
	}

	var errs []error
	// §6.6 step 3: the pins of every version.
	if err := r.pins.ReleaseDataset(ctx, dataset); err != nil {
		errs = append(errs, err)
	}
	// §6.6 step 4: policies and grants, then the control-plane records.
	errs = append(errs, r.svc.dropAccessGrants(ctx, dataset)...)
	errs = append(errs, r.svc.dropImmutabilityGuard(ctx, dataset)...)
	if err := r.store.DeleteDataset(ctx, dataset, rec); err != nil && !isNotFound(err) {
		errs = append(errs, fmt.Errorf("datasets: delete record of %q: %w", dataset, err))
	}
	for _, version := range versions {
		if err := r.store.DeleteVersion(ctx, dataset, version.Version); err != nil && !isNotFound(err) {
			errs = append(errs, err)
		}
	}
	if proposals, err := r.store.ListProposals(ctx, dataset); err != nil && !errors.Is(err, kv.ErrNotFound) {
		errs = append(errs, err)
	} else {
		for _, proposal := range proposals {
			if err := r.store.DeleteProposal(ctx, dataset, proposal.ID); err != nil && !isNotFound(err) {
				errs = append(errs, err)
			}
		}
	}
	// §9.5 note: the aggregate read policy must forget the dataset.
	if err := r.svc.ensureAllReadPolicy(ctx); err != nil {
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		report.StatesRepaired++
		r.log.WithField("dataset", dataset).Warn("datasets: finished an interrupted delete")
	}
	return errors.Join(errs...)
}

// ensureDatasetPolicies re-creates and re-attaches the per-dataset managed policies of
// §9.2/§9.4 (§12.3 item 3). It reuses the service's builders and bootstrap helpers; it does
// not build policy statements of its own.
func (r *Reconciler) ensureDatasetPolicies(ctx context.Context, dataset string, report *ReconcileReport) error {
	// Immutability guard (§9.4). Written before the first version, so it must exist for
	// every dataset record, including one created empty.
	immutabilityName := ImmutabilityPolicyName(dataset)
	_, policyErr := r.svc.authService.GetPolicy(ctx, immutabilityName)
	missingPolicy := errors.Is(policyErr, auth.ErrNotFound)
	if policyErr != nil && !missingPolicy {
		return fmt.Errorf("datasets: read policy %s: %w", immutabilityName, policyErr)
	}
	if err := r.svc.ensureImmutabilityGuard(ctx, dataset); err != nil {
		return err
	}
	if missingPolicy {
		report.PoliciesReattached++
		r.log.WithFields(logging.Fields{
			"dataset": dataset,
			"policy":  immutabilityName,
		}).Warn("datasets: re-created a missing immutability guard")
	}

	// Consumer read policy (§9.2): only datasets with grants have one; re-attach it to the
	// principals named by the grant records.
	grants, err := r.store.ListGrants(ctx, dataset)
	if err != nil {
		return err
	}
	if len(grants) == 0 {
		return nil
	}
	consumerName := ConsumerReadPolicyName(dataset)
	_, policyErr = r.svc.authService.GetPolicy(ctx, consumerName)
	missingPolicy = errors.Is(policyErr, auth.ErrNotFound)
	if policyErr != nil && !missingPolicy {
		return fmt.Errorf("datasets: read policy %s: %w", consumerName, policyErr)
	}
	if err := r.svc.ensureConsumerPolicy(ctx, dataset); err != nil {
		return err
	}
	if missingPolicy {
		report.PoliciesReattached++
		r.log.WithFields(logging.Fields{
			"dataset": dataset,
			"policy":  consumerName,
		}).Warn("datasets: re-created a missing consumer read policy")
	}
	for _, grant := range grants {
		var attachErr error
		switch grant.PrincipalType {
		case PrincipalTypeUser:
			attachErr = r.svc.authService.AttachPolicyToUser(ctx, consumerName, grant.PrincipalID)
		case PrincipalTypeGroup:
			attachErr = r.svc.authService.AttachPolicyToGroup(ctx, consumerName, grant.PrincipalID)
		default:
			continue
		}
		if attachErr != nil && !errors.Is(attachErr, auth.ErrAlreadyExists) && !errors.Is(attachErr, auth.ErrNotFound) {
			return fmt.Errorf("datasets: re-attach %s to %s %q: %w",
				consumerName, grant.PrincipalType, grant.PrincipalID, attachErr)
		}
		if attachErr == nil {
			report.PoliciesReattached++
			r.log.WithFields(logging.Fields{
				"dataset":   dataset,
				"policy":    consumerName,
				"user":      grant.PrincipalID,
				"principal": grant.PrincipalType,
			}).Warn("datasets: re-attached a detached consumer read policy")
		}
	}
	return nil
}

// ensureInstallationPolicies keeps the installation-wide managed policies in step with the
// dataset set and re-attaches them to their groups (§9.5 note, §12.3 item 3).
func (r *Reconciler) ensureInstallationPolicies(ctx context.Context, names []string, report *ReconcileReport) error {
	drift, err := DetectDatasetsAllReadDrift(ctx, r.svc.authService, names, r.now())
	if err != nil {
		return err
	}
	if drift {
		if _, err := EnsureDatasetsAllReadPolicy(ctx, r.svc.authService, names, r.now()); err != nil {
			return err
		}
		report.PoliciesReattached++
		r.log.Warn("datasets: rewrote the managed datasets-all-read policy")
	}
	attachments := []struct {
		group    string
		policies []string
	}{
		{DatasetAdminsGroup, []string{DatasetsFullAccessPolicyName, DatasetsAllReadPolicyName}},
		{DatasetConsumersGroup, []string{DatasetsReadAllPolicyName, DatasetsAllReadPolicyName}},
	}
	for _, attachment := range attachments {
		if err := attachDatasetPolicies(ctx, r.svc.authService, attachment.group, attachment.policies); err != nil {
			return err
		}
	}
	return nil
}

// reconcileDegraded applies §12.3 item 4: mark an active dataset degraded when a pinned
// source repository is gone, and clear the mark when every source is reachable again.
func (r *Reconciler) reconcileDegraded(ctx context.Context, rec *DatasetRecord, missing bool, report *ReconcileReport) error {
	switch {
	case missing && rec.State == DatasetStateActive:
		_, marked, err := r.pins.CheckDatasetDegraded(ctx, rec.Name)
		if err != nil {
			return err
		}
		if marked {
			report.Degraded++
			r.log.WithField("dataset", rec.Name).Warn("datasets: marked a dataset degraded; a pinned source repository is gone")
		}
	case !missing && rec.State == DatasetStateDegraded:
		cleared, err := r.pins.ClearDatasetDegraded(ctx, rec.Name)
		if err != nil {
			return err
		}
		if cleared {
			report.Recovered++
			r.log.WithField("dataset", rec.Name).Info("datasets: cleared the degraded mark; every source is reachable again")
		}
	}
	return nil
}

// removeOrphanPins deletes the pin records, and their branches, that no version references
// (§12.3 item 2). A pin is an orphan when its reference set is empty, or when every
// reference names a dataset that no longer exists; a pin held by a dataset that is still
// being published is left alone, because §12.3 item 1 owns that record.
func (r *Reconciler) removeOrphanPins(ctx context.Context, records []*DatasetRecord, report *ReconcileReport) error {
	pins, err := r.store.listAllPins(ctx)
	if err != nil {
		return err
	}
	dsMetrics.pinsTotal.Set(float64(len(pins)))

	known := make(map[string]struct{}, len(records))
	for _, rec := range records {
		if rec != nil {
			known[rec.Name] = struct{}{}
		}
	}
	for _, pin := range pins {
		if ctx.Err() != nil {
			return nil
		}
		if !pinIsOrphan(pin, known) {
			continue
		}
		src := PinnedSource{Repository: pin.Repository, CommitID: pin.CommitID}
		if err := r.pins.deletePinBranch(ctx, src); err != nil {
			report.Errors++
			dsMetrics.reconcileErrors.Inc()
			r.log.WithError(err).WithField("pin", src.String()).Warn("datasets: could not delete an orphan pin branch")
			continue
		}
		if err := r.store.DeletePin(ctx, pin.Repository, pin.CommitID); err != nil && !errors.Is(err, kv.ErrNotFound) {
			report.Errors++
			dsMetrics.reconcileErrors.Inc()
			r.log.WithError(err).WithField("pin", src.String()).Warn("datasets: could not delete an orphan pin record")
			continue
		}
		report.PinsRemoved++
		r.log.WithField("pin", src.String()).Info("datasets: removed an orphan GC pin")
	}
	return nil
}

// pinIsOrphan reports whether no existing dataset references pin.
func pinIsOrphan(pin *PinRecord, known map[string]struct{}) bool {
	if pin == nil || len(pin.References) == 0 {
		return true
	}
	for _, ref := range pin.References {
		name, ok := datasetOfPinReference(ref)
		if !ok {
			continue // an unparseable reference is not evidence of an orphan
		}
		if _, exists := known[name]; exists {
			return false
		}
	}
	return true
}

// datasetOfPinReference splits a "<dataset>@v<n>" memo (PinReference) into the dataset name.
func datasetOfPinReference(ref string) (string, bool) {
	idx := strings.LastIndex(ref, "@v")
	if idx <= 0 {
		return "", false
	}
	return ref[:idx], true
}

// rebuildStats recomputes the ItemStats of a version from its dataset repository commit
// (§5.3 VersionRecord.Stats), so a rebuilt record is not left with zeroes. Entries are
// attributed to the definition item whose target contains them; §5.2 rule 3 keeps targets
// disjoint, so the attribution is unambiguous. The synthetic definition entry is skipped.
func (r *Reconciler) rebuildStats(ctx context.Context, dataset, commitID string, def Definition) (ItemStats, error) {
	items := make([]ItemStat, len(def.Data))
	for i := range def.Data {
		items[i].Target = def.Data[i].Target
	}
	after := ""
	for {
		entries, hasMore, err := r.catalog.ListEntries(ctx, dataset, commitID, "", after, "", catalog.ListEntriesLimitMax)
		if err != nil {
			return ItemStats{}, err
		}
		for _, entry := range entries {
			if entry.Path == DefinitionPath {
				continue
			}
			idx := definitionItemIndex(&def, entry.Path)
			if idx < 0 {
				continue
			}
			items[idx].Objects++
			items[idx].Bytes += entry.Size
		}
		if !hasMore || len(entries) == 0 {
			break
		}
		after = entries[len(entries)-1].Path
	}
	stats := ItemStats{Items: items}
	for _, item := range items {
		stats.Objects += item.Objects
		stats.Bytes += item.Bytes
	}
	return stats, nil
}

// definitionItemIndex returns the index of the definition item that owns path, or -1 when no
// item does. The longest matching target wins; object items match their target exactly.
func definitionItemIndex(def *Definition, path string) int {
	best, bestLen := -1, -1
	for i := range def.Data {
		item := &def.Data[i]
		if item.Type == ItemTypeObject {
			if path != item.Target {
				continue
			}
		} else if item.Target != RootTarget &&
			path != item.Target && !strings.HasPrefix(path, item.Target+"/") {
			continue
		}
		if len(item.Target) > bestLen {
			best, bestLen = i, len(item.Target)
		}
	}
	return best
}
