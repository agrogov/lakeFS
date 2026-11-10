package catalog

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/distributed"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// repositoryDeletionRecordExpiry is how long a DONE record stays readable after the deletion
	// ended (R14): 24 hours.
	repositoryDeletionRecordExpiry = 24 * time.Hour

	// repositoryDeletionDummyKey is the namespace marker lakeFS writes when a repository is created
	// (ensureStorageNamespace).  It is deleted last: until then the namespace still looks taken
	// (R12).
	repositoryDeletionDummyKey = "_lakefs/dummy"

	// repositoryDeletionMaxBackoff caps the exponential retry delay (R15).
	repositoryDeletionMaxBackoff = 10 * time.Minute

	// repositoryDeletionOwnerPrefix is the MostlyCorrectOwner prefix and the acquire/refresh
	// intervals of the design (section 5).
	repositoryDeletionOwnerPrefix           = "repository-deletions/owner"
	repositoryDeletionOwnerAcquireInterval  = 30 * time.Second
	repositoryDeletionOwnerRefreshInterval  = 10 * time.Second
	repositoryDeletionDefaultSettleInterval = 5 * time.Second
)

// RepositoryDeletionWorkerOption configures a repository deletion worker.  The production values
// come from the configuration; tests use the options to run a worker in milliseconds.
type RepositoryDeletionWorkerOption func(*repositoryDeletionWorker)

// WithRepositoryDeletionSettle overrides the SETTLE interval.  Production passes the repository
// cache expiry from the configuration: another replica may still hold the deleted repository in
// its cache and accept one more write until then (design section 5).
func WithRepositoryDeletionSettle(settle time.Duration) RepositoryDeletionWorkerOption {
	return func(w *repositoryDeletionWorker) {
		if settle > 0 {
			w.settle = settle
		}
	}
}

// WithRepositoryDeletionOwnershipIntervals overrides the MostlyCorrectOwner intervals.
func WithRepositoryDeletionOwnershipIntervals(acquire, refresh time.Duration) RepositoryDeletionWorkerOption {
	return func(w *repositoryDeletionWorker) {
		if acquire > 0 {
			w.acquireInterval = acquire
		}
		if refresh > 0 {
			w.refreshInterval = refresh
		}
	}
}

// repositoryDeletionWorker owns one record at a time and advances it until it is DONE, it fails, the
// process shuts down, or the record becomes due later (REPO_DELETE_STORAGE_DESIGN.md section 5).
type repositoryDeletionWorker struct {
	catalog *Catalog
	log     logging.Logger

	owner   *distributed.MostlyCorrectOwner
	ownerID string

	interval        time.Duration
	batchSize       int
	maxAttempts     int
	settle          time.Duration
	acquireInterval time.Duration
	refreshInterval time.Duration
}

// StartRepositoryDeletionWorker starts the background namespace deletion loop, one goroutine per
// lakeFS process (R21, G3).  It is called from cmd/lakefs/cmd/run.go, only in the server process.
//
// The worker uses the process context: a page in flight at shutdown is either recorded or redone
// after restart, and both are correct (A3).
func (c *Catalog) StartRepositoryDeletionWorker(ctx context.Context, opts ...RepositoryDeletionWorkerOption) {
	cfg := c.RepositoryDeletionStorage
	log := logging.FromContext(ctx).WithField("component", "repository_deletion_worker")
	if !cfg.Enabled {
		log.Info("Repository storage deletion is disabled; worker not started")
		return
	}

	w := &repositoryDeletionWorker{
		catalog:         c,
		log:             log,
		ownerID:         c.instanceID,
		interval:        cfg.Interval,
		batchSize:       cfg.BatchSize,
		maxAttempts:     cfg.MaxAttempts,
		settle:          repositoryDeletionDefaultSettleInterval,
		acquireInterval: repositoryDeletionOwnerAcquireInterval,
		refreshInterval: repositoryDeletionOwnerRefreshInterval,
	}
	if w.interval <= 0 {
		w.interval = config.DefaultRepositoryDeletionStorageInterval
	}
	if w.batchSize <= 0 {
		w.batchSize = config.DefaultRepositoryDeletionStorageBatchSize
	}
	if w.maxAttempts <= 0 {
		w.maxAttempts = config.DefaultRepositoryDeletionStorageMaxAttempts
	}
	for _, opt := range opts {
		opt(w)
	}
	w.owner = distributed.NewMostlyCorrectOwner(w.log, c.KVStore, repositoryDeletionOwnerPrefix, w.acquireInterval, w.refreshInterval)

	log.WithFields(logging.Fields{
		"interval":     w.interval,
		"batch_size":   w.batchSize,
		"max_attempts": w.maxAttempts,
		"settle":       w.settle,
	}).Info("Starting repository storage deletion worker")
	go w.run(ctx)
}

// run scans immediately and then every interval plus jitter, so replicas do not scan in lockstep.
func (w *repositoryDeletionWorker) run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			w.scan(ctx)
			timer.Reset(w.interval + deletionJitter(w.interval))
		}
	}
}

// scan expires old DONE records, refreshes the gauge, and processes every due record one at a time.
// One record at a time per process bounds the provider request rate without a separate limiter (L3).
func (w *repositoryDeletionWorker) scan(ctx context.Context) {
	log := w.log.WithContext(ctx)
	records, err := w.catalog.ListRepositoryDeletions(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.WithError(err).Error("Failed to list repository storage deletions")
		return
	}

	now := time.Now().UTC()
	states := make(map[RepositoryDeletionState]int)
	due := make([]*RepositoryDeletion, 0, len(records))
	for _, record := range records {
		// R14: the status is readable for 24 hours after the deletion ends.  Anonymous expiry keeps
		// the deletion partition bounded without a separate scheduler.
		if record.State == RepositoryDeletionState_DONE && now.Sub(record.UpdatedAt) > repositoryDeletionRecordExpiry {
			if err := w.catalog.DeleteRepositoryDeletion(ctx, record.Repository); err != nil {
				log.WithError(err).WithField("repository", record.Repository).Warn("Failed to expire a repository deletion record")
				states[record.State]++
				continue
			}
			log.WithField("repository", record.Repository).Info("Expired repository storage deletion record")
			continue
		}
		states[record.State]++
		if w.isDue(record, now) {
			due = append(due, record)
		}
	}
	setRepositoryStorageDeletionsGauge(states)

	for _, record := range due {
		if ctx.Err() != nil {
			return
		}
		w.processRecord(ctx, record)
	}
}

// isDue reports whether the worker should try a record now.
func (w *repositoryDeletionWorker) isDue(record *RepositoryDeletion, now time.Time) bool {
	if record.State != RepositoryDeletionState_PENDING && record.State != RepositoryDeletionState_DELETING {
		return false
	}
	return record.NextAttemptAt.IsZero() || !record.NextAttemptAt.After(now)
}

// processRecord takes mostly-correct ownership of one record and runs its steps.  Ownership is only
// an optimization: every write uses the predicate from this worker's own read, so two owners can at
// worst both delete the same page, and deletes are idempotent (design section 3.2).
func (w *repositoryDeletionWorker) processRecord(ctx context.Context, record *RepositoryDeletion) {
	log := w.log.WithContext(ctx).WithFields(logging.Fields{
		"repository":        record.Repository,
		"storage_namespace": record.StorageNamespace,
		"step":              record.Step.String(),
		"objects_deleted":   record.ObjectsDeleted,
	})

	release, err := w.owner.Own(ctx, w.ownerID, string(RepositoryDeletionKey(record.Repository)))
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.WithError(err).Warn("Failed to take ownership of a repository storage deletion")
		return
	}
	defer release()

	// Re-read under ownership: another owner may have advanced the record while this worker waited.
	record, predicate, err := w.catalog.getRepositoryDeletionWithPredicate(ctx, record.Repository)
	if err != nil {
		if ctx.Err() == nil {
			log.WithError(err).Warn("Failed to read a repository storage deletion record")
		}
		return
	}
	if !w.isDue(record, time.Now().UTC()) {
		return
	}
	log = log.WithFields(logging.Fields{
		"step":            record.Step.String(),
		"objects_deleted": record.ObjectsDeleted,
	})
	log.Info("Repository storage deletion started")

	predicate, err = w.runSteps(ctx, record, predicate, log)
	if err != nil {
		if ctx.Err() != nil {
			log.WithError(err).Info("Repository storage deletion interrupted by shutdown; it will resume")
			return
		}
		// A lost race means another owner advanced the record: this worker drops it for this tick.
		// Counting it as a failure would burn the record's attempts for no reason (design section 3.2).
		if errors.Is(err, kv.ErrPredicateFailed) {
			log.WithError(err).Debug("Another owner advanced the repository storage deletion record; dropping it for this tick")
			return
		}
		w.recordFailure(ctx, record, predicate, err, log)
	}
}

// runSteps advances the record until it is DONE or a step fails.  Every step persists the record
// before it moves on, so a crash at any point resumes from the recorded step and marker (R13).  It
// returns the last predicate it successfully wrote with, so a failure can be recorded without
// losing a race against this worker's own last checkpoint.
func (w *repositoryDeletionWorker) runSteps(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate, log logging.Logger) (kv.Predicate, error) {
	for {
		step := record.Step
		var err error
		switch step {
		case RepositoryDeletionStep_METADATA:
			predicate, err = w.stepMetadata(ctx, record, predicate)
		case RepositoryDeletionStep_SETTLE:
			predicate, err = w.stepSettle(ctx, record, predicate)
		case RepositoryDeletionStep_OBJECTS:
			predicate, err = w.stepObjects(ctx, record, predicate, log)
		case RepositoryDeletionStep_VERIFY:
			predicate, err = w.stepVerify(ctx, record, predicate, log)
		case RepositoryDeletionStep_MARKER:
			predicate, err = w.stepMarker(ctx, record, predicate, log)
		case RepositoryDeletionStep_FINISHED:
			return predicate, w.finish(ctx, record, predicate, log)
		default:
			return predicate, fmt.Errorf("repository %s: unknown step %d: %w", record.Repository, step, ErrInvalidRepositoryDeletionRecord)
		}
		if err != nil {
			return predicate, err
		}
		if record.Step == step {
			return predicate, fmt.Errorf("repository %s: step %s made no progress: %w", record.Repository, step, ErrNamespaceDeleteNoProgress)
		}
	}
}

// stepMetadata retries the metadata delete.  The request path already ran it; this covers a failure
// there, and the restart of a record that never got there.
func (w *repositoryDeletionWorker) stepMetadata(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate) (kv.Predicate, error) {
	record.State = RepositoryDeletionState_DELETING
	err := w.catalog.Store.DeleteRepository(ctx, graveler.RepositoryID(record.Repository), graveler.WithForce(true))
	if err != nil && !errors.Is(err, graveler.ErrNotFound) {
		return predicate, fmt.Errorf("delete repository metadata: %w", err)
	}
	record.Step = RepositoryDeletionStep_SETTLE
	if record.MetadataDeletedAt.IsZero() {
		record.MetadataDeletedAt = time.Now().UTC()
	}
	return w.persist(ctx, record, predicate)
}

// stepSettle waits out the repository cache of the other replicas: one may still answer with the
// deleted repository and accept one more write for the cache expiry.  VERIFY then removes whatever
// that late write added (A5 in the design's risks table).
func (w *repositoryDeletionWorker) stepSettle(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate) (kv.Predicate, error) {
	if !record.MetadataDeletedAt.IsZero() {
		deadline := record.MetadataDeletedAt.Add(w.settle + deletionJitter(w.settle))
		if err := sleepUntil(ctx, time.Until(deadline)); err != nil {
			return predicate, err
		}
	}
	record.Step = RepositoryDeletionStep_OBJECTS
	return w.persist(ctx, record, predicate)
}

// stepObjects walks the namespace once, deleting every page and checkpointing its marker.
func (w *repositoryDeletionWorker) stepObjects(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate, log logging.Logger) (kv.Predicate, error) {
	for {
		after := record.Marker
		result, err := w.deletePage(ctx, record, after, []string{repositoryDeletionDummyKey})
		if err != nil {
			return predicate, err
		}
		w.recordPage(record, result, log)

		if result.Done {
			record.Step = RepositoryDeletionStep_VERIFY
			record.Marker = ""
			return w.persist(ctx, record, predicate)
		}
		if result.LastKey == after {
			return predicate, fmt.Errorf("after %q: %w", after, ErrNamespaceDeleteNoProgress)
		}
		// The marker is checkpointed after every page (R13): a crash redoes at most one page, and
		// deleting an object that is already gone is success.
		record.Marker = result.LastKey
		predicate, err = w.persist(ctx, record, predicate)
		if err != nil {
			return predicate, err
		}
	}
}

// stepVerify makes one full pass from the start with the marker still kept.  Anything it deletes was
// written behind the marker by a replica that still had the repository cached.  Only a pass that
// deletes nothing and reaches the end means the namespace is empty except the marker.
func (w *repositoryDeletionWorker) stepVerify(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate, log logging.Logger) (kv.Predicate, error) {
	for {
		passDeleted, passPredicate, err := w.verifyPass(ctx, record, predicate, log)
		predicate = passPredicate
		if err != nil {
			return predicate, err
		}
		if passDeleted == 0 {
			record.Step = RepositoryDeletionStep_MARKER
			return w.persist(ctx, record, predicate)
		}
		log.WithField("objects_deleted", record.ObjectsDeleted).Info("Repository storage deletion found late writes; verifying again")
	}
}

// verifyPass is one full pass from the start of the namespace.  It returns the number of objects it
// deleted, so the caller can tell an empty namespace from one that is still receiving late writes.
func (w *repositoryDeletionWorker) verifyPass(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate, log logging.Logger) (int, kv.Predicate, error) {
	passDeleted := 0
	after := ""
	for {
		result, err := w.deletePage(ctx, record, after, []string{repositoryDeletionDummyKey})
		if err != nil {
			return passDeleted, predicate, err
		}
		w.recordPage(record, result, log)
		passDeleted += result.Deleted
		if result.Done {
			record.Marker = ""
			predicate, err = w.persist(ctx, record, predicate)
			return passDeleted, predicate, err
		}
		if result.LastKey == after {
			return passDeleted, predicate, fmt.Errorf("verify after %q: %w", after, ErrNamespaceDeleteNoProgress)
		}
		after = result.LastKey
		record.Marker = after
		predicate, err = w.persist(ctx, record, predicate)
		if err != nil {
			return passDeleted, predicate, err
		}
	}
}

// stepMarker deletes what is left with an empty Keep: the marker itself, and on Azure the directory
// blobs.  The marker goes last (R12).
func (w *repositoryDeletionWorker) stepMarker(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate, log logging.Logger) (kv.Predicate, error) {
	for {
		after := record.Marker
		result, err := w.deletePage(ctx, record, after, nil)
		if err != nil {
			return predicate, err
		}
		w.recordPage(record, result, log)
		if result.Done {
			record.Step = RepositoryDeletionStep_FINISHED
			record.Marker = ""
			return w.persist(ctx, record, predicate)
		}
		if result.LastKey == after {
			return predicate, fmt.Errorf("marker after %q: %w", after, ErrNamespaceDeleteNoProgress)
		}
		record.Marker = result.LastKey
		predicate, err = w.persist(ctx, record, predicate)
		if err != nil {
			return predicate, err
		}
	}
}

// finish marks the record DONE, which also releases the name and the namespace for reuse (G5, R16),
// and leaves it readable until it expires.
func (w *repositoryDeletionWorker) finish(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate, log logging.Logger) error {
	record.State = RepositoryDeletionState_DONE
	record.Step = RepositoryDeletionStep_FINISHED
	record.LastError = ""
	if _, err := w.persist(ctx, record, predicate); err != nil {
		return err
	}
	log.WithFields(logging.Fields{
		"step":            record.Step.String(),
		"objects_deleted": record.ObjectsDeleted,
	}).Info("Repository storage deletion finished")
	return nil
}

// deletePage is the one adapter call the worker makes.  Keep is passed through as relative keys: the
// adapter is responsible for never touching a key outside "<namespace>/" (G4).
func (w *repositoryDeletionWorker) deletePage(ctx context.Context, record *RepositoryDeletion, after string, keep []string) (*block.DeleteNamespaceResult, error) {
	result, err := w.catalog.BlockAdapter.DeleteNamespacePage(ctx, record.StorageID, record.StorageNamespace, block.DeleteNamespaceOpts{
		After: after,
		Limit: w.batchSize,
		Keep:  keep,
	})
	if err != nil {
		return nil, fmt.Errorf("delete namespace page of %s after %q: %w", record.StorageNamespace, after, err)
	}
	return result, nil
}

// recordPage adds a page's deletion count to the record and to the counter (R21), and logs it with
// the fields the specification asks for.
func (w *repositoryDeletionWorker) recordPage(record *RepositoryDeletion, result *block.DeleteNamespaceResult, log logging.Logger) {
	record.ObjectsDeleted += int64(result.Deleted)
	recordRepositoryStorageDeletedObjects(result.Deleted)
	log.WithFields(logging.Fields{
		"step":            record.Step.String(),
		"objects_deleted": record.ObjectsDeleted,
		"page_deleted":    result.Deleted,
	}).Debug("Repository storage deletion page deleted")
}

// persist writes the record with the predicate from this worker's own read and returns a fresh
// predicate.  A lost race (kv.ErrPredicateFailed) is returned to the caller, which drops the record
// for this tick: another owner advanced it (design section 3.2).
func (w *repositoryDeletionWorker) persist(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate) (kv.Predicate, error) {
	if err := w.catalog.UpdateRepositoryDeletion(ctx, record, predicate); err != nil {
		return nil, err
	}
	_, fresh, err := w.catalog.getRepositoryDeletionWithPredicate(ctx, record.Repository)
	if err != nil {
		return nil, err
	}
	return fresh, nil
}

// recordFailure applies the R15 retry policy: attempts grows, the error is kept, the next attempt
// backs off up to 10 minutes, and the record ends FAILED at max_attempts.  No error classification is
// needed: transient errors heal within the attempts, and permanent ones (access denied, container
// gone) end in FAILED with the provider's message, which is what L2 asks for.
func (w *repositoryDeletionWorker) recordFailure(ctx context.Context, record *RepositoryDeletion, predicate kv.Predicate, cause error, log logging.Logger) {
	record.Attempts++
	record.LastError = cause.Error()
	record.State = RepositoryDeletionState_DELETING
	record.NextAttemptAt = time.Now().UTC().Add(w.backoff(record.Attempts))
	if int(record.Attempts) >= w.maxAttempts {
		record.State = RepositoryDeletionState_FAILED
	}
	recordRepositoryStorageDeletionError(record.Step)

	if _, err := w.persist(ctx, record, predicate); err != nil {
		if ctx.Err() == nil && !errors.Is(err, kv.ErrPredicateFailed) {
			log.WithError(err).Error("Failed to record a repository storage deletion failure")
		}
	}

	fields := logging.Fields{
		"step":            record.Step.String(),
		"objects_deleted": record.ObjectsDeleted,
		"attempts":        record.Attempts,
		"last_error":      record.LastError,
	}
	if record.State == RepositoryDeletionState_FAILED {
		log.WithError(cause).WithFields(fields).Error("Repository storage deletion failed")
		return
	}
	log.WithError(cause).WithFields(fields).Warn("Repository storage deletion attempt failed; it will be retried")
}

// backoff is min(interval * 2^attempts, 10m).
func (w *repositoryDeletionWorker) backoff(attempts int32) time.Duration {
	if attempts < 1 {
		return 0
	}
	delay := w.interval
	for i := int32(0); i < attempts; i++ {
		if delay >= repositoryDeletionMaxBackoff {
			return repositoryDeletionMaxBackoff
		}
		delay *= 2
	}
	if delay > repositoryDeletionMaxBackoff {
		return repositoryDeletionMaxBackoff
	}
	return delay
}

// sleepUntil sleeps for d or until the context is done.
func sleepUntil(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// deletionJitter returns a random delay in [0, d/3), so replicas that started together do not stay
// in lockstep.  It has no security implications.
func deletionJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	//nolint:gosec,mnd
	return time.Duration(rand.Int63n(int64(d)/3 + 1))
}
