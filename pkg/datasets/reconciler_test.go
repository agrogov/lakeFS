// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// reconciler_test.go covers §12.3 and the observability of §12.4. The fixture is the §15.1 one:
// kv/mem, block/mem, a real catalog and the ACL auth service. Every test injects a crash the way
// a node would leave one - a record in a transient state, a missing tag, a missing version record,
// a missing pin branch, a detached policy - and then runs exactly one repair pass and asserts that
// the state converged, because that is the contract of the reconciler: it is idempotent and it can
// be run at any time.
package datasets

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// reconcilerFor builds a reconciler on the service's own clock, so staleness in the tests uses
// the same clock the service records timestamps with.
func (f *serviceFixture) reconcilerFor() *Reconciler {
	return NewReconciler(f.svc)
}

// leaveInTransientState CASes a dataset record into a transient state as if the operation that
// owned it had crashed: the record is older than datasets.publish_timeout, which is what makes the
// reconciler take it over (§5.4 step 2, §12.3 item 1).
func (f *serviceFixture) leaveInTransientState(t *testing.T, dataset, state string, pendingVersion int) {
	t.Helper()
	current := f.datasetRecord(t, dataset)
	crashed := *current
	crashed.State = state
	crashed.PendingVersion = pendingVersion
	crashed.UpdatedAt = f.svc.now().Add(-f.cfg.PublishTimeout - time.Minute)
	require.NoError(t, f.svc.store.PutDatasetIf(f.ctx, &crashed, current))
}

// reconcile runs one repair pass and requires that this node got the lease and the pass succeeded.
func (f *serviceFixture) reconcile(t *testing.T) *ReconcileReport {
	t.Helper()
	report, err := f.reconcilerFor().Reconcile(f.ctx)
	require.NoError(t, err, "reconcile pass")
	require.NotNil(t, report, "this node should have held the reconcile lease")
	return report
}

// publishFirstVersion creates a dataset with a published v1 from a source repository holding one
// object, and returns the source commit and the dataset view.
func (f *serviceFixture) publishFirstVersion(t *testing.T, dataset, source string) (*DatasetView, graveler.CommitID) {
	t.Helper()
	commit := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "a"})
	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       dataset,
		Definition: prefixDefinition(source, commit.String(), "data", "data/"),
		Publish:    true,
		Message:    "v1",
	})
	require.NoError(t, err, "create dataset %s", dataset)
	require.Equal(t, 1, view.Record.LatestVersion)
	return view, commit
}

// TestReconcile_RebuildsMissingVersionRecordWhenTagExists is §12.3 item 1(a): the tag vN is the
// source of truth (§5.4), so a publish that committed and tagged but crashed before writing the
// VersionRecord converges to "active at vN" with the record rebuilt from the definition object.
func TestReconcile_RebuildsMissingVersionRecordWhenTagExists(t *testing.T) {
	const (
		dataset = "rebuild-from-tag"
		source  = "src-rebuild-tag"
	)
	f := newServiceFixture(t)
	view, _ := f.publishFirstVersion(t, dataset, source)
	require.NotNil(t, view.Version)
	original := view.Version

	// Crash window: the version record was never written, the record still says publishing.
	require.NoError(t, f.svc.store.DeleteVersion(f.ctx, dataset, 1))
	f.leaveInTransientState(t, dataset, DatasetStatePublishing, 1)

	report := f.reconcile(t)
	require.Equal(t, 1, report.VersionsRebuilt)
	require.Equal(t, 1, report.StatesRepaired)
	require.Zero(t, report.Errors)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 1, head.LatestVersion)
	require.Zero(t, head.PendingVersion)

	rebuilt, err := f.svc.store.GetVersion(f.ctx, dataset, 1)
	require.NoError(t, err, "the version record was rebuilt")
	require.Equal(t, original.CommitID, rebuilt.CommitID)
	require.Equal(t, original.MetaRangeID, rebuilt.MetaRangeID)
	require.Equal(t, original.Definition, rebuilt.Definition, "the definition comes from _dataset/definition.json")
	require.Equal(t, original.Message, rebuilt.Message)
	require.Equal(t, original.Publisher, rebuilt.Publisher)
	require.Equal(t, original.Stats, rebuilt.Stats, "stats are recomputed from the version commit")

	// The pass is idempotent: a second one repairs nothing.
	second := f.reconcile(t)
	require.Zero(t, second.StatesRepaired)
	require.Zero(t, second.VersionsRebuilt)
	require.Zero(t, second.Errors)
}

// TestReconcile_CreatesMissingTagFromHead is §12.3 item 1(b): the commit exists and the latest HEAD
// carries dataset.version=N, so the missing tag is created and the version record rebuilt from it.
func TestReconcile_CreatesMissingTagFromHead(t *testing.T) {
	const (
		dataset = "rebuild-from-head"
		source  = "src-rebuild-head"
	)
	f := newServiceFixture(t)
	view, _ := f.publishFirstVersion(t, dataset, source)
	commitID := view.Version.CommitID

	// Crash window: commit landed, tag and record did not.
	require.NoError(t, f.cat.DeleteTag(f.ctx, dataset, VersionTag(1), graveler.WithForce(true)))
	require.NoError(t, f.svc.store.DeleteVersion(f.ctx, dataset, 1))
	f.leaveInTransientState(t, dataset, DatasetStatePublishing, 1)

	// The HEAD carries dataset.version=1, so the reconciler must trust it.
	head, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err)
	require.Equal(t, commitID, head)
	commitLog, err := f.cat.GetCommit(f.ctx, dataset, head)
	require.NoError(t, err)
	require.Equal(t, "1", commitLog.Metadata[CommitMetadataVersionKey])

	report := f.reconcile(t)
	require.Equal(t, 1, report.VersionsRebuilt)
	require.Equal(t, 1, report.StatesRepaired)

	tagCommit, err := f.cat.GetTag(f.ctx, dataset, VersionTag(1))
	require.NoError(t, err, "the missing tag was created")
	require.Equal(t, commitID, tagCommit)

	rebuilt, err := f.svc.store.GetVersion(f.ctx, dataset, 1)
	require.NoError(t, err)
	require.Equal(t, commitID, rebuilt.CommitID)

	head2 := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head2.State)
	require.Equal(t, 1, head2.LatestVersion)
}

// TestReconcile_RollsBackPublishWithoutWork is §12.3 item 1(c): neither the tag nor a HEAD commit
// carrying dataset.version=N exists, so the pins acquired for N are released and the dataset goes
// back to active without a new version.
func TestReconcile_RollsBackPublishWithoutWork(t *testing.T) {
	const (
		dataset = "rollback-empty-publish"
		source  = "src-rollback"
	)
	f := newServiceFixture(t)
	_, _ = f.publishFirstVersion(t, dataset, source)

	// A second, unpinned source commit stands in for the publish that never got anywhere.
	second := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "b"})
	secondDef := prefixDefinition(source, second.String(), "data", "data/")
	_, err := f.svc.pins.Acquire(f.ctx, secondDef, PinReference(dataset, 2))
	require.NoError(t, err, "pins for the interrupted v2 were acquired before the crash")

	f.leaveInTransientState(t, dataset, DatasetStatePublishing, 2)

	report := f.reconcile(t)
	require.Equal(t, 1, report.StatesRepaired)
	require.Zero(t, report.VersionsRebuilt)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 1, head.LatestVersion, "the failed publish did not advance the head")
	require.Zero(t, head.PendingVersion)

	_, err = f.svc.store.GetVersion(f.ctx, dataset, 2)
	require.ErrorIs(t, err, kv.ErrNotFound, "no v2 record was invented")

	// The pins acquired for v2 were released, and their branch is gone because nothing else
	// referenced that commit.
	_, err = f.svc.store.GetPin(f.ctx, source, second.String())
	require.ErrorIs(t, err, kv.ErrNotFound)
	_, err = f.cat.GetBranchReference(f.ctx, source, PinBranch(second.String()))
	require.ErrorIs(t, err, graveler.ErrBranchNotFound)
}

// TestReconcile_FinishesCrashedDelete is §12.3 item 1 for "deleting": a delete that got as far as
// removing the repository is finished, not retried forever, and nothing of the dataset is left in
// KV.
func TestReconcile_FinishesCrashedDelete(t *testing.T) {
	const (
		dataset = "crashed-delete"
		source  = "src-crashed-delete"
	)
	f := newServiceFixture(t)
	_, sourceCommit := f.publishFirstVersion(t, dataset, source)

	// Crash window: the repository is gone, the record still says deleting.
	require.NoError(t, f.cat.DeleteRepository(f.ctx, dataset, graveler.WithForce(true)))
	f.leaveInTransientState(t, dataset, DatasetStateDeleting, 0)

	report := f.reconcile(t)
	require.Equal(t, 1, report.StatesRepaired)
	require.Zero(t, report.Errors)

	_, err := f.svc.store.GetDataset(f.ctx, dataset)
	require.ErrorIs(t, err, kv.ErrNotFound, "the crashed delete was finished")

	versions, err := f.svc.store.ListVersions(f.ctx, dataset)
	require.NoError(t, err)
	require.Empty(t, versions)

	_, err = f.svc.store.GetPin(f.ctx, source, sourceCommit.String())
	require.ErrorIs(t, err, kv.ErrNotFound, "the pins of the deleted dataset were released")
}

// TestReconcile_RecreatesMissingPinBranch is §12.3 item 2: a pin branch deleted by hand is
// re-created (and reported as a WARN with a metric).
func TestReconcile_RecreatesMissingPinBranch(t *testing.T) {
	const (
		dataset = "missing-pin"
		source  = "src-missing-pin"
	)
	f := newServiceFixture(t)
	_, sourceCommit := f.publishFirstVersion(t, dataset, source)

	// Somebody deleted the hidden pin branch (§8.2 risks).
	require.NoError(t, f.cat.DeleteBranch(f.ctx, source, PinBranch(sourceCommit.String()), graveler.WithForce(true)))
	_, err := f.cat.GetBranchReference(f.ctx, source, PinBranch(sourceCommit.String()))
	require.ErrorIs(t, err, graveler.ErrBranchNotFound)

	report := f.reconcile(t)
	require.Equal(t, 1, report.PinsRecreated)

	repaired, err := f.cat.GetBranchReference(f.ctx, source, PinBranch(sourceCommit.String()))
	require.NoError(t, err, "the pin branch was re-created")
	require.Equal(t, sourceCommit.String(), repaired)

	// The reference count is untouched: recreating a branch is not a new reference.
	pin, err := f.svc.store.GetPin(f.ctx, source, sourceCommit.String())
	require.NoError(t, err)
	require.Equal(t, []string{PinReference(dataset, 1)}, pin.References)
}

// TestReconcile_RemovesOrphanPin is §12.3 item 2: a pin that no version references is removed with
// its branch.
func TestReconcile_RemovesOrphanPin(t *testing.T) {
	const source = "src-orphan-pin"
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})

	// A pin for a dataset that does not exist: exactly what a dataset delete that failed after
	// releasing the reference set but before deleting the branch could leave behind.
	_, err := f.svc.pins.Acquire(f.ctx, objectDefinition(source, commit.String(), "data", "data/a.parquet"),
		PinReference("ghost-dataset", 3))
	require.NoError(t, err)

	report := f.reconcile(t)
	require.Equal(t, 1, report.PinsRemoved)

	_, err = f.svc.store.GetPin(f.ctx, source, commit.String())
	require.ErrorIs(t, err, kv.ErrNotFound, "the orphan pin record was removed")
	_, err = f.cat.GetBranchReference(f.ctx, source, PinBranch(commit.String()))
	require.ErrorIs(t, err, graveler.ErrBranchNotFound, "the orphan pin branch was removed")
}

// TestReconcile_ReattachesDetachedPolicies is §12.3 item 3: the immutability guard and the managed
// consumer read policy are re-created and re-attached when they were detached by hand (§9.4, §9.2).
func TestReconcile_ReattachesDetachedPolicies(t *testing.T) {
	const (
		dataset  = "detached-policies"
		source   = "src-detached-policies"
		consumer = "dataset-consumer"
	)
	f := newServiceFixture(t)
	_, _ = f.publishFirstVersion(t, dataset, source)
	f.addUser(t, consumer)
	_, err := f.svc.GrantAccess(f.ctx, f.caller(), dataset, PrincipalTypeUser, consumer)
	require.NoError(t, err)

	// Detach the immutability guard from a base group, and remove the consumer policy entirely.
	require.NoError(t, f.authService.DetachPolicyFromGroup(f.ctx, ImmutabilityPolicyName(dataset), "Admins"))
	require.NoError(t, f.authService.DetachPolicyFromUser(f.ctx, ConsumerReadPolicyName(dataset), consumer))
	require.NoError(t, f.authService.DeletePolicy(f.ctx, ConsumerReadPolicyName(dataset)))

	report := f.reconcile(t)
	require.GreaterOrEqual(t, report.PoliciesReattached, 2)
	require.Zero(t, report.Errors)

	requirePolicyAttachedToGroup(t, f, "Admins", ImmutabilityPolicyName(dataset))
	requirePolicyAttachedToUser(t, f, consumer, ConsumerReadPolicyName(dataset))

	// §9.5 note: the installation-wide managed read policy lists the dataset again.
	policy, err := f.authService.GetPolicy(f.ctx, DatasetsAllReadPolicyName)
	require.NoError(t, err)
	resources := make([]string, 0, len(policy.Statement))
	for _, statement := range policy.Statement {
		resources = append(resources, statement.Resource)
	}
	require.Contains(t, resources, permissions.RepoArn(dataset))
}

// TestReconcile_DegradedFollowsSourceReachability is §12.3 item 4: a dataset whose pinned source
// repository disappeared is marked degraded, and a degraded dataset whose sources are reachable is
// cleared again.
func TestReconcile_DegradedFollowsSourceReachability(t *testing.T) {
	const (
		goneDataset = "degraded-source-gone"
		backDataset = "degraded-source-back"
		goneSource  = "src-degraded-gone"
		backSource  = "src-degraded-back"
	)
	f := newServiceFixture(t)
	_, _ = f.publishFirstVersion(t, goneDataset, goneSource)
	_, _ = f.publishFirstVersion(t, backDataset, backSource)

	// The source of one dataset is deleted: its pins die with it (§8.2 risks).
	require.NoError(t, f.cat.DeleteRepository(f.ctx, goneSource, graveler.WithForce(true)))

	// The other dataset carries a degraded mark from an earlier incident; its source is back.
	current := f.datasetRecord(t, backDataset)
	marked := *current
	marked.State = DatasetStateDegraded
	marked.UpdatedAt = f.svc.now().Add(-time.Hour)
	require.NoError(t, f.svc.store.PutDatasetIf(f.ctx, &marked, current))

	report := f.reconcile(t)
	require.Equal(t, 1, report.Degraded)
	require.Equal(t, 1, report.Recovered)

	require.Equal(t, DatasetStateDegraded, f.datasetRecord(t, goneDataset).State)
	require.Equal(t, DatasetStateActive, f.datasetRecord(t, backDataset).State)

	// A second pass is stable: the degraded dataset stays degraded, the recovered one does not
	// flap back.
	second := f.reconcile(t)
	require.Zero(t, second.Degraded)
	require.Zero(t, second.Recovered)
	require.Equal(t, DatasetStateDegraded, f.datasetRecord(t, goneDataset).State)
	require.Equal(t, DatasetStateActive, f.datasetRecord(t, backDataset).State)
}

// TestReconcile_LeaseSerialisesPasses pins the lease of §12.3: while one reconciler runs a pass, a
// second one does not, and the lease is released when the first pass ends.
func TestReconcile_LeaseSerialisesPasses(t *testing.T) {
	f := newServiceFixture(t)
	_, _ = f.publishFirstVersion(t, "lease-dataset", "src-lease")

	first := f.reconcilerFor()
	entered := make(chan struct{})
	release := make(chan struct{})
	first.onPass = func() {
		close(entered)
		<-release
	}

	type passResult struct {
		report *ReconcileReport
		err    error
	}
	finished := make(chan passResult, 1)
	go func() {
		report, err := first.Reconcile(f.ctx)
		finished <- passResult{report: report, err: err}
	}()

	<-entered // the first pass holds the lease

	second := f.reconcilerFor()
	require.NotEqual(t, first.owner, second.owner)
	report, err := second.Reconcile(f.ctx)
	require.NoError(t, err)
	require.Nil(t, report, "a second reconciler must not run a pass while the lease is held")

	close(release)
	got := <-finished
	require.NoError(t, got.err)
	require.NotNil(t, got.report)

	// The first pass released the lease, so the second one can run now.
	report, err = second.Reconcile(f.ctx)
	require.NoError(t, err)
	require.NotNil(t, report)
}

// TestReconcile_LeaseExpiryAllowsTakeover pins the crash case of the lease: a stale lease (an owner
// that died without releasing) is taken over, while a live one is not.
func TestReconcile_LeaseExpiryAllowsTakeover(t *testing.T) {
	f := newServiceFixture(t)
	now := time.Now().UTC()

	acquired, err := f.svc.store.AcquireLease(f.ctx, "dead-node", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	acquired, err = f.svc.store.AcquireLease(f.ctx, "other-node", time.Minute)
	require.NoError(t, err)
	require.False(t, acquired, "a live lease is not taken over")

	// Age the lease past its expiry: the dead node never released it.
	expired, err := leaseJSON(leaseRecord{Owner: "dead-node", ExpiresAt: now.Add(-time.Minute)})
	require.NoError(t, err)
	require.NoError(t, f.svc.store.put(f.ctx, []byte(ReconcileLeaseKey), expired))

	acquired, err = f.svc.store.AcquireLease(f.ctx, "other-node", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired, "an expired lease is taken over")

	// Releasing as the wrong owner is a no-op: the live owner keeps it.
	require.NoError(t, f.svc.store.ReleaseLease(f.ctx, "dead-node"))
	acquired, err = f.svc.store.AcquireLease(f.ctx, "third-node", time.Minute)
	require.NoError(t, err)
	require.False(t, acquired)

	require.NoError(t, f.svc.store.ReleaseLease(f.ctx, "other-node"))
	acquired, err = f.svc.store.AcquireLease(f.ctx, "third-node", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired, "the owner's release frees the lease")
}

// TestReconcilerRun_RunsPassesAndExitsOnCancel pins the background loop of §12.3: it runs a pass
// on every interval and returns when its context is cancelled.
func TestReconcilerRun_RunsPassesAndExitsOnCancel(t *testing.T) {
	f := newServiceFixture(t)
	_, _ = f.publishFirstVersion(t, "run-dataset", "src-run")

	r := f.reconcilerFor()
	r.interval = time.Millisecond
	ran := make(chan struct{}, 1)
	r.onPass = func() {
		select {
		case ran <- struct{}{}:
		default:
		}
	}

	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx)
	}()

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the reconciler loop did not run a pass")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reconciler loop did not exit after cancellation")
	}
}

// TestDatasetMetrics_RegistrationIsIdempotent pins §12.4: a second registration of the metric set
// must not panic (promauto/MustRegister would), and the collector in use is the one already there.
func TestDatasetMetrics_RegistrationIsIdempotent(t *testing.T) {
	reg := prometheus.NewRegistry()
	first := newDatasetMetrics(reg)
	second := newDatasetMetrics(reg) // AlreadyRegisteredError; must not panic

	require.Same(t, first.publishDuration, second.publishDuration)
	require.Same(t, first.publishObjects, second.publishObjects)
	require.Same(t, first.datasetsTotal, second.datasetsTotal)
	require.Same(t, first.pinsTotal, second.pinsTotal)
	require.Same(t, first.reconcileErrors, second.reconcileErrors)
	require.Same(t, first.pinsRecreated, second.pinsRecreated)

	// The process-wide set is registered against the default registry at init; registering it
	// again (a wrapper binary importing the package twice, a reload in a test) is also a no-op.
	require.NotPanics(t, func() { newDatasetMetrics(prometheus.DefaultRegisterer) })
	require.NotNil(t, dsMetrics.publishDuration)

	// HistogramVec families only appear after a label combination is observed.
	first.publishDuration.WithLabelValues(publishResultSuccess).Observe(1)
	first.publishObjects.Observe(1)

	families, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	for _, want := range []string{
		"datasets_publish_duration_seconds",
		"datasets_publish_objects",
		"datasets_total",
		"datasets_pins_total",
		"datasets_reconcile_errors_total",
		"datasets_pins_recreated_total",
	} {
		require.Contains(t, names, want)
	}
}

// --- helpers ---

// leaseJSON encodes a lease record the way AcquireLease does.
func leaseJSON(rec leaseRecord) ([]byte, error) {
	return json.Marshal(rec)
}

// requirePolicyAttachedToGroup asserts that policy is attached to group.
func requirePolicyAttachedToGroup(t *testing.T, f *serviceFixture, group, policy string) {
	t.Helper()
	policies, _, err := f.authService.ListGroupPolicies(f.ctx, group, &model.PaginationParams{Amount: MaxListAmount})
	require.NoError(t, err)
	names := make([]string, 0, len(policies))
	for _, p := range policies {
		names = append(names, p.DisplayName)
	}
	require.Contains(t, names, policy)
}

// requirePolicyAttachedToUser asserts that policy is attached to user.
func requirePolicyAttachedToUser(t *testing.T, f *serviceFixture, user, policy string) {
	t.Helper()
	policies, _, err := f.authService.ListUserPolicies(f.ctx, user, &model.PaginationParams{Amount: MaxListAmount})
	require.NoError(t, err)
	names := make([]string, 0, len(policies))
	for _, p := range policies {
		names = append(names, p.DisplayName)
	}
	require.Contains(t, names, policy)
}
