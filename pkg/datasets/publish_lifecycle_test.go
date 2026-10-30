package datasets

// publish_lifecycle_test.go covers the two publish-lifecycle failures of the review's Part C:
//
//   - BUG-2: a KV failure after the commit and tag exist must never release the version's GC
//     pins nor roll the record back. The writes are retried, and if they still fail the record
//     stays in publishing(PendingVersion=N) for the reconciler to finish from the tag. A retried
//     publish adopts a version that was already committed and tagged.
//   - BUG-3: a publish that runs longer than datasets.publish_timeout heartbeats its record so
//     the reconciler does not take it over, and re-verifies ownership before the commit and the
//     tag so a publish that did lose the record aborts without committing.

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// flakyVersionStore fails SetIf for version records, which is how the BUG-2 tests inject a KV
// failure in the window between the commit/tag and the VersionRecord write. failures < 0 means
// "fail every write"; allowWrites restores the store.
type flakyVersionStore struct {
	kv.Store

	mu       sync.Mutex
	failures int
}

func (s *flakyVersionStore) SetIf(ctx context.Context, partitionKey, key, value []byte, predicate kv.Predicate) error {
	// Version records live at versions/<dataset>/<n;010d>, so the prefix is enough to fail
	// only the write under test.
	if bytes.HasPrefix(key, []byte(versionKeyPrefix+kv.PathDelimiter)) {
		s.mu.Lock()
		fail := s.failures != 0
		if s.failures > 0 {
			s.failures--
		}
		s.mu.Unlock()
		if fail {
			return errors.New("injected version write failure")
		}
	}
	return s.Store.SetIf(ctx, partitionKey, key, value, predicate)
}

func (s *flakyVersionStore) allowWrites() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = 0
}

// fakeClock is the replaceable service clock of the BUG-3 tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// pinReferencesOf returns the reference sets of a dataset's pins, keyed by the pinned commit.
func (f *serviceFixture) pinReferencesOf(t *testing.T, dataset string) map[string][]string {
	t.Helper()
	pins, err := f.svc.store.ListDatasetPins(f.ctx, dataset)
	require.NoError(t, err, "list pins of %s", dataset)
	refs := make(map[string][]string, len(pins))
	for _, pin := range pins {
		refs[pin.CommitID] = pin.References
	}
	return refs
}

// TestPublishVersion_PutVersionFailureAfterCommitIsRetried is BUG-2's main case: the commit and
// tag v2 exist, the first VersionRecord write fails, and the retry finishes the publish. The pins
// of v2 must never be released, and the next publish must be v3.
func TestPublishVersion_PutVersionFailureAfterCommitIsRetried(t *testing.T) {
	const (
		dataset = "post-commit-retry"
		source  = "src-post-commit-retry"
	)
	f := newServiceFixture(t)
	f.svc.retryDelay = 0

	first := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "a"})
	f.createEmptyDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))
	v1 := f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, first.String(), "data", "data/"),
		Message:    "v1",
	})
	require.Equal(t, 1, v1.Version)

	second := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "b"})
	flaky := &flakyVersionStore{Store: f.kvStore, failures: 1}
	f.svc.store = NewStore(flaky)

	v2 := f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, second.String(), "data", "data/"),
		Message:    "v2",
	})
	require.Equal(t, 2, v2.Version, "the one-shot failure is retried, not surfaced")

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 2, head.LatestVersion)
	require.Zero(t, head.PendingVersion)

	// The version record is there, and its tag points at its commit.
	stored, err := f.svc.store.GetVersion(f.ctx, dataset, 2)
	require.NoError(t, err)
	require.Equal(t, v2.CommitID, stored.CommitID)
	tagCommit, err := f.tagCommit(t, dataset, VersionTag(2))
	require.NoError(t, err)
	require.Equal(t, v2.CommitID, tagCommit)

	// BUG-2: the pins of v2 were never released.
	require.Equal(t, map[string][]string{
		first.String():  {PinReference(dataset, 1)},
		second.String(): {PinReference(dataset, 2)},
	}, f.pinReferencesOf(t, dataset))

	// The dataset is not wedged: the next publish is v3, not another attempt at v2.
	third := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "c"})
	v3 := f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, third.String(), "data", "data/"),
		Message:    "v3",
	})
	require.Equal(t, 3, v3.Version)
	require.Equal(t, 3, f.datasetRecord(t, dataset).LatestVersion)
}

// TestPublishVersion_PersistentVersionWriteFailureIsFinishedByReconcile is the other half of
// BUG-2: when the retries are exhausted the record stays publishing(PendingVersion=N) with its
// pins, and the reconciler finishes the version from tag vN. A later publish is N+1.
func TestPublishVersion_PersistentVersionWriteFailureIsFinishedByReconcile(t *testing.T) {
	const (
		dataset = "post-commit-reconcile"
		source  = "src-post-commit-reconcile"
	)
	f := newServiceFixture(t)
	f.svc.retryDelay = 0

	first := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "a"})
	f.createEmptyDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))
	require.Equal(t, 1, f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, first.String(), "data", "data/"),
		Message:    "v1",
	}).Version)

	second := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "b"})
	def := prefixDefinition(source, second.String(), "data", "data/")
	flaky := &flakyVersionStore{Store: f.kvStore, failures: -1}
	f.svc.store = NewStore(flaky)

	_, err := f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{Definition: def, Message: "v2"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected version write failure")

	// BUG-2: the commit and tag are the source of truth. The record is left in publishing(N),
	// the pins of N are intact and the version record is the only thing missing.
	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStatePublishing, head.State)
	require.Equal(t, 2, head.PendingVersion)
	require.Equal(t, 1, head.LatestVersion)

	tagCommit, err := f.tagCommit(t, dataset, VersionTag(2))
	require.NoError(t, err, "the tag of v2 exists even though the record write failed")
	require.Equal(t, tagCommit, f.latestHead(t, dataset))
	_, err = f.svc.store.GetVersion(f.ctx, dataset, 2)
	require.ErrorIs(t, err, kv.ErrNotFound)

	require.Equal(t, map[string][]string{
		first.String():  {PinReference(dataset, 1)},
		second.String(): {PinReference(dataset, 2)},
	}, f.pinReferencesOf(t, dataset), "a failure after the commit must not release the pins")

	// Age the record past publish_timeout, as a crashed publish leaves it, and let the
	// reconciler finish the version from the tag.
	f.leaveInTransientState(t, dataset, DatasetStatePublishing, 2)
	flaky.allowWrites()

	report := f.reconcile(t)
	require.Equal(t, 1, report.VersionsRebuilt)
	require.Equal(t, 1, report.StatesRepaired)

	head = f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 2, head.LatestVersion)
	require.Zero(t, head.PendingVersion)

	rebuilt, err := f.svc.store.GetVersion(f.ctx, dataset, 2)
	require.NoError(t, err)
	require.Equal(t, tagCommit, rebuilt.CommitID)

	// Still not wedged, and the pins survived the repair.
	third := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "c"})
	v3 := f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, third.String(), "data", "data/"),
		Message:    "v3",
	})
	require.Equal(t, 3, v3.Version)
}

// TestPublishVersion_AdoptsAlreadyCommittedVersion is BUG-2's beginPublish half: when tag vN
// already points at a commit carrying dataset.version=N, the next publish adopts that version
// instead of failing on the tag, and the following publish is N+1.
func TestPublishVersion_AdoptsAlreadyCommittedVersion(t *testing.T) {
	const (
		dataset = "adopt"
		source  = "src-adopt"
	)
	f := newServiceFixture(t)
	f.svc.retryDelay = 0

	first := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "a"})
	f.createEmptyDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))
	require.Equal(t, 1, f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, first.String(), "data", "data/"),
		Message:    "v1",
	}).Version)

	v2Commit := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "b"})
	def := prefixDefinition(source, v2Commit.String(), "data", "data/")

	// Simulate the wedge: commit and tag v2 exist, the record and version record were rolled
	// back (the pre-BUG-2 failure mode), so the publish agent has to repair them.
	require.NoError(t, f.svc.validateDefinition(f.ctx, &def))
	resolved, err := f.svc.resolver.Resolve(f.ctx, &def)
	require.NoError(t, err)
	result, err := (&Materializer{Catalog: f.cat}).Publish(f.ctx, PublishRequest{
		Dataset: dataset, Version: 2, Definition: *resolved, Message: "v2", Publisher: testServiceUser,
	})
	require.NoError(t, err)
	require.Equal(t, VersionTag(2), result.Tag)

	_, err = f.svc.store.GetVersion(f.ctx, dataset, 2)
	require.ErrorIs(t, err, kv.ErrNotFound, "the version record is the missing piece")

	adopted := f.publishVersion(t, dataset, PublishVersionRequest{Definition: def, Message: "v2 retry"})
	require.Equal(t, 2, adopted.Version)
	require.Equal(t, result.CommitID.String(), adopted.CommitID)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 2, head.LatestVersion)

	// The next publish is v3, so the dataset is not wedged on tag v2.
	third := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "c"})
	v3 := f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, third.String(), "data", "data/"),
		Message:    "v3",
	})
	require.Equal(t, 3, v3.Version)
}

// TestPublishVersion_TakenOverPublishAbortsWithoutCommitting is BUG-3: a publish blocked past
// publish_timeout is taken over by the reconciler; when it finally reaches the commit, the
// ownership CAS fails, so it aborts instead of committing a version it no longer owns. The pins
// that still serve v1 survive.
func TestPublishVersion_TakenOverPublishAbortsWithoutCommitting(t *testing.T) {
	const (
		dataset = "long-publish"
		source  = "src-long-publish"
	)
	f := newServiceFixture(t)
	clock := &fakeClock{now: time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)}
	f.svc.now = clock.Now
	// Keep the heartbeat quiet: this test is about the guard, not about the refresh keeping the
	// record alive. The next test covers the heartbeat itself.
	f.svc.heartbeatInterval = time.Hour

	sourceCommit := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "a"})
	f.createEmptyDataset(t, dataset, prefixDefinition(source, sourceCommit.String(), "data", "data/"))
	v1 := f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, sourceCommit.String(), "data", "data/"),
		Message:    "v1",
	})
	require.Equal(t, 1, v1.Version)

	// v2 pins the same source commit (so it shares v1's pin) but has a different definition.
	def := prefixDefinition(source, sourceCommit.String(), "copy", "data/")

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	f.svc.publishFn = func(ctx context.Context, materializer *Materializer, req PublishRequest) (*PublishResult, error) {
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return materializer.Publish(ctx, req)
	}

	type publishResult struct {
		outcome *PublishOutcome
		err     error
	}
	done := make(chan publishResult, 1)
	go func() {
		outcome, err := f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{
			Definition: def,
			Message:    "v2",
		})
		done <- publishResult{outcome: outcome, err: err}
	}()
	<-entered

	// The publish is past its pins and its CAS to publishing, and is now stuck in
	// materialisation.
	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStatePublishing, head.State)
	require.Equal(t, 2, head.PendingVersion)

	// Time passes past publish_timeout without a heartbeat, so the reconciler legitimately
	// takes the record over and rolls it back.
	clock.Advance(f.cfg.PublishTimeout + time.Minute)
	report := f.reconcile(t)
	require.Equal(t, 1, report.StatesRepaired)
	require.Equal(t, DatasetStateActive, f.datasetRecord(t, dataset).State)

	close(release)
	got := <-done
	require.Error(t, got.err, "the taken-over publish must abort")
	require.ErrorIs(t, got.err, errPublishLeaseLost)
	require.ErrorIs(t, got.err, ErrDatasetConflict, "the HTTP layer answers 409")

	// Nothing was committed: no v2 tag, no v2 version record and latest is still v1.
	_, err := f.tagCommit(t, dataset, VersionTag(2))
	require.ErrorIs(t, err, graveler.ErrNotFound)
	require.Equal(t, v1.CommitID, f.latestHead(t, dataset))
	_, err = f.svc.store.GetVersion(f.ctx, dataset, 2)
	require.ErrorIs(t, err, kv.ErrNotFound)

	head = f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 1, head.LatestVersion)

	// The pins that serve v1 survived the takeover: the aborted publish did not release them.
	require.Equal(t, map[string][]string{
		sourceCommit.String(): {PinReference(dataset, 1)},
	}, f.pinReferencesOf(t, dataset))
	_, err = f.cat.GetBranchReference(f.ctx, source, PinBranch(sourceCommit.String()))
	require.NoError(t, err, "the pin branch of the surviving version is intact")
}

// TestPublishHeartbeat_KeepsTheRecordFresh is BUG-3's other half: the heartbeat CASes UpdatedAt
// forward, so a publish slower than publish_timeout is not mistaken for a crashed one.
func TestPublishHeartbeat_KeepsTheRecordFresh(t *testing.T) {
	const (
		dataset = "heartbeat"
		source  = "src-heartbeat"
	)
	f := newServiceFixture(t)
	clock := &fakeClock{now: time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)}
	f.svc.now = clock.Now
	f.svc.heartbeatInterval = time.Hour

	sourceCommit := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "a"})
	f.createEmptyDataset(t, dataset, prefixDefinition(source, sourceCommit.String(), "data", "data/"))
	require.Equal(t, 1, f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, sourceCommit.String(), "data", "data/"),
		Message:    "v1",
	}).Version)

	// Leave the record exactly as a running publish has it: publishing v2, old UpdatedAt.
	current := f.datasetRecord(t, dataset)
	_, currentPredicate, err := f.svc.store.GetDataset(f.ctx, dataset)
	require.NoError(t, err)
	running := *current
	running.State = DatasetStatePublishing
	running.PendingVersion = 2
	running.UpdatedAt = clock.Now()
	runningPredicate, err := f.svc.store.PutDatasetIf(f.ctx, &running, currentPredicate)
	require.NoError(t, err)

	lease, _ := f.svc.newPublishLease(f.ctx, &running, runningPredicate)
	defer lease.stop()

	clock.Advance(f.cfg.PublishTimeout + time.Minute)
	require.NoError(t, lease.check(f.ctx), "the heartbeat re-verifies ownership with a CAS")

	report := f.reconcile(t)
	require.Zero(t, report.StatesRepaired, "a fresh heartbeat keeps the reconciler away")
	require.Equal(t, DatasetStatePublishing, f.datasetRecord(t, dataset).State,
		"the live publish still owns the record")
}
