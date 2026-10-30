package datasets

// versions_test.go covers publishing versions through the control-plane service (DATASETS_DESIGN.md
// §6.2 steps 1-12, §5.4 concurrency and §6.5 asynchronous publishes) and the two version reads of
// §10.1. The fixture is the §15.1 one: a real catalog on kv/mem + block/mem, and the ACL auth
// service that the lakefs-sso binary runs.
//
// Assertions are deliberately made against both planes: the control plane (dataset record, version
// records, task records) and the data plane (the `vN` tags and the `latest` branch head of the
// dataset repository), because §5.4 makes the tag the source of truth that the KV record has to
// agree with.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// publishVersion publishes req through the service and fails the test on error, asserting that
// the publish ran synchronously.
func (f *serviceFixture) publishVersion(t *testing.T, name string, req PublishVersionRequest) *VersionRecord {
	t.Helper()
	outcome, err := f.svc.PublishVersion(f.ctx, f.caller(), name, req)
	require.NoError(t, err, "publish %s", name)
	require.False(t, outcome.Accepted(), "publish %s ran asynchronously", name)
	require.NotNil(t, outcome.Version)
	return outcome.Version
}

// createEmptyDataset creates a dataset with no published version, the starting point of the
// publish tests that build v1..vN explicitly.
func (f *serviceFixture) createEmptyDataset(t *testing.T, name string, def Definition) {
	t.Helper()
	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: name, Definition: def})
	require.NoError(t, err, "create dataset %s", name)
	require.Equal(t, DatasetStateActive, view.Record.State)
	require.Zero(t, view.Record.LatestVersion)
}

// commitTo creates the source repository on first use and commits objects onto branch, returning
// the new commit. Every commit writes fresh physical addresses, so the same logical path can hold
// different bytes in two commits — which is what makes "v1 still reads v1 data" meaningful.
func (f *serviceFixture) commitTo(t *testing.T, repo, branch string, objects map[string]string) graveler.CommitID {
	t.Helper()
	if _, err := f.cat.GetRepository(f.ctx, repo); err != nil {
		f.createRepository(t, repo, branch, false)
	}
	items := make([]testObject, 0, len(objects))
	for _, key := range sortedKeys(objects) {
		items = append(items, testObject{key: key, content: []byte(objects[key])})
	}
	return f.commitObjects(t, repo, branch, items...)
}

// datasetRecord reads the current head record of a dataset.
func (f *serviceFixture) datasetRecord(t *testing.T, name string) *DatasetRecord {
	t.Helper()
	rec, err := f.svc.store.GetDataset(f.ctx, name)
	require.NoError(t, err, "get dataset record %s", name)
	return rec
}

// latestHead returns the commit the dataset repository's "latest" branch points at.
func (f *serviceFixture) latestHead(t *testing.T, dataset string) string {
	t.Helper()
	head, err := f.cat.GetBranchReference(f.ctx, dataset, DefaultBranch)
	require.NoError(t, err, "get %s/%s", dataset, DefaultBranch)
	return head
}

// tagCommit returns the commit of a version tag, or graveler.ErrNotFound when it is absent.
func (f *serviceFixture) tagCommit(t *testing.T, dataset, tag string) (string, error) {
	t.Helper()
	return f.cat.GetTag(f.ctx, dataset, tag)
}

// TestPublishVersion_ThreeVersionsPushLatest is the v1 -> v2 -> v3 case of the card: every publish
// creates its tag, moves the `latest` HEAD, records the version and keeps the older versions
// readable at their own tags (§5.4, §6.2 steps 9-11).
func TestPublishVersion_ThreeVersionsPushLatest(t *testing.T) {
	const (
		dataset = "three-versions"
		source  = "src-three"
	)
	f := newServiceFixture(t)
	commitOne := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "v1"})
	f.createEmptyDataset(t, dataset, prefixDefinition(source, commitOne.String(), "data", "data/"))

	commits := map[int]graveler.CommitID{1: commitOne}
	commits[2] = f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "v2"})
	commits[3] = f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "v3"})

	heads := make([]string, 0, 3)
	for version := 1; version <= 3; version++ {
		rec := f.publishVersion(t, dataset, PublishVersionRequest{
			Definition: prefixDefinition(source, commits[version].String(), "data", "data/"),
			Message:    fmt.Sprintf("version %d", version),
		})
		require.Equal(t, version, rec.Version)
		require.Equal(t, fmt.Sprintf("version %d", version), rec.Message)
		require.Equal(t, testServiceUser, rec.Publisher)
		require.Equal(t, int64(1), rec.Stats.Objects)

		// §6.2 step 10: the tag exists and names the version's commit.
		tagCommit, err := f.tagCommit(t, dataset, VersionTag(version))
		require.NoError(t, err, "tag %s", VersionTag(version))
		require.Equal(t, rec.CommitID, tagCommit)

		// §5.4: latest HEAD advances to the newest version.
		require.Equal(t, rec.CommitID, f.latestHead(t, dataset))
		heads = append(heads, rec.CommitID)

		// §6.4: every version is self-describing.
		require.Equal(t, version, f.readDefinitionDocument(t, dataset, VersionTag(version)).Version)

		// The head record advanced with the version (§6.2 step 11).
		head := f.datasetRecord(t, dataset)
		require.Equal(t, DatasetStateActive, head.State)
		require.Equal(t, version, head.LatestVersion)
		require.Zero(t, head.PendingVersion, "an active record has no pending version")

		// The data plane serves exactly this version's data.
		require.Equal(t, []byte(fmt.Sprintf("v%d", version)),
			f.readDatasetObject(t, dataset, VersionTag(version), "data/a.parquet"))
	}

	require.NotEqual(t, heads[0], heads[1])
	require.NotEqual(t, heads[1], heads[2])

	// Older versions keep serving their own data (§7.1).
	require.Equal(t, []byte("v1"), f.readDatasetObject(t, dataset, VersionTag(1), "data/a.parquet"))
	require.Equal(t, []byte("v2"), f.readDatasetObject(t, dataset, VersionTag(2), "data/a.parquet"))
	require.Equal(t, []byte("v3"), f.readDatasetObject(t, dataset, DefaultBranch, "data/a.parquet"))

	// GET /datasets/{ds}/versions returns them newest first, with the definitions inlined.
	result, err := f.svc.ListVersions(f.ctx, f.caller(), dataset, "", 0)
	require.NoError(t, err)
	require.Equal(t, []int{3, 2, 1}, versionNumbers(result.Versions))
	require.False(t, result.HasMore)
	for _, version := range result.Versions {
		require.False(t, definitionIsZero(version.Definition), "version %d carries its definition", version.Version)
	}

	// GET /datasets/{ds}/versions/{version} accepts vN, N and latest.
	for ref, want := range map[string]int{"v2": 2, "2": 2, "latest": 3} {
		version, err := f.svc.GetVersion(f.ctx, f.caller(), dataset, ref)
		require.NoError(t, err, "get version %q", ref)
		require.Equal(t, want, version.Version, "get version %q", ref)
	}

	// §8.2: every distinct source commit is pinned, one reference per version.
	pins, err := f.svc.store.ListDatasetPins(f.ctx, dataset)
	require.NoError(t, err)
	require.Len(t, pins, 3, "one pin per source commit")
	for _, pin := range pins {
		require.Len(t, pin.References, 1)
	}
}

// TestPublishVersion_NoChangesIsConflict pins §6.2 step 8 through the service: republishing the
// same definition is a 409, and the dataset is left exactly as it was (active, no new version, no
// new tag, no leaked pin reference).
func TestPublishVersion_NoChangesIsConflict(t *testing.T) {
	const dataset = "no-change"
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src-nochange", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-nochange", commit.String(), "data", "data/")

	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: dataset, Definition: def, Publish: true, Message: "v1"})
	require.NoError(t, err)

	_, err = f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{Definition: def, Message: "v2"})
	require.ErrorIs(t, err, ErrDatasetConflict)
	require.ErrorIs(t, err, ErrNoChanges)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State, "a rejected publish is rolled back to active")
	require.Equal(t, 1, head.LatestVersion)
	require.Zero(t, head.PendingVersion)

	_, err = f.svc.store.GetVersion(f.ctx, dataset, 2)
	require.ErrorIs(t, err, kv.ErrNotFound, "no version record for the rejected publish")
	_, err = f.tagCommit(t, dataset, VersionTag(2))
	require.ErrorIs(t, err, graveler.ErrNotFound, "no tag for the rejected publish")

	// The v2 pin reference the attempt took was released again.
	pins, err := f.svc.store.ListDatasetPins(f.ctx, dataset)
	require.NoError(t, err)
	require.NotEmpty(t, pins)
	for _, pin := range pins {
		require.Equal(t, []string{PinReference(dataset, 1)}, pin.References)
	}
}

// TestPublishVersion_ExpectedVersion pins the ETag behaviour of §5.4: a stale expected_version is
// a 409 whose error carries the current latest_version, and the matching value publishes.
func TestPublishVersion_ExpectedVersion(t *testing.T) {
	const dataset = "etag"
	f := newServiceFixture(t)
	first := f.sourceRepo(t, "src-etag", map[string]string{"data/a.parquet": "a"})
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       dataset,
		Definition: prefixDefinition("src-etag", first.String(), "data", "data/"),
		Publish:    true,
	})
	require.NoError(t, err)

	second := f.commitTo(t, "src-etag", "main", map[string]string{"data/a.parquet": "b"})
	next := prefixDefinition("src-etag", second.String(), "data", "data/")

	// A matching expected_version publishes.
	version := f.publishVersion(t, dataset, PublishVersionRequest{
		Definition:      next,
		Message:         "v2",
		ExpectedVersion: ptr(1),
	})
	require.Equal(t, 2, version.Version)

	// A stale expected_version is a 409 that names the current latest_version.
	_, err = f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{
		Definition:      next,
		Message:         "v3",
		ExpectedVersion: ptr(1),
	})
	require.ErrorIs(t, err, ErrDatasetConflict)
	var conflict *VersionConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, 2, conflict.LatestVersion)
	require.Equal(t, 1, conflict.Expected)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 2, head.LatestVersion, "the rejected publish did not advance the dataset")
}

// TestPublishVersion_ConcurrentPublishesExactlyOneWins pins the CAS race of §5.4 step 3 and §14
// ("two concurrent publishes"): the loser gets a 409 from the state CAS, not a second version.
//
// The materialisation hook holds the winner inside the window in which the record is
// "publishing", so the interleaving is deterministic instead of timing-dependent.
func TestPublishVersion_ConcurrentPublishesExactlyOneWins(t *testing.T) {
	const dataset = "raced"
	f := newServiceFixture(t)
	first := f.sourceRepo(t, "src-raced", map[string]string{"data/a.parquet": "a"})
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       dataset,
		Definition: prefixDefinition("src-raced", first.String(), "data", "data/"),
		Publish:    true,
	})
	require.NoError(t, err)

	second := f.commitTo(t, "src-raced", "main", map[string]string{"data/a.parquet": "b"})
	def := prefixDefinition("src-raced", second.String(), "data", "data/")

	// Hold the winner inside materialisation, i.e. after the CAS to "publishing".
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	f.svc.publishFn = func(ctx context.Context, materializer *Materializer, req PublishRequest) (*PublishResult, error) {
		once.Do(func() { close(entered) })
		<-release
		return materializer.Publish(ctx, req)
	}

	type result struct {
		version *VersionRecord
		err     error
	}
	winner := make(chan result, 1)
	go func() {
		outcome, err := f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{Definition: def, Message: "v2"})
		winner <- result{version: outcome.Version, err: err}
	}()
	<-entered

	// The second publish reaches the state CAS while the first is still mid-flight.
	_, err = f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{Definition: def, Message: "v2 again"})
	require.ErrorIs(t, err, ErrPublishInProgress)
	require.ErrorIs(t, err, ErrDatasetConflict)

	close(release)
	got := <-winner
	require.NoError(t, got.err)
	require.Equal(t, 2, got.version.Version)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 2, head.LatestVersion, "exactly one version was created")
	require.Zero(t, head.PendingVersion)

	versions, err := f.svc.store.ListVersions(f.ctx, dataset)
	require.NoError(t, err)
	require.Equal(t, []int{2, 1}, versionNumbers(versions))
}

// TestPublishVersion_TakesOverStalePublishingState pins §5.4 step 2: a record left in "publishing"
// by a crashed node is taken over once it is older than datasets.publish_timeout; a fresh one is
// not taken over.
func TestPublishVersion_TakesOverStalePublishingState(t *testing.T) {
	const dataset = "stale-pending"
	f := newServiceFixture(t)
	first := f.sourceRepo(t, "src-stale", map[string]string{"data/a.parquet": "a"})
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       dataset,
		Definition: prefixDefinition("src-stale", first.String(), "data", "data/"),
		Publish:    true,
	})
	require.NoError(t, err)

	second := f.commitTo(t, "src-stale", "main", map[string]string{"data/a.parquet": "b"})
	def := prefixDefinition("src-stale", second.String(), "data", "data/")

	// Simulate a publish that crashed right after the CAS to "publishing".
	leavePublishing := func(age time.Duration) {
		t.Helper()
		current := f.datasetRecord(t, dataset)
		crashed := *current
		crashed.State = DatasetStatePublishing
		crashed.PendingVersion = 2
		crashed.UpdatedAt = f.svc.now().Add(-age)
		require.NoError(t, f.svc.store.PutDatasetIf(f.ctx, &crashed, current))
	}

	// A recent "publishing" record is not stale: the publish is refused.
	leavePublishing(time.Minute)
	_, err = f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{Definition: def, Message: "v2"})
	require.ErrorIs(t, err, ErrPublishInProgress)
	require.Equal(t, DatasetStatePublishing, f.datasetRecord(t, dataset).State)

	// Once it is older than publish_timeout it is taken over and version 2 is published.
	leavePublishing(f.cfg.PublishTimeout + time.Minute)
	version := f.publishVersion(t, dataset, PublishVersionRequest{Definition: def, Message: "v2"})
	require.Equal(t, 2, version.Version)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 2, head.LatestVersion)
	tagCommit, err := f.tagCommit(t, dataset, VersionTag(2))
	require.NoError(t, err)
	require.Equal(t, version.CommitID, tagCommit)
}

// TestPublishVersion_AsyncTaskLifecycle pins §6.5: above sync_publish_max_objects the request
// answers with a task, the task walks pending -> running -> completed, the publish runs on the
// service context (not the request's) and the version lands when it finishes.
func TestPublishVersion_AsyncTaskLifecycle(t *testing.T) {
	const dataset = "async-publish"
	f := newServiceFixture(t)
	f.cfg.SyncPublishMaxObjects = 1 // more than one object per version means asynchronous
	commit := f.sourceRepo(t, "src-async", map[string]string{
		"data/a.parquet": "a",
		"data/b.parquet": "b",
	})
	f.createEmptyDataset(t, dataset, prefixDefinition("src-async", commit.String(), "data", "data/"))

	// Observe the state machine, and prove the goroutine does not run on the request context.
	var (
		mu       sync.Mutex
		statuses []string
	)
	f.svc.onTaskUpdate = func(task TaskRecord) {
		mu.Lock()
		defer mu.Unlock()
		statuses = append(statuses, task.Status)
	}
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	f.svc.publishFn = func(ctx context.Context, materializer *Materializer, req PublishRequest) (*PublishResult, error) {
		require.NoError(t, ctx.Err(), "the async publish must not run on the request context")
		return materializer.Publish(ctx, req)
	}

	outcome, err := f.svc.PublishVersion(requestCtx, f.caller(), dataset, PublishVersionRequest{
		Definition: prefixDefinition("src-async", commit.String(), "data", "data/"),
		Message:    "v1",
	})
	require.NoError(t, err)
	require.True(t, outcome.Accepted(), "a publish above the limit answers 202")
	require.NotNil(t, outcome.Task)
	require.Equal(t, TaskStatusPending, outcome.Task.Status)
	require.Equal(t, TaskKindPublish, outcome.Task.Kind)
	require.Zero(t, outcome.Task.Version)

	// The client disconnects: the accepted publish must still finish (§6.5).
	cancelRequest()
	f.svc.Wait()

	mu.Lock()
	observed := append([]string(nil), statuses...)
	mu.Unlock()
	require.Equal(t, []string{TaskStatusPending, TaskStatusRunning, TaskStatusCompleted}, observed,
		"the task walks pending -> running -> completed")

	task, err := f.svc.GetTask(f.ctx, f.caller(), dataset, outcome.Task.ID)
	require.NoError(t, err)
	require.Equal(t, TaskStatusCompleted, task.Status)
	require.Equal(t, 1, task.Version)
	require.Empty(t, task.Error)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Equal(t, 1, head.LatestVersion)
	tagCommit, err := f.tagCommit(t, dataset, VersionTag(1))
	require.NoError(t, err)
	require.Equal(t, f.latestHead(t, dataset), tagCommit)

	// An unknown task is a 404.
	_, err = f.svc.GetTask(f.ctx, f.caller(), dataset, "01Jnosuchtask000000000000")
	require.ErrorIs(t, err, ErrTaskNotFound)
}

// TestPublishVersion_AsyncFailureIsRolledBack covers the asynchronous failure path: the dataset
// goes back to active with no version and no tag, and the task ends failed with the reason.
func TestPublishVersion_AsyncFailureIsRolledBack(t *testing.T) {
	const dataset = "async-failure"
	f := newServiceFixture(t)
	f.cfg.SyncPublishMaxObjects = 1
	commit := f.sourceRepo(t, "src-async-fail", map[string]string{
		"data/a.parquet": "a",
		"data/b.parquet": "b",
	})
	def := prefixDefinition("src-async-fail", commit.String(), "data", "data/")
	f.createEmptyDataset(t, dataset, def)

	f.svc.publishFn = func(context.Context, *Materializer, PublishRequest) (*PublishResult, error) {
		return nil, errors.New("blockstore unavailable")
	}
	outcome, err := f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{Definition: def, Message: "v1"})
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	f.svc.Wait()

	task, err := f.svc.GetTask(f.ctx, f.caller(), dataset, outcome.Task.ID)
	require.NoError(t, err)
	require.Equal(t, TaskStatusFailed, task.Status)
	require.Contains(t, task.Error, "blockstore unavailable")
	require.Zero(t, task.Version)

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State)
	require.Zero(t, head.LatestVersion)
	_, err = f.svc.store.GetVersion(f.ctx, dataset, 1)
	require.ErrorIs(t, err, kv.ErrNotFound)
	_, err = f.tagCommit(t, dataset, VersionTag(1))
	require.ErrorIs(t, err, graveler.ErrNotFound)
}

// TestPublishVersion_FailedMaterialisationRollsBack is the rollback assertion of the card: a
// materialisation that fails after the record was moved to "publishing" leaves the dataset active
// with no version record, no tag and no pin reference for the failed version (§5.4 step 5).
func TestPublishVersion_FailedMaterialisationRollsBack(t *testing.T) {
	const dataset = "rollback"
	f := newServiceFixture(t)
	first := f.sourceRepo(t, "src-rollback", map[string]string{"data/a.parquet": "a"})
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       dataset,
		Definition: prefixDefinition("src-rollback", first.String(), "data", "data/"),
		Publish:    true,
	})
	require.NoError(t, err)
	headBefore := f.latestHead(t, dataset)

	second := f.commitTo(t, "src-rollback", "main", map[string]string{"data/a.parquet": "b"})
	def := prefixDefinition("src-rollback", second.String(), "data", "data/")

	// Fail after the CAS to "publishing" and after the pins were taken (publish pins before it
	// calls this hook).
	f.svc.publishFn = func(context.Context, *Materializer, PublishRequest) (*PublishResult, error) {
		return nil, errors.New("metarange write failed")
	}
	_, err = f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{Definition: def, Message: "v2"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "metarange write failed")

	head := f.datasetRecord(t, dataset)
	require.Equal(t, DatasetStateActive, head.State, "the failed publish is rolled back to active")
	require.Equal(t, 1, head.LatestVersion)
	require.Zero(t, head.PendingVersion)

	// No version record, no tag, and latest did not move.
	_, err = f.svc.store.GetVersion(f.ctx, dataset, 2)
	require.ErrorIs(t, err, kv.ErrNotFound)
	_, err = f.tagCommit(t, dataset, VersionTag(2))
	require.ErrorIs(t, err, graveler.ErrNotFound)
	require.Equal(t, headBefore, f.latestHead(t, dataset))

	// The pin references of the failed version were released: only v1 keeps its pin.
	pins, err := f.svc.store.ListDatasetPins(f.ctx, dataset)
	require.NoError(t, err)
	require.NotEmpty(t, pins)
	for _, pin := range pins {
		require.Equal(t, []string{PinReference(dataset, 1)}, pin.References,
			"pin %s still claims the failed version", pin.CommitID)
	}

	// The dataset is usable again.
	f.svc.publishFn = publishWithMaterializer
	version := f.publishVersion(t, dataset, PublishVersionRequest{Definition: def, Message: "v2 retry"})
	require.Equal(t, 2, version.Version)
	require.Equal(t, 2, f.datasetRecord(t, dataset).LatestVersion)
}

// TestPublishVersion_RequiresPermission pins that publishing is authorized by the datasets action
// of §9.1 before anything is read or written.
func TestPublishVersion_RequiresPermission(t *testing.T) {
	const dataset = "publish-authz"
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src-authz", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-authz", commit.String(), "data", "data/")
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: dataset, Definition: def, Publish: true})
	require.NoError(t, err)

	f.addUser(t, "watcher")
	f.grant(t, "watcher", "watcher-read", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{ActionReadDataset},
		Resource: DatasetArn(dataset),
	}})

	_, err = f.svc.PublishVersion(f.ctx, f.callerFor("watcher"), dataset, PublishVersionRequest{Definition: def, Message: "nope"})
	require.ErrorIs(t, err, ErrPermissionDenied)

	// ... but the same principal may read the versions it can see.
	_, err = f.svc.ListVersions(f.ctx, f.callerFor("watcher"), dataset, "", 0)
	require.NoError(t, err)
	_, err = f.svc.GetVersion(f.ctx, f.callerFor("watcher"), dataset, "latest")
	require.NoError(t, err)
}

// TestVersions_PaginationAndRefs pins the version-read contract of §10.1: newest first, the
// version-number cursor, and the three accepted ref spellings plus their error cases.
func TestVersions_PaginationAndRefs(t *testing.T) {
	const (
		dataset = "version-pages"
		source  = "src-pages"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "v0"})
	f.createEmptyDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))

	for version := 1; version <= 5; version++ {
		commit = f.commitTo(t, source, "main", map[string]string{"data/a.parquet": fmt.Sprintf("v%d", version)})
		rec := f.publishVersion(t, dataset, PublishVersionRequest{
			Definition: prefixDefinition(source, commit.String(), "data", "data/"),
			Message:    fmt.Sprintf("version %d", version),
		})
		require.Equal(t, version, rec.Version)
	}

	first, err := f.svc.ListVersions(f.ctx, f.caller(), dataset, "", 2)
	require.NoError(t, err)
	require.Equal(t, []int{5, 4}, versionNumbers(first.Versions))
	require.True(t, first.HasMore)
	require.Equal(t, "4", first.NextOffset)
	require.Equal(t, 2, first.MaxPerPage)

	second, err := f.svc.ListVersions(f.ctx, f.caller(), dataset, first.NextOffset, 2)
	require.NoError(t, err)
	require.Equal(t, []int{3, 2}, versionNumbers(second.Versions))
	require.True(t, second.HasMore)

	third, err := f.svc.ListVersions(f.ctx, f.caller(), dataset, second.NextOffset, 2)
	require.NoError(t, err)
	require.Equal(t, []int{1}, versionNumbers(third.Versions))
	require.False(t, third.HasMore)
	require.Empty(t, third.NextOffset)

	t.Run("unknown dataset", func(t *testing.T) {
		_, err := f.svc.ListVersions(f.ctx, f.caller(), "no-such-dataset", "", 0)
		require.ErrorIs(t, err, ErrDatasetNotFound)
		_, err = f.svc.GetVersion(f.ctx, f.caller(), "no-such-dataset", "latest")
		require.ErrorIs(t, err, ErrDatasetNotFound)
	})

	t.Run("unknown version", func(t *testing.T) {
		_, err := f.svc.GetVersion(f.ctx, f.caller(), dataset, "v99")
		require.ErrorIs(t, err, ErrVersionNotFound)
	})

	t.Run("malformed ref", func(t *testing.T) {
		for _, ref := range []string{"v0", "zero", "v"} {
			_, err := f.svc.GetVersion(f.ctx, f.caller(), dataset, ref)
			require.Error(t, err, "ref %q", ref)
			var validation *ValidationError
			require.ErrorAs(t, err, &validation, "ref %q", ref)
			require.Equal(t, "version", validation.Problems[0].Path)
		}
	})

	t.Run("dataset without versions", func(t *testing.T) {
		f.createEmptyDataset(t, "no-versions", prefixDefinition(source, commit.String(), "data", "data/"))
		_, err := f.svc.GetVersion(f.ctx, f.caller(), "no-versions", "latest")
		require.ErrorIs(t, err, ErrVersionNotFound)
		result, err := f.svc.ListVersions(f.ctx, f.caller(), "no-versions", "", 0)
		require.NoError(t, err)
		require.Empty(t, result.Versions)
	})

	t.Run("malformed cursor", func(t *testing.T) {
		_, err := f.svc.ListVersions(f.ctx, f.caller(), dataset, "last", 0)
		require.Error(t, err)
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "after", validation.Problems[0].Path)
	})
}

// TestVersions_OutOfLineDefinition covers the §5.3 large-definition path on the read side: a
// VersionRecord that only carries a DefinitionRef is completed from the definition object of its
// own commit, so the versions endpoints never return a definition-less version.
func TestVersions_OutOfLineDefinition(t *testing.T) {
	const dataset = "large-definition"
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src-large", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-large", commit.String(), "data", "data/")
	f.createEmptyDataset(t, dataset, def)
	version := f.publishVersion(t, dataset, PublishVersionRequest{Definition: def, Message: "v1"})

	// Rewrite v1 in the out-of-line form that Store.PutVersion produces above
	// MaxInlineDefinitionBytes (§5.3).
	stored, err := f.svc.store.GetVersion(f.ctx, dataset, 1)
	require.NoError(t, err)
	require.NoError(t, f.svc.store.DeleteVersion(f.ctx, dataset, 1))
	outOfLine := *stored
	outOfLine.Definition = Definition{}
	outOfLine.DefinitionRef = &DefinitionRef{CommitID: stored.CommitID, Path: DefinitionPath}
	_, err = f.svc.store.PutVersion(f.ctx, outOfLine)
	require.NoError(t, err)

	reread, err := f.svc.store.GetVersion(f.ctx, dataset, 1)
	require.NoError(t, err)
	require.False(t, reread.HasInlineDefinition(), "the record now stores only a DefinitionRef")

	read, err := f.svc.GetVersion(f.ctx, f.caller(), dataset, "v1")
	require.NoError(t, err)
	require.Equal(t, version.Definition, read.Definition, "the definition is rebuilt from the commit")

	listed, err := f.svc.ListVersions(f.ctx, f.caller(), dataset, "", 0)
	require.NoError(t, err)
	require.Len(t, listed.Versions, 1)
	require.Equal(t, version.Definition, listed.Versions[0].Definition)

	// A record whose definition object is gone still answers with its identity (§12.3 reports it).
	require.NoError(t, f.svc.store.DeleteVersion(f.ctx, dataset, 1))
	broken := *stored
	broken.Definition = Definition{}
	broken.DefinitionRef = &DefinitionRef{CommitID: stored.CommitID, Path: DefinitionPath + ".missing"}
	_, err = f.svc.store.PutVersion(f.ctx, broken)
	require.NoError(t, err)
	degraded, err := f.svc.GetVersion(f.ctx, f.caller(), dataset, "1")
	require.NoError(t, err)
	require.True(t, definitionIsZero(degraded.Definition))
	require.Equal(t, stored.CommitID, degraded.CommitID)
}

// TestEstimateSourceObjects pins the §6.5 sizing rule: the estimate stops counting once the
// running total passes the sync threshold, but §5.2 rule 5 is still checked for every item.
func TestEstimateSourceObjects(t *testing.T) {
	const (
		source = "src-estimate"
		count  = 30
	)
	f := newServiceFixture(t)
	objects := make(map[string]string, count)
	for i := range count {
		objects[fmt.Sprintf("data/left/%s.parquet", twoDigits(i))] = "payload"
	}
	commit := f.sourceRepo(t, source, objects)

	def := Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target: "left", Repository: source, Ref: SourceRef{Type: RefTypeCommit, ID: commit.String()},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/left/"},
		}},
	}
	require.NoError(t, Validate(&def, ValidateOptions{}))

	t.Run("capped at the threshold", func(t *testing.T) {
		stats, err := f.svc.estimateSourceObjects(f.ctx, &def, 3)
		require.NoError(t, err)
		require.Equal(t, int64(4), stats.Objects, "counting stops just past the threshold")
	})

	t.Run("exact when uncapped", func(t *testing.T) {
		stats, err := f.svc.sourceStats(f.ctx, &def)
		require.NoError(t, err)
		require.Equal(t, int64(count), stats.Objects)
		require.Equal(t, int64(count*len("payload")), stats.Bytes)
	})

	t.Run("an empty item is still rejected above the cap", func(t *testing.T) {
		big := make(map[string]string, count)
		for i := range count {
			big[fmt.Sprintf("data/left/%s.parquet", twoDigits(i))] = "payload"
		}
		bigCommit := f.sourceRepo(t, "src-estimate-big", big)
		withEmpty := Definition{
			FormatVersion: FormatVersion,
			Data: []DataItem{
				{
					Target: "left", Repository: "src-estimate-big",
					Ref:  SourceRef{Type: RefTypeCommit, ID: bigCommit.String()},
					Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/left/"},
				},
				{
					Target: "right", Repository: "src-estimate-big",
					Ref:  SourceRef{Type: RefTypeCommit, ID: bigCommit.String()},
					Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/right/"},
				},
			},
		}
		require.NoError(t, Validate(&withEmpty, ValidateOptions{}))
		_, err := f.svc.estimateSourceObjects(f.ctx, &withEmpty, 3)
		require.Error(t, err, "the empty item must fail even though the cap was passed already")
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "data[1].prefix.path", validation.Problems[0].Path)
	})
}

func versionNumbers(versions []*VersionRecord) []int {
	out := make([]int, len(versions))
	for i, version := range versions {
		out[i] = version.Version
	}
	return out
}

func ptr[T any](value T) *T { return &value }
