package datasets

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// withServiceCaller marks ctx as belonging to the datasets service itself, which the immutability
// guard lets through. It is unexported and lives in the test file on purpose: the guard's value is
// that a caller cannot switch it off, so the marker must not be buildable outside this package
// (see serviceCallerKey). Production does not need it - the service writes with ReadOnly + force
// and never routes its own writes through Authorize (DATASETS_DESIGN.md §4.6, §9.4).
func withServiceCaller(ctx context.Context) context.Context {
	return context.WithValue(ctx, serviceCallerKey{}, true)
}

// TestImmutabilityAuthServiceDeniesRegardlessOfGroups is the SEC-2 test: a custom group with
// FSFullAccess and a user with a directly attached fs:* policy are both refused every write
// action on a dataset repository, and are still allowed on other repositories and for reads.
func TestImmutabilityAuthServiceDeniesRegardlessOfGroups(t *testing.T) {
	f := newAuthzFixture(t)
	full := model.Statements{{Effect: model.StatementEffectAllow, Action: []string{"fs:*", "pr:*", "catalog:*"}, Resource: "*"}}
	f.policy(t, "FSFullAccess", full)
	f.group(t, "data-eng")
	f.attachToGroup(t, "FSFullAccess", "data-eng")
	f.user(t, "grouped")
	f.addToGroup(t, "grouped", "data-eng")
	f.user(t, "direct")
	f.attachToUser(t, "FSFullAccess", "direct")

	repos := newDatasetRepoSet(func(context.Context) ([]string, error) { return []string{"foo"}, nil }, time.Hour, time.Now)
	guard := NewImmutabilityAuthService(f.svc, repos)
	authz := NewAuthorizer(guard)

	writes := map[string]string{
		permissions.WriteObjectAction:            permissions.ObjectArn("foo", "latest/a"),
		permissions.CreateTagAction:              permissions.TagArn("foo", "v3"),
		permissions.DeleteBranchAction:           permissions.BranchArn("foo", "latest"),
		permissions.UpdateRepositoryAction:       permissions.RepoArn("foo"),
		permissions.ImportCancelAction:           permissions.BranchArn("foo", "latest"),
		permissions.WritePullRequestAction:       permissions.RepoArn("foo"),
		permissions.CatalogCreateNamespaceAction: permissions.CatalogNamespaceArn("foo", "ns"),
		permissions.CatalogUpdateTableAction:     permissions.CatalogTableArn("foo", "ns", "t"),
		permissions.CatalogDeleteViewAction:      permissions.CatalogViewArn("foo", "ns", "v"),
	}
	for _, user := range []string{"grouped", "direct"} {
		caller := Caller{Username: user}
		for action, resource := range writes {
			node := permissions.Node{Permission: permissions.Permission{Action: action, Resource: resource}}
			err := authz.Check(t.Context(), caller, node, "")
			require.ErrorIsf(t, err, ErrPermissionDenied, "%s %s on dataset repo", user, action)
			require.Equal(t, 403, asStatus(err))

			// The same action on an ordinary repository is untouched.
			other := permissions.Node{Permission: permissions.Permission{Action: action, Resource: swapRepo(resource, "foo", "plain")}}
			require.NoErrorf(t, authz.Check(t.Context(), caller, other, ""), "%s %s on plain repo", user, action)
		}
		// Reads on the dataset repository stay allowed.
		read := permissions.Node{Permission: permissions.Permission{Action: permissions.ReadObjectAction, Resource: permissions.ObjectArn("foo", "x")}}
		require.NoError(t, authz.Check(t.Context(), caller, read, ""))

		// The datasets service itself is exempt; only in-package test code can build that marker.
		svcCtx := withServiceCaller(t.Context())
		node := permissions.Node{Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("foo", "x")}}
		require.NoError(t, authz.Check(svcCtx, caller, node, ""))
	}

	// An Or node survives while one alternative is allowed, an And node does not.
	caller := Caller{Username: "direct"}
	deniedLeaf := permissions.Node{Permission: permissions.Permission{Action: permissions.DeleteTagAction, Resource: permissions.TagArn("foo", "v1")}}
	okLeaf := permissions.Node{Permission: permissions.Permission{Action: permissions.ReadTagAction, Resource: permissions.TagArn("foo", "v1")}}
	require.NoError(t, authz.Check(t.Context(), caller, permissions.Node{Type: permissions.NodeTypeOr, Nodes: []permissions.Node{deniedLeaf, okLeaf}}, ""))
	require.ErrorIs(t, authz.Check(t.Context(), caller, permissions.Node{Type: permissions.NodeTypeAnd, Nodes: []permissions.Node{deniedLeaf, okLeaf}}, ""), ErrPermissionDenied)
}

func swapRepo(resource, from, to string) string {
	if strings.Contains(resource, "repository/"+from) {
		return strings.Replace(resource, "repository/"+from, "repository/"+to, 1)
	}
	for _, kind := range []string{"namespace/", "table/", "view/"} {
		if strings.Contains(resource, kind+from+"/") {
			return strings.Replace(resource, kind+from+"/", kind+to+"/", 1)
		}
	}
	return resource
}

// TestRepositoryOfArn pins the resources the immutability guard recognizes: every fs ARN that
// names a repository, and the catalog ARNs of ICEBERG_CATALOG_PLAN.md §7.2. A resource that does
// not belong to a repository (or an unknown catalog kind) must not be attributed to one.
func TestRepositoryOfArn(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		wantRepo string
		wantOK   bool
	}{
		{"repository", permissions.RepoArn("foo"), "foo", true},
		{"object", permissions.ObjectArn("foo", "a/b"), "foo", true},
		{"branch", permissions.BranchArn("foo", "main"), "foo", true},
		{"tag", permissions.TagArn("foo", "v1"), "foo", true},
		{"catalog namespace", permissions.CatalogNamespaceArn("foo", "ns"), "foo", true},
		{"catalog namespace at repository", permissions.CatalogNamespaceArn("foo", ""), "foo", true},
		{"catalog table", permissions.CatalogTableArn("foo", "ns", "t"), "foo", true},
		{"catalog view", permissions.CatalogViewArn("foo", "ns", "v"), "foo", true},
		{"empty fs repository", "arn:lakefs:fs:::repository/", "", false},
		{"storage namespace", "arn:lakefs:fs:::namespace/ns", "", false},
		{"catalog unknown kind", "arn:lakefs:catalog:::database/foo/db", "", false},
		{"catalog missing repository", "arn:lakefs:catalog:::table/", "", false},
		{"auth resource", "arn:lakefs:auth:::user/bob", "", false},
		{"wildcard", "*", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, ok := repositoryOfArn(tt.resource)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantRepo, repo)
		})
	}
}

// TestImmutabilityAuthServiceFailsClosedWithoutRepoSet covers a miswired guard: without a name
// set it must refuse to decide, never allow a write to a dataset repository.
func TestImmutabilityAuthServiceFailsClosedWithoutRepoSet(t *testing.T) {
	guard := NewImmutabilityAuthService(&stubAuthService{}, nil)
	_, err := guard.Authorize(t.Context(), &auth.AuthorizationRequest{RequiredPermissions: permissions.Node{
		Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("a", "x")},
	}})
	require.ErrorIs(t, err, errNoDatasetRepoSet)
}

// TestDatasetRepoSetCacheAndFailClosed covers the cached name set: TTL refresh, stale fallback on
// a failing refresh, and the error when nothing was ever loaded.
func TestDatasetRepoSetCacheAndFailClosed(t *testing.T) {
	now := time.Unix(1000, 0)
	names := []string{"a"}
	var failing bool
	calls := 0
	set := newDatasetRepoSet(func(context.Context) ([]string, error) {
		calls++
		if failing {
			return nil, errors.New("kv down")
		}
		return names, nil
	}, time.Second, func() time.Time { return now })

	ok, err := set.Contains(t.Context(), "a")
	require.NoError(t, err)
	require.True(t, ok)
	names = []string{"a", "b"}
	ok, _ = set.Contains(t.Context(), "b")
	require.False(t, ok, "served from cache inside the TTL")
	require.Equal(t, 1, calls)
	now = now.Add(2 * time.Second)
	ok, _ = set.Contains(t.Context(), "b")
	require.True(t, ok, "refreshed after the TTL")

	failing = true
	now = now.Add(2 * time.Second)
	ok, err = set.Contains(t.Context(), "a")
	require.NoError(t, err)
	require.True(t, ok, "stale set used when the refresh fails")

	// A failed attempt counts against the TTL: the failing store is not hit again on every
	// authorization, and the last known set keeps answering meanwhile.
	ok, err = set.Contains(t.Context(), "b")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 3, calls)

	cold := newDatasetRepoSet(func(context.Context) ([]string, error) { return nil, errors.New("kv down") }, time.Second, time.Now)
	_, err = cold.Contains(t.Context(), "a")
	require.Error(t, err)

	// Authorize fails closed (an error, never an allow) when the set cannot be loaded.
	guard := NewImmutabilityAuthService(&stubAuthService{}, cold)
	_, err = guard.Authorize(t.Context(), &auth.AuthorizationRequest{RequiredPermissions: permissions.Node{
		Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("a", "x")}}})
	require.Error(t, err)
}

// stubAuthService allows everything.
type stubAuthService struct{ auth.Service }

func (*stubAuthService) Authorize(context.Context, *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{Allowed: true}, nil
}

// TestCreateInvalidatesGuardedRepos: Create drops the guard's cached dataset names, so a new
// dataset repository is guarded from its first write instead of after the cache TTL.
func TestCreateInvalidatesGuardedRepos(t *testing.T) {
	f := newServiceFixture(t)
	store := NewStore(f.kvStore)
	repos := newDatasetRepoSet(func(ctx context.Context) ([]string, error) {
		recs, err := store.ListDatasets(ctx)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(recs))
		for _, rec := range recs {
			names = append(names, rec.Name)
		}
		return names, nil
	}, time.Hour, time.Now)
	svc, err := NewService(f.cfg, f.kvStore, f.cat, NewImmutabilityAuthService(f.authService, repos))
	require.NoError(t, err)

	guarded, err := repos.Contains(f.ctx, "guarded-on-create")
	require.NoError(t, err)
	require.False(t, guarded)

	commit := f.sourceRepo(t, "src-guarded-on-create", map[string]string{"data/a.csv": "a"})
	_, err = svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       "guarded-on-create",
		Definition: prefixDefinition("src-guarded-on-create", commit.String(), "data", "data/"),
		Message:    "v1",
	})
	require.NoError(t, err)
	guarded, err = repos.Contains(f.ctx, "guarded-on-create")
	require.NoError(t, err)
	require.True(t, guarded, "the cached set must not hide the new dataset for its TTL")
}

// TestDatasetRepoSetInvalidateServesLastKnownSet covers the window after a dataset create: the
// invalidation owes a reload, but when that reload fails the set still answers from the names it
// has, so a KV hiccup does not fail every write in the installation. Recovery happens on the next
// TTL, and a set that never loaded still fails closed (TestDatasetRepoSetCacheAndFailClosed).
func TestDatasetRepoSetInvalidateServesLastKnownSet(t *testing.T) {
	now := time.Unix(2000, 0)
	names := []string{"a"}
	var failing bool
	calls := 0
	set := newDatasetRepoSet(func(context.Context) ([]string, error) {
		calls++
		if failing {
			return nil, errors.New("kv down")
		}
		return names, nil
	}, time.Hour, func() time.Time { return now })

	ok, err := set.Contains(t.Context(), "a")
	require.NoError(t, err)
	require.True(t, ok)

	// A dataset was created (Invalidate) and the reload fails: the guard answers from the set it
	// already has instead of turning the hiccup into an authorization error.
	failing = true
	set.Invalidate()
	ok, err = set.Contains(t.Context(), "a")
	require.NoError(t, err, "a failed reload must not become an authorization error")
	require.True(t, ok)
	ok, err = set.Contains(t.Context(), "plain")
	require.NoError(t, err, "an unrelated repository is not failed by the guard's KV hiccup")
	require.False(t, ok)
	require.Equal(t, 2, calls, "one reload after Invalidate, then the TTL holds off the failing store")

	// At the Authorize level that is the difference between a hiccup and an outage: writes to
	// unrelated repositories keep working, and the dataset still refuses the write.
	guard := NewImmutabilityAuthService(&stubAuthService{}, set)
	res, err := guard.Authorize(t.Context(), &auth.AuthorizationRequest{RequiredPermissions: permissions.Node{
		Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("plain", "x")}}})
	require.NoError(t, err)
	require.True(t, res.Allowed)
	res, err = guard.Authorize(t.Context(), &auth.AuthorizationRequest{RequiredPermissions: permissions.Node{
		Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("a", "x")}}})
	require.NoError(t, err)
	require.False(t, res.Allowed, "the known dataset stays immutable while the set is stale")

	// Recovery: after the TTL the new dataset is picked up.
	failing = false
	names = []string{"a", "b"}
	now = now.Add(2 * time.Hour)
	ok, err = set.Contains(t.Context(), "b")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 3, calls)
}

// TestDatasetRepoSetInvalidateDuringLoad covers the two properties the loader running outside the
// mutex must not lose: an Invalidate (a dataset create on another goroutine) does not block until
// the scan finishes, and it is not masked by that scan - the next Contains reloads and sees the
// new dataset, so the guard does not hide it for a whole TTL.
func TestDatasetRepoSetInvalidateDuringLoad(t *testing.T) {
	now := time.Unix(3000, 0)
	names := []string{"a"}
	loading := make(chan struct{})
	release := make(chan struct{})

	var (
		mu    sync.Mutex
		calls int
	)
	var first sync.Once
	set := newDatasetRepoSet(func(context.Context) ([]string, error) {
		mu.Lock()
		calls++
		snapshot := append([]string(nil), names...) // what the scan of the store sees right now
		mu.Unlock()
		first.Do(func() {
			close(loading)
			<-release
		})
		return snapshot, nil
	}, time.Hour, func() time.Time { return now })

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := set.Contains(context.Background(), "b"); err != nil {
			t.Error(err)
		}
	}()
	<-loading

	// The mutex is not held across the load, so a create on another goroutine is not stuck behind
	// the scan.
	invalidated := make(chan struct{})
	go func() {
		set.Invalidate()
		close(invalidated)
	}()
	select {
	case <-invalidated:
	case <-time.After(10 * time.Second):
		t.Fatal("Invalidate blocked while a load was running: the mutex is held across the scan")
	}

	// The dataset the invalidation is about lands while the scan is running.
	mu.Lock()
	names = []string{"a", "b"}
	mu.Unlock()
	close(release)
	<-done

	ok, err := set.Contains(t.Context(), "b")
	require.NoError(t, err)
	require.True(t, ok, "the invalidation must not be masked by the scan that was already running")
	mu.Lock()
	require.Equal(t, 2, calls)
	mu.Unlock()

	// And once the invalidation is served, the cache answers normally again.
	ok, err = set.Contains(t.Context(), "b")
	require.NoError(t, err)
	require.True(t, ok)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, calls)
}

// TestDatasetRepoSetSingleFlight proves that a burst of concurrent authorizations runs exactly one
// loader - in production a full scan of the datasets/ partition - and that the other callers wait
// on that load rather than on a lock held across it.
func TestDatasetRepoSetSingleFlight(t *testing.T) {
	const callers = 50

	var (
		mu    sync.Mutex
		calls int
	)
	loading := make(chan struct{})
	release := make(chan struct{})
	var started sync.Once
	set := newDatasetRepoSet(func(context.Context) ([]string, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		started.Do(func() { close(loading) })
		<-release
		return []string{"a"}, nil
	}, time.Hour, time.Now)

	// The load is held open until every caller is in flight, so the burst really is concurrent.
	var wg sync.WaitGroup
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := set.Contains(context.Background(), "a")
			switch {
			case err != nil:
				results <- err
			case !ok:
				results <- errors.New("dataset repository not found in the cached set")
			}
		}()
	}
	<-loading
	close(release)
	wg.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, calls, "a burst of authorizations must run one loader, not one per caller")
}

// TestDatasetRepoSetLoadIgnoresLeaderCancellation: the leader's load is shared with every waiter,
// so a leader whose request is cancelled must not fail the others with its context.Canceled.
func TestDatasetRepoSetLoadIgnoresLeaderCancellation(t *testing.T) {
	set := newDatasetRepoSet(func(ctx context.Context) ([]string, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, ok := ctx.Deadline(); !ok {
			return nil, errors.New("the load must be bounded by a deadline of its own")
		}
		return []string{"a"}, nil
	}, time.Hour, time.Now)

	leaderCtx, cancel := context.WithCancel(t.Context())
	cancel()
	ok, err := set.Contains(leaderCtx, "a")
	require.NoError(t, err, "the load must not inherit the leader's cancellation")
	require.True(t, ok)

	ok, err = set.Contains(t.Context(), "a")
	require.NoError(t, err)
	require.True(t, ok)
}

// TestDatasetRepoSetLoaderPanicReleasesGate: a panicking loader must not leave the single-flight
// gate set, or every later authorization would wait on a channel nobody closes.
func TestDatasetRepoSetLoaderPanicReleasesGate(t *testing.T) {
	now := time.Unix(4000, 0)
	panicking := true
	set := newDatasetRepoSet(func(context.Context) ([]string, error) {
		if panicking {
			panic("loader bug")
		}
		return []string{"a"}, nil
	}, time.Second, func() time.Time { return now })

	require.Panics(t, func() { _, _ = set.Contains(t.Context(), "a") })

	// Before the TTL the failed attempt answers; nothing was ever loaded, so it fails closed.
	_, err := set.Contains(t.Context(), "a")
	require.ErrorIs(t, err, errDatasetRepoSetLoadPanicked)

	// After the TTL a new load runs instead of waiting on the abandoned gate.
	panicking = false
	now = now.Add(time.Minute)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ok, err := set.Contains(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)
}
