// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// immutability_guard_test.go covers the §9.4 authorizer guard of immutability_guard.go and the
// dataset repository index it reads (SEC-2 of DATASETS_REVIEW.md): a principal whose write power
// does not come from a base group - a custom group, or a policy attached directly to the user -
// must still be refused every write on a dataset repository, while a repository that is not a
// dataset, and every read action, stay untouched.
package datasets

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// guardDataset is the dataset repository every guard test writes to.
	guardDataset = "customer-360"
	// guardSibling is a repository whose name starts with guardDataset: the guard must leave it
	// alone, exactly like the per-dataset deny policy (§9.4).
	guardSibling = "customer-360-archive"
)

// guardFixture is the internal-RBAC (policy) installation the guard runs in: the RBAC base groups
// and policies of pkg/auth/setup plus the datasets bootstrap, on an in-memory KV store.
type guardFixture struct {
	svc   *acl.AuthService
	guard auth.Service
	index *DatasetRepoIndex
}

func newGuardFixture(t *testing.T) *guardFixture {
	t.Helper()
	ctx := t.Context()
	store := kvtest.GetStore(ctx, t)
	svc := acl.NewAuthService(store, crypt.NewSecretStore([]byte("datasets-guard-test-secret")), authparams.ServiceCache{Enabled: false})
	ts := time.Now()
	require.NoError(t, setup.EnsureRBACBaseGroups(ctx, svc, ts))
	require.NoError(t, Bootstrap(ctx, svc, ts))

	// Two dataset repositories known to the index, one that is not a dataset.
	index := newDatasetRepoIndex(func(context.Context) ([]string, error) {
		return []string{guardDataset, "events"}, nil
	}, time.Minute, time.Now, logging.ContextUnavailable())
	return &guardFixture{
		svc:   svc,
		guard: NewImmutabilityGuardService(svc, index, logging.ContextUnavailable()),
		index: index,
	}
}

// userWithGroup creates a user in a fresh custom group that holds the named policies.
func (f *guardFixture) userWithGroup(t *testing.T, username, group string, policyNames ...string) {
	t.Helper()
	ctx := t.Context()
	_, err := f.svc.CreateUser(ctx, &model.User{Username: username, CreatedAt: time.Now()})
	require.NoError(t, err)
	_, err = f.svc.CreateGroup(ctx, &model.Group{DisplayName: group, CreatedAt: time.Now()})
	require.NoError(t, err)
	require.NoError(t, f.svc.AddUserToGroup(ctx, username, group))
	for _, name := range policyNames {
		require.NoErrorf(t, f.svc.AttachPolicyToGroup(ctx, name, group), "attach %s to %s", name, group)
	}
}

// userWithPolicy creates a user whose only grant is a policy attached directly to the user.
func (f *guardFixture) userWithPolicy(t *testing.T, username, policyName string) {
	t.Helper()
	ctx := t.Context()
	_, err := f.svc.CreateUser(ctx, &model.User{Username: username, CreatedAt: time.Now()})
	require.NoError(t, err)
	require.NoError(t, f.svc.AttachPolicyToUser(ctx, policyName, username))
}

// authorize asks the wrapped service, the way the core API does.
func (f *guardFixture) authorize(t *testing.T, username, action, resource string) (*auth.AuthorizationResponse, error) {
	t.Helper()
	return f.guard.Authorize(t.Context(), &auth.AuthorizationRequest{
		Username:            username,
		RequiredPermissions: permissions.Node{Permission: permissions.Permission{Action: action, Resource: resource}},
	})
}

// requireDenied requires that the guard answers a denial carrying auth.ErrInsufficientPermissions,
// which is what makes the core API answer with its authorization-failure status instead of a 500.
func (f *guardFixture) requireDenied(t *testing.T, username, action, resource string) {
	t.Helper()
	resp, err := f.authorize(t, username, action, resource)
	require.NoErrorf(t, err, "%s %s", action, resource)
	require.Falsef(t, resp.Allowed, "%s %s must be denied", action, resource)
	require.ErrorIsf(t, resp.Error, auth.ErrInsufficientPermissions, "%s %s must be denied", action, resource)
}

func (f *guardFixture) requireAllowed(t *testing.T, username, action, resource string) {
	t.Helper()
	resp, err := f.authorize(t, username, action, resource)
	require.NoErrorf(t, err, "%s %s", action, resource)
	require.Truef(t, resp.Allowed, "%s %s must be allowed: %v", action, resource, resp.Error)
}

// TestImmutabilityGuardDeniesCustomGroupWithFSFullAccess is the SEC-2 test: a member of a custom
// group that holds FSFullAccess - a group the per-dataset deny policy is never attached to - is
// refused every write on a dataset repository, and only there.
func TestImmutabilityGuardDeniesCustomGroupWithFSFullAccess(t *testing.T) {
	t.Parallel()
	f := newGuardFixture(t)
	f.userWithGroup(t, "data-eng", "data-eng", "FSFullAccess")

	// The four writes of DATASETS_REVIEW.md SEC-2: upload, CreateTag, DeleteBranch and
	// SetRepositoryMetadata. Every resource shape the core API authorizes them against is covered.
	denied := []struct{ action, resource string }{
		{permissions.WriteObjectAction, permissions.ObjectArn(guardDataset, "latest/customers/c1.parquet")},
		{permissions.CreateTagAction, permissions.TagArn(guardDataset, "v3")},
		{permissions.DeleteBranchAction, permissions.BranchArn(guardDataset, DefaultBranch)},
		{permissions.UpdateRepositoryAction, permissions.RepoArn(guardDataset)},
	}
	for _, c := range denied {
		f.requireDenied(t, "data-eng", c.action, c.resource)
	}

	// The rest of the extended deny list of SEC-2: imports, pull requests and the Iceberg catalog
	// mutations, plus the actions the §9.4 policy already denied.
	for _, c := range []struct{ action, resource string }{
		{permissions.ImportCancelAction, permissions.BranchArn(guardDataset, DefaultBranch)},
		{permissions.ImportFromStorageAction, permissions.RepoArn(guardDataset)},
		{permissions.WritePullRequestAction, permissions.RepoArn(guardDataset)},
		{permissions.CreateCommitAction, permissions.BranchArn(guardDataset, DefaultBranch)},
		{permissions.DeleteRepositoryAction, permissions.RepoArn(guardDataset)},
		{permissions.CatalogCreateTableAction, permissions.CatalogTableArn(guardDataset, "analytics", "events")},
		{permissions.CatalogUpdateNamespaceAction, permissions.CatalogNamespaceArn(guardDataset, "analytics")},
		{permissions.CatalogDeleteViewAction, permissions.CatalogViewArn(guardDataset, "analytics", "v")},
		// A catalog action added after this list was written must be denied too: the list holds
		// patterns, not only the concrete constants.
		{"catalog:CreateSomethingNew", permissions.CatalogNamespaceArn(guardDataset, "analytics")},
	} {
		f.requireDenied(t, "data-eng", c.action, c.resource)
	}

	// Reading a dataset, and every write on a repository that is not a dataset, stay allowed: the
	// guard only ever adds the dataset-repository denial.
	f.requireAllowed(t, "data-eng", permissions.ReadObjectAction, permissions.ObjectArn(guardDataset, "v1/customers/c1.parquet"))
	f.requireAllowed(t, "data-eng", permissions.WriteObjectAction, permissions.ObjectArn(guardSibling, "data/c1.parquet"))
	f.requireAllowed(t, "data-eng", permissions.WriteObjectAction, permissions.ObjectArn("lakehouse-repo", "data/c1.parquet"))
	f.requireAllowed(t, "data-eng", permissions.CreateRepositoryAction, permissions.RepoArn("brand-new-repo"))
}

// TestImmutabilityGuardDeniesDirectlyAttachedPolicy covers the second half of SEC-2: a user with a
// policy attached directly to the user, in no group at all, is covered as well.
func TestImmutabilityGuardDeniesDirectlyAttachedPolicy(t *testing.T) {
	t.Parallel()
	f := newGuardFixture(t)
	require.NoError(t, f.svc.WritePolicy(t.Context(), &model.Policy{
		DisplayName: "in-house-fs-all",
		CreatedAt:   time.Now(),
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*", "catalog:*"},
			Resource: permissions.All,
		}},
	}, false))
	f.userWithPolicy(t, "solo-writer", "in-house-fs-all")

	for _, c := range []struct{ action, resource string }{
		{permissions.WriteObjectAction, permissions.ObjectArn(guardDataset, "latest/customers/c2.parquet")},
		{permissions.CreateTagAction, permissions.TagArn(guardDataset, "v4")},
		{permissions.DeleteBranchAction, permissions.BranchArn(guardDataset, DefaultBranch)},
		{permissions.UpdateRepositoryAction, permissions.RepoArn(guardDataset)},
		{permissions.CatalogCreateTableAction, permissions.CatalogTableArn(guardDataset, "analytics", "events")},
	} {
		f.requireDenied(t, "solo-writer", c.action, c.resource)
	}
	// Nothing else changes for them.
	f.requireAllowed(t, "solo-writer", permissions.WriteObjectAction, permissions.ObjectArn(guardSibling, "data/c1.parquet"))
}

// TestImmutabilityGuardNodeSemantics pins that the guard follows the policy engine's And/Or
// semantics instead of denying a whole request because one branch of an Or is a dataset write.
func TestImmutabilityGuardNodeSemantics(t *testing.T) {
	t.Parallel()
	f := newGuardFixture(t)
	f.userWithGroup(t, "data-eng", "data-eng", "FSFullAccess")

	write := permissions.Node{Permission: permissions.Permission{
		Action:   permissions.WriteObjectAction,
		Resource: permissions.ObjectArn(guardDataset, "latest/x"),
	}}
	read := permissions.Node{Permission: permissions.Permission{
		Action:   permissions.ReadObjectAction,
		Resource: permissions.ObjectArn(guardDataset, "latest/x"),
	}}

	check := func(node permissions.Node) (*auth.AuthorizationResponse, error) {
		return f.guard.Authorize(t.Context(), &auth.AuthorizationRequest{
			Username:            "data-eng",
			RequiredPermissions: node,
		})
	}

	// And: one denied child denies the request.
	resp, err := check(permissions.Node{Type: permissions.NodeTypeAnd, Nodes: []permissions.Node{read, write}})
	require.NoError(t, err)
	require.False(t, resp.Allowed)
	require.ErrorIs(t, resp.Error, auth.ErrInsufficientPermissions)

	// Or: like the policy engine, one denied branch denies the whole request - a Deny statement
	// matching any required permission wins, and a narrower rule would be a way around the guard.
	resp, err = check(permissions.Node{Type: permissions.NodeTypeOr, Nodes: []permissions.Node{read, write}})
	require.NoError(t, err)
	require.False(t, resp.Allowed)
	require.ErrorIs(t, resp.Error, auth.ErrInsufficientPermissions)

	// Or with only allowed branches is decided by the policy engine, as usual.
	resp, err = check(permissions.Node{Type: permissions.NodeTypeOr, Nodes: []permissions.Node{read, read}})
	require.NoError(t, err)
	require.True(t, resp.Allowed)
}

// TestImmutabilityGuardNilServiceAndIndex pins the fail-safe constructors: wrapping nothing, or
// wrapping without an index, must never produce a service that denies or panics.
func TestImmutabilityGuardNilServiceAndIndex(t *testing.T) {
	t.Parallel()
	require.Nil(t, WrapAuthService(nil, nil, nil))
	require.Nil(t, NewImmutabilityGuardService(nil, NewDatasetRepoIndex(nil, nil), nil))
	f := newGuardFixture(t)
	f.userWithGroup(t, "data-eng", "data-eng", "FSFullAccess")
	unwrapped := NewImmutabilityGuardService(f.svc, nil, nil)
	require.NotNil(t, unwrapped)
	resp, err := unwrapped.Authorize(t.Context(), &auth.AuthorizationRequest{
		Username:            "data-eng",
		RequiredPermissions: permissions.Node{Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn(guardDataset, "latest/x")}},
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed, "without an index the guard adds nothing; FSFullAccess alone decides")
}

// TestRepositoryFromResource pins the resource parsing of the guard.
func TestRepositoryFromResource(t *testing.T) {
	t.Parallel()
	tests := []struct {
		resource string
		want     string
		ok       bool
	}{
		{permissions.RepoArn("repo"), "repo", true},
		{permissions.ObjectArn("repo", "latest/data/c1.parquet"), "repo", true},
		{permissions.BranchArn("repo", "main"), "repo", true},
		{permissions.TagArn("repo", "v1"), "repo", true},
		{permissions.CatalogNamespaceArn("repo", "analytics"), "repo", true},
		{permissions.CatalogTableArn("repo", "analytics", "events"), "repo", true},
		{permissions.CatalogViewArn("repo", "analytics", "v"), "repo", true},
		{permissions.All, "", false},
		{permissions.UserArn("u"), "", false},
		{permissions.StorageNamespace("mem://x"), "", false},
		{"", "", false},
		{"arn:lakefs:fs:::repository/", "", false},
		{"arn:lakefs:fs:::repository/*", "", false},
		{"arn:lakefs:catalog:::table", "", false},
	}
	for _, tt := range tests {
		got, ok := RepositoryFromResource(tt.resource)
		require.Equalf(t, tt.ok, ok, "ok for %q", tt.resource)
		require.Equalf(t, tt.want, got, "repository of %q", tt.resource)
	}
}

// TestIsImmutabilityDeniedAction pins the action matching, including the catalog patterns.
func TestIsImmutabilityDeniedAction(t *testing.T) {
	t.Parallel()
	for _, action := range []string{
		permissions.WriteObjectAction,
		permissions.ImportCancelAction,
		permissions.WritePullRequestAction,
		permissions.CatalogCreateTableAction,
		permissions.CatalogUpdateNamespaceAction,
		permissions.CatalogDeleteViewAction,
		"catalog:CreateSomethingNew",
		ciWriteAction,
	} {
		require.Truef(t, isImmutabilityDeniedAction(action), "%s must be denied", action)
	}
	for _, action := range []string{
		"",
		permissions.ReadObjectAction,
		permissions.ListObjectsAction,
		permissions.CatalogReadTableAction,
		permissions.CatalogListTablesAction,
		permissions.PrepareGarbageCollectionCommitsAction,
		ActionReadDataset,
	} {
		require.Falsef(t, isImmutabilityDeniedAction(action), "%s must not be denied", action)
	}
}

// TestDatasetRepoIndex covers the cache: initial load, TTL refresh, immediate add/remove, and the
// promise that a failed refresh keeps the last known set instead of failing open.
func TestDatasetRepoIndex(t *testing.T) {
	t.Parallel()
	var (
		mu    sync.Mutex
		names = []string{"d1"}
		calls int
		fail  bool
	)
	now := time.Now()
	index := newDatasetRepoIndex(func(context.Context) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if fail {
			return nil, errors.New("kv down")
		}
		return append([]string(nil), names...), nil
	}, time.Minute, func() time.Time { return now }, logging.ContextUnavailable())

	ctx := t.Context()
	require.True(t, index.IsDatasetRepo(ctx, "d1"))
	require.False(t, index.IsDatasetRepo(ctx, "d2"))
	require.Equal(t, 1, calls, "the set is loaded once and then cached")
	require.False(t, index.IsDatasetRepo(ctx, ""), "an empty name is never a dataset")

	// Add/Remove are immediate: the service must not wait for a refresh.
	index.Add("d2")
	require.True(t, index.IsDatasetRepo(ctx, "d2"))
	index.Remove("d2")
	require.False(t, index.IsDatasetRepo(ctx, "d2"))
	require.Equal(t, 1, calls, "local updates do not reload")

	// A hit is answered from memory; a miss after the TTL reloads, and the local changes made
	// since the last load survive the reload.
	now = now.Add(2 * time.Minute)
	require.Equal(t, 1, calls, "a hit never reloads")
	require.True(t, index.IsDatasetRepo(ctx, "d1"))
	require.False(t, index.IsDatasetRepo(ctx, "d3"), "an unknown name is a miss")
	require.Equal(t, 2, calls, "the set is reloaded once the TTL expired")
	require.False(t, index.IsDatasetRepo(ctx, "d2"), "a name the service removed must not come back")

	// A failed refresh keeps the last known set, and does not turn every call into a reload.
	mu.Lock()
	fail = true
	mu.Unlock()
	now = now.Add(2 * time.Minute)
	require.False(t, index.IsDatasetRepo(ctx, "d4"), "an unknown name is still a miss")
	require.Equal(t, 3, calls)
	require.True(t, index.IsDatasetRepo(ctx, "d1"), "a KV failure must not forget the datasets we knew about")
	require.False(t, index.IsDatasetRepo(ctx, "d4"))
	require.Equal(t, 3, calls, "a failed refresh is retried only after the TTL")

	// Recovery.
	mu.Lock()
	fail = false
	names = []string{"d3"}
	mu.Unlock()
	now = now.Add(2 * time.Minute)
	require.True(t, index.IsDatasetRepo(ctx, "d3"), "a reload picks up the current set")
	require.False(t, index.IsDatasetRepo(ctx, "d1"), "a reload drops what the source no longer has")
}

// TestDatasetRepoIndexReadsTheStore covers the production loader: the index is built from the
// datasets KV partition, so a dataset created before this process started is protected at once.
func TestDatasetRepoIndexReadsTheStore(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := NewStore(kvtest.GetStore(ctx, t))
	require.NoError(t, store.CreateDataset(ctx, &DatasetRecord{Name: guardDataset, State: DatasetStateActive, CreatedAt: time.Now(), UpdatedAt: time.Now()}))

	index := NewDatasetRepoIndex(store, logging.ContextUnavailable())
	require.True(t, index.IsDatasetRepo(ctx, guardDataset))
	require.False(t, index.IsDatasetRepo(ctx, "events"))
}

// TestImmutabilityGuardAnswersForbiddenThroughTheCoreControllerContract pins that a guard denial
// is shaped like a policy denial: the datasets Authorizer reports it as a 403, and the core
// controller's authorization failure status (401 by default, 403 when configured) applies.
func TestImmutabilityGuardAnswersForbiddenThroughTheCoreControllerContract(t *testing.T) {
	t.Parallel()
	f := newGuardFixture(t)
	f.userWithGroup(t, "data-eng", "data-eng", "FSFullAccess")
	authz := NewAuthorizer(f.guard)
	err := authz.Check(t.Context(), Caller{Username: "data-eng"}, permissions.Node{Permission: permissions.Permission{
		Action:   permissions.WriteObjectAction,
		Resource: permissions.ObjectArn(guardDataset, "latest/x"),
	}}, "")
	var permErr *PermissionError
	require.ErrorAs(t, err, &permErr)
	require.Equal(t, http.StatusForbidden, permErr.HTTPStatusCode())
	require.Contains(t, err.Error(), "dataset repositories are immutable")
}

// TestDatasetRepoIndexConcurrentAccess exercises the cache the way the authorization path does:
// many readers, a refresh expiring under them, and the service adding and removing names at the
// same time. It is the -race test for the index.
func TestDatasetRepoIndexConcurrentAccess(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	index := newDatasetRepoIndex(func(context.Context) ([]string, error) {
		calls.Add(1)
		return []string{"d1", "d2"}, nil
	}, time.Millisecond, time.Now, logging.ContextUnavailable())

	ctx := t.Context()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				switch j % 3 {
				case 0:
					index.Add(fmt.Sprintf("local-%d-%d", i, j))
				case 1:
					index.Remove(fmt.Sprintf("local-%d-%d", i, j-1))
				default:
					require.True(t, index.IsDatasetRepo(ctx, "d1"))
				}
			}
		}(i)
	}
	wg.Wait()
	require.Positive(t, calls.Load(), "the set must have been loaded")
	require.True(t, index.IsDatasetRepo(ctx, "d1"))
}
