package datasets

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

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

		// The datasets service itself is exempt.
		svcCtx := WithServiceCaller(t.Context())
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
