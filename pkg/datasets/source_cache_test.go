package datasets

// source_cache_test.go covers the SEC-3 decision cache (source_cache.go): which paths share a
// cached decision, that an unanalysable policy set falls back to exact ARNs, and that the cache is
// scoped to one policy set rather than to the process.

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// countingAuthService counts the Authorize calls that reach the wrapped auth service, so a test can
// observe how many decisions the source-read cache actually asked for. It embeds auth.Service, so
// it stays an EffectivePoliciesLister and the cache keeps its prefix keys.
type countingAuthService struct {
	auth.Service

	mu    sync.Mutex
	calls int
}

// Authorize implements auth.Authorizer.
func (s *countingAuthService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.Service.Authorize(ctx, req)
}

func (s *countingAuthService) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// newCountingAuthzFixture builds the authz fixture plus an Authorizer whose auth calls are
// counted, and a user that may read everything except the paths the test denies.
func newCountingAuthzFixture(t *testing.T, denies ...string) (*countingAuthService, *Authorizer, string) {
	t.Helper()
	const user = "reader"
	f := newAuthzFixture(t)
	f.user(t, user)
	f.policy(t, "allow-all", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"fs:*"},
		Resource: permissions.All,
	}})
	f.attachToUser(t, "allow-all", user)
	if len(denies) > 0 {
		statements := make(model.Statements, 0, len(denies))
		for _, resource := range denies {
			statements = append(statements, model.Statement{
				Effect:   model.StatementEffectDeny,
				Action:   []string{permissions.ReadObjectAction},
				Resource: resource,
			})
		}
		f.policy(t, "deny", statements)
		f.attachToUser(t, "deny", user)
	}
	counting := &countingAuthService{Service: f.svc}
	return counting, NewAuthorizer(counting), user
}

// authorizeOnly hides every auth.Service method except Authorize, so NewAuthorizer cannot list
// policies and the cache has to stay in exact-ARN mode.
type authorizeOnly struct {
	auth.Authorizer

	mu    sync.Mutex
	calls int
}

// Authorize implements auth.Authorizer.
func (s *authorizeOnly) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.Authorizer.Authorize(ctx, req)
}

// TestSourceReadCacheBuckets pins the bucketing of source_cache.go: one authorization per (policy
// set, deepest policy wildcard prefix), and never one decision for paths a policy could treat
// differently.
func TestSourceReadCacheBuckets(t *testing.T) {
	const src = "src"
	counting, authz, user := newCountingAuthzFixture(t,
		permissions.ObjectArn(src, "secret/*"),   // a deny on a whole subtree
		permissions.ObjectArn(src, "private/a*"), // a deny whose literal prefix ends mid-segment
	)
	reader := authz.SourceReader(Caller{Username: user})
	ctx := t.Context()

	// The whole-repository bucket: only the "*" allow can reach these paths, so one decision
	// serves all of them.
	for _, path := range []string{"pub/a.csv", "pub/b.csv", "other/c.csv"} {
		require.NoError(t, reader.CheckObject(ctx, src, path))
	}
	require.Equal(t, 1, counting.count(), "one decision covers every path no deny can reach")

	// .../object/secret/ is its own bucket: the first object is authorized and denied, and the
	// rest reuse the denial (the materialiser stops at the first one anyway).
	require.ErrorIs(t, reader.CheckObject(ctx, src, "secret/a.csv"), ErrSourceReadDenied)
	require.Equal(t, 2, counting.count())
	require.ErrorIs(t, reader.CheckObject(ctx, src, "secret/b.csv"), ErrSourceReadDenied)
	require.Equal(t, 2, counting.count(), "a denied bucket is cached like an allowed one")

	// .../object/private/a* ends mid-segment, so its bucket is the literal prefix itself: a1 and
	// a2 share it, and a path outside it falls back to the whole-repository bucket.
	require.ErrorIs(t, reader.CheckObject(ctx, src, "private/a1"), ErrSourceReadDenied)
	require.Equal(t, 3, counting.count())
	require.ErrorIs(t, reader.CheckObject(ctx, src, "private/a2"), ErrSourceReadDenied)
	require.Equal(t, 3, counting.count(), "the mid-segment bucket is shared")
	require.NoError(t, reader.CheckObject(ctx, src, "private/b1"))
	require.Equal(t, 3, counting.count(), "private/b1 is outside both denies, in the cached bucket")
	require.NoError(t, reader.CheckObject(ctx, src, "sec/c.csv"))
	require.Equal(t, 3, counting.count(), "a sibling of the denied prefix is not denied")
}

// TestSourceReadCacheDenyIsNotLeakedByAnAllowedNeighbour is the security property of the cache: an
// allow cached for one path is never reused for a path a Deny covers.
func TestSourceReadCacheDenyIsNotLeakedByAnAllowedNeighbour(t *testing.T) {
	const src = "src"
	_, authz, user := newCountingAuthzFixture(t, permissions.ObjectArn(src, "secret/*"))
	reader := authz.SourceReader(Caller{Username: user})
	ctx := t.Context()

	// Warm every bucket that is not the denied one.
	for _, path := range []string{"pub/a.csv", "sec/b.txt", "secretive/c.txt"} {
		require.NoError(t, reader.CheckObject(ctx, src, path))
	}
	require.ErrorIs(t, reader.CheckObject(ctx, src, "secret/d.csv"), ErrSourceReadDenied)
	require.ErrorIs(t, reader.CheckObject(ctx, src, "secret/e.csv"), ErrSourceReadDenied)
}

// TestSourceReadCacheExactFallback: a policy set the cache cannot analyse into prefixes costs one
// authorization per distinct path, never fewer; only the exact ARN is remembered.
func TestSourceReadCacheExactFallback(t *testing.T) {
	const src = "src"
	// A wildcard in the middle of a segment: .../object/data/*/x matches some paths under
	// .../object/data/ and not others, so no prefix of it may be a cache key.
	counting, authz, user := newCountingAuthzFixture(t, permissions.ObjectArn(src, "data/*/x"))
	reader := authz.SourceReader(Caller{Username: user})
	ctx := t.Context()

	require.NoError(t, reader.CheckObject(ctx, src, "pub/a"))
	require.NoError(t, reader.CheckObject(ctx, src, "pub/b"))
	require.Equal(t, 2, counting.count(), "an unanalysable policy set disables prefix keys")
	require.NoError(t, reader.CheckObject(ctx, src, "pub/a"))
	require.Equal(t, 2, counting.count(), "the exact ARN is still memoised")
}

// TestSourceReadCacheWithoutPolicyLister: an auth service that cannot list policies keeps exact-ARN
// keys, so every object is authorized.
func TestSourceReadCacheWithoutPolicyLister(t *testing.T) {
	const (
		src  = "src"
		user = "reader"
	)
	f := newAuthzFixture(t)
	f.user(t, user)
	f.policy(t, "allow-all", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"fs:*"},
		Resource: permissions.All,
	}})
	f.attachToUser(t, "allow-all", user)

	wrapped := &authorizeOnly{Authorizer: f.svc}
	reader := NewAuthorizer(wrapped).SourceReader(Caller{Username: user})
	ctx := t.Context()
	require.NoError(t, reader.CheckObject(ctx, src, "pub/a"))
	require.NoError(t, reader.CheckObject(ctx, src, "pub/b"))
	require.Equal(t, 2, wrapped.calls, "without the policy set every path is authorized")
}

// TestSourceReadCacheIsPerPolicySet: the cache belongs to a SourceReader, and a SourceReader is
// built per operation, so a Deny written after an operation started is honoured by the next one.
func TestSourceReadCacheIsPerPolicySet(t *testing.T) {
	const (
		src  = "src"
		user = "reader"
	)
	f := newAuthzFixture(t)
	ctx := t.Context()
	f.user(t, user)
	f.policy(t, "allow-all", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"fs:*"},
		Resource: permissions.All,
	}})
	f.attachToUser(t, "allow-all", user)

	reader := NewAuthorizer(f.svc).SourceReader(Caller{Username: user})
	require.NoError(t, reader.CheckObject(ctx, src, "secret/a.csv"))
	require.NoError(t, reader.CheckObject(ctx, src, "secret/b.csv"))

	f.policy(t, "deny-secret", model.Statements{{
		Effect:   model.StatementEffectDeny,
		Action:   []string{permissions.ReadObjectAction},
		Resource: permissions.ObjectArn(src, "secret/*"),
	}})
	f.attachToUser(t, "deny-secret", user)

	next := NewAuthorizer(f.svc).SourceReader(Caller{Username: user})
	require.ErrorIs(t, next.CheckObject(ctx, src, "secret/a.csv"), ErrSourceReadDenied,
		"a new operation sees the new policy set")
	require.NoError(t, next.CheckObject(ctx, src, "pub/a.csv"))
}

// TestPolicyPrefixes pins how the cache reads a policy set into literal prefixes.
func TestPolicyPrefixes(t *testing.T) {
	t.Parallel()
	policies := []*model.Policy{{Statement: model.Statements{
		{Effect: model.StatementEffectAllow, Action: []string{"fs:*"}, Resource: permissions.ObjectArn("src", "*")},
		{
			Effect: model.StatementEffectDeny,
			Action: []string{permissions.ReadObjectAction},
			Resource: `["arn:lakefs:fs:::repository/src/object/secret/*",` +
				`"arn:lakefs:fs:::repository/src/object/${user}/*"]`,
		},
	}}}
	prefixes, ok := policyPrefixes(policies, "alice")
	require.True(t, ok)
	require.ElementsMatch(t, []string{
		"arn:lakefs:fs:::repository/src/object/",
		"arn:lakefs:fs:::repository/src/object/secret/",
		"arn:lakefs:fs:::repository/src/object/alice/",
	}, prefixes, "the ${user} placeholder is interpolated, the way pkg/auth does")

	// A wildcard the cache cannot reason about disables prefix keys: the decision it would cache
	// for a prefix is not constant over that prefix.
	for _, resource := range []string{
		"arn:lakefs:fs:::repository/src/object/data/*/x", // wildcard in the middle of the path
		"arn:lakefs:fs:::repository/src/object/da?a/",    // single-character wildcard
	} {
		t.Run(resource, func(t *testing.T) {
			t.Parallel()
			_, ok := policyPrefixes([]*model.Policy{{Statement: model.Statements{{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"fs:*"},
				Resource: resource,
			}}}}, "alice")
			require.False(t, ok, "%q must disable prefix keys", resource)
		})
	}
}
