package auth_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
)

const (
	testCacheSize = 10
	testCacheTTL  = time.Hour
)

// countingUserSetFn returns a setFn that returns a user named username and
// counts how many times it was called.
func countingUserSetFn(username string, calls *int) auth.UserSetFn {
	return func() (*model.User, error) {
		*calls++
		return &model.User{Username: username}, nil
	}
}

// countingUserPoliciesSetFn returns a setFn that returns a single policy for
// userID and counts how many times it was called.
func countingUserPoliciesSetFn(userID string, calls *int) auth.UserPoliciesSetFn {
	return func() ([]*model.Policy, error) {
		*calls++
		return []*model.Policy{{DisplayName: "policy-" + userID}}, nil
	}
}

func TestLRUCacheInvalidateUser(t *testing.T) {
	t.Parallel()
	c := auth.NewLRUCache(testCacheSize, testCacheTTL, 0)

	aliceKey := auth.UserKey{Username: "alice"}
	bobKey := auth.UserKey{ID: "bob-id"}
	aliceCalls, bobCalls := 0, 0

	_, err := c.GetUser(aliceKey, countingUserSetFn("alice", &aliceCalls))
	require.NoError(t, err)
	_, err = c.GetUser(bobKey, countingUserSetFn("bob", &bobCalls))
	require.NoError(t, err)
	require.Equal(t, 1, aliceCalls)
	require.Equal(t, 1, bobCalls)

	// Both entries are cached.
	_, err = c.GetUser(aliceKey, countingUserSetFn("alice", &aliceCalls))
	require.NoError(t, err)
	_, err = c.GetUser(bobKey, countingUserSetFn("bob", &bobCalls))
	require.NoError(t, err)
	require.Equal(t, 1, aliceCalls)
	require.Equal(t, 1, bobCalls)

	// Invalidating all keys refetches both.
	c.InvalidateUser(aliceKey, bobKey)
	_, err = c.GetUser(aliceKey, countingUserSetFn("alice", &aliceCalls))
	require.NoError(t, err)
	_, err = c.GetUser(bobKey, countingUserSetFn("bob", &bobCalls))
	require.NoError(t, err)
	require.Equal(t, 2, aliceCalls)
	require.Equal(t, 2, bobCalls)

	// Invalidating a single key does not affect other keys.
	c.InvalidateUser(aliceKey)
	_, err = c.GetUser(aliceKey, countingUserSetFn("alice", &aliceCalls))
	require.NoError(t, err)
	_, err = c.GetUser(bobKey, countingUserSetFn("bob", &bobCalls))
	require.NoError(t, err)
	require.Equal(t, 3, aliceCalls)
	require.Equal(t, 2, bobCalls)

	// Invalidating a key that is not cached is a no-op.
	c.InvalidateUser(auth.UserKey{Username: "nobody"})
}

func TestLRUCacheInvalidateUserPolicies(t *testing.T) {
	t.Parallel()
	c := auth.NewLRUCache(testCacheSize, testCacheTTL, 0)

	aliceCalls, bobCalls := 0, 0

	_, err := c.GetUserPolicies("alice", countingUserPoliciesSetFn("alice", &aliceCalls))
	require.NoError(t, err)
	_, err = c.GetUserPolicies("bob", countingUserPoliciesSetFn("bob", &bobCalls))
	require.NoError(t, err)
	require.Equal(t, 1, aliceCalls)
	require.Equal(t, 1, bobCalls)

	// Both entries are cached.
	_, err = c.GetUserPolicies("alice", countingUserPoliciesSetFn("alice", &aliceCalls))
	require.NoError(t, err)
	_, err = c.GetUserPolicies("bob", countingUserPoliciesSetFn("bob", &bobCalls))
	require.NoError(t, err)
	require.Equal(t, 1, aliceCalls)
	require.Equal(t, 1, bobCalls)

	// Invalidating a single user's policies refetches only that user.
	c.InvalidateUserPolicies("alice")
	_, err = c.GetUserPolicies("alice", countingUserPoliciesSetFn("alice", &aliceCalls))
	require.NoError(t, err)
	_, err = c.GetUserPolicies("bob", countingUserPoliciesSetFn("bob", &bobCalls))
	require.NoError(t, err)
	require.Equal(t, 2, aliceCalls)
	require.Equal(t, 1, bobCalls)

	// Purging refetches everyone.
	c.PurgePolicies()
	_, err = c.GetUserPolicies("alice", countingUserPoliciesSetFn("alice", &aliceCalls))
	require.NoError(t, err)
	_, err = c.GetUserPolicies("bob", countingUserPoliciesSetFn("bob", &bobCalls))
	require.NoError(t, err)
	require.Equal(t, 3, aliceCalls)
	require.Equal(t, 2, bobCalls)

	// Purging an empty cache is a no-op.
	c.PurgePolicies()
}

func TestDummyCacheInvalidationIsNoop(t *testing.T) {
	t.Parallel()
	c := &auth.DummyCache{}

	calls := 0
	_, err := c.GetUser(auth.UserKey{Username: "alice"}, countingUserSetFn("alice", &calls))
	require.NoError(t, err)
	_, err = c.GetUser(auth.UserKey{Username: "alice"}, countingUserSetFn("alice", &calls))
	require.NoError(t, err)
	// DummyCache never caches, so both calls reached setFn.
	require.Equal(t, 2, calls)

	policyCalls := 0
	_, err = c.GetUserPolicies("alice", countingUserPoliciesSetFn("alice", &policyCalls))
	require.NoError(t, err)
	_, err = c.GetUserPolicies("alice", countingUserPoliciesSetFn("alice", &policyCalls))
	require.NoError(t, err)
	require.Equal(t, 2, policyCalls)

	// Invalidation calls must not panic and must not change DummyCache semantics.
	c.InvalidateUser(auth.UserKey{Username: "alice"})
	c.InvalidateUserPolicies("alice")
	c.PurgePolicies()
	_, err = c.GetUser(auth.UserKey{Username: "alice"}, countingUserSetFn("alice", &calls))
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}
