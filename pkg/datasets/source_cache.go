package datasets

// source_cache.go is the caching half of SEC-3. The authoritative source-read check authorizes
// fs:ReadObject on every object the materialiser reads (materialize.go, SourceReader.CheckObject),
// which is one auth evaluation per listed key: a prefix item over a large table would pay it
// millions of times, always with the same answer. sourceReadCache memoises those decisions for one
// caller under one policy set, keyed by the path prefix that decides them - the literal prefix of
// the deepest policy wildcard that covers the object.
//
// Why that key is sound. A policy resource is a glob: ArnMatch matches the policy side as the
// pattern, so a resource can match a requested object ARN only if the resource's literal prefix L
// (everything before its first '*' or '?') is a prefix of that ARN. For a requested ARN the key is
// the longest such L that is a prefix of it. Two ARNs with the same key therefore share the exact
// set of literal prefixes that are prefixes of them (a longer one would be the key, and every
// shorter one is a prefix of the key), so the same set of resources can match both. They decide
// identically as long as the wildcard part of every resource whose prefix is a prefix of the key
// matches every ARN under that prefix - which this cache guarantees by only using prefix keys when
// every wildcard part is a run of '*' (see policyPrefixes). A resource with anything else in it
// ('?', or literal text after a '*') matches some paths under a prefix and not others; such a
// policy set disables prefix keys, and the cache then only remembers exact ARNs.
//
// A resource with no wildcard at all matches exactly one ARN, not the paths under it, so it never
// contributes a prefix key: "object/x.csv" says nothing about "object/x.csv.old". An ARN equal to
// such a resource is keyed by itself instead, in a key space of its own, so its decision is never
// shared with - nor borrowed from - the longer ARNs that start with it.
//
// The policy set is enumerated once per SourceReader, and a SourceReader is built per operation, so
// a policy written while an operation is running cannot be picked up in the middle of it: the cache
// key is (policy set, path prefix), never just the path prefix.
//
// Nothing here decides anything: a miss goes through Authorizer.Check, and a failure to load the
// policy set only costs the optimisation, never a check.

import (
	"context"
	"strings"
	"sync"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
)

// EffectivePoliciesLister is the part of auth.Service that lets a SourceReader see the caller's
// policy set. NewAuthorizer asserts its auth service to it; an auth.Authorizer that does not
// implement it (a test double, or a decorator that only forwards Authorize) simply keeps the cache
// in exact-ARN mode.
type EffectivePoliciesLister interface {
	ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
}

// sourceDecision is a cached source-read decision.
type sourceDecision int

const (
	// sourceDecisionAllow reports that the caller may read every object under the cached key.
	sourceDecisionAllow sourceDecision = iota
	// sourceDecisionDeny reports that the caller may read no object under the cached key.
	sourceDecisionDeny
)

// sourceReadCache memoises the fs:ReadObject decisions of one caller under one policy set. The zero
// value is not usable: build it with newSourceReadCache.
type sourceReadCache struct {
	// lister enumerates the caller's policy set. Nil disables prefix keys.
	lister EffectivePoliciesLister

	// user is the principal the policy set belongs to.
	user string

	mu     sync.Mutex
	loaded bool
	// prefixes holds the literal prefix of every wildcard resource of the policy set. A non-nil
	// slice - including an empty one, which means "no policy matches anything" - enables prefix
	// keys; nil means the set could not be analysed and keys are exact ARNs.
	prefixes []string
	// exact holds every resource of the policy set that has no wildcard: an ARN equal to one is
	// keyed by itself (exactKeyPrefix + ARN), never by a prefix.
	exact     map[string]struct{}
	decisions map[string]sourceDecision
}

// newSourceReadCache builds the source-read decision cache of user. A nil lister keeps exact-ARN
// memoisation only.
func newSourceReadCache(lister EffectivePoliciesLister, user string) *sourceReadCache {
	return &sourceReadCache{
		lister:    lister,
		user:      user,
		decisions: make(map[string]sourceDecision),
	}
}

// exactKeyPrefix marks a cache key that stands for one ARN only. No ARN starts with a NUL, so an
// exact key never equals a prefix key.
const exactKeyPrefix = "\x00"

// key returns the cache key of the object ARN arn: the longest literal policy prefix that is a
// prefix of arn, or arn itself when the policy set cannot be analysed or names arn exactly. The
// first call enumerates the policy set; the result is kept for the lifetime of the cache, i.e. for
// one operation.
func (c *sourceReadCache) key(ctx context.Context, arn string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.load(ctx)
	if c.prefixes == nil {
		return exactKeyPrefix + arn
	}
	if _, ok := c.exact[arn]; ok {
		return exactKeyPrefix + arn
	}
	key := ""
	for _, prefix := range c.prefixes {
		if len(prefix) > len(key) && strings.HasPrefix(arn, prefix) {
			key = prefix
		}
	}
	return key
}

// load enumerates the caller's policy set, once. A failure leaves the cache in exact-ARN mode: the
// cache is an optimisation, and the authoritative per-object check still runs.
func (c *sourceReadCache) load(ctx context.Context) {
	if c.loaded {
		return
	}
	c.loaded = true
	if c.lister == nil {
		return
	}
	policies, _, err := c.lister.ListEffectivePolicies(ctx, c.user, &model.PaginationParams{Amount: -1})
	if err != nil {
		return
	}
	prefixes, exact, ok := policyPrefixes(policies, c.user)
	if !ok {
		return
	}
	c.prefixes = prefixes
	c.exact = make(map[string]struct{}, len(exact))
	for _, resource := range exact {
		c.exact[resource] = struct{}{}
	}
}

// get returns the cached decision of key.
func (c *sourceReadCache) get(key string) (sourceDecision, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	decision, ok := c.decisions[key]
	return decision, ok
}

// put records the decision of key.
func (c *sourceReadCache) put(key string, decision sourceDecision) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.decisions[key] = decision
}

// policyPrefixes returns the literal prefix of every wildcard resource of every statement of
// policies, and every resource that has no wildcard at all, with the ${user} placeholder of
// pkg/auth already interpolated. It reports false when the set cannot be analysed into prefixes,
// which keeps the caller on exact-ARN caching: a resource that is not a valid policy resource list,
// or whose wildcard part is not a run of '*' characters.
func policyPrefixes(policies []*model.Policy, user string) (prefixes, exact []string, ok bool) {
	prefixes = make([]string, 0, len(policies))
	exact = make([]string, 0)
	for _, policy := range policies {
		if policy == nil {
			continue
		}
		for _, statement := range policy.Statement {
			resources, err := auth.ParsePolicyResourceAsList(statement.Resource)
			if err != nil {
				return nil, nil, false
			}
			for _, resource := range resources {
				// pkg/auth interpolates ${user} before it matches a resource (interpolateUser).
				resource = strings.ReplaceAll(resource, "${user}", user)
				literal, wildcard := splitResourceWildcard(resource)
				switch {
				case wildcard == "":
					// Matches this one ARN only; it is no prefix of anything (SEC-3).
					exact = append(exact, literal)
				case strings.Trim(wildcard, "*") != "":
					return nil, nil, false
				default:
					prefixes = append(prefixes, literal)
				}
			}
		}
	}
	return prefixes, exact, true
}

// splitResourceWildcard splits a policy resource glob at its first wildcard character.
func splitResourceWildcard(resource string) (literal, wildcard string) {
	i := strings.IndexAny(resource, "*?")
	if i < 0 {
		return resource, ""
	}
	return resource[:i], resource[i:]
}
