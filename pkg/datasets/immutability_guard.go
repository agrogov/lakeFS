package datasets

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// SEC-2: dataset repositories are immutable for every principal, whatever groups or policies
// grant them. The deny policy attached to well-known groups (ImmutabilityPolicy) is only a
// defence in depth, because write power also comes from custom groups, SSO-synced groups and
// policies attached directly to users. The authoritative enforcement lives here, in an
// auth.Service decorator: every Authorize call that requires an immutabilityDeniedActions action
// on a dataset repository is refused before the wrapped service is consulted.

// fsRepositoryArnPrefix is the ARN prefix of every repository-scoped fs resource.
var fsRepositoryArnPrefix = permissions.RepoArn("")

// catalogRepositoryArnPrefix is the ARN prefix of every repository-scoped catalog resource
// (ICEBERG_CATALOG_PLAN.md §7.2). Catalog ARNs carry the repository as the first path segment
// after the resource kind: namespace/<repo>/<ns>, table/<repo>/<ns>/<table> and
// view/<repo>/<ns>/<view>.
const catalogRepositoryArnPrefix = "arn:lakefs:catalog:::"

// errNoDatasetRepoSet is returned by Authorize when the guard was built without a name set. It
// is a failure to decide, never an allow: the immutability guarantee must not depend on the
// caller having wired the set correctly.
var errNoDatasetRepoSet = errors.New("datasets: no dataset repository set configured for the immutability guard")

// errDatasetRepoSetNotLoaded is returned when the set is asked to decide before any load ever
// completed. Like errNoDatasetRepoSet it is a failure to decide, never an allow.
var errDatasetRepoSetNotLoaded = errors.New("datasets: dataset repository set was never loaded")

// datasetRepoCacheTTL bounds how stale the dataset-repository name set may be. The set is
// refreshed lazily - only when a denied action on a repository is being authorized - so
// ordinary read traffic never touches the KV store.
const datasetRepoCacheTTL = 2 * time.Second

// datasetRepoLoadTimeout bounds one reload of the set. The reload is detached from the request
// that triggered it (see refresh), so it needs a bound of its own.
const datasetRepoLoadTimeout = 30 * time.Second

// errDatasetRepoSetLoadPanicked is recorded when the loader panicked, so the attempt counts as a
// failed refresh rather than leaving the set in the middle of one.
var errDatasetRepoSetLoadPanicked = errors.New("datasets: dataset repository set loader panicked")

// serviceCallerKey is the context key of the service-caller marker, and it is deliberately
// unexported. The marker only ever exempts a caller from the SEC-2 guard, and the guard's value
// is that a caller cannot switch it off, so nothing outside this package may build it: no
// exported helper sets it, and its only builder (withServiceCaller) lives in
// immutability_guard_test.go.
//
// Production does not build it either, because it does not need to. The datasets service writes
// the dataset repository with ReadOnly + force and never routes those writes through Authorize
// (DATASETS_DESIGN.md §4.6, §9.4), so the only bypass the design sanctions is the force flag
// itself, not a context value.
type serviceCallerKey struct{}

func isServiceCaller(ctx context.Context) bool {
	v, _ := ctx.Value(serviceCallerKey{}).(bool)
	return v
}

// DatasetRepoSet is a cached set of dataset repository names, refreshed from a loader.
//
// Contains sits on the authorization path of every write in the installation: every object
// write, commit, branch and repository mutation that names an immutability-denied action asks
// this set whether the repository is a dataset. It must therefore neither hold a lock across
// the loader - in production a full scan of the datasets/ KV partition (Store.ListDatasets) -
// nor turn a KV hiccup into an authorization error, because either one would stall or fail
// writes to unrelated repositories.
//
// Hence: the loader runs outside s.mu and only its result is swapped in under it, and a
// single-flight gate lets one goroutine reload while the rest wait for that one reload instead
// of starting scans of their own.
type DatasetRepoSet struct {
	load func(ctx context.Context) ([]string, error)
	ttl  time.Duration
	now  func() time.Time

	mu sync.Mutex
	// names is the most recent successfully loaded set. It is replaced, never mutated in place.
	names map[string]struct{}
	// fetched is the time of the last load attempt, successful or not. Counting failed attempts
	// against the TTL keeps a slow or failing store from being hit on every authorization.
	fetched time.Time
	// loaded reports that a load succeeded at least once, so names is meaningful. It survives
	// Invalidate: the last known set is what a failed refresh falls back to.
	loaded bool
	// stale asks the next Contains to reload before the TTL runs out (Invalidate). It is
	// cleared by the attempt it asked for, successful or not.
	stale bool
	// generation counts Invalidate calls, so a load that was already running when one landed
	// does not clear the flag: its scan may predate the record the invalidation is about.
	generation uint64
	// lastErr is the error of the last failed load. It is only answered while nothing was ever
	// loaded: then the guard has no set to compare against and must fail closed.
	lastErr error
	// refreshing is non-nil while a load runs and is closed when its result is visible. Callers
	// that find it set wait for it rather than reload.
	refreshing chan struct{}
}

// NewDatasetRepoSet builds a set that lists dataset names from the datasets KV partition.
func NewDatasetRepoSet(kvs kv.Store) *DatasetRepoSet {
	store := NewStore(kvs)
	return newDatasetRepoSet(func(ctx context.Context) ([]string, error) {
		recs, err := store.ListDatasets(ctx)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(recs))
		for _, rec := range recs {
			names = append(names, rec.Name)
		}
		return names, nil
	}, datasetRepoCacheTTL, time.Now)
}

func newDatasetRepoSet(load func(context.Context) ([]string, error), ttl time.Duration, now func() time.Time) *DatasetRepoSet {
	return &DatasetRepoSet{load: load, ttl: ttl, now: now}
}

// Contains reports whether repo is a dataset repository. When the refresh fails it falls back to
// the last known set; with no known set at all it returns an error, so the caller fails closed.
func (s *DatasetRepoSet) Contains(ctx context.Context, repo string) (bool, error) {
	s.mu.Lock()
	if s.fresh() {
		ok, err := s.cached(repo)
		s.mu.Unlock()
		return ok, err
	}
	done := s.refreshing
	leader := done == nil
	if leader {
		// Single flight: the caller that got here first reloads, everyone else joins its load.
		done = make(chan struct{})
		s.refreshing = done
	}
	s.mu.Unlock()

	if leader {
		s.refresh(ctx, done)
	} else {
		// No lock is held while waiting, so a burst of authorizations queues on the one scan
		// (bounded by its duration) instead of serialising behind a global mutex.
		select {
		case <-done:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cached(repo)
}

// refresh runs the loader outside s.mu and swaps its result in under it. Only the goroutine that
// won the single-flight gate calls it; it closes done once the new state is visible, so the
// waiters in Contains answer from the same load.
//
// The load runs on a context detached from the leader's cancellation and bounded by
// datasetRepoLoadTimeout: the leader's result is shared with every waiter, so one cancelled
// request must not hand its context.Canceled to the others. The state is published and done
// closed in a deferred call, so a panicking loader cannot leave the gate set and every later
// authorization waiting on a channel nobody closes.
func (s *DatasetRepoSet) refresh(ctx context.Context, done chan struct{}) {
	s.mu.Lock()
	generation := s.generation
	s.mu.Unlock()

	var names []string
	err := errDatasetRepoSetLoadPanicked // replaced unless the loader panics
	defer func() {
		s.mu.Lock()
		s.fetched = s.now()
		if s.generation == generation {
			// No Invalidate landed while the load ran, so this attempt is the one that was owed
			// and its result is not known to be incomplete.
			s.stale = false
		}
		if err != nil {
			// Failed refresh: keep the last known set, if any. A KV hiccup must not fail every
			// write in the installation, and the attempt counts against the TTL, so the store is
			// retried on the next TTL rather than on every authorization.
			s.lastErr = err
		} else {
			set := make(map[string]struct{}, len(names))
			for _, n := range names {
				set[n] = struct{}{}
			}
			s.names, s.loaded, s.lastErr = set, true, nil
		}
		s.refreshing = nil
		s.mu.Unlock()
		close(done)
	}()

	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), datasetRepoLoadTimeout)
	defer cancel()
	names, err = s.load(loadCtx)
}

// fresh reports whether the cached set may answer without a load: no invalidation is pending and
// the TTL has not run out since the last attempt. The zero fetched time makes the first call
// load.
func (s *DatasetRepoSet) fresh() bool {
	return !s.stale && s.now().Sub(s.fetched) < s.ttl
}

// cached answers repo from the current set. With no successful load ever it returns the loader's
// error instead of a set, so a guard that cannot name the dataset repositories refuses to decide
// rather than allowing the write.
func (s *DatasetRepoSet) cached(repo string) (bool, error) {
	if !s.loaded {
		if s.lastErr != nil {
			return false, s.lastErr
		}
		return false, errDatasetRepoSetNotLoaded
	}
	_, ok := s.names[repo]
	return ok, nil
}

// Invalidate forces the next Contains to reload while keeping the last known set: if that reload
// fails, the set still answers instead of failing every authorization in the installation.
func (s *DatasetRepoSet) Invalidate() {
	s.mu.Lock()
	s.stale = true
	s.generation++
	s.mu.Unlock()
}

// invalidateGuardedRepos drops the cached dataset names of the SEC-2 guard, when the service's
// auth service is the guard (cmd/lakefs-sso wraps it so). Other nodes keep their copy until its
// TTL (datasetRepoCacheTTL) runs out.
func (s *Service) invalidateGuardedRepos() {
	if guard, ok := s.authService.(*ImmutabilityAuthService); ok && guard.repos != nil {
		guard.repos.Invalidate()
	}
}

// immutableActionSet is immutabilityDeniedActions as a set.
var immutableActionSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(immutabilityDeniedActions))
	for _, a := range immutabilityDeniedActions {
		m[a] = struct{}{}
	}
	return m
}()

// ImmutabilityAuthService decorates an auth.Service: Authorize refuses every
// immutabilityDeniedActions action on a dataset repository (SEC-2).
type ImmutabilityAuthService struct {
	auth.Service
	repos *DatasetRepoSet
}

// NewImmutabilityAuthService wraps inner with the immutability guard. repos names the dataset
// repositories; use NewDatasetRepoSet(kvStore) in production.
func NewImmutabilityAuthService(inner auth.Service, repos *DatasetRepoSet) *ImmutabilityAuthService {
	return &ImmutabilityAuthService{Service: inner, repos: repos}
}

// Authorize implements auth.Service. There is no context in which a caller can switch the guard
// off: the service-caller marker (see serviceCallerKey) is unexported and built only by the
// package's tests, and production relies on the force flag of §4.6/§9.4 instead.
func (s *ImmutabilityAuthService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	if req == nil || isServiceCaller(ctx) {
		return s.Service.Authorize(ctx, req)
	}
	denied, err := s.denies(ctx, req.RequiredPermissions)
	if err != nil {
		return nil, err
	}
	if denied {
		return &auth.AuthorizationResponse{Allowed: false, Error: auth.ErrInsufficientPermissions}, nil
	}
	return s.Service.Authorize(ctx, req)
}

// denies reports whether the requirement cannot be satisfied: an And node with a forbidden leaf
// is unsatisfiable, and an Or node is unsatisfiable only when every alternative is forbidden.
func (s *ImmutabilityAuthService) denies(ctx context.Context, node permissions.Node) (bool, error) {
	switch node.Type {
	case permissions.NodeTypeNode:
		return s.leafForbidden(ctx, node.Permission)
	case permissions.NodeTypeAnd:
		for _, child := range node.Nodes {
			d, err := s.denies(ctx, child)
			if err != nil || d {
				return d, err
			}
		}
		return false, nil
	case permissions.NodeTypeOr:
		if len(node.Nodes) == 0 {
			return false, nil
		}
		for _, child := range node.Nodes {
			d, err := s.denies(ctx, child)
			if err != nil {
				return false, err
			}
			if !d {
				return false, nil
			}
		}
		return true, nil
	default:
		return false, nil
	}
}

func (s *ImmutabilityAuthService) leafForbidden(ctx context.Context, p permissions.Permission) (bool, error) {
	if _, ok := immutableActionSet[p.Action]; !ok {
		return false, nil
	}
	repo, ok := repositoryOfArn(p.Resource)
	if !ok {
		return false, nil
	}
	if s.repos == nil {
		return false, errNoDatasetRepoSet
	}
	return s.repos.Contains(ctx, repo)
}

// repositoryOfArn returns the repository an fs or catalog resource ARN belongs to. Catalog
// actions are included because immutabilityDeniedActions covers the catalog writes of
// ICEBERG_CATALOG_PLAN.md §7.2; an fs-only parser would silently leave them unguarded.
func repositoryOfArn(resource string) (string, bool) {
	if rest, ok := strings.CutPrefix(resource, fsRepositoryArnPrefix); ok {
		repo, _, _ := strings.Cut(rest, "/")
		return repo, repo != ""
	}
	rest, ok := strings.CutPrefix(resource, catalogRepositoryArnPrefix)
	if !ok {
		return "", false
	}
	kind, tail, ok := strings.Cut(rest, "/")
	if !ok {
		return "", false
	}
	switch kind {
	case "namespace", "table", "view":
		repo, _, _ := strings.Cut(tail, "/")
		return repo, repo != ""
	default:
		return "", false
	}
}
