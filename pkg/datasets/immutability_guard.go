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

// datasetRepoCacheTTL bounds how stale the dataset-repository name set may be. The set is
// refreshed lazily - only when a denied action on a repository is being authorized - so
// ordinary read traffic never touches the KV store.
const datasetRepoCacheTTL = 2 * time.Second

type serviceCallerKey struct{}

// WithServiceCaller marks ctx as belonging to the datasets service itself, which the
// immutability guard lets through. The service normally writes through the catalog and never
// calls Authorize for its own repository writes (§4.6); the marker exists for the cases where
// it does.
func WithServiceCaller(ctx context.Context) context.Context {
	return context.WithValue(ctx, serviceCallerKey{}, true)
}

func isServiceCaller(ctx context.Context) bool {
	v, _ := ctx.Value(serviceCallerKey{}).(bool)
	return v
}

// DatasetRepoSet is a cached set of dataset repository names, refreshed from a loader.
type DatasetRepoSet struct {
	load func(ctx context.Context) ([]string, error)
	ttl  time.Duration
	now  func() time.Time

	mu      sync.Mutex
	names   map[string]struct{}
	fetched time.Time
	loaded  bool
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
// the last known set; with no known set at all it returns the error, so the caller fails closed.
func (s *DatasetRepoSet) Contains(ctx context.Context, repo string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.loaded || s.now().Sub(s.fetched) >= s.ttl {
		names, err := s.load(ctx)
		switch {
		case err == nil:
			set := make(map[string]struct{}, len(names))
			for _, n := range names {
				set[n] = struct{}{}
			}
			s.names, s.fetched, s.loaded = set, s.now(), true
		case !s.loaded:
			return false, err
		}
	}
	_, ok := s.names[repo]
	return ok, nil
}

// Invalidate forces the next Contains to reload.
func (s *DatasetRepoSet) Invalidate() {
	s.mu.Lock()
	s.loaded = false
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

// Authorize implements auth.Service.
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
