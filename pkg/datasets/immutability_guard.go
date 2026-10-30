// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// immutability_guard.go enforces the §9.4 immutability guarantee in the authorizer instead of
// through per-group policy attachments (SEC-2 of DATASETS_REVIEW.md).
//
// Why the authorizer and not a policy attachment: the per-dataset deny policy of §9.4
// (ImmutabilityPolicy) only protects the principals the bootstrap knows about - the base groups of
// the two group templates plus datasets.immutable_guard_groups. Under `internal` RBAC, write power
// also comes from custom groups, from groups created by the SSO group sync (pkg/sso/group_sync.go)
// and from policies attached directly to a user. None of those were covered, so a member of, say, a
// custom `data-eng` group holding FSFullAccess could upload into <dataset>/latest/..., move a
// dataset tag, delete the `latest` branch or drop the lakefs.dataset repository metadata. That
// breaks the "a published version is immutable" guarantee every consumer relies on.
//
// What the guard does: WrapAuthService decorates the auth.Service the server hands to the core API
// and the S3 gateway. Every authorization request that asks for one of
// immutabilityDeniedActions on a resource under a dataset repository is answered with a denial,
// whoever the caller is and however the permission was granted. The service itself is not
// affected: it writes with graveler.WithForce(true) and never authorizes as a principal
// (pkg/datasets/service.go, §4.6), so there is no service principal to exempt.
//
// Which repositories are datasets: the guard keeps a cached set of dataset names read from the
// datasets KV partition (DatasetsPartitionKey). The cache is refreshed lazily with a TTL and is
// updated immediately by the service when it creates or deletes a dataset, so a dataset repository
// is protected from the moment it exists. A failed refresh keeps the last known set (and waits for
// the TTL before retrying, so a KV outage does not turn every upload into a KV scan).
//
// The per-group attachment machinery of authz.go is kept as defence in depth: it denies through
// the ordinary policy engine, and it is what protects an installation whose authorizer is not
// wrapped (a stock lakeFS binary, or a test that builds the router without the decorator).
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS documentation only.
// No code, SQL, protobuf or UI asset from lakeFS Enterprise, and nothing from upstream
// lakeFS >= 1.87, was used.
package datasets

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// DefaultDatasetRepoIndexTTL bounds how stale the cached set of dataset names may be. The
	// service updates the set synchronously on create and delete, so the TTL only matters for a
	// dataset created by another node in a multi-node deployment.
	DefaultDatasetRepoIndexTTL = 30 * time.Second

	// fsRepositoryArnPrefix is the resource prefix the core API authorizes repository, branch,
	// tag and object permissions against (pkg/permissions RepoArn/ObjectArn/BranchArn/TagArn).
	fsRepositoryArnPrefix = "arn:lakefs:fs:::repository/"

	// catalogArnPrefix is the prefix of the Iceberg catalog resources (pkg/permissions
	// CatalogNamespaceArn and friends), whose first path segment is the repository.
	catalogArnPrefix = "arn:lakefs:catalog:::"
)

// --- the cached set of dataset repositories ------------------------------------------------

// DatasetRepoIndex is the cached set of repository names that hold datasets. It is read on the
// authorization path, so it never blocks on the KV store more than one goroutine at a time and
// never fails: a stale set is always preferable to an authorization failure.
//
// The zero DatasetRepoIndex is unusable: build one with NewDatasetRepoIndex.
type DatasetRepoIndex struct {
	mu       sync.RWMutex
	names    map[string]struct{}
	loaded   bool
	loadedAt time.Time
	// added and removed hold the changes the service made since the last successful load. A
	// refresh replays them on top of what it read, so a dataset the service created while a scan
	// was in flight is not forgotten, and one it deleted is not resurrected.
	added   map[string]struct{}
	removed map[string]struct{}

	refreshing atomic.Bool
	// firstLoad runs the initial load once and makes the callers that arrive while it is in flight
	// wait for it: before the set has ever been read, "not a dataset" is not an answer the guard
	// may act on.
	firstLoad sync.Once

	ttl    time.Duration
	now    func() time.Time
	load   func(ctx context.Context) ([]string, error)
	logger logging.Logger
}

// NewDatasetRepoIndex returns an index that reads the dataset names from store, a Store over the
// datasets KV partition. logger reports a failed refresh; it may be nil.
func NewDatasetRepoIndex(store *Store, logger logging.Logger) *DatasetRepoIndex {
	return newDatasetRepoIndex(func(ctx context.Context) ([]string, error) {
		if store == nil {
			return nil, nil
		}
		records, err := store.ListDatasets(ctx)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(records))
		for _, rec := range records {
			if rec != nil {
				names = append(names, rec.Name)
			}
		}
		return names, nil
	}, DefaultDatasetRepoIndexTTL, time.Now, logger)
}

// newDatasetRepoIndex is the testable constructor: load, the TTL and the clock are injected.
func newDatasetRepoIndex(load func(ctx context.Context) ([]string, error), ttl time.Duration, now func() time.Time, logger logging.Logger) *DatasetRepoIndex {
	if ttl <= 0 {
		ttl = DefaultDatasetRepoIndexTTL
	}
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = logging.ContextUnavailable()
	}
	return &DatasetRepoIndex{
		names:  make(map[string]struct{}),
		ttl:    ttl,
		now:    now,
		load:   load,
		logger: logger,
	}
}

// IsDatasetRepo reports whether repo is the name of a dataset repository.
//
// The first call loads the set and every concurrent first caller waits for it, so the guard never
// treats "the set is still empty because nobody read it yet" as "this repository is not a dataset".
// After that it refreshes when the TTL expired, at most once concurrently: a caller that arrives
// during a refresh keeps using the previous snapshot. The lookup is done under the read lock,
// because Add and Remove mutate the map the service shares with the guard.
func (i *DatasetRepoIndex) IsDatasetRepo(ctx context.Context, repo string) bool {
	if i == nil || repo == "" {
		return false
	}
	i.firstLoad.Do(func() { i.refresh(ctx) })
	if i.contains(repo) {
		return true
	}
	if !i.stale() {
		return false
	}
	i.refresh(ctx)
	return i.contains(repo)
}

// contains reports whether repo is in the current set.
func (i *DatasetRepoIndex) contains(repo string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	_, ok := i.names[repo]
	return ok
}

// stale reports whether the set must be reloaded before it can be trusted to answer "no".
func (i *DatasetRepoIndex) stale() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return !i.loaded || i.now().Sub(i.loadedAt) >= i.ttl
}

// Add records that repo is a dataset repository. It is what makes a dataset immutable from the
// moment the service created its repository, without waiting for a refresh.
func (i *DatasetRepoIndex) Add(repo string) {
	if i == nil || repo == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.initLocked()
	i.names[repo] = struct{}{}
	i.added[repo] = struct{}{}
	delete(i.removed, repo)
}

// Remove forgets repo, after the service deleted it.
func (i *DatasetRepoIndex) Remove(repo string) {
	if i == nil || repo == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.initLocked()
	delete(i.names, repo)
	i.removed[repo] = struct{}{}
	delete(i.added, repo)
}

// initLocked makes the maps usable. The caller holds the write lock.
func (i *DatasetRepoIndex) initLocked() {
	if i.names == nil {
		i.names = make(map[string]struct{})
	}
	if i.added == nil {
		i.added = make(map[string]struct{})
	}
	if i.removed == nil {
		i.removed = make(map[string]struct{})
	}
}

// refresh reloads the set from the source of truth and replays the local changes made since the
// last successful load. Only one goroutine refreshes at a time; the others keep the previous
// snapshot, which is what keeps a burst of requests from scanning the datasets partition. It never
// fails: a failed load leaves the last known set in place.
func (i *DatasetRepoIndex) refresh(ctx context.Context) {
	if i == nil {
		return
	}
	if !i.refreshing.CompareAndSwap(false, true) {
		return
	}
	defer i.refreshing.Store(false)

	names, err := i.load(ctx)

	i.mu.Lock()
	defer i.mu.Unlock()
	i.initLocked()
	i.loadedAt = i.now()
	if err != nil {
		// Keep the last known set, and do not retry before the TTL: a KV outage must not turn
		// every authorization decision into a KV round trip.
		i.logger.WithError(err).Debug("datasets: cannot refresh the dataset repository set; keeping the last known set")
		return
	}

	next := make(map[string]struct{}, len(names)+len(i.added))
	for _, name := range names {
		if name != "" {
			next[name] = struct{}{}
		}
	}
	for name := range i.added {
		next[name] = struct{}{}
	}
	for name := range i.removed {
		delete(next, name)
	}
	i.names, i.loaded = next, true
	i.added = make(map[string]struct{})
	i.removed = make(map[string]struct{})
}

// --- the process-wide index ----------------------------------------------------------------

// installedDatasetRepoIndex is the index shared between the authorizer decorator built by
// WrapAuthService and the Service started by RoutesBuilder. The decorator is built while the auth
// service is constructed, before the datasets routes exist, so the two cannot pass the index to
// each other through a constructor argument.
var installedDatasetRepoIndex atomic.Pointer[DatasetRepoIndex]

// installDatasetRepoIndex replaces the process-wide index, so the last installation wins.
func installDatasetRepoIndex(index *DatasetRepoIndex) *DatasetRepoIndex {
	installedDatasetRepoIndex.Store(index)
	return index
}

// InstalledDatasetRepoIndex returns the index the authorizer decorator installed, or nil when no
// decorator was installed in this process.
func InstalledDatasetRepoIndex() *DatasetRepoIndex {
	return installedDatasetRepoIndex.Load()
}

// installedOrNewDatasetRepoIndex returns the index the authorizer decorator installed, or installs a
// fresh one built from kvs. RoutesBuilder uses it so the datasets Service always updates the set the
// guard reads, including on an installation (or a test) whose auth service was not wrapped.
func installedOrNewDatasetRepoIndex(kvs kv.Store, logger logging.Logger) *DatasetRepoIndex {
	if index := InstalledDatasetRepoIndex(); index != nil {
		return index
	}
	return installDatasetRepoIndex(NewDatasetRepoIndex(NewStore(kvs), logger))
}

// WrapAuthService decorates authService with the §9.4 immutability guard and installs the dataset
// repository index the returned service consults, so the datasets Service can keep it up to date
// (InstalledDatasetRepoIndex).
//
// It is called by the lakefs-sso auth-service builder (cmd/lakefs-sso/main.go), which is the only
// place that sees the auth service the core API and the S3 gateway use. A nil service or a nil KV
// store leaves the service unwrapped: a build without a KV store has no datasets either.
func WrapAuthService(authService auth.Service, kvs kv.Store, logger logging.Logger) auth.Service {
	if authService == nil {
		return nil
	}
	if kvs == nil {
		return authService
	}
	index := installDatasetRepoIndex(NewDatasetRepoIndex(NewStore(kvs), logger))
	return NewImmutabilityGuardService(authService, index, logger)
}

// NewImmutabilityGuardService returns authService with the §9.4 immutability guard in front of
// Authorize. The returned service forwards every other method of auth.Service unchanged.
//
// A nil index disables the guard (the service is returned as-is), which is what keeps the wrapper
// from denying anything before the dataset set is known.
func NewImmutabilityGuardService(authService auth.Service, index *DatasetRepoIndex, logger logging.Logger) auth.Service {
	if authService == nil || index == nil {
		return authService
	}
	if logger == nil {
		logger = logging.ContextUnavailable()
	}
	return &immutabilityGuardedAuthService{
		Service: authService,
		index:   index,
		logger:  logger,
	}
}

// immutabilityGuardedAuthService denies the §9.4 write actions on dataset repositories, after the
// underlying service has had no say: the guard only ever turns "allowed" into "denied", never the
// other way round.
type immutabilityGuardedAuthService struct {
	auth.Service
	index  *DatasetRepoIndex
	logger logging.Logger
}

// Authorize implements auth.Authorizer. A request that asks for a denied action on a dataset
// repository is answered with auth.ErrInsufficientPermissions - the same error a DENY statement
// produces - so the core API maps it to the configured authorization-failure status exactly as it
// maps a policy denial.
func (s *immutabilityGuardedAuthService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	if req == nil || !s.denied(ctx, req.RequiredPermissions) {
		return s.Service.Authorize(ctx, req)
	}
	// The audit trail of a policy denial names the denied action, which is what an operator
	// reading the lakeFS log expects to see for a refusal.
	denied := deniedActions(ctx, req.RequiredPermissions, s.index)
	s.logger.WithFields(logging.Fields{
		"user":    req.Username,
		"actions": denied,
	}).Debug("datasets: denied a write to an immutable dataset repository")
	return &auth.AuthorizationResponse{
		Allowed: false,
		Error: fmt.Errorf("%w: dataset repositories are immutable: %s is denied on a dataset repository",
			auth.ErrInsufficientPermissions, strings.Join(denied, ", ")),
	}, nil
}

// denied reports whether node asks for a denied action on a dataset repository.
//
// The node is evaluated with the deny semantics of the policy engine (pkg/auth/service.go
// CheckPermissions): a Deny statement that matches one required permission denies the whole
// request, and an And is denied as soon as one of its children is denied.
//
// An Or is denied when any of its branches is, exactly like the engine, which returns CheckDeny as
// soon as one branch is denied (pkg/auth/service.go, NodeTypeOr). Anything narrower would let a
// caller reach a dataset repository through a branch of an Or, which is precisely the hole the
// guard exists to close.
func (s *immutabilityGuardedAuthService) denied(ctx context.Context, node permissions.Node) bool {
	switch node.Type {
	case permissions.NodeTypeNode:
		return permissionDeniedOnDatasetRepo(ctx, s.index, node.Permission)
	case permissions.NodeTypeAnd, permissions.NodeTypeOr:
		for _, child := range node.Nodes {
			if s.denied(ctx, child) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// deniedActions lists the actions of node the guard refuses, for the log line and the error.
func deniedActions(ctx context.Context, node permissions.Node, index *DatasetRepoIndex) []string {
	seen := make(map[string]struct{})
	var out []string
	var walk func(permissions.Node)
	walk = func(n permissions.Node) {
		switch n.Type {
		case permissions.NodeTypeNode:
			if permissionDeniedOnDatasetRepo(ctx, index, n.Permission) {
				if _, ok := seen[n.Permission.Action]; !ok {
					seen[n.Permission.Action] = struct{}{}
					out = append(out, n.Permission.Action)
				}
			}
		default:
			for _, child := range n.Nodes {
				walk(child)
			}
		}
	}
	walk(node)
	return out
}

// permissionDeniedOnDatasetRepo reports whether permission is a §9.4 write action on a resource of
// a dataset repository.
func permissionDeniedOnDatasetRepo(ctx context.Context, index *DatasetRepoIndex, permission permissions.Permission) bool {
	if index == nil || !isImmutabilityDeniedAction(permission.Action) {
		return false
	}
	repo, ok := RepositoryFromResource(permission.Resource)
	if !ok {
		return false
	}
	return index.IsDatasetRepo(ctx, repo)
}

// isImmutabilityDeniedAction reports whether action is denied on a dataset repository. The list
// contains exact action names and action patterns (catalog:Create* and friends), and it is matched
// the way the policy engine matches a statement action against a requested action.
func isImmutabilityDeniedAction(action string) bool {
	if action == "" {
		return false
	}
	for _, pattern := range immutabilityDeniedActions {
		if wildcard.Match(pattern, action) {
			return true
		}
	}
	return false
}

// RepositoryFromResource returns the repository name a lakeFS fs or catalog resource ARN belongs
// to, and whether the resource names one at all.
//
//   - arn:lakefs:fs:::repository/<repo>[/object|/branch|/tag/...] -> <repo>
//   - arn:lakefs:catalog:::namespace/<repo>/<namespace>            -> <repo>
//   - arn:lakefs:catalog:::table/<repo>/<namespace>/<table>        -> <repo>
//   - arn:lakefs:catalog:::view/<repo>/<namespace>/<view>          -> <repo>
//
// A resource that is not one of those shapes - "*", an auth ARN, a wildcard ARN - reports false:
// the guard must never deny on a resource it cannot attribute to a single repository, and every
// authorization request the core API makes carries a concrete ARN.
func RepositoryFromResource(resource string) (string, bool) {
	if rest, ok := strings.CutPrefix(resource, fsRepositoryArnPrefix); ok {
		return pathSegment(rest)
	}
	if rest, ok := strings.CutPrefix(resource, catalogArnPrefix); ok {
		// Drop the object type: namespace, table or view.
		_, rest, ok = strings.Cut(rest, "/")
		if !ok {
			return "", false
		}
		return pathSegment(rest)
	}
	return "", false
}

// pathSegment returns the first non-empty path segment of path.
func pathSegment(path string) (string, bool) {
	segment, _, _ := strings.Cut(path, "/")
	if segment == "" || strings.ContainsAny(segment, "*?") {
		return "", false
	}
	return segment, true
}
