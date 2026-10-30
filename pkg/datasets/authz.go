// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// authz.go holds the authorization building blocks of the control plane, design §9.1-§9.4:
// the datasets:* action constants, the dataset / metadata-key ARNs, the Authorize helpers the
// service layer calls, and the per-dataset policies the service attaches (the consumer read
// policy of §9.2 and the immutability deny policy of §9.4). It contains no HTTP handler: the
// handlers of §10 map the typed errors below onto 403/500 (§10.4).
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS documentation
// only. No code, SQL, protobuf or UI asset from lakeFS Enterprise, and nothing from upstream
// lakeFS >= 1.87, was used.
package datasets

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// datasetsArnPrefix is the ARN prefix of the datasets service, §9.1. The service name is the
// plural "datasets", which is the serviceSet entry added to pkg/permissions/actions.go by the
// RBAC work.
const datasetsArnPrefix = "arn:lakefs:datasets:::"

// Control-plane actions, DATASETS_DESIGN.md §9.1.
//
// These constants deliberately live here rather than in pkg/permissions/actions.go: design
// §4.4 keeps the datasets actions out of actions.gen.go codegen, so nothing has to be
// regenerated. The "datasets" serviceSet entry already exists in pkg/permissions/actions.go
// (added by the RBAC work, with the same plural service name), so permissions.IsValidAction -
// and therefore model.ValidateActionName when a policy is written - accepts every constant
// below by service prefix alone. No codegen is needed for them, and none may be added: the
// codegen that produces actions.gen.go only reads pkg/permissions/actions.go.
const (
	// ActionListDatasets lists datasets. The results are filtered by ActionReadDataset (§9.1).
	ActionListDatasets = "datasets:ListDatasets"
	// ActionCreateDataset creates a dataset and its materialised repository (§9.1).
	ActionCreateDataset = "datasets:CreateDataset"
	// ActionReadDataset reads a dataset: definition, versions, diff and proposals (§9.1).
	ActionReadDataset = "datasets:ReadDataset"
	// ActionPublishVersion publishes a version, directly or by merging a proposal (§9.1).
	ActionPublishVersion = "datasets:PublishVersion"
	// ActionCreateProposal opens, updates or closes a dataset proposal (§9.1).
	ActionCreateProposal = "datasets:CreateProposal"
	// ActionDeleteDataset deletes a dataset, its repository and its grants (§9.1, §6.6).
	ActionDeleteDataset = "datasets:DeleteDataset"
	// ActionManageAccess grants and revokes consumer access to a dataset (§9.1, §9.2).
	ActionManageAccess = "datasets:ManageAccess"
	// ActionManageMetadataKeys manages the typed metadata keys shared by all datasets (§9.1, §5.5).
	ActionManageMetadataKeys = "datasets:ManageMetadataKeys"
)

// DatasetArn returns the ARN of the dataset name, §9.1:
// arn:lakefs:datasets:::dataset/<name>.
func DatasetArn(name string) string {
	return datasetsArnPrefix + "dataset/" + name
}

// MetadataKeyArn returns the ARN of the metadata key key, §9.1:
// arn:lakefs:datasets:::metadata-key/<key>.
func MetadataKeyArn(key string) string {
	return datasetsArnPrefix + "metadata-key/" + key
}

// ErrPermissionDenied reports that the authorization decision was "not allowed". Every
// *PermissionError matches it, so callers that only care about the 403 can write
// errors.Is(err, datasets.ErrPermissionDenied).
var ErrPermissionDenied = errors.New("permission denied")

// ErrNoAuthorizer is returned when an Authorizer is used without an auth service. It is a
// programming error, not a denial: the control plane must never fall through to "allowed".
var ErrNoAuthorizer = errors.New("datasets: no auth service configured for authorization")

// ErrNoAuthorizationDecision is returned when the auth service answers neither allowed nor
// denied. Like ErrNoAuthorizer it is a failure to decide, never a permission.
var ErrNoAuthorizationDecision = errors.New("datasets: auth service returned no authorization decision")

// PermissionError is a denial: the caller is not allowed to perform the required permissions
// on the resource. It implements the "403-style" errors of §9.3, naming the offending data item
// when the check was an attach-time source check.
type PermissionError struct {
	// Username is the principal the decision was made for.
	Username string
	// Item names the data item the check was about, as produced by SourceItemName. It is
	// empty for control-plane checks.
	Item string
	// Required is the permission set that was checked, flattened from the permission node.
	Required []permissions.Permission
	// Err is the underlying reason. It may be auth.ErrInsufficientPermissions, or a
	// "denied permission to fs:WriteObject" error when a deny statement matched (§9.4).
	Err error
}

// Error implements error. The message starts with "forbidden" so that a caller that only logs
// the error still sees the intended HTTP status, and names the data item that failed (§9.3).
func (e *PermissionError) Error() string {
	var b strings.Builder
	b.WriteString("forbidden: user ")
	b.WriteString(e.Username)
	b.WriteString(" ")
	switch {
	case e.Err != nil:
		b.WriteString(e.Err.Error())
	case len(e.Required) > 0:
		fmt.Fprintf(&b, "is not allowed to %s", joinActions(e.Required))
	default:
		b.WriteString("is not allowed")
	}
	if e.Item != "" {
		b.WriteString(" on item ")
		b.WriteString(e.Item)
	}
	return b.String()
}

// Unwrap reports both ErrPermissionDenied and the underlying auth error, so callers can match
// either one with errors.Is.
func (e *PermissionError) Unwrap() error {
	return errors.Join(ErrPermissionDenied, e.Err)
}

// HTTPStatusCode is the status the control plane answers with when it returns err (§10.4).
func (e *PermissionError) HTTPStatusCode() int {
	return http.StatusForbidden
}

// AuthorizationError reports that the authorization decision itself failed - the auth service
// returned an error, or answered something that is not a decision. It is a server-side failure,
// never a denial, and must not be reported as 403 (§10.4).
type AuthorizationError struct {
	// Username is the principal the decision was made for.
	Username string
	// Item names the data item the check was about, when any.
	Item string
	// Err is the failure.
	Err error
}

// Error implements error.
func (e *AuthorizationError) Error() string {
	msg := "datasets: authorization failed for user " + e.Username
	if e.Item != "" {
		msg += " on item " + e.Item
	}
	return msg + ": " + e.Err.Error()
}

// Unwrap exposes the underlying failure.
func (e *AuthorizationError) Unwrap() error { return e.Err }

// HTTPStatusCode is the status the control plane answers with when it returns err (§10.4).
func (e *AuthorizationError) HTTPStatusCode() int {
	return http.StatusInternalServerError
}

// Caller is the principal an authorization decision is made for, together with the
// request-scoped condition context the decision is evaluated in.
//
// ClientIP and Fields mirror the two condition inputs pkg/api Controller.conditionContext
// builds: the client IP (the SourceIp condition field) and, optionally, a lazy provider of
// further condition fields (lakefs:RepositoryMetadata/<key> in the core API). A nil Fields
// means "no further fields": a statement conditioned on one of them does not match, and so
// does not grant - the same behaviour as the core API without a provider.
type Caller struct {
	Username string
	ClientIP string
	Fields   func(key string) (string, bool)
}

// Authorizer answers the authorization questions of the datasets control plane by delegating
// to the auth service, exactly the way pkg/api Controller.authorize does (§9.1): one
// auth.AuthorizationRequest with a permissions.Node, so that Deny precedence, wildcards,
// conditions and ${user} interpolation all behave identically to the core API.
type Authorizer struct {
	service auth.Authorizer

	// policies is the same auth service seen as a policy source, when it has one. It is what
	// lets CheckSources see a Deny statement on a sub-path of an attached prefix, and what the
	// decision cache of SourceReadGuard is keyed on (SEC-3). It is nil for an auth service that
	// cannot list effective policies (auth.BasicAuthService answers auth.ErrNotImplemented), and
	// then the per-object check of SourceReadGuard remains the only sub-path check.
	policies effectivePolicyLister
}

// NewAuthorizer wraps an auth service (auth.Service implements auth.Authorizer) in an
// Authorizer. A nil service makes every check fail with ErrNoAuthorizer: the control plane must
// never treat "no auth service" as "allowed" (§4.6).
//
// When service can also list effective policies - both auth implementations that are actually
// deployed (acl.AuthService under lakefs-sso, APIAuthService against a policy server) can, and
// so do their generated MonitoredService wrappers - the Authorizer uses that for the sub-path
// analysis of CheckSources (SEC-3).
func NewAuthorizer(service auth.Authorizer) *Authorizer {
	authorizer := &Authorizer{service: service}
	if lister, ok := service.(effectivePolicyLister); ok {
		authorizer.policies = lister
	}
	return authorizer
}

// Check authorizes node for caller and returns nil when the decision is "allow".
//
// A denial is returned as *PermissionError (403) naming item; a failure to decide is returned
// as *AuthorizationError (500). Callers must treat any non-nil error as a refusal.
//
// The condition context is both put on ctx (auth.WithConditionContext) and passed on the
// request (ClientIP/Fields). The core API does the same through Controller.conditionContext
// plus its request fields, because the two auth service implementations read conditions from
// different places: acl.AuthService evaluates policies from the context, and
// auth.APIAuthService builds its condition context from the request. Setting both means an
// IpAddress or lakefs:RepositoryMetadata condition is honoured by either implementation, so a
// conditional deny is never silently skipped.
func (a *Authorizer) Check(ctx context.Context, caller Caller, node permissions.Node, item string) error {
	if a == nil || a.service == nil {
		return &AuthorizationError{Username: caller.Username, Item: item, Err: ErrNoAuthorizer}
	}

	conditionCtx := auth.WithConditionContext(ctx, auth.NewConditionContextWithProvider(caller.ClientIP, caller.Fields))
	resp, err := a.service.Authorize(conditionCtx, &auth.AuthorizationRequest{
		Username:            caller.Username,
		RequiredPermissions: node,
		ClientIP:            caller.ClientIP,
		Fields:              caller.Fields,
	})
	if err != nil {
		return &AuthorizationError{Username: caller.Username, Item: item, Err: err}
	}
	switch {
	case resp == nil:
		return &AuthorizationError{Username: caller.Username, Item: item, Err: ErrNoAuthorizationDecision}
	case resp.Error != nil && errors.Is(resp.Error, auth.ErrInsufficientPermissions):
		return &PermissionError{Username: caller.Username, Item: item, Required: requiredPermissions(node), Err: resp.Error}
	case resp.Error != nil:
		return &AuthorizationError{Username: caller.Username, Item: item, Err: resp.Error}
	case !resp.Allowed:
		return &PermissionError{Username: caller.Username, Item: item, Required: requiredPermissions(node), Err: auth.ErrInsufficientPermissions}
	default:
		return nil
	}
}

// CheckDataset authorizes action - one of the Action* constants of §9.1 - on dataset name.
func (a *Authorizer) CheckDataset(ctx context.Context, caller Caller, action, dataset string) error {
	return a.Check(ctx, caller, DatasetPermission(action, dataset), "")
}

// CheckMetadataKey authorizes action - ActionManageMetadataKeys or ActionListDatasets - on the
// metadata key key (§9.1).
func (a *Authorizer) CheckMetadataKey(ctx context.Context, caller Caller, action, key string) error {
	return a.Check(ctx, caller, MetadataKeyPermission(action, key), "")
}

// CheckSources runs the attach-time source check of §9.3 for every data item: the caller must be
// able to read the source commit and the source objects, item by item.
//
// The check is per item - not one big And node - so that a failure can name the offending item
// (§9.3: "Any failure -> 403 naming the item"). On the services that cache effective policies
// this costs one policy evaluation per item plus the per-principal cache hit; the definitions of
// §5.2 are bounded by datasets.max_items (default 1000).
//
// The caller must be the principal that is attaching the data:
//   - create, publish and proposal create/update: the caller of the request;
//   - proposal merge: the merger, since the merger publishes the data under their authority
//     (§9.3), in addition to the author check done at proposal creation.
func (a *Authorizer) CheckSources(ctx context.Context, caller Caller, items []DataItem) error {
	// The effective policies are read at most once per call, and only when a prefix item needs
	// them: an object item needs no analysis, because the ARN it authorises is literal.
	var (
		policies  []*model.Policy
		loaded    bool
		loadError error
	)
	for i, item := range items {
		name := SourceItemName(i, item)
		if err := a.Check(ctx, caller, SourceReadNode(item), name); err != nil {
			return err
		}
		if item.Type != ItemTypePrefix {
			continue
		}
		// A prefix item authorises one glob ARN, so a Deny on a sub-path of the prefix never
		// matches it (policy resources are the pattern side of the match). Check the caller's
		// Deny statements against the prefix separately, and reject the item before anything is
		// materialised. The per-object check of SourceReadGuard is the authoritative one; this
		// is the early answer that also covers callers whose materialisation never runs.
		if !loaded {
			policies, loadError = a.effectivePolicies(ctx, caller.Username)
			loaded = true
		}
		if loadError != nil {
			return &AuthorizationError{Username: caller.Username, Item: name, Err: loadError}
		}
		if err := a.checkPrefixDenies(caller, &item, name, policies); err != nil {
			return err
		}
	}
	return nil
}

// effectivePolicyLister is the part of auth.Service the source checks need: the policies that
// apply to a principal, directly, through groups, or through both. auth.Service implements it;
// the type assertion in NewAuthorizer picks it up when the service supports it.
type effectivePolicyLister interface {
	ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
}

// effectivePolicies returns the effective policies of username. A nil slice and a nil error mean
// "the auth service cannot tell" (no lister, or auth.ErrNotImplemented), which degrades to the
// per-object check of SourceReadGuard rather than to "allowed".
//
// It asks for the whole list in one page (Amount -1): that is the request the core API makes
// when it filters a listing by policy (pkg/api Controller.ListRepositories), and the ACL service
// answers it from its per-principal cache.
func (a *Authorizer) effectivePolicies(ctx context.Context, username string) ([]*model.Policy, error) {
	if a.policies == nil {
		return nil, nil
	}
	policies, _, err := a.policies.ListEffectivePolicies(ctx, username, &model.PaginationParams{Amount: -1})
	if errors.Is(err, auth.ErrNotImplemented) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return policies, nil
}

// checkPrefixDenies rejects a prefix item when the caller holds a Deny statement that could
// cover an object underneath the prefix (SEC-3).
//
// The match is deliberately conservative: it answers "could the two overlap", never "does the
// deny cover exactly this". A false positive refuses an item the per-object check might have
// allowed, which costs the caller a prefix rename; a false negative would copy data the caller
// is denied into the dataset. A deny on repository/other/... and a prefix in repository/src/ do
// not overlap, so the common "another repository is off limits" policy costs nothing.
func (a *Authorizer) checkPrefixDenies(caller Caller, item *DataItem, name string, policies []*model.Policy) error {
	path := itemSourcePath(*item)
	prefixArn := SourceObjectArn(*item)
	for _, policy := range policies {
		for _, statement := range policy.Statement {
			if statement.Effect != model.StatementEffectDeny || !statementCoversSourceRead(statement) {
				continue
			}
			resources, err := auth.ParsePolicyResourceAsList(statement.Resource)
			if err != nil {
				continue
			}
			for _, resource := range resources {
				if !sourcePrefixOverlapsDeny(resource, caller.Username, item.Repository, path) {
					continue
				}
				return &PermissionError{
					Username: caller.Username,
					Item:     name,
					Required: []permissions.Permission{SourceObjectReadPermission(item.Repository, path)},
					Err: fmt.Errorf("%w: deny statement on %s may cover objects under %s",
						auth.ErrInsufficientPermissions, resource, prefixArn),
				}
			}
		}
	}
	return nil
}

// statementCoversSourceRead reports whether a statement denies reading source objects: either
// fs:ReadObject (reading one object) or fs:ListObjects (finding it at all). A deny on either one
// makes part of the prefix unreadable, so both must be honoured (§9.3).
func statementCoversSourceRead(statement model.Statement) bool {
	for _, action := range statement.Action {
		if wildcard.Match(action, permissions.ReadObjectAction) || wildcard.Match(action, permissions.ListObjectsAction) {
			return true
		}
	}
	return false
}

// sourcePrefixOverlapsDeny reports whether the deny resource could match an object ARN under
// the item's prefix (SEC-3).
//
// The test is a plain string-prefix overlap between the deny's literal part - the resource up to
// its first wildcard - and the item's prefix ARN, taken without the trailing "/" so that
// "repository/src/object/sec" and "repository/src/object/secret/*" are seen as overlapping.
// That is deliberately conservative in both directions: a deny written for "secret/" is treated
// as possibly covering "sec/", and a deny deeper than the prefix counts as covered by it. The
// cost of a false positive is a refused attach that the per-object check of SourceReadGuard
// would have allowed; the cost of a false negative would be copying data the caller is denied.
func sourcePrefixOverlapsDeny(resource, username, repository, path string) bool {
	resource = interpolateUserPlaceholder(resource, username)
	if resource == permissions.All {
		return true
	}
	base := permissions.ObjectArn(repository, strings.TrimSuffix(path, "/"))
	literal := resource
	if index := strings.IndexAny(resource, "*?"); index >= 0 {
		literal = resource[:index]
	}
	return strings.HasPrefix(literal, base) || strings.HasPrefix(base, literal)
}

// interpolateUserPlaceholder substitutes the ${user} placeholder of a policy resource the way
// the auth service does before it matches, so a per-user resource is analysed for the right user.
func interpolateUserPlaceholder(resource, username string) string {
	return strings.ReplaceAll(resource, "${user}", username)
}

// DatasetPermission builds the leaf permission node of a datasets control-plane action on a
// dataset (§9.1).
func DatasetPermission(action, dataset string) permissions.Node {
	return permissions.Node{
		Permission: permissions.Permission{
			Action:   action,
			Resource: DatasetArn(dataset),
		},
	}
}

// MetadataKeyPermission builds the leaf permission node of a datasets control-plane action on a
// metadata key (§9.1).
func MetadataKeyPermission(action, key string) permissions.Node {
	return permissions.Node{
		Permission: permissions.Permission{
			Action:   action,
			Resource: MetadataKeyArn(key),
		},
	}
}

// SourceReadNode builds the §9.3 source-read requirement of one data item, as an And node:
//
//	fs:ListObjects on arn:lakefs:fs:::repository/<src>
//	fs:ReadObject on the object ARN of the item
//
// where the object ARN is ObjectArn(src, <path>) for an "object" item and
// ObjectArn(src, <prefix>*) for a "prefix" item (a prefix of "" - the whole repository - becomes
// ObjectArn(src, "*")).
//
// The two resources are the ones the core API authorises (DOC-15): listing objects is
// authorised on the repository ARN, pkg/api Controller.DiffBranch and Controller.ListObjects
// pass permissions.RepoArn(repository), while reading one object is authorised on its object
// ARN. A grant written from the public documentation - fs:ListObjects on
// repository/<src> plus fs:ReadObject on the source path - therefore passes this check.
//
// fs:ReadCommit is deliberately not required: the documentation does not ask for it, and it is
// not what a reader needs to read the data. Nothing is lost, because every object the item
// promises is read through fs:ReadObject, which the per-object check of SourceReadGuard applies
// to the exact object ARN.
//
// For a prefix item the object ARN carries a glob, and the policy side of ArnMatch is the
// pattern, so this node cannot see a Deny on a sub-path of the prefix; CheckSources and the
// per-object check of SourceReadGuard close that hole (SEC-3).
//
// "table" and "namespace" items are rejected by validation in the MVP (§1.2); they fall back to
// requiring the whole repository, which is stricter, so a future implementation that forgets to
// extend this function fails closed.
func SourceReadNode(item DataItem) permissions.Node {
	return permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ListObjectsAction,
					Resource: permissions.RepoArn(item.Repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadObjectAction,
					Resource: SourceObjectArn(item),
				},
			},
		},
	}
}

// SourceObjectReadPermission returns the fs:ReadObject permission on the exact source path: the
// permission the materialiser checks per object (SEC-3), and the object half of SourceReadNode.
func SourceObjectReadPermission(repository, path string) permissions.Permission {
	return permissions.Permission{
		Action:   permissions.ReadObjectAction,
		Resource: permissions.ObjectArn(repository, path),
	}
}

// SourceObjectReadNode wraps SourceObjectReadPermission in a one-leaf node.
func SourceObjectReadNode(repository, path string) permissions.Node {
	return permissions.Node{Permission: SourceObjectReadPermission(repository, path)}
}

// SourceObjectArn returns the object ARN a source item is read from (§9.3): the exact object for
// an "object" item, and the path glob "<prefix>*" for a "prefix" item.
func SourceObjectArn(item DataItem) string {
	if item.Type == ItemTypePrefix {
		return permissions.ObjectArn(item.Repository, itemSourcePath(item)+"*")
	}
	if item.Type == ItemTypeObject {
		return permissions.ObjectArn(item.Repository, itemSourcePath(item))
	}
	return permissions.ObjectArn(item.Repository, "*")
}

// itemSourcePath returns the source path of item, which is empty when the matching descriptor is
// missing (validation reports that separately).
func itemSourcePath(item DataItem) string {
	switch item.Type {
	case ItemTypeObject:
		if item.Object != nil {
			return item.Object.Path
		}
	case ItemTypePrefix:
		if item.Prefix != nil {
			return item.Prefix.Path
		}
	}
	return ""
}

// SourceItemName describes data item index as one string, for error messages and audit logs:
//
//	data[2] "customers" (prefix lakehouse-repo@30f9eb1c:data/customers/)
//
// The index is the position in the resolved definition, i.e. the canonical (sorted) order of
// §5.2, which is the order the definitions are stored and diffed in.
func SourceItemName(index int, item DataItem) string {
	name := fmt.Sprintf("data[%d]", index)
	if item.Target != "" {
		name += fmt.Sprintf(" %q", item.Target)
	}
	return fmt.Sprintf("%s (%s %s@%s:%s)", name, item.Type, item.Repository, item.Ref.ID, itemSourcePath(item))
}

// joinActions renders the actions of required, deduplicated, for error messages.
func joinActions(required []permissions.Permission) string {
	seen := make(map[string]struct{}, len(required))
	actions := make([]string, 0, len(required))
	for _, p := range required {
		if _, ok := seen[p.Action]; ok {
			continue
		}
		seen[p.Action] = struct{}{}
		actions = append(actions, p.Action)
	}
	return strings.Join(actions, ", ")
}

// requiredPermissions flattens a permission node into its leaf permissions, in node order.
func requiredPermissions(node permissions.Node) []permissions.Permission {
	switch node.Type {
	case permissions.NodeTypeNode:
		return []permissions.Permission{node.Permission}
	case permissions.NodeTypeOr, permissions.NodeTypeAnd:
		var out []permissions.Permission
		for _, child := range node.Nodes {
			out = append(out, requiredPermissions(child)...)
		}
		return out
	default:
		return nil
	}
}

// --- Per-object source check (SEC-3) ---

// SourceReadGuard authorizes the caller's fs:ReadObject right on the exact object ARN of every
// source path a materialisation reads (SEC-3), and remembers the decisions.
//
// The item-level check of CheckSources authorises one ARN per data item, and for a prefix item
// that ARN carries a glob (ObjectArn(repo, "<prefix>*")). Because ArnMatch treats the policy
// resource as the pattern side of the match, a Deny on a sub-path - say
// arn:lakefs:fs:::repository/hr/object/salaries/* - never matches that literal glob, so the
// deny would go unseen and the materialiser would copy the denied objects into the dataset.
// The guard asks the auth service about the exact object ARN of every path the iterator yields,
// which is the same question the gateway asks when a client reads the source object directly.
//
// A decision is cached per (policy set, path prefix):
//
//   - The policy set is the guard's own lifetime. A guard serves exactly one publish, which is
//     a short single-goroutine operation, so the policies a decision was made under cannot
//     change underneath it in a way one publish could observe.
//   - The path prefix is the deepest prefix of the object ARN that the caller's policies cannot
//     distinguish. A policy resource that is a literal prefix ending in "/" and followed by
//     nothing but "*" matches a path exactly when the path starts with that literal, so two
//     paths with the same deepest such prefix match exactly the same statements and share one
//     decision. Anything else - a literal ARN, a wildcard inside a path segment, a "?", or
//     literal text after a wildcard - can distinguish two paths inside one directory, so the
//     guard stops bucketing for the whole policy set and keys decisions on the full ARN.
//     Reusing a decision is therefore never coarser than the policies: every path in a bucket
//     matches the same set of statements, and a statement's conditions are evaluated against
//     the same caller for all of them.
//
// Without an auth service that can list effective policies the guard still checks every object;
// it just cannot reuse a decision, which is slower, not less strict.
type SourceReadGuard struct {
	authorizer *Authorizer
	caller     Caller

	once    sync.Once
	buckets sourceReadBuckets

	mu        sync.Mutex
	decisions map[string]error
}

// NewSourceReadGuard returns a guard that authorizes source reads for caller. One guard serves
// one publish (or one source walk), so the decisions it caches live exactly as long as the
// policy set they were made under.
func NewSourceReadGuard(authorizer *Authorizer, caller Caller) *SourceReadGuard {
	return &SourceReadGuard{
		authorizer: authorizer,
		caller:     caller,
		decisions:  make(map[string]error),
	}
}

// CheckObject authorizes fs:ReadObject on ObjectArn(item.Repository, path) for the caller. A
// denial is returned as a *SourceError that wraps the *PermissionError, so the item index, the
// source repository and ref, and the denied path all survive to the API error (§10.4) while
// errors.Is(err, ErrPermissionDenied) keeps working.
func (g *SourceReadGuard) CheckObject(ctx context.Context, index int, item *DataItem, path string) error {
	if g == nil {
		return nil
	}
	resource := permissions.ObjectArn(item.Repository, path)
	key := g.cacheKey(ctx, resource)

	g.mu.Lock()
	decided, ok := g.decisions[key]
	g.mu.Unlock()
	if ok {
		return decided
	}

	err := g.authorizer.Check(ctx, g.caller, SourceObjectReadNode(item.Repository, path),
		deniedSourceName(index, item, path))
	var permission *PermissionError
	if errors.As(err, &permission) {
		err = &SourceError{
			Item:       index,
			Repository: item.Repository,
			Ref:        item.Ref,
			Path:       path,
			Err:        err,
		}
	}

	g.mu.Lock()
	g.decisions[key] = err
	g.mu.Unlock()
	return err
}

// cacheKey is the decision-cache key of the object ARN: the deepest bucket prefix the ARN
// belongs to, or the ARN itself when the caller's policies can distinguish paths inside one
// directory (see the type comment).
func (g *SourceReadGuard) cacheKey(ctx context.Context, resource string) string {
	g.once.Do(func() { g.buckets = g.loadBuckets(ctx) })
	return g.buckets.key(resource)
}

// loadBuckets derives the bucket prefixes from the caller's effective policies. It never fails:
// a policy set it cannot read degrades to per-ARN keys, never to a coarser bucket.
func (g *SourceReadGuard) loadBuckets(ctx context.Context) sourceReadBuckets {
	policies, err := g.authorizer.effectivePolicies(ctx, g.caller.Username)
	if err != nil {
		return sourceReadBuckets{}
	}
	var buckets sourceReadBuckets
	for _, policy := range policies {
		for _, statement := range policy.Statement {
			if !statementCoversReadObject(statement) {
				continue
			}
			resources, err := auth.ParsePolicyResourceAsList(statement.Resource)
			if err != nil {
				buckets.exact = true
				continue
			}
			for _, resource := range resources {
				prefix, ok := readBucketPrefix(interpolateUserPlaceholder(resource, g.caller.Username))
				if !ok {
					buckets.exact = true
					continue
				}
				if prefix != "" {
					buckets.prefixes = append(buckets.prefixes, prefix)
				}
			}
		}
	}
	return buckets
}

// statementCoversReadObject reports whether a statement can decide fs:ReadObject at all. Both
// effects matter: an Allow and a Deny are equally able to make two paths in one directory
// disagree.
func statementCoversReadObject(statement model.Statement) bool {
	for _, action := range statement.Action {
		if wildcard.Match(action, permissions.ReadObjectAction) {
			return true
		}
	}
	return false
}

// readBucketPrefix returns the bucket prefix of a policy resource: the literal text before its
// first wildcard, when the resource is a directory prefix followed by nothing but "*". The
// second return value is false when the resource can distinguish two paths inside one directory
// (a literal ARN, a wildcard inside a segment, a "?", or literal text after a wildcard), which
// disables bucketing for the whole policy set.
func readBucketPrefix(resource string) (prefix string, ok bool) {
	index := strings.IndexAny(resource, "*?")
	if index < 0 {
		return "", false
	}
	if index == 0 {
		// "*" (or "?*"): matches every resource, which constrains nothing.
		return "", true
	}
	literal := resource[:index]
	if !strings.HasSuffix(literal, "/") {
		return "", false
	}
	if strings.Trim(resource[index:], "*") != "" {
		return "", false
	}
	return literal, true
}

// sourceReadBuckets is the bucketing readBucketPrefix derived from the caller's policies.
type sourceReadBuckets struct {
	// prefixes are the directory prefixes a decision can be shared under.
	prefixes []string

	// exact reports that some statement can distinguish two paths inside one directory, so a
	// decision may only be reused for the exact ARN.
	exact bool
}

// key returns the decision-cache key of resource: its deepest bucket prefix, the resource
// itself when no prefix applies, and always the resource when bucketing is off.
func (b sourceReadBuckets) key(resource string) string {
	if b.exact {
		return resource
	}
	best := ""
	for _, prefix := range b.prefixes {
		if len(prefix) > len(best) && strings.HasPrefix(resource, prefix) {
			best = prefix
		}
	}
	if best == "" {
		return resource
	}
	return best
}

// deniedSourceName is the item description a per-object denial carries: the item of §9.3 plus
// the exact source path that was denied, so the message names both what was attached and which
// object inside it was refused.
func deniedSourceName(index int, item *DataItem, path string) string {
	return fmt.Sprintf("%s object %q", SourceItemName(index, *item), path)
}

// --- Per-dataset policies (§9.2, §9.4) ---

// datasetReadActions is the repository-level read action set of the consumer read policy (§9.2).
// The managed datasets-all-read policy of §9.5 uses the same set per dataset.
var datasetReadActions = []string{
	permissions.ReadRepositoryAction,
	permissions.ListObjectsAction,
	permissions.ReadObjectAction,
	permissions.ReadCommitAction,
	permissions.ListCommitsAction,
	permissions.ReadBranchAction,
	permissions.ListBranchesAction,
	permissions.ReadTagAction,
	permissions.ListTagsAction,
}

// datasetObjectReadActions is the object-level read action set of §9.2.
var datasetObjectReadActions = []string{
	permissions.ReadObjectAction,
	permissions.ListObjectsAction,
}

// datasetGlobalListActions is the "list what you may read" statement of §9.2: without it a
// consumer cannot find the dataset in GET /repositories or GET /datasets.
var datasetGlobalListActions = []string{
	permissions.ListRepositoriesAction,
	ActionListDatasets,
}

// ConsumerReadPolicyName is the display name of the managed read policy of dataset name (§9.2):
// dataset-<name>-read.
func ConsumerReadPolicyName(dataset string) string {
	return "dataset-" + dataset + "-read"
}

// ConsumerReadPolicy builds the consumer read policy of dataset name, byte-for-byte the
// statements of design §9.2: the dataset is materialised as the read-only repository <name>, so
// reading a dataset is reading that repository.
//
// The policy is returned as a model.Policy so that the service can write it
// (auth.Service.WritePolicy) and attach it to a user or a group
// (AttachPolicyToUser / AttachPolicyToGroup) when it records the grant.
func ConsumerReadPolicy(dataset string, ts time.Time) *model.Policy {
	repoArn := permissions.RepoArn(dataset)
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: ConsumerReadPolicyName(dataset),
		Statement: model.Statements{
			{
				Effect:   model.StatementEffectAllow,
				Action:   append([]string(nil), datasetReadActions...),
				Resource: repoArn,
			},
			{
				Effect:   model.StatementEffectAllow,
				Action:   append([]string(nil), datasetObjectReadActions...),
				Resource: permissions.ObjectArn(dataset, "*"),
			},
			{
				Effect:   model.StatementEffectAllow,
				Action:   []string{ActionReadDataset},
				Resource: DatasetArn(dataset),
			},
			{
				Effect:   model.StatementEffectAllow,
				Action:   append([]string(nil), datasetGlobalListActions...),
				Resource: permissions.All,
			},
		},
	}
}

// ImmutabilityPolicyName is the display name of the immutability deny policy of dataset name
// (§9.4): dataset-<name>-immutable.
func ImmutabilityPolicyName(dataset string) string {
	return "dataset-" + dataset + "-immutable"
}

// immutabilityDeniedActions is the deny action list of §9.4. It covers every way a writer could
// change the dataset repository through the data plane, the management API or the Iceberg catalog
// API: objects, commits, branches, tags, imports, pull requests, repository settings, storage
// attachment, GC rules, branch protection rules, catalog objects and CI actions.
//
// The list is also read by the authorizer guard of immutability_guard.go, so an action that is
// missing here is a way around the immutability guarantee for callers the per-group attachment
// does not cover (SEC-2 of DATASETS_REVIEW.md).
var immutabilityDeniedActions = []string{
	permissions.WriteObjectAction,
	permissions.DeleteObjectAction,
	permissions.CreateCommitAction,
	permissions.CreateBranchAction,
	permissions.DeleteBranchAction,
	permissions.RevertBranchAction,
	permissions.CreateTagAction,
	permissions.DeleteTagAction,
	permissions.UpdateRepositoryAction,
	permissions.DeleteRepositoryAction,
	permissions.ImportFromStorageAction,
	permissions.ImportCancelAction,
	permissions.AttachStorageNamespaceAction,
	permissions.SetBranchProtectionRulesAction,
	permissions.SetGarbageCollectionRulesAction,
	permissions.WritePullRequestAction,
	catalogCreateActionPattern,
	catalogUpdateActionPattern,
	catalogDeleteActionPattern,
	ciWriteAction,
}

// ciWriteAction is "ci:WriteAction". pkg/permissions has a constant for ci:ReadAction but none
// for ci:WriteAction, and the datasets constants must not add entries to pkg/permissions
// (design §4.4).
const ciWriteAction = "ci:WriteAction"

// Iceberg catalog mutation actions (ICEBERG_CATALOG_PLAN.md §7.2): creating, updating or deleting
// a namespace, table or view rewrites the catalog state of the dataset repository, so it must be
// denied on a dataset repository like any other write.
//
// They are patterns rather than the concrete permissions.Catalog* constants on purpose: a catalog
// mutation action added later is denied by default instead of silently bypassing the guard.
// pkg/permissions.IsValidAction validates the service prefix of an action pattern, not the whole
// name, so a pattern is a valid policy action and the deny list matches exactly the way the policy
// engine matches actions (wildcard.Match, pkg/auth/service.go CheckPermissions).
const (
	catalogCreateActionPattern = "catalog:Create*"
	catalogUpdateActionPattern = "catalog:Update*"
	catalogDeleteActionPattern = "catalog:Delete*"
)

// ImmutabilityPolicy builds the immutability deny policy of dataset name, byte-for-byte the
// statements of design §9.4: the deny action list on the dataset repository ARN and on
// arn:lakefs:fs:::repository/<name>/*.
//
// It is the defence in depth of the §9.4 guarantee: the guarantee itself is enforced in the
// authorizer (immutability_guard.go), because a policy can only deny the principals it is attached
// to, and write power under `internal` RBAC also comes from custom groups, SSO-synced groups and
// policies attached directly to a user (SEC-2 of DATASETS_REVIEW.md).
//
// It uses two exact resources - repository/<name> and repository/<name>/* - and NOT
// repository/<name>*, which would also deny access to an unrelated repository named
// <name>-something (§9.4). The policy is a deny, so it wins over every allow, including the
// FSFullAccess allow of an admin; only the service itself is unaffected, because it calls the
// catalog directly instead of going through Authorize (§4.6).
func ImmutabilityPolicy(dataset string, ts time.Time) *model.Policy {
	repoArn := permissions.RepoArn(dataset)
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: ImmutabilityPolicyName(dataset),
		Statement: model.Statements{
			{
				Effect:   model.StatementEffectDeny,
				Action:   append([]string(nil), immutabilityDeniedActions...),
				Resource: repoArn,
			},
			{
				Effect:   model.StatementEffectDeny,
				Action:   append([]string(nil), immutabilityDeniedActions...),
				Resource: repoArn + "/*",
			},
		},
	}
}

// DatasetsPinsProtectPolicyName is the display name of the installation-wide deny policy that keeps
// ordinary principals from deleting GC pin branches in source repositories (§8.2):
// "datasets-pins-protect".
//
// It is a deny, so it wins over the fs:* allow of an administrator: only the service itself can
// delete a pin branch, and it does that through the catalog directly instead of through Authorize
// (§4.6). Without it, a source-repository admin deleting a pin branch would silently disarm the
// protection of every dataset version pinned to that commit, and the next GC run in the source
// repository would delete objects datasets still serve.
const DatasetsPinsProtectPolicyName = "datasets-pins-protect"

// PinBranchResourcePattern is the resource of the datasets-pins-protect policy (§8.2):
// arn:lakefs:fs:::repository/*/branch/_lakefs_ds_pin_*. Spike S2b (spikes_pins_test.go) proves that
// auth.ArnMatch wildcard-matches the middle of an ARN resource, that it spans "/", that it selects
// pin branches in every repository, and that it leaves ordinary branches alone.
var PinBranchResourcePattern = "arn:lakefs:fs:::repository/*/branch/" + PinBranchPrefix + "*"

// pinsProtectDeniedActions is the deny action list of the datasets-pins-protect policy (§8.2).
// fs:DeleteBranch is the action the design names: it removes the pin and disarms the protection of
// every dataset version pinned to that commit. The other three are the remaining ways a principal
// can defeat a pin through the API. All of them are authorized against a *branch* ARN
// (pkg/api Controller.DeleteBranch, Controller.CreateBranch, Controller.Commit and
// Controller.ResetBranch), so the pin resource pattern matches them: fs:CreateCommit moves the
// pin's HEAD off the pinned commit, fs:RevertBranch does the same by pointing it at an ancestor,
// and fs:CreateBranch lets a principal *squat* on the name before a publish pins the commit - the
// pre-created branch then fails pins.verifyPinBranch with ErrPinBranchConflict, which makes every
// publish of that commit fail. Deleting the whole source repository also deletes its pins, but that
// action is authorized against a repository ARN (which this branch pattern deliberately does not
// match); it is the "source repo deleted" risk of §8.2, handled by degraded detection instead.
var pinsProtectDeniedActions = []string{
	permissions.DeleteBranchAction,
	permissions.CreateBranchAction,
	permissions.CreateCommitAction,
	permissions.RevertBranchAction,
}

// PinsProtectPolicy builds the datasets-pins-protect deny policy of §8.2: no principal may delete a
// hidden pin branch, in any repository, no principal may create one (a squatted name makes every
// publish of the commit it impersonates fail), and pin branches are not writable.
//
// It is installation-wide (no dataset name in it) because pins live in the source repositories,
// whose names have nothing to do with the datasets that pinned them. Bootstrap attaches it to the
// base groups plus the extra groups the installation lists in datasets.immutable_guard_groups; the
// service is not a group member, and it bypasses Authorize anyway (§4.6).
func PinsProtectPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsPinsProtectPolicyName,
		Statement: model.Statements{{
			Effect:   model.StatementEffectDeny,
			Action:   append([]string(nil), pinsProtectDeniedActions...),
			Resource: PinBranchResourcePattern,
		}},
	}
}

// PinsProtectGroups returns the groups the datasets-pins-protect policy is attached to (§8.2): the
// same non-service principals the immutability guard covers - the base groups of both group
// templates plus extra - deduplicated and in a stable order.
func PinsProtectGroups(extra []string) []string {
	return ImmutabilityGuardGroups(extra)
}

// immutabilityGuardBaseGroups are the groups every installation guards (§9.4): the base groups of
// both lakeFS group templates, so the guard covers members of either one. The simplified ACL
// template ships Admins/Supers/Writers/Readers and the RBAC template ships
// Admins/SuperUsers/Developers/Viewers (pkg/auth/setup). Unknown names are harmless: attaching a
// policy to a group that does not exist fails with auth.ErrNotFound and the service skips it.
var immutabilityGuardBaseGroups = []string{
	"Admins",
	"Supers",
	"Writers",
	"Readers",
	"SuperUsers",
	"Developers",
	"Viewers",
}

// ImmutabilityGuardGroups returns the groups the immutability deny policy of a dataset is
// attached to (§9.4): the base groups of both templates plus extra (the configured
// datasets.immutable_guard_groups), deduplicated, in a stable order. Empty names are dropped.
func ImmutabilityGuardGroups(extra []string) []string {
	seen := make(map[string]struct{}, len(immutabilityGuardBaseGroups)+len(extra))
	groups := make([]string, 0, len(immutabilityGuardBaseGroups)+len(extra))
	for _, group := range append(append([]string(nil), immutabilityGuardBaseGroups...), extra...) {
		if group == "" {
			continue
		}
		if _, ok := seen[group]; ok {
			continue
		}
		seen[group] = struct{}{}
		groups = append(groups, group)
	}
	return groups
}
