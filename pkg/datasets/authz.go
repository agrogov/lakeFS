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
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
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
}

// NewAuthorizer wraps an auth service (auth.Service implements auth.Authorizer) in an
// Authorizer. A nil service makes every check fail with ErrNoAuthorizer: the control plane must
// never treat "no auth service" as "allowed" (§4.6).
func NewAuthorizer(service auth.Authorizer) *Authorizer {
	return &Authorizer{service: service}
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
	for i, item := range items {
		if err := a.Check(ctx, caller, SourceReadNode(item), SourceItemName(i, item)); err != nil {
			return err
		}
	}
	return nil
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
//	fs:ReadCommit on arn:lakefs:fs:::repository/<src>
//	fs:ReadObject on the object ARN of the item
//	fs:ListObjects on the same object ARN
//
// where the object ARN is ObjectArn(src, <path>) for an "object" item and
// ObjectArn(src, <prefix>*) for a "prefix" item (a prefix of "" - the whole repository - becomes
// ObjectArn(src, "*")).
//
// "table" and "namespace" items are rejected by validation in the MVP (§1.2); they fall back to
// requiring the whole repository, which is stricter, so a future implementation that forgets to
// extend this function fails closed.
func SourceReadNode(item DataItem) permissions.Node {
	objectArn := SourceObjectArn(item)
	return permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadCommitAction,
					Resource: permissions.RepoArn(item.Repository),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadObjectAction,
					Resource: objectArn,
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ListObjectsAction,
					Resource: objectArn,
				},
			},
		},
	}
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
// change the dataset repository through the data plane or the management API: objects, commits,
// branches, tags, repository settings, storage attachment, GC rules, branch protection rules and
// CI actions.
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
	permissions.AttachStorageNamespaceAction,
	permissions.SetBranchProtectionRulesAction,
	permissions.SetGarbageCollectionRulesAction,
	ciWriteAction,
}

// ciWriteAction is "ci:WriteAction". pkg/permissions has a constant for ci:ReadAction but none
// for ci:WriteAction, and the datasets constants must not add entries to pkg/permissions
// (design §4.4).
const ciWriteAction = "ci:WriteAction"

// ImmutabilityPolicy builds the immutability deny policy of dataset name, byte-for-byte the
// statements of design §9.4: the deny action list on the dataset repository ARN and on
// arn:lakefs:fs:::repository/<name>/*.
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
// every dataset version pinned to that commit. The other two are the remaining ways a principal can
// defeat a pin through the API, and both are authorized against a *branch* ARN
// (pkg/api Controller.Commit and Controller.ResetBranch), so the pin resource pattern matches them:
// fs:CreateCommit moves the pin's HEAD off the pinned commit, and fs:RevertBranch does the same by
// pointing it at an ancestor. Deleting the whole source repository also deletes its pins, but that
// action is authorized against a repository ARN (which this branch pattern deliberately does not
// match); it is the "source repo deleted" risk of §8.2, handled by degraded detection instead.
var pinsProtectDeniedActions = []string{
	permissions.DeleteBranchAction,
	permissions.CreateCommitAction,
	permissions.RevertBranchAction,
}

// PinsProtectPolicy builds the datasets-pins-protect deny policy of §8.2: no principal may delete a
// hidden pin branch, in any repository, and pin branches are not writable.
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
