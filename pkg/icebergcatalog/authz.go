package icebergcatalog

// Authorization for the Iceberg REST catalog: the matrix of ICEBERG_CATALOG_PLAN.md §7.2, the
// "authz matrix" unit test of §12 and decision Q-I4 of §14.
//
// Every catalog request is authorized on the *catalog* ARNs lakeFS already defines
// (pkg/permissions/permission.go, CatalogNamespaceArn / CatalogTableArn / CatalogViewArn):
//
//	arn:lakefs:catalog:::namespace/<repo>/<namespace>
//	arn:lakefs:catalog:::table/<repo>/<namespace>/<table>
//	arn:lakefs:catalog:::view/<repo>/<namespace>/<view>
//
// A view has its own ARN and its own actions (catalog:ListViews/ReadView/CreateView/UpdateView/
// DeleteView, pkg/permissions/actions.go), so a policy can grant tables without views and the
// reverse; both ARN families are branch-agnostic in the same way.
//
// Those ARNs are branch-agnostic by design (§7.2): the catalog state of every branch shares the
// same namespace, table and view names, so a branch-scoped ARN would either have to be repeated for
// every branch of a repository or would silently make a table unaddressable after a rename. The
// consequence is deliberate and is what makes the branch, tag and commit cases work: a policy
// grant or deny on a catalog ARN applies to the table on every ref, and the *ref* rules are
// enforced separately - reads are allowed on any ref, writes only on a branch (the Q-I4 rule of
// Authorize, refused as *ReadOnlyRefError).
//
// On top of the catalog action, every request needs fs:ReadRepository on the repository, and
// every write additionally needs fs:WriteObject on ObjectArn(repo, "_lakefs_iceberg/*"). The
// second one is what makes the existing lakeFS denies apply to catalog writes without any new
// policy:
//
//   - a datasets repository is guarded by the dataset-<name>-immutable deny policy
//     (pkg/datasets/authz.go, ImmutabilityPolicy), which denies fs:WriteObject - among the
//     catalog:Create*/Update*/Delete* actions it already lists - on the repository ARN and on
//     repository/<name>/*. Denying fs:WriteObject on repository/<name>/* matches the requested
//     repository/<name>/object/_lakefs_iceberg/* and so denies the whole And node;
//   - a deny of fs:* on a repository ARN blocks the catalog for that repository;
//   - the reserved _lakefs_iceberg/ prefix cannot be written through the fs: API by a principal
//     that lacks it, because it is the very permission the catalog requires.
//
// This file only builds and evaluates permission nodes; it performs no I/O, resolves no ref and
// never reads a repository. The HTTP layer (http.go, errors.go) maps the typed errors below onto
// the Iceberg error envelope: a *PermissionDeniedError and a *ReadOnlyRefError are 403
// ForbiddenException, an *AuthorizationError is a 500 - a failure to decide is never reported as
// a permission.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) and the
// public lakeFS documentation only. No code from lakeFS Enterprise or from upstream lakeFS >= 1.87
// was used.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// ConditionFieldTableName is the policy condition field the catalog supplies for
	// table-scoped decisions, §7.2: a policy may carry
	//
	//	"Condition": {"StringLike": {"catalog:TableName": ["analytics-*"]}}
	//
	// and the catalog evaluates it with the table's own name. The same field is the per-table
	// filter of ListTables (FilterTables): a table is visible exactly when catalog:ListTables
	// is allowed on the namespace ARN with catalog:TableName set to that table.
	ConditionFieldTableName = "catalog:TableName"

	// ConditionFieldViewName is ConditionFieldTableName for views: the condition field of a
	// view-scoped decision, and the per-view filter of ListViews (FilterViews). A policy that
	// grants catalog:ListViews only for "analytics-*" views is written as
	//
	//	"Condition": {"StringLike": {"catalog:ViewName": ["analytics-*"]}}
	//
	// It is a separate field from catalog:TableName on purpose: a view is a different object
	// with its own ARNs and actions, so a policy that scopes tables must not silently scope
	// views as well.
	ConditionFieldViewName = "catalog:ViewName"

	// ErrorTypeForbidden is the Iceberg error type of every refusal in this file. The REST
	// OpenAPI spec answers 403 with {"error": {"type": "ForbiddenException", ...}}
	// (components.schemas.ErrorResponse / responses.ForbiddenResponse).
	ErrorTypeForbidden = "ForbiddenException"

	// ErrorTypeInternal is the Iceberg error type of a failure to decide. It is never used for
	// a refusal.
	ErrorTypeInternal = "InternalServerError"

	// icebergWriteObjectKey is the object key the catalog's write permission is granted on,
	// §7.2: every catalog write reaches the reserved _lakefs_iceberg/ area of the branch, so the
	// write permission of the catalog is fs:WriteObject on ObjectArn(repo, "_lakefs_iceberg/*").
	// Unqualified, the glob also covers the reserved namespace markers, the table pointers and
	// the staged table files.
	icebergWriteObjectKey = "_lakefs_iceberg/*"
)

// Refusal sentinels. They exist so a caller can classify an error with errors.Is without
// matching on type: ErrPermissionDenied is "the caller may not do this", ErrRefReadOnly is Q-I4's
// "writes need a branch", and ErrInvalidAuthzRequest is a programming error in the HTTP layer
// (an operation that does not match its address), never a property of the request's owner.
var (
	// ErrPermissionDenied is wrapped by *PermissionDeniedError.
	ErrPermissionDenied = errors.New("permission denied")

	// ErrRefReadOnly is wrapped by *ReadOnlyRefError: the write addresses a ref that is not a
	// branch (Q-I4). Its message is the message of the Iceberg error, "ref is read-only".
	ErrRefReadOnly = errors.New("ref is read-only")

	// ErrNoAuthorizer is returned when an Authorizer is used without an auth service. It is a
	// programming error: the catalog must never fall through to "allowed".
	ErrNoAuthorizer = errors.New("icebergcatalog: no auth service configured for authorization")

	// ErrNoAuthorizationDecision is returned when the auth service answers neither allowed nor
	// denied. Like ErrNoAuthorizer it is a failure to decide, never a permission.
	ErrNoAuthorizationDecision = errors.New("icebergcatalog: auth service returned no authorization decision")

	// ErrInvalidAuthzRequest is wrapped by every error that reports a mismatch between an
	// Operation and its Address.
	ErrInvalidAuthzRequest = errors.New("icebergcatalog: invalid authorization request")
)

// Caller is the principal an authorization decision is made for, together with the request-scoped
// condition context the decision is evaluated in.
//
// ClientIP and Fields mirror the two condition inputs pkg/api Controller.conditionContext builds:
// the client IP (the SourceIp condition field) and, optionally, a lazy provider of further
// condition fields (lakefs:RepositoryMetadata/<key> in the core API). A nil Fields means "no
// further fields": a statement conditioned on one of them does not match, and so does not grant -
// the same behaviour as the core API without a provider. The catalog adds
// ConditionFieldTableName itself, so a caller never has to supply it.
type Caller struct {
	Username string
	ClientIP string
	Fields   func(key string) (string, bool)
}

// Operation is one request of the catalog REST API, named after the Iceberg endpoint that
// implements it. The zero value is not an operation: Action reports it as invalid.
type Operation string

// The operations of the §7.2 matrix. Namespaces and tables are addressed through the Address of
// this package (ident.go), which carries the repository, the ref, the namespace and - for a
// table - the table name.
const (
	// OpListNamespaces lists the levels below a parent: repositories when the address has no
	// repository, branches when it has no ref, namespaces below that (§5). Action:
	// catalog:ListNamespaces on the parent's namespace ARN.
	OpListNamespaces Operation = "ListNamespaces"

	// OpGetNamespace reads one namespace (GET and HEAD). Action: catalog:GetNamespace.
	OpGetNamespace Operation = "GetNamespace"

	// OpCreateNamespace creates a namespace. Action: catalog:CreateNamespace.
	OpCreateNamespace Operation = "CreateNamespace"

	// OpUpdateNamespace updates the properties of a namespace. Action:
	// catalog:UpdateNamespace.
	OpUpdateNamespace Operation = "UpdateNamespace"

	// OpDeleteNamespace deletes an empty namespace. Action: catalog:DeleteNamespace.
	OpDeleteNamespace Operation = "DeleteNamespace"

	// OpListTables lists the tables of a namespace. Action: catalog:ListTables on the
	// namespace ARN, filtered per table by ConditionFieldTableName (FilterTables).
	OpListTables Operation = "ListTables"

	// OpListViews lists the views of a namespace. Action: catalog:ListViews on the namespace
	// ARN, filtered per view by ConditionFieldViewName (FilterViews).
	OpListViews Operation = "ListViews"

	// OpReadTable loads a table: its pointer and its metadata (GET and HEAD). Action:
	// catalog:ReadTable on the table ARN.
	OpReadTable Operation = "ReadTable"

	// OpCreateTable creates a table, including the staged create of §6.2. Action:
	// catalog:CreateTable.
	OpCreateTable Operation = "CreateTable"

	// OpUpdateTable changes a table: commit, register, properties, metrics. Action:
	// catalog:UpdateTable.
	OpUpdateTable Operation = "UpdateTable"

	// OpDeleteTable drops a table. Action: catalog:DeleteTable.
	OpDeleteTable Operation = "DeleteTable"

	// OpReadTableData reads the table's data files: vended read credentials and remote signing
	// (§8, phase I-4). Action: catalog:ReadTableData. It is deliberately separate from
	// OpReadTable: the metadata of a table may be readable while its data is not.
	OpReadTableData Operation = "ReadTableData"

	// OpWriteTableData writes the table's data files: vended write credentials and remote
	// signing (§8, phase I-4). Action: catalog:WriteTableData.
	OpWriteTableData Operation = "WriteTableData"

	// OpReadView loads a view: its pointer and its metadata (GET and HEAD). Action:
	// catalog:ReadView on the view ARN.
	OpReadView Operation = "ReadView"

	// OpCreateView creates a view. Action: catalog:CreateView.
	OpCreateView Operation = "CreateView"

	// OpUpdateView replaces a view: the commit of an Iceberg view (requirements and updates).
	// Action: catalog:UpdateView.
	OpUpdateView Operation = "UpdateView"

	// OpDeleteView drops a view. Action: catalog:DeleteView.
	OpDeleteView Operation = "DeleteView"
)

// catalogActions is the action column of the §7.2 matrix. An operation missing here is invalid.
var catalogActions = map[Operation]string{
	OpListNamespaces:  permissions.CatalogListNamespacesAction,
	OpGetNamespace:    permissions.CatalogGetNamespaceAction,
	OpCreateNamespace: permissions.CatalogCreateNamespaceAction,
	OpUpdateNamespace: permissions.CatalogUpdateNamespaceAction,
	OpDeleteNamespace: permissions.CatalogDeleteNamespaceAction,
	OpListTables:      permissions.CatalogListTablesAction,
	OpReadTable:       permissions.CatalogReadTableAction,
	OpCreateTable:     permissions.CatalogCreateTableAction,
	OpUpdateTable:     permissions.CatalogUpdateTableAction,
	OpDeleteTable:     permissions.CatalogDeleteTableAction,
	OpReadTableData:   permissions.CatalogReadTableDataAction,
	OpWriteTableData:  permissions.CatalogWriteTableDataAction,
	OpListViews:       permissions.CatalogListViewsAction,
	OpReadView:        permissions.CatalogReadViewAction,
	OpCreateView:      permissions.CatalogCreateViewAction,
	OpUpdateView:      permissions.CatalogUpdateViewAction,
	OpDeleteView:      permissions.CatalogDeleteViewAction,
}

// tableOperations are the operations authorized on a table ARN. Everything else in
// catalogActions is authorized on a namespace ARN (§7.2).
var tableOperations = map[Operation]struct{}{
	OpReadTable:      {},
	OpCreateTable:    {},
	OpUpdateTable:    {},
	OpDeleteTable:    {},
	OpReadTableData:  {},
	OpWriteTableData: {},
}

// viewOperations are the operations authorized on a view ARN. A view has its own ARN
// (permissions.CatalogViewArn, arn:lakefs:catalog:::view/<repo>/<ns>/<view>), so a policy can grant
// the table actions without the view actions and the reverse - which is the shape the lakeFS
// permission catalogue already publishes (catalog:ListViews, catalog:ReadView, catalog:CreateView,
// catalog:UpdateView, catalog:DeleteView).
var viewOperations = map[Operation]struct{}{
	OpReadView:   {},
	OpCreateView: {},
	OpUpdateView: {},
	OpDeleteView: {},
}

// writeOperations are the operations that change the repository, and therefore need both a
// branch (Q-I4) and fs:WriteObject on the reserved _lakefs_iceberg/ area (§7.2). Reads of a tag
// or a commit are not in the set: §5 allows them on any ref.
var writeOperations = map[Operation]struct{}{
	OpCreateNamespace: {},
	OpUpdateNamespace: {},
	OpDeleteNamespace: {},
	OpCreateTable:     {},
	OpUpdateTable:     {},
	OpDeleteTable:     {},
	OpWriteTableData:  {},
	OpCreateView:      {},
	OpUpdateView:      {},
	OpDeleteView:      {},
}

// Action returns the catalog:* action of op (§7.2). It reports an error for an operation that is
// not part of the matrix, so an unwired endpoint fails closed.
func (op Operation) Action() (string, error) {
	action, ok := catalogActions[op]
	if !ok {
		return "", fmt.Errorf("%w: unknown operation %q", ErrInvalidAuthzRequest, string(op))
	}
	return action, nil
}

// IsTableOperation reports whether op is authorized on a table ARN rather than a namespace ARN.
func (op Operation) IsTableOperation() bool {
	_, ok := tableOperations[op]
	return ok
}

// IsViewOperation reports whether op is authorized on a view ARN rather than a namespace ARN.
func (op Operation) IsViewOperation() bool {
	_, ok := viewOperations[op]
	return ok
}

// IsWrite reports whether op changes the branch, and so needs a branch ref and fs:WriteObject on
// the reserved _lakefs_iceberg/ area (§7.2, Q-I4).
func (op Operation) IsWrite() bool {
	_, ok := writeOperations[op]
	return ok
}

// String implements fmt.Stringer.
func (op Operation) String() string { return string(op) }

// Request is one catalog operation on one address, as the HTTP layer resolves it from the route.
type Request struct {
	// Op is the operation the endpoint implements.
	Op Operation

	// Address is the parsed address of ident.go. For OpListTables, Address.Table is optional:
	// when it is set, the decision additionally evaluates ConditionFieldTableName for that
	// table, which is the per-table filter of a listing (FilterTables). OpListViews has the
	// same optional View field, evaluated with ConditionFieldViewName (FilterViews).
	Address Address

	// RefIsBranch reports whether Address.Ref resolved to a branch. It is only consulted for a
	// write, where a false value is the Q-I4 refusal: the zero value therefore denies writes,
	// which is the direction a caller that forgets to set it must fail in.
	RefIsBranch bool
}

// Authorizer answers the authorization questions of the catalog by delegating to the auth
// service, exactly the way pkg/api Controller.authorizeCallback does and pkg/datasets
// Authorizer.Check does: one auth.AuthorizationRequest with a permissions.Node, so that Deny
// precedence, wildcards, conditions and ${user} interpolation behave identically to the core API.
type Authorizer struct {
	service auth.Authorizer
}

// NewAuthorizer wraps an auth service (auth.Service implements auth.Authorizer) in an Authorizer.
// A nil service makes every check fail with ErrNoAuthorizer: the catalog must never treat "no
// auth service" as "allowed".
func NewAuthorizer(service auth.Authorizer) *Authorizer {
	return &Authorizer{service: service}
}

// Authorize authorizes req for caller and returns nil when the decision is "allow".
//
// The permission decision is made first and the Q-I4 ref rule second, deliberately: an
// unauthorized caller learns nothing about the ref it named - not even whether it is a branch -
// because every refusal for it is the same 403 "permission denied". A caller that is allowed to
// write but addressed a tag or a commit gets the Q-I4 refusal, "ref is read-only".
//
// A denial is returned as *PermissionDeniedError or *ReadOnlyRefError (both 403); a failure to
// decide is returned as *AuthorizationError (500); a Request that does not describe a valid
// operation/address pair is returned as an error wrapping ErrInvalidAuthzRequest. Callers must
// treat any non-nil error as a refusal.
func (a *Authorizer) Authorize(ctx context.Context, caller Caller, req Request) error {
	if err := a.authorizePermission(ctx, caller, req); err != nil {
		return err
	}
	if !req.Op.IsWrite() || req.RefIsBranch {
		return nil
	}
	// Q-I4: writes need a branch. A tag or a commit ID is a read-only ref, and letting it
	// through would move the repository's catalog state to a ref that cannot be committed to.
	return &ReadOnlyRefError{Operation: req.Op, Ref: req.Address.Ref}
}

// FilterTables returns the tables of addr the caller may list, in the order of names.
//
// §7.2 authorizes listing on the namespace ARN and filters the result with the
// catalog:TableName condition, so a policy can grant catalog:ListTables for "analytics-*" only.
// The filter is the listing gate itself, evaluated once per name:
//
//   - a name is visible when catalog:ListTables is allowed on the namespace ARN with
//     ConditionFieldTableName set to that name;
//   - a denial hides the name. A failure to decide is returned as an error: it must never be
//     turned into "not visible", which would hide data, nor into "visible", which would reveal
//     it;
//   - when the namespace-level gate (the same check without a table name) is a denial and no
//     name survives, the denial is returned rather than an empty page: a caller who may not list
//     the namespace must not be able to tell an empty namespace from a forbidden one.
//
// The per-name evaluation costs one auth decision per table, which is the same order as the
// branch listing of the core API (Controller.ListBranches filters per branch). A caller with an
// unconditional grant pays it too; the alternative - inspecting effective policies for a
// catalog:TableName condition - needs an optional auth service capability and would make the
// decision depend on which auth service implementation is installed.
//
// addr must be a namespace address (no table); Operation OpListTables.
func (a *Authorizer) FilterTables(ctx context.Context, caller Caller, addr Address, names []string) ([]string, error) {
	return a.filterNamed(ctx, caller, addr, names, OpListTables, "table")
}

// FilterViews returns the views of addr the caller may list, in the order of names.
//
// It is FilterTables for the view half of the listing surface: §7.2's rule is that a listing is
// authorized on the namespace ARN and filtered per object by a condition, and for views that
// condition field is catalog:ViewName (ConditionFieldViewName). The consequences are the same as
// for tables, and they are the reason the filter is the listing gate itself rather than a
// post-processing step:
//
//   - a name is visible when catalog:ListViews is allowed on the namespace ARN with
//     ConditionFieldViewName set to that name;
//   - a denial hides the name; a failure to decide is returned as an error and is never turned
//     into "not visible" or "visible";
//   - when the namespace-level gate is a denial and no name survives, the denial is returned
//     rather than an empty page, so a caller who may not list the namespace cannot tell an empty
//     namespace from a forbidden one.
//
// addr must be a namespace address (no view); Operation OpListViews.
func (a *Authorizer) FilterViews(ctx context.Context, caller Caller, addr Address, names []string) ([]string, error) {
	return a.filterNamed(ctx, caller, addr, names, OpListViews, "view")
}

// filterNamed is the shared implementation of FilterTables and FilterViews: the namespace-level
// gate, then one decision per name with the language's per-object condition field set. op and noun
// are the only things the two listings differ in, so the two cannot drift apart on a question that
// matters (what a denial means, what an empty result means, what the gate is).
func (a *Authorizer) filterNamed(ctx context.Context, caller Caller, addr Address, names []string, op Operation, noun string) ([]string, error) {
	if addr.Table != "" || addr.View != "" {
		return nil, fmt.Errorf("%w: listing %ss needs a namespace address, got %s %q",
			ErrInvalidAuthzRequest, noun, noun, cmp.Or(addr.View, addr.Table))
	}
	gate := Request{Op: op, Address: Address{Repo: addr.Repo, Ref: addr.Ref, Namespace: addr.Namespace}}
	gateErr := a.authorizePermission(ctx, caller, gate)
	if gateErr != nil && !IsDenial(gateErr) {
		return nil, gateErr
	}

	visible := make([]string, 0, len(names))
	for _, name := range names {
		probe := Address{Repo: addr.Repo, Ref: addr.Ref, Namespace: addr.Namespace}
		if noun == "view" {
			probe.View = name
		} else {
			probe.Table = name
		}
		err := a.authorizePermission(ctx, caller, Request{Op: op, Address: probe})
		switch {
		case err == nil:
			visible = append(visible, name)
		case !IsDenial(err):
			return nil, err
		}
	}
	if gateErr != nil && len(visible) == 0 {
		// Nothing is visible and the caller may not list the namespace either: refuse, so that
		// an empty result cannot be told apart from a forbidden namespace.
		return nil, gateErr
	}
	return visible, nil
}

// IsDenial reports whether err is a refusal rather than a failure to decide. It matches both
// refusal types of this file.
func IsDenial(err error) bool {
	var (
		denied   *PermissionDeniedError
		readOnly *ReadOnlyRefError
	)
	return errors.As(err, &denied) || errors.As(err, &readOnly)
}

// authorizePermission runs the §7.2 permission node of req against the auth service, without the
// Q-I4 ref rule (which is not a property of the caller).
func (a *Authorizer) authorizePermission(ctx context.Context, caller Caller, req Request) error {
	node, err := RequiredNode(req)
	if err != nil {
		return err
	}
	fields := tableNameFields(caller.Fields, req.Address.Table)
	if req.Address.View != "" {
		// A view-scoped decision carries catalog:ViewName instead of catalog:TableName: the two
		// conditions are separate fields, so neither can accidentally scope the other's objects.
		fields = viewNameFields(caller.Fields, req.Address.View)
	}
	if a == nil || a.service == nil {
		return &AuthorizationError{Username: caller.Username, Operation: req.Op, Err: ErrNoAuthorizer}
	}

	// The condition context is both put on ctx (auth.WithConditionContext) and passed on the
	// request (ClientIP/Fields), like pkg/datasets Authorizer.Check: acl.AuthService evaluates
	// conditions from the context, auth.APIAuthService builds its condition context from the
	// request. Setting both means a condition is honoured by either implementation, so a
	// conditional deny is never silently skipped.
	conditionCtx := auth.WithConditionContext(ctx, auth.NewConditionContextWithProvider(caller.ClientIP, fields))
	resp, err := a.service.Authorize(conditionCtx, &auth.AuthorizationRequest{
		Username:            caller.Username,
		RequiredPermissions: node,
		ClientIP:            caller.ClientIP,
		Fields:              fields,
	})
	if err != nil {
		return &AuthorizationError{Username: caller.Username, Operation: req.Op, Err: err}
	}
	switch {
	case resp == nil:
		return &AuthorizationError{Username: caller.Username, Operation: req.Op, Err: ErrNoAuthorizationDecision}
	case resp.Error != nil && errors.Is(resp.Error, auth.ErrInsufficientPermissions):
		return &PermissionDeniedError{
			Username:  caller.Username,
			Operation: req.Op,
			Required:  requiredPermissions(node),
			Err:       resp.Error,
		}
	case resp.Error != nil:
		return &AuthorizationError{Username: caller.Username, Operation: req.Op, Err: resp.Error}
	case !resp.Allowed:
		return &PermissionDeniedError{
			Username:  caller.Username,
			Operation: req.Op,
			Required:  requiredPermissions(node),
			Err:       auth.ErrInsufficientPermissions,
		}
	default:
		return nil
	}
}

// RequiredNode builds the §7.2 permission node of req:
//
//	catalog:<Action>   on CatalogTableArn(repo, ns, t) or CatalogNamespaceArn(repo, ns)  (§7.2)
//	fs:ReadRepository  on RepoArn(repo)                                                  (§7.2)
//	fs:WriteObject     on ObjectArn(repo, "_lakefs_iceberg/*")          writes only      (§7.2)
//
// joined with And, so that a Deny on any of them refuses the request: that is how the existing
// dataset, repository and fs: denies reach the catalog without a catalog-shaped deny statement.
//
// The one address without a repository is the root of the namespace listing ("GET namespaces with
// no parent lists repositories the caller can read", §5). That level is a lakeFS repository list,
// not a catalog object, so it requires fs:ListRepositories on every resource and no catalog
// action: catalog ARNs are per repository, and requiring catalog:ListNamespaces on "*" would
// refuse every policy that grants the action on a concrete ARN.
//
// The node is exported so the HTTP layer can log it and so the tests can assert the matrix
// without an auth service.
func RequiredNode(req Request) (permissions.Node, error) {
	action, err := req.Op.Action()
	if err != nil {
		return permissions.Node{}, err
	}
	addr := req.Address

	if addr.Repo == "" {
		// The repository level of the namespace listing (§5).
		if req.Op != OpListNamespaces {
			return permissions.Node{}, fmt.Errorf("%w: %s needs a repository", ErrInvalidAuthzRequest, req.Op)
		}
		if addr.Ref != "" || len(addr.Namespace) > 0 || addr.Table != "" {
			return permissions.Node{}, fmt.Errorf("%w: %s without a repository cannot name a ref, "+
				"namespace or table", ErrInvalidAuthzRequest, req.Op)
		}
		return permissions.Node{
			Type:  permissions.NodeTypeAnd,
			Nodes: []permissions.Node{leaf(permissions.ListRepositoriesAction, permissions.All)},
		}, nil
	}

	switch {
	case req.Op.IsTableOperation():
		if addr.Table == "" {
			return permissions.Node{}, fmt.Errorf("%w: %s needs a table", ErrInvalidAuthzRequest, req.Op)
		}
		if len(addr.Namespace) == 0 {
			return permissions.Node{}, fmt.Errorf("%w: %s needs a namespace between the ref and the table",
				ErrInvalidAuthzRequest, req.Op)
		}
	case req.Op.IsViewOperation():
		if addr.View == "" {
			return permissions.Node{}, fmt.Errorf("%w: %s needs a view", ErrInvalidAuthzRequest, req.Op)
		}
		if len(addr.Namespace) == 0 {
			return permissions.Node{}, fmt.Errorf("%w: %s needs a namespace between the ref and the view",
				ErrInvalidAuthzRequest, req.Op)
		}
	case req.Op == OpListTables, req.Op == OpListViews:
		// A listing may carry one name: that is the per-object probe of FilterTables /
		// FilterViews, not an address of a table or a view. The other name must stay empty,
		// or the address would describe both a table and a view at once.
		if req.Op == OpListTables && addr.View != "" {
			return permissions.Node{}, fmt.Errorf("%w: %s is a table listing, not view %q",
				ErrInvalidAuthzRequest, req.Op, addr.View)
		}
		if req.Op == OpListViews && addr.Table != "" {
			return permissions.Node{}, fmt.Errorf("%w: %s is a view listing, not table %q",
				ErrInvalidAuthzRequest, req.Op, addr.Table)
		}
	default:
		if addr.Table != "" || addr.View != "" {
			return permissions.Node{}, fmt.Errorf("%w: %s is a namespace operation, not table %q or view %q",
				ErrInvalidAuthzRequest, req.Op, addr.Table, addr.View)
		}
	}
	if addr.Ref == "" && req.Op != OpListNamespaces {
		// Only the namespace listing has a repository level; everything else addresses
		// repo.ref (or below), and an empty ref would silently widen the address to the whole
		// repository.
		return permissions.Node{}, fmt.Errorf("%w: %s needs a ref", ErrInvalidAuthzRequest, req.Op)
	}

	resource := permissions.CatalogNamespaceArn(addr.Repo, addr.Namespace.Dotted())
	switch {
	case req.Op.IsTableOperation():
		resource = permissions.CatalogTableArn(addr.Repo, addr.Namespace.Dotted(), addr.Table)
	case req.Op.IsViewOperation():
		resource = permissions.CatalogViewArn(addr.Repo, addr.Namespace.Dotted(), addr.View)
	}
	nodes := []permissions.Node{
		leaf(action, resource),
		leaf(permissions.ReadRepositoryAction, permissions.RepoArn(addr.Repo)),
	}
	if req.Op.IsWrite() {
		nodes = append(nodes, leaf(permissions.WriteObjectAction,
			permissions.ObjectArn(addr.Repo, icebergWriteObjectKey)))
	}
	return permissions.Node{Type: permissions.NodeTypeAnd, Nodes: nodes}, nil
}

// leaf builds one permission leaf of a Node.
func leaf(action, resource string) permissions.Node {
	return permissions.Node{Permission: permissions.Permission{Action: action, Resource: resource}}
}

// tableNameFields returns the condition-field provider of one decision: it answers
// ConditionFieldTableName with table and delegates every other key to base. With an empty table
// it is base itself, so a policy statement conditioned on catalog:TableName does not match a
// decision that is not about one table - which is exactly what makes the namespace-level list
// gate a different question from the per-table one.
func tableNameFields(base func(key string) (string, bool), table string) func(key string) (string, bool) {
	return namedFields(base, ConditionFieldTableName, table)
}

// viewNameFields is tableNameFields for a view: it answers ConditionFieldViewName with view and
// delegates every other key to base. A decision that is not about one view gets base itself, so
// catalog:ViewName never matches the namespace-level gate.
func viewNameFields(base func(key string) (string, bool), view string) func(key string) (string, bool) {
	return namedFields(base, ConditionFieldViewName, view)
}

// namedFields is the shared builder of the two providers above: the named condition field is
// answered with value when value is non-empty, and every other key is delegated to base.
func namedFields(base func(key string) (string, bool), field, value string) func(key string) (string, bool) {
	if value == "" {
		return base
	}
	return func(key string) (string, bool) {
		if key == field {
			return value, true
		}
		if base == nil {
			return "", false
		}
		return base(key)
	}
}

// PermissionDeniedError is the §7.2 refusal: the caller is not allowed the operation on the
// address. It is a 403 ForbiddenException for the Iceberg error envelope.
type PermissionDeniedError struct {
	// Username is the principal the decision was made for.
	Username string
	// Operation is the operation that was refused.
	Operation Operation
	// Required is the permission set that was checked, flattened from the node.
	Required []permissions.Permission
	// Err is the underlying reason: auth.ErrInsufficientPermissions, or the auth service's own
	// error describing which action was denied.
	Err error
}

// Error implements error. It names the operation, the permissions that were checked and the
// underlying reason, so an operator reading a log knows what to grant. The address is not
// repeated: the message of a denial must not echo a namespace or table name the caller has no
// grant on.
func (e *PermissionDeniedError) Error() string {
	var b strings.Builder
	b.WriteString("forbidden: user ")
	b.WriteString(e.Username)
	b.WriteString(" is not allowed to ")
	b.WriteString(e.Operation.String())
	if actions := joinActions(e.Required); actions != "" {
		b.WriteString(" (")
		b.WriteString(actions)
		b.WriteString(")")
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	return b.String()
}

// Unwrap reports ErrPermissionDenied and the underlying auth error, so callers can match either
// one with errors.Is.
func (e *PermissionDeniedError) Unwrap() error {
	return errors.Join(ErrPermissionDenied, e.Err)
}

// HTTPStatusCode is the status of the refusal, 403.
func (e *PermissionDeniedError) HTTPStatusCode() int { return http.StatusForbidden }

// ErrorType is the Iceberg error type of the refusal.
func (e *PermissionDeniedError) ErrorType() string { return ErrorTypeForbidden }

// ReadOnlyRefError is the Q-I4 refusal: a write addressed a ref that is not a branch, so the
// catalog can read the table there but must not change it. It is a 403 ForbiddenException whose
// message is "ref is read-only".
type ReadOnlyRefError struct {
	// Operation is the write that was refused.
	Operation Operation
	// Ref is the ref the request addressed. It is a tag or a commit, never a branch.
	Ref string
}

// Error implements error: the Q-I4 message, with the ref, for logs. It does not name the
// repository, namespace or table.
func (e *ReadOnlyRefError) Error() string {
	return fmt.Sprintf("%s: %q is not a branch", ErrRefReadOnly, e.Ref)
}

// Message is the message of the Iceberg error envelope: exactly "ref is read-only" (Q-I4). The
// HTTP layer puts it in {"error": {"message": ...}}; Error is the log form and carries the ref.
func (e *ReadOnlyRefError) Message() string { return ErrRefReadOnly.Error() }

// Unwrap reports ErrRefReadOnly, so errors.Is(err, ErrRefReadOnly) classifies the refusal.
func (e *ReadOnlyRefError) Unwrap() error { return ErrRefReadOnly }

// HTTPStatusCode is the status of the refusal, 403 (Q-I4).
func (e *ReadOnlyRefError) HTTPStatusCode() int { return http.StatusForbidden }

// ErrorType is the Iceberg error type of the refusal.
func (e *ReadOnlyRefError) ErrorType() string { return ErrorTypeForbidden }

// AuthorizationError reports that the authorization decision itself failed - the auth service
// returned an error, or answered something that is not a decision. It is a server-side failure,
// never a denial, and must not be reported as 403.
type AuthorizationError struct {
	// Username is the principal the decision was made for.
	Username string
	// Operation is the operation that could not be decided.
	Operation Operation
	// Err is the failure.
	Err error
}

// Error implements error.
func (e *AuthorizationError) Error() string {
	return fmt.Sprintf("icebergcatalog: authorization failed for user %s on %s: %s",
		e.Username, e.Operation, e.Err)
}

// Unwrap exposes the underlying failure.
func (e *AuthorizationError) Unwrap() error { return e.Err }

// HTTPStatusCode is the status of the failure, 500.
func (e *AuthorizationError) HTTPStatusCode() int { return http.StatusInternalServerError }

// ErrorType is the Iceberg error type of the failure.
func (e *AuthorizationError) ErrorType() string { return ErrorTypeInternal }

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
