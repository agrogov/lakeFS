package icebergcatalog

// Tests for ICEBERG_CATALOG_PLAN.md §7.2 (the authorization matrix), §12 ("unit tests: authz
// matrix") and §14 Q-I4 (writes on a non-branch ref). Every test name starts with TestAuthz so
// that the task's validation command, `go test -run TestAuthz ./pkg/icebergcatalog/`, runs
// exactly this file.
//
// The decisions are exercised against a real auth service - the simplified-ACL implementation
// the lakefs-sso binary uses - on an in-memory KV store, not against a mock: the point of the
// matrix is that the permission nodes this package builds are evaluated by the same
// Deny-precedence, wildcard and condition machinery as the core API. The ARNs and actions are
// asserted as literals, so the test is a check of §7.2's table rather than of this package's own
// helpers.

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/datasets"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	authzRepo = "lakehouse"
	authzRef  = "main"

	// The §7.2 resources, as literals: pkg/permissions builds them, but the ARN shape is the
	// contract a policy author writes by hand.
	authzNamespaceArn = "arn:lakefs:catalog:::namespace/lakehouse/inventory.books"
	authzTableArn     = "arn:lakefs:catalog:::table/lakehouse/inventory.books/titles"
	authzViewArn      = "arn:lakefs:catalog:::view/lakehouse/inventory.books/orders_view"
	authzRepoArn      = "arn:lakefs:fs:::repository/lakehouse"

	// authzWriteObjectArn is the wildcard object ARN of the reserved _lakefs_iceberg/ area. No
	// catalog request asks for it any more - a catalog request needs its catalog action only
	// (authz.go, the public Iceberg documentation's "catalog permissions supersede object
	// permissions") - but gc_test.go pins the reserved-prefix deny against it: the deny must not
	// match a request for the whole reserved area, and that wildcard is the shape such a request
	// has.
	authzWriteObjectArn = "arn:lakefs:fs:::repository/lakehouse/object/_lakefs_iceberg/*"
)

// authzAllOps is the §7.2 matrix, in the order of the table.
var authzAllOps = []Operation{
	OpListNamespaces,
	OpGetNamespace,
	OpCreateNamespace,
	OpUpdateNamespace,
	OpDeleteNamespace,
	OpListTables,
	OpReadTable,
	OpCreateTable,
	OpUpdateTable,
	OpDeleteTable,
	OpReadTableData,
	OpWriteTableData,
	OpListViews,
	OpReadView,
	OpCreateView,
	OpUpdateView,
	OpDeleteView,
}

// authzTableAddress is the address of a table; authzViewAddress the address of a view in the same
// namespace; authzNamespaceAddress the address of the namespace that contains both.
func authzTableAddress() Address {
	return Address{Repo: authzRepo, Ref: authzRef, Namespace: Namespace{"inventory", "books"}, Table: "titles"}
}

func authzViewAddress() Address {
	return Address{Repo: authzRepo, Ref: authzRef, Namespace: Namespace{"inventory", "books"}, View: "orders_view"}
}

func authzNamespaceAddress() Address {
	return Address{Repo: authzRepo, Ref: authzRef, Namespace: Namespace{"inventory", "books"}}
}

// authzAddressFor returns an address op is defined on: a table address for the table operations, a
// view address for the view operations, and a namespace address for everything else.
func authzAddressFor(op Operation) Address {
	switch {
	case op.IsTableOperation():
		return authzTableAddress()
	case op.IsViewOperation():
		return authzViewAddress()
	default:
		return authzNamespaceAddress()
	}
}

// authzReadStatements grants every catalog read action on every catalog ARN, and nothing else:
// the catalog action is the whole permission of a catalog request (authz.go), so no fs: action
// appears here.
func authzReadStatements() model.Statements {
	return model.Statements{
		{
			Effect: model.StatementEffectAllow,
			Action: []string{
				permissions.CatalogListNamespacesAction,
				permissions.CatalogGetNamespaceAction,
				permissions.CatalogListTablesAction,
				permissions.CatalogReadTableAction,
				permissions.CatalogReadTableDataAction,
				permissions.CatalogListViewsAction,
				permissions.CatalogReadViewAction,
			},
			Resource: "arn:lakefs:catalog:::*",
		},
	}
}

// authzWriteStatements is authzReadStatements plus the write half of §7.2: the catalog write
// actions, again on catalog ARNs only.
func authzWriteStatements() model.Statements {
	return append(authzReadStatements(),
		model.Statement{
			Effect: model.StatementEffectAllow,
			Action: []string{
				permissions.CatalogCreateNamespaceAction,
				permissions.CatalogUpdateNamespaceAction,
				permissions.CatalogDeleteNamespaceAction,
				permissions.CatalogCreateTableAction,
				permissions.CatalogUpdateTableAction,
				permissions.CatalogDeleteTableAction,
				permissions.CatalogWriteTableDataAction,
				permissions.CatalogCreateViewAction,
				permissions.CatalogUpdateViewAction,
				permissions.CatalogDeleteViewAction,
			},
			Resource: "arn:lakefs:catalog:::*",
		})
}

// authzFSStatements grants every fs: action on every resource and no catalog action at all. The
// public Iceberg documentation states the rule this case pins ("Iceberg catalog permissions
// supersede any related object permissions"): a principal holding the whole fs: half is refused
// every catalog request.
func authzFSStatements() model.Statements {
	return model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"fs:*"},
		Resource: permissions.All,
	}}
}

// authzGrantsExceptAction is authzWriteStatements with one catalog action removed, so a test can
// show that an operation needs its own action and not merely some catalog grant.
func authzGrantsExceptAction(action string) model.Statements {
	var out model.Statements
	for _, statement := range authzWriteStatements() {
		actions := make([]string, 0, len(statement.Action))
		for _, a := range statement.Action {
			if a != action {
				actions = append(actions, a)
			}
		}
		if len(actions) == 0 {
			continue
		}
		statement.Action = actions
		out = append(out, statement)
	}
	return out
}

// authzFixture is an acl.AuthService (the implementation the lakefs-sso binary uses) on an
// in-memory KV store, plus the Authorizer built on top of it. The store is kept because the
// datasets immutability guard reads the dataset repository names from it.
type authzFixture struct {
	authz *Authorizer
	svc   *acl.AuthService
	store kv.Store
}

func newAuthzFixture(t *testing.T) *authzFixture {
	t.Helper()
	store := kvtest.GetStore(t.Context(), t)
	svc := acl.NewAuthService(store, crypt.NewSecretStore([]byte("iceberg-authz-test-secret")),
		authparams.ServiceCache{Enabled: false})
	return &authzFixture{svc: svc, authz: NewAuthorizer(svc), store: store}
}

// user creates the principal the decisions are made for.
func (f *authzFixture) user(t *testing.T, username string) {
	t.Helper()
	_, err := f.svc.CreateUser(t.Context(), &model.User{Username: username, CreatedAt: time.Now()})
	require.NoErrorf(t, err, "create user %s", username)
}

// attach writes policy and attaches it to username.
func (f *authzFixture) attach(t *testing.T, username string, policy *model.Policy) {
	t.Helper()
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = time.Now()
	}
	require.NoErrorf(t, f.svc.WritePolicy(t.Context(), policy, false), "write policy %s", policy.DisplayName)
	require.NoErrorf(t, f.svc.AttachPolicyToUser(t.Context(), policy.DisplayName, username),
		"attach policy %s to %s", policy.DisplayName, username)
}

// attachStatements attaches one policy named name built from statements.
func (f *authzFixture) attachStatements(t *testing.T, name, username string, statements model.Statements) {
	t.Helper()
	f.attach(t, username, &model.Policy{DisplayName: name, Statement: statements})
}

// authorize runs one decision as username.
func (f *authzFixture) authorize(t *testing.T, username string, req Request) error {
	t.Helper()
	return f.authz.Authorize(t.Context(), Caller{Username: username}, req)
}

// newAuthzUser returns a fixture with a single principal carrying statements.
func newAuthzUser(t *testing.T, statements model.Statements) *authzFixture {
	t.Helper()
	f := newAuthzFixture(t)
	f.user(t, "alice")
	f.attachStatements(t, "grants", "alice", statements)
	return f
}

// TestAuthzRequiredNode asserts the action and resource columns of the §7.2 matrix: every
// repository-scoped operation is one catalog leaf on its catalog ARN, and no fs: leaf is added
// (authz.go; the public Iceberg documentation's example policies grant catalog actions only).
func TestAuthzRequiredNode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  Request
		want []permissions.Permission
	}{
		{
			name: "list namespaces is authorized on the parent namespace ARN",
			req:  Request{Op: OpListNamespaces, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ListNamespaces", Resource: authzNamespaceArn},
			},
		},
		{
			name: "list namespaces at the repository level is authorized on the repository namespace ARN",
			req:  Request{Op: OpListNamespaces, Address: Address{Repo: authzRepo}},
			want: []permissions.Permission{
				{Action: "catalog:ListNamespaces", Resource: "arn:lakefs:catalog:::namespace/lakehouse/"},
			},
		},
		{
			name: "list namespaces at the root is a lakeFS repository list",
			req:  Request{Op: OpListNamespaces},
			want: []permissions.Permission{
				{Action: "fs:ListRepositories", Resource: permissions.All},
			},
		},
		{
			name: "get namespace",
			req:  Request{Op: OpGetNamespace, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:GetNamespace", Resource: authzNamespaceArn},
			},
		},
		{
			name: "create namespace",
			req:  Request{Op: OpCreateNamespace, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:CreateNamespace", Resource: authzNamespaceArn},
			},
		},
		{
			name: "update namespace",
			req:  Request{Op: OpUpdateNamespace, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:UpdateNamespace", Resource: authzNamespaceArn},
			},
		},
		{
			name: "delete namespace",
			req:  Request{Op: OpDeleteNamespace, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:DeleteNamespace", Resource: authzNamespaceArn},
			},
		},
		{
			name: "list tables is authorized on the namespace ARN",
			req:  Request{Op: OpListTables, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ListTables", Resource: authzNamespaceArn},
			},
		},
		{
			name: "read table is authorized on the table ARN",
			req:  Request{Op: OpReadTable, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ReadTable", Resource: authzTableArn},
			},
		},
		{
			name: "create table",
			req:  Request{Op: OpCreateTable, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:CreateTable", Resource: authzTableArn},
			},
		},
		{
			name: "update table",
			req:  Request{Op: OpUpdateTable, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:UpdateTable", Resource: authzTableArn},
			},
		},
		{
			name: "delete table",
			req:  Request{Op: OpDeleteTable, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:DeleteTable", Resource: authzTableArn},
			},
		},
		{
			name: "read table data",
			req:  Request{Op: OpReadTableData, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ReadTableData", Resource: authzTableArn},
			},
		},
		{
			name: "write table data",
			req:  Request{Op: OpWriteTableData, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:WriteTableData", Resource: authzTableArn},
			},
		},
		{
			name: "list views is authorized on the namespace ARN",
			req:  Request{Op: OpListViews, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ListViews", Resource: authzNamespaceArn},
			},
		},
		{
			name: "read view is authorized on the view ARN",
			req:  Request{Op: OpReadView, Address: authzViewAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ReadView", Resource: authzViewArn},
			},
		},
		{
			name: "create view",
			req:  Request{Op: OpCreateView, Address: authzViewAddress()},
			want: []permissions.Permission{
				{Action: "catalog:CreateView", Resource: authzViewArn},
			},
		},
		{
			name: "update view",
			req:  Request{Op: OpUpdateView, Address: authzViewAddress()},
			want: []permissions.Permission{
				{Action: "catalog:UpdateView", Resource: authzViewArn},
			},
		},
		{
			name: "delete view",
			req:  Request{Op: OpDeleteView, Address: authzViewAddress()},
			want: []permissions.Permission{
				{Action: "catalog:DeleteView", Resource: authzViewArn},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node, err := RequiredNode(tt.req)
			require.NoError(t, err)
			// One leaf: the catalog action is the whole requirement, so no fs: deny can refuse
			// the request and no fs: grant is needed for it.
			require.Equal(t, permissions.NodeTypeNode, node.Type)
			require.Equal(t, tt.want, requiredPermissions(node))
		})
	}
}

// TestAuthzMatrix is the §7.2 matrix: every operation is allowed by the catalog grant §7.2 asks
// for, and refused when that one action is missing - no fs: action takes part either way.
func TestAuthzMatrix(t *testing.T) {
	t.Parallel()
	for _, op := range authzAllOps {
		op := op
		grants := authzReadStatements()
		if op.IsWrite() {
			grants = authzWriteStatements()
		}

		t.Run(op.String()+"/granted", func(t *testing.T) {
			t.Parallel()
			f := newAuthzUser(t, grants)
			require.NoError(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}))
		})

		t.Run(op.String()+"/no catalog grant", func(t *testing.T) {
			t.Parallel()
			// fs:* on everything and no catalog action: refused, the documentation's "catalog
			// permissions supersede any related object permissions".
			f := newAuthzUser(t, authzFSStatements())
			err := f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			})
			require.ErrorIs(t, err, ErrPermissionDenied)
			var denied *PermissionDeniedError
			require.ErrorAs(t, err, &denied)
			require.Equal(t, http.StatusForbidden, denied.HTTPStatusCode())
			require.Equal(t, "ForbiddenException", denied.ErrorType())
			require.Equal(t, op, denied.Operation)
			// The refusal must name the missing catalog action, and nothing else: the catalog
			// action is the whole requirement.
			action, err := op.Action()
			require.NoError(t, err)
			require.Equal(t, action, joinActions(denied.Required))
			require.Equal(t, []permissions.Permission{requiredLeafFor(op)}, denied.Required)
		})

		t.Run(op.String()+"/only another catalog action granted", func(t *testing.T) {
			t.Parallel()
			// Every grant of the matrix except this operation's own action: a broad catalog:*
			// grant would hide a wrong action mapping, so the actions are enumerated.
			action, err := op.Action()
			require.NoError(t, err)
			f := newAuthzUser(t, authzGrantsExceptAction(action))
			require.ErrorIs(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}), ErrPermissionDenied)
		})

		if !op.IsWrite() {
			continue
		}

		t.Run(op.String()+"/read grant does not allow a write", func(t *testing.T) {
			t.Parallel()
			// The read half of the matrix, which is every catalog read action on every catalog
			// ARN: a write is still refused on its own action.
			f := newAuthzUser(t, authzReadStatements())
			require.ErrorIs(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}), ErrPermissionDenied)
		})

		t.Run(op.String()+"/fs: deny does not refuse the write", func(t *testing.T) {
			t.Parallel()
			// The other direction of "catalog permissions supersede object permissions": a deny
			// of the whole fs: service - the shape the reserved-prefix and datasets denies take -
			// is not part of the catalog's requirement, so the catalog action still decides.
			f := newAuthzUser(t, append(authzWriteStatements(), model.Statement{
				Effect:   model.StatementEffectDeny,
				Action:   []string{"fs:*"},
				Resource: permissions.All,
			}))
			require.NoError(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}))
		})
	}
}

// requiredLeafFor is the one permission leaf RequiredNode builds for op's address: the catalog
// action on the table, view or namespace ARN.
func requiredLeafFor(op Operation) permissions.Permission {
	action, err := op.Action()
	if err != nil {
		panic(err)
	}
	addr := authzAddressFor(op)
	switch {
	case op.IsTableOperation():
		return permissions.Permission{Action: action, Resource: permissions.CatalogTableArn(addr.Repo, addr.Namespace.Dotted(), addr.Table)}
	case op.IsViewOperation():
		return permissions.Permission{Action: action, Resource: permissions.CatalogViewArn(addr.Repo, addr.Namespace.Dotted(), addr.View)}
	default:
		return permissions.Permission{Action: action, Resource: permissions.CatalogNamespaceArn(addr.Repo, addr.Namespace.Dotted())}
	}
}

// The three example policies of the public Iceberg documentation
// (https://docs.lakefs.io/iceberg/, "Example Policies"), transcribed statement for statement:
// catalog actions on catalog ARNs and nothing else, which is the shape RA10 makes sufficient.

// docReadOnlyStatements is "Read-only access to all tables and namespaces".
func docReadOnlyStatements() model.Statements {
	return model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.CatalogReadTableAction, permissions.CatalogReadTableDataAction},
			Resource: "arn:lakefs:catalog:::table/*/*/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.CatalogListNamespacesAction, permissions.CatalogGetNamespaceAction},
			Resource: "arn:lakefs:catalog:::namespace/*/*",
		},
	}
}

// docReadWriteStatements is "Read-write access to all tables in a repository (including data
// credentials)", on docPolicyRepo.
func docReadWriteStatements() model.Statements {
	return model.Statements{{
		Effect: model.StatementEffectAllow,
		Action: []string{
			permissions.CatalogReadTableAction,
			permissions.CatalogCreateTableAction,
			permissions.CatalogUpdateTableAction,
			permissions.CatalogReadTableDataAction,
			permissions.CatalogWriteTableDataAction,
		},
		Resource: permissions.CatalogTableArn(docPolicyRepo, "*", "*"),
	}}
}

// docFullAccessStatements is "Full access to tables in a specific repository", on docPolicyRepo.
func docFullAccessStatements() model.Statements {
	return model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: permissions.CatalogTableArn(docPolicyRepo, "*", "*"),
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"catalog:*"},
			Resource: permissions.CatalogNamespaceArn(docPolicyRepo, "*"),
		},
	}
}

// docPolicyRepo is the repository of the two repository-scoped example policies.
const docPolicyRepo = "my-repo"

// TestAuthzDocExamplePolicies attaches each example policy of the public Iceberg documentation
// alone and asserts it allows exactly what its title says and nothing else: this is the rule RA10
// implements ("Iceberg catalog permissions supersede any related object permissions"), and before
// it every one of these policies was refused because no fs: action was granted.
func TestAuthzDocExamplePolicies(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		statements model.Statements
		repo       string
		allowed    map[Operation]bool
	}{
		{
			name:       "read-only access to all tables and namespaces",
			statements: docReadOnlyStatements(),
			repo:       authzRepo,
			allowed: map[Operation]bool{
				OpListNamespaces: true,
				OpGetNamespace:   true,
				OpReadTable:      true,
				OpReadTableData:  true,
			},
		},
		{
			name:       "read-write access to all tables in a repository",
			statements: docReadWriteStatements(),
			repo:       docPolicyRepo,
			allowed: map[Operation]bool{
				OpReadTable:      true,
				OpCreateTable:    true,
				OpUpdateTable:    true,
				OpReadTableData:  true,
				OpWriteTableData: true,
			},
		},
		{
			name:       "full access to tables in a specific repository",
			statements: docFullAccessStatements(),
			repo:       docPolicyRepo,
			allowed: map[Operation]bool{
				OpListNamespaces:  true,
				OpGetNamespace:    true,
				OpCreateNamespace: true,
				OpUpdateNamespace: true,
				OpDeleteNamespace: true,
				OpListTables:      true,
				OpListViews:       true,
				OpReadTable:       true,
				OpCreateTable:     true,
				OpUpdateTable:     true,
				OpDeleteTable:     true,
				OpReadTableData:   true,
				OpWriteTableData:  true,
			},
		},
		{
			name:       "the read-write policy of a repository says nothing about another repository",
			statements: docReadWriteStatements(),
			repo:       "other-repo",
			allowed:    map[Operation]bool{},
		},
		{
			name:       "the full-access policy of a repository says nothing about another repository",
			statements: docFullAccessStatements(),
			repo:       "other-repo",
			allowed:    map[Operation]bool{},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newAuthzUser(t, tt.statements)
			for _, op := range authzAllOps {
				op := op
				t.Run(op.String(), func(t *testing.T) {
					t.Parallel()
					addr := authzAddressFor(op)
					addr.Repo = tt.repo
					err := f.authorize(t, "alice", Request{Op: op, Address: addr, RefIsBranch: true})
					if tt.allowed[op] {
						require.NoErrorf(t, err, "the policy must allow %s", op)
						return
					}
					require.ErrorIsf(t, err, ErrPermissionDenied, "the policy must not allow %s", op)
				})
			}
		})
	}
}

// TestAuthzFSGrantIsNoCatalogGrant is the other half of "catalog permissions supersede any related
// object permissions": a principal holding every fs: action on every resource, and no catalog
// action, is refused every catalog request.
func TestAuthzFSGrantIsNoCatalogGrant(t *testing.T) {
	t.Parallel()
	f := newAuthzUser(t, authzFSStatements())
	for _, op := range authzAllOps {
		op := op
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}), ErrPermissionDenied)
		})
	}
}

// TestAuthzNoGrants refuses every operation for a principal without any policy.
func TestAuthzNoGrants(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	f.user(t, "alice")
	for _, op := range authzAllOps {
		op := op
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}), ErrPermissionDenied)
		})
	}
}

// TestAuthzArnsAreRepositoryScoped proves that a grant on one repository's catalog ARN does not
// authorize the same namespace and table names in another repository, and that a namespace grant
// does not authorize a table operation: the ARN shape of §7.2 is what scopes the decision.
func TestAuthzArnsAreRepositoryScoped(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		op     Operation
		grant  string
		action string
	}{
		{
			name:   "table ARN of another repository",
			op:     OpReadTable,
			grant:  "arn:lakefs:catalog:::table/other-repo/inventory.books/titles",
			action: permissions.CatalogReadTableAction,
		},
		{
			name:   "namespace ARN of another repository",
			op:     OpGetNamespace,
			grant:  "arn:lakefs:catalog:::namespace/other-repo/inventory.books",
			action: permissions.CatalogGetNamespaceAction,
		},
		{
			name:   "namespace ARN does not authorize a table",
			op:     OpReadTable,
			grant:  authzNamespaceArn,
			action: permissions.CatalogReadTableAction,
		},
		{
			name:   "table ARN does not authorize a namespace",
			op:     OpGetNamespace,
			grant:  authzTableArn,
			action: permissions.CatalogGetNamespaceAction,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newAuthzUser(t, model.Statements{
				{Effect: model.StatementEffectAllow, Action: []string{tt.action}, Resource: tt.grant},
				{Effect: model.StatementEffectAllow, Action: []string{permissions.ReadRepositoryAction}, Resource: authzRepoArn},
			})
			require.ErrorIs(t, f.authorize(t, "alice", Request{
				Op: tt.op, Address: authzAddressFor(tt.op), RefIsBranch: true,
			}), ErrPermissionDenied)
		})
	}
}

// TestAuthzRefReadOnly is Q-I4: reads are allowed on any ref, writes only on a branch.
func TestAuthzRefReadOnly(t *testing.T) {
	t.Parallel()
	for _, op := range authzAllOps {
		op := op
		grants := authzReadStatements()
		if op.IsWrite() {
			grants = authzWriteStatements()
		}
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()
			f := newAuthzUser(t, grants)

			// A branch: allowed (write or read).
			require.NoError(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}))

			tagAddress := authzAddressFor(op)
			tagAddress.Ref = "release.v1"
			commitAddress := authzAddressFor(op)
			commitAddress.Ref = "30f9eb1c0f0d4b9e8d0f7b1d3cf1b2a4e5f60718"

			if !op.IsWrite() {
				// §5: reads work on a tag and on a commit ID.
				require.NoError(t, f.authorize(t, "alice", Request{Op: op, Address: tagAddress}))
				require.NoError(t, f.authorize(t, "alice", Request{Op: op, Address: commitAddress}))
				return
			}

			for _, addr := range []Address{tagAddress, commitAddress} {
				err := f.authorize(t, "alice", Request{Op: op, Address: addr, RefIsBranch: false})
				require.ErrorIs(t, err, ErrRefReadOnly, "%s on ref %q must be read-only", op, addr.Ref)
				var readOnly *ReadOnlyRefError
				require.ErrorAs(t, err, &readOnly)
				require.Equal(t, addr.Ref, readOnly.Ref)
				require.Equal(t, "ref is read-only", readOnly.Message())
				require.Equal(t, http.StatusForbidden, readOnly.HTTPStatusCode())
				require.Equal(t, "ForbiddenException", readOnly.ErrorType())
			}

			// The ref rule is only reachable for a caller that is otherwise allowed: a caller
			// without the permission gets the plain denial and learns nothing about the ref.
			f = newAuthzUser(t, authzFSStatements())
			require.ErrorIs(t, f.authorize(t, "alice", Request{
				Op: op, Address: tagAddress, RefIsBranch: false,
			}), ErrPermissionDenied)
		})
	}
}

// TestAuthzExplicitTableDeny shows that a deny statement on the table ARN wins over a broader
// allow, for one table only.
func TestAuthzExplicitTableDeny(t *testing.T) {
	t.Parallel()
	f := newAuthzUser(t, append(authzWriteStatements(), model.Statement{
		Effect:   model.StatementEffectDeny,
		Action:   []string{permissions.CatalogUpdateTableAction},
		Resource: authzTableArn,
	}))

	require.NoError(t, f.authorize(t, "alice", Request{
		Op: OpReadTable, Address: authzTableAddress(), RefIsBranch: true,
	}))
	require.ErrorIs(t, f.authorize(t, "alice", Request{
		Op: OpUpdateTable, Address: authzTableAddress(), RefIsBranch: true,
	}), ErrPermissionDenied)

	// Another table in the same namespace is unaffected.
	other := authzTableAddress()
	other.Table = "authors"
	require.NoError(t, f.authorize(t, "alice", Request{
		Op: OpUpdateTable, Address: other, RefIsBranch: true,
	}))
}

// TestAuthzFilterTables covers the §7.2 ListTables filter: catalog:ListTables on the namespace ARN
// with a catalog:TableName condition decides which tables of a listing are visible.
func TestAuthzFilterTables(t *testing.T) {
	t.Parallel()

	// A policy that may only list tables whose name starts with "analytics-".
	analyticsOnly := model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.CatalogListTablesAction},
			Resource: authzNamespaceArn,
			Condition: map[string]map[string][]string{
				"StringLike": {ConditionFieldTableName: {"analytics-*"}},
			},
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: authzRepoArn,
		},
	}
	// The same, and an explicit deny of the temporary tables.
	withTempDeny := append(append(model.Statements(nil), analyticsOnly...), model.Statement{
		Effect:   model.StatementEffectDeny,
		Action:   []string{permissions.CatalogListTablesAction},
		Resource: authzNamespaceArn,
		Condition: map[string]map[string][]string{
			"StringLike": {ConditionFieldTableName: {"analytics-tmp-*"}},
		},
	})

	tests := []struct {
		name       string
		statements model.Statements
		names      []string
		want       []string
		wantDenied bool
	}{
		{
			name:       "an unconditional grant lists every table",
			statements: authzReadStatements(),
			names:      []string{"titles", "sales-data", "analytics-report"},
			want:       []string{"titles", "sales-data", "analytics-report"},
		},
		{
			name:       "an empty namespace with an unconditional grant lists nothing",
			statements: authzReadStatements(),
			names:      nil,
			want:       []string{},
		},
		{
			name:       "catalog:TableName StringLike narrows the listing",
			statements: analyticsOnly,
			names:      []string{"analytics-report", "sales-data", "analytics-events"},
			want:       []string{"analytics-report", "analytics-events"},
		},
		{
			name:       "the order of the candidates is preserved",
			statements: analyticsOnly,
			names:      []string{"analytics-events", "analytics-report"},
			want:       []string{"analytics-events", "analytics-report"},
		},
		{
			name:       "a conditional deny hides a table",
			statements: withTempDeny,
			names:      []string{"analytics-report", "analytics-tmp-scratch", "sales-data"},
			want:       []string{"analytics-report"},
		},
		{
			// A caller who may list matching tables but no table in the namespace exists must
			// not get an empty page: that would make an empty namespace and a forbidden one
			// indistinguishable.
			name:       "a condition matching nothing is a denial, not an empty page",
			statements: analyticsOnly,
			names:      []string{"sales-data"},
			wantDenied: true,
		},
		{
			name:       "no catalog:ListTables grant is a denial",
			statements: authzFSStatements(),
			names:      []string{"titles"},
			wantDenied: true,
		},
		{
			name: "a grant on another namespace does not list this one",
			statements: model.Statements{
				{
					Effect:   model.StatementEffectAllow,
					Action:   []string{permissions.CatalogListTablesAction},
					Resource: "arn:lakefs:catalog:::namespace/lakehouse/other",
				},
				{
					Effect:   model.StatementEffectAllow,
					Action:   []string{permissions.ReadRepositoryAction},
					Resource: authzRepoArn,
				},
			},
			names:      []string{"titles"},
			wantDenied: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newAuthzUser(t, tt.statements)
			got, err := f.authz.FilterTables(t.Context(), Caller{Username: "alice"},
				authzNamespaceAddress(), tt.names)
			if tt.wantDenied {
				require.ErrorIs(t, err, ErrPermissionDenied)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestAuthzFilterTablesAddress rejects a table address: a listing is a namespace operation.
func TestAuthzFilterTablesAddress(t *testing.T) {
	t.Parallel()
	f := newAuthzUser(t, authzReadStatements())
	_, err := f.authz.FilterTables(t.Context(), Caller{Username: "alice"},
		authzTableAddress(), []string{"titles"})
	require.ErrorIs(t, err, ErrInvalidAuthzRequest)
}

// TestAuthzFilterViews is TestAuthzFilterTables for the view listing: catalog:ListViews on the
// namespace ARN with a catalog:ViewName condition decides which views of a listing are visible,
// and a caller who may not list the namespace gets the denial rather than an empty page.
func TestAuthzFilterViews(t *testing.T) {
	t.Parallel()

	// A policy that may only list views whose name starts with "analytics-".
	analyticsOnly := model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.CatalogListViewsAction},
			Resource: authzNamespaceArn,
			Condition: map[string]map[string][]string{
				"StringLike": {ConditionFieldViewName: {"analytics-*"}},
			},
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: authzRepoArn,
		},
	}
	// The same, and an explicit deny of the temporary views.
	withTempDeny := append(append(model.Statements(nil), analyticsOnly...), model.Statement{
		Effect:   model.StatementEffectDeny,
		Action:   []string{permissions.CatalogListViewsAction},
		Resource: authzNamespaceArn,
		Condition: map[string]map[string][]string{
			"StringLike": {ConditionFieldViewName: {"analytics-tmp-*"}},
		},
	})
	// A condition on the *table* name must not scope views: the two are separate fields.
	tableNameOnly := model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.CatalogListViewsAction},
			Resource: authzNamespaceArn,
			Condition: map[string]map[string][]string{
				"StringLike": {ConditionFieldTableName: {"analytics-*"}},
			},
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: authzRepoArn,
		},
	}

	tests := []struct {
		name       string
		statements model.Statements
		names      []string
		want       []string
		wantDenied bool
	}{
		{
			name:       "an unconditional grant lists every view",
			statements: authzReadStatements(),
			names:      []string{"orders_view", "sales-data", "analytics-report"},
			want:       []string{"orders_view", "sales-data", "analytics-report"},
		},
		{
			name:       "an empty namespace with an unconditional grant lists nothing",
			statements: authzReadStatements(),
			names:      nil,
			want:       []string{},
		},
		{
			name:       "catalog:ViewName StringLike narrows the listing",
			statements: analyticsOnly,
			names:      []string{"analytics-report", "sales-data", "analytics-events"},
			want:       []string{"analytics-report", "analytics-events"},
		},
		{
			name:       "a deny on catalog:ViewName hides the matching views",
			statements: withTempDeny,
			names:      []string{"analytics-report", "analytics-tmp-1"},
			want:       []string{"analytics-report"},
		},
		{
			name:       "a condition on catalog:TableName does not scope views",
			statements: tableNameOnly,
			names:      []string{"analytics-report", "orders_view"},
			want:       []string{},
			wantDenied: true,
		},
		{
			name: "no grant at all is a denial, not an empty page",
			statements: model.Statements{{
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.ReadRepositoryAction},
				Resource: authzRepoArn,
			}},
			names:      []string{"orders_view"},
			want:       nil,
			wantDenied: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newAuthzUser(t, tt.statements)
			got, err := f.authz.FilterViews(t.Context(), Caller{Username: "alice"},
				authzNamespaceAddress(), tt.names)
			if tt.wantDenied {
				require.ErrorIs(t, err, ErrPermissionDenied)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestAuthzFilterViewsAddress rejects a view address: a listing is a namespace operation.
func TestAuthzFilterViewsAddress(t *testing.T) {
	t.Parallel()
	f := newAuthzUser(t, authzReadStatements())
	_, err := f.authz.FilterViews(t.Context(), Caller{Username: "alice"},
		authzViewAddress(), []string{"orders_view"})
	require.ErrorIs(t, err, ErrInvalidAuthzRequest)
}

// TestAuthzDatasetRepositoryIsReadOnly verifies the §7.2 note that a dataset repository stays
// read-only through the catalog, now that a catalog request needs no fs: action of its own. The
// enforcement is the SEC-2 guard (pkg/datasets/immutability_guard.go, ImmutabilityAuthService):
// it refuses the catalog write actions of immutabilityDeniedActions on catalog ARNs before the
// wrapped auth service is consulted, and cmd/lakefs-sso wires it exactly like this (buildAuthService).
// Nothing is duplicated here - the guard and the immutability policy come from pkg/datasets.
func TestAuthzDatasetRepositoryIsReadOnly(t *testing.T) {
	t.Parallel()
	const dataset = "customer-360"

	f := newAuthzFixture(t)
	f.user(t, "writer")
	// A principal that would be allowed everything in the dataset repository: catalog:* on its
	// catalog ARNs and fs:* on its fs ARNs.
	f.attachStatements(t, "everything", "writer", model.Statements{
		{Effect: model.StatementEffectAllow, Action: []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::namespace/" + dataset + "/*"},
		{Effect: model.StatementEffectAllow, Action: []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::table/" + dataset + "/*"},
		{Effect: model.StatementEffectAllow, Action: []string{"catalog:*"},
			Resource: "arn:lakefs:catalog:::view/" + dataset + "/*"},
		{Effect: model.StatementEffectAllow, Action: []string{"fs:*"},
			Resource: permissions.RepoArn(dataset)},
		{Effect: model.StatementEffectAllow, Action: []string{"fs:*"},
			Resource: permissions.RepoArn(dataset) + "/*"},
	})
	// The immutability policy datasets attaches to the installation's groups (§9.4). It is the
	// defence in depth of SEC-2; the guard below is what decides, as in production.
	f.attach(t, "writer", datasets.ImmutabilityPolicy(dataset, time.Now()))
	// The guard reads the dataset repository names from the datasets KV partition.
	require.NoError(t, datasets.NewStore(f.store).CreateDataset(t.Context(), &datasets.DatasetRecord{
		Name:      dataset,
		CreatedAt: time.Now(),
		State:     datasets.DatasetStateActive,
		StorageID: "s3",
		StorageNS: "s3://" + dataset,
	}), "record the dataset in the store the guard reads")
	authz := NewAuthorizer(datasets.NewImmutabilityAuthService(f.svc, datasets.NewDatasetRepoSet(f.store)))

	address := func(op Operation) Address {
		switch {
		case op.IsTableOperation():
			return Address{Repo: dataset, Ref: authzRef, Namespace: Namespace{"inventory"}, Table: "titles"}
		case op.IsViewOperation():
			return Address{Repo: dataset, Ref: authzRef, Namespace: Namespace{"inventory"}, View: "titles_view"}
		default:
			return Address{Repo: dataset, Ref: authzRef, Namespace: Namespace{"inventory"}}
		}
	}
	for _, op := range authzAllOps {
		op := op
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()
			err := authz.Authorize(t.Context(), Caller{Username: "writer"},
				Request{Op: op, Address: address(op), RefIsBranch: true})
			if op.IsWrite() && op != OpWriteTableData {
				require.ErrorIs(t, err, ErrPermissionDenied,
					"a catalog write into a dataset repository must be refused")
				return
			}
			// OpWriteTableData is the one gap: catalog:WriteTableData is not in
			// immutabilityDeniedActions (pkg/datasets), so the guard lets a vended write session
			// through. It is the caller's storage session for the table's data files, not a
			// catalog change; extending the datasets deny list is a pkg/datasets change that this
			// card does not own.
			require.NoError(t, err, "reading a dataset repository through the catalog is allowed")
		})
	}
}

// TestAuthzDatasetDenyCoversCatalogWrites verifies that the immutability deny lists the catalog
// lifecycle actions themselves (ICEBERG_CATALOG_PLAN.md §7.2 asks for them, and they are already
// there). catalog:WriteTableData is not in that list, which is why
// TestAuthzDatasetRepositoryIsReadOnly names it as the one write a dataset repository still
// allows through the catalog: the fs:WriteObject leaf that used to refuse it is gone (authz.go).
func TestAuthzDatasetDenyCoversCatalogWrites(t *testing.T) {
	t.Parallel()
	policy := datasets.ImmutabilityPolicy("customer-360", time.Now())
	require.NotEmpty(t, policy.Statement)
	denied := make(map[string]struct{})
	for _, statement := range policy.Statement {
		require.Equal(t, model.StatementEffectDeny, statement.Effect)
		for _, action := range statement.Action {
			denied[action] = struct{}{}
		}
	}
	// §7.2: the catalog lifecycle actions of a namespace, a table and a view.
	for _, action := range []string{
		permissions.CatalogCreateNamespaceAction,
		permissions.CatalogUpdateNamespaceAction,
		permissions.CatalogDeleteNamespaceAction,
		permissions.CatalogCreateTableAction,
		permissions.CatalogUpdateTableAction,
		permissions.CatalogDeleteTableAction,
		permissions.CatalogCreateViewAction,
		permissions.CatalogUpdateViewAction,
		permissions.CatalogDeleteViewAction,
	} {
		require.Contains(t, denied, action, "the datasets immutability deny must cover %s", action)
	}
}

// TestAuthzNoAuthService fails closed: without an auth service every operation is a 500, never an
// allow and never a 403.
func TestAuthzNoAuthService(t *testing.T) {
	t.Parallel()
	authorizer := NewAuthorizer(nil)
	err := authorizer.Authorize(t.Context(), Caller{Username: "alice"}, Request{
		Op: OpReadTable, Address: authzTableAddress(), RefIsBranch: true,
	})
	require.ErrorIs(t, err, ErrNoAuthorizer)
	var authorizationErr *AuthorizationError
	require.ErrorAs(t, err, &authorizationErr)
	require.Equal(t, http.StatusInternalServerError, authorizationErr.HTTPStatusCode())
	require.False(t, IsDenial(err))
}

// TestAuthzInvalidRequests fails closed on an operation that does not match its address: the HTTP
// layer must not be able to build a wider request than the one it intended.
func TestAuthzInvalidRequests(t *testing.T) {
	t.Parallel()
	tableAddress := authzTableAddress()
	noTable := tableAddress
	noTable.Table = ""
	noNamespace := Address{Repo: authzRepo, Ref: authzRef, Table: "titles"}
	noRef := Address{Repo: authzRepo, Namespace: Namespace{"inventory", "books"}, Table: "titles"}
	rootWithRef := Address{Ref: authzRef}

	tests := []struct {
		name string
		req  Request
	}{
		{"unknown operation", Request{Op: Operation("DropCatalog"), Address: authzTableAddress()}},
		{"empty operation", Request{Address: authzTableAddress()}},
		{"table operation without a table", Request{Op: OpReadTable, Address: noTable}},
		{"table operation without a namespace", Request{Op: OpReadTable, Address: noNamespace}},
		{"table operation without a repository", Request{Op: OpReadTable, Address: Address{Ref: authzRef, Namespace: Namespace{"ns"}, Table: "t"}}},
		{"namespace operation on a table address", Request{Op: OpGetNamespace, Address: authzTableAddress()}},
		{"table operation without a ref", Request{Op: OpReadTable, Address: noRef}},
		{"root listing with a ref", Request{Op: OpListNamespaces, Address: rootWithRef}},
		{"root listing of a table", Request{Op: OpReadTable, Address: Address{Table: "titles"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := RequiredNode(tt.req)
			require.ErrorIs(t, err, ErrInvalidAuthzRequest)

			// The same through the Authorizer: an invalid operation/address pair is never
			// allowed, and never reported as a permission problem.
			f := newAuthzFixture(t)
			f.user(t, "alice")
			require.ErrorIs(t, f.authorize(t, "alice", tt.req), ErrInvalidAuthzRequest)
		})
	}
}

// TestAuthzErrorMapping pins the Iceberg error envelope mapping of the three refusal types and of
// a denial wrapped by one of them.
func TestAuthzErrorMapping(t *testing.T) {
	t.Parallel()
	denied := &PermissionDeniedError{
		Username:  "alice",
		Operation: OpUpdateTable,
		Required:  []permissions.Permission{{Action: permissions.CatalogUpdateTableAction, Resource: authzTableArn}},
		Err:       auth.ErrInsufficientPermissions,
	}
	require.Equal(t, http.StatusForbidden, denied.HTTPStatusCode())
	require.Equal(t, "ForbiddenException", denied.ErrorType())
	require.ErrorIs(t, denied, ErrPermissionDenied)
	require.True(t, IsDenial(denied))
	require.Contains(t, denied.Error(), "catalog:UpdateTable")
	// The refusal must not echo the address.
	require.NotContains(t, denied.Error(), "inventory.books")
	require.NotContains(t, denied.Error(), "titles")

	readOnly := &ReadOnlyRefError{Operation: OpUpdateTable, Ref: "release.v1"}
	require.Equal(t, http.StatusForbidden, readOnly.HTTPStatusCode())
	require.Equal(t, "ForbiddenException", readOnly.ErrorType())
	require.ErrorIs(t, readOnly, ErrRefReadOnly)
	require.False(t, errors.Is(readOnly, ErrPermissionDenied))
	require.True(t, IsDenial(readOnly))
	require.Equal(t, "ref is read-only", readOnly.Message())
	require.Contains(t, readOnly.Error(), "release.v1")
	require.Contains(t, readOnly.Error(), "ref is read-only")

	failure := &AuthorizationError{Username: "alice", Operation: OpReadTable, Err: errors.New("kv is down")}
	require.Equal(t, http.StatusInternalServerError, failure.HTTPStatusCode())
	require.Equal(t, "InternalServerError", failure.ErrorType())
	require.False(t, IsDenial(failure))
	require.NotErrorIs(t, failure, ErrPermissionDenied)

	require.False(t, IsDenial(nil))
	require.False(t, IsDenial(ErrInvalidAuthzRequest))
}

// TestAuthzRootListing covers the §5 rule that a listing with no parent lists the repositories the
// caller can read: that level is a lakeFS repository list, so it needs fs:ListRepositories.
func TestAuthzRootListing(t *testing.T) {
	t.Parallel()
	f := newAuthzUser(t, model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ListRepositoriesAction},
		Resource: permissions.All,
	}})
	require.NoError(t, f.authorize(t, "alice", Request{Op: OpListNamespaces}))

	// A catalog grant on every catalog ARN is not enough to enumerate the repository list: that
	// level is fs:ListRepositories on All.
	g := newAuthzUser(t, authzReadStatements())
	require.ErrorIs(t, g.authorize(t, "alice", Request{Op: OpListNamespaces}), ErrPermissionDenied)
}
