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
	authzRepoArn      = "arn:lakefs:fs:::repository/lakehouse"
	// §7.2: writes additionally need fs:WriteObject on the reserved _lakefs_iceberg/ area.
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
}

// authzTableAddress is the address of a table; authzNamespaceAddress the address of the namespace
// that contains it.
func authzTableAddress() Address {
	return Address{Repo: authzRepo, Ref: authzRef, Namespace: Namespace{"inventory", "books"}, Table: "titles"}
}

func authzNamespaceAddress() Address {
	return Address{Repo: authzRepo, Ref: authzRef, Namespace: Namespace{"inventory", "books"}}
}

// authzAddressFor returns an address op is defined on: a table address for the table operations,
// a namespace address for everything else.
func authzAddressFor(op Operation) Address {
	if op.IsTableOperation() {
		return authzTableAddress()
	}
	return authzNamespaceAddress()
}

// authzReadStatements grants every catalog read action on every catalog ARN, plus the
// fs:ReadRepository every request needs (§7.2).
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
			},
			Resource: "arn:lakefs:catalog:::*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: authzRepoArn,
		},
	}
}

// authzWriteStatements is authzReadStatements plus the write half of §7.2: the catalog write
// actions and fs:WriteObject on the reserved _lakefs_iceberg/ area.
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
			},
			Resource: "arn:lakefs:catalog:::*",
		},
		model.Statement{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.WriteObjectAction},
			Resource: authzWriteObjectArn,
		},
	)
}

// authzFSStatements grants only the fs half of §7.2. A request authorized with it is refused on
// the catalog action, which is the main deny case of the matrix.
func authzFSStatements() model.Statements {
	return model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: authzRepoArn,
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.WriteObjectAction},
			Resource: authzWriteObjectArn,
		},
	}
}

// authzCatalogOnlyStatements grants only the catalog half of §7.2, so a request is refused on
// fs:ReadRepository.
func authzCatalogOnlyStatements() model.Statements {
	return model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"catalog:*"},
		Resource: "arn:lakefs:catalog:::*",
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
// in-memory KV store, plus the Authorizer built on top of it.
type authzFixture struct {
	authz *Authorizer
	svc   *acl.AuthService
}

func newAuthzFixture(t *testing.T) *authzFixture {
	t.Helper()
	store := kvtest.GetStore(t.Context(), t)
	svc := acl.NewAuthService(store, crypt.NewSecretStore([]byte("iceberg-authz-test-secret")),
		authparams.ServiceCache{Enabled: false})
	return &authzFixture{svc: svc, authz: NewAuthorizer(svc)}
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

// TestAuthzRequiredNode asserts the action and resource columns of the §7.2 matrix, and that the
// two additional fs requirements are joined with And (not Or) so that a deny on any of them
// refuses the request.
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
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
			},
		},
		{
			name: "list namespaces at the repository level is authorized on the repository namespace ARN",
			req:  Request{Op: OpListNamespaces, Address: Address{Repo: authzRepo}},
			want: []permissions.Permission{
				{Action: "catalog:ListNamespaces", Resource: "arn:lakefs:catalog:::namespace/lakehouse/"},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
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
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
			},
		},
		{
			name: "create namespace needs the write area",
			req:  Request{Op: OpCreateNamespace, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:CreateNamespace", Resource: authzNamespaceArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
				{Action: "fs:WriteObject", Resource: authzWriteObjectArn},
			},
		},
		{
			name: "update namespace needs the write area",
			req:  Request{Op: OpUpdateNamespace, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:UpdateNamespace", Resource: authzNamespaceArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
				{Action: "fs:WriteObject", Resource: authzWriteObjectArn},
			},
		},
		{
			name: "delete namespace needs the write area",
			req:  Request{Op: OpDeleteNamespace, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:DeleteNamespace", Resource: authzNamespaceArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
				{Action: "fs:WriteObject", Resource: authzWriteObjectArn},
			},
		},
		{
			name: "list tables is authorized on the namespace ARN",
			req:  Request{Op: OpListTables, Address: authzNamespaceAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ListTables", Resource: authzNamespaceArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
			},
		},
		{
			name: "read table is authorized on the table ARN",
			req:  Request{Op: OpReadTable, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ReadTable", Resource: authzTableArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
			},
		},
		{
			name: "create table needs the write area",
			req:  Request{Op: OpCreateTable, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:CreateTable", Resource: authzTableArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
				{Action: "fs:WriteObject", Resource: authzWriteObjectArn},
			},
		},
		{
			name: "update table needs the write area",
			req:  Request{Op: OpUpdateTable, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:UpdateTable", Resource: authzTableArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
				{Action: "fs:WriteObject", Resource: authzWriteObjectArn},
			},
		},
		{
			name: "delete table needs the write area",
			req:  Request{Op: OpDeleteTable, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:DeleteTable", Resource: authzTableArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
				{Action: "fs:WriteObject", Resource: authzWriteObjectArn},
			},
		},
		{
			name: "read table data",
			req:  Request{Op: OpReadTableData, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:ReadTableData", Resource: authzTableArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
			},
		},
		{
			name: "write table data needs the write area",
			req:  Request{Op: OpWriteTableData, Address: authzTableAddress()},
			want: []permissions.Permission{
				{Action: "catalog:WriteTableData", Resource: authzTableArn},
				{Action: "fs:ReadRepository", Resource: authzRepoArn},
				{Action: "fs:WriteObject", Resource: authzWriteObjectArn},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node, err := RequiredNode(tt.req)
			require.NoError(t, err)
			// And, never Or: a deny on any leaf must refuse the whole request.
			require.Equal(t, permissions.NodeTypeAnd, node.Type)
			require.Equal(t, tt.want, requiredPermissions(node))
		})
	}
}

// TestAuthzMatrix is the §7.2 matrix: every operation is allowed by the grants §7.2 asks for, and
// refused when any one of them is missing.
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
			// The refusal must name the missing catalog action.
			action, err := op.Action()
			require.NoError(t, err)
			require.Contains(t, joinActions(denied.Required), action)
		})

		t.Run(op.String()+"/no fs:ReadRepository grant", func(t *testing.T) {
			t.Parallel()
			f := newAuthzUser(t, authzCatalogOnlyStatements())
			require.ErrorIs(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}), ErrPermissionDenied)
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

		t.Run(op.String()+"/no fs:WriteObject grant", func(t *testing.T) {
			t.Parallel()
			// Everything except the reserved-area write.
			f := newAuthzUser(t, append(authzCatalogOnlyStatements(), model.Statement{
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.ReadRepositoryAction},
				Resource: authzRepoArn,
			}))
			require.ErrorIs(t, f.authorize(t, "alice", Request{
				Op: op, Address: authzAddressFor(op), RefIsBranch: true,
			}), ErrPermissionDenied)
		})

		t.Run(op.String()+"/reserved area denied", func(t *testing.T) {
			t.Parallel()
			// A deny of fs:WriteObject on the reserved prefix refuses the whole write, which is
			// how the datasets guard and the _lakefs_iceberg guard reach the catalog (§7.2).
			f := newAuthzUser(t, append(authzWriteStatements(), model.Statement{
				Effect:   model.StatementEffectDeny,
				Action:   []string{permissions.WriteObjectAction},
				Resource: authzWriteObjectArn,
			}))
			err := f.authorize(t, "alice", Request{Op: op, Address: authzAddressFor(op), RefIsBranch: true})
			require.ErrorIs(t, err, ErrPermissionDenied)
			var denied *PermissionDeniedError
			require.ErrorAs(t, err, &denied)
			require.Contains(t, denied.Required, permissions.Permission{
				Action: permissions.WriteObjectAction, Resource: authzWriteObjectArn,
			}, "the refusal must name the denied reserved-area write")
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

// TestAuthzDatasetRepositoryIsReadOnly verifies the §7.2 note that the datasets guard already
// makes a dataset repository read-only through the catalog, without a catalog-shaped deny:
// the dataset-<name>-immutable policy denies fs:WriteObject, and fs:WriteObject is part of every
// catalog write (§7.2). Nothing is duplicated here - the policy comes from pkg/datasets.
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
		{Effect: model.StatementEffectAllow, Action: []string{"fs:*"},
			Resource: permissions.RepoArn(dataset)},
		{Effect: model.StatementEffectAllow, Action: []string{"fs:*"},
			Resource: permissions.RepoArn(dataset) + "/*"},
	})
	// The immutability guard datasets attaches to the installation's groups (§9.4).
	f.attach(t, "writer", datasets.ImmutabilityPolicy(dataset, time.Now()))

	address := func(op Operation) Address {
		if op.IsTableOperation() {
			return Address{Repo: dataset, Ref: authzRef, Namespace: Namespace{"inventory"}, Table: "titles"}
		}
		return Address{Repo: dataset, Ref: authzRef, Namespace: Namespace{"inventory"}}
	}
	for _, op := range authzAllOps {
		op := op
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()
			err := f.authorize(t, "writer", Request{Op: op, Address: address(op), RefIsBranch: true})
			if op.IsWrite() {
				require.ErrorIs(t, err, ErrPermissionDenied,
					"a catalog write into a dataset repository must be refused")
				return
			}
			require.NoError(t, err, "reading a dataset repository through the catalog is allowed")
		})
	}
}

// TestAuthzDatasetDenyCoversCatalogWrites also verifies that the immutability deny lists the
// catalog lifecycle actions themselves (ICEBERG_CATALOG_PLAN.md §7.2 asks to add them; this task
// verifies they are already there instead of duplicating them). The data-plane write action
// catalog:WriteTableData is not in that list - it was added after the datasets deny list was
// written - and does not need to be: a data-plane write also needs fs:WriteObject, which the same
// policy denies on repository/<name>/*, so TestAuthzDatasetRepositoryIsReadOnly proves the refusal
// end to end.
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
	// §7.2: the catalog lifecycle actions of a namespace and of a table.
	for _, action := range []string{
		permissions.CatalogCreateNamespaceAction,
		permissions.CatalogUpdateNamespaceAction,
		permissions.CatalogDeleteNamespaceAction,
		permissions.CatalogCreateTableAction,
		permissions.CatalogUpdateTableAction,
		permissions.CatalogDeleteTableAction,
	} {
		require.Contains(t, denied, action, "the datasets immutability deny must cover %s", action)
	}
	require.Contains(t, denied, permissions.WriteObjectAction,
		"the datasets immutability deny must cover the catalog's fs:WriteObject requirement")
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

	// fs:ReadRepository on one repository is not enough to enumerate the repository list.
	g := newAuthzUser(t, authzReadStatements())
	require.ErrorIs(t, g.authorize(t, "alice", Request{Op: OpListNamespaces}), ErrPermissionDenied)
}
