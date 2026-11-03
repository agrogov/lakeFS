// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// authz_test.go covers the authorization building blocks of design §9.1-§9.4: the ARN helpers,
// the action constants, the two per-dataset policies, and the attach-time source check against a
// real acl.AuthService on an in-memory KV store (§15.1).
package datasets

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// authzFixture is an acl.AuthService (the simplified-ACL implementation the lakefs-sso binary
// uses) on an in-memory KV store, plus the Authorizer built on top of it.
type authzFixture struct {
	svc   *acl.AuthService
	authz *Authorizer
}

func newAuthzFixture(t *testing.T) *authzFixture {
	t.Helper()
	// kvtest.GetStore registers the mem driver, so no blank import is needed here.
	store := kvtest.GetStore(t.Context(), t)
	svc := acl.NewAuthService(store, crypt.NewSecretStore([]byte("datasets-authz-test-secret")), authparams.ServiceCache{Enabled: false})
	return &authzFixture{
		svc:   svc,
		authz: NewAuthorizer(svc),
	}
}

// user creates a user.
func (f *authzFixture) user(t *testing.T, username string) {
	t.Helper()
	_, err := f.svc.CreateUser(t.Context(), &model.User{Username: username, CreatedAt: time.Now()})
	require.NoErrorf(t, err, "create user %s", username)
}

// group creates a group.
func (f *authzFixture) group(t *testing.T, name string) {
	t.Helper()
	_, err := f.svc.CreateGroup(t.Context(), &model.Group{DisplayName: name, CreatedAt: time.Now()})
	require.NoErrorf(t, err, "create group %s", name)
}

// policy creates a policy.
func (f *authzFixture) policy(t *testing.T, name string, statements model.Statements) {
	t.Helper()
	err := f.svc.WritePolicy(t.Context(), &model.Policy{
		DisplayName: name,
		CreatedAt:   time.Now(),
		Statement:   statements,
	}, false)
	require.NoErrorf(t, err, "write policy %s", name)
}

// attachToUser attaches a policy to a user.
func (f *authzFixture) attachToUser(t *testing.T, policyName, username string) {
	t.Helper()
	require.NoErrorf(t, f.svc.AttachPolicyToUser(t.Context(), policyName, username),
		"attach policy %s to user %s", policyName, username)
}

// attachToGroup attaches a policy to a group.
func (f *authzFixture) attachToGroup(t *testing.T, policyName, groupName string) {
	t.Helper()
	require.NoErrorf(t, f.svc.AttachPolicyToGroup(t.Context(), policyName, groupName),
		"attach policy %s to group %s", policyName, groupName)
}

// addToGroup adds a user to a group.
func (f *authzFixture) addToGroup(t *testing.T, username, groupName string) {
	t.Helper()
	require.NoErrorf(t, f.svc.AddUserToGroup(t.Context(), username, groupName),
		"add user %s to group %s", username, groupName)
}

func TestDatasetArns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"dataset", DatasetArn("customer-360"), "arn:lakefs:dataset:::dataset/customer-360"},
		{"dataset wildcard", DatasetArn("*"), "arn:lakefs:dataset:::dataset/*"},
		{"dataset empty name", DatasetArn(""), "arn:lakefs:dataset:::dataset/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.got)
			// The helpers must produce ARNs the policy validator accepts, and the dataset
			// service part must be the singular one of the public RBAC documentation.
			require.NoError(t, model.ValidateArn(tt.got), "policy validator must accept %q", tt.got)
			require.True(t, strings.HasPrefix(tt.got, "arn:lakefs:dataset:::dataset/"))
		})
	}
}

func TestActionConstantsAreValid(t *testing.T) {
	t.Parallel()
	// §9.1. The values are the contract: policies written by hand, by the UI or by the service
	// all use these strings.
	actions := map[string]string{
		ActionListDatasets:      "dataset:ListDatasets",
		ActionCreateDataset:     "dataset:CreateDataset",
		ActionReadDataset:       "dataset:ReadDataset",
		ActionUpdateDataset:     "dataset:UpdateDataset",
		ActionDeleteDataset:     "dataset:DeleteDataset",
		ActionManageAccess:      "dataset:ManageAccess",
		ActionListMetadataKeys:  "dataset:ListMetadataKeys",
		ActionReadMetadataKey:   "dataset:ReadMetadataKey",
		ActionCreateMetadataKey: "dataset:CreateMetadataKey",
		ActionUpdateMetadataKey: "dataset:UpdateMetadataKey",
		ActionDeleteMetadataKey: "dataset:DeleteMetadataKey",
	}
	for got, want := range actions {
		require.Equal(t, want, got)
		// The "dataset" serviceSet entry exists in pkg/permissions/actions.go, so these
		// actions validate without any actions.gen.go codegen.
		require.NoErrorf(t, permissions.IsValidAction(got), "action %q must be valid", got)
		require.NoErrorf(t, model.ValidateActionName(got), "policy validator must accept %q", got)
	}
}

func TestConsumerReadPolicy(t *testing.T) {
	t.Parallel()
	const dataset = "customer-360"
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	policy := ConsumerReadPolicy(dataset, ts)

	require.Equal(t, "dataset-customer-360-read", policy.DisplayName)
	require.Equal(t, ConsumerReadPolicyName(dataset), policy.DisplayName)
	require.Equal(t, ts, policy.CreatedAt)
	require.NoError(t, acl.ValidatePolicy(policy), "the read policy must be a valid lakeFS policy")

	// Byte-for-byte the statements of §9.2.
	require.Len(t, policy.Statement, 4)
	require.Equal(t, model.Statements{
		{
			Effect: model.StatementEffectAllow,
			Action: []string{
				"fs:ReadRepository", "fs:ListObjects", "fs:ReadObject",
				"fs:ReadCommit", "fs:ListCommits", "fs:ReadBranch",
				"fs:ListBranches", "fs:ReadTag", "fs:ListTags",
			},
			Resource: "arn:lakefs:fs:::repository/customer-360",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:ReadObject", "fs:ListObjects"},
			Resource: "arn:lakefs:fs:::repository/customer-360/object/*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"dataset:ReadDataset"},
			Resource: "arn:lakefs:dataset:::dataset/customer-360",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:ListRepositories", "dataset:ListDatasets"},
			Resource: "*",
		},
	}, policy.Statement)

	// A consumer grant must not leak into the source repositories, and it must not grant any
	// write: §9.2 is a read policy.
	for _, stmt := range policy.Statement {
		require.Equal(t, model.StatementEffectAllow, stmt.Effect)
		for _, action := range stmt.Action {
			require.NotContains(t, []string{
				"fs:WriteObject", "fs:DeleteObject", "fs:CreateCommit", "fs:DeleteRepository",
			}, action, "the consumer read policy must not grant writes")
			require.Falsef(t, strings.HasPrefix(action, "dataset:Update") ||
				strings.HasPrefix(action, "dataset:Create") ||
				strings.HasPrefix(action, "dataset:Delete") ||
				strings.HasPrefix(action, "dataset:Manage"), "unexpected control-plane action %q", action)
		}
		require.NotContains(t, stmt.Resource, "repository/ref-data",
			"a consumer grant is on the dataset repository only (§9.2)")
	}
}

func TestImmutabilityPolicy(t *testing.T) {
	t.Parallel()
	const dataset = "customer-360"
	ts := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	policy := ImmutabilityPolicy(dataset, ts)

	require.Equal(t, "dataset-customer-360-immutable", policy.DisplayName)
	require.Equal(t, ImmutabilityPolicyName(dataset), policy.DisplayName)
	require.Equal(t, ts, policy.CreatedAt)
	require.NoError(t, acl.ValidatePolicy(policy), "the immutability policy must be a valid lakeFS policy")

	// §9.4: the full deny action list.
	wantActions := []string{
		"fs:WriteObject", "fs:DeleteObject", "fs:CreateCommit", "fs:CreateBranch", "fs:DeleteBranch",
		"fs:RevertBranch", "fs:CreateTag", "fs:DeleteTag", "fs:UpdateRepository", "fs:DeleteRepository",
		"fs:ImportFromStorage", "fs:AttachStorageNamespace", "branches:SetBranchProtectionRules",
		"retention:SetGarbageCollectionRules", "fs:ImportCancel", "pr:WritePullRequest",
		"catalog:CreateNamespace", "catalog:UpdateNamespace", "catalog:DeleteNamespace",
		"catalog:CreateTable", "catalog:UpdateTable", "catalog:DeleteTable",
		"catalog:CreateView", "catalog:UpdateView", "catalog:DeleteView", "ci:WriteAction",
	}
	require.Len(t, policy.Statement, 2, "exactly two statements: repository and repository/*")
	for i, stmt := range policy.Statement {
		require.Equalf(t, model.StatementEffectDeny, stmt.Effect, "statement %d must deny", i)
		require.Equalf(t, wantActions, stmt.Action, "statement %d action list", i)
	}

	// §9.4 limit: two exact resources, NOT repository/<name>*. A guard on
	// repository/customer-360* would also lock an unrelated repository customer-360-archive.
	require.Equal(t, "arn:lakefs:fs:::repository/customer-360", policy.Statement[0].Resource)
	require.Equal(t, "arn:lakefs:fs:::repository/customer-360/*", policy.Statement[1].Resource)
	for _, stmt := range policy.Statement {
		require.NotEqual(t, "arn:lakefs:fs:::repository/customer-360*", stmt.Resource,
			"the guard must not use a repository/<name>* resource")
	}
}

// TestImmutabilityGuardDoesNotLeakToSiblingRepositories evaluates the guard through the real
// policy engine: the deny must bite on the dataset repository and on its objects, and must not
// bite on a repository whose name merely starts with the dataset name (§9.4).
func TestImmutabilityGuardDoesNotLeakToSiblingRepositories(t *testing.T) {
	t.Parallel()
	const dataset = "customer-360"
	f := newAuthzFixture(t)
	ctx := t.Context()
	f.user(t, "writer")
	f.policy(t, "allow-fs-all", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"fs:*"},
		Resource: permissions.All,
	}})
	f.attachToUser(t, "allow-fs-all", "writer")

	caller := Caller{Username: "writer"}

	// Without the guard the writer may write anywhere: this is the fs:WriteObject + force=true
	// bypass the guard exists for (§9.4, §7.3).
	require.NoError(t, f.authz.Check(ctx, caller, permissions.Node{
		Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn(dataset, "customers/p0.parquet")},
	}, ""))

	immutable := ImmutabilityPolicy(dataset, time.Now())
	require.NoError(t, f.svc.WritePolicy(ctx, immutable, false))
	f.attachToUser(t, immutable.DisplayName, "writer")

	denied := f.authz.Check(ctx, caller, permissions.Node{
		Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn(dataset, "customers/p0.parquet")},
	}, "")
	require.Error(t, denied, "an object write on the dataset repository must be denied")
	require.True(t, errors.Is(denied, ErrPermissionDenied))
	require.Equal(t, http.StatusForbidden, asStatus(denied))

	var permErr *PermissionError
	require.ErrorAs(t, denied, &permErr)
	require.Contains(t, denied.Error(), "denied permission", "the deny statement, not a missing allow, must be the reason")

	denied = f.authz.Check(ctx, caller, permissions.Node{
		Permission: permissions.Permission{Action: permissions.UpdateRepositoryAction, Resource: permissions.RepoArn(dataset)},
	}, "")
	require.Error(t, denied, "a repository update must be denied")

	// The sibling repository is not covered by the guard: this is why §9.4 insists on the two
	// exact resources instead of repository/<name>*.
	require.NoError(t, f.authz.Check(ctx, caller, permissions.Node{
		Permission: permissions.Permission{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn(dataset+"-archive", "customers/p0.parquet")},
	}, ""))
}

func TestImmutabilityGuardGroups(t *testing.T) {
	t.Parallel()
	groups := ImmutabilityGuardGroups([]string{"DatasetAdmins", "Readers", ""})
	// Base groups of both templates (§9.4), deduplicated, then the extra groups in order.
	require.Equal(t, []string{
		"Admins", "Supers", "Writers", "Readers", "SuperUsers", "Developers", "Viewers", "DatasetAdmins",
	}, groups)
	require.Equal(t, []string{"Admins", "Supers", "Writers", "Readers", "SuperUsers", "Developers", "Viewers"},
		ImmutabilityGuardGroups(nil), "no extra groups")
}

func TestSourceReadNode(t *testing.T) {
	t.Parallel()
	commitRef := SourceRef{Type: RefTypeCommit, ID: "30f9eb1c"}

	tests := []struct {
		name            string
		item            DataItem
		wantObject      string
		wantPermissions []permissions.Permission
	}{
		{
			name:       "object",
			item:       DataItem{Repository: "ref-data", Ref: commitRef, Type: ItemTypeObject, Object: &PathSource{Path: "geo/countries.csv"}},
			wantObject: "arn:lakefs:fs:::repository/ref-data/object/geo/countries.csv",
		},
		{
			name:       "prefix",
			item:       DataItem{Repository: "lakehouse-repo", Ref: commitRef, Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/customers/"}},
			wantObject: "arn:lakefs:fs:::repository/lakehouse-repo/object/data/customers/*",
		},
		{
			name:       "prefix of the whole repository",
			item:       DataItem{Repository: "lakehouse-repo", Ref: commitRef, Type: ItemTypePrefix, Prefix: &PathSource{Path: ""}},
			wantObject: "arn:lakefs:fs:::repository/lakehouse-repo/object/*",
		},
		{
			// §9 of the Iceberg plan: a table is authorized on the catalog ARNs the catalog
			// itself checks, not on an fs object glob.
			name:       "table",
			item:       DataItem{Repository: "lakehouse-repo", Ref: commitRef, Type: ItemTypeTable, Table: &TableSrc{Namespace: "analytics", Name: "orders"}},
			wantObject: "arn:lakefs:fs:::repository/lakehouse-repo/object/*",
			wantPermissions: []permissions.Permission{
				{Action: "catalog:ReadTable", Resource: "arn:lakefs:catalog:::table/lakehouse-repo/analytics/orders"},
				{Action: "catalog:ReadTableData", Resource: "arn:lakefs:catalog:::table/lakehouse-repo/analytics/orders"},
			},
		},
		{
			// A multi-level namespace is dotted in a definition, which is the form the
			// catalog ARN carries, so the two builds agree without a conversion.
			name:       "table in a multi-level namespace",
			item:       DataItem{Repository: "lakehouse-repo", Ref: commitRef, Type: ItemTypeTable, Table: &TableSrc{Namespace: "analytics.sales", Name: "orders"}},
			wantObject: "arn:lakefs:fs:::repository/lakehouse-repo/object/*",
			wantPermissions: []permissions.Permission{
				{Action: "catalog:ReadTable", Resource: "arn:lakefs:catalog:::table/lakehouse-repo/analytics.sales/orders"},
				{Action: "catalog:ReadTableData", Resource: "arn:lakefs:catalog:::table/lakehouse-repo/analytics.sales/orders"},
			},
		},
		{
			// A table item without its descriptor cannot be named, so it keeps the
			// whole-repository object requirement: fail closed, never an unnameable grant.
			name:       "table without a descriptor falls back to the whole repository",
			item:       DataItem{Repository: "lakehouse-repo", Ref: commitRef, Type: ItemTypeTable},
			wantObject: "arn:lakefs:fs:::repository/lakehouse-repo/object/*",
		},
		{
			// A namespace has no single catalog ARN: it covers every table below it, so the
			// object requirement stays repository-wide (stricter, never wider).
			name:       "namespace falls back to the whole repository",
			item:       DataItem{Repository: "lakehouse-repo", Ref: commitRef, Type: ItemTypeNamespace, Namespace: ptr("analytics")},
			wantObject: "arn:lakefs:fs:::repository/lakehouse-repo/object/*",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node := SourceReadNode(tt.item)
			want := tt.wantPermissions
			if want == nil {
				want = []permissions.Permission{
					{Action: "fs:ListObjects", Resource: "arn:lakefs:fs:::repository/" + tt.item.Repository},
					{Action: "fs:ReadObject", Resource: tt.wantObject},
				}
			}
			require.Equal(t, permissions.NodeTypeAnd, node.Type)
			require.Equal(t, want, requiredPermissions(node))
			require.Equal(t, tt.wantObject, SourceObjectArn(tt.item))
		})
	}
}

// TestSourceTableArn pins the table ARN SourceReadNode authorizes on, including the cases where
// it refuses to name one: the descriptor is missing, or a part of the catalog path is empty. An
// item the authorizer cannot name keeps the repository-wide requirement instead.
func TestSourceTableArn(t *testing.T) {
	t.Parallel()
	repo := "lakehouse-repo"

	tests := []struct {
		name string
		item DataItem
		want string
		ok   bool
	}{
		{
			name: "table",
			item: DataItem{Repository: repo, Type: ItemTypeTable, Table: &TableSrc{Namespace: "analytics", Name: "orders"}},
			want: "arn:lakefs:catalog:::table/lakehouse-repo/analytics/orders",
			ok:   true,
		},
		{
			name: "multi-level namespace",
			item: DataItem{Repository: repo, Type: ItemTypeTable, Table: &TableSrc{Namespace: "analytics.sales", Name: "orders"}},
			want: "arn:lakefs:catalog:::table/lakehouse-repo/analytics.sales/orders",
			ok:   true,
		},
		{name: "not a table item", item: DataItem{Repository: repo, Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"}}},
		{name: "missing descriptor", item: DataItem{Repository: repo, Type: ItemTypeTable}},
		{name: "empty namespace", item: DataItem{Repository: repo, Type: ItemTypeTable, Table: &TableSrc{Name: "orders"}}},
		{name: "empty table name", item: DataItem{Repository: repo, Type: ItemTypeTable, Table: &TableSrc{Namespace: "analytics"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := SourceTableArn(tt.item)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSourceItemName(t *testing.T) {
	t.Parallel()
	item := DataItem{
		Target:     "customers",
		Repository: "lakehouse-repo",
		Ref:        SourceRef{Type: RefTypeCommit, ID: "30f9eb1c"},
		Type:       ItemTypePrefix,
		Prefix:     &PathSource{Path: "data/customers/"},
	}
	require.Equal(t, `data[2] "customers" (prefix lakehouse-repo@30f9eb1c:data/customers/)`, SourceItemName(2, item))
	// A missing descriptor must not panic: validation reports it, authorization still names it.
	require.Equal(t, "data[0] (object src@:)", SourceItemName(0, DataItem{Repository: "src", Type: ItemTypeObject}))
}

// TestAuthorizerSourceCheckMatrix is the source-check matrix of §9.3 / §15.1: a creator must be
// able to read every source it attaches, and the denial names the offending item.
func TestAuthorizerSourceCheckMatrix(t *testing.T) {
	t.Parallel()
	const (
		creator = "creator"
		src     = "src-repo"
	)
	prefixItem := DataItem{
		Target:     "customers",
		Repository: src,
		Ref:        SourceRef{Type: RefTypeCommit, ID: "aaa"},
		Type:       ItemTypePrefix,
		Prefix:     &PathSource{Path: "data/customers/"},
	}
	objectItem := DataItem{
		Target:     "reference/countries.csv",
		Repository: src,
		Ref:        SourceRef{Type: RefTypeCommit, ID: "bbb"},
		Type:       ItemTypeObject,
		Object:     &PathSource{Path: "geo/countries.csv"},
	}

	// DOC-15: the documented grant - fs:ListObjects on the repository, fs:ReadObject on the path.
	repoRead := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ListObjectsAction},
		Resource: permissions.RepoArn(src),
	}
	prefixRead := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ReadObjectAction},
		Resource: permissions.ObjectArn(src, "data/*"),
	}
	objectRead := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ReadObjectAction},
		Resource: permissions.ObjectArn(src, "geo/countries.csv"),
	}

	tests := []struct {
		name string
		// statements are attached to the creator as the "creator-grants" policy. When empty the
		// creator gets no policy at all.
		statements model.Statements
		items      []DataItem
		wantErr    bool
		// wantItem is the item the error must name.
		wantItem string
		// wantStatus is the HTTP status the control plane reports (§10.4).
		wantStatus int
	}{
		{
			name:       "no grants at all",
			statements: nil,
			items:      []DataItem{prefixItem},
			wantErr:    true,
			wantItem:   `data[0] "customers"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "list on the repository only",
			statements: model.Statements{repoRead},
			items:      []DataItem{prefixItem},
			wantErr:    true,
			wantItem:   `data[0] "customers"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "repository read instead of list and object read",
			statements: model.Statements{{Effect: model.StatementEffectAllow, Action: []string{permissions.ReadRepositoryAction}, Resource: permissions.RepoArn(src)}},
			items:      []DataItem{prefixItem},
			wantErr:    true,
			wantItem:   `data[0] "customers"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "data prefix read but no list on the repository",
			statements: model.Statements{prefixRead},
			items:      []DataItem{prefixItem},
			wantErr:    true,
			wantItem:   `data[0] "customers"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "list on the repository and prefix read",
			statements: model.Statements{repoRead, prefixRead},
			items:      []DataItem{prefixItem},
		},
		{
			name:       "list on the repository and object read",
			statements: model.Statements{repoRead, objectRead},
			items:      []DataItem{objectItem},
		},
		{
			name: "object item covered by a broader prefix grant",
			statements: model.Statements{repoRead, {
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.ReadObjectAction},
				Resource: permissions.ObjectArn(src, "geo/*"),
			}},
			items: []DataItem{objectItem},
		},
		{
			name:       "second item is the offending one",
			statements: model.Statements{repoRead, prefixRead},
			items:      []DataItem{prefixItem, objectItem},
			wantErr:    true,
			wantItem:   `data[1] "reference/countries.csv"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "read object without list objects",
			statements: model.Statements{{Effect: model.StatementEffectAllow, Action: []string{permissions.ReadObjectAction}, Resource: permissions.ObjectArn(src, "geo/countries.csv")}},
			items:      []DataItem{objectItem},
			wantErr:    true,
			wantItem:   `data[0] "reference/countries.csv"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name: "an explicit deny beats an allow",
			statements: model.Statements{
				repoRead, prefixRead, objectRead,
				{Effect: model.StatementEffectDeny, Action: []string{permissions.ReadObjectAction}, Resource: permissions.ObjectArn(src, "geo/*")},
			},
			items:      []DataItem{prefixItem, objectItem},
			wantErr:    true,
			wantItem:   `data[1] "reference/countries.csv"`,
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newAuthzFixture(t)
			ctx := t.Context()
			f.user(t, creator)
			if tt.statements != nil {
				f.policy(t, "creator-grants", tt.statements)
				f.attachToUser(t, "creator-grants", creator)
			}

			err := f.authz.CheckSources(ctx, Caller{Username: creator}, tt.items)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrPermissionDenied), "a source denial must match ErrPermissionDenied: %v", err)
			var permErr *PermissionError
			require.ErrorAs(t, err, &permErr)
			require.Equal(t, tt.wantStatus, permErr.HTTPStatusCode())
			require.Contains(t, err.Error(), tt.wantItem, "the error must name the offending item (§9.3)")
			require.Contains(t, err.Error(), "forbidden")
		})
	}
}

// TestAuthorizerTableSourceCheck is the table half of the §9.3 source check (ICEBERG_CATALOG_PLAN.md
// §9): attaching a table item requires the catalog read pair on the table's own catalog ARN - the
// same gate /iceberg/api applies - and nothing else about the table.
func TestAuthorizerTableSourceCheck(t *testing.T) {
	t.Parallel()
	const (
		creator = "creator"
		src     = "src-repo"
	)
	tableItem := DataItem{
		Target:     "analytics/orders",
		Repository: src,
		Ref:        SourceRef{Type: RefTypeCommit, ID: "aaa"},
		Type:       ItemTypeTable,
		Table:      &TableSrc{Namespace: "analytics", Name: "orders"},
	}
	const tableArn = "arn:lakefs:catalog:::table/src-repo/analytics/orders"

	readTable := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.CatalogReadTableAction},
		Resource: tableArn,
	}
	readTableData := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.CatalogReadTableDataAction},
		Resource: tableArn,
	}

	tests := []struct {
		name       string
		statements model.Statements
		items      []DataItem
		wantErr    bool
		wantItem   string
	}{
		{
			name:    "no grants at all",
			items:   []DataItem{tableItem},
			wantErr: true, wantItem: `data[0] "analytics/orders"`,
		},
		{
			name:       "the catalog read pair on the table ARN",
			statements: model.Statements{readTable, readTableData},
			items:      []DataItem{tableItem},
		},
		{
			// ReadTable alone loads metadata; copying the table also copies its data files.
			name:       "metadata read without the data read",
			statements: model.Statements{readTable},
			items:      []DataItem{tableItem},
			wantErr:    true, wantItem: `data[0] "analytics/orders"`,
		},
		{
			name:       "data read without the metadata read",
			statements: model.Statements{readTableData},
			items:      []DataItem{tableItem},
			wantErr:    true, wantItem: `data[0] "analytics/orders"`,
		},
		{
			// The ARN names one table: a grant on a sibling does not carry over.
			name: "the pair on another table",
			statements: model.Statements{
				{Effect: model.StatementEffectAllow, Action: []string{permissions.CatalogReadTableAction}, Resource: "arn:lakefs:catalog:::table/src-repo/analytics/customers"},
				{Effect: model.StatementEffectAllow, Action: []string{permissions.CatalogReadTableDataAction}, Resource: "arn:lakefs:catalog:::table/src-repo/analytics/customers"},
			},
			items:   []DataItem{tableItem},
			wantErr: true, wantItem: `data[0] "analytics/orders"`,
		},
		{
			// fs access to the source is not the table gate: catalog actions are.
			name: "fs read on the source repository",
			statements: model.Statements{
				{Effect: model.StatementEffectAllow, Action: []string{"fs:*"}, Resource: permissions.All},
			},
			items:   []DataItem{tableItem},
			wantErr: true, wantItem: `data[0] "analytics/orders"`,
		},
		{
			name: "a wildcard catalog grant covers the table",
			statements: model.Statements{
				{Effect: model.StatementEffectAllow, Action: []string{"catalog:Read*"}, Resource: "arn:lakefs:catalog:::table/src-repo/*"},
			},
			items: []DataItem{tableItem},
		},
		{
			name: "an explicit deny on the data read beats the allow",
			statements: model.Statements{
				readTable, readTableData,
				{Effect: model.StatementEffectDeny, Action: []string{permissions.CatalogReadTableDataAction}, Resource: tableArn},
			},
			items:   []DataItem{tableItem},
			wantErr: true, wantItem: `data[0] "analytics/orders"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newAuthzFixture(t)
			ctx := t.Context()
			f.user(t, creator)
			if tt.statements != nil {
				f.policy(t, "creator-grants", tt.statements)
				f.attachToUser(t, "creator-grants", creator)
			}

			err := f.authz.CheckSources(ctx, Caller{Username: creator}, tt.items)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrPermissionDenied, "a source denial must match ErrPermissionDenied: %v", err)
			var permErr *PermissionError
			require.ErrorAs(t, err, &permErr)
			require.Equal(t, http.StatusForbidden, permErr.HTTPStatusCode())
			require.Contains(t, err.Error(), tt.wantItem, "the error must name the offending item (§9.3)")
			// The 403 names the actions that were required, so an operator can see it was the
			// catalog pair and not an fs permission.
			require.Contains(t, err.Error(), "catalog:ReadTable", "the denial names the catalog actions")
		})
	}
}

// TestSourceItemNameReserved verifies that a table or namespace denial names the catalog name a
// reader recognises, rather than an empty path.
func TestSourceItemNameReserved(t *testing.T) {
	t.Parallel()
	ref := SourceRef{Type: RefTypeCommit, ID: "abc"}
	table := DataItem{Target: "analytics/orders", Repository: "src", Ref: ref, Type: ItemTypeTable,
		Table: &TableSrc{Namespace: "analytics.sales", Name: "orders"}}
	namespace := DataItem{Target: "analytics", Repository: "src", Ref: ref, Type: ItemTypeNamespace, Namespace: ptr("analytics.sales")}

	require.Equal(t, `data[0] "analytics/orders" (table src@abc:analytics.sales.orders)`, SourceItemName(0, table))
	require.Equal(t, `data[1] "analytics" (namespace src@abc:analytics.sales)`, SourceItemName(1, namespace))
}

// TestCheckSourcesRespectsSubPathDeny is the unit half of SEC-3. CheckSources matches the caller's
// policy resources as patterns against the item's own ARN, so a Deny on a sub-path of the source
// repository denies every item that names a path inside it or below it. It is blind to a Deny that
// sits below a prefix item - the policy side is the pattern, so .../object/secret/* never matches
// the requested literal .../object/* - which is why SourceReader.CheckObject re-checks every object
// the materialiser reads (see TestSourceReadDenyOnSecretSubPath).
func TestCheckSourcesRespectsSubPathDeny(t *testing.T) {
	const (
		creator = "creator"
		src     = "src"
	)
	f := newAuthzFixture(t)
	ctx := t.Context()
	f.user(t, creator)
	// The review's caller: allow everything, deny reading salaries.
	f.policy(t, "allow-all", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"fs:*"},
		Resource: permissions.All,
	}})
	f.policy(t, "deny-secret", model.Statements{{
		Effect:   model.StatementEffectDeny,
		Action:   []string{permissions.ReadObjectAction},
		Resource: permissions.ObjectArn(src, "secret/*"),
	}})
	f.attachToUser(t, "allow-all", creator)
	f.attachToUser(t, "deny-secret", creator)

	object := func(path string) DataItem {
		return DataItem{Repository: src, Ref: SourceRef{Type: RefTypeCommit, ID: "aaa"}, Type: ItemTypeObject, Object: &PathSource{Path: path}}
	}
	prefix := func(path string) DataItem {
		return DataItem{Repository: src, Ref: SourceRef{Type: RefTypeCommit, ID: "aaa"}, Type: ItemTypePrefix, Prefix: &PathSource{Path: path}}
	}

	tests := []struct {
		name   string
		items  []DataItem
		denied bool
	}{
		{"an object inside the denied subtree", []DataItem{object("secret/salaries.csv")}, true},
		{"an object outside it", []DataItem{object("pub/report.csv")}, false},
		{"a prefix inside the denied subtree", []DataItem{prefix("secret/")}, true},
		{"a prefix deeper inside it", []DataItem{prefix("secret/salaries/")}, true},
		{"a second item inside it", []DataItem{object("pub/report.csv"), object("secret/salaries.csv")}, true},
		{"an item in another repository", []DataItem{{Repository: "other", Ref: SourceRef{Type: RefTypeCommit, ID: "aaa"}, Type: ItemTypeObject, Object: &PathSource{Path: "secret/salaries.csv"}}}, false},
		// The coarse check cannot see a deny below the item's own prefix: .../object/* does not
		// match .../object/secret/*. The per-object check is what closes this gap.
		{"a prefix above it", []DataItem{prefix("")}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := f.authz.CheckSources(ctx, Caller{Username: creator}, tt.items)
			if !tt.denied {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrPermissionDenied)
			var permErr *PermissionError
			require.ErrorAs(t, err, &permErr)
			require.Equal(t, http.StatusForbidden, permErr.HTTPStatusCode())
			require.Contains(t, err.Error(), "data[", "the denial names the offending item (§9.3)")
		})
	}
}

// TestConsumerGrantDoesNotGrantSourceRead is the §9.2/§9.3 boundary: a dataset grant is on the
// dataset repository, so it never lets its holder attach data from the source repositories.
func TestConsumerGrantDoesNotGrantSourceRead(t *testing.T) {
	t.Parallel()
	const consumer = "consumer"
	f := newAuthzFixture(t)
	ctx := t.Context()
	f.user(t, consumer)

	read := ConsumerReadPolicy("customer-360", time.Now())
	require.NoError(t, f.svc.WritePolicy(ctx, read, false))
	f.attachToUser(t, read.DisplayName, consumer)

	// The grant works on the dataset control plane and data plane...
	require.NoError(t, f.authz.CheckDataset(ctx, Caller{Username: consumer}, ActionReadDataset, "customer-360"))
	require.NoError(t, f.authz.Check(ctx, Caller{Username: consumer}, permissions.Node{
		Permission: permissions.Permission{Action: permissions.ReadObjectAction, Resource: permissions.ObjectArn("customer-360", "customers/p0.parquet")},
	}, ""))
	// ...but not on any source repository, nor on another dataset.
	require.Error(t, f.authz.Check(ctx, Caller{Username: consumer}, permissions.Node{
		Permission: permissions.Permission{Action: permissions.ReadObjectAction, Resource: permissions.ObjectArn("lakehouse-repo", "data/customers/c1.parquet")},
	}, ""))
	require.Error(t, f.authz.CheckDataset(ctx, Caller{Username: consumer}, ActionReadDataset, "other-dataset"))
}

// TestAuthorizerConditionsMatchTheCoreAPI pins that a conditional policy statement is evaluated
// with the request condition context, the way pkg/api Controller.authorize builds it. The
// acl.AuthService reads conditions from the context (not from AuthorizationRequest.Fields), so
// this also proves the Authorizer puts the condition context on ctx.
func TestAuthorizerConditionsMatchTheCoreAPI(t *testing.T) {
	t.Parallel()
	const user = "alice"
	f := newAuthzFixture(t)
	ctx := t.Context()
	f.user(t, user)
	f.policy(t, "conditional-read", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{ActionReadDataset},
		Resource: DatasetArn("customer-360"),
		Condition: map[string]map[string][]string{
			auth.OperatorNameIpAddress: {"SourceIp": {"10.0.0.0/8"}},
		},
	}})
	f.attachToUser(t, "conditional-read", user)

	require.NoError(t, f.authz.CheckDataset(ctx, Caller{Username: user, ClientIP: "10.1.2.3"}, ActionReadDataset, "customer-360"),
		"an IpAddress condition that matches must allow")
	require.Error(t, f.authz.CheckDataset(ctx, Caller{Username: user, ClientIP: "192.0.2.7"}, ActionReadDataset, "customer-360"),
		"an IpAddress condition that does not match must not allow")
	require.Error(t, f.authz.CheckDataset(ctx, Caller{Username: user}, ActionReadDataset, "customer-360"),
		"a missing condition field must not allow")
}

// TestAuthorizerFailsClosed pins that an Authorizer without an auth service never allows.
func TestAuthorizerFailsClosed(t *testing.T) {
	t.Parallel()
	for _, authz := range []*Authorizer{NewAuthorizer(nil), nil} {
		err := authz.Check(t.Context(), Caller{Username: "alice"}, DatasetPermission(ActionReadDataset, "customer-360"), "")
		require.ErrorIs(t, err, ErrNoAuthorizer)
		var authErr *AuthorizationError
		require.ErrorAs(t, err, &authErr)
		require.Equal(t, http.StatusInternalServerError, authErr.HTTPStatusCode())
		require.False(t, errors.Is(err, ErrPermissionDenied), "a broken authorizer is not a denial")
	}
}

// TestMetadataKeyPermission pins the documented dataset/* resource of metadata-key actions.
func TestMetadataKeyPermission(t *testing.T) {
	t.Parallel()
	node := MetadataKeyPermission(ActionUpdateMetadataKey)
	require.Equal(t, []permissions.Permission{{
		Action:   "dataset:UpdateMetadataKey",
		Resource: "arn:lakefs:dataset:::dataset/*",
	}}, requiredPermissions(node))
}

// asStatus returns the HTTP status of a typed authorization error, or 0.
func asStatus(err error) int {
	var statused interface{ HTTPStatusCode() int }
	if errors.As(err, &statused) {
		return statused.HTTPStatusCode()
	}
	return 0
}
