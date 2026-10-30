// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// authz_test.go covers the authorization building blocks of design §9.1-§9.4: the ARN helpers,
// the action constants, the two per-dataset policies, and the attach-time source check against a
// real acl.AuthService on an in-memory KV store (§15.1).
package datasets

import (
	"context"
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
		{"dataset", DatasetArn("customer-360"), "arn:lakefs:datasets:::dataset/customer-360"},
		{"metadata key", MetadataKeyArn("classification"), "arn:lakefs:datasets:::metadata-key/classification"},
		{"dataset empty name", DatasetArn(""), "arn:lakefs:datasets:::dataset/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.got)
			// The helpers must produce ARNs the policy validator accepts, and the datasets
			// service part must be the one of §9.1.
			require.NoError(t, model.ValidateArn(tt.got), "policy validator must accept %q", tt.got)
			require.True(t, strings.HasPrefix(tt.got, "arn:lakefs:datasets:::dataset/") ||
				strings.HasPrefix(tt.got, "arn:lakefs:datasets:::metadata-key/"))
		})
	}
}

func TestActionConstantsAreValid(t *testing.T) {
	t.Parallel()
	// §9.1. The values are the contract: policies written by hand, by the UI or by the service
	// all use these strings.
	actions := map[string]string{
		ActionListDatasets:       "datasets:ListDatasets",
		ActionCreateDataset:      "datasets:CreateDataset",
		ActionReadDataset:        "datasets:ReadDataset",
		ActionPublishVersion:     "datasets:PublishVersion",
		ActionCreateProposal:     "datasets:CreateProposal",
		ActionDeleteDataset:      "datasets:DeleteDataset",
		ActionManageAccess:       "datasets:ManageAccess",
		ActionManageMetadataKeys: "datasets:ManageMetadataKeys",
	}
	for got, want := range actions {
		require.Equal(t, want, got)
		// The "datasets" serviceSet entry exists in pkg/permissions/actions.go (RBAC work), so
		// these actions validate without any actions.gen.go codegen.
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
			Action:   []string{"datasets:ReadDataset"},
			Resource: "arn:lakefs:datasets:::dataset/customer-360",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:ListRepositories", "datasets:ListDatasets"},
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
			require.Falsef(t, strings.HasPrefix(action, "datasets:Publish") ||
				strings.HasPrefix(action, "datasets:Create") ||
				strings.HasPrefix(action, "datasets:Delete") ||
				strings.HasPrefix(action, "datasets:Manage"), "unexpected control-plane action %q", action)
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
		"retention:SetGarbageCollectionRules", "ci:WriteAction",
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
		name       string
		item       DataItem
		wantObject string
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
			name:       "table falls back to the whole repository",
			item:       DataItem{Repository: "lakehouse-repo", Ref: commitRef, Type: ItemTypeTable},
			wantObject: "arn:lakefs:fs:::repository/lakehouse-repo/object/*",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node := SourceReadNode(tt.item)
			require.Equal(t, permissions.NodeTypeAnd, node.Type)
			// DOC-15: the two permissions are the ones the core API authorises - listing is on the
			// repository ARN, reading is on the object ARN - and fs:ReadCommit is not required.
			require.Equal(t, []permissions.Permission{
				{Action: "fs:ListObjects", Resource: "arn:lakefs:fs:::repository/" + tt.item.Repository},
				{Action: "fs:ReadObject", Resource: tt.wantObject},
			}, requiredPermissions(node))
			require.Equal(t, tt.wantObject, SourceObjectArn(tt.item))
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

	// The grants below are written the way the public documentation describes them (DOC-15):
	// fs:ListObjects on the *repository* ARN - what the core API authorises for a listing - and
	// fs:ReadObject on the source path. fs:ReadCommit is not part of the check any more, so
	// repoRead grants it to show that it makes no difference either way.
	repoList := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ListObjectsAction},
		Resource: permissions.RepoArn(src),
	}
	repoRead := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ReadCommitAction},
		Resource: permissions.RepoArn(src),
	}
	prefixRead := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ReadObjectAction, permissions.ListObjectsAction},
		Resource: permissions.ObjectArn(src, "data/*"),
	}
	objectRead := model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.ReadObjectAction, permissions.ListObjectsAction},
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
			name:       "commit read only",
			statements: model.Statements{repoRead},
			items:      []DataItem{prefixItem},
			wantErr:    true,
			wantItem:   `data[0] "customers"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "repository read instead of object read",
			statements: model.Statements{{Effect: model.StatementEffectAllow, Action: []string{permissions.ReadRepositoryAction}, Resource: permissions.RepoArn(src)}},
			items:      []DataItem{prefixItem},
			wantErr:    true,
			wantItem:   `data[0] "customers"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "data prefix read but no list objects",
			statements: model.Statements{prefixRead},
			items:      []DataItem{prefixItem},
			wantErr:    true,
			wantItem:   `data[0] "customers"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name: "list objects on the object ARN is not what the core API authorises",
			statements: model.Statements{
				prefixRead,
				{Effect: model.StatementEffectAllow, Action: []string{permissions.ListObjectsAction}, Resource: permissions.ObjectArn(src, "data/*")},
			},
			items:      []DataItem{prefixItem},
			wantErr:    true,
			wantItem:   `data[0] "customers"`,
			wantStatus: http.StatusForbidden,
		},
		{
			// fs:ReadCommit is not part of the check any more (DOC-15): the documented pair is
			// enough, and nothing here grants commit read.
			name:       "list objects and prefix read, without commit read",
			statements: model.Statements{repoList, prefixRead},
			items:      []DataItem{prefixItem},
		},
		{
			name:       "list objects and object read",
			statements: model.Statements{repoList, repoRead, objectRead},
			items:      []DataItem{objectItem},
		},
		{
			name: "object item covered by a broader prefix grant",
			statements: model.Statements{repoList, repoRead, {
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.ReadObjectAction, permissions.ListObjectsAction},
				Resource: permissions.ObjectArn(src, "geo/*"),
			}},
			items: []DataItem{objectItem},
		},
		{
			name:       "second item is the offending one",
			statements: model.Statements{repoList, repoRead, prefixRead},
			items:      []DataItem{prefixItem, objectItem},
			wantErr:    true,
			wantItem:   `data[1] "reference/countries.csv"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name: "read object without list objects",
			statements: model.Statements{repoRead, {
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.ReadObjectAction},
				Resource: permissions.ObjectArn(src, "geo/countries.csv"),
			}},
			items:      []DataItem{objectItem},
			wantErr:    true,
			wantItem:   `data[0] "reference/countries.csv"`,
			wantStatus: http.StatusForbidden,
		},
		{
			name: "an explicit deny beats an allow",
			statements: model.Statements{
				repoList, repoRead, prefixRead, objectRead,
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

// TestMetadataKeyPermission pins the §9.1 metadata-key ARN.
func TestMetadataKeyPermission(t *testing.T) {
	t.Parallel()
	node := MetadataKeyPermission(ActionManageMetadataKeys, "classification")
	require.Equal(t, []permissions.Permission{{
		Action:   "datasets:ManageMetadataKeys",
		Resource: "arn:lakefs:datasets:::metadata-key/classification",
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

// subPathDenyUser is the SEC-3 exfiltration scenario: a principal with a blanket fs:* allow and
// one deny on a sub-path of a source repository.
const (
	subPathDenyUser = "exfiltrator"
	subPathDenyRepo = "src"
)

// blanketAllow is the fs:* on * grant of a principal who may read everything except the paths a
// deny policy excludes.
var blanketAllow = model.Statement{
	Effect:   model.StatementEffectAllow,
	Action:   []string{"fs:*"},
	Resource: permissions.All,
}

// subPathDeny is the deny of the SEC-3 scenario: fs:ReadObject on <repo>/object/secret/*.
func subPathDeny(action string) model.Statement {
	return model.Statement{
		Effect:   model.StatementEffectDeny,
		Action:   []string{action},
		Resource: permissions.ObjectArn(subPathDenyRepo, "secret/*"),
	}
}

// TestAuthorizerCheckSourcesSubPathDeny is the SEC-3 regression. The item-level check authorizes
// ObjectArn(src, "<prefix>*"), and ArnMatch treats the policy resource as the pattern side of the
// match, so a Deny on "<prefix>/secret/*" never matches that literal glob. CheckSources therefore
// analyses the caller's Deny statements against the prefix on its own, and refuses the item.
func TestAuthorizerCheckSourcesSubPathDeny(t *testing.T) {
	t.Parallel()
	commitRef := SourceRef{Type: RefTypeCommit, ID: "aaa"}
	prefixItem := func(path string) DataItem {
		return DataItem{
			Target:     "customers",
			Repository: subPathDenyRepo,
			Ref:        commitRef,
			Type:       ItemTypePrefix,
			Prefix:     &PathSource{Path: path},
		}
	}

	tests := []struct {
		name       string
		statements model.Statements
		items      []DataItem
		wantDenied bool
		// wantDenyNamed reports whether the message must name the deny resource, which only
		// the sub-path analysis does (the item-level check reports the permissions it tried).
		wantDenyNamed bool
	}{
		{
			name:          "deny on a sub-path of a whole-repository prefix",
			statements:    model.Statements{blanketAllow, subPathDeny(permissions.ReadObjectAction)},
			items:         []DataItem{prefixItem("")},
			wantDenied:    true,
			wantDenyNamed: true,
		},
		{
			name:          "deny on a sub-path that only starts with the prefix",
			statements:    model.Statements{blanketAllow, subPathDeny(permissions.ReadObjectAction)},
			items:         []DataItem{prefixItem("sec/")},
			wantDenied:    true,
			wantDenyNamed: true,
		},
		{
			name:       "deny on a sub-path inside the prefix",
			statements: model.Statements{blanketAllow, subPathDeny(permissions.ReadObjectAction)},
			items:      []DataItem{prefixItem("secret/")},
			wantDenied: true,
		},
		{
			name:          "deny on ListObjects is enough",
			statements:    model.Statements{blanketAllow, subPathDeny(permissions.ListObjectsAction)},
			items:         []DataItem{prefixItem("")},
			wantDenied:    true,
			wantDenyNamed: true,
		},
		{
			name:       "deny on an unrelated prefix",
			statements: model.Statements{blanketAllow, subPathDeny(permissions.ReadObjectAction)},
			items:      []DataItem{prefixItem("public/")},
		},
		{
			name:       "deny on a sibling prefix",
			statements: model.Statements{blanketAllow, subPathDeny(permissions.ReadObjectAction)},
			items:      []DataItem{prefixItem("salaries/")},
		},
		{
			name: "deny in another repository",
			statements: model.Statements{blanketAllow, {
				Effect:   model.StatementEffectDeny,
				Action:   []string{permissions.ReadObjectAction},
				Resource: permissions.ObjectArn("other-repo", "secret/*"),
			}},
			items: []DataItem{prefixItem("")},
		},
		{
			name: "a write deny does not make the prefix unreadable",
			statements: model.Statements{blanketAllow, {
				Effect:   model.StatementEffectDeny,
				Action:   []string{permissions.WriteObjectAction},
				Resource: permissions.ObjectArn(subPathDenyRepo, "secret/*"),
			}},
			items: []DataItem{prefixItem("")},
		},
		{
			name:       "an object item needs no analysis: its ARN is literal",
			statements: model.Statements{blanketAllow, subPathDeny(permissions.ReadObjectAction)},
			items: []DataItem{{
				Target:     "leak",
				Repository: subPathDenyRepo,
				Ref:        commitRef,
				Type:       ItemTypeObject,
				Object:     &PathSource{Path: "secret/salaries.csv"},
			}},
			wantDenied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newAuthzFixture(t)
			ctx := t.Context()
			f.user(t, subPathDenyUser)
			f.policy(t, "exfiltrator-grants", tt.statements)
			f.attachToUser(t, "exfiltrator-grants", subPathDenyUser)

			err := f.authz.CheckSources(ctx, Caller{Username: subPathDenyUser}, tt.items)
			if !tt.wantDenied {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorIs(t, err, ErrPermissionDenied)
			var permission *PermissionError
			require.ErrorAs(t, err, &permission)
			require.Equal(t, http.StatusForbidden, permission.HTTPStatusCode())
			require.Contains(t, err.Error(), "data[0]", "the 403 names the item (§9.3)")
			if tt.wantDenyNamed {
				require.Contains(t, err.Error(), "secret/*", "the 403 names the deny that made it fail")
			}
		})
	}
}

// TestAuthorizerCheckSourcesWithoutPolicyLister pins the degradation of the sub-path analysis: an
// auth service that cannot list effective policies (auth.BasicAuthService answers
// auth.ErrNotImplemented) leaves CheckSources with the item-level check only - and the per-object
// check of SourceReadGuard, which needs no policy listing, stays the authority (SEC-3).
func TestAuthorizerCheckSourcesWithoutPolicyLister(t *testing.T) {
	t.Parallel()
	f := newAuthzFixture(t)
	ctx := t.Context()
	authorizerOnly := NewAuthorizer(struct{ auth.Authorizer }{f.svc})
	require.Nil(t, authorizerOnly.policies, "an authorizer-only service has no policy source")

	// The same ACL service still backs the guard, so the per-object decision is the real one.
	guard := NewSourceReadGuard(NewAuthorizer(f.svc), Caller{Username: subPathDenyUser})
	f.user(t, subPathDenyUser)
	f.policy(t, "exfiltrator-grants", model.Statements{blanketAllow, subPathDeny(permissions.ReadObjectAction)})
	f.attachToUser(t, "exfiltrator-grants", subPathDenyUser)

	item := DataItem{
		Target:     "customers",
		Repository: subPathDenyRepo,
		Ref:        SourceRef{Type: RefTypeCommit, ID: "aaa"},
		Type:       ItemTypePrefix,
		Prefix:     &PathSource{Path: ""},
	}
	require.NoError(t, authorizerOnly.CheckSources(ctx, Caller{Username: subPathDenyUser}, []DataItem{item}),
		"without a policy source the item-level check cannot see the sub-path deny")

	denied := guard.CheckObject(ctx, 0, &item, "secret/salaries.csv")
	require.Error(t, denied, "the per-object check must still deny")
	require.ErrorIs(t, denied, ErrPermissionDenied)
	var sourceErr *SourceError
	require.ErrorAs(t, denied, &sourceErr)
	require.Equal(t, "secret/salaries.csv", sourceErr.Path)
	require.Contains(t, denied.Error(), "secret/salaries.csv")
}

// countingAuthService counts the authorization questions the source guard actually asks, which is
// how the decision cache of SourceReadGuard becomes observable.
type countingAuthService struct {
	*acl.AuthService
	calls int
}

func (c *countingAuthService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	c.calls++
	return c.AuthService.Authorize(ctx, req)
}

// TestSourceReadGuard pins SEC-3 at the unit the materialiser uses: every source path is
// authorized on its own object ARN, a denial becomes a *SourceError naming the path, and the
// decision cache reuses a decision exactly as deeply as the caller's policies allow.
func TestSourceReadGuard(t *testing.T) {
	t.Parallel()
	const (
		user = "reader"
		repo = "src"
	)
	item := DataItem{
		Target:     "customers",
		Repository: repo,
		Ref:        SourceRef{Type: RefTypeCommit, ID: "aaa"},
		Type:       ItemTypePrefix,
		Prefix:     &PathSource{Path: ""},
	}

	setup := func(t *testing.T, statements model.Statements) (*SourceReadGuard, *countingAuthService) {
		t.Helper()
		f := newAuthzFixture(t)
		counting := &countingAuthService{AuthService: f.svc}
		f.user(t, user)
		f.policy(t, "reader-grants", statements)
		f.attachToUser(t, "reader-grants", user)
		return NewSourceReadGuard(NewAuthorizer(counting), Caller{Username: user}), counting
	}

	t.Run("denied sub-path is named", func(t *testing.T) {
		t.Parallel()
		guard, _ := setup(t, model.Statements{blanketAllow, subPathDeny(permissions.ReadObjectAction)})
		require.NoError(t, guard.CheckObject(t.Context(), 0, &item, "public/report.csv"))

		err := guard.CheckObject(t.Context(), 0, &item, "secret/salaries.csv")
		require.ErrorIs(t, err, ErrPermissionDenied, "a denied source path must be a permission denial")
		var sourceErr *SourceError
		require.ErrorAs(t, err, &sourceErr)
		require.Equal(t, 0, sourceErr.Item)
		require.Equal(t, repo, sourceErr.Repository)
		require.Equal(t, "secret/salaries.csv", sourceErr.Path, "the error names the denied path")
		require.Equal(t, http.StatusForbidden, asStatus(err))
	})

	t.Run("one decision per policy prefix", func(t *testing.T) {
		t.Parallel()
		// A single directory-scoped allow: every path in that directory shares one decision.
		guard, counting := setup(t, model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadObjectAction},
			Resource: permissions.ObjectArn(repo, "data/*"),
		}})
		for _, path := range []string{"data/a.parquet", "data/b.parquet", "data/nested/c.parquet"} {
			require.NoError(t, guard.CheckObject(t.Context(), 0, &item, path))
		}
		require.Equal(t, 1, counting.calls, "the three paths share one bucket and one decision")
	})

	t.Run("a deny below the wildcard needs a decision per path", func(t *testing.T) {
		t.Parallel()
		// The deny constrains the file name ("*.csv"), so it can distinguish two files of one
		// directory: the guard must stop bucketing and ask per path.
		guard, counting := setup(t, model.Statements{
			blanketAllow,
			{
				Effect:   model.StatementEffectDeny,
				Action:   []string{permissions.ReadObjectAction},
				Resource: permissions.ObjectArn(repo, "data/*.csv"),
			},
		})
		require.NoError(t, guard.CheckObject(t.Context(), 0, &item, "data/report.json"))
		require.Error(t, guard.CheckObject(t.Context(), 0, &item, "data/salaries.csv"))
		require.GreaterOrEqual(t, counting.calls, 2, "a decision per path, never one for the directory")
	})

	t.Run("no guard is no check", func(t *testing.T) {
		t.Parallel()
		var guard *SourceReadGuard
		require.NoError(t, guard.CheckObject(t.Context(), 0, &item, "secret/salaries.csv"),
			"a nil guard is the internal-repair path, and must not panic")
	})
}

// TestReadBucketPrefix pins the bucket analysis the decision cache is built on: only a directory
// prefix followed by nothing but "*" can key a shared decision.
func TestReadBucketPrefix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		resource   string
		wantPrefix string
		wantOK     bool
	}{
		{"arn:lakefs:fs:::repository/src/object/data/*", "arn:lakefs:fs:::repository/src/object/data/", true},
		{"*", "", true},
		{"arn:lakefs:fs:::repository/src/object/data/**", "arn:lakefs:fs:::repository/src/object/data/", true},
		{"arn:lakefs:fs:::repository/src/object/data/x.csv", "", false},
		{"arn:lakefs:fs:::repository/src/object/custo*", "", false},
		{"arn:lakefs:fs:::repository/src/object/data/*.csv", "", false},
		{"arn:lakefs:fs:::repository/src/object/data/x?", "", false},
		{"arn:lakefs:fs:::repository/*/object/data/*", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.resource, func(t *testing.T) {
			t.Parallel()
			prefix, ok := readBucketPrefix(tt.resource)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantPrefix, prefix)
		})
	}
}
