package acl_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authacl "github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/contrib/auth/acl/testutil"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

var reconcileSecret = []byte("some secret")

func staleStatements() model.Statements {
	return model.Statements{
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadObjectAction},
			Resource: permissions.All,
		},
	}
}

func withoutEmptyConditions(statements model.Statements) model.Statements {
	normalized := make(model.Statements, len(statements))
	for i, statement := range statements {
		if len(statement.Condition) == 0 {
			statement.Condition = nil
		}
		normalized[i] = statement
	}
	return normalized
}

func writePolicy(t *testing.T, ctx context.Context, svc *authacl.AuthService, policy *model.Policy) {
	t.Helper()
	if err := svc.WritePolicy(ctx, policy, false); err != nil {
		t.Fatalf("WritePolicy(%s): %v", policy.DisplayName, err)
	}
}

func TestReconcileACLPolicies_FixesStalePolicy(t *testing.T) {
	ctx := t.Context()
	svc, _ := testutil.SetupService(t, ctx, reconcileSecret)

	acl := model.ACL{Permission: authacl.WritePermission}
	policy := &model.Policy{
		CreatedAt:   time.Unix(12345678, 0),
		DisplayName: "ACL(_-_)Writers",
		Statement:   staleStatements(),
		ACL:         acl,
	}
	writePolicy(t, ctx, svc, policy)

	custom := &model.Policy{
		DisplayName: "custom",
		Statement:   staleStatements(),
	}
	writePolicy(t, ctx, svc, custom)

	require.NoError(t, authacl.ReconcileACLPolicies(ctx, svc, logging.Dummy()))

	want, err := authacl.ACLToStatement(acl)
	require.NoError(t, err)
	got, err := svc.GetPolicy(ctx, policy.DisplayName)
	require.NoError(t, err)
	require.Equal(t, want, withoutEmptyConditions(got.Statement))
	require.Equal(t, acl, got.ACL)
	require.Equal(t, policy.CreatedAt.Unix(), got.CreatedAt.Unix())

	gotCustom, err := svc.GetPolicy(ctx, custom.DisplayName)
	require.NoError(t, err)
	require.Equal(t, withoutEmptyConditions(custom.Statement), withoutEmptyConditions(gotCustom.Statement))
}

// TestReconcileACLPolicies_ReconcilesEditedACLPolicy verifies that an ACL-derived
// policy is a template snapshot: local edits are overwritten while the policy still
// carries an ACL.
func TestReconcileACLPolicies_ReconcilesEditedACLPolicy(t *testing.T) {
	ctx := t.Context()
	svc, _ := testutil.SetupService(t, ctx, reconcileSecret)

	acl := model.ACL{Permission: authacl.WritePermission}
	policy := &model.Policy{
		DisplayName: "ACL(_-_)Edited",
		Statement: model.Statements{
			{
				Effect:   model.StatementEffectDeny,
				Action:   []string{permissions.WriteObjectAction},
				Resource: permissions.All,
			},
		},
		ACL: acl,
	}
	writePolicy(t, ctx, svc, policy)

	require.NoError(t, authacl.ReconcileACLPolicies(ctx, svc, logging.Dummy()))

	want, err := authacl.ACLToStatement(acl)
	require.NoError(t, err)
	got, err := svc.GetPolicy(ctx, policy.DisplayName)
	require.NoError(t, err)
	require.Equal(t, want, withoutEmptyConditions(got.Statement))
}

// TestReconcileACLPolicies_RerunIsNoOp verifies the template version guard: after a
// successful run, later calls do not rewrite policies for the same template version.
func TestReconcileACLPolicies_RerunIsNoOp(t *testing.T) {
	ctx := t.Context()
	svc, _ := testutil.SetupService(t, ctx, reconcileSecret)

	policy := &model.Policy{
		DisplayName: "ACL(_-_)Writers",
		Statement:   staleStatements(),
		ACL:         model.ACL{Permission: authacl.WritePermission},
	}
	writePolicy(t, ctx, svc, policy)

	require.NoError(t, authacl.ReconcileACLPolicies(ctx, svc, logging.Dummy()))

	// Simulate a stale policy appearing after this template version was reconciled.
	edited := staleStatements()
	edited = append(edited, model.Statement{
		Effect:   model.StatementEffectDeny,
		Action:   []string{permissions.DeleteObjectAction},
		Resource: permissions.All,
	})
	policy.Statement = edited
	require.NoError(t, svc.WritePolicy(ctx, policy, true))

	require.NoError(t, authacl.ReconcileACLPolicies(ctx, svc, logging.Dummy()))

	got, err := svc.GetPolicy(ctx, policy.DisplayName)
	require.NoError(t, err)
	require.Equal(t, withoutEmptyConditions(edited), withoutEmptyConditions(got.Statement), "second reconcile for the same template version must be a no-op")
}
