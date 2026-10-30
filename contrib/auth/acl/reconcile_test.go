package acl_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authacl "github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/contrib/auth/acl/testutil"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// legacyACLTemplateVersionKeyName is the pre-R1 name of the ACL template version record. It is
// spelled out here on purpose: the migration has to keep reading what older binaries wrote.
const legacyACLTemplateVersionKeyName = "acl_template_version"

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

// countingService counts policy writes, so a test can assert that a reconcile pass rewrote
// nothing. It embeds *authacl.AuthService, so it still satisfies the package's KV-store provider.
type countingService struct {
	*authacl.AuthService
	policyWrites int
}

func (c *countingService) WritePolicy(ctx context.Context, policy *model.Policy, update bool) error {
	c.policyWrites++
	return c.AuthService.WritePolicy(ctx, policy, update)
}

func setTemplateVersion(t *testing.T, ctx context.Context, store kv.Store, key []byte, version string) {
	t.Helper()
	require.NoError(t, store.Set(ctx, []byte(model.PartitionKey), key, []byte(version)))
}

func templateVersion(t *testing.T, ctx context.Context, store kv.Store, key []byte) (string, bool) {
	t.Helper()
	value, err := store.Get(ctx, []byte(model.PartitionKey), key)
	if errors.Is(err, kv.ErrNotFound) {
		return "", false
	}
	require.NoError(t, err)
	return string(value.Value), true
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

// TestReconcileACLPolicies_EquivalentResourceEncodingIsNotRewritten verifies that the
// comparison, not only the version guard, keeps a policy from being rewritten: Resource is a
// JSON array of alternative ARNs (auth.ParsePolicyResourceAsList), so a re-encoded array that
// grants the same resources is not a template change. The version guard is deliberately left
// open for both passes, otherwise it alone would explain the missing second rewrite.
func TestReconcileACLPolicies_EquivalentResourceEncodingIsNotRewritten(t *testing.T) {
	ctx := t.Context()
	svc, store := testutil.SetupService(t, ctx, reconcileSecret)

	policy := &model.Policy{
		DisplayName: "ACL(_-_)Writers",
		Statement:   staleStatements(),
		ACL:         model.ACL{Permission: authacl.WritePermission},
	}
	writePolicy(t, ctx, svc, policy)
	setTemplateVersion(t, ctx, store, authacl.TemplateVersionKey(authacl.ACLTemplateVersionKeyName), "stale")

	counting := &countingService{AuthService: svc}
	require.NoError(t, authacl.ReconcileACLPolicies(ctx, counting, logging.Dummy()))
	require.Equal(t, 1, counting.policyWrites, "the stale statements must be rewritten on the first pass")

	// Re-encode every resource of the freshly written template as a single-element JSON array
	// with different spacing: the same grant, different bytes. The ACL writer's ValidatePolicy
	// rejects array resources today, so this bypasses it and stores the record directly -
	// exactly what a policy written by another writer (or by an older binary) looks like on
	// disk. The reconcile comparison must not rewrite it.
	got, err := svc.GetPolicy(ctx, policy.DisplayName)
	require.NoError(t, err)
	reencoded := make(model.Statements, len(got.Statement))
	for i, statement := range got.Statement {
		statement.Resource = fmt.Sprintf("[ %q ]", statement.Resource)
		reencoded[i] = statement
	}
	got.Statement = reencoded
	require.NoError(t, kv.SetMsg(ctx, store, model.PartitionKey, model.PolicyPath(policy.DisplayName), model.ProtoFromPolicy(got)))

	counting.policyWrites = 0
	setTemplateVersion(t, ctx, store, authacl.TemplateVersionKey(authacl.ACLTemplateVersionKeyName), "stale")
	require.NoError(t, authacl.ReconcileACLPolicies(ctx, counting, logging.Dummy()))
	require.Zero(t, counting.policyWrites, "an equivalent but differently-encoded Resource must not be rewritten")

	stored, err := svc.GetPolicy(ctx, policy.DisplayName)
	require.NoError(t, err)
	require.Equal(t, withoutEmptyConditions(reencoded), withoutEmptyConditions(stored.Statement))
}

// TestReconcileACLPolicies_VersionKey covers the shared template-version key convention and the
// migration of the pre-R1 record: a version recorded under either key must not be reconciled
// again, and the legacy record is copied forward so later boots only read the current key.
func TestReconcileACLPolicies_VersionKey(t *testing.T) {
	currentKey := authacl.TemplateVersionKey(authacl.ACLTemplateVersionKeyName)
	legacyKey := authacl.TemplateVersionKey(legacyACLTemplateVersionKeyName)

	require.Equal(t, []byte(model.MetadataKeyPath(authacl.ACLTemplateVersionKeyName)), currentKey)
	require.NotEqual(t, currentKey, legacyKey, "the current and legacy records must be distinct keys")

	tests := []struct {
		name string
		// arrange records the applied version the way the binary under test would find it.
		arrange func(t *testing.T, ctx context.Context, store kv.Store)
	}{
		{
			name: "current key",
			arrange: func(t *testing.T, ctx context.Context, store kv.Store) {
				setTemplateVersion(t, ctx, store, currentKey, authacl.ACLTemplateVersion)
			},
		},
		{
			name: "legacy key",
			arrange: func(t *testing.T, ctx context.Context, store kv.Store) {
				setTemplateVersion(t, ctx, store, legacyKey, authacl.ACLTemplateVersion)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			svc, store := testutil.SetupService(t, ctx, reconcileSecret)
			writePolicy(t, ctx, svc, &model.Policy{
				DisplayName: "ACL(_-_)Writers",
				Statement:   staleStatements(),
				ACL:         model.ACL{Permission: authacl.WritePermission},
			})
			tt.arrange(t, ctx, store)

			counting := &countingService{AuthService: svc}
			require.NoError(t, authacl.ReconcileACLPolicies(ctx, counting, logging.Dummy()))
			require.Zero(t, counting.policyWrites, "an already reconciled version must not be reconciled again")

			// The version round-trips through the current key, also when it came from the legacy one.
			version, ok := templateVersion(t, ctx, store, currentKey)
			require.True(t, ok, "the current version key must be recorded")
			require.Equal(t, authacl.ACLTemplateVersion, version)

			// The legacy record can disappear without triggering another reconciliation.
			require.NoError(t, store.Delete(ctx, []byte(model.PartitionKey), legacyKey))
			require.NoError(t, authacl.ReconcileACLPolicies(ctx, counting, logging.Dummy()))
			require.Zero(t, counting.policyWrites)
		})
	}
}
