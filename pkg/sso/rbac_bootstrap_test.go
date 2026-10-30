package sso_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/kv"
	logging "github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
)

var rbacBaseGroups = []string{
	setup.AdminsGroup,
	setup.SuperUsersGroup,
	setup.DevelopersGroup,
	setup.ViewersGroup,
}

// legacyRBACVersionKeyName is the pre-R1 key of the RBAC bootstrap version record: a bare key in
// the auth partition root, written before the version record moved under the shared
// "metadata/" template-version path. Spelled out on purpose: the migration has to keep reading
// what older binaries wrote.
const legacyRBACVersionKeyName = "rbac-bootstrap-version"

func setRBACVersion(t *testing.T, ctx context.Context, store kv.Store, raw string) {
	t.Helper()
	require.NoError(t, store.Set(ctx, []byte(model.PartitionKey), sso.RBACBootstrapVersionKey(), []byte(raw)))
}

// setLegacyRBACVersion records a version the way binaries before R1 did.
func setLegacyRBACVersion(t *testing.T, ctx context.Context, store kv.Store, raw string) {
	t.Helper()
	require.NoError(t, store.Set(ctx, []byte(model.PartitionKey), []byte(legacyRBACVersionKeyName), []byte(raw)))
}

// storedRBACVersion returns the raw stored version and whether the key exists.
func storedRBACVersion(t *testing.T, ctx context.Context, store kv.Store) (string, bool) {
	t.Helper()
	value, err := store.Get(ctx, []byte(model.PartitionKey), sso.RBACBootstrapVersionKey())
	if errors.Is(err, kv.ErrNotFound) {
		return "", false
	}
	require.NoError(t, err)
	return string(value.Value), true
}

func detachViewersFSReadAll(t *testing.T, ctx context.Context, svc *acl.AuthService) {
	t.Helper()
	require.NoError(t, svc.DetachPolicyFromGroup(ctx, "FSReadAll", setup.ViewersGroup))
}

func requireBaseGroupsExist(t *testing.T, ctx context.Context, svc *acl.AuthService) {
	t.Helper()
	for _, group := range rbacBaseGroups {
		_, err := svc.GetGroup(ctx, group)
		require.NoErrorf(t, err, "group %s", group)
	}
}

func viewersPolicies(t *testing.T, ctx context.Context, svc *acl.AuthService) []string {
	t.Helper()
	policies, _, err := svc.ListGroupPolicies(ctx, setup.ViewersGroup, &model.PaginationParams{Amount: 100})
	require.NoError(t, err)
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.DisplayName)
	}
	return names
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// failingAuthService fails the very first step of the RBAC bootstrap, to prove that the
// version is recorded only after a successful bootstrap.
type failingAuthService struct {
	auth.Service
}

func (failingAuthService) CreateGroup(context.Context, *model.Group) (*model.Group, error) {
	return nil, errors.New("create group failed")
}

func TestEnsureRBACBase(t *testing.T) {
	version := func(v string) *string { return &v }
	current := strconv.Itoa(sso.RBACBootstrapVersion)

	tests := []struct {
		name string
		// firstRun simulates a previous boot that already applied the template.
		firstRun bool
		// arrange runs after firstRun and before the run under test.
		arrange func(t *testing.T, ctx context.Context, store kv.Store, svc *acl.AuthService)
		// failing replaces the auth service with one whose bootstrap fails.
		failing bool
		wantErr bool
		// wantVersion is nil when the version key must not exist.
		wantVersion *string
		// wantLegacyVersion is checked only when set: the pre-R1 key must keep its value.
		wantLegacyVersion *string
		// wantGroups is true when the base groups must exist after the run.
		wantGroups bool
		// wantViewersFSReadAll is checked only when wantGroups is true.
		wantViewersFSReadAll bool
	}{
		{
			name:                 "fresh install applies the template and records the current version",
			wantVersion:          version(current),
			wantGroups:           true,
			wantViewersFSReadAll: true,
		},
		{
			name:     "rerun on the same version is a no-op",
			firstRun: true,
			// Detaching a base policy is the observable probe: a bootstrap run re-attaches
			// it, a version-gated no-op leaves it detached.
			arrange: func(t *testing.T, ctx context.Context, _ kv.Store, svc *acl.AuthService) {
				detachViewersFSReadAll(t, ctx, svc)
			},
			wantVersion:          version(current),
			wantGroups:           true,
			wantViewersFSReadAll: false,
		},
		{
			name: "stored current version skips the bootstrap",
			arrange: func(t *testing.T, ctx context.Context, store kv.Store, _ *acl.AuthService) {
				setRBACVersion(t, ctx, store, current)
			},
			wantVersion:          version(current),
			wantGroups:           false,
			wantViewersFSReadAll: false,
		},
		{
			name:     "version bump re-applies the template once",
			firstRun: true,
			arrange: func(t *testing.T, ctx context.Context, store kv.Store, svc *acl.AuthService) {
				// An older binary left the previous version behind, and an admin detached a base
				// policy.
				setRBACVersion(t, ctx, store, strconv.Itoa(sso.RBACBootstrapVersion-1))
				detachViewersFSReadAll(t, ctx, svc)
			},
			wantVersion:          version(current),
			wantGroups:           true,
			wantViewersFSReadAll: true,
		},
		{
			// A version recorded under the pre-R1 bare key is migrated, so the first boot of
			// this binary does not re-run a bootstrap that already happened.
			name: "version recorded under the legacy key is migrated and does not re-bootstrap",
			arrange: func(t *testing.T, ctx context.Context, store kv.Store, _ *acl.AuthService) {
				setLegacyRBACVersion(t, ctx, store, current)
			},
			wantVersion:       version(current),
			wantLegacyVersion: version(current),
			wantGroups:        false,
		},
		{
			// The documented bump behaviour: existing installations re-apply the template once.
			// The previous binary recorded its version under the legacy key and left no current
			// record, so the bootstrap runs once and records the current version.
			name: "legacy key with an older version re-applies the bootstrap once",
			arrange: func(t *testing.T, ctx context.Context, store kv.Store, _ *acl.AuthService) {
				setLegacyRBACVersion(t, ctx, store, strconv.Itoa(sso.RBACBootstrapVersion-1))
			},
			wantVersion:          version(current),
			wantLegacyVersion:    version(strconv.Itoa(sso.RBACBootstrapVersion - 1)),
			wantGroups:           true,
			wantViewersFSReadAll: true,
		},
		{
			name: "newer stored version is neither re-applied nor downgraded",
			arrange: func(t *testing.T, ctx context.Context, store kv.Store, _ *acl.AuthService) {
				setRBACVersion(t, ctx, store, "99")
			},
			wantVersion:          version("99"),
			wantGroups:           false,
			wantViewersFSReadAll: false,
		},
		{
			name: "unparseable version fails instead of re-bootstrapping",
			arrange: func(t *testing.T, ctx context.Context, store kv.Store, _ *acl.AuthService) {
				setRBACVersion(t, ctx, store, "v1")
			},
			wantErr:     true,
			wantVersion: version("v1"),
			wantGroups:  false,
		},
		{
			name:        "failed bootstrap does not record the version",
			failing:     true,
			wantErr:     true,
			wantVersion: nil,
			wantGroups:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, store, _, svc := newACLService(t)

			if tt.firstRun {
				require.NoError(t, sso.EnsureRBACBase(ctx, svc, store, logging.Dummy()))
			}
			if tt.arrange != nil {
				tt.arrange(t, ctx, store, svc)
			}

			var authService auth.Service = svc
			if tt.failing {
				authService = failingAuthService{Service: svc}
			}
			err := sso.EnsureRBACBase(ctx, authService, store, logging.Dummy())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			gotVersion, ok := storedRBACVersion(t, ctx, store)
			if tt.wantVersion == nil {
				require.False(t, ok, "version key must not be recorded, got %q", gotVersion)
			} else {
				require.True(t, ok, "version key is missing")
				require.Equal(t, *tt.wantVersion, gotVersion)
			}
			if tt.wantLegacyVersion != nil {
				value, err := store.Get(ctx, []byte(model.PartitionKey), []byte(legacyRBACVersionKeyName))
				require.NoError(t, err, "the legacy record must stay readable for a rollback")
				require.Equal(t, *tt.wantLegacyVersion, string(value.Value))
			}

			if !tt.wantGroups {
				// The bootstrap was skipped (or failed before creating groups).
				_, err := svc.GetGroup(ctx, setup.ViewersGroup)
				require.ErrorIs(t, err, auth.ErrNotFound)
				return
			}
			requireBaseGroupsExist(t, ctx, svc)
			require.Equal(t, tt.wantViewersFSReadAll, containsString(viewersPolicies(t, ctx, svc), "FSReadAll"))
		})
	}
}

// TestRBACBootstrapVersionKeyConvention pins the shared template-version key convention: the RBAC
// bootstrap record is built by the same helper (and so lives under the same path and partition)
// as the ACL policy reconciler's record, while the two records stay distinct keys.
func TestRBACBootstrapVersionKeyConvention(t *testing.T) {
	key := sso.RBACBootstrapVersionKey()
	require.Equal(t, acl.TemplateVersionKey(sso.RBACBootstrapVersionKeyName), key)
	require.Equal(t, []byte(model.MetadataKeyPath(sso.RBACBootstrapVersionKeyName)), key, "the record lives under the metadata/ path in the auth partition")
	require.NotEqual(t, acl.TemplateVersionKey(acl.ACLTemplateVersionKeyName), key, "the two template-version records must not collide")
}

// TestEnsureRBACBaseKeepsAdminEdits proves that a version bump does not overwrite built-in
// policies an admin edited.
func TestEnsureRBACBaseKeepsAdminEdits(t *testing.T) {
	ctx, store, _, svc := newACLService(t)
	require.NoError(t, sso.EnsureRBACBase(ctx, svc, store, logging.Dummy()))

	edited := &model.Policy{
		CreatedAt:   time.Now().UTC(),
		DisplayName: "FSReadAll",
		Statement: model.Statements{
			{
				Action:    []string{"fs:ReadObject"},
				Resource:  "arn:lakefs:fs:::repository/edited",
				Effect:    model.StatementEffectAllow,
				Condition: map[string]map[string][]string{},
			},
		},
	}
	require.NoError(t, svc.WritePolicy(ctx, edited, true))

	// An older version makes the next bootstrap run; the edited built-in must survive.
	setRBACVersion(t, ctx, store, strconv.Itoa(sso.RBACBootstrapVersion-1))
	require.NoError(t, sso.EnsureRBACBase(ctx, svc, store, logging.Dummy()))

	got, err := svc.GetPolicy(ctx, "FSReadAll")
	require.NoError(t, err)
	require.Equal(t, edited.Statement, got.Statement)
}
