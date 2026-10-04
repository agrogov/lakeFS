package sso_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	logging "github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
)

var baseGroups = []string{acl.AdminsGroup, acl.SupersGroup, acl.WritersGroup, acl.ReadersGroup}

func newACLService(t *testing.T) (context.Context, kv.Store, crypt.SecretStore, *acl.AuthService) {
	t.Helper()
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	secrets := crypt.NewSecretStore([]byte("test-secret"))
	return ctx, store, secrets, acl.NewAuthService(store, secrets, authparams.ServiceCache{Enabled: false})
}

// markBasicAuthSetup does what "lakefs setup" does on a basic-auth installation: it
// records the setup timestamp that the ACL service also treats as "already initialized".
func markBasicAuthSetup(t *testing.T, ctx context.Context, store kv.Store) {
	t.Helper()
	mm := auth.NewKVMetadataManager("test", "install-id", "mem", store)
	if err := mm.UpdateSetupTimestamp(ctx, time.Now(), auth.SetupAuthTypeKeyPrefix); err != nil {
		t.Fatalf("UpdateSetupTimestamp: %v", err)
	}
}

func requireBaseGroups(t *testing.T, ctx context.Context, svc *acl.AuthService) {
	t.Helper()
	for _, g := range baseGroups {
		if _, err := svc.GetGroup(ctx, g); err != nil {
			t.Errorf("group %s: %v", g, err)
		}
	}
}

func TestEnsureACLBaseGroups_FreshInstall(t *testing.T) {
	ctx, _, _, svc := newACLService(t)
	if err := sso.EnsureACLBaseGroups(ctx, svc); err != nil {
		t.Fatal(err)
	}
	requireBaseGroups(t, ctx, svc)
	// Idempotent.
	if err := sso.EnsureACLBaseGroups(ctx, svc); err != nil {
		t.Fatalf("second call: %v", err)
	}
}

// TestEnsureACLBaseGroups_AfterBasicAuthSetup is the regression test for sso-migrate and
// server start on an installation set up before SSO: acl.SetupACLServer alone creates no
// groups there.
func TestEnsureACLBaseGroups_AfterBasicAuthSetup(t *testing.T) {
	ctx, store, _, svc := newACLService(t)
	markBasicAuthSetup(t, ctx, store)

	if err := acl.SetupACLServer(ctx, svc); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetGroup(ctx, acl.AdminsGroup); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("precondition: expected SetupACLServer to skip group creation, got err=%v", err)
	}

	if err := sso.EnsureACLBaseGroups(ctx, svc); err != nil {
		t.Fatal(err)
	}
	requireBaseGroups(t, ctx, svc)
}

func TestMigrateBasicToACL_AfterBasicAuthSetup(t *testing.T) {
	ctx, store, secrets, svc := newACLService(t)
	logger := logging.Dummy()

	// An installation created by "lakefs setup": admin lives in the basic-auth partition.
	basic := auth.NewBasicAuthService(store, secrets, authparams.ServiceCache{Enabled: false}, logger)
	if _, err := basic.CreateUser(ctx, &model.User{Username: "admin", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	const keyID, secret = "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	if _, err := basic.AddCredentials(ctx, "admin", keyID, secret); err != nil {
		t.Fatal(err)
	}
	markBasicAuthSetup(t, ctx, store)

	for i := 0; i < 2; i++ { // second run proves idempotency
		if err := sso.MigrateBasicToACL(ctx, store, secrets, logger); err != nil {
			t.Fatalf("migrate run %d: %v", i+1, err)
		}
	}

	if _, err := svc.GetUser(ctx, "admin"); err != nil {
		t.Fatalf("admin not in ACL partition: %v", err)
	}
	if _, err := svc.GetCredentials(ctx, keyID); err != nil {
		t.Fatalf("credentials not migrated: %v", err)
	}
	groups, _, err := svc.ListUserGroups(ctx, "admin", &model.PaginationParams{Amount: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].DisplayName != acl.AdminsGroup {
		t.Fatalf("admin groups = %v, want [Admins]", groups)
	}
}
