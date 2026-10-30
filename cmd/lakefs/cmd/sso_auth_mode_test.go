package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/sso"
)

// useSSO wires the process like cmd/lakefs-sso/main.go does: the sso: section is readable and
// sso.BuildAuthService is the auth-service builder. This is the configuration the conformance
// review found untested (GAP-02): `setup` and `superuser` running through the real builder.
func useSSO(t *testing.T) {
	t.Helper()
	for k, v := range map[string]any{
		"sso.enabled":           true,
		"sso.client_id":         "test-client-id",
		"sso.client_secret":     "test-client-secret",
		"sso.issuer_url":        "https://issuer.example.com",
		"sso.callback_base_url": "https://lakefs.example.com",
	} {
		viper.Set(k, v)
	}
	// Ensure the optional break-glass admin is not seeded from leftover test state.
	viper.Set("installation.user_name", "")
	viper.Set("installation.access_key_id", "")
	viper.Set("installation.secret_access_key", "")

	prev := authServiceBuilder
	SetAuthServiceBuilder(sso.BuildAuthService)
	t.Cleanup(func() {
		SetAuthServiceBuilder(prev)
		viper.Set("sso.enabled", false)
		for _, k := range []string{"sso.client_id", "sso.client_secret", "sso.issuer_url", "sso.callback_base_url"} {
			viper.Set(k, "")
		}
	})
}

// useNoSSO pins SSO to disabled, so the resolvers must keep the configured auth mode unchanged.
func useNoSSO(t *testing.T) {
	t.Helper()
	viper.Set("sso.enabled", false)
}

func newSSOTestConfig(mode string) *config.ConfigImpl {
	cfg := &config.ConfigImpl{}
	cfg.Database.Type = "mem"
	cfg.Auth.GetAuthUIConfig().RBAC = mode
	cfg.Auth.GetBaseAuthConfig().Encrypt.SecretKey = "test-secret-key"
	return cfg
}

func requireGroup(t *testing.T, ctx context.Context, svc auth.Service, username, wanted string) {
	t.Helper()
	groups, _, err := svc.ListUserGroups(ctx, username, &model.PaginationParams{Amount: 100})
	if err != nil {
		t.Fatalf("ListUserGroups(%s): %v", username, err)
	}
	for _, g := range groups {
		if g.DisplayName == wanted {
			return
		}
	}
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.DisplayName)
	}
	t.Fatalf("user %s groups = %v, want %q", username, names, wanted)
}

func requireAdminRights(t *testing.T, ctx context.Context, svc auth.Service, username string, creds *model.Credential) {
	t.Helper()
	if _, err := svc.GetCredentials(ctx, creds.AccessKeyID); err != nil {
		t.Fatalf("credentials %s do not authenticate: %v", creds.AccessKeyID, err)
	}
	resp, err := svc.Authorize(ctx, &auth.AuthorizationRequest{
		Username: username,
		RequiredPermissions: permissions.Node{
			Permission: permissions.Permission{
				Action:   permissions.ListUsersAction,
				Resource: permissions.All,
			},
		},
	})
	if err != nil {
		t.Fatalf("Authorize(%s): %v", username, err)
	}
	if !resp.Allowed {
		t.Fatalf("%s (%v) is not allowed %s: %v", username, resp, permissions.ListUsersAction, resp.Error)
	}
}

// TestSetupLakeFS_ThroughSSOBuilder_FreshKV is the regression test for GAP-01: on a fresh KV with
// SSO enabled, the auth-service builder must leave the installation uninitialized so that
// `lakefs setup` without --no-check creates the first admin instead of reporting "Setup is
// already complete". It runs the builder first, exactly like setupCmd does.
func TestSetupLakeFS_ThroughSSOBuilder_FreshKV(t *testing.T) {
	for _, mode := range []string{config.AuthRBACNone, config.AuthRBACSimplified, config.AuthRBACInternal} {
		t.Run(mode, func(t *testing.T) {
			useSSO(t)
			ctx := context.Background()
			store := kvtest.GetStore(ctx, t)
			cfg := newSSOTestConfig(mode)
			logger := logging.ContextUnavailable()
			metadataManager := auth.NewKVMetadataManager("test", "", "mem", store)

			authService := authServiceBuilder(ctx, cfg, logger, store, metadataManager)
			if authService == nil {
				t.Fatal("auth service builder returned nil")
			}

			initialized, err := metadataManager.IsInitialized(ctx)
			if err != nil {
				t.Fatalf("IsInitialized after builder: %v", err)
			}
			if initialized {
				t.Fatal("auth-service builder marked the fresh KV as initialized")
			}

			creds, err := setupLakeFS(ctx, cfg, metadataManager, authService, "root", "", "", false)
			if err != nil {
				t.Fatalf("setupLakeFS: %v", err)
			}
			if creds == nil {
				t.Fatal("setupLakeFS treated a fresh KV as already complete (GAP-01)")
			}

			initialized, err = metadataManager.IsInitialized(ctx)
			if err != nil {
				t.Fatalf("IsInitialized after setup: %v", err)
			}
			if !initialized {
				t.Fatal("setup did not record the installation as initialized")
			}

			requireGroup(t, ctx, authService, "root", acl.AdminsGroup)
			requireAdminRights(t, ctx, authService, "root", creds)

			// A second setup on an initialized KV must still refuse: the guard is not weakened.
			again, err := setupLakeFS(ctx, cfg, metadataManager, authService, "root2", "", "", false)
			if err != nil {
				t.Fatalf("second setupLakeFS: %v", err)
			}
			if again != nil {
				t.Fatal("second setup on an initialized KV returned credentials; it must refuse")
			}
		})
	}
}

// TestCreateSuperuser_UnderSSO covers GAP-03 (rbac: none must put the user in Admins like the
// other modes) and the SSO half of GAP-04 (rbac: external runs as simplified, so an admin can be
// created instead of the command exiting early).
func TestCreateSuperuser_UnderSSO(t *testing.T) {
	for _, mode := range []string{config.AuthRBACNone, config.AuthRBACSimplified, config.AuthRBACInternal, config.AuthRBACExternal} {
		t.Run(mode, func(t *testing.T) {
			useSSO(t)
			ctx := context.Background()
			store := kvtest.GetStore(ctx, t)
			cfg := newSSOTestConfig(mode)
			logger := logging.ContextUnavailable()
			metadataManager := auth.NewKVMetadataManager("test", "", "mem", store)

			creds, err := createSuperuser(ctx, cfg, logger, store, metadataManager, "su1", "", "")
			if err != nil {
				t.Fatalf("createSuperuser: %v", err)
			}
			if creds == nil {
				t.Fatal("createSuperuser returned no credentials")
			}

			// Build the service again to inspect what the command created (the bootstraps are
			// idempotent).
			authService := authServiceBuilder(ctx, cfg, logger, store, metadataManager)
			requireGroup(t, ctx, authService, "su1", acl.AdminsGroup)
			requireAdminRights(t, ctx, authService, "su1", creds)
		})
	}
}

// TestSetupExternalAuthMode pins the decided GAP-04 contract: `rbac: external` with SSO enabled
// is served in-process as `simplified` (RBAC_DESIGN.md §4.1/§4.2), so setup creates the local
// admin; without SSO it is a genuinely external auth service and setup does nothing.
func TestSetupExternalAuthMode(t *testing.T) {
	t.Run("with_sso_falls_back_to_simplified", func(t *testing.T) {
		useSSO(t)
		cfg := newSSOTestConfig(config.AuthRBACExternal)
		if !setupCreatesLocalAdmin(cfg, logging.ContextUnavailable()) {
			t.Fatal("setup must create the local admin when rbac external runs as simplified under SSO")
		}
		if got := cfg.Auth.GetAuthUIConfig().RBAC; got != config.AuthRBACSimplified {
			t.Fatalf("config RBAC = %q, want normalized %q", got, config.AuthRBACSimplified)
		}
	})

	t.Run("without_sso_does_nothing", func(t *testing.T) {
		useNoSSO(t)
		cfg := newSSOTestConfig(config.AuthRBACExternal)
		if setupCreatesLocalAdmin(cfg, logging.ContextUnavailable()) {
			t.Fatal("setup must do nothing when a remote external auth service manages its users")
		}
	})
}

// TestCreateSuperuserExternalWithoutSSO pins the other half of the GAP-04 contract: with a
// genuinely external auth service, superuser must keep refusing (upstream behaviour) instead of
// creating a local user.
func TestCreateSuperuserExternalWithoutSSO(t *testing.T) {
	useNoSSO(t)
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	cfg := newSSOTestConfig(config.AuthRBACExternal)
	metadataManager := auth.NewKVMetadataManager("test", "", "mem", store)

	_, err := createSuperuser(ctx, cfg, logging.ContextUnavailable(), store, metadataManager, "su1", "", "")
	if !errors.Is(err, errExternalAuth) {
		t.Fatalf("createSuperuser error = %v, want errExternalAuth", err)
	}
}

// TestSetupLakeFS_BreakGlassAdminAlreadySeeded covers the interaction between the SSO builder and
// run's installation.* auto-setup on a fresh KV: the builder seeds the break-glass admin first, so
// setup must complete instead of failing with "already exists" (which aborted startup). It also
// proves the seeding still works - break-glass remains a viable way in on a fresh installation.
func TestSetupLakeFS_BreakGlassAdminAlreadySeeded(t *testing.T) {
	useSSO(t)
	viper.Set("installation.user_name", "admin")
	viper.Set("installation.access_key_id", "AKIAINSTALLATION0001")
	viper.Set("installation.secret_access_key", "installation-secret")
	t.Cleanup(func() {
		viper.Set("installation.user_name", "")
		viper.Set("installation.access_key_id", "")
		viper.Set("installation.secret_access_key", "")
	})

	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	cfg := newSSOTestConfig(config.AuthRBACSimplified)
	logger := logging.ContextUnavailable()
	metadataManager := auth.NewKVMetadataManager("test", "", "mem", store)

	// The builder seeds the break-glass admin from installation.*, exactly as run does before
	// calling setupLakeFS for the installation.* auto-setup.
	authService := authServiceBuilder(ctx, cfg, logger, store, metadataManager)

	creds, err := setupLakeFS(ctx, cfg, metadataManager, authService, "admin", "AKIAINSTALLATION0001", "installation-secret", false)
	if err != nil {
		t.Fatalf("setupLakeFS with a builder-seeded break-glass admin: %v", err)
	}
	if creds == nil {
		t.Fatal("expected credentials for the already-seeded admin")
	}
	requireGroup(t, ctx, authService, "admin", acl.AdminsGroup)
	requireAdminRights(t, ctx, authService, "admin", creds)

	initialized, err := metadataManager.IsInitialized(ctx)
	if err != nil || !initialized {
		t.Fatalf("IsInitialized = %v, %v; want true, nil", initialized, err)
	}
}
