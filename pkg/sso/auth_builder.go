package sso

import (
	"context"
	"sync"

	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

// builtAuthService holds the auth.Service constructed by BuildAuthService so that
// BuildAuthenticationService can pass it to NativeOIDCService for group sync without
// changing the AuthenticationServiceBuilder function signature (and therefore run.go /
// hooks.go). In production, run.go calls authServiceBuilder before
// authenticationServiceBuilder, so the value is always populated in time.
// The mutex makes reads and writes safe if tests exercise these builders concurrently.
var (
	builtAuthServiceMu sync.RWMutex
	builtAuthService   auth.Service
)

// authServiceWithProfiles is the built auth.Service together with the ACL profile-update
// API. The generated monitored-service wrapper only forwards auth.Service methods.
type authServiceWithProfiles struct {
	auth.Service
	profileUpdater
}

// rbacAuthService is the ACL KV service with policy management exposed: the engine and
// CRUD are the same, only the capability flag differs.
type rbacAuthService struct{ *acl.AuthService }

func (rbacAuthService) IsAdvancedAuth() bool { return true }

// BuildAuthService is the AuthServiceBuilder for the lakefs-sso binary.
// When SSO is enabled it instantiates acl.AuthService in-process: "simplified" mode for
// the ACL dropdown, or "internal" mode wrapped as rbacAuthService for policy-based RBAC.
// When SSO is disabled it falls back to the upstream default.
func BuildAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, mm *auth.KVMetadataManager) auth.Service {
	ssoCfg := LoadSSOConfig()
	if !ssoCfg.Enabled {
		return auth.NewAuthService(ctx, cfg, logger, kvStore, mm)
	}

	mode := resolveAuthMode(cfg.AuthConfig().GetAuthUIConfig(), logger)

	baseAuthCfg := cfg.AuthConfig().GetBaseAuthConfig()
	secretStore := crypt.NewSecretStore([]byte(baseAuthCfg.Encrypt.SecretKey))
	cacheConf := authparams.ServiceCache(baseAuthCfg.Cache)

	// Construct the concrete *acl.AuthService. Must happen before wrapping because
	// acl.SetupACLServer requires the concrete type, not the auth.Service interface.
	aclService := acl.NewAuthService(kvStore, secretStore, cacheConf)

	// Bootstrap the 4 default ACL groups (Admins/Supers/Writers/Readers) when missing,
	// including on installations set up with basic auth before SSO was enabled.
	// Idempotent: safe to call on every startup.
	if err := EnsureACLBaseGroups(ctx, aclService); err != nil {
		logger.WithError(err).Warn("acl base group bootstrap failed")
	}

	// Keep ACL-derived policies in step with the current templates after template fixes.
	// Guarded by a template version, so it rewrites at most once per version.
	if err := acl.ReconcileACLPolicies(ctx, aclService, logger); err != nil {
		logger.WithError(err).Warn("acl policy reconciliation failed")
	}

	svc := auth.Service(aclService)
	if mode == config.AuthRBACInternal {
		// Policy RBAC needs the RBAC base groups/policies in addition to the ACL groups,
		// then the same service exposed with IsAdvancedAuth()==true for the UI and logs.
		if err := EnsureRBACBase(ctx, aclService, kvStore, logger); err != nil {
			logger.WithError(err).Error("rbac base bootstrap failed")
		}
		svc = rbacAuthService{aclService}
	}

	// Seed the optional break-glass local admin after the groups it joins exist. Admins is
	// the same group in both models; in internal mode it holds the RBAC admin policies, so
	// the break-glass user needs no other group.
	if _, err := EnsureBreakGlassAdmin(ctx, aclService, logger); err != nil {
		logger.WithError(err).Error("break-glass admin bootstrap failed")
	}

	svc = auth.NewMonitoredAuthService(svc)
	// Keep the profile-update API reachable through the wrappers: oidc_provider asserts
	// the built service to profileUpdater, and the RBAC capability flag travels inside.
	svc = authServiceWithProfiles{
		Service:        svc,
		profileUpdater: aclService,
	}
	builtAuthServiceMu.Lock()
	builtAuthService = svc
	builtAuthServiceMu.Unlock()
	return svc
}

// resolveAuthMode normalizes auth.ui_config.rbac for the in-process ACL auth service and
// returns the effective mode. Empty and "none" mean the SSO default, "simplified"; "internal"
// enables policy RBAC; "external" is rejected because no remote auth service exists in this
// fork. Unknown values fall back to "simplified" as well. The config is shared by pointer, so
// setup and the API controller (/setup_lakefs login_config) see the normalized value.
func resolveAuthMode(uiCfg *config.AuthUIConfig, logger logging.Logger) string {
	switch uiCfg.RBAC {
	case "", config.AuthRBACNone:
		uiCfg.RBAC = config.AuthRBACSimplified
	case config.AuthRBACSimplified, config.AuthRBACInternal:
		// Supported as-is.
	case config.AuthRBACExternal:
		logger.WithField("rbac", uiCfg.RBAC).Warn("sso: auth.ui_config.rbac \"external\" is not supported, using \"simplified\"")
		uiCfg.RBAC = config.AuthRBACSimplified
	default:
		logger.WithField("rbac", uiCfg.RBAC).Warn("sso: unknown auth.ui_config.rbac, using \"simplified\"")
		uiCfg.RBAC = config.AuthRBACSimplified
	}
	return uiCfg.RBAC
}
