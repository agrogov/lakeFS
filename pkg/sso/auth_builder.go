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

// BuildAuthService is the AuthServiceBuilder for the lakefs-sso binary.
// When SSO is enabled it instantiates acl.AuthService in-process (RBAC "simplified" mode),
// which supports multi-user, groups, and ACL-based authorization — all required for OIDC.
// When SSO is disabled it falls back to the upstream default.
func BuildAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, mm *auth.KVMetadataManager) auth.Service {
	ssoCfg := LoadSSOConfig()
	if !ssoCfg.Enabled {
		return auth.NewAuthService(ctx, cfg, logger, kvStore, mm)
	}

	ensureSimplifiedRBAC(cfg.AuthConfig().GetAuthUIConfig(), logger)

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

	// Seed the optional break-glass local admin (after the groups it joins exist).
	if _, err := EnsureBreakGlassAdmin(ctx, aclService, logger); err != nil {
		logger.WithError(err).Error("break-glass admin bootstrap failed")
	}

	svc := authServiceWithProfiles{
		Service:        auth.NewMonitoredAuthService(aclService),
		profileUpdater: aclService,
	}
	builtAuthServiceMu.Lock()
	builtAuthService = svc
	builtAuthServiceMu.Unlock()
	return svc
}

// ensureSimplifiedRBAC forces auth.ui_config.rbac to "simplified", the only mode acl.AuthService
// supports. The default ("none") makes the UI hide users/groups behind "Unlock Access Control", and
// "external"/"internal" expect a policy-capable remote auth service. The config is shared by pointer,
// so setup and the API controller (/setup_lakefs login_config) see the corrected value.
func ensureSimplifiedRBAC(uiCfg *config.AuthUIConfig, logger logging.Logger) {
	if uiCfg.RBAC == config.AuthRBACSimplified {
		return
	}
	if uiCfg.RBAC != config.AuthRBACNone && uiCfg.RBAC != "" {
		logger.WithField("rbac", uiCfg.RBAC).Warn("sso: auth.ui_config.rbac is not supported with the ACL auth service, using \"simplified\"")
	}
	uiCfg.RBAC = config.AuthRBACSimplified
}
