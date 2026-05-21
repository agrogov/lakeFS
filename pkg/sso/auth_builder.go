package sso

import (
	"context"

	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

// BuildAuthService is the AuthServiceBuilder for the lakefs-sso binary.
// When SSO is enabled it instantiates acl.AuthService in-process (RBAC "simplified" mode),
// which supports multi-user, groups, and ACL-based authorization — all required for OIDC.
// When SSO is disabled it falls back to the upstream default.
func BuildAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, mm *auth.KVMetadataManager) auth.Service {
	ssoCfg := LoadSSOConfig()
	if !ssoCfg.Enabled {
		return auth.NewAuthService(ctx, cfg, logger, kvStore, mm)
	}

	baseAuthCfg := cfg.AuthConfig().GetBaseAuthConfig()
	secretStore := crypt.NewSecretStore([]byte(baseAuthCfg.Encrypt.SecretKey))
	cacheConf := authparams.ServiceCache(baseAuthCfg.Cache)

	// Construct the concrete *acl.AuthService. Must happen before wrapping because
	// acl.SetupACLServer requires the concrete type, not the auth.Service interface.
	aclService := acl.NewAuthService(kvStore, secretStore, cacheConf)

	// Bootstrap the 4 default ACL groups (Admins/Supers/Writers/Readers) on first run.
	// Idempotent: safe to call on every startup.
	if err := acl.SetupACLServer(ctx, aclService); err != nil {
		logger.WithError(err).Warn("acl setup skipped (may already exist)")
	}

	svc := auth.NewMonitoredAuthService(aclService)
	// Cache so BuildAuthenticationService can wire it into NativeOIDCService for group sync.
	// run.go calls authServiceBuilder before authenticationServiceBuilder, so this is always
	// populated by the time BuildAuthenticationService runs.
	builtAuthService = svc
	return svc
}

// builtAuthService holds the auth.Service constructed by BuildAuthService so that
// BuildAuthenticationService can pass it to NativeOIDCService without changing the
// AuthenticationServiceBuilder function signature (and therefore run.go / hooks.go).
var builtAuthService auth.Service
