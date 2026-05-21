package sso

import (
	"context"

	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/config"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

// BuildAuthenticationService is the AuthenticationServiceBuilder for the lakefs-sso binary.
// When SSO is enabled it returns a NativeOIDCService; otherwise it falls back to the
// upstream default (DummyService or APIService when authentication_api.endpoint is set).
//
// builtAuthService (set by BuildAuthService) is passed in for group sync. run.go calls
// authServiceBuilder before authenticationServiceBuilder, guaranteeing it is populated.
func BuildAuthenticationService(ctx context.Context, cfg config.Config, logger logging.Logger) (authentication.Service, error) {
	ssoCfg := LoadSSOConfig()
	if !ssoCfg.Enabled {
		return authentication.NewAuthenticationService(ctx, cfg, logger)
	}

	builtAuthServiceMu.RLock()
	cachedSvc := builtAuthService
	builtAuthServiceMu.RUnlock()

	logoutRedirectURL := cfg.AuthConfig().GetBaseAuthConfig().LogoutRedirectURL
	svc, err := NewNativeOIDCService(ctx, ssoCfg, cachedSvc, logger, logoutRedirectURL)
	if err != nil {
		return nil, err
	}
	logger.WithField("issuer", ssoCfg.IssuerURL).Info("native OIDC service initialized")
	return svc, nil
}
