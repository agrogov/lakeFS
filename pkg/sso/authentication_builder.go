package sso

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/config"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

// BuildAuthenticationService is the AuthenticationServiceBuilder for the lakefs-sso binary.
// When SSO is enabled it returns a NativeOIDCService; otherwise it falls back to the
// upstream default (DummyService or APIService when authentication_api.endpoint is set).
func BuildAuthenticationService(ctx context.Context, cfg config.Config, logger logging.Logger, authService auth.Service) (authentication.Service, error) {
	ssoCfg := LoadSSOConfig()
	if !ssoCfg.Enabled {
		return authentication.NewAuthenticationService(ctx, cfg, logger)
	}

	svc, err := NewNativeOIDCService(ctx, ssoCfg, authService, logger)
	if err != nil {
		return nil, err
	}
	logger.WithField("issuer", ssoCfg.IssuerURL).Info("native OIDC service initialized")
	return svc, nil
}
