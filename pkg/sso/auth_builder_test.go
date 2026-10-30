package sso

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestResolveAuthMode(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty defaults to simplified", "", config.AuthRBACSimplified},
		{"none defaults to simplified", config.AuthRBACNone, config.AuthRBACSimplified},
		{"simplified", config.AuthRBACSimplified, config.AuthRBACSimplified},
		{"internal", config.AuthRBACInternal, config.AuthRBACInternal},
		{"external falls back to simplified", config.AuthRBACExternal, config.AuthRBACSimplified},
		{"unknown falls back to simplified", "unknown", config.AuthRBACSimplified},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uiCfg := &config.AuthUIConfig{RBAC: tt.in}
			if got := resolveAuthMode(uiCfg, logging.ContextUnavailable()); got != tt.want {
				t.Errorf("resolveAuthMode() = %q, want %q", got, tt.want)
			}
			// The config is shared by pointer, so the caller (setup, API controller) must see
			// the same normalized value that was returned.
			if uiCfg.RBAC != tt.want {
				t.Errorf("uiCfg.RBAC = %q, want %q", uiCfg.RBAC, tt.want)
			}
		})
	}
}

// setSSOEnabled sets the sso: section that LoadSSOConfig reads. The required fields are filled
// in so that enabling SSO does not trip the "missing required config" panic.
func setSSOEnabled(t *testing.T, enabled bool) {
	t.Helper()
	viper.Set("sso.enabled", enabled)
	viper.Set("sso.client_id", "test-client-id")
	viper.Set("sso.client_secret", "test-client-secret")
	viper.Set("sso.issuer_url", "https://issuer.example.com")
	viper.Set("sso.callback_base_url", "https://lakefs.example.com")
	t.Cleanup(func() {
		viper.Set("sso.enabled", false)
		for _, k := range []string{"sso.client_id", "sso.client_secret", "sso.issuer_url", "sso.callback_base_url"} {
			viper.Set(k, "")
		}
	})
}

// TestResolveAuthMode_SSOEnabled exercises the exported resolver that `lakefs setup` and
// `lakefs superuser` call: with SSO enabled "none" and "external" are not what the in-process
// ACL service runs with (GAP-03, GAP-04).
func TestResolveAuthMode_SSOEnabled(t *testing.T) {
	setSSOEnabled(t, true)
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty defaults to simplified", "", config.AuthRBACSimplified},
		{"none defaults to simplified", config.AuthRBACNone, config.AuthRBACSimplified},
		{"simplified", config.AuthRBACSimplified, config.AuthRBACSimplified},
		{"internal", config.AuthRBACInternal, config.AuthRBACInternal},
		{"external falls back to simplified", config.AuthRBACExternal, config.AuthRBACSimplified},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.ConfigImpl{}
			cfg.Auth.GetAuthUIConfig().RBAC = tt.in
			if got := ResolveAuthMode(cfg, logging.ContextUnavailable()); got != tt.want {
				t.Errorf("ResolveAuthMode() = %q, want %q", got, tt.want)
			}
			if got := cfg.Auth.GetAuthUIConfig().RBAC; got != tt.want {
				t.Errorf("config RBAC = %q, want normalized %q", got, tt.want)
			}
		})
	}
}

// TestResolveAuthMode_SSODisabledKeepsConfiguredMode pins the other half of the contract: with
// SSO disabled the upstream auth service builds from the raw value, so an external auth API must
// stay external and setup/superuser keep their upstream behaviour.
func TestResolveAuthMode_SSODisabledKeepsConfiguredMode(t *testing.T) {
	setSSOEnabled(t, false)
	for _, mode := range []string{config.AuthRBACNone, config.AuthRBACSimplified, config.AuthRBACInternal, config.AuthRBACExternal} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.ConfigImpl{}
			cfg.Auth.GetAuthUIConfig().RBAC = mode
			if got := ResolveAuthMode(cfg, logging.ContextUnavailable()); got != mode {
				t.Errorf("ResolveAuthMode() = %q, want the configured %q", got, mode)
			}
			if got := cfg.Auth.GetAuthUIConfig().RBAC; got != mode {
				t.Errorf("config RBAC = %q, want unchanged %q", got, mode)
			}
		})
	}
}
