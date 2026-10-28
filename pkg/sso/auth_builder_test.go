package sso

import (
	"testing"

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
