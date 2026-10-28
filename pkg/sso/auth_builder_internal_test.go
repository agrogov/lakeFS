package sso

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestEnsureSimplifiedRBAC(t *testing.T) {
	for _, rbac := range []string{"", config.AuthRBACNone, config.AuthRBACSimplified, config.AuthRBACExternal, config.AuthRBACInternal} {
		t.Run(rbac, func(t *testing.T) {
			uiCfg := &config.AuthUIConfig{RBAC: rbac}
			ensureSimplifiedRBAC(uiCfg, logging.ContextUnavailable())
			if uiCfg.RBAC != config.AuthRBACSimplified {
				t.Fatalf("rbac = %q, want %q", uiCfg.RBAC, config.AuthRBACSimplified)
			}
		})
	}
}
