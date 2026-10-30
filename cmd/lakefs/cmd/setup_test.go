package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
)

// TestSetupWithAuthService_RoutesThroughBuilder covers the setup.go half of GAP-02: `lakefs
// setup` must obtain its auth service from authServiceBuilder (sso.BuildAuthService in the
// lakefs-sso binary), not from auth.NewAuthService. Reverting that line makes the recorded
// builder call go missing and the ACL-store assertions fail.
func TestSetupWithAuthService_RoutesThroughBuilder(t *testing.T) {
	for _, mode := range []string{config.AuthRBACNone, config.AuthRBACSimplified, config.AuthRBACInternal} {
		t.Run(mode, func(t *testing.T) {
			useSSO(t)
			called := false
			SetAuthServiceBuilder(func(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *auth.KVMetadataManager) auth.Service {
				called = true
				return sso.BuildAuthService(ctx, cfg, logger, kvStore, metadataManager)
			})

			ctx := context.Background()
			store := kvtest.GetStore(ctx, t)
			cfg := newSSOTestConfig(mode)
			// auth.NewAuthService - the factory the mutant falls back to - tries to reach an
			// auth API before setup ever creates a user. Skip its health check so the mutant
			// fails deterministically instead of retrying a non-existent endpoint.
			cfg.Auth.GetBaseAuthConfig().API.SkipHealthCheck = true
			logger := logging.Dummy()
			metadataManager := auth.NewKVMetadataManager("test", "", "mem", store)

			creds, err := setupWithAuthService(ctx, cfg, logger, store, metadataManager, "root", "", "", false)

			require.True(t, called, "setup did not obtain the auth service from authServiceBuilder")
			require.NoError(t, err)
			require.NotNil(t, creds)

			// The admin must live in the ACL partition, which only the SSO builder provisions.
			aclSvc := acl.NewAuthService(store, crypt.NewSecretStore([]byte("test-secret-key")), authparams.ServiceCache{Enabled: false})
			_, err = aclSvc.GetUser(ctx, "root")
			require.NoError(t, err, "setup did not create the admin through the ACL/RBAC service")
			groups, _, err := aclSvc.ListUserGroups(ctx, "root", &model.PaginationParams{Amount: -1})
			require.NoError(t, err)
			groupNames := make([]string, 0, len(groups))
			for _, group := range groups {
				groupNames = append(groupNames, group.DisplayName)
			}
			require.Contains(t, groupNames, acl.AdminsGroup)

			if mode == config.AuthRBACInternal {
				for _, group := range []string{"SuperUsers", "Developers", "Viewers"} {
					_, err := aclSvc.GetGroup(ctx, group)
					require.NoErrorf(t, err, "setup through the internal-mode builder must bootstrap RBAC group %s", group)
				}
			}

			initialized, err := metadataManager.IsInitialized(ctx)
			require.NoError(t, err)
			require.True(t, initialized, "setup did not record the installation as initialized")
		})
	}
}
