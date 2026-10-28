package gateway

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/local"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
)

const bulkDeleteConditionRepo = "bulk-delete-condition-repo"

// conditionPolicyGatewayAuthService authorizes from a fixed policy set, evaluating conditions from
// the request context exactly like the in-process ACL service does.
type conditionPolicyGatewayAuthService struct {
	policies []*model.Policy
}

func (s *conditionPolicyGatewayAuthService) GetCredentials(_ context.Context, accessKey string) (*model.Credential, error) {
	return &model.Credential{
		Username:       "user1",
		BaseCredential: model.BaseCredential{AccessKeyID: accessKey},
	}, nil
}

func (s *conditionPolicyGatewayAuthService) GetUser(_ context.Context, username string) (*model.User, error) {
	return &model.User{Username: username}, nil
}

func (s *conditionPolicyGatewayAuthService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	missing := &auth.MissingPermissions{}
	if auth.CheckPermissions(ctx, req.RequiredPermissions, req.Username, s.policies, missing) != auth.CheckAllow {
		return &auth.AuthorizationResponse{
			Allowed: false,
			Error:   auth.ErrInsufficientPermissions,
		}, nil
	}
	return &auth.AuthorizationResponse{Allowed: true}, nil
}

func (s *conditionPolicyGatewayAuthService) ListEffectivePolicies(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return s.policies, nil, nil
}

func newBulkDeleteConditionHandler(t *testing.T, svc auth.GatewayService) http.Handler {
	t.Helper()
	ctx := t.Context()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)

	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	testutil.MustDo(t, "config", err)
	cfg.Committed.LocalCache.Dir = path.Join(t.TempDir(), "cache")

	store, err := kv.Open(ctx, kvparams.Config{Type: "local", Local: &kvparams.Local{Path: "memory:gateway_deleteobjects_condition", PrefetchSize: 256}})
	testutil.MustDo(t, "open kv store", err)
	t.Cleanup(func() { store.Close() })

	c, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      store,
		PathProvider: upload.DefaultPathProvider,
	})
	testutil.MustDo(t, "build catalog", err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.CreateRepository(ctx, bulkDeleteConditionRepo, "", "mem://"+bulkDeleteConditionRepo, "main", false)
	testutil.MustDo(t, "create repository", err)

	return NewHandler(
		"us-east-1",
		c,
		multipart.NewTracker(store),
		testutil.NewBlockAdapterByType(t, block.BlockstoreTypeMem),
		svc,
		[]string{"example.com"},
		&stats.NullCollector{},
		upload.DefaultPathProvider,
		nil,
		config.DefaultLoggingAuditLogLevel,
		false,
		false,
		false,
	)
}

// TestDeleteObjectsPolicyConditions verifies that the per-object authorization performed by S3
// multi-object delete evaluates policy conditions with the request's condition context. Without it
// a conditional deny is skipped (the condition evaluates against a nil context), so the objects are
// deleted even though the policy denies the delete from that client IP.
func TestDeleteObjectsPolicyConditions(t *testing.T) {
	const allowedIP = "203.0.113.5"

	ipCondition := map[string]map[string][]string{
		auth.OperatorNameIpAddress: {"SourceIp": {allowedIP + "/32"}},
	}
	svc := &conditionPolicyGatewayAuthService{
		policies: []*model.Policy{{
			DisplayName: "delete-object-condition-policy",
			Statement: model.Statements{
				{
					Effect:   model.StatementEffectAllow,
					Action:   []string{permissions.DeleteObjectAction},
					Resource: permissions.ObjectArn(bulkDeleteConditionRepo, "*"),
				},
				{
					Effect:    model.StatementEffectDeny,
					Action:    []string{permissions.DeleteObjectAction},
					Resource:  permissions.ObjectArn(bulkDeleteConditionRepo, "*"),
					Condition: ipCondition,
				},
			},
		}},
	}
	handler := newBulkDeleteConditionHandler(t, svc)

	tests := []struct {
		name     string
		clientIP string
		wantDeny bool
	}{
		{name: "conditional deny from matching IP blocks the delete", clientIP: allowedIP, wantDeny: true},
		{name: "conditional deny from another IP does not apply", clientIP: "198.51.100.7", wantDeny: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := `<Delete><Object><Key>main/file.txt</Key></Object></Delete>`
			req := httptest.NewRequest(http.MethodPost, "/"+bulkDeleteConditionRepo+"/?delete", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/xml")
			req.RemoteAddr = net.JoinHostPort(tt.clientIP, "1234")
			req = req.WithContext(auth.WithUser(req.Context(), &model.User{Username: "user1"}))

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			require.Equal(t, http.StatusOK, recorder.Code)
			if tt.wantDeny {
				require.Contains(t, recorder.Body.String(), "AccessDenied")
				require.NotContains(t, recorder.Body.String(), "<Deleted>")
			} else {
				require.NotContains(t, recorder.Body.String(), "AccessDenied")
				require.Contains(t, recorder.Body.String(), "<Deleted>")
			}
		})
	}
}
