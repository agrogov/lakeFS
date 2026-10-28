package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/gateway/operations"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/local"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	authorizeFieldsRepoName = "authorize-fields-repo"
	authorizeFieldsTeam     = "analytics"
)

// recordingGatewayAuthService records the authorization request it is handed, so a test can
// inspect the condition fields the gateway resolved.
type recordingGatewayAuthService struct {
	request *auth.AuthorizationRequest
	ctx     context.Context
}

func (s *recordingGatewayAuthService) GetCredentials(_ context.Context, accessKey string) (*model.Credential, error) {
	return &model.Credential{
		Username:       "user1",
		BaseCredential: model.BaseCredential{AccessKeyID: accessKey},
	}, nil
}

func (s *recordingGatewayAuthService) GetUser(_ context.Context, username string) (*model.User, error) {
	return &model.User{Username: username}, nil
}

func (s *recordingGatewayAuthService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	s.request = req
	s.ctx = ctx
	return &auth.AuthorizationResponse{Allowed: true}, nil
}

func (s *recordingGatewayAuthService) ListEffectivePolicies(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return nil, nil, auth.ErrNotImplemented
}

// TestAuthorize_RepositoryMetadataFields verifies that the gateway exposes the bucket's repository
// metadata to policy conditions as lakefs:RepositoryMetadata/<key>, that the lookup is lazy, and
// that a key it does not resolve is reported as missing (and so denies).
func TestAuthorize_RepositoryMetadataFields(t *testing.T) {
	ctx := t.Context()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	testutil.MustDo(t, "config", err)
	cfg.Committed.LocalCache.Dir = path.Join(t.TempDir(), "cache")

	store, err := kv.Open(ctx, kvparams.Config{Type: "local", Local: &kvparams.Local{Path: "memory:gateway_authorize_fields", PrefetchSize: 256}})
	testutil.MustDo(t, "open kv store", err)
	t.Cleanup(func() { store.Close() })

	c, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      store,
		PathProvider: upload.DefaultPathProvider,
	})
	testutil.MustDo(t, "build catalog", err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.CreateRepository(ctx, authorizeFieldsRepoName, "", "mem://"+authorizeFieldsRepoName, "main", false)
	testutil.MustDo(t, "create repository", err)
	testutil.MustDo(t, "set repository metadata",
		c.UpdateRepositoryMetadata(ctx, authorizeFieldsRepoName, graveler.RepositoryMetadata{"team": authorizeFieldsTeam}))

	svc := &recordingGatewayAuthService{}
	req := httptest.NewRequest(http.MethodGet, "/"+authorizeFieldsRepoName+"/main/object", nil)
	reqCtx := auth.WithUser(ctx, &model.User{Username: "user1"})
	reqCtx = context.WithValue(reqCtx, ContextKeyOperation, &operations.Operation{Catalog: c})
	reqCtx = context.WithValue(reqCtx, ContextKeyRepositoryID, authorizeFieldsRepoName)
	req = req.WithContext(reqCtx)

	op := authorize(httptest.NewRecorder(), req, svc, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(authorizeFieldsRepoName, "object"),
		},
	})
	require.NotNil(t, op)
	require.NotNil(t, svc.request)
	require.NotNil(t, svc.request.Fields, "the gateway must expose repository metadata to conditions")

	value, found := svc.request.Fields(auth.RepositoryMetadataConditionKeyPrefix + "team")
	require.True(t, found)
	require.Equal(t, authorizeFieldsTeam, value)

	_, found = svc.request.Fields(auth.RepositoryMetadataConditionKeyPrefix + "missing")
	require.False(t, found, "a missing metadata key must report as missing, and so deny")

	_, found = svc.request.Fields("SourceIp")
	require.False(t, found, "the gateway only resolves repository metadata keys")

	// Services that evaluate policies from the context (e.g. the in-process ACL service) must see
	// the same fields: a statement conditioned on the repository metadata matches, while one
	// conditioned on an absent key does not.
	policies := []*model.Policy{{
		DisplayName: "allow-team",
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadObjectAction},
			Resource: "*",
			Condition: map[string]map[string][]string{
				auth.OperatorNameStringLike: {
					auth.RepositoryMetadataConditionKeyPrefix + "team": {authorizeFieldsTeam},
				},
			},
		}},
	}}
	node := permissions.Node{
		Type: permissions.NodeTypeNode,
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(authorizeFieldsRepoName, "object"),
		},
	}
	require.Equal(t, auth.CheckAllow,
		auth.CheckPermissions(svc.ctx, node, "user1", policies, &auth.MissingPermissions{}))

	policies[0].Statement[0].Condition = map[string]map[string][]string{
		auth.OperatorNameStringLike: {
			auth.RepositoryMetadataConditionKeyPrefix + "missing": {authorizeFieldsTeam},
		},
	}
	require.NotEqual(t, auth.CheckAllow,
		auth.CheckPermissions(svc.ctx, node, "user1", policies, &auth.MissingPermissions{}),
		"a missing metadata key must not grant permission")
}

func TestRepositoryMetadataFields_NoRepository(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	require.Nil(t, repositoryMetadataFields(ctx, nil, "repo"), "no catalog")
	require.Nil(t, repositoryMetadataFields(ctx, &catalog.Catalog{}, ""), "no bucket")
}
