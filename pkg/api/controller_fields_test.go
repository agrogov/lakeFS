package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	authorizeFieldsRepoName = "authorize-fields-repo"
	authorizeFieldsTeam     = "analytics"
)

// recordingControllerAuthService records the authorization request (and the context it was called
// with) so a test can inspect the condition fields the controller resolved.
type recordingControllerAuthService struct {
	auth.Service // only Authorize is used by authorizeCallback
	request      *auth.AuthorizationRequest
	ctx          context.Context
}

func (s *recordingControllerAuthService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	s.request = req
	s.ctx = ctx
	return &auth.AuthorizationResponse{Allowed: true}, nil
}

// TestAuthorizeCallback_RepositoryMetadataFields verifies that authorizeCallback exposes the
// metadata of the repository path parameter to policy conditions as
// lakefs:RepositoryMetadata/<key>, that the lookup is lazy, and that a key it does not resolve is
// reported as missing (and so denies).
func TestAuthorizeCallback_RepositoryMetadataFields(t *testing.T) {
	ctx := t.Context()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg := &config.ConfigImpl{}
	_, err := config.NewConfig("", cfg)
	testutil.MustDo(t, "config", err)
	cfg.Committed.LocalCache.Dir = path.Join(t.TempDir(), "cache")

	c, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      kvtest.GetStore(ctx, t),
		PathProvider: upload.DefaultPathProvider,
	})
	testutil.MustDo(t, "build catalog", err)
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.CreateRepository(ctx, authorizeFieldsRepoName, "", "mem://"+authorizeFieldsRepoName, "main", false)
	testutil.MustDo(t, "create repository", err)
	testutil.MustDo(t, "set repository metadata",
		c.UpdateRepositoryMetadata(ctx, authorizeFieldsRepoName, graveler.RepositoryMetadata{"team": authorizeFieldsTeam}))

	svc := &recordingControllerAuthService{}
	controller := &Controller{Catalog: c, Auth: svc}

	router := chi.NewRouter()
	router.Get("/repositories/{repository}/branches", func(w http.ResponseWriter, r *http.Request) {
		controller.authorizeCallback(w, r, permissions.Node{
			Permission: permissions.Permission{
				Action:   permissions.ListBranchesAction,
				Resource: permissions.RepoArn(authorizeFieldsRepoName),
			},
		}, writeError)
	})

	req := httptest.NewRequest(http.MethodGet, "/repositories/"+authorizeFieldsRepoName+"/branches", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &model.User{Username: "user1"}))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code)

	require.NotNil(t, svc.request)
	require.Equal(t, "user1", svc.request.Username)
	require.NotNil(t, svc.request.Fields, "the controller must expose repository metadata to conditions")

	value, found := svc.request.Fields(auth.RepositoryMetadataConditionKeyPrefix + "team")
	require.True(t, found)
	require.Equal(t, authorizeFieldsTeam, value)

	_, found = svc.request.Fields(auth.RepositoryMetadataConditionKeyPrefix + "missing")
	require.False(t, found, "a missing metadata key must report as missing, and so deny")

	// A service that evaluates policies from the context (e.g. the in-process ACL service) must see
	// the same fields: the statement matches for the metadata value a policy requires, and does not
	// match for an absent key.
	node := permissions.Node{
		Type: permissions.NodeTypeNode,
		Permission: permissions.Permission{
			Action:   permissions.ListBranchesAction,
			Resource: permissions.RepoArn(authorizeFieldsRepoName),
		},
	}
	policies := []*model.Policy{{
		DisplayName: "allow-team",
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ListBranchesAction},
			Resource: "*",
			Condition: map[string]map[string][]string{
				auth.OperatorNameStringLike: {
					auth.RepositoryMetadataConditionKeyPrefix + "team": {authorizeFieldsTeam},
				},
			},
		}},
	}}
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
