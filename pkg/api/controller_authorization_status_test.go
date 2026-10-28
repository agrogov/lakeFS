package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// deniedControllerAuthService denies every authorization request with
// auth.ErrInsufficientPermissions, the error authorizeCallback maps through
// auth.authorization_failure_status.
type deniedControllerAuthService struct {
	auth.Service // only Authorize is used by authorizeCallback
}

func (s *deniedControllerAuthService) Authorize(context.Context, *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{
		Allowed: false,
		Error:   fmt.Errorf("%w: no policy grants it", auth.ErrInsufficientPermissions),
	}, nil
}

// TestAuthorizeCallback_AuthorizationFailureStatus verifies that an authorization failure is
// returned as 401 by default and as 403 when auth.authorization_failure_status opts in, so the
// opt-in path is not only configured but actually wired to the response.
func TestAuthorizeCallback_AuthorizationFailureStatus(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   int
	}{
		{
			name:   "unset keeps the default unauthorized",
			status: 0,
			want:   http.StatusUnauthorized,
		},
		{
			name:   "explicit unauthorized",
			status: http.StatusUnauthorized,
			want:   http.StatusUnauthorized,
		},
		{
			name:   "opt-in forbidden",
			status: http.StatusForbidden,
			want:   http.StatusForbidden,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.ConfigImpl{}
			cfg.Auth.AuthorizationFailureStatus = tt.status
			controller := &Controller{
				Auth:   &deniedControllerAuthService{},
				Config: cfg,
			}

			req := httptest.NewRequest(http.MethodGet, "/repositories/repo/branches", nil)
			req = req.WithContext(auth.WithUser(req.Context(), &model.User{Username: "user1"}))
			recorder := httptest.NewRecorder()

			allowed := controller.authorizeCallback(recorder, req, permissions.Node{
				Permission: permissions.Permission{
					Action:   permissions.ListBranchesAction,
					Resource: permissions.RepoArn("repo"),
				},
			}, writeError)

			require.False(t, allowed)
			require.Equal(t, tt.want, recorder.Code)
		})
	}

	t.Run("non authorization errors still fail with internal server error", func(t *testing.T) {
		cfg := &config.ConfigImpl{}
		cfg.Auth.AuthorizationFailureStatus = http.StatusForbidden
		controller := &Controller{
			Auth:   &failingControllerAuthService{},
			Config: cfg,
		}

		req := httptest.NewRequest(http.MethodGet, "/repositories/repo/branches", nil)
		req = req.WithContext(auth.WithUser(req.Context(), &model.User{Username: "user1"}))
		recorder := httptest.NewRecorder()

		allowed := controller.authorizeCallback(recorder, req, permissions.Node{
			Permission: permissions.Permission{
				Action:   permissions.ListBranchesAction,
				Resource: permissions.RepoArn("repo"),
			},
		}, writeError)

		require.False(t, allowed)
		require.Equal(t, http.StatusInternalServerError, recorder.Code)
	})
}

// failingControllerAuthService fails authorizations with an error that is not
// auth.ErrInsufficientPermissions.
type failingControllerAuthService struct {
	auth.Service // only Authorize is used by authorizeCallback
}

func (s *failingControllerAuthService) Authorize(context.Context, *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return nil, errors.New("auth backend unavailable")
}
