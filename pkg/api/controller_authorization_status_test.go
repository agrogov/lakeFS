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

// deniedControllerAuthService denies every authorization request. With withError it reports the
// denial through auth.ErrInsufficientPermissions, the error authorizeCallback maps through
// auth.authorization_failure_status; without it returns a plain not-allowed response, the shape
// that C5 turned from a 500 into a 401. Both must honour the configured status.
type deniedControllerAuthService struct {
	auth.Service // only Authorize is used by authorizeCallback
	withError    bool
}

func (s *deniedControllerAuthService) Authorize(context.Context, *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	resp := &auth.AuthorizationResponse{Allowed: false}
	if s.withError {
		resp.Error = fmt.Errorf("%w: no policy grants it", auth.ErrInsufficientPermissions)
	}
	return resp, nil
}

// TestAuthorizeCallback_AuthorizationFailureStatus verifies that an authorization failure is
// returned as 401 by default and as 403 when auth.authorization_failure_status opts in, for both
// denial shapes (an ErrInsufficientPermissions error and a plain not-allowed response), and that
// a denial is never rendered as a success status.
func TestAuthorizeCallback_AuthorizationFailureStatus(t *testing.T) {
	denials := []struct {
		name      string
		withError bool
	}{
		{name: "insufficient permissions error", withError: true},
		{name: "not allowed without an error", withError: false},
	}
	statuses := []struct {
		name   string
		status int
		want   int
	}{
		{name: "unset keeps the default unauthorized", status: 0, want: http.StatusUnauthorized},
		{name: "explicit unauthorized", status: http.StatusUnauthorized, want: http.StatusUnauthorized},
		{name: "opt-in forbidden", status: http.StatusForbidden, want: http.StatusForbidden},
	}
	for _, denial := range denials {
		for _, tt := range statuses {
			t.Run(denial.name+" / "+tt.name, func(t *testing.T) {
				cfg := &config.ConfigImpl{}
				cfg.Auth.AuthorizationFailureStatus = tt.status
				controller := &Controller{
					Auth:   &deniedControllerAuthService{withError: denial.withError},
					Config: cfg,
				}

				recorder := authorizeTestRequest(t, controller)

				require.Equal(t, tt.want, recorder.Code)
				require.GreaterOrEqual(t, recorder.Code, http.StatusBadRequest,
					"an authorization failure must never be returned as a success status")
			})
		}
	}

	t.Run("non authorization errors still fail with internal server error", func(t *testing.T) {
		cfg := &config.ConfigImpl{}
		cfg.Auth.AuthorizationFailureStatus = http.StatusForbidden
		controller := &Controller{
			Auth:   &failingControllerAuthService{},
			Config: cfg,
		}

		recorder := authorizeTestRequest(t, controller)

		require.Equal(t, http.StatusInternalServerError, recorder.Code)
	})
}

// authorizeTestRequest runs controller.authorizeCallback for a denied ListBranches request with a
// recorded response, asserting the request was not authorized.
func authorizeTestRequest(t *testing.T, controller *Controller) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/repositories/repo/branches", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &model.User{Username: "user1"}))
	recorder := httptest.NewRecorder()

	allowed := controller.authorizeCallback(recorder, req, permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListBranchesAction,
			Resource: permissions.RepoArn("repo"),
		},
	}, writeError)

	require.False(t, allowed, "authorizeCallback should not allow a denied request")
	return recorder
}

// failingControllerAuthService fails authorizations with an error that is not
// auth.ErrInsufficientPermissions.
type failingControllerAuthService struct {
	auth.Service // only Authorize is used by authorizeCallback
}

func (s *failingControllerAuthService) Authorize(context.Context, *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return nil, errors.New("auth backend unavailable")
}
