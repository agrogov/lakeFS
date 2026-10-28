package auth_test

import (
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// TestAPIAuthService_Authorize_Fields verifies that AuthorizationRequest.Fields is used for
// condition keys the request does not carry itself (lakefs:RepositoryMetadata/<key>), that it is
// consulted lazily, and that a key it does not resolve denies.
func TestAPIAuthService_Authorize_Fields(t *testing.T) {
	t.Parallel()

	const (
		metadataKey   = auth.RepositoryMetadataConditionKeyPrefix + "team"
		requiredRes   = "arn:lakefs:fs:::repository/main/*"
		requiredAct   = "fs:ReadObject"
		repositoryMdt = "analytics"
	)

	allowIfTeamIsAnalytics := []auth.Policy{
		{
			Name: "allow-metadata-team",
			Statement: []auth.Statement{
				{
					Action:   []string{requiredAct},
					Resource: requiredRes,
					Effect:   model.StatementEffectAllow,
					Condition: &auth.Statement_Condition{
						AdditionalProperties: map[string]auth.PolicyCondition{
							"StringLike": {
								AdditionalProperties: map[string][]string{
									metadataKey: {repositoryMdt},
								},
							},
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name            string
		fields          func(calls *int) func(key string) (string, bool)
		policies        []auth.Policy
		expectedAllowed bool
	}{
		{
			name: "field resolves to the required value",
			fields: func(calls *int) func(key string) (string, bool) {
				return func(key string) (string, bool) {
					*calls = *calls + 1
					require.Equal(t, metadataKey, key)
					return repositoryMdt, true
				}
			},
			policies:        allowIfTeamIsAnalytics,
			expectedAllowed: true,
		},
		{
			name: "field resolves to another value",
			fields: func(*int) func(key string) (string, bool) {
				return func(string) (string, bool) { return "data", true }
			},
			policies:        allowIfTeamIsAnalytics,
			expectedAllowed: false,
		},
		{
			name: "missing field denies",
			fields: func(*int) func(key string) (string, bool) {
				return func(string) (string, bool) { return "", false }
			},
			policies:        allowIfTeamIsAnalytics,
			expectedAllowed: false,
		},
		{
			name:            "no field provider denies",
			fields:          func(*int) func(key string) (string, bool) { return nil },
			policies:        allowIfTeamIsAnalytics,
			expectedAllowed: false,
		},
		{
			name: "provider not consulted without a condition",
			fields: func(calls *int) func(key string) (string, bool) {
				return func(string) (string, bool) {
					*calls = *calls + 1
					return repositoryMdt, true
				}
			},
			policies: []auth.Policy{
				{
					Name: "allow-read",
					Statement: []auth.Statement{
						{
							Action:   []string{requiredAct},
							Resource: requiredRes,
							Effect:   model.StatementEffectAllow,
						},
					},
				},
			},
			expectedAllowed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockClient, s := NewTestApiService(t, false)

			response := &auth.ListUserPoliciesResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200:      &auth.PolicyList{Results: tt.policies},
			}
			mockClient.EXPECT().ListUserPoliciesWithResponse(gomock.Any(), "user1", gomock.Any()).
				Return(response, nil)

			calls := 0
			authResp, err := s.Authorize(t.Context(), &auth.AuthorizationRequest{
				Username: "user1",
				ClientIP: "10.0.0.1",
				RequiredPermissions: permissions.Node{
					Type: permissions.NodeTypeNode,
					Permission: permissions.Permission{
						Action:   requiredAct,
						Resource: requiredRes,
					},
				},
				Fields: tt.fields(&calls),
			})
			require.NoError(t, err)
			require.NotNil(t, authResp)
			require.Equal(t, tt.expectedAllowed, authResp.Allowed)
			if !tt.expectedAllowed {
				require.ErrorIs(t, authResp.Error, auth.ErrInsufficientPermissions)
			}
			if tt.name == "provider not consulted without a condition" {
				require.Zero(t, calls, "the field provider must only run when a condition asks for it")
			}
		})
	}
}
