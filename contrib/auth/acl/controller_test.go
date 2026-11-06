package acl_test

// Tests for the two policy endpoints of api/authorization.yml that used to answer 501: ListPolicies
// and DeletePolicy (review R3S-5). The in-process service implemented both, and only the HTTP
// controller did not; pkg/auth.APIAuthService talks to the controller, and maps the 501 to
// auth.ErrUnexpectedStatusCode, which is not the one degraded error a caller may tolerate. Every
// request goes through contrib/auth/acl.Serve, so the swagger request validator runs exactly as it
// does in a real external-auth deployment.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authacl "github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// creationTime is a fixed creation timestamp for the policies of these tests.
var creationTime = time.Unix(creationDate, 0)

// newControllerTestServer serves the ACL auth API over httptest, with the in-process service the
// binary uses when auth is not external.
func newControllerTestServer(t *testing.T) (auth.Service, string) {
	t.Helper()
	svc := authacl.NewAuthService(kvtest.GetStore(t.Context(), t), crypt.NewSecretStore(someSecret),
		authparams.ServiceCache{Enabled: false})
	server := httptest.NewServer(authacl.Serve(svc, logging.DummyLogger{}))
	t.Cleanup(server.Close)
	return svc, server.URL + authacl.BaseURL
}

func TestControllerListPolicies(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	svc, baseURL := newControllerTestServer(t)

	for _, name := range []string{"policies-a", "policies-b", "other-c"} {
		require.NoError(t, svc.WritePolicy(ctx, &model.Policy{
			CreatedAt:   creationTime,
			DisplayName: name,
			Statement: model.Statements{{
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.ReadRepositoryAction},
				Resource: permissions.RepoArn("repo"),
			}},
		}, false /* create */))
	}

	get := func(t *testing.T, path string) (*http.Response, auth.PolicyList) {
		t.Helper()
		resp, err := http.Get(baseURL + path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })
		var list auth.PolicyList
		if resp.StatusCode == http.StatusOK {
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
		}
		return resp, list
	}

	t.Run("lists every policy", func(t *testing.T) {
		resp, list := get(t, "/auth/policies")
		require.Equal(t, http.StatusOK, resp.StatusCode, "GET /auth/policies must be implemented, not 501")
		names := make([]string, 0, len(list.Results))
		for _, policy := range list.Results {
			names = append(names, policy.Name)
		}
		require.ElementsMatch(t, []string{"policies-a", "policies-b", "other-c"}, names)
		require.NotEmpty(t, list.Results[0].Statement, "a listed policy carries its statements")
		require.NotNil(t, list.Results[0].CreationDate)

		t.Run("pagination block is filled", func(t *testing.T) {
			require.Equal(t, len(list.Results), list.Pagination.Results)
			require.False(t, list.Pagination.HasMore)
			require.Empty(t, list.Pagination.NextOffset)
		})
	})

	t.Run("prefix filters", func(t *testing.T) {
		resp, list := get(t, "/auth/policies?prefix=policies-")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		names := make([]string, 0, len(list.Results))
		for _, policy := range list.Results {
			names = append(names, policy.Name)
		}
		require.ElementsMatch(t, []string{"policies-a", "policies-b"}, names,
			"the prefix is what pkg/mds/bootstrap passes to find its own chunks")
	})
}

func TestControllerDeletePolicy(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	svc, baseURL := newControllerTestServer(t)

	const policyName = "delete-me"
	require.NoError(t, svc.WritePolicy(ctx, &model.Policy{
		CreatedAt:   creationTime,
		DisplayName: policyName,
		Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: permissions.RepoArn("repo"),
		}},
	}, false /* create */))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+"/auth/policies/"+policyName, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "DELETE /auth/policies/{policyId} must be implemented, not 501")

	_, err = svc.GetPolicy(ctx, policyName)
	require.ErrorIs(t, err, auth.ErrNotFound, "the policy is gone")

	// A missing policy is a 404, as api/authorization.yml documents, and not the 501 that would
	// stop a caller from treating it as already deleted.
	req, err = http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+"/auth/policies/"+policyName, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
