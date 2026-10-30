package cmd

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAccessListCommand pins the access grant list request (§9.2, §10.1).
func TestAccessListCommand(t *testing.T) {
	listBody := `{"results": [
	  {"principal_type": "user", "principal_id": "alice", "granted_by": "admin", "granted_at": 1767225600000},
	  {"principal_type": "group", "principal_id": "analysts"}
	], "pagination": ` + emptyPagination + `}`
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: listBody})

	out := mustExecute(t, "access", "list", "customer-360")
	request := fake.OnlyRequest()
	require.Equal(t, http.MethodGet, request.Method)
	require.Equal(t, controlPlanePath+"/customer-360/access", request.Path)
	require.Contains(t, out, "alice")
	require.Contains(t, out, "analysts")
	require.Contains(t, out, "admin")
}

// TestAccessGrantCommand pins the grant request of §9.2: the principal type comes from the flag.
func TestAccessGrantCommand(t *testing.T) {
	grantBody := `{"principal_type": "group", "principal_id": "analysts", "granted_by": "admin"}`

	t.Run("group", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: grantBody})
		out := mustExecute(t, "access", "grant", "customer-360", "--group", "analysts")

		request := fake.OnlyRequest()
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, controlPlanePath+"/customer-360/access", request.Path)
		requireJSONBody(t, request.Body, `{"principal_type": "group", "principal_id": "analysts"}`)
		require.Contains(t, out, "Granted group")
	})

	t.Run("user", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{
			Status: http.StatusCreated,
			Body:   `{"principal_type": "user", "principal_id": "alice"}`,
		})
		mustExecute(t, "access", "grant", "customer-360", "--user", "alice")
		requireJSONBody(t, fake.OnlyRequest().Body, `{"principal_type": "user", "principal_id": "alice"}`)
	})

	t.Run("both principals", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: grantBody})
		_, err := executeCommand(t, "access", "grant", "customer-360", "--user", "alice", "--group", "analysts")
		require.ErrorContains(t, err, "not both")
		require.Empty(t, fake.Requests())
	})

	t.Run("no principal", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: grantBody})
		_, err := executeCommand(t, "access", "grant", "customer-360")
		require.ErrorContains(t, err, "requires --user or --group")
		require.Empty(t, fake.Requests())
	})
}

// TestAccessRevokeCommand pins the revoke request: the principal is part of the path (§10.1).
func TestAccessRevokeCommand(t *testing.T) {
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusNoContent})
	out := mustExecute(t, "access", "revoke", "customer-360", "--user", "alice")

	request := fake.OnlyRequest()
	require.Equal(t, http.MethodDelete, request.Method)
	require.Equal(t, controlPlanePath+"/customer-360/access/user/alice", request.Path)
	require.Contains(t, out, "Revoked user")
}

// TestParseAccessGrantOptions pins the principal parsing of `access grant|revoke`.
func TestParseAccessGrantOptions(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantType string
		wantID   string
		wantErr  string
	}{
		{name: "user", args: []string{"customer-360", "--user", "alice"}, wantType: principalTypeUser, wantID: "alice"},
		{name: "group", args: []string{"customer-360", "--group", "analysts"}, wantType: principalTypeGroup, wantID: "analysts"},
		{name: "both", args: []string{"customer-360", "--user", "alice", "--group", "g"}, wantErr: "not both"},
		{name: "none", args: []string{"customer-360"}, wantErr: "requires --user or --group"},
		{name: "missing dataset", args: nil, wantErr: "expects exactly one dataset name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := NewRoot()
			cmd, _, err := root.Find([]string{"access", "grant"})
			require.NoError(t, err)
			require.NoError(t, cmd.ParseFlags(tt.args))
			got, err := parseAccessGrantOptions(cmd, cmd.Flags().Args(), "grant")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantType, got.PrincipalType)
			require.Equal(t, tt.wantID, got.PrincipalID)
		})
	}
}
