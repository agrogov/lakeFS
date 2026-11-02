package cmd

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCommandsIssueExpectedRequests is the table test of DATASETS_DESIGN.md §12.1: for every
// command of the plugin it pins the HTTP request the command sends to the control plane. The
// fake server records the request; no live lakeFS server is involved.
func TestCommandsIssueExpectedRequests(t *testing.T) {
	file := writeDefinitionFile(t, testDefinition)
	base := controlPlanePath

	tests := []struct {
		name       string
		args       []string
		status     int
		body       string
		wantMethod string
		wantPath   string
		wantQuery  map[string]string
		wantBody   string // expected JSON body, empty when the command sends none
	}{
		{
			name:       "list",
			args:       []string{"list", "--prefix", "customer", "--metadata", "tier=gold"},
			status:     http.StatusOK,
			body:       `{"results": [], "pagination": ` + emptyPagination + `}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/",
			wantQuery:  map[string]string{"prefix": "customer", "amount": "100", "metadata.tier": "gold"},
		},
		{
			name:       "show",
			args:       []string{"show", "customer-360"},
			status:     http.StatusOK,
			body:       testDatasetBody,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360",
		},
		{
			name:       "show version",
			args:       []string{"show", "customer-360", "--version", "v2"},
			status:     http.StatusOK,
			body:       testVersionBody,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360/versions/v2",
		},
		{
			name:       "create",
			args:       []string{"create", "customer-360", "--file", file, "--message", "initial"},
			status:     http.StatusCreated,
			body:       testDatasetBody,
			wantMethod: http.MethodPost,
			wantPath:   base + "/",
			wantBody:   `{"name": "customer-360", "definition": ` + testDefinition + `, "publish": {"message": "initial"}}`,
		},
		{
			name:       "publish",
			args:       []string{"publish", "customer-360", "--file", file, "--message", "second", "--expected-version", "1"},
			status:     http.StatusCreated,
			body:       testVersionBody,
			wantMethod: http.MethodPost,
			wantPath:   base + "/customer-360/versions",
			wantBody:   `{"definition": ` + testDefinition + `, "message": "second", "expected_version": 1}`,
		},
		{
			name:       "validate",
			args:       []string{"validate", "customer-360", "--file", file},
			status:     http.StatusOK,
			body:       `{"definition": ` + testDefinition + `}`,
			wantMethod: http.MethodPost,
			wantPath:   base + "/customer-360/validate",
			wantBody:   `{"definition": ` + testDefinition + `}`,
		},
		{
			name:       "versions",
			args:       []string{"versions", "customer-360"},
			status:     http.StatusOK,
			body:       `{"results": [], "pagination": ` + emptyPagination + `}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360/versions",
			wantQuery:  map[string]string{"amount": "100"},
		},
		{
			name:       "diff definition",
			args:       []string{"diff", "customer-360", "v1", "v2"},
			status:     http.StatusOK,
			body:       `{"data": []}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360/refs/v1/diff/v2",
			wantQuery:  map[string]string{"type": "definition", "amount": "100"},
		},
		{
			name:       "diff data",
			args:       []string{"diff", "customer-360", "v1", "latest", "--data"},
			status:     http.StatusOK,
			body:       `{"results": [], "pagination": ` + emptyPagination + `}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360/refs/v1/diff/latest",
			wantQuery:  map[string]string{"type": "data", "amount": "100"},
		},
		{
			name:       "delete",
			args:       []string{"delete", "customer-360", "--yes"},
			status:     http.StatusNoContent,
			wantMethod: http.MethodDelete,
			wantPath:   base + "/customer-360",
		},
		{
			name:       "pulls list",
			args:       []string{"pulls", "list", "customer-360", "--status", "open"},
			status:     http.StatusOK,
			body:       `{"results": [], "pagination": ` + emptyPagination + `}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360/pulls",
			wantQuery:  map[string]string{"status": "open", "amount": "100"},
		},
		{
			name:       "pulls list all",
			args:       []string{"pulls", "list", "--status", "all"},
			status:     http.StatusOK,
			body:       `{"results": [], "pagination": ` + emptyPagination + `}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/pulls",
			wantQuery:  map[string]string{"status": "all", "amount": "100"},
		},
		{
			name:       "pulls show",
			args:       []string{"pulls", "show", "customer-360", "proposal-1"},
			status:     http.StatusOK,
			body:       testPullRequestBody,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360/pulls/proposal-1",
		},
		{
			name:       "pulls create",
			args:       []string{"pulls", "create", "customer-360", "--file", file, "--title", "title"},
			status:     http.StatusCreated,
			body:       testPullRequestBody,
			wantMethod: http.MethodPost,
			wantPath:   base + "/customer-360/pulls",
			wantBody:   `{"definition": ` + testDefinition + `, "title": "title"}`,
		},
		{
			name:       "pulls diff",
			args:       []string{"pulls", "diff", "customer-360", "proposal-1"},
			status:     http.StatusOK,
			body:       `{"data": []}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360/pulls/proposal-1/diff",
		},
		{
			name:       "pulls merge",
			args:       []string{"pulls", "merge", "customer-360", "proposal-1"},
			status:     http.StatusOK,
			body:       `{"version": 4, "reference": "v4"}`,
			wantMethod: http.MethodPost,
			wantPath:   base + "/customer-360/pulls/proposal-1/merge",
		},
		{
			name:       "pulls close",
			args:       []string{"pulls", "close", "customer-360", "proposal-1"},
			status:     http.StatusNoContent,
			wantMethod: http.MethodDelete,
			wantPath:   base + "/customer-360/pulls/proposal-1",
		},
		{
			name:       "access list",
			args:       []string{"access", "list", "customer-360"},
			status:     http.StatusOK,
			body:       `{"results": [], "pagination": ` + emptyPagination + `}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/customer-360/access",
		},
		{
			name:       "access grant",
			args:       []string{"access", "grant", "customer-360", "--user", "alice"},
			status:     http.StatusCreated,
			body:       `{"principal_type": "user", "principal_id": "alice"}`,
			wantMethod: http.MethodPost,
			wantPath:   base + "/customer-360/access",
			wantBody:   `{"principal_type": "user", "principal_id": "alice"}`,
		},
		{
			name:       "access revoke",
			args:       []string{"access", "revoke", "customer-360", "--group", "analysts"},
			status:     http.StatusNoContent,
			wantMethod: http.MethodDelete,
			wantPath:   base + "/customer-360/access/group/analysts",
		},
		{
			name:       "metadata-keys list",
			args:       []string{"metadata-keys", "list"},
			status:     http.StatusOK,
			body:       `{"results": [], "pagination": ` + emptyPagination + `}`,
			wantMethod: http.MethodGet,
			wantPath:   base + "/metadata",
			wantQuery:  map[string]string{"amount": "100"},
		},
		{
			name:       "metadata-keys create",
			args:       []string{"metadata-keys", "create", "owner", "--type", "string"},
			status:     http.StatusCreated,
			body:       `{"name": "owner", "type": "string", "required": false, "allow_multiple": false, "created_at": 1, "created_by": "admin"}`,
			wantMethod: http.MethodPost,
			wantPath:   base + "/metadata",
			wantBody:   `{"name": "owner", "type": "string", "required": false, "allow_multiple": false}`,
		},
		{
			name:       "metadata-keys delete",
			args:       []string{"metadata-keys", "delete", "owner"},
			status:     http.StatusNoContent,
			wantMethod: http.MethodDelete,
			wantPath:   base + "/metadata/owner",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeControlPlane(t, fakeResponse{Status: tt.status, Body: tt.body})
			_, err := executeCommand(t, tt.args...)
			require.NoError(t, err)

			request := fake.OnlyRequest()
			require.Equal(t, tt.wantMethod, request.Method)
			require.Equal(t, tt.wantPath, request.Path)
			for name, value := range tt.wantQuery {
				require.Equal(t, []string{value}, request.Query[name], "query parameter %q", name)
			}
			if tt.wantBody != "" {
				requireJSONBody(t, request.Body, tt.wantBody)
			}
		})
	}
}
