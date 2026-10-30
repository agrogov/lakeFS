package cmd

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// testProposalBody is a proposal response (§5.6, §10.2).
const testProposalBody = `{
  "id": "proposal-1",
  "dataset": "customer-360",
  "base_version": 2,
  "title": "add churn column",
  "description": "expose churn risk to analysts",
  "author": "admin",
  "status": "open",
  "created_at": 1767225600000,
  "preview_branch": "_proposal-proposal-1",
  "definition": ` + testDefinition + `,
  "diff": {"items": [{"target": "customers", "change": "added", "to": {"target": "customers", "repository": "lakehouse-repo", "ref": {"type": "commit", "id": "30f9eb1c"}, "type": "prefix", "prefix": {"path": "data/customers/"}}}]}
}`

// TestProposalsListCommand pins the proposal list request (§10.1).
func TestProposalsListCommand(t *testing.T) {
	listBody := `{"results": [` + testProposalBody + `], "pagination": ` + emptyPagination + `}`
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: listBody})

	out := mustExecute(t, "proposals", "list", "customer-360", "--status", "open", "--amount", "20")
	request := fake.OnlyRequest()
	require.Equal(t, http.MethodGet, request.Method)
	require.Equal(t, controlPlanePath+"/customer-360/proposals", request.Path)
	require.Equal(t, []string{"open"}, request.Query["status"])
	require.Equal(t, []string{"20"}, request.Query["amount"])
	require.Contains(t, out, "proposal-1")
	require.Contains(t, out, "add churn column")
	require.Contains(t, out, "open")
}

// TestParseProposalListOptions pins the argument parsing of `proposals list`.
func TestParseProposalListOptions(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    proposalListOptions
		wantErr string
	}{
		{
			name: "defaults",
			args: []string{"list", "customer-360"},
			want: proposalListOptions{paginationOptions: paginationOptions{Amount: 100}, Dataset: "customer-360", Output: outputTable},
		},
		{
			name: "closed status",
			args: []string{"list", "customer-360", "--status", "closed"},
			want: proposalListOptions{
				paginationOptions: paginationOptions{Amount: 100},
				Dataset:           "customer-360",
				Status:            "closed",
				Output:            outputTable,
			},
		},
		{
			name:    "unknown status",
			args:    []string{"list", "customer-360", "--status", "merged-away"},
			wantErr: "invalid --status",
		},
		{
			name:    "missing dataset",
			args:    []string{"list"},
			wantErr: "expects exactly one dataset name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := NewRoot()
			cmd, _, err := root.Find([]string{"proposals", "list"})
			require.NoError(t, err)
			require.NoError(t, cmd.ParseFlags(tt.args[1:]))
			got, err := parseProposalListOptions(cmd, cmd.Flags().Args())
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestProposalsShowCommand pins the proposal read request (§10.1).
func TestProposalsShowCommand(t *testing.T) {
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: testProposalBody})
	out := mustExecute(t, "proposals", "show", "customer-360", "proposal-1")

	request := fake.OnlyRequest()
	require.Equal(t, http.MethodGet, request.Method)
	require.Equal(t, controlPlanePath+"/customer-360/proposals/proposal-1", request.Path)
	require.Contains(t, out, "proposal-1")
	require.Contains(t, out, "Preview Branch: _proposal-proposal-1")
	require.Contains(t, out, "customers")
}

// TestProposalsCreateCommand pins the proposal create request (§5.6, §10.1).
func TestProposalsCreateCommand(t *testing.T) {
	file := writeDefinitionFile(t, testDefinition)

	t.Run("with description", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testProposalBody})
		mustExecute(t, "proposals", "create", "customer-360",
			"--file", file, "--title", "add churn column", "--description", "expose churn risk to analysts")

		request := fake.OnlyRequest()
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, controlPlanePath+"/customer-360/proposals", request.Path)
		requireJSONBody(t, request.Body,
			`{"definition": `+testDefinition+`, "title": "add churn column", "description": "expose churn risk to analysts"}`)
	})

	t.Run("without description", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testProposalBody})
		mustExecute(t, "proposals", "create", "customer-360", "--file", file, "--title", "title")
		requireJSONBody(t, fake.OnlyRequest().Body, `{"definition": `+testDefinition+`, "title": "title"}`)
	})

	t.Run("missing title", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testProposalBody})
		_, err := executeCommand(t, "proposals", "create", "customer-360", "--file", file)
		require.ErrorContains(t, err, "requires a non-empty --title")
	})

	t.Run("missing file", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testProposalBody})
		_, err := executeCommand(t, "proposals", "create", "customer-360", "--title", "title")
		require.ErrorContains(t, err, "requires --file")
	})
}

// TestProposalsMergeCommand pins the merge request (§5.6, §10.1).
func TestProposalsMergeCommand(t *testing.T) {
	t.Run("merge", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testVersionBody})
		out := mustExecute(t, "proposals", "merge", "customer-360", "proposal-1", "--message", "add churn column")

		request := fake.OnlyRequest()
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, controlPlanePath+"/customer-360/proposals/proposal-1/merge", request.Path)
		requireJSONBody(t, request.Body, `{"message": "add churn column"}`)
		require.Contains(t, out, "Version: v3")
	})

	t.Run("stale base version", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{
			Status: http.StatusConflict,
			Body:   `{"message": "rebase required: proposal is based on v2, latest is v3"}`,
		})
		_, err := executeCommand(t, "proposals", "merge", "customer-360", "proposal-1", "--message", "m")
		require.ErrorContains(t, err, "rebase required")
	})

	t.Run("missing message", func(t *testing.T) {
		_, err := executeCommand(t, "proposals", "merge", "customer-360", "proposal-1")
		require.ErrorContains(t, err, "non-empty --message")
	})
}

// TestProposalsCloseCommand pins the close request: a PATCH with status closed (§10.1).
func TestProposalsCloseCommand(t *testing.T) {
	closedBody := `{
	  "id": "proposal-1",
	  "dataset": "customer-360",
	  "base_version": 2,
	  "title": "add churn column",
	  "author": "admin",
	  "status": "closed",
	  "created_at": 1767225600000,
	  "closed_at": 1767225700000,
	  "preview_branch": "_proposal-proposal-1"
	}`
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: closedBody})

	out := mustExecute(t, "proposals", "close", "customer-360", "proposal-1")
	request := fake.OnlyRequest()
	require.Equal(t, http.MethodPatch, request.Method)
	require.Equal(t, controlPlanePath+"/customer-360/proposals/proposal-1", request.Path)
	requireJSONBody(t, request.Body, `{"status": "closed"}`)
	require.Contains(t, out, "Status: closed")
}
