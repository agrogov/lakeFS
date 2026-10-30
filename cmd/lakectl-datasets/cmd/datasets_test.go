package cmd

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestListCommand pins the request `datasets list` issues for its flags (§10.1, §12.1).
func TestListCommand(t *testing.T) {
	listBody := `{"results": [` + testDatasetBody + `], "pagination": ` + emptyPagination + `}`

	t.Run("no filters", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: listBody})
		mustExecute(t, "list")

		request := fake.OnlyRequest()
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, controlPlanePath+"/", request.Path)
		require.Equal(t, []string{"100"}, request.Query["amount"])
		require.NotContains(t, request.Query, "prefix")
		require.NotContains(t, request.Query, "after")
	})

	t.Run("prefix metadata and pagination", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: listBody})
		mustExecute(t, "list",
			"--prefix", "customer",
			"--metadata", "tier=gold",
			"--metadata", "owner=data-platform",
			"--amount", "5",
			"--after", "customer-100",
		)

		request := fake.OnlyRequest()
		require.Equal(t, controlPlanePath+"/", request.Path)
		require.Equal(t, []string{"customer"}, request.Query["prefix"])
		require.Equal(t, []string{"5"}, request.Query["amount"])
		require.Equal(t, []string{"customer-100"}, request.Query["after"])
		// Metadata filters are the dynamic metadata.<key>=<value> parameters of §10.1.
		require.Equal(t, []string{"gold"}, request.Query["metadata.tier"])
		require.Equal(t, []string{"data-platform"}, request.Query["metadata.owner"])
	})

	t.Run("json output", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: listBody})
		out := mustExecute(t, "list", "-o", "json")
		require.Contains(t, out, `"name": "customer-360"`)
		require.Contains(t, out, "\n  \"results\"")
	})

	t.Run("table output lists the datasets", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: listBody})
		out := mustExecute(t, "list")
		require.Contains(t, out, "customer-360")
		require.Contains(t, out, "v3")
	})

	t.Run("server error is reported", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{Status: http.StatusNotImplemented, Body: `{"message": "datasets are disabled on this server"}`})
		_, err := executeCommand(t, "list")
		require.ErrorContains(t, err, "datasets are disabled on this server")
	})
}

// TestParseListOptions pins the argument parsing and validation of `datasets list`.
func TestParseListOptions(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    listOptions
		wantErr string
	}{
		{
			name: "defaults",
			args: []string{"list"},
			want: listOptions{paginationOptions: paginationOptions{Amount: 100}, Metadata: map[string]string{}, Output: outputTable},
		},
		{
			name: "all flags",
			args: []string{"list", "--prefix", "pre", "--metadata", "a=1,b=2", "--metadata", "c=", "--amount", "7", "--after", "x"},
			want: listOptions{
				paginationOptions: paginationOptions{After: "x", Amount: 7},
				Prefix:            "pre",
				Metadata:          map[string]string{"a": "1,b=2", "c": ""},
				Output:            outputTable,
			},
		},
		{
			name:    "metadata without a value",
			args:    []string{"list", "--metadata", "tier"},
			wantErr: "expects key=value",
		},
		{
			name:    "metadata with an empty key",
			args:    []string{"list", "--metadata", "=gold"},
			wantErr: "expects key=value",
		},
		{
			name:    "non-positive amount",
			args:    []string{"list", "--amount", "0"},
			wantErr: "must be at least 1",
		},
		{
			name:    "unknown output format",
			args:    []string{"list", "-o", "yaml"},
			wantErr: "unsupported output format",
		},
		{
			name:    "unexpected argument",
			args:    []string{"list", "extra"},
			wantErr: "unknown command",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: `{"results": [], "pagination": ` + emptyPagination + `}`})
			if tt.wantErr == "" {
				root := NewRoot()
				cmd, _, err := root.Find([]string{"list"})
				require.NoError(t, err)
				require.NoError(t, cmd.ParseFlags(tt.args[1:]))
				got, err := parseListOptions(cmd)
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
				return
			}
			_, err := executeCommand(t, tt.args...)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestShowCommand pins the requests and output of `datasets show` (§10.1).
func TestShowCommand(t *testing.T) {
	t.Run("dataset", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: testDatasetBody})
		out := mustExecute(t, "show", "customer-360")
		request := fake.OnlyRequest()
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, controlPlanePath+"/customer-360", request.Path)
		require.Contains(t, out, "Dataset: customer-360")
		require.Contains(t, out, "Latest Version: v3")
		require.Contains(t, out, "lakehouse-repo")
	})

	t.Run("version", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: testVersionBody})
		out := mustExecute(t, "show", "customer-360", "--version", "v3")
		request := fake.OnlyRequest()
		require.Equal(t, controlPlanePath+"/customer-360/versions/v3", request.Path)
		require.Contains(t, out, "Version: v3")
		require.Contains(t, out, "add churn column")
	})

	t.Run("latest", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: testVersionBody})
		mustExecute(t, "show", "customer-360", "--version", "latest")
		require.Equal(t, controlPlanePath+"/customer-360/versions/latest", fake.OnlyRequest().Path)
	})

	t.Run("json output", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: testDatasetBody})
		out := mustExecute(t, "show", "customer-360", "-o", "json")
		require.Contains(t, out, `"latest_version": 3`)
	})

	t.Run("invalid version", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: testVersionBody})
		_, err := executeCommand(t, "show", "customer-360", "--version", "not-a-ref")
		require.ErrorContains(t, err, "invalid version")
		require.Empty(t, fake.Requests())
	})

	t.Run("missing dataset argument", func(t *testing.T) {
		_, err := executeCommand(t, "show")
		require.Error(t, err)
	})
}

// TestCreateCommand pins the create request of §10.1, including the optional publish envelope.
func TestCreateCommand(t *testing.T) {
	file := writeDefinitionFile(t, testDefinition)

	t.Run("without publish", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testDatasetBody})
		mustExecute(t, "create", "customer-360", "--file", file)

		request := fake.OnlyRequest()
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, controlPlanePath+"/", request.Path)
		requireJSONBody(t, request.Body, `{"name": "customer-360", "definition": `+testDefinition+`}`)
	})

	t.Run("with publish", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testDatasetBody})
		mustExecute(t, "create", "customer-360", "--file", file, "--publish", "--message", "initial dataset")

		request := fake.OnlyRequest()
		requireJSONBody(t, request.Body,
			`{"name": "customer-360", "definition": `+testDefinition+`, "publish": {"message": "initial dataset"}}`)
	})

	t.Run("definition envelope", func(t *testing.T) {
		envelope := `{"name": "customer-360", "definition": ` + testDefinition + `, "publish": {"message": "ignored"}}`
		envelopeFile := writeDefinitionFile(t, envelope)
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testDatasetBody})
		mustExecute(t, "create", "customer-360", "--file", envelopeFile)
		// The envelope's publish block is not used: create takes it from the command line.
		requireJSONBody(t, fake.OnlyRequest().Body, `{"name": "customer-360", "definition": `+testDefinition+`}`)
	})

	t.Run("envelope for another dataset", func(t *testing.T) {
		envelopeFile := writeDefinitionFile(t, `{"name": "other-dataset", "definition": `+testDefinition+`}`)
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testDatasetBody})
		_, err := executeCommand(t, "create", "customer-360", "--file", envelopeFile)
		require.ErrorContains(t, err, "definition file is for dataset")
		require.Empty(t, fake.Requests())
	})

	t.Run("asynchronous publish", func(t *testing.T) {
		taskBody := `{"id": "task-1", "status": "pending", "created_at": 1767225600000, "updated_at": 1767225600000}`
		newFakeControlPlane(t, fakeResponse{Status: http.StatusAccepted, Body: taskBody})
		out := mustExecute(t, "create", "customer-360", "--file", file, "--publish", "--message", "big")
		require.Contains(t, out, "task-1")
		require.Contains(t, out, "pending")
	})

	t.Run("message without publish", func(t *testing.T) {
		_, err := executeCommand(t, "create", "customer-360", "--file", file, "--message", "nope")
		require.ErrorContains(t, err, "only used with --publish")
	})

	t.Run("publish without message", func(t *testing.T) {
		_, err := executeCommand(t, "create", "customer-360", "--file", file, "--publish")
		require.ErrorContains(t, err, "requires --message")
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := executeCommand(t, "create", "customer-360")
		require.ErrorContains(t, err, "requires --file")
	})
}

// TestPublishCommand pins the publish request of §5.4/§10.1 and the --wait polling.
func TestPublishCommand(t *testing.T) {
	file := writeDefinitionFile(t, testDefinition)

	t.Run("publish", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testVersionBody})
		out := mustExecute(t, "publish", "customer-360", "--file", file, "--message", "add churn column")

		request := fake.OnlyRequest()
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, controlPlanePath+"/customer-360/versions", request.Path)
		requireJSONBody(t, request.Body,
			`{"definition": `+testDefinition+`, "message": "add churn column"}`)
		require.Contains(t, out, "Version: v3")
	})

	t.Run("expected version", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusCreated, Body: testVersionBody})
		mustExecute(t, "publish", "customer-360", "--file", file, "--message", "msg", "--expected-version", "2")
		requireJSONBody(t, fake.OnlyRequest().Body,
			`{"definition": `+testDefinition+`, "message": "msg", "expected_version": 2}`)
	})

	t.Run("version conflict", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{
			Status: http.StatusConflict,
			Body:   `{"message": "version conflict: latest is 4", "latest_version": 4}`,
		})
		_, err := executeCommand(t, "publish", "customer-360", "--file", file, "--message", "msg", "--expected-version", "2")
		require.ErrorContains(t, err, "version conflict: latest is 4")
	})

	t.Run("wait for the publish task", func(t *testing.T) {
		taskPendingBody := `{"id": "task-9", "status": "pending", "created_at": 1767225600000, "updated_at": 1767225600000}`
		taskDoneBody := `{"id": "task-9", "status": "completed", "version": 3, "created_at": 1767225600000, "updated_at": 1767225600000}`
		fake := newFakeControlPlane(t,
			fakeResponse{Status: http.StatusAccepted, Body: taskPendingBody},
			fakeResponse{Status: http.StatusOK, Body: taskDoneBody},
		)
		previous := taskPollInterval
		taskPollInterval = time.Millisecond
		t.Cleanup(func() { taskPollInterval = previous })

		out := mustExecute(t, "publish", "customer-360", "--file", file, "--message", "big", "--wait")
		requests := fake.Requests()
		require.Len(t, requests, 2)
		require.Equal(t, controlPlanePath+"/customer-360/versions", requests[0].Path)
		require.Equal(t, controlPlanePath+"/customer-360/tasks/task-9", requests[1].Path)
		require.Contains(t, out, "task-9")
		require.Contains(t, out, "v3")
	})

	t.Run("wait with json output prints the completed task", func(t *testing.T) {
		taskDoneBody := `{"id": "task-9", "status": "completed", "version": 3, "created_at": 1, "updated_at": 2}`
		newFakeControlPlane(t,
			fakeResponse{Status: http.StatusAccepted, Body: `{"id": "task-9", "status": "pending", "created_at": 1, "updated_at": 1}`},
			fakeResponse{Status: http.StatusOK, Body: taskDoneBody},
		)
		previous := taskPollInterval
		taskPollInterval = time.Millisecond
		t.Cleanup(func() { taskPollInterval = previous })

		out := mustExecute(t, "publish", "customer-360", "--file", file, "--message", "big", "--wait", "-o", "json")
		require.Contains(t, out, `"status": "completed"`)
		require.NotContains(t, out, `"status": "pending"`)
	})

	t.Run("failed publish task", func(t *testing.T) {
		taskFailedBody := `{"id": "task-9", "status": "failed", "error": "source repository is gone", "created_at": 1767225600000, "updated_at": 1767225600000}`
		newFakeControlPlane(t,
			fakeResponse{Status: http.StatusAccepted, Body: `{"id": "task-9", "status": "pending", "created_at": 1, "updated_at": 1}`},
			fakeResponse{Status: http.StatusOK, Body: taskFailedBody},
		)
		previous := taskPollInterval
		taskPollInterval = time.Millisecond
		t.Cleanup(func() { taskPollInterval = previous })

		_, err := executeCommand(t, "publish", "customer-360", "--file", file, "--message", "big", "--wait")
		require.ErrorContains(t, err, "source repository is gone")
	})

	t.Run("missing message", func(t *testing.T) {
		_, err := executeCommand(t, "publish", "customer-360", "--file", file)
		require.ErrorContains(t, err, "non-empty --message")
	})

	t.Run("invalid expected version", func(t *testing.T) {
		_, err := executeCommand(t, "publish", "customer-360", "--file", file, "--message", "m", "--expected-version", "two")
		require.ErrorContains(t, err, "expects a version number")
	})
}

// TestValidateCommand pins the dry-run request of §10.1.
func TestValidateCommand(t *testing.T) {
	file := writeDefinitionFile(t, testDefinition)
	validateBody := `{"definition": ` + testDefinition + `, "stats": {"objects": 12, "bytes": 4096, "items": []}}`
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: validateBody})

	out := mustExecute(t, "validate", "customer-360", "--file", file)
	request := fake.OnlyRequest()
	require.Equal(t, http.MethodPost, request.Method)
	require.Equal(t, controlPlanePath+"/customer-360/validate", request.Path)
	requireJSONBody(t, request.Body, `{"definition": `+testDefinition+`}`)
	require.Contains(t, out, "Valid: true")
	require.Contains(t, out, "12 objects")
}

// TestValidateCommandReportsValidationErrors checks that every problem of a 400 answer is shown
// (§10.4).
func TestValidateCommandReportsValidationErrors(t *testing.T) {
	file := writeDefinitionFile(t, testDefinition)
	newFakeControlPlane(t, fakeResponse{
		Status: http.StatusBadRequest,
		Body: `{"message": "invalid definition", "errors": [` +
			`{"path": "data[2].target", "message": "target overlaps data[0]"},` +
			`{"path": "metadata.tier", "message": "not an allowed value"}]}`,
	})

	_, err := executeCommand(t, "validate", "customer-360", "--file", file)
	require.ErrorContains(t, err, "invalid definition")
	require.ErrorContains(t, err, "data[2].target: target overlaps data[0]")
	require.ErrorContains(t, err, "metadata.tier: not an allowed value")
}

// TestVersionsCommand pins the version list request of §10.1.
func TestVersionsCommand(t *testing.T) {
	versionsBody := `{"results": [` + testVersionBody + `], "pagination": ` + emptyPagination + `}`
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: versionsBody})

	out := mustExecute(t, "versions", "customer-360", "--amount", "10", "--after", "v9")
	request := fake.OnlyRequest()
	require.Equal(t, http.MethodGet, request.Method)
	require.Equal(t, controlPlanePath+"/customer-360/versions", request.Path)
	require.Equal(t, []string{"10"}, request.Query["amount"])
	require.Equal(t, []string{"v9"}, request.Query["after"])
	require.Contains(t, out, "v3")
	require.Contains(t, out, "add churn column")
	require.Contains(t, out, "abc123")
}

// TestDiffCommand pins both diff modes of §10.3.
func TestDiffCommand(t *testing.T) {
	definitionDiffBody := `{
	  "description": {"from": "old", "to": "new"},
	  "metadata": [{"key": "tier", "change": "added", "to": "gold"}],
	  "items": [
	    {"target": "customers", "change": "added", "to": {"target": "customers", "repository": "lakehouse-repo", "ref": {"type": "commit", "id": "30f9eb1c"}, "type": "prefix", "prefix": {"path": "data/customers/"}}},
	    {"target": "reference/countries.csv", "change": "removed", "from": {"target": "reference/countries.csv", "repository": "ref-data", "ref": {"type": "commit", "id": "a1b2c3"}, "type": "object", "object": {"path": "geo/countries.csv"}}}
	  ]
	}`

	t.Run("definition diff", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: definitionDiffBody})
		out := mustExecute(t, "diff", "customer-360", "v1", "v2")

		request := fake.OnlyRequest()
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, controlPlanePath+"/customer-360/refs/v1/diff/v2", request.Path)
		require.Equal(t, []string{"definition"}, request.Query["type"])
		require.Contains(t, out, "customers")
		require.Contains(t, out, "added")
		require.Contains(t, out, "reference/countries.csv")
		require.Contains(t, out, "removed")
	})

	t.Run("data diff", func(t *testing.T) {
		dataDiffBody := `{"results": [{"path": "customers/part-0.parquet", "type": "added", "size_bytes": 42}], "pagination": ` + emptyPagination + `}`
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: dataDiffBody})
		out := mustExecute(t, "diff", "customer-360", "v1", "latest", "--data", "--prefix", "customers/", "--amount", "5")

		request := fake.OnlyRequest()
		require.Equal(t, controlPlanePath+"/customer-360/refs/v1/diff/latest", request.Path)
		require.Equal(t, []string{"data"}, request.Query["type"])
		require.Equal(t, []string{"customers/"}, request.Query["prefix"])
		require.Equal(t, []string{"5"}, request.Query["amount"])
		require.Contains(t, out, "customers/part-0.parquet")
		require.Contains(t, out, "42")
	})

	t.Run("json output", func(t *testing.T) {
		newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: definitionDiffBody})
		out := mustExecute(t, "diff", "customer-360", "v1", "v2", "-o", "json")
		require.Contains(t, out, `"change": "added"`)
	})

	t.Run("invalid ref", func(t *testing.T) {
		fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: definitionDiffBody})
		_, err := executeCommand(t, "diff", "customer-360", "v1", "bogus")
		require.ErrorContains(t, err, "invalid version")
		require.Empty(t, fake.Requests())
	})

	t.Run("missing argument", func(t *testing.T) {
		_, err := executeCommand(t, "diff", "customer-360", "v1")
		require.Error(t, err)
	})
}

// TestDeleteCommand pins the delete request of §6.6.
func TestDeleteCommand(t *testing.T) {
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusNoContent})
	out := mustExecute(t, "delete", "customer-360", "--yes")

	request := fake.OnlyRequest()
	require.Equal(t, http.MethodDelete, request.Method)
	require.Equal(t, controlPlanePath+"/customer-360", request.Path)
	require.Contains(t, strings.ToLower(out), "deleted")
}
