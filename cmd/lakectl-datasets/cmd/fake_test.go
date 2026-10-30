package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// recordedRequest is one request the commands issued, as the fake control plane saw it.
type recordedRequest struct {
	Method string
	Path   string
	Query  map[string][]string
	Header http.Header
	Body   []byte
}

// fakeResponse is one canned answer of the fake control plane.
type fakeResponse struct {
	Status int
	Body   string
}

// fakeControlPlane is a stand-in for the datasets control plane (api/datasets.yml): it records
// every request and answers from a canned list, so the tests assert the request each command
// issues without a live server (DATASETS_DESIGN.md §12.1).
type fakeControlPlane struct {
	t *testing.T

	mu        sync.Mutex
	requests  []recordedRequest
	responses []fakeResponse
}

// newFakeControlPlane starts a fake control plane that answers the given responses in order and
// repeats the last one for any further request. It also points newAPIClient at the server for the
// duration of the test.
func newFakeControlPlane(t *testing.T, responses ...fakeResponse) *fakeControlPlane {
	t.Helper()
	fake := &fakeControlPlane{t: t, responses: responses}
	server := httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(server.Close)

	previous := newAPIClient
	newAPIClient = func() (*apigen.ClientWithResponses, error) {
		return apigen.NewClientWithResponses(server.URL + controlPlanePath)
	}
	t.Cleanup(func() { newAPIClient = previous })
	return fake
}

func (f *fakeControlPlane) handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	require.NoError(f.t, err)

	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{
		Method: r.Method,
		Path:   r.URL.Path,
		Query:  r.URL.Query(),
		Header: r.Header.Clone(),
		Body:   body,
	})
	response := f.responses[len(f.responses)-1]
	if len(f.requests) <= len(f.responses) {
		response = f.responses[len(f.requests)-1]
	}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.Status)
	if response.Body != "" {
		_, _ = io.WriteString(w, response.Body)
	}
}

// Requests returns the requests received so far.
func (f *fakeControlPlane) Requests() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

// OnlyRequest returns the single request the command issued, failing the test when there was not
// exactly one.
func (f *fakeControlPlane) OnlyRequest() recordedRequest {
	f.t.Helper()
	requests := f.Requests()
	require.Len(f.t, requests, 1, "expected exactly one request")
	return requests[0]
}

// executeCommand runs the datasets command tree with args and returns its standard output. The
// command tree is executed on its own (not through lakectl's root), so the test does not need the
// lakectl configuration the client factory would read.
func executeCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := NewRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetIn(bytes.NewReader(nil))
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

// mustExecute runs the command tree and fails the test when the command fails.
func mustExecute(t *testing.T, args ...string) string {
	t.Helper()
	out, err := executeCommand(t, args...)
	require.NoError(t, err)
	return out
}

// writeDefinitionFile writes content to a file in a fresh temporary directory and returns its path.
func writeDefinitionFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "definition.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// requireJSONBody asserts that body is the JSON encoding of want.
func requireJSONBody(t *testing.T, body []byte, want string) {
	t.Helper()
	var got any
	require.NoError(t, json.Unmarshal(body, &got), "request body is not JSON: %s", body)
	var expected any
	require.NoError(t, json.Unmarshal([]byte(want), &expected))
	require.Equal(t, expected, got)
}

// testDefinition is a minimal valid definition document (§5.1).
const testDefinition = `{
  "format_version": 1,
  "description": "Curated customer view",
  "metadata": {"tier": "gold"},
  "data": [
    {
      "target": "customers",
      "repository": "lakehouse-repo",
      "ref": {"type": "commit", "id": "30f9eb1c"},
      "type": "prefix",
      "prefix": {"path": "data/customers/"}
    }
  ]
}`

// testVersionBody is a version response (§10.2).
const testVersionBody = `{
  "version": 3,
  "message": "add churn column",
  "publisher": "admin",
  "published_at": 1767225600000,
  "commit_id": "abc123",
  "definition": ` + testDefinition + `,
  "stats": {"objects": 12, "bytes": 4096, "items": [{"target": "customers", "objects": 12, "bytes": 4096}]}
}`

// testDatasetBody is a dataset response (§10.2).
const testDatasetBody = `{
  "name": "customer-360",
  "description": "Curated customer view for analytics",
  "created_at": 1767225600000,
  "created_by": "admin",
  "latest_version": 3,
  "state": "active",
  "definition": ` + testDefinition + `,
  "stats": {"objects": 12, "bytes": 4096, "items": []}
}`

// emptyPagination is the pagination object of every list response (§10.2).
const emptyPagination = `{"has_more": false, "next_offset": "", "results": 0, "max_per_page": 100}`
