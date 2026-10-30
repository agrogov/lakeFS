package datasets

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

const (
	testAccessKeyID     = "AKIADATASETSTEST0001"
	testSecretAccessKey = "datasets-test-secret-access-key"
	testUsername        = "datasets-tester"
)

// enterpriseDefinition is the definition example of DATASETS_DESIGN.md §5.1: the format is
// compatible with the documented `format_version: 1` definitions, so it must decode into the
// generated types unchanged.
const enterpriseDefinition = `{
  "format_version": 1,
  "description": "Curated customer view for analytics",
  "metadata": {"classification": "internal", "owner": "data-platform", "tier": "gold", "retention": "1y"},
  "data": [
    {
      "target": "customers",
      "repository": "lakehouse-repo",
      "ref": {"type": "commit", "id": "30f9eb1c9a3d4b8e8f0d0a6c1b7e5f2a9c4d3e1f"},
      "type": "prefix",
      "prefix": {"path": "data/customers/"}
    },
    {
      "target": "reference/countries.csv",
      "repository": "ref-data",
      "ref": {"type": "commit", "id": "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"},
      "type": "object",
      "object": {"path": "geo/countries.csv"}
    }
  ]
}`

const validPublishBody = `{
  "message": "second cut",
  "definition": ` + enterpriseDefinition + `
}`

const validCreateBody = `{
  "name": "customer-360",
  "definition": ` + enterpriseDefinition + `,
  "publish": {"message": "initial dataset"}
}`

const validMetadataKeyBody = `{"name": "classification", "type": "enum", "allowed_values": ["public", "internal"]}`

// newControlPlane builds the datasets control plane exactly as RoutesBuilder does (validator,
// api.GenericAuthMiddleware, authenticated-user requirement, generated server) on top of the
// core catch-all, and creates a user that can authenticate with basic auth and holds the
// built-in datasets permissions of §9.5 plus the ACL base read/write grants.
func newControlPlane(t *testing.T) (*chi.Mux, string, string) {
	t.Helper()
	mux, _, accessKey, secret := newControlPlaneWithDeps(t)
	return mux, accessKey, secret
}

// newControlPlaneWithDeps is newControlPlane plus the server dependencies, for tests that also
// inspect the KV store or the auth service.
func newControlPlaneWithDeps(t *testing.T) (*chi.Mux, cmdpkg.ServerDeps, string, string) {
	t.Helper()
	return newControlPlaneWithConfig(t, "datasets:\n  enabled: true\n  storage_namespace_prefix: \"mem://datasets\"\n")
}

// newControlPlaneWithConfig is newControlPlaneWithDeps with a caller-supplied datasets
// configuration section, for tests that need a different sync_publish_max_objects and so on.
func newControlPlaneWithConfig(t *testing.T, datasetsConfig string) (*chi.Mux, cmdpkg.ServerDeps, string, string) {
	t.Helper()
	useConfig(t, datasetsConfig)

	deps := testDeps(t)
	ctx := t.Context()
	_, err := deps.AuthService.CreateUser(ctx, &model.User{Username: testUsername})
	require.NoError(t, err)
	cred, err := deps.AuthService.AddCredentials(ctx, testUsername, testAccessKeyID, testSecretAccessKey)
	require.NoError(t, err)
	// Admins carries the ACL base grants (fs:* etc.), so the user may read sources;
	// DatasetAdmins carries DatasetsFullAccess (§9.5), so the user may use the control plane.
	require.NoError(t, deps.AuthService.AddUserToGroup(ctx, testUsername, acl.AdminsGroup))
	require.NoError(t, deps.AuthService.AddUserToGroup(ctx, testUsername, DatasetAdminsGroup))

	mux := coreRouter()
	require.NoError(t, RoutesBuilder(ctx, deps, mux))
	return mux, deps, cred.AccessKeyID, cred.SecretAccessKey
}

// doRequest issues a request against the control plane. accessKey/secret are optional: an empty
// access key sends no credentials at all.
func doRequest(t *testing.T, handler http.Handler, method, path, body, accessKey, secret string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	req := httptest.NewRequest(method, APIBasePath+path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if accessKey != "" {
		req.SetBasicAuth(accessKey, secret)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) errorResponse {
	t.Helper()
	var body errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "body: %s", rec.Body.String())
	return body
}

// TestDatasetsSpecLoadsAndValidates checks that api/datasets.yml is a valid OpenAPI 3 document
// that covers every endpoint of DATASETS_DESIGN.md §10.1 and the schemas of §10.2, with the
// status codes of §10.4 documented per operation.
func TestDatasetsSpecLoadsAndValidates(t *testing.T) {
	swagger, err := apigen.GetSwagger()
	require.NoError(t, err)
	require.NoError(t, swagger.Validate(context.Background()))

	t.Run("base path", func(t *testing.T) {
		require.Len(t, swagger.Servers, 1)
		require.Equal(t, APIBasePath, swagger.Servers[0].URL)
	})

	t.Run("every endpoint of §10.1 is declared", func(t *testing.T) {
		endpoints := map[string][]string{
			"/":                             {http.MethodGet, http.MethodPost},
			"/config":                       {http.MethodGet},
			"/metadata-keys":                {http.MethodGet, http.MethodPost},
			"/metadata-keys/{key}":          {http.MethodGet, http.MethodPut, http.MethodDelete},
			"/{dataset}":                    {http.MethodGet, http.MethodDelete},
			"/{dataset}/validate":           {http.MethodPost},
			"/{dataset}/versions":           {http.MethodGet, http.MethodPost},
			"/{dataset}/versions/{version}": {http.MethodGet},
			"/{dataset}/refs/{leftRef}/diff/{rightRef}":         {http.MethodGet},
			"/{dataset}/proposals":                              {http.MethodGet, http.MethodPost},
			"/{dataset}/proposals/{id}":                         {http.MethodGet, http.MethodPatch},
			"/{dataset}/proposals/{id}/merge":                   {http.MethodPost},
			"/{dataset}/access":                                 {http.MethodGet, http.MethodPost},
			"/{dataset}/access/{principal_type}/{principal_id}": {http.MethodDelete},
			"/{dataset}/tasks/{task_id}":                        {http.MethodGet},
		}
		for path, methods := range endpoints {
			item := swagger.Paths.Find(path)
			require.NotNil(t, item, "path %q is missing from the spec", path)
			for _, method := range methods {
				require.NotNil(t, item.GetOperation(method), "%s %s is missing from the spec", method, path)
			}
		}
	})

	t.Run("core schemas of §10.2 are declared", func(t *testing.T) {
		for _, name := range []string{
			"Dataset", "DatasetDefinition", "DataItem", "Version", "DatasetStats",
			"DefinitionDiff", "DataDiffList", "Proposal", "MetadataKey", "Task", "Error",
		} {
			require.Contains(t, swagger.Components.Schemas, name, "schema %q is missing", name)
		}
	})

	t.Run("error schema matches the core API", func(t *testing.T) {
		errSchema := swagger.Components.Schemas["Error"].Value
		require.Equal(t, []string{"message"}, errSchema.Required)
		require.Contains(t, errSchema.Properties, "message")
		// §10.4 adds one entry per validation problem.
		require.Contains(t, errSchema.Properties, "errors")
	})

	t.Run("DataItem is a union discriminated by type", func(t *testing.T) {
		item := swagger.Components.Schemas["DataItem"].Value
		require.True(t, item.Type.Is("object"), "DataItem must be an object schema, got %v", item.Type)
		require.Equal(t, "type", item.Extensions["x-one-of-by"])
		for _, member := range []string{"object", "prefix"} {
			require.Contains(t, item.Properties, member, "DataItem must declare the %q member", member)
		}
		typeSchema := item.Properties["type"].Value
		require.ElementsMatch(t, []any{"object", "prefix", "table", "namespace"}, typeSchema.Enum)
	})

	t.Run("every operation documents the status codes of §10.4", func(t *testing.T) {
		// Codes that must be reachable somewhere in the control plane.
		wantSomewhere := []string{"200", "201", "202", "204", "400", "401", "403", "404", "409", "422", "501"}
		seen := map[string]bool{}
		for path, item := range swagger.Paths.Map() {
			for method, operation := range item.Operations() {
				require.NotEmpty(t, operation.OperationID, "%s %s has no operationId", method, path)
				codes := operation.Responses.Map()
				require.Contains(t, codes, "401", "%s %s must document 401", method, path)
				require.Contains(t, codes, "501", "%s %s must document 501", method, path)
				for code := range codes {
					seen[code] = true
				}
			}
		}
		for _, code := range wantSomewhere {
			require.True(t, seen[code], "status code %s is documented nowhere", code)
		}
	})
}

// TestGeneratedDefinitionTypes checks the generated types are usable as the definition model:
// in particular DataItem is a strong type (not interface{}), so callers and later cards can
// work with the union instead of map[string]interface{}.
func TestGeneratedDefinitionTypes(t *testing.T) {
	var def apigen.DatasetDefinition
	require.NoError(t, json.Unmarshal([]byte(enterpriseDefinition), &def))

	require.Equal(t, 1, def.FormatVersion)
	require.NotNil(t, def.Description)
	require.Equal(t, "Curated customer view for analytics", *def.Description)
	require.Len(t, def.Data, 2)

	// Concatenating into []apigen.DataItem only compiles while DataItem is a named type.
	items := append([]apigen.DataItem{}, def.Data...)
	prefixItem := items[0]
	require.Equal(t, "prefix", prefixItem.Type)
	require.NotNil(t, prefixItem.Prefix)
	require.Equal(t, "data/customers/", prefixItem.Prefix.Path)
	require.Nil(t, prefixItem.Object)

	objectItem := items[1]
	require.Equal(t, "object", objectItem.Type)
	require.NotNil(t, objectItem.Object)
	require.Equal(t, "geo/countries.csv", objectItem.Object.Path)
	require.Nil(t, objectItem.Prefix)

	require.Equal(t, "commit", objectItem.Ref.Type)
	require.Equal(t, "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678", objectItem.Ref.Id)

	require.NotNil(t, def.Metadata)
	metadata := *def.Metadata
	require.Equal(t, "gold", metadata["tier"])

	// Round trip: the definition the control plane serves is the definition it accepted.
	encoded, err := json.Marshal(def)
	require.NoError(t, err)
	var decoded apigen.DatasetDefinition
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, def, decoded)
}

// TestDatasetsUnauthenticated401 is the 401 mapping of §10.4: every route is behind
// api.GenericAuthMiddleware plus the authenticated-user requirement.
func TestDatasetsUnauthenticated401(t *testing.T) {
	handler, _, _ := newControlPlane(t)

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"list datasets", http.MethodGet, "", ""},
		{"collection root", http.MethodGet, "/", ""},
		{"get dataset", http.MethodGet, "/customer-360", ""},
		{"list versions", http.MethodGet, "/customer-360/versions", ""},
		{"get version", http.MethodGet, "/customer-360/versions/v1", ""},
		{"publish version", http.MethodPost, "/customer-360/versions", validPublishBody},
		{"delete dataset", http.MethodDelete, "/customer-360", ""},
		{"config", http.MethodGet, "/config", ""},
		{"list metadata keys", http.MethodGet, "/metadata-keys", ""},
		{"diff", http.MethodGet, "/customer-360/refs/v1/diff/v2?type=data", ""},
		{"task", http.MethodGet, "/customer-360/tasks/01J8Z", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, handler, tc.method, tc.path, tc.body, "", "")
			require.Equal(t, http.StatusUnauthorized, rec.Code, "body: %s", rec.Body.String())
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.Equal(t, ErrAuthenticationRequired.Error(), decodeError(t, rec).Message)
		})
	}

	t.Run("unknown credentials", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/customer-360", "", "unknown-access-key", "unknown-secret")
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Equal(t, api.ErrAuthenticatingRequest.Error(), decodeError(t, rec).Message)
	})
}

// TestDatasetsBadRequest400 is the 400 mapping of §10.4: the request validator rejects a
// malformed request before the stub handler runs, with the error shape of §10.4 (message plus
// one entry per problem).
func TestDatasetsBadRequest400(t *testing.T) {
	handler, accessKey, secret := newControlPlane(t)

	cases := []struct {
		name     string
		method   string
		path     string
		body     string
		wantPath string
	}{
		{
			name:   "malformed json body",
			method: http.MethodPost,
			path:   "",
			body:   `{"name": "customer-360",`,
		},
		{
			name:   "missing required name",
			method: http.MethodPost,
			path:   "",
			body:   `{"definition": ` + enterpriseDefinition + `}`,
		},
		{
			name:     "unknown item type",
			method:   http.MethodPost,
			path:     "",
			body:     validCreateBodyWithItemType("bogus"),
			wantPath: "definition.data[0].type",
		},
		{
			name:     "unsupported format version",
			method:   http.MethodPost,
			path:     "",
			body:     strings.Replace(validCreateBody, `"format_version": 1`, `"format_version": 2`, 1),
			wantPath: "definition.format_version",
		},
		{
			name:   "missing publish message",
			method: http.MethodPost,
			path:   "/customer-360/versions",
			body:   `{"definition": ` + enterpriseDefinition + `}`,
		},
		{
			name:   "invalid dataset name in path",
			method: http.MethodGet,
			path:   "/NOT_A_VALID_NAME",
		},
		{
			name:   "invalid version in path",
			method: http.MethodGet,
			path:   "/customer-360/versions/nope",
		},
		{
			name:   "invalid metadata key in path",
			method: http.MethodGet,
			path:   "/metadata-keys/Bad-Key",
		},
		{
			name:   "invalid access principal type",
			method: http.MethodDelete,
			path:   "/customer-360/access/service-account/alice",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, handler, tc.method, tc.path, tc.body, accessKey, secret)
			require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
			body := decodeError(t, rec)
			require.NotEmpty(t, body.Message)
			require.NotEmpty(t, body.Errors, "400 responses list every problem (§10.4)")
			for _, detail := range body.Errors {
				require.NotEmpty(t, detail.Message)
			}
			if tc.wantPath != "" {
				require.Contains(t, pathsOf(body.Errors), tc.wantPath)
			}
		})
	}
}

// TestDatasetsNotImplemented501 is the 501 mapping for the operations of later phases: they
// are still stubs and say so, and the answer is reached only after authentication and request
// validation succeed. The implemented operations - CRUD, validate, access grants, version
// publish/read and the version diff - are covered by the CRUD, versions_http_test.go and
// diff_test.go tests instead, so their routes are absent from the table below.
func TestDatasetsNotImplemented501(t *testing.T) {
	handler, accessKey, secret := newControlPlane(t)

	cases := []struct {
		operation string
		method    string
		path      string
		body      string
	}{
		{"GetDatasetsConfig", http.MethodGet, "/config", ""},
		{"ListMetadataKeys", http.MethodGet, "/metadata-keys", ""},
		{"CreateMetadataKey", http.MethodPost, "/metadata-keys", validMetadataKeyBody},
		{"GetMetadataKey", http.MethodGet, "/metadata-keys/classification", ""},
		{"UpdateMetadataKey", http.MethodPut, "/metadata-keys/classification", `{"required": true}`},
		{"DeleteMetadataKey", http.MethodDelete, "/metadata-keys/classification", ""},
		// The version and task routes of §10.1 are implemented; their HTTP contract is covered
		// by versions_http_test.go. The DiffRefs route of §10.3 is implemented too; its HTTP
		// contract is covered by TestDatasetsHTTPDiff in diff_test.go. None of them answer 501
		// any more, so none has a row here.
		{"ListProposals", http.MethodGet, "/customer-360/proposals?status=open", ""},
		{"CreateProposal", http.MethodPost, "/customer-360/proposals",
			`{"title": "add countries", "definition": ` + enterpriseDefinition + `}`},
		{"GetProposal", http.MethodGet, "/customer-360/proposals/01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8", ""},
		{"UpdateProposal", http.MethodPatch, "/customer-360/proposals/01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8", `{"status": "closed"}`},
		{"MergeProposal", http.MethodPost, "/customer-360/proposals/01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8/merge", ""},
	}
	for _, tc := range cases {
		t.Run(tc.operation, func(t *testing.T) {
			rec := doRequest(t, handler, tc.method, tc.path, tc.body, accessKey, secret)
			require.Equal(t, http.StatusNotImplemented, rec.Code, "body: %s", rec.Body.String())
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			message := decodeError(t, rec).Message
			// Clearly marked not-implemented error naming the operation.
			require.True(t, strings.HasPrefix(message, ErrNotImplemented.Error()+": "), "message: %q", message)
			require.Contains(t, message, tc.operation)
		})
	}

	t.Run("validation runs before the stub", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodPost, "/customer-360/versions", `{"message": "x"}`, accessKey, secret)
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func pathsOf(details []errorDetail) []string {
	paths := make([]string, 0, len(details))
	for _, detail := range details {
		paths = append(paths, detail.Path)
	}
	return paths
}

// validCreateBodyWithItemType is validCreateBody with every item type replaced, used to check the
// `type` enum of DataItem.
func validCreateBodyWithItemType(itemType string) string {
	return strings.Replace(validCreateBody, `"type": "prefix"`, `"type": "`+itemType+`"`, 1)
}

// TestNewRouterRequiresAuthMiddlewareShape pins the constructor contract used by RoutesBuilder:
// without an auth middleware the router still validates and serves, and every route answers 401
// because no principal can be authenticated.
func TestNewRouterWithoutAuthMiddleware(t *testing.T) {
	// A server with no service is enough: requireUser answers 401 before any handler runs.
	handler, err := NewRouter(nil, newServer(nil, nil))
	require.NoError(t, err)

	// NewRouter returns the sub-router; the validator resolves paths through the spec server
	// entry, so mount it exactly as RoutesBuilder does.
	mux := chi.NewRouter()
	mux.Mount(APIBasePath, handler)

	req := httptest.NewRequest(http.MethodGet, APIBasePath+"/config", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	var body errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, ErrAuthenticationRequired.Error(), body.Message)
}

// TestDatasetsUnknownRoute404And405 checks the control plane answers routing problems with the
// datasets error shape instead of chi's plain-text answers: an unknown path under the control
// plane is 404 (§10.4), an unsupported method on a known path is 405.
func TestDatasetsUnknownRoute404And405(t *testing.T) {
	handler, accessKey, secret := newControlPlane(t)

	t.Run("unknown path", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/customer-360/not-a-route", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.NotEmpty(t, decodeError(t, rec).Message)
	})

	t.Run("unsupported method", func(t *testing.T) {
		// Fixed paths (here /config) resolve to a path item without the requested method, so the
		// router reports 405. Parametrised paths report "no matching operation" instead and are
		// answered 404 by kin-openapi's router.
		rec := doRequest(t, handler, http.MethodPatch, "/config", "", accessKey, secret)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code, "body: %s", rec.Body.String())
		require.NotEmpty(t, decodeError(t, rec).Message)
	})

	t.Run("unsupported method on a parametrised path", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodPatch, "/customer-360", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
	})
}
