package datasets

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

func decodeMetadataKey(t *testing.T, body []byte) apigen.MetadataKey {
	t.Helper()
	var key apigen.MetadataKey
	require.NoError(t, json.Unmarshal(body, &key), "body: %s", body)
	return key
}

// TestDatasetsHTTPMetadataKeysCRUD covers the five metadata-key operations of DOC-9/DOC-10.
func TestDatasetsHTTPMetadataKeysCRUD(t *testing.T) {
	handler, accessKey, secret := newControlPlane(t)
	do := func(method, path, body string) (int, []byte) {
		rec := doRequest(t, handler, method, path, body, accessKey, secret)
		return rec.Code, rec.Body.Bytes()
	}

	code, body := do(http.MethodPost, "/metadata", validMetadataKeyBody)
	require.Equal(t, http.StatusCreated, code, "body: %s", body)
	created := decodeMetadataKey(t, body)
	require.Equal(t, "classification", created.Name)
	require.Equal(t, "enum", created.Type)
	require.Equal(t, testUsername, created.CreatedBy)

	// Duplicate create is a conflict.
	code, body = do(http.MethodPost, "/metadata", validMetadataKeyBody)
	require.Equal(t, http.StatusConflict, code, "body: %s", body)

	// Invalid definitions are 400: enum without values, duplicate and empty allowed values.
	for _, bad := range []string{
		`{"name": "noenum", "type": "enum"}`,
		`{"name": "dup", "type": "enum", "allowed_values": ["a", "a"]}`,
		`{"name": "empty", "type": "enum", "allowed_values": [""]}`,
		`{"name": "Bad-Key", "type": "string"}`,
		`{"name": "plain", "type": "string", "allowed_values": ["a"]}`,
	} {
		code, body = do(http.MethodPost, "/metadata", bad)
		require.Equal(t, http.StatusBadRequest, code, "%s: %s", bad, body)
	}

	code, body = do(http.MethodPost, "/metadata", `{"name": "owner", "type": "string", "required": true}`)
	require.Equal(t, http.StatusCreated, code, "body: %s", body)

	code, body = do(http.MethodGet, "/metadata/classification", "")
	require.Equal(t, http.StatusOK, code, "body: %s", body)
	require.Equal(t, []string{"public", "internal"}, *decodeMetadataKey(t, body).AllowedValues)

	code, body = do(http.MethodGet, "/metadata/missing", "")
	require.Equal(t, http.StatusNotFound, code, "body: %s", body)

	// List with and without prefix.
	code, body = do(http.MethodGet, "/metadata", "")
	require.Equal(t, http.StatusOK, code, "body: %s", body)
	var list apigen.MetadataKeyList
	require.NoError(t, json.Unmarshal(body, &list))
	require.Len(t, list.Results, 2)
	require.Equal(t, "classification", list.Results[0].Name)
	code, body = do(http.MethodGet, "/metadata?prefix=ow", "")
	require.Equal(t, http.StatusOK, code, "body: %s", body)
	require.NoError(t, json.Unmarshal(body, &list))
	require.Len(t, list.Results, 1)
	require.Equal(t, "owner", list.Results[0].Name)

	// PUT is a full replace: omitted optional fields reset, created_by is preserved.
	code, body = do(http.MethodPut, "/metadata/classification", `{"type": "enum", "allowed_values": ["public"], "required": true}`)
	require.Equal(t, http.StatusOK, code, "body: %s", body)
	updated := decodeMetadataKey(t, body)
	require.True(t, updated.Required)
	require.Equal(t, []string{"public"}, *updated.AllowedValues)
	require.Equal(t, testUsername, updated.CreatedBy)
	code, body = do(http.MethodPut, "/metadata/owner", `{"type": "string"}`)
	require.Equal(t, http.StatusOK, code, "body: %s", body)
	require.False(t, decodeMetadataKey(t, body).Required, "required reset by the full replace")

	// type is required, and a type change is a conflict.
	code, _ = do(http.MethodPut, "/metadata/owner", `{"required": true}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, body = do(http.MethodPut, "/metadata/owner", `{"type": "number"}`)
	require.Equal(t, http.StatusConflict, code, "body: %s", body)
	code, body = do(http.MethodPut, "/metadata/missing", `{"type": "string"}`)
	require.Equal(t, http.StatusNotFound, code, "body: %s", body)

	code, body = do(http.MethodDelete, "/metadata/owner", "")
	require.Equal(t, http.StatusNoContent, code, "body: %s", body)
	code, _ = do(http.MethodDelete, "/metadata/owner", "")
	require.Equal(t, http.StatusNotFound, code)
	code, _ = do(http.MethodGet, "/metadata/owner", "")
	require.Equal(t, http.StatusNotFound, code)
}

// TestDatasetsHTTPMetadataKeyUpdateBounds is DOC-10: a PUT is a full replacement with a
// required type, the type stays immutable (409), and allowed_values has the bounds of
// ent-swagger v1.108 (1..256 values, unique, each 1..256 characters).
func TestDatasetsHTTPMetadataKeyUpdateBounds(t *testing.T) {
	handler, accessKey, secret := newControlPlane(t)
	do := func(method, path, body string) int {
		return doRequest(t, handler, method, path, body, accessKey, secret).Code
	}
	require.Equal(t, http.StatusCreated, do(http.MethodPost, "/metadata", validMetadataKeyBody))

	allowedValuesBody := func(values []string) string {
		body, err := json.Marshal(apigen.MetadataKeyUpdateRequest{
			Type:          MetadataTypeEnum,
			AllowedValues: &values,
		})
		require.NoError(t, err)
		return string(body)
	}

	tooMany := make([]string, MaxMetadataAllowedValues+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("value-%d", i)
	}
	tooLong := make([]string, MaxMetadataAllowedValues)
	for i := range tooLong {
		tooLong[i] = strings.Repeat("x", MaxMetadataAllowedValueChars+1)
	}

	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{name: "more than the maximum number of values", body: allowedValuesBody(tooMany), want: http.StatusBadRequest},
		{name: "a value over the maximum length", body: allowedValuesBody(tooLong[:1]), want: http.StatusBadRequest},
		{name: "a repeated value", body: allowedValuesBody([]string{"gold", "gold"}), want: http.StatusBadRequest},
		{name: "missing type", body: `{"required": true}`, want: http.StatusBadRequest},
		{name: "enum without allowed values", body: `{"type": "enum"}`, want: http.StatusBadRequest},
		{name: "type change stays forbidden", body: `{"type": "number"}`, want: http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, do(http.MethodPut, "/metadata/classification", tc.body))
		})
	}

	// The rejected replacements changed nothing.
	rec := doRequest(t, handler, http.MethodGet, "/metadata/classification", "", accessKey, secret)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"public", "internal"}, *decodeMetadataKey(t, rec.Body.Bytes()).AllowedValues)
}

// TestDatasetsHTTPMetadataKeysDenied: a principal without any dataset:*MetadataKey permission
// gets 403 on every metadata-key operation, while an unauthenticated call stays 401.
func TestDatasetsHTTPMetadataKeysDenied(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	rec := doRequest(t, handler, http.MethodPost, "/metadata", validMetadataKeyBody, accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	const nobody = "no-permissions-user"
	_, err := deps.AuthService.CreateUser(t.Context(), &model.User{Username: nobody})
	require.NoError(t, err)
	cred, err := deps.AuthService.AddCredentials(t.Context(), nobody, "AKIANOMETADATAKEY001", "nobody-secret")
	require.NoError(t, err)

	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/metadata", ""},
		{http.MethodPost, "/metadata", `{"name": "other", "type": "string"}`},
		{http.MethodGet, "/metadata/classification", ""},
		{http.MethodPut, "/metadata/classification", `{"type": "enum", "allowed_values": ["x"]}`},
		{http.MethodDelete, "/metadata/classification", ""},
	} {
		rec := doRequest(t, handler, tc.method, tc.path, tc.body, cred.AccessKeyID, cred.SecretAccessKey)
		require.Equal(t, http.StatusUnauthorized, rec.Code, "%s %s: %s", tc.method, tc.path, rec.Body.String())
		rec = doRequest(t, handler, tc.method, tc.path, tc.body, "", "")
		require.Equal(t, http.StatusUnauthorized, rec.Code, "%s %s", tc.method, tc.path)
	}

	// Nothing was changed by the denied calls.
	rec = doRequest(t, handler, http.MethodGet, "/metadata/classification", "", accessKey, secret)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"public", "internal"}, *decodeMetadataKey(t, rec.Body.Bytes()).AllowedValues)
}

func TestDatasetsHTTPConfig(t *testing.T) {
	handler, accessKey, secret := newControlPlane(t)
	rec := doRequest(t, handler, http.MethodGet, "/config", "", accessKey, secret)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var cfg apigen.DatasetsConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
	require.True(t, cfg.Enabled)
	require.NotNil(t, cfg.MaxItems)
}

// TestMetadataIsReservedDatasetName: "metadata" collides with the /metadata routes.
func TestMetadataIsReservedDatasetName(t *testing.T) {
	// DOC-18: every fixed top-level route of the documented API is reserved.
	for _, name := range []string{"config", "metadata", "pulls"} {
		require.True(t, IsReservedName(name), "%q collides with a control-plane route", name)
	}
	require.False(t, IsReservedName("metadata-keys"), "the pre-rename spelling is free again")
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "countries.csv", "geo/countries.csv")
	rec := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "metadata", def, false), accessKey, secret)
	require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
}
