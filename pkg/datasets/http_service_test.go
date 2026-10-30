package datasets

// http_service_test.go covers the HTTP contract of this phase: the handler bodies that call
// the service, exercising the status-code mapping of DATASETS_DESIGN.md §10.4 over httptest
// with basic authentication (the real api.GenericAuthMiddleware and requireUser are in the
// path, because the tests mount the router exactly as RoutesBuilder does).

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// seedHandlerSource creates a source repository with one committed object and returns the
// commit ID the definitions of this file pin to.
func seedHandlerSource(t *testing.T, cat *catalog.Catalog) graveler.CommitID {
	t.Helper()
	ctx := t.Context()
	_, err := cat.CreateRepository(ctx, "handler-src", "", "mem://handler-src-ns", "main", false)
	require.NoError(t, err)
	require.NoError(t, cat.CreateEntry(ctx, "handler-src", "main", catalog.DBEntry{
		Path:            "geo/countries.csv",
		PhysicalAddress: "objects/000/countries.csv",
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    time.Now(),
		Size:            7,
		Checksum:        "deadbeef",
		ContentType:     "text/csv",
		Metadata:        catalog.Metadata{"origin": "handler-test"},
	}))
	log, err := cat.Commit(ctx, "handler-src", "main", "seed", "tester", nil, nil, nil, false)
	require.NoError(t, err)
	return graveler.CommitID(log.Reference)
}

// marshalBody marshals a request body for doRequest.
func marshalBody(t *testing.T, body any) string {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	return string(encoded)
}

// createDatasetBody builds a create request with an optional publish instruction.
func createDatasetBody(t *testing.T, name string, def Definition, publish bool) string {
	t.Helper()
	body := apigen.DatasetCreateRequest{Name: name, Definition: definitionToAPI(def)}
	if publish {
		message := "initial dataset"
		body.Publish = &apigen.PublishOptions{Message: &message}
	}
	return marshalBody(t, body)
}

func TestDatasetsHTTPCreateGetDelete(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")
	def.Metadata = map[string]any{"tier": "gold"}

	create := createDatasetBody(t, "handler-set", def, true)
	rec := doRequest(t, handler, http.MethodPost, "", create, accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	var created apigen.Dataset
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.Equal(t, "handler-set", created.Name)
	require.Equal(t, 1, created.LatestVersion)
	require.Equal(t, DatasetStateActive, created.State)
	require.NotNil(t, created.Definition)
	require.NotNil(t, created.Stats)
	require.Equal(t, int64(1), created.Stats.Objects)
	require.Equal(t, int64(7), created.Stats.Bytes)

	t.Run("get", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/handler-set", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var got apigen.Dataset
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, created.Name, got.Name)
		require.Equal(t, created.LatestVersion, got.LatestVersion)
	})

	t.Run("missing dataset is 404", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/missing-set", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
		require.Contains(t, decodeError(t, rec).Message, "not found")
	})

	t.Run("duplicate name is 409", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodPost, "", create, accessKey, secret)
		require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("repository name is 409", func(t *testing.T) {
		_, err := deps.Catalog.CreateRepository(t.Context(), "handler-repo", "", "mem://handler-repo-ns", "main", false)
		require.NoError(t, err)
		body := createDatasetBody(t, "handler-repo", def, false)
		rec := doRequest(t, handler, http.MethodPost, "", body, accessKey, secret)
		require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
		require.Contains(t, decodeError(t, rec).Message, "repository")
	})

	t.Run("list with metadata filter", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "?metadata.tier=gold", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var list apigen.DatasetList
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Len(t, list.Results, 1)
		require.Equal(t, "handler-set", list.Results[0].Name)
		require.False(t, list.Pagination.HasMore)

		rec = doRequest(t, handler, http.MethodGet, "?metadata.tier=bronze", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Empty(t, list.Results)
	})

	t.Run("delete", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodDelete, "/handler-set", "", accessKey, secret)
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())
		require.Empty(t, rec.Body.String())

		rec = doRequest(t, handler, http.MethodGet, "/handler-set", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code)

		rec = doRequest(t, handler, http.MethodDelete, "/handler-set", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestDatasetsHTTPValidationErrors(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)

	t.Run("400 lists every problem", func(t *testing.T) {
		// Two items with the same target: a structural problem only the service can see, so
		// it is the §10.4 400 answer with the per-problem list.
		def := Definition{
			FormatVersion: FormatVersion,
			Data: []DataItem{
				objectDefinition("handler-src", commit.String(), "dup", "geo/countries.csv").Data[0],
				objectDefinition("handler-src", commit.String(), "dup", "geo/countries.csv").Data[0],
			},
		}
		body := createDatasetBody(t, "bad-set", def, false)
		rec := doRequest(t, handler, http.MethodPost, "", body, accessKey, secret)
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
		body400 := decodeError(t, rec)
		require.NotEmpty(t, body400.Errors)
		require.Contains(t, pathsOf(body400.Errors), "data[1].target")
	})

	t.Run("422 storage ID mismatch", func(t *testing.T) {
		// The source repository lives on a storage ID the datasets configuration does not
		// use, so its full addresses could not be read back (§5.2 rule 4, §6.3).
		_, err := deps.Catalog.CreateRepository(t.Context(), "other-storage", "another-storage", "mem://other-storage-ns", "main", false)
		require.NoError(t, err)
		def := objectDefinition("other-storage", "0123456789abcdef0123456789abcdef01234567", "x", "x")
		body := createDatasetBody(t, "mismatch-set", def, true)
		rec := doRequest(t, handler, http.MethodPost, "", body, accessKey, secret)
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code, "body: %s", rec.Body.String())
		require.Contains(t, decodeError(t, rec).Message, "storage ID")
	})

	t.Run("403 names the source item", func(t *testing.T) {
		ctx := t.Context()
		const weak = "weak-publisher"
		_, err := deps.AuthService.CreateUser(ctx, &model.User{Username: weak})
		require.NoError(t, err)
		weakCred, err := deps.AuthService.AddCredentials(ctx, weak, "AKIAWEAKPUBLISHER001", "weak-secret")
		require.NoError(t, err)
		// DatasetsFullAccess but none of the ACL base grants: the caller may create a
		// dataset, but must not be able to read the source it attaches (§9.3).
		require.NoError(t, deps.AuthService.AddUserToGroup(ctx, weak, DatasetAdminsGroup))

		def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")
		body := createDatasetBody(t, "denied-set", def, true)
		rec := doRequest(t, handler, http.MethodPost, "", body, weakCred.AccessKeyID, weakCred.SecretAccessKey)
		require.Equal(t, http.StatusForbidden, rec.Code, "body: %s", rec.Body.String())
		message := decodeError(t, rec).Message
		require.Contains(t, message, "data[0]")
		require.Contains(t, message, "handler-src")
	})
}

func TestDatasetsHTTPValidate(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)

	def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")

	t.Run("resolves a branch ref to a commit", func(t *testing.T) {
		def.Data[0].Ref = SourceRef{Type: RefTypeBranch, ID: "main"}
		body := marshalBody(t, apigen.DatasetValidateRequest{Definition: definitionToAPI(def)})
		rec := doRequest(t, handler, http.MethodPost, "/dry-run/validate", body, accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var result apigen.DatasetValidateResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
		require.Equal(t, "commit", result.Definition.Data[0].Ref.Type)
		require.Equal(t, commit.String(), result.Definition.Data[0].Ref.Id)
		require.NotNil(t, result.Stats)
		require.Equal(t, int64(1), result.Stats.Objects)
	})

	t.Run("400 from the service", func(t *testing.T) {
		bad := objectDefinition("handler-src", commit.String(), "failed", "no/such/key")
		body := marshalBody(t, apigen.DatasetValidateRequest{Definition: definitionToAPI(bad)})
		rec := doRequest(t, handler, http.MethodPost, "/dry-run/validate", body, accessKey, secret)
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
		require.Contains(t, pathsOf(decodeError(t, rec).Errors), "data[0].object.path")
	})
}

func TestDatasetsHTTPAccessGrants(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "countries.csv", "geo/countries.csv")

	create := createDatasetBody(t, "access-set", def, true)
	rec := doRequest(t, handler, http.MethodPost, "", create, accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	_, err := deps.AuthService.CreateUser(t.Context(), &model.User{Username: "bob"})
	require.NoError(t, err)

	grantBody := `{"principal_type": "user", "principal_id": "bob"}`
	rec = doRequest(t, handler, http.MethodPost, "/access-set/access", grantBody, accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())
	var grant apigen.AccessGrant
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &grant))
	require.Equal(t, PrincipalTypeUser, grant.PrincipalType)
	require.Equal(t, "bob", grant.PrincipalId)

	rec = doRequest(t, handler, http.MethodGet, "/access-set/access", "", accessKey, secret)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var list apigen.AccessGrantList
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	require.Len(t, list.Results, 1)
	require.Equal(t, "bob", list.Results[0].PrincipalId)

	t.Run("unknown principal is 404", func(t *testing.T) {
		body := `{"principal_type": "user", "principal_id": "nobody"}`
		rec := doRequest(t, handler, http.MethodPost, "/access-set/access", body, accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("revoke round trip", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodDelete, "/access-set/access/user/bob", "", accessKey, secret)
		require.Equal(t, http.StatusNoContent, rec.Code, "body: %s", rec.Body.String())

		rec = doRequest(t, handler, http.MethodGet, "/access-set/access", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var list apigen.AccessGrantList
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Empty(t, list.Results)

		rec = doRequest(t, handler, http.MethodDelete, "/access-set/access/user/bob", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code)
	})
}
