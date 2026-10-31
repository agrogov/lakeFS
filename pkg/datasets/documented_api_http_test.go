package datasets

// documented_api_http_test.go pins the wire-level findings of the docs-parity review that are
// about representation rather than routing: DOC-11 (metadata is always emitted as arrays),
// DOC-12 (format_version is server-set), DOC-13 (a namespace item names a string) and DOC-14
// (create always publishes v1).

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// TestDatasetsHTTPCreateAlwaysPublishesV1 is DOC-14: the documented create has no empty-dataset
// mode, and async is an explicit opt-in that does not change the version the create produces.
func TestDatasetsHTTPCreateAlwaysPublishesV1(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)

	t.Run("synchronous create publishes v1", func(t *testing.T) {
		def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")
		rec := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "doc-create", def, false), accessKey, secret)
		require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

		var created apigen.Dataset
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
		require.Equal(t, "doc-create", created.Id)
		require.Equal(t, 1, derefInt(created.Version), "every create publishes v1")
		require.NotNil(t, created.Definition)
		require.NotNil(t, created.Stats)

		// The default version message names the dataset; a create without publish.message
		// must not leave the dataset without a version.
		versionRec := doRequest(t, handler, http.MethodGet, "/doc-create/versions/v1", "", accessKey, secret)
		require.Equal(t, http.StatusOK, versionRec.Code, "body: %s", versionRec.Body.String())
		var version apigen.DatasetVersion
		require.NoError(t, json.Unmarshal(versionRec.Body.Bytes(), &version))
		require.Equal(t, DefaultCreateMessage("doc-create"), deref(version.Message))
	})

	t.Run("async create publishes v1 as a task", func(t *testing.T) {
		def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")
		rec := doRequest(t, handler, http.MethodPost, "?async=true", createDatasetBody(t, "doc-async-create", def, false), accessKey, secret)
		require.Equal(t, http.StatusAccepted, rec.Code, "body: %s", rec.Body.String())

		var task apigen.Task
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &task))
		require.NotEmpty(t, task.Id)

		deadline := time.Now().Add(30 * time.Second)
		var final apigen.Task
		for {
			rec := doRequest(t, handler, http.MethodGet, "/doc-async-create/tasks/"+task.Id, "", accessKey, secret)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &final))
			if final.Status == TaskStatusCompleted || final.Status == TaskStatusFailed {
				break
			}
			require.True(t, time.Now().Before(deadline), "the asynchronous create did not finish in time")
			time.Sleep(10 * time.Millisecond)
		}
		require.Equal(t, TaskStatusCompleted, final.Status, "task error: %v", final.Error)
		require.NotNil(t, final.Version)
		require.Equal(t, 1, *final.Version)

		datasetRec := doRequest(t, handler, http.MethodGet, "/doc-async-create", "", accessKey, secret)
		require.Equal(t, http.StatusOK, datasetRec.Code)
		var dataset apigen.Dataset
		require.NoError(t, json.Unmarshal(datasetRec.Body.Bytes(), &dataset))
		require.Equal(t, 1, derefInt(dataset.Version))
		require.Equal(t, DatasetStateActive, deref(dataset.State))
	})
}

// TestDatasetsHTTPDefinitionCompat is DOC-11 with DOC-12 and DOC-13: format_version may be
// omitted, a namespace item names its namespace as a string, and metadata is accepted as
// scalars or arrays but always emitted as arrays of strings.
func TestDatasetsHTTPDefinitionCompat(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)

	t.Run("format_version is optional and metadata is emitted as arrays", func(t *testing.T) {
		body := `{
			"name": "doc-compat",
			"definition": {
				"metadata": {"owner": "data-platform", "tags": ["a", "b"], "count": 3, "flag": true},
				"data": [{
					"target": "reference/countries.csv",
					"repository": "handler-src",
					"ref": {"type": "commit", "id": "` + commit.String() + `"},
					"type": "object",
					"object": {"path": "geo/countries.csv"}
				}]
			}
		}`
		rec := doRequest(t, handler, http.MethodPost, "", body, accessKey, secret)
		require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

		var created map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
		definition, ok := created["definition"].(map[string]any)
		require.True(t, ok, "the dataset carries its definition")
		require.Equal(t, float64(1), definition["format_version"], "the server stores format_version 1")

		metadata, ok := definition["metadata"].(map[string]any)
		require.True(t, ok)
		// Every value is a JSON array of strings on output (DOC-11), whatever was sent.
		require.Equal(t, []any{"data-platform"}, metadata["owner"])
		require.Equal(t, []any{"a", "b"}, metadata["tags"])
		require.Equal(t, []any{"3"}, metadata["count"])
		require.Equal(t, []any{"true"}, metadata["flag"])
	})

	t.Run("an empty data list is still rejected", func(t *testing.T) {
		// DOC-12 drops minItems from the spec; the validator keeps the non-empty rule so the
		// client still gets the usual data path in the 400 body.
		body := `{"name": "doc-empty-data", "definition": {"data": []}}`
		rec := doRequest(t, handler, http.MethodPost, "", body, accessKey, secret)
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
		require.Contains(t, pathsOf(decodeError(t, rec).Errors), "data")
	})

	t.Run("a namespace item is a string and answers 501", func(t *testing.T) {
		// DOC-13: ent-swagger declares "namespace": "analytics", not {"name": "analytics"}.
		body := `{"definition": {
			"format_version": 1,
			"data": [{
				"target": "analytics",
				"repository": "handler-src",
				"ref": {"type": "commit", "id": "` + commit.String() + `"},
				"type": "namespace",
				"namespace": "analytics"
			}]
		}}`
		rec := doRequest(t, handler, http.MethodPost, "/doc-namespace/validate", body, accessKey, secret)
		require.Equal(t, http.StatusNotImplemented, rec.Code,
			"the item decodes and reaches validation, so it is the reserved-type 501: %s", rec.Body.String())
	})
}
