package datasets

// versions_http_test.go covers the HTTP contract of publishing and reading versions
// (DATASETS_DESIGN.md §10.1 and §6.5) end to end: the generated request validator, the real
// api.GenericAuthMiddleware, the generated server and the handlers, mounted exactly as
// RoutesBuilder mounts them.

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// seedSecondHandlerSource commits a second object into handler-src, so a publish of it has more
// than one object and can be pushed over a deliberately low sync_publish_max_objects.
func seedSecondHandlerSource(t *testing.T, cat *catalog.Catalog) graveler.CommitID {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, cat.CreateEntry(ctx, "handler-src", "main", catalog.DBEntry{
		Path:            "geo/cities.csv",
		PhysicalAddress: "objects/001/cities.csv",
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    time.Now(),
		Size:            9,
		Checksum:        "feedface",
		ContentType:     "text/csv",
	}))
	log, err := cat.Commit(ctx, "handler-src", "main", "seed cities", "tester", nil, nil, nil, false)
	require.NoError(t, err)
	return graveler.CommitID(log.Reference)
}

// publishBody renders a POST /{dataset}/versions body.
func publishBody(def Definition, message string, expectedVersion *int) string {
	body := apigen.VersionPublishRequest{
		Definition:      definitionToAPI(def),
		Message:         message,
		ExpectedVersion: expectedVersion,
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

// TestDatasetsHTTPPublishAndReadVersions covers the 201/200 path: publish v1, read it back with
// vN, N and latest, list the versions newest first, then publish v2 and watch both the list and
// latest move.
func TestDatasetsHTTPPublishAndReadVersions(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")

	created := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "http-versions", def, false), accessKey, secret)
	require.Equal(t, http.StatusCreated, created.Code, "body: %s", created.Body.String())

	// 201 with the version, not 202: the publish is below sync_publish_max_objects.
	published := doRequest(t, handler, http.MethodPost, "/http-versions/versions",
		publishBody(def, "initial publish", nil), accessKey, secret)
	require.Equal(t, http.StatusCreated, published.Code, "body: %s", published.Body.String())
	var version apigen.Version
	require.NoError(t, json.Unmarshal(published.Body.Bytes(), &version))
	require.Equal(t, 1, version.Version)
	require.Equal(t, "initial publish", deref(version.Message))
	require.Equal(t, testUsername, version.Publisher)
	require.NotEmpty(t, version.CommitId)
	require.NotNil(t, version.Definition)
	require.NotNil(t, version.Stats)
	require.Equal(t, int64(1), version.Stats.Objects)

	// GET /{ds}/versions/{vN|N|latest}.
	for _, ref := range []string{"v1", "1", "latest"} {
		rec := doRequest(t, handler, http.MethodGet, "/http-versions/versions/"+ref, "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "ref %q: %s", ref, rec.Body.String())
		var got apigen.Version
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, 1, got.Version, "ref %q", ref)
		require.Equal(t, version.CommitId, got.CommitId, "ref %q", ref)
	}

	// Publish v2 from a new source commit and check that the list is newest first.
	secondCommit := seedSecondHandlerSource(t, deps.Catalog)
	secondDef := objectDefinition("handler-src", secondCommit.String(), "reference/cities.csv", "geo/cities.csv")
	rec := doRequest(t, handler, http.MethodPost, "/http-versions/versions",
		publishBody(secondDef, "second publish", ptr(1)), accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	list := doRequest(t, handler, http.MethodGet, "/http-versions/versions?amount=10", "", accessKey, secret)
	require.Equal(t, http.StatusOK, list.Code, "body: %s", list.Body.String())
	var versions struct {
		Results    []apigen.Version `json:"results"`
		Pagination struct {
			HasMore    bool `json:"has_more"`
			Results    int  `json:"results"`
			MaxPerPage int  `json:"max_per_page"`
		} `json:"pagination"`
	}
	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &versions))
	require.Len(t, versions.Results, 2)
	require.Equal(t, []int{2, 1}, []int{versions.Results[0].Version, versions.Results[1].Version})
	require.Equal(t, 2, versions.Pagination.Results)
	require.False(t, versions.Pagination.HasMore)

	// The dataset's latest_version moved to 2.
	got := doRequest(t, handler, http.MethodGet, "/http-versions", "", accessKey, secret)
	require.Equal(t, http.StatusOK, got.Code)
	var dataset apigen.Dataset
	require.NoError(t, json.Unmarshal(got.Body.Bytes(), &dataset))
	require.Equal(t, 2, dataset.LatestVersion)

	// Failures: an unknown version, task and dataset are 404; a malformed version ref is the
	// 400 of §10.4 (it names the offending parameter).
	require.Equal(t, http.StatusNotFound,
		doRequest(t, handler, http.MethodGet, "/http-versions/versions/v99", "", accessKey, secret).Code)
	require.Equal(t, http.StatusBadRequest,
		doRequest(t, handler, http.MethodGet, "/http-versions/versions/v0", "", accessKey, secret).Code)
	require.Equal(t, http.StatusNotFound,
		doRequest(t, handler, http.MethodGet, "/http-versions/tasks/01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8", "", accessKey, secret).Code)
	require.Equal(t, http.StatusNotFound,
		doRequest(t, handler, http.MethodGet, "/no-such-dataset/versions", "", accessKey, secret).Code)
}

// TestDatasetsHTTPPublishConflicts covers the 409 bodies of §5.4: expected_version mismatch
// carries the current latest_version, and a no-op publish is a plain conflict.
func TestDatasetsHTTPPublishConflicts(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")

	created := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "http-conflict", def, true), accessKey, secret)
	require.Equal(t, http.StatusCreated, created.Code, "body: %s", created.Body.String())

	t.Run("expected_version mismatch", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodPost, "/http-conflict/versions",
			publishBody(def, "stale", ptr(0)), accessKey, secret)
		require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())

		var body struct {
			Message       string `json:"message"`
			LatestVersion int    `json:"latest_version"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Equal(t, 1, body.LatestVersion, "a 409 names the version the client should retry from")
		require.Contains(t, body.Message, "expected_version")
	})

	t.Run("no changes", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodPost, "/http-conflict/versions",
			publishBody(def, "again", nil), accessKey, secret)
		require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
		require.Contains(t, decodeError(t, rec).Message, ErrNoChanges.Error())
	})
}

// TestDatasetsHTTPAsyncPublish covers the 202 path of §6.5 over HTTP: a publish above
// sync_publish_max_objects answers with a task, and polling GET /{ds}/tasks/{task} sees it
// complete and report the version it produced.
func TestDatasetsHTTPAsyncPublish(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithConfig(t,
		"datasets:\n  enabled: true\n  storage_namespace_prefix: \"mem://datasets\"\n  sync_publish_max_objects: 1\n")

	seedHandlerSource(t, deps.Catalog)
	commit := seedSecondHandlerSource(t, deps.Catalog)
	// Two objects: above the limit of 1, so the publish runs asynchronously.
	def := Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{
			{
				Target: "reference/countries.csv", Repository: "handler-src",
				Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type: ItemTypeObject, Object: &PathSource{Path: "geo/countries.csv"},
			},
			{
				Target: "reference/cities.csv", Repository: "handler-src",
				Ref:  SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type: ItemTypeObject, Object: &PathSource{Path: "geo/cities.csv"},
			},
		},
	}

	created := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "http-async", def, false), accessKey, secret)
	require.Equal(t, http.StatusCreated, created.Code, "body: %s", created.Body.String())

	published := doRequest(t, handler, http.MethodPost, "/http-async/versions",
		publishBody(def, "async publish", nil), accessKey, secret)
	require.Equal(t, http.StatusAccepted, published.Code, "body: %s", published.Body.String())
	var task apigen.Task
	require.NoError(t, json.Unmarshal(published.Body.Bytes(), &task))
	require.NotEmpty(t, task.Id)
	require.Contains(t, []string{TaskStatusPending, TaskStatusRunning, TaskStatusCompleted}, task.Status)
	require.Nil(t, task.Version)

	// Poll until the task completes. The publish runs on the service's own context, so it
	// finishes even though this request is over.
	deadline := time.Now().Add(30 * time.Second)
	var final apigen.Task
	for {
		rec := doRequest(t, handler, http.MethodGet, "/http-async/tasks/"+task.Id, "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &final))
		if final.Status == TaskStatusCompleted || final.Status == TaskStatusFailed {
			break
		}
		require.True(t, time.Now().Before(deadline), "the asynchronous publish did not finish in time")
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, TaskStatusCompleted, final.Status, "task error: %s", deref(final.Error))
	require.NotNil(t, final.Version)
	require.Equal(t, 1, *final.Version)

	got := doRequest(t, handler, http.MethodGet, "/http-async", "", accessKey, secret)
	require.Equal(t, http.StatusOK, got.Code)
	var dataset apigen.Dataset
	require.NoError(t, json.Unmarshal(got.Body.Bytes(), &dataset))
	require.Equal(t, 1, dataset.LatestVersion)
	require.Equal(t, DatasetStateActive, dataset.State)
	require.NotNil(t, dataset.Definition)
	require.Equal(t, int64(2), dataset.Stats.Objects)
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
