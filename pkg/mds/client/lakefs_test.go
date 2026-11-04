package client

// lakefs_test.go drives every method of the wrapper against an httptest fake of the lakeFS
// API. The fake is deliberately literal: it answers the exact paths and JSON documents the
// generated client sends and expects, so a change to the request shape (a dropped
// first_parent, a three-dot diff, a page without a cursor) fails here rather than in the
// indexer. Pagination and 404 are the two behaviours every caller depends on (§4.2, §4.4,
// §4.5), so they are tested for each listing and for every method.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

const (
	testPageSize   = 3
	testRepository = "repo"
	testBranch     = "main"
)

// recordedCall is one request the fake received.
type recordedCall struct {
	method string
	path   string
	query  url.Values
	body   []byte
}

// recorder collects requests. The HTTP handler runs in another goroutine, so access is
// guarded and the test reads a copy.
type recorder struct {
	mu    sync.Mutex
	calls []recordedCall
}

func (r *recorder) record(call recordedCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recorder) snapshot() []recordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedCall(nil), r.calls...)
}

func (r *recorder) lastCall(t *testing.T) recordedCall {
	t.Helper()
	calls := r.snapshot()
	require.NotEmpty(t, calls, "no request was recorded")
	return calls[len(calls)-1]
}

// newTestClient starts a fake lakeFS server and returns a client whose page size is
// testPageSize. The server answers the API under apiutil.BaseURL, like a real endpoint.
func newTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *recorder) {
	t.Helper()
	return newTestClientPageSize(t, handler, testPageSize)
}

func newTestClientPageSize(t *testing.T, handler http.HandlerFunc, pageSize int) (*Client, *recorder) {
	t.Helper()
	rec := &recorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			http.Error(w, "cannot read body", http.StatusInternalServerError)
			return
		}
		rec.record(recordedCall{method: r.Method, path: r.URL.Path, query: r.URL.Query(), body: body})
		handler(w, r)
	}))
	t.Cleanup(server.Close)

	api, err := apigen.NewClientWithResponses(server.URL + apiutil.BaseURL)
	require.NoError(t, err)
	return New(api, pageSize), rec
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeNotFound(w http.ResponseWriter) {
	writeJSON(w, http.StatusNotFound, map[string]string{"message": "not found"})
}

func listPage(results []map[string]any, hasMore bool, nextOffset string) map[string]any {
	return map[string]any{
		"pagination": map[string]any{
			"has_more":     hasMore,
			"next_offset":  nextOffset,
			"results":      len(results),
			"max_per_page": testPageSize,
		},
		"results": results,
	}
}

func decodeBody(t *testing.T, call recordedCall) map[string]any {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(call.body, &body))
	return body
}

func requireQuery(t *testing.T, call recordedCall, key, want string) {
	t.Helper()
	require.Equal(t, want, call.query.Get(key), "query parameter %q", key)
}

func apiPath(suffix string) string {
	return apiutil.BaseURL + suffix
}

func TestGetRepository(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"id":                testRepository,
			"storage_namespace": "s3://bucket/repo/",
			"default_branch":    testBranch,
			"storage_id":        "s3-store",
			"read_only":         true,
		})
	})

	repo, err := c.GetRepository(context.Background(), testRepository)
	require.NoError(t, err)
	require.Equal(t, Repository{
		Name:             testRepository,
		StorageNamespace: "s3://bucket/repo",
		DefaultBranch:    testBranch,
		StorageID:        "s3-store",
		ReadOnly:         true,
	}, repo)

	call := rec.lastCall(t)
	require.Equal(t, http.MethodGet, call.method)
	require.Equal(t, apiPath("/repositories/repo"), call.path)
}

func TestCreateRepository(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusCreated, map[string]any{
			"id":                "repo-metadata",
			"storage_namespace": "s3://bucket/repo-metadata/",
			"default_branch":    testBranch,
			"storage_id":        "s3-store",
		})
	})

	repo, err := c.CreateRepository(context.Background(), RepositoryOptions{
		Name:             "repo-metadata",
		StorageNamespace: "s3://bucket/repo-metadata",
		DefaultBranch:    testBranch,
		StorageID:        "s3-store",
	})
	require.NoError(t, err)
	require.Equal(t, "repo-metadata", repo.Name)
	require.Equal(t, "s3://bucket/repo-metadata", repo.StorageNamespace)

	call := rec.lastCall(t)
	require.Equal(t, http.MethodPost, call.method)
	require.Equal(t, apiPath("/repositories"), call.path)
	body := decodeBody(t, call)
	require.Equal(t, "repo-metadata", body["name"])
	require.Equal(t, "s3://bucket/repo-metadata", body["storage_namespace"])
	require.Equal(t, testBranch, body["default_branch"])
	require.Equal(t, "s3-store", body["storage_id"])
	require.NotContains(t, body, "read_only")
}

func TestListBranchesPagination(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("after") {
		case "":
			writeJSON(w, http.StatusOK, listPage([]map[string]any{
				{"id": "main", "commit_id": "c1"},
				{"id": "dev", "commit_id": "c2"},
			}, true, "dev"))
		case "dev":
			writeJSON(w, http.StatusOK, listPage([]map[string]any{
				{"id": "feature", "commit_id": "c3"},
			}, false, ""))
		default:
			writeNotFound(w)
		}
	})

	first, hasMore, err := c.ListBranches(context.Background(), testRepository, "")
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Equal(t, []Branch{
		{Name: "main", CommitID: "c1"},
		{Name: "dev", CommitID: "c2"},
	}, first)

	// The caller derives the next cursor from the last item, as §4.2's walk does.
	second, hasMore, err := c.ListBranches(context.Background(), testRepository, first[len(first)-1].Name)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, []Branch{{Name: "feature", CommitID: "c3"}}, second)

	calls := rec.snapshot()
	require.Len(t, calls, 2)
	require.Equal(t, apiPath("/repositories/repo/branches"), calls[0].path)
	requireQuery(t, calls[0], "amount", strconv.Itoa(testPageSize))
	requireQuery(t, calls[0], "after", "")
	requireQuery(t, calls[1], "amount", strconv.Itoa(testPageSize))
	requireQuery(t, calls[1], "after", "dev")
}

func TestDefaultPageSize(t *testing.T) {
	c, rec := newTestClientPageSize(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, listPage(nil, false, ""))
	}, 0)

	_, _, err := c.ListBranches(context.Background(), testRepository, "")
	require.NoError(t, err)
	requireQuery(t, rec.lastCall(t), "amount", strconv.Itoa(DefaultPageSize))
}

func TestListBranchesBadPagination(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		// More results, but no cursor: following that answer would loop forever.
		writeJSON(w, http.StatusOK, listPage([]map[string]any{{"id": "main", "commit_id": "c1"}}, true, ""))
	})

	_, _, err := c.ListBranches(context.Background(), testRepository, "")
	require.ErrorIs(t, err, ErrBadPagination)
}

func TestLogCommits(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, listPage([]map[string]any{
			{
				"id":            "c2",
				"message":       "second",
				"committer":     "alice",
				"creation_date": 1700000000,
				"parents":       []string{"c1"},
				"metadata":      map[string]string{"lakefs.mds.source-commit": "c2"},
			},
			{
				"id":            "c1",
				"message":       "first",
				"committer":     "bob",
				"creation_date": 1600000000,
				"parents":       []string{},
			},
		}, true, "c1"))
	})

	commits, hasMore, err := c.LogCommits(context.Background(), testRepository, testBranch, LogCommitsOptions{
		FirstParent: true,
		StopAt:      "c0",
		After:       "cursor",
	})
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Equal(t, []Commit{
		{
			ID:           "c2",
			Message:      "second",
			Parents:      []string{"c1"},
			Committer:    "alice",
			CreationDate: time.Unix(1700000000, 0).UTC(),
			Metadata:     map[string]string{"lakefs.mds.source-commit": "c2"},
		},
		{
			ID:           "c1",
			Message:      "first",
			Committer:    "bob",
			CreationDate: time.Unix(1600000000, 0).UTC(),
		},
	}, commits)

	call := rec.lastCall(t)
	require.Equal(t, apiPath("/repositories/repo/refs/main/commits"), call.path)
	requireQuery(t, call, "amount", strconv.Itoa(testPageSize))
	requireQuery(t, call, "first_parent", "true")
	requireQuery(t, call, "stop_at", "c0")
	requireQuery(t, call, "after", "cursor")
}

func TestDiffRefs(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, listPage([]map[string]any{
			{
				"path":       "data/added.parquet",
				"type":       "added",
				"path_type":  "object",
				"size_bytes": 42,
				"right": map[string]any{
					"checksum":     "etag-added",
					"content_type": "application/octet-stream",
					"mtime":        1700000000,
					"metadata":     map[string]string{"x-amz-meta-owner": "alice"},
				},
			},
			{
				"path":       "data/changed.parquet",
				"type":       "changed",
				"path_type":  "object",
				"size_bytes": 7,
				"right":      map[string]any{"checksum": "etag-changed", "mtime": 0},
			},
			{
				"path":       "data/removed.parquet",
				"type":       "removed",
				"path_type":  "object",
				"size_bytes": 3,
			},
		}, false, ""))
	})

	changes, hasMore, err := c.DiffRefs(context.Background(), testRepository, "main", "feature", "")
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, []Change{
		{
			Path:      "data/added.parquet",
			Type:      ChangeAdded,
			SizeBytes: 42,
			Right: &ObjectStat{
				Path:         "data/added.parquet",
				SizeBytes:    42,
				Mtime:        time.Unix(1700000000, 0).UTC(),
				ETag:         "etag-added",
				ContentType:  "application/octet-stream",
				UserMetadata: map[string]string{"x-amz-meta-owner": "alice"},
			},
		},
		{
			Path:      "data/changed.parquet",
			Type:      ChangeChanged,
			SizeBytes: 7,
			Right: &ObjectStat{
				Path:      "data/changed.parquet",
				SizeBytes: 7,
				ETag:      "etag-changed",
			},
		},
		{
			Path:      "data/removed.parquet",
			Type:      ChangeRemoved,
			SizeBytes: 3,
		},
	}, changes)

	call := rec.lastCall(t)
	require.Equal(t, apiPath("/repositories/repo/refs/main/diff/feature"), call.path)
	requireQuery(t, call, "amount", strconv.Itoa(testPageSize))
	requireQuery(t, call, "type", "two_dot")
	requireQuery(t, call, "include_right_stats", "true")
	requireQuery(t, call, "delimiter", "")
	requireQuery(t, call, "after", "")
}

func TestListObjects(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, listPage([]map[string]any{
			{
				"path":             "data/a.parquet",
				"size_bytes":       10,
				"mtime":            1700000000,
				"checksum":         "etag-a",
				"content_type":     "application/octet-stream",
				"physical_address": "s3://bucket/repo/data/a.parquet",
				"path_type":        "object",
				"metadata":         map[string]string{"x-amz-meta-owner": "alice"},
			},
			{
				"path":             "data/b.parquet",
				"size_bytes":       20,
				"mtime":            0,
				"checksum":         "etag-b",
				"physical_address": "s3://bucket/repo/data/b.parquet",
				"path_type":        "object",
			},
		}, true, "data/b.parquet"))
	})

	objects, hasMore, err := c.ListObjects(context.Background(), testRepository, testBranch, "data/a.parquet")
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Equal(t, []ObjectStat{
		{
			Path:         "data/a.parquet",
			SizeBytes:    10,
			Mtime:        time.Unix(1700000000, 0).UTC(),
			ETag:         "etag-a",
			ContentType:  "application/octet-stream",
			UserMetadata: map[string]string{"x-amz-meta-owner": "alice"},
		},
		{
			Path:      "data/b.parquet",
			SizeBytes: 20,
			ETag:      "etag-b",
		},
	}, objects)

	call := rec.lastCall(t)
	require.Equal(t, apiPath("/repositories/repo/refs/main/objects/ls"), call.path)
	requireQuery(t, call, "amount", strconv.Itoa(testPageSize))
	requireQuery(t, call, "user_metadata", "true")
	requireQuery(t, call, "after", "data/a.parquet")
}

func TestStatObject(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"path":         "data/a.parquet",
			"size_bytes":   10,
			"mtime":        1700000000,
			"checksum":     "etag-a",
			"content_type": "application/octet-stream",
			"metadata":     map[string]string{"owner": "alice"},
			"path_type":    "object",
		})
	})

	object, err := c.StatObject(context.Background(), testRepository, testBranch, "data/a.parquet")
	require.NoError(t, err)
	require.Equal(t, ObjectStat{
		Path:         "data/a.parquet",
		SizeBytes:    10,
		Mtime:        time.Unix(1700000000, 0).UTC(),
		ETag:         "etag-a",
		ContentType:  "application/octet-stream",
		UserMetadata: map[string]string{"owner": "alice"},
	}, object)

	call := rec.lastCall(t)
	require.Equal(t, apiPath("/repositories/repo/refs/main/objects/stat"), call.path)
	requireQuery(t, call, "path", "data/a.parquet")
	requireQuery(t, call, "user_metadata", "true")
}

func TestGetCommit(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"id":            "abc123",
			"message":       "index repo@abc123",
			"committer":     "mds-indexer",
			"creation_date": 1700000000,
			"parents":       []string{"parent"},
			"metadata":      map[string]string{"lakefs.mds.source-repository": "repo"},
		})
	})

	commit, err := c.GetCommit(context.Background(), testRepository, "abc123")
	require.NoError(t, err)
	require.Equal(t, "abc123", commit.ID)
	require.Equal(t, "mds-indexer", commit.Committer)
	require.Equal(t, time.Unix(1700000000, 0).UTC(), commit.CreationDate)
	require.Equal(t, []string{"parent"}, commit.Parents)
	require.Equal(t, map[string]string{"lakefs.mds.source-repository": "repo"}, commit.Metadata)

	call := rec.lastCall(t)
	require.Equal(t, http.MethodGet, call.method)
	require.Equal(t, apiPath("/repositories/repo/commits/abc123"), call.path)
}

func TestCommit(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusCreated, map[string]any{
			"id":            "new-commit",
			"message":       "mds: index repo@abc",
			"committer":     "mds-indexer",
			"creation_date": 1700000001,
			"parents":       []string{"abc"},
			"metadata":      map[string]string{"lakefs.mds.source-commit": "abc"},
		})
	})

	commit, err := c.Commit(context.Background(), "repo-metadata", testBranch, CommitOptions{
		Message:    "mds: index repo@abc",
		Metadata:   map[string]string{"lakefs.mds.source-commit": "abc"},
		AllowEmpty: false,
	})
	require.NoError(t, err)
	require.Equal(t, "new-commit", commit.ID)
	require.Equal(t, map[string]string{"lakefs.mds.source-commit": "abc"}, commit.Metadata)

	call := rec.lastCall(t)
	require.Equal(t, http.MethodPost, call.method)
	require.Equal(t, apiPath("/repositories/repo-metadata/branches/main/commits"), call.path)
	body := decodeBody(t, call)
	require.Equal(t, "mds: index repo@abc", body["message"])
	require.Equal(t, false, body["allow_empty"])
	metadata, ok := body["metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "abc", metadata["lakefs.mds.source-commit"])
}

func TestTags(t *testing.T) {
	t.Run("list paged", func(t *testing.T) {
		c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, listPage([]map[string]any{
				{"id": "mds-a", "commit_id": "mc-a"},
				{"id": "mds-b", "commit_id": "mc-b"},
			}, true, "mds-b"))
		})

		tags, hasMore, err := c.ListTags(context.Background(), "repo-metadata", "mds-a")
		require.NoError(t, err)
		require.True(t, hasMore)
		require.Equal(t, []Tag{
			{Name: "mds-a", CommitID: "mc-a"},
			{Name: "mds-b", CommitID: "mc-b"},
		}, tags)

		call := rec.lastCall(t)
		require.Equal(t, http.MethodGet, call.method)
		require.Equal(t, apiPath("/repositories/repo-metadata/tags"), call.path)
		requireQuery(t, call, "amount", strconv.Itoa(testPageSize))
		requireQuery(t, call, "after", "mds-a")
	})

	t.Run("create with force", func(t *testing.T) {
		c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusCreated, map[string]any{"id": "mds-abc", "commit_id": "metadata-commit"})
		})

		tag, err := c.CreateTag(context.Background(), "repo-metadata", "mds-abc", "metadata-commit", true)
		require.NoError(t, err)
		require.Equal(t, Tag{Name: "mds-abc", CommitID: "metadata-commit"}, tag)

		call := rec.lastCall(t)
		require.Equal(t, http.MethodPost, call.method)
		require.Equal(t, apiPath("/repositories/repo-metadata/tags"), call.path)
		body := decodeBody(t, call)
		require.Equal(t, "mds-abc", body["id"])
		require.Equal(t, "metadata-commit", body["ref"])
		require.Equal(t, true, body["force"])
	})

	t.Run("get", func(t *testing.T) {
		c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{"id": "mds-abc", "commit_id": "metadata-commit"})
		})

		tag, err := c.GetTag(context.Background(), "repo-metadata", "mds-abc")
		require.NoError(t, err)
		require.Equal(t, Tag{Name: "mds-abc", CommitID: "metadata-commit"}, tag)

		call := rec.lastCall(t)
		require.Equal(t, http.MethodGet, call.method)
		require.Equal(t, apiPath("/repositories/repo-metadata/tags/mds-abc"), call.path)
	})

	t.Run("delete", func(t *testing.T) {
		c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})

		require.NoError(t, c.DeleteTag(context.Background(), "repo-metadata", "mds-abc"))
		call := rec.lastCall(t)
		require.Equal(t, http.MethodDelete, call.method)
		require.Equal(t, apiPath("/repositories/repo-metadata/tags/mds-abc"), call.path)
	})
}

func TestBranchWrites(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})

		require.NoError(t, c.CreateBranch(context.Background(), "repo-metadata", "feature", "mds-abc"))
		call := rec.lastCall(t)
		require.Equal(t, http.MethodPost, call.method)
		require.Equal(t, apiPath("/repositories/repo-metadata/branches"), call.path)
		body := decodeBody(t, call)
		require.Equal(t, "feature", body["name"])
		require.Equal(t, "mds-abc", body["source"])
	})

	t.Run("delete", func(t *testing.T) {
		c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})

		require.NoError(t, c.DeleteBranch(context.Background(), "repo-metadata", "feature"))
		call := rec.lastCall(t)
		require.Equal(t, http.MethodDelete, call.method)
		require.Equal(t, apiPath("/repositories/repo-metadata/branches/feature"), call.path)
		requireQuery(t, call, "force", "")
	})

	t.Run("reset", func(t *testing.T) {
		c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})

		require.NoError(t, c.ResetBranch(context.Background(), "repo-metadata", "feature"))
		call := rec.lastCall(t)
		require.Equal(t, http.MethodPut, call.method)
		require.Equal(t, apiPath("/repositories/repo-metadata/branches/feature"), call.path)
		body := decodeBody(t, call)
		require.Equal(t, "reset", body["type"])
	})

	t.Run("hard reset", func(t *testing.T) {
		c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})

		require.NoError(t, c.HardResetBranch(context.Background(), "repo-metadata", "feature", "mds-abc"))
		call := rec.lastCall(t)
		require.Equal(t, http.MethodPut, call.method)
		require.Equal(t, apiPath("/repositories/repo-metadata/branches/feature/hard_reset"), call.path)
		requireQuery(t, call, "ref", "mds-abc")
		requireQuery(t, call, "force", "")
	})
}

func TestFindMergeBase(t *testing.T) {
	c, rec := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"base_commit_id":        "base",
			"source_commit_id":      "last",
			"destination_commit_id": "head",
		})
	})

	base, err := c.FindMergeBase(context.Background(), testRepository, "last", "head")
	require.NoError(t, err)
	require.Equal(t, "base", base)

	call := rec.lastCall(t)
	require.Equal(t, http.MethodGet, call.method)
	require.Equal(t, apiPath("/repositories/repo/refs/last/merge/head"), call.path)
}

// TestNotFound maps a 404 of every method to *NotFoundError. §4.4 and §4.5 branch on
// errors.Is(err, ErrNotFound) for a missing branch, tag or commit, so every call has to
// answer that way rather than with a generic failure.
func TestNotFound(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{"get repository", func(c *Client) error {
			_, err := c.GetRepository(ctx, testRepository)
			return err
		}},
		{"create repository", func(c *Client) error {
			_, err := c.CreateRepository(ctx, RepositoryOptions{Name: testRepository, StorageNamespace: "s3://bucket/repo"})
			return err
		}},
		{"list branches", func(c *Client) error {
			_, _, err := c.ListBranches(ctx, testRepository, "")
			return err
		}},
		{"create branch", func(c *Client) error {
			return c.CreateBranch(ctx, testRepository, "feature", "main")
		}},
		{"delete branch", func(c *Client) error {
			return c.DeleteBranch(ctx, testRepository, "feature")
		}},
		{"reset branch", func(c *Client) error {
			return c.ResetBranch(ctx, testRepository, "feature")
		}},
		{"hard reset branch", func(c *Client) error {
			return c.HardResetBranch(ctx, testRepository, "feature", "main")
		}},
		{"find merge base", func(c *Client) error {
			_, err := c.FindMergeBase(ctx, testRepository, "last", "head")
			return err
		}},
		{"log commits", func(c *Client) error {
			_, _, err := c.LogCommits(ctx, testRepository, testBranch, LogCommitsOptions{FirstParent: true})
			return err
		}},
		{"get commit", func(c *Client) error {
			_, err := c.GetCommit(ctx, testRepository, "abc")
			return err
		}},
		{"commit", func(c *Client) error {
			_, err := c.Commit(ctx, testRepository, testBranch, CommitOptions{Message: "m"})
			return err
		}},
		{"create tag", func(c *Client) error {
			_, err := c.CreateTag(ctx, testRepository, "mds-abc", "abc", true)
			return err
		}},
		{"get tag", func(c *Client) error {
			_, err := c.GetTag(ctx, testRepository, "mds-abc")
			return err
		}},
		{"list tags", func(c *Client) error {
			_, _, err := c.ListTags(ctx, testRepository, "")
			return err
		}},
		{"delete tag", func(c *Client) error {
			return c.DeleteTag(ctx, testRepository, "mds-abc")
		}},
		{"diff refs", func(c *Client) error {
			_, _, err := c.DiffRefs(ctx, testRepository, "main", "feature", "")
			return err
		}},
		{"list objects", func(c *Client) error {
			_, _, err := c.ListObjects(ctx, testRepository, testBranch, "")
			return err
		}},
		{"stat object", func(c *Client) error {
			_, err := c.StatObject(ctx, testRepository, testBranch, "data/a.parquet")
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				writeNotFound(w)
			})

			err := tt.call(c)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrNotFound)

			var notFound *NotFoundError
			require.ErrorAs(t, err, &notFound)
			require.NotEmpty(t, notFound.Operation)
			require.NotEmpty(t, notFound.Subject)
			require.Contains(t, err.Error(), "lakeFS resource not found")
		})
	}
}

func TestErrorStatuses(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		status int
		want   error
	}{
		{"unauthorized", http.StatusUnauthorized, ErrForbidden},
		{"forbidden", http.StatusForbidden, ErrForbidden},
		{"conflict", http.StatusConflict, ErrConflict},
		{"bad request", http.StatusBadRequest, ErrBadRequest},
		{"server error", http.StatusInternalServerError, ErrUnexpectedResponse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, tt.status, map[string]string{"message": "server says no"})
			})

			_, err := c.GetRepository(ctx, testRepository)
			require.ErrorIs(t, err, tt.want)
			require.Contains(t, err.Error(), "server says no")
		})
	}
}

// TestTransportError shows that a failure below HTTP is not turned into a not-found: §4.4's
// recovery must not mistake an unreachable lakeFS for an absent tag.
func TestTransportError(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()

	api, err := apigen.NewClientWithResponses(server.URL + apiutil.BaseURL)
	require.NoError(t, err)
	c := New(api, testPageSize)

	_, err = c.GetCommit(context.Background(), testRepository, "abc")
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrNotFound))
}
