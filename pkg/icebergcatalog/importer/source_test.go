package importer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSourceListsPaginatedCatalog exercises the source client against a small REST catalog: the
// prefix comes from /v1/config, the warehouse is forwarded, both listings follow next-page-token,
// the namespace path parameter is the unit-separated form, and LoadTable returns the document
// unchanged.
func TestSourceListsPaginatedCatalog(t *testing.T) {
	var (
		mu                 sync.Mutex
		namespacePageCalls int
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			require.Equal(t, "wh-1", r.URL.Query().Get("warehouse"))
			writeTestJSON(t, w, map[string]any{
				"defaults":  map[string]string{"prefix": "ignored"},
				"overrides": map[string]string{"prefix": "lakefs"},
			})
		case "/v1/lakefs/namespaces":
			mu.Lock()
			namespacePageCalls++
			mu.Unlock()
			require.Empty(t, r.URL.Query().Get("parent"))
			if r.URL.Query().Get("pageToken") == "" {
				writeTestJSON(t, w, map[string]any{
					"namespaces":      [][]string{{"sales"}, {"marketing"}},
					"next-page-token": "page-2",
				})
				return
			}
			writeTestJSON(t, w, map[string]any{
				"namespaces": [][]string{{"sales", "eu"}},
			})
		case "/v1/lakefs/namespaces/sales/tables":
			writeTestJSON(t, w, map[string]any{
				"identifiers": []map[string]any{
					{"namespace": []string{"sales"}, "name": "orders"},
				},
			})
		case "/v1/lakefs/namespaces/sales\x1feu/tables":
			writeTestJSON(t, w, map[string]any{
				"identifiers": []map[string]any{
					{"namespace": []string{"sales", "eu"}, "name": "orders"},
				},
			})
		case "/v1/lakefs/namespaces/sales/tables/orders":
			require.Equal(t, "all", r.URL.Query().Get("snapshots"))
			writeTestJSON(t, w, map[string]any{
				"metadata-location": "s3://bucket/sales/orders/metadata/00001.metadata.json",
				"metadata":          map[string]any{"format-version": 2, "location": "s3://bucket/sales/orders"},
			})
		case "/v1/lakefs/namespaces/sales/tables/missing":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"message":"no such table","type":"NoSuchTableException","code":404}}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	source, err := NewSource(SourceConfig{URI: server.URL + "/v1", Warehouse: "wh-1"})
	require.NoError(t, err)

	namespaces, err := source.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, []Namespace{{"sales"}, {"marketing"}, {"sales", "eu"}}, namespaces)
	require.Equal(t, 2, namespacePageCalls)

	tables, err := source.ListTables(context.Background(), Namespace{"sales"})
	require.NoError(t, err)
	require.Equal(t, []TableIdentifier{{Namespace: []string{"sales"}, Name: "orders"}}, tables)

	tables, err = source.ListTables(context.Background(), Namespace{"sales", "eu"})
	require.NoError(t, err)
	require.Len(t, tables, 1)

	loaded, err := source.LoadTable(context.Background(), TableIdentifier{Namespace: []string{"sales"}, Name: "orders"})
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/sales/orders/metadata/00001.metadata.json", loaded.MetadataLocation)
	require.JSONEq(t, `{"format-version":2,"location":"s3://bucket/sales/orders"}`, string(loaded.Metadata))

	_, err = source.LoadTable(context.Background(), TableIdentifier{Namespace: []string{"sales"}, Name: "missing"})
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorIs(t, err, ErrSource)
	var catalogErr *CatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, "NoSuchTableException", catalogErr.ErrorType)
	require.Equal(t, http.StatusNotFound, catalogErr.StatusCode)
}

// TestSourceOAuthClientCredentials checks that --source-credential exchanges the client credential
// at /v1/oauth/tokens and sends the resulting bearer token, and that --source-token is used as-is.
func TestSourceOAuthClientCredentials(t *testing.T) {
	const accessToken = "token-from-oauth"
	var tokenCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/oauth/tokens":
			tokenCalls++
			require.NoError(t, r.ParseForm())
			require.Equal(t, "client_credentials", r.PostForm.Get("grant_type"))
			require.Equal(t, "id", r.PostForm.Get("client_id"))
			require.Equal(t, "secret", r.PostForm.Get("client_secret"))
			require.Equal(t, "catalog", r.PostForm.Get("scope"))
			writeTestJSON(t, w, map[string]any{"access_token": accessToken, "token_type": "bearer"})
		case "/v1/config":
			require.Equal(t, "Bearer "+accessToken, r.Header.Get("Authorization"))
			writeTestJSON(t, w, map[string]any{"overrides": map[string]string{"prefix": "p"}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	source, err := NewSource(SourceConfig{
		URI:        server.URL,
		Credential: "id:secret",
		Props:      map[string]string{"scope": "catalog"},
	})
	require.NoError(t, err)
	prefix, err := source.Prefix(context.Background())
	require.NoError(t, err)
	require.Equal(t, "p", prefix)
	prefix, err = source.Prefix(context.Background())
	require.NoError(t, err)
	require.Equal(t, "p", prefix)
	require.Equal(t, 1, tokenCalls, "the token is fetched once")

	bearer, err := NewSource(SourceConfig{URI: server.URL, Token: "static-token", Props: map[string]string{"prefix": "static"}})
	require.NoError(t, err)
	prefix, err = bearer.Prefix(context.Background())
	require.NoError(t, err)
	require.Equal(t, "static", prefix, "--source-prop prefix skips the config call")
}

// TestSourceOptionValidation pins the refusals that must happen before any request.
func TestSourceOptionValidation(t *testing.T) {
	for name, cfg := range map[string]SourceConfig{
		"no uri":           {},
		"not http":         {URI: "s3://bucket/catalog"},
		"no host":          {URI: "http:///catalog"},
		"token and cred":   {URI: "http://example.com", Token: "t", Credential: "a:b"},
		"malformed cred":   {URI: "http://example.com", Credential: "no-colon"},
		"query in the uri": {URI: "http://example.com/catalog?x=1"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewSource(cfg)
			require.ErrorIs(t, err, ErrInvalidOptions)
		})
	}

	for raw, want := range map[string]string{
		"http://example.com":            "http://example.com",
		"http://example.com/":           "http://example.com",
		"http://example.com/v1":         "http://example.com",
		"http://example.com/iceberg/":   "http://example.com/iceberg",
		"http://example.com/iceberg/v1": "http://example.com/iceberg",
	} {
		got, err := normalizeCatalogURL(raw)
		require.NoError(t, err, raw)
		require.Equal(t, want, got, raw)
	}
}

// TestSourceRejectsRepeatedPageToken makes sure a broken catalog cannot loop the importer.
func TestSourceRejectsRepeatedPageToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			writeTestJSON(t, w, map[string]any{"overrides": map[string]string{"prefix": "lakefs"}})
		default:
			writeTestJSON(t, w, map[string]any{"namespaces": [][]string{{"a"}}, "next-page-token": "same"})
		}
	}))
	t.Cleanup(server.Close)

	source, err := NewSource(SourceConfig{URI: server.URL})
	require.NoError(t, err)
	_, err = source.ListNamespaces(context.Background(), nil)
	require.ErrorIs(t, err, ErrSource)
	require.Contains(t, err.Error(), "repeats page token")
}

// writeTestJSON writes body as a JSON response.
func writeTestJSON(t *testing.T, w http.ResponseWriter, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(body))
}

// TestCatalogErrorFormatting pins the message a user sees for a catalog refusal.
func TestCatalogErrorFormatting(t *testing.T) {
	err := newCatalogError(catalogTarget, http.MethodPost, "http://lakefs/iceberg/api/v1/x", http.StatusConflict,
		[]byte(`{"error":{"message":"already exists","type":"AlreadyExistsException","code":409}}`))
	require.ErrorIs(t, err, ErrTarget)
	require.True(t, strings.Contains(err.Error(), "AlreadyExistsException"))
	require.True(t, strings.Contains(err.Error(), "already exists"))
	require.True(t, isConflict(err))
	require.False(t, isNotFound(err))

	notFound := newCatalogError(catalogSource, http.MethodGet, "http://source/v1/x", http.StatusNotFound,
		[]byte("<html>not found</html>"))
	require.True(t, errors.Is(notFound, ErrNotFound))
	require.Contains(t, notFound.Error(), "not found")
}
