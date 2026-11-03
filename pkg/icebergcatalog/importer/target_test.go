package importer

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTargetCatalogOperations drives the target client against a fake lakeFS catalog: namespace
// creation is incremental from the first iceberg level, register sends the spec's
// RegisterTableRequest, existence is a LoadTable 404, and drop is a DELETE.
func TestTargetCatalogOperations(t *testing.T) {
	var (
		mu             sync.Mutex
		namespaces     [][]string
		registered     map[string]any
		dropped        bool
		tableExistsVal = true
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/v1/config" && r.Method == http.MethodGet:
			writeTestJSON(t, w, map[string]any{"overrides": map[string]string{"prefix": "lakefs"}})
		case r.URL.Path == "/v1/lakefs/namespaces" && r.Method == http.MethodPost:
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var request struct {
				Namespace []string `json:"namespace"`
			}
			require.NoError(t, json.Unmarshal(body, &request))
			for _, existing := range namespaces {
				if len(existing) == len(request.Namespace) && stringsEqual(existing, request.Namespace) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"error":{"message":"exists","type":"NamespaceAlreadyExistsException","code":409}}`))
					return
				}
			}
			namespaces = append(namespaces, request.Namespace)
			writeTestJSON(t, w, map[string]any{"namespace": request.Namespace, "properties": map[string]string{}})
		case r.URL.Path == "/v1/lakefs/namespaces/repo\x1fmain\x1fsales/tables/orders" && r.Method == http.MethodGet:
			if !tableExistsVal {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":{"message":"no such table","type":"NoSuchTableException","code":404}}`))
				return
			}
			writeTestJSON(t, w, map[string]any{"metadata-location": "mem://ns/_iceberg/u/metadata/1.metadata.json"})
		case r.URL.Path == "/v1/lakefs/namespaces/repo\x1fmain\x1fsales/tables/orders" && r.Method == http.MethodDelete:
			dropped = true
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/v1/lakefs/namespaces/repo\x1fmain\x1fsales/register" && r.Method == http.MethodPost:
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			registered = map[string]any{}
			require.NoError(t, json.Unmarshal(body, &registered))
			writeTestJSON(t, w, map[string]any{"metadata-location": registered["metadata-location"]})
		default:
			t.Errorf("unexpected target request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	target, err := NewTarget(TargetConfig{BaseURL: server.URL})
	require.NoError(t, err)

	ctx := context.Background()
	levels := []string{"repo", "main", "sales", "eu"}
	require.NoError(t, target.EnsureNamespace(ctx, levels))
	require.Equal(t, [][]string{{"repo", "main", "sales"}, {"repo", "main", "sales", "eu"}}, namespaces)

	// A second run must not fail on the 409s it gets for namespaces that already exist.
	require.NoError(t, target.EnsureNamespace(ctx, levels))
	require.Len(t, namespaces, 2)

	exists, err := target.TableExists(ctx, []string{"repo", "main", "sales"}, "orders")
	require.NoError(t, err)
	require.True(t, exists)
	tableExistsVal = false
	exists, err = target.TableExists(ctx, []string{"repo", "main", "sales"}, "orders")
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, target.RegisterTable(ctx, []string{"repo", "main", "sales"}, "orders",
		"mem://ns/_iceberg/u/metadata/1.metadata.json"))
	require.Equal(t, "orders", registered["name"])
	require.Equal(t, "mem://ns/_iceberg/u/metadata/1.metadata.json", registered["metadata-location"])

	require.NoError(t, target.DropTable(ctx, []string{"repo", "main", "sales"}, "orders"))
	require.True(t, dropped)
}

// TestTargetDisabledCatalog surfaces the catalog's 501 as a target error with the envelope's
// message, which is what a user of a lakeFS server with iceberg.enabled=false needs to read.
func TestTargetDisabledCatalog(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte(`{"error":{"message":"the Iceberg catalog is disabled","type":"NotImplementedException","code":501}}`))
	}))
	t.Cleanup(server.Close)
	target, err := NewTarget(TargetConfig{BaseURL: server.URL})
	require.NoError(t, err)
	_, err = target.Prefix(context.Background())
	require.ErrorIs(t, err, ErrTarget)
	require.Contains(t, err.Error(), "disabled")
}

// TestTargetRequiresNamespaceLevels refuses a namespace that has no iceberg level: the first two
// levels are the repository and the branch, not a namespace.
func TestTargetRequiresNamespaceLevels(t *testing.T) {
	target, err := NewTarget(TargetConfig{BaseURL: "http://lakefs.example/iceberg/api"})
	require.NoError(t, err)
	require.ErrorIs(t, target.EnsureNamespace(context.Background(), []string{"repo", "main"}), ErrInvalidOptions)
}

func TestLakeFSCatalogURL(t *testing.T) {
	require.Equal(t, "http://lakefs:8000/iceberg/api", LakeFSCatalogURL("http://lakefs:8000/"))
	require.Equal(t, "https://lakefs.example/base/iceberg/api", LakeFSCatalogURL("https://lakefs.example/base"))
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
