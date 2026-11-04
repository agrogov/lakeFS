package indexer

// internal_test.go tests the small unexported rules of this package directly: the URL the lakeFS
// client is built for, the storage-namespace prefix test that §3.3 shares with the uncommitted GC,
// and the branch selectors of §4.1. The behaviour they feed is covered from the outside in
// *_test.go; these cases pin the rules themselves, including the inputs that are hard to reach
// through a tick.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// TestAPIBaseURL pins the API base URL derivation: the endpoint may be the server root or already
// carry the API path, and both must produce one /api/v1.
func TestAPIBaseURL(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
		wantErr  bool
	}{
		{endpoint: "http://lakefs:8000", want: "http://lakefs:8000/api/v1"},
		{endpoint: "http://lakefs:8000/", want: "http://lakefs:8000/api/v1"},
		{endpoint: "http://lakefs:8000/api/v1", want: "http://lakefs:8000/api/v1"},
		{endpoint: "http://lakefs:8000/api/v1/", want: "http://lakefs:8000/api/v1"},
		{endpoint: "https://lakefs.example.com/lakefs", want: "https://lakefs.example.com/lakefs/api/v1"},
		{endpoint: "http://lakefs:8000?x=1#frag", want: "http://lakefs:8000/api/v1"},
		{endpoint: "lakefs:8000", wantErr: true},
		{endpoint: "", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.endpoint, func(t *testing.T) {
			got, err := apiBaseURL(test.endpoint)
			if test.wantErr {
				require.ErrorIs(t, err, ErrNoEndpoint)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

// TestNamespaceHasPath pins the bucket-root rule of §3.3.
func TestNamespaceHasPath(t *testing.T) {
	tests := map[string]bool{
		"s3://bucket":             false,
		"s3://bucket/":            false,
		"s3://bucket/data":        true,
		"s3://bucket/data/":       true,
		"local:///var/lib/lakefs": true,
		"gs://bucket/a/b":         true,
		"relative/path":           true,
		"noscheme":                false,
	}
	for namespace, want := range tests {
		require.Equal(t, want, namespaceHasPath(namespace), "namespace %q", namespace)
	}
}

// TestOverlapsStorageNamespace pins the prefix test §3.3 borrows from the uncommitted GC: a shared
// string prefix is not an overlap unless it ends on a path boundary.
func TestOverlapsStorageNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		other     string
		want      bool
	}{
		{namespace: "s3://bucket/data", other: "s3://bucket/data", want: true},
		{namespace: "s3://bucket/data", other: "s3://bucket/data/inner", want: true},
		{namespace: "s3://bucket/data/inner", other: "s3://bucket/data", want: true},
		{namespace: "s3://bucket/data/", other: "s3://bucket/data", want: true},
		{namespace: "s3://bucket/data2", other: "s3://bucket/data", want: false},
		{namespace: "s3://bucket/data-metadata", other: "s3://bucket/data", want: false},
		{namespace: "", other: "s3://bucket/data", want: false},
	}
	for _, test := range tests {
		require.Equal(t, test.want, overlapsStorageNamespace(test.namespace, test.other),
			"%q against %q", test.namespace, test.other)
	}
}

// TestMatchBranch pins the selectors of §4.1: an exact name, or a single trailing "*".
func TestMatchBranch(t *testing.T) {
	tests := []struct {
		selector string
		branch   string
		want     bool
	}{
		{selector: "main", branch: "main", want: true},
		{selector: "main", branch: "main2", want: false},
		{selector: "dev-*", branch: "dev-1", want: true},
		{selector: "dev-*", branch: "dev-", want: true},
		{selector: "dev-*", branch: "main", want: false},
		{selector: "*", branch: "anything", want: true},
		{selector: "feature/*", branch: "feature/x", want: true},
	}
	for _, test := range tests {
		require.Equal(t, test.want, matchBranch(test.selector, test.branch),
			"%q against %q", test.selector, test.branch)
	}
}

// TestLastNameOf pins the pagination cursor helper: a page with items ends on its last item, and an
// empty page has no cursor, which is what turns "more results" without fuel into an error.
func TestLastNameOf(t *testing.T) {
	require.Empty(t, lastNameOf(0, func(int) string { return "unused" }))
	require.Equal(t, "c", lastNameOf(3, func(i int) string { return []string{"a", "b", "c"}[i] }))
}

// TestTagAndMetadataNames pins the names of §3.3: the tag prefix and the two commit metadata keys.
func TestTagAndMetadataNames(t *testing.T) {
	require.Equal(t, "mds-abc123", tagName("abc123"))
	require.Equal(t, "repo-metadata", metadataRepository("repo"))
	require.Equal(t, map[string]string{
		MetadataKeySourceCommit:     "abc123",
		MetadataKeySourceRepository: "repo",
	}, sourceMetadata("repo", "abc123"))
}

// TestServerBaseURL pins the catalog endpoint derivation: the catalog lives at the server root,
// while the API lives under /api/v1. An endpoint given either way has to produce the same root,
// because IcebergStore appends /iceberg/relative_to/... to it (§4.3 step 3).
func TestServerBaseURL(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
		wantErr  bool
	}{
		{endpoint: "http://lakefs:8000", want: "http://lakefs:8000"},
		{endpoint: "http://lakefs:8000/", want: "http://lakefs:8000"},
		{endpoint: "http://lakefs:8000/api/v1", want: "http://lakefs:8000"},
		{endpoint: "http://lakefs:8000/api/v1/", want: "http://lakefs:8000"},
		{endpoint: "https://lakefs.example.com/lakefs", want: "https://lakefs.example.com/lakefs"},
		{endpoint: "https://lakefs.example.com/lakefs/api/v1", want: "https://lakefs.example.com/lakefs"},
		{endpoint: "http://lakefs:8000?x=1#frag", want: "http://lakefs:8000"},
		{endpoint: "lakefs:8000", wantErr: true},
		{endpoint: "", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.endpoint, func(t *testing.T) {
			got, err := serverBaseURL(test.endpoint)
			if test.wantErr {
				require.ErrorIs(t, err, ErrNoEndpoint)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

// TestCatalogURLFromEndpoint pins the defect the end-to-end run found: New used to pass the API
// base to IcebergStore, so the catalog URL became <endpoint>/api/v1/iceberg/... and answered 404.
// The endpoint with and without /api/v1 must produce the same catalog URL.
func TestCatalogURLFromEndpoint(t *testing.T) {
	endpoints := []string{
		"http://lakefs:8000",
		"http://lakefs:8000/",
		"http://lakefs:8000/api/v1",
		"http://lakefs:8000/api/v1/",
	}
	var urls []string
	for _, endpoint := range endpoints {
		idx, err := New(Options{Endpoint: endpoint, AccessKeyID: "key", SecretKey: "secret"})
		require.NoError(t, err)
		store, ok := idx.tables.(*IcebergStore)
		require.True(t, ok, "New builds the production IcebergStore")
		uri, err := table.CatalogConfig{
			Endpoint:        store.Endpoint,
			Repository:      "R-metadata",
			Branch:          "main",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}.URI()
		require.NoError(t, err)
		urls = append(urls, uri)
	}
	for _, uri := range urls {
		require.Equal(t, "http://lakefs:8000/iceberg/relative_to/R-metadata.main/api", uri)
	}
}

// TestAutomaticCreationCommitMessage pins the one lakeFS constant this package spells out because
// the indexer is a REST client and does not import pkg/graveler.
func TestAutomaticCreationCommitMessage(t *testing.T) {
	require.Equal(t, graveler.FirstCommitMsg, automaticCreationCommitMessage)
}
