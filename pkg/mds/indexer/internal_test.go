package indexer

// internal_test.go tests the small unexported rules of this package directly: the URL the lakeFS
// client is built for, the storage-namespace prefix test that §3.3 shares with the uncommitted GC,
// and the branch selectors of §4.1. The behaviour they feed is covered from the outside in
// *_test.go; these cases pin the rules themselves, including the inputs that are hard to reach
// through a tick.

import (
	"testing"

	"github.com/stretchr/testify/require"
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
