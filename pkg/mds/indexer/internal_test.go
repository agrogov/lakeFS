package indexer

// internal_test.go tests the small unexported rules of this package directly: the URL the lakeFS
// client is built for, the storage-namespace prefix test that §3.3 shares with the uncommitted GC,
// the branch selectors of §4.1, and the two error mappings at the package's seams (the forbidden
// write of §3.3/§4.8 and the writer's conflicts of §4.4). The behaviour they feed is covered from
// the outside in *_test.go; these cases pin the rules themselves, including the inputs that are
// hard to reach through a tick.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/mds/client"
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

// TestMetadataNamespaceRoots pins the bucket-root and container-root rules of §3.3 on the helper
// itself (METADATA_SEARCH_AZURE D4): a namespace with a path component derives the companion in the
// same bucket, and on Azure in the same container, while a bucket root - and an Azure container
// root, whose first path segment is the container - is refused with an error that names the
// container to set an explicit storage_namespace for.
func TestMetadataNamespaceRoots(t *testing.T) {
	tests := []struct {
		name          string
		dataNamespace string
		want          string
		wantErr       bool
		wantContainer string
	}{
		{name: "s3 path derives", dataNamespace: "s3://bucket/data", want: "s3://bucket/data-metadata"},
		{name: "s3 bucket root is refused", dataNamespace: "s3://bucket", wantErr: true},
		{
			name:          "azure path derives in the same container",
			dataNamespace: "https://acct.blob.core.windows.net/data/repo",
			want:          "https://acct.blob.core.windows.net/data/repo-metadata",
		},
		{
			name:          "azure container root is refused",
			dataNamespace: "https://acct.blob.core.windows.net/data",
			wantErr:       true,
			wantContainer: "data",
		},
		{
			name:          "azure container root with the http scheme is refused",
			dataNamespace: "http://acct.blob.core.windows.net/data",
			wantErr:       true,
			wantContainer: "data",
		},
		{
			name:          "azure container root with a trailing slash is refused",
			dataNamespace: "https://acct.blob.core.windows.net/data/",
			wantErr:       true,
			wantContainer: "data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := metadataNamespace("repo", test.dataNamespace, "")
			if test.wantErr {
				require.ErrorIs(t, err, ErrBucketRootNamespace)
				require.Empty(t, got)
				if test.wantContainer != "" {
					require.Contains(t, err.Error(), fmt.Sprintf("container %q", test.wantContainer),
						"the refusal names the container to set a storage namespace under")
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
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
		// On Azure http and https name the same container and prefix (METADATA_SEARCH_AZURE D4), so a
		// companion on one scheme overlaps a data namespace on the other.
		{
			namespace: "http://acct.blob.core.windows.net/data/repo-metadata",
			other:     "https://acct.blob.core.windows.net/data/repo",
			want:      false,
		},
		{
			namespace: "http://acct.blob.core.windows.net/data/repo/metadata",
			other:     "https://acct.blob.core.windows.net/data/repo",
			want:      true,
		},
		{
			namespace: "https://acct.blob.core.windows.net/data/repo-metadata",
			other:     "https://acct.blob.core.windows.net/data/repo",
			want:      false,
		},
		// Another Azure account or container is a different physical prefix, whatever the scheme.
		{
			namespace: "http://acct.blob.core.windows.net/data/repo",
			other:     "https://other.blob.core.windows.net/data/repo",
			want:      false,
		},
		{
			namespace: "http://acct.blob.core.windows.net/other/repo",
			other:     "https://acct.blob.core.windows.net/data/repo",
			want:      false,
		},
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

// TestMetadataBranchWriteError pins R3I-1: a forbidden write is always the
// ErrReadOnlyMetadataRepository family (a companion repository that refuses every write must fail the
// repository, not every branch with a bare 403), but only lakeFS's own protection messages claim
// branch protection. A 401 authorization denial and a catalog permission refusal carry the same
// sentinel - pkg/mds/client/lakefs.go collapses 401 and 403 into client.ErrForbidden, and lakeFS
// answers an authorization failure with 401 by default - so the predicate must read the wrapped
// message, not the status.
func TestMetadataBranchWriteError(t *testing.T) {
	const (
		claimText = "a branch protection rule blocks"
		remedy    = "remove the rule"
	)

	t.Run("an authorization refusal is not branch protection", func(t *testing.T) {
		// The default denial of a principal that lacks the companion repository's grant: lakeFS
		// answers 401 (pkg/api/controller_authorization_status_test.go) and the client reports it as
		// client.ErrForbidden.
		err := fmt.Errorf("apply repo@s2: %w: permission denied: no policy grants fs:WriteObject", client.ErrForbidden)

		got := metadataBranchWriteError("repo-metadata", "main", err)

		require.ErrorIs(t, got, ErrReadOnlyMetadataRepository, "still the same family")
		require.ErrorIs(t, got, client.ErrForbidden, "the cause stays in the chain")
		require.NotContains(t, got.Error(), claimText, "branch protection is not the diagnosis")
		require.NotContains(t, got.Error(), remedy, "and the destructive remedy is not offered")
		require.NotContains(t, got.Error(), "read_only=false")
		require.Contains(t, got.Error(), "branch protection", "the neutral text still names the candidates")
		require.Contains(t, got.Error(), "read_only")
		require.Contains(t, got.Error(), "permissions")
		require.Contains(t, got.Error(), "no policy grants fs:WriteObject", "the cause is quoted")
	})

	t.Run("a catalog permission refusal is not branch protection", func(t *testing.T) {
		// pkg/icebergcatalog/tables.go answers a PermissionDeniedError 403, which iceberg-go reports
		// as rest.ErrForbidden: the indexer principal may not write that catalog table.
		err := fmt.Errorf("apply repo@s2: %w: ForbiddenException: permission denied for catalog:WriteTableData", rest.ErrForbidden)

		got := metadataBranchWriteError("repo-metadata", "main", err)

		require.ErrorIs(t, got, ErrReadOnlyMetadataRepository)
		require.ErrorIs(t, got, rest.ErrForbidden)
		require.NotContains(t, got.Error(), claimText)
		require.NotContains(t, got.Error(), remedy)
		require.Contains(t, got.Error(), "catalog:WriteTableData")
	})

	for _, test := range []struct {
		name string
		err  error
	}{
		{
			name: "commit to a protected branch",
			err:  fmt.Errorf("commit %q@main: %w: cannot commit to protected branch", "repo-metadata", client.ErrForbidden),
		},
		{
			name: "write to a protected branch",
			err:  fmt.Errorf("write %q@main: %w: cannot write to protected branch", "repo-metadata", client.ErrForbidden),
		},
		{
			name: "a read-only repository",
			err:  fmt.Errorf("commit %q@main: %w: read-only repository", "repo-metadata", client.ErrForbidden),
		},
		{
			name: "the catalog's staging refusal",
			err:  fmt.Errorf("create %s.%s: %w: ForbiddenException: cannot write to protected branch", table.Namespace, table.Name, rest.ErrForbidden),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := metadataBranchWriteError("repo-metadata", "main", test.err)

			require.ErrorIs(t, got, ErrReadOnlyMetadataRepository)
			require.ErrorIs(t, got, test.err, "the cause is wrapped")
			require.Contains(t, got.Error(), claimText, "lakeFS named the cause, so the text claims it")
			require.Contains(t, got.Error(), remedy)
		})
	}

	t.Run("a non-forbidden error passes through", func(t *testing.T) {
		err := errors.New("block: write failed")

		require.Equal(t, err, metadataBranchWriteError("repo-metadata", "main", err))
	})

	t.Run("an already mapped error is not wrapped twice", func(t *testing.T) {
		err := fmt.Errorf("%w: %q", ErrReadOnlyMetadataRepository, "repo-metadata")

		require.Equal(t, err, metadataBranchWriteError("repo-metadata", "main", err))
	})
}

// TestTableErrorMapsTheWriterConflict pins R3I-3: table.ErrStaleTable is the writer's second "the
// table moved since it was read" answer (the repair commit of ensureNoReplay observed the move), and
// pkg/mds/table/writer.go documents it as the caller's retry (§4.4). runBranch restarts a branch on
// ErrConflict only, so tableError has to map it like icebergtable.ErrCommitFailed: otherwise the
// branch fails for the whole tick - and /_health reports the table as broken - exactly in the
// two-replica overlap the retry exists for.
func TestTableErrorMapsTheWriterConflict(t *testing.T) {
	t.Run("a stale table", func(t *testing.T) {
		err := fmt.Errorf("apply repo@s2: %w: snapshot 1 is now 2", table.ErrStaleTable)

		got := tableError(err)

		require.ErrorIs(t, got, ErrConflict, "the tick reloads and retries")
		require.ErrorIs(t, got, table.ErrStaleTable, "the writer's cause stays in the chain")
	})

	t.Run("a refused commit", func(t *testing.T) {
		got := tableError(fmt.Errorf("commit: %w", icebergtable.ErrCommitFailed))

		require.ErrorIs(t, got, ErrConflict)
		require.ErrorIs(t, got, icebergtable.ErrCommitFailed)
	})

	t.Run("any other error passes through", func(t *testing.T) {
		err := fmt.Errorf("read: %w", table.ErrUnknownSchema)

		require.Equal(t, err, tableError(err))
	})
}
