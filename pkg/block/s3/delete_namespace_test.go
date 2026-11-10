package s3_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
)

// TestDeleteNamespacePageAccessDenied pins the per-key DeleteObjects error handling: an
// Errors[AccessDenied] response must fail the page and name the key, so an access-denied bucket
// surfaces instead of looking like a successful deletion (REPO_DELETE_STORAGE_DESIGN.md 2.3, L2).
func TestDeleteNamespacePageAccessDenied(t *testing.T) {
	t.Parallel()
	const (
		bucket = "bucket"
		prefix = "ns/"
		key    = "ns/a"
	)
	var listCalls, deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			listCalls.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>%s</Name>
  <Prefix>%s</Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>%s</Key>
    <LastModified>2024-01-01T00:00:00.000Z</LastModified>
    <ETag>"abc"</ETag>
    <Size>1</Size>
  </Contents>
</ListBucketResult>`, bucket, prefix, key)
		case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
			deleteCalls.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Error>
    <Key>%s</Key>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
  </Error>
</DeleteResult>`, key)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	adapter, err := s3a.NewAdapter(t.Context(), params.S3{
		Region:         "us-east-1",
		Endpoint:       server.URL,
		ForcePathStyle: true,
		Credentials: params.S3Credentials{
			AccessKeyID:     "test",
			SecretAccessKey: "test",
		},
	}, s3a.WithDiscoverBucketRegion(false))
	require.NoError(t, err)

	result, err := adapter.DeleteNamespacePage(t.Context(), "", "s3://"+bucket+"/ns", block.DeleteNamespaceOpts{Limit: 100})
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, s3a.ErrS3)
	require.Contains(t, err.Error(), key)
	require.Contains(t, err.Error(), "AccessDenied")
	require.EqualValues(t, 1, listCalls.Load())
	require.EqualValues(t, 1, deleteCalls.Load())
}
