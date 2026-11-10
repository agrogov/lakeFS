package azure

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
)

type fakeBlobNamespacePager struct {
	entries []blobNamespaceEntry
}

func (p *fakeBlobNamespacePager) List(_ context.Context, prefix, _ string) ([]blobNamespaceEntry, string, bool, error) {
	entries := make([]blobNamespaceEntry, 0, len(p.entries))
	for _, entry := range p.entries {
		if strings.HasPrefix(entry.Name, prefix) {
			entries = append(entries, entry)
		}
	}
	return entries, "", false, nil
}

type fakeBlobNamespaceDeleter struct {
	batches [][]string
}

func (d *fakeBlobNamespaceDeleter) DeleteBatch(_ context.Context, names []string) error {
	d.batches = append(d.batches, append([]string(nil), names...))
	return nil
}

func (d *fakeBlobNamespaceDeleter) DeleteDirectories(_ context.Context, names []string) error {
	d.batches = append(d.batches, append([]string(nil), names...))
	return nil
}

// TestDeleteNamespacePageDirectoryBlobs pins the Azure HNS rule of REPO_DELETE_STORAGE_DESIGN.md
// section 2.3: hdi_isfolder directory blobs are skipped during the file passes and, on the final
// Done page, deleted deepest-first so a directory is empty when its own Delete Blob arrives.
func TestDeleteNamespacePageDirectoryBlobs(t *testing.T) {
	t.Parallel()
	pager := &fakeBlobNamespacePager{entries: []blobNamespaceEntry{
		{Name: "ns/a/file"},
		{Name: "ns/b/deep/file"},
		{Name: "ns/b", IsFolder: true},
		{Name: "ns/b/deep", IsFolder: true},
		{Name: "ns/dir", IsFolder: true},
		{Name: "ns/dir/sub", IsFolder: true},
		{Name: "ns-metadata/keep"},
	}}
	deleter := &fakeBlobNamespaceDeleter{}

	result, err := deleteNamespacePage(t.Context(), "ns/", block.DeleteNamespaceOpts{Limit: 1000}, pager, deleter)
	require.NoError(t, err)
	require.True(t, result.Done)
	require.Equal(t, 2, result.Deleted)
	require.Equal(t, "dir/sub", result.LastKey)
	require.Equal(t, [][]string{
		{"ns/a/file", "ns/b/deep/file"},
		{"ns/dir/sub", "ns/b/deep"},
		{"ns/dir", "ns/b"},
	}, deleter.batches)
}
