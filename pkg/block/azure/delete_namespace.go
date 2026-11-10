package azure

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/treeverse/lakefs/pkg/block"
)

const (
	// maxAzureBatchSize is the Azure Blob Batch sub-request limit (Blob Batch REST API).
	maxAzureBatchSize = 256
)

// blobNamespaceEntry is one listed blob. IsFolder marks a hierarchical namespace (HNS) directory
// blob: Delete Blob removes such a blob only when the directory is empty, so it has to wait for
// every file below it (REPO_DELETE_STORAGE_DESIGN.md section 2.3).
type blobNamespaceEntry struct {
	Name     string
	IsFolder bool
}

// blobNamespacePager lists one service page of the blobs under prefix. A marker of "" starts at
// the beginning.
type blobNamespacePager interface {
	List(ctx context.Context, prefix, marker string) (entries []blobNamespaceEntry, nextMarker string, hasMore bool, err error)
}

// blobNamespaceDeleter deletes one batch of at most maxAzureBatchSize blobs.
type blobNamespaceDeleter interface {
	DeleteBatch(ctx context.Context, names []string) error
	// DeleteDirectories deletes HNS directory blobs. A directory that still has content below it
	// cannot be deleted yet, and that is not an error: the marker page retries after the marker is
	// gone.
	DeleteDirectories(ctx context.Context, names []string) error
}

// DeleteNamespacePage deletes one page of the blobs under storageNamespace. Files are deleted in
// Blob Batch requests of at most 256 sub-requests. Directory blobs are skipped during the file
// passes and, on the final Done page, deleted deepest-first: an HNS directory can only be deleted
// once nothing is below it. A 404 sub-response counts as success.
func (a *Adapter) DeleteNamespacePage(ctx context.Context, storageID, storageNamespace string, opts block.DeleteNamespaceOpts) (*block.DeleteNamespaceResult, error) {
	var err error
	defer reportMetrics("DeleteNamespacePage", storageID, time.Now(), nil, &err)

	root, prefix, err := block.NamespaceDeletePrefix(storageNamespace)
	if err != nil {
		return nil, err
	}
	rootURL, err := url.Parse(root)
	if err != nil {
		return nil, fmt.Errorf("parse namespace root %q: %w", root, err)
	}
	info, err := ResolveBlobURLInfoFromURL(rootURL)
	if err != nil {
		return nil, err
	}
	containerClient, err := a.clientCache.NewContainerClient(info.StorageAccountName, info.ContainerName)
	if err != nil {
		return nil, err
	}
	return deleteNamespacePage(ctx, prefix, opts,
		&containerBlobNamespacePager{client: containerClient},
		&containerBlobNamespaceDeleter{client: containerClient})
}

// deleteNamespacePage is the backend-independent part of the Azure deletion, parameterised by its
// listing and deletion primitives so the ordering rules are unit-testable without an HNS account.
func deleteNamespacePage(ctx context.Context, prefix string, opts block.DeleteNamespaceOpts, pager blobNamespacePager, deleter blobNamespaceDeleter) (*block.DeleteNamespaceResult, error) {
	limit := opts.Limit
	if limit <= 0 || limit > maxAzureBatchSize {
		limit = maxAzureBatchSize
	}
	fullAfter := ""
	if opts.After != "" {
		fullAfter = prefix + opts.After
	}

	result := &block.DeleteNamespaceResult{LastKey: opts.After}
	fileNames := make([]string, 0, limit)
	marker := ""
	listingExhausted := false
	limitReached := false
	for !listingExhausted && !limitReached {
		entries, nextMarker, hasMore, err := pager.List(ctx, prefix, marker)
		if err != nil {
			return nil, fmt.Errorf("list azure blobs under %q: %w", prefix, err)
		}
		for _, entry := range entries {
			if fullAfter != "" && entry.Name <= fullAfter {
				continue
			}
			// The second guard (REPO_DELETE_STORAGE_DESIGN.md section 2.2): a listing bug must fail
			// the page rather than delete outside the namespace.
			if !strings.HasPrefix(entry.Name, prefix) {
				return nil, fmt.Errorf("%w: %q", block.ErrNamespaceDeleteOutsidePrefix, entry.Name)
			}
			relativeKey := strings.TrimPrefix(entry.Name, prefix)
			if entry.IsFolder {
				result.LastKey = relativeKey
				continue
			}
			if isKeptDeleteNamespaceKey(relativeKey, opts.Keep) {
				result.LastKey = relativeKey
				continue
			}
			if len(fileNames) == limit {
				limitReached = true
				break
			}
			fileNames = append(fileNames, entry.Name)
			result.LastKey = relativeKey
		}
		if !hasMore || nextMarker == "" {
			listingExhausted = true
		} else {
			marker = nextMarker
		}
	}
	result.Done = listingExhausted && !limitReached

	for start := 0; start < len(fileNames); start += maxAzureBatchSize {
		end := min(start+maxAzureBatchSize, len(fileNames))
		if err := deleter.DeleteBatch(ctx, fileNames[start:end]); err != nil {
			return nil, err
		}
	}
	result.Deleted = len(fileNames)

	if result.Done {
		if err := deleteDirectoryBlobs(ctx, prefix, pager, deleter); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// deleteDirectoryBlobs deletes the remaining HNS directory blobs deepest-first. Batches never mix
// two depths, so a parent is only attempted after every child has been removed.
func deleteDirectoryBlobs(ctx context.Context, prefix string, pager blobNamespacePager, deleter blobNamespaceDeleter) error {
	type directoryBlob struct {
		name  string
		depth int
	}
	var directories []directoryBlob
	marker := ""
	for {
		entries, nextMarker, hasMore, err := pager.List(ctx, prefix, marker)
		if err != nil {
			return fmt.Errorf("list azure directory blobs under %q: %w", prefix, err)
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name, prefix) {
				return fmt.Errorf("%w: %q", block.ErrNamespaceDeleteOutsidePrefix, entry.Name)
			}
			if entry.IsFolder {
				directories = append(directories, directoryBlob{name: entry.Name, depth: strings.Count(entry.Name, "/")})
			}
		}
		if !hasMore || nextMarker == "" {
			break
		}
		marker = nextMarker
	}
	sort.Slice(directories, func(i, j int) bool {
		if directories[i].depth != directories[j].depth {
			return directories[i].depth > directories[j].depth
		}
		return directories[i].name > directories[j].name
	})
	for start := 0; start < len(directories); {
		depth := directories[start].depth
		end := start
		for end < len(directories) && end-start < maxAzureBatchSize && directories[end].depth == depth {
			end++
		}
		names := make([]string, 0, end-start)
		for _, directory := range directories[start:end] {
			names = append(names, directory.name)
		}
		if err := deleter.DeleteDirectories(ctx, names); err != nil {
			return err
		}
		start = end
	}
	return nil
}

type containerBlobNamespacePager struct {
	client *container.Client
}

func (p *containerBlobNamespacePager) List(ctx context.Context, prefix, marker string) ([]blobNamespaceEntry, string, bool, error) {
	options := &container.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Metadata: true},
	}
	if marker != "" {
		options.Marker = &marker
	}
	pager := p.client.NewListBlobsFlatPager(options)
	if !pager.More() {
		return nil, "", false, nil
	}
	response, err := pager.NextPage(ctx)
	if err != nil {
		return nil, "", false, err
	}
	entries := make([]blobNamespaceEntry, 0, len(response.Segment.BlobItems))
	for _, item := range response.Segment.BlobItems {
		if item.Name == nil {
			continue
		}
		entries = append(entries, blobNamespaceEntry{
			Name:     *item.Name,
			IsFolder: isBlobItemFolder(item),
		})
	}
	nextMarker := ""
	if response.NextMarker != nil {
		nextMarker = *response.NextMarker
	}
	return entries, nextMarker, pager.More(), nil
}

type containerBlobNamespaceDeleter struct {
	client *container.Client
}

func (d *containerBlobNamespaceDeleter) DeleteBatch(ctx context.Context, names []string) error {
	return d.delete(ctx, names, false)
}

func (d *containerBlobNamespaceDeleter) DeleteDirectories(ctx context.Context, names []string) error {
	return d.delete(ctx, names, true)
}

func (d *containerBlobNamespaceDeleter) delete(ctx context.Context, names []string, tolerateNotEmpty bool) error {
	if len(names) == 0 {
		return nil
	}
	builder, err := d.client.NewBatchBuilder()
	if err != nil {
		return fmt.Errorf("create azure blob batch: %w", err)
	}
	for _, name := range names {
		if err := builder.Delete(name, nil); err != nil {
			return fmt.Errorf("add %q to azure blob batch: %w", name, err)
		}
	}
	response, err := d.client.SubmitBatch(ctx, builder, nil)
	if err != nil {
		return fmt.Errorf("submit azure blob batch: %w", err)
	}
	for index, item := range response.Responses {
		if item == nil || item.Error == nil {
			continue
		}
		if bloberror.HasCode(item.Error, bloberror.BlobNotFound) {
			continue
		}
		if tolerateNotEmpty && isDirectoryNotEmpty(item.Error) {
			continue
		}
		name := ""
		switch {
		case item.ContentID != nil && *item.ContentID >= 0 && *item.ContentID < len(names):
			name = names[*item.ContentID]
		case item.BlobName != nil:
			name = *item.BlobName
		case index < len(names):
			name = names[index]
		}
		return fmt.Errorf("delete azure blob %q: %w", name, item.Error)
	}
	return nil
}

// isDirectoryNotEmpty reports the HNS error of deleting a directory that still has content below it.
func isDirectoryNotEmpty(err error) bool {
	var responseError *azcore.ResponseError
	if errors.As(err, &responseError) {
		return responseError.ErrorCode == "DirectoryNotEmpty"
	}
	return strings.Contains(err.Error(), "DirectoryNotEmpty")
}

func isKeptDeleteNamespaceKey(key string, keep []string) bool {
	for _, kept := range keep {
		if key == kept {
			return true
		}
	}
	return false
}
