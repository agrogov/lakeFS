package gs

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/treeverse/lakefs/pkg/block"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

const (
	// gcsDeleteConcurrency bounds the per-object Delete requests of one page: the GCS Go client
	// has no batch API.
	gcsDeleteConcurrency = 32
	// defaultDeleteNamespacePageSize is used when the caller leaves Limit unset.
	defaultDeleteNamespacePageSize = 1000
)

// DeleteNamespacePage deletes one page of the objects under storageNamespace with one
// ObjectHandle.Delete per object on an errgroup limited to gcsDeleteConcurrency. Deleting an object
// that is already gone (storage.ErrObjectNotExist) is success (REPO_DELETE_STORAGE_DESIGN.md
// section 2.3).
func (a *Adapter) DeleteNamespacePage(ctx context.Context, storageID, storageNamespace string, opts block.DeleteNamespaceOpts) (*block.DeleteNamespaceResult, error) {
	root, prefix, err := block.NamespaceDeletePrefix(storageNamespace)
	if err != nil {
		return nil, err
	}
	rootURL, err := url.Parse(root)
	if err != nil {
		return nil, fmt.Errorf("parse namespace root %q: %w", root, err)
	}
	bucket := a.client.Bucket(rootURL.Host)

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultDeleteNamespacePageSize
	}
	fullAfter := ""
	if opts.After != "" {
		fullAfter = prefix + opts.After
	}
	query := &storage.Query{Prefix: prefix}
	if fullAfter != "" {
		query.StartOffset = fullAfter
	}
	iter := bucket.Objects(ctx, query)

	result := &block.DeleteNamespaceResult{LastKey: opts.After}
	keys := make([]string, 0, limit)
	for len(keys) < limit {
		attrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			result.Done = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list objects under %q: %w", prefix, err)
		}
		if fullAfter != "" && attrs.Name <= fullAfter {
			continue
		}
		// The second guard (REPO_DELETE_STORAGE_DESIGN.md section 2.2): a listing bug must fail the
		// page rather than delete outside the namespace.
		if !strings.HasPrefix(attrs.Name, prefix) {
			return nil, fmt.Errorf("%w: %q", block.ErrNamespaceDeleteOutsidePrefix, attrs.Name)
		}
		keys = append(keys, attrs.Name)
	}
	// The iterator reports the end only on the Next that follows the last object. Peek once so a
	// page that ends exactly on the listing can say Done instead of making the worker ask again.
	if len(keys) == limit && !result.Done {
		_, err := iter.Next()
		switch {
		case errors.Is(err, iterator.Done):
			result.Done = true
		case err != nil:
			return nil, fmt.Errorf("list objects under %q: %w", prefix, err)
		}
	}

	deleteKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		relativeKey := strings.TrimPrefix(key, prefix)
		result.LastKey = relativeKey
		if isKeptDeleteNamespaceKey(relativeKey, opts.Keep) {
			continue
		}
		deleteKeys = append(deleteKeys, key)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(gcsDeleteConcurrency)
	for _, key := range deleteKeys {
		group.Go(func() error {
			err := bucket.Object(key).Delete(groupCtx)
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				return fmt.Errorf("delete object %q: %w", key, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	result.Deleted = len(deleteKeys)
	return result, nil
}

func isKeptDeleteNamespaceKey(key string, keep []string) bool {
	for _, kept := range keep {
		if key == kept {
			return true
		}
	}
	return false
}
