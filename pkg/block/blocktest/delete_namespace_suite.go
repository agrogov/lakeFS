package blocktest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"golang.org/x/sync/errgroup"
)

const (
	deleteNamespaceObjects        = 2500
	deleteNamespacePutJobs        = 32
	deleteNamespacePageSize       = 1000
	deleteNamespaceMaxPages       = 100
	deleteNamespaceKeepDeleted    = 2
	deleteNamespaceResumeObjects  = 25
	deleteNamespaceResumePageSize = 10
	deleteNamespaceResumeMaxPages = 10
	deleteNamespaceResumeMinPages = 3
)

// testAdapterDeleteNamespace is the conformance suite of Adapter.DeleteNamespacePage. Its sibling
// and parent cases are the acceptance gate for "nothing outside the namespace" (A2).
func testAdapterDeleteNamespace(t *testing.T, adapter block.Adapter, storageNamespace string) {
	t.Run("many_objects", func(t *testing.T) {
		ctx := context.Background()
		namespace := storageNamespace + "/delete_many"
		deleteNamespacePutMany(t, adapter, namespace, deleteNamespaceObjects)

		total := 0
		for page := 0; ; page++ {
			require.Less(t, page, deleteNamespaceMaxPages, "namespace deletion did not reach Done")
			result, err := adapter.DeleteNamespacePage(ctx, "", namespace, block.DeleteNamespaceOpts{Limit: deleteNamespacePageSize})
			require.NoError(t, err)
			total += result.Deleted
			if result.Done {
				break
			}
		}
		require.Equal(t, deleteNamespaceObjects, total)
		require.Empty(t, deleteNamespaceList(t, adapter, namespace), "namespace should list empty")
	})

	t.Run("outside_objects_survive", func(t *testing.T) {
		namespace := storageNamespace + "/delete_survive"
		deleteNamespacePut(t, adapter, namespace, "obj/0")
		deleteNamespacePut(t, adapter, namespace, "obj/1")

		siblingAddress := deleteNamespaceAddress(t, adapter, namespace+"-metadata", "x")
		require.NoError(t, deleteNamespacePutAddress(adapter, namespace, siblingAddress), "put sibling object")
		parentAddress := deleteNamespaceParentAddress(t, adapter, namespace)
		require.NoError(t, deleteNamespacePutAddress(adapter, namespace, parentAddress), "put parent object")

		deleteNamespaceAll(t, adapter, namespace)

		require.Empty(t, deleteNamespaceList(t, adapter, namespace), "namespace should list empty")
		require.True(t, deleteNamespaceExists(t, adapter, namespace, siblingAddress), "sibling prefix object must survive")
		require.True(t, deleteNamespaceExists(t, adapter, namespace, parentAddress), "parent prefix object must survive")
	})

	t.Run("keep_marker", func(t *testing.T) {
		ctx := context.Background()
		namespace := storageNamespace + "/delete_keep"
		deleteNamespacePut(t, adapter, namespace, "a")
		deleteNamespacePut(t, adapter, namespace, "b")
		deleteNamespacePut(t, adapter, namespace, "_lakefs/dummy")

		result, err := adapter.DeleteNamespacePage(ctx, "", namespace, block.DeleteNamespaceOpts{
			Limit: deleteNamespacePageSize,
			Keep:  []string{"_lakefs/dummy"},
		})
		require.NoError(t, err)
		require.True(t, result.Done)
		require.Equal(t, deleteNamespaceKeepDeleted, result.Deleted)
		require.Equal(t, []string{"_lakefs/dummy"}, deleteNamespaceList(t, adapter, namespace))

		result, err = adapter.DeleteNamespacePage(ctx, "", namespace, block.DeleteNamespaceOpts{Limit: deleteNamespacePageSize})
		require.NoError(t, err)
		require.True(t, result.Done)
		require.Equal(t, 1, result.Deleted)
		require.Empty(t, deleteNamespaceList(t, adapter, namespace))
	})

	t.Run("resume_from_last_key", func(t *testing.T) {
		ctx := context.Background()
		namespace := storageNamespace + "/delete_resume"
		const objectCount = deleteNamespaceResumeObjects
		for i := range objectCount {
			deleteNamespacePut(t, adapter, namespace, fmt.Sprintf("r/%04d", i))
		}

		total := 0
		after := ""
		pages := 0
		for {
			require.Less(t, pages, deleteNamespaceResumeMaxPages, "namespace deletion did not reach Done")
			result, err := adapter.DeleteNamespacePage(ctx, "", namespace, block.DeleteNamespaceOpts{
				After: after,
				Limit: deleteNamespaceResumePageSize,
			})
			require.NoError(t, err)
			total += result.Deleted
			pages++
			if result.Done {
				break
			}
			require.NotEmpty(t, result.LastKey, "a page that is not Done must return the next marker")
			after = result.LastKey
		}
		require.GreaterOrEqual(t, pages, deleteNamespaceResumeMinPages, "Limit 10 over 25 objects must take more than one page")
		require.Equal(t, objectCount, total)
		require.Empty(t, deleteNamespaceList(t, adapter, namespace))
	})

	t.Run("missing_namespace", func(t *testing.T) {
		namespace := storageNamespace + "/delete_missing"
		result, err := adapter.DeleteNamespacePage(context.Background(), "", namespace, block.DeleteNamespaceOpts{Limit: deleteNamespacePageSize})
		require.NoError(t, err)
		require.True(t, result.Done)
		require.Zero(t, result.Deleted)
	})

	t.Run("namespace_root", func(t *testing.T) {
		root := deleteNamespaceRoot(t, storageNamespace)
		_, err := adapter.DeleteNamespacePage(context.Background(), "", root, block.DeleteNamespaceOpts{Limit: deleteNamespacePageSize})
		require.ErrorIs(t, err, block.ErrNamespaceIsRoot)
	})
}

func deleteNamespaceAll(t *testing.T, adapter block.Adapter, namespace string) {
	t.Helper()
	ctx := context.Background()
	for page := 0; ; page++ {
		require.Less(t, page, deleteNamespaceMaxPages, "namespace deletion did not reach Done")
		result, err := adapter.DeleteNamespacePage(ctx, "", namespace, block.DeleteNamespaceOpts{Limit: deleteNamespacePageSize})
		require.NoError(t, err)
		if result.Done {
			return
		}
	}
}

func deleteNamespacePutMany(t *testing.T, adapter block.Adapter, namespace string, count int) {
	t.Helper()
	group := errgroup.Group{}
	group.SetLimit(deleteNamespacePutJobs)
	for i := range count {
		key := fmt.Sprintf("obj/%04d", i)
		group.Go(func() error {
			return deleteNamespacePutError(adapter, namespace, key)
		})
	}
	require.NoError(t, group.Wait())
}

func deleteNamespacePut(t *testing.T, adapter block.Adapter, namespace, key string) {
	t.Helper()
	require.NoError(t, deleteNamespacePutError(adapter, namespace, key))
}

func deleteNamespacePutError(adapter block.Adapter, namespace, key string) error {
	address, err := deleteNamespaceAddressError(adapter, namespace, key)
	if err != nil {
		return err
	}
	return deleteNamespacePutAddress(adapter, namespace, address)
}

func deleteNamespacePutAddress(adapter block.Adapter, storageNamespace, address string) error {
	_, err := adapter.Put(context.Background(), block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       address,
		IdentifierType:   block.IdentifierTypeFull,
	}, int64(len(address)), strings.NewReader(address), block.PutOpts{})
	return err
}

func deleteNamespaceAddress(t *testing.T, adapter block.Adapter, storageNamespace, key string) string {
	t.Helper()
	address, err := deleteNamespaceAddressError(adapter, storageNamespace, key)
	require.NoError(t, err)
	return address
}

func deleteNamespaceAddressError(adapter block.Adapter, storageNamespace, key string) (string, error) {
	qk, err := adapter.ResolveNamespace("", storageNamespace, key, block.IdentifierTypeRelative)
	if err != nil {
		return "", err
	}
	return qk.Format(), nil
}

func deleteNamespaceExists(t *testing.T, adapter block.Adapter, storageNamespace, address string) bool {
	t.Helper()
	exists, err := adapter.Exists(context.Background(), block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       address,
		IdentifierType:   block.IdentifierTypeFull,
	})
	require.NoError(t, err)
	return exists
}

// deleteNamespaceParentAddress addresses "<parent>/y" for the namespace: one level up from the
// namespace directory or prefix, and therefore outside the namespace itself.
func deleteNamespaceParentAddress(t *testing.T, adapter block.Adapter, storageNamespace string) string {
	t.Helper()
	address := deleteNamespaceAddress(t, adapter, storageNamespace, "")
	parsed, err := url.Parse(strings.TrimSuffix(address, "/"))
	require.NoError(t, err)
	parent := path.Dir(parsed.Path)
	return parsed.Scheme + "://" + parsed.Host + path.Join("/", parent, "y")
}

func deleteNamespaceRoot(t *testing.T, storageNamespace string) string {
	t.Helper()
	parsed, err := url.Parse(storageNamespace)
	require.NoError(t, err)
	switch parsed.Scheme {
	case "local", "mem":
		return parsed.Scheme + "://"
	default:
		root, _, err := block.NamespaceRoot(storageNamespace)
		require.NoError(t, err)
		return root
	}
}

// deleteNamespaceList returns the keys under the namespace, relative to it. The walker is given a
// trailing-slash URI so its RelativeKey is namespace-relative, and a trimmed URI for GetWalker
// because the local adapter rejects a path with a trailing separator.
func deleteNamespaceList(t *testing.T, adapter block.Adapter, storageNamespace string) []string {
	t.Helper()
	ctx := context.Background()
	address := deleteNamespaceAddress(t, adapter, storageNamespace, "")
	trimmed, err := url.Parse(strings.TrimSuffix(address, "/"))
	require.NoError(t, err)
	walkURI, err := url.Parse(strings.TrimSuffix(address, "/") + "/")
	require.NoError(t, err)

	walker, err := adapter.GetWalker("", block.WalkerOptions{StorageURI: trimmed})
	require.NoError(t, err)
	var keys []string
	err = walker.Walk(ctx, walkURI, block.WalkOptions{}, func(e block.ObjectStoreEntry) error {
		keys = append(keys, e.RelativeKey)
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	return keys
}
