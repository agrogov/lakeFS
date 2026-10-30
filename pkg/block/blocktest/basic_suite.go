package blocktest

import (
	"context"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
)

// AdapterBasicObjectTest Test suite of adapter basic functionality on objects
func AdapterBasicObjectTest(t *testing.T, adapter block.Adapter, storageNamespace, externalPath string) {
	t.Run("Adapter_PutGet", func(t *testing.T) { testAdapterPutGet(t, adapter, storageNamespace, externalPath) })
	t.Run("Adapter_GetFullAddressInOtherNamespace", func(t *testing.T) {
		testAdapterGetFullAddressInOtherNamespace(t, adapter, storageNamespace)
	})
	t.Run("Adapter_Copy", func(t *testing.T) { testAdapterCopy(t, adapter, storageNamespace) })
	t.Run("Adapter_Exists", func(t *testing.T) { testAdapterExists(t, adapter, storageNamespace) })
}

// Parameterized test to first Put object via Storage Adapter then Get it and check that the contents match
func testAdapterPutGet(t *testing.T, adapter block.Adapter, storageNamespace, externalPath string) {
	ctx := context.Background()
	const contents = "test_file"
	size := int64(len(contents))

	cases := []struct {
		name           string
		identifierType block.IdentifierType
		path           string
	}{
		{"identifier_relative", block.IdentifierTypeRelative, "test_file"},
		{"identifier_full", block.IdentifierTypeFull, externalPath + "/" + "test_file"},
		{"identifier_relative_escaped", block.IdentifierTypeRelative, "special%3Atest_file"},
		{"identifier_full_escaped", block.IdentifierTypeFull, externalPath + "/" + "special%3Atest_file"},
		{"identifier_unknown_relative", block.IdentifierTypeUnknownDeprecated, "test_file"},                  //nolint:staticcheck
		{"identifier_unknown_full", block.IdentifierTypeUnknownDeprecated, externalPath + "/" + "test_file"}, //nolint:staticcheck
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := block.ObjectPointer{
				StorageID:        "",
				StorageNamespace: storageNamespace,
				Identifier:       c.path,
				IdentifierType:   c.identifierType,
			}

			_, err := adapter.Put(ctx, obj, size, strings.NewReader(contents), block.PutOpts{})
			require.NoError(t, err)

			reader, err := adapter.Get(ctx, obj)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, reader.Close())
			}()
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, contents, string(got))
		})
	}
}

// testAdapterGetFullAddressInOtherNamespace pins the read path that zero-copy datasets rely on
// (DATASETS_DESIGN.md sections 6.3 and 18, spike S4): a dataset entry's PhysicalAddress is a FULL
// address pointing into the *source* repository's namespace, and it is read while the reading
// repository's namespace is the dataset's own (which holds none of that data).
//
// The source object is created with a RELATIVE identifier in namespace A and read from a DIFFERENT
// namespace B through an address that points into A. Unlike the identifier_full cases of
// testAdapterPutGet, this adapter never Put the object through the address it later reads, and the
// object never existed in namespace B -- so a successful read can only come from resolving the full
// address, not from a fallback to the ObjectPointer's StorageNamespace.
func testAdapterGetFullAddressInOtherNamespace(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	const contents = "zero-copy-dataset-bytes"

	// Distinct key per test: the docker-backed adapters share one bucket/container per package run, so
	// this keeps the case independent of the other cases in the suite.
	relativePath := path.Join("full_address_read", strings.ReplaceAll(t.Name(), "/", "_"), "c1.parquet")

	// The source object is written the way a source repository writes it: relative identifier, in
	// namespace A, with no knowledge of any dataset.
	_, err := adapter.Put(ctx, block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: storageNamespace,
		Identifier:       relativePath,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err, "put source object with a relative identifier into namespace A")

	// Namespace B is a sibling of A, so it is allocated the way each adapter allocates namespaces:
	// the same bucket/container/account, or a sibling directory under the local adapter's root (the
	// root is always an allowed path, so local's allowed_external_prefixes restriction is not in
	// play here -- see pkg/datasets/spikes_address_test.go for that rule).
	otherNamespace := storageNamespace + "-dataset-reader"

	// Build the address exactly as publish does (design section 6.3 converts only RELATIVE addresses):
	// namespace A joined with the relative path. For URI-namespaced backends that is A + "/" +
	// relativePath (the Azure QualifiedKey formats the scheme as https even when the namespace uses an
	// http test endpoint, so compare content rather than the literal string); the local adapter maps a
	// namespace onto a directory below its root and so yields the equivalent absolute form,
	// local://<root>/<namespace>/<relativePath>.
	qk, err := adapter.ResolveNamespace("", storageNamespace, relativePath, block.IdentifierTypeRelative)
	require.NoError(t, err, "resolve namespace A")
	fullAddress := qk.Format()
	require.Contains(t, fullAddress, relativePath, "the full address must point at the source object")
	require.NotContains(t, fullAddress, otherNamespace, "the full address must point into namespace A, not into the namespace it is read through")

	fullObj := block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: otherNamespace,
		Identifier:       fullAddress,
		IdentifierType:   block.IdentifierTypeFull,
	}
	reader, err := adapter.Get(ctx, fullObj)
	require.NoError(t, err, "get a full address living in namespace A while reading through namespace B")
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, contents, string(got), "bytes read through the full address")

	// The object is genuinely absent from namespace B, which proves the read above went through the
	// full address and not through a lucky namespace fallback.
	relativeInOther := block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: otherNamespace,
		Identifier:       relativePath,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	_, err = adapter.Get(ctx, relativeInOther)
	require.ErrorIs(t, err, block.ErrDataNotFound,
		"a relative identifier in namespace B must not reach the object stored in namespace A")
	exists, err := adapter.Exists(ctx, relativeInOther)
	require.NoError(t, err)
	require.False(t, exists, "the object must not exist in namespace B")

	// No explicit cleanup: the Adapter interface exposes no Delete, and both namespaces are private to
	// the caller -- the mem adapter is per-test, local uses t.TempDir, and the docker-backed adapters
	// purge their container in TestMain. Namespace B is never written to.
}

// Test to Copy an object via Storage Adapter, then check that the contents of the copied object matches the original
func testAdapterCopy(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	contents := "foo bar baz quux"
	src := block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: storageNamespace,
		Identifier:       "src",
		IdentifierType:   block.IdentifierTypeRelative,
	}
	dst := block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: storageNamespace,
		Identifier:       "export/to/dst",
		IdentifierType:   block.IdentifierTypeRelative,
	}
	_, err := adapter.Put(ctx, src, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	require.NoError(t, adapter.Copy(ctx, src, dst))
	reader, err := adapter.Get(ctx, dst)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, contents, string(got))
}

// Parameterized test of the object Exists method of the Storage adapter
func testAdapterExists(t *testing.T, adapter block.Adapter, storageNamespace string) {
	// TODO (niro): Test abs paths
	const contents = "exists"
	ctx := context.Background()
	_, err := adapter.Put(ctx, block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: storageNamespace,
		Identifier:       contents,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	_, err = adapter.Put(ctx, block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: storageNamespace,
		Identifier:       "nested/and/" + contents,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	cases := []struct {
		name   string
		path   string
		exists bool
	}{
		{"exists", "exists", true},
		{"nested_exists", "nested/and/exists", true},
		{"simple_missing", "missing", false},
		{"nested_missing", "nested/down", false},
		{"nested_deep_missing", "nested/quite/deeply/and/missing", false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := adapter.Exists(ctx, block.ObjectPointer{
				StorageID:        "",
				StorageNamespace: storageNamespace,
				Identifier:       tt.path,
				IdentifierType:   block.IdentifierTypeRelative,
			})
			require.NoError(t, err)
			require.Equal(t, tt.exists, ok)
		})
	}
}
