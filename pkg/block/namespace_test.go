package block_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/block"
)

func TestResolveNamespace(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name             string
		DefaultNamespace string
		Key              string
		Type             block.IdentifierType
		ExpectedErr      error
		Expected         block.CommonQualifiedKey
	}{
		{
			Name:             "valid_namespace_no_trailing_slash",
			DefaultNamespace: "s3://foo",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "foo",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_namespace_with_trailing_slash",
			DefaultNamespace: "s3://foo/",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "foo",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_namespace_mem_with_trailing_slash",
			DefaultNamespace: "mem://foo/",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeMem,
				StorageNamespace: "foo",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_namespace_with_prefix_and_trailing_slash",
			DefaultNamespace: "gs://foo/bla/",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeGS,
				StorageNamespace: "foo/bla",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_namespace_with_prefix_and_no_trailing_slash",
			DefaultNamespace: "gs://foo/bla",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeGS,
				StorageNamespace: "foo/bla",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_namespace_with_prefix_and_leading_key_slash",
			DefaultNamespace: "gs://foo/bla",
			Key:              "/bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeGS,
				StorageNamespace: "foo/bla",
				Key:              "/bar/baz",
			},
		},
		{
			Name:             "valid_fq_key",
			DefaultNamespace: "mem://foo/",
			Key:              "s3://example/bar/baz",
			Type:             block.IdentifierTypeFull,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "example",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_fq_key_encoded",
			DefaultNamespace: "s3://foo/",
			Key:              "s3://example/bar/foo%3Abaz",
			Type:             block.IdentifierTypeFull,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "example",
				Key:              "bar/foo%3Abaz",
			},
		},
		{
			Name:             "valid_relative_encoded",
			DefaultNamespace: "s3://foo/",
			Key:              "bar/foo%3Abaz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "foo",
				Key:              "bar/foo%3Abaz",
			},
		},
		{
			Name:             "invalid_namespace_wrong_scheme",
			DefaultNamespace: "memzzzz://foo/",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      block.ErrInvalidAddress,
			Expected:         block.CommonQualifiedKey{},
		},
		{
			Name:             "invalid_namespace_invalid_uri",
			DefaultNamespace: "foo",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      block.ErrInvalidAddress,
			Expected:         block.CommonQualifiedKey{},
		},
		{
			Name:             "invalid_key_wrong_scheme",
			DefaultNamespace: "s3://foo/",
			Key:              "s4://bar/baz",
			Type:             block.IdentifierTypeFull,
			ExpectedErr:      block.ErrInvalidAddress,
			Expected:         block.CommonQualifiedKey{},
		},
		{
			Name:             "key_weird_format",
			DefaultNamespace: "s3://foo/",
			Key:              "://invalid/baz",
			Type:             block.IdentifierTypeRelative,
			Expected: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "foo",
				Key:              "://invalid/baz",
			},
		},
	}

	for _, cas := range cases {
		for _, r := range []block.IdentifierType{cas.Type, block.IdentifierTypeUnknownDeprecated} {
			relativeName := ""
			switch r {
			case block.IdentifierTypeUnknownDeprecated:
				relativeName = "unknown"
			case block.IdentifierTypeRelative:
				relativeName = "relative"
			case block.IdentifierTypeFull:
				relativeName = "full"
			}
			t.Run(fmt.Sprintf("%s/%s", cas.Name, relativeName), func(t *testing.T) {
				resolved, err := block.DefaultResolveNamespace(cas.DefaultNamespace, cas.Key, r)
				if err != nil && !errors.Is(err, cas.ExpectedErr) {
					t.Fatalf("got unexpected error: %v - expected %v", err, cas.ExpectedErr)
				}
				if cas.ExpectedErr == nil {
					if diff := deep.Equal(resolved, cas.Expected); diff != nil {
						t.Fatalf("mismatch in resolved namespace: %s", diff)
					}
				}
			})
		}
	}
}

func TestFormatQualifiedKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name         string
		QualifiedKey block.CommonQualifiedKey
		Expected     string
	}{
		{
			Name: "simple_path",
			QualifiedKey: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeGS,
				StorageNamespace: "some-bucket",
				Key:              "path",
			},
			Expected: "gs://some-bucket/path",
		},
		{
			Name: "path_with_prefix",
			QualifiedKey: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "some-bucket/",
				Key:              "path/to/file",
			},
			Expected: "s3://some-bucket/path/to/file",
		},
		{
			Name: "bucket_with_prefix",
			QualifiedKey: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "some-bucket/prefix/",
				Key:              "path/to/file",
			},
			Expected: "s3://some-bucket/prefix/path/to/file",
		},
		{
			Name: "path_with_prefix_leading_slash",
			QualifiedKey: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "some-bucket",
				Key:              "/path/to/file",
			},
			Expected: "s3://some-bucket//path/to/file",
		},
		{
			Name: "bucket_with_prefix_leading_slash",
			QualifiedKey: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "some-bucket/prefix",
				Key:              "/path/to/file",
			},
			Expected: "s3://some-bucket/prefix//path/to/file",
		},
		{
			Name: "dont_eliminate_dots",
			QualifiedKey: block.CommonQualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "some-bucket/prefix/",
				Key:              "path/to/../file",
			},
			Expected: "s3://some-bucket/prefix/path/to/../file",
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			formatted := cas.QualifiedKey.Format()
			if formatted != cas.Expected {
				t.Fatalf("Format() got '%s', expected '%s'", formatted, cas.Expected)
			}
		})
	}
}

// TestNamespaceRoot pins the (root, path) split of METADATA_SEARCH_AZURE D4 for every blockstore:
// the root is "<scheme>://<host>" for s3, gs, mem and local, and "<scheme>://<host>/<container>" for
// Azure, where the first path segment is the container and not a prefix inside a bucket.
func TestNamespaceRoot(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name        string
		Namespace   string
		Expected    string
		ExpectedErr bool
		Path        string
	}{
		{Name: "s3_bucket_root", Namespace: "s3://bucket", Expected: "s3://bucket", Path: ""},
		{Name: "s3_bucket_root_trailing_slash", Namespace: "s3://bucket/", Expected: "s3://bucket", Path: ""},
		{Name: "s3_prefix", Namespace: "s3://bucket/data", Expected: "s3://bucket", Path: "data"},
		{Name: "s3_nested_prefix_trailing_slash", Namespace: "s3://bucket/data/inner/", Expected: "s3://bucket", Path: "data/inner"},
		{Name: "gs_prefix", Namespace: "gs://bucket/a/b", Expected: "gs://bucket", Path: "a/b"},
		{Name: "mem_bucket_root", Namespace: "mem://analytics", Expected: "mem://analytics", Path: ""},
		{Name: "local", Namespace: "local:///var/lib/lakefs", Expected: "local://", Path: "var/lib/lakefs"},
		{
			Name:      "azure_path",
			Namespace: "https://acct.blob.core.windows.net/c/p",
			Expected:  "https://acct.blob.core.windows.net/c",
			Path:      "p",
		},
		{
			Name:      "azure_nested_path_trailing_slash",
			Namespace: "https://acct.blob.core.windows.net/c/p/q/",
			Expected:  "https://acct.blob.core.windows.net/c",
			Path:      "p/q",
		},
		{
			Name:      "azure_path_http",
			Namespace: "http://acct.azurite.test/c/p",
			Expected:  "http://acct.azurite.test/c",
			Path:      "p",
		},
		{
			Name:      "azure_container_root",
			Namespace: "https://acct.blob.core.windows.net/c",
			Expected:  "https://acct.blob.core.windows.net/c",
			Path:      "",
		},
		{
			Name:      "azure_container_root_trailing_slash",
			Namespace: "https://acct.blob.core.windows.net/c/",
			Expected:  "https://acct.blob.core.windows.net/c",
			Path:      "",
		},
		{
			Name:      "azure_container_root_http",
			Namespace: "http://acct.azurite.test/c",
			Expected:  "http://acct.azurite.test/c",
			Path:      "",
		},
		{Name: "empty", Namespace: "", ExpectedErr: true},
		{Name: "no_scheme", Namespace: "noscheme", ExpectedErr: true},
		{Name: "relative_path", Namespace: "relative/path", ExpectedErr: true},
		{Name: "unknown_scheme", Namespace: "ftp://bucket/data", ExpectedErr: true},
		{Name: "azure_without_container", Namespace: "https://acct.blob.core.windows.net", ExpectedErr: true},
		{Name: "azure_without_container_trailing_slash", Namespace: "https://acct.blob.core.windows.net/", ExpectedErr: true},
	}
	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			t.Parallel()
			root, path, err := block.NamespaceRoot(cas.Namespace)
			if cas.ExpectedErr {
				if err == nil {
					t.Fatalf("NamespaceRoot(%q) got (%q, %q), expected an error", cas.Namespace, root, path)
				}
				return
			}
			if err != nil {
				t.Fatalf("NamespaceRoot(%q): %v", cas.Namespace, err)
			}
			if root != cas.Expected {
				t.Fatalf("NamespaceRoot(%q) root got %q, expected %q", cas.Namespace, root, cas.Expected)
			}
			if path != cas.Path {
				t.Fatalf("NamespaceRoot(%q) path got %q, expected %q", cas.Namespace, path, cas.Path)
			}
		})
	}
}
