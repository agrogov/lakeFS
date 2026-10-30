//go:build datasets

package esti

// datasets_blockstore_test.go decides which blockstore the datasets system-test suite runs
// against (DATASETS_DESIGN.md §18 spike S4).
//
// The suite starts its own lakefs-sso server, so it does not inherit the ambient esti server's
// blockstore unless we give it one. By default it stays on a self-contained local blockstore in
// a temporary directory, exactly as before, which keeps `make esti-datasets` runnable with no
// credentials and no docker.
//
// When the environment supplies a cloud blockstore (the product's LAKEFS_BLOCKSTORE_TYPE plus
// the esti storage namespace that the rest of esti reads), the suite instead runs the same
// scenarios against that blockstore: it allocates a unique per-run prefix below the esti
// namespace for the datasets and their sources, and removes that prefix when the test finishes.
// Namespaces are built with block.DefaultResolveNamespace, the same code path the product uses,
// so local://, s3://, gs:// and Azure's https:// forms are all produced by the blockstore
// implementation rather than by string concatenation.

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	// localDatasetsNamespace and localSourcesNamespace are the two prefixes the self-contained
	// local run uses inside its temporary blockstore directory. They are resolved to addresses
	// through block.DefaultResolveNamespace like every other namespace in this suite.
	localDatasetsNamespace = "local://datasets"
	localSourcesNamespace  = "local://sources"

	// datasetsCleanupTimeout bounds the removal of the per-run cloud prefix.
	datasetsCleanupTimeout = 5 * time.Minute
)

// datasetsBlockstore is the blockstore configuration of one datasets test run.
type datasetsBlockstore struct {
	// Type is the blockstore type the throw-away server runs on (block.BlockstoreType*).
	Type string
	// BlockstoreSection is the "blockstore:" section of the generated lakefs.yaml. A cloud
	// run emits nothing here: the server reads its whole blockstore configuration (type,
	// credentials, region, endpoint) from the ambient LAKEFS_* environment, exactly like any
	// other lakeFS deployment. Emitting a partial blockstore block would shadow the nested
	// defaults in viper.
	BlockstoreSection string
	// StorageNamespacePrefix is datasets.storage_namespace_prefix: where dataset repositories
	// are created.
	StorageNamespacePrefix string
	// SourcesPrefix is the parent namespace of the source repositories.
	SourcesPrefix string
}

// newDatasetsBlockstore returns the blockstore configuration for the suite. It uses the ambient
// cloud blockstore when the environment supplies one and otherwise falls back to the local
// blockstore in dir, which is today's behaviour.
func newDatasetsBlockstore(t *testing.T, dir string) *datasetsBlockstore {
	t.Helper()
	if bs, ok := ambientDatasetsBlockstore(t); ok {
		t.Logf("datasets: running on the ambient %s blockstore under %s (sources under %s)",
			bs.Type, bs.StorageNamespacePrefix, bs.SourcesPrefix)
		return bs
	}

	root := filepath.Join(dir, "data")
	require.NoError(t, os.MkdirAll(root, 0o755), "create the local blockstore directory")
	return &datasetsBlockstore{
		Type:                   block.BlockstoreTypeLocal,
		BlockstoreSection:      fmt.Sprintf("blockstore:\n  type: local\n  local:\n    path: %q\n", root),
		StorageNamespacePrefix: datasetsNamespace(t, localDatasetsNamespace),
		SourcesPrefix:          datasetsNamespace(t, localSourcesNamespace),
	}
}

// ambientDatasetsBlockstore reads the ambient blockstore configuration using the same sources
// the rest of esti uses: LAKEFS_BLOCKSTORE_TYPE for the adapter and the esti storage_namespace
// (ESTI_STORAGE_NAMESPACE or esti.yaml) for the prefix the suite may allocate under.
//
// It reports ok=false when no cloud blockstore is configured, which is the signal to keep the
// self-contained local run. A cloud type with a namespace that addresses a different blockstore
// is an operator error: the suite skips with an explanation instead of writing to the wrong
// place.
func ambientDatasetsBlockstore(t *testing.T) (*datasetsBlockstore, bool) {
	t.Helper()
	blockstoreType := strings.ToLower(os.Getenv("LAKEFS_BLOCKSTORE_TYPE"))
	switch blockstoreType {
	case "", block.BlockstoreTypeLocal, block.BlockstoreTypeMem, block.BlockstoreTypeTransient:
		return nil, false
	}

	namespace := viper.GetString(ViperStorageNamespaceKey)
	if namespace == "" {
		t.Skipf("LAKEFS_BLOCKSTORE_TYPE=%s is set but the esti storage_namespace is empty; "+
			"set ESTI_STORAGE_NAMESPACE to a prefix the datasets suite may allocate under", blockstoreType)
	}
	// The rest of esti reads its own blockstore type from ESTI_BLOCKSTORE_TYPE/esti.yaml. If it
	// explicitly names a different backend than the server will use, the run would mix two
	// blockstores: skip instead.
	if estiType, ok := estiConfiguredValue(ViperBlockstoreType); ok && strings.ToLower(estiType) != blockstoreType {
		t.Skipf("the ambient esti blockstore_type %q does not match LAKEFS_BLOCKSTORE_TYPE=%q", estiType, blockstoreType)
	}
	qk, err := block.DefaultResolveNamespace(namespace, "", block.IdentifierTypeRelative)
	if err != nil {
		t.Skipf("the esti storage_namespace %q cannot be resolved by the blockstore package: %v", namespace, err)
	}
	if got := qk.GetStorageType().BlockstoreType(); got != blockstoreType {
		t.Skipf("the esti storage_namespace %q addresses a %s blockstore, but LAKEFS_BLOCKSTORE_TYPE=%s",
			namespace, got, blockstoreType)
	}

	// Allocate one prefix per run and keep datasets and sources below it (§5.2 rule 4: both must
	// live on the same storage). A single prefix makes the cleanup a single recursive delete.
	runPrefix := datasetsNamespace(t, namespace, "datasets-"+uniqueSuffix())
	bs := &datasetsBlockstore{
		Type:                   blockstoreType,
		BlockstoreSection:      "",
		StorageNamespacePrefix: datasetsNamespace(t, runPrefix, "datasets"),
		SourcesPrefix:          datasetsNamespace(t, runPrefix, "sources"),
	}

	// Cleanups are LIFO: this one is registered before the server, so the server is stopped
	// before the prefix is removed. t.Context() is already cancelled at that point, hence the
	// fresh context.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), datasetsCleanupTimeout)
		defer cancel()
		if err := deleteDatasetsNamespace(ctx, blockstoreType, runPrefix); err != nil {
			t.Errorf("delete the datasets run prefix %s: %v", runPrefix, err)
		}
	})
	return bs, true
}

// estiConfiguredValue returns the value of an esti configuration key only when the surrounding
// environment explicitly supplied it (through the ESTI_ environment variable or esti.yaml),
// never a viper default. That is what separates "the ambient environment configured a
// blockstore" from the defaults testutil.SetupTestingEnv installs for every run.
func estiConfiguredValue(key string) (string, bool) {
	envKey := "ESTI_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	if value := os.Getenv(envKey); value != "" {
		return value, true
	}
	if viper.InConfig(key) {
		return viper.GetString(key), true
	}
	return "", false
}

// datasetsNamespace resolves elems under base with the blockstore's own namespace rules, so the
// result uses the right form for the configured adapter (local://, s3://, gs://, https:// for
// Azure) instead of being assembled by the caller.
func datasetsNamespace(t *testing.T, base string, elems ...string) string {
	t.Helper()
	key := path.Join(elems...)
	qk, err := block.DefaultResolveNamespace(base, key, block.IdentifierTypeRelative)
	require.NoErrorf(t, err, "resolve %q under the blockstore namespace %q", key, base)
	return qk.Format()
}

// deleteDatasetsNamespace removes everything below the per-run prefix of the configured cloud
// blockstore. lakeFS never deletes physical objects by itself, so the suite does it with the
// blockstore's own SDK, using the credentials the ambient environment supplies to the server.
func deleteDatasetsNamespace(ctx context.Context, blockstoreType, namespace string) error {
	u, err := url.Parse(namespace)
	if err != nil {
		return fmt.Errorf("parse namespace %q: %w", namespace, err)
	}
	switch blockstoreType {
	case block.BlockstoreTypeS3:
		return deleteS3Prefix(ctx, u)
	case block.BlockstoreTypeGS:
		return deleteGSPrefix(ctx, u)
	case block.BlockstoreTypeAzure:
		return deleteAzurePrefix(ctx, u)
	default:
		return fmt.Errorf("no prefix cleanup for blockstore type %q", blockstoreType)
	}
}

func deleteS3Prefix(ctx context.Context, u *url.URL) error {
	opts := make([]func(*awsconfig.LoadOptions) error, 0, 2)
	if accessKey := os.Getenv("LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID"); accessKey != "" {
		secretKey := os.Getenv("LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY")
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	if os.Getenv("AWS_REGION") == "" && os.Getenv("AWS_DEFAULT_REGION") == "" {
		opts = append(opts, awsconfig.WithRegion("us-east-1"))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("load the AWS configuration: %w", err)
	}

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		// Mirror the S3-compatible endpoint settings the server uses, so the cleanup also works
		// against MinIO and other S3-compatible stores, not only against AWS itself.
		if endpoint := os.Getenv("LAKEFS_BLOCKSTORE_S3_ENDPOINT"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		if strings.EqualFold(os.Getenv("LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE"), "true") {
			o.UsePathStyle = true
		}
	})
	bucket := u.Host
	prefix := strings.TrimPrefix(u.Path, "/")
	var token *string
	for {
		page, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}
		if len(page.Contents) > 0 {
			objects := make([]awss3types.ObjectIdentifier, 0, len(page.Contents))
			for _, obj := range page.Contents {
				objects = append(objects, awss3types.ObjectIdentifier{Key: obj.Key})
			}
			_, err = client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &awss3types.Delete{Objects: objects, Quiet: aws.Bool(true)},
			})
			if err != nil {
				return fmt.Errorf("delete objects under s3://%s/%s: %w", bucket, prefix, err)
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return nil
		}
		token = page.NextContinuationToken
	}
}

func deleteGSPrefix(ctx context.Context, u *url.URL) error {
	var opts []option.ClientOption
	if credentialsJSON := os.Getenv("LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON"); credentialsJSON != "" {
		opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(credentialsJSON)))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("create the GCS client: %w", err)
	}
	defer func() { _ = client.Close() }()

	bucket := client.Bucket(u.Host)
	prefix := strings.TrimPrefix(u.Path, "/")
	objects := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := objects.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("list gs://%s/%s: %w", u.Host, prefix, err)
		}
		if err := bucket.Object(attrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("delete gs://%s/%s: %w", u.Host, attrs.Name, err)
		}
	}
}

func deleteAzurePrefix(ctx context.Context, u *url.URL) error {
	// An Azure namespace is https://<account>.blob.core.windows.net/<container>/<prefix>.
	trimmed := strings.TrimPrefix(u.Path, "/")
	containerName, prefix, _ := strings.Cut(trimmed, "/")
	if containerName == "" {
		return fmt.Errorf("azure namespace %q has no container", u.String())
	}
	account := strings.SplitN(u.Host, ".", 2)[0]
	accessKey := os.Getenv("LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY")
	if accessKey == "" {
		return fmt.Errorf("azure prefix cleanup needs LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY")
	}
	cred, err := azblob.NewSharedKeyCredential(account, accessKey)
	if err != nil {
		return fmt.Errorf("create the Azure shared key credential: %w", err)
	}
	client, err := service.NewClientWithSharedKeyCredential(u.Scheme+"://"+u.Host, cred, nil)
	if err != nil {
		return fmt.Errorf("create the Azure blob service client: %w", err)
	}

	containerClient := client.NewContainerClient(containerName)
	pager := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list azure://%s/%s: %w", containerName, prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			if _, err := containerClient.NewBlobClient(*item.Name).Delete(ctx, nil); err != nil {
				return fmt.Errorf("delete azure://%s/%s: %w", containerName, *item.Name, err)
			}
		}
	}
	return nil
}
