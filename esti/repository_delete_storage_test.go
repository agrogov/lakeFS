// Package esti holds the lakeFS system tests.
//
// repository_delete_storage_test.go is the acceptance suite of repository deletion with storage
// (REPO_DELETE_STORAGE_SPEC.md A1, A2, A5; REPO_DELETE_STORAGE_PLAN.md task D8). It runs in the
// existing blockstore matrix of .github/workflows/esti.yaml, on S3, GCS and Azure, and reads the
// namespace through the provider's own client - not through lakeFS - because the claim under test
// is about the physical objects:
//
//	delete with storage            a repository with committed, staged and uncommitted data and a
//	                               sibling object at "<namespace>-metadata/keep" is deleted with
//	                               delete_storage=true; the status is polled to done; the namespace
//	                               lists empty and both the sibling and the parent-prefix object
//	                               survive (A1, A2).
//	re-create on the namespace     a new repository on the deleted namespace is created (A1, G5).
//	refusals                       a root namespace, a namespace shared with another repository and
//	                               a duplicate request answer their status and code, name the code in
//	                               the message, and change nothing (A5, R9-R11).
//
// The provider client is built from the same settings the server uses (LAKEFS_BLOCKSTORE_*), which
// is what a local run of the matrix exports; when the test process has no credentials for the
// backend - the GCS job of CI does not pass its service account to the esti container - the
// provider-level assertions are logged and skipped, and the API-level ones still run. That is the
// GCS job of CI, whose compose file passes the service account to the lakefs service only; the local
// run of the matrix covers the provider listing on fake-gcs through STORAGE_EMULATOR_HOST, which the
// GCS client honours.
package esti

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	// repositoryDeletionPollInterval is how often the suite asks for the R14 status. The worker runs
	// every repository_deletion.storage.interval (30s by default) and then waits out the catalog's
	// repository cache in its settle step, so a couple of seconds between polls is plenty.
	repositoryDeletionPollInterval = 2 * time.Second

	// repositoryDeletionTimeout bounds one poll-to-done wait: a tick, the settle wait, and a few
	// pages for a namespace of this size.
	repositoryDeletionTimeout = 5 * time.Minute

	// deleteStorageSiblingKey and deleteStorageParentKey are the objects the case writes outside
	// the namespace to prove the delete prefix ends on a path boundary (A2).
	deleteStorageSiblingKey = "keep"
	deleteStorageParentKey  = "parent-keep"
)

// TestRepositoryDeleteStorage is A1 and A2 on the matrix backend: a repository whose namespace holds
// committed, staged and uncommitted objects is deleted with delete_storage=true, the namespace is
// emptied through the provider client, the sibling and parent-prefix objects survive, and the
// namespace is reusable.
func TestRepositoryDeleteStorage(t *testing.T) {
	RequireBlockstoreType(t, block.BlockstoreTypeS3, block.BlockstoreTypeGS, block.BlockstoreTypeAzure)
	ctx := t.Context()

	store, storeErr := newProviderNamespaceStore(ctx, t)
	if storeErr != nil {
		t.Logf("provider-level namespace assertions are skipped: %v", storeErr)
	}

	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	createRepository(ctx, t, repoName, storage, false)
	t.Cleanup(func() { deleteRepositoryMetadataOnly(t, repoName) })

	// Committed data: one object on main, then a commit. Staged data: an object uploaded to main
	// after the commit. Uncommitted data: an object on a second branch, never committed. All three
	// live under the namespace the worker deletes (R12).
	_, _ = UploadFileAndReport(ctx, repoName, mainBranch, "committed.txt", "committed before the deletion", false, nil)
	commitResp, err := client.CommitWithResponse(ctx, repoName, mainBranch, &apigen.CommitParams{},
		apigen.CommitJSONRequestBody{Message: "data before the storage deletion"})
	require.NoError(t, err, "commit the source object")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode(), "commit: %s", commitResp.Body)

	_, _ = UploadFileAndReport(ctx, repoName, mainBranch, "staged.txt", "staged after the commit", false, nil)
	featureBranch := "delete-storage-feature"
	branchResp, err := client.CreateBranchWithResponse(ctx, repoName, apigen.CreateBranchJSONRequestBody{
		Name:   featureBranch,
		Source: mainBranch,
	})
	require.NoError(t, err, "create branch %s", featureBranch)
	require.Equal(t, http.StatusCreated, branchResp.StatusCode(), "create branch %s: %s", featureBranch, branchResp.Body)
	_, _ = UploadFileAndReport(ctx, repoName, featureBranch, "uncommitted.txt", "never committed", false, nil)

	siblingNamespace := strings.TrimRight(storage, "/") + "-metadata"
	siblingObject := siblingNamespace + "/" + deleteStorageSiblingKey
	parentNamespace := parentStorageNamespace(t, storage)
	parentObject := strings.TrimRight(parentNamespace, "/") + "/" + deleteStorageParentKey
	if store != nil {
		require.NoError(t, store.put(ctx, siblingObject, []byte("sibling data")), "put the sibling object")
		require.NoError(t, store.put(ctx, parentObject, []byte("parent data")), "put the parent-prefix object")
		t.Cleanup(func() {
			_ = store.remove(context.Background(), siblingObject)
			_ = store.remove(context.Background(), parentObject)
		})
	}

	deleteRepositoryWithStorage(ctx, t, client, repoName)

	// R3: the metadata is deleted inline. The record is gone at once, but a replica's repository
	// cache can keep answering for the cache's expiry (which is why the worker has a settle step),
	// so the unreadability is asserted once the cache has had time to drop it.
	require.Eventuallyf(t, func() bool {
		getResp, err := client.GetRepositoryWithResponse(ctx, repoName)
		return err == nil && getResp.StatusCode() == http.StatusNotFound
	}, 30*time.Second, 500*time.Millisecond,
		"the repository must be unreadable after the accepted deletion")

	deletion := waitForRepositoryDeletion(ctx, t, client, repoName)
	require.Equal(t, "done", deletion.State, "the deletion must finish")
	require.NotZero(t, deletion.ObjectsDeleted, "the deletion must have deleted the namespace's objects")

	if store != nil {
		require.Emptyf(t, listProviderObjects(t, ctx, store, storage),
			"the namespace must be empty after the deletion; sibling %s and parent object %s must survive",
			siblingObject, parentObject)
		require.Containsf(t, listProviderObjects(t, ctx, store, siblingNamespace), deleteStorageSiblingKey,
			"A2: the sibling namespace must keep its object")
		require.Truef(t, providerObjectExists(t, ctx, store, parentObject),
			"A2: the parent-prefix object %s must survive", parentObject)
	}

	// A1, G5: the namespace can be reused once the deletion is done.
	recreatedName := GenerateUniqueRepositoryName()
	createRepository(ctx, t, recreatedName, storage, false)
	t.Cleanup(func() { deleteRepositoryMetadataOnly(t, recreatedName) })
	recreated, err := client.GetRepositoryWithResponse(ctx, recreatedName)
	require.NoError(t, err, "get the re-created repository")
	require.Equal(t, http.StatusOK, recreated.StatusCode(), "the re-created repository must be readable: %s", recreated.Body)
	require.Equal(t, storage, recreated.JSON200.StorageNamespace, "the re-created repository keeps the namespace")
}

// TestRepositoryDeleteStorageRefusals is A5 for the refusals of the spec that a system test can
// produce on a live installation: R9 (a root namespace), R10 (a namespace shared with another
// repository) and R11 (a duplicate request). Every case asserts that nothing changed; the
// create-time conflict of R16 is a catalog behaviour whose cache makes it unsuitable for a live
// single-replica trigger, so it is covered by the catalog and controller tests.
func TestRepositoryDeleteStorageRefusals(t *testing.T) {
	RequireBlockstoreType(t, block.BlockstoreTypeS3, block.BlockstoreTypeGS, block.BlockstoreTypeAzure)
	ctx := t.Context()

	store, storeErr := newProviderNamespaceStore(ctx, t)
	if storeErr != nil {
		t.Logf("provider-level namespace assertions are skipped: %v", storeErr)
	}

	t.Run("root namespace", func(t *testing.T) {
		root := storageRootNamespace(t, viper.GetString(ViperStorageNamespaceKey))
		repoName := GenerateUniqueRepositoryName()

		// A bare repository writes no marker: the case can create one on the storage root without
		// writing "_lakefs/dummy" to a bucket or container that every run shares.
		resp, err := client.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{Bare: swag.Bool(true)},
			apigen.CreateRepositoryJSONRequestBody{
				Name:             repoName,
				StorageNamespace: root,
				DefaultBranch:    apiutil.Ptr(mainBranch),
			})
		require.NoError(t, err, "create bare repository %s on the storage root", repoName)
		require.Equal(t, http.StatusCreated, resp.StatusCode(), "create bare repository %s: %s", repoName, resp.Body)
		t.Cleanup(func() { deleteRepositoryMetadataOnly(t, repoName) })

		reason, status := deleteRepositoryWithStorageError(ctx, t, client, repoName)
		require.Equal(t, http.StatusBadRequest, status, "R9: a root namespace is a bad request")
		require.Contains(t, reason, "namespace_is_root", "R9: the message must say why")

		// Nothing changed: the repository still exists.
		stillThere, err := client.GetRepositoryWithResponse(ctx, repoName)
		require.NoError(t, err, "get the repository after the refusal")
		require.Equal(t, http.StatusOK, stillThere.StatusCode(), "R9 must not delete the repository: %s", stillThere.Body)
	})

	t.Run("shared namespace", func(t *testing.T) {
		ownerName := GenerateUniqueRepositoryName()
		ownerStorage := GenerateUniqueStorageNamespace(ownerName)
		createRepository(ctx, t, ownerName, ownerStorage, false)
		t.Cleanup(func() { deleteRepositoryMetadataOnly(t, ownerName) })

		// The second repository's namespace is a child of the first's: R10's overlap is decided on
		// path segments, so the child owns part of the owner's namespace.
		innerName := GenerateUniqueRepositoryName()
		innerStorage := strings.TrimRight(ownerStorage, "/") + "/inner"
		createRepository(ctx, t, innerName, innerStorage, false)
		t.Cleanup(func() { deleteRepositoryMetadataOnly(t, innerName) })

		_, _ = UploadFileAndReport(ctx, ownerName, mainBranch, "owner.txt", "owned by the outer repository", false, nil)
		commitResp, err := client.CommitWithResponse(ctx, ownerName, mainBranch, &apigen.CommitParams{},
			apigen.CommitJSONRequestBody{Message: "the object an overlapping deletion must not touch"})
		require.NoError(t, err, "commit the owner's object")
		require.Equal(t, http.StatusCreated, commitResp.StatusCode(), "commit: %s", commitResp.Body)

		reason, status := deleteRepositoryWithStorageError(ctx, t, client, ownerName)
		require.Equal(t, http.StatusConflict, status, "R10: a shared namespace is a conflict")
		require.Contains(t, reason, "namespace_shared", "R10: the message must say why")
		require.Contains(t, reason, innerName, "R10: the message must name the other repository")

		// Nothing changed: the repository and the deletion partition are untouched, and the object
		// is still in the namespace.
		stillThere, err := client.GetRepositoryWithResponse(ctx, ownerName)
		require.NoError(t, err, "get the repository after the refusal")
		require.Equal(t, http.StatusOK, stillThere.StatusCode(), "R10 must not delete the repository: %s", stillThere.Body)
		record, err := client.GetRepositoryDeletionWithResponse(ctx, ownerName)
		require.NoError(t, err, "get the deletion record after the refusal")
		require.Equal(t, http.StatusNotFound, record.StatusCode(), "R10 must not write a deletion record: %s", record.Body)
		if store != nil {
			require.NotEmpty(t, listProviderObjects(t, ctx, store, ownerStorage), "R10 must not delete objects")
		}
	})

	t.Run("duplicate request", func(t *testing.T) {
		repoName := GenerateUniqueRepositoryName()
		storage := GenerateUniqueStorageNamespace(repoName)
		createRepository(ctx, t, repoName, storage, false)
		t.Cleanup(func() { deleteRepositoryMetadataOnly(t, repoName) })
		_, _ = UploadFileAndReport(ctx, repoName, mainBranch, "duplicate.txt", "a namespace being deleted", false, nil)

		deleteRepositoryWithStorage(ctx, t, client, repoName)

		// R11: the same request while the deletion is pending. The record is written by the first
		// request and the worker has not finished it yet (the worker's settle step gives the
		// duplicate request a window of several seconds).
		reason, status := deleteRepositoryWithStorageError(ctx, t, client, repoName)
		require.Equal(t, http.StatusConflict, status, "R11: a pending deletion is a conflict")
		require.Contains(t, reason, "deletion_in_progress", "R11: the message must say why")

		// The first deletion still finishes once.
		deletion := waitForRepositoryDeletion(ctx, t, client, repoName)
		require.Equal(t, "done", deletion.State, "the first request's deletion must finish")
	})
}

// deleteRepositoryWithStorage sends delete_storage=true and requires 202 with a record.
func deleteRepositoryWithStorage(ctx context.Context, t *testing.T, cl apigen.ClientWithResponsesInterface, repo string) *apigen.RepositoryDeletion {
	t.Helper()
	resp, err := cl.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{DeleteStorage: swag.Bool(true)})
	require.NoErrorf(t, err, "delete repository %s with its storage", repo)
	require.Equalf(t, http.StatusAccepted, resp.StatusCode(),
		"delete repository %s with its storage must answer 202: %s", repo, resp.Body)
	require.NotNilf(t, resp.JSON202, "delete repository %s must return the deletion record: %s", repo, resp.Body)
	require.Equal(t, repo, resp.JSON202.Repository, "the record names the repository")
	require.NotEqualf(t, "done", resp.JSON202.State, "the request must return before the objects are deleted")
	return resp.JSON202
}

// deleteRepositoryWithStorageError sends delete_storage=true and returns the reason and status of a
// refusal. A 2xx answer fails the test: the caller expects the request to be refused.
func deleteRepositoryWithStorageError(ctx context.Context, t *testing.T, cl apigen.ClientWithResponsesInterface, repo string) (string, int) {
	t.Helper()
	resp, err := cl.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{DeleteStorage: swag.Bool(true)})
	require.NoErrorf(t, err, "delete repository %s with its storage", repo)
	require.GreaterOrEqualf(t, resp.StatusCode(), minHTTPErrorStatusCode,
		"the request must be refused, got %s: %s", resp.Status(), resp.Body)
	reason := string(resp.Body)
	if resp.JSON400 != nil {
		reason = resp.JSON400.Message
	}
	if resp.JSON409 != nil {
		reason = resp.JSON409.Message
	}
	return reason, resp.StatusCode()
}

// waitForRepositoryDeletion polls the R14 status endpoint until the deletion is done.
func waitForRepositoryDeletion(ctx context.Context, t *testing.T, cl apigen.ClientWithResponsesInterface, repo string) *apigen.RepositoryDeletion {
	t.Helper()
	deadline := time.Now().Add(repositoryDeletionTimeout)
	var last string
	for {
		resp, err := cl.GetRepositoryDeletionWithResponse(ctx, repo)
		require.NoErrorf(t, err, "get the deletion status of %s", repo)
		switch resp.StatusCode() {
		case http.StatusOK:
			require.NotNilf(t, resp.JSON200, "get the deletion status of %s: %s", repo, resp.Body)
			last = resp.JSON200.State
			switch resp.JSON200.State {
			case "done":
				return resp.JSON200
			case "failed":
				t.Fatalf("the storage deletion of %s failed after %d attempts: %s",
					repo, resp.JSON200.Attempts, swag.StringValue(resp.JSON200.LastError))
			}
		case http.StatusNotFound:
			// The record is written by the request that accepted the deletion, so a 404 is only the
			// first moments of a resume or a race with another replica. Keep polling.
			last = "not found"
		default:
			t.Fatalf("get the deletion status of %s: %s: %s", repo, resp.Status(), resp.Body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the storage deletion of %s did not finish within %s; last state: %s",
				repo, repositoryDeletionTimeout, last)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the storage deletion of %s was interrupted: %v", repo, ctx.Err())
		case <-time.After(repositoryDeletionPollInterval):
		}
	}
}

// deleteRepositoryMetadataOnly removes a repository without its storage. The refusals leave
// repositories behind and the suite cleans them up; the namespace is unique per run, so the objects
// are left to the run's own cleanup.
func deleteRepositoryMetadataOnly(t *testing.T, repo string) {
	t.Helper()
	ctx := context.Background()
	resp, err := client.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{Force: swag.Bool(true)})
	if err != nil {
		logger.WithError(err).WithField("repo", repo).Error("delete repository during cleanup")
		return
	}
	if resp.StatusCode() != http.StatusNoContent && resp.StatusCode() != http.StatusNotFound {
		logger.WithFields(logging.Fields{"repo": repo, "status": resp.Status()}).Error("delete repository during cleanup")
	}
}

// parentStorageNamespace returns the namespace one path segment above namespace. It is the prefix
// A2 requires to survive: "<parent>/<object>" is outside "<namespace>/".
func parentStorageNamespace(t *testing.T, namespace string) string {
	t.Helper()
	u, err := url.Parse(namespace)
	require.NoErrorf(t, err, "parse the storage namespace %q", namespace)
	u.Path = path.Dir(strings.TrimRight(u.Path, "/"))
	if u.Path == "." || u.Path == "/" {
		u.Path = ""
	}
	return u.String()
}

// storageRootNamespace returns the root of namespace for the configured blockstore: the bucket for
// S3 and GCS, the container for Azure. It is what R9 refuses to delete.
func storageRootNamespace(t *testing.T, namespace string) string {
	t.Helper()
	u, err := url.Parse(namespace)
	require.NoErrorf(t, err, "parse the storage namespace %q", namespace)
	require.NotEmptyf(t, u.Host, "the storage namespace %q has no host", namespace)
	if viper.GetString(config.BlockstoreTypeKey) != block.BlockstoreTypeAzure {
		return u.Scheme + "://" + u.Host
	}
	container, _, _ := strings.Cut(strings.Trim(u.Path, "/"), "/")
	require.NotEmptyf(t, container, "the Azure storage namespace %q names no container", namespace)
	return u.Scheme + "://" + u.Host + "/" + container
}

// ---------------------------------------------------------------------------------------------
// the provider clients
// ---------------------------------------------------------------------------------------------

// providerNamespaceStore is the direct object-store access A1 and A2 need. It writes and removes the
// objects outside the namespace and lists the namespace itself, all through the provider's own
// client, because the claim "the namespace is empty and the sibling is not" is about the physical
// storage, not about lakeFS's view of it.
type providerNamespaceStore interface {
	// put writes content at the object rawURL.
	put(ctx context.Context, rawURL string, content []byte) error
	// list returns the keys under rawURL as a prefix ending on a path boundary, relative to it.
	list(ctx context.Context, rawURL string) ([]string, error)
	// exists reports whether the object rawURL exists.
	exists(ctx context.Context, rawURL string) (bool, error)
	// remove deletes the object rawURL. A missing object is not an error.
	remove(ctx context.Context, rawURL string) error
}

// newProviderNamespaceStore builds the client of the configured blockstore. An error means the test
// process has no settings for it: the caller logs it and keeps the API-level assertions.
func newProviderNamespaceStore(ctx context.Context, t *testing.T) (providerNamespaceStore, error) {
	t.Helper()
	switch blockstoreType := viper.GetString(config.BlockstoreTypeKey); blockstoreType {
	case block.BlockstoreTypeS3:
		return newS3NamespaceStore(ctx)
	case block.BlockstoreTypeGS:
		return newGSNamespaceStore(ctx)
	case block.BlockstoreTypeAzure:
		return newAzureNamespaceStore()
	default:
		return nil, fmt.Errorf("no provider namespace store for blockstore type %q", blockstoreType)
	}
}

// listProviderObjects returns the keys of namespace, relative to it. A key is under the namespace
// only when it shares the namespaces path prefix, so the listing prefix always ends in "/": that is
// what keeps "<namespace>-metadata/keep" out of a listing of "<namespace>" (A2, design section 2.2).
func listProviderObjects(t *testing.T, ctx context.Context, store providerNamespaceStore, namespace string) []string {
	t.Helper()
	keys, err := store.list(ctx, namespace)
	require.NoErrorf(t, err, "list the objects under %s", namespace)
	return keys
}

// providerObjectExists reports whether the object rawURL exists.
func providerObjectExists(t *testing.T, ctx context.Context, store providerNamespaceStore, rawURL string) bool {
	t.Helper()
	exists, err := store.exists(ctx, rawURL)
	require.NoErrorf(t, err, "check the object %s", rawURL)
	return exists
}

// s3NamespaceStore is the S3 client of the test process. It reads the same blockstore settings the
// server does; when they are absent it falls back to the AWS default chain, which is what the CI
// runs of the S3 job provide.
type s3NamespaceStore struct {
	client *awss3.Client
}

func newS3NamespaceStore(ctx context.Context) (*s3NamespaceStore, error) {
	var opts []func(*awsconfig.LoadOptions) error
	// The esti container of CI gets the storage credentials as ESTI_AWS_* (the compose file passes
	// them through); a local run exports the blockstore settings the server uses. The AWS default
	// chain is the fallback, and it is what the workflow's own "Check files in S3 bucket" step uses.
	accessKey := firstNonEmptyEnv("ESTI_AWS_ACCESS_KEY_ID", "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID")
	if accessKey != "" {
		secretKey := firstNonEmptyEnv("ESTI_AWS_SECRET_ACCESS_KEY", "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY")
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	region := firstNonEmptyEnv("LAKEFS_BLOCKSTORE_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-east-1"
	}
	opts = append(opts, awsconfig.WithRegion(region))
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load the AWS configuration: %w", err)
	}
	endpoint := os.Getenv("LAKEFS_BLOCKSTORE_S3_ENDPOINT")
	forcePathStyle := strings.EqualFold(os.Getenv("LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE"), "true")
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = forcePathStyle
	})
	return &s3NamespaceStore{client: client}, nil
}

// splitS3Namespace returns the bucket and the path of an s3:// namespace.
func splitS3Namespace(rawURL string) (string, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("parse the S3 URL %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("the S3 URL %q has no bucket", rawURL)
	}
	return u.Host, strings.Trim(u.Path, "/"), nil
}

func (s *s3NamespaceStore) put(ctx context.Context, rawURL string, content []byte) error {
	bucket, key, err := splitS3Namespace(rawURL)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(content)),
	})
	return err
}

func (s *s3NamespaceStore) list(ctx context.Context, rawURL string) ([]string, error) {
	bucket, prefix, err := splitS3Namespace(rawURL)
	if err != nil {
		return nil, err
	}
	return s.listPrefix(ctx, bucket, strings.TrimRight(prefix, "/")+"/")
}

func (s *s3NamespaceStore) exists(ctx context.Context, rawURL string) (bool, error) {
	bucket, key, err := splitS3Namespace(rawURL)
	if err != nil {
		return false, err
	}
	keys, err := s.listPrefix(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	return len(keys) == 1, nil
}

func (s *s3NamespaceStore) listPrefix(ctx context.Context, bucket, prefix string) ([]string, error) {
	var keys []string
	var token *string
	for {
		page, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, strings.TrimPrefix(aws.ToString(obj.Key), prefix))
		}
		if !aws.ToBool(page.IsTruncated) {
			return keys, nil
		}
		token = page.NextContinuationToken
	}
}

func (s *s3NamespaceStore) remove(ctx context.Context, rawURL string) error {
	bucket, key, err := splitS3Namespace(rawURL)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

// gsNamespaceStore is the GCS client of the test process. STORAGE_EMULATOR_HOST points it at
// fake-gcs-server without credentials, which is how the local run of the matrix works; the CI job
// supplies a service account through LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON when it has one.
type gsNamespaceStore struct {
	client *storage.Client
}

func newGSNamespaceStore(ctx context.Context) (*gsNamespaceStore, error) {
	var opts []option.ClientOption
	switch {
	case os.Getenv("LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON") != "":
		opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount,
			[]byte(os.Getenv("LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON"))))
	case os.Getenv("LAKEFS_BLOCKSTORE_GS_CREDENTIALS_FILE") != "":
		opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount,
			os.Getenv("LAKEFS_BLOCKSTORE_GS_CREDENTIALS_FILE")))
	case os.Getenv("STORAGE_EMULATOR_HOST") != "" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "":
		// The default chain of the client handles both an emulator and an ambient service account.
	default:
		return nil, fmt.Errorf("no GCS credentials: set LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON, " +
			"GOOGLE_APPLICATION_CREDENTIALS or STORAGE_EMULATOR_HOST")
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create the GCS client: %w", err)
	}
	return &gsNamespaceStore{client: client}, nil
}

// splitGSNamespace returns the bucket and the path of a gs:// namespace.
func splitGSNamespace(rawURL string) (string, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("parse the GCS URL %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("the GCS URL %q has no bucket", rawURL)
	}
	return u.Host, strings.Trim(u.Path, "/"), nil
}

func (g *gsNamespaceStore) put(ctx context.Context, rawURL string, content []byte) error {
	bucket, key, err := splitGSNamespace(rawURL)
	if err != nil {
		return err
	}
	writer := g.client.Bucket(bucket).Object(key).NewWriter(ctx)
	if _, err := writer.Write(content); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write gs://%s/%s: %w", bucket, key, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close gs://%s/%s: %w", bucket, key, err)
	}
	return nil
}

func (g *gsNamespaceStore) list(ctx context.Context, rawURL string) ([]string, error) {
	bucket, prefix, err := splitGSNamespace(rawURL)
	if err != nil {
		return nil, err
	}
	return g.listPrefix(ctx, bucket, strings.TrimRight(prefix, "/")+"/")
}

func (g *gsNamespaceStore) exists(ctx context.Context, rawURL string) (bool, error) {
	bucket, key, err := splitGSNamespace(rawURL)
	if err != nil {
		return false, err
	}
	keys, err := g.listPrefix(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	return len(keys) == 1, nil
}

func (g *gsNamespaceStore) listPrefix(ctx context.Context, bucket, prefix string) ([]string, error) {
	objects := g.client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	var keys []string
	for {
		attrs, err := objects.Next()
		if errors.Is(err, iterator.Done) {
			return keys, nil
		}
		if err != nil {
			return nil, fmt.Errorf("list gs://%s/%s: %w", bucket, prefix, err)
		}
		keys = append(keys, strings.TrimPrefix(attrs.Name, prefix))
	}
}

func (g *gsNamespaceStore) remove(ctx context.Context, rawURL string) error {
	bucket, key, err := splitGSNamespace(rawURL)
	if err != nil {
		return err
	}
	err = g.client.Bucket(bucket).Object(key).Delete(ctx)
	if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("delete gs://%s/%s: %w", bucket, key, err)
	}
	return nil
}

// azureNamespaceStore is the Azure Blob client of the test process. On a real account it addresses
// "<account>.<domain>"; ESTI_AZURE_ENDPOINT selects an emulator and the account is then a path
// segment, which is the path style Azurite serves.
type azureNamespaceStore struct {
	client *azblob.Client
}

func newAzureNamespaceStore() (*azureNamespaceStore, error) {
	account := firstNonEmptyEnv("ESTI_AZURE_STORAGE_ACCOUNT", "LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT")
	accessKey := firstNonEmptyEnv("ESTI_AZURE_STORAGE_ACCESS_KEY", "LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY")
	if account == "" || accessKey == "" {
		return nil, fmt.Errorf("no Azure credentials: set ESTI_AZURE_STORAGE_ACCOUNT and ESTI_AZURE_STORAGE_ACCESS_KEY")
	}
	credential, err := azblob.NewSharedKeyCredential(account, accessKey)
	if err != nil {
		return nil, fmt.Errorf("create the Azure shared key credential: %w", err)
	}
	serviceURL := strings.TrimRight(os.Getenv("ESTI_AZURE_ENDPOINT"), "/")
	if serviceURL != "" {
		serviceURL += "/" + account
	} else {
		domain := os.Getenv("LAKEFS_BLOCKSTORE_AZURE_DOMAIN")
		if domain == "" {
			domain = "blob.core.windows.net"
		}
		serviceURL = "https://" + account + "." + domain
	}
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("create the Azure blob client for %q: %w", serviceURL, err)
	}
	return &azureNamespaceStore{client: client}, nil
}

// splitAzureNamespace returns the container and the path of an Azure namespace URL.
func splitAzureNamespace(rawURL string) (string, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("parse the Azure URL %q: %w", rawURL, err)
	}
	container, rest, _ := strings.Cut(strings.Trim(u.Path, "/"), "/")
	if container == "" {
		return "", "", fmt.Errorf("the Azure URL %q names no container", rawURL)
	}
	return container, rest, nil
}

func (a *azureNamespaceStore) put(ctx context.Context, rawURL string, content []byte) error {
	container, key, err := splitAzureNamespace(rawURL)
	if err != nil {
		return err
	}
	_, err = a.client.UploadBuffer(ctx, container, key, content, nil)
	if err != nil {
		return fmt.Errorf("upload azure://%s/%s: %w", container, key, err)
	}
	return nil
}

func (a *azureNamespaceStore) list(ctx context.Context, rawURL string) ([]string, error) {
	container, prefix, err := splitAzureNamespace(rawURL)
	if err != nil {
		return nil, err
	}
	return a.listPrefix(ctx, container, strings.TrimRight(prefix, "/")+"/")
}

func (a *azureNamespaceStore) exists(ctx context.Context, rawURL string) (bool, error) {
	container, key, err := splitAzureNamespace(rawURL)
	if err != nil {
		return false, err
	}
	keys, err := a.listPrefix(ctx, container, key)
	if err != nil {
		return false, err
	}
	return len(keys) == 1, nil
}

func (a *azureNamespaceStore) listPrefix(ctx context.Context, container, prefix string) ([]string, error) {
	containerClient := a.client.ServiceClient().NewContainerClient(container)
	pager := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{Prefix: &prefix})
	var keys []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list azure://%s/%s: %w", container, prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			keys = append(keys, strings.TrimPrefix(*item.Name, prefix))
		}
	}
	return keys, nil
}

func (a *azureNamespaceStore) remove(ctx context.Context, rawURL string) error {
	container, key, err := splitAzureNamespace(rawURL)
	if err != nil {
		return err
	}
	_, err = a.client.DeleteBlob(ctx, container, key, nil)
	return err
}

// firstNonEmptyEnv returns the first non-empty environment variable of names.
func firstNonEmptyEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
