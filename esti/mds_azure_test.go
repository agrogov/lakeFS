// Package esti holds the lakeFS system tests.
//
// mds_azure_test.go is the Metadata Search acceptance test on an Azure blockstore
// (METADATA_SEARCH_AZURE.md §5 task AZ5, goal G5): the end-to-end proof that the catalog, the
// indexer and two published query engines work against Azure Blob Storage, with Azurite as the
// emulator and with no Azure credential in any configuration file.
//
// # What runs
//
// The environment is the MDS suite's own (mds_test.go): a lakefs-sso server on a throw-away local
// KV store, the ACL auth service, the Metadata Search bootstrap of §8 and `lakefs-sso mds run` as a
// child process. Only the blockstore is different, and it is the Azure one:
//
//   - the server runs with blockstore.type azure, blockstore.azure.domain azurite.test,
//     blockstore.azure.storage_account/storage_access_key and blockstore.azure.test_endpoint_url
//     pointed at the emulator. test_endpoint_url carries the account in its path
//     (http://127.0.0.1:10000/<account>) because the emulator is the only Azure endpoint this
//     suite has and the server is a host process: it cannot resolve <account>.azurite.test, and
//     Azurite serves the path-style URL the container's own connection string uses.
//   - the repository's storage namespace is https://<account>.azurite.test/<container>/esti-<run>,
//     i.e. a prefix inside a container, the shape a real installation has. That is what makes the
//     catalog hand out the canonical abfs[s]://<container>@<account>.<dfs domain>/... locations of
//     METADATA_SEARCH_AZURE D1 and keep every physical address in the adapter's own http(s) form
//     (D2) - the two spellings AZ4 pins on the table package, here read back by real clients.
//   - the indexer is configured with LAKEFS_METADATA_SEARCH_AZURE_* environment variables ONLY
//     (goal G4): the generated lakefs.yaml carries no metadata_search.storage and no
//     metadata_search.azure key, and the case asserts that. The variables are the ones D3 defines,
//     and the endpoint is the emulator's host:port, which iceberg-go turns into a path-style
//     service URL (io/gocloud/azure: a storage domain that starts with 127.0.0.1 is an emulator
//     endpoint), so the indexer needs no DNS either.
//
// # The cases (§5 AZ5)
//
//	upload and commit                     three orders/ objects, one commit; the row set, the
//	                                      commit_id of every row and the lakefs.mds.source-commit
//	                                      property of R.<ref>.system.object_metadata are read back
//	                                      by PyIceberg with adls.connection-string and by DuckDB,
//	                                      on both catalog mounts of docs/metadata-search.md; the
//	                                      table's metadata location is the abfs:// form of D1
//	container root is refused             a repository whose namespace is the container root has no
//	                                      derivable companion namespace, so the indexer reports the
//	                                      refusal naming the container and creates no R-metadata (D4)
//	guard group cannot create on it       the R3S-3 spelling case of the S3 suite with the Azure
//	                                      pair: the deny names the companion namespace with http and
//	                                      with https, bare and not, and refuses all four (D4)
//
// Trino and Spark are not run here: Trino's native Azure file system has no emulator mode, so their
// Azure verification is AZ7 (a real account). PyIceberg and DuckDB are the two engines whose FileIO
// an emulator can carry.
//
// # Running it
//
// Azurite is the `azurite` service of esti/ops/docker-compose-mds.yaml, which documents the same
// run; the suite creates the container and removes its own prefixes when it finishes, so one
// long-lived emulator is enough for repeated runs:
//
//	docker compose -f esti/ops/docker-compose-mds.yaml up -d azurite
//	ESTI_AZURE_STORAGE_ACCOUNT=acct \
//	ESTI_AZURE_STORAGE_ACCESS_KEY=a2V5MQ== \
//	go test -count=1 -v ./esti -run 'TestMetadataSearchAzure' \
//	  --args --system-tests --use-local-credentials
//
// The suite skips - it never fails - when the Azure settings are absent, so the S3 run of the same
// package is unaffected; the emulator having been configured but not started is a failed case.
//
// Clean-room note: written from METADATA_SEARCH_AZURE.md D1-D4 and §5 AZ5, the Azurite
// documentation and the Azure Storage Blob documentation. No lakeFS Enterprise code and no upstream
// lakeFS >= 1.87 (BSL 1.1) code was used.
package esti

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// mdsAzureAccountEnv, mdsAzureAccessKeyEnv and mdsAzureEndpointEnv are the emulator's account
	// and endpoint: the ones esti/ops/docker-compose-mds.yaml's azurite service is started with.
	// The suite reads them and skips when they are absent, like the S3 suite reads LAKEFS_*.
	mdsAzureAccountEnv   = "ESTI_AZURE_STORAGE_ACCOUNT"
	mdsAzureAccessKeyEnv = "ESTI_AZURE_STORAGE_ACCESS_KEY"
	mdsAzureEndpointEnv  = "ESTI_AZURE_ENDPOINT"

	// mdsAzureDefaultEndpoint is where the compose file publishes Azurite's blob service.
	mdsAzureDefaultEndpoint = "http://127.0.0.1:10000"

	// mdsAzureContainer is the container the run's repositories live in. The design's D1 table
	// writes it as "c" schematically; Azurite enforces Azure's three-character minimum for a
	// container name, the same reason the table package's Azure test uses a longer one.
	mdsAzureContainer = "esti"

	// mdsAzureOrdersPrefix is the prefix of the three objects of the happy path.
	mdsAzureOrdersPrefix = "orders/"
)

// mdsAzureEnv is the MDS environment of an Azure run: the shared harness plus the emulator's
// account, its host-side endpoint and the container the run's repositories live in.
type mdsAzureEnv struct {
	*mdsEnv

	// account and accessKey are the emulator's shared key, as AZURITE_ACCOUNTS gives them to
	// Azurite. They configure the server's blockstore (blockstore.azure.*), the indexer's FileIO
	// (LAKEFS_METADATA_SEARCH_AZURE_*, D3) and the clients (adls.connection-string).
	account   string
	accessKey string

	// endpoint is the blob service on the host, http://127.0.0.1:<port>, and endpointHostPort is
	// its host:port: what blockstore.azure.test_endpoint_url and adls.endpoint take (the adapter
	// and iceberg-go both append the container and the account themselves).
	endpoint         string
	endpointHostPort string
	endpointProtocol string

	// container is the container the run's namespaces are rooted in.
	container string
}

// newMDSAzureEnv starts the MDS environment on the emulator and returns it. It skips when the
// Azure settings are absent, so this file's cases do not need a special CI selector.
func newMDSAzureEnv(t *testing.T) *mdsAzureEnv {
	t.Helper()
	account := os.Getenv(mdsAzureAccountEnv)
	accessKey := os.Getenv(mdsAzureAccessKeyEnv)
	if account == "" || accessKey == "" {
		t.Skipf("the Metadata Search Azure acceptance test needs the Azurite emulator of "+
			"esti/ops/docker-compose-mds.yaml: set %s and %s", mdsAzureAccountEnv, mdsAzureAccessKeyEnv)
	}
	endpoint := os.Getenv(mdsAzureEndpointEnv)
	if endpoint == "" {
		endpoint = mdsAzureDefaultEndpoint
	}
	parsed, err := url.Parse(endpoint)
	require.NoErrorf(t, err, "parse the Azure endpoint %q", endpoint)
	require.Containsf(t, []string{"http", "https"}, parsed.Scheme,
		"the Azure endpoint %q must have an http or https scheme", endpoint)
	require.NotEmptyf(t, parsed.Host, "the Azure endpoint %q has no host", endpoint)

	env := newMDSEnvSkeleton(t, "esti-"+icebergUniqueSuffix())
	azureEnv := &mdsAzureEnv{
		mdsEnv:           env,
		account:          account,
		accessKey:        accessKey,
		endpoint:         strings.TrimRight(endpoint, "/"),
		endpointHostPort: parsed.Host,
		endpointProtocol: parsed.Scheme,
		container:        mdsAzureContainer,
	}

	// The repository's namespace is a prefix inside the container, exactly as on S3 it is a prefix
	// inside the bucket; the https spelling is the one a real installation stores. The companion
	// repository the indexer derives from it is a sibling prefix in the same container (D4).
	env.storageNamespace = fmt.Sprintf("https://%s.%s/%s/%s",
		account, azure.BlobEndpointTestDomain, mdsAzureContainer, env.repo)
	env.runPrefix = env.repo
	env.blockstoreSection = azureEnv.blockstoreSection()
	env.storageSection = "" // no metadata_search.storage: the indexer reads D3's env vars only
	env.setupStorage = azureEnv.ensureContainer
	env.cleanupStorage = azureEnv.deleteRunPrefix
	env.indexerEnv = azureEnv.indexerEnvironment()

	env.start(t)
	return azureEnv
}

// blockstoreSection is the server's blockstore section: the Azure adapter, the emulator's shared
// key and the path-style test endpoint (blockstore.azure.test_endpoint_url).
func (e *mdsAzureEnv) blockstoreSection() string {
	return fmt.Sprintf(`blockstore:
  type: azure
  azure:
    storage_account: %q
    storage_access_key: %q
    domain: %q
    test_endpoint_url: %q
`, e.account, e.accessKey, azure.BlobEndpointTestDomain, e.accountEndpoint())
}

// accountEndpoint is the emulator's blob service with the account in the path: the path-style URL
// Azurite serves for <endpoint>/<account>/<container>/<blob>. The server appends the container and
// the blob, so it needs no <account>.azurite.test name resolution, which a host process cannot have.
func (e *mdsAzureEnv) accountEndpoint() string {
	return e.endpoint + "/" + e.account
}

// indexerEnvironment is the environment of the indexer process: METADATA_SEARCH_AZURE.md D3's
// variables and nothing else. The configuration file has no Azure key, so the indexer can only work
// if these are read (goal G4).
func (e *mdsAzureEnv) indexerEnvironment() []string {
	return []string{
		"LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCOUNT=" + e.account,
		"LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCESS_KEY=" + e.accessKey,
		"LAKEFS_METADATA_SEARCH_AZURE_ENDPOINT=" + e.endpointHostPort,
		"LAKEFS_METADATA_SEARCH_AZURE_PROTOCOL=" + e.endpointProtocol,
	}
}

// azBlobClient builds the emulator client of the test process itself: the shared key, path-style.
func (e *mdsAzureEnv) azBlobClient(t *testing.T) *azblob.Client {
	t.Helper()
	credential, err := azblob.NewSharedKeyCredential(e.account, e.accessKey)
	require.NoError(t, err, "build the emulator's shared-key credential")
	client, err := azblob.NewClientWithSharedKeyCredential(e.accountEndpoint(), credential, nil)
	require.NoErrorf(t, err, "build the emulator client for %q", e.accountEndpoint())
	return client
}

// ensureContainer creates the container the run's namespaces are rooted in. Azurite is long-lived
// across runs (esti/ops/docker-compose-mds.yaml), so an existing container is the normal case.
func (e *mdsAzureEnv) ensureContainer(t *testing.T) {
	t.Helper()
	client := e.azBlobClient(t)
	_, err := client.CreateContainer(t.Context(), e.container, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		require.NoErrorf(t, err, "create the container %q in the emulator at %s", e.container, e.endpoint)
	}
}

// deleteRunPrefix removes the blobs of this run: the data repository's and the companion
// repository's prefixes are siblings in the container (D4), and lakeFS never deletes physical
// objects itself. The run's names are unique, so a long-lived emulator does not grow a prefix per
// run.
func (e *mdsAzureEnv) deleteRunPrefix(ctx context.Context) error {
	credential, err := azblob.NewSharedKeyCredential(e.account, e.accessKey)
	if err != nil {
		return err
	}
	client, err := azblob.NewClientWithSharedKeyCredential(e.accountEndpoint(), credential, nil)
	if err != nil {
		return err
	}
	container := client.ServiceClient().NewContainerClient(e.container)
	for _, prefix := range []string{e.repo, e.metadataRepo} {
		pager := container.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{Prefix: &prefix})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				if bloberror.HasCode(err, bloberror.ContainerNotFound) {
					return nil
				}
				return err
			}
			for _, blob := range page.Segment.BlobItems {
				if blob.Name == nil {
					continue
				}
				if _, err := container.NewBlobClient(*blob.Name).Delete(ctx, nil); err != nil &&
					!bloberror.HasCode(err, bloberror.BlobNotFound) {
					return fmt.Errorf("delete %q: %w", *blob.Name, err)
				}
			}
		}
	}
	return nil
}

// connectionString is the adls.connection-string the clients read the table with (D5): the
// emulator's account and shared key, its path-style blob endpoint as a container reaches it, and the
// same URL as the DFS endpoint. Azurite has no Data Lake endpoint, and HDFS-style listings are the
// only thing the engines would use it for: the table's files are read through the Blob API, which
// the emulator serves. Without DfsEndpoint the Data Lake client of an abfs:// location would address
// <account>.dfs.core.windows.net and be authenticated by the real cloud instead of the emulator, so
// both endpoints name the one host the run has.
func (e *mdsAzureEnv) connectionString() string {
	blobEndpoint := e.dockerReachableURL(e.accountEndpoint())
	return fmt.Sprintf("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;DfsEndpoint=%s;",
		e.endpointProtocol, e.account, e.accessKey, blobEndpoint, blobEndpoint)
}

// icebergLocationPrefix is the canonical location form the catalog hands out for a table of this
// repository (D1): abfs[s]://<container>@<account>.<dfs domain>/<path>. The namespace is https, so
// the location is abfss://.
func (e *mdsAzureEnv) icebergLocationPrefix() string {
	return fmt.Sprintf("abfss://%s@%s.%s/", e.container, e.account, azure.BlobEndpointTestDomain)
}

// readRows reads the virtual table of R at ref through both engines the emulator can carry, on both
// catalog mounts of docs/metadata-search.md, and requires that they agree.
func (e *mdsAzureEnv) readRows(t *testing.T, ref string) mdsRead {
	t.Helper()
	read := mdsDecode[mdsRead](t, e.runReadStep(t, ref))
	read.requireAgrees(t, ref)
	return read
}

// runReadStep is mdsEnv.runReadStep with the Azure FileIO: the table, the ref, the four catalog
// mounts and the connection string every engine reads the table's files with.
func (e *mdsAzureEnv) runReadStep(t *testing.T, ref string) map[string]any {
	t.Helper()
	identifier, err := json.Marshal([]string{e.repo, ref, icebergcatalog.SystemNamespace, icebergcatalog.ObjectMetadataTable})
	require.NoError(t, err, "marshal the PyIceberg identifier")
	return e.runClient(t, "mds-azure-read:"+ref, mdsAzureReadScript, map[string]string{
		"MDS_WAREHOUSE":               e.storageNamespace,
		"MDS_IDENTIFIER":              string(identifier),
		"MDS_CREDENTIAL":              e.key + ":" + e.secret,
		"MDS_CATALOG_URI":             e.dockerReachableURL(e.baseURL + icebergcatalog.MetadataSearchBasePath),
		"MDS_RELATIVE_URI":            e.dockerReachableURL(e.baseURL+icebergcatalog.RelativeToBasePath) + "/" + e.repo + "." + ref + "/api",
		"MDS_OAUTH_URI":               e.dockerReachableURL(e.baseURL+icebergcatalog.MetadataSearchBasePath) + "/v1/oauth/tokens",
		"MDS_RELATIVE_OAUTH_URI":      e.dockerReachableURL(e.baseURL+icebergcatalog.APIBasePath) + "/v1/oauth/tokens",
		"MDS_RELATIVE_TABLE":          icebergcatalog.SystemNamespace + "." + icebergcatalog.ObjectMetadataTable,
		"MDS_MDS_SCHEMA":              e.repo + "." + ref + "." + icebergcatalog.SystemNamespace,
		"MDS_AZURE_CONNECTION_STRING": e.connectionString(),
	})
}

// TestMetadataSearchAzureEndToEnd is the AZ5 case list: the happy path read by both engines, the
// container-root refusal of D4 and the R3S-3 deny in its Azure spellings.
func TestMetadataSearchAzureEndToEnd(t *testing.T) {
	env := newMDSAzureEnv(t)
	env.startIndexer(t)

	t.Run("upload and commit", func(t *testing.T) {
		// Goal G4 is a property of the run, not of one request: the indexer writes the table with
		// the D3 variables alone. Claiming it is only meaningful when the file cannot provide them.
		config, err := os.ReadFile(env.configPath)
		require.NoError(t, err, "read the configuration both processes share")
		require.NotContainsf(t, string(config), "adls.",
			"the generated configuration must carry no iceberg-go Azure property: the indexer's Azure "+
				"IO is configured through LAKEFS_METADATA_SEARCH_AZURE_* alone (goal G4)")
		require.NotContainsf(t, string(config), "  storage:",
			"the generated configuration must carry no metadata_search.storage section: the run proves "+
				"that the env-var configuration of METADATA_SEARCH_AZURE D3 is enough")

		objects := []struct{ path, content string }{
			{mdsAzureOrdersPrefix + "a.txt", "orders a"},
			{mdsAzureOrdersPrefix + "b.txt", "orders bb"},
			{mdsAzureOrdersPrefix + "c.txt", "orders ccc"},
		}
		paths := make([]string, 0, len(objects))
		contents := make(map[string]string, len(objects))
		for _, object := range objects {
			env.uploadObject(t, mdsBranch, object.path, object.content)
			paths = append(paths, object.path)
			contents[object.path] = object.content
		}
		sourceCommit := env.commitBranch(t, env.admin, mdsBranch, "mds azure test: three orders objects")
		env.waitIndexed(t, mdsBranch, sourceCommit)

		read := env.readRows(t, mdsBranch)
		require.Equal(t, paths, mdsPaths(read.PyIceberg.Rows),
			"PyIceberg with adls.connection-string must see one row per object of %s", mdsBranch)
		require.Equal(t, mdsPaths(read.PyIceberg.Rows), mdsPaths(read.DuckDB.Rows),
			"DuckDB must see the same rows as PyIceberg over the same abfs:// locations")
		require.Equal(t, sourceCommit, read.PyIceberg.SourceCommit,
			"the table property %s is the freshness value and must be the indexed commit",
			mdsTableSourceCommit)

		require.Truef(t, strings.HasPrefix(read.PyIceberg.MetadataLocation, env.icebergLocationPrefix()),
			"the table location the catalog serves must be the canonical abfs[s] form of D1, %q..., got %q",
			env.icebergLocationPrefix(), read.PyIceberg.MetadataLocation)

		for _, name := range paths {
			row := mdsRowOf(t, read.PyIceberg.Rows, name)
			require.Equal(t, env.repo, row.Repository, "repository is the data repository")
			require.Equal(t, sourceCommit, row.CommitID, "commit_id is the commit that added the object")
			require.Equal(t, int64(len(contents[name])), row.SizeBytes)
			require.NotEmpty(t, row.ETag, "etag is the object's checksum")
			require.NotNil(t, row.Committer)
			require.Equal(t, mdsAdminUsername, *row.Committer)
			require.NotNil(t, row.ContentType)
			require.Equal(t, mdsObjectContentType, *row.ContentType)
			require.Empty(t, row.UserMetadata, "an object without user metadata has an empty map, never a null")
			require.Equal(t, row.ETag, mdsRowOf(t, read.DuckDB.Rows, name).ETag,
				"both engines must read the same etag")
		}
	})

	t.Run("container root namespace is refused", func(t *testing.T) {
		// D4: on Azure the first path segment of a namespace is the container, so a namespace that
		// has none names a container root and no companion namespace can be derived from it - the
		// derived name would be <container>-metadata, a container that does not exist. The refusal
		// must name the container and it must happen before anything is created, so R-metadata does
		// not exist afterwards.
		//
		// The repository is bare, and not because the case is about bare repositories: a bare
		// CreateRepository skips the storage-namespace-in-use check
		// (pkg/api/controller.ensureStorageNamespace), and the emulator is long-lived, so a marker
		// object an earlier run put at the container root would otherwise make this a failed
		// CreateRepository instead of the indexer's refusal. The refusal is the indexer's, and the
		// indexer never looks at the bare flag.
		rootRepo := env.repo + "-root"
		created, err := env.admin.CreateRepositoryWithResponse(t.Context(),
			&apigen.CreateRepositoryParams{Bare: apiutil.Ptr(true)},
			apigen.CreateRepositoryJSONRequestBody{
				Name: rootRepo,
				StorageNamespace: fmt.Sprintf("https://%s.%s/%s",
					env.account, azure.BlobEndpointTestDomain, env.container),
				DefaultBranch: apiutil.Ptr(mdsBranch),
			})
		require.NoErrorf(t, err, "create the container-root repository %s", rootRepo)
		require.Equalf(t, http.StatusCreated, created.StatusCode(),
			"create the container-root repository %s: %s", rootRepo, created.Body)

		// The indexer's repository set is its configuration, so the case restarts it with the new
		// repository. R stays configured: its own index is untouched and every later case needs it.
		env.extraRepositories = []string{rootRepo}
		env.stopIndexer(t)
		env.writeConfig(t, env.indexerKey, env.indexerSecret)
		env.startIndexer(t)

		deadline := time.Now().Add(mdsIndexTimeout)
		var last string
		for {
			env.exitedWhileRunning(t, env.indexer, "waiting for the container-root refusal")
			report, ok := env.indexerHealth(t)
			if ok {
				if health, found := report.Repositories[rootRepo]; found {
					last = health.LastError
					if last != "" {
						break
					}
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("the indexer did not report the container-root refusal of %s within %s "+
					"(last error %q); last %d lines of its log:\n%s", rootRepo, mdsIndexTimeout, last,
					mdsLogTailLines, mdsLogTail(env.indexPath, mdsLogTailLines))
			}
			time.Sleep(mdsPollInterval)
		}
		require.Containsf(t, last, "container root",
			"the refusal must say the namespace is a container root: %s", last)
		require.Containsf(t, last, env.container,
			"the refusal must name the container %q, or an operator cannot act on it: %s", env.container, last)
		require.Containsf(t, last, rootRepo+".storage_namespace",
			"the refusal must name the key that fixes it: %s", last)

		companion := icebergcatalog.MetadataRepository(rootRepo)
		repo, err := env.admin.GetRepositoryWithResponse(t.Context(), companion)
		require.NoErrorf(t, err, "read %s after the refusal", companion)
		require.Equalf(t, http.StatusNotFound, repo.StatusCode(),
			"a container-root namespace must not produce %s: %s", companion, repo.Body)

		// Back to the configured set, so a later case sees the environment the suite describes.
		env.extraRepositories = nil
		env.stopIndexer(t)
		env.writeConfig(t, env.indexerKey, env.indexerSecret)
		env.startIndexer(t)
		env.waitIndexed(t, mdsBranch, env.headCommit(t, env.admin, mdsBranch))
	})

	t.Run("guard group cannot create a repository on the companion storage namespace", func(t *testing.T) {
		// The R3S-3 case of mds_test.go on Azure, with the pair D4 adds: the adapter accepts a
		// namespace with either scheme, so the deny must name the companion namespace with http and
		// with https, and with and without a trailing slash. A deny that named only the stored
		// spelling would leave the same physical prefix to a guard-group member with fs:*, who could
		// create a bare repository on it and let its uncommitted GC sweep the index files.
		env.waitMetadataRepository(t)
		meta, err := env.admin.GetRepositoryWithResponse(t.Context(), env.metadataRepo)
		require.NoErrorf(t, err, "read %s", env.metadataRepo)
		require.Equalf(t, http.StatusOK, meta.StatusCode(), "read %s: %s", env.metadataRepo, meta.Body)
		require.NotNilf(t, meta.JSON200, "read %s: %s", env.metadataRepo, meta.Body)
		stored := strings.TrimRight(meta.JSON200.StorageNamespace, "/")
		require.Truef(t, strings.HasPrefix(stored, fmt.Sprintf("https://%s.%s/%s/",
			env.account, azure.BlobEndpointTestDomain, env.container)),
			"%s must live in the run's container: %q", env.metadataRepo, stored)

		// The full enumeration of storageNamespaceDenyResources for an Azure namespace with a
		// path: the stored spelling, that spelling with one "/", and the other scheme's twin of
		// each (D4). A container root would add "//" as well; a companion repository never is one.
		httpsSpellings := []string{stored, stored + "/"}
		spellings := make([]string, 0, 2*len(httpsSpellings))
		resources := make(map[string]struct{}, 2*len(httpsSpellings))
		for _, spelling := range httpsSpellings {
			httpSpelling := strings.Replace(spelling, "https://", "http://", 1)
			spellings = append(spellings, spelling, httpSpelling)
			for _, s := range []string{spelling, httpSpelling} {
				resources[permissions.StorageNamespace(s)] = struct{}{}
			}
		}

		// The deny is re-derived by the bootstrap's refresher once a minute; every spelling must be
		// named by the policy set of the guard group before a write is asserted, or a 403 could be
		// an unrelated refusal. Waiting for each ARN on its own asserts the whole set, not just one.
		groupID := env.groupID(t, mdsGuardGroup)
		for _, resource := range sortedKeysOfSet(resources) {
			env.waitCompanionNamespaceDeny(t, groupID, map[string]struct{}{resource: {}})
		}

		member := env.createPrincipal(t, "esti-mds-guard-member", nil)
		added, err := env.admin.AddGroupMembershipWithResponse(t.Context(), groupID, member.username)
		require.NoErrorf(t, err, "add %s to %s", member.username, mdsGuardGroup)
		require.Equalf(t, http.StatusCreated, added.StatusCode(), "add %s to %s: %s",
			member.username, mdsGuardGroup, added.Body)
		memberClient := env.coreAPIAs(t, member.key, member.secret)

		// The control: the same principal creates and deletes an ordinary repository in the same
		// container. Its fs:* allow is what the deny overrides below, so a failure here means the
		// grant is broken and the 403s are not evidence of anything.
		controlNS := fmt.Sprintf("https://%s.%s/%s/%s-guard-control",
			env.account, azure.BlobEndpointTestDomain, env.container, env.repo)
		for _, bare := range []bool{true, false} {
			name := env.repo + "-guard-control"
			if bare {
				name += "-bare"
			}
			created, err := memberClient.CreateRepositoryWithResponse(t.Context(),
				&apigen.CreateRepositoryParams{Bare: apiutil.Ptr(bare)},
				apigen.CreateRepositoryJSONRequestBody{
					Name:             name,
					StorageNamespace: controlNS,
					DefaultBranch:    apiutil.Ptr(mdsBranch),
				})
			require.NoErrorf(t, err, "create the control repository %s", name)
			require.Equalf(t, http.StatusCreated, created.StatusCode(),
				"%s is a member of %s and holds fs:*, so creating %s (bare=%t) on the ordinary namespace %q "+
					"must succeed, or the refusals below are not the deny of §8: %s",
				member.username, mdsGuardGroup, name, bare, controlNS, created.Body)
			deleted, err := memberClient.DeleteRepositoryWithResponse(t.Context(), name, &apigen.DeleteRepositoryParams{})
			require.NoErrorf(t, err, "delete the control repository %s", name)
			require.Equalf(t, http.StatusNoContent, deleted.StatusCode(),
				"delete the control repository %s: %s", name, deleted.Body)
		}

		for i, spelling := range spellings {
			for _, bare := range []bool{true, false} {
				name := fmt.Sprintf("%s-guard-%d-%t", env.repo, i, bare)
				res, err := memberClient.CreateRepositoryWithResponse(t.Context(),
					&apigen.CreateRepositoryParams{Bare: apiutil.Ptr(bare)},
					apigen.CreateRepositoryJSONRequestBody{
						Name:             name,
						StorageNamespace: spelling,
						DefaultBranch:    apiutil.Ptr(mdsBranch),
					})
				require.NoErrorf(t, err, "create %s on the companion storage namespace spelled %q", name, spelling)
				require.Equalf(t, http.StatusForbidden, res.StatusCode(),
					"%s is a member of %s and holds fs:*, so only the deny of §8 can refuse this: a repository "+
						"(bare=%t) on the storage namespace of %s spelled %q must be 403 (R3S-3, D4): %s",
					member.username, mdsGuardGroup, bare, env.metadataRepo, spelling, res.Body)
				require.Containsf(t, string(res.Body), "denied permission",
					"the refusal of the spelling %q must come from the deny policy, not from a missing grant: %s",
					spelling, res.Body)
				require.Containsf(t, string(res.Body), permissions.AttachStorageNamespaceAction,
					"the refusal of the spelling %q must name %s, the action the deny carries: %s",
					spelling, permissions.AttachStorageNamespaceAction, res.Body)

				found, err := memberClient.GetRepositoryWithResponse(t.Context(), name)
				require.NoErrorf(t, err, "read %s after the refused create", name)
				require.Equalf(t, http.StatusNotFound, found.StatusCode(),
					"a refused CreateRepository must not create %s: %s", name, found.Body)
			}
		}
	})
}

// mdsAzureReadScript is the read step of the Azure cases: the recipes of docs/metadata-search.md
// with the FileIO of an Azure installation (D5). It is mdsReadScript with one substitution - the
// engines read the table's files with adls.connection-string instead of the s3.* keys - because the
// S3 script's own path must stay byte-identical for the S3 suite.
//
// The connection string carries the emulator's path-style endpoint for both the Blob and the DFS
// service (see mdsAzureEnv.connectionString): Azurite serves table files through the Blob API, and
// DuckDB's abfs[s] filesystem is the only one it registers that reads a Data Lake URI, so both
// ends of that URI must resolve to the endpoint the emulator has.
const mdsAzureReadScript = `
import json
import os

import duckdb
from pyiceberg.catalog.rest import RestCatalog


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def sql_identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def metadata_map(value):
    # PyArrow reports an Iceberg map column as a list of (key, value) pairs, while older
    # versions (and DuckDB) report a dict. Both shapes mean the same map (design 3.1).
    if not value:
        return {}
    if isinstance(value, dict):
        items = value.items()
    else:
        items = value
    return {str(key): str(item) for key, item in items}


connection_string = required("MDS_AZURE_CONNECTION_STRING")

# D5: PyIceberg's Azure FileIO is adlfs, and adls.connection-string is the property that carries
# the account, the key and the emulator's own endpoint (a real installation passes account/key).
fileio_properties = {"adls.connection-string": connection_string}


def pyiceberg_read(uri, identifier):
    catalog = RestCatalog(
        name="lakefs",
        uri=uri,
        credential=required("MDS_CREDENTIAL"),
        warehouse=required("MDS_WAREHOUSE"),
        **fileio_properties,
    )
    table = catalog.load_table(identifier)
    scanned = table.scan().to_arrow()
    rows = []
    for row in scanned.to_pylist():
        rows.append({
            "path": row["path"],
            "repository": row["repository"],
            "commit_id": row["commit_id"],
            "etag": row["etag"],
            "size_bytes": row["size_bytes"],
            "user_metadata": metadata_map(row["user_metadata"]),
            "committer": row["committer"],
            "content_type": row["content_type"],
            "last_modified": str(row["last_modified"]),
            "creation_date": str(row["creation_date"]),
        })
    return {
        "rows": rows,
        "count": len(rows),
        "metadata_location": table.metadata_location,
        "source_commit": table.properties.get("lakefs.mds.source-commit", ""),
    }


def duckdb_iceberg_connection(oauth_uri):
    # A connection per mount: the catalog secret names the OAuth server of the mount the ATTACH
    # below uses, and a fresh duckdb.connect() keeps the two secrets and ATTACHes apart.
    con = duckdb.connect()
    con.execute("INSTALL azure")
    con.execute("LOAD azure")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    con.execute(
        "CREATE OR REPLACE SECRET lakefs_data (TYPE azure, CONNECTION_STRING %s)"
        % sql_literal(connection_string)
    )
    con.execute(
        "CREATE OR REPLACE SECRET lakefs_catalog (TYPE iceberg, CLIENT_ID %s, "
        "CLIENT_SECRET %s, OAUTH2_SERVER_URI %s)" % (
            sql_literal(required("MDS_CREDENTIAL").split(":", 1)[0]),
            sql_literal(required("MDS_CREDENTIAL").split(":", 1)[1]),
            sql_literal(oauth_uri),
        )
    )
    return con


def duckdb_rows(con, query, columns):
    rows = []
    for record in con.execute(query).fetchall():
        row = {}
        for index, column in enumerate(columns):
            value = record[index]
            if column == "user_metadata":
                value = metadata_map(value)
            row[column] = value
        rows.append(row)
    return {"rows": rows, "count": len(rows)}


# --- PyIceberg, through /mds/iceberg/api (the upstream endpoint of design §5.5) ---------------
pyiceberg = pyiceberg_read(required("MDS_CATALOG_URI"), tuple(json.loads(required("MDS_IDENTIFIER"))))

# --- PyIceberg, through /iceberg/relative_to/{repo}.{ref}/api (the warehouse form) ------------
pyiceberg_relative = pyiceberg_read(required("MDS_RELATIVE_URI"), required("MDS_RELATIVE_TABLE"))

# --- DuckDB, through /iceberg/relative_to/{repo}.{ref}/api ------------------------------------
duckdb_columns = ["path", "etag", "commit_id", "size_bytes", "user_metadata"]
con = duckdb_iceberg_connection(required("MDS_RELATIVE_OAUTH_URI"))
con.execute(
    "ATTACH %s AS lakefs (TYPE iceberg, ENDPOINT %s)" % (
        sql_literal(required("MDS_WAREHOUSE")),
        sql_literal(required("MDS_RELATIVE_URI")),
    )
)
duckdb_query = 'SELECT %s FROM lakefs."system"."object_metadata"' % ", ".join(duckdb_columns)
duckdb_relative = duckdb_rows(con, duckdb_query, duckdb_columns)

# --- DuckDB, through /mds/iceberg/api (the identifier form: one schema, three levels) ---------
mds_schema = sql_identifier(required("MDS_MDS_SCHEMA"))
con_mds = duckdb_iceberg_connection(required("MDS_OAUTH_URI"))
con_mds.execute(
    "ATTACH %s AS lakefs_mds (TYPE iceberg, ENDPOINT %s)" % (
        sql_literal(required("MDS_WAREHOUSE")),
        sql_literal(required("MDS_CATALOG_URI")),
    )
)
duckdb_mds_query = 'SELECT %s FROM lakefs_mds.%s."object_metadata"' % (
    ", ".join(duckdb_columns), mds_schema,
)
duckdb_identifier = duckdb_rows(con_mds, duckdb_mds_query, duckdb_columns)

print("RESULT:" + json.dumps({
    "pyiceberg": pyiceberg,
    "pyiceberg_relative": pyiceberg_relative,
    "duckdb": duckdb_relative,
    "duckdb_identifier": duckdb_identifier,
}))
`
