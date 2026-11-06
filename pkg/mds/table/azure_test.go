package table_test

// The Azure integration test: the whole Metadata Search write path on an Azure blockstore, against
// Azurite (METADATA_SEARCH_AZURE.md §5 task AZ4, decisions D1-D3).
//
// Two halves of the product meet in this test, and both are real:
//
//   - the server half is the fixture of server_test.go - a real lakeFS catalog, a real block
//     adapter, and the production catalog routes over HTTP - with the Azure adapter instead of the
//     local one. The adapter is configured with blockstore.azure.test_endpoint_url, the same hook
//     pkg/block/azure's own tests use, so an object it reads or writes is an object Azurite holds.
//   - the client half is the indexer's iceberg-go client (table.NewCatalog), writing through
//     iceberg-go's Azure IO, which catalog.go registers for the indexer (D3). Its IO properties are
//     the adls.* keys of D3, and they point at the same Azurite.
//
// What the test proves is exactly what D1 and D2 promise. The catalog hands the client one form of
// a location - abfs[s]://<container>@acct.azurite.test/... - that every target FileIO opens, and it
// keeps talking to its own storage in the block adapter's form -
// http[s]://acct.azurite.test/<container>/... - so that lakeFS GC, stat and the S3 gateway can read
// what the catalog wrote. Between them sit the six blockIdentifier call sites of D2: the initial
// metadata.json of a create, the metadata.json of every commit, the manifest and manifest-list
// reads of the file collector, and the physical addresses it stages. A single missing call site
// makes a commit fail (the adapter cannot resolve an abfs:// address) or leaves a staged entry
// unreadable (asserted below on every staged file).
//
// The test runs twice, once per D1 row the emulator can carry: an http namespace, which translates
// to abfs:// and http:// (the forms the task pins), and an https namespace, which translates to
// abfss:// and https:// (the forms a real installation has).
//
// The scenario is the indexer's: create the table, append, apply a delta with a delete, compact,
// expire snapshots, then read the table back. Every step writes files through the client's Azure
// IO and commits through the catalog, so an iceberg-go Azure backend defect (the risk of §7, v0.7.0
// being young code) shows up as a failure of one step rather than as a surprise in production.
//
// The emulator, not a real account, is the runner: AZ4 is the CI gate, and METADATA_SEARCH_AZURE.md
// §7 requires the real-account gate (AZ7) before the feature is called done. Azurite has neither a
// dfs endpoint nor HNS, which is why the namespace's domain is azurite.test and why DFSDomainFor
// keeps that domain as its own DFS host (D1). The container is started with --skipApiVersionCheck
// for the same reason pkg/block/azure's tests need it: the Azure SDK negotiates a newer REST API
// version than the emulator implements.
//
// Docker, like the block adapter tests: skipped when it is unavailable, so the package's other
// tests still run on a machine without it.
//
//	go test -count=1 -race -run TestAzureObjectMetadataRoundTrip ./pkg/mds/table/

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/benburkert/dns"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/azure"
	lakefscatalog "github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

const (
	// azuriteAccount and azuriteKey are the one pair AZURITE_ACCOUNTS gives the emulator and the
	// test gives every client. The key is valid base64 because both sides of a shared-key
	// signature decode it as such.
	azuriteAccount = "acct"
	azuriteKey     = "a2V5MQ==" // "key1"

	// azuriteContainer is the container the repository's namespace is rooted in, so the test's
	// assertions read abfs://<container>@<account>.<domain>/... and
	// http://<account>.<domain>/<container>/... The emulator enforces Azure's rule that a
	// container name is at least three characters long (METADATA_SEARCH_AZURE.md D1 writes the
	// container as "c" in its schematic table, which Azurite would refuse).
	azuriteContainer = "container1"

	// azuriteTag pins the emulator image (METADATA_SEARCH_AZURE.md §7: the Azure backend is
	// young code, so the runner is pinned like the iceberg-go version).
	azuriteTag = "3.35.0"

	// azuriteContainerTimeoutSeconds expires the container even if the test process dies before
	// its cleanup runs, so a killed run cannot leak the emulator.
	azuriteContainerTimeoutSeconds = 10 * 60

	// azuriteResolutionTTL is the TTL of the A record the custom resolver answers for the account
	// host, the same value pkg/block/azure's tests use.
	azuriteResolutionTTL = 5 * time.Minute

	// azuriteSkipReason is why the test skips instead of failing when the emulator cannot be
	// started: this is an integration test, and a machine without Docker still runs the package.
	azuriteSkipReason = "the Azurite integration test needs Docker, like the pkg/block/azure tests"
)

// azurite is one running emulator, in the three spellings the two halves of the test need.
type azurite struct {
	// endpoint is the base URL of the blob service, http://acct.azurite.test:<port>. It is
	// blockstore.azure.test_endpoint_url for the adapter and the URL the test creates the
	// container with.
	endpoint string
	// containerURL is the container in the block adapter's own spelling, without the port:
	// http://acct.azurite.test/<container>. The fixture's storage namespaces are built from it,
	// because a namespace is what the catalog translates (D1) and the port belongs to the test
	// endpoint, not to the account.
	containerURL string
	// adlsEndpoint is the host:port iceberg-go's Azure IO appends the account name to
	// (<account>.<adls.endpoint>), so it is the endpoint without the scheme.
	adlsEndpoint string
}

// startAzurite starts the emulator the way pkg/block/azure's main_test.go does - same image, same
// command line, same AZURITE_ACCOUNTS trick for a keyed account - and skips the test when Docker is
// unavailable. The container is purged in t.Cleanup.
func startAzurite(t *testing.T) *azurite {
	t.Helper()
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("%s: %v", azuriteSkipReason, err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Skipf("%s: %v", azuriteSkipReason, err)
	}

	// The default Cmd of the image is overridden, so its host bindings are restated here, and
	// --skipApiVersionCheck accepts the REST API version the SDK negotiates.
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		Tag:        azuriteTag,
		Env: []string{
			fmt.Sprintf("AZURITE_ACCOUNTS=%s:%s", azuriteAccount, azuriteKey),
		},
		Cmd: []string{
			"azurite", "-l", "/data",
			"--blobHost", "0.0.0.0",
			"--queueHost", "0.0.0.0",
			"--tableHost", "0.0.0.0",
			"--skipApiVersionCheck",
		},
	})
	if err != nil {
		t.Skipf("%s: %v", azuriteSkipReason, err)
	}
	t.Cleanup(func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("purge the Azurite container: %v", err)
		}
	})
	require.NoError(t, resource.Expire(azuriteContainerTimeoutSeconds), "expire the Azurite container")

	port := resource.GetPort("10000/tcp")
	endpoint := fmt.Sprintf("http://%s.%s:%s", azuriteAccount, azure.BlobEndpointTestDomain, port)

	credential, err := azblob.NewSharedKeyCredential(azuriteAccount, azuriteKey)
	require.NoError(t, err, "build the Azurite shared-key credential")
	client, err := azblob.NewClientWithSharedKeyCredential(endpoint, credential, nil)
	require.NoError(t, err, "build the Azurite client")
	_, err = client.CreateContainer(t.Context(), azuriteContainer, nil)
	require.NoError(t, err, "create the Azurite container")

	return &azurite{
		endpoint:     endpoint,
		containerURL: fmt.Sprintf("http://%s.%s/%s", azuriteAccount, azure.BlobEndpointTestDomain, azuriteContainer),
		adlsEndpoint: fmt.Sprintf("%s:%s", azure.BlobEndpointTestDomain, port),
	}
}

// TestMain installs the emulator's DNS record before the first test runs, the way pkg/block/azure's
// main_test.go does.
//
// It cannot happen in the test that starts the container: net.DefaultResolver is the resolver every
// dial in the process reads, so replacing it while the binary has goroutines - and the dockertest
// client is dialling the moment the emulator starts - is a data race the race detector reports.
// Installing it here, before m.Run() starts a single test, writes it while nothing reads it, and it
// costs nothing when the test skips: no test of this package resolves anything but localhost.
func TestMain(m *testing.M) {
	installAzuriteResolver()
	os.Exit(m.Run())
}

// installAzuriteResolver answers A queries for the account host from a zone of the test's own and
// forwards every other query to the process's upstream servers, so the global resolver replacement
// cannot break name resolution for the rest of the test binary.
func installAzuriteResolver() {
	zone := dns.Zone{
		Origin: azure.BlobEndpointTestDomain + ".",
		TTL:    azuriteResolutionTTL,
		RRs: dns.RRSet{
			azuriteAccount: map[dns.Type][]dns.Record{
				dns.TypeA: {
					&dns.A{A: net.IPv4(127, 0, 0, 1).To4()},
				},
			},
		},
	}
	mux := new(dns.ResolveMux)
	mux.Handle(dns.TypeANY, zone.Origin, &zone)
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial:     (&dns.Client{Resolver: mux}).Dial,
	}
}

// blockstoreConfig is the Azure blockstore of the fixture: the account, its shared key, the test
// domain and the emulator's endpoint (blockstore.azure.test_endpoint_url, which the adapter uses
// instead of <account>.<domain>).
func (a *azurite) blockstoreConfig() map[string]any {
	return map[string]any{
		config.BlockstoreTypeKey:              block.BlockstoreTypeAzure,
		"blockstore.azure.storage_account":    azuriteAccount,
		"blockstore.azure.storage_access_key": azuriteKey,
		"blockstore.azure.domain":             azure.BlobEndpointTestDomain,
		"blockstore.azure.test_endpoint_url":  a.endpoint,
	}
}

// clientStorage is metadata_search.storage of an indexer that writes to this emulator: the adls.*
// properties of METADATA_SEARCH_AZURE.md D3, with the endpoint and the protocol that make
// iceberg-go's Azure IO address <account>.<adls.endpoint> over http rather than the public cloud.
func (a *azurite) clientStorage() iceberg.Properties {
	return iceberg.Properties{
		icebergio.ADLSSharedKeyAccountName: azuriteAccount,
		icebergio.ADLSSharedKeyAccountKey:  azuriteKey,
		icebergio.ADLSEndpoint:             a.adlsEndpoint,
		icebergio.ADLSProtocol:             "http",
	}
}

// namespace is the storage namespace of a repository whose data lives in the emulator's container,
// in the block adapter's spelling. The scheme is the caller's: an http namespace translates to
// abfs:// and an https one to abfss:// (D1), and both are what the adapter stores.
func (a *azurite) namespace(scheme, repository string) string {
	return fmt.Sprintf("%s://%s.%s/%s/repos/%s",
		scheme, azuriteAccount, azure.BlobEndpointTestDomain, azuriteContainer, repository)
}

// icebergNamespace is the location form the catalog must hand out for that namespace: the canonical
// abfs[s] form of D1 - non-TLS abfs:// for an http namespace, abfss:// for an https one - with the
// container and the account of the namespace it came from.
func (a *azurite) icebergNamespace(scheme, repository string) string {
	locationScheme := "abfs"
	if scheme == "https" {
		locationScheme = "abfss"
	}
	return fmt.Sprintf("%s://%s@%s.%s/repos/%s",
		locationScheme, azuriteContainer, azuriteAccount, azure.BlobEndpointTestDomain, repository)
}

// TestAzureObjectMetadataRoundTrip runs the indexer's write path against Azurite
// (METADATA_SEARCH_AZURE.md §5 AZ4) over both spellings of a repository namespace: the two rows of
// D1 the emulator can carry.
func TestAzureObjectMetadataRoundTrip(t *testing.T) {
	emulator := startAzurite(t)
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			runObjectMetadataRoundTrip(t, emulator, scheme)
		})
	}
}

// runObjectMetadataRoundTrip creates a table, appends, compacts, expires and reads it back, then
// checks the two spellings the catalog must keep apart for a repository whose namespace is scheme:
// the abfs[s]:// form a client sees and the http(s):// form the block adapter stores.
//
// Both schemes are worth running. http is the one the task pins (abfs:// locations and http://
// physical addresses, which is what Azurite's own endpoint speaks), and https is the shape a real
// installation has (abfss:// locations, https:// physical addresses): the emulator is addressed over
// http either way - blockstore.azure.test_endpoint_url and adls.protocol - so the two cases differ
// only in the strings the catalog produces and validates.
func runObjectMetadataRoundTrip(t *testing.T, emulator *azurite, scheme string) {
	t.Helper()
	env := newTestEnvWith(t, testEnvOptions{
		viper: emulator.blockstoreConfig(),
		storageNamespace: func(_, repository string) string {
			return emulator.namespace(scheme, repository)
		},
		storage: emulator.clientStorage(),
	})

	first := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	second := first.Add(time.Hour)

	created := env.createTable(t)

	// Append: the rows go through iceberg-go's Azure IO (Parquet, manifest, manifest list) and the
	// commit through the catalog, which rewrites metadata.json below the abfs[s] location of the
	// table and reads the manifest list back through the block adapter.
	appended, err := table.ApplyDelta(t.Context(), created, []table.Row{
		row(t, "orders/a", "c1", 1, first, map[string]string{"k": "1"}),
		row(t, "orders/b", "c1", 2, first, nil),
		row(t, "orders/c", "c1", 3, first, nil),
	}, nil, "c1")
	require.NoError(t, err, "append through iceberg-go's Azure IO")
	require.Equal(t, "c1", appended.Properties()[table.PropertySourceCommit])

	// A delta with a delete: position delete files are read back by the compaction, so a location
	// translation that broke only on the delete path would fail here rather than in production.
	updated, err := table.ApplyDelta(t.Context(), appended, []table.Row{
		row(t, "orders/a", "c2", 10, second, map[string]string{"k": "10"}),
	}, []string{"orders/b"}, "c2")
	require.NoError(t, err, "apply a delta with a delete through iceberg-go's Azure IO")
	require.Positive(t, summaryInt(t, updated, "total-position-deletes"))

	compacted, err := table.Compact(t.Context(), updated, "c2")
	require.NoError(t, err, "compact through iceberg-go's Azure IO")
	require.Zero(t, summaryInt(t, compacted, "total-position-deletes"),
		"the compaction removes the delete files it applied")

	// Expire rewrites the table metadata with iceberg-go's MetadataBuilder and commits the removal
	// of every snapshot but the current one: the remove-snapshots rewrite the catalog serves a
	// client, and the shape of the metadata file lakectl iceberg cleanup writes server-side
	// (design §4.9, docs/metadata-search.md "Snapshot expiry"). Post-commit file deletion is off:
	// this test is about the location translation of a metadata rewrite, and deleting the files
	// would test the emulator's DELETE instead.
	expired, err := expireSnapshots(t, compacted)
	require.NoError(t, err, "expire snapshots through iceberg-go's Azure IO")
	require.Len(t, expired.Metadata().Snapshots(), 1, "expiry keeps the current snapshot only")

	// Read back: a full scan of the table as a reader sees it, over the client's Azure IO.
	requireScanRows(t, env.loadTable(t), []table.Row{
		row(t, "orders/a", "c2", 10, second, map[string]string{"k": "10"}),
		row(t, "orders/c", "c1", 3, first, nil),
	})

	requireAzureLocations(t, env, emulator, scheme, expired)
}

// expireSnapshots expires every snapshot but the current one through the catalog, the way an
// operator's maintenance run rewrites and commits table metadata.
func expireSnapshots(t *testing.T, tbl *icebergtable.Table) (*icebergtable.Table, error) {
	t.Helper()
	txn := tbl.NewTransaction()
	err := txn.ExpireSnapshots(
		icebergtable.WithOlderThan(0),
		icebergtable.WithRetainLast(1),
		icebergtable.WithPostCommit(false),
	)
	if err != nil {
		return nil, err
	}
	return txn.Commit(t.Context())
}

// requireAzureLocations asserts the two spellings of D1/D2 on the branch and in the table metadata
// the test just wrote, for a repository whose namespace scheme is scheme.
func requireAzureLocations(t *testing.T, env *testEnv, emulator *azurite, scheme string, tbl *icebergtable.Table) {
	t.Helper()
	tableUUID := tbl.Metadata().TableUUID().String()
	icebergRoot := emulator.icebergNamespace(scheme, env.repo) + "/_iceberg/" + tableUUID
	adapterRoot := emulator.namespace(scheme, env.repo) + "/_iceberg/" + tableUUID + "/"

	require.Equal(t, icebergRoot, tbl.Metadata().Location(),
		"the table location in metadata.json is the canonical abfs[s] form of D1, not the namespace the server reads")

	pointer := env.tablePointer(t)
	metadataLocation := pointer.Metadata[icebergcatalog.MetadataLocationKey]
	require.True(t, strings.HasPrefix(metadataLocation, icebergRoot+"/metadata/"),
		"the pointer must name the metadata.json in the abfs[s] form, got %q", metadataLocation)
	require.True(t, strings.HasSuffix(metadataLocation, ".metadata.json"),
		"the pointer must name a metadata document, got %q", metadataLocation)

	files := env.stagedTableFiles(t)
	require.NotEmpty(t, files, "the commits staged no table file")
	var sawDataFile, sawMetadata bool
	for _, file := range files {
		require.True(t, strings.HasPrefix(file.PhysicalAddress, adapterRoot),
			"the staged entry %q must point at the adapter's spelling %q, got %q",
			file.Path, adapterRoot, file.PhysicalAddress)
		if strings.Contains(file.PhysicalAddress, "/data/") && strings.HasSuffix(file.PhysicalAddress, ".parquet") {
			sawDataFile = true
		}
		if strings.Contains(file.PhysicalAddress, "/metadata/") && strings.HasSuffix(file.PhysicalAddress, ".metadata.json") {
			sawMetadata = true
		}
	}
	require.True(t, sawDataFile, "no staged entry points at a data file: the append wrote none")
	require.True(t, sawMetadata, "no staged entry points at a metadata document: a commit wrote none")
}

// tablePointer returns the table's .pointer entry on the fixture's branch, i.e. the compare-and-swap
// target that carries the metadata location of D2.
func (e *testEnv) tablePointer(t *testing.T) *lakefscatalog.DBEntry {
	t.Helper()
	path := strings.Join([]string{
		icebergcatalog.ReservedPrefix,
		icebergcatalog.TablesDirectory,
		table.Namespace,
		table.Name,
		icebergcatalog.TablePointerName,
	}, "/")
	entry, err := e.cat.GetEntry(t.Context(), e.repo, testBranch, path, lakefscatalog.GetEntryParams{})
	require.NoErrorf(t, err, "read the table pointer %q", path)
	return entry
}

// stagedTableFiles returns the table-file entries the branch holds - one per file the catalog
// registered for a commit (ICEBERG_CATALOG_PLAN.md §6.3) - so a test can assert the physical address
// of each.
func (e *testEnv) stagedTableFiles(t *testing.T) []*lakefscatalog.DBEntry {
	t.Helper()
	prefix := icebergcatalog.ReservedPrefix + "/" + icebergcatalog.FilesDirectory + "/"
	entries, _, err := e.cat.ListEntries(t.Context(), e.repo, testBranch, prefix, "", "", lakefscatalog.ListEntriesLimitMax)
	require.NoErrorf(t, err, "list the staged table files under %q", prefix)
	return entries
}
