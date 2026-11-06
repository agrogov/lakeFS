package icebergcatalog

// Azure Blob Storage locations (METADATA_SEARCH_AZURE.md D1, D2, task AZ1).
//
// A repository on an Azure blockstore stores its data under a namespace of the form
//
//	https://<account>.<blob domain>/<container>/<path>
//
// and the server reads and writes it through the azure block adapter. No Iceberg FileIO opens that
// form - iceberg-go registers abfs[s]/wasb[s] in io/gocloud/azure only, Iceberg Java's ADLSFileIO
// takes abfs[s]/wasb[s], Trino's native Azure file system takes abfs[s] only, and PyIceberg maps
// abfs[s] to adlfs - so the catalog translates the namespace into the one form all of them open
// (D1):
//
//	https://acct.blob.core.windows.net/c/p  ->  abfss://c@acct.dfs.core.windows.net/p
//	http://acct.blob.core.windows.net/c/p   ->  abfs://c@acct.dfs.core.windows.net/p
//
// The scheme follows the namespace's transport (https -> abfss, http -> abfs); the host's service
// part is the Data Lake Storage Gen2 endpoint of the same account, except for the test domain,
// which only has the host Azurite serves, and for an installation that sets
// iceberg.azure.location_host: blob (D1's one switch, because Hadoop >= 3.4's ABFS driver refuses
// an FNS account over the dfs endpoint and expects the blob host instead).
//
// The mapping is a pure function of the namespace and of that setting, and its inverse is not a
// general parser: a location is mapped back to the block adapter's spelling only after it has been
// checked to be under the table area, by replacing that area prefix (D2, location.go
// blockIdentifier). Nothing outside the area is ever translated, so the §13 "write-anywhere" guard
// is exactly as strong as it was before Azure locations were translated.
//
// Clean-room note (ICEBERG_CATALOG_PLAN.md §2): written from METADATA_SEARCH_AZURE.md D1/D2, the
// public Azure "Use the Azure Data Lake Storage URI" documentation and the public Iceberg/Trino
// file-system documentation. No code was taken from lakeFS Enterprise or from upstream lakeFS.

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/treeverse/lakefs/pkg/block/azure"
)

const (
	// AzureLocationHostDFS selects the Data Lake Storage Gen2 endpoint as the host's service part
	// of a translated Azure location (abfss://c@acct.dfs.core.windows.net/p). It is the default:
	// it is the form Trino, Iceberg Java's ADLSFileIO, PyIceberg and DuckDB all open.
	AzureLocationHostDFS = "dfs"

	// AzureLocationHostBlob selects the Blob endpoint instead (abfss://c@acct.blob.core.windows.net/p),
	// for an engine whose ABFS driver refuses a non-HNS account over the dfs endpoint. The scheme
	// stays abfs[s]; only the service part of the host changes (D1).
	AzureLocationHostBlob = "blob"

	// DefaultAzureLocationHost is the documented default of iceberg.azure.location_host.
	DefaultAzureLocationHost = AzureLocationHostDFS
)

// ErrInvalidAzureNamespace is wrapped when a storage namespace cannot be translated into an
// Iceberg location: it is not an http(s) namespace, or its host is not a storage account of a
// domain this blockstore supports. It is a configuration/data error rather than a client error:
// repositories are created against the adapter's own validity regex, so a namespace that fails here
// could not have been created by this installation.
var ErrInvalidAzureNamespace = errors.New("invalid Azure storage namespace")

// azureLocationHostSetting is the configured iceberg.azure.location_host. It is process state
// because the mapping is used by package functions that have no configuration in hand
// (TableAreaLocation, TableRoot, blockIdentifier); LoadConfig sets it once at startup, and the
// atomic value keeps a test that reads a location while another loads a configuration race-free.
// Its zero value is "not stored", which azureLocationHost reads as the default.
var azureLocationHostSetting atomic.Value // string

// setAzureLocationHost records the configured location host. An empty value is the default, so a
// caller that has no setting keeps dfs.
func setAzureLocationHost(host string) {
	if host == "" {
		host = DefaultAzureLocationHost
	}
	azureLocationHostSetting.Store(host)
}

// azureLocationHost returns the configured location host (dfs or blob).
func azureLocationHost() string {
	host, ok := azureLocationHostSetting.Load().(string)
	if !ok || host == "" {
		return DefaultAzureLocationHost
	}
	return host
}

// IcebergNamespaceLocation translates a repository's Azure storage namespace into the Iceberg
// location form of D1:
//
//	https://acct.<blob domain>/<container>/<path> -> abfss://<container>@acct.<dfs domain>/<path>
//	http://acct.<blob domain>/<container>/<path>  -> abfs://<container>@acct.<dfs domain>/<path>
//
// The result has no trailing slash; a container-root namespace maps to the container root
// (abfss://<container>@acct.<dfs domain>). A namespace of another blockstore, or a host that is not
// a supported Azure storage account domain, is an ErrInvalidAzureNamespace: the mapping must not
// invent a location the server cannot read back through the same adapter.
func IcebergNamespaceLocation(storageNamespace string) (string, error) {
	return icebergNamespaceLocationForHost(storageNamespace, azureLocationHost())
}

// icebergNamespaceLocation is IcebergNamespaceLocation for a caller that only wants the mapping
// when it exists: it reports false for every namespace the catalog must leave in its own spelling
// (a non-Azure blockstore, or an Azure-shaped namespace whose domain this adapter does not serve).
func icebergNamespaceLocation(storageNamespace string) (string, bool) {
	location, err := IcebergNamespaceLocation(storageNamespace)
	if err != nil {
		return "", false
	}
	return location, true
}

// icebergNamespaceLocationForHost is the pure mapping: storageNamespace in the block adapter's
// spelling, hostKind the configured service part (AzureLocationHostDFS or AzureLocationHostBlob).
func icebergNamespaceLocationForHost(storageNamespace, hostKind string) (string, error) {
	trimmed := strings.TrimSpace(storageNamespace)
	// A cheap scheme check before url.Parse: this runs for every file a commit collects, and a
	// repository on another blockstore must cost nothing more than it did before.
	if !strings.HasPrefix(trimmed, "http://") && !strings.HasPrefix(trimmed, "https://") {
		return "", fmt.Errorf("%w: %q is not an http(s) Azure storage namespace",
			ErrInvalidAzureNamespace, storageNamespace)
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("%w: %q cannot be parsed: %w", ErrInvalidAzureNamespace, storageNamespace, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("%w: %q is not an http(s) Azure storage namespace",
			ErrInvalidAzureNamespace, storageNamespace)
	}
	// ResolveBlobURLInfoFromURL is the adapter's own split of <account>.<domain>/<container>/<path>,
	// so the translated location names the same account, container and path the server reads with
	// its credentials.
	info, err := azure.ResolveBlobURLInfoFromURL(parsed)
	if err != nil {
		return "", fmt.Errorf("%w: %q: %w", ErrInvalidAzureNamespace, storageNamespace, err)
	}
	blobDomain := strings.TrimPrefix(info.Host, info.StorageAccountName+".")
	if blobDomain == info.Host {
		return "", fmt.Errorf("%w: %q has no storage account domain", ErrInvalidAzureNamespace, storageNamespace)
	}
	dfsDomain, err := azure.DFSDomainFor(blobDomain)
	if err != nil {
		return "", fmt.Errorf("%w: %q: %w", ErrInvalidAzureNamespace, storageNamespace, err)
	}
	serviceDomain := dfsDomain
	if hostKind == AzureLocationHostBlob {
		// D1's one switch: the cloud's blob domain that the DFS domain was derived from.
		serviceDomain = blobDomain
	}
	scheme := "abfs"
	if parsed.Scheme == "https" {
		scheme = "abfss"
	}
	location := scheme + "://" + info.ContainerName + "@" + info.StorageAccountName + "." + serviceDomain
	if info.BlobURL != "" {
		location += "/" + info.BlobURL
	}
	return location, nil
}
