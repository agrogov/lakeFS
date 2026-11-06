package table

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"

	// Blank imports: the indexer writes its Parquet, Avro and metadata files through iceberg-go's
	// IO. With vended credentials the catalog hands the client a session, and without them
	// metadata_search.storage supplies static keys (design §4.6). The imports register the S3 and
	// Azure schemes in iceberg-go's IO registry: s3, s3a and s3n here, abfs, abfss, wasb and wasbs
	// from the Azure backend (METADATA_SEARCH_AZURE.md D3). The file and local schemes are
	// registered by iceberg-go itself and the local block adapter is not part of a supported
	// Metadata Search deployment.
	_ "github.com/apache/iceberg-go/io/gocloud/azure"
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
)

const (
	// CatalogName labels the client side of the catalog. iceberg-go looks its own configuration
	// file up by catalog name; the name is not part of the address.
	CatalogName = "lakefs-mds"

	// RelativeToPath is the addressing form the indexer uses (design §4.3 step 3):
	//
	//	<lakefs_mds_endpoint>/iceberg/relative_to/<R-metadata>.<branch>/api
	//
	// It fixes the repository and the branch in the URL, so the table identifier the writer sends
	// is the plain <namespace>.<table> of §3.1 rather than an address.
	RelativeToPath = "/iceberg/relative_to"
)

// The errors of this package's entry points. They are static so that a caller can match them with
// errors.Is, and so that a message is never built from a value the caller cannot see.
var (
	// ErrInvalidCatalogConfig is wrapped by every validation error of CatalogConfig.
	ErrInvalidCatalogConfig = errors.New("invalid metadata search catalog configuration")

	// ErrNoCatalog is returned by CreateTable without a catalog: a table cannot be created through
	// a client that does not exist, and a nil dereference would be the alternative.
	ErrNoCatalog = errors.New("no catalog")
)

// DefaultRequestTimeout is how long one HTTP request of a catalog client may take when
// CatalogConfig.RequestTimeout is zero. iceberg-go builds its own http.Client with no Timeout
// (catalog/rest/rest.go createSession), so without this bound a connection that neither answers
// nor resets blocks the caller forever: for the indexer that is one branch tick holding the whole
// tick loop, while /_health keeps reporting the tick as merely in progress (§4.1, §9).
const DefaultRequestTimeout = 60 * time.Second

// CatalogConfig is what the indexer needs to reach the Iceberg REST catalog of one branch of a
// companion repository (design §4.3 step 3, §4.6).
type CatalogConfig struct {
	// Endpoint is the lakeFS base URL (metadata_search.lakefs_mds_endpoint), e.g.
	// http://lakefs:8000. The API and the catalog both live under it.
	Endpoint string
	// Repository is the companion repository R-metadata (§3.3), not the data repository.
	Repository string
	// Branch is the branch of R-metadata the writes land on: the branch of the data repository
	// that is being indexed (§3.3, §4.2).
	Branch string
	// AccessKeyID and SecretAccessKey are the credentials of the indexer principal. They are sent
	// as HTTP basic authentication, which the catalog accepts on every route (design §5,
	// pkg/icebergcatalog/auth.go), so no OAuth round trip is needed.
	AccessKeyID     string
	SecretAccessKey string
	// Storage is passed to iceberg-go with rest.WithAdditionalProps (§4.6): the IO properties
	// (s3.endpoint, s3.region, s3.access-key-id, s3.secret-access-key, s3.path-style-access, and the
	// adls.* properties of METADATA_SEARCH_AZURE.md D3) used when the catalog does not vend write
	// credentials. Empty means "the catalog vends, or the client's own environment is enough".
	Storage iceberg.Properties
	// RequestTimeout bounds every HTTP request of the client this configuration builds (§4.1,
	// §7). Zero means DefaultRequestTimeout; it does not mean "no timeout". It is the transport's
	// bound, not the caller's context's, so it survives a call that passes the process context.
	RequestTimeout time.Duration
}

// Validate reports whether the configuration can address a catalog.
func (c CatalogConfig) Validate() error {
	switch {
	case strings.TrimSpace(c.Endpoint) == "":
		return fmt.Errorf("%w: no endpoint", ErrInvalidCatalogConfig)
	case strings.TrimSpace(c.Repository) == "":
		return fmt.Errorf("%w: no repository", ErrInvalidCatalogConfig)
	case strings.TrimSpace(c.Branch) == "":
		return fmt.Errorf("%w: no branch", ErrInvalidCatalogConfig)
	case c.AccessKeyID == "" || c.SecretAccessKey == "":
		return fmt.Errorf("%w: no credentials", ErrInvalidCatalogConfig)
	}
	return nil
}

// URI returns the catalog URI of the configured branch: the relative_to form of design §4.3 step
// 3, <endpoint>/iceberg/relative_to/<repository>.<branch>/api.
//
// The warehouse segment is one URL path segment and a lakeFS branch name may contain "/", so the
// segment is percent-escaped once; the catalog decodes it before splitting the repository from
// the ref (pkg/icebergcatalog/ident.go ParseWarehouse).
func (c CatalogConfig) URI() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	endpoint, err := url.Parse(strings.TrimSpace(c.Endpoint))
	if err != nil {
		return "", fmt.Errorf("%w: endpoint %q: %w", ErrInvalidCatalogConfig, c.Endpoint, err)
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return "", fmt.Errorf("%w: endpoint %q is not an absolute URL", ErrInvalidCatalogConfig, c.Endpoint)
	}
	warehouse := url.PathEscape(strings.TrimSpace(c.Repository) + "." + strings.TrimSpace(c.Branch))
	return strings.TrimRight(strings.TrimSpace(c.Endpoint), "/") + RelativeToPath + "/" + warehouse + "/api", nil
}

// Authorization returns the HTTP basic Authorization header value of the configured credentials.
func (c CatalogConfig) Authorization() string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(c.AccessKeyID+":"+c.SecretAccessKey))
}

// NewCatalog builds the iceberg-go REST client for one branch of a companion repository (design
// §4.3 step 3, §4.6): rest.NewCatalog over the relative_to URI, HTTP basic credentials and the
// static IO properties of metadata_search.storage.
//
// The two options are the ones design §4.6 names. rest.WithHeaders carries the basic credentials
// on every request, including the GET /v1/config the client starts with. rest.WithAdditionalProps
// carries the storage properties into the table's IO, which is what lets the transaction write
// Parquet, Avro and metadata files when the catalog does not vend credentials - iceberg-go does
// not implement S3 remote signing.
//
// Vended mode needs no option here and no property in Storage: the client asks for vended
// credentials itself. iceberg-go's REST session sends `X-Iceberg-Access-Delegation:
// vended-credentials` on every request as a session default (catalog/rest/rest.go), which
// vended_test.go asserts at the HTTP boundary. The client uses the session only when the
// loadTable or createTable response carries `storage-credentials` (catalog/rest/rest.go:
// `credsVended := len(ret.StorageCredentials) > 0`); the separate GET .../tables/{table}/credentials
// endpoint is only used to refresh a session already in hand. pkg/icebergcatalog fills that member
// with a write session on a branch for a caller allowed catalog:WriteTableData, and the
// credentials endpoint refreshes the same kind of session.
//
// The caller owns the returned catalog and should Close it.
func NewCatalog(ctx context.Context, cfg CatalogConfig) (*rest.Catalog, error) {
	uri, err := cfg.URI()
	if err != nil {
		return nil, err
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	// iceberg-go keeps the map it is given and writes the server's /v1/config properties into it,
	// while the indexer builds one catalog per branch concurrently from the same Storage map, so
	// each catalog gets its own copy (a shared map is a fatal "concurrent map writes"). The storage
	// map carries the adls.* properties of the metadata_search.azure section as well as the raw
	// metadata_search.storage keys: pkg/mds merges the former under the latter, so an explicit key
	// still wins, and every catalog this function builds gets the same merged set.
	return rest.NewCatalog(ctx, CatalogName, uri,
		rest.WithHeaders(map[string]string{"Authorization": cfg.Authorization()}),
		rest.WithAdditionalProps(maps.Clone(cfg.Storage)),
		// iceberg-go builds the transport of its session from this factory
		// (rest.WithTransportFactory) and runs the returned cleanup on Close, so the client owns no
		// transport the caller cannot release. The bound is per request, so the /v1/config
		// exchange of NewCatalog is covered as well as every later table call.
		rest.WithTransportFactory(func(tlsConfig *tls.Config) (http.RoundTripper, func()) {
			transport := &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: tlsConfig}
			return &timeoutTransport{base: transport, timeout: timeout}, transport.CloseIdleConnections
		}),
	)
}

// timeoutTransport bounds every request sent through it, including reading the response body.
//
// A context deadline is what carries the bound instead of http.Client.Timeout, because iceberg-go
// builds the *http.Client itself and offers no option for its Timeout. Cancelling the request
// context closes the connection, so a body read that stalls after the headers arrived fails too.
// The deadline lives until the body is closed, which the catalog always does (catalog/rest/rest.go
// do()).
type timeoutTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *timeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(req.Context(), t.timeout)
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = &timeoutBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

var _ http.RoundTripper = (*timeoutTransport)(nil)

// timeoutBody releases the deadline of timeoutTransport when the response is closed.
type timeoutBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *timeoutBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// Identifier returns the identifier of system.object_metadata (§3.1).
func Identifier() icebergtable.Identifier {
	return catalog.ToIdentifier(Namespace, Name)
}

// CreateTable creates system.object_metadata in the catalog's current repository and branch with
// the fixed schema, sort order and properties of §3.1 (design §4.8: an indexer initialises the
// table when a repository is added to its configuration).
//
// extra properties are merged over the defaults of Properties, so a caller can add properties but
// cannot accidentally drop the merge-on-read modes: they are written last.
func CreateTable(ctx context.Context, cat catalog.Catalog, repository string, extra iceberg.Properties) (*icebergtable.Table, error) {
	if cat == nil {
		return nil, ErrNoCatalog
	}
	order, err := SortOrder()
	if err != nil {
		return nil, err
	}
	props := iceberg.Properties{}
	maps.Copy(props, extra)
	// The fixed properties of §3.1 are written last: a caller extends the table, it does not get
	// to turn the merge-on-read delete mode off by accident.
	maps.Copy(props, Properties(repository))

	return cat.CreateTable(ctx, Identifier(), Schema(),
		catalog.WithProperties(props),
		catalog.WithSortOrder(order),
	)
}
