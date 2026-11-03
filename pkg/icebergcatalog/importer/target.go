package importer

// The target half of the import: a client of the lakeFS Iceberg REST catalog.
//
// The catalog is mounted at <lakefs-server>/iceberg/api (ICEBERG_CATALOG_PLAN.md §4.1) and speaks
// the Apache Iceberg REST protocol there, so the importer talks to it with the same protocol
// operations a Spark or PyIceberg client would use:
//
//	GET    /v1/config                                        (prefix, and "is the catalog on")
//	POST   /v1/{prefix}/namespaces                           (CreateNamespace)
//	GET    /v1/{prefix}/namespaces/{namespace}/tables/{table} (LoadTable, for existence)
//	DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table} (DropTable, for --overwrite)
//	POST   /v1/{prefix}/namespaces/{namespace}/register       (RegisterTable)
//
// The catalog's namespace tree carries the repository and the ref as its first two levels
// (docs/iceberg.md, "Addressing"), so every namespace the importer passes here includes them:
// ["repo", "branch", "ns", ...]. The importer never reaches into pkg/icebergcatalog itself.
//
// --overwrite is implemented as drop-then-register because the register operation deliberately
// never replaces an existing table: its pointer write is a compare-and-swap on an absent value
// (pkg/icebergcatalog/register.go). A drop stages the removal of the old pointer and releases the
// table's file entries; the following register stages the new ones, and one explicit lakeFS commit
// publishes both.

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	// lakeFSCatalogBasePath is where the lakeFS server mounts the Iceberg REST catalog. It is a
	// literal rather than an import of pkg/icebergcatalog on purpose: the importer is an HTTP
	// client of the catalog, not a user of its internals.
	lakeFSCatalogBasePath = "/iceberg/api"

	// targetConfigPath is the conversation the client uses to learn the prefix and whether the
	// catalog is enabled.
	targetConfigPath = "/v1/config"

	// macroNamespaceLevels is the number of namespace levels the lakeFS catalog reserves for the
	// repository and the ref (docs/iceberg.md, "Addressing").
	macroNamespaceLevels = 2
)

// TargetConfig is how the lakeFS catalog is reached.
type TargetConfig struct {
	// BaseURL is the catalog base URL, <lakefs-server>/iceberg/api.
	BaseURL string

	// HTTPClient is used for every request; nil means http.DefaultClient.
	HTTPClient *http.Client

	// Authorize adds the lakeFS credentials to a request. It is required in practice: the
	// catalog authenticates every request, including GET /v1/config.
	Authorize func(*http.Request) error
}

// Target is a connection to the lakeFS Iceberg REST catalog.
type Target struct {
	baseURL string
	doer    *doer

	mu         sync.Mutex
	prefix     string
	prefixRead bool
}

// NewTarget validates cfg and builds the client. Like NewSource it does not contact the server
// until the first operation.
func NewTarget(cfg TargetConfig) (*Target, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("%w: a lakeFS catalog URL is required", ErrInvalidOptions)
	}
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	target := &Target{baseURL: baseURL}
	target.doer = &doer{client: client, catalog: catalogTarget, authorizer: cfg.Authorize}
	return target, nil
}

// Prefix asks the catalog for its prefix, which is also the first check that the catalog is
// enabled: a disabled catalog answers 501 in the Iceberg error envelope.
func (t *Target) Prefix(ctx context.Context) (string, error) {
	t.mu.Lock()
	if t.prefixRead {
		prefix := t.prefix
		t.mu.Unlock()
		return prefix, nil
	}
	t.mu.Unlock()

	var config CatalogConfig
	if err := t.doer.do(ctx, http.MethodGet, t.baseURL+targetConfigPath, nil, &config); err != nil {
		return "", err
	}
	prefix := config.Overrides["prefix"]
	if prefix == "" {
		prefix = config.Defaults["prefix"]
	}
	prefix = strings.Trim(prefix, "/")

	t.mu.Lock()
	t.prefix = prefix
	t.prefixRead = true
	t.mu.Unlock()
	return prefix, nil
}

// EnsureNamespace creates the target namespace levels and every ancestor namespace level that may
// be missing. levels are the full catalog namespace, including the repository and the ref as the
// first two levels; markers are created from the third level on, because the first two are the
// repository and the branch and are not namespaces of their own.
//
// A namespace that already exists is not an error: the create answers 409
// NamespaceAlreadyExistsException, and resuming an import must not fail on it.
func (t *Target) EnsureNamespace(ctx context.Context, levels []string) error {
	if len(levels) <= macroNamespaceLevels {
		return fmt.Errorf("%w: a target namespace needs a repository, a ref and at least one namespace level", ErrInvalidOptions)
	}
	prefix, err := t.Prefix(ctx)
	if err != nil {
		return err
	}
	for depth := 3; depth <= len(levels); depth++ {
		if err := t.createNamespace(ctx, prefix, levels[:depth]); err != nil {
			if isConflict(err) {
				continue
			}
			return err
		}
	}
	return nil
}

func (t *Target) createNamespace(ctx context.Context, prefix string, levels []string) error {
	body := struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties,omitempty"`
	}{Namespace: levels}
	return t.doer.do(ctx, http.MethodPost, t.operationURL(prefix, nil, "namespaces"), body, nil)
}

// TableExists reports whether the target catalog already has the table. The existence read is a
// LoadTable: a 404 is the answer "not there", and any other failure is an error, because treating a
// 500 or a 403 as "absent" would turn an authorization problem into a spurious create.
func (t *Target) TableExists(ctx context.Context, levels []string, table string) (bool, error) {
	prefix, err := t.Prefix(ctx)
	if err != nil {
		return false, err
	}
	err = t.doer.do(ctx, http.MethodGet, t.tableURL(prefix, levels, table, nil), nil, nil)
	switch {
	case err == nil:
		return true, nil
	case isNotFound(err):
		return false, nil
	default:
		return false, err
	}
}

// RegisterTable adopts metadataLocation as the table's metadata document
// (RegisterTableRequest: name + metadata-location). The catalog validates the location against the
// repository's storage namespace and stages the table's files; an error here is reported for this
// table and the import moves on.
func (t *Target) RegisterTable(ctx context.Context, levels []string, table, metadataLocation string) error {
	prefix, err := t.Prefix(ctx)
	if err != nil {
		return err
	}
	body := struct {
		Name             string `json:"name"`
		MetadataLocation string `json:"metadata-location"`
	}{Name: table, MetadataLocation: metadataLocation}
	registerURL := t.operationURL(prefix, nil, "namespaces", tableNamespaceParam(levels), "register")
	return t.doer.do(ctx, http.MethodPost, registerURL, body, nil)
}

// DropTable stages the removal of a table, used by --overwrite before the new register. A table
// that disappeared between the existence check and the drop is not an error.
func (t *Target) DropTable(ctx context.Context, levels []string, table string) error {
	prefix, err := t.Prefix(ctx)
	if err != nil {
		return err
	}
	err = t.doer.do(ctx, http.MethodDelete, t.tableURL(prefix, levels, table, nil), nil, nil)
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

// tableURL builds the address of one table, with the table name escaped as a path segment.
func (t *Target) tableURL(prefix string, levels []string, table string, query url.Values) string {
	return t.operationURL(prefix, query, "namespaces", tableNamespaceParam(levels), "tables", url.PathEscape(table))
}

// operationURL builds <base>/v1/[<prefix>/]parts[?query]. The parts are already escaped.
func (t *Target) operationURL(prefix string, query url.Values, parts ...string) string {
	var segments []string
	segments = append(segments, "v1")
	if prefix != "" {
		segments = append(segments, url.PathEscape(prefix))
	}
	segments = append(segments, parts...)
	target := t.baseURL + "/" + strings.Join(segments, "/")
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	return target
}

// LakeFSCatalogURL builds the catalog base URL of a lakeFS server endpoint. It is exported so the
// command does not have to know the mount path, and so a test can point a Target at a test server.
func LakeFSCatalogURL(endpoint string) string {
	return strings.TrimRight(strings.TrimSpace(endpoint), "/") + lakeFSCatalogBasePath
}
