// Package conformance_test holds the external conformance suites of ICEBERG_CATALOG_PLAN.md §12
// and the I-2 exit criteria: iceberg-go's catalog/catalogtest, driven over the network against a
// running lakefs-sso instance, and (documented in README.md) the Apache REST Compatibility Kit.
//
// Conformance here means a suite we did not write, run against the catalog the way a client sees
// it - a real HTTP server, a real OAuth token, real lakeFS commits - rather than against the
// handlers in a Go test. That is what makes it an exit gate: pkg/icebergcatalog's own tests
// exercise the handlers we chose to call, while catalogtest specifies the behaviour every
// iceberg-go catalog must have, in iceberg-go's words and with iceberg-go's client
// (https://github.com/apache/iceberg-go/tree/main/catalog/catalogtest).
//
// The suite is skipped unless it is pointed at a server. Configuration is three environment
// variables, matching the naming the plan fixed (§12):
//
//	REST_CATALOG_URI         the catalog URI, i.e. https://<lakefs-sso>/iceberg/relative_to/<repo>.<branch>/api
//	REST_CATALOG_CREDENTIAL  <access_key_id>:<secret_access_key>, exchanged at /v1/oauth/tokens
//	REST_CATALOG_WAREHOUSE   the warehouse the client names, <repo>.<branch> for lakeFS
//
// README.md has the exact commands, including the repository to create first and the RCK run.
//
// Clean-room note (ICEBERG_CATALOG_PLAN.md §2, DATASETS_DESIGN.md §2): written from
// ICEBERG_CATALOG_PLAN.md §12, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml), the REST
// catalog documentation (https://github.com/apache/iceberg/blob/main/docs/docs/rest-catalog.md),
// the iceberg-go catalogtest package documentation and the public lakeFS documentation only. No
// lakeFS Enterprise code and no upstream lakeFS >= 1.87 (BSL) code was used. iceberg-go is
// Apache-2.0 and is already a dependency of pkg/icebergcatalog (go.mod).
package conformance_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/catalogtest"
	"github.com/apache/iceberg-go/table"

	// Blank import: catalog.Load resolves "type": "rest" through the catalog registry, and
	// catalog/rest is what registers "rest" (and the http/https aliases) in its init. Without this
	// import Load fails with "catalog not found: rest" - the conformance suite would look like a
	// broken catalog rather than a missing import.
	_ "github.com/apache/iceberg-go/catalog/rest"
)

const (
	// The environment variables the suite is configured by (§12).
	CatalogURIEnv        = "REST_CATALOG_URI"
	CatalogCredentialEnv = "REST_CATALOG_CREDENTIAL"
	CatalogWarehouseEnv  = "REST_CATALOG_WAREHOUSE"

	// catalogName is the name the catalog is loaded under. It only labels the client (iceberg-go
	// looks its own configuration file up by name), so it is not part of the address.
	catalogName = "lakefs"

	// relativeToPath is the segment of the relative_to addressing form (§5 rows 4 and 5). The
	// catalog is required to be reached through it, not through /iceberg/api; see catalogEnv.
	relativeToPath = "/relative_to/"
)

// TestExternalCatalogConformance runs iceberg-go's catalogtest suite against the lakeFS catalog
// named by the environment.
//
// catalogtest hands every subtest its own catalog (Config.NewCatalog), and each subtest names its
// namespace, and the table in it, after a fresh UUID. That is deliberate on catalogtest's side -
// a REST catalog outlives a single test - and it means this suite may run against a repository
// that already holds data: it never touches a name it did not mint, and every subtest drops what
// it created. A subtest that fails before its cleanup leaves its namespace staged on the branch
// (catalog writes are staged, not committed, §6.2); those leftovers are inert, since the names
// are never reused.
func TestExternalCatalogConformance(t *testing.T) {
	uri, credential, warehouse := catalogEnv(t)

	catalogtest.RunCatalogTests(t, catalogtest.Config{
		// Namespace properties are part of the I-2 scope (§11: "properties"): namespaces.go
		// stores them in the namespace marker's user metadata and serves create-with-properties,
		// load, set, update and remove. catalogtest skips the property tests when this is false,
		// so claiming true here is claiming the behaviour, and the suite then checks it.
		SupportsNamespaceProperties: true,
		NewCatalog: func(t *testing.T) catalog.Catalog {
			t.Helper()

			// catalog.Load builds the same client any other iceberg-go user builds from these
			// properties: a REST session that negotiates GET /v1/config, takes the prefix from
			// overrides, and mints a token from `credential` at /v1/oauth/tokens. Using Load
			// rather than rest.NewCatalog also covers the registry path, i.e. the "type": "rest"
			// dispatch a client actually goes through.
			cat, err := catalog.Load(context.Background(), catalogName, iceberg.Properties{
				"type":       "rest",
				"uri":        uri,
				"credential": credential,
				"warehouse":  warehouse,
			})
			if err != nil {
				t.Fatalf("load the REST catalog at %s: %v", uri, err)
			}
			t.Cleanup(func() {
				if closer, ok := cat.(catalog.Closer); ok {
					_ = closer.Close()
				}
			})

			return cat
		},
	})
}

// catalogEnv reads and validates the three environment variables, and skips the suite when it has
// not been configured.
//
// The distinction the validation draws is between absent and half-configured. Unset is the normal
// case - this suite is not part of `make test`, because no lakefs-sso server is running there -
// and skips. Setting some of the variables but not all of them is a mistake in the invocation
// rather than an absent suite, and is reported as a failure with the variables that are missing:
// a suite that silently skips because a variable was misspelled reads exactly like a suite that
// passed.
func catalogEnv(t *testing.T) (uri, credential, warehouse string) {
	t.Helper()

	uri = strings.TrimRight(strings.TrimSpace(os.Getenv(CatalogURIEnv)), "/")
	credential = strings.TrimSpace(os.Getenv(CatalogCredentialEnv))
	warehouse = strings.TrimSpace(os.Getenv(CatalogWarehouseEnv))

	if uri == "" && credential == "" && warehouse == "" {
		t.Skipf("set %s, %s and %s to run the iceberg-go conformance suite against a lakefs-sso catalog; see pkg/icebergcatalog/conformance/README.md",
			CatalogURIEnv, CatalogCredentialEnv, CatalogWarehouseEnv)
	}

	var missing []string
	for _, env := range []struct {
		name  string
		value string
	}{
		{CatalogURIEnv, uri},
		{CatalogCredentialEnv, credential},
		{CatalogWarehouseEnv, warehouse},
	} {
		if env.value == "" {
			missing = append(missing, env.name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%s must be set together with the rest of the conformance configuration; missing: %s",
			CatalogURIEnv, strings.Join(missing, ", "))
	}

	// The warehouse is an opaque name to lakeFS: the catalog is addressed by the identifier, and
	// GET /v1/config ignores the `warehouse` query parameter (http.go handleConfig, §5). It is
	// still required here, because a REST catalog that scopes by warehouse needs it and the same
	// harness runs against those. The value must name the same scope as the URI, or the suite
	// would be describing a warehouse the requests never reach, so the check is that the URI
	// names the warehouse.
	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		t.Fatalf("%s must be an http:// or https:// catalog URI, got %q", CatalogURIEnv, uri)
	}
	if !strings.Contains(uri, relativeToPath) {
		t.Fatalf("%s must be the catalog's relative_to form, e.g. https://lakefs.example/iceberg/relative_to/<repo>.<branch>/api, got %q: "+
			"catalogtest names its namespaces after nothing but a UUID, so the repository and the branch have to be fixed by the URL "+
			"(ICEBERG_CATALOG_PLAN.md §5 rows 4-5); /iceberg/api would read the UUID as a repository name and answer 404 NoSuchWarehouseException",
			CatalogURIEnv, uri)
	}
	if !strings.HasSuffix(uri, "/iceberg/relative_to/"+warehouse+"/api") {
		t.Fatalf("%s (%q) does not name the warehouse %s names (%q); they must name the same <repo>.<branch>",
			CatalogURIEnv, uri, CatalogWarehouseEnv, warehouse)
	}
	if !strings.Contains(credential, ":") {
		t.Fatalf("%s must be <access_key_id>:<secret_access_key>, the pair the client posts to /v1/oauth/tokens (RFC 6749 §4.4.2)", CatalogCredentialEnv)
	}

	return uri, credential, warehouse
}

// CatalogMetadataSearchEnv switches on the Metadata Search conformance check and names nothing by
// itself: the data repository is the one the warehouse already names. Any non-empty value enables
// the test, because the only thing it can be is a statement that the installation under test has
// Metadata Search enabled AND indexed the warehouse's repository.
const CatalogMetadataSearchEnv = "REST_CATALOG_METADATA_SEARCH"

// TestExternalMetadataSearchVirtualTable is the external half of METADATA_SEARCH_DESIGN.md §5.2-5.4:
// the suite catalogtest cannot reach, because a virtual table is a lakeFS extension rather than a
// REST operation every catalog has.
//
// It runs the same client catalogtest does, over the same relative_to URI, and asserts the three
// things a client sees: the data repository's ref lists the `system` namespace, that namespace
// lists `object_metadata`, and the table loads. The catalog resolves all three out of the
// companion repository (<repo>-metadata), which the client never names and needs no access to
// (§5.3) - the credential used here is the one the rest of the suite uses.
//
// It is skipped unless REST_CATALOG_METADATA_SEARCH is set, since a catalog without the feature
// answers 404 for all three and an installation only has the table after the indexer ran. README.md
// has the invocation.
func TestExternalMetadataSearchVirtualTable(t *testing.T) {
	uri, credential, warehouse := catalogEnv(t)
	if strings.TrimSpace(os.Getenv(CatalogMetadataSearchEnv)) == "" {
		t.Skipf("set %s to run the Metadata Search virtual-table check against a catalog with lakefs.metadata_search.enabled and an indexed warehouse; see README.md",
			CatalogMetadataSearchEnv)
	}

	ctx := context.Background()
	cat, err := catalog.Load(ctx, catalogName, iceberg.Properties{
		"type":       "rest",
		"uri":        uri,
		"credential": credential,
		"warehouse":  warehouse,
	})
	if err != nil {
		t.Fatalf("load the REST catalog at %s: %v", uri, err)
	}
	if closer, ok := cat.(catalog.Closer); ok {
		t.Cleanup(func() { _ = closer.Close() })
	}

	// §5.4: the data repository's ref lists the reserved namespace.
	namespaces, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		t.Fatalf("list namespaces of %s: %v", warehouse, err)
	}
	var hasSystem bool
	for _, namespace := range namespaces {
		if len(namespace) == 1 && namespace[0] == "system" {
			hasSystem = true
		}
	}
	if !hasSystem {
		t.Fatalf("the warehouse %s does not list the Metadata Search namespace %q: %v (is iceberg.metadata_search.enabled set, and has the indexer run?)",
			warehouse, "system", namespaces)
	}

	// §5.4: the namespace lists the virtual table, although no pointer entry exists for it.
	var tables []table.Identifier
	for identifier, err := range cat.ListTables(ctx, table.Identifier{"system"}) {
		if err != nil {
			t.Fatalf("list tables of system in %s: %v", warehouse, err)
		}
		tables = append(tables, identifier)
	}
	var hasObjectMetadata bool
	for _, identifier := range tables {
		if len(identifier) > 0 && identifier[len(identifier)-1] == "object_metadata" {
			hasObjectMetadata = true
		}
	}
	if !hasObjectMetadata {
		t.Fatalf("the namespace %q of %s does not list %q: %v", "system", warehouse, "object_metadata", tables)
	}

	// §5.2/§5.3: the load resolves the table out of the companion repository with the same
	// credential, which has no grant on that repository.
	loaded, err := cat.LoadTable(ctx, table.Identifier{"system", "object_metadata"})
	if err != nil {
		t.Fatalf("load system.object_metadata of %s: %v", warehouse, err)
	}
	if loaded.Location() == "" {
		t.Fatal("the loaded table has no location: the response is not a table metadata document")
	}
	if loaded.Schema() == nil || len(loaded.Schema().Fields()) == 0 {
		t.Fatalf("the loaded table has no schema: %+v", loaded.Schema())
	}
	if loaded.MetadataLocation() == "" {
		t.Fatal("the loaded table has no metadata-location")
	}
}
