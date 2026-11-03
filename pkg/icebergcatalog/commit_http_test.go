package icebergcatalog

// Tests for the HTTP half of the commit path: POST /v1/{prefix}/namespaces/{ns}/tables/{table}
// (ICEBERG_CATALOG_PLAN.md §2.1 "Commit", §6.2 "Atomic commit", §7.2 "Authorization", §12
// "CAS race", §15.3 "the commit response carries both metadata and metadata-location").
//
// The fixture is the table fixture of tables_test.go - a real catalog store on an in-memory KV
// store, an in-memory block adapter, the ACL auth service and the production middleware stack - so
// a commit here is the commit a client gets: authenticated, authorized, decoded, staged with a
// compare-and-swap, and answered with the LoadTableResult of the spec.
//
// Four behaviours are the point of these tests, and each has a failure mode that is silent if it is
// wrong:
//
//   - a commit works over HTTP at all (the store path of I2-3 was complete but unreachable), and the
//     response carries both metadata and metadata-location: PyIceberg raises after a successful
//     commit when metadata-location is missing (§15.3);
//   - a stale requirement is 409 CommitFailedException, not 400 or 500: only 409 makes a client
//     refresh and retry;
//   - a lost compare-and-swap is 409 too, never 500: 500 would tell the client the outcome is
//     unknown when the commit certainly did not land, and 409 is what it retries;
//   - a write addressed at a tag is 403 ForbiddenException (§14 Q-I4): a tag cannot be committed to.
//
// The concurrent subtest is a real race between two HTTP requests, not a simulation: a catalog
// wrapper holds both pointer swaps until both requests have read the same base table, so the two
// writers demonstrably pass the requirement check against the same state and the only thing that
// decides the winner is the conditional write underneath (§6.2 step 6).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// commitTableBody builds a CommitTableRequest body from raw requirements and updates arrays.
func commitTableBody(requirements, updates string) string {
	return `{"requirements":` + requirements + `,"updates":` + updates + `}`
}

// setPropertiesUpdate is the update every commit test starts from: it changes one table property,
// so "the commit landed" is observable in the metadata document the response and a later load carry.
func setPropertiesUpdate(owner string) string {
	return `[{"action":"set-properties","updates":{"owner":"` + owner + `"}}]`
}

// commitTable sends one commit request as user.
func (f *tablesFixture) commitTable(t *testing.T, namespaceParam, table, user, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, tablesLoadTarget(namespaceParam, table, nil), strings.NewReader(body))
	req.Header.Set("X-Test-User", user)
	req.Header.Set("Content-Type", contentTypeJSON)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// withCommitService returns a copy of the fixture whose router commits through store, with cfg as
// the catalog configuration. The auth service, the table lister and the stored catalog entries are
// the fixture's, so a request still goes through the production middleware stack, the §7.2
// authorization and the real pointer entries - only the store (for a forced race or a different
// body limit) and the configuration are swapped.
func (f *tablesFixture) withCommitService(t *testing.T, cfg *Config, store *Store) *tablesFixture {
	t.Helper()
	service := NewTableService(store, f.authz.authz, f.catalog, cfg)
	router, err := newRouter(tablesUser, cfg, catalogServerWith(cfg, service.Routes()...))
	require.NoError(t, err, "build the catalog router")

	copied := *f
	copied.serve = service
	copied.router = router
	return &copied
}

// tablePropertiesOf decodes the properties member of a metadata document a commit returned.
func tablePropertiesOf(t *testing.T, metadata map[string]json.RawMessage) map[string]string {
	t.Helper()
	properties := make(map[string]string)
	require.NoError(t, json.Unmarshal(metadata["properties"], &properties), "decode the properties")
	return properties
}

// barrierCatalog is the fixture's catalog with a CreateEntry that waits for permit before writing.
// A commit reaches CreateEntry only after it read the base pointer, ran the requirements and wrote
// its metadata file, so holding two commits there makes them race on exactly the compare-and-swap
// of §6.2 step 6 - the state a real pair of concurrent commits produces.
//
// Only the pointer write is held. A commit also stages the table files it added through the same
// CreateEntry before the swap (§6.2 step 5, files.go), and those writes must not count as an
// arrival: the barrier is the swap, not the staging that precedes it.
type barrierCatalog struct {
	Catalog
	arrived func()
	permit  <-chan struct{}
}

func (c *barrierCatalog) CreateEntry(ctx context.Context, repositoryID, branch string, entry catalog.DBEntry, opts ...graveler.SetOptionsFunc) error {
	if strings.HasSuffix(entry.Path, "/"+TablePointerName) {
		c.arrived()
		<-c.permit
	}
	return c.Catalog.CreateEntry(ctx, repositoryID, branch, entry, opts...)
}

// TestCommitTableHTTP drives the commit endpoint over HTTP: the successful commit and its
// LoadTableResult, the two 409 shapes, a real concurrent race, and the refusals a client can hit.
func TestCommitTableHTTP(t *testing.T) {
	t.Run("a commit stages a new metadata file and returns the LoadTableResult", func(t *testing.T) {
		f := newTablesFixture(t)
		books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})
		base := f.tablePointer(t, tableAddressOf(Namespace{"inventory", "books"}, "orders"))
		head := f.tablesCatalogRef(t)

		rec := f.commitTable(t, books, "orders", "carol",
			commitTableBody(`[]`, setPropertiesUpdate("commit-team")))
		body := decodeLoadTable(t, rec)

		// Both members of the commit response are required by PyIceberg (§15.3): the new
		// metadata-location and the document it names.
		require.NotEmpty(t, body.MetadataLocation)
		require.Contains(t, body.MetadataLocation, "/metadata/", "a commit writes a new metadata file")
		require.NotEqual(t, base.MetadataLocation, body.MetadataLocation, "metadata.json is never rewritten in place")
		require.True(t, strings.HasPrefix(body.MetadataLocation, f.storageNamespace+"/_iceberg/"),
			"the metadata file lives under the table's location, got %q", body.MetadataLocation)
		require.Equal(t, "commit-team", tablePropertiesOf(t, body.Metadata)["owner"])
		require.NotNil(t, body.Config, "config is a map, not null")

		// The document that was written is the document that was returned.
		require.Equal(t, tablePropertiesOf(t, body.Metadata), tablePropertiesOfRaw(t, f.tableObject(t, body.MetadataLocation)),
			"the response metadata is the metadata.json the commit wrote")

		// Staged, not committed: the pointer moved on the branch and the branch head did not.
		pointer := f.stagedPointerOf(t, tableAddressOf(Namespace{"inventory", "books"}, "orders"))
		require.Equal(t, body.MetadataLocation, pointer[MetadataLocationKey])
		require.Equal(t, base.TableUUID, pointer[TableUUIDKey], "a commit keeps the table uuid")
		require.Equal(t, head, f.tablesCatalogRef(t), "an Iceberg commit must not commit the lakeFS branch")

		// And the read path serves the committed table immediately.
		loaded := decodeLoadTable(t, f.request(t, http.MethodGet,
			tablesLoadTarget(books, "orders", nil), "alice"))
		require.Equal(t, body.MetadataLocation, loaded.MetadataLocation)
		require.Equal(t, "commit-team", tablePropertiesOf(t, loaded.Metadata)["owner"])
	})

	t.Run("a stale requirement is 409 CommitFailedException and changes nothing", func(t *testing.T) {
		f := newTablesFixture(t)
		books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})
		before := f.stagedPointerOf(t, tableAddressOf(Namespace{"inventory", "books"}, "orders"))

		rec := f.commitTable(t, books, "orders", "carol", commitTableBody(
			`[{"type":"assert-table-uuid","uuid":"`+storeAltUUID+`"}]`,
			setPropertiesUpdate("must-not-land")))

		apiError := requireEnvelope(t, rec, http.StatusConflict, ErrTypeCommitFailed)
		require.Contains(t, apiError.Message, "assert-table-uuid",
			"a client debugging a stale requirement is told which one failed")
		require.NotContains(t, rec.Body.String(), ErrTypeInternalServerError)
		require.Equal(t, before, f.stagedPointerOf(t, tableAddressOf(Namespace{"inventory", "books"}, "orders")),
			"a refused commit must not move the pointer")
	})

	t.Run("a lost compare-and-swap is 409, not 500", func(t *testing.T) {
		f := newTablesFixture(t)
		books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})
		before := f.stagedPointerOf(t, tableAddressOf(Namespace{"inventory", "books"}, "orders"))

		// The pointer moved between the read and the swap: failingCatalog is commit_test.go's
		// catalog whose CreateEntry returns the error the graveler condition produces.
		g := f.withCommitService(t, &Config{Prefix: CatalogPrefix},
			NewStore(&failingCatalog{Catalog: f.catalog, err: graveler.ErrPreconditionFailed}, f.catalog.BlockAdapter))

		rec := g.commitTable(t, books, "orders", "carol", commitTableBody(`[]`, setPropertiesUpdate("loser")))

		apiError := requireEnvelope(t, rec, http.StatusConflict, ErrTypeCommitFailed)
		require.NotContains(t, apiError.Message, "assert-", "the pointer moved; no requirement failed")
		require.NotContains(t, rec.Body.String(), ErrorTypeCommitStateUnknown,
			"a lost race certainly did not land, so it is not an unknown outcome")
		require.Equal(t, before, f.stagedPointerOf(t, tableAddressOf(Namespace{"inventory", "books"}, "orders")),
			"the winner's pointer must survive")
	})

	t.Run("two concurrent commits: exactly one wins and the other gets 409", func(t *testing.T) {
		f := newTablesFixture(t)
		books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})

		var (
			arrived   sync.WaitGroup
			bothReady = make(chan struct{})
			permit    = make(chan struct{})
		)
		arrived.Add(2)
		go func() { arrived.Wait(); close(bothReady) }()

		g := f.withCommitService(t, &Config{Prefix: CatalogPrefix}, NewStore(&barrierCatalog{
			Catalog: f.catalog,
			arrived: func() { arrived.Done() },
			permit:  permit,
		}, f.catalog.BlockAdapter))

		type outcome struct {
			status int
			body   string
			err    error
		}
		results := make(chan outcome, 2)
		commit := func(owner string) {
			req := httptest.NewRequest(http.MethodPost, tablesLoadTarget(books, "orders", nil),
				strings.NewReader(commitTableBody(`[]`, setPropertiesUpdate(owner))))
			req.Header.Set("X-Test-User", "carol")
			req.Header.Set("Content-Type", contentTypeJSON)
			rec := httptest.NewRecorder()
			g.router.ServeHTTP(rec, req)
			results <- outcome{status: rec.Code, body: rec.Body.String()}
		}
		go commit("first")
		go commit("second")

		select {
		case <-bothReady:
		case <-time.After(30 * time.Second):
			close(permit)
			t.Fatal("the two commits never reached the pointer swap: the race was not set up")
		}
		close(permit)

		var statuses []int
		var winnerMetadataLocation string
		for i := 0; i < 2; i++ {
			select {
			case result := <-results:
				statuses = append(statuses, result.status)
				if result.status == http.StatusOK {
					var body loadTableBody
					require.NoError(t, json.Unmarshal([]byte(result.body), &body), "body: %s", result.body)
					winnerMetadataLocation = body.MetadataLocation
				} else {
					require.Equal(t, http.StatusConflict, result.status, "body: %s", result.body)
					var response ErrorResponse
					require.NoError(t, json.Unmarshal([]byte(result.body), &response), "body: %s", result.body)
					require.Equal(t, ErrTypeCommitFailed, response.Error.Type, "body: %s", result.body)
				}
			case <-time.After(60 * time.Second):
				t.Fatal("a racing commit never finished")
			}
		}

		require.ElementsMatch(t, []int{http.StatusOK, http.StatusConflict}, statuses,
			"both commits read the same base, so exactly one may swap the pointer")
		require.Equal(t, winnerMetadataLocation,
			f.stagedPointerOf(t, tableAddressOf(Namespace{"inventory", "books"}, "orders"))[MetadataLocationKey],
			"the pointer names the winner's metadata file")
	})

	t.Run("a commit to a tag is 403 ForbiddenException", func(t *testing.T) {
		f := newTablesFixture(t)
		tagged := tablesNamespaceParam(tablesTestTag, Namespace{"inventory", "books"})

		rec := f.commitTable(t, tagged, "orders", "carol", commitTableBody(`[]`, setPropertiesUpdate("on-a-tag")))

		apiError := requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
		require.Contains(t, apiError.Message, "read-only", "the Q-I4 refusal says why")
	})

	t.Run("a missing table is 404 NoSuchTableException", func(t *testing.T) {
		f := newTablesFixture(t)
		books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})

		rec := f.commitTable(t, books, "invoices", "carol", commitTableBody(`[]`, setPropertiesUpdate("nowhere")))
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("a caller without catalog:UpdateTable is refused before its body is read", func(t *testing.T) {
		f := newTablesFixture(t)
		books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})

		// alice holds the read half of §7.2 only, bob holds nothing; both get the same 403, and a
		// body that would be a 400 for an authorized caller leaks nothing either.
		for _, user := range []string{"alice", "bob"} {
			t.Run(user, func(t *testing.T) {
				rec := f.commitTable(t, books, "orders", user, `{"requirements":`)
				requireEnvelope(t, rec, http.StatusForbidden, ErrTypeForbidden)
			})
		}
	})

	t.Run("a malformed commit body is 400", func(t *testing.T) {
		f := newTablesFixture(t)
		books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})

		for name, body := range map[string]string{
			"empty":            "",
			"truncated json":   `{"updates":`,
			"not an object":    `[]`,
			"unknown action":   commitTableBody(`[]`, `[{"action":"teleport-table"}]`),
			"unknown require":  commitTableBody(`[{"type":"assert-vibes"}]`, `[]`),
			"requirement only": commitTableBody(`[{"type":"assert-table-uuid"}]`, `[]`),
		} {
			t.Run(name, func(t *testing.T) {
				rec := f.commitTable(t, books, "orders", "carol", body)
				requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
			})
		}
	})

	t.Run("set-location is refused with 400", func(t *testing.T) {
		f := newTablesFixture(t)
		books := tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})

		rec := f.commitTable(t, books, "orders", "carol", commitTableBody(`[]`,
			`[{"action":"set-location","location":"mem://elsewhere/table"}]`))
		apiError := requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		require.Contains(t, apiError.Message, "location",
			"the refusal names the update that cannot be served")
	})

	t.Run("a body over iceberg.max_request_body is 413", func(t *testing.T) {
		f := newTablesFixture(t)
		g := f.withCommitService(t, &Config{Prefix: CatalogPrefix, MaxRequestBody: 32},
			NewStore(f.catalog, f.catalog.BlockAdapter))

		rec := g.commitTable(t, booksNamespace(), "orders", "carol", commitTableBody(`[]`, setPropertiesUpdate(
			strings.Repeat("x", 256))))
		apiError := requireEnvelope(t, rec, http.StatusRequestEntityTooLarge, ErrTypeRequestTooLarge)
		require.Contains(t, apiError.Message, "32", "the message names the configured limit")
	})

	t.Run("a prefix other than the catalog prefix is 404 NoSuchWarehouseException", func(t *testing.T) {
		f := newTablesFixture(t)
		req := httptest.NewRequest(http.MethodPost,
			"/v1/not-the-prefix/namespaces/"+booksNamespace()+"/tables/orders",
			strings.NewReader(commitTableBody(`[]`, setPropertiesUpdate("x"))))
		req.Header.Set("X-Test-User", "carol")
		rec := httptest.NewRecorder()
		f.router.ServeHTTP(rec, req)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
	})

	t.Run("config advertises the commit endpoint the router mounts", func(t *testing.T) {
		f := newTablesFixture(t)

		rec := f.request(t, http.MethodGet, configPath, "alice")
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Contains(t, response.Endpoints, commitTableEndpoint,
			"an operation is advertised only when it is mounted")

		// The advertised path is served: a request to it reaches the handler (an empty body is
		// this handler's 400) rather than the router's 404.
		served := f.commitTable(t, booksNamespace(), "orders", "carol", "")
		requireEnvelope(t, served, http.StatusBadRequest, ErrTypeBadRequest)
	})
}

// booksNamespace is the namespace path parameter of inventory/books on the fixture's branch.
func booksNamespace() string {
	return tablesNamespaceParam(tablesTestRef, Namespace{"inventory", "books"})
}

// tablePropertiesOfRaw decodes the owner property of a metadata document read from object storage,
// so a test can compare what a commit returned with what it actually wrote.
func tablePropertiesOfRaw(t *testing.T, document string) map[string]string {
	t.Helper()
	var metadata struct {
		Properties map[string]string `json:"properties"`
	}
	require.NoError(t, json.Unmarshal([]byte(document), &metadata), "decode the written metadata")
	return metadata.Properties
}
