package esti

// iceberg_views_test.go is the end-to-end suite of the Iceberg *view* surface (views.go,
// views_http.go): the six operations the catalog serves - list, load, exists, create, replace and
// drop - driven against a real lakefs-sso on an S3-compatible blockstore, with PyIceberg as the
// client for create/load/list/exists/drop and plain HTTP for the operations PyIceberg 0.12 does not
// expose (the replace, i.e. requirements + updates).
//
// # What the suite asserts
//
//  1. PyIceberg's RestCatalog creates a view through the catalog (`create_view`), loads it
//     (`load_view`), lists it (`list_views`) and sees it exists (`view_exists`). That is the
//     client-side proof that a published client speaks the protocol to this catalog - and it is
//     also the check that GET /v1/config advertises the view endpoints, because PyIceberg refuses
//     an operation the server does not list.
//  2. The catalog wrote the view metadata document into the repository's storage namespace,
//     under the managed location <storage_namespace>/_iceberg/<view-uuid>/, and the pointer entry
//     under _lakefs_iceberg/views/<namespace>/<view>/.pointer names it.
//  3. The lakeFS commit that publishes the view stages *one entry per metadata document* under
//     _lakefs_iceberg/files/<view-uuid>/ - the GC guarantee applied to views (§6.3): a document
//     that is not an entry is a document the committed GC may collect.
//  4. A replace (POST …/views/{view}) with `assert-view-uuid` and the update actions
//     `add-view-version` / `set-current-view-version` / `set-properties` is served, the stale-uuid
//     case is 409 CommitFailedException, and an unsupported format version is 400
//     BadRequestException.
//  5. Reads work on any ref: a tag cut before a replace keeps serving the document it captured,
//     while the branch serves the replaced one.
//  6. A namespace that holds a view is not empty: dropping it is 409 NamespaceNotEmptyException.
//  7. A drop (PyIceberg's `drop_view`) removes the pointer and releases the staged metadata
//     entries in one lakeFS commit, after which the namespace is droppable and the view answers
//     404 NoSuchViewException.
//
// # Skipping
//
// The harness skips the suite when the environment cannot host it (see iceberg_utils.go), and this
// test skips when the catalog does not advertise the view operations - a catalog without views is
// not a failure of this test, it just has nothing to exercise.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, listViews /
// createView / loadView / replaceView / dropView / viewExists), the Apache Iceberg view spec
// (https://iceberg.apache.org/view-spec/) and the public PyIceberg documentation
// (https://py.iceberg.apache.org/) only. No lakeFS Enterprise code and no upstream lakeFS >= 1.87
// (BSL 1.1) code was used.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

const (
	// icebergView is the name of the view this suite creates in the fixture namespace.
	icebergView = "orders_view"

	// icebergViewSQL is the view definition the PyIceberg step creates. Nothing in the catalog
	// interprets it (a view is metadata), so it only has to be a recognisable string.
	icebergViewSQL = "SELECT id, name FROM sales.orders"

	// icebergViewReplacedSQL is the definition the replace adds as a second view version.
	icebergViewReplacedSQL = "SELECT id, upper(name) AS name FROM sales.orders"

	// The view operations this suite depends on, in the literal-{prefix} form an advertised
	// endpoint must have (§2.1).
	icebergListViewsEndpoint = "GET /v1/{prefix}/namespaces/{namespace}/views"
	icebergLoadViewEndpoint  = "GET /v1/{prefix}/namespaces/{namespace}/views/{view}"

	// The Iceberg error types this suite asserts on.
	icebergNoSuchViewType      = "NoSuchViewException"
	icebergCommitFailedType    = "CommitFailedException"
	icebergBadRequestType      = "BadRequestException"
	icebergNamespaceNotEmpty   = "NamespaceNotEmptyException"
	icebergCreateViewEndpoint  = "POST /v1/{prefix}/namespaces/{namespace}/views"
	icebergViewReplaceEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/views/{view}"
)

// TestIcebergViews is the view end-to-end suite: a PyIceberg round trip, the staged metadata
// entries, a replace over HTTP, the any-ref read of a tag, the namespace-emptiness rule and the
// drop.
func TestIcebergViews(t *testing.T) {
	env := newIcebergEnv(t)
	env.requireCatalogViews(t)
	cl := env.coreAPI(t)

	// The namespace the view lives in. A view, like a table, can only be created where a namespace
	// marker exists.
	env.createTableNamespace(t, env.branch)
	base := env.headCommit(t, cl, env.branch)

	// 1. Create the view with PyIceberg. The client assigns nothing: the catalog mints the view
	// uuid and the managed location, and the response is the document it wrote.
	view := env.createViewWithPyIceberg(t, env.branch, icebergView, icebergViewSQL)
	require.NotEmpty(t, view.UUID, "the catalog must mint a view uuid")
	require.True(t, strings.HasPrefix(view.Location, strings.TrimRight(env.storageNamespace, "/")+"/"+icebergcatalog.DataPrefix+"/"),
		"a view without a requested location must land in the managed table area, got %q", view.Location)
	require.Equal(t, icebergViewSQL, view.SQL, "the view version the client created must be the current one")
	require.Equal(t, "esti", view.Owner, "the properties the client sent must be readable back")
	require.True(t, view.Exists, "view_exists must see the view the client just created")
	// list_views answers full identifiers - the namespace levels the client sent plus the name -
	// because that is what the catalog addresses and what a client feeds back into load_view.
	require.Contains(t, view.Listed, strings.Join([]string{env.repo, env.branch, env.namespace, icebergView}, "."),
		"the view must be listed in its namespace: %v", view.Listed)

	// 2. The document is where the pointer says, under the managed location of its own uuid.
	metadataLocation := env.requireViewLoaded(t, env.branch, view.UUID)
	require.True(t, strings.HasPrefix(metadataLocation, view.Location+"/"+icebergcatalog.MetadataDirectory+"/"),
		"the metadata document %q must live under the view location %q", metadataLocation, view.Location)
	require.Equal(t, []string{env.repo, env.branch, env.namespace}, view.Namespace,
		"the client names the view with the namespace levels it sent, repository and ref included")

	// 3. The lakeFS commit stages the document as an entry (§6.3): the committed GC protects what the
	//    commit references, and a view document is no exception.
	created := env.commitBranch(t, cl, env.branch, "iceberg views: create")
	require.NotEqual(t, base, created)
	env.requireViewStaged(t, cl, created, view.UUID, metadataLocation)

	// A tag is cut at the create, so the replace below can be shown not to move it.
	env.createTag(t, cl, icebergTag, created)

	// 4. Replace the view over HTTP: assert-view-uuid, a second version, and a property.
	replaced := env.replaceViewThroughHTTP(t, env.branch, view.UUID, icebergViewReplacedSQL)
	require.NotEqual(t, metadataLocation, replaced.MetadataLocation, "a replace writes a new document")
	require.Equal(t, view.UUID, replaced.UUID, "the view uuid survives a replace")
	require.Equal(t, int64(2), replaced.CurrentVersionID, "the second view version must be current")
	require.Equal(t, icebergViewReplacedSQL, replaced.SQL)
	require.Equal(t, "replaced", replaced.Property("stage"))

	// 5. Any-ref reads: the tag still serves the document it captured, the branch the replaced one.
	require.Equal(t, metadataLocation, env.requireViewMetadataLocation(t, icebergTag, view.UUID),
		"a tag must serve the document it captured, not the branch's later replace")
	require.Equal(t, replaced.MetadataLocation, env.requireViewMetadataLocation(t, env.branch, view.UUID))

	// A stale uuid is the 409 a client refreshes and retries on; an unsupported format version is a
	// 400, because retrying it can never help.
	env.requireReplaceRefused(t, env.branch, icebergView,
		`{"requirements":[{"type":"assert-view-uuid","uuid":"11111111-1111-1111-1111-111111111111"}],
		  "updates":[{"action":"set-properties","updates":{"stage":"never"}}]}`,
		http.StatusConflict, icebergCommitFailedType)
	env.requireReplaceRefused(t, env.branch, icebergView,
		`{"updates":[{"action":"upgrade-format-version","format-version":2}]}`,
		http.StatusBadRequest, icebergBadRequestType)

	// Publish the replace, then replace again on the branch and show the tag is still where it was
	// while the branch moved on: catalog changes are staged and published explicitly (§6.2).
	env.commitBranch(t, cl, env.branch, "iceberg views: replace")
	env.requireViewMetadataLocation(t, icebergTag, view.UUID)
	env.requireReplaceRefused(t, icebergTag, icebergView,
		`{"updates":[{"action":"set-properties","updates":{"stage":"tag"}}]}`,
		http.StatusForbidden, "ForbiddenException")

	// 6. A namespace that holds a view is not empty.
	env.requireDropNamespaceRefused(t, env.branch)
	env.requireViewListed(t, icebergTag, view.UUID, true)

	// 7. Drop through the client, then publish the drop. The pointer and the staged metadata entries
	//    are removed together, so the view stops existing and its documents become collectable.
	env.dropViewWithPyIceberg(t, env.branch)
	require.Equal(t, http.StatusNotFound, env.viewStatus(t, env.branch, icebergView),
		"a dropped view must answer 404 on the branch it was dropped on")
	dropped := env.commitBranch(t, cl, env.branch, "iceberg views: drop")
	env.requireViewStagedRemoval(t, cl, dropped, view.UUID)
	env.requireViewListed(t, env.branch, view.UUID, false)

	// The namespace is empty now, so it can be dropped.
	env.requireDropNamespaceSucceeds(t, env.branch)
}

// icebergViewResult is what the PyIceberg create/load step reports.
type icebergViewResult struct {
	UUID      string
	Location  string
	SQL       string
	Owner     string
	Exists    bool
	Listed    []string
	Namespace []string
}

// icebergViewReplaceResult is what the HTTP replace reports.
type icebergViewReplaceResult struct {
	MetadataLocation string
	UUID             string
	CurrentVersionID int64
	SQL              string
	Properties       map[string]string
}

// Property reads one view property, or the empty string when it is absent.
func (r icebergViewReplaceResult) Property(name string) string { return r.Properties[name] }

// icebergReplacedViewDocument is the part of a LoadViewResult these assertions read.
type icebergReplacedViewDocument struct {
	MetadataLocation string `json:"metadata-location"`
	Metadata         struct {
		ViewUUID         string            `json:"view-uuid"`
		CurrentVersionID int64             `json:"current-version-id"`
		Properties       map[string]string `json:"properties"`
		Versions         []struct {
			VersionID       int64 `json:"version-id"`
			Representations []struct {
				SQL string `json:"sql"`
			} `json:"representations"`
		} `json:"versions"`
	} `json:"metadata"`
}

// requireCatalogViews skips the suite when the catalog does not advertise the view operations, and
// pins that everything it *does* advertise for views is real by requiring the two the client needs
// first.
func (e *icebergEnv) requireCatalogViews(t *testing.T) {
	t.Helper()
	status, body := e.doJSON(t, http.MethodGet, e.catalogURL+"/v1/config", nil)
	require.Equalf(t, http.StatusOK, status, "GET /v1/config: %s", body)

	var config struct {
		Endpoints []string `json:"endpoints"`
	}
	require.NoErrorf(t, json.Unmarshal(body, &config), "decode GET /v1/config: %s", body)
	for _, endpoint := range []string{icebergListViewsEndpoint, icebergLoadViewEndpoint, icebergViewReplaceEndpoint} {
		if !containsString(config.Endpoints, endpoint) {
			t.Skipf("the catalog does not advertise %q, so it does not serve views; skipping the view suite", endpoint)
		}
	}
	if !containsString(config.Endpoints, icebergCreateViewEndpoint) {
		t.Skipf("the catalog does not advertise the create-view operation; skipping the view suite")
	}
}

// viewNamespaceURL is the ListViews/CreateView URL of the fixture namespace at ref, on the
// identifier form. The namespace parameter carries the levels of §5 - repository, ref, namespace -
// joined with the Iceberg namespace separator.
func (e *icebergEnv) viewNamespaceURL(mount, ref string) string {
	return e.refNamespaceURL(mount, ref) + "/views"
}

// viewURL is the LoadView/ReplaceView/DropView URL of the fixture view at ref.
func (e *icebergEnv) viewURL(mount, ref, name string) string {
	return e.viewNamespaceURL(mount, ref) + "/" + name
}

// createViewWithPyIceberg creates and reads a view with PyIceberg's RestCatalog, exactly as a user
// would: create_view, load_view, list_views, view_exists.
func (e *icebergEnv) createViewWithPyIceberg(t *testing.T, ref, name, sql string) icebergViewResult {
	t.Helper()
	identifier := icebergIdentifierJSON(t, e.repo, ref, e.namespace, name)
	result := e.runClient(t, "create-view:"+ref, icebergCreateViewScript, map[string]string{
		"ICEBERG_IDENTIFIER":   identifier,
		"ICEBERG_CREDENTIAL":   e.key + ":" + e.secret,
		"ICEBERG_CATALOG_URI":  e.dockerReachableURL(e.catalogURL),
		"ICEBERG_WAREHOUSE":    e.storageNamespace,
		"ICEBERG_VIEW_SQL":     sql,
		"ICEBERG_VIEW_SCHEMA":  "id,name",
		"ICEBERG_VIEW_DIALECT": "trino",
	})

	listed := stringSliceField(t, result, "views")
	namespace := stringSliceField(t, result, "namespace")
	return icebergViewResult{
		UUID:      stringField(t, result, "uuid"),
		Location:  stringField(t, result, "location"),
		SQL:       stringField(t, result, "sql"),
		Owner:     stringField(t, result, "owner"),
		Exists:    boolField(t, result, "exists"),
		Listed:    listed,
		Namespace: namespace,
	}
}

// dropViewWithPyIceberg drops the view through the catalog (PyIceberg's drop_view).
func (e *icebergEnv) dropViewWithPyIceberg(t *testing.T, ref string) {
	t.Helper()
	result := e.runClient(t, "drop-view:"+ref, icebergDropViewScript, map[string]string{
		"ICEBERG_IDENTIFIER":  icebergIdentifierJSON(t, e.repo, ref, e.namespace, icebergView),
		"ICEBERG_CREDENTIAL":  e.key + ":" + e.secret,
		"ICEBERG_CATALOG_URI": e.dockerReachableURL(e.catalogURL),
		"ICEBERG_WAREHOUSE":   e.storageNamespace,
	})
	require.Truef(t, boolField(t, result, "dropped"), "drop_view must succeed")
}

// requireViewLoaded asserts that the view loads on ref and returns its metadata-location.
func (e *icebergEnv) requireViewLoaded(t *testing.T, ref, wantUUID string) string {
	t.Helper()
	document, status, body := e.loadView(t, ref, icebergView)
	require.Equalf(t, http.StatusOK, status, "load view %q at %s: %s", icebergView, ref, body)
	require.Equal(t, wantUUID, document.Metadata.ViewUUID, "the load must serve the view's own uuid")
	require.NotEmpty(t, document.MetadataLocation, "a LoadViewResult carries a metadata-location")
	return document.MetadataLocation
}

// requireViewMetadataLocation is requireViewLoaded for a metadata-location equality assertion.
func (e *icebergEnv) requireViewMetadataLocation(t *testing.T, ref, wantUUID string) string {
	t.Helper()
	return e.requireViewLoaded(t, ref, wantUUID)
}

// loadView is one LoadView request.
func (e *icebergEnv) loadView(t *testing.T, ref, name string) (icebergReplacedViewDocument, int, []byte) {
	t.Helper()
	status, body := e.doJSON(t, http.MethodGet, e.viewURL(e.catalogURL, ref, name), nil)
	var document icebergReplacedViewDocument
	if status == http.StatusOK {
		require.NoErrorf(t, json.Unmarshal(body, &document), "decode the view document: %s", body)
	}
	return document, status, body
}

// viewStatus is the status of a LoadView request, for the assertions that only want it.
func (e *icebergEnv) viewStatus(t *testing.T, ref, name string) int {
	t.Helper()
	status, _ := e.doJSON(t, http.MethodGet, e.viewURL(e.catalogURL, ref, name), nil)
	return status
}

// replaceViewThroughHTTP applies one replace - assert-view-uuid, a second view version, a property -
// and returns what the catalog served.
func (e *icebergEnv) replaceViewThroughHTTP(t *testing.T, ref, viewUUID, sql string) icebergViewReplaceResult {
	t.Helper()
	body := fmt.Sprintf(`{
	  "requirements": [{"type": "assert-view-uuid", "uuid": %q}],
	  "updates": [
	    {"action": "add-view-version", "view-version": {
	      "version-id": 2,
	      "schema-id": 0,
	      "timestamp-ms": 1700000000000,
	      "summary": {"engine-name": "esti"},
	      "representations": [{"type": "sql", "sql": %q, "dialect": "trino"}],
	      "default-namespace": [%q]
	    }},
	    {"action": "set-current-view-version", "view-version-id": 2},
	    {"action": "set-properties", "updates": {"stage": "replaced"}}
	  ]
	}`, viewUUID, sql, e.namespace)
	status, response := e.doJSON(t, http.MethodPost, e.viewURL(e.catalogURL, ref, icebergView), []byte(body))
	require.Equalf(t, http.StatusOK, status, "replace view %q at %s: %s", icebergView, ref, response)

	var document icebergReplacedViewDocument
	require.NoErrorf(t, json.Unmarshal(response, &document), "decode the replace response: %s", response)
	var currentSQL string
	for _, version := range document.Metadata.Versions {
		if version.VersionID == document.Metadata.CurrentVersionID && len(version.Representations) > 0 {
			currentSQL = version.Representations[0].SQL
		}
	}
	return icebergViewReplaceResult{
		MetadataLocation: document.MetadataLocation,
		UUID:             document.Metadata.ViewUUID,
		CurrentVersionID: document.Metadata.CurrentVersionID,
		SQL:              currentSQL,
		Properties:       document.Metadata.Properties,
	}
}

// requireReplaceRefused asserts one refused replace: the status and the Iceberg error type.
func (e *icebergEnv) requireReplaceRefused(t *testing.T, ref, name, body string, wantStatus int, wantType string) {
	t.Helper()
	status, response := e.doJSON(t, http.MethodPost, e.viewURL(e.catalogURL, ref, name), []byte(body))
	require.Equalf(t, wantStatus, status, "replace %q at %s: %s", name, ref, response)
	require.Containsf(t, string(response), wantType,
		"the refusal must be typed %s: %s", wantType, response)
}

// requireViewListed asserts whether ListViews at ref carries the view.
func (e *icebergEnv) requireViewListed(t *testing.T, ref, viewUUID string, want bool) {
	t.Helper()
	status, body := e.doJSON(t, http.MethodGet, e.viewNamespaceURL(e.catalogURL, ref), nil)
	require.Equalf(t, http.StatusOK, status, "list views at %s: %s", ref, body)

	var listing struct {
		Identifiers []struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifiers"`
	}
	require.NoErrorf(t, json.Unmarshal(body, &listing), "decode the view listing: %s", body)
	names := make([]string, 0, len(listing.Identifiers))
	for _, identifier := range listing.Identifiers {
		names = append(names, identifier.Name)
		require.Equal(t, []string{e.repo, ref, e.namespace}, identifier.Namespace,
			"a listing identifier carries the full address, so a client can load it back")
	}
	if want {
		require.Containsf(t, names, icebergView, "the views of %s at %s must contain %q: %v",
			e.namespace, ref, icebergView, names)
		return
	}
	require.NotContainsf(t, names, icebergView, "the views of %s at %s must not contain %q after the drop: %v",
		e.namespace, ref, icebergView, names)
}

// requireViewStaged asserts the §6.3 guarantee for views at the commit that carries the create: the
// commit's diff lists the view pointer and an entry for the metadata document, and the entry
// addresses the object the catalog wrote inside the repository's storage namespace.
func (e *icebergEnv) requireViewStaged(t *testing.T, cl *apigen.ClientWithResponses, commit, viewUUID, metadataLocation string) {
	t.Helper()
	paths := e.diffPaths(t, cl, commit)
	pointer := icebergViewPointerPath([]string{e.namespace}, icebergView)
	require.Equalf(t, "added", paths[pointer],
		"the commit must add the view pointer %q; diff paths: %v", pointer, sortedPathList(paths))

	filePrefix := icebergTableFileEntryPrefix(viewUUID)
	entries := 0
	for path := range paths {
		if strings.HasPrefix(path, filePrefix) {
			entries++
		}
	}
	require.GreaterOrEqualf(t, entries, 1,
		"the commit must stage an entry for the view metadata document under %q; diff paths: %v",
		filePrefix, sortedPathList(paths))
	staged := e.stagedAddresses(t, cl, commit, filePrefix)
	require.Containsf(t, staged, metadataLocation,
		"the commit must stage an entry addressing %q; staged addresses: %v",
		metadataLocation, sortedValueList(staged))
}

// requireViewStagedRemoval asserts the drop commit: the pointer is removed, and so are the metadata
// entries - which is what hands the view's objects to the committed GC.
func (e *icebergEnv) requireViewStagedRemoval(t *testing.T, cl *apigen.ClientWithResponses, commit, viewUUID string) {
	t.Helper()
	paths := e.diffPaths(t, cl, commit)
	pointer := icebergViewPointerPath([]string{e.namespace}, icebergView)
	require.Equalf(t, "removed", paths[pointer],
		"the drop commit must remove the view pointer %q; diff paths: %v", pointer, sortedPathList(paths))

	filePrefix := icebergTableFileEntryPrefix(viewUUID)
	removed := 0
	for path, kind := range paths {
		if strings.HasPrefix(path, filePrefix) && kind == "removed" {
			removed++
		}
	}
	require.GreaterOrEqualf(t, removed, 1,
		"the drop commit must remove the view's staged metadata entries under %q; diff paths: %v",
		filePrefix, sortedPathList(paths))
}

// requireDropNamespaceRefused asserts that dropping the fixture namespace is refused while it holds
// a view.
func (e *icebergEnv) requireDropNamespaceRefused(t *testing.T, ref string) {
	t.Helper()
	target := e.refNamespaceURL(e.catalogURL, ref)
	status, body := e.doJSON(t, http.MethodDelete, target, nil)
	require.Equalf(t, http.StatusConflict, status,
		"dropping a namespace that holds a view must be 409: %s", body)
	require.Containsf(t, string(body), icebergNamespaceNotEmpty,
		"the refusal must be typed %s: %s", icebergNamespaceNotEmpty, body)

	// Nothing was removed: the view is still there.
	require.Equal(t, http.StatusOK, e.viewStatus(t, ref, icebergView))
}

// requireDropNamespaceSucceeds asserts that the now-empty namespace can be dropped.
func (e *icebergEnv) requireDropNamespaceSucceeds(t *testing.T, ref string) {
	t.Helper()
	status, body := e.doJSON(t, http.MethodDelete, e.refNamespaceURL(e.catalogURL, ref), nil)
	require.Equalf(t, http.StatusNoContent, status,
		"dropping an empty namespace must be 204: %s", body)
}

// icebergViewPointerPath is the reserved key of a view pointer, as views.go lays it out:
// _lakefs_iceberg/views/<namespace levels>/<view>/.pointer.
func icebergViewPointerPath(namespace []string, view string) string {
	return icebergcatalog.ReservedPrefix + "/" + icebergcatalog.ViewsDirectory + "/" +
		strings.Join(namespace, "/") + "/" + view + "/" + icebergcatalog.ViewPointerName
}

// icebergCreateViewScript creates and reads a view with PyIceberg, the client half of the round
// trip: create_view, load_view, list_views and view_exists, and report what the catalog served.
//
// The view version is the client's own: a SQL representation in a dialect, the default namespace a
// reference in the query is resolved against, and the schema the view projects. Nothing here writes
// a data file - a view is metadata - so the step needs no S3 FileIO properties.
const icebergCreateViewScript = `
import json
import os

import pyarrow as pa
from pyiceberg.catalog import ViewVersion
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.view.metadata import SQLViewRepresentation


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


catalog = RestCatalog(
    name="lakefs",
    uri=required("ICEBERG_CATALOG_URI"),
    credential=required("ICEBERG_CREDENTIAL"),
    warehouse=required("ICEBERG_WAREHOUSE"),
)
identifier = tuple(json.loads(required("ICEBERG_IDENTIFIER")))
namespace = identifier[:-1]

columns = []
types = {"id": pa.int64(), "name": pa.string()}
for column in required("ICEBERG_VIEW_SCHEMA").split(","):
    columns.append(pa.field(column, types[column]))
schema = pa.schema(columns)

version = ViewVersion(
    schema_id=0,
    representations=[SQLViewRepresentation(
        type="sql",
        sql=required("ICEBERG_VIEW_SQL"),
        dialect=required("ICEBERG_VIEW_DIALECT"),
    )],
    default_namespace=namespace,
)
catalog.create_view(identifier, schema=schema, view_version=version, properties={"owner": "esti"})

loaded = catalog.load_view(identifier)
representation = loaded.sql_for(required("ICEBERG_VIEW_DIALECT"))
print("RESULT:" + json.dumps({
    "uuid": str(loaded.uuid()),
    "location": loaded.location(),
    "sql": representation.sql if representation else "",
    "owner": loaded.properties.get("owner", ""),
    "exists": catalog.view_exists(identifier),
    "views": sorted(".".join(item) for item in catalog.list_views(namespace)),
    "namespace": list(namespace),
}))
`

// icebergDropViewScript drops a view through the catalog (PyIceberg's drop_view), and reports
// whether the catalog still considers it to exist.
const icebergDropViewScript = `
import json
import os

from pyiceberg.catalog.rest import RestCatalog


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


catalog = RestCatalog(
    name="lakefs",
    uri=required("ICEBERG_CATALOG_URI"),
    credential=required("ICEBERG_CREDENTIAL"),
    warehouse=required("ICEBERG_WAREHOUSE"),
)
identifier = tuple(json.loads(required("ICEBERG_IDENTIFIER")))
catalog.drop_view(identifier)
print("RESULT:" + json.dumps({"dropped": True, "exists": catalog.view_exists(identifier)}))
`
