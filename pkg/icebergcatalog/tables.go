package icebergcatalog

// Table reads, the staged create, the drop and the metrics sink for the Iceberg REST catalog
// (ICEBERG_CATALOG_PLAN.md §2.1 "Protocol facts", §5 "Addressing rules", §6.2 "Atomic commit",
// §7.2 "Authorization", §11 I-1 "tables list/load/head on any ref" and I-2 "create table, drop,
// metrics (accept and discard)").
//
// Six operations live here: the three reads of I1-9, which are the read half of table addressing
// (§5 row 3), the staged create of I2-4, which is the first operation of the write path, and the
// drop and the metrics sink of I2-5.
//
//	GET    /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables
//	    → the table identifiers below one namespace, pageToken / pageSize in, `next-page-token` out
//	GET    /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables/{table}
//	    → LoadTableResult{metadata-location, metadata, config}
//	HEAD   /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables/{table}
//	    → 204 when the table exists, 404 NoSuchTableException when it does not
//	POST   /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables
//	    CreateTableRequest in, 200 LoadTableResult out, 409 AlreadyExistsException when taken
//	DELETE /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables/{table}
//	    → 204 after the table's pointer and its file entries are removed, 404 NoSuchTableException
//	      when there is no pointer
//	POST   /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables/{table}/metrics
//	    ReportMetricsRequest in, 204 out: the report is accepted and discarded
//
// The seventh operation of the path, the commit (POST on the table rather than on tables or
// metrics), is served by commit_http.go; its route and endpoint constant are part of the table
// service here, so mounting the table surface mounts the whole of it. This file also owns the entity
// tag of a table load (tableLoadETag): a 304 is an answer of the load handler, so the tag belongs
// next to it.
//
// A table is read on Address.Ref, which may be a branch, a tag or a commit: that is the any-ref
// read of §1.1 goal 1, the reason the pointer and its metadata.json are resolved through the
// store's ref-agnostic path rather than through a branch name.
//
// What the responses contain, and why:
//
//   - metadata-location is the pointer's physical URI (§6.1 `iceberg.metadata-location`), the
//     location of the metadata.json a client can hand to a FileIO. It is deliberately not a
//     lakeFS path: a client resolves it with storage credentials and an object-store client of
//     its own, so an s3:// (or mem:// in tests) URI is the only form it can use.
//   - metadata is the table metadata JSON of the spec
//     (https://github.com/apache/iceberg/blob/main/format/spec.md, "Table Metadata"). The store
//     keeps the exact bytes of metadata.json, so the default response passes them through
//     unchanged; only the `snapshots` query parameter rewrites the document (below).
//   - config is the table-specific client configuration of LoadTableResult. It is empty in v1:
//     credential vending and remote signing are phase I-4, and an empty map - not a null - is
//     what a client decodes without a special case.
//
// The `snapshots` query parameter (spec: components.parameters.snapshots on loadTable) selects
// which snapshots the returned metadata carries:
//
//	absent or all → every snapshot currently valid for the table (the default, per the spec)
//	refs          → only the snapshots referenced by a branch or a tag, so a client that needs
//	                the current state of the refs does not download the whole history
//
// "Referenced by a branch or a tag" is read literally as the metadata's `refs` map plus
// `current-snapshot-id`. The format spec says "There is always a `main` branch reference pointing
// to the `current-snapshot-id` even if the `refs` map is null"
// (https://github.com/apache/iceberg/blob/main/format/spec.md, "Snapshot References"), so the
// current snapshot is retained explicitly: a v1 metadata document whose `refs` is absent must
// still be readable in refs mode. Ancestors of a referenced snapshot are not added - the spec
// names the snapshots referenced by refs, and the default mode is what history is for. Only the
// `snapshots` field is rewritten, because that is the field the parameter names; `snapshot-log`
// and the rest of the document are passed through untouched.
//
// Listing follows the spec's ListTablesResponse: `identifiers` (each a namespace plus a name) and
// an optional `next-page-token`. The namespace of an identifier is the full lakeFS namespace -
// repository, ref, then the client namespace levels - exactly as the namespace endpoints report
// it (§5, ident.go), so a client can feed a listed identifier back into a load request. The token
// is opaque to clients; this implementation encodes an offset into the filtered result (the same
// helper the namespace listing uses). The table names themselves come from the reserved
// `_lakefs_iceberg/tables/<ns>/<table>/.pointer` layout of §6.1: a pointer is the only thing that
// makes a table exist, so the listing is a walk over pointer keys and a namespace's tables are the
// pointers exactly one level below its own key prefix. That is why the walk does not use a
// listing delimiter: with one, a nested namespace's tables would be indistinguishable from a
// table of the listed namespace.
//
// The create is an initialize-and-stage, never an auto-commit (§6.2 "why stage instead of
// auto-commit", §14 Q-I2):
//
//  1. the table UUID is generated here, because it is what names the table's managed location
//     (§14 Q-I5, location.go) and what metadata.json carries as `table-uuid`;
//  2. the location is the client's when it sent one inside the catalog's table area, and
//     <storage_namespace>/_iceberg/<table-uuid> otherwise. It never contains the ref, so a table's
//     files are the same on every branch - the property that keeps metadata valid after a merge
//     (§6.1, and the alternative §6.3 rejects);
//  3. the initial metadata.json is written under <location>/metadata/, so the pointer a reader
//     finds never names a file that is not there (metadata.go);
//  4. that metadata file becomes a lakeFS entry, _lakefs_iceberg/files/<table-uuid>/metadata/<file>,
//     because the pointer names it from entry user metadata and nothing else would reference it
//     (§6.3): a table created and never appended to must survive the committed GC and must show up
//     in diff and merge. The registration is the commit path's (files.go StageTableFiles) - the
//     same foreign-location check, the same iceberg.max_staged_files cap, the same deterministic
//     key - so create and commit cannot drift apart;
//  5. the pointer is staged with the assert-create condition of store.CreateTable - a
//     compare-and-swap on an absent value, i.e. If-None-Match: * (§6.2). A name that is already
//     taken is 409 AlreadyExistsException, and the metadata.json of the losing request is left
//     unreferenced for GC, exactly like the orphan of a lost commit (§6.2 step 4).
//
// Steps 4 and 5 are store.CreateTableWithMetadata, in that order: the file entry is staged before
// the pointer, so a failure of file registration never leaves a pointer naming an unregistered
// metadata file. Such a failure happens after the metadata write and answers exactly what the
// create path answered before this step existed.
//
// The response is the LoadTableResult of the spec's CreateTableResponse, built from the bytes that
// were just written, so a client sees the same document a later load returns.
//
// The spec's `stage-create` member is honoured in its documented sense: with stage-create=true the
// metadata is initialized and returned and the table is *not* created, so neither a pointer nor a
// table-file entry is staged (a file entry without a table would be an uncommitted entry for a
// table no one can read).
// The phase-two commit such a client sends next (assert-create against a table that does not exist
// yet) is not served by Store.Commit, which requires a base pointer; until that path exists, a
// client that asks for a staged create gets the initializing half of it and no table.
//
// The namespace a table is created in has to exist, i.e. its marker has to be present on the
// branch: that is the spec's 404 NoSuchNamespaceException for createTable, and §6.1 defines a
// namespace by its marker. (A listing still reports a table whose namespace was never created,
// because there the pointer is what makes the table exist; create is the operation that requires
// the namespace.)
//
// The drop removes the two kinds of entry that make up a table: the pointer, which is the one entry
// that makes the table exist (§6.1: a pointer is the only thing that makes a table exist), and the
// _lakefs_iceberg/files/<table-uuid>/… entries that §6.3 staged for it. The files themselves -
// metadata.json, manifest lists, manifests and data files - stay where they are: the catalog's job on
// a drop is to stop pointing at them *and* to stop protecting them, and reclaiming them is GC's job
// once nothing references them (§6.3, phase I-3). Releasing the file entries is what makes the
// objects reclaimable: the committed GC's live set is the set of addresses reachable from the active
// commits, so an entry that outlived its table would keep the table's objects alive for as long as
// the ref keeps the commits that carry it (RESULTS.md N-3).
//
// The Q-I8 rule of §14 still holds on the drop - v1 never expires or tombstones a file - and the drop
// stays reversible: because it is staged like every other catalog write (§6.2, §14 Q-I2) and only
// removes entries, reverting the lakeFS commit that carries it brings the table back (the entries of
// the commit being reverted are restored with it), and a table dropped on one branch stays readable
// on a branch or a tag whose pointer still names its metadata.
//
// So a drop sends no delete to object storage. The spec's `purgeRequested` query parameter is
// accepted and deliberately has no effect whatever its value: purging would make one catalog
// request the owner of files that other branches, tags and commits' pointers may still reference,
// and the lakeFS GC already answers "is this file unreachable?" for the whole repository at once
// (ICEBERG_CATALOG_PLAN.md §6.3, §14 Q-I8).
//
// Existence for a drop is the pointer entry, not a readable metadata.json: a drop has to be able to
// remove a pointer that names a document this catalog cannot parse, or a broken table could never be
// dropped. GET and HEAD are stricter on purpose (handleHeadTable), only because serving a table is
// a different question from removing its name.
//
// The metrics sink of reportMetrics is telemetry: a client reports what a scan or a commit cost and
// takes no interest in the answer beyond "it was not an error". v1 stores none of it - nothing in
// §6.1 has a place for a report - so the endpoint resolves the table (the spec's 404
// NoSuchTableException for a table this catalog does not have), drains the request body and answers
// 204. The payload is therefore not decoded: a report is discarded whether it is a scan report, a
// commit report or something a newer spec added, and a client that sends one is never handed a 400
// for the shape of data the server is throwing away anyway.
//
// Authorization is the §7.2 row for each operation - catalog:ListTables filtered per table by the
// catalog:TableName condition, catalog:ReadTable on the table ARN, catalog:CreateTable for create
// and catalog:DeleteTable for drop - plus the fs:ReadRepository every catalog request needs and the
// fs:WriteObject §7.2 adds for the reserved _lakefs_iceberg/ area of a write. reportMetrics has no
// row of its own in §7.2; authz.go assigns it to catalog:UpdateTable on the table ARN, the
// "commit, register, properties, metrics" action of the write half. The listing asks
// Authorizer.FilterTables, which evaluates the namespace-level gate and the per-table condition in
// one place, so the endpoint cannot return a table the caller may not see. Load, head, drop and the
// metrics sink are all authorized before the pointer is resolved, so a caller without the action
// cannot distinguish a missing table from a forbidden one (both requests are 403 for it, §12).

// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, listTables /
// loadTable / tableExists / createTable / dropTable / reportMetrics,
// components.schemas.CreateTableRequest and components.schemas.ReportMetricsRequest, and
// components.parameters.snapshots), the Apache Iceberg table format spec
// (https://github.com/apache/iceberg/blob/main/format/spec.md, "Table Metadata" and
// "Snapshot References") and the public lakeFS documentation only. No code from lakeFS Enterprise
// or from upstream lakeFS >= 1.87 (BSL) was used.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
)

const (
	// tablesPattern serves ListTables and is relative to the catalog mount (APIBasePath).
	// {prefix} is the value GET /v1/config advertised as overrides.prefix (§14 Q-I3).
	tablesPattern = "/v1/{prefix}/namespaces/{namespace}/tables"

	// tablePattern serves LoadTable (GET), TableExists (HEAD) and DropTable (DELETE) for one table.
	tablePattern = "/v1/{prefix}/namespaces/{namespace}/tables/{table}"

	// tableMetricsPattern serves ReportMetrics (POST), which hangs off one table and is the only
	// table operation with a sub-path of its own.
	tableMetricsPattern = "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"

	// The wire forms advertised in GET /v1/config once this file's routes are mounted. They are
	// the operation paths of the REST OpenAPI spec with the catalog's {prefix} segment.
	// commitTableEndpoint itself is declared in commit_http.go, next to the handler it advertises.
	listTablesEndpoint    = "GET /v1/{prefix}/namespaces/{namespace}/tables"
	loadTableEndpoint     = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
	tableExistsEndpoint   = "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"
	createTableEndpoint   = "POST /v1/{prefix}/namespaces/{namespace}/tables"
	dropTableEndpoint     = "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"
	reportMetricsEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"

	// ErrTypeNoSuchTable is the Iceberg error type of a missing table, 404
	// (components.schemas.NoSuchTableException, the 404 of loadTable and tableExists).
	ErrTypeNoSuchTable = "NoSuchTableException"

	// ErrTypeAlreadyExists is the Iceberg error type of a create whose name is taken, 409
	// (components.schemas.AlreadyExistsException, the 409 of createTable and of the other create
	// operations of the spec). It is not CommitFailedException: a create is not a lost commit, and
	// the client must not retry it - the table it asked for is already there.
	ErrTypeAlreadyExists = "AlreadyExistsException"

	// SnapshotsAll is the default value of the `snapshots` query parameter: every snapshot
	// currently valid for the table. It is also what an absent parameter means.
	SnapshotsAll = "all"

	// SnapshotsRefs is the value that returns only the snapshots referenced by a branch or a
	// tag (components.parameters.snapshots).
	SnapshotsRefs = "refs"
)

// ErrInvalidCreateTable is wrapped by every validation failure of a create request that is not
// already an identifier or a location problem: a missing schema, a schema or partition spec or sort
// order that cannot describe a table, and a format version this catalog does not serve. The HTTP
// layer maps it to 400 BadRequestException - the request is refused as sent, and nothing is staged.
var ErrInvalidCreateTable = errors.New("invalid iceberg create table request")

// BranchLister reports whether a ref names a branch. The write path needs it for the Q-I4 rule of
// §7.2: a table may be created on a branch, never on a tag or a commit, which is a refusal the
// Authorizer can only make when it is told what the ref is (Request.RefIsBranch).
//
// Resolving it is a lakeFS ref read, so it lives next to the listing rather than in Store.
type BranchLister interface {
	GetBranchReference(ctx context.Context, repositoryID string, branch string) (string, error)
}

// TableLister is the lakeFS surface the table endpoints need in addition to Store: the entry walk
// that finds the table pointers of a namespace, the single-entry read that decides whether a table
// exists, the entry removal that drops one, and the ref resolution a write needs. It is a separate
// interface rather than more methods on Store because these are lakeFS operations, not catalog
// state, and *catalog.Catalog (also the NamespaceLister of the namespace endpoints) implements it.
type TableLister interface {
	BranchLister
	ListEntries(ctx context.Context, repositoryID string, reference string, prefix string, after string, delimiter string, limit int) ([]*catalog.DBEntry, bool, error)

	// GetEntry resolves one entry on a ref - the branch staging area first, then the committed
	// data - so the table pointer of a staged create is found exactly like a committed one. Drop
	// and the metrics sink use it to decide existence without loading metadata.json, and the drop
	// reads the table uuid out of the entry it gets back.
	GetEntry(ctx context.Context, repositoryID string, reference string, path string, params catalog.GetEntryParams) (*catalog.DBEntry, error)

	// DeleteEntry stages the removal of one entry on a branch: dropping a table removes its
	// pointer, and then the file entries the commits staged for it (files.go releaseTableFiles).
	// It carries no graveler.SetOptionsFunc, so a protected branch or a read-only repository still
	// refuses it.
	DeleteEntry(ctx context.Context, repositoryID string, branch string, path string, opts ...graveler.SetOptionsFunc) error
}

// tableService serves the table read endpoints of §5. It owns the three things a handler needs:
// the catalog state (Store), the authorization matrix of §7.2 (Authorizer) and the pointer walk
// (TableLister).
type tableService struct {
	store  *Store
	authz  *Authorizer
	lister TableLister
	prefix string

	// maxBody is iceberg.max_request_body: the bound this service applies to a request body it
	// reads itself (the commit of commit_http.go). The router's middleware already bounds every
	// body, but a service mounted without it must still refuse an unbounded one (§10, §12).
	maxBody int64
}

// NewTableService builds the table read surface. The prefix is the value of GET /v1/config
// overrides.prefix, i.e. the prefix a client sends back in every path; an empty configuration
// falls back to CatalogPrefix, exactly like server.prefix (http.go).
//
// Like NewNamespaceService it exists so mounting the read path is one entry in the route table
// (http.go) and needs no change to this file.
func NewTableService(store *Store, authz *Authorizer, lister TableLister, cfg *Config) *tableService {
	prefix := ""
	if cfg != nil {
		prefix = cfg.Prefix
	}
	if prefix == "" {
		prefix = CatalogPrefix
	}
	return &tableService{
		store:   store,
		authz:   authz,
		lister:  lister,
		prefix:  prefix,
		maxBody: maxRequestBody(cfg),
	}
}

// Routes is this file's contribution to the catalog route table (server.ownRoutes in http.go).
// The endpoint strings are the compatibility contract of §2.1: they are advertised in
// GET /v1/config only once the routes are actually mounted, which the same table change does.
func (s *tableService) Routes() []route {
	return []route{
		{
			method:   http.MethodGet,
			pattern:  tablesPattern,
			handler:  s.handleListTables,
			endpoint: listTablesEndpoint,
		},
		{
			method:   http.MethodGet,
			pattern:  tablePattern,
			handler:  s.handleLoadTable,
			endpoint: loadTableEndpoint,
		},
		{
			method:   http.MethodHead,
			pattern:  tablePattern,
			handler:  s.handleHeadTable,
			endpoint: tableExistsEndpoint,
		},
		{
			method:   http.MethodPost,
			pattern:  tablesPattern,
			handler:  s.handleCreateTable,
			endpoint: createTableEndpoint,
		},
		{
			// The commit of §6.2 / commit_http.go. It shares its path with the load and the drop;
			// only the method tells the three apart.
			method:   http.MethodPost,
			pattern:  tablePattern,
			handler:  s.handleCommitTable,
			endpoint: commitTableEndpoint,
		},
		{
			method:   http.MethodDelete,
			pattern:  tablePattern,
			handler:  s.handleDropTable,
			endpoint: dropTableEndpoint,
		},
		{
			method:   http.MethodPost,
			pattern:  tableMetricsPattern,
			handler:  s.handleReportMetrics,
			endpoint: reportMetricsEndpoint,
		},
	}
}

// tableIdentifier is TableIdentifier (spec: components.schemas.TableIdentifier): the namespace
// the table lives in and its name, in the client's own terms. On the /iceberg/api form the
// namespace carries the repository and ref levels, because that is how this catalog addresses a
// table (§5) and what a client has to send back to load it; on a relative_to form it is the same
// address without the levels the URL already fixed (relative.go clientNamespace).
type tableIdentifier struct {
	Namespace Namespace `json:"namespace"`
	Name      string    `json:"name"`
}

// listTablesResponse is ListTablesResponse (spec: components.schemas.ListTablesResponse).
type listTablesResponse struct {
	Identifiers   []tableIdentifier `json:"identifiers"`
	NextPageToken string            `json:"next-page-token,omitempty"`
}

// loadTableResult is LoadTableResult (spec: components.schemas.LoadTableResult) as v1 serves it:
// the physical location of the current metadata.json, the metadata document itself, and the
// table-specific client configuration. metadata is a json.RawMessage so the document goes out as
// it was written (or, in refs mode, only its snapshots field rewritten) instead of being
// re-encoded from a model that would drop fields this catalog does not interpret.
//
// storage-credentials, remote-signing-config and read-restrictions are absent: credential vending
// and remote signing are phase I-4, and the catalog imposes no read restrictions.
type loadTableResult struct {
	MetadataLocation string            `json:"metadata-location"`
	Metadata         json.RawMessage   `json:"metadata"`
	Config           map[string]string `json:"config"`
}

// createTableRequest is CreateTableRequest (spec: components.schemas.CreateTableRequest): the
// table to create, its schema, and the optional members that shape the initial metadata.json.
//
// The three structural members are decoded into iceberg-go's *unbound* types, which is the
// documented way for a catalog to read this request (table.UnboundPartitionSpec,
// table.UnboundSortOrder): a create request carries the client's placeholder source IDs - Spark
// numbers the columns of a new table from zero - not field IDs of a table that does not exist yet.
// table.NewMetadataWithUUID resolves those placeholders through the schema in the same request and
// assigns fresh IDs; decoding into the bound types instead would reject a valid request, or bind a
// placeholder to an unrelated column.
//
// The members this catalog does not interpret are absent rather than rejected: the spec allows a
// server to ignore what it does not model, and an unknown member of a create request cannot change
// the table this endpoint stages.
type createTableRequest struct {
	// Name is the table's name. Create addresses a namespace, so the name is in the body.
	Name string `json:"name"`

	// Schema is the initial schema in the format spec's JSON form. It is required.
	Schema *iceberg.Schema `json:"schema"`

	// Location is the table's base location. Optional: an absent location becomes the managed
	// location of §14 Q-I5 (location.go), and a location outside the catalog's table area is 400.
	Location string `json:"location,omitempty"`

	// PartitionSpec is the initial partition spec; absent means unpartitioned.
	PartitionSpec *iceberg.UnboundPartitionSpec `json:"partition-spec,omitempty"`

	// WriteOrder is the initial sort order; absent means unsorted.
	WriteOrder *table.UnboundSortOrder `json:"write-order,omitempty"`

	// StageCreate asks for the metadata to be initialized without creating the table (below).
	StageCreate bool `json:"stage-create"`

	// Properties are the initial table properties, including `format-version` when the client
	// asks for a version other than the default (2).
	Properties iceberg.Properties `json:"properties,omitempty"`
}

// handleListTables is GET /v1/{prefix}/namespaces/{namespace}/tables: the table identifiers
// exactly one level below the addressed namespace (§5).
//
// The order is parse, then list, then authorize per name, then paginate. The names have to be
// known before the per-table condition of §7.2 can be evaluated, which is exactly what
// Authorizer.FilterTables does: it evaluates the namespace-level gate and the catalog:TableName
// condition for every name, and refuses with the gate's denial when nothing survives it - so a
// caller that may not list the namespace gets 403 rather than an empty page.
func (s *tableService) handleListTables(w http.ResponseWriter, r *http.Request) {
	if !s.checkPrefix(w, r) {
		return
	}
	addr, ok := s.namespaceAddress(w, r)
	if !ok {
		return
	}
	caller, ok := s.caller(w, r)
	if !ok {
		return
	}
	pageSize, err := parsePageSize(r)
	if err != nil {
		s.writeError(w, err)
		return
	}

	names, err := s.tableNames(r.Context(), addr)
	if err != nil {
		s.writeError(w, err)
		return
	}

	// A namespace that has no table and no marker does not exist: the spec's listTables answers
	// 404 NoSuchNamespaceException for it. A namespace with markers of its own but no table is an
	// empty listing, and a table whose namespace was never created is still listed, because the
	// pointer is what makes a table exist (§6.1).
	if len(names) == 0 {
		if _, err := s.store.GetNamespace(r.Context(), addr); err != nil {
			s.writeError(w, err)
			return
		}
	}

	visible, err := s.authz.FilterTables(r.Context(), caller, addr, names)
	if err != nil {
		s.writeError(w, err)
		return
	}

	identifiers := make([]tableIdentifier, 0, len(visible))
	for _, name := range visible {
		identifiers = append(identifiers, tableIdentifier{
			// The client's own terms, as in every response that carries a namespace: the full
			// [repo, ref, ns…] address on the /iceberg/api form, the levels below the fixed
			// warehouse on a relative_to form (relative.go clientNamespace, D-4).
			Namespace: clientNamespace(r, fullNamespace(addr, "")),
			Name:      name,
		})
	}
	page, next, err := paginate(identifiers, r.URL.Query().Get("pageToken"), pageSize)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if page == nil {
		page = []tableIdentifier{}
	}
	writeJSON(w, http.StatusOK, listTablesResponse{Identifiers: page, NextPageToken: next})
}

// handleLoadTable is GET /v1/{prefix}/namespaces/{namespace}/tables/{table}: the LoadTableResult
// of §11 I-1, honouring the `snapshots` query parameter.
func (s *tableService) handleLoadTable(w http.ResponseWriter, r *http.Request) {
	// The query parameter is validated before anything is resolved: a malformed request is
	// answered 400 whether or not the table exists, and no metadata.json is read to say so.
	mode, err := parseSnapshotsMode(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	table, ok := s.authorizeTable(w, r)
	if !ok {
		return
	}
	if len(table.MetadataJSON) == 0 {
		// Unreachable for a store-built Table: the bytes are kept precisely so the response does
		// not depend on re-marshalling. Treat it as the server-side data error it is.
		s.writeError(w, fmt.Errorf("%w: the table has no metadata document", ErrInvalidPointer))
		return
	}
	metadata := json.RawMessage(table.MetadataJSON)
	if mode == SnapshotsRefs {
		filtered, err := refsOnlyMetadata(table.MetadataJSON)
		if err != nil {
			s.writeError(w, err)
			return
		}
		metadata = filtered
	}

	// The entity tag of this representation, and the conditional answer of §2.1 / I5-2: a client
	// that already holds this exact table (and this exact snapshots mode) is answered 304 with no
	// body. The tag is set on the 304 too, as RFC 9110 requires of a validator response.
	etag := tableLoadETag(table.Pointer.MetadataLocation, mode)
	w.Header().Set("ETag", etag)
	if ifNoneMatchMatches(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, loadTableResult{
		// The physical URI of the pointer's metadata.json, never a lakeFS path: the client reads
		// it with its own object-store client (§6.1, I1-9 note).
		MetadataLocation: table.Pointer.MetadataLocation,
		Metadata:         metadata,
		Config:           map[string]string{},
	})
}

// handleHeadTable is HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table} (spec: tableExists):
// 204 with no body when the table exists, 404 NoSuchTableException when it does not.
//
// Existence is decided by the same authorized read GET performs, so HEAD cannot answer 204 for a
// table GET would refuse or fail on: a pointer whose metadata.json is missing or unparseable is
// not a table this catalog can serve, and reporting it as existing would send the client into a
// second, failing request.
func (s *tableService) handleHeadTable(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorizeTable(w, r); !ok {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleCreateTable is POST /v1/{prefix}/namespaces/{namespace}/tables (spec: createTable): a new
// table's initial metadata.json, its pointer staged on the branch with assert-create semantics, and
// the LoadTableResult of the spec's CreateTableResponse.
//
// The body is decoded before the authorization decision, and it has to be: createTable carries the
// table name in the body, so the catalog:TableName ARN of §7.2 cannot be built until the body is
// read. That leaks nothing - an unauthorized caller gets the same 403 whatever name it sends - and
// the body is bounded by iceberg.max_request_body (http.go), so neither the order of the
// middleware stack (authentication first) nor the size bound is given up.
//
// The order after that is the read path's: validate, authorize, resolve, act. Authorization needs
// the branch resolution first (Q-I4), and the refusal of a tag or a commit is the Authorizer's so
// that an unauthorized caller cannot tell a tag from a branch.
func (s *tableService) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	if !s.checkPrefix(w, r) {
		return
	}
	addr, ok := s.namespaceAddress(w, r)
	if !ok {
		return
	}

	var request createTableRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}
	name, err := decodeTableName(request.Name)
	if err != nil {
		s.writeError(w, err)
		return
	}
	addr.Table = name

	caller, ok := s.caller(w, r)
	if !ok {
		return
	}
	isBranch, err := s.isBranchRef(r.Context(), addr)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if err := s.authz.Authorize(r.Context(), caller, Request{
		Op:          OpCreateTable,
		Address:     addr,
		RefIsBranch: isBranch,
	}); err != nil {
		s.writeError(w, err)
		return
	}

	result, err := s.createTable(r.Context(), addr, request)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// createTable is the create itself, once the request is parsed and authorized: initialize the
// metadata, write it, register it, and stage the pointer (§6.2, §14 Q-I5, §6.3).
//
// Every failure before the pointer write leaves nothing behind that a reader can see: the table
// does not exist, so a load is 404 whatever staged file entries a failed attempt left. A failure of
// the pointer write after metadata.json exists leaves an unreferenced metadata file, which is the
// same orphan a lost commit leaves and which GC collects (§6.2 step 4); a duplicate create
// therefore costs one collectable object and one collectable entry, and nothing can point a table
// at the loser's document. Every failure after the metadata write - including one of the file
// registration of step 4 - answers what this path answered before that step existed.
func (s *tableService) createTable(ctx context.Context, addr Address, request createTableRequest) (*loadTableResult, error) {
	if request.Schema == nil {
		return nil, fmt.Errorf("%w: the request carries no schema", ErrInvalidCreateTable)
	}
	// §6.1: a namespace exists when its marker does, and the spec's createTable answers 404
	// NoSuchNamespaceException for one that does not. The check is a read on the same ref the
	// pointer is staged on, so it sees the branch's staging area too.
	if _, err := s.store.GetNamespace(ctx, addr); err != nil {
		return nil, err
	}

	tableUUID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate a table uuid: %w", err)
	}
	location, err := s.store.TableLocation(ctx, addr.Repo, request.Location, tableUUID.String())
	if err != nil {
		return nil, err
	}

	// The unbound spec and order are handed over embedded: NewMetadataWithUUID resolves their
	// placeholder source IDs against the schema of this same request and assigns fresh IDs, which
	// is exactly the create-table flow the iceberg-go types document.
	var spec *iceberg.PartitionSpec
	if request.PartitionSpec != nil {
		spec = &request.PartitionSpec.PartitionSpec
	}
	order := table.UnsortedSortOrder
	if request.WriteOrder != nil {
		order = request.WriteOrder.SortOrder
	}
	metadata, err := table.NewMetadataWithUUID(request.Schema, spec, order, location, request.Properties, tableUUID)
	if err != nil {
		return nil, fmt.Errorf("%w: the schema, partition spec or sort order cannot describe a table: %w",
			ErrInvalidCreateTable, err)
	}
	// Format version 3 is a v1 non-goal (§1.2), exactly as on the commit path.
	version := metadata.Version()
	if version != 1 && version != 2 {
		return nil, fmt.Errorf("%w: format version %d is not supported: this catalog serves format versions 1 and 2",
			ErrInvalidCreateTable, version)
	}

	metadataLocation, err := NewMetadataFileLocation(location, "", metadata)
	if err != nil {
		return nil, err
	}
	metadataJSON, err := s.store.WriteMetadata(ctx, addr.Repo, metadataLocation, metadata)
	if err != nil {
		return nil, err
	}
	pointer := TablePointer{
		MetadataLocation: metadataLocation,
		TableUUID:        tableUUID.String(),
		FormatVersion:    version,
	}
	if !request.StageCreate {
		// Steps 4-5 of the file comment: the metadata file becomes a lakeFS entry (§6.3) and the
		// pointer is staged with assert-create semantics (§6.2). A duplicate create is
		// ErrAlreadyExists there, which the HTTP layer answers 409 AlreadyExistsException.
		if err := s.store.CreateTableWithMetadata(ctx, addr, pointer, metadata, int64(len(metadataJSON))); err != nil {
			return nil, err
		}
	}

	return &loadTableResult{
		MetadataLocation: metadataLocation,
		Metadata:         json.RawMessage(metadataJSON),
		Config:           map[string]string{},
	}, nil
}

// handleDropTable is DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table} (spec: dropTable):
// remove the table's pointer and the table files staged for it from the branch, and answer 204 with
// no body.
//
// The order is the create's without the body: parse, authorize (including the Q-I4 branch rule),
// resolve, act. A table that has no pointer on addr.Ref is 404 NoSuchTableException, and a pointer
// that names an unreadable metadata.json is still dropped - see the file comment for why that
// difference from GET and HEAD is deliberate. What the drop does need from the pointer is its
// table uuid, because that uuid - not metadata.json - names the directory of the table's file
// entries; a pointer too corrupt to carry one is answered as the corrupt pointer it is.
//
// The query string is not read at all. The spec's `purgeRequested` parameter asks for the data files
// to be deleted with the table; v1 accepts every value and deletes no object, because the objects
// are reclaimed by GC once nothing references them (§6.3, §14 Q-I8). The entries that referenced
// them are removed (dropTable), which is what makes them reclaimable.
func (s *tableService) handleDropTable(w http.ResponseWriter, r *http.Request) {
	addr, ok := s.authorizeTableWrite(w, r, OpDeleteTable)
	if !ok {
		return
	}
	if err := s.dropTable(r.Context(), addr); err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// dropTable removes the two kinds of entry that make up a table on the branch: its pointer, the one
// entry that makes the table exist (§6.1), and every table-file entry staged for it under
// _lakefs_iceberg/files/<table-uuid>/ (§6.3). No object is deleted - the metadata.json the pointer
// named and every data file the metadata references stay in the object store - which is what keeps
// the drop staged, reversible and GC-collected rather than destructive (file comment, §14 Q-I8).
//
// Why the files are released here and not only by GC: §6.3 stages the file entries so that the
// committed GC protects exactly the live files, and that GC's live set is the set of addresses
// reachable from the active commits. For a table that no longer exists, an entry that survives the
// drop is the opposite of the goal - it keeps the dropped table's objects alive for as long as the
// ref keeps the commits that carry it. A drop is the reverse of the create and the commit: the same
// commit removes the pointer and the file entries, so the objects become unreferenced together
// (measured defect N-3: the drop used to remove the pointer and no other path).
//
// Order: the pointer first, then the files. Both writes are staged, so a client's one lakeFS commit
// publishes them together and no reader ever sees half a drop; the order matters for a failure
// between the two steps, and only this order is the conservative one. A pointer that is gone with
// files still staged leaves a table that does not exist whose objects are still protected - exactly
// what v1 did before this release existed, so the failure cannot lose data - while the other order
// would leave a table that does exist with its files no longer protected, i.e. a live table the
// committed GC may collect after the next commit. (The first failure state is not repairable
// through this endpoint: a second drop finds no pointer and therefore no uuid to release. It is
// left for an operator, and it is why the two steps are ordered instead of interleaved.)
//
// A release that fails is reported as the failure it is: the client did not get its 204, and the
// entries that were not removed still protect their objects.
func (s *tableService) dropTable(ctx context.Context, addr Address) error {
	pointerPath, entry, err := s.requireTableEntry(ctx, addr)
	if err != nil {
		return err
	}
	// The uuid is read from the pointer's own user metadata and not from metadata.json: the drop
	// must keep working for a pointer that names an unreadable document (file comment), and it is
	// the pointer's uuid that names the files directory files.go stages under.
	tableUUID, err := tableUUIDOfPointer(pointerPath, entry)
	if err != nil {
		return err
	}
	if err := s.lister.DeleteEntry(ctx, addr.Repo, addr.Ref, pointerPath); err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			// The pointer was removed by another writer between the read and the delete: the answer
			// is the 404 a client that drops twice should see.
			return &NoSuchTableException{
				Repo:      addr.Repo,
				Ref:       addr.Ref,
				Namespace: addr.Namespace,
				Table:     addr.Table,
			}
		}
		return err
	}
	_, err = releaseTableFiles(ctx, s.lister, addr, tableUUID, DefaultStageWorkers)
	return err
}

// tableUUIDOfPointer returns the table uuid the pointer entry of pointerPath carries, i.e. the
// directory name of the table's file entries in the reserved area (§6.3).
//
// It decodes the pointer's user metadata, which this catalog wrote (store.go EncodeTablePointer),
// and never reads the metadata.json the pointer names: a drop must be able to remove a pointer that
// names a document this catalog cannot read. A pointer whose user metadata is not a pointer this
// catalog could have written - a missing or non-uuid table-uuid, a format version that is not
// served - is reported as ErrInvalidPointer, the same error a load of that pointer answers, because
// nothing can say which directory to release and a drop that removed only the pointer would leave
// exactly the leak the release exists to close.
func tableUUIDOfPointer(pointerPath string, entry *catalog.DBEntry) (uuid.UUID, error) {
	pointer, err := DecodeTablePointer(entry.Metadata)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode pointer %q: %w", pointerPath, err)
	}
	tableUUID, err := uuid.Parse(pointer.TableUUID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode pointer %q: %w: %q is not a table uuid: %w",
			pointerPath, ErrInvalidPointer, pointer.TableUUID, err)
	}
	return tableUUID, nil
}

// handleReportMetrics is POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics
// (spec: reportMetrics): accept a client's scan or commit report and discard it with 204.
//
// The endpoint is telemetry, so it answers exactly two questions: may this caller address this
// table (§7.2), and does the table exist (the spec's 404 NoSuchTableException). The body is then
// drained and forgotten - v1 has nowhere to put a report (§6.1) - so a payload this catalog does
// not model, or does not model yet, is still accepted: refusing it would fail a request whose only
// possible outcome is that both sides lose the data (file comment).
func (s *tableService) handleReportMetrics(w http.ResponseWriter, r *http.Request) {
	addr, ok := s.authorizeTableWrite(w, r, OpUpdateTable)
	if !ok {
		return
	}
	if _, err := s.requireTablePointer(r.Context(), addr); err != nil {
		s.writeError(w, err)
		return
	}
	// Drain the body instead of decoding it, so the connection stays reusable and an oversized
	// report cannot be buffered: limitRequestBody (http.go) already bounded what can be read.
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
	}
	w.WriteHeader(http.StatusNoContent)
}

// requireTablePointer returns the reserved key of the table pointer of addr, or
// NoSuchTableException when the pointer is not present on addr.Ref.
//
// Existence is the pointer entry itself, not a metadata.json that parses: the drop has to be able to
// remove a pointer that names a document this catalog cannot read, and the metrics sink has no
// reason to read a document it throws away. Both therefore ask the cheaper, more literal question
// §6.1 defines - "is the pointer there?" - and resolve it the way a reader does, staging area first.
func (s *tableService) requireTablePointer(ctx context.Context, addr Address) (string, error) {
	pointerPath, _, err := s.requireTableEntry(ctx, addr)
	return pointerPath, err
}

// requireTableEntry is requireTablePointer with the entry it read, for a caller that needs the
// pointer's own user metadata: the drop reads the table uuid out of it to release the table's file
// entries (dropTable). The read is one entry read, so the pointer's existence and its metadata are
// answered together and cannot describe two different states of the ref.
func (s *tableService) requireTableEntry(ctx context.Context, addr Address) (string, *catalog.DBEntry, error) {
	pointerPath, err := tablePointerPath(addr)
	if err != nil {
		return "", nil, err
	}
	entry, err := s.lister.GetEntry(ctx, addr.Repo, addr.Ref, pointerPath, catalog.GetEntryParams{})
	if err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			return "", nil, &NoSuchTableException{
				Repo:      addr.Repo,
				Ref:       addr.Ref,
				Namespace: addr.Namespace,
				Table:     addr.Table,
			}
		}
		return "", nil, err
	}
	return pointerPath, entry, nil
}

// authorizeTableWrite parses and authorizes the table address of a write endpoint of this file that
// carries no body: drop and report metrics. It is the write counterpart of authorizeTable, and it
// writes the error itself when a step fails so the two verbs cannot diverge on what they accept or
// who may ask.
//
// The ref is resolved before the decision so that Request.RefIsBranch is truthful (Q-I4); a ref that
// is not a branch makes Authorizer answer the read-only 403, and a caller without the action gets
// the same 403 for a branch and a tag alike.
func (s *tableService) authorizeTableWrite(w http.ResponseWriter, r *http.Request, op Operation) (Address, bool) {
	if !s.checkPrefix(w, r) {
		return Address{}, false
	}
	addr, err := ParseTable(chi.URLParam(r, "namespace"), chi.URLParam(r, "table"))
	if err != nil {
		s.writeError(w, err)
		return Address{}, false
	}
	caller, ok := s.caller(w, r)
	if !ok {
		return Address{}, false
	}
	isBranch, err := s.isBranchRef(r.Context(), addr)
	if err != nil {
		s.writeError(w, err)
		return Address{}, false
	}
	if err := s.authz.Authorize(r.Context(), caller, Request{
		Op:          op,
		Address:     addr,
		RefIsBranch: isBranch,
	}); err != nil {
		s.writeError(w, err)
		return Address{}, false
	}
	return addr, true
}

// isBranchRef reports whether addr.Ref names a branch, for the Q-I4 rule of §7.2 that only a branch
// may be written to.
//
// A ref the catalog cannot resolve to a branch is "not a branch", which is the fail-safe direction:
// the Authorizer then refuses the write with the 403 "ref is read-only" answer of §14 Q-I4.
// graveler.ErrInvalidBranchID belongs in that group - a tag such as "v1.0" is a valid ref and not a
// valid branch name, and reporting it as a server error would turn a client's typo into a 500. A
// repository that does not exist is propagated instead: it is the 404 NoSuchWarehouseException of
// §5, not a read-only ref.
func (s *tableService) isBranchRef(ctx context.Context, addr Address) (bool, error) {
	_, err := s.lister.GetBranchReference(ctx, addr.Repo, addr.Ref)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, graveler.ErrBranchNotFound), errors.Is(err, graveler.ErrInvalidBranchID):
		return false, nil
	default:
		return false, err
	}
}

// authorizeTable parses, authorizes and resolves the table address of GET and HEAD, writing the
// error itself when a step fails. The two verbs share it so they cannot diverge on what they
// accept, who may ask, or what "exists" means.
func (s *tableService) authorizeTable(w http.ResponseWriter, r *http.Request) (*Table, bool) {
	if !s.checkPrefix(w, r) {
		return nil, false
	}
	addr, err := ParseTable(chi.URLParam(r, "namespace"), chi.URLParam(r, "table"))
	if err != nil {
		s.writeError(w, err)
		return nil, false
	}
	caller, ok := s.caller(w, r)
	if !ok {
		return nil, false
	}
	if err := s.authz.Authorize(r.Context(), caller, Request{Op: OpReadTable, Address: addr}); err != nil {
		s.writeError(w, err)
		return nil, false
	}
	table, err := s.store.GetTable(r.Context(), addr)
	if err != nil {
		s.writeError(w, err)
		return nil, false
	}
	return table, true
}

// namespaceAddress parses and validates the namespace path parameter of a listing. Listing tables
// addresses a namespace, not the repository or the branch level of §5: a table always lives in a
// namespace, so "repo<sep>ref" is not a table listing address.
func (s *tableService) namespaceAddress(w http.ResponseWriter, r *http.Request) (Address, bool) {
	addr, err := ParseNamespace(chi.URLParam(r, "namespace"))
	if err != nil {
		s.writeError(w, err)
		return Address{}, false
	}
	if addr.Ref == "" || len(addr.Namespace) == 0 {
		s.writeError(w, fmt.Errorf("%w: a table listing needs a repository, a ref and at least one namespace level",
			ErrInvalidIdentifier))
		return Address{}, false
	}
	return addr, true
}

// tableNames returns the names of the tables in addr.Namespace on addr.Ref, in path order.
//
// A table exists exactly when its `_lakefs_iceberg/tables/<ns…>/<table>/.pointer` entry does
// (§6.1), so the walk below the namespace's key prefix collects every pointer path whose
// remainder is exactly `<table>/.pointer`. Pointers deeper than that belong to a nested namespace
// (`tables/inventory/books/orders/.pointer` is not a table of `inventory`), which is why the walk
// asks for no listing delimiter and decides by path shape instead.
func (s *tableService) tableNames(ctx context.Context, addr Address) ([]string, error) {
	prefix := tableEntryPrefix(addr.Namespace)
	names := make([]string, 0)
	var after string
	for {
		page, hasMore, err := s.lister.ListEntries(ctx, addr.Repo, addr.Ref, prefix, after, "", catalog.ListEntriesLimitMax)
		if err != nil {
			return nil, err
		}
		for _, entry := range page {
			name, ok, err := tableNameOfPointer(prefix, entry.Path)
			if err != nil {
				return nil, err
			}
			if ok {
				names = append(names, name)
			}
		}
		if !hasMore || len(page) == 0 {
			return names, nil
		}
		after = page[len(page)-1].Path
	}
}

// tableEntryPrefix is the reserved key prefix of the pointers below namespace (§6.1):
// `_lakefs_iceberg/tables/<ns levels…>/`. The trailing slash is what keeps a namespace from
// matching a sibling whose name it is a prefix of.
func tableEntryPrefix(namespace Namespace) string {
	return ReservedPrefix + "/" + TablesDirectory + "/" + strings.Join(namespace, "/") + "/"
}

// tableNameOfPointer reports whether entryPath is the pointer of a table directly in the namespace
// whose key prefix is prefix, and if so returns the table's name. An entry that is not exactly
// `<prefix><table>/.pointer` is not a table of this namespace and is skipped: it is either a
// deeper pointer (a nested namespace's table) or something this catalog did not write.
func tableNameOfPointer(prefix, entryPath string) (string, bool, error) {
	return pointerHolderName(prefix, entryPath)
}

// pointerHolderName is the walk a namespace listing does over the pointers below its own key
// prefix: the remainder of a pointer key must be exactly `<name>/.pointer`, and that name is what
// the listing reports. Tables (tableNames) and views (viewNames, views_http.go) list their
// pointers with it, because a pointer is the one entry that makes either object exist.
func pointerHolderName(prefix, entryPath string) (string, bool, error) {
	if !strings.HasPrefix(entryPath, prefix) {
		return "", false, fmt.Errorf("%w: entry %q is not below %q", ErrInvalidPointer, entryPath, prefix)
	}
	rest := strings.TrimPrefix(entryPath, prefix)
	name, tail, ok := strings.Cut(rest, "/")
	if !ok || tail != TablePointerName {
		return "", false, nil
	}
	if err := validateLayoutSegment(name); err != nil {
		return "", false, fmt.Errorf("pointer %q: %w", entryPath, err)
	}
	return name, true, nil
}

// ---------------------------------------------------------------------------------------------
// Table-load entity tags (§2.1 "ETag / If-None-Match", §11 I-5)
// ---------------------------------------------------------------------------------------------
//
// ETags are optional in the spec, but the one requirement it does put on them is load-bearing: the
// tag must vary with the `snapshots` query parameter. `all` and `refs` are two different
// representations of the same table - the second carries only the snapshots a branch or a tag
// points at - so a tag derived from the table alone would let a client that cached `all` be handed
// that representation for a `refs` request, or the reverse: a silently wrong answer instead of a
// 304. The tag below is therefore a digest of both the thing that changes on every commit (the
// pointer's metadata-location) and the mode that selects the representation.
//
// The tag is strong: the same location and the same mode always describe the same document, because
// a metadata.json is never rewritten in place (§6.2, metadata.go) and refs-mode filtering is
// deterministic over it. A client that sends no If-None-Match is unaffected - it gets the full
// response, and the tag is only information it may use later.

// tableLoadETag returns the entity tag of a table-load response for a metadata location and a
// snapshots mode. The digest is quoted, as an ETag header value must be (RFC 9110 §8.8.3).
func tableLoadETag(metadataLocation, snapshotsMode string) string {
	digest := sha256.Sum256([]byte(metadataLocation + "\x1f" + snapshotsMode))
	return `"` + hex.EncodeToString(digest[:]) + `"`
}

// ifNoneMatchMatches reports whether an If-None-Match header value matches etag.
//
// The comparison is the weak one RFC 9110 §13.1.2 defines for If-None-Match: a W/ prefix is
// ignored and only the opaque tag is compared, so a client that stored a weak tag still gets its
// 304. A list is honoured as a list (If-None-Match takes one or more entity tags), and `*` matches
// any current representation.
func ifNoneMatchMatches(header, etag string) bool {
	if header == "" {
		return false
	}
	opaque := etagOpaque(etag)
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if candidate == "*" {
			return true
		}
		if etagOpaque(candidate) == opaque {
			return true
		}
	}
	return false
}

// etagOpaque strips the weak marker and the quotes from one entity tag, so two tags are compared
// by the value they carry rather than by how a client happened to write it down.
func etagOpaque(tag string) string {
	tag = strings.TrimSpace(tag)
	tag = strings.TrimPrefix(tag, "W/")
	return strings.Trim(tag, `"`)
}

// parseSnapshotsMode reads the optional `snapshots` query parameter
// (spec: components.parameters.snapshots). An absent parameter is SnapshotsAll, the spec's
// default; a value outside the enum is a malformed request.
func parseSnapshotsMode(r *http.Request) (string, error) {
	switch raw := r.URL.Query().Get("snapshots"); raw {
	case "", SnapshotsAll:
		return SnapshotsAll, nil
	case SnapshotsRefs:
		return SnapshotsRefs, nil
	default:
		return "", fmt.Errorf("%w: snapshots %q is not one of %q, %q",
			ErrInvalidIdentifier, raw, SnapshotsAll, SnapshotsRefs)
	}
}

// snapshotRef is the part of a snapshot reference object this file needs
// (https://github.com/apache/iceberg/blob/main/format/spec.md, "Snapshot References").
type snapshotRef struct {
	SnapshotID int64 `json:"snapshot-id"`
}

// refsOnlyMetadata rewrites a metadata.json document so that its `snapshots` field carries only
// the snapshots referenced by a branch or a tag, keeping every other field as it was.
//
// The document is handled as raw JSON rather than through a parsed model for two reasons: a field
// this catalog does not interpret (a newer spec's addition) survives untouched, and the values
// are never decoded into a float, so a 64-bit snapshot ID keeps its exact value. Only `snapshots`
// is rewritten, because that is the field the query parameter names; `snapshot-log` and the rest
// of the metadata are passed through.
func refsOnlyMetadata(metadataJSON []byte) ([]byte, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(metadataJSON, &document); err != nil {
		return nil, fmt.Errorf("%w: decode table metadata: %w", ErrInvalidPointer, err)
	}
	rawSnapshots, ok := document["snapshots"]
	if !ok {
		// Metadata of a table without snapshots may omit the field entirely; there is nothing
		// to filter and the document is returned exactly as it was written.
		return metadataJSON, nil
	}
	var snapshots []json.RawMessage
	if err := json.Unmarshal(rawSnapshots, &snapshots); err != nil {
		return nil, fmt.Errorf("%w: decode the snapshots field: %w", ErrInvalidPointer, err)
	}
	retained, err := referencedSnapshotIDs(document)
	if err != nil {
		return nil, err
	}
	kept := make([]json.RawMessage, 0, len(snapshots))
	for _, snapshot := range snapshots {
		var id struct {
			SnapshotID int64 `json:"snapshot-id"`
		}
		if err := json.Unmarshal(snapshot, &id); err != nil {
			return nil, fmt.Errorf("%w: decode a snapshot: %w", ErrInvalidPointer, err)
		}
		if _, ok := retained[id.SnapshotID]; ok {
			kept = append(kept, snapshot)
		}
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return nil, fmt.Errorf("%w: encode the filtered snapshots: %w", ErrInvalidPointer, err)
	}
	document["snapshots"] = encoded
	filtered, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("%w: encode the filtered metadata: %w", ErrInvalidPointer, err)
	}
	return filtered, nil
}

// referencedSnapshotIDs collects the snapshot IDs a refs-mode response must keep: the IDs of every
// entry of the `refs` map, plus `current-snapshot-id`, which the format spec defines as the
// snapshot the `main` branch points at even when `refs` is null.
func referencedSnapshotIDs(document map[string]json.RawMessage) (map[int64]struct{}, error) {
	retained := make(map[int64]struct{})
	if raw, ok := document["current-snapshot-id"]; ok {
		// A pointer: the spec allows current-snapshot-id to be absent, and older writers used
		// null or -1 for "no current snapshot", which must not be decoded as a real ID.
		var current *int64
		if err := json.Unmarshal(raw, &current); err != nil {
			return nil, fmt.Errorf("%w: decode current-snapshot-id: %w", ErrInvalidPointer, err)
		}
		if current != nil && *current >= 0 {
			retained[*current] = struct{}{}
		}
	}
	if raw, ok := document["refs"]; ok {
		var refs map[string]snapshotRef
		if err := json.Unmarshal(raw, &refs); err != nil {
			return nil, fmt.Errorf("%w: decode the refs field: %w", ErrInvalidPointer, err)
		}
		for _, ref := range refs {
			retained[ref.SnapshotID] = struct{}{}
		}
	}
	return retained, nil
}

// checkPrefix validates the {prefix} path segment against the catalog's configured prefix. The
// prefix is a constant of GET /v1/config (§14 Q-I3): a request that sends a different one names a
// warehouse that does not exist, not a table of this catalog.
func (s *tableService) checkPrefix(w http.ResponseWriter, r *http.Request) bool {
	prefix := chi.URLParam(r, "prefix")
	if prefix == s.prefix {
		return true
	}
	writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
		fmt.Sprintf("prefix %q is not the catalog prefix %q", prefix, s.prefix))
	return false
}

// caller builds the authorization caller of a request: the authenticated principal (requireUser
// in http.go already answered a request without one) and the client IP the auth service evaluates
// SourceIp conditions with, extracted the same way pkg/datasets does.
func (s *tableService) caller(w http.ResponseWriter, r *http.Request) (Caller, bool) {
	user, err := auth.GetUser(r.Context())
	if err != nil {
		writeIcebergError(w, http.StatusUnauthorized, ErrTypeNotAuthorized, authFailureMessage(http.StatusUnauthorized))
		return Caller{}, false
	}
	return Caller{
		Username: user.Username,
		ClientIP: httputil.ExtractClientIP(r.Header, r.RemoteAddr),
	}, true
}

// writeError maps the typed errors of this package onto the catalog envelope and the statuses of
// §2.1. A refusal is 403 ForbiddenException; a missing table - on a load, a head, a drop or a
// metrics report - is 404 NoSuchTableException; a missing namespace is 404
// NoSuchNamespaceException; a missing repository or ref is 404 NoSuchWarehouseException; a taken
// name is 409 AlreadyExistsException; a malformed identifier, page token, query value or create
// request is 400; a write the branch or the repository refuses is 403; anything undecided or
// unexpected is 500 with no detail, never a permission (§12).
func (s *tableService) writeError(w http.ResponseWriter, err error) {
	var (
		denied       *PermissionDeniedError
		readOnly     *ReadOnlyRefError
		authzFailure *AuthorizationError
		noTable      *NoSuchTableException
		noNamespace  *NoSuchNamespaceException
	)
	switch {
	case errors.Is(err, ErrInvalidIdentifier), errors.Is(err, ErrInvalidCreateTable),
		errors.Is(err, ErrInvalidTableLocation):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
	case errors.Is(err, ErrAlreadyExists):
		// A create whose name is taken: the table the client asked for exists, so this is not a
		// lost commit the client should retry as-is (§6.2).
		writeIcebergError(w, http.StatusConflict, ErrTypeAlreadyExists, err.Error())
	case errors.As(err, &readOnly):
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, readOnly.Message())
	case errors.As(err, &denied):
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, denied.Error())
	case errors.As(err, &authzFailure):
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the authorization decision failed")
	case errors.As(err, &noTable):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchTable, noTable.Error())
	case errors.As(err, &noNamespace):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchNamespace, noNamespace.Error())
	case errors.Is(err, graveler.ErrWriteToProtectedBranch), errors.Is(err, graveler.ErrReadOnlyRepository):
		// A staging write the repository refuses: a protected branch or an immutable (datasets)
		// repository. The catalog never passes graveler.WithForce, so the core rules apply unchanged
		// (§6.2 "why stage instead of auto-commit").
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, err.Error())
	case isMissingWarehouse(err):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
			"the repository or ref named by the request does not exist")
	default:
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the catalog could not serve the request")
	}
}
