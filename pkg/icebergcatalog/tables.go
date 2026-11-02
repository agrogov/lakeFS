package icebergcatalog

// Table reads for the Iceberg REST catalog (ICEBERG_CATALOG_PLAN.md §2.1 "Protocol facts",
// §5 "Addressing rules", §7.2 "Authorization" and §11 I-1 "tables list/load/head on any ref").
//
// Three operations live here, and they are the read half of table addressing (§5 row 3):
//
//	GET  /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables
//	    → the table identifiers below one namespace, pageToken / pageSize in, `next-page-token` out
//	GET  /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables/{table}
//	    → LoadTableResult{metadata-location, metadata, config}
//	HEAD /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables/{table}
//	    → 204 when the table exists, 404 NoSuchTableException when it does not
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
// Authorization is the §7.2 row for each operation - catalog:ListTables filtered per table by the
// catalog:TableName condition, and catalog:ReadTable on the table ARN - plus the fs:ReadRepository
// every catalog request needs. The listing asks Authorizer.FilterTables, which evaluates the
// namespace-level gate and the per-table condition in one place, so the endpoint cannot return a
// table the caller may not see. Load and head are authorized before the pointer is resolved, so a
// caller without catalog:ReadTable cannot distinguish a missing table from a forbidden one (both
// requests are 403 for it, §12).
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, listTables /
// loadTable / tableExists, and components.parameters.snapshots), the Apache Iceberg table format
// spec (https://github.com/apache/iceberg/blob/main/format/spec.md, "Table Metadata" and
// "Snapshot References") and the public lakeFS documentation only. No code from lakeFS Enterprise
// or from upstream lakeFS >= 1.87 (BSL) was used.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/httputil"
)

const (
	// tablesPattern serves ListTables and is relative to the catalog mount (APIBasePath).
	// {prefix} is the value GET /v1/config advertised as overrides.prefix (§14 Q-I3).
	tablesPattern = "/v1/{prefix}/namespaces/{namespace}/tables"

	// tablePattern serves LoadTable (GET) and TableExists (HEAD) for one table.
	tablePattern = "/v1/{prefix}/namespaces/{namespace}/tables/{table}"

	// The wire forms advertised in GET /v1/config once this file's routes are mounted. They are
	// the operation paths of the REST OpenAPI spec with the catalog's {prefix} segment.
	listTablesEndpoint  = "GET /v1/{prefix}/namespaces/{namespace}/tables"
	loadTableEndpoint   = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
	tableExistsEndpoint = "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"

	// ErrTypeNoSuchTable is the Iceberg error type of a missing table, 404
	// (components.schemas.NoSuchTableException, the 404 of loadTable and tableExists).
	ErrTypeNoSuchTable = "NoSuchTableException"

	// SnapshotsAll is the default value of the `snapshots` query parameter: every snapshot
	// currently valid for the table. It is also what an absent parameter means.
	SnapshotsAll = "all"

	// SnapshotsRefs is the value that returns only the snapshots referenced by a branch or a
	// tag (components.parameters.snapshots).
	SnapshotsRefs = "refs"
)

// TableLister is the listing surface the table read endpoints need in addition to Store: the
// entry walk that finds the table pointers of a namespace. It is a separate interface rather than
// more methods on Store because it is a lakeFS listing, not catalog state, and *catalog.Catalog
// (also the NamespaceLister of the namespace endpoints) implements it.
type TableLister interface {
	ListEntries(ctx context.Context, repositoryID string, reference string, prefix string, after string, delimiter string, limit int) ([]*catalog.DBEntry, bool, error)
}

// tableService serves the table read endpoints of §5. It owns the three things a handler needs:
// the catalog state (Store), the authorization matrix of §7.2 (Authorizer) and the pointer walk
// (TableLister).
type tableService struct {
	store  *Store
	authz  *Authorizer
	lister TableLister
	prefix string
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
	return &tableService{store: store, authz: authz, lister: lister, prefix: prefix}
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
	}
}

// tableIdentifier is TableIdentifier (spec: components.schemas.TableIdentifier): the namespace
// the table lives in and its name. The namespace carries the repository and ref levels, because
// that is how this catalog addresses a table (§5) and what a client has to send back to load it.
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
			Namespace: fullNamespace(addr, ""),
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
	if !strings.HasPrefix(entryPath, prefix) {
		return "", false, fmt.Errorf("%w: entry %q is not below %q", ErrInvalidPointer, entryPath, prefix)
	}
	rest := strings.TrimPrefix(entryPath, prefix)
	name, tail, ok := strings.Cut(rest, "/")
	if !ok || tail != TablePointerName {
		return "", false, nil
	}
	if err := validateLayoutSegment(name); err != nil {
		return "", false, fmt.Errorf("table pointer %q: %w", entryPath, err)
	}
	return name, true, nil
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
// §2.1. A refusal is 403 ForbiddenException; a missing table is 404 NoSuchTableException; a
// missing namespace is 404 NoSuchNamespaceException; a missing repository or ref is 404
// NoSuchWarehouseException; a malformed identifier, page token or query value is 400; anything
// undecided or unexpected is 500 with no detail, never a permission (§12).
func (s *tableService) writeError(w http.ResponseWriter, err error) {
	var (
		denied       *PermissionDeniedError
		readOnly     *ReadOnlyRefError
		authzFailure *AuthorizationError
		noTable      *NoSuchTableException
		noNamespace  *NoSuchNamespaceException
	)
	switch {
	case errors.Is(err, ErrInvalidIdentifier):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
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
	case isMissingWarehouse(err):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
			"the repository or ref named by the request does not exist")
	default:
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the catalog could not serve the request")
	}
}
