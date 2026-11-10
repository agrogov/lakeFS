package icebergcatalog

// The view endpoints of the Iceberg REST catalog: list, load, exists, create, replace and drop.
//
// Six operations live here:
//
//	GET    /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/views
//	    → the view identifiers below one namespace, pageToken / pageSize in, `next-page-token` out
//	GET    /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/views/{view}
//	    → LoadViewResult{metadata-location, metadata, config}
//	HEAD   /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/views/{view}
//	    → 204 when the view exists, 404 NoSuchViewException when it does not
//	POST   /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/views
//	    CreateViewRequest in, 200 LoadViewResult out, 409 AlreadyExistsException when taken
//	POST   /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/views/{view}
//	    CommitViewRequest in, 200 LoadViewResult out: the replace, i.e. requirements + updates
//	DELETE /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/views/{view}
//	    → 204 after the view's pointer and its metadata entries are removed, 404 NoSuchViewException
//
// The shape of every operation is the table one, deliberately: the same prefix check, the same
// parse-then-authorize-then-resolve order, the same listing filter (FilterViews, the per-view
// catalog:ViewName condition of §7.2), the same "a create needs a namespace marker" rule, the same
// "existence is the pointer entry" rule for HEAD and the drop, and the same LoadResult envelope.
// What differs is the state and the metadata model, and those are views.go's.
//
// The two write endpoints that carry a body differ in one way worth naming: the create carries the
// *name* in the body (so the view ARN cannot be built before the body is read, exactly like
// createTable), while the replace repeats the address the URL already carries (so the body is read
// only after authorization, exactly like the table commit). Requiring both to be read at the same
// point would either leak the name to an unauthorized caller or make an authorized caller's
// address wait on a body it does not need.
//
// The view routes are mounted by http.go's RoutesBuilder through NewViewService, and their endpoint
// strings reach GET /v1/config through the same route table as every other operation, so a view
// operation is advertised exactly when it is served (§2.1). Rename is deliberately absent: it stays a
// §1.2 non-goal, and http.go's checkEndpointScope refuses to mount a route that would advertise it.
//
// Authorization is the §7.2 row of each operation, on the view ARN
// (arn:lakefs:catalog:::view/<repo>/<namespace>/<view>, permissions.CatalogViewArn):
// catalog:ListViews for the listing (filtered per view by catalog:ViewName), catalog:ReadView for
// load and head, catalog:CreateView for create, catalog:UpdateView for replace and
// catalog:DeleteView for drop. The catalog action on the view ARN is the whole permission
// (authz.go: catalog permissions supersede object permissions), so no fs: grant is needed. All
// four are actions the lakeFS permission catalogue already publishes, so a policy written for the
// catalog covers views without a new action name.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, listViews /
// createView / loadView / replaceView / dropView / viewExists, components.schemas.CreateViewRequest,
// LoadViewResult and CommitViewRequest) and the Apache Iceberg view spec
// (https://iceberg.apache.org/view-spec/) only. No code from lakeFS Enterprise or from upstream
// lakeFS >= 1.87 (BSL) was used.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/view"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	// viewsPattern serves ListViews and CreateView and is relative to the catalog mount.
	viewsPattern = "/v1/{prefix}/namespaces/{namespace}/views"

	// viewPattern serves LoadView (GET), ViewExists (HEAD), ReplaceView (POST) and DropView
	// (DELETE) for one view. The method is what tells the four apart.
	viewPattern = "/v1/{prefix}/namespaces/{namespace}/views/{view}"

	// The wire forms advertised in GET /v1/config once this file's routes are mounted.
	listViewsEndpoint   = "GET /v1/{prefix}/namespaces/{namespace}/views"
	loadViewEndpoint    = "GET /v1/{prefix}/namespaces/{namespace}/views/{view}"
	viewExistsEndpoint  = "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}"
	createViewEndpoint  = "POST /v1/{prefix}/namespaces/{namespace}/views"
	replaceViewEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/views/{view}"
	dropViewEndpoint    = "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}"

	// ErrTypeNoSuchView is the Iceberg error type of a missing view, 404
	// (components.schemas.NoSuchViewException, the 404 of loadView, viewExists, replaceView and
	// dropView).
	ErrTypeNoSuchView = "NoSuchViewException"
)

// ErrInvalidCreateView is wrapped by every validation failure of a create-view request that is not
// already an identifier or a location problem: a missing schema, a missing view version, or a
// version whose representations the view spec cannot accept. The HTTP layer maps it to 400
// BadRequestException - the request is refused as sent, and nothing is staged.
var ErrInvalidCreateView = errors.New("invalid iceberg create view request")

// viewService serves the view endpoints. It embeds the table service for the reasons
// registerService does: the prefix check (Q-I3), the caller of a request, the Q-I4 branch
// resolution of a write, the namespace address validation of a listing and the error mapping of
// §2.1 are the same questions, and a second copy of them would be a second answer.
type viewService struct {
	*tableService
}

// NewViewService builds the view surface. The arguments are the table service's: the catalog state
// (Store), the §7.2 matrix (Authorizer), the lakeFS reads a write path needs (TableLister) and the
// configuration (prefix and body limit).
func NewViewService(store *Store, authz *Authorizer, lister TableLister, cfg *Config) *viewService {
	return &viewService{tableService: NewTableService(store, authz, lister, cfg)}
}

// Routes is this file's contribution to the catalog route table. It is its own table, so mounting
// views cannot mount a table operation twice and GET /v1/config advertises exactly the six
// operations below.
func (s *viewService) Routes() []route {
	return []route{
		{
			method:   http.MethodGet,
			pattern:  viewsPattern,
			handler:  s.handleListViews,
			endpoint: listViewsEndpoint,
		},
		{
			method:   http.MethodGet,
			pattern:  viewPattern,
			handler:  s.handleLoadView,
			endpoint: loadViewEndpoint,
		},
		{
			method:   http.MethodHead,
			pattern:  viewPattern,
			handler:  s.handleHeadView,
			endpoint: viewExistsEndpoint,
		},
		{
			method:   http.MethodPost,
			pattern:  viewsPattern,
			handler:  s.handleCreateView,
			endpoint: createViewEndpoint,
		},
		{
			// The replace: requirements and updates in, the new view out. It shares its path with
			// the load, the exists and the drop; only the method tells them apart.
			method:   http.MethodPost,
			pattern:  viewPattern,
			handler:  s.handleReplaceView,
			endpoint: replaceViewEndpoint,
		},
		{
			method:   http.MethodDelete,
			pattern:  viewPattern,
			handler:  s.handleDropView,
			endpoint: dropViewEndpoint,
		},
	}
}

// viewIdentifier is TableIdentifier as a view listing carries it (spec:
// components.schemas.ListTablesResponse, which listViews returns): the namespace the view lives in
// and its name, in the client's own terms.
type viewIdentifier struct {
	Namespace Namespace `json:"namespace"`
	Name      string    `json:"name"`
}

// listViewsResponse is the body of ListViews (spec: components.schemas.ListTablesResponse).
type listViewsResponse struct {
	Identifiers   []viewIdentifier `json:"identifiers"`
	NextPageToken string           `json:"next-page-token,omitempty"`
}

// loadViewResult is LoadViewResult (spec: components.schemas.LoadViewResult): the physical
// location of the current metadata document, the document itself, and the view-specific client
// configuration. metadata is a json.RawMessage so the document goes out as it was written.
type loadViewResult struct {
	MetadataLocation string            `json:"metadata-location"`
	Metadata         json.RawMessage   `json:"metadata"`
	Config           map[string]string `json:"config"`
}

// createViewRequest is CreateViewRequest (spec: components.schemas.CreateViewRequest): the view to
// create in the namespace the URL addresses, its schema, its first version, and the optional
// members that shape the initial metadata document.
//
// The schema is decoded into iceberg.Schema and the version into view.Version - the *bound* forms,
// unlike createTable's unbound partition spec and sort order. A view schema has no placeholder
// source IDs to resolve (there is no partition spec to point at a field), and a view version names
// a schema by ID, so the client's own IDs are already the ones the document will carry.
type createViewRequest struct {
	// Name is the view's name. Create addresses a namespace, so the name is in the body.
	Name string `json:"name"`

	// Schema is the initial schema of the view. It is required by the spec and by this catalog:
	// a view without a schema has nothing to type its versions against.
	Schema *iceberg.Schema `json:"schema"`

	// Location is the view's base location. Optional: an absent location becomes the managed
	// <storage_namespace>/_iceberg/<view-uuid>, and a location outside the catalog's table area is
	// 400 (location.go).
	Location string `json:"location,omitempty"`

	// Properties are the initial view properties.
	Properties iceberg.Properties `json:"properties,omitempty"`

	// ViewVersion is the first version of the view: its SQL representations, default namespace
	// and summary. The catalog assigns version-id 1 (the view spec requires the initial version to
	// be 1), so a client-sent value is normalized rather than trusted.
	ViewVersion *view.Version `json:"view-version"`
}

// handleListViews is GET /v1/{prefix}/namespaces/{namespace}/views: the view identifiers exactly
// one level below the addressed namespace.
//
// The order is the table listing's, for the same reasons: parse, list, authorize per name (so a
// caller that may not list the namespace gets 403 rather than an empty page), then paginate. A
// namespace with neither a view nor a marker does not exist (404 NoSuchNamespaceException); one with
// a marker and no view is an empty listing.
func (s *viewService) handleListViews(w http.ResponseWriter, r *http.Request) {
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
		s.writeViewError(w, err)
		return
	}

	names, err := s.viewNames(r.Context(), addr)
	if err != nil {
		s.writeViewError(w, err)
		return
	}
	if len(names) == 0 {
		if _, err := s.store.GetNamespace(r.Context(), addr); err != nil {
			s.writeViewError(w, err)
			return
		}
	}

	visible, err := s.authz.FilterViews(r.Context(), caller, addr, names)
	if err != nil {
		s.writeViewError(w, err)
		return
	}

	identifiers := make([]viewIdentifier, 0, len(visible))
	for _, name := range visible {
		identifiers = append(identifiers, viewIdentifier{
			Namespace: clientNamespace(r, fullNamespace(addr, "")),
			Name:      name,
		})
	}
	page, next, err := paginate(identifiers, r.URL.Query().Get("pageToken"), pageSize)
	if err != nil {
		s.writeViewError(w, err)
		return
	}
	if page == nil {
		page = []viewIdentifier{}
	}
	writeJSON(w, http.StatusOK, listViewsResponse{Identifiers: page, NextPageToken: next})
}

// viewNames returns the names of the views in addr.Namespace on addr.Ref, in path order.
//
// It is tableNames for the views directory: a view exists exactly when its
// `_lakefs_iceberg/views/<ns…>/<view>/.pointer` entry does, so the walk below the namespace's key
// prefix collects every pointer path whose remainder is exactly `<view>/.pointer`, and a pointer
// deeper than that belongs to a nested namespace.
func (s *viewService) viewNames(ctx context.Context, addr Address) ([]string, error) {
	prefix := viewEntryPrefix(addr.Namespace)
	names := make([]string, 0)
	var after string
	for {
		page, hasMore, err := s.lister.ListEntries(ctx, addr.Repo, addr.Ref, prefix, after, "", catalog.ListEntriesLimitMax)
		if err != nil {
			return nil, err
		}
		for _, entry := range page {
			name, ok, err := pointerHolderName(prefix, entry.Path)
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

// handleLoadView is GET /v1/{prefix}/namespaces/{namespace}/views/{view} (spec: loadView): the
// LoadViewResult of the view on the addressed ref.
func (s *viewService) handleLoadView(w http.ResponseWriter, r *http.Request) {
	loaded, ok := s.authorizeView(w, r)
	if !ok {
		return
	}
	if len(loaded.MetadataJSON) == 0 {
		// Unreachable for a store-built View: the bytes are kept precisely so the response does
		// not depend on re-marshalling.
		s.writeViewError(w, fmt.Errorf("%w: the view has no metadata document", ErrInvalidPointer))
		return
	}
	writeJSON(w, http.StatusOK, loadViewResult{
		// The physical URI of the pointer's metadata document, never a lakeFS path: the client
		// reads it with its own object-store client.
		MetadataLocation: loaded.Pointer.MetadataLocation,
		Metadata:         json.RawMessage(loaded.MetadataJSON),
		Config:           map[string]string{},
	})
}

// handleHeadView is HEAD /v1/{prefix}/namespaces/{namespace}/views/{view} (spec: viewExists): 204
// with no body when the view exists, 404 NoSuchViewException when it does not.
//
// Existence is decided by the same authorized read the load performs, so HEAD cannot answer 204 for
// a view the load would fail on: a pointer whose document is unreadable is not a view this catalog
// can serve.
func (s *viewService) handleHeadView(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorizeView(w, r); !ok {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleCreateView is POST /v1/{prefix}/namespaces/{namespace}/views (spec: createView): a new
// view's initial metadata document, its entry and its pointer staged on the branch with
// assert-create semantics, and the LoadViewResult of the spec's CreateViewResponse.
func (s *viewService) handleCreateView(w http.ResponseWriter, r *http.Request) {
	if !s.checkPrefix(w, r) {
		return
	}
	addr, ok := s.namespaceAddress(w, r)
	if !ok {
		return
	}

	var request createViewRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}
	name, err := decodeViewName(request.Name)
	if err != nil {
		s.writeViewError(w, err)
		return
	}
	addr.View = name

	caller, ok := s.caller(w, r)
	if !ok {
		return
	}
	isBranch, err := s.isBranchRef(r.Context(), addr)
	if err != nil {
		s.writeViewError(w, err)
		return
	}
	if err := s.authz.Authorize(r.Context(), caller, Request{
		Op:          OpCreateView,
		Address:     addr,
		RefIsBranch: isBranch,
	}); err != nil {
		s.writeViewError(w, err)
		return
	}
	if !s.requireWritableNamespace(w, addr) {
		return
	}

	result, err := s.createView(r.Context(), addr, request)
	if err != nil {
		s.writeViewError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// createView is the create itself, once the request is parsed and authorized: initialize the
// document, write it, register it, and stage the pointer (views.go, store.CreateViewWithMetadata).
//
// A namespace has to exist for a view to be created in it, exactly as for a table: §6.1 defines a
// namespace by its marker, and the spec's createView answers 404 NoSuchNamespaceException for one
// that does not. Every failure before the pointer write leaves nothing a reader can see; the
// unreferenced document of a lost create is what GC collects, like a lost commit's.
func (s *viewService) createView(ctx context.Context, addr Address, request createViewRequest) (*loadViewResult, error) {
	if request.Schema == nil {
		return nil, fmt.Errorf("%w: the request carries no schema", ErrInvalidCreateView)
	}
	if request.ViewVersion == nil {
		return nil, fmt.Errorf("%w: the request carries no view-version", ErrInvalidCreateView)
	}
	if _, err := s.store.GetNamespace(ctx, addr); err != nil {
		return nil, err
	}

	viewUUID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate a view uuid: %w", err)
	}
	location, err := s.store.ViewLocation(ctx, addr.Repo, request.Location, viewUUID.String())
	if err != nil {
		return nil, err
	}
	metadata, err := view.NewMetadataWithUUID(request.ViewVersion, request.Schema, location,
		request.Properties, viewUUID)
	if err != nil {
		return nil, fmt.Errorf("%w: the schema or view version cannot describe a view: %w",
			ErrInvalidCreateView, err)
	}
	if version := metadata.FormatVersion(); version != SupportedViewFormatVersion {
		return nil, fmt.Errorf("%w: view format version %d is not supported: this catalog serves view format version %d",
			ErrInvalidCreateView, version, SupportedViewFormatVersion)
	}
	metadataLocation, err := NewViewMetadataFileLocation(location, "", metadata)
	if err != nil {
		return nil, err
	}
	document, err := s.store.WriteViewMetadata(ctx, addr.Repo, metadataLocation, metadata)
	if err != nil {
		return nil, err
	}
	pointer := ViewPointer{
		MetadataLocation: metadataLocation,
		ViewUUID:         viewUUID.String(),
		FormatVersion:    metadata.FormatVersion(),
	}
	// The document's entry is staged before the pointer, so a failure of file registration never
	// leaves a pointer naming an unregistered document.
	if err := s.store.CreateViewWithMetadata(ctx, addr, pointer, location, int64(len(document))); err != nil {
		return nil, err
	}
	return &loadViewResult{
		MetadataLocation: metadataLocation,
		Metadata:         json.RawMessage(document),
		Config:           map[string]string{},
	}, nil
}

// handleReplaceView is POST /v1/{prefix}/namespaces/{namespace}/views/{view} (spec: replaceView):
// apply one view commit - requirements and updates - and answer with the new LoadViewResult.
//
// The order is authorization, then body, then commit: the request repeats the address the URL
// already carries, so no part of the body is needed to build the view ARN, and a caller without
// catalog:UpdateView learns nothing about the body it sent. A body over iceberg.max_request_body is
// 413 (read commit body), a body the view model cannot decode is 400, and the commit's own
// classifications are Store.CommitView's: 409 for a failed requirement or a lost compare-and-swap,
// 400 for an update the view forbids, and 500 CommitStateUnknownException for a failure after the
// document was written.
func (s *viewService) handleReplaceView(w http.ResponseWriter, r *http.Request) {
	addr, ok := s.authorizeViewWrite(w, r, OpUpdateView)
	if !ok {
		return
	}
	if !s.requireWritableNamespace(w, addr) {
		return
	}
	body, ok := s.readCommitBody(w, r)
	if !ok {
		return
	}
	request, err := decodeViewCommitRequest(body)
	if err != nil {
		s.writeViewError(w, err)
		return
	}
	result, err := s.store.CommitView(r.Context(), addr, request)
	if err != nil {
		s.writeViewError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, loadViewResult{
		MetadataLocation: result.Pointer.MetadataLocation,
		Metadata:         json.RawMessage(result.MetadataJSON),
		Config:           map[string]string{},
	})
}

// decodeViewCommitRequest decodes the body of a replace. A body that is not a view commit request -
// an unknown update action, an unknown requirement type, a missing required field - is a 400
// BadRequestException, and it fails here rather than half-way through the replace.
func decodeViewCommitRequest(body []byte) (ViewCommitRequest, error) {
	var request ViewCommitRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return ViewCommitRequest{}, &BadRequestError{
			Message: "malformed commit request",
			Err:     err,
		}
	}
	return request, nil
}

// handleDropView is DELETE /v1/{prefix}/namespaces/{namespace}/views/{view} (spec: dropView):
// remove the view's pointer and the metadata entries staged for it from the branch, and answer 204
// with no body.
//
// The spec's `purgeRequested` query parameter is accepted and has no effect whatever its value,
// exactly as on the table drop: no object is deleted. The entries that referenced the view's
// documents are removed, which is what hands those objects to the committed GC once nothing else
// references them (§6.3, §14 Q-I8). A view dropped on one branch stays readable on a branch or a tag
// whose pointer still names its document, and reverting the commit that carries the drop brings it
// back.
func (s *viewService) handleDropView(w http.ResponseWriter, r *http.Request) {
	addr, ok := s.authorizeViewWrite(w, r, OpDeleteView)
	if !ok {
		return
	}
	if !s.requireWritableNamespace(w, addr) {
		return
	}
	if err := s.dropView(r.Context(), addr); err != nil {
		s.writeViewError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// dropView removes the two kinds of entry that make up a view on the branch: its pointer and every
// staged metadata entry under _lakefs_iceberg/files/<view-uuid>/.
//
// The order is the table drop's and it is the conservative one: the pointer first, then the files.
// A failure between the two steps leaves a view that does not exist whose documents are still
// protected - exactly the state before this code existed, so nothing can be lost - while the other
// order would leave a live view whose documents are no longer protected.
func (s *viewService) dropView(ctx context.Context, addr Address) error {
	pointerPath, entry, err := s.requireViewEntry(ctx, addr)
	if err != nil {
		return err
	}
	viewUUID, err := viewUUIDOfPointer(pointerPath, entry)
	if err != nil {
		return err
	}
	if err := s.lister.DeleteEntry(ctx, addr.Repo, addr.Ref, pointerPath); err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			// Another writer removed the pointer between the read and the delete.
			return &NoSuchViewException{
				Repo:      addr.Repo,
				Ref:       addr.Ref,
				Namespace: addr.Namespace,
				View:      addr.View,
			}
		}
		return err
	}
	_, err = releaseViewMetadata(ctx, s.lister, addr, viewUUID, DefaultStageWorkers)
	return err
}

// viewUUIDOfPointer returns the view UUID the pointer entry of pointerPath carries, i.e. the
// directory name of the view's metadata entries in the reserved area.
//
// It decodes the pointer's own user metadata and never reads the document the pointer names: a drop
// has to be able to remove a pointer that names a document this catalog cannot read.
func viewUUIDOfPointer(pointerPath string, entry *catalog.DBEntry) (uuid.UUID, error) {
	pointer, err := DecodeViewPointer(entry.Metadata)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode view pointer %q: %w", pointerPath, err)
	}
	viewUUID, err := uuid.Parse(pointer.ViewUUID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode view pointer %q: %w: %q is not a view uuid: %w",
			pointerPath, ErrInvalidPointer, pointer.ViewUUID, err)
	}
	return viewUUID, nil
}

// requireViewEntry returns the reserved key of the view pointer of addr and the entry it read, or
// NoSuchViewException when the pointer is not present on addr.Ref.
func (s *viewService) requireViewEntry(ctx context.Context, addr Address) (string, *catalog.DBEntry, error) {
	pointerPath, err := viewPointerPath(addr)
	if err != nil {
		return "", nil, err
	}
	entry, err := s.lister.GetEntry(ctx, addr.Repo, addr.Ref, pointerPath, catalog.GetEntryParams{})
	if err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			return "", nil, &NoSuchViewException{
				Repo:      addr.Repo,
				Ref:       addr.Ref,
				Namespace: addr.Namespace,
				View:      addr.View,
			}
		}
		return "", nil, err
	}
	return pointerPath, entry, nil
}

// authorizeView parses, authorizes and resolves the view address of GET and HEAD, writing the error
// itself when a step fails. The two verbs share it so they cannot diverge on what they accept, who
// may ask, or what "exists" means.
func (s *viewService) authorizeView(w http.ResponseWriter, r *http.Request) (*View, bool) {
	if !s.checkPrefix(w, r) {
		return nil, false
	}
	addr, err := ParseView(chi.URLParam(r, "namespace"), chi.URLParam(r, "view"))
	if err != nil {
		s.writeViewError(w, err)
		return nil, false
	}
	caller, ok := s.caller(w, r)
	if !ok {
		return nil, false
	}
	if err := s.authz.Authorize(r.Context(), caller, Request{Op: OpReadView, Address: addr}); err != nil {
		s.writeViewError(w, err)
		return nil, false
	}
	loaded, err := s.store.GetView(r.Context(), addr)
	if err != nil {
		s.writeViewError(w, err)
		return nil, false
	}
	return loaded, true
}

// authorizeViewWrite parses and authorizes the view address of a body-less write endpoint (the
// drop) and of a body-carrying one whose body is read afterwards (the replace), writing the error
// itself when a step fails.
//
// The ref is resolved before the decision so that Request.RefIsBranch is truthful (Q-I4): a write
// needs a branch, and a caller without the action gets the same 403 for a branch and a tag alike.
func (s *viewService) authorizeViewWrite(w http.ResponseWriter, r *http.Request, op Operation) (Address, bool) {
	if !s.checkPrefix(w, r) {
		return Address{}, false
	}
	addr, err := ParseView(chi.URLParam(r, "namespace"), chi.URLParam(r, "view"))
	if err != nil {
		s.writeViewError(w, err)
		return Address{}, false
	}
	caller, ok := s.caller(w, r)
	if !ok {
		return Address{}, false
	}
	isBranch, err := s.isBranchRef(r.Context(), addr)
	if err != nil {
		s.writeViewError(w, err)
		return Address{}, false
	}
	if err := s.authz.Authorize(r.Context(), caller, Request{
		Op:          op,
		Address:     addr,
		RefIsBranch: isBranch,
	}); err != nil {
		s.writeViewError(w, err)
		return Address{}, false
	}
	return addr, true
}

// writeViewError maps the errors of the view endpoints onto the catalog envelope and the statuses
// of §2.1:
//
//   - a missing view - on a load, a head, a replace or a drop - is 404 NoSuchViewException;
//   - a create whose name is taken is 409 AlreadyExistsException (writeError's ErrAlreadyExists);
//   - a malformed create request is 400 BadRequestException;
//   - everything else is the table path's error: the commit classifications of a replace
//     (writeCommitError), then the shared mapping of identifiers, locations, permissions,
//     read-only refs and the reserved-prefix refusals (writeError).
func (s *viewService) writeViewError(w http.ResponseWriter, err error) {
	var noView *NoSuchViewException
	switch {
	case errors.As(err, &noView):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchView, noView.Error())
	case errors.Is(err, ErrInvalidCreateView):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
	default:
		s.writeCommitError(w, err)
	}
}
