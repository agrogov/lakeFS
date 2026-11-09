package icebergcatalog

// Namespace reads and writes for the Iceberg REST catalog (ICEBERG_CATALOG_PLAN.md §5
// "Addressing rules", §6.1 "Layout on the branch", §7.2 "Authorization", §11 I-1 "namespaces
// list/get/head (repo/ref levels)" and §11 I-2 "namespaces create/update-properties/drop").
//
// Iceberg has no repository or branch level: a namespace is an ordered list of levels and a table
// is a namespace plus a name. lakeFS addresses a repository and a ref on top of that, so the
// catalog's namespace tree has two macro levels that ident.go inserts before the real namespace
// levels (§5, "Levels 1 and 2 are always repo and ref"):
//
//	GET /v1/{prefix}/namespaces
//	    no parent          → the repositories the caller may read
//	    parent=<repo>      → the branches of that repository
//	    parent=<repo><sep><ref> → the real namespaces below that ref
//
//	GET  /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}
//	HEAD /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}
//	    → the namespace object of §5, with the properties carried by its marker entry
//
//	POST   /v1/{prefix}/namespaces
//	    → stage the marker of a new namespace, with its properties (I2-2)
//	POST   /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/properties
//	    → merge an update into the properties of an existing marker (I2-2)
//	DELETE /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}
//	    → remove an empty namespace marker (I2-2)
//
// The write half of this file is the HTTP and authorization layer of §6.2's staging writes: the
// store creates and updates the marker (store.go, I2-1), and the lakeFS catalog removes it. This
// file decides which marker is written or removed and who may do it, keeps the properties merge of
// the spec, and never assumes a client's precondition (a drop checks the tables under the marker
// itself). Writes are staged on the branch and become part of the repository on an explicit lakeFS
// commit, and they are refused on a tag or a commit (Q-I4, enforced by authz.go from I1-6).
//
// The macro-level mapping lives in exactly one place, ident.go: ParseNamespace turns the
// namespace path parameter (or the `parent` query parameter) into an Address whose Repo and Ref
// are the first two levels. This file only switches on which of those levels is present, so a
// client cannot skip, reorder or replace a macro level by adding namespace levels - the repository
// and ref are always the levels the parser chose, never ones smuggled in as a namespace
// (ICEBERG_CATALOG_PLAN.md §5 and the security property documented in ident.go).
//
// Listing rules of §5 and §7.2:
//
//   - a listing with no parent is a lakeFS repository list; RequiredNode authorizes it with
//     fs:ListRepositories on All, and every repository is then filtered with the same
//     Authorizer the rest of the catalog uses, so a caller only sees repositories it can read;
//   - a listing with parent <repo> is authorized on the repository-level namespace ARN
//     (catalog:ListNamespaces on CatalogNamespaceArn(repo, "")), and each branch is filtered
//     through the same helper (the Authorizer) rather than returned unconditionally;
//   - a listing with parent <repo><sep><ref> is authorized on the parent namespace ARN, which is
//     exactly the §7.2 row "list namespaces under repo.ref";
//   - get and head are authorized with catalog:GetNamespace on the namespace ARN.
//
// The write rules of §7.2 and Q-I4:
//
//   - create, update-properties and drop are authorized with
//     catalog:Create/Update/DeleteNamespace on the namespace ARN, plus fs:ReadRepository and
//     fs:WriteObject on the reserved _lakefs_iceberg/ area (authz.go, RequiredNode);
//   - a write must address a branch. A tag or a commit answers 403 ForbiddenException "ref is
//     read-only", and a ref that resolves to nothing answers 404 NoSuchWarehouseException, the
//     same answer a read of it gets.
//
// Reads work on any ref (branch, tag or commit), which is why the store is asked for the marker
// on Address.Ref rather than on a branch (§1.1 goal 1). Properties are the user metadata of the
// `_lakefs_iceberg/ns/<ns>/.namespace` marker entry (§6.1): no second lookup is needed.
//
// Pagination follows the REST OpenAPI spec's ListNamespacesRequest/ListNamespacesResponse
// (pageToken/pageSize in, `next-page-token` out). The token is opaque to clients; this
// implementation encodes an offset into the filtered result, so a client that never sends
// pageSize still gets one page and a client that does can walk the whole listing.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml,
// ListNamespaces / GetNamespace / NamespaceExists / CreateNamespace / UpdateProperties /
// DropNamespace) and the public lakeFS documentation only. No code from lakeFS Enterprise or from
// upstream lakeFS >= 1.87 was used.

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
)

const (
	// namespacesPattern serves ListNamespaces and is relative to the catalog mount
	// (APIBasePath). {prefix} is the value GET /v1/config advertised as overrides.prefix; a
	// different value cannot name this catalog (§14 Q-I3).
	namespacesPattern = "/v1/{prefix}/namespaces"

	// namespacePattern serves GetNamespace (GET), NamespaceExists (HEAD) and DropNamespace
	// (DELETE) for one namespace.
	namespacePattern = "/v1/{prefix}/namespaces/{namespace}"

	// namespacePropertiesPattern serves UpdateProperties (POST), which changes the properties of
	// one existing namespace.
	namespacePropertiesPattern = "/v1/{prefix}/namespaces/{namespace}/properties"

	// The wire forms advertised in GET /v1/config once this file's routes are mounted. They
	// come from the REST OpenAPI spec's operation paths (components.paths) with the catalog's
	// {prefix} segment; the route table builds the same string so a client can match one against
	// the other.
	listNamespacesEndpoint            = "GET /v1/{prefix}/namespaces"
	getNamespaceEndpoint              = "GET /v1/{prefix}/namespaces/{namespace}"
	headNamespaceEndpoint             = "HEAD /v1/{prefix}/namespaces/{namespace}"
	createNamespaceEndpoint           = "POST /v1/{prefix}/namespaces"
	updateNamespacePropertiesEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/properties"
	dropNamespaceEndpoint             = "DELETE /v1/{prefix}/namespaces/{namespace}"

	// ErrTypeNoSuchNamespace is the Iceberg error type of a missing namespace, 404
	// (components.schemas.NoSuchNamespaceException).
	ErrTypeNoSuchNamespace = "NoSuchNamespaceException"

	// ErrTypeNamespaceAlreadyExists is the Iceberg error type of a create on a namespace name
	// that is taken, 409 (§11 I-2; the spec's CreateNamespaceResponse lists
	// NamespaceAlreadyExistsException as the model of its 409).
	ErrTypeNamespaceAlreadyExists = "NamespaceAlreadyExistsException"

	// ErrTypeNamespaceNotEmpty is the Iceberg error type of a drop of a namespace that still
	// holds tables, 409 (components.examples.NamespaceNotEmptyError).
	ErrTypeNamespaceNotEmpty = "NamespaceNotEmptyException"

	// ErrTypeUnprocessableEntity is the Iceberg error type of a properties request the server
	// cannot process, 422: a key that appears in both the removals and the updates
	// (components.examples.UnprocessableEntityDuplicateKey).
	ErrTypeUnprocessableEntity = "UnprocessableEntityException"

	// namespaceListDelimiter is the listing delimiter used to collapse the marker tree below a
	// parent namespace into one entry per immediate child namespace: every
	// `ns/<parent>/<child>/…` path is reported as the common prefix `ns/<parent>/<child>/`
	// (pkg/catalog/entry_listing_iterator.go), so the child name is the only path component left
	// after the parent prefix.
	namespaceListDelimiter = "/"

	// namespaceChildPageSize is the page size of the child-namespace walk of a drop check (§11
	// I-2). A namespace rarely has more than a handful of children, and the walk still paginates,
	// so a small page keeps the emptiness check cheap without capping how many children it sees.
	namespaceChildPageSize = 32
)

// NamespaceLister is the lakeFS surface the namespace endpoints need in addition to Store. It is a
// separate interface rather than more methods on Store because these calls are lakeFS-level
// operations (repository, branch and entry listings, ref resolution and the one entry removal the
// write half performs), not catalog state; *catalog.Catalog implements all of them. Keeping it
// explicit means the HTTP layer cannot accidentally use a lakeFS path the store does not provide.
type NamespaceLister interface {
	ListRepositories(ctx context.Context, limit int, prefix, searchString, after string, opts ...catalog.ListRepositoriesOptionsFunc) ([]*catalog.Repository, bool, error)
	ListBranches(ctx context.Context, repositoryID string, prefix string, limit int, after string, opts ...graveler.ListOptionsFunc) ([]*catalog.Branch, bool, error)
	ListEntries(ctx context.Context, repositoryID string, reference string, prefix string, after string, delimiter string, limit int) ([]*catalog.DBEntry, bool, error)

	// BranchExists reports whether ref names a branch of the repository. A write may only
	// address a branch (Q-I4), so this is the call that decides whether a write is allowed on
	// the ref at all.
	BranchExists(ctx context.Context, repositoryID string, branch string) (bool, error)

	// GetCommit resolves a ref - branch, tag or commit ID - to a commit, or reports that it
	// resolves to nothing. The write half asks it after BranchExists answered false, so a tag
	// or a commit is refused as a read-only ref while a ref that exists nowhere is a missing
	// warehouse (404), exactly as a read of the same ref would answer.
	GetCommit(ctx context.Context, repositoryID string, reference string) (*catalog.CommitLog, error)

	// DeleteEntry stages the removal of one entry on a branch: dropping a namespace removes its
	// marker. It is called only after the namespace was read and found empty, and it carries the
	// caller's SetOptionsFuncs so a protected branch can still refuse it.
	DeleteEntry(ctx context.Context, repositoryID string, branch string, path string, opts ...graveler.SetOptionsFunc) error
}

// namespaceService serves the namespace read endpoints of §5. It owns the three things a handler
// needs and nothing else: the catalog state (Store), the authorization matrix of §7.2
// (Authorizer) and the lakeFS listings of the macro levels (NamespaceLister).
type namespaceService struct {
	store  *Store
	authz  *Authorizer
	lister NamespaceLister
	prefix string
}

// NewNamespaceService builds the namespace read surface. The prefix is the value of GET
// /v1/config overrides.prefix, i.e. the prefix a client sends back in every path; an empty
// configuration falls back to CatalogPrefix, exactly like server.prefix (http.go).
//
// It is exported because http.go's route table (server.ownRoutes) is assembled in a later task:
// the constructor and Routes are the whole integration surface, so mounting the read path is one
// entry in that table and needs no change to this file.
func NewNamespaceService(store *Store, authz *Authorizer, lister NamespaceLister, cfg *Config) *namespaceService {
	prefix := ""
	if cfg != nil {
		prefix = cfg.Prefix
	}
	if prefix == "" {
		prefix = CatalogPrefix
	}
	return &namespaceService{store: store, authz: authz, lister: lister, prefix: prefix}
}

// Routes is this file's contribution to the catalog route table (server.ownRoutes in http.go).
// The endpoint strings are the compatibility contract of §2.1: they are advertised in
// GET /v1/config only once the routes are actually mounted, which the same table change does.
func (n *namespaceService) Routes() []route {
	return []route{
		{
			method:   http.MethodGet,
			pattern:  namespacesPattern,
			handler:  n.handleListNamespaces,
			endpoint: listNamespacesEndpoint,
		},
		{
			method:   http.MethodGet,
			pattern:  namespacePattern,
			handler:  n.handleGetNamespace,
			endpoint: getNamespaceEndpoint,
		},
		{
			method:   http.MethodHead,
			pattern:  namespacePattern,
			handler:  n.handleHeadNamespace,
			endpoint: headNamespaceEndpoint,
		},
		{
			method:   http.MethodPost,
			pattern:  namespacesPattern,
			handler:  n.handleCreateNamespace,
			endpoint: createNamespaceEndpoint,
		},
		{
			method:   http.MethodPost,
			pattern:  namespacePropertiesPattern,
			handler:  n.handleUpdateNamespaceProperties,
			endpoint: updateNamespacePropertiesEndpoint,
		},
		{
			method:   http.MethodDelete,
			pattern:  namespacePattern,
			handler:  n.handleDeleteNamespace,
			endpoint: dropNamespaceEndpoint,
		},
	}
}

// listNamespacesResponse is ListNamespacesResponse (spec: components.schemas.ListNamespacesResponse).
// Namespace is the spec's Namespace type: an array of levels, in the client's own terms. On the
// /iceberg/api form the macro levels are part of every entry - a branch level entry is
// [repo, branch] and a namespace level entry is [repo, ref, ns…] - the same shape a client sent as
// `parent`, with one more level. A relative_to client named neither macro level (its URL did), so
// it gets back the levels it sent (relative.go clientNamespace).
type listNamespacesResponse struct {
	Namespaces    []Namespace `json:"namespaces"`
	NextPageToken string      `json:"next-page-token,omitempty"`
}

// getNamespaceResponse is GetNamespaceResponse (spec: components.schemas.GetNamespaceResponse).
// Namespace is the address the client named, in its own terms: the full [repo, ref, ns…] path on
// the /iceberg/api form, and the levels below the warehouse on a relative_to form. Properties is
// the user metadata of the namespace marker entry (§6.1).
type getNamespaceResponse struct {
	Namespace  Namespace         `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

// createNamespaceRequest is CreateNamespaceRequest (spec: components.schemas.CreateNamespaceRequest).
// Namespace is the address to create, in the client's own terms: the full [repo, ref, ns…] path on
// the /iceberg/api form, and the levels below the warehouse on a relative_to form, where
// handleCreateNamespace prepends what the URL fixed; `namespace` is the only required field of the
// model.
type createNamespaceRequest struct {
	Namespace  []string          `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

// createNamespaceResponse is CreateNamespaceResponse (spec: components.schemas.CreateNamespaceResponse):
// the namespace that was created and the properties the server stored for it, which is exactly what
// the request asked for because §6.1 stores the properties verbatim as the marker's user metadata.
type createNamespaceResponse struct {
	Namespace  Namespace         `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

// updateNamespacePropertiesRequest is UpdateNamespacePropertiesRequest
// (spec: components.schemas.UpdateNamespacePropertiesRequest): the keys to remove and the keys to
// set. Properties that are in neither list are left untouched.
type updateNamespacePropertiesRequest struct {
	Removals []string          `json:"removals"`
	Updates  map[string]string `json:"updates"`
}

// updateNamespacePropertiesResponse is UpdateNamespacePropertiesResponse
// (spec: components.schemas.UpdateNamespacePropertiesResponse). Updated and Removed are required by
// the model and are therefore always arrays; Missing lists the removals that named a property the
// namespace did not have, which the spec describes as a partial success and not an error.
type updateNamespacePropertiesResponse struct {
	Updated []string `json:"updated"`
	Removed []string `json:"removed"`
	Missing []string `json:"missing,omitempty"`
}

// handleListNamespaces is GET /v1/{prefix}/namespaces: the three levels of the namespace tree of
// §5, selected by the optional `parent` query parameter.
//
// The order is authorization, then lookup, then pagination: a caller that may not list a parent
// learns nothing about what is below it, and the answer is the same 403 whether the parent exists
// or not.
func (n *namespaceService) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	if !n.checkPrefix(w, r) {
		return
	}
	caller, ok := n.caller(w, r)
	if !ok {
		return
	}
	ctx := r.Context()

	var parent Address
	if raw := r.URL.Query().Get("parent"); raw != "" {
		addr, err := ParseNamespace(raw)
		if err != nil {
			n.writeError(w, err)
			return
		}
		parent = addr
	}

	pageSize, err := parsePageSize(r)
	if err != nil {
		n.writeError(w, err)
		return
	}

	var (
		namespaces []Namespace
		listErr    error
	)
	switch {
	case parent.Repo == "":
		namespaces, listErr = n.listRepositories(ctx, caller)
	case parent.Ref == "":
		namespaces, listErr = n.listBranches(ctx, caller, parent.Repo)
	default:
		namespaces, listErr = n.listChildNamespaces(ctx, caller, parent)
	}
	if listErr != nil {
		n.writeError(w, listErr)
		return
	}

	page, next, err := paginate(namespaces, r.URL.Query().Get("pageToken"), pageSize)
	if err != nil {
		n.writeError(w, err)
		return
	}
	if page == nil {
		page = []Namespace{}
	}
	// The entries above are the catalog's own addresses, which carry the repository and the ref
	// (fullNamespace). A relative_to client named neither of them - its URL did - so the fixed
	// levels are dropped again before the answer goes out, which is what makes the listing
	// identity-preserving for that client (relative.go clientNamespace, D-4).
	for i, namespace := range page {
		page[i] = clientNamespace(r, namespace)
	}
	writeJSON(w, http.StatusOK, listNamespacesResponse{Namespaces: page, NextPageToken: next})
}

// handleGetNamespace is GET /v1/{prefix}/namespaces/{namespace}: the namespace object with the
// properties of its marker entry (§6.1). The marker is read on Address.Ref, so the same request
// answers on a branch, a tag and a commit.
func (n *namespaceService) handleGetNamespace(w http.ResponseWriter, r *http.Request) {
	addr, ok := n.authorizeGetNamespace(w, r)
	if !ok {
		return
	}
	marker, err := n.store.GetNamespace(r.Context(), addr)
	if err != nil {
		n.writeError(w, err)
		return
	}
	properties := marker.Properties
	if properties == nil {
		properties = map[string]string{}
	}
	writeJSON(w, http.StatusOK, getNamespaceResponse{
		Namespace:  clientNamespace(r, fullNamespace(addr, "")),
		Properties: properties,
	})
}

// handleHeadNamespace is HEAD /v1/{prefix}/namespaces/{namespace} (spec: NamespaceExists). The
// spec answers 204 with no body on success and 404 when the namespace is absent; the existence
// check is the same authorized marker read as GET, so HEAD cannot reveal a namespace GET hides.
func (n *namespaceService) handleHeadNamespace(w http.ResponseWriter, r *http.Request) {
	addr, ok := n.authorizeGetNamespace(w, r)
	if !ok {
		return
	}
	if _, err := n.store.GetNamespace(r.Context(), addr); err != nil {
		n.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// authorizeGetNamespace parses and authorizes the namespace path parameter for GET and HEAD,
// writing the error itself when either step fails. The two verbs share it so they cannot diverge
// on what they accept or who may ask.
func (n *namespaceService) authorizeGetNamespace(w http.ResponseWriter, r *http.Request) (Address, bool) {
	if !n.checkPrefix(w, r) {
		return Address{}, false
	}
	addr, ok := n.namespaceTarget(w, r)
	if !ok {
		return Address{}, false
	}
	caller, ok := n.caller(w, r)
	if !ok {
		return Address{}, false
	}
	if err := n.authz.Authorize(r.Context(), caller, Request{Op: OpGetNamespace, Address: addr}); err != nil {
		n.writeError(w, err)
		return Address{}, false
	}
	return addr, true
}

// namespaceTarget parses the {namespace} path parameter of a get, head, properties or delete
// request and requires it to name a namespace: a repository, a ref and at least one namespace
// level. The repository and the ref macro levels are never writable or readable as namespaces
// themselves, because no marker entry can exist for them (§6.1).
func (n *namespaceService) namespaceTarget(w http.ResponseWriter, r *http.Request) (Address, bool) {
	addr, err := ParseNamespace(chi.URLParam(r, "namespace"))
	if err != nil {
		n.writeError(w, err)
		return Address{}, false
	}
	if addr.Ref == "" || len(addr.Namespace) == 0 {
		// GET/HEAD/DELETE/properties address a namespace, not the repository or the branch
		// level: a namespace object only exists where a marker entry can exist (§6.1).
		n.writeError(w, fmt.Errorf("%w: a namespace address needs a repository, a ref and at least one namespace level",
			ErrInvalidIdentifier))
		return Address{}, false
	}
	return addr, true
}

// handleCreateNamespace is POST /v1/{prefix}/namespaces: it stages a marker entry for a new
// namespace, carrying the request's properties as the marker's user metadata (§6.1), and answers
// 200 with the namespace it created (spec: CreateNamespaceResponse).
//
// The create is a create on an absent value in the store (§6.2, I2-1), so a name that is already
// taken - in the staging area or in the committed data - is a conflict and not an overwrite: it is
// answered 409 NamespaceAlreadyExistsException, and the existing marker keeps its properties.
//
// The address of a create comes from the request body, so the body is decoded before the
// authorization decision. That is the one place where the catalog cannot authorize first: the
// repository and the ref the caller must be allowed to write only exist in the body. The
// middleware still rejected an unauthenticated caller long before this handler ran (§7.1), and a
// caller without a grant learns nothing but the shape of its own request.
func (n *namespaceService) handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
	if !n.checkPrefix(w, r) {
		return
	}
	var body createNamespaceRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}
	// A relative_to client carries only its own levels in the body: the repository and the ref are
	// fixed by its URL, and the create would otherwise read its first two levels as them and fail
	// (relative.go absoluteNamespaceLevels, D-2). On the /iceberg/api form this is the identity.
	body.Namespace = absoluteNamespaceLevels(r, body.Namespace)
	addr, err := namespaceAddressFromLevels(body.Namespace)
	if err != nil {
		n.writeError(w, err)
		return
	}
	if err := validateProperties(body.Properties); err != nil {
		n.writeError(w, err)
		return
	}
	if !n.authorizeWrite(w, r, OpCreateNamespace, addr) {
		return
	}
	if !n.requireWritableNamespace(w, addr) {
		return
	}
	if err := n.store.CreateNamespace(r.Context(), addr, body.Properties); err != nil {
		n.writeError(w, err)
		return
	}
	properties := body.Properties
	if properties == nil {
		// An omitted properties object is a namespace without properties, which is still a
		// namespace: the response model defaults it to {}, so the client gets an object and not
		// a null it would have to check.
		properties = map[string]string{}
	}
	writeJSON(w, http.StatusOK, createNamespaceResponse{
		Namespace:  clientNamespace(r, fullNamespace(addr, "")),
		Properties: properties,
	})
}

// handleUpdateNamespaceProperties is POST /v1/{prefix}/namespaces/{namespace}/properties: it sets
// and removes properties of an existing namespace, leaving every property it was not asked about
// alone (spec: UpdateProperties).
//
// The merge is a read-modify-write over the marker's user metadata, which is the whole state of a
// namespace (§6.1). It is done here rather than in the store because only the handler knows what to
// report back: the updated keys, the removed keys and the removals that named a property the
// namespace did not have (the spec's "missing", a partial success). A namespace that does not exist
// is 404 NoSuchNamespaceException, and the store's update requires the marker to exist so a
// properties update cannot resurrect a dropped namespace.
func (n *namespaceService) handleUpdateNamespaceProperties(w http.ResponseWriter, r *http.Request) {
	if !n.checkPrefix(w, r) {
		return
	}
	addr, ok := n.namespaceTarget(w, r)
	if !ok {
		return
	}
	// Authorize before the body is decoded: the address is entirely in the path, so the caller's
	// grant (and the Q-I4 branch rule) can be decided without touching the request body.
	if !n.authorizeWrite(w, r, OpUpdateNamespace, addr) {
		return
	}
	if !n.requireWritableNamespace(w, addr) {
		return
	}
	var body updateNamespacePropertiesRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}
	if err := validateProperties(body.Updates); err != nil {
		n.writeError(w, err)
		return
	}
	if err := validatePropertyRemovals(body.Removals); err != nil {
		n.writeError(w, err)
		return
	}
	for _, key := range body.Removals {
		if _, both := body.Updates[key]; both {
			// The spec answers 422 for a key that appears in two fields that must be disjoint
			// (components.examples.UnprocessableEntityDuplicateKey).
			writeIcebergError(w, http.StatusUnprocessableEntity, ErrTypeUnprocessableEntity,
				fmt.Sprintf("property %q appears in both removals and updates", key))
			return
		}
	}

	marker, err := n.store.GetNamespace(r.Context(), addr)
	if err != nil {
		n.writeError(w, err)
		return
	}
	merged := marker.Properties
	if merged == nil {
		merged = make(map[string]string, len(body.Updates))
	}
	updated := make([]string, 0, len(body.Updates))
	for key, value := range body.Updates {
		merged[key] = value
		updated = append(updated, key)
	}
	removed := make([]string, 0, len(body.Removals))
	missing := make([]string, 0)
	for _, key := range body.Removals {
		if _, exists := merged[key]; !exists {
			missing = append(missing, key)
			continue
		}
		delete(merged, key)
		removed = append(removed, key)
	}
	// The lists are reported in a stable order, so a client (and a test) can compare them.
	sort.Strings(updated)
	sort.Strings(removed)
	sort.Strings(missing)

	if err := n.store.UpdateNamespace(r.Context(), addr, merged); err != nil {
		n.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updateNamespacePropertiesResponse{
		Updated: updated,
		Removed: removed,
		Missing: missing,
	})
}

// handleDeleteNamespace is DELETE /v1/{prefix}/namespaces/{namespace}: it removes the marker of an
// empty namespace and answers 204 with no body (spec: DropNamespace).
//
// The client's "must be empty" precondition is not taken on trust: the catalog lists what is below
// the marker itself (§11 I-2). A namespace is not empty when a table pointer exists anywhere under
// "tables/<ns>/" - including the tables of a descendant namespace, because that key prefix is a
// prefix of theirs - or when a child namespace marker exists below "ns/<ns>/". Either case is 409
// NamespaceNotEmptyException. A namespace that does not exist at all is 404
// NoSuchNamespaceException.
func (n *namespaceService) handleDeleteNamespace(w http.ResponseWriter, r *http.Request) {
	if !n.checkPrefix(w, r) {
		return
	}
	addr, ok := n.namespaceTarget(w, r)
	if !ok {
		return
	}
	if !n.authorizeWrite(w, r, OpDeleteNamespace, addr) {
		return
	}
	if !n.requireWritableNamespace(w, addr) {
		return
	}
	ctx := r.Context()
	if _, err := n.store.GetNamespace(ctx, addr); err != nil {
		n.writeError(w, err)
		return
	}
	empty, err := n.namespaceIsEmpty(ctx, addr)
	if err != nil {
		n.writeError(w, err)
		return
	}
	if !empty {
		n.writeError(w, &NamespaceNotEmptyException{Repo: addr.Repo, Ref: addr.Ref, Namespace: addr.Namespace})
		return
	}
	markerPath, err := namespaceMarkerPath(addr.Namespace)
	if err != nil {
		n.writeError(w, err)
		return
	}
	if err := n.lister.DeleteEntry(ctx, addr.Repo, addr.Ref, markerPath); err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			// The marker was removed by another writer between the read and the delete: the
			// answer is the 404 a client that asks twice should see.
			err = &NoSuchNamespaceException{Repo: addr.Repo, Ref: addr.Ref, Namespace: addr.Namespace}
		}
		n.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// refKind is what a write resolved its ref to. A write needs a branch (Q-I4): a tag or a commit
// is readable but not writable, and a ref that resolves to nothing means the warehouse the client
// named does not exist.
type refKind int

const (
	// refMissing is a ref that resolves to no branch, tag or commit. It is a missing warehouse
	// (404), never an empty listing and never a read-only ref.
	refMissing refKind = iota
	// refBranch is a branch: the only ref a write may address.
	refBranch
	// refReadOnly is a tag or a commit: readable on any ref, never writable (Q-I4).
	refReadOnly
)

// refKind resolves ref of repo to a branch, a read-only ref (tag or commit) or nothing.
//
// BranchExists cannot answer for a ref that is not a valid branch name - a tag may carry a dot or
// a character a branch name forbids - so that answer is treated as "not a branch" and GetCommit,
// which dereferences any ref, decides whether the ref exists at all. A repository that does not
// exist is a missing ref: the write answers NoSuchWarehouseException, like a read of the same
// address.
func (n *namespaceService) refKind(ctx context.Context, repo, ref string) (refKind, error) {
	isBranch, err := n.lister.BranchExists(ctx, repo, ref)
	switch {
	case err == nil && isBranch:
		return refBranch, nil
	case err != nil && !errors.Is(err, graveler.ErrInvalidBranchID) && !isMissingWarehouse(err):
		return refMissing, err
	}
	if _, err := n.lister.GetCommit(ctx, repo, ref); err != nil {
		if isMissingWarehouse(err) {
			return refMissing, nil
		}
		return refMissing, err
	}
	return refReadOnly, nil
}

// authorizeWrite authorizes one namespace write on addr, including the Q-I4 rule that the ref must
// be a branch.
//
// The ref is resolved first only so the decision can be made with the right RefIsBranch value; the
// permission check inside Authorize still runs first, so a caller without a grant learns nothing
// about the ref it named - not whether it exists, not whether it is a branch. A ref that resolves
// to nothing is turned into NoSuchWarehouseException after the permission check has passed: a
// missing ref is not a read-only ref, and a client that named a repository or a branch that does
// not exist should get the same 404 a read would.
func (n *namespaceService) authorizeWrite(w http.ResponseWriter, r *http.Request, op Operation, addr Address) bool {
	caller, ok := n.caller(w, r)
	if !ok {
		return false
	}
	kind, err := n.refKind(r.Context(), addr.Repo, addr.Ref)
	if err != nil {
		n.writeError(w, err)
		return false
	}
	err = n.authz.Authorize(r.Context(), caller, Request{Op: op, Address: addr, RefIsBranch: kind == refBranch})
	if err != nil {
		var readOnly *ReadOnlyRefError
		if kind == refMissing && errors.As(err, &readOnly) {
			// Only the ref rule objected: the caller may write, but the ref does not exist.
			n.writeError(w, &NoSuchWarehouseException{Repo: addr.Repo, Ref: addr.Ref})
			return false
		}
		n.writeError(w, err)
		return false
	}
	return true
}

// requireWritableNamespace refuses a namespace write into a namespace a table resolver reserves on
// a data repository (resolver.go, METADATA_SEARCH_DESIGN.md §5.4). It is the namespace endpoint's
// use of the same rule the table and view writes apply: create, update-properties and drop under a
// reserved namespace are 403 ForbiddenException, and in <repo>-metadata the namespace is ordinary.
// Nothing is refused while no resolver is registered.
//
// It runs after the permission decision, so an unauthorized caller gets its permission refusal
// rather than a description of a namespace it could not reach anyway.
func (n *namespaceService) requireWritableNamespace(w http.ResponseWriter, addr Address) bool {
	if err := n.store.RequireWritableNamespace(addr); err != nil {
		n.writeError(w, err)
		return false
	}
	return true
}

// namespaceAddressFromLevels turns the `namespace` array of a create request into a catalog
// Address: the first two levels are the repository and the ref (§5, "Levels 1 and 2 are always
// repo and ref") and every level after them is a real namespace level.
//
// The levels are validated with the same rules as a path parameter (ident.go), so a body cannot
// name a repository, a ref or a namespace level that the /v1/{prefix}/namespaces/{namespace} form
// could not, and it cannot address the repository or repository.ref level itself: a create needs at
// least one namespace level, because a marker only exists for a real namespace (§6.1).
func namespaceAddressFromLevels(levels []string) (Address, error) {
	if err := validateLevelShape(levels); err != nil {
		return Address{}, err
	}
	if len(levels) < macroLevels+1 {
		return Address{}, fmt.Errorf("%w: namespace %q must name a repository, a ref and at least one namespace level",
			ErrInvalidIdentifier, Namespace(levels).String())
	}
	addr := Address{Repo: levels[0], Ref: levels[1], Namespace: Namespace(levels[macroLevels:])}
	if err := validateRepo(addr.Repo); err != nil {
		return Address{}, err
	}
	if err := validateRef(addr.Ref); err != nil {
		return Address{}, err
	}
	if err := validateNamespace(addr.Namespace); err != nil {
		return Address{}, err
	}
	return addr, nil
}

// validateProperties rejects a property set the marker cannot carry: §6.1 stores the properties as
// the entry user metadata, and a user-metadata key must not be empty. It is a 400, not a 500: the
// request is malformed, the server is fine.
func validateProperties(properties map[string]string) error {
	for key := range properties {
		if key == "" {
			return fmt.Errorf("%w: a namespace property has an empty name", ErrInvalidIdentifier)
		}
	}
	return nil
}

// validatePropertyRemovals applies the same rule to the removals list of a properties update.
func validatePropertyRemovals(removals []string) error {
	for _, key := range removals {
		if key == "" {
			return fmt.Errorf("%w: a namespace property has an empty name", ErrInvalidIdentifier)
		}
	}
	return nil
}

// namespaceIsEmpty reports whether the namespace holds no tables, no views and no child
// namespace, i.e. whether dropping it loses nothing. The checks look under the marker in the layout
// of §6.1:
//
//   - any entry under `_lakefs_iceberg/tables/<ns>/` is a table of this namespace or of a
//     descendant namespace (a table pointer of a child namespace is below the same prefix), so
//     the namespace is not empty;
//   - any entry under `_lakefs_iceberg/views/<ns>/` is a view of this namespace or of a descendant
//     namespace, for exactly the same reason: a view is named state that a drop namespace would
//     otherwise orphan, and the spec's DropNamespace answers 409 NamespaceNotEmptyException when
//     anything is still in the namespace;
//   - any common-prefix entry under `_lakefs_iceberg/ns/<ns>/` is a child namespace marker.
//     The namespace's own `_lakefs_iceberg/ns/<ns>/.namespace` marker is not a common prefix and
//     is therefore not mistaken for a child.
func (n *namespaceService) namespaceIsEmpty(ctx context.Context, addr Address) (bool, error) {
	for _, prefix := range []string{tableEntriesPrefix(addr.Namespace), viewEntriesPrefix(addr.Namespace)} {
		hasNamed, err := n.anyEntry(ctx, addr, prefix)
		if err != nil {
			return false, err
		}
		if hasNamed {
			return false, nil
		}
	}
	hasChild, err := n.hasChildNamespace(ctx, addr)
	if err != nil {
		return false, err
	}
	return !hasChild, nil
}

// viewEntriesPrefix is viewEntryPrefix for the namespace-emptiness check: the reserved key prefix
// under which a view of this namespace or of a descendant namespace appears. It is declared here
// next to tableEntriesPrefix so the two checks read as one rule.
func viewEntriesPrefix(namespace Namespace) string {
	return viewEntryPrefix(namespace)
}

// anyEntry reports whether any entry at all exists below prefix on addr.Ref.
func (n *namespaceService) anyEntry(ctx context.Context, addr Address, prefix string) (bool, error) {
	entries, hasMore, err := n.lister.ListEntries(ctx, addr.Repo, addr.Ref, prefix, "",
		namespaceListDelimiter, 1)
	if err != nil {
		return false, err
	}
	return len(entries) > 0 || hasMore, nil
}

// hasChildNamespace reports whether a child namespace marker exists below the namespace. It walks
// the delimiter listing in pages, because the namespace's own marker is an ordinary entry of that
// listing and may share a page with a child prefix.
func (n *namespaceService) hasChildNamespace(ctx context.Context, addr Address) (bool, error) {
	prefix := namespaceEntryPrefix(addr.Namespace)
	after := ""
	for {
		entries, hasMore, err := n.lister.ListEntries(ctx, addr.Repo, addr.Ref, prefix, after,
			namespaceListDelimiter, namespaceChildPageSize)
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if entry.CommonLevel {
				return true, nil
			}
		}
		if !hasMore || len(entries) == 0 {
			return false, nil
		}
		after = entries[len(entries)-1].Path
	}
}

// tableEntriesPrefix is the reserved key prefix of the tables of a namespace and of its descendants
// (§6.1): `_lakefs_iceberg/tables/<ns levels…>/`.
func tableEntriesPrefix(namespace Namespace) string {
	return ReservedPrefix + "/" + TablesDirectory + "/" + strings.Join(namespace, "/") + "/"
}

// NoSuchWarehouseException is the 404 of a write that names a repository or a ref that does not
// exist. Reads answer the underlying lakeFS error (isMissingWarehouse); a write has to resolve the
// ref before it can decide whether it is a branch, so it rebuilds the same answer from a kind it
// resolved to nothing.
type NoSuchWarehouseException struct {
	Repo string
	Ref  string
}

// Error implements error. It names the repository and the ref, which the caller named itself.
func (e *NoSuchWarehouseException) Error() string {
	return fmt.Sprintf("no such repository %q or ref %q", e.Repo, e.Ref)
}

// NamespaceNotEmptyException is the 409 of a drop of a namespace that still holds tables or child
// namespaces (spec: components.examples.NamespaceNotEmptyError).
type NamespaceNotEmptyException struct {
	Repo      string
	Ref       string
	Namespace Namespace
}

// Error implements error.
func (e *NamespaceNotEmptyException) Error() string {
	return fmt.Sprintf("namespace %q in %s@%s is not empty: it still holds tables or child namespaces",
		e.Namespace.Dotted(), e.Repo, e.Ref)
}

// listRepositories is the root level of §5: "GET namespaces with no parent lists repositories the
// caller can read". The gate is the repository-list permission RequiredNode assigns to the
// address-less listing; each repository is then filtered through the same Authorizer, so a caller
// with a grant on one repository does not learn the names of the others.
func (n *namespaceService) listRepositories(ctx context.Context, caller Caller) ([]Namespace, error) {
	if err := n.authz.Authorize(ctx, caller, Request{Op: OpListNamespaces}); err != nil {
		return nil, err
	}
	repositories, err := n.allRepositories(ctx)
	if err != nil {
		return nil, err
	}
	namespaces := make([]Namespace, 0, len(repositories))
	for _, repo := range repositories {
		req := Request{Op: OpListNamespaces, Address: Address{Repo: repo.Name}}
		err := n.authz.Authorize(ctx, caller, req)
		switch {
		case err == nil:
			namespaces = append(namespaces, Namespace{repo.Name})
		case IsDenial(err):
			// The caller may list repositories but not this one: hide it, exactly like the core
			// ListRepositories RBAC filter.
			continue
		default:
			return nil, err
		}
	}
	return namespaces, nil
}

// listBranches is the repository level of §5: with parent <repo>, the namespace tree continues
// with the repository's branches. The repository-level listing is authorized once, and every
// branch is filtered with the Authorizer rather than returned unconditionally, so a branch the
// caller cannot read is not disclosed by its name.
func (n *namespaceService) listBranches(ctx context.Context, caller Caller, repo string) ([]Namespace, error) {
	gate := Request{Op: OpListNamespaces, Address: Address{Repo: repo}}
	if err := n.authz.Authorize(ctx, caller, gate); err != nil {
		return nil, err
	}
	branches, err := n.allBranches(ctx, repo)
	if err != nil {
		return nil, err
	}
	namespaces := make([]Namespace, 0, len(branches))
	for _, branch := range branches {
		req := Request{Op: OpListNamespaces, Address: Address{Repo: repo, Ref: branch.Name}}
		err := n.authz.Authorize(ctx, caller, req)
		switch {
		case err == nil:
			namespaces = append(namespaces, Namespace{repo, branch.Name})
		case IsDenial(err):
			continue
		default:
			return nil, err
		}
	}
	return namespaces, nil
}

// listChildNamespaces is the namespace level of §5: with parent <repo><sep><ref>, the entries are
// the real namespaces of that ref, one level below the parent. §7.2 authorizes the listing on the
// parent namespace ARN (catalog:ListNamespaces on CatalogNamespaceArn(repo, parent)), which is the
// same permission that grants the repository and branch levels above.
//
// A parent namespace does not have to carry its own marker: the listing is a prefix walk over the
// marker tree, so a namespace whose only markers are its children is still listable. GET on that
// parent is the request that returns 404, because there is no marker to read.
func (n *namespaceService) listChildNamespaces(ctx context.Context, caller Caller, parent Address) ([]Namespace, error) {
	req := Request{Op: OpListNamespaces, Address: parent}
	if err := n.authz.Authorize(ctx, caller, req); err != nil {
		return nil, err
	}
	entries, err := n.allEntries(ctx, parent.Repo, parent.Ref, namespaceEntryPrefix(parent.Namespace))
	if err != nil {
		return nil, err
	}
	namespaces := make([]Namespace, 0, len(entries))
	for _, entry := range entries {
		if !entry.CommonLevel {
			// Only common prefixes are immediate child namespaces. A file entry below the prefix
			// is the parent's own marker (`ns/<parent>/.namespace`), which names the parent and
			// not a child.
			continue
		}
		child, err := childNamespace(parent.Namespace, entry.Path)
		if err != nil {
			return nil, err
		}
		namespaces = append(namespaces, fullNamespace(parent, child))
	}
	// A resolver may own child namespaces that have no marker (resolver.go, §5.4): the system
	// namespace of a Metadata Search data repository is one, and it is listed only while the
	// resolver that declares it is registered and its table resolves. The order of the listing is by
	// name, so the added levels are merged into it rather than appended after it.
	virtual, err := n.store.VirtualNamespaces(ctx, parent)
	if err != nil {
		return nil, err
	}
	return withVirtualNamespaces(parent, namespaces, virtual), nil
}

// allRepositories walks the repository listing to the end. The catalog lists repositories in
// pages; the namespace tree needs the whole filtered set before it can paginate by namespace, so
// the pages are concatenated here.
func (n *namespaceService) allRepositories(ctx context.Context) ([]*catalog.Repository, error) {
	var (
		repositories []*catalog.Repository
		after        string
	)
	for {
		page, hasMore, err := n.lister.ListRepositories(ctx, catalog.ListRepositoriesLimitMax, "", "", after)
		if err != nil {
			return nil, err
		}
		repositories = append(repositories, page...)
		if !hasMore || len(page) == 0 {
			return repositories, nil
		}
		after = page[len(page)-1].Name
	}
}

// allBranches walks the branch listing of repo to the end.
func (n *namespaceService) allBranches(ctx context.Context, repo string) ([]*catalog.Branch, error) {
	var (
		branches []*catalog.Branch
		after    string
	)
	for {
		page, hasMore, err := n.lister.ListBranches(ctx, repo, "", catalog.ListBranchesLimitMax, after)
		if err != nil {
			return nil, err
		}
		branches = append(branches, page...)
		if !hasMore || len(page) == 0 {
			return branches, nil
		}
		after = page[len(page)-1].Name
	}
}

// allEntries walks the entry listing below prefix on ref to the end.
func (n *namespaceService) allEntries(ctx context.Context, repo, ref, prefix string) ([]*catalog.DBEntry, error) {
	var (
		entries []*catalog.DBEntry
		after   string
	)
	for {
		page, hasMore, err := n.lister.ListEntries(ctx, repo, ref, prefix, after,
			namespaceListDelimiter, catalog.ListEntriesLimitMax)
		if err != nil {
			return nil, err
		}
		entries = append(entries, page...)
		if !hasMore || len(page) == 0 {
			return entries, nil
		}
		after = page[len(page)-1].Path
	}
}

// namespaceEntryPrefix is the reserved key prefix of the markers below parent (§6.1):
// `_lakefs_iceberg/ns/<parent levels…>/` or `_lakefs_iceberg/ns/` for the top of the namespace
// tree. The trailing slash is what makes the walk descend exactly one level with
// namespaceListDelimiter.
func namespaceEntryPrefix(parent Namespace) string {
	prefix := ReservedPrefix + "/" + NamespaceDirectory + "/"
	if len(parent) == 0 {
		return prefix
	}
	return prefix + strings.Join(parent, "/") + "/"
}

// childNamespace extracts the immediate child namespace name from one common-prefix entry.
func childNamespace(parent Namespace, entryPath string) (string, error) {
	prefix := namespaceEntryPrefix(parent)
	if !strings.HasPrefix(entryPath, prefix) {
		return "", fmt.Errorf("%w: namespace listing entry %q is not below %q",
			ErrInvalidPointer, entryPath, prefix)
	}
	child := strings.TrimSuffix(strings.TrimPrefix(entryPath, prefix), namespaceListDelimiter)
	if child == "" || strings.Contains(child, namespaceListDelimiter) {
		return "", fmt.Errorf("%w: namespace listing entry %q is not one level below the parent",
			ErrInvalidPointer, entryPath)
	}
	if err := validateLayoutSegment(child); err != nil {
		return "", fmt.Errorf("namespace listing entry %q: %w", entryPath, err)
	}
	return child, nil
}

// withVirtualNamespaces merges the child namespaces a resolver adds into the child namespaces a
// listing discovered, keeping the listing in name order (resolver.go, §5.4). With nothing to add
// the discovered namespaces are returned as they are, so a listing of a catalog without a resolver
// is unchanged.
func withVirtualNamespaces(parent Address, discovered []Namespace, virtual []string) []Namespace {
	if len(virtual) == 0 {
		return discovered
	}
	merged := make([]Namespace, 0, len(discovered)+len(virtual))
	merged = append(merged, discovered...)
	for _, name := range virtual {
		merged = append(merged, fullNamespace(parent, name))
	}
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].Dotted() < merged[j].Dotted() })
	return merged
}

// fullNamespace returns the full namespace path of addr plus one optional child level, in the
// shape of the spec's Namespace type: the repository and ref macro levels first, then the real
// namespace levels.
func fullNamespace(addr Address, child string) Namespace {
	levels := make(Namespace, 0, macroLevels+len(addr.Namespace)+1)
	levels = append(levels, addr.Repo, addr.Ref)
	levels = append(levels, addr.Namespace...)
	if child != "" {
		levels = append(levels, child)
	}
	return levels
}

// checkPrefix validates the {prefix} path segment against the catalog's configured prefix. The
// prefix is a constant of GET /v1/config (§14 Q-I3): a request that sends a different one names a
// warehouse that does not exist, not a namespace of this catalog.
func (n *namespaceService) checkPrefix(w http.ResponseWriter, r *http.Request) bool {
	prefix := chi.URLParam(r, "prefix")
	if prefix == n.prefix {
		return true
	}
	writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
		fmt.Sprintf("prefix %q is not the catalog prefix %q", prefix, n.prefix))
	return false
}

// caller builds the authorization caller of a request: the authenticated principal (requireUser
// in http.go already answered a request without one) and the client IP the auth service evaluates
// SourceIp conditions with, extracted the same way pkg/datasets does.
func (n *namespaceService) caller(w http.ResponseWriter, r *http.Request) (Caller, bool) {
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
// §2.1. A refusal is 403 ForbiddenException; a missing repository or ref on a listing is 404
// NoSuchWarehouseException; a missing marker is 404 NoSuchNamespaceException; a create on a taken
// name and a drop of a non-empty namespace are 409; a malformed identifier or page token is 400;
// anything undecided or unexpected is 500 with no detail, never a permission (§12).
func (n *namespaceService) writeError(w http.ResponseWriter, err error) {
	var (
		denied       *PermissionDeniedError
		readOnly     *ReadOnlyRefError
		reserved     *ReservedNamespaceError
		authzFailure *AuthorizationError
		noNamespace  *NoSuchNamespaceException
		noWarehouse  *NoSuchWarehouseException
		notEmpty     *NamespaceNotEmptyException
	)
	switch {
	case errors.Is(err, ErrInvalidIdentifier):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
	case errors.As(err, &readOnly):
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, readOnly.Message())
	case errors.As(err, &reserved):
		// A write into a namespace a table resolver owns (resolver.go, §5.4).
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, reserved.Error())
	case errors.As(err, &denied):
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, denied.Error())
	case errors.As(err, &authzFailure):
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the authorization decision failed")
	case errors.As(err, &noNamespace):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchNamespace, noNamespace.Error())
	case errors.As(err, &noWarehouse):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse, noWarehouse.Error())
	case errors.As(err, &notEmpty):
		writeIcebergError(w, http.StatusConflict, ErrTypeNamespaceNotEmpty, notEmpty.Error())
	case errors.Is(err, ErrAlreadyExists):
		// The store's create found an existing marker: the name is taken (I2-1).
		writeIcebergError(w, http.StatusConflict, ErrTypeNamespaceAlreadyExists, err.Error())
	case isMissingWarehouse(err):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
			"the repository or ref named by the request does not exist")
	default:
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the catalog could not serve the request")
	}
}

// isMissingWarehouse reports whether err is one of the lakeFS "this repository or ref does not
// exist" answers a listing can produce. The namespace tree is addressed below a repository and a
// ref, so no such repository/ref means the warehouse the client named does not exist, which the
// spec's NoSuchWarehouseException describes; it is not an empty listing and must not be reported
// as one.
func isMissingWarehouse(err error) bool {
	return errors.Is(err, graveler.ErrRepositoryNotFound) ||
		errors.Is(err, graveler.ErrBranchNotFound) ||
		errors.Is(err, graveler.ErrTagNotFound) ||
		errors.Is(err, graveler.ErrCommitNotFound) ||
		errors.Is(err, graveler.ErrNotFound)
}

// parsePageSize reads the optional pageSize query parameter. An absent parameter means "one
// page": pageSize is an upper bound the client imposes, and a client that does not ask for pages
// gets the whole listing (§2.1). A negative or non-integer value is a malformed request.
func parsePageSize(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("pageSize")
	if raw == "" {
		return 0, nil
	}
	size, err := strconv.Atoi(raw)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("%w: pageSize %q is not a non-negative integer", ErrInvalidIdentifier, raw)
	}
	return size, nil
}

// paginate applies the client's page token and page size to one filtered listing: it returns the
// page and the token of the next one (empty on the last page). The token is opaque to the client;
// this catalog encodes an offset, which is stable because the listing itself is ordered by the
// lakeFS iterators (repositories and branches by name, namespaces by marker path).
func paginate[T any](items []T, token string, pageSize int) ([]T, string, error) {
	start, err := decodePageToken(token)
	if err != nil {
		return nil, "", err
	}
	if start > len(items) {
		start = len(items)
	}
	if pageSize <= 0 || start+pageSize >= len(items) {
		return items[start:], "", nil
	}
	end := start + pageSize
	return items[start:end], encodePageToken(end), nil
}

// encodePageToken renders an offset as an opaque page token.
func encodePageToken(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodePageToken parses a page token produced by encodePageToken. An empty token is the first
// page; anything else that is not a non-negative offset is a malformed request.
func decodePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("%w: pageToken is not a valid page token", ErrInvalidIdentifier)
	}
	offset, err := strconv.Atoi(string(raw))
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: pageToken is not a valid page token", ErrInvalidIdentifier)
	}
	return offset, nil
}
