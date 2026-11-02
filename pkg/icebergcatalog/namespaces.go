package icebergcatalog

// Namespace reads for the Iceberg REST catalog (ICEBERG_CATALOG_PLAN.md §5 "Addressing rules",
// §7.2 "Authorization" and §11 I-1 "namespaces list/get/head (repo/ref levels)").
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
// ListNamespaces / GetNamespace / NamespaceExists) and the public lakeFS documentation only. No
// code from lakeFS Enterprise or from upstream lakeFS >= 1.87 was used.

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
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

	// namespacePattern serves GetNamespace (GET) and NamespaceExists (HEAD) for one namespace.
	namespacePattern = "/v1/{prefix}/namespaces/{namespace}"

	// The wire forms advertised in GET /v1/config once this file's routes are mounted. They
	// come from the REST OpenAPI spec's operation paths (components.paths) with the catalog's
	// {prefix} segment; the route table builds the same string so a client can match one against
	// the other.
	listNamespacesEndpoint = "GET /v1/{prefix}/namespaces"
	getNamespaceEndpoint   = "GET /v1/{prefix}/namespaces/{namespace}"
	headNamespaceEndpoint  = "HEAD /v1/{prefix}/namespaces/{namespace}"

	// ErrTypeNoSuchNamespace is the Iceberg error type of a missing namespace, 404
	// (components.schemas.NoSuchNamespaceException).
	ErrTypeNoSuchNamespace = "NoSuchNamespaceException"

	// namespaceListDelimiter is the listing delimiter used to collapse the marker tree below a
	// parent namespace into one entry per immediate child namespace: every
	// `ns/<parent>/<child>/…` path is reported as the common prefix `ns/<parent>/<child>/`
	// (pkg/catalog/entry_listing_iterator.go), so the child name is the only path component left
	// after the parent prefix.
	namespaceListDelimiter = "/"
)

// NamespaceLister is the listing surface the namespace read endpoints need in addition to Store.
// It is a separate interface rather than more methods on Store because these three calls are
// lakeFS-level listings (repositories, branches and entries), not catalog state; *catalog.Catalog
// implements all three. Keeping it explicit means the HTTP layer cannot accidentally use a read
// path the store does not provide.
type NamespaceLister interface {
	ListRepositories(ctx context.Context, limit int, prefix, searchString, after string, opts ...catalog.ListRepositoriesOptionsFunc) ([]*catalog.Repository, bool, error)
	ListBranches(ctx context.Context, repositoryID string, prefix string, limit int, after string, opts ...graveler.ListOptionsFunc) ([]*catalog.Branch, bool, error)
	ListEntries(ctx context.Context, repositoryID string, reference string, prefix string, after string, delimiter string, limit int) ([]*catalog.DBEntry, bool, error)
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
	}
}

// listNamespacesResponse is ListNamespacesResponse (spec: components.schemas.ListNamespacesResponse).
// Namespace is the spec's Namespace type: an array of levels. The macro levels are part of every
// entry, so a branch level entry is [repo, branch] and a namespace level entry is
// [repo, ref, ns…] - the same shape a client sent as `parent`, with one more level.
type listNamespacesResponse struct {
	Namespaces    []Namespace `json:"namespaces"`
	NextPageToken string      `json:"next-page-token,omitempty"`
}

// getNamespaceResponse is GetNamespaceResponse (spec: components.schemas.GetNamespaceResponse).
// Namespace is the full path including the repository and ref levels, and Properties is the
// user metadata of the namespace marker entry (§6.1).
type getNamespaceResponse struct {
	Namespace  Namespace         `json:"namespace"`
	Properties map[string]string `json:"properties"`
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
	writeJSON(w, http.StatusOK, listNamespacesResponse{Namespaces: page, NextPageToken: next})
}

// handleGetNamespace is GET /v1/{prefix}/namespaces/{namespace}: the namespace object with the
// properties of its marker entry (§6.1). The marker is read on Address.Ref, so the same request
// answers on a branch, a tag and a commit.
func (n *namespaceService) handleGetNamespace(w http.ResponseWriter, r *http.Request) {
	addr, _, ok := n.authorizeGetNamespace(w, r)
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
		Namespace:  fullNamespace(addr, ""),
		Properties: properties,
	})
}

// handleHeadNamespace is HEAD /v1/{prefix}/namespaces/{namespace} (spec: NamespaceExists). The
// spec answers 204 with no body on success and 404 when the namespace is absent; the existence
// check is the same authorized marker read as GET, so HEAD cannot reveal a namespace GET hides.
func (n *namespaceService) handleHeadNamespace(w http.ResponseWriter, r *http.Request) {
	addr, _, ok := n.authorizeGetNamespace(w, r)
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
func (n *namespaceService) authorizeGetNamespace(w http.ResponseWriter, r *http.Request) (Address, Caller, bool) {
	if !n.checkPrefix(w, r) {
		return Address{}, Caller{}, false
	}
	addr, err := ParseNamespace(chi.URLParam(r, "namespace"))
	if err != nil {
		n.writeError(w, err)
		return Address{}, Caller{}, false
	}
	if addr.Ref == "" || len(addr.Namespace) == 0 {
		// GET/HEAD address a namespace, not the repository or the branch level: a namespace
		// object only exists where a marker entry can exist (§6.1).
		n.writeError(w, fmt.Errorf("%w: a namespace address needs a repository, a ref and at least one namespace level",
			ErrInvalidIdentifier))
		return Address{}, Caller{}, false
	}
	caller, ok := n.caller(w, r)
	if !ok {
		return Address{}, Caller{}, false
	}
	if err := n.authz.Authorize(r.Context(), caller, Request{Op: OpGetNamespace, Address: addr}); err != nil {
		n.writeError(w, err)
		return Address{}, Caller{}, false
	}
	return addr, caller, true
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
	return namespaces, nil
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
// NoSuchWarehouseException; a missing marker is 404 NoSuchNamespaceException; a malformed
// identifier or page token is 400; anything undecided or unexpected is 500 with no detail, never a
// permission (§12).
func (n *namespaceService) writeError(w http.ResponseWriter, err error) {
	var (
		denied       *PermissionDeniedError
		readOnly     *ReadOnlyRefError
		authzFailure *AuthorizationError
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
