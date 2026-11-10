package icebergcatalog

// The relative_to addressing forms (ICEBERG_CATALOG_PLAN.md §5 rows 4 and 5, §1.1 goal 2,
// §11 I-1 task I1-10).
//
// A client can name its warehouse in the identifier or in the URL. The first form carries the
// repository and the ref as the leading levels of the namespace path parameter:
//
//	/iceberg/api/v1/lakefs/namespaces/{repo<sep>ref<sep>ns…}
//
// The second form, "relative to" a warehouse, carries them in front of the catalog path and
// leaves the client's own levels relative to them:
//
//	/iceberg/relative_to/{repo}.{branch}/api/v1/…/namespaces/{ns…}       (§5 row 4)
//	/iceberg/relative_to/{repo}/api/v1/…/namespaces/{ref<sep>ns…}        (§5 row 5)
//
// The two rows above are the data-repository warehouses of §5. The current Metadata Search guide
// adds two root-tree warehouses, so one catalog also serves the metadata tables it addresses at
// `system.metadata` (METADATA_SEARCH_REALIGN.md G3):
//
//	/iceberg/relative_to/system.metadata/api/v1/…/namespaces/{repo<sep>ref}/tables/objects
//	/iceberg/relative_to/system.metadata.{data repo}/api/v1/…/namespaces/{ref}/tables/objects
//
// i.e. `<repo>.<ref>.objects` and `<ref>.objects`, which is the addressing a DuckDB ATTACH on the
// warehouse uses. The first fixes the two root levels, the second fixes the root levels and one
// data repository; the client names only what is below them, and this file prefixes those levels
// to the address exactly as it does for §5 rows 4 and 5 (the catalog then sees the ordinary
// address `system.metadata.<repo>.<ref>.objects`, resolver.go).
//
// Both are published lakeFS recipes: the relative_to form is what DuckDB and Starburst use,
// because their ATTACH / CREATE CATALOG syntax (and the `warehouse` client property) has room
// for a URL but not for a repository and a ref inside the identifier
// (https://docs.lakefs.io/iceberg/, https://docs.lakefs.io/integrations/duckdb/,
// https://docs.lakefs.io/integrations/starburst_galaxy/, and §1.1 goal 2).
//
// http.go mounts the catalog twice for exactly this reason: at APIBasePath, and at
// <RelativeToBasePath>/{warehouse}/api, where chi keeps the segment as the "warehouse" URL
// parameter (relativeToPattern). RelativeTo is the middleware that turns the second mount into
// the first:
//
//  1. it parses the warehouse segment into the levels the URL fixes (parseWarehouseLevels): {repo}
//     fixes the repository, {repo}.{branch} fixes the repository and the ref, and the two root-tree
//     forms fix `system.metadata` and `system.metadata.{data repo}` (ident.go ParseWarehouse is the
//     `{repo}[.{branch}]` parser those levels come from for the §5 rows);
//  2. it binds those levels to the request context, so the handlers of namespaces.go and tables.go
//     can tell which form they are serving (requestScope) and translate between the two
//     addresses;
//  3. it prefixes the client's own levels with the fixed ones, joined with the Iceberg namespace
//     separator, so the request the handlers see is the request the same client would have sent
//     to /iceberg/api;
//  4. it delegates to the same handler chain, so both forms run the same code and authorize the
//     same way.
//
// The forms therefore answer the same *address*, and not the same bytes. That distinction is the
// whole of D-4: an address that /iceberg/api names [repo, ref, ns…] is named [ns…] by a
// relative_to client, and [ref<sep>ns…] by a client whose URL fixed only the repository, because
// the repository and the ref are fixed by the URL and were never the client's to name. A client
// that sent ["inv"] has to read ["inv"] back - not ["repo", "main", "inv"], which it has no way
// to strip - and that round trip is what catalogtest's ListNamespaces subtest asserts. The
// translation is symmetric, and this file owns both directions:
//
//   - into the catalog: bindRelativeScope rewrites the {namespace} path parameter and the
//     `parent` query parameter, and absoluteNamespaceLevels rewrites the `namespace` member of a
//     CreateNamespace request body (D-2). The create asks the handler for that rewrite rather
//     than the middleware, because the body is bounded by iceberg.max_request_body inside the
//     catalog's middleware stack, which runs behind this one;
//   - out of the catalog: clientNamespace, which every response that carries a namespace is
//     built through (namespaces.go and tables.go), drops the levels the URL fixed again.
//
// The rewrite into the catalog covers the three places an address can appear in a catalog
// request, and only those:
//
//   - the {namespace} path parameter of the namespaces and tables routes (namespaces.go
//     namespacesPattern / namespacePattern; the tables routes of I1-9 use the same parameter).
//     The parameter is part of the route path the catalog's router still has to match, so the
//     binding inserts the fixed levels into that route path rather than patching an already
//     matched parameter: chi captures the parameter when the catalog's own router matches, and
//     that happens after this middleware has run;
//   - the `parent` query parameter of ListNamespaces. It is absent for the warehouse level
//     itself, and "absent" means something different once the URL fixes part of the address: with
//     {repo}.{branch} fixed, no parent lists the namespaces of that ref (§5 row 4), and with only
//     {repo} fixed it lists the refs of that repository (§5 row 5). bindRelativeScope supplies
//     the fixed levels in that case instead of leaving the request parentless, which would
//     otherwise be answered as "list the repositories";
//   - the `namespace` member of a CreateNamespace request body, which namespaces.go rewrites with
//     absoluteNamespaceLevels once it has decoded it.
//
// Everything else in the request - the {prefix} segment, pageToken, pageSize, the table name -
// is passed through untouched, so pagination, the prefix check (Q-I3) and the response shape of
// the /iceberg/api form apply unchanged.
//
// Clean-room note (ICEBERG_CATALOG_PLAN.md §2, DATASETS_DESIGN.md §2): written from
// ICEBERG_CATALOG_PLAN.md §5, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml,
// components.parameters.namespace, components.parameters.prefix and
// ListNamespacesRequest.parent) and the public lakeFS documentation only. No lakeFS Enterprise
// code and no upstream lakeFS >= 1.87 (BSL) code was used.
//
// Security properties, which are the reason the binding is a rewrite and not a second set of
// handlers:
//
//   - The repository and the ref are the *leading* levels after the rewrite, and the client can
//     only append: a namespace level that looks like an address ("other-repo<sep>main") becomes
//     a namespace level below the fixed warehouse, never a second repository. The same holds for
//     `parent`, so neither parameter can move the request out of the scope the URL chose.
//   - The client's own parameter is carried over with its original encoding, so a level that
//     contains a percent-escaped separator is decoded once by the handler that validates it
//     (ident.go), exactly as on the /iceberg/api form. This middleware never re-encodes a level
//     it did not create.
//   - clientNamespace drops a prefix only when the levels are exactly the ones the URL fixed. An
//     address outside the fixed scope cannot be produced by these handlers - the rewrite can only
//     append to those levels - and if one ever were, returning it unchanged is the honest answer
//     rather than a value that silently loses a level.
//   - The root-tree warehouses are not a second way into the data repositories: they fix the
//     reserved `system` repository and the `metadata` level below it, so a client of
//     `system.metadata.{data repo}` still names only ref-and-below levels, and a client of
//     `system.metadata` names the data repository and its ref as ordinary namespace levels. The
//     data repository of the second form is a single level, split below in
//     systemMetadataWarehouseLevels, whatever the client sends.
//   - No I/O and no lookup happen here. A warehouse segment that does not parse names no
//     warehouse of this catalog, and is answered 404 NoSuchWarehouseException right away (§5,
//     errors.go). Whether a segment that *does* parse names a repository and a ref that exist is
//     decided by the handlers, through namespaces.go's isMissingWarehouse mapping: this
//     middleware wraps the mounted handler and therefore runs *outside* the catalog's
//     authentication and authorization stack, so it must not read the catalog and must not
//     disclose whether a repository or a ref exists to a caller that has not been authenticated
//     yet. For the listing levels that mapping already answers 404 NoSuchWarehouseException. For
//     GET and HEAD of a namespace the status is 404 as well but the type is NoSuchNamespace, a
//     classification detail of store.GetNamespace (I1-7: graveler's ErrRepositoryNotFound,
//     ErrBranchNotFound and ErrTagNotFound wrap ErrNotFound) that I1-10 inherits rather than
//     introduces; the identity tests in relative_test.go pin the status for both forms and
//     document the type.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

const (
	// warehouseParam is the chi URL parameter that relativeToPattern (http.go) captures: the
	// {repo}[.{branch}] segment of a relative_to URL. It is absent on the /iceberg/api mount,
	// which is how RelativeTo tells the two forms apart without looking at the path.
	warehouseParam = "warehouse"

	// namespacesSegment is the route path segment below which a client namespace is carried as a
	// path parameter: /v1/{prefix}/namespaces/{namespace}[/tables/{table}]. The binding inserts the
	// warehouse levels immediately after it.
	namespacesSegment = "namespaces"

	// parentParam is the ListNamespacesRequest.parent query parameter: the address whose children
	// are listed. Absent means "the top of the tree" on the /iceberg/api form (§5).
	parentParam = "parent"

	// namespacesCollectionPath is the path suffix of ListNamespaces, the one operation that takes
	// its address as a query parameter rather than a path parameter, and so the one operation
	// whose parentless request has to be given the warehouse this URL fixes.
	namespacesCollectionPath = "/" + namespacesSegment
)

// ErrInvalidWarehouse is wrapped when the warehouse segment of a relative_to URL does not name a
// repository, a repository and a ref (§5), or one of the root-tree warehouses of
// METADATA_SEARCH_REALIGN.md G3 (`system.metadata`, `system.metadata.{data repo}`). Unlike
// ErrInvalidIdentifier it is not a client syntax error about an Iceberg identifier but a warehouse
// that this catalog cannot name at all, so the answer is 404 NoSuchWarehouseException, not 400.
var ErrInvalidWarehouse = errors.New("the warehouse segment does not name a repository[.ref]")

// RelativeTo returns the middleware that binds the warehouse of a relative_to request and hands
// the rewritten request to the same route table as /iceberg/api. http.go's RoutesBuilder owns
// both mounts: /iceberg/api serves the catalog handler as it is, and the relative_to mount wraps
// that same handler with this middleware:
//
//	relativeTo.Mount(relativeToPattern, RelativeTo(handler))
//
// Wrapping is harmless on the /iceberg/api mount too (a request that carries no warehouse URL
// parameter reaches the handler unchanged), so a single wrapper around both mounts is also
// correct. A warehouse segment that does not parse is answered immediately with 404
// NoSuchWarehouseException (§5) and never reaches a handler.
func RelativeTo(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixed, relative, err := relativeScope(r)
		if err != nil {
			// The message names the shape the client has to use, because a warehouse is the one
			// part of the address the catalog does not correct for the client.
			writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse, fmt.Sprintf(
				"%s %s: %s; expected %s/{repo}[.{branch}]/api/v1/...", r.Method, r.URL.Path, err,
				RelativeToBasePath))
			return
		}
		if relative {
			bindRelativeScope(fixed, r)
			// The fixed levels also travel with the request, because two of the three rewrites - and
			// the un-prefixing of the response - happen inside the handlers (see the file comment).
			// The route context chi carries in the same context is preserved: WithContext only adds a
			// value to it.
			r = r.WithContext(withRequestScope(r.Context(), fixed))
		}
		next.ServeHTTP(w, r)
	})
}

// fixedLevels is the part of a relative_to address that the URL fixes: the levels the catalog
// names in front of the client's own, in order. It is the Scope of ident.go generalised by one
// level, because the root Metadata Search tree fixes up to three:
//
//	/iceberg/relative_to/{repo}/api                      → {repo}
//	/iceberg/relative_to/{repo}.{branch}/api             → {repo, branch}
//	/iceberg/relative_to/system.metadata/api             → {system, metadata}
//	/iceberg/relative_to/system.metadata.{data repo}/api → {system, metadata, data repo}
//
// Everything this file does - the rewrite into the catalog, the un-prefixing of responses and the
// namespace in a create body - is a function of exactly this list, so the forms need no branches
// of their own beyond parsing.
type fixedLevels []string

// requestScopeKey is the context key under which RelativeTo stores the fixedLevels it bound. It is
// a private type, so no other package can collide with it or forge one.
type requestScopeKey struct{}

// withRequestScope binds fixed to ctx: from here on the request is one of the relative_to forms.
func withRequestScope(ctx context.Context, fixed fixedLevels) context.Context {
	return context.WithValue(ctx, requestScopeKey{}, fixed)
}

// requestScope is withRequestScope's reader. The bool reports whether the request answers one of
// the relative_to forms; a request on the /iceberg/api mount carries no scope, and every helper
// below is the identity for it.
func requestScope(r *http.Request) (fixedLevels, bool) {
	fixed, ok := r.Context().Value(requestScopeKey{}).(fixedLevels)
	return fixed, ok
}

// clientNamespace returns full - a namespace as the catalog names it internally, with the
// repository and the ref in front (namespaces.go fullNamespace) - as the client of this request
// names it.
//
// On the /iceberg/api mount that is the same value: the client named the macro levels itself. On
// a relative_to request the URL fixed them, so the client-visible value is the suffix below them,
// i.e. the levels the client sent, and this is what makes the two forms identity-preserving for a
// client: a namespace created as ["inv"] through
// /iceberg/relative_to/{repo}.{branch}/api is listed and read back as ["inv"], never as
// ["repo", "branch", "inv"] (conformance/RESULTS.md §10 D-4).
//
// Only a namespace that starts with exactly the levels the URL fixed is stripped (see the security
// note in the file comment). A repository-level entry - which a relative_to request cannot reach,
// because bindRelativeScope always supplies a parent - is returned unchanged rather than as an
// empty namespace.
func clientNamespace(r *http.Request, full Namespace) Namespace {
	fixed, ok := requestScope(r)
	if !ok {
		return full
	}
	if len(full) <= len(fixed) {
		return full
	}
	for i, level := range fixed {
		if full[i] != level {
			return full
		}
	}
	return full[len(fixed):]
}

// absoluteNamespaceLevels is clientNamespace's other direction for an address a client carried in
// a request body: the levels the URL fixed, followed by the levels the client sent.
//
// The create of namespaces.go is the only operation that takes its address from the body
// (POST /v1/{prefix}/namespaces), and without this rewrite the client of a relative_to URL cannot
// create a namespace at all: its ["inv"] would be read as a repository and a ref and answered 400
// (conformance/RESULTS.md §10 D-2). The handler applies it after it has decoded the body, so the
// body is still read through the iceberg.max_request_body bound of the catalog's own middleware
// stack, which runs behind RelativeTo.
func absoluteNamespaceLevels(r *http.Request, levels []string) []string {
	fixed, ok := requestScope(r)
	if !ok {
		return levels
	}
	absolute := make([]string, 0, len(fixed)+len(levels))
	absolute = append(absolute, fixed...)
	return append(absolute, levels...)
}

// relativeScope reads the warehouse segment of a relative_to request. The bool reports whether
// the request belongs to the relative_to form: a request without the warehouse URL parameter (the
// /iceberg/api mount, whose route pattern is relativeToPattern without a warehouse) is not, and
// must be served untouched.
func relativeScope(r *http.Request) (fixedLevels, bool, error) {
	if !hasURLParam(r, warehouseParam) {
		return nil, false, nil
	}
	segment := chi.URLParam(r, warehouseParam)
	fixed, err := parseWarehouseLevels(segment)
	if err != nil {
		return nil, false, fmt.Errorf("%w %q: %w", ErrInvalidWarehouse, segment, err)
	}
	return fixed, true, nil
}

// parseWarehouseLevels parses the warehouse segment into the levels it fixes (fixedLevels).
//
// The root Metadata Search tree has its own two warehouse forms (METADATA_SEARCH_REALIGN.md G3):
// `system.metadata` fixes {system, metadata}, and `system.metadata.{data repo}` fixes one more
// level. The first is also what ident.go ParseWarehouse answers for the segment - the tree's
// `system` repository with the fixed `metadata` ref - so only the third level is new. Everything
// else is the §5 form, unchanged: `{repo}` fixes the repository and `{repo}.{branch}` fixes the
// repository and the ref, with the ref split at the first dot so a tag may carry dots.
//
// `system.metadata.{data repo}` has a single level below `metadata` because a lakeFS repository
// name cannot contain a dot, so `system.metadata.a.b` names no warehouse and is answered 404
// NoSuchWarehouseException like every other segment that does not parse.
func parseWarehouseLevels(segment string) (fixedLevels, error) {
	if fixed, ok, err := systemMetadataWarehouseLevels(segment); ok {
		return fixed, err
	}
	scope, err := ParseWarehouse(segment)
	if err != nil {
		return nil, err
	}
	return scopeLevels(scope), nil
}

// systemMetadataWarehouseLevels recognises the root-tree warehouses of METADATA_SEARCH_REALIGN.md
// G3: the segment is `system.metadata.` followed by exactly one level, the data repository R. It
// reports whether the segment has that shape, so a segment below another repository stays with
// ParseWarehouse and its §5 rules.
//
// The segment is decoded once, like every path parameter, before the shape is decided - so an
// escaped warehouse (`%73ystem.metadata`) is the tree too - and R is validated as a lakeFS
// repository name. A segment that begins with `system.metadata.` is never handed back to
// ParseWarehouse: there it would be read as the repository `system` and a ref called
// `metadata.{R}`, which is not the address the client meant (the guide's tags may carry dots, but
// `system` is the reserved root of the catalog, so a data repository is what follows).
func systemMetadataWarehouseLevels(segment string) (fixedLevels, bool, error) {
	decoded, err := unescapeSegment(segment)
	if err != nil {
		// Let ParseWarehouse report the encoding error: it decodes the same segment.
		return nil, false, nil
	}
	prefix := SystemNamespace + "." + MetadataNamespace + "."
	if !strings.HasPrefix(decoded, prefix) {
		return nil, false, nil
	}
	dataRepo, extra, hasExtra := strings.Cut(strings.TrimPrefix(decoded, prefix), ".")
	if hasExtra {
		return nil, true, fmt.Errorf("%w: %q names %q below %s.%s, and a data repository is one level",
			ErrInvalidIdentifier, decoded, dataRepo+"."+extra, SystemNamespace, MetadataNamespace)
	}
	if err := validateRepo(dataRepo); err != nil {
		return nil, true, err
	}
	return fixedLevels{SystemNamespace, MetadataNamespace, dataRepo}, true, nil
}

// bindRelativeScope rewrites a relative_to request into the equivalent /iceberg/api request: the
// fixed warehouse levels are prepended to the address the client sent, wherever that address is
// (the {namespace} path parameter of the route path, or the parent query parameter), and
// everything else is left alone. The one address this function does not rewrite is the namespace
// in the body of a create: that request is decoded by the handler, which prepends the same levels
// with absoluteNamespaceLevels (D-2, see the file comment).
func bindRelativeScope(fixed fixedLevels, r *http.Request) {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return
	}
	if path, ok := prefixNamespacePath(rctx.RoutePath, fixed); ok {
		// A namespaces or tables address. The route path is what the catalog's own router matches
		// next (chi strips the mount prefix into RoutePath before calling the mounted handler), so
		// rewriting it is what makes the handlers read the address the /iceberg/api form would
		// have carried. The client's own segment keeps its encoding: only the levels this
		// middleware creates are encoded here (see the security note above).
		rctx.RoutePath = path
		return
	}
	if r.Method != http.MethodGet {
		// Only ListNamespaces takes an address in the query, and only a GET can be it. A POST on
		// the same path is the create, whose address is in the body and is rewritten by
		// absoluteNamespaceLevels; injecting a parent into its query would be meaningless.
		return
	}
	if parent := r.URL.Query().Get(parentParam); parent != "" {
		// ListNamespaces below a parent: the parent is already decoded by net/url, so the levels
		// are joined decoded and re-encoded by url.Values when the query is written back.
		levels := make([]string, 0, len(fixed)+1)
		levels = append(levels, fixed...)
		setQueryParam(r, parentParam, joinLevels(append(levels, parent), NamespaceSeparator))
		return
	}
	if strings.HasSuffix(r.URL.Path, namespacesCollectionPath) {
		// ListNamespaces with no parent. On the /iceberg/api form that lists repositories; on a
		// relative_to URL the warehouse is fixed, so it lists one level below the warehouse
		// itself: the namespaces of {repo}.{branch} (§5 row 4), the refs of {repo} (§5 row 5), the
		// data repositories of system.metadata and the refs of system.metadata.{data repo} (G3).
		setQueryParam(r, parentParam, joinLevels(fixed, NamespaceSeparator))
	}
}

// prefixNamespacePath inserts the warehouse levels into the namespace path parameter of a
// mount-relative route path, and reports whether the route path carries one:
//
//	/v1/lakefs/namespaces/{ns}/tables/{t} → /v1/lakefs/namespaces/{repo<sep>ref<sep>ns}/tables/{t}
//
// The separator is inserted in its encoded form; a handler decodes its path parameter exactly
// once (ident.go unescapeSegment), which is also how it reads the /iceberg/api form.
func prefixNamespacePath(routePath string, fixed fixedLevels) (string, bool) {
	marker := "/" + namespacesSegment + "/"
	start := strings.Index(routePath, marker)
	if start < 0 {
		return routePath, false
	}
	start += len(marker)
	warehouse := joinLevels(fixed, EncodedNamespaceSeparator)
	if warehouse == "" {
		return routePath, false
	}
	return routePath[:start] + warehouse + EncodedNamespaceSeparator + routePath[start:], true
}

// scopeLevels is the warehouse part of a `{repo}[.{branch}]` address: the repository, and the ref
// when the URL fixed one. §5 rows 4 and 5 differ in exactly this: {repo}.{branch} fixes two
// levels, {repo} one. The root-tree forms are parsed by systemMetadataWarehouseLevels directly,
// because their third level is below the ref and so is not part of a Scope.
func scopeLevels(scope Scope) fixedLevels {
	levels := make([]string, 0, macroLevels)
	levels = append(levels, scope.Repo)
	if scope.Ref != "" {
		levels = append(levels, scope.Ref)
	}
	return levels
}

// joinLevels joins address levels with the given separator, skipping an empty trailing level so
// that an empty parentless parent renders as the warehouse level itself.
func joinLevels(levels []string, separator string) string {
	parts := make([]string, 0, len(levels))
	for _, level := range levels {
		if level == "" {
			continue
		}
		parts = append(parts, level)
	}
	return strings.Join(parts, separator)
}

// hasURLParam reports whether the route that has matched so far has the named URL parameter. It
// only inspects the parameters matched *before* this middleware runs: the catalog's own routes
// (and their {namespace} parameter) are matched by the router behind RelativeTo.
func hasURLParam(r *http.Request, name string) bool {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return false
	}
	for _, key := range rctx.URLParams.Keys {
		if key == name {
			return true
		}
	}
	return false
}

// setQueryParam replaces one query parameter, keeping every other parameter and its value.
func setQueryParam(r *http.Request, name, value string) {
	query := r.URL.Query()
	query.Set(name, value)
	rewritten := *r.URL
	rewritten.RawQuery = query.Encode()
	r.URL = &rewritten
}
