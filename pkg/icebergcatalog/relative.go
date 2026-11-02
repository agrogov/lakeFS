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
// Both are published lakeFS recipes: the relative_to form is what DuckDB and Starburst use,
// because their ATTACH / CREATE CATALOG syntax (and the `warehouse` client property) has room
// for a URL but not for a repository and a ref inside the identifier
// (https://docs.lakefs.io/iceberg/, https://docs.lakefs.io/integrations/duckdb/,
// https://docs.lakefs.io/integrations/starburst_galaxy/, and §1.1 goal 2).
//
// http.go mounts the catalog twice for exactly this reason: at APIBasePath, and at
// <RelativeToBasePath>/{warehouse}/api, where chi keeps the segment as the "warehouse" URL
// parameter (relativeToPattern). RelativeTo is the middleware that turns the second mount into
// the first. It is a pure rewrite of the request and nothing more:
//
//  1. it parses the warehouse segment into a Scope (ident.go ParseWarehouse): {repo} fixes the
//     repository, {repo}.{branch} fixes the repository and the ref;
//  2. it prefixes the client's own levels with the fixed ones, joined with the Iceberg namespace
//     separator, so the request the handlers see is the request the same client would have sent
//     to /iceberg/api;
//  3. it delegates to the same handler chain, so both forms run the same code, authorize the
//     same way and answer identically. That identity is the point of I1-10: a table read through
//     /iceberg/relative_to/{repo}.{branch}/api and through /iceberg/api must be
//     indistinguishable, which is why nothing is un-prefixed on the way out either. A namespace
//     in a response is always the full [repo, ref, ns…] path, exactly as the Iceberg Namespace
//     type and the lakeFS docs describe it, whichever form asked.
//
// The rewrite covers the two places an address can appear in a catalog request, and only those:
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
//     otherwise be answered as "list the repositories".
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
// repository, or a repository and a ref (§5). Unlike ErrInvalidIdentifier it is not a client
// syntax error about an Iceberg identifier but a warehouse that this catalog cannot name at all,
// so the answer is 404 NoSuchWarehouseException, not 400.
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
		scope, relative, err := relativeScope(r)
		if err != nil {
			// The message names the shape the client has to use, because a warehouse is the one
			// part of the address the catalog does not correct for the client.
			writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse, fmt.Sprintf(
				"%s %s: %s; expected %s/{repo}[.{branch}]/api/v1/...", r.Method, r.URL.Path, err,
				RelativeToBasePath))
			return
		}
		if relative {
			bindRelativeScope(scope, r)
		}
		next.ServeHTTP(w, r)
	})
}

// relativeScope reads the warehouse segment of a relative_to request. The bool reports whether
// the request belongs to the relative_to form: a request without the warehouse URL parameter (the
// /iceberg/api mount, whose route pattern is relativeToPattern without a warehouse) is not, and
// must be served untouched.
func relativeScope(r *http.Request) (Scope, bool, error) {
	if !hasURLParam(r, warehouseParam) {
		return Scope{}, false, nil
	}
	scope, err := ParseWarehouse(chi.URLParam(r, warehouseParam))
	if err != nil {
		return Scope{}, false, fmt.Errorf("%w %q: %w", ErrInvalidWarehouse,
			chi.URLParam(r, warehouseParam), err)
	}
	return scope, true, nil
}

// bindRelativeScope rewrites a relative_to request into the equivalent /iceberg/api request: the
// fixed warehouse levels are prepended to the address the client sent, wherever that address is
// (the {namespace} path parameter of the route path, or the parent query parameter), and
// everything else is left alone.
func bindRelativeScope(scope Scope, r *http.Request) {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return
	}
	if path, ok := prefixNamespacePath(rctx.RoutePath, scope); ok {
		// A namespaces or tables address. The route path is what the catalog's own router matches
		// next (chi strips the mount prefix into RoutePath before calling the mounted handler), so
		// rewriting it is what makes the handlers read the address the /iceberg/api form would
		// have carried. The client's own segment keeps its encoding: only the levels this
		// middleware creates are encoded here (see the security note above).
		rctx.RoutePath = path
		return
	}
	if parent := r.URL.Query().Get(parentParam); parent != "" {
		// ListNamespaces below a parent: the parent is already decoded by net/url, so the levels
		// are joined decoded and re-encoded by url.Values when the query is written back.
		setQueryParam(r, parentParam, joinLevels(
			append(scopeLevels(scope), parent), NamespaceSeparator))
		return
	}
	if strings.HasSuffix(r.URL.Path, namespacesCollectionPath) {
		// ListNamespaces with no parent. On the /iceberg/api form that lists repositories; on a
		// relative_to URL the warehouse is fixed, so it lists one level below the warehouse
		// itself: the namespaces of {repo}.{branch} (§5 row 4) or the refs of {repo} (§5 row 5).
		setQueryParam(r, parentParam, joinLevels(scopeLevels(scope), NamespaceSeparator))
	}
}

// prefixNamespacePath inserts the warehouse levels into the namespace path parameter of a
// mount-relative route path, and reports whether the route path carries one:
//
//	/v1/lakefs/namespaces/{ns}/tables/{t} → /v1/lakefs/namespaces/{repo<sep>ref<sep>ns}/tables/{t}
//
// The separator is inserted in its encoded form; a handler decodes its path parameter exactly
// once (ident.go unescapeSegment), which is also how it reads the /iceberg/api form.
func prefixNamespacePath(routePath string, scope Scope) (string, bool) {
	marker := "/" + namespacesSegment + "/"
	start := strings.Index(routePath, marker)
	if start < 0 {
		return routePath, false
	}
	start += len(marker)
	warehouse := joinLevels(scopeLevels(scope), EncodedNamespaceSeparator)
	if warehouse == "" {
		return routePath, false
	}
	return routePath[:start] + warehouse + EncodedNamespaceSeparator + routePath[start:], true
}

// scopeLevels is the warehouse part of an address: the repository, and the ref when the URL
// fixed one. §5 rows 4 and 5 differ in exactly this: {repo}.{branch} fixes two levels, {repo}
// one.
func scopeLevels(scope Scope) []string {
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
