package api

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// notImplementedMounts are the URL prefixes reserved for services that the open-source build
// does not serve. When no hook claimed a prefix, mounting a placeholder answers 501 there
// instead of letting the request reach the router's root handler, which serves the UI or an
// S3-gateway error and so answers something other than the intended 501.
var notImplementedMounts = []string{
	"/iceberg/api/",         // Iceberg REST Catalog (ICEBERG_CATALOG_PLAN.md §4.1, I-1)
	"/iceberg/relative_to/", // the same catalog, with the warehouse carried in the URL
	"/mds/iceberg/api/",     // never implemented: this one stays a 501 in every build
}

// MountNotImplementedServices registers placeholder routes that return HTTP 501 for
// services not available in the open-source build, skipping every prefix a wrapper binary
// has already claimed. A hook (lakefs-sso) mounts the real handler for a prefix it enables,
// and that handler must win for two reasons: chi v5.2.4 panics when Mount() targets a
// pattern that is already mounted ("attempting to Mount() a handler on an existing path",
// mux.go:297), and a second mount would shadow the real handler anyway.
//
// Callers must therefore invoke it *after* the extra-routes builders, as
// cmd/lakefs/cmd/run.go and pkg/api/serve_test.go do. See ICEBERG_CATALOG_PLAN.md §4.1.
func MountNotImplementedServices(router *chi.Mux) {
	for _, prefix := range notImplementedMounts {
		if mountPatternClaimed(router, prefix) {
			continue
		}
		router.Mount(prefix, http.HandlerFunc(NotImplementedIcebergCatalogHandler))
	}
}

// mountPatternClaimed reports whether prefix already has a handler registered on router.
//
// Find() with a fresh RouteContext is chi's own way of asking "would a request for this
// path be routed here, and by which pattern?". The pattern matters: the core router ends
// with a catch-all, so a bare Match() answers true for every path, and only a pattern that
// itself sits under prefix means the prefix is taken. A sub-router, however, matches only
// the exact paths it serves, which cannot be known here, so Routes() is consulted as well:
// chi records every mount (plain handler or sub-router, empty or not) as the pattern
// "<prefix>*", the very pattern its Mount panic check looks for.
func mountPatternClaimed(router *chi.Mux, prefix string) bool {
	if pattern := router.Find(chi.NewRouteContext(), http.MethodGet, prefix+"probe"); strings.HasPrefix(pattern, prefix) {
		return true
	}
	mountPattern := prefix + "*"
	for _, route := range router.Routes() {
		if route.Pattern == mountPattern {
			return true
		}
	}
	return false
}

// NotImplementedIcebergCatalogHandler returns HTTP 501 Not Implemented status for Iceberg REST Catalog endpoints.
func NotImplementedIcebergCatalogHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
}
