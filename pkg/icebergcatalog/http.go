package icebergcatalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
)

const (
	// APIBasePath is where the catalog is mounted. Clients take the prefix from
	// GET /v1/config (overrides.prefix) and call <APIBasePath>/v1/<prefix>/... here
	// (ICEBERG_CATALOG_PLAN.md §2.1, §5).
	APIBasePath = "/iceberg/api"

	// RelativeToBasePath is where the second documented addressing form is mounted: the client
	// carries the warehouse in the URL and appends the catalog path to it,
	// <RelativeToBasePath>/{repo}[.{branch}]/api/v1/... (§5, §1.1 goal 2).
	RelativeToBasePath = "/iceberg/relative_to"

	// relativeToPattern is the route under RelativeToBasePath. chi mounts it as
	// <RelativeToBasePath>/{warehouse}/api/*, which is also how the warehouse segment survives
	// as the "warehouse" URL parameter for ident.go to bind (I1-10). Everything after /api is
	// routed by the same sub-router the /iceberg/api mount serves, so both forms answer the
	// same request identically.
	relativeToPattern = "/{warehouse}/api"

	// configPath is the negotiation endpoint of §2.1. It is the one catalog operation this task
	// implements: every other route is added to catalogRoutes together with its test.
	configPath = "/v1/config"

	// DisabledMessage is answered with 501 while iceberg.enabled is false (§10).
	DisabledMessage = "the Apache Iceberg catalog is disabled on this server"

	contentTypeJSON = "application/json"
)

// The errors the router returns while it is being built. They are startup errors: run.go turns
// a builder error into a fatal, because a catalog that cannot be mounted must not be half-served.
var (
	// ErrMissingDeps is returned when the catalog is enabled but the builder was not called
	// through cmd.AddExtraRoutesBuilder, i.e. without server dependencies.
	ErrMissingDeps = errors.New("icebergcatalog: enabled but the server dependencies are missing")

	// ErrAuthenticationRequired is answered with 401 when a request reaches the catalog without
	// an authenticated principal. The catalog's own middleware (auth.go) authenticates but lets an
	// anonymous request through to the handler, so the catalog adds an explicit requirement in
	// front of every route (§7.1).
	ErrAuthenticationRequired = errors.New("authentication required")

	// errNonGoalEndpoint is returned for a route entry that would advertise an operation §1.2
	// puts out of scope for v1.
	errNonGoalEndpoint = errors.New("icebergcatalog: endpoint is out of scope for v1 and must not be advertised")
)

// nonGoalEndpointFragments are the operations §1.2 excludes from v1: views, table rename,
// multi-table transaction commit and server-side scan planning. An advertised endpoint is a
// promise: §2.1 says a client calls only the endpoints the server lists, so advertising one of
// these would either be a lie or force clients into a path this catalog does not implement.
// A route that carries one of these fragments is rejected while the router is built, not when a
// client first calls it.
var nonGoalEndpointFragments = []string{
	"/views",
	"/tables/rename",
	"/transactions/",
	"/plan",
	"/tasks",
}

// RoutesBuilder mounts the catalog and is registered by the lakefs-sso binary through
// cmd.AddExtraRoutesBuilder next to datasets.RoutesBuilder (ICEBERG_CATALOG_PLAN.md §4.1).
// It returns an error for an invalid configuration or unusable dependencies; the hook loop in
// run.go turns that into a fatal startup error.
//
// While iceberg.enabled is false the catalog mounts nothing but its 501 answer: the prefixes are
// still claimed, so a client asking a disabled catalog gets "disabled" in the Iceberg error
// envelope rather than the core placeholder or the lakeFS UI. Claiming the prefixes is also what
// keeps api.MountNotImplementedServices from mounting its own placeholder over a live catalog
// (I0-3, §4.1).
//
// ctx is the server's lifetime context. Mounting is synchronous and starts no goroutine, so
// nothing is bound to it yet; I1-7 (store.go) and I1-5 (the OAuth token cache) are the first
// users.
func RoutesBuilder(ctx context.Context, deps cmdpkg.ServerDeps, r *chi.Mux) error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		MountDisabled(r)
		return nil
	}
	if deps.Config == nil || deps.AuthService == nil || deps.Authenticator == nil || deps.Logger == nil {
		return ErrMissingDeps
	}

	// The catalog authenticates itself: the middleware of auth.go accepts a lakeFS bearer token
	// or HTTP basic on every route (§7.1) and serves POST /v1/oauth/tokens to a client that
	// presents an access key pair (oauth.go), so a Spark, Trino, PyIceberg or DuckDB client can
	// connect with just its credentials. The core AuthMiddleware is bound to the swagger routes
	// and knows nothing about the token endpoint.
	authMiddleware, err := newAuthMiddleware(deps, cfg)
	if err != nil {
		return err
	}

	// The operations of §2.1 that need the server's catalog, block adapter and auth service are
	// contributed by a service of their own (namespaces.go, tables.go). Their endpoint strings reach
	// GET /v1/config through the route table, so the catalog cannot advertise an operation it does
	// not mount.
	//
	// A builder that was handed no catalog or block adapter (a unit test's minimal ServerDeps)
	// mounts GET /v1/config only, and then advertises nothing but itself: advertisedEndpoints is
	// derived from the mounted routes, so an empty endpoint list is the true statement that this
	// router serves no catalog operation. The lakefs-sso binary always supplies both.
	var services []routeSource
	if deps.Catalog != nil && deps.BlockAdapter != nil && deps.AuthService != nil {
		store := NewStore(deps.Catalog, deps.BlockAdapter)
		authorizer := NewAuthorizer(deps.AuthService)
		services = append(services,
			NewNamespaceService(store, authorizer, deps.Catalog, cfg),
			// The table reads of I1-9, the staged create of I2-4, and the drop and metrics sink
			// of I2-5.
			NewTableService(store, authorizer, deps.Catalog, cfg),
			// Register (register.go): adopting a metadata.json written outside the catalog. It is its
			// own service, so its endpoint is advertised exactly when it is mounted.
			NewRegisterService(store, authorizer, deps.Catalog, cfg),
			// The two data-plane services of §8. Each is enabled only by what it needs - remote
			// signing by an S3 blockstore whose pre-signer is available, credential vending by an
			// S3 blockstore plus a configured blockstore.s3.credentials_vending.role_arn - and a
			// disabled service contributes neither a route nor an endpoint, so GET /v1/config
			// advertises exactly the data-plane operations that are actually mounted.
			NewRemoteSigningService(store, authorizer, deps.BlockAdapter, deps.Catalog, cfg),
			NewCredentialService(store, authorizer, cfg, nil, deps.Logger))
	}
	s := newServer(cfg, services...)
	// Responses to a request that carries an Idempotency-Key are kept in the server's KV. Only
	// when there is a KV to keep them in does the catalog wrap its mutations and advertise
	// idempotency-key-lifetime (I5-3): §2.1 requires the advertised window to be real. A
	// builder handed no KVStore (a minimal test deps) serves the catalog without idempotency.
	if deps.KVStore != nil {
		s.idempotency = NewIdempotencyStore(deps.KVStore, DefaultIdempotencyKeyLifetime)
	}
	handler, err := newRouter(authMiddleware, cfg, s)
	if err != nil {
		return err
	}

	r.Mount(APIBasePath, handler)

	// The relative_to form gets its own mount so that a path without the
	// <warehouse>/api shape is answered by the catalog, not by the core catch-all.
	relativeTo := chi.NewRouter()
	relativeTo.NotFound(func(w http.ResponseWriter, req *http.Request) {
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
			fmt.Sprintf("%s %s does not name a warehouse: expected %s/{repo}[.{branch}]/api/v1/...",
				req.Method, req.URL.Path, RelativeToBasePath))
	})
	relativeTo.Mount(relativeToPattern, handler)
	r.Mount(RelativeToBasePath, relativeTo)
	return nil
}

// newAuthMiddleware builds the catalog's authentication middleware from the server dependencies
// (auth.go). It is the single point at which the catalog's own scheme list replaces the core
// api.GenericAuthMiddleware, and the one call site where the token endpoint of oauth.go becomes
// reachable: a catalog mounted with any other middleware answers POST /v1/oauth/tokens with 404.
//
// The startup error is wrapped here so that a fatal mount error names the catalog.
func newAuthMiddleware(deps cmdpkg.ServerDeps, cfg *Config) (func(http.Handler) http.Handler, error) {
	middleware, err := NewAuthMiddleware(deps, cfg)
	if err != nil {
		return nil, fmt.Errorf("icebergcatalog: build auth middleware: %w", err)
	}
	return middleware, nil
}

// MountDisabled mounts the catalog's 501 answer at both catalog prefixes. The prefixes are
// claimed even though the catalog is off: a client that asks for it must be told it is
// disabled, and the answer is a catalog error like any other (§12).
func MountDisabled(r *chi.Mux) {
	disabled := DisabledHandler()
	r.Mount(APIBasePath, disabled)
	r.Mount(RelativeToBasePath, disabled)
}

// DisabledHandler answers every catalog request with 501 and the error envelope.
func DisabledHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeIcebergError(w, http.StatusNotImplemented, ErrTypeNotImplemented, DisabledMessage)
	})
}

// NewRouter builds the catalog sub-router. It is mounted at APIBasePath (and, with a warehouse
// segment in between, at RelativeToBasePath) by RoutesBuilder, so its routes are relative to
// that mount point. services are the operation tables of the catalog's own services (namespaces
// and tables): every operation they carry is mounted and, through the route table, advertised in
// GET /v1/config.
//
// The middleware stack is ordered the way §12 and I1-3 require, and the order is not
// interchangeable:
//
//  1. authentication (auth.go), which accepts a lakeFS bearer token or HTTP basic, answers
//     POST /v1/oauth/tokens itself, and whose 401 and 403 answers are rewritten into the
//     catalog envelope (authenticate);
//  2. the authenticated-user requirement: 401 before anything else happens;
//  3. the request-body limit (iceberg.max_request_body), so no body is buffered unboundedly;
//  4. the handlers, which validate and decode the request.
//
// Authentication therefore runs before any request or body validation. This is the order the
// datasets control plane established for its own mount (pkg/datasets/http.go runs the spec
// validator last); the core API builds its stack the other way round (pkg/api/serve.go runs
// OapiRequestValidatorWithOptions before AuthMiddleware), which lets an unauthenticated caller
// learn whether its request was well-formed and makes the server parse a body it will reject
// anyway. The catalog keeps the corrected order, and a request without a principal is answered
// 401 without its body being read.
//
// authMiddleware is a parameter (rather than built here) so the router can be exercised without
// a full auth service; a nil middleware leaves only the authenticated-user requirement, so every
// route answers 401.
func NewRouter(authMiddleware func(http.Handler) http.Handler, cfg *Config, services ...routeSource) (http.Handler, error) {
	return newRouter(authMiddleware, cfg, newServer(cfg, services...))
}

// newRouter is NewRouter with the catalog surface injected, so a test can put an operation that
// decodes a body - or one that advertises a §1.2 non-goal - in front of the same middleware
// stack before the read-path handlers land (I1-8, I1-9).
func newRouter(authMiddleware func(http.Handler) http.Handler, cfg *Config, s *server) (http.Handler, error) {
	router := chi.NewRouter()

	// Wrap the authentication middleware exactly, not the whole chain: the envelope is for its
	// rejections only (see authErrorWriter).
	router.Use(func(next http.Handler) http.Handler {
		return authenticate(authMiddleware, next)
	})
	router.Use(requireUser)
	router.Use(limitRequestBody(maxRequestBody(cfg)))

	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeIcebergError(w, http.StatusNotFound, ErrTypeNotFound,
			fmt.Sprintf("%s %s is not an Apache Iceberg REST catalog endpoint", r.Method, r.URL.Path))
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeIcebergError(w, http.StatusMethodNotAllowed, ErrTypeMethodNotAllowed,
			fmt.Sprintf("%s is not supported for %s", r.Method, r.URL.Path))
	})
	if err := s.mount(router); err != nil {
		return nil, err
	}
	return router, nil
}

// authenticate runs the authentication middleware and answers its failures in the catalog's
// error envelope (authErrorWriter). It is a function rather than a plain chi middleware because
// the wrapper has to end where the middleware's own answers end: every handler behind it already
// writes the envelope.
func authenticate(authMiddleware func(http.Handler) http.Handler, next http.Handler) http.Handler {
	if authMiddleware == nil {
		return next
	}
	downstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writer, ok := w.(*authErrorWriter); ok {
			writer.downstream = true
		}
		next.ServeHTTP(w, r)
	})
	guarded := authMiddleware(downstream)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		guarded.ServeHTTP(&authErrorWriter{ResponseWriter: w}, r)
	})
}

// requireUser rejects a request that carries no authenticated principal. It runs immediately
// after authentication, which puts the user on the context (auth.WithUser) but deliberately lets
// an unauthenticated request through. Every catalog operation requires a principal, including
// GET /v1/config: the catalog is served over the same credentials as the rest of lakeFS, and a
// client that can reach it can reach every repository it names.
func requireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := auth.GetUser(r.Context()); err != nil {
			writeIcebergError(w, http.StatusUnauthorized, ErrTypeNotAuthorized, ErrAuthenticationRequired.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

// limitRequestBody bounds what the rest of the stack may read from the request body
// (iceberg.max_request_body, §10). It wraps the body in http.MaxBytesReader, so a read fails as
// soon as the limit is exceeded and nothing buffers more than limit bytes; decodeJSONBody turns
// that failure into 413. A non-positive limit means DefaultMaxRequestBody, so a zero Config built
// in a test cannot silently remove the bound.
func limitRequestBody(limit int64) func(http.Handler) http.Handler {
	if limit <= 0 {
		limit = DefaultMaxRequestBody
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// maxRequestBody is the configured body limit, with the documented default for a Config that was
// not built by LoadConfig.
func maxRequestBody(cfg *Config) int64 {
	if cfg == nil || cfg.MaxRequestBody <= 0 {
		return DefaultMaxRequestBody
	}
	return cfg.MaxRequestBody
}

// decodeJSONBody decodes one JSON request body into dst and reports whether it succeeded. On
// failure it writes the catalog error and the caller returns: a body over
// iceberg.max_request_body is 413, an empty or malformed body is 400 (§12).
//
// The handlers that decode a body are the write operations: POST /v1/oauth/tokens (I1-5), the
// remote-signing endpoint (I-4), and the create of I2-4 (tables.go handleCreateTable). The helper
// is written here, next to the limit it enforces, so that the 413 answer has exactly one owner
// instead of being re-derived by every handler; the router tests exercise it through the
// production middleware stack.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	err := json.NewDecoder(r.Body).Decode(dst)
	if err == nil {
		return true
	}
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		writeIcebergError(w, http.StatusRequestEntityTooLarge, ErrTypeRequestTooLarge,
			fmt.Sprintf("request body exceeds the server limit of %d bytes", maxBytesErr.Limit))
		return false
	}
	if errors.Is(err, io.EOF) {
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, "the request body is empty")
		return false
	}
	writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, "malformed request body: "+err.Error())
	return false
}

// CatalogConfig is the response of GET /v1/config (spec: components.schemas.CatalogConfig).
// Endpoints is always sent, even while it is still empty: an absent list means "the default
// set", which includes table rename and multi-table commit (§2.1), so absence would be a promise
// this catalog does not keep.
//
// IdempotencyKeyLifetime is present only when the catalog actually stores responses to a request
// that carries an Idempotency-Key (I5-3). The spec reads its presence as "this server supports
// Idempotency-Key semantics" and its value as the window the responses are kept for, so an
// absent field is the truthful answer for a server with no KV wired, and an empty value is never
// sent.
type CatalogConfig struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
	Endpoints []string          `json:"endpoints"`

	// IdempotencyKeyLifetime is an ISO-8601 duration (PT30M), the replay window of I5-3.
	IdempotencyKeyLifetime string `json:"idempotency-key-lifetime,omitempty"`
}

// routeSource is a catalog service that contributes operations to the route table: every route it
// returns is mounted on the catalog router and, when it carries a wire form, advertised in
// GET /v1/config. The namespace service (namespaces.go) is the first implementation.
type routeSource interface {
	Routes() []route
}

// route is one catalog operation: the chi route that serves it, and the wire form the catalog
// advertises for it in GET /v1/config.
type route struct {
	method  string
	pattern string
	handler http.HandlerFunc
	// endpoint is the "METHOD /v1/{prefix}/..." string clients match an operation against. It is
	// empty for an operation that is served but never advertised - GET /v1/config is the one:
	// the spec makes it part of every catalog and lists no endpoint for it.
	endpoint string
}

// server is the catalog's HTTP surface: its configuration, the route table and the endpoint
// list those routes advertise. (It is not named after the catalog because pkg/catalog, which
// store.go reads through, is imported as `catalog` in this package.)
type server struct {
	cfg *Config
	// routes is the whole route table: the catalog's own operations followed by the route tables
	// of its services.
	routes []route
	// services are the operation tables this catalog mounts in addition to GET /v1/config.
	services []routeSource
	// endpoints is the explicit list GET /v1/config returns, derived from routes so that it
	// cannot promise an operation the router does not serve (§2.1).
	endpoints []string
	// idempotency stores the final response of a mutating operation by its Idempotency-Key and
	// replays it (idempotency.go, I5-3). A nil store means the server has no KV to keep them in:
	// mutating routes are mounted unwrapped and GET /v1/config advertises no lifetime.
	idempotency *IdempotencyStore
}

// newServer builds the catalog's HTTP surface and derives its endpoint list from its own route
// table. A server without services is a catalog that serves GET /v1/config only.
func newServer(cfg *Config, services ...routeSource) *server {
	s := &server{cfg: cfg, services: services}
	s.routes = s.ownRoutes()
	s.endpoints = advertisedEndpoints(s.routes)
	return s
}

// ownRoutes is the catalog's route table and with it the compatibility contract of §2.1.
//
// An operation is added here - and so both mounted and advertised - only when it is implemented
// and its test passes. GET /v1/config is the catalog's own operation; everything else arrives with
// a service that contributes its Routes(), so the endpoint list is exactly what the router serves.
// The read path arrives in this order: namespaces (I1-8), tables (I1-9), relative_to (I1-10, which
// reuses these routes), then the write path of I-2 (namespaces here, tables in I2-4 and - drop and
// report-metrics - I2-5).
func (s *server) ownRoutes() []route {
	routes := make([]route, 0, 1+len(s.services))
	routes = append(routes, route{method: http.MethodGet, pattern: configPath, handler: s.handleConfig})
	for _, service := range s.services {
		routes = append(routes, service.Routes()...)
	}
	return routes
}

// advertisedEndpoints is the endpoints list of GET /v1/config: every route that carries a wire
// form, in table order. The result is never nil, so the response always has an array.
func advertisedEndpoints(routes []route) []string {
	endpoints := make([]string, 0, len(routes))
	for _, rt := range routes {
		if rt.endpoint != "" {
			endpoints = append(endpoints, rt.endpoint)
		}
	}
	return endpoints
}

// mount registers the route table on r, and refuses a route that would advertise a §1.2
// non-goal. A mutating operation is additionally wrapped in the Idempotency-Key middleware of
// I5-3 when the server has a store, so a retry with the same key replays the stored final
// response instead of running the operation again. Read operations (GET, HEAD, OPTIONS) are
// never wrapped: they have no effect to repeat, and the spec defines the key for mutations.
func (s *server) mount(r chi.Router) error {
	for _, rt := range s.routes {
		if err := checkEndpointScope(rt.endpoint); err != nil {
			return err
		}
		handler := rt.handler
		if s.idempotency != nil && isMutatingMethod(rt.method) {
			handler = s.idempotency.middleware(handler).ServeHTTP
		}
		r.Method(rt.method, rt.pattern, handler)
	}
	return nil
}

// isMutatingMethod reports whether a method can change catalog state, i.e. whether an operation
// served with it is one an Idempotency-Key applies to. OPTIONS is not a catalog operation here
// (chi answers it separately), and GET and HEAD only read.
func isMutatingMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

// checkEndpointScope rejects an endpoint that belongs to an operation §1.2 puts out of scope.
func checkEndpointScope(endpoint string) error {
	for _, fragment := range nonGoalEndpointFragments {
		if endpoint != "" && strings.Contains(endpoint, fragment) {
			return fmt.Errorf("%w: %q contains %q", errNonGoalEndpoint, endpoint, fragment)
		}
	}
	return nil
}

// prefix is the value of the prefix override: the constant clients prepend to every identifier
// they send back (§5, §14 Q-I3).
func (s *server) prefix() string {
	if s.cfg == nil || s.cfg.Prefix == "" {
		return CatalogPrefix
	}
	return s.cfg.Prefix
}

// handleConfig is GET /v1/config, the negotiation endpoint of §2.1.
//
// Two of its fields are constants of the catalog: overrides.prefix, which clients insert into
// every path, and endpoints, the explicit list of operations this server serves. defaults carries
// no property, because the catalog is addressed by the identifier itself
// (<repo>.<ref>.<namespace>[.<ns>...].<table>, §5) rather than by a warehouse property a server
// would have to impose on the client.
func (s *server) handleConfig(w http.ResponseWriter, _ *http.Request) {
	response := CatalogConfig{
		Defaults:  map[string]string{},
		Overrides: map[string]string{"prefix": s.prefix()},
		Endpoints: s.endpoints,
	}
	// The lifetime is a promise about stored responses (§2.1), so it is sent only when there is
	// storage behind it. ISO8601Lifetime is empty for a server without an IdempotencyStore, and
	// omitempty keeps the field out of the body entirely in that case.
	if s.idempotency != nil {
		response.IdempotencyKeyLifetime = s.idempotency.ISO8601Lifetime()
	}
	writeJSON(w, http.StatusOK, response)
}

// writeJSON writes one catalog response body. A response that cannot be marshalled is answered
// with 500 and the envelope instead, because a client cannot decode anything else.
func writeJSON(w http.ResponseWriter, status int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		writeIcebergError(w, http.StatusInternalServerError, "", "cannot encode the response")
		return
	}
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
