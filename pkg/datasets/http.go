package datasets

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/go-chi/chi/v5"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
	"github.com/treeverse/lakefs/pkg/httputil"
)

//go:generate go run github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1.5.6 -package apigen -generate "types,client,chi-server,spec" -templates ../api/tmpl -o apigen/datasets.gen.go ../../api/datasets.yml

// APIBasePath is the base path of the datasets control plane. It sits next to the core
// /api/v1 routes; chi resolves the more specific mount before the /api/v1 catch-all
// (InvalidAPIEndpointHandler). Spike S1 in http_test.go pins that behaviour
// (DATASETS_DESIGN.md §4.5, §18).
const APIBasePath = apiutil.BaseURL + "/datasets"

// DisabledMessage is returned with 501 when a request reaches the control plane while
// datasets.enabled is false.
const DisabledMessage = "datasets are disabled on this server"

// ErrMissingDeps is returned when the control plane is enabled but the builder was not called
// through cmd.AddExtraRoutesBuilder, i.e. without server dependencies.
var ErrMissingDeps = errors.New("datasets: enabled but the server dependencies are missing")

// ErrAuthenticationRequired is answered with 401 when a request reaches the control plane
// without an authenticated principal. api.GenericAuthMiddleware authenticates but leaves
// anonymous requests to the handler (as it does for the core API, where the controller decides),
// so the datasets router adds an explicit requirement in front of every route (§4.5).
var ErrAuthenticationRequired = errors.New("authentication required")

// RoutesBuilder mounts the datasets control plane and is registered by the lakefs-sso binary
// through cmd.AddExtraRoutesBuilder (DATASETS_DESIGN.md §4.4). It returns an error for an
// invalid configuration; the hook loop in run.go turns that into a fatal startup error.
//
// ctx bounds the background goroutines the builder may start (the reconciler, §12.3). It is
// unused until the reconciler lands.
func RoutesBuilder(_ context.Context, deps cmdpkg.ServerDeps, r *chi.Mux) error {
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

	// The core AuthMiddleware is bound to the swagger routes, so the datasets router wraps
	// itself with the exported GenericAuthMiddleware. It honours the same schemes as the core
	// API and puts the authenticated user on the context (DATASETS_DESIGN.md §4.5).
	baseAuthCfg := deps.Config.AuthConfig().GetBaseAuthConfig()
	oidcConfig := auth.OIDCConfig(baseAuthCfg.OIDC)
	cookieAuthConfig := auth.CookieAuthConfig(baseAuthCfg.CookieAuthVerification)
	authMiddleware, err := api.GenericAuthMiddleware(deps.Logger, deps.Authenticator, deps.AuthService, &oidcConfig, &cookieAuthConfig)
	if err != nil {
		return fmt.Errorf("datasets: build auth middleware: %w", err)
	}

	datasetsRouter, err := NewRouter(authMiddleware, newStubServer())
	if err != nil {
		return err
	}
	r.Mount(APIBasePath, datasetsRouter)
	return nil
}

// NewRouter builds the datasets sub-router. It is mounted at APIBasePath by RoutesBuilder, so
// the routes registered by the generated server are relative to that mount point.
//
// The middleware stack mirrors the core API (pkg/api/serve.go): request validation first, then
// authentication. The validator answers a malformed request with the datasets error shape
// (§10.4) before any handler runs; the authenticated-user requirement after it answers 401,
// so no control-plane route is reachable without a principal (DATASETS_DESIGN.md §4.5).
//
// authMiddleware is a parameter (rather than built here) so the router can be exercised without
// a full auth service - without one nothing can authenticate, so every route answers 401.
// server is a parameter so later cards can supply the real implementation.
func NewRouter(authMiddleware func(http.Handler) http.Handler, server apigen.ServerInterface) (http.Handler, error) {
	swagger, err := apigen.GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("datasets: load control-plane spec: %w", err)
	}
	validator, err := newRequestValidator(swagger)
	if err != nil {
		return nil, err
	}

	router := chi.NewRouter()
	router.Use(validator)
	if authMiddleware != nil {
		router.Use(authMiddleware)
	}
	router.Use(requireUser)
	apigen.HandlerFromMux(server, router)
	return router, nil
}

// requireUser rejects requests that carry no authenticated principal. It runs after
// api.GenericAuthMiddleware, which puts the authenticated user on the context (auth.WithUser)
// but deliberately lets unauthenticated requests through (DATASETS_DESIGN.md §4.5). Every
// datasets control-plane route requires a user.
func requireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := auth.GetUser(r.Context()); err != nil {
			writeErrorResponse(w, r, http.StatusUnauthorized, errorResponse{Message: ErrAuthenticationRequired.Error()})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// MountDisabled mounts the 501 handler for a disabled control plane. The routes are mounted
// anyway: a client asking for datasets must get "disabled" rather than "invalid API endpoint".
func MountDisabled(r *chi.Mux) {
	r.Mount(APIBasePath, DisabledHandler())
}

// DisabledHandler answers every datasets request with 501 and the core Error JSON shape.
func DisabledHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httputil.WriteAPIError(w, r, http.StatusNotImplemented, DisabledMessage)
	})
}

// errorResponse is the datasets error body (DATASETS_DESIGN.md §10.4): the core Error shape
// {"message": ...} plus, for validation failures, one errorDetail per problem.
type errorResponse struct {
	Message string        `json:"message"`
	Errors  []errorDetail `json:"errors,omitempty"`
}

// errorDetail locates one validation problem, e.g. {"path": "data[2].target", ...}.
type errorDetail struct {
	Path    string `json:"path,omitempty"`
	Message string `json:"message"`
}

// newRequestValidator builds the oapi-codegen request validator over api/datasets.yml
// (DATASETS_DESIGN.md §4.5, §10.4).
//
// It is the datasets counterpart of api.OapiRequestValidatorWithOptions: the same
// kin-openapi validation, but errors are rendered as the datasets error shape with the list of
// problems instead of the core package's single-message writer.
func newRequestValidator(swagger *openapi3.T) (func(http.Handler) http.Handler, error) {
	router, err := legacy.NewRouter(swagger)
	if err != nil {
		return nil, fmt.Errorf("datasets: build request validator: %w", err)
	}
	options := &openapi3filter.Options{
		// Authentication is enforced by api.GenericAuthMiddleware, not by the spec validator
		// (same split as the core API, pkg/api/serve.go).
		AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route, pathParams, err := findRoute(router, r)
			if err != nil {
				writeErrorResponse(w, r, routeErrorStatus(err), errorResponse{Message: err.Error()})
				return
			}
			input := &openapi3filter.RequestValidationInput{
				Request:    r,
				PathParams: pathParams,
				Route:      route,
				Options:    options,
			}
			if err := openapi3filter.ValidateRequest(r.Context(), input); err != nil {
				writeValidationError(w, r, err)
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

// findRoute matches the request against the spec.
//
// The spec declares a relative server URL ("/api/v1/datasets") and paths relative to it, and
// the sub-router is mounted on that same base path. kin-openapi reports the remaining path of
// the collection root as "", which matches no path in the spec, so the mount point itself is
// probed as "<base>/" (the form the sub-router already serves). The original request is left
// untouched so handlers see exactly what the client sent.
func findRoute(router routers.Router, r *http.Request) (*routers.Route, map[string]string, error) {
	probe := r
	if probe.URL.Path == APIBasePath {
		probe = r.Clone(r.Context())
		probe.URL.Path += "/"
	}
	return router.FindRoute(probe)
}

// routeErrorStatus maps a route lookup failure to its HTTP status: an unknown path under the
// control plane is 404 and a known path with an unsupported method is 405 (§10.4). Anything else
// is a client error.
func routeErrorStatus(err error) int {
	var routeErr *routers.RouteError
	if errors.As(err, &routeErr) {
		switch routeErr.Reason {
		case routers.ErrPathNotFound.Error():
			return http.StatusNotFound
		case routers.ErrMethodNotAllowed.Error():
			return http.StatusMethodNotAllowed
		}
	}
	return http.StatusBadRequest
}

// writeValidationError maps a validation failure to a status code and the datasets error body
// (§10.4): 400 for a malformed request or body, 401 for a missing security requirement, 500
// otherwise.
func writeValidationError(w http.ResponseWriter, r *http.Request, err error) {
	var reqErr *openapi3filter.RequestError
	var secErr *openapi3filter.SecurityRequirementsError
	switch {
	case errors.As(err, &reqErr):
		writeErrorResponse(w, r, http.StatusBadRequest, errorResponse{
			Message: err.Error(),
			Errors:  validationDetails(reqErr),
		})
	case errors.As(err, &secErr):
		writeErrorResponse(w, r, http.StatusUnauthorized, errorResponse{Message: err.Error()})
	default:
		writeErrorResponse(w, r, http.StatusInternalServerError, errorResponse{Message: err.Error()})
	}
}

// validationDetails turns a request error into the per-problem list of §10.4. Nested schema
// errors carry the path to the offending field as a list of JSON pointer tokens (for example
// ["data", "2", "target"], rendered as data[2].target); parameter errors name the parameter.
func validationDetails(err *openapi3filter.RequestError) []errorDetail {
	details := make([]errorDetail, 0, 1)
	var schemaErr *openapi3.SchemaError
	if errors.As(err.Err, &schemaErr) {
		details = append(details, errorDetail{
			Path:    renderPath(schemaErr.JSONPointer()),
			Message: schemaErr.Error(),
		})
		return details
	}
	if err.Parameter != nil {
		details = append(details, errorDetail{
			Path:    err.Parameter.Name,
			Message: err.Error(),
		})
		return details
	}
	details = append(details, errorDetail{Message: err.Error()})
	return details
}

// renderPath renders JSON pointer tokens as the path syntax of §10.4: ["data", "2", "target"]
// becomes "data[2].target" and ["message"] becomes "message".
func renderPath(tokens []string) string {
	var b []byte
	for _, token := range tokens {
		switch {
		case token == "":
			continue
		case isArrayIndex(token):
			b = append(b, '[')
			b = append(b, token...)
			b = append(b, ']')
		case len(b) == 0:
			b = append(b, token...)
		default:
			b = append(b, '.')
			b = append(b, token...)
		}
	}
	return string(b)
}

func isArrayIndex(token string) bool {
	if token == "" {
		return false
	}
	for i := 0; i < len(token); i++ {
		if token[i] < '0' || token[i] > '9' {
			return false
		}
	}
	return true
}

// writeErrorResponse writes the datasets error body. It uses the core response writer (headers,
// cancellation handling, JSON encoding) but carries the per-problem list of §10.4, which
// httputil.WriteAPIError cannot express.
func writeErrorResponse(w http.ResponseWriter, r *http.Request, code int, body errorResponse) {
	httputil.WriteAPIResponse(w, r, code, body)
}
