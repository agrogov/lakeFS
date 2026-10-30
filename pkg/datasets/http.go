package datasets

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/httputil"
)

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

	r.Mount(APIBasePath, NewRouter(authMiddleware))
	return nil
}

// NewRouter builds the datasets sub-router wrapped in authMiddleware. The sub-router is empty
// until the control-plane handlers land, so every request to it currently ends in chi's 404
// from inside the mounted handler - never in the core invalid-endpoint catch-all.
//
// authMiddleware is a parameter (rather than built here) so that the mount can be exercised
// without a full auth service.
func NewRouter(authMiddleware func(http.Handler) http.Handler) http.Handler {
	router := chi.NewRouter()
	if authMiddleware == nil {
		return router
	}
	return authMiddleware(router)
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
