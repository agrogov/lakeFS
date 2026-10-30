package cmd

import (
	"context"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

// AuthServiceBuilder builds the auth.Service used by the server.
type AuthServiceBuilder func(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *auth.KVMetadataManager) auth.Service

// AuthenticationServiceBuilder builds the authentication.Service used by the server.
type AuthenticationServiceBuilder func(ctx context.Context, cfg config.Config, logger logging.Logger) (authentication.Service, error)

// authServiceBuilder defaults to the upstream auth.NewAuthService factory.
var authServiceBuilder AuthServiceBuilder = auth.NewAuthService

// authenticationServiceBuilder defaults to the upstream authentication.NewAuthenticationService factory.
var authenticationServiceBuilder AuthenticationServiceBuilder = authentication.NewAuthenticationService

// SetAuthServiceBuilder overrides the auth.Service factory. Call before Execute().
func SetAuthServiceBuilder(b AuthServiceBuilder) { authServiceBuilder = b }

// SetAuthenticationServiceBuilder overrides the authentication.Service factory. Call before Execute().
func SetAuthenticationServiceBuilder(b AuthenticationServiceBuilder) {
	authenticationServiceBuilder = b
}

// ServerDeps exposes server internals to out-of-tree route builders.
type ServerDeps struct {
	Config        config.Config
	Catalog       *catalog.Catalog
	AuthService   auth.Service
	Authenticator auth.Authenticator
	BlockAdapter  block.Adapter
	KVStore       kv.Store
	Logger        logging.Logger
}

// ExtraRoutesBuilder mounts additional HTTP routes on the API router. It may start
// background goroutines bound to ctx.
type ExtraRoutesBuilder func(ctx context.Context, deps ServerDeps, r *chi.Mux) error

var extraRoutesBuilders []ExtraRoutesBuilder

// AddExtraRoutesBuilder registers an additional route builder. Call before Execute().
func AddExtraRoutesBuilder(b ExtraRoutesBuilder) {
	extraRoutesBuilders = append(extraRoutesBuilders, b)
}
