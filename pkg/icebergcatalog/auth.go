package icebergcatalog

// Authentication for the Iceberg REST catalog (ICEBERG_CATALOG_PLAN.md §7.1, and the security
// tests of §12).
//
// A catalog client is a machine: Spark, Trino, PyIceberg, DuckDB or iceberg-go. It has no browser
// and no session cookie, so the catalog accepts exactly the two schemes such a client can send:
//
//   - Authorization: Bearer <token>, a lakeFS JWT - either minted by POST /v1/oauth/tokens
//     (oauth.go) or handed to the client as a static token - verified with auth.VerifyToken
//     (pkg/auth/token.go), i.e. the same secret and the same claims the lakeFS login token uses;
//   - HTTP basic, for clients configured with rest.auth.type=basic (the REST spec's
//     rest.auth.type property), verified through the authenticator the core API's basic_auth
//     scheme uses, so access keys, external principals and any configured Authenticator behave
//     identically here and there.
//
// There is no browser flow (§7.1): an SSO user reaches the catalog with an access key. Every
// interactive scheme (cookie session, OIDC, SAML) needs a browser that a catalog client does not
// have, and accepting one here would authenticate the server's user rather than the client's.
//
// Two answers are deliberately detail-free (§7.1, and §12 "OAuth with bad creds gets 401 and
// leaks no detail"): a rejected credential is answered 401 with the constant message of
// authFailureMessage, and the token endpoint's invalid_client carries no error_description. The
// cause goes to the server log, never to the client, so a caller cannot learn whether an access
// key exists, whether the secret was close, or which of the two schemes matched.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml), RFC 6749 and
// the public lakeFS documentation only. No code from lakeFS Enterprise or from upstream lakeFS
// >= 1.87 was used.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// OAuthTokenPath is the token endpoint of §7.1, relative to the catalog mount
	// (APIBasePath). A client pointed at <server>/iceberg/api therefore posts to
	// <server>/iceberg/api/v1/oauth/tokens - the URL the lakeFS Iceberg documentation gives, and
	// the {catalog}/v1/oauth/tokens fallback the clients build when oauth2-server-uri is unset
	// (Iceberg REST spec, /v1/oauth/tokens).
	OAuthTokenPath = "/v1/oauth/tokens" // #nosec G101 -- a URL path, not a credential

	// bearerScheme is the authorization scheme of the token this catalog mints and accepts
	// (§7.1). Matching is case-insensitive, as RFC 7235 §2.1 requires of auth schemes.
	bearerScheme = "Bearer"
)

// catalogAuth is the catalog's authentication: it turns the credentials of a request into the
// lakeFS user behind them, and it is the only place that mints a token (§7.1).
type catalogAuth struct {
	logger        logging.Logger
	authService   auth.Service
	authenticator auth.Authenticator
	cfg           *Config
	// now is the clock the token's issue and expiry times are taken from. It is a field so that a
	// test can pin it and assert the iat/exp claims exactly.
	now func() time.Time
}

// NewAuthMiddleware builds the catalog's authentication middleware from the server dependencies,
// the way pkg/datasets/http.go builds its own: the same deps fields, the same
// (func(http.Handler) http.Handler) shape the router takes, and a startup error rather than a
// half-working catalog.
//
// The returned middleware is a drop-in replacement for the generic core middleware
// (api.GenericAuthMiddleware) that http.go's newAuthMiddleware builds today, and it differs from
// it in the two ways §7.1 requires:
//
//   - it serves POST /v1/oauth/tokens itself, so the token endpoint answers an unauthenticated
//     request (that is what makes it a way *in*) while every other route still needs a principal;
//   - its accepted schemes are the two a catalog client can send, and their verification is
//     auth.VerifyToken for a bearer token and the core authenticator for HTTP basic, rather than
//     whatever the core swagger's security requirements happen to list.
//
// It authenticates but does not require a principal: an anonymous request is left to requireUser
// (http.go), which is what answers it - the same division of labour api.GenericAuthMiddleware and
// requireUser already have, so the catalog has one owner for "401 because there is no user".
//
// Wiring note: newAuthMiddleware (http.go) is the single call site, and passing NewAuthMiddleware
// there instead of api.GenericAuthMiddleware is the whole change. This file owns no route and
// changes nothing about the stack http.go mounts, so until that one line is swapped the catalog
// keeps the core middleware's scheme list and does not answer POST /v1/oauth/tokens; the tests
// below build that swapped stack directly (authTestRouter), which is why they cover the endpoint
// as a client addresses it.
func NewAuthMiddleware(deps cmdpkg.ServerDeps, cfg *Config) (func(http.Handler) http.Handler, error) {
	a, err := newCatalogAuth(deps, cfg)
	if err != nil {
		return nil, err
	}
	return a.middleware, nil
}

// newCatalogAuth validates the dependencies the middleware cannot work without.
func newCatalogAuth(deps cmdpkg.ServerDeps, cfg *Config) (*catalogAuth, error) {
	if deps.AuthService == nil || deps.Authenticator == nil {
		return nil, ErrMissingDeps
	}
	if cfg == nil {
		cfg = &Config{}
	}
	logger := deps.Logger
	if logger == nil {
		logger = logging.ContextUnavailable()
	}
	return &catalogAuth{
		logger:        logger,
		authService:   deps.AuthService,
		authenticator: deps.Authenticator,
		cfg:           cfg,
		now:           time.Now,
	}, nil
}

// middleware authenticates one request and puts the user on its context (auth.WithUser), which is
// what requireUser and the authorization layer (authz.go) read.
//
// The token endpoint is served here rather than from a route entry, because authentication is
// what makes it different from every other operation: it must answer a request that carries no
// credential at all. Serving it first in the chain keeps that exception in one place, and keeps
// it out of the advertised endpoint list - a client does not discover the token endpoint from
// GET /v1/config; it is configured with the URL (or the {catalog}/v1/oauth/tokens fallback) and
// asks for a token before it ever reads the config.
//
// Every other request goes through authenticateRequest and then on to the router, which means an
// anonymous caller still reaches requireUser and is answered there with the catalog's 401.
func (a *catalogAuth) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && requestPath(r) == OAuthTokenPath {
			// The token endpoint answers in the OAuth error model, not in the generic envelope
			// the authentication layer's rejections are rewritten into (errors.go), and its own
			// 401 is a legitimate answer rather than a middleware rejection.
			markDownstream(w)
			a.handleToken(w, r)
			return
		}

		user, err := a.authenticateRequest(r)
		if err != nil {
			// §7.1, §12: 401, and the reason stays in the log. authErrorWriter (errors.go) would
			// replace this body anyway; writing the envelope here keeps the answer the same
			// whether or not the middleware is wrapped by it.
			a.logger.WithContext(r.Context()).WithError(err).Warn("iceberg catalog: authentication failed")
			writeIcebergError(w, http.StatusUnauthorized, ErrTypeNotAuthorized,
				authFailureMessage(http.StatusUnauthorized))
			return
		}
		if user != nil {
			ctx := logging.AddFields(r.Context(), logging.Fields{logging.UserFieldKey: user.Username})
			r = r.WithContext(auth.WithUser(ctx, user))
		}
		next.ServeHTTP(w, r)
	})
}

// authenticateRequest resolves the credentials of a request. It returns (nil, nil) for a request
// that carries none, which requireUser answers, and an error for a request whose credentials are
// present but wrong.
func (a *catalogAuth) authenticateRequest(r *http.Request) (*model.User, error) {
	if token, ok := bearerToken(r); ok {
		return a.userFromToken(r.Context(), token)
	}
	if accessKey, secretKey, ok := r.BasicAuth(); ok {
		return a.userFromCredentials(r.Context(), accessKey, secretKey)
	}
	return nil, nil //nolint:nilnil // "no credentials" is not an error: requireUser answers it.
}

// requestPath is the path of the request relative to the catalog mount, which is the path the
// router routes on.
//
// The catalog is mounted (RoutesBuilder, http.go), and chi's Mount keeps the full request path in
// r.URL.Path while publishing the mount-relative one - the path the sub-router actually matches
// against - in its route context. Matching an operation on r.URL.Path would therefore fail for
// every mounted catalog and only work for a router served at the root, which is what a test does.
// Both forms are read here: the route context when the catalog is mounted, r.URL.Path when it is
// not.
func requestPath(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil && rctx.RoutePath != "" {
		return rctx.RoutePath
	}
	return r.URL.Path
}

// bearerToken returns the token of an "Authorization: Bearer <token>" header. Field splitting
// follows the core middleware (pkg/api/auth_middleware.go) so that a header this catalog rejects
// is exactly one the core API rejects.
func bearerToken(r *http.Request) (string, bool) {
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], bearerScheme) {
		return "", false
	}
	return parts[1], true
}

// userFromToken verifies a lakeFS JWT with auth.VerifyToken and resolves its subject to a user.
//
// VerifyToken is what the core API's jwt_token scheme uses: it checks the signature with the auth
// service's shared secret, requires an expiry, accepts only the HMAC family, and accepts only the
// login audience (pkg/auth/token.go). A token minted by oauth.go passes all four, and so does a
// login token a client was given directly - which is why a static token works as well as an
// exchanged one (Q-I6: there is no separate catalog token type).
//
// Resolving the subject is not optional: a signature proves the token was issued by this server,
// not that the user still exists, so a token for a deleted user is refused here exactly as
// auth.UserByToken refuses one on a core route.
func (a *catalogAuth) userFromToken(ctx context.Context, token string) (*model.User, error) {
	claims, err := auth.VerifyToken(a.secret(), token)
	if err != nil {
		return nil, fmt.Errorf("%w: verify token: %w", auth.ErrAuthenticatingRequest, err)
	}
	user, err := a.authService.GetUser(ctx, claims.Subject)
	if err != nil {
		return nil, fmt.Errorf("%w: subject %q of token %q: %w",
			auth.ErrAuthenticatingRequest, claims.Subject, claims.ID, err)
	}
	return user, nil
}

// userFromCredentials verifies an access key pair the way the core API's basic_auth scheme does:
// the configured authenticator checks the secret (built-in access keys when the installation
// uses them, or an external one when it does not), and the auth service resolves the username it
// returns to a user.
//
// It is shared by the two ways a client can present that pair: HTTP basic on any route
// (rest.auth.type=basic), and client_id/client_secret on POST /v1/oauth/tokens (oauth.go). Both
// are the same credential, so they are verified by the same code and answer the same way.
func (a *catalogAuth) userFromCredentials(ctx context.Context, accessKey, secretKey string) (*model.User, error) {
	username, err := a.authenticator.AuthenticateUser(ctx, accessKey, secretKey)
	if err != nil {
		return nil, fmt.Errorf("%w: access key %q: %w", auth.ErrAuthenticatingRequest, accessKey, err)
	}
	user, err := a.authService.GetUser(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("%w: user %q: %w", auth.ErrAuthenticatingRequest, username, err)
	}
	return user, nil
}

// secret is the auth service's shared secret: the key tokens are signed with and the key
// auth.VerifyToken checks them against.
func (a *catalogAuth) secret() []byte {
	return a.authService.SecretStore().SharedSecret()
}

// markDownstream marks w as past the authentication layer, so an answer this middleware writes
// itself - the token endpoint's - is not rewritten by authErrorWriter (errors.go). That wrapper
// exists to replace the *authentication layer's* rejections with the catalog envelope; the token
// endpoint is the layer's own handler, and its 401 is an OAuth error a client parses, not a
// middleware rejection.
func markDownstream(w http.ResponseWriter) {
	if writer, ok := w.(*authErrorWriter); ok {
		writer.downstream = true
	}
}
