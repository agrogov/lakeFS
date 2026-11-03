package icebergcatalog

// Authentication for the Iceberg REST catalog (ICEBERG_CATALOG_PLAN.md §7.1, and the security
// tests of §12).
//
// A catalog client is a machine: Spark, Trino, PyIceberg, DuckDB or iceberg-go. It has no browser
// and no session cookie, so the catalog accepts these two schemes from it:
//
//   - Authorization: Bearer <token>, a lakeFS JWT - either minted by POST /v1/oauth/tokens
//     (oauth.go) or handed to the client as a static token - verified with auth.VerifyToken
//     (pkg/auth/token.go), i.e. the same secret and the same claims the lakeFS login token uses;
//   - HTTP basic, for clients configured with rest.auth.type=basic (the REST spec's
//     rest.auth.type property), verified through the authenticator the core API's basic_auth
//     scheme uses, so access keys, external principals and any configured Authenticator behave
//     identically here and there.
//
// One more scheme is served, for the *browser* rather than for a catalog client: the lakeFS login
// session cookie (the cookie_auth scheme of api.GenericAuthMiddleware, read here as the same
// internal_auth_session session carrying the same token). The web UI authenticates by cookie, and
// the repository Tables tab explores the catalog's own read endpoints through it, so without the
// cookie a signed-in user's browser could not read the tables it is allowed to read.
//
// The cookie is accepted for GET and HEAD, and - since the Tables tab also manages tables - for
// exactly four state-changing UI operations (cookieUIManagementOperations):
//
//   - a cookie is the one credential a browser attaches by itself, so a write authorised by a
//     cookie is a write any page the user visits could try to issue on their behalf. That is why
//     the management exception is closed rather than general: it names four operations and no
//     others, and it additionally requires the request to identify itself as the web UI with an
//     X-Lakefs-Client header starting with "lakefs-webui".
//   - the header is what makes the exception usable at all. It is a non-simple header, so a
//     browser sends it only after a CORS preflight, and this catalog answers no preflight and
//     grants no cross-origin access: a cross-site page cannot reach a different-origin POST or
//     DELETE with that header at all. The login cookie is SameSite=Lax as well, so it is not
//     attached to a cross-site request in the first place. The header on its own is not a secret
//     and is not the security boundary - the preflight refusal and SameSite are - but it keeps
//     the exception scoped to the UI: a catalog client, a script, or any other same-origin
//     caller that does not claim to be the web UI gets 401 for a cookie write, exactly as before.
//   - every other write stays bearer/basic only, and deliberately so: POST tables/{table} (the
//     commit), register, properties, rename, metrics, remote signing and
//     POST /v1/oauth/tokens are not UI management operations and are never authenticated by a
//     cookie. Those are the operations that change table contents or mint a credential, which is
//     the larger surface a browser-supplied credential must not reach.
//     The six view operations (views_http.go) are in that group as well. Two of them happen to
//     share the shape of a management operation - create a view is POST …/namespaces/{ns}/views
//     and drop a view is DELETE …/namespaces/{ns}/views/{view} - but neither pattern below names
//     "views", so both are refused for a cookie and no view write is reachable from a browser
//     session. That is deliberate: the Tables tab manages tables, a view is not a table, and a
//     cookie-authenticated create or replace would be exactly the browser-issued catalog write
//     this exception exists to keep narrow. A UI that manages views would have to add them here,
//     in a visible change, with the same header requirement.
//   - GET and HEAD stay open to the cookie without a header, because a read cannot change catalog
//     state and the UI's own reads (the namespace tree, the table load) are the reason the cookie
//     scheme exists.
//   - catalog clients never send a cookie, so no client loses an option it had.
//
// Every other interactive scheme (OIDC, SAML) stays out (§7.1): those need a browser flow and a
// cookie verification configuration that a catalog client cannot drive, and a catalog mount has
// no login or callback route to host it.
//
// Two answers are deliberately detail-free (§7.1, and §12 "OAuth with bad creds gets 401 and
// leaks no detail"): a rejected credential is answered 401 with the constant message of
// authFailureMessage, and the token endpoint's invalid_client carries no error_description. The
// cause goes to the server log, never to the client, so a caller cannot learn whether an access
// key exists, whether the secret was close, or which of the schemes matched.
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
	"github.com/gorilla/sessions"
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

// cookieAuthMethods are the methods the login session cookie authenticates without any further
// condition. They are exactly the methods a catalog read uses: GET for the operations themselves
// and HEAD for TableExists and NamespaceExists. Every other method requires a bearer token or HTTP
// basic, except for the four UI management operations of cookieUIManagementOperations (see
// cookieManagementAllowed).
var cookieAuthMethods = map[string]bool{
	http.MethodGet:  true,
	http.MethodHead: true,
}

const (
	// uiClientHeader is the header the web UI identifies itself with (webui/src/lib/api/index.js
	// sends it on every core and catalog request as `lakefs-webui/<build version>`). It is the
	// marker that scopes the cookie-write exception to the UI; see the file comment for why it is
	// not itself the security boundary.
	uiClientHeader = "X-Lakefs-Client" // #nosec G101 -- a header name, not a credential

	// uiClientPrefix is the value uiClientHeader must start with for the cookie to authenticate a
	// management write.
	uiClientPrefix = "lakefs-webui"

	// catalogAPIVersion is the first path segment of every catalog operation, the version prefix
	// the routes of http.go are declared under (/v1/...).
	catalogAPIVersion = "v1"

	// namespacesPathSegment and tablesPathSegment are the two collection segments of the operation
	// patterns in cookieUIManagementOperations.
	namespacesPathSegment = "namespaces"
	tablesPathSegment     = "tables"

	// anyPathSegment is the wildcard of an operation pattern: it matches exactly one segment, the
	// {prefix}, {namespace} or {table} of a route.
	anyPathSegment = "*"
)

// cookieUIManagementOperations is the closed list of state-changing operations the login session
// cookie authenticates, as method plus the path segments below the catalog mount the operation
// matches. It is a list rather than a condition so that the exception is auditable at a glance and
// so that adding an operation is a visible change here:
//
//	POST   /v1/{prefix}/namespaces                            create a namespace
//	DELETE /v1/{prefix}/namespaces/{namespace}                drop an empty namespace
//	POST   /v1/{prefix}/namespaces/{namespace}/tables          create a table
//	DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}  drop a table
//
// Everything absent from this list is not a management operation the UI may make with a cookie:
// the commit (POST .../tables/{table}), register (POST .../register), properties
// (POST .../properties), metrics (POST .../metrics), remote signing (POST .../sign), the token
// endpoint (POST /v1/oauth/tokens) and every read (GET/HEAD, which need no entry here). The list
// matches on shape and not on names: the {namespace} segment is one segment however many levels it
// joins with the separator, and its value is irrelevant to the decision - what the caller may do
// with it is the Authorizer's question, not this one's.
var cookieUIManagementOperations = []struct {
	method  string
	pattern []string
}{
	{method: http.MethodPost, pattern: []string{catalogAPIVersion, anyPathSegment, namespacesPathSegment}},
	{method: http.MethodDelete, pattern: []string{catalogAPIVersion, anyPathSegment, namespacesPathSegment, anyPathSegment}},
	{method: http.MethodPost, pattern: []string{catalogAPIVersion, anyPathSegment, namespacesPathSegment, anyPathSegment, tablesPathSegment}},
	{method: http.MethodDelete, pattern: []string{catalogAPIVersion, anyPathSegment, namespacesPathSegment, anyPathSegment, tablesPathSegment, anyPathSegment}},
}

// catalogAuth is the catalog's authentication: it turns the credentials of a request into the
// lakeFS user behind them, and it is the only place that mints a token (§7.1).
type catalogAuth struct {
	logger        logging.Logger
	authService   auth.Service
	authenticator auth.Authenticator
	cfg           *Config
	// sessionStore reads the browser's login session, with the same key and the same session
	// name api.GenericAuthMiddleware uses for the core API's cookie_auth scheme: the cookie the
	// UI is signed in with is the cookie this middleware verifies.
	sessionStore sessions.Store
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
// Wiring: newAuthMiddleware (http.go) is the single call site, and it builds this middleware
// rather than api.GenericAuthMiddleware. That is what makes POST /v1/oauth/tokens reachable at
// <server>/iceberg/api/v1/oauth/tokens and at the relative_to mount; a catalog mounted with the
// generic core middleware answers the token endpoint with 404. This file owns no route: the
// token endpoint is served from the middleware chain itself, so it cannot be advertised in
// GET /v1/config and cannot be mounted twice.
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
		sessionStore:  sessions.NewCookieStore(deps.AuthService.SecretStore().SharedSecret()),
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
//
// The order is bearer, then basic, then cookie: a client that sends an Authorization header is
// never authenticated by the cookie it happens to carry, and the cookie is consulted only when the
// request carries no other credential.
func (a *catalogAuth) authenticateRequest(r *http.Request) (*model.User, error) {
	if token, ok := bearerToken(r); ok {
		return a.userFromToken(r.Context(), token)
	}
	if accessKey, secretKey, ok := r.BasicAuth(); ok {
		return a.userFromCredentials(r.Context(), accessKey, secretKey)
	}
	if cookieAuthMethods[r.Method] || cookieManagementAllowed(r) {
		return a.userFromCookie(r)
	}
	return nil, nil //nolint:nilnil // "no credentials" is not an error: requireUser answers it.
}

// cookieManagementAllowed reports whether the login session cookie may authenticate this request
// as one of the Tables tab's management operations.
//
// Two conditions, both required:
//
//   - the request identifies itself as the web UI, with an X-Lakefs-Client header that starts with
//     "lakefs-webui". This is what keeps the exception scoped to the UI rather than to "a POST or a
//     DELETE"; the CORS preflight a browser must send before it will attach this header to a
//     cross-origin request is refused by the catalog (it grants no cross-origin access), and the
//     login cookie is SameSite=Lax, so a cross-site page reaches neither.
//   - the method and the path are exactly one of the four operations of
//     cookieUIManagementOperations - create namespace, drop namespace, create table, drop table -
//     and not merely a POST or a DELETE somewhere below the catalog.
//
// A request that fails either condition has no cookie credential, exactly as before this exception
// existed: requireUser answers it with the constant 401, and a bearer token or an access key pair
// still authenticate the same request (they are read first in authenticateRequest).
func cookieManagementAllowed(r *http.Request) bool {
	if !strings.HasPrefix(r.Header.Get(uiClientHeader), uiClientPrefix) {
		return false
	}
	segments := requestPathSegments(r)
	for _, operation := range cookieUIManagementOperations {
		if operation.method == r.Method && matchPathSegments(operation.pattern, segments) {
			return true
		}
	}
	return false
}

// matchPathSegments reports whether segments has exactly the length of pattern and agrees with it
// on every segment that is not the anyPathSegment wildcard.
func matchPathSegments(pattern, segments []string) bool {
	if len(pattern) != len(segments) {
		return false
	}
	for i, want := range pattern {
		if want != anyPathSegment && want != segments[i] {
			return false
		}
	}
	return true
}

// requestPathSegments is the path of the request relative to the catalog mount (requestPath),
// split on "/" with the leading and trailing separators removed. Segment counts are what the shape
// match of cookieManagementAllowed compares, so an empty path is no segments at all rather than one
// empty one.
func requestPathSegments(r *http.Request) []string {
	path := strings.Trim(requestPath(r), "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

// userFromCookie resolves the lakeFS login session cookie the web UI is signed in with. It is the
// same session and the same token key the core API's cookie_auth scheme reads
// (api.GenericAuthMiddleware, pkg/api/auth_middleware.go), verified the same way - through
// auth.UserByToken, so the signature, the expiry, the audience and the existence of the user are
// all checked exactly as they are on a core route.
//
// The caller has already decided that this request is one the cookie may authenticate
// (authenticateRequest): a GET or a HEAD, or one of the four UI management operations with the
// X-Lakefs-Client header of the web UI.
//
// A request without the cookie, or with a session that carries no token, has no cookie
// credential: (nil, nil), which requireUser answers with 401 - the same answer an anonymous
// request gets. The web UI does not turn that answer into a login redirect: the repository Tables
// tab reports it in place, with its own "sign in again" state and a plain link
// (webui/src/lib/api/iceberg.ts), because the catalog's 401 is about this mount's credential, not
// about the reader's core lakeFS session. A session whose token does not verify is a rejected
// credential (an error), not an absent one, and is logged as such.
func (a *catalogAuth) userFromCookie(r *http.Request) (*model.User, error) {
	// The error is deliberately ignored, as api.GenericAuthMiddleware's cookie_auth scheme ignores
	// it: gorilla/sessions reports a missing or undecodable cookie as an error while still
	// returning a session to read, so the token - not the error - decides whether there is a
	// cookie credential.
	session, _ := a.sessionStore.Get(r, auth.InternalAuthSessionName)
	token := ""
	if session != nil {
		token, _ = session.Values[auth.TokenSessionKeyName].(string)
	}
	if token == "" {
		return nil, nil //nolint:nilnil // no cookie credential: requireUser answers it.
	}
	user, err := auth.UserByToken(r.Context(), a.authService, token)
	if err != nil {
		return nil, fmt.Errorf("%w: session cookie: %w", auth.ErrAuthenticatingRequest, err)
	}
	return user, nil
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
