package icebergcatalog

// Tests for the authentication of ICEBERG_CATALOG_PLAN.md §7.1 and the security tests of §12:
// every route accepts a lakeFS bearer token or HTTP basic, and a rejected credential is answered
// 401 without detail. The token endpoint those credentials can be exchanged at is in oauth_test.go.

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
)

// authTestRouter is the production stack of http.go - newRouter over newServer, with the
// authentication middleware newAuthMiddleware builds - mounted the way RoutesBuilder mounts it, but
// over a single-mount mux so a test can address a route without an HTTP server. It exercises the
// real middleware, the real authenticated-user requirement, the real error envelope and the real
// mount-relative paths, not just the middleware in isolation.
//
// Mounting matters: chi publishes the mount-relative path in its route context and keeps the full
// path in r.URL.Path, so a router served at the root would not prove that a request is reached at
// the URL a client sends (<server>/iceberg/api/v1/...).
func authTestRouter(t *testing.T, cfg *Config) (http.Handler, cmdpkg.ServerDeps) {
	t.Helper()
	deps := newServerDeps(t)
	middleware, err := NewAuthMiddleware(deps, cfg)
	require.NoError(t, err)
	router, err := newRouter(middleware, cfg, newServer(cfg))
	require.NoError(t, err)
	mux := coreRouter()
	mux.Mount(APIBasePath, router)
	return mux, deps
}

// catalogPath is the path of a catalog operation as a client addresses it.
func catalogPath(pattern string) string { return APIBasePath + pattern }

// bearerRequest sends a request with the given bearer token (empty for none).
func bearerRequest(t *testing.T, handler http.Handler, method, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", bearerScheme+" "+token)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// requireUnauthorized asserts the detail-free 401 of §7.1 and §12: the catalog envelope, the
// constant message, and none of the caller's credentials in the body.
func requireUnauthorized(t *testing.T, rec *httptest.ResponseRecorder, secrets ...string) {
	t.Helper()
	apiError := requireEnvelope(t, rec, http.StatusUnauthorized, ErrTypeNotAuthorized)
	require.Equal(t, authFailureMessage(http.StatusUnauthorized), apiError.Message, "the answer names no reason")
	for _, secret := range secrets {
		require.NotContains(t, rec.Body.String(), secret, "the answer must not echo the credential")
	}
}

// TestAuthAcceptsBearerAndBasicOnEveryRoute is the other half of §7.1: outside the token endpoint,
// a catalog request carries the minted (or a static) bearer token, or, for rest.auth.type=basic
// clients, its access key pair as HTTP basic.
func TestAuthAcceptsBearerAndBasicOnEveryRoute(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	router, deps := authTestRouter(t, cfg)

	t.Run("bearer token", func(t *testing.T) {
		rec := bearerRequest(t, router, http.MethodGet, catalogPath(configPath), mintToken(t, router))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("a static lakeFS login token", func(t *testing.T) {
		// A client may be handed a token instead of exchanging its access key; it is the same
		// token type, so it authenticates the same way (Q-I6).
		token, err := auth.GenerateJWTLogin(deps.AuthService.SecretStore().SharedSecret(),
			testUsername, time.Now(), time.Now().Add(time.Hour))
		require.NoError(t, err)
		rec := bearerRequest(t, router, http.MethodGet, catalogPath(configPath), token)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("basic credentials", func(t *testing.T) {
		rec := do(t, router, http.MethodGet, catalogPath(configPath), testAccessKeyID, testSecretAccessKey)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("the scheme is case-insensitive", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, catalogPath(configPath), nil)
		req.Header.Set("Authorization", "bEaReR "+mintToken(t, router))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})
}

// TestAuthRejectsWrongCredentials: every way a catalog client can be wrong is answered 401 with
// no detail (§7.1, §12) - a token that does not verify, a token of a user that no longer exists, a
// basic pair that does not verify, and a scheme the catalog does not serve.
func TestAuthRejectsWrongCredentials(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	router, deps := authTestRouter(t, cfg)
	secret := deps.AuthService.SecretStore().SharedSecret()

	expired, err := auth.GenerateJWTLogin(secret, testUsername, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	foreignKey, err := auth.GenerateJWTLogin([]byte("a-different-secret"), testUsername, time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, err)
	deletedUser, err := auth.GenerateJWTLogin(secret, "user-that-does-not-exist", time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, err)

	// A signed token whose audience is not the login audience: the catalog accepts lakeFS login
	// tokens only, which is the rule auth.VerifyToken applies to every other route as well.
	otherAudience := auth.LoginClaims{
		Issuer:    "auth",
		Subject:   testUsername,
		Audience:  "some-other-service",
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	otherAudienceToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, otherAudience).SignedString(secret)
	require.NoError(t, err)

	for name, token := range map[string]string{
		"a token that is not a JWT":       "not-a-jwt",
		"a token signed with another key": foreignKey,
		"an expired token":                expired,
		"a token of a deleted user":       deletedUser,
		"a token for another audience":    otherAudienceToken,
	} {
		t.Run(name, func(t *testing.T) {
			requireUnauthorized(t, bearerRequest(t, router, http.MethodGet, catalogPath(configPath), token))
		})
	}

	t.Run("wrong basic secret", func(t *testing.T) {
		rec := do(t, router, http.MethodGet, catalogPath(configPath), testAccessKeyID, "not-the-secret")
		requireUnauthorized(t, rec, testAccessKeyID, "not-the-secret")
	})

	t.Run("unknown access key", func(t *testing.T) {
		rec := do(t, router, http.MethodGet, catalogPath(configPath), "AKIAUNKNOWNKEY0001", testSecretAccessKey)
		requireUnauthorized(t, rec, "AKIAUNKNOWNKEY0001")
	})

	t.Run("a scheme the catalog does not serve", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, catalogPath(configPath), nil)
		req.Header.Set("Authorization", "Digest username=\"iceberg\"")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		requireUnauthorized(t, rec)
	})
}

// TestAuthLeavesAnonymousRequestsToRequireUser pins the division of labour the catalog inherited
// from http.go: the middleware authenticates, requireUser requires. A request with no credential
// is answered 401 by requireUser, with the same constant message, so a caller cannot tell "no
// credential" from "wrong credential".
func TestAuthLeavesAnonymousRequestsToRequireUser(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	router, _ := authTestRouter(t, cfg)

	rec := bearerRequest(t, router, http.MethodGet, catalogPath(configPath), "")
	requireUnauthorized(t, rec)
	require.Equal(t, ErrAuthenticationRequired.Error(), authFailureMessage(http.StatusUnauthorized),
		"an anonymous request and a rejected credential are answered identically")
}

// TestAuthMiddlewareNeedsDeps: a catalog cannot be served without a way to verify a credential, so
// building the middleware fails rather than mounting one that refuses every request.
func TestAuthMiddlewareNeedsDeps(t *testing.T) {
	deps := cmdpkg.ServerDeps{}
	_, err := NewAuthMiddleware(deps, &Config{})
	require.ErrorIs(t, err, ErrMissingDeps)

	middleware, err := NewAuthMiddleware(cmdpkg.ServerDeps{}, nil)
	require.ErrorIs(t, err, ErrMissingDeps)
	require.Nil(t, middleware)
}
