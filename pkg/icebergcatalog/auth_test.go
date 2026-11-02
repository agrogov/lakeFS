package icebergcatalog

// Tests for the authentication of ICEBERG_CATALOG_PLAN.md §7.1 and the security tests of §12:
// POST /v1/oauth/tokens mints a lakeFS JWT from an access key pair, every other route accepts a
// bearer token or HTTP basic, and a rejected credential is answered 401 without detail.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
)

// authTestRouter is the production stack of http.go - newRouter over newServer - with this task's
// authentication middleware in place of the generic core one, mounted the way RoutesBuilder
// mounts it. That is exactly the swap http.go's newAuthMiddleware documents, so the tests below
// exercise the real router, the real authenticated-user requirement, the real error envelope and
// the real mount-relative paths, not just the middleware.
//
// Mounting matters: chi publishes the mount-relative path in its route context and keeps the full
// path in r.URL.Path, so a router served at the root would not prove that the token endpoint is
// reached at the URL a client posts to (<server>/iceberg/api/v1/oauth/tokens).
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

// postToken posts a form-encoded token request, the way every Iceberg client does
// (RFC 6749 §4.4.2, AuthStyleInParams).
func postToken(t *testing.T, handler http.Handler, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, catalogPath(OAuthTokenPath), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// mintToken performs the exchange with the test access key and returns the token.
func mintToken(t *testing.T, handler http.Handler) string {
	t.Helper()
	rec := postToken(t, handler, url.Values{
		"grant_type":    {grantTypeClientCredentials},
		"client_id":     {testAccessKeyID},
		"client_secret": {testSecretAccessKey},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response OAuthTokenResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.NotEmpty(t, response.AccessToken)
	return response.AccessToken
}

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

// TestOAuthTokenExchange is the client_credentials grant of §7.1: an access key pair is verified
// against the auth service and exchanged for a lakeFS JWT.
func TestOAuthTokenExchange(t *testing.T) {
	const ttl = 15 * time.Minute
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: ttl, MaxRequestBody: DefaultMaxRequestBody}
	router, deps := authTestRouter(t, cfg)

	t.Run("a valid access key pair mints a lakeFS JWT", func(t *testing.T) {
		rec := postToken(t, router, url.Values{
			"grant_type":    {grantTypeClientCredentials},
			"client_id":     {testAccessKeyID},
			"client_secret": {testSecretAccessKey},
			"scope":         {"catalog"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, contentTypeJSON, rec.Header().Get("Content-Type"))
		// RFC 6749 §5.1: a token response must not be stored.
		require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
		require.Equal(t, "no-cache", rec.Header().Get("Pragma"))

		var response OAuthTokenResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.NotEmpty(t, response.AccessToken)
		require.Equal(t, tokenTypeBearer, response.TokenType)
		require.Equal(t, IssuedTokenType, response.IssuedTokenType)
		require.Equal(t, int64(ttl/time.Second), response.ExpiresIn, "expires_in is iceberg.token_ttl in seconds")

		// Q-I6: the token is a lakeFS login JWT - the same token the core login issues and the
		// same one auth.VerifyToken checks on every other route. There is no refresh flow, so the
		// response carries no refresh_token.
		require.NotContains(t, rec.Body.String(), "refresh_token")
		claims, err := auth.VerifyToken(deps.AuthService.SecretStore().SharedSecret(), response.AccessToken)
		require.NoError(t, err)
		require.Equal(t, testUsername, claims.Subject, "the subject is the user the access key belongs to")
		require.Equal(t, auth.LoginAudience, claims.Audience)
		require.WithinDuration(t, time.Now().Add(ttl), claims.ExpiresAt.Time, time.Minute)
	})

	t.Run("the token's lifetime is iceberg.token_ttl from the server's clock", func(t *testing.T) {
		// The clock is pinned, so the two claims are asserted exactly rather than within a
		// tolerance: exp - iat is the configured lifetime, and nothing else.
		issuedAt := time.Now().Truncate(time.Second)
		pinned, err := newCatalogAuth(deps, &Config{TokenTTL: ttl})
		require.NoError(t, err)
		pinned.now = func() time.Time { return issuedAt }

		req := httptest.NewRequest(http.MethodPost, OAuthTokenPath, strings.NewReader(url.Values{
			"grant_type":    {grantTypeClientCredentials},
			"client_id":     {testAccessKeyID},
			"client_secret": {testSecretAccessKey},
		}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		pinned.handleToken(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response OAuthTokenResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, int64(ttl/time.Second), response.ExpiresIn)
		claims, err := auth.VerifyToken(deps.AuthService.SecretStore().SharedSecret(), response.AccessToken)
		require.NoError(t, err)
		require.Equal(t, issuedAt.Unix(), claims.IssuedAt.Unix())
		require.Equal(t, issuedAt.Add(ttl).Unix(), claims.ExpiresAt.Unix())
	})

	t.Run("the minted token authenticates every other route", func(t *testing.T) {
		rec := bearerRequest(t, router, http.MethodGet, catalogPath(configPath), mintToken(t, router))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, map[string]string{"prefix": CatalogPrefix}, response.Overrides)
	})

	t.Run("the credential may also come from the Authorization header", func(t *testing.T) {
		// RFC 6749 §2.3.1: a client may send client_id/client_secret as HTTP basic instead of in
		// the form.
		req := httptest.NewRequest(http.MethodPost, catalogPath(OAuthTokenPath),
			strings.NewReader(url.Values{"grant_type": {grantTypeClientCredentials}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth(testAccessKeyID, testSecretAccessKey)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("an unfamiliar scope is not a reason to refuse", func(t *testing.T) {
		// The catalog authorizes per repository, namespace and table (authz.go), not per token
		// scope, so a scope it does not know cannot grant or deny anything.
		rec := postToken(t, router, url.Values{
			"grant_type":    {grantTypeClientCredentials},
			"client_id":     {testAccessKeyID},
			"client_secret": {testSecretAccessKey},
			"scope":         {"something-else"},
			"audience":      {"another-service"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("the token endpoint is not advertised as a catalog operation", func(t *testing.T) {
		// It is reached before GET /v1/config, not discovered from it: a client is configured with
		// the URL and asks for a token first.
		require.NotContains(t, newServer(&Config{}).endpoints, "POST /v1/{prefix}/oauth/tokens")
	})

	t.Run("only POST is served", func(t *testing.T) {
		// The documented method is the only one: anything else falls through to the route table,
		// which does not have the path, and is answered 404 once the caller is authenticated.
		req := httptest.NewRequest(http.MethodGet, catalogPath(OAuthTokenPath), nil)
		req.SetBasicAuth(testAccessKeyID, testSecretAccessKey)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNotFound)
	})
}

// TestOAuthTokenEndpointRejectsBadCredentials is the §12 security test: "OAuth with bad creds gets
// 401 and leaks no detail".
func TestOAuthTokenEndpointRejectsBadCredentials(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	router, _ := authTestRouter(t, cfg)

	for name, form := range map[string]url.Values{
		"unknown access key": {
			"grant_type":    {grantTypeClientCredentials},
			"client_id":     {"AKIAUNKNOWNKEY0001"},
			"client_secret": {testSecretAccessKey},
		},
		"wrong secret": {
			"grant_type":    {grantTypeClientCredentials},
			"client_id":     {testAccessKeyID},
			"client_secret": {"not-the-secret"},
		},
		"empty secret": {
			"grant_type": {grantTypeClientCredentials},
			"client_id":  {testAccessKeyID},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := postToken(t, router, form)
			require.Equal(t, http.StatusUnauthorized, rec.Code, "body: %s", rec.Body.String())
			require.Equal(t, contentTypeJSON, rec.Header().Get("Content-Type"))

			var response OAuthErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, oauthErrorInvalidClient, response.Error)
			require.Empty(t, response.ErrorDescription, "a rejected credential gets no description")

			// Nothing about the credential, and nothing about *why* it failed, reaches the client.
			body := rec.Body.String()
			for _, leak := range []string{
				testAccessKeyID, testSecretAccessKey, "AKIAUNKNOWNKEY0001", "not-the-secret",
				"invalid access", "does not exist", "no such", "not found", "secret",
			} {
				require.NotContains(t, body, leak)
			}
		})
	}
}

// TestOAuthTokenRequestErrors covers the requests the token endpoint refuses because they are not
// client_credentials requests at all. None of them looks at a credential, so each can say what is
// wrong with the request.
func TestOAuthTokenRequestErrors(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: 128}
	router, _ := authTestRouter(t, cfg)

	for name, tc := range map[string]struct {
		form       url.Values
		wantStatus int
		wantCode   string
	}{
		"no grant_type": {
			form:       url.Values{"client_id": {testAccessKeyID}, "client_secret": {testSecretAccessKey}},
			wantStatus: http.StatusBadRequest,
			wantCode:   oauthErrorInvalidRequest,
		},
		"another grant_type": {
			form:       url.Values{"grant_type": {"password"}, "client_id": {testAccessKeyID}},
			wantStatus: http.StatusBadRequest,
			wantCode:   oauthErrorUnsupportedGrantType,
		},
		"no client_id": {
			form:       url.Values{"grant_type": {grantTypeClientCredentials}},
			wantStatus: http.StatusBadRequest,
			wantCode:   oauthErrorInvalidRequest,
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := postToken(t, router, tc.form)
			require.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
			var response OAuthErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, tc.wantCode, response.Error)
			require.NotEmpty(t, response.ErrorDescription, "a malformed request is not a credential failure")
		})
	}

	t.Run("a body that is not a form", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, catalogPath(OAuthTokenPath), strings.NewReader("%zz=1"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
		var response OAuthErrorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, oauthErrorInvalidRequest, response.Error)
	})

	t.Run("an oversized body is 413", func(t *testing.T) {
		// The token endpoint runs before the router's body limit, so it applies the same bound of
		// iceberg.max_request_body itself (§12).
		rec := postToken(t, router, url.Values{
			"grant_type":    {grantTypeClientCredentials},
			"client_id":     {testAccessKeyID},
			"client_secret": {strings.Repeat("s", 4*int(cfg.MaxRequestBody))},
		})
		apiError := requireEnvelope(t, rec, http.StatusRequestEntityTooLarge, ErrTypeRequestTooLarge)
		require.Contains(t, apiError.Message, "128", "the message names the configured limit")
	})
}

// TestOAuthTokenExchangeWithARealIcebergClient proves the wire contract with the client the rest of
// this package is pinned to (github.com/apache/iceberg-go, I0-1): a catalog configured with
// credential=<access_key_id>:<secret> exchanges that pair at <uri>/v1/oauth/tokens using
// golang.org/x/oauth2 with AuthStyleInParams, then sends the returned token as a bearer token on
// every other request. Nothing about the exchange is stubbed - the client builds the form and the
// Authorization header itself - so this test fails if the endpoint's shape, its path or the
// accepted scheme drifts from what a real client sends.
func TestOAuthTokenExchangeWithARealIcebergClient(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: DefaultTokenTTL, MaxRequestBody: DefaultMaxRequestBody}
	router, _ := authTestRouter(t, cfg)

	var tokenRequests, bearerRequests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == catalogPath(OAuthTokenPath):
			tokenRequests++
		case strings.HasPrefix(r.Header.Get("Authorization"), bearerScheme+" "):
			bearerRequests++
		}
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	cat, err := rest.NewCatalog(t.Context(), "test", srv.URL+APIBasePath,
		rest.WithCredential(testAccessKeyID+":"+testSecretAccessKey))
	require.NoError(t, err, "the client exchanges its credential and reads the config behind it")
	require.NotNil(t, cat)
	require.Equal(t, 1, tokenRequests, "the client used POST /v1/oauth/tokens exactly once")
	require.Greater(t, bearerRequests, 0, "every catalog request carried the minted token")
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

// TestAuthTokenTTLDefault: a Config that did not come from LoadConfig still mints a usable token,
// for the documented default lifetime.
func TestAuthTokenTTLDefault(t *testing.T) {
	router, _ := authTestRouter(t, &Config{})

	rec := postToken(t, router, url.Values{
		"grant_type":    {grantTypeClientCredentials},
		"client_id":     {testAccessKeyID},
		"client_secret": {testSecretAccessKey},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response OAuthTokenResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, int64(DefaultTokenTTL/time.Second), response.ExpiresIn)
}
