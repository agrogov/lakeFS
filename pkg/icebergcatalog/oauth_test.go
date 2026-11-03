package icebergcatalog

// Tests for the OAuth 2.0 token endpoint of ICEBERG_CATALOG_PLAN.md §7.1 and the security tests of
// §12: POST /v1/oauth/tokens exchanges an access key pair for a lakeFS JWT, a rejected credential
// is answered 401 without detail, an oversized body is 413, and the endpoint is reachable through
// the stack RoutesBuilder actually mounts (the wiring half - a handler that is never wired answers
// 404, which is what this file pins).
//
// The bearer/basic half of §7.1 - every other route accepts the minted token or HTTP basic - is in
// auth_test.go, next to the middleware both halves share.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
)

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

// TestRoutesBuilderServesOAuthTokenEndpoint is the wiring test of §7.1: POST /v1/oauth/tokens is
// part of the middleware chain RoutesBuilder mounts, so it answers at the documented URL
// (/iceberg/api/v1/oauth/tokens) and at the relative_to mount, its token authenticates the
// catalog, and Basic and bearer keep working beside it. A catalog mounted with the generic core
// middleware instead answers the token endpoint with 404 - the regression this test exists for.
func TestRoutesBuilderServesOAuthTokenEndpoint(t *testing.T) {
	useConfig(t, "iceberg:\n  enabled: true\n  token_ttl: 30m\n")

	deps := newServerDeps(t)
	mux := coreRouter()
	require.NoError(t, RoutesBuilder(t.Context(), deps, mux))

	tokenForm := url.Values{
		"grant_type":    {grantTypeClientCredentials},
		"client_id":     {testAccessKeyID},
		"client_secret": {testSecretAccessKey},
	}

	var minted string
	t.Run("the documented URL exchanges an access key pair for a token", func(t *testing.T) {
		// A 404 here is the regression this test exists for: it is what a catalog mounted with the
		// generic core middleware answers, because the token endpoint is served by the catalog's
		// own authentication middleware and by no route.
		rec := postToken(t, mux, tokenForm)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response OAuthTokenResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.NotEmpty(t, response.AccessToken)
		require.Equal(t, tokenTypeBearer, response.TokenType)
		require.Equal(t, IssuedTokenType, response.IssuedTokenType)
		require.Equal(t, int64((30*time.Minute)/time.Second), response.ExpiresIn,
			"expires_in is the configured iceberg.token_ttl")

		claims, err := auth.VerifyToken(deps.AuthService.SecretStore().SharedSecret(), response.AccessToken)
		require.NoError(t, err)
		require.Equal(t, testUsername, claims.Subject)
		minted = response.AccessToken
	})

	t.Run("the minted token authenticates the mounted catalog", func(t *testing.T) {
		rec := bearerRequest(t, mux, http.MethodGet, catalogPath(configPath), minted)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, map[string]string{"prefix": CatalogPrefix}, response.Overrides)
	})

	t.Run("basic and stateless bearer tokens keep working", func(t *testing.T) {
		rec := do(t, mux, http.MethodGet, catalogPath(configPath), testAccessKeyID, testSecretAccessKey)
		require.Equal(t, http.StatusOK, rec.Code, "basic auth still authenticates the catalog: %s", rec.Body.String())

		staticToken, err := auth.GenerateJWTLogin(deps.AuthService.SecretStore().SharedSecret(),
			testUsername, time.Now(), time.Now().Add(time.Hour))
		require.NoError(t, err)
		rec = bearerRequest(t, mux, http.MethodGet, catalogPath(configPath), staticToken)
		require.Equal(t, http.StatusOK, rec.Code, "a static lakeFS JWT still authenticates: %s", rec.Body.String())
	})

	t.Run("the relative_to mount serves the same token endpoint", func(t *testing.T) {
		// The token endpoint lives in the middleware, and both mounts share it, so a client that
		// carries its warehouse in the URL can exchange its credential there too.
		path := RelativeToBasePath + "/repo.main/api" + OAuthTokenPath
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tokenForm.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("the token endpoint is an authentication scheme, not an advertised operation", func(t *testing.T) {
		rec := do(t, mux, http.MethodGet, catalogPath(configPath), testAccessKeyID, testSecretAccessKey)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.NotContains(t, response.Endpoints, "POST /v1/{prefix}/oauth/tokens",
			"a client is configured with the token URL; it does not discover it from GET /v1/config")
	})
}

// TestRoutesBuilderOAuthTokenEndpointRejections pins the two security tests of §12 against the
// mounted stack rather than a hand-built router: a bad credential is 401 invalid_client with no
// description and nothing about the credential in the body, and a body over
// iceberg.max_request_body is 413.
func TestRoutesBuilderOAuthTokenEndpointRejections(t *testing.T) {
	t.Run("bad credentials are 401 without detail", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n")
		deps := newServerDeps(t)
		mux := coreRouter()
		require.NoError(t, RoutesBuilder(t.Context(), deps, mux))

		rec := postToken(t, mux, url.Values{
			"grant_type":    {grantTypeClientCredentials},
			"client_id":     {testAccessKeyID},
			"client_secret": {"not-the-secret"},
		})
		require.Equal(t, http.StatusUnauthorized, rec.Code, "body: %s", rec.Body.String())

		var response OAuthErrorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, oauthErrorInvalidClient, response.Error)
		require.Empty(t, response.ErrorDescription, "a rejected credential gets no description")
		for _, leak := range []string{testAccessKeyID, "not-the-secret", "secret"} {
			require.NotContains(t, rec.Body.String(), leak, "the answer must not echo or describe the credential")
		}
	})

	t.Run("an oversized body is 413 before anything is verified", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  max_request_body: 128\n")
		deps := newServerDeps(t)
		mux := coreRouter()
		require.NoError(t, RoutesBuilder(t.Context(), deps, mux))

		rec := postToken(t, mux, url.Values{
			"grant_type":    {grantTypeClientCredentials},
			"client_id":     {testAccessKeyID},
			"client_secret": {strings.Repeat("s", 512)},
		})
		apiError := requireEnvelope(t, rec, http.StatusRequestEntityTooLarge, ErrTypeRequestTooLarge)
		require.Contains(t, apiError.Message, "128", "the message names the configured limit")
	})
}
