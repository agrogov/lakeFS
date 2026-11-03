package icebergcatalog

// Tests for the authentication of ICEBERG_CATALOG_PLAN.md §7.1 and the security tests of §12:
// every route accepts a lakeFS bearer token or HTTP basic, and a rejected credential is answered
// 401 without detail. The token endpoint those credentials can be exchanged at is in oauth_test.go.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/sessions"
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

// sessionCookie builds the lakeFS login session cookie the web UI sends: the same
// internal_auth_session session, signed with the same auth service secret, carrying the same
// token key that api.GenericAuthMiddleware's cookie_auth scheme reads. It is written by the same
// gorilla/sessions store the middleware reads, so the test proves the cookie path end to end
// rather than by mocking the session.
func sessionCookie(t *testing.T, deps cmdpkg.ServerDeps, token string) *http.Cookie {
	t.Helper()
	store := sessions.NewCookieStore(deps.AuthService.SecretStore().SharedSecret())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	session, err := store.New(req, auth.InternalAuthSessionName)
	require.NoError(t, err)
	session.Values[auth.TokenSessionKeyName] = token
	rec := httptest.NewRecorder()
	require.NoError(t, session.Save(req, rec))
	cookies := rec.Result().Cookies()
	require.Len(t, cookies, 1, "the session store wrote exactly one cookie")
	return cookies[0]
}

// cookieRequest sends a request authenticated only by the cookie the browser holds.
func cookieRequest(t *testing.T, handler http.Handler, method, path string, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestAuthAcceptsCookieOnReads is the UI half of §7.1: the web UI authenticates by cookie, so the
// catalog's read endpoints accept the lakeFS login session for GET and HEAD. Without it a
// signed-in user's browser could not read the tables it is allowed to read at all.
func TestAuthAcceptsCookieOnReads(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	router, deps := authTestRouter(t, cfg)
	cookie := sessionCookie(t, deps, mintToken(t, router))

	t.Run("GET is served", func(t *testing.T) {
		rec := cookieRequest(t, router, http.MethodGet, catalogPath(configPath), cookie)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, map[string]string{"prefix": CatalogPrefix}, response.Overrides)
	})

	t.Run("HEAD is authenticated", func(t *testing.T) {
		// GET /v1/config is the only route this minimal router serves, so HEAD reaches the router
		// and is answered 405 there. That is the proof: 405 means the request got past
		// authentication and the authenticated-user requirement; a cookie the middleware did not
		// accept would be answered 401 by requireUser.
		rec := cookieRequest(t, router, http.MethodHead, catalogPath(configPath), cookie)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code, "body: %s", rec.Body.String())
	})
}

// TestAuthRejectsCookieOnWrites pins the boundary the UI path must not cross: a cookie is the one
// credential a browser attaches by itself, so it authenticates no state-changing request. A
// session cookie on a POST or a DELETE is 401 - the caller has no credential the catalog accepts -
// which is what keeps the cookie path free of a CSRF-exposed write.
func TestAuthRejectsCookieOnWrites(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	router, deps := authTestRouter(t, cfg)
	cookie := sessionCookie(t, deps, mintToken(t, router))

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			rec := cookieRequest(t, router, method, catalogPath(configPath), cookie)
			requireUnauthorized(t, rec)
		})
	}

	t.Run("POST "+OAuthTokenPath+" is not authenticated by a cookie", func(t *testing.T) {
		// The token endpoint is the one POST an unauthenticated caller may make, and it must not
		// accept the browser session either: a cookie is not an access key pair, so a
		// client_credentials request whose pair does not verify is answered 401 invalid_client and
		// mints no token.
		body := strings.NewReader("grant_type=" + grantTypeClientCredentials + "&client_id=nobody&client_secret=wrong")
		req := httptest.NewRequest(http.MethodPost, catalogPath(OAuthTokenPath), body)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(cookie)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code, "body: %s", rec.Body.String())
		require.NotContains(t, rec.Body.String(), "access_token")
	})
}

// TestAuthAcceptsCookieOnTheTableRoutes proves the cookie works where the UI needs it: on the
// read routes the repository Tables tab calls, with the user on the request context the handlers
// read (not only on GET /v1/config). The route table is a stand-in that echoes the authenticated
// user, because this task changes authentication and not the table handlers.
func TestAuthAcceptsCookieOnTheTableRoutes(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	deps := newServerDeps(t)
	middleware, err := NewAuthMiddleware(deps, cfg)
	require.NoError(t, err)

	whoami := func(w http.ResponseWriter, r *http.Request) {
		user, err := auth.GetUser(r.Context())
		if err != nil {
			writeIcebergError(w, http.StatusUnauthorized, ErrTypeNotAuthorized, authFailureMessage(http.StatusUnauthorized))
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"user": user.Username})
	}
	serve := testServer(cfg,
		route{method: http.MethodGet, pattern: "/v1/{prefix}/namespaces", handler: whoami},
		route{method: http.MethodGet, pattern: "/v1/{prefix}/namespaces/{namespace}", handler: whoami},
		route{method: http.MethodGet, pattern: "/v1/{prefix}/namespaces/{namespace}/tables", handler: whoami},
		route{method: http.MethodGet, pattern: "/v1/{prefix}/namespaces/{namespace}/tables/{table}", handler: whoami},
	)
	router, err := newRouter(middleware, cfg, serve)
	require.NoError(t, err)
	mux := coreRouter()
	mux.Mount(APIBasePath, router)

	cookie := sessionCookie(t, deps, mintToken(t, mux))
	for _, path := range []string{
		"/v1/" + CatalogPrefix + "/namespaces",
		"/v1/" + CatalogPrefix + "/namespaces/repo%1Fmain",
		"/v1/" + CatalogPrefix + "/namespaces/repo%1Fmain/tables",
		"/v1/" + CatalogPrefix + "/namespaces/repo%1Fmain%1Finventory/tables/books",
	} {
		t.Run(path, func(t *testing.T) {
			rec := cookieRequest(t, mux, http.MethodGet, catalogPath(path), cookie)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			require.JSONEq(t, `{"user":"`+testUsername+`"}`, rec.Body.String(),
				"the handler sees the user the cookie authenticated")
		})
	}
}

// TestAuthRejectsBadCookie: a session cookie the catalog cannot trust is a rejected credential,
// not an accepted one - an expired login token, a token signed with another key, and a cookie the
// session store cannot decode.
func TestAuthRejectsBadCookie(t *testing.T) {
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	router, deps := authTestRouter(t, cfg)
	secret := deps.AuthService.SecretStore().SharedSecret()

	expired, err := auth.GenerateJWTLogin(secret, testUsername, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	foreignKey, err := auth.GenerateJWTLogin([]byte("a-different-secret"), testUsername, time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, err)

	for name, token := range map[string]string{
		"an expired session token":          expired,
		"a session token for another key":   foreignKey,
		"a session token that is not a jwt": "not-a-jwt",
	} {
		t.Run(name, func(t *testing.T) {
			rec := cookieRequest(t, router, http.MethodGet, catalogPath(configPath), sessionCookie(t, deps, token))
			requireUnauthorized(t, rec, token)
		})
	}

	t.Run("a cookie this server did not sign", func(t *testing.T) {
		rec := cookieRequest(t, router, http.MethodGet, catalogPath(configPath),
			&http.Cookie{Name: auth.InternalAuthSessionName, Value: "forged"})
		requireUnauthorized(t, rec)
	})

	t.Run("a session without a token authenticates nothing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		store := sessions.NewCookieStore(secret)
		session, err := store.New(req, auth.InternalAuthSessionName)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		require.NoError(t, session.Save(req, rec))
		got := cookieRequest(t, router, http.MethodGet, catalogPath(configPath), rec.Result().Cookies()[0])
		requireUnauthorized(t, got)
	})
}

// uiManagementPath is the path of one UI management operation as the web UI addresses it, with the
// catalog's own {prefix} segment (CatalogPrefix).
func uiManagementPath(segments ...string) string {
	path := "/v1/" + CatalogPrefix
	for _, segment := range segments {
		path += "/" + segment
	}
	return catalogPath(path)
}

// uiManagementTestRouter is the production stack over a route table that carries the four UI
// management operations and the writes the cookie must not reach. Every one of them is registered,
// so a 401 in these tests is the authentication layer's answer and never a 404 or a 405 from a route
// that is not there: the request shapes are exactly the ones auth.go matches on.
func uiManagementTestRouter(t *testing.T) (http.Handler, cmdpkg.ServerDeps) {
	t.Helper()
	cfg := &Config{Prefix: CatalogPrefix, TokenTTL: time.Hour, MaxRequestBody: DefaultMaxRequestBody}
	deps := newServerDeps(t)
	middleware, err := NewAuthMiddleware(deps, cfg)
	require.NoError(t, err)

	whoami := func(w http.ResponseWriter, r *http.Request) {
		user, err := auth.GetUser(r.Context())
		if err != nil {
			writeIcebergError(w, http.StatusUnauthorized, ErrTypeNotAuthorized, authFailureMessage(http.StatusUnauthorized))
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"user": user.Username})
	}
	routeFor := func(method, pattern string) route {
		return route{method: method, pattern: pattern, handler: whoami}
	}
	serve := testServer(cfg,
		// The four operations the login session cookie may authenticate (with the UI's header):
		// create namespace, drop namespace, create table, drop table.
		routeFor(http.MethodPost, "/v1/{prefix}/namespaces"),
		routeFor(http.MethodDelete, "/v1/{prefix}/namespaces/{namespace}"),
		routeFor(http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/tables"),
		routeFor(http.MethodDelete, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"),
		// The writes it may not: the commit, register, properties, metrics, remote signing and
		// the (v1 non-goal, but registered here anyway) rename.
		routeFor(http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"),
		routeFor(http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/register"),
		routeFor(http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/properties"),
		routeFor(http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"),
		routeFor(http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign"),
		routeFor(http.MethodPost, "/v1/{prefix}/tables/rename"),
	)
	router, err := newRouter(middleware, cfg, serve)
	require.NoError(t, err)
	mux := coreRouter()
	mux.Mount(APIBasePath, router)
	return mux, deps
}

// clientCookieRequest sends a request authenticated only by the browser's cookie, identifying itself
// with the given X-Lakefs-Client value (empty for none).
func clientCookieRequest(t *testing.T, handler http.Handler, method, path string, cookie *http.Cookie, client string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	req.AddCookie(cookie)
	if client != "" {
		req.Header.Set(uiClientHeader, client)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestAuthAcceptsCookieOnUIManagementOperations is the UI-write half of the cookie rule: the Tables
// tab manages tables, so the login session cookie authenticates exactly these four operations -
// POST namespaces, DELETE namespace, POST tables, DELETE table - and only when the request carries
// the web UI's X-Lakefs-Client header.
func TestAuthAcceptsCookieOnUIManagementOperations(t *testing.T) {
	router, deps := uiManagementTestRouter(t)
	cookie := sessionCookie(t, deps, mintToken(t, router))

	operations := []struct{ name, method, path string }{
		{"create namespace", http.MethodPost, uiManagementPath("namespaces")},
		{"drop namespace", http.MethodDelete, uiManagementPath("namespaces", "repo%1Fmain%1Finventory")},
		{"create table", http.MethodPost, uiManagementPath("namespaces", "repo%1Fmain%1Finventory", "tables")},
		{"drop table", http.MethodDelete, uiManagementPath("namespaces", "repo%1Fmain%1Finventory", "tables", "books")},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			rec := clientCookieRequest(t, router, operation.method, operation.path, cookie, uiClientPrefix+"/1.2.3")
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			require.JSONEq(t, `{"user":"`+testUsername+`"}`, rec.Body.String(),
				"the handler sees the user the cookie authenticated")
		})
	}

	t.Run("the bare UI marker is enough", func(t *testing.T) {
		rec := clientCookieRequest(t, router, http.MethodPost, uiManagementPath("namespaces"), cookie, uiClientPrefix)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("a multi-level namespace is one path segment", func(t *testing.T) {
		// The {namespace} parameter joins its levels with %1F, so the drop of a nested namespace
		// is still the four-segment shape and not a different operation.
		rec := clientCookieRequest(t, router, http.MethodDelete,
			uiManagementPath("namespaces", "repo%1Fmain%1Finventory%1Fbooks"), cookie, uiClientPrefix+"/9.9.9")
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})
}

// TestAuthRejectsCookieOnUIManagementWithoutTheClientHeader pins the condition that scopes the
// exception: the cookie authenticates those four writes only for a request that identifies itself as
// the web UI. Without the header - or with any other client - the request carries no credential the
// catalog accepts, exactly as before the exception existed.
func TestAuthRejectsCookieOnUIManagementWithoutTheClientHeader(t *testing.T) {
	router, deps := uiManagementTestRouter(t)
	cookie := sessionCookie(t, deps, mintToken(t, router))

	operations := []struct{ method, path string }{
		{http.MethodPost, uiManagementPath("namespaces")},
		{http.MethodDelete, uiManagementPath("namespaces", "repo%1Fmain%1Finventory")},
		{http.MethodPost, uiManagementPath("namespaces", "repo%1Fmain%1Finventory", "tables")},
		{http.MethodDelete, uiManagementPath("namespaces", "repo%1Fmain%1Finventory", "tables", "books")},
	}
	for _, client := range []string{"", "curl/8.5.0", "spark/3.5.0", "lakefs-cli/1.40.0", "not-lakefs-webui"} {
		for _, operation := range operations {
			t.Run(operation.method+" "+operation.path+" as "+client, func(t *testing.T) {
				rec := clientCookieRequest(t, router, operation.method, operation.path, cookie, client)
				requireUnauthorized(t, rec)
			})
		}
	}
}

// TestAuthRejectsCookieOnWritesOutsideUIManagement pins the other boundary: even with the web UI's
// header, a cookie authenticates none of the writes the UI does not make. The commit, register,
// properties, metrics, remote signing and rename are the operations that change table contents or
// mint a credential, and they stay bearer/basic only. The view writes are in that group too: the
// Tables tab manages tables, and a view created or replaced from a browser session would be
// exactly the cookie-issued catalog write the four-operation exception exists to keep narrow.
func TestAuthRejectsCookieOnWritesOutsideUIManagement(t *testing.T) {
	router, deps := uiManagementTestRouter(t)
	cookie := sessionCookie(t, deps, mintToken(t, router))
	namespace := "repo%1Fmain%1Finventory"

	excluded := []struct{ name, method, path string }{
		{"commit table", http.MethodPost, uiManagementPath("namespaces", namespace, "tables", "books")},
		{"register table", http.MethodPost, uiManagementPath("namespaces", namespace, "register")},
		{"update properties", http.MethodPost, uiManagementPath("namespaces", namespace, "properties")},
		{"report metrics", http.MethodPost, uiManagementPath("namespaces", namespace, "tables", "books", "metrics")},
		{"remote signing", http.MethodPost, uiManagementPath("namespaces", namespace, "tables", "books", "sign")},
		{"table rename", http.MethodPost, uiManagementPath("tables", "rename")},
		{"create view", http.MethodPost, uiManagementPath("namespaces", namespace, "views")},
		{"replace view", http.MethodPost, uiManagementPath("namespaces", namespace, "views", "orders_view")},
		{"drop view", http.MethodDelete, uiManagementPath("namespaces", namespace, "views", "orders_view")},
	}
	for _, operation := range excluded {
		t.Run(operation.name, func(t *testing.T) {
			rec := clientCookieRequest(t, router, operation.method, operation.path, cookie, uiClientPrefix+"/1.2.3")
			requireUnauthorized(t, rec)
		})
	}

	t.Run("POST "+OAuthTokenPath+" is not authenticated by a cookie, header or not", func(t *testing.T) {
		body := strings.NewReader("grant_type=" + grantTypeClientCredentials + "&client_id=nobody&client_secret=wrong")
		req := httptest.NewRequest(http.MethodPost, catalogPath(OAuthTokenPath), body)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set(uiClientHeader, uiClientPrefix+"/1.2.3")
		req.AddCookie(cookie)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code, "body: %s", rec.Body.String())
		require.NotContains(t, rec.Body.String(), "access_token")
	})
}

// TestAuthBearerAndBasicAreUnchangedByTheUIManagementRule: the new condition is a cookie condition
// and nothing else. A bearer token or an access key pair authenticates every route with or without
// the UI header, on the management operations and away from them alike, and the header is not a
// credential on its own.
func TestAuthBearerAndBasicAreUnchangedByTheUIManagementRule(t *testing.T) {
	router, _ := uiManagementTestRouter(t)

	operations := []struct{ method, path string }{
		{http.MethodPost, uiManagementPath("namespaces")},
		{http.MethodDelete, uiManagementPath("namespaces", "repo%1Fmain%1Finventory")},
		{http.MethodPost, uiManagementPath("namespaces", "repo%1Fmain%1Finventory", "tables")},
		{http.MethodDelete, uiManagementPath("namespaces", "repo%1Fmain%1Finventory", "tables", "books")},
		{http.MethodPost, uiManagementPath("namespaces", "repo%1Fmain%1Finventory", "tables", "books")}, // the commit
	}

	t.Run("bearer, with and without the UI header", func(t *testing.T) {
		token := mintToken(t, router)
		for _, operation := range operations {
			for _, client := range []string{"", uiClientPrefix + "/1.2.3"} {
				req := httptest.NewRequest(operation.method, operation.path, nil)
				req.Header.Set("Authorization", bearerScheme+" "+token)
				if client != "" {
					req.Header.Set(uiClientHeader, client)
				}
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			}
		}
	})

	t.Run("basic, with and without the UI header", func(t *testing.T) {
		for _, operation := range operations {
			for _, client := range []string{"", uiClientPrefix + "/1.2.3"} {
				req := httptest.NewRequest(operation.method, operation.path, nil)
				req.SetBasicAuth(testAccessKeyID, testSecretAccessKey)
				if client != "" {
					req.Header.Set(uiClientHeader, client)
				}
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			}
		}
	})

	t.Run("a wrong bearer stays 401 even with the UI header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, uiManagementPath("namespaces"), nil)
		req.Header.Set("Authorization", bearerScheme+" not-a-jwt")
		req.Header.Set(uiClientHeader, uiClientPrefix+"/1.2.3")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		requireUnauthorized(t, rec)
	})

	t.Run("the UI header alone is not a credential", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, uiManagementPath("namespaces"), nil)
		req.Header.Set(uiClientHeader, uiClientPrefix+"/1.2.3")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		requireUnauthorized(t, rec)
	})
}
