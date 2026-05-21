package sso_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/auth"
	oidcencoding "github.com/treeverse/lakefs/pkg/auth/oidc/encoding"
	logging "github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
)

// mockOIDCServer is a minimal OIDC provider for testing (httptest-based).
type mockOIDCServer struct {
	server  *httptest.Server
	privKey *rsa.PrivateKey
	keyID   string
}

func newMockOIDCServer(t *testing.T) *mockOIDCServer {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	m := &mockOIDCServer{privKey: priv, keyID: "test-key-1"}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", m.serveDiscovery)
	mux.HandleFunc("/jwks", m.serveJWKS)
	mux.HandleFunc("/token", m.serveToken)
	m.server = httptest.NewServer(mux)
	return m
}

func (m *mockOIDCServer) issuerURL() string { return m.server.URL }
func (m *mockOIDCServer) close()            { m.server.Close() }

func (m *mockOIDCServer) serveDiscovery(w http.ResponseWriter, _ *http.Request) {
	base := m.server.URL
	doc := map[string]any{
		"issuer":                                base,
		"authorization_endpoint":                base + "/auth",
		"token_endpoint":                        base + "/token",
		"jwks_uri":                              base + "/jwks",
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(doc)
}

func (m *mockOIDCServer) serveJWKS(w http.ResponseWriter, _ *http.Request) {
	jwk := jose.JSONWebKey{Key: &m.privKey.PublicKey, KeyID: m.keyID, Algorithm: "RS256", Use: "sig"}
	set := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{jwk}}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(set)
}

// buildIDToken creates a signed RS256 JWT with the given claims.
func (m *mockOIDCServer) buildIDToken(clientID, nonce, oid string, extraClaims map[string]any) (string, error) {
	sig, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: m.privKey},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", m.keyID),
	)
	if err != nil {
		return "", err
	}
	now := time.Now()
	cl := jwt.Claims{
		Issuer:   m.server.URL,
		Audience: jwt.Audience{clientID},
		Subject:  "sub-value", // will be overridden by oid→sub in provider
		IssuedAt: jwt.NewNumericDate(now),
		Expiry:   jwt.NewNumericDate(now.Add(time.Hour)),
	}
	extra := map[string]any{
		"nonce": nonce,
		"oid":   oid,
	}
	for k, v := range extraClaims {
		extra[k] = v
	}
	return jwt.Signed(sig).Claims(cl).Claims(extra).Serialize()
}

// serveToken returns a token response containing an id_token built from form values.
func (m *mockOIDCServer) serveToken(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	// golang.org/x/oauth2 sends client credentials via HTTP Basic Auth by default.
	clientID, _, _ := r.BasicAuth()
	if clientID == "" {
		clientID = r.FormValue("client_id")
	}
	// In tests the code encodes "<nonce>:<oid>" for simplicity.
	code := r.FormValue("code")
	parts := strings.SplitN(code, ":", 2)
	if len(parts) != 2 {
		http.Error(w, "bad code", http.StatusBadRequest)
		return
	}
	nonce := parts[0]
	oid := parts[1]
	idToken, err := m.buildIDToken(clientID, nonce, oid, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := map[string]any{
		"access_token": "test-access-token",
		"token_type":   "Bearer",
		"id_token":     idToken,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// buildSessionStore creates an in-memory cookie store for test use.
func buildSessionStore() *sessions.CookieStore {
	store := sessions.NewCookieStore([]byte("test-secret-32-bytes-padded-here"))
	store.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
		Secure:   false,
		SameSite: http.SameSiteLaxMode,
	}
	return store
}

// buildService creates a NativeOIDCService pointed at the mock server.
func buildService(t *testing.T, mock *mockOIDCServer) *sso.NativeOIDCService {
	t.Helper()
	cfg := &sso.SSOConfig{
		Enabled:           true,
		ClientID:          "test-client-id",
		ClientSecret:      "test-secret",
		IssuerURL:         mock.issuerURL(),
		CallbackBaseURL:   "http://localhost",
		Scopes:            []string{"openid", "profile"},
		UserIDClaim:       "oid",
		FriendlyNameClaim: "preferred_username",
		GroupsClaim:       "roles",
	}
	// Pass nil for authService and a no-op logger — group sync is not exercised here.
	svc, err := sso.NewNativeOIDCService(context.Background(), cfg, nil, logging.Dummy(), "/auth/login")
	if err != nil {
		t.Fatalf("NewNativeOIDCService: %v", err)
	}
	return svc
}

// TestLoginRedirect verifies that GET /oidc/login redirects to the IdP with state+nonce.
func TestLoginRedirect(t *testing.T) {
	mock := newMockOIDCServer(t)
	defer mock.close()

	svc := buildService(t, mock)
	store := buildSessionStore()

	r := chi.NewRouter()
	svc.RegisterAdditionalRoutes(r, store)

	req := httptest.NewRequest(http.MethodGet, "/oidc/login?next=/dashboard", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302, got %d", resp.StatusCode)
	}

	loc := resp.Header.Get("Location")
	if !strings.Contains(loc, mock.issuerURL()+"/auth") {
		t.Errorf("redirect not to IdP: %s", loc)
	}
	if !strings.Contains(loc, "state=") {
		t.Errorf("no state in redirect URL: %s", loc)
	}
	if !strings.Contains(loc, "nonce=") {
		t.Errorf("no nonce in redirect URL: %s", loc)
	}
}

// TestOauthCallbackSuccess exercises the full callback flow with valid state + nonce.
func TestOauthCallbackSuccess(t *testing.T) {
	mock := newMockOIDCServer(t)
	defer mock.close()

	svc := buildService(t, mock)
	store := buildSessionStore()

	// Simulate the login step to seed the flow session and get back the session cookie.
	state, nonce, loginCookies := seedFlowSession(t, svc, store)

	// Build the callback request carrying the flow session cookie.
	oid := "azure-object-id-12345"
	code := fmt.Sprintf("%s:%s", nonce, oid)

	callbackURL := fmt.Sprintf("/api/v1/oidc/callback?code=%s&state=%s",
		url.QueryEscape(code), url.QueryEscape(state))
	req := httptest.NewRequest(http.MethodGet, callbackURL, nil)
	for _, c := range loginCookies {
		req.AddCookie(c)
	}
	w := httptest.NewRecorder()

	svc.OauthCallback(w, req, store)

	resp := w.Result()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("callback: expected 302, got %d; body: %s", resp.StatusCode, w.Body.String())
	}
	if loc := resp.Header.Get("Location"); loc != "/" {
		t.Errorf("expected redirect to /, got %s", loc)
	}

	// Verify the oidc_auth_session was set with oid→sub normalisation.
	authSess, _ := store.Get(req, auth.OIDCAuthSessionName)
	claims, ok := authSess.Values[auth.IDTokenClaimsSessionKey].(oidcencoding.Claims)
	if !ok {
		t.Fatalf("no claims in oidc_auth_session")
	}
	if sub, _ := claims["sub"].(string); sub != oid {
		t.Errorf("expected sub=%s (normalised from oid), got %s", oid, sub)
	}
}

// TestOauthCallbackInvalidState verifies that a tampered state is rejected.
func TestOauthCallbackInvalidState(t *testing.T) {
	mock := newMockOIDCServer(t)
	defer mock.close()

	svc := buildService(t, mock)
	store := buildSessionStore()

	_, nonce, loginCookies := seedFlowSession(t, svc, store)

	code := fmt.Sprintf("%s:some-oid", nonce)
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/oidc/callback?code="+url.QueryEscape(code)+"&state=TAMPERED", nil)
	for _, c := range loginCookies {
		req.AddCookie(c)
	}
	w := httptest.NewRecorder()

	svc.OauthCallback(w, req, store)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on invalid state, got %d", w.Code)
	}
}

// TestValidateSTS exercises the CLI code-exchange path.
func TestValidateSTS(t *testing.T) {
	mock := newMockOIDCServer(t)
	defer mock.close()

	svc := buildService(t, mock)

	oid := "cli-user-oid-abc"
	nonce := "unused-for-cli"
	code := fmt.Sprintf("%s:%s", nonce, oid)

	// The redirect URI must match what the service was configured with; the mock token
	// endpoint ignores it, but real IdPs validate it.
	redirectURI := "http://localhost/api/v1/oidc/callback"
	// state must be non-empty (CLI generates it; server rejects empty values).
	state := "cli-state-value"

	externalID, err := svc.ValidateSTS(context.Background(), code, redirectURI, state)
	if err != nil {
		t.Fatalf("ValidateSTS: %v", err)
	}
	if externalID != oid {
		t.Errorf("expected externalID=%s, got %s", oid, externalID)
	}
}

// ---- helpers ----------------------------------------------------------------

// seedFlowSession calls /oidc/login and returns state, nonce, and the Set-Cookie headers
// that must be attached to the subsequent callback request.
func seedFlowSession(t *testing.T, svc *sso.NativeOIDCService, store sessions.Store) (state, nonce string, cookies []*http.Cookie) {
	t.Helper()
	r := chi.NewRouter()
	svc.RegisterAdditionalRoutes(r, store)

	req := httptest.NewRequest(http.MethodGet, "/oidc/login", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("login: expected 302, got %d", w.Code)
	}
	loc := w.Header().Get("Location")
	u, _ := url.Parse(loc)
	state = u.Query().Get("state")
	nonce = u.Query().Get("nonce")
	cookies = w.Result().Cookies()
	return state, nonce, cookies
}

