package sso

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/authentication/apiclient"
	"github.com/treeverse/lakefs/pkg/auth"
	oidcencoding "github.com/treeverse/lakefs/pkg/auth/oidc/encoding"
	"golang.org/x/oauth2"
)

const (
	oidcFlowSessionName = "oidc_flow_session"
	sessionKeyState     = "state"
	sessionKeyNonce     = "nonce"
	sessionKeyNext      = "next"
)

// NativeOIDCService implements authentication.Service for Azure Entra ID OIDC.
// It handles the authorization code flow natively without an external auth service.
type NativeOIDCService struct {
	cfg       *SSOConfig
	provider  *gooidc.Provider
	oauth2Cfg oauth2.Config
}

// NewNativeOIDCService constructs the service by performing OIDC discovery against
// cfg.IssuerURL. This makes an HTTP request so the context should be alive.
func NewNativeOIDCService(ctx context.Context, cfg *SSOConfig) (*NativeOIDCService, error) {
	provider, err := gooidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("oidc: discover %s: %w", cfg.IssuerURL, err)
	}

	callbackURL := strings.TrimRight(cfg.CallbackBaseURL, "/") + "/api/v1/oidc/callback"

	oauth2Cfg := oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  callbackURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       cfg.Scopes,
	}

	return &NativeOIDCService{
		cfg:       cfg,
		provider:  provider,
		oauth2Cfg: oauth2Cfg,
	}, nil
}

func (s *NativeOIDCService) IsExternalPrincipalsEnabled() bool { return false }

func (s *NativeOIDCService) ExternalPrincipalLogin(_ context.Context, _ map[string]any) (*apiclient.ExternalPrincipal, error) {
	return nil, authentication.ErrNotImplemented
}

// RegisterAdditionalRoutes registers the /oidc/login route that starts the OIDC flow.
// The callback is handled by the swagger /api/v1/oidc/callback endpoint which delegates
// to OauthCallback.
func (s *NativeOIDCService) RegisterAdditionalRoutes(r *chi.Mux, sessionStore sessions.Store) {
	r.Get("/oidc/login", s.loginHandler(sessionStore))
}

// loginHandler generates state + nonce, stores them in a temporary session, and
// redirects the browser to the Azure Entra ID authorization endpoint.
func (s *NativeOIDCService) loginHandler(sessionStore sessions.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state, err := randomToken(32)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		nonce, err := randomToken(32)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		// Preserve the post-login redirect target (set by the UI via ?next=).
		next := r.URL.Query().Get("next")

		flowSess, err := sessionStore.Get(r, oidcFlowSessionName)
		if err != nil {
			http.Error(w, "session error", http.StatusInternalServerError)
			return
		}
		flowSess.Values[sessionKeyState] = state
		flowSess.Values[sessionKeyNonce] = nonce
		flowSess.Values[sessionKeyNext] = next
		if err = flowSess.Save(r, w); err != nil {
			http.Error(w, "session save failed", http.StatusInternalServerError)
			return
		}

		opts := []oauth2.AuthCodeOption{gooidc.Nonce(nonce)}
		for k, v := range s.cfg.AuthorizeParams {
			opts = append(opts, oauth2.SetAuthURLParam(k, v))
		}

		http.Redirect(w, r, s.oauth2Cfg.AuthCodeURL(state, opts...), http.StatusFound)
	}
}

// OauthCallback is called by the controller when the Azure callback lands on
// /api/v1/oidc/callback. It validates state + nonce, exchanges the code for an
// ID token, normalises the user ID claim (oid→sub), and stores claims in the
// oidc_auth_session cookie so that upstream UserFromOIDCSession works unchanged.
func (s *NativeOIDCService) OauthCallback(w http.ResponseWriter, r *http.Request, sessionStore sessions.Store) {
	ctx := r.Context()

	flowSess, err := sessionStore.Get(r, oidcFlowSessionName)
	if err != nil || flowSess.IsNew {
		http.Error(w, "no login session found", http.StatusBadRequest)
		return
	}

	storedState, _ := flowSess.Values[sessionKeyState].(string)
	storedNonce, _ := flowSess.Values[sessionKeyNonce].(string)
	next, _ := flowSess.Values[sessionKeyNext].(string)

	if storedState == "" || r.URL.Query().Get("state") != storedState {
		http.Error(w, "invalid or missing state", http.StatusBadRequest)
		return
	}

	if errParam := r.URL.Query().Get("error"); errParam != "" {
		desc := r.URL.Query().Get("error_description")
		http.Error(w, fmt.Sprintf("IdP error: %s — %s", errParam, desc), http.StatusUnauthorized)
		return
	}

	code := r.URL.Query().Get("code")
	token, err := s.oauth2Cfg.Exchange(ctx, code)
	if err != nil {
		http.Error(w, "token exchange failed", http.StatusUnauthorized)
		return
	}

	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		http.Error(w, "no id_token in response", http.StatusUnauthorized)
		return
	}

	verifier := s.provider.Verifier(&gooidc.Config{ClientID: s.cfg.ClientID})
	idToken, err := verifier.Verify(ctx, rawIDToken)
	if err != nil {
		http.Error(w, "id_token verification failed", http.StatusUnauthorized)
		return
	}

	if idToken.Nonce != storedNonce {
		http.Error(w, "nonce mismatch", http.StatusUnauthorized)
		return
	}

	claims := oidcencoding.Claims{}
	if err = idToken.Claims(&claims); err != nil {
		http.Error(w, "failed to parse claims", http.StatusInternalServerError)
		return
	}

	// Normalise: copy the configured user ID claim (e.g. "oid") into "sub" so that
	// upstream UserFromOIDCSession (pkg/auth/request_auth.go:155) works unchanged.
	if s.cfg.UserIDClaim != "sub" {
		if val, ok := claims[s.cfg.UserIDClaim]; ok {
			claims["sub"] = val
		}
	}

	// Store verified claims in the OIDC auth session — the middleware reads this.
	authSess, err := sessionStore.Get(r, auth.OIDCAuthSessionName)
	if err != nil {
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}
	authSess.Values[auth.IDTokenClaimsSessionKey] = claims
	if err = authSess.Save(r, w); err != nil {
		http.Error(w, "session save failed", http.StatusInternalServerError)
		return
	}

	// Expire the temporary flow session.
	flowSess.Options.MaxAge = -1
	_ = flowSess.Save(r, w)

	if next == "" || !strings.HasPrefix(next, "/") {
		next = "/"
	}
	http.Redirect(w, r, next, http.StatusFound)
}

// ValidateSTS handles the lakectl CLI SSO flow. It exchanges the authorization
// code for an ID token and returns the user's external ID (the configured
// user_id_claim, e.g. "oid"). State validation is the caller's responsibility.
func (s *NativeOIDCService) ValidateSTS(ctx context.Context, code, redirectURI, _ string) (string, error) {
	// Use the redirect URI supplied by the CLI — it differs from the browser callback URL.
	cfg := s.oauth2Cfg
	cfg.RedirectURL = redirectURI

	token, err := cfg.Exchange(ctx, code)
	if err != nil {
		return "", fmt.Errorf("oidc: code exchange: %w", err)
	}

	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		return "", fmt.Errorf("oidc: no id_token in token response")
	}

	// go-oidc/v3 exposes idToken.Nonce but does not auto-validate it; the caller is
	// responsible. For the CLI flow the nonce is not sent, so we skip the check here.
	verifier := s.provider.Verifier(&gooidc.Config{ClientID: s.cfg.ClientID})
	idToken, err := verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return "", fmt.Errorf("oidc: id_token verification: %w", err)
	}

	claims := map[string]any{}
	if err = idToken.Claims(&claims); err != nil {
		return "", fmt.Errorf("oidc: parse claims: %w", err)
	}

	claim := s.cfg.UserIDClaim
	externalID, ok := claims[claim].(string)
	if !ok || externalID == "" {
		return "", fmt.Errorf("oidc: claim %q not found or empty in id_token", claim)
	}

	return externalID, nil
}

// randomToken generates a URL-safe base64-encoded random string of n bytes.
func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
