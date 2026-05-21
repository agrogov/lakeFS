package sso

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/auth"
	oidcencoding "github.com/treeverse/lakefs/pkg/auth/oidc/encoding"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/authentication/apiclient"
	logging "github.com/treeverse/lakefs/pkg/logging"
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
	cfg         *SSOConfig
	provider    *gooidc.Provider
	oauth2Cfg   oauth2.Config
	authService groupManager
	logger      logging.Logger
}

// NewNativeOIDCService constructs the service by performing OIDC discovery against
// cfg.IssuerURL. This makes an HTTP request so the context should be alive.
// authService may be nil when group sync is disabled.
func NewNativeOIDCService(ctx context.Context, cfg *SSOConfig, authService auth.Service, logger logging.Logger) (*NativeOIDCService, error) {
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
		cfg:         cfg,
		provider:    provider,
		oauth2Cfg:   oauth2Cfg,
		authService: authService,
		logger:      logger,
	}, nil
}

func (s *NativeOIDCService) IsExternalPrincipalsEnabled() bool {
	// External principals are managed through ACL groups, not through this service.
	return false
}

func (s *NativeOIDCService) ExternalPrincipalLogin(_ context.Context, _ map[string]any) (*apiclient.ExternalPrincipal, error) {
	return nil, authentication.ErrNotImplemented
}

// RegisterAdditionalRoutes registers SSO-specific routes and middleware on the root router.
//
// /oidc/login  — starts the authorization code flow (browser) or CLI redirect.
// logoutMiddleware — clears oidc_auth_session and oidc_flow_session on any /logout
//
//	request before the upstream logout handler runs. The upstream handler
//	(registered via r.Mount) clears internal_auth_session and performs the
//	redirect; chi v5 matches r.Mount before r.Get for the same path, so we use
//	a middleware instead of a competing route.
func (s *NativeOIDCService) RegisterAdditionalRoutes(r *chi.Mux, sessionStore sessions.Store) {
	r.Use(s.logoutMiddleware(sessionStore))
	r.Get("/oidc/login", s.loginHandler(sessionStore))
}

// logoutMiddleware clears OIDC-specific sessions on logout requests, then
// delegates to the next handler (the upstream logout handler that clears
// internal_auth_session and performs the redirect). Errors clearing individual
// sessions are logged but do not abort the logout flow.
func (s *NativeOIDCService) logoutMiddleware(sessionStore sessions.Store) func(http.Handler) http.Handler {
	sessionsToClear := []string{
		auth.OIDCAuthSessionName,
		oidcFlowSessionName,
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/logout" {
				for _, name := range sessionsToClear {
					sess, _ := sessionStore.Get(r, name)
					sess.Options.MaxAge = -1
					if err := sess.Save(r, w); err != nil {
						s.logger.WithError(err).WithField("session", name).Error("logout: failed to clear OIDC session")
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// loginHandler handles the authorization code flow start.
//
// Browser flow (no redirect_uri param): generates state + nonce, stores them in a
// temporary session cookie, then redirects to the IdP.
//
// CLI flow (redirect_uri param present): uses the caller-supplied state and the
// CLI's local redirect URI. No session cookie is set — the CLI owns state
// verification and nonce is skipped (ValidateSTS does not check nonce).
func (s *NativeOIDCService) loginHandler(sessionStore sessions.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if redirectURI := r.URL.Query().Get("redirect_uri"); redirectURI != "" {
			s.cliLoginRedirect(w, r, redirectURI)
			return
		}

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

// cliLoginRedirect handles the CLI authorization code flow start. The CLI provides
// its own redirect_uri (a local HTTP server) and state. No session cookie is set.
func (s *NativeOIDCService) cliLoginRedirect(w http.ResponseWriter, r *http.Request, redirectURI string) {
	state := r.URL.Query().Get("state")
	if state == "" {
		http.Error(w, "state is required for CLI flow", http.StatusBadRequest)
		return
	}
	cfg := s.oauth2Cfg
	cfg.RedirectURL = redirectURI
	var opts []oauth2.AuthCodeOption
	for k, v := range s.cfg.AuthorizeParams {
		opts = append(opts, oauth2.SetAuthURLParam(k, v))
	}
	http.Redirect(w, r, cfg.AuthCodeURL(state, opts...), http.StatusFound)
}

// OauthCallback is called by the controller when the Azure callback lands on
// /api/v1/oidc/callback. It validates state + nonce, exchanges the code for an
// ID token, normalises the user ID claim (oid→sub), stores claims in the
// oidc_auth_session cookie, and optionally syncs group memberships.
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

	// Invalidate the flow session immediately after state is verified to prevent
	// session-fixation attacks on subsequent error paths.
	flowSess.Options.MaxAge = -1
	if err = flowSess.Save(r, w); err != nil {
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}

	if errParam := r.URL.Query().Get("error"); errParam != "" {
		// Log IdP error details server-side; do not reflect attacker-controlled
		// query-string values verbatim to the browser.
		s.logger.WithFields(logging.Fields{
			"error":             errParam,
			"error_description": r.URL.Query().Get("error_description"),
		}).Warn("IdP returned an error on callback")
		http.Error(w, "authentication failed", http.StatusUnauthorized)
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
	// upstream UserFromOIDCSession (pkg/auth/request_auth.go) works unchanged.
	if s.cfg.UserIDClaim != "sub" {
		if val, ok := claims[s.cfg.UserIDClaim]; ok {
			claims["sub"] = val
		}
	}

	username, _ := claims["sub"].(string)

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

	// Sync group memberships from the token. Errors are non-fatal: the user is
	// already authenticated at this point, so we log and continue.
	if s.authService != nil && username != "" && s.cfg.SyncGroupsOnLogin && s.cfg.GroupsClaim != "" {
		rawGroups, claimPresent := claims[s.cfg.GroupsClaim]
		if !claimPresent {
			// Absent claim is distinct from an empty list. Skipping sync rather than
			// silently stripping all managed memberships (e.g. misconfigured manifest).
			s.logger.WithFields(logging.Fields{
				"user":  username,
				"claim": s.cfg.GroupsClaim,
			}).Warn("group sync: groups claim absent from token, skipping sync")
		} else {
			tokenGroups := ExtractStringSlice(rawGroups)
			if syncErr := SyncGroups(ctx, s.authService, s.logger, username, tokenGroups, s.cfg); syncErr != nil {
				s.logger.WithField("user", username).WithError(syncErr).Warn("group sync failed")
			}
		}
	}

	if next == "" {
		next = "/"
	} else {
		// Guard against open-redirect via protocol-relative URLs (//host) or
		// backslash tricks (/\host) accepted by some browsers.
		u, parseErr := url.Parse(next)
		if parseErr != nil || u.Host != "" || u.Scheme != "" || !strings.HasPrefix(u.Path, "/") {
			next = "/"
		}
	}
	http.Redirect(w, r, next, http.StatusFound)
}

// ValidateSTS handles the lakectl CLI SSO flow. It exchanges the authorization
// code for an ID token and returns the user's external ID (the configured
// user_id_claim, e.g. "oid"). The state parameter is forwarded by the CLI but
// cannot be verified server-side because state is generated by the CLI, not the
// server. The CLI is responsible for verifying its own state value.
func (s *NativeOIDCService) ValidateSTS(ctx context.Context, code, redirectURI, state string) (string, error) {
	if state == "" {
		return "", fmt.Errorf("oidc: ValidateSTS: state must not be empty")
	}

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
