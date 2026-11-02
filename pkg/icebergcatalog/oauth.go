package icebergcatalog

// The OAuth 2.0 token endpoint of ICEBERG_CATALOG_PLAN.md §7.1: POST /v1/oauth/tokens with
// grant_type=client_credentials, client_id=<access_key_id>, client_secret=<secret>.
//
// The endpoint is deprecated in the Iceberg REST spec (since Iceberg 1.6, slated for removal in
// 2.0), but Spark, PyIceberg, Trino and iceberg-go still use it by default: a catalog configured
// with credential=<access_key_id>:<secret_access_key> posts that pair here and sends the returned
// token as Authorization: Bearer on every other request
// (https://github.com/apache/iceberg/blob/main/docs/docs/rest-catalog.md). lakeFS documents the
// same URL, /iceberg/api/v1/oauth/tokens (https://docs.lakefs.io/iceberg/), which is
// APIBasePath + OAuthTokenPath.
//
// The exchange is the client_credentials grant of RFC 6749 §4.4: an
// application/x-www-form-urlencoded request (§4.4.2), a JSON token response (§5.1), and a §5.2
// error object on failure. The Iceberg REST spec's components.schemas.OAuthTokenRequest,
// OAuthTokenResponse and OAuthErrorResponse are exactly those shapes
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//
// The minted token is a lakeFS login JWT (auth.GenerateJWTLogin), so every other route verifies it
// with auth.VerifyToken and no server-side session or token table is involved. There is no
// refresh token and no token introspection (Q-I6): the token lives for iceberg.token_ttl, and a
// client that sees it expire re-authenticates from its configured credential, which is what the
// OAuth2 clients do when they do not get a refresh_token.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec,
// RFC 6749 and the public lakeFS documentation only. No code from lakeFS Enterprise or from
// upstream lakeFS >= 1.87 was used.

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
)

const (
	// grantTypeClientCredentials is the only grant the endpoint serves (RFC 6749 §1.3.4): the
	// catalog authenticates a client, it never issues a token on behalf of an absent user.
	grantTypeClientCredentials = "client_credentials"

	// tokenTypeBearer is the type of the token in the response (RFC 6749 §5.1). The spec's
	// examples spell it lowercase, and clients compare it case-insensitively.
	tokenTypeBearer = "bearer"

	// IssuedTokenType is the issued_token_type field of the spec's OAuthTokenResponse, the token
	// type URN of RFC 8693 §2.2.1 for an ordinary access token. The catalog issues nothing else -
	// there are no refresh or ID tokens here (Q-I6) - so the value is a constant.
	IssuedTokenType = "urn:ietf:params:oauth:token-type:access_token" // #nosec G101 -- a token-type URN, not a credential

	// The RFC 6749 §5.2 error codes this endpoint can answer with.
	//
	// oauthErrorInvalidClient is the answer to a credential that does not verify. Per §5.2 it
	// carries no error_description: telling a caller that the access key does not exist, or that
	// the secret is wrong, is exactly the detail §7.1 and §12 forbid.
	oauthErrorInvalidClient = "invalid_client"

	// oauthErrorInvalidRequest is a request that is not a usable client_credentials request at
	// all - no grant_type, no client_id, or a body that is not a form. It describes the request,
	// not the credentials, so it can say what is missing.
	oauthErrorInvalidRequest = "invalid_request"

	// oauthErrorUnsupportedGrantType is a grant_type other than client_credentials. No credential
	// is even looked at, so this cannot leak anything about one.
	oauthErrorUnsupportedGrantType = "unsupported_grant_type"
)

// OAuthTokenResponse is the token response of RFC 6749 §5.1 and of the Iceberg REST spec's
// components.schemas.OAuthTokenResponse.
//
// refresh_token is deliberately absent (Q-I6: there is no refresh flow), and scope is omitted
// because the catalog has no scopes to grant - lakeFS authorizes per repository, namespace and
// table (authz.go), not per token.
type OAuthTokenResponse struct {
	AccessToken     string `json:"access_token"`
	TokenType       string `json:"token_type"`
	ExpiresIn       int64  `json:"expires_in"`
	IssuedTokenType string `json:"issued_token_type"`
}

// OAuthErrorResponse is the error object of RFC 6749 §5.2
// (components.schemas.OAuthErrorResponse). error_description is omitted when it would say
// anything about a credential that failed, which is every invalid_client answer.
type OAuthErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description,omitempty"`
}

// oauthTokenRequest is one parsed token request. The scope of the request is not part of it: a
// client always sends one (iceberg-go sends "catalog"), the catalog has nothing to grant per
// scope, and rejecting an unfamiliar scope would break clients for no benefit.
type oauthTokenRequest struct {
	clientID     string
	clientSecret string
}

// handleToken serves POST /v1/oauth/tokens: it verifies the client's access key pair and answers
// with a lakeFS JWT for the user it belongs to.
//
// A rejected credential is answered 401 invalid_client with no description, before anything about
// a user is looked up or minted (§7.1, §12). Every other failure - a malformed request, a body
// over the limit, a signing error - is answered with the status that describes it, never with a
// detail about the credential.
func (a *catalogAuth) handleToken(w http.ResponseWriter, r *http.Request) {
	// The token endpoint runs before the router's body limit (limitRequestBody, http.go), because
	// the authentication layer is the first middleware, so it applies the same bound itself: an
	// unauthenticated caller must not be able to make the server buffer an unbounded body (§12,
	// "oversized bodies get 413").
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody(a.cfg))
	}

	req, ok := a.parseTokenRequest(w, r)
	if !ok {
		return
	}

	user, err := a.userFromCredentials(r.Context(), req.clientID, req.clientSecret)
	if err != nil {
		a.logger.WithContext(r.Context()).WithError(err).Warn("iceberg catalog: token request rejected")
		writeOAuthError(w, http.StatusUnauthorized, oauthErrorInvalidClient, "")
		return
	}

	issuedAt := a.now()
	expiresAt := issuedAt.Add(a.tokenTTL())
	accessToken, err := auth.GenerateJWTLogin(a.secret(), user.Username, issuedAt, expiresAt)
	if err != nil {
		a.logger.WithContext(r.Context()).WithError(err).Error("iceberg catalog: cannot sign a token")
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError, "")
		return
	}

	// RFC 6749 §5.1: a token response must not be stored by a cache. Both headers are set,
	// because RFC 2616-era caches (and every proxy in front of a data platform) honour only
	// Pragma.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	writeJSON(w, http.StatusOK, OAuthTokenResponse{
		AccessToken:     accessToken,
		TokenType:       tokenTypeBearer,
		ExpiresIn:       int64(a.tokenTTL() / time.Second),
		IssuedTokenType: IssuedTokenType,
	})
}

// parseTokenRequest decodes a client_credentials request (RFC 6749 §4.4.2) and answers it itself
// when it is not one, so handleToken only ever sees a request with a credential to verify.
//
// The credential may arrive in the form body (client_id/client_secret, which is what
// AuthStyleInParams does and what every Iceberg client sends) or, per RFC 6749 §2.3.1, in an
// HTTP basic Authorization header. The form is read first, because a client that sends both means
// the form.
func (a *catalogAuth) parseTokenRequest(w http.ResponseWriter, r *http.Request) (*oauthTokenRequest, bool) {
	if err := r.ParseForm(); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeIcebergError(w, http.StatusRequestEntityTooLarge, ErrTypeRequestTooLarge,
				fmt.Sprintf("request body exceeds the server limit of %d bytes", maxBytesErr.Limit))
			return nil, false
		}
		writeOAuthError(w, http.StatusBadRequest, oauthErrorInvalidRequest,
			"the request body is not an application/x-www-form-urlencoded form")
		return nil, false
	}

	switch grantType := r.PostForm.Get("grant_type"); grantType {
	case "":
		writeOAuthError(w, http.StatusBadRequest, oauthErrorInvalidRequest, "grant_type is required")
		return nil, false
	case grantTypeClientCredentials:
		// The only supported grant; nothing to do.
	default:
		writeOAuthError(w, http.StatusBadRequest, oauthErrorUnsupportedGrantType,
			fmt.Sprintf("only %q is supported", grantTypeClientCredentials))
		return nil, false
	}

	clientID := r.PostForm.Get("client_id")
	clientSecret := r.PostForm.Get("client_secret")
	if clientID == "" {
		if accessKey, secretKey, ok := r.BasicAuth(); ok {
			clientID, clientSecret = accessKey, secretKey
		}
	}
	if clientID == "" {
		// A request without a client_id is not a credential failure: there is nothing to verify,
		// so this says what is missing rather than pretending the credential was wrong.
		writeOAuthError(w, http.StatusBadRequest, oauthErrorInvalidRequest, "client_id is required")
		return nil, false
	}
	return &oauthTokenRequest{clientID: clientID, clientSecret: clientSecret}, true
}

// tokenTTL is iceberg.token_ttl (§10), with the documented default for a Config that did not come
// from LoadConfig - the same convention maxRequestBody (http.go) keeps, so a zero Config built in
// a test cannot silently mint a token that is already expired.
func (a *catalogAuth) tokenTTL() time.Duration {
	if a.cfg == nil || a.cfg.TokenTTL <= 0 {
		return DefaultTokenTTL
	}
	return a.cfg.TokenTTL
}

// writeOAuthError answers a token-endpoint failure in the RFC 6749 §5.2 error model, which is what
// an OAuth client parses - and, like a token response, it must not be cached.
func writeOAuthError(w http.ResponseWriter, status int, code, description string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	writeJSON(w, status, OAuthErrorResponse{Error: code, ErrorDescription: description})
}
