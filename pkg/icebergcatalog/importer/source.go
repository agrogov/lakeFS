package importer

// The source half of the import: a minimal Apache Iceberg REST catalog client.
//
// Only the four operations an import needs are implemented, with the request and response shapes
// of the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml):
//
//	GET  /v1/config
//	GET  /v1/{prefix}/namespaces                  (ListNamespaces, pageToken/pageSize)
//	GET  /v1/{prefix}/namespaces/{namespace}/tables          (ListTables)
//	GET  /v1/{prefix}/namespaces/{namespace}/tables/{table}  (LoadTable)
//
// Namespace path parameters are the namespace levels joined by the spec's unit separator
// (U+001F), percent-encoded on the wire. The prefix is the one the catalog reports in
// GET /v1/config (overrides first, then defaults), and it is inserted before /namespaces exactly
// as the spec's `{prefix}` placeholder says; a catalog that reports no prefix is addressed
// without that segment.
//
// Authentication is either a bearer token supplied by the caller (--source-token) or the OAuth2
// client-credentials flow (--source-credential, "client_id:client_secret"): the client posts
// grant_type=client_credentials to /v1/oauth/tokens and sends the access token as a bearer token on
// every later request. The token endpoint can be overridden with --source-prop
// oauth2-server-uri=..., which some catalogs (and every catalog fronted by a different identity
// provider) need.
//
// Everything is clean-room: the client is written from the OpenAPI document and the public
// Iceberg REST catalog documentation, with no code from an Iceberg vendor SDK.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	// sourceNamespaceSeparator is the spec's default namespace separator, U+001F. A namespace is
	// an ordered list of levels in JSON, but in a path parameter it is this separator.
	sourceNamespaceSeparator = "\x1f"

	// sourceTokenPath is where a catalog serves the OAuth2 token endpoint, relative to the
	// catalog base URL (the URL of /v1/config).
	oauthEndpointPath = "/v1/oauth/tokens"

	// sourceConfigPath is the catalog configuration endpoint.
	sourceConfigPath = "/v1/config"

	// maxListingPages bounds a listing walk so a catalog that returns a repeating page token
	// cannot make the importer loop forever. The walk also stops on a repeated token; this is the
	// second, cheaper bound.
	maxListingPages = 10000

	// maxOAuthTokenResponse bounds the token endpoint's answer, which is a small JSON object.
	maxOAuthTokenResponse = 1 << 20
)

// SourceConfig is how a source catalog is reached.
type SourceConfig struct {
	// URI is the catalog base URL: the URL a client would use to reach <URI>/v1/config. A
	// trailing "/v1" is accepted and removed, so a URL copied from a client configuration works.
	URI string

	// Credential is an OAuth2 client credential, "client_id:client_secret". Mutually exclusive
	// with Token.
	Credential string

	// Token is a bearer token to send as-is. Mutually exclusive with Credential.
	Token string

	// Warehouse is the catalog's warehouse value, sent as the `warehouse` query parameter of
	// GET /v1/config. It is how Glue selects an account and how several catalogs select a
	// warehouse.
	Warehouse string

	// Props are additional client settings, from --source-prop k=v. Recognized keys are
	// "prefix" (skip the config call and use this prefix), "oauth2-server-uri" (the token
	// endpoint), "scope" (the OAuth2 scope) and the s3.* keys of the source FileIO. Unknown keys
	// are kept for the FileIO and are otherwise ignored - they are never sent as query
	// parameters, because a catalog does not ask for them and a property may be a secret.
	Props map[string]string

	// HTTPClient, when non-nil, is used for every request. The caller supplies one with the
	// timeouts and transport it wants; nil means http.DefaultClient.
	HTTPClient *http.Client
}

// Source is a connection to a source Iceberg REST catalog.
type Source struct {
	baseURL    string
	warehouse  string
	props      map[string]string
	prefixProp string
	token      string
	credential string
	tokenURL   string
	scope      string

	client *http.Client
	doer   *doer

	mu         sync.Mutex
	prefix     string
	prefixRead bool
	oauthToken string
}

// NewSource validates cfg and builds the client. It does not contact the catalog: the first
// request does, which keeps a dry run that fails on an option from opening a socket.
func NewSource(cfg SourceConfig) (*Source, error) {
	baseURL, err := normalizeCatalogURL(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("%w: source URI: %w", ErrInvalidOptions, err)
	}
	if cfg.Token != "" && cfg.Credential != "" {
		return nil, fmt.Errorf("%w: --source-token and --source-credential are mutually exclusive", ErrInvalidOptions)
	}
	credential := cfg.Credential
	if credential != "" && !strings.Contains(credential, ":") {
		return nil, fmt.Errorf("%w: --source-credential must be client_id:client_secret", ErrInvalidOptions)
	}

	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	props := cfg.Props
	if props == nil {
		props = map[string]string{}
	}

	source := &Source{
		baseURL:    baseURL,
		warehouse:  cfg.Warehouse,
		props:      props,
		prefixProp: props["prefix"],
		token:      cfg.Token,
		credential: credential,
		scope:      props["scope"],
		client:     client,
	}
	source.tokenURL = props["oauth2-server-uri"]
	if source.tokenURL == "" {
		source.tokenURL = baseURL + oauthEndpointPath
	}
	if source.warehouse == "" {
		source.warehouse = props["warehouse"]
	}
	source.doer = &doer{client: client, catalog: catalogSource, authorizer: source.authorize}
	return source, nil
}

// normalizeCatalogURL validates a catalog base URL and removes surrounding whitespace, trailing
// slashes and a trailing /v1, so both the catalog root and a client-configured /v1 URL work.
func normalizeCatalogURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: a source URI is required", ErrInvalidOptions)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("%w: %q is not an http(s) URL", ErrInvalidOptions, raw)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("%w: %q has no host", ErrInvalidOptions, raw)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%w: %q must not carry a query or a fragment", ErrInvalidOptions, raw)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.Path = strings.TrimSuffix(parsed.Path, "/v1")
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return parsed.String(), nil
}

// authorize adds the source catalog's credentials to req.
func (s *Source) authorize(req *http.Request) error {
	token := s.token
	if token == "" && s.credential != "" {
		var err error
		token, err = s.accessToken(req.Context())
		if err != nil {
			return err
		}
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return nil
}

// accessToken returns the token obtained from the source catalog's OAuth2 token endpoint,
// fetching it once and reusing it for the rest of the run.
func (s *Source) accessToken(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.oauthToken != "" {
		return s.oauthToken, nil
	}

	clientID, clientSecret, _ := strings.Cut(s.credential, ":")
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}
	if s.scope != "" {
		form.Set("scope", s.scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("%w: build OAuth2 token request: %w", ErrSource, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: OAuth2 token request to %s: %w", ErrSource, s.tokenURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxOAuthTokenResponse+1))
	if err != nil {
		return "", fmt.Errorf("%w: read OAuth2 token response: %w", ErrSource, err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", newCatalogError(catalogSource, http.MethodPost, s.tokenURL, resp.StatusCode, data)
	}

	var tokenResponse struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(data, &tokenResponse); err != nil {
		return "", fmt.Errorf("%w: decode OAuth2 token response: %w", ErrSource, err)
	}
	if tokenResponse.AccessToken == "" {
		return "", fmt.Errorf("%w: OAuth2 token response carries no access_token", ErrSource)
	}
	s.oauthToken = tokenResponse.AccessToken
	return s.oauthToken, nil
}

// Config returns the catalog configuration of GET /v1/config, with the prefix the caller must
// insert into an operation path.
func (s *Source) Config(ctx context.Context) (CatalogConfig, error) {
	query := url.Values{}
	if s.warehouse != "" {
		query.Set("warehouse", s.warehouse)
	}
	var config CatalogConfig
	if err := s.doer.do(ctx, http.MethodGet, s.configURL(query), nil, &config); err != nil {
		return CatalogConfig{}, err
	}
	return config, nil
}

func (s *Source) configURL(query url.Values) string {
	target := s.baseURL + sourceConfigPath
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	return target
}

// CatalogConfig is the response of GET /v1/config: defaults and overrides a client merges into its
// own configuration. Only `prefix` is used by the importer.
type CatalogConfig struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
}

// Prefix returns the catalog prefix: the --source-prop prefix when the caller set one, else the
// `prefix` override, else the `prefix` default, else "" (no prefix segment).
func (s *Source) Prefix(ctx context.Context) (string, error) {
	s.mu.Lock()
	if s.prefixRead {
		prefix := s.prefix
		s.mu.Unlock()
		return prefix, nil
	}
	s.mu.Unlock()

	prefix := s.prefixProp
	if prefix == "" {
		config, err := s.Config(ctx)
		if err != nil {
			return "", err
		}
		prefix = config.Overrides["prefix"]
		if prefix == "" {
			prefix = config.Defaults["prefix"]
		}
	}
	prefix = strings.Trim(prefix, "/")

	s.mu.Lock()
	s.prefix = prefix
	s.prefixRead = true
	s.mu.Unlock()
	return prefix, nil
}

// TableIdentifier is an Iceberg table name: its namespace levels and its name
// (components.schemas.TableIdentifier).
type TableIdentifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

// Dotted renders the identifier the way a client writes it: the namespace levels and the table
// name joined by dots.
func (t TableIdentifier) Dotted() string {
	parts := append(append([]string{}, t.Namespace...), t.Name)
	return strings.Join(parts, ".")
}

// Namespace is an Iceberg namespace: ordered levels.
type Namespace []string

// Dotted renders the namespace levels joined by dots.
func (n Namespace) Dotted() string { return strings.Join(n, ".") }

// LoadedTable is the part of a LoadTableResult the importer uses: the physical location of
// metadata.json and the document itself, exactly as the catalog returned it.
type LoadedTable struct {
	MetadataLocation string            `json:"metadata-location"`
	Metadata         json.RawMessage   `json:"metadata"`
	Config           map[string]string `json:"config"`
}

// ListNamespaces lists the child namespaces of parent. A nil or empty parent lists the root
// namespaces. Pagination is followed to the end.
func (s *Source) ListNamespaces(ctx context.Context, parent Namespace) ([]Namespace, error) {
	prefix, err := s.Prefix(ctx)
	if err != nil {
		return nil, err
	}

	var namespaces []Namespace
	pageToken := ""
	seenTokens := map[string]bool{}
	for page := 0; page < maxListingPages; page++ {
		query := url.Values{}
		if len(parent) > 0 {
			query.Set("parent", strings.Join(parent, sourceNamespaceSeparator))
		}
		if pageToken != "" {
			query.Set("pageToken", pageToken)
		}
		var response struct {
			Namespaces    []Namespace `json:"namespaces"`
			NextPageToken string      `json:"next-page-token"`
		}
		if err := s.doer.do(ctx, http.MethodGet, s.operationURL(prefix, query, "namespaces"), nil, &response); err != nil {
			return nil, err
		}
		namespaces = append(namespaces, response.Namespaces...)
		if response.NextPageToken == "" {
			return namespaces, nil
		}
		if seenTokens[response.NextPageToken] {
			return nil, fmt.Errorf("%w: namespaces listing repeats page token %q", ErrSource, response.NextPageToken)
		}
		seenTokens[response.NextPageToken] = true
		pageToken = response.NextPageToken
	}
	return nil, fmt.Errorf("%w: namespaces listing exceeds %d pages", ErrSource, maxListingPages)
}

// ListTables lists the tables of a namespace. Pagination is followed to the end.
func (s *Source) ListTables(ctx context.Context, namespace Namespace) ([]TableIdentifier, error) {
	prefix, err := s.Prefix(ctx)
	if err != nil {
		return nil, err
	}

	var tables []TableIdentifier
	pageToken := ""
	seenTokens := map[string]bool{}
	for page := 0; page < maxListingPages; page++ {
		query := url.Values{}
		if pageToken != "" {
			query.Set("pageToken", pageToken)
		}
		namespaceParam := tableNamespaceParam(namespace)
		var response struct {
			Identifiers   []TableIdentifier `json:"identifiers"`
			NextPageToken string            `json:"next-page-token"`
		}
		if err := s.doer.do(ctx, http.MethodGet,
			s.operationURL(prefix, query, "namespaces", namespaceParam, "tables"), nil, &response); err != nil {
			return nil, err
		}
		tables = append(tables, response.Identifiers...)
		if response.NextPageToken == "" {
			return tables, nil
		}
		if seenTokens[response.NextPageToken] {
			return nil, fmt.Errorf("%w: table listing repeats page token %q", ErrSource, response.NextPageToken)
		}
		seenTokens[response.NextPageToken] = true
		pageToken = response.NextPageToken
	}
	return nil, fmt.Errorf("%w: table listing exceeds %d pages", ErrSource, maxListingPages)
}

// LoadTable loads one table. `snapshots=all` asks the catalog for the complete snapshot list,
// which is what a copy needs; catalogs that ignore the parameter already return all snapshots.
func (s *Source) LoadTable(ctx context.Context, identifier TableIdentifier) (*LoadedTable, error) {
	if identifier.Name == "" {
		return nil, fmt.Errorf("%w: table identifier has no name", ErrInvalidOptions)
	}
	prefix, err := s.Prefix(ctx)
	if err != nil {
		return nil, err
	}
	query := url.Values{"snapshots": {"all"}}
	var loaded LoadedTable
	if err := s.doer.do(ctx, http.MethodGet, s.operationURL(prefix, query,
		"namespaces", tableNamespaceParam(Namespace(identifier.Namespace)), "tables", url.PathEscape(identifier.Name)),
		nil, &loaded); err != nil {
		return nil, err
	}
	return &loaded, nil
}

// operationURL builds <base>/v1/[<prefix>/]parts[?query]. The parts are already escaped.
func (s *Source) operationURL(prefix string, query url.Values, parts ...string) string {
	var segments []string
	segments = append(segments, "v1")
	if prefix != "" {
		segments = append(segments, url.PathEscape(prefix))
	}
	segments = append(segments, parts...)
	target := s.baseURL + "/" + strings.Join(segments, "/")
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	return target
}

// tableNamespaceParam renders a namespace as the spec's path parameter: levels joined by the unit
// separator, percent-encoded.
func tableNamespaceParam(namespace Namespace) string {
	return url.PathEscape(strings.Join(namespace, sourceNamespaceSeparator))
}
