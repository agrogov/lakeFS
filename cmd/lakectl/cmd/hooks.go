package cmd

// hooks.go is fork-owned glue (it is not part of upstream cmd/lakectl): it exports the entry
// points wrapper binaries need without touching any upstream file of cmd/lakectl.

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

// GetRoot returns the root command so wrapper binaries (e.g. lakectl-sso) can
// inject additional subcommands before calling Execute().
func GetRoot() *cobra.Command { return rootCmd }

// APIClientTransport exposes the endpoint and the HTTP conventions lakectl uses for its own
// API client: the retry-enabled HTTP client and the request editors that add the credentials
// (basic auth, JWT token or AWS IAM) and the User-Agent header.
//
// Wrapper binaries that talk to another lakeFS API surface build the generated client for that
// surface from this transport (cmd/lakectl-datasets does it for the datasets control plane), so
// the server endpoint from ~/.lakectl.yaml, the configured credentials, the retry policy and the
// User-Agent behave exactly as they do for the core API. server is the configured endpoint
// without the /api/v1 API root, so callers compose their own API paths.
//
// It reads the lakectl configuration, so it must be called after lakectl's PersistentPreRun has
// run. Subcommands registered on GetRoot() satisfy that by construction.
func APIClientTransport() (server string, doer apigen.HttpRequestDoer, editors []apigen.RequestEditorFn) {
	client := getClient()
	inner, ok := client.ClientInterface.(*apigen.Client)
	if !ok {
		Die("could not read the configured API client transport", 1)
	}
	// inner.Server is the normalized lakeFS API root, <endpoint>/api/v1 (apiutil.NormalizeLakeFSEndpoint).
	server, err := apiEndpointURL(inner.Server)
	if err != nil {
		Die(fmt.Sprintf("could not read the configured API endpoint: %s", err), 1)
	}
	return server, inner.Client, inner.RequestEditors
}

// APIClientTransportWithRetry is APIClientTransport with a different retry policy: the endpoint,
// the credentials and the User-Agent editors are the same, but the HTTP client retries according
// to checkRetry (which has the go-retryablehttp signature) instead of lakectl's policy. A wrapper
// binary whose API has non-idempotent writes uses it to retry the requests it knows to be safe
// and nothing else; cmd/lakectl-datasets retries only GET and HEAD for that reason (CLI-2).
func APIClientTransportWithRetry(checkRetry func(ctx context.Context, resp *http.Response, err error) (bool, error)) (server string, doer apigen.HttpRequestDoer, editors []apigen.RequestEditorFn) {
	server, _, editors = APIClientTransport()
	// The configured retry counts and waits still apply; only the predicate changes.
	return server, getHTTPClientWithRetryConfig(checkRetry, cfg.Server.Retries), editors
}

// apiEndpointURL turns the normalized lakeFS API root, <endpoint>/api/v1, back into the endpoint
// URL wrapper binaries compose their API paths on top of. It works on the parsed URL - path,
// query and escaping included - instead of trimming "/api/v1" off the raw string, so an endpoint
// that serves lakeFS under a custom path (or that already names the API root) keeps its path
// (CLI-9).
func apiEndpointURL(apiRoot string) (string, error) {
	parsed, err := url.Parse(apiRoot)
	if err != nil {
		return "", err
	}
	// An endpoint configured with a path is documented to be the API root itself
	// (apiutil.NormalizeLakeFSEndpoint only appends /api/v1 to a bare host), so dropping the
	// suffix is a no-op there and removes the API root when NormalizeLakeFSEndpoint added it.
	if parsed.Path != "" {
		parsed.Path = strings.TrimSuffix(path.Clean(parsed.Path), apiutil.BaseURL)
	}
	parsed.RawPath = ""
	return parsed.String(), nil
}
