package cmd

// hooks.go is fork-owned glue (it is not part of upstream cmd/lakectl): it exports the entry
// points wrapper binaries need without touching any upstream file of cmd/lakectl.

import (
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
	return strings.TrimSuffix(strings.TrimSuffix(inner.Server, "/"), apiutil.BaseURL), inner.Client, inner.RequestEditors
}
