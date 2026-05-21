// Package main is the entry point for the lakectl-sso binary.
// It is identical to the upstream lakectl binary except that it adds an
// "sso-login" subcommand that performs the OIDC authorization code flow from
// the CLI: opens a browser, receives the callback, and exchanges the code for
// a lakeFS JWT token via the /api/v1/sts/login endpoint.
//
// Merge-friendliness: the upstream cmd/lakectl directory is untouched except
// for cmd/lakectl/cmd/hooks.go (new file), which exports GetRoot().
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	lakectlCmd "github.com/treeverse/lakefs/cmd/lakectl/cmd"
	"github.com/treeverse/lakefs/pkg/sso"
)

func main() {
	lakectlCmd.GetRoot().AddCommand(newSSOLoginCmd())
	lakectlCmd.Execute()
}

func newSSOLoginCmd() *cobra.Command {
	var (
		endpoint string
		ttl      int
	)
	cmd := &cobra.Command{
		Use:   "sso-login",
		Short: "Authenticate via Azure Entra ID SSO and obtain a lakeFS JWT token",
		Long: `Opens a browser to complete the OIDC authorization code flow against
Azure Entra ID. After successful authentication the lakeFS server exchanges
the authorization code for a JWT token via /api/v1/sts/login.

The token is printed to stdout and can be stored in the LAKECTL_ACCESS_KEY_ID
environment variable (with empty LAKECTL_SECRET_ACCESS_KEY) for subsequent
lakectl commands, or written manually to ~/.lakectl.yaml.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			token, expiry, err := sso.BrowserLogin(cmd.Context(), endpoint)
			if err != nil {
				return err
			}
			expiryStr := ""
			if expiry > 0 {
				expiryStr = fmt.Sprintf(" (expires %s)", time.Unix(expiry, 0).Format(time.RFC3339))
			}
			fmt.Fprintf(os.Stdout, "Token%s:\n%s\n", expiryStr, token)
			return nil
		},
	}
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "lakeFS server endpoint URL (required)")
	cmd.Flags().IntVar(&ttl, "ttl", 3600, "token time-to-live in seconds (max 43200)")
	_ = cmd.MarkFlagRequired("endpoint")
	return cmd
}
