package sso

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

// BrowserLogin performs the OIDC authorization code flow from the CLI.
// It starts a local HTTP server to receive the IdP callback, opens a browser
// to the lakeFS OIDC login endpoint, waits for the authorization code, then
// exchanges it via the lakeFS STS endpoint and returns a JWT token + expiry.
func BrowserLogin(ctx context.Context, endpoint string) (token string, tokenExpiry int64, err error) {
	state, err := randomToken(32)
	if err != nil {
		return "", 0, fmt.Errorf("sso: generate state: %w", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", 0, fmt.Errorf("sso: start callback listener: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	redirectURI := fmt.Sprintf("http://127.0.0.1:%d", port)

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)

	mux := http.NewServeMux()
	srv := &http.Server{Handler: mux}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != state {
			select {
			case errCh <- fmt.Errorf("sso: state mismatch in callback"):
			default:
			}
			http.Error(w, "invalid state", http.StatusBadRequest)
			return
		}
		if e := r.URL.Query().Get("error"); e != "" {
			select {
			case errCh <- fmt.Errorf("sso: IdP error: %s", e):
			default:
			}
			http.Error(w, "authentication failed", http.StatusUnauthorized)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			select {
			case errCh <- fmt.Errorf("sso: no code in callback"):
			default:
			}
			http.Error(w, "no code", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, "<html><body>Authentication successful. You may close this window.</body></html>")
		select {
		case codeCh <- code:
		default:
		}
	})

	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			select {
			case errCh <- fmt.Errorf("sso: callback server: %w", serveErr):
			default:
			}
		}
	}()
	defer srv.Shutdown(ctx) //nolint:errcheck

	endpoint = strings.TrimRight(endpoint, "/")
	loginURL, err := buildLoginURL(endpoint, redirectURI, state)
	if err != nil {
		return "", 0, fmt.Errorf("sso: build login URL: %w", err)
	}

	if openErr := openBrowser(loginURL); openErr != nil {
		fmt.Printf("Could not open browser automatically. Please visit:\n\n  %s\n\n", loginURL)
	}

	var code string
	select {
	case code = <-codeCh:
	case err = <-errCh:
		return "", 0, err
	case <-time.After(5 * time.Minute):
		return "", 0, fmt.Errorf("sso: timed out waiting for browser callback")
	case <-ctx.Done():
		return "", 0, ctx.Err()
	}

	normalizedEndpoint, err := apiutil.NormalizeLakeFSEndpoint(endpoint)
	if err != nil {
		return "", 0, fmt.Errorf("sso: normalize endpoint: %w", err)
	}

	client, err := apigen.NewClientWithResponses(normalizedEndpoint)
	if err != nil {
		return "", 0, fmt.Errorf("sso: create lakeFS client: %w", err)
	}

	resp, err := client.StsLoginWithResponse(ctx, apigen.StsLoginJSONRequestBody{
		Code:        code,
		State:       state,
		RedirectUri: redirectURI,
	})
	if err != nil {
		return "", 0, fmt.Errorf("sso: STS login request: %w", err)
	}
	if resp.JSON200 == nil {
		return "", 0, fmt.Errorf("sso: STS login failed (HTTP %d)", resp.StatusCode())
	}

	expiry := int64(0)
	if resp.JSON200.TokenExpiration != nil {
		expiry = *resp.JSON200.TokenExpiration
	}

	return resp.JSON200.Token, expiry, nil
}

func buildLoginURL(endpoint, redirectURI, state string) (string, error) {
	u, err := url.Parse(endpoint + "/oidc/login")
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("redirect_uri", redirectURI)
	q.Set("state", state)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func openBrowser(rawURL string) error {
	var (
		cmd  string
		args []string
	)
	switch runtime.GOOS {
	case "windows":
		cmd, args = "cmd", []string{"/c", "start", rawURL}
	case "darwin":
		cmd, args = "open", []string{rawURL}
	default:
		cmd, args = "xdg-open", []string{rawURL}
	}
	return exec.Command(cmd, args...).Start()
}
