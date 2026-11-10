// check.go is the startup check of the current guide's Metadata Search section
// (METADATA_SEARCH_REALIGN.md RA5, G12), verbatim: the service "checks that lakefs_endpoint answers
// as the lakeFS Iceberg REST catalog, and stops if it does not or stays unreachable for two
// minutes. If the catalog rejects the service's token or denies it access, the service logs the
// likely causes, starts anyway, and retries on every sync cycle."
//
// "Answers as the lakeFS Iceberg REST catalog" is checked at the catalog's negotiation endpoint,
// GET <lakefs_endpoint>/iceberg/api/v1/config: a 200 whose body is the Iceberg REST config and
// whose prefix is the lakeFS catalog's own "lakefs". Any other answer - a connection failure, a
// timeout, a 404/5xx, an HTML page, or another Iceberg catalog's prefix - is retried until the
// deadline (two minutes by default) and then returned as the startup error.
//
// 401 and 403 are the two answers the guide treats differently: they mean the endpoint is the
// catalog but would not accept the identity, so the check logs the likely causes and returns nil.
// The sync then starts and every tick retries the same authentication, which is where the failure
// surfaces as /_health and metrics rather than as a process that never starts.
package bootstrap

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

const (
	// CatalogConfigPath is the negotiation endpoint below the lakeFS base URL. The leading
	// /iceberg/api is the catalog mount of the current guide; /v1/config is the Iceberg REST
	// operation every catalog answers.
	CatalogConfigPath = "/iceberg/api/v1/config"

	// DefaultCatalogCheckTimeout is the guide's two minutes: the service stops when the endpoint
	// stays unreachable or does not answer as the catalog for this long.
	DefaultCatalogCheckTimeout = 2 * time.Minute

	// DefaultCatalogCheckInterval is how long the check waits between two attempts.
	DefaultCatalogCheckInterval = 2 * time.Second

	// catalogCheckRequestTimeout bounds one attempt, so a connection that accepts the request and
	// never answers cannot consume the whole check window in one call.
	catalogCheckRequestTimeout = 10 * time.Second

	// maxCatalogConfigBytes bounds the config body the check reads. GET /v1/config is a small JSON
	// document; the bound only keeps a misbehaving endpoint from streaming forever.
	maxCatalogConfigBytes = 1 << 20

	// lakeFSIcebergCatalogPrefix is the prefix GET /v1/config of the lakeFS Iceberg REST catalog
	// reports (pkg/icebergcatalog/config.go, CatalogPrefix). It is what distinguishes an answer
	// from the lakeFS catalog from one of another Iceberg REST catalog behind the same URL.
	lakeFSIcebergCatalogPrefix = "lakefs"
)

// ErrEndpointRequired is returned by CheckCatalog without a lakefs_endpoint: there is no address
// to check. The configuration loader already refuses an empty endpoint; this is the
// programming-error guard of a caller that builds CatalogCheckConfig by hand.
var ErrEndpointRequired = errors.New("mds startup check: no lakefs_endpoint")

// ErrNotTheCatalog is wrapped by every probe error that means "the endpoint answered, but not as
// the lakeFS Iceberg REST catalog". It is static so the retry loop and a caller can recognise it;
// the concrete answer is in the wrapping message.
var ErrNotTheCatalog = errors.New("the endpoint does not answer as the lakeFS Iceberg REST catalog")

// ErrInvalidEndpoint is wrapped by a lakefs_endpoint that is not an absolute URL.
var ErrInvalidEndpoint = errors.New("mds startup check: lakefs_endpoint is not an absolute URL")

// CatalogCheckConfig is the startup check's input: the endpoint, the identity the indexer will use
// (the service user's own signed token when both keys are empty, HTTP basic otherwise), and the
// server configuration's auth.encrypt.secret_key that signs that token.
type CatalogCheckConfig struct {
	// Endpoint is the lakeFS base URL (metadata_search.lakefs_endpoint).
	Endpoint string

	// AccessKeyID and SecretKey are the optional credentials of another user running the sync.
	// Empty means the check authenticates as mds-service-user with a token it signs itself.
	AccessKeyID string
	SecretKey   string

	// EncryptSecretKey is auth.encrypt.secret_key of the server configuration. It must match the
	// server's key, because the catalog verifies the service user's token with its own.
	EncryptSecretKey string

	// Timeout is the guide's two minutes when zero.
	Timeout time.Duration

	// Interval is the pause between two attempts. Zero means DefaultCatalogCheckInterval.
	Interval time.Duration

	// Log receives the retries and the likely causes of a rejected identity. Nil means the
	// process logger.
	Log logging.Logger
}

// CheckCatalog performs the guide's startup check. It returns nil when the endpoint answers as the
// lakeFS Iceberg REST catalog, or when the catalog answered 401/403 for the identity (the guide
// starts anyway and retries on every sync cycle; the likely causes are logged). It returns an
// error when the endpoint stays unreachable or does not answer as the catalog for the configured
// timeout, and when ctx is cancelled.
func CheckCatalog(ctx context.Context, cfg CatalogCheckConfig) error {
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	if endpoint == "" {
		return ErrEndpointRequired
	}
	configURL, err := catalogConfigURL(endpoint)
	if err != nil {
		return err
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultCatalogCheckTimeout
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultCatalogCheckInterval
	}
	log := cfg.Log
	if log == nil {
		log = logging.ContextUnavailable()
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for attempt := 1; ; attempt++ {
		authorization, authErr := cfg.authorization()
		if authErr != nil {
			return authErr
		}
		status, checkErr := probeCatalog(ctx, configURL, authorization)
		switch {
		case checkErr == nil:
			log.WithFields(logging.Fields{
				"endpoint": endpoint,
				"attempt":  attempt,
			}).Info("mds: lakefs_endpoint answers as the lakeFS Iceberg REST catalog")
			return nil
		case status == http.StatusUnauthorized || status == http.StatusForbidden:
			// The endpoint is the catalog, but it refused the identity. The guide starts anyway and
			// lets every sync cycle retry, so this is a warning, not a stop.
			logRejectedIdentity(ctx, log, endpoint, status, cfg.usingStaticKeys(), checkErr)
			return nil
		}
		lastErr = checkErr
		if time.Now().After(deadline) {
			return fmt.Errorf("mds: %s did not answer as the lakeFS Iceberg REST catalog within %s: %w",
				configURL, timeout, lastErr)
		}
		log.WithFields(logging.Fields{
			"endpoint": endpoint,
			"attempt":  attempt,
			"error":    checkErr.Error(),
		}).Info("mds: lakefs_endpoint does not answer as the lakeFS Iceberg REST catalog yet; retrying")

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// probeCatalog performs one GET of the catalog config endpoint. It returns the HTTP status of an
// answer that was received (0 when there was none) together with the reason the answer is not the
// lakeFS catalog; both are nil when the endpoint answered as the catalog.
func probeCatalog(ctx context.Context, configURL, authorization string) (int, error) {
	requestCtx, cancel := context.WithTimeout(ctx, catalogCheckRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, configURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build the catalog request: %w", err)
	}
	req.Header.Set("Authorization", authorization)
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request %s: %w", configURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxCatalogConfigBytes))
	if readErr != nil {
		return resp.StatusCode, fmt.Errorf("read the catalog answer: %w", readErr)
	}
	switch {
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return resp.StatusCode, fmt.Errorf("%w: the catalog answered %d", ErrNotTheCatalog, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return resp.StatusCode, fmt.Errorf("%w: the catalog answered %d, not 200", ErrNotTheCatalog, resp.StatusCode)
	}
	var config struct {
		Overrides map[string]string `json:"overrides"`
		Endpoints []string          `json:"endpoints"`
	}
	if err := json.Unmarshal(body, &config); err != nil {
		return resp.StatusCode, fmt.Errorf("%w: the answer is not an Iceberg REST catalog config: %w",
			ErrNotTheCatalog, err)
	}
	if config.Overrides["prefix"] != lakeFSIcebergCatalogPrefix {
		return resp.StatusCode, fmt.Errorf("%w: the answer is an Iceberg REST catalog config with prefix %q, not the lakeFS catalog's %q",
			ErrNotTheCatalog, config.Overrides["prefix"], lakeFSIcebergCatalogPrefix)
	}
	return resp.StatusCode, nil
}

// catalogConfigURL builds the config URL of a lakeFS base endpoint.
func catalogConfigURL(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("mds: lakefs_endpoint %q: %w", endpoint, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("%w: %q", ErrInvalidEndpoint, endpoint)
	}
	return endpoint + CatalogConfigPath, nil
}

// authorization returns the Authorization header of the configured identity: HTTP basic when the
// operator configured access keys, otherwise a login token the service user signs for itself with
// auth.encrypt.secret_key.
func (c CatalogCheckConfig) authorization() (string, error) {
	if c.AccessKeyID != "" || c.SecretKey != "" {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(c.AccessKeyID+":"+c.SecretKey)), nil
	}
	token, err := table.SignServiceLoginToken(c.EncryptSecretKey, time.Now())
	if err != nil {
		return "", err
	}
	return "Bearer " + token, nil
}

// usingStaticKeys reports whether the check authenticated with access keys rather than the
// service user's own token; the logged likely causes differ between the two.
func (c CatalogCheckConfig) usingStaticKeys() bool {
	return c.AccessKeyID != "" || c.SecretKey != ""
}

// logRejectedIdentity logs the likely causes of a 401/403 the guide asks the service to report
// before it starts anyway.
func logRejectedIdentity(ctx context.Context, log logging.Logger, endpoint string, status int, staticKeys bool, cause error) {
	causes := []string{
		"the service user " + table.ServiceUserName + " does not exist (the lakeFS server provisions it on start)",
		"the " + PolicyName + " policy is not attached to the sync identity",
		"auth.encrypt.secret_key does not match the lakeFS server's key, so the signed login token is rejected",
	}
	if staticKeys {
		causes = []string{
			"the configured access_key_id/secret_key are wrong or the user no longer exists",
			"the configured user does not have the " + PolicyName + " policy attached",
			"the user is not a member of every tenant it syncs (installations with tenants)",
		}
	}
	log.WithContext(ctx).WithFields(logging.Fields{
		"endpoint":     endpoint,
		"status":       status,
		"static_keys":  staticKeys,
		"likely_cause": causes,
		"error":        cause.Error(),
	}).Warn("mds: the lakeFS catalog rejected the sync identity; starting anyway and retrying on every sync cycle")
}
