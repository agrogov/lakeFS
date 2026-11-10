package bootstrap

// Tests for the guide's startup check (METADATA_SEARCH_REALIGN.md RA5, G12): the endpoint must
// answer as the lakeFS Iceberg REST catalog or the service stops after two minutes; a catalog that
// rejects the identity is logged and the service starts anyway.

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

const checkSecretKey = "mds-startup-check-encrypt-secret-key"

// catalogStub answers the catalog config path with config, and counts the requests.
type catalogStub struct {
	status  int
	body    string
	handler func(http.ResponseWriter, *http.Request)
	requests atomic.Int32
	lastAuth atomic.Value
}

func (s *catalogStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	s.lastAuth.Store(r.Header.Get("Authorization"))
	if s.handler != nil {
		s.handler(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(s.status)
	_, _ = w.Write([]byte(s.body))
}

func newCatalogStub(t *testing.T, status int, body string) (*catalogStub, *httptest.Server) {
	t.Helper()
	stub := &catalogStub{status: status, body: body}
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)
	return stub, server
}

const lakefsCatalogConfig = `{"defaults":{},"overrides":{"prefix":"lakefs"},"endpoints":["GET /v1/config"]}`

func TestCheckCatalogAcceptsTheLakeFSCatalog(t *testing.T) {
	stub, server := newCatalogStub(t, http.StatusOK, lakefsCatalogConfig)

	err := CheckCatalog(t.Context(), CatalogCheckConfig{
		Endpoint:         server.URL,
		EncryptSecretKey: checkSecretKey,
		Timeout:          time.Second,
		Interval:         10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), stub.requests.Load())
	authorization, _ := stub.lastAuth.Load().(string)
	require.Contains(t, authorization, "Bearer ",
		"without keys the check authenticates as the service user with a token it signs itself")

	token := authorization[len("Bearer "):]
	claims, err := table.VerifyServiceLoginToken(checkSecretKey, token)
	require.NoError(t, err)
	require.Equal(t, table.ServiceUserName, claims.Subject)
}

func TestCheckCatalogUsesTheConfiguredKeys(t *testing.T) {
	stub, server := newCatalogStub(t, http.StatusOK, lakefsCatalogConfig)

	err := CheckCatalog(t.Context(), CatalogCheckConfig{
		Endpoint:    server.URL,
		AccessKeyID: "AKIACHECK00000000001",
		SecretKey:   "check-secret",
		Timeout:     time.Second,
		Interval:    10 * time.Millisecond,
	})
	require.NoError(t, err)
	authorization, _ := stub.lastAuth.Load().(string)
	require.Equal(t,
		"Basic "+base64.StdEncoding.EncodeToString([]byte("AKIACHECK00000000001:check-secret")),
		authorization, "with keys the check authenticates as the user those keys belong to")
}

func TestCheckCatalogRetriesUntilTheCatalogAnswers(t *testing.T) {
	stub := &catalogStub{}
	var attempts atomic.Int32
	stub.handler = func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(lakefsCatalogConfig))
	}
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)

	err := CheckCatalog(t.Context(), CatalogCheckConfig{
		Endpoint:         server.URL,
		EncryptSecretKey: checkSecretKey,
		Timeout:          2 * time.Second,
		Interval:         10 * time.Millisecond,
	})
	require.NoError(t, err, "an endpoint that recovers inside the window is the catalog")
	require.Equal(t, int32(3), attempts.Load())
}

func TestCheckCatalogStopsOnANonCatalogAnswer(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "an HTML page", status: http.StatusOK, body: "<html>not a catalog</html>"},
		{name: "another catalog", status: http.StatusOK, body: `{"overrides":{"prefix":"other"}}`},
		{name: "not found", status: http.StatusNotFound, body: `{}`},
		{name: "server error", status: http.StatusInternalServerError, body: `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub, server := newCatalogStub(t, tt.status, tt.body)
			err := CheckCatalog(t.Context(), CatalogCheckConfig{
				Endpoint:         server.URL,
				EncryptSecretKey: checkSecretKey,
				Timeout:          100 * time.Millisecond,
				Interval:         10 * time.Millisecond,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "did not answer as the lakeFS Iceberg REST catalog")
			require.Greater(t, stub.requests.Load(), int32(1), "the check retries until the deadline")
		})
	}
}

func TestCheckCatalogStopsOnUnreachableEndpoint(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close() // the address is now unreachable

	err := CheckCatalog(t.Context(), CatalogCheckConfig{
		Endpoint:         server.URL,
		EncryptSecretKey: checkSecretKey,
		Timeout:          100 * time.Millisecond,
		Interval:         10 * time.Millisecond,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "did not answer as the lakeFS Iceberg REST catalog")
}

func TestCheckCatalogStartsAnywayWhenTheIdentityIsRejected(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			_, server := newCatalogStub(t, status, `{"error":{"message":"no"}}`)
			readLog := capturePackageLog(t)

			err := CheckCatalog(t.Context(), CatalogCheckConfig{
				Endpoint:         server.URL,
				EncryptSecretKey: checkSecretKey,
				Timeout:          100 * time.Millisecond,
				Interval:         10 * time.Millisecond,
			})
			require.NoError(t, err, "the guide starts anyway when the catalog rejects the token")

			output := readLog()
			require.Contains(t, output, "level=warning")
			require.Contains(t, output, "rejected the sync identity")
			require.Contains(t, output, PolicyName, "the likely causes name the policy to attach")
			require.Contains(t, output, "auth.encrypt.secret_key", "the likely causes name the key mismatch")
		})
	}
}

func TestCheckCatalogStartsAnywayWithConfiguredKeys(t *testing.T) {
	_, server := newCatalogStub(t, http.StatusUnauthorized, `{}`)
	readLog := capturePackageLog(t)

	err := CheckCatalog(t.Context(), CatalogCheckConfig{
		Endpoint:    server.URL,
		AccessKeyID: "AKIACHECK00000000001",
		SecretKey:   "wrong-secret",
		Timeout:     100 * time.Millisecond,
		Interval:    10 * time.Millisecond,
	})
	require.NoError(t, err)
	output := readLog()
	require.Contains(t, output, "access_key_id/secret_key", "the likely causes are about the configured keys")
}

func TestCheckCatalogStopsWhenContextEnds(t *testing.T) {
	_, server := newCatalogStub(t, http.StatusNotFound, `{}`)
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		done <- CheckCatalog(ctx, CatalogCheckConfig{
			Endpoint:         server.URL,
			EncryptSecretKey: checkSecretKey,
			Timeout:          time.Minute,
			Interval:         time.Second,
		})
	}()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the check did not stop when its context was cancelled")
	}
}

func TestCheckCatalogValidatesTheEndpoint(t *testing.T) {
	require.ErrorIs(t, CheckCatalog(t.Context(), CatalogCheckConfig{}), ErrEndpointRequired)
	require.ErrorIs(t, CheckCatalog(t.Context(), CatalogCheckConfig{Endpoint: "lakefs:8000"}), ErrInvalidEndpoint)
}
