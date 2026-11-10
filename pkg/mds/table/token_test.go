package table_test

// token_test.go pins the sync identity of the current guide (METADATA_SEARCH_REALIGN.md RA5, G12):
// the service signs its own login token for mds-service-user with auth.encrypt.secret_key, and the
// token must be one the lakeFS Iceberg REST catalog accepts - the same claims and signature
// auth.VerifyToken checks - and must expire.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

const tokenSecret = "mds-token-test-encrypt-secret-key-0123456789"

func TestSignServiceLoginToken(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	token, err := table.SignServiceLoginToken(tokenSecret, now)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	claims, err := table.VerifyServiceLoginToken(tokenSecret, token)
	require.NoError(t, err)
	require.Equal(t, table.ServiceUserName, claims.Subject)
	require.Equal(t, auth.LoginAudience, claims.Audience)
	require.Equal(t, now.Unix(), claims.IssuedAt.Unix())
	require.Equal(t, now.Add(table.ServiceLoginTokenTTL).Unix(), claims.ExpiresAt.Unix(),
		"the token expires, as auth.VerifyToken requires")
	require.NotEmpty(t, claims.ID, "every token carries its own jti")
}

func TestSignServiceLoginTokenExpires(t *testing.T) {
	// A token issued two TTLs ago expired one TTL ago; the catalog's verifier rejects it exactly as
	// it rejects an expired login token.
	issuedAt := time.Now().Add(-2 * table.ServiceLoginTokenTTL)
	token, err := table.SignServiceLoginToken(tokenSecret, issuedAt)
	require.NoError(t, err)

	_, err = table.VerifyServiceLoginToken(tokenSecret, token)
	require.ErrorIs(t, err, auth.ErrInvalidToken, "an expired token is not accepted")
}

func TestSignServiceLoginTokenRejectsAnotherKey(t *testing.T) {
	token, err := table.SignServiceLoginToken(tokenSecret, time.Now())
	require.NoError(t, err)
	_, err = table.VerifyServiceLoginToken("a-different-server-secret", token)
	require.ErrorIs(t, err, auth.ErrInvalidToken,
		"auth.encrypt.secret_key has to match the server's, or the catalog rejects the token")
}

func TestSignServiceLoginTokenNeedsTheKey(t *testing.T) {
	_, err := table.SignServiceLoginToken("", time.Now())
	require.ErrorIs(t, err, table.ErrNoEncryptSecretKey)
}

// TestCatalogConfigBearerToken pins the catalog client side: a token is sent as a bearer
// credential, and it alone satisfies the configuration's credential requirement.
func TestCatalogConfigBearerToken(t *testing.T) {
	cfg := table.CatalogConfig{
		Endpoint:    "http://lakefs:8000",
		Repository:  "repo-metadata",
		Branch:      "main",
		AccessToken: "signed-token",
	}
	require.NoError(t, cfg.Validate(), "a bearer token is a credential")
	require.Equal(t, "Bearer signed-token", cfg.Authorization())

	_, err := (table.CatalogConfig{Endpoint: "http://lakefs:8000", Repository: "r", Branch: "main"}).URI()
	require.ErrorIs(t, err, table.ErrInvalidCatalogConfig, "no credential at all is still refused")
}
