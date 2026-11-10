package table

// token.go is the sync identity of the current guide (METADATA_SEARCH_REALIGN.md RA5, G12). The
// service writes the metadata tables through the lakeFS Iceberg REST catalog as mds-service-user,
// "which lakeFS provisions on every start ... The service signs its own login tokens for that user
// with auth.encrypt.secret_key, which must match the lakeFS server's key ... so there is no user or
// credentials to create."
//
// A login token is the token auth.GenerateJWTLogin mints for POST /auth/login: the same claims,
// the same HMAC signature and the same login audience that pkg/auth.VerifyToken checks. The Iceberg
// REST catalog accepts it as "Authorization: Bearer <token>" (pkg/icebergcatalog/auth.go,
// userFromToken), so the service needs no access key pair and no operator-created credential.
//
// The token carries an expiry because auth.VerifyToken requires one. The TTL is deliberately shorter
// than a typical tick's lifetime: the catalog client of each branch is built per tick
// (pkg/mds/indexer), so a fresh token is minted often and a long-lived one is never necessary.
import (
	"errors"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
)

const (
	// ServiceUserName is the principal lakeFS provisions and the sync authenticates as. It is the
	// guide's spelling and the name pkg/mds/bootstrap creates.
	ServiceUserName = "mds-service-user"

	// ServiceLoginTokenTTL is how long a token the service signs for itself is valid. Every catalog
	// client the indexer builds signs a fresh one, and a table write is bounded by the catalog
	// client's request timeout, so one hour covers a write with a wide margin.
	ServiceLoginTokenTTL = time.Hour
)

// ErrNoEncryptSecretKey is returned by SignServiceLoginToken without auth.encrypt.secret_key: a
// token signed with an empty key would be rejected by the server (or, worse, accepted by a server
// that also configured an empty key), so the caller is told to fix its configuration instead.
var ErrNoEncryptSecretKey = errors.New("no auth.encrypt.secret_key to sign the metadata search service token with")

// SignServiceLoginToken signs the login token of ServiceUserName, issued at now and expiring
// ServiceLoginTokenTTL later. secretKey is auth.encrypt.secret_key of the lakeFS server
// configuration; it has to be the server's own key because the server verifies the token with it.
func SignServiceLoginToken(secretKey string, now time.Time) (string, error) {
	if secretKey == "" {
		return "", ErrNoEncryptSecretKey
	}
	token, err := auth.GenerateJWTLogin([]byte(secretKey), ServiceUserName, now, now.Add(ServiceLoginTokenTTL))
	if err != nil {
		return "", err
	}
	return token, nil
}

// VerifyServiceLoginToken verifies a token SignServiceLoginToken signed, the way the lakeFS Iceberg
// REST catalog verifies a bearer token (auth.VerifyToken, pkg/icebergcatalog/auth.go). It is what a
// caller uses to prove the sync identity a token names, and a test uses to pin signing and expiry.
func VerifyServiceLoginToken(secretKey, token string) (*auth.LoginClaims, error) {
	if secretKey == "" {
		return nil, ErrNoEncryptSecretKey
	}
	return auth.VerifyToken([]byte(secretKey), token)
}
