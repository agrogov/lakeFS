package icebergcatalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

// credsTablesFixture is the table fixture with credential vending wired into the table service the
// way http.go wires it, and the credentials endpoint mounted next to it. The fixture's repository
// lives on a mem:// storage namespace, which cannot be scoped as S3, so both scopes answer the
// credentials fixture's S3 root; what they were asked about is recorded.
type credsTablesFixture struct {
	*tablesFixture
	sts    *credsFakeSTS
	router http.Handler

	// createdUUID and createdLocation are the arguments of the last created-table scope.
	createdUUID, createdLocation string
}

func newCredsTablesFixture(t *testing.T) *credsTablesFixture {
	t.Helper()
	f := &credsTablesFixture{tablesFixture: newTablesFixture(t), sts: &credsFakeSTS{}}
	creds := newTestCredentialService(credsTestAdapter(), f.authz.authz, credsTestVending(), f.sts, nil)
	creds.refs = f.catalog
	creds.createdScope = func(_ context.Context, _, tableUUID, location string) (tableDataScope, error) {
		f.createdUUID, f.createdLocation = tableUUID, location
		return tableDataScope{Root: credsTestRoot, Location: credsTestRoot}, nil
	}
	tables := NewTableService(f.serve.store, f.authz.authz, f.catalog, &Config{Prefix: CatalogPrefix}).
		WithCredentials(creds)
	cfg := &Config{Prefix: CatalogPrefix}
	router, err := newRouter(tablesUser, cfg, catalogServerWith(cfg, append(tables.Routes(), creds.Routes()...)...))
	require.NoError(t, err, "build catalog router")
	f.router = router
	return f
}

// send serves one request as user, asking for vended credentials when delegation is set.
func (f *credsTablesFixture) send(t *testing.T, method, target, user, delegation, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("X-Test-User", user)
	if body != "" {
		req.Header.Set("Content-Type", contentTypeJSON)
	}
	if delegation != "" {
		req.Header.Set(AccessDelegationHeader, delegation)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// storageCredentials decodes the storage-credentials member of a LoadTableResult.
func storageCredentials(t *testing.T, rec *httptest.ResponseRecorder) []StorageCredential {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var body struct {
		StorageCredentials []StorageCredential `json:"storage-credentials"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "body: %s", rec.Body.String())
	return body.StorageCredentials
}

// requireSession asserts the vended session of the fake STS, and whether its policy writes.
func (f *credsTablesFixture) requireSession(t *testing.T, credentials []StorageCredential, write bool) {
	t.Helper()
	require.Len(t, credentials, 1)
	require.Equal(t, credsTestRoot, credentials[0].Prefix)
	require.Equal(t, credsTestAccessKeyID, credentials[0].Config[accessKeyIDProperty])
	require.Equal(t, credsTestSessionToken, credentials[0].Config[sessionTokenProperty])
	require.Equal(t, write, strings.Contains(aws.ToString(f.sts.input.Policy), "s3:PutObject"),
		"write session %v, policy %s", write, aws.ToString(f.sts.input.Policy))
}

// TestTableStorageCredentials pins the storage-credentials member of a load and a create answer
// (§8, §2.1): iceberg-go enters its vended-credentials mode only when that member is present, so
// the credentials endpoint alone leaves a client without a session.
func TestTableStorageCredentials(t *testing.T) {
	books := Namespace{"inventory", "books"}
	onMain := tablesNamespaceParam(tablesTestRef, books)
	onTag := tablesNamespaceParam(tablesTestTag, books)

	t.Run("a load on a branch vends a write session to a writer", func(t *testing.T) {
		f := newCredsTablesFixture(t)
		rec := f.send(t, http.MethodGet, tablesLoadTarget(onMain, "orders", nil), "carol",
			VendedCredentialsDelegation, "")
		f.requireSession(t, storageCredentials(t, rec), true)
		require.Contains(t, aws.ToString(f.sts.input.RoleSessionName), "carol")
	})

	t.Run("a load vends a read session to a reader", func(t *testing.T) {
		f := newCredsTablesFixture(t)
		rec := f.send(t, http.MethodGet, tablesLoadTarget(onMain, "orders", nil), "alice",
			VendedCredentialsDelegation, "")
		f.requireSession(t, storageCredentials(t, rec), false)
	})

	t.Run("a load on a tag vends a read session to a writer", func(t *testing.T) {
		f := newCredsTablesFixture(t)
		rec := f.send(t, http.MethodGet, tablesLoadTarget(onTag, "orders", nil), "carol",
			"remote-signing, "+VendedCredentialsDelegation, "")
		f.requireSession(t, storageCredentials(t, rec), false)
	})

	t.Run("a load that does not ask for vended credentials gets none", func(t *testing.T) {
		f := newCredsTablesFixture(t)
		for _, delegation := range []string{"", RemoteSigningDelegation} {
			rec := f.send(t, http.MethodGet, tablesLoadTarget(onMain, "orders", nil), "carol", delegation, "")
			require.Nil(t, storageCredentials(t, rec), "delegation %q", delegation)
			require.NotContains(t, rec.Body.String(), "storage-credentials")
		}
		require.Zero(t, f.sts.calls)
	})

	t.Run("a load is served when vending fails", func(t *testing.T) {
		f := newCredsTablesFixture(t)
		f.sts.err = context.DeadlineExceeded
		rec := f.send(t, http.MethodGet, tablesLoadTarget(onMain, "orders", nil), "carol",
			VendedCredentialsDelegation, "")
		require.Nil(t, storageCredentials(t, rec), "§8 best effort: no credentials, not an error")
		require.Equal(t, 1, f.sts.calls)
	})

	t.Run("a table service without vending answers no credentials", func(t *testing.T) {
		f := newTablesFixture(t)
		req := httptest.NewRequest(http.MethodGet, tablesLoadTarget(onMain, "orders", nil), nil)
		req.Header.Set("X-Test-User", "carol")
		req.Header.Set(AccessDelegationHeader, VendedCredentialsDelegation)
		rec := httptest.NewRecorder()
		f.router.ServeHTTP(rec, req)
		require.Nil(t, storageCredentials(t, rec))
	})

	t.Run("a create vends a write session scoped to the new table", func(t *testing.T) {
		f := newCredsTablesFixture(t)
		for _, members := range [][]string{nil, {`"stage-create": true`}} {
			name := "vended_create"
			if members != nil {
				name = "vended_staged_create"
			}
			rec := f.send(t, http.MethodPost, tablesCreateTarget(tablesNamespaceParam(tablesTestRef, Namespace{"sales"})),
				"carol", VendedCredentialsDelegation, createTableBody(name, createTableSchema, members...))
			credentials := storageCredentials(t, rec)
			f.requireSession(t, credentials, true)

			body := decodeLoadTable(t, rec)
			var tableUUID, location string
			require.NoError(t, json.Unmarshal(body.Metadata["table-uuid"], &tableUUID))
			require.NoError(t, json.Unmarshal(body.Metadata["location"], &location))
			require.Equal(t, tableUUID, f.createdUUID, "%s: the session is scoped to the created table", name)
			require.Equal(t, location, f.createdLocation, name)
		}
	})

	t.Run("the credentials endpoint vends a write session on a branch", func(t *testing.T) {
		f := newCredsTablesFixture(t)
		target := tablesLoadTarget(onMain, "orders", nil) + "/credentials"
		rec := f.send(t, http.MethodGet, target, "carol", VendedCredentialsDelegation, "")
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var response LoadCredentialsResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		f.requireSession(t, response.StorageCredentials, true)

		// A reader, and a writer on a tag, refresh a read session.
		for _, request := range []struct{ target, user string }{
			{target, "alice"},
			{tablesLoadTarget(onTag, "orders", nil) + "/credentials", "carol"},
		} {
			rec = f.send(t, http.MethodGet, request.target, request.user, VendedCredentialsDelegation, "")
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			f.requireSession(t, response.StorageCredentials, false)
		}
	})
}
