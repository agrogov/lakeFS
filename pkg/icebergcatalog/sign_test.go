package icebergcatalog

// Tests for the remote-signing endpoint (ICEBERG_CATALOG_PLAN.md §8, §2.1 "Access delegation",
// §7.2 "Authorization"). Every test name starts with TestRemoteSigning so that the task's
// validation command, `go test -run TestRemoteSigning ./pkg/icebergcatalog/`, runs exactly this
// file.
//
// The authorization half uses the same simplified-ACL service as authz_test.go, so a read grant
// and a write grant are real policy decisions rather than a stubbed "allow". The storage half is
// a recording block adapter: the signing handler is exercised end to end through the production
// middleware stack, and the recording adapter proves both which URI was sent to the presigner and
// that the location check refused a foreign URI before the presigner was called.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// signTestRepo, signTestRef and signTestTable are the table address of authz_test.go's §7.2
	// fixture (arn:lakefs:catalog:::table/lakehouse/inventory.books/titles).
	signTestRepo  = authzRepo
	signTestRef   = authzRef
	signTestTable = "titles"

	// signTestLocation is the `location` of the fixture table: the prefix every signed URI must
	// stay under.
	signTestLocation = "s3://lakehouse-data/_iceberg/9c12d441-03fe-4693-9a96-a0705ddf69c1"

	// signTestSignedURL is what the fake block adapter's presigner returns.
	signTestSignedURL = "https://signed.example/object?X-Amz-Signature=test"
)

// signingCall is one invocation of the block adapter's presigner.
type signingCall struct {
	identifier string
	mode       block.PreSignMode
}

// signingAdapter is the recording block adapter a signing request reaches. It reports the
// blockstore type and presign capability the way the S3 adapter does, so the same code decides
// whether remote signing is enabled, and it records every URL it is asked to sign.
type signingAdapter struct {
	block.Adapter

	blockstoreType   string
	preSignSupported bool
	signed           string
	signErr          error
	calls            []signingCall
}

func (a *signingAdapter) BlockstoreType() string { return a.blockstoreType }

func (a *signingAdapter) GetStorageNamespaceInfo(string) *block.StorageNamespaceInfo {
	return &block.StorageNamespaceInfo{PreSignSupport: a.preSignSupported}
}

func (a *signingAdapter) GetPreSignedURL(_ context.Context, obj block.ObjectPointer, mode block.PreSignMode, _ string) (string, time.Time, error) {
	a.calls = append(a.calls, signingCall{identifier: obj.Identifier, mode: mode})
	if a.signErr != nil {
		return "", time.Time{}, a.signErr
	}
	signed := a.signed
	if signed == "" {
		signed = signTestSignedURL
	}
	return signed, time.Now().Add(time.Hour), nil
}

// signingEnabledAdapter is an S3 blockstore with a working presigner: remote signing is on.
func signingEnabledAdapter() *signingAdapter {
	return &signingAdapter{
		blockstoreType:   block.BlockstoreTypeS3,
		preSignSupported: true,
	}
}

// signRefResolver answers the Q-I4 branch question for the fixture: only main is a branch, so a
// request to a tag is read-only.
type signRefResolver struct {
	branches map[string]bool
}

func (r signRefResolver) GetBranchReference(_ context.Context, repositoryID, branch string) (string, error) {
	if r.branches[repositoryID+"/"+branch] {
		return "branch-reference", nil
	}
	return "", graveler.ErrBranchNotFound
}

func signingMainRefResolver() signRefResolver {
	return signRefResolver{branches: map[string]bool{signTestRepo + "/" + signTestRef: true}}
}

// newTestSigningService builds the signing service with a fixed table location, so the tests can
// focus on the URI check and the authorization matrix without materializing metadata.json.
func newTestSigningService(adapter block.Adapter, authz *Authorizer, location string, refs RefResolver) *remoteSigningService {
	return &remoteSigningService{
		locate: func(context.Context, Address, AuthorizeFunc) (string, error) {
			return location, nil
		},
		authz:   authz,
		adapter: adapter,
		refs:    refs,
		prefix:  CatalogPrefix,
		enabled: RemoteSigningSupported(adapter),
	}
}

// newTestSigningRouter mounts the service's routes behind the production middleware stack.
func newTestSigningRouter(t *testing.T, service *remoteSigningService) http.Handler {
	t.Helper()
	router, err := newRouter(namespacesUser, &Config{}, testServer(&Config{Prefix: CatalogPrefix}, service.Routes()...))
	require.NoError(t, err, "build catalog router")
	return router
}

// signTarget is the signing URL of one ref of the fixture table.
func signTarget(ref string) string {
	return "/v1/" + CatalogPrefix + "/namespaces/" + nodes(signTestRepo, ref, "inventory", "books") +
		"/tables/" + signTestTable + "/sign"
}

// postSign sends one signing request through router. An empty delegation means "no header".
func postSign(t *testing.T, router http.Handler, user, ref, uri, method, delegation string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(RemoteSignRequest{
		Region:  "us-east-1",
		URI:     uri,
		Method:  method,
		Headers: map[string][]string{},
	})
	require.NoError(t, err, "encode signing request")
	req := httptest.NewRequest(http.MethodPost, signTarget(ref), strings.NewReader(string(body)))
	req.Header.Set("Content-Type", contentTypeJSON)
	req.Header.Set("X-Test-User", user)
	if delegation != "" {
		req.Header.Set(AccessDelegationHeader, delegation)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// requireSigned asserts a successful signing response and returns its body.
func requireSigned(t *testing.T, rec *httptest.ResponseRecorder) RemoteSignResult {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var result RemoteSignResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result), "body: %s", rec.Body.String())
	require.Equal(t, signTestSignedURL, result.URI)
	require.NotNil(t, result.Headers, "headers is a required field of RemoteSignResult")
	return result
}

func TestRemoteSigning(t *testing.T) {
	t.Run("signs a GET under the table location", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
		router := newTestSigningRouter(t, service)

		uri := signTestLocation + "/data/file.parquet"
		rec := postSign(t, router, "alice", signTestRef, uri, http.MethodGet, RemoteSigningDelegation)

		requireSigned(t, rec)
		require.Equal(t, []signingCall{{identifier: uri, mode: block.PreSignModeRead}}, adapter.calls,
			"the blockstore must sign exactly the requested URI with its read presigner")
	})

	t.Run("signs a PUT under the table location", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		authz := newAuthzUser(t, authzWriteStatements())
		service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
		router := newTestSigningRouter(t, service)

		uri := signTestLocation + "/data/file.parquet"
		rec := postSign(t, router, "alice", signTestRef, uri, http.MethodPut, RemoteSigningDelegation)

		requireSigned(t, rec)
		require.Equal(t, []signingCall{{identifier: uri, mode: block.PreSignModeWrite}}, adapter.calls,
			"the blockstore must sign exactly the requested URI with its write presigner")
	})

	t.Run("requires ReadTableData for reads", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		// Write statements deliberately lack ReadTableData here, so a GET cannot borrow a write
		// grant. authzWriteStatements includes the read half normally; this is that set minus the
		// one action the GET needs.
		authz := newAuthzUser(t, authzGrantsExceptAction(permissions.CatalogReadTableDataAction))
		service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
		router := newTestSigningRouter(t, service)

		rec := postSign(t, router, "alice", signTestRef, signTestLocation+"/data/file.parquet",
			http.MethodGet, RemoteSigningDelegation)

		requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)
		require.Empty(t, adapter.calls, "an unauthorized request must not reach the presigner")
	})

	t.Run("requires WriteTableData for writes", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
		router := newTestSigningRouter(t, service)

		rec := postSign(t, router, "alice", signTestRef, signTestLocation+"/data/file.parquet",
			http.MethodPut, RemoteSigningDelegation)

		requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)
		require.Empty(t, adapter.calls, "an unauthorized request must not reach the presigner")
	})

	t.Run("a write on a tag is read-only", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		authz := newAuthzUser(t, authzWriteStatements())
		service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
		router := newTestSigningRouter(t, service)

		rec := postSign(t, router, "alice", "release", signTestLocation+"/data/file.parquet",
			http.MethodPut, RemoteSigningDelegation)

		apiError := requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)
		require.Equal(t, ErrRefReadOnly.Error(), apiError.Message)
		require.Empty(t, adapter.calls, "a read-only ref must not reach the presigner")
	})

	t.Run("rejects a URI outside the table location", func(t *testing.T) {
		cases := []struct {
			name    string
			uri     string
			status  int
			errType string
		}{
			{
				name:    "another bucket",
				uri:     "s3://other-bucket/_iceberg/9c12d441-03fe-4693-9a96-a0705ddf69c1/data/file.parquet",
				status:  http.StatusForbidden,
				errType: ErrorTypeForbidden,
			},
			{
				name:    "outside the table prefix",
				uri:     "s3://lakehouse-data/_iceberg/some-other-table/data/file.parquet",
				status:  http.StatusForbidden,
				errType: ErrorTypeForbidden,
			},
			{
				name:    "sibling prefix",
				uri:     signTestLocation + "-other/data/file.parquet",
				status:  http.StatusForbidden,
				errType: ErrorTypeForbidden,
			},
			{
				name:    "traversal with ..",
				uri:     signTestLocation + "/../secret",
				status:  http.StatusBadRequest,
				errType: ErrTypeBadRequest,
			},
			{
				name:    "encoded traversal",
				uri:     signTestLocation + "/%2e%2e/secret",
				status:  http.StatusBadRequest,
				errType: ErrTypeBadRequest,
			},
			{
				name:    "non-s3 scheme",
				uri:     "https://lakehouse-data/_iceberg/9c12d441-03fe-4693-9a96-a0705ddf69c1/data/file.parquet",
				status:  http.StatusBadRequest,
				errType: ErrTypeBadRequest,
			},
			{
				name:    "query string",
				uri:     signTestLocation + "/data/file.parquet?versionId=1",
				status:  http.StatusBadRequest,
				errType: ErrTypeBadRequest,
			},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				adapter := signingEnabledAdapter()
				authz := newAuthzUser(t, authzReadStatements())
				service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
				router := newTestSigningRouter(t, service)

				rec := postSign(t, router, "alice", signTestRef, tt.uri, http.MethodGet, RemoteSigningDelegation)

				requireEnvelope(t, rec, tt.status, tt.errType)
				require.Empty(t, adapter.calls,
					"the location check must run before the presigner: %s was signed", tt.uri)
			})
		}
	})

	t.Run("allows the table location itself and an s3a spelling", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
		router := newTestSigningRouter(t, service)

		rec := postSign(t, router, "alice", signTestRef, signTestLocation, http.MethodGet, RemoteSigningDelegation)
		requireSigned(t, rec)
		require.Equal(t, signTestLocation, adapter.calls[0].identifier)

		adapter.calls = nil
		s3aURI := strings.Replace(signTestLocation, "s3://", "s3a://", 1) + "/data/file.parquet"
		rec = postSign(t, router, "alice", signTestRef, s3aURI, http.MethodGet, RemoteSigningDelegation)
		requireSigned(t, rec)
		require.Equal(t, signTestLocation+"/data/file.parquet", adapter.calls[0].identifier,
			"the s3a spelling is normalized to the s3 URI the block adapter signs")
	})

	t.Run("refuses a request that asked for a different delegation", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
		router := newTestSigningRouter(t, service)
		uri := signTestLocation + "/data/file.parquet"

		for _, delegation := range []string{"vended-credentials", "none"} {
			rec := postSign(t, router, "alice", signTestRef, uri, http.MethodGet, delegation)
			requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		}
		require.Empty(t, adapter.calls)

		// No header at all is a client calling the signing endpoint directly; that is allowed.
		rec := postSign(t, router, "alice", signTestRef, uri, http.MethodGet, "")
		requireSigned(t, rec)
		require.Len(t, adapter.calls, 1)
	})

	t.Run("refuses methods the blockstore cannot sign", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		authz := newAuthzUser(t, authzWriteStatements())
		service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
		router := newTestSigningRouter(t, service)
		uri := signTestLocation + "/data/file.parquet"

		for _, method := range []string{http.MethodHead, http.MethodDelete, http.MethodPost, "TRACE"} {
			rec := postSign(t, router, "alice", signTestRef, uri, method, RemoteSigningDelegation)
			requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
		}
		require.Empty(t, adapter.calls, "an unsupported method must not reach the presigner")
	})

	t.Run("a missing table is 404", func(t *testing.T) {
		adapter := signingEnabledAdapter()
		authz := newAuthzUser(t, authzReadStatements())
		service := &remoteSigningService{
			locate: func(context.Context, Address, AuthorizeFunc) (string, error) {
				return "", &NoSuchTableException{
					Repo: signTestRepo, Ref: signTestRef,
					Namespace: Namespace{"inventory", "books"}, Table: signTestTable,
				}
			},
			authz:   authz.authz,
			adapter: adapter,
			refs:    signingMainRefResolver(),
			prefix:  CatalogPrefix,
			enabled: true,
		}
		router := newTestSigningRouter(t, service)

		rec := postSign(t, router, "alice", signTestRef, signTestLocation+"/data/file.parquet",
			http.MethodGet, RemoteSigningDelegation)

		requireEnvelope(t, rec, http.StatusNotFound, errTypeNoSuchTable)
		require.Empty(t, adapter.calls)
	})

	t.Run("advertises the endpoint only when the blockstore can sign", func(t *testing.T) {
		enabled := signingEnabledAdapter()
		require.True(t, RemoteSigningSupported(enabled))
		service := newTestSigningService(enabled, nil, signTestLocation, nil)
		routes := service.Routes()
		require.Len(t, routes, 1)
		require.Equal(t, remoteSigningEndpoint, routes[0].endpoint)
		require.Contains(t, advertisedEndpoints(routes), remoteSigningEndpoint)

		disabledPreSigner := &signingAdapter{
			blockstoreType:   block.BlockstoreTypeS3,
			preSignSupported: false,
		}
		require.False(t, RemoteSigningSupported(disabledPreSigner))
		require.Empty(t, newTestSigningService(disabledPreSigner, nil, signTestLocation, nil).Routes())

		notS3 := &signingAdapter{
			blockstoreType:   block.BlockstoreTypeMem,
			preSignSupported: true,
		}
		require.False(t, RemoteSigningSupported(notS3))
		require.Empty(t, newTestSigningService(notS3, nil, signTestLocation, nil).Routes())
	})

	t.Run("advertises s3.remote-signing-enabled only when requested", func(t *testing.T) {
		service := newTestSigningService(signingEnabledAdapter(), nil, signTestLocation, nil)
		want := map[string]string{RemoteSigningEnabledProperty: "true"}

		require.Equal(t, want, service.TableConfig("remote-signing"))
		require.Equal(t, want, service.TableConfig("vended-credentials,remote-signing"))
		require.Equal(t, want, service.TableConfig("vended-credentials, remote-signing"))
		require.Nil(t, service.TableConfig(""))
		require.Nil(t, service.TableConfig("vended-credentials"))
		require.Nil(t, service.TableConfig("none"))

		disabled := newTestSigningService(&signingAdapter{
			blockstoreType:   block.BlockstoreTypeS3,
			preSignSupported: false,
		}, nil, signTestLocation, nil)
		require.Nil(t, disabled.TableConfig("remote-signing"),
			"a blockstore without a presigner must not advertise remote signing")
	})
}

// TestRemoteSigningNonS3 pins the OSS decision that remote signing is S3 only. An Azure or GCS
// blockstore gets no service, no route and no advertisement, and a handler that is reached anyway
// refuses with a 501 that names the S3 requirement. A non-S3 *location* on an S3 deployment is a
// 400 naming the scheme, refused before the presigner is called - never silently signed.
func TestRemoteSigningNonS3(t *testing.T) {
	for _, blockstore := range []string{block.BlockstoreTypeAzure, block.BlockstoreTypeGS} {
		t.Run(blockstore, func(t *testing.T) {
			adapter := &signingAdapter{blockstoreType: blockstore, preSignSupported: true}
			require.False(t, RemoteSigningSupported(adapter),
				"remote signing is S3 only, so a %s blockstore must not be signed for", blockstore)

			service := newTestSigningService(adapter, nil, signTestLocation, nil)
			require.False(t, service.Enabled())
			require.Empty(t, service.Routes(), "a service that cannot sign must not mount its route")

			// The router answer: the endpoint does not exist on this deployment.
			handler := newTestSigningRouter(t, service)
			requireEnvelope(t, postSign(t, handler, "alice", signTestRef,
				signTestLocation+"/data/file.parquet", http.MethodGet, RemoteSigningDelegation),
				http.StatusNotFound, ErrTypeNotFound)

			// The handler answer, should anything reach it directly: a clear 501 naming S3.
			rec := httptest.NewRecorder()
			service.handleSign(rec, httptest.NewRequest(http.MethodPost, signTarget(signTestRef), nil))
			apiError := requireEnvelope(t, rec, http.StatusNotImplemented, ErrTypeNotImplemented)
			require.Equal(t, RemoteSigningUnsupportedMessage, apiError.Message)
			require.Contains(t, apiError.Message, "S3", "the refusal names the supported blockstore")
		})
	}

	// An S3 deployment is still asked to sign something outside S3. Both the Azure and the GCS
	// spelling of that location are a 400 with the scheme in the message, and the presigner is
	// never called.
	for _, uri := range []string{
		"abfs://lakehouse-data/_iceberg/9c12d441-03fe-4693-9a96-a0705ddf69c1/data/file.parquet",
		"gs://lakehouse-data/_iceberg/9c12d441-03fe-4693-9a96-a0705ddf69c1/data/file.parquet",
	} {
		t.Run(uri, func(t *testing.T) {
			adapter := signingEnabledAdapter()
			authz := newAuthzUser(t, authzReadStatements())
			service := newTestSigningService(adapter, authz.authz, signTestLocation, signingMainRefResolver())
			handler := newTestSigningRouter(t, service)

			rec := postSign(t, handler, "alice", signTestRef, uri, http.MethodGet, RemoteSigningDelegation)
			apiError := requireEnvelope(t, rec, http.StatusBadRequest, ErrTypeBadRequest)
			require.Contains(t, apiError.Message, "is not s3", "the refusal names the unsupported location")
			require.Empty(t, adapter.calls, "a non-S3 location must never reach the presigner")
		})
	}
}
