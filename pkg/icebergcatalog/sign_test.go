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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
		resolve: func(_ context.Context, addr Address, _ AuthorizeFunc) (ResolvedTable, error) {
			return ResolvedTable{Target: addr, Authz: addr}, nil
		},
		locationOf: func(context.Context, Address) (string, error) { return location, nil },
		authz:      authz,
		adapter:    adapter,
		refs:       refs,
		prefix:     CatalogPrefix,
		enabled:    RemoteSigningSupported(adapter),
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
	return signTargetFor(nodes(signTestRepo, ref, "inventory", "books"), signTestTable)
}

// signTargetFor is the signing URL of one namespace and table, e.g. the virtual table the table
// resolver redirects, which does not live in the fixture's own namespace.
func signTargetFor(namespaceParam, table string) string {
	return "/v1/" + CatalogPrefix + "/namespaces/" + namespaceParam +
		"/tables/" + url.PathEscape(table) + "/sign"
}

// postSign sends one signing request through router. An empty delegation means "no header".
func postSign(t *testing.T, router http.Handler, user, ref, uri, method, delegation string) *httptest.ResponseRecorder {
	t.Helper()
	return postSignAt(t, router, signTarget(ref), user, uri, method, delegation)
}

// postSignAt sends one signing request to an explicit signing URL.
func postSignAt(t *testing.T, router http.Handler, target, user, uri, method, delegation string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(RemoteSignRequest{
		Region:  "us-east-1",
		URI:     uri,
		Method:  method,
		Headers: map[string][]string{},
	})
	require.NoError(t, err, "encode signing request")
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(string(body)))
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
			resolve: func(context.Context, Address, AuthorizeFunc) (ResolvedTable, error) {
				return ResolvedTable{}, &NoSuchTableException{
					Repo: signTestRepo, Ref: signTestRef,
					Namespace: Namespace{"inventory", "books"}, Table: signTestTable,
				}
			},
			locationOf: func(context.Context, Address) (string, error) { return signTestLocation, nil },
			authz:      authz.authz,
			adapter:    adapter,
			refs:       signingMainRefResolver(),
			prefix:     CatalogPrefix,
			enabled:    true,
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

// ---------------------------------------------------------------------------------------------
// The reserved namespace (§5.4 on the data plane)
// ---------------------------------------------------------------------------------------------

const (
	// signWritableTable is an ordinary table of the data repository, in an ordinary namespace,
	// whose files are addressable at an s3:// location: the write sign this fix must keep granting.
	signWritableTable = "signing_target"

	// signWritableUUID is the table uuid of signWritableTable.
	signWritableUUID = "77777777-7777-7777-7777-777777777777"
)

// signingObjectStore is the object store the resolver fixture's tables live in: a minimal fake S3
// adapter that keeps metadata.json documents keyed by their full s3:// URI.
//
// Remote signing is S3 only (parseS3Location refuses every other scheme), so the tables of this
// fixture have to live at s3:// locations. The tables fixture's own repository is backed by the
// in-memory block adapter, whose identifiers are mem://; a store built over this adapter reads the
// s3:// metadata.json documents instead. Only the methods the signing fixture uses are
// implemented: the embedded block.Adapter is nil, and every other method is a fixture bug rather
// than a behaviour under test.
type signingObjectStore struct {
	block.Adapter

	objects map[string][]byte
}

func newSigningObjectStore() *signingObjectStore {
	return &signingObjectStore{objects: make(map[string][]byte)}
}

func (s *signingObjectStore) BlockstoreType() string { return block.BlockstoreTypeS3 }

func (s *signingObjectStore) GetStorageNamespaceInfo(string) *block.StorageNamespaceInfo {
	return &block.StorageNamespaceInfo{PreSignSupport: true}
}

func (s *signingObjectStore) Get(_ context.Context, obj block.ObjectPointer) (io.ReadCloser, error) {
	document, ok := s.objects[obj.Identifier]
	if !ok {
		return nil, fmt.Errorf("%w: %s", block.ErrDataNotFound, obj.Identifier)
	}
	return io.NopCloser(bytes.NewReader(document)), nil
}

// put stores one document at its full identifier location, the way an Iceberg writer puts a
// metadata.json into the table.
func (s *signingObjectStore) put(location, document string) {
	s.objects[location] = []byte(document)
}

// signResolverFixture is the signing endpoint on a catalog with a table resolver: the data
// repository of tables_test.go, the Metadata Search companion repository R-metadata with the
// object_metadata table the resolver redirects the virtual table to, and one ordinary table of the
// data repository at an s3:// location.
type signResolverFixture struct {
	*tablesFixture

	// store resolves addresses through the built-in Metadata Search resolver; objects is the fake
	// S3 object store the metadata.json documents live in.
	store   *Store
	objects *signingObjectStore

	// adapter is the recording S3 presigner the signing endpoint uses, router its mounted routes.
	adapter *signingAdapter
	router  http.Handler

	// metaRepo is the companion repository, metaTableLocation the `location` of its
	// object_metadata table, and metaDataURI an object below it.
	metaRepo          string
	metaTableLocation string
	metaDataURI       string

	// writableTarget and writableURI are the signing URL of the ordinary table and an object URI
	// the write sign for it must still grant.
	writableTarget string
	writableURI    string
}

// newSignResolverFixture builds the two-repository fixture. It is newResolverFixture of
// resolver_test.go with the one change remote signing needs: the tables live at s3:// locations,
// which the fake S3 object store serves.
func newSignResolverFixture(t *testing.T) *signResolverFixture {
	t.Helper()
	base := newTablesFixture(t)
	ctx := base.ctx
	objects := newSigningObjectStore()
	store := NewStore(base.catalog, objects, WithTableResolver(NewMetadataSearchResolver(base.catalog, nil)))
	seq := tablesFixtureSeq.Add(1)

	// The state the indexer leaves behind (§3.2): the companion repository with a system namespace
	// and the object_metadata table on the mirrored branch.
	metaRepo := tablesTestRepo + MetadataRepositorySuffix
	metaStorageNamespace := fmt.Sprintf("s3://iceberg-sign-mds-%d-ns", seq)
	_, err := base.catalog.CreateRepository(ctx, metaRepo, "", metaStorageNamespace, tablesTestRef, false)
	require.NoError(t, err, "create the companion repository")
	metaNamespaceAddr := Address{Repo: metaRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}}
	require.NoError(t, store.CreateNamespace(ctx, metaNamespaceAddr, map[string]string{"owner": "mds"}),
		"create the system namespace of the companion repository")
	metaTableLocation := strings.TrimRight(metaStorageNamespace, "/") + "/_iceberg/" + resolverMetaUUID
	objects.put(metaTableLocation+"/metadata/00001.metadata.json",
		tablesTestMetadata(resolverMetaUUID, metaTableLocation))
	metaTableAddr := metaNamespaceAddr
	metaTableAddr.Table = resolverObjectMetadataTable
	require.NoError(t, store.CreateTable(ctx, metaTableAddr, TablePointer{
		MetadataLocation: metaTableLocation + "/metadata/00001.metadata.json",
		TableUUID:        resolverMetaUUID,
		FormatVersion:    2,
	}), "create system.object_metadata in the companion repository")

	// The ordinary table a write sign must keep granting: a normal namespace of the data
	// repository, at an s3:// location.
	writableTableLocation := fmt.Sprintf("s3://iceberg-sign-data-%d/_iceberg/%s", seq, signWritableUUID)
	objects.put(writableTableLocation+"/metadata/00001.metadata.json",
		tablesTestMetadata(signWritableUUID, writableTableLocation))
	require.NoError(t, store.CreateTable(ctx, Address{
		Repo: tablesTestRepo, Ref: tablesTestRef,
		Namespace: Namespace{"inventory", "books"}, Table: signWritableTable,
	}, TablePointer{
		MetadataLocation: writableTableLocation + "/metadata/00001.metadata.json",
		TableUUID:        signWritableUUID,
		FormatVersion:    2,
	}), "create the ordinary table")

	adapter := signingEnabledAdapter()
	service := NewRemoteSigningService(store, NewAuthorizer(base.authz.svc), adapter, base.catalog,
		&Config{Prefix: CatalogPrefix})
	require.True(t, service.Enabled(), "the fake blockstore can sign")

	return &signResolverFixture{
		tablesFixture:     base,
		store:             store,
		objects:           objects,
		adapter:           adapter,
		router:            newTestSigningRouter(t, service),
		metaRepo:          metaRepo,
		metaTableLocation: metaTableLocation,
		metaDataURI:       metaTableLocation + "/data/00000-0-" + resolverMetaUUID + ".parquet",
		writableTarget: signTargetFor(nodes(tablesTestRepo, tablesTestRef, "inventory", "books"),
			signWritableTable),
		writableURI: writableTableLocation + "/data/00000-0-" + signWritableUUID + ".parquet",
	}
}

// signRedirectResolver is the minimal TableResolver of a resolver that hides a catalog-managed
// table behind an address of its own: it sends one address to another and reserves the namespace of
// the target. It is how this file proves the write refusal follows a redirect instead of trusting
// the namespace the caller named.
type signRedirectResolver struct {
	from     Address
	to       Address
	reserved Address // Repo, Ref and Namespace of the namespace to reserve
}

func (r signRedirectResolver) Resolve(_ context.Context, addr Address) (ResolvedTable, bool, error) {
	if !sameAddress(addr, r.from) {
		return ResolvedTable{}, false, nil
	}
	return ResolvedTable{Target: r.to, Authz: addr}, true, nil
}

func (r signRedirectResolver) VirtualNamespaces(Address) []string { return nil }

func (r signRedirectResolver) VirtualTables(Address) []string { return nil }

func (r signRedirectResolver) ReservedNamespace(addr Address) bool {
	return addr.Repo == r.reserved.Repo && addr.Ref == r.reserved.Ref &&
		addr.Namespace.Dotted() == r.reserved.Namespace.Dotted()
}

// TestRemoteSigningReservedNamespace pins the third check of the signing endpoint (§5.4,
// resolver.go): a write signature is refused for a namespace a table resolver reserved, both on the
// address the caller named and on the address the resolver redirects it to, while a read signature
// for the same virtual table and a write signature for an ordinary table are still granted. The
// first case fails before the fix: the write used to be authorized on the data repository and then
// signed under the companion repository's location.
func TestRemoteSigningReservedNamespace(t *testing.T) {
	f := newSignResolverFixture(t)
	virtualTarget := signTargetFor(nodes(tablesTestRepo, tablesTestRef, SystemNamespace),
		resolverObjectMetadataTable)
	reservedMessage := (&ReservedNamespaceError{
		Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace},
	}).Error()

	t.Run("a write sign on the reserved virtual address is refused", func(t *testing.T) {
		// Every write method of the spec, not only the one the adapter can sign: the refusal is the
		// namespace, so it precedes "the blockstore cannot sign this".
		for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch} {
			f.adapter.calls = nil
			rec := postSignAt(t, f.router, virtualTarget, "carol", f.metaDataURI, method, RemoteSigningDelegation)
			apiError := requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)
			require.Equal(t, reservedMessage, apiError.Message, "method %s", method)
			require.Empty(t, f.adapter.calls,
				"a reserved namespace must not reach the presigner: %s", method)
		}
	})

	t.Run("a read sign on the reserved virtual address is still granted", func(t *testing.T) {
		f.adapter.calls = nil
		rec := postSignAt(t, f.router, virtualTarget, "alice", f.metaDataURI, http.MethodGet, RemoteSigningDelegation)
		requireSigned(t, rec)
		require.Equal(t, []signingCall{{identifier: f.metaDataURI, mode: block.PreSignModeRead}}, f.adapter.calls,
			"§5.4: the virtual table is readable; only writes are refused")
	})

	t.Run("a write sign on an ordinary table is still granted", func(t *testing.T) {
		f.adapter.calls = nil
		rec := postSignAt(t, f.router, f.writableTarget, "carol", f.writableURI, http.MethodPut, RemoteSigningDelegation)
		requireSigned(t, rec)
		require.Equal(t, []signingCall{{identifier: f.writableURI, mode: block.PreSignModeWrite}}, f.adapter.calls)
	})

	t.Run("a redirect into a reserved namespace is refused", func(t *testing.T) {
		// The requested address is ordinary; the resolver sends it to a table in a namespace it
		// reserves. The refusal has to follow the redirect, or naming a front address would dodge
		// the check the reserved address itself gets.
		front := Address{
			Repo: tablesTestRepo, Ref: tablesTestRef,
			Namespace: Namespace{"inventory", "books"}, Table: signWritableTable,
		}
		target := Address{
			Repo: f.metaRepo, Ref: tablesTestRef,
			Namespace: Namespace{SystemNamespace}, Table: resolverObjectMetadataTable,
		}
		store := NewStore(f.catalog, f.objects, WithTableResolver(signRedirectResolver{
			from: front, to: target, reserved: target,
		}))
		adapter := signingEnabledAdapter()
		service := NewRemoteSigningService(store, NewAuthorizer(f.authz.svc), adapter, f.catalog,
			&Config{Prefix: CatalogPrefix})
		router := newTestSigningRouter(t, service)

		rec := postSignAt(t, router, f.writableTarget, "carol", f.metaDataURI, http.MethodPut, RemoteSigningDelegation)
		apiError := requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)
		require.Equal(t, (&ReservedNamespaceError{
			Repo: target.Repo, Ref: target.Ref, Namespace: target.Namespace,
		}).Error(), apiError.Message)
		require.Empty(t, adapter.calls, "a redirect into a reserved namespace must not reach the presigner")

		// The same redirect is readable for a read sign: the reservation is a write rule only.
		rec = postSignAt(t, router, f.writableTarget, "alice", f.metaDataURI, http.MethodGet, RemoteSigningDelegation)
		requireSigned(t, rec)
		require.Equal(t, []signingCall{{identifier: f.metaDataURI, mode: block.PreSignModeRead}}, adapter.calls)
	})
}
