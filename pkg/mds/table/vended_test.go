package table_test

// Vended mode of METADATA_SEARCH_DESIGN.md §4.6 (METADATA_SEARCH_PLAN.md risk "No remote signing in
// iceberg-go: deployments without credential vending must give the indexer object-store keys
// (storage). Document it, and test both modes where possible").
//
// The static mode is what every other test of this package runs: metadata_search.storage supplies
// the iceberg-go IO properties (s3.endpoint, s3.access-key-id, ...) with rest.WithAdditionalProps.
// This file proves the other mode - metadata_search.storage empty, the indexer's iceberg-go client
// gets its write credentials from the catalog - end to end against the production catalog routes:
//
//	client catalog (table.NewCatalog, Storage nil)
//	    → POST /iceberg/relative_to/<R-metadata>.<b>/api/v1/.../tables  (create)
//	    → POST .../tables/object_metadata                               (the delta's commit)
//	    → the catalog assumes blockstore.s3.credentials_vending.role_arn through STS and answers
//	      with a session scoped to <storage namespace>/_iceberg/<table-uuid>/
//	    → iceberg-go builds the table's FileIO with the vended s3.access-key-id, s3.secret-access-key
//	      and s3.session-token and writes the delta's Parquet, Avro and metadata.json files with them
//
// The proof has two halves, and both are needed because either one alone can be faked:
//
//   - the catalog really vends. The fake STS records every AssumeRole call, and the test asks
//     GET .../tables/object_metadata/credentials the way a client does and checks that the answer
//     carries the exact session the fake STS issued.
//   - the writes really used the vended session. The iceberg-go IO registry is replaced for the
//     duration of this test binary with a FileIO that records the FileIO properties of every
//     construction and the credentials of every file it create/wrote, then delegates to a real S3
//     FileIO over the fixture's object store. It refuses to build an IO without an access key, so
//     ambient AWS credentials - which the test sets to a value the object store does not know -
//     can never make a write succeed.
//
// The object store is SeaweedFS (esti/ops/docker-compose-mds.yaml), skipped when it is not
// running, because vending is S3 only (§8) and the catalog must stage the files the client wrote.
// SeaweedFS ignores the session token, so the token doubles as the marker of the vended session:
// the object store knows one identity (estiaccesskey/estisecretkey) and nothing else in the fixture
// supplies a token.
//
// Two server-side pieces make this work, and the test fails without either one:
//
//  1. iceberg-go v0.7.0 enables its vended-credentials mode only when the loadTable/createTable
//     response carries `storage-credentials` (catalog/rest/rest.go:
//     `credsVended := len(ret.StorageCredentials) > 0`); it uses the separate
//     GET .../tables/{table}/credentials endpoint only to refresh an already vended session.
//     pkg/icebergcatalog's load and create answers carry the member (tables.go).
//  2. The session is a write session: the load, the create and the credentials endpoint call Vend
//     with OpWriteTableData on a branch for a caller allowed catalog:WriteTableData (creds.go
//     TableCredentials), so the indexer gets write credentials for the Parquet and Avro files of
//     §4.3 step 3.
//
// The client side needs nothing: table.NewCatalog already makes iceberg-go send
// `X-Iceberg-Access-Delegation: vended-credentials` on every request (the session default), which
// the test asserts, so catalog.go needs no extra header.
//
// Run it with SeaweedFS up:
//
//	docker compose -f esti/ops/docker-compose-mds.yaml up -d
//	go test -count=1 -race -run TestVendedCredentials ./pkg/mds/table/

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/io/gocloud/blobfs"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/s3blob"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

const (
	// vendedS3EndpointEnv and vendedS3DefaultEndpoint name the SeaweedFS S3 gateway the fixture
	// writes to: the one esti/ops/docker-compose-mds.yaml publishes on 127.0.0.1:8333.
	vendedS3EndpointEnv     = "LAKEFS_MDS_VENDED_S3_ENDPOINT"
	vendedS3DefaultEndpoint = "http://127.0.0.1:8333"
	vendedS3Region          = "us-east-1"

	// The SeaweedFS identity of esti/ops/seaweedfs-s3.json, the one the blockstore of the fixture
	// uses. It is also what the fake STS vends, because the object store knows no other identity;
	// the session token below is what distinguishes the vended session.
	vendedS3AccessKeyID     = "estiaccesskey"      //nolint:gosec // a test fixture's S3 access key, not a secret
	vendedS3SecretAccessKey = "estisecretkey"      //nolint:gosec // a test fixture's S3 secret key, not a secret
	vendedSessionToken      = "mds-vended-session" //nolint:gosec // a marker of the vended session, not a secret

	// The ambient credentials the test puts in the environment. The fake STS client signs its
	// AssumeRole call with them (any value signs), and the object store does not know them: an IO
	// that fell back to the process environment instead of the vended session cannot write.
	vendedAmbientAccessKeyID     = "ambient-aws-key-not-configured-here"
	vendedAmbientSecretAccessKey = "ambient-aws-secret-not-configured-here" //nolint:gosec // see above
	vendedAmbientSessionToken    = "ambient-aws-session-not-configured-here"

	// vendedRoleARN is the role the catalog assumes. The fixture's fake STS answers for it, so the
	// ARN only has to satisfy the setting's ARN check.
	vendedRoleARN = "arn:aws:iam::123456789012:role/lakefs-mds-vended"

	// vendedDialTimeout bounds the liveness check of the object store, so a stopped SeaweedFS is
	// a skip in seconds instead of a hung test.
	vendedDialTimeout = 2 * time.Second

	// vendedRequestTimeout bounds one inspection request of the test (GET /v1/config, GET the
	// credentials endpoint), so a wedged server fails the test in seconds.
	vendedRequestTimeout = 10 * time.Second

	// vendedCleanupTimeout bounds the bucket sweep of t.Cleanup.
	vendedCleanupTimeout = 30 * time.Second
)

// vendedS3Store is the object store of the fixture: a bucket in a running SeaweedFS, and the
// credentials the server-side blockstore uses.
type vendedS3Store struct {
	endpoint string
	bucket   string
	client   *s3.Client
}

// newVendedS3Store skips the test when no S3-compatible store answers on the endpoint, and creates
// the bucket the fixture writes into. The bucket is swept and deleted in t.Cleanup, so a failed run
// leaves nothing behind.
func newVendedS3Store(t *testing.T) *vendedS3Store {
	t.Helper()
	endpoint := os.Getenv(vendedS3EndpointEnv)
	if endpoint == "" {
		endpoint = vendedS3DefaultEndpoint
	}
	parsed, err := url.Parse(endpoint)
	require.NoErrorf(t, err, "parse the S3 endpoint %q", endpoint)
	conn, err := net.DialTimeout("tcp", parsed.Host, vendedDialTimeout)
	if err != nil {
		t.Skipf("no S3 endpoint at %s (%v): start the object store of the vended-credentials test with\n"+
			"\tdocker compose -f esti/ops/docker-compose-mds.yaml up -d\n"+
			"or set %s to another S3-compatible endpoint", endpoint, err, vendedS3EndpointEnv)
	}
	require.NoError(t, conn.Close())

	store := &vendedS3Store{endpoint: endpoint, bucket: "mds-vended-" + uuid.NewString()}
	store.client = s3.NewFromConfig(aws.Config{
		Region: vendedS3Region,
		Credentials: awscredentials.NewStaticCredentialsProvider(
			vendedS3AccessKeyID, vendedS3SecretAccessKey, ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	ctx, cancel := context.WithTimeout(t.Context(), vendedCleanupTimeout)
	defer cancel()
	_, err = store.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(store.bucket)})
	require.NoErrorf(t, err, "create the bucket s3://%s", store.bucket)
	t.Cleanup(func() { store.cleanup(t) })
	return store
}

// blockstoreConfig returns the viper keys of an S3 blockstore over the store, and the storage
// namespace of the companion repository the fixture indexes.
func (s *vendedS3Store) blockstoreConfig(repository string) (map[string]any, string) {
	return map[string]any{
		config.BlockstoreTypeKey:                       block.BlockstoreTypeS3,
		"blockstore.s3.endpoint":                       s.endpoint,
		"blockstore.s3.region":                         vendedS3Region,
		"blockstore.s3.force_path_style":               true,
		"blockstore.s3.discover_bucket_region":         false,
		"blockstore.s3.credentials.access_key_id":      vendedS3AccessKeyID,
		"blockstore.s3.credentials.secret_access_key":  vendedS3SecretAccessKey,
		"iceberg.credentials_vending.role_arn":         vendedRoleARN,
		"iceberg.credentials_vending.session_duration": "1h",
	}, "s3://" + s.bucket + "/metadata/" + repository
}

// cleanup removes every object of the bucket and the bucket itself. Failures are logged, not
// failed: a cleanup runs on the way out of a test that may already have failed for its own reason.
func (s *vendedS3Store) cleanup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), vendedCleanupTimeout)
	defer cancel()
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Logf("vended credentials fixture: list s3://%s: %v", s.bucket, err)
			return
		}
		if len(page.Contents) == 0 {
			continue
		}
		objects := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			objects = append(objects, s3types.ObjectIdentifier{Key: object.Key})
		}
		_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &s3types.Delete{Objects: objects},
		})
		if err != nil {
			t.Logf("vended credentials fixture: delete objects of s3://%s: %v", s.bucket, err)
			return
		}
	}
	if _, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(s.bucket)}); err != nil {
		t.Logf("vended credentials fixture: delete bucket s3://%s: %v", s.bucket, err)
	}
}

// stsCall is one recorded AssumeRole request. The session policy is the interesting field: it is
// how a test tells a read session from the write session the indexer needs.
type stsCall struct {
	Action          string
	RoleARN         string
	RoleSessionName string
	Policy          string
	DurationSeconds string
	ExternalID      string
}

// fakeSTS is an in-process STS endpoint. The catalog builds its own STS client (newSTSClient) and
// reaches this server through blockstore.s3.credentials_vending.endpoint, so the production
// vending code path runs unmodified: the request is the AssumeRole call of §8, and the answer is
// the session the catalog hands the client.
type fakeSTS struct {
	server *httptest.Server

	mu    sync.Mutex
	calls []stsCall
}

// stsResponseFormat is an AssumeRoleResponse of the STS query protocol, the shape the AWS SDK
// decodes: the credentials in the FileIO form §4.6 expects, with an expiry far enough out that a
// test never sees a refresh.
const stsResponseFormat = `<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <AssumedRoleUser>
      <AssumedRoleId>AROAMDSVENDED:%[1]s</AssumedRoleId>
      <Arn>arn:aws:sts::123456789012:assumed-role/mds-vended/%[1]s</Arn>
    </AssumedRoleUser>
    <Credentials>
      <AccessKeyId>%[2]s</AccessKeyId>
      <SecretAccessKey>%[3]s</SecretAccessKey>
      <SessionToken>%[4]s</SessionToken>
      <Expiration>%[5]s</Expiration>
    </Credentials>
    <PackedPolicySize>6</PackedPolicySize>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>%[6]s</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>`

func newFakeSTS(t *testing.T) *fakeSTS {
	t.Helper()
	f := &fakeSTS{}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeSTS) handle(w http.ResponseWriter, r *http.Request) {
	// A handler runs outside the test goroutine, so it reports with HTTP status codes rather than
	// with require: a malformed request reaches the catalog as a vending failure, which the test
	// then reports with its own assertions.
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("fake STS: parse the AssumeRole request: %v", err), http.StatusBadRequest)
		return
	}
	call := stsCall{
		Action:          r.Form.Get("Action"),
		RoleARN:         r.Form.Get("RoleArn"),
		RoleSessionName: r.Form.Get("RoleSessionName"),
		Policy:          r.Form.Get("Policy"),
		DurationSeconds: r.Form.Get("DurationSeconds"),
		ExternalID:      r.Form.Get("ExternalId"),
	}
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	if call.Action != "AssumeRole" {
		http.Error(w, fmt.Sprintf("fake STS: unexpected action %q", call.Action), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, stsResponseFormat, call.RoleSessionName, vendedS3AccessKeyID, vendedS3SecretAccessKey,
		vendedSessionToken, time.Now().Add(time.Hour).UTC().Format(time.RFC3339), uuid.NewString())
}

// recorded returns the AssumeRole calls so far.
func (f *fakeSTS) recorded() []stsCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]stsCall(nil), f.calls...)
}

// writeCalls returns the AssumeRole calls whose session policy grants s3:PutObject, i.e. the write
// sessions of §8 that the indexer needs to upload the files of §4.3 step 3.
func (f *fakeSTS) writeCalls() []stsCall {
	var calls []stsCall
	for _, call := range f.recorded() {
		if strings.Contains(call.Policy, "s3:PutObject") {
			calls = append(calls, call)
		}
	}
	return calls
}

// catalogRequest is one request the fixture's HTTP server answered.
type catalogRequest struct {
	Method string
	Path   string
	// Delegation is the X-Iceberg-Access-Delegation header of the request.
	Delegation string
}

// requestLog records the requests iceberg-go sends to the catalog, so the test can assert the
// data-access delegation header without touching the client's internals.
type requestLog struct {
	mu       sync.Mutex
	requests []catalogRequest
}

func (l *requestLog) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		l.requests = append(l.requests, catalogRequest{
			Method:     r.Method,
			Path:       r.URL.Path,
			Delegation: strings.Join(r.Header.Values(icebergcatalog.AccessDelegationHeader), ","),
		})
		l.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (l *requestLog) recorded() []catalogRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]catalogRequest(nil), l.requests...)
}

// fileConstruction is one FileIO iceberg-go built for the table's files: the object-store
// properties it was handed.
type fileConstruction struct {
	AccessKeyID  string
	SessionToken string
	Endpoint     string
}

// fileWrite is one file the table's FileIO created or wrote, with the credentials of the FileIO
// that did it.
type fileWrite struct {
	Name         string
	AccessKeyID  string
	SessionToken string
}

// ioRecorder is the active recorder of the replaced s3 IO scheme (see installS3IORecorder): it
// builds the S3 FileIO from the properties iceberg-go passes, refuses to build one without
// credentials, and keeps the audit trail the test asserts on.
type ioRecorder struct {
	endpoint string

	mu            sync.Mutex
	constructions []fileConstruction
	writes        []fileWrite
	closers       []func() error
}

func (r *ioRecorder) newIO(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
	accessKeyID := props[icebergio.S3AccessKeyID]
	secretAccessKey := props[icebergio.S3SecretAccessKey]
	sessionToken := props[icebergio.S3SessionToken]
	r.mu.Lock()
	r.constructions = append(r.constructions, fileConstruction{
		AccessKeyID:  accessKeyID,
		SessionToken: sessionToken,
		Endpoint:     props[icebergio.S3EndpointURL],
	})
	r.mu.Unlock()
	if accessKeyID == "" || secretAccessKey == "" {
		// The real S3 FileIO would fall back to the ambient AWS chain here, which is exactly the
		// mode design §4.6 rules out for the indexer. Refuse instead, so that a missing vended
		// session cannot be papered over by the credentials the process happens to have.
		return nil, errors.New("iceberg-go built the table's FileIO without object-store credentials: " +
			"no s3.access-key-id/s3.secret-access-key in the table's IO properties, and " +
			"metadata_search.storage is empty, so the catalog vended no session for this table")
	}

	region := props[icebergio.S3Region]
	if region == "" {
		region = vendedS3Region
	}
	endpoint := props[icebergio.S3EndpointURL]
	if endpoint == "" {
		// In a deployment on AWS the endpoint is the default one of the region. The fixture's
		// object store is SeaweedFS, which is addressed by the endpoint the test knows.
		endpoint = r.endpoint
	}
	client := s3.NewFromConfig(aws.Config{
		Region: region,
		Credentials: awscredentials.NewStaticCredentialsProvider(
			accessKeyID, secretAccessKey, sessionToken),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	bucket, err := s3blob.OpenBucket(ctx, client, parsed.Host, nil)
	if err != nil {
		return nil, fmt.Errorf("open s3://%s: %w", parsed.Host, err)
	}
	r.track(func() error { return bucket.Close() })
	return &recordingFileIO{
		FileIO: blobfs.New(ctx, bucket, blobfs.DefaultObjectLocationExtractor(parsed.Host, "s3")),
		rec:    r,
		creds: fileWrite{
			AccessKeyID:  accessKeyID,
			SessionToken: sessionToken,
		},
	}, nil
}

func (r *ioRecorder) track(closeFn func() error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closers = append(r.closers, closeFn)
}

func (r *ioRecorder) recordedConstructions() []fileConstruction {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fileConstruction(nil), r.constructions...)
}

func (r *ioRecorder) recordedWrites() []fileWrite {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fileWrite(nil), r.writes...)
}

// close releases the S3 buckets the recorder opened.
func (r *ioRecorder) close() {
	r.mu.Lock()
	closers := r.closers
	r.closers = nil
	r.mu.Unlock()
	for _, closeFn := range closers {
		_ = closeFn()
	}
}

// recordingFileIO records every file it creates or writes with the credentials of its FileIO.
// *blobfs.FileIO is embedded, so the optional interfaces iceberg-go looks for (ListableIO, StatIO,
// BulkRemovableIO, ...) stay on the method set.
type recordingFileIO struct {
	*blobfs.FileIO
	rec   *ioRecorder
	creds fileWrite
}

func (f *recordingFileIO) Create(name string) (icebergio.FileWriter, error) {
	writer, err := f.FileIO.Create(name)
	if err != nil {
		return nil, err
	}
	f.rec.recordFileWrite(name, f.creds)
	return writer, nil
}

func (f *recordingFileIO) WriteFile(name string, content []byte) error {
	if err := f.FileIO.WriteFile(name, content); err != nil {
		return err
	}
	f.rec.recordFileWrite(name, f.creds)
	return nil
}

// recordFileWrite appends one write to the audit trail.
func (r *ioRecorder) recordFileWrite(name string, creds fileWrite) {
	r.mu.Lock()
	defer r.mu.Unlock()
	creds.Name = name
	r.writes = append(r.writes, creds)
}

// activeS3IO is the recorder the replaced s3 IO factory consults. It is nil outside the vended
// credentials test, so nothing else in this test binary can reach the fake store by accident.
var activeS3IO atomic.Pointer[ioRecorder]

// s3IORecorderOnce guards the replacement of the s3 IO scheme.
var s3IORecorderOnce sync.Once

// installS3IORecorder replaces iceberg-go's s3, s3a and s3n FileIO factories with the recording one.
//
// iceberg-go resolves a FileIO by URI scheme (io.inferFileIOFromScheme), and the table location of a
// vended table is s3://<bucket>/_iceberg/<uuid>, so recording what the FileIO is built from means
// owning that scheme. The replacement is done once per test binary; nothing else in this package
// uses s3 (the other fixtures use the local adapter's local:// scheme), and with no recorder
// installed the factory fails instead of reaching a real bucket.
func installS3IORecorder() {
	s3IORecorderOnce.Do(func() {
		for _, scheme := range []string{"s3", "s3a", "s3n"} {
			icebergio.Unregister(scheme)
			icebergio.Register(scheme, recordingS3Factory)
		}
	})
}

// recordingS3Factory is the FileIO factory of the replaced schemes.
func recordingS3Factory(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
	recorder := activeS3IO.Load()
	if recorder == nil {
		return nil, fmt.Errorf("no recording S3 FileIO is installed for s3://%s/%s", parsed.Host, parsed.Path)
	}
	return recorder.newIO(ctx, parsed, props)
}

// requireCatalogVends are the checks that the catalog really vends, run before the write path so
// that a failure of the write path cannot be mistaken for vending being switched off.
func requireCatalogVends(t *testing.T, env *testEnv, sts *fakeSTS) {
	t.Helper()
	require.NoError(t, env.catalogConfig(nil).Validate(),
		"the fixture's client configuration must address the catalog")

	// GET /v1/config advertises the credentials endpoint only while the blockstore is S3 and a
	// role is configured (creds.go Routes), so this is the assertion that vending is on.
	advertised := getCatalogConfig(t, env)
	require.Contains(t, advertised.Endpoints,
		"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
		"the catalog must advertise the credential vending endpoint; GET /v1/config answered %+v", advertised)

	// Ask the credentials endpoint the way a client does, with the delegation header. This runs
	// the production vending path - the authorizer, the table scope resolver, the STS client built
	// from blockstore.s3.credentials_vending - against the fake STS.
	credentials := getStorageCredentials(t, env)
	require.Len(t, credentials, 1, "the catalog answered the credentials request with no session")
	require.Equal(t, vendedS3AccessKeyID, credentials[0].Config["s3.access-key-id"],
		"the credentials endpoint must hand out the session the STS issued")
	require.Equal(t, vendedSessionToken, credentials[0].Config["s3.session-token"],
		"the credentials endpoint must hand out the session the STS issued")
	require.NotEmpty(t, sts.recorded(), "the fake STS was never asked for a session")
}

// catalogConfigResponse is the part of GET /v1/config a test reads.
type catalogConfigResponse struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
	Endpoints []string          `json:"endpoints"`
}

// getCatalogConfig reads GET /iceberg/api/v1/config with the indexer's credentials.
func getCatalogConfig(t *testing.T, env *testEnv) catalogConfigResponse {
	t.Helper()
	var out catalogConfigResponse
	getJSON(t, env, "/iceberg/api/v1/config", &out)
	return out
}

// storageCredential is one StorageCredential of the credentials endpoint.
type storageCredential struct {
	Prefix string            `json:"prefix"`
	Config map[string]string `json:"config"`
}

// getStorageCredentials asks the credentials endpoint of system.object_metadata for the session a
// client of the table gets, exactly as iceberg-go's refresher would.
func getStorageCredentials(t *testing.T, env *testEnv) []storageCredential {
	t.Helper()
	var out struct {
		StorageCredentials []storageCredential `json:"storage-credentials"`
	}
	// The /iceberg/api form addresses the repository and the ref as the first two levels of the
	// namespace path, joined with the catalog's advertised separator (ident.go).
	tableNamespace := url.PathEscape(env.repo + icebergcatalog.NamespaceSeparator + testBranch +
		icebergcatalog.NamespaceSeparator + table.Namespace)
	getJSON(t, env, "/iceberg/api/v1/lakefs/namespaces/"+tableNamespace+"/tables/"+table.Name+"/credentials", &out)
	return out.StorageCredentials
}

// getJSON performs one authenticated catalog GET.
func getJSON(t *testing.T, env *testEnv, path string, out any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), vendedRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, env.server.URL+path, nil)
	require.NoError(t, err, "build the request")
	request.Header.Set("Authorization", env.catalogConfig(nil).Authorization())
	request.Header.Set(icebergcatalog.AccessDelegationHeader, icebergcatalog.VendedCredentialsDelegation)
	response, err := http.DefaultClient.Do(request)
	require.NoErrorf(t, err, "GET %s", path)
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	require.NoErrorf(t, err, "read the answer of GET %s", path)
	require.Equalf(t, http.StatusOK, response.StatusCode, "GET %s answered %s: %s", path,
		response.Status, body)
	require.NoErrorf(t, json.Unmarshal(body, out), "decode the answer of GET %s: %s", path, body)
}

// TestVendedCredentials is the proof of design §4.6's vended mode: with metadata_search.storage
// empty, the indexer's iceberg-go client gets its write credentials from the catalog.
func TestVendedCredentials(t *testing.T) {
	store := newVendedS3Store(t)
	sts := newFakeSTS(t)
	requests := &requestLog{}

	// The catalog's STS client is built from the process' AWS configuration (newSTSClient), so the
	// test gives it a value it can sign with. The object store does not know these credentials:
	// they are here to sign AssumeRole, never to write a file.
	t.Setenv("AWS_ACCESS_KEY_ID", vendedAmbientAccessKeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", vendedAmbientSecretAccessKey)
	t.Setenv("AWS_SESSION_TOKEN", vendedAmbientSessionToken)
	t.Setenv("AWS_REGION", vendedS3Region)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	recorder := &ioRecorder{endpoint: store.endpoint}
	installS3IORecorder()
	activeS3IO.Store(recorder)
	t.Cleanup(func() {
		activeS3IO.Store(nil)
		recorder.close()
	})

	viperKeys, storageNamespace := store.blockstoreConfig("data-repo-metadata")
	viperKeys["iceberg.credentials_vending.endpoint"] = sts.server.URL
	env := newTestEnvWith(t, testEnvOptions{
		viper:            viperKeys,
		storageNamespace: func(_, _ string) string { return storageNamespace },
		// storage stays nil: this is the vended mode.
		wrapHandler: requests.wrap,
	})

	// The mode is what the test claims it is: the client has no static IO properties at all.
	require.Empty(t, env.clientConfig.Storage,
		"the vended mode test must run with metadata_search.storage empty")

	tbl := env.createTable(t)
	// Vending is on and answers with a session for the table: the preconditions of the write path.
	requireCatalogVends(t, env, sts)

	first := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	rows := []table.Row{
		row(t, "a", "c1", 1, first, nil),
		row(t, "b", "c1", 2, first, map[string]string{"k": "v"}),
	}
	// Create + ApplyDelta is the indexer's write path of §4.3 step 3: the delta's Parquet and Avro
	// files and the new metadata.json are written by the client, and the catalog stages them.
	_, err := table.ApplyDelta(t.Context(), tbl, rows, nil, "c1")
	require.NoErrorf(t, err, "create and ApplyDelta in vended mode (client FileIO constructions: %+v, "+
		"AssumeRole calls: %+v)", recorder.recordedConstructions(), sts.recorded())

	// Every file the delta wrote was written with the session the catalog vended: the access key,
	// the secret and the session token of the fake STS, not the ambient AWS configuration.
	writes := recorder.recordedWrites()
	require.NotEmpty(t, writes, "the delta wrote no files through the table's FileIO")
	for _, write := range writes {
		require.Equal(t, vendedS3AccessKeyID, write.AccessKeyID,
			"%s was written with access key %q, not with the vended one", write.Name, write.AccessKeyID)
		require.Equal(t, vendedSessionToken, write.SessionToken,
			"%s was written without the vended session token: its FileIO carried %q",
			write.Name, write.SessionToken)
	}
	require.NotEqual(t, vendedAmbientAccessKeyID, writes[0].AccessKeyID,
		"the write fell back to the process' ambient AWS credentials instead of the vended session")

	// The files are the table's: the catalog staged them and a read returns the delta's rows.
	requireScanRows(t, env.loadTable(t), rows)

	// The session the catalog vended for the write is a write session: §8's policy grants
	// s3:PutObject on the table prefix, which the read session of the credentials endpoint does not.
	writeCalls := sts.writeCalls()
	require.NotEmptyf(t, writeCalls, "the catalog never vended a write session; its AssumeRole calls "+
		"were %+v", sts.recorded())
	require.Equal(t, vendedRoleARN, writeCalls[0].RoleARN)
	require.Contains(t, writeCalls[0].RoleSessionName, testUser,
		"the session name must attribute the session to the indexer principal")
	require.Contains(t, writeCalls[0].Policy, "/_iceberg/",
		"the session must be scoped to the table prefix, not to the storage namespace")

	// The client asked for vended credentials: iceberg-go sends the data-access delegation header
	// on every request as a session default, so table.NewCatalog does not have to add it.
	for _, request := range requests.recorded() {
		require.Contains(t, request.Delegation, icebergcatalog.VendedCredentialsDelegation,
			"%s %s did not ask for vended credentials", request.Method, request.Path)
	}
	require.NotEmpty(t, requests.recorded(), "the catalog answered no request from the client")
}
