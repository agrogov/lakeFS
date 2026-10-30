//go:build datasets

// Package esti holds the lakeFS system tests.
//
// datasets_test.go is the datasets system-test suite of DATASETS_DESIGN.md §15.2 (DS-13). It
// runs against the lakefs-sso binary, because that is the fork-owned binary that mounts the
// datasets control plane (§4.4); the upstream lakefs binary has no /api/v1/datasets routes.
//
// The suite starts its own server so that it is independent of whatever the surrounding esti
// environment runs: a throw-away Postgres database on the esti Postgres instance (or a local
// KV store when no database connection string is configured). By default the server uses a
// local blockstore in a temporary directory. When the environment supplies a cloud blockstore
// (LAKEFS_BLOCKSTORE_TYPE plus the esti storage namespace), the suite instead runs the same
// scenarios on that blockstore under a unique per-run prefix, and removes it afterwards - that
// is what closes spike S4 of §18. Both the datasets *and* their sources live on one blockstore,
// which §5.2 requires (same storage ID, zero-copy full addresses, §6.3).
//
// Scenarios (§15.2):
//
//  1. read paths: a dataset built from two source repositories, read through the REST API, the
//     S3 gateway with minio-go and a presigned URL; the dataset repository is read-only;
//  2. versions: publish v2, diff v1..v2 (definition and data), and v1 still returns the old data;
//  3. GC safety: delete the source branch, run the local GC flow (rules + commit preparation,
//     the artifact the GC job consumes) and confirm the pinned commit is retained and the
//     dataset still readable;
//  4. proposals: create with a preview branch, browse the preview, merge, and close;
//  5. access: a grant read by a user who is in no base ACL group (§9.6), while the sources stay
//     unreadable to that user.
//
// Clean-room note (§2): this file was written from DATASETS_DESIGN.md and the public lakeFS
// documentation only. It contains no code, SQL, protobuf or UI asset from lakeFS Enterprise and
// nothing from upstream lakeFS >= 1.87.
//
// The suite skips cleanly - rather than failing - when it cannot run: lakefs-sso is neither in
// the binaries directory nor buildable from the sources next to the tests, or the server answers
// the control plane with the disabled handler. It is wired into `make esti` through the
// `datasets` build tag and can be run on its own with `make esti-datasets`.
package esti

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	// datasetsAdminUsername is the user created by the setup call on the throw-away server.
	datasetsAdminUsername = "esti-datasets"

	// datasetsServerTimeout bounds the wait for the throw-away server to become healthy.
	datasetsServerTimeout = 3 * time.Minute
)

// --- control-plane payloads -----------------------------------------------------------------
//
// The control-plane types are declared here instead of importing pkg/datasets/apigen: the
// dependency rule of §4.6 keeps pkg/datasets out of everything except the fork-owned entry
// points, and a system test is not one of them. Only the fields these tests use are declared.

type dsRef struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type dsPath struct {
	Path string `json:"path"`
}

type dsDataItem struct {
	Target     string  `json:"target"`
	Repository string  `json:"repository"`
	Ref        dsRef   `json:"ref"`
	Type       string  `json:"type"`
	Object     *dsPath `json:"object,omitempty"`
	Prefix     *dsPath `json:"prefix,omitempty"`
}

type dsDefinition struct {
	FormatVersion int            `json:"format_version"`
	Description   string         `json:"description,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Data          []dsDataItem   `json:"data"`
}

type dsPublishOptions struct {
	Message string `json:"message,omitempty"`
}

type dsCreateRequest struct {
	Name       string            `json:"name"`
	Definition dsDefinition      `json:"definition"`
	Publish    *dsPublishOptions `json:"publish,omitempty"`
}

type dsPublishRequest struct {
	Definition      dsDefinition `json:"definition"`
	Message         string       `json:"message,omitempty"`
	ExpectedVersion *int         `json:"expected_version,omitempty"`
}

type dsDataset struct {
	Name          string        `json:"name"`
	Description   string        `json:"description,omitempty"`
	CreatedBy     string        `json:"created_by"`
	LatestVersion int           `json:"latest_version"`
	State         string        `json:"state"`
	Definition    *dsDefinition `json:"definition,omitempty"`
}

type dsVersion struct {
	Version    int           `json:"version"`
	CommitID   string        `json:"commit_id"`
	Publisher  string        `json:"publisher"`
	Message    string        `json:"message,omitempty"`
	ProposalID string        `json:"proposal_id,omitempty"`
	Definition *dsDefinition `json:"definition,omitempty"`
}

type dsVersionList struct {
	Results []dsVersion `json:"results"`
}

type dsItemChange struct {
	Target string `json:"target"`
	Change string `json:"change"`
	From   any    `json:"from,omitempty"`
	To     any    `json:"to,omitempty"`
}

type dsDefinitionDiff struct {
	Items []dsItemChange `json:"items"`
}

type dsDataDiffEntry struct {
	Path      string `json:"path"`
	Type      string `json:"type"`
	SizeBytes *int64 `json:"size_bytes,omitempty"`
}

type dsDataDiff struct {
	Results []dsDataDiffEntry `json:"results"`
}

type dsProposalCreateRequest struct {
	Definition  dsDefinition `json:"definition"`
	Title       string       `json:"title"`
	Description string       `json:"description,omitempty"`
}

type dsProposal struct {
	ID            string        `json:"id"`
	Dataset       string        `json:"dataset"`
	BaseVersion   int           `json:"base_version"`
	Title         string        `json:"title"`
	Author        string        `json:"author"`
	Status        string        `json:"status"`
	PreviewBranch string        `json:"preview_branch"`
	MergedVersion *int          `json:"merged_version,omitempty"`
	Definition    *dsDefinition `json:"definition,omitempty"`
}

type dsAccessGrant struct {
	PrincipalID   string `json:"principal_id"`
	PrincipalType string `json:"principal_type"`
}

type dsAccessGrantRequest struct {
	PrincipalID   string `json:"principal_id"`
	PrincipalType string `json:"principal_type"`
}

// --- the datasets control-plane client ------------------------------------------------------

// datasetsClient is a minimal HTTP client for /api/v1/datasets. It uses basic auth so it honours
// the same authentication schemes as the rest of esti.
type datasetsClient struct {
	t      *testing.T
	base   string // http://host/api/v1/datasets/
	key    string
	secret string
	http   *http.Client
}

func newDatasetsClient(t *testing.T, endpointURL, key, secret string) *datasetsClient {
	t.Helper()
	return &datasetsClient{
		t:      t,
		base:   strings.TrimRight(endpointURL, "/") + "/datasets/",
		key:    key,
		secret: secret,
		http:   &http.Client{Timeout: 2 * time.Minute},
	}
}

// do issues one request and returns the status code and the raw body.
func (c *datasetsClient) do(ctx context.Context, method, path string, body any) (int, []byte) {
	c.t.Helper()
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		require.NoError(c.t, err, "marshal datasets request body")
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	require.NoError(c.t, err, "build datasets request %s %s", method, path)
	req.SetBasicAuth(c.key, c.secret)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	require.NoError(c.t, err, "call datasets API %s %s", method, path)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err, "read datasets response %s %s", method, path)
	return resp.StatusCode, data
}

// must issues a request, requires the given status code and decodes the body into out (when out
// is not nil).
func (c *datasetsClient) must(ctx context.Context, want int, method, path string, body, out any) {
	c.t.Helper()
	status, data := c.do(ctx, method, path, body)
	require.Equalf(c.t, want, status, "%s %s: body %s", method, path, data)
	if out != nil {
		require.NoErrorf(c.t, json.Unmarshal(data, out), "%s %s: decode body %s", method, path, data)
	}
}

// --- the throw-away server ------------------------------------------------------------------

// datasetsEnv is one running lakefs-sso server with the datasets control plane enabled.
type datasetsEnv struct {
	t             *testing.T
	endpoint      string // http://host:port/api/v1
	s3Host        string // host:port, for the S3 gateway clients
	key           string
	secret        string
	sourcesPrefix string // parent namespace of the source repositories
	core          *apigen.ClientWithResponses
	ds            *datasetsClient
	s3            *minio.Client
}

// coreFor returns a core API client authenticated as the given principal.
func (e *datasetsEnv) coreFor(t *testing.T, key, secret string) *apigen.ClientWithResponses {
	t.Helper()
	cl, err := testutil.NewClientFromCreds(logging.ContextUnavailable(), key, secret, e.endpoint)
	require.NoError(t, err, "build core API client")
	return cl
}

// dsFor returns a datasets control-plane client authenticated as the given principal.
func (e *datasetsEnv) dsFor(key, secret string) *datasetsClient {
	return newDatasetsClient(e.t, e.endpoint, key, secret)
}

// s3For returns an S3 gateway client for the given principal. The gateway is addressed by path
// style (bucket = repository), so no DNS is needed.
func (e *datasetsEnv) s3For(t *testing.T, key, secret string) *minio.Client {
	t.Helper()
	cl, err := minio.New(e.s3Host, &minio.Options{
		Creds:        credentials.NewStaticV4(key, secret, ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
		Region:       "us-east-1",
	})
	require.NoError(t, err, "build S3 gateway client")
	return cl
}

// newDatasetsEnv starts lakefs-sso with datasets enabled and returns the environment. It skips
// the calling test when the binary is missing or when the server reports the control plane as
// disabled, so it is safe to wire into the shared `make esti` target.
func newDatasetsEnv(t *testing.T) *datasetsEnv {
	t.Helper()
	dir := t.TempDir()
	blockstore := newDatasetsBlockstore(t, dir)
	binary := datasetsServerBinary(t)

	dbSection, dropDatabase := datasetsDatabase(t, dir)
	issuer := startOIDCDiscoveryStub(t)

	port := freePort(t)
	hostPort := fmt.Sprintf("127.0.0.1:%d", port)
	// The sso: section is not used for a login flow here; it is what makes lakefs-sso build
	// the ACL auth service instead of the single-admin BasicAuthService. The datasets control
	// plane needs a policy-capable service for its managed read and immutability policies
	// (§9.2, §9.4). rbac: internal additionally exposes the policy API to the suite, which is
	// how it installs the datasets control-plane permission below.
	config := fmt.Sprintf(`listen_address: %q
logging:
  level: "warn"
database:
%s
auth:
  encrypt:
    secret_key: "esti-datasets-test-secret-key"
  ui_config:
    rbac: "internal"
sso:
  enabled: true
  client_id: "esti-datasets"
  client_secret: "esti-datasets-secret"
  issuer_url: %q
  callback_base_url: "http://%s"
%sdatasets:
  enabled: true
  storage_namespace_prefix: %q
  reconcile_interval: "24h"
  publish_timeout: "10m"
`, hostPort, dbSection, issuer, hostPort, blockstore.BlockstoreSection, blockstore.StorageNamespacePrefix)

	configPath := filepath.Join(dir, "lakefs.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o600))

	logPath := filepath.Join(dir, "lakefs-sso.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err, "create the server log file")

	// The server runs in the temporary directory: GC commit preparation writes a local graph
	// store there (see pkg/datasets/pins_gc_test.go), and nothing may land in the repo.
	cmd := exec.CommandContext(t.Context(), binary, "run", "--config", configPath)
	cmd.Dir = dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Registered before the stop cleanup, so it runs after the process is gone and the log is
	// complete: a failing cloud run is otherwise very hard to debug from CI.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Logf("lakefs-sso log unavailable: %v", err)
			return
		}
		const maxLogBytes = 64 << 10
		if len(log) > maxLogBytes {
			log = log[len(log)-maxLogBytes:]
		}
		t.Logf("lakefs-sso log (tail):\n%s", log)
	})
	require.NoError(t, cmd.Start(), "start lakefs-sso")
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = logFile.Close()
		dropDatabase()
	})

	baseURL := "http://" + hostPort
	waitForDatasetsServer(t, baseURL, logPath)

	env := &datasetsEnv{
		t:             t,
		endpoint:      baseURL + apiutil.BaseURL,
		s3Host:        hostPort,
		key:           DefaultAdminAccessKeyID,
		secret:        DefaultAdminSecretAccessKey,
		sourcesPrefix: blockstore.SourcesPrefix,
	}

	// First run on the throw-away server: create the admin principal with known credentials,
	// exactly as `lakefs setup` would.
	anonymous, err := apigen.NewClientWithResponses(env.endpoint)
	require.NoError(t, err, "build anonymous core API client")
	setup, err := anonymous.SetupWithResponse(t.Context(), apigen.SetupJSONRequestBody{
		Username:       datasetsAdminUsername,
		Key:            &apigen.AccessKeyCredentials{AccessKeyId: env.key, SecretAccessKey: env.secret},
		FeatureUpdates: swag.Bool(false),
	})
	require.NoError(t, err, "setup lakefs-sso")
	require.Equalf(t, http.StatusOK, setup.StatusCode(), "setup lakefs-sso: %s", setup.Body)

	env.core = env.coreFor(t, env.key, env.secret)
	env.ds = env.dsFor(env.key, env.secret)
	env.s3 = env.s3For(t, env.key, env.secret)

	// `make esti` also runs against servers without the datasets control plane, where the
	// routes builder mounts a 501 handler instead of the control plane (§4.4). Answer that with
	// a skip rather than a failure, so the suite can be wired into the shared target (§15.2).
	status, body := env.ds.do(t.Context(), http.MethodGet, "", nil)
	if status == http.StatusNotImplemented || status == http.StatusNotFound {
		t.Skipf("datasets are disabled on this server (%d: %s); skipping the datasets system tests",
			status, strings.TrimSpace(string(body)))
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		// A stock build has no principal with `datasets:*` (see grantDatasetsControlPlane).
		grantDatasetsControlPlane(t, env)
		status, body = env.ds.do(t.Context(), http.MethodGet, "", nil)
	}
	require.Equalf(t, http.StatusOK, status, "list datasets: %s", body)
	return env
}

// grantDatasetsControlPlane gives the suite's principal the datasets control-plane permissions.
//
// The installation-wide datasets policies of §9.5 are created by the RBAC bootstrap, but that
// bootstrap ships with the feature switched off (pkg/auth/setup's defaultDatasetsEnabled), so a
// stock lakefs-sso has no principal with `datasets:*`. The suite therefore installs the
// equivalent policy itself through the policy API, which internal RBAC mode exposes. This is
// harness setup, not part of what the scenarios assert.
func grantDatasetsControlPlane(t *testing.T, env *datasetsEnv) {
	t.Helper()
	const policyID = "esti-datasets-admin"
	created, err := env.core.CreatePolicyWithResponse(t.Context(), apigen.CreatePolicyJSONRequestBody{
		Id: policyID,
		Statement: []apigen.Statement{{
			Effect:   "allow",
			Action:   []string{"datasets:*"},
			Resource: "*",
		}},
	})
	require.NoError(t, err, "create the datasets test policy")
	require.Equalf(t, http.StatusCreated, created.StatusCode(), "create the datasets test policy: %s", created.Body)

	attached, err := env.core.AttachPolicyToUserWithResponse(t.Context(), datasetsAdminUsername, policyID)
	require.NoError(t, err, "attach the datasets test policy")
	require.Equalf(t, http.StatusCreated, attached.StatusCode(), "attach the datasets test policy: %s", attached.Body)
}

// datasetsDatabase returns the "database:" YAML section of the throw-away server and a cleanup
// that removes the database it created.
//
// The esti Postgres instance is shared with the rest of the suite, so the test creates its own
// database there: the setup call must run on a fresh KV store, and the datasets control plane
// must not see the other suite's repositories. Without a Postgres connection string (a local
// run) it falls back to a KV store on disk.
func datasetsDatabase(t *testing.T, dir string) (string, func()) {
	t.Helper()
	connectionString := viper.GetString("database_connection_string")
	if connectionString == "" {
		section := fmt.Sprintf("  type: local\n  local:\n    path: %q\n", filepath.Join(dir, "metadata"))
		return section, func() {}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, connectionString)
	require.NoError(t, err, "connect to the esti Postgres instance")
	name := "esti_datasets_" + strings.ToLower(randstr.String(10))
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	if err != nil {
		_ = admin.Close(ctx)
		t.Skipf("cannot create the datasets test database (needs CREATEDB): %v", err)
	}

	adminURL, err := url.Parse(connectionString)
	require.NoError(t, err, "the esti Postgres connection string must be a URL")
	adminURL.Path = "/" + name

	cleanup := func() {
		// Cleanups run LIFO and t.Context() is already cancelled here, so the server is stopped
		// by the time the database is dropped: nothing may still hold a connection to it.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", name)
		_, _ = admin.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize())
		_ = admin.Close(cleanupCtx)
	}
	section := fmt.Sprintf("  type: postgres\n  postgres:\n    connection_string: %q\n", adminURL.String())
	return section, cleanup
}

// datasetsServerBinary returns the lakefs-sso binary to run, building it from source when the
// binaries directory does not have it. The datasets control plane is mounted by cmd/lakefs-sso
// only, and the esti container ships just the lakefs and lakectl binaries in /app, so building
// on demand is what lets the suite run there. It skips the test when neither route is available.
func datasetsServerBinary(t *testing.T) string {
	t.Helper()
	installed := lakeBinaryLocation("lakefs-sso")
	if _, err := os.Stat(installed); err == nil {
		return installed
	}

	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("datasets system tests need the lakefs-sso binary (%s) and there is no Go toolchain to build it: %v", installed, err)
	}
	packageDir, err := os.Getwd()
	require.NoError(t, err, "locate the esti package directory")
	repoRoot := filepath.Dir(packageDir)
	cmdDir := filepath.Join(repoRoot, "cmd", "lakefs-sso")
	if _, err := os.Stat(cmdDir); err != nil {
		t.Skipf("datasets system tests need the lakefs-sso binary (%s) and its sources (%s): %v", installed, cmdDir, err)
	}

	built := filepath.Join(t.TempDir(), "lakefs-sso")
	build := exec.CommandContext(t.Context(), goTool, "build", "-o", built, "./cmd/lakefs-sso")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Skipf("cannot build lakefs-sso for the datasets system tests: %v\n%s", err, output)
	}
	return built
}

// startOIDCDiscoveryStub serves an OIDC discovery document on a loopback port and returns its// issuer URL. lakefs-sso performs discovery for the sso: section, and the datasets suite only
// needs the ACL auth service that section selects - no token is ever exchanged.
func startOIDCDiscoveryStub(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen for the OIDC discovery stub")
	issuer := "http://" + listener.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                issuer,
			"authorization_endpoint":                issuer + "/authorize",
			"token_endpoint":                        issuer + "/token",
			"jwks_uri":                              issuer + "/keys",
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"scopes_supported":                      []string{"openid", "profile", "email"},
		})
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return issuer
}

// freePort asks the kernel for a free TCP port on the loopback interface.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "allocate a free port")
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// waitForDatasetsServer blocks until /_health answers, or fails the test with the server log.
func waitForDatasetsServer(t *testing.T, baseURL, logPath string) {
	t.Helper()
	deadline := time.Now().Add(datasetsServerTimeout)
	client := &http.Client{Timeout: 5 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/_health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	log, _ := os.ReadFile(logPath)
	t.Fatalf("lakefs-sso did not become healthy at %s within %s; server log:\n%s", baseURL, datasetsServerTimeout, log)
}

// --- fixtures -------------------------------------------------------------------------------

var (
	datasetContentCustomers = []byte("parquet-bytes-of-c1")
	datasetContentCountries = []byte("code,name\nil,israel\n")
)

func uniqueSuffix() string {
	return strings.ToLower(randstr.String(8))
}

// createRepo creates a writable source repository on the blockstore the dataset uses.
func (e *datasetsEnv) createRepo(t *testing.T, name, defaultBranch string) {
	t.Helper()
	namespace := datasetsNamespace(t, e.sourcesPrefix, name)
	res, err := e.core.CreateRepositoryWithResponse(t.Context(), &apigen.CreateRepositoryParams{},
		apigen.CreateRepositoryJSONRequestBody{
			Name:             name,
			StorageNamespace: namespace,
			DefaultBranch:    swag.String(defaultBranch),
		})
	require.NoError(t, err, "create repository %s", name)
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "create repository %s: %s", name, res.Body)
}

func (e *datasetsEnv) createBranch(t *testing.T, repo, branch, source string) {
	t.Helper()
	res, err := e.core.CreateBranchWithResponse(t.Context(), repo, apigen.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: source,
	})
	require.NoError(t, err, "create branch %s/%s", repo, branch)
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "create branch %s/%s: %s", repo, branch, res.Body)
}

func (e *datasetsEnv) deleteBranch(t *testing.T, repo, branch string) {
	t.Helper()
	res, err := e.core.DeleteBranchWithResponse(t.Context(), repo, branch, &apigen.DeleteBranchParams{Force: swag.Bool(true)})
	require.NoError(t, err, "delete branch %s/%s", repo, branch)
	require.Equalf(t, http.StatusNoContent, res.StatusCode(), "delete branch %s/%s: %s", repo, branch, res.Body)
}

// putObject uploads one object through the S3 gateway.
func (e *datasetsEnv) putObject(t *testing.T, s3 *minio.Client, repo, branch, path string, content []byte) {
	t.Helper()
	_, err := s3.PutObject(t.Context(), repo, branch+"/"+path, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})
	require.NoErrorf(t, err, "upload %s/%s", repo, path)
}

// commit commits the branch working tree and returns the commit ID.
func (e *datasetsEnv) commit(t *testing.T, repo, branch, message string) string {
	t.Helper()
	res, err := e.core.CommitWithResponse(t.Context(), repo, branch, &apigen.CommitParams{},
		apigen.CommitJSONRequestBody{Message: message})
	require.NoError(t, err, "commit %s/%s", repo, branch)
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "commit %s/%s: %s", repo, branch, res.Body)
	require.NotNil(t, res.JSON201)
	return res.JSON201.Id
}

func (e *datasetsEnv) createDataset(t *testing.T, name string, def dsDefinition, message string) dsDataset {
	t.Helper()
	var created dsDataset
	e.ds.must(t.Context(), http.StatusCreated, http.MethodPost, "",
		dsCreateRequest{Name: name, Definition: def, Publish: &dsPublishOptions{Message: message}}, &created)
	require.Equal(t, name, created.Name)
	return created
}

func (e *datasetsEnv) publish(t *testing.T, dataset string, req dsPublishRequest) dsVersion {
	t.Helper()
	var version dsVersion
	e.ds.must(t.Context(), http.StatusCreated, http.MethodPost, dataset+"/versions", req, &version)
	return version
}

// readObjectREST reads an object through the lakeFS REST API (the path `lakectl fs cat` takes).
func readObjectREST(t *testing.T, cl *apigen.ClientWithResponses, repo, ref, path string) []byte {
	t.Helper()
	res, err := cl.GetObjectWithResponse(t.Context(), repo, ref, &apigen.GetObjectParams{Path: path})
	require.NoError(t, err, "read %s/%s/%s", repo, ref, path)
	require.Equalf(t, http.StatusOK, res.StatusCode(), "read %s/%s/%s", repo, ref, path)
	return res.Body
}

// tryReadObjectS3 reads an object through the S3 gateway, returning the error instead of failing,
// so that denials can be asserted.
func tryReadObjectS3(t *testing.T, cl *minio.Client, bucket, key string) ([]byte, error) {
	t.Helper()
	object, err := cl.GetObject(t.Context(), bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	return io.ReadAll(object)
}

func readObjectS3(t *testing.T, cl *minio.Client, bucket, key string) []byte {
	t.Helper()
	data, err := tryReadObjectS3(t, cl, bucket, key)
	require.NoErrorf(t, err, "read s3://%s/%s", bucket, key)
	return data
}

// readObjectPresigned reads an object through a presigned S3 gateway URL. lakeFS validates the
// query-string signature itself, so this works on every blockstore, including local.
func readObjectPresigned(t *testing.T, cl *minio.Client, bucket, key string) []byte {
	t.Helper()
	signed, err := cl.PresignedGetObject(t.Context(), bucket, key, time.Minute, url.Values{})
	require.NoErrorf(t, err, "presign s3://%s/%s", bucket, key)

	resp, err := http.Get(signed.String())
	require.NoErrorf(t, err, "GET the presigned URL of s3://%s/%s", bucket, key)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read the presigned URL body")
	require.Equalf(t, http.StatusOK, resp.StatusCode, "presigned GET s3://%s/%s", bucket, key)
	return data
}

// --- scenarios ------------------------------------------------------------------------------

// TestDatasets is the datasets system-test suite of DATASETS_DESIGN.md §15.2. Every subtest
// builds its own sources and dataset, so subtests can be run individually.
func TestDatasets(t *testing.T) {
	env := newDatasetsEnv(t)

	t.Run("read_paths", func(t *testing.T) { testDatasetsReadPaths(t, env) })
	t.Run("versions_and_diff", func(t *testing.T) { testDatasetsVersionsAndDiff(t, env) })
	t.Run("gc_keeps_pinned_source_data", func(t *testing.T) { testDatasetsGCSafety(t, env) })
	t.Run("proposal_flow", func(t *testing.T) { testDatasetsProposals(t, env) })
	t.Run("access_grant_outside_base_groups", func(t *testing.T) { testDatasetsAccessGrant(t, env) })
}

// testDatasetsReadPaths covers §15.2 bullet 1: create from two source repositories and read the
// data through every data-plane path, with the dataset repository refusing writes.
func testDatasetsReadPaths(t *testing.T, env *datasetsEnv) {
	uid := uniqueSuffix()
	srcCustomers := "ds-customers-" + uid
	srcReference := "ds-reference-" + uid
	env.createRepo(t, srcCustomers, mainBranch)
	env.createRepo(t, srcReference, mainBranch)

	env.createBranch(t, srcCustomers, "snap", mainBranch)
	env.putObject(t, env.s3, srcCustomers, "snap", "data/customers/c1.parquet", datasetContentCustomers)
	commitCustomers := env.commit(t, srcCustomers, "snap", "customers v1")

	env.putObject(t, env.s3, srcReference, mainBranch, "geo/countries.csv", datasetContentCountries)
	commitReference := env.commit(t, srcReference, mainBranch, "reference data")

	dataset := "customer-360-" + uid
	def := dsDefinition{
		FormatVersion: 1,
		Description:   "Curated customer view for analytics",
		Metadata:      map[string]any{"owner": "data-platform", "tier": "gold"},
		Data: []dsDataItem{
			{
				Target:     "customers",
				Repository: srcCustomers,
				Ref:        dsRef{Type: "commit", ID: commitCustomers},
				Type:       "prefix",
				Prefix:     &dsPath{Path: "data/customers/"},
			},
			{
				Target:     "reference/countries.csv",
				Repository: srcReference,
				Ref:        dsRef{Type: "commit", ID: commitReference},
				Type:       "object",
				Object:     &dsPath{Path: "geo/countries.csv"},
			},
		},
	}
	created := env.createDataset(t, dataset, def, "initial dataset")
	require.Equal(t, 1, created.LatestVersion, "create with publish produces v1")
	require.Equal(t, "active", created.State)

	t.Run("rest", func(t *testing.T) {
		require.Equal(t, datasetContentCustomers, readObjectREST(t, env.core, dataset, "v1", "customers/c1.parquet"))
		require.Equal(t, datasetContentCountries, readObjectREST(t, env.core, dataset, "v1", "reference/countries.csv"))
		// `latest` is the branch that points at the newest version (§7.1).
		require.Equal(t, datasetContentCustomers, readObjectREST(t, env.core, dataset, "latest", "customers/c1.parquet"))
	})

	t.Run("s3_gateway", func(t *testing.T) {
		require.Equal(t, datasetContentCustomers, readObjectS3(t, env.s3, dataset, "v1/customers/c1.parquet"))
		require.Equal(t, datasetContentCountries, readObjectS3(t, env.s3, dataset, "v1/reference/countries.csv"))
	})

	t.Run("presigned_url", func(t *testing.T) {
		require.Equal(t, datasetContentCustomers, readObjectPresigned(t, env.s3, dataset, "v1/customers/c1.parquet"))
	})

	t.Run("read_only_repository", func(t *testing.T) {
		// §6.1/§7.3: the backing repository is read-only, so data-plane writes are refused.
		_, err := env.s3.PutObject(t.Context(), dataset, "v1/customers/forged.parquet",
			strings.NewReader("forged"), int64(len("forged")), minio.PutObjectOptions{})
		require.Error(t, err, "the dataset repository must refuse S3 gateway writes")
	})

	t.Run("list", func(t *testing.T) {
		var list struct {
			Results []dsDataset `json:"results"`
		}
		env.ds.must(t.Context(), http.StatusOK, http.MethodGet, "?prefix="+dataset, nil, &list)
		require.NotEmpty(t, list.Results)
		require.Equal(t, dataset, list.Results[0].Name)
	})
}

// testDatasetsVersionsAndDiff covers §15.2 bullet 2: publish v2, diff v1..v2 (definition and
// data) and confirm v1 keeps returning the old data.
func testDatasetsVersionsAndDiff(t *testing.T, env *datasetsEnv) {
	uid := uniqueSuffix()
	source := "ds-versions-" + uid
	env.createRepo(t, source, mainBranch)

	env.putObject(t, env.s3, source, mainBranch, "data/a.txt", []byte("a"))
	commitV1 := env.commit(t, source, mainBranch, "a")
	env.putObject(t, env.s3, source, mainBranch, "data/b.txt", []byte("b"))
	commitV2 := env.commit(t, source, mainBranch, "b")

	prefixItem := func(commit string) dsDataItem {
		return dsDataItem{
			Target:     "data",
			Repository: source,
			Ref:        dsRef{Type: "commit", ID: commit},
			Type:       "prefix",
			Prefix:     &dsPath{Path: "data/"},
		}
	}
	dataset := "versions-" + uid
	env.createDataset(t, dataset, dsDefinition{FormatVersion: 1, Data: []dsDataItem{prefixItem(commitV1)}}, "v1")

	// v1 is pinned to the first commit: b.txt does not exist there.
	require.Equal(t, []byte("a"), readObjectREST(t, env.core, dataset, "v1", "data/a.txt"))
	_, bInV1 := readObjectStatus(t, env.core, dataset, "v1", "data/b.txt")
	require.Equal(t, http.StatusNotFound, bInV1, "v1 must not contain the object committed later")

	v2 := env.publish(t, dataset, dsPublishRequest{
		Definition: dsDefinition{FormatVersion: 1, Data: []dsDataItem{prefixItem(commitV2)}},
		Message:    "v2",
	})
	require.Equal(t, 2, v2.Version)

	var versions dsVersionList
	env.ds.must(t.Context(), http.StatusOK, http.MethodGet, dataset+"/versions", nil, &versions)
	require.Len(t, versions.Results, 2)
	require.Equal(t, 2, versions.Results[0].Version, "versions are listed newest first")

	t.Run("definition_diff", func(t *testing.T) {
		var diff dsDefinitionDiff
		env.ds.must(t.Context(), http.StatusOK, http.MethodGet, dataset+"/refs/v1/diff/v2", nil, &diff)
		require.Len(t, diff.Items, 1)
		require.Equal(t, "data", diff.Items[0].Target)
		require.Equal(t, "changed", diff.Items[0].Change, "the pinned commit moved between v1 and v2")
	})

	t.Run("data_diff", func(t *testing.T) {
		var diff dsDataDiff
		env.ds.must(t.Context(), http.StatusOK, http.MethodGet, dataset+"/refs/v1/diff/v2?type=data", nil, &diff)
		paths := make(map[string]string, len(diff.Results))
		for _, entry := range diff.Results {
			paths[entry.Path] = entry.Type
		}
		require.Equal(t, "added", paths["data/b.txt"], "b.txt is the only data change between v1 and v2")
	})

	t.Run("v1_still_serves_old_data", func(t *testing.T) {
		require.Equal(t, []byte("a"), readObjectREST(t, env.core, dataset, "v1", "data/a.txt"))
		require.Equal(t, []byte("b"), readObjectREST(t, env.core, dataset, "v2", "data/b.txt"))
		// The S3 gateway sees exactly the same thing.
		require.Equal(t, []byte("b"), readObjectS3(t, env.s3, dataset, "v2/data/b.txt"))
		_, err := tryReadObjectS3(t, env.s3, dataset, "v1/data/b.txt")
		require.Error(t, err, "v1 must still hide the object added in v2")
	})

	t.Run("expected_version_conflict", func(t *testing.T) {
		stale := 1
		status, body := env.ds.do(t.Context(), http.MethodPost, dataset+"/versions", dsPublishRequest{
			Definition:      dsDefinition{FormatVersion: 1, Data: []dsDataItem{prefixItem(commitV2)}},
			Message:         "stale",
			ExpectedVersion: &stale,
		})
		require.Equalf(t, http.StatusConflict, status, "a stale expected_version must conflict: %s", body)
	})
}

// readObjectStatus reads an object and returns its body (when present) with the status code.
func readObjectStatus(t *testing.T, cl *apigen.ClientWithResponses, repo, ref, path string) ([]byte, int) {
	t.Helper()
	res, err := cl.GetObjectWithResponse(t.Context(), repo, ref, &apigen.GetObjectParams{Path: path})
	require.NoError(t, err, "read %s/%s/%s", repo, ref, path)
	if res.StatusCode() != http.StatusOK {
		return nil, res.StatusCode()
	}
	return res.Body, res.StatusCode()
}

// testDatasetsGCSafety covers §15.2 bullet 3 and §8.2: the source branch is deleted and the
// local GC flow runs; the pin branch must keep the pinned commit in the GC retain set, and the
// dataset must stay readable.
//
// The OSS GC deletion step runs in a Spark job, so the "local GC flow" here is everything the
// lakeFS server itself does: set the retention rules and prepare the GC commits. The resulting
// active-commits CSV is the contract the GC job consumes (pkg/datasets/pins_gc_test.go asserts
// the same artifact), so retaining the pinned commit there is exactly what protects the data.
func testDatasetsGCSafety(t *testing.T, env *datasetsEnv) {
	uid := uniqueSuffix()
	source := "ds-gc-" + uid
	env.createRepo(t, source, mainBranch)

	env.createBranch(t, source, "snap", mainBranch)
	env.putObject(t, env.s3, source, "snap", "data/customers/c1.parquet", datasetContentCustomers)
	pinnedCommit := env.commit(t, source, "snap", "payload the dataset needs")

	dataset := "gc-" + uid
	env.createDataset(t, dataset, dsDefinition{FormatVersion: 1, Data: []dsDataItem{{
		Target:     "customers",
		Repository: source,
		Ref:        dsRef{Type: "commit", ID: pinnedCommit},
		Type:       "prefix",
		Prefix:     &dsPath{Path: "data/customers/"},
	}}}, "v1")
	require.Equal(t, datasetContentCustomers, readObjectREST(t, env.core, dataset, "v1", "customers/c1.parquet"))

	// The pin branch exists in the source repository; it is hidden, so a default branch listing
	// does not show it (§8.2), which is why this asks for it by name.
	pinBranch := "_lakefs_ds_pin_" + pinnedCommit
	t.Run("pin_branch_exists", func(t *testing.T) {
		pin, err := env.core.GetBranchWithResponse(t.Context(), source, pinBranch)
		require.NoError(t, err, "get the pin branch %s/%s", source, pinBranch)
		require.Equalf(t, http.StatusOK, pin.StatusCode(),
			"the dataset must pin its source commit with the hidden branch %s: %s", pinBranch, pin.Body)
		require.NotNil(t, pin.JSON200)
		require.Equal(t, pinnedCommit, pin.JSON200.CommitId)
	})

	// The source branch goes away: the pinned commit is now reachable only from the pin branch.
	env.deleteBranch(t, source, "snap")

	t.Run("source_branch_is_gone", func(t *testing.T) {
		res, err := env.core.GetBranchWithResponse(t.Context(), source, "snap")
		require.NoError(t, err, "get deleted branch %s/snap", source)
		require.Equal(t, http.StatusNotFound, res.StatusCode())
	})

	// The local GC flow: retention rules, then commit preparation (the GC job's input).
	res, err := env.core.SetGCRulesWithResponse(t.Context(), source, apigen.SetGCRulesJSONRequestBody{
		// One-day retention for the default branch; the prepared commit graph is what decides
		// the rest, exactly as in a real run.
		Branches:             []apigen.GarbageCollectionRule{{BranchId: mainBranch, RetentionDays: 1}},
		DefaultRetentionDays: 1,
	})
	require.NoError(t, err, "set GC rules on %s", source)
	require.Equal(t, http.StatusNoContent, res.StatusCode(), string(res.Body))

	prepared := prepareGCCommits(t, env, source)
	active := readActiveCommits(t, prepared)

	_, pinnedActive := active[pinnedCommit]
	require.Truef(t, pinnedActive,
		"§8.2: the pinned commit must be in the GC retain set after its source branch was deleted; retain set: %v", active)

	// And the dataset is still readable end to end after the GC flow.
	require.Equal(t, datasetContentCustomers, readObjectREST(t, env.core, dataset, "v1", "customers/c1.parquet"))
	require.Equal(t, datasetContentCustomers, readObjectS3(t, env.s3, dataset, "v1/customers/c1.parquet"))
}

// prepareGCCommits runs the GC preparation step (POST /repositories/{repo}/gc/prepare_commits)
// and returns its metadata, including the location of the active-commits CSV.
//
// The request is issued directly rather than through the generated client: the core controller
// answers with a single JSON body, but when the blockstore cannot presign the CSV (the local
// adapter, which is the default here) it writes an error body first and the result after it, so
// the payload holds two JSON values and the generated client refuses to decode it. Taking the
// last value that carries a commits location works for both shapes.
func prepareGCCommits(t *testing.T, env *datasetsEnv, repo string) apigen.GarbageCollectionPrepareResponse {
	t.Helper()
	path := "/repositories/" + repo + "/gc/prepare_commits"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, env.endpoint+path, nil)
	require.NoError(t, err, "build %s", path)
	req.SetBasicAuth(env.key, env.secret)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "prepare GC commits on %s", repo)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read the GC preparation response")
	// The controller answers with a single JSON body on a blockstore that can presign the CSV.
	// On the local adapter (and on emulators that cannot sign) it writes an error body first and
	// the result after it, so the payload holds two JSON values and the status is the error's.
	// Either way the result carries a commits location: fail only when there is none.

	var (
		out   apigen.GarbageCollectionPrepareResponse
		found bool
	)
	decoder := json.NewDecoder(bytes.NewReader(body))
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			break
		}
		var candidate apigen.GarbageCollectionPrepareResponse
		if err := json.Unmarshal(raw, &candidate); err == nil && candidate.GcCommitsLocation != "" {
			out, found = candidate, true
		}
	}
	require.Truef(t, found, "no GC commit preparation result in the response (status %d): %s", resp.StatusCode, body)
	return out
}

// readActiveCommits reads the GC active-commits CSV written by the commit preparation step and
// returns the set of commit IDs the GC job will retain.
//
// On a cloud blockstore the CSV lives in the blockstore and is downloaded through the presigned
// URL the preparation response carries; on the local adapter it is a local:// file, which is
// what the local adapter formats full addresses as. If neither is available the assertion is
// skipped with the reason, instead of failing the whole suite on a blockstore that cannot be
// read back here.
func readActiveCommits(t *testing.T, prepared apigen.GarbageCollectionPrepareResponse) map[string]struct{} {
	t.Helper()
	reader := openActiveCommits(t, prepared)
	defer reader.Close()

	records, err := csv.NewReader(reader).ReadAll()
	require.NoError(t, err, "parse the GC active-commits CSV")
	require.NotEmpty(t, records, "the GC preparation must write at least a header")
	require.Equal(t, []string{"commit_id", "expired", "metarange_id"}, records[0],
		"the CSV header is the GC job's contract")

	active := make(map[string]struct{}, len(records)-1)
	for _, row := range records[1:] {
		require.Len(t, row, 3)
		if row[1] == "false" {
			active[row[0]] = struct{}{}
		}
	}
	return active
}

// openActiveCommits returns the GC active-commits CSV, from the presigned URL when the configured
// blockstore provides one and from the local filesystem for the local adapter.
func openActiveCommits(t *testing.T, prepared apigen.GarbageCollectionPrepareResponse) io.ReadCloser {
	t.Helper()
	if prepared.GcCommitsPresignedUrl != nil && *prepared.GcCommitsPresignedUrl != "" {
		resp, err := http.Get(*prepared.GcCommitsPresignedUrl) //nolint:gosec // test-only, URL from the local server
		require.NoError(t, err, "GET the presigned GC active-commits CSV")
		require.Equalf(t, http.StatusOK, resp.StatusCode, "GET the presigned GC active-commits CSV: %s", resp.Status)
		return resp.Body
	}
	location := prepared.GcCommitsLocation
	if path, ok := strings.CutPrefix(location, "local://"); ok {
		file, err := os.Open(path)
		require.NoErrorf(t, err, "open the GC active-commits CSV at %s", path)
		return file
	}
	t.Skipf("the GC commit preparation wrote its active-commits CSV to %s, but the configured blockstore "+
		"exposes no presigned URL for it and it is not a local file; skipping the GC retain-set assertion", location)
	return nil
}

// testDatasetsProposals covers §15.2 bullet 4 and §5.6: open a proposal with a preview branch,
// browse the preview data, merge it, and close a second one.
func testDatasetsProposals(t *testing.T, env *datasetsEnv) {
	uid := uniqueSuffix()
	source := "ds-proposals-" + uid
	env.createRepo(t, source, mainBranch)
	env.putObject(t, env.s3, source, mainBranch, "data/a.txt", []byte("a"))
	baseCommit := env.commit(t, source, mainBranch, "a")

	dataset := "proposals-" + uid
	baseDefinition := dsDefinition{FormatVersion: 1, Data: []dsDataItem{{
		Target:     "data",
		Repository: source,
		Ref:        dsRef{Type: "commit", ID: baseCommit},
		Type:       "prefix",
		Prefix:     &dsPath{Path: "data/"},
	}}}
	env.createDataset(t, dataset, baseDefinition, "v1")

	// A second source object, committed after the dataset's base version.
	env.putObject(t, env.s3, source, mainBranch, "data/c.txt", []byte("c"))
	proposalCommit := env.commit(t, source, mainBranch, "c")
	proposalDefinition := dsDefinition{FormatVersion: 1, Data: []dsDataItem{
		baseDefinition.Data[0],
		{
			Target:     "extra/c.txt",
			Repository: source,
			Ref:        dsRef{Type: "commit", ID: proposalCommit},
			Type:       "object",
			Object:     &dsPath{Path: "data/c.txt"},
		},
	}}

	var proposal dsProposal
	env.ds.must(t.Context(), http.StatusCreated, http.MethodPost, dataset+"/proposals",
		dsProposalCreateRequest{Definition: proposalDefinition, Title: "add c"}, &proposal)
	require.Equal(t, 1, proposal.BaseVersion)
	require.Equal(t, "open", proposal.Status)
	require.NotEmpty(t, proposal.PreviewBranch, "the preview branch is returned for reviewers")

	t.Run("preview_is_browsable", func(t *testing.T) {
		// §5.6: the proposal is materialised on the hidden branch, so reviewers can read it.
		require.Equal(t, []byte("c"), readObjectREST(t, env.core, dataset, proposal.PreviewBranch, "extra/c.txt"))
		_, status := readObjectStatus(t, env.core, dataset, "latest", "extra/c.txt")
		require.Equal(t, http.StatusNotFound, status, "the proposal must not be visible on latest before merging")
	})

	t.Run("merge_publishes_v2", func(t *testing.T) {
		var merged dsVersion
		env.ds.must(t.Context(), http.StatusCreated, http.MethodPost, dataset+"/proposals/"+proposal.ID+"/merge",
			map[string]string{"message": "merge proposal"}, &merged)
		require.Equal(t, 2, merged.Version)
		require.Equal(t, proposal.ID, merged.ProposalID)
		require.Equal(t, []byte("c"), readObjectREST(t, env.core, dataset, "latest", "extra/c.txt"))
	})

	t.Run("close_discards_the_preview", func(t *testing.T) {
		var second dsProposal
		env.ds.must(t.Context(), http.StatusCreated, http.MethodPost, dataset+"/proposals",
			dsProposalCreateRequest{Definition: proposalDefinition, Title: "discard me"}, &second)

		var closed dsProposal
		env.ds.must(t.Context(), http.StatusOK, http.MethodPatch, dataset+"/proposals/"+second.ID,
			map[string]string{"status": "closed"}, &closed)
		require.Equal(t, "closed", closed.Status)
		require.Nil(t, closed.MergedVersion)

		_, status := readObjectStatus(t, env.core, dataset, second.PreviewBranch, "extra/c.txt")
		require.Equal(t, http.StatusNotFound, status, "closing a proposal removes its preview branch")
	})
}

// testDatasetsAccessGrant covers §15.2 bullet 5 and §9.2/§9.6: a user who is in no base ACL group
// can read the dataset after a grant - and nothing else.
func testDatasetsAccessGrant(t *testing.T, env *datasetsEnv) {
	uid := uniqueSuffix()
	source := "ds-access-" + uid
	env.createRepo(t, source, mainBranch)
	env.putObject(t, env.s3, source, mainBranch, "data/customers/c1.parquet", datasetContentCustomers)
	commit := env.commit(t, source, mainBranch, "customers")

	dataset := "access-" + uid
	env.createDataset(t, dataset, dsDefinition{FormatVersion: 1, Data: []dsDataItem{{
		Target:     "customers",
		Repository: source,
		Ref:        dsRef{Type: "commit", ID: commit},
		Type:       "prefix",
		Prefix:     &dsPath{Path: "data/customers/"},
	}}}, "v1")

	// A fresh consumer: not a member of Readers/Writers/Admins (§9.6), so it starts with no
	// permissions at all.
	consumer := "consumer-" + uid
	userRes, err := env.core.CreateUserWithResponse(t.Context(), apigen.CreateUserJSONRequestBody{Id: consumer})
	require.NoError(t, err, "create user %s", consumer)
	require.Equalf(t, http.StatusCreated, userRes.StatusCode(), "create user %s: %s", consumer, userRes.Body)

	credentialsRes, err := env.core.CreateCredentialsWithResponse(t.Context(), consumer)
	require.NoError(t, err, "create credentials for %s", consumer)
	require.Equalf(t, http.StatusCreated, credentialsRes.StatusCode(), "create credentials: %s", credentialsRes.Body)
	require.NotNil(t, credentialsRes.JSON201)
	consumerKey, consumerSecret := credentialsRes.JSON201.AccessKeyId, credentialsRes.JSON201.SecretAccessKey

	consumerCore := env.coreFor(t, consumerKey, consumerSecret)
	consumerDS := env.dsFor(consumerKey, consumerSecret)
	consumerS3 := env.s3For(t, consumerKey, consumerSecret)

	t.Run("no_access_before_the_grant", func(t *testing.T) {
		_, err := tryReadObjectS3(t, consumerS3, dataset, "v1/customers/c1.parquet")
		require.Error(t, err, "a user in no group must not read the dataset")

		status, body := consumerDS.do(t.Context(), http.MethodGet, dataset, nil)
		require.NotEqualf(t, http.StatusOK, status, "a user in no group must not read the dataset definition: %s", body)
	})

	// §9.2: the grant creates and attaches the managed read policy.
	env.ds.must(t.Context(), http.StatusCreated, http.MethodPost, dataset+"/access",
		dsAccessGrantRequest{PrincipalType: "user", PrincipalID: consumer}, new(dsAccessGrant))

	t.Run("granted_user_reads_the_dataset", func(t *testing.T) {
		require.Equal(t, datasetContentCustomers, readObjectS3(t, consumerS3, dataset, "v1/customers/c1.parquet"),
			"the grant must allow the data plane read")
		require.Equal(t, datasetContentCustomers, readObjectREST(t, consumerCore, dataset, "v1", "customers/c1.parquet"))

		var got dsDataset
		consumerDS.must(t.Context(), http.StatusOK, http.MethodGet, dataset, nil, &got)
		require.Equal(t, 1, got.LatestVersion)
	})

	t.Run("granted_user_does_not_gain_the_sources", func(t *testing.T) {
		// §9.2: consumers need a grant only on the dataset, never on its sources.
		_, status := readObjectStatus(t, consumerCore, source, mainBranch, "data/customers/c1.parquet")
		require.NotEqualf(t, http.StatusOK, status, "the dataset grant must not leak source repository access")
	})

	t.Run("granted_user_cannot_publish", func(t *testing.T) {
		status, body := consumerDS.do(t.Context(), http.MethodPost, dataset+"/versions", dsPublishRequest{
			Definition: dsDefinition{FormatVersion: 1, Data: []dsDataItem{}},
			Message:    "not allowed",
		})
		require.NotEqualf(t, http.StatusCreated, status, "a read grant must not allow publishing: %s", body)
	})

	t.Run("grants_are_listed_and_revoked", func(t *testing.T) {
		var grants struct {
			Results []dsAccessGrant `json:"results"`
		}
		env.ds.must(t.Context(), http.StatusOK, http.MethodGet, dataset+"/access", nil, &grants)
		require.Len(t, grants.Results, 1)
		require.Equal(t, consumer, grants.Results[0].PrincipalID)

		env.ds.must(t.Context(), http.StatusNoContent, http.MethodDelete,
			dataset+"/access/user/"+consumer, nil, nil)

		_, err := tryReadObjectS3(t, consumerS3, dataset, "v1/customers/c1.parquet")
		require.Error(t, err, "after the revoke the consumer must lose access again")
	})
}
