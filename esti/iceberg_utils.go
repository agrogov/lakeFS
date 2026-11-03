// Package esti holds the lakeFS system tests.
//
// iceberg_utils.go is the harness of the Apache Iceberg REST catalog read-path smoke test
// (ICEBERG_CATALOG_PLAN.md §11 I-1 exit criteria, §11 I-2 task I2-8, §12 "Esti"). It starts a
// lakefs-sso server on an S3-compatible blockstore - MinIO in the OSS test environment - with
// iceberg.enabled, and it runs the two published client recipes (PyIceberg through
// /iceberg/api, DuckDB through /iceberg/relative_to/{repo}.{branch}/api) inside docker, so that a
// run needs neither a locally installed client nor a particular laptop setup (§12: "run
// PyIceberg, DuckDB and Spark in docker against lakefs-sso on MinIO").
//
// # What the harness provides
//
//   - newIcebergEnv starts the throw-away server. The catalog is mounted by cmd/lakefs-sso only
//     (§4.1), so the stock lakefs binary in the esti container cannot serve it: the binary is
//     built from the sources next to the tests when <binaries_dir>/lakefs-sso is missing.
//   - It allocates one unique prefix below the esti storage namespace and creates a repository
//     under it, so the run writes into the bucket the ambient environment already configured
//     (MinIO locally, a real S3 bucket in CI) and removes exactly its own objects afterwards.
//   - writeFixture writes an Iceberg table *outside* the catalog with PyIceberg, registerFixtureTable
//     adopts it with the catalog's register operation (I2-6), and readWithClients reads it back
//     through both clients. Every client step is a self-contained Python program handed to the
//     container as a file, which keeps the recipes readable instead of a shell one-liner.
//
// # Skipping
//
// The suite skips - rather than fails - whenever it cannot run, following the conventions the
// rest of esti uses (RequireBlockstoreType, RequirePostgresDB, and the datasets suite's
// "binary missing" / "feature disabled" skips):
//
//   - the ambient blockstore is not S3 (LAKEFS_BLOCKSTORE_TYPE is unset or names another
//     adapter): the clients write the fixture's files with their own credentials into the bucket
//     the server reads from, so a single-bucket S3 environment is required;
//   - there is no docker CLI, or no reachable docker daemon;
//   - the docker image cannot provide the clients (pyiceberg, pyarrow, duckdb) because it has no
//     package index to install them from;
//   - the catalog does not advertise the register operation, which is what makes a table written
//     outside the catalog readable through it (§11 I-2 task I2-6).
//
// Local run, against a MinIO the environment already points lakeFS at:
//
//	LAKEFS_BLOCKSTORE_TYPE=s3 \
//	LAKEFS_BLOCKSTORE_S3_ENDPOINT=http://127.0.0.1:9000 \
//	LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID=minio \
//	LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY=minio123 \
//	LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE=true \
//	ESTI_STORAGE_NAMESPACE=s3://esti-system-testing \
//	go test -count=1 -run TestIcebergReadPath -v ./esti --args --system-tests
//
// ESTI_ICEBERG_CLIENT_IMAGE points the client steps at a prebuilt image that already carries
// pyiceberg, pyarrow and duckdb; without it the harness uses python:3.12-slim and installs them
// with pip.
//
// Clean-room note (ICEBERG_CATALOG_PLAN.md §2, DATASETS_DESIGN.md §2): written from
// ICEBERG_CATALOG_PLAN.md §5, §6.1, §7.1, §11, §12, the Apache Iceberg REST OpenAPI catalog spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml), the Apache
// Iceberg table spec (https://iceberg.apache.org/spec/#table-metadata), the public PyIceberg and
// DuckDB documentation (https://py.iceberg.apache.org/, https://duckdb.org/docs/stable/core_extensions/iceberg/overview)
// and the public lakeFS documentation (https://docs.lakefs.io/iceberg/). No lakeFS Enterprise code
// and no upstream lakeFS >= 1.87 (BSL 1.1) code was used.
//
//nolint:unused // the harness is used by the esti tests only, and .golangci.yml does not analyse tests
package esti

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	// icebergBranch is the branch the fixture table is registered on. Both addressing forms of
	// §5 use it: as a namespace level through /iceberg/api, and as the warehouse segment of
	// /iceberg/relative_to/{repo}.{branch}/api.
	icebergBranch = "main"

	// icebergNamespace is the client-visible namespace of the fixture table: the levels below
	// the repository and the ref, which are the catalog's two macro levels (§5).
	icebergNamespace = "sales"

	// icebergTable is the fixture table's name.
	icebergTable = "orders"

	// icebergAdminUsername is the principal the setup call creates on the throw-away server.
	icebergAdminUsername = "esti-iceberg"

	// icebergClientImageEnv names the docker image the client steps run in. A prebuilt image
	// that already has pyiceberg, pyarrow and duckdb makes a run hermetic and fast; without it
	// the default image installs them when a step starts.
	icebergClientImageEnv = "ESTI_ICEBERG_CLIENT_IMAGE"

	// defaultIcebergClientImage is a python image with pip and nothing else: every client
	// dependency is installed by the step itself, which keeps the recipes architecture-neutral.
	defaultIcebergClientImage = "python:3.12-slim"

	// icebergClientPackages is what a step installs when the image does not provide the clients.
	// sql-sqlite is the SqlCatalog the fixture writer needs (no catalog server is involved in
	// writing the fixture files), s3fs is the FileIO of an s3:// warehouse, and pyarrow is what
	// both the writer and the PyIceberg reader use for data.
	icebergClientPackages = `"pyiceberg[sql-sqlite,s3fs,pyarrow]" duckdb`

	// icebergClientInstallFailed is the exit code of a step whose dependency install failed, and
	// the marker it prints. It is how the harness tells "this environment has no package index"
	// (skip) from "the read path broke" (fail).
	icebergClientInstallFailed = 97
	icebergClientInstallMark   = "ICEBERG_CLIENT_INSTALL_FAILED"
	icebergClientImportProbe   = "import pyiceberg, pyarrow, duckdb"
	icebergClientResultMarker  = "RESULT:"
	icebergClientStepTimeout   = 20 * time.Minute

	// icebergClientScriptEnv is how a step's program reaches the container. It is passed in the
	// environment and run with `python -c` rather than bind-mounted: a mount would tie the suite
	// to the directories the container runtime shares with the host (a temporary directory under
	// /var/folders is not shared by Docker Desktop, for example), while an environment variable
	// works everywhere the rest of esti works.
	icebergClientScriptEnv = "ICEBERG_STEP_SCRIPT"

	// icebergConfigFileMode is the mode of the generated server configuration: readable by the
	// test process alone, because it carries the blockstore credentials.
	icebergConfigFileMode = 0o600

	// icebergSuffixLength is the length of the random suffix shared by the repository name and
	// the run prefix.
	icebergSuffixLength = 8

	// icebergHealthTimeout is the timeout of a single health probe while the server starts, and
	// icebergHealthInterval the pause between two probes.
	icebergHealthTimeout  = 5 * time.Second
	icebergHealthInterval = 200 * time.Millisecond

	// icebergRequestTimeout bounds one catalog request issued by the test process.
	icebergRequestTimeout = 2 * time.Minute

	icebergDockerInfoTimeout    = 2 * time.Minute
	icebergServerTimeout        = 3 * time.Minute
	icebergCleanupTimeout       = 5 * time.Minute
	icebergContainerHostAlias   = "host.docker.internal"
	icebergContainerHostGateway = "host.docker.internal:host-gateway"
	icebergS3Scheme             = "s3"
	icebergDefaultS3Host        = "s3.amazonaws.com"
	icebergFixtureNamespace     = "fixture"
	icebergFixtureTableName     = "orders"
	icebergFixtureSQLiteName    = "catalog.db"
)

// icebergS3Config is the S3 configuration the server, the test process and the client containers
// share. Endpoint keeps its scheme (PyIceberg takes a URL) and is empty when the installation uses
// AWS itself, in which case only Host - the endpoint without a scheme, for the S3 client of the
// test process (DuckDB takes the same form) - is filled in.
type icebergS3Config struct {
	Endpoint  string
	Host      string
	Secure    bool
	Region    string
	PathStyle bool
	AccessKey string
	SecretKey string
}

// icebergEnv is one running lakefs-sso server with iceberg.enabled, plus everything the client
// steps need to reach it and the bucket behind it.
type icebergEnv struct {
	// baseURL is the server's root URL as the test process sees it.
	baseURL string
	// catalogURL is the identifier-form catalog mount (§5 row 3) and relativeToURL the
	// warehouse-form mount (§5 rows 4 and 5).
	catalogURL    string
	relativeToURL string

	// repo, branch, namespace and table name the fixture table.
	repo      string
	branch    string
	namespace string
	table     string

	// bucket and runPrefix locate the run's objects; storageNamespace is the s3:// namespace of
	// the repository, i.e. what a client writes table files to and what register validates
	// against.
	bucket           string
	runPrefix        string
	storageNamespace string

	// s3 is the blockstore configuration the server and the clients share.
	s3 icebergS3Config

	// key and secret are the lakeFS credentials of the fixture principal: the server's admin,
	// who holds AllAccess (fs:*, catalog:*, ...) and therefore passes the catalog's
	// authorization checks (authz.go) on its own repository.
	key    string
	secret string

	// clientImage is the docker image the client steps run in.
	clientImage string

	// workDir is the temporary directory the server runs in, and logPath its log file.
	workDir string
	logPath string

	// fixtureUUID is the uuid directory of the fixture table below <storage_namespace>/_iceberg.
	fixtureUUID string
}

// icebergFixture is what the writer step reports about the table it wrote outside the catalog.
type icebergFixture struct {
	MetadataLocation string
	IDs              []int64
	DataFiles        []string
}

// icebergReadResult is what the read step reports about the registered table.
type icebergReadResult struct {
	PyIcebergIDs       []int64
	PyIcebergLocations []string
	DuckDBIDs          []int64
	DuckDBRelation     string
}

// newIcebergEnv starts lakefs-sso with the Iceberg catalog enabled on the ambient S3 blockstore
// and returns the environment. It skips the calling test when the environment cannot host the
// run; see the file comment for the reasons.
func newIcebergEnv(t *testing.T) *icebergEnv {
	t.Helper()
	requireIcebergS3Blockstore(t)
	requireIcebergDocker(t)

	dir := t.TempDir()
	binary := icebergServerBinary(t)

	port := icebergFreePort(t)
	// The server listens on every interface, and the test process talks to it on the loopback
	// address. The client containers reach it as host.docker.internal, which is a different
	// address than the host's loopback on a container runtime that runs its own virtual machine,
	// so a server bound to 127.0.0.1 alone would be unreachable for them.
	listenAddress := fmt.Sprintf("0.0.0.0:%d", port)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	env := &icebergEnv{
		baseURL:       baseURL,
		catalogURL:    baseURL + icebergcatalog.APIBasePath,
		relativeToURL: baseURL + icebergcatalog.RelativeToBasePath,
		repo:          "iceberg-read-path-" + icebergUniqueSuffix(),
		branch:        icebergBranch,
		namespace:     icebergNamespace,
		table:         icebergTable,
		key:           DefaultAdminAccessKeyID,
		secret:        DefaultAdminSecretAccessKey,
		clientImage:   icebergClientImage(),
		workDir:       dir,
		logPath:       filepath.Join(dir, "lakefs-sso.log"),
	}
	env.s3 = icebergS3Settings(t)
	env.bucket, env.runPrefix, env.storageNamespace = icebergStorageNamespace(t, env.repo)

	// The catalog's installation-wide deny policy is created through the auth service, so the
	// server has to run with an auth service that can store policies (see icebergServerConfig).
	issuerURL := icebergOIDCIssuer(t)

	configPath := filepath.Join(dir, "lakefs.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(icebergServerConfig(listenAddress, dir, env.s3, issuerURL)), icebergConfigFileMode))

	logFile, err := os.Create(env.logPath)
	require.NoError(t, err, "create the server log file")

	// The server runs in the temporary directory: nothing it writes may land in the repository.
	cmd := exec.CommandContext(t.Context(), binary, "run", "--config", configPath)
	cmd.Dir = dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Registered before the stop cleanup, so it runs after the process is gone and the log is
	// complete: a failing run is otherwise very hard to debug from CI.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		log, err := os.ReadFile(env.logPath)
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
	})
	// Cleanups run LIFO, so this one is registered after the server is stopped: the run prefix is
	// removed from the bucket only when nothing writes to it any more. t.Context() is already
	// cancelled at that point, hence the fresh context.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), icebergCleanupTimeout)
		defer cancel()
		if err := env.deleteRunPrefix(ctx); err != nil {
			t.Errorf("delete the iceberg run prefix s3://%s/%s: %v", env.bucket, env.runPrefix, err)
		}
	})

	icebergWaitForServer(t, baseURL, env.logPath)

	// First run on the throw-away server: create the admin principal with known credentials,
	// exactly as `lakefs setup` would.
	anonymous, err := apigen.NewClientWithResponses(env.baseURL + apiutil.BaseURL)
	require.NoError(t, err, "build the anonymous core API client")
	setup, err := anonymous.SetupWithResponse(t.Context(), apigen.SetupJSONRequestBody{
		Username:       icebergAdminUsername,
		Key:            &apigen.AccessKeyCredentials{AccessKeyId: env.key, SecretAccessKey: env.secret},
		FeatureUpdates: swag.Bool(false),
	})
	require.NoError(t, err, "setup lakefs-sso")
	require.Equalf(t, http.StatusOK, setup.StatusCode(), "setup lakefs-sso: %s", setup.Body)

	env.createRepository(t)
	env.requireCatalogEnabled(t)
	return env
}

// requireIcebergS3Blockstore skips the test unless the ambient blockstore is S3.
//
// The fixture is written by the clients themselves, and only an S3-compatible store lets the test
// process, the server and a container address the same bucket with the same credentials. The
// check reads an explicitly set variable rather than a default: esti installs s3 as the default
// blockstore type for every run, so a default would not tell "configured for this suite" from
// "never configured at all".
func requireIcebergS3Blockstore(t *testing.T) {
	t.Helper()
	blockstoreType := strings.ToLower(os.Getenv("LAKEFS_BLOCKSTORE_TYPE"))
	if blockstoreType != block.BlockstoreTypeS3 {
		t.Skipf("the Iceberg read-path system test runs on an S3-compatible blockstore (MinIO locally, S3 in CI): "+
			"LAKEFS_BLOCKSTORE_TYPE=%q", blockstoreType)
	}
	namespace := viper.GetString(ViperStorageNamespaceKey)
	if namespace == "" {
		t.Skipf("LAKEFS_BLOCKSTORE_TYPE=s3 is set but the esti storage_namespace is empty; " +
			"set ESTI_STORAGE_NAMESPACE to a bucket prefix this suite may allocate under")
	}
	if scheme := namespaceScheme(t, namespace); scheme != icebergS3Scheme {
		t.Skipf("the esti storage_namespace %q is not an s3:// namespace but a %q one", namespace, scheme)
	}
}

// icebergS3Settings resolves the S3 endpoint, region and credentials the server and the clients
// share. It reads the configuration keys the lakeFS documentation publishes for an S3-compatible
// store (blockstore.s3.endpoint, force_path_style) and the AWS credentials esti already uses, so
// pointing the suite at MinIO is the same configuration a deployment uses.
func icebergS3Settings(t *testing.T) icebergS3Config {
	t.Helper()
	endpoint := os.Getenv("LAKEFS_BLOCKSTORE_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("AWS_ENDPOINT_URL")
	}
	if endpoint != "" && !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	cfg := icebergS3Config{Endpoint: endpoint}
	if endpoint == "" {
		cfg.Host = icebergDefaultS3Host
		cfg.Secure = true
	} else {
		u, err := url.Parse(endpoint)
		require.NoErrorf(t, err, "parse the S3 endpoint %q", endpoint)
		require.NotEmptyf(t, u.Host, "the S3 endpoint %q has no host", endpoint)
		cfg.Host = u.Host
		cfg.Secure = u.Scheme == "https"
	}

	cfg.Region = os.Getenv("AWS_REGION")
	cfg.AccessKey = os.Getenv("LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID")
	cfg.SecretKey = os.Getenv("LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY")
	if cfg.Region == "" {
		cfg.Region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.AccessKey == "" {
		cfg.AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if cfg.SecretKey == "" {
		cfg.SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		t.Skipf("the Iceberg read-path system test needs S3 credentials in " +
			"LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID/_SECRET_ACCESS_KEY or AWS_ACCESS_KEY_ID/_SECRET_ACCESS_KEY")
	}
	// Path-style addressing is what an S3-compatible store that is not AWS (MinIO, LocalStack)
	// needs, and what bucket-as-subdomain cannot express on a loopback host. AWS itself keeps
	// virtual-hosted addressing unless the installation asked for path style.
	cfg.PathStyle = strings.EqualFold(os.Getenv("LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE"), "true") ||
		isLoopbackHost(cfg.Host) || !strings.HasSuffix(cfg.Host, "amazonaws.com")
	return cfg
}

// icebergStorageNamespace allocates one prefix per run below the esti storage namespace and
// returns the bucket, that prefix, and the s3:// namespace of the repository inside it.
func icebergStorageNamespace(t *testing.T, repo string) (bucket, runPrefix, namespace string) {
	t.Helper()
	u, err := url.Parse(viper.GetString(ViperStorageNamespaceKey))
	require.NoError(t, err, "parse the esti storage_namespace")
	bucket = u.Host
	runPrefix = path.Join(strings.Trim(u.Path, "/"), "iceberg-"+icebergUniqueSuffix())
	namespace = "s3://" + bucket + "/" + path.Join(runPrefix, repo)
	return bucket, runPrefix, namespace
}

// icebergOIDCIssuer serves an OpenID Connect discovery document from the test process and returns
// its issuer URL, for the sso: section icebergServerConfig writes.
//
// It exists to select lakefs-sso's ACL auth service, the only auth service of this binary that
// implements the policy writes pkg/icebergcatalog.Bootstrap needs (see icebergServerConfig); the
// binary builds its OIDC-aware authentication service only when sso.enabled is true, and that
// constructor discovers the issuer at startup. No login is ever performed and no token is ever
// verified through it: every client of this suite authenticates with a lakeFS access key pair, so
// the jwks_uri is advertised and never fetched.
func icebergOIDCIssuer(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/.well-known/openid-configuration") {
			http.NotFound(w, r)
			return
		}
		// gooidc requires the document's issuer to equal the URL it was discovered from, so it is
		// derived from the request rather than from the listener address.
		issuer := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                issuer,
			"authorization_endpoint":                issuer + "/authorize",
			"token_endpoint":                        issuer + "/token",
			"jwks_uri":                              issuer + "/jwks",
			"userinfo_endpoint":                     issuer + "/userinfo",
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"scopes_supported":                      []string{"openid", "profile", "email"},
		}); err != nil {
			t.Errorf("write the OIDC discovery document: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// icebergServerConfig is the generated lakefs.yaml: the listen address of the free port, a
// warning-level log, a local KV store in the temporary directory, the auth secret every lakeFS
// installation needs, the S3 blockstore the clients also point at, the sso: section that selects
// the ACL auth service, and the iceberg section of ICEBERG_CATALOG_PLAN.md §10.
//
// The blockstore section is written out in full instead of being inherited from the ambient
// LAKEFS_* variables: a partial section would shadow viper's nested defaults, and the server, the
// test process and the client containers must all address the same bucket with the same
// credentials and path style for the fixture's files to be readable through the catalog.
//
// The sso: section is a workaround for a defect of the catalog, not a property of this suite.
// pkg/icebergcatalog.Bootstrap creates and attaches the installation-wide
// iceberg-reserved-prefix-protect deny policy through the auth service, and fails startup when
// that write fails. With the default auth mode lakefs-sso runs auth.BasicAuthService, whose
// WritePolicy/GetPolicy/AttachPolicyToGroup answer auth.ErrNotImplemented, so with iceberg.enabled
// and no sso: section the server refuses to start with
//
//	icebergcatalog: read policy iceberg-reserved-prefix-protect: not implemented
//
// and no catalog test of this suite can run at all (the run is recorded in
// pkg/icebergcatalog/conformance/RESULTS.md). sso.enabled selects the ACL auth service, which
// does implement those calls, so the suite can start a server whose catalog is enabled. Drop this
// section once the catalog degrades gracefully (or correctly) when the auth service cannot store
// policies.
func icebergServerConfig(listenAddress, dir string, s3 icebergS3Config, issuerURL string) string {
	var blockstore strings.Builder
	blockstore.WriteString("blockstore:\n  type: s3\n  s3:\n")
	fmt.Fprintf(&blockstore, "    region: %q\n", s3.Region)
	if s3.Endpoint != "" {
		fmt.Fprintf(&blockstore, "    endpoint: %q\n", s3.Endpoint)
	}
	fmt.Fprintf(&blockstore, "    force_path_style: %t\n", s3.PathStyle)
	// The region is configured, so there is nothing to discover: leaving discovery on would make
	// the adapter ask the store for a bucket location on the read path, which an S3-compatible
	// store that is not AWS (MinIO, the fake S3 the tests use) does not always implement.
	blockstore.WriteString("    discover_bucket_region: false\n")
	fmt.Fprintf(&blockstore, "    credentials:\n      access_key_id: %q\n      secret_access_key: %q\n",
		s3.AccessKey, s3.SecretKey)

	return fmt.Sprintf(`listen_address: %q
logging:
  level: "warn"
database:
  type: local
  local:
    path: %q
auth:
  encrypt:
    secret_key: "esti-iceberg-read-path-secret-key"
sso:
  enabled: true
  client_id: "esti-iceberg"
  client_secret: "esti-iceberg-client-secret"
  issuer_url: %q
  callback_base_url: %q
%siceberg:
  enabled: true
  prefix: %q
`, listenAddress, filepath.Join(dir, "metadata"), issuerURL, "http://"+listenAddress,
		blockstore.String(), icebergcatalog.CatalogPrefix)
}

// icebergServerBinary returns the lakefs-sso binary to run, building it from source when the
// binaries directory does not have it: the catalog is mounted by cmd/lakefs-sso only (§4.1), and
// the esti container ships just the lakefs and lakectl binaries in /app.
func icebergServerBinary(t *testing.T) string {
	t.Helper()
	installed := lakeBinaryLocation("lakefs-sso")
	if _, err := os.Stat(installed); err == nil {
		return installed
	}

	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("the Iceberg system tests need the lakefs-sso binary (%s) and there is no Go toolchain to build it: %v", installed, err)
	}
	packageDir, err := os.Getwd()
	require.NoError(t, err, "locate the esti package directory")
	repoRoot := filepath.Dir(packageDir)
	cmdDir := filepath.Join(repoRoot, "cmd", "lakefs-sso")
	if _, err := os.Stat(cmdDir); err != nil {
		t.Skipf("the Iceberg system tests need the lakefs-sso binary (%s) and its sources (%s): %v", installed, cmdDir, err)
	}

	built := filepath.Join(t.TempDir(), "lakefs-sso")
	build := exec.CommandContext(t.Context(), goTool, "build", "-o", built, "./cmd/lakefs-sso")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Skipf("cannot build lakefs-sso for the Iceberg system tests: %v\n%s", err, output)
	}
	return built
}

// requireIcebergDocker skips the test when no docker daemon is reachable: the client steps run in
// containers by design (§12), so the suite has nothing to fall back to.
func requireIcebergDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("the Iceberg read-path system test runs its clients (PyIceberg, DuckDB) in docker: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), icebergDockerInfoTimeout)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("the Iceberg read-path system test needs a running docker daemon: %v\n%s", err, output)
	}
}

// icebergClientImage is the image the client steps run in, defaulting to a bare python image the
// step installs into.
func icebergClientImage() string {
	if image := os.Getenv(icebergClientImageEnv); image != "" {
		return image
	}
	return defaultIcebergClientImage
}

// icebergFreePort asks the kernel for a free TCP port on the loopback interface.
func icebergFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "allocate a free port")
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

// icebergUniqueSuffix is a short random suffix shared by the repository name and the run prefix.
func icebergUniqueSuffix() string {
	return strings.ToLower(randstr.String(icebergSuffixLength))
}

// namespaceScheme returns the URL scheme of a storage namespace, so that a namespace which does
// not address the configured adapter is reported instead of written to.
func namespaceScheme(t *testing.T, namespace string) string {
	t.Helper()
	u, err := url.Parse(namespace)
	require.NoErrorf(t, err, "parse the storage namespace %q", namespace)
	return strings.ToLower(u.Scheme)
}

// isLoopbackHost reports whether a host names the local machine.
func isLoopbackHost(host string) bool {
	switch host {
	case "localhost", "::1":
		return true
	}
	return strings.HasPrefix(host, "127.")
}

// icebergWaitForServer blocks until /_health answers, or fails the test with the server log.
func icebergWaitForServer(t *testing.T, baseURL, logPath string) {
	t.Helper()
	deadline := time.Now().Add(icebergServerTimeout)
	client := &http.Client{Timeout: icebergHealthTimeout}
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/_health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(icebergHealthInterval)
	}
	log, _ := os.ReadFile(logPath)
	t.Fatalf("lakefs-sso did not become healthy at %s within %s; server log:\n%s", baseURL, icebergServerTimeout, log)
}

// createRepository creates the repository the fixture table is registered in, on the namespace
// allocated below the esti storage prefix.
func (e *icebergEnv) createRepository(t *testing.T) {
	t.Helper()
	cl, err := testutil.NewClientFromCreds(logging.ContextUnavailable(), e.key, e.secret, e.baseURL+apiutil.BaseURL)
	require.NoError(t, err, "build the core API client")
	res, err := cl.CreateRepositoryWithResponse(t.Context(), &apigen.CreateRepositoryParams{},
		apigen.CreateRepositoryJSONRequestBody{
			Name:             e.repo,
			StorageNamespace: e.storageNamespace,
			DefaultBranch:    swag.String(e.branch),
		})
	require.NoError(t, err, "create repository %s", e.repo)
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "create repository %s: %s", e.repo, res.Body)
}

// requireCatalogEnabled skips the test when the catalog - or the operation this suite depends on
// - is not served. GET /v1/config is the negotiation endpoint of §2.1, and its endpoints list is
// the catalog's own statement about which operations are mounted (http.go advertisedEndpoints).
func (e *icebergEnv) requireCatalogEnabled(t *testing.T) {
	t.Helper()
	status, body := e.doJSON(t, http.MethodGet, e.catalogURL+"/v1/config", nil)
	if status == http.StatusNotImplemented || status == http.StatusNotFound {
		t.Skipf("the Iceberg catalog is disabled on this server (%d: %s); skipping the read-path smoke test",
			status, strings.TrimSpace(string(body)))
	}
	require.Equalf(t, http.StatusOK, status, "GET /v1/config: %s", body)

	var config struct {
		Overrides map[string]string `json:"overrides"`
		Endpoints []string          `json:"endpoints"`
	}
	require.NoErrorf(t, json.Unmarshal(body, &config), "decode GET /v1/config: %s", body)
	require.Equal(t, icebergcatalog.CatalogPrefix, config.Overrides["prefix"])

	// register (I2-6) is what makes a table written outside the catalog readable through it, and
	// it is this suite's dependency: without it the read path below has nothing to read.
	if !containsString(config.Endpoints, icebergRegisterEndpoint) {
		t.Skipf("the catalog does not advertise %q, so a fixture table cannot be registered; "+
			"skipping the read-path smoke test", icebergRegisterEndpoint)
	}
}

// icebergRegisterEndpoint is the register operation of the Iceberg REST spec (the register-table
// request), in the literal-{prefix} form §2.1 requires an advertised endpoint to use.
const icebergRegisterEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/register"

// writeFixture writes an Iceberg table *outside* the catalog and returns what the writer step
// reported. PyIceberg writes the data, manifest and metadata files with its own FileIO, which is
// exactly the "the client writes the files, the catalog adopts the pointer" flow of §6.2 and of
// register (I2-6): everything lands under the managed table location inside the repository's
// storage namespace, and nothing is staged as a lakeFS entry yet.
func (e *icebergEnv) writeFixture(t *testing.T) icebergFixture {
	t.Helper()
	result := e.runClient(t, "write-fixture", icebergFixtureScript, map[string]string{
		"ICEBERG_WAREHOUSE":         e.fixtureLocation(),
		"ICEBERG_SQLITE_DIR":        "/tmp",
		"ICEBERG_SQLITE_NAME":       icebergFixtureSQLiteName,
		"ICEBERG_FIXTURE_NAMESPACE": icebergFixtureNamespace,
		"ICEBERG_FIXTURE_TABLE":     icebergFixtureTableName,
	})

	fixture := icebergFixture{
		MetadataLocation: stringField(t, result, "metadata_location"),
		IDs:              int64SliceField(t, result, "ids"),
		DataFiles:        stringSliceField(t, result, "data_files"),
	}
	require.NotEmpty(t, fixture.MetadataLocation, "the fixture writer must report the metadata.json it wrote")
	require.NotEmpty(t, fixture.IDs, "the fixture table must have rows")
	// The managed layout of §6.1/§14 Q-I5: every file the fixture wrote lives under
	// <storage_namespace>/_iceberg/<uuid>/, which is what register validates and what the
	// uncommitted-GC grace protects (§6.3).
	require.Truef(t, strings.HasPrefix(fixture.MetadataLocation, e.fixtureLocation()+"/"),
		"the fixture metadata location %q must be inside %q", fixture.MetadataLocation, e.fixtureLocation())
	for _, file := range fixture.DataFiles {
		require.Truef(t, strings.HasPrefix(file, e.fixtureLocation()+"/"),
			"the fixture data file %q must be inside %q", file, e.fixtureLocation())
	}
	return fixture
}

// createFixtureNamespace creates the client-visible namespace through the catalog (I2-2), so that
// register (I2-6) adopts the table into a namespace that exists.
func (e *icebergEnv) createFixtureNamespace(t *testing.T) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"namespace":  []string{e.repo, e.branch, e.namespace},
		"properties": map[string]string{},
	})
	require.NoError(t, err, "marshal the create-namespace request")
	status, response := e.doJSON(t, http.MethodPost, e.namespacesURL(e.catalogURL), body)
	require.Equalf(t, http.StatusOK, status, "create namespace %s: %s", e.namespace, response)
}

// registerFixtureTable adopts the fixture's metadata.json as the table pointer (I2-6): the request
// carries the table name and the metadata location inside the managed table area, and the catalog
// stages the pointer on the branch without touching a single data file.
func (e *icebergEnv) registerFixtureTable(t *testing.T, fixture icebergFixture) {
	t.Helper()
	e.createFixtureNamespace(t)
	body, err := json.Marshal(map[string]string{
		"name":              e.table,
		"metadata-location": fixture.MetadataLocation,
	})
	require.NoError(t, err, "marshal the register request")
	status, response := e.doJSON(t, http.MethodPost, e.namespaceURL(e.catalogURL)+"/register", body)
	require.Equalf(t, http.StatusOK, status, "register %s: %s", e.table, response)

	var registered struct {
		MetadataLocation string `json:"metadata-location"`
	}
	require.NoErrorf(t, json.Unmarshal(response, &registered), "decode the register response: %s", response)
	require.Equal(t, fixture.MetadataLocation, registered.MetadataLocation,
		"register must adopt the metadata.json it was given")
}

// requireLoadTableAgrees asserts that both addressing forms of §5 answer the same table: the
// identifier form (/iceberg/api, repository and ref as the leading namespace levels) and the
// warehouse form (/iceberg/relative_to/{repo}.{branch}/api, with the client's own levels below
// them) must return the same metadata-location for the same table (I1-10).
func (e *icebergEnv) requireLoadTableAgrees(t *testing.T, wantMetadataLocation string) {
	t.Helper()
	load := func(name, url string) {
		status, body := e.doJSON(t, http.MethodGet, url, nil)
		require.Equalf(t, http.StatusOK, status, "load table through %s: %s", name, body)
		var loaded struct {
			MetadataLocation string `json:"metadata-location"`
		}
		require.NoErrorf(t, json.Unmarshal(body, &loaded), "decode load table through %s: %s", name, body)
		require.Equal(t, wantMetadataLocation, loaded.MetadataLocation,
			"%s must serve the registered metadata.json", name)
	}

	load("the identifier form", e.tableURL(e.catalogURL))
	load("the relative_to form", e.relativeToTableURL(e.branch))
}

// readWithClients reads the registered table with PyIceberg through /iceberg/api and with DuckDB
// through /iceberg/relative_to/{repo}.{branch}/api - the two published client recipes that §1.1
// goals 1-3 name, run in docker by icebergReadScript.
func (e *icebergEnv) readWithClients(t *testing.T) icebergReadResult {
	t.Helper()
	identifier, err := json.Marshal([]string{e.repo, e.branch, e.namespace, e.table})
	require.NoError(t, err, "marshal the PyIceberg identifier")
	result := e.runClient(t, "read-path", icebergReadScript, map[string]string{
		"ICEBERG_WAREHOUSE":       e.storageNamespace,
		"ICEBERG_IDENTIFIER":      string(identifier),
		"ICEBERG_CREDENTIAL":      e.key + ":" + e.secret,
		"ICEBERG_CATALOG_URI":     e.dockerReachableURL(e.catalogURL),
		"ICEBERG_RELATIVE_URI":    e.dockerReachableURL(e.relativeToURL) + "/" + e.repo + "." + e.branch + "/api",
		"ICEBERG_OAUTH_URI":       e.dockerReachableURL(e.catalogURL) + "/v1/oauth/tokens",
		"ICEBERG_DUCKDB_RELATION": e.namespace + "." + e.table,
	})
	return icebergReadResult{
		PyIcebergIDs:       int64SliceField(t, result, "pyiceberg_ids"),
		PyIcebergLocations: stringSliceField(t, result, "pyiceberg_metadata_locations"),
		DuckDBIDs:          int64SliceField(t, result, "duckdb_ids"),
		DuckDBRelation:     stringField(t, result, "duckdb_relation"),
	}
}

// runClient runs one dockerised client step and returns the JSON object it printed.
//
// The step's program reaches the container in its environment and runs there, and every knob it
// needs arrives the same way: the s3:// warehouse, the S3 endpoint and credentials (the clients
// read and write the table's files themselves), and the catalog URLs. The container reaches the
// test process and the bucket through host.docker.internal, which works both on a Linux CI runner
// (--add-host ...:host-gateway) and on Docker Desktop.
func (e *icebergEnv) runClient(t *testing.T, step, script string, extraEnv map[string]string) map[string]any {
	t.Helper()
	env := e.clientEnv()
	env[icebergClientScriptEnv] = script
	for key, value := range extraEnv {
		env[key] = value
	}

	args := []string{"run", "--rm", "--add-host", icebergContainerHostGateway}
	for _, key := range sortedKeys(env) {
		args = append(args, "-e", key+"="+env[key])
	}
	args = append(args, e.clientImage, "sh", "-c", icebergClientCommand())

	ctx, cancel := context.WithTimeout(t.Context(), icebergClientStepTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if bytes.Contains(output, []byte(icebergClientInstallMark)) ||
			(cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == icebergClientInstallFailed) {
			t.Skipf("the docker image %q cannot provide the Iceberg clients (pyiceberg, pyarrow, duckdb): "+
				"set %s to an image that has them, or give the run access to a package index\n%s",
				e.clientImage, icebergClientImageEnv, output)
		}
		t.Fatalf("the Iceberg client step %q failed: %v\n%s", step, err, output)
	}

	result := icebergResult(t, step, output)
	t.Logf("iceberg client step %q reported: %v", step, result)
	return result
}

// clientEnv is the environment every client step receives: the bucket and the endpoints, with the
// loopback addresses rewritten for a container.
func (e *icebergEnv) clientEnv() map[string]string {
	// An empty endpoint is the AWS default of the clients' S3 FileIO: it is sent as empty rather
	// than as a URL, so a run against AWS keeps the SDK's own endpoint resolution (regions,
	// redirects) instead of pinning one endpoint.
	endpoint := e.dockerReachableURL(e.s3.Endpoint)
	return map[string]string{
		"ICEBERG_S3_ENDPOINT":   endpoint,
		"ICEBERG_S3_HOST":       strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://"),
		"ICEBERG_S3_REGION":     e.s3.Region,
		"ICEBERG_S3_PATH_STYLE": fmt.Sprint(e.s3.PathStyle),
		"ICEBERG_S3_KEY":        e.s3.AccessKey,
		"ICEBERG_S3_SECRET":     e.s3.SecretKey,
		// The AWS SDKs the clients use send request checksums by default, which an S3-compatible
		// store older than the checksum trailers (MinIO before 2024, LocalStack) rejects. Asking
		// for them only when an operation requires one keeps the suite runnable against the
		// stores lakeFS itself supports; AWS treats the setting as a harmless relaxation.
		"AWS_REQUEST_CHECKSUM_CALCULATION": "when_required",
		"AWS_RESPONSE_CHECKSUM_VALIDATION": "when_required",
	}
}

// dockerReachableURL rewrites a loopback URL so a container can reach it. Docker Desktop and a
// Linux runner with --add-host ...:host-gateway both resolve host.docker.internal to the host.
func (e *icebergEnv) dockerReachableURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if !isLoopbackHost(u.Hostname()) {
		return raw
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	u.Host = net.JoinHostPort(icebergContainerHostAlias, port)
	return u.String()
}

// fixtureLocation is the managed table location of §6.1/§14 Q-I5:
//
//	<storage_namespace>/_iceberg/<uuid>
//
// The uuid is generated here rather than taken from the metadata: it is what makes the location of
// a drop-and-recreate unique, and register only requires the location to be inside the catalog's
// table area (location.go normalizeTableLocation does the same check for create).
func (e *icebergEnv) fixtureLocation() string {
	if e.fixtureUUID == "" {
		e.fixtureUUID = icebergUniqueSuffix() + "-" + icebergUniqueSuffix()
	}
	return strings.TrimRight(e.storageNamespace, "/") + "/" + icebergcatalog.DataPrefix + "/" + e.fixtureUUID
}

// namespacesURL is the ListNamespaces/CreateNamespace URL of a catalog mount (§2.1), with the
// literal prefix the catalog advertises in overrides.prefix.
func (e *icebergEnv) namespacesURL(mount string) string {
	return mount + "/v1/" + icebergcatalog.CatalogPrefix + "/namespaces"
}

// namespaceURL is the URL of the fixture table's namespace below a catalog mount. The namespace
// parameter carries the levels of §5 joined with the Iceberg namespace separator, percent-encoded
// as the spec requires (ident.go NamespaceSeparator/EncodedNamespaceSeparator). Register (I2-6) is
// an operation of the namespace, not of a table, so it hangs off this URL.
func (e *icebergEnv) namespaceURL(mount string) string {
	return e.refNamespaceURL(mount, e.branch)
}

// refNamespaceURL is namespaceURL for an arbitrary ref: the namespace levels of §5 with ref in the
// second place. A tag and a commit id are refs like a branch is, and reads are served on all three
// (§5 "Reads work on any ref"), so the same URL shape addresses a table at a tag.
func (e *icebergEnv) refNamespaceURL(mount, ref string) string {
	encoded := make([]string, 0, len([]string{e.repo, ref, e.namespace}))
	for _, level := range []string{e.repo, ref, e.namespace} {
		encoded = append(encoded, url.PathEscape(level))
	}
	return e.namespacesURL(mount) + "/" + strings.Join(encoded, icebergcatalog.EncodedNamespaceSeparator)
}

// tableURL is the LoadTable URL of the fixture table below a catalog mount.
func (e *icebergEnv) tableURL(mount string) string {
	return e.refTableURL(mount, e.branch)
}

// refTableURL is tableURL for an arbitrary ref.
func (e *icebergEnv) refTableURL(mount, ref string) string {
	return e.refNamespaceURL(mount, ref) + "/tables/" + url.PathEscape(e.table)
}

// relativeToTableURL is the LoadTable URL of the fixture table on the relative_to mount. The URL
// fixes the repository and the ref (§5 rows 4-5), so the path parameter carries the client's own
// namespace levels only: the catalog's RelativeTo middleware prepends the two fixed levels to
// whatever the parameter holds, and sending them again would address [repo, ref, repo, ref, ns].
func (e *icebergEnv) relativeToTableURL(ref string) string {
	mount := e.relativeToURL + "/" + e.repo + "." + ref + "/api"
	return e.namespacesURL(mount) + "/" + url.PathEscape(e.namespace) + "/tables/" + url.PathEscape(e.table)
}

// doJSON issues one catalog request with basic auth and returns the status and the body.
func (e *icebergEnv) doJSON(t *testing.T, method, rawURL string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, rawURL, bytes.NewReader(body))
	require.NoErrorf(t, err, "build %s %s", method, rawURL)
	req.SetBasicAuth(e.key, e.secret)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: icebergRequestTimeout}).Do(req)
	require.NoErrorf(t, err, "call %s %s", method, rawURL)
	defer func() { _ = resp.Body.Close() }()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(resp.Body)
	require.NoErrorf(t, err, "read %s %s", method, rawURL)
	return resp.StatusCode, buf.Bytes()
}

// deleteRunPrefix removes everything this run wrote below the allocated prefix. lakeFS never
// deletes physical objects by itself, so the suite removes its own objects with the same
// credentials the server uses.
func (e *icebergEnv) deleteRunPrefix(ctx context.Context) error {
	lookup := minio.BucketLookupAuto
	if e.s3.PathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(e.s3.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(e.s3.AccessKey, e.s3.SecretKey, ""),
		Secure:       e.s3.Secure,
		Region:       e.s3.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return fmt.Errorf("build the S3 client: %w", err)
	}
	objects := client.ListObjects(ctx, e.bucket, minio.ListObjectsOptions{
		Prefix:    e.runPrefix,
		Recursive: true,
	})
	var failure error
	for object := range client.RemoveObjects(ctx, e.bucket, objects, minio.RemoveObjectsOptions{}) {
		if object.Err != nil && failure == nil {
			failure = object.Err
		}
	}
	if failure != nil {
		return fmt.Errorf("remove objects under %q: %w", e.runPrefix, failure)
	}
	return nil
}

// icebergClientCommand is the shell a step runs: it installs the clients when the image does not
// already provide them, and then runs the step's program. A failed install prints the marker the
// harness turns into a skip, because "no package index" is an environment limitation and not a
// catalog failure.
func icebergClientCommand() string {
	return "python -c '" + icebergClientImportProbe + "' >/dev/null 2>&1 || " +
		"pip install --quiet --disable-pip-version-check " + icebergClientPackages + " || " +
		"{ echo " + icebergClientInstallMark + "; exit " + fmt.Sprint(icebergClientInstallFailed) + "; }; " +
		"exec python -c \"$" + icebergClientScriptEnv + "\""
}

// icebergResult extracts the JSON object a step printed after the result marker.
func icebergResult(t *testing.T, step string, output []byte) map[string]any {
	t.Helper()
	var payload []byte
	for _, line := range bytes.Split(output, []byte("\n")) {
		trimmed := bytes.TrimSpace(line)
		if bytes.HasPrefix(trimmed, []byte(icebergClientResultMarker)) {
			payload = bytes.TrimPrefix(trimmed, []byte(icebergClientResultMarker))
		}
	}
	require.NotNilf(t, payload, "the Iceberg client step %q printed no %q line\n%s", step, icebergClientResultMarker, output)
	var result map[string]any
	require.NoErrorf(t, json.Unmarshal(payload, &result), "decode the result of step %q: %s", step, payload)
	return result
}

// stringField reads one string out of a step's result.
func stringField(t *testing.T, result map[string]any, key string) string {
	t.Helper()
	raw, ok := result[key]
	require.Truef(t, ok, "the step result has no %q field: %v", key, result)
	value, ok := raw.(string)
	require.Truef(t, ok, "%q of the step result is not a string: %v", key, raw)
	return value
}

// stringSliceField reads a list of strings out of a step's result.
func stringSliceField(t *testing.T, result map[string]any, key string) []string {
	t.Helper()
	raw, ok := result[key]
	require.Truef(t, ok, "the step result has no %q field: %v", key, result)
	items, ok := raw.([]any)
	require.Truef(t, ok, "%q of the step result is not a list: %v", key, raw)
	values := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		require.Truef(t, ok, "%q of the step result holds a non-string item: %v", key, item)
		values = append(values, value)
	}
	return values
}

// boolField reads one boolean out of a step's result.
func boolField(t *testing.T, result map[string]any, key string) bool {
	t.Helper()
	raw, ok := result[key]
	require.Truef(t, ok, "the step result has no %q field: %v", key, result)
	value, ok := raw.(bool)
	require.Truef(t, ok, "%q of the step result is not a boolean: %v", key, raw)
	return value
}

// int64SliceField reads a list of JSON numbers out of a step's result.
func int64SliceField(t *testing.T, result map[string]any, key string) []int64 {
	t.Helper()
	raw, ok := result[key]
	require.Truef(t, ok, "the step result has no %q field: %v", key, result)
	items, ok := raw.([]any)
	require.Truef(t, ok, "%q of the step result is not a list: %v", key, raw)
	values := make([]int64, 0, len(items))
	for _, item := range items {
		number, ok := item.(float64)
		require.Truef(t, ok, "%q of the step result holds a non-number item: %v", key, item)
		values = append(values, int64(number))
	}
	return values
}

// sortedKeys returns the keys of a map in order, so that a docker command line is stable.
func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// containsString reports whether values contains want.
func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// createTag cuts tag at ref. A tag is a ref a reader addresses by name afterwards, which is what
// makes a released version of a table readable at <repo>.<tag>.<namespace>.<table> (§1.1 goal 1,
// §5 "Reads work on any ref").
func (e *icebergEnv) createTag(t *testing.T, cl *apigen.ClientWithResponses, tag, ref string) {
	t.Helper()
	res, err := cl.CreateTagWithResponse(t.Context(), e.repo, apigen.CreateTagJSONRequestBody{Id: tag, Ref: ref})
	require.NoErrorf(t, err, "create tag %s at %s", tag, ref)
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "create tag %s at %s: %s", tag, ref, res.Body)
}

// dropTableThroughCatalog drops the fixture table through the catalog on ref: PyIceberg's
// drop_table, i.e. the DELETE /v1/.../tables/{table} operation (I2-5), which stages the deletion of
// the pointer entry exactly as create staged its addition (§6.2).
func (e *icebergEnv) dropTableThroughCatalog(t *testing.T, ref string) {
	t.Helper()
	result := e.runClient(t, "drop:"+ref, icebergDropScript, map[string]string{
		"ICEBERG_CATALOG_URI": e.dockerReachableURL(e.catalogURL),
		"ICEBERG_CREDENTIAL":  e.key + ":" + e.secret,
		"ICEBERG_IDENTIFIER":  icebergIdentifierJSON(t, e.repo, ref, e.namespace, e.table),
		"ICEBERG_WAREHOUSE":   e.storageNamespace,
	})
	require.Truef(t, boolField(t, result, "dropped"), "drop %q through the catalog on %s must succeed", e.table, ref)
}

// requireTableAbsent asserts that the catalog answers 404 NoSuchTable for the fixture table at ref.
// After a drop this is the observable outcome on that ref, before and after the lakeFS commit that
// publishes it - a drop is staged like every other catalog write (§6.2).
func (e *icebergEnv) requireTableAbsent(t *testing.T, ref string) {
	t.Helper()
	for _, mount := range []struct {
		name string
		url  string
	}{
		{"the identifier form", e.refTableURL(e.catalogURL, ref)},
		{"the relative_to form", e.relativeToTableURL(ref)},
	} {
		status, body := e.doJSON(t, http.MethodGet, mount.url, nil)
		require.Equalf(t, http.StatusNotFound, status,
			"loading %q at %s through %s must be 404 after the drop: %s", e.table, ref, mount.name, body)
		require.Containsf(t, string(body), icebergNoSuchTableType,
			"the answer through %s must be typed %s: %s", mount.name, icebergNoSuchTableType, body)
	}
}

// requireTableListed asserts whether the namespace listing at ref carries the fixture table: the
// table is an entry of the namespace, so a staged drop removes it from the listing as well as from
// the load path.
func (e *icebergEnv) requireTableListed(t *testing.T, ref string, want bool) {
	t.Helper()
	status, body := e.doJSON(t, http.MethodGet, e.refNamespaceURL(e.catalogURL, ref)+"/tables", nil)
	require.Equalf(t, http.StatusOK, status, "list the tables of %s at %s: %s", e.namespace, ref, body)

	var listing struct {
		Identifiers []struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifiers"`
	}
	require.NoErrorf(t, json.Unmarshal(body, &listing), "decode the table listing: %s", body)

	names := make([]string, 0, len(listing.Identifiers))
	for _, identifier := range listing.Identifiers {
		names = append(names, identifier.Name)
	}
	if want {
		require.Containsf(t, names, e.table, "the listing of %s at %s must contain %q", e.namespace, ref, e.table)
		return
	}
	require.NotContainsf(t, names, e.table, "the listing of %s at %s must not contain %q after the drop",
		e.namespace, ref, e.table)
}

// requireRefReadOnly asserts that a client which tries to append through ref is refused with 403
// and the documented "ref is read-only" message (Q-I4): writes need a branch, and a tag or a commit
// id is not one.
func (e *icebergEnv) requireRefReadOnly(t *testing.T, ref string) {
	t.Helper()
	result := e.runClient(t, "write-refused:"+ref, icebergWriteRefusedScript, map[string]string{
		"ICEBERG_CATALOG_URI": e.dockerReachableURL(e.catalogURL),
		"ICEBERG_CREDENTIAL":  e.key + ":" + e.secret,
		"ICEBERG_IDENTIFIER":  icebergIdentifierJSON(t, e.repo, ref, e.namespace, e.table),
		"ICEBERG_WAREHOUSE":   e.storageNamespace,
	})
	require.Truef(t, boolField(t, result, "refused"),
		"an append addressed at %s must fail: writes to a tag are read-only (Q-I4)", ref)
	errorText := stringField(t, result, "error")
	require.Containsf(t, errorText, "ref is read-only",
		"the refusal on %s must say the ref is read-only: %s", ref, errorText)
}

// icebergNoSuchTableType is the type of the 404 answer for a table that does not exist.
const icebergNoSuchTableType = "NoSuchTableException"

// icebergIdentifierJSON encodes a full catalog identifier for a client step: the namespace levels of
// §5, i.e. [repository, ref, namespace, table].
func icebergIdentifierJSON(t *testing.T, repo, ref, namespace, table string) string {
	t.Helper()
	encoded, err := json.Marshal([]string{repo, ref, namespace, table})
	require.NoError(t, err, "marshal the PyIceberg identifier")
	return string(encoded)
}

// icebergFixtureScript writes the fixture table outside the catalog.
//
// A SqlCatalog over a throw-away SQLite file is only a way to drive PyIceberg's table writer: the
// catalog's database lives in the container and is discarded with it, and the table itself is
// written to the s3:// warehouse given by ICEBERG_WAREHOUSE with the client's own FileIO, exactly
// as an external writer would. Nothing here talks to lakeFS.
const icebergFixtureScript = `
import json
import os

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


warehouse = required("ICEBERG_WAREHOUSE")
properties = {
    "uri": "sqlite:///" + os.path.join(os.environ.get("ICEBERG_SQLITE_DIR", "/tmp"),
                                       os.environ.get("ICEBERG_SQLITE_NAME", "catalog.db")),
    "warehouse": warehouse,
}
if os.environ.get("ICEBERG_S3_ENDPOINT"):
    properties["s3.endpoint"] = os.environ["ICEBERG_S3_ENDPOINT"]
    properties["s3.path-style-access"] = os.environ.get("ICEBERG_S3_PATH_STYLE", "true")
for key, env_name in (("s3.access-key-id", "ICEBERG_S3_KEY"),
                      ("s3.secret-access-key", "ICEBERG_S3_SECRET"),
                      ("s3.region", "ICEBERG_S3_REGION")):
    if os.environ.get(env_name):
        properties[key] = os.environ[env_name]

catalog = SqlCatalog("fixture", **properties)
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
])
rows = [
    {"id": 1, "name": "alpha"},
    {"id": 2, "name": "beta"},
    {"id": 3, "name": "gamma"},
]
# The location is the managed table location of ICEBERG_CATALOG_PLAN.md §6.1, so register can
# adopt the pointer and the uncommitted-GC grace (§6.3) covers the files from the first byte on.
catalog.create_namespace_if_not_exists(required("ICEBERG_FIXTURE_NAMESPACE"))
table = catalog.create_table(
    required("ICEBERG_FIXTURE_NAMESPACE") + "." + required("ICEBERG_FIXTURE_TABLE"),
    schema=schema,
    location=warehouse,
)
table.append(pa.Table.from_pylist(rows, schema=schema))

print("RESULT:" + json.dumps({
    "metadata_location": table.metadata_location,
    "ids": sorted(row["id"] for row in rows),
    "data_files": [task.file.file_path for task in table.scan().plan_files()],
}))
`

// icebergReadScript reads the registered table with both published clients.
//
// PyIceberg uses the identifier form of §5: the RestCatalog it builds prepends the prefix from
// GET /v1/config and sends the namespace levels the catalog returned, so the identifier is
// [repository, ref, namespace, table].
//
// DuckDB uses the warehouse form: its ATTACH endpoint is
// /iceberg/relative_to/{repository}.{branch}/api, so the URL fixes the repository and the ref and
// the relation it queries is the client's own namespace and table - which is the point of that
// addressing form (§1.1 goal 2) and of the published DuckDB recipe.
const icebergReadScript = `
import json
import os

import duckdb
from pyiceberg.catalog.rest import RestCatalog


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


s3_properties = {
    "s3.access-key-id": required("ICEBERG_S3_KEY"),
    "s3.secret-access-key": required("ICEBERG_S3_SECRET"),
    "s3.region": os.environ.get("ICEBERG_S3_REGION", "us-east-1"),
}
if os.environ.get("ICEBERG_S3_ENDPOINT"):
    s3_properties["s3.endpoint"] = os.environ["ICEBERG_S3_ENDPOINT"]
    s3_properties["s3.path-style-access"] = os.environ.get("ICEBERG_S3_PATH_STYLE", "true")

# --- PyIceberg, through /iceberg/api ---------------------------------------------------------
catalog = RestCatalog(
    name="lakefs",
    uri=required("ICEBERG_CATALOG_URI"),
    credential=required("ICEBERG_CREDENTIAL"),
    warehouse=required("ICEBERG_WAREHOUSE"),
    **s3_properties,
)
identifier = tuple(json.loads(required("ICEBERG_IDENTIFIER")))
table = catalog.load_table(identifier)
pyiceberg_rows = table.scan().to_arrow().to_pylist()

# --- DuckDB, through /iceberg/relative_to/{repo}.{branch}/api --------------------------------
relation = required("ICEBERG_DUCKDB_RELATION")
con = duckdb.connect()
con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")
con.execute(
    "CREATE OR REPLACE SECRET lakefs_catalog (TYPE iceberg, CLIENT_ID %s, "
    "CLIENT_SECRET %s, OAUTH2_SERVER_URI %s)" % (
        sql_literal(required("ICEBERG_CREDENTIAL").split(":", 1)[0]),
        sql_literal(required("ICEBERG_CREDENTIAL").split(":", 1)[1]),
        sql_literal(required("ICEBERG_OAUTH_URI")),
    )
)
if os.environ.get("ICEBERG_S3_ENDPOINT"):
    con.execute(
        "CREATE OR REPLACE SECRET lakefs_data (TYPE s3, KEY_ID %s, SECRET %s, REGION %s, "
        "ENDPOINT %s, URL_STYLE 'path', USE_SSL %s)" % (
            sql_literal(required("ICEBERG_S3_KEY")),
            sql_literal(required("ICEBERG_S3_SECRET")),
            sql_literal(os.environ.get("ICEBERG_S3_REGION", "us-east-1")),
            sql_literal(required("ICEBERG_S3_HOST")),
            "true" if os.environ["ICEBERG_S3_ENDPOINT"].startswith("https://") else "false",
        )
    )
con.execute(
    "ATTACH %s AS lakefs (TYPE iceberg, ENDPOINT %s)" % (
        sql_literal(required("ICEBERG_WAREHOUSE")),
        sql_literal(required("ICEBERG_RELATIVE_URI")),
    )
)
duckdb_ids = sorted(row[0] for row in
                    con.execute("SELECT id FROM lakefs.%s" % relation).fetchall())

print("RESULT:" + json.dumps({
    "pyiceberg_ids": sorted(row["id"] for row in pyiceberg_rows),
    "pyiceberg_metadata_locations": [table.metadata_location],
    "duckdb_ids": duckdb_ids,
    "duckdb_relation": relation,
}))
`

// icebergDropScript drops a table through the catalog.
//
// The drop is the DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table} operation: the catalog
// stages the deletion of the pointer entry on the branch (§6.2), and the lakeFS commit that follows
// is what publishes it. The drop is not a purge - lakeFS garbage collection, not a client, decides
// when the files can go (§6.3) - so nothing here deletes an object.
const icebergDropScript = `
import json
import os

from pyiceberg.catalog.rest import RestCatalog


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


catalog = RestCatalog(
    name="lakefs",
    uri=required("ICEBERG_CATALOG_URI"),
    credential=required("ICEBERG_CREDENTIAL"),
    warehouse=required("ICEBERG_WAREHOUSE"),
)
identifier = tuple(json.loads(required("ICEBERG_IDENTIFIER")))
catalog.drop_table(identifier)
print("RESULT:" + json.dumps({"dropped": True, "identifier": list(identifier)}))
`

// icebergWriteRefusedScript tries to append to a table addressed at a tag or a commit id and reports
// the refusal. It is how the suite asserts the read-only rule of §5/Q-I4 from a client's side: the
// status a client sees (403) is what selects its exception class, so the assertion is on the message
// the catalog documents and on the fact that the write did not go through.
const icebergWriteRefusedScript = `
import json
import os

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


# The append rewrites data and manifests, so this step needs the same FileIO the append step uses.
file_io_properties = {
    "s3.access-key-id": required("ICEBERG_S3_KEY"),
    "s3.secret-access-key": required("ICEBERG_S3_SECRET"),
    "s3.region": os.environ.get("ICEBERG_S3_REGION", "us-east-1"),
}
if os.environ.get("ICEBERG_S3_ENDPOINT"):
    file_io_properties["s3.endpoint"] = os.environ["ICEBERG_S3_ENDPOINT"]
    file_io_properties["s3.path-style-access"] = os.environ.get("ICEBERG_S3_PATH_STYLE", "true")

catalog = RestCatalog(
    name="lakefs",
    uri=required("ICEBERG_CATALOG_URI"),
    credential=required("ICEBERG_CREDENTIAL"),
    warehouse=required("ICEBERG_WAREHOUSE"),
    **file_io_properties,
)
identifier = tuple(json.loads(required("ICEBERG_IDENTIFIER")))
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
])
table = catalog.load_table(identifier)
try:
    table.append(pa.Table.from_pylist([{"id": 99, "name": "refused"}], schema=schema))
except Exception as exc:  # the client's own exception class, whatever it chose for the status
    print("RESULT:" + json.dumps({"refused": True, "error": type(exc).__name__ + ": " + str(exc)}))
else:
    print("RESULT:" + json.dumps({"refused": False, "error": ""}))
`
