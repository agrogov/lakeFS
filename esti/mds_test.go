// Package esti holds the lakeFS system tests.
//
// mds_test.go is the Metadata Search end-to-end suite of METADATA_SEARCH_DESIGN.md §9 (task M6 of
// METADATA_SEARCH_PLAN.md): the security tests of the design, plus the object-lifecycle cases the
// index has to get right, run through both published query engines on both catalog mounts of
// docs/metadata-search.md - PyIceberg and DuckDB, each through the upstream endpoint
// /mds/iceberg/api (identifier form) and through /iceberg/relative_to/{repo}.{ref}/api (warehouse
// form) - and the timing of a 100k-object backfill.
//
// # What runs
//
// The suite starts its own lakefs-sso (like the Iceberg suite, iceberg_utils.go), because the
// indexer and the virtual table are served by cmd/lakefs-sso only:
//
//   - the server runs with iceberg.enabled and iceberg.metadata_search.enabled, on the ambient
//     S3-compatible blockstore (SeaweedFS locally - esti/ops/docker-compose-mds.yaml - and S3 in
//     CI), with the ACL auth service selected by an sso: section and rbac: internal, so that the
//     principals and policies of §8 can be created and enforced;
//   - installation.* seeds the break-glass admin before the server mounts its extra routes, so the
//     base groups (Admins, Supers, Writers, Readers, ...) exist when the Metadata Search bootstrap
//     of §8 attaches mds-metadata-repo-protect to them - which is what makes the deny real in this
//     run rather than a no-op;
//   - the server creates the mds-indexer user, the MetadataSearchIndexers group and both policies
//     (§8), and the suite - not the server - creates the credentials of mds-indexer, exactly as an
//     administrator does with `lakectl auth users credentials create --id mds-indexer`;
//   - `lakefs-sso mds run --config` then runs as a child process against that server, with the
//     static `storage` keys of §4.6 (SeaweedFS does not vend credentials) and period 2s, so a case
//     waits a couple of seconds for the index instead of a minute.
//
// # The cases (§9 "Security tests", M6)
//
// TestMetadataSearchEndToEnd walks one data repository R through the index's lifecycle and asserts
// the table of R-metadata after every step, through both engines:
//
//	upload + commit          rows appear (repository, path, size, etag, commit_id, committer,
//	                         content_type); the table property lakefs.mds.source-commit is the head
//	overwrite                the same row carries the new etag, size and commit_id
//	delete                   the row is gone
//	S3-gateway upload        the row carries the object's X-Amz-Meta-* user metadata
//	filtered scan            PyIceberg's row_filter and DuckDB's WHERE ... ORDER BY ... LIMIT
//	                         return the same data/ rows through all four engine/mount recipes
//	new branch               the branch is seeded from the mds-<commit> tag of the indexed
//	                         ancestor (§4.5 step 1), which its own history proves
//	merge                    the object a merge brings in gets the merge commit's id (§3.2)
//	tag, short commit id     both refs read the snapshot the index recorded for that commit (§6)
//	hard reset               the branch is rewritten to the indexed ancestor (§4.7)
//	indexer restart          a deterministic kill after the Iceberg commit and before the lakeFS
//	                         commit is recovered, with no replay and no leftover staging (§4.4)
//	user without R access    403 on the virtual table
//	user with R-metadata only  200 on R-metadata's own table, 403 through R.<ref>.system.object_metadata
//	non-indexer write        admin cannot write R-metadata (the deny of §8 is in force)
//	guard-group attach        a member of a base group cannot CreateRepository, bare or not, on
//	                          either accepted spelling of R-metadata's storage namespace: each is
//	                          403 from the deny (R3S-3), while an ordinary namespace is created
//	                          and deleted by the same principal
//	system on R rejects writes  a principal that may write R (not the admin) cannot create,
//	                            commit, rename or drop system.object_metadata on R, nor create or
//	                            drop the system namespace, through either mount (§5.3); the
//	                            virtual table still reads afterwards
//
// TestMetadataSearchBackfill pushes a 100k-object commit into R and times the first indexing of the
// branch - the full-listing rebuild of §4.5 step 2 - reporting the number. It is a measurement, not
// a gate: the assertions are about the rows that come out, and the duration is logged (and quoted
// in the commit that added this file).
//
// TestMetadataSearchCompactionAndTagRetention runs §4.9 on a real catalog with a low
// max_delete_files and a short tag_retention: the compaction commit, the pre-compaction tag, the
// unchanged rows and position, lakefs_mds_compactions_total, and the retirement of an old tag
// while the live position's tag stays. TestMetadataSearchBranchDeletion runs §4.8's mirrored
// deletion: the R-metadata branch is gone, its tags and history remain readable.
//
// mds_azure_test.go runs the acceptance cases of METADATA_SEARCH_AZURE.md §5 AZ5 on the same
// harness against an Azure blockstore (Azurite): it is the one place the harness takes another
// storage profile, and it is why this file's environment is split into a blockstore-neutral
// skeleton (newMDSEnvSkeleton, start) and the S3 profile (newMDSEnv).
//
// # No workarounds
//
// The suite drives the product exactly as a user would: the indexer runs from its own section of
// the same configuration file, under the principal and policy its bootstrap created, and the test
// does nothing on its behalf beyond creating that principal's credentials (which §8 leaves to an
// administrator). There is no compensating policy, no proxy in front of the server and no manual
// repository initialisation: if the indexer cannot create R-metadata, cannot reach the catalog or
// cannot record an indexed commit, the case that needs it fails and the failure names the product
// defect. Earlier drafts of this suite carried workarounds for four such defects (design §8's
// missing fs:AttachStorageNamespace, the catalog URL, the initialisation commit and an inclusive
// log stop_at); they were fixed in the product and every workaround is gone.
//
// # Skipping
//
// Following the conventions of the Iceberg suite (see iceberg_utils.go), the suite skips - it never
// fails - when the environment cannot host it: no S3 blockstore, no docker daemon for the query
// engines, no way to build lakefs-sso, or a docker image that cannot install pyiceberg/duckdb. A
// skip message names what is missing.
//
// # Clean-room note
//
// Written from METADATA_SEARCH_DESIGN.md §3.1-§4.9, §6-§9, the Apache Iceberg REST OpenAPI catalog
// spec, the Apache Iceberg table spec and the public PyIceberg and DuckDB documentation. No lakeFS
// Enterprise code and no upstream lakeFS >= 1.87 (BSL 1.1) code was used.
package esti

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	minioCredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/datasets"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/bootstrap"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	// mdsBranch is the branch the suite indexes and commits on. mdsFeatureBranch is the second
	// branch of the configured selectors: it covers the seeding of §4.5 and the merge of §3.2.
	mdsBranch        = "main"
	mdsFeatureBranch = "feature"

	// mdsTag is the tag the suite cuts on R and reads the virtual table at (§6: a tag maps to the
	// mds-<commit> tag of R-metadata, never to a branch).
	mdsTag = "mds-e2e-v1"

	// mdsAdminUsername is the break-glass admin the installation.* section seeds. It is the
	// committer of every commit the suite makes through the API, so rows can be asserted on it.
	mdsAdminUsername = "esti-mds-admin"

	// mdsIndexerPeriod is the metadata_search.period of the child indexer: a case waits at most a
	// few seconds for the next tick, instead of the 60s default. mdsIndexerPeriodDuration is the
	// same value for the code that waits on it.
	mdsIndexerPeriod         = "2s"
	mdsIndexerPeriodDuration = 2 * time.Second

	// mdsServerStartTimeout bounds the wait for the server's /_health, mdsIndexerStartTimeout
	// the wait for the indexer's own /_health. Both are short because each process is watched:
	// a process that exits fails the test at once instead of being waited out.
	mdsServerStartTimeout  = 2 * time.Minute
	mdsIndexerStartTimeout = 60 * time.Second

	// mdsIndexTimeout bounds one wait for the index to reach a commit: the indexer ticks every
	// mdsIndexerPeriod (2s), so a minute is many periods. The 100k backfill has its own bound.
	mdsIndexTimeout = 60 * time.Second

	// mdsBackfillIndexTimeout bounds the wait for the 100k-object backfill. The first indexing
	// writes one row per object and reads every object of the branch, so it is bounded at ten
	// minutes. (The 10k run indexed 2.7s after the indexer started.)
	mdsBackfillIndexTimeout = 10 * time.Minute

	// mdsBucketOperationTimeout bounds one bulk bucket operation of the backfill setup (writing
	// the objects, waiting for the import). It is not a poll bound: the work is proportional to
	// the object count on a single-node object store.
	mdsBucketOperationTimeout = 10 * time.Minute

	// mdsCleanupTimeout bounds the removal of the run's objects at teardown.
	mdsCleanupTimeout = 5 * time.Minute

	// mdsPollInterval is the pause between two health probes and between two status polls.
	mdsPollInterval = 500 * time.Millisecond

	// mdsRequestTimeout bounds one HTTP request the suite issues itself.
	mdsRequestTimeout = 30 * time.Second

	// mdsPolicyRefreshTimeout bounds the wait for the companion-repository deny policy to name a
	// repository created after the server started. The bootstrap's refresher runs every
	// bootstrap.policyRefreshInterval (one minute) and re-derives the whole policy set, so this is
	// one interval plus the store round trips. Missing it is a failed case: the deny of §8 would
	// not be in force at the moment the case asserts it.
	mdsPolicyRefreshTimeout = 90 * time.Second

	// mdsPolicyListAmount is how many policies one list call asks for: the policy set of the suite
	// is a handful of chunks, well below the API's default page.
	mdsPolicyListAmount = 100

	// mdsLogTailLines is how much of a dead process's log a failure reports.
	mdsLogTailLines = 50

	// mdsConfigFileMode is the mode of the generated configuration: it carries the blockstore
	// credentials and the indexer's key pair.
	mdsConfigFileMode = 0o600

	// mdsObjectContentType is the content type of the objects the suite uploads through the API,
	// so content_type is asserted rather than ignored.
	mdsObjectContentType = "text/plain"

	// mdsBackfillObjectsEnv overrides the number of objects of the backfill measurement. The
	// design (§9, plan M6) fixes 100k as the number to report; a smaller value is a development
	// convenience and the reported number says which one ran.
	mdsBackfillObjectsEnv = "ESTI_MDS_BACKFILL_OBJECTS"

	// mdsBackfillObjectsDefault is the size of the backfill commit the plan asks to time.
	mdsBackfillObjectsDefault = 100_000

	// mdsBackfillSetupEnv selects how the source commit of the backfill is built. It exists so
	// the two ways can be measured against each other (see buildBackfillCommit); an unset or
	// unknown value uses the cheap one.
	mdsBackfillSetupEnv = "ESTI_MDS_BACKFILL_SETUP"

	// mdsBackfillSetupShared imports one source object into count destinations: one object in
	// the bucket, many entries in the commit. This is the default.
	mdsBackfillSetupShared = "shared"

	// mdsBackfillSetupPrefix writes count source objects and imports their prefixes. It is kept
	// for the comparison run only.
	mdsBackfillSetupPrefix = "prefix"

	// mdsBackfillConcurrency is how many objects the prefix setup writes to the bucket at a time.
	mdsBackfillConcurrency = 256

	// mdsBackfillParts is the number of source prefixes the backfill is split into. lakeFS walks
	// each source prefix on its own inside the import work pool, so splitting the objects into
	// equal parts turns one long walk of a 100k-key prefix into many short ones - the difference
	// between an import that costs seconds and one that costs minutes on a single-node store.
	mdsBackfillParts = 100

	// mdsCleanupStreams is how many parallel removals the teardown runs. The single-node SeaweedFS
	// of the local recipe removes a 100k-object prefix slowly on one stream, and the run has to fit
	// in the suite's own time budget.
	mdsCleanupStreams = 16

	// mdsCommitWindowTimeout bounds the wait for the indexer to reach the lakeFS commit of
	// R-metadata after a data commit: the indexer ticks every mdsIndexerPeriod, so ten seconds is
	// several periods. Missing it is a failed case, not a slower run: the restart case must kill
	// inside the crash window of §4.4, not between ticks.
	mdsCommitWindowTimeout = 10 * time.Second

	// mdsHeldCommitTimeout bounds how long the intercepting proxy holds a lakeFS commit when the
	// test does not release it. It is a safety net for a failed test, not a wait the case relies
	// on.
	mdsHeldCommitTimeout = 30 * time.Second

	// mdsFilteredPathPrefix is the prefix of the filtered-scan example of
	// docs/metadata-search.md: the suite's objects live under data/ instead of orders/.
	mdsFilteredPathPrefix = "data/"

	// mdsGuardGroup is the base group the guard-group case adds its principal to. Admins is the
	// RBAC template's most privileged group - FSFullAccess is fs:* on every resource - so a
	// refusal of a CreateRepository by a member of it is the deny of §8 and not a missing grant.
	mdsGuardGroup = "Admins"
)

// mdsEnv is one running lakefs-sso with Metadata Search enabled, one running `lakefs-sso mds run`,
// and everything the suite needs to talk to both.
//
// It embeds the Iceberg suite's environment (iceberg_utils.go) for the parts that do not depend on
// how the server was started: the S3 settings and the run prefix, the docker client plumbing
// (runClient, clientEnv, dockerReachableURL), and the core-API helpers (coreAPI, commitBranch,
// headCommit, createBranch, createTag, requireMergeSucceeds).
type mdsEnv struct {
	*icebergEnv

	// binary is the lakefs-sso binary both the server and the indexer run from.
	binary string
	// dir holds the server's working directory, the KV store and the logs.
	dir string
	// configPath is the one lakefs.yaml both processes read: the server ignores the
	// metadata_search section, the indexer reads only it (§7).
	configPath string
	// indexPath is the indexer's log file; the server's is icebergEnv.logPath.
	indexPath string
	// indexerURL is the indexer's own HTTP server (/_health, /metrics, /_pprof/).
	indexerURL string
	// issuer is the OIDC discovery document the sso: section selects the ACL auth service with.
	// The indexer ignores it; only the server reads it, and only at startup.
	issuer string
	// listenAddress and indexerListenAddress are what the two processes bind to.
	listenAddress        string
	indexerListenAddress string

	// backfillObjects and backfillPrefix are the keys writeBackfillObjects wrote, known exactly
	// without listing. deleteRunPrefix removes them directly: on a single-node object store the
	// listing of a 100k-key prefix costs far more than the deletion itself. The shared-object
	// setup leaves them zero: it writes a single source object, which the ordinary listing sweep
	// removes.
	backfillObjects int
	backfillPrefix  string

	// ctx is cancelled when a child process exits while the test still needs it, so that every
	// request and every wait stops at once instead of running into its own timeout.
	ctx    context.Context
	cancel context.CancelFunc

	// server and indexer are the two watched child processes; indexer is nil while it is
	// stopped.
	server  *mdsProcess
	indexer *mdsProcess

	// admin is the core API client of the break-glass administrator.
	admin *apigen.ClientWithResponses

	// metadataRepo is R + icebergcatalog.MetadataRepositorySuffix (§3.3).
	metadataRepo string

	// indexerEndpoint is the lakeFS base URL written to metadata_search.lakefs_mds_endpoint.
	// It is the intercepting proxy below, so the restart case can hold one lakeFS request and
	// kill the indexer inside the crash window of §4.4.
	indexerEndpoint string

	// indexerProxy forwards every indexer request to the server and can hold the lakeFS commit
	// of R-metadata after the Iceberg commit has been staged.
	indexerProxy *mdsProxy

	// The maintenance settings of metadata_search (§4.9, §7). newMDSEnv writes the design's
	// defaults; a maintenance case overrides them before startIndexer.
	tagRetention             string
	deleteBranches           bool
	compactionMaxDeleteFiles int
	compactionEveryCommits   int

	// indexerKey and indexerSecret are the credentials of mds-indexer, created by this suite
	// through the auth API after the server's bootstrap created the user (§8).
	indexerKey    string
	indexerSecret string

	// indexerEnv is the environment of the indexer process only, appended to os.Environ(). It is
	// how the Azure case proves G4 of METADATA_SEARCH_AZURE.md: the indexer is configured with
	// LAKEFS_METADATA_SEARCH_AZURE_* variables and the configuration file carries no Azure key at
	// all. nil keeps the indexer's environment identical to the server's.
	indexerEnv []string

	// blockstoreSection and storageSection are the YAML fragments writeConfig writes for the
	// blockstore section and for metadata_search.storage. newMDSEnv fills in the S3 profile; the
	// Azure profile of mds_azure_test.go fills in its own, and writes no storage key at all.
	blockstoreSection string
	storageSection    string

	// setupStorage creates the bucket or container the run's namespaces live in, and
	// cleanupStorage removes the run's objects at teardown with that blockstore's own client.
	setupStorage   func(t *testing.T)
	cleanupStorage func(ctx context.Context) error

	// extraRepositories are further data repositories the indexer is configured to index. The
	// suite's own repository always is; mds_azure_test.go adds one whose storage namespace the
	// indexer must refuse (a container root, METADATA_SEARCH_AZURE D4) and removes it again.
	extraRepositories []string
}

// TestMetadataSearchEndToEnd is the M6 case list of METADATA_SEARCH_DESIGN.md §9: every row
// lifecycle change, every ref kind and every security case, read back through PyIceberg
// (/mds/iceberg/api) and DuckDB (/iceberg/relative_to/{repo}.{ref}/api).
func TestMetadataSearchEndToEnd(t *testing.T) {
	env := newMDSEnv(t)
	env.startIndexer(t)
	cl := env.admin

	// The commits the later cases build on: each one is a state a later ref has to reproduce.
	var (
		firstCommit    string // the two initial objects
		afterOverwrite string // data/one.txt replaced
		afterDelete    string // data/two.txt removed
		afterGateway   string // data/gateway.txt added, with user metadata
	)

	t.Run("upload and commit", func(t *testing.T) {
		env.uploadObject(t, mdsBranch, "data/one.txt", "one")
		env.uploadObject(t, mdsBranch, "data/two.txt", "two two")
		firstCommit = env.commitBranch(t, cl, mdsBranch, "mds test: two objects")
		env.waitIndexed(t, mdsBranch, firstCommit)

		// Every read of the suite goes through all four recipes of docs/metadata-search.md -
		// PyIceberg and DuckDB, each through /mds/iceberg/api and /iceberg/relative_to - and
		// readRows fails when they disagree. The two recipes no earlier run exercised are
		// PyIceberg through /iceberg/relative_to (load_table("system.object_metadata")) and
		// DuckDB's three-level identifier form ("{repo}.{ref}.system"."object_metadata").
		read := env.readRows(t, mdsBranch)
		require.Equal(t, []string{"data/one.txt", "data/two.txt"}, mdsPaths(read.PyIceberg.Rows),
			"PyIceberg through %s must see one row per object of %s", icebergcatalog.MetadataSearchBasePath, mdsBranch)
		require.Equal(t, mdsPaths(read.PyIceberg.Rows), mdsPaths(read.DuckDB.Rows),
			"DuckDB through relative_to must see the same rows as PyIceberg")
		require.Equal(t, firstCommit, read.PyIceberg.SourceCommit,
			"the table property %s is the freshness value of §9 and must be the indexed commit",
			mdsTableSourceCommit)

		row := mdsRowOf(t, read.PyIceberg.Rows, "data/one.txt")
		require.Equal(t, env.repo, row.Repository, "repository is the data repository")
		require.Equal(t, firstCommit, row.CommitID, "commit_id is the commit that added the object")
		require.Equal(t, int64(len("one")), row.SizeBytes)
		require.NotEmpty(t, row.ETag, "etag is the object's checksum")
		require.NotNil(t, row.Committer)
		require.Equal(t, mdsAdminUsername, *row.Committer)
		require.NotNil(t, row.ContentType)
		require.Equal(t, mdsObjectContentType, *row.ContentType)
		require.Empty(t, row.UserMetadata, "an object without user metadata has an empty map, never a null")
		require.Equal(t, row.ETag, mdsRowOf(t, read.DuckDB.Rows, "data/one.txt").ETag,
			"both engines must read the same etag")
	})

	t.Run("overwrite", func(t *testing.T) {
		before := mdsRowOf(t, env.readRows(t, mdsBranch).PyIceberg.Rows, "data/one.txt")
		env.uploadObject(t, mdsBranch, "data/one.txt", "one overwritten")
		afterOverwrite = env.commitBranch(t, cl, mdsBranch, "mds test: overwrite one object")
		env.waitIndexed(t, mdsBranch, afterOverwrite)

		read := env.readRows(t, mdsBranch)
		require.Len(t, read.PyIceberg.Rows, 2, "an overwrite replaces the row, it does not add one")
		row := mdsRowOf(t, read.PyIceberg.Rows, "data/one.txt")
		require.NotEqual(t, before.ETag, row.ETag, "an overwrite must change the row's etag")
		require.Equal(t, afterOverwrite, row.CommitID, "an overwrite must move the row to the new commit")
		require.Equal(t, int64(len("one overwritten")), row.SizeBytes)
		require.Equal(t, afterOverwrite, read.PyIceberg.SourceCommit)
	})

	t.Run("delete", func(t *testing.T) {
		env.deleteObject(t, mdsBranch, "data/two.txt")
		afterDelete = env.commitBranch(t, cl, mdsBranch, "mds test: delete one object")
		env.waitIndexed(t, mdsBranch, afterDelete)

		read := env.readRows(t, mdsBranch)
		require.Equal(t, []string{"data/one.txt"}, mdsPaths(read.PyIceberg.Rows),
			"a removed object's row must be gone (merge-on-read position delete)")
		require.Equal(t, mdsPaths(read.PyIceberg.Rows), mdsPaths(read.DuckDB.Rows),
			"DuckDB must read the position-delete the delta wrote")
	})

	t.Run("s3 gateway user metadata", func(t *testing.T) {
		// The design (§3.1) says user metadata keys are stored as lakeFS stores them, and that an
		// object written through the S3 gateway carries X-Amz-Meta-<lowercased name>. Writing it
		// through the gateway is the only way to assert that end to end.
		env.putObjectThroughGateway(t, mdsBranch, "data/gateway.txt", "gateway",
			map[string]string{"Owner": "esti"})
		afterGateway = env.commitBranch(t, cl, mdsBranch, "mds test: gateway upload")
		env.waitIndexed(t, mdsBranch, afterGateway)

		read := env.readRows(t, mdsBranch)
		row := mdsRowOf(t, read.PyIceberg.Rows, "data/gateway.txt")
		require.Equal(t, "esti", row.UserMetadata["X-Amz-Meta-owner"],
			"the gateway stores X-Amz-Meta-<lowercased name>; row metadata: %v", row.UserMetadata)
		require.Equal(t, row.UserMetadata, mdsRowOf(t, read.DuckDB.Rows, "data/gateway.txt").UserMetadata,
			"DuckDB must read the map column the same way")
	})

	t.Run("filtered scan", func(t *testing.T) {
		// The guide's filtered reads - PyIceberg's row_filter and DuckDB's WHERE ... ORDER BY
		// size_bytes DESC LIMIT - are not the full scan every other case runs. This step runs them
		// through all four engine/mount recipes and requires the same signatures: the predicate is
		// pushed down and the two engines must not answer different tables.
		read := env.readFilteredRows(t, mdsBranch)
		require.Equal(t, []string{"data/gateway.txt", "data/one.txt"}, mdsPaths(read.PyIceberg.Rows),
			"the row_filter GreaterThan(size_bytes, 0) & StartsWith(path, %q) must return exactly the data/ rows",
			mdsFilteredPathPrefix)
		require.NotEmpty(t, read.DuckDB.Rows, "DuckDB's filtered WHERE ... ORDER BY ... LIMIT must return the same rows")
		require.Equal(t, "data/one.txt", read.DuckDB.Rows[0].Path,
			"DuckDB's ORDER BY size_bytes DESC must put the largest row first")
		requireNonIncreasingSizes(t, read.DuckDB.Rows, "DuckDB through "+icebergcatalog.RelativeToBasePath)
		requireNonIncreasingSizes(t, read.DuckDBIdentifier.Rows, "DuckDB through "+icebergcatalog.MetadataSearchBasePath)
	})

	t.Run("new branch is seeded", func(t *testing.T) {
		env.createBranch(t, cl, mdsFeatureBranch, mdsBranch)
		require.Equal(t, afterGateway, env.headCommit(t, cl, mdsFeatureBranch))
		// Let the indexer index the new branch at the commit it was created at, before the first
		// commit of its own. That is the read of §4.5 step 1 this case is about - the branch
		// exists at A, so its table must come from tag mds-<A> - and it makes the case
		// deterministic: without the wait the indexer may first see the branch already at
		// featureCommit, which is a valid seed too (the log walk of the new head finds mds-<A>),
		// but exercises a different branch of the rule.
		env.waitIndexed(t, mdsFeatureBranch, afterGateway)
		env.uploadObject(t, mdsFeatureBranch, "data/feature.txt", "feature")
		featureCommit := env.commitBranch(t, cl, mdsFeatureBranch, "mds test: object on the feature branch")
		env.waitIndexed(t, mdsFeatureBranch, featureCommit)

		read := env.readRows(t, mdsFeatureBranch)
		require.Equal(t, []string{"data/feature.txt", "data/gateway.txt", "data/one.txt"},
			mdsPaths(read.PyIceberg.Rows),
			"a new branch is seeded from the indexed state and then follows its own commits (§4.5)")
		seeded := mdsRowOf(t, read.PyIceberg.Rows, "data/gateway.txt")
		require.Equal(t, afterGateway, seeded.CommitID,
			"the seeded rows carry the commit the branch was created at (§3.2 backfill rows)")

		// The rows alone do not prove the seeding of §4.5 step 1: this run configures
		// backfill: head, so a branch rebuilt from a full listing at its head writes exactly the
		// same rows with exactly the same commit_id. R-metadata's own history is what tells the
		// two apart - a seed descends from the mds-<A> tag of an indexed ancestor, a backfill of
		// the new head starts again at the initialisation commit.
		mainLog := env.metadataLog(t, mdsBranch)
		seededAt := env.metadataCommitTagged(t, afterGateway)
		require.Equal(t, mainLog[0].Id, seededAt,
			"tag %s%s must name main's commit that recorded %s - the indexed ancestor a seed is "+
				"taken from (§4.5 step 1); main's history: %v", indexer.TagPrefix, afterGateway, afterGateway,
			mdsCommitLog(mainLog))
		featureLog := env.metadataLog(t, mdsFeatureBranch)
		require.GreaterOrEqualf(t, len(featureLog), 2,
			"R-metadata@%s must have a commit for %s on top of the commit it was seeded from; history: %v",
			mdsFeatureBranch, featureCommit, mdsCommitLog(featureLog))
		require.Equal(t, featureCommit, mdsSourceCommitOf(featureLog[0]),
			"the new branch's table must be at %s, the commit made on it; history: %v",
			featureCommit, mdsCommitLog(featureLog))
		require.Equal(t, seededAt, featureLog[1].Id,
			"R-metadata@%s must have been created from the commit tagged %s%s (the seed of §4.5 step 1): "+
				"the one commit above the position is %s. A backfill of the new head would have rebuilt "+
				"the table at %s from the initialisation commit instead; history: %v",
			mdsFeatureBranch, indexer.TagPrefix, afterGateway, featureCommit, featureCommit, mdsCommitLog(featureLog))
		require.Equal(t, mdsCommitIDs(mainLog), mdsCommitIDs(featureLog)[1:],
			"below its own commit, R-metadata@%s must be main's history at %s - not a rebuild: the "+
				"branch's first commits stay the automatic root and the %q commit of §4.8 (§4.5 step 1)",
			mdsFeatureBranch, afterGateway, indexer.InitCommitMessage)
		require.Equal(t, indexer.InitCommitMessage, featureLog[len(featureLog)-2].Message,
			"the branch starts at R-metadata's initialisation commit and shares main's history; "+
				"history: %v", mdsCommitLog(featureLog))

		mainRead := env.readRows(t, mdsBranch)
		require.NotContains(t, mdsPaths(mainRead.PyIceberg.Rows), "data/feature.txt",
			"a commit on the feature branch must not appear on main")
	})

	t.Run("merge", func(t *testing.T) {
		beforeMerge := env.headCommit(t, cl, mdsBranch)
		env.requireMergeSucceeds(t, cl, mdsFeatureBranch, mdsBranch)
		mergeCommit := env.headCommit(t, cl, mdsBranch)
		require.NotEqual(t, beforeMerge, mergeCommit, "the merge must create a commit on main")
		env.waitIndexed(t, mdsBranch, mergeCommit)

		read := env.readRows(t, mdsBranch)
		require.Contains(t, mdsPaths(read.PyIceberg.Rows), "data/feature.txt",
			"the merged object must be indexed on main")
		row := mdsRowOf(t, read.PyIceberg.Rows, "data/feature.txt")
		require.Equal(t, mergeCommit, row.CommitID,
			"an object a merge brings in gets the merge commit's id (§3.2)")
		require.Equal(t, mergeCommit, read.PyIceberg.SourceCommit)
	})

	var afterTag string

	t.Run("tag and short commit id", func(t *testing.T) {
		// The tag captures the merged state; a later commit on main must not be visible at it.
		env.createTag(t, cl, mdsTag, env.headCommit(t, cl, mdsBranch))
		env.uploadObject(t, mdsBranch, "data/after-tag.txt", "after the tag")
		afterTag = env.commitBranch(t, cl, mdsBranch, "mds test: commit after the tag")
		env.waitIndexed(t, mdsBranch, afterTag)

		// A tag is not a branch: §6 maps it through the mds-<commit> tag of R-metadata.
		tagRead := env.readRows(t, mdsTag)
		require.Contains(t, mdsPaths(tagRead.PyIceberg.Rows), "data/feature.txt")
		require.NotContains(t, mdsPaths(tagRead.PyIceberg.Rows), "data/after-tag.txt",
			"a tag is immutable: the commit after it must not appear at the tag")
		require.Equal(t, mdsPaths(tagRead.PyIceberg.Rows), mdsPaths(tagRead.DuckDB.Rows),
			"DuckDB must read the virtual table at a tag through relative_to as well")

		// A short commit id resolves through the catalog's own prefix resolution to the full id,
		// and then through the index's mds-<commit> tag (§6).
		short := afterTag[:8]
		shortRead := env.readRows(t, short)
		require.Contains(t, mdsPaths(shortRead.PyIceberg.Rows), "data/after-tag.txt")
		require.Equal(t, afterTag, shortRead.PyIceberg.SourceCommit,
			"a short commit id must serve the snapshot of that commit")
	})

	t.Run("hard reset", func(t *testing.T) {
		// Rewind main to the tagged commit: the indexer's position (afterTag) is not an ancestor of
		// the new head, which is the history rewrite of §4.7.
		res, err := cl.HardResetBranchWithResponse(t.Context(), env.repo, mdsBranch,
			&apigen.HardResetBranchParams{Ref: mdsTag})
		require.NoError(t, err, "hard reset %s to %s", mdsBranch, mdsTag)
		require.Equalf(t, http.StatusNoContent, res.StatusCode(), "hard reset: %s", res.Body)
		env.waitIndexed(t, mdsBranch, env.headCommit(t, cl, mdsBranch))

		read := env.readRows(t, mdsBranch)
		require.NotContains(t, mdsPaths(read.PyIceberg.Rows), "data/after-tag.txt",
			"a hard reset must rewrite the branch's table to the ancestor's rows (§4.7)")
		require.Contains(t, mdsPaths(read.PyIceberg.Rows), "data/feature.txt")
		require.Equal(t, mdsPaths(read.PyIceberg.Rows), mdsPaths(read.DuckDB.Rows))
	})

	t.Run("indexer restart", func(t *testing.T) {
		// §4.4 row 1: a kill after the Iceberg commit (step 3) and before the lakeFS commit
		// (step 4) must be recovered by the next process. The test does not hope to catch the gap
		// between two ticks: it holds the lakeFS commit request at the indexer's proxy, which is
		// the observable moment after the staged pointer exists and before it is committed, and
		// fails when that request does not arrive within a bounded wait.
		env.uploadObject(t, mdsBranch, "data/before-restart.txt", "before")
		beforeRestart := env.commitBranch(t, cl, mdsBranch, "mds test: commit before the restart")
		env.waitIndexed(t, mdsBranch, beforeRestart)
		seededAt := env.metadataCommitTagged(t, beforeRestart)

		gate := env.indexerProxy.holdNextMetadataCommit(t)
		env.uploadObject(t, mdsBranch, "data/during-window.txt", "window")
		windowCommit := env.commitBranch(t, cl, mdsBranch, "mds test: commit in the crash window")
		env.waitForMetadataCommitWindow(t, gate, windowCommit)
		env.stopIndexer(t)
		gate.letGo()

		// The kill landed after step 3 and before step 4: the staged pointer is on the branch,
		// and the branch head still records the previous commit because the held commit was never
		// forwarded.
		require.NotEmptyf(t, env.metadataStaging(t, mdsBranch),
			"the kill must land after the Iceberg commit staged %s on %s@%s",
			windowCommit, env.metadataRepo, mdsBranch)
		afterKill := env.metadataLog(t, mdsBranch)
		require.Equalf(t, seededAt, afterKill[0].Id,
			"the held lakeFS commit of %s must not exist after the kill; history: %v",
			windowCommit, mdsCommitLog(afterKill))

		env.uploadObject(t, mdsBranch, "data/after-restart.txt", "after")
		restartCommit := env.commitBranch(t, cl, mdsBranch, "mds test: commit while the indexer is down")
		env.startIndexer(t)
		env.waitIndexed(t, mdsBranch, restartCommit)
		env.waitMetadataStagingEmpty(t, mdsBranch)

		read := env.readRows(t, mdsBranch)
		require.Contains(t, mdsPaths(read.PyIceberg.Rows), "data/before-restart.txt")
		require.Contains(t, mdsPaths(read.PyIceberg.Rows), "data/during-window.txt",
			"the commit the killed process had already staged must be completed, not lost")
		require.Contains(t, mdsPaths(read.PyIceberg.Rows), "data/after-restart.txt",
			"a restarted indexer must index the commits it missed")
		require.Equal(t, restartCommit, read.PyIceberg.SourceCommit)

		// Recovery must advance the position by exactly the two commits the process missed: one
		// indexing commit for the staged windowCommit and one for restartCommit, with no replay
		// of windowCommit and no leftover staging.
		log := env.metadataLog(t, mdsBranch)
		var aboveSeed []apigen.Commit
		seenSeed := false
		for _, commit := range log {
			if commit.Id == seededAt {
				seenSeed = true
				break
			}
			aboveSeed = append(aboveSeed, commit)
		}
		require.Truef(t, seenSeed, "the log of %s@%s must still contain %s; history: %v",
			env.metadataRepo, mdsBranch, seededAt, mdsCommitLog(log))
		indexCommits := make([]string, 0, 2)
		for _, commit := range aboveSeed {
			if strings.HasPrefix(commit.Message, "mds: index ") {
				indexCommits = append(indexCommits, commit.Message)
			}
		}
		require.ElementsMatchf(t, []string{
			fmt.Sprintf(indexer.IndexCommitMessage, env.repo, windowCommit),
			fmt.Sprintf(indexer.IndexCommitMessage, env.repo, restartCommit),
		}, indexCommits,
			"recovery must write exactly one indexing commit per missed commit and no replay; history: %v",
			mdsCommitLog(log))
	})

	t.Run("user without access is refused", func(t *testing.T) {
		outsider := env.createPrincipal(t, "esti-mds-outsider", nil)
		status, body := env.loadTableAs(t, outsider.key, outsider.secret, mdsBranch)
		require.Equalf(t, http.StatusForbidden, status,
			"%s, who has no grant at all, must get 403 on the virtual table of %s (§9); body: %s",
			outsider.username, env.repo, body)
	})

	t.Run("R-metadata access does not open the virtual table", func(t *testing.T) {
		// §9: a user with access to R-metadata only gets 403 through R.<ref>.system.object_metadata.
		// The direct read below proves the user really does hold a grant on R-metadata, so the 403
		// is the virtual table's rule and not a missing permission.
		metadataOnly := env.createPrincipal(t, "esti-mds-metadata-only", []apigen.Statement{
			{Effect: "allow", Action: []string{"fs:*"}, Resource: mdsRepoPattern(env.metadataRepo)},
			{Effect: "allow", Action: []string{"fs:*"}, Resource: mdsRepoPattern(env.metadataRepo) + "/*"},
			{Effect: "allow", Action: []string{"catalog:*"}, Resource: "arn:lakefs:catalog:::*/*-metadata/*"},
		})

		status, body := env.loadTableAs(t, metadataOnly.key, metadataOnly.secret, mdsBranch)
		require.Equalf(t, http.StatusForbidden, status,
			"%s, who has access to %s only, must get 403 through the virtual table (§5.2, §9); body: %s",
			metadataOnly.username, env.metadataRepo, body)

		status, body = env.loadMetadataTableAs(t, metadataOnly.key, metadataOnly.secret, mdsBranch)
		require.Equalf(t, http.StatusOK, status,
			"the same user must be able to read %s's table directly, so the 403 above is not a "+
				"missing grant; body: %s", env.metadataRepo, body)
	})

	t.Run("non-indexer cannot write R-metadata", func(t *testing.T) {
		// §8: mds-metadata-repo-protect denies the fs: and catalog: writes on the companion
		// repositories of every base group, and a deny wins over the administrator's fs:* allow.
		//
		// The suite opts into auth.authorization_failure_status: 403, so the core API reports the
		// refusal with the 403 plan M6 names; the S3 gateway and the Iceberg catalog answer 403
		// anyway. Both APIs are asserted, so neither convention can hide a missing deny.
		res, err := cl.UploadObjectWithBodyWithResponse(t.Context(), env.metadataRepo, mdsBranch,
			&apigen.UploadObjectParams{Path: "forged/row.txt"}, mdsObjectContentType, strings.NewReader("forged"))
		require.NoError(t, err, "upload into %s", env.metadataRepo)
		require.Equalf(t, http.StatusForbidden, res.StatusCode(),
			"an administrator must not be able to write %s through the API while the deny of §8 is "+
				"in force: %s", env.metadataRepo, res.Body)
		require.Containsf(t, string(res.Body), "denied permission",
			"the refusal must come from the deny policy, not from a missing grant: %s", res.Body)

		_, err = env.s3ClientFor(t, env.key, env.secret).PutObject(t.Context(), env.metadataRepo, mdsBranch+"/forged/row.txt",
			strings.NewReader("forged"), int64(len("forged")), minio.PutObjectOptions{})
		require.Errorf(t, err, "an administrator must not be able to write %s through the S3 gateway either",
			env.metadataRepo)
		response := minio.ToErrorResponse(err)
		require.Equalf(t, "AccessDenied", response.Code,
			"the S3 gateway must refuse the write with 403 AccessDenied, the same deny on the other "+
				"API: %v", err)
	})

	t.Run("guard group cannot create a repository on the companion storage namespace", func(t *testing.T) {
		// §8 (R3S-3, MDS_REVIEW_3.md): the deny carries fs:AttachStorageNamespace on the storage
		// namespace of every companion repository, and CreateRepository authorizes that action on
		// the raw string the client sent (pkg/api/controller.go) - not on a canonical form - while
		// the block layer resolves one trailing "/" away (pkg/block.resolveRelative,
		// formatPathWithNamespace). A deny that named only the stored spelling would therefore leave
		// the same physical prefix to a guard-group member with fs:*, who could create a bare
		// repository on it and let its uncommitted GC sweep the index files (§3.3).
		//
		// bootstrap.storageNamespaceDenyResources emits one ARN per accepted spelling for exactly
		// that reason. This case drives the real CreateRepository with each spelling, bare and not
		// bare - the bare flag is not read before the authorization node, but it decides what the
		// request would write, so both are sent - and requires the 403.
		//
		// The companion repository itself is what carries the namespace, and the indexer creates it
		// on its first tick of R (§3.3), so the case waits for it instead of assuming an earlier
		// case indexed something: run alone, nothing has been indexed yet.
		env.waitMetadataRepository(t)
		meta, err := env.admin.GetRepositoryWithResponse(t.Context(), env.metadataRepo)
		require.NoErrorf(t, err, "read %s", env.metadataRepo)
		require.Equalf(t, http.StatusOK, meta.StatusCode(), "read %s: %s", env.metadataRepo, meta.Body)
		require.NotNilf(t, meta.JSON200, "read %s: %s", env.metadataRepo, meta.Body)
		stored := strings.TrimRight(meta.JSON200.StorageNamespace, "/")
		require.NotEmptyf(t, stored, "%s must report its storage namespace: %s", env.metadataRepo, meta.Body)

		// For a namespace with a path - the indexer derives R-metadata's from R's, so it always has
		// one - this is the whole enumeration of storageNamespaceDenyResources: the stored spelling
		// and that spelling with one "/". A bucket-root companion has one more ("…//"), and is not
		// what this installation creates.
		spellings := []string{stored, stored + "/"}
		resources := make(map[string]struct{}, len(spellings))
		for _, spelling := range spellings {
			resources[permissions.StorageNamespace(spelling)] = struct{}{}
		}

		// The deny is re-derived by the bootstrap's refresher, which runs every one minute, so a
		// companion repository the indexer created after the server started is named only after the
		// next refresh. Waiting for it is part of §8's contract, not a workaround: the window is at
		// most one interval, and the wait fails the case (with the policy set it saw) if it is not
		// closed within mdsPolicyRefreshTimeout.
		groupID := env.groupID(t, mdsGuardGroup)
		env.waitCompanionNamespaceDeny(t, groupID, resources)

		// The principal is created after the deny is in force, so its first authorization answer is
		// the current policy set: the auth service caches a user's effective policies, and a cache
		// filled before the refresh would hide the deny for its TTL.
		member := env.createPrincipal(t, "esti-mds-guard-member", nil)
		added, err := env.admin.AddGroupMembershipWithResponse(t.Context(), groupID, member.username)
		require.NoErrorf(t, err, "add %s to %s", member.username, mdsGuardGroup)
		require.Equalf(t, http.StatusCreated, added.StatusCode(), "add %s to %s: %s",
			member.username, mdsGuardGroup, added.Body)
		memberClient := env.coreAPIAs(t, member.key, member.secret)

		// The control: the same principal, in the same group, creates and deletes an ordinary
		// repository. Its fs:* allow is what the deny overrides below, so a failure here means the
		// grant is broken and the 403s are not evidence of anything.
		controlNS := "s3://" + env.bucket + "/" + env.runPrefix + "/guard-control"
		for _, bare := range []bool{true, false} {
			name := env.repo + "-guard-control"
			if bare {
				name += "-bare"
			}
			created, err := memberClient.CreateRepositoryWithResponse(t.Context(),
				&apigen.CreateRepositoryParams{Bare: apiutil.Ptr(bare)},
				apigen.CreateRepositoryJSONRequestBody{
					Name:             name,
					StorageNamespace: controlNS,
					DefaultBranch:    apiutil.Ptr(mdsBranch),
				})
			require.NoErrorf(t, err, "create the control repository %s", name)
			require.Equalf(t, http.StatusCreated, created.StatusCode(),
				"%s is a member of %s and holds fs:*, so creating %s (bare=%t) on the ordinary namespace %q "+
					"must succeed, or the refusals below are not the deny of §8: %s",
				member.username, mdsGuardGroup, name, bare, controlNS, created.Body)

			deleted, err := memberClient.DeleteRepositoryWithResponse(t.Context(), name, &apigen.DeleteRepositoryParams{})
			require.NoErrorf(t, err, "delete the control repository %s", name)
			require.Equalf(t, http.StatusNoContent, deleted.StatusCode(),
				"delete the control repository %s: %s", name, deleted.Body)
		}

		for i, spelling := range spellings {
			// Bare first: it is the shape the pre-fix deny let through, because a bare
			// CreateRepository skips the storage-namespace-in-use check that refuses a non-bare one
			// on the same prefix (R3S-3).
			for _, bare := range []bool{true, false} {
				name := fmt.Sprintf("%s-guard-%d-%t", env.repo, i, bare)
				res, err := memberClient.CreateRepositoryWithResponse(t.Context(),
					&apigen.CreateRepositoryParams{Bare: apiutil.Ptr(bare)},
					apigen.CreateRepositoryJSONRequestBody{
						Name:             name,
						StorageNamespace: spelling,
						DefaultBranch:    apiutil.Ptr(mdsBranch),
					})
				require.NoErrorf(t, err, "create %s on the companion storage namespace spelled %q", name, spelling)
				require.Equalf(t, http.StatusForbidden, res.StatusCode(),
					"%s is a member of %s and holds fs:*, so only the deny of §8 can refuse this: a repository "+
						"(bare=%t) on the storage namespace of %s spelled %q must be 403 (R3S-3); it would share "+
						"the companion's physical prefix and let its uncommitted GC sweep the index files: %s",
					member.username, mdsGuardGroup, bare, env.metadataRepo, spelling, res.Body)
				require.Containsf(t, string(res.Body), "denied permission",
					"the refusal of the spelling %q must come from the deny policy, not from a missing grant: %s",
					spelling, res.Body)
				require.Containsf(t, string(res.Body), permissions.AttachStorageNamespaceAction,
					"the refusal of the spelling %q must name %s, the action the deny carries: %s",
					spelling, permissions.AttachStorageNamespaceAction, res.Body)

				// Nothing is created: the authorization node precedes every write.
				found, err := memberClient.GetRepositoryWithResponse(t.Context(), name)
				require.NoErrorf(t, err, "read %s after the refused create", name)
				require.Equalf(t, http.StatusNotFound, found.StatusCode(),
					"a refused CreateRepository must not create %s: %s", name, found.Body)
			}
		}
	})

	t.Run("system on R rejects writes", func(t *testing.T) {
		// The last row of the security tests of §9, with §5.3: on a data repository the system
		// namespace belongs to the virtual table, so no client may create, commit, rename or drop
		// anything in it, nor create or drop it as a namespace. The rule must hold on both
		// addressing forms - /mds/iceberg/api, where the client names the repository and the ref
		// in the identifier, and /iceberg/relative_to/{repo}.{ref}/api, where its URL fixes them
		// (and where DuckDB and Starburst send it).
		//
		// The principal is deliberately not the administrator: it holds a write grant on R and on
		// R only (fs:* on the repository, catalog:* on its namespace and table ARNs), which is
		// what a query user who also writes tables has. Every refusal below is therefore the
		// reservation of §5.3 and not a missing permission - the positive control at the end of
		// each form creates and drops an ordinary namespace and table with the same credentials,
		// so a 403 caused by a broken grant cannot pass for the rule.
		writer := env.createPrincipal(t, "esti-mds-writer", []apigen.Statement{
			{Effect: "allow", Action: []string{"fs:*"}, Resource: mdsRepoPattern(env.repo)},
			{Effect: "allow", Action: []string{"fs:*"}, Resource: mdsRepoPattern(env.repo) + "/*"},
			{Effect: "allow", Action: []string{"catalog:*"}, Resource: "arn:lakefs:catalog:::namespace/" + env.repo + "/*"},
			{Effect: "allow", Action: []string{"catalog:*"}, Resource: "arn:lakefs:catalog:::table/" + env.repo + "/*"},
		})

		// The table every refusal must leave alone, read before and after the writes.
		before := env.readRows(t, mdsBranch)
		status, body := env.loadTableAs(t, writer.key, writer.secret, mdsBranch)
		require.Equalf(t, http.StatusOK, status,
			"%s must be able to read the virtual table before the refused writes, or the 403s below "+
				"could be its read grant instead: %s", writer.username, body)

		const (
			createTableBody = `{"name":"clients_table","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}}`
			commitBody      = `{"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"mallory"}}]}`
			controlNS       = "inventory"
			controlTable    = "clients_table"
		)

		for _, mount := range env.systemMounts(mdsBranch) {
			t.Run(mount.name, func(t *testing.T) {
				systemNS := mount.namespace(icebergcatalog.SystemNamespace)
				tablesURL := mount.url("/namespaces/" + systemNS + "/tables")
				systemTable := tablesURL + "/" + url.PathEscape(icebergcatalog.ObjectMetadataTable)

				t.Run("create table", func(t *testing.T) {
					status, body := env.catalogAs(t, writer, http.MethodPost, tablesURL, createTableBody)
					requireMDSReservedNamespace(t, status, body,
						"creating a table in "+icebergcatalog.SystemNamespace+" of "+env.repo)
				})

				t.Run("commit", func(t *testing.T) {
					status, body := env.catalogAs(t, writer, http.MethodPost, systemTable, commitBody)
					requireMDSReservedNamespace(t, status, body,
						"committing to "+icebergcatalog.SystemNamespace+"."+icebergcatalog.ObjectMetadataTable+" of "+env.repo)
				})

				t.Run("rename", func(t *testing.T) {
					// §5.3: the catalog serves no rename operation at all (it is a v1 non-goal and
					// http.go refuses to advertise one), so there is nothing to block and the answer
					// is the unknown-endpoint 404 - the same on both forms. What matters here is that a
					// rename cannot move the virtual table's pointer; the read at the end of the case
					// proves it did not.
					renameBody, err := json.Marshal(map[string]any{
						"source": map[string]any{"namespace": mount.levels(icebergcatalog.SystemNamespace), "name": icebergcatalog.ObjectMetadataTable},
						"dest":   map[string]any{"namespace": mount.levels(icebergcatalog.SystemNamespace), "name": "object_metadata_renamed"},
					})
					require.NoError(t, err, "marshal the rename request")
					status, body := env.catalogAs(t, writer, http.MethodPost, mount.url("/tables/rename"), string(renameBody))
					requireMDSRefusal(t, status, body, http.StatusNotFound, icebergcatalog.ErrTypeNotFound,
						"renaming "+icebergcatalog.SystemNamespace+"."+icebergcatalog.ObjectMetadataTable+" of "+env.repo)
				})

				t.Run("drop table", func(t *testing.T) {
					status, body := env.catalogAs(t, writer, http.MethodDelete, systemTable, "")
					requireMDSReservedNamespace(t, status, body,
						"dropping "+icebergcatalog.SystemNamespace+"."+icebergcatalog.ObjectMetadataTable+" of "+env.repo)
				})

				t.Run("create namespace", func(t *testing.T) {
					namespaceBody, err := json.Marshal(map[string]any{"namespace": mount.levels(icebergcatalog.SystemNamespace)})
					require.NoError(t, err, "marshal the create namespace request")
					status, body := env.catalogAs(t, writer, http.MethodPost, mount.url("/namespaces"), string(namespaceBody))
					requireMDSReservedNamespace(t, status, body,
						"creating namespace "+icebergcatalog.SystemNamespace+" on "+env.repo)
				})

				t.Run("drop namespace", func(t *testing.T) {
					status, body := env.catalogAs(t, writer, http.MethodDelete, mount.url("/namespaces/"+systemNS), "")
					requireMDSReservedNamespace(t, status, body,
						"dropping namespace "+icebergcatalog.SystemNamespace+" on "+env.repo)
				})

				t.Run("an ordinary namespace is writable", func(t *testing.T) {
					// The control for the refusals above: on the same mount and with the same
					// credentials, an ordinary namespace and its table are created and dropped. If
					// the grant was wrong, this fails and the 403s above are not evidence of
					// anything; if it passes, they are the reservation of §5.3.
					namespaceBody, err := json.Marshal(map[string]any{"namespace": mount.levels(controlNS)})
					require.NoError(t, err, "marshal the control create namespace request")
					status, body := env.catalogAs(t, writer, http.MethodPost, mount.url("/namespaces"), string(namespaceBody))
					require.Equalf(t, http.StatusOK, status,
						"%s may write %s, so creating namespace %s must succeed: %s", writer.username, env.repo, controlNS, body)

					controlTableURL := mount.url("/namespaces/" + mount.namespace(controlNS) + "/tables/" + url.PathEscape(controlTable))
					status, body = env.catalogAs(t, writer, http.MethodPost,
						mount.url("/namespaces/"+mount.namespace(controlNS)+"/tables"), createTableBody)
					require.Equalf(t, http.StatusOK, status,
						"%s may write %s, so creating table %s.%s must succeed: %s", writer.username, env.repo, controlNS, controlTable, body)

					status, body = env.catalogAs(t, writer, http.MethodDelete, controlTableURL, "")
					require.Equalf(t, http.StatusNoContent, status,
						"the control table %s.%s must drop: %s", controlNS, controlTable, body)
					status, body = env.catalogAs(t, writer, http.MethodDelete, mount.url("/namespaces/"+mount.namespace(controlNS)), "")
					require.Equalf(t, http.StatusNoContent, status,
						"the control namespace %s must drop: %s", controlNS, body)
				})
			})
		}

		// Nothing above may have reached the table: it still reads, through both published engines,
		// and it still reports the commit the indexer recorded before the writes.
		after := env.readRows(t, mdsBranch)
		require.Equal(t, mdsPaths(before.PyIceberg.Rows), mdsPaths(after.PyIceberg.Rows),
			"a refused write must not change the rows of the virtual table")
		require.Equal(t, before.PyIceberg.SourceCommit, after.PyIceberg.SourceCommit,
			"a refused write must not move %s", mdsTableSourceCommit)
		require.Equal(t, mdsPaths(after.PyIceberg.Rows), mdsPaths(after.DuckDB.Rows),
			"DuckDB must still read the virtual table through relative_to after the refusals")
		status, body = env.loadTableAs(t, writer.key, writer.secret, mdsBranch)
		require.Equalf(t, http.StatusOK, status,
			"the virtual table must still load for %s after the refused writes: %s", writer.username, body)
	})

	t.Run("indexer still owns R-metadata", func(t *testing.T) {
		// The deny must not disarm the indexer: a new commit of R is still indexed after it.
		env.uploadObject(t, mdsBranch, "data/final.txt", "final")
		final := env.commitBranch(t, cl, mdsBranch, "mds test: commit after the security cases")
		env.waitIndexed(t, mdsBranch, final)
		require.Contains(t, mdsPaths(env.readRows(t, mdsBranch).PyIceberg.Rows), "data/final.txt")
	})
}

// TestMetadataSearchBackfill times the first indexing of a 100k-object branch: the full listing and
// rebuild of METADATA_SEARCH_DESIGN.md §4.5 step 2 and §4.3 step 2.
//
// It is a measurement, not a gate (plan M6). The assertions are about the outcome - the row count
// both engines read - and the duration is reported in the test log and in the commit message.
func TestMetadataSearchBackfill(t *testing.T) {
	count := mdsBackfillObjectCount()
	env := newMDSEnv(t)

	// Build the source commit before the clock starts: the measurement is the indexer's first
	// indexing of the branch, not the test's ability to create 100k entries. Both setups below
	// produce exactly that shape (one commit of R with `count` objects); they differ only in how
	// many bytes the object store has to write.
	setupStart := time.Now()
	head := env.buildBackfillCommit(t, count)
	setup := time.Since(setupStart)

	t.Logf("MDS BACKFILL SETUP: %d objects committed to %s@%s in %s; the indexing clock below starts when the indexer starts",
		count, env.repo, head, setup.Round(time.Millisecond))

	start := time.Now()
	env.startIndexer(t)
	env.waitIndexedWithin(t, mdsBranch, head, mdsBackfillIndexTimeout)
	elapsed := time.Since(start)
	rate := float64(count) / elapsed.Seconds()
	// The wall clock from process start is dominated by one period: the indexer ticks every
	// metadata_search.period. The counters and the tick histogram of §9 are what say how long the
	// work itself took, so they are reported (and read from /metrics) beside it.
	t.Logf("MDS BACKFILL: %d objects first-indexed %s after the indexer started (%.0f objects/s, period %s); "+
		"rows_written_total=%s rebuilds_total=%s tick_duration_seconds_count=%s tick_duration_seconds_sum=%s",
		count, elapsed.Round(time.Millisecond), rate, mdsIndexerPeriod,
		env.indexerMetric(t, "lakefs_mds_rows_written_total"),
		env.indexerMetric(t, "lakefs_mds_rebuilds_total"),
		env.indexerMetric(t, "lakefs_mds_tick_duration_seconds_count"),
		env.indexerMetric(t, "lakefs_mds_tick_duration_seconds_sum"))

	read := env.countRows(t, mdsBranch)
	require.Equal(t, count, read.PyIceberg.Count,
		"PyIceberg must read one row per imported object")
	require.Equal(t, count, read.DuckDB.Count,
		"DuckDB must read one row per imported object")
	require.Equal(t, head, read.PyIceberg.SourceCommit,
		"the backfilled table must record the imported commit as its position")
}

// TestMetadataSearchCompactionAndTagRetention runs §4.9 on a real catalog: a low max_delete_files
// makes two deletes trigger the compaction, the compaction commit is visible in R-metadata's
// history, the mds-* tag stays on the pre-compaction commit, the rows and the position do not
// change, lakefs_mds_compactions_total reports the rewrite, and a short tag_retention retires the
// old tag while keeping the tag of the live position.
func TestMetadataSearchCompactionAndTagRetention(t *testing.T) {
	env := newMDSEnv(t)
	env.compactionMaxDeleteFiles = 1
	env.compactionEveryCommits = 1000
	env.tagRetention = "1s"
	env.writeConfig(t, env.indexerKey, env.indexerSecret)
	env.startIndexer(t)
	cl := env.admin

	for path, content := range map[string]string{
		"data/keep-one.txt": "keep one",
		"data/keep-two.txt": "keep two",
		"data/gone-one.txt": "gone one",
		"data/gone-two.txt": "gone two",
	} {
		env.uploadObject(t, mdsBranch, path, content)
	}
	first := env.commitBranch(t, cl, mdsBranch, "mds test: four rows before compaction")
	env.waitIndexed(t, mdsBranch, first)
	require.Len(t, env.readRows(t, mdsBranch).PyIceberg.Rows, 4, "the four objects must be indexed")

	// Age the first source commit beyond tag_retention: retention measures the source commit's
	// creation date, not the tag's.
	time.Sleep(1500 * time.Millisecond)

	// Two separate deletes are two merge-on-read transactions, so the table carries two delete
	// files; max_delete_files=1 makes the second one cross the threshold.
	env.deleteObject(t, mdsBranch, "data/gone-one.txt")
	firstDelete := env.commitBranch(t, cl, mdsBranch, "mds test: first delete")
	env.waitIndexed(t, mdsBranch, firstDelete)
	env.deleteObject(t, mdsBranch, "data/gone-two.txt")
	secondDelete := env.commitBranch(t, cl, mdsBranch, "mds test: second delete")
	env.waitIndexed(t, mdsBranch, secondDelete)

	log := env.metadataLog(t, mdsBranch)
	require.NotEmpty(t, log)
	require.Equalf(t, fmt.Sprintf(indexer.CompactCommitMessage, env.repo, secondDelete), log[0].Message,
		"the tick that crossed max_delete_files must end with the %q commit of §4.9; history: %v",
		indexer.CompactCommitMessage, mdsCommitLog(log))
	require.Equal(t, secondDelete, mdsSourceCommitOf(log[0]),
		"the compaction commit keeps the same source commit")

	tagCommit := env.metadataCommitTagged(t, secondDelete)
	require.NotEqualf(t, log[0].Id, tagCommit,
		"the compaction must not move %s%s: the tag stays on the pre-compaction commit; history: %v",
		indexer.TagPrefix, secondDelete, mdsCommitLog(log))
	require.Equalf(t, log[1].Id, tagCommit,
		"%s%s must stay on the indexing commit the compaction rewrote, not on an older one; history: %v",
		indexer.TagPrefix, secondDelete, mdsCommitLog(log))
	require.Equal(t, secondDelete, mdsSourceCommitOf(log[1]),
		"the commit under the compaction commit must be the index commit it compacted; history: %v",
		mdsCommitLog(log))

	read := env.readRows(t, mdsBranch)
	require.Equal(t, []string{"data/keep-one.txt", "data/keep-two.txt"}, mdsPaths(read.PyIceberg.Rows),
		"compaction must keep the rows that were not deleted and must not duplicate the survivors")
	require.Equal(t, secondDelete, read.PyIceberg.SourceCommit,
		"compaction must not move the freshness property")

	line := env.indexerMetric(t, "lakefs_mds_compactions_total")
	require.NotEqualf(t, "unavailable", line, "the indexer must expose %s", "lakefs_mds_compactions_total")
	fields := strings.Fields(line)
	require.Lenf(t, fields, 2, "unexpected %s line: %q", "lakefs_mds_compactions_total", line)
	require.Equalf(t, "1", fields[1], "one compaction must be counted: %q", line)

	// Retention is measured on the source commit, so the old tag is retired once it is older than
	// tag_retention. The tag that records the branch's current position stays: §4.4 would recreate
	// it on the next tick, and retention must not fight tag repair.
	env.waitTagGone(t, indexer.TagPrefix+first)
	res, err := env.admin.GetTagWithResponse(t.Context(), env.metadataRepo, indexer.TagPrefix+secondDelete)
	require.NoErrorf(t, err, "get tag %s%s", indexer.TagPrefix, secondDelete)
	require.Equalf(t, http.StatusOK, res.StatusCode(),
		"tag_retention must keep the tag of the live position: %s", res.Body)
}

// TestMetadataSearchBranchDeletion runs §4.8's mirrored deletion on a real catalog: deleting a data
// branch removes its R-metadata branch, but the mds-* tags stay, so the branch's history is still
// readable by commit and by tag.
func TestMetadataSearchBranchDeletion(t *testing.T) {
	env := newMDSEnv(t)
	env.startIndexer(t)
	cl := env.admin

	env.uploadObject(t, mdsBranch, "data/base.txt", "base")
	base := env.commitBranch(t, cl, mdsBranch, "mds test: base for the deleted branch")
	env.waitIndexed(t, mdsBranch, base)

	env.createBranch(t, cl, mdsFeatureBranch, mdsBranch)
	env.waitIndexed(t, mdsFeatureBranch, base)
	env.uploadObject(t, mdsFeatureBranch, "data/feature-only.txt", "feature only")
	featureCommit := env.commitBranch(t, cl, mdsFeatureBranch, "mds test: commit on the branch to delete")
	env.waitIndexed(t, mdsFeatureBranch, featureCommit)
	tagged := env.metadataCommitTagged(t, featureCommit)

	force := true
	res, err := cl.DeleteBranchWithResponse(t.Context(), env.repo, mdsFeatureBranch,
		&apigen.DeleteBranchParams{Force: &force})
	require.NoErrorf(t, err, "delete branch %s", mdsFeatureBranch)
	require.Equalf(t, http.StatusNoContent, res.StatusCode(), "delete branch %s: %s", mdsFeatureBranch, res.Body)

	env.waitMetadataBranchGone(t, mdsFeatureBranch)
	require.Equalf(t, http.StatusOK, env.metadataBranchStatus(t, mdsBranch),
		"R-metadata's default branch must never be deleted")

	tag, err := env.admin.GetTagWithResponse(t.Context(), env.metadataRepo, indexer.TagPrefix+featureCommit)
	require.NoErrorf(t, err, "get the tag of the deleted branch's commit")
	require.Equalf(t, http.StatusOK, tag.StatusCode(),
		"§4.8: the deleted branch's tags stay, so its history is still addressable; body: %s", tag.Body)
	require.NotNilf(t, tag.JSON200, "get tag %s%s: %s", indexer.TagPrefix, featureCommit, tag.Body)
	require.Equal(t, tagged, tag.JSON200.CommitId, "the tag must still point at the indexed commit")

	read := env.readRows(t, featureCommit)
	require.Contains(t, mdsPaths(read.PyIceberg.Rows), "data/feature-only.txt",
		"a commit read of a deleted branch's history must still work through its mds-* tag")

	report, ok := env.indexerHealth(t)
	require.True(t, ok, "the indexer must still serve /_health")
	_, found := report.Repositories[env.repo].Branches[mdsFeatureBranch]
	require.Falsef(t, found,
		"§4.8: /_health must not keep an entry for the deleted branch; report: %+v", report.Repositories[env.repo])
}

// mdsTableSourceCommit is the table property that holds the indexing position (§3.1, §3.3, §9).
const mdsTableSourceCommit = "lakefs.mds.source-commit"

// ---------------------------------------------------------------------------------------------
// the throw-away installation
// ---------------------------------------------------------------------------------------------

// newMDSEnv starts lakefs-sso with Metadata Search enabled, creates the indexer's credentials and
// returns the environment. The indexer itself is not started: a test starts it when the state it
// wants to index exists (which is what makes the backfill measurement meaningful).
func newMDSEnv(t *testing.T) *mdsEnv {
	t.Helper()
	requireIcebergS3Blockstore(t)

	env := newMDSEnvSkeleton(t, "mds-e2e-"+icebergUniqueSuffix())
	env.s3 = icebergS3Settings(t)
	env.bucket, env.runPrefix, env.storageNamespace = icebergStorageNamespace(t, env.repo)
	env.blockstoreSection = mdsS3BlockstoreSection(env.s3)
	env.storageSection = mdsS3StorageSection(env.s3)
	env.setupStorage = env.ensureBucket
	env.cleanupStorage = env.deleteRunPrefix
	return env.start(t)
}

// newMDSEnvSkeleton allocates everything that does not depend on the blockstore: the two free
// ports, the lakefs-sso binary, the working directory and the OIDC issuer the sso: section needs.
// The caller fills in the storage profile - the namespace, the configuration fragments and the two
// storage life-cycle functions - and then calls start, so the S3 suite (newMDSEnv) and the Azure
// acceptance test (newMDSAzureEnv, mds_azure_test.go) differ in the blockstore alone.
func newMDSEnvSkeleton(t *testing.T, repo string) *mdsEnv {
	t.Helper()
	requireIcebergDocker(t)

	dir := t.TempDir()
	binary := icebergServerBinary(t)
	ports := mdsFreePorts(t, 2)
	serverPort, indexerPort := ports[0], ports[1]
	ctx, cancel := context.WithCancel(t.Context())
	env := &mdsEnv{
		ctx:    ctx,
		cancel: cancel,
		icebergEnv: &icebergEnv{
			baseURL:       fmt.Sprintf("http://127.0.0.1:%d", serverPort),
			repo:          repo,
			branch:        mdsBranch,
			key:           DefaultAdminAccessKeyID,
			secret:        DefaultAdminSecretAccessKey,
			clientImage:   icebergClientImage(),
			workDir:       dir,
			logPath:       filepath.Join(dir, "lakefs-sso.log"),
			catalogURL:    fmt.Sprintf("http://127.0.0.1:%d", serverPort) + icebergcatalog.APIBasePath,
			relativeToURL: fmt.Sprintf("http://127.0.0.1:%d", serverPort) + icebergcatalog.RelativeToBasePath,
		},
		binary:               binary,
		dir:                  dir,
		configPath:           filepath.Join(dir, "lakefs.yaml"),
		indexPath:            filepath.Join(dir, "mds-indexer.log"),
		indexerURL:           fmt.Sprintf("http://127.0.0.1:%d", indexerPort),
		listenAddress:        fmt.Sprintf("0.0.0.0:%d", serverPort),
		indexerListenAddress: fmt.Sprintf("0.0.0.0:%d", indexerPort),
		issuer:               icebergOIDCIssuer(t),
	}
	env.metadataRepo = icebergcatalog.MetadataRepository(env.repo)
	return env
}

// start runs the server of a skeleton environment, waits for it, asserts the Metadata Search
// bootstrap of §8, creates the data repository and writes the final configuration with the
// credentials the suite created for mds-indexer. The indexer is started by the case itself.
func (e *mdsEnv) start(t *testing.T) *mdsEnv {
	t.Helper()
	e.setupStorage(t)

	// §7 defaults, written explicitly so a maintenance case can override one without losing the
	// others. tag_retention 0 keeps every tag; compaction fires above 200 delete files or every
	// 500 indexed commits; a deleted data branch loses its metadata branch.
	e.tagRetention = "0s"
	e.deleteBranches = true
	e.compactionMaxDeleteFiles = 200
	e.compactionEveryCommits = 500
	e.startIndexerProxy(t)

	// The server and the indexer read one file, as §7 documents. The metadata_search section is
	// written without credentials first, because the mds-indexer user does not exist until the
	// server has bootstrapped it; the file is rewritten with the created credentials before the
	// indexer starts.
	e.writeConfig(t, "", "")
	e.startServer(t)
	t.Cleanup(func() { e.stop(t) })

	e.waitForServer(t)
	e.admin = e.coreAPI(t)
	e.requireMDSCore(t)
	e.createRepository(t)
	e.indexerKey, e.indexerSecret = e.createIndexerCredentials(t)
	e.writeConfig(t, e.indexerKey, e.indexerSecret)
	return e
}

// writeConfig writes the one lakefs.yaml both processes read (design §7).
//
// The blockstore section is spelled out in full instead of being inherited from the ambient
// LAKEFS_* variables: a partial section would shadow viper's nested defaults, and the server, the
// indexer and the query-engine containers must all address the same bucket with the same
// credentials and path style. The section itself is the storage profile's (e.blockstoreSection),
// so the Azure case writes an Azure blockstore and no metadata_search.storage at all.
func (e *mdsEnv) writeConfig(t *testing.T, indexerKey, indexerSecret string) {
	t.Helper()
	document := fmt.Sprintf(`listen_address: %q
logging:
  format: text
  level: "warn"
database:
  type: local
  local:
    path: %q
# The break-glass administrator of the SSO auth service. installation.* makes run seed it, and the
# ACL/RBAC base groups, before the extra routes builders - i.e. before the Metadata Search bootstrap
# of §8 attaches the metadata-repo deny, which is what makes that deny real in this run.
installation:
  user_name: %q
  access_key_id: %q
  secret_access_key: %q
auth:
  encrypt:
    secret_key: "esti-metadata-search-auth-secret"
  ui_config:
    rbac: "internal"
  # The refusals of §9 are asserted as 403 (plan M6). lakeFS answers an authorization failure 401
  # by default and only the opt-in changes that (auth.authorization_failure_status, CHANGELOG_SSO.md),
  # so the suite sets it: a guard-group member's CreateRepository on a companion namespace and a
  # non-indexer's write into R-metadata are then the 403 the plan names, on the core API too.
  authorization_failure_status: 403
sso:
  enabled: true
  client_id: "esti-mds"
  client_secret: "esti-mds-client-secret"
  issuer_url: %q
  callback_base_url: %q
%siceberg:
  enabled: true
  prefix: %q
  metadata_search:
    enabled: true
# The indexer section (§7). The server ignores it; lakefs-sso mds run reads only it.
metadata_search:
  lakefs_mds_endpoint: %q
  access_key_id: %q
  secret_key: %q
  listen_address: %q
  period: %s
  concurrency: 4
  max_commits: 1000
  backfill: head
  delete_branches: %t
  tag_retention: %s
  compaction:
    max_delete_files: %d
    every_commits: %d
%s%s`, e.listenAddress, filepath.Join(e.dir, "metadata"),
		mdsAdminUsername, e.key, e.secret,
		e.issuer, "http://"+e.listenAddress,
		e.blockstoreSection, icebergcatalog.CatalogPrefix,
		e.indexerEndpoint, indexerKey, indexerSecret, e.indexerListenAddress, mdsIndexerPeriod,
		e.deleteBranches, e.tagRetention, e.compactionMaxDeleteFiles, e.compactionEveryCommits,
		e.storageSectionYAML(), e.repositoriesSection())

	require.NoError(t, os.WriteFile(e.configPath, []byte(document), mdsConfigFileMode),
		"write the lakefs.yaml both processes read")
}

// storageSectionYAML renders metadata_search.storage, or nothing at all when the storage profile
// configures the Indexer's FileIO some other way: the Azure case leaves it to the
// LAKEFS_METADATA_SEARCH_AZURE_* environment of the indexer process (METADATA_SEARCH_AZURE G4).
func (e *mdsEnv) storageSectionYAML() string {
	if e.storageSection == "" {
		return ""
	}
	return "  storage:\n" + e.storageSection
}

// repositoriesSection renders the repositories of the indexer configuration: the suite's own data
// repository, indexed on both of its branches, and the extra ones a case configured. An extra
// repository is indexed on the suite's branch alone: it exists to be refused or to be a control,
// and a branch it does not have would only add a per-branch error to its report.
func (e *mdsEnv) repositoriesSection() string {
	var repositories strings.Builder
	repositories.WriteString("  repositories:\n")
	fmt.Fprintf(&repositories, "    %s:\n      branches: [%s, %s]\n", e.repo, mdsBranch, mdsFeatureBranch)
	for _, name := range e.extraRepositories {
		fmt.Fprintf(&repositories, "    %s:\n      branches: [%s]\n", name, mdsBranch)
	}
	return repositories.String()
}

// mdsS3BlockstoreSection is the blockstore section of the S3 profile, written out in full.
func mdsS3BlockstoreSection(s3 icebergS3Config) string {
	var blockstore strings.Builder
	blockstore.WriteString("blockstore:\n  type: s3\n  s3:\n")
	fmt.Fprintf(&blockstore, "    region: %q\n", s3.Region)
	if s3.Endpoint != "" {
		fmt.Fprintf(&blockstore, "    endpoint: %q\n", s3.Endpoint)
	}
	fmt.Fprintf(&blockstore, "    force_path_style: %t\n", s3.PathStyle)
	blockstore.WriteString("    discover_bucket_region: false\n")
	fmt.Fprintf(&blockstore, "    credentials:\n      access_key_id: %q\n      secret_access_key: %q\n",
		s3.AccessKey, s3.SecretKey)
	return blockstore.String()
}

// mdsS3StorageSection is metadata_search.storage of the S3 profile: the same endpoint and
// credentials, in the iceberg-go property names of design §4.6.
func mdsS3StorageSection(s3 icebergS3Config) string {
	var storage strings.Builder
	if s3.Endpoint != "" {
		fmt.Fprintf(&storage, "    s3.endpoint: %q\n", s3.Endpoint)
	}
	fmt.Fprintf(&storage, "    s3.region: %q\n", s3.Region)
	fmt.Fprintf(&storage, "    s3.access-key-id: %q\n", s3.AccessKey)
	fmt.Fprintf(&storage, "    s3.secret-access-key: %q\n", s3.SecretKey)
	if s3.PathStyle {
		storage.WriteString("    s3.path-style-access: \"true\"\n")
	}
	return storage.String()
}

// ensureBucket makes sure the bucket the run allocates its prefix in exists. On AWS it does; on the
// SeaweedFS instance of esti/ops/docker-compose-mds.yaml it may not, and creating it here keeps the
// documented local run a single command.
func (e *mdsEnv) ensureBucket(t *testing.T) {
	t.Helper()
	cl := e.s3Client(t)
	ctx, cancel := context.WithTimeout(t.Context(), mdsRequestTimeout)
	defer cancel()
	exists, err := cl.BucketExists(ctx, e.bucket)
	if err == nil && exists {
		return
	}
	err = cl.MakeBucket(ctx, e.bucket, minio.MakeBucketOptions{Region: e.s3.Region})
	require.NoErrorf(t, err, "create the bucket %q the Metadata Search run writes into", e.bucket)
}

// mdsProxy is an in-process reverse proxy in front of the lakeFS server. The indexer's
// metadata_search.lakefs_mds_endpoint names it, so a case can observe the exact request boundary
// of §4.4: the Iceberg commit has already staged the metadata table on R-metadata@b when the
// indexer sends the following POST of the lakeFS commit. Holding that request and killing the
// indexer lands the kill after step 3 and before step 4, deterministically, instead of hoping to
// catch the few milliseconds between two ticks.
type mdsProxy struct {
	server       *httptest.Server
	reverse      *httputil.ReverseProxy
	metadataRepo string
	branch       string

	mu   sync.Mutex
	gate *mdsCommitGate
}

// mdsCommitGate is one armed hold of the next lakeFS commit of R-metadata@branch. reached is
// closed when the commit request arrives; release allows the handler to finish without forwarding
// the request.
type mdsCommitGate struct {
	reached chan struct{}
	release chan struct{}

	releaseOnce sync.Once
}

// letGo releases a held commit request. It is safe to call more than once (the test releases it
// after the kill and the cleanup releases it again if the case failed before that point).
func (g *mdsCommitGate) letGo() {
	g.releaseOnce.Do(func() { close(g.release) })
}

// startIndexerProxy starts the proxy and points the indexer configuration at it. Every request is
// forwarded unchanged except a commit request while a hold is armed.
func (e *mdsEnv) startIndexerProxy(t *testing.T) {
	t.Helper()
	target, err := url.Parse(e.baseURL)
	require.NoErrorf(t, err, "parse the lakeFS URL %q for the indexer proxy", e.baseURL)
	p := &mdsProxy{
		reverse:      httputil.NewSingleHostReverseProxy(target),
		metadataRepo: e.metadataRepo,
		branch:       mdsBranch,
	}
	p.server = httptest.NewServer(http.HandlerFunc(p.serveHTTP))
	e.indexerProxy = p
	e.indexerEndpoint = p.server.URL
	t.Cleanup(func() { p.server.Close() })
}

// holdNextMetadataCommit arms the next lakeFS commit of R-metadata@main. The returned gate's
// reached channel is closed when that request is in flight, which is the observed signal the
// restart case waits for; the request itself is never forwarded.
func (p *mdsProxy) holdNextMetadataCommit(t *testing.T) *mdsCommitGate {
	t.Helper()
	gate := &mdsCommitGate{reached: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	require.Nil(t, p.gate, "a lakeFS commit hold is already armed")
	p.gate = gate
	p.mu.Unlock()
	t.Cleanup(gate.letGo)
	return gate
}

// takeGate consumes an armed hold. A hold is consumed by the first matching request, so a retry
// after the case has decided the window was reached is not held again.
func (p *mdsProxy) takeGate() *mdsCommitGate {
	p.mu.Lock()
	defer p.mu.Unlock()
	gate := p.gate
	p.gate = nil
	return gate
}

func (p *mdsProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if isMetadataCommitRequest(r, p.metadataRepo, p.branch) {
		if gate := p.takeGate(); gate != nil {
			close(gate.reached)
			select {
			case <-gate.release:
			case <-r.Context().Done():
			case <-time.After(mdsHeldCommitTimeout):
			}
			// The request never reaches the server: the process being killed has already
			// staged the Iceberg commit, and no lakeFS commit exists for it.
			http.Error(w, "the Metadata Search restart case held this lakeFS commit", http.StatusServiceUnavailable)
			return
		}
	}
	p.reverse.ServeHTTP(w, r)
}

// isMetadataCommitRequest reports whether r is POST .../repositories/<metadataRepo>/branches/<branch>/commits.
func isMetadataCommitRequest(r *http.Request, metadataRepo, branch string) bool {
	if r.Method != http.MethodPost {
		return false
	}
	want := "/api/v1/repositories/" + url.PathEscape(metadataRepo) + "/branches/" + url.PathEscape(branch) + "/commits"
	return r.URL.Path == want
}

// waitForMetadataCommitWindow waits, with a bounded timeout, until the indexer has sent the lakeFS
// commit of R-metadata and the proxy holds it. A kill outside that window proves nothing about
// §4.4, so a missed window fails the case with the indexer's observed state.
func (e *mdsEnv) waitForMetadataCommitWindow(t *testing.T, gate *mdsCommitGate, sourceCommit string) {
	t.Helper()
	timer := time.NewTimer(mdsCommitWindowTimeout)
	defer timer.Stop()
	select {
	case <-gate.reached:
		t.Logf("the indexer staged %s on %s@%s and its lakeFS commit is in flight; killing it now",
			sourceCommit, e.metadataRepo, mdsBranch)
		return
	case <-e.indexer.done:
		e.exitedWhileRunning(t, e.indexer, "waiting for the lakeFS commit of "+sourceCommit)
		t.Fatalf("the indexer exited before it sent the lakeFS commit of %s", sourceCommit)
	case <-timer.C:
		report := "unavailable"
		if health, ok := e.indexerHealth(t); ok {
			encoded, _ := json.Marshal(health)
			report = string(encoded)
		}
		t.Fatalf("the indexer did not send the lakeFS commit of %s within %s, so a kill would land between ticks and prove nothing about §4.4; last health report: %s\nlast %d lines of the indexer log:\n%s",
			sourceCommit, mdsCommitWindowTimeout, report, mdsLogTailLines, mdsLogTail(e.indexPath, mdsLogTailLines))
	}
}

// metadataStaging returns the uncommitted changes of R-metadata@branch. §4.4 row 1 calls the
// non-empty staging the observable state a crash after step 3 leaves behind.
func (e *mdsEnv) metadataStaging(t *testing.T, branch string) []apigen.Diff {
	t.Helper()
	res, err := e.admin.DiffBranchWithResponse(t.Context(), e.metadataRepo, branch, &apigen.DiffBranchParams{})
	require.NoErrorf(t, err, "diff the uncommitted changes of %s@%s", e.metadataRepo, branch)
	require.Equalf(t, http.StatusOK, res.StatusCode(), "diff the uncommitted changes of %s@%s: %s",
		e.metadataRepo, branch, res.Body)
	require.NotNilf(t, res.JSON200, "diff the uncommitted changes of %s@%s: %s", e.metadataRepo, branch, res.Body)
	return res.JSON200.Results
}

// waitMetadataStagingEmpty waits until R-metadata@branch has no uncommitted changes. The restart
// case uses it after the recovery: a recovered staged pointer is committed, not left behind.
func (e *mdsEnv) waitMetadataStagingEmpty(t *testing.T, branch string) {
	t.Helper()
	deadline := time.Now().Add(mdsIndexTimeout)
	for {
		e.exitedWhileRunning(t, e.indexer, "waiting for the staging of "+e.metadataRepo+"@"+branch+" to clear")
		if len(e.metadataStaging(t, branch)) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s@%s still has uncommitted changes after %s; last %d lines of the indexer log:\n%s",
				e.metadataRepo, branch, mdsIndexTimeout, mdsLogTailLines, mdsLogTail(e.indexPath, mdsLogTailLines))
		}
		time.Sleep(mdsPollInterval)
	}
}

// mdsProcess is one watched child process. A goroutine reaps it, so its exit status is recorded
// as soon as it happens; done is closed at that point and every wait loop of the suite selects on
// it. An exit nobody asked for (killRequested stays false) is the failure that cancels the
// environment, which stops the sibling process and every request and wait of the test.
type mdsProcess struct {
	name    string
	cmd     *exec.Cmd
	logPath string

	done chan struct{}

	// mu guards the fields below: the reaper writes them, the test reads them after done is
	// closed (and also while it is open, for the diagnostic message of a stop failure).
	mu            sync.Mutex
	err           error
	killRequested bool
}

// startProcess starts name with args, tees its output to logPath and reaps it in the background.
// A non-nil extraEnv is appended to os.Environ() for this process only, which is how the Azure case
// configures the indexer through LAKEFS_METADATA_SEARCH_AZURE_* alone (METADATA_SEARCH_AZURE G4).
func (e *mdsEnv) startProcess(t *testing.T, name, logPath string, extraEnv []string, args ...string) *mdsProcess {
	t.Helper()
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, mdsConfigFileMode)
	require.NoErrorf(t, err, "open the %s log file", name)
	cmd := exec.CommandContext(e.ctx, e.binary, args...)
	cmd.Dir = e.dir
	if extraEnv != nil {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoErrorf(t, cmd.Start(), "start %s", name)

	p := &mdsProcess{name: name, cmd: cmd, logPath: logPath, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		_ = logFile.Close()
		p.mu.Lock()
		p.err = err
		killRequested := p.killRequested
		p.mu.Unlock()
		close(p.done)
		if !killRequested {
			// Nobody asked for this exit: the process the test needs is gone. Cancelling the
			// environment kills the sibling and makes every request and wait return at once.
			e.cancel()
		}
	}()
	return p
}

// stop kills the process if it is still running and waits for the reaper.
func (e *mdsEnv) stopProcess(t *testing.T, p *mdsProcess) {
	t.Helper()
	if p == nil {
		return
	}
	p.mu.Lock()
	p.killRequested = true
	err := p.err
	p.mu.Unlock()
	if err == nil {
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.done:
	case <-time.After(mdsIndexerStartTimeout):
		t.Errorf("%s did not exit within %s of the kill", p.name, mdsIndexerStartTimeout)
	}
}

// exitedWhileRunning reports whether p ended without the suite asking it to, and fails the test
// with the exit status and the tail of its log when it did. Every wait of the suite calls it.
func (e *mdsEnv) exitedWhileRunning(t *testing.T, p *mdsProcess, doing string) {
	t.Helper()
	if p == nil {
		return
	}
	select {
	case <-p.done:
		p.mu.Lock()
		err, killRequested := p.err, p.killRequested
		p.mu.Unlock()
		if killRequested {
			return
		}
		t.Fatalf("%s exited while %s (exit: %v); last %d lines of its log:\n%s",
			p.name, doing, err, mdsLogTailLines, mdsLogTail(p.logPath, mdsLogTailLines))
	default:
	}
}

// mdsLogTail returns the last lines of a process log, for a failure message. A missing or empty
// log is reported as such rather than as an error.
func mdsLogTail(path string, lines int) string {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("(log unavailable: %v)", err)
	}
	split := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if len(split) > lines {
		split = split[len(split)-lines:]
	}
	return strings.Join(split, "\n")
}

// startServer starts `lakefs-sso run` on the generated configuration.
func (e *mdsEnv) startServer(t *testing.T) {
	t.Helper()
	e.server = e.startProcess(t, "lakefs-sso", e.logPath, nil, "run", "--config", e.configPath)
}

// startIndexer starts `lakefs-sso mds run` on the same configuration (§4.1) and waits until its own
// HTTP server answers.
func (e *mdsEnv) startIndexer(t *testing.T) {
	t.Helper()
	require.Nil(t, e.indexer, "the indexer is already running; stop it first")
	e.indexer = e.startProcess(t, "lakefs-sso mds run", e.indexPath, e.indexerEnv,
		"mds", "run", "--config", e.configPath)
	t.Logf("metadata search indexer started (pid %d, period %s)", e.indexer.cmd.Process.Pid, mdsIndexerPeriod)
	e.waitForIndexerServer(t)
}

// stopIndexer stops the indexer if it is running.
func (e *mdsEnv) stopIndexer(t *testing.T) {
	t.Helper()
	e.stopProcess(t, e.indexer)
	e.indexer = nil
}

// waitForServer waits for the server's /_health and fails at once if the server exits. It replaces
// the health wait of the Iceberg environment, which cannot watch a process it did not start.
func (e *mdsEnv) waitForServer(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: icebergHealthTimeout}
	deadline := time.Now().Add(mdsServerStartTimeout)
	for {
		e.exitedWhileRunning(t, e.server, "waiting for /_health")
		resp, err := client.Get(e.baseURL + "/_health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("lakefs-sso did not become healthy at %s within %s; last %d lines of its log:\n%s",
				e.baseURL, mdsServerStartTimeout, mdsLogTailLines, mdsLogTail(e.logPath, mdsLogTailLines))
		}
		time.Sleep(icebergHealthInterval)
	}
}

// stop stops the indexer and the server and removes the objects of the run. It is registered as
// one cleanup so the order is explicit: the processes are gone before the bucket prefix is swept,
// and the logs are dumped while they are still wanted.
func (e *mdsEnv) stop(t *testing.T) {
	t.Helper()
	e.stopIndexer(t)
	e.stopProcess(t, e.server)
	e.server = nil
	e.cancel()

	// The run's objects are removed with the credentials the server used: lakeFS never deletes
	// physical objects, so the suite cleans its own prefix. The context of the test is already
	// cancelled here, hence the fresh one.
	ctx, cancel := context.WithTimeout(context.Background(), mdsCleanupTimeout)
	defer cancel()
	if err := e.cleanupStorage(ctx); err != nil {
		t.Errorf("delete the run prefix of %s: %v", e.repo, err)
	}

	if t.Failed() {
		for _, log := range []struct{ name, path string }{
			{"lakefs-sso", e.logPath},
			{"lakefs-sso mds run", e.indexPath},
		} {
			const maxLogBytes = 64 << 10
			body, err := os.ReadFile(log.path)
			if err != nil {
				t.Logf("%s log unavailable: %v", log.name, err)
				continue
			}
			if len(body) > maxLogBytes {
				body = body[len(body)-maxLogBytes:]
			}
			t.Logf("%s log (tail):\n%s", log.name, body)
		}
	}
}

// ---------------------------------------------------------------------------------------------
// principals of §8
// ---------------------------------------------------------------------------------------------

// requireMDSCore asserts that the server's Metadata Search bootstrap ran: the mds-indexer user
// and allow policy exist, the metadata-repo deny is stored only as numeric chunks (R3S-2: the deny
// has no repository-independent statement, so it has no base policy), and every chunk is attached
// to the base groups, which is what makes the deny of §8 enforced in this installation.
func (e *mdsEnv) requireMDSCore(t *testing.T) {
	t.Helper()
	user, err := e.admin.GetUserWithResponse(t.Context(), bootstrap.UserName)
	require.NoErrorf(t, err, "get user %s", bootstrap.UserName)
	require.Equalf(t, http.StatusOK, user.StatusCode(),
		"the server must bootstrap the %s user of §8 at startup: %s", bootstrap.UserName, user.Body)

	// The allow policy has repository-independent statements, so its base policy always exists.
	indexer, err := e.admin.GetPolicyWithResponse(t.Context(), bootstrap.IndexerPolicyName)
	require.NoErrorf(t, err, "get policy %s", bootstrap.IndexerPolicyName)
	require.Equalf(t, http.StatusOK, indexer.StatusCode(),
		"the server must bootstrap the %s policy of §8 at startup: %s", bootstrap.IndexerPolicyName, indexer.Body)

	// R3S-2: the deny has no repository-independent statement, and a policy with zero statements
	// is not a policy (api/authorization.yml requires minItems: 1), so the base name must not
	// exist at all - the deny lives only in its numeric chunks.
	base, err := e.admin.GetPolicyWithResponse(t.Context(), bootstrap.MetadataRepoProtectPolicyName)
	require.NoErrorf(t, err, "get policy %s", bootstrap.MetadataRepoProtectPolicyName)
	require.Equalf(t, http.StatusNotFound, base.StatusCode(),
		"the %s deny of §8 must not exist as a base policy, only as numeric chunks: %s: %s",
		bootstrap.MetadataRepoProtectPolicyName, base.Status(), base.Body)

	// List the chunks the server wrote. Chunk names are matched exactly (base + '-' + digits),
	// never by a loose prefix, so a policy the bootstrap does not own cannot pass for a chunk.
	prefix := apigen.PaginationPrefix(bootstrap.MetadataRepoProtectPolicyName)
	amount := apigen.PaginationAmount(-1)
	policies, err := e.admin.ListPoliciesWithResponse(t.Context(), &apigen.ListPoliciesParams{
		Prefix: &prefix,
		Amount: &amount,
	})
	require.NoErrorf(t, err, "list the %s chunk policies", bootstrap.MetadataRepoProtectPolicyName)
	require.Equalf(t, http.StatusOK, policies.StatusCode(),
		"list the %s chunk policies: %s", bootstrap.MetadataRepoProtectPolicyName, policies.Body)
	require.NotNilf(t, policies.JSON200, "list the %s chunk policies: %s",
		bootstrap.MetadataRepoProtectPolicyName, policies.Body)
	chunks := make([]string, 0, len(policies.JSON200.Results))
	for _, policy := range policies.JSON200.Results {
		if bootstrap.IsMetadataRepoProtectChunk(policy.Id) {
			chunks = append(chunks, policy.Id)
		}
	}
	require.NotEmptyf(t, chunks,
		"the server must bootstrap at least one %s-<n> deny chunk of §8 at startup; policies with that prefix: %+v",
		bootstrap.MetadataRepoProtectPolicyName, policies.JSON200.Results)
	sort.Strings(chunks)

	// Every chunk is attached to the base groups of §8 - the same guard list the server passes to
	// the deny, whatever group template the installation runs. A group of the other template does
	// not exist here and the bootstrap skips it, so a 404 is expected for it; a base group that
	// does exist must carry every chunk, or the deny is not in force for its members. Admins is
	// the break-glass administrator's group, so this is also the attachment the "non-indexer
	// cannot write" case needs.
	for _, group := range datasets.PinsProtectGroups(nil) {
		res, err := e.admin.ListGroupPoliciesWithResponse(t.Context(), group, &apigen.ListGroupPoliciesParams{
			Prefix: &prefix,
			Amount: &amount,
		})
		require.NoErrorf(t, err, "list the policies of the %s group", group)
		if res.StatusCode() == http.StatusNotFound {
			continue
		}
		require.Equalf(t, http.StatusOK, res.StatusCode(), "list the policies of %s: %s", group, res.Body)
		require.NotNilf(t, res.JSON200, "list the policies of %s: %s", group, res.Body)
		attached := make([]string, 0, len(res.JSON200.Results))
		for _, policy := range res.JSON200.Results {
			require.Truef(t, bootstrap.IsMetadataRepoProtectChunk(policy.Id),
				"%s carries %q, which is not a numeric %s chunk of §8", group, policy.Id,
				bootstrap.MetadataRepoProtectPolicyName)
			attached = append(attached, policy.Id)
		}
		sort.Strings(attached)
		require.ElementsMatchf(t, chunks, attached,
			"every %s-<n> deny chunk of §8 must be attached to the base group %s, or the write "+
				"protection is not in force in this run", bootstrap.MetadataRepoProtectPolicyName, group)
	}
}

// createIndexerCredentials creates the access key pair of mds-indexer, which §8 leaves to the
// administrator: the bootstrap creates the user, the operator creates its credentials and puts them
// in the indexer configuration.
func (e *mdsEnv) createIndexerCredentials(t *testing.T) (key, secret string) {
	t.Helper()
	res, err := e.admin.CreateCredentialsWithResponse(t.Context(), bootstrap.UserName)
	require.NoErrorf(t, err, "create credentials for %s", bootstrap.UserName)
	require.Equalf(t, http.StatusCreated, res.StatusCode(),
		"create credentials for %s: %s", bootstrap.UserName, res.Body)
	require.NotNilf(t, res.JSON201, "create credentials for %s: %s", bootstrap.UserName, res.Body)
	require.NotEmpty(t, res.JSON201.AccessKeyId)
	require.NotEmpty(t, res.JSON201.SecretAccessKey)
	return res.JSON201.AccessKeyId, res.JSON201.SecretAccessKey
}

// mdsPrincipal is one principal the suite creates to assert the authorization rules of §9.
type mdsPrincipal struct {
	username string
	key      string
	secret   string
}

// createPrincipal creates a user with an optional policy and one access key pair.
//
// The user is deliberately not added to any group: the policy is the whole grant, so the case that
// asserts a 403 cannot be satisfied by a base-group policy.
func (e *mdsEnv) createPrincipal(t *testing.T, username string, statements []apigen.Statement) mdsPrincipal {
	t.Helper()
	created, err := e.admin.CreateUserWithResponse(t.Context(), apigen.CreateUserJSONRequestBody{Id: username})
	require.NoErrorf(t, err, "create user %s", username)
	require.Equalf(t, http.StatusCreated, created.StatusCode(), "create user %s: %s", username, created.Body)

	if len(statements) > 0 {
		policyID := username + "-policy"
		policy, err := e.admin.CreatePolicyWithResponse(t.Context(), apigen.CreatePolicyJSONRequestBody{
			Id:        policyID,
			Statement: statements,
		})
		require.NoErrorf(t, err, "create policy %s", policyID)
		require.Equalf(t, http.StatusCreated, policy.StatusCode(), "create policy %s: %s", policyID, policy.Body)

		attached, err := e.admin.AttachPolicyToUserWithResponse(t.Context(), username, policyID)
		require.NoErrorf(t, err, "attach policy %s to %s", policyID, username)
		require.Equalf(t, http.StatusCreated, attached.StatusCode(),
			"attach policy %s to %s: %s", policyID, username, attached.Body)
	}

	creds, err := e.admin.CreateCredentialsWithResponse(t.Context(), username)
	require.NoErrorf(t, err, "create credentials for %s", username)
	require.Equalf(t, http.StatusCreated, creds.StatusCode(), "create credentials for %s: %s", username, creds.Body)
	require.NotNilf(t, creds.JSON201, "create credentials for %s: %s", username, creds.Body)
	return mdsPrincipal{username: username, key: creds.JSON201.AccessKeyId, secret: creds.JSON201.SecretAccessKey}
}

// mdsRepoPattern is the repository ARN a policy statement matches (§8 spells the same shape).
func mdsRepoPattern(repo string) string { return "arn:lakefs:fs:::repository/" + repo }

// coreAPIAs builds a core API client that authenticates as one principal, so a case can act as a
// user other than the break-glass administrator.
func (e *mdsEnv) coreAPIAs(t *testing.T, key, secret string) *apigen.ClientWithResponses {
	t.Helper()
	client, err := testutil.NewClientFromCreds(logging.ContextUnavailable(), key, secret, e.baseURL+apiutil.BaseURL)
	require.NoError(t, err, "build a core API client for a suite principal")
	return client
}

// groupID resolves the id the membership API takes for a group name. The in-repo ACL service keys a
// group by its display name and reports it as the id, while the API's Group carries both fields; a
// group that is missing is a failed case, not a skip - the bootstrap of §8 is what should have
// created it, and the deny is attached to it.
func (e *mdsEnv) groupID(t *testing.T, name string) string {
	t.Helper()
	amount := apigen.PaginationAmount(mdsPolicyListAmount)
	res, err := e.admin.ListGroupsWithResponse(e.ctx, &apigen.ListGroupsParams{Amount: &amount})
	require.NoError(t, err, "list the groups")
	require.Equalf(t, http.StatusOK, res.StatusCode(), "list the groups: %s", res.Body)
	require.NotNilf(t, res.JSON200, "list the groups: %s", res.Body)
	for _, group := range res.JSON200.Results {
		if group.Id == name || (group.Name != nil && *group.Name == name) {
			if group.Id != "" {
				return group.Id
			}
			return name
		}
	}
	t.Fatalf("the group %q does not exist in this installation; the groups are %v", name, res.JSON200.Results)
	return ""
}

// waitMetadataRepository waits until the indexer has created R-metadata, which it does on its
// first tick of a data repository (§3.3), and fails the case with the indexer's log tail if that does
// not happen within mdsIndexTimeout.
func (e *mdsEnv) waitMetadataRepository(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(mdsIndexTimeout)
	for {
		res, err := e.admin.GetRepositoryWithResponse(e.ctx, e.metadataRepo)
		require.NoErrorf(t, err, "read %s", e.metadataRepo)
		switch res.StatusCode() {
		case http.StatusOK:
			return
		case http.StatusNotFound:
		default:
			t.Fatalf("read %s: %d: %s", e.metadataRepo, res.StatusCode(), res.Body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the indexer did not create %s within %s (design §3.3)\nindexer log:\n%s",
				e.metadataRepo, mdsIndexTimeout, mdsLogTail(e.indexPath, mdsLogTailLines))
		}
		time.Sleep(mdsPollInterval)
	}
}

// waitCompanionNamespaceDeny waits until the deny policy set of §8 names the given storage-namespace
// ARNs, and fails the case with the policy set it saw if that does not happen within
// mdsPolicyRefreshTimeout.
//
// The set is re-derived by the bootstrap's refresher while the server runs
// (bootstrap.startPolicyRefresher), so a companion repository created after the server started is
// named at most one refresh interval later. Reading the policy back is the only way to know the deny
// is in force without trying the write it refuses, and the write is what the case asserts.
func (e *mdsEnv) waitCompanionNamespaceDeny(t *testing.T, groupID string, resources map[string]struct{}) {
	t.Helper()
	amount := apigen.PaginationAmount(mdsPolicyListAmount)
	deadline := time.Now().Add(mdsPolicyRefreshTimeout)
	var seen []string
	for {
		res, err := e.admin.ListGroupPoliciesWithResponse(e.ctx, groupID, &apigen.ListGroupPoliciesParams{Amount: &amount})
		require.NoErrorf(t, err, "list the policies of group %s", groupID)
		require.Equalf(t, http.StatusOK, res.StatusCode(), "list the policies of group %s: %s", groupID, res.Body)
		require.NotNilf(t, res.JSON200, "list the policies of group %s: %s", groupID, res.Body)

		seen = seen[:0]
		for _, policy := range res.JSON200.Results {
			if !strings.HasPrefix(policy.Id, bootstrap.MetadataRepoProtectPolicyName+"-") {
				continue
			}
			for _, statement := range policy.Statement {
				seen = append(seen, policy.Id+": "+statement.Resource)
				if statement.Effect != "deny" || !containsString(statement.Action, permissions.AttachStorageNamespaceAction) {
					continue
				}
				if _, ok := resources[statement.Resource]; ok {
					return
				}
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("the deny of §8 does not name the companion storage namespace within %s; the %s policies "+
				"attached to group %s carried: %v; wanted one of %v",
				mdsPolicyRefreshTimeout, bootstrap.MetadataRepoProtectPolicyName, groupID, seen, sortedKeysOfSet(resources))
		}
		time.Sleep(mdsPollInterval)
	}
}

// sortedKeysOfSet returns the keys of a string set in order, so a failure message is stable.
func sortedKeysOfSet(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------------------------------------
// the data repository and the objects
// ---------------------------------------------------------------------------------------------

// uploadObject writes one object through the core API on branch.
func (e *mdsEnv) uploadObject(t *testing.T, branch, path, content string) {
	t.Helper()
	res, err := e.admin.UploadObjectWithBodyWithResponse(e.ctx, e.repo, branch,
		&apigen.UploadObjectParams{Path: path}, mdsObjectContentType, strings.NewReader(content))
	require.NoErrorf(t, err, "upload %s on %s", path, branch)
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "upload %s on %s: %s", path, branch, res.Body)
}

// deleteObject removes one object through the core API on branch.
func (e *mdsEnv) deleteObject(t *testing.T, branch, path string) {
	t.Helper()
	res, err := e.admin.DeleteObjectWithResponse(e.ctx, e.repo, branch,
		&apigen.DeleteObjectParams{Path: path})
	require.NoErrorf(t, err, "delete %s on %s", path, branch)
	require.Equalf(t, http.StatusNoContent, res.StatusCode(), "delete %s on %s: %s", path, branch, res.Body)
}

// putObjectThroughGateway writes one object through the S3 gateway, which is the only path that
// stores user metadata (§3.1: keys are X-Amz-Meta-<lowercased name>). The gateway is addressed path
// style - bucket = repository, key = <branch>/<path> - so no DNS is involved.
func (e *mdsEnv) putObjectThroughGateway(t *testing.T, branch, path, content string, userMetadata map[string]string) {
	t.Helper()
	cl := e.s3ClientFor(t, e.key, e.secret)
	_, err := cl.PutObject(e.ctx, e.repo, branch+"/"+path, strings.NewReader(content),
		int64(len(content)), minio.PutObjectOptions{
			ContentType:  mdsObjectContentType,
			UserMetadata: userMetadata,
		})
	require.NoErrorf(t, err, "upload %s through the S3 gateway", path)
}

// s3ClientFor builds an S3 client for the gateway on the lakefs server, authenticated as the given
// principal.
func (e *mdsEnv) s3ClientFor(t *testing.T, key, secret string) *minio.Client {
	t.Helper()
	host := strings.TrimPrefix(strings.TrimPrefix(e.baseURL, "https://"), "http://")
	cl, err := minio.New(host, &minio.Options{
		Creds:        minioCredentials.NewStaticV4(key, secret, ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
		Region:       e.s3.Region,
	})
	require.NoError(t, err, "build the S3 gateway client")
	return cl
}

// ---------------------------------------------------------------------------------------------
// reading the virtual table
// ---------------------------------------------------------------------------------------------

// mdsRow is one row of system.object_metadata as a client reports it (§3.1).
type mdsRow struct {
	Path         string            `json:"path"`
	Repository   string            `json:"repository"`
	CommitID     string            `json:"commit_id"`
	ETag         string            `json:"etag"`
	SizeBytes    int64             `json:"size_bytes"`
	UserMetadata map[string]string `json:"user_metadata"`
	Committer    *string           `json:"committer"`
	ContentType  *string           `json:"content_type"`
	LastModified string            `json:"last_modified"`
	CreationDate *string           `json:"creation_date"`
}

// mdsClientRead is what one engine reports about the table.
type mdsClientRead struct {
	Rows             []mdsRow `json:"rows"`
	Count            int      `json:"count"`
	MetadataLocation string   `json:"metadata_location"`
	SourceCommit     string   `json:"source_commit"`
}

// mdsRead is the result of one read step: the same table through both published engines on both
// catalog mounts of docs/metadata-search.md.
type mdsRead struct {
	// PyIceberg is PyIceberg through /mds/iceberg/api, loading the identifier
	// [repository, ref, system, object_metadata].
	PyIceberg mdsClientRead `json:"pyiceberg"`
	// PyIcebergRelative is PyIceberg through /iceberg/relative_to/{repo}.{ref}/api, loading
	// "system.object_metadata": the mount fixes the repository and the ref.
	PyIcebergRelative mdsClientRead `json:"pyiceberg_relative"`
	// DuckDB is DuckDB through /iceberg/relative_to/{repo}.{ref}/api, from
	// "system"."object_metadata".
	DuckDB mdsClientRead `json:"duckdb"`
	// DuckDBIdentifier is DuckDB through /mds/iceberg/api, from
	// "{repository}.{ref}.system"."object_metadata": the three levels are one schema.
	DuckDBIdentifier mdsClientRead `json:"duckdb_identifier"`
}

// requireAgrees asserts that every engine/mount combination of the guide reads the same table, so
// that a case cannot pass on one form while the other serves something else. It compares the
// columns every engine reports - path, commit_id, etag, size_bytes and user_metadata - and, when
// the step materialises no rows, the count.
func (r mdsRead) requireAgrees(t *testing.T, ref string) {
	t.Helper()
	engines := []struct {
		name string
		read mdsClientRead
	}{
		{"DuckDB through " + icebergcatalog.RelativeToBasePath, r.DuckDB},
		{"PyIceberg through " + icebergcatalog.RelativeToBasePath, r.PyIcebergRelative},
		{"DuckDB through " + icebergcatalog.MetadataSearchBasePath, r.DuckDBIdentifier},
	}
	if len(r.PyIceberg.Rows) == 0 {
		// countRows: the rows are not materialised, so the count is the whole report.
		for _, engine := range engines {
			require.Equalf(t, r.PyIceberg.Count, engine.read.Count,
				"%s must count the same rows of %s as PyIceberg through %s",
				engine.name, ref, icebergcatalog.MetadataSearchBasePath)
		}
		return
	}
	want := mdsRowSignature(r.PyIceberg.Rows)
	for _, engine := range engines {
		require.Equalf(t, want, mdsRowSignature(engine.read.Rows),
			"%s must read the same rows of %s as PyIceberg through %s: both mounts of the catalog "+
				"serve the same table",
			engine.name, ref, icebergcatalog.MetadataSearchBasePath)
	}
	require.Equalf(t, r.PyIceberg.SourceCommit, r.PyIcebergRelative.SourceCommit,
		"PyIceberg must read the same %s property through %s as through %s",
		mdsTableSourceCommit, icebergcatalog.RelativeToBasePath, icebergcatalog.MetadataSearchBasePath)
}

// mdsRowSignature is the part of a row that every engine and every mount reports, in a comparable
// form (sorted by path, so the order a scan returns rows in is not part of the comparison).
func mdsRowSignature(rows []mdsRow) []string {
	signatures := make([]string, 0, len(rows))
	for _, row := range rows {
		keys := make([]string, 0, len(row.UserMetadata))
		for key := range row.UserMetadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		metadata := make([]string, 0, len(keys))
		for _, key := range keys {
			metadata = append(metadata, key+"="+row.UserMetadata[key])
		}
		signatures = append(signatures, fmt.Sprintf("%s|%s|%s|%d|%s",
			row.Path, row.CommitID, row.ETag, row.SizeBytes, strings.Join(metadata, ",")))
	}
	sort.Strings(signatures)
	return signatures
}

// requireNonIncreasingSizes asserts that a DuckDB result is in the ORDER BY size_bytes DESC order
// the guide's filtered snippet asks for.
func requireNonIncreasingSizes(t *testing.T, rows []mdsRow, description string) {
	t.Helper()
	for i := 1; i < len(rows); i++ {
		require.GreaterOrEqualf(t, rows[i-1].SizeBytes, rows[i].SizeBytes,
			"%s must return the filtered rows in size_bytes DESC order: %s is %d, %s is %d",
			description, rows[i-1].Path, rows[i-1].SizeBytes, rows[i].Path, rows[i].SizeBytes)
	}
}

// readRows reads the virtual table of R at ref with both published engines on both published
// mounts: PyIceberg and DuckDB through /mds/iceberg/api (the upstream endpoint of §5.5) and through
// /iceberg/relative_to/{repo}.{ref}/api, the four recipes of docs/metadata-search.md. Every row of
// §3.1 is reported by PyIceberg; DuckDB reports the columns the cases assert on, because that is
// what its published recipe selects. The four reports must agree (requireAgrees).
func (e *mdsEnv) readRows(t *testing.T, ref string) mdsRead {
	t.Helper()
	read := mdsDecode[mdsRead](t, e.runReadStep(t, ref, true, false))
	read.requireAgrees(t, ref)
	return read
}

// readFilteredRows runs the guide's filtered scans - PyIceberg's row_filter expression and DuckDB's
// WHERE ... ORDER BY size_bytes DESC LIMIT - through the same four engine/mount recipes. It
// returns the same result shape as readRows so the four answers are compared the same way.
func (e *mdsEnv) readFilteredRows(t *testing.T, ref string) mdsRead {
	t.Helper()
	result := e.runReadStep(t, ref, true, true)
	filtered, ok := result["filtered"]
	require.Truef(t, ok, "the read step must report the filtered scan: %v", result)
	filteredMap, ok := filtered.(map[string]any)
	require.Truef(t, ok, "the read step must report the filtered scan as an object: %v", filtered)
	read := mdsDecode[mdsRead](t, filteredMap)
	read.requireAgrees(t, ref)
	require.NotEmptyf(t, read.PyIceberg.Rows,
		"the filtered scan of %s must return rows, or the four engines agree only on an empty answer", ref)
	return read
}

// countRows reads the same table without materialising the rows, for a table too large to pass
// through the step's JSON result.
func (e *mdsEnv) countRows(t *testing.T, ref string) mdsRead {
	t.Helper()
	read := mdsDecode[mdsRead](t, e.runReadStep(t, ref, false, false))
	read.requireAgrees(t, ref)
	return read
}

// runReadStep runs the read program in the client container and returns its JSON object.
func (e *mdsEnv) runReadStep(t *testing.T, ref string, withRows, filtered bool) map[string]any {
	t.Helper()
	env := e.readStepEnv(t, ref, withRows)
	env["MDS_FILTERED"] = "0"
	if filtered {
		env["MDS_FILTERED"] = "1"
		env["MDS_FILTER_PREFIX"] = mdsFilteredPathPrefix
	}
	return e.runClient(t, "mds-read:"+ref, mdsReadScript, env)
}

// readStepEnv is the environment of one read step: the table, the ref and the four catalog mounts.
func (e *mdsEnv) readStepEnv(t *testing.T, ref string, withRows bool) map[string]string {
	t.Helper()
	identifier, err := json.Marshal([]string{e.repo, ref, icebergcatalog.SystemNamespace, icebergcatalog.ObjectMetadataTable})
	require.NoError(t, err, "marshal the PyIceberg identifier")
	rows := "1"
	if !withRows {
		rows = "0"
	}
	return map[string]string{
		"MDS_WAREHOUSE":          e.storageNamespace,
		"MDS_IDENTIFIER":         string(identifier),
		"MDS_CREDENTIAL":         e.key + ":" + e.secret,
		"MDS_CATALOG_URI":        e.dockerReachableURL(e.baseURL + icebergcatalog.MetadataSearchBasePath),
		"MDS_RELATIVE_URI":       e.dockerReachableURL(e.baseURL+icebergcatalog.RelativeToBasePath) + "/" + e.repo + "." + ref + "/api",
		"MDS_OAUTH_URI":          e.dockerReachableURL(e.baseURL+icebergcatalog.MetadataSearchBasePath) + "/v1/oauth/tokens",
		"MDS_RELATIVE_OAUTH_URI": e.dockerReachableURL(e.baseURL+icebergcatalog.APIBasePath) + "/v1/oauth/tokens",
		"MDS_RELATIVE_TABLE":     icebergcatalog.SystemNamespace + "." + icebergcatalog.ObjectMetadataTable,
		"MDS_MDS_SCHEMA":         e.repo + "." + ref + "." + icebergcatalog.SystemNamespace,
		"MDS_ROWS":               rows,
	}
}

// loadTableAs loads the virtual table of R at ref as an arbitrary principal and returns the status
// and body: the negative authorization cases are about the status, and the body says which
// exception the catalog chose.
func (e *mdsEnv) loadTableAs(t *testing.T, key, secret, ref string) (int, []byte) {
	t.Helper()
	return e.loadAs(t, key, secret, e.virtualTableURL(ref))
}

// loadMetadataTableAs loads R-metadata's own table at ref as an arbitrary principal. It is the
// control for the "R-metadata only" case: the grant really is there, the virtual table is not.
func (e *mdsEnv) loadMetadataTableAs(t *testing.T, key, secret, ref string) (int, []byte) {
	t.Helper()
	return e.loadAs(t, key, secret, e.tableURLFor(icebergcatalog.MetadataSearchBasePath, e.metadataRepo, ref))
}

// loadAs issues one authenticated loadTable request.
func (e *mdsEnv) loadAs(t *testing.T, key, secret, rawURL string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(e.ctx, http.MethodGet, rawURL, nil)
	require.NoErrorf(t, err, "build GET %s", rawURL)
	req.SetBasicAuth(key, secret)
	resp, err := (&http.Client{Timeout: mdsRequestTimeout}).Do(req)
	require.NoErrorf(t, err, "GET %s", rawURL)
	defer func() { _ = resp.Body.Close() }()
	var body bytes.Buffer
	_, err = body.ReadFrom(resp.Body)
	require.NoErrorf(t, err, "read GET %s", rawURL)
	return resp.StatusCode, body.Bytes()
}

// virtualTableURL is the loadTable URL of R.<ref>.system.object_metadata through the upstream
// endpoint of §5.5.
func (e *mdsEnv) virtualTableURL(ref string) string {
	return e.tableURLFor(icebergcatalog.MetadataSearchBasePath, e.repo, ref)
}

// tableURLFor is the loadTable URL of repo.ref.system.object_metadata below a catalog mount.
func (e *mdsEnv) tableURLFor(mountPath, repo, ref string) string {
	levels := []string{repo, ref, icebergcatalog.SystemNamespace}
	encoded := make([]string, 0, len(levels))
	for _, level := range levels {
		encoded = append(encoded, url.PathEscape(level))
	}
	return e.baseURL + mountPath + "/v1/" + icebergcatalog.CatalogPrefix + "/namespaces/" +
		strings.Join(encoded, icebergcatalog.EncodedNamespaceSeparator) +
		"/tables/" + url.PathEscape(icebergcatalog.ObjectMetadataTable)
}

// systemMount is one of the two ways a client addresses the catalog of a data repository: the
// upstream endpoint /mds/iceberg/api (§5.5), where the client names the repository and the ref in
// the identifier, and /iceberg/relative_to/{repo}.{ref}/api, where its URL fixes them and the
// client's own levels start at the namespace (relative.go). The rules of §5.3 must hold on both.
type systemMount struct {
	// name is how a failure message names the form.
	name string
	// base is the mount the /v1/{prefix}/... paths of the REST spec hang from.
	base string
	// namespace turns the client's own levels into the {namespace} path parameter of this form.
	namespace func(levels ...string) string
	// levels turns the client's own levels into the namespace member of a CreateNamespace body.
	levels func(levels ...string) []string
}

// url is the URL of one catalog operation on this mount: path starts at the operation, i.e. with
// "/namespaces" or "/tables".
func (m systemMount) url(path string) string {
	return m.base + "/v1/" + icebergcatalog.CatalogPrefix + path
}

// systemMounts returns both addressing forms of the catalog that serves the virtual table of a
// repository at ref.
func (e *mdsEnv) systemMounts(ref string) []systemMount {
	encode := func(levels []string) string {
		encoded := make([]string, 0, len(levels))
		for _, level := range levels {
			encoded = append(encoded, url.PathEscape(level))
		}
		return strings.Join(encoded, icebergcatalog.EncodedNamespaceSeparator)
	}
	abstract := func(levels ...string) []string {
		return append([]string{e.repo, ref}, levels...)
	}
	return []systemMount{
		{
			name:      icebergcatalog.MetadataSearchBasePath,
			base:      e.baseURL + icebergcatalog.MetadataSearchBasePath,
			namespace: func(levels ...string) string { return encode(abstract(levels...)) },
			levels:    abstract,
		},
		{
			name: icebergcatalog.RelativeToBasePath + "/" + e.repo + "." + ref + "/api",
			base: e.baseURL + icebergcatalog.RelativeToBasePath + "/" +
				url.PathEscape(e.repo+"."+ref) + "/api",
			namespace: func(levels ...string) string { return encode(levels) },
			levels: func(levels ...string) []string {
				return append([]string(nil), levels...)
			},
		},
	}
}

// catalogAs issues one authenticated catalog request as an arbitrary principal and returns the
// status and body. body is the JSON payload, empty for a request without one.
func (e *mdsEnv) catalogAs(t *testing.T, p mdsPrincipal, method, rawURL, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(e.ctx, method, rawURL, strings.NewReader(body))
	require.NoErrorf(t, err, "%s %s", method, rawURL)
	req.SetBasicAuth(p.key, p.secret)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: mdsRequestTimeout}).Do(req)
	require.NoErrorf(t, err, "%s %s", method, rawURL)
	defer func() { _ = resp.Body.Close() }()
	var out bytes.Buffer
	_, err = out.ReadFrom(resp.Body)
	require.NoErrorf(t, err, "read %s %s", method, rawURL)
	return resp.StatusCode, out.Bytes()
}

// mdsCatalogEnvelope is the Iceberg REST error envelope (the spec's ErrorResponse): every catalog
// refusal names its reason in error.type.
type mdsCatalogEnvelope struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    int    `json:"code"`
	} `json:"error"`
}

// requireMDSReservedNamespace asserts that a catalog write into the system namespace of a data
// repository is refused by the reservation of §5.3: the status and type the design names, answered
// with the message that names the namespace as reserved. A 403 from a permission rule has the same
// status and the same type, and the message is what tells them apart: the positive control of the
// case rules out a missing grant, and this rules out any other 403 in the same envelope.
func requireMDSReservedNamespace(t *testing.T, status int, body []byte, what string) {
	t.Helper()
	requireMDSRefusal(t, status, body, http.StatusForbidden, icebergcatalog.ErrorTypeForbidden, what)
	require.Containsf(t, strings.ToLower(string(body)), "reserved",
		"%s must be refused by the reservation of %s, not by another 403: %s",
		what, icebergcatalog.SystemNamespace, body)
}

// requireMDSRefusal asserts that a catalog answer is the refusal the design names - the status and
// the Iceberg error type - and not a 404, a 409 or a 500 that would pass for one in a green run.
func requireMDSRefusal(t *testing.T, status int, body []byte, wantStatus int, wantType, what string) {
	t.Helper()
	var envelope mdsCatalogEnvelope
	require.NoErrorf(t, json.Unmarshal(body, &envelope),
		"%s must answer the Iceberg error envelope, got %d: %s", what, status, body)
	require.Equalf(t, wantStatus, status, "%s must be refused with %d %s: %s", what, wantStatus, wantType, body)
	require.Equalf(t, wantType, envelope.Error.Type,
		"%s must be refused with %d %s: %s", what, wantStatus, wantType, body)
}

// ---------------------------------------------------------------------------------------------
// the companion repository's own history
// ---------------------------------------------------------------------------------------------

// metadataLog returns the first-parent log of R-metadata@branch, newest first (§3.3). It is how the
// suite reads the indexing position and the seeding of a new branch (§4.5) from the companion
// repository itself, instead of inferring them from rows a backfill writes the same way.
func (e *mdsEnv) metadataLog(t *testing.T, branch string) []apigen.Commit {
	t.Helper()
	firstParent := true
	amount := apigen.PaginationAmount(100)
	after := ""
	var commits []apigen.Commit
	for {
		params := &apigen.LogCommitsParams{FirstParent: &firstParent, Amount: &amount}
		if after != "" {
			cursor := apigen.PaginationAfter(after)
			params.After = &cursor
		}
		res, err := e.admin.LogCommitsWithResponse(t.Context(), e.metadataRepo, branch, params)
		require.NoErrorf(t, err, "log commits of %s@%s", e.metadataRepo, branch)
		require.Equalf(t, http.StatusOK, res.StatusCode(),
			"log commits of %s@%s: %s", e.metadataRepo, branch, res.Body)
		require.NotNilf(t, res.JSON200, "log commits of %s@%s: %s", e.metadataRepo, branch, res.Body)
		commits = append(commits, res.JSON200.Results...)
		if !res.JSON200.Pagination.HasMore {
			return commits
		}
		require.NotEmptyf(t, res.JSON200.Pagination.NextOffset,
			"log commits of %s@%s has more results without a cursor", e.metadataRepo, branch)
		after = res.JSON200.Pagination.NextOffset
	}
}

// metadataCommitTagged returns the commit of R-metadata that tag mds-<sourceCommit> names (§3.3,
// §4.3 step 5): the commit that recorded sourceCommit of R.
func (e *mdsEnv) metadataCommitTagged(t *testing.T, sourceCommit string) string {
	t.Helper()
	tag := indexer.TagPrefix + sourceCommit
	res, err := e.admin.GetTagWithResponse(t.Context(), e.metadataRepo, tag)
	require.NoErrorf(t, err, "get tag %s of %s", tag, e.metadataRepo)
	require.Equalf(t, http.StatusOK, res.StatusCode(),
		"the indexer must tag %s with %s once it has indexed %s: %s", sourceCommit, tag, sourceCommit, res.Body)
	require.NotNilf(t, res.JSON200, "get tag %s of %s: %s", tag, e.metadataRepo, res.Body)
	return res.JSON200.CommitId
}

// metadataBranchStatus returns the status of GET branch of R-metadata, for the lifecycle cases of
// §4.8 that assert a mirrored branch exists or is gone.
func (e *mdsEnv) metadataBranchStatus(t *testing.T, branch string) int {
	t.Helper()
	res, err := e.admin.GetBranchWithResponse(t.Context(), e.metadataRepo, branch)
	require.NoErrorf(t, err, "get branch %s of %s", branch, e.metadataRepo)
	return res.StatusCode()
}

// waitMetadataBranchGone waits, bounded, until the deletion of a data branch has removed its
// R-metadata branch (§4.8). The data branch itself is already gone; this is the indexer's mirror.
func (e *mdsEnv) waitMetadataBranchGone(t *testing.T, branch string) {
	t.Helper()
	deadline := time.Now().Add(mdsIndexTimeout)
	for {
		e.exitedWhileRunning(t, e.indexer, "waiting for "+e.metadataRepo+"@"+branch+" to be deleted")
		status := e.metadataBranchStatus(t, branch)
		if status == http.StatusNotFound {
			t.Logf("%s@%s was deleted after the data branch was deleted (§4.8)", e.metadataRepo, branch)
			return
		}
		require.Equalf(t, http.StatusOK, status,
			"getting %s@%s answered %d, not 200 while it exists or 404 after the §4.8 deletion",
			e.metadataRepo, branch, status)
		if time.Now().After(deadline) {
			t.Fatalf("%s@%s still exists %s after the data branch was deleted; last %d lines of the indexer log:\n%s",
				e.metadataRepo, branch, mdsIndexTimeout, mdsLogTailLines, mdsLogTail(e.indexPath, mdsLogTailLines))
		}
		time.Sleep(mdsPollInterval)
	}
}

// waitTagGone waits, bounded, until tag_retention has removed a tag of R-metadata (§4.9).
func (e *mdsEnv) waitTagGone(t *testing.T, tag string) {
	t.Helper()
	deadline := time.Now().Add(mdsIndexTimeout)
	for {
		e.exitedWhileRunning(t, e.indexer, "waiting for tag "+tag+" to be retired")
		res, err := e.admin.GetTagWithResponse(t.Context(), e.metadataRepo, tag)
		require.NoErrorf(t, err, "get tag %s of %s", tag, e.metadataRepo)
		switch res.StatusCode() {
		case http.StatusNotFound:
			t.Logf("tag_retention retired %s", tag)
			return
		case http.StatusOK:
		default:
			t.Fatalf("getting tag %s of %s answered %d: %s", tag, e.metadataRepo, res.StatusCode(), res.Body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("tag %s of %s was not retired within %s; last %d lines of the indexer log:\n%s",
				tag, e.metadataRepo, mdsIndexTimeout, mdsLogTailLines, mdsLogTail(e.indexPath, mdsLogTailLines))
		}
		time.Sleep(mdsPollInterval)
	}
}

// mdsSourceCommitOf returns the commit of R a commit of R-metadata recorded, or "" on a commit that
// carries no position (the automatic root and the initialisation commit of §4.8).
func mdsSourceCommitOf(commit apigen.Commit) string {
	if commit.Metadata == nil {
		return ""
	}
	return commit.Metadata.AdditionalProperties[indexer.MetadataKeySourceCommit]
}

// mdsCommitIDs returns the ids of a commit log, in order.
func mdsCommitIDs(commits []apigen.Commit) []string {
	ids := make([]string, 0, len(commits))
	for _, commit := range commits {
		ids = append(ids, commit.Id)
	}
	return ids
}

// mdsCommitLog renders a commit log for a failure message: one "<short id> <message>" line per
// commit, newest first.
func mdsCommitLog(commits []apigen.Commit) []string {
	lines := make([]string, 0, len(commits))
	for _, commit := range commits {
		id := commit.Id
		if len(id) > 8 {
			id = id[:8]
		}
		lines = append(lines, fmt.Sprintf("%s %s", id, commit.Message))
	}
	return lines
}

// mdsPaths returns the sorted paths of a read's rows.
func mdsPaths(rows []mdsRow) []string {
	paths := make([]string, 0, len(rows))
	for _, row := range rows {
		paths = append(paths, row.Path)
	}
	sort.Strings(paths)
	return paths
}

// mdsRowOf returns the row of path, failing the test when it is not there.
func mdsRowOf(t *testing.T, rows []mdsRow, path string) mdsRow {
	t.Helper()
	for _, row := range rows {
		if row.Path == path {
			return row
		}
	}
	t.Fatalf("the table has no row for %q; rows: %v", path, mdsPaths(rows))
	return mdsRow{}
}

// mdsDecode re-encodes a client step's JSON object into a typed result.
func mdsDecode[T any](t *testing.T, result map[string]any) T {
	t.Helper()
	encoded, err := json.Marshal(result)
	require.NoError(t, err, "re-encode the client step's result")
	var typed T
	require.NoErrorf(t, json.Unmarshal(encoded, &typed), "decode the client step's result: %s", encoded)
	return typed
}

// ---------------------------------------------------------------------------------------------
// indexing progress
// ---------------------------------------------------------------------------------------------

// mdsHealth is the part of the indexer's /_health report (§9) this suite reads.
type mdsHealth struct {
	Healthy      bool `json:"healthy"`
	Repositories map[string]struct {
		InProgress bool   `json:"in_progress"`
		LastError  string `json:"last_error"`
		Branches   map[string]struct {
			LastIndexed string `json:"last_indexed"`
			Head        string `json:"head"`
			LagCommits  int64  `json:"lag_commits"`
			LastError   string `json:"last_error"`
		} `json:"branches"`
	} `json:"repositories"`
}

// indexerHealth fetches the indexer's report; ok is false while the server does not answer yet.
func (e *mdsEnv) indexerHealth(t *testing.T) (mdsHealth, bool) {
	t.Helper()
	client := &http.Client{Timeout: icebergHealthTimeout}
	resp, err := client.Get(e.indexerURL + "/_health")
	if err != nil {
		return mdsHealth{}, false
	}
	defer func() { _ = resp.Body.Close() }()
	var report mdsHealth
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		return mdsHealth{}, false
	}
	return report, true
}

// indexerMetric returns one metric line of the indexer's /metrics endpoint of §9, or "unavailable"
// when the process does not serve it: the measurement of the backfill is reported, not gated, so a
// missing metric must not fail the run.
func (e *mdsEnv) indexerMetric(t *testing.T, name string) string {
	t.Helper()
	client := &http.Client{Timeout: mdsRequestTimeout}
	resp, err := client.Get(e.indexerURL + "/metrics")
	if err != nil {
		t.Logf("read %s from the indexer: %v", e.indexerURL+"/metrics", err)
		return "unavailable"
	}
	defer func() { _ = resp.Body.Close() }()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			return line
		}
	}
	if err := scanner.Err(); err != nil {
		t.Logf("scan %s: %v", e.indexerURL+"/metrics", err)
	}
	return "unavailable"
}

// waitForIndexerServer waits until the indexer's own HTTP server answers (§9: /_health, 200 while
// every repository is healthy and 503 as soon as one carries an error - both mean it is serving).
// An indexer that exits while it is starting fails the test at once with its exit status.
func (e *mdsEnv) waitForIndexerServer(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(mdsIndexerStartTimeout)
	for {
		e.exitedWhileRunning(t, e.indexer, "starting")
		if _, ok := e.indexerHealth(t); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the Metadata Search indexer did not serve %s/_health within %s; last %d lines of its log:\n%s",
				e.indexerURL, mdsIndexerStartTimeout, mdsLogTailLines, mdsLogTail(e.indexPath, mdsLogTailLines))
		}
		select {
		case <-e.indexer.done:
			// The next iteration reports the exit with its status and log tail.
		case <-time.After(icebergHealthInterval):
		}
	}
}

// waitIndexed waits until the index of branch is at wantHead with no error, and reports how long
// that took. A reported error fails the test with the report and the indexer log: a failing tick is
// the product behaviour the case is about, not something to wait out.
func (e *mdsEnv) waitIndexed(t *testing.T, branch, wantHead string) {
	t.Helper()
	e.waitIndexedWithin(t, branch, wantHead, mdsIndexTimeout)
}

// waitIndexedWithin is waitIndexed with an explicit timeout.
func (e *mdsEnv) waitIndexedWithin(t *testing.T, branch, wantHead string, timeout time.Duration) {
	t.Helper()
	start := time.Now()
	deadline := start.Add(timeout)
	var last mdsHealth
	for {
		e.exitedWhileRunning(t, e.indexer, "waiting for the index to reach "+wantHead)
		report, ok := e.indexerHealth(t)
		if ok {
			last = report
			repo, found := report.Repositories[e.repo]
			if found {
				require.Emptyf(t, repo.LastError, "the indexer failed on %s@%s: %s",
					e.repo, branch, repo.LastError)
				if state, found := repo.Branches[branch]; found {
					require.Emptyf(t, state.LastError, "the indexer failed on %s@%s: %s",
						e.repo, branch, state.LastError)
					if state.LastIndexed == wantHead && state.LagCommits == 0 {
						t.Logf("indexed %s@%s after %s", e.repo, branch, time.Since(start).Round(time.Millisecond))
						return
					}
				}
			}
		}
		if time.Now().After(deadline) {
			encoded, _ := json.Marshal(last)
			t.Fatalf("the index of %s@%s did not reach %s within %s; last health report: %s\nlast %d lines of the indexer log:\n%s",
				e.repo, branch, wantHead, timeout, encoded, mdsLogTailLines, mdsLogTail(e.indexPath, mdsLogTailLines))
		}
		select {
		case <-e.indexer.done:
			// The next iteration reports the exit with its status and log tail.
		case <-time.After(mdsPollInterval):
		}
	}
}

// ---------------------------------------------------------------------------------------------
// the 100k-object backfill
// ---------------------------------------------------------------------------------------------

// mdsBackfillObjectCount is the number of objects the backfill measurement indexes: the design's
// 100k, or the value of ESTI_MDS_BACKFILL_OBJECTS for a development run.
func mdsBackfillObjectCount() int {
	if raw := os.Getenv(mdsBackfillObjectsEnv); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			return value
		}
	}
	return mdsBackfillObjectsDefault
}

// buildBackfillCommit creates one commit of the branch with count objects and returns its ID.
// The object store work is what the two setups trade: the shared setup writes one object and
// repeats it in the commit, the prefix setup writes every object and imports the prefixes.
//
// The shared setup is the default because the prefix setup does not fit the test budget at 100k:
// measured on the local SeaweedFS recipe, its 100k upload alone took 3m23s and the run was killed by
// the 10m test timeout while the import was still running. The shared setup committed the same
// 100k objects in 4m26s (26ms for the one source object, the rest the import). The indexer's timed
// work is the same either way: it reads the commit's entries, never the object payloads.
func (e *mdsEnv) buildBackfillCommit(t *testing.T, count int) string {
	t.Helper()
	setup := os.Getenv(mdsBackfillSetupEnv)
	if setup == "" {
		setup = mdsBackfillSetupShared
	}
	if setup == mdsBackfillSetupPrefix {
		e.writeBackfillObjects(t, count)
		return e.importPrefix(t, count)
	}
	source := e.writeBackfillSource(t)
	return e.importSharedObject(t, count, source)
}

// writeBackfillSource writes the one object the shared setup imports count times. The indexer
// never reads object data, only the entries of the commit, so a commit whose paths all point at
// the same payload is exactly as large as one with count distinct payloads for the backfill. The
// test names the sharing in its log; the import below is a normal lakeFS import of object paths.
func (e *mdsEnv) writeBackfillSource(t *testing.T) string {
	t.Helper()
	cl := e.s3Client(t)
	key := e.importSourcePrefix() + "shared/object.txt"
	content := []byte("metadata search backfill object\n")

	start := time.Now()
	ctx, cancel := context.WithTimeout(e.ctx, mdsRequestTimeout)
	defer cancel()
	_, err := cl.PutObject(ctx, e.bucket, key, bytes.NewReader(content), int64(len(content)),
		minio.PutObjectOptions{ContentType: mdsObjectContentType})
	require.NoErrorf(t, err, "write the backfill source object s3://%s/%s", e.bucket, key)
	t.Logf("backfill setup: wrote one source object s3://%s/%s in %s", e.bucket, key,
		time.Since(start).Round(time.Millisecond))
	return "s3://" + e.bucket + "/" + key
}

// writeBackfillObjects writes count small objects straight into the bucket below the repository's
// storage namespace, outside any commit. The import step is what puts them into one commit; writing
// them through the API instead would make the setup, not the backfill, the subject of the timing.
func (e *mdsEnv) writeBackfillObjects(t *testing.T, count int) {
	t.Helper()
	cl := e.s3Client(t)
	prefix := e.importSourcePrefix()
	content := []byte("metadata search backfill object\n")

	// Record the keys as they are written, so the teardown sweeps them without listing the prefix.
	e.backfillObjects = count
	e.backfillPrefix = prefix

	ctx, cancel := context.WithTimeout(e.ctx, mdsBucketOperationTimeout)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	semaphore := make(chan struct{}, mdsBackfillConcurrency)
	progressEvery := count / 10
	start := time.Now()
	for i := range count {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-semaphore }()
			key := mdsBackfillKey(prefix, count, i)
			_, err := cl.PutObject(ctx, e.bucket, key, bytes.NewReader(content), int64(len(content)),
				minio.PutObjectOptions{ContentType: mdsObjectContentType})
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
			if progressEvery > 0 && i > 0 && i%progressEvery == 0 {
				t.Logf("backfill setup: wrote %d/%d objects", i, count)
			}
		}(i)
	}
	wg.Wait()
	require.NoErrorf(t, firstErr, "write the backfill objects to s3://%s/%s", e.bucket, prefix)
	t.Logf("backfill setup: wrote %d objects directly to the bucket in %s",
		count, time.Since(start).Round(time.Millisecond))
}

// deleteRunPrefix removes everything this run wrote below the allocated prefix, many streams at a
// time. It is the mdsEnv override of the Iceberg environment's single-stream sweep.
//
// The 100k-object backfill leaves a large prefix, and the test time budget leaves no room for two
// costs of the naive sweep: listing a 100k-key prefix (the walker pages are not free on a
// single-node store) and draining it on one RemoveObjects call (four internal workers). The keys
// the backfill wrote are known, so they are removed without a listing; only the handful of objects
// left (the repositories' table files) is listed and swept.
func (e *mdsEnv) deleteRunPrefix(ctx context.Context) error {
	lookup := minio.BucketLookupAuto
	if e.s3.PathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(e.s3.Host, &minio.Options{
		Creds:        minioCredentials.NewStaticV4(e.s3.AccessKey, e.s3.SecretKey, ""),
		Secure:       e.s3.Secure,
		Region:       e.s3.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return fmt.Errorf("build the S3 client: %w", err)
	}

	if err := e.sweep(ctx, client, e.knownBackfillKeys()); err != nil {
		return err
	}
	return e.sweep(ctx, client, client.ListObjects(ctx, e.bucket, minio.ListObjectsOptions{
		Prefix:    e.runPrefix,
		Recursive: true,
	}))
}

// knownBackfillKeys enumerates the objects writeBackfillObjects wrote, in the same order, so the
// sweep can remove them without listing the prefix.
func (e *mdsEnv) knownBackfillKeys() <-chan minio.ObjectInfo {
	keys := make(chan minio.ObjectInfo, 1000)
	if e.backfillObjects == 0 {
		close(keys)
		return keys
	}
	go func() {
		defer close(keys)
		for i := range e.backfillObjects {
			keys <- minio.ObjectInfo{Key: mdsBackfillKey(e.backfillPrefix, e.backfillObjects, i)}
		}
	}()
	return keys
}

// mdsBackfillPartSize is the number of objects in one source prefix of the backfill, and
// mdsBackfillKey the bucket key of one object. Both are deterministic, so the writer, the importer
// and the teardown agree on the key set without a listing.
func mdsBackfillPartSize(count int) int {
	partSize := (count + mdsBackfillParts - 1) / mdsBackfillParts
	if partSize < 1 {
		partSize = 1
	}
	return partSize
}

func mdsBackfillKey(prefix string, count, index int) string {
	partSize := mdsBackfillPartSize(count)
	return fmt.Sprintf("%spart-%03d/obj-%06d.txt", prefix, index/partSize, index%partSize)
}

// sweep removes every object of objects with mdsCleanupStreams parallel RemoveObjects calls, and
// reports the first error. A cancelled context stops the sweep early.
func (e *mdsEnv) sweep(ctx context.Context, client *minio.Client, objects <-chan minio.ObjectInfo) error {
	streams := make([]chan minio.ObjectInfo, mdsCleanupStreams)
	for i := range streams {
		streams[i] = make(chan minio.ObjectInfo, 1000)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		failure error
	)
	for i := range streams {
		wg.Add(1)
		go func(stream <-chan minio.ObjectInfo) {
			defer wg.Done()
			for removed := range client.RemoveObjects(ctx, e.bucket, stream, minio.RemoveObjectsOptions{}) {
				if removed.Err == nil {
					continue
				}
				mu.Lock()
				if failure == nil {
					failure = removed.Err
				}
				mu.Unlock()
			}
		}(streams[i])
	}

	index := 0
distribute:
	for object := range objects {
		if object.Err != nil {
			mu.Lock()
			if failure == nil {
				failure = object.Err
			}
			mu.Unlock()
			break
		}
		select {
		case streams[index%len(streams)] <- object:
			index++
		case <-ctx.Done():
			break distribute
		}
	}
	for _, stream := range streams {
		close(stream)
	}
	wg.Wait()

	if failure != nil {
		return fmt.Errorf("remove objects under %q: %w", e.runPrefix, failure)
	}
	return ctx.Err()
}

// importSourcePrefix is the key prefix below the run prefix the backfill objects are written to.
// It is deliberately outside the repository's storage namespace: lakeFS refuses an import whose
// source is inside the target repository's namespace ("invalid import source"), because such an
// object is already addressable there.
func (e *mdsEnv) importSourcePrefix() string {
	return strings.Trim(e.runPrefix, "/") + "/import-source/"
}

// importSharedObject imports one source object into count destinations and returns the commit it
// created. Importing the same object under different paths is a normal lakeFS object import: the
// commit has count entries, and the indexer reads those entries, not the payload (§4.3, §4.5 step
// 2). Setup therefore costs one bucket PUT and no per-object bucket write.
func (e *mdsEnv) importSharedObject(t *testing.T, count int, source string) string {
	t.Helper()
	paths := make([]apigen.ImportLocation, count)
	for i := range count {
		paths[i] = apigen.ImportLocation{
			Type:        "object",
			Path:        source,
			Destination: "data/" + mdsBackfillKey("", count, i),
		}
	}
	return e.importLocations(t, paths, fmt.Sprintf("%d object paths of %s", count, source))
}

// importPrefix imports the count objects the prefix setup wrote into one commit of the branch and
// returns the commit it created.
//
// The objects were written into mdsBackfillParts source prefixes, and the import declares one
// common_prefix per part: lakeFS walks each source prefix on its own, so the walk runs in the
// import work pool instead of as one pass over 100k keys.
func (e *mdsEnv) importPrefix(t *testing.T, count int) string {
	t.Helper()
	partSize := mdsBackfillPartSize(count)
	parts := (count + partSize - 1) / partSize
	paths := make([]apigen.ImportLocation, 0, parts)
	firstSource := "s3://" + e.bucket + "/" + e.importSourcePrefix() + "part-000/"
	for part := range parts {
		paths = append(paths, apigen.ImportLocation{
			Type:        "common_prefix",
			Path:        fmt.Sprintf("s3://%s/%spart-%03d/", e.bucket, e.importSourcePrefix(), part),
			Destination: fmt.Sprintf("data/part-%03d/", part),
		})
	}
	return e.importLocations(t, paths, fmt.Sprintf("%d source prefixes below %s", parts, firstSource))
}

// importLocations starts one import, waits for it to complete and returns its commit. Every wait
// is bounded by mdsBucketOperationTimeout; a timeout reports the server log tail.
func (e *mdsEnv) importLocations(t *testing.T, paths []apigen.ImportLocation, description string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, mdsBucketOperationTimeout)
	defer cancel()

	start := time.Now()
	started, err := e.admin.ImportStartWithResponse(ctx, e.repo, mdsBranch,
		apigen.ImportStartJSONRequestBody{
			Paths:  paths,
			Commit: apigen.CommitCreation{Message: fmt.Sprintf("mds test: import %s", description)},
		})
	if err != nil {
		t.Fatalf("start the import of %s: %v\nserver log tail:\n%s",
			description, err, mdsLogTail(e.logPath, mdsLogTailLines))
	}
	require.Equalf(t, http.StatusAccepted, started.StatusCode(),
		"start the import of %s: %s\nserver log tail:\n%s",
		description, started.Body, mdsLogTail(e.logPath, mdsLogTailLines))
	require.NotNilf(t, started.JSON202, "start the import of %s: %s", description, started.Body)

	id := started.JSON202.Id
	for {
		e.exitedWhileRunning(t, e.server, "waiting for the import to complete")
		status, err := e.admin.ImportStatusWithResponse(ctx, e.repo, mdsBranch,
			&apigen.ImportStatusParams{Id: id})
		if err != nil {
			t.Fatalf("import status %s: %v\nserver log tail:\n%s",
				id, err, mdsLogTail(e.logPath, mdsLogTailLines))
		}
		require.Equalf(t, http.StatusOK, status.StatusCode(),
			"import status %s: %s\nserver log tail:\n%s",
			id, status.Body, mdsLogTail(e.logPath, mdsLogTailLines))
		require.NotNilf(t, status.JSON200, "import status %s: %s", id, status.Body)
		if status.JSON200.Error != nil {
			t.Fatalf("import %s failed: %+v\nserver log tail:\n%s",
				id, status.JSON200.Error, mdsLogTail(e.logPath, mdsLogTailLines))
		}
		if status.JSON200.Completed {
			require.NotNilf(t, status.JSON200.Commit, "import %s completed without a commit", id)
			t.Logf("backfill setup: imported %s into %s@%s in %s",
				description, e.repo, status.JSON200.Commit.Id, time.Since(start).Round(time.Millisecond))
			return status.JSON200.Commit.Id
		}
		select {
		case <-ctx.Done():
			t.Fatalf("import %s (%s) did not complete within %s: %v\nserver log tail:\n%s",
				id, description, mdsBucketOperationTimeout, ctx.Err(),
				mdsLogTail(e.logPath, mdsLogTailLines))
		case <-time.After(mdsPollInterval):
		}
	}
}

// ---------------------------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------------------------

// mdsFreePorts allocates count distinct free ports. icebergFreePort returns one at a time, and two
// consecutive calls can return the same number: the kernel is free to reuse a port as soon as the
// probe listener is closed.
func mdsFreePorts(t *testing.T, count int) []int {
	t.Helper()
	seen := make(map[int]struct{}, count)
	ports := make([]int, 0, count)
	for len(ports) < count {
		port := icebergFreePort(t)
		if _, ok := seen[port]; ok {
			continue
		}
		seen[port] = struct{}{}
		ports = append(ports, port)
	}
	return ports
}

// ---------------------------------------------------------------------------------------------
// the client program
// ---------------------------------------------------------------------------------------------

// mdsReadScript reads R.<ref>.system.object_metadata with both published engines on both published
// mounts, exactly as docs/metadata-search.md publishes them, and reports the four results and the
// freshness property (§9).
//
// PyIceberg, identifier form: /mds/iceberg/api, the upstream endpoint of §5.5. The RestCatalog
// prepends the prefix from GET /v1/config, so the identifier is
// [repository, ref, system, object_metadata].
//
// PyIceberg, warehouse form: /iceberg/relative_to/{repository}.{ref}/api, where the URL fixes both
// the repository and the ref, so `load_table("system.object_metadata")` names the table alone.
//
// DuckDB, warehouse form: /iceberg/relative_to/{repository}.{ref}/api, the relation
// "system"."object_metadata".
//
// DuckDB, identifier form: /mds/iceberg/api, where the repository and the ref stay in the
// identifier, so the schema has three levels and DuckDB spells them as one quoted schema:
// "{repository}.{ref}.system"."object_metadata". Its OAuth server is the /mds one.
//
// The four reports must agree: mdsRead.requireAgrees compares them.
//
// MDS_ROWS=0 reports counts only, which is what the 100k-object case needs: a hundred thousand rows
// of JSON would not survive a docker exec's output.
const mdsReadScript = `
import json
import os

import duckdb
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import GreaterThan, StartsWith


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def sql_identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def metadata_map(value):
    # PyArrow reports an Iceberg map column as a list of (key, value) pairs, while older
    # versions (and DuckDB) report a dict. Both shapes mean the same map (design 3.1).
    if not value:
        return {}
    if isinstance(value, dict):
        items = value.items()
    else:
        items = value
    return {str(key): str(item) for key, item in items}


with_rows = os.environ.get("MDS_ROWS", "1") != "0"
filtered = os.environ.get("MDS_FILTERED", "0") != "0"

s3_properties = {
    "s3.access-key-id": required("ICEBERG_S3_KEY"),
    "s3.secret-access-key": required("ICEBERG_S3_SECRET"),
    "s3.region": os.environ.get("ICEBERG_S3_REGION", "us-east-1"),
}
if os.environ.get("ICEBERG_S3_ENDPOINT"):
    s3_properties["s3.endpoint"] = os.environ["ICEBERG_S3_ENDPOINT"]
    s3_properties["s3.path-style-access"] = os.environ.get("ICEBERG_S3_PATH_STYLE", "true")


def pyiceberg_read(uri, identifier, row_filter=None):
    catalog = RestCatalog(
        name="lakefs",
        uri=uri,
        credential=required("MDS_CREDENTIAL"),
        warehouse=required("MDS_WAREHOUSE"),
        **s3_properties,
    )
    table = catalog.load_table(identifier)
    if row_filter is None:
        scanned = table.scan().to_arrow()
    else:
        scanned = table.scan(row_filter=row_filter).to_arrow()
    rows = []
    if with_rows:
        for row in scanned.to_pylist():
            rows.append({
                "path": row["path"],
                "repository": row["repository"],
                "commit_id": row["commit_id"],
                "etag": row["etag"],
                "size_bytes": row["size_bytes"],
                "user_metadata": metadata_map(row["user_metadata"]),
                "committer": row["committer"],
                "content_type": row["content_type"],
                "last_modified": str(row["last_modified"]),
                "creation_date": str(row["creation_date"]),
            })
    return {
        "rows": rows,
        "count": len(rows) if with_rows else scanned.num_rows,
        "metadata_location": table.metadata_location,
        "source_commit": table.properties.get("lakefs.mds.source-commit", ""),
    }


def duckdb_iceberg_connection(oauth_uri):
    # A connection per mount: the catalog secret names the OAuth server of the mount the ATTACH
    # below uses, and a fresh duckdb.connect() keeps the two secrets and ATTACHes apart.
    con = duckdb.connect()
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
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
        "CREATE OR REPLACE SECRET lakefs_catalog (TYPE iceberg, CLIENT_ID %s, "
        "CLIENT_SECRET %s, OAUTH2_SERVER_URI %s)" % (
            sql_literal(required("MDS_CREDENTIAL").split(":", 1)[0]),
            sql_literal(required("MDS_CREDENTIAL").split(":", 1)[1]),
            sql_literal(oauth_uri),
        )
    )
    return con


def duckdb_rows(con, query, columns):
    rows = []
    count = 0
    for record in con.execute(query).fetchall():
        if with_rows:
            row = {}
            for index, column in enumerate(columns):
                value = record[index]
                if column == "user_metadata":
                    value = metadata_map(value)
                row[column] = value
            rows.append(row)
        else:
            count = record[0]
    return {"rows": rows, "count": len(rows) if with_rows else count}


# --- PyIceberg, through /mds/iceberg/api (the upstream endpoint of design §5.5) ---------------
pyiceberg = pyiceberg_read(required("MDS_CATALOG_URI"), tuple(json.loads(required("MDS_IDENTIFIER"))))

# --- PyIceberg, through /iceberg/relative_to/{repo}.{ref}/api (the warehouse form) ------------
pyiceberg_relative = pyiceberg_read(required("MDS_RELATIVE_URI"), required("MDS_RELATIVE_TABLE"))

# --- DuckDB, through /iceberg/relative_to/{repo}.{ref}/api ------------------------------------
relation = required("MDS_RELATIVE_TABLE")
duckdb_columns = ["path", "etag", "commit_id", "size_bytes", "user_metadata"]
con = duckdb_iceberg_connection(required("MDS_RELATIVE_OAUTH_URI"))
con.execute(
    "ATTACH %s AS lakefs (TYPE iceberg, ENDPOINT %s)" % (
        sql_literal(required("MDS_WAREHOUSE")),
        sql_literal(required("MDS_RELATIVE_URI")),
    )
)
duckdb_query = 'SELECT %s FROM lakefs."system"."object_metadata"' % ", ".join(duckdb_columns)
if not with_rows:
    duckdb_query = 'SELECT count(*) FROM lakefs."system"."object_metadata"'
duckdb_relative = duckdb_rows(con, duckdb_query, duckdb_columns)

# --- DuckDB, through /mds/iceberg/api (the identifier form: one schema, three levels) ---------
# The guide's note for this form: the OAuth token URI is the /mds one, and the repository and the
# ref stay in the identifier, so the schema is the single quoted name
# "{repository}.{ref}.system" and the table stands on its own.
mds_schema = sql_identifier(required("MDS_MDS_SCHEMA"))
con_mds = duckdb_iceberg_connection(required("MDS_OAUTH_URI"))
con_mds.execute(
    "ATTACH %s AS lakefs_mds (TYPE iceberg, ENDPOINT %s)" % (
        sql_literal(required("MDS_WAREHOUSE")),
        sql_literal(required("MDS_CATALOG_URI")),
    )
)
duckdb_mds_query = 'SELECT %s FROM lakefs_mds.%s."object_metadata"' % (
    ", ".join(duckdb_columns), mds_schema,
)
if not with_rows:
    duckdb_mds_query = 'SELECT count(*) FROM lakefs_mds.%s."object_metadata"' % mds_schema
duckdb_identifier = duckdb_rows(con_mds, duckdb_mds_query, duckdb_columns)

# --- the guide's filtered scans ---------------------------------------------------------------
# PyIceberg takes the expression as a row_filter; DuckDB spells the same predicate in SQL and adds
# the guide's ORDER BY size_bytes DESC LIMIT 20. Both are pushed down to the catalog.
result = {
    "pyiceberg": pyiceberg,
    "pyiceberg_relative": pyiceberg_relative,
    "duckdb": duckdb_relative,
    "duckdb_identifier": duckdb_identifier,
}
if filtered:
    prefix = required("MDS_FILTER_PREFIX")
    expression = GreaterThan("size_bytes", 0) & StartsWith("path", prefix)
    filtered_pyiceberg = pyiceberg_read(required("MDS_CATALOG_URI"),
                                         tuple(json.loads(required("MDS_IDENTIFIER"))), expression)
    filtered_pyiceberg_relative = pyiceberg_read(required("MDS_RELATIVE_URI"),
                                                  required("MDS_RELATIVE_TABLE"), expression)
    like = sql_literal(prefix + "%")
    filtered_duckdb_query = (
        'SELECT %s FROM lakefs."system"."object_metadata" '
        'WHERE path LIKE %s AND size_bytes > 0 ORDER BY size_bytes DESC LIMIT 20'
    ) % (", ".join(duckdb_columns), like)
    filtered_duckdb_query_mds = (
        'SELECT %s FROM lakefs_mds.%s."object_metadata" '
        'WHERE path LIKE %s AND size_bytes > 0 ORDER BY size_bytes DESC LIMIT 20'
    ) % (", ".join(duckdb_columns), mds_schema, like)
    result["filtered"] = {
        "pyiceberg": filtered_pyiceberg,
        "pyiceberg_relative": filtered_pyiceberg_relative,
        "duckdb": duckdb_rows(con, filtered_duckdb_query, duckdb_columns),
        "duckdb_identifier": duckdb_rows(con_mds, filtered_duckdb_query_mds, duckdb_columns),
    }

print("RESULT:" + json.dumps(result))
`
