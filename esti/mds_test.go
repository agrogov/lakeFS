// Package esti holds the lakeFS system tests.
//
// mds_test.go is the Metadata Search end-to-end suite of METADATA_SEARCH_DESIGN.md §9 (task M6 of
// METADATA_SEARCH_PLAN.md): the security tests of the design, plus the object-lifecycle cases the
// index has to get right, run through the two published query engines - PyIceberg through the
// upstream endpoint /mds/iceberg/api and DuckDB through /iceberg/relative_to/{repo}.{ref}/api - and
// the timing of a 100k-object backfill.
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
//	new branch               the branch is seeded from the indexed state (§4.5)
//	merge                    the object a merge brings in gets the merge commit's id (§3.2)
//	tag, short commit id     both refs read the snapshot the index recorded for that commit (§6)
//	hard reset               the branch is rewritten to the indexed ancestor (§4.7)
//	indexer restart          a kill mid-tick and a restart converge on the branch head (§4.4)
//	user without R access    403 on the virtual table
//	user with R-metadata only  200 on R-metadata's own table, 403 through R.<ref>.system.object_metadata
//	non-indexer write        admin cannot write R-metadata (the deny of §8 is in force)
//
// TestMetadataSearchBackfill pushes a 100k-object commit into R and times the first indexing of the
// branch - the full-listing rebuild of §4.5 step 2 - reporting the number. It is a measurement, not
// a gate: the assertions are about the rows that come out, and the duration is logged (and quoted
// in the commit that added this file).
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
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/mds/bootstrap"
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

	// indexerKey and indexerSecret are the credentials of mds-indexer, created by this suite
	// through the auth API after the server's bootstrap created the user (§8).
	indexerKey    string
	indexerSecret string
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

	t.Run("new branch is seeded", func(t *testing.T) {
		env.createBranch(t, cl, mdsFeatureBranch, mdsBranch)
		require.Equal(t, afterGateway, env.headCommit(t, cl, mdsFeatureBranch))
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
		// A restart must be harmless: the position is a lakeFS commit, so the next process resumes
		// there (§4.4). The kill is attempted while a tick is in progress - a crash window the
		// unit tests of M4 pin exactly; here the observable outcome is convergence, not recovery.
		env.uploadObject(t, mdsBranch, "data/before-restart.txt", "before")
		env.commitBranch(t, cl, mdsBranch, "mds test: commit before the restart")
		env.stopIndexerWhenTicking(t)

		env.uploadObject(t, mdsBranch, "data/after-restart.txt", "after")
		restartCommit := env.commitBranch(t, cl, mdsBranch, "mds test: commit while the indexer is down")
		env.startIndexer(t)
		env.waitIndexed(t, mdsBranch, restartCommit)

		read := env.readRows(t, mdsBranch)
		require.Contains(t, mdsPaths(read.PyIceberg.Rows), "data/before-restart.txt")
		require.Contains(t, mdsPaths(read.PyIceberg.Rows), "data/after-restart.txt",
			"a restarted indexer must index the commits it missed")
		require.Equal(t, restartCommit, read.PyIceberg.SourceCommit)
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
		// METADATA_SEARCH_PLAN.md (M6) says 403. The lakeFS *core API* reports insufficient
		// permissions as 401 (its long-standing convention, pkg/api), while the S3 gateway and the
		// Iceberg catalog answer 403; the case asserts both, so neither convention can hide a
		// missing deny. See the commit message: the design says "cannot write", not "403".
		res, err := cl.UploadObjectWithBodyWithResponse(t.Context(), env.metadataRepo, mdsBranch,
			&apigen.UploadObjectParams{Path: "forged/row.txt"}, mdsObjectContentType, strings.NewReader("forged"))
		require.NoError(t, err, "upload into %s", env.metadataRepo)
		require.Equalf(t, http.StatusUnauthorized, res.StatusCode(),
			"an administrator must not be able to write %s through the API while the deny of §8 is "+
				"in force; the core API reports insufficient permissions as 401: %s", env.metadataRepo, res.Body)
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
			repo:          "mds-e2e-" + icebergUniqueSuffix(),
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
	env.s3 = icebergS3Settings(t)
	env.bucket, env.runPrefix, env.storageNamespace = icebergStorageNamespace(t, env.repo)
	env.metadataRepo = icebergcatalog.MetadataRepository(env.repo)
	env.ensureBucket(t)

	// The server and the indexer read one file, as §7 documents. The metadata_search section is
	// written without credentials first, because the mds-indexer user does not exist until the
	// server has bootstrapped it; the file is rewritten with the created credentials before the
	// indexer starts.
	env.writeConfig(t, "", "")
	env.startServer(t)
	t.Cleanup(func() { env.stop(t) })

	env.waitForServer(t)
	env.admin = env.coreAPI(t)
	env.requireMDSCore(t)
	env.createRepository(t)
	env.indexerKey, env.indexerSecret = env.createIndexerCredentials(t)
	env.writeConfig(t, env.indexerKey, env.indexerSecret)
	return env
}

// writeConfig writes the one lakefs.yaml both processes read (design §7).
//
// The blockstore section is spelled out in full instead of being inherited from the ambient
// LAKEFS_* variables: a partial section would shadow viper's nested defaults, and the server, the
// indexer and the query-engine containers must all address the same bucket with the same
// credentials and path style.
func (e *mdsEnv) writeConfig(t *testing.T, indexerKey, indexerSecret string) {
	t.Helper()
	var blockstore strings.Builder
	blockstore.WriteString("blockstore:\n  type: s3\n  s3:\n")
	fmt.Fprintf(&blockstore, "    region: %q\n", e.s3.Region)
	if e.s3.Endpoint != "" {
		fmt.Fprintf(&blockstore, "    endpoint: %q\n", e.s3.Endpoint)
	}
	fmt.Fprintf(&blockstore, "    force_path_style: %t\n", e.s3.PathStyle)
	blockstore.WriteString("    discover_bucket_region: false\n")
	fmt.Fprintf(&blockstore, "    credentials:\n      access_key_id: %q\n      secret_access_key: %q\n",
		e.s3.AccessKey, e.s3.SecretKey)

	var storage strings.Builder
	if e.s3.Endpoint != "" {
		fmt.Fprintf(&storage, "    s3.endpoint: %q\n", e.s3.Endpoint)
	}
	fmt.Fprintf(&storage, "    s3.region: %q\n", e.s3.Region)
	fmt.Fprintf(&storage, "    s3.access-key-id: %q\n", e.s3.AccessKey)
	fmt.Fprintf(&storage, "    s3.secret-access-key: %q\n", e.s3.SecretKey)
	if e.s3.PathStyle {
		storage.WriteString("    s3.path-style-access: \"true\"\n")
	}

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
  storage:
%s  repositories:
    %s:
      branches: [%s, %s]
`, e.listenAddress, filepath.Join(e.dir, "metadata"),
		mdsAdminUsername, e.key, e.secret,
		e.issuer, "http://"+e.listenAddress,
		blockstore.String(), icebergcatalog.CatalogPrefix,
		e.baseURL, indexerKey, indexerSecret, e.indexerListenAddress, mdsIndexerPeriod,
		storage.String(), e.repo, mdsBranch, mdsFeatureBranch)

	require.NoError(t, os.WriteFile(e.configPath, []byte(document), mdsConfigFileMode),
		"write the lakefs.yaml both processes read")
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
func (e *mdsEnv) startProcess(t *testing.T, name, logPath string, args ...string) *mdsProcess {
	t.Helper()
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, mdsConfigFileMode)
	require.NoErrorf(t, err, "open the %s log file", name)
	cmd := exec.CommandContext(e.ctx, e.binary, args...)
	cmd.Dir = e.dir
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
	e.server = e.startProcess(t, "lakefs-sso", e.logPath, "run", "--config", e.configPath)
}

// startIndexer starts `lakefs-sso mds run` on the same configuration (§4.1) and waits until its own
// HTTP server answers.
func (e *mdsEnv) startIndexer(t *testing.T) {
	t.Helper()
	require.Nil(t, e.indexer, "the indexer is already running; stop it first")
	e.indexer = e.startProcess(t, "lakefs-sso mds run", e.indexPath, "mds", "run", "--config", e.configPath)
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
	if err := e.deleteRunPrefix(ctx); err != nil {
		t.Errorf("delete the run prefix s3://%s/%s: %v", e.bucket, e.runPrefix, err)
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

// stopIndexerWhenTicking kills the indexer while a tick is in progress, if it can observe one.
//
// A tick of a repository reports in_progress on /_health (§9), so the suite can aim the kill at the
// window §4.4 is about (the Iceberg commit staged, or the lakeFS commit written) instead of at an
// idle period. A tick can still be too short to catch; the kill then lands between ticks, which is
// just as valid a restart and is logged.
func (e *mdsEnv) stopIndexerWhenTicking(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(3 * mdsIndexerPeriodDuration)
	ticking := false
	for time.Now().Before(deadline) {
		report, ok := e.indexerHealth(t)
		if ok {
			repo, found := report.Repositories[e.repo]
			if found && repo.InProgress {
				ticking = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ticking {
		t.Logf("no tick was in progress when the indexer was killed; the restart covers a kill between ticks")
	}
	e.stopIndexer(t)
}

// ---------------------------------------------------------------------------------------------
// principals of §8
// ---------------------------------------------------------------------------------------------

// requireMDSCore asserts that the server's Metadata Search bootstrap ran: the mds-indexer user
// exists, both policies of §8 exist, and mds-metadata-repo-protect is attached to the base groups,
// which is what makes the deny of §8 enforced in this installation.
func (e *mdsEnv) requireMDSCore(t *testing.T) {
	t.Helper()
	user, err := e.admin.GetUserWithResponse(t.Context(), bootstrap.UserName)
	require.NoErrorf(t, err, "get user %s", bootstrap.UserName)
	require.Equalf(t, http.StatusOK, user.StatusCode(),
		"the server must bootstrap the %s user of §8 at startup: %s", bootstrap.UserName, user.Body)

	for _, policy := range []string{bootstrap.IndexerPolicyName, bootstrap.MetadataRepoProtectPolicyName} {
		res, err := e.admin.GetPolicyWithResponse(t.Context(), policy)
		require.NoErrorf(t, err, "get policy %s", policy)
		require.Equalf(t, http.StatusOK, res.StatusCode(),
			"the server must bootstrap the %s policy of §8 at startup: %s", policy, res.Body)
	}

	// The deny is attached to the base groups (§8). Admins is the group the break-glass
	// administrator is in, so this is the attachment the "non-indexer cannot write" case needs.
	groups, err := e.admin.ListGroupPoliciesWithResponse(t.Context(), "Admins", &apigen.ListGroupPoliciesParams{})
	require.NoError(t, err, "list the policies of the Admins group")
	require.Equalf(t, http.StatusOK, groups.StatusCode(), "list the policies of Admins: %s", groups.Body)
	require.NotNil(t, groups.JSON200)
	names := make([]string, 0, len(groups.JSON200.Results))
	for _, policy := range groups.JSON200.Results {
		names = append(names, policy.Id)
	}
	require.Containsf(t, names, bootstrap.MetadataRepoProtectPolicyName,
		"the %s deny of §8 must be attached to the base groups, or the write protection is not "+
			"in force in this run; Admins policies: %v", bootstrap.MetadataRepoProtectPolicyName, names)
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

// mdsRead is the result of one read step: the same table through both published engines.
type mdsRead struct {
	PyIceberg mdsClientRead `json:"pyiceberg"`
	DuckDB    mdsClientRead `json:"duckdb"`
}

// readRows reads the virtual table of R at ref with PyIceberg (through /mds/iceberg/api, the
// upstream endpoint of §5.5) and with DuckDB (through /iceberg/relative_to/{repo}.{ref}/api), and
// returns both. Every row of §3.1 is reported by PyIceberg; DuckDB reports the columns the cases
// assert on, because that is what its published recipe selects.
func (e *mdsEnv) readRows(t *testing.T, ref string) mdsRead {
	t.Helper()
	return mdsDecode[mdsRead](t, e.runReadStep(t, ref, true))
}

// countRows reads the same table without materialising the rows, for a table too large to pass
// through the step's JSON result.
func (e *mdsEnv) countRows(t *testing.T, ref string) mdsRead {
	t.Helper()
	return mdsDecode[mdsRead](t, e.runReadStep(t, ref, false))
}

// runReadStep runs the read program in the client container and returns its JSON object.
func (e *mdsEnv) runReadStep(t *testing.T, ref string, withRows bool) map[string]any {
	t.Helper()
	identifier, err := json.Marshal([]string{e.repo, ref, icebergcatalog.SystemNamespace, icebergcatalog.ObjectMetadataTable})
	require.NoError(t, err, "marshal the PyIceberg identifier")
	rows := "1"
	if !withRows {
		rows = "0"
	}
	return e.runClient(t, "mds-read:"+ref, mdsReadScript, map[string]string{
		"MDS_WAREHOUSE":       e.storageNamespace,
		"MDS_IDENTIFIER":      string(identifier),
		"MDS_CREDENTIAL":      e.key + ":" + e.secret,
		"MDS_CATALOG_URI":     e.dockerReachableURL(e.baseURL + icebergcatalog.MetadataSearchBasePath),
		"MDS_RELATIVE_URI":    e.dockerReachableURL(e.baseURL+icebergcatalog.RelativeToBasePath) + "/" + e.repo + "." + ref + "/api",
		"MDS_OAUTH_URI":       e.dockerReachableURL(e.baseURL+icebergcatalog.MetadataSearchBasePath) + "/v1/oauth/tokens",
		"MDS_DUCKDB_RELATION": icebergcatalog.SystemNamespace + "." + icebergcatalog.ObjectMetadataTable,
		"MDS_ROWS":            rows,
	})
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

// mdsReadScript reads R.<ref>.system.object_metadata with both published engines and reports the
// rows and the freshness property (§9).
//
// PyIceberg uses the identifier form against /mds/iceberg/api, the upstream endpoint of §5.5: the
// RestCatalog prepends the prefix from GET /v1/config, so the identifier is
// [repository, ref, system, object_metadata].
//
// DuckDB uses the warehouse form against /iceberg/relative_to/{repository}.{ref}/api: the URL fixes
// the repository and the ref, so the relation it queries is the client's own namespace and table.
//
// MDS_ROWS=0 reports counts only, which is what the 100k-object case needs: a hundred thousand rows
// of JSON would not survive a docker exec's output.
const mdsReadScript = `
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

s3_properties = {
    "s3.access-key-id": required("ICEBERG_S3_KEY"),
    "s3.secret-access-key": required("ICEBERG_S3_SECRET"),
    "s3.region": os.environ.get("ICEBERG_S3_REGION", "us-east-1"),
}
if os.environ.get("ICEBERG_S3_ENDPOINT"):
    s3_properties["s3.endpoint"] = os.environ["ICEBERG_S3_ENDPOINT"]
    s3_properties["s3.path-style-access"] = os.environ.get("ICEBERG_S3_PATH_STYLE", "true")

# --- PyIceberg, through /mds/iceberg/api (the upstream endpoint of design §5.5) ---------------
catalog = RestCatalog(
    name="lakefs",
    uri=required("MDS_CATALOG_URI"),
    credential=required("MDS_CREDENTIAL"),
    warehouse=required("MDS_WAREHOUSE"),
    **s3_properties,
)
identifier = tuple(json.loads(required("MDS_IDENTIFIER")))
table = catalog.load_table(identifier)
scanned = table.scan().to_arrow()
pyiceberg_rows = []
if with_rows:
    for row in scanned.to_pylist():
        pyiceberg_rows.append({
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

# --- DuckDB, through /iceberg/relative_to/{repo}.{ref}/api ------------------------------------
relation = required("MDS_DUCKDB_RELATION")
con = duckdb.connect()
con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")
con.execute(
    "CREATE OR REPLACE SECRET lakefs_catalog (TYPE iceberg, CLIENT_ID %s, "
    "CLIENT_SECRET %s, OAUTH2_SERVER_URI %s)" % (
        sql_literal(required("MDS_CREDENTIAL").split(":", 1)[0]),
        sql_literal(required("MDS_CREDENTIAL").split(":", 1)[1]),
        sql_literal(required("MDS_OAUTH_URI")),
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
        sql_literal(required("MDS_WAREHOUSE")),
        sql_literal(required("MDS_RELATIVE_URI")),
    )
)
duckdb_query = 'SELECT path, etag, commit_id, size_bytes, user_metadata FROM lakefs."system"."object_metadata"'
if not with_rows:
    duckdb_query = 'SELECT count(*) FROM lakefs."system"."object_metadata"'
duckdb_rows = []
duckdb_count = 0
for record in con.execute(duckdb_query).fetchall():
    if with_rows:
        duckdb_rows.append({
            "path": record[0],
            "etag": record[1],
            "commit_id": record[2],
            "size_bytes": record[3],
            "user_metadata": metadata_map(record[4]),
        })
    else:
        duckdb_count = record[0]

print("RESULT:" + json.dumps({
    "pyiceberg": {
        "rows": pyiceberg_rows,
        "count": len(pyiceberg_rows) if with_rows else scanned.num_rows,
        "metadata_location": table.metadata_location,
        "source_commit": table.properties.get("lakefs.mds.source-commit", ""),
    },
    "duckdb": {
        "rows": duckdb_rows,
        "count": len(duckdb_rows) if with_rows else duckdb_count,
    },
}))
`
