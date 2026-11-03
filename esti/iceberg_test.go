// Package esti holds the lakeFS system tests.
//
// iceberg_test.go is the Iceberg REST catalog end-to-end suite of ICEBERG_CATALOG_PLAN.md §11:
// the I-1/I-2 read-path smoke test (I2-8) and the I-3 exit criteria (I3-3) - append, lakeFS commit,
// GC, still readable; then branch, write, merge, read on main, and the conflict §6.4 requires.
//
// # The read path (I2-8)
//
// A table that was written *outside* the catalog is adopted with the catalog's register operation
// (I2-6) and then read back by the two published client recipes, PyIceberg through /iceberg/api and
// DuckDB through /iceberg/relative_to/{repo}.{branch}/api (§1.1 goals 1-3, §5 "Addressing",
// §12 "Testing / Esti"). The scenario is one straight line, because each step is a property the
// read path has to have:
//
//  1. an Iceberg table is written with PyIceberg's own FileIO into the managed table location
//     <storage_namespace>/_iceberg/<uuid>/ of a repository on the ambient S3 blockstore (MinIO
//     locally, S3 in CI): data, manifest and metadata files all live in the bucket, and no lakeFS
//     entry exists for them yet;
//  2. the catalog adopts the table's metadata.json with register (I2-6), which is the only write
//     this suite performs and the write the I-1 exit criteria are stated in terms of;
//  3. both addressing forms of §5 must then answer the same table: the identifier form, where the
//     repository and the ref are the leading namespace levels, and the warehouse form, where they
//     are the URL. I1-10 makes the two indistinguishable, and this is what pins it end to end;
//  4. PyIceberg scans the table through /iceberg/api and DuckDB through
//     /iceberg/relative_to/{repo}.{branch}/api, and both must return the rows the writer wrote.
//
// Step 3 is checked with plain HTTP as well as with the clients: the clients prove that real
// published recipes work, while the HTTP assertions state the property (same metadata-location,
// same table) independently of any client's error handling, so a client regression cannot hide a
// catalog regression.
//
// # The write path (I3-3, §11 I-3 exit criteria)
//
// The I-3 exit criteria are "Esti: append, commit, GC, then table still readable; branch, write,
// merge, read on main" (§11), and they are the guarantee of §6.3: a table file is a lakeFS entry,
// so the committed GC protects it, the diff shows it, and a merge carries it. Two tests state them,
// one per sentence of the exit criteria:
//
//   - TestIcebergAppendGC: PyIceberg creates a table and appends rows through the catalog, so the
//     catalog writes metadata.json and stages the table files as lakeFS entries (I3-1) on the
//     branch. The suite then makes the lakeFS commit, and asserts that the commit's diff carries
//     one entry per file the append wrote under _lakefs_iceberg/files/<table-uuid>/ - not only the
//     pointer - and that every entry addresses the physical object the client wrote. It then runs
//     the server-side half of the committed GC (prepare_commits) and asserts that the commit
//     carrying those files is one of the active commits GC protects, that the objects are still in
//     the bucket, and that the table still reads on main.
//   - TestIcebergBranchMerge: a second append happens on a branch of its own, is committed, and is
//     merged into main; the merged table must read back with both appends' rows. Then two branches
//     change the same table from a common base, and the second merge must fail with 409: the
//     pointer is a single entry, so concurrent changes to one table conflict at merge (§6.4).
//   - TestIcebergTagRead: the table is read at a tag, where the rows are the ones the tag captured
//     and not the branch's later appends, and a write addressed at the tag is refused with the
//     documented "ref is read-only" message (§5, Q-I4).
//   - TestIcebergDrop: the table is dropped through the catalog; the staged drop makes it stop
//     loading and stop being listed, and the lakeFS commit publishes that by removing the pointer
//     entry - the one entry that makes the table exist (§6.1). The file entries §6.3 staged are not
//     removed by the drop, which is measured and recorded rather than asserted away
//     (pkg/icebergcatalog/conformance/RESULTS.md §10 N-3).
//
// # What the GC step covers, and what it does not
//
// The committed GC sweep is a Spark job that reads the commits CSV this test produces
// (graveler/retention, the "gc" command of cmd/lakefs) plus the repository's live addresses. That
// job needs a Spark cluster and the lakeFS metadata client jar, which the throw-away lakefs-sso
// server of this harness does not have (§12 runs those suites against the esti deployment, not
// against a per-test server). What is asserted here is therefore the whole safety input of the
// sweep - every file the table references is the target of a lakeFS entry in the commit, and the
// commit is in the active-commit set the sweep protects - plus the two observable outcomes that
// matter to a reader: the objects are still in the bucket and the table still reads. §15.2 measured
// that staging is what puts a file into the GC's view: nothing else does.
//
// # Preconditions and skipping
//
// The clients run in docker (see iceberg_utils.go), and the suite skips - it never fails - when
// the environment cannot host it: no S3 blockstore, no docker daemon, no way to install the
// clients, or a catalog that does not advertise an operation the step needs (register for the read
// path, the commit operation for the write path). A reader of the skip message gets the missing
// operation and what it unblocks, so a suite that silently does nothing is not possible.
//
// # Clean-room note
//
// Written from ICEBERG_CATALOG_PLAN.md §5, §6.1, §6.3, §6.4, §11, §12, §15, the Apache Iceberg REST
// OpenAPI catalog spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml), the Apache
// Iceberg table spec (https://iceberg.apache.org/spec/#table-metadata), the public PyIceberg and
// DuckDB documentation (https://py.iceberg.apache.org/, https://duckdb.org/docs/stable/core_extensions/iceberg/overview)
// and the public lakeFS documentation (https://docs.lakefs.io/iceberg/). No lakeFS Enterprise code
// and no upstream lakeFS >= 1.87 (BSL 1.1) code was used.
package esti

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	// icebergCommitEndpoint is the commit operation of the Iceberg REST spec, in the literal-{prefix}
	// form §2.1 requires an advertised endpoint to use. It is what a client's append becomes, so it
	// is the precondition of every write assertion below (§6.2, I2-3).
	icebergCommitEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"

	// icebergFeatureBranch is the branch TestIcebergBranchMerge writes its second append on, and
	// the source of the merge into main.
	icebergFeatureBranch = "feature"

	// icebergConflictBranchA and icebergConflictBranchB change the same table from a common base;
	// the second one to be merged must conflict (§6.4).
	icebergConflictBranchA = "append-a"
	icebergConflictBranchB = "append-b"

	// icebergCommitPageSize is the page size of the listing calls the assertions below page
	// through: one append of a few files is far below it, and paging keeps the helpers correct for
	// a table whose commit stages more entries than one page holds.
	icebergCommitPageSize = 1000

	// icebergTag is the tag TestIcebergTagRead cuts and reads: a named, immutable ref, which is the
	// third kind of ref a reader may address (§5 "Reads work on any ref").
	icebergTag = "iceberg-v1"
)

// TestIcebergReadPath registers a fixture table and reads it with PyIceberg and DuckDB: the
// read-path smoke test of §11 I-2 task I2-8.
func TestIcebergReadPath(t *testing.T) {
	env := newIcebergEnv(t)

	// Write the table outside the catalog, then adopt its metadata.json: the I-1 exit criteria
	// ("PyIceberg, DuckDB and Spark can read a table registered by register", §11).
	fixture := env.writeFixture(t)
	env.registerFixtureTable(t, fixture)

	// The two addressing forms of §5 must be indistinguishable (I1-10): the same table, the same
	// metadata.json, whichever way the client names its warehouse.
	env.requireLoadTableAgrees(t, fixture.MetadataLocation)

	// Read it with both published clients, in docker.
	read := env.readWithClients(t)
	require.Equal(t, fixture.IDs, read.PyIcebergIDs,
		"PyIceberg must read the registered table's rows through %s", env.catalogURL)
	require.Equal(t, []string{fixture.MetadataLocation}, read.PyIcebergLocations,
		"PyIceberg must load the metadata.json that register adopted")
	require.Equal(t, fixture.IDs, read.DuckDBIDs,
		"DuckDB must read the registered table's rows through %s",
		env.relativeToURL+"/"+env.repo+"."+env.branch+"/api")
	require.Equal(t, env.namespace+"."+env.table, read.DuckDBRelation,
		"the relative_to form must address the table by its own namespace levels only, "+
			"with the repository and the ref fixed by the URL")
}

// TestIcebergAppendGC is the first half of the I-3 exit criteria (§11): a client appends through
// the catalog, the lakeFS commit stages the files, the committed GC runs over the repository, and
// the table is still readable afterwards.
//
// The order matters and is the order of §6.2 and §6.3: the client writes the objects and the
// catalog stages an entry for every one of them together with the pointer, so one lakeFS commit
// publishes the files and the pointer that references them. A file that is not staged is a file
// the committed GC may delete (§15.2: nothing but staging puts a table file into the GC's view),
// which is why the assertions are about the entries and not only about the metadata.
func TestIcebergAppendGC(t *testing.T) {
	env := newIcebergEnv(t)
	env.requireCatalogCommit(t)
	cl := env.coreAPI(t)

	// The lakeFS commit this append will be measured against.
	base := env.headCommit(t, cl, env.branch)

	// Create the table and append rows through the catalog. Before the append the table exists as
	// a staged pointer only; the append is the first commit that also stages table files.
	appended := env.appendRows(t, env.branch, true, []icebergRow{{1, "alpha"}, {2, "beta"}, {3, "gamma"}})
	commit := env.commitBranch(t, cl, env.branch, "iceberg: create and append")
	require.NotEqualf(t, base, commit, "the commit must be a new commit on %s", env.branch)

	// §11 I-3: "the lakeFS commit stages the files". The commit's diff must list one entry per file
	// the append wrote - metadata.json, the manifest list, every manifest and every data file -
	// each pointing at the physical object the client wrote, and the pointer entry beside them.
	// A diff that shows only the pointer is the failure this test exists to catch (§6.3).
	required := appended.requiredObjects()
	env.requireStagedFiles(t, cl, commit, appended.TableUUID, required)

	// §11 I-3: "GC runs and the table is still readable". The sweep itself is the Spark job that
	// consumes the commits CSV; the assertions are its whole safety input, plus the outcome.
	env.requireGCPrepareKeepsCommit(t, cl, commit)
	env.requireObjectsPresent(t, required)

	read := env.readTable(t, env.branch)
	require.Equal(t, appended.IDs, read.PyIcebergIDs,
		"PyIceberg must read the appended rows after the commit and the GC prepare")
	require.Equal(t, appended.IDs, read.DuckDBIDs,
		"DuckDB must read the appended rows after the commit and the GC prepare")
}

// TestIcebergBranchMerge is the second half of the I-3 exit criteria (§11): write on a branch,
// merge into main, read the merged table - and, because the pointer is a single entry, a second,
// concurrent change to the same table must conflict at merge (§6.4).
func TestIcebergBranchMerge(t *testing.T) {
	env := newIcebergEnv(t)
	env.requireCatalogCommit(t)
	cl := env.coreAPI(t)

	// main: create the table and write the base rows.
	base := env.appendRows(t, env.branch, true, []icebergRow{{1, "alpha"}, {2, "beta"}, {3, "gamma"}})
	env.commitBranch(t, cl, env.branch, "iceberg: base rows on main")
	require.Equal(t, []int64{1, 2, 3}, base.IDs, "the base append must write its own rows")

	// A branch of its own, and a second append on it. The catalog stages the append on that
	// branch, exactly as it did on main: a branch is where catalog writes land (§6.2).
	env.createBranch(t, cl, icebergFeatureBranch, env.branch)
	appended := env.appendRows(t, icebergFeatureBranch, false, []icebergRow{{4, "delta"}, {5, "epsilon"}})
	env.commitBranch(t, cl, icebergFeatureBranch, "iceberg: rows on the feature branch")

	// Merge the branch into main. The table is one pointer entry among the branch's changes, so the
	// merge carries it as an ordinary change (§6.4) and main's table is the merged one.
	env.requireMergeSucceeds(t, cl, icebergFeatureBranch, env.branch)

	// appended.IDs is already the whole table after the feature branch's append - the scan of the
	// merged table - so it carries the base rows as well as the two the branch added.
	wantIDs := appended.IDs
	merged := env.readTable(t, env.branch)
	require.Equal(t, []int64{1, 2, 3, 4, 5}, merged.PyIcebergIDs,
		"the merged table on main must be the base rows plus the feature branch's")
	require.Equal(t, wantIDs, merged.PyIcebergIDs,
		"after the merge main must read both appends: the base rows and the feature branch's")
	require.Equal(t, wantIDs, merged.DuckDBIDs,
		"DuckDB must read the merged table through the relative_to form")

	// §6.4: two branches change the same table from a common base. The first merge lands, the
	// second one must fail - with a conflict, not by silently overwriting the first.
	env.createBranch(t, cl, icebergConflictBranchA, env.branch)
	env.createBranch(t, cl, icebergConflictBranchB, env.branch)
	env.appendRows(t, icebergConflictBranchA, false, []icebergRow{{6, "zeta"}})
	env.commitBranch(t, cl, icebergConflictBranchA, "iceberg: concurrent append A")
	env.appendRows(t, icebergConflictBranchB, false, []icebergRow{{7, "eta"}})
	env.commitBranch(t, cl, icebergConflictBranchB, "iceberg: concurrent append B")

	env.requireMergeSucceeds(t, cl, icebergConflictBranchA, env.branch)
	beforeConflict := env.headCommit(t, cl, env.branch)
	requireMergeConflicts(t, cl, env.repo, icebergConflictBranchB, env.branch)
	require.Equal(t, beforeConflict, env.headCommit(t, cl, env.branch),
		"a conflicted merge must not move the destination branch: %s stays where it was",
		env.branch)
}

// TestIcebergTagRead is the "tag read" half of the ref rules of §5: a table is readable at a tag
// and a write addressed at one is refused.
//
// A tag is what publishes a table version outside the branch that produced it, so the property that
// matters is that the reader sees the table *as of the tag*, not the branch: the append that lands
// on main after the tag is cut must not appear through the tag. Both published clients read it - the
// identifier form for PyIceberg, the relative_to form for DuckDB - and both are read-only by
// construction, so the append through the tag is expected to be refused with the documented
// "ref is read-only" message (Q-I4) rather than to succeed quietly.
func TestIcebergTagRead(t *testing.T) {
	env := newIcebergEnv(t)
	env.requireCatalogCommit(t)
	cl := env.coreAPI(t)

	// The table as of the tag: create it, append, commit, then cut the tag.
	tagged := env.appendRows(t, env.branch, true, []icebergRow{{1, "alpha"}, {2, "beta"}})
	taggedCommit := env.commitBranch(t, cl, env.branch, "iceberg: rows the tag captures")
	env.createTag(t, cl, icebergTag, taggedCommit)

	// A later append on the branch, committed after the tag exists: it must not be visible at the
	// tag, because a tag is an immutable ref and not an alias of the branch.
	laterRows := []icebergRow{{3, "gamma"}}
	later := env.appendRows(t, env.branch, false, laterRows)
	env.commitBranch(t, cl, env.branch, "iceberg: rows after the tag")

	read := env.readTable(t, icebergTag)
	require.Equal(t, tagged.IDs, read.PyIcebergIDs,
		"PyIceberg must read the rows the tag captured, not the branch's later append")
	require.Equal(t, tagged.IDs, read.DuckDBIDs,
		"DuckDB must read the tag through the relative_to form")
	require.NotContains(t, read.PyIcebergIDs, laterRows[0].ID,
		"the row appended after the tag must not appear through the tag")

	// The tag still points where it did, and the branch still reads the whole table: later.IDs is
	// already the full scan of the branch after its append, tagged rows included.
	branchRead := env.readTable(t, env.branch)
	wantBranch := later.IDs
	require.Equal(t, wantBranch, branchRead.PyIcebergIDs,
		"%s must read every append, including the one after the tag", env.branch)

	// Writes need a branch: an append addressed at the tag is refused (§5, Q-I4).
	env.requireRefReadOnly(t, icebergTag)
}

// TestIcebergDrop covers the drop operation (I2-5) and what the lakeFS commit does with it.
//
// A drop is staged like every other catalog write (§6.2): the catalog removes the pointer entry on
// the branch, the table stops loading and stops being listed immediately, and the lakeFS commit
// publishes the removal. The contract asserted here is that the commit's diff *removes* the pointer,
// the single entry §6.1 says a table is.
//
// The other half is deliberately an observation, not an assertion: the `_lakefs_iceberg/files/...`
// entries that §6.3 staged for the table are **not** removed by the drop (measured; see
// pkg/icebergcatalog/conformance/RESULTS.md §10 N-3), so they keep the table's objects in the
// committed GC's live set after the table is gone. Pinning that as an expectation would make a
// future fix - tombstoning the entries, which is what would let the GC reclaim a dropped table's
// files - look like a regression, so the test logs what survived instead.
func TestIcebergDrop(t *testing.T) {
	env := newIcebergEnv(t)
	env.requireCatalogCommit(t)
	cl := env.coreAPI(t)

	appended := env.appendRows(t, env.branch, true, []icebergRow{{1, "alpha"}, {2, "beta"}})
	base := env.commitBranch(t, cl, env.branch, "iceberg: create and append before the drop")
	env.requireTableListed(t, env.branch, true)

	// Drop through the catalog. Everything below happens on the staged state: the catalog writes
	// into the branch's staging area, so the drop is visible to the catalog and not yet a commit.
	env.dropTableThroughCatalog(t, env.branch)
	env.requireTableAbsent(t, env.branch)
	env.requireTableListed(t, env.branch, false)

	// Publish the drop, then assert the same outcomes on the committed state, and that the commit
	// removed the pointer - the one entry whose presence is the table.
	dropped := env.commitBranch(t, cl, env.branch, "iceberg: drop the table")
	require.NotEqualf(t, base, dropped, "the drop commit must be a new commit on %s", env.branch)
	env.requireTableAbsent(t, env.branch)
	env.requireTableListed(t, env.branch, false)

	paths := env.diffPaths(t, cl, dropped)
	require.Equalf(t, "removed", paths[icebergPointerPath([]string{env.namespace}, env.table)],
		"the drop commit must remove the table pointer; diff paths: %v", sortedPathList(paths))

	// Observed, not asserted: what the drop left behind under the table's file-entry directory.
	filePrefix := icebergTableFileEntryPrefix(appended.TableUUID)
	retained := 0
	for path := range paths {
		if strings.HasPrefix(path, filePrefix) {
			retained++
		}
	}
	t.Logf("the drop commit changed %d entries below %q; the catalog deletes the pointer only, so a "+
		"dropped table's file entries stay in the branch and keep its objects GC-live "+
		"(conformance/RESULTS.md §10 N-3)", retained, filePrefix)
}

// icebergRow is one row of the test table. The table is deliberately trivial - an id and a name -
// because the subject of these tests is where the files live and what lakeFS does with them, not
// what is inside them.
type icebergRow struct {
	ID   int64
	Name string
}

// icebergAppendResult is what the append step reports about the table it wrote through the
// catalog: the pointer's metadata-location, the table UUID that names the staged entry directory,
// and every object the append made the table reference.
type icebergAppendResult struct {
	MetadataLocation string
	TableUUID        string
	ManifestList     string
	Manifests        []string
	DataFiles        []string
	IDs              []int64
}

// requiredObjects is the set of physical objects the table's current metadata references, i.e. the
// set a GC must not delete for the table to stay readable: metadata.json, the manifest list of the
// current snapshot, its manifests, and their data files.
func (a icebergAppendResult) requiredObjects() []string {
	objects := make([]string, 0, 2+len(a.Manifests)+len(a.DataFiles))
	objects = append(objects, a.MetadataLocation, a.ManifestList)
	objects = append(objects, a.Manifests...)
	objects = append(objects, a.DataFiles...)
	return objects
}

// coreAPI returns a client of the core lakeFS API - not of the catalog - for the git operations
// this suite drives: commit, branch, merge, diff and the GC prepare step. Repository and
// credential plumbing is the same as the harness uses for its setup call.
func (e *icebergEnv) coreAPI(t *testing.T) *apigen.ClientWithResponses {
	t.Helper()
	cl, err := testutil.NewClientFromCreds(logging.ContextUnavailable(), e.key, e.secret, e.baseURL+apiutil.BaseURL)
	require.NoError(t, err, "build the core API client")
	return cl
}

// requireCatalogCommit skips the test when the catalog does not advertise the commit operation.
//
// The commit is what every write assertion below is stated in terms of: PyIceberg's append sends
// an add-snapshot commit, and the catalog answers it by staging the pointer and the snapshot's
// files (§6.2, §6.3). GET /v1/config is the catalog's own statement about what it serves (§2.1),
// so an operation missing from it is not implemented and the exit criteria cannot be exercised -
// a skip with the reason, not a failure of this suite.
func (e *icebergEnv) requireCatalogCommit(t *testing.T) {
	t.Helper()
	status, body := e.doJSON(t, http.MethodGet, e.catalogURL+"/v1/config", nil)
	require.Equalf(t, http.StatusOK, status, "GET /v1/config: %s", body)

	var config struct {
		Endpoints []string `json:"endpoints"`
	}
	require.NoErrorf(t, json.Unmarshal(body, &config), "decode GET /v1/config: %s", body)
	if !containsString(config.Endpoints, icebergCommitEndpoint) {
		t.Skipf("the catalog does not advertise %q, so no client can append through it and the "+
			"I-3 exit criteria (append, commit, GC, read; branch, merge) cannot be exercised; "+
			"skipping the Iceberg write-path test", icebergCommitEndpoint)
	}
}

// headCommit returns the commit id a branch currently points at.
func (e *icebergEnv) headCommit(t *testing.T, cl *apigen.ClientWithResponses, branch string) string {
	t.Helper()
	amount := apigen.PaginationAmount(1)
	res, err := cl.LogCommitsWithResponse(t.Context(), e.repo, branch, &apigen.LogCommitsParams{Amount: &amount})
	require.NoErrorf(t, err, "log commits on %s", branch)
	require.Equalf(t, http.StatusOK, res.StatusCode(), "log commits on %s: %s", branch, res.Body)
	require.NotNilf(t, res.JSON200, "log commits on %s: %s", branch, res.Body)
	require.NotEmptyf(t, res.JSON200.Results, "branch %s has no commits", branch)
	return res.JSON200.Results[0].Id
}

// commitBranch publishes what the catalog staged on branch: the lakeFS commit of §6.2 that turns
// the staged pointer and staged table files into a commit a reader and the GC can see.
func (e *icebergEnv) commitBranch(t *testing.T, cl *apigen.ClientWithResponses, branch, message string) string {
	t.Helper()
	res, err := cl.CommitWithResponse(t.Context(), e.repo, branch, &apigen.CommitParams{},
		apigen.CommitJSONRequestBody{Message: message})
	require.NoErrorf(t, err, "commit %s", branch)
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "commit %s: %s", branch, res.Body)
	require.NotNilf(t, res.JSON201, "commit %s: %s", branch, res.Body)
	return res.JSON201.Id
}

// createBranch creates branch from source.
func (e *icebergEnv) createBranch(t *testing.T, cl *apigen.ClientWithResponses, branch, source string) {
	t.Helper()
	res, err := cl.CreateBranchWithResponse(t.Context(), e.repo,
		apigen.CreateBranchJSONRequestBody{Name: branch, Source: source})
	require.NoErrorf(t, err, "create branch %s", branch)
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "create branch %s from %s: %s",
		branch, source, res.Body)
}

// requireMergeSucceeds merges source into destination and asserts that a merge result came back.
func (e *icebergEnv) requireMergeSucceeds(t *testing.T, cl *apigen.ClientWithResponses, source, destination string) {
	t.Helper()
	message := fmt.Sprintf("iceberg test: merge %s into %s", source, destination)
	res, err := cl.MergeIntoBranchWithResponse(t.Context(), e.repo, source, destination,
		apigen.MergeIntoBranchJSONRequestBody{Message: &message})
	require.NoErrorf(t, err, "merge %s into %s", source, destination)
	require.Equalf(t, http.StatusOK, res.StatusCode(), "merge %s into %s: %s", source, destination, res.Body)
	require.NotNilf(t, res.JSON200, "merge %s into %s: %s", source, destination, res.Body)
}

// requireMergeConflicts asserts that merging source into destination fails with 409, the status a
// client maps to a merge conflict (§6.4: the table pointer is a single entry, so two branches that
// both changed it cannot both land).
func requireMergeConflicts(t *testing.T, cl *apigen.ClientWithResponses, repo, source, destination string) {
	t.Helper()
	message := fmt.Sprintf("iceberg test: conflicting merge of %s into %s", source, destination)
	res, err := cl.MergeIntoBranchWithResponse(t.Context(), repo, source, destination,
		apigen.MergeIntoBranchJSONRequestBody{Message: &message})
	require.NoErrorf(t, err, "merge %s into %s", source, destination)
	require.Equalf(t, http.StatusConflict, res.StatusCode(),
		"merging %s into %s must conflict: both branches changed the table pointer (§6.4); got %s: %s",
		source, destination, res.Status(), res.Body)
}

// createTableNamespace stages the client-visible namespace of the append step's create_table, the
// way registerFixtureTable does for the read path (createFixtureNamespace): a table can only be
// created inside a namespace that exists, and the namespace is a staged marker of its own on the
// branch (§6.2, I2-2).
func (e *icebergEnv) createTableNamespace(t *testing.T, branch string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"namespace":  []string{e.repo, branch, e.namespace},
		"properties": map[string]string{},
	})
	require.NoError(t, err, "marshal the create-namespace request")
	status, response := e.doJSON(t, http.MethodPost, e.namespacesURL(e.catalogURL), body)
	require.Equalf(t, http.StatusOK, status, "create namespace %s on %s: %s", e.namespace, branch, response)
}

// appendRows runs one PyIceberg append through the catalog on branch, creating the table first
// when create is set, and returns what the step reported about the table afterwards.
func (e *icebergEnv) appendRows(t *testing.T, branch string, create bool, rows []icebergRow) icebergAppendResult {
	t.Helper()
	if create {
		// create_table needs the namespace to exist on this branch; nothing has written it yet
		// when the first append runs.
		e.createTableNamespace(t, branch)
	}
	encodedIdentifier, err := json.Marshal([]string{e.repo, branch, e.namespace, e.table})
	require.NoError(t, err, "marshal the PyIceberg identifier")

	encode := "0"
	if create {
		encode = "1"
	}
	result := e.runClient(t, "append:"+branch, icebergAppendScript, map[string]string{
		"ICEBERG_WAREHOUSE":      e.storageNamespace,
		"ICEBERG_IDENTIFIER":     string(encodedIdentifier),
		"ICEBERG_CREDENTIAL":     e.key + ":" + e.secret,
		"ICEBERG_CATALOG_URI":    e.dockerReachableURL(e.catalogURL),
		"ICEBERG_CREATE":         encode,
		"ICEBERG_TABLE_LOCATION": e.fixtureLocation(),
		"ICEBERG_ROWS":           icebergRowsJSON(t, rows),
	})

	appended := icebergAppendResult{
		MetadataLocation: stringField(t, result, "metadata_location"),
		TableUUID:        stringField(t, result, "table_uuid"),
		ManifestList:     stringField(t, result, "manifest_list"),
		Manifests:        stringSliceField(t, result, "manifests"),
		DataFiles:        stringSliceField(t, result, "data_files"),
		IDs:              int64SliceField(t, result, "ids"),
	}
	require.NotEmpty(t, appended.MetadataLocation, "the append step must report the metadata.json it committed")
	require.NotEmpty(t, appended.TableUUID, "the append step must report the table uuid")
	require.NotEmpty(t, appended.ManifestList, "the append step must report the snapshot's manifest list")
	require.NotEmpty(t, appended.Manifests, "the append step must report the snapshot's manifests")
	require.NotEmpty(t, appended.DataFiles, "the append step must report the files the scan plans")
	require.NotEmpty(t, appended.IDs, "the table must have rows after the append")
	// The managed layout of §6.1/§14 Q-I5: everything the client wrote for this table lives under
	// <storage_namespace>/_iceberg/<uuid>/, which is what the commit path validates (§13) and what
	// the uncommitted-GC grace protects (§6.3).
	for _, object := range appended.requiredObjects() {
		require.Truef(t, strings.HasPrefix(object, e.fixtureLocation()+"/"),
			"object %q must live inside the managed table location %q", object, e.fixtureLocation())
	}
	return appended
}

// readTable reads the table on branch with both published clients, exactly as readWithClients does
// for the registered fixture: PyIceberg through /iceberg/api and DuckDB through
// /iceberg/relative_to/{repo}.{branch}/api.
func (e *icebergEnv) readTable(t *testing.T, branch string) icebergReadResult {
	t.Helper()
	encodedIdentifier, err := json.Marshal([]string{e.repo, branch, e.namespace, e.table})
	require.NoError(t, err, "marshal the PyIceberg identifier")

	result := e.runClient(t, "read:"+branch, icebergReadScript, map[string]string{
		"ICEBERG_WAREHOUSE":       e.storageNamespace,
		"ICEBERG_IDENTIFIER":      string(encodedIdentifier),
		"ICEBERG_CREDENTIAL":      e.key + ":" + e.secret,
		"ICEBERG_CATALOG_URI":     e.dockerReachableURL(e.catalogURL),
		"ICEBERG_RELATIVE_URI":    e.dockerReachableURL(e.relativeToURL) + "/" + e.repo + "." + branch + "/api",
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

// requireStagedFiles asserts the §6.3 guarantee at the commit that carries it: the commit's diff
// lists the table's file entries - one per object the append wrote, each addressing the physical
// object the client wrote - and not only the pointer entry.
func (e *icebergEnv) requireStagedFiles(t *testing.T, cl *apigen.ClientWithResponses, commit, tableUUID string, required []string) {
	t.Helper()
	filePrefix := icebergTableFileEntryPrefix(tableUUID)

	// The diff, not a listing of the branch: "diff shows the staged file entries, not only the
	// pointer" is exactly what a reader, a merge and a dataset materialisation consume (§6.3, §9).
	paths := e.diffPaths(t, cl, commit)
	entries := 0
	for path := range paths {
		if strings.HasPrefix(path, filePrefix) {
			entries++
		}
	}
	require.GreaterOrEqualf(t, entries, len(required),
		"the commit must stage one entry per table file - metadata.json, the manifest list, every "+
			"manifest and every data file: %d entries under %q for %d objects; diff paths: %v",
		entries, filePrefix, len(required), sortedPathList(paths))

	pointer := icebergPointerPath([]string{e.namespace}, e.table)
	require.Containsf(t, paths, pointer,
		"the commit must also stage the table pointer %q; diff paths: %v", pointer, sortedPathList(paths))

	// Each entry must point at the physical object the client wrote (AddressType=Full): a staged
	// entry that addresses nothing, or something else, protects nothing.
	staged := e.stagedAddresses(t, cl, commit, filePrefix)
	for _, object := range required {
		require.Containsf(t, staged, object,
			"the commit must stage an entry addressing %q; staged addresses: %v", object, sortedValueList(staged))
	}
}

// requireGCPrepareKeepsCommit runs the server-side half of the committed GC and asserts that the
// commit carrying the table's files is part of what the sweep protects.
//
// The CSV the prepare step writes is the sweep's input: every commit listed in it is a live
// starting point, and addresses reachable from a live commit are not expired. Asking that the
// commit appear there is asking that the files' protection does not depend on the branch still
// pointing at it (§15.2).
func (e *icebergEnv) requireGCPrepareKeepsCommit(t *testing.T, cl *apigen.ClientWithResponses, commit string) {
	t.Helper()
	// The prepare step reads the repository's GC rules; a repository that never had rules set is
	// 404 "get gc rules: not found", so publish them first (as unified_gc_test.go does).
	rules, err := cl.SetGCRulesWithResponse(t.Context(), e.repo, apigen.SetGCRulesJSONRequestBody{
		Branches: []apigen.GarbageCollectionRule{
			{BranchId: e.branch, RetentionDays: 1},
			{BranchId: icebergFeatureBranch, RetentionDays: 1},
		},
		DefaultRetentionDays: 1,
	})
	require.NoError(t, err, "set GC rules")
	require.Equalf(t, http.StatusNoContent, rules.StatusCode(), "set GC rules: %s", rules.Body)

	res, err := cl.PrepareGarbageCollectionCommitsWithResponse(t.Context(), e.repo)
	require.NoError(t, err, "prepare GC commits")
	require.Equalf(t, http.StatusCreated, res.StatusCode(), "prepare GC commits: %s", res.Body)
	require.NotNilf(t, res.JSON201, "prepare GC commits: %s", res.Body)
	require.NotEmpty(t, res.JSON201.RunId, "prepare GC commits must report a run id")

	if res.JSON201.GcCommitsPresignedUrl == nil || *res.JSON201.GcCommitsPresignedUrl == "" {
		// The blockstore could not presign the CSV (the adapter may not support it). The prepare
		// step still ran; the active-commit set itself is then not readable from here, and the
		// staging assertions above carry the claim.
		t.Logf("prepare GC commits returned no presigned url for %q; skipping the active-commit check",
			res.JSON201.GcCommitsLocation)
		return
	}
	csv := httpGetString(t, *res.JSON201.GcCommitsPresignedUrl)
	require.Containsf(t, csv, commit,
		"the GC prepare step must list commit %s as an active commit; CSV: %s", commit, csv)
	require.Containsf(t, csv, commit+",false,",
		"the GC prepare step must mark commit %s as not expired; CSV: %s", commit, csv)
}

// requireObjectsPresent asserts that every object the table references is still in the bucket:
// nothing on the way to the GC prepare step deleted a file the table needs.
func (e *icebergEnv) requireObjectsPresent(t *testing.T, objects []string) {
	t.Helper()
	client := e.s3Client(t)
	for _, object := range objects {
		key, err := e.objectKey(object)
		require.NoErrorf(t, err, "object %q must address the bucket %q", object, e.bucket)
		_, err = client.StatObject(t.Context(), e.bucket, key, minio.StatObjectOptions{})
		require.NoErrorf(t, err, "object %q must still exist after the commit and the GC prepare", object)
	}
}

// s3Client builds an S3 client for the run's bucket with the credentials the server and the
// clients share, the same way the harness's cleanup does.
func (e *icebergEnv) s3Client(t *testing.T) *minio.Client {
	t.Helper()
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
	require.NoError(t, err, "build the S3 client")
	return client
}

// objectKey turns an s3:// URL of the run's bucket into the object key of that bucket.
func (e *icebergEnv) objectKey(raw string) (string, error) {
	prefix := icebergS3Scheme + "://" + e.bucket + "/"
	if !strings.HasPrefix(raw, prefix) {
		return "", fmt.Errorf("object %q is not inside s3://%s/", raw, e.bucket)
	}
	return strings.TrimPrefix(raw, prefix), nil
}

// diffPaths returns the paths a commit changed against its parent, with the change type.
func (e *icebergEnv) diffPaths(t *testing.T, cl *apigen.ClientWithResponses, commit string) map[string]string {
	t.Helper()
	commits, err := cl.LogCommitsWithResponse(t.Context(), e.repo, commit, &apigen.LogCommitsParams{})
	require.NoErrorf(t, err, "log commit %s", commit)
	require.Equalf(t, http.StatusOK, commits.StatusCode(), "log commit %s: %s", commit, commits.Body)
	require.NotEmptyf(t, commits.JSON200.Results, "commit %s has no parents", commit)
	parent := commits.JSON200.Results[0].Parents
	require.Lenf(t, parent, 1, "commit %s must have exactly one parent to diff against", commit)

	paths := make(map[string]string)
	after := apigen.PaginationAfter("")
	for {
		amount := apigen.PaginationAmount(icebergCommitPageSize)
		params := &apigen.DiffRefsParams{Amount: &amount}
		if after != "" {
			params.After = &after
		}
		res, err := cl.DiffRefsWithResponse(t.Context(), e.repo, parent[0], commit, params)
		require.NoErrorf(t, err, "diff %s..%s", parent[0], commit)
		require.Equalf(t, http.StatusOK, res.StatusCode(), "diff %s..%s: %s", parent[0], commit, res.Body)
		require.NotNilf(t, res.JSON200, "diff %s..%s: %s", parent[0], commit, res.Body)
		for _, diff := range res.JSON200.Results {
			paths[diff.Path] = diff.Type
		}
		if !res.JSON200.Pagination.HasMore {
			return paths
		}
		after = apigen.PaginationAfter(res.JSON200.Pagination.NextOffset)
	}
}

// stagedAddresses lists the entries below prefix at commit and returns the physical addresses they
// address. A staged table file entry is created with AddressType=Full (§6.3), so the address is the
// object the client wrote, which is what makes the entry GC-relevant.
func (e *icebergEnv) stagedAddresses(t *testing.T, cl *apigen.ClientWithResponses, commit, prefix string) []string {
	t.Helper()
	addresses := make([]string, 0, 8)
	after := apigen.PaginationAfter("")
	for {
		amount := apigen.PaginationAmount(icebergCommitPageSize)
		objectPrefix := apigen.PaginationPrefix(prefix)
		params := &apigen.ListObjectsParams{Prefix: &objectPrefix, Amount: &amount}
		if after != "" {
			params.After = &after
		}
		res, err := cl.ListObjectsWithResponse(t.Context(), e.repo, commit, params)
		require.NoErrorf(t, err, "list %q at %s", prefix, commit)
		require.Equalf(t, http.StatusOK, res.StatusCode(), "list %q at %s: %s", prefix, commit, res.Body)
		require.NotNilf(t, res.JSON200, "list %q at %s: %s", prefix, commit, res.Body)
		for _, object := range res.JSON200.Results {
			if object.PhysicalAddress == "" {
				continue
			}
			addresses = append(addresses, object.PhysicalAddress)
		}
		if !res.JSON200.Pagination.HasMore {
			return addresses
		}
		after = apigen.PaginationAfter(res.JSON200.Pagination.NextOffset)
	}
}

// icebergTableFileEntryPrefix is the reserved key prefix of a table's file entries, as §6.3 lays it
// out: _lakefs_iceberg/files/<table-uuid>/.
func icebergTableFileEntryPrefix(tableUUID string) string {
	return icebergcatalog.ReservedPrefix + "/" + icebergcatalog.FilesDirectory + "/" + tableUUID + "/"
}

// icebergPointerPath is the reserved key of a table pointer, as the store lays it out:
// _lakefs_iceberg/tables/<namespace levels>/<table>/.pointer.
func icebergPointerPath(namespace []string, table string) string {
	return icebergcatalog.ReservedPrefix + "/" + icebergcatalog.TablesDirectory + "/" +
		strings.Join(namespace, "/") + "/" + table + "/" + icebergcatalog.TablePointerName
}

// icebergRowsJSON encodes rows for the append step.
func icebergRowsJSON(t *testing.T, rows []icebergRow) string {
	t.Helper()
	pairs := make([][2]any, 0, len(rows))
	for _, row := range rows {
		pairs = append(pairs, [2]any{row.ID, row.Name})
	}
	encoded, err := json.Marshal(pairs)
	require.NoError(t, err, "marshal the rows to append")
	return string(encoded)
}

// sortedPathList renders a diff's paths for an assertion message.
func sortedPathList(paths map[string]string) []string {
	list := make([]string, 0, len(paths))
	for path := range paths {
		list = append(list, path)
	}
	sort.Strings(list)
	return list
}

// sortedValueList renders a list of addresses for an assertion message.
func sortedValueList(values []string) []string {
	list := append(make([]string, 0, len(values)), values...)
	sort.Strings(list)
	return list
}

// httpGetString fetches a URL and returns its body as a string: the GC prepare step presigns the
// commits CSV, and reading it is how the test sees the active-commit set the sweep will use.
func httpGetString(t *testing.T, rawURL string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
	require.NoErrorf(t, err, "build GET %s", rawURL)
	resp, err := (&http.Client{Timeout: icebergRequestTimeout}).Do(req)
	require.NoErrorf(t, err, "GET %s", rawURL)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoErrorf(t, err, "read GET %s", rawURL)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "GET %s: %s", rawURL, body)
	return string(body)
}

// icebergAppendScript creates a table when asked and appends rows to it through the catalog, then
// reports the pointer, the table UUID and every object the append made the table reference.
//
// The write goes through the catalog, not around it: PyIceberg's RestCatalog sends the create and
// then an add-snapshot commit to the catalog, which is the request I3-1 turns into lakeFS entries.
// The reported file lists come from the committed metadata after a reload, so they describe what
// the append actually wrote rather than what it intended to write.
const icebergAppendScript = `
import json
import os

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit("missing environment variable " + name)
    return value


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
rows = [{"id": row[0], "name": row[1]} for row in json.loads(required("ICEBERG_ROWS"))]

if os.environ.get("ICEBERG_CREATE") == "1":
    catalog.create_table(identifier, schema=schema, location=required("ICEBERG_TABLE_LOCATION"))
else:
    catalog.load_table(identifier)

table = catalog.load_table(identifier)
table.append(pa.Table.from_pylist(rows, schema=schema))

# Reload: the metadata-location, the current snapshot and the file lists below must describe the
# table after the append, i.e. the commit the catalog just staged.
table = catalog.load_table(identifier)
snapshot = table.metadata.current_snapshot()
# inspect.manifests() returns a list of ManifestFile objects on PyIceberg before 0.12 and a
# PyArrow Table (one row per manifest, the path in the "path" column) on 0.12 and later; read both
# shapes so the step reports the same thing on either client.
inspection = table.inspect.manifests()
if hasattr(inspection, "to_pylist"):
    columns = set(inspection.column_names)
    path_column = "manifest_path" if "manifest_path" in columns else "path"
    manifests = [row[path_column] for row in inspection.to_pylist()]
else:
    manifests = [manifest.manifest_path for manifest in inspection]
data_files = [task.file.file_path for task in table.scan().plan_files()]

print("RESULT:" + json.dumps({
    "metadata_location": table.metadata_location,
    "table_uuid": str(table.metadata.table_uuid),
    "manifest_list": snapshot.manifest_list,
    "manifests": sorted(manifests),
    "data_files": sorted(data_files),
    "ids": sorted(row["id"] for row in table.scan().to_arrow().to_pylist()),
}))
`
