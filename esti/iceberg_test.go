// Package esti holds the lakeFS system tests.
//
// iceberg_test.go is the Iceberg REST catalog read-path smoke test of ICEBERG_CATALOG_PLAN.md
// §11 I-1 "exit criteria" and §11 I-2 task I2-8: a table that was written *outside* the catalog
// is adopted with the catalog's register operation (I2-6) and then read back by the two published
// client recipes, PyIceberg through /iceberg/api and DuckDB through
// /iceberg/relative_to/{repo}.{branch}/api (§1.1 goals 1-3, §5 "Addressing", §12 "Testing / Esti").
//
// The scenario is one straight line, because each step is a property the read path has to have:
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
// The clients run in docker (see iceberg_utils.go), and the suite skips - it never fails - when
// the environment cannot host it: no S3 blockstore, no docker daemon, no way to install the
// clients, or a catalog that does not advertise register.
//
// I3-3 extends this file with the write path (append, commit, GC, branch/merge) once the file
// staging of I3-1 and the uncommitted-GC grace of I3-2 are in place.
//
// Clean-room note (ICEBERG_CATALOG_PLAN.md §2, DATASETS_DESIGN.md §2): written from
// ICEBERG_CATALOG_PLAN.md §5, §6.1, §11, §12, the Apache Iceberg REST OpenAPI catalog spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml), the Apache
// Iceberg table spec (https://iceberg.apache.org/spec/#table-metadata), the public PyIceberg and
// DuckDB documentation (https://py.iceberg.apache.org/, https://duckdb.org/docs/stable/core_extensions/iceberg/overview)
// and the public lakeFS documentation (https://docs.lakefs.io/iceberg/). No lakeFS Enterprise code
// and no upstream lakeFS >= 1.87 (BSL 1.1) code was used.
package esti

import (
	"testing"

	"github.com/stretchr/testify/require"
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
