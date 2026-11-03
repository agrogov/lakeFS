// Package importer imports Apache Iceberg tables into lakeFS from another Iceberg catalog.
//
// The importer is a client of two Iceberg REST catalogs and of the lakeFS object API, and of
// nothing else:
//
//   - the *source* is any catalog that speaks the Apache Iceberg REST protocol. Glue, Hive and
//     Nessie are reached through their own REST endpoints, so no vendor SDK is needed. The
//     client here implements the four operations an import needs - GET /v1/config, list
//     namespaces, list tables and load table - and the OAuth2 client-credentials flow
//     (POST /v1/oauth/tokens) for catalogs that require a token.
//   - the *target* is the lakeFS Iceberg REST catalog mounted at <lakefs-server>/iceberg/api
//     (ICEBERG_CATALOG_PLAN.md §4.1). Tables are adopted with the catalog's register operation,
//     POST /v1/{prefix}/namespaces/{namespace}/register, after their namespaces are created with
//     POST /v1/{prefix}/namespaces. The importer never touches the catalog's storage directly and
//     never imports pkg/icebergcatalog: every write is one of those two HTTP requests.
//
// # Metadata locations and the two modes
//
// Register reads metadata.json, the manifest lists and the manifests with the *lakeFS server's*
// storage credentials, so the catalog requires the metadata location to be inside the
// repository's storage namespace and the table's files to be inside
// <storage_namespace>/_iceberg/ (pkg/icebergcatalog/register.go and files.go). A table that lives
// in another catalog's warehouse therefore cannot be registered where it is.
//
// The importer has two modes, and the mode is an explicit choice because the difference is
// visible in the repository:
//
//   - without Copy, a table is registered in place. The source metadata location and the table's
//     `location` are checked against the repository storage namespace first, and a table whose
//     files are foreign is reported as failed with a message that names --copy. This is the
//     cheap, no-duplication mode, and the one to use when the source and lakeFS share a
//     warehouse (for example when importing from another lakeFS catalog).
//   - with Copy, the table's metadata and every file its snapshots reference are copied into
//     <storage_namespace>/_iceberg/<new-table-uuid>/ through the lakeFS object API, the absolute
//     locations stored in the metadata document, the manifest lists and the manifests are
//     rewritten to the new root, and the rewritten document is registered. Copying is
//     content-preserving for data files and re-encodes only the Avro manifest and manifest-list
//     files (the spec's OCF format), because the paths inside them are absolute.
//
// # What copy mode changes in the metadata document
//
// The new table is a table of its own, so it gets its own table UUID - derived deterministically
// from the source metadata location and the target identifier, which keeps a resumed import on the
// same location and gives two target tables of one source table distinct
// _lakefs_iceberg/files/<uuid>/ entries. The document's `location`, every snapshot's
// `manifest-list` (and the v1 inline `manifests`), and the file paths in the manifests are
// rewritten. `metadata-log`, `statistics` and `partition-statistics` are dropped: they point at
// files the register operation does not stage, so a copied table must not claim them. Everything
// else - schemas, partition specs, sort orders, snapshots, refs, snapshot-log and properties - is
// preserved.
//
// # Failure semantics
//
// Import is per table. A table that cannot be imported - format version 3, a foreign location in
// the default mode, an unreadable source file, a failed register - is reported as failed and the
// import continues with the next table. Nothing is committed: register and namespace create stage
// entries on the branch, and an explicit lakeFS commit publishes them
// (ICEBERG_CATALOG_PLAN.md §6.2) - which is also what makes a partially completed import safe to
// resume: an already-registered table is skipped unless --overwrite is set.
package importer
