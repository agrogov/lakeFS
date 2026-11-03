// Package iceberglayout declares the on-branch key layout of the lakeFS Apache Iceberg
// catalog: the reserved prefix, the directories under it, the marker file names and the
// table-pointer user-metadata keys (ICEBERG_CATALOG_PLAN.md §6.1, §6.3).
//
// It is a leaf package on purpose, and that is its whole reason to exist.
// pkg/icebergcatalog owns this layout and re-exports every constant below under its
// original name, so nothing outside this file has to care where the values live.
// pkg/datasets materialises catalog workspaces and must build the same keys
// (materialize.go), so it needs the values too, but it must not import
// pkg/icebergcatalog: pkg/icebergcatalog's own tests import pkg/datasets to check the
// §7.2 authorization matrix against a real auth service (authz_test.go), and an
// internal test package makes
//
//	pkg/icebergcatalog[test] -> pkg/datasets -> pkg/icebergcatalog
//
// an import cycle, which fails the whole test package at setup. Holding the shared
// values here - reachable from both, importing neither - breaks the cycle without
// moving a single test out of the package it belongs to.
//
// Nothing in this package may import pkg/icebergcatalog or pkg/datasets; that would
// restore the cycle. It holds names, not logic: the helpers that turn validated
// identifiers into keys stay in pkg/icebergcatalog (store.go, files.go), which also
// keeps the validation that makes a key unambiguous.
package iceberglayout

const (
	// ReservedPrefix is the reserved key prefix that holds all catalog state on the branch.
	// The catalog hides it from fs: writes with a deny statement, the way datasets protects
	// _lakefs_ds_pin_* (ICEBERG_CATALOG_PLAN.md §6.1).
	ReservedPrefix = "_lakefs_iceberg"

	// NamespaceDirectory holds one .namespace marker per namespace.
	NamespaceDirectory = "ns"

	// TablesDirectory holds one .pointer entry per table.
	TablesDirectory = "tables"

	// FilesDirectory is the subdirectory of the reserved prefix that holds one entry per table
	// file (ICEBERG_CATALOG_PLAN.md §6.3):
	//
	//	_lakefs_iceberg/files/<table-uuid>/<path relative to the table root>
	//
	// It is the third directory of the reserved area, next to NamespaceDirectory ("ns") and
	// TablesDirectory ("tables"), and like them it is hidden from fs: writes by the deny
	// statement of pkg/icebergcatalog gc.go.
	FilesDirectory = "files"

	// NamespaceMarkerName is the file name of a namespace marker.
	NamespaceMarkerName = ".namespace"

	// TablePointerName is the file name of a table pointer.
	TablePointerName = ".pointer"

	// MetadataLocationKey is the pointer user-metadata key that holds the physical URI of the
	// current metadata.json. It is the compare-and-swap field of a commit: the write half
	// conditions on this exact value (§6.2 step 6).
	MetadataLocationKey = "iceberg.metadata-location"

	// TableUUIDKey is the pointer user-metadata key that holds the table UUID, the same UUID
	// metadata.json carries as table-uuid. Clients use it to detect a table that was dropped
	// and recreated under the same name. It is also the one path component of the staged-file
	// prefix, so materialisation reads it out of a source pointer (pkg/datasets
	// icebergEntryTableUUID).
	TableUUIDKey = "iceberg.table-uuid"

	// FormatVersionKey is the pointer user-metadata key that holds the Iceberg format version,
	// 1 or 2. Version 3 is out of scope for v1 (ICEBERG_CATALOG_PLAN.md §1.2), so a pointer that
	// advertises it is rejected instead of being served by code that does not implement it.
	FormatVersionKey = "iceberg.format-version"
)
