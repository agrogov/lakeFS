// Package icebergcatalog implements the lakeFS OSS Apache Iceberg REST Catalog
// (see ICEBERG_CATALOG_PLAN.md). It is served by the lakefs-sso hook at
// /iceberg/api/ (with the relative_to alias /iceberg/relative_to/…/api/), and it
// stores catalog state as ordinary lakeFS entries, so branch, diff, merge, revert
// and commit work on it for free.
//
// Licensing: this package is a clean-room implementation. It contains no code taken
// from lakeFS Enterprise or from upstream lakeFS >= 1.87 (which is BUSL 1.1). The
// inputs are the Apache Iceberg format spec, the Apache Iceberg REST OpenAPI spec,
// the public lakeFS documentation and ICEBERG_CATALOG_PLAN.md only.
//
// Apache Iceberg REST OpenAPI spec (Apache-2.0):
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
//
// Dependency rule: the metadata model comes from github.com/apache/iceberg-go,
// pinned in go.mod. Only its model is used — table.ParseMetadataBytes,
// table.MetadataBuilderFromBase, table.UpdateTableMetadata, table.Updates,
// table.Requirements, table.Requirement.Validate and table.NewMetadataWithUUID.
// Its write helpers live in catalog/internal and cannot be imported, so writing
// metadata.json is this package's own code.
//
// The package is built layer by layer (ICEBERG_CATALOG_PLAN.md section 4):
//
//   - http.go, router.go — the chi router, the authentication middleware and the
//     error model;
//   - ident.go — namespace and identifier parsing (repo.ref.ns….table, %1F,
//     relative_to);
//   - authz.go — catalog:* checks on CatalogNamespaceArn and CatalogTableArn;
//   - store.go — namespace markers and table pointers as graveler entries, with
//     conditional writes;
//   - commit.go — requirement validation, update application via iceberg-go,
//     metadata.json write and the compare-and-swap of the table pointer;
//   - register.go — staging of table files as lakeFS entries (GC safety);
//   - creds.go — vended credentials and remote signing.
//
// Today the package holds only this doc comment: phase I-0 pinned the dependency
// and proved the update/requirement coverage in updates_coverage_test.go (see
// ICEBERG_SPIKE_S-I1.md), and the files above land in phases I-1 to I-4.
package icebergcatalog
