// Package datasets implements lakeFS Datasets (see DATASETS_DESIGN.md).
//
// Licensing: this package is a clean-room implementation. It contains no code, SQL,
// protobuf definitions or UI assets taken from lakeFS Enterprise or from upstream lakeFS
// >= 1.87 (which is BUSL 1.1). Inputs used for this package are DATASETS_DESIGN.md and the
// public lakeFS documentation only.
//
// Dependency rule (DATASETS_DESIGN.md section 4.6): nothing outside the fork-owned entry
// points (the lakefs-sso RoutesBuilder, cmd/lakectl-datasets) may import this package, and
// this package may import pkg/catalog, pkg/graveler, pkg/auth, pkg/block, pkg/kv,
// pkg/permissions and pkg/api (only for GenericAuthMiddleware).
//
// The package holds the definition model and metadata validation (definition.go,
// validate.go, metadata_keys.go), the control-plane KV persistence layer (store.go, §5.3),
// authorization and bootstrap (authz.go, bootstrap.go), the OpenAPI spec and its generated
// server stubs (api/datasets.yml, apigen, http.go, handlers.go), materialisation
// (resolve.go, materialize.go), the GC pins with degraded detection of §8.2 (pins.go), the
// service that owns the dataset lifecycle including publish/versions and the validate dry
// run (service.go), the version diff of §10.3 (diff.go), the proposals of §5.6 with their
// preview branches (proposals.go), and the design-phase spike evidence (S2 and S4) as
// executable tests. Metadata-key CRUD, the reconciler, the CLI and the remaining UI pages
// land in later phases.
package datasets
