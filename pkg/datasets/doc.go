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
// validate.go, metadata_keys.go), the publish primitive that turns a resolved definition
// into a dataset version (resolve.go, materialize.go), the GC pins of §8.2 (pins.go), the
// control-plane KV persistence layer (store.go, §5.3), the service that owns the dataset
// lifecycle, the validate dry run and the access grants (service.go), the HTTP handlers
// (handlers.go, http.go) and the design-phase spike evidence (S2 and S4) as executable
// tests. Publishing a new version, diffs, proposals, metadata key CRUD and the reconciler
// land in later phases.
package datasets
