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
// The package currently holds the design-phase spike evidence (S2 and S4) as executable
// tests, the definition model and metadata validation, and the control-plane KV
// persistence layer (store.go, §5.3); the service itself lands in later phases.
package datasets
