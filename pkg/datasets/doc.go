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
// The package currently holds five layers. The definition model and its metadata
// validation live in definition.go, validate.go and metadata_keys.go (§5.1-§5.2,
// §5.5). The control-plane KV persistence layer lives in store.go (§5.3-§5.4). The
// authorization building blocks and bootstrap policies live in authz.go and
// bootstrap.go (§9). The control-plane OpenAPI spec (api/datasets.yml) and its
// generated stubs (pkg/datasets/apigen, wired by http.go and handlers.go) define the
// REST surface of §10. The materialisation primitive in resolve.go and materialize.go
// resolves refs, merges the source iterators and commits and tags a dataset version
// (§6.2-§6.4), as the library the service layer will call. The design-phase spike
// evidence (S2 and S4) remains as executable tests.
package datasets
