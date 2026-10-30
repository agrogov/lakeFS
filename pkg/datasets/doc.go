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
// The package holds, layer by layer:
//
//   - the configuration section and its validation (config.go, §13);
//   - the definition model with metadata validation (definition.go, validate.go,
//     metadata_validate.go, metadata_keys.go);
//   - the control-plane KV store (store.go, §5.3);
//   - authorization and bootstrap: actions, ARNs, per-dataset managed policies and grants
//     (authz.go), and the installation-wide policies and groups (bootstrap.go, §9);
//   - the OpenAPI spec, its generated stubs and the HTTP plumbing (api/datasets.yml,
//     apigen, http.go, handlers.go, §10);
//   - materialisation (resolve.go, materialize.go, §6);
//   - the service that owns the dataset lifecycle, publish/versions and the validate dry run
//     (service.go), the version diff of §10.3 (diff.go) and the proposals of §5.6 with their
//     preview branches (proposals.go);
//   - the GC pin manager, including degraded-source detection (pins.go, §8.2);
//   - the background reconciler that repairs state, pins and policies (reconciler.go, §12.3);
//   - the metrics of §12.4 (metrics.go);
//   - the design-phase spike evidence (S2 and S4) as executable tests.
//
// The metadata-key records are persisted by store.go; the CLI and the UI pages live outside
// this package (cmd/lakectl-datasets, webui/src/pages/datasets).
package datasets
