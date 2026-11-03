# Iceberg REST catalog conformance

The two external conformance suites of ICEBERG_CATALOG_PLAN.md §12, and the I-2 exit gate:

| Suite | Driver | What it is |
|---|---|---|
| iceberg-go `catalog/catalogtest` | `external_conformance_test.go` (this directory) | Go-native. iceberg-go's own specification of what a catalog must do, driven against a running lakefs-sso over the network. |
| Apache REST Compatibility Kit (RCK) | Gradle, from an [apache/iceberg](https://github.com/apache/iceberg) checkout | The reference Java suite for the REST spec. Not Go, so it is documented here rather than wired into `go test`. |

Neither suite is part of `make test`: both need a lakefs-sso server, a repository and a credential.
Both are skipped, not failed, when they are not configured; see each section for what "configured"
means.

- [1. iceberg-go catalogtest](#1-iceberg-go-catalogtest)
- [2. Apache RCK](#2-apache-rck)
- [3. Recorded runs](#3-recorded-runs)
- [4. Clean-room and licensing](#4-clean-room-and-licensing)

## 1. iceberg-go catalogtest

### 1.1 What it needs

A lakefs-sso with the catalog enabled (`iceberg.enabled: true`), a repository, and a credential
that may write to it. Two constraints the suite imposes on the setup:

- **The URI must be the `relative_to` form, with the branch fixed by the URL.** catalogtest names
  every namespace it creates after nothing but a UUID, so nothing in the client's identifier can
  name the repository and the ref. `/iceberg/relative_to/<repo>.<branch>/api` fixes them in the
  URL, which is exactly what §5 rows 4 and 5 are for. Pointing the suite at `/iceberg/api` would
  make it read the UUID as a repository name and answer `404 NoSuchWarehouseException`; the test
  says so instead of failing 18 times.
- **The ref must be a branch.** Catalog writes are staged on a branch and take effect on an
  explicit lakeFS commit (§6.2). A tag or a commit ID is read-only (Q-I4) and the write subtests
  would answer `403`.

The suite stages namespaces and tables on the branch. It is not required to commit; the staged
entries are what the conformance subtests read back, and every subtest drops what it created.

### 1.2 Preparing a server

Any lakefs-sso works. This is the local recipe the recorded run used (quickstart credentials,
local blockstore, catalog on).

**It must run in a policy-capable auth mode.** With the default auth mode the server exits at startup
(`icebergcatalog: read policy iceberg-reserved-prefix-protect: not implemented`, defect D-3 in
[`RESULTS.md`](RESULTS.md) §10), so the recipe below writes a `lakefs.yaml` whose `sso:` section selects
the ACL auth service. `RESULTS.md` §3 has the details and the stub OIDC issuer that section needs;
without such a section nothing below runs.

```shell
# from the repository root
# lakefs.yaml: database/blockstore local, iceberg.enabled, and the sso: section of RESULTS.md §3.
go build -o /tmp/lakefs-sso ./cmd/lakefs-sso

/tmp/lakefs-sso run --config /tmp/lfsconf/config.yaml --quickstart &

# The repository's storage namespace must be inside blockstore.local.path, or the metadata
# writes fail: the local adapter refuses a path outside the adapter's root.
curl -u AKIAIOSFOLQUICKSTART:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  -X POST http://127.0.0.1:8000/api/v1/repositories \
  -H 'Content-Type: application/json' \
  -d '{"name":"conformance","storage_namespace":"local:///tmp/lfsconf/block/conformance",
       "default_branch":"main","exist_ok":false}'
```

### 1.3 Running

```shell
REST_CATALOG_URI=http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api \
REST_CATALOG_CREDENTIAL=AKIAIOSFOLQUICKSTART:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
REST_CATALOG_WAREHOUSE=conformance.main \
  go test -count=1 -v ./pkg/icebergcatalog/conformance/
```

| Variable | Meaning | lakeFS value |
|---|---|---|
| `REST_CATALOG_URI` | The catalog URI the client is configured with. | `https://<lakefs-sso>/iceberg/relative_to/<repo>.<branch>/api` |
| `REST_CATALOG_CREDENTIAL` | `<access_key_id>:<secret_access_key>`, the pair the client exchanges at `POST /v1/oauth/tokens` (RFC 6749 §4.4.2, §7.1). | A lakeFS access key pair with `catalog:*` on the repository. |
| `REST_CATALOG_WAREHOUSE` | The `warehouse` client property. Sent to `GET /v1/config` as the spec's `warehouse` parameter. | `<repo>.<branch>`, the same scope the URI names. |

`REST_CATALOG_WAREHOUSE` is a name lakeFS does not act on: the catalog is addressed by the
identifier and `GET /v1/config` ignores the query parameter (§5, Q-I3). It stays required, and the
test checks that the URI names the same warehouse, because the same harness has to run against
servers that *do* scope by warehouse, and a warehouse that disagrees with the URI would describe a
scope the requests never reach.

All three variables must be set together. Unset means "no server to run against" and skips; a
partially configured invocation is a mistake in the command line, not an absent suite, and fails
with the names of the missing variables - a suite that skips over a typo reads like a suite that
passed.

## 2. Apache RCK

The RCK is the Java reference suite; it lives in the iceberg checkout under `open-api`, and drives
a REST catalog through the Java client. It is a compatibility kit, not a client test: it asserts
the reference `CatalogTests` behaviours against an arbitrary server
([RCK README](https://github.com/apache/iceberg/blob/main/open-api/README.md)).

### 2.1 Preparing a server

The same repository as §1.2, with one addition: the RCK's `warehouse` property is read by the
*client* only, so the URI carries the scope exactly as above. Views are not served by this catalog
(§1.2 of the plan), so the view half of the suite cannot pass - see §2.4.

### 2.2 Running

From an [apache/iceberg](https://github.com/apache/iceberg) checkout:

```shell
./gradlew :iceberg-open-api:test --tests RESTCompatibilityKitSuite \
  -Drck.local=false \
  -Drck.requires-namespace-create=true \
  -Drck.supports-names-with-dot=false \
  -Drck.uri=http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api \
  -Drck.warehouse=conformance.main \
  -Drck.credential=<access_key_id>:<secret_access_key>
```

| Flag | Why this value |
|---|---|
| `-Drck.local=false` | The default (`true`) starts the suite's own local REST server on port 8181 and tests *that*. `false` is what points the suite at an external server; without it the run says nothing about lakeFS. |
| `-Drck.requires-namespace-create=true` | lakeFS has no implicit namespaces: a table needs a namespace marker created first (§6.1, I2-2). |
| `-Drck.supports-names-with-dot=false` | The plan's RCK flag (§5, §14). lakeFS namespace levels are the components of a `<repo>.<ref>.<ns>...` address, so a namespace level containing a dot cannot round-trip: it would decode as more levels than the client sent. `false` tells the suite not to expect dot-names to work. This is a lakeFS addressing limit, not an RCK gap. |

Every `rck.` property is also accepted as an environment variable with the `CATALOG_` prefix
(`CATALOG_URI`, `CATALOG_WAREHOUSE`, `CATALOG_CREDENTIAL`, `CATALOG_IO__IMPL`, ...), with `_` → `.`
and `__` → `-`; use that form when the credential must not appear in the process list.

`-Drck.io-impl=<FileIO>` may be needed for the subtests that write manifests through the client:
they write them under the table location the server returns, so the FileIO has to understand the
scheme of `<storage_namespace>` (S3: `org.apache.iceberg.aws.s3.S3FileIO`; a local blockstore:
leave it at the default HadoopFileIO).

### 2.3 Allowlist: the two tests that hard-code local paths

Two RCK tests assume the client process can reach the table's metadata as a *local file*, so they
cannot pass against any server whose storage is not the test's own filesystem:

| Test | Why it cannot pass against an external server |
|---|---|
| `org.apache.iceberg.catalog.CatalogTests#testLoadTableWithMissingMetadataFile` | It renames the metadata file the catalog returned, on the local filesystem, with `java.nio.file.Files.move`, and asserts the load failure names that path. The metadata of a lakeFS table lives in object storage under `<storage_namespace>/_iceberg/<table-uuid>/metadata/`; there is no local file to move, so the first `Files.move` fails. |
| `org.apache.iceberg.view.ViewCatalogTests#createViewWithCustomMetadataLocation` | Both the location and the `write.metadata.path` it asserts on are built from JUnit's `@TempDir` (`viewLocation()` → `tempDir.toFile().toURI()`), i.e. a hard-coded `file:` path, and the test then requires the created view's `metadataFileLocation()` to start with it. |

Exclude them by method name rather than by index, so a test that gets renamed fails loudly instead
of silently sliding into the allowlist:

```shell
# rck-exclude.gradle.kts - pass with -I rck-exclude.gradle.kts
allprojects {
  tasks.withType<Test>().configureEach {
    filter {
      excludeTestsMatching("*testLoadTableWithMissingMetadataFile")
      excludeTestsMatching("*createViewWithCustomMetadataLocation")
    }
  }
}
```

```shell
./gradlew :iceberg-open-api:test --tests RESTCompatibilityKitSuite \
  -Drck.local=false -Drck.requires-namespace-create=true -Drck.supports-names-with-dot=false \
  -Drck.uri=... -Drck.warehouse=... -Drck.credential=... \
  -I rck-exclude.gradle.kts
```

Equivalently, add the same `filter { }` block to the `test` task in `open-api/build.gradle` when
running from a local checkout.

**Shrinking the allowlist.** Both entries are properties of the test, not of lakeFS, so they are
re-evaluated with every Iceberg release:

- `testLoadTableWithMissingMetadataFile` can be dropped as soon as the test resolves the metadata
  location through the catalog's FileIO (or the suite gains a flag for "metadata is not a local
  file") instead of `Files.move`.
- `createViewWithCustomMetadataLocation` can be dropped once views are served at all (a v1 non-goal,
  §1.2) *and* the test stops deriving the location from `@TempDir`.

Check by deleting one entry and running the suite; a reintroduced test that now passes is the
signal to delete the entry permanently.

### 2.4 Two open decisions this suite will surface

Recorded here so a future run is not mistaken for a regression:

1. **Requested table locations.** Several catalog subtests create a table with an explicit
   `location` of `file:/tmp/<ns>/<table>` and, unless `rck.overrides-requested-location=true`,
   assert that the server kept it. This catalog accepts a requested location only inside
   `<storage_namespace>/_iceberg/` and answers 400 outside it (I2-4, §13 "client writes outside the
   table location"); `overrides-requested-location=true` only skips the client's assertion, it does
   not stop the location being sent, so those subtests fail with 400 either way. Either the catalog
   learns to accept-and-ignore a requested location, or the subtests have to be excluded explicitly.
2. **Views.** `RESTCompatibilityKitSuite` selects `RESTCompatibilityKitViewCatalogTests` as well as
   the catalog tests, and views are a v1 non-goal that this catalog does not advertise (§1.2). Use
   `--tests RESTCompatibilityKitCatalogTests` to run only the catalog half, or wait for views.

## 3. Recorded runs

**The current record is [`RESULTS.md`](RESULTS.md).** It has the environment, the exact commands, the
tool versions, the pass/fail result of each suite, the defects found and the gaps. Read it before
believing anything below, and update it, not this section, when you run a suite.

The latest recorded outcome, in one line each:

| Suite | Result |
|---|---|
| `go vet`, `go test ./pkg/icebergcatalog/...` | pass |
| iceberg-go catalogtest (`REST_CATALOG_*` set) | **fail — 0 of 18 subtests**; the `relative_to` middleware is never mounted (`http.go`), so the client's namespace arrives un-prefixed |
| Apache RCK | not run — no JDK/Gradle/apache-iceberg checkout on the recording host |
| esti (`esti/iceberg_test.go`) | attempted, **all 5 tests skipped** — no docker daemon and no S3-compatible store; no assertion executed |

### 3.1 Superseded notes from the first attempt

Kept because the analysis is still worth reading, with what is now known to be out of date marked:

- **OAuth token endpoint (superseded).** The first attempt failed with `401` on
  `POST /v1/oauth/tokens` because `http.go` still built `api.GenericAuthMiddleware`. That is fixed:
  the endpoint answers `200` with a bearer token (`RESULTS.md` §1).
- **Catalog startup (new, and it comes first now).** With the default auth mode, enabling the catalog
  makes the server exit at startup with
  `icebergcatalog: read policy iceberg-reserved-prefix-protect: not implemented`. Any run — including
  §1.2 above and §2.2 below — has to start `lakefs-sso` in a policy-capable auth mode; see
  `RESULTS.md` §3, which also gives the workaround the recorded run used. Defect D-3 in
  `RESULTS.md` §10.
- **`relative_to` is not mounted (still open).** `http.go` mounts the bare handler, not
  `RelativeTo(handler)`, so the warehouse segment is ignored. Until that is fixed every subtest of the
  Go suite and every DuckDB read fails on the first request; defect D-1.
- **Namespace in the request body (still presumed open, not re-measured).** The rewrite covers the
  `{namespace}` path parameter and the `parent` query parameter, not the namespace inside the JSON body
  of `POST /v1/namespaces`; defect D-2.
- **ListNamespaces under `relative_to` (predicted, not measured).** Responses are not un-prefixed, so
  the client sees `[repo, ref, ns]` where it sent `[ns]`; defect D-4.
- **An earlier run with local patches applied outside the change reported 17 of 18 subtests passing.**
  That number was never committed and is **not** reproducible from this tree; do not cite it.

## 4. Clean-room and licensing

Written from ICEBERG_CATALOG_PLAN.md §5, §6, §12, the Apache Iceberg REST OpenAPI spec
(<https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>), the REST
protocol notes (<https://github.com/apache/iceberg/blob/main/docs/docs/rest-protocol.md>), the RCK
README (<https://github.com/apache/iceberg/blob/main/open-api/README.md>), RFC 6749 §4.4 and the
public lakeFS documentation. No lakeFS Enterprise code and no upstream lakeFS >= 1.87 (BSL) code was
used.

iceberg-go is Apache-2.0 and is already a dependency (go.mod); the RCK is Apache-2.0 and is run from
an apache/iceberg checkout, so nothing from it is vendored here.
