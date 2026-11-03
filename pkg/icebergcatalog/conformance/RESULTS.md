# Live-client evidence for the Iceberg REST catalog — recorded runs

This file is the record of what was **actually executed** against a real `lakefs-sso` serving the
Apache Iceberg REST catalog, on which machine, with which tool versions, and with which result.
It exists because the previous state of the tree had no live evidence at all: `ICEBERG_CATALOG_PLAN.md`
§15.4 says so ("no green conformance run is recorded") and `README.md` §3 records a run that failed
before it reached the catalog, on the OAuth token endpoint.

Where a thing did not run, or did not pass, this file says so and gives the reason. Rows marked
**measured** were produced by a command whose output is quoted or summarised here. Where a conclusion
comes from reading the code rather than from a run — D-2 and D-4 in §10, and the GC consequence in
N-3 — the entry says so, and those three are the only such claims in this file.

- [1. Environment](#1-environment)
- [2. What was run, and the result](#2-what-was-run-and-the-result)
- [3. The catalog server: it does not start in the default auth mode](#3-the-catalog-server-it-does-not-start-in-the-default-auth-mode)
- [4. iceberg-go `catalogtest` — 0 of 18 subtests pass](#4-iceberg-go-catalogtest--0-of-18-subtests-pass)
- [5. The write/read path over `/iceberg/api` — measured working](#5-the-writeread-path-over-icebergapi--measured-working)
- [6. PyIceberg 0.12.0 — full scenario, measured](#6-pyiceberg-0120--full-scenario-measured)
- [7. DuckDB 2.0.0 — attach works, queries do not](#7-duckdb-200--attach-works-queries-do-not)
- [8. Apache RCK — not run](#8-apache-rck--not-run)
- [9. esti (`esti/iceberg_test.go`) — attempted, skipped](#9-esti-estiiceberg_testgo--attempted-skipped)
- [10. Defects to file](#10-defects-to-file)
- [11. Known gaps](#11-known-gaps)
- [12. Reproducing this](#12-reproducing-this)

## 1. Environment

| Item | Value |
|---|---|
| Host | macOS 26.7.2, arm64 (Darwin), no docker daemon, no JDK |
| Go toolchain | `go1.26.4 darwin/arm64` (`$HOME/sdk/go1.25.8/bin/go` with `GOTOOLCHAIN=auto`, which upgrades to 1.26.4 per `go.mod`) |
| Tree | `66d2d170e` — "icebergcatalog: resolve compaction conflicts in real merges (I5-1)" |
| Server | `lakefs-sso` built from that tree: `go build -o /tmp/evidence/lakefs-sso ./cmd/lakefs-sso` |
| Server backend | `blockstore.type=local`, `database.type=local` (no S3 and no Postgres available; see §9 and §11) |
| iceberg-go | `v0.7.0` (`go.mod`) — the conformance driver |
| PyIceberg | `0.12.0`, pyarrow `26.0.1`, Python `3.11.18` |
| DuckDB | `2.0.0` (Python module), iceberg extension `4c0c0874c4` |
| JDK / Gradle | none (`/usr/bin/java` is the macOS stub: "Unable to locate a Java Runtime") |

The `lakefs-sso` server for the measurements below:

```shell
mkdir -p /tmp/lfsconf/{kv,block}
# config.yaml is §12's file; iceberg.enabled=true, blockstore/database local, plus the sso:
# section of §3 (which is a workaround for a defect, see there)
/tmp/evidence/lakefs-sso run --config /tmp/lfsconf/config.yaml --quickstart

curl -u AKIAIOSFOLQUICKSTART:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  -X POST http://127.0.0.1:8000/api/v1/repositories \
  -H 'Content-Type: application/json' \
  -d '{"name":"conformance","storage_namespace":"local:///tmp/lfsconf/block/conformance",
       "default_branch":"main","exist_ok":false}'      # -> 201
```

The OAuth token endpoint, which is what `README.md` §3 recorded as the first blocker, now answers:

```shell
curl -u AKIAIOSFOLQUICKSTART:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  -X POST http://127.0.0.1:8000/iceberg/api/v1/oauth/tokens \
  -d 'grant_type=client_credentials&scope=catalog'
# HTTP 200
# {"access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...","token_type":"bearer",
#  "expires_in":3600,"issued_token_type":"urn:ietf:params:oauth:token-type:access_token"}
```

That finding of `README.md` §3 (401 on `POST /v1/oauth/tokens` because `http.go` still built
`api.GenericAuthMiddleware`) is **fixed** in this tree: `http.go:109` builds the catalog's own
middleware. `README.md` §3 is superseded by §4 below for everything else it recorded.

## 2. What was run, and the result

| # | Suite / check | Result |
|---|---|---|
| 1 | `go build ./cmd/lakefs-sso` | **pass** |
| 2 | `go vet ./pkg/icebergcatalog/... ./esti/` | **pass** |
| 3 | `go test ./pkg/icebergcatalog/...` (unit) | **pass** (`ok ... 16.1s`, `ok .../conformance 0.79s` — the suite skips, no server configured) |
| 4 | Server start, default auth mode, `iceberg.enabled=true` | **FAIL — server exits** (§3, defect D-3) |
| 5 | Server start, ACL auth mode (`sso.enabled`), `iceberg.enabled=true` | **pass** (§3) |
| 6 | iceberg-go `catalogtest` over `/iceberg/relative_to/<repo>.<branch>/api` | **FAIL — 0 of 18 subtests pass** (§4, defect D-1) |
| 7 | `/iceberg/api` (identifier form): namespace/table/properties/commit over raw HTTP | **pass** (§5) |
| 8 | PyIceberg 0.12.0: create, append, lakeFS commit, branch, merge, tag read, drop, register, GC-prepare | **pass** (§6) |
| 9 | PyIceberg 0.12.0 through `/iceberg/relative_to/...` | **FAIL** on every namespace-scoped call (§6.2, defect D-1) |
| 10 | DuckDB 2.0.0: extension install + `ATTACH` | **pass** |
| 11 | DuckDB 2.0.0: relation read / table listing | **FAIL** (§7, defect D-1) |
| 12 | Apache RCK | **not run — no JDK, no Gradle, no apache/iceberg checkout** (§8) |
| 13 | esti `esti/iceberg_test.go` | **attempted, all 5 tests SKIP** — no docker daemon, no S3-compatible store (§9) |

So: the catalog's read and write paths work as a client experiences them through `/iceberg/api`, and
the `relative_to` addressing form — one of the two forms `docs/iceberg.md` publishes and the only one
DuckDB and Starburst can use — is not mounted. The I-2 exit gate (iceberg-go `catalogtest` green) is
**not met**.

## 3. The catalog server: it does not start in the default auth mode

**Measured.** With the catalog enabled and the default auth mode (no `sso:` section, which is what
`esti/iceberg_utils.go` used to generate and what `README.md` §1.2 documents):

```shell
LAKEFS_DATABASE_LOCAL_PATH=/tmp/lfsconf/kv LAKEFS_BLOCKSTORE_LOCAL_PATH=/tmp/lfsconf/block \
LAKEFS_ICEBERG_ENABLED=true /tmp/evidence/lakefs-sso run --quickstart
```

```
level=info msg="starting HTTP server" listen_address="0.0.0.0:8000"
level=fatal msg="extra routes" error="icebergcatalog: read policy iceberg-reserved-prefix-protect: not implemented"
```

`GET /api/v1/healthcheck` never answers (connection refused). The cause is defect **D-3** (§10):
`pkg/icebergcatalog/bootstrap.go:149` `EnsureReservedPrefixProtectPolicy` treats every error other
than `ErrNotFound` from `authService.GetPolicy` as fatal (`bootstrap.go:169`), and with the default
auth mode `lakefs-sso` runs `auth.BasicAuthService`, whose policy methods return
`auth.ErrNotImplemented` (`pkg/auth/basic_service.go:424-464`). Enabling the catalog therefore kills
the server for every installation that has not configured an external auth service or SSO.

**Workaround used for every measurement below** (it is a workaround, not a fix): run `lakefs-sso` in
its ACL auth mode, which does implement `GetPolicy`/`WritePolicy`/`AttachPolicyToGroup`
(`contrib/auth/acl/service.go:745`). That mode is only selected when `sso.enabled` is true, and its
OIDC-aware authentication service discovers the issuer at startup, so the run points it at a stub
discovery document served by a local process:

```yaml
sso:
  enabled: true
  client_id: "esti-iceberg"
  client_secret: "esti-iceberg-client-secret"
  issuer_url: "http://127.0.0.1:9999"      # a 30-line stub serving /.well-known/openid-configuration
  callback_base_url: "http://127.0.0.1:8000"
```

With that, the server starts and installs its controls:

```
level=info msg="iceberg catalog: reserved-prefix deny attached, uncommitted-GC grace and merge hook installed"
  policy=iceberg-reserved-prefix-protect uncommitted_gc_grace_s=86400 ignore_compaction_commits=true
```

No OIDC login is performed anywhere in these runs: every client authenticates with a lakeFS access
key pair (`credential=<key>:<secret>` → `POST /v1/oauth/tokens`, or HTTP basic), exactly as
`docs/iceberg.md` documents.

`esti/iceberg_utils.go` now generates the same `sso:` section (plus an in-process discovery stub,
`icebergOIDCIssuer`), with a comment naming D-3 as the reason and the condition for deleting it.
**Caveat:** that change is compile- and vet-verified only; the esti suite could not be executed here
(§9), so the harness change itself has no live evidence — only its two ingredients were measured
separately: this §3 server start, and a `POST /api/v1/setup_lakefs` in the same auth mode, which
answered `200` with the esti admin credentials and left the catalog answering `200` on
`GET /iceberg/api/v1/config`.

## 4. iceberg-go `catalogtest` — 0 of 18 subtests pass

**Measured.** Exactly the command `README.md` §1.3 documents:

```shell
REST_CATALOG_URI=http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api \
REST_CATALOG_CREDENTIAL=AKIAIOSFOLQUICKSTART:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
REST_CATALOG_WAREHOUSE=conformance.main \
  go test -count=1 -v ./pkg/icebergcatalog/conformance/
```

```
--- FAIL: TestExternalCatalogConformance (0.02s)
    --- FAIL: TestExternalCatalogConformance/BasicCreateTable (0.00s)
    ... 18 subtests, all FAIL ...
FAIL	github.com/treeverse/lakefs/pkg/icebergcatalog/conformance	0.868s
```

18 fail, 0 pass. They fail for one reason, on the first request each subtest makes:

```
BadRequestException: invalid iceberg identifier: namespace "conformance_ad958887_9afe_4801_b061_007453c0199c"
must name a repository, a ref and at least one namespace level
```

and, for the two `…NamespaceDoesNotExist` subtests, the same error in its other form:

```
expected "namespace does not exist"
in chain: "BadRequestException: invalid iceberg identifier: \"conformance_346c86ce…\" is not a valid lakeFS repository name"
```

**This is defect D-1, not a suite problem.** `catalogtest` names every namespace after nothing but a
UUID, which is exactly why `README.md` §1.1 requires the `relative_to` URI: the repository and the
branch have to be fixed by the URL. The UUID is reaching `ident.go` un-prefixed, i.e. the
`relative_to` rewrite is not happening at all.

**Measured** directly, with plain HTTP:

```shell
# the same table that the identifier form serves happily
curl -u KEY:SECRET 'http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api/v1/lakefs/namespaces/sales/tables/orders'
# {"error":{"message":"invalid iceberg identifier: namespace \"sales\" must start with a repository and a ref",
#           "type":"BadRequestException","code":400}}

curl -u KEY:SECRET -X POST 'http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api/v1/lakefs/namespaces' \
  -H 'Content-Type: application/json' -d '{"namespace":["warehouse"],"properties":{}}'
# {"error":{"message":"invalid iceberg identifier: namespace \"warehouse\" must name a repository, a ref and at
#  least one namespace level","type":"BadRequestException","code":400}}

curl -u KEY:SECRET 'http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api/v1/lakefs/namespaces'
# {"namespaces":[["conformance"]]}       <- the top level (repositories), not the ref's namespaces
```

The last one is the second half of the same defect: §5 row 4 says a `relative_to` request with no
`parent` lists the namespaces of the ref in the URL, and it answered with repositories instead, so
`bindRelativeScope` is not running either.

### Why it does not run at all

`pkg/icebergcatalog/relative.go:138` defines `RelativeTo`, and `relative.go:132` documents the mount
as `relativeTo.Mount(relativeToPattern, RelativeTo(handler))`. The actual mount is
`pkg/icebergcatalog/http.go:166`:

```go
relativeTo.Mount(relativeToPattern, handler)   // RelativeTo never mounted
```

So both `relative_to` mounts (the `/{repo}.{branch}/api` form and the `/{repo}/api` form) serve the
raw handler, the `{warehouse}` URL parameter is captured by chi and then ignored, and every request
through them behaves as if no warehouse had been named. This is finding 1 of `README.md` §3, still
open. `relative_test.go` tests the middleware in isolation, which is why it is green.

The two allowlist entries and the request-body gap of `README.md` §2.3/§3 could not be re-evaluated:
the suite never gets past its first request.

## 5. The write/read path over `/iceberg/api` — measured working

Because `relative_to` is dead, none of the 18 `catalogtest` behaviours can be exercised *by that
suite*. They can be exercised by hand over the identifier form, and they were, so that this file
reports what the catalog does rather than only what one addressing form fails to do.

Raw HTTP against `/iceberg/api` (namespace path parameter joined with `%1F`):

| Request | Expected by `catalogtest` | Measured |
|---|---|---|
| `POST /v1/lakefs/namespaces` `["conformance","main","probe"]` | created | **200** |
| same request again | already exists | **409** `AlreadyExistsException` |
| `GET /v1/lakefs/namespaces/{ns}` | properties | **200** |
| `HEAD /v1/lakefs/namespaces/{ns}` | exists | **204** |
| `POST /{ns}/properties` `{"updates":{"owner":"esti"}}` | updated | **200**, and the next `GET` returns `{"owner":"esti"}` |
| `POST /{ns}/properties` on a missing namespace | does not exist | **404** |
| `DELETE /{ns}` while it holds a table | not empty | **409** `NamespaceNotEmptyException` |
| `DELETE` a missing namespace | does not exist | **404** |
| `GET /v1/lakefs/namespaces?parent=conformance%1Fmain` | the ref's namespaces | **200**, `[["conformance","main","probe"],["conformance","main","sales"]]` |
| `GET /{ns}/tables/nope` | no such table | **404** `NoSuchTableException` |
| `HEAD /{ns}/tables/nope` | no such table | **404** |

### 5.1 A dotted namespace level round-trips

`docs/iceberg.md` says a dot is an ordinary character inside a level, because levels are split on the
unit separator and the catalog advertises no `namespace-separator` override; that is what makes
`supports-names-with-dot=false` the right RCK flag (§5, §14). **Measured** on the identifier form:

```shell
POST /iceberg/api/v1/lakefs/namespaces
  {"namespace":["conformance","main","dotted.ns"],"properties":{}}     # 200
GET  /iceberg/api/v1/lakefs/namespaces/conformance%1Fmain%1Fdotted.ns     # 200
  {"namespace":["conformance","main","dotted.ns"],"properties":{}}
GET  /iceberg/api/v1/lakefs/namespaces?parent=conformance%1Fmain          # 200, lists ["conformance","main","dotted.ns"]
```

The level came back as one level containing a dot, not as two levels. This is **not** the RCK claim:
no RCK run happened (§8), and the `relative_to` form could not be checked at all (D-1).

Commit over HTTP, with `requirements` and `updates` as §2.1 requires:
```shell
POST /iceberg/api/v1/lakefs/namespaces/conformance%1Fmain%1Fprobe/tables/t1
{"identifier":{"namespace":["conformance","main","probe"],"name":"t1"},
 "requirements":[{"type":"assert-table-uuid","uuid":"a1a4a7c4-…"},
                 {"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
 "updates":[{"action":"set-properties","updates":{"probe-prop":"1"}}]}
```

```
HTTP 200
keys: ['config', 'metadata', 'metadata-location']
metadata-location: local:///…/_iceberg/a1a4a7c4-…/metadata/00002-ed2ac31d-….metadata.json
properties: {'probe-prop': '1'}
```

The response carries **both** `metadata` and `metadata-location` (the PyIceberg requirement of §2.1),
and a requirement that the current table does not satisfy is refused with the status the client maps
to `CommitFailedException`:

```
POST …/tables/t1  {"requirements":[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000000"}], …}
HTTP 409  {"error":{"message":"the requirement \"assert-table-uuid\" is not satisfied by the current table",
                    "type":"CommitFailedException","code":409}}
```

So the behaviours `catalogtest` specifies are present on the identifier form; what is missing is the
addressing layer that would let the suite reach them, and any caller that names its warehouse in the
URL.

## 6. PyIceberg 0.12.0 — full scenario, measured

PyIceberg 0.12.0 was installed from PyPI into a fresh venv on Python 3.11.18 (`pip install
"pyiceberg[s3fs,pyarrow]==0.12.0" duckdb`), no extra authorisation needed. Because this host has no
S3-compatible store (§9, §11), the runs use `blockstore.type=local`, so the client's FileIO needs
`local://`; the published recipe uses `s3://` and needs no shim. The shim is a 10-line subclass
registered through PyIceberg's own `py-io-impl` property:

```python
class LocalSchemeFileIO(PyArrowFileIO):
    def _initialize_fs(self, scheme, netloc=None):
        return self._initialize_local_fs() if scheme == "local" else super()._initialize_fs(scheme, netloc)
```

| # | Step | Command shape | Result |
|---|---|---|---|
| 1 | connect + `list_namespaces()` | `RestCatalog(uri=…/iceberg/api, credential=…, warehouse="conformance")` | **pass** → `[('conformance',)]` |
| 2 | create table (`create_table`, no location given) | namespace `("conformance","main","sales")`, table `orders` | **pass**, location `local:///…/_iceberg/324db7a4-2fe3-4fb4-8eff-b7c0b8470a47` (the §14 Q-I5 default) |
| 3 | append 2 rows over the HTTP commit | `table.append(...)` | **pass**, `metadata-location …/metadata/00002-….metadata.json` |
| 4 | read back | `table.scan().to_arrow()` | **pass** → `[{id:1,alpha},{id:2,beta}]` |
| 5 | lakeFS commit of the branch | `POST /api/v1/repositories/conformance/branches/main/commits` | **201** |
| 6 | the commit's diff carries the table's files (§6.3) | `GET /refs/{base}/diff/{head}` | **pass** — 7 entries: 1 data file, 2 `metadata.json`, 1 manifest, 1 manifest-list, the namespace marker and the pointer |
| 7 | create branch + append on it + commit | `feature`, `append([{4,delta}])` | **pass**, `…/metadata/00003-….metadata.json`, reads 3 rows on `feature` |
| 8 | merge `feature` → `main` | `POST /refs/feature/merge/main` | **200**; `main` reads all 3 rows |
| 9 | tag `v1` at `main`'s head + read at the tag | `POST /repositories/conformance/tags`; `load_table(("conformance","v1","sales","orders"))` | **201**, then **pass** → 3 rows, same `metadata-location` as the tagged commit |
| 10 | write addressed at the tag | `table.append(...)` on the tag identifier | **refused**: `ForbiddenError: ForbiddenException: ref is read-only` (§5, Q-I4) |
| 11 | register (§I-2) | `register_table(("conformance","main","sales","orders_registered"), <existing metadata.json>)` | **pass**, reads the same 3 rows |
| 12 | drop (I2-5) | `drop_table(("conformance","main","sales","to_drop"))` | **pass**; `table_exists` → `False`, `load_table` → `NoSuchTableError: no such table "to_drop" in namespace "sales" of conformance@main`; the drop commit's diff removes the pointer **and nothing else** (measured again on `probe.t2`: `removed _lakefs_iceberg/tables/probe/t2/.pointer`, no file entry removed — see N-3) |
| 13 | GC after the commit keeps the table readable | `POST /api/v1/repositories/conformance/gc/prepare_commits` (after `PUT …/settings/gc_rules`) | prepare **ran**; the CSV lists every Iceberg commit with `expired=false`, including the append (`e4b6f8a8…`) and the merge (`3174c484…`); the table still reads on `main` and on `v1` afterwards |
| 14 | `list_tables` | `list_tables(("conformance","main","sales"))` | **pass** → `orders`, `orders_registered` |

Step 6, verbatim from the diff (the §6.3 claim, measured):

```
added _lakefs_iceberg/files/324db7a4-…/data/00000-0-d6e5d0ea-….parquet
added _lakefs_iceberg/files/324db7a4-…/metadata/00001-1c52fcc0-….metadata.json
added _lakefs_iceberg/files/324db7a4-…/metadata/00002-68485f79-….metadata.json
added _lakefs_iceberg/files/324db7a4-…/metadata/d6e5d0ea-…-m0.avro
added _lakefs_iceberg/files/324db7a4-…/metadata/snap-373637492048446497-0-d6e5d0ea-….avro
added _lakefs_iceberg/ns/sales/.namespace
added _lakefs_iceberg/tables/sales/orders/.pointer
```

Step 13, the GC input (the addresses and the objects were also checked on disk — every file the
table references was still present after the prepare step):

```
commit_id,expired,metarange_id
13842873…,false,30b31761…
e4b6f8a8…,false,c0d5e038…     <- the append
fb685378…,false,
43f82611…,false,a446a52a…     <- register + drop
3174c484…,false,30b31761…     <- the merge
```

**What step 13 does not cover:** the sweep itself. `prepare_commits` produces the commits CSV and the
addresses file that the Spark job consumes; running that job needs a Spark cluster and the lakeFS
metadata client jar, neither of which exists here. What is measured is the whole safety *input* of the
sweep (the commit carrying the table's files is listed, not expired) plus the reader-visible outcome
(the objects are still there, the table still reads), which is what §15.2 identified as the thing
staging buys.

**One artifact of the local blockstore, not a catalog defect:** `prepare_commits` answered **HTTP 400**
with `{"message":"local adapter presigned URL: operation not supported"}` *while performing the run*
(`gc_commits_location` and `run_id` were in the same body, and the CSV was written). The response is
therefore unusable for a client that reads the presigned URL, and `esti/iceberg_utils.go`'s
`requireGCPrepareKeepsCommit` expects `201` plus a presigned URL — which is correct on S3 and
unreachable on a local blockstore.

### 6.2 The same client through `relative_to` — fails

```python
RestCatalog(uri="http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api",
            credential="…", warehouse="conformance.main")
```

```
list_namespaces()                 -> [('conformance',)]      # the top level, not the ref's namespaces
create_namespace(('inv',))        -> BadRequestException: namespace "inv" must name a repository, a ref and at least one namespace level
load_table(('sales','orders'))    -> BadRequestException: namespace "sales" must start with a repository and a ref
```

Same defect (D-1) as §4, reproduced by a published client.

## 7. DuckDB 2.0.0 — attach works, queries do not

The iceberg extension installs and loads from `extensions.duckdb.org` without extra authorisation:

```python
con.execute("INSTALL iceberg"); con.execute("LOAD iceberg")
# iceberg extension loaded: [('4c0c0874c4',)]
```

Following the DuckDB recipe of `docs/iceberg.md` (`ATTACH … ENDPOINT '<…/relative_to/<repo>.<branch>/api'`):

```sql
ATTACH 'conformance.main' AS lake (TYPE iceberg,
  ENDPOINT 'http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api',
  CLIENT_ID 'AKIA…', CLIENT_SECRET '…')
```

`ATTACH` itself returns **success** — it is lazy and performs no catalog call. The first real query
fails:

```
SHOW ALL TABLES
-> HTTPException: HTTP Error: GET …/relative_to/conformance.main/api/v1/lakefs/namespaces/conformance/tables
   returned an error response (HTTP 400). BadRequestException: invalid iceberg identifier:
   a table listing needs a repository, a ref and at least one namespace level

SELECT * FROM lake.sales.orders
-> HTTPException: GetTableInformation endpoint returned response code BadRequest_400 with message
   "invalid iceberg identifier: namespace \"sales\" must start with a repository and a ref"
```

DuckDB has no way to name the repository and the ref other than in the endpoint URL, so with
`relative_to` unmounted (D-1) **DuckDB cannot read a lakeFS table at all**. The identifier-form
endpoint (`…/iceberg/api`) attaches too, but then the client namespace would have to be
`conformance.main.sales`, which DuckDB's `catalog.schema.table` addressing cannot express; that path
was not exercised further.

## 8. Apache RCK — not run

Not executed. Reasons, in order of finality:

- There is no Java runtime on this host: `/usr/bin/java -version` → *"Unable to locate a Java
  Runtime"*, and `javac` fails the same way. No `gradle` on `PATH` either.
- The RCK is not a dependency here: it runs from an `apache/iceberg` checkout with Gradle
  (`./gradlew :iceberg-open-api:test --tests RESTCompatibilityKitSuite …`), and no such checkout
  exists on this host.

`README.md` §2 documents the invocation, the flags
(`-Drck.local=false -Drck.requires-namespace-create=true -Drck.supports-names-with-dot=false`), the
two allowlist entries that hard-code local paths, and the two open decisions (requested table
locations, views). None of that has been re-validated by this run: it is documentation, not evidence.

Note that even with a JDK, the RCK could not have got further than iceberg-go's suite: it drives the
same `relative_to` URI, so D-1 would stop it in the same place.

## 9. esti (`esti/iceberg_test.go`) — attempted, skipped

**Measured.** The suite compiles, starts, and every Iceberg test skips with its own reason. Neither
skip is a pass: no esti assertion has been executed.

```shell
ESTI_SETUP_LAKEFS=false go test -count=1 -run 'TestIcebergReadPath' -v ./esti/ --args --system-tests
# === RUN   TestIcebergReadPath
#     iceberg_test.go:137: the Iceberg read-path system test runs on an S3-compatible blockstore
#                          (MinIO locally, S3 in CI): LAKEFS_BLOCKSTORE_TYPE=""
# --- SKIP: TestIcebergReadPath (0.00s)
# ok  github.com/treeverse/lakefs/esti  1.044s

ESTI_SETUP_LAKEFS=false LAKEFS_BLOCKSTORE_TYPE=s3 \
LAKEFS_BLOCKSTORE_S3_ENDPOINT=http://127.0.0.1:9000 ESTI_STORAGE_NAMESPACE=s3://esti-system-testing \
  go test -count=1 -run 'TestIceberg' -v ./esti/ --args --system-tests
# --- SKIP: TestIcebergReadPath     (docker: failed to connect to the docker API at unix:///var/run/docker.sock)
# --- SKIP: TestIcebergAppendGC
# --- SKIP: TestIcebergBranchMerge
# --- SKIP: TestIcebergTagRead
# --- SKIP: TestIcebergDrop
```

(`ESTI_SETUP_LAKEFS=false` is only because the evidence server of §1 was already listening on
`:8000`; the suite's `TestMain` otherwise sets a lakeFS installation up there.)

The two environment reasons, in the drop's own words:

- **No docker daemon.** `docker info` fails, and the suite's client steps (PyIceberg, DuckDB, Spark
  recipes) run in containers by design (§12), so `requireIcebergDocker` skips.
- **No S3-compatible store.** The harness requires `LAKEFS_BLOCKSTORE_TYPE=s3` and a bucket that the
  test process, the server and the containers can all address. MinIO is no longer distributable — its
  download site answers `410 Gone` ("The open-source MinIO Server … are archived and no longer
  maintained … These files are no longer served from this site") — and there is no other
  S3-compatible server on this host.

Consequently the esti assertions have **no live evidence from this run**: they compile and they skip
cleanly, nothing more.

What this change did to the suite (so a follow-up knows what is new):

Coverage of the operations this task asked for, by test:

| Operation | esti test |
|---|---|
| create table | `TestIcebergAppendGC` (create through the catalog), `TestIcebergReadPath` (register) |
| append over the HTTP commit | `TestIcebergAppendGC`, `TestIcebergBranchMerge`, `TestIcebergTagRead` |
| branch | `TestIcebergBranchMerge` (`feature`, `append-a`, `append-b`) |
| lakeFS commit | every write test (`commitBranch`) |
| merge | `TestIcebergBranchMerge` (a successful merge and a 409 conflict) |
| tag read | `TestIcebergTagRead` (**new**) |
| drop | `TestIcebergDrop` (**new**) |
| register | `TestIcebergReadPath` |
| GC after commit keeps the table readable | `TestIcebergAppendGC` (`prepare_commits` + objects present + re-read) |

- `TestIcebergTagRead` (new): creates, appends, commits, cuts a tag, appends again on the branch,
  then asserts the tag reads the rows it captured and not the later append, on both clients; and that
  an append addressed at the tag is refused with `ref is read-only`.
- `TestIcebergDrop` (new): drops through the catalog, asserts the table stops loading (404
  `NoSuchTableException`) and stops being listed on both addressing forms while the drop is staged and
  after the commit, and that the commit's diff *removes* the pointer. What the drop leaves behind under
  `_lakefs_iceberg/files/` is logged, not asserted (N-3), so a future tombstoning fix is not a
  regression here.
- `iceberg_utils.go`: ref-parameterised URL helpers (`refNamespaceURL`, `refTableURL`), `createTag`,
  `dropTableThroughCatalog`, `requireTableAbsent`, `requireTableListed`, `requireRefReadOnly`,
  `boolField`, `icebergIdentifierJSON`, and the two client scripts (`icebergDropScript`,
  `icebergWriteRefusedScript`).
- `iceberg_utils.go`: **fixed a latent bug in the existing harness.** `requireLoadTableAgrees` built
  the `relative_to` load URL with the full `[repo, ref, ns]` namespace, but `RelativeTo`
  (`relative.go:213` `prefixNamespacePath`) *prepends* the warehouse to whatever the parameter holds,
  so that request would have addressed `[repo, ref, repo, ref, ns]` and answered 400 even after D-1 is
  fixed. `relativeToTableURL` now sends the client namespace only, which is what DuckDB and the
  documented recipe send.
- `iceberg_utils.go`: the generated `lakefs.yaml` now carries the `sso:` section of §3 (with
  `icebergOIDCIssuer`, an in-process discovery stub), because without it the server the harness starts
  cannot boot at all (D-3). This is a workaround for a catalog defect and is commented as one.

`TestIcebergReadPath` and `TestIcebergTagRead` assert through `requireLoadTableAgrees`/`refTableURL` on
the `relative_to` form, so they would fail on this tree for D-1 as well — before reaching any of the
behaviour they are written to check.

## 10. Defects to file

Found while producing this evidence. **None of them is fixed here**: this change is the evidence,
and the catalog's own files were left alone.

| # | Severity | Defect | Where | Evidence |
|---|---|---|---|---|
| **D-1** | blocker | The `relative_to` addressing form is not mounted: `RelativeTo` is defined and unit-tested but the mount passes the bare `handler`, so the `{warehouse}` segment is captured and ignored. Every namespace-scoped request through `/iceberg/relative_to/…` is answered as if no warehouse was named; requests without a `parent` list repositories instead of the ref's namespaces. | `pkg/icebergcatalog/http.go:166` (`relativeTo.Mount(relativeToPattern, handler)`), `pkg/icebergcatalog/relative.go:138` (`RelativeTo`), doc at `relative.go:132` | §4 (18/18 fail), §4 curl outputs, §6.2, §7 |
| **D-2** | blocker (latent) | Under `relative_to`, the namespace carried in the **JSON body** of `POST /v1/namespaces` is not rewritten, so a client cannot create a namespace at all through that form even once D-1 is fixed (`relative.go` documents a rewrite of the `{namespace}` path parameter and the `parent` query parameter only). | `pkg/icebergcatalog/relative.go` (rewrite covers path + `parent` only) | **Not re-measured** — masked by D-1. Recorded because `README.md` §3 finding 2 reported it and nothing in this tree appears to have changed it. |
| **D-3** | blocker | `iceberg.enabled: true` makes the server exit at startup when the auth service cannot store policies, which is the **default** auth mode of `lakefs-sso` (and of `lakefs`): `auth.BasicAuthService` returns `auth.ErrNotImplemented` for `GetPolicy`, and `EnsureReservedPrefixProtectPolicy` turns every non-`ErrNotFound` error into a fatal startup error. The deny policy is a hardening measure, so its absence should be a warning, not a reason to refuse to run. | `pkg/icebergcatalog/bootstrap.go:149-170` (`bootstrap.go:169` is the fatal branch); `pkg/auth/basic_service.go:424` | §3 (fatal log), reproduced with `LAKEFS_ICEBERG_ENABLED=true` and no `sso:` section |
| **D-4** | gap (predicted) | Under `relative_to`, responses are not un-prefixed: `ListNamespaces` returns the server-side path `[repo, ref, ns]` where the client sent `[ns]`, so a REST client sees its own identifiers change and `catalogtest`'s `ListNamespaces` subtest cannot pass. `relative.go` states this is deliberate ("nothing is un-prefixed on the way out"), but §5 says the *client* namespace under `relative_to` is `ns…` and a client has no way to strip a server-imposed prefix. | `pkg/icebergcatalog/relative.go` (top-of-file doc), `namespaces.go` listing | **Not measured in this run** (D-1 masks it); `README.md` §3 finding 3 recorded the same, with local patches applied. |

Also recorded, so none of it is mistaken for something else. **N-3 is a defect candidate** for the
owner of §6.3; N-1 and N-2 are environment/harness notes, not catalog defects.

| # | Observation | Where |
|---|---|---|
| N-1 | `POST /gc/prepare_commits` answers **400** with `local adapter presigned URL: operation not supported` after performing the run, when the blockstore cannot presign. A client that reads the presigned URL gets an error for a run that succeeded. | core GC + `blockstore.type=local` (§6, step 13) |
| N-2 | The `sso:` section `esti/iceberg_utils.go` now generates is a workaround for D-3, not a property of the suite. Delete it when D-3 is fixed. | `esti/iceberg_utils.go` `icebergServerConfig` |
| N-3 | **A drop does not release the table's files to the committed GC.** `dropTable` (`tables.go:722`) deletes the pointer entry and nothing else, so the `_lakefs_iceberg/files/<uuid>/…` entries §6.3 staged in an earlier commit survive the drop. Measured diff of a create+append commit followed by a drop commit: `removed _lakefs_iceberg/tables/probe/t2/.pointer` — and no other path. Because the committed GC's live set is the addresses reachable from the active commits, those entries keep the dropped table's objects alive for as long as the branch does. Whether that is a defect or an accepted v1 limitation is a decision for the owner of §6.3/§14 Q-I8 (the plan does not say what a drop should do to the entries); what is certain is that it is not what §6.3's stated goal — "file entries exist so the GC protects exactly the live files" — implies for a table that no longer exists. Not fixed here. | `pkg/icebergcatalog/tables.go:722` (`dropTable`), measured in §6 step 12 |

## 11. Known gaps

- **No S3 in this environment.** Every measurement used `blockstore.type=local` and a `local://`
  FileIO shim in PyIceberg (§6). The published recipes (`s3://`, `s3.endpoint`) were not exercised, so
  nothing here validates S3 path handling, presigning, credential vending or remote signing. MinIO is
  no longer downloadable, and no other S3-compatible server was available.
- **No real GC sweep.** Only `prepare_commits` (§6, step 13). The Spark job that consumes it was not
  run. "GC after commit keeps the table readable" is therefore evidenced as: the commit is in the
  active set, the objects are still present, the table still reads.
- **No RCK, no Spark, no Trino.** RCK is §8. Spark and Trino request patterns are documented in the
  plan (§15.3 "Not covered") and were not measured here either.
- **No views, no `/plan`, no `/tasks`, no rename, no multi-table transactions.** They are v1 non-goals
  (§1.2) and are not advertised; nothing in this run touched them.
- **`register` was exercised only with a metadata.json the catalog itself had written** — the current
  metadata of `orders` on `main`, adopted as a second table name (§6, step 11). The esti suite's
  variant — a table written entirely outside the catalog with PyIceberg's `SqlCatalog` — could not run
  (§9).
- **`supports-names-with-dot=false`** is not re-validated here, because no RCK run happened (§8).
  What *was* measured is the property the flag exists to avoid relying on: on the identifier form a
  namespace level containing a dot round-trips unchanged (§5.1).
- **Dropping a table does not release its files** (§10 N-3): the pointer goes, the
  `_lakefs_iceberg/files/<uuid>/…` entries stay, so a dropped table's objects remain GC-live. Measured
  here only as the drop commit's diff and reasoned from how the committed GC builds its live set; the
  Spark sweep itself was not run (§11), so the retention itself is not observed end to end.
- **No esti assertion was executed** (§9): the suite compiles, starts and skips on two missing
  environment dependencies, so the two tests added by this change are unverified by execution.

## 12. Reproducing this

Prerequisites: Go (the repo's toolchain), a writable directory, and — for §3 onwards — a policy-capable
auth mode (`sso:` section per §3); Python 3.10+ with `pyiceberg==0.12.0` and `duckdb` for §6-§7.

```shell
# 1. build and start a server whose catalog can boot (see §3 for why the sso: section is needed)
go build -o /tmp/evidence/lakefs-sso ./cmd/lakefs-sso
# ... a config.yaml with database/blockstore local, iceberg.enabled, sso.enabled + a stub issuer ...
/tmp/evidence/lakefs-sso run --config /tmp/lfsconf/config.yaml --quickstart
curl -u AKIAIOSFOLQUICKSTART:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY -X POST \
  http://127.0.0.1:8000/api/v1/repositories -H 'Content-Type: application/json' \
  -d '{"name":"conformance","storage_namespace":"local:///tmp/lfsconf/block/conformance",
       "default_branch":"main","exist_ok":false}'

# 2. the external suite (fails today, D-1)
REST_CATALOG_URI=http://127.0.0.1:8000/iceberg/relative_to/conformance.main/api \
REST_CATALOG_CREDENTIAL=AKIAIOSFOLQUICKSTART:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
REST_CATALOG_WAREHOUSE=conformance.main \
  go test -count=1 -v ./pkg/icebergcatalog/conformance/

# 3. RCK: needs a JDK and an apache/iceberg checkout; see README.md §2 and §8 above.
# 4. esti: needs a docker daemon and an S3-compatible bucket; see esti/iceberg_utils.go and §9 above.
```

## 13. Provenance

Written by the change that produced the runs it records, from the measured output of the commands
above, `ICEBERG_CATALOG_PLAN.md` §2/§5/§6/§12/§15, `docs/iceberg.md`, the Apache Iceberg REST OpenAPI
spec, and the public PyIceberg and DuckDB documentation. No lakeFS Enterprise code and no upstream
lakeFS >= 1.87 (BSL 1.1) code was read or used.
