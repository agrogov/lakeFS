# OSS Iceberg REST Catalog — Implementation Plan

Status: draft for review. Branch base: `feature/datasets` (HEAD `423222b58`).
Unblocks: DATASETS_DESIGN.md §1.2 (non-goal), §3 parity row "Items: `table`, `namespace`", §7.4, §17 Phase 5, §18 Q4.

All external facts carry a source link. Statements about lakeFS Enterprise come from its **public docs only**
(clean-room rule, DATASETS_DESIGN.md §2). Nothing here is copied from Enterprise code or upstream ≥ 1.87.

---

## 0. Why this is its own project

Datasets `table` / `namespace` items are pointers into a catalog (namespace + table name → current
`metadata.json`), not object paths. OSS lakeFS has no catalog: `pkg/api/services.go:11-15` mounts 501 stubs for
`/iceberg/api/`, `/iceberg/relative_to/` and `/mds/iceberg/api/`. Copying table directories as `prefix` items does not
work because Iceberg metadata and manifests store fully qualified paths
([spec, "paths must be fully qualified"](https://github.com/apache/iceberg/blob/main/format/spec.md)), so a table
copied into a dataset repo still points into the source repo.

A catalog means implementing the Iceberg REST protocol: about 20 endpoints, a 23-action metadata update language,
atomic compare-and-swap commits, auth and optionally credential vending, all on top of graveler branches. That is a
bigger surface than datasets.

---

## 1. Goals / non-goals

### 1.1 Goals (v1)
1. An Iceberg REST catalog served by `lakefs-sso` at `/iceberg/api`. Spark, Trino, PyIceberg and DuckDB must be able
   to create, read, append to, update and drop tables on any lakeFS branch, and read them on any ref (tag or commit).
2. Addressing that is wire-compatible with the public lakeFS docs: `<repo>.<branch>.<namespace>[.<ns>…].<table>`, plus
   `/iceberg/relative_to/<repo>.<branch>/api` and `/iceberg/relative_to/<repo>/api`
   ([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/), [DuckDB](https://docs.lakefs.io/integrations/duckdb/),
   [Starburst Galaxy](https://docs.lakefs.io/integrations/starburst_galaxy/)).
3. Git semantics: catalog changes are **staged** on the branch and take effect on an explicit lakeFS commit. One commit
   can cover several Iceberg operations ([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/): "staged on the
   target branch, just like any other change in lakeFS").
4. RBAC through the existing `catalog:*` actions and ARNs, which are branch-agnostic, plus `catalog:ReadTableData` and
   `catalog:WriteTableData` ([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/)).
5. GC-safe: no data file or metadata file that a commit references can be collected.
6. Unblock datasets `table` and `namespace` items (§9 of this plan).

### 1.2 Non-goals (v1) — and not advertised in `/v1/config.endpoints`
- Views, functions, server-side scan planning (`/plan`, `/tasks`), `tables/rename`, location change through commit,
  Iceberg format v3. Enterprise does not support these either
  ([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/)).
- Multi-table `POST /v1/transactions/commit`. The spec says this is "primarily a Java API feature" that servers don't
  have to support ([rest-protocol.md](https://github.com/apache/iceberg/blob/main/docs/docs/rest-protocol.md)).
  DuckDB has `DISABLE_MULTI_TABLE_COMMIT`
  ([DuckDB](https://duckdb.org/docs/2.0/core_extensions/iceberg/catalogs.html)).
- Metadata search (`/mds/iceberg/api`). It stays a 501.
- Automatic merge resolution of compaction commits (`lakefs.compaction.operation-id`) is deferred to phase I-5.
- Iceberg v4 relative paths. They are still under development
  ([PR #15630](https://github.com/apache/iceberg/pull/15630), [REST v2 WIP #17543](https://github.com/apache/iceberg/pull/17543))
  and no released client relies on them yet.

---

## 2. External references (verified)

| Topic | Source |
|---|---|
| REST OpenAPI spec (OpenAPI 3.1.1, `info.version 0.0.1`, Apache-2.0) | https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml |
| REST protocol notes (endpoints negotiation, idempotency, multi-table) | https://github.com/apache/iceberg/blob/main/docs/docs/rest-protocol.md |
| Table spec: atomic swap / check-and-put | https://github.com/apache/iceberg/blob/main/format/spec.md (§ "Metastore Tables") |
| Client catalog properties (`credential`, `oauth2-server-uri`, `namespace-separator`, …) | https://github.com/apache/iceberg/blob/main/docs/docs/rest-catalog.md |
| Iceberg latest release 1.12.0 | https://github.com/apache/iceberg/releases |
| REST Compatibility Kit (RCK) | https://github.com/apache/iceberg/blob/main/open-api/README.md, https://github.com/apache/iceberg/pull/10908 |
| iceberg-go v0.7.0 (Apache-2.0; client + metadata model, **no server**) | https://github.com/apache/iceberg-go, https://github.com/apache/iceberg-go/releases |
| lakeFS Iceberg REST catalog (Enterprise, public behaviour only) | https://docs.lakefs.io/iceberg/ |
| lakeFS blog on the REST catalog (any-ref reads) | https://lakefs.io/blog/lakefs-iceberg-rest-catalog/ |
| Deprecated OSS HadoopCatalog `io.lakefs:lakefs-iceberg` | https://github.com/treeverse/lakeFS-iceberg |
| Reference servers: Polaris, Nessie, Gravitino, Lakekeeper; fixture `apache/iceberg-rest-fixture` | https://polaris.apache.org, https://projectnessie.org/guides/iceberg-rest/, https://gravitino.apache.org, https://github.com/lakekeeper/lakekeeper |
| Clients: Trino, DuckDB, PyIceberg, Snowflake, Athena | https://trino.io/docs/current/object-storage/metastores.html, https://duckdb.org/docs/2.0/core_extensions/iceberg/catalogs.html, https://py.iceberg.apache.org/configuration/, https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest, https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html |

### 2.1 Protocol facts the design depends on
- **Endpoint negotiation.** `GET /v1/config` returns `{defaults, overrides, endpoints?, idempotency-key-lifetime?}`.
  Clients call only the advertised endpoints. If `endpoints` is absent, clients assume the default set: namespaces
  CRUD + properties, tables CRUD, register, metrics, rename, transactions/commit
  ([spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)). **We must send an
  explicit `endpoints` list** so that clients don't call rename or transactions.
- **Prefix.** The client takes `prefix` from the config overrides and inserts it into every path as `/v1/{prefix}/…`.
- **Namespace encoding.** Multi-level namespaces are joined with `%1F` in path parameters. The server must also accept
  any advertised `namespace-separator` ([spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)).
- **Commit.** `POST /v1/{prefix}/namespaces/{ns}/tables/{t}` takes `{requirements[], updates[]}`.
  - 409 means `CommitFailedException`, and the client refreshes and retries.
  - 500, 502 and 504 mean `CommitStateUnknownException`, and the client must not assume failure.
  - Requirements (8): `assert-create`, `assert-table-uuid`, `assert-ref-snapshot-id`, `assert-last-assigned-field-id`,
    `assert-current-schema-id`, `assert-last-assigned-partition-id`, `assert-default-spec-id`,
    `assert-default-sort-order-id`.
- **Errors.** `{"error": {message, type, code, stack?}}`.
- **ETag / If-None-Match.** Optional. The ETag must vary with the `snapshots` query parameter (`all` or `refs`).
- **Idempotency-Key.** Optional; advertise `idempotency-key-lifetime` only if responses are actually stored.
- **Access delegation.** `X-Iceberg-Access-Delegation: vended-credentials | remote-signing`. The server may ignore it.
- **OAuth.** `POST /v1/oauth/tokens` is deprecated since Iceberg 1.6 and slated for removal in 2.0. Clients should use
  `oauth2-server-uri`, but Spark, PyIceberg and Trino still use it by default with `credential=id:secret`
  ([rest-catalog.md](https://github.com/apache/iceberg/blob/main/docs/docs/rest-catalog.md)). The lakeFS docs use
  `/iceberg/api/v1/oauth/tokens` with `credential=<access_key_id>:<secret_access_key>`
  ([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/)).

---

## 3. In-repo anchor points (verified at HEAD `423222b58`)

| What | Where | Notes |
|---|---|---|
| 501 stubs | `pkg/api/services.go:11-15` | Mounts `/iceberg/api/`, `/iceberg/relative_to/`, `/mds/iceberg/api/` |
| Mount order | `cmd/lakefs/cmd/run.go:303-311` | Extra route builders run **before** `api.MountNotImplementedServices` |
| chi duplicate mount panic | chi v5.2.4 `mux.go:297` (`attempting to Mount() a handler on an existing path`) | Mounting `/iceberg/api/` from a hook panics today |
| Hook API | `cmd/lakefs/cmd/hooks.go:37-55` (`ServerDeps`, `ExtraRoutesBuilder`, `AddExtraRoutesBuilder`) | `ServerDeps` has Catalog, AuthService, Authenticator, BlockAdapter, KVStore |
| Hook registration pattern | `cmd/lakefs-sso/main.go:44` | `lakefsCmd.AddExtraRoutesBuilder(datasets.RoutesBuilder)` |
| RoutesBuilder pattern (config, disabled → 501, auth middleware) | `pkg/datasets/http.go:50-86`, `NewRouter` `:127-145` | Copy the structure, but fix the middleware order per REVIEW (validator after auth) |
| Auth middleware | `pkg/api/auth_middleware.go:31` `GenericAuthMiddleware` | Handles basic, JWT and cookie |
| JWT mint / verify | `pkg/auth/token.go:54` `VerifyToken`, `:74` `GenerateJWTLogin` | Backs the OAuth token endpoint |
| `catalog:*` actions | `pkg/permissions/actions.go:77-91` | Namespace, table and view actions exist; **`ReadTableData` / `WriteTableData` are missing** |
| Catalog ARNs | `pkg/permissions/permission.go:66-76` | `CatalogNamespaceArn`, `CatalogTableArn`, `CatalogViewArn` |
| Built-in policies | `pkg/auth/base.go:84` `CatalogRead`, `:95` `CatalogReadWrite` | RBAC group mapping in `RBAC_DESIGN.md:222,250-251` (`CatalogReadWriteAll` for Admins, Developers and SuperUsers) |
| Conditional staging writes | `pkg/graveler/graveler.go:218` `ConditionFunc`, `:237` `SetOptions.Condition`, `:283` `WithCondition`, `:644` `Update` | Check-and-put on one key; `kv.ErrPredicateFailed` → `graveler.ErrPreconditionFailed` |
| KV CAS primitive | `pkg/kv/store.go:47-60` (predicates), `:100` `SetIf` | |
| Catalog write / commit | `pkg/catalog/catalog.go:1201` `CreateEntry(…, opts ...graveler.SetOptionsFunc)`, `:1361` `Commit` | Options are passed through |
| Address types | `pkg/block/adapter.go:30-54` (`IdentifierTypeRelative` / `Full`), `pkg/catalog/catalog.go:1156-1176` | Lets entries point at existing physical objects |
| Datasets reserved item types | `pkg/datasets/definition.go:89-90` (`ItemTypeTable`, `ItemTypeNamespace`), `:103-113` (`TableSrc`, `NsSrc`) | |
| Datasets 501 rejection | `pkg/datasets/validate.go:222-225` | Remove in phase I-6 |
| Design refs | `DATASETS_DESIGN.md:26-27, 51, 81, 587-593, 1107, 1127` | Update when this lands |

---

## 4. Architecture

```
Spark / Trino / PyIceberg / DuckDB
        │  Iceberg REST (JSON)                 data files: direct to object store
        ▼                                        (client creds | vended | remote-signed)
/iceberg/api/v1/…    /iceberg/relative_to/…/api/v1/…
        │
pkg/icebergcatalog  (new package, mounted by lakefs-sso hook)
  ├─ http.go       router, auth (basic | JWT | OAuth token), error model, endpoints list
  ├─ ident.go      namespace / identifier parsing: repo.ref.ns….table, %1F, relative_to
  ├─ authz.go      catalog:* checks on CatalogNamespaceArn / CatalogTableArn
  ├─ store.go      namespace markers + table pointers as graveler entries (conditional writes)
  ├─ commit.go     requirements validate → updates apply (iceberg-go) → write metadata.json → CAS pointer
  ├─ register.go   stage table files as lakeFS entries (GC safety, diff, merge)  [§6.3]
  └─ creds.go      (phase I-4) vended credentials / remote signing
        │
pkg/catalog → pkg/graveler → pkg/kv   +   pkg/block (metadata.json writes / reads)
```

### 4.1 Mounting (blocker fix, phase I-0)
`MountNotImplementedServices` must not mount a pattern that a hook already mounted. Change
`pkg/api/services.go:11-15` to take the router and skip already-registered patterns: chi exposes
`router.Match(chi.NewRouteContext(), "GET", "/iceberg/api/v1/config")`. Alternatively, pass a set of patterns
the hooks claimed. That is a ~6-line core diff; record it in DATASETS_DESIGN.md §16 (merge-conflict surface). Add a
test in `pkg/api/serve_test.go` (the stub routes are already tested near line 291) showing that a pre-mounted
`/iceberg/api/` survives.

### 4.2 Dependency
Use `github.com/apache/iceberg-go` v0.7.0 (Apache-2.0; module requires Go 1.26.3, repo is on 1.26.4 per `go.mod:3`)
for the metadata model only:
- `table.ParseMetadataBytes`;
- `table.MetadataBuilderFromBase`;
- `table.UpdateTableMetadata(base, updates, metadataLoc)`;
- JSON decoding of `table.Requirements` and `table.Updates`;
- `Requirement.Validate(meta)`, which returns `ErrCommitFailed`;
- `NewMetadataWithUUID`.

Its write helpers are in `catalog/internal` and cannot be imported, so writing metadata files is our own code.
**Spike S-I1:** confirm that its Updates/Requirements JSON covers all 23 update actions in the spec version we
advertise, notably the encryption-key and statistics updates. Implement any that are missing locally.

---

## 5. Addressing

| Client sees | Parsed as |
|---|---|
| `/iceberg/api/v1/config?warehouse=…` | Returns `overrides.prefix = "lakefs"`, `defaults`, explicit `endpoints` |
| `/iceberg/api/v1/lakefs/namespaces/{repo%1Fref%1Fns…}` | `repo`, `ref`, namespace path `ns…` |
| `/iceberg/api/v1/lakefs/namespaces/{repo%1Fref%1Fns}/tables/{t}` | + table `t` |
| `/iceberg/relative_to/{repo}.{branch}/api/v1/…/namespaces/{ns}` | `repo`, `branch` fixed by URL; client namespace = `ns…` |
| `/iceberg/relative_to/{repo}/api/v1/…/namespaces/{ref%1Fns}` | `repo` fixed; first level = ref |

Rules:
- Namespace rules from the docs: case-sensitive, under 255 characters total, no empty components, no special
  characters except dots ([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/)).
- Levels 1 and 2 are always repo and ref. `GET namespaces` with no parent lists repositories the caller can read. With
  parent `repo` it lists branches; with `repo.ref` it lists real namespaces.
- Writes require `ref` to be a **branch**. A tag or commit ID returns 403 `ForbiddenException` with message "ref is
  read-only". Enterprise's exact status code is not documented (open question Q-I4).
- Advertise `namespace-separator` only if needed. Always accept `%1F` (spec MUST).
- RCK flag: `supports-names-with-dot=false`.

---

## 6. Storage model

### 6.1 Layout on the branch (all ordinary lakeFS entries, so diff, merge, revert and commit work for free)

```
_lakefs_iceberg/
  ns/<ns1>/<ns2>/.namespace                 namespace marker; entry user-metadata = namespace properties (JSON)
  tables/<ns1>/<ns2>/<table>/.pointer        table pointer; entry user-metadata:
                                               iceberg.metadata-location = <physical URI of current metadata.json>
                                               iceberg.table-uuid       = <uuid>
                                               iceberg.format-version   = 1|2
```
The reserved prefix `_lakefs_iceberg/` is hidden from `fs:` writes by a deny statement, the same way datasets protects
`_lakefs_ds_pin_*`. Table files live under
`<storage_namespace>/_iceberg/<table-uuid>/{metadata,data}/…`, a physical location that is stable across branches.
Enterprise uses a `_managed/` area in the storage namespace per its
[Starburst IAM example](https://docs.lakefs.io/integrations/starburst_galaxy/). Our layout is our own design;
Enterprise's layout is not public (Q-I1).

### 6.2 Atomic commit (check-and-put)
1. Read the pointer entry on the branch (staged, else committed) → `base.metadata-location`. Load `metadata.json` via
   the block adapter.
2. Run `Requirement.Validate(base)` for each requirement. On failure → 409 `CommitFailedException`.
3. `UpdateTableMetadata(base, updates, newLoc)`. Reject `set-location` with 400, matching the documented Enterprise
   limitation.
4. Write `newLoc = …/metadata/<seq>-<uuid>.metadata.json` to the object store. Never overwrite it.
5. Stage the files (§6.3).
6. `catalog.CreateEntry(repo, branch, pointerEntry{metadata-location: newLoc}, graveler.WithCondition(func(cur) error {
   if cur.metadata-location != base.metadata-location { return ErrPreconditionFailed } }))`
   (`pkg/catalog/catalog.go:1201`, `pkg/graveler/graveler.go:283`).
   - `graveler.ErrPreconditionFailed` → 409.
   - Any other error after step 4 → 500 `CommitStateUnknownException`. The orphan metadata file is harmless; GC
     collects it.
7. Return `LoadTableResult{metadata-location, metadata, config}`.

`assert-create` (create table) uses `WithCondition` on an absent value, which maps to `If-None-Match: *` semantics
already supported in `pkg/api/apiutil/conditions.go:16-36`.

Why stage instead of auto-commit: this matches the documented behaviour ("staged on the target branch"). It also lets
one lakeFS commit cover several table updates, and it avoids commit contention on hot branches. Branch protection
applies naturally: writes go through graveler without `Force`, so protected branches reject them, and that is what
users want.

### 6.3 Table files become lakeFS entries (GC safety) — **design decision, spike S-I2**
Clients write data and manifest files straight to `<storage_namespace>/_iceberg/<uuid>/…`. Unless those files are
lakeFS entries:
- lakeFS GC (and uncommitted-object GC) may delete them, because they sit inside the storage namespace and nothing
  references them;
- `lakectl diff` and merge only see the pointer change;
- datasets cannot materialise them.

**Proposal.** On every commit that contains `add-snapshot`:
1. Read the new snapshot's manifest list.
2. Read the manifests it adds, using iceberg-go's Avro manifest reader.
3. Stage one entry per new metadata, manifest-list, manifest and data file, under
   `_lakefs_iceberg/files/<table-uuid>/<relative path>`, with `AddressType=Full` and the physical address the client
   wrote.

On `remove-snapshots`, stage tombstones for files no longer reachable. That follows Iceberg's expire semantics;
Enterprise does not support expire (Q-I8), so v1 can simply ignore `remove-snapshots` for files.

Costs and risks (to be measured in S-I2):
- One manifest read per commit, and one staged entry per data file. A 10k-file append means 10k staging writes. Batch
  them through the `DeleteBatch` / `CreateEntries` path or the import mechanism.
- Uncommitted-GC window: files written by the client before the catalog commit stages them. Mitigation: uncommitted
  GC must skip `_iceberg/` objects younger than a grace period (config `iceberg.uncommitted_gc_grace`, default 24h).
  Check how `pkg/catalog` uncommitted GC lists the storage namespace before choosing.

Rejected alternative: point table locations at the S3 gateway (`s3://<repo>/<branch>/…`). Files become entries
natively, but absolute paths then embed the branch, so after a merge `main` metadata points into `feature/…`, which
breaks when that branch is deleted. Same problem as the deprecated HadoopCatalog
([lakeFS-iceberg](https://github.com/treeverse/lakeFS-iceberg)).

### 6.4 Merge
The pointer is a single entry, so concurrent changes to the same table on two branches conflict at merge. This matches
the docs: "metadata files are treated as ordinary files"
([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/)). The compaction auto-resolve and the
`iceberg.ignore_compaction_commits` flag (default true in Enterprise) are phase I-5.

---

## 7. Auth & RBAC

### 7.1 Authentication
- `POST /iceberg/api/v1/oauth/tokens` takes `grant_type=client_credentials`, `client_id=<access_key_id>`,
  `client_secret=<secret>`. Verify with the auth service's credential lookup, mint a lakeFS JWT with
  `auth.GenerateJWTLogin` (`pkg/auth/token.go:74`), and return
  `{access_token, token_type:"bearer", expires_in, issued_token_type}`.
- Every other route: `Authorization: Bearer <jwt>`, verified with `auth.VerifyToken` (`pkg/auth/token.go:54`). Also
  accept HTTP basic for `rest.auth.type=basic` clients
  ([rest-catalog.md](https://github.com/apache/iceberg/blob/main/docs/docs/rest-catalog.md)). Reuse
  `GenericAuthMiddleware` (`pkg/api/auth_middleware.go:31`) where possible. It is bound to the main swagger, so wrap it
  the way `pkg/datasets/http.go` does, but put authentication **before** request validation.
- SSO users: they use access keys. There is no browser flow for catalog clients.

### 7.2 Authorization (branch-agnostic ARNs, `pkg/permissions/permission.go:66-76`)

| Endpoint | Action | Resource |
|---|---|---|
| list namespaces under `repo.ref` | `catalog:ListNamespaces` | `CatalogNamespaceArn(repo, parent)` |
| get / head namespace | `catalog:GetNamespace` | `CatalogNamespaceArn(repo, ns)` |
| create / update props / delete namespace | `catalog:Create/Update/DeleteNamespace` | idem |
| list tables | `catalog:ListTables` (+ filter with `catalog:TableName` StringLike condition) | `CatalogNamespaceArn` |
| load table | `catalog:ReadTable` | `CatalogTableArn(repo, ns, t)` |
| create / register / commit / drop | `catalog:CreateTable` / `UpdateTable` / `DeleteTable` | `CatalogTableArn` |
| vended read / write creds | **`catalog:ReadTableData` / `catalog:WriteTableData`** (new) | `CatalogTableArn` |

Additionally, require `fs:ReadRepository` on the repo and, for writes, `fs:WriteObject` on
`ObjectArn(repo, "_lakefs_iceberg/*")`. Then the existing branch, repo and immutability denies (datasets
`dataset-<name>-immutable`) also block catalog writes into dataset repos. Add `catalog:Create*`, `catalog:Update*` and
`catalog:Delete*` to datasets `immutabilityDeniedActions` (`pkg/datasets/authz.go:481-497`).

Changes:
- `pkg/permissions/actions.go:77-91`: add `CatalogReadTableDataAction = "catalog:ReadTableData"` and
  `CatalogWriteTableDataAction = "catalog:WriteTableData"`, then run `make gen-code` (the permissions enum generator).
- `pkg/auth/base.go:84`: add `ReadTableData` to `CatalogRead`. `CatalogReadWrite` already uses `catalog:*`.
- `contrib/auth/acl` templates and `RBAC_DESIGN.md` §4.5: confirm `CatalogReadWriteAll` is attached to Admins,
  Developers and SuperUsers (`RBAC_DESIGN.md:222,250-251`) and `CatalogRead` to Viewers.
- The Enterprise docs warn that custom policies created before the upgrade must add the data actions manually. Add the
  same note to our docs.

---

## 8. Data-plane access (phase I-4)

| Mode | v1? | Notes |
|---|---|---|
| None: the client brings its own storage creds | **yes (default)** | Works with Spark (S3FileIO), Trino, PyIceberg and Snowflake external volumes. Simplest. |
| Remote signing (`POST …/tables/{t}/sign`, `s3.remote-signing-enabled=true`) | phase I-4a | Reuses lakeFS's own blockstore credentials; the server checks the URI is under the table location and checks ReadTableData or WriteTableData. PyIceberg supports it only with FsspecFileIO ([PyIceberg](https://py.iceberg.apache.org/configuration/)). Nessie implements it ([Nessie](https://projectnessie.org/guides/iceberg-rest/)). |
| STS vended credentials | phase I-4b | Enterprise's approach: config `blockstore.s3.credentials_vending.{role_arn, session_duration (default 1h, min 15m), external_id, endpoint}`, a session policy scoped to the table prefix, best-effort ([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/)). S3 only. |
| Presigned URLs | no | Iceberg clients don't use them. |

---

## 9. Datasets integration (phase I-6)

Because table files are lakeFS entries with **full physical addresses** (§6.3), a datasets `table` item materialises
the same way as a `prefix` item. It copies:
- the pointer entry;
- the `_lakefs_iceberg/files/<uuid>/…` entries reachable from the pinned snapshot.

The absolute paths inside metadata stay valid because:
- the physical objects are shared;
- the datasets pin branch (`pkg/datasets/pins.go`) keeps them out of GC.

This removes the portability problem in DATASETS_DESIGN.md §7.4.

Changes:
- `pkg/datasets/validate.go:222-225`: accept `table` and `namespace` when `iceberg.enabled`.
- `pkg/datasets/materialize.go`: add an item opener that resolves the table at the source ref and iterates its file
  entries plus the pointer.
- The datasets read path: `catalog.<ds>.<ref>.<ns>.<table>` is just `/iceberg/api` with repo = the dataset repo. It is
  read-only because of the immutability deny (§7.2).
- `pkg/datasets/authz.go` `SourceObjectArn`: for table items, check `catalog:ReadTable` and `catalog:ReadTableData` on
  `CatalogTableArn`.
- `api/datasets.yml` and `docs/datasets.md`: document table and namespace items. Update DATASETS_DESIGN.md §1.2, §3,
  §7.4, §17 and Q4.

---

## 10. Config (`lakefs-sso`, like `datasets.*` in `pkg/datasets/config.go`)

```yaml
iceberg:
  enabled: false                 # true mounts /iceberg/api and /iceberg/relative_to; false keeps the 501 stubs
  prefix: lakefs                 # value of /v1/config overrides.prefix
  token_ttl: 1h
  max_request_body: 8MiB
  uncommitted_gc_grace: 24h      # §6.3
  ignore_compaction_commits: true # phase I-5
```

---

## 11. Phases & estimates (one engineer)

| Phase | Scope | Exit criteria | Est. |
|---|---|---|---|
| **I-0 Spikes** | S-I1 iceberg-go coverage; S-I2 file registration cost and GC window (10k-file append); S-I3 mount-order fix (§4.1); S-I4 PyIceberg + DuckDB against a stub `/v1/config` | Written spike notes appended here | 3 d |
| **I-1 Read path** | config, OAuth token, namespaces list/get/head (repo/ref levels), tables list/load/head on any ref, error model, authz, `relative_to` both forms | PyIceberg, DuckDB and Spark can read a table registered by `register` | 5 d |
| **I-2 Write path** | create namespace/table (incl. staged create), commit (§6.2), drop, register, properties, metrics (accept and discard) | iceberg-go `catalogtest` external conformance green; RCK green with `supports-names-with-dot=false` | 7 d |
| **I-3 File registration + GC** | §6.3 staging of table files; uncommitted-GC grace; protect `_lakefs_iceberg/` | Esti: append, commit, GC, then table still readable; branch, write, merge, read on main | 5 d |
| **I-4 Data access** | (a) remote signing; (b) STS vending + ReadTableData / WriteTableData | Spark with `X-Iceberg-Access-Delegation: vended-credentials` on MinIO and S3 | 4 d |
| **I-5 Merge niceties** | compaction auto-resolve, ETag/304, Idempotency-Key | Unit + esti | 4 d |
| **I-6 Datasets items** | §9 | Esti: dataset with table item, published, then read via catalog at `<ds>.v1` | 4 d |

Total ≈ 6–7 weeks. I-1 to I-3 is the usable minimum (≈ 4 weeks).

---

## 12. Testing

- **Unit:** identifier parsing (`%1F`, dots, relative_to); requirement and update application (table-driven from spec
  examples); CAS race (two goroutines committing to the same table, exactly one wins and the other gets 409); authz
  matrix.
- **Conformance:**
  - iceberg-go `catalog/catalogtest` via `rest/external_conformance_test.go`, with env vars `REST_CATALOG_URI`,
    `REST_CATALOG_CREDENTIAL`, `REST_CATALOG_WAREHOUSE` ([iceberg-go](https://github.com/apache/iceberg-go)). This is
    Go-native and can run in esti.
  - Apache RCK: `./gradlew :iceberg-open-api:test --tests RESTCompatibilityKitSuite -Drck.local=false -Drck.uri=…`
    ([README](https://github.com/apache/iceberg/blob/main/open-api/README.md)). Two tests are known to fail against
    external servers in 1.11 because they hard-code local paths
    ([issue context](https://github.com/apache/iceberg/issues/18215)); allowlist them.
- **Esti (`esti/iceberg_test.go`):** run PyIceberg, DuckDB and Spark in docker against lakefs-sso on MinIO. Cover
  create, append, branch, commit, merge, tag read, drop, GC, and the dataset table item.
- **Security tests:**
  - a user without `catalog:ReadTable` gets 403 on load;
  - writes to a tag ref get 403;
  - writes into a dataset repo get 403;
  - `_lakefs_iceberg/` cannot be written through `fs:`;
  - oversized bodies get 413;
  - OAuth with bad creds gets 401 and leaks no detail.

---

## 13. Risks

| Risk | Mitigation |
|---|---|
| iceberg-go lags behind the spec | Pin the version; implement missing updates locally; advertise only the endpoints we pass |
| Staging thousands of entries per append | Batch staging; measure in S-I2; fallback is to register only manifests and metadata, and protect data files through a per-table GC rule |
| Client writes outside the table location | On commit, reject add-snapshot whose manifests reference paths outside `<storage_namespace>/_iceberg/<uuid>/` (400); this also blocks reads of foreign buckets |
| Legal: protocol compatibility with Enterprise | Implement from the Apache spec and public docs only. Note it in the PR. Legal sign-off per DATASETS_DESIGN.md §2 / Q5 |
| Alternative: run Polaris or Nessie beside lakeFS (DATASETS_DESIGN.md Q4) | Rejected for v1: they keep their own commit log, so lakeFS branch, merge and GC semantics would not cover catalog state |

---

## 14. Open questions

| # | Question | How to resolve |
|---|---|---|
| Q-I1 | Enterprise pointer key and metadata file naming are not public. Is wire compatibility enough? | Assume yes; data written by Enterprise is not imported |
| Q-I2 | Does Enterprise stage or auto-commit create and drop? The docs only say "staged" | Stage everything (§6.2) |
| Q-I3 | What does `prefix=lakefs` mean in Enterprise beyond being a constant? | Treat it as a constant; reject others with 404 `NoSuchWarehouseException` |
| Q-I4 | Status code for writes to a tag or commit ref | 403 `ForbiddenException`; revisit if a client mishandles it |
| Q-I5 | Default table location when the client omits one | `<storage_namespace>/_iceberg/<table-uuid>` |
| Q-I6 | Is the OAuth token a lakeFS JWT? Token exchange or refresh? | Yes, JWT; no refresh (clients re-auth with `credential`) |
| Q-I7 | Is the iceberg-go dependency acceptable (Apache-2.0)? | Legal and security review |
| Q-I8 | `remove-snapshots` / expire: support file tombstoning in v1? | No (Enterprise doesn't); accept the update and keep files |
| Q-I9 | Multi-table transactions and `/sign` in v1? | Not advertised in v1; `/sign` in I-4a |
