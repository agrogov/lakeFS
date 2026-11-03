# OSS Iceberg REST Catalog — Implementation Plan

Status: implemented and closed on `feature/iceberg-catalog`; spike findings folded into §15, and §15.4
records the close-out — every v1 capability is landed, and the remaining gaps are environmental (no RCK
run, no S3 blockstore, esti unexecuted) or v1 non-goals. A later review round added two things on top of
that close-out, both recorded in §15.4: the web UI **Tables tab**, a read-only explorer of a ref's Iceberg
tables, and the login **session cookie** as a read-only authentication scheme for it (§7.1). The
user-facing documentation is [`docs/iceberg.md`](docs/iceberg.md).
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
  Clients call only the advertised endpoints. If `endpoints` is absent, each client assumes its own default set, and
  that set is **not** the spec default: PyIceberg 0.12 assumes 12 endpoints that include `tables/rename` but exclude
  `transactions/commit`, metrics and the `HEAD` endpoints (§15.3). **We must send an explicit `endpoints` list** so
  that clients don't call rename or transactions.
- **The list is a hint, not a fence.** PyIceberg gates a call on the list (raising `NotImplementedError` without an
  HTTP request); DuckDB ignores it for rename, create and drop and sends the request anyway (§15.3). Security and
  501/404 answers must therefore never assume the client read the list (§7.2, §12).
- **Prefix.** The client takes `prefix` from the config overrides and inserts it into every path as `/v1/{prefix}/…`.
  The entries of `endpoints` keep the **literal** `{prefix}` placeholder — expanding it to `lakefs` makes every
  PyIceberg call fail after a successful connect (§15.3). The two must not be conflated.
- **Config response shape.** Always emit both `defaults` and `overrides` objects, even when empty, and keep every
  value a JSON string: DuckDB hard-fails on a missing object or a non-string value, while PyIceberg tolerates both
  (§15.3).
- **Namespace encoding.** Multi-level namespaces are joined with `%1F` in path parameters. The server must also accept
  any advertised `namespace-separator` ([spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)).
- **Commit.** `POST /v1/{prefix}/namespaces/{ns}/tables/{t}` takes `{identifier, requirements[], updates[]}` and the
  **response** must carry both `metadata` and `metadata-location`: PyIceberg raises after a successful commit if
  `metadata-location` is missing (§15.3).
  - 409 means `CommitFailedException`, and the client refreshes and retries.
  - 500, 502 and 504 mean `CommitStateUnknownException`, and the client must not assume failure.
  - Requirements (8): `assert-create`, `assert-table-uuid`, `assert-ref-snapshot-id`, `assert-last-assigned-field-id`,
    `assert-current-schema-id`, `assert-last-assigned-partition-id`, `assert-default-spec-id`,
    `assert-default-sort-order-id`.
- **Errors.** `{"error": {message, type, code, stack?}}`, with all three fields present: a partial envelope degrades to
  "unexpected JSON payload" in PyIceberg and to a raw body in DuckDB. The exception *class* is chosen from the HTTP
  status, not from `type`, and `404` becomes `NoSuchTableError` whatever the endpoint was (§15.3).
- **ETag / If-None-Match.** Optional. The ETag must vary with the `snapshots` query parameter (`all` or `refs`).
- **Idempotency-Key.** Optional; advertise `idempotency-key-lifetime` only if responses are actually stored.
- **Access delegation.** `X-Iceberg-Access-Delegation: vended-credentials | remote-signing`. The server may ignore it;
  both clients fall back to their own credentials when the response carries neither `storage-credentials` nor a
  signing config, so answering the header with an empty `config` is safe (§15.3).
- **OAuth.** `POST /v1/oauth/tokens` is deprecated since Iceberg 1.6 and slated for removal in 2.0. Clients should use
  `oauth2-server-uri`, but Spark, PyIceberg and Trino still use it by default with `credential=id:secret`
  ([rest-catalog.md](https://github.com/apache/iceberg/blob/main/docs/docs/rest-catalog.md)). The lakeFS docs use
  `/iceberg/api/v1/oauth/tokens` with `credential=<access_key_id>:<secret_access_key>`
  ([docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/)). PyIceberg calls it twice at connect time and DuckDB
  sends the credentials through it as HTTP Basic (§15.3).

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
| Datasets 501 rejection | `pkg/datasets/validate.go:222-225` | Resolved in phase I-6 (I6-1): `table`/`namespace` items are accepted when `iceberg.enabled` |
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
**Spike S-I1 (§15.1):** iceberg-go v0.7.0 covers all 23 update actions and all 8 requirement types, so nothing has
to be implemented locally — but `Requirement.Validate` does not return `ErrCommitFailed` and
`MetadataBuilderFromBase` takes the location of the *current* metadata file. Both are recorded in §15.1.

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

### 6.3 Table files become lakeFS entries (GC safety) — **design decision, spike S-I2 (§15.2)**
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

Costs and risks (**measured** in spike S-I2, §15.2):
- One manifest read per commit, and one staged entry per data file. A 10k-file append means 10k staging writes:
  **≈2.3 s** at 32-way concurrency, **≈1.2 s** against a KV with a 1 ms round trip, and **≈76 s** with a plain
  sequential loop because of `graveler.max_batch_delay=3ms`. Staging must be parallel and bounded, or the batched
  `WriteRange`/`Store.Import` path must be used — which it cannot be here, because `Store.Import` commits and takes
  no `WithCondition`, so it would bypass the pointer CAS of §6.2 step 6.
- Uncommitted-GC window: files written by the client before the catalog commit stages them. `PrepareGCUncommitted`
  lists **staged entries only** — measured: 0 of 200 raw client writes under `_iceberg/` appear in the report, and a
  staged entry for the same object does. So the control is an uncommitted-GC grace: skip `_iceberg/` entries younger
  than `iceberg.uncommitted_gc_grace` (default 24h; `0` protects every `_iceberg/` object regardless of age).
  Residual window and the interaction with the GC job's own minimum age: §15.2.
- Staging is what makes a table file visible to the GC at all, so the grace bounds one window and cannot close the
  other: `[client writes the object → the catalog stages it]` is unprotected by construction.

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
  the way `pkg/datasets/http.go` does, but put authentication **before** request validation. As landed, the catalog does
  not reuse it: `NewAuthMiddleware` (`pkg/icebergcatalog/auth.go`) is a drop-in replacement that keeps the bearer/basic
  schemes, serves the token endpoint itself and adds the cookie scheme below.
- SSO users: they use access keys. There is no browser *login* flow on the catalog mount — no login or callback route.
- **The web UI's login session cookie, for `GET` and `HEAD` only.** A signed-in browser must be able to read the
  catalog it is already authenticated against (the repository Tables tab, §15.4), so the `internal_auth_session`
  cookie is accepted on reads and on nothing else. It is the same session, the same token key and the same
  `auth.UserByToken` verification the core API's `cookie_auth` scheme uses. A state-changing request carrying only
  a cookie is `401`: a cookie is the one credential a browser attaches by itself, so accepting it for a write would
  expose every catalog write to CSRF. `POST /v1/oauth/tokens` is served ahead of that decision (it is the
  middleware's own handler), so a cookie cannot be exchanged for a token, and the `Authorization` header is read
  first, so a client that sends a bearer token is never authenticated by the cookie. Authorisation is unchanged —
  the cookie passes the same `catalog:*` checks.

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

**Status: landed.** `table` and `namespace` items are accepted when `iceberg.enabled`, materialised from the
pointer and the staged file entries, authorized at the source, and read back through the catalog at
`<dataset>.<ref>.<namespace>.<table>`; they are no longer rejected. The design notes below are what the
implementation follows.

Because table files are lakeFS entries with **full physical addresses** (§6.3), a datasets `table` item materialises
the same way as a `prefix` item. It copies:
- the pointer entry;
- the `_lakefs_iceberg/files/<uuid>/…` entries present at the source ref.

The absolute paths inside metadata stay valid because:
- the physical objects are shared;
- the datasets pin branch (`pkg/datasets/pins.go`) keeps them out of GC.

This removes the portability problem in DATASETS_DESIGN.md §7.4.

Landed as:
- `pkg/datasets/validate.go`: `table` and `namespace` are accepted when `iceberg.enabled`.
- `pkg/datasets/materialize.go`: an item opener resolves the table at the source ref and iterates its file entries
  plus the pointer.
- The datasets read path: `catalog.<ds>.<ref>.<ns>.<table>` is just `/iceberg/api` with repo = the dataset repo. It is
  read-only because of the immutability deny (§7.2).
- `pkg/datasets/authz.go` `SourceObjectArn`: for table items, checks `catalog:ReadTable` and `catalog:ReadTableData` on
  `CatalogTableArn`.
- `api/datasets.yml` and `docs/datasets.md` document table and namespace items.

---

## 10. Config (`lakefs-sso`, like `datasets.*` in `pkg/datasets/config.go`)

```yaml
iceberg:
  enabled: false                 # true mounts /iceberg/api and /iceberg/relative_to; false keeps the 501 stubs
  prefix: lakefs                 # value of /v1/config overrides.prefix
  token_ttl: 1h
  max_request_body: 8MiB
  uncommitted_gc_grace: 24h      # §6.3
  max_staged_files: 50000        # §6.3, spike S-I2
  ignore_compaction_commits: true # phase I-5
```

---

## 11. Phases & estimates (one engineer)

| Phase | Scope | Exit criteria | Est. |
|---|---|---|---|
| **I-0 Spikes** | S-I1 iceberg-go coverage; S-I2 file registration cost and GC window (10k-file append); S-I3 mount-order fix (§4.1); S-I4 PyIceberg + DuckDB against a stub `/v1/config` | Spike notes folded into §15 | 3 d |
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
- **Web UI (`webui/src`, vitest):** `lib/api/iceberg.test.ts` — the error mapping (`501` is disabled, `404` is
  disabled, `500` is not, `403` is forbidden, a `401` with the catalog's constant message is "no session" and not a
  permission denial, the core `{"message": …}` placeholder body), the request shapes (the catalog's own mount, not
  `/api/v1`; `%1F`; a nested parent; `pageToken`/`pageSize`; an encoded table name; the prefix cached once) and the
  metadata helpers. `lib/components/repository/tablesExplorer.test.tsx` — the Enterprise upsell on `501`, the `401`
  answer not being an upsell, the permission state on `403` from the config and from a listing, the empty state, and
  the namespace → table → details flow.
- **Auth:** `pkg/icebergcatalog/auth_test.go` — a cookie `GET` succeeds on `/v1/config` and on the four read routes
  (with the user on the request context), a cookie `POST`/`PUT`/`PATCH`/`DELETE` is `401`, the token endpoint mints
  nothing for a cookie, and a cookie whose token does not verify is rejected.
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
| Staging thousands of entries per append | Batch staging in a bounded worker pool; measured in §15.2 at ≈2.3 s for 10k files at 32-way concurrency. Fallback is to register only manifests and metadata, and protect data files through a per-table GC rule — but that rules out GC safety in v1, because nothing protects the data files |
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

---

## 15. Spike findings (folded)

The four I-0 spikes ran in parallel and were written up in per-spike notes
(`ICEBERG_SPIKE_S-I1..S-I4.md`); this section is the single record they were folded into. S-I3 was the
mount-order fix of §4.1 and shipped as code (`pkg/api/services.go`, `pkg/api/serve_test.go`) rather than
as a note, which is why there are three notes and four spikes.

Provenance for the whole section: measured and written from the Apache Iceberg REST OpenAPI spec
(<https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>), the Apache Iceberg
table spec (<https://iceberg.apache.org/spec/>) and the clients' public, Apache-2.0 behaviour. No
lakeFS Enterprise code and nothing from upstream lakeFS >= 1.87 (BUSL 1.1) was read or copied.

### 15.1 S-I1 — iceberg-go update and requirement coverage

**Verdict: covered as-is. Nothing had to be implemented locally for updates or requirements.**
`github.com/apache/iceberg-go` v0.7.0 (Apache-2.0) decodes and re-encodes **23 of 23** `TableUpdate`
actions and **8 of 8** `TableRequirement` types, and applies every action in a commit-shaped request.
The gate is `TestUpdatesCoverage` (`pkg/icebergcatalog/updates_coverage_test.go`); it is driven by the
spec's own action and requirement lists, so an action added to the spec without a matching case fails it.
The `apply` column came from a throwaway harness, not from the committed test.

Two corrections to §4.2, both load-bearing for the commit path:

1. **`Requirement.Validate` does not return `ErrCommitFailed`** in v0.7.0 — it returns plain errors, so
   `errors.Is(err, table.ErrCommitFailed)` is false. The commit path must map a failed requirement to
   `409 CommitFailedException` itself; a 400 or 500 there makes the client give up instead of refreshing.
2. **`MetadataBuilderFromBase(base, currentFileLocation)`** takes the location of the *current* (existing)
   `metadata.json`, not the one about to be written; `UpdateTableMetadata(base, updates, metadataLoc)`
   forwards the same parameter under a misleading name. Passing the new file's location logs a wrong
   `metadata-log` entry, and passing `""` does not extend `metadata-log` at all.

What remains ours, because iceberg-go keeps it private (`catalog/internal`) or does not model it:

- writing `metadata.json` itself (object write, file name `v<N>.metadata.json`, location);
- mapping a failed requirement to 409 and every apply error to 400 — including `add-snapshot`'s
  "invalid snapshot timestamp", the one refusal that wraps **no** sentinel and therefore has to be
  recognised by its message or by checking the timestamp first;
- the pointer CAS and its retry semantics (§6.2);
- the format-version gate: `add-encryption-key` only applies at format version >= 3, which is a v1
  non-goal (§1.2) — it must decode and then be refused with a 400 that names the reason.

`add-snapshot` has two behaviours worth owning explicitly: iceberg-go rejects a `timestamp-ms` more than
one minute behind the base's `last-updated-ms`, and it stamps `last-updated-ms` from `now()` with no way
to pass an explicit value. A client whose clock is far behind gets a refusal no other catalog produces,
and the recorded time is iceberg-go's wall clock rather than the lakeFS commit time.

Dependency cost, for the review of `go.mod`: v0.7.0 requires newer versions of libraries this repository
already uses, and minimal version selection takes the maximum — `gocloud.dev` to v0.46.0,
`aws-sdk-go-v2` to v1.45.1, `cloud.google.com/go/storage` to v1.66.0, `google.golang.org/api` to
v0.295.0, `google.golang.org/grpc` to v1.83.2 and `testify` to v1.12.1. The `gocloud.dev` and
`aws-sdk-go-v2` jumps touch the block adapters, so their integration suites belong in CI.

### 15.2 S-I2 — file-registration cost and the uncommitted-GC window

**Verdict: stage all files, in a bounded worker pool. 24h grace, enforced in the prepare step.**

Measured on a real `catalog.Catalog` with the shapes §6.3 prescribes (`_lakefs_iceberg/files/<uuid>/data/part-NNNNN.parquet`,
`AddressType=Full`), mem and local KV, mem block adapter, darwin/arm64, `GOMAXPROCS=16`:

| Staging strategy | 10k entries | Per entry |
|---|---|---|
| one `CreateEntry` per file, sequential | **≈76 s** | 6.9–7.6 ms |
| 8 workers | 9.3 s | 0.93 ms |
| **32 workers** | **2.2–2.4 s** | 0.23 ms |
| 32 workers, KV with a 1 ms round trip | **1.18–1.20 s** | — |
| `WriteRange` → `WriteMetaRange` → `Store.Import` | 39–63 ms | not usable (below) |

The sequential floor is not the storage: it is `graveler.max_batch_delay=3ms` paid twice per call in
`batch.BatchFor` when there is a single producer. With the delay set to `0` the same loop runs at
11–18 µs/entry. Each staged entry costs **3 KV operations** (one staging write, two reads), so a 10k-file
append is 30 000 KV operations. The lakeFS commit that publishes the staged entries is cheap by
comparison: **40–70 ms**, one SSTable of ≈0.7 MB, because the commit path does not touch the KV per file.

The batched `Store.Import` path is **rejected** for §6.3: it creates its own commit and takes no
`graveler.WithCondition`, so it would bypass the pointer CAS of §6.2 step 6 and break both the
single-commit guarantee and the 409-on-race semantics. Its import API also rejects source paths inside the
repository's own storage namespace, which is where `_iceberg/` lives.

**Does the uncommitted GC see a client-written file?** Measured against a real catalog:

| Case | Objects | Listed by `PrepareGCUncommitted` |
|---|---|---|
| raw client writes under `_iceberg/`, no lakeFS entry | 200 | **0** |
| staged `AddressType=Full` entries for 5 of them | 5 | **5** |
| staged `AddressType=Relative` entry | 1 | **1** |
| staged entry pointing outside the namespace | 1 | **0** (skipped) |
| the same entries after `catalog.Commit` | 6 | **0** |
| 10k staged file entries, one branch | 10k | 10k (prepare took 15 ms) |

`PrepareGCUncommitted` never walks the storage namespace: it walks branches and takes each branch's
uncommitted diff. **Staging is what puts a table file into the report — nothing else does.** Before
staging, the hazard is not the uncommitted GC at all but the committed-GC sweep, which deletes namespace
objects that no retained commit references.

Guidance this hands to the file-registration task, all of it adopted:

1. stage with a bounded pool of 16–32 goroutines calling `catalog.CreateEntry`; never the import path;
2. order: read the manifest list and manifests, apply the foreign-location check for the whole snapshot
   **first** so nothing is staged for a snapshot we reject, then stage the files, then swap the pointer —
   all in one staging area, so one lakeFS commit publishes files and pointer together;
3. set each entry's `LastModified` from the object's write time (it becomes the `creation_date` the GC job
   ages staged objects by); take size and ETag from the manifest record, never HEAD 10k files;
4. put a hard cap on files per commit (`iceberg.max_staged_files`, recommended 50 000 ≈ 6 s) and reject
   above it — silently skipping files converts a slow commit into a GC-deletable one;
5. keep entry paths deterministic so a retried commit overwrites the same keys.

**`iceberg.uncommitted_gc_grace`:** default **24h**, enforced on the server in the prepare step, not by the
GC job. Rationale and residual windows, which the public docs must state:

- 24h comfortably covers "client appended, operator commits the branch the same day"; an unbounded grace
  would make `_iceberg/` areas permanently uncollectable.
- A staged entry older than the grace is collectable, exactly like any other uncommitted lakeFS write. The
  natural fix is committing the branch, which moves the files into the protected, commit-referenced set.
- The exposure window is `[client writes the object → the branch is committed]`. Server-side protection
  starts only when the catalog stages the entry, so the existing lakeFS rule — do not run GC while writers
  are active — applies unchanged to unregistered files.
- Deletion is decided by the external GC job, which has its own
  `lakefs.debug.gc.uncommitted_min_age_seconds`. An object goes only if it is both listed *and* old enough,
  so the effective protection is the larger of the two; keep the grace at or above that setting.
- A future `lastModified` (clock skew) is protected rather than collected: skew delays, never deletes.

Open items this spike handed on, and where they stand:

| Item | Owner |
|---|---|
| Confirm against a real GC run (append → commit → GC → table still readable; stage → wait → GC → files gone) | I3-3: `esti/iceberg_test.go` covers append → commit → GC prepare → still readable, and `gc_test.go` covers stage → grace → report; the external Spark sweep itself is not run in-repo |
| Decide whether `_iceberg/` may be excluded from the committed-GC sweep; the §13 fallback needs it | §13 / Q-I10 — open |
| Replace the 1 ms-RTT emulation with real `kv/postgres` numbers | I3-1 — optional, not done |
| Whether the 3 ms batching floor can be avoided with a bulk staging primitive (2.3 s → ≈0.3 s) | future graveler work, out of scope for v1 |

### 15.3 S-I4 — PyIceberg and DuckDB against a stub catalog

**Verdict: both clients connect and read without an `endpoints` list, and neither rejects an incomplete
one — but they honour it very differently, and two findings change the design.**

Measured with PyIceberg 0.12.0 and DuckDB 2.0.0 against a ~390-line stub that answered `GET /v1/config`
plus the read endpoints, logging every request. The full request logs are not reproduced here; the
conclusions are:

1. **`{prefix}` in the advertised endpoint strings must stay a literal placeholder.** Advertising
   `GET /v1/lakefs/namespaces` instead of `GET /v1/{prefix}/namespaces` makes **every** PyIceberg call fail
   with `NotImplementedError` **after** a successful connect — zero HTTP requests. PyIceberg parses each
   entry with `Endpoint.from_string` and compares it to its capability templates for byte equality. DuckDB
   tolerates the expanded form. The catalog therefore emits the templates verbatim and a test pins it.
2. **The `endpoints` list is a hint, not an access-control boundary.** Against a read-only list PyIceberg
   issued three requests and refused create/rename/drop client-side, while DuckDB against the *same* list
   sent `DELETE …/tables/{t}?purgeRequested=false` and `POST …/namespaces`. Unimplemented endpoints must
   answer with a correct error envelope, and authorisation must never rely on the client staying away.
3. **An absent list is not the spec default.** PyIceberg assumes 12 endpoints that include
   `tables/rename` but exclude `transactions/commit`, metrics and the `HEAD` endpoints; DuckDB assumes
   nothing. Sending an explicit list is what keeps renew/transactions out of PyIceberg's reach — the reason
   to send it is the opposite of what §2.1 originally recorded.
4. **Multi-table transactions fail cleanly.** With `transactions/commit` advertised, DuckDB commits the
   transaction; without it, DuckDB refuses client-side with a clear message and sends **no** write request.
   It does not silently degrade to two single-table commits.

Required versus optional response fields, measured:

| Field | PyIceberg 0.12 | DuckDB 2.0 |
|---|---|---|
| `config.defaults` / `config.overrides` | optional | **required**, even when empty, and every value must be a JSON string |
| `overrides.prefix` | optional (absent ⇒ no path segment) | optional |
| `idempotency-key-lifetime` | not needed | not needed |
| `namespaces` / `identifiers` in listings | **required** | optional (absent ⇒ empty list) |
| `metadata` on load table | **required** | **required** |
| `metadata-location` on load table | optional | optional |
| `config` on load table | optional | optional |
| `storage-credentials` | optional, even when the client asked for vended credentials | optional, same |
| `metadata` **and** `metadata-location` on the commit response | **both required** | **both required** |

Error handling, measured: both clients read `message` for the user-facing text and choose the exception
class from the HTTP status, not from `type`; PyIceberg's model requires all three fields. `401` triggers
one automatic retry in PyIceberg; `404` becomes `NoSuchTableError` on *any* endpoint; commit `409` becomes
`CommitFailedException` and `500`/`502`/`504` become `CommitStateUnknownException`. Choose the status for
the client-side class you want, not only for its HTTP meaning.

Also measured: both clients send `X-Iceberg-Access-Delegation` (PyIceberg on every request, DuckDB on the
table-scoped ones) and both accept a response with no `storage-credentials` — so vending can land later
without breaking either. And the OAuth flow differs: PyIceberg posts `client_id`/`client_secret` to the
token endpoint (twice at connect time) and DuckDB sends the same credentials as HTTP Basic.

Corrections folded into §2.1 (already applied above): the absent-list default, the literal `{prefix}`, the
`identifier` member and the commit response, and the error-class mapping.

Not covered: Spark and Trino (their request patterns and delegation behaviour are unmeasured), pagination,
PyIceberg versions other than 0.12, real object storage, and credentials vending.

### 15.4 Close-out status of the plan

The board this plan was broken into (`tasks.json`) is closed. The implementation status below is what the
public documentation states as behaviour: every v1 capability is now landed, and what remains is the list
of gaps at the end of this section, not unwired work:

| Area | Status |
|---|---|
| Read path: `GET /v1/config`, namespace and table reads on any ref, `relative_to` in both forms | **landed**; `relative_to` is identity-preserving — a client's own short names round-trip |
| Write path: namespace create/update/drop, table create, drop, metrics, commit (§6.2) | **landed** |
| Commit over HTTP: `POST …/tables/{table}`, staged, CAS, `400` above `iceberg.max_staged_files` | **landed** |
| `register` (§I-2) | **landed and advertised**: `POST /v1/{prefix}/namespaces/{namespace}/register` adopts a client-written `metadata.json`, staging its files and then the pointer (`register.go`) |
| §6.3 file registration as lakeFS entries | **landed and called by the commit path** (`files.go`, step 5), capped by `iceberg.max_staged_files`; **a create stages the initial `metadata.json`** (`Store.CreateTableWithMetadata`) and **a drop releases the table's file entries** in the same commit that removes the pointer (`releaseTableFiles`) |
| §6.3 uncommitted-GC grace and the `_lakefs_iceberg/` deny | **landed and wired**: `Bootstrap` installs the grace via `catalog.SetUncommittedGCProtector` and attaches `iceberg-reserved-prefix-protect`. An auth backend that cannot store policies (the **default** auth mode) degrades to a warning and the deny is **not attached** — a real security gap for those deployments |
| §8 remote signing and vended credentials | **services mounted and advertised** when the blockstore supports them; the table load still returns an empty `config` |
| §7.1 OAuth token endpoint | **landed**: the catalog's own auth middleware serves `POST /v1/oauth/tokens` (form `client_id`/`client_secret` or HTTP Basic) |
| §7.1 cookie reads (added after the close-out) | **landed**: `NewAuthMiddleware` also accepts the web UI's `internal_auth_session` cookie, for `GET` and `HEAD` only. A request carrying only a cookie on `POST`/`PUT`/`PATCH`/`DELETE` — including `POST /v1/oauth/tokens` — is `401`, so no catalog write is reachable from a cookie. Bearer and basic are read first; authorisation stays the `catalog:*` checks |
| Web UI Tables tab (added after the close-out) | **landed**: the repository Tables tab is a read-only explorer of the current ref through `/iceberg/api` (`webui/src/lib/api/iceberg.ts`, `repository/tablesExplorer.tsx`, `tablesDetails.tsx`, `tablesStates.tsx`). `GET /v1/config` picks the explorer, the Enterprise upsell (`501`; also `404` when the path is not routed at all), the permission state (`403`) or the login redirect (`401`); it uses `GET /v1/config`, the namespace listing, the table listing and the table load, and writes nothing |
| §7.2 `catalog:ReadTableData` / `catalog:WriteTableData` and the `CatalogRead` grant | **landed** |
| §6.4 compaction auto-resolve | **landed and wired**: keyed on the `lakefs.compaction.operation-id` snapshot property (not the commit message), installed as the `pkg/catalog` merge hook by `Bootstrap` when `iceberg.enabled` and `iceberg.ignore_compaction_commits` are both true (`commit.go`, `pkg/catalog/merge_conflict.go`) |
| I-5 ETag / `If-None-Match` | **landed** on table load, varying with `snapshots`; no tag on `HEAD` |
| §9 datasets `table` / `namespace` items | **landed**: accepted when `iceberg.enabled`, materialised, authorized at the source, readable through the catalog |
| §12 conformance and esti | iceberg-go external suite **green: 18 of 18 subtests, 0 skipped**, through the `relative_to` URI (`pkg/icebergcatalog/conformance/RESULTS.md`); esti suites written but unexecuted (no docker daemon, no S3 bucket) |

**Remaining gaps** — none of these is unwired work; each is environmental or a v1 non-goal:

- Apache RCK was never run: the evidence host has no JDK or Gradle, so it cannot start.
- No S3-compatible blockstore was available, so every measurement used `blockstore.type=local` with a
  `local://` FileIO shim; S3 presigning, credential vending and remote signing are unvalidated.
- The esti suite compiles and skips — no esti assertion has executed (no docker daemon, no S3 bucket).
- Only `gc/prepare_commits` was run, never the real Spark GC sweep it feeds.
- Views, `/plan`, `/tasks`, rename and multi-table transactions remain v1 non-goals (§1.2) and are never
  advertised.
- The Tables tab's permission state prints `catalog:LoadTable` for the table load; no such action exists —
  the action the catalog checks is `catalog:ReadTable`. The state is otherwise correct, and the tab itself is
  a viewer by design, not a gap.

The user-facing summary of this table lives in [`docs/iceberg.md`](docs/iceberg.md), including the
uncommitted-GC window, the RBAC note for policies written before the data-plane actions existed, the
cookie-on-reads rule and the [Tables tab](docs/iceberg.md#web-ui-the-tables-tab).
