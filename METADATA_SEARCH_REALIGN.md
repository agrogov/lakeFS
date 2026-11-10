# Metadata Search: realign with the current official guide

Status: **RA1, RA2, RA3, RA4, RA4b, RA5, RA6, RA7, RA9, RA10 and RA8 are done** (RA8, the documentation and
acceptance-test card, is the commit that sets this line). X5 - the removal of the server's
`iceberg.metadata_search.enabled` flag - is the one gap id this plan asks for that no card implemented; the
design records it as a deviation. The hr2 deployment manifests prepared for RA4b/RA5 are still unapplied (see
RA4b "Deployment (hr2)"), so the deployment work RA8 does not cover - Spark, Trino and the real Azure run -
stays open for a separate card.

## 1. Why

We built Metadata Search from the public guide as it read when the design was written (2026-11-03/04). The guide has
since been rewritten. The new version moves the tables to another address and deprecates the endpoint we
implemented. This plan lists every gap between our build and the guide as it reads today, and how to close each one.

Sources:

- Current guide, fetched 2026-11-09: https://docs.lakefs.io/guides/metadata-search/
- Earlier guide, Web Archive capture of 2026-09-01 (the only capture):
  https://web.archive.org/web/20260901103227/https://docs.lakefs.io/guides/metadata-search/

The earlier text matches what we built, quoted verbatim: "These tables are always available under the data
repository namespace: `<repo>.<ref>.system.object_metadata`", with `'uri': f'{lakefs_endpoint}/mds/iceberg/api'`.
The exact date the guide changed is unknown.

## 2. What the current guide requires (verbatim quotes)

| # | Guide says | Our build today | Gap |
|---|---|---|---|
| G1 | "served by the lakeFS Iceberg REST catalog at `<lakefs_endpoint>/iceberg/api`, under the reserved `system` namespace: `system.metadata.<repo>.<ref>.objects`" | `<repo>.<ref>.system.object_metadata` on `/iceberg/api`, `/iceberg/relative_to` and `/mds/iceberg/api` | **Missing**: the root `system.metadata` tree and the table name `objects` |
| G2 | "engines can list `system.metadata` to find the repositories that have metadata, and list a repository to find its synced branches. Tags and commit IDs are not listed, but you can load them directly." | our build lists `system.object_metadata` inside each data repository (`80f6927e2`) | **Missing**: listings of the new tree |
| G3 | "mount `relative_to/system.metadata/api` to address tables as `<repo>.<ref>.objects`, or `relative_to/system.metadata.<repo>/api` to address one repository's tables as `<ref>.objects`" | relative_to accepts only `{repo}[.{branch}]` | **Missing** |
| G4 | "A client mounted on a data repository cannot reach `system`, so it needs a second catalog for metadata search." | `relative_to/R.ref` serves `system.object_metadata` | **Contradicts** |
| G5 | "The separate Metadata Search catalog at `<lakefs_endpoint>/mds/iceberg/api` is deprecated … Until then it keeps serving the `<repo>.<ref>.system.object_metadata` identifier" | `/mds/iceberg/api` is an alias of the lakeFS catalog itself (same handler), and every mount serves the old identifier | **Contradicts**: the guide's deprecated catalog is *separate*; the lakeFS catalog never serves the old identifier |
| G6 | "A tag name that contains dots … Pass the identifier as separate parts instead, e.g. `('system', 'metadata', 'my-repo', 'v0.2.1', 'objects')`" | n/a | Covered by G1 if the namespace levels are not split on dots |
| G7 | Permissions: "allow `catalog:ReadTable` and `catalog:ReadTableData` on `arn:lakefs:catalog:::table/<repo>/system/object_metadata`, and `catalog:GetNamespace` and `catalog:ListNamespaces` on `arn:lakefs:catalog:::namespace/<repo>/system`" | Authz address is `R/system/object_metadata`, but every request also needs `fs:ReadRepository` on R | ARN **matches** (the new identifier must authorize at it); the guide's grant alone is **refused** by our build (X14, RA10) |
| G8 | "Vended credentials for metadata tables are always read-only." | Read-only vending and signing for the virtual table | Matches; extend to the new address |
| G9 | "Metadata repositories are hidden from repository listings, but remain reachable by name." | `R-metadata` is listed everywhere | **Missing** |
| G10 | Config: "`lakefs_endpoint` … Required." "`lakefs_mds_endpoint` - deprecated in favor of `lakefs_endpoint`. When only this key is set, it must end in `/mds/iceberg/api`, and the service logs a warning and uses the URL without that suffix" | `lakefs_mds_endpoint` required, and it is a **base URL** without the suffix | **Contradicts**: our existing configs (hr2) break under the new rule |
| G11 | "`access_key_id` (string : "") - optional … Set together with `secret_key`"; validation: "both set or both empty", period > 0, concurrency >= 1, max_commits >= 1 | Keys required | **Missing**: optional keys |
| G12 | Sync identity: "writes the metadata tables through the lakeFS Iceberg REST catalog as `mds-service-user`, which lakeFS provisions on every start with the catalog permissions the sync needs … signs its own login tokens for that user with `auth.encrypt.secret_key`". Another user needs "the `MDSCatalogAccess` policy". "lakeFS detaches the read-only `MDSFSReadAccess` policy from `mds-service-user` on start" | User `mds-indexer`, group `MetadataSearchIndexers`, policies `mds-indexer` and `mds-metadata-repo-protect`, static keys, writes via `/iceberg/relative_to` | **Contradicts** (names, provisioning, auth, endpoint) |
| G13 | Schema: `user_metadata` required **no** ("If none exists, an empty map is stored"); `creation_date` present | `user_metadata` required; `creation_date` present; `size_bytes` | `user_metadata` must become optional |
| G14 | The Web UI Metadata Search tab and query service (`iceberg_query.*`, Helm `iqs.*`), "Available in lakeFS Team and lakeFS Enterprise" | Not built (option B) | Out of scope unless asked |

**The rule for this plan:** `system` is a single reserved namespace at the root of the lakeFS catalog. There is no
`system` namespace inside any data repository. The lakeFS catalog (`/iceberg/api` and `relative_to`) has nothing
virtual, listed or reserved below `<repo>.<ref>`.

The old identifier `<repo>.<ref>.system.object_metadata` belongs only to the guide's deprecated, *separate* catalog
at `/mds/iceberg/api` (G5), never to the lakeFS catalog (section 4).

The physical table stays where the guide puts it, "on each matching branch in the metadata repository". Its path
inside `R-metadata` (`system.object_metadata` today) is not named by the guide. It is an ordinary namespace in a
hidden repository (G9), not a reserved one.

## 3. What we built against the old guide, and commit `b2e7747ac`

Everything we built to follow the old guide's `<repo>.<ref>.system.object_metadata` identifier. None of it has
backing in the current guide:

- **The virtual table** `<R>.<ref>.system.object_metadata` (`80f6927e2`).
- **The 403 on writes** under `<R>.<ref>.system` (`80f6927e2`, extended to the data plane in `d2811fdcc` and
  `500a00a37`).
- **The listing** of `system` and `object_metadata` in every data repository (`80f6927e2`).

`b2e7747ac` narrowed that listing to refs where the table resolves. Hiding a per-repo `system` has no purpose, so it
was reverted in `a9949864d`. Until RA3 lands, every data repository lists `system/object_metadata` again, and loading
it answers 404 where the repository is not indexed (the `jeff-training` case).

RA3 removes the remaining three parts from the lakeFS catalog, on every mount it has (`/iceberg/api`, `relative_to`,
and the `/mds/iceberg/api` alias).

## 4. What the guide settles (former D1–D3)

There are no open decisions. The rule is the guide's text; where the guide is silent on something a user can see,
we do not have it.

- **`/mds/iceberg/api`** — "The separate Metadata Search catalog at `<lakefs_endpoint>/mds/iceberg/api` is
  deprecated and will be removed in a future release. Until then it keeps serving the
  `<repo>.<ref>.system.object_metadata` identifier." So it stays, as a **separate** catalog serving that identifier,
  resolved to the same table as `system.metadata.<repo>.<ref>.objects`. It is not an alias of the lakeFS catalog.
  The guide's only other sentence about it describes earlier versions: "which did not check catalog permissions".
  Task RA3.
- **A repository named `system`** — "served … under the reserved `system` namespace". `system` at the root is
  reserved, so it is the metadata tree. Nothing else is added (no warning, no special case).
- **The init-commit proof** — the guide has no init commit and no proof. It does say the sync may run "as another
  user instead" with `MDSCatalogAccess`, so any check pinned to one committer name contradicts it. The committer
  check (MDS-SEC-3) is removed. Task RA5.

## 4b. What we built that the guide does not have

How it happened: the design (`METADATA_SEARCH_DESIGN.md`, 2026-11-03/04) claims to be "written only from the public
guide", but it was not. I added features to it that the guide never had, and later my own review passes
(`MDS_REVIEW`, `review/mds` of 2026-11-05) turned their findings into more features, again without checking them
against the guide or marking them as deviations. Some of it contradicts even the old guide: the old guide already
named the user `mds-service-user`, and we called ours `mds-indexer`. Then I never re-read the live guide before
building, which is how the identifier change of §1 was missed.

Each item below is removed or changed to the guide's text. Internal mechanics no user can see or configure (the
`mds-<commit>` tags, the `mds: init` commit, retry and timeout constants) may stay, and are not documented as
features.

| # | Ours | Guide | Change | Task |
|---|---|---|---|---|
| X1 | Principal `mds-indexer`, group `MetadataSearchIndexers`, policy `mds-indexer` | `mds-service-user`, policy `MDSCatalogAccess` (old guide also: `mds-service-user`) | Replace | RA5 |
| X2 | Deny policy set `mds-metadata-repo-protect[-<n>]` on `R-metadata` | Not in the guide | Remove | RA5 |
| X3 | Commit guard `iceberg-reserved-prefix-protect` for Metadata Search names | Not in the guide | Remove the Metadata Search part | RA5 |
| X4 | Init committer check MDS-SEC-3 | Not in the guide; contradicts "as another user instead" | Remove | RA5 |
| X5 | Server flag `iceberg.metadata_search.enabled`, and the server reading `metadata_search.repositories` | Not in the guide: "Metadata search activates when this map is non-empty" | Remove the flag; the server serves `system` when metadata repositories exist | RA1 |
| X6 | Keys `request_timeout`, `backfill`, `seed_lookback`, `rebuild_threshold`, `delete_branches`, `tag_retention`, `compaction.*`, `storage.*`, `azure.*` | Not in the guide's key list | Remove from config; behaviour fixed at today's defaults | RA4 |
| X7 | `access_key_id`/`secret_key` required | Optional, both or neither | Change | RA4 |
| X8 | Empty `repositories` or `branches` fails startup | "activates when this map is non-empty" | Empty map means disabled | RA4 |
| X9 | A repository name ending in `-metadata` or longer than 54 characters fails startup | Not in the guide | Remove | RA4 |
| X10 | A bucket- or container-root namespace with no `storage_namespace` fails | "If omitted, lakeFS appends `-metadata` to the repository's storage namespace" | Append, as the guide says | RA4 |
| X11 | Default `backfill: head` (only the head's objects are indexed when a branch is first seen) | `since`: "If omitted, metadata is captured from the time the branch was created" | History from the branch's creation, as the guide says | RA4 |
| X12 | `user_metadata` required | Required: no | Change | RA7 |
| X13 | Indexer is a REST-only client reading only `metadata_search` | Chart: same binary, same configuration, opens KV, builds the catalog | Run in-process on the server configuration | RA4b |
| X14 | Every catalog request also needs `fs:ReadRepository` on the repository, and every write also `fs:WriteObject` (`pkg/icebergcatalog/authz.go:601-608`, our design §7.2) | The guide's grant for Metadata Search alone has only `catalog:*` actions; the Iceberg docs' example policies have no `fs:*` action, and "Iceberg catalog permissions supersede any related object permissions" | Catalog requests authorize the catalog action only | RA10 |

**Object-store credentials of the sync (X6 `storage.*`, `azure.*`).** The guide has no such keys. The official
lakefs Helm chart (1.12.48, deployed on hr2 under `apps/lakefs/charts/lakefs`) answers how upstream does it:
`templates/mds/_mds.tpl` says "The `lakefs mds run` subcommand opens the KV store, builds the catalog, and validates
the license against the same configuration the lakeFS server reads", and gives the MDS pod the server's shared env
(`lakefs.sharedSecretEnv`) plus `mds.extraEnvVars` / `mds.extraEnvVarsSecret`. So the sync reads the server's own
`blockstore.*` settings, the same env vars the server pod gets from its secret (on hr2:
`LAKEFS_BLOCKSTORE_TYPE=azure`, `LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT`/`_ACCESS_KEY` from `lakefs-secrets`).

RA4 therefore removes `storage.*` and `azure.*` and has the sync build its object-store access from the standard
lakeFS `blockstore.*` configuration (file or `LAKEFS_BLOCKSTORE_*` env). This reverses design §4.6's "deliberately
does not read `blockstore.*`". On hr2 the MDS pod already gets `LAKEFS_BLOCKSTORE_TYPE` and the
`LAKEFS_BLOCKSTORE_AZURE_*` keys (RA4b "Deployment (hr2)"); the `LAKEFS_METADATA_SEARCH_AZURE_*` vars go with RA4.
esti passes `LAKEFS_BLOCKSTORE_S3_*` for SeaweedFS.

**Process model of `mds run` (X13).** The chart's PR that wired the secrets in
([treeverse/charts#398](https://github.com/treeverse/charts/pull/398)) says the MDS process "shares the same binary
as the lakeFS server and reads the same configuration" to "open the KV store, build the catalog, and validate the
license", and so "needs the same database connection string, auth encrypt key, and license token". Ours is the
opposite: design §2 says the indexer "is a **REST client** of lakeFS … does not import `pkg/catalog` or
`pkg/graveler` and does not touch KV or the block adapter", it reads only the `metadata_search` section, and it reads
data repositories through the REST API (`pkg/mds/client`). Task RA4b fixes this.

What the sources fix and what they do not:

- Fixed by the chart: same binary, `mds run`, the full server configuration (database, `auth.encrypt.secret_key`,
  blockstore, license), the KV store opened and the catalog built in the MDS process.
- Fixed by the guide: the tables are written "through the lakeFS Iceberg REST catalog" at
  `<lakefs_endpoint>/iceberg/api` as `mds-service-user`, and startup checks that endpoint "answers as the lakeFS
  Iceberg REST catalog". So writes stay on REST even though KV is open in-process.
- Not stated anywhere: what the MDS process reads through KV. The reading this plan takes is the one the two
  sentences leave: data repositories (refs, commits, diffs, object metadata) are read in-process through
  `pkg/catalog` over KV and the block adapter, replacing `pkg/mds/client`.

## 5. Tasks

One commit per task, each verified per `METADATA_SEARCH_PLAN.md` "Verification rules". The order respects the
dependencies.

### RA1 — the root `system.metadata` tree on `/iceberg/api` (G1, G2, G6, G7, G8)

- **Resolver.** `system.metadata.<R>.<ref>.objects` resolves to `<R>-metadata.<mapped ref>.system.object_metadata`.
  - Same ref mapping as today: a branch maps to itself; a tag or commit goes through the RefMapper.
  - **Authz address:** `R/system/object_metadata`, so the guide's ARNs in G7 apply unchanged.
- **Namespaces.** The tree is `system` → `system.metadata` → `system.metadata.<R>` → `system.metadata.<R>.<ref>`.
  - None has a marker.
  - GET/HEAD answer 200 when the node exists, with existence derived from the companion repo and its branches.
  - The whole tree is reserved, so every write answers 403 `ForbiddenException`.
- **Listings.**
  - Root: `system` is listed when at least one metadata repository exists (X5: there is no server flag).
  - `system`: lists `metadata`.
  - `system.metadata`: lists every R the caller may read whose `objects` resolves on at least one branch.
  - `system.metadata.R`: lists R's synced branches (the companion's branches whose table resolves). Tags and commits
    are not listed.
  - `system.metadata.R.ref`: lists `objects`.
  - All listings run the existing authorization filter.
- **Credentials.** Vended credentials and remote signing use the redirected target, read-only as today.
- **Tests.**
  - Load, head and list at every level.
  - Branch, tag and commit refs, including a dotted tag passed as separate levels.
  - 403 on every write.
  - A caller with only the guide's grant (`catalog:ReadTable`/`ReadTableData` on
    `table/<R>/system/object_metadata`, `catalog:GetNamespace`/`ListNamespaces` on `namespace/<R>/system`) reads,
    lists and gets read-only vended credentials, with no `fs:*` grant (needs RA10).
  - A caller without `catalog:ReadTable` on that ARN gets 403.
  - A caller with no grant on `R-metadata` can still read.
  - An unindexed R is not listed.
- **Files:** `pkg/icebergcatalog/resolver.go`, `namespaces.go`, `tables.go`, `creds.go`, `sign.go`, and tests.
- **Depends on:** RA10 for the guide-grant test.

### RA2 — `relative_to/system.metadata[.<R>]/api` (G3)

- Extend the warehouse parser (`relative.go`) to accept `system.metadata` and `system.metadata.<R>`, prefixing the
  levels the warehouse fixes.
- **Tests:** DuckDB-shaped addressing as `<repo>.<ref>.objects` and `<ref>.objects`, plus an invalid warehouse
  answering the existing 404.
- **Depends on:** RA1.

### RA3 — delete the per-repo code from the lakeFS catalog; `/mds/iceberg/api` as a separate catalog (G4, G5)

- **Delete the three parts listed in section 3 from the lakeFS catalog code** (`80f6927e2`, `d2811fdcc`,
  `500a00a37`; `b2e7747ac` is already reverted in `a9949864d`): the virtual table resolution, the listing injection, and the per-repo reservation. After
  this, the lakeFS catalog handles every address below `<repo>.<ref>` the same way, with no name treated specially.
- **Keep only the root reservation:** `ReservedNamespace` and the data-plane refusals apply to the root `system`
  tree from RA1, and nowhere else.
- **`/mds/iceberg/api`:** a separate catalog that serves (load, head, list) only `<repo>.<ref>.system.object_metadata`,
  resolved through RA1's resolver. It is never again an alias of the lakeFS catalog.
- **UI:** the Tables tab of a data repository no longer shows anything the user did not create.
- **Tests:**
  - On `/iceberg/api` and `relative_to`: with `R` indexed, listings of `R.main` show only what the user created, and
    the old identifier answers 404 like any absent table.
  - The root tree from RA1 serves the indexed table, and refuses writes.
  - `/mds/iceberg/api`: the old identifier loads the same table as the new one; nothing else resolves there.
  - Delete or rewrite the tests that pin the old behaviour: `TestMetadataSearchReservedNamespace`, the listing
    subtests of `TestMetadataSearchVirtualTableRead`, and the esti `system`-of-`R` refusal (`8ae229ec2`).
- **Docs:** remove every statement that a data repository has a `system` namespace, from `docs/metadata-search.md`
  and design §5.1–§5.3.
- **Depends on:** RA1.

### RA4 — indexer configuration (G10, G11, X6–X11)

- `lakefs_endpoint` is required: the base URL, with writes going to `<lakefs_endpoint>/iceberg/api`.
- `lakefs_mds_endpoint` is deprecated:
  - accepted only when it ends in `/mds/iceberg/api`;
  - the suffix is stripped, with a warning.
- `access_key_id` and `secret_key` are optional: both or neither.
- Validation is exactly the guide's: endpoint set, keys both or neither, `period > 0`, `concurrency >= 1`,
  `max_commits >= 1`. Nothing else fails startup (X7, X9).
- An empty `repositories` map disables Metadata Search instead of failing (X8).
- Remove the keys of X6; object-store access comes from the standard `blockstore.*` config, as for the server. A missing `storage_namespace` appends `-metadata` to R's namespace in every case (X10).
- First sync of a branch captures metadata "from the time the branch was created", or from `since` (X11).
- **Migration note:** our current `lakefs_mds_endpoint` is a base URL, so existing configs (hr2) must be renamed to
  `lakefs_endpoint`. The old form fails validation with a message that names the fix.
- **Files:** `pkg/mds/config.go`, `pkg/mds/table/catalog.go`, `pkg/mds/indexer/indexer.go`, config tests.

### RA4b — `mds run` on the server binary and the server configuration (X13)

- **Command:** `mds run --config <file>` on the server binary, as the chart invokes it
  (`args: [mds, run, --config, /app/config.yaml]`). It loads the full lakeFS configuration the way `run` does (file
  plus `LAKEFS_*` env), not only `metadata_search`.
- **In-process:** open the KV store from `database.*`, build `pkg/catalog` (graveler and the block adapter from
  `blockstore.*`), and read `auth.encrypt.secret_key` for RA5's token signing. A missing or unreachable database
  fails startup, like `run`.
- **Reads of data repositories:** branches, commits, diffs and object metadata come from `pkg/catalog` in-process.
  `pkg/mds/client` and its REST reads are removed.
- **Writes:** unchanged in kind: through the Iceberg REST catalog at `<lakefs_endpoint>/iceberg/api` (guide). Parquet
  and Avro files are written with the block adapter's credentials (replaces X6 `storage.*`/`azure.*`).
- **License:** OSS has no license check; nothing to validate.
- **Deployment (hr2):** the chart's `lakefs.sharedSecretEnv` passes the database string only from
  `secrets.databaseConnectionString` or `secretKeys.databaseConnectionString`; hr2 sets neither (the server gets it
  via `extraEnvVars` from `lakefs-pg-app`). The chart never fills `access_key_id`/`secret_key`: its `mds.config`
  default has example keys only.
  - **Done (2026-11-09, `infobip-balt-ai`, uncommitted, not applied):** `mds.extraEnvVars` adds
    `LAKEFS_DATABASE_TYPE=postgres`, `LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING` from `lakefs-pg-app` and
    `LAKEFS_BLOCKSTORE_TYPE=azure`; `mds.extraEnvVarsSecret: lakefs-secrets` gives the Azure keys (and also the
    `LAKEFS_INSTALLATION_*` admin keys, as on the server pod). The `LAKEFS_METADATA_SEARCH_ACCESS_KEY_ID`/`SECRET_KEY`
    vars, `mds-indexer-sealed-secret.yaml` and its `kustomization.yaml` entry are deleted; `access_key_id: null` and
    `secret_key: null` stay, so the chart's example keys never reach the config and the sync runs as
    `mds-service-user` (RA5). The current `dev-mds` image requires the keys, so these manifests must not be synced
    before an image with RA4, RA4b and RA5 is deployed.
  - **Left for that deployment:** remove the `LAKEFS_METADATA_SEARCH_AZURE_*` vars (RA4), rename
    `lakefs_mds_endpoint` to `lakefs_endpoint` (RA4 migration note), delete the `mds-indexer` user, its access key,
    `MetadataSearchIndexers` and the `mds-indexer` policies in lakeFS (RA5 bootstrap does it), and the
    `mds-indexer-creds` Secret if Argo CD does not prune it.
- **esti:** the MDS container gets the same database and blockstore settings as the lakeFS container.
- **Tests:** `mds run` against the same KV and blockstore as an in-process server in unit tests; esti end-to-end.
- **Depends on:** RA4. RA5 depends on it.

### RA5 — sync identity `mds-service-user` (G12, X1–X4)

- **Server bootstrap, on every start:**
  - provision user `mds-service-user`;
  - create or repair policy `MDSCatalogAccess` (the catalog permissions the sync needs) and attach it;
  - detach `MDSFSReadAccess` if attached;
  - remove `mds-indexer`, `MetadataSearchIndexers`, `mds-metadata-repo-protect[-<n>]` and the Metadata Search part of
    `iceberg-reserved-prefix-protect` (X1–X3).
- **Indexer authentication.**
  - With no keys, it signs its own login token for `mds-service-user` with `auth.encrypt.secret_key`, which must
    match the server's.
  - With keys, it logs in as that user, who needs `MDSCatalogAccess`.
- **Startup check** (guide, verbatim): "checks that `lakefs_endpoint` answers as the lakeFS Iceberg REST catalog, and
  stops if it does not or stays unreachable for two minutes. If the catalog rejects the service's token or denies it
  access, the service logs the likely causes, starts anyway, and retries on every sync cycle."
- **Init-commit proof:** removed (X4).
- **Tests:** bootstrap idempotence and repair, token signing and expiry, a sync run as another user with `MDSCatalogAccess`, and the
  esti end-to-end run with no keys configured.
- **Depends on:** RA4b.

### RA6 — hide metadata repositories (G9)

- Exclude `*-metadata` companion repositories from `ListRepositories` (REST API, which also covers lakectl and the
  UI) and from the catalog root listing. Get by name keeps working.
- "Companion" means a repo with the proven init commit, not every name ending in `-metadata`, so a user's own
  `foo-metadata` is not hidden.
- **Open check before coding:** the cost of the proof per listed repo. Cache it the way the init-commit cache already
  does.

### RA7 — `user_metadata` optional (G13)

- Flip `Required` to false in `pkg/mds/table/schema.go`, keeping the same field ID. Iceberg allows making a required
  field optional.
- The writer applies a schema update to existing tables once.
- Rows keep writing an empty map, as the guide says.

### RA8 — docs and esti

- Rewrite `docs/metadata-search.md` to the new identifiers, endpoint, config keys and identity, with a
  deprecated-catalog section mirroring the guide's.
- Update `METADATA_SEARCH_DESIGN.md` and `METADATA_SEARCH_PLAN.md`.
- Move `esti/mds_test.go` and the engine recipes (`esti/ops/mds-engines/`) to `system.metadata.<R>.<ref>.objects`
  on `/iceberg/api` and `relative_to/system.metadata/api`. Keep one case on `/mds/iceberg/api` with the old
  identifier.
- Re-run Spark, Trino, PyIceberg and DuckDB, and AZ7 on hr2 after the config rename.

### RA9 — UI write buttons under the root `system` tree

- The Tables tab browses one repository at a time and the root `system` is not inside any repository, so today it
  never shows `system`. This task is needed only if the UI later browses the root `system` tree. Then hide
  "New table" and the drop buttons there, driven by the server (namespace properties or `/v1/config`) rather than
  hard-coded names.

### RA10 — catalog authorization per the Iceberg docs (X14)

- Source: https://docs.lakefs.io/iceberg/ (fetched 2026-11-09). Its example policies ("Read-only access to all
  tables and namespaces", "Read-write access to all tables in a repository", "Full access to tables in a specific
  repository") grant only `catalog:*` actions on catalog ARNs, and it states "Iceberg catalog permissions supersede
  any related object permissions". No `fs:*` action is listed for catalog requests (only for the Tables Cleanup
  job). The Metadata Search guide's grant "for Metadata Search alone" is likewise catalog-only.
- Change: `pkg/icebergcatalog/authz.go` authorizes each catalog request with the catalog action on its catalog ARN
  only; drop the `fs:ReadRepository` leaf and the `fs:WriteObject` leaf on writes. Update the comments that
  describe them (`creds.go`, `sign.go`, `tables.go`, `namespaces.go`, `views_http.go`, `register.go`,
  `commit_http.go`) and our docs.
- **Tests:** each of the docs' example policies, attached alone, allows exactly what it says; the read-only one also
  vends read-only credentials; a principal with `fs:*` but no `catalog:*` gets 403.
- **Depends on:** nothing. RA1's guide-grant test depends on it.

## 6. Out of scope

- G14: the Web UI Metadata Search tab and its query service (Enterprise in the guide, option B in our design).
- Tenants ("membership in every tenant"): OSS refuses the tenant header (R9-1).
