# Metadata Search for lakeFS OSS — design

Status: revised for implementation. Companion: [`METADATA_SEARCH_PLAN.md`](METADATA_SEARCH_PLAN.md).
Builds on [`ICEBERG_CATALOG_PLAN.md`](ICEBERG_CATALOG_PLAN.md) and `docs/iceberg.md` (section "Metadata Search").

Clean-room note: upstream Metadata Search is a lakeFS Team/Enterprise feature. This is an independent OSS
implementation written only from the public guide (https://docs.lakefs.io/guides/metadata-search/), the
Apache Iceberg specs and this repository's code. No upstream Enterprise code, configs or binaries are used.

## 1. Goal and scope

Let users find objects by system metadata (path, size, last modified, committer, content type, etag) and
user metadata, with any Iceberg engine, at any indexed branch, tag or commit of a repository.

Reference behaviour (upstream guide):

- for each data repository `R`, a companion repository `R-metadata` holds the table `system.object_metadata`,
  with branches named like the indexed branches of `R`;
- a background indexer keeps the table in sync with the commits of `R`;
- the catalog resolves `R.<ref>.system.object_metadata` to that table in `R-metadata`;
- one row per object per branch, eventually consistent, atomic per commit of `R`.

### Delivery options

| Option | Content | Status |
|---|---|---|
| **A** | Indexer, `R-metadata` management, catalog resolution. Users query with DuckDB, Spark, Trino or PyIceberg. | **This design, to be built.** |
| **B** | A + Web UI "Metadata Search" tab + a server-side query service (embedded DuckDB). | Documented for later (section 11). |

Non-goals of A: UI tab, server-side SQL, multi-tenancy, Azure/GCS credential vending, full-text or vector search,
exact upstream `commit_id` semantics for objects that existed before the first indexed commit (section 3.2).

## 2. What exists and what is new

| Piece | Where | State |
|---|---|---|
| Virtual table resolution, authz on `R`, read-only `system` namespace, `iceberg.metadata_search.enabled` | `pkg/icebergcatalog/resolver.go`, `http.go`, `config.go`, `docs/iceberg.md` | **Done** (R10-1, commit `80f6927e2`). Section 5 records the landed API. |
| `/mds/iceberg/api` alias of the catalog (upstream endpoint) | `pkg/icebergcatalog/http.go` | **New**, small (task R10-2, section 5.5). |
| `RefMapper` for tags and commits | new `pkg/mds/refmap`, installed from `cmd/lakefs-sso/main.go` | New (section 6). |
| Indexer (`lakefs-sso mds run`) | new `pkg/mds/...`, registered in `cmd/lakefs-sso/main.go` | New (sections 4, 7). |
| Bootstrap of indexer principal and `R-metadata` deny policy | new `pkg/mds/bootstrap.go`, extra routes builder in `cmd/lakefs-sso/main.go` | New (section 8). |

The indexer is a **REST client** of lakeFS (lakeFS API + the Iceberg REST catalog). It does not import
`pkg/catalog` or `pkg/graveler` and does not touch KV or the block adapter. The only server-side code is the
`RefMapper`, the alias mount and bootstrap.

## 3. Data model

### 3.1 Table `system.object_metadata` (in `R-metadata`)

Iceberg format **v2**, unpartitioned, sort order `path ASC`, `identifier-field-ids = [2]`.
Field IDs below are the ones the created table has; schema changes only add optional fields with new IDs.
The catalog's create path (iceberg-go `NewMetadataWithUUID`) renumbers IDs in pre-order, so the map's key and
value come right after `user_metadata`. Readers and the writer take IDs from the loaded table, never from constants.

| ID | Column | Iceberg type | Req. | Value |
|---|---|---|---|---|
| 1 | `repository` | string | required | data repository name `R` |
| 2 | `path` | string | required | object key |
| 3 | `commit_id` | string | required | full ID of the commit of `R` that last added or changed the object on this branch (3.2) |
| 4 | `size_bytes` | long | required | object size |
| 5 | `last_modified` | timestamptz (µs) | required | object mtime (`DBEntry.CreationDate` / diff `right.mtime`) |
| 6 | `etag` | string | required | object checksum, content only |
| 7 | `user_metadata` | map<string,string> (key ID 8, value ID 9, value optional) | required | user metadata, empty map when none (never null); the indexer never writes a null value |
| 10 | `committer` | string | optional | committer of `commit_id` |
| 11 | `content_type` | string | optional | content type |
| 12 | `creation_date` | timestamptz (µs) | optional | creation time of the commit `commit_id` (not of the object) |

The map value is optional because iceberg-go v0.7.0 reads parquet map columns with nullable items; a required
value makes its own copy-on-write rewrite and compaction fail with "record schema does not match writer's".

User metadata keys are stored as lakeFS stores them. Objects written through the S3 gateway have keys
`X-Amz-Meta-<lowercased name>` (`pkg/gateway/operations/operation_utils.go`), as upstream documents.

Table properties set at creation:

```
write.delete.mode = merge-on-read
write.update.mode = merge-on-read
write.parquet.compression-codec = zstd
lakefs.mds.schema-version = 1
lakefs.mds.repository = <R>
```

Property updated on **every** indexed commit (section 4.3): `lakefs.mds.source-commit = <full commit ID of R>`.
Every snapshot the indexer produces also carries `lakefs.mds.source-commit` in its summary.

### 3.2 Row semantics

- One row per `(branch, path)`. An added/changed object replaces its row; a removed object deletes it.
- Commits are walked along the **first-parent** chain of the branch. For a merge commit the diff against its
  first parent is applied, so objects brought in by a merge get the **merge commit's** ID, committer and date.
- Backfill rows (objects present at the starting commit, section 4.5) get the starting commit as `commit_id`.
  Upstream defines `commit_id` as the latest commit that changed the object; finding that per object costs a
  log walk per path, so this is a documented deviation for pre-index history only.
- Objects on a branch that is not indexed have no rows; commits not on an indexed branch's first-parent chain
  are not addressable (section 6).

### 3.3 `R-metadata` repository

| Property | Value |
|---|---|
| Name | `R + "-metadata"` (`icebergcatalog.MetadataRepositorySuffix`). `R` longer than 54 characters cannot be indexed (repository IDs are at most 63 characters); config validation rejects it. A data repository whose own name ends in `-metadata` cannot be indexed either (`icebergcatalog.IsMetadataRepository` treats it as a companion). |
| Default branch | same name as `R`'s default branch |
| Storage ID | same as `R` |
| Storage namespace | `repositories.<R>.storage_namespace` if set; else `<R namespace without trailing "/">-metadata` **only if** `R`'s namespace has a path component (`s3://bucket/path` → `s3://bucket/path-metadata`). A bucket-root namespace (`s3://bucket`) requires an explicit value. The namespace must **not** be inside `R`'s namespace (nor `R`'s inside it): `R`'s uncommitted GC sweeps every unreferenced object under `R`'s prefix and would delete `R-metadata`'s files. Validated at startup with the same `"/"`-normalised prefix test the GC uses (`normalizeStorageNamespace`). |
| Read-only flag | **not** set: catalog writes never pass `graveler.WithForce`, so a read-only repository would also block the indexer. User writes are blocked by a deny policy instead (section 8). |
| Contents | namespace `system`, table `system.object_metadata` (pointer under `_lakefs_iceberg/tables/…`, files under `<ns>/_iceberg/<table-uuid>/`). No other bookkeeping objects. |
| Commits | one lakeFS commit per indexed commit of `R`, message `mds: index <R>@<commit>`, metadata `lakefs.mds.source-commit=<commit>`, `lakefs.mds.source-repository=<R>`. |
| Initialisation | the initialisation commit carries message `mds: init` and metadata `lakefs.mds.init=true`. It is the first commit after lakeFS's automatic `Repository created` root that a non-bare `CreateRepository` writes; for a companion repository created bare it is the root itself. A repository whose whole history is that automatic root is initialised by adding the namespace, table and `mds: init` commit on top (section 4.8). |
| Tags | `mds-<full commit ID of R>` on the lakeFS commit that recorded it (section 6). |

There is no `state.json`: the indexing position of branch `b` is the `lakefs.mds.source-commit` metadata of
the head commit of `R-metadata@b`. It moves atomically with the table, because both are in the same lakeFS commit.

## 4. Indexer

### 4.1 Process model

`lakefs-sso mds run --config <file>` reads the top-level `metadata_search` section (section 7) and runs until
SIGTERM. It is a separate process (a goroutine mode inside the server is out of scope for A). It needs only HTTP
access to lakeFS (`lakefs_mds_endpoint`) and, unless credentials are vended, write access to the object store
(section 4.6).

Per tick (`period`): for each configured repository, list `R`'s branches, match them against `branches`
(exact names and `prefix*` globs; only a trailing `*` is supported), and process up to `concurrency` branches
in parallel. Within a branch everything is sequential.

Run **one** indexer replica. Correctness under accidental concurrency comes from the Iceberg CAS and the
idempotency check (4.4), not from a lease; a second replica only wastes work and produces 409s.

### 4.2 Per-branch tick

1. **Recover** (4.4).
2. `last` = `lakefs.mds.source-commit` of `R-metadata@b` head commit (none on a branch that was never indexed → 4.5).
3. `head` = head commit of `R@b`. If `last == head`, done.
4. If `last` is not an ancestor of `head` (`findMergeBase(R, last, head)` ≠ `last`) → history rewrite (4.7).
5. `todo` = first-parent commits of `R@b` after `last` up to `head`, **oldest first**, at most `max_commits`
   (`logCommits(R, head, first_parent=true, stop_at=last)` paged, then reversed).
6. For each commit `C` in `todo`, apply it (4.3). A failure stops this branch for this tick; the next tick resumes at
   the recorded position.

### 4.3 Applying one commit `C` of `R`

1. **Change set.** `P` = first parent of `C`. If `C` has no parent, every object of `C` is an upsert
   (paged `listObjects(R, C)`). Else page `diffRefs(R, left=P, right=C, type=two_dot, include_right_stats=true)`,
   no delimiter. `added`/`changed` → upsert, `removed` → delete. `include_right_stats` gives `checksum`, `mtime`,
   `content_type`, `metadata`; `size_bytes` is on the diff. The parameter is marked experimental in the swagger;
   if a needed field is missing, `statObject(R, C, path)` fills it in.
   `committer` and `creation_date` come from `C` (`getCommit`), the same for every row of the commit.
2. **Large change sets.** If upserts + deletes > `rebuild_threshold` (default 100 000), skip the delta and do a
   full rebuild of the branch at `C`: rows = paged `listObjects(R, C)`, all with `commit_id = C`, written with
   `Transaction.Overwrite` (no filter). This bounds memory and the size of delete predicates. The loss of
   per-object `commit_id` history on a rebuild is documented.
3. **Iceberg transaction** on the table, via `iceberg-go` v0.7.0 `catalog/rest` against
   `<lakefs_mds_endpoint>/iceberg/relative_to/<R>-metadata.<b>/api`, identifier `system.object_metadata`:
   - `tx.Delete(ctx, In("path", upserts ∪ deletes), props)`. With `write.delete.mode=merge-on-read` on a v2 table
     this writes **position delete** files (supported by PyIceberg, DuckDB, Spark, Trino). The `IN` list is
     chunked (≤ 10 000 paths per `Delete` call) inside the same transaction.
   - `tx.Append(ctx, rows, props)` with an Arrow record reader built from the upserts (Arrow schema from
     `table.SchemaToArrowSchema(schema, nil, true, false)`).
   - `tx.SetProperties({"lakefs.mds.source-commit": C})` (makes an empty change set a metadata change too).
   - `props = {"lakefs.mds.source-commit": C, "lakefs.mds.source-repository": R}` on every snapshot.
   - `tx.Commit(ctx)` → one `POST …/tables/object_metadata` with `assert-ref-snapshot-id(main)` and
     `assert-table-uuid`. The catalog **stages** the new pointer and the files on `R-metadata@b`
     (`Store.Commit`: "staged on addr.Ref … part of the repository only on an explicit lakeFS commit").
4. **lakeFS commit** of `R-metadata@b`: `POST /repositories/<R>-metadata/branches/<b>/commits` with the message
   and metadata of 3.3, `allow_empty=false`. The commit is never legitimately empty: step 3 always moves the
   `lakefs.mds.source-commit` property, even for a change set with no objects. The walk of 4.2 step 5 must
   therefore drop the recorded position itself even though the log API's `stop_at` is inclusive; applying it
   again would stage nothing and fail here with `commit: no changes`.
5. **Tag** `mds-<C>` → the new commit (`createTag`, `force=true`, so a retry after a crash overwrites it).

Step 3 is the atomicity boundary for readers on the branch head (the staged pointer is what the catalog reads on a
branch). Step 4 is the atomicity boundary for the indexing position and for tag/commit reads.

### 4.4 Idempotency and crash recovery

Crash windows and what the next tick does (step 1 of 4.2):

| Crashed after | State found | Action |
|---|---|---|
| 3 (Iceberg commit staged, no lakeFS commit) | `R-metadata@b` has uncommitted changes, and the staged table's `lakefs.mds.source-commit` = next commit `C` after `last` | do steps 4–5 for `C` |
| 3, but the staged property is anything else (partial or foreign writes) | uncommitted changes | `resetBranch(R-metadata, b)` (discard staging), then index normally |
| 4 (lakeFS commit done, no tag) | head metadata = `C`, tag `mds-<C>` missing or pointing elsewhere | do step 5 |
| during 3 (data files written, commit not sent) | orphan files under `_iceberg/<uuid>/data/`, nothing staged | nothing. The files are unreferenced and removed by `lakectl iceberg cleanup` (section 9). |

Before step 3 the indexer also reloads the table and skips to step 4 if the current
`lakefs.mds.source-commit` already equals `C`. A 409 from the Iceberg commit means another writer moved the
table: reload, re-run recovery, continue.

### 4.5 First indexing of a branch (backfill)

When `R-metadata@b` does not exist:

1. **Seed from an indexed ancestor.** Walk the first-parent log of `R@b` from its head (at most `seed_lookback`,
   default 1000 commits). If a commit `A` with tag `mds-<A>` exists in `R-metadata`, create
   `R-metadata@b` from that tag and continue with 4.2 (`last = A`). This makes a new feature branch cheap.
2. **Else full listing.** `S` = the oldest first-parent commit of `R@b` with `creation_date ≥ since` (or the head
   if `since` is unset and `backfill: head`, the default; `backfill: history` starts at the branch's oldest
   first-parent commit, like upstream's "branch creation"). Create `R-metadata@b` from the `R-metadata`
   initialisation commit (empty table), write all objects of `R@S` with `commit_id = S` (rebuild path of 4.3
   step 2), commit with `lakefs.mds.source-commit = S`, tag `mds-<S>`. Then continue forward.

### 4.6 Writing files: credentials

The catalog stages client-written files; the indexer must write Parquet and Avro files to
`<R-metadata ns>/_iceberg/<table-uuid>/`. `iceberg-go` does **not** implement S3 remote signing
(`io/gocloud/s3`: "remote S3 request signing is not supported"), so:

- If the catalog vends write credentials (S3 blockstore with `credentials_vending.role_arn`), the indexer requests
  `X-Iceberg-Access-Delegation: vended-credentials` (the iceberg-go REST default) and needs nothing else.
- Otherwise `metadata_search.storage` supplies iceberg-go IO properties (`s3.endpoint`, `s3.region`,
  `s3.access-key-id`, `s3.secret-access-key`, `s3.path-style-access`), passed with `rest.WithAdditionalProps`.
  This is the mode used by esti (SeaweedFS, static keys).

Azure/GCS are out of scope for A (no vending, no signer); with static `storage` properties for those schemes it may
work, but it is not tested.

### 4.7 History rewrites (hard reset, revert-by-reset, force-moved branches)

When `last` is not an ancestor of `head`: `mb = findMergeBase(R, last, head)`. If tag `mds-<mb>` exists,
`hardResetBranch(R-metadata, b, "mds-<mb>")` and continue from `mb`. Otherwise do the full listing of 4.5
step 2 at `head`, on a branch hard-reset to the initialisation commit. (A normal `revert` is a new commit and needs
none of this.)

### 4.8 Branch and repository lifecycle

| Event in `R` | Indexer action |
|---|---|
| matching branch created | 4.5 |
| matching branch deleted | delete `R-metadata@b` if `delete_branches` (default true). Its tags stay, so commit/tag reads of its history still work. |
| branch stops matching the config | nothing (left as is) |
| repository added to config | create `R-metadata` (3.3). lakeFS creates it non-bare, so its default branch starts with the automatic `Repository created` root; create namespace `system` and the table through the catalog on the default branch, then lakeFS commit `mds: init` as the first commit after that root (the **initialisation commit**, metadata `lakefs.mds.init=true`). Then 4.5 for each matching branch. |
| `R-metadata` exists and its history is only lakeFS's automatic `Repository created` root | complete the initialisation on top: create namespace `system` and the table, then the `mds: init` commit (metadata `lakefs.mds.init=true`). |
| `R-metadata` exists with the `lakefs.mds.init=true` marker at its root (a bare repository) or at its first commit after the automatic root | accept it as the indexer's; later commits are indexing commits. |
| `R-metadata` exists with any other history and no init marker at one of the two accepted positions | refuse that repository, log an error. In particular, a foreign commit after the automatic root without the marker is refused. |
| repository deleted | log, skip. `R-metadata` is left for the admin. |

### 4.9 Maintenance

- **Compaction.** Position deletes accumulate. When the table has more than `compaction.max_delete_files`
  (default 200) delete files, or every `compaction.every_commits` (default 500) commits, the indexer scans the table
  at the current snapshot and, in one transaction, removes every data and delete file (`Transaction.ReplaceFiles`)
  and appends the scanned rows, with the same `lakefs.mds.source-commit`. A plain `Overwrite` is not enough: it
  rewrites data files but leaves their delete files in place, so deletes would keep accumulating. Then it does a lakeFS commit `mds: compact <R>@<C>` (no new tag; the tag
  stays on the pre-compaction commit, which has the same rows).
- **Snapshot expiry and file sweep** are not done by the indexer: run `lakectl iceberg cleanup -r <R>-metadata`
  on a schedule (`cmd/lakectl/cmd/iceberg_cleanup.go`). It honours lakeFS references (branch heads, tags, commits
  retained by GC), so `mds-*` tags keep their snapshots readable.
- **Tag retention.** `tag_retention` (default `0` = keep forever) deletes `mds-*` tags whose source commit is
  older than the duration, then lakeFS GC can reclaim their data.

## 5. Catalog side (landed in R10-1, plus R10-2)

### 5.1 API as implemented (`pkg/icebergcatalog/resolver.go`)

```go
const (
    SystemNamespace          = "system"
    ObjectMetadataTable      = "object_metadata"
    MetadataRepositorySuffix = "-metadata"
)

type ResolvedTable struct {
    Target Address // where the pointer is read: R-metadata, mapped ref, system, object_metadata
    Authz  Address // what authorization is checked against: the data repo address
}
type TableResolver interface {
    Resolve(ctx context.Context, addr Address) (ResolvedTable, bool, error)
}
type RefMapper interface {
    MapRef(ctx context.Context, dataRepo, ref string) (metadataRef string, err error)
}

// Seam: assigned before the server mounts the catalog; nil → tags/commits answer NoSuchTableException.
var MetadataSearchRefMapper func(branches BranchLister) RefMapper

func IsMetadataRepository(repo string) bool
func NewMetadataSearchResolver(branches BranchLister, mapper RefMapper) *MetadataSearchResolver
func WithTableResolver(resolver TableResolver) StoreOption
```

`http.go` registers the resolver when `iceberg.metadata_search.enabled` is true, with
`metadataSearchRefMapper(deps.Catalog)`. Resolution: a ref that is a branch of `R`
(`GetBranchReference`) maps to the same name; anything else (`ErrBranchNotFound`, `ErrInvalidBranchID`) goes to
the mapper. A mapped branch that does not exist in `R-metadata` answers 404 `NoSuchTableException`.

### 5.2 Authorization (landed)

`Store.ResolveAuthorized` checks the requested address on `R` (`catalog:ReadTable`, plus `catalog:ReadTableData`
for signing/vending) and `fs:ReadRepository` on `R`. The redirected read of `R-metadata` runs with the server's
identity. Caveat (upstream too): the grant covers metadata of all objects on all refs of `R`. Path-level or
branch-level `fs:` policies do not narrow it. Vended credentials and signing for the virtual table are scoped to
`R-metadata`'s table location (`creds.go`, `sign.go`).

### 5.3 Read-only `system` namespace (landed)

On a data repository, writes under `system` answer 403 `ForbiddenException` (`ReservedNamespaceError`).
`GET`/`HEAD` of the namespace is 404 (no marker). Listings show `system` and `object_metadata`. The catalog has no
rename endpoint, so there is nothing to block there. In `R-metadata`, `system` is a normal namespace.

### 5.4 Config (landed)

`iceberg.metadata_search.enabled` (default false). When it is false, nothing changes.

### 5.5 Upstream endpoint alias (R10-2, new)

Upstream clients point at `<lakefs>/mds/iceberg/api` (warehouse/prefix `lakefs`, OAuth at
`<lakefs>/mds/iceberg/api/v1/oauth/tokens`). When `iceberg.metadata_search.enabled` is true, `RoutesBuilder`
also mounts the same `/iceberg/api` handler at `/mds/iceberg/api`. Same auth, same prefix (`CatalogPrefix = "lakefs"`).
No `relative_to` alias. Upstream configs then work unchanged apart from the host.

## 6. Ref mapping (`pkg/mds/refmap`, server side)

Installed in `cmd/lakefs-sso/main.go` before `Execute`:

```go
icebergcatalog.MetadataSearchRefMapper = refmap.New
```

`refmap.New(branches icebergcatalog.BranchLister) icebergcatalog.RefMapper` type-asserts `branches` to

```go
type refResolver interface {
    GetCommit(ctx context.Context, repositoryID, reference string) (*catalog.CommitLog, error)
    GetTag(ctx context.Context, repositoryID, tagID string) (string, error)
}
```

(`*catalog.Catalog` implements it). If the assertion fails it returns nil, keeping the default behaviour, and logs once.

`MapRef(ctx, R, ref)`:

1. `GetCommit(R, ref)`. This resolves tags, full commit IDs, short commit IDs (`GetCommitByPrefix`) and ref
   expressions (`main~2`). Not found → `*NoSuchTableException`.
2. `GetTag(R-metadata, "mds-" + commit.Reference)`. Found → return `"mds-<id>"` (a tag of `R-metadata`; reads
   on a tag are read-only in the catalog, which is right).
3. Not found → `*NoSuchTableException` (the commit is not indexed: not yet, not on an indexed branch's first-parent
   chain, or its tag was retired). The mapper never answers with an ancestor's table, because that would return
   wrong rows.

Errors other than not-found are returned as is (500).

## 7. Indexer configuration

Read by `lakefs-sso mds run` from the top-level key `metadata_search` (upstream-compatible name). The server reads
only `iceberg.metadata_search.enabled`. Both can live in the same file.

```yaml
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000       # required; lakeFS base URL (API at /api/v1, catalog at /iceberg)
  access_key_id: AKIA...                       # required; the mds-indexer user's key
  secret_key: ...                              # required
  listen_address: 0.0.0.0:8080                 # /_health, /metrics, /_pprof/
  period: 60s                                  # > 0
  concurrency: 25                              # branches in parallel, > 0
  max_commits: 1000                            # per branch per tick, > 0
  since: 2026-01-01T00:00:00Z                  # optional, RFC 3339; with backfill: history
  backfill: head                               # head | history
  seed_lookback: 1000
  rebuild_threshold: 100000
  delete_branches: true
  tag_retention: 0s
  compaction:
    max_delete_files: 200
    every_commits: 500
  storage:                                     # optional iceberg-go IO props when no vended credentials
    s3.endpoint: http://seaweedfs:8333
    s3.region: us-east-1
    s3.access-key-id: ...
    s3.secret-access-key: ...
    s3.path-style-access: "true"
  repositories:                                # at least one
    my-repo:
      branches: [main, "dev-*"]                # at least one; exact or trailing-* glob
      storage_namespace: s3://bucket/metadata/my-repo   # optional (3.3)
```

Startup fails with one error per problem when: the endpoint or keys are empty; `period`, `concurrency`,
`max_commits`, `seed_lookback` or `rebuild_threshold` are not positive; `backfill` is unknown; `repositories`
or a `branches` list is empty; a repository name ends in `-metadata` or is longer than 54 characters; a glob has a
`*` that is not trailing; or `since` does not parse. Per-repository runtime errors (repository missing, namespace
not derivable or overlapping) mark that repository failed in `/_health` and metrics, and the others continue.

Environment overrides follow lakeFS conventions (`LAKEFS_METADATA_SEARCH_SECRET_KEY`, …).

## 8. Principals, policies, bootstrap

Two principals, as upstream separates indexing from querying:

| Principal | Created by | Permissions |
|---|---|---|
| `mds-indexer` (group `MetadataSearchIndexers`, policy `mds-indexer`) | `pkg/mds` bootstrap, an extra routes builder in lakefs-sso that runs when `iceberg.metadata_search.enabled`. It creates user, group and policy idempotently. **Credentials are created by an admin** (`lakectl auth users credentials create --id mds-indexer`) and placed in the indexer config. | On `*` (data repos): `fs:ReadRepository`, `fs:ListObjects`, `fs:ReadObject`, `fs:ListBranches`, `fs:ReadBranch`, `fs:ListCommits`, `fs:ReadCommit`. On `arn:lakefs:fs:::repository/*-metadata` and below: `fs:*`, `catalog:*` on `arn:lakefs:catalog:::*/*-metadata/*`, `fs:CreateRepository` on the repository ARN pattern, and `fs:AttachStorageNamespace` on `arn:lakefs:fs:::namespace/*-metadata`. The last two are the pair `CreateRepository` checks. The namespace pattern is the narrowest one that covers the derived namespaces of 3.3 (`<R namespace>-metadata`); an explicit `storage_namespace` that does not itself end in `-metadata` needs an additional grant. |
| query users | existing users | Through the catalog: only read on `R` (5.2). Engines reading `R-metadata` directly (not through the virtual table) need read on `R-metadata` like any repository. |

Write protection of `R-metadata`: policy `mds-metadata-repo-protect` (Deny `fs:Write*`, `fs:Delete*`,
`fs:Create*`, `fs:Revert*`, `catalog:Create*`, `catalog:Update*`, `catalog:Delete*`, `catalog:WriteTableData` on
the `*-metadata` ARNs), attached to the base groups (`datasets.PinsProtectGroups`, the same list
`iceberg-reserved-prefix-protect` uses), never to `MetadataSearchIndexers`. As with the reserved-prefix deny, an auth
service without policies (`BasicAuthService`) logs a warning naming what is not enforced, and startup continues.

## 9. Operations

- **Health:** `/_health` returns 200 when every repository's last tick succeeded or is in progress, 503 otherwise,
  with a JSON body per repository and branch (`last_indexed`, `head`, `lag_commits`, `last_error`).
- **Metrics** (Prometheus, prefix `lakefs_mds_`): `indexed_commits_total{repository}`,
  `lag_commits{repository,branch}`, `errors_total{repository,stage}`, `tick_duration_seconds`,
  `rows_written_total`, `rebuilds_total`, `compactions_total`.
- **Freshness for users:** compare table property `lakefs.mds.source-commit` (or the latest snapshot summary) with
  the branch head of `R`. Upstream's recipe `SELECT … WHERE commit_id = '<head>'` works only when the head commit
  changed at least one object; document both.
- **GC:** `R-metadata` is an ordinary repository. Configure lakeFS GC rules and schedule
  `lakectl iceberg cleanup -r <R>-metadata`. The uncommitted-GC grace for `_iceberg/` (`iceberg.uncommitted_gc_grace`)
  covers files between steps 3 and 4 of 4.3.
- **Size:** one table per branch, unpartitioned, sorted by `path` at write time per file. Partitioning by path prefix
  is a later version.
- **Security tests:** a user without `fs:ReadRepository` on `R` gets 403 on the virtual table; a user with access to
  `R-metadata` only gets 403 through `R.<ref>.system.object_metadata`; a non-indexer user cannot write `R-metadata`
  when the deny is in force; `system` on `R` rejects writes.

## 10. Decisions (former open questions)

1. Indexer runs as the `lakefs-sso mds run` subcommand only. A goroutine mode is out of scope for A.
2. `iceberg-go` v0.7.0 writes merge-on-read position deletes (`Transaction.Delete` with `write.delete.mode=merge-on-read`
   on v2), equality deletes (`WriteEqualityDeletes` + `RowDelta`) and copy-on-write (`Overwrite` with filter).
   Position deletes are chosen because PyIceberg does not read equality deletes. M3's spike confirmed the catalog
   stages position delete files. `Overwrite` is used for rebuilds; compaction uses `ReplaceFiles` + `Append` (4.9).
3. Commit/tag mapping: `mds-<commit>` tags in `R-metadata` plus lakeFS commit metadata. No index file, no
   snapshot-summary scan at read time.
4. Config: top-level `metadata_search` for the indexer, `iceberg.metadata_search.enabled` for the server.
5. Endpoint: `/mds/iceberg/api` alias plus the existing `/iceberg/api` and `/iceberg/relative_to`.

## 11. Future: option B (not built now)

Everything of A, plus:

- **Query service:** a stateless pod running embedded DuckDB. The lakeFS server authenticates and authorizes the UI
  request (`fs:ReadRepository` on every repository the query references, audit-logged), forwards the SQL with
  short-lived credentials scoped to the table's data and metadata prefixes, the pod writes Parquet results, and the
  server streams the first 1000 rows plus the row count.
- **Config `iceberg_query`:** `enabled`, `endpoint`, `token`, `results_storage_id`, `results_location` (default
  prefix `_lakefs/mds_query/results/`, outside every repository namespace, with a lifecycle rule),
  `request_timeout` (default 30s).
- **Statements:** allow `SELECT`, `WITH`, `VALUES`, `SHOW TABLES`, `DESCRIBE`, `SUMMARIZE` (upstream list), one
  statement, wrapped as `SELECT * FROM (…) AS q LIMIT n`. Reject `EXPLAIN`, `PRAGMA`, `SET`, `CREATE`, `COPY`,
  `ATTACH`, file-reading and session functions. Row, byte and time caps; one query at a time per installation.
- **UI tab:** ref selector, SQL editor with table name `"<repo>.<ref>.system".object_metadata`, results table; Tables
  tab conventions.
- **Requirements:** S3 with credential vending (`role_arn`), results lifecycle rule, single pod.
