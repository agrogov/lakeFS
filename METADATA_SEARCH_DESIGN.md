# Metadata Search for lakeFS OSS — design

Status: draft for review. Companion: [`METADATA_SEARCH_PLAN.md`](METADATA_SEARCH_PLAN.md).
Builds on [`ICEBERG_CATALOG_PLAN.md`](ICEBERG_CATALOG_PLAN.md) and `docs/iceberg.md`.

## 1. Goal and scope

Let users find objects by system metadata (path, size, last modified, committer, content type, etag) and
user-defined metadata, with any Iceberg engine, across the commits of a repository.

The reference behaviour is the lakeFS "Metadata Search" guide (https://docs.lakefs.io/guides/metadata-search/):

- for each data repository `R`, a companion repository `R-metadata` holds Iceberg tables with the same branch names;
- a background indexer keeps the tables in sync with the commits of `R`;
- the catalog resolves `R.<ref>.system.object_metadata` to the matching table in `R-metadata`;
- the query result is one row per object per branch, eventually consistent, atomic per commit.

### Delivery options

| Option | Content | Status |
|---|---|---|
| **A** | Indexer, `R-metadata` management, catalog resolution. Users query with DuckDB, Spark, Trino or PyIceberg. | **This design, to be built.** |
| **B** | A + Web UI "Metadata Search" tab + a query service (embedded DuckDB) that runs SQL server-side. | **Documented for the future** (section 9). Not built now. |

Non-goals of A: a UI tab, server-side SQL, multi-tenancy, non-S3 credential vending, full-text or vector search.

## 2. Does the Iceberg catalog need to change?

Yes, in one small place; everything else is outside the catalog.

| Piece | Where | Catalog change? |
|---|---|---|
| Indexer (reads commits of `R`, writes rows) | new package and `lakefs` subcommand | No. It writes `R-metadata` through the catalog's public API (create table, commit) or directly with the block adapter plus the register endpoint. |
| `R-metadata` repositories and branches | indexer | No. They are ordinary lakeFS repositories. |
| Resolving `R.<ref>.system.object_metadata` | `pkg/icebergcatalog` | **Yes** — a table-resolver hook (section 5). |
| Authorization of that read | `pkg/icebergcatalog/authz.go` | **Yes** — checked on `R`, never on `R-metadata`. |
| Read-only `system` namespace on data repos | `pkg/icebergcatalog` | **Yes** (section 5.4). |
| UI tab and SQL service | option B | No (the service calls the catalog like any client). |

The catalog change ships first (task R10-1) so the rest can be built on a branch that already has it.

## 3. Data model

### 3.1 Table `system.object_metadata` (in `R-metadata`)

| Column | Type | Notes |
|---|---|---|
| `repository` | string | data repository name |
| `path` | string | object key |
| `commit_id` | string | the commit that last changed this row on the branch |
| `size_bytes` | long | |
| `last_modified` | timestamp (UTC) | |
| `etag` | string | |
| `user_metadata` | map<string,string> | keys as stored (S3 gateway keys are lowercased and carry `X-Amz-Meta-`) |
| `committer` | string | |
| `content_type` | string | |
| `creation_date` | timestamp (UTC) | |

Format v2, identity partitioning is **not** used. Sort order: `path`. Rows are written as Parquet. One row per
`(branch, path)`: a row is replaced when the object changes and removed when it is deleted.

### 3.2 `R-metadata` layout

- Repository name `R-metadata`, same default branch as `R`, storage namespace `<R storage namespace>-metadata` unless
  overridden (`storage_namespace` per repository, as upstream).
- Branches mirror the configured branches of `R` (exact names or `prefix-*`).
- Table `system.object_metadata` exists on every mirrored branch. Its pointer entry lives under
  `_lakefs_iceberg/tables/...` like any catalog table, so a lakeFS branch of `R-metadata` carries its own table state.
- A bookkeeping object `_lakefs_mds/state.json` per branch records `last_indexed_commit` of `R` for that branch.

## 4. Indexer

### 4.1 Process

A service run as `lakefs mds run` (or a goroutine of the server when configured), disabled unless
`metadata_search.repositories` is non-empty. Per tick (`period`, default 60s), per configured branch, up to
`concurrency` branches in parallel:

1. Read `last_indexed_commit` of branch `b` from `R-metadata`.
2. List commits of `R@b` after it, oldest first, at most `max_commits` (default 1000), starting at `since`
   (RFC 3339) or branch creation. Commits are processed **sequentially**, a child only after its parent.
3. For each commit, take the diff against its first parent (`Diff` API, paged). Added/changed paths become upserts
   (stat gives size, etag, content type, user metadata, mtime, committer); removed paths become deletes.
4. Apply the change set as **one Iceberg commit** on `R-metadata@b` (a merge-on-read position/equality delete plus a
   data file of the new rows, or a copy-on-write rewrite of the affected files for small tables). The Iceberg commit
   carries snapshot summary `lakefs.mds.source-commit = <commit id of R>`.
5. In the same lakeFS commit on `R-metadata@b`, update `state.json`. A reader sees all of a commit's metadata or none.

Merge commits are diffed against the first parent. Branch creation seeds the table from the source branch's table:
a lakeFS branch of `R-metadata` is created from the parent branch's tip, then indexed forward.

### 4.2 Idempotency and crash safety

`state.json` and the table pointer move in one lakeFS commit, so a crash between steps repeats a commit that was not
recorded; upserts are keyed by `(path)` so replays are harmless. Concurrent indexers are prevented by a CAS on
`state.json` (`graveler.WithCondition`); the loser backs off.

### 4.3 Writing Iceberg

Preferred: the indexer builds data files with the Go Parquet writer, then calls the catalog's own
`POST /v1/{prefix}/namespaces/system/tables/object_metadata` commit endpoint as an authenticated client of `R-metadata`
(the `mds-service-user`, section 7). This dogfoods the catalog, keeps GC staging correct, and keeps the indexer out of
catalog internals. The Go `iceberg-go` library (v0.7.0 in `go.mod`) is used for metadata construction; if its
transaction API cannot express the delete files needed, the indexer falls back to copy-on-write.

### 4.4 Consistency and visibility

Eventually consistent: a commit becomes searchable a few minutes after it lands. A client checks readiness by comparing
the branch head commit of `R` with the `lakefs.mds.source-commit` summary of the current snapshot (exposed also as
`state.json`).

## 5. Catalog change (task R10-1)

### 5.1 TableResolver hook

An interface registered by the server; default is none, and with none the catalog behaves exactly as today.

```go
type ResolvedTable struct {
    Target Address // where the pointer is read (R-metadata, mapped ref, system, object_metadata)
    Authz  Address // what authorization is checked against (the DATA repo address)
}

type TableResolver interface {
    Resolve(ctx context.Context, addr Address) (ResolvedTable, bool, error) // bool: handled
}

type RefMapper interface {
    // MapRef maps a ref of the data repo to a ref of its metadata repo.
    MapRef(ctx context.Context, dataRepo, ref string) (metadataRef string, err error)
}
```

Wired into every table read path: load, HEAD, list tables of the namespace, credentials, sign, metrics, and the
`/relative_to` mount (which already builds an `Address` from the path).

### 5.2 Virtual table

`R.<ref>.system.object_metadata` resolves to `R-metadata`, ref = `MapRef(R, ref)`, namespace `system`, table
`object_metadata`. Default `RefMapper`: same name for branches; commits and tags go to the mapper, which resolves them
through the `lakefs.mds.source-commit` snapshot summaries (the Iceberg snapshot whose source commit is the requested
commit, or the latest one at or before it) and may answer `NoSuchTableException`.

### 5.3 Authorization

One `catalog:ReadTable` and `catalog:ReadTableData` check on the **data** repo (`system`, `object_metadata`). A user
who can read `R` can search its metadata without any access to `R-metadata`; the redirected read runs under the server
identity. This matches the upstream guide ("users who can browse a repository can search its metadata") and its caveat:
the permission covers metadata of all objects on all refs; branch or object policies do not narrow it. Documented as such.

### 5.4 Read-only `system` namespace

On a data repo, namespace `system` is reserved for the resolver: create, commit, register, drop, rename, and
create/drop namespace under it answer `403 ForbiddenException`. `ListNamespaces`/`ListTables` on the data repo show
`system` and `object_metadata` only when the resolver is registered and
`iceberg.metadata_search.enabled` is true. In `R-metadata` itself `system` is a normal namespace (the indexer writes it).

### 5.5 Config

`iceberg.metadata_search.enabled` (default false). Without it nothing changes.

## 6. Configuration of the indexer

Top-level key `metadata_search`, as upstream, so documentation and Helm values carry over:

```yaml
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000   # required
  access_key_id: ...
  secret_key: ...
  listen_address: 0.0.0.0:8080              # /_health, /metrics, /_pprof/
  period: 60s
  concurrency: 25
  max_commits: 1000
  since: 2026-01-01T00:00:00Z
  repositories:
    my-repo:
      branches: [main, "dev-*"]
      storage_namespace: s3://bucket/metadata/my-repo   # optional override
```

Startup refuses when `lakefs_mds_endpoint` is empty or `period`, `concurrency`, `max_commits` are not positive.

## 7. Auth

The indexer authenticates with a dedicated user `mds-service-user`: read on the data repos (`fs:ReadRepository`,
`fs:ListObjects`, `fs:ReadObject`) and read-write on `R-metadata` (`fs:*`, `catalog:*` on it). Bootstrap creates the
user and the policy like `iceberg-reserved-prefix-protect` (`bootstrap.go`). Engines that connect directly need read
access to both repositories (upstream parity); engines going through the catalog need only the data repo (5.3).

## 8. Operations, limits, risks

- **Backfill:** the first run on a large repo walks `max_commits` per tick; progress is visible through `state.json`
  and metrics (`mds_indexed_commits_total`, `mds_lag_commits`, `mds_errors_total`).
- **Rewrites of history** (hard reset, revert): a branch whose recorded commit is no longer an ancestor is rebuilt from
  its merge base. Documented limitation for the first version: rebuild by dropping and reindexing the branch.
- **Size:** unpartitioned, unsorted tables of hundreds of millions of rows scan slowly; a later version adds
  partitioning on `path` prefix.
- **Security:** the cross-repo read in 5.3 is the main risk. Tests assert a user without `fs:ReadRepository` on `R`
  gets 403, and a user with only `R-metadata` access gets nothing through the virtual table.
- **S3 gateway metadata keys:** stored with the `X-Amz-Meta-` prefix and lowercase, as upstream.
- **No UI, no SQL service in A:** ad-hoc search needs a client engine.

## 9. Future: option B (not built now)

Everything of A, plus:

- **Query service:** a separate stateless pod running embedded DuckDB. The lakeFS server authenticates and authorizes the
  UI request (`fs:ReadRepository` on every repo the query references, audit-logged), forwards the SQL with short-lived
  credentials scoped to the table's data and metadata prefixes (the existing S3 credential vending), the pod writes
  Parquet results to a results prefix, and the server streams the first 1000 rows plus the row count back.
- **Server config `iceberg_query`:** `enabled`, `endpoint`, `token`, `results_location` (outside any repository
  namespace, with a lifecycle rule), `request_timeout` (default 30s).
- **Query limits:** wrap as `SELECT * FROM (...) AS q LIMIT n`; one SELECT-shaped statement; reject multiple
  statements, `EXPLAIN`, `PRAGMA`, `SET`, `CREATE`, `COPY`, other tables, file-reading and session functions; row, byte
  and time caps; one query at a time per installation.
- **UI tab:** ref selector, SQL editor, Execute, results table; reuses the Tables tab conventions (no login redirect,
  in-tab errors).
- **Requirements:** S3 with credential vending (`role_arn`), a results prefix lifecycle rule, single pod (no HA at first).

## 10. Open questions

1. Indexer as a goroutine of `lakefs` or as a separate `lakefs mds run` subcommand only? (Proposed: subcommand first.)
2. Can `iceberg-go` v0.7.0 write position deletes, or is copy-on-write needed from the start?
3. Where should the commit/tag to `R-metadata` snapshot mapping live: snapshot summaries only, or an index file?
4. Name and placement of the config key: `metadata_search` (upstream-compatible) versus `iceberg.metadata_search`.
