# Metadata Search for lakeFS OSS — design

Status: revised for implementation, realigned with the current guide (`METADATA_SEARCH_REALIGN.md` RA8).
Companion: [`METADATA_SEARCH_PLAN.md`](METADATA_SEARCH_PLAN.md). Builds on
[`ICEBERG_CATALOG_PLAN.md`](ICEBERG_CATALOG_PLAN.md) and `docs/iceberg.md` (section "Metadata Search").

Realignment: this document used to describe the guide as it read in 2026-11-03/04. The current guide
(2026-11-09) moved the tables to the root `system.metadata` tree of the lakeFS catalog, deprecated the
separate `/mds/iceberg/api` catalog we had implemented, and rewrote the indexer's configuration and sync
identity. `METADATA_SEARCH_REALIGN.md` is the list of gaps and the card that closed each one; the
sections below describe the code after those cards, not the earlier build. Where a section still
mentions a commit of the earlier design, it is there because the behaviour or the residual did not
change.

Clean-room note: upstream Metadata Search is a lakeFS Team/Enterprise feature. This is an independent OSS
implementation written only from the public guide (https://docs.lakefs.io/guides/metadata-search/), the
Apache Iceberg specs and this repository's code. No upstream Enterprise code, configs or binaries are used.

## 1. Goal and scope

Let users find objects by system metadata (path, size, last modified, committer, content type, etag) and
user metadata, with any Iceberg engine, at any indexed branch, tag or commit of a repository.

Reference behaviour (upstream guide):

- the tables are "served by the lakeFS Iceberg REST catalog at `<lakefs_endpoint>/iceberg/api`, under the
  reserved `system` namespace: `system.metadata.<repo>.<ref>.objects`";
- for each data repository `R` a companion repository `R-metadata` holds the table
  `system.object_metadata`, with branches named like the indexed branches of `R`;
- a background indexer keeps the table in sync with the commits of `R`;
- the catalog resolves `system.metadata.<R>.<ref>.objects` to that table in `R-metadata`;
- the separate Metadata Search catalog at `<lakefs_endpoint>/mds/iceberg/api` is deprecated and keeps
  serving the older identifier `<repo>.<ref>.system.object_metadata` until it is removed;
- one row per object per branch, eventually consistent, atomic per commit of `R`.

### Delivery options

| Option | Content | Status |
|---|---|---|
| **A** | Indexer, `R-metadata` management, catalog resolution. Users query with DuckDB, Spark, Trino or PyIceberg. | **Built** (this design; realigned by RA1–RA7, RA9, RA10, RA4b and RA8). |
| **B** | A + Web UI "Metadata Search" tab + a server-side query service (embedded DuckDB). | Documented for later (section 11). |

Non-goals of A: UI tab, server-side SQL, multi-tenancy, Azure/GCS credential vending, full-text or vector search,
exact upstream `commit_id` semantics for objects that existed before the first indexed commit (section 3.2).

## 2. What exists and what is new

| Piece | Where | State |
|---|---|---|
| Root `system.metadata` tree, authz on `R`, read-only tree, `relative_to` root-tree warehouses | `pkg/icebergcatalog/resolver.go`, `namespaces.go`, `tables.go`, `relative.go`, `http.go` | **Done** (RA1, RA2; sections 5.1–5.3). |
| Deprecated, separate Metadata Search catalog at `/mds/iceberg/api` | `pkg/icebergcatalog/http.go`, `resolver.go` | **Done** (RA3; section 5.5). |
| `RefMapper` for tags and commits | `pkg/mds/refmap`, installed from `cmd/lakefs-sso/main.go` | Done (section 6). |
| Indexer (`lakefs mds run` on the server binary and configuration) | `pkg/mds/...`, `cmd/lakefs/mdscmd/mds.go` | Done (RA4, RA4b; sections 4, 7). |
| Sync identity `mds-service-user` and `MDSCatalogAccess` | `pkg/mds/bootstrap/bootstrap.go`, extra routes builder in `cmd/lakefs-sso/main.go` | Done (RA5; section 8). |

The indexer runs on the **server binary and configuration** (RA4b): `lakefs mds run --config <file>`
opens the KV store of `database.*`, builds `pkg/catalog` over the `blockstore.*` adapter, and reads the
data repositories in process. It does not use the REST API for reads and does not read the
`metadata_search` section for anything but its own keys (section 7). Its table writes go through the
lakeFS Iceberg REST catalog at `<lakefs_endpoint>/iceberg/api`, as the guide says, authenticated as
`mds-service-user` (section 8).

## 3. Data model

### 3.1 Table `system.object_metadata` (in `R-metadata`)

Iceberg format **v2**, unpartitioned, sort order `path ASC`, `identifier-field-ids = [2]`.
Field IDs below are the ones the created table has; schema changes only add optional fields with new IDs.
The catalog's create path (iceberg-go `NewMetadataWithUUID`) renumbers IDs in pre-order, so the map's key and
value come right after `user_metadata`. The fixed-ID constants in `pkg/mds/table/schema.go` are these created IDs:
the create request is built in the same pre-order, so the catalog's renumbering is a no-op and the constants, the
request and the created table agree (commit `39486c0d2`). Readers and the writer still take IDs from the loaded
table, never from the constants; the constants only describe the table this package creates.

| ID | Column | Iceberg type | Req. | Value |
|---|---|---|---|---|
| 1 | `repository` | string | required | data repository name `R` |
| 2 | `path` | string | required | object key |
| 3 | `commit_id` | string | required | full ID of the commit of `R` that last added or changed the object on this branch (3.2) |
| 4 | `size_bytes` | long | required | object size |
| 5 | `last_modified` | timestamptz (µs) | required | object mtime (`DBEntry.CreationDate` / diff `right.mtime`) |
| 6 | `etag` | string | required | object checksum, content only |
| 7 | `user_metadata` | map<string,string> (key ID 8, value ID 9, value optional) | optional | user metadata, empty map when none (never null); the indexer never writes a null value |
| 10 | `committer` | string | optional | committer of `commit_id` |
| 11 | `content_type` | string | optional | content type |
| 12 | `creation_date` | timestamptz (µs) | optional | creation time of the commit `commit_id` (not of the object) |

The map value is optional because iceberg-go v0.7.0 reads parquet map columns with nullable items; a required
value makes its own copy-on-write rewrite and compaction fail with "record schema does not match writer's".
The map **field** is optional too (guide: `user_metadata` is required: no; METADATA_SEARCH_REALIGN.md G13,
RA7): the indexer still writes an empty map for an object without user metadata, and the writer applies one
schema update to a table created with the field required, keeping ID 7, which Iceberg allows for
required → optional.

Schema changes are additive - this section allows an optional column a later version appends with a new ID -
and a compaction follows them, but a write that has to build rows does not: a table that already carries a
top-level column this version of the writer does not know is refused with `table.ErrUnknownSchema` by
`ApplyDelta` with upserts and by every `Rebuild`, instead of being written with that column nulled over
another writer's data. A delete-only delta still applies, because it writes no row - in either delete
mode, because the copy-on-write arm writes an empty reader over the table's own schema instead of
building rows (commit `5d68d07af`, MDS_REVIEW_4 R4T-3) - and `Compact` re-emits the scan's own Arrow
columns rather than rebuilding rows, so such a column keeps its values through a compaction (commit
`d993d602e`).

User metadata keys are stored as lakeFS stores them. Objects written through the S3 gateway have keys
`X-Amz-Meta-<lowercased name>` (`pkg/gateway/operations/operation_utils.go`), as upstream documents.

Table properties set at creation:

```
write.delete.mode = merge-on-read
write.update.mode = merge-on-read
write.parquet.compression-codec = zstd
lakefs.mds.schema-version = 1
lakefs.mds.repository = <R>
commit.retry.num-retries = 0
```

`commit.retry.num-retries = 0` is what makes every write of this package non-replayable (section 4.3
step 2): iceberg-go decides replayability from the retry budget of the metadata a transaction started
on, so the value is set at creation, re-asserted by every write's own `SetProperties`, and repaired
with one metadata-only commit before the write when an older or operator-raised table is met
(commit `d993d602e`).

The delete mode a delta runs is the table's `write.delete.mode`, unless the caller passes
`WithDeleteMode` explicitly (which only a test and the fallback's caller do); when the property is
**absent** the package keeps iceberg-go's own default, copy-on-write, and does not answer
merge-on-read. The mode a transaction actually runs is read from the metadata the transaction is
building, and the documented upstream default is `copy-on-write` (iceberg-go v0.7.0
`table/transaction.go` and `table/properties.go`, and the upstream configuration reference);
merge-on-read would also be a mode the write does not run unless this writer added a
per-table setting to a table it did not create, and that setting would outlive the write. Every table
this package creates carries merge-on-read, so an absent property is an operator- or
another-writer-shaped table, and the copy-on-write arm is correct there because of the fold of section
4.3 step 3 (commit `5d68d07af`, MDS_REVIEW_4 R4T-1 decision). The upserts of one delta name a path at
most once: a path that appears twice is refused with `table.ErrRowsOutOfOrder` before anything is
committed - the same input contract `Rebuild`'s source has - rather than writing two rows under one
identifier, which nothing in Iceberg refuses on append (commit `5d68d07af`, R4T-5).

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
| Name | `R + "-metadata"` (`icebergcatalog.MetadataRepositorySuffix`). lakeFS's repository ID limit is 63 characters, so `R` longer than 54 characters has no companion name that fits; the guide has no validation for it and the configuration does not fail (X9), the repository is refused when the indexer tries to create the companion. A data repository whose own name ends in `-metadata` is still `icebergcatalog.IsMetadataRepository`'s case, so it cannot be indexed; the configuration accepts it (X9) and the repository reports that refusal. |
| Default branch | same name as `R`'s default branch |
| Storage ID | same as `R` |
| Storage namespace | `repositories.<R>.storage_namespace` if set; else `-metadata` is appended to `R`'s namespace in **every** case (X10, the guide: "If omitted, lakeFS appends `-metadata` to the repository's storage namespace"): `s3://bucket/path` → `s3://bucket/path-metadata` and a bucket root `s3://bucket` → `s3://bucket-metadata` (a different bucket, which the operator must then create or name explicitly). **Azure** (`METADATA_SEARCH_AZURE.md` D4): the first path segment is the container, so `https://acct.blob.core.windows.net/data` derives `https://acct.blob.core.windows.net/data-metadata` - a container the operator must create - while `…/data/esti` derives `…/data/esti-metadata`, in the same container. The split is `block.NamespaceRoot(namespace)`, the one helper the indexer and the catalog's own namespace checks call, so they cannot disagree about where a namespace's root is. The namespace must **not** be inside `R`'s namespace (nor `R`'s inside it): `R`'s uncommitted GC sweeps every unreferenced object under `R`'s prefix and would delete `R-metadata`'s files. Checked on every tick, not only when the companion repository is created, with the same `"/"`-normalised prefix test the GC uses (`normalizeStorageNamespace`): a companion repository that already exists with an overlapping namespace (created earlier, by hand, by a replica with another namespace, or because `R`'s namespace moved) is refused - that repository is marked failed in `/_health` and metrics - instead of indexed (commit `7ec4ebec7`). On Azure an `http://` and an `https://` spelling name the same container and prefix, and the overlap test normalises the scheme before it compares, so a companion cannot hide behind the other spelling. |
| Read-only flag | **not** set: catalog writes never pass `graveler.WithForce`, so a read-only repository would also block the indexer. The guide has no companion-repository deny policy and this build installs none (X2, RA5): `R-metadata` is an ordinary repository whose users' own grants apply, and the indexer's writes are the only writes the catalog side makes. The indexer requires the flag to be false: a companion repository lakeFS reports read-only is refused at the repository level - once per tick, with `ErrReadOnlyMetadataRepository`, before any branch is touched (commit `f884a0627`). Branch protection is diagnosed separately, because the lakeFS API does not report protection rules on the repository record: a rule that blocks `COMMIT` or `STAGING_WRITE` on a companion branch is answered at the first write the tick attempts (`Commit`, `ResetBranch` and `HardResetBranch`, and the catalog's table writes, the initialisation's `CreateTable` included) with the same `ErrReadOnlyMetadataRepository` family, so a bare 403 never reaches `/_health` (commits `da24d421f`, `2ae794038`). The refusal is reported **per branch**, not per repository: every branch whose first write lakeFS refuses carries the family in its `last_error` and increments `errors_total{stage="branch"}`, the repository's own `last_error` stays empty and `/_health` is 503 either way - three protected branches are three branch errors per tick, not one repository-level error (MDS_REVIEW_3 R3I-4, commit `2ae794038`). The message claims branch protection and the read-only flag only when lakeFS's own wording is in the wrapped cause (`cannot commit to protected branch`, `cannot write to protected branch`, `read-only repository`); any other forbidden answer - an authorization failure, which the in-process reader reports as its own forbidden sentinel and the catalog client as a 401 or 403 - stays in the family but names the three candidates (the branch's protection rules, the repository's `read_only` flag and the indexer principal's own permissions) without the destructive "create the repository with read_only=false" advice (MDS_REVIEW_3 R3I-1, commit `2ae794038`). A `STAGING_WRITE` rule present before the first tick is refused one step **before** the initialisation commit, at the catalog's `CreateTable`, and is mapped into the same family and message there (MDS_REVIEW_3 R3I-2, commit `2ae794038`). |
| Contents | namespace `system`, table `system.object_metadata` (pointer under `_lakefs_iceberg/tables/…`, files under `<ns>/_iceberg/<table-uuid>/`). No other bookkeeping objects. On an Azure blockstore `<ns>/_iceberg/…` is spelled `abfs[s]://<container>@<account>.<dfs host>/…`, the Iceberg form the catalog derives from the namespace (D1, §4.6); the pointer carries that form as its `iceberg.metadata-location`, while the lakeFS entries that record a file's physical address keep the block adapter's `http(s)://` spelling (D2). |
| Commits | one lakeFS commit per indexed commit of `R`, message `mds: index <R>@<commit>`, metadata `lakefs.mds.source-commit=<commit>`, `lakefs.mds.source-repository=<R>`. |
| Initialisation | the initialisation commit carries message `mds: init` and metadata `lakefs.mds.init=true`. It is the first commit after lakeFS's automatic `Repository created` root that a non-bare `CreateRepository` writes; for a companion repository created bare it is the root itself. A repository whose whole history is that automatic root is initialised by adding the namespace, table and `mds: init` commit on top (section 4.8). |
| Tags | `mds-<full commit ID of R>` on the lakeFS commit that recorded it (section 6). |

There is no `state.json`: the indexing position of branch `b` is the `lakefs.mds.source-commit` metadata of
the head commit of `R-metadata@b`. It moves atomically with the table, because both are in the same lakeFS commit.

## 4. Indexer

### 4.1 Process model

`lakefs mds run --config <file>` runs on the **server binary and the server configuration** (RA4b, X13),
as the official chart invokes it. It loads the whole lakeFS configuration the way `run` does (the file plus
`LAKEFS_*`), opens the KV store of `database.*`, builds `pkg/catalog` over the `blockstore.*` adapter, and
reads the data repositories in process. A missing or unreachable database fails startup, like `run`. It is a
separate process (a goroutine mode inside the server is out of scope for A); run one replica.

It also needs HTTP access to the catalog at `<lakefs_endpoint>/iceberg/api` (section 7), which is where its
table writes go, and the object-store credentials of `blockstore.*` for the files of those writes unless the
catalog vends them (section 4.6).

Per tick (`period`): for each configured repository, list `R`'s branches, match them against `branches`
(exact names and `prefix*` globs; only a trailing `*` is supported), and process up to `concurrency` branches
in parallel. Within a branch everything is sequential.

Run **one** indexer replica. Correctness under accidental concurrency comes from the Iceberg CAS and the
idempotency check (4.4), not from a lease; a second replica only wastes work and produces 409s. A second
process that meets the first on a freshly created `R-metadata` does not damage it either: the `mds: init`
commit is made with `allow_empty=false`, so only one of them can commit it, and the loser adopts the winner's
initialisation commit (4.8, commit `db8172798`).

Every HTTP request of the Iceberg catalog client is bounded by a fixed 60s request timeout (the removed
`metadata_search.request_timeout`, X6): a connection that neither answers nor resets fails its branch, or its
repository when the call is not attributable to one branch, instead of stalling the tick loop. Without the
bound a single wedged request stopped every repository from being indexed while `/_health` stayed 200 because
the tick was merely "in progress" (commit `7f952279b`).

### 4.2 Per-branch tick

1. **Recover** (4.4). The code reaches this only after it has read the position and the pending list; see
   "Order in the code" below.
2. `last` = `lakefs.mds.source-commit` of `R-metadata@b` head commit (none on a branch that was never indexed → 4.5).
3. `head` = head commit of `R@b`. If `last == head`, there is nothing to apply; the tag repair (4.4, "crashed
   after 4" - it repairs only a missing tag, or one whose commit does not record `last` - and the 4.9 compaction
   check still run.
4. If `last` is not an ancestor of `head` (`findMergeBase(R, last, head)` ≠ `last`) → history rewrite (4.7).
   A 404 from `findMergeBase` whose `getCommit(last)` also answers 404 means the recorded position no longer
   exists → the §4.7 rebuild (commit `6440e2f13`). The step is skipped when `last == head` - step 3 has
   already established that there is nothing to apply, and `last` is trivially its own ancestor - so a
   caught-up branch makes no `findMergeBase` request at all and a transient 404 on that endpoint cannot
   fail a branch that had no work (commit `da24d421f`).
5. `todo` = first-parent commits of `R@b` after `last` up to `head`, **oldest first**, at most `max_commits`
   (`logCommits(R, head, first_parent=true, stop_at=last)` paged, then reversed). The walk is bounded: it keeps
   a window of the oldest `max_commits`+1 commits read so far and stops after `max_commits` plus one page, so
   one tick holds O(`max_commits` + one page) commits and makes O(`max_commits`/page + 1) `logCommits`
   requests, not one per page of the whole backlog (commit `6440e2f13`). A position deeper than that window
   (more than `max_commits` plus one page behind) cannot be continued from without the unbounded read this
   bound removes, so it takes the §4.7 full-rebuild at the head instead: the head becomes the position and the
   lag is 0. Raise `max_commits` until the backlog fits one window to catch up commit by commit. The count is
   kept per page, so a walk that reaches `last` reports the exact lag (§9).
6. For each commit `C` in `todo`, apply it (4.3). A failure stops this branch for this tick; the next tick resumes at
   the recorded position.

**Order in the code.** `runBranchOnce` (`pkg/mds/indexer/branch.go`) runs these steps as 3, then 2 (loading
`R-metadata@b` first, and creating it per 4.5 if it does not exist), then 4, 5, then 1, then 6, and
finally the 4.9 compaction check. Recovery therefore runs *after* the pending list is computed, not
before it. That is equivalent to the logical order above:

- Recovery's decision compares the staged `lakefs.mds.source-commit` with exactly two values - the recorded
  position (step 2) and the next commit to apply (step 5, `todo[0]`) - so the list has to exist before the
  comparison can be made. Recovery reads the position the tick already has; when the staged property equals the
  recorded position it also costs one paged branch-diff call (`GET /repositories/{repo}/branches/{branch}/diff`,
  `client.DiffBranch`) to tell an empty staging from a foreign staged write that records the same source commit.
- The position is still read before anything is written, and recovery never moves it backwards. It either
  finishes the staged commit and advances the position to it, consuming `todo[0]`, or discards the staging
  and leaves both `last` and `todo` untouched.
- Because it consumes at most the first element and never appends, the sequence of writes is the same in
  either order: 4.3 steps 4–5 for the staged commit followed by the remaining commits, or a discarded
  staging followed by the commits after `last`. Step 5 drops the recorded position itself even though the
  log API's `stop_at` is inclusive (4.3 step 4), so no commit is applied twice and none is skipped.

### 4.3 Applying one commit `C` of `R`

1. **Change set.** `P` = first parent of `C`. If `C` has no parent, every object of `C` is an upsert
   (a paged listing of `R@C`). Else page the in-process diff `P..C` (`pkg/mds/reader`, `catalog.Diff`), no
   delimiter. `added`/`changed` → upsert, `removed` → delete. A diff entry of an added or changed object carries
   the object's checksum, mtime, `content_type` and user metadata, and its size; if a needed field is missing, the
   entry is statted with `catalog.GetEntry`. "Missing" means the whole right-side stat absent, an empty checksum, a
   zero mtime or a zero size. `content_type` and `metadata` are excluded on purpose: an object may legitimately
   have neither, so an empty value there is a fact about the object, not a gap in the answer. The size is on the
   entry, and a size the reader could not read is zero, which is also the size of an empty object, so a zero-size
   entry is statted: one extra call per empty object, in exchange for never writing a size lakeFS did not
   confirm (commit `1cb131f13`). `committer` and `creation_date` come from `C` (the commit record),
   the same for every row of the commit.
2. **Large change sets.** If upserts + deletes exceed a fixed threshold of 100 000 (the removed `rebuild_threshold`, X6), skip the delta and do a
   full rebuild of the branch at `C`: rows = paged `listObjects(R, C)`, all with `commit_id = C`, written in one
   transaction as `Transaction.ReplaceFiles` over every live data file and every live delete file of the current
   snapshot, followed by `Append`. `Transaction.Overwrite` (no filter) is not used: iceberg-go's overwrite
   rewrites the data files and leaves their position delete files in the snapshot, where they stay referenced by
   the live snapshot and no cleanup can reclaim them (§4.9). The removal is also re-validated against the live
   snapshot (`ReplaceFiles` refuses a file list that no longer matches), and the write is never replayed: the
   table carries `commit.retry.num-retries = 0` (§3.1), every write re-asserts it, and a table created before
   the property existed is repaired before the write, so a concurrent writer loses the CAS terminally instead
   of having the removal replayed against a newer snapshot - which would keep that writer's new data file and
   re-add the rows this one read, duplicating paths. The write returns the conflict (or `table.ErrStaleTable`
   when the repair commit itself observed a moved branch) and the branch tick reloads and retries (§4.4,
   commits `e3083e279` and `d993d602e`); the indexer maps both to `ErrConflict`, so both restart the tick
   (MDS_REVIEW_3 R3I-3, commit `2ae794038`). This bounds
   memory and the size of delete predicates. The walk of the diff
   stops at the first entry that takes the count over the threshold (`changeSet.large`): the entries read so far
   are dropped and no `statObject` call is made for that commit, so a commit that is going to be rebuilt costs
   neither the rest of its diff nor a single stat (commit `1cb131f13`). The loss of per-object `commit_id` history
   on a rebuild is documented. The rebuild streams its rows, and a panic in the caller's row source is
   reported as the reader's error - the new `table.ErrRowsPanic` - instead of killing the process: the source
   is pulled on iceberg-go's writer goroutine, where no recover of the indexer can reach it (commit
   `2b494acc4`, MDS_REVIEW_4 R4T-4). The source of a rebuild must be in the table's declared `path ASC` order:
   `Rebuild` refuses a source whose paths are not strictly ascending with the new `table.ErrRowsOutOfOrder`
   instead of writing files that contradict the table metadata (commit `2daeca114`). The production caller is
   unaffected - its source is the paged lakeFS listing, which is lexicographic - and refusing beats sorting the
   branch in memory, which would break the memory bound of this step.
3. **Iceberg transaction** on the table, via `iceberg-go` v0.7.0 `catalog/rest` against
   `<lakefs_endpoint>/iceberg/relative_to/<R>-metadata.<b>/api`, identifier `system.object_metadata`:
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
   - **Copy-on-write fallback.** A write with `write.delete.mode=copy-on-write` (§3.1, the copy-on-write
     mode of section 10 decision 2, which the plan keeps as the fallback if position delete files cannot be
     staged) first commits one compaction of the table (`compactDeleteFiles`) whenever the snapshot still
     carries live delete files, before the `Overwrite` above. `iceberg-go`'s copy-on-write rewrite reads the
     data file it rewrites and nothing else - `rewriteSingleFile` builds a scan task with no delete files -
     so without the fold it writes the rows an earlier merge-on-read delete removed back into the new file,
     and the old delete file, whose data file is no longer in the snapshot, can neither remove them nor be
     applied to the new file
     (upstream `apache/iceberg-go#2042`, fixed upstream in `apache/iceberg-go#2046` on 2026-10-15, after the
     pinned v0.7.0 of 2026-10-01). The fold applies and removes the delete files, so the rewrite reads data
     files only. It is a commit of its own, so the branch head moves twice inside the same lakeFS commit of
     step 4, and the cost is paid at most once after the last merge-on-read delete, because a copy-on-write
     delta writes no delete file itself. The fold records the table's **current**
     `lakefs.mds.source-commit` as its source commit rather than `C`, because the indexer's recovery finishes
     a staged write whose property is the next commit `C` (4.4): a fold that recorded `C` would let a delta
     that failed after it be committed as if `C` had been applied, with `C`'s change set missing. A table that
     carries delete files and **no** `lakefs.mds.source-commit` was not written by this package and has no
     position the fold could keep it at, so a copy-on-write delta there is refused with `table.ErrNoSourceCommit`;
     a `Rebuild`, which needs no position, fixes such a table (commit `5d68d07af`, MDS_REVIEW_4 R4T-1).
4. **lakeFS commit** of `R-metadata@b` - in process, through `pkg/catalog` (RA4b; it was
   `POST /repositories/<R>-metadata/branches/<b>/commits` before) - with the message and metadata of 3.3,
   `allow_empty=false`. The commit is never legitimately empty: step 3 always moves the
   `lakefs.mds.source-commit` property, even for a change set with no objects. The walk of 4.2 step 5 must
   therefore drop the recorded position itself even though the log API's `stop_at` is inclusive; applying it
   again would stage nothing and fail here with `commit: no changes`.
5. **Tag** `mds-<C>` → the new commit, in process as well. lakeFS never overwrites a tag, even with `force`:
   `pkg/graveler/graveler.go` `CreateTag` uses force only to pass the read-only check and returns
   `ErrTagAlreadyExists` for an existing tag, so the indexer deletes a stale tag before creating it (commit
   `30748d488`). A tag whose commit already records `C` is left where it is, even when it does not point at the
   branch head: after a compaction (4.9) the head is the compaction commit and the tag stays on the
   pre-compaction indexing commit, which has the same rows (commit `aab9216aa`).

Step 3 is the atomicity boundary for readers on the branch head (the staged pointer is what the catalog reads on a
branch). Step 4 is the atomicity boundary for the indexing position and for tag/commit reads.

### 4.4 Idempotency and crash recovery

Crash windows and what the next tick does (step 1 of 4.2):

| Crashed after | State found | Action |
|---|---|---|
| 3 (Iceberg commit staged, no lakeFS commit) | `R-metadata@b` has uncommitted changes, and the staged table's `lakefs.mds.source-commit` = next commit `C` after `last` | do steps 4–5 for `C` |
| 3, but the staged property is anything else (partial or foreign writes) | uncommitted changes | `resetBranch(R-metadata, b)` (discard staging), then index normally |
| 3, but a foreign write staged changes whose property happens to equal `last` | uncommitted changes, property = `last` | the branch diff (4.2) shows the staged changes, so `resetBranch(R-metadata, b)` (discard staging), then index normally |
| 4 (lakeFS commit done, no tag) | head metadata = `C`, tag `mds-<C>` missing or pointing elsewhere | do step 5 (a mispointed tag is deleted and created again) |
| during 3 (data files written, commit not sent) | orphan files under `_iceberg/<uuid>/data/`, nothing staged | nothing. The files are unreferenced and removed by `lakectl iceberg cleanup` (section 9). |

The copy-on-write fold of 4.3 step 3 is a commit of its own, so one such delta stages two snapshots. The
fold records the table's *current* `lakefs.mds.source-commit`, not `C`, which keeps the table above exact: a
staging area left by a delta that failed after its fold carries the recorded position rather than the next
commit, so it is the "anything else" row, discarded with `resetBranch`, and the tick re-indexes `C` (commit
`5d68d07af`, MDS_REVIEW_4 R4T-1).

A history rewrite (4.7) discards the staging before it resets the branch: real lakeFS `ResetHard` keeps the
staging area (`pkg/graveler/graveler.go`), so a pointer staged by a crashed step 3 would otherwise survive the
hard reset and the rewrite would run on top of the stale table (commit `383bf37b5`).

Before step 3 the indexer also reloads the table (so the "already at `C`, skip to step 4" check reads durable
state, not the tick's cached table) and skips to step 4 if the current `lakefs.mds.source-commit` already equals
`C`. A 409 from the Iceberg commit means another writer moved the table: the branch tick restarts up to 3 times
(`conflictRetries` in `pkg/mds/indexer/indexer.go`), re-reading the position and re-running recovery each time;
after the third restart the branch fails for this tick and the next tick resumes at the recorded position
(commit `383bf37b5`). The writer's other "the table moved since it was read" answer, `table.ErrStaleTable`, is the
same restart: `tableError` maps it to `ErrConflict`, so a tick that meets it - including on the repair commit that
is meant to make an old table non-replayable - reloads and retries instead of failing the branch for the period
(MDS_REVIEW_3 R3I-3, commit `2ae794038`).

### 4.5 First indexing of a branch (backfill)

When `R-metadata@b` does not exist:

1. **Seed from an indexed ancestor.** Walk the first-parent log of `R@b` from its head (at most a fixed 1000
   commits, the removed `seed_lookback`, X6). If a commit `A` with tag `mds-<A>` exists in `R-metadata`, create
   `R-metadata@b` from that tag and continue with 4.2 (`last = A`). This makes a new feature branch cheap.
2. **Else full listing.** `S` = the oldest first-parent commit of `R@b` with `creation_date ≥ since` (or the
   branch's oldest first-parent commit when `since` is unset). That is the guide's rule (X11): "If omitted,
   metadata is captured from the time the branch was created". There is no `backfill` key any more; the removed
   `backfill: head` behaviour (index only the head on a branch's first sync) is gone. Create `R-metadata@b` from
   the `R-metadata` initialisation commit (empty table), write all objects of `R@S` with `commit_id = S`
   (rebuild path of 4.3 step 2), commit with `lakefs.mds.source-commit = S`, tag `mds-<S>`. Then continue
   forward.

### 4.6 Writing files: credentials

The catalog stages client-written files; the indexer must write Parquet and Avro files to
`<R-metadata ns>/_iceberg/<table-uuid>/`. `iceberg-go` does **not** implement S3 remote signing
(`io/gocloud/s3`: "remote S3 request signing is not supported"), so:

- If the catalog vends write credentials (S3 blockstore with `credentials_vending.role_arn`), the indexer requests
  `X-Iceberg-Access-Delegation: vended-credentials` (the iceberg-go REST default) and needs nothing else.
- Otherwise the indexer writes with the **standard `blockstore.*` configuration of the process** (X6, RA4b):
  `cmd/lakefs/mdscmd/mds.go` translates `blockstore.s3.*` (region, endpoint, static credentials, path style) or
  `blockstore.azure.*` (storage account and key) into the iceberg-go IO properties (`s3.*`, `adls.*`) and passes
  them with `rest.WithAdditionalProps`. This is the mode esti runs in (SeaweedFS, static keys). There is no
  `metadata_search.storage` or `metadata_search.azure` section any more: the guide has no such keys, and the
  official chart gives the MDS process the server's own environment (`lakefs.sharedSecretEnv` plus
  `mds.extraEnvVars`/`extraEnvVarsSecret`), so it reads exactly what the server reads.
- A blockstore without static credentials (GS, or S3 on an instance role) yields no properties and leaves the
  client's own credential chain, or the catalog's vended session, in charge.

**Azure (`METADATA_SEARCH_AZURE.md` D3).** The companion table's location on an Azure blockstore is
`abfs[s]://<container>@<account>.<dfs host>/…` (§3.3), so the indexer needs an IO backend that opens
that scheme: `pkg/mds/table/catalog.go` blank-imports iceberg-go's `io/gocloud/azure` next to
`…/s3`, which registers `abfs`, `abfss`, `wasb` and `wasbs`. Without it the client would answer
"unsupported scheme" for a location the catalog itself just handed out.

The Azure properties come from `blockstore.azure.storage_account` and
`blockstore.azure.storage_access_key` (RA4b), the same values the server pod gets, so one secret carries both and
there is no second identity to configure. `blockstore.azure.test_endpoint_url` is **not** translated: it is an
emulator-only setting of the lakeFS block adapter, and the indexer's iceberg-go client builds its service URL
from `adls.endpoint` itself (apache/iceberg-go#2311). The removed `metadata_search.azure` section had `endpoint`,
`protocol`, `use_managed_identity` and `client_id` for that; on a real account the default
`DefaultAzureCredential` chain (workload identity, managed identity, CLI) is what a pod without a shared key
uses, and it needs no configuration.

GCS is out of scope (N2): `gs://` needs no location translation and every FileIO opens it, but this
build registers no GCS IO on the indexer and nothing here is tested on GCS.

GCS is out of scope (N2): `gs://` needs no location translation and every FileIO opens it, but this
build registers no GCS IO on the indexer and nothing here is tested on GCS.

### 4.7 History rewrites (hard reset, revert-by-reset, force-moved branches)

When `last` is not an ancestor of `head`: discard the staging (4.4) and then `mb = findMergeBase(R, last, head)`.
If tag `mds-<mb>` exists, `hardResetBranch(R-metadata, b, "mds-<mb>")` and continue from `mb`. Otherwise do the
full listing of 4.5 step 2 at `head`, on a branch hard-reset to the initialisation commit. The fallback is also
taken when the server answers `findMergeBase` with its unmapped "no merge base" 500 (unrelated histories after a
force-move), and when `last` is an ancestor of `head` only through a merge's second parent: the first-parent walk
never meets `last` there, so replaying the chain would rebuild the table from the root on top of the stale one.
(A normal `revert` is a new commit and needs none of this; commit `383bf37b5`.)

The same full listing at `head` is the fallback for two more conditions (commit `6440e2f13`):

- **The recorded position no longer exists in `R`.** Committed GC after a force-move while the indexer was
  down, or `R` deleted and re-created under the same name (§4.8), make the position unreachable. A 404 from
  `findMergeBase` — or from the log's `stop_at`, which `pkg/catalog` dereferences before it walks — is treated
  as "the position is gone" only after `getCommit(last)` also answers 404; a 404 about the head, or any other
  transient 404, still fails the tick. Without the fallback the branch reported `last_error` on every tick
  forever, with `/_health` at 503 and no way back.
- **The position is deeper than one tick may walk** (`max_commits` plus one page, §4.2 step 5).

Both run the full listing at `head` on a branch hard-reset to the initialisation commit, logged as a rebuild
(`mds: rebuilding the metadata branch at the data branch head` with the reason).

### 4.8 Branch and repository lifecycle

| Event in `R` | Indexer action |
|---|---|
| matching branch created | 4.5 |
| matching branch deleted | delete `R-metadata@b`. The guide has no `delete_branches` key (X6) and the behaviour is fixed at the old default, true. Its tags stay, so commit/tag reads of its history still work. Its `/_health` entry and lag gauge are always removed. `R-metadata`'s **default branch is never deleted**, even when its name is no longer a branch of `R` (an `R` default-branch rename): lakeFS refuses a default-branch delete (`pkg/graveler/graveler.go` `DeleteBranch` returns `ErrDeleteDefaultBranch`), so the indexer skips it and logs the skip once per process. |
| branch stops matching the config | nothing to the table (left as is); its `/_health` entry and its lag gauge are removed, so a stale error does not keep `/_health` at 503 and a branch dropped from the selector list does not keep reporting its last lag forever. The cleanup is derived from the branches the previous tick followed, not from a listing of `R-metadata`, so it also reaches a branch that failed before its metadata branch existed (commit `98f035d34`). |
| repository added to config | create `R-metadata` (3.3). lakeFS creates it non-bare, so its default branch starts with the automatic `Repository created` root; create namespace `system` and the table through the catalog on the default branch, then lakeFS commit `mds: init` as the first commit after that root (the **initialisation commit**, metadata `lakefs.mds.init=true`), with `allow_empty=false`. Then 4.5 for each matching branch. |
| `R-metadata` exists and its history is only lakeFS's automatic `Repository created` root | complete the initialisation on top: create namespace `system` and the table, then the `mds: init` commit (metadata `lakefs.mds.init=true`) with `allow_empty=false`. Only one of two concurrent processes can commit it, because the commit carries the table creation; the loser finds nothing staged, gets `lakefsErrNoChanges`, re-verifies the branch, and adopts the winner's initialisation commit instead of writing a second marker (commit `db8172798`). |
| `R-metadata` exists with a contiguous run of `mds: init` commits (`lakefs.mds.init=true`) that reaches the root (a bare repository) or ends directly above the automatic root | accept it as the indexer's; later commits are indexing commits. The run, not a single commit, is the idempotency point: a duplicate marker left by an older indexer (`[init2 init1 root]`) is accepted. The member closest to the root is recorded as the initialisation commit — the one that created the table and the one 4.5 and 4.7 reset to. |
| `R-metadata` exists with any other history, or with a marker outside such a run | refuse that repository, log an error. In particular, a foreign commit after the automatic root without the marker is refused. |
| repository deleted | log, skip. `R-metadata` is left for the admin. |

The initialisation commit is the ownership point of the table as well as of the repository. The catalog's
table creation **stages** the pointer, so a principal that stages `system.object_metadata` on a fresh
`R-metadata@b` before the first tick would otherwise supply the metadata (schema, `location`, properties)
that `initialise` commits as `mds: init` - permanently, because the branch is initialised afterwards. The
`ErrTableExists` arm therefore loads the staged table and refuses it with `ErrForeignRepository` unless it
carries `lakefs.mds.repository = R`; a table with another value or none gets no initialisation commit
(commit `da24d421f`).

The concurrency rule of the table above is also bounded: a 400 from the initialisation commit is treated as
"lost the race" only when re-verification finds another replica's contiguous `mds: init` run to adopt. A
branch that still shows only lakeFS's automatic root has nothing to adopt, so the original 400
(`block.ErrWriteFailed`, a validation error, an HTTP intermediary) is returned as that repository's error for
the tick instead of re-entering `initialise` (commit `da24d421f`).

### 4.9 Maintenance

- **Compaction.** Position deletes accumulate. When the table has more than `compaction.max_delete_files`
  (default 200) delete files, or every `compaction.every_commits` (default 500) commits, the indexer scans the table
  at the current snapshot and, in one transaction, removes every data file of the scan plan - one entry per
  file: `iceberg-go` splits a Parquet file above `read.split.target-size` into several tasks that carry the same
  file, each with its own offset and length, and `ReplaceFiles` refuses a delete list in which a path appears
  twice, so the removal list is de-duplicated by path (MDS_REVIEW_3 R3T-1, commit `7f5fd526b`) - and every delete
  file of the current snapshot (`Transaction.ReplaceFiles`), then appends the scanned rows, with the same
  `lakefs.mds.source-commit`. A trigger set to `0` is **disabled**, not replaced by the default: the documented
  defaults apply only to an absent key, and a negative value is refused at startup (commit `98f035d34`). A plain
  `Overwrite` is not enough: it rewrites data files but leaves their delete
  files in place, so deletes would keep accumulating. The delete files are enumerated from the snapshot's
  manifest list, not from the scan tasks: a scan task carries a delete file only through the data file it
  applies to, so a delete file whose data files a rebuild (§4.3 step 2) replaced is carried by no task, yet it
  is still referenced by the live snapshot — no file cleanup can reclaim it, and `total-delete-files`, the
  counter of the delete-file trigger, would never fall back to zero and the trigger would latch on forever
  (commit `e3083e279`). The rows it rewrites keep the table's declared `path ASC` order in its files without collecting the table: a
  compaction merges the per-file sorted streams (a k-way merge), because `iceberg-go` sorts each record batch on its own and the scan plans files in the order the
  commits wrote them - streaming the scan back into `Append` wrote `[b d a c]` for the interleaved files `[a c]`
  and `[b d]`, whereas two such files now compact into `[a b c d]`, whose narrow path lower/upper bounds are
  what the next delta's `IN (path …)` scan prunes with. While a compaction runs the process holds (MDS_REVIEW_3
  R3T-5, commit `7f5fd526b`): (a) the merge's own set - one read batch per data file, each
  `max(256, 131072/files)` rows (2 × 64 Ki-row record batches divided by the files, floor 256), plus one output
  batch of 64 Ki rows - which overrides the table's `read.parquet.batch-size` during the compaction. The batch
  is sized by the plan's **data files**, not its scan tasks: `iceberg-go` plans several tasks for one Parquet
  file above `read.split.target-size`, and dividing by the task count undersized every file's batch (commit
  `2b494acc4`, MDS_REVIEW_4 R4T-6). The reader still opens one run per scan task, so on a table that holds such
  a file the merge holds one batch of that size per split range - up to (tasks/files) × the budget, rather than
  one batch per file; the per-file figure is what this section budgets and what the code holds for a table of
  files below the split target, and closing the split-range gap needs one run per file, a change of the stream
  layout rather than of this arithmetic; (b)
  `iceberg-go`'s scan prefetch per file, the arrow scanner's records channel and the sequenced channel it feeds,
  both sized by the scan's concurrency (one worker for a single-task stream, so a small constant), carrying
  batches of the same per-file size; (c) `iceberg-go`'s writer queue, up to 64 record batches of at most 64 Ki
  rows (`rollingDataWriterQueueCapacity`), retained until written, plus the zero-copy source batches those
  slices keep alive; (d) one Parquet row group buffered by the writer, up to `write.parquet.row-group-limit`
  rows (1 048 576 default) or `write.parquet.row-group-size-bytes` (128 MiB default); and (e) for a table last
  compacted before commit `2daeca114`, every legacy file at once (see below). (a)-(d) are bounded independently
  of the table's row count, and (a) grows with the number of files rather than with the number of rows: above
  512 files the floor starts to bind and the merge set is 256 rows per file. Every file this
  writer produces is an ascending contiguous slice of the declared order; a file written by a compaction before
  commit `2daeca114` need not be, so a first pass reads that file's path column alone, and a file that fails the
  check is read whole, sorted on its own and merged as a materialised run - the memory of **every** legacy file
  at once, not of one file and not of the table: a k-way merge cannot stream a run whose rows are not already in
  order, so each legacy file stays resident until the merge is done. That is bounded by the legacy files and one
  compaction removes the reason for it (MDS_REVIEW_3 R3T-3, commit `7f5fd526b`). The output is the scan's own
  Arrow columns, sliced and concatenated, never rows rebuilt from the ten
  `Row` fields, so a top-level column a later schema version appended with a new ID keeps its values through a
  compaction (§3.1). The compaction reader declares the Arrow schema `iceberg-go` returns for the projected
  columns - the scan's own schema, whose fields carry none of the table's field-ID metadata - rather than
  `ArrowSchema(projection)`, so the schema it declares is the schema of every batch it yields, the ones
  `buildMergedBatch` concatenates included; what the writer puts in the Parquet files is unchanged, so field
  IDs still come from the writer's own conversion (MDS_REVIEW_3 R3T-8, commit `7f5fd526b`). The reader owns the
  streams it opens and frees them on `Release` whether or not a batch was pulled, so a `ReplaceFiles` or an
  `Append` that fails before the first pull closes every file and frees a legacy file's batches (MDS_REVIEW_3
  R3T-7, commit `7f5fd526b`). A panic in the merge's own pull loop becomes `table.ErrMergeRows`, because
  `iceberg-go` runs the merge on the writer's goroutine, where the indexer cannot recover it; the setup reads
  before the merge - the path-order scan and the materialisation of a legacy file - run in the caller's
  goroutine and are not converted (MDS_REVIEW_3 R3T-6, commit `7f5fd526b`). The rebuild's row source is
  converted the same way and for the same reason: `rowBatches` reports a panic of the caller's `Rows` source as
  the new `table.ErrRowsPanic`, which is what `Transaction.Append` and therefore `Rebuild` return, so a panicking
  source fails the branch instead of taking the process down (commit `2b494acc4`, MDS_REVIEW_4 R4T-4; 4.3
  step 2). The transaction pins `commit.retry.num-retries = 0` like every other write, so a lost CAS
  is a conflict the indexer retries rather than a replay of the removals on top of a newer snapshot (commits
  `2daeca114` and `d993d602e`). Then it does a lakeFS commit `mds: compact <R>@<C>` (no
  new tag; the tag stays on the pre-compaction commit, which has the same rows).
- **Snapshot expiry and file sweep** are not done by the indexer: run `lakectl iceberg cleanup -r <R>-metadata`
  on a schedule (`cmd/lakectl/cmd/iceberg_cleanup.go`). It honours lakeFS references (branch heads, tags, commits
  retained by GC), so `mds-*` tags keep their snapshots readable.
- **Tag retention.** Every `mds-*` tag is kept forever: the guide has no `tag_retention` key (X6), so the
  retention code is fixed at `0`. The rules it implements while it runs (a tag that records a live position is
  never retired; a tag whose source commit no longer exists is retired because it can no longer resolve) stay in
  the code for a future key, and are covered by tests, but no configuration reaches them (commit `aab9216aa`).

## 5. Catalog side (landed; realigned by RA1-RA3 and RA10)

### 5.1 API as implemented (`pkg/icebergcatalog/resolver.go`)

```go
const (
    SystemNamespace          = "system"
    ObjectMetadataTable      = "object_metadata"
    MetadataNamespace        = "metadata"
    ObjectsTable             = "objects"
    MetadataRepositorySuffix = "-metadata"
)

type ResolvedTable struct {
    Target Address // where the pointer is read: R-metadata, mapped ref, system, object_metadata
    Authz  Address // what authorization is checked against: the data repository's catalog ARN
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
func NewDeprecatedMetadataSearchResolver(branches BranchLister, mapper RefMapper) *MetadataSearchResolver
func WithTableResolver(resolver TableResolver) StoreOption
```

`http.go` builds **two** catalogs when `iceberg.metadata_search.enabled` is true, with
`metadataSearchRefMapper(deps.Catalog)` in both and each with a store of its own: the lakeFS catalog
(`/iceberg/api` and `relative_to`) registers `NewMetadataSearchResolver`, and the guide's deprecated Metadata
Search catalog (`/mds/iceberg/api`) registers `NewDeprecatedMetadataSearchResolver` and mounts the read operations
only (METADATA_SEARCH_REALIGN.md G4, G5; RA3).

The lakeFS catalog resolves `system.metadata.<R>.<ref>.objects` to
`R-metadata.<mapped ref>.system.object_metadata` and nothing else. There is no `system` namespace, no virtual table
and no reservation below `<repo>.<ref>`: `R.<ref>.system.object_metadata` is an absent table there, and it answers
the 404 of one, like every other name nobody created (§5.3). The deprecated catalog resolves the older identifier
`R.<ref>.system.object_metadata` to the same companion table - it serves load, head and list of it and resolves
nothing else, and the root address is not its address either.

Resolution of the ref, in both catalogs: a ref that is a branch of `R`
(`GetBranchReference`) maps to the same name; anything else (`ErrBranchNotFound`, `ErrInvalidBranchID`) goes to
the mapper. A mapped branch that does not exist in `R-metadata` answers 404 `NoSuchTableException`.

The resolved table is also **proved** before it is served: `MetadataSearchResolver` implements
`ResolvedTableVerifier`, and `Store.Resolve` serves `R-metadata`'s `system.object_metadata` only when **two**
independent facts hold. First, the loaded table's `lakefs.mds.repository` property equals the data repository the
caller named. Second, the served ref's first-parent history carries the indexer's initialisation commit: message
`mds: init`, metadata `lakefs.mds.init=true`, and position as the first commit directly above the repository's
root commit, or - for a companion repository created bare - the root commit itself (§4.8). The **committer is not
part of the proof**: the guide lets the sync run "as another user instead" with `MDSCatalogAccess`, so a check
pinned to one committer name would refuse a legitimate sync (METADATA_SEARCH_REALIGN.md X4, RA5). The property alone proves nothing: it lives in
`metadata.json`, which anyone who can create `R-metadata` can write, while the initialisation commit is written by
the indexer after it creates the table, so it also commits the table and is the state a forger cannot reproduce by
editing a document. A table that names another repository, carries no property, or whose history has no such
commit answers 404 `NoSuchTableException` for the address the caller asked about: the name `R-metadata` alone does
not prove who wrote the table, and the redirected read runs with the server's identity (§5.2), so a companion
repository someone else created first would otherwise be served to every reader of `R`. Because the check lives
in `Store.Resolve` it also covers `HEAD`, metrics, vended credentials and remote signing, whose scope and location
come from the resolved table. The proof loads the target's `metadata.json` once in `Store.Resolve` and the read
path loads it again; correctness, not read count, motivated the placement (commit `4aaf60cb3`).

`Commit` takes the message and the metadata from the client, so those two facts alone prove nothing; the position
is where the commit sits in history, which is what the check reads. The earlier design also checked the committer
and reserved the `mds-indexer` name in `CreateCommitRecord` (commit `2a822c719`). `pkg/api` still carries that
reservation as a leftover of the earlier design (`metadataSearchIndexerPrincipal`), but it is no longer
load-bearing: the resolver does not read the committer, and the guide has no such reservation. `LoadCommits`,
which `lakectl refs-restore` uses, copies commit records verbatim without going through that endpoint; a caller
able to write forged metaranges directly into the block store is outside the API trust boundary.

The remaining boundary is `LoadCommits`, not this endpoint. `lakectl refs-restore` restores a repository through
`RestoreSubmit` → `catalog.LoadCommits`, which copies commit records verbatim from the manifest's metaranges and
never calls `CreateCommitRecord`: restoring an ordinary repository is unchanged, restoring a companion repository
can reproduce the true records - the indexer's included - but not fabricate them through the API, because a
manifest from another companion carries another `lakefs.mds.repository` property, which the first check still
refuses. A caller able to write forged metaranges directly into the block store is outside the API trust boundary
(commit `2a822c719`).

The position rule is the one the indexer applies, for both shapes of companion: the initialisation commit is the
first commit directly above a root commit, **or** the root commit itself when the companion repository was created
bare - its `mds: init` commit is the branch's first commit and has no parent (§3.3, §4.8). The indexer accepted
that history before the catalog did, so a bare companion used to be indexed but not served: every read of its
`system.object_metadata` answered the 404 of an unindexed ref. The catalog now accepts the root form too, with the
same two facts (message `mds: init`, metadata `lakefs.mds.init=true`), so a bare companion is served like a
non-bare one; a commit with no parents cannot be appended to a history the catalog already trusts. The negative
shape stays refused: an init commit with another commit between it and the root (commit `30af7b3a5`).

The history half costs the length of the served history in the worst case, because the initialisation commit is
the oldest non-root commit (or the root itself on a bare companion): the walk is newest first, one 256-commit
page at a time, and stops at the root. The
init commit it finds is then cached per companion repository - immutable once found - together with a bounded
set of at most 8 heads already proven to descend from it, and a new or repeated head is verified newest first only
until it reaches that commit or one of those heads, so a branch that gained commits pays for the new commits and
not for its whole history again. The cache holds at most 1024 repository entries, evicted oldest first, so its
size follows the repositories served and not the number of indexed commits, and a history that does not carry the
immutable init commit is refused even when it carries a look-alike marker (MDS_REVIEW_3 R3S-4 and R3S-8, commit
`db52bd9a7`). A resolver whose `BranchLister` cannot read commit history fails closed -
the property proves nothing without the commit - so the metadata table answers the 404 an unindexed ref answers
(commit `500a00a37`).

### 5.2 Authorization (landed)

`Store.ResolveAuthorized` checks the requested address on `R` with the **catalog action only**:
`catalog:ReadTable`, plus `catalog:ReadTableData` for signing/vending. There is no `fs:ReadRepository` leaf (X14,
RA10): the Iceberg docs' example policies grant only `catalog:*` actions on catalog ARNs, state that "Iceberg
catalog permissions supersede any related object permissions", and the Metadata Search guide's grant "for
Metadata Search alone" is likewise catalog-only. The redirected read of `R-metadata` runs with the server's
identity. Caveat (upstream too): the grant covers metadata of all objects on all refs of `R`. Path-level or
branch-level `fs:` policies do not narrow it, and an `fs:*` grant without a `catalog:*` one is refused. Vended credentials and signing for a metadata table are scoped to
`R-metadata`'s table location (`creds.go`, `sign.go`), and a write session is never vended for one: a write that
the catalog redirects to the companion repository becomes a read session (5.3). The root address authorizes at the
guide's ARNs of `R` - `table/<R>/system/object_metadata` and `namespace/<R>/system` - which is why the same policy
covers the root address, the deprecated catalog's identifier and the data plane of both.

### 5.3 Read-only root `system` tree; nothing reserved below a data repository

The only reserved namespace is the root `system` namespace of METADATA_SEARCH_REALIGN.md RA1. Every write into the
tree - create, commit, register, drop, and namespace create, update-properties and drop - answers 403
`ForbiddenException` (`ReservedNamespaceError`), at every level, because the tree is marker-less and the catalog
serves it. Its `GET`/`HEAD` endpoints do not consult markers: existence is derived from the companion repository
and its branches (Store.SystemMetadataExists, SystemMetadataRepositories, SystemMetadataBranches,
SystemObjectsExists) and the ownership proof of §5.1.

Nothing below `<repo>.<ref>` is reserved any more (METADATA_SEARCH_REALIGN.md §2, RA3). A data repository's `system`
namespace is an ordinary namespace: a client that may write the repository can create it, create tables in it,
read and drop them, and the catalog lists exactly what the client created. The old per-repository rule - 403 on
every write under `R.<ref>.system`, listings that showed `system` and `object_metadata` without a pointer entry -
is deleted, together with the data-plane extension of it (commits `80f6927e2`, `d2811fdcc`, `500a00a37`).

The data plane follows the same split. A remote-signature request for a write (`PUT`, `POST`, `DELETE`, `PATCH` -
any method the authorizer classifies as `catalog:WriteTableData`) into the reserved tree is refused with 403
`ForbiddenException` (`*ReservedNamespaceError`); read signs (`GET`, `HEAD`) stay allowed, so the metadata table
stays readable. The vended-credentials path answers a write vend for an address the resolver redirects to the
companion repository with `*ReservedNamespaceError`, which `TableCredentials` treats as a write refusal and turns
into the read session (METADATA_SEARCH_REALIGN.md G8: vended credentials for metadata tables are always
read-only), so a redirect never hands out a write session for `R-metadata`'s files; a direct write `Vend` is
refused rather than silently downgraded. The deprecated catalog mounts no write operation at all, so neither path
exists there.

### 5.4 Config (landed)

`iceberg.metadata_search.enabled` (default false) is the server switch of this build: when it is false, nothing
changes - no tree, no deprecated catalog, no ownership proof. It **requires** `iceberg.enabled`: Metadata Search is
a mode of the catalog, so `enabled: false` with `metadata_search.enabled: true` is a startup error,
"metadata_search.enabled requires enabled: Metadata Search is served by the Iceberg catalog, which is off"
(commit `4aaf60cb3`).

The guide describes activation differently - "Metadata search activates when this map is non-empty", the map being
`metadata_search.repositories`. The server does not read that map for this purpose any more (RA5 made
`MDSCatalogAccess` installation-wide), so the flag above is what turns the catalog side on. That is a deviation
from the guide's text, and it is the one the build has; the card that removes the flag is X5, which is not
implemented.

### 5.5 Deprecated, separate Metadata Search catalog (RA3)

`<lakefs>/mds/iceberg/api` is the guide's deprecated catalog: it serves load, head and list of
`<repo>.<ref>.system.object_metadata` and nothing else, resolved to the same companion table as
`system.metadata.<repo>.<ref>.objects`. It is a **catalog of its own**, not an alias of `/iceberg/api` (G4, G5):
`http.go` builds two catalogs with two stores when `iceberg.metadata_search.enabled` is true, the lakeFS catalog
with `NewMetadataSearchResolver` and this one with `NewDeprecatedMetadataSearchResolver`, and it mounts the read
operations only. It has no `relative_to` form and no root tree. Same auth, same prefix (`CatalogPrefix = "lakefs"`),
OAuth at `<lakefs>/mds/iceberg/api/v1/oauth/tokens`.

G5's rule is why the lakeFS catalog never answers the old identifier: `<repo>.<ref>.system.object_metadata` on
`/iceberg/api` or on a `relative_to` warehouse is an absent table there, exactly like any other name nobody
created. The one place the identifier still resolves is this mount.

While `iceberg.metadata_search.enabled` is false neither catalog is mounted, and the prefix is left to the core
router's not-implemented placeholder: `/mds/iceberg/api/…` answers **501** (`http.StatusNotImplemented`), not a
plain 404. The placeholder is `notImplementedMounts` in `pkg/api/services.go`, mounted by `cmd/lakefs/cmd/run.go`
after the extra-routes builders, so a plain `lakefs` binary answers the same 501.

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

Read by `lakefs mds run` from the top-level key `metadata_search` of the **server** configuration (RA4b). The
server itself reads only `iceberg.metadata_search.enabled`; the rest of the configuration of that process is the
ordinary lakeFS one (`database.*`, `blockstore.*`, `auth.encrypt.secret_key`). The guide's key list (G10, G11, X6)
is exactly this:

```yaml
metadata_search:
  lakefs_endpoint: http://lakefs:8000       # required; lakeFS base URL (catalog at /iceberg/api)
  access_key_id: AKIA...                    # optional, together with secret_key
  secret_key: ...                           # optional, together with access_key_id
  listen_address: 0.0.0.0:8080              # /_health, /metrics, /_pprof/
  period: 60s                               # > 0
  concurrency: 25                           # branches in parallel, >= 1
  max_commits: 1000                         # per branch per tick, >= 1
  since: 2026-01-01T00:00:00Z               # optional, RFC 3339
  repositories:                             # an empty map disables Metadata Search
    my-repo:
      branches: [main, "dev-*"]             # exact or trailing-* glob
      storage_namespace: s3://bucket/metadata/my-repo   # optional (3.3)
```

- **`lakefs_endpoint`** is the base URL, required. Table writes go to `<lakefs_endpoint>/iceberg/api`.
- **`lakefs_mds_endpoint`** is the deprecated spelling (G10). It is accepted **only** when it ends in
  `/mds/iceberg/api`, which is stripped with a warning; a value without that suffix is a base URL and is refused
  with a message that names the rename to `lakefs_endpoint`. An installation that used the old key, as this one's
  old configs did, must rename it.
- **`access_key_id`/`secret_key`** are optional, and both-or-neither (G11, X7). With neither, the sync
  authenticates as `mds-service-user` with a login token it signs itself (section 8).
- **Validation** is exactly the guide's: `lakefs_endpoint` set, the key pair both set or both empty, `period > 0`,
  `concurrency >= 1`, `max_commits >= 1`. Nothing else fails startup (X7, X9): an empty `repositories` map
  disables Metadata Search instead of failing, a repository name that ends in `-metadata` or is longer than 54
  characters is accepted, and a branch selector that matches nothing is not an error.
- **`since`** bounds a backfill of a branch; unset means "from the time the branch was created" (X11).

Everything the removed keys of X6 used to configure is fixed behaviour now: the catalog client's request bound is
60s, the seed lookback is 1000 commits, the rebuild threshold is 100 000, compaction fires above 200 delete files
or every 500 indexed commits, `mds-*` tags are kept forever, and a deleted data branch loses its metadata branch.
The object-store credentials of a write come from the process's `blockstore.*` configuration (section 4.6), not
from a `metadata_search` key.

Per-repository runtime errors (repository missing, namespace not derivable or overlapping, companion repository
read-only, or a branch-protection rule the first write meets) mark that repository failed in `/_health` and
metrics, and the others continue.

Environment overrides follow lakeFS conventions (`LAKEFS_METADATA_SEARCH_SECRET_KEY`, …).

## 8. Principals, policies, bootstrap

The guide provisions one sync identity on every lakeFS start. `pkg/mds/bootstrap` does it as an extra routes
builder of `lakefs-sso` that mounts no route: it has to exist before the first indexer request is authorized,
whether or not a request ever arrives.

| Principal | Created by | Permissions |
|---|---|---|
| `mds-service-user` (user, no group) | `pkg/mds` bootstrap on every start | policy `MDSCatalogAccess`: `catalog:*` on `arn:lakefs:catalog:::*` - the catalog permissions the sync needs to create the `system` namespace and the `object_metadata` table in a companion repository and to commit to it, and nothing outside the Iceberg catalog. |
| query users | existing users | Through the catalog: only read on `R` (5.2). Engines reading `R-metadata` directly (not through the metadata tree) need read on `R-metadata` like any repository. |

- **Authentication.** With no keys configured, the sync signs its own login token for `mds-service-user` with
  `auth.encrypt.secret_key` (`pkg/mds/table.SignServiceLoginToken`, the same claims and HMAC as
  `POST /auth/login`), and the catalog accepts it as a bearer token. There is no credential to create, so the
  operator configures none. With `access_key_id`/`secret_key`, the sync logs in as that user instead, who needs
  the same `MDSCatalogAccess` policy attached: the policy is a named, attachable policy for exactly that reason.
- **No `fs:` grant.** The sync reads the data repositories in process through `pkg/catalog` (RA4b), so it needs
  nothing on `arn:lakefs:fs:::*`; its one policy is catalog-only.
- **Idempotence and repair.** Every start creates the user when absent (`auth.ErrAlreadyExists` is success),
  creates `MDSCatalogAccess` when absent, rewrites it when its statements differ from the built-in ones (a
  hand-edited or pre-created policy is repaired, not trusted), attaches it tolerating `ErrAlreadyExists`, and
  detaches `MDSFSReadAccess` when it is still attached (the guide detaches the read-only policy on start; the
  policy itself is not deleted, it may be attached elsewhere).
- **What it removes.** The principals, group and policies of the earlier design: user `mds-indexer` (with its
  credentials), group `MetadataSearchIndexers`, the `mds-indexer` allow policies including their numbered chunks,
  every `mds-metadata-repo-protect[-<n>]` deny policy, and the Metadata Search part of
  `iceberg-reserved-prefix-protect` (X1-X3; there was none to remove, that policy reserves `_lakefs_iceberg/`).
  Only names this package owns are removed.
- **Degraded auth backends.** `auth.basic` stores one administrator and answers every policy call with
  `auth.ErrNotImplemented`; creating `mds-service-user` there would rename that administrator, so the bootstrap
  logs a warning naming what is not in force and continues. The in-process ACL service of `lakefs-sso` and the
  Enterprise auth service both implement the attachment; a backend that cannot attach a policy to a user fails
  startup with that answer instead of starting an unauthorized sync.
- **Startup check** (guide, verbatim): the service "checks that `lakefs_endpoint` answers as the lakeFS Iceberg
  REST catalog, and stops if it does not or stays unreachable for two minutes. If the catalog rejects the
  service's token or denies it access, the service logs the likely causes, starts anyway, and retries on every
  sync cycle." `pkg/mds/startupcheck.CheckCatalog` probes `GET <endpoint>/iceberg/api/v1/config`.
- **No init-commit proof.** The earlier design's MDS-SEC-3 committer check is gone (X4): the guide's "as another
  user instead" contradicts a check pinned to one committer, and the ownership proof reads the marker and the
  position only (5.1).

## 9. Operations

- **Health:** `/_health` returns 200 when every repository's last tick succeeded or is in progress, 503 otherwise,
  with a JSON body per repository and branch (`last_indexed`, `head`, `lag_commits`, `last_error`). A tick in
  progress never clears the outcome of the previous one: a repository whose tick failed stays 503 for the whole of
  its retry, because a retry is not an outcome, and a cancelled tick (a graceful stop) is not an outcome at all -
  it ends `in_progress` and leaves the previous result as it was (commit `98f035d34`). `lag_commits` is
  exact, never a lower bound: the bounded walk of 4.2 step 5 counts it per page, and a rebuild at the head leaves it
  at 0 (commit `6440e2f13`). It is **`-1`** when the last attempt failed before it counted the pending commits
  (`indexer.LagUnknown`): the branch then reports the position it recorded (read back from the head of
  `R-metadata@b`) and an unknown lag, instead of `lag_commits: 0` with an empty `last_indexed`, which reported the
  failure mode the lag alert exists for as healthy. A healthy branch never reports `-1` (commit `98f035d34`). A
  request that exceeds the catalog client's fixed 60s bound fails the branch or the repository, so the probe turns 503 instead of
  staying 200 while the tick hangs (commit `7f952279b`). A protection rule, or a companion repository that a write
  finds read-only, lands in each branch whose first write lakeFS refused: `/_health` is 503 with a per-branch
  `last_error`, the repository's own `last_error` stays empty, and `errors_total{stage="branch"}` counts the
  branches rather than the repository (§3.3, MDS_REVIEW_3 R3I-4, commit `2ae794038`).
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
- **Security tests** (esti `mds_test.go`): a user without any grant gets 403 on the metadata tree; a user with
  access to `R-metadata` only gets 403 through `system.metadata.<repo>.<ref>.objects` while reading `R-metadata`'s
  own table; the guide's catalog-only grant alone reads and lists the tree; a write signature for an address of
  the tree is 403 while a read signature is 200; `mds-service-user` carries `MDSCatalogAccess` and the earlier
  design's principals are gone. The earlier deny-policy cases are gone with the deny (X2).

## 10. Decisions (former open questions)

1. Indexer runs as the `mds run` subcommand of the server binary, on the server configuration (RA4b). A goroutine
   mode inside the server is out of scope for A.
2. `iceberg-go` v0.7.0 writes merge-on-read position deletes (`Transaction.Delete` with `write.delete.mode=merge-on-read`
   on v2), equality deletes (`WriteEqualityDeletes` + `RowDelta`) and copy-on-write (`Overwrite` with filter).
   Position deletes are chosen because PyIceberg does not read equality deletes. M3's spike confirmed the catalog
   stages position delete files. Rebuilds and compaction both use `ReplaceFiles` + `Append` (4.3 step 2, 4.9):
   `Overwrite` leaves the position delete files it rewrites in the snapshot.
3. Commit/tag mapping: `mds-<commit>` tags in `R-metadata` plus lakeFS commit metadata. No index file, no
   snapshot-summary scan at read time.
4. Config: the guide's top-level `metadata_search` key list (section 7) for the indexer;
   `iceberg.metadata_search.enabled` for the server (section 5.4). The indexer's object-store credentials are the
   process's `blockstore.*`.
5. Endpoints: `/iceberg/api` and `/iceberg/relative_to` serve the root `system.metadata` tree (including
   `relative_to/system.metadata[.<repo>]/api`); `/mds/iceberg/api` is the deprecated, separate catalog of the old
   identifier (section 5.5).

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
