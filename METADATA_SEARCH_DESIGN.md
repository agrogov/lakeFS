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
| Bootstrap of indexer principal and `R-metadata` deny policy | new `pkg/mds/bootstrap/bootstrap.go`, extra routes builder in `cmd/lakefs-sso/main.go` | New (section 8). |

The indexer is a **REST client** of lakeFS (lakeFS API + the Iceberg REST catalog). It does not import
`pkg/catalog` or `pkg/graveler` and does not touch KV or the block adapter. The only server-side code is the
`RefMapper`, the alias mount and bootstrap.

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
| 7 | `user_metadata` | map<string,string> (key ID 8, value ID 9, value optional) | required | user metadata, empty map when none (never null); the indexer never writes a null value |
| 10 | `committer` | string | optional | committer of `commit_id` |
| 11 | `content_type` | string | optional | content type |
| 12 | `creation_date` | timestamptz (µs) | optional | creation time of the commit `commit_id` (not of the object) |

The map value is optional because iceberg-go v0.7.0 reads parquet map columns with nullable items; a required
value makes its own copy-on-write rewrite and compaction fail with "record schema does not match writer's".

Schema changes are additive - this section allows an optional column a later version appends with a new ID -
and a compaction follows them, but a write that has to build rows does not: a table that already carries a
top-level column this version of the writer does not know is refused with `table.ErrUnknownSchema` by
`ApplyDelta` with upserts and by every `Rebuild`, instead of being written with that column nulled over
another writer's data. A delete-only delta still applies, because it writes no row, and `Compact` re-emits
the scan's own Arrow columns rather than rebuilding rows, so such a column keeps its values through a
compaction (commit `d993d602e`).

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
| Storage namespace | `repositories.<R>.storage_namespace` if set; else `<R namespace without trailing "/">-metadata` **only if** `R`'s namespace has a path component (`s3://bucket/path` → `s3://bucket/path-metadata`). A bucket-root namespace (`s3://bucket`) requires an explicit value. The namespace must **not** be inside `R`'s namespace (nor `R`'s inside it): `R`'s uncommitted GC sweeps every unreferenced object under `R`'s prefix and would delete `R-metadata`'s files. Checked on every tick, not only when the companion repository is created, with the same `"/"`-normalised prefix test the GC uses (`normalizeStorageNamespace`): a companion repository that already exists with an overlapping namespace (created earlier, by hand, by a replica with another namespace, or because `R`'s namespace moved) is refused - that repository is marked failed in `/_health` and metrics - instead of indexed (commit `7ec4ebec7`). |
| Read-only flag | **not** set: catalog writes never pass `graveler.WithForce`, so a read-only repository would also block the indexer. User writes are blocked by a deny policy instead (section 8). The indexer requires the flag to be false: a companion repository lakeFS reports read-only is refused at the repository level - once per tick, with `ErrReadOnlyMetadataRepository`, before any branch is touched (commit `f884a0627`). Branch protection is diagnosed separately, because the lakeFS API does not report protection rules on the repository record: a rule that blocks `COMMIT` or `STAGING_WRITE` on a companion branch is answered at the first write the tick attempts (`Commit`, `ResetBranch` and `HardResetBranch`, and the catalog's table writes) with the same `ErrReadOnlyMetadataRepository` family and a message naming branch protection and the read-only flag, so one repository-level error replaces a bare 403 on every branch while `/_health` stays 503. A rule present before the first tick is refused during the initialisation commit itself, still before any branch is touched (commit `da24d421f`). |
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
idempotency check (4.4), not from a lease; a second replica only wastes work and produces 409s. A second
process that meets the first on a freshly created `R-metadata` does not damage it either: the `mds: init`
commit is made with `allow_empty=false`, so only one of them can commit it, and the loser adopts the winner's
initialisation commit (4.8, commit `db8172798`).

Every HTTP request either production client sends is bounded by `metadata_search.request_timeout` (default
60s, section 7): a connection that neither answers nor resets fails its branch, or its repository when the
call is not attributable to one branch, instead of stalling the tick loop. Without the bound a single wedged
request stopped every repository from being indexed while `/_health` stayed 200 because the tick was merely
"in progress" (commit `7f952279b`).

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
   (paged `listObjects(R, C)`). Else page `diffRefs(R, left=P, right=C, type=two_dot, include_right_stats=true)`,
   no delimiter. `added`/`changed` → upsert, `removed` → delete. `include_right_stats` gives `checksum`, `mtime`,
   `content_type`, `metadata`; `size_bytes` is on the diff. The parameter is marked experimental in the swagger;
   if a needed field is missing, `statObject(R, C, path)` fills it in. "Missing" means the whole right-side stat
   absent (`include_right_stats` not honoured), an empty checksum, a zero mtime or a zero size. `content_type` and
   `metadata` are excluded on purpose: an object may legitimately have neither, so an empty value there is a fact
   about the object, not a gap in the answer. The size is on the diff and the narrowed client reports a size the
   server omitted as zero, which is also the size of an empty object, so a zero-size entry is statted: one extra
   call per empty object, in exchange for never writing a size the server did not confirm (commit `1cb131f13`).
   `committer` and `creation_date` come from `C` (`getCommit`), the same for every row of the commit.
2. **Large change sets.** If upserts + deletes > `rebuild_threshold` (default 100 000), skip the delta and do a
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
   commits `e3083e279` and `d993d602e`). This bounds
   memory and the size of delete predicates. The walk of the diff
   stops at the first entry that takes the count over the threshold (`changeSet.large`): the entries read so far
   are dropped and no `statObject` call is made for that commit, so a commit that is going to be rebuilt costs
   neither the rest of its diff nor a single stat (commit `1cb131f13`). The loss of per-object `commit_id` history
   on a rebuild is documented. The source of a rebuild must be in the table's declared `path ASC` order:
   `Rebuild` refuses a source whose paths are not strictly ascending with the new `table.ErrRowsOutOfOrder`
   instead of writing files that contradict the table metadata (commit `2daeca114`). The production caller is
   unaffected - its source is the paged lakeFS listing, which is lexicographic - and refusing beats sorting the
   branch in memory, which would break the memory bound of this step.
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
5. **Tag** `mds-<C>` → the new commit (`createTag`). lakeFS never overwrites a tag, even with `force`:
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

A history rewrite (4.7) discards the staging before it resets the branch: real lakeFS `ResetHard` keeps the
staging area (`pkg/graveler/graveler.go`), so a pointer staged by a crashed step 3 would otherwise survive the
hard reset and the rewrite would run on top of the stale table (commit `383bf37b5`).

Before step 3 the indexer also reloads the table (so the "already at `C`, skip to step 4" check reads durable
state, not the tick's cached table) and skips to step 4 if the current `lakefs.mds.source-commit` already equals
`C`. A 409 from the Iceberg commit means another writer moved the table: the branch tick restarts up to 3 times
(`conflictRetries` in `pkg/mds/indexer/indexer.go`), re-reading the position and re-running recovery each time;
after the third restart the branch fails for this tick and the next tick resumes at the recorded position
(commit `383bf37b5`).

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
| matching branch deleted | delete `R-metadata@b` if `delete_branches` (default true). Its tags stay, so commit/tag reads of its history still work. Its `/_health` entry and lag gauge are removed whatever `delete_branches` says - only the deletion of the mirror depends on the flag, because a failed last attempt of the deleted branch must not keep `/_health` at 503. `R-metadata`'s **default branch is never deleted**, even when its name is no longer a branch of `R` (an `R` default-branch rename): lakeFS refuses a default-branch delete (`pkg/graveler/graveler.go` `DeleteBranch` returns `ErrDeleteDefaultBranch`), so the indexer skips it and logs the skip once per process. |
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
  at the current snapshot and, in one transaction, removes every data file of the scan plan and every delete file
  of the current snapshot (`Transaction.ReplaceFiles`), then appends the scanned rows, with the same
  `lakefs.mds.source-commit`. A trigger set to `0` is **disabled**, not replaced by the default: the documented
  defaults apply only to an absent key, and a negative value is refused at startup (commit `98f035d34`). A plain
  `Overwrite` is not enough: it rewrites data files but leaves their delete
  files in place, so deletes would keep accumulating. The delete files are enumerated from the snapshot's
  manifest list, not from the scan tasks: a scan task carries a delete file only through the data file it
  applies to, so a delete file whose data files a rebuild (§4.3 step 2) replaced is carried by no task, yet it
  is still referenced by the live snapshot — no file cleanup can reclaim it, and `total-delete-files`, the
  counter of the delete-file trigger, would never fall back to zero and the trigger would latch on forever
  (commit `e3083e279`). The rows it rewrites keep the table's declared `path ASC` order in its files without collecting the table: a
  compaction merges the per-file sorted streams (a k-way merge, one read batch per file plus one batch of
  output), because `iceberg-go` sorts each record batch on its own and the scan plans files in the order the
  commits wrote them - streaming the scan back into `Append` wrote `[b d a c]` for the interleaved files `[a c]`
  and `[b d]`, whereas two such files now compact into `[a b c d]`, whose narrow path lower/upper bounds are
  what the next delta's `IN (path …)` scan prunes with. Memory therefore follows the number of files, not the
  number of rows: the per-file read batch is `compactMergeRowBudget / files` (2 × 64 Ki-row record batches,
  floor 256 rows), which overrides the table's `read.parquet.batch-size` during the compaction. Every file this
  writer produces is an ascending contiguous slice of the declared order; a file written by a compaction before
  commit `2daeca114` need not be, so a first pass reads that file's path column alone, and a file that fails the
  check is read whole, sorted on its own and merged as a materialised run - that one file's memory, not the
  table's. The output is the scan's own Arrow columns, sliced and concatenated, never rows rebuilt from the ten
  `Row` fields, so a top-level column a later schema version appended with a new ID keeps its values through a
  compaction (§3.1). The transaction pins `commit.retry.num-retries = 0` like every other write, so a lost CAS
  is a conflict the indexer retries rather than a replay of the removals on top of a newer snapshot (commits
  `2daeca114` and `d993d602e`). Then it does a lakeFS commit `mds: compact <R>@<C>` (no
  new tag; the tag stays on the pre-compaction commit, which has the same rows).
- **Snapshot expiry and file sweep** are not done by the indexer: run `lakectl iceberg cleanup -r <R>-metadata`
  on a schedule (`cmd/lakectl/cmd/iceberg_cleanup.go`). It honours lakeFS references (branch heads, tags, commits
  retained by GC), so `mds-*` tags keep their snapshots readable.
- **Tag retention.** `tag_retention` (default `0` = keep forever) deletes `mds-*` tags whose source commit is
  older than the duration, then lakeFS GC can reclaim their data. A tag that records the current position (head
  source commit) of any live branch of `R-metadata` is never retired, however old its source commit is: tag
  repair (4.4) recreates a missing tag of an indexed position, so retiring it would make the next tick recreate
  it and the tick after that delete it again (commit `aab9216aa`). A tag whose source commit no longer exists is
  retired because it can no longer resolve.

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

The resolved table is also **proved** before it is served: `MetadataSearchResolver` implements
`ResolvedTableVerifier`, and `Store.Resolve` serves `R-metadata`'s `system.object_metadata` only when **two**
independent facts hold. First, the loaded table's `lakefs.mds.repository` property equals the data repository the
caller named. Second, the served ref's first-parent history carries the indexer's initialisation commit - message
`mds: init` and metadata `lakefs.mds.init=true` (§4.8). The property alone proves nothing: it lives in
`metadata.json`, which anyone who can create `R-metadata` can write, while the initialisation commit is written by
the indexer after it creates the table, so it also commits the table and is the state a forger cannot reproduce by
editing a document. A table that names another repository, carries no property, or whose history has no such
commit answers 404 `NoSuchTableException` for the address the caller asked about: the name `R-metadata` alone does
not prove who wrote the table, and the redirected read runs with the server's identity (§5.2), so a companion
repository someone else created first would otherwise be served to every reader of `R`. Because the check lives
in `Store.Resolve` it also covers `HEAD`, metrics, vended credentials and remote signing, whose scope and location
come from the resolved table. The proof loads the target's `metadata.json` once in `Store.Resolve` and the read
path loads it again; correctness, not read count, motivated the placement (commit `4aaf60cb3`).

The history half costs the length of the served history in the worst case, because the initialisation commit is
the oldest non-root commit: the walk is newest first, one 256-commit page at a time, and stops at the root. The
answer is cached per companion repository and served head commit, so an unchanged branch pays for the walk once
per process and not once per request. A resolver whose `BranchLister` cannot read commit history fails closed -
the property proves nothing without the commit - so the virtual table answers the 404 an unindexed ref answers
(commit `500a00a37`).

### 5.2 Authorization (landed)

`Store.ResolveAuthorized` checks the requested address on `R` (`catalog:ReadTable`, plus `catalog:ReadTableData`
for signing/vending) and `fs:ReadRepository` on `R`. The redirected read of `R-metadata` runs with the server's
identity. Caveat (upstream too): the grant covers metadata of all objects on all refs of `R`. Path-level or
branch-level `fs:` policies do not narrow it. Vended credentials and signing for the virtual table are scoped to
`R-metadata`'s table location (`creds.go`, `sign.go`), and a write session is never vended for the virtual table:
a write that resolves into a reserved namespace becomes a read session (5.3).

### 5.3 Read-only `system` namespace (landed)

On a data repository, writes under `system` answer 403 `ForbiddenException` (`ReservedNamespaceError`).
`GET`/`HEAD` of the namespace is 404 (no marker). Listings show `system` and `object_metadata`. The catalog has no
rename endpoint, so there is nothing to block there. In `R-metadata`, `system` is a normal namespace.

The rule covers the data plane as well as the catalog API. A remote-signature request for a write (`PUT`,
`POST`, `DELETE`, `PATCH` — any method the authorizer classifies as `catalog:WriteTableData`) into a reserved
namespace is refused with 403 `ForbiddenException` (`*ReservedNamespaceError`); read signs (`GET`, `HEAD`) stay
allowed, so the virtual table stays readable. The refusal is checked twice - on the address the caller named and,
after resolution, on the address a table resolver redirects the read to - because a redirect must not turn a
write the caller may make on `R` into a signature or a session for `R-metadata`'s `metadata.json`, manifests or
data files. The signing path refuses a redirected write with 403 (commit `d2811fdcc`). The vended-credentials
path answers a write vend whose resolved target is reserved with `*ReservedNamespaceError`, which
`TableCredentials` treats as a write refusal and turns into the read session, so a redirect from an ordinary
address vends read-only credentials (no `s3:PutObject`); a direct write `Vend` is refused rather than silently
downgraded (commit `500a00a37`; the `pkg/icebergcatalog` comments cite this rule as §5.4).

### 5.4 Config (landed)

`iceberg.metadata_search.enabled` (default false). When it is false, nothing changes. It **requires**
`iceberg.enabled`: Metadata Search is a mode of the catalog - the catalog mount serves the virtual table and owns
the §5.5 alias - so `enabled: false` with `metadata_search.enabled: true` is a startup error,
"metadata_search.enabled requires enabled: Metadata Search is served by the Iceberg catalog, which is off".
Without the check startup succeeded, the §8 principals and deny were installed, and the documented endpoint
answered 501 (commit `4aaf60cb3`).

### 5.5 Upstream endpoint alias (R10-2, new)

Upstream clients point at `<lakefs>/mds/iceberg/api` (warehouse/prefix `lakefs`, OAuth at
`<lakefs>/mds/iceberg/api/v1/oauth/tokens`). When `iceberg.metadata_search.enabled` is true, `RoutesBuilder`
also mounts the same `/iceberg/api` handler at `/mds/iceberg/api`. Same auth, same prefix (`CatalogPrefix = "lakefs"`).
No `relative_to` alias. Upstream configs then work unchanged apart from the host.

While `iceberg.metadata_search.enabled` is false the alias is not mounted, and the prefix is left to the core
router's not-implemented placeholder: `/mds/iceberg/api/…` answers **501** (`http.StatusNotImplemented`), not a
plain 404. The placeholder is `notImplementedMounts` in `pkg/api/services.go`, mounted by `cmd/lakefs/cmd/run.go`
after the extra-routes builders, so a plain `lakefs` binary (no `lakefs-sso` hook) answers the same 501. Only a
wrapper binary that claims the prefix with the real handler replaces it.

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
`iceberg.metadata_search.enabled` and, for the §8 policies, the repository names in `metadata_search.repositories`
(never the rest of the section). Both processes can read the same file.

```yaml
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000       # required; lakeFS base URL (API at /api/v1, catalog at /iceberg)
  access_key_id: AKIA...                       # required; the mds-indexer user's key
  secret_key: ...                              # required
  listen_address: 0.0.0.0:8080                 # /_health, /metrics, /_pprof/
  period: 60s                                  # > 0
  request_timeout: 60s                         # bound on one HTTP request, > 0
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

`request_timeout` bounds every HTTP request the indexer's lakeFS API client and its Iceberg catalog client send,
including the catalog's `GET /v1/config`. It does not cover data-plane file reads and writes, which go through
the object-store client's own timeouts. A request that exceeds the bound fails its branch, or its repository
when the call is not attributable to one branch, and `/_health` answers 503 rather than staying 200 while the
tick hangs (commit `7f952279b`). `0` is rejected: the bound is not disableable.

Startup fails with one error per problem when: the endpoint or keys are empty; `period`,
`request_timeout`, `concurrency`, `max_commits`, `seed_lookback` or `rebuild_threshold` are not positive;
`backfill` is unknown; `repositories`
or a `branches` list is empty; a repository name ends in `-metadata` or is longer than 54 characters; a glob has a
`*` that is not trailing; `since` does not parse; or `tag_retention`, `compaction.max_delete_files` or
`compaction.every_commits` is negative. Per-repository runtime errors (repository missing, namespace
not derivable or overlapping, companion repository read-only, or a branch-protection rule the first write
meets) mark that repository failed in `/_health` and metrics, and the others continue.

`tag_retention` and both `compaction` thresholds treat **`0` as "disabled"**: `0` keeps every `mds-*` tag forever
and turns the corresponding compaction trigger off. The documented defaults (`0s`, 200, 500) apply only when the
key is absent; a negative value is refused at startup instead of being silently replaced by the default (commit
`98f035d34`).

Environment overrides follow lakeFS conventions (`LAKEFS_METADATA_SEARCH_SECRET_KEY`, …).

## 8. Principals, policies, bootstrap

Two principals, as upstream separates indexing from querying:

| Principal | Created by | Permissions |
|---|---|---|
| `mds-indexer` (group `MetadataSearchIndexers`, policy `mds-indexer`) | `pkg/mds` bootstrap, an extra routes builder in lakefs-sso that runs when `iceberg.metadata_search.enabled`. It creates user, group and policy idempotently. **Credentials are created by an admin** (`lakectl auth users credentials create --id mds-indexer`) and placed in the indexer config. | On `*` (data repos): `fs:ReadRepository`, `fs:ListObjects`, `fs:ReadObject`, `fs:ListBranches`, `fs:ReadBranch`, `fs:ListCommits`, `fs:ReadCommit`. On each companion repository `R-metadata` and below it (`arn:lakefs:fs:::repository/<R>-metadata` and `…/*`): `fs:*`; on `arn:lakefs:catalog:::table/<R>-metadata/*`, `…/namespace/<R>-metadata/*` and `…/view/<R>-metadata/*`: `catalog:*`. `fs:CreateRepository` on the companion repository ARN and `fs:AttachStorageNamespace` on `arn:lakefs:fs:::namespace/*-metadata` are the pair `CreateRepository` checks. The namespace stays a pattern because the companion repository of a repository the server knows need not exist yet; an explicit `storage_namespace` that does not itself end in `-metadata` needs an operator-owned policy of its own (for example `mds-indexer-extra-storage-namespace`) attached to `MetadataSearchIndexers`, because the repair rewrites `mds-indexer` to its built-in statements and would delete an added statement at the next startup (commit `26a66b633`). |
| query users | existing users | Through the catalog: only read on `R` (5.2). Engines reading `R-metadata` directly (not through the virtual table) need read on `R-metadata` like any repository. |

The policies name companion repositories explicitly; they do not use a `*-metadata` wildcard for the repository
name (commit `5b997972e`). `pkg/auth/wildcard.Match` lets `*` span `/`, so
`arn:lakefs:fs:::repository/*-metadata` also matched `arn:lakefs:fs:::repository/analytics/object/-metadata`, and
`repository/*-metadata/object/*` matched an object key such as `a/b-metadata/object/c`; the catalog pattern
`catalog:::*/*-metadata/*` likewise matched a data-repository table whose namespace ended in `-metadata`. That
gave `mds-indexer` — which §8 limits to reads on data repositories — write actions on such objects and tables, and
made the deny refuse an ordinary `foo-metadata/data.yml` object inside a data repository to its own guard-group
members. No resource pattern can express "the repository name ends in `-metadata`", so the fix stops using a
wildcard for the repository name.

The set of companion repositories is derived on the server from every repository in the catalog plus the names in
`metadata_search.repositories`, mapped through `R` → `R-metadata`. A repository whose own name already ends in
`-metadata` is a companion only when its data repository `R` exists - in the catalog or in
`metadata_search.repositories` - because the suffix alone does not prove it is one: an ordinary repository named
`team-metadata` that predates Metadata Search stays an ordinary repository, so it is neither granted to
`mds-indexer` (which cannot index it anyway) nor made unwritable for its own users by the deny. The criterion is
the data repository's existence rather than the `lakefs.mds.init` marker, which the bootstrap does not otherwise
read and which would race an indexer that has created the repository but not its commit (commit `26a66b633`).
Both policies spell the same set, so the indexer's grant and the deny cannot drift. Because an exact ARN
is a snapshot, the bootstrap starts a refresher bound to the server's context that re-derives the set every minute
and repairs a policy whose statements changed. A repository created after startup is therefore covered without a
restart; between its creation and the next refresh the indexer fails closed on that repository (an authorization
error in `/_health`, never a silent write) and the deny does not yet cover its companion repository. The indexer
bootstrap requires the server's catalog (`deps.Catalog`, else `ErrMissingDeps`) because that is where the
repository names come from.

Both policies are built as a base policy plus numbered chunk policies. `mds-indexer` and
`mds-metadata-repo-protect` carry only the statements that do not depend on a repository;
`mds-indexer-<n>` and `mds-metadata-repo-protect-<n>` carry at most `companionChunkSize` = 64 companion
repositories each (the first chunk is `<base>-1`), and every chunk is attached to the same group as its base. A
stored policy is one KV item and DynamoDB rejects an item above 400 KiB: an unchunked set crossed that at roughly
560 repositories, `WritePolicy` failed, and the server refused to start on a supported backend.
`MaxPolicyStoredBytes` = 160 KiB is the documented cap on one generated policy (1000 repositories store 34
policies, the largest 67,944 bytes). A set that grows past a chunk boundary gets a new chunk attached, and a chunk
that is no longer needed - a repository was deleted, or a legacy `*-metadata` repository turned out not to be a
companion - is deleted, so a shrinking set shrinks the policy set too. The refresher deletes only names this
package owns (the base name and numeric chunk suffixes), so an operator's policy such as
`mds-indexer-extra-storage-namespace` is never removed (commit `26a66b633`).

**Accepted trade-off (owner decision):** that window stays, and it is bounded by the refresher at one minute. The
policies must name companion repositories exactly - no resource pattern can express "the repository name ends in
`-metadata`" (see below) - and an exact ARN is a snapshot taken when a statement is written, so the set has to be
re-derived while the server runs; a repository created in the last minute is the price of the exact naming that
replaced the over-matching wildcard. During the window the uncovered state is loud and fail-closed rather than
silent: the indexer's own grant does not reach the new companion repository, so its first tick fails with an
authorization error in `/_health` and nothing is written, and the deny does not yet protect the companion
repository, whose first write is therefore possible only for a principal that already holds a broader grant. The
next refresh, or a restart, closes the window without operator action. The rewrite is visible to authorization at
once: after rewriting or deleting a policy the bootstrap purges the auth service's per-user effective-policy cache
(`auth.APIAuthService` caches it for `auth.cache.ttl` and does not purge on `WritePolicy`), so an external-auth
deployment's window is the refresher's one minute and not one minute plus that TTL. A service without a cache is
unaffected (commit `26a66b633`).

Write protection of `R-metadata`: policy `mds-metadata-repo-protect` (Deny `fs:Write*`, `fs:Delete*`,
`fs:Create*`, `fs:Revert*`, `fs:Update*`, `fs:Import*`, `branches:SetBranchProtectionRules`,
`catalog:Create*`, `catalog:Update*`, `catalog:Delete*`, `catalog:WriteTableData`, and
`fs:AttachStorageNamespace` on the exact storage namespace of every companion repository that exists, on the same
exact ARNs as the allow), attached to the caller's guard groups - `cmd/lakefs-sso/main.go`'s `guardGroups()`, which is
`datasets.PinsProtectGroups(datasets.immutable_guard_groups)`: the base groups of both group templates plus the
operator's extras, the same list `iceberg-reserved-prefix-protect` uses - and never to `MetadataSearchIndexers`,
which the bootstrap drops from the list with a warning even when `datasets.immutable_guard_groups` names it
(commit `451bca5a1`). `fs:Update*` covers `fs:UpdateRepository` (`SetRepositoryMetadata` / `DeleteRepositoryMetadata`),
which writes a companion repository and was outside the deny (commit `5b997972e`, review MDS-SEC-5). `fs:Import*`
is the import pair, decided by the owner and added in commit `9cf0869c8`:

- `ImportCancel` authorizes `fs:ImportCancel` on `arn:lakefs:fs:::repository/<R>-metadata/branch/<b>`, which the
  companion subtree statement matches, so a guard-group member with an `fs:*` allow can no longer cancel an import
  on a companion branch.
- `ImportFromStorage` is authorized on the **source** storage namespace (`arn:lakefs:fs:::namespace/<path>`), never
  on a repository ARN, so no namespace statement was added, deliberately. An import whose destination is a
  companion repository was already denied: `ImportStart` also requires `fs:WriteObject` on the destination branch
  and object and `fs:CreateCommit` on the branch, and `fs:Write*`/`fs:Create*` cover both, whatever the source. An
  import whose *source* is a companion repository's namespace writes the destination repository, not the companion
  one: it copies files out, which is what `fs:ReadObject` on the companion repository already allows, and the deny
  deliberately leaves companion reads open. A namespace statement could not be exact anyway - the narrow
  `namespace/*-metadata` pattern would also deny an import from an ordinary data repository whose own namespace
  ends in `-metadata`, the same over-match the exact-ARN change removed - and the server does not read the explicit
  `metadata_search.repositories.<R>.storage_namespace` override (§7 reads only the names), so it could not name
  that namespace before the companion repository exists in the catalog.

Two further write paths into a companion repository are covered explicitly (commit `26a66b633`):

- `branches:SetBranchProtectionRules` is denied on a companion repository, because a rule that blocks `COMMIT` or
  `STAGING_WRITE` on its branches stops the indexer permanently - a write of repository state that decides whether
  the indexer may write, which is the state §3.3 exists to keep diagnosable. `retention:SetGarbageCollectionRules`
  and `retention:PrepareGarbageCollectionCommits` stay **allowed** deliberately: neither can remove the live index
  without deleting the branch first (`fs:DeleteBranch` is denied), and an operator may legitimately set a GC policy
  for a companion repository.
- `fs:AttachStorageNamespace` is denied on the exact `arn:lakefs:fs:::namespace/<namespace>` of every companion
  repository that exists in the catalog. `CreateRepository` checks `fs:CreateRepository` on the repository ARN and
  `fs:AttachStorageNamespace` on the namespace, and lakeFS does not require a namespace to be unique, so without
  this statement a guard-group member could create another repository on a companion's namespace and let its
  uncommitted GC sweep the index files (§3.3). The pattern `namespace/*-metadata` stays in the allow, because a
  companion that does not exist yet has no namespace to name.

As with the reserved-prefix deny, an auth service without policies (`BasicAuthService`) logs a warning naming what
is not enforced, and startup continues.

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
  request that exceeds `request_timeout` fails the branch or the repository, so the probe turns 503 instead of
  staying 200 while the tick hangs (commit `7f952279b`).
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
  when the deny is in force; `system` on `R` rejects writes, and a write signature for `R.<ref>.system.object_metadata`
  is 403 while a read signature on the same address is 200.

## 10. Decisions (former open questions)

1. Indexer runs as the `lakefs-sso mds run` subcommand only. A goroutine mode is out of scope for A.
2. `iceberg-go` v0.7.0 writes merge-on-read position deletes (`Transaction.Delete` with `write.delete.mode=merge-on-read`
   on v2), equality deletes (`WriteEqualityDeletes` + `RowDelta`) and copy-on-write (`Overwrite` with filter).
   Position deletes are chosen because PyIceberg does not read equality deletes. M3's spike confirmed the catalog
   stages position delete files. Rebuilds and compaction both use `ReplaceFiles` + `Append` (4.3 step 2, 4.9):
   `Overwrite` leaves the position delete files it rewrites in the snapshot.
3. Commit/tag mapping: `mds-<commit>` tags in `R-metadata` plus lakeFS commit metadata. No index file, no
   snapshot-summary scan at read time.
4. Config: top-level `metadata_search` for the indexer, `iceberg.metadata_search.enabled` for the server; the server
   also reads the repository names in `metadata_search.repositories` to build the §8 policies.
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
