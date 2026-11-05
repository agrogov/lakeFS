## Metadata Search table writer (`pkg/mds/table`) and ref mapper (`pkg/mds/refmap`)

Scope: design §3.1 (schema, field IDs, properties), §3.2 (row semantics as far as encoding goes), §4.3 steps 3–4
(upsert/delete files, position deletes, chunked `IN`, snapshot properties, optimistic concurrency), §4.9
(compaction rewrite), §4.6 (iceberg-go usage, credentials/IO), §5.x and §6 (virtual table, ref mapping seam).
Files read end to end: `pkg/mds/table/{schema,catalog,rows,writer}.go`, `pkg/mds/refmap/refmap.go`, all
`*_test.go` in both packages, plus the callers the writer's contract depends on
(`pkg/mds/indexer/{iceberg,maintenance,branch,indexer}.go`, `pkg/mds/service.go`,
`pkg/icebergcatalog/{resolver,ident,http}.go`, `pkg/graveler/{errors,ref/resolve_ref}.go`) and the vendored
iceberg-go v0.7.0 sources the writer drives (`table/{table,transaction,arrow_utils}.go`, `catalog/rest/rest.go`).

The audit-fix commits named in `METADATA_SEARCH_PLAN.md` were re-read, not re-reported: field IDs (`39486c0d2`),
bounded 409 restarts and the pre-step-3 reload (`383bf37b5`), tag repair (`aab9216aa`, `30748d488`), stat fallback
(`1cb131f13`). The findings below are on top of those.

### Verified

| Command | Result |
|---|---|
| `go build ./...` (Go 1.25.8, `GOTOOLCHAIN=auto`) | PASS, 14.9 s, no output |
| `go vet ./pkg/mds/table/... ./pkg/mds/refmap/...` | PASS, `vet exit=0` |
| `go test -count=1 -race -timeout 10m ./pkg/mds/table/... ./pkg/mds/refmap/...` | PASS: `ok pkg/mds/table 7.317s`, `ok pkg/mds/refmap 2.748s` |
| `go tool golangci-lint run ./pkg/mds/table/... ./pkg/mds/refmap/...` | PASS, `0 issues.` |
| Probe `TestProbeOrphanDeleteFiles` (temporary, deleted before commit) | delete-file count stays `1`/`2` through rebuild and repeated compaction (see TB-1) |
| Probe `TestProbeOrphansAccumulate` (temporary, deleted) | `del=2` before, after rebuild and after 3 compactions (see TB-1) |
| Probe `TestProbeCompactionSortOrder` (temporary, deleted) | compacted file holds rows `[b d a c]`, sort-order id still `1` = path ASC (see TB-2) |
| Probe `TestProbeConcurrentSameDelta` (temporary, deleted) | a stale handle's commit is refused, not replayed (see "Checked, no finding") |

Probes were run with `go test -count=1 -timeout 8m -run <name> -v ./pkg/mds/table/`; the files were removed and
`git status --short` is clean before this fragment was added.

### TB-1 - MAJOR: position delete files orphaned by a rebuild are never removed, so the compaction trigger can latch on forever

**Where.** `pkg/mds/table/writer.go:291` (`dataFiles, deleteFiles := filesOf(tasks)`), `writer.go:301`
(`txn.ReplaceFiles(ctx, dataFiles, nil, deleteFiles, …)`), `writer.go:322-338` (`filesOf` walks
`task.DeleteFiles` / `task.EqualityDeleteFiles` of the scan tasks); the orphan source is
`pkg/mds/table/writer.go:241` (`Rebuild` → `txn.Overwrite` with no filter, which rewrites the data files and
leaves the delete files in the snapshot — the property `Compact`'s own doc comment states at
`writer.go:258-262`); the trigger that latches is `pkg/mds/indexer/maintenance.go:53-55`
(`deleteFiles := tbl.DeleteFiles(); due := … deleteFiles > x.compaction.MaxDeleteFiles`) reading
`pkg/mds/indexer/iceberg.go:129-139`, i.e. the snapshot summary `total-delete-files`.

**What happens.** Compaction removes a delete file only if some *current* scan task carries it
(`writer.go:327-336`). A position delete file whose data files were replaced by `Rebuild` is carried by no task, so
`ReplaceFiles` never names it, the file stays in the manifest list, and the snapshot summary keeps counting it.
Concretely: a branch has 2 delete files → a large commit (or a branch seeded from an `mds-<A>` tag, §4.5 step 1)
runs `Rebuild`, orphaning both → every later compaction leaves both, so the summary stays `2` no matter how often
it runs. Two consequences:

1. Leak: the orphaned files are never reclaimed. `lakectl iceberg cleanup` cannot reclaim them either — they are
   referenced by the live snapshot. Each further `Rebuild` adds the delete files attached at that moment to the
   permanent set, so the count is monotonically non-decreasing across the branch's life.
2. Latch: the indexer's delete-file trigger compares that summary number, so once the orphan count alone exceeds
   `compaction.max_delete_files` (default 200) `due` is true on every tick and never becomes false: a full table
   rewrite plus a `mds: compact <R>@<C>` lakeFS commit (`maintenance.go:70-78`) every `period` (default 60 s),
   forever. Reaching `orphans > 200` needs one rebuild while more than 200 delete files are attached — reachable
   in one tick, because a tick applies up to `max_commits` (default 1000) commits before `compactIfNeeded` runs,
   and one commit deleting objects spread over 200 data files writes 200 delete files in that commit alone
   (iceberg-go `performMergeOnReadDeletion` → `writePositionDeletesForFiles`, one file per matched data file);
   it is also reachable after one failed/skipped compaction, or by lowering `max_delete_files` under the
   accumulated orphan count.

**Evidence.**
```
$ go test -count=1 -run TestProbeOrphansAccumulate -v ./pkg/mds/table/
    zz_probe3_test.go:33: after two MOR deletes: del=2 data=1
    zz_probe3_test.go:40: after rebuild: del=2 data=1
    zz_probe3_test.go:46: after compact #1: del=2 data=1
    zz_probe3_test.go:46: after compact #2: del=2 data=1
    zz_probe3_test.go:46: after compact #3: del=2 data=1
```
and the single-orphan case (`TestProbeOrphanDeleteFiles`): `after mor delta: del=1 pos=2 data=2`, `after
rebuild: del=1 pos=2 data=1`, `after compact: del=1 pos=2 data=1`, `after second compact: del=1 pos=2 data=1`.
Rows are correct throughout, so this is waste/commit growth, not corruption. `TestCompact`
(`pkg/mds/table/writer_test.go:281-318`) asserts `total-delete-files == 0` but only on a table whose delete files
are still attached (`writer_test.go:304`), which is why the suite is green; `TestRebuild`
(`writer_test.go:228-240`) rebuilds a table that has delete files and asserts only rows and the source commit.

**Suggested fix.** Enumerate delete files to remove from the snapshot's manifest list, not from the scan tasks
(e.g. `tbl.Inspect().AllDeleteFiles(ctx)`, iceberg-go `table/inspect_files.go:72`, or
`GetAllManifests` filtered to `EntryContentDeleteFile`), and let `Rebuild` do the same. The test that fails before
the fix: extend `TestRebuild`'s delete-file case with `table.Compact` and require `total-delete-files == 0`.

### TB-2 - MINOR: compaction writes rows in scan order, so the table's declared sort order (`path ASC`) no longer describes its files

**Where.** `pkg/mds/table/writer.go:287-307` (`Compact` streams the scan's batches straight into `Append`, with no
sort); `pkg/mds/table/schema.go:199-213` (`SortOrder()` = order id 1, field 2, `SortASC`, applied at creation and
kept forever); contrast `writer.go:148` where the delta path does `sort.SliceStable` by path and `rows.go:96-100`
which documents that sorting is the caller's job.

**What happens.** Two data files `[a c]` and `[b d]` (two commits, each sorted) compact into one file whose rows are
`[b d a c]`. The compacted table still advertises sort-order id 1 = `path ASC` (`SortOrder().OrderID()` = 1, one
field), while its data is not sorted. Consequences: the table metadata lies about physical ordering (iceberg spec:
`sort-order` is what writers honour), so any later tool that trusts it (sorted compaction/z-order planning, an
engine's sorted-merge or `write.distribution-mode` decision) gets an unsorted table; and because per-file
`lower_bounds`/`upper_bounds` on `path` become wide, the next `ApplyDelta`'s `IN (path …)` position-delete scan
(`writer.go:153-162`) and any path-range filter prune less and read more files. No reader returns wrong rows.

**Evidence.**
```
$ go test -count=1 -run TestProbeCompactionSortOrder -v ./pkg/mds/table/
    zz_probe3_test.go:69: before compaction data files=2 rows=[a b c d]
    zz_probe3_test.go:87: compacted file rows in written order: [b d a c]
    zz_probe3_test.go:90: sort order id after compaction: 1 (fields 1)
```
(`scanRows` sorts by path, so `TestCompact` at `writer_test.go:281-318` cannot see the order.)

**Suggested fix.** Sort the scanned rows before `Append` (re-batch through `rows.go`'s sorted builder, or sort the
scan's batches by `path` per output file), or, if compaction is meant to be order-preserving only when the input is
ordered, drop/replace the sort order in the same transaction. Test: compact two interleaved files and assert the
written order is `[a b c d]`.

### TB-3 - MINOR: the writer's iceberg-go REST client has no request timeout, and the tick it runs in has no deadline, so one stalled lakeFS call hangs the indexer for the life of the process

**Where.** `pkg/mds/table/catalog.go:136-147` (`NewCatalog` builds `rest.NewCatalog` with headers and props but no
transport/timeout option); caller `pkg/mds/indexer/iceberg.go:94-103` builds one such catalog per `LoadTable`;
`pkg/mds/service.go:186-197` runs the tick and then waits on `<-tickDone` (no `context.WithTimeout`), so the tick
runs on the process context only.

**What happens.** iceberg-go's REST client is `cl := &http.Client{Transport: session}` with no `Timeout`
(`catalog/rest/rest.go:1074`), and the lakeFS API client built at `pkg/mds/indexer/indexer.go:613` is the
generated `apigen` client (also no timeout). A lakeFS that accepts the connection and never answers (deadlock
behind a lock, black-holed LB socket, a proxy that holds the response) blocks the in-flight `LoadTable`,
`txn.Commit` or position-delete scan indefinitely: the tick never finishes, `tickUntilDone` never starts the next
one, and `/_health` still answers 200 because an in-progress tick is defined as healthy (§9,
`pkg/mds/health.go`). Recovery requires restarting the process; nothing retries, logs, or fails the branch.

**Evidence.** Code cites above; `grep -rn Timeout pkg/mds` returns only the health server's `ReadHeaderTimeout` and
`shutdownTimeout` in `service.go:31-35` plus test clients. No probe was written: reproducing needs a hung HTTP
server and adds nothing to the code evidence (the absent option is decisive).

**Suggested fix.** Bound each tick (`ctx, cancel := context.WithTimeout(ctx, cfg.Period)` in
`Service.tickOnce`) and/or give the catalog a transport with `ResponseHeaderTimeout` via
`rest.WithCustomTransport` (`catalog/rest/options.go:164`). A test that fails before the fix: an `httptest` server
that blocks on the first `/v1/config` call and a tick with a short deadline must return an error and stay
restartable.

### TB-4 - NIT: `Compact` accepts `…Option` and discards all of them

**Where.** `pkg/mds/table/writer.go:280` (`if _, err := resolve(tbl, opts); err != nil`) — the resolved
`writeOptions` are thrown away; the body never reads them (`writer.go:269-318`).

**What happens.** `Compact(ctx, tbl, c, table.WithDeleteChunkSize(1))` or `WithDeleteMode(copy-on-write)` is
accepted and silently ignored, while `WithDeleteMode("nonsense")` is an error, so the option set looks honoured
but is not. `NewBatchesRecordReader` also fixes the batch size at the scan's, so `WithRecordBatchSize` cannot work
here either.

**Suggested fix.** Drop the `opts` parameter from `Compact` (only `resolve`'s validation is used), or honour the
options and document which ones compaction ignores. Test: `Compact` with an invalid mode still errors (keep) and
the signature no longer promises the rest.

### TB-5 - NIT: nothing at compile time or in unit tests proves `*catalog.Catalog` still satisfies the ref mapper's `refResolver`

**Where.** `pkg/mds/refmap/refmap.go:47-50` (unexported `refResolver`), `refmap.go:73-78` (runtime type assertion;
failure returns `nil` + one warning), `cmd/lakefs-sso/main.go:93` (the only installation), tests use only the
`fakeCatalog` of `pkg/mds/refmap/refmap_test.go:26-84`.

**What happens.** If `catalog.Catalog.GetCommit`/`GetTag` are ever re-shaped (or a deployment wraps the catalog in
another `BranchLister` implementation — `ServerDeps.Catalog` is `*catalog.Catalog` today, `cmd/lakefs/cmd/hooks.go:39`),
`refmap.New` returns `nil` and every tag and commit read of `R.<ref>.system.object_metadata` silently becomes
`NoSuchTableException`; the only signal is a single `Warn` at startup and the failure surfaces only in
`esti/mds_test.go` (tag and short-commit case), not in `go test ./pkg/mds/...`.

**Suggested fix.** Add `var _ refResolver = (*catalog.Catalog)(nil)` next to the interface, plus one wiring test
that builds the resolver over a real catalog (the `pkg/api`/`pkg/catalog` test fixture already used by
`pkg/mds/table/server_test.go`) and maps a tag of `R` to `mds-<commit>`.

### Checked, no finding

- **Schema and field IDs (§3.1).** `schema.go:148-192` builds the create request in iceberg-go's
  `AssignFreshSchemaIDs` pre-order (map key/value are 8/9, trailing columns 10/11/12);
  `TestCreateTableFieldIDs` (`catalog_test.go:81-133`) pins every ID against a *real* lakeFS catalog over HTTP, not
  a fake, and re-checks that the request and the created table agree; `TestCreateTableSchema` pins types,
  requiredness, `identifier-field-ids = [2]`, format v2, sort order, and the map value's optionality. Readers and the
  writer take IDs/columns from `Table.Schema()` (`rows.go:87-93`), never from the constants. Sort order's source
  ID is validated against the created schema (`catalog_test.go:70-76`).
- **Row encoding (§3.1, §3.2).** `timestamptz` maps to `arrow.FixedWidthTypes.Timestamp_us`
  (iceberg-go `table/arrow_utils.go:705-707`), so `rows.go:273`'s `UnixMicro` is the right unit — the only unit bug
  that would escape the round-trip tests, and it is not present. `user_metadata` is always `Append(true)` with
  sorted keys (`rows.go:291-303`), so an object without user metadata is an empty map, never null, and the same row
  produces the same bytes. Optional columns are `AppendNull` (`rows.go:312-319`). `TestApplyDeltaSourceRepository`
  pins `lakefs.mds.source-repository` to the table property rather than to stale row data.
- **Upserts, delete files and position deletes (§4.3 step 3, §10.2).** The spike
  (`spike_test.go:53-100`) proves against the production catalog routes that `Delete`+`Append`+`SetProperties` in one
  transaction stages position delete files and reads back exactly the expected rows; `ContentType`/metadata and
  copies of the same path are covered. Chunking is bounded at 10 000 paths with an in-test `>10 000` case
  (`writer_test.go:154-176`), and the copy-on-write fallback names the same path set (`writer.go:368-390`,
  `writer_internal_test.go:33-49`); `changedPaths` clips and sorts so a retried commit sends identical predicates.
- **Snapshot properties.** Every `Delete`, `Append`, `Overwrite`, `ReplaceFiles` call passes
  `SnapshotProperties(repository, sourceCommit)` (`writer.go:145,153-194`), each chunked delete included; the
  property-only delta (`writer.go:196-205`) still moves the indexing position while adding no snapshot
  (`writer_test.go:106-125`), which is what §4.3 step 4 relies on.
- **Optimistic concurrency and 409s.** A stale handle is refused with `CommitFailedException: the requirement
  "assert-ref-snapshot-id" is not satisfied` (probe: writer A commits, writer B on a handle loaded at the same
  position is refused; the table keeps one row). `rest.Catalog.CommitTable` maps 409 → `table.ErrCommitFailed`
  (`catalog/rest/rest.go:1626-1633`), which `tableError` turns into `indexer.ErrConflict`
  (`pkg/mds/indexer/iceberg.go:183-190`), which `runBranch` bounds at 3 restarts (`pkg/mds/indexer/branch.go:25-36`).
  I also checked that iceberg-go cannot silently replay the CAS: `doCommit`'s refresh-and-rewrite loop only runs for
  `commit.retry.num-retries > 0` and the default is `0` (`table/table.go:1048-1049`, `table/properties.go:161`), and
  the writer never sets that property; if an admin set it in the table properties the CAS would be rewritten away,
  which is worth keeping in mind but is not a defect of this code.
- **iceberg-go usage and credentials/IO (§4.6).** `CatalogConfig.URI` percent-escapes the one warehouse segment
  (`catalog.go:104-112`) and the server splits `repo.ref` at the first dot, which is unambiguous because repository
  IDs cannot contain a dot (`pkg/icebergcatalog/ident.go:152-174`) — a branch name with `.` or `/` works
  (`catalog_test.go:161-215`). Basic auth is a session default header, so it also covers the bootstrap `GET /v1/config`.
  `maps.Clone(cfg.Storage)` per catalog is required (iceberg-go writes the server config into that map) and is
  pinned by `TestNewCatalogConcurrentSharedStorage`. The static storage properties are *not* leaked into table
  metadata: `rest.Catalog.CreateTable` sends only `catalog.WithProperties` (`catalog/rest/rest.go:1507-1515`) and the
  lakeFS catalog's `/v1/config` overrides carry only `prefix` (`pkg/icebergcatalog/http.go:602`), so
  `s3.secret-access-key` stays client-side; the vended path is proven end to end by
  `vended_test.go:616-701` (fake STS + real object store, ambient credentials cannot make the write pass).
- **Compaction rewrite (§4.9) other than TB-1/TB-2.** `ReplaceFiles(data, nil, delete, …)` + `Append` is the right
  primitive (a plain `Overwrite` would keep the delete files); rows survive, the attached delete files are removed,
  the source commit does not move (`writer_test.go:281-318`), and the transaction is crash-safe in the same way as a
  delta (files are orphans, staging is discarded by §4.4 recovery).
- **Ref mapper seam (§5.1, §6).** The fake in `refmap_test.go` reports the same error *wrapping* real lakeFS uses —
  `ErrCommitNotFound`, `ErrTagNotFound`, `ErrRepositoryNotFound` all wrap `graveler.ErrNotFound`
  (`pkg/graveler/errors.go:15,36,38,42`) — so the not-found branches are exercised honestly. Short commit IDs really do
  resolve through `GetCommit` (`pkg/graveler/ref/resolve_ref.go:128-148`), ref expressions through the modifier walk
  (`resolve_ref.go:66-127`), and `$`-staging refs are rejected as `ErrInvalidRef` (returned as-is, 500, not
  mistaken for "not indexed"). No ancestor fallback: the mapper asks for exactly one tag and the tests assert the
  exact `GetTag` lookups. `noSuchTable` builds the same address shape `isObjectMetadataAddress`
  (`pkg/icebergcatalog/resolver.go:396-406`) serves, and `TestMetadataSearchResolverWiring` pins that a branch needs
  no mapper while a tag does.
- **Authorization around the seam.** The mapped ref is a tag, and writes through the virtual address never reach a
  resolution (`ReservedNamespace`, `resolver.go:369-372`, plus §5.3 authz on `R`), so the mapper cannot be used to
  write `R-metadata`; the mapper's own catalog lookups run with the server identity, which is the documented
  behaviour (§5.2 caveat) and not a new exposure.
- **Unbounded memory in my area.** `Rebuild` and `Compact` stream (`rows.go:141-231`, `writer.go:236-247,296-307`);
  a delta is bounded by `rebuild_threshold` by its caller and its Arrow batch count by `DefaultRecordBatchSize`;
  `pullRecordReader` is coroutine-based (`iter.Pull2`), so `Release` neither leaks a goroutine nor blocks on the
  producer.
- **Races.** `go test -race` clean on both packages; the writer holds no shared mutable state (`NewCatalog` copies
  the caller's `Storage`, `Schema()`/`SortOrder()` return fresh values).
- **Dead code in my area.** Only TB-4 (unused `Compact` options); `pathFilter`'s `AlwaysFalse` branch is reachable
  from tests and from the "rows only" copy-on-write case; no unused exported helper was found.
