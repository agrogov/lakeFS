# Table/refmap and the esti MDS cases (delete-file removal, compaction order, esti)

Review range `e2333be26..HEAD`, area: `pkg/mds/table`, `pkg/mds/refmap`, `esti/mds_test.go`,
`esti/ops/mds-engines`. Review-only card: no product, test, design or doc file is changed by this
fragment.

Commits in the range (documentation-only ones excluded from the fix review, as instructed):

```
e3083e279 mds/table: remove every delete file of the snapshot on rebuild and compaction (TB-1)   <- this area
2daeca114 mds/table, mds/refmap: sort compaction output, drop dead Compact options (TB-2/4/5)   <- this area
7f952279b mds: bound every indexer HTTP request (IX-2, was TB-3)                                <- table half is here
866d4e85c esti/mds: fix TS-6, TS-7, TS-8 and TS-9 of the metadata-search review                 <- this area
d2811fdcc, db8172798, 5b997972e, 6440e2f13, fe1e3bbf5(docs), 4aaf60cb3, 98f035d34, 9cf0869c8,
f884a0627, a397ed971(merge), 224fc82d3(merge), df9744488(docs)                                  <- other reviewers
```

Every finding below was checked against `MDS_REVIEW.md` (TB-1/TB-2/TB-4/TB-5/TS-6..TS-9) and against
the design (`METADATA_SEARCH_DESIGN.md` §3.1, §4.3, §4.9) plus real lakeFS/iceberg-go behaviour.

## Verified

| Command (bounded) | Result |
|---|---|
| `git log --oneline e2333be26..HEAD` and per-commit `git show` | 15 commits: 11 code, 2 docs-only (`fe1e3bbf5`, `df9744488`), 2 merges (`224fc82d3`, `a397ed971`); all 11 code diffs read, the 4 in this area line by line |
| `go build ./...` | pass, exit 0, no output (18.0 s) |
| `go vet ./pkg/mds/table/... ./pkg/mds/refmap/... ./esti/...` | pass, exit 0, no output |
| `go test -count=1 -race -timeout 10m ./pkg/mds/...` | all ok — `mds 2.930s`, `bootstrap 7.159s`, `client 2.018s`, `indexer 11.359s`, `refmap 4.776s`, `table 13.704s` |
| `go test -count=1 -race -timeout 10m ./pkg/mds/table/... ./pkg/mds/refmap/...` | `ok table 9.226s`, `ok refmap 3.906s` |
| `go tool golangci-lint run ./pkg/mds/table/... ./pkg/mds/refmap/... ./esti/...` | `0 issues.` |
| Revert probe TB-1 (`writer.go`: `Rebuild` back to `txn.Overwrite` with no filter, `Compact` back to the scan-task delete files), then restore | `TestRebuild` FAIL *"Should be zero, but was 2"* (`writer_test.go:277`); `TestCompact` FAIL *"Should be zero, but was 2"* (`writer_test.go:384`) — the new assertions really fail without the fix |
| Revert probe TB-2 (`Compact` streams the scan's batches again, `Rebuild` without `orderedRows`), then restore | `TestCompactWritesRowsInPathOrder` FAIL actual `[b d a c]` (expected `[a b c d]`); `TestRebuildRefusesRowsOutOfOrder` FAIL *"Expected error … but got nil"* |
| Probe `TestProbeCompactExtraColumn` (temporary, deleted) | old streaming compaction keeps `extra=keep-me`; new `Compact` **panics** `arrow/array: some fields have excessive number of rows (want at most 0, have 2)`; `ApplyDelta` panics the same way → R2T-1 |
| Probe `TestProbeCompactPeakHeap` / `TestProbeCompactHeapBreakdown` (temporary, deleted) | 200 000 rows: scan-only peak 101 MiB (530 B/row), scan + rows slice 166 MiB (872 B/row), full `Compact` 310–325 MiB (1.45–1.6 KiB/row); 20 000 → 89 MiB, 80 000 → 168 MiB, so the peak is linear in the row count → R2T-2 |
| Probe `TestProbeCompactMultiFileRowsPreserved` (temporary, deleted) | 12 data files, 9 delete files, 1 596 rows: `Compact` then a second `Compact` then a delta leave exactly the same rows, no duplicate path, `total-delete-files == 0` — no row loss in the TB-1 fix |
| Probe `TestProbeCompactEmptyTable` (temporary, deleted) | `Compact` of an empty table still stages a new `metadata.json` (`…00003…` → `…00004…`), so the indexer's `AllowEmpty: false` commit can succeed; no finding |
| esti (`go test -run '^TestMetadataSearch…' ./esti -system-tests`) | **not run**: needs SeaweedFS + docker and this brief forbids docker runs. `go vet ./esti/...` compiles and vets both new cases; R2T-4 comes from reading the harness |
| Probe files (`pkg/mds/table/zz_probe_review_test.go`, `pkg/mds/table/zz_probe_expose.go`) | deleted; `git status` clean apart from this fragment |

## Findings

### R2T-1 - MAJOR: `Compact` panics the whole indexer process on a table whose schema has a column outside the ten `Row` fields

**Where.** `pkg/mds/table/writer.go:357-362` (`rowsOfBatches` → `rowBatches(schema, rowSource(rows), …)`),
`pkg/mds/table/rows.go:161-193` (`rowBatches` → `builder.NewRecordBatch()`), `pkg/mds/table/rows.go:267-281`
(`appendRow` fills fields 0…9 only), decode at `pkg/mds/table/writer.go:578-600` (positional
`batch.Column(0…9)`). The schema contract it breaks is `METADATA_SEARCH_DESIGN.md:51` and
`pkg/mds/table/schema.go:14` — "schema changes only ever add optional fields with new IDs" — and
`pkg/mds/table/schema.go:148-197` already appends `committer`, `content_type`, `creation_date` this
way.

**What happens.** `Compact` no longer streams the scan's batches: it decodes every row into `Row`,
sorts, and re-emits them through `appendRow`, which knows exactly ten fields. On a table that carries
an eleventh (optional) column — the state a later MDS schema version creates, and the state a
rolled-back or mixed-version fleet sees — the record builder appends 0 rows to that column and 2 to
the others, so `array.NewRecordBatch` raises

```
panic: arrow/array: some fields have excessive number of rows (want at most 0, have 2)
```

and the panic is raised **inside the goroutine iceberg-go starts for the write** (`table.unpartitionedWrite`,
`table/arrow_utils.go:2131-2136`, driven by `IterFromReader` → `pullRecordReader.Next`), so no
`recover` in the indexer can contain it: the `lakefs-sso mds run` process exits (the panic is not on
the tick loop's goroutine, so it cannot even be logged), and the indexing of *every* repository that
this indexer follows stops with it. The compaction trigger is still due on the next start (the table
was never rewritten, `pkg/mds/indexer/maintenance.go:53-63`), so a supervisor restart crashes again:
a crash loop, not a one-off failure.

This is a regression of `2daeca114`: the probe runs the pre-fix implementation (the scan's own
batches into `Append`) on the same table and the eleventh column survives with its value.

**Evidence.**
```
$ go test -count=1 -run TestProbeCompactExtraColumn -v ./pkg/mds/table/
    probe: before compaction, extra column = map[a:(null) b:keep-me]
    probe: old streaming compaction extra column = map[a:(null) b:keep-me]
panic: arrow/array: some fields have excessive number of rows (want at most 0, have 2)
goroutine 703 [running]:
iter.Pull2[...].func2() ...
github.com/treeverse/lakefs/pkg/mds/table.(*pullRecordReader).Next(...)
github.com/apache/iceberg-go@v0.7.0/table.(*Transaction).Append.IterFromReader.func1(...)
github.com/apache/iceberg-go@v0.7.0/table.unpartitionedWrite.func1()
created by github.com/apache/iceberg-go@v0.7.0/table.unpartitionedWrite in goroutine 47
$ go test -count=1 -run TestProbeApplyDeltaExtraColumn -v ./pkg/mds/table/
    probe: ApplyDelta panic = "arrow/array: some fields have excessive number of rows (want at most 0, have 1)"
```
The `ApplyDelta` panic pre-dates this range (it is the same `appendRow` limitation); the compaction
panic is new, and it turns a path that worked into a process kill.

**Suggested fix.** Do not rebuild rows field by field: sort by path while keeping the scan's batches
(a bounded merge of the per-file sorted streams and re-emit of the scan's own arrow columns, or copy
the scan's batch schema field-by-field), and make a schema the writer does not know an `error`
(`ErrUnknownSchema`), never a panic. Test that fails before the fix: create a table, `UpdateSchema`
an optional `extra` string column, append one row with `extra = "keep-me"` through iceberg-go, then
`table.Compact` and require the value back (today the test binary dies on the panic).

### R2T-2 - MINOR: compaction collects the whole table in memory; peak heap is ~1.5 KiB per row, and the design's bound is O(table), not a batch

**Where.** `pkg/mds/table/writer.go:357-362` (`rows, err := rowsOfBatches(batches)`, `slices.SortFunc`,
`rowSource`), `pkg/mds/table/writer.go:557-576` (`rowsOfBatches` keeps every row), doc claim at
`METADATA_SEARCH_DESIGN.md:347` ("Peak memory is the table's rows plus one record batch"); contrast
`MDS_REVIEW.md:1413` ("`Rebuild` and `Compact` stream").

**What happens.** To honour the declared `path ASC` order (TB-2) `Compact` now materialises every row
of the table as a `table.Row` (128 B struct plus string contents and maps) and hands them back one
batch at a time — so the live set is the table, and the *whole* compaction path (scan + rows + write)
peaks at ~1.45–1.6 KiB/row. Measured on this schema: 89 MiB at 20 000 rows, 168 MiB at 80 000, 310–325
MiB at 200 000 rows; the breakdown at 200 000 rows is scan 101 MiB / scan + rows 166 MiB / full
compaction 310 MiB, i.e. ~340 B/row is the new rows slice and the rest is the pre-existing scan and
writer buffering. Extrapolated, a 10 M-object branch needs ~14 GiB during a compaction, and
compaction runs by default every 500 indexed commits (`compaction.every_commits`,
`pkg/mds/indexer/maintenance.go:49-63`) for every indexed branch. An OOM there is not self-healing:
the table is never rewritten, so the trigger stays due and the next tick allocates the same amount
again. The pre-fix streaming `Compact` measured ~1.15 KiB/row, so the regression is ~25% — real, but
the doc sentence at `METADATA_SEARCH_DESIGN.md:347` is wrong either way: it promises "rows plus one
record batch", which is O(rows).

**Evidence.**
```
probe: scan only rows=200000 peak=101 MiB (530 B/row)
probe: scan + rows slice rows=200000 peak=166 MiB (872 B/row)
probe: full compaction rows=200000 peak=310 MiB (1626 B/row)
probe: Row size=128 B; rows=20000 … peak=89 MiB; rows=80000 … peak=168 MiB
probe: streaming (pre-fix) rows=200000 … peak=231 MiB over its own baseline
```
(the numbers are `runtime.MemStats.HeapAlloc` sampled every 2 ms during the call, so they include
uncollected garbage; the *slope* per row is the point.)

**Suggested fix.** Keep the declared order with bounded memory: since TB-2 now guarantees every file
the writer produces is an ascending contiguous slice, merge the per-file streams with a bounded heap
(K = number of files) or spill sorted runs to temporary files, and correct §4.9's memory sentence.
Test: compact 100 000 and 400 000 rows and assert the sampled peak grows sub-linearly in the row
count (today it grows by ~4×).

### R2T-3 - MINOR: the rebuild/compaction "non-replayable" guarantee only holds when a delete file was removed

**Where.** `METADATA_SEARCH_DESIGN.md:203-206` (and the `e3083e279` commit message): "The removal is
snapshot-relative, so iceberg-go marks the commit non-replayable: a concurrent writer is refused with
a conflict"; `pkg/mds/table/writer.go:389-403` (`replaceSnapshotFiles`), `pkg/mds/table/writer.go:363-374`
(`Compact`'s optional `ReplaceFiles`); iceberg-go v0.7.0 `table/snapshot_producers.go:1616-1621` sets
`txn.noReplay` **only** when `sp.deletedDeleteFiles`/`deletedDVsByRef` is non-empty, and
`table/table.go:1049` decides replayability from `commit.retry.num-retries` (`table/properties.go:161`,
default 0).

**What happens.** A table with data files but no live delete file (a freshly built index, and the only
state in which `Compact`'s `every_commits` trigger can fire without the delete-file trigger) has its
`ReplaceFiles` removal of *data* files treated by iceberg-go as replayable. With
`commit.retry.num-retries > 0` (a table property an operator can set; the writer never pins it) a CAS
conflict refreshes the parent and replays the staged updates: the rebuild's `Append` (or the
compaction's rewrite) is re-added on top of the *fresh* parent while the concurrent writer's new data
file survives — it was never in the captured removal set — so the same `path` (the identifier field)
appears twice, and a reader sees a duplicate row. The design states the opposite as an unconditional
property of the fix. With the default 0 retries the conflict is terminal and the indexer retries the
tick, so this is latent, not live.

**Evidence.** `sed -n 1616,1621p table/snapshot_producers.go` of iceberg-go v0.7.0:
`if len(sp.deletedDeleteFiles) > 0 || len(sp.deletedDVsByRef) > 0 { sp.txn.noReplay = true }`;
`table/table.go:1049` `numRetries := props.GetUInt64(CommitNumRetriesKey, CommitNumRetriesDefault)`
with `CommitNumRetriesDefault = 0`. Code reading, not a probe: reproducing it needs two writers and a
non-default table property, which the fixture does not set.

**Suggested fix.** Write `commit.retry.num-retries = 0` into `Properties` (`pkg/mds/table/schema.go:215-225`)
so the table itself guarantees "no replay" for every writer, and qualify §4.3 step 2 with "when the
snapshot holds a delete file". Test: create the table, set the property to 1, commit a data file
concurrently with a rebuild, and require either a conflict or no duplicated `path`.

### R2T-4 - MINOR: the new Spark row gate compares only the first `part-*` file and not the guide's snippet

**Where.** `esti/ops/mds-engines/run-spark.sh:60-88` (`INSERT OVERWRITE DIRECTORY` at `:64`,
`part=$(find "$rows_dir" -type f -name 'part-*' | head -n 1 || true)` at `:82`, gate at `:88`),
`esti/ops/mds-engines/common.sh:62-90` (`assert_orders_rows`); the claim it is supposed to fix is
`MDS_REVIEW.md:873-901` (TS-6: "grep -q 'orders/'").

**What happens.** `INSERT OVERWRITE DIRECTORY … ORDER BY path` writes one output file per non-empty
output partition, so `find … | head -n 1` can read a strict subset of what the engine returned. Two
consequences: (a) rows the engine returns in any *other* `part-*` file — a stale snapshot's row, an
extra row of another repository — are never compared, which is the TS-6 failure mode ("some `orders/`
row came back") applied per file; (b) if Spark spreads the three expected rows over several part
files (it does whenever the sorted output is not coalesced to one partition, e.g. a run with
`spark.sql.adaptive.enabled=false` or a non-default `spark.sql.shuffle.partitions`), the script dies
with "returned 1 orders/ rows, expected exactly 3" although the engine was correct — a false failure
against the pinned image's accidental partitioning rather than a property of the endpoint. Separately,
the gated statement is not the guide's snippet: the snippet (four columns including `user_metadata`)
is still only printed into the container log, and the compared query is a rewritten
`concat_ws(path, size_bytes, commit_id)` projection, so "the guide's snippet was verified" is still a
reading, not a check (the Trino side does gate the same SQL).

**Evidence.** `run-spark.sh:64-70` (`INSERT OVERWRITE DIRECTORY '/work/spark-${variant}.rows' USING
text SELECT concat_ws('\t', path, cast(size_bytes as string), commit_id) … ORDER BY path`), `:82`
single-file `head -n 1`, `:88` `assert_orders_rows "$rows_file"`; `common.sh:70-88` compares the one
normalised file with the three expected rows.

**Suggested fix.** Concatenate every `part-*` file (`find "$rows_dir" -name 'part-*' -exec cat {} +`
then normalise) before comparing, and gate the snippet's own output too (e.g. write the four columns
as `concat_ws('\t', path, cast(size_bytes as string), commit_id, to_json(user_metadata))`). Test: a
run with two part files, one carrying the expected rows and one an extra row, must fail today.

## Checked, no finding

- **TB-1's fix is correct for rows.** `Compact` removes every data file of the scan plan and every
  delete file of the snapshot's manifest list, then appends the scanned rows; the appended data files
  get a higher sequence number, so a removed delete file can never apply to them, and a delete file
  that applies to nothing is simply dropped. Probe: 12 data files / 9 delete files / 1 596 rows
  survive `Compact`, a second `Compact` and a following delta exactly, with no duplicate `path` and
  `total-delete-files == 0`; `TestCompact` and `TestRebuild` fail without the fix (revert probe above).
  `ReplaceFiles` re-validates membership against the live snapshot
  (`table/transaction.go:1841-1985,2039-2048`), so a stale file list fails loudly instead of dropping
  a live file, and a `ctx`-driven race is bounded by the CAS (`assert-ref-snapshot-id`).
- **The trigger cannot latch any more.** Both orphan sources named in TB-1 (a rebuild over delete
  files; a copy-on-write rewrite that keeps a position delete file) end with
  `total-delete-files == 0` (`writer_test.go:323-411`), and `Compact`'s delete-file list comes from
  the manifests, not from the scan tasks.
- **TB-2's order fix is sound.** `Compact` sorts by `Row.Path` and batches after sorting, so every
  iceberg-go batch is a contiguous slice of the translated order; `Rebuild` refuses a source that is
  not strictly ascending (`ErrRowsOutOfOrder`) instead of writing unordered files, and its production
  source is `pkg/mds/indexer/changes.go:172-212`, i.e. the paged `listObjects` listing, which is
  byte-ordered and duplicate-free (`pkg/catalog` `Store.List` over ordered keys) — so the new error
  cannot fire on the indexer's own path. The table's `sort-order` id is unchanged by both writers.
- **TB-4 / TB-5.** `Compact` takes no `Option` and no caller passes one (`grep -rn "\.Compact(" pkg/` →
  `pkg/mds/indexer/iceberg.go:171` and tests); the compile assertion
  (`pkg/mds/refmap/refmap.go:47-58`) is load-bearing: removing it makes `New` return `nil` for a
  re-shaped catalog, which is the silent 404 TB-5 is about.
- **`pkg/mds/table/catalog.go` request bound (`7f952279b`).** The transport factory is used for both
  the `/v1/config` exchange and the session client (pinned by `TestNewCatalogRequestTimeout`), the
  deadline is per request and is released by `Close` on the body, and it cannot bind the data path
  (iceberg-go writes through its own `FileIO`); a commit that times out leaves an unknown outcome that
  §4.4 recovery handles. No finding.
- **Esti TS-7 (compaction/tag retention).** Reviewed statically (not run): `waitIndexed` reads
  `/_health`, which the indexer updates only after `compactIfNeeded` returns
  (`pkg/mds/indexer/indexer.go:584`, `pkg/mds/indexer/branch.go:164`), so `log[0]` is the compaction
  commit or the
  case fails; `log[1].Id == tagCommit` would fail if the delete files survived (a second compaction
  commit is inserted) and the `lakefs_mds_compactions_total == 1` assertion cannot pick up another
  branch's compaction (the fixture indexes one branch). Sizes/rows asserted match the four uploads.
- **Esti TS-8 (restart window).** The indexer's lakeFS client posts
  `/api/v1/repositories/{repo}/branches/{branch}/commits` (`pkg/mds/client/lakefs.go:534-550`, base URL
  from `pkg/mds/indexer/indexer.go:715-733`), which is exactly what `isMetadataCommitRequest`
  (`esti/mds_test.go:1248-1255`) matches; the hold is on the POST that follows the Iceberg commit, and
  `stopProcess` is `Process.Kill` (SIGKILL, `esti/mds_test.go:1359-1376`), so the staged pointer
  necessarily survives — the case cannot silently degrade to a between-ticks kill any more. The
  post-restart assertions (staging empty, both missed commits present, exactly one index commit per
  missed commit) hold for the recovery path *and* for the equally correct idempotent re-apply path of
  `applyCommit`, which is fine: both converge with one commit.
- **Esti TS-9 (filtered scans).** The filtered reads run the guide's predicate through all four
  engine/mount recipes and the path set/sizes asserted (`data/gateway.txt` 7 B and `data/one.txt`
  15 B at that point of `TestMetadataSearchEndToEnd`) make the DESC assertion deterministic.
- **Esti cleanup.** Both new top-level cases build their own `mdsEnv` and clean up in `env.stop`
  (kill both processes, then sweep the run prefix and fail the case if the sweep fails); the fixtures
  use unique repository names, and the new tests add no container or process that outlives the test.
- **Errors, leaks, IO.** `rowsOfBatches` releases a batch on its error path and an early `return` from
  a `range`-over-func iterator unwinds the producer (no leaked goroutine); `manifestEntriesOf` closes
  the manifest on both paths; `snapshotFilesOf` skips DELETED entries and de-duplicates by path. The
  only new O(N) state is the rows slice (R2T-2) and the file lists, which are O(number of files) =
  O(rows/65 536) for files this writer produces, so not a second unbounded term.
- **Security.** No authorization surface in this area: the writer is called only by the indexer
  against `R-metadata` and every call reauthorizes there; the esti proxy forwards the admin/indexer
  credentials unchanged and binds to loopback.
- **Dead code.** None introduced: `filesOf` was replaced by `dataFilesOf` + `snapshotFilesOf` (both
  used), `Compact`'s discarded options are gone, `ErrRowsOutOfOrder`/`orderedRows` are used and
  tested. `compactRecordBatchSize` (`writer.go:32`) is production mutable state mutated only by the
  test-only `CompactRecordBatchSize` (`writer_internal_test.go:58-70`); no test runs in parallel, so
  `-race` is clean, but a future parallel test would race on it.
