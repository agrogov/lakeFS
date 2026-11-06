# MDS review 4 — table: `WithRecordBatchSize`, the delete-mode deltas, the Rebuild stream, and the compaction merge's refcounts

## Branch and HEAD reviewed

- Worktree `/Users/arogov/.cline/worktrees/a80d5/lakeFS`, detached at
  `ca46444c9ccc38ea8cdf446f6621add0a3933b43` (`ca46444c9`, "docs: record the third MDS review fixes"),
  clean before this file and clean after it except for this file.
- Spec read first, as the brief requires:
  - `METADATA_SEARCH_DESIGN.md` §3.1 (`:49-107`, the table, the additive-schema rule and the
    delete-only sentence at `:75-82`), §4.3 (`:203-264`, the change set, the rebuild and the
    transaction), §4.4 (`:266-290`, idempotency, crash recovery and the `ErrStaleTable` restart),
    §4.9 (`:370-434`, compaction, its read path, its memory set and the panic rule).
  - `METADATA_SEARCH_PLAN.md` §"Verification rules" (`:130-136`).
  - `MDS_REVIEW_3.md`: `:940` (the refcounts were "balanced by reading") and `:975-995` (the
    "Areas not checked at all" list this card exists to close).
- Pinned sources, all read in `$GOMODCACHE` at the versions in `go.mod`:
  - `github.com/apache/iceberg-go v0.7.0` (`go.mod:73`)
  - `github.com/apache/arrow-go/v18 v18.8.0` (`go.mod:72`)
  - the Go toolchain (`GOTOOLCHAIN=auto` → go1.26.4), for `src/iter/iter.go`, the `iter.Pull2`
    implementation both pull readers are built on.
- Only `pkg/mds/table` and those module-cache sources were read. The indexer, esti, docker,
  SeaweedFS and Spark were not run; see "Review limits".

## Scope and method

The card closes the four areas `MDS_REVIEW_3.md` recorded as unread:

1. `WithRecordBatchSize` (`pkg/mds/table/writer.go:103-106`), on `ApplyDelta` and `Rebuild`.
2. The **delete-mode** paths of `ApplyDelta` beyond the unknown-schema guard: the merge-on-read
   arm (`writer.go:266-282`) and the copy-on-write arm (`writer.go:283-307`).
3. The `Rebuild` **streaming** path: `NewRowsRecordReader`/`rowBatches` (`rows.go:148-213`) and
   `Rebuild` (`writer.go:352-400`).
4. R3T-7's Arrow `Retain`/`Release` balance in the compaction merge, which was previously a
   reading only.

Two kinds of evidence are used below, and every finding names which:

- **Probe** — a temporary test file (`pkg/mds/table/zz_probe_mds4_test.go`,
  `zz_probe_mds4_internal_test.go`, `zz_probe_mds4_panic_test.go`), run under
  `memory.NewCheckedAllocator`, arrow's global `memory.DefaultAllocator` (a package variable,
  `arrow-go/v18@v18.8.0/arrow/memory/default_allocator.go:25`), and `-tags assert`, then deleted.
  The probe outputs are quoted verbatim. `git status` after the run shows no probe file.
- **Code read against the pinned source** — for facts a probe cannot show cheaply, e.g. which
  field of `FileScanTask` the iceberg-go rewrite leaves unset.

No product code, test, design or doc was edited. `Compact` and `Rebuild` behaviour is unchanged.

## Verdict

**0 BLOCKER, 1 MAJOR, 2 MINOR, 3 NIT — 6 findings.** Five are **CONFIRMED** by a probe; the sixth
(R4T-6, an arithmetic/doc drift) is a reading and is marked **UNVERIFIED**.

The four unread areas are now read, and every question the brief asked has a definite answer:

- **R3T-7's refcounts are balanced, and this is now proven rather than read.** A compaction of
  several interleaved files whose output batch is concatenated from two source batches leaves
  `CurrentAlloc() == 0` and passes `AssertSize(t, 0)`; the merge's error path releases every batch
  it retained; the release-before-first-pull path is the existing
  `TestCompactionReaderReleaseFreesStreamsWithoutAPull`. Nothing in the compaction read path leaks.
- **`WithRecordBatchSize` does what its doc says** (`ceil(rows/size)` batches, `<= size` rows each,
  non-positive means the default) and does not change the rows or the file count.
- **The R3T-7 fix on the *streaming* reader of `Rebuild` is not clean**: `rowBatches` releases its
  first record builder twice and never releases the one it allocates last (R4T-2). Under the default
  build this is silent; under arrow's assertion tag it kills the process.
- **The copy-on-write arm of `ApplyDelta` silently resurrects rows that an earlier merge-on-read
  delete removed** (R4T-1). This is the one finding that matters, and its reach is narrower than its
  severity suggests: no caller in this repository passes `WithDeleteMode`, and every table
  `CreateTable` builds carries `write.delete.mode = merge-on-read` (§3.1), so the indexer never takes
  the arm. It becomes every writer's default on a table that does **not** carry the property
  (`writer.go:110` mirrors iceberg-go's `WriteDeleteModeDefault = WriteModeCopyOnWrite`,
  `table/properties.go:102`), and it is the design's sanctioned fallback if position delete files
  cannot be staged (`METADATA_SEARCH_PLAN.md:140-141`, `METADATA_SEARCH_DESIGN.md:833-835`). I kept
  it at MAJOR because it is silent data loss in a documented mode, but it does not block the fragment
  while the fallback is not selected; the owner should either fix it or record that copy-on-write
  deltas are unsupported on a table that carries delete files.

## Summary of findings

| Key | Severity | Status | Confirmation | Title | File:line |
|---|---|---|---|---|---|
| R4T-1 | MAJOR | Fixed in `5d68d07af` | CONFIRMED (probe) | A copy-on-write delta rewrites data files without their position delete files, so rows a merge-on-read delete removed come back | `pkg/mds/table/writer.go:283-295` |
| R4T-2 | MINOR | Fixed in `2b494acc4` | CONFIRMED (probe; the trailing-builder half is a reading) | `rowBatches` releases its first `array.RecordBuilder` twice and never releases the last one it allocates | `pkg/mds/table/rows.go:176-186` |
| R4T-3 | MINOR | Fixed in `5d68d07af` | CONFIRMED (probe) | A delete-only delta is refused with `ErrUnknownSchema` in the copy-on-write mode, which §3.1 says cannot happen | `pkg/mds/table/writer.go:290` |
| R4T-4 | NIT | Fixed in `2b494acc4` | CONFIRMED (probe) | A panic in `Rebuild`'s row source runs on iceberg-go's writer goroutine and kills the process; unlike the merge there is no `ErrMergeRows` conversion | `pkg/mds/table/rows.go:196-208` |
| R4T-5 | NIT | Fixed in `5d68d07af` | CONFIRMED (probe) | `ApplyDelta` writes two rows for a path that appears twice in the upserts, a table its own `Rebuild` refuses | `pkg/mds/table/writer.go:260-261,266-282` |
| R4T-6 | NIT | Fixed in `2b494acc4` (design sentence still to say which figure the package guarantees) | UNVERIFIED (read only) | §4.9 says the compaction's per-file read batch is `max(256, 131072/files)`; the code divides by the scan-task count, so a split file gets a smaller batch than documented | `pkg/mds/table/rows.go:459` vs `:350-360` |

All six findings are fixed; none was left open, so no row says `Not fixed`. The
`Confirmation` column is this review's own evidence for the finding (a probe, or a reading), kept
so the probes quoted below stay attributable; `Status` is the fix. Each fix commit carries the
probe as a permanent test. The behaviour changes they make to the documented design (the
delete-mode default, the copy-on-write fold, `ErrNoSourceCommit`, the row-source panic as
`ErrRowsPanic`, the compaction's per-file batch and its split-range residual) are recorded in
`METADATA_SEARCH_DESIGN.md` §3.1, §4.3, §4.4 and §4.9 and in `docs/metadata-search.md` by the docs
commit that follows them.

## Duplicates and relations

Nothing here is a duplicate of `MDS_REVIEW_3.md` or of each other. The pairs that look similar are
different code and different fixes, and are kept apart:

- **R4T-1 and R4T-3** are both in the copy-on-write arm, but R4T-1 is a wrong *row set* (resurrection,
  caused by what iceberg-go reads) and R4T-3 is a wrong *refusal* (caused by this package calling
  `NewRecordReader` for a row set of length zero). Either can be fixed first, and fixing one does not
  fix the other.
- **R4T-2 is not R3T-7.** R3T-7 was the compaction's *streams* (scans, cursors, a legacy file's
  batches) and was fixed by `newMergeReader`'s `onRelease`; the probe above confirms that fix. R4T-2 is
  the *row source → batch* adapter that `Rebuild` uses; R3T-7's fix does not touch it.
- **R4T-4 is the `Rebuild` twin of R3T-6, not a duplicate.** R3T-6/R3T-8's `ErrMergeRows` conversion
  is `mergedBatches`'s own recover (`rows.go:810-819`), which the design promises for the merge and
  for "the merge's own pull loop" only. `rowBatches` is a second pull loop on the same
  writer goroutine and has no conversion. If the owner wants the promise to cover every write, it is
  one recover in a second place; the design would then need a sentence, not a change.
- **R4T-5 and R4T-6** are independent and are the only two findings outside the four areas'
  main paths: R4T-5 is an inconsistency between the two write paths' input contracts, R4T-6 is a
  docs/arithmetic drift in the number §4.9 quotes.

## Verified

Commands were run with `export PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto` (go1.26.4).
The `go build`, `go vet`, `go test -race` and `golangci-lint` rows, and the baseline, are the
**final** runs, after every probe file was deleted; the probe rows are the temporary runs whose
outputs are quoted under each finding.

| Command | Result |
|---|---|
| `git rev-parse HEAD` | `ca46444c9ccc38ea8cdf446f6621add0a3933b43` |
| `go build ./...` | rc=0 |
| `go vet ./pkg/mds/table/...` | rc=0 |
| `go test -count=1 -race -timeout 10m ./pkg/mds/table/...` | `ok github.com/treeverse/lakefs/pkg/mds/table 16.035s` |
| `go tool golangci-lint run --allow-parallel-runners --timeout 10m ./pkg/mds/table/...` | `0 issues.` |
| `go test -count=1 -timeout 10m ./pkg/mds/table/` (baseline, before the probes) | `ok … 8.505s` |
| **Probe run** `go test -count=1 -timeout 10m -run TestProbe -v ./pkg/mds/table/` (20 probe tests, temporary files) | `ok … 4.563s`, all PASS; every log line quoted below is from this run |
| **Probe run** `go test -count=1 -race -timeout 10m -run TestProbe ./pkg/mds/table/` | `ok … 8.140s`, no race report |
| **Probe run** `go test -count=1 -tags assert -run TestProbeRowBatchesRelease ./pkg/mds/table/` | **panic: too many releases**, exit 1; stack quoted in R4T-2 |
| **Probe run** `go test -count=1 -run TestProbeRebuildSourcePanic ./pkg/mds/table/` | **panic: assignment to entry in nil map**, exit 1, in `iceberg-go` `table/arrow_utils.go:2136`; quoted in R4T-4 |
| `git status --short` after deleting the probes | empty; only `review/mds4/table.md` is added by this commit |

Not run, and why: docker/colima, SeaweedFS, `esti`, the Spark gate and `./esti/...` — nothing in this
card's four areas needs a live environment, and the brief's docker rule applies only to a card that
does. No `MemStats` curve was taken; the checked-allocator curve was, and is reported under
"Checked, no finding".

## MAJOR (1)

### R4T-1 - MAJOR: a copy-on-write delta rewrites data files without their position delete files, so rows a merge-on-read delete removed come back

Status: **CONFIRMED** (probe ran).

**Fixed in `5d68d07af`** - option (a), not (b): when the snapshot still carries live delete files,
`ApplyDelta` runs one `Compact` first (`compactDeleteFiles`), so the file the rewrite reads has no
delete file through it. The fold records the table's current position as its source commit, so a
delta that fails after its fold is discarded by §4.4's recovery and re-indexed. Option (b) -
refusing the mode - was not taken: it would have made the plan's copy-on-write fallback useless on
exactly the tables that need it. A table that carries delete files and **no** `lakefs.mds.source-commit` is
refused with the new `ErrNoSourceCommit`, because the fold has no position to record; `Rebuild`,
which needs none, fixes such a table. The absent-property default stays `copy-on-write` (iceberg-go's
and the documented upstream default), now correct because of the fold; it is pinned by a decision
test, not a regression test. Tests: `TestApplyDeltaCopyOnWriteAfterMergeOnReadDelete` (upsert and
delete-only shapes), `TestApplyDeltaCopyOnWriteDeleteOnlyOnAnEvolvedTable`,
`TestApplyDeltaWithoutTheDeleteModePropertyIsCopyOnWrite`, and `TestCompact`'s copy-on-write block,
which now requires the fold instead of the delete file it used to keep. Upstream
`apache/iceberg-go#2042`, fixed upstream in `apache/iceberg-go#2046` after the pinned v0.7.0.

**Where**

- `pkg/mds/table/writer.go:283-295`: the `case WriteModeCopyOnWrite` arm of `ApplyDelta` builds a
  reader from the upserts and calls `txn.Overwrite(ctx, reader, snapshotProps,
  icebergtable.WithOverwriteFilter(pathFilter(paths, o.deleteChunkSize)))`.
- `pkg/mds/table/writer.go:110`: the delete mode defaults to the table's `write.delete.mode`, and
  when the property is **absent** it defaults to `WriteModeCopyOnWrite` — iceberg-go's own default
  (`github.com/apache/iceberg-go@v0.7.0/table/properties.go:101-102`).
- `github.com/apache/iceberg-go@v0.7.0/table/transaction.go:2370-2383` (`Overwrite` calls
  `performCopyOnWriteDeletion` unconditionally, whatever `write.update.mode`/`write.delete.mode`
  says), `:2466-2478` (classify: whole-file deletes, partial files to rewrite),
  `:2820-2862` (`rewriteFilesWithFilter` → `rewriteSingleFile`), `:2889-2899`
  (`scanTask := &FileScanTask{File: …, Start: 0, Length: …}` — **no `DeleteFiles`**), `:2969`
  (`scanner.GetRecords(ctx, []FileScanTask{*scanTask})`).
- `github.com/apache/iceberg-go@v0.7.0/table/scanner.go:2110-2115`: `FileScanTask` does carry
  `DeleteFiles`, `EqualityDeleteFiles` and `DeletionVectorFiles`; the rewrite leaves all three nil.
- Design/plan claims that make this a defect of the documented behaviour, not an unused corner:
  `METADATA_SEARCH_PLAN.md:140-141` ("Fallback: copy-on-write deltas (`Overwrite` with `In("path", …)`
  filter), which are slower on large tables but use only data files") and
  `METADATA_SEARCH_DESIGN.md:833-835` (decision 2 lists copy-on-write as a write mode of the
  package).

**What happens**

1. A row is deleted with a merge-on-read delta: this package's default, `write.delete.mode =
   merge-on-read` (§3.1), writes a **position delete file** that removes row `z` from data file `F`
   (`F = [w, z]`).
2. A later copy-on-write delta touches a path in `F`. iceberg-go classifies `F` as *partially*
   matching the filter and rewrites it with the rows that do **not** match, reading `F` alone: the
   scan task at `transaction.go:2895-2899` has no `DeleteFiles`, so `z` is read back as a live row.
3. The rewritten file now contains `z` again. The old delete file still points at `F`, whose path
   is no longer in the snapshot, so it can neither remove the resurrected row nor be applied to the
   new file. `z` is live again, with the values it had before it was deleted, and the indexer's next
   delta has no way to know.
4. It is silent: the delta succeeds, the position moves, and a scan returns one more row than the
   data repository has. That is a divergence between `R-metadata` and `R` in exactly the direction
   §3.2's "one row per `(branch, path)` … a removed object deletes it" forbids.

**Evidence (probe output)**

`TestProbeCOWAfterMergeOnReadDelete`: `w` and `z` written at `c1` (one data file), `z` deleted at
`c2` merge-on-read, then a copy-on-write upsert of `w` at `c3`:

```
zz_probe_mds4_test.go:47: after the cow upsert: total-delete-files=1
zz_probe_mds4_test.go:48: rows after the cow upsert: [w z]
```

`TestProbeCOWDeleteOnlyAfterMergeOnReadDelete` — the same on the delete-only shape (the delete of
`w` rewrites the file and keeps `z`, which is dead):

```
zz_probe_mds4_test.go:70: rows after the cow delete: [z]
```

The control probes show that this is specific to a table that already carries delete files, not to
the copy-on-write arm in general: `TestProbeCopyOnWriteUpsertAfterCopyOnWriteDelete`
(`cow then cow: rows=[w] delete-files=0`) and `TestProbeCopyOnWriteFullFileDeleteThenUpsert`
(`rows=[w] delete-files=0 data-files=1`) are both correct, and so is the merge-on-read control
`TestProbeMergeOnReadDeleteOnlyUnknownSchema`. `TestProbeMergeOnReadChunkedDeltaUpsertAndDelete`
(`rows: [b]`) shows the merge-on-read arm applies a delete and an upsert of the same path correctly.

**Suggested fix** (not made here)

- Do not let a copy-on-write delta run on a snapshot that has live delete files unless the rewrite
  applies them. The two honest options are (a) make the arm a compaction first, so the file the
  rewrite reads has no delete file (one `Compact` + `Overwrite`, or `ReplaceFiles` for the touched
  files), or (b) refuse `WithDeleteMode(WriteModeCopyOnWrite)` when
  `total-delete-files > 0`, with a new sentinel, and record in the design/plan that the fallback
  needs a compaction first. Option (b) changes what §13's "fallback" means, so it is the owner's
  call; either way a test has to be added that fails today: the two probes above are it.
- A separate, smaller decision: if the fallback stays, `resolve`'s default for an absent
  `write.delete.mode` (`writer.go:110`) should probably be `merge-on-read`, the mode this package
  writes and tests, not iceberg-go's default. That would also hide R4T-1 from a table whose property
  was dropped, at the cost of diverging from upstream's default.

## MINOR (2)

### R4T-2 - MINOR: `rowBatches` releases its first `array.RecordBuilder` twice and never releases the last one it allocates

Status: **CONFIRMED** (probe ran: the double `Release`); the trailing-builder half is
**CONFIRMED** by reading (it allocates no buffer, so a checked allocator cannot see it).

**Fixed in `2b494acc4`** - the deferred method value became a closure that releases whatever
builder the body holds when it returns, so exactly one builder is released on every exit: drained
source, last batch at the boundary, consumer stopping early, and failing source. The new
`rowBatchesAllocator` seam is what lets a test route the adapter through
`memory.CheckedAllocator`. Test: `TestRowBatchesReleasesTheBuilderItHolds`, which fails before the
fix (under `-tags assert` with "too many releases", which the new recover of R4T-4 turns into a
plain failure instead of a process death).

**Where**

- `pkg/mds/table/rows.go:174-213` (`rowBatches`, the row source → batch adapter of
  `NewRowsRecordReader`, which `Rebuild` uses at `writer.go:379`).
- `:176-177`: `builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)` and
  `defer builder.Release()`. A deferred method call evaluates its receiver **at the defer statement**,
  so this always releases the first builder.
- `:184-186`: `flush` creates the batch, calls `builder.Release()` on the *current* builder, and
  replaces it with a fresh one. For the first flush that current builder is the same object the
  `defer` captured.
- `:196-208`: the row source may also end with an error, in which case the last builder is dropped
  without a release.
- `github.com/apache/arrow-go/v18@v18.8.0/arrow/array/record.go:335-345`: `RecordBuilder.Release`
  asserts `refCount > 0` and decrements; `arrow/internal/debug/assert_on.go:24-28` panics on failure,
  `arrow/internal/debug/assert_off.go:23-26` compiles the assert out (the default build).

**What happens**

- Every `Rebuild` whose source yields at least one row executes `flush` at least once: the last row
  (or the batch boundary) calls `flush`, which releases builder **B0** and allocates **B1**. The
  deferred release then releases **B0 for the second time**.
- In the default build the second `Release` decrements B0's refcount from 0 to −1 without freeing
  anything (its fields were already released), so nothing is corrupted today and no test sees it.
  In an `assert` build — arrow's own debugging mode — it panics, and because the pull body runs on
  iceberg-go's writer goroutine (`table/arrow_utils.go:2131-2141`) the panic is not reachable by any
  recover in this package or in the indexer: the process dies.
- Independently, the builder allocated at `:186` after the last flush is **never released**. With
  `memory.DefaultAllocator` it is a Go object the collector reclaims, so the leak is object
  bookkeeping, not native memory; that is also why a `memory.CheckedAllocator` cannot show it (an
  empty `RecordBuilder` allocates no Arrow buffer). A build using a non-Go allocator would leak it
  permanently.

**Evidence (probe output)**

Isolating the adapter and running it synchronously (no `iter.Pull2`), with arrow's assertions on
(`go test -tags assert -run TestProbeRowBatchesRelease ./pkg/mds/table/`):

```
panic: too many releases [recovered, repanicked]
github.com/apache/arrow-go/v18/arrow/internal/debug.Assert(...)
	…/arrow-go/v18@v18.8.0/arrow/internal/debug/assert_on.go:27
github.com/apache/arrow-go/v18/arrow/array.(*RecordBuilder).Release(0x…)
	…/arrow-go/v18@v18.8.0/arrow/array/record.go:336 +0x104
github.com/treeverse/lakefs/pkg/mds/table.TestProbeRowBatchesRelease.rowBatches.func2(0x…)
	/…/pkg/mds/table/rows.go:212 +0xf0
```

`rows.go:212` is the closing brace of `rowBatches`'s returned closure, i.e. the deferred call
installed at `:177`. The same probe without `-tags assert` passes (`rowBatches yielded 2 batches`),
which is why the suite is green today. The probe file was deleted before this commit.

`-tags assert` cannot be used for the whole package for an unrelated reason: the existing table
tests fail with `len for zstd CompressBound should be > 0` from an upstream assertion in the Parquet
writer when a write hands it an empty buffer; that is why the probe calls `rowBatches` directly.

**Suggested fix** (not made here)

Replace the captured-first-builder defer with a call on the current builder at the end of the body
(e.g. `defer func() { builder.Release() }()` or an explicit `builder.Release()` before the body
returns), so exactly one builder — the current one — is released on every path. A test can be the
probe above, made permanent: run `rowBatches` synchronously under `-tags assert` (or drive it through
`NewRowsRecordReader`), and require no panic. Once `rowBatches` takes an allocator, a
`memory.CheckedAllocator` test can also require `AssertSize(t, 0)` after a drained source — today it
would pass for the wrong reason, because an empty builder allocates nothing.

### R4T-3 - MINOR: a delete-only delta is refused with `ErrUnknownSchema` in the copy-on-write mode, which §3.1 says cannot happen

Status: **CONFIRMED** (probe ran).

**Fixed in `5d68d07af`** - the arm builds `deltaUpsertReader`: the upserts when there are any, and
otherwise an empty reader over the table's own Arrow schema (`NewBatchesRecordReader` with no
batches, so nothing is written and `checkKnownSchema` is never called for a row set of length
zero). The `Overwrite` itself re-emits the columns of the files it rewrites, so the appended column
keeps its value. The §3.1 sentence was not qualified: a delete-only delta applies in both modes.
Tests: `TestApplyDeltaCopyOnWriteDeleteOnlyOnAnEvolvedTable` (the evolved column survives the delete
and a later fold compaction) and the delete-only half of
`TestApplyDeltaCopyOnWriteAfterMergeOnReadDelete`.

**Where**

- `pkg/mds/table/writer.go:283-295`: the copy-on-write arm runs whenever `len(paths) > 0`, and it
  calls `NewRecordReader(tbl.Schema(), rows, …)` at `:290` even when `len(rows) == 0` (a delete-only
  delta). `NewRecordReader` calls `checkKnownSchema` unconditionally (`rows.go:105-111`), so it
  refuses a table with an appended column.
- The guard `if len(upserts) > 0 { checkKnownSchema }` at `writer.go:246-252` deliberately lets a
  delete-only delta through, and the merge-on-read arm never builds a reader for one.
- `METADATA_SEARCH_DESIGN.md:75-82`: "A delete-only delta still applies, because it writes no row".
  The sentence is not qualified by delete mode, and the mode is a per-write option that the package
  documents as selectable (`writer.go:87-95`).

**What happens**

A table with one optional column appended by a later writer (§3.1's additive schema) receives a
delete-only delta. With the default mode it applies (`TestWritesRefuseAColumnTheWriterDoesNotKnow`,
"a delete-only delta still applies" — the probe's control confirms it: `mor delete-only on a table
with an unknown column: err=<nil>`). With `WithDeleteMode(WriteModeCopyOnWrite)` the same delta fails
with `unknown table schema: 11 columns, this writer knows 10`. If the copy-on-write arm is the mode a
deployment runs (R4T-1's fallback, or a table whose property was dropped), every delete fails the
branch permanently, because `ErrUnknownSchema` is not a conflict the indexer retries. The rows a
delete would remove stay live and the position never moves.

**Evidence (probe output)**

```
zz_probe_mds4_test.go:88: cow delete-only on a table with an unknown column: err=unknown table schema: 11 columns, this writer knows 10
zz_probe_mds4_test.go:103: mor delete-only on a table with an unknown column: err=<nil>
```

**Suggested fix** (not made here)

Skip the upsert reader when `len(rows) == 0` (pass an empty reader over the table's Arrow schema to
`Overwrite`, which is what the current code effectively writes anyway), or give `NewRecordReader` a
"delete-only" path that builds no rows and does not call `checkKnownSchema`. The test is the probe
above: a delete-only copy-on-write delta on the evolved table must succeed and remove exactly the
named path while the unknown column keeps its value. If instead the owner wants the §3.1 sentence
qualified, that is a docs change and a different decision — the code's delete-only rule and the
design's would then have to say the same thing.

## NIT (3)

### R4T-4 - NIT: a panic in `Rebuild`'s row source runs on iceberg-go's writer goroutine and kills the process; unlike the merge there is no `ErrMergeRows` conversion

Status: **CONFIRMED** (probe ran: the test binary died).

**Fixed in `2b494acc4`** - the promise is extended rather than documented away: `rowBatches` has
the same recover shape as `mergedBatches` and reports the panic as the reader's error, the new
sentinel `table.ErrRowsPanic` ("the row source panicked") - not a second use of `ErrMergeRows`,
because the failing loop is the row source and a caller logging it should name it - which is what
`Transaction.Append` and therefore `Rebuild` return. Tests:
`TestRowsRecordReaderConvertsAPanicOfTheSource` (a process death before the fix, verified) and
`TestRebuildConvertsAPanicOfTheSource` (a real table, a panicking source, `ErrRowsPanic`).

**Where**

- `pkg/mds/table/rows.go:196-208`: `rowBatches` runs the caller's `Rows` source.
- `pkg/mds/table/writer.go:379` (`NewRowsRecordReader(tbl.Schema(), orderedRows(rows), …)`) and
  `github.com/apache/iceberg-go@v0.7.0/table/arrow_utils.go:2131-2141`: `unpartitionedWrite` starts a
  goroutine and ranges over `array.IterFromReader(rdr)` there (`arrow-go/v18@v18.8.0/arrow/array/record.go:743-758`
  is the iterator), so every `Next()` — and therefore every pull of the caller's source — runs on
  that goroutine. `Transaction.Append` (`transaction.go:883-917`) waits for it, but no frame of this
  package is on that goroutine's stack.
- The contrast is `mergedBatches`'s recover (`pkg/mds/table/rows.go:810-819`), which exists for
  exactly this reason (R3T-6) and is promised by `METADATA_SEARCH_DESIGN.md:419-422` for "the
  merge's own pull loop". `rowBatches` is a second pull loop on the same goroutine and has none.

**What happens**

A caller whose `Rows` closure panics — a nil map, an out-of-range index in a future source, or a
third-party iterator — does not get an error from `Rebuild`: the panic unwinds the writer goroutine,
no recover in the indexer can reach it, and the `lakefs-sso mds run` process exits. The failure is
also not turned into a per-branch `last_error`; the whole indexer stops. Rows already written before
the panic are orphaned (the design's §4.4 orphan-file case), and the next start re-indexes the tick.
No caller in this repository panics today, and the design does not promise otherwise for `Rebuild`,
so this is a NIT and a decision to record: either promise the conversion for every write path (one
recover around the source's yield, or a recover in `rowBatches`), or say in §4.3 that a panic in a
row source is a caller bug that takes the process down.

**Evidence (probe output)**

`TestProbeRebuildSourcePanic` — a source that yields one row and then writes to a nil map:

```
exit=1
panic: assignment to entry in nil map
	/…/pkg/mds/table/rows.go:241 +0x48
github.com/apache/iceberg-go/table.unpartitionedWrite.func1()
	…/iceberg-go@v0.7.0/table/arrow_utils.go:2136 +0xf4
created by github.com/apache/iceberg-go/table.unpartitionedWrite in goroutine 107
	…/iceberg-go@v0.7.0/table/arrow_utils.go:2131 +0xdc
FAIL	github.com/treeverse/lakefs/pkg/mds/table	1.278s
```

**Suggested fix** (not made here)

If the promise is extended, wrap the source iteration in the same `recover` shape `mergedBatches`
uses and return it as the reader's error (it would need its own sentinel or `ErrMergeRows` reused with
a clear message). The probe above becomes the test: `Rebuild` must return an error, not exit. If the
promise is not extended, §4.3 should say so next to the memory paragraph and this finding is closed as
"documented".

### R4T-5 - NIT: `ApplyDelta` writes two rows for a path that appears twice in the upserts, a table its own `Rebuild` refuses

Status: **CONFIRMED** (probe ran).

**Fixed in `5d68d07af`** - refusal, not last-wins: after the existing stable sort, an adjacent equal
path returns `ErrRowsOutOfOrder` before `ensureNoReplay`, mirroring `orderedRows`, so the two write
paths keep one input contract and a refused delta cannot even repair the table first. Test:
`TestApplyDeltaRefusesDuplicateUpserts`.

**Where**

- `pkg/mds/table/writer.go:260-261`: the upserts are cloned and sorted by path, **not** deduplicated;
  `changedPaths` (`:627-638`) deduplicates only the predicate's path list (the union of upserts and
  deletes), and the merge-on-read arm appends every row of `rows` (`:273-279`).
- `pkg/mds/table/writer.go:672-687` (`orderedRows`): the same input through `Rebuild` is refused with
  `ErrRowsOutOfOrder`, because the second row of a path is not strictly after the first.
- `METADATA_SEARCH_DESIGN.md:51,62` (path is the identifier field) and `:108` ("One row per
  `(branch, path)`").

**What happens**

A caller that yields the same path twice — a future change-set builder that merges two paged
answers, a retry that appends instead of replacing — gets a table with two rows for its identifier
path. Nothing in Iceberg enforces identifier uniqueness on append, and nothing in this package
notices: the scan returns both rows, a later `IN (path …)` delete happens to remove both (2 position
deletes for one path, as the probe shows), and `Rebuild` refuses the identical input. The indexer
cannot produce this today (a diff has one entry per path), so the impact is a missing precondition
check on a public function whose sibling refuses the same input, not a live defect.

**Evidence (probe output)**

```
zz_probe_mds4_test.go:486: rows after a duplicate upsert: [a a b]
zz_probe_mds4_test.go:490: rows after deleting the duplicated path: [b]
zz_probe_mds4_test.go:491: delete-files=1 position-deletes=2
```

**Suggested fix** (not made here)

Deduplicate `rows` after sorting (last wins, which is the shallowest change and deterministic with
the stable sort) or refuse duplicates with a sentinel, mirroring `orderedRows`. The probe above is
the failing test either way.

### R4T-6 - NIT: §4.9 says the compaction's per-file read batch is `max(256, 131072/files)`; the code divides by the scan-task count

Status: **UNVERIFIED** (read only). The arithmetic is exact; I did not build a split-file probe to
observe the smaller batch at runtime.

**Fixed in `2b494acc4`** - the code moved to the number rather than the sentence: the plan is
reduced to its distinct data file paths inside `rows.go` (`compactionFileCount` /
`compactionFileBatchRows`, the same path de-duplication `dataFilesOf` uses), so the batch is
`max(256, 131072/files)` whatever `read.split.target-size` does to the task count; `compactReader`'s
signature is unchanged. Residual for the design, recorded in §4.9 by the docs commit: the reader
still opens one run per scan task, so on a table that holds a file above the split target the merge
holds one batch of that size per split range - up to (tasks/files) × the budget - rather than one
batch per file; closing that needs one run per file, a change of the stream layout. Tests:
`TestCompactionFileBatchRowsCountsFiles` (131 072 rows for one file split into two tasks, 65 536 for
two files) and `TestCompactSizesTheReadBatchByFilesOnASplitTable` (a real split table whose merge
must pull 1 000-row batches, not 500).

**Where**

- `pkg/mds/table/rows.go:459`: `batchRows := compactFileBatchRows(len(tasks))`, where `tasks` is the
  scan plan (`Compact` at `writer.go:481-493`). `dataFilesOf` (`writer.go:610-622`) is the function
  that reduces tasks to files, and `Compact` already has its answer at `:488`.
- `pkg/mds/table/rows.go:355-357`: `compactFileBatchRows` is documented "for a snapshot of files data
  files", and §4.9's memory set (`METADATA_SEARCH_DESIGN.md:392-394`) says "each
  `max(256, 131072/files)` rows".
- iceberg-go splits a Parquet file above `read.split.target-size` into several tasks that carry the
  same file (`table/scan_splits.go:55-60`, planned at `table/scanner.go:1820` — the R3T-1 citation),
  so `len(tasks) > len(dataFiles)` exactly on the tables that hold a file above 128 MiB.

**What happens**

On a table with one 200 MiB file, the code computes `max(256, 131072/len(tasks))` where `len(tasks)`
is the number of split ranges, not 1. The merge therefore reads **smaller** batches than the design
budgets — many more round trips through the scan and the writer, and a row set held per file that can
fall to the 256 floor with a single file. It can never exceed the documented bound (tasks ≥ files), so
this is a performance and documentation drift, not a memory-safety defect, and it is the same
split-file shape as R3T-1 with the harmless outcome (a smaller batch instead of a refused file). No failure scenario exists beyond
"the compaction of a table with one large file does `tasks/1` times more read iterations than §4.9
says it will"; a reader sizing a compaction from §4.9's number would be wrong.

**Suggested fix** (not made here)

Pass `len(dataFiles)` (already computed at `writer.go:488` before `compactReader` is called) or
rename the parameter and the §4.9 sentence to say "scan tasks". Since §4.9's bound is a memory figure
and the code is tighter, the cheaper fix is the design sentence; a runtime test would use a split
table (the shape of `TestCompactReplacesASplitFileOnce`) plus `StreamBatchRows` and require the
observed rows to be `max(256, 131072/files)`, which fails today.

## Checked, no finding

Everything below was read and, where a probe is named, run. These are the cells of the "areas not
checked at all" list that turned out clean.

- **`WithRecordBatchSize` (`writer.go:103-129`, `rows.go:105-141`, `writer.go:379`)** — the option is
  resolved once (`resolve`), a non-positive value falls back to `DefaultRecordBatchSize`, and the
  value reaches both readers (`ApplyDelta`'s `NewRecordReader`, `Rebuild`'s `NewRowsRecordReader`).
  The boundary probe (`TestProbeNewRecordReaderBatches`, 7 rows) reports exactly
  `ceil(rows/size)` batches with no batch over the size, for `-1, 0, 1, 2, 3, 7, 8, 65536`:
  `[7] [7] [1 1 1 1 1 1 1] [2 2 2 1] [3 3 1] [7] [7] [7]`. The end-to-end probe
  (`TestProbeRecordBatchSizes`, batch sizes `-1, 0, 1, 2, 3, 7, 64`) writes the same 7 rows and one
  data file for every size. No finding: the option changes batching only, and the writer's file
  rolling is independent of it.
- **The merge-on-read delete paths beyond the unknown-schema guard** — a chunked delta that both
  deletes and upserts the same path keeps the upsert and sends one predicate per chunk
  (`TestProbeMergeOnReadChunkedDeltaUpsertAndDelete`: `rows: [b]`, `snapshots=6`, i.e. the first
  delta's empty delete snapshot and its append, then three one-path chunks and one append); a delete
  of a path the table does not
  hold still moves the position and commits (`merge-on-read: rows=[a] snapshots 2 -> 3,
  source-commit="c2"`); the existing `TestApplyDeltaMergeOnRead`, `TestApplyDeltaChunkedDeletes`,
  `TestApplyDeltaManyDeletesChunksTheINList` and `TestSpikeMergeOnReadDelta` cover the rest. The
  position-delete counts the probe sees (`total-delete-files=1`, `position-deletes=2` for a
  duplicated path) are the ones the design expects. No finding beyond R4T-5's input contract.
- **Snapshot count per upsert delta (an observation, not a finding).** §4.3 step 3 prescribes
  `tx.Delete(In("path", upserts ∪ deletes))` for **every** delta, so a delta with upserts and no
  deletes still issues a `Delete` over the upsert paths (a no-op when the table has none of them)
  and adds an empty snapshot before the append. A temporary read of
  `Table.Metadata().Snapshots()` (deleted with the other probes, re-run while writing this card)
  shows that one `ApplyDelta` of three rows leaves **two** snapshots, `op=delete`
  (`total-position-deletes=0`) and then `op=append`. Nothing breaks:
  `TestApplyDeltaEmptyDelta`'s "an empty delta adds no snapshot" is about a delta with neither
  upserts nor deletes, and the compaction trigger that would care is `compaction.every_commits`,
  which the indexer counts as **indexed commits, not snapshots**
  (`pkg/mds/indexer/maintenance_test.go:71-79`, `TestCompactionEveryCommits`). Recorded so that a
  later reader of the snapshot count (expiry, cleanup) is not surprised that one indexed commit is
  two snapshots.
- **The `Rebuild` streaming path** — a source larger than the record batch streams in several
  batches and writes the declared order (`TestProbeRebuildStreamsSeveralBatches`: `rebuilt 500 rows
  in 14 batches; first=p00000 last=p00499`, one data file); an empty source into an empty table adds
  one snapshot and records the commit (`snapshots 0 -> 1, source-commit="c1", rows=[]`), and a second
  one does it again (`snapshots -> 2`); the pull source's goroutine is gone when `Rebuild` returns,
  on success and on a source error (`baseline=15 after a successful rebuild=15`, `15` after a failed
  one); `Rebuild`'s `ReplaceFiles`-over-every-live-file (§4.3 step 2) is `replaceSnapshotFiles`
  (`writer.go:523-536`) and includes the delete files from the manifest list, which
  `TestRebuild`/`TestRebuildEmpty` pin. The two defects found are R4T-2 and R4T-4; the rest of the
  path matches §4.3 step 2.
- **R3T-7's Arrow `Retain`/`Release` balance in the compaction merge — now proven, not read.**
  - `TestProbeCompactMergeChecked` compacts two interleaved files (the output batch is concatenated
    from two source batches, so the multi-group arm of `buildMergedBatch` and its `Concatenate` both
    run) on `table.CompactionAllocator(memory.NewCheckedAllocator(...))`:
    `compaction rows: [a b e f]`, `after Compact: CurrentAlloc=0 (AssertSize below)`, and
    `AssertSize(t, 0)` passes. The existing `TestCompactReleasesEveryBatchItReads` makes the same
    assertion for three files.
  - The merge's **error** path — `TestProbeMergeErrorPathRefcounts` drives `mergedBatches` with one
    stream that fails after its first row: `after the failed merge: CurrentAlloc=0`, `AssertSize(t,
    0)` passes. Every `Retain` in the pending set is paired with a `Release` in `releasePending`,
    and the output batch built by `buildMergedBatch` is released by the consumer.
  - The **release-before-first-pull** path is `TestCompactionReaderReleaseFreesStreamsWithoutAPull`
    (`rows_test.go:387-406`), which is R3T-7's own test and passes.
  - The concatenation cannot alias: `array.Concatenate` copies (`arrow-go/v18@v18.8.0/arrow/array/concat.go:41-63`,
    `:173-211`, `:634-746`), and the single-group arm returns a `NewSlice` that retains its columns,
    so releasing the pending batches after `buildMergedBatch` cannot invalidate the emitted batch.
- **The rest of the compaction read path against §4.9**: the merge set (`rows.go:342-360`), the
  de-duplicated file list (`dataFilesOf`, R3T-1), the legacy-file detection and materialisation, the
  reader's declared schema (R3T-8) and the `ErrMergeRows` recover (`rows.go:810-819`) match the
  design at `METADATA_SEARCH_DESIGN.md:386-424`. The one number that does not is R4T-6, and it is
  conservative.
- **§4.4's restart claims as they touch these paths**: `ApplyDelta`, `Rebuild` and `Compact` all
  call `ensureNoReplay` before reading anything (`writer.go:246-259`, `:373-378`, `:478-483`), and
  `TestApplyDeltaStaleTable` covers the stale handle. Nothing in the delete-mode or streaming paths
  bypasses it.
- **Things I saw and could not attribute, reported for completeness (not a finding of this card).**
  Replacing arrow's global `memory.DefaultAllocator` with a `memory.CheckedAllocator` and running
  `Rebuild` shows `CurrentAlloc` growing by ~99 KB per call
  (`rebuild 1 (10 rows): CurrentAlloc=198144 (delta since warm-up 99072)`, `+99072` per call). This
  is **not** the streaming reader: draining `NewRowsRecordReader` and releasing it alone returns
  `CurrentAlloc` to `0` on every call (`reader only, call 1..3: CurrentAlloc=0 (delta 0)`), and
  `ApplyDelta` — which does not use `rowBatches` at all — reproduces the same per-call growth
  (`ApplyDelta call 1..3: delta 101696, 244416, 412544`). The growth is therefore in the writer /
  catalog path, most plausibly the in-process fixture's **mem KV** retaining each commit's snapshot
  and manifest metadata (the server side of the test fixture, and it grows with the number of
  commits, not with rows or batches: batch size 1 and 2 show the same per-call delta). It is outside
  this card's four areas and I did not chase it; if it is a real client-side buffer leak it belongs
  to a card that reads the write path's memory with a real KV, which is outside `pkg/mds/table`.

## Review limits

- **Only `pkg/mds/table` was read.** The indexer's use of these functions, the esti cases, the server
  catalog and the Spark gate are other cards' files. The four areas of this card are all inside
  `pkg/mds/table` except the iceberg-go/arrow-go sources they call.
- **Nothing was run against a live environment.** No docker, no SeaweedFS, no `esti`, no Spark. The
  brief's docker paragraph was not needed: every question in this card is answerable in-process, and
  the fixture (`server_test.go`) mounts the real catalog routes over a real HTTP server with a local
  block adapter.
- **`-tags assert` could not be used for the whole package.** The existing table tests fail under it
  with `len for zstd CompressBound should be > 0` from an upstream assertion in the Parquet writer,
  unrelated to these areas. R4T-2's probe therefore calls `rowBatches` directly, where the panic is
  unambiguous.
- **No `MemStats` curve.** The memory question for §4.9 is a `memory.CheckedAllocator` question and
  was answered that way for the merge. A `runtime.MemStats` curve past the writer queue (R3T-5's
  leftover) was not taken; it was not in this card's list.
- **The COW findings are probes of this package's use of iceberg-go, not an audit of iceberg-go.**
  I read `Overwrite`/`performCopyOnWriteDeletion`/`rewriteSingleFile` far enough to name the cause (the
  scan task carries no `DeleteFiles`, `transaction.go:2895-2899`); I did not look for an upstream
  issue or a newer version's behaviour, because the pinned version decides what this branch does.
  No web search was needed for any finding here: every fact is in the pinned module cache or the
  probe output, and the brief's research rule applies to decisions the sources cannot settle.
- **The probe files were temporary and are deleted.** `pkg/mds/table/zz_probe_mds4_test.go`,
  `zz_probe_mds4_internal_test.go` and `zz_probe_mds4_panic_test.go` are not in this commit; the
  outputs quoted above are their complete relevant output, and the final `go test -race`,
  `go vet` and `golangci-lint` runs are on the package **without** them. Nothing else in the tree
  was modified: this commit adds `review/mds4/table.md` only.
- **Not re-reviewed here:** the fixes of `MDS_REVIEW_3.md` other than R3T-7's refcount claim; the
  indexer's handling of the errors these paths return (`ErrUnknownSchema`, `ErrRowsOutOfOrder`,
  `ErrMergeRows`); and the `Compact`/`Rebuild` numbers already verified in that review (batch sizes,
  the merge budget, the Spark gate).
