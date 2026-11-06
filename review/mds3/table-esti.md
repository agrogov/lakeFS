## pkg/mds/table compaction (d993d602e) and the Spark gate (2af2532b7)

Range: the MDS fix commits after `902839f67`, as far as the table area and `esti/ops/mds-engines/run-spark.sh`
are concerned. Findings under test: R2T-1..R2T-3 (`d993d602e`) and R2T-4 (`2af2532b7`) of `MDS_REVIEW_2.md`.
Nothing recorded in `MDS_REVIEW.md` or `MDS_REVIEW_2.md` is re-reported. `table.ErrStaleTable` not being a
§4.4 conflict for the indexer is **R3I-3** in `review/mds3/indexer.md` (`45087bc2e`) and is only cross-referenced here.

**This fragment is partial.** The review was cut short. Nothing below was proven by a probe test, and none of the
revert proofs was run. Every finding is marked CONFIRMED or UNVERIFIED:

- **CONFIRMED** means the code (or a test) was read and is cited on both sides, or a command output is quoted.
- **UNVERIFIED** means the mechanism was read but the consequence was not run. It is a suspicion, not a result.

See "Not completed" at the end for what the brief asked for and this fragment does not contain.

### Verified

The commands ran in a separate worktree (`/Users/arogov/.cline/worktrees/7f18b/lakeFS`, detached at `0dbf5db98`,
clean tree, `PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto`, which selects go1.26.4). That worktree's code is
the same as this one's: `git diff --stat 0dbf5db98 HEAD` here lists only `review/mds3/indexer.md`.

| Command | Result |
|---|---|
| `go build ./...` | exit 0, no output |
| `go vet ./pkg/mds/table/...` | exit 0 |
| `go test -count=1 -race -timeout 10m ./pkg/mds/table/...` | `ok  … 15.109s` |
| `go tool golangci-lint run --timeout 10m ./pkg/mds/table/...` | `0 issues.` |
| `./esti/ops/mds-engines/run-spark.sh --self-test` | exit 0; 7 `PASS:` lines, last line `self-test passed`; `git status` clean afterwards |
| `grep -rn "ErrMergeRows\|compactFileBatchRows" pkg/mds --include=*_test.go` | no match in any test file |
| `grep -n "CheckedAllocator\|AssertSize\|WithAllocator" pkg/mds/table/*.go` | no match |

Not run: any probe, any revert proof, `run-spark.sh` against a live environment (no docker), `./esti/...`.

### R3T-1 - MAJOR: Compact hands one data file to `ReplaceFiles` once per split scan task, which iceberg-go v0.7.0 rejects

Status: **UNVERIFIED** end to end (not run). The mechanism is read on both sides and cited below.

**Where**

- `pkg/mds/table/writer.go:474-476`: `scan.PlanFiles` then `dataFilesOf(tasks)`.
- `pkg/mds/table/writer.go:586-593`: `dataFilesOf` appends `task.File` for every task. Its comment says "a scan plans one task per live data file of the snapshot".
- `pkg/mds/table/writer.go:489`: `txn.ReplaceFiles(ctx, dataFiles, nil, deleteFiles, snapshotProps)`.
- iceberg-go v0.7.0, `table/scanner.go:1792` and `:1820`: local planning calls `splitParquetScanTask(task, splitTargetSize)` for every planned file.
- iceberg-go v0.7.0, `table/scan_splits.go:32-90`: a Parquet file larger than `read.split.target-size` (128 MiB default, `table/properties.go:40-41`) with at least two split offsets is returned as several tasks. Each task copies the same `File` and has its own `Start` and `Length`.
- iceberg-go v0.7.0, `table/table.go:1350`: `Table.Scan` plans locally, so `ScanPlanningLocal` is what `tbl.Scan()` uses.
- iceberg-go v0.7.0, `table/transaction.go:1682` (`ReplaceDataFilesWithDataFiles`) returns "delete data file paths must be unique for ReplaceDataFilesWithDataFiles". `replaceFiles` (starts `:1794`) has the same check in its `setToDelete` loop ("delete data file paths must be unique for ReplaceFiles"). `ReplaceFiles` reaches one of the two whichever way `deleteFiles` falls.

**What happens**

Once one data file of the snapshot is above 128 MiB and has at least two row groups, `PlanFiles` returns two or more tasks for it.

- `dataFilesOf` then lists the file twice and `ReplaceFiles` returns the duplicate-path error.
- `Compact` returns "replace the files of the table at …".
- The compaction trigger stays due (`pkg/mds/indexer/maintenance.go:49-90`), and the indexer fails the branch on it, so the same error repeats on every tick.

A compaction output can itself be such a file. The rolling writer rolls files at 512 MiB (`write.target-file-size-bytes`) and row groups at 128 MiB or 1 048 576 rows. So the first compaction of a large table can produce a file that no later compaction can replace. `Rebuild` is not affected: `snapshotFilesOf` reads the manifests and de-duplicates by path (`writer.go:533-563`).

The `d993d602e` read path also treats each split task as its own file. `openCompactionStreams` opens one run per task and `compactFileBatchRows(len(tasks))` divides the budget by tasks (`rows.go:349`, `:428-475`), so the split ranges are read as separate sorted streams. That part is harmless. Only the removal list is wrong.

Not run: whether a table can reach this state in a reasonable time, and the exact error text at runtime.

**Evidence**

Code only, cited above. `dataFilesOf` was introduced by `e3083e279`, and no test builds a file above the split target. `TestCompact*` in `writer_test.go` use files of a few rows.

**Suggested fix**

De-duplicate by `File.FilePath()` in `dataFilesOf` (keep the first). Keep the tasks as they are for the read side; each split range is a disjoint set of row groups.

A test that fails before the fix: create the table with `read.split.target-size=1` and `write.parquet.row-group-limit=2`, apply a delta of at least 5 rows so the file has several row groups, assert `PlanFiles` returns more tasks than files, then `Compact`. Before the fix it fails with the duplicate-path error; after it, it must keep every row once.

### R3T-2 - MINOR: the Spark self-test's two `user_metadata` cases fail on the row count, so they do not show that `user_metadata` is compared

Status: **CONFIRMED** by reading and arithmetic. The mutation that would show it (drop the fourth column from the comparison) was not run.

**Where**

- `esti/ops/mds-engines/run-spark.sh:161`: `bad` is `three` with `{"team":"analytics"}`.
- `esti/ops/mds-engines/run-spark.sh:209-229`: the two cases "a user_metadata in part-00001 is compared" and "… in part-00000 is compared".
- `esti/ops/mds-engines/common.sh:98`: `assert_orders_rows` dies when the normalised row count is not exactly 3.
- `esti/ops/mds-engines/common.sh:103`: the `diff -u` against the expected rows comes after it.

**What happens**

Each of those two cases writes `one, two, three` into one part file and `bad, two, one` into the other. That is six lines. `common.sh:98` rejects it as "returned 6 orders/ rows, expected exactly 3" before `diff` runs. The case would still pass if the gate ignored the fourth column completely.

The self-test has no case with exactly three rows in total where only a `user_metadata` value is wrong. The only self-test case that exercises `{}` at all is the passing one. The commit message of `2af2532b7` presents these two cases as the proof that "a wrong user_metadata … is compared". The production gate does compare the column (`write_expected_orders_rows` with `columns=4` and `diff -u`); the test just does not prove it.

**Also**

`run-spark.sh:96-109` writes two statements. The first is the guide's `SELECT path, size_bytes, commit_id, user_metadata …`; its output goes to the container log and is never parsed. The gate asserts the second statement (`INSERT OVERWRITE DIRECTORY … concat_ws(…, to_json(user_metadata))`). That is the same table, columns and filter today. The two cannot drift apart without something noticing only if a human reads the log.

The self-test proves the gate logic on fabricated files. It does not prove Spark ever writes more than one part file. The commit message already says the live run wrote one.

**Suggested fix**

Add self-test cases of exactly three rows in total that must fail through the `diff`:

- one part file holding `one`, `two`, `bad`;
- `one`, `two` in part-00000 and `bad` in part-00001, and the mirror image.

Check that each fails with "rows differ", not "wrong number". Optionally make `expect` assert which message was produced.

### R3T-3 - MINOR: a table of legacy files is materialised all at once, not "one file's memory"

Status: **CONFIRMED** by reading. Not measured.

**Where**

- `pkg/mds/table/rows.go:428-475` (`openCompactionStreams`): the loop opens a run for every task before the merge starts.
- `pkg/mds/table/rows.go:603-660` (`newMaterializedRun`, `materializedRun.release`): the whole file's batches are appended to `run.batches` and released only in `release`, which `mergedBatches` calls at the very end (`:757-764`).
- `METADATA_SEARCH_DESIGN.md:387-391` and `rows.go` doc on `compactReader`: "that one file's memory, not the table's".

**What happens**

The pre-`2daeca114` compaction streamed batches into `Append`, so each file it wrote is sorted inside a batch and not across the file. After such a compaction every file of the table fails `taskRowsInPathOrder`. The next compaction then reads each of those files whole, at open time, and keeps all of them until the merge finishes, so the memory is the table's, which is R2T-2's own defect for that one compaction.

`materializedRun.held()` is `len(rows)-next` (`:655`). It shrinks as rows are consumed, but the batches are not freed, so `mergeRowsHeld` under-reports it too.

This applies only to a table last compacted by the old code. The feature is not released, so the impact is on development and test deployments. The doc statement is wrong as written.

**Suggested fix**

Correct the doc to "the memory of every legacy file at once", or materialise legacy files one at a time. For example, sort each legacy file to a temporary sorted run (re-written through the same batch path) before the merge. A test: write two legacy files (as `TestCompactSortsAFileWrittenBeforeTheFix` does) and observe the held rows with `MergeRowsHeld` before the first output batch.

### R3T-4 - MINOR: the memory tests do not exercise the budget, the floor or more than one file

Status: **CONFIRMED** by reading and arithmetic. The numbers 256 and 264 are derived, not observed.

**Where**

- `pkg/mds/table/writer_test.go:851-899` (`TestCompactHoldsABoundedNumberOfRows`): `mergeBudget = 200` (`:853`), one data file (`require.Len(t, tasks, 1, …)`), and the assertion `large <= mergeBudget+outputBatch` (`:898`).
- `pkg/mds/table/rows.go:342-354`: `compactFileBatchRows(files) = max(256, budget/files)`.
- `pkg/mds/table/rows_test.go`: `TestMergeHoldsABoundedNumberOfRows` uses two synthetic streams, `batchSize = 100`, `output = 64`.

**What happens**

- With one file and a budget of 200, the per-file batch is `max(256, 200) = 256`, not 200. The assertion is `<= 264`, and the real bound is the 256-row batch plus the 64 pending rows, so it holds with a few rows of slack, not by the budget.
- Nothing runs more than one real file, so the division `budget/files` is never exercised, and nothing runs more than 512 files, so the point where the floor overrides the budget is never reached.
- `compactFileBatchRows` has no test (grep above).
- The observer (`mergeRowsHeld`, `rows.go:784`) counts `pending` plus each stream's unread rows only (see R3T-5), so neither test can see the memory outside the merge.
- A "revert" of the fix cannot run `TestCompactHoldsABoundedNumberOfRows` against the old code, since `MergeRowsHeld` is part of the new code. The test is therefore a regression guard for the new structure, not a test that failed before the fix.

**Also**

`openCompactionStreams` opens a scan, a cursor and (for an in-order file) a worker pipeline for every task at once. The floor means the live rows are `files × 256` once there are more than 512 files, and §4.9's compaction runs after a whole tick of commits (`branch.go:164-165`), so the file count is not bounded by `every_commits`. Open handles and goroutines per file were not measured: UNVERIFIED.

**Suggested fix**

Add a unit test of `compactFileBatchRows` (1, 2, 512, 513, 10 000 files, budget 131 072) and a test with several real files and a budget that makes `budget/files` differ from the floor. Assert the batch rows seen by a stream instead of the sum.

### R3T-5 - MINOR: the documented memory bound counts the merge's own rows, not the scan pipeline or the writer queue

Status: **UNVERIFIED.** The mechanism is read in iceberg-go v0.7.0; no memory was measured beyond what `d993d602e`'s own commit message reports.

**Where**

- `METADATA_SEARCH_DESIGN.md:381-387` ("one read batch per file plus one batch of output").
- iceberg-go v0.7.0:
  - `table/arrow_scanner.go:2472-2478`: the scan's `records` channel has capacity `numWorkers`. `internal/utils.go:85-110` (`MakeSequencedChanWithDiscard`) adds an output channel of the same size. So a stream can have more than one batch alive beyond the one the merge holds.
  - `table/rolling_data_writer.go:51` and `:366-370`: the writer's `recordCh` holds 64 batches by default, and `Add` (`:423`) retains each one until it is written.
  - `table/arrow_utils.go:2126`: `unpartitionedWrite` pulls the merge from its own goroutine, so the merge can run ahead of the Parquet encoder by that queue.
  - `table/internal/parquet_files.go:838-845`: the writer buffers a row group (128 MiB or 1 048 576 rows by default) before it rotates.
- `pkg/mds/table/rows.go:829-830` and `:904`: an output batch of one source batch is a zero-copy slice, so a queued output batch keeps its whole source batch alive.

**What happens**

The merge's own live set is bounded as documented. What the process holds while compacting is the merge's set plus the scan pipeline's prefetch per file, up to 64 queued output batches of `compactRecordBatchSize` (64 Ki) rows and their source batches, and one buffered row group. All of that is bounded and independent of the table size, but it is a large constant, and it is not what §4.9, the tests or the comment claim.

The author's own numbers support the doubt: the commit message of `d993d602e` reports peaks of 50 MiB for 50 000 rows and 128 MiB for 200 000 rows after the fix ("2.56x for 4x rows") and says it "saturates at the 131 072-row read batch", but 200 000 rows is far below the 4.2 M rows the writer queue can hold, so that probe cannot show saturation.

**Suggested fix**

Either state the bound as "merge set + scan prefetch + writer queue + one row group" in §4.9, or bound it: pass `icebergtable.WithRecordBatchBufferSize` (iceberg-go `write_records.go:105`) if the `Append` path allows it. A measurement that would settle it: a `runtime.MemStats` probe over 0.2 M, 2 M and 8 M rows.

### R3T-6 - MINOR: `ErrMergeRows` has no test, and the recover only covers the merge's pull loop

Status: **CONFIRMED** that there is no test (grep above). The scope sub-claim is **UNVERIFIED**.

**Where**

- `pkg/mds/table/rows.go:767-770`: `recover()` in `mergedBatches` turns a panic into `yield(nil, fmt.Errorf("%w: %v", ErrMergeRows, recovered))`.
- `pkg/mds/table/rows.go:481-535` (`taskRowsInPathOrder`) and `:603-655` (`newMaterializedRun`): both iterate a scan from `compactReader` (`:418-426`), before `mergedBatches` runs, so they are outside that recover.

**What happens**

`d993d602e` says "a merge panic becomes `ErrMergeRows`", and §3.1 and the commit message rely on it, but no test in `pkg/mds` names `ErrMergeRows` or injects a panic into a stream. The sibling fragment exercised it only through an injected error at the indexer's `Table` fake, which does not enter `rows.go`. The recover has the right shape (the body runs inside iter.Pull2's coroutine, and the deferred `releasePending` runs before it), but nothing demonstrates it, including that `yield` can be called from a deferred function and that the next `Next` returns the error.

The setup reads (path detection, materialisation) run in the caller's goroutine. A panic there is not converted. Whether that is a problem depends on whether the caller recovers; that was not checked.

**Suggested fix**

Add a `rowStream` test double whose `advance` panics. Run `newPullRecordReader(…, mergedBatches(…))` and require `Next()` false and `errors.Is(reader.Err(), ErrMergeRows)`. It fails before the recover is added.

### R3T-7 - NIT: streams opened by `compactReader` are released only by the merge's own `defer`, which does not run if the reader is released before its first pull; and no test checks Arrow refcounts

Status: **CONFIRMED** by reading. Harmless under the Go allocator in production; not probed.

**Where**

- `pkg/mds/table/writer.go:481-485` (`compactReader`, then `defer reader.Release()`), `:489-496` (`ReplaceFiles` and `Append` can fail before any batch is pulled).
- `pkg/mds/table/rows.go:755-766`: the stream release is a `defer` inside the iterator body.
- go1.26.4 `src/iter/iter.go:376-445`: `Pull2`'s coroutine starts with `if pull.done { return }`, and `stop` sets `done` first. So `stop()` before the first `next()` never runs the iterator body.
- `pkg/mds/table/rows.go:904`: `array.Concatenate(parts, memory.DefaultAllocator)`; also `:109-141` and `:152`.

**What happens**

If `ReplaceFiles` or `Append` fails before the first pull, `reader.Release()` calls `stop()`, the merge body never runs, and `release()` of the opened streams (scans, cursors, and for a legacy file its batches) is never called. The doc comment on `rowStream.release` ("frees the stream: its file, its scan, its batches and its goroutine") is not true on that path. Under `memory.DefaultAllocator` the garbage collector reclaims everything, so no leak follows. A `memory.CheckedAllocator` would report it.

The code uses `memory.DefaultAllocator` directly, so no test can inject a checked allocator for `buildMergedBatch`. By reading, the Retain and Release in the merge are balanced: one `Retain` per pending row (`:829`) and one `Release` per pending row in `releasePending`; the concatenated columns are released after `NewRecordBatch`; the error path releases `columns[:c]` and `parts`. That is a reading, not a test.

**Suggested fix**

Release the streams in `compactReader`'s reader (for example from `pullRecordReader.Release`) instead of from the iterator body, or open the streams lazily inside the body. For a test, route the allocator through a package variable, use `memory.NewCheckedAllocator`, and `AssertSize(t, 0)` after (a) a full compaction and (b) a compaction whose `Append` is made to fail before the first pull.

### R3T-8 - NIT: the schema of the compaction reader is `nil` for a streamed run, so the "no task" branch is the common path and the declared schema differs from the yielded batches

Status: **UNVERIFIED** consequence (the suite is green). The nil schema is **CONFIRMED** by reading.

**Where**

- `pkg/mds/table/rows.go:537-557`: `newStreamedRun` does not store the schema `ReadTasks` returned.
- `pkg/mds/table/rows.go:559` and `:723`: `streamedRun.schema()` returns `r.batch.schema`, which `recordCursor.advance` sets only when it reads the first batch.
- `pkg/mds/table/rows.go:454-475`: `schema = stream.schema()` runs before any `advance`; the "No task, so no row" branch then computes `ArrowSchema(projection)`.

**What happens**

For a table whose last file is in order (the normal case), `schema` is `nil` at `:456`. The reader therefore declares `ArrowSchema(tbl.Scan().Projection())` (with field IDs, `rows.go:91`), while the single-run batches it yields are slices of the scan's batches, whose schema has no field IDs; only multi-run batches use the declared schema (`buildMergedBatch`, `:877-915`). When the last file is a legacy one, the schema is the scan's own. The comment at `:457` describes only the empty-table case. All current tests pass, so iceberg-go copes with both, but the declared schema depends on which kind of run happens to be last.

**Suggested fix**

Store `schema` in the cursor at open (as `numberedStream` does in `rows_test.go`). Add an assertion in `TestCompactKeepsAColumnTheWriterDoesNotKnow` that the reader's schema equals the schema of the batches it yields.

### Checked, no finding

By reading unless a command is named. Nothing below was run beyond the Verified table.

- **k-way merge order, ties and duplicate paths:** `streamHeap.Less` (`rows.go:855-862`) orders by path and then by file position, so a tie is deterministic. `mergedBatches` rejects only a strictly smaller path (`:818-826`), so equal paths pass and every row is kept (no de-duplication, as documented in `taskRowsInPathOrder`).
- **Empty files and batches:** `recordCursor.advance` (`rows.go:694-726`) skips a nil batch and releases and skips an exhausted or empty one. With no task, `Compact` writes nothing (`writer.go:493`) and the reader still has a schema (`rows.go:456`). An empty-row file simply yields no run row.
- **Per-file batch size and `WithArrowBatchSize`:** the option is stored on the scan (`table.go:1330` in iceberg-go v0.7.0), forwarded at `scanner.go:2247`, and overrides `read.parquet.batch-size` at `arrow_scanner.go:2584-2593`. The three scans of a file (path detection, streamed run, materialised run) all pass `batchRows` (`rows.go:481`, `:537`, `:603`).
- **Unknown-column preservation vs `ErrUnknownSchema`:** `buildMergedBatch` never addresses a column by position, and `pathColumn` finds `path` by name (`rows.go:877-915`, `:925`). `checkKnownSchema` guards `ApplyDelta` only when there are upserts (`writer.go:250`), `Rebuild` (`writer.go:370`) and the two record readers (`rows.go:109`, `:152`); `Compact` does not call it. It compares count, name and ID by position (`schema.go:186-199`), so a reordered table is refused conservatively. It does not compare types; I found no legal Iceberg promotion for the ten columns' types, so no consequence was found.
- **`commit.retry.num-retries=0`:** `ensureNoReplay` (`writer.go:171-193`) reads the property with the same `GetUInt64` default that iceberg-go's `readRetryConfig` uses (`table.go:1048`), repairs with one metadata-only commit, and returns `ErrStaleTable` only when the repair moved the snapshot. `schema_test.go:184` asserts the stale rebuild returns `ErrStaleTable`. The indexer not treating `ErrStaleTable` as a conflict is **R3I-3**.
- **Arrow refcounts in the merge:** balanced by reading (see R3T-7); not run under a checked allocator.
- **Docs against code for the numbers:** §4.9's "2 × 64 Ki-row record batches, floor 256" matches `rows.go:342-354`; §3.1's `ErrUnknownSchema` rule (refused on upserts and `Rebuild`, delete-only allowed, `Compact` re-emits the scan's columns) matches `writer.go:250`, `:370`, `:453-497`. The one doc claim that does not hold is "that one file's memory" (R3T-3).
- **Spark gate, real code path:** `check_spark_rows` and `concat_part_rows` (`common.sh:117-127`) match `part-*` only, so `.crc` and `_SUCCESS` files are ignored; a directory with no part file fails; `assert_orders_rows` requires exactly three rows and then `diff -u` (`common.sh:86-108`). The self-test run (7 PASS) is what the commit message reports.

### Not completed

The brief asked for the following, which this fragment does not contain:

- **Revert proofs:** none was run. I did not check out the `d993d602e^` or `2af2532b7^` versions to see the new tests fail. In particular, whether `TestAStaleRewriteIsRefusedNotReplayed`, `TestEveryWriteRepairsTheCommitRetryProperty`, `TestWritesRefuseAColumnTheWriterDoesNotKnow`, `TestCompactSortsAFileWrittenBeforeTheFix` and the Spark self-test fail without their fixes is **not established** here. Only the commit messages claim it. For the memory tests, R3T-4 explains why a revert cannot even compile.
- **Probes:** none was written or run. Missing: a split-file Compact (R3T-1), a `memory.CheckedAllocator` leak check (R3T-7), ties and duplicate paths across files at runtime, many files against the budget with `MergeRowsHeld`, nested or reordered columns at runtime, a panic inside the merge, and a `MemStats` curve past the writer queue (R3T-5).
- **iceberg-go v0.7.0 retry behaviour** was read, not exercised; the stale-rebuild scenario was not reproduced.
- **Spark:** the live run was not made (no docker). The multi-part gate is judged on the self-test and by reading only.
- **Docs of `b4c8c24ba`:** I compared `METADATA_SEARCH_DESIGN.md` §3.1 and §4.9 and the Spark section of `docs/metadata-search.md` (`:504-570`) with the code. I did not read the esti README, the design's other changed sections, or `METADATA_SEARCH_PLAN.md`.
- **Other commits of the range:** `26a66b633` (bootstrap policies) and `500a00a37`/`8df39cabd` (iceberg catalog) were not read by me. `da24d421f` (indexer) is covered by `review/mds3/indexer.md`.
- **Areas not checked at all:** `WithRecordBatchSize` and the delete-mode paths of `ApplyDelta` beyond the unknown-schema guard, and the `Rebuild` streaming path, which `d993d602e` did not change apart from `ensureNoReplay` and `checkKnownSchema`.
