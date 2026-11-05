# MDS review 2 — consolidated fix review

## Branch and HEAD reviewed

- Branch: `feature/metadata-search` (also `origin/feature/metadata-search`).
- HEAD reviewed: `4bcd54852135b2a5a649a952c5ccc8fafa61afd0` (`4bcd54852`), the esti-run card.
  The worktree is checked out detached at exactly this commit, working tree clean.
- The four cards consolidated here: `6635b12c1` (indexer), `1bbbf57ab` (table/refmap and the esti
  MDS cases), `d5ca86f53` (server security), `4bcd54852` (esti run). Each card reviewed the same
  16-commit fragment at a different HEAD.
- Fix-review verdict on that fragment is in the next sections. `MDS_REVIEW.md` (the first review)
  is untouched by this consolidation and is referenced by key only.

## Commit range reviewed

`e2333be263907d4cf165f09c934eacb1a9c70337..4bcd54852135b2a5a649a952c5ccc8fafa61afd0` — 20 commits,
of which 4 are the review cards themselves and **16 are under review**:

| # | Commit | Subject | Card |
|---|---|---|---|
| 1 | `e3083e279` | mds/table: remove every delete file of the snapshot on rebuild and compaction (TB-1) | table/esti |
| 2 | `d2811fdcc` | iceberg: refuse remote-signature writes into a reserved namespace | security |
| 3 | `db8172798` | mds/indexer: make a second initialisation of R-metadata harmless (IO-1, TS-5) | indexer |
| 4 | `7f952279b` | mds: bound every indexer HTTP request (MDS_REVIEW IX-2, was TB-3) | indexer + table (catalog half) |
| 5 | `6440e2f13` | mds/indexer: bound the pending walk, rebuild a lost position (IX-1, TS-1) | indexer |
| 6 | `5b997972e` | mds/bootstrap: name the companion repositories explicitly in the §8 policies (MDS-SEC-2, MDS-SEC-5) | security |
| 7 | `224fc82d3` | Merge branch `feature/iceberg-catalog` into `feature/metadata-search` | merge |
| 8 | `fe1e3bbf5` | docs: record the MDS review MAJOR fixes | docs |
| 9 | `4aaf60cb3` | iceberg: prove a companion table is R's index; require the catalog for Metadata Search | security |
| 10 | `98f035d34` | mds: retire unfollowed branches, publish a failed branch's position (IO-3..IO-7) | indexer |
| 11 | `f884a0627` | mds: fix the indexer review MINOR/NIT findings IX-3, IX-4, IX-6, IX-7, TS-3, TS-4, TS-10 | indexer |
| 12 | `9cf0869c8` | mds/bootstrap: deny the import actions on companion repositories | security |
| 13 | `2daeca114` | mds/table, mds/refmap: sort compaction output, drop dead Compact options, assert the resolver (TB-2, TB-4, TB-5) | table/esti |
| 14 | `a397ed971` | Merge branch `feature/iceberg-catalog` into `feature/metadata-search` | merge |
| 15 | `866d4e85c` | esti/mds: fix TS-6, TS-7, TS-8 and TS-9 of the metadata-search review | table/esti |
| 16 | `df9744488` | docs: record the MDS review MINOR/NIT fixes | docs |

(The table/esti card's Verified row says "15 commits: 11 code, 2 docs-only, 2 merges"; it then
enumerates 16, which is the right number — 12 code, 2 docs, 2 merges. The list is correct, the
arithmetic in that one row is not.)

## Verdict

Every fix in the fragment removes the finding it claims to remove, and every fix test the cards
reverted really goes red without its fix (16 revert probes across the three code cards; a
seventeenth, deleting the refresher call from `RoutesBuilder`, stays green and is R2S-8), so the
fragment is honest work: nothing is claimed that the code does not do. But two of the fixes
introduced a new MAJOR of their own, both on paths the suite cannot see: the IO-1 re-initialisation
arm recurses without a bound on any 400 that is not "no changes", so one failing commit takes the
whole indexer process down (R2I-1), and the TB-2 compaction rewrite rebuilds rows field by field, so
a table carrying an eleventh column — the state the design's own schema-compatibility rule allows —
panics inside iceberg-go's writer goroutine and kills the process (R2T-1). Both are CONFIRMED below.
The remaining 12 MINOR and 3 NIT are documentation drift, robustness costs, defence-in-depth gaps
and two §8 policy claims that do not hold at scale or at the edges (repository count, storage
namespace, operator-added statements, the auth cache window). The esti run on `df9744488` passed
7 of 7 steps, 32 of 32 subtests, 4 of 4 engine variants, so the happy paths these fixes touch are
demonstrably live. **Verdict: fix R2I-1 and R2T-1, and this fragment is mergeable; the MINOR/NIT set
can follow.** No BLOCKER was found.

## Fix review

All 17 findings are fixed, in five commits on top of the consolidated review `902839f67`; each finding below
records the commit that fixes it, and `METADATA_SEARCH_PLAN.md` summarises the set. `da24d421f` is the indexer
(R2I-1…R2I-4), `500a00a37` the two catalog findings (R2S-1, R2S-5; made on `feature/iceberg-catalog` and
merged in `8df39cabd`), `26a66b633` the §8 policy findings (R2S-2…R2S-9), `d993d602e` the table findings
(R2T-1…R2T-3) and `2af2532b7` the esti harness (R2T-4). The docs commit that follows updates
`METADATA_SEARCH_DESIGN.md`, `docs/metadata-search.md` and `METADATA_SEARCH_PLAN.md` wherever a fix changed a
described behaviour; it changes no code or test. On the merged head, `go build ./...` passes and
`go test -count=1 -race -timeout 10m ./pkg/mds/... ./pkg/icebergcatalog/... ./cmd/lakefs-sso/...` passes
(every package `ok`; `cmd/lakefs-sso` has no test files).

## Summary of findings

| Key | Severity | Area | Title | File:line | Status |
|---|---|---|---|---|---|
| R2I-1 | MAJOR | indexer | A 400 that is not "no changes" re-enters the whole initialisation recursively, without a bound | `pkg/mds/indexer/repo.go:327-339` | Fixed in `da24d421f` |
| R2T-1 | MAJOR | table | `Compact` panics the whole indexer process on a table whose schema has a column outside the ten `Row` fields | `pkg/mds/table/writer.go:357-362` | Fixed in `d993d602e` |
| R2I-2 | MINOR | indexer | Every caught-up branch now pays one `FindMergeBase` per tick, and a transient 404 on it fails the branch | `pkg/mds/indexer/branch.go:98-104` | Fixed in `da24d421f` |
| R2I-3 | MINOR | indexer | TS-4's fix does not see a COMMIT-protected companion branch — the review's claim that it does is wrong, and the state it was supposed to cure is unchanged | `pkg/mds/indexer/repo.go:70-74` | Fixed in `da24d421f` |
| R2I-4 | MINOR | indexer | The initialisation commits whatever table is already staged, so the marker is sealed on top of a table the indexer did not create | `pkg/mds/indexer/repo.go:303-306` | Fixed in `da24d421f` |
| R2S-1 | MINOR | security | The MDS-SEC-3 "proof" is a table property the attacker writes, so the finding it claims to fix still works | `pkg/icebergcatalog/resolver.go:406-419` | Fixed in `500a00a37` |
| R2S-2 | MINOR | security | The policies grow one statement group per repository; the deny exceeds DynamoDB's 400 KiB item limit at ~560 repositories and the server then refuses to start | `pkg/mds/bootstrap/bootstrap.go:158-181` | Fixed in `26a66b633` |
| R2S-3 | MINOR | security | The deny names repository ARNs but not the companion repositories' storage namespaces, so a guard-group member can create another repository on that namespace and sweep the index | `pkg/mds/bootstrap/bootstrap.go:233-241` | Fixed in `26a66b633` |
| R2S-4 | MINOR | security | A data repository whose name ends in `-metadata` becomes unwritable and undeletable for every base group, while `mds-indexer` gets `fs:*` on it — and the indexer refuses to index it | `pkg/mds/bootstrap/bootstrap.go:158-181` | Fixed in `26a66b633` |
| R2S-5 | MINOR | security | The vended-credentials write guard checks the requested address only, while the write-sign fix it is credited with also checks the resolved target | `pkg/icebergcatalog/creds.go:452-458` | Fixed in `500a00a37` |
| R2S-6 | MINOR | security | The documented "extra statement" for an explicit non-`-metadata` storage namespace is deleted again by the policy repair at the next startup | `pkg/mds/bootstrap/bootstrap.go:713-733` | Fixed in `26a66b633` |
| R2T-2 | MINOR | table | Compaction collects the whole table in memory; peak heap is ~1.5 KiB per row, and the design's bound is O(table), not a batch | `pkg/mds/table/writer.go:357-362` | Fixed in `d993d602e` |
| R2T-3 | MINOR | table | The rebuild/compaction "non-replayable" guarantee only holds when a delete file was removed | `pkg/mds/table/writer.go:389-403` | Fixed in `d993d602e` |
| R2T-4 | MINOR | esti | The new Spark row gate compares only the first `part-*` file and not the guide's snippet | `esti/ops/mds-engines/run-spark.sh:60-88` | Fixed in `2af2532b7` |
| R2S-7 | NIT | security | The deny leaves `branches:SetBranchProtectionRules` (and `retention:*`) open, so a base-group member can stop the indexer permanently | `pkg/mds/bootstrap/bootstrap.go:233-241` | Fixed in `26a66b633` |
| R2S-8 | NIT | security | Nothing tests that `RoutesBuilder` starts the refresher, so the production half of the one-minute guarantee can be deleted unnoticed | `pkg/mds/bootstrap/bootstrap.go:426` | Fixed in `26a66b633` |
| R2S-9 | NIT | security | On an external auth service the accepted one-minute window is really one minute plus the auth cache TTL | `pkg/auth/service.go:612-655` | Fixed in `26a66b633` |

**Totals: 0 BLOCKER, 2 MAJOR, 12 MINOR, 3 NIT.** No finding was dropped, merged away or
downgraded in consolidation; see "Duplicates" for the pairs that were examined and kept separate.

## esti run

Run on `df97444884b1e09a9b6dcb992b6c554497d3df46` (`df9744488`) — *docs: record the MDS review
MINOR/NIT fixes* — on branch `feature/metadata-search`, working tree clean before and after. Run on
2026-11-05 (15:30–15:50 CET; the step logs record UTC), report-only: no code, test or doc was
changed.

**Environment**

- Docker 27.3.1 on colima (x86_64 VM on arm64 host), no compose plugin.
- Object store: the already-running container `esti-seaweedfs`
  (`chrislusf/seaweedfs:latest`, `:8333`) — reused, never started or stopped by this card.
- Go: `PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto`; `go.mod` declares `go 1.26.4`, so the
  auto toolchain resolved to **go1.26.4 darwin/arm64** for every step.
- esti client image: `ESTI_ICEBERG_CLIENT_IMAGE=esti-iceberg-client:pinned`
  (pyiceberg 0.12.0, pyarrow, duckdb 2.0.0), so no per-step pip install.
- Common esti environment for every `go test` step (`ESTI_SETUP_LAKEFS=false`, the suite starts its
  own `lakefs-sso` and its own `lakefs-sso mds run`, as the CI job
  `metadata-search-system-tests` does):

  ```bash
  LAKEFS_BLOCKSTORE_TYPE=s3
  LAKEFS_BLOCKSTORE_S3_ENDPOINT=http://127.0.0.1:8333
  LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID=estiaccesskey
  LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY=estisecretkey
  LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE=true
  AWS_REGION=us-east-1
  ESTI_STORAGE_NAMESPACE=s3://esti-mds-test/mds2-<step>-<epoch>   # one unique prefix per step
  ESTI_SETUP_LAKEFS=false
  ESTI_ICEBERG_CLIENT_IMAGE=esti-iceberg-client:pinned
  ```

- Bounding/FAIL-FAST: every step ran under a wrapper that runs the command in its own process
  group, polls it once a second, and at the bound prints `=== TIMEOUT after Ns: killing process
  group <pid>` into the step log, sends `TERM` to the whole group, then `KILL` after 20s
  (`rc=124`). A child that exits early is reaped at once (`wait`), never waited out. Wall time
  below is that wrapper's clock for the whole step. Step logs are in `/tmp/mds2/<step>.log`; the
  suite's own bounds (`mdsIndexTimeout` 60s, `mdsBackfillIndexTimeout` 10m,
  `mdsBucketOperationTimeout` 10m) were left at their in-code values, and `go test -timeout` was
  set on every step.

| # | Step | Command | Result | Wall time |
|---|------|---------|--------|-----------|
| 1 | Build | `go build ./...` (bound 1200s) | pass, `rc=0`, no output | **18s** |
| 2 | Vet (brief) | `go vet ./esti/...` (bound 600s) | pass, `rc=0` | **2s** |
| 3 | Vet (plan M6 rule) | `go vet ./pkg/mds/... ./cmd/lakefs-sso/...` (bound 600s) | pass, `rc=0` | **2s** |
| 4 | E2E suite | `go test -count=1 -timeout 20m -v ./esti -system-tests -use-local-credentials -run '^TestMetadataSearchEndToEnd$'` (bound 1500s) | pass — `--- PASS: TestMetadataSearchEndToEnd (401.39s)`, `ok ... 402.659s`, **32/32 PASS (1 parent + 31 subtests), 0 FAIL, 0 SKIP** | **407s** |
| 5 | Backfill, 10k | same env + `ESTI_MDS_BACKFILL_OBJECTS=10000`, `go test -count=1 -timeout 15m -v ./esti -system-tests -use-local-credentials -run '^TestMetadataSearchBackfill$'` (bound 1200s) | pass — `--- PASS: TestMetadataSearchBackfill (57.75s)` | **64s** |
| 6 | Backfill, 100k | same env + `ESTI_MDS_BACKFILL_OBJECTS=100000`, `go test -count=1 -timeout 30m -v ./esti ... -run '^TestMetadataSearchBackfill$'` (bound 1800s) | pass — `--- PASS: TestMetadataSearchBackfill (209.90s)` | **217s** |
| 7 | Engine recipes | `./esti/ops/mds-engines/run-all.sh` (bound 1800s; `env-up.sh` → `run-spark.sh` → `run-trino.sh` → `env-down.sh`, 300s per engine container) | pass — `all engine snippets passed`, 4/4 variants (Spark `mds`+`relative`, Trino `mds`+`relative`), endpoint captures identical | **373s** |

**Totals:** 7 of 7 steps passed; 32 of 32 esti subtests passed, 0 skipped; 4 of 4 engine variants
passed; 0 failures. Total wall time 1085s (~18 min).

### Step 4 — TestMetadataSearchEndToEnd, every subtest

All 15 top-level cases of METADATA_SEARCH_DESIGN.md §9 passed — 31 `--- PASS` subtest lines in total,
because `system on R rejects writes` fans out to 7 cases × 2 mounts — and every read of the suite
goes through all four recipes (PyIceberg `pyiceberg`, PyIceberg on `/iceberg/relative_to`, DuckDB
`duckdb`, DuckDB identifier form) at each state:

| Subtest | Time | Subtest | Time |
|---|---|---|---|
| upload and commit | 22.91s | tag and short commit id | 43.71s |
| overwrite | 44.50s | hard reset | 26.58s |
| delete | 23.48s | indexer restart | 27.52s |
| s3 gateway user metadata | 21.40s | user without access is refused | 0.00s |
| filtered scan | 26.34s | R-metadata access does not open the virtual table | 0.02s |
| new branch is seeded | 56.37s | non-indexer cannot write R-metadata | 0.00s |
| merge | 24.10s | system on R rejects writes (7 cases × 2 mounts) | 43.62s |
| indexer still owns R-metadata | 25.67s | | |

Observed index latency after each state change: 1.0–2.5s (`mds_test.go:328/362/376/392/427/430/481/499/525/542/566/796`),
consistent with `period: 2s`.

### Steps 5–6 — backfill timings vs `docs/metadata-search.md`

The guide's figures are from commit `59828bcd1` (`docs/metadata-search.md`:772-786):

| Measure | 10k, guide | 10k, this run | 100k, guide | 100k, this run |
|---|---|---|---|---|---|
| source commit setup | — | 18.174s | 5m46.159s | **2m49.911s** |
| first indexed after indexer start | 2.7s | **2.711s** | 3.212s | **3.213s** |
| throughput | — | 3,689 objects/s | 31,135 objects/s | **31,125 objects/s** |
| `lakefs_mds_tick_duration_seconds_sum` | 0.46s | **0.546551s** | 0.86s | **0.823102s** |
| tick count / rebuilds | 1 / — | 1 / 1 | 1 / 1 | 1 / 1 |
| `mds_rows_written_total` | 10,000 | **10,000** | 100,000 | **100,000** |
| rows counted by PyIceberg and DuckDB | 10,000 | **10,000 both** | 100,000 | **100,000 both**, `source_commit` = head |
| whole test | — | 57.75s | 447.26s | **209.90s** |

The indexer-side numbers reproduce the documented ones within noise (first-indexed within 1ms at
100k, throughput 0.03% apart). The **setup** is the only figure that moved: 2m49.911s here vs
5m46.159s in the guide, and therefore the whole test 209.90s vs 447.26s. That is the import of a
100,000-entry commit on this host/SeaweedFS state, not indexing work — a note, not a failure; the
guide's sentence that "what used to exceed the test timeout was building the source commit, not
indexing it" still holds.

### Step 7 — the four engine recipes

`run-all.sh` (bound 1800s) passed: `mds-engines: all engine snippets passed`. The scripts bounded
each container to 300s and asserted exact rows, not log text:

| Variant | Endpoint | Evidence |
|---|---|---|
| Spark `mds` | `http://lakefs:8000/mds/iceberg/api` | `SELECT` took 23.853s, fetched exactly the 3 `orders/` rows at `SOURCE_COMMIT` |
| Spark `relative` | `http://lakefs:8000/iceberg/relative_to/my-repo.main/api` | 22.981s, same 3 rows; `Spark returned the same rows through both catalog endpoints` |
| Trino `mds` | `/mds/iceberg/api` | `Trino mds returned exactly the three expected orders/ rows at 11d984f4…607d` |
| Trino `relative` | `/iceberg/relative_to/my-repo.main/api` | `Trino relative returned exactly the three expected orders/ rows`; endpoints diffed equal |

Captured rows (both engines, both endpoints, `SOURCE_COMMIT=11d984f4ba8a17d68024bc348ea5353669238ccd85bdd6f3dc372fef46db607d`):

```
orders/2024/one.parquet    34  <SOURCE_COMMIT>
orders/2024/two.parquet    34  <SOURCE_COMMIT>
orders/2025/three.parquet  36  <SOURCE_COMMIT>
```

`customers/c1.json` stayed outside the `orders/%` filter, as the README expects. Repo
`my-repo`, branch `main`, namespace `s3://esti-mds-engines/mds-engines-20261105154307/my-repo`,
indexer first-indexed 4 rows 2s after start (`backfill=head`, `period=2s`, `concurrency=4`).
Total wall 373s. Observed clock: OIDC up 15:43:21 and lakeFS config 15:43:23 (`env-up.sh`), Spark
row captures 15:44:52 and 15:46:06, Trino row captures 15:47:36 and 15:49:16, environment stopped
15:49:18. Each engine container ran under its 300s bound and none approached it.

Note on R2T-4: this run's Spark gate passed, which means the single `part-*` file it read carried
all three expected rows. The multi-file false-failure and the un-compared-extra-row paths R2T-4
describes were therefore not exercised by the run.

### Failures

**None.** 0 failures, 0 skips, 0 timeouts, so no `E2-<n>` entry exists in this run. For the record,
the failure protocol was live: the step wrapper's bound and kill path was self-tested before step 1
(`selftest rc=124 wall=5s`, no leftover process) and was not triggered by any step. The
"Classification" vocabulary that would have been used (product bug / test bug / environment) was
not needed.

### Teardown and post-run checks

- `run-all.sh`'s own `env-down.sh` (trap on success and failure) stopped the `lakefs-sso` server,
  the `mds run` indexer and the OIDC document server it had started: `mds-engines: environment
  stopped` (pids 25981, 25391, 25058).
- The two esti steps that start containers cleaned up after themselves (`docker ps -a` shows no
  `mds-*`, spark or trino container).
- Nothing this card started was left running; the SeaweedFS container was running before the run
  and was left untouched.

```
$ ps -ef | grep -E "lakefs-sso|mds run|oidc" | grep -v grep
(no output)
$ docker ps --format '{{.Names}}\t{{.Status}}'
esti-seaweedfs	Up 2 hours          # pre-existing, reused, not started or stopped here
$ docker ps -a --format '{{.Names}}' | grep -Ei "mds|spark|trino|lakefs"
(none)
$ lsof -nP -iTCP:8000  -sTCP:LISTEN   # empty
$ lsof -nP -iTCP:18080 -sTCP:LISTEN   # empty
$ lsof -nP -iTCP:18099 -sTCP:LISTEN   # empty
```

`esti/ops/mds-engines/.run/` was written by the recipes as documented; it is git-ignored
(`esti/ops/mds-engines/.gitignore:.run/`), so it is not part of the commit.

## Verified

Merged from the three code cards; duplicate rows (build, vet, lint, the `pkg/mds/...` test run)
appear once with every card's result. `S` = server security card, `I` = indexer card, `T` =
table/refmap and esti card.

| Run by | Command | Result |
|---|---|---|
| S, I, T | `git log --oneline e2333be26..HEAD` | 16 commits under review in every card's range (the range held 17/18/19/20 commits in total at each card's HEAD, the extra ones being the review cards). `T`'s row says "15" but enumerates 16 — see the commit-range note above. |
| S, I, T | `export PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto; go version` | `go version go1.26.4 darwin/arm64` (the `go1.25.8` directory holds a 1.26.4 toolchain) |
| S, I, T | `go build ./...` | pass, exit 0 — 20.8 s (S), 17.4 s (I), 18.0 s (T) |
| I | `go vet ./pkg/mds/...` | pass, no output |
| T | `go vet ./pkg/mds/table/... ./pkg/mds/refmap/... ./esti/...` | pass, exit 0, no output |
| S | `go vet ./pkg/mds/bootstrap/... ./pkg/icebergcatalog/... ./cmd/lakefs-sso/...` | pass, no output, `vet exit=0` |
| I | `go test -count=1 -race -timeout 10m ./pkg/mds/...` | all `ok` — `pkg/mds` 4.417s, `bootstrap` 4.471s, `client` 1.406s, `indexer` 13.506s, `refmap` 7.619s, `table` 15.788s |
| T | `go test -count=1 -race -timeout 10m ./pkg/mds/...` | all ok — `mds` 2.930s, `bootstrap` 7.159s, `client` 2.018s, `indexer` 11.359s, `refmap` 4.776s, `table` 13.704s (same command, second run); `./pkg/mds/table/... ./pkg/mds/refmap/...` again: `ok table 9.226s`, `ok refmap 3.906s` |
| S | `go test -count=1 -race -timeout 10m ./pkg/mds/bootstrap/ ./pkg/icebergcatalog/ ./cmd/lakefs-sso/` | `ok pkg/mds/bootstrap 4.070s`, `ok pkg/icebergcatalog 27.168s`, `cmd/lakefs-sso [no test files]` |
| I, T | `go tool golangci-lint run ./pkg/mds/...` / `./pkg/mds/table/... ./pkg/mds/refmap/... ./esti/...` | `0 issues.` |
| S | `go tool golangci-lint run --allow-parallel-runners ./pkg/mds/bootstrap/... ./pkg/icebergcatalog/... ./cmd/lakefs-sso/...` | `0 issues.` |
| S | Revert `d2811fdcc`'s two guards in `pkg/icebergcatalog/sign.go` (`&& false` on both `requireWritable` calls), `go test -run TestRemoteSigningReservedNamespace` | **red**: both `a write sign on the reserved virtual address is refused` and `a redirect into a reserved namespace is refused` answer **200** instead of 403 (body `{"uri":"https://signed.example/object?X-Amz-Signature=test",...}`). Restored with `cp /tmp/sign.go.bak`; `git diff` empty. |
| S | Revert `4aaf60cb3`'s verifier block in `Store.Resolve` (`&& false`), `go test -run TestMetadataSearchCompanionRepositoryProof` | **red**: `a companion table that names another repository is not served` and `a companion table with no repository property is not served` answer **200** instead of 404. Restored; `git diff` empty. |
| S | Revert `4aaf60cb3`'s `Config.Validate` cross-check (`&& false`), `go test -run TestLoadConfigValidationErrors` | **red**: `An error is expected but got nil` for both `metadata_search without the catalog` cases. Restored; `git diff` empty. |
| S | Remove `fs:Import*` from `metadataRepoProtectFSDeniedActions`, `go test -run 'TestMetadataRepoProtectCoversImportActions\|TestMetadataRepoProtectPolicyStatements'` | **red**: `Not equal: expected: false` (ImportCancel allowed) and `Should be true` (statement list). Restored; `git diff` empty. |
| S | Rebuild the pre-`5b997972e` wildcard policy statements in a probe and run the MDS-SEC-2 decisions through the real ACL service | **as the review found**: `mds-indexer fs:WriteObject …/analytics/object/foo-metadata/data.yml → allowed=true`, `fs:DeleteObject …/analytics/object/-metadata → true`, `catalog:DeleteTable …/table/analytics/foo-metadata/orders → true`, guard-group operator `fs:WriteObject …/foo-metadata/data.yml → allowed=false`. So `TestMDSPoliciesDoNotReachDataRepositories` is red on the policy the fix replaced. |
| S | Delete the `startPolicyRefresher(ctx, deps, policyRefreshInterval)` call from `RoutesBuilder`, `go test ./pkg/mds/bootstrap/ -run 'TestRoutesBuilder\|TestKnownDataRepositories\|TestPolicyRefresher\|TestEnsurePrincipals'` | **green**, `ok … 1.310s` — the production wiring of the refresher is untested (R2S-8). Restored; `git diff` empty. |
| S | Probe — `pkg/mds/bootstrap/probe_r2s_test.go` (deleted before commit): policy size, operator-statement repair, ordinary `*-metadata` repository, deny gaps, namespace bypass | outputs quoted in R2S-2, R2S-3, R2S-4, R2S-6, R2S-7 |
| S | Probe — `pkg/icebergcatalog/probe_r2s_test.go` (deleted before commit): ownership-proof forgery, credential redirect | outputs quoted in R2S-1 and R2S-5 |
| S | Probe — `wildcard.Match` on the exact ARN shapes (`pkg/mds/bootstrap`, temporary) | `namespace/comp/*` matches `namespace/comp/` (the repo-level listing ARN) and `namespace/comp/system`; `table/comp/*/*` matches `table/comp/system/object_metadata` and a deeper name, and **not** `table/comp2/system/x`; `repository/comp/*` matches `repository/comp/branch/main` and **not** `repository/comp` or `repository/comp2/object/x`; `namespace/*-metadata` matches only names ending in the suffix. |
| I | Revert `if total >= limit+pageSize` → `if false && …` in `pending` | `TestFarBehindBranchIsRebuiltInsteadOfWalked` FAILs (`expected: 2, actual: 21` LogCommits requests) — restored |
| I | Revert `case errors.Is(err, client.ErrNotFound)` in `continueFrom` → `case false` | `TestCollectedPositionIsRebuilt` FAILs — restored (`TestCollectedPositionDuringTheWalkIsRebuilt` still passes: it pins the `stop_at` half, which is a separate code path) |
| I | Revert `AllowEmpty: false` → `true` in `initialise` | `TestConcurrentInitialisationCommitsOnce` FAILs (two `mds: init` commits: `index-2`, `index-3`) — restored |
| I | Revert drop `retireUnfollowedBranches` + `ClearLag` from the non-matching loop | `TestDeletedBranchLeavesHealthWithoutDeletingTheMirror` and `TestBranchStopsMatchingClearsItsLagGauge` FAIL — restored |
| I | Revert `if meta.ReadOnly` → `if false && meta.ReadOnly` | `TestReadOnlyMetadataRepositoryFailsTheRepository` FAILs — restored |
| I | Revert the `lastIndexed` read-back in `indexBranch` (IO-4) | `TestFailedBranchReportsTheRecordedPosition` FAILs — restored |
| I | Revert `tableHandle.refresh` → `TableStore.LoadTable` (IX-3) | `TestReloadReusesTheBranchCatalog` FAILs (`expected: 1, actual: 3` `/v1/config`) — restored |
| I | Revert `http.Client{Timeout: requestTimeout}` → `Timeout: 0` (IX-2) | `TestTickAgainstAnAPIThatNeverAnswers` FAILs after 10 s (`the tick did not return within 10s`) — restored |
| I | Revert re-add the 200/500 compaction defaults in `applyDefaults` (IO-6) | `TestCompactionDisabledByZero/max_delete_files:_0` and `/every_commits:_0` FAIL — restored |
| I | Revert drop `TickCanceller.CancelTick` from `tickRepository` (IO-5) | `TestCancelledTickIsNotLeftInProgress` FAILs — restored |
| I | Probe 1 (`zz_probe_review_test.go`, deleted before committing): persistent non-`ErrNoChanges` 400 on the init commit | **26 initialisation attempts in one tick**, growing wrapped error — see R2I-1 |
| I | Probe 2 (same file): request counts of a caught-up tick vs the backfill tick | `FindMergeBase=1 LogCommits(R)=0 GetCommit=3 (backfill tick: mergebase=0 logs=1 gets=3)` — see R2I-2 |
| I | Probe 3 (same file): transient 404 on `FindMergeBase`, branch already at its head | `branch error=merge base of repo@s1..s1: find merge base "s1": lakeFS resource not found`, `lag=-1` — see R2I-2 |
| I | `git status --porcelain` after every revert | clean apart from the probe file; `git diff` empty when the fragment was written |
| T | Revert probe TB-1 (`writer.go`: `Rebuild` back to `txn.Overwrite` with no filter, `Compact` back to the scan-task delete files), then restore | `TestRebuild` FAIL *"Should be zero, but was 2"* (`writer_test.go:277`); `TestCompact` FAIL *"Should be zero, but was 2"* (`writer_test.go:384`) — the new assertions really fail without the fix |
| T | Revert probe TB-2 (`Compact` streams the scan's batches again, `Rebuild` without `orderedRows`), then restore | `TestCompactWritesRowsInPathOrder` FAIL actual `[b d a c]` (expected `[a b c d]`); `TestRebuildRefusesRowsOutOfOrder` FAIL *"Expected error … but got nil"* |
| T | Probe `TestProbeCompactExtraColumn` (temporary, deleted) | old streaming compaction keeps `extra=keep-me`; new `Compact` **panics** `arrow/array: some fields have excessive number of rows (want at most 0, have 2)`; `ApplyDelta` panics the same way → R2T-1 |
| T | Probe `TestProbeCompactPeakHeap` / `TestProbeCompactHeapBreakdown` (temporary, deleted) | 200 000 rows: scan-only peak 101 MiB (530 B/row), scan + rows slice 166 MiB (872 B/row), full `Compact` 310–325 MiB (1.45–1.6 KiB/row); 20 000 → 89 MiB, 80 000 → 168 MiB, so the peak is linear in the row count → R2T-2 |
| T | Probe `TestProbeCompactMultiFileRowsPreserved` (temporary, deleted) | 12 data files, 9 delete files, 1 596 rows: `Compact` then a second `Compact` then a delta leave exactly the same rows, no duplicate path, `total-delete-files == 0` — no row loss in the TB-1 fix |
| T | Probe `TestProbeCompactEmptyTable` (temporary, deleted) | `Compact` of an empty table still stages a new `metadata.json` (`…00003…` → `…00004…`), so the indexer's `AllowEmpty: false` commit can succeed; no finding |
| T | esti (`go test -run '^TestMetadataSearch…' ./esti -system-tests`) | **not run by that card**: needs SeaweedFS + docker and that brief forbade docker runs. `go vet ./esti/...` compiles and vets both new cases; R2T-4 comes from reading the harness. The esti-run card later ran the suite from `df9744488` — see the esti run section (7/7 steps, 32/32 subtests). |
| T | Probe files (`pkg/mds/table/zz_probe_review_test.go`, `pkg/mds/table/zz_probe_expose.go`) | deleted; `git status` clean apart from the fragment |

`I` notes that TS-5 (`initScan` streaming) and TS-3/TS-10 (fake writes, `FirstParent`, best common
ancestor) were checked by reading the code and the real implementations
(`pkg/graveler/graveler.go:2239-2260`, `:2581-2605`, `:2637-2680`, `pkg/graveler/errors.go:16-46`,
`pkg/api/controller.go:3196-3240`, `pkg/graveler/ref/commit_generation_priority_queue.go`,
`pkg/graveler/ref/merge_base_finder.go`) rather than by reverting: TS-5's revert is a rewrite of the
function, and TS-3/TS-10 only change the fake, so their "test fails before the fix" claim is about
the fake, not product code.

## Spot-check of every BLOCKER and MAJOR

There is no BLOCKER. Both MAJORs were re-checked against the tree at `4bcd54852`, by reading the
code and the vendored/module sources. Neither card's probe was re-run: re-creating one would have
meant adding a test file, which this card may not do.

| Key | Severity | Spot-check | Reason |
|---|---|---|---|
| R2I-1 | MAJOR | **CONFIRMED** | `initialise` maps every `client.ErrBadRequest` to `verifyInitCommit` (`repo.go:327-339`), whose `freshAutomaticRoot()` arm calls `initialise` again (`repo.go:196-203`); a branch that still shows only the automatic root takes that arm on every pass, so a persistent 400 — `block.ErrWriteFailed` is one, `controller.go:3225` — re-enters with no counter, re-wrapping the old error each level. |
| R2T-1 | MAJOR | **CONFIRMED** | `Compact` now decodes and re-emits rows (`writer.go:357-362` → `rowBatches`), and `appendRow` fills exactly the ten fixed columns by position (`rows.go:267-281`); arrow-go's `RecordBuilder.NewRecordBatch` panics when column lengths differ, and iceberg-go consumes the batch iterator inside the `go func()` of `unpartitionedWrite` (`table/arrow_utils.go:2126-2140`), so the panic is on a goroutine no `recover` can reach. |

Two notes on the confirmation, for the record: the design's own rule
(`pkg/mds/table/schema.go:14` and §3.1) is that schema changes only ever add optional fields with
new IDs, and `committer`/`content_type`/`creation_date` were added that way, so R2T-1's trigger is
the sanctioned evolution path, not a hypothetical. And R2I-1's severity rests on the recursion being
unbounded rather than on the specific 400: any non-`ErrNoChanges` 400 has the same shape.

## Findings

### BLOCKER (0)

None.

### MAJOR (2)

#### R2I-1 - MAJOR: a 400 that is not "no changes" re-enters the whole initialisation recursively, without a bound

**Fixed in `da24d421f`** - the `ErrBadRequest` arm re-verifies with `completeInitialisation=false` and returns the original 400 unless a contiguous `mds: init` run of another replica is there to adopt; the regression test pins one `CreateTable` and one commit attempt instead of 26.

**Spot-check: CONFIRMED** — see the table above.

**Where** `pkg/mds/indexer/repo.go:327-339` (`initialise`'s `errors.Is(err, client.ErrBadRequest)` arm),
reached again from `pkg/mds/indexer/repo.go:196-203` (`verifyInitCommit` → `freshAutomaticRoot` → `initialise`).
Introduced by `db8172798` (IO-1).

**What happens** `initialise` now maps **every** HTTP 400 from the initialisation commit to "a concurrent
initialisation committed first": it calls `verifyInitCommit`, which for a companion repository that still shows
only lakeFS's automatic `Repository created` root takes the `freshAutomaticRoot()` arm and calls `initialise`
straight back. When the 400 is `graveler.ErrNoChanges` that is correct and terminates on the second pass (the
winner's marker is now on the branch). When the 400 is anything else, the two branches never exit: the same
several requests and the same commit are retried from a new stack frame, forever.

`POST /repositories/{r}/branches/{b}/commits` answers 400 for `ErrNoChanges` **and** for
`block.ErrWriteFailed` (`pkg/api/controller.go:3225`), `validator.ErrInvalidValue`/`graveler.ErrInvalidValue`
(`pkg/api/controller.go:3228-3230`), `graveler.ErrCommitMetaRangeDirtyBranch`, and so on. A local blockstore
failing to write the metarange (`pkg/block/local/adapter.go:177` returns `block.ErrWriteFailed`) is a realistic
persistent trigger; so is any HTTP intermediary or lakeFS version that answers a 400 to the commit.

Consequences, all in one goroutine and one tick:

- unbounded recursion: each pass adds a frame and, worse, re-wraps the previous error
  (`"commit the initialisation of …: %w; the branch is not initialised either: %w"`), so the string grows by one
  copy of itself per level — O(depth²) bytes held and a monstrous `/_health` `last_error`. The process
  eventually dies of stack exhaustion (`goroutine stack exceeds 1000000000-byte limit`), taking every
  repository of the tick loop with it, or of memory first.
- `/_health` never turns 503 in the meantime: the whole tick is stuck inside `ensureMetadataRepository`, i.e.
  the repository is merely "in progress" — exactly the IX-2/IO-5 shape.
- the failure is *hidden* on the first pass: a single spurious 400 is silently retried instead of failing the
  repository, which is the opposite of what the other repository-level failures do.

**Evidence** Probe 1 (a `client.LakeFS` wrapper that answers `client.ErrBadRequest` with a
`block: write failed` message to every `mds: init` commit, and gives up after 25 attempts so the test can
finish):

```
PROBE: a persistent 400 on the initialisation commit caused 26 initialisation attempts in one tick
log: mds: repository skipped error="commit the initialisation of \"repo-metadata\"@main: commit \"repo-metadata\"@main: block: write failed: lakeFS request rejected; the branch is not initialised either: commit the initialisation of \"repo-metadata\"@main: commit … (26 nested copies)"
```

Each attempt also re-ran `CreateTable`/`LoadTable` (the `ErrTableExists` arm) and one `LogCommits` page.
`logrus` printed `mds: completing the initialisation of a newly created metadata repository` 26 times.

**Suggested fix** Treat the 400 as "lost the race" only if the re-verification *accepts* the branch **and** the
commit it adopts is different from the one the failing `initialise` tried to make; if `verifyInitCommit` comes
back with `freshAutomaticRoot` (nothing to adopt) return the original 400 instead of initialising again — or,
equivalently, give `initialise` an `attempt` counter of 1 and refuse to recurse. Test that fails before it: the
probe above, asserting the repository error contains `block: write failed` and that `CreateTable` was called
once, not 26 times.

#### R2T-1 - MAJOR: `Compact` panics the whole indexer process on a table whose schema has a column outside the ten `Row` fields

**Fixed in `d993d602e`** - `Compact` merges the per-file streams and re-emits the scan's own Arrow columns - a group of consecutive rows of one source batch is a zero-copy slice, several are concatenated column by column - so a top-level column the writer does not know keeps its values, and every column has the batch's row count. A write that has to **build** rows (`ApplyDelta` with upserts, every `Rebuild`) refuses such a table with the new `ErrUnknownSchema` instead of panicking or nulling the column, a delete-only delta still applies, and an unexpected panic in the merge becomes `ErrMergeRows` rather than reaching iceberg-go's writer goroutine.

**Spot-check: CONFIRMED** — see the table above.

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

### MINOR (12)

#### R2I-2 - MINOR: every caught-up branch now pays one `FindMergeBase` per tick, and a transient 404 on it fails the branch

**Fixed in `da24d421f`** - `continueFrom` returns before `FindMergeBase` when `last == head.ID`; the tests pin zero merge-base calls on an idle tick and a healthy branch when that tick's `findMergeBase` answers 404.

**Where** `pkg/mds/indexer/branch.go:98-104` (`runBranchOnce` calls `continueFrom` unconditionally) →
`branch.go:172-174` (`FindMergeBase`). Introduced by `6440e2f13`: the guard `if last != head.ID` around the
step-4 merge-base read was dropped when the block was extracted into `continueFrom`
(`git show e2333be26:pkg/mds/indexer/branch.go` line 86 (and 110, for the todo walk) has the guard; the new `continueFrom` has no
equivalent).

**What happens** Design §4.2 step 3 says a branch whose recorded position equals its head has "nothing to apply;
the tag repair … and the 4.9 compaction check still run", and step 4 only asks whether `last` is an ancestor of
`head` — which is trivially true when they are the same commit. The code now asks lakeFS anyway, on every tick,
for every branch that is already up to date, i.e. the steady state of a healthy indexer: +1 API request per
branch per tick (10 000 branches at `period: 60s` ⇒ 10 000 extra requests a minute) and one extra chance to
fail.

The second half is a real wrong answer, not just cost: a 404 from that unnecessary call makes
`continueFrom` ask `positionGone(last)`, which is false (the position is the head, and it exists), so the branch
is reported failed — `/_health` 503 with `lag_commits: -1` — for a branch that had nothing to do. Before the
change the same transient answer touched nothing. (`TestNotFoundThatIsNotThePositionIsAFailure` covers the
"there is work to do" case, not this one, so the suite is green.)

**Evidence** Probe 2 (counting `client.LakeFS` wrapper, two ticks, second one with no new commits):

```
PROBE caught-up tick: FindMergeBase=1 LogCommits(R)=0 GetCommit=3 (backfill tick: mergebase=0 logs=1 gets=3)
```

Probe 3 (same env, first tick indexes `s1`, then `findMergeBaseErr = &client.NotFoundError{…"s1"}` and a second
tick):

```
mds: branch not indexed … error="merge base of repo@s1..s1: find merge base \"s1\": lakeFS resource not found" head=s1 lag=-1 last=s1
PROBE idle branch, transient 404 on findMergeBase: branch error=merge base of repo@s1..s1: find merge base "s1": lakeFS resource not found
```

**Suggested fix** Restore the guard inside `continueFrom`: `if last == head.ID { return last, nil }` before the
`FindMergeBase` call (the caller already skips `pending` in that case). Test that fails before it: a second tick
with no new commits and a counting client, asserting zero `FindMergeBase` calls, plus a variant that injects
`findMergeBaseErr` on that tick and asserts the branch stays healthy.

#### R2I-3 - MINOR: TS-4's fix does not see a COMMIT-protected companion branch — the review's claim that it does is wrong, and the state it was supposed to cure is unchanged

**Fixed in `da24d421f`** - `metadataBranchWriteError` answers a 403 from `Commit`, `ResetBranch`, `HardResetBranch` and the catalog's table writes with the `ErrReadOnlyMetadataRepository` family and a message naming branch protection; the fake now models `COMMIT` and `STAGING_WRITE` the way graveler and `pkg/api` answer them.

**Where** `pkg/mds/indexer/repo.go:70-74` (`ensureMetadataRepository` reads only `meta.ReadOnly`), versus
`MDS_REVIEW.md` TS-4 ("a read-only (or COMMIT-protected) `R-metadata` fails the repository once per tick, with a
message naming both causes") and `METADATA_SEARCH_DESIGN.md` §7 ("Per-repository runtime errors (… companion
repository read-only) mark that repository failed in `/_health`").

**What happens** A branch-protection rule that blocks `COMMIT` on `R-metadata@main` is not visible in the
repository record, so the check never fires: `graveler.Commit` returns `ErrCommitToProtectedBranch`
(`pkg/graveler/graveler.go:2245-2252`, wrapping `ErrProtectedBranch`, `pkg/graveler/errors.go:21`), which
`pkg/api/controller.go:3196-3200` maps to **403**. Every tick, every branch therefore fails with the same
`cannot commit to protected branch` 403, `/_health` stays 503 (and now with `lag_commits: -1`, hiding the real
lag), tag retention never runs, and the error the operator sees is the indexer's wrapper, not the message that
names the cause and the fix. That is precisely the state TS-4 described; the fix removed it only for the
`read_only: true` half. The message that "names both causes" exists only inside the `ErrReadOnlyMetadataRepository`
error, which a protected branch never produces.

No test can see it: the fake has no branch-protection model (`fake_test.go` `requireWritableBranchLocked` only
checks repository existence, `ReadOnly` and branch existence), so `TestReadOnlyMetadataRepositoryFailsTheRepository`
pins the flag and nothing else.

**Evidence** `grep -n "ReadOnly\|Protect" pkg/mds/indexer/repo.go` → only `meta.ReadOnly` at `:70`; the design
sentence quoted above; `pkg/graveler/graveler.go:2245-2252` (protection is checked *before* `ReadOnly` and before
the branch lookup) and `:2254-2257` (`ReadOnly` → `ErrReadOnlyRepository`).

**Suggested fix** Widen the repository-level refusal to the protection case: in `runBranchOnce`'s write path (or
in `ensureMetadataRepository`, over the branches the tick is going to write) detect a 403 from a write to
`R-metadata` and answer an error of the `ErrReadOnlyMetadataRepository` family that names branch protection, or
drop the "(or COMMIT-protected)" parenthetical from MDS_REVIEW and design §7. Test that fails before it: a fake
repository flagged protected whose `Commit`/`ResetBranch` answer
`ErrForbidden("cannot commit to protected branch")`, asserting one repository-level error instead of a per-branch
403 on every tick.

#### R2I-4 - MINOR: the initialisation commits whatever table is already staged, so the marker is sealed on top of a table the indexer did not create

**Fixed in `da24d421f`** - the `ErrTableExists` arm loads the staged table and refuses it with `ErrForeignRepository` unless its `lakefs.mds.repository` is `R`; the check uses an optional `Repository()` accessor, so the `Table` seam is unchanged.

**Where** `pkg/mds/indexer/repo.go:303-306` (`ErrTableExists` → `LoadTable`, no property check),
`repo.go:325` (`AllowEmpty: false` commit of *that* staging), `pkg/mds/table/writer.go:118-125`
(`repositoryOf` is read, never compared) versus `writer.go:152-163`/`:254-265`/`:333-340` (the value is only
passed to `SnapshotProperties`).

**What happens** §4.8's refusal rule is about a repository "the indexer did not create", and MDS-SEC-3 added the
`lakefs.mds.repository` proof for the *read* side because "whoever can create a repository named `R-metadata`
before the indexer does … owns the table". The write side has no equivalent: the catalog's table creation
**stages** the pointer, so a principal that stages `system.object_metadata` on a fresh `R-metadata@main` before
the indexer's first tick supplies the table metadata (schema, `location`, properties) that `initialise` then
*commits* as the `mds: init` commit — and after that the branch is "initialised", so no later tick re-checks
anything. Two outcomes, both bad and both permanent:

- the adopted table carries no `lakefs.mds.repository`: `repositoryOf` (`writer.go:118-125`) fails on the first
  `Rebuild`/`ApplyDelta`, so the repository is initialised *and* unindexable for ever (a manual
  `delete R-metadata` is the only way out, which is the failure mode §4.8 exists to avoid);
- it carries the property (any value): the indexer writes its rows into a table whose `location` and schema come
  from that metadata document, with the server's own storage credentials, and every snapshot it adds records the
  attacker-chosen `lakefs.mds.source-repository`.

The threat model is the same as MDS-SEC-3's, one step earlier: it needs `catalog:*` on `R-metadata`, which the
§8 deny covers only for the caller's guard groups. The defect predates the fixes (the old code adopted the same
staging with `AllowEmpty: true`), but `db8172798` is the commit that made the initialisation commit "the
idempotency point" without making it the ownership point, and no test covers it — the fake cannot even express
it, because `fakeTable.Rebuild` replaces the whole `fakeState` (`fake_test.go`, `newState()`), so the foreign
table's "metadata" disappears in the fake and the test passes for the wrong reason.

**Evidence** code path above; `pkg/mds/table/schema.go:227-232` (`SnapshotProperties` is the only consumer of
`repositoryOf`'s answer); `MDS_REVIEW.md` MDS-SEC-3 for the read-side counterpart;
`pkg/mds/indexer/fake_test.go` `Rebuild`/`CreateTable` for the missing modelling (one `tableExists` bool, no
schema/location/property).

**Suggested fix** In the `ErrTableExists` arm, load the staged table and refuse to commit it unless
`tbl.Properties().Get(table.PropertyRepository) == repository` (and, if cheap, the schema's field IDs match
`table.Schema()`); answer `ErrForeignRepository` otherwise, as §4.8 does for a foreign history. Test that fails
before it: a table store whose `LoadTable` reports a table with a different/absent `lakefs.mds.repository`,
asserting `ErrForeignRepository` and no `mds: init` commit.

#### R2S-1 - MINOR: the MDS-SEC-3 "proof" is a table property the attacker writes, so the finding it claims to fix still works

**Fixed in `500a00a37`** - a companion table is served only when the property **and** the served ref's first-parent `mds: init` commit (`lakefs.mds.init=true`) both hold; the walk is newest-to-root, one 256-commit page at a time, cached per repository and served head commit, and a `BranchLister` that cannot read history fails closed.

**Where**: `pkg/icebergcatalog/resolver.go:406-419` (`VerifyResolved` compares
`tbl.Metadata.Properties()[MetadataSearchRepositoryProperty]` with `requested.Repo`),
`pkg/icebergcatalog/resolver.go:483-510` (`Store.Resolve` calls it),
`pkg/icebergcatalog/resolver.go:126` (the property literal), and the write half
`pkg/mds/table/schema.go:221` (`Properties(repository)` puts it in the create request).

**What happens**: MDS-SEC-3's scenario is "whoever creates a repository named `R-metadata` before the
indexer does owns the table served to every reader of `R`". The fix makes the catalog serve that
repository only when its `system.object_metadata` table carries `lakefs.mds.repository = R` — a value the
attacker puts in `metadata.json`, or in the `properties` of `POST /v1/…/tables` (the catalog passes the
client's `request.Properties` straight into `table.NewMetadataWithUUID`, `tables.go:724`). Nothing
distinguishes a table the indexer wrote from one the attacker wrote, and the write is not authorized by
the *server*: it is content. So the attack is unchanged except for one extra property, and the
consequence MDS-SEC-3 lists is unchanged too: the untrusted `metadata.json` `location` is what `sign.go`
(`tableLocation`/`loadLocation`) and `creds.go` (`newTableScopeResolver`) trust, so a reader of `R` is
handed **read** signatures/sessions scoped to a prefix the attacker chose.

**Evidence**: probe (`pkg/icebergcatalog/probe_r2s_test.go`, `newResolverFixtureWithTableProperty(t, "")`):
```
PROBE ownership proof: alice load of lakehouse.main.system.object_metadata before the property: 404
PROBE ownership proof: alice load after the same document was rewritten with lakefs.mds.repository=lakehouse: 200
```
The only change between the two lines is the content of the same `metadata.json` at the same location, via
`putResolverObject(t, f.catalog, resolverMetaRepo, f.metaLocation, …)` — an operation any principal that
can write that repository performs. `TestMetadataSearchCompanionRepositoryProof`
(`resolver_test.go:687`) pins the 404 cases and `TestMetadataSearchVirtualTableRead` pins the 200 for a
fixture whose companion repository *the fixture* created; no test asks who wrote the property.
The review's own suggested fix asked for "the `lakefs.mds.init` marker at an accepted position … and,
ideally" — that half was not implemented (`git grep lakefs.mds.init pkg/icebergcatalog` is empty;
`pkg/mds/indexer/repo.go` knows the marker but the catalog does not).

**Suggested fix**: prove ownership with state the attacker's `metadata.json` cannot forge: require the
`mds: <R>@<commit>` init commit (`lakefs.mds.init=true`, `pkg/mds/indexer/repo.go`) on the served ref's
first-parent history, or record the companion table's UUID in server-side state at creation and compare,
in addition to the property. Test: a companion table with `lakefs.mds.repository=R` but no init commit is
404 (it is 200 today); a genuine index is 200.

#### R2S-2 - MINOR: the policies grow one statement group per repository; the deny exceeds DynamoDB's 400 KiB item limit at ~560 repositories and the server then refuses to start

**Fixed in `26a66b633`** - both policies are a base policy plus numbered chunk policies of at most 64 companion repositories each (the first chunk is `<base>-1`); `MaxPolicyStoredBytes` is 160 KiB, and the 1000-repository test stores 34 policies, the largest 67,944 bytes.

**Where**: `pkg/mds/bootstrap/bootstrap.go:158-181` (`companionRepositories`), `:280-333`
(`IndexerPolicy`, six statements per companion), `:338-365` (`MetadataRepoProtectPolicy`, five per
companion), `:643-650` (`refreshPolicies`), `:713-741` (`ensurePolicy` → `WritePolicy`),
`pkg/auth/model/model.go:295` and `pkg/kv/msg.go:30-35` (a policy is one KV item, `proto.Marshal` +
`Set`, never chunked).

**What happens**: an installation with N repositories writes `6N+2` allow statements and `5N` deny
statements. The proto value is 361 031 bytes for 500 repositories and 722 031 bytes for 1000, so the deny
item crosses DynamoDB's 400 KiB item limit at about 560 repositories (CosmosDB's 2 MB at about 2 800).
At that point `WritePolicy` fails with a DynamoDB `ValidationException`; `ensurePolicy` wraps it as
`rewrite policy …`/`create policy …`, `EnsurePrincipals` returns it, `RoutesBuilder` returns it, and
`cmd/lakefs/cmd/run.go:306` turns it into `logger.WithError(err).Fatal("extra routes")` — the **server
does not start**, on a supported KV backend, purely because of the repository count. Independently of the
size limit, every authorization decision for any base-group member evaluates the whole deny
(`contrib/auth/acl/service.go:987` → `auth.CheckPermissions` over all effective policies), i.e. 5N wildcard matches
per request, and every refresh that changes the set rewrites and `PurgePolicies`s a multi-hundred-KB
value.

**Evidence** (probe, `pkg/mds/bootstrap/probe_r2s_test.go`, `IndexerPolicy`/`MetadataRepoProtectPolicy` +
`proto.Marshal(model.ProtoFromPolicy(p))`):
```
PROBE scale n=100 policy=mds-indexer statements=602 proto_bytes=49116
PROBE scale n=100 policy=mds-metadata-repo-protect statements=500 proto_bytes=72231
PROBE scale n=400 policy=mds-indexer statements=2402 proto_bytes=195816
PROBE scale n=400 policy=mds-metadata-repo-protect statements=2000 proto_bytes=288831
PROBE scale n=500 policy=mds-indexer statements=3002 proto_bytes=244716
PROBE scale n=500 policy=mds-metadata-repo-protect statements=2500 proto_bytes=361031
PROBE scale n=1000 policy=mds-indexer statements=6002 proto_bytes=489216 over_dynamodb_400KiB=true
PROBE scale n=1000 policy=mds-metadata-repo-protect statements=5000 proto_bytes=722031 over_dynamodb_400KiB=true
```
Nothing in METADATA_SEARCH_DESIGN.md §8 or `docs/metadata-search.md` names a repository-count limit.

**Suggested fix**: bound the item size, e.g. split the companion set into chunks of a few hundred and
write `mds-metadata-repo-protect-<k>` policies (the refresher already repairs by exact statement
comparison, so chunks are as idempotent as one policy), or deny the same set through one policy per
companion repository. Test: with 1000 repositories every policy value stays under a documented cap and
`RoutesBuilder` succeeds (fails today on DynamoDB and is silent about the limit everywhere).

#### R2S-3 - MINOR: the deny names repository ARNs but not the companion repositories' storage namespaces, so a guard-group member can create another repository on that namespace and sweep the index

**Fixed in `26a66b633`** - the deny adds `fs:AttachStorageNamespace` on the exact storage namespace of every companion repository that exists in the catalog; the `namespace/*-metadata` pattern stays in the allow because a companion that does not exist yet has no namespace to name.

**Where**: `pkg/mds/bootstrap/bootstrap.go:233-241` and `:338-365` (the deny's resources are
`RepoArn(companion)`, `RepoArn(companion)+"/*"` and the three catalog ARNs; no
`fs:AttachStorageNamespace` statement), `pkg/api/controller.go:2290-2305` (`CreateRepository` authorizes
`fs:CreateRepository` on `repository/<name>` and `fs:AttachStorageNamespace` on the *storage namespace*),
`pkg/catalog/catalog.go:533-557` (no uniqueness or overlap check on the namespace).

**What happens**: `POST /api/v1/repositories {"name":"evil","storage_namespace":"s3://bucket/analytics-metadata"}`
by a member of `Admins`/`Developers` needs two permissions the deny deliberately does not cover — neither
ARN is a companion repository ARN — and lakeFS accepts a second repository on an existing namespace. From
then on `evil`'s uncommitted GC scans `s3://bucket/analytics-metadata`, exactly the sweep METADATA_SEARCH_DESIGN.md
§3.3 says "would delete `R-metadata`'s files" (that is why the *indexer* refuses an overlapping companion
namespace on every tick, `pkg/mds/indexer/repo.go`). The deny's promise — "no principal in a base group
may write, delete, create … inside a companion repository" — is therefore reachable through a
non-companion ARN.

**Evidence**: probe (`pkg/mds/bootstrap/probe_r2s_test.go`), real ACL service, `EnsurePrincipals` with
`dataRepositories=[analytics]` and the standard guard list:
```
PROBE namespace bypass: guard-group admin fs:CreateRepository on arn:lakefs:fs:::repository/evil -> allowed=true
PROBE namespace bypass: guard-group admin fs:AttachStorageNamespace on arn:lakefs:fs:::namespace/s3://bucket/analytics-metadata -> allowed=true
PROBE namespace bypass: guard-group admin fs:CreateRepository on arn:lakefs:fs:::repository/analytics-metadata -> allowed=false
PROBE namespace bypass: create evil on the companion's namespace mem://shared: err=<nil>
```
(The last line is the real `catalog.Catalog` of the bootstrap fixture: `analytics-metadata` and then
`evil` on `mem://shared` both succeed.)

**Suggested fix**: for every companion repository the catalog lists, add the exact
`fs:AttachStorageNamespace` deny on `permissions.StorageNamespace(repo.StorageNamespace)`, and keep
`CreateRepository`'s `fs:Create*` as it is (`knownDataRepositories` already lists the repositories; it
needs to also read each companion's namespace, skip ones the caller cannot see, and the pattern
`namespace/*-metadata` stays for not-yet-existing companions). Test: a guard-group member's
`CreateRepository`+`AttachStorageNamespace` node on a companion repository's namespace is denied (it is
allowed today) and on an ordinary namespace stays allowed.

#### R2S-4 - MINOR: a data repository whose name ends in `-metadata` becomes unwritable and undeletable for every base group, while `mds-indexer` gets `fs:*` on it — and the indexer refuses to index it

**Fixed in `26a66b633`** - chosen criterion: a name ending in `-metadata` is a companion only when its data repository `R` exists in the catalog or in `metadata_search.repositories` - the existence of `R`, not the `lakefs.mds.init` marker the finding suggested, because the bootstrap already has that repository list, does not otherwise read commits, and requiring the marker would walk the companion's history on every refresh and still race an indexer that created the repository before its init commit. An ordinary `x-metadata` repository stays writable for its users and gets no `mds-indexer` grant.

**Where**: `pkg/mds/bootstrap/bootstrap.go:158-181` (`companionRepositories` keeps a name that
`icebergcatalog.IsMetadataRepository` accepts instead of mapping it to `<name>-metadata`),
`pkg/icebergcatalog/resolver.go:285-287` (`IsMetadataRepository`), `pkg/mds/config.go:407-411`
(`validateRepositories`: "a data repository name must not end in `-metadata`").

**What happens**: a pre-existing ordinary repository named `team-metadata` (a plausible name; nothing
stopped it before Metadata Search existed) is in the catalog, so `knownDataRepositories` returns it and
`companionRepositories` keeps it as a *companion*. Both policies then name it: `mds-indexer` gets `fs:*`
and `catalog:*` on it, and `mds-metadata-repo-protect` denies `fs:Write*/Delete*/Create*/Revert*/Update*/Import*`
and the catalog writes on it to every base group. The indexer, however, will never index it: as a data
repository its name is rejected by `pkg/mds/config.go` (and the indexer refuses a `*-metadata` repository
without its init marker as a foreign repository, `pkg/mds/indexer/repo.go`). Net effect: the indexer —
whose whole point is "never writes a data repository" (`bootstrap.go:191-194`) — has full write and delete
on an ordinary data repository, and `Admins`/`Developers` cannot write, commit to or delete it, with no
signal beyond their 403 and no way out except editing the catalog. The fix's own test pins the
behaviour (`bootstrap_test.go`, `TestKnownDataRepositories`: "legacy-metadata is a companion repository by
§3.3"), and §3.3 says such a repository "cannot be indexed" — it does not say the policies should treat it
as a companion.

**Evidence**: probe (`pkg/mds/bootstrap/probe_r2s_test.go`), real ACL service, `dataRepositories=["team-metadata"]`:
```
PROBE ordinary *-metadata repo "indexer write": fs:WriteObject mds-indexer -> allowed=true companions=[team-metadata]
PROBE ordinary *-metadata repo "indexer delete repo": fs:DeleteRepository mds-indexer -> allowed=true companions=[team-metadata]
PROBE ordinary *-metadata repo "operator write": fs:WriteObject operator -> allowed=false
PROBE ordinary *-metadata repo "operator commit": fs:CreateCommit operator -> allowed=false
PROBE ordinary *-metadata repo "operator delete repo": fs:DeleteRepository operator -> allowed=false
PROBE ordinary *-metadata repo "operator read": fs:ReadObject operator -> allowed=true
```

**Suggested fix**: treat a name that ends in the suffix as a companion only when it carries the indexer's
init marker (`lakefs.mds.init`, `pkg/mds/indexer/repo.go`), i.e. when it really is one; otherwise leave it
out of both policies (a repository the indexer cannot index has no companion to grant or protect). Test:
an unborn-ordinary repository named `x-metadata` stays writable for a guard group and is not granted to
`mds-indexer` (both are wrong today).

#### R2S-5 - MINOR: the vended-credentials write guard checks the requested address only, while the write-sign fix it is credited with also checks the resolved target

**Fixed in `500a00a37`** - a write `Vend` checks the namespace of the resolved target as well as the requested address; `TableCredentials` treats the `*ReservedNamespaceError` as a write refusal and falls back to the read session, so a redirect from an ordinary address vends read-only credentials and a direct write `Vend` is refused rather than downgraded.

**Where**: `pkg/icebergcatalog/creds.go:452-458` (`TableCredentials` calls `s.writableNamespace(addr)` on
the *requested* address), `:515-517` (`writableNamespace` = `RequireWritableNamespace(addr) == nil`),
`:408-441` (`Vend` resolves through `s.tableScope` and never re-checks the target), `:820-853`
(`newTableScopeResolver` takes root/location from `resolved.Target`); compare
`pkg/icebergcatalog/sign.go:522-535` (`writableTableLocation` refuses
`requireWritable(resolved.Target)`).

**What happens**: the MDS-SEC-1 fix states "the vended-credentials path already refused that write
(`creds.go` `writableNamespace`)". It refuses it only when the *caller's* address is already reserved.
A resolver that redirects an ordinary address into a reserved namespace — exactly the case
`TestRemoteSigningReservedNamespace` builds with `signRedirectResolver` — passes the requested-address
check, and `Vend` then takes the root and location of the redirected table, so the caller receives the
*write* session (`s3:PutObject`, `s3:DeleteObject`) for the reserved namespace's files. Only in-tree code
can install a resolver (`WithTableResolver`), so this is a defence-in-depth gap in the guard the fix
relies on, not an exploit of the shipped resolver.

**Evidence** (probe, `pkg/icebergcatalog/probe_r2s_test.go`, real `NewCredentialService` +
`signRedirectResolver` from `sign_test.go`, `credsTestVending()`, recording STS fake):
```
PROBE creds redirect: protected namespace of the redirect target "lakehouse-metadata@main.system" is reserved=true; requested address namespace is reserved=false
PROBE creds redirect: TableCredentials(lakehouse@main.inventory.books) err=<nil> credentials=1 elapsed=4ms
PROBE creds redirect: session prefix="s3://iceberg-sign-mds-4-ns/_iceberg/66666666-6666-6666-6666-666666666666" actions=[s3:DeleteObject s3:PutObject]
```
The same shape through the signing endpoint is 403 (`TestRemoteSigningReservedNamespace`, verified red
without the fix above).

**Suggested fix**: in `Vend` (or in `newTableScopeResolver`, which already has `resolved.Target`), refuse
`store.RequireWritableNamespace(resolved.Target)` for `OpWriteTableData` and fall back to the read
session, exactly as `sign.go`'s `writableTableLocation` does. Test: the `signRedirectResolver` +
write-credentials case returns a read session (or no write actions) — it returns `s3:PutObject` today.

#### R2S-6 - MINOR: the documented "extra statement" for an explicit non-`-metadata` storage namespace is deleted again by the policy repair at the next startup

**Fixed in `26a66b633`** - chosen option: the extra grant for an explicit non-`-metadata` namespace is an operator-owned policy attached to `MetadataSearchIndexers` (for example `mds-indexer-extra-storage-namespace`), and the refresher deletes only names this package owns - a base name or a numeric chunk suffix - so that policy survives every repair and restart.

**Where**: `pkg/mds/bootstrap/bootstrap.go:713-733` (`ensurePolicy`: any statement difference triggers
`WritePolicy(…, true)` and the warning "rewrote a metadata search policy whose statements did not match"),
`:780-792` (`statementsEqual` compares the whole list), `:280-333` (`IndexerPolicy` has the single
`fs:AttachStorageNamespace` pattern `namespace/*-metadata`); docs `docs/metadata-search.md:825-828` and
METADATA_SEARCH_DESIGN.md §8 ("An explicit `metadata_search.repositories.<R>.storage_namespace` that does
not itself end in `-metadata` … needs an extra statement").

**What happens**: an operator follows the documented workaround for a bucket-root/derived-elsewhere
namespace and adds the extra `fs:AttachStorageNamespace` (and `fs:CreateRepository`) statement to the
`mds-indexer` policy. On the next `lakefs-sso` start, `EnsurePrincipals` → `refreshPolicies` →
`ensurePolicy` sees statements that differ from the built-ins and replaces the policy with the built-ins,
so the grant is gone (one warning, no error); the indexer's next `CreateRepository` of `R-metadata` is 403,
the indexer cannot start, and the same happens after every restart. Only a *second* policy attached to
`MetadataSearchIndexers` survives, and neither the design nor the guide says so.

**Evidence**: probe (`pkg/mds/bootstrap/probe_r2s_test.go`, real ACL service, extra statement appended with
`WritePolicy(…, true)`):
```
PROBE operator extra statement: before_refresh=1 after_refresh=0 statements_before=9 statements_after=8
```
`TestEnsurePrincipalsRepairsDriftedPolicy` pins the repair itself; nothing says which drift is a repair and
which is an operator grant.

**Suggested fix**: decide and document: either the extra grant must live in a separate policy attached to
the group (state it in §8 and the guide, and add a test that such a policy survives `RoutesBuilder`), or
`ensurePolicy` keeps operator-added statements (replace only the companion-repository statements it
generates). Test: add the documented statement, run `RoutesBuilder` again, assert it is still in force (it
is gone today).

#### R2T-2 - MINOR: compaction collects the whole table in memory; peak heap is ~1.5 KiB per row, and the design's bound is O(table), not a batch

**Fixed in `d993d602e`** - `Compact` merges the per-file sorted streams with `container/heap` instead of collecting the table: one read batch per file plus one output batch, the per-file batch being `compactMergeRowBudget / files` (2 x 64 Ki rows, floor 256) set with iceberg-go's `WithArrowBatchSize`. A file written before `2daeca114` need not be sorted; a path-column-only pass detects it and it is read whole, sorted on its own and merged as a materialised run, so the fallback costs that file and not the table.

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

#### R2T-3 - MINOR: the rebuild/compaction "non-replayable" guarantee only holds when a delete file was removed

**Fixed in `d993d602e`** - `commit.retry.num-retries=0` is written at creation, re-asserted by every write's `SetProperties`, and repaired by `ensureNoReplay` with one metadata-only commit before the write (iceberg-go reads the budget from the metadata the transaction started on, not from the properties it stages). A write whose repair commit observed a moved branch returns the new `ErrStaleTable`, so the indexer reloads and retries instead of overwriting a commit it never read.

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

#### R2T-4 - MINOR: the new Spark row gate compares only the first `part-*` file and not the guide's snippet

**Fixed in `2af2532b7`** - the Spark gate concatenates every `part-*` file (`concat_part_rows`, which fails when there is none) and compares the guide's own four projected columns, including `to_json(user_metadata)`; `run-spark.sh --self-test` proves the multi-file and extra-row paths (7/7) without docker.

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

### NIT (3)

#### R2S-7 - NIT: the deny leaves `branches:SetBranchProtectionRules` (and `retention:*`) open, so a base-group member can stop the indexer permanently — the §3.3 promise that "user writes are blocked by a deny policy" does not cover it

**Fixed in `26a66b633`** - `branches:SetBranchProtectionRules` is denied on a companion repository. `retention:SetGarbageCollectionRules` and `retention:PrepareGarbageCollectionCommits` stay allowed, as this finding itself recommended: they cannot remove the live index without `fs:DeleteBranch`, which is denied, and an operator may legitimately set a GC policy for a companion repository.

**Where**: `pkg/mds/bootstrap/bootstrap.go:233-241` (deny actions are `fs:*`/`catalog:*` wildcards only),
`pkg/api/controller.go:2633-2656` (`SetBranchProtectionRules` authorizes `branches:SetBranchProtectionRules`
on `RepoArn(repo)` and blocks `STAGING_WRITE` and `COMMIT` on every matching branch),
`pkg/graveler/branch/protection_manager.go:47-70`, `pkg/auth/setup/setup.go:170-186`
(`RepoManagementFullAccess` = `ci:*`, `retention:*`, `branches:*`, `pr:*`, attached to `Admins` in the RBAC
template).

**What happens**: a member of `Admins`/`SuperUsers` can `POST /repositories/analytics-metadata/branch_protection`
with pattern `*`; the companion branch then refuses every commit, so the indexer fails every tick and
`/_health` stays 503 — the failure mode `f884a0627` added `ErrReadOnlyMetadataRepository` for. The deny's
own comment ("no principal in a base group may write … inside a companion repository") and §3.3's "user
writes are blocked by a deny policy instead" both claim this class is closed; `branches:SetBranchProtectionRules`
is a write of repository state that determines whether the indexer may write. It is loud, not silent, which
is why this is a NIT and not higher. `retention:SetGarbageCollectionRules` /
`PrepareGarbageCollectionCommits` are also allowed, but cannot delete the live index without deleting the
branch first (`fs:DeleteBranch` is denied), so I did **not** count them as a finding.

**Evidence**: probe (`pkg/mds/bootstrap/probe_r2s_test.go`), a guard-group principal with the RBAC admin
grants:
```
PROBE deny gap: guard-group admin fs:WriteObject on arn:lakefs:fs:::repository/analytics-metadata/object/forged.parquet -> allowed=false
PROBE deny gap: guard-group admin fs:DeleteRepository on arn:lakefs:fs:::repository/analytics-metadata -> allowed=false
PROBE deny gap: guard-group admin branches:SetBranchProtectionRules on arn:lakefs:fs:::repository/analytics-metadata -> allowed=true
PROBE deny gap: guard-group admin retention:SetGarbageCollectionRules on arn:lakefs:fs:::repository/analytics-metadata -> allowed=true
PROBE deny gap: guard-group admin retention:PrepareGarbageCollectionCommits on arn:lakefs:fs:::repository/analytics-metadata -> allowed=true
PROBE deny gap: guard-group admin pr:WritePullRequest on arn:lakefs:fs:::repository/analytics-metadata -> allowed=true
```
(`pr:WritePullRequest` alone cannot merge — `UpdatePullRequest`'s merge node adds `fs:CreateCommit` on the
destination branch, `controller.go:6179-6185`, which the deny covers.)

**Suggested fix**: add `branches:SetBranchProtectionRules` (and, if the owner agrees, the two
`retention:Prepare*` actions) to `metadataRepoProtectCatalogDeniedActions`, and say in §8 why
`retention:SetGarbageCollectionRules` itself stays allowed. Test: a guard-group member's
`SetBranchProtectionRules` on a companion repository is denied (allowed today).

#### R2S-8 - NIT: nothing tests that `RoutesBuilder` starts the refresher, so the production half of the one-minute guarantee can be deleted unnoticed

**Fixed in `26a66b633`** - `TestRoutesBuilderStartsPolicyRefresher` creates a repository after `RoutesBuilder` returns and requires the refresher it started to add it to both policy sets within a shortened interval; the same card also covers a shrinking set (stale chunks are deleted) and attaching a new chunk when the set grows past a boundary.

**Where**: `pkg/mds/bootstrap/bootstrap.go:426` (`startPolicyRefresher(ctx, deps, policyRefreshInterval)`);
`pkg/mds/bootstrap/bootstrap_test.go:984-1020` (`TestPolicyRefresher` calls `startPolicyRefresher` directly).

**What happens**: every refresher test drives the goroutine itself; no test goes through
`RoutesBuilder` → `startPolicyRefresher`, so removing the call (or replacing `ctx` with
`context.Background()`, which would also outlive the server) leaves the package green — verified below.
The same tests never cover the set *shrinking* (a repository deleted: `knownDataRepositories` is only ever
observed growing) or a refresh that fails (`pkg/mds/bootstrap/bootstrap.go:513-528`, the "logged and
retried" branch), so the two behaviours the design promises are untested.

**Evidence**: delete line 426, `go test ./pkg/mds/bootstrap/ -run
'TestRoutesBuilder|TestKnownDataRepositories|TestPolicyRefresher|TestEnsurePrincipals'` → `ok
github.com/treeverse/lakefs/pkg/mds/bootstrap 1.310s`.

**Suggested test**: `RoutesBuilder` with a small `policyRefreshInterval` (needs an injection point, e.g. a
package variable the test overrides) creates a repository in the catalog and requires both policies to
name it, and requires the refresher to stop when the context is cancelled; plus a delete-then-refresh case
asserting the statements shrink, and a failing-`WritePolicy` case asserting the retry.

#### R2S-9 - NIT: on an external auth service the accepted one-minute window is really one minute plus the auth cache TTL

**Fixed in `26a66b633`** - `ensurePolicy` purges the auth service's cache after a rewrite and after a create, and stale-chunk deletion purges after each delete, so an external auth service does not extend the refresh window by `auth.cache.ttl`; a service whose `Cache()` is nil keeps working.

**Where**: design §8 ("that window … is bounded by the refresher at one minute") vs
`pkg/auth/service.go:612-655` (`APIAuthService.WritePolicy` never calls `PurgePolicies`, unlike
`contrib/auth/acl/service.go:715-740`), `pkg/auth/service.go:1024-1050` (`Authorize` uses
`ListEffectivePolicies`, cached per user), `pkg/config/defaults.go:66-69` (`auth.cache.enabled` true,
`ttl` 20 s, `jitter` 3 s).

**What happens**: with `lakefs-sso` in external-auth mode (`auth.ui_config.rbac: external` →
`auth.NewAuthService` → `APIAuthService`), the refresher's rewrite is invisible to authorization until the
per-user effective-policy cache entry expires, so a newly created companion repository's deny (and the
indexer's grant) can take up to ~83 s to apply, not 60 s. The ACL service (the `lakefs-sso` SSO mode) does
purge, so this is the non-SSO/external deployment only.

**Evidence**: `git grep -n 'PurgePolicies()' -- pkg/ contrib/` lists the interface and the two
implementations (`pkg/auth/cache.go:39,105,134`) and five call sites, **all** in
`contrib/auth/acl/service.go` (564, 730, 799, 906, 929); `APIAuthService` has none, and
`pkg/auth/service.go:612-655` ends at `validateResponse`. `auth.cache.ttl` default 20 s plus 3 s jitter
(`pkg/config/defaults.go:66-69`).

**Suggested fix**: after a rewrite, call `deps.AuthService.Cache().PurgePolicies()` when the service
exposes a non-nil cache (guard the nil `Cache()` of a stub service with the `policyBackendAvailable`
style check), and state the residual cache window in §8. Test: with a cache-enabled service, a statement
rewritten by `refreshPolicies` is effective on the next `Authorize` (it is not today).

## Duplicates

The four cards overlapped in scope, so every pair that could be the same defect was compared. **No
finding duplicates another** — each of the 17 keys names a distinct defect with a distinct fix, and none
was dropped or merged:

- **R2S-1 / R2I-4** (same threat model, MDS-SEC-3, but two sides of it). R2S-1 is the read path: the
  catalog serves a companion table whose `lakefs.mds.repository` property the attacker wrote. R2I-4 is the
  write path: `initialise` adopts and commits a table the indexer did not create. Different code, and
  neither fix closes the other — an init-marker proof on the read side (R2S-1) does not stop the indexer
  committing a foreign table, and refusing the staging (R2I-4) does not stop the catalog serving a
  crafted `metadata.json` for a repository the attacker created. Kept separate, and cross-referenced.
- **R2S-7 / R2I-3** (same trigger state, branch protection on `R-metadata@main`). R2S-7 is the policy
  gap that lets an `Admins`/`SuperUsers` member set the rule at all; R2I-3 is the indexer's failure mode
  once the rule exists (a per-branch 403 on every tick instead of one repository-level error, and the
  TS-4 claim is wrong). Adding the deny action (R2S-7) does not make the indexer diagnose the state, and
  diagnosing it (R2I-3) does not close the deny. Kept separate, and cross-referenced.
- **R2S-2 / R2S-6** both concern `ensurePolicy`'s rewrite, but not the same defect: R2S-2 is the item
  size (the server will not start at ~560 repositories), R2S-6 is an operator-added statement being
  replaced by the built-ins. A chunked layout (R2S-2) would not preserve an operator statement, and a
  statement-preserving repair (R2S-6) would not bound the item. Kept separate.
- **R2T-1 / R2T-2** both concern the same `Compact` rewrite, but one is a correctness kill (the panic)
  and the other a cost regression (peak heap). Kept separate.

Two overlapping *checks* were deduplicated instead, since they produce no finding:
`request_timeout` / the `pkg/mds/table/catalog.go` request bound was covered by both the indexer and the
table card; it is listed once, under the table area, and the indexer area points at it. The
build/vet/lint/`pkg/mds/...`-test rows of the Verified tables are merged into one table above.

## Checked, no finding

### Server security — `pkg/mds/bootstrap`, `pkg/icebergcatalog`, `cmd/lakefs-sso`

**The exact-ARN rewrite (`5b997972e`) itself.** `catalogResources` spells namespace/table/view ARNs
(`bootstrap.go:183-188`), which are exactly the three resource shapes `pkg/icebergcatalog/authz.go:594-599`
authorizes, plus `permissions.RepoArn` for `fs:ReadRepository`; a probe of `wildcard.Match` shows the
shapes are *not* over-matching: `table/comp/*/*` does not match `table/comp2/…`, `repository/comp/*` does
not match `repository/comp2/…` nor the repository ARN itself (which has its own statement), and the
repo-level listing ARN `namespace/comp/` (namespace `""`) *is* matched by the trailing star. The `*`
matching is segment-agnostic on purpose and only the trailing part of a statement that already contains
the literal companion name, so the review's MDS-SEC-2 property (a data-repository key/branch/namespace
ending in `-metadata` being reached) is gone — confirmed both by the rebuilt pre-fix policy (Verified
table) and by `TestMDSPoliciesDoNotReachDataRepositories` passing now. The `MetadataNamespacePattern`
stays a pattern (`bootstrap.go:128-132`) and `fs:AttachStorageNamespace` is authorized only by
`CreateRepository` (`controller.go:2297-2305`), so its over-reach cannot grant object or branch access;
the indexer's *own* namespace-overlap check (`pkg/mds/indexer/repo.go`, design §3.3 `7ec4ebec7`) is what
keeps a pattern-chosen namespace from destroying a companion repository.

**The two §8 policies against real lakeFS ARNs.** `fs:CreateCommit`/`fs:WriteObject` on
`BranchArn`/`ObjectArn`, `catalog:*` on `Catalog*Arn`, and `fs:UpdateRepository` from
`controller.go:2573-2612` are all decided as the design says; `TestEnsurePrincipalsAuthorization`,
`TestMDSPoliciesDoNotReachDataRepositories` and `TestMetadataRepoProtectCoversImportActions` run through
the real `contrib/auth/acl` service (not a mock), so deny-beats-allow is a property of the service, not of
the test. The import decision (`9cf0869c8`): `ImportStart`'s node is `fs:WriteObject` +
`fs:CreateCommit` on the destination branch, `fs:ImportFromStorage` on the *source* namespace and
`fs:WriteObject` on the destination object (`controller.go:3350-3378`); `ImportCancel` is
`fs:ImportCancel` on `BranchArn(repo, branch)` (`controller.go:3489-3497`). `fs:Import*` therefore closes
cancel and was already covered for import-into-a-companion; no namespace statement is needed, and the
review's decision text matches the code.

**The refresher's lifecycle, context and concurrency.** `RoutesBuilder` runs from the builder loop in
`run.go:303-308` with `ctx` = `signal.NotifyContext` (`run.go:80`), i.e. the server's lifetime; the
goroutine closes `done` on `ctx.Done()` and on the degraded path (`bootstrap.go:501-545`), the ticker is
stopped, a failed list/refresh is logged and retried (`:513-528`), and `inForce == false` stops it
instead of warning every minute. `time.Ticker` drops ticks, so a slow catalog cannot pile up goroutines;
`knownDataRepositories` has no timeout of its own but is bound to the server context, and the only
production caller starts one refresher per process. Two `lakefs-sso` replicas derive the same sorted,
deduplicated statement list (`companionRepositories` sorts) and `ensurePolicy` writes only on a
difference, so they converge; `WritePolicy` has no compare-and-swap, so in the window where one replica's
list already has a new repository and the other's does not, the two can rewrite each other's policy and
purge the auth cache — transient, converging, and not worth a finding.

**Refresher pagination and the repository list.** `knownDataRepositories` pages with
`ListRepositories(ListRepositoriesLimitMax, "", "", after)`; the iterator is keyed by repository ID,
`SeekGE(after)` + `if record.RepositoryID == afterRepositoryID { continue }` is exclusive, and the break
conditions (`!hasMore || len(repositories)==0`) cannot loop (`pkg/catalog/catalog.go:679-742`). A
configured name that is not a valid repository ID is dropped with a warning, and `viper.GetStringMap`
lower-cases keys — harmless, because `validator.ReValidRepositoryID` is `^[a-z0-9][a-z0-9-]{2,62}$`
(`pkg/validator/validate.go:12`), the same rule `graveler.ValidateRepositoryID` enforces on creation.
Repositories longer than 54 characters are silently dropped by `companionRepositories` (their companion
cannot exist), which is correct.

**`RoutesBuilder` deps, degradation and startup.** A nil `Catalog` is `ErrMissingDeps` (the policies
would name nothing); `BasicAuthService.GetPolicy` returns `ErrNotImplemented`
(`pkg/auth/basic_service.go:428-430`), which `policyBackendAvailable` treats as the one degraded startup,
before `CreateUser` — so the administrator is not renamed (the existing test asserts exactly that); a
genuine error still propagates and stops the server. The idempotency contract holds: `ErrAlreadyExists`
is tolerated on user/group/membership/policy create and on attach, `EnsurePrincipals`' order (policies
before attachments) means an attachment never names a missing policy, and `statementsEqual`
(`bootstrap.go:780-799`) treats nil and empty conditions as equal, so a round-tripped policy is not
rewritten on every restart (`TestEnsurePrincipalsIdempotent` pins `CreatedAt`).

**`metadata_search` requires `iceberg.enabled` (`4aaf60cb3`).** `Config.Validate`
(`config.go:363-375`) is reached by every `LoadConfig` (`config.go:200-213`), which both `RoutesBuilder`s
and `pkg/icebergcatalog/bootstrap.go:85` call, so the invalid combination is fatal before any principal
or deny is installed; the only `MetadataSearchEnabled()` call sites are behind that validation
(`http.go:157,222`, `bootstrap.go:396`). The message names both keys and the reason.

**The write-sign refusal (`d2811fdcc`).** Every method `signingOperation` classifies as
`OpWriteTableData` (`PUT`, `POST`, `DELETE`, `PATCH`, `OPTIONS`, `sign.go:464-475`) goes through the
requested-address check (`:387`), and the resolved target is checked again before the redirected table is
read (`:522-535`), so the only address shape that resolves to `R-metadata` (`system.object_metadata`,
`resolver.go:455-463`) is refused twice; reads (`GET`, `HEAD`) keep signing and the location scoping is
unchanged. I tried to dodge it by naming a table of `R` whose location could sit in the companion's
prefix: `TableLocation`/`normalizeTableLocation` confine a table to `<R namespace>/_lakefs_iceberg`
(`location.go:101-151`), which is not the companion's `_iceberg/<uuid>` prefix, so a write sign for a
table of `R` cannot name a companion file. The remaining asymmetry is R2S-5.

**The companion-table proof's placement (`4aaf60cb3`).** `Store.Resolve` (`resolver.go:483-510`) loads the
target once before serving, so HEAD, metrics, vended credentials and remote signing are covered by
construction (`GetTable` does not recurse into `Resolve`, `store.go:315-348`); a resolver that omits
`Authz` gets the requested address (`:496-500`), and an incomplete target is `ErrInvalidResolution`. The
load happens before `ResolveAuthorized`'s second authorization (`:530-545`), which is a table-existence
oracle for a cross-repository `Authz` only after the caller was authorized on the requested address —
acceptable, and not reachable with the built-in resolver. The extra load is documented in design §5.1.

**`cmd/lakefs-sso` wiring.** Both builders run in registration order, the deny gets the same
`guardGroups()` as `iceberg-reserved-prefix-protect` (`main.go:76-89` and `:114-121`), the mds builder is a
no-op while `iceberg.metadata_search.enabled` is false, and `MetadataSearchRefMapper = refmap.New` is
assigned before `Execute` so the catalog sees it at mount time. `guardGroups()` loading
`datasets.LoadConfig()` twice (once per builder) is harmless.

### Indexer — `pkg/mds/indexer`, `pkg/mds/client`, `pkg/mds/{config,health,metrics,service}.go`

- **The bounded `pending` walk and its exact lag** (`branch.go:496-575`): the window keeps the oldest
  `limit+1` commits and only its tail can be the todo; `total` counts commits per page and drops the inclusive
  `stop_at` commit exactly once; `metStop` can only be true on the last page because `pkg/catalog`'s
  `foundAllCommits`/`logCommitsResult` (`pkg/catalog/catalog.go:1665-1690`) stop the walk *at* `stop_at` and
  truncate to `Amount`, so a page containing the stop commit never reports `hasMore`; the
  `total >= limit+pageSize` give-up is the documented boundary and not off by one — the server reads
  `Amount+1` commits to set `has_more`, so a position at depth `max_commits + page` is *inside* the pages read
  and is walked, and only depth `> max_commits + page` rebuilds. The lag after a rebuild is 0 because the
  position becomes the head, and after a failed apply the per-commit decrements leave the exact remainder
  (`TestMaxCommitsBoundsATick`, `TestPendingPagesTheLog`, `TestFailedBranchReportsThePositionItAdvancedTo`).
  The fake's paging (`pageOf`) and its `stop_at` dereference (`404` on a missing stop) match
  `pkg/catalog/catalog.go:1488-1497` and `logCommitsResult`, so the paging tests are not fake-only artefacts.
- **The vanished-position rebuild** (`branch.go:172-235`): a 404 is only taken as "the position is gone" after
  `positionGone` → `GetCommit(repository, position)` also answers 404, and `GetCommit` resolves a full commit ID
  (`pkg/api/controller.go:4080-4099` → `catalog.GetCommit` → `dereferenceCommitID`), so a 404 about the head, the
  repository listing race or a flaky intermediary still fails the tick
  (`TestNotFoundThatIsNotThePositionIsAFailure`). The recursion guard for a *double* transient 404 is weak, but a
  rebuild in that case yields correct rows at the head (`§4.7`), not corruption — I could not construct a wrong
  answer from it, only cost, so it is not reported separately.
- **Concurrent initialisation** (`repo.go:298-345`, `:127-160`): the staging area is shared server-side, so the
  loser's `Commit` finds nothing to commit and gets `ErrNoChanges`; `initScan` accepts the winner's run and the
  branch continues on top of the branch head (`TestConcurrentInitialisationCommitsOnce`,
  `TestDuplicateInitCommitsAboveTheAutomaticRootAreAccepted`, `TestInitMarkerOutsideTheRunIsStillRefused`). The
  `initScan` acceptance rule (`:216-290`) matches §4.8's two shapes, refuses a marker below a gap and a marker
  above a hand-made root, and is O(1) in memory (`TestInitialisationCheckKeepsTheHistoryOutOfMemory`).
- **`request_timeout`** (`config.go:163,250-252,296-298,350-353`; `indexer.go:728-745`; the catalog half —
  `pkg/mds/table/catalog.go:151-215` — is listed once under the table area, to avoid duplicating that card's
  entry): 60 s default, `0`/negative refused at startup, `0` in `indexer.Options` means "the default", not "no
  bound"; the API client uses `http.Client.Timeout` (covers body reads). A timeout is not typed as 404/409
  (`checkStatus` is only reached on a response), so it is never mistaken for a missing position or a conflict,
  is not retried by the client, and fails the branch or the repository. A timeout *after* the server committed
  the staging or the lakeFS commit lands exactly in §4.4's recovery windows (row 1 via the staged property, row
  4 via `ensureTag`), which I traced through `recoverStaging` and `ensureTag`; data-plane writes are iceberg-go
  IO and are documented as out of scope. No long operation in this area (a 1000-entry diff page with
  `include_right_stats`, a 50 000-entry manifest staging commit) plausibly exceeds 60 s at the documented
  defaults.
- **Per-tick catalog reuse** (`branch.go:232-240`, `iceberg.go:110-120`): `Table.Refresh` calls the handle's own
  catalog (`iceberg-go table/table.go:225-250`), so a tick pays one `/v1/config` and keeps one connection pool;
  `TestReloadReusesTheBranchCatalog` counts exactly 1 on an in-process lakeFS for a two-commit tick and fails at 3
  when reverted. `Refresh`'s extra UUID check (`validateTableUUID`) only rejects a table that was dropped and
  recreated under the handle, where "load a new table handle" is the right advice and the next tick does exactly
  that.
- **Health/lag on deleted and unfollowed branches** (`indexer.go:496-536`, `:600-635`; `health.go:188-215`): the
  `/_health` entry and the lag series of a branch that is no longer followed are both cleared whatever
  `delete_branches` says; the cleanup is derived from the previous tick's followed set, so it also reaches a
  branch that failed before its mirror existed; a cancelled tick ends `in_progress` without inventing an outcome
  (`CancelTick`), and `BeginTick` deliberately keeps the previous error. `lag_commits: -1` is published for a
  failure before the count and read back from durable state (`TestFailedBranchReportsTheRecordedPosition`,
  `TestDeletedBranchLeavesHealthWithoutDeletingTheMirror`, `TestCancelledTickIsNotLeftInProgress`).
- **Compaction config semantics** (`config.go:109-126,366-380`, `indexer.go:418-441`): `0` disables
  `max_delete_files`/`every_commits`/`tag_retention`, a negative value is refused with one error per key, and the
  200/500/0s defaults are applied only when the key is absent (`viper.IsSet`), so an explicit `0` survives
  `LoadConfig` and reaches `compactIfNeeded`'s `> 0` guards. The trigger order (free delete-file check first),
  `indexedSinceCompaction`'s durable count and the "compaction adds no tag" rule are unchanged and still hold.
- **The stricter fake vs real lakeFS** (TS-3/TS-10): `Commit`/`ResetBranch`/`HardResetBranch` return a typed 404
  for a missing branch or repository in the same order as graveler (`pkg/graveler/graveler.go:2245-2257`,
  `:2581-2605`, `:2637-2650` — protection and `ReadOnly` before the branch lookup, `getRepository` before all of
  them), the default-branch delete is 403 (`ErrDeleteDefaultBranch`, `pkg/api/controller.go:3196-3200`), the
  `force` flag never overwrites a tag, and `FindMergeBase` reproduces `pkg/graveler/ref/merge_base_finder.go`
  including the generation/newest-first queue tie-break (`commit_generation_priority_queue.go:24-32`) and
  `ErrNoMergeBase` for an unrelated history (`pkg/graveler/graveler.go:3530-3532`). `LogCommits` with
  `FirstParent: false` is breadth-first and therefore not graveler's order, which the code states and no
  production path observes.
- **`pkg/mds/client`**: unchanged by these commits; re-read for the paths the fixes lean on — `checkStatus` never
  turns a transport error into 404/409, `morePage` refuses "more" without a cursor, the log/diff cursors are the
  last returned ID/path, and `errorMessage` is bounded. No finding.
- **`pkg/mds/service.go`**: only the `RequestTimeout` line changed in this range; `tickUntilDone` still starts one
  tick at a time, abandons (rather than waits for) an in-flight tick on stop, and the HTTP server keeps its 10 s
  read-header and shutdown bounds. The abandoned goroutine writes only through mutex-guarded sinks, so `-race`
  over the package is clean. No finding.
- **`pkg/mds/metrics.go`**: only the dead stage constants were removed; all seven §9 series remain and the
  `LagUnknown` value is documented. No finding.
- **Crash/restart safety of the new fallbacks**: `rebuildAtHead` → `rewrite` discards staging before the hard
  reset (`graveler.ResetHard` keeps it), and a crash between the hard reset and the rebuild is recovered by
  `startBranch` (head metadata has no `lakefs.mds.source-commit` ⇒ `last == ""`), which re-runs the rebuild from
  the staged position instead of double-applying; the 409 restart path is unchanged and still bounded at
  `conflictRetries = 3`. No idempotency hole found.

### Table and refmap — `pkg/mds/table`, `pkg/mds/refmap`

- **TB-1's fix is correct for rows.** `Compact` removes every data file of the scan plan and every
  delete file of the snapshot's manifest list, then appends the scanned rows; the appended data files
  get a higher sequence number, so a removed delete file can never apply to them, and a delete file
  that applies to nothing is simply dropped. Probe: 12 data files / 9 delete files / 1 596 rows
  survive `Compact`, a second `Compact` and a following delta exactly, with no duplicate `path` and
  `total-delete-files == 0`; `TestCompact` and `TestRebuild` fail without the fix (revert probe above).
  `ReplaceFiles` re-validates membership against the live snapshot
  (`table/transaction.go:1841-1985,2039-2048`), so a stale file list fails loudly instead of dropping a
  live file, and a `ctx`-driven race is bounded by the CAS (`assert-ref-snapshot-id`).
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
- **`pkg/mds/table/catalog.go` request bound (`7f952279b`)** — this is also the catalog half of IX-2 and
  is listed only here: the transport factory is used for both the `/v1/config` exchange and the session
  client (pinned by `TestNewCatalogRequestTimeout`), the deadline is per request and is released by
  `Close` on the body, and it cannot bind the data path (iceberg-go writes through its own `FileIO`); a
  commit that times out leaves an unknown outcome that §4.4 recovery handles. No finding.
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

### esti — `esti/mds_test.go`, `esti/ops/mds-engines`

- **Esti TS-7 (compaction/tag retention).** Reviewed statically (not run by the table card):
  `waitIndexed` reads `/_health`, which the indexer updates only after `compactIfNeeded` returns
  (`pkg/mds/indexer/indexer.go:584`, `pkg/mds/indexer/branch.go:164`), so `log[0]` is the compaction
  commit or the case fails; `log[1].Id == tagCommit` would fail if the delete files survived (a second
  compaction commit is inserted) and the `lakefs_mds_compactions_total == 1` assertion cannot pick up
  another branch's compaction (the fixture indexes one branch). Sizes/rows asserted match the four
  uploads. The esti-run card later ran this case: it passed in the 32/32 subtest run.
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
  The esti run's post-run checks (no `lakefs-sso`, `mds run`, OIDC process; no `mds-*`/spark/trino
  container) confirm this on a live run.

## Limits of this consolidation

- The esti suite was run once, on `df9744488`, not on `4bcd54852`; the only commits between them are the
  review cards, which touch no product, test, design or doc file (verified: every card commit adds files
  under `review/` only).
- The DynamoDB backend was not available, so R2S-2's 400 KiB failure is derived from the stored value
  size plus DynamoDB's documented per-item limit, not from a live write.
- Both MAJORs were confirmed by reading the code and the on-disk module sources (arrow-go v18.8.0,
  iceberg-go v0.7.0); their probes were not re-run, because this card may not add a test file.
- This card changed no code, test, design or doc file, and did not touch `MDS_REVIEW.md`.
