## esti run

**HEAD:** `df97444884b1e09a9b6dcb992b6c554497d3df46` (`df9744488`) — *docs: record the MDS review MINOR/NIT fixes*
Branch `feature/metadata-search`, working tree clean before and after the run. Run on 2026-11-05
(15:30–15:50 CET; the step logs record UTC), report-only: no code, test or doc was changed.

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
|---|---|---|---|---|
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
