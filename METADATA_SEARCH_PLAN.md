# Metadata Search for lakeFS OSS — implementation plan

Design: [`METADATA_SEARCH_DESIGN.md`](METADATA_SEARCH_DESIGN.md) (section numbers below refer to it). Option **A**.
Option B (design section 11) is not scheduled.

## Branching

All tasks run as Kanban cards with base `feature/iceberg-catalog` and auto-commit (the path the board already uses).
No other cards are in flight there, so `pkg/icebergcatalog` ownership is not contended. A split to
`feature/metadata-search` can be done after M7 if wanted.

File ownership rules on top of each task's list:
- `cmd/lakefs-sso/main.go` is edited by M1, M5a and M5b only, which run in that order (Kanban links).
- `go.mod` / `go.sum` are edited by M3 only.
- `docs/iceberg.md` is edited by R10-2 and M7 only (M7 runs after R10-2 through M6).

## Status

| Task | State |
|---|---|
| R10-1 catalog hook (design 5.1–5.4) | **Done**, commit `80f6927e2` |
| R9-1 tenant header refusal, OSS-only docs | Done, `a27f0edf5` (not part of this feature, listed so it is not redone) |
| R10-2 `/mds/iceberg/api` alias | Done, `d690b14f3` |
| M1 config, subcommand, health/metrics | Done, `e3e42d240`; config moved into the shared file in `f2b6d1a45` |
| M2 lakeFS client layer | Done, `b4d01018a` |
| M3 spike and table writer | Done, `9483ca0e8`; design aligned in `350f764c2` |
| M4 indexer core | Done, `0f7ac16f4`; defects found by M6 fixed in `e0940e964` and `99b48ebf0` |
| M5a ref mapper and server wiring | Done, `823cd8542` |
| M5b bootstrap and deny policy | Done, `4e552e9d9` |
| M6 esti end to end | Done, `547707a1e`; CI job `metadata-search-system-tests` in `.github/workflows/esti.yaml`. The CI job's missing `ESTI_SETUP_LAKEFS=false` is fixed in `71e390816`. 100k backfill timed in `59828bcd1`: setup 5m46.159s (import 5m46.1s of it), first-indexed 3.212s after the indexer started (31 135 objects/s, one rebuild, 100 000 rows; PyIceberg and DuckDB both count 100 000); CI stays at `ESTI_MDS_BACKFILL_OBJECTS=10000`. The `system`-of-`R` write refusal (design 9) is an esti case since `8ae229ec2`, passing on both mounts |
| M7 docs | Done, `68df447e1`; the Spark and Trino snippets it left unrun were run and fixed in `c659fb08b` |
| Compaction trigger from durable state (design 4.9) | Done, `7f06dad27`: `every_commits` is counted from the first-parent log of `R-metadata@b` (indexing commits since the last compaction commit), so a restart or a second replica neither defers nor advances a compaction; `max_delete_files` is unchanged and still checked first |
| M7 engine recipes rerun (Spark, Trino) | Done, `c659fb08b`: Spark 3.5.5 with Iceberg 1.8.1 and Trino 484 return the same rows and the same `lakefs.mds.source-commit` through `/mds/iceberg/api` and `/iceberg/relative_to`; `docs/metadata-search.md` carries only snippets that ran; harness in `esti/ops/mds-engines/`. PyIceberg and DuckDB were not re-run there (they stay covered by `esti/mds_test.go`) |
| Vended credentials (design 4.6, risk "no remote signing") | **Done**. `pkg/mds/table/vended_test.go` (`de5f81aac`) runs the indexer's create + ApplyDelta with `metadata_search.storage` empty against SeaweedFS and a fake STS, and passes. It needed two `pkg/icebergcatalog` fixes, made on `feature/iceberg-catalog` (`09ee4107c`) and merged here: load and create answers carry `storage-credentials`, because iceberg-go v0.7.0 enters its vended mode only then and uses the credentials endpoint only to refresh; and load, create and the credentials endpoint vend a write session on a branch for a caller with `catalog:WriteTableData` (a read session otherwise). Not tested against real AWS STS |
| Audit fixes (design 3.1, 3.3, 4.1–4.9, 5.5, 8) | **Done**, each commit carries the test that fails before its fix and its verification summary. `7ec4ebec7` checks the `R-metadata`/`R` namespace overlap on every tick, not only at creation, so an existing overlapping companion repository is refused instead of indexed. `39486c0d2` makes the table's field-ID constants the created table's IDs (the create request is built in the created pre-order, so the catalog's renumbering is a no-op) and pins every ID in a test; readers still take IDs from the loaded table. `451bca5a1` attaches `mds-metadata-repo-protect` to the caller's `guardGroups()` (so `datasets.immutable_guard_groups` is covered) and drops `MetadataSearchIndexers` from it with a warning. `383bf37b5` discards staging before a rewrite (`ResetHard` keeps it), takes the full-listing fallback on the server's unmapped "no merge base" 500, treats a recorded position reachable only through a merge's second parent as a rewrite, detects foreign staged changes with a branch diff, reloads the table before step 3, and bounds 409 restarts at 3. `aab9216aa` repairs only a missing or mispointed tag (a tag on the pre-compaction commit stays), keeps current-position tags in retention, never deletes `R-metadata`'s default branch, and removes deleted or no-longer-matching branches from `/_health`. `1cb131f13` stops reading the diff at `rebuild_threshold`+1 and makes no `statObject` call for a rebuilt commit, and completes the stat fallback (absent right-side stat, missing size; `content_type` and user metadata excluded). `30748d488` deletes a stale `mds-<C>` tag before creating it, because lakeFS never overwrites a tag even with `force`. The esti audit card `91b30f485` proves the branch seed from the `mds-<A>` tag through R-metadata's own commit log and runs all four engine recipes (PyIceberg and DuckDB on both `/mds/iceberg/api` and `/iceberg/relative_to`); it reported no wrong snippet in `docs/metadata-search.md`. |
| Review fixes (MAJOR) | **Done**, six commits on top of the consolidated review `e2333be26` (0 BLOCKER, 7 MAJOR, 14 MINOR, 10 NIT); each commit carries the test that fails before its fix and a verification summary, and `MDS_REVIEW.md` records each finding's status. `6440e2f13` bounds the `pending` walk to `max_commits` plus one page, so a branch farther behind is rebuilt at the head and `lag_commits` stays exact, and rebuilds a recorded position that no longer exists in `R` once `GetCommit` also answers 404 (IX-1 with IO-2/TS-2, TS-1; the paging test IX-5). `7f952279b` adds `metadata_search.request_timeout` (default 60s) and bounds every indexer HTTP request on both production clients, so a stalled request fails the branch or the repository and `/_health` turns 503 (IX-2/TB-3). `db8172798` makes the `mds: init` commit `allow_empty=false` and accepts a contiguous run of init commits, so a concurrent initialisation adopts the winner's commit instead of refusing the repository forever, and streams the init scan (IO-1, TS-5). `e3083e279` removes every delete file of the snapshot on rebuild and compaction, enumerated from the manifest list, so a delete file orphaned by a rebuild no longer latches the compaction trigger (TB-1). `d2811fdcc`, made on `feature/iceberg-catalog` and merged in `224fc82d3`, refuses a remote-signature write into a reserved namespace, on the requested and on the resolved address (MDS-SEC-1). `5b997972e` names the companion repositories explicitly in both §8 policies instead of the `*-metadata` wildcard, adds a one-minute policy refresher, and adds `fs:Update*` to the deny (MDS-SEC-2, MDS-SEC-5). |
| Review fixes (MINOR/NIT) | **Done**, six commits on top of the MAJOR fixes; each carries the test that fails before its fix and a verification summary, and `MDS_REVIEW.md` records every MINOR and NIT finding as `Fixed in <commit>` - none is left open. `4aaf60cb3`, made on `feature/iceberg-catalog` and merged in `a397ed971`, makes the catalog serve a resolved virtual table only when its `lakefs.mds.repository` property names the data repository and refuses `metadata_search.enabled` without `iceberg.enabled` (MDS-SEC-3, MDS-SEC-4). `98f035d34` retires the `/_health` entry and lag gauge of a branch that is no longer followed whatever `delete_branches` says, publishes the recorded position and an unknown lag (`-1`) for a branch that fails early, pins "in progress does not mask a previous error" and the cancelled tick, makes `0` mean "disabled" for `tag_retention`/`compaction.*` with negatives refused at startup, and drops the dead stage/health seams (IO-3…IO-7). `f884a0627` reloads through `Table.Refresh` instead of a new REST catalog per commit, covers the partial-write recovery case, prints `repository@branch` in the five messages, fixes the `SnapshotCount` comment, makes the fake answer what lakeFS answers on a missing branch or repository and on a read-only repository (a read-only `R-metadata` now fails the repository once per tick), and gives the fake `FirstParent` and graveler's best-common-ancestor merge base (IX-3, IX-4, IX-6, IX-7, TS-3, TS-4, TS-10). `9cf0869c8` adds `fs:Import*` to the deny, the owner's §8 decision on the import actions (the import half of MDS-SEC-5). `2daeca114` sorts compaction output into the declared `path ASC` order and refuses an unsorted rebuild source, drops `Compact`'s discarded options, and asserts the ref mapper's resolver at compile time (TB-2, TB-4, TB-5). `866d4e85c` closes the four findings that needed the esti suite and the engine harness: the Spark and Trino scripts capture the rows, require the three expected `orders/` rows and diff both endpoints with the pinned image digests; new `TestMetadataSearchCompactionAndTagRetention` and `TestMetadataSearchBranchDeletion` cases run §4.9 and §4.8 end to end; the restart case holds the companion branch's lakeFS commit so the kill lands inside the §4.4 crash window deterministically; and a filtered-scan step exercises the guide's `row_filter` and `WHERE … ORDER BY … LIMIT` through all four engine/mount recipes (TS-6, TS-7, TS-8, TS-9). |

## Tasks

Each task owns the listed files, and no other task edits them. Every task ends with the verification rules below,
and its commit message includes the commands it ran and their output summary.

### R10-2 — `/mds/iceberg/api` alias (design 5.5)
- Files: `pkg/icebergcatalog/http.go`, `pkg/icebergcatalog/http_test.go`, `docs/iceberg.md` (Metadata Search section).
- Do: when `cfg.MetadataSearchEnabled()`, mount the same handler as `/iceberg/api` at `/mds/iceberg/api`. Disabled
  mode mounts nothing at the alias: the prefix stays unclaimed and the core router's placeholder answers it with
  **501** (`pkg/api/services.go` `notImplementedMounts`, mounted by `cmd/lakefs/cmd/run.go` after the extra-routes
  builders), not a plain 404. The `/iceberg/api` prefix keeps the catalog's own 501 answer.
- Done when: test shows `GET /mds/iceberg/api/v1/config` and `POST /mds/iceberg/api/v1/oauth/tokens` behave
  exactly like the `/iceberg/api` ones when enabled, and are 501 when disabled.

### M1 — config, subcommand, health/metrics skeleton (design 4.1, 7, 9)
- Files: `pkg/mds/config.go`, `pkg/mds/config_test.go`, `pkg/mds/service.go` (run loop with no-op tick),
  `pkg/mds/health.go`, `pkg/mds/metrics.go`, `cmd/lakefs-sso/mds.go`, one `AddCommand` line in `cmd/lakefs-sso/main.go`.
- Do: viper-based load of `metadata_search` with defaults and every validation of design 7 (table test per rule);
  `lakefs-sso mds run --config`; HTTP server on `listen_address` with `/_health`, `/metrics`, `/_pprof/`; graceful stop.
- Done when: bad configs fail with the documented message; a valid config starts, serves the three endpoints, stops on SIGTERM.

### M2 — lakeFS and catalog client layer
- Files: `pkg/mds/client/lakefs.go` (wraps `apigen.ClientWithResponses`: branches, logCommits, diffRefs with
  `include_right_stats`, listObjects, statObject, getCommit, commit, createTag, getTag, createBranch, deleteBranch,
  resetBranch, hardResetBranch, findMergeBase, createRepository, getRepository), `pkg/mds/client/lakefs_test.go`
  with `httptest`. No `go.mod` change.
- Pattern to follow: `pkg/icebergcatalog/cleanup/apiclient.go` (paging, error mapping).
- Done when: every call is unit-tested against a fake server, including pagination and 404 → typed not-found error.

### M3 — spike then table writer (design 3.1, 4.3 step 3, 4.6)
- First hour, a spike test: against an in-process lakeFS test server (`pkg/api` testutil) with the catalog enabled and
  the local block adapter (iceberg-go `file://` IO), create the table, `Delete` (MOR) + `Append` + `SetProperties` in
  one transaction, commit, read it back with iceberg-go scan. If MOR position deletes are rejected by the catalog's
  staging (`files.go` walks ADDED manifest entries, so delete files should be staged too), fix that in a separate
  R10-x task on `feature/iceberg-catalog`, not here.
- Files: `pkg/mds/table/catalog.go` (`rest.NewCatalog` for `<endpoint>/iceberg/relative_to/<repo>.<branch>/api`
  with basic-auth credentials and `storage` props via `rest.WithAdditionalProps`), `go.mod`/`go.sum` if needed,
  `pkg/mds/table/schema.go` (fixed schema, field IDs, properties), `pkg/mds/table/rows.go` (Arrow builder),
  `pkg/mds/table/writer.go` (`ApplyDelta(ctx, tbl, upserts, deletes, sourceCommit)`, `Rebuild(ctx, tbl, rowsIter, sourceCommit)`,
  `Compact`), tests.
- Done when: unit tests cover delta, empty delta (property still moves), chunked `IN` (>10k paths), rebuild, compaction,
  and a scan returns exactly the expected rows.

### M4 — indexer core (design 3.2, 3.3, 4.2–4.5, 4.7, 4.8)
- Files: `pkg/mds/indexer/repo.go` (create `R-metadata`, namespace rules, init commit), `pkg/mds/indexer/branch.go`
  (tick per branch, recovery table of 4.4, backfill 4.5, rewrite 4.7, lifecycle 4.8), `pkg/mds/indexer/changes.go`
  (diff → change set, rebuild threshold), `pkg/mds/indexer/maintenance.go` (compaction, tag retention), tests with
  fakes of the M2/M3 interfaces, and the tick hook-up in `pkg/mds/service.go` (M1's file, free by then).
- Done when: tests cover each row of the 4.4 recovery table (inject a crash after each step), seed from an
  ancestor tag, full backfill, first-parent merge handling, hard-reset rewrite, branch delete, namespace
  derivation/overlap refusal, `max_commits` bound per tick.

### M5a — ref mapper and server wiring (design 6)
- Files: `pkg/mds/refmap/refmap.go`, `pkg/mds/refmap/refmap_test.go`, `cmd/lakefs-sso/main.go`
  (one line: `icebergcatalog.MetadataSearchRefMapper = refmap.New`).
- Do: `MapRef` per design 6 (full ID, short ID, tag, `main~1`, unindexed → `NoSuchTableException`).
- Done when: unit tests for the mapper; a test wiring `NewMetadataSearchResolver` with the mapper shows a tag of `R`
  resolves to the `mds-<id>` ref of `R-metadata`.

### M5b — bootstrap of principals and deny policy (design 8)
- Files: `pkg/mds/bootstrap/bootstrap.go`, `pkg/mds/bootstrap/bootstrap_test.go`, `cmd/lakefs-sso/main.go`
  (one line: `AddExtraRoutesBuilder(bootstrap.RoutesBuilder)`, a builder that mounts no route).
- Do: user/group/policies per design 8, idempotent, degrades on `BasicAuthService` like
  `icebergcatalog.EnsureReservedPrefixProtectPolicy`; runs only when `iceberg.metadata_search.enabled`.
- Done when: unit tests cover create, repair of a drifted policy, idempotent rerun, degraded auth service.

### M6 — esti end to end (design 9 security tests)
- Files: `esti/mds_test.go`, `esti/ops/docker-compose-mds.yaml` (or the existing compose plus a service),
  CI workflow job if esti jobs are per file.
- Setup: lakefs-sso with `iceberg.enabled`, `iceberg.metadata_search.enabled`, SeaweedFS as S3 (recipe in commit
  `057bfb935`), indexer with `storage` static keys, `period: 2s`.
- Cases: upload/commit → rows visible via PyIceberg and DuckDB (`relative_to` and `/mds/iceberg/api`); overwrite
  changes `etag`/`commit_id`; delete removes row; S3-gateway upload has `X-Amz-Meta-*` keys; new branch seeded;
  merge; tag and short-SHA reads; hard reset; user without `R` access → 403; user with only `R-metadata` access
  → 403 through the virtual table; non-indexer write to `R-metadata` → 403; restart of the indexer mid-run is
  harmless; 100k-object backfill timed and reported (not a pass/fail gate).
- Done when: all cases pass locally (`make esti` subset), and the run log is in the commit message.

### M7 — docs
- Files: `docs/metadata-search.md`, link from `docs/iceberg.md`, sample config in `docs/reference/configuration.md`.
- Content: setup (server flag, principals, credentials, indexer config, running the subcommand), engine snippets
  (PyIceberg, DuckDB, Spark, Trino) for both endpoints, column semantics and deviations (3.2), freshness check,
  limits (name length, `-metadata` names, bucket-root namespaces, first-parent only, Azure/GCS), permissions caveat,
  maintenance (`lakectl iceberg cleanup`, GC, tag retention), option B outlook.
- Done when: every config key and metric in the design appears, and every snippet was run in M6.

Order (Kanban links): R10-2 and M1 start at once. M2, M3 and M5a wait on M1. M5b waits on M5a. M4 waits on M2
and M3. M6 waits on M4, M5b and R10-2. M7 waits on M6. At most four cards are ever in progress together.

## Verification rules (every task)

- `GOTOOLCHAIN=auto go build ./...`, `go vet ./pkg/mds/... ./cmd/lakefs-sso/...`,
  `go test -count=1 -race` on touched packages, `go tool golangci-lint run` on touched packages.
- Catalog tasks also run `go test -count=1 -race ./pkg/icebergcatalog/...`.
- M6 needs docker (colima) and SeaweedFS. A task that cannot run something says so in its commit message.
- No edits to generated files (`pkg/api/apigen`, mocks); no new swagger endpoints are needed.

## Risks

- **Delete files and catalog staging:** must be proven in M3's spike. Fallback: copy-on-write deltas (`Overwrite`
  with `In("path", …)` filter), which are slower on large tables but use only data files.
- **`include_right_stats` is experimental:** M2 tests it, and the indexer falls back to `statObject`.
- **No remote signing in iceberg-go:** deployments without credential vending must give the indexer object-store
  keys (`storage`). Document it, and test both modes where possible.
- **Cross-repo read authorization:** landed and tested in R10-1. M6 repeats the negative cases end to end.
- **Backfill time on large repositories:** measured in M6.

## Option B backlog (later)

B1 query service pod with DuckDB, B2 `iceberg_query` config and audit log, B3 results location and lifecycle,
B4 UI tab and API, B5 statement allow-list and limits. See design section 11.
