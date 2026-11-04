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
| everything below | not started; no Kanban cards yet |

## Tasks

Each task owns the listed files, and no other task edits them. Every task ends with the verification rules below,
and its commit message includes the commands it ran and their output summary.

### R10-2 — `/mds/iceberg/api` alias (design 5.5)
- Files: `pkg/icebergcatalog/http.go`, `pkg/icebergcatalog/http_test.go`, `docs/iceberg.md` (Metadata Search section).
- Do: when `cfg.MetadataSearchEnabled()`, mount the same handler as `/iceberg/api` at `/mds/iceberg/api`. Disabled
  mode keeps answering `/mds/iceberg/api` with nothing (unmounted, plain 404).
- Done when: test shows `GET /mds/iceberg/api/v1/config` and `POST /mds/iceberg/api/v1/oauth/tokens` behave
  exactly like the `/iceberg/api` ones when enabled, and are 404 when disabled.

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
