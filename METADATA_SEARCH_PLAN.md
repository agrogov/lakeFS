# Metadata Search for lakeFS OSS — implementation plan

Design: [`METADATA_SEARCH_DESIGN.md`](METADATA_SEARCH_DESIGN.md). Delivery option **A** (indexer + catalog resolution).
Option B (UI tab + query service) is documented in the design (section 9) and is not scheduled.

## Branching

1. **Here, on `feature/iceberg-catalog`:** M0 only (the catalog hook, task R10-1), because it changes
   `pkg/icebergcatalog`, which this branch owns.
2. **Dedicated branch `feature/metadata-search`**, cut from `feature/iceberg-catalog` after M0 lands: M1 to M6.
   These two documents move to that branch as the first commit.

## Milestones

| # | Milestone | Content | Files | Done when |
|---|---|---|---|---|
| M0 | Catalog hook | `TableResolver`, `RefMapper`, virtual table, authz on the data repo, read-only `system`, `iceberg.metadata_search.enabled` | `pkg/icebergcatalog/**`, `docs/iceberg.md` | unit and conformance tests green; default behaviour unchanged |
| M1 | Skeleton | `pkg/mds` config (`metadata_search`), validation, `lakefs mds run` subcommand, health and metrics endpoints, `mds-service-user` bootstrap | `pkg/mds/config*`, `cmd/lakefs/cmd/mds.go`, bootstrap | service starts, refuses bad config, creates the user |
| M2 | Metadata repo | create `R-metadata`, mirror branches (exact and `prefix-*`), create `system.object_metadata` through the catalog, `state.json` | `pkg/mds/repo*` | branches mirror, table visible on each |
| M3 | Indexer core | commit walk, diff, stat, row building, Iceberg commit per source commit, CAS on `state.json`, crash replay | `pkg/mds/indexer*` | a commit sequence on `R` is searchable; replay is idempotent |
| M4 | Ref mapping | `RefMapper` for commits and tags from `lakefs.mds.source-commit` summaries; registered into the catalog | `pkg/mds/refmap*` | tag and commit queries return the right snapshot |
| M5 | Verification | esti test with PyIceberg and DuckDB on SeaweedFS (recipe in commit `057bfb935`): write, delete, branch, tag, merge, permission cases | `esti/mds_test.go` | all esti cases pass; user without access to `R` gets 403 |
| M6 | Docs | `docs/metadata-search.md`: config, queries, limits, S3 gateway key casing, permission caveat, option B outlook | `docs/**` | reviewed |

## Verification rules (every milestone)

- `GOTOOLCHAIN=auto go build ./...`, `go vet`, `go test -count=1 -race` on the packages touched, `golangci-lint`.
- M5 needs docker (colima) and SeaweedFS as the S3 store.
- Each task commits with its passing output in the message; a task that cannot run something says so.

## Risks to settle early

- `iceberg-go` write capability (design open question 2): spike in M3's first hour; copy-on-write is the fallback.
- Cross-repo read authorization (design 5.3): reviewed before M0 merges; negative tests are part of M0.
- Backfill time on large repositories: measure in M5 with a generated 100k-object repository.

## Option B backlog (for later)

B1 query service pod with DuckDB, B2 `iceberg_query` server config and audit log, B3 results prefix handling,
B4 UI tab and API, B5 query limits and tests. See design section 9.
