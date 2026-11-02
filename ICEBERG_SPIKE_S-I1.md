# Spike S-I1 — iceberg-go update and requirement coverage

**Task:** I0-1 · **Plan refs:** ICEBERG_CATALOG_PLAN.md §4.2, §2.1, §11 (I-0) ·
**Blocks:** I2-3 (commit path)

## Verdict

**Covered as-is. Nothing has to be implemented locally for updates or requirements.**

`github.com/apache/iceberg-go` v0.7.0 decodes and re-encodes **23 of 23**
`TableUpdate` actions, decodes and re-encodes **8 of 8** `TableRequirement` types, and
applies every one of the 23 in a realistic commit body. The commit path of I2-3 can
hand the request body straight to iceberg-go and keep its own code for the parts
iceberg-go does not ship: the `metadata.json` write, the CAS on the table pointer, and
the mapping from iceberg-go errors to Iceberg REST error types.

Two corrections to ICEBERG_CATALOG_PLAN.md §4.2 are needed (both recorded below and
for D1):

1. `Requirement.Validate` does **not** return `ErrCommitFailed` in v0.7.0. It returns
   plain errors. I2-3 must wrap them into the 409 `CommitFailedException` itself.
2. `MetadataBuilderFromBase(base, currentFileLocation)` takes a second argument the plan
   omits: the location of the **current** (existing) metadata file, not of the one about
   to be written. iceberg-go appends it to `metadata-log` together with the base's
   `last-updated-ms`. Pass the new file's location here and every commit logs a wrong
   entry; pass `""` and `metadata-log` is silently not extended at all (the doc comment
   allows that only for stage-create). `UpdateTableMetadata(base, updates, metadataLoc)`
   has the same parameter under a misleading name and forwards it unchanged, so I2-3
   must thread the *previous* `metadata.json` path through both. (Verified: passing
   `v999.metadata.json` logs `v999.metadata.json` as the previous file; passing `""`
   logs nothing.)

## What was run

- Dependency: `github.com/apache/iceberg-go v0.7.0` (Apache-2.0), a direct requirement
  in `go.mod`. Its module declares `go 1.25.9`; this repository is on `go 1.26.4`.
- Test: `pkg/icebergcatalog/updates_coverage_test.go`
  ```
  go test -run TestUpdatesCoverage ./pkg/icebergcatalog/
  ok  github.com/treeverse/lakefs/pkg/icebergcatalog
  ```
  `-v` shows 23 `TestUpdatesCoverage/updates/<action>` subtests and 8
  `TestUpdatesCoverage/requirements/<type>` subtests, all PASS.
- A second, non-gate test `TestUnknownUpdatePayloadRejected` records how malformed
  payloads fail (it does not match `-run TestUpdatesCoverage`, by name).
- **Scope of the committed test.** It is the gate the task asks for: decode, re-encode,
  and `Requirement.Validate`. The **"apply"** columns in the tables below come from a
  throwaway harness run against the same 23 payloads and the same base metadata during
  the spike; they are not (yet) regression-guarded by a committed test. If I2-3 wants
  that guard, it should land it with the commit path, where the base table and the
  staged-commit semantics it asserts are the real ones.
- Spec source, and the only source for the action and requirement names below: the
  Apache Iceberg REST OpenAPI spec (Apache-2.0),
  <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
  — `components.schemas.TableUpdate.anyOf` (23 members) and
  `components.schemas.TableRequirement` (8 discriminator entries). The example
  payloads in the test are written field by field from the per-action schemas in that
  file.

The test is driven by the spec's own lists, not by iceberg-go's constants: an action
added to the spec without a matching case fails the test, and so does a case for an
action the spec does not have.

## Update actions — verdict per action

`decode` = JSON → `table.Updates` yields one update whose `Action()` is the action.
`re-encode` = that update marshals back into a payload that decodes to the same
action. `apply` = `table.UpdateTableMetadata` accepts it; "standalone" is against a
minimal format-version 2 base table, "in a commit" is the same action sent together
with the update that creates its precondition.

| # | action | decode | re-encode | apply standalone | apply in a commit | notes |
|---|---|---|---|---|---|---|
| 1 | `assign-uuid` | ✅ | ✅ | ✅ | ✅ | |
| 2 | `upgrade-format-version` | ✅ | ✅ | ✅ | ✅ | any integer decodes; on a v2 base, 2 and 3 apply and 1 is refused (no downgrade) |
| 3 | `add-schema` | ✅ | ✅ | ✅ | ✅ | schema decoded by `iceberg.Schema`; `last-column-id` is tolerated and ignored |
| 4 | `set-current-schema` | ✅ | ✅ | ❌ | ✅ | `-1` means "last added schema" and needs an `add-schema` in the same request |
| 5 | `add-spec` | ✅ | ✅ | ✅ | ✅ | decoded as `UnboundPartitionSpec`, so source ids need not resolve yet |
| 6 | `set-default-spec` | ✅ | ✅ | ❌ | ✅ | same `-1` rule as #4 |
| 7 | `add-sort-order` | ✅ | ✅ | ✅ | ✅ | decoded as `UnboundSortOrder` |
| 8 | `set-default-sort-order` | ✅ | ✅ | ❌ | ✅ | same `-1` rule as #4 |
| 9 | `add-snapshot` | ✅ | ✅ | ❌ | ✅ | refuses a `timestamp-ms` older than the base's `last-updated-ms` (see below) |
| 10 | `set-snapshot-ref` | ✅ | ✅ | ❌ | ✅ | needs the snapshot to exist; `type`, `snapshot-id` required; retention fields optional |
| 11 | `remove-snapshots` | ✅ | ✅ | ✅ | ✅ | removing an unknown id is a no-op |
| 12 | `remove-snapshot-ref` | ✅ | ✅ | ❌ | ✅ | refuses an unknown ref |
| 13 | `set-location` | ✅ | ✅ | ✅ | ✅ | a v1 non-goal to *accept* from clients (§1.2), but it applies |
| 14 | `set-properties` | ✅ | ✅ | ✅ | ✅ | also accepts the legacy `updated` key |
| 15 | `remove-properties` | ✅ | ✅ | ✅ | ✅ | also accepts the legacy `removed` key |
| 16 | `set-statistics` | ✅ | ✅ | ✅ | ✅ | the deprecated top-level `snapshot-id` is accepted and ignored; `statistics.snapshot-id` wins |
| 17 | `remove-statistics` | ✅ | ✅ | ✅ | ✅ | |
| 18 | `set-partition-statistics` | ✅ | ✅ | ✅ | ✅ | |
| 19 | `remove-partition-statistics` | ✅ | ✅ | ✅ | ✅ | |
| 20 | `remove-partition-specs` | ✅ | ✅ | ❌ | ✅ | refuses to remove the default spec |
| 21 | `remove-schemas` | ✅ | ✅ | ❌ | ✅ | refuses to remove the current schema |
| 22 | `add-encryption-key` | ✅ | ✅ | ❌ | ✅ | only applies at format version ≥ 3; with `upgrade-format-version` to 3 in the same request it applies |
| 23 | `remove-encryption-key` | ✅ | ✅ | ✅ | ✅ | removing an unknown key id is a no-op |

Every case that applied — the 14 standalone ones, the six chained groups, and the two
removals below — also produced a metadata document that `table.ParseMetadataBytes` reads
back, i.e. the written `metadata.json` is what our own reader will accept.

**The nine ❌ in "apply standalone" are state, not coverage.** Each one is the
precondition the spec itself defines for that action (nothing has been added yet to
point `-1` at; the snapshot does not exist; the ref does not exist; the default spec,
current schema or format version forbids it). The "apply in a commit" column sends each
of them with its precondition in the same `updates` array, which is the shape a real
client sends, and all apply. `remove-partition-specs` and `remove-schemas` need a table
with more than one spec or schema before there is anything they are allowed to remove;
both were applied against a base holding two of each, and the result re-parsed.

`add-snapshot` deserves two lines of its own for I2-3. First, iceberg-go rejects a
`timestamp-ms` more than **one minute** behind `max(previous metadata's
last-updated-ms, any snapshot already added in this commit)`. It also sets the new
`last-updated-ms` from `now()` when no snapshot carries one, and offers no way to pass
an explicit value. So a client whose clock is more than a minute behind the previous
commit gets a 400 that no other catalog produces, and the value we write is iceberg-go's
wall clock rather than the lakeFS commit time. §6.2 has to own that decision; at
minimum it must not pass a fixed or mocked clock that goes backwards. Second, that
timestamp refusal is the one apply error in the whole set that wraps no sentinel (see
item 4 below).

## Requirement types — verdict per type

`validate` = `Requirement.Validate` accepts an existing table that satisfies it and
rejects a missing table (except `assert-create`, which is the mirror image). The base
used is a minimal format-version 2 document with one schema (id 0, last column id 3),
one empty spec (id 0, last partition id 999) and no refs.

| # | type | decode | re-encode | validate | notes |
|---|---|---|---|---|---|
| 1 | `assert-create` | ✅ | ✅ | ✅ | rejects an existing table, accepts `nil` metadata |
| 2 | `assert-table-uuid` | ✅ | ✅ | ✅ | UUID mismatch rejected |
| 3 | `assert-ref-snapshot-id` | ✅ | ✅ | ✅ | `"snapshot-id": null` is preserved as "ref must not exist"; the field must be present, absent is an error |
| 4 | `assert-last-assigned-field-id` | ✅ | ✅ | ✅ | |
| 5 | `assert-current-schema-id` | ✅ | ✅ | ✅ | |
| 6 | `assert-last-assigned-partition-id` | ✅ | ✅ | ✅ | |
| 7 | `assert-default-spec-id` | ✅ | ✅ | ✅ | |
| 8 | `assert-default-sort-order-id` | ✅ | ✅ | ✅ | |

## Local-implementation list

**Updates and requirements: nothing.** All 23 and all 8 are iceberg-go's. There is no
action for I2-3 to re-implement.

The following are still ours, and are the reasons I2-3 is not a one-liner on top of
this spike. None of them is an update or a requirement — they are the parts iceberg-go
deliberately keeps private (`catalog/internal`) or does not model:

1. **Writing `metadata.json`.** `json.Marshal(meta)` on the value returned by
   `UpdateTableMetadata` produces the document (verified: it re-parses), but the object
   write, the file name (`v<N>.metadata.json`), the location, and the
   `currentFileLocation` we hand to the builder all belong to us. iceberg-go's writer is
   in `catalog/internal`. Getting that one argument wrong is what breaks `metadata-log`
   — see the second correction at the top.
2. **`Requirement.Validate` → 409.** In v0.7.0 a failed requirement returns a plain
   `errors.New`/`fmt.Errorf`; `errors.Is(err, table.ErrCommitFailed)` is **false**
   (verified). The handler must map these to 409 `CommitFailedException` itself:
   §2.1 makes 409 the signal for "refresh and retry", and a 400 or 500 here would make
   clients give up instead of retrying.
3. **Malformed update → 400.** Unknown action, missing `action` and missing required
   field all wrap `iceberg.ErrInvalidArgument` (verified by
   `TestUnknownUpdatePayloadRejected`), so `errors.Is` is the discriminator for
   `BadRequestException`. The wrapped text is not a contract.
4. **Apply errors → 400, never 409.** Eight of the nine standalone refusals wrap
   `iceberg.ErrInvalidArgument` (verified), so `errors.Is` classifies them as
   `BadRequestException`. The ninth is a trap: `add-snapshot`'s "invalid snapshot
   timestamp" error wraps **no sentinel at all** — `errors.Is` is false for
   `iceberg.ErrInvalidArgument`, `table.ErrInvalidMetadata` and
   `table.ErrCommitFailed`. Matching on its message, or checking the timestamp before
   calling apply, is our only option. Nothing on this metadata-only path produces
   `table.ErrCommitFailed`, so "apply failed" must never map to 409: a 409 tells the
   client to refresh and retry the same doomed request forever.
5. **CAS and retry.** The check-and-put of the table pointer, and what we do when it
   loses, is `pkg/graveler`'s conditional write (§6.2). iceberg-go's
   `ErrCommitFailed`/`ErrConflictingDataFiles` sentinels are for its own optimistic
   concurrency and are not produced by the metadata-only path we use.
6. **Format-version gate.** `add-encryption-key` applies only at format version ≥ 3,
   and format v3 is a v1 non-goal (§1.2). The action must still decode (it does) and
   must be rejected with a 400 that names the reason, not surfaced as a 500.
7. **Views.** The spec's `ViewUpdate` actions (`add-view-version`,
   `set-current-view-version`) are not part of `TableUpdate` and are not in this spike.
   iceberg-go v0.7.0 does ship a `view` package with its own `Updates`/`Requirements`,
   so if views are ever un-deferred the same approach applies. They are a §1.2
   non-goal today.

## Dependency cost (for the review of this commit)

`go get github.com/apache/iceberg-go@v0.7.0` moved more than the new module: v0.7.0's
own `go.mod` requires newer versions of libraries this repository already uses, and MVS
takes the maximum. The largest are `gocloud.dev`
v0.34.1-0.20231122211418-53ccd8db26a1 → v0.46.0, `github.com/aws/aws-sdk-go-v2`
v1.42.0 → v1.45.1, `cloud.google.com/go/storage` v1.62.3 → v1.66.0,
`google.golang.org/api` v0.285.0 → v0.295.0, `google.golang.org/grpc` v1.81.1 →
v1.83.2 and `github.com/stretchr/testify` v1.11.1 → v1.12.1. Net effect on the two
files: `go.mod` 452 → 466 lines, `go.sum` 2 145 → 2 364 lines.

Checks run on the result: `go build ./...` succeeds, and `go vet ./pkg/icebergcatalog/`
is clean. The only test failures in the touched areas are the pre-existing
Docker-dependent suites (`pkg/block/gs`, `pkg/block/s3`, `pkg/kv/dynamodb`), which fail
because this environment has no `/var/run/docker.sock`, not because of the bump.

Worth a look in review: the `gocloud.dev` and `aws-sdk-go-v2` jumps touch the block
adapters, so their integration suites should run in CI rather than be taken from this
spike.

## Provenance

Implemented from the Apache Iceberg REST OpenAPI spec and the public lakeFS
documentation only; no code was read from lakeFS Enterprise or from upstream lakeFS ≥
1.87 (BUSL 1.1). Spec URLs to cite in the PR:

- <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml> —
  `TableUpdate`, `TableRequirement`, and the per-action schemas used for the payloads
- <https://github.com/apache/iceberg/blob/main/format/spec.md> — required fields of the
  update objects themselves
- <https://github.com/apache/iceberg-go> — v0.7.0 release that was pinned

## How to re-run

```bash
go test -run TestUpdatesCoverage ./pkg/icebergcatalog/        # the gate
go test -v -run TestUpdatesCoverage ./pkg/icebergcatalog/     # one line per action
go test -v -run TestUnknownUpdatePayloadRejected ./pkg/icebergcatalog/
```
