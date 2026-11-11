# Repository deletion with storage: implementation plan

Spec: [`REPO_DELETE_STORAGE_SPEC.md`](REPO_DELETE_STORAGE_SPEC.md) (R*, A*). Design:
[`REPO_DELETE_STORAGE_DESIGN.md`](REPO_DELETE_STORAGE_DESIGN.md) (§ numbers below refer to it).

Status: D1–D10 done, 2026-11-11. D10 was the only test of HNS directory deletion on real Azure; it
passed on hr2 (output in the status row below).

## Branching

Base `feature/metadata-search`. Each task is one commit (or one card) with the test that proves it. Tasks run in the
order of the dependency graph below. Two tasks never edit the same file at the same time; the file lists are the
ownership.

```
D1 adapter ──► D3 worker ──► D4 API ──► D5 lakectl ─┐
D2 record ───┘                    │                  ├─► D8 esti ─► D9 docs ─► D10 hr2
                                  ├──► D6 webui ─────┤
                                  └──► D7 MDS cascade┘
```

D1 and D2 can run in parallel. D5, D6 and D7 can run in parallel after D4.

Shared-file rules:
- `api/swagger.yml` and the generated clients are edited by D4 only.
- `pkg/config/*` is edited by D2 only.
- `cmd/lakefs/cmd/run.go` is edited by D3 only; `cmd/lakefs-sso/main.go` by D7 only.

## Status

| Task | State | Commit |
|---|---|---|
| D1 adapter method | Done | `f0a97958b` |
| D2 deletion record, config, conflict check | Done | `7b6e28aa1` |
| D3 worker | Done | `4cacc6a35` |
| D4 API, authorization, codegen | Done | `4202719b1` |
| D5 lakectl | Done | `a16ded56c` |
| D6 web UI | Done | `461d9dfec` |
| D7 MDS cascade and indexer cache fix | Done | `835d47f8b` |
| D8 esti | Done | `8187c2ad1` |
| D9 docs | Done | This commit |
| D10 hr2 rollout and verification | Done | This commit — passed on hr2, see below |

### D10 result — hr2, Azure `cpaasailakefs` (HNS), 2026-11-11

Image `docker.ib-ci.com/lakefs:dev-mds-delete` (`sha256:f697681c…`), rolled by ArgoCD at 11:12Z; the server
reports `dev-mds-delete`, `storage_config.delete_namespace_support=true`, no `repository_deletion.*` override (so the
§9 defaults applied: `enabled`, `30s`, `1000`, `10`). Throwaway namespace `…/lakefs/rds-6266ce/`. Fixture: 300
objects in nested paths committed through lakeFS, plus 4 blobs written straight to storage at depth 5 so that HNS
directories exist (see the note below on why lakeFS's own writes do not create them).

Before: 608 blobs under the prefix; directories `rds-6266ce/{_lakefs/,data/,manual/}`,
`rds-6266ce/data/{ftdbamb8b51s73b3cok0/,ftdbank2r8ac73acrdb0/}`, `rds-6266ce/manual/deep/l1/→l2/→l3/`; container
total 466 blobs. Command under test:

```
$ /tmp/lakectl-d10 repo delete lakefs://rds-6266ce --delete-storage --wait --yes
Repository: lakefs://rds-6266ce
Warning: Deleting repository 'rds-6266ce' also deletes every object under 'https://cpaasailakefs.blob.core.windows.net/lakefs/rds-6266ce/'. Objects referenced from other repositories will break.

Repository 'rds-6266ce' storage deletion requested
repository: rds-6266ce
storage namespace: https://cpaasailakefs.blob.core.windows.net/lakefs/rds-6266ce/
state: done
objects deleted: 608
requested by: 4ac0477c-488a-44ae-af4c-b6c747cb693e
requested at: 2026-11-11 12:20:11 +0100 CET
updated at: 2026-11-11 12:20:41 +0100 CET
attempts: 0

real	0m30.250s        (start 12:20:11 CET, end 12:20:41 CET, exit 0)
```

Verification (`az storage blob list --container-name lakefs`, container is `lakefs`, key from `lakefs-secrets`):

```
$ /tmp/lakectl-d10 repo deletion-status lakefs://rds-6266ce
state: done / objects deleted: 608 / attempts: 0

$ /tmp/lakectl-d10 repo list | grep -c rds-6266ce          -> 0
$ /tmp/lakectl-d10 fs ls lakefs://rds-6266ce/main/         -> repository not found, 404 Not Found

$ az storage blob list --prefix rds-6266ce/                          -> (no output), count 0
$ az storage blob list --prefix rds-6266ce/ --delimiter /            -> (no output), count 0
$ az storage blob list --prefix rds-6266ce/manual/deep/ --delimiter / -> count 0

$ az storage blob list --delimiter /                       # siblings untouched
_lakefs/
destination-scoring-modeling-metadata/
destination-scoring-modeling/
jeff-training-metadata/
jeff-training/

$ az storage blob list --num-results 5000 | length(@)      -> 466   (== the pre-test baseline)
$ per prefix: jeff-training/ 369, jeff-training-metadata/ 88, destination-scoring-modeling/ 1,
                destination-scoring-modeling-metadata/ 7, _lakefs/ 1  (all unchanged; `_lakefs/dummy` still there)

# The blob API cannot show directories that hold no blobs, so the same prefix was also walked at the
# HNS namespace level (dfs endpoint; it sits behind a TLS-intercepting proxy, so the two reads below
# ran with AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1, read only):
$ az storage fs file list -f lakefs --path ""                        -> 5 entries, no rds-6266ce
$ az storage fs file list -f lakefs --path rds-6266ce --recursive true
ERROR: The specified path does not exist.
$ az storage fs file list -f lakefs --path jeff-training --recursive true   -> 369 (control: the command works)

$ kubectl -n lakefs logs deploy/lakefs | grep rds-6266ce
11:20:39Z "Repository storage deletion started"  step=SETTLE objects_deleted=0   repository=rds-6266ce
11:20:41Z "Repository storage deletion finished" step=FINISHED objects_deleted=608 repository=rds-6266ce

$ curl /api/v1/repository_deletions
{"attempts":0,"objects_deleted":608,"repository":"rds-6266ce","state":"done",
 "storage_namespace":"https://cpaasailakefs.blob.core.windows.net/lakefs/rds-6266ce/", ...}
```

`objects_deleted=608` equals the blob count measured before the delete, and the container is back to its exact
baseline. Nothing outside `rds-6266ce/` was touched (A2 on a real account). No MDS companion existed or was created:
the indexer only knows `jeff-training` and `destination-scoring-modeling`.

Two facts this run established that the plan assumed otherwise:

- **Nested logical paths do not create nested HNS directories on Azure.** lakeFS's Azure adapter stores object data
  under opaque keys, `<namespace>/data/<id>/<id>`, so a few hundred objects in `fixture/dNN/sMM/…` produce exactly
  the directories `_lakefs/` and `data/<id>/`. The depth-5 `manual/deep/l1/l2/l3/` tree therefore had to be written
  straight to storage to make the directory part of R12 non-vacuous.
- **Empty HNS directories disappear with their last blob.** Deleting every blob removed `manual/`, `data/` and
  `_lakefs/` as well: after the run the whole `rds-6266ce/` prefix is gone from the delimiter listing, not just
  empty. The design §2.3 path for `hdi_isfolder` blobs (skip during file passes, delete deepest-first on `Done`) was
  therefore **not exercised**: the namespace contained no `hdi_isfolder` blobs, and explicit directories created via
  the DFS API were not tested. That remains the one part of the HNS story covered only by the unit test with the
  fake pager.

## Tasks

### D1 — `Adapter.DeleteNamespacePage` (§2; R9, R12, G4)
- Files:
  - interface: `pkg/block/adapter.go`;
  - namespace prefix helper: `pkg/block/namespace.go` (`NamespaceDeletePrefix`, `ErrNamespaceIsRoot`,
    `StorageNamespaceInfo.DeleteNamespaceSupport`) plus `pkg/block/namespace_test.go`;
  - backends: `pkg/block/{s3,azure,gs,local,mem,transient}/adapter.go`, plus a new `delete_namespace.go` per backend
    if the body is long;
  - wrappers and fakes: `pkg/block/metrics.go`, `pkg/testutil/adapter.go`;
  - conformance suite: `pkg/block/blocktest/adapter.go`.
- Do:
  - Write the interface, the prefix helper and the per-backend bodies as in the §2.3 table. Start from upstream
    `17d693c0f`'s removed `Remove` bodies (`git show 17d693c0f`) for the client calls.
  - S3: `DeleteObjects` batches; abort multipart uploads on `Done`.
  - Azure: `SubmitBatch` with ≤256 sub-requests; directory blobs deepest-first on `Done`.
  - GCS: per-object deletes via an errgroup with limit 32.
  - Local: verified path, never `_lakefs_cache`.
  - Set `DeleteNamespaceSupport` per backend.
- Tests:
  - `blocktest.testAdapterDeleteNamespace`, run by every backend's existing `AdapterTest` (MinIO, Azurite,
    fake-gcs, local, mem), cases:
    1. 2 500 objects (3 pages at 1000; 10 at 256), `Done` at the end, the namespace lists empty.
    2. Sibling prefix `<ns>-metadata/x` and parent-prefix object `<parent>/y` survive (A2).
    3. `Keep: ["_lakefs/dummy"]` leaves exactly the marker, then a `Keep: nil` call removes it.
    4. Resume: delete with `Limit: 10`, then continue from the returned `LastKey`; the total equals the object count.
    5. A missing namespace returns `Done`, 0 deleted, no error.
    6. A bucket/container/local root returns `ErrNamespaceIsRoot`.
  - Azure unit test with a fake pager returning `hdi_isfolder` blobs: directory blobs are deleted last, deepest first.
  - S3 unit test: a `DeleteObjects` response with `Errors[AccessDenied]` returns an error naming the key.
  - `NamespaceDeletePrefix` table test: trailing `/` handling, Azure container root, `local://`, `mem://`.
- Done when all of the above pass with `-race`, and `go build ./...` passes (every `Adapter` implementation
  compiles).

### D2 — deletion record, configuration, create-time conflict (§3, §4.2 checks, §9; R10, R11, R16, R20)
- Files:
  - record: `pkg/catalog/catalog.proto` and the regenerated `catalog.pb.go` (`make gen-proto`);
  - catalog code: new `pkg/catalog/storage_deletion_record.go` and `pkg/catalog/storage_deletion_checks.go`, plus
    their `_test.go` files; `pkg/catalog/catalog.go` (`CreateRepository`/`CreateBareRepository` call the conflict
    check);
  - config: `pkg/config/config.go`, `pkg/config/defaults.go`.
- Do:
  - Write the proto messages from §3.1 and the record CRUD with `kv.SetMsgIf` (§3.2).
  - Write `GetRepositoryDeletion`/`ListRepositoryDeletions`.
  - Write the overlap scan (§4.2 step 2): every repository record via `graveler.ListRepositories`, plus
    non-`DONE` deletion records.
  - Write `CheckRepositoryDeletionConflict`, wired into repository creation.
  - Add the `repository_deletion.storage.*` config with defaults.
- Tests, with the mem KV:
  - create, then a duplicate create fails with `ErrDeletionInProgress`;
  - resetting a `FAILED` record keeps `step`/`marker`;
  - overlap: equal, child and parent prefixes conflict, and `x` vs `x-metadata` does not;
  - a different `storageID` with the same path does not conflict;
  - an `IN_DELETION` repository counts as an owner;
  - `CreateRepository` with the same name or an overlapping namespace fails while a record is pending and passes
    after `DONE`.
- Done when the tests pass and `make validate-proto` is clean.

### D3 — worker (§4.2 `DeleteRepositoryWithStorage`, §5; R3, R12–R15, R21; needs D1, D2)
- Files:
  - catalog code: new `pkg/catalog/storage_deletion.go`, `pkg/catalog/storage_deletion_worker.go`,
    `pkg/catalog/storage_deletion_metrics.go`, plus their `_test.go` files;
  - wiring: `cmd/lakefs/cmd/run.go` (start the worker next to `scheduleCleanupJobs`).
- Do:
  - Write `DeleteRepositoryWithStorage` (checks for all targets, then record writes, then the inline metadata step),
    with no cascade yet (the seam is added in D7).
  - Write the worker loop: `distributed.NewMostlyCorrectOwner`, the steps METADATA → SETTLE → OBJECTS → VERIFY →
    MARKER → FINISHED, backoff, `FAILED` after `max_attempts`, and 24 h expiry of `DONE` records.
  - Add the metrics and logs.
- Tests, with the mem adapter and mem KV:
  1. A happy path with 3 000 objects across branches, staged data and `_lakefs/`: the record ends `DONE`,
     `objects_deleted` is right, the namespace is empty, and the repository is 404.
  2. Crash/resume: cancel the context after the first page, start a new worker on the same KV, and the deletion
     completes with no object left (A3).
  3. Two workers on one KV and one adapter run concurrently; the deletion completes and every object is gone (A4).
  4. Late write: an object put under the namespace after OBJECTS passed its key is removed by VERIFY.
  5. An adapter error on every call: `attempts` grows, `next_attempt_at` backs off, and the record ends `FAILED`
     with `last_error`. A re-request resets it and it completes once the error is cleared.
  6. Refusals R6, R8, R9, R10 and R11 each change nothing (KV and objects compared before and after).
  7. Expiry: a `DONE` record older than 24 h is removed.
- Done when the tests pass with `-race` and `go test ./pkg/catalog/...` is green.

### D4 — API, authorization, codegen (§4.1, §7; R1–R5, R14, R22, R25; needs D3)
- Files:
  - API definition: `api/swagger.yml`;
  - generated code: `pkg/api/apigen/*` (via `make gen-api`), `clients/{python,java,rust}/**` (via `make clients`);
  - handlers: `pkg/api/controller.go`, plus `pkg/api/controller_test.go` (new cases only);
  - permissions: `pkg/permissions/actions.go` and `actions.gen.go` (via `go generate`);
  - storage config: `pkg/api/` StorageConfig assembly (`delete_namespace_support`).
- Do:
  - swagger: `delete_storage` on `deleteRepository`, plus 202/409 responses, `RepositoryDeletion`,
    `listRepositoryDeletions`, `getRepositoryDeletion`, and `StorageConfig.delete_namespace_support` (optional).
  - Controller: the flow in §4.1, including the resume-by-name path for a deleted repository.
  - Map the new errors with message prefixes.
  - Add `DeleteStorageNamespaceAction`.
- Tests (`controller_test.go`):
  - `delete_storage` absent: the existing `TestController_DeleteRepositoryHandler` passes unmodified (A6).
  - `delete_storage=true`: 202 with a body. The `GET` on the repository gives 404 and the status endpoint shows the
    record. After the worker runs (driven in the test), the status is `done` and a create on the same namespace
    succeeds (A1 on mem).
  - RBAC, next to the cases around `controller_test.go:7236`:
    - `fs:DeleteRepository` without `fs:DeleteStorageNamespace` → 401;
    - with both → 202;
    - `FSFullAccess` → 202;
    - `FSReadWrite` → 401.
  - Status endpoints: `fs:ReadRepository` is required for get, `fs:ListRepositories` for list.
  - Each refusal maps to the status and message prefix in §4.1.
- Done when the tests pass, and `make validate-api`, `make validate-permissions-gen` and the client generation are
  clean.

### D5 — lakectl (§8; R23; needs D4)
- Files: `cmd/lakectl/cmd/repo_delete.go`, new `cmd/lakectl/cmd/repo_deletion_status.go`, new
  `esti/golden/lakectl_repo_delete_storage*.golden`, and the esti lakectl test file that runs them.
- Do: add `--delete-storage`, `--wait`, `--force`, the namespace-naming prompt, and `repo deletion-status`.
- Tests:
  - Existing `lakectl_repo_delete*.golden` files are unchanged (A6).
  - New goldens for the prompt, the 202 output, the `--wait` success, and the `--wait` failure (exit 1).
- Done when `go test ./cmd/lakectl/...` passes, plus the new esti lakectl cases in D8.

### D6 — web UI (§8; R24; needs D4)
- Files: `webui/src/lib/api/index.js`, `webui/src/pages/repositories/repository/settings/general.jsx`, plus a vitest
  file next to it.
- Do: add the checkbox (shown only when `delete_namespace_support`), the warning text that changes with it, and the
  202 handling.
- Tests: vitest for the checkbox showing and hiding and the request parameter.
- Done when `npm test`, `npm run lint` and `npm run format:check` pass in `webui/`.

### D7 — Metadata Search cascade and indexer cache reset (§6; R17–R19, A7; needs D3)
- Files:
  - seam: `pkg/catalog/storage_deletion.go` (the `SetDeletionCascade` seam; D3 is finished by then);
  - companion proof: `pkg/catalog/metadata_companion.go` (export `IsMetadataCompanion`);
  - installer: `pkg/icebergcatalog/` (the cascade installer, in the file that installs the other catalog seams);
  - wiring: `cmd/lakefs-sso/main.go`;
  - indexer: `pkg/mds/indexer/repo.go` (reset `metaInfo` before re-creating a missing companion), plus tests in each
    package.
- Do: as in §6.
- Tests:
  - A companion with the init commit is cascaded: two records, both namespaces empty.
  - A user's own `foo-metadata` without the init commit is not cascaded.
  - The companion is read-only, so the whole request is refused and nothing changes (R19).
  - Without `delete_storage`, the companion is untouched (R18).
  - Indexer: delete and re-create R and its companion within one process; the init commit is written again.
  - Indexer: a create of the companion while R's deletion is pending is refused (R16 through the indexer's client).
- Done when `go test -race ./pkg/catalog/... ./pkg/icebergcatalog/... ./pkg/mds/... ./cmd/lakefs-sso/...` passes.

### D8 — esti (A1–A7; needs D4–D7)
- Files: new `esti/repository_delete_storage_test.go`; `esti/mds_test.go` (one new case); `esti/esti_utils.go`
  (helper only if needed).
- Do:
  - Within the existing blockstore matrix (`.github/workflows/esti.yaml`, s3/gs/azure):
    1. Create a repository, upload, commit, leave staged and uncommitted data, and put a sibling object directly in
       storage at `<ns>-metadata/keep`.
    2. Delete with `delete_storage=true` and poll the status to `done`.
    3. List the namespace through the provider client: it is empty, and the sibling is still there.
    4. Re-create a repository on the same namespace: success.
    5. Each refusal: a root namespace, a shared namespace, a duplicate request.
  - lakectl goldens from D5.
  - MDS: index R, delete R with storage, and check that R, `R-metadata` and both namespaces are gone and the indexer
    does not recreate `R-metadata`.
- Done when the cases pass locally on MinIO, Azurite and fake-gcs. Record the commands and timings in the commit.
  If one blockstore can't be run locally, say so in the commit and leave it to CI.

### D9 — docs (needs D1–D8)
- Files: `docs/reference/configuration.md` (`repository_deletion.*`), `docs/metadata-search.md` (cascade
  paragraph), new `docs/repository-deletion.md`. The spec, design and plan are updated to "as built".
- Content of `docs/repository-deletion.md`:
  - what `delete_storage` deletes and what it never deletes (L1);
  - required object-store permissions per provider (L2): S3 `s3:DeleteObject` and `s3:AbortMultipartUpload`; Azure
    Storage Blob Data Contributor; GCS `storage.objects.delete`;
  - versioning and soft delete (L4);
  - status and retry (R14, R15);
  - the RBAC action.
- Done when every config key in code appears in the reference and every snippet in the new page has been run once.

### D10 — hr2 rollout and real-Azure verification (needs D9; cluster writes only with the owner's go-ahead)
- Build and push `dev-mds`. Changes to `lakefs-values.yaml` (branch `AIOR-83`) are needed only if the defaults
  change.
- On hr2 (Azure `cpaasailakefs`, HNS):
  1. Create a throwaway repository on `…/lakefs/rds-<random>`.
  2. Upload a few hundred objects in nested paths (so HNS directories exist) and commit.
  3. Delete with `lakectl repo delete --delete-storage --wait`.
  4. Check `az storage blob list --prefix rds-<random>/` returns nothing, directories included.
  5. Check the sibling prefixes are untouched.
- This is the only test of HNS directory deletion (design §11 risk). Record the raw command output in this plan's
  status row.

## Verification rules (every task)

- `export PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto`; `go build ./...`; `go vet` and
  `go tool golangci-lint run` on touched packages; `go test -count=1 -race` on touched packages.
- Generated files only through their generators (`make gen-api`, `make gen-proto`, `go generate ./pkg/permissions`,
  `make clients`). The matching `make validate-*` must be clean.
- Each commit message lists the commands run and a summary of their output. A task that could not run something says
  so.
- No commits of secrets. hr2 credentials are read at run time from `lakefs-secrets` only.

## Risks (see design §11)

- **Deleting outside the namespace:** D1's sibling and parent cases are the gate. D3 is not started until they
  pass on all five backends.
- **Azure HNS directories untested in CI:** D10 is the gate before anyone uses `--delete-storage` on hr2 data.
- **Upstream drift:** the interface change touches every adapter. Rebase cost is limited to the method addition.
