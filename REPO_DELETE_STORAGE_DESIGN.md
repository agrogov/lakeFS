# Repository deletion with storage: design

Status: draft, 2026-11-10. Implements `REPO_DELETE_STORAGE_SPEC.md` (requirements R1–R25 are referenced by number).
Tasks: `REPO_DELETE_STORAGE_PLAN.md`.

## 1. Overview

```
DELETE /repositories/{repo}?delete_storage=true
  │  Controller: AND authz (fs:DeleteRepository + fs:DeleteStorageNamespace)
  ▼
Catalog.DeleteRepositoryWithStorage
  1. read the repository record(s): R plus its MDS companion when the cascade seam names one
  2. refusal checks R6–R11 for every repository, before any write
  3. per repository: write the deletion record (SetIf "absent") in the global partition
  4. per repository: delete the metadata now (existing graveler path), so GET answers 404
  5. answer 202 with the record
  ▼
Storage deletion worker (one goroutine per lakeFS process, every `interval`)
  for each pending record → own it (MostlyCorrectOwner) →
    step metadata  (idempotent: retries 4 if it failed)
    step settle    (waits out other replicas' repository cache)
    step objects   (adapter.DeleteNamespacePage, repeated, checkpointing the marker)
    step verify    (one pass from the start: the namespace must be empty except the marker)
    step marker    (delete _lakefs/dummy)
    → done; the record expires 24 h later
```

Three parts are new: one block-adapter method, a KV record with a worker in `pkg/catalog`, and an API/CLI/UI
surface. Everything else reuses existing code: graveler's metadata delete, `distributed.MostlyCorrectOwner`,
`kv.SetIf`, and the metadata-companion proof.

## 2. Block adapter

### 2.1 Interface

Add one method to `block.Adapter` (`pkg/block/adapter.go:187`):

```go
// DeleteNamespacePage deletes up to opts.Limit objects under storageNamespace whose keys sort after
// opts.After, skipping opts.Keep (keys relative to the namespace). It never touches a key outside
// "<namespace>/". Deleting a missing object is success. Done reports that the listing reached the end.
DeleteNamespacePage(ctx context.Context, storageID, storageNamespace string, opts DeleteNamespaceOpts) (*DeleteNamespaceResult, error)

type DeleteNamespaceOpts struct {
    After string   // relative key; "" starts from the beginning
    Limit int      // objects to delete in this call; the adapter may delete fewer
    Keep  []string // relative keys to leave in place (the worker passes "_lakefs/dummy")
}
type DeleteNamespaceResult struct {
    Deleted int
    LastKey string // relative key of the last listed object; the worker stores it as the next After
    Done    bool
}
```

**Why one page-level method instead of reintroducing `Remove` plus the walker:**

- The backend-specific parts stay inside the adapter: batch sizes, region-aware clients, Azure directory blobs and
  local directories. The worker stays storage-agnostic.
- The Azure walker skips HNS directory blobs (`pkg/block/azure/walker.go:121`), so a walker-based delete would leave
  them behind.
- The upstream `Remove` (removed in `17d693c0f`, 2025-12-11) deleted one object per call. That is 1000× more
  requests than S3 `DeleteObjects`.

Add `DeleteNamespaceSupport bool` to `block.StorageNamespaceInfo` (`pkg/block/namespace.go:47`). The controller
uses it for R6, and the API exposes it as `StorageConfig.delete_namespace_support` for the UI (R24).

### 2.2 Key safety (G4), shared by every backend

`block.NamespaceDeletePrefix(storageNamespace) (root, prefix string, err error)` is a new function next to
`NamespaceRoot` (`pkg/block/namespace.go:133`). It returns the prefix with exactly one trailing `/`, and
`ErrNamespaceIsRoot` when the prefix would be empty (R9). For `local://` and `mem://` the "root" is the scheme
alone: `local://repo1` is a namespace, and `local://` is the root. Every adapter builds its listing prefix from this
function only. Every adapter also checks every key it is about to delete with
`strings.HasPrefix(key, prefix)` and fails the page if one doesn't match. That is a second guard against a listing
bug.

The trailing `/` is what keeps `…/mds-az7-e1f13de-metadata/` out of a delete of `…/mds-az7-e1f13de`.

### 2.3 Per backend

| Backend | Listing | Delete | Notes |
|---|---|---|---|
| S3 (`pkg/block/s3`) | `ListObjectsV2{Prefix, StartAfter, MaxKeys: min(Limit,1000)}` on the region-aware `clients.Get(ctx, bucket)` | `DeleteObjects` with `Quiet: true`, ≤1000 keys | Per-key `Errors[]` fail the page with the first error, so AccessDenied surfaces (L2). Template: `pkg/actions/lua/storage/aws/s3.go:83-130`. When `Done`, abort incomplete multipart uploads under the prefix (`ListMultipartUploads{Prefix}` + `AbortMultipartUpload`), since they are billed and not listed as objects. |
| Azure (`pkg/block/azure`) | `NewListBlobsFlatPager{Prefix, Include.Metadata}`; skip names ≤ After | `container.Client.NewBatchBuilder` + `BatchBuilder.Delete` + `SubmitBatch`, ≤256 per batch (azblob v1.8.0, `container/client.go:375,401`) | Blob Batch limit is 256 sub-requests, 4 MB body, non-atomic, each sub-request with its own status ([Blob Batch](https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch)). 404 sub-responses count as success. **HNS:** "If you use the Delete Blob API to delete a directory, the directory is deleted only if it's empty" ([known issues](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-known-issues)). So directory blobs (`hdi_isfolder=true`) are skipped during the file passes. When the files are gone, the final `Done` page deletes the remaining directory blobs deepest-first. Blob-only on purpose: the DFS recursive delete needs the `dfs` endpoint (a separate private endpoint) and a new SDK module. Uncommitted blocks are discarded by the service after a week, so no cleanup is needed for them. |
| GCS (`pkg/block/gs`) | `Bucket.Objects(Query{Prefix, StartOffset})` | `ObjectHandle.Delete` on an `errgroup` with limit 32 | The Go client does not support batch requests ([GCS batch docs](https://docs.cloud.google.com/storage/docs/batch)). `storage.ErrObjectNotExist` counts as success. |
| Local (`pkg/block/local`) | `filepath.WalkDir` of the verified namespace directory, sorted | `os.Remove` per file, then empty directories bottom-up | Path through the existing `verifyRelPath`. Never enters the walker's `_lakefs_cache` (`local/walker.go:19`). When `Done`, removes the namespace directory itself if it is empty. |
| Mem (`pkg/block/mem`) | keys of `data[storageID]` with the prefix, sorted | map delete | Used by catalog and controller tests. |
| Transient | n/a | `ErrOperationNotSupported` | `DeleteNamespaceSupport=false`. |

Wrappers:

- `pkg/block/metrics.go`: add a `delete_namespace_page` operation label.
- `pkg/testutil/adapter.go`: the hand-written MockAdapter gets a recording implementation.
- Test fakes that embed the interface compile unchanged.

## 3. Deletion record

### 3.1 Storage

KV partition `repository-deletions` (global, **not** the repository partition, which the metadata delete wipes).
Key `deletions/<repository id>`. One record per repository name enforces R11, and the name lookup serves R14/R16.

Proto, added to `pkg/catalog/catalog.proto`:

```proto
enum RepositoryDeletionState { PENDING = 0; DELETING = 1; DONE = 2; FAILED = 3; }
enum RepositoryDeletionStep  { METADATA = 0; SETTLE = 1; OBJECTS = 2; VERIFY = 3; MARKER = 4; FINISHED = 5; }

message RepositoryDeletionData {
  string repository = 1;
  string storage_id = 2;
  string storage_namespace = 3;
  RepositoryDeletionState state = 4;
  RepositoryDeletionStep step = 5;
  string marker = 6;                 // DeleteNamespaceOpts.After for the next page
  int64 objects_deleted = 7;
  string requested_by = 8;
  google.protobuf.Timestamp requested_at = 9;
  google.protobuf.Timestamp updated_at = 10;
  google.protobuf.Timestamp metadata_deleted_at = 11;
  google.protobuf.Timestamp next_attempt_at = 12;
  int32 attempts = 13;
  string last_error = 14;
  bool force = 15;                   // passed to the metadata step on retry
  string cascade_of = 16;            // set on a companion's record: the data repository that caused it
}
```

### 3.2 Writes

- **Create:** `kv.SetMsgIf(partition, key, msg, nil)`. The predicate `nil` means "must not exist", and
  `kv.ErrPredicateFailed` maps to 409 `deletion_in_progress` (R11).
- **Re-request of a `FAILED` record (R15):** `SetMsgIf` with the read predicate resets it to `PENDING`, sets
  `attempts=0`, and keeps `step` and `marker`, so the deletion continues where it stopped.
- **Every worker update** uses `SetMsgIf` with the predicate from its own read. A lost race means another owner
  advanced the record, and this worker drops it for this tick. That is what makes the "mostly correct" ownership
  safe: two owners can at worst both delete the same page, and deletes are idempotent.

### 3.3 Repository state

The repository record itself keeps today's lifecycle: `IN_DELETION`, then removed by `deleteRepository`
(`pkg/graveler/ref/manager.go:301-349`). Nothing new goes into `RepositoryData`. After step `METADATA`, the deletion
record is the only trace of the repository. That's why R16's checks read the deletion partition.

## 4. Request path

### 4.1 Controller (`pkg/api/controller.go:2516` `DeleteRepository`)

```
if !delete_storage → today's code, unchanged (R2, A6)
rec, err := Catalog.GetRepository(repo)
switch:
  found            → ns = rec.StorageNamespace
  ErrNotFound      → d, err := Catalog.GetRepositoryDeletion(repo)   // resume path (R15)
                      not found → 404; found → ns = d.StorageNamespace
  ErrRepositoryInDeletion → read the raw record via a new Catalog.GetRepositoryRecordAnyState
authorize AND{ fs:DeleteRepository on RepoArn(repo), fs:DeleteStorageNamespace on StorageNamespace(ns) }   (R5)
LogAction "delete_repo" (+ a "delete_storage" field, R22)
d, err := Catalog.DeleteRepositoryWithStorage(ctx, repo, DeleteStorageOptions{Force, RequestedBy: user})
202 + RepositoryDeletion
```

Authorization reuses the `NodeTypeAnd` shape of `CreateRepository` (`controller.go:2290-2312`). The new action is
`DeleteStorageNamespaceAction = "fs:DeleteStorageNamespace"` in `pkg/permissions/actions.go`. Then `make gen-code`
regenerates `actions.gen.go`. `FSFullAccess` (`fs:*`) and `AllAccess` cover it. `FSReadWrite`
(`pkg/auth/base.go`) lists its actions explicitly and does not.

New error mapping in `handleAPIError` (`controller.go:~3200-3251`). The swagger `Error` schema has only `message`,
so the codes from the spec are message prefixes:

| Catalog error | HTTP | message starts with |
|---|---|---|
| `ErrStorageDeletionNotSupported` (adapter flag false or config disabled) | 400 | `not_supported:` |
| `graveler.ErrReadOnlyRepository` (from the storage path, regardless of force) | 400 | `read_only_storage:` |
| `block.ErrNamespaceIsRoot` | 400 | `namespace_is_root:` |
| `ErrNamespaceShared` | 409 | `namespace_shared: <other repo>` |
| `ErrDeletionInProgress` | 409 | `deletion_in_progress:` |

### 4.2 Catalog (`pkg/catalog/storage_deletion.go`, new)

`DeleteRepositoryWithStorage(ctx, repository string, opts DeleteStorageOptions) ([]*RepositoryDeletion, error)`:

1. **Resolve targets.** The target is the repository record. If `c.deletionCascade` is set (§6) and returns a
   companion, the companion's own record is a second target. Its namespace is read from its record, never derived:
   the indexer allows an override (`pkg/mds/indexer/repo.go:~526-545`).
2. **Checks for all targets first, then writes (R19):**
   - R6/R7: `BlockAdapter.GetStorageNamespaceInfo(storageID).DeleteNamespaceSupport` and
     `cfg.RepositoryDeletion.Storage.Enabled`.
   - R8: `record.ReadOnly`.
   - R9: `block.NamespaceDeletePrefix`.
   - R10, overlap: iterate **all** repository records with `graveler.ListRepositories` (it returns every state,
     `IN_DELETION` included), plus every record in `repository-deletions` that is not `DONE`. Compare
     `(storageID, prefix)` pairs: overlap means one prefix `HasPrefix` of the other. This is O(repositories) once
     per request, fine for a rare admin action. Targets of the same request are excluded from each other's check.
   - R11: an existing record that is not `FAILED` and not `DONE`.
3. **Write a record per target** (§3.2). If a later record write fails, the earlier one stays and the worker
   finishes it. Each target is valid on its own because it passed its checks.
4. **Metadata step inline** for each target: `c.Store.DeleteRepository(ctx, id, graveler.WithForce(true))`. Force
   is safe here: R8 already refused read-only repositories, and `force` is still recorded. On success, set
   `step=SETTLE` and `metadata_deleted_at=now`. On failure, return 202 anyway: the worker retries the step. The error
   is in `last_error`.
5. Return the records.

Also in the catalog:

- `GetRepositoryDeletion`, `ListRepositoryDeletions` (R14).
- `CheckRepositoryDeletionConflict(name, storageID, ns)`, called from `Catalog.CreateRepository` (and
  `CreateBareRepository`) before graveler: a not-`DONE` record with the same name or an overlapping prefix
  returns `ErrDeletionInProgress` (R16).

## 5. Worker

`Catalog.StartRepositoryDeletionWorker(ctx)` is started from `cmd/lakefs/cmd/run.go` next to
`scheduleCleanupJobs` (`run.go:159`), only in the server process (not `mds run`). The loop:

```
every cfg.Interval (30s) + jitter:
  for each record in repository-deletions (kv.NewPrimaryIterator):
     DONE   && updated_at < now-24h → delete record (expiry, R14)
     DONE | FAILED                  → skip
     next_attempt_at > now          → skip
     release, err := owner.Own(ctx, ownerID, key)    // distributed.NewMostlyCorrectOwner(log, kv, "repository-deletions/owner", 30s, 10s)
     run steps until FINISHED, the context ends, or an error
     release()
```

Records are processed one at a time per process. With N replicas, N deletions run in parallel at most. That bounds
provider request rates (L3) without a separate limiter.

Steps:

| Step | Action | Moves on when |
|---|---|---|
| METADATA | `Store.DeleteRepository(force)`; `graveler.ErrNotFound` = success | record gone |
| SETTLE | wait until `metadata_deleted_at + graveler.repository_cache.expiry + jitter` (defaults 5s + jitter, `pkg/config/defaults.go:156`). Another replica may still hold the repository in its cache and accept one more write. | time passed |
| OBJECTS | loop `DeleteNamespacePage{After: marker, Limit: batch_size, Keep: ["_lakefs/dummy"]}`; after each page `SetMsgIf` with `marker=LastKey`, `objects_deleted+=Deleted` | `Done` |
| VERIFY | one more pass from `After=""` with the same Keep. Anything it deletes was written behind the marker by a late writer; the count is added. | a pass that reports `Done` and `Deleted==0` |
| MARKER | `DeleteNamespacePage{Keep: nil}`: only the marker (plus any directory entries) is left | `Done` |
| FINISHED | `state=DONE` | — |

The marker is deleted last (R12). If a deletion stops halfway, `ensureStorageNamespace` still refuses the namespace
for a new repository. R16 refuses it too, so the protection doesn't rely on R16 alone.

**Errors (R15).**

- An error sets `attempts++`, `last_error`, `next_attempt_at = now + min(interval·2^attempts, 10m)`, with
  `state=DELETING`.
- When `attempts ≥ max_attempts`, the state becomes `FAILED`.
- Classification is not needed: transient errors heal within the attempts, and permanent ones (AccessDenied,
  container gone) end in `FAILED` with the provider's message, which is what L2 asks for.
- A namespace that no longer exists (bucket or container deleted) lists as empty and finishes as `DONE`.

**Shutdown.** The worker uses the server context. A page in flight is either recorded or redone after restart,
and both are correct (A3).

**Metrics (R21)**, registered in `pkg/catalog`:

- `lakefs_repository_storage_deletions{state}`: gauge, set on each scan.
- `lakefs_repository_storage_deleted_objects_total`: counter.
- `lakefs_repository_storage_deletion_errors_total{step}`: counter.

Logs carry `repository`, `storage_namespace`, `step`, `objects_deleted`.

## 6. Metadata Search cascade (R17–R19)

The catalog gets a seam in the same style as `SetMergeConflictResolver` (`pkg/catalog/merge_conflict.go:89-95`):

```go
type DeletionCascade func(ctx context.Context, repository *Repository) (companion string, ok bool, err error)
func (c *Catalog) SetDeletionCascade(f DeletionCascade)   // atomic.Pointer, nil = no cascade
```

`pkg/icebergcatalog` installs it from its existing extra builder in `cmd/lakefs-sso/main.go`, only when Metadata
Search is enabled. It returns `MetadataRepository(repo.Name)` only if that repository exists **and** passes the
init-commit proof. The proof is the one `ListRepositories` uses (`isHiddenMetadataCompanion`,
`pkg/catalog/metadata_companion.go:122`), exported as `Catalog.IsMetadataCompanion(ctx, repo, defaultBranch)` so
that the two can't disagree. A user's own `foo-metadata` without the init commit is never cascaded (R17).

**Indexer interaction:**

- **After R is deleted,** `ensureMetadataRepository` (`pkg/mds/indexer/repo.go:139-160`) reads R first and fails
  with "data repository not found". It never reaches the companion re-create. `/_health` reports R as failing
  while R stays configured, which is the existing behaviour for a missing repository (A7).
- **A sync tick that read R just before the delete** may try to create the companion after the cascade deleted it.
  `CheckRepositoryDeletionConflict` refuses that create (R16), and the next tick sees R missing.
- **Stale init cache.** If R and its companion are later re-created under the same names, the indexer's per-process
  `initialised` flag (`repo.go:224-227`) would skip the init commit, and the new companion would fail the proof.
  Fix: in the `ErrNotFound` branch of `ensureMetadataRepository` (the companion is being created), reset
  `metaInfo(repository)` before `createMetadataRepository`. That one line covers every path that recreates a
  companion, not only this feature's.

## 7. API (`api/swagger.yml`)

- `deleteRepository` (`swagger.yml:3338-3361`):
  - new query param `delete_storage` (boolean, default false);
  - new response `202` with schema `RepositoryDeletion`;
  - `409` is added (currently missing), and `410` stays undocumented as today.
- `GET /repository_deletions` (`listRepositoryDeletions`, `prefix`/`after`/`amount` like other lists) and
  `GET /repository_deletions/{repository}` (`getRepositoryDeletion`), tag `repositories`.
- `RepositoryDeletion` schema: the fields in R14, `state` as an enum string.
- `StorageConfig.delete_namespace_support` (boolean, **not** required, so older servers parse).
- `make gen-api` regenerates `pkg/api/apigen/lakefs.gen.go`.
- `make clients` regenerates the Python, Java and Rust clients.

## 8. Clients

### lakectl (`cmd/lakectl/cmd/repo_delete.go`)

- Flags:
  - `--delete-storage`;
  - `--wait` (only with `--delete-storage`), which polls `getRepositoryDeletion` every 2 s, prints
    `objects_deleted`, exits 0 on `done` and 1 on `failed` with `last_error`;
  - `--force`, which passes `force` (today lakectl can't send it).
- The prompt for `--delete-storage` first reads the repository to show its namespace: "Delete repository X **and
  every object under** `<ns>`? Objects referenced from other repositories will break." `--yes` skips it, as today.
- New command `lakectl repo deletion-status <repo uri>`.
- Without the new flags, output is unchanged, so the existing goldens hold (A6).

### Web UI

- `webui/src/lib/api/index.js:575` `delete(repoId, {deleteStorage, force})`.
- `DeleteRepositoryModal` (`settings/general.jsx:16-62`): a checkbox shown when
  `storageConfig.delete_namespace_support`. The existing warning text ("Data in the underlying object store will not
  be deleted…") changes with the checkbox. On 202 the modal shows "Deleting objects in the background" and
  navigates to the repository list.
- The repository list shows nothing for deleted repositories. Status is available through lakectl and the API only;
  a UI status page is out of scope.

### Python wrapper (`clients/python-wrapper/lakefs/repository.py:71`)

`Repository.delete(delete_storage=False)` returns the `RepositoryDeletion` when `delete_storage=True`.

## 9. Configuration

In `pkg/config/config.go` (+ `defaults.go`, `docs/reference/configuration.md`):

```yaml
repository_deletion:
  storage:
    enabled: true        # R7
    interval: 30s
    batch_size: 1000     # per DeleteNamespacePage; adapters cap to their own limit (S3 1000, Azure 256 per batch)
    max_attempts: 10
```

## 10. Rejected alternatives

- **Synchronous delete in the request.** Fails G2: a large namespace outlives any HTTP timeout. This is the
  "can take a lot of time" concern from the forum.
- **The existing catalog task mechanism (`RunBackgroundTaskSteps`, catalog.go:2271).** Task status lives in the
  repository partition, which the metadata delete wipes. Tasks are also not resumable after a restart (only marked
  stale). Fails G3.
- **Keeping the repository `IN_DELETION` until the objects are gone.** Leaves a half-repository visible in every
  `ListRepositories` caller (the listing does not filter by state) for hours, and mixes two lifecycles in one
  record.
- **Provider-side lifecycle rules (S3 lifecycle expiration, Azure lifecycle management).** Asynchronous with no
  completion signal, needs management-plane rights lakeFS doesn't have, and is per bucket, not per prefix.
- **Azure DFS recursive directory delete.** One call on HNS accounts, but it needs the `dfs` endpoint ("some
  operations (such as … deleting directories) require a DFS private endpoint",
  [known issues](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-known-issues)) and the
  `azdatalake` module. It doesn't work on non-HNS accounts either, so the Blob Batch path is needed anyway.

## 11. Risks

| Risk | Mitigation |
|---|---|
| Deleting outside the namespace | Single prefix function with trailing `/`, a per-key `HasPrefix` re-check in every adapter, blocktest sibling-prefix case (A2) |
| Shared namespace | R10 overlap scan over every repository state and every pending record |
| Late writes from another replica's cache | SETTLE step + VERIFY pass |
| Breaking imported or linked entries in other repositories (L1) | Documented and shown in prompts; not detectable cheaply |
| Azurite has no HNS, so directory deletion is untested in CI | Unit test with a fake pager; one manual run on an HNS account before rollout |
| Upstream merge conflicts | The new method touches every adapter. It's additive, and upstream removed `Remove` but has no method by this name. |

## 12. Sources

- Upstream issue: https://github.com/treeverse/lakeFS/issues/5566
- Removed `Remove`: upstream commit `17d693c0f` (2025-12-11, "Remove Remove method from Adapter interface and its
  implementations"); its per-backend bodies are the starting point for the delete calls.
- Azure Blob Batch: https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch
- Azure HNS Delete Blob on directories, DFS private endpoint:
  https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-known-issues
- GCS batch not supported in the Go client: https://docs.cloud.google.com/storage/docs/batch
- S3 DeleteObjects (1000 keys): https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
