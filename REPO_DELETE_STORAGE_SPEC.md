# Repository deletion with storage: specification

Status: implemented, 2026-11-11, as tasks D1–D10 of `REPO_DELETE_STORAGE_PLAN.md`. D10 (the hr2
rollout and real-Azure verification) passed; its raw output is in the plan's status table. Companion documents:
`REPO_DELETE_STORAGE_DESIGN.md` (how), `REPO_DELETE_STORAGE_PLAN.md` (tasks).

## 1. Problem

Deleting a lakeFS repository today removes only its metadata in the KV store: branches, tags, commits and
repository settings (`pkg/graveler/ref/manager.go:301-349`). Every object under the repository's storage namespace
stays in the object store, including the `_lakefs/dummy` marker. Two things follow:

- **The storage keeps costing money.** Nothing in lakeFS will ever delete it, and the user has to find the
  namespace and delete it by hand.
- **The namespace can't be reused.** A new repository on the same namespace fails with "storage namespace already in
  use" (`ensureStorageNamespace`, `pkg/api/controller.go:2459-2514`).

The UI already says so: "Data in the underlying object store will not be deleted by this action."
(`webui/src/pages/repositories/repository/settings/general.jsx:45`).

**Upstream position.** Upstream issue treeverse/lakeFS#5566 is open, labels P3 and feature-request. On 2024-02-29
the maintainers decided to document manual deletion rather than build this. The reasons given on the lakeFS forum
were: deleting all the data "can take a lot of time and is really scary", and lakeFS usually runs without delete
rights on the bucket. So this feature is our extension. No official guide exists to follow; the requirements below
are ours, and they answer each of those concerns explicitly.

Real case: on 2026-11-10 the hr2 test repositories `mds-az7` and `mds-az7-metadata` were deleted. That left their blobs
under `cpaasailakefs/lakefs/mds-az7-e1f13de/` and `.../mds-az7-e1f13de-metadata/`, which had to be deleted by hand
with `az storage blob delete-batch`.

## 2. Goals

- G1. One request deletes a repository **and** every object under its storage namespace.
- G2. The request returns quickly whatever the data size. The object deletion runs in the background.
- G3. The deletion survives server restarts and multiple replicas, and always finishes or reports why it can't.
- G4. Nothing outside the repository's own storage namespace is ever deleted.
- G5. After it finishes, the namespace can be reused for a new repository.
- G6. For Metadata Search, deleting a data repository with storage also deletes its metadata repository
  (`<repo>-metadata`) and that repository's storage.

## 3. Non-goals

- Changing the default. A plain delete keeps today's behaviour exactly, metadata only, for every existing client.
- Deleting objects outside the namespace: imported data, linked physical addresses, external locations.
- Recovering storage left behind by repositories deleted in the past ("orphan sweep"). Out of scope; see §9.
- Cancelling a storage deletion once it has started.

## 4. Requirements

### Request

- R1. `DELETE /api/v1/repositories/{repository}` accepts a new optional query parameter `delete_storage` (boolean,
  default `false`).
- R2. With `delete_storage=false` or absent, behaviour, status codes and response are unchanged (204).
- R3. With `delete_storage=true` the server answers **202 Accepted** and returns the deletion status (R14). The
  repository is no longer readable from that moment: `GET` answers 404 once its metadata is deleted.
- R4. `force` keeps its meaning (bypass read-only protection for the metadata delete) and does not affect R8.

### Authorization

- R5. Deleting storage requires `fs:DeleteRepository` on `arn:lakefs:fs:::repository/<repo>` **and** a new action
  `fs:DeleteStorageNamespace` on `arn:lakefs:fs:::namespace/<storage namespace>`. Both are checked with the
  same AND node that `CreateRepository` uses for `fs:AttachStorageNamespace`. `fs:*` and the built-in
  `FSFullAccess` and `AllAccess` policies include the new action; `FSReadWrite`, which lists its actions explicitly
  (`pkg/auth/base.go`), does not.

### Refusals (checked before anything is deleted; nothing changes on refusal)

- R6. The blockstore type does not support namespace deletion (for example `transient`): 400 `not_supported`.
- R7. Storage deletion is disabled in the server configuration (R20): 400 `not_supported`.
- R8. The repository is read-only: 400 `read_only_storage`, with or without `force`. A read-only repository never
  wrote its own marker, so its namespace may hold data that lakeFS does not own.
- R9. The namespace is the root of a bucket or container (S3/GCS bucket, Azure container, local root): 400
  `namespace_is_root`.
- R10. Another repository's namespace overlaps this one: equal to it, inside it, or containing it. This includes any
  repository being deleted and any pending storage deletion. Answer 409 `namespace_shared`, naming the other
  repository. Overlap is decided on path segments, so `…/mds-az7-e1f13de` does not overlap
  `…/mds-az7-e1f13de-metadata`.
- R11. A storage deletion for this repository name is already pending: 409 `deletion_in_progress`.

### Execution

- R12. The storage deletion deletes every object whose key is under `<storage namespace>/` and nothing else. It
  covers committed, staged, uncommitted and GC-pending data, `_lakefs/` ranges and metaranges, and on Azure with
  hierarchical namespace, the directory entries. `_lakefs/dummy` is deleted **last**.
- R13. The deletion is resumable. After a restart, a crash or a lost lease, another or the same replica continues
  from the last recorded position without listing the namespace from the start. Deleting an object that is already
  gone counts as success.
- R14. The deletion status is readable while it runs and for 24 hours after it ends:
  `GET /api/v1/repository_deletions/{repository}`, plus a list endpoint `GET /api/v1/repository_deletions`. Fields:
  `repository`, `storage_namespace`, `state` (`pending`, `deleting`, `done`, `failed`), `objects_deleted`,
  `requested_by`, `requested_at`, `updated_at`, `attempts`, `last_error`.
  - Reading a status needs `fs:ReadRepository` on the repository's ARN.
  - Listing statuses needs `fs:ListRepositories`.
- R15. Errors are retried with backoff and never dropped. An access-denied or other permanent error sets `failed`
  with `last_error` after 10 attempts. A `failed` deletion is retried by repeating the delete request with
  `delete_storage=true`: the repository no longer exists, so the request resumes the pending deletion by name.
- R16. While a storage deletion is pending, `CreateRepository` refuses the same repository name and any overlapping
  namespace with 409 `deletion_in_progress`. After `done`, both are allowed again (G5).

### Metadata Search (enterprise binary only)

- R17. Deleting a data repository R with `delete_storage=true` also deletes `R-metadata` with `delete_storage=true`,
  but only when `R-metadata` is a proven companion (the init-commit check, `pkg/catalog/metadata_companion.go`).
  The same caller's request covers it, and no separate authorization is checked on the companion: it is derived data
  of R.
- R18. Without `delete_storage`, R's companion is left as today, matching the MDS design (`METADATA_SEARCH_DESIGN.md:451`).
- R19. If R and its companion pass different refusal checks (R6–R11), nothing is deleted and the response names the
  repository that refused.

### Configuration and observability

- R20. Server configuration `repository_deletion.storage.enabled` (default `true`), `…interval` (default `30s`),
  `…batch_size` (default `1000`), `…max_attempts` (default `10`).
- R21. Metrics: `lakefs_repository_storage_deletions` (gauge by state) and
  `lakefs_repository_storage_deleted_objects_total`. Errors count through
  `lakefs_repository_storage_deletion_errors_total`. Every start, finish and failure is also logged with the
  repository and namespace.
- R22. Every accepted request is audit-logged as `delete_repo` with `delete_storage=true`.

### Clients

- R23. lakectl: `lakectl repo delete <uri> --delete-storage [--wait]`. The confirmation prompt names the storage
  namespace. `--wait` polls R14 until `done` or `failed` and exits non-zero on `failed`.
  `lakectl repo deletion-status <repo>` prints R14.
- R24. Web UI: the delete dialog shows an unchecked "Also delete all objects in `<storage namespace>`" checkbox when
  the blockstore supports it. Checked, it sends `delete_storage=true` and explains that the deletion runs in the
  background.
- R25. Generated SDKs (Python, Java, Rust) and the Python wrapper expose `delete_storage` and the status endpoints.

## 5. Known limitations (documented to users, not solved)

- L1. **References from other repositories.** A repository B can hold entries whose physical address is under A's
  namespace: imported from it, or linked with a physical address. Deleting A's storage breaks those entries in B.
  lakeFS cannot detect this cheaply. The UI, lakectl prompt and docs say so.
- L2. **Credentials.** lakeFS needs delete permission on the bucket or container. Without it, the deletion ends in
  `failed` with the provider's access-denied message (R15).
- L3. **Cost and time.** The deletion is paced by the provider's request rate. S3 deletes up to 1000 keys per request;
  Azure up to 256 per batch; GCS has one request per object.
- L4. **Versioned buckets / soft delete.** On S3 versioned buckets, deletes add delete markers. With Azure soft delete
  or GCS object versioning, deleted objects stay recoverable and still billed until the provider's retention ends.
  lakeFS does not purge versions.

## 6. Acceptance criteria

- A1. On S3 (MinIO), Azure (Azurite) and GCS (fake-gcs-server), plus local and mem, a repository with committed,
  staged and uncommitted objects is deleted with `delete_storage=true`. The namespace then lists empty, and a new
  repository on the same namespace is created successfully.
- A2. Objects in a sibling prefix (`<ns>-metadata/`) and in the parent prefix are still present afterwards.
- A3. Killing the server in the middle of a deletion, then starting it again, still ends in `done`.
- A4. With two replicas, the deletion completes once and every object is deleted.
- A5. Each refusal R6–R11 and R16 answers its status and code, and nothing is deleted.
- A6. A plain delete is byte-for-byte unchanged: `esti/golden/lakectl_repo_delete*.golden` and the existing
  controller tests pass unmodified.
- A7. MDS: deleting an indexed R with `delete_storage=true` removes R, `R-metadata` and both namespaces. The indexer
  then reports R as missing and does not recreate `R-metadata` (R16).

## 7. Sources

- treeverse/lakeFS#5566, "Provide option to delete storage on repo delete, or add an explanation":
  https://github.com/treeverse/lakeFS/issues/5566
- lakeFS forum, repository deletion leaves the data:
  https://forum.lakefs.io/t/16330034/hi-i-deleted-a-repository-and-then-i-checked-the-bucket-in-m
- lakeFS forum, storage namespace reuse:
  https://forum.lakefs.io/t/16076696/hello-u04r5jr13k9-and-i-are-wondering-if-the-fact-that-a-sto
- Metadata Search guide, metadata repository naming and storage namespace: https://docs.lakefs.io/guides/metadata-search/

## 8. Decisions taken in this spec (change here if you disagree)

- D1. **New action `fs:DeleteStorageNamespace`** rather than reusing `fs:AttachStorageNamespace`. Deleting data is
  a stronger right than pointing a repository at a location.
- D2. **202 + status endpoint** rather than a long-running 204. This meets G2 and gives lakectl `--wait` something to
  poll.
- D3. **Read-only and bucket-root namespaces are refused** (R8, R9) rather than allowed with `force`.
- D4. **Same-name re-creation is blocked until the deletion is done** (R16). This keeps one status per name and stops
  the MDS indexer from recreating a companion mid-deletion.
- D5. **The companion cascade checks no separate authorization** (R17).

## 9. Later (not in this spec)

- An orphan sweep: list namespaces under a configured root that have `_lakefs/dummy` but no repository.
- Cancelling a pending deletion.
