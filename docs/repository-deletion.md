# Repository deletion with storage

A plain `DELETE /api/v1/repositories/{repository}` (and `lakectl repo delete`) removes only the
repository's metadata: branches, tags, commits and settings. Every object under the repository's
storage namespace stays in the object store, including the `_lakefs/dummy` marker, so the data keeps
costing money and the namespace cannot be reused — a new repository on it fails with "storage
namespace already in use".

`delete_storage=true` deletes the storage too. The request returns as soon as the metadata is gone;
the objects are deleted by a background worker, so the call takes about the same time whatever the
size of the namespace. The deletion survives a restart, is resumable, and ends as `done`, or as
`failed` with the provider's error.

The requirements are in [`REPO_DELETE_STORAGE_SPEC.md`](../REPO_DELETE_STORAGE_SPEC.md), the design
in [`REPO_DELETE_STORAGE_DESIGN.md`](../REPO_DELETE_STORAGE_DESIGN.md), and the configuration keys
in [`reference/configuration.md`](reference/configuration.md#repository-deletion-with-storage).

## Requesting it

On the API, add the optional boolean query parameter (default `false`):

```
DELETE /api/v1/repositories/my-repo?delete_storage=true
```

With `delete_storage=false` or absent, the request, its status codes and its response are exactly
what they were before: `204 No Content`, metadata only. With `delete_storage=true` the server
answers `202 Accepted` and returns a repository deletion record, the same object that
[`Status and retry`](#status-and-retry) describes. The repository's metadata is deleted inline, so
the repository normally answers `404` as soon as the request returns; if that delete failed, the
record carries the error and the worker retries it before touching any object.

From `lakectl`:

```bash
# Delete the repository and every object under its storage namespace.
lakectl repo delete lakefs://my-repo --delete-storage --yes
```

The confirmation prompt names the storage namespace, and a warning is printed even when `--yes`
skips the prompt. `--force` keeps its old meaning — bypass read-only protection for the metadata
delete — and does **not** make a read-only repository deletable with storage (see
[`Refusals`](#refusals-nothing-is-deleted)).

`--wait` polls the status every two seconds, prints the record, and exits non-zero when the state
is `failed`, with `last_error`:

```bash
lakectl repo delete lakefs://my-repo --delete-storage --wait --yes
```

Without `--wait`, the command returns after the `202` and the deletion continues in the background.

## What it deletes

- The repository's **metadata**: branches, tags, commits, and repository settings.
- **Every object whose key is under `<storage namespace>/`**, whatever put it there: committed
  data, staged data, uncommitted data on any branch, data that a garbage-collection run has marked
  but not yet swept, and the `_lakefs/` ranges and metaranges of the committed store.
- On Azure accounts with hierarchical namespace, the **directory entries** under the prefix, deepest
  first, after the files.
- `_lakefs/dummy` is deleted **last**. Until then the namespace still looks taken, including to
  another lakeFS replica that has not noticed the deletion.

On S3, incomplete multipart uploads under the prefix are aborted when the listing reaches the end:
they are billed and are not listed as objects. On Azure, uncommitted blocks are discarded by the
service after a week and need no cleanup. GCS has no equivalent of either.

## What it never deletes

- **Anything outside `<storage namespace>/`.** The delete prefix ends on a path boundary, so
  deleting `…/mds-az7-e1f13de` leaves `…/mds-az7-e1f13de-metadata/` and the parent prefix
  `…/lakefs/` alone. Objects in another bucket or container are never touched.
- **Data that lives somewhere else**: imported objects, entries linked to a physical address under
  another namespace, and external locations. They are not under this namespace, so this deletion
  does not reach them.
- **Object versions and soft-deleted objects.** lakeFS deletes the current object; it never purges
  versions. See [`Versioning and soft delete`](#versioning-and-soft-delete).
- **Namespaces left behind by repositories deleted before this feature existed.** There is no orphan
  sweep: the worker acts only on a deletion record this feature wrote.

**References from other repositories (the main caveat).** A repository `B` can hold entries whose
physical address is under `A`'s namespace: imported from `A`, or linked with a physical address. A
plain metadata delete of `A` left those bytes in place; deleting `A` with storage removes them, and
`B`'s entries then point at nothing. lakeFS cannot detect this cheaply, so it warns instead of
blocking: the web UI, the `lakectl` prompt and this page say so. If another repository may reference
this data, copy it into that repository's own namespace (or re-upload it there) before deleting.

## Required object-store permissions

lakeFS needs delete and list rights on the bucket or container. Without them the deletion retries,
then ends `failed` with the provider's access-denied message in `last_error`.

| Provider | What lakeFS calls | Permission |
|---|---|---|
| S3 | `ListObjectsV2`, `DeleteObjects`, `ListMultipartUploads`, `AbortMultipartUpload` | `s3:ListBucket`, `s3:DeleteObject`, `s3:ListBucketMultipartUploads`, `s3:AbortMultipartUpload` |
| Azure Blob Storage | `List Blobs`, `Blob Batch` deletes | **Storage Blob Data Contributor** on the container or storage account |
| GCS | `Objects` listing, one `Delete` per object | `storage.objects.list` and `storage.objects.delete` (for example `roles/storage.objectAdmin`) |
| Local | `filepath.WalkDir` and `os.Remove` | The OS user running lakeFS must be able to remove the namespace directory; no provider credentials |
| Mem | in-memory map | None |

The S3 permissions in one policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:DeleteObject",
        "s3:ListBucketMultipartUploads",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/*"
      ]
    }
  ]
}
```

**Storage Blob Data Contributor** already includes listing, writing and deleting blobs, which is
what Blob Batch's delete sub-requests need. On GCS, `storage.objects.delete` without
`storage.objects.list` fails before the first delete, because the worker has to list the prefix.

## Versioning and soft delete

The deletion removes the **current** objects. It does not purge history, so on a bucket with
versioning or soft delete the bytes may still exist and still be billed after the status says
`done`:

- **S3 versioned buckets.** A delete adds a delete marker. Older versions stay in the bucket until
  you expire them with a lifecycle rule or delete them yourself, and the namespace no longer lists
  them as current objects.
- **Azure soft delete and blob versioning.** Soft-deleted blobs (and containers) stay recoverable
  and billed for the configured retention period. Previous blob versions are separate objects from
  lakeFS's point of view.
- **GCS object versioning and soft delete.** Deleting a live object makes the previous generation
  noncurrent; noncurrent generations and soft-deleted objects remain until their lifecycle rules
  or retention remove them.

lakeFS does not purge versions, on any provider. "The namespace lists empty" therefore means "no
current objects", not "no bytes". If the storage must be fully reclaimed, configure the provider's
lifecycle or retention for the namespace yourself; that is outside lakeFS.

Without versioning, or after the retention period, the namespace is gone and a new repository can
be created on it.

## Refusals: nothing is deleted

Every check runs before the first write, and a refusal changes nothing. For a Metadata Search data
repository, the same is true when either the data repository or its companion refuses: the request
fails and names the repository that refused.

| Answer | When |
|---|---|
| `400 not_supported` | The blockstore cannot delete a namespace (`transient`), or `repository_deletion.storage.enabled` is `false`. |
| `400 read_only_storage` | The repository is read-only, with or without `--force`. A read-only repository never wrote its own marker, so its namespace may hold data lakeFS does not own. |
| `400 namespace_is_root` | The namespace is the root of a bucket, a container, or the local/mem scheme: there is no namespace-only prefix to delete under. |
| `409 namespace_shared` | Another repository, or another pending storage deletion, owns an equal, enclosing or enclosed namespace. The message names it. The comparison is on path segments, so `…/mds-az7-e1f13de` and `…/mds-az7-e1f13de-metadata` do not conflict. |
| `409 deletion_in_progress` | A storage deletion for this repository name is already pending. Wait for it, or retry it after it is `failed`. |

While a deletion record is not `done` — `pending`, `deleting` or `failed` — creating a repository
with the same name or an overlapping namespace is refused with `409 deletion_in_progress`. After
`done`, both are allowed: the namespace is reusable and the name can be taken again. A `failed`
record keeps the name and the namespace blocked until a retried deletion reaches `done`; only
`done` records expire, 24 hours after they end.

## Status and retry

The deletion record is readable while the deletion runs and for **24 hours** after it ends:

- `GET /api/v1/repository_deletions/{repository}` returns one record;
- `GET /api/v1/repository_deletions` lists them, with the usual `prefix`, `after` and `amount`
  pagination.

```bash
lakectl repo deletion-status lakefs://my-repo
```

A record has these fields:

| Field | Meaning |
|---|---|
| `repository` | The repository name. One record per name. |
| `storage_namespace` | The namespace being deleted, kept because the repository record is already gone. |
| `state` | `pending` (record written, not started), `deleting` (an attempt is running or waiting for a retry), `done`, or `failed`. |
| `objects_deleted` | Objects deleted so far, over every page and every retry. |
| `requested_by` | The user that sent the request. |
| `requested_at`, `updated_at` | Unix time in seconds. |
| `attempts` | Failed attempts so far. Reset when a `failed` deletion is retried. |
| `last_error` | The last failure, with the provider's message when the provider refused. Absent when there has been no failure. |

Reading one status needs `fs:ReadRepository` on the repository's ARN; listing the statuses needs
`fs:ListRepositories`.

**Retries are automatic and never dropped.** A failed attempt keeps the record `deleting`, increments
`attempts`, stores `last_error`, and schedules the next attempt at
`min(interval × 2^attempts, 10 minutes)`. When `attempts` reaches
`repository_deletion.storage.max_attempts` (default `10`), the state becomes `failed` and the worker
stops touching it.

**Retrying a failed deletion.** Repeat the delete request with `delete_storage=true`. The repository
no longer exists, so the request resumes the record by name: it resets `attempts`, keeps the step and
the last position, and the worker continues from there instead of listing the namespace from the
start. Repeating the request while the record is still `pending` or `deleting` is a
`409 deletion_in_progress`, not a retry.

The deletion is also resumable without a retry request: after a restart, a crash or a lost lease,
another or the same replica continues from the last recorded position. Deleting an object that is
already gone counts as success, so a page redone after a crash is harmless.

### How a deletion runs

One worker runs per lakeFS server process (it is not started by `lakefs-sso mds run`), scans every
`interval`, and processes one record at a time per process, so `N` replicas run at most `N`
deletions in parallel. A record moves through these steps:

1. **metadata** — retry the metadata delete if the request path's attempt failed;
2. **settle** — wait out the repository cache of other replicas, which could otherwise accept one
   more write against a repository that is already deleted;
3. **objects** — delete page after page under the namespace, keeping `_lakefs/dummy`, and record the
   last key after every page;
4. **verify** — one full pass from the start, removing anything a late writer added behind the
   marker; the step ends only after a pass that deletes nothing;
5. **marker** — delete `_lakefs/dummy`, and on Azure the remaining directory entries;
6. **finished** — set `state=done`.

## RBAC

Deleting storage is a stronger right than deleting metadata, so it needs two actions, both checked
together:

- `fs:DeleteRepository` on `arn:lakefs:fs:::repository/<repository>`;
- `fs:DeleteStorageNamespace` on `arn:lakefs:fs:::namespace/<storage namespace>`.

For `lakectl repo delete lakefs://my-repo --delete-storage` on namespace
`s3://my-bucket/my-repo`, a policy that allows exactly that is:

```json
[
  {
    "effect": "allow",
    "action": ["fs:DeleteRepository"],
    "resource": "arn:lakefs:fs:::repository/my-repo"
  },
  {
    "effect": "allow",
    "action": ["fs:DeleteStorageNamespace"],
    "resource": "arn:lakefs:fs:::namespace/s3://my-bucket/my-repo"
  }
]
```

`fs:*`, the built-in `FSFullAccess` policy and `AllAccess` include the new action. `FSReadWrite`
lists its actions explicitly and does not include it (it does not include `fs:DeleteRepository`
either). A caller with only `fs:DeleteRepository` gets `401`; a caller with both gets `202`.

Each accepted request is audit-logged as `delete_repo` with `delete_storage=true`.

## Metadata Search

On a server that serves Metadata Search (`iceberg.enabled` and `iceberg.metadata_search.enabled`
are both true), deleting an indexed data repository `R` with storage also deletes its companion
`R-metadata` and the companion's storage. The companion is deleted only when `<R>-metadata` exists
and passes the init-commit proof, so a user's own repository with that name is never cascaded; the
companion's
namespace is read from its own record, so an overridden `storage_namespace` is honoured. The
caller's two permissions on `R` cover the companion too: it is derived data of `R`, and no separate
authorization is checked.

Each repository gets its own deletion record: `lakectl repo deletion-status lakefs://R-metadata`
shows the companion's progress. If either repository fails a refusal check, nothing is deleted and
the response names the repository that refused.

A plain delete, without `delete_storage`, leaves the companion exactly as before. The indexer does
not recreate a companion after the cascade: a tick reads `R` first, finds it missing and reports
`R` as failing while it stays configured; a tick that raced the delete is refused by the pending
deletion record. See
[`Deleting a data repository`](metadata-search.md#deleting-a-data-repository).

## Observability

Every start, finish and failure is logged with `repository`, `storage_namespace`, `step` and
`objects_deleted` (each page is logged at debug level). The metrics are:

| Metric | Type | Labels |
|---|---|---|
| `lakefs_repository_storage_deletions` | gauge | `state` |
| `lakefs_repository_storage_deleted_objects_total` | counter | — |
| `lakefs_repository_storage_deletion_errors_total` | counter | `step` |

Alert on `lakefs_repository_storage_deletions{state="failed"}` staying above zero, and on
`lakefs_repository_storage_deletion_errors_total` increasing for longer than the backoff can
explain.
