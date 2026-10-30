# Datasets

A **dataset** is a named, versioned, read-only collection of data curated from one or more
lakeFS repositories. Each item in a dataset is pinned to a commit, so a dataset version is a
stable, reproducible view over data that keeps moving in its source repositories.

Datasets are a **control plane** on top of the object data: no bytes are copied. A dataset
`customer-360` is materialised as a read-only lakeFS repository with the same name, whose
entries point — by full physical address — at the objects of the source repositories. That is
why every existing lakeFS read path works on a dataset without any change.

The companion design document is [`DATASETS_DESIGN.md`](../DATASETS_DESIGN.md).

## Enabling datasets

Datasets are disabled by default and are served by the `lakefs-sso` binary, which mounts the
control plane under `/api/v1/datasets`. Enable them with a `datasets:` block (see
[Configuration](#configuration)); an installation that has the block but sets
`enabled: false` answers `501` on every control-plane request.

## Definition format

A dataset version is described by a JSON definition with `format_version: 1`:

```json
{
  "name": "customer-360",
  "definition": {
    "format_version": 1,
    "description": "Curated customer view for analytics",
    "metadata": {
      "classification": "internal",
      "owner": "data-platform",
      "tier": "gold",
      "retention": "1y"
    },
    "data": [
      {
        "target": "customers",
        "repository": "lakehouse-repo",
        "ref": { "type": "commit", "id": "30f9eb1c9d0d4bb4b0d3d9a6f1f2a8b5a11f0c4e" },
        "type": "prefix",
        "prefix": { "path": "data/customers/" }
      },
      {
        "target": "reference/countries.csv",
        "repository": "ref-data",
        "ref": { "type": "commit", "id": "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4" },
        "type": "object",
        "object": { "path": "geo/countries.csv" }
      }
    ]
  },
  "publish": { "message": "Initial dataset with customer objects" }
}
```

Fields:

| Field | Meaning |
|---|---|
| `definition.format_version` | Always `1`. |
| `definition.description` | Free text, optional. |
| `definition.metadata` | String, number, boolean or array-of-string values. Values are validated against the metadata key rules of the installation, and free-form keys are accepted unless strict metadata mode is on. |
| `data[].target` | Path of the item inside the dataset. No leading `/`, no `.`/`..` segments. |
| `data[].repository` | Source repository. It must exist, must not be a dataset, and must live on the same storage as the dataset. |
| `data[].ref` | `{"type": "commit", "id": "<commit>"}`. On input, `branch` and `tag` are accepted and resolved to a commit; stored definitions only contain commits. |
| `data[].type` | `object` or `prefix`. |
| `data[].object.path` / `data[].prefix.path` | The source path. Prefix paths always end with `/`; an empty prefix is the whole repository. |
| `publish` | Optional on create: publish the definition as `v1` in the same request. `message` is the version message. |

Target semantics:

| Item type | Source | Target | Keys in the dataset |
|---|---|---|---|
| `object` | `geo/countries.csv` | `reference/countries.csv` | exactly `reference/countries.csv` |
| `prefix` | `data/customers/` | `customers` | `customers/<relative path>` for every `data/customers/<relative path>` |

Targets must not overlap: a target may not equal, or be a path prefix of, another target
(`a` and `a/b` collide; `a` and `a-b` do not). That is what makes each dataset key resolve to
exactly one source object.

Validation also checks that the submitted definition is internally consistent and that the
caller may read every source it attaches (see [Access](#access)). Publishing the same
definition twice is rejected as "no changes", and a publish that passes `expected_version`
older than the current `latest_version` is rejected with a conflict.

## Versions

Versions are dense integers: `v1`, `v2`, … Each publish writes one commit on the branch
`latest` of the dataset repository and tag `vN` for the same commit. `latest` always points at
the newest version. Version diffs are available for the definition (`items`, `metadata`,
`description`) and for the data:

```
GET /api/v1/datasets/customer-360/refs/v1/diff/v2
GET /api/v1/datasets/customer-360/refs/v1/diff/v2?type=data
```

Versions are immutable. Re-reading `v1` after `v3` exists returns exactly the data that was
pinned when `v1` was published.

## Reading a dataset

A dataset is read like any repository, using the dataset name as the repository and the
version as the ref (`v1`, `v2`, …, `latest`, or a commit ID):

```bash
lakectl fs ls  lakefs://customer-360/latest/customers/
lakectl fs cat lakefs://customer-360/v2/reference/countries.csv

aws s3 ls s3://customer-360/v3/customers/ --endpoint-url https://lakefs.example.com
```

Spark and DuckDB work unchanged, because they go through the S3 gateway:

```python
spark.read.parquet("s3a://customer-360/latest/customers/")
```

```sql
SELECT * FROM 's3://customer-360/v1/customers/*.parquet';
```

The REST API, the object browser, the SDKs and presigned URLs work as well: presigned S3
requests are signed against the lakeFS gateway like any other repository read, and
`presign=true` REST calls return a blockstore-presigned URL when the underlying blockstore
supports presigning (S3, GS and Azure Blob do; the local filesystem blockstore does not).

Writes are refused. The backing repository is read-only, so uploads, deletes, commits, tag
changes and branch changes through the data plane fail with a "read-only repository" error.

## Garbage collection safety

Dataset versions reference source objects by physical address, and the source repository's
garbage collection only retains objects reachable from *its own* branches. Without protection,
running GC on a source repository would delete the objects a dataset still serves.

For every `(source repository, commit)` that any dataset version references, the service keeps
a hidden branch in the source repository:

```
_lakefs_ds_pin_<commit-id>
```

Garbage collection treats every branch head as a starting point, and hidden branches are
included, so the whole tree at that commit is retained. Pins are reference counted per commit
(many datasets can share one pin) and the branch is removed when the last reference goes away,
which in practice is when the dataset is deleted. Branch names use the commit ID only, so a
pin does not leak dataset names into the source repository, and hidden branches do not appear
in default branch listings.

Practical consequences:

- **Tags do not protect data.** Tags are not garbage-collection starting points; a dataset
  pin is a branch on purpose.
- **Do not delete `_lakefs_ds_pin_*` branches.** If one is removed and GC runs before it is
  recreated, the affected objects are collected and dataset reads start returning "not found";
  the reconciler recreates missing pins and reports the dataset as degraded.
- **Deleting a source repository** also removes its pins. Dataset reads keep working until
  somebody removes the underlying storage data, but the dataset is reported as degraded.
- **Retention policies on source repositories are still worth setting** as defence in depth.
  They are not a substitute for pins: they are global and cover neither deleted branches nor
  old commits in general.
- The dataset repository's own GC is not a risk: it only lists the dataset's storage
  namespace, and dataset entries point outside it.

## Proposals

A dataset version can be published directly, or reviewed first through a dataset proposal
(pull request):

1. `POST /api/v1/datasets/{dataset}/proposals` materialises the proposed definition on the
   hidden branch `_proposal-<id>` in the dataset repository, so reviewers can browse the
   actual data at `lakefs://<dataset>/_proposal-<id>/…`, and records a diff against `latest`.
2. `POST /api/v1/datasets/{dataset}/proposals/{id}/merge` publishes the definition as the
   next version, records the proposal ID on that version and deletes the preview branch.
3. `PATCH /api/v1/datasets/{dataset}/proposals/{id}` with `{"status": "closed"}` closes a
   proposal without publishing and deletes the preview branch.

A proposal merges only on top of the version it was opened against. If `latest` moved in the
meantime the merge is rejected with a conflict ("rebase required"); update the proposal
against the current version and merge again. Rebasing is deliberately not automatic: the
conflicts are semantic, so the author decides.

## Access

Data-plane access to a dataset is access to the repository with the dataset's name, so a
consumer needs a grant on the dataset only — never on its source repositories:

```bash
curl -u "$LAKEFS_KEY:$LAKEFS_SECRET" -X POST \
  -H 'Content-Type: application/json' \
  -d '{"principal_type": "user", "principal_id": "analyst"}' \
  https://lakefs.example.com/api/v1/datasets/customer-360/access
```

The grant attaches a managed read policy (`dataset-<name>-read`) to the user or group, and the
managed `dataset-<name>-immutable` deny policy keeps every non-service principal from using
`force` to write into the dataset repository. Revoke with
`DELETE /api/v1/datasets/{dataset}/access/{principal_type}/{principal_id}`; listing is
`GET /api/v1/datasets/{dataset}/access`.

Control-plane actions are authorised on `arn:lakefs:datasets:::dataset/<name>`:
`datasets:ListDatasets`, `datasets:CreateDataset`, `datasets:ReadDataset`,
`datasets:PublishVersion`, `datasets:CreateProposal`, `datasets:DeleteDataset`,
`datasets:ManageAccess` and `datasets:ManageMetadataKeys`. Whoever creates or publishes a
dataset must be able to read every source it attaches (`fs:ReadCommit`, `fs:ReadObject`,
`fs:ListObjects` on the source repository); a failure names the item that could not be read.
On a merge the source check runs for the merging principal, because the merge publishes the
data under their authority.

### Access model note

`datasets.access_model` is `open` (the only value this build accepts):

- In the simplified ACL mode the base groups grant `fs:*`/`fs:Read*` on `*`. Members of
  `Readers` and above can therefore read **every** dataset through the data plane, whether or
  not they hold a dataset grant.
- Dataset grants consequently isolate only principals that are in **no** base group — external
  consumers, service accounts, and users created without group membership. For those, the
  grant is what makes a dataset readable, and it grants nothing else: the sources stay
  unreadable, and publishing is still denied.
- The control plane (`datasets:*`) is always governed by policies, never by the base groups'
  blanket read access.

A `strict` access model that replaces the global read ACLs with per-repository policies is not
part of this implementation; configuring it fails startup with a clear error instead of
silently behaving like `open`.

## Configuration

```yaml
datasets:
  enabled: true
  # Storage for dataset repositories. Must equal the storage ID of every source repository.
  storage_id: ""                                   # "" = the default single blockstore
  storage_namespace_prefix: "s3://my-bucket/lakefs-datasets"   # required when enabled
  max_items: 1000                                  # data items per definition
  allow_empty_items: false                         # allow prefix items matching no object
  sync_publish_max_objects: 100000                 # bigger publishes run async (202 + task)
  merge_fan_in: 64                                 # per-item iterators merged in one pass
  publish_timeout: 30m                             # stale transient state is rolled back
  reconcile_interval: 10m                          # background repair loop
  access_model: open                               # open (see the note above); strict is rejected
  immutable_guard_groups: []                       # extra groups that get the immutability deny policy
  metadata:
    strict: false                                  # reject metadata keys that are not defined
```

Each key also has the usual environment variable (`LAKEFS_DATASETS_ENABLED`,
`LAKEFS_DATASETS_STORAGE_NAMESPACE_PREFIX`, `LAKEFS_DATASETS_METADATA_STRICT`, …). An invalid
`datasets:` section with `enabled: true` fails startup instead of starting a half-configured
control plane.

The dataset storage must be the same blockstore as the sources: entries reference source
objects by full physical address, which only that blockstore can serve. Configurations that
span storage IDs are rejected.

## Current limitations

- **Items are `object` and `prefix` only.** Iceberg `table` and `namespace` items are not
  supported, and neither is `catalog.<dataset>.<ref>.<namespace>.<table>` addressing: there is
  no Iceberg REST catalog in this build. File-based tables (Parquet directories, Delta Lake,
  Hudi, Iceberg tables read through a Hadoop catalog) can be included as `prefix` items.
  Iceberg metadata that embeds absolute `s3://…` paths is not portable this way.
- **No version deletion or retention policies.** Versions are immutable and kept; a dataset's
  pins are released when the dataset is deleted.
- **No copy-on-publish ("snapshot") mode.** A dataset is always zero-copy, so it depends on
  the source objects being retained (which is what pins are for).
- **Single storage only.** A dataset cannot span blockstores with different storage IDs.
- **API compatibility is limited to the definition format and path shapes.** Definitions use
  `format_version: 1` and dataset refs use `vN`/`latest`, so they are portable, but this build
  makes no promise about matching any other implementation's wire behaviour field by field.
- **Typed metadata keys are enforced but not manageable over the API yet.** Metadata values
  are validated against the metadata key rules the installation has, and
  `datasets.metadata.strict` rejects undeclared keys; the metadata-key CRUD endpoints and
  `GET /datasets/config` are not implemented in this build and answer `501`.
- **Built-in datasets policies are not created by default.** The RBAC bootstrap can create the
  datasets policies, but it ships with the feature switched off, so a stock installation has
  no principal with `datasets:*`. Grant the control-plane actions explicitly (see
  [Access](#access)) or run the bootstrap with the datasets policies enabled before relying on
  the built-in `DatasetsFullAccess` / `DatasetsReadAll` policies.
- **No dataset-level Python helper.** Reads work through the lakeFS SDK, `lakectl`,
  Spark and the S3 gateway; the CLI helper is the `lakectl-datasets` plugin.
