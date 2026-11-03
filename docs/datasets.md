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

Datasets are served by the `lakefs-sso` binary, which mounts the control plane under
`/api/v1/datasets`. As in the documented configuration there is no separate switch to
remember: setting `datasets.storage_namespace_prefix` (see
[Configuration](#configuration)) enables the feature. `datasets.enabled` still exists as an
override — an explicit `enabled: false` wins, so an installation can keep the section for
later and answer `501` on every control-plane request.

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
| `definition.format_version` | Set by the server on write: it may be omitted and is stored as `1`. |
| `definition.description` | Free text, optional. |
| `definition.metadata` | String, number, boolean or array-of-string values on input. Responses always carry an array of strings per key, the `additionalProperties: {type: array, items: string}` shape of the Enterprise API. Values are validated against the metadata key rules of the installation, and free-form keys are accepted unless strict metadata mode is on. |
| `data[].target` | Path of the item inside the dataset. No leading `/`, no `.`/`..` segments. |
| `data[].repository` | Source repository. It must exist, must not be a dataset, and must live on the same storage as the dataset. |
| `data[].ref` | `{"type": "commit", "id": "<commit>"}`. On input, `branch` and `tag` are also accepted and resolved to a commit — an input superset of the Enterprise schema, which only has `commit`; stored definitions only contain commits. |
| `data[].type` | `object`, `prefix`, or - with the Iceberg REST catalog enabled (`iceberg.enabled`) - `table` and `namespace`. See [Iceberg table and namespace items](#iceberg-table-and-namespace-items). |
| `data[].object.path` / `data[].prefix.path` | The source path. Prefix paths always end with `/`; an empty prefix is the whole repository. |
| `data[].table` / `data[].namespace` | The Iceberg source of a `table`/`namespace` item: `{"namespace": ..., "name": ...}` for a table, the dotted namespace string for a namespace. |
| `publish` | Optional on create: it names version `v1`. A create always publishes `v1`; `message` defaults to `Create dataset <name>`. |

Target semantics:

| Item type | Source | Target | Keys in the dataset |
|---|---|---|---|
| `object` | `geo/countries.csv` | `reference/countries.csv` | exactly `reference/countries.csv` |
| `prefix` | `data/customers/` | `customers` | `customers/<relative path>` for every `data/customers/<relative path>` |
| `table` | table `analytics.orders` | `analytics/orders`, fixed | the table's reserved `_lakefs_iceberg/` entries |
| `namespace` | namespace `analytics` | `analytics`, fixed | the reserved `_lakefs_iceberg/` entries of the whole subtree |

Targets must not overlap: a target may not equal, or be a path prefix of, another target
(`a` and `a/b` collide; `a` and `a-b` do not). That is what makes each dataset key resolve to
exactly one source object.

Validation also checks that the submitted definition is internally consistent and that the
caller may read every source it attaches (see [Access](#access)). Publishing the same
definition twice is rejected as "no changes", and a publish that passes `expected_version`
older than the current `latest_version` is rejected with a conflict.

### Iceberg table and namespace items

When the lakeFS Iceberg REST catalog is enabled (`iceberg.enabled`, see
[Iceberg REST catalog](iceberg.md)) a definition may name Iceberg tables instead of file trees:

```json
{
  "target": "analytics/orders",
  "repository": "lakehouse-repo",
  "ref": { "type": "commit", "id": "30f9eb1c9d0d4bb4b0d3d9a6f1f2a8b5a11f0c4e" },
  "type": "table",
  "table": { "namespace": "analytics", "name": "orders" }
}
```

```json
{
  "target": "analytics",
  "repository": "lakehouse-repo",
  "ref": { "type": "commit", "id": "30f9eb1c9d0d4bb4b0d3d9a6f1f2a8b5a11f0c4e" },
  "type": "namespace",
  "namespace": "analytics"
}
```

- A `table` item copies the table's pointer entry and every staged file entry of the pinned
  snapshot (`_lakefs_iceberg/files/<table-uuid>/…`). A `namespace` item copies the namespace
  markers, the pointers of every table below it, and their files. A multi-level namespace is a
  dotted string: `analytics.sales` is the two-level namespace `[analytics, sales]`. A namespace
  level that contains a `.` cannot be expressed.
- The `target` of both types is fixed to the catalog path — `<namespace>/<table>` for a table,
  `<namespace>` for a namespace — because the reserved keys a dataset copies are repository-wide.
  Targets are also what the overlap rule compares, so a namespace item and a table inside it
  still collide: the namespace item already copies that table.
- Only a namespace item copies namespace markers. A dataset of table items alone exposes those
  tables but lists no namespaces; add a namespace item when the namespace itself should be
  readable. Table items never copy a marker, so two sibling tables can be attached together.
- The data is not copied: the source objects are shared and the dataset's pin branch keeps them
  out of garbage collection ([Garbage collection safety](#garbage-collection-safety)). That is
  what keeps the absolute `s3://…` paths inside Iceberg metadata valid.
- A table or namespace that does not exist at the pinned commit is a `400` naming the item.
- The result is read through the catalog at `<dataset>.<ref>.<namespace>.<table>`, and it is
  read-only like every dataset.

Attaching a `table` item requires the pair the catalog itself checks — `catalog:ReadTable` and
`catalog:ReadTableData` on `arn:lakefs:catalog:::table/<source-repo>/<namespace>/<table>` — plus
`fs:ReadRepository` and the per-object `fs:ReadObject` of every entry the materialiser reads. A
`namespace` item keeps the repository-wide object requirement, because one namespace covers many
tables.

Without the catalog (`iceberg.enabled` unset or false) both item types answer `501`, exactly as
before: nothing could materialise them and no catalog could serve the result.

## Versions

Versions are dense integers: `v1`, `v2`, … Each publish writes one commit on the branch
`latest` of the dataset repository and tag `vN` for the same commit. `latest` always points at
the newest version.

Every create or update produces the next version. The documented update is a synchronous
publish that answers the updated dataset:

```
PUT /api/v1/datasets/customer-360
{ "definition": { ... }, "publish": { "message": "v3" } }
```

A create needs no `publish` block to produce a version: `POST /api/v1/datasets` always
publishes `v1` and answers `201` with the `Dataset`. `POST
/api/v1/datasets/{dataset}/versions` remains available as an extension: it takes an optional
`expected_version` (an ETag-like check) and answers `202` with a task when the publish is
large; `?async=true` on the create is the same opt-in for the initial version.
`GET /api/v1/datasets/{dataset}/refs/{ref}` returns the dataset at `latest` or `vN`.

Refs are `latest` or `v<N>`: that is the documented grammar. A diff also accepts a pull
request's `source_ref`, `pr/<id>`. The bare `3`, upper-case `V3` and the older `proposal-<id>`
spellings are rejected with `400` rather than silently accepted.

A `Dataset` is `{"id": ..., "creation_date": <Unix seconds>, "version": N, ...}` and a
`DatasetVersion` is `{"id": "v3", "author": ..., "creation_date": <Unix seconds>, ...}`; the
`definition` of a dataset reports a per-item `status` (`ok`, `source_repo_unavailable`,
`source_ref_unavailable`, `caller_unauthorized`).

Version diffs are available for the definition (`data`, `metadata`, `description`) and for the
data:

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

A dataset built from Iceberg `table`/`namespace` items is also read through the lakeFS Iceberg
REST catalog: the dataset name is the repository and the version is the ref, so listing
namespaces and loading a table answer `<dataset>.<ref>.<namespace>.<table>`. The dataset is
read-only there as well.

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

## Pull requests

A dataset version can be published directly, or reviewed first through a dataset pull request:

1. `POST /api/v1/datasets/{dataset}/pulls` materialises the proposed definition on the hidden
   branch `_proposal-<id>` in the dataset repository, so reviewers can browse the actual data at
   `lakefs://<dataset>/_proposal-<id>/…`, and records a diff against `latest`. The pull request
   is also addressable through its ref, `pr/<id>`.
2. `GET /api/v1/datasets/{dataset}/pulls/{pullRequest}/diff` returns the structural diff
   against `latest`.
3. `POST /api/v1/datasets/{dataset}/pulls/{pullRequest}/merge` publishes the definition as the
   next version, records the pull-request ID on that version and deletes the preview branch.
   It takes no body and answers `200 {"version": N, "reference": "vN"}`.
4. `DELETE /api/v1/datasets/{dataset}/pulls/{pullRequest}` closes a pull request without
   publishing and deletes the preview branch (`204`). `PATCH` with `{"status": "closed"}`
   still closes one as well.

`GET /api/v1/datasets/pulls` lists every pull request of the installation (`status` is `open`,
`closed`, `merged` or `all`; `dataset` filters by dataset). The fields are `id`, `dataset`,
`title`, `description`, `author`, `status`, `base_version`, `latest_version`, `stale`,
`conflicted`, `source_ref`, `creation_date` and `closed_date`.

A pull request merges only on top of the version it was opened against. If `latest` moved in
the meantime `stale` is true and the merge is rejected with a conflict ("rebase required");
update the pull request against the current version and merge again. Rebasing is deliberately
not automatic: the conflicts are semantic, so the author decides.

A pull request may be opened before its dataset exists: merging it then creates the dataset and
publishes `v1`. If the name is taken by the time of the merge, `conflicted` is true and the
merge is rejected with `409`.

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

Control-plane actions are authorised on `arn:lakefs:dataset:::dataset/<name>`:
`dataset:ListDatasets`, `dataset:ReadDataset`, `dataset:CreateDataset`, `dataset:UpdateDataset`
(publishing a version, opening, updating or merging a pull request) and `dataset:DeleteDataset`;
the metadata-key actions `dataset:ListMetadataKeys`, `dataset:ReadMetadataKey`,
`dataset:CreateMetadataKey`, `dataset:UpdateMetadataKey` and `dataset:DeleteMetadataKey` are
authorised on `arn:lakefs:dataset:::dataset/*`. `dataset:ManageAccess` (grant and revoke consumer
access) is an extension of this implementation. The legacy plural `datasets:` namespace is still
accepted when policies are validated, and the managed policies are migrated automatically.
Whoever creates or publishes a dataset must be able to read every source it attaches. The check
matches the documented one and the core API: `fs:ListObjects` on the source **repository**
(`arn:lakefs:fs:::repository/<src>`) and `fs:ReadObject` on the source **object**
(`arn:lakefs:fs:::repository/<src>/object/<path>`), authorized per object while the version is
materialised; a failure names the item and the first path that could not be read. No
`fs:ReadCommit` is required — this build deliberately relaxes the attach-time check to the
documented pair. On a merge the source check runs for the merging principal, because the merge
publishes the data under their authority.

A `table` item is authorized on the catalog ARN instead: `catalog:ReadTable` and
`catalog:ReadTableData` on `arn:lakefs:catalog:::table/<src>/<namespace>/<table>`, the same pair
`/iceberg/api` checks when it loads a table and reads its data files. `fs:ReadRepository` on the
source repository is required for every item, and every entry the materialiser reads is still
authorized per object.

When datasets are configured, the RBAC bootstrap creates the four documented policies on
`arn:lakefs:dataset:::dataset/*`: `DatasetsFullAccess` (`dataset:*`), `DatasetsReadWriteAll`,
`DatasetsReadAll` and `DatasetsManageMetadataKeys`, and maps `Admins` to `DatasetsFullAccess`,
`SuperUsers` and `Developers` to `DatasetsReadWriteAll`, and `Viewers` to `DatasetsReadAll`.

A denied control-plane request answers `auth.authorization_failure_status`, the same setting the
core API uses: `401` by default, or `403` when the installation asks for it. Clients must accept
either status for a denial.

## Metadata keys

Metadata keys are the typed schema of dataset metadata. They are global (they apply to every
dataset) and are managed through:

```
GET    /api/v1/datasets/metadata          # ?prefix= filter
POST   /api/v1/datasets/metadata
GET    /api/v1/datasets/metadata/{key}
PUT    /api/v1/datasets/metadata/{key}
DELETE /api/v1/datasets/metadata/{key}
```

A `PUT` is a full replacement and requires `type`; changing `type` is rejected (`409`). The
`allowed_values` list of an `enum` key has the documented bounds: 1–256 values, each 1–256
characters, all distinct. Required keys are enforced on every definition write (create, update,
publish and pull requests), never on existing versions. `GET /api/v1/datasets/config` reports
the feature configuration.

Values in responses are always arrays of strings, so a client generated from the Enterprise
OpenAPI decodes them; on input a scalar is accepted as well (see
[Definition format](#definition-format)).

### Access model note

`datasets.access_model` is `open` (the only value this build accepts):

- In the simplified ACL mode the base groups grant `fs:*`/`fs:Read*` on `*`. Members of
  `Readers` and above can therefore read **every** dataset through the data plane, whether or
  not they hold a dataset grant.
- Dataset grants consequently isolate only principals that are in **no** base group — external
  consumers, service accounts, and users created without group membership. For those, the
  grant is what makes a dataset readable, and it grants nothing else: the sources stay
  unreadable, and publishing is still denied.
- The control plane (`dataset:*`) is always governed by policies, never by the base groups'
  blanket read access.

A `strict` access model that replaces the global read ACLs with per-repository policies is not
part of this implementation; configuring it fails startup with a clear error instead of
silently behaving like `open`.

## Configuration

```yaml
datasets:
  enabled: true                                    # optional override; a namespace prefix alone enables
  # Storage for dataset repositories. Must equal the storage ID of every source repository.
  storage_id: ""                                   # "" = the default single blockstore
  storage_namespace_prefix: "s3://my-bucket/lakefs-datasets"   # required when enabled
  max_items: 1000                                  # data items per definition
  max_request_body: 4MiB                           # largest control-plane request body (413 above it)
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
`LAKEFS_DATASETS_STORAGE_NAMESPACE_PREFIX`, `LAKEFS_DATASETS_METADATA_STRICT`, …). Setting only
`storage_namespace_prefix` is enough: as in the documented configuration, a configured prefix
enables the feature when `enabled` is unset, while an explicit `enabled: false` keeps it off. An
invalid `datasets:` section with `enabled: true` fails startup instead of starting a
half-configured control plane.

The dataset storage must be the same blockstore as the sources: entries reference source
objects by full physical address, which only that blockstore can serve. Configurations that
span storage IDs are rejected.

## Current limitations

- **Iceberg `table` and `namespace` items need the Iceberg REST catalog.** They are accepted
  when `iceberg.enabled` is on (see
  [Iceberg table and namespace items](#iceberg-table-and-namespace-items)) and answer `501`
  otherwise. Remaining gaps: a namespace level may not contain a `.`, a table item does not copy
  its namespace marker, and metadata a client writes outside lakeFS is not tracked. File-based
  tables (Parquet directories, Delta Lake, Hudi, Iceberg tables read through a Hadoop catalog)
  can still be included as `prefix` items. Iceberg metadata that embeds absolute `s3://…` paths
  needs a `table` item, because it is not portable as a `prefix`.
- **No version deletion or retention policies.** Versions are immutable and kept; a dataset's
  pins are released when the dataset is deleted.
- **No copy-on-publish ("snapshot") mode.** A dataset is always zero-copy, so it depends on
  the source objects being retained (which is what pins are for).
- **Single storage only.** A dataset cannot span blockstores with different storage IDs.
- **The compatibility target is the public Enterprise OpenAPI, ent-swagger v1.108**
  (`docs.lakefs.io/reference/api/enterprise/v1.108/swagger.yml`): the document format, the
  control-plane paths of this revision, the field names and the `dataset:*` RBAC actions follow
  it. The deliberate deviations from that document are:
  - `GET /datasets/config`;
  - `POST /{dataset}/validate` (dry run);
  - `POST /{dataset}/versions` and its asynchronous `202` (`?async=true` on create is the same
    opt-in for the initial version);
  - `GET /{dataset}/tasks/{id}`;
  - `GET|POST /{dataset}/access` and `DELETE /{dataset}/access/{principal_type}/{principal_id}`;
  - the `?type=data` extension of the diff;
  - the `dataset:ManageAccess` action, which the RBAC table does not define.
- **Typed metadata keys are enforced, and manageable over the API** (see
  [Metadata keys](#metadata-keys)); `datasets.metadata.strict` rejects undeclared keys.
- **Built-in datasets policies are created when datasets are configured.** The RBAC bootstrap
  creates `DatasetsFullAccess`, `DatasetsReadWriteAll`, `DatasetsReadAll` and
  `DatasetsManageMetadataKeys` and attaches them to the base groups (see [Access](#access)).
- **No dataset-level Python helper.** Reads work through the lakeFS SDK, `lakectl`,
  Spark and the S3 gateway; the CLI helper is the `lakectl-datasets` plugin.

### Known design divergences

These are documented gaps, not regressions: the behaviour below is what this build does, and
the design document says something stricter.

- **`datasets.merge_fan_in` is accepted but not applied.** The materialiser opens one
  metadata-only iterator per data item and merges them in one pass, instead of merging in
  two passes above the fan-in. `datasets.max_items` (1000 by default) bounds the number of
  open iterators, so the divergence costs memory, not correctness. The configuration key is
  validated and kept for compatibility.
- **The "identical metarange" check never fires.** A publish whose data would equal the
  previous version is rejected earlier, by comparing the definition, and the metarange
  includes the synthetic `_dataset/definition.json` entry, which carries the version number.
  The metarange comparison in `materialize.go` therefore can never succeed for a new version;
  it stays as a defensive check for a caller that knows only the previous metarange.
- **A commit on `latest` without `dataset.version` is not detected.** The reconciler repairs
  and rolls back *transient* dataset records (`creating`, `publishing`, `deleting`). A commit
  that reached `latest` without the `dataset.version` metadata on an otherwise `active`
  dataset is not found by the repair pass, because only the version tags, not every commit
  of `latest`, are inspected. Publishing the next version still tags `latest + 1` correctly.
