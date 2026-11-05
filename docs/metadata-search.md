# Metadata Search

Metadata Search makes the object metadata of a lakeFS repository queryable with any Iceberg
engine: path, size, last modified time, etag, content type, committer and user metadata, at any
indexed branch, tag or commit. There is no separate query service and no new query language — an
engine connects to the lakeFS Iceberg REST catalog and reads one table:

```
my-repo.main.system.object_metadata
```

The table is **virtual**: it lives in the companion repository `my-repo-metadata` and is kept in
sync by a background indexer. The catalog resolves the identifier above to the real table of the
companion repository, on the branch of the same name, and authorizes the read against the *data*
repository. The feature is off by default, and it is an OSS implementation of the design in
[`../METADATA_SEARCH_DESIGN.md`](../METADATA_SEARCH_DESIGN.md) and the plan in
[`../METADATA_SEARCH_PLAN.md`](../METADATA_SEARCH_PLAN.md); section numbers below refer to the
design.

This page is the operator and user guide: how to enable it, how to run the indexer, how to query
the table from each engine, what the columns mean, how fresh they are, what the limits are, and
how to maintain the companion repository. The Iceberg catalog itself, its addressing forms,
staged commits and authorization model are documented in [`iceberg.md`](iceberg.md).

## How it works

- **One table per branch.** For each data repository `R` the indexer maintains a companion
  repository `R-metadata`. Its branches mirror the indexed branches of `R`, and each branch holds
  the Iceberg table `system.object_metadata`.
- **One row per object per branch.** A commit that adds or changes an object replaces its row; a
  commit that removes it deletes the row. The row carries the commit that last changed the object
  (`commit_id`), so a query can join metadata back to history.
- **Atomic per source commit.** Every commit of `R` becomes one Iceberg transaction followed by one
  lakeFS commit of `R-metadata`. Readers on the branch head see a snapshot that corresponds to a
  commit of `R`, never a half-applied one.
- **Eventually consistent.** The indexer runs on a period. A query is as fresh as the last indexed
  commit; [`Freshness`](#freshness) shows how to read the exact position.
- **Any ref.** A branch of `R` maps to the same branch of `R-metadata`. A tag, a full commit ID, a
  short commit ID or a ref expression such as `main~1` maps through the indexer's `mds-<commit>`
  tags, so history is addressable too.

## Setup

### 1. Enable the catalog and Metadata Search on the server

Both switches live in the `iceberg:` section and are read by `lakefs-sso` at startup. They are
server settings, not client ones:

```yaml
iceberg:
  enabled: true
  metadata_search:
    enabled: true
```

The second key is the only Metadata Search setting the server itself reads (design §5.4). While
`iceberg.enabled` is true and `iceberg.metadata_search.enabled` is true:

- `<repo>.<ref>.system.object_metadata` resolves to the companion repository;
- the `system` namespace of every data repository is reserved and read-only for clients;
- the catalog is mounted a second time at `/mds/iceberg/api`, the upstream endpoint
  ([`iceberg.md` § The upstream endpoint](iceberg.md#the-upstream-endpoint-mdsicebergapi)).

While either switch is false the virtual table does not resolve, no namespace is reserved and
`/mds/iceberg/api` is not mounted: the request falls through to the core router's not-implemented
placeholder and answers **`501 Not Implemented`**, the same answer a plain `lakefs` binary gives at
`/iceberg/api` (design §5.5; `pkg/api/services.go`, mounted by `cmd/lakefs/cmd/run.go`). It is not a
`404`. Metadata Search is a mode of the catalog, so `iceberg.metadata_search.enabled: true` with
`iceberg.enabled: false` is a **startup error**, not a half-applied state: the server refuses it with
`metadata_search.enabled requires enabled: Metadata Search is served by the Iceberg catalog, which is
off` and installs no principals (design §5.4). Set both keys or neither. There is no per-repository
switch in this version: enabling the
feature with a configuration that lists no repositories is harmless, because the indexer, not the
catalog, decides what is indexed.

### 2. Principals and credentials

Enabling the feature also makes `lakefs-sso` bootstrap two principals and two policies on every
startup (design §8). The bootstrap is idempotent: it creates what is missing, repairs a policy
whose statements drifted from the built-in ones, and tolerates a rerun.

| Principal | Created by | Purpose |
|---|---|---|
| user `mds-indexer`, member of group `MetadataSearchIndexers` | `lakefs-sso` bootstrap | the identity the indexer authenticates as |
| group `MetadataSearchIndexers` | `lakefs-sso` bootstrap | carries the `mds-indexer` allow policy, and only that policy |
| policy `mds-indexer` (allow) | `lakefs-sso` bootstrap | read on every data repository, full `fs:`/`catalog:` access on the companion repositories |
| policy `mds-metadata-repo-protect` (deny) | `lakefs-sso` bootstrap | keeps ordinary principals from writing the companion repositories |

The indexer's allow policy is deliberately narrow on data repositories: `fs:ReadRepository`,
`fs:ListObjects`, `fs:ReadObject`, `fs:ListBranches`, `fs:ReadBranch`, `fs:ListCommits` and
`fs:ReadCommit` on `*`. Its write grants name each companion repository explicitly — `fs:*` on
`arn:lakefs:fs:::repository/<R>-metadata` and below it, `catalog:*` on its table, namespace and view
ARNs, `fs:CreateRepository` on its repository ARN and `fs:AttachStorageNamespace` on
`arn:lakefs:fs:::namespace/*-metadata` — which covers creating the companion repository, its branches,
objects and tables. The set is every repository in the catalog plus the names in
`metadata_search.repositories` (the server reads that list for this purpose), mapped `R` →
`R-metadata`. A `*-metadata` wildcard is deliberately not used: lakeFS policy matching lets `*` span
`/`, so `repository/*-metadata` would also match an object or branch of a data repository whose key
ends in `-metadata`. A companion repository whose
`metadata_search.repositories.<R>.storage_namespace` does not end in `-metadata` is outside the
namespace grant and needs an extra statement (see [`Limits`](#limits-and-known-deviations)).

The same exact set is used by the deny policy, and the server re-derives it every minute; a repository
created after startup is therefore covered without a restart. Until the next refresh, the indexer fails
closed on that repository (an authorization error in `/_health`, never a silent write) and the deny does
not yet cover its companion repository. That one-minute window is an **accepted trade-off**: the policies
must name companion repositories exactly (a `*-metadata` wildcard would also match data-repository
objects, see below) and an exact ARN is a snapshot, so a repository created in the last minute is the
price of the exact naming. The wait is bounded by the refresher and needs no operator action, and the
uncovered state is loud — the new repository's first tick fails in `/_health` — rather than silent. The
deny covers `fs:UpdateRepository`, which writes repository metadata, through `fs:Update*`, and the import
pair (`fs:ImportFromStorage`, `fs:ImportCancel`) through `fs:Import*`; see
[`Permissions`](#permissions) for what each of the two import actions is and is not denied on.

Credentials are **not** created by the bootstrap: an administrator creates them once and places
them in the indexer configuration.

```bash
lakectl auth users credentials create --id mds-indexer
```

```
Credentials created successfully.
Access Key ID:     AKIA...
Secret Access Key: ...
```

Put that pair in `metadata_search.access_key_id` and `metadata_search.secret_key` below. Store the
file with restrictive permissions and prefer the environment variables for the secret.

The deny policy is attached to the base groups of both lakeFS group templates — `Admins`, `Supers`,
`Writers`, `Readers`, `SuperUsers`, `Developers` and `Viewers` — plus any group listed in
`datasets.immutable_guard_groups`, which is the same list the installation-wide datasets deny and
the reserved-prefix deny are attached to. It is never attached to `MetadataSearchIndexers`, and the
bootstrap drops that group from the list with a warning even when
`datasets.immutable_guard_groups` names it (design §8). It denies `fs:Write*`, `fs:Delete*`,
`fs:Create*`, `fs:Revert*`, `fs:Update*`, `catalog:Create*`, `catalog:Update*`, `catalog:Delete*` and
`catalog:WriteTableData` on the exact ARNs of the companion repositories, so an administrator's
`fs:*` allow cannot make a companion repository a hand-edited second source of truth. The indexer
and the catalog are not in those groups and keep writing.

**Auth backends without policies.** The default `auth.basic` service stores a single administrator
and implements no policy API. The bootstrap detects that, logs a warning naming what is not
enforced, and continues; it does not create `mds-indexer` or take over the administrator account.
In that mode there is no automatically provisioned indexer user and no deny policy, so either run
the indexer with the installation administrator's credentials, or use a policy-capable auth
backend (for example `auth.type: postgres`), which is what a shared or production installation
should do.

### 3. Indexer configuration

The indexer reads the **top-level** `metadata_search:` section of a YAML file. The file may be the
same file the server uses; the server ignores this section, and the indexer ignores the rest.

```yaml
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000       # required; lakeFS base URL
  access_key_id: AKIA...                       # required; the mds-indexer key
  secret_key: ...                              # required
  listen_address: 0.0.0.0:8080                 # /_health, /metrics, /_pprof/
  period: 60s                                  # > 0
  request_timeout: 60s                         # bound on one HTTP request, > 0
  concurrency: 25                              # branches in parallel, > 0
  max_commits: 1000                            # per branch per tick, > 0
  since: 2026-01-01T00:00:00Z                  # optional, RFC 3339
  backfill: head                               # head | history
  seed_lookback: 1000                          # commits searched for an indexed ancestor
  rebuild_threshold: 100000                    # change-set size that triggers a rebuild
  delete_branches: true                        # mirror branch deletion
  tag_retention: 0s                            # 0 keeps every mds-* tag forever
  compaction:
    max_delete_files: 200                      # delete files that trigger a compaction
    every_commits: 500                         # commits between compactions
  storage:                                     # optional; see "Credentials for writing files"
    s3.endpoint: http://seaweedfs:8333
    s3.region: us-east-1
    s3.access-key-id: ...
    s3.secret-access-key: ...
    s3.path-style-access: "true"
  repositories:                                # at least one
    my-repo:
      branches: [main, "dev-*"]                # at least one; exact or trailing-* glob
      storage_namespace: s3://bucket/metadata/my-repo   # optional
```

The same keys are listed one by one in
[`reference/configuration.md`](reference/configuration.md).

Every key, its default and its meaning:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `lakefs_mds_endpoint` | string | — (required) | Base URL of lakeFS. The API is used at `/api/v1` and the catalog at `/iceberg`; do not append either. |
| `access_key_id` | string | — (required) | Access key of `mds-indexer`. |
| `secret_key` | string | — (required) | Secret key of `mds-indexer`. |
| `listen_address` | string | `0.0.0.0:8080` | Address of the indexer's own HTTP server: `/_health`, `/metrics`, `/_pprof/`. |
| `period` | duration | `60s` | Interval between ticks. Every tick processes every configured repository. |
| `request_timeout` | duration | `60s` | Bound on every HTTP request the indexer sends, on the lakeFS API client and on the Iceberg catalog client (including its `GET /v1/config`). A request that exceeds it fails the branch, or the repository when the call is not attributable to one branch, so `/_health` answers `503` instead of staying `200` while indexing hangs. `0` is rejected. |
| `concurrency` | int | `25` | Number of branches one tick processes in parallel. Within a branch everything is sequential. |
| `max_commits` | int | `1000` | Upper bound on source commits one branch applies per tick. The next tick resumes at the recorded position. |
| `since` | RFC 3339 timestamp | unset | Oldest commit a first indexing starts at, by creation date. When set, the indexer walks first-parent history back to the oldest commit created at or after it, whatever `backfill` says. |
| `backfill` | `head` or `history` | `head` | What the first indexing of a branch covers when `since` is unset: the head commit only (`head`, the default) or the branch's oldest first-parent commit (`history`, like upstream's "branch creation"). |
| `seed_lookback` | int | `1000` | How many first-parent commits of a new branch the indexer walks back looking for an already-indexed ancestor whose `mds-*` tag it can seed the new branch from. |
| `rebuild_threshold` | int | `100000` | If one commit's upserts plus deletes exceed this, the branch's table is rebuilt from a full listing instead of applying a delta. |
| `delete_branches` | bool | `true` | Delete `R-metadata@b` when branch `b` of `R` is deleted. Its `mds-*` tags stay. |
| `tag_retention` | duration | `0s` | Delete `mds-*` tags whose source commit is older than this. `0` keeps them forever. |
| `compaction.max_delete_files` | int | `200` | Compact a branch's table when it has more delete files than this. `0` disables this trigger. |
| `compaction.every_commits` | int | `500` | Compact a branch's table after this many indexed commits. `0` disables this trigger. The count is read back from the first-parent commit log of the companion branch, so a restart neither defers nor advances a compaction. |
| `storage` | map | empty | iceberg-go IO properties used when the catalog does not vend write credentials. See below. |
| `repositories` | map | — (required) | The data repositories to index, by name. At least one entry. |
| `repositories.<R>.branches` | list | — (required) | Branch selectors: exact names, or a prefix with a single trailing `*` (`dev-*`). At least one. |
| `repositories.<R>.storage_namespace` | string | derived | Storage namespace of `R-metadata`. See [`Limits`](#limits-and-known-deviations). |

`metadata_search` is read with viper, so scalar keys can also be set through the lakeFS environment
conventions: prefix `LAKEFS_`, replace every `.` with `_`, upper-case. For example
`LAKEFS_METADATA_SEARCH_SECRET_KEY` and `LAKEFS_METADATA_SEARCH_LAKEFS_MDS_ENDPOINT`. The `repositories`
and `storage` maps are read from the file.

The command validates the section before the first tick and reports **one error per problem**, so a
bad file is fixed in one pass. Startup fails when:

- `lakefs_mds_endpoint`, `access_key_id` or `secret_key` is empty;
- `period`, `concurrency`, `max_commits`, `seed_lookback` or `rebuild_threshold` is not positive;
- `request_timeout` is not positive;
- `backfill` is neither `head` nor `history`;
- `repositories` or a `repositories.<R>.branches` list is empty;
- a repository name ends in `-metadata`, or is longer than 54 characters;
- a branch selector contains a `*` that is not the final character, or is empty;
- `since` is not an RFC 3339 timestamp;
- `tag_retention`, `compaction.max_delete_files` or `compaction.every_commits` is negative.

`0` is a valid value, not a mistake: `tag_retention: 0s` keeps every `mds-*` tag forever, and
`compaction.max_delete_files: 0` / `compaction.every_commits: 0` turn that compaction trigger off. The
defaults in the table above apply only when the key is **absent**; a negative value is refused at startup
rather than silently replaced by the default (design §7).

Per-repository problems that can only be discovered against the server — the data repository does
not exist, `R-metadata` cannot be derived or overlaps `R`, `R-metadata` is not the indexer's, or
`R-metadata` is read-only — do
not stop the process: that repository is marked failed in `/_health` and in the metrics, and the
other repositories continue.

#### Credentials for writing files

The indexer writes Parquet and Avro files for the table, so it needs write access to the companion
repository's object-store prefix. There are two modes (design §4.6):

- **Vended credentials.** If the catalog vends write credentials (S3 blockstore with
  `blockstore.s3.credentials_vending.role_arn` set), the indexer requests them the way any Iceberg
  client does (`X-Iceberg-Access-Delegation: vended-credentials`) and needs no object-store keys of
  its own.
- **Static storage properties.** Otherwise, set `metadata_search.storage` to the iceberg-go IO
  properties for the object store: `s3.endpoint`, `s3.region`, `s3.access-key-id`,
  `s3.secret-access-key`, `s3.path-style-access`. These are the same keys an Iceberg client uses.
  This is the mode the esti suite runs with (SeaweedFS and static keys).

Azure and GCS are out of scope for this version: there is no credential vending and no remote
signer, so those deployments must supply static properties for their scheme and are not tested.

The indexer does not use `storage` to read the catalog: catalog requests are authenticated with
`access_key_id`/`secret_key`. A deployment that vends credentials does not need the `storage`
section at all.

### 4. Run the indexer

```bash
lakefs-sso mds run --config /etc/lakefs/config.yaml
```

`--config` is required. The process reads the `metadata_search` section, binds
`listen_address`, and ticks until `SIGTERM` or `SIGINT`; it shuts down gracefully and logs
`mds: indexer stopped`. Run **one** replica. Correctness under accidental concurrency comes from the
Iceberg compare-and-swap and the crash recovery, not from a lease, so a second replica mostly wastes
work and produces `409` responses. A second process that meets the first on a brand-new companion
repository is harmless: the `mds: init` commit carries the table creation, so only one of them commits
it and the other adopts it instead of failing the repository as foreign.

What a first run does, in order (design §4.2, §4.5, §4.8):

1. For every configured repository, ensure `R-metadata` exists, initialise it with the
   `system` namespace, the table and an `mds: init` commit, and record its default branch.
2. For every branch of `R` that matches `branches`, create the mirrored branch in `R-metadata`.
   A new branch is seeded from the `mds-*` tag of the nearest already-indexed first-parent ancestor
   within `seed_lookback`; otherwise it is built from a full listing as described by `backfill` and
   `since`.
3. Then apply the source commits after the recorded position, oldest first, up to `max_commits`
   per tick.

The indexer is safe to restart at any point. A crash between the Iceberg commit (staged on the
branch) and the lakeFS commit is recovered by committing the staged table; a crash after the lakeFS
commit but before the tag is recovered by creating the tag — a tag that points at a commit that does
not record the position is deleted and created again, because lakeFS never overwrites a tag even
with `force`. A branch with uncommitted changes that are not the expected next commit is reset and
re-indexed; that includes foreign staged changes whose table property happens to record the
expected commit, because the indexer also asks the branch for its uncommitted changes. A history
rewrite discards the staging before it moves the branch. When a concurrent writer moves the table,
a `409` restarts the branch tick up to 3 times before the branch fails for that tick. Orphaned data
files from an interrupted Iceberg commit are unreferenced and removed by `lakectl iceberg cleanup`
(see [`Maintenance`](#maintenance)).

### 5. Verify

```bash
curl -s http://indexer:8080/_health | jq .
curl -s http://indexer:8080/metrics | grep '^lakefs_mds_'
```

`/_health` answers `200` while every repository's last tick succeeded or is in progress, and `503`
as soon as one repository or branch carries an error. A tick in progress does **not** hide the result
of the previous one: a repository whose last tick failed stays `503` for the whole of its retry, and a
tick cancelled by a graceful stop ends `in_progress` without changing the previous result. Its body is
per repository and per branch:

```json
{
  "healthy": true,
  "repositories": {
    "my-repo": {
      "in_progress": false,
      "branches": {
        "main": {
          "last_indexed": "a1b2c3...",
          "head": "a1b2c3...",
          "lag_commits": 0
        }
      }
    }
  }
}
```

`last_error` appears only when the last attempt failed. A repository-level failure (missing
repository, companion namespace not derivable or overlapping, companion repository read-only) is on the
repository object; a branch
failure is in that branch's object and clears on the next successful attempt. A branch that is no
longer mirrored — deleted in `my-repo`, or no longer matching `branches` — is removed from the
report together with its `lakefs_mds_lag_commits` series, whatever `delete_branches` says, so its
last error does not keep `/_health` at `503` until the indexer restarts (design §4.8).

`lag_commits` is normally the exact number of source commits not indexed yet. It is **`-1`** when the
last attempt failed before it could count them: the branch then also reports the position it recorded
(`last_indexed`), so a stuck branch shows *where* it stopped and an unknown lag rather than a
misleading `0` (design §9). Alert on `lag_commits != 0` rather than `> 0` to catch that state; a
healthy branch never reports `-1`.

## Querying

The table is addressed in the two ways the catalog supports. Both serve the same data.

| Form | URL | Identifier |
|---|---|---|
| `relative_to` | `http://lakefs:8000/iceberg/relative_to/my-repo.main/api` | `system.object_metadata` |
| upstream endpoint | `http://lakefs:8000/mds/iceberg/api` | `my-repo.main.system.object_metadata` |

The `/iceberg/api` form also serves `my-repo.main.system.object_metadata`; `/mds/iceberg/api` is
the same catalog mounted at the prefix upstream clients use. There is no `relative_to` form under
`/mds/iceberg/api`: the repository and ref stay in the identifier.

Every snippet below assumes:

- the lakeFS server is at `http://lakefs:8000`;
- the data repository is `my-repo` and the ref is `main`;
- the examples pass S3 credentials explicitly for reading data files. Omit those properties when the
  catalog vends read credentials.

A query principal needs read access to `my-repo` (through the catalog) and nothing on
`my-repo-metadata`; see [`Permissions`](#permissions).

### PyIceberg

`/mds/iceberg/api`, the identifier form:

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "lakefs",
    uri="http://lakefs:8000/mds/iceberg/api",
    credential="<access_key_id>:<secret_access_key>",
    warehouse="s3://my-bucket",          # the data repository's storage namespace
    **{
        "s3.access-key-id": "<key>",
        "s3.secret-access-key": "<secret>",
        "s3.region": "us-east-1",
    },
)
table = catalog.load_table(("my-repo", "main", "system", "object_metadata"))
table.scan().to_pandas()
```

`/iceberg/relative_to`, where the URL fixes both the repository and the ref:

```python
catalog = RestCatalog(
    "lakefs",
    uri="http://lakefs:8000/iceberg/relative_to/my-repo.main/api",
    credential="<access_key_id>:<secret_access_key>",
    warehouse="s3://my-bucket",
    **{
        "s3.access-key-id": "<key>",
        "s3.secret-access-key": "<secret>",
        "s3.region": "us-east-1",
    },
)
table = catalog.load_table("system.object_metadata")
```

For a tag or a commit, put it in the place of `main`:
`http://lakefs:8000/iceberg/relative_to/my-repo.v1.0/api`, or load
`("my-repo", "v1.0", "system", "object_metadata")` through `/mds/iceberg/api`. A filtered scan is
pushed down as Iceberg expressions:

```python
from pyiceberg.expressions import GreaterThan, StartsWith

table.scan(
    row_filter=GreaterThan("size_bytes", 1_000_000) & StartsWith("path", "orders/"),
).to_pandas()
```

### DuckDB

DuckDB uses the `relative_to` form, because its `ENDPOINT` is a base URL that fixes the warehouse.
The catalog secret authenticates the client to lakeFS; the S3 secret lets DuckDB read the data
files when the catalog does not vend credentials.

```sql
INSTALL iceberg; LOAD iceberg;

CREATE OR REPLACE SECRET lakefs_catalog (
  TYPE iceberg,
  CLIENT_ID '<access_key_id>',
  CLIENT_SECRET '<secret_access_key>',
  OAUTH2_SERVER_URI 'http://lakefs:8000/iceberg/api/v1/oauth/tokens'
);

CREATE OR REPLACE SECRET lakefs_data (
  TYPE s3,
  KEY_ID '<key>',
  SECRET '<secret>',
  REGION 'us-east-1'
  -- , ENDPOINT 'seaweedfs:8333', URL_STYLE 'path', USE_SSL false  -- non-AWS S3
);

ATTACH 's3://my-bucket' AS lakefs (
  TYPE iceberg,
  ENDPOINT 'http://lakefs:8000/iceberg/relative_to/my-repo.main/api'
);

SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs."system"."object_metadata"
WHERE path LIKE 'orders/%'
ORDER BY size_bytes DESC
LIMIT 20;
```

The `/mds/iceberg/api` form keeps the repository and ref in the identifier, so the schema has three
levels:

```sql
ATTACH 's3://my-bucket' AS lakefs (
  TYPE iceberg,
  ENDPOINT 'http://lakefs:8000/mds/iceberg/api'
);

SELECT path, size_bytes, commit_id
FROM lakefs."my-repo.main.system"."object_metadata"
WHERE commit_id = '<full source commit ID>';
```

For `/mds/iceberg/api`, the OAuth token URI is
`http://lakefs:8000/mds/iceberg/api/v1/oauth/tokens`. To use a lakeFS JWT instead of a key pair,
`ATTACH ... (TYPE iceberg, ENDPOINT '…', TOKEN '<lakefs-jwt>')` works for a short-lived read.

### Spark

PySpark with the Iceberg Spark runtime. The catalog name (`lakefs`) is arbitrary; the URI must not
include `/v1`. The S3 FileIO properties are only needed when the catalog does not vend
credentials; omit them when it does.

`/mds/iceberg/api`:

```properties
spark.sql.defaultCatalog=lakefs
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakefs=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakefs.type=rest
spark.sql.catalog.lakefs.uri=http://lakefs:8000/mds/iceberg/api
spark.sql.catalog.lakefs.rest.auth.type=basic
spark.sql.catalog.lakefs.credential=<access_key_id>:<secret_access_key>
spark.sql.catalog.lakefs.warehouse=s3://my-bucket
# When the catalog does not vend credentials, the Iceberg S3FileIO reads the data files:
spark.sql.catalog.lakefs.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakefs.s3.endpoint=http://seaweedfs:8333
spark.sql.catalog.lakefs.s3.path-style-access=true
spark.sql.catalog.lakefs.s3.access-key-id=<key>
spark.sql.catalog.lakefs.s3.secret-access-key=<secret>
spark.sql.catalog.lakefs.client.region=us-east-1
```

```sql
SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs.`my-repo`.`main`.`system`.object_metadata
WHERE path LIKE 'orders/%';
```

`/iceberg/relative_to`:

```properties
spark.sql.defaultCatalog=lakefs
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakefs=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakefs.type=rest
spark.sql.catalog.lakefs.uri=http://lakefs:8000/iceberg/relative_to/my-repo.main/api
spark.sql.catalog.lakefs.rest.auth.type=basic
spark.sql.catalog.lakefs.credential=<access_key_id>:<secret_access_key>
spark.sql.catalog.lakefs.warehouse=s3://my-bucket
spark.sql.catalog.lakefs.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakefs.s3.endpoint=http://seaweedfs:8333
spark.sql.catalog.lakefs.s3.path-style-access=true
spark.sql.catalog.lakefs.s3.access-key-id=<key>
spark.sql.catalog.lakefs.s3.secret-access-key=<secret>
spark.sql.catalog.lakefs.client.region=us-east-1
```

```sql
SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs.system.object_metadata
WHERE path LIKE 'orders/%';
```

There are three namespace levels in the identifier form (`my-repo`, `main`, `system`), so quote
each level separately, as above. Writing the whole dotted namespace as one backquoted token makes
it one namespace level, and the catalog rejects it with
`namespace "my-repo.main.system" must start with a repository and a ref`.

Iceberg 1.8 takes the AWS region from `client.region`; older Spark runtimes take it from
`s3.region` (or from `AWS_REGION`). Set the key the runtime you use reads.

### Trino

Trino's Iceberg connector speaks the same REST protocol. The snippets below are for Trino 484 and
later, where the REST catalog security is `OAUTH2`: Trino exchanges the key pair at the catalog's
OAuth2 token endpoint, and `vended-credentials-enabled=false` makes it use its own S3 credentials,
which is the supported mode unless the catalog vends. For Trino before 484, use
`iceberg.rest-catalog.security=BASIC` with `iceberg.rest-catalog.user` and
`iceberg.rest-catalog.password` instead of the three `oauth2.*` keys, and
`fs.native-s3.enabled=true` instead of `fs.s3.enabled=true`.

`/mds/iceberg/api`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakefs:8000/mds/iceberg/api
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=<access_key_id>:<secret_access_key>
iceberg.rest-catalog.oauth2.server-uri=http://lakefs:8000/mds/iceberg/api/v1/oauth/tokens
iceberg.rest-catalog.vended-credentials-enabled=false
iceberg.rest-catalog.nested-namespace-enabled=true
# Trino reads the files with its own S3 client when the catalog does not vend credentials:
fs.s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.region=us-east-1
s3.aws-access-key=<key>
s3.aws-secret-key=<secret>
s3.path-style-access=true
```

```sql
SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs."my-repo.main.system".object_metadata
WHERE path LIKE 'orders/%';
```

`/iceberg/relative_to`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakefs:8000/iceberg/relative_to/my-repo.main/api
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=<access_key_id>:<secret_access_key>
iceberg.rest-catalog.oauth2.server-uri=http://lakefs:8000/iceberg/api/v1/oauth/tokens
iceberg.rest-catalog.vended-credentials-enabled=false
fs.s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.region=us-east-1
s3.aws-access-key=<key>
s3.aws-secret-key=<secret>
s3.path-style-access=true
```

```sql
SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs.system.object_metadata
WHERE path LIKE 'orders/%';
```

### What was verified

The end-to-end suite [`../esti/mds_test.go`](../esti/mds_test.go) runs the PyIceberg and DuckDB
snippets above against a real `lakefs-sso` and a real indexer on SeaweedFS, on both endpoints: PyIceberg
through `/mds/iceberg/api` (identifier form) and through `/iceberg/relative_to`, DuckDB through
`/iceberg/relative_to` and through `/mds/iceberg/api` with the three-level identifier. Every read of
the suite takes all four paths and requires them to agree on path, `commit_id`, etag, `size_bytes`
and user metadata (commit `91b30f485`), including tags, short commit IDs, a hard reset and an indexer
restart. The filtered reads of the guide are covered too: a step runs PyIceberg's `row_filter`
(`GreaterThan(size_bytes, 0) & StartsWith(path, "data/")`) and DuckDB's
`WHERE path LIKE 'data/%' AND size_bytes > 0 ORDER BY size_bytes DESC LIMIT 20` through all four
engine/mount recipes and requires the same rows and the `DESC` ordering. The restart case is
deterministic: it holds the companion branch's lakeFS commit, so the kill lands between the staged
Iceberg commit and the lakeFS commit (§4.4) instead of hoping to hit a tick. §4.9 compaction and tag
retention, and the §4.8 branch-deletion mirror, run end to end as well, including
`lakefs_mds_compactions_total` and the `/_health` entry of a deleted branch (commit `866d4e85c`).

The Spark and Trino snippets are run separately from the esti suite, against the
same local recipe (a `lakefs-sso` with both switches on, an indexer with `period: 2s`, and the
SeaweedFS of `esti/ops/docker-compose-mds.yaml`) with the runnable scripts in
[`../esti/ops/mds-engines/`](../esti/ops/mds-engines/): Spark 3.5.5 with Iceberg 1.8.1
(`tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460`)
and Trino 484 (`trinodb/trino@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f`).
Both `/mds/iceberg/api` and `/iceberg/relative_to` returned the same three rows for the same
objects, with the same `commit_id`. The scripts capture the rows, require exactly those three and
diff the two endpoints against each other, so the observation is repeatable rather than read off a
log; the image defaults are the digests above. The scripts' README records the exact commands and
the environment they expect.

## Columns and semantics

The table is Iceberg format v2, unpartitioned, sort order `path ASC`, with `path` as the identifier
field. The names below are the contract; field IDs are what schema evolution preserves, so a writer
or reader takes them from the loaded table rather than hard-coding them.

| Field ID | Column | Iceberg type | Required | Meaning |
|---|---|---|---|---|
| 1 | `repository` | `string` | yes | The data repository name: `my-repo`. |
| 2 | `path` | `string` | yes | The object key (no `lakefs://` prefix, no leading slash). Identifier field. |
| 3 | `commit_id` | `string` | yes | Full ID of the commit of `R` that last added or changed the object on this branch. |
| 4 | `size_bytes` | `long` | yes | Object size in bytes. |
| 5 | `last_modified` | `timestamptz` (microseconds) | yes | Object modification time (`DBEntry.CreationDate`). |
| 6 | `etag` | `string` | yes | Object checksum, content only. |
| 7 | `user_metadata` | `map<string,string>` | yes | User metadata as lakeFS stores it; an empty map when the object has none, never null. The map value is optional on the wire. |
| 10 | `committer` | `string` | no | Committer of `commit_id`. |
| 11 | `content_type` | `string` | no | Object content type. |
| 12 | `creation_date` | `timestamptz` (microseconds) | no | Creation time of the **commit** `commit_id`, not of the object. |

The map's key and value field IDs are 8 and 9 in the table this version creates, but a reader must
take them from the loaded table rather than assume them. The map value is optional because
iceberg-go v0.7.0 reads parquet map columns with
nullable items; a required value would make the indexer's own copy-on-write rewrite and compaction
fail. The indexer never writes a null value.

### Row semantics

- **One row per `(branch, path)`.** Each indexed branch has its own table snapshot and its own row
  set. An added or changed object replaces its row; a removed object deletes it.
- **First-parent only.** Commits are walked along the first-parent chain of the branch. For a merge
  commit, the diff against its first parent is applied, so objects brought in by the merge get the
  **merge commit's** ID, committer and date. Commits reachable only through a second parent are not
  indexed on that branch.
- **`commit_id` is the last change the indexer saw.** For commits after the first indexed position
  that is the true commit that added or changed the object.
- **User metadata keys are literal.** Objects written through the S3 gateway have keys of the form
  `X-Amz-Meta-<lowercased-name>` (for example `X-Amz-Meta-Owner` becomes
  `X-Amz-Meta-owner`). Other writers store the keys they send; inspect the map for a few objects
  before matching on it, and expect the gateway form for gateway uploads.
- **`etag` is the object checksum**, the same value lakeFS reports for the object. It changes when
  the content changes, which makes it the cheap way to detect an overwrite.
- **`creation_date` is the commit time.** It is not the object's creation time; that is
  `last_modified`. The name comes from the source field, not from the row's meaning.
- **`content_type` and `committer` can be null** for objects or commits where lakeFS has no value;
  treat them as optional in queries.

### Deviations from "the commit that changed the object"

These are documented deviations, not bugs (design §3.2 and §4.3):

1. **Backfill rows.** Objects that were present at the commit where indexing of a branch first
   started get that starting commit as `commit_id`, not the commit that originally created them.
   Finding the original commit would cost a log walk per path. History before the first indexed
   position is therefore approximate in `commit_id`; history after it is exact.
2. **Rebuild rows.** When one commit's change set exceeds `rebuild_threshold`, the indexer rebuilds
   the branch's table from a full listing at that commit; every row gets the rebuild commit as
   `commit_id`. The same applies to a history rewrite that has no `mds-*` tag to reset to. The rows
   themselves are correct; only per-object `commit_id` detail is lost for the rebuilt set.
3. **Merges.** See above: the merge commit, not the original commit from a side branch, is recorded.
4. **Unindexed refs.** A tag or commit that is not on an indexed branch's first-parent chain has no
   `mds-*` tag and answers `404 NoSuchTableException`; the catalog never substitutes an ancestor's
   table, because that would return rows that do not describe the ref.
5. **No object history table.** The table holds the current state, one row per object. For change
   history, query two refs and compare, or use lakeFS `log`/`diff` directly.

## Freshness

The table is eventually consistent. Its position is a table property, so it moves atomically with
the rows: `lakefs.mds.source-commit` is the full ID of the last source commit the table reflects.
Every snapshot the indexer writes also carries `lakefs.mds.source-commit` and
`lakefs.mds.source-repository` in its summary.

Read the property from the engine that loaded the table. With PyIceberg:

```python
table = catalog.load_table(("my-repo", "main", "system", "object_metadata"))
indexed = table.properties["lakefs.mds.source-commit"]
print(indexed)
```

With Spark:

```sql
SHOW TBLPROPERTIES lakefs.`my-repo`.`main`.`system`.object_metadata;
```

Compare it with the head of the branch in lakeFS:

```bash
lakectl log lakefs://my-repo/main --amount 1
```

or `GET /api/v1/repositories/my-repo/branches/main` and its `commit_id` field. If they are equal,
the table is at the branch head; if not, the indexer has not caught up yet. One tick is bounded by
`period` plus the work of `max_commits` commits, so the usual lag is one period.

`/_health`'s `lag_commits` and the `lakefs_mds_lag_commits` metric are exact, not a lower bound. When a
branch is more than `max_commits` plus one log page behind, the indexer does not page through the
backlog commit by commit: it rebuilds the branch's table from a full listing at the head, and the lag
drops to `0` in that tick. Raise `max_commits` above the backlog if you want incremental catch-up
instead of a rebuild. A failed attempt that could not count the backlog reports the recorded position
and a lag of `-1` (unknown) instead of `0`; see [`Verify`](#5-verify). Every HTTP request is bounded by
`request_timeout`, so a connection that stops
answering fails the branch (or the repository) and `/_health` turns `503`; it does not freeze indexing
silently.

The upstream SQL recipe filters by the head commit:

```sql
SELECT * FROM lakefs.system.object_metadata WHERE commit_id = '<full head commit ID>';
```

It answers "which objects did the head commit change", which is usually what a data engineer wants,
but it is **not** a freshness check on its own: a head commit that changed no object (an empty
commit, or a commit that only changed another table) produces no rows even though the index is
fresh. Compare `lakefs.mds.source-commit` with the head for freshness; use the `commit_id` filter
for "what changed".

`lakefs_mds_lag_commits{repository,branch}` reports the same lag as a metric, and `/_health`
reports `lag_commits` per branch.

## Limits and known deviations

| Limit | Detail |
|---|---|
| Repository name length | A data repository name is at most **54** characters. `R-metadata` must fit lakeFS's 63-character repository ID limit, and configuration validation rejects a longer name at startup. |
| The `-metadata` suffix | A data repository whose name ends in `-metadata` cannot be indexed: `icebergcatalog.IsMetadataRepository` treats it as a companion repository. Validation rejects it. |
| Bucket-root namespace | If `R`'s storage namespace is a bucket root (`s3://bucket`), the companion namespace cannot be derived. Set `metadata_search.repositories.<R>.storage_namespace` explicitly, for example `s3://bucket/metadata/my-repo`. |
| Non-overlapping namespaces | `R-metadata`'s namespace must not be inside `R`'s namespace, nor `R`'s inside it. `R`'s uncommitted GC sweeps every unreferenced object under `R`'s prefix and would delete the metadata files. The indexer checks the pair on every tick, not only when it creates the companion repository, refuses such a configuration and marks the repository failed. |
| First-parent only | Only first-parent history is indexed and addressable. A commit that exists only on a merged side branch has no `mds-*` tag. |
| Indexed branches only | Branches that do not match `branches` have no rows and no virtual table. Adding a branch to the configuration later indexes it from its head (or history, per `backfill`/`since`). |
| One row per object | No change history is stored in the table; `commit_id` is the last change. |
| `commit_id` after backfill/rebuild | The starting (or rebuilding) commit, not the original change, as described in [`Deviations`](#deviations-from-the-commit-that-changed-the-object). |
| Rebuild memory | Above `rebuild_threshold` upserts plus deletes, the indexer stops reading the diff at the first entry over the threshold and rebuilds from a full listing rather than holding a huge delta in memory; a rebuilt commit makes no per-object stat call. A full listing still costs one page per `listObjects` page and one row per object. |
| Per-tick work | One branch applies at most `max_commits` commits per tick, and the log walk reads at most `max_commits` plus one page; a backlog deeper than that is rebuilt from a full listing at the head (the lag drops to `0`) instead of being paged through. `concurrency` bounds parallel branches, not commits. |
| Query scale | The table is unpartitioned and sorted by `path` within each file. There is no path-prefix partitioning and no server-side scan planning, so a filtered query is only as selective as the Iceberg expression the engine pushes down. Partitioning by prefix is a later version. |
| Structured metadata only | There is no full-text, vector or JSON-path search. `user_metadata` is a map of strings, queryable with the engine's map functions. |
| Companion table identity | The catalog serves `my-repo.<ref>.system.object_metadata` only when the companion table's `lakefs.mds.repository` property names `my-repo`. A repository named `my-repo-metadata` that the indexer did not create — the name is taken by an ordinary repository — answers `404 NoSuchTableException` on the virtual address instead of serving someone else's rows (design §5.1). |
| Companion repository must be writable | `my-repo-metadata` must not be created with `read_only: true`, and a branch-protection rule must not block `COMMIT` on its branches. The indexer checks this every tick: a read-only companion repository fails at the repository level, once per tick, with a message naming both causes, instead of failing every branch forever (design §3.3). |
| Azure and GCS | Out of scope for this version: no credential vending and no remote signing. Static `storage` properties may work; they are not tested. |
| One indexer replica | Run exactly one. A second replica does duplicate work and gets `409` responses; it does not corrupt the table. |
| No UI, no server-side SQL | OSS has no Metadata Search tab and no SQL endpoint. Queries run in the engine that connects to the catalog (PyIceberg, DuckDB, Spark, Trino). Option B is future work (see [`Option B`](#option-b-outlook)). |

The measured backfill numbers (commit `59828bcd1`) are two runs of
[`TestMetadataSearchBackfill`](../esti/mds_test.go), which times the first indexing separately from
building the source commit:

- **10,000 objects** (the CI setting): first indexed 2.7 seconds after the indexer started, one
tick, 0.46 seconds of indexer work, all four engine paths counting 10,000 rows.
- **100,000 objects**: the source commit was built in 5m46.159s (33ms for the one uploaded object,
the rest the import of 100,000 entries). The indexer first indexed it 3.212 seconds after it started
(31,135 objects/s at `period: 2s`), with `lakefs_mds_tick_duration_seconds_sum` 0.86s, one rebuild
and 100,000 rows written; PyIceberg and DuckDB both count 100,000 rows and see `source_commit` at
the head. The whole test took 447.26s, almost all of it setup.

So the 100,000-object case has been measured; what used to exceed the test timeout was building the
source commit, not indexing it. The CI job keeps `ESTI_MDS_BACKFILL_OBJECTS=10000` for run time.

## Permissions

The read path is designed so query users need nothing on the companion repository:

- A read of `my-repo.<ref>.system.object_metadata` is authorized with one `catalog:ReadTable` (and
  `catalog:ReadTableData` for vended credentials or remote signing) on `my-repo`, plus
  `fs:ReadRepository` on `my-repo` in the catalog's repository checks. The redirected read of
  `my-repo-metadata` runs with the server's identity, so the caller needs no permission there.
- **The caveat:** `catalog:` ARNs are branch-agnostic and address the table, so the grant covers the
  metadata of **all objects on all refs** of `my-repo`. A policy that narrows another table, a
  namespace, an object path or a branch does not narrow this virtual table. Grant
  `catalog:ReadTable` on `my-repo` deliberately, and treat it as "may see the object inventory of
  the whole repository".

Other permission facts:

- A user who may read `my-repo-metadata` directly but not `my-repo` gets `403` on the virtual table:
  authorization is checked on the data repository, not on the companion one.
- Reading `my-repo-metadata` directly as an ordinary repository needs read access to
  `my-repo-metadata`, like any repository. This is useful for inspecting the indexer's own table but
  is not the supported query path.
- Ordinary principals cannot write `my-repo-metadata` while `mds-metadata-repo-protect` is in force;
  the deny wins over an administrator's `fs:*` allow. It covers `fs:Write*`, `fs:Delete*`,
  `fs:Create*`, `fs:Revert*`, `fs:Update*` (including `fs:UpdateRepository`, which sets repository
  metadata) and the catalog write actions on the companion repositories. It also covers the import
  actions through `fs:Import*`: cancelling an import on a companion branch is denied outright, and an
  import whose **destination** is a companion repository is denied by the `fs:WriteObject` on that
  branch and object and the `fs:CreateCommit` on it, whatever the source. An import whose *source*
  is a companion repository's namespace is **not** denied: it writes another repository and copies
  files out, which is what reading the companion repository already allows; the deny protects writes
  inside a companion repository, not copies of its data. The indexer and the catalog are outside
  its groups and keep writing.
- On an auth backend without policies (`auth.basic`), the deny is not enforced and the bootstrap
  prints a warning naming that consequence. Do not rely on the deny in that mode.
- The `system` namespace of a data repository is read-only for clients while the feature is enabled:
  creates, commits, registers, drops and namespace changes under `system` answer `403
  ForbiddenException`, and `GET`/`HEAD` of the namespace is `404`. Listings still show `system` and
  `object_metadata`. In `my-repo-metadata` it is an ordinary namespace.
- An explicit `storage_namespace` that does not itself end in `-metadata` falls outside the
  namespace grant of the bootstrap's `mds-indexer` policy and needs an extra
  `fs:AttachStorageNamespace` statement (plus the matching `fs:CreateRepository`); the derived
  `<R namespace>-metadata` form is covered.

## Maintenance

The companion repository is an ordinary lakeFS repository with an Iceberg table in it. It needs the
same table maintenance any Iceberg table does, plus the tag lifecycle that makes history readable.

### Automatic compaction

Merge-on-read deletes accumulate position-delete files instead of rewriting data files on every
change. The indexer compacts a branch's table when either trigger in `metadata_search.compaction` is
crossed: more than `max_delete_files` delete files, or `every_commits` indexed commits since the
last compaction. A trigger set to `0` is off — it is not read as "use the default", which applies
only when the key is absent — and a negative value is refused at startup. A compaction scans the table
at the current snapshot, replaces all data and delete
files with rewritten data files in one transaction, keeps `lakefs.mds.source-commit`, and commits
with `mds: compact <R>@<C>`. The rewritten rows are sorted by `path` before they are written, so the
table's declared `path ASC` order keeps describing its files and their per-file path bounds stay
narrow, which is what a later filtered scan prunes with. The delete files are taken from the snapshot's manifest list, not from the
scan plan, so a delete file that no scan task carries any more — a rebuild replaces the data files it
applied to — is removed too; `max_delete_files` therefore counts a number that a compaction actually
drives to zero. It does not create a new tag; the existing `mds-*` tag still points at
the pre-compaction commit, which holds the same rows. Compactions are counted by
`lakefs_mds_compactions_total`.

### Snapshot expiry and file sweep

The indexer does not expire snapshots or delete files. Run `lakectl iceberg cleanup` on the
companion repository on a schedule; it is the open-source, Spark-free job documented in
[`iceberg-cleanup.md`](iceberg-cleanup.md). It honours lakeFS references (branch heads, tags and
GC-retained commits), so the `mds-*` tags keep their snapshots readable.

```bash
# Expire, mark and sweep in one run. Start with --dry-run in a new installation.
lakectl iceberg cleanup --repo my-repo-metadata \
  --max-snapshot-age-days 5 \
  --min-snapshots-to-keep 2 \
  --safety-age 72h
```

The run stages its changes on each branch; publish them with a lakeFS commit on the branches the
run reports, for example:

```bash
lakectl commit lakefs://my-repo-metadata/main -m "iceberg cleanup"
```

`--repo` is required. The other flags and the report layout are in
[`iceberg-cleanup.md`](iceberg-cleanup.md); the important ones here are the snapshot age, the
minimum number of snapshots kept and the sweep safety age. `lakectl iceberg cleanup` needs read and
write access to `my-repo-metadata` (an operator, not the indexer) and no blockstore credentials: the
physical bytes are reclaimed by lakeFS GC after the sweep removes the registrations that protected
them.

The uncommitted-GC grace of the catalog, `iceberg.uncommitted_gc_grace` (default `24h`, see
[`iceberg.md`](iceberg.md#garbage-collection)), covers the window between the indexer's Iceberg
commit (which stages files) and its lakeFS commit. Keep it comfortably longer than a tick plus the
slowest commit. The value `0` protects every `_iceberg/` object regardless of age: that is safe, but
it never reclaims the orphan files of an interrupted commit.

### lakeFS garbage collection

Configure GC rules for `my-repo-metadata` exactly as for a data repository, and schedule both the
committed GC and the uncommitted GC:

```bash
lakectl gc set-config lakefs://my-repo-metadata -f gc-rules.json
```

Without GC rules lakeFS retains every commit and every object reachable from it, which also means
`lakectl iceberg cleanup` treats every commit as retained and can sweep almost nothing. A retention
period shorter than the history you want to keep addressable through `mds-*` tags makes those tags
point at unreachable commits; see `tag_retention` below.

### Tag retention

Every indexed source commit gets a tag `mds-<full commit ID>` on the corresponding commit of
`my-repo-metadata`. Those tags are what make a tag, a full commit ID, a short ID or `main~1` of the
data repository readable through the virtual table. They also keep their snapshots alive: a tagged
commit is retained forever by lakeFS GC.

`tag_retention` (default `0`, keep forever) retires `mds-*` tags whose **source** commit is older
than the duration. A tag that records the current position of a live branch of `my-repo-metadata` is
never retired, however old its source commit is: tag repair would recreate it, so retiring it would
just delete and recreate it on every tick. After the tag is gone, the ref mapper answers
`404 NoSuchTableException` for that commit or tag, and lakeFS GC may reclaim the data. Set it only as
long as your users need commit-level history; branch reads do not depend on the tags of old commits.
A tag whose source commit no longer exists is retired too, because it can no longer resolve.

`delete_branches` (default `true`) deletes the mirrored branch when a matching branch of `R` is
deleted. Its tags stay, so history that was tagged remains readable; the branch itself does not. The
companion repository's own default branch is never deleted, even after `R` renames its default
branch: lakeFS refuses a default-branch delete, so the indexer keeps it and logs the skip once. With
`delete_branches: false` the mirror is kept and only its `/_health` entry and
`lakefs_mds_lag_commits` series are dropped, so a failed last attempt of the deleted branch cannot
keep `/_health` at `503`.

### Upgrades and restarts

- Restarting the indexer is safe and is part of normal operation; it resumes at the recorded
  position. A restart does not defer a scheduled compaction: the `every_commits` count is read
  back from the companion branch's commit log, not kept in process state. A rolling restart that
  overlaps two processes is safe even during the first initialisation of a companion repository:
  only one of them commits `mds: init`, and the other adopts it.
- If the recorded position no longer exists in `R` — committed GC after a force-move while the
  indexer was down, or `R` deleted and re-created under the same name — the indexer does not fail the
  branch forever. The next tick rebuilds the branch's table from a full listing at the head and logs
  `mds: rebuilding the metadata branch at the data branch head`.
- A lakeFS upgrade that changes the catalog or the table schema should be followed by a normal
  indexer tick. The indexer reloads the table before every commit and takes field IDs from the
  loaded table.
- If `R-metadata` is reset or restored from a backup, the next tick recovers as it would after a
  crash: it finds the recorded position in the head commit's `lakefs.mds.source-commit` and either
  continues, resets an unexpected staging area, or rebuilds from a full listing when the history
  was rewritten.

### Metrics

The indexer serves Prometheus metrics on `listen_address` at `/metrics`, all with the `lakefs_mds_`
prefix (design §9):

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `lakefs_mds_indexed_commits_total` | counter | `repository` | Source commits applied to the mirror. |
| `lakefs_mds_lag_commits` | gauge | `repository`, `branch` | Source commits of a branch not indexed yet; `-1` when the last attempt failed before it counted them. |
| `lakefs_mds_errors_total` | counter | `repository`, `stage` | Failures by stage: `tick`, `repository` or `branch`. |
| `lakefs_mds_tick_duration_seconds` | histogram | — | Duration of one tick. |
| `lakefs_mds_rows_written_total` | counter | — | Object metadata rows written. |
| `lakefs_mds_rebuilds_total` | counter | — | Rebuilds of a branch's table from a full listing. |
| `lakefs_mds_compactions_total` | counter | — | Compactions of a branch's table. |

Alert on `_health` returning `503`, on `lakefs_mds_errors_total` increasing, and on
`lakefs_mds_lag_commits` staying away from zero (`> 0`, or `-1` for "unknown") for longer than a few
periods.

## Option B outlook

Option A, documented here, deliberately stops at the index and the catalog: users query with their
own engine. Option B (design §11) adds a query service and a UI, and is **not built**. Its shape, for
planning:

- A stateless pod running embedded DuckDB. The lakeFS server authenticates and authorizes the UI
  request (`fs:ReadRepository` on every repository the SQL references, audit-logged), forwards the
  statement with short-lived credentials scoped to the table's data and metadata prefixes, and
  streams the first 1,000 rows plus the row count.
- A new `iceberg_query` configuration section: `iceberg_query.enabled`, `iceberg_query.endpoint`,
  `iceberg_query.token`, `iceberg_query.results_storage_id`, `iceberg_query.results_location`
  (default prefix `_lakefs/mds_query/results/`, outside every repository namespace, with a
  lifecycle rule) and `iceberg_query.request_timeout` (default `30s`).
- A statement allow-list: `SELECT`, `WITH`, `VALUES`, `SHOW TABLES`, `DESCRIBE`, `SUMMARIZE`, one
  statement, wrapped as `SELECT * FROM (…) AS q LIMIT n`. `EXPLAIN`, `PRAGMA`, `SET`, `CREATE`,
  `COPY`, `ATTACH`, file-reading functions and session functions are rejected. Row, byte and time
  caps; one query at a time per installation.
- A "Metadata Search" tab in the web UI: ref selector, SQL editor with the table name
  `"<repo>.<ref>.system".object_metadata`, and a results table.
- Requirements: S3 with credential vending (`role_arn`), a results lifecycle rule, and a single
  query pod.

None of those keys exist in this build. Until Option B lands, use the catalog from your engine of
choice and the snippets above.

## Clean-room note

Upstream Metadata Search is a lakeFS Team/Enterprise feature. This is an independent OSS
implementation written only from the public guide
(<https://docs.lakefs.io/guides/metadata-search/>), the Apache Iceberg specifications and this
repository's code. No upstream Enterprise code, configuration or binary is used.
