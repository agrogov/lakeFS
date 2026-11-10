# Metadata Search

Metadata Search makes the object metadata of a lakeFS repository queryable with any Iceberg
engine: path, size, last modified time, etag, content type, committer and user metadata, at any
indexed branch, tag or commit. There is no separate query service and no new query language — an
engine connects to the lakeFS Iceberg REST catalog and reads one table:

```
system.metadata.my-repo.main.objects
```

The table is **virtual**: it lives in the companion repository `my-repo-metadata` and is kept in
sync by a background indexer. The older identifier `<repo>.<ref>.system.object_metadata` is served
only by the guide's deprecated, separate catalog at `/mds/iceberg/api`; the lakeFS catalog itself
has no `system` namespace inside any repository. The catalog resolves the identifier above to the real table of the
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
  one Iceberg table: physically `system.object_metadata` of the companion repository, served by the
  catalog at the root of the namespace tree as `system.metadata.<R>.<ref>.objects`.
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

- the metadata table of every indexed ref is served at the root of the catalog as
  `system.metadata.<repo>.<ref>.objects`, resolved to the companion repository;
- the root `system` namespace is reserved and read-only for clients, while nothing below
  `<repo>.<ref>` is virtual or reserved: a data repository's addresses - `system` included - mean
  what they say;
- `relative_to` accepts the two root-tree warehouses, `system.metadata` (identifiers
  `<repo>.<ref>.objects`) and `system.metadata.<repo>` (identifiers `<ref>.objects`), next to the
  `<repo>[.<ref>]` data-repository warehouses;
- the guide's deprecated, separate Metadata Search catalog is mounted at `/mds/iceberg/api`, where
  the older identifier `<repo>.<ref>.system.object_metadata` still resolves (and nothing else).
  It serves read operations only and is not an alias of `/iceberg/api`.

While either switch is false the metadata table does not resolve, no namespace is reserved and
`/mds/iceberg/api` is not mounted: the request falls through to the core router's not-implemented
placeholder and answers **`501 Not Implemented`**, the same answer a plain `lakefs` binary gives at
`/iceberg/api` (design §5.5; `pkg/api/services.go`, mounted by `cmd/lakefs/cmd/run.go`). It is not a
`404`. Metadata Search is a mode of the catalog, so `iceberg.metadata_search.enabled: true` with
`iceberg.enabled: false` is a **startup error**, not a half-applied state: the server refuses it with
`metadata_search.enabled requires enabled: Metadata Search is served by the Iceberg catalog, which is
off` (design §5.4). Set both keys or neither. There is no per-repository switch on the server:
enabling the feature with a configuration that lists no repositories is harmless, because the indexer,
not the catalog, decides what is indexed. (The guide describes activation by a non-empty
`metadata_search.repositories` map; this build still keeps the server flag, design §5.4.)

### 2. The sync identity

`lakefs-sso` provisions the guide's sync identity on **every** start of the server, as the guide says lakeFS
does. The bootstrap is idempotent: it creates what is missing, repairs a policy whose statements drifted from the
built-in ones, and tolerates a rerun.

| Principal | Created by | Purpose |
|---|---|---|
| user `mds-service-user`, carrying policy `MDSCatalogAccess` | `lakefs-sso` bootstrap | the identity the indexer writes the metadata tables as |

`MDSCatalogAccess` grants `catalog:*` on `arn:lakefs:catalog:::*` - the catalog permissions the sync needs to
create the `system` namespace and the `object_metadata` table in a companion repository and to commit to it - and
nothing outside the Iceberg catalog. It is an allow on the catalog API only, so it grants no access to the objects
or branches of a data repository; the sync reads those in process (design §4.1).

**There is no credential to create.** The sync signs its own login token for `mds-service-user` with the server's
`auth.encrypt.secret_key`, exactly as the guide says, and sends it to the catalog as a bearer token. Both processes
read the same configuration file, so the key matches by construction. You configure no access key pair at all;
setting `metadata_search.access_key_id`/`secret_key` is the guide's "run the sync as another user instead" mode, and
that user needs `MDSCatalogAccess` attached to it.

The bootstrap also removes the principals and policies of an earlier Metadata Search build: the `mds-indexer` user
with its credentials, the `MetadataSearchIndexers` group, the `mds-indexer` allow policies with their numbered
chunks, every `mds-metadata-repo-protect[-<n>]` deny policy, and the read-only `MDSFSReadAccess` policy when it is
still attached to `mds-service-user`. Only names the bootstrap owns are removed.

**Auth backends without policies.** The default `auth.basic` service stores a single administrator and implements
no policy API. The bootstrap detects that, logs a warning naming what is not enforced, and continues; it does not
create `mds-service-user` or take over the administrator account. In that mode run the indexer with an
administrator's credentials (`metadata_search.access_key_id`/`secret_key`), or use a policy-capable auth backend
(for example `auth.type: postgres`), which is what a shared or production installation should do. The bootstrap
attaches `MDSCatalogAccess` to a user, so an auth service that cannot do that (the in-repo `contrib/auth/acl` HTTP
controller answers the attachment with `501`) fails startup with that answer instead of starting an unauthorized
sync.

### 3. Indexer configuration

`lakefs mds run` runs on the **server binary and the server configuration** (design §4.1): it opens the KV store
of `database.*`, builds the catalog over the `blockstore.*` adapter and reads the data repositories in process.
So the guide's `metadata_search` section lives in the same file as the server's configuration, and the
object-store credentials it writes with are the server's `blockstore.*` settings - there is no
`metadata_search.storage` or `metadata_search.azure` section.

```yaml
metadata_search:
  lakefs_endpoint: http://lakefs:8000       # required; lakeFS base URL, catalog at /iceberg/api
  access_key_id: AKIA...                    # optional, together with secret_key
  secret_key: ...                           # optional, together with access_key_id
  listen_address: 0.0.0.0:8080              # /_health, /metrics, /_pprof/
  period: 60s                               # > 0
  concurrency: 25                           # branches in parallel, >= 1
  max_commits: 1000                         # per branch per tick, >= 1
  since: 2026-01-01T00:00:00Z               # optional, RFC 3339
  repositories:                             # an empty map disables Metadata Search
    my-repo:
      branches: [main, "dev-*"]             # exact or trailing-* glob
      storage_namespace: s3://bucket/metadata/my-repo   # optional
```

Every key, its default and its meaning:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `lakefs_endpoint` | string | — (required) | Base URL of lakeFS. The indexer writes the metadata tables through the catalog at `<lakefs_endpoint>/iceberg/api`. |
| `access_key_id` | string | unset | Optional. Access key of a user that runs the sync instead of `mds-service-user`; that user needs `MDSCatalogAccess`. Set together with `secret_key`, or not at all. |
| `secret_key` | string | unset | Optional. Secret key of that user. |
| `listen_address` | string | `0.0.0.0:8080` | Address of the indexer's own HTTP server: `/_health`, `/metrics`, `/_pprof/`. |
| `period` | duration | `60s` | Interval between ticks. Every tick processes every configured repository. |
| `concurrency` | int | `25` | Number of branches one tick processes in parallel. Within a branch everything is sequential. |
| `max_commits` | int | `1000` | Upper bound on source commits one branch applies per tick. The next tick resumes at the recorded position. |
| `since` | RFC 3339 timestamp | unset | Oldest commit a first indexing starts at. Unset means "from the time the branch was created" (design §4.5). |
| `repositories` | map | empty | The data repositories to index, by name. An empty map disables Metadata Search; it is not a startup error. |
| `repositories.<R>.branches` | list | empty | Branch selectors: exact names, or a prefix with a single trailing `*` (`dev-*`). |
| `repositories.<R>.storage_namespace` | string | derived | Storage namespace of `R-metadata`. Unset means `-metadata` is appended to `R`'s namespace (see [`Limits`](#limits-and-known-deviations)). |

`metadata_search` is read with viper, so scalar keys can also be set through the lakeFS environment
conventions: prefix `LAKEFS_`, replace every `.` with `_`, upper-case. For example
`LAKEFS_METADATA_SEARCH_SECRET_KEY` and `LAKEFS_METADATA_SEARCH_LAKEFS_ENDPOINT`. The `repositories` map is read
from the file.

The command validates the section before the first tick and reports **one error per problem**, so a bad file is
fixed in one pass. Startup fails exactly when:

- `lakefs_endpoint` is empty;
- `access_key_id` and `secret_key` are not set both or neither;
- `period` is not positive, `concurrency` is below 1, or `max_commits` is below 1.

Nothing else is a startup error. An empty `repositories` map disables the feature; a repository name that ends in
`-metadata` or is longer than 54 characters is accepted (and reports at runtime why it cannot be indexed); a branch
selector that matches nothing selects nothing.

### The deprecated key

`lakefs_mds_endpoint` named the separate Metadata Search catalog before the guide moved the tables to the lakeFS
catalog. It is still accepted, but **only** when its value ends in `/mds/iceberg/api`: the suffix is stripped, the
service logs a warning, and the rest is used as `lakefs_endpoint` (for example
`http://lakefs:8000/mds/iceberg/api` becomes `http://lakefs:8000`). A value that is a plain base URL is refused
with a message that names the rename - that is what a configuration written for the earlier build looks like, so
rename it to `lakefs_endpoint`.

### Credentials for writing files

The indexer writes Parquet and Avro files for the table, so it needs write access to the companion repository's
object-store prefix. There are two modes (design §4.6):

- **Vended credentials.** If the catalog vends write credentials (`blockstore.s3.credentials_vending.role_arn`),
  the indexer requests them the way any Iceberg client does (`X-Iceberg-Access-Delegation: vended-credentials`) and
  needs no object-store keys of its own. Vending is S3-only; an Azure blockstore never vends.
- **The server's `blockstore.*`.** Otherwise the indexer writes with the block adapter's own settings from the
  same configuration file: `blockstore.s3.*` (region, endpoint, static credentials, path style), or
  `blockstore.azure.*` (storage account and key). This is also what the official lakeFS chart gives the MDS
  process, which shares the server's secret. This is the mode the esti suite runs in (SeaweedFS, static keys).

A blockstore with no static credentials (GS, or S3 on an instance role) leaves the client's own credential chain in
charge. GCS is out of scope for this version (`METADATA_SEARCH_AZURE.md` N2): the indexer registers no `gs://` IO,
so a GCS companion namespace cannot be written. Azure credential vending and remote signing do not exist either
(N1) - the indexer and the engines use their own Azure credentials - and the web UI object search stays S3-only
(N3).

The indexer never uses these credentials for catalog requests: those are authenticated as the sync identity
(§2).

#### Azure Blob Storage

On an Azure blockstore (`blockstore.type: azure`) the companion repository of `my-repo` lives in the same container
as `my-repo` - its derived namespace is a sibling prefix - and the catalog hands out its table's files under
`abfs[s]://<container>@<account>.<dfs domain>/<path>/_iceberg/<table-uuid>/…`
(`https://acct.blob.core.windows.net/c/p` becomes `abfss://c@acct.dfs.core.windows.net/p`, and an `http`
namespace becomes `abfs://`). No Iceberg FileIO opens the blockstore's own `https://` spelling, so this
translation is what lets the indexer and an engine read the table. It is a pure function of the repository's
namespace; an `https://` host this installation does not serve is left in the blockstore's own spelling, exactly as
before, rather than mapped to a location no client could open.

The catalog spells the location on the Data Lake Storage Gen2 (`dfs`) endpoint. If an engine reads through
Hadoop's ABFS driver (Hadoop ≥ 3.4) and the storage account is not HNS-enabled, that driver refuses the `dfs`
host: set `iceberg.azure.location_host: blob` on the **server** and the locations become
`abfss://c@acct.blob.core.windows.net/…` instead. It is one setting for the whole installation, not a per-engine
one, and the scheme stays `abfs[s]`.

The indexer's Azure credentials are `blockstore.azure.storage_account` and
`blockstore.azure.storage_access_key` from the same file - one secret, one place, exactly what the server pod
reads. Without a shared key, iceberg-go's default credential chain applies (workload identity: `AZURE_CLIENT_ID`,
`AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`; then a managed identity; then the Azure CLI), so a pod with a
federated identity needs no secret at all.

```yaml
# One secret, one place: the server and the indexer both read it as blockstore.azure.*.
apiVersion: v1
kind: Secret
metadata:
  name: azure-storage
type: Opaque
stringData:
  storage_account: myaccount
  storage_access_key: "<storage account key>"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lakefs
spec:
  replicas: 1
  selector: { matchLabels: { app: lakefs } }
  template:
    metadata: { labels: { app: lakefs } }
    spec:
      containers:
        - name: lakefs
          image: <the lakefs-sso image you deploy>
          args: ["run", "--config", "/etc/lakefs/config.yaml"]
          env:
            - name: LAKEFS_BLOCKSTORE_TYPE
              value: azure
            - name: LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT
              valueFrom: { secretKeyRef: { name: azure-storage, key: storage_account } }
            - name: LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY
              valueFrom: { secretKeyRef: { name: azure-storage, key: storage_access_key } }
        - name: lakefs-mds     # or its own Deployment; one replica, see "Run the indexer"
          image: <the lakefs-sso image you deploy>
          args: ["mds", "run", "--config", "/etc/lakefs/config.yaml"]
          env:
            - name: LAKEFS_BLOCKSTORE_TYPE
              value: azure
            - name: LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT
              valueFrom: { secretKeyRef: { name: azure-storage, key: storage_account } }
            - name: LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY
              valueFrom: { secretKeyRef: { name: azure-storage, key: storage_access_key } }
```

A container-root namespace (`https://myaccount.blob.core.windows.net/my-container`) derives
`…/my-container-metadata`, a container the operator then has to create; if that is not what you want, set
`metadata_search.repositories.my-repo.storage_namespace` explicitly to a prefix in the same container.

### 4. Run the indexer

```bash
lakefs mds run --config /etc/lakefs/config.yaml
```

`mds run` is a subcommand of the server binary itself, so the image that runs the server runs the
indexer too: the official `lakefs` image and the chart's
`args: [mds, run, --config, /app/config.yaml]` need no second image. (This repository's own
test binary, which also carries the Iceberg catalog and the SSO features, exposes the same command
as `lakefs-sso mds run`; it is the same code on the same configuration.)

`--config` is required. The process loads the whole lakeFS configuration - the file plus `LAKEFS_*` -
opens the KV store of `database.*` and the blockstore of `blockstore.*`, reads the `metadata_search`
section, binds `listen_address`, and ticks until `SIGTERM` or `SIGINT`; it shuts down gracefully and logs
`mds: indexer stopped`. A database it cannot open fails startup, exactly as it does for the server.

Run **one** replica. Correctness under accidental concurrency comes from the
Iceberg compare-and-swap and the crash recovery, not from a lease, so a second replica mostly wastes
work and produces `409` responses. A second process that meets the first on a brand-new companion
repository is harmless: the `mds: init` commit carries the table creation, so only one of them commits
it and the other adopts it instead of failing the repository as foreign.

What a first run does, in order (design §4.2, §4.5, §4.8):

1. For every configured repository, ensure `R-metadata` exists, initialise it with the
   `system` namespace, the table and an `mds: init` commit, and record its default branch. The
   initialisation refuses to commit a table it did not create: a staged `system.object_metadata` whose
   `lakefs.mds.repository` property is not `R` fails that repository for the tick instead of sealing the
   marker on a foreign table. A `400` from the initialisation commit is treated as "another replica won"
   only when that replica's `mds: init` commit is there to adopt; any other `400` is reported once, as that
   repository's error, instead of being retried in a loop.
2. For every branch of `R` that matches `branches`, create the mirrored branch in `R-metadata`.
   A new branch is seeded from the `mds-*` tag of the nearest already-indexed first-parent ancestor
   (within 1,000 commits); otherwise it is built from a full listing back to the branch's creation, or to
   `since` when that is set.
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
failure is in that branch's object and clears on the next successful attempt. A write lakeFS refuses
on a companion branch — a protection rule, or a repository that is read-only at the write — is reported
per refused branch, not on the repository object (see
[`Limits`](#limits-and-known-deviations)). A branch that is no
longer mirrored — deleted in `my-repo`, or no longer matching `branches` — is removed from the
report together with its `lakefs_mds_lag_commits` series, so its
last error does not keep `/_health` at `503` until the indexer restarts (design §4.8).

`lag_commits` is normally the exact number of source commits not indexed yet. It is **`-1`** when the
last attempt failed before it could count them: the branch then also reports the position it recorded
(`last_indexed`), so a stuck branch shows *where* it stopped and an unknown lag rather than a
misleading `0` (design §9). Alert on `lag_commits != 0` rather than `> 0` to catch that state; a
healthy branch never reports `-1`.

## Querying

The table is addressed at the root of the lakeFS catalog, in the reserved `system` namespace:

| Form | URL | Identifier |
|---|---|---|
| `lakeFS catalog` | `http://lakefs:8000/iceberg/api` | `system.metadata.my-repo.main.objects` |
| `relative_to`, root tree | `http://lakefs:8000/iceberg/relative_to/system.metadata/api` | `my-repo.main.objects` |
| `relative_to`, one repository | `http://lakefs:8000/iceberg/relative_to/system.metadata.my-repo/api` | `main.objects` |
| deprecated catalog | `http://lakefs:8000/mds/iceberg/api` | `my-repo.main.system.object_metadata` |

`/iceberg/api` is the lakeFS catalog and the supported address: `system` is a repository level
there, `metadata` its ref, and the next two levels are the data repository and its ref.

The `relative_to` form carries a warehouse in the URL and prefixes it to every identifier. A
data-repository warehouse - `<repo>[.<ref>]` - addresses ordinary tables of that repository; the two
root-tree warehouses fix `system.metadata`, or `system.metadata.<repo>`, and let the client name the
rest: `<repo>.<ref>.objects` and `<ref>.objects`, which is the addressing a DuckDB `ATTACH` on the
warehouse uses.

`/mds/iceberg/api` is the guide's deprecated, separate Metadata Search catalog: it serves load, head
and list of `<repo>.<ref>.system.object_metadata` and nothing else, resolved to the same companion
table as the root address. It is not an alias of `/iceberg/api` and has no `relative_to` form. The
recipes below are all on the current addresses; the deprecated catalog's recipes are collected in
[`Deprecated catalog`](#deprecated-catalog).

Every snippet below assumes:

- the lakeFS server is at `http://lakefs:8000`;
- the data repository is `my-repo` and the ref is `main`;
- the examples pass S3 credentials explicitly for reading data files. Omit those properties when the
  catalog vends read credentials. On an Azure blockstore the catalog never vends (its credentials
  endpoint is not mounted), the table's location is `abfs[s]://<container>@<account>.<dfs domain>/…`,
  and the snippet's FileIO is the engine's Azure one - each subsection below shows it.

A query principal needs read access to `my-repo` (through the catalog) and nothing on
`my-repo-metadata`; see [`Permissions`](#permissions).

### PyIceberg

`/iceberg/api`, the lakeFS catalog:

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "lakefs",
    uri="http://lakefs:8000/iceberg/api",
    credential="<access_key_id>:<secret_access_key>",
    warehouse="s3://my-bucket",          # the data repository's storage namespace
    **{
        "s3.access-key-id": "<key>",
        "s3.secret-access-key": "<secret>",
        "s3.region": "us-east-1",
    },
)
table = catalog.load_table(("system", "metadata", "my-repo", "main", "objects"))
table.scan().to_pandas()
```

The guide's note on a tag whose name contains dots applies to the identifier: pass the identifier
as separate parts, `("system", "metadata", "my-repo", "v0.2.1", "objects")`, rather than as one
dotted string, which would be split into four extra namespace levels.

For a tag or a commit, put it in the place of `main`: `system.metadata.my-repo.v1.0.objects` through
`/iceberg/api`, or `my-repo.v1.0.objects` through the root-tree `relative_to` warehouse. A filtered
scan is pushed down as Iceberg expressions:

```python
from pyiceberg.expressions import GreaterThan, StartsWith

table.scan(
    row_filter=GreaterThan("size_bytes", 1_000_000) & StartsWith("path", "orders/"),
).to_pandas()
```

On an Azure blockstore, PyIceberg's FileIO is `adlfs` and its properties are the `adls.*` ones; the
`warehouse` is the data repository's storage namespace, in the form lakeFS stores it:

```python
catalog = RestCatalog(
    "lakefs",
    uri="http://lakefs:8000/iceberg/api",
    credential="<access_key_id>:<secret_access_key>",
    warehouse="https://myaccount.blob.core.windows.net/my-container/my-repo",
    **{
        "adls.account-name": "myaccount",
        "adls.account-key": "<storage account key>",
        # Or instead of the pair: "adls.connection-string": "...",
        # or "adls.sas-token.<account>.dfs.core.windows.net": "...".
    },
)
table = catalog.load_table(("system", "metadata", "my-repo", "main", "objects"))
table.scan().to_pandas()
```

The deprecated catalog takes the same `adls.*` properties with the older identifier.

### DuckDB

DuckDB attaches a catalog to a base URL, so its `ENDPOINT` is `/iceberg/api` and the identifier
names the whole address. The catalog secret authenticates the client to lakeFS; the S3 secret lets
DuckDB read the data files when the catalog does not vend credentials.

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
  ENDPOINT 'http://lakefs:8000/iceberg/api'
);

SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs."system.metadata.my-repo.main"."objects"
WHERE path LIKE 'orders/%'
ORDER BY size_bytes DESC
LIMIT 20;
```

The deprecated `/mds/iceberg/api` catalog keeps the older identifier `<repo>.<ref>.system.object_metadata`;
its recipe is under [`Deprecated catalog`](#deprecated-catalog). To use a lakeFS JWT instead of a key
pair on `/iceberg/api`, `ATTACH ... (TYPE iceberg, ENDPOINT '…', TOKEN '<lakefs-jwt>')` works for a
short-lived read.

On an Azure blockstore, load the `azure` extension and create a `TYPE azure` secret for the data
files; the catalog secret is unchanged and the `ATTACH` takes the same form, with the repository's
`https://` namespace as its warehouse:

```sql
INSTALL azure; LOAD azure;
INSTALL iceberg; LOAD iceberg;

-- Or AccountName/AccountKey, a SAS token, or a service principal instead of the connection string.
CREATE OR REPLACE SECRET lakefs_data (
  TYPE azure,
  CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=<key>;EndpointSuffix=core.windows.net'
);

CREATE OR REPLACE SECRET lakefs_catalog (
  TYPE iceberg,
  CLIENT_ID '<access_key_id>',
  CLIENT_SECRET '<secret_access_key>',
  OAUTH2_SERVER_URI 'http://lakefs:8000/iceberg/api/v1/oauth/tokens'
);

ATTACH 'https://myaccount.blob.core.windows.net/my-container/my-repo' AS lakefs (
  TYPE iceberg,
  ENDPOINT 'http://lakefs:8000/iceberg/api'
);

SELECT path, size_bytes, commit_id
FROM lakefs."system.metadata.my-repo.main"."objects"
WHERE path LIKE 'orders/%';
```

### Spark

PySpark with the Iceberg Spark runtime. The catalog name (`lakefs`) is arbitrary; the URI must not
include `/v1`. The S3 FileIO properties are only needed when the catalog does not vend
credentials; omit them when it does.

`/iceberg/api`, the lakeFS catalog:

```properties
spark.sql.defaultCatalog=lakefs
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakefs=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakefs.type=rest
spark.sql.catalog.lakefs.uri=http://lakefs:8000/iceberg/api
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
FROM lakefs.system.metadata.`my-repo`.main.objects
WHERE path LIKE 'orders/%';
```

The root address has four namespace levels (`system`, `metadata`, `my-repo`, `main`) and the table
is `objects`; quote a level that is not a bare identifier, as above for the repository name.
Writing the whole dotted namespace as one backquoted token makes it one namespace level, which the
catalog refuses.

Iceberg 1.8 takes the AWS region from `client.region`; older Spark runtimes take it from
`s3.region` (or from `AWS_REGION`). Set the key the runtime you use reads.

On an Azure blockstore, the FileIO is Iceberg's `ADLSFileIO`, which comes in the
`iceberg-azure-bundle` jar (`--packages org.apache.iceberg:iceberg-azure-bundle:1.8.1`, or the jar
that matches your Iceberg runtime). Everything the catalog side needs is unchanged:

```properties
spark.sql.defaultCatalog=lakefs
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakefs=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakefs.type=rest
spark.sql.catalog.lakefs.uri=http://lakefs:8000/iceberg/api
spark.sql.catalog.lakefs.rest.auth.type=basic
spark.sql.catalog.lakefs.credential=<access_key_id>:<secret_access_key>
spark.sql.catalog.lakefs.warehouse=https://myaccount.blob.core.windows.net/my-container/my-repo
# ADLSFileIO reads the data files; the class is in the iceberg-azure-bundle jar:
spark.sql.catalog.lakefs.io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO
spark.sql.catalog.lakefs.adls.auth.shared-key.account.name=myaccount
spark.sql.catalog.lakefs.adls.auth.shared-key.account.key=<storage account key>
```

Omit the two `adls.auth.shared-key.*` keys to use ADLSFileIO's default credential chain (the
`AZURE_*` environment, a managed identity, or the Azure CLI). The table's files are read from the
location the catalog returns for the table (`abfs[s]://…`), not from `warehouse`, which is set to the
repository's namespace only as in the S3 snippets above.

### Trino

Trino's Iceberg connector speaks the same REST protocol. The snippets below are for Trino 484 and
later, where the REST catalog security is `OAUTH2`: Trino exchanges the key pair at the catalog's
OAuth2 token endpoint, and `vended-credentials-enabled=false` makes it use its own S3 credentials,
which is the supported mode unless the catalog vends. For Trino before 484, use
`iceberg.rest-catalog.security=BASIC` with `iceberg.rest-catalog.user` and
`iceberg.rest-catalog.password` instead of the three `oauth2.*` keys, and
`fs.native-s3.enabled=true` instead of `fs.s3.enabled=true`.

The deprecated `/mds/iceberg/api` catalog keeps the older identifier
`<repo>.<ref>.system.object_metadata`; its recipe is under [`Deprecated catalog`](#deprecated-catalog).

`/iceberg/api`, the lakeFS catalog:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakefs:8000/iceberg/api
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=<access_key_id>:<secret_access_key>
iceberg.rest-catalog.oauth2.server-uri=http://lakefs:8000/iceberg/api/v1/oauth/tokens
iceberg.rest-catalog.vended-credentials-enabled=false
iceberg.rest-catalog.nested-namespace-enabled=true
fs.s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.region=us-east-1
s3.aws-access-key=<key>
s3.aws-secret-key=<secret>
s3.path-style-access=true
```

```sql
SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs."system.metadata.my-repo.main".objects
WHERE path LIKE 'orders/%';
```

On an Azure blockstore, Trino reads the data files with its native Azure file system instead of the
S3 one; the catalog properties are the same as above.

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakefs:8000/iceberg/api
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=<access_key_id>:<secret_access_key>
iceberg.rest-catalog.oauth2.server-uri=http://lakefs:8000/iceberg/api/v1/oauth/tokens
iceberg.rest-catalog.vended-credentials-enabled=false
iceberg.rest-catalog.nested-namespace-enabled=true
# Trino's native Azure file system reads the files when the catalog does not vend credentials:
fs.azure.enabled=true          # the newer name of the same switch is fs.native-azure.enabled
azure.auth-type=ACCESS_KEY
azure.access-key=<storage account key>
# azure.endpoint=https://myaccount.blob.core.windows.net   # only for a non-public cloud
```

`azure.auth-type=ACCESS_KEY` takes the account from the table location
(`abfss://<container>@<account>.dfs.core.windows.net/…`), so it needs no account property.
`MANAGED_IDENTITY` (with `azure.managed-identity.client-id` for a user-assigned one) and `OAUTH`
(with `azure.oauth2.*`) are the other auth types; `azure.endpoint` is required for a non-public
cloud.

### Deprecated catalog

The guide's deprecation section, mirrored: "The separate Metadata Search catalog at
`<lakefs_endpoint>/mds/iceberg/api` is deprecated and will be removed in a future release. Until
then it keeps serving the `<repo>.<ref>.system.object_metadata` identifier." It is a catalog of its
own, not an alias of `/iceberg/api`: it answers load, head and list of that identifier, resolved to
the same companion table, and nothing else. Do not build new integrations on it - the same table is
at `system.metadata.<repo>.<ref>.objects` on `/iceberg/api`, and at `<repo>.<ref>.objects` or
`<ref>.objects` through the root-tree `relative_to` warehouses.

PyIceberg:

```python
catalog = RestCatalog(
    "lakefs",
    uri="http://lakefs:8000/mds/iceberg/api",
    credential="<access_key_id>:<secret_access_key>",
    warehouse="s3://my-bucket",
)
# The identifier has four parts here: repository, ref, namespace, table.
table = catalog.load_table(("my-repo", "main", "system", "object_metadata"))
```

DuckDB, whose OAuth token endpoint on this mount is
`http://lakefs:8000/mds/iceberg/api/v1/oauth/tokens`:

```sql
ATTACH 's3://my-bucket' AS lakefs (
  TYPE iceberg,
  ENDPOINT 'http://lakefs:8000/mds/iceberg/api'
);

SELECT path, size_bytes, commit_id
FROM lakefs."my-repo.main.system"."object_metadata"
WHERE commit_id = '<full source commit ID>';
```

Spark, where each identifier level is quoted separately (writing the whole dotted namespace as one
backquoted token makes it one namespace level and the catalog refuses it):

```properties
spark.sql.catalog.lakefs.uri=http://lakefs:8000/mds/iceberg/api
spark.sql.catalog.lakefs.rest.auth.type=basic
spark.sql.catalog.lakefs.credential=<access_key_id>:<secret_access_key>
```

```sql
SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs.`my-repo`.`main`.`system`.object_metadata
WHERE path LIKE 'orders/%';
```

Trino 484 and later:

```properties
iceberg.rest-catalog.uri=http://lakefs:8000/mds/iceberg/api
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=<access_key_id>:<secret_access_key>
iceberg.rest-catalog.oauth2.server-uri=http://lakefs:8000/mds/iceberg/api/v1/oauth/tokens
iceberg.rest-catalog.nested-namespace-enabled=true
```

```sql
SELECT path, size_bytes, commit_id, user_metadata
FROM lakefs."my-repo.main.system".object_metadata
WHERE path LIKE 'orders/%';
```

The remaining catalog properties of each engine are the ones in its section above; only `uri`, the
token endpoint and the identifier change. The suite [`../esti/mds_test.go`](../esti/mds_test.go)
keeps exactly one case on this mount: it reads the table through this catalog with PyIceberg and
requires the same rows as `system.metadata.<repo>.<ref>.objects`.

### What was verified

The suite [`../esti/mds_test.go`](../esti/mds_test.go) runs the PyIceberg and DuckDB recipes above
against a real `lakefs-sso` and a real `lakefs-sso mds run` (this repository's own binary, not the
server image's `lakefs mds run`) on SeaweedFS. Every read takes four paths -
PyIceberg and DuckDB, each through `/iceberg/api` with `("system", "metadata", "<repo>", "<ref>",
"objects")` and through `/iceberg/relative_to/system.metadata/api` with `("<repo>", "<ref>",
"objects")` - and requires them to agree on path, `commit_id`, etag, `size_bytes` and user metadata.
The cases cover the object lifecycle (upload, overwrite, delete, S3-gateway user metadata), a
filtered scan (PyIceberg's `row_filter` and DuckDB's `WHERE … ORDER BY … LIMIT`), a new branch, a
merge, a tag, a short commit ID, a hard reset and an indexer restart in the crash window of design
§4.4. The suite also has exactly one case on the deprecated catalog: it reads
`<repo>.<ref>.system.object_metadata` through `/mds/iceberg/api` with PyIceberg, requires the same
rows as the root-tree address, and requires the lakeFS catalog to answer the old identifier with
`404 NoSuchTableException`.

The `lakefs-sso mds run` process of this repository's test binary runs on the server
configuration, so the suite starts a PostgreSQL container for the two processes to share (a local
Badger database cannot be opened twice) and configures no credential for the sync at all: it writes
as `mds-service-user`.

The recorded run (2026-11-10, RA8): SeaweedFS 4.50 on `:8333`, a locally built `esti-iceberg-client`
image (`ESTI_ICEBERG_CLIENT_IMAGE`, so no step installs its clients), and the throw-away PostgreSQL
the suite starts.

- `TestMetadataSearchEndToEnd` **PASS 766.49s**, all 15 cases: `upload and commit` 46.83s,
  `overwrite` 87.76s, `delete` 44.85s, `s3 gateway user metadata` 48.24s, `filtered scan` 49.79s,
  `new branch is seeded` 90.61s, `merge` 45.92s, `tag and short commit id` 87.97s, `hard reset`
  45.26s, `indexer restart` 49.98s, `user without access is refused` 0.16s, `R-metadata access does
  not open the metadata tree` 0.21s, `deprecated catalog serves the old identifier` 88.26s,
  `sync identity` 0.06s, `indexer keeps indexing` 46.64s. No case was skipped.
- `TestMetadataSearchBackfill` **PASS 99.00s** at `ESTI_MDS_BACKFILL_OBJECTS=10000`: the source
  commit took 18.396s, the first indexing 6.732s after the indexer started (1,485 objects/s at
  `period: 2s`), `rows_written_total` 10,000, `rebuilds_total` 1,
  `tick_duration_seconds_sum` 3.41s.
- `TestMetadataSearchBranchDeletion` **PASS 87.39s**.

`TestMetadataSearchAzureEndToEnd` was not part of these runs (it needs the Azurite emulator and its
account in the environment), and it is stale for the reason below.

**Spark and Trino are not re-run by RA8.** Their recipes in
[`../esti/ops/mds-engines/`](../esti/ops/mds-engines/) were moved to the current addresses, but the
last recorded Spark 3.5.5 + Iceberg 1.8.1 and Trino 484 runs (digests
`tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460`,
`trinodb/trino@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f`) were
against the earlier identifiers and the earlier, REST-client indexer; re-running them needs a
deployment and is a separate card. The scripts keep their row gate (they require exactly the three
`orders/` rows and diff the two addresses) and the Spark script's `--self-test`.

**Azure is not re-verified by RA8 either.** The last recorded Azurite acceptance run
([`../esti/mds_azure_test.go`](../esti/mds_azure_test.go), the same shape via PyIceberg with
`adls.connection-string` and DuckDB with a `TYPE azure` secret) configured the indexer through the
`LAKEFS_METADATA_SEARCH_AZURE_*` environment variables, and that section is gone (X6): the indexer now
takes its Azure credentials from the server's `blockstore.azure.*`, and its iceberg-go client builds
its endpoint from the table location rather than from the removed `metadata_search.azure.endpoint`. The
acceptance test has to move to the `blockstore.*` path before that run is repeated; that file is not
part of this card.

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
2. **Rebuild rows.** When one commit's change set exceeds 100,000 entries, the indexer rebuilds
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
table = catalog.load_table(("system", "metadata", "my-repo", "main", "objects"))
indexed = table.properties["lakefs.mds.source-commit"]
print(indexed)
```

With Spark:

```sql
SHOW TBLPROPERTIES lakefs.system.metadata.`my-repo`.main.objects;
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
and a lag of `-1` (unknown) instead of `0`; see [`Verify`](#5-verify). Every catalog request is bounded
by a fixed 60-second timeout, so a connection that stops answering fails the branch (or the repository)
and `/_health` turns `503`; it does not freeze indexing silently.

The upstream SQL recipe filters by the head commit:

```sql
SELECT * FROM lakefs."system.metadata.my-repo.main".objects WHERE commit_id = '<full head commit ID>';
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
| Repository name length | A data repository name is at most **54** characters: `R-metadata` must fit lakeFS's 63-character repository ID limit. The configuration does not reject a longer name (the guide has no such rule); the indexer reports that repository as failed. |
| The `-metadata` suffix | A data repository whose name ends in `-metadata` cannot be indexed: `icebergcatalog.IsMetadataRepository` treats it as a companion repository. The configuration accepts it and the indexer reports that repository as failed. |
| Bucket-root namespace | If `R`'s storage namespace is a bucket root (`s3://bucket`), the guide's rule applies: `-metadata` is appended to the namespace, so the companion is `s3://bucket-metadata` - a different bucket, which the operator must create or name explicitly with `metadata_search.repositories.<R>.storage_namespace` (for example `s3://bucket/metadata/my-repo`). On Azure the first path segment is the **container**, so a container root (`https://myaccount.blob.core.windows.net/data`) derives `…/data-metadata`, a container the operator must create, and a prefix inside a container derives a companion prefix inside that container (`…/data/esti-metadata`). |
| Non-overlapping namespaces | `R-metadata`'s namespace must not be inside `R`'s namespace, nor `R`'s inside it. `R`'s uncommitted GC sweeps every unreferenced object under `R`'s prefix and would delete the metadata files. The indexer checks the pair on every tick, not only when it creates the companion repository, refuses such a configuration and marks the repository failed. On Azure the `http://` and `https://` spellings of one container and prefix are the same prefix, and the check normalises the scheme, so a companion cannot hide behind the other spelling. |
| First-parent only | Only first-parent history is indexed and addressable. A commit that exists only on a merged side branch has no `mds-*` tag. |
| Indexed branches only | Branches that do not match `branches` have no rows and no metadata table. Adding a branch to the configuration later indexes it from its creation, or from `since` when that is set. |
| One row per object | No change history is stored in the table; `commit_id` is the last change. |
| `commit_id` after backfill/rebuild | The starting (or rebuilding) commit, not the original change, as described in [`Deviations`](#deviations-from-the-commit-that-changed-the-object). |
| Table with a column a newer version added | The indexer builds rows from the ten columns above. A table that already carries an extra top-level column — one a newer version appended, which the schema contract allows — is refused with `unknown table schema` by a delta that has upserts and by a rebuild, instead of being written with that column silently emptied; a delete-only delta still applies and a compaction keeps the column's values. Run an indexer at least as new as the table it writes. |
| Rebuild memory | Above 100,000 upserts plus deletes, the indexer stops reading the diff at the first entry over the threshold and rebuilds from a full listing rather than holding a huge delta in memory; a rebuilt commit makes no per-object stat call. A full listing still costs one page per `listObjects` page and one row per object. |
| Per-tick work | One branch applies at most `max_commits` commits per tick, and the log walk reads at most `max_commits` plus one page; a backlog deeper than that is rebuilt from a full listing at the head (the lag drops to `0`) instead of being paged through. `concurrency` bounds parallel branches, not commits. |
| Query scale | The table is unpartitioned and sorted by `path` within each file. There is no path-prefix partitioning and no server-side scan planning, so a filtered query is only as selective as the Iceberg expression the engine pushes down. Partitioning by prefix is a later version. |
| Structured metadata only | There is no full-text, vector or JSON-path search. `user_metadata` is a map of strings, queryable with the engine's map functions. |
| Companion table identity | The catalog serves `system.metadata.my-repo.<ref>.objects` (and the deprecated catalog's `my-repo.<ref>.system.object_metadata`, which names the same table) only when **both** proofs hold: the companion table's `lakefs.mds.repository` property names `my-repo`, and the served ref's first-parent history carries the indexer's `mds: init` commit (`lakefs.mds.init=true`) positioned as the first commit directly above the repository's root commit — or, for a companion repository created **bare**, as the root commit itself. The property alone is a document anyone who can create `my-repo-metadata` can write; the initialisation commit is written by the indexer after it creates the table, so it also commits the table and is the state a forger cannot reproduce by editing a document. The **committer is not part of the proof** (the guide lets the sync run as another user with `MDSCatalogAccess`), so the committer name of that commit is not checked. A repository named `my-repo-metadata` that the indexer did not create — the name is taken by an ordinary repository — answers `404 NoSuchTableException` on the metadata tree instead of serving someone else's rows (design §5.1). A bare companion is served like a non-bare one: its `mds: init` commit is the branch's first commit and has no parent (design §4.8). An init commit with another commit between it and the root is still refused. The history check walks the first-parent log newest to root, one 256-commit page at a time (the initialisation commit is the oldest non-root commit, or the root of a bare companion); the commit it finds is cached per companion repository, and a later head is checked only back to that commit or to a head already verified, so an unchanged branch pays nothing and a branch that moved pays for its new commits. The cache holds at most 1024 repositories. |
| Companion repository must be writable | `my-repo-metadata` must not be created with `read_only: true`, and a branch-protection rule must not block `COMMIT` or `STAGING_WRITE` on its branches. A read-only companion repository is detected from the repository record and fails at the repository level, once per tick (design §3.3). A protection rule is not reported on the repository record, so it is diagnosed at the first write the tick attempts — the catalog's `CreateTable` during the initialisation included — and is reported **per branch**: `/my-repo/_health` stays `503`, each refused branch carries a `last_error` naming the family and increments `errors_total{stage="branch"}`, and the repository's own `last_error` stays empty. The message names branch protection and the read-only flag only when lakeFS's own wording is in the cause; an authorization refusal (for example a sync identity that is missing `MDSCatalogAccess`) keeps the same family with neutral wording that names branch protection, `read_only` and the indexer's permissions as the candidates, instead of the bare `403` this used to be. |
| Azure Blob Storage | Supported, with limits that are decisions: a table's location is `abfs[s]://<container>@<account>.<dfs domain>/…` - `wasb[s]://` is never emitted and not accepted as a new location (N4) - the indexer writes it with the server's `blockstore.azure.*` credentials, and the `http`/`https` spellings of one namespace are treated as the same prefix. Credential vending and remote signing are **S3-only** (N1) - an engine uses its own Azure `FileIO` - the web UI object search is **S3-only** (N3), and `iceberg.azure.location_host: blob` is the server-wide switch for an engine whose Hadoop ABFS driver refuses a non-HNS account on the `dfs` host. See [Azure Blob Storage](#azure-blob-storage). |
| GCS | Out of scope (N2): `gs://` needs no location translation, but the indexer registers no GCS IO, so a GCS blockstore cannot be indexed in this version. |
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

- A read of `system.metadata.my-repo.<ref>.objects` (or of the deprecated catalog's
  `my-repo.<ref>.system.object_metadata`, which is the same table) is authorized with one
  `catalog:ReadTable` (and `catalog:ReadTableData` for vended credentials or remote signing) on the
  guide's ARNs of `my-repo` - `table/my-repo/system/object_metadata` and
  `namespace/my-repo/system`. There is **no** `fs:ReadRepository` requirement: Iceberg catalog
  permissions supersede any related object permissions, so a catalog-only policy is enough. The
  redirected read of `my-repo-metadata` runs with the server's identity, so the caller needs no
  permission there.
- **The caveat:** `catalog:` ARNs are branch-agnostic and address the table, so the grant covers the
  metadata of **all objects on all refs** of `my-repo`. A policy that narrows another table, a
  namespace, an object path or a branch does not narrow this table. Grant `catalog:ReadTable` on
  `my-repo` deliberately, and treat it as "may see the object inventory of the whole repository".
- Vended credentials and remote signing for the metadata table are **always read-only**: a write that
  the catalog redirects to the companion repository is answered with the read session (and a
  remote-signature write is refused with `403`), so an engine can read the table but cannot use a
  `catalog:WriteTableData` grant on `my-repo` to write the companion repository's files.

The guide's grant "for Metadata Search alone" is exactly those four catalog actions:

```json
[
  {
    "effect": "allow",
    "action": ["catalog:ReadTable", "catalog:ReadTableData"],
    "resource": "arn:lakefs:catalog:::table/my-repo/system/object_metadata"
  },
  {
    "effect": "allow",
    "action": ["catalog:GetNamespace", "catalog:ListNamespaces"],
    "resource": "arn:lakefs:catalog:::namespace/my-repo/system"
  }
]
```

Attach it to a query user and that user can read the table, list the tree above it and get read-only
vended credentials, with no `fs:` action at all. A user whose policy has an `fs:*` grant but no
`catalog:ReadTable` on that ARN gets `403`.

Other permission facts:

- A user who may read `my-repo-metadata` directly but not `my-repo` gets `403` on the metadata tree:
  authorization is checked on the data repository, not on the companion one.
- Reading `my-repo-metadata` directly as an ordinary repository needs read access to
  `my-repo-metadata`, like any repository. This is useful for inspecting the indexer's own table but
  is not the supported query path.
- The indexer writes as the sync identity, user `mds-service-user`, which carries the catalog-only
  `MDSCatalogAccess` policy (`catalog:*` on `arn:lakefs:catalog:::*`). That policy is a named,
  attachable policy: a user configured as the sync through `metadata_search.access_key_id`/`secret_key`
  needs the same policy attached. See [`The sync identity`](#2-the-sync-identity).
- There is no companion-repository deny policy in this build: the guide has none, and the earlier
  build's `mds-metadata-repo-protect-*` policies are removed on startup. Writing `my-repo-metadata` is
  governed by the ordinary repository permissions of the principal doing it; do not grant write access
  on a companion repository to principals that should not edit the index.
- On an auth backend without policies (`auth.basic`), the sync identity cannot be created or given
  its policy; the bootstrap prints a warning naming that consequence. Do not run Metadata Search on
  that backend without `access_key_id`/`secret_key` for an administrator.
- The root `system` namespace of the catalog is read-only for clients while the feature is enabled:
  creates, commits, registers, drops and namespace changes under it answer `403 ForbiddenException`.
  Nothing below `<repo>.<ref>` is reserved - a data repository's `system` namespace is an ordinary
  namespace a client may create and write, and the metadata table is not in it.

## Maintenance

The companion repository is an ordinary lakeFS repository with an Iceberg table in it. It needs the
same table maintenance any Iceberg table does, plus the tag lifecycle that makes history readable.

### Automatic compaction

Merge-on-read deletes accumulate position-delete files instead of rewriting data files on every
change. The indexer compacts a branch's table when either fixed trigger is crossed: more than **200**
delete files, or **500** indexed commits since the last compaction. The guide has no settings for them,
so they are not configurable in this build. A compaction scans the table
at the current snapshot, replaces all data and delete
files with rewritten data files in one transaction, keeps `lakefs.mds.source-commit`, and commits
with `mds: compact <R>@<C>`. The rewritten rows keep the table's declared `path ASC` order without
holding the whole table in memory: the compaction merges the per-file sorted streams, one read batch per
file plus one batch of output, so its memory follows the number of files and not the number of rows (above
512 files the floor of 256 rows per file is what binds). That is the merge's own share only: while a
compaction runs, the process also holds iceberg-go's per-file scan prefetch, its writer queue of up to 64
record batches, and one buffered Parquet row group (up to 1 048 576 rows or 128 MiB by default), all of
which are bounded independently of the table's row count. A
file written before commit `2daeca114` need not be internally sorted; it is detected from its path column
alone, then read whole and sorted on its own. Such a legacy file is kept resident until the merge finishes,
so a table last compacted by an older indexer costs the memory of **every** legacy file at once, not of one
file; one compaction removes the reason. A file the scan splits into several tasks is removed once, not
once per task. A
top-level column a later schema version appends keeps its values through a compaction, because the
compaction re-emits the scan's own Arrow columns instead of rebuilding rows. The delete files are taken
from the snapshot's manifest list, not from the
scan plan, so a delete file that no scan task carries any more — a rebuild replaces the data files it
applied to — is removed too; the 200-file trigger therefore counts a number that a compaction actually
drives to zero. It does not create a new tag; the existing `mds-*` tag still points at
the pre-compaction commit, which holds the same rows. Compactions are counted by
`lakefs_mds_compactions_total`.

**If the companion table's `write.delete.mode` is `copy-on-write`** — the fallback the plan keeps
when position delete files cannot be staged — deletes are still applied correctly, at a price. Without
the property at all, the indexer keeps iceberg-go's documented default, which is also
`copy-on-write`. In that mode a delta first folds the snapshot's live delete files into its data
files with one compaction, because iceberg-go's copy-on-write rewrite reads a data file without the
delete files that apply to it (upstream `apache/iceberg-go#2042`, fixed upstream in `#2046`). So one
source commit advances the companion branch twice, and the extra full table rewrite is paid at most
once after the last merge-on-read delete, since a copy-on-write delta writes no delete file itself.
A table that carries delete files but **no** `lakefs.mds.source-commit` property has no position for
that fold to record, so the delta is refused with `no source commit`; a rebuild of the branch — a
change set above 100,000 entries, or a history rewrite — repairs such a table. The indexer's own
tables carry `merge-on-read`, so this only concerns a table someone writes another way.

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
point at unreachable commits; see [`Tag retention`](#tag-retention) below.

### Tag retention

Every indexed source commit gets a tag `mds-<full commit ID>` on the corresponding commit of
`my-repo-metadata`. Those tags are what make a tag, a full commit ID, a short ID or `main~1` of the
data repository readable through the metadata table. They also keep their snapshots alive: a tagged
commit is retained forever by lakeFS GC.

Every `mds-*` tag is kept forever: the guide has no retention setting, so the indexer's tag-retention
code is fixed at "keep". Plan your lakeFS GC rules around that; branch reads do not depend on the tags
of old commits, so you may not need to keep their snapshots. (The retirement rules the code implements
when retention is on - never retire a tag that records a live position, retire one whose source commit
no longer exists - are covered by the indexer's tests and are reached by no configuration.)

Deleting a matching branch of `R` deletes the mirrored branch, with the guide's fixed behaviour. Its
tags stay, so history that was tagged remains readable; the branch itself does not. The companion
repository's own default branch is never deleted, even after `R` renames its default branch: lakeFS
refuses a default-branch delete, so the indexer keeps it and logs the skip once. The deleted branch's
`/_health` entry and `lakefs_mds_lag_commits` series are dropped, so a failed last attempt of the
deleted branch cannot keep `/_health` at `503`.

### Upgrades and restarts

- Restarting the indexer is safe and is part of normal operation; it resumes at the recorded
  position. A restart does not defer a scheduled compaction: the 500-commit count is read
  back from the companion branch's commit log, not kept in process state. A rolling restart that
  overlaps two processes is safe even during the first initialisation of a companion repository:
  only one of them commits `mds: init`, and the other adopts it.
- The table carries `commit.retry.num-retries: 0`, written at creation and re-asserted by every write.
  It is what makes a lost optimistic-concurrency check a conflict the indexer retries instead of a replay
  that could duplicate rows. Do not raise it: the next write of the indexer repairs it with a
  metadata-only commit before writing, and a repair that finds the branch moved fails the write with a
  stale-table error, so the tick reloads and retries.
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
  `system.metadata.<repo>.<ref>.objects`, and a results table.
- Requirements: S3 with credential vending (`role_arn`), a results lifecycle rule, and a single
  query pod.

None of those keys exist in this build. Until Option B lands, use the catalog from your engine of
choice and the snippets above.

## Clean-room note

Upstream Metadata Search is a lakeFS Team/Enterprise feature. This is an independent OSS
implementation written only from the public guide
(<https://docs.lakefs.io/guides/metadata-search/>), the Apache Iceberg specifications and this
repository's code. No upstream Enterprise code, configuration or binary is used.
