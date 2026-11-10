# Running the Metadata Search engine snippets

These are the runnable forms of the Spark and Trino snippets in
[`docs/metadata-search.md`](../../../docs/metadata-search.md). They run against a throw-away
`lakefs-sso` with `iceberg.enabled` and `iceberg.metadata_search.enabled`, a real
`lakefs-sso mds run` indexer, and the SeaweedFS instance of
[`../docker-compose-mds.yaml`](../docker-compose-mds.yaml). The lakeFS and indexer configuration is
the one [`esti/mds_test.go`](../../mds_test.go) generates: the SSO/ACL auth service with the
break-glass `installation.*` administrator, the bootstrap's sync identity (`mds-service-user` with
`MDSCatalogAccess`), the server's own `blockstore.*` credentials for the indexer's writes (there is
no `metadata_search.storage` any more), `period: 2s`, and the repository `my-repo` with branch
`main`.

`lakefs-sso mds run` runs on the lakeFS server configuration (RA4b), so it opens the same
`database.*` KV store as the server. A local Badger database cannot be opened by two processes, so
`env-up.sh` starts a throw-away **PostgreSQL** container (`MDS_POSTGRES_*`, pinned digest) for the
run and stops it in `env-down.sh`; SeaweedFS is still only the object store.

The scripts do not edit or import the esti suite; they write the same layout into `.run/`.

## Prerequisites

- Docker (this repository's local recipe uses colima) and a reachable daemon.
- SeaweedFS on `http://127.0.0.1:8333`, with the credentials of
  [`../seaweedfs-s3.json`](../seaweedfs-s3.json). Reuse a running one; the scripts never start or
  stop it:

  ```bash
  cd /path/to/lakeFS
  docker compose -f esti/ops/docker-compose-mds.yaml up -d
  ```

  Without the compose plugin (for example colima with only the docker client):

  ```bash
  docker run -d --name esti-seaweedfs -p 8333:8333 \
    -v "$PWD/esti/ops/seaweedfs-s3.json:/etc/seaweedfs/s3.json:ro" \
    -v esti-seaweedfs:/data chrislusf/seaweedfs:4.53 \
    server -s3 -dir=/data -s3.config=/etc/seaweedfs/s3.json
  ```

- Go, `curl`, `jq` and `python3` on `PATH`. The scripts build `lakefs-sso` and a one-purpose bucket
  helper from the repository sources. The brief's Go toolchain:

  ```bash
  export PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto
  ```

- The PyIceberg/DuckDB client image of [`esti/mds_test.go`](../../mds_test.go) bakes the pinned
  clients in, so no test step spends its time in `pip install`. Build it locally (never pushed to a
  registry) and point the suite at it:

  ```bash
  docker build -t esti-iceberg-client esti/ops/iceberg-client && export ESTI_ICEBERG_CLIENT_IMAGE=esti-iceberg-client
  ```

- Free host ports `8000` (lakeFS), `18080` (indexer) and `18099` (the OIDC discovery document the
  SSO section needs). Override them with `MDS_SERVER_PORT`, `MDS_INDEXER_PORT` and
  `MDS_OIDC_PORT`; the container host alias `lakefs` follows `MDS_SERVER_PORT`.

## Exact commands

All commands run from the repository root.

```bash
# Everything: start the installation, run both Spark variants and both Trino variants, tear down.
./esti/ops/mds-engines/run-all.sh
```

Or step by step, keeping the installation up between steps:

```bash
./esti/ops/mds-engines/env-up.sh
./esti/ops/mds-engines/run-spark.sh
./esti/ops/mds-engines/run-trino.sh
./esti/ops/mds-engines/env-down.sh
```

`run-spark.sh` also has a self-test that needs neither docker nor an environment; it fabricates the
`part-*` layouts Spark can write and proves the row gate:

```bash
./esti/ops/mds-engines/run-spark.sh --self-test
```

`run-all.sh` calls `env-down.sh` on success and on failure. `env-down.sh` stops only the
`lakefs-sso`, indexer and OIDC processes these scripts started, plus their engine containers; it
leaves SeaweedFS running.

Logs, generated properties/SQL, the lakeFS data directory and the state file are under `.run/`
(ignored by git). The state file holds the generated indexer key pair and the bucket namespace of
the run.

## What each script does

`env-up.sh`:

1. builds `lakefs-sso` and `tools/bucket`;
2. serves a static OIDC discovery document (no login is performed; every client authenticates with
   a lakeFS key pair) and starts `lakefs-sso run`;
3. creates the bucket and deletes `my-repo` / `my-repo-metadata` left by an earlier run;
4. creates `my-repo` in a unique namespace, uploads `orders/2024/one.parquet`,
   `orders/2024/two.parquet`, `orders/2025/three.parquet` and `customers/c1.json`, and commits;
5. starts `lakefs-sso mds run` (with its own `LAKEFS_COMMITTED_LOCAL_CACHE_DIR`) and waits until
   `/_health` reports `main.last_indexed == head`.

`run-spark.sh` writes the guide's Spark properties into `.run/spark-*.properties` and runs
`spark-sql` in `tabulario/spark-iceberg` once per address. The first SQL statement is the guide's
snippet unchanged; the second writes the snippet's own projection (the same four columns, table
and filter, `to_json(user_metadata)` so the map is text) into `.run/spark-<variant>.rows`. The gate
then concatenates **every** `part-*` file Spark wrote before comparing them with the three expected
rows, because `INSERT OVERWRITE DIRECTORY` writes one file per non-empty output partition: reading
only the first file would miss an extra row in any other one, and would report a correct engine as
wrong whenever Spark spreads the three rows over several files. The two addresses are:

- `catalog`: `/iceberg/api`, reading `lakefs.system.metadata.`my-repo`.main.objects`;
- `relative`: `/iceberg/relative_to/system.metadata/api`, reading
  ``lakefs.`my-repo`.`main`.objects`` (the warehouse fixes `system.metadata`).

`run-trino.sh` writes the guide's Trino catalog properties into
`.run/trino-*/etc/catalog/lakefs.properties`, starts `trinodb/trino`, waits for `SERVER STARTED`,
and runs the query through the image's Trino CLI once per address
(`/iceberg/api` with `"system.metadata.<repo>.<ref>".objects` and
`/iceberg/relative_to/system.metadata/api` with `"<repo>.<ref>".objects`; both need
`nested-namespace-enabled=true`).

Both runners bound each container run to **300 seconds** (`MDS_SPARK_TIMEOUT`,
`MDS_TRINO_TIMEOUT`). On timeout they remove the container, print the last 80 log lines and exit
non-zero. A container that exits unexpectedly is reported at once with its exit status and log
tail; it is never waited out.

## Versions used

The snippets were run on 2026-11-04 with the images below. `run-spark.sh` and `run-trino.sh`
default to these digests, so a rerun reproduces the recorded versions; override `MDS_SPARK_IMAGE` or
`MDS_TRINO_IMAGE` to use another image (for example a local `:latest` tag).

| Component | Version / image |
|---|---|
| Spark | Apache Spark 3.5.5, Scala 2.12.18, OpenJDK 17.0.14 |
| Iceberg Spark runtime | 1.8.1 (`iceberg-spark-runtime-3.5_2.12-1.8.1.jar`) |
| Spark image | `tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460` |
| Trino | 484, Java 25.0.4 |
| Trino image | `trinodb/trino@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f` |
| SeaweedFS | `chrislusf/seaweedfs@sha256:c2658820e763294a9e9de7b36c81e59b879e3459148351aee4e31a126ca64860` |
| lakefs-sso / indexer | built from this branch (`go build ./cmd/lakefs-sso`) |

The colima VM here is amd64 on an arm64 host. Both runners set
`JAVA_TOOL_OPTIONS=-XX:-TieredCompilation` by default: the JVM's C1 compiler was observed to
segfault under that emulation, while C2-only startup is stable. The code under test is unchanged;
override `MDS_SPARK_JAVA_TOOL_OPTIONS` / `MDS_TRINO_JAVA_TOOL_OPTIONS` on a native amd64 host if
you want the default JVM settings.

## Observed result

The earlier build's runs below (`/mds/iceberg/api` and the data-repository `relative_to` form) are
kept as the last recorded observation of those images; RA8 moved the scripts to the current
addresses but did not re-run Spark or Trino, which needs a deployment. The rows the current build
serves are the same three `orders/` rows.

```
orders/2024/one.parquet    34  <source commit ID>  {}
orders/2024/two.parquet    34  <source commit ID>  {}
orders/2025/three.parquet  36  <source commit ID>  {}
```

The `customers/c1.json` object was deliberately outside the `orders/%` filter. The Spark and Trino
runs used the same source commit as the indexer's `/_health` report.

That observation is a gate, not a reading of `.run/*.log`: each runner captures its row file
(`.run/spark-*.rows.tsv`, `.run/trino-*.rows.tsv`), requires exactly those three rows with those
sizes and `SOURCE_COMMIT` from `.run/state.env`, and diffs the two catalog endpoints' captures. A
missing row, a stale snapshot, an extra row of another repository or a different commit fails the
script.

The Spark gate compares the guide's snippet's own projection, `path`, `size_bytes`, `commit_id`
**and** `user_metadata` (the empty map the uploads have), and it concatenates every `part-*` file
Spark wrote before comparing, so the three numbers above are checked over the whole result and an
extra row in a second, third, … part file fails the run. `./run-spark.sh --self-test` proves both
properties on fabricated part files without docker; the eight cases are a single part file with the
expected rows, the same rows spread over two part files, an extra row in either part file (each of
the two creation orders), a wrong `user_metadata` in a single three-row part file and in either of
two part files (three rows in total, so the row count passes and only the comparison can reject
them), and a directory with no `part-*` file at all.

### Rerun after the row-gate fix (2026-11-05)

`./run-all.sh` on the digests above, after the Spark gate was changed to concatenate every
`part-*` file and to compare the guide's snippet's own four columns: 4/4 engine runs passed,
wall **508s** (8m28s) including `env-up.sh` and `env-down.sh`, `SOURCE_COMMIT=962ec8eb…445ae`.

Spark wrote **one** `part-*` file per variant in this run (`spark mds wrote 1 part file(s) …;
comparing all of them`), so the layout this run happened to produce does not exercise the
multi-file path. That path is proven by `./run-spark.sh --self-test` instead (7/7 cases passed at
the time; `0f45215bc` added the single-file `user_metadata` case, and 8/8 pass since): with
the pre-fix gate (`find … -name 'part-*' | head -n 1`) the same self-test fails 3 cases — the rows
split over two part files are reported as 2 rows, and an extra row in a part file goes unnoticed
whenever the correct file is the one the gate reads.

## Snippet changes this run required

- **Spark identifier form.** The guide previously wrote
  ``lakefs.`my-repo.main.system`.object_metadata``. Spark treats that as one namespace level named
  `my-repo.main.system`, and the catalog answers
  `namespace "my-repo.main.system" must start with a repository and a ref`. Quote each level:
  ``lakefs.`my-repo`.`main`.`system`.object_metadata``.
- **Spark S3 FileIO and region.** The previous snippet had no way to read data files when the
  catalog does not vend credentials, and Iceberg 1.8 reads the AWS region from `client.region`, not
  `s3.region`. The guide now lists `io-impl`, the `s3.*` FileIO keys and `client.region`.
- **Trino 484 security and FileIO keys.** Trino 484 removed `iceberg.rest-catalog.security=BASIC`
  (the enum is now `NONE`, `OAUTH2`, `SIGV4`, `GOOGLE`) and the `user`/`password` properties. The
  guide now uses `OAUTH2` with `oauth2.credential` and `oauth2.server-uri`, and
  `fs.s3.enabled=true` instead of the renamed `fs.native-s3.enabled`. Trino also needs
  `nested-namespace-enabled=true` for the three-level identifier form.

PyIceberg and DuckDB are covered by [`esti/mds_test.go`](../../mds_test.go), not by these scripts;
they were not re-run here. SeaweedFS does not vend credentials, so all four engine runs use static
S3 keys, which is the mode the guide's "pass S3 credentials explicitly" note describes.

## Azure (hr2)

> **Historical record (AZ7, 2026-11-09).** These runs were made against the earlier build: the
> `/mds/iceberg/api` catalog and the data-repository `relative_to` form, an indexer that was a REST
> client configured through `LAKEFS_METADATA_SEARCH_AZURE_*`, and the `mds-indexer` principal. Since
> METADATA_SEARCH_REALIGN.md RA4, RA4b and RA5, the indexer runs on the server configuration (its
> Azure credentials are `blockstore.azure.*`), the tables are at `system.metadata.<repo>.<ref>.objects`
> on `/iceberg/api` or through `relative_to/system.metadata/api`, and the sync identity is
> `mds-service-user` with `MDSCatalogAccess`. These runs were not repeated after the realignment;
> doing so needs the deployment and is a separate card.

The same four engine runs (both engines, both catalog mounts) against a **real Azure storage
account**, on the hr2 cluster, 2026-11-09. This is AZ7 of
[`METADATA_SEARCH_AZURE.md`](../../../METADATA_SEARCH_AZURE.md): the Azurite cases
([`esti/mds_azure_test.go`](../../mds_azure_test.go)) cover the catalog, the indexer and the
PyIceberg/DuckDB recipes, but an emulator has no `dfs` endpoint and no hierarchical namespace, and
Spark and Trino cannot run against one at all.

Installation: the lakeFS Deployment `lakefs` in namespace `lakefs` (blockstore `azure`, shared key,
`iceberg.enabled`, `iceberg.metadata_search.enabled`, `iceberg.azure.location_host` unset), the
indexer `mds-indexer` on the same image with its Azure credentials from
`LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCOUNT` / `…_STORAGE_ACCESS_KEY` alone, storage account
`cpaasailakefs`, container `lakefs`, repository `mds-az7` on
`https://cpaasailakefs.blob.core.windows.net/lakefs/mds-az7-e1f13de`, branch `main`. The indexer's
`/_health` reported `main.last_indexed == head == 53c0d597…95bb9` with `lag_commits: 0`, 33 s
after the commit. The runners, logs, row captures and the corporate-CA truststore stayed outside
the repository (`~/az7-hr2/engines/`); they are the same scripts as `run-spark.sh`/`run-trino.sh`
adapted for the two mounts of a remote installation, and this directory was not edited for the run.

### Versions used

| Component | Version / reference |
|---|---|
| lakeFS (`lakefs-sso`, server and indexer) | `docker.ib-ci.com/lakefs:dev-mds`, digest `docker.ib-ci.com/lakefs@sha256:038fdf36e7fee16eeba8c21e1d987ea86b3d794c7b220e797e47aa5102dab759`; version string `dev-mds` (upstream base 1.88.0) |
| lakeFS source SHA | **not recoverable from the deployed image**: the version string is the mutable tag `dev-mds`, the image config carries no labels and the manifest no annotations, so the digest above is the only immutable identifier of the build that served these runs |
| Spark | Apache Spark 3.5.5, Scala 2.12.18, OpenJDK 17.0.14 |
| Spark image | `tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460` (the SeaweedFS digest, for comparability) |
| Iceberg Spark runtime | 1.8.1 (`iceberg-spark-runtime-3.5_2.12-1.8.1.jar`) |
| Iceberg Azure bundle | `org.apache.iceberg:iceberg-azure-bundle:1.8.1`, passed as `--packages` |
| Trino | 484, Iceberg 1.12.0, native Azure file system (`fs.azure.enabled=true`) |
| Trino image | `trinodb/trino@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f` (the SeaweedFS digest) |
| Storage account kind | **HNS** (hierarchical namespace) - `GET`/`HEAD` on `https://cpaasailakefs.dfs.core.windows.net/…` answered `200` with `x-ms-resource-type: directory`, which an FNS account cannot do |
| `iceberg.azure.location_host` | unset → default **`dfs`** for all four runs; no engine needed `blob` |

### Observed result

All four runs returned the same three `orders/` rows, byte for byte the rows the SeaweedFS record
above reports (same sizes, because the objects were uploaded with the same content; `user_metadata`
is `{}` for the same reason - the core lakeFS REST API cannot set user metadata on an upload, and
the S3 gateway is not exposed on hr2):

```
path                        size_bytes  commit_id                                                           user_metadata
orders/2024/one.parquet     34          53c0d5979f937dc8542b7e96c98993f950a6a3b05278945b51e14460e8e95bb9    {}
orders/2024/two.parquet     34          53c0d5979f937dc8542b7e96c98993f950a6a3b05278945b51e14460e8e95bb9    {}
orders/2025/three.parquet   36          53c0d5979f937dc8542b7e96c98993f950a6a3b05278945b51e14460e8e95bb9    {}
```

`commit_id` is the branch head the indexer indexed, as `/_health` reported it.

| # | Engine | Endpoint | Rows | `location_host` |
|---|---|---|---|---|
| 1 | Spark 3.5.5 + Iceberg 1.8.1, `ADLSFileIO` | `https://lakefs-hr2.ibfr4eq.local/mds/iceberg/api` | 3/3 exact | `dfs` |
| 2 | Spark 3.5.5 + Iceberg 1.8.1, `ADLSFileIO` | `https://lakefs-hr2.ibfr4eq.local/iceberg/relative_to/mds-az7.main/api` | 3/3 exact | `dfs` |
| 3 | Trino 484, native Azure FS | `https://lakefs-hr2.ibfr4eq.local/mds/iceberg/api` | 3/3 exact | `dfs` |
| 4 | Trino 484, native Azure FS | `https://lakefs-hr2.ibfr4eq.local/iceberg/relative_to/mds-az7.main/api` | 3/3 exact | `dfs` |

Warehouse for both engines: `https://cpaasailakefs.blob.core.windows.net/lakefs/mds-az7-e1f13de`.
What the catalog hands them is D1's canonical form, read back with the REST API:

```
GET …/mds/iceberg/api/v1/lakefs/namespaces/mds-az7<0x1f>main<0x1f>system/tables/object_metadata
→ metadata-location: abfss://lakefs@cpaasailakefs.dfs.core.windows.net/mds-az7-e1f13de-metadata/
                         _iceberg/878d1bae-…/metadata/00002-48296561-….metadata.json
```

and each engine opens its files through the **`dfs`** host, not `blob`: Spark's `ADLSFileIO` derives
its endpoint from the location (`ADLSLocation.host()`, via
`azureProperties.applyClientConfiguration`), and Trino 484's `AzureFileSystem` builds its DataLake
client from the default `azure.endpoint=core.windows.net` with the constant `https://%s.dfs.%s`.
Trino's startup dump of the catalog properties shows `fs.azure.enabled false → true` and
`azure.auth-type DEFAULT → ACCESS_KEY`, so the native Azure FS is the path the read took.

As on SeaweedFS this is a **gate, not a reading of a log**: each runner fails unless its capture
has exactly three `orders/` rows equal to the three above on all four columns, then the two mounts'
captures are compared with each other. Both Spark runs wrote one `part-*` file, and every file is
concatenated before the comparison, as in `run-spark.sh`. The gate was probed with fabricated
captures and rejects a wrong `size_bytes`, a wrong `user_metadata` and a fourth `orders/` row.
Unlike the local runs the gate is not a self-test of the snippet alone: the rows exist only because
the indexer wrote them from real lakeFS commits on the real account.

### Snippet changes this run required

Everything else in the guide's Azure Spark and Trino blocks ran unchanged. Exact replacement text:

1. **Trino, the switch comment.** The guide's line and comment:

   ```
   fs.azure.enabled=true          # the newer name of the same switch is fs.native-azure.enabled
   ```

   replace with:

   ```
   fs.azure.enabled=true          # Trino 484: the @Config name; fs.native-azure.enabled is its @LegacyConfig alias
   ```

   Verified against the pinned image, which is what D5 asked for: in
   `trino-filesystem-manager-484.jar`, `FileSystemConfig.setAzureEnabled` carries
   `@Config("fs.azure.enabled")` and `@LegacyConfig(["fs.native-azure.enabled"])`. The run wrote
   `fs.azure.enabled=true` and Trino took it. The legacy alias itself was not run. This also means
   D5's parenthetical in `METADATA_SEARCH_AZURE.md` reads backwards: 484 renamed
   `fs.native-azure.enabled` **to** `fs.azure.enabled`, not the other way round.

2. **Trino, the relative-to endpoint.** The guide's Azure part shows only the `/mds/iceberg/api`
   block. For `/iceberg/relative_to` the same block applies with two values replaced and the nested
   flag dropped:

   ```
   iceberg.rest-catalog.uri=https://<lakefs>/iceberg/relative_to/mds-az7.main/api
   iceberg.rest-catalog.oauth2.server-uri=https://<lakefs>/iceberg/api/v1/oauth/tokens
   # no iceberg.rest-catalog.nested-namespace-enabled (three-level identifier not used on this mount)
   ```

   (`nested-namespace-enabled=true` **is** required on the `/mds/iceberg/api` mount, and the guide's
   Azure Trino block already has it.)

3. **Spark, a recommended key the guide omits.** Iceberg 1.8.1 logs a warning and falls back to the
   MDS mount's token endpoint; the snippet should carry the value it falls back to:

   ```
   spark.sql.catalog.lakefs.oauth2-server-uri=https://<lakefs>/mds/iceberg/api/v1/oauth/tokens
   ```

   (for relative-to, `…/iceberg/api/v1/oauth/tokens`).

4. **The guide's placeholders, filled for this installation** - values, not snippet changes: `uri`
   (both mounts above), `credential` (the installation admin key pair, read from the cluster's
   Secret at use time and never written to disk), `warehouse`
   (`https://cpaasailakefs.blob.core.windows.net/lakefs/mds-az7-e1f13de`),
   `adls.auth.shared-key.account.name=cpaasailakefs` and `…account.key=<storage account key>`,
   `azure.access-key=<storage account key>`. No `azure.endpoint` is needed on public cloud.

5. **Harness only, not a snippet change: the corporate CA.** From the office network, TLS to the
   lakeFS ingress and to `*.dfs.core.windows.net` (which is MITM'd) is signed by a corporate root
   the images do not trust, so both runners add that public chain to a copy of the image's own
   `cacerts` and point `JAVA_TOOL_OPTIONS` at it, keeping `-XX:-TieredCompilation` as in the
   SeaweedFS record. The cluster pods are not affected; this is a property of the office network,
   not of Azure support.

### Not verified by this run

- **Writes.** Both engines only read. No table was created or committed through either mount.
- **Non-empty `user_metadata`.** The rows carry `{}` (see above), so an engine rendering a populated
  map on Azure is untested; on SeaweedFS it is covered.
- **`location_host: blob`.** Never exercised: the account is HNS and no engine failed on `dfs`, so
  D1's `blob` branch remains unexercised on real Azure and the ConfigMap was not touched.
- **The `fs.native-azure.enabled` legacy alias** - recorded from the connector's bytecode only.
- **In-cluster service DNS.** Both engines ran through the ingress, not through
  `http://lakefs.lakefs.svc.cluster.local:8000`.
- **Iceberg tables in the data repository itself.** The MDS table lives in the companion namespace
  (`…/mds-az7-e1f13de-metadata/`), which is the same prefix form but not the data repository's own
  namespace.
- **PyIceberg and DuckDB on real Azure** - covered on Azurite by
  [`esti/mds_azure_test.go`](../../mds_azure_test.go), not re-run here.
