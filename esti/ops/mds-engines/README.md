# Running the Metadata Search engine snippets

These are the runnable forms of the Spark and Trino snippets in
[`docs/metadata-search.md`](../../../docs/metadata-search.md). They run against a throw-away
`lakefs-sso` with `iceberg.enabled` and `iceberg.metadata_search.enabled`, a real
`lakefs-sso mds run` indexer, and the SeaweedFS instance of
[`../docker-compose-mds.yaml`](../docker-compose-mds.yaml). The lakeFS and indexer configuration is
the one [`esti/mds_test.go`](../../mds_test.go) generates: the SSO/ACL auth service with the
break-glass `installation.*` administrator, the bootstrap's `mds-indexer` principal and deny policy,
static SeaweedFS `storage` keys, `period: 2s`, and the repository `my-repo` with branch `main`.

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

- Go, `curl`, `jq` and `python3` on `PATH`. The scripts build `lakefs-sso` and a one-purpose bucket
  helper from the repository sources. The brief's Go toolchain:

  ```bash
  export PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto
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
3. creates the bucket, creates credentials for the bootstrap-created `mds-indexer`, and deletes
   `my-repo` / `my-repo-metadata` left by an earlier run (the companion repository is deleted with
   the indexer's credentials because the bootstrap deny policy keeps the administrator out);
4. creates `my-repo` in a unique namespace, uploads `orders/2024/one.parquet`,
   `orders/2024/two.parquet`, `orders/2025/three.parquet` and `customers/c1.json`, and commits;
5. starts `lakefs-sso mds run` and waits until `/_health` reports
   `main.last_indexed == head`.

`run-spark.sh` writes the guide's Spark properties into `.run/spark-*.properties` and runs
`spark-sql` in `tabulario/spark-iceberg` once per endpoint. The identifier form quotes each
namespace level: ``lakefs.`my-repo`.`main`.`system`.object_metadata``.

`run-trino.sh` writes the guide's Trino catalog properties into
`.run/trino-*/etc/catalog/lakefs.properties`, starts `trinodb/trino`, waits for `SERVER STARTED`,
and runs the query through the image's Trino CLI once per endpoint.

Both runners bound each container run to **300 seconds** (`MDS_SPARK_TIMEOUT`,
`MDS_TRINO_TIMEOUT`). On timeout they remove the container, print the last 80 log lines and exit
non-zero. A container that exits unexpectedly is reported at once with its exit status and log
tail; it is never waited out.

## Versions used

The snippets were run on 2026-11-04 with:

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

Both endpoints returned the same three `orders/` rows for both engines:

```
orders/2024/one.parquet    34  <source commit ID>  {}
orders/2024/two.parquet    34  <source commit ID>  {}
orders/2025/three.parquet  36  <source commit ID>  {}
```

The `customers/c1.json` object was deliberately outside the `orders/%` filter. The Spark and Trino
runs used the same source commit as the indexer's `/_health` report.

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
