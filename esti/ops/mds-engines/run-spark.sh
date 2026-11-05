#!/usr/bin/env bash
# Run both Spark snippets of docs/metadata-search.md against the environment env-up.sh started.
#
# Spark is the Apache Spark 3.5.5 distribution with the Iceberg 1.8.1 runtime from
# tabulario/spark-iceberg. The catalog properties are the guide's, plus the S3 FileIO properties the
# guide says the examples pass explicitly: on SeaweedFS the client reads the Parquet files itself.
#
# Each variant is one container run with a five-minute budget.

set -euo pipefail

# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
load_state

require_command docker

SPARK_IMAGE=${MDS_SPARK_IMAGE:-tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460}
SPARK_TIMEOUT=${MDS_SPARK_TIMEOUT:-300}
# The colima VM is amd64 on an arm64 host. The JVM's C1 compiler has been observed to segfault under
# that emulation; compiling with C2 only is stable and still exercises the same Spark code.
SPARK_JAVA_TOOL_OPTIONS=${MDS_SPARK_JAVA_TOOL_OPTIONS:--XX:-TieredCompilation}

run_spark_variant() {
  local variant=$1 uri=$2 sql
  local props="$RUN_DIR/spark-${variant}.properties"
  local logfile="$RUN_DIR/spark-${variant}.log"
  local name="mds-spark-${variant}"
  local rows_dir="$RUN_DIR/spark-${variant}.rows"
  local rows_file="$RUN_DIR/spark-${variant}.rows.tsv"
  local from_clause

  cat >"$props" <<EOF
spark.sql.defaultCatalog lakefs
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakefs org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakefs.type rest
spark.sql.catalog.lakefs.uri ${uri}
spark.sql.catalog.lakefs.rest.auth.type basic
spark.sql.catalog.lakefs.credential ${MDS_ADMIN_KEY}:${MDS_ADMIN_SECRET}
spark.sql.catalog.lakefs.warehouse ${MDS_NAMESPACE}
spark.sql.catalog.lakefs.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakefs.s3.endpoint ${MDS_SEAWEEDFS_CONTAINER_ENDPOINT}
spark.sql.catalog.lakefs.s3.path-style-access true
spark.sql.catalog.lakefs.s3.access-key-id ${MDS_S3_ACCESS_KEY}
spark.sql.catalog.lakefs.s3.secret-access-key ${MDS_S3_SECRET_KEY}
spark.sql.catalog.lakefs.client.region ${MDS_S3_REGION}
EOF

  if [ "$variant" = "mds" ]; then
    from_clause="lakefs.\`${MDS_REPO}\`.\`main\`.\`system\`.object_metadata"
  else
    from_clause="lakefs.system.object_metadata"
  fi
  sql="$RUN_DIR/spark-${variant}.sql"
  # The first statement is the guide's snippet, unchanged. The second writes the same rows in a
  # parseable form, so the script can require exactly the three expected orders/ rows instead of
  # accepting any line that mentions orders/.
  cat >"$sql" <<SQL
SELECT path, size_bytes, commit_id, user_metadata
FROM ${from_clause}
WHERE path LIKE 'orders/%';

INSERT OVERWRITE DIRECTORY '/work/spark-${variant}.rows'
USING text
SELECT concat_ws('\t', path, cast(size_bytes as string), commit_id)
FROM ${from_clause}
WHERE path LIKE 'orders/%'
ORDER BY path;
SQL
  rm -rf "$rows_dir"

  log "running Spark ${variant} against ${uri}"
  run_docker_bounded "$name" "$SPARK_TIMEOUT" "$logfile" -- \
    --entrypoint bash \
    --add-host lakefs:host-gateway \
    --add-host seaweedfs:host-gateway \
    -e "JAVA_TOOL_OPTIONS=${SPARK_JAVA_TOOL_OPTIONS}" \
    -v "${RUN_DIR}:/work" \
    "$SPARK_IMAGE" \
    -lc "/opt/spark/bin/spark-sql --properties-file /work/spark-${variant}.properties -f /work/spark-${variant}.sql"
  part=$(find "$rows_dir" -type f -name 'part-*' | head -n 1 || true)
  if [ -z "$part" ]; then
    tail_file "$logfile" 80
    die "Spark ${variant} wrote no row file under $rows_dir"
  fi
  tr -d '\r' <"$part" | sed '/^[[:space:]]*$/d' >"$rows_file"
  assert_orders_rows "$rows_file" "Spark ${variant}"
}

run_spark_variant mds "${MDS_DOCKER_SERVER_URL}/mds/iceberg/api"
run_spark_variant relative "${MDS_DOCKER_SERVER_URL}/iceberg/relative_to/${MDS_REPO}.main/api"
assert_same_rows "$RUN_DIR/spark-mds.rows.tsv" "$RUN_DIR/spark-relative.rows.tsv" "Spark"
log "Spark snippets passed"
