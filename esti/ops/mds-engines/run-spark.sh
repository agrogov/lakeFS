#!/usr/bin/env bash
# Run both Spark snippets of docs/metadata-search.md against the environment env-up.sh started.
#
# Spark is the Apache Spark 3.5.5 distribution with the Iceberg 1.8.1 runtime from
# tabulario/spark-iceberg. The catalog properties are the guide's, plus the S3 FileIO properties the
# guide says the examples pass explicitly: on SeaweedFS the client reads the Parquet files itself.
#
# The first statement of each run is the guide's snippet, unchanged. The second writes the snippet's
# own projection -- the same four columns, table and filter -- to text, so the script can compare
# every part file Spark wrote with the three expected orders/ rows. Spark writes one output file per
# non-empty output partition, so reading only the first part-* file compares a strict subset of what
# the engine returned; all of them are concatenated before the comparison.
#
# Each variant is one container run with a five-minute budget.
#
# Usage:
#   ./run-spark.sh              # both variants, against the environment env-up.sh started
#   ./run-spark.sh --self-test  # prove the row gate; needs no docker and no environment
#
# --self-test fabricates part-file layouts under a temporary directory and requires the gate to
# accept the three expected rows spread over several part files and to reject an extra row or a
# wrong user_metadata in any part file.

set -euo pipefail

# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

SPARK_IMAGE=${MDS_SPARK_IMAGE:-tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460}
SPARK_TIMEOUT=${MDS_SPARK_TIMEOUT:-300}
# The colima VM is amd64 on an arm64 host. The JVM's C1 compiler has been observed to segfault under
# that emulation; compiling with C2 only is stable and still exercises the same Spark code.
SPARK_JAVA_TOOL_OPTIONS=${MDS_SPARK_JAVA_TOOL_OPTIONS:--XX:-TieredCompilation}

usage() {
  cat <<'EOF'
usage: run-spark.sh [--self-test]

  (no argument)  run both Spark variants of docs/metadata-search.md against the environment
                 env-up.sh started
  --self-test    prove the row gate on fabricated part files; no docker, no environment
  -h, --help     this text
EOF
}

# check_spark_rows DIR DESCRIPTION ROWS_FILE
# Gate one Spark variant's output: concatenate every part-* file DIR holds, strip CRs and blank
# lines, and require exactly the three expected orders/ rows of state.env as the guide's snippet
# projects them -- path, size_bytes, commit_id and user_metadata. A row that only some other part
# file carries fails here; the first part-* file is never trusted on its own.
check_spark_rows() {
  local dir=$1 description=$2 rows_file=$3

  concat_part_rows "$dir" "$rows_file.concat" "$description"
  tr -d '\r' <"$rows_file.concat" | sed '/^[[:space:]]*$/d' >"$rows_file"
  assert_orders_rows "$rows_file" "$description" 4
}

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
  # The first statement is the guide's snippet, unchanged. The second writes the snippet's own
  # projection -- path, size_bytes, commit_id and user_metadata, from the same table with the same
  # filter -- to a parseable text file, so the script can require exactly the three expected
  # orders/ rows, including the empty user_metadata map, instead of accepting any line that
  # mentions orders/.
  cat >"$sql" <<SQL
SELECT path, size_bytes, commit_id, user_metadata
FROM ${from_clause}
WHERE path LIKE 'orders/%';

INSERT OVERWRITE DIRECTORY '/work/spark-${variant}.rows'
USING text
SELECT concat_ws('\t', path, cast(size_bytes as string), commit_id, to_json(user_metadata))
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
  if [ ! -d "$rows_dir" ]; then
    tail_file "$logfile" 80
    die "Spark ${variant} wrote no row directory at $rows_dir"
  fi
  check_spark_rows "$rows_dir" "Spark ${variant}" "$rows_file"
}

# --- self-test -------------------------------------------------------------
# Fabricates the part-file layouts Spark can write and asserts that the gate (a) passes the three
# expected rows in one part file, (b) passes them spread over two part files -- the false failure a
# first-file-only comparison produces whenever Spark does not coalesce the output, (c) fails when
# one part file holds exactly the expected rows and another one an extra row, in both creation
# orders (the R2T-4 probe: a first-file-only gate passes whichever of the two layouts it happens to
# read the correct file from), (d) fails when one part file holds the three expected rows and
# another the same rows with a user_metadata the guide's snippet selects, in both creation orders,
# and (e) fails when the directory holds no part-* file at all.
#
# Each two-file case is written twice, once with the wrong content in part-00000 and once with it in
# part-00001. A gate that reads only one part-* file passes one of the two layouts whatever order
# the filesystem lists the two names in, so it cannot pass both; a gate that reads all of them
# rejects both, because the rows they hold together are not the three expected rows.

write_part_file() { # <dir> <name> <contents>
  mkdir -p "$1"
  printf '%s' "$3" >"$1/$2"
}

run_self_test() {
  local tmp failures=0
  tmp=$(mktemp -d)
  # shellcheck disable=SC2064 # expand $tmp now, it is local to this function
  trap "rm -rf '${tmp}'" EXIT

  RUN_DIR="$tmp/run"
  mkdir -p "$RUN_DIR"
  SOURCE_COMMIT=0000000000000000000000000000000000000000000000000000000000000000

  local one two three extra bad where
  one=$(printf 'orders/2024/one.parquet\t34\t%s\t{}' "$SOURCE_COMMIT")
  two=$(printf 'orders/2024/two.parquet\t34\t%s\t{}' "$SOURCE_COMMIT")
  three=$(printf 'orders/2025/three.parquet\t36\t%s\t{}' "$SOURCE_COMMIT")
  extra=$(printf 'orders/2025/four.parquet\t37\t%s\t{}' "$SOURCE_COMMIT")
  bad=$(printf 'orders/2025/three.parquet\t36\t%s\t{"team":"analytics"}' "$SOURCE_COMMIT")

  expect() { # <expected-exit> <dir> <description>
    local expected=$1 dir=$2 description=$3 status=0
    ( check_spark_rows "$dir" "$description" "$RUN_DIR/selftest.rows.tsv" ) \
      >"$RUN_DIR/selftest.log" 2>&1 || status=$?
    if [ "$status" -eq "$expected" ]; then
      printf 'PASS: %s (exit %s)\n' "$description" "$status"
    else
      printf 'FAIL: %s (expected exit %s, got %s)\n' "$description" "$expected" "$status" >&2
      tail_file "$RUN_DIR/selftest.log" 20
      failures=$((failures + 1))
    fi
  }

  where="$tmp/one-file"
  write_part_file "$where" 'part-00000-0000-0000-0000-c000.txt' "$one
$two
$three
"
  expect 0 "$where" "selftest: one part file with the three expected rows passes"

  where="$tmp/split"
  write_part_file "$where" 'part-00000-0000-0000-0000-c000.txt' "$one
"
  write_part_file "$where" 'part-00001-0000-0000-0000-c000.txt' "$two
$three
"
  expect 0 "$where" "selftest: the three expected rows over two part files pass"

  where="$tmp/extra-in-part-00001"
  write_part_file "$where" 'part-00000-0000-0000-0000-c000.txt' "$one
$two
$three
"
  write_part_file "$where" 'part-00001-0000-0000-0000-c000.txt' "$extra
"
  expect 1 "$where" "selftest: an extra row in part-00001 fails (R2T-4)"

  where="$tmp/extra-in-part-00000"
  write_part_file "$where" 'part-00001-0000-0000-0000-c000.txt' "$one
$two
$three
"
  write_part_file "$where" 'part-00000-0000-0000-0000-c000.txt' "$extra
"
  expect 1 "$where" "selftest: an extra row in part-00000 fails (R2T-4)"

  where="$tmp/metadata-in-part-00001"
  write_part_file "$where" 'part-00000-0000-0000-0000-c000.txt' "$one
$two
$three
"
  write_part_file "$where" 'part-00001-0000-0000-0000-c000.txt' "$bad
$two
$one
"
  expect 1 "$where" "selftest: a user_metadata in part-00001 is compared"

  where="$tmp/metadata-in-part-00000"
  write_part_file "$where" 'part-00001-0000-0000-0000-c000.txt' "$one
$two
$three
"
  write_part_file "$where" 'part-00000-0000-0000-0000-c000.txt' "$bad
$two
$one
"
  expect 1 "$where" "selftest: a user_metadata in part-00000 is compared"

  where="$tmp/no-part"
  write_part_file "$where" '_SUCCESS' ''
  expect 1 "$where" "selftest: a directory with no part-* file fails"

  if [ "$failures" -ne 0 ]; then
    printf 'self-test FAILED (%s case(s))\n' "$failures" >&2
    exit 2
  fi
  printf 'self-test passed\n'
  exit 0
}

main() {
  load_state

  require_command docker

  run_spark_variant mds "${MDS_DOCKER_SERVER_URL}/mds/iceberg/api"
  run_spark_variant relative "${MDS_DOCKER_SERVER_URL}/iceberg/relative_to/${MDS_REPO}.main/api"
  assert_same_rows "$RUN_DIR/spark-mds.rows.tsv" "$RUN_DIR/spark-relative.rows.tsv" "Spark"
  log "Spark snippets passed"
}

case "${1:-}" in
"") main ;;
--self-test) run_self_test ;;
-h | --help)
  usage
  exit 0
  ;;
*)
  usage >&2
  exit 2
  ;;
esac
