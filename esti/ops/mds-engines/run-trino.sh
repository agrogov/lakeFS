#!/usr/bin/env bash
# Run both Trino snippets of docs/metadata-search.md against the environment env-up.sh started.
#
# Trino 484 removed the old BASIC rest-catalog security setting: the key pair is exchanged at the
# catalog's OAuth2 token endpoint (iceberg.rest-catalog.oauth2.credential) and the connector uses
# its own S3 credentials because vended-credentials-enabled=false. The S3 FileIO block is the
# "pass S3 credentials explicitly" part the guide's engine examples assume.
#
# Each variant is one container run with a five-minute budget. The container is started detached so
# the script can wait for "SERVER STARTED" in its log and then issue the query through the Trino CLI.

set -euo pipefail

# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
load_state

require_command docker

TRINO_IMAGE=${MDS_TRINO_IMAGE:-trinodb/trino@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f}
TRINO_TIMEOUT=${MDS_TRINO_TIMEOUT:-300}
# The colima VM is amd64 on an arm64 host. Trino 484's JVM crashes in the C1 compiler under that
# emulation; C2-only startup is stable and runs the same connector code.
TRINO_JAVA_TOOL_OPTIONS=${MDS_TRINO_JAVA_TOOL_OPTIONS:--XX:-TieredCompilation}

CURRENT_CONTAINER=
cleanup_container() {
  if [ -n "$CURRENT_CONTAINER" ]; then
    docker_rm "$CURRENT_CONTAINER"
  fi
}
trap cleanup_container EXIT

run_trino_variant() {
  local variant=$1 uri=$2 oauth_uri=$3 nested=$4
  local etc="$RUN_DIR/trino-${variant}/etc/catalog"
  local logfile="$RUN_DIR/trino-${variant}.log"
  local querylog="$RUN_DIR/trino-${variant}.query.log"
  local rows_file="$RUN_DIR/trino-${variant}.rows.tsv"
  local name="mds-trino-${variant}"
  local start deadline remaining status

  mkdir -p "$etc"
  {
    echo "connector.name=iceberg"
    echo "iceberg.catalog.type=rest"
    echo "iceberg.rest-catalog.uri=${uri}"
    echo "iceberg.rest-catalog.security=OAUTH2"
    echo "iceberg.rest-catalog.oauth2.credential=${MDS_ADMIN_KEY}:${MDS_ADMIN_SECRET}"
    echo "iceberg.rest-catalog.oauth2.server-uri=${oauth_uri}"
    echo "iceberg.rest-catalog.vended-credentials-enabled=false"
    # Both addresses name a multi-level namespace (<repo>.<ref> or system.metadata.<repo>.<ref>), so
    # the nested-namespace switch is required on both.
    [ "$nested" = "true" ] && echo "iceberg.rest-catalog.nested-namespace-enabled=true"
    echo "fs.s3.enabled=true"
    echo "s3.endpoint=${MDS_SEAWEEDFS_CONTAINER_ENDPOINT}"
    echo "s3.region=${MDS_S3_REGION}"
    echo "s3.aws-access-key=${MDS_S3_ACCESS_KEY}"
    echo "s3.aws-secret-key=${MDS_S3_SECRET_KEY}"
    echo "s3.path-style-access=true"
  } >"$etc/lakefs.properties"

  log "starting Trino ${variant} against ${uri}"
  docker_rm "$name"
  CURRENT_CONTAINER=$name
  start=$(date +%s)
  deadline=$((start + TRINO_TIMEOUT))
  docker run -d --name "$name" \
    --add-host lakefs:host-gateway \
    --add-host seaweedfs:host-gateway \
    -e "JAVA_TOOL_OPTIONS=${TRINO_JAVA_TOOL_OPTIONS}" \
    -v "${etc}:/etc/trino/catalog" \
    "$TRINO_IMAGE" >/dev/null
  docker logs -f "$name" >"$logfile" 2>&1 &
  local log_pid=$!

  while :; do
    status=$(docker ps -a --filter "name=${name}" --format '{{.Status}}' 2>/dev/null || true)
    if [ -z "$status" ] || [ "${status#Up}" = "$status" ]; then
      printf 'mds-engines: Trino %s exited while starting (status: %s)\n' "$variant" "${status:-missing}" >&2
      kill "$log_pid" 2>/dev/null || true
      tail_file "$logfile" 100
      return 1
    fi
    if grep -q 'SERVER STARTED' "$logfile" 2>/dev/null; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      printf 'mds-engines: Trino %s did not reach SERVER STARTED within %ss\n' "$variant" "$TRINO_TIMEOUT" >&2
      kill "$log_pid" 2>/dev/null || true
      tail_file "$logfile" 100
      return 124
    fi
    sleep 2
  done
  kill "$log_pid" 2>/dev/null || true
  wait "$log_pid" 2>/dev/null || true

  remaining=$((deadline - $(date +%s)))
  [ "$remaining" -gt 0 ] || die "Trino ${variant} consumed its whole ${TRINO_TIMEOUT}s budget before the query"

  if [ "$variant" = "catalog" ]; then
    sql="SELECT path, size_bytes, commit_id, user_metadata FROM lakefs.\"system.metadata.${MDS_REPO}.main\".objects WHERE path LIKE 'orders/%';"
  else
    sql="SELECT path, size_bytes, commit_id, user_metadata FROM lakefs.\"${MDS_REPO}.main\".objects WHERE path LIKE 'orders/%';"
  fi

  log "querying Trino ${variant}"
  run_bounded "$remaining" "$querylog" -- docker exec "$name" /usr/bin/trino \
    --output-format TSV --execute "$sql"
  # The guide selects path, size_bytes, commit_id and user_metadata. TSV keeps them tab-separated,
  # so the first three fields are the row the script compares with state.env's SOURCE_COMMIT.
  awk -F'\t' '$1 ~ /^orders\// { print $1 "\t" $2 "\t" $3 }' "$querylog" |
    tr -d '\r' >"$rows_file"
  assert_orders_rows "$rows_file" "Trino ${variant}"

  docker_rm "$name"
  CURRENT_CONTAINER=
}

run_trino_variant catalog \
  "${MDS_DOCKER_SERVER_URL}/iceberg/api" \
  "${MDS_DOCKER_SERVER_URL}/iceberg/api/v1/oauth/tokens" \
  true
run_trino_variant relative \
  "${MDS_DOCKER_SERVER_URL}/iceberg/relative_to/system.metadata/api" \
  "${MDS_DOCKER_SERVER_URL}/iceberg/api/v1/oauth/tokens" \
  true
assert_same_rows "$RUN_DIR/trino-catalog.rows.tsv" "$RUN_DIR/trino-relative.rows.tsv" "Trino"
log "Trino snippets passed"
