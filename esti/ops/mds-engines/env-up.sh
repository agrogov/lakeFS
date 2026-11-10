#!/usr/bin/env bash
# Start the throw-away Metadata Search installation the engine recipes run against.
#
# It writes the same configuration esti/mds_test.go generates (iceberg.enabled,
# iceberg.metadata_search.enabled, the SSO/ACL installation admin, and the guide's metadata_search
# key list), starts lakefs-sso and `lakefs-sso mds run` on that one configuration, creates my-repo with
# objects under orders/, and waits until the index of main is at the branch head.
#
# The indexer opens the same database as the server (RA4b), so the script starts a throw-away
# PostgreSQL container for the run: a local Badger database cannot be opened by two processes.
#
# The object store is the SeaweedFS of esti/ops/docker-compose-mds.yaml. Reuse a running one; this
# script never starts or stops it.

set -euo pipefail

# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

MDS_SERVER_PORT=${MDS_SERVER_PORT:-8000}
MDS_INDEXER_PORT=${MDS_INDEXER_PORT:-18080}
MDS_OIDC_PORT=${MDS_OIDC_PORT:-18099}
MDS_REPO=${MDS_REPO:-my-repo}
MDS_BUCKET=${MDS_BUCKET:-esti-mds-engines}
MDS_SEAWEEDFS_ENDPOINT=${MDS_SEAWEEDFS_ENDPOINT:-http://127.0.0.1:8333}
MDS_SEAWEEDFS_CONTAINER_ENDPOINT=${MDS_SEAWEEDFS_CONTAINER_ENDPOINT:-http://seaweedfs:8333}
MDS_S3_REGION=${MDS_S3_REGION:-us-east-1}
MDS_S3_ACCESS_KEY=${MDS_S3_ACCESS_KEY:-estiaccesskey}
MDS_S3_SECRET_KEY=${MDS_S3_SECRET_KEY:-estisecretkey}
MDS_ADMIN_KEY=${MDS_ADMIN_KEY:-AKIAIOSFDNN7EXAMPLEQ}
MDS_ADMIN_SECRET=${MDS_ADMIN_SECRET:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}
MDS_INDEXER_PERIOD=${MDS_INDEXER_PERIOD:-2s}
MDS_POSTGRES_PORT=${MDS_POSTGRES_PORT:-15432}
MDS_POSTGRES_IMAGE=${MDS_POSTGRES_IMAGE:-postgres@sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139}
MDS_POSTGRES_CONTAINER=${MDS_POSTGRES_CONTAINER:-mds-engines-postgres}
MDS_POSTGRES_USER=${MDS_POSTGRES_USER:-lakefs}
MDS_POSTGRES_PASSWORD=${MDS_POSTGRES_PASSWORD:-lakefs}
MDS_POSTGRES_DB=${MDS_POSTGRES_DB:-lakefs}
MDS_POSTGRES_START_TIMEOUT=${MDS_POSTGRES_START_TIMEOUT:-60}
MDS_INDEXER_START_TIMEOUT=${MDS_INDEXER_START_TIMEOUT:-120}
MDS_SERVER_START_TIMEOUT=${MDS_SERVER_START_TIMEOUT:-120}
MDS_INDEX_TIMEOUT=${MDS_INDEX_TIMEOUT:-120}

MDS_RUN_PREFIX=${MDS_RUN_PREFIX:-"mds-engines-$(date +%Y%m%d%H%M%S)"}
MDS_NAMESPACE="s3://${MDS_BUCKET}/${MDS_RUN_PREFIX}/${MDS_REPO}"
MDS_SERVER_URL="http://127.0.0.1:${MDS_SERVER_PORT}"
MDS_DOCKER_SERVER_URL="http://lakefs:${MDS_SERVER_PORT}"
MDS_INDEXER_URL="http://127.0.0.1:${MDS_INDEXER_PORT}"
MDS_OIDC_URL="http://127.0.0.1:${MDS_OIDC_PORT}"

SUCCESS=0
cleanup_failed() {
  if [ "$SUCCESS" -eq 1 ]; then
    return 0
  fi
  log "setup failed; stopping the processes this script started"
  for name in indexer server oidc; do
    if [ -f "$RUN_DIR/$name.pid" ]; then
      stop_pid "$(cat "$RUN_DIR/$name.pid")" "$name" || true
      rm -f "$RUN_DIR/$name.pid"
    fi
  done
  docker rm -f "$MDS_POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  rm -f "$STATE_FILE"
}
trap cleanup_failed EXIT

require_command docker
require_command curl
require_command go
require_command jq
require_command python3

if [ -f "$STATE_FILE" ]; then
  log "an existing environment is recorded in $STATE_FILE; stopping it first"
  "$ENGINE_DIR/env-down.sh" || true
fi

mkdir -p "$RUN_DIR" "$BIN_DIR" "$RUN_DIR/oidc/.well-known"

# Build the two binaries this recipe needs. lakefs-sso serves the catalog and the virtual table;
# mds-bucket creates the one bucket lakeFS does not create itself.
log "building lakefs-sso and mds-bucket"
(cd "$REPO_ROOT" && go build -o "$BIN_DIR/lakefs-sso" ./cmd/lakefs-sso)
(cd "$REPO_ROOT" && go build -o "$BIN_DIR/mds-bucket" ./esti/ops/mds-engines/tools/bucket)

# The SSO/ACL auth service discovers its issuer at startup. The discovery document is static here:
# no login is performed, and the snippet authenticates every client with a lakeFS key pair.
cat >"$RUN_DIR/oidc/.well-known/openid-configuration" <<EOF
{
  "issuer": "${MDS_OIDC_URL}",
  "authorization_endpoint": "${MDS_OIDC_URL}/authorize",
  "token_endpoint": "${MDS_OIDC_URL}/token",
  "jwks_uri": "${MDS_OIDC_URL}/jwks",
  "userinfo_endpoint": "${MDS_OIDC_URL}/userinfo",
  "response_types_supported": ["code"],
  "subject_types_supported": ["public"],
  "id_token_signing_alg_values_supported": ["RS256"],
  "scopes_supported": ["openid", "profile", "email"]
}
EOF

python3 -m http.server "$MDS_OIDC_PORT" --bind 127.0.0.1 --directory "$RUN_DIR/oidc" \
  >"$RUN_DIR/oidc.log" 2>&1 &
echo $! >"$RUN_DIR/oidc.pid"
wait_for_http "${MDS_OIDC_URL}/.well-known/openid-configuration" 20 "OIDC discovery document" "$RUN_DIR/oidc.log"

# The database both processes open. `lakefs mds run` runs on the lakeFS configuration (RA4b), so it
# opens database.* in its own process; a local Badger database refuses a second process.
log "starting PostgreSQL on 127.0.0.1:${MDS_POSTGRES_PORT}"
docker rm -f "$MDS_POSTGRES_CONTAINER" >/dev/null 2>&1 || true
docker run -d --rm --name "$MDS_POSTGRES_CONTAINER" \
  -e "POSTGRES_USER=${MDS_POSTGRES_USER}" \
  -e "POSTGRES_PASSWORD=${MDS_POSTGRES_PASSWORD}" \
  -e "POSTGRES_DB=${MDS_POSTGRES_DB}" \
  -p "127.0.0.1:${MDS_POSTGRES_PORT}:5432" \
  "$MDS_POSTGRES_IMAGE" >/dev/null

postgres_start=$(date +%s)
while :; do
  if docker exec "$MDS_POSTGRES_CONTAINER" pg_isready -U "$MDS_POSTGRES_USER" -d "$MDS_POSTGRES_DB" 2>/dev/null | grep -q 'accepting connections'; then
    break
  fi
  if [ $(( $(date +%s) - postgres_start )) -ge "$MDS_POSTGRES_START_TIMEOUT" ]; then
    printf 'mds-engines: PostgreSQL did not accept connections within %ss\n' "$MDS_POSTGRES_START_TIMEOUT" >&2
    docker logs "$MDS_POSTGRES_CONTAINER" >&2 || true
    exit 1
  fi
  sleep 1
done
MDS_POSTGRES_DSN="postgres://${MDS_POSTGRES_USER}:${MDS_POSTGRES_PASSWORD}@127.0.0.1:${MDS_POSTGRES_PORT}/${MDS_POSTGRES_DB}?sslmode=disable"

write_config() {
  cat >"$RUN_DIR/lakefs.yaml" <<YAML
listen_address: "0.0.0.0:${MDS_SERVER_PORT}"
logging:
  format: text
  level: "warn"
database:
  type: postgres
  postgres:
    connection_string: "${MDS_POSTGRES_DSN}"
committed:
  local_cache:
    dir: "${RUN_DIR}/cache"
installation:
  user_name: "mds-admin"
  access_key_id: "${MDS_ADMIN_KEY}"
  secret_access_key: "${MDS_ADMIN_SECRET}"
auth:
  encrypt:
    secret_key: "mds-engines-local-secret-key"
  ui_config:
    rbac: "internal"
sso:
  enabled: true
  client_id: "mds-engines"
  client_secret: "mds-engines-client-secret"
  issuer_url: "${MDS_OIDC_URL}"
  callback_base_url: "${MDS_SERVER_URL}"
blockstore:
  type: s3
  s3:
    region: "${MDS_S3_REGION}"
    endpoint: "${MDS_SEAWEEDFS_ENDPOINT}"
    force_path_style: true
    discover_bucket_region: false
    credentials:
      access_key_id: "${MDS_S3_ACCESS_KEY}"
      secret_access_key: "${MDS_S3_SECRET_KEY}"
iceberg:
  enabled: true
  prefix: "lakefs"
  metadata_search:
    enabled: true
metadata_search:
  lakefs_endpoint: "${MDS_SERVER_URL}"
  listen_address: "0.0.0.0:${MDS_INDEXER_PORT}"
  period: ${MDS_INDEXER_PERIOD}
  concurrency: 4
  max_commits: 1000
  repositories:
    ${MDS_REPO}:
      branches: [main, feature]
YAML
}

# One configuration for both processes: the sync identity is provisioned by the server at startup
# (mds-service-user with MDSCatalogAccess) and the indexer signs its own token, so there is no
# credential to create and no second configuration to write.
write_config

log "starting lakefs-sso on ${MDS_SERVER_URL}"
"$BIN_DIR/lakefs-sso" run --config "$RUN_DIR/lakefs.yaml" >"$RUN_DIR/server.log" 2>&1 &
echo $! >"$RUN_DIR/server.pid"

wait_for_process_http() {
  local name=$1 pid_file=$2 url=$3 timeout=$4 logfile=$5
  local pid start
  pid=$(cat "$pid_file")
  start=$(date +%s)
  while :; do
    if ! kill -0 "$pid" 2>/dev/null; then
      printf 'mds-engines: %s exited while starting\n' "$name" >&2
      tail_file "$logfile" 80
      return 1
    fi
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    if [ $(( $(date +%s) - start )) -ge "$timeout" ]; then
      printf 'mds-engines: %s did not answer %s within %ss\n' "$name" "$url" "$timeout" >&2
      tail_file "$logfile" 80
      return 1
    fi
    sleep 1
  done
}

wait_for_process_http "lakefs-sso" "$RUN_DIR/server.pid" "${MDS_SERVER_URL}/_health" "$MDS_SERVER_START_TIMEOUT" "$RUN_DIR/server.log"

"$BIN_DIR/mds-bucket" \
  -endpoint "$MDS_SEAWEEDFS_ENDPOINT" \
  -access-key-id "$MDS_S3_ACCESS_KEY" \
  -secret-access-key "$MDS_S3_SECRET_KEY" \
  -region "$MDS_S3_REGION" \
  -bucket "$MDS_BUCKET"

api() {
  curl -fsS -u "${MDS_ADMIN_KEY}:${MDS_ADMIN_SECRET}" -H 'Content-Type: application/json' "$@"
}

# Remove repositories from an earlier run of the same names. Repositories are global while the
# namespace is unique per run, so only the names need cleaning; the old objects stay under their
# own run prefix and are never read. The server acknowledges the delete before the name is free
# again, so wait for GET to answer 404 before creating the same name.
delete_repository() {
  local repo=$1 status deadline
  status=$(curl -s -o /dev/null -w '%{http_code}' -u "${MDS_ADMIN_KEY}:${MDS_ADMIN_SECRET}" \
    "${MDS_SERVER_URL}/api/v1/repositories/${repo}")
  if [ "$status" != "200" ]; then
    return 0
  fi
  log "deleting repository ${repo} from an earlier run"
  curl -fsS -u "${MDS_ADMIN_KEY}:${MDS_ADMIN_SECRET}" -X DELETE "${MDS_SERVER_URL}/api/v1/repositories/${repo}?force=true" >/dev/null
  deadline=$(( $(date +%s) + 30 ))
  while :; do
    status=$(curl -s -o /dev/null -w '%{http_code}' -u "${MDS_ADMIN_KEY}:${MDS_ADMIN_SECRET}" \
      "${MDS_SERVER_URL}/api/v1/repositories/${repo}")
    [ "$status" = "404" ] && return 0
    if [ "$(date +%s)" -ge "$deadline" ]; then
      die "repository ${repo} is still present (status ${status}) after delete"
    fi
    sleep 1
  done
}

delete_repository "$MDS_REPO"
delete_repository "$MDS_REPO-metadata"

log "creating repository ${MDS_REPO} with namespace ${MDS_NAMESPACE}"
api -X POST "${MDS_SERVER_URL}/api/v1/repositories" \
  -d "$(jq -n --arg name "$MDS_REPO" --arg namespace "$MDS_NAMESPACE" \
    '{name: $name, storage_namespace: $namespace, default_branch: "main"}')" >/dev/null

for path in \
  orders/2024/one.parquet \
  orders/2024/two.parquet \
  orders/2025/three.parquet \
  customers/c1.json; do
  api -X POST "${MDS_SERVER_URL}/api/v1/repositories/${MDS_REPO}/branches/main/objects?path=${path}" \
    --data-binary "content of ${path}" >/dev/null
done

SOURCE_COMMIT=$(api -X POST "${MDS_SERVER_URL}/api/v1/repositories/${MDS_REPO}/branches/main/commits" \
  -d '{"message":"seed objects for the docs snippets"}' | jq -r '.id')
[ -n "$SOURCE_COMMIT" ] && [ "$SOURCE_COMMIT" != "null" ] || die "could not read the commit id"

log "starting the indexer on ${MDS_INDEXER_URL}"
LAKEFS_COMMITTED_LOCAL_CACHE_DIR="${RUN_DIR}/mds-cache" \
  "$BIN_DIR/lakefs-sso" mds run --config "$RUN_DIR/lakefs.yaml" >"$RUN_DIR/indexer.log" 2>&1 &
echo $! >"$RUN_DIR/indexer.pid"

wait_for_process_http "the Metadata Search indexer" "$RUN_DIR/indexer.pid" "${MDS_INDEXER_URL}/_health" "$MDS_INDEXER_START_TIMEOUT" "$RUN_DIR/indexer.log"

wait_indexed() {
  local start pid report last err
  start=$(date +%s)
  pid=$(cat "$RUN_DIR/indexer.pid")
  while :; do
    if ! kill -0 "$pid" 2>/dev/null; then
      printf 'mds-engines: the indexer exited while waiting for %s\n' "$SOURCE_COMMIT" >&2
      tail_file "$RUN_DIR/indexer.log" 80
      return 1
    fi
    report=$(curl -s "${MDS_INDEXER_URL}/_health" 2>/dev/null || true)
    if [ -n "$report" ]; then
      err=$(printf '%s' "$report" | jq -r --arg repo "$MDS_REPO" \
        '.repositories[$repo].last_error // .repositories[$repo].branches.main.last_error // empty' 2>/dev/null || true)
      if [ -n "$err" ]; then
        printf 'mds-engines: the indexer failed on %s: %s\n' "$MDS_REPO" "$err" >&2
        tail_file "$RUN_DIR/indexer.log" 80
        return 1
      fi
      last=$(printf '%s' "$report" | jq -r --arg repo "$MDS_REPO" \
        '.repositories[$repo].branches.main.last_indexed // empty' 2>/dev/null || true)
      if [ "$last" = "$SOURCE_COMMIT" ]; then
        return 0
      fi
    fi
    if [ $(( $(date +%s) - start )) -ge "$MDS_INDEX_TIMEOUT" ]; then
      printf 'mds-engines: the index of %s@main did not reach %s within %ss; last report: %s\n' \
        "$MDS_REPO" "$SOURCE_COMMIT" "$MDS_INDEX_TIMEOUT" "${report:-<none>}" >&2
      tail_file "$RUN_DIR/indexer.log" 80
      return 1
    fi
    sleep 1
  done
}

wait_indexed

cat >"$STATE_FILE" <<EOF
MDS_SERVER_PORT="${MDS_SERVER_PORT}"
MDS_INDEXER_PORT="${MDS_INDEXER_PORT}"
MDS_OIDC_PORT="${MDS_OIDC_PORT}"
MDS_REPO="${MDS_REPO}"
MDS_METADATA_REPO="${MDS_REPO}-metadata"
MDS_BUCKET="${MDS_BUCKET}"
MDS_POSTGRES_CONTAINER="${MDS_POSTGRES_CONTAINER}"
MDS_RUN_PREFIX="${MDS_RUN_PREFIX}"
MDS_NAMESPACE="${MDS_NAMESPACE}"
MDS_SEAWEEDFS_ENDPOINT="${MDS_SEAWEEDFS_ENDPOINT}"
MDS_SEAWEEDFS_CONTAINER_ENDPOINT="${MDS_SEAWEEDFS_CONTAINER_ENDPOINT}"
MDS_S3_REGION="${MDS_S3_REGION}"
MDS_S3_ACCESS_KEY="${MDS_S3_ACCESS_KEY}"
MDS_S3_SECRET_KEY="${MDS_S3_SECRET_KEY}"
MDS_ADMIN_KEY="${MDS_ADMIN_KEY}"
MDS_ADMIN_SECRET="${MDS_ADMIN_SECRET}"
MDS_SERVER_URL="${MDS_SERVER_URL}"
MDS_DOCKER_SERVER_URL="${MDS_DOCKER_SERVER_URL}"
MDS_INDEXER_URL="${MDS_INDEXER_URL}"
SOURCE_COMMIT="${SOURCE_COMMIT}"
EOF

SUCCESS=1
trap - EXIT

log "environment ready; state is in $STATE_FILE"
log "  lakeFS:   ${MDS_SERVER_URL}"
log "  indexer:  ${MDS_INDEXER_URL}"
log "  repo:     ${MDS_REPO} (namespace ${MDS_NAMESPACE})"
log "  indexed:  ${SOURCE_COMMIT}"
log "run run-spark.sh and run-trino.sh next; stop it with env-down.sh"
