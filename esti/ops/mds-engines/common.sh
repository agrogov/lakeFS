#!/usr/bin/env bash
# Shared helpers for the Metadata Search engine recipes of docs/metadata-search.md.
#
# The scripts under esti/ops/mds-engines/ start a throw-away lakefs-sso with
# iceberg.enabled and iceberg.metadata_search.enabled on the SeaweedFS instance of
# esti/ops/docker-compose-mds.yaml, run `lakefs-sso mds run` next to it, and then run the Spark and
# Trino snippets of the guide against both catalog endpoints. They do not edit, or depend on, the
# esti suite's own configuration: they write the same layout into .run/.
#
# Every wait and every container run is bounded in seconds. A child that dies is reported at once
# with its exit status and the tail of its log; it is never waited out.

set -euo pipefail

ENGINE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$ENGINE_DIR/../../.." && pwd)
RUN_DIR=${MDS_ENGINES_RUN_DIR:-"$ENGINE_DIR/.run"}
STATE_FILE="$RUN_DIR/state.env"
BIN_DIR="$RUN_DIR/bin"

die() {
  printf 'mds-engines: %s\n' "$*" >&2
  exit 1
}

log() {
  printf 'mds-engines: %s\n' "$*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

tail_file() {
  local file=$1 lines=${2:-60}
  if [ -f "$file" ]; then
    printf -- '--- %s (last %s lines) ---\n' "$file" "$lines" >&2
    tail -n "$lines" "$file" >&2 || true
  fi
}

load_state() {
  [ -f "$STATE_FILE" ] || die "no environment state at $STATE_FILE; run env-up.sh first"
  # shellcheck disable=SC1090
  . "$STATE_FILE"
}

# write_expected_orders_rows FILE COMMIT
# The three rows env-up.sh uploads, as the guide's engine snippets project them: path, size_bytes,
# commit_id. The commit is the SOURCE_COMMIT of state.env, so a stale snapshot, a row of another
# repository or a different commit fails the comparison.
write_expected_orders_rows() {
  local file=$1 commit=$2
  printf '%s\t%s\t%s\n' \
    'orders/2024/one.parquet' 34 "$commit" \
    'orders/2024/two.parquet' 34 "$commit" \
    'orders/2025/three.parquet' 36 "$commit" |
    LC_ALL=C sort >"$file"
}

# normalize_rows INPUT OUTPUT
# Trim CRs and blank lines and sort, so the endpoint comparison is about the rows and not about the
# order or the line ending an engine happened to print.
normalize_rows() {
  local input=$1 output=$2
  tr -d '\r' <"$input" | sed '/^[[:space:]]*$/d' | LC_ALL=C sort >"$output"
}

# assert_orders_rows FILE DESCRIPTION
# Require exactly the three expected orders/ rows of state.env. A file with a missing row, an extra
# row, a different size or a different commit_id fails here, not in a human reading .run/*.log.
assert_orders_rows() {
  local file=$1 description=$2
  local expected="$RUN_DIR/expected-orders.tsv"
  local normalized="$file.normalized"
  local count

  write_expected_orders_rows "$expected" "$SOURCE_COMMIT"
  normalize_rows "$file" "$normalized"
  count=$(wc -l <"$normalized" | tr -d ' ')
  if [ "$count" -ne 3 ]; then
    printf 'mds-engines: %s returned %s orders/ rows, expected exactly 3; rows:\n' "$description" "$count" >&2
    cat "$normalized" >&2 || true
    die "$description returned the wrong number of orders/ rows"
  fi
  if ! diff -u "$expected" "$normalized" >&2; then
    die "$description rows differ from the expected orders/ rows (SOURCE_COMMIT=${SOURCE_COMMIT})"
  fi
  log "$description returned exactly the three expected orders/ rows at ${SOURCE_COMMIT}"
}

# assert_same_rows FILE_A FILE_B DESCRIPTION
# The two catalog endpoints must return the same rows: diff their normalized captures.
assert_same_rows() {
  local a=$1 b=$2 description=$3
  if ! diff -u "$a.normalized" "$b.normalized" >&2; then
    die "$description returned different rows through the two catalog endpoints"
  fi
  log "$description returned the same rows through both catalog endpoints"
}

docker_rm() {
  docker rm -f "$1" >/dev/null 2>&1 || true
}

# wait_for_http URL TIMEOUT_SECONDS DESCRIPTION LOGFILE
wait_for_http() {
  local url=$1 timeout=$2 description=$3 logfile=${4:-}
  local start
  start=$(date +%s)
  while :; do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    if [ $(( $(date +%s) - start )) -ge "$timeout" ]; then
      printf 'mds-engines: timed out after %ss waiting for %s (%s)\n' "$timeout" "$description" "$url" >&2
      tail_file "$logfile" 80
      return 1
    fi
    sleep 1
  done
}

# wait_for_pid_exit PID TIMEOUT_SECONDS
wait_for_pid_exit() {
  local pid=$1 timeout=$2
  local start
  start=$(date +%s)
  while kill -0 "$pid" 2>/dev/null; do
    if [ $(( $(date +%s) - start )) -ge "$timeout" ]; then
      return 1
    fi
    sleep 1
  done
  return 0
}

# stop_pid PID NAME
stop_pid() {
  local pid=$1 name=$2
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  log "stopping $name (pid $pid)"
  kill -TERM "$pid" 2>/dev/null || true
  if ! wait_for_pid_exit "$pid" 10; then
    log "$name did not stop within 10s; killing it"
    kill -KILL "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

# run_bounded TIMEOUT_SECONDS LOGFILE -- command...
# Run a foreground command with a wall-clock budget, keeping its output in LOGFILE.
run_bounded() {
  local timeout=$1 logfile=$2
  shift 2
  [ "${1:-}" = "--" ] && shift
  local start pid rc
  start=$(date +%s)
  "$@" >"$logfile" 2>&1 &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    if [ $(( $(date +%s) - start )) -ge "$timeout" ]; then
      printf 'mds-engines: command exceeded its %ss budget: %s\n' "$timeout" "$*" >&2
      kill -TERM "$pid" 2>/dev/null || true
      sleep 2
      kill -KILL "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      tail_file "$logfile" 80
      return 124
    fi
    sleep 1
  done
  if wait "$pid"; then
    rc=0
  else
    rc=$?
  fi
  if [ "$rc" -ne 0 ]; then
    printf 'mds-engines: command failed with exit status %s: %s\n' "$rc" "$*" >&2
    tail_file "$logfile" 80
  fi
  return "$rc"
}

# run_docker_bounded NAME TIMEOUT_SECONDS LOGFILE -- docker run arguments...
# The container is named, so a timed-out or failed run can always be removed. The timeout is the
# wall clock of the whole run, including image startup.
run_docker_bounded() {
  local name=$1 timeout=$2 logfile=$3
  shift 3
  [ "${1:-}" = "--" ] && shift
  docker_rm "$name"
  local start pid rc
  start=$(date +%s)
  docker run --name "$name" --rm "$@" >"$logfile" 2>&1 &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    if [ $(( $(date +%s) - start )) -ge "$timeout" ]; then
      printf 'mds-engines: container %s exceeded its %ss budget\n' "$name" "$timeout" >&2
      docker_rm "$name"
      kill -TERM "$pid" 2>/dev/null || true
      sleep 2
      kill -KILL "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      tail_file "$logfile" 80
      return 124
    fi
    sleep 1
  done
  if wait "$pid"; then
    rc=0
  else
    rc=$?
  fi
  if [ "$rc" -ne 0 ]; then
    printf 'mds-engines: container %s failed with exit status %s\n' "$name" "$rc" >&2
    tail_file "$logfile" 80
  fi
  docker_rm "$name"
  return "$rc"
}
