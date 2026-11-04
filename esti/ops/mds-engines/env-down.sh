#!/usr/bin/env bash
# Stop the lakefs-sso, indexer and OIDC issuer env-up.sh started.
#
# SeaweedFS is deliberately not touched: esti/ops/docker-compose-mds.yaml owns it and another run
# may be using it.

set -euo pipefail

# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [ -f "$STATE_FILE" ]; then
  # shellcheck disable=SC1090
  . "$STATE_FILE"
fi

for name in indexer server oidc; do
  if [ -f "$RUN_DIR/$name.pid" ]; then
    pid=$(cat "$RUN_DIR/$name.pid")
    stop_pid "$pid" "$name" || true
    rm -f "$RUN_DIR/$name.pid"
  fi
done

# Engine containers are removed by their runners, but remove leftovers from an interrupted run.
for name in mds-spark-mds mds-spark-relative mds-trino-mds mds-trino-relative; do
  docker_rm "$name"
done

rm -f "$STATE_FILE"
log "environment stopped"
