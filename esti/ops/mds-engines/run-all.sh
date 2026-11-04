#!/usr/bin/env bash
# Run every engine snippet of docs/metadata-search.md that this directory covers - Spark and Trino,
# both the /mds/iceberg/api and the /iceberg/relative_to form - against a fresh local lakefs-sso and
# indexer on SeaweedFS.
#
# The environment is torn down on success and on failure.

set -euo pipefail

# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

cleanup() {
  "$ENGINE_DIR/env-down.sh" || true
}
trap cleanup EXIT

"$ENGINE_DIR/env-up.sh"
"$ENGINE_DIR/run-spark.sh"
"$ENGINE_DIR/run-trino.sh"
log "all engine snippets passed"
